"""Owner-authorized reduction of one immutable approved strategy target.

This adapter deliberately has no position, quantity, direction, account, or
strategy selector of its own.  It resolves a previously approved AFI target
through the exact ``ApprovedTargetAuthorityV1`` configured on the existing
``ExecutionOrderEntryService``, accepts only the v1 NET/ALL ``CLOSE`` shape,
and submits through the existing approved-target/Risk/Execution path.

The caller must expose this handler only behind the exact ``owner.reduce``
service grant and a live exact account/strategy/run claim.  This module checks
the exact server-authenticated service identity and owner binding, but does
not replace the transport registry's operation authorization.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from .approved_target import (
    APPROVED_TARGET_OPERATION,
    ApprovedTargetAuthorityError,
    ApprovedTargetAuthorityV1,
    ApprovedTargetDocumentV1,
    ApprovedTargetSubmitRequestV1,
)
from .contracts import ExecutionState, PositionEffect, TERMINAL_STATES
from .errors import ContractValidationError
from .service_order_entry import ExecutionOrderEntryService

OWNER_REDUCE_OPERATION = "owner.reduce"
_REQUEST_SCHEMA_ID = "bt_api.owner_strategy_reduction"
_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_REQUEST_FIELDS = frozenset(
    {
        "schema_id",
        "schema_version",
        "approval_ref",
        "approval_version",
        "target_sha256",
        "intent_id",
    }
)
_DEFINITE_NOT_DISPATCHED_CODES = frozenset(
    {
        "INVALID_REQUEST",
        "ROLE_NOT_AUTHORIZED",
        "SCOPE_DENIED",
        "UNSUPPORTED_OPERATION",
        "APPROVAL_REQUIRED",
        "APPROVED_TARGET_SCOPE_NOT_PROTECTED",
        "APPROVAL_OWNER_BINDING_MISMATCH",
        "APPROVAL_IDENTITY_MISMATCH",
        "APPROVAL_SCOPE_MISMATCH",
        "APPROVAL_NOT_FOUND",
        "APPROVAL_NOT_APPROVED",
        "APPROVAL_REVOKED",
        "APPROVAL_EXPIRED",
        "APPROVAL_VERSION_MISMATCH",
        "APPROVAL_TARGET_DIGEST_MISMATCH",
        "APPROVAL_TARGET_INVALID",
        "APPROVAL_POLICY_STALE",
        "APPROVAL_OPERATION_CONFLICT",
        "APPROVAL_CONSUMED_CONFLICT",
    }
)


@dataclass(frozen=True, slots=True)
class OwnerStrategyReductionRequestV1:
    """A reduction command references one approved target and its durable ID.

    ``intent_id`` must be the ID returned by AFI's existing persistent
    approved-target operation mapping.  No client-controlled order terms or
    scope selectors are accepted here.
    """

    approval_ref: str
    approval_version: int
    target_sha256: str
    intent_id: str

    def __post_init__(self) -> None:
        if type(self.approval_ref) is not str or not _REFERENCE.fullmatch(self.approval_ref):
            raise ContractValidationError("invalid reduction approval_ref")
        if type(self.approval_version) is not int or self.approval_version <= 0:
            raise ContractValidationError("invalid reduction approval_version")
        if type(self.target_sha256) is not str or not _SHA256.fullmatch(self.target_sha256):
            raise ContractValidationError("invalid reduction target_sha256")
        if type(self.intent_id) is not str or not _REFERENCE.fullmatch(self.intent_id):
            raise ContractValidationError("invalid reduction intent_id")

    @classmethod
    def from_arguments(cls, value: object) -> OwnerStrategyReductionRequestV1:
        if not isinstance(value, Mapping) or any(type(key) is not str for key in value):
            raise ContractValidationError("reduction request must be a mapping")
        if frozenset(value) != _REQUEST_FIELDS:
            raise ContractValidationError("reduction request has an invalid shape")
        if (
            type(value["schema_id"]) is not str
            or value["schema_id"] != _REQUEST_SCHEMA_ID
            or type(value["schema_version"]) is not int
            or value["schema_version"] != 1
        ):
            raise ContractValidationError("unsupported reduction request schema")
        return cls(
            approval_ref=value["approval_ref"],
            approval_version=value["approval_version"],
            target_sha256=value["target_sha256"],
            intent_id=value["intent_id"],
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_id": _REQUEST_SCHEMA_ID,
            "schema_version": 1,
            "approval_ref": self.approval_ref,
            "approval_version": self.approval_version,
            "target_sha256": self.target_sha256,
            "intent_id": self.intent_id,
        }

    def approved_target_request(self) -> ApprovedTargetSubmitRequestV1:
        return ApprovedTargetSubmitRequestV1(
            approval_ref=self.approval_ref,
            approval_version=self.approval_version,
            target_sha256=self.target_sha256,
            intent_id=self.intent_id,
        )


class StrategyReductionStatus(StrEnum):
    NOT_DISPATCHED = "NOT_DISPATCHED"
    PENDING = "PENDING"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    TERMINAL = "TERMINAL"
    UNKNOWN = "UNKNOWN"


class StrategyReductionStage(StrEnum):
    AUTHORITY = "AUTHORITY"
    EXECUTION = "EXECUTION"


@dataclass(frozen=True, slots=True)
class StrategyReductionOutcomeV1:
    """Finite projection of the current durable result for this target.

    ``FILLED`` means the approved order quantity filled.  It does not claim
    the strategy or account is flat.  A partial fill carries its actual
    cumulative quantity, while ACKED and other open states remain PENDING.
    """

    intent_id: str
    status: StrategyReductionStatus
    stage: StrategyReductionStage
    execution_state: ExecutionState | None
    filled_quantity: str
    is_terminal: bool
    review_required: bool
    reason_code: str

    def __post_init__(self) -> None:
        if type(self.intent_id) is not str or not _REFERENCE.fullmatch(self.intent_id):
            raise ContractValidationError("invalid reduction outcome intent_id")
        if type(self.status) is not StrategyReductionStatus:
            raise ContractValidationError("invalid reduction outcome status")
        if type(self.stage) is not StrategyReductionStage:
            raise ContractValidationError("invalid reduction outcome stage")
        if self.execution_state is not None and type(self.execution_state) is not ExecutionState:
            raise ContractValidationError("invalid reduction outcome execution_state")
        _quantity(self.filled_quantity)
        if type(self.is_terminal) is not bool or type(self.review_required) is not bool:
            raise ContractValidationError("invalid reduction outcome flags")
        if type(self.reason_code) is not str or not _SAFE_CODE.fullmatch(self.reason_code):
            raise ContractValidationError("invalid reduction outcome reason_code")
        if self.execution_state is None:
            if self.is_terminal or self.filled_quantity != "0":
                raise ContractValidationError("unpersisted reduction outcome is inconsistent")
            if self.status not in {
                StrategyReductionStatus.NOT_DISPATCHED,
                StrategyReductionStatus.UNKNOWN,
            }:
                raise ContractValidationError("unpersisted reduction outcome has an invalid status")
        else:
            expected_terminal = self.execution_state in TERMINAL_STATES
            if self.is_terminal is not expected_terminal:
                raise ContractValidationError("reduction outcome terminal flag is inconsistent")
            if self.status is not _status_for_state(self.execution_state):
                raise ContractValidationError("reduction outcome status is inconsistent")
            if self.execution_state is ExecutionState.UNKNOWN and not self.review_required:
                raise ContractValidationError("unknown reduction outcome requires review")

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_id": "bt_api.owner_strategy_reduction_outcome",
            "schema_version": 1,
            "intent_id": self.intent_id,
            "status": self.status.value,
            "stage": self.stage.value,
            "execution_state": (
                None if self.execution_state is None else self.execution_state.value
            ),
            "filled_quantity": self.filled_quantity,
            "is_terminal": self.is_terminal,
            "review_required": self.review_required,
            "reason_code": self.reason_code,
        }


class StrategyReductionHandlerV1:
    """Use the configured approved-target authority and existing E write path.

    Construction is restricted to the real package-owned order-entry service;
    callers cannot provide an executor callback or a second Risk/E ledger.
    The service's transport registry must still authorize ``owner.reduce``
    before calling :meth:`handle`.
    """

    def __init__(self, entry_service: ExecutionOrderEntryService) -> None:
        if type(entry_service) is not ExecutionOrderEntryService:
            raise ContractValidationError("reduction requires the canonical execution owner")
        authority = getattr(entry_service, "_approved_target_authority", None)
        if type(authority) is not ApprovedTargetAuthorityV1:
            raise ContractValidationError("reduction requires the configured approved-target authority")
        if APPROVED_TARGET_OPERATION not in entry_service.supported_operations:
            raise ContractValidationError("approved-target operation is unavailable")
        self._entry_service = entry_service
        self._authority = authority

    def handle(
        self,
        *,
        principal_ref: str,
        identity: WireIdentity,
        scope: WireScope,
        arguments: Mapping[str, Any],
    ) -> StrategyReductionOutcomeV1:
        try:
            request = OwnerStrategyReductionRequestV1.from_arguments(arguments)
        except ContractValidationError:
            return _not_dispatched("invalid_request", "INVALID_REQUEST")

        if (
            type(identity) is not WireIdentity
            or identity.role is not WireRole.SERVICE
            or type(principal_ref) is not str
            or identity.principal_ref != principal_ref
            or not _exact_strategy_run_scope(scope)
        ):
            return _not_dispatched(request.intent_id, "OWNER_SCOPE_DENIED")

        binding = self._authority.binding_for(principal_ref, identity, scope)
        if binding is None or not self._authority.requires_approval(scope):
            return _not_dispatched(request.intent_id, "APPROVAL_OWNER_BINDING_MISMATCH")

        target_request = request.approved_target_request()
        try:
            # The AFI authority requires this operation claim before it returns
            # a target.  This is the existing idempotency row, not a reduction
            # ledger; the E service repeats the same idempotent calls below.
            self._authority.reserve(target_request, binding)
            target = self._authority.resolve_target(target_request, binding)
        except ApprovedTargetAuthorityError as error:
            if error.ambiguous:
                return _unknown(request.intent_id, StrategyReductionStage.AUTHORITY)
            return _not_dispatched(request.intent_id, error.code)
        except ContractValidationError:
            return _not_dispatched(request.intent_id, "APPROVAL_TARGET_INVALID")
        except Exception:
            return _unknown(request.intent_id, StrategyReductionStage.AUTHORITY)

        if type(target) is not ApprovedTargetDocumentV1:
            return _not_dispatched(request.intent_id, "APPROVAL_TARGET_INVALID")
        if target.position_effect is not PositionEffect.CLOSE or not target.reduce_only:
            return _not_dispatched(request.intent_id, "TARGET_NOT_REDUCTION")
        # ApprovedTargetDocumentV1 has no position_side/bucket fields.  Its
        # ``to_order_intent`` contract therefore emits only NET/ALL.  The
        # reduction V1 route intentionally rejects CLOSE_TODAY/YESTERDAY above;
        # non-default dimensions need a target version that signs them.

        try:
            reply = self._entry_service.handle(
                APPROVED_TARGET_OPERATION,
                principal_ref=principal_ref,
                identity=identity,
                scope=scope,
                arguments=target_request.to_payload(),
            )
        except Exception:
            # The provider may have accepted the request before the reply was
            # lost.  The stable intent ID is the only safe reconciliation key.
            return _unknown(request.intent_id, StrategyReductionStage.EXECUTION)
        return _outcome_from_reply(request.intent_id, reply)


def _exact_strategy_run_scope(scope: object) -> bool:
    return (
        type(scope) is WireScope
        and scope.account_ref is not None
        and not scope.account_refs
        and scope.strategy_ref is not None
        and scope.run_id is not None
        and scope.manual_book_ref is None
        and scope.group_id is None
        and scope.leg_id is None
    )


def _status_for_state(state: ExecutionState) -> StrategyReductionStatus:
    if state is ExecutionState.FILLED:
        return StrategyReductionStatus.FILLED
    if state is ExecutionState.PARTIALLY_FILLED:
        return StrategyReductionStatus.PARTIAL
    if state is ExecutionState.UNKNOWN:
        return StrategyReductionStatus.UNKNOWN
    if state in TERMINAL_STATES:
        return StrategyReductionStatus.TERMINAL
    return StrategyReductionStatus.PENDING


def _outcome_from_reply(intent_id: str, reply: object) -> StrategyReductionOutcomeV1:
    if not isinstance(reply, Mapping) or set(reply) != {"ok", "code", "result"}:
        return _unknown(intent_id, StrategyReductionStage.EXECUTION)
    if type(reply["ok"]) is not bool or type(reply["code"]) is not str:
        return _unknown(intent_id, StrategyReductionStage.EXECUTION)
    code = reply["code"] if _SAFE_CODE.fullmatch(reply["code"]) else "EXECUTION_REPLY_INVALID"
    result = reply["result"]
    if not isinstance(result, Mapping):
        return _unknown(intent_id, StrategyReductionStage.EXECUTION)
    record = result.get("record")
    if record is None:
        if (
            reply["ok"] is False
            and result.get("found") is False
            and code in _DEFINITE_NOT_DISPATCHED_CODES
        ):
            return _not_dispatched(
                intent_id, code, stage=StrategyReductionStage.EXECUTION
            )
        return _unknown(intent_id, StrategyReductionStage.EXECUTION)
    if not isinstance(record, Mapping):
        return _unknown(intent_id, StrategyReductionStage.EXECUTION)
    try:
        if record.get("intent_id") != intent_id:
            raise ContractValidationError("reduction record intent mismatch")
        state = ExecutionState(record["state"])
        quantity = _quantity(record["filled_quantity"])
        is_terminal = record["is_terminal"]
        review_required = record["review_required"]
        if type(is_terminal) is not bool or type(review_required) is not bool:
            raise ContractValidationError("reduction record flags are invalid")
        if is_terminal is not (state in TERMINAL_STATES):
            raise ContractValidationError("reduction record terminal state is inconsistent")
        return StrategyReductionOutcomeV1(
            intent_id=intent_id,
            status=_status_for_state(state),
            stage=StrategyReductionStage.EXECUTION,
            execution_state=state,
            filled_quantity=quantity,
            is_terminal=is_terminal,
            review_required=review_required or state is ExecutionState.UNKNOWN,
            reason_code=code,
        )
    except (ContractValidationError, KeyError, TypeError, ValueError):
        return _unknown(intent_id, StrategyReductionStage.EXECUTION)


def _quantity(value: object) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ContractValidationError("invalid reduction filled_quantity")
    try:
        number = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ContractValidationError("invalid reduction filled_quantity") from None
    if not number.is_finite() or number < 0:
        raise ContractValidationError("invalid reduction filled_quantity")
    return format(number, "f")


def _not_dispatched(
    intent_id: str,
    reason_code: str,
    *,
    stage: StrategyReductionStage = StrategyReductionStage.AUTHORITY,
) -> StrategyReductionOutcomeV1:
    safe_code = reason_code if _SAFE_CODE.fullmatch(reason_code) else "REDUCTION_NOT_DISPATCHED"
    return StrategyReductionOutcomeV1(
        intent_id=intent_id,
        status=StrategyReductionStatus.NOT_DISPATCHED,
        stage=stage,
        execution_state=None,
        filled_quantity="0",
        is_terminal=False,
        review_required=False,
        reason_code=safe_code,
    )


def _unknown(
    intent_id: str, stage: StrategyReductionStage
) -> StrategyReductionOutcomeV1:
    return StrategyReductionOutcomeV1(
        intent_id=intent_id,
        status=StrategyReductionStatus.UNKNOWN,
        stage=stage,
        execution_state=None,
        filled_quantity="0",
        is_terminal=False,
        review_required=True,
        reason_code="OUTCOME_UNKNOWN",
    )


__all__ = [
    "OWNER_REDUCE_OPERATION",
    "OwnerStrategyReductionRequestV1",
    "StrategyReductionHandlerV1",
    "StrategyReductionOutcomeV1",
    "StrategyReductionStage",
    "StrategyReductionStatus",
]
