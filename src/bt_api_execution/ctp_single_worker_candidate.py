"""Fake-only single-worker contract for the managed CTP command outbox.

This module has no provider imports and is not wired into the SDK facade. It
models the future Store/SDK seam against the existing SQLite execution row:
the caller prepares a complete native request, stages it against the same
database's committed OrderRef reservation, persists the actual local queue
receipt before publishing work, and lets one worker claim and invoke one
injected sender. A sender outcome is only a local dispatch receipt.

The OrderRef reservation check proves equality inside this execution store. It
does not authenticate or reconcile a separate ``bt_api_py`` reservation store.
"""

from __future__ import annotations

import inspect
import json
import re
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from types import MappingProxyType
from typing import Any, Literal, Protocol

from .contracts import ExecutionScope, canonical_json, payload_sha256
from .errors import ContractValidationError
from .store import (
    CtpCancelTarget,
    CtpDispatchAuthorityVerifier,
    CtpDispatchCommand,
    CtpDispatchProjection,
    CtpDispatchReceipt,
    CtpOrderIdentityReservation,
    CtpOrderTargetProjectionHandle,
    SqliteExecutionStore,
    WriterLease,
    _is_local_queue_receipt_id,
)

_HEX_32 = re.compile(r"^[0-9a-f]{32}$", re.ASCII)
_HEX_64 = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_RUNTIME_ID = re.compile(r"^bt-managed-v1:[0-9a-f]{64}$", re.ASCII)
_ORDER_REF = re.compile(r"^[0-9]{12}$", re.ASCII)


def stable_managed_command_id(
    operation: Literal["submit", "cancel"],
    managed_intent_id: str,
    runtime_order_id: str,
    managed_cancel_intent_id: str | None = None,
) -> str:
    """Return Backtrader's versioned stable key for one managed action."""

    if operation not in {"submit", "cancel"}:
        raise ContractValidationError("invalid managed CTP operation")
    if type(managed_intent_id) is not str or not managed_intent_id.isascii():
        raise ContractValidationError("invalid managed CTP intent")
    if type(runtime_order_id) is not str or not _RUNTIME_ID.fullmatch(runtime_order_id):
        raise ContractValidationError("invalid managed CTP runtime order id")
    cancel_id = managed_cancel_intent_id or ""
    if type(cancel_id) is not str or not cancel_id.isascii():
        raise ContractValidationError("invalid managed CTP cancel intent")
    material = "\0".join(
        (
            "backtrader.ctp.managed-outbox.v1",
            operation,
            managed_intent_id,
            runtime_order_id,
            cancel_id,
        )
    )
    return "ctp-outbox-v1:" + sha256(material.encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True)
class CtpManagedPreparedDispatch:
    """Complete pre-enqueue request and authority echoes for one CTP action.

    ``order_ref_reservation`` must be read back from the same execution SQLite
    store during staging. A caller-supplied object alone is not trusted.
    ``local_queue_receipt_id`` is allocated by the queue owner and supplied
    unchanged; the execution package never derives one from ``command_id``.
    """

    operation: Literal["submit", "cancel"]
    command_id: str
    managed_intent_id: str
    runtime_order_id: str
    order_ref: str
    request_payload: Mapping[str, Any]
    order_ref_reservation: CtpOrderIdentityReservation
    approval_use_id: str
    approval_digest: str
    session_binding: Mapping[str, Any]
    session_generation_id: str
    dispatch_front_id: int
    dispatch_session_id: int
    native_request_id: int
    local_queue_receipt_id: str
    managed_cancel_intent_id: str | None = None
    cancel_target_exchange_id: str | None = None
    cancel_target_order_sys_id: str | None = None
    cancel_target_front_id: int | None = None
    cancel_target_session_id: int | None = None

    def __post_init__(self) -> None:
        if self.operation not in {"submit", "cancel"}:
            raise ContractValidationError("invalid managed CTP operation")
        if not isinstance(self.order_ref_reservation, CtpOrderIdentityReservation):
            raise ContractValidationError("typed same-store CTP OrderRef reservation is required")
        if type(self.command_id) is not str or self.command_id != stable_managed_command_id(
            self.operation,
            self.managed_intent_id,
            self.runtime_order_id,
            self.managed_cancel_intent_id,
        ):
            raise ContractValidationError("managed CTP command id does not match stable identity")
        if not _RUNTIME_ID.fullmatch(self.runtime_order_id):
            raise ContractValidationError("invalid managed CTP runtime order id")
        if not _ORDER_REF.fullmatch(self.order_ref):
            raise ContractValidationError("invalid managed CTP OrderRef")
        if not _is_local_queue_receipt_id(self.local_queue_receipt_id):
            raise ContractValidationError("invalid managed CTP local queue receipt id")
        if not isinstance(self.request_payload, Mapping) or not self.request_payload:
            raise ContractValidationError("typed managed CTP request payload is required")
        try:
            request_payload = canonical_json(dict(self.request_payload))
            session_binding = canonical_json(dict(self.session_binding))
        except (TypeError, ValueError) as error:
            raise ContractValidationError(
                "managed CTP prepared binding is not canonical"
            ) from error
        if self.operation == "submit":
            if self.managed_cancel_intent_id is not None:
                raise ContractValidationError("submit cannot carry cancel action identity")
            if any(
                value is not None
                for value in (
                    self.cancel_target_exchange_id,
                    self.cancel_target_order_sys_id,
                    self.cancel_target_front_id,
                    self.cancel_target_session_id,
                )
            ):
                raise ContractValidationError("submit cannot carry a cancel target")
        else:
            if (
                type(self.managed_cancel_intent_id) is not str
                or not self.managed_cancel_intent_id
                or self.managed_cancel_intent_id == self.managed_intent_id
            ):
                raise ContractValidationError("cancel requires a distinct managed action id")
            if (
                type(self.cancel_target_exchange_id) is not str
                or not self.cancel_target_exchange_id
                or type(self.cancel_target_order_sys_id) is not str
                or not self.cancel_target_order_sys_id
                or type(self.cancel_target_front_id) is not int
                or self.cancel_target_front_id <= 0
                or type(self.cancel_target_session_id) is not int
                or self.cancel_target_session_id <= 0
            ):
                raise ContractValidationError("cancel requires a complete exact native target")
        if (
            self.order_ref_reservation.managed_intent_id != self.managed_intent_id
            or self.order_ref_reservation.runtime_order_id != self.runtime_order_id
            or self.order_ref_reservation.order_ref != self.order_ref
        ):
            raise ContractValidationError("prepared CTP OrderRef differs from its reservation")
        if type(self.session_binding) is not dict and not isinstance(self.session_binding, Mapping):
            raise ContractValidationError("typed CTP session binding is required")
        if (
            not isinstance(self.approval_use_id, str)
            or not self.approval_use_id
            or not isinstance(self.approval_digest, str)
            or not _HEX_64.fullmatch(self.approval_digest)
            or not isinstance(self.session_generation_id, str)
            or not self.session_generation_id
            or type(self.dispatch_front_id) is not int
            or self.dispatch_front_id <= 0
            or type(self.dispatch_session_id) is not int
            or self.dispatch_session_id <= 0
            or type(self.native_request_id) is not int
            or not (1 <= self.native_request_id <= 2_147_483_647)
        ):
            raise ContractValidationError("managed CTP approval/session binding is incomplete")
        expected_session = {
            "session_generation_id": self.session_generation_id,
            "dispatch_front_id": self.dispatch_front_id,
            "dispatch_session_id": self.dispatch_session_id,
        }
        decoded_session = json.loads(session_binding)
        if "OrderActionRef" in json.loads(request_payload):
            raise ContractValidationError("prepared logical payload cannot supply OrderActionRef")
        if any(decoded_session.get(key) != value for key, value in expected_session.items()):
            raise ContractValidationError("managed CTP session binding echo differs")
        # Keep the staged inputs detached from mutable caller dictionaries.
        object.__setattr__(self, "request_payload", MappingProxyType(json.loads(request_payload)))
        object.__setattr__(self, "session_binding", MappingProxyType(decoded_session))


@dataclass(frozen=True, slots=True)
class CtpManagedDispatchBinding:
    """Lossless, credential-free binding echo for the same command row."""

    operation: Literal["submit", "cancel"]
    command_id: str
    account_key: str
    scope_key: str
    trading_day: str
    managed_intent_id: str
    runtime_order_id: str
    managed_action_id: str
    order_ref: str
    request_payload_sha256: str
    approval_use_id: str
    approval_digest: str
    session_binding_sha256: str
    session_generation_id: str
    dispatch_front_id: int
    dispatch_session_id: int
    native_request_id: int
    native_action_ref: int | None
    cancel_target_exchange_id: str | None
    cancel_target_order_sys_id: str | None
    cancel_target_front_id: int | None
    cancel_target_session_id: int | None
    local_queue_receipt_id: str
    local_queue_receipt_queued: bool | None
    request_payload: Mapping[str, Any]
    native_request_payload: Mapping[str, Any]
    native_request_payload_sha256: str
    session_binding: Mapping[str, Any]
    order_ref_reservation_created_at_ns: int

    def __post_init__(self) -> None:
        if self.operation not in {"submit", "cancel"}:
            raise ContractValidationError("invalid managed CTP binding operation")
        if (
            type(self.native_request_id) is not int
            or not 1 <= self.native_request_id <= 2_147_483_647
        ):
            raise ContractValidationError("invalid managed CTP binding RequestID")
        if self.operation == "cancel":
            if (
                type(self.native_action_ref) is not int
                or not 1 <= self.native_action_ref <= 2_147_483_647
            ):
                raise ContractValidationError("invalid Store-issued native ActionRef")
        elif self.native_action_ref is not None:
            raise ContractValidationError("submit binding cannot carry a native ActionRef")
        if not isinstance(self.request_payload, Mapping) or not isinstance(
            self.native_request_payload, Mapping
        ):
            raise ContractValidationError("managed CTP binding lacks native request payload")
        logical = dict(self.request_payload)
        native = dict(self.native_request_payload)
        if "OrderActionRef" in logical:
            raise ContractValidationError("logical managed payload contains native ActionRef")
        expected = dict(logical)
        if self.operation == "cancel":
            expected["OrderActionRef"] = self.native_action_ref
        if native != expected or not _HEX_64.fullmatch(self.native_request_payload_sha256):
            raise ContractValidationError("managed CTP native payload differs from Store binding")
        if payload_sha256(native) != self.native_request_payload_sha256:
            raise ContractValidationError("managed CTP native payload digest differs")

    @property
    def version(self) -> int:
        return 2

    @property
    def managed_cancel_intent_id(self) -> str | None:
        return self.managed_action_id if self.operation == "cancel" else None

    @property
    def cancel_target_order_ref(self) -> str | None:
        return self.order_ref if self.operation == "cancel" else None

    @classmethod
    def from_command(
        cls,
        command: CtpDispatchCommand,
        reservation: CtpOrderIdentityReservation,
    ) -> CtpManagedDispatchBinding:
        key = command.correlation_key
        if (
            key is None
            or key.version != 2
            or command.native_request_payload is None
            or command.native_request_payload_sha256 is None
            or command.local_queue_receipt_id is None
        ):
            raise ContractValidationError("managed CTP command binding is incomplete")
        operation: Literal["submit", "cancel"] = (
            "submit" if command.operation == "SUBMIT" else "cancel"
        )
        native_action_ref = key.native_action_ref
        if native_action_ref is not None and (
            isinstance(native_action_ref, bool) or not isinstance(native_action_ref, int)
        ):
            raise ContractValidationError("managed CTP command has an invalid native ActionRef")
        return cls(
            operation=operation,
            command_id=command.command_id,
            account_key=command.account_key,
            scope_key=command.scope_key,
            trading_day=command.trading_day,
            managed_intent_id=key.reservation_managed_intent_id,
            runtime_order_id=key.runtime_order_id,
            managed_action_id=key.managed_action_id,
            order_ref=key.order_ref,
            request_payload_sha256=command.request_payload_sha256,
            approval_use_id=command.approval_use_id,
            approval_digest=command.approval_digest,
            session_binding_sha256=command.session_binding_sha256,
            session_generation_id=key.session_generation_id,
            dispatch_front_id=key.dispatch_front_id,
            dispatch_session_id=key.dispatch_session_id,
            native_request_id=key.native_request_id,
            native_action_ref=native_action_ref,
            cancel_target_exchange_id=key.cancel_target_exchange_id,
            cancel_target_order_sys_id=key.cancel_target_order_sys_id,
            cancel_target_front_id=key.cancel_target_front_id,
            cancel_target_session_id=key.cancel_target_session_id,
            local_queue_receipt_id=command.local_queue_receipt_id,
            local_queue_receipt_queued=command.local_queue_receipt_queued,
            request_payload=MappingProxyType(dict(command.request_payload)),
            native_request_payload=MappingProxyType(dict(command.native_request_payload)),
            native_request_payload_sha256=command.native_request_payload_sha256,
            session_binding=MappingProxyType(dict(command.session_binding)),
            order_ref_reservation_created_at_ns=reservation.created_at_ns,
        )

    def to_payload(self) -> dict[str, Any]:
        """Return a complete structural echo; none of these fields authorize."""

        return {
            "version": self.version,
            "operation": self.operation,
            "command_id": self.command_id,
            "account_key": self.account_key,
            "scope_key": self.scope_key,
            "trading_day": self.trading_day,
            "managed_intent_id": self.managed_intent_id,
            "runtime_order_id": self.runtime_order_id,
            "managed_action_id": self.managed_action_id,
            "order_ref": self.order_ref,
            "cancel_target_order_ref": self.cancel_target_order_ref,
            "request_payload_sha256": self.request_payload_sha256,
            "native_request_payload_sha256": self.native_request_payload_sha256,
            "approval_use_id": self.approval_use_id,
            "approval_digest": self.approval_digest,
            "session_binding_sha256": self.session_binding_sha256,
            "session_generation_id": self.session_generation_id,
            "dispatch_front_id": self.dispatch_front_id,
            "dispatch_session_id": self.dispatch_session_id,
            "native_request_id": self.native_request_id,
            "native_action_ref": self.native_action_ref,
            "cancel_target_exchange_id": self.cancel_target_exchange_id,
            "cancel_target_order_sys_id": self.cancel_target_order_sys_id,
            "cancel_target_front_id": self.cancel_target_front_id,
            "cancel_target_session_id": self.cancel_target_session_id,
            "local_queue_receipt_id": self.local_queue_receipt_id,
            "local_queue_receipt_queued": self.local_queue_receipt_queued,
            "managed_cancel_intent_id": self.managed_cancel_intent_id,
            "request_payload": dict(self.request_payload),
            "native_request_payload": dict(self.native_request_payload),
            "session_binding": dict(self.session_binding),
            "order_ref_reservation_created_at_ns": self.order_ref_reservation_created_at_ns,
        }


@dataclass(frozen=True, slots=True)
class CtpNativeDispatchResult:
    """Fake-injectable local sender result; it is never a provider ACK.

    ``REJECTED`` is accepted as a sender report for compatibility with the
    fake boundary, but this candidate has no trusted proof verifier for
    no-send/no-callback claims. The worker therefore persists it as UNKNOWN.
    Only the pre-send durable queue receipt can establish a local rejection.
    """

    outcome: Literal["QUEUED", "REJECTED", "UNKNOWN"]
    receipt_payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.outcome not in {"QUEUED", "REJECTED", "UNKNOWN"}:
            raise ContractValidationError("invalid managed CTP sender outcome")
        if not isinstance(self.receipt_payload, Mapping) or not self.receipt_payload:
            raise ContractValidationError("managed CTP sender receipt must be a non-empty mapping")


class CtpNativeDispatchSender(Protocol):
    def __call__(
        self, command: CtpDispatchCommand
    ) -> CtpNativeDispatchResult | Awaitable[CtpNativeDispatchResult]:
        """Send one already-claimed command and return only local receipt data."""


def _binding_matches_command(
    binding: CtpManagedDispatchBinding,
    command: CtpDispatchCommand,
    reservation: CtpOrderIdentityReservation,
) -> bool:
    try:
        return binding == CtpManagedDispatchBinding.from_command(command, reservation)
    except ContractValidationError:
        return False


class CtpManagedSingleWorkerCandidate:
    """Candidate adapter intended to run inside one persistent host worker.

    It owns no thread and starts no provider. The host's sole worker calls
    ``dispatch_managed_command`` with the already-published command ID and its
    typed binding. The SQLite claim is the at-most-once boundary; a crash after
    claim remains CLAIMED until recovery fences it to UNKNOWN.
    """

    def __init__(
        self,
        store: SqliteExecutionStore,
        scope: ExecutionScope,
        writer_lease: WriterLease,
        authority_verifier: CtpDispatchAuthorityVerifier,
    ) -> None:
        self._store = store
        self._scope = scope
        self._writer_lease = writer_lease
        self._authority_verifier = authority_verifier

    def stage_prepared_dispatch(
        self,
        prepared: CtpManagedPreparedDispatch,
        *,
        cancel_target_projection: CtpOrderTargetProjectionHandle | None = None,
    ) -> CtpManagedDispatchBinding:
        if type(prepared) is not CtpManagedPreparedDispatch:
            raise ContractValidationError("typed managed CTP prepared dispatch is required")
        reservation = self._store.read_ctp_order_identity(self._scope, prepared.managed_intent_id)
        if (
            type(reservation) is not CtpOrderIdentityReservation
            or reservation != prepared.order_ref_reservation
            or reservation.runtime_order_id != prepared.runtime_order_id
            or reservation.order_ref != prepared.order_ref
        ):
            raise ContractValidationError(
                "prepared OrderRef does not match a committed same-store reservation"
            )
        if prepared.request_payload.get("OrderRef") != reservation.order_ref:
            raise ContractValidationError("prepared request OrderRef differs from reservation")
        cancel_target = None
        if prepared.operation == "cancel":
            cancel_target = CtpCancelTarget(
                order_ref=reservation.order_ref,
                exchange_id=prepared.cancel_target_exchange_id or "",
                order_sys_id=prepared.cancel_target_order_sys_id or "",
                front_id=prepared.cancel_target_front_id or 0,
                session_id=prepared.cancel_target_session_id or 0,
            )
        command = self._store.stage_ctp_dispatch_command(
            self._scope,
            prepared.command_id,
            prepared.operation.upper(),
            prepared.request_payload,
            approval_use_id=prepared.approval_use_id,
            approval_digest=prepared.approval_digest,
            session_binding=prepared.session_binding,
            writer_lease=self._writer_lease,
            managed_intent_id=(
                prepared.managed_intent_id if prepared.operation == "submit" else None
            ),
            order_ref=prepared.order_ref if prepared.operation == "submit" else None,
            cancel_target=cancel_target,
            managed_action_id=(
                prepared.managed_cancel_intent_id
                if prepared.operation == "cancel"
                else prepared.managed_intent_id
            ),
            session_generation_id=prepared.session_generation_id,
            dispatch_front_id=prepared.dispatch_front_id,
            dispatch_session_id=prepared.dispatch_session_id,
            native_request_id=prepared.native_request_id,
            local_queue_receipt_id=prepared.local_queue_receipt_id,
            cancel_target_projection=cancel_target_projection,
        )
        return CtpManagedDispatchBinding.from_command(command, reservation)

    def record_managed_queue_receipt(
        self,
        command_id: str,
        binding: CtpManagedDispatchBinding,
        queue_receipt: Mapping[str, Any],
    ) -> CtpManagedDispatchBinding:
        _, reservation = self._read_and_require_binding(command_id, binding)
        command = self._store.record_ctp_dispatch_queue_receipt(
            self._scope,
            command_id,
            queue_receipt,
            writer_lease=self._writer_lease,
        )
        return CtpManagedDispatchBinding.from_command(command, reservation)

    async def dispatch_managed_command(
        self,
        command_id: str,
        binding: CtpManagedDispatchBinding,
        sender: CtpNativeDispatchSender,
    ) -> CtpDispatchProjection:
        self._require_binding(command_id, binding)
        if not callable(sender):
            raise ContractValidationError("one managed CTP sender callable is required")
        command, _ = self._read_and_require_binding(command_id, binding)
        if command.status in {"COMPLETED", "UNKNOWN", "CLAIMED"}:
            projection = self._store.read_ctp_dispatch_projection(self._scope, command_id)
            if projection is None:
                raise ContractValidationError("managed CTP durable projection is missing")
            return projection
        if command.local_queue_receipt_queued is not True:
            raise ContractValidationError("managed CTP queue receipt is not committed as queued")

        claimed = self._store.claim_ctp_dispatch_command(
            self._scope,
            command_id,
            writer_lease=self._writer_lease,
            authority_verifier=self._authority_verifier,
            required_local_queue_receipt_id=binding.local_queue_receipt_id,
        )
        if claimed is None:
            projection = self._store.read_ctp_dispatch_projection(self._scope, command_id)
            if projection is None:
                raise ContractValidationError("managed CTP durable projection is missing")
            return projection

        try:
            if claimed.operation == "CANCEL":
                self._store.require_fresh_ctp_cancel_target_for_command(self._scope, command_id)
            result = sender(claimed)
            if inspect.isawaitable(result):
                result = await result
            if type(result) is not CtpNativeDispatchResult:
                raise ContractValidationError("managed CTP sender returned an untyped result")
            if result.outcome == "REJECTED":
                # Sender-provided data cannot prove that native send and every
                # callback were absent. There is no trusted no-send proof port
                # in this candidate, so a post-claim rejection is ambiguous.
                outcome = "UNKNOWN"
                receipt_payload = {"kind": "native_dispatch", "outcome": "UNKNOWN"}
            else:
                outcome = result.outcome
                receipt_payload = dict(result.receipt_payload)
                # A fake/native adapter may include an unsafe or non-canonical
                # payload. Treat it as ambiguous and persist only a fixed marker.
                canonical_json(receipt_payload)
                self._store._reject_sensitive_command_fields(receipt_payload)
        except Exception:
            # Deliberately discard exception text; it may contain credentials.
            outcome = "UNKNOWN"
            receipt_payload = {"kind": "native_dispatch", "outcome": "UNKNOWN"}

        receipt = CtpDispatchReceipt(
            receipt_type="ctp_dispatch_receipt.v2",
            command_id=claimed.command_id,
            account_key=claimed.account_key,
            scope_key=claimed.scope_key,
            trading_day=claimed.trading_day,
            operation=claimed.operation,
            request_payload_sha256=claimed.request_payload_sha256,
            reservation_managed_intent_id=claimed.reservation_managed_intent_id,
            order_ref=claimed.order_ref,
            cancel_target_order_ref=claimed.cancel_target_order_ref,
            cancel_target_exchange_id=claimed.cancel_target_exchange_id,
            cancel_target_order_sys_id=claimed.cancel_target_order_sys_id,
            cancel_target_front_id=claimed.cancel_target_front_id,
            cancel_target_session_id=claimed.cancel_target_session_id,
            approval_use_id=claimed.approval_use_id,
            approval_digest=claimed.approval_digest,
            session_binding_sha256=claimed.session_binding_sha256,
            outcome=outcome,
            native_receipt_payload=receipt_payload,
            correlation_key=claimed.correlation_key,
            local_queue_receipt_id=claimed.local_queue_receipt_id,
        )
        self._store.complete_ctp_dispatch_command(
            self._scope, receipt, writer_lease=self._writer_lease
        )
        projection = self._store.read_ctp_dispatch_projection(self._scope, command_id)
        if projection is None:
            raise ContractValidationError("managed CTP durable projection is missing")
        return projection

    def _require_binding(self, command_id: str, binding: CtpManagedDispatchBinding) -> None:
        if type(binding) is not CtpManagedDispatchBinding or binding.command_id != command_id:
            raise ContractValidationError("typed managed CTP binding does not match command id")
        if not _HEX_32.fullmatch(binding.local_queue_receipt_id):
            raise ContractValidationError("managed CTP queue receipt ID is invalid")

    def _read_and_require_binding(
        self, command_id: str, binding: CtpManagedDispatchBinding
    ) -> tuple[CtpDispatchCommand, CtpOrderIdentityReservation]:
        self._require_binding(command_id, binding)
        command = self._store.read_ctp_dispatch_command(self._scope, command_id)
        if type(command) is not CtpDispatchCommand:
            raise ContractValidationError("managed CTP command is missing")
        reservation = self._store.read_ctp_order_identity(self._scope, binding.managed_intent_id)
        if type(reservation) is not CtpOrderIdentityReservation or not _binding_matches_command(
            binding, command, reservation
        ):
            raise ContractValidationError("managed CTP binding differs from durable command")
        return command, reservation


__all__ = [
    "CtpManagedDispatchBinding",
    "CtpManagedPreparedDispatch",
    "CtpManagedSingleWorkerCandidate",
    "CtpNativeDispatchResult",
    "CtpNativeDispatchSender",
    "stable_managed_command_id",
]
