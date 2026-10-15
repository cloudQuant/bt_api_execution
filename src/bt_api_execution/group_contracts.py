"""Provider-neutral contracts for coordinator-authorized group order legs.

The group coordinator and account owner are separate authorities.  A
``GroupPermitBinding`` records the exact server-created group reservation,
actual child request, and immutable deadline that an account owner must
verify before dispatch.  Per-attempt coordinator and account writer epochs
live in ``GroupLegAuthorization``: they are auditable facts, but are not part
of the immutable business identity and can be refreshed only for an unclaimed
leg. Authentication and risk mapping remain injected server-side
responsibilities.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING, Protocol

from .contracts import OrderIntent, payload_sha256
from .errors import ContractValidationError

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from bt_api_base.service_protocol import WireIdentity, WireScope

    from .contracts import ExecutionScope
    from .store import WriterLease

_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GROUP_LEG_PREFIX = "group:"


def _reference(value: object, field_name: str) -> str:
    if type(value) is not str or not _REFERENCE.fullmatch(value):
        raise ContractValidationError(f"invalid {field_name}")
    return value


def _digest(value: object, field_name: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise ContractValidationError(f"invalid {field_name}")
    return value


def risk_account_scope_fingerprint(scope_key: str) -> str:
    """Hash Risk's canonical ``AccountScope.key`` without persisting its ID."""

    if type(scope_key) is not str or not scope_key or scope_key != scope_key.strip():
        raise ContractValidationError("invalid trusted Risk account scope key")
    return sha256(scope_key.encode("utf-8")).hexdigest()


def writer_lease_fingerprint(writer_lease: WriterLease) -> str:
    """Bind an authorization attempt to the exact local execution lease."""

    values = {
        "scope_key": getattr(writer_lease, "scope_key", None),
        "owner_id": getattr(writer_lease, "owner_id", None),
        "fencing_token": getattr(writer_lease, "fencing_token", None),
        "expires_at_ns": getattr(writer_lease, "expires_at_ns", None),
        "family_key": getattr(writer_lease, "family_key", None),
        "family_owner_intent_id": getattr(writer_lease, "family_owner_intent_id", None),
    }
    if (
        type(values["scope_key"]) is not str
        or type(values["owner_id"]) is not str
        or type(values["fencing_token"]) is not int
        or values["fencing_token"] <= 0
        or type(values["expires_at_ns"]) is not int
        or values["expires_at_ns"] <= 0
        or any(
            value is not None and type(value) is not str
            for value in (values["family_key"], values["family_owner_intent_id"])
        )
    ):
        raise ContractValidationError("invalid execution writer lease")
    return payload_sha256(values)


def group_leg_intent_id(group_id: str, leg_id: str) -> str:
    """Return the reserved execution-intent namespace for one group leg."""

    group_id = _reference(group_id, "group_id")
    leg_id = _reference(leg_id, "leg_id")
    value = f"{_GROUP_LEG_PREFIX}{group_id}:leg:{leg_id}"
    if len(value) > 128:
        raise ContractValidationError("group leg intent identity is too long")
    return value


def is_group_leg_intent_id(value: object) -> bool:
    """Return whether an intent id is reserved for the group-owner path."""

    return type(value) is str and value.startswith(_GROUP_LEG_PREFIX)


@dataclass(frozen=True, slots=True)
class GroupPermitBinding:
    """Immutable account-owner binding for one pre-reserved group child.

    ``reserved_intent_hash`` identifies the immutable maximum Risk reservation.
    ``actual_child_intent_hash`` identifies the mapper-produced Risk intent for
    this exact child quantity; the execution ``order_intent_hash`` is a third,
    distinct digest.  The account Risk adapter recomputes the actual Risk hash
    from the received order and compares it before asking Risk to claim.
    """

    group_id: str
    leg_id: str
    portfolio_ref: str
    strategy_id: str
    account_ref: str
    coordinator_principal_ref: str
    risk_scope_fingerprint: str
    permit_id: str
    group_fingerprint: str
    policy_fingerprint: str
    reserved_intent_hash: str
    exposure_profile_fingerprint: str
    actual_child_intent_hash: str
    order_intent_hash: str
    deadline_ns: int

    def __post_init__(self) -> None:
        for name in (
            "group_id",
            "leg_id",
            "portfolio_ref",
            "strategy_id",
            "account_ref",
            "coordinator_principal_ref",
            "permit_id",
        ):
            object.__setattr__(self, name, _reference(getattr(self, name), name))
        for name in (
            "group_fingerprint",
            "policy_fingerprint",
            "reserved_intent_hash",
            "exposure_profile_fingerprint",
            "actual_child_intent_hash",
            "order_intent_hash",
            "risk_scope_fingerprint",
        ):
            object.__setattr__(self, name, _digest(getattr(self, name), name))
        if type(self.deadline_ns) is not int or self.deadline_ns <= 0:
            raise ContractValidationError("invalid deadline_ns")

    def validate_order(self, intent: OrderIntent) -> None:
        """Bind this authorization to one exact execution intent and scope."""

        if type(intent) is not OrderIntent:
            raise ContractValidationError("group order intent is required")
        if (
            intent.intent_id != group_leg_intent_id(self.group_id, self.leg_id)
            or intent.scope.strategy_id != self.strategy_id
            or intent.scope.account_ref != self.account_ref
            or intent.deadline_ns != self.deadline_ns
            or intent.fingerprint != self.order_intent_hash
        ):
            raise ContractValidationError("group permit binding does not match the order intent")

    @property
    def fingerprint(self) -> str:
        return payload_sha256(self.to_payload())

    def to_payload(self) -> dict[str, object]:
        return {
            "group_id": self.group_id,
            "leg_id": self.leg_id,
            "portfolio_ref": self.portfolio_ref,
            "strategy_id": self.strategy_id,
            "account_ref": self.account_ref,
            "coordinator_principal_ref": self.coordinator_principal_ref,
            "risk_scope_fingerprint": self.risk_scope_fingerprint,
            "permit_id": self.permit_id,
            "group_fingerprint": self.group_fingerprint,
            "policy_fingerprint": self.policy_fingerprint,
            "reserved_intent_hash": self.reserved_intent_hash,
            "exposure_profile_fingerprint": self.exposure_profile_fingerprint,
            "actual_child_intent_hash": self.actual_child_intent_hash,
            "order_intent_hash": self.order_intent_hash,
            "deadline_ns": self.deadline_ns,
        }

    @classmethod
    def from_payload(cls, payload: object) -> GroupPermitBinding:
        if not isinstance(payload, dict) or set(payload) != {
            "group_id",
            "leg_id",
            "portfolio_ref",
            "strategy_id",
            "account_ref",
            "coordinator_principal_ref",
            "risk_scope_fingerprint",
            "permit_id",
            "group_fingerprint",
            "policy_fingerprint",
            "reserved_intent_hash",
            "exposure_profile_fingerprint",
            "actual_child_intent_hash",
            "order_intent_hash",
            "deadline_ns",
        }:
            raise ContractValidationError("invalid persisted group permit binding")
        try:
            return cls(**payload)
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid persisted group permit binding") from error


@dataclass(frozen=True, slots=True)
class GroupLegAuthorization:
    """Auditable current-fence authorization for one submit attempt."""

    authorization_attempt_id: str
    principal_ref: str
    portfolio_ref: str
    strategy_id: str
    account_ref: str
    group_id: str
    leg_id: str
    coordinator_epoch: int
    group_fingerprint: str
    binding_fingerprint: str
    deadline_ns: int
    account_writer_epoch: int
    risk_scope_fingerprint: str
    execution_writer_lease_fingerprint: str
    action: str

    def __post_init__(self) -> None:
        for name in (
            "principal_ref",
            "portfolio_ref",
            "strategy_id",
            "account_ref",
            "group_id",
            "leg_id",
            "action",
            "authorization_attempt_id",
        ):
            object.__setattr__(self, name, _reference(getattr(self, name), name))
        object.__setattr__(
            self, "group_fingerprint", _digest(self.group_fingerprint, "group_fingerprint")
        )
        object.__setattr__(
            self, "binding_fingerprint", _digest(self.binding_fingerprint, "binding_fingerprint")
        )
        object.__setattr__(
            self,
            "risk_scope_fingerprint",
            _digest(self.risk_scope_fingerprint, "risk_scope_fingerprint"),
        )
        object.__setattr__(
            self,
            "execution_writer_lease_fingerprint",
            _digest(
                self.execution_writer_lease_fingerprint,
                "execution_writer_lease_fingerprint",
            ),
        )
        if type(self.coordinator_epoch) is not int or self.coordinator_epoch <= 0:
            raise ContractValidationError("invalid coordinator_epoch")
        if type(self.account_writer_epoch) is not int or self.account_writer_epoch <= 0:
            raise ContractValidationError("invalid account_writer_epoch")
        if type(self.deadline_ns) is not int or self.deadline_ns <= 0:
            raise ContractValidationError("invalid deadline_ns")
        if self.action != "orders.submit_group_leg":
            raise ContractValidationError("invalid group coordinator action")

    def validate_binding(self, binding: GroupPermitBinding) -> None:
        if (
            type(binding) is not GroupPermitBinding
            or self.portfolio_ref != binding.portfolio_ref
            or self.strategy_id != binding.strategy_id
            or self.account_ref != binding.account_ref
            or self.group_id != binding.group_id
            or self.leg_id != binding.leg_id
            or self.group_fingerprint != binding.group_fingerprint
            or self.binding_fingerprint != binding.fingerprint
            or self.risk_scope_fingerprint != binding.risk_scope_fingerprint
            or self.deadline_ns != binding.deadline_ns
            or self.principal_ref != binding.coordinator_principal_ref
        ):
            raise ContractValidationError("group authorization does not match its binding")

    def validate_writer_lease(self, writer_lease: WriterLease) -> None:
        if self.execution_writer_lease_fingerprint != writer_lease_fingerprint(writer_lease):
            raise ContractValidationError("group authorization does not match the writer lease")

    @property
    def fingerprint(self) -> str:
        return payload_sha256(self.to_payload())

    def to_payload(self) -> dict[str, object]:
        return {
            "authorization_attempt_id": self.authorization_attempt_id,
            "principal_ref": self.principal_ref,
            "portfolio_ref": self.portfolio_ref,
            "strategy_id": self.strategy_id,
            "account_ref": self.account_ref,
            "group_id": self.group_id,
            "leg_id": self.leg_id,
            "coordinator_epoch": self.coordinator_epoch,
            "account_writer_epoch": self.account_writer_epoch,
            "group_fingerprint": self.group_fingerprint,
            "binding_fingerprint": self.binding_fingerprint,
            "deadline_ns": self.deadline_ns,
            "risk_scope_fingerprint": self.risk_scope_fingerprint,
            "execution_writer_lease_fingerprint": self.execution_writer_lease_fingerprint,
            "action": self.action,
        }

    @classmethod
    def from_payload(cls, payload: object) -> GroupLegAuthorization:
        if not isinstance(payload, dict) or set(payload) != {
            "authorization_attempt_id",
            "principal_ref",
            "portfolio_ref",
            "strategy_id",
            "account_ref",
            "group_id",
            "leg_id",
            "coordinator_epoch",
            "account_writer_epoch",
            "group_fingerprint",
            "binding_fingerprint",
            "deadline_ns",
            "risk_scope_fingerprint",
            "execution_writer_lease_fingerprint",
            "action",
        }:
            raise ContractValidationError("invalid persisted group authorization")
        try:
            return cls(**payload)
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid persisted group authorization") from error


class GroupCoordinatorAuthority(Protocol):
    """Trusted authority guard held continuously through one group dispatch.

    The guard is entered only after the facade acquires the actual Execution
    writer lease. The authority must validate the authenticated coordinator,
    delegation, current coordinator/account epochs, mapped Risk scope, exact
    binding and lease, and prevent those authority facts from changing until
    the context exits. User requests never supply the authority or its token.
    """

    def hold_group_leg_authorization(
        self,
        *,
        principal_ref: str,
        identity: WireIdentity,
        wire_scope: WireScope,
        execution_scope: ExecutionScope,
        intent: OrderIntent,
        binding: GroupPermitBinding,
        writer_lease: WriterLease,
    ) -> AbstractContextManager[GroupLegAuthorization]: ...


__all__ = [
    "GroupCoordinatorAuthority",
    "GroupLegAuthorization",
    "GroupPermitBinding",
    "group_leg_intent_id",
    "is_group_leg_intent_id",
    "risk_account_scope_fingerprint",
    "writer_lease_fingerprint",
]
