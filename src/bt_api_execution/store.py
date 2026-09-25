"""SQLite-backed durable state for the first public execution spine.

This store owns only execution facts: immutable intent identity, child progress,
the replayable outbox, and the execution writer lease.  It intentionally does
not contain a risk ledger or provider transport.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterator

from .contracts import (
    TERMINAL_STATES,
    CancelEvent,
    CancelIntent,
    CancelObservation,
    ExecutionEvent,
    ExecutionScope,
    ExecutionState,
    OrderIntent,
    ProviderObservation,
    can_transition,
    cancel_intent_from_payload,
    canonical_json,
    order_intent_from_payload,
    payload_sha256,
)
from .errors import (
    ContractValidationError,
    DurableStoreError,
    IntentConflictError,
    InvalidStateTransition,
    WriterLeaseUnavailable,
)


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def _is_local_queue_receipt_id(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 32
        and all(char in "0123456789abcdef" for char in value)
    )


def _is_prefixed_digest(value: Any, prefix: str) -> bool:
    return type(value) is str and value.startswith(prefix) and _is_sha256(value[len(prefix) :])


def _validate_correlation_text(value: Any, field_name: str) -> None:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > 128
        or not value.isascii()
        or any(not (char.isalnum() or char in "._:-") for char in value)
    ):
        raise ContractValidationError("invalid CTP dispatch " + field_name)


_CTP_ORDER_PROJECTION_STATES = frozenset(
    {"ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"}
)
_CTP_ORDER_TERMINAL_STATES = frozenset({"FILLED", "CANCELLED", "REJECTED"})
_CTP_CANCEL_ACTION_STATES = frozenset({"ACKNOWLEDGED", "REJECTED", "TERMINAL"})
_CTP_CANCEL_ACTION_TERMINAL_STATES = frozenset({"REJECTED", "TERMINAL"})


@dataclass(frozen=True, slots=True)
class WriterLease:
    """A scope-local writer lease with a monotonically increasing fence."""

    scope_key: str
    owner_id: str
    fencing_token: int
    expires_at_ns: int


@dataclass(frozen=True, slots=True)
class ExecutionRecord:
    """The durable execution projection for one immutable intent."""

    intent_id: str
    scope_key: str
    payload_sha256: str
    state: ExecutionState
    provider_order_id: str | None
    filled_quantity: Decimal
    average_price: Decimal | None
    permit_reference: str | None
    dispatch_attempts: int
    unknown_reason: str | None
    review_required: bool
    created_at_ns: int
    updated_at_ns: int
    # Kept last with a default so existing callers that construct the public
    # record positionally retain their v2 call shape.
    cumulative_commission: Decimal | None = None

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES


@dataclass(frozen=True, slots=True)
class CancelRecord:
    """The durable cancellation projection for one immutable cancel intent."""

    cancel_id: str
    scope_key: str
    target_intent_id: str
    provider_order_id: str
    payload_sha256: str
    state: ExecutionState
    permit_reference: str | None
    dispatch_attempts: int
    unknown_reason: str | None
    review_required: bool
    created_at_ns: int
    updated_at_ns: int

    @property
    def is_terminal(self) -> bool:
        """Return whether no later cancellation evidence may change this record."""

        return self.state in {
            ExecutionState.CANCELLED,
            ExecutionState.REJECTED,
            ExecutionState.BLOCKED,
        }


@dataclass(frozen=True, slots=True)
class CtpOrderIdentityReservation:
    """One account-wide, durable binding for a managed CTP order identity.

    ``order_ref`` is allocated by the same SQLite authority as the execution
    journal.  It is deliberately kept distinct from ``runtime_order_id``:
    the former is the native 12-digit CTP reference and the latter is the
    stable runtime identity supplied by the managed caller.
    """

    account_key: str
    trading_day: str
    scope_key: str
    managed_intent_id: str
    runtime_order_id: str
    order_ref: str
    created_at_ns: int


@dataclass(frozen=True, slots=True)
class CtpOrderRefLegacyMapping:
    """One exact mapping imported from a pre-cutover CTP identity source."""

    source_name: str
    account_key: str
    trading_day: str
    scope_key: str
    managed_intent_id: str
    runtime_order_id: str
    order_ref: str


@dataclass(frozen=True, slots=True)
class CtpOrderRefSeedProof:
    """Caller-supplied, session-bound inputs for an offline OrderRef cutover.

    The typed mapping inventory and separate source digests let the store
    import the exact legacy identities it was given. They do not prove the
    source files were complete, authenticate a native login observation, or
    establish an external account-wide writer fence. No registered runtime
    currently creates this proof.
    """

    trading_day: str
    native_max_order_ref: str
    legacy_ledger_max_order_ref: str
    legacy_ledger_sha256: str
    account_key: str
    scope_key: str
    session_generation_id: str
    native_front_id: int
    native_session_id: int
    existing_native_order_refs: tuple[str, ...]
    legacy_source_sha256: tuple[tuple[str, str], ...]
    legacy_mappings: tuple[CtpOrderRefLegacyMapping, ...]


@dataclass(frozen=True, slots=True)
class CtpDispatchCorrelationKey:
    """Versioned exact action/session identity carried by one CTP outbox row.

    This is a local correlation contract, not callback authenticity or provider
    state evidence. ``native_request_id`` and ``native_action_ref`` must be
    supplied to the eventual native adapter unchanged; no adapter is provided
    by this package.
    """

    version: int
    account_key: str
    scope_key: str
    trading_day: str
    operation: str
    command_id: str
    request_payload_sha256: str
    reservation_managed_intent_id: str
    managed_action_id: str
    runtime_order_id: str
    order_ref: str
    cancel_target_exchange_id: str | None
    cancel_target_order_sys_id: str | None
    cancel_target_front_id: int | None
    cancel_target_session_id: int | None
    approval_use_id: str
    approval_digest: str
    session_binding_sha256: str
    session_generation_id: str
    dispatch_front_id: int
    dispatch_session_id: int
    native_request_id: int
    native_action_ref: str | None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ContractValidationError("unsupported CTP dispatch correlation version")
        if type(self.operation) is not str or self.operation not in {"SUBMIT", "CANCEL"}:
            raise ContractValidationError("invalid CTP dispatch correlation operation")
        for value, name in (
            (self.command_id, "command_id"),
            (self.reservation_managed_intent_id, "managed intent id"),
            (self.managed_action_id, "managed action id"),
            (self.approval_use_id, "approval use id"),
            (self.session_generation_id, "session generation id"),
        ):
            _validate_correlation_text(value, name)
        if not _is_prefixed_digest(self.account_key, "account:"):
            raise ContractValidationError("invalid CTP dispatch correlation account key")
        if not _is_prefixed_digest(self.scope_key, "scope:"):
            raise ContractValidationError("invalid CTP dispatch correlation scope key")
        if (
            type(self.trading_day) is not str
            or len(self.trading_day) != 8
            or not self.trading_day.isascii()
            or not self.trading_day.isdigit()
        ):
            raise ContractValidationError("invalid CTP dispatch correlation trading day")
        for value, name in (
            (self.request_payload_sha256, "request payload digest"),
            (self.approval_digest, "approval digest"),
            (self.session_binding_sha256, "session binding digest"),
        ):
            if not _is_sha256(value):
                raise ContractValidationError("invalid CTP dispatch correlation " + name)
        if not self.runtime_order_id.startswith("bt-managed-v1:") or not _is_sha256(
            self.runtime_order_id.removeprefix("bt-managed-v1:")
        ):
            raise ContractValidationError("invalid CTP dispatch correlation runtime order id")
        if (
            type(self.order_ref) is not str
            or len(self.order_ref) != 12
            or not self.order_ref.isascii()
            or not self.order_ref.isdigit()
        ):
            raise ContractValidationError("invalid CTP dispatch correlation OrderRef")
        if type(self.dispatch_front_id) is not int or self.dispatch_front_id <= 0:
            raise ContractValidationError("invalid CTP dispatch correlation front id")
        if type(self.dispatch_session_id) is not int or self.dispatch_session_id <= 0:
            raise ContractValidationError("invalid CTP dispatch correlation session id")
        if (
            type(self.native_request_id) is not int
            or self.native_request_id <= 0
            or self.native_request_id > 2_147_483_647
        ):
            raise ContractValidationError("invalid CTP dispatch native request id")
        if self.native_action_ref is not None:
            _validate_correlation_text(self.native_action_ref, "native action reference")
        if self.operation == "SUBMIT":
            if self.managed_action_id != self.reservation_managed_intent_id:
                raise ContractValidationError("submit action id must equal its managed intent id")
            if self.native_action_ref is not None:
                raise ContractValidationError("submit correlation cannot carry a cancel ActionRef")
            if any(
                value is not None
                for value in (
                    self.cancel_target_exchange_id,
                    self.cancel_target_order_sys_id,
                    self.cancel_target_front_id,
                    self.cancel_target_session_id,
                )
            ):
                raise ContractValidationError("submit correlation cannot carry a cancel target")
        else:
            if self.managed_action_id == self.reservation_managed_intent_id:
                raise ContractValidationError(
                    "cancel action id must be distinct from target intent"
                )
            if (
                not isinstance(self.cancel_target_exchange_id, str)
                or not self.cancel_target_exchange_id
                or not isinstance(self.cancel_target_order_sys_id, str)
                or not self.cancel_target_order_sys_id
                or type(self.cancel_target_front_id) is not int
                or self.cancel_target_front_id <= 0
                or type(self.cancel_target_session_id) is not int
                or self.cancel_target_session_id <= 0
            ):
                raise ContractValidationError("cancel correlation requires its exact native target")
            _validate_correlation_text(self.cancel_target_exchange_id, "cancel target ExchangeID")
            _validate_correlation_text(self.cancel_target_order_sys_id, "cancel target OrderSysID")

    def to_payload(self) -> dict[str, Any]:
        """Return the exact, JSON-safe action/session binding."""

        return {
            "version": self.version,
            "account_key": self.account_key,
            "scope_key": self.scope_key,
            "trading_day": self.trading_day,
            "operation": self.operation,
            "command_id": self.command_id,
            "request_payload_sha256": self.request_payload_sha256,
            "reservation_managed_intent_id": self.reservation_managed_intent_id,
            "managed_action_id": self.managed_action_id,
            "runtime_order_id": self.runtime_order_id,
            "order_ref": self.order_ref,
            "cancel_target_exchange_id": self.cancel_target_exchange_id,
            "cancel_target_order_sys_id": self.cancel_target_order_sys_id,
            "cancel_target_front_id": self.cancel_target_front_id,
            "cancel_target_session_id": self.cancel_target_session_id,
            "approval_use_id": self.approval_use_id,
            "approval_digest": self.approval_digest,
            "session_binding_sha256": self.session_binding_sha256,
            "session_generation_id": self.session_generation_id,
            "dispatch_front_id": self.dispatch_front_id,
            "dispatch_session_id": self.dispatch_session_id,
            "native_request_id": self.native_request_id,
            "native_action_ref": self.native_action_ref,
        }


@dataclass(frozen=True, slots=True)
class CtpDispatchCallbackKey:
    """Provider-neutral callback correlation keys, untrusted without a verifier.

    Structural matching can reject mismatches but cannot authenticate source.
    The store persists this key only after an injected verifier returns an exact,
    fresh evidence binding; the default verifier rejects.
    """

    version: int
    correlation_key: CtpDispatchCorrelationKey
    callback_family: str
    stream_id: str
    event_id: str
    native_request_id: int
    native_action_ref: str | None
    order_ref: str
    exchange_id: str | None = None
    order_sys_id: str | None = None
    target_front_id: int | None = None
    target_session_id: int | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ContractValidationError("unsupported CTP callback key version")
        if type(self.correlation_key) is not CtpDispatchCorrelationKey:
            raise ContractValidationError("typed CTP dispatch correlation key is required")
        _validate_correlation_text(self.stream_id, "callback stream id")
        _validate_correlation_text(self.event_id, "callback event id")
        if type(self.callback_family) is not str or self.callback_family not in {
            "ORDER",
            "TRADE",
            "CANCEL_ACTION",
        }:
            raise ContractValidationError("invalid CTP callback family")
        if (
            self.correlation_key.operation == "SUBMIT"
            and self.callback_family not in {"ORDER", "TRADE"}
        ) or (
            self.correlation_key.operation == "CANCEL" and self.callback_family != "CANCEL_ACTION"
        ):
            raise ContractValidationError("CTP callback family does not match action")
        if (
            type(self.native_request_id) is not int
            or self.native_request_id <= 0
            or self.native_request_id > 2_147_483_647
        ):
            raise ContractValidationError("invalid CTP callback request id")
        if self.native_action_ref is not None:
            _validate_correlation_text(self.native_action_ref, "callback action reference")
        if (
            type(self.order_ref) is not str
            or len(self.order_ref) != 12
            or not self.order_ref.isascii()
            or not self.order_ref.isdigit()
        ):
            raise ContractValidationError("invalid CTP callback OrderRef")
        for value, name in (
            (self.exchange_id, "exchange id"),
            (self.order_sys_id, "system order id"),
        ):
            if value is not None:
                _validate_correlation_text(value, name)
        if (self.exchange_id is None) != (self.order_sys_id is None):
            raise ContractValidationError(
                "callback exchange and system order ids must appear together"
            )
        if self.correlation_key.operation == "CANCEL":
            if (
                self.exchange_id != self.correlation_key.cancel_target_exchange_id
                or self.order_sys_id != self.correlation_key.cancel_target_order_sys_id
                or type(self.target_front_id) is not int
                or type(self.target_session_id) is not int
                or self.target_front_id != self.correlation_key.cancel_target_front_id
                or self.target_session_id != self.correlation_key.cancel_target_session_id
            ):
                raise ContractValidationError("CTP cancel callback target does not match action")
        elif self.target_front_id is not None or self.target_session_id is not None:
            raise ContractValidationError("submit callback cannot carry a cancel target session")

    def to_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "correlation_key": self.correlation_key.to_payload(),
            "callback_family": self.callback_family,
            "stream_id": self.stream_id,
            "event_id": self.event_id,
            "native_request_id": self.native_request_id,
            "native_action_ref": self.native_action_ref,
            "order_ref": self.order_ref,
            "exchange_id": self.exchange_id,
            "order_sys_id": self.order_sys_id,
            "target_front_id": self.target_front_id,
            "target_session_id": self.target_session_id,
        }


@dataclass(frozen=True, slots=True)
class CtpVerifiedCallbackEvidence:
    """Injected verifier output for one exact callback event.

    A digest is only an evidence reference. The store accepts this value only
    as the return from an injected verifier; constructing it directly does not
    authenticate callback provenance.
    """

    evidence_type: str
    callback_key: CtpDispatchCallbackKey
    callback_payload_sha256: str
    projection_state: str
    source_digest_sha256: str
    verifier_id: str
    verified_at_ns: int
    expires_at_ns: int

    def __post_init__(self) -> None:
        if self.evidence_type != "ctp_verified_callback.v1":
            raise ContractValidationError("invalid verified CTP callback evidence type")
        if type(self.callback_key) is not CtpDispatchCallbackKey:
            raise ContractValidationError("typed CTP callback key is required")
        if not _is_sha256(self.callback_payload_sha256):
            raise ContractValidationError("invalid CTP callback payload digest")
        if type(self.projection_state) is not str:
            raise ContractValidationError("invalid CTP callback projection state")
        if self.callback_key.correlation_key.operation == "SUBMIT":
            if self.projection_state not in _CTP_ORDER_PROJECTION_STATES:
                raise ContractValidationError("invalid submit callback projection state")
        elif self.projection_state not in _CTP_CANCEL_ACTION_STATES:
            raise ContractValidationError("invalid cancel callback projection state")
        if not _is_sha256(self.source_digest_sha256):
            raise ContractValidationError("invalid CTP callback source digest")
        _validate_correlation_text(self.verifier_id, "callback verifier id")
        if (
            type(self.verified_at_ns) is not int
            or type(self.expires_at_ns) is not int
            or self.expires_at_ns <= self.verified_at_ns
        ):
            raise ContractValidationError("invalid CTP callback verification interval")


class CtpDispatchCallbackVerifier(Protocol):
    """Trusted source adapter; structural callback matching alone is not trust."""

    def verify_callback(
        self,
        command: CtpDispatchCommand,
        callback: CtpDispatchCallbackKey,
        callback_payload: Mapping[str, Any],
        *,
        now_ns: int,
    ) -> CtpVerifiedCallbackEvidence: ...


@dataclass(frozen=True, slots=True)
class CtpUnknownResolutionAttestation:
    """Fresh external reconciliation attestation for one UNKNOWN action.

    Submit resolution requires an exact terminal order state. Cancel resolution
    requires both a terminal cancel-action result and a terminal target-order
    state. Real adapters must attest their complete exact order/trade/position/
    cancel evidence set; the SDK stores only its digest and typed conclusion.
    """

    attestation_type: str
    correlation_key: CtpDispatchCorrelationKey
    order_terminal_state: str
    cancel_action_terminal_state: str | None
    source_digest_sha256: str
    verifier_id: str
    verified_at_ns: int
    expires_at_ns: int

    def __post_init__(self) -> None:
        if self.attestation_type != "ctp_unknown_resolution.v1":
            raise ContractValidationError("invalid CTP UNKNOWN resolution attestation type")
        if type(self.correlation_key) is not CtpDispatchCorrelationKey:
            raise ContractValidationError("typed CTP correlation key is required")
        if (
            type(self.order_terminal_state) is not str
            or self.order_terminal_state not in _CTP_ORDER_TERMINAL_STATES
        ):
            raise ContractValidationError("UNKNOWN resolution requires a terminal order state")
        if self.correlation_key.operation == "SUBMIT":
            if self.cancel_action_terminal_state is not None:
                raise ContractValidationError("submit resolution cannot contain cancel state")
        elif (
            type(self.cancel_action_terminal_state) is not str
            or self.cancel_action_terminal_state not in _CTP_CANCEL_ACTION_TERMINAL_STATES
        ):
            raise ContractValidationError(
                "cancel resolution requires a terminal cancel-action state"
            )
        if not _is_sha256(self.source_digest_sha256):
            raise ContractValidationError("invalid CTP reconciliation source digest")
        _validate_correlation_text(self.verifier_id, "reconciliation verifier id")
        if (
            type(self.verified_at_ns) is not int
            or type(self.expires_at_ns) is not int
            or self.expires_at_ns <= self.verified_at_ns
        ):
            raise ContractValidationError("invalid CTP reconciliation verification interval")


class CtpDispatchReconciliationVerifier(Protocol):
    """Trusted external evidence adapter required to resolve an UNKNOWN fence."""

    def verify_unknown(
        self, command: CtpDispatchCommand, *, now_ns: int
    ) -> CtpUnknownResolutionAttestation: ...


@dataclass(frozen=True, slots=True)
class CtpDispatchCallbackApplyResult:
    """Atomic callback-ledger/projection result; not a ProviderObservation."""

    callback_key: CtpDispatchCallbackKey
    projection_state: str
    duplicate: bool
    account_fence_open: bool


@dataclass(frozen=True, slots=True)
class CtpDispatchUnknownResolutionResult:
    """A durable fence resolution; the original outbox command stays UNKNOWN."""

    command_id: str
    correlation_key: CtpDispatchCorrelationKey
    order_terminal_state: str
    cancel_action_terminal_state: str | None
    account_fence_open: bool
    duplicate: bool


@dataclass(frozen=True, slots=True)
class CtpProjectedOrderState:
    """One committed provider-order projection, or an explicitly absent one."""

    provider_state: str | None = None
    terminal: bool | None = None
    source_kind: str | None = None
    updated_at_ns: int | None = None

    def __post_init__(self) -> None:
        if self.provider_state is None:
            if any(
                value is not None
                for value in (self.terminal, self.source_kind, self.updated_at_ns)
            ):
                raise ContractValidationError("absent CTP order projection has partial state")
            return
        if (
            type(self.provider_state) is not str
            or self.provider_state not in _CTP_ORDER_PROJECTION_STATES
        ):
            raise ContractValidationError("invalid projected CTP order state")
        expected_terminal = self.provider_state in _CTP_ORDER_TERMINAL_STATES
        if type(self.terminal) is not bool or self.terminal is not expected_terminal:
            raise ContractValidationError("invalid projected CTP order terminal flag")
        if self.source_kind not in {"CALLBACK", "RECONCILIATION"}:
            raise ContractValidationError("invalid projected CTP order source")
        if type(self.updated_at_ns) is not int or self.updated_at_ns <= 0:
            raise ContractValidationError("invalid projected CTP order timestamp")


@dataclass(frozen=True, slots=True)
class CtpSubmitActionProjection:
    """A submit command and its separate provider-order projection."""

    managed_intent_id: str
    runtime_order_id: str
    order_ref: str
    order_state: CtpProjectedOrderState

    def __post_init__(self) -> None:
        _validate_correlation_text(self.managed_intent_id, "managed intent id")
        if not _is_prefixed_digest(self.runtime_order_id, "bt-managed-v1:"):
            raise ContractValidationError("invalid projected CTP runtime order id")
        if (
            type(self.order_ref) is not str
            or len(self.order_ref) != 12
            or not self.order_ref.isascii()
            or not self.order_ref.isdigit()
        ):
            raise ContractValidationError("invalid projected CTP order reference")
        if type(self.order_state) is not CtpProjectedOrderState:
            raise ContractValidationError("typed CTP order projection state is required")


@dataclass(frozen=True, slots=True)
class CtpTargetOrderProjection:
    """The exact existing order targeted by a cancel action."""

    managed_intent_id: str
    runtime_order_id: str
    order_ref: str
    exchange_id: str
    order_sys_id: str
    front_id: int
    session_id: int
    order_state: CtpProjectedOrderState

    def __post_init__(self) -> None:
        _validate_correlation_text(self.managed_intent_id, "target managed intent id")
        if not _is_prefixed_digest(self.runtime_order_id, "bt-managed-v1:"):
            raise ContractValidationError("invalid projected CTP target runtime order id")
        if (
            type(self.order_ref) is not str
            or len(self.order_ref) != 12
            or not self.order_ref.isascii()
            or not self.order_ref.isdigit()
        ):
            raise ContractValidationError("invalid projected CTP target order reference")
        _validate_correlation_text(self.exchange_id, "target exchange id")
        _validate_correlation_text(self.order_sys_id, "target system order id")
        for value, name in ((self.front_id, "target front id"), (self.session_id, "target session id")):
            if type(value) is not int or value <= 0:
                raise ContractValidationError("invalid projected CTP " + name)
        if type(self.order_state) is not CtpProjectedOrderState:
            raise ContractValidationError("typed CTP target order state is required")


@dataclass(frozen=True, slots=True)
class CtpCancelActionProjection:
    """One cancellation action, kept distinct from its target-order state."""

    managed_action_id: str
    action_state: str | None
    terminal: bool | None
    source_kind: str | None
    updated_at_ns: int | None
    target_order: CtpTargetOrderProjection

    def __post_init__(self) -> None:
        _validate_correlation_text(self.managed_action_id, "managed action id")
        if self.action_state is None:
            if any(value is not None for value in (self.terminal, self.source_kind, self.updated_at_ns)):
                raise ContractValidationError("absent CTP cancel projection has partial state")
        else:
            if (
                type(self.action_state) is not str
                or self.action_state not in _CTP_CANCEL_ACTION_STATES
            ):
                raise ContractValidationError("invalid projected CTP cancel-action state")
            expected_terminal = self.action_state in _CTP_CANCEL_ACTION_TERMINAL_STATES
            if type(self.terminal) is not bool or self.terminal is not expected_terminal:
                raise ContractValidationError("invalid projected CTP cancel-action terminal flag")
            if self.source_kind not in {"CALLBACK", "RECONCILIATION"}:
                raise ContractValidationError("invalid projected CTP cancel-action source")
            if type(self.updated_at_ns) is not int or self.updated_at_ns <= 0:
                raise ContractValidationError("invalid projected CTP cancel-action timestamp")
        if type(self.target_order) is not CtpTargetOrderProjection:
            raise ContractValidationError("typed CTP target-order projection is required")
        if self.managed_action_id == self.target_order.managed_intent_id:
            raise ContractValidationError("CTP cancel action must differ from its target intent")


@dataclass(frozen=True, slots=True)
class CtpUnknownResolutionProjection:
    """Committed reconciliation conclusion for a command that remains UNKNOWN."""

    order_terminal_state: str
    cancel_action_terminal_state: str | None
    verified_at_ns: int
    resolved_at_ns: int

    def __post_init__(self) -> None:
        if (
            type(self.order_terminal_state) is not str
            or self.order_terminal_state not in _CTP_ORDER_TERMINAL_STATES
        ):
            raise ContractValidationError("invalid projected CTP UNKNOWN order resolution")
        if self.cancel_action_terminal_state is not None and (
            type(self.cancel_action_terminal_state) is not str
            or self.cancel_action_terminal_state not in _CTP_CANCEL_ACTION_TERMINAL_STATES
        ):
            raise ContractValidationError("invalid projected CTP UNKNOWN cancel resolution")
        for value, name in (
            (self.verified_at_ns, "verification timestamp"),
            (self.resolved_at_ns, "resolution timestamp"),
        ):
            if type(value) is not int or value <= 0:
                raise ContractValidationError("invalid CTP UNKNOWN " + name)
        if self.resolved_at_ns < self.verified_at_ns:
            raise ContractValidationError("CTP UNKNOWN resolution predates verification")


@dataclass(frozen=True, slots=True)
class CtpDispatchProjection:
    """Credential-free read model for one exact durable CTP dispatch command.

    ``command_status`` describes only local outbox dispatch. Provider state is
    exposed separately, and ``local_dispatch_outcome`` never implies a provider
    acknowledgement. An UNKNOWN command remains UNKNOWN after reconciliation.
    The optional action variants are absent for legacy commands without typed
    v8 correlation keys.
    """

    command_id: str
    operation: str
    command_status: str
    local_dispatch_outcome: str | None
    unknown_reason: str | None
    submit_action: CtpSubmitActionProjection | None
    cancel_action: CtpCancelActionProjection | None
    unknown_resolution: CtpUnknownResolutionProjection | None
    local_queue_receipt_id: str | None = None
    local_queue_receipt_queued: bool | None = None

    def __post_init__(self) -> None:
        _validate_correlation_text(self.command_id, "command id")
        if type(self.operation) is not str or self.operation not in {"SUBMIT", "CANCEL"}:
            raise ContractValidationError("invalid projected CTP operation")
        if type(self.command_status) is not str or self.command_status not in {
            "READY",
            "CLAIMED",
            "COMPLETED",
            "UNKNOWN",
        }:
            raise ContractValidationError("invalid projected CTP command status")
        if self.local_dispatch_outcome is not None and (
            type(self.local_dispatch_outcome) is not str
            or self.local_dispatch_outcome not in {"QUEUED", "REJECTED", "UNKNOWN"}
        ):
            raise ContractValidationError("invalid projected CTP local dispatch outcome")
        if self.command_status == "COMPLETED" and self.local_dispatch_outcome not in {
            "QUEUED",
            "REJECTED",
        }:
            raise ContractValidationError("completed CTP command lacks a local outcome")
        if self.command_status == "UNKNOWN" and self.local_dispatch_outcome != "UNKNOWN":
            raise ContractValidationError("unknown CTP command must retain UNKNOWN outcome")
        if self.command_status in {"READY", "CLAIMED"} and self.local_dispatch_outcome is not None:
            raise ContractValidationError("undispatched CTP command has a local outcome")
        if self.unknown_reason is not None and type(self.unknown_reason) is not str:
            raise ContractValidationError("invalid projected CTP UNKNOWN reason")
        if self.submit_action is not None and type(self.submit_action) is not CtpSubmitActionProjection:
            raise ContractValidationError("typed CTP submit-action projection is required")
        if self.cancel_action is not None and type(self.cancel_action) is not CtpCancelActionProjection:
            raise ContractValidationError("typed CTP cancel-action projection is required")
        if (
            self.unknown_resolution is not None
            and type(self.unknown_resolution) is not CtpUnknownResolutionProjection
        ):
            raise ContractValidationError("typed CTP UNKNOWN resolution is required")
        if self.operation == "SUBMIT" and self.cancel_action is not None:
            raise ContractValidationError("submit projection cannot contain a cancel action")
        if self.operation == "CANCEL" and self.submit_action is not None:
            raise ContractValidationError("cancel projection cannot contain a submit action")
        if self.unknown_resolution is not None and self.command_status != "UNKNOWN":
            raise ContractValidationError("resolved CTP command must retain UNKNOWN status")
        if self.unknown_resolution is not None and (
            (self.operation == "SUBMIT"
             and self.unknown_resolution.cancel_action_terminal_state is not None)
            or (self.operation == "CANCEL"
                and self.unknown_resolution.cancel_action_terminal_state is None)
        ):
            raise ContractValidationError("CTP UNKNOWN resolution does not match operation")
        if self.local_queue_receipt_id is not None and not _is_local_queue_receipt_id(
            self.local_queue_receipt_id
        ):
            raise ContractValidationError("invalid projected CTP local queue receipt id")
        if self.local_queue_receipt_queued is not None and type(
            self.local_queue_receipt_queued
        ) is not bool:
            raise ContractValidationError("invalid projected CTP queue disposition")
        if self.local_queue_receipt_id is None and self.local_queue_receipt_queued is not None:
            raise ContractValidationError("projected CTP queue disposition has no receipt id")
        if self.command_status in {"CLAIMED", "UNKNOWN"} and (
            self.local_queue_receipt_id is not None
            and self.local_queue_receipt_queued is not True
        ):
            raise ContractValidationError("claimed CTP projection lacks a committed queue receipt")
        if self.command_status == "COMPLETED" and self.local_queue_receipt_id is not None:
            if self.local_queue_receipt_queued is None:
                raise ContractValidationError("completed CTP projection lacks queue disposition")
            if self.local_queue_receipt_queued is True and self.local_dispatch_outcome not in {
                "QUEUED",
                "REJECTED",
            }:
                raise ContractValidationError("completed CTP queue projection has invalid outcome")
        if self.local_queue_receipt_queued is False and (
            self.command_status != "COMPLETED" or self.local_dispatch_outcome != "REJECTED"
        ):
            raise ContractValidationError("rejected CTP queue projection is not terminal")


class _RejectCtpDispatchVerifier:
    """Safe default for callback and reconciliation verification."""

    def verify_callback(
        self,
        command: CtpDispatchCommand,
        callback: CtpDispatchCallbackKey,
        callback_payload: Mapping[str, Any],
        *,
        now_ns: int,
    ) -> CtpVerifiedCallbackEvidence:
        raise ContractValidationError("trusted CTP callback verifier is required")

    def verify_unknown(
        self, command: CtpDispatchCommand, *, now_ns: int
    ) -> CtpUnknownResolutionAttestation:
        raise ContractValidationError("trusted CTP reconciliation verifier is required")


_REJECT_CTP_DISPATCH_VERIFIER = _RejectCtpDispatchVerifier()


@dataclass(frozen=True)
class CtpDispatchCommand:
    """Immutable staged request plus local dispatch state, not provider order state.

    ``COMPLETED`` means a typed local native-call receipt was persisted. It does
    not mean the provider accepted, filled, or cancelled the order.
    """

    account_key: str
    scope_key: str
    trading_day: str
    operation: str
    command_id: str
    request_payload: Mapping[str, Any]
    request_payload_sha256: str
    reservation_managed_intent_id: str
    order_ref: str | None
    cancel_target_order_ref: str | None
    cancel_target_exchange_id: str | None
    cancel_target_order_sys_id: str | None
    cancel_target_front_id: int | None
    cancel_target_session_id: int | None
    approval_use_id: str
    approval_digest: str
    session_binding: Mapping[str, Any]
    session_binding_sha256: str
    status: str
    created_at_ns: int
    updated_at_ns: int
    claimed_at_ns: int | None
    claimed_owner_id: str | None
    claimed_fencing_token: int | None
    completed_at_ns: int | None
    unknown_at_ns: int | None
    unknown_reason: str | None
    native_receipt_payload: Mapping[str, Any] | None
    native_receipt_sha256: str | None
    completion_echo_sha256: str | None
    correlation_key: CtpDispatchCorrelationKey | None = None
    local_queue_receipt_id: str | None = None
    local_queue_receipt_queued: bool | None = None

    @property
    def authority_binding_sha256(self) -> str:
        """Return the canonical digest of every immutable dispatch binding.

        This is an input to a trusted action verifier, not authorization by
        itself. In particular, the stored approval digest is only an echo.
        """

        return payload_sha256(
            {
                "binding_type": "ctp_dispatch_action_binding.v2",
                "account_key": self.account_key,
                "scope_key": self.scope_key,
                "trading_day": self.trading_day,
                "operation": self.operation,
                "command_id": self.command_id,
                "request_payload_sha256": self.request_payload_sha256,
                "reservation_managed_intent_id": self.reservation_managed_intent_id,
                "order_ref": self.order_ref,
                "cancel_target_order_ref": self.cancel_target_order_ref,
                "cancel_target_exchange_id": self.cancel_target_exchange_id,
                "cancel_target_order_sys_id": self.cancel_target_order_sys_id,
                "cancel_target_front_id": self.cancel_target_front_id,
                "cancel_target_session_id": self.cancel_target_session_id,
                "approval_use_id": self.approval_use_id,
                "approval_digest": self.approval_digest,
                "session_binding_sha256": self.session_binding_sha256,
                "local_queue_receipt_id": self.local_queue_receipt_id,
                "correlation_key": (
                    None if self.correlation_key is None else self.correlation_key.to_payload()
                ),
            }
        )


@dataclass(frozen=True, slots=True)
class CtpDispatchAuthority:
    """Fresh verifier output bound to exactly one staged CTP action.

    Constructing this record does not grant authority. The store accepts it
    only as the result of the mandatory verifier callback and consumes its
    approval use in the same transaction that claims the command.
    """

    authority_type: str
    command_binding_sha256: str
    approval_use_id: str
    approval_digest: str
    source_digest_sha256: str
    verifier_id: str
    verified_at_ns: int
    expires_at_ns: int


class CtpDispatchAuthorityVerifier(Protocol):
    """Trusted local verifier for one fresh approval and source snapshot.

    Implementations must re-verify current per-action approval and its exact
    source bindings on every invocation. They must be bounded and read-only;
    network access and native provider calls are outside this package's
    contract. The callback runs inside the SQLite claim transaction so its
    result cannot be separated from durable one-use consumption.
    """

    def verify_action(
        self, command: CtpDispatchCommand, *, now_ns: int
    ) -> CtpDispatchAuthority: ...


@dataclass(frozen=True)
class CtpDispatchReceipt:
    """Typed local queue receipt echoing every immutable command binding.

    ``native_receipt_payload`` is retained as evidence only. Outcomes describe
    local dispatch disposition and are not provider order or cancellation
    acknowledgements.
    """

    receipt_type: str
    command_id: str
    account_key: str
    scope_key: str
    trading_day: str
    operation: str
    request_payload_sha256: str
    reservation_managed_intent_id: str
    order_ref: str | None
    cancel_target_order_ref: str | None
    cancel_target_exchange_id: str | None
    cancel_target_order_sys_id: str | None
    cancel_target_front_id: int | None
    cancel_target_session_id: int | None
    approval_use_id: str
    approval_digest: str
    session_binding_sha256: str
    outcome: str
    native_receipt_payload: Mapping[str, Any]
    correlation_key: CtpDispatchCorrelationKey | None = None
    local_queue_receipt_id: str | None = None


def require_ctp_dispatch_callback_match(
    command: CtpDispatchCommand, callback: CtpDispatchCallbackKey
) -> CtpDispatchCallbackKey:
    """Require exact structural correlation without asserting callback trust.

    This helper is deliberately pure. It cannot authorize, persist, project, or
    resolve an outbox command, including a command already in ``UNKNOWN``.
    """

    if type(command) is not CtpDispatchCommand or command.correlation_key is None:
        raise ContractValidationError("versioned CTP dispatch correlation is required")
    if type(callback) is not CtpDispatchCallbackKey:
        raise ContractValidationError("typed CTP callback correlation key is required")
    if callback.correlation_key != command.correlation_key:
        raise ContractValidationError("CTP callback correlation does not match command")
    expected_order_ref = command.order_ref or command.cancel_target_order_ref
    if callback.order_ref != expected_order_ref:
        raise ContractValidationError("CTP callback OrderRef does not match command")
    if callback.native_request_id != command.correlation_key.native_request_id:
        raise ContractValidationError("CTP callback RequestID does not match command")
    if callback.native_action_ref != command.correlation_key.native_action_ref:
        raise ContractValidationError("CTP callback ActionRef does not match command")
    if command.operation == "CANCEL" and (
        callback.exchange_id != command.cancel_target_exchange_id
        or callback.order_sys_id != command.cancel_target_order_sys_id
        or callback.target_front_id != command.cancel_target_front_id
        or callback.target_session_id != command.cancel_target_session_id
    ):
        raise ContractValidationError("CTP cancel callback target does not match command")
    return callback


@dataclass(frozen=True)
class CtpCancelTarget:
    """Exact native identity required to stage a CTP cancel command."""

    order_ref: str
    exchange_id: str
    order_sys_id: str
    front_id: int
    session_id: int


_CANCEL_ALLOWED_TRANSITIONS: dict[ExecutionState, frozenset[ExecutionState]] = {
    ExecutionState.PENDING_ADMISSION: frozenset(
        {ExecutionState.PENDING_DISPATCH, ExecutionState.REJECTED, ExecutionState.BLOCKED}
    ),
    ExecutionState.PENDING_DISPATCH: frozenset(
        {ExecutionState.DISPATCHING, ExecutionState.BLOCKED}
    ),
    ExecutionState.DISPATCHING: frozenset(
        {
            ExecutionState.ACKED,
            ExecutionState.CANCELLED,
            ExecutionState.REJECTED,
            ExecutionState.BLOCKED,
            ExecutionState.UNKNOWN,
        }
    ),
    ExecutionState.ACKED: frozenset(
        {ExecutionState.CANCELLED, ExecutionState.REJECTED, ExecutionState.UNKNOWN}
    ),
    ExecutionState.UNKNOWN: frozenset(
        {ExecutionState.ACKED, ExecutionState.CANCELLED, ExecutionState.REJECTED}
    ),
    ExecutionState.CANCELLED: frozenset(),
    ExecutionState.REJECTED: frozenset(),
    ExecutionState.BLOCKED: frozenset(),
    ExecutionState.PARTIALLY_FILLED: frozenset(),
    ExecutionState.FILLED: frozenset(),
}


def _can_cancel_transition(source: ExecutionState, target: ExecutionState) -> bool:
    return target in _CANCEL_ALLOWED_TRANSITIONS[source]


class SqliteExecutionStore:
    """One local SQLite execution authority with a durable event outbox.

    SQLite's transaction is deliberately bounded to local facts.  A provider
    call always occurs after ``claim_for_dispatch`` commits, so a crash after a
    potentially sent request is represented as ``UNKNOWN`` rather than retried.
    """

    # Version 2 makes intent identity scope-qualified. Version 3 adds observed
    # cumulative commission. Version 4 adds CTP order identity reservations.
    # Version 5 adds an offline-only command queue in this same authority; the
    # generic event outbox remains an event log, not a command source.
    # Version 6 records one-use action authority atomically with each claim.
    # Version 7 binds typed runtime/action/session/request identities to commands.
    # Version 8 adds an injected-source callback ledger, separate projections,
    # and an external-reconciliation-only UNKNOWN fence resolution record.
    # Version 9 adds an account-wide allocated OrderRef watermark and exact,
    # session-bound cutover evidence with imported legacy identity mappings.
    # Version 10 binds a prepublished local queue receipt to the same command
    # row and gates its unique worker claim on that receipt being queued.
    # Version 11 persisted per-event callback ingestion guards. Version 12
    # replaces them with a permanent account source-lifecycle fence.
    _SCHEMA_VERSION = 12

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 5_000) -> None:
        if type(busy_timeout_ms) is not int or busy_timeout_ms <= 0:
            raise ContractValidationError("invalid busy_timeout_ms")
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        try:
            self._connection = sqlite3.connect(
                self.path,
                isolation_level=None,
                check_same_thread=False,
            )
            self._connection.row_factory = sqlite3.Row
            self._connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = FULL")
            self._lock = RLock()
            self._create_schema()
        except sqlite3.Error as error:
            raise DurableStoreError("unable to initialize execution store") from error

    def close(self) -> None:
        """Close the SQLite connection; no background thread is owned here."""

        with self._lock:
            self._connection.close()

    def _migrate_legacy_ctp_callback_source_lifecycle_fences(
        self, cursor: sqlite3.Cursor
    ) -> None:
        """Fail closed while migrating callback facts from pre-v12 schemas."""

        table_names = {
            str(row["name"])
            for row in cursor.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        fenced_accounts = {
            str(row["account_key"])
            for row in cursor.execute(
                "SELECT account_key FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchall()
        }

        legacy_guard_table = "ctp_dispatch_callback_ingestion_guards"
        if legacy_guard_table in table_names:
            columns = {
                str(row["name"])
                for row in cursor.execute(
                    "PRAGMA table_info(ctp_dispatch_callback_ingestion_guards)"
                ).fetchall()
            }
            required = {
                "account_key",
                "scope_key",
                "command_id",
                "guard_id",
                "correlation_key_sha256",
                "session_binding_sha256",
                "created_at_ns",
            }
            if not required.issubset(columns):
                raise DurableStoreError(
                    "legacy CTP callback lifecycle fence cannot be migrated"
                )
            legacy_guards = cursor.execute(
                """
                SELECT account_key, scope_key, command_id, guard_id,
                       correlation_key_sha256, session_binding_sha256
                FROM ctp_dispatch_callback_ingestion_guards
                ORDER BY account_key, created_at_ns, command_id, guard_id
                """
            ).fetchall()
            for old in legacy_guards:
                account_key = str(old["account_key"])
                command_id = str(old["command_id"])
                command_row = cursor.execute(
                    """
                    SELECT * FROM ctp_dispatch_commands
                    WHERE account_key = ? AND command_id = ?
                    """,
                    (account_key, command_id),
                ).fetchone()
                if command_row is None:
                    raise DurableStoreError(
                        "legacy CTP callback lifecycle fence has no command"
                    )
                command = self._ctp_dispatch_command_from_row(command_row)
                if (
                    command.correlation_key is None
                    or not _is_local_queue_receipt_id(str(old["guard_id"]))
                    or not _is_sha256(str(old["correlation_key_sha256"]))
                    or not _is_sha256(str(old["session_binding_sha256"]))
                    or command.scope_key != str(old["scope_key"])
                    or command.session_binding_sha256
                    != str(old["session_binding_sha256"])
                    or payload_sha256(command.correlation_key.to_payload())
                    != str(old["correlation_key_sha256"])
                ):
                    raise DurableStoreError(
                        "legacy CTP callback lifecycle fence binding is inconsistent"
                    )
                if account_key not in fenced_accounts:
                    cursor.execute(
                        """
                        INSERT INTO ctp_dispatch_callback_source_lifecycle_fences(
                            account_key, scope_key, command_id, source_lifecycle_fence_id,
                            correlation_key_sha256, session_binding_sha256, created_at_ns
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            account_key,
                            command.scope_key,
                            command_id,
                            str(old["guard_id"]),
                            str(old["correlation_key_sha256"]),
                            str(old["session_binding_sha256"]),
                            time.time_ns(),
                        ),
                    )
                    fenced_accounts.add(account_key)

        # A callback ledger row proves the old source consumer may already
        # have consumed native events even when no legacy guard was persisted.
        # Preserve that uncertainty instead of reopening claims after upgrade.
        legacy_callbacks = cursor.execute(
            """
            SELECT account_key, command_id, correlation_key_sha256
            FROM ctp_dispatch_callback_ledger
            ORDER BY account_key, applied_at_ns, command_id
            """
        ).fetchall()
        for old in legacy_callbacks:
            account_key = str(old["account_key"])
            command_id = str(old["command_id"])
            command_row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND command_id = ?
                """,
                (account_key, command_id),
            ).fetchone()
            if command_row is None:
                raise DurableStoreError("legacy CTP callback ledger has no command")
            command = self._ctp_dispatch_command_from_row(command_row)
            if (
                command.correlation_key is None
                or not _is_sha256(str(old["correlation_key_sha256"]))
                or payload_sha256(command.correlation_key.to_payload())
                != str(old["correlation_key_sha256"])
            ):
                raise DurableStoreError(
                    "legacy CTP callback ledger binding is inconsistent"
                )
            if account_key in fenced_accounts:
                continue
            cursor.execute(
                """
                INSERT INTO ctp_dispatch_callback_source_lifecycle_fences(
                    account_key, scope_key, command_id, source_lifecycle_fence_id,
                    correlation_key_sha256, session_binding_sha256, created_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_key,
                    command.scope_key,
                    command_id,
                    uuid.uuid4().hex,
                    str(old["correlation_key_sha256"]),
                    command.session_binding_sha256,
                    time.time_ns(),
                ),
            )
            fenced_accounts.add(account_key)

    def _create_schema(self) -> None:
        with self._transaction() as cursor:
            self._execute_schema_statements(
                cursor,
                """
                CREATE TABLE IF NOT EXISTS execution_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS execution_records (
                    scope_key TEXT NOT NULL,
                    intent_id TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    provider_order_id TEXT,
                    filled_quantity TEXT NOT NULL,
                    average_price TEXT,
                    cumulative_commission TEXT,
                    permit_reference TEXT,
                    dispatch_attempts INTEGER NOT NULL DEFAULT 0,
                    unknown_reason TEXT,
                    review_required INTEGER NOT NULL DEFAULT 0,
                    created_at_ns INTEGER NOT NULL,
                    updated_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(scope_key, intent_id)
                );
                CREATE TABLE IF NOT EXISTS execution_outbox (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    intent_id TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    state TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at_ns INTEGER NOT NULL,
                    FOREIGN KEY(scope_key, intent_id)
                        REFERENCES execution_records(scope_key, intent_id)
                );
                CREATE INDEX IF NOT EXISTS execution_outbox_intent_sequence
                    ON execution_outbox(scope_key, intent_id, sequence);
                CREATE TABLE IF NOT EXISTS cancellation_records (
                    scope_key TEXT NOT NULL,
                    cancel_id TEXT NOT NULL,
                    target_intent_id TEXT NOT NULL,
                    provider_order_id TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    permit_reference TEXT,
                    dispatch_attempts INTEGER NOT NULL DEFAULT 0,
                    unknown_reason TEXT,
                    review_required INTEGER NOT NULL DEFAULT 0,
                    created_at_ns INTEGER NOT NULL,
                    updated_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(scope_key, cancel_id),
                    FOREIGN KEY(scope_key, target_intent_id)
                        REFERENCES execution_records(scope_key, intent_id)
                );
                CREATE INDEX IF NOT EXISTS cancellation_records_target
                    ON cancellation_records(scope_key, target_intent_id);
                CREATE TABLE IF NOT EXISTS cancellation_outbox (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    cancel_id TEXT NOT NULL,
                    target_intent_id TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    state TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at_ns INTEGER NOT NULL,
                    FOREIGN KEY(scope_key, cancel_id)
                        REFERENCES cancellation_records(scope_key, cancel_id)
                );
                CREATE INDEX IF NOT EXISTS cancellation_outbox_cancel_sequence
                    ON cancellation_outbox(scope_key, cancel_id, sequence);
                CREATE TABLE IF NOT EXISTS execution_writer_leases (
                    scope_key TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    expires_at_ns INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ctp_order_identity_reservations (
                    account_key TEXT NOT NULL,
                    trading_day TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    managed_intent_id TEXT NOT NULL,
                    runtime_order_id TEXT NOT NULL,
                    order_ref TEXT NOT NULL
                        CHECK(length(order_ref) = 12 AND order_ref NOT GLOB '*[^0-9]*'),
                    created_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, scope_key, managed_intent_id),
                    UNIQUE(account_key, runtime_order_id),
                    UNIQUE(account_key, order_ref)
                );
                CREATE INDEX IF NOT EXISTS ctp_order_identity_scope_day
                    ON ctp_order_identity_reservations(account_key, trading_day, scope_key);
                CREATE TABLE IF NOT EXISTS ctp_order_ref_watermarks (
                    account_key TEXT NOT NULL,
                    trading_day TEXT NOT NULL,
                    native_max_order_ref TEXT NOT NULL
                        CHECK(length(native_max_order_ref) = 12
                              AND native_max_order_ref NOT GLOB '*[^0-9]*'),
                    legacy_ledger_max_order_ref TEXT NOT NULL
                        CHECK(length(legacy_ledger_max_order_ref) = 12
                              AND legacy_ledger_max_order_ref NOT GLOB '*[^0-9]*'),
                    legacy_ledger_sha256 TEXT NOT NULL
                        CHECK(length(legacy_ledger_sha256) = 64),
                    updated_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, trading_day)
                );
                CREATE TABLE IF NOT EXISTS ctp_order_ref_account_watermarks (
                    account_key TEXT PRIMARY KEY,
                    watermark_order_ref TEXT NOT NULL
                        CHECK(length(watermark_order_ref) = 12
                              AND watermark_order_ref NOT GLOB '*[^0-9]*'),
                    cutover_established INTEGER NOT NULL CHECK(cutover_established IN (0, 1)),
                    last_trading_day TEXT,
                    last_cutover_evidence_sha256 TEXT
                        CHECK(last_cutover_evidence_sha256 IS NULL
                              OR length(last_cutover_evidence_sha256) = 64),
                    updated_at_ns INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ctp_order_ref_cutover_sessions (
                    account_key TEXT NOT NULL,
                    trading_day TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    session_generation_id TEXT NOT NULL,
                    native_front_id INTEGER NOT NULL CHECK(native_front_id > 0),
                    native_session_id INTEGER NOT NULL CHECK(native_session_id > 0),
                    native_max_order_ref TEXT NOT NULL
                        CHECK(length(native_max_order_ref) = 12
                              AND native_max_order_ref NOT GLOB '*[^0-9]*'),
                    native_refs_sha256 TEXT NOT NULL CHECK(length(native_refs_sha256) = 64),
                    legacy_ledger_max_order_ref TEXT NOT NULL
                        CHECK(length(legacy_ledger_max_order_ref) = 12
                              AND legacy_ledger_max_order_ref NOT GLOB '*[^0-9]*'),
                    backtrader_prototype_sha256 TEXT NOT NULL
                        CHECK(length(backtrader_prototype_sha256) = 64),
                    sdk_jsonl_sha256 TEXT NOT NULL CHECK(length(sdk_jsonl_sha256) = 64),
                    legacy_ledger_sha256 TEXT NOT NULL CHECK(length(legacy_ledger_sha256) = 64),
                    legacy_mappings_sha256 TEXT NOT NULL
                        CHECK(length(legacy_mappings_sha256) = 64),
                    evidence_sha256 TEXT NOT NULL CHECK(length(evidence_sha256) = 64),
                    created_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, trading_day, scope_key, session_generation_id)
                );
                CREATE TABLE IF NOT EXISTS ctp_order_ref_legacy_imports (
                    account_key TEXT NOT NULL,
                    source_name TEXT NOT NULL
                        CHECK(source_name IN ('backtrader_prototype', 'sdk_jsonl')),
                    order_ref TEXT NOT NULL
                        CHECK(length(order_ref) = 12 AND order_ref NOT GLOB '*[^0-9]*'),
                    trading_day TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    managed_intent_id TEXT NOT NULL,
                    runtime_order_id TEXT NOT NULL,
                    mapping_sha256 TEXT NOT NULL CHECK(length(mapping_sha256) = 64),
                    imported_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, source_name, order_ref),
                    FOREIGN KEY(account_key, order_ref)
                        REFERENCES ctp_order_identity_reservations(account_key, order_ref)
                );
                CREATE TRIGGER IF NOT EXISTS ctp_order_ref_cutover_immutable_update
                BEFORE UPDATE ON ctp_order_ref_cutover_sessions
                BEGIN
                    SELECT RAISE(ABORT, 'CTP OrderRef cutover evidence is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_order_ref_cutover_immutable_delete
                BEFORE DELETE ON ctp_order_ref_cutover_sessions
                BEGIN
                    SELECT RAISE(ABORT, 'CTP OrderRef cutover evidence is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_order_ref_legacy_import_immutable_update
                BEFORE UPDATE ON ctp_order_ref_legacy_imports
                BEGIN
                    SELECT RAISE(ABORT, 'CTP legacy OrderRef import is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_order_ref_legacy_import_immutable_delete
                BEFORE DELETE ON ctp_order_ref_legacy_imports
                BEGIN
                    SELECT RAISE(ABORT, 'CTP legacy OrderRef import is immutable');
                END;
                CREATE TABLE IF NOT EXISTS ctp_dispatch_commands (
                    account_key TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    trading_day TEXT NOT NULL,
                    operation TEXT NOT NULL CHECK(operation IN ('SUBMIT', 'CANCEL')),
                    command_id TEXT NOT NULL,
                    request_payload_json TEXT NOT NULL,
                    request_payload_sha256 TEXT NOT NULL CHECK(length(request_payload_sha256) = 64),
                    reservation_managed_intent_id TEXT NOT NULL,
                    order_ref TEXT,
                    cancel_target_order_ref TEXT,
                    cancel_target_exchange_id TEXT,
                    cancel_target_order_sys_id TEXT,
                    cancel_target_front_id INTEGER,
                    cancel_target_session_id INTEGER,
                    approval_use_id TEXT NOT NULL,
                    approval_digest TEXT NOT NULL CHECK(length(approval_digest) = 64),
                    session_binding_json TEXT NOT NULL,
                    session_binding_sha256 TEXT NOT NULL CHECK(length(session_binding_sha256) = 64),
                    correlation_version INTEGER NOT NULL DEFAULT 0,
                    runtime_order_id TEXT,
                    managed_action_id TEXT,
                    session_generation_id TEXT,
                    dispatch_front_id INTEGER,
                    dispatch_session_id INTEGER,
                    native_request_id INTEGER,
                    native_action_ref TEXT,
                    local_queue_receipt_id TEXT
                        CHECK(local_queue_receipt_id IS NULL OR
                              (length(local_queue_receipt_id) = 32 AND
                               local_queue_receipt_id NOT GLOB '*[^0-9a-f]*')),
                    local_queue_receipt_queued INTEGER
                        CHECK(local_queue_receipt_queued IS NULL
                              OR local_queue_receipt_queued IN (0, 1)),
                    status TEXT NOT NULL CHECK(status IN ('READY', 'CLAIMED', 'COMPLETED', 'UNKNOWN')),
                    created_at_ns INTEGER NOT NULL,
                    updated_at_ns INTEGER NOT NULL,
                    claimed_at_ns INTEGER,
                    claimed_owner_id TEXT,
                    claimed_fencing_token INTEGER,
                    completed_at_ns INTEGER,
                    unknown_at_ns INTEGER,
                    unknown_reason TEXT,
                    native_receipt_payload_json TEXT,
                    native_receipt_sha256 TEXT,
                    completion_echo_json TEXT,
                    completion_echo_sha256 TEXT,
                    PRIMARY KEY(account_key, command_id),
                    UNIQUE(account_key, approval_use_id),
                    CHECK((operation = 'SUBMIT' AND order_ref IS NOT NULL
                           AND cancel_target_order_ref IS NULL
                           AND cancel_target_exchange_id IS NULL
                           AND cancel_target_order_sys_id IS NULL
                           AND cancel_target_front_id IS NULL
                           AND cancel_target_session_id IS NULL)
                       OR (operation = 'CANCEL' AND order_ref IS NULL
                           AND cancel_target_order_ref IS NOT NULL
                           AND cancel_target_exchange_id IS NOT NULL
                           AND cancel_target_order_sys_id IS NOT NULL
                           AND cancel_target_front_id IS NOT NULL
                           AND cancel_target_session_id IS NOT NULL)),
                    FOREIGN KEY(account_key, scope_key, reservation_managed_intent_id)
                        REFERENCES ctp_order_identity_reservations(
                            account_key, scope_key, managed_intent_id
                        )
                );
                CREATE INDEX IF NOT EXISTS ctp_dispatch_commands_scope_status
                    ON ctp_dispatch_commands(account_key, scope_key, status, created_at_ns);
                CREATE INDEX IF NOT EXISTS ctp_dispatch_commands_account_status
                    ON ctp_dispatch_commands(account_key, status, created_at_ns);
                CREATE TABLE IF NOT EXISTS ctp_dispatch_callback_ledger (
                    account_key TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    trading_day TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    operation TEXT NOT NULL CHECK(operation IN ('SUBMIT', 'CANCEL')),
                    managed_action_id TEXT NOT NULL,
                    runtime_order_id TEXT NOT NULL,
                    order_ref TEXT NOT NULL,
                    session_generation_id TEXT NOT NULL,
                    callback_stream_id TEXT NOT NULL,
                    callback_event_id TEXT NOT NULL,
                    callback_family TEXT NOT NULL
                        CHECK(callback_family IN ('ORDER', 'TRADE', 'CANCEL_ACTION')),
                    callback_key_json TEXT NOT NULL,
                    callback_key_sha256 TEXT NOT NULL CHECK(length(callback_key_sha256) = 64),
                    callback_payload_sha256 TEXT NOT NULL
                        CHECK(length(callback_payload_sha256) = 64),
                    correlation_key_sha256 TEXT NOT NULL
                        CHECK(length(correlation_key_sha256) = 64),
                    projection_state TEXT NOT NULL,
                    source_digest_sha256 TEXT NOT NULL
                        CHECK(length(source_digest_sha256) = 64),
                    verifier_id TEXT NOT NULL,
                    verified_at_ns INTEGER NOT NULL,
                    applied_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(
                        account_key, session_generation_id,
                        callback_stream_id, callback_event_id
                    ),
                    FOREIGN KEY(account_key, command_id)
                        REFERENCES ctp_dispatch_commands(account_key, command_id)
                );
                CREATE INDEX IF NOT EXISTS ctp_dispatch_callback_command
                    ON ctp_dispatch_callback_ledger(account_key, command_id, applied_at_ns);
                CREATE TABLE IF NOT EXISTS ctp_dispatch_callback_source_lifecycle_fences (
                    account_key TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    source_lifecycle_fence_id TEXT NOT NULL
                        CHECK(length(source_lifecycle_fence_id) = 32
                              AND source_lifecycle_fence_id NOT GLOB '*[^0-9a-f]*'),
                    correlation_key_sha256 TEXT NOT NULL
                        CHECK(length(correlation_key_sha256) = 64),
                    session_binding_sha256 TEXT NOT NULL
                        CHECK(length(session_binding_sha256) = 64),
                    created_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, command_id, source_lifecycle_fence_id),
                    FOREIGN KEY(account_key, command_id)
                        REFERENCES ctp_dispatch_commands(account_key, command_id)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ctp_dispatch_callback_source_lifecycle_account
                    ON ctp_dispatch_callback_source_lifecycle_fences(account_key);
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_callback_source_lifecycle_immutable_update
                BEFORE UPDATE ON ctp_dispatch_callback_source_lifecycle_fences
                BEGIN
                    SELECT RAISE(ABORT, 'CTP callback source lifecycle fence is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_callback_source_lifecycle_immutable_delete
                BEFORE DELETE ON ctp_dispatch_callback_source_lifecycle_fences
                BEGIN
                    SELECT RAISE(ABORT, 'CTP callback source lifecycle fence is immutable');
                END;
                CREATE TABLE IF NOT EXISTS ctp_dispatch_order_projection (
                    account_key TEXT NOT NULL,
                    runtime_order_id TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    trading_day TEXT NOT NULL,
                    managed_intent_id TEXT NOT NULL,
                    order_ref TEXT NOT NULL,
                    provider_state TEXT NOT NULL CHECK(provider_state IN (
                        'ACKNOWLEDGED', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED'
                    )),
                    terminal INTEGER NOT NULL CHECK(terminal IN (0, 1)),
                    correlation_key_sha256 TEXT NOT NULL
                        CHECK(length(correlation_key_sha256) = 64),
                    source_digest_sha256 TEXT NOT NULL
                        CHECK(length(source_digest_sha256) = 64),
                    last_source_kind TEXT NOT NULL
                        CHECK(last_source_kind IN ('CALLBACK', 'RECONCILIATION')),
                    last_event_generation_id TEXT,
                    last_event_stream_id TEXT,
                    last_event_id TEXT,
                    updated_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, runtime_order_id),
                     CHECK(
                         (provider_state IN ('FILLED', 'CANCELLED', 'REJECTED') AND terminal = 1)
                         OR (
                             provider_state IN ('ACKNOWLEDGED', 'PARTIALLY_FILLED')
                             AND terminal = 0
                         )
                     ),
                    FOREIGN KEY(account_key, runtime_order_id)
                        REFERENCES ctp_order_identity_reservations(account_key, runtime_order_id)
                );
                CREATE TABLE IF NOT EXISTS ctp_dispatch_cancel_projection (
                    account_key TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    managed_action_id TEXT NOT NULL,
                    runtime_order_id TEXT NOT NULL,
                    order_ref TEXT NOT NULL,
                    target_exchange_id TEXT NOT NULL,
                    target_order_sys_id TEXT NOT NULL,
                    target_front_id INTEGER NOT NULL,
                    target_session_id INTEGER NOT NULL,
                    provider_state TEXT NOT NULL CHECK(provider_state IN (
                        'ACKNOWLEDGED', 'REJECTED', 'TERMINAL'
                    )),
                    terminal INTEGER NOT NULL CHECK(terminal IN (0, 1)),
                    correlation_key_sha256 TEXT NOT NULL
                        CHECK(length(correlation_key_sha256) = 64),
                    source_digest_sha256 TEXT NOT NULL
                        CHECK(length(source_digest_sha256) = 64),
                    last_source_kind TEXT NOT NULL
                        CHECK(last_source_kind IN ('CALLBACK', 'RECONCILIATION')),
                    last_event_generation_id TEXT,
                    last_event_stream_id TEXT,
                    last_event_id TEXT,
                    updated_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, scope_key, managed_action_id),
                    CHECK((provider_state IN ('REJECTED', 'TERMINAL') AND terminal = 1)
                       OR (provider_state = 'ACKNOWLEDGED' AND terminal = 0)),
                    FOREIGN KEY(account_key, runtime_order_id)
                        REFERENCES ctp_order_identity_reservations(account_key, runtime_order_id)
                );
                CREATE TABLE IF NOT EXISTS ctp_dispatch_unknown_resolutions (
                    account_key TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    operation TEXT NOT NULL CHECK(operation IN ('SUBMIT', 'CANCEL')),
                    correlation_key_sha256 TEXT NOT NULL
                        CHECK(length(correlation_key_sha256) = 64),
                    order_terminal_state TEXT NOT NULL
                        CHECK(order_terminal_state IN ('FILLED', 'CANCELLED', 'REJECTED')),
                    cancel_action_terminal_state TEXT
                        CHECK(cancel_action_terminal_state IN ('REJECTED', 'TERMINAL')),
                    source_digest_sha256 TEXT NOT NULL
                        CHECK(length(source_digest_sha256) = 64),
                    verifier_id TEXT NOT NULL,
                    verified_at_ns INTEGER NOT NULL,
                    resolved_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, command_id),
                    CHECK((operation = 'SUBMIT' AND cancel_action_terminal_state IS NULL)
                       OR (operation = 'CANCEL' AND cancel_action_terminal_state IS NOT NULL)),
                    FOREIGN KEY(account_key, command_id)
                        REFERENCES ctp_dispatch_commands(account_key, command_id)
                );
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_callback_immutable_update
                BEFORE UPDATE ON ctp_dispatch_callback_ledger
                BEGIN
                    SELECT RAISE(ABORT, 'CTP callback ledger is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_callback_immutable_delete
                BEFORE DELETE ON ctp_dispatch_callback_ledger
                BEGIN
                    SELECT RAISE(ABORT, 'CTP callback ledger is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_resolution_immutable_update
                BEFORE UPDATE ON ctp_dispatch_unknown_resolutions
                BEGIN
                    SELECT RAISE(ABORT, 'CTP UNKNOWN resolution is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_resolution_immutable_delete
                BEFORE DELETE ON ctp_dispatch_unknown_resolutions
                BEGIN
                    SELECT RAISE(ABORT, 'CTP UNKNOWN resolution is immutable');
                END;
                CREATE TABLE IF NOT EXISTS ctp_dispatch_authority_uses (
                    account_key TEXT NOT NULL,
                    approval_use_id TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    command_binding_sha256 TEXT NOT NULL
                        CHECK(length(command_binding_sha256) = 64),
                    approval_digest TEXT NOT NULL CHECK(length(approval_digest) = 64),
                    source_digest_sha256 TEXT NOT NULL
                        CHECK(length(source_digest_sha256) = 64),
                    verifier_id TEXT NOT NULL,
                    verified_at_ns INTEGER NOT NULL,
                    expires_at_ns INTEGER NOT NULL CHECK(expires_at_ns > verified_at_ns),
                    writer_owner_id TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    PRIMARY KEY(account_key, approval_use_id),
                    UNIQUE(account_key, command_id),
                    FOREIGN KEY(account_key, command_id)
                        REFERENCES ctp_dispatch_commands(account_key, command_id)
                );
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_authority_uses_immutable_update
                BEFORE UPDATE ON ctp_dispatch_authority_uses
                BEGIN
                    SELECT RAISE(ABORT, 'CTP dispatch authority use is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_authority_uses_immutable_delete
                BEFORE DELETE ON ctp_dispatch_authority_uses
                BEGIN
                    SELECT RAISE(ABORT, 'CTP dispatch authority use is immutable');
                END;
                """,
            )
            row = cursor.execute(
                "SELECT value FROM execution_meta WHERE key = ?", ("schema_version",)
            ).fetchone()
            if row is None:
                cursor.execute(
                    "INSERT INTO execution_meta(key, value) VALUES (?, ?)",
                    ("schema_version", str(self._SCHEMA_VERSION)),
                )
                version = str(self._SCHEMA_VERSION)
            else:
                version = str(row["value"])
            if version == "2":
                columns = {
                    str(item["name"])
                    for item in cursor.execute("PRAGMA table_info(execution_records)").fetchall()
                }
                if "cumulative_commission" not in columns:
                    cursor.execute(
                        "ALTER TABLE execution_records ADD COLUMN cumulative_commission TEXT"
                    )
            elif version not in {
                "3", "4", "5", "6", "7", "8", "9", "10", "11",
                str(self._SCHEMA_VERSION),
            }:
                raise DurableStoreError("unsupported execution store schema")

            columns = {
                str(item["name"])
                for item in cursor.execute("PRAGMA table_info(ctp_dispatch_commands)").fetchall()
            }
            correlation_columns = {
                "correlation_version": "INTEGER NOT NULL DEFAULT 0",
                "runtime_order_id": "TEXT",
                "managed_action_id": "TEXT",
                "session_generation_id": "TEXT",
                "dispatch_front_id": "INTEGER",
                "dispatch_session_id": "INTEGER",
                "native_request_id": "INTEGER",
                "native_action_ref": "TEXT",
                "local_queue_receipt_id": (
                    "TEXT CHECK(local_queue_receipt_id IS NULL OR "
                    "(length(local_queue_receipt_id) = 32 AND "
                    "local_queue_receipt_id NOT GLOB '*[^0-9a-f]*'))"
                ),
                "local_queue_receipt_queued": (
                    "INTEGER CHECK(local_queue_receipt_queued IS NULL OR "
                    "local_queue_receipt_queued IN (0, 1))"
                ),
            }
            for name, declaration in correlation_columns.items():
                if name not in columns:
                    cursor.execute(
                        f"ALTER TABLE ctp_dispatch_commands ADD COLUMN {name} {declaration}"
                    )
            if version != str(self._SCHEMA_VERSION):
                self._migrate_legacy_ctp_callback_source_lifecycle_fences(cursor)
                # Old rows have no exact OrderRef cutover-session evidence.
                # Never let migration make those commands dispatchable.
                cursor.execute(
                    """
                    UPDATE ctp_dispatch_commands
                    SET status = 'UNKNOWN', unknown_at_ns = COALESCE(unknown_at_ns, updated_at_ns),
                        unknown_reason = 'schema_upgrade_requires_ctp_orderref_cutover'
                    WHERE status IN ('READY', 'CLAIMED')
                    """
                )
                cursor.execute(
                    "UPDATE execution_meta SET value = ? WHERE key = ?",
                    (str(self._SCHEMA_VERSION), "schema_version"),
                )
            cursor.execute(
                """
                INSERT OR IGNORE INTO ctp_order_ref_account_watermarks(
                    account_key, watermark_order_ref, cutover_established,
                    last_trading_day, last_cutover_evidence_sha256, updated_at_ns
                )
                SELECT account_key, printf('%012d', MAX(CAST(order_ref AS INTEGER))),
                       0, NULL, NULL, MAX(updated_at_ns)
                FROM (
                    SELECT account_key, order_ref, created_at_ns AS updated_at_ns
                    FROM ctp_order_identity_reservations
                    UNION ALL
                    SELECT account_key, native_max_order_ref, updated_at_ns
                    FROM ctp_order_ref_watermarks
                    UNION ALL
                    SELECT account_key, legacy_ledger_max_order_ref, updated_at_ns
                    FROM ctp_order_ref_watermarks
                ) AS known_refs
                GROUP BY account_key
                """
            )
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ctp_dispatch_session_request_unique
                ON ctp_dispatch_commands(account_key, session_generation_id, native_request_id)
                WHERE correlation_version = 1
                """
            )
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ctp_dispatch_managed_action_unique
                ON ctp_dispatch_commands(account_key, scope_key, managed_action_id)
                WHERE correlation_version = 1
                """
            )
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ctp_dispatch_local_queue_receipt_unique
                ON ctp_dispatch_commands(account_key, local_queue_receipt_id)
                WHERE local_queue_receipt_id IS NOT NULL
                """
            )
            cursor.execute("DROP TRIGGER IF EXISTS ctp_dispatch_commands_immutable")
            cursor.execute(
                """
                CREATE TRIGGER ctp_dispatch_commands_immutable
                BEFORE UPDATE OF account_key, scope_key, trading_day, operation,
                    command_id, request_payload_json, request_payload_sha256,
                    reservation_managed_intent_id, order_ref, cancel_target_order_ref,
                    cancel_target_exchange_id, cancel_target_order_sys_id,
                    cancel_target_front_id, cancel_target_session_id,
                    approval_use_id, approval_digest, session_binding_json,
                    session_binding_sha256, correlation_version, runtime_order_id,
                    managed_action_id, session_generation_id, dispatch_front_id,
                    dispatch_session_id, native_request_id, native_action_ref,
                    local_queue_receipt_id
                ON ctp_dispatch_commands
                BEGIN
                    SELECT RAISE(ABORT, 'CTP dispatch command identity is immutable');
                END;
                """
            )
            cursor.execute("DROP TRIGGER IF EXISTS ctp_dispatch_queue_receipt_once")
            cursor.execute(
                """
                CREATE TRIGGER ctp_dispatch_queue_receipt_once
                BEFORE UPDATE OF local_queue_receipt_queued ON ctp_dispatch_commands
                WHEN OLD.local_queue_receipt_queued IS NOT NULL
                  OR NEW.local_queue_receipt_queued IS NULL
                  OR OLD.local_queue_receipt_id IS NULL
                  OR OLD.status != 'READY'
                BEGIN
                    SELECT RAISE(ABORT, 'CTP queue receipt disposition is immutable');
                END;
                """
            )

    @staticmethod
    def _execute_schema_statements(cursor: sqlite3.Cursor, script: str) -> None:
        """Execute each schema statement without escaping the current transaction.

        ``sqlite3.Cursor.executescript`` commits a pending transaction before it
        runs the script. Schema creation and migrations must instead stay inside
        the ``BEGIN IMMEDIATE`` transaction owned by ``_create_schema``.
        ``sqlite3.complete_statement`` keeps compound trigger bodies together.
        """

        pending: list[str] = []
        for line in script.splitlines():
            pending.append(line)
            statement = "\n".join(pending)
            if sqlite3.complete_statement(statement):
                if statement.strip():
                    cursor.execute(statement)
                pending.clear()
        if "\n".join(pending).strip():
            raise sqlite3.OperationalError("incomplete SQL statement in execution schema")

    @staticmethod
    def _validate_ctp_order_identity_scope(scope: ExecutionScope) -> tuple[str, str, str]:
        if not isinstance(scope, ExecutionScope) or scope.provider.upper() != "CTP":
            raise ContractValidationError("CTP order identity requires a CTP execution scope")
        trading_day = scope.trading_day
        if (
            not isinstance(trading_day, str)
            or len(trading_day) != 8
            or not trading_day.isascii()
            or not trading_day.isdigit()
        ):
            raise ContractValidationError("CTP order identity requires YYYYMMDD trading_day")
        try:
            date(int(trading_day[:4]), int(trading_day[4:6]), int(trading_day[6:8]))
        except ValueError as error:
            raise ContractValidationError("invalid CTP trading_day") from error
        return scope.account_key, trading_day, scope.key

    @staticmethod
    def _ctp_order_identity_from_row(row: sqlite3.Row) -> CtpOrderIdentityReservation:
        return CtpOrderIdentityReservation(
            account_key=str(row["account_key"]),
            trading_day=str(row["trading_day"]),
            scope_key=str(row["scope_key"]),
            managed_intent_id=str(row["managed_intent_id"]),
            runtime_order_id=str(row["runtime_order_id"]),
            order_ref=str(row["order_ref"]),
            created_at_ns=int(row["created_at_ns"]),
        )

    def reserve_ctp_order_identity(
        self,
        scope: ExecutionScope,
        managed_intent_id: str,
        runtime_order_id: str,
    ) -> CtpOrderIdentityReservation:
        """Durably bind one CTP intent/runtime identity to a fresh 12-digit ref.

        A new reservation requires an already established exact-session cutover;
        the first one must use ``seed_ctp_order_ref_and_reserve_identity``.
        The sequence is account-wide across trading days and persists every
        allocated value. ``BEGIN IMMEDIATE`` serializes allocation across
        independent store instances. Repeating an existing mapping is
        idempotent; reusing either identity in a conflicting mapping fails.

        This reservation is intentionally not an execution/outbox dispatch and
        grants no provider or write authority.
        """

        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        if (
            not isinstance(managed_intent_id, str)
            or managed_intent_id != managed_intent_id.strip()
            or not managed_intent_id
            or not managed_intent_id.isascii()
            or len(managed_intent_id) > 128
            or any(not (char.isalnum() or char in "._:-") for char in managed_intent_id)
        ):
            raise ContractValidationError("invalid managed_intent_id")
        runtime_prefix = "bt-managed-v1:"
        if (
            not isinstance(runtime_order_id, str)
            or len(runtime_order_id) != len(runtime_prefix) + 64
            or not runtime_order_id.startswith(runtime_prefix)
            or any(
                char not in "0123456789abcdef" for char in runtime_order_id[len(runtime_prefix) :]
            )
        ):
            raise ContractValidationError("invalid runtime_order_id")

        now_ns = time.time_ns()
        with self._transaction() as cursor:
            existing = cursor.execute(
                """
                SELECT account_key, trading_day, scope_key, managed_intent_id,
                       runtime_order_id, order_ref, created_at_ns
                FROM ctp_order_identity_reservations
                WHERE account_key = ? AND scope_key = ? AND managed_intent_id = ?
                """,
                (account_key, scope_key, managed_intent_id),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["trading_day"]) != trading_day
                    or str(existing["runtime_order_id"]) != runtime_order_id
                ):
                    raise IntentConflictError(
                        "managed CTP intent identity conflicts with reservation"
                    )
                return self._ctp_order_identity_from_row(existing)

            runtime_owner = cursor.execute(
                """
                SELECT scope_key, managed_intent_id
                FROM ctp_order_identity_reservations
                WHERE account_key = ? AND runtime_order_id = ?
                """,
                (account_key, runtime_order_id),
            ).fetchone()
            if runtime_owner is not None:
                raise IntentConflictError("managed CTP runtime identity is already reserved")

            account_watermark = cursor.execute(
                """
                SELECT watermark_order_ref, cutover_established, last_trading_day,
                       last_cutover_evidence_sha256
                FROM ctp_order_ref_account_watermarks WHERE account_key = ?
                """,
                (account_key,),
            ).fetchone()
            daily_watermark = cursor.execute(
                """
                SELECT native_max_order_ref, legacy_ledger_max_order_ref
                FROM ctp_order_ref_watermarks
                WHERE account_key = ? AND trading_day = ?
                """,
                (account_key, trading_day),
            ).fetchone()
            if (
                account_watermark is None
                or not bool(account_watermark["cutover_established"])
                or str(account_watermark["last_trading_day"]) != trading_day
                or daily_watermark is None
            ):
                raise ContractValidationError(
                    "CTP OrderRef cutover is required before a new reservation"
                )
            cutover_session = cursor.execute(
                """
                SELECT session_generation_id, native_front_id, native_session_id
                FROM ctp_order_ref_cutover_sessions
                WHERE account_key = ? AND trading_day = ? AND scope_key = ?
                  AND evidence_sha256 = ?
                """,
                (
                    account_key,
                    trading_day,
                    scope_key,
                    str(account_watermark["last_cutover_evidence_sha256"]),
                ),
            ).fetchone()
            if cutover_session is None:
                raise ContractValidationError(
                    "CTP OrderRef cutover is required before a new reservation"
                )
            self._require_ctp_order_ref_cutover_session(
                cursor,
                account_key=account_key,
                trading_day=trading_day,
                scope_key=scope_key,
                session_generation_id=str(cutover_session["session_generation_id"]),
                native_front_id=int(cutover_session["native_front_id"]),
                native_session_id=int(cutover_session["native_session_id"]),
                order_ref=str(account_watermark["watermark_order_ref"]),
            )

            latest = cursor.execute(
                """
                SELECT order_ref
                FROM ctp_order_identity_reservations
                WHERE account_key = ?
                ORDER BY order_ref DESC
                LIMIT 1
                """,
                (account_key,),
            ).fetchone()
            next_value = 1 if latest is None else int(str(latest["order_ref"])) + 1
            next_value = max(
                next_value, int(str(account_watermark["watermark_order_ref"])) + 1
            )
            seeded_floor = max(
                int(str(daily_watermark["native_max_order_ref"])),
                int(str(daily_watermark["legacy_ledger_max_order_ref"])),
            )
            next_value = max(next_value, seeded_floor + 1)
            if next_value > 999_999_999_999:
                raise DurableStoreError("CTP OrderRef sequence is exhausted")
            order_ref = f"{next_value:012d}"
            try:
                cursor.execute(
                    """
                    INSERT INTO ctp_order_identity_reservations(
                        account_key, trading_day, scope_key, managed_intent_id,
                        runtime_order_id, order_ref, created_at_ns
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_key,
                        trading_day,
                        scope_key,
                        managed_intent_id,
                        runtime_order_id,
                        order_ref,
                        now_ns,
                    ),
                )
            except sqlite3.IntegrityError as error:
                error_name = getattr(error, "sqlite_errorname", "")
                if error_name in {"SQLITE_CONSTRAINT_PRIMARYKEY", "SQLITE_CONSTRAINT_UNIQUE"}:
                    raise IntentConflictError(
                        "managed CTP order identity is already reserved"
                    ) from error
                raise DurableStoreError("unable to persist CTP order identity") from error
            row = cursor.execute(
                """
                SELECT account_key, trading_day, scope_key, managed_intent_id,
                       runtime_order_id, order_ref, created_at_ns
                FROM ctp_order_identity_reservations
                WHERE account_key = ? AND scope_key = ? AND managed_intent_id = ?
                """,
                (account_key, scope_key, managed_intent_id),
            ).fetchone()
            assert row is not None
            self._advance_ctp_account_order_ref_watermark(
                cursor, account_key, order_ref, now_ns=now_ns
            )
            return self._ctp_order_identity_from_row(row)

    def read_ctp_order_identity(
        self, scope: ExecutionScope, managed_intent_id: str
    ) -> CtpOrderIdentityReservation | None:
        """Read one previously committed CTP identity mapping without mutation."""

        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        if (
            not isinstance(managed_intent_id, str)
            or managed_intent_id != managed_intent_id.strip()
            or not managed_intent_id
            or not managed_intent_id.isascii()
            or len(managed_intent_id) > 128
            or any(not (char.isalnum() or char in "._:-") for char in managed_intent_id)
        ):
            raise ContractValidationError("invalid managed_intent_id")
        with self._lock:
            try:
                row = self._connection.execute(
                    """
                    SELECT account_key, trading_day, scope_key, managed_intent_id,
                           runtime_order_id, order_ref, created_at_ns
                    FROM ctp_order_identity_reservations
                    WHERE account_key = ? AND trading_day = ? AND scope_key = ?
                      AND managed_intent_id = ?
                    """,
                    (account_key, trading_day, scope_key, managed_intent_id),
                ).fetchone()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read CTP order identity") from error
        return None if row is None else self._ctp_order_identity_from_row(row)

    @staticmethod
    def _validate_ctp_order_ref(value: str, field_name: str) -> str:
        if (
            not isinstance(value, str)
            or len(value) != 12
            or not value.isascii()
            or not value.isdigit()
        ):
            raise ContractValidationError("invalid " + field_name)
        return value

    @staticmethod
    def _ctp_legacy_mapping_payload(mapping: CtpOrderRefLegacyMapping) -> dict[str, str]:
        return {
            "source_name": mapping.source_name,
            "account_key": mapping.account_key,
            "trading_day": mapping.trading_day,
            "scope_key": mapping.scope_key,
            "managed_intent_id": mapping.managed_intent_id,
            "runtime_order_id": mapping.runtime_order_id,
            "order_ref": mapping.order_ref,
        }

    @staticmethod
    def _ctp_legacy_mapping_sha256(mapping: CtpOrderRefLegacyMapping) -> str:
        return payload_sha256(SqliteExecutionStore._ctp_legacy_mapping_payload(mapping))

    def _validate_ctp_order_ref_seed_proof(
        self,
        scope: ExecutionScope,
        proof: CtpOrderRefSeedProof,
    ) -> tuple[str, str, tuple[dict[str, str], ...]]:
        """Validate exact offline cutover facts and return manifest/evidence digests."""

        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        if (
            type(proof) is not CtpOrderRefSeedProof
            or proof.trading_day != trading_day
            or proof.account_key != account_key
            or proof.scope_key != scope_key
        ):
            raise ContractValidationError("CTP OrderRef proof does not bind the exact account/scope/day")
        self._validate_ctp_order_ref(proof.native_max_order_ref, "native MaxOrderRef")
        self._validate_ctp_order_ref(
            proof.legacy_ledger_max_order_ref, "legacy ledger MaxOrderRef"
        )
        self._validate_sha256(proof.legacy_ledger_sha256, "legacy ledger digest")
        _validate_correlation_text(proof.session_generation_id, "OrderRef proof session generation")
        if type(proof.native_front_id) is not int or proof.native_front_id <= 0:
            raise ContractValidationError("invalid OrderRef proof FrontID")
        if type(proof.native_session_id) is not int or proof.native_session_id <= 0:
            raise ContractValidationError("invalid OrderRef proof SessionID")

        source_digests: dict[str, str] = {}
        if type(proof.legacy_source_sha256) is not tuple or len(proof.legacy_source_sha256) != 2:
            raise ContractValidationError("both legacy OrderRef source digests are required")
        for pair in proof.legacy_source_sha256:
            if type(pair) is not tuple or len(pair) != 2:
                raise ContractValidationError("invalid legacy OrderRef source digest entry")
            source_name, digest = pair
            if source_name not in {"backtrader_prototype", "sdk_jsonl"}:
                raise ContractValidationError("unknown legacy OrderRef source")
            self._validate_sha256(digest, "legacy source digest")
            if source_name in source_digests:
                raise ContractValidationError("duplicate legacy OrderRef source digest")
            source_digests[source_name] = digest
        if set(source_digests) != {"backtrader_prototype", "sdk_jsonl"}:
            raise ContractValidationError("both legacy OrderRef sources must be inventoried")
        if payload_sha256(source_digests) != proof.legacy_ledger_sha256:
            raise ContractValidationError("legacy source digests do not match the ledger digest")

        if type(proof.existing_native_order_refs) is not tuple:
            raise ContractValidationError("verified native OrderRefs must be a tuple")
        native_refs = tuple(proof.existing_native_order_refs)
        for reference in native_refs:
            self._validate_ctp_order_ref(reference, "verified native OrderRef")
        if len(set(native_refs)) != len(native_refs):
            raise ContractValidationError("duplicate verified native OrderRef")
        if native_refs and max(map(int, native_refs)) > int(proof.native_max_order_ref):
            raise ContractValidationError("native MaxOrderRef is below a verified existing OrderRef")

        if type(proof.legacy_mappings) is not tuple:
            raise ContractValidationError("legacy OrderRef mappings must be a tuple")
        mapping_payloads: list[dict[str, str]] = []
        source_refs: set[tuple[str, str]] = set()
        legacy_refs: list[int] = []
        for mapping in proof.legacy_mappings:
            if type(mapping) is not CtpOrderRefLegacyMapping:
                raise ContractValidationError("invalid typed legacy OrderRef mapping")
            if mapping.source_name not in source_digests:
                raise ContractValidationError("legacy mapping has no inventoried source")
            if mapping.account_key != account_key:
                raise ContractValidationError("legacy OrderRef mapping belongs to another account")
            if (
                type(mapping.trading_day) is not str
                or len(mapping.trading_day) != 8
                or not mapping.trading_day.isascii()
                or not mapping.trading_day.isdigit()
            ):
                raise ContractValidationError("legacy OrderRef mapping has an invalid trading day")
            try:
                date(
                    int(mapping.trading_day[:4]),
                    int(mapping.trading_day[4:6]),
                    int(mapping.trading_day[6:8]),
                )
            except ValueError as error:
                raise ContractValidationError(
                    "legacy OrderRef mapping has an invalid trading day"
                ) from error
            if not _is_prefixed_digest(mapping.scope_key, "scope:"):
                raise ContractValidationError("legacy OrderRef mapping has an invalid scope key")
            self._validate_command_identifier(mapping.managed_intent_id, "legacy managed intent id")
            self._validate_command_identifier(mapping.runtime_order_id, "legacy runtime order id")
            self._validate_ctp_order_ref(mapping.order_ref, "legacy OrderRef")
            source_ref = (mapping.source_name, mapping.order_ref)
            if source_ref in source_refs:
                raise ContractValidationError("duplicate source OrderRef mapping")
            source_refs.add(source_ref)
            legacy_refs.append(int(mapping.order_ref))
            mapping_payloads.append(self._ctp_legacy_mapping_payload(mapping))
        expected_legacy_max = f"{max(legacy_refs, default=0):012d}"
        if expected_legacy_max != proof.legacy_ledger_max_order_ref:
            raise ContractValidationError(
                "legacy ledger maximum does not match the complete imported mapping set"
            )

        native_refs_sha256 = payload_sha256(sorted(native_refs))
        mapping_payloads.sort(
            key=lambda item: (
                item["source_name"],
                item["trading_day"],
                item["scope_key"],
                item["managed_intent_id"],
                item["order_ref"],
            )
        )
        mappings_sha256 = payload_sha256(mapping_payloads)
        evidence_sha256 = payload_sha256(
            {
                "account_key": account_key,
                "scope_key": scope_key,
                "trading_day": trading_day,
                "session_generation_id": proof.session_generation_id,
                "native_front_id": proof.native_front_id,
                "native_session_id": proof.native_session_id,
                "native_max_order_ref": proof.native_max_order_ref,
                "native_refs_sha256": native_refs_sha256,
                "legacy_ledger_max_order_ref": proof.legacy_ledger_max_order_ref,
                "legacy_ledger_sha256": proof.legacy_ledger_sha256,
                "legacy_source_sha256": source_digests,
                "legacy_mappings_sha256": mappings_sha256,
            }
        )
        return mappings_sha256, evidence_sha256, tuple(mapping_payloads)

    def _import_ctp_legacy_order_ref_mappings(
        self,
        cursor: sqlite3.Cursor,
        proof: CtpOrderRefSeedProof,
        *,
        now_ns: int,
    ) -> None:
        """Import exact legacy mappings, preserving their original identities."""

        proof_mappings = {
            (mapping.source_name, mapping.order_ref): self._ctp_legacy_mapping_sha256(mapping)
            for mapping in proof.legacy_mappings
        }
        prior_imports = cursor.execute(
            """
            SELECT source_name, order_ref, mapping_sha256
            FROM ctp_order_ref_legacy_imports WHERE account_key = ?
            """,
            (proof.account_key,),
        ).fetchall()
        if any(
            proof_mappings.get((str(row["source_name"]), str(row["order_ref"])))
            != str(row["mapping_sha256"])
            for row in prior_imports
        ):
            raise IntentConflictError("legacy OrderRef cutover omits an imported mapping")

        for mapping in proof.legacy_mappings:
            existing = cursor.execute(
                """
                SELECT account_key, trading_day, scope_key, managed_intent_id,
                       runtime_order_id, order_ref, created_at_ns
                FROM ctp_order_identity_reservations
                WHERE account_key = ? AND scope_key = ? AND managed_intent_id = ?
                """,
                (mapping.account_key, mapping.scope_key, mapping.managed_intent_id),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["trading_day"]) != mapping.trading_day
                    or str(existing["runtime_order_id"]) != mapping.runtime_order_id
                    or str(existing["order_ref"]) != mapping.order_ref
                ):
                    raise IntentConflictError("legacy CTP identity conflicts with a reservation")
            else:
                try:
                    cursor.execute(
                        """
                        INSERT INTO ctp_order_identity_reservations(
                            account_key, trading_day, scope_key, managed_intent_id,
                            runtime_order_id, order_ref, created_at_ns
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            mapping.account_key,
                            mapping.trading_day,
                            mapping.scope_key,
                            mapping.managed_intent_id,
                            mapping.runtime_order_id,
                            mapping.order_ref,
                            now_ns,
                        ),
                    )
                except sqlite3.IntegrityError as error:
                    error_name = getattr(error, "sqlite_errorname", "")
                    if error_name in {
                        "SQLITE_CONSTRAINT_PRIMARYKEY",
                        "SQLITE_CONSTRAINT_UNIQUE",
                    }:
                        raise IntentConflictError(
                            "legacy CTP identity conflicts with an account reservation"
                        ) from error
                    raise DurableStoreError(
                        "unable to persist CTP order identity"
                    ) from error

            mapping_sha256 = self._ctp_legacy_mapping_sha256(mapping)
            prior = cursor.execute(
                """
                SELECT trading_day, scope_key, managed_intent_id, runtime_order_id,
                       mapping_sha256
                FROM ctp_order_ref_legacy_imports
                WHERE account_key = ? AND source_name = ? AND order_ref = ?
                """,
                (proof.account_key, mapping.source_name, mapping.order_ref),
            ).fetchone()
            if prior is not None:
                if (
                    str(prior["trading_day"]) != mapping.trading_day
                    or str(prior["scope_key"]) != mapping.scope_key
                    or str(prior["managed_intent_id"]) != mapping.managed_intent_id
                    or str(prior["runtime_order_id"]) != mapping.runtime_order_id
                    or str(prior["mapping_sha256"]) != mapping_sha256
                ):
                    raise IntentConflictError("legacy source mapping conflicts with prior import")
                continue
            cursor.execute(
                """
                INSERT INTO ctp_order_ref_legacy_imports(
                    account_key, source_name, order_ref, trading_day, scope_key,
                    managed_intent_id, runtime_order_id, mapping_sha256, imported_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    proof.account_key,
                    mapping.source_name,
                    mapping.order_ref,
                    mapping.trading_day,
                    mapping.scope_key,
                    mapping.managed_intent_id,
                    mapping.runtime_order_id,
                    mapping_sha256,
                    now_ns,
                ),
            )

    def _store_ctp_order_ref_cutover_evidence(
        self,
        cursor: sqlite3.Cursor,
        proof: CtpOrderRefSeedProof,
        *,
        mappings_sha256: str,
        evidence_sha256: str,
        now_ns: int,
    ) -> None:
        """Persist immutable session evidence and the per-day collision floor."""

        native_refs_sha256 = payload_sha256(sorted(proof.existing_native_order_refs))
        source_digests = dict(proof.legacy_source_sha256)
        existing = cursor.execute(
            """
            SELECT scope_key, native_front_id, native_session_id, native_max_order_ref,
                   native_refs_sha256, legacy_ledger_max_order_ref,
                   backtrader_prototype_sha256, sdk_jsonl_sha256,
                   legacy_ledger_sha256, legacy_mappings_sha256, evidence_sha256
            FROM ctp_order_ref_cutover_sessions
            WHERE account_key = ? AND trading_day = ? AND scope_key = ?
              AND session_generation_id = ?
            """,
            (proof.account_key, proof.trading_day, proof.scope_key, proof.session_generation_id),
        ).fetchone()
        session_was_present = existing is not None
        expected = (
            proof.scope_key,
            proof.native_front_id,
            proof.native_session_id,
            proof.native_max_order_ref,
            native_refs_sha256,
            proof.legacy_ledger_max_order_ref,
            source_digests["backtrader_prototype"],
            source_digests["sdk_jsonl"],
            proof.legacy_ledger_sha256,
            mappings_sha256,
            evidence_sha256,
        )
        if existing is None:
            cursor.execute(
                """
                INSERT INTO ctp_order_ref_cutover_sessions(
                    account_key, trading_day, scope_key, session_generation_id,
                    native_front_id, native_session_id, native_max_order_ref,
                    native_refs_sha256, legacy_ledger_max_order_ref,
                    backtrader_prototype_sha256, sdk_jsonl_sha256,
                    legacy_ledger_sha256, legacy_mappings_sha256, evidence_sha256,
                    created_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    proof.account_key,
                    proof.trading_day,
                    proof.scope_key,
                    proof.session_generation_id,
                    proof.native_front_id,
                    proof.native_session_id,
                    proof.native_max_order_ref,
                    native_refs_sha256,
                    proof.legacy_ledger_max_order_ref,
                    source_digests["backtrader_prototype"],
                    source_digests["sdk_jsonl"],
                    proof.legacy_ledger_sha256,
                    mappings_sha256,
                    evidence_sha256,
                    now_ns,
                ),
            )
        elif tuple(
            str(existing[name]) if name in {
                "scope_key",
                "native_max_order_ref",
                "native_refs_sha256",
                "legacy_ledger_max_order_ref",
                "backtrader_prototype_sha256",
                "sdk_jsonl_sha256",
                "legacy_ledger_sha256",
                "legacy_mappings_sha256",
                "evidence_sha256",
            } else int(existing[name])
            for name in (
                "scope_key",
                "native_front_id",
                "native_session_id",
                "native_max_order_ref",
                "native_refs_sha256",
                "legacy_ledger_max_order_ref",
                "backtrader_prototype_sha256",
                "sdk_jsonl_sha256",
                "legacy_ledger_sha256",
                "legacy_mappings_sha256",
                "evidence_sha256",
            )
        ) != expected:
            raise IntentConflictError("CTP OrderRef session evidence conflicts with prior cutover")

        watermark = cursor.execute(
            """
            SELECT native_max_order_ref, legacy_ledger_max_order_ref,
                   legacy_ledger_sha256, updated_at_ns
            FROM ctp_order_ref_watermarks
            WHERE account_key = ? AND trading_day = ?
            """,
            (proof.account_key, proof.trading_day),
        ).fetchone()
        if (
            watermark is not None
            and session_was_present
            and str(watermark["native_max_order_ref"]) == proof.native_max_order_ref
            and str(watermark["legacy_ledger_max_order_ref"])
            == proof.legacy_ledger_max_order_ref
            and str(watermark["legacy_ledger_sha256"]) == proof.legacy_ledger_sha256
        ):
            return
        if watermark is None:
            cursor.execute(
                """
                INSERT INTO ctp_order_ref_watermarks(
                    account_key, trading_day, native_max_order_ref,
                    legacy_ledger_max_order_ref, legacy_ledger_sha256, updated_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    proof.account_key,
                    proof.trading_day,
                    proof.native_max_order_ref,
                    proof.legacy_ledger_max_order_ref,
                    proof.legacy_ledger_sha256,
                    now_ns,
                ),
            )
        else:
            if (
                int(proof.native_max_order_ref) < int(str(watermark["native_max_order_ref"]))
                or int(proof.legacy_ledger_max_order_ref)
                < int(str(watermark["legacy_ledger_max_order_ref"]))
            ):
                raise IntentConflictError("CTP OrderRef seed watermark cannot move backward")
            cursor.execute(
                """
                UPDATE ctp_order_ref_watermarks
                SET native_max_order_ref = ?, legacy_ledger_max_order_ref = ?,
                    legacy_ledger_sha256 = ?, updated_at_ns = ?
                WHERE account_key = ? AND trading_day = ?
                """,
                (
                    proof.native_max_order_ref,
                    proof.legacy_ledger_max_order_ref,
                    proof.legacy_ledger_sha256,
                    now_ns,
                    proof.account_key,
                    proof.trading_day,
                ),
            )

    @staticmethod
    def _advance_ctp_account_order_ref_watermark(
        cursor: sqlite3.Cursor,
        account_key: str,
        order_ref: str,
        *,
        now_ns: int,
        trading_day: str | None = None,
        evidence_sha256: str | None = None,
        cutover_established: bool | None = None,
    ) -> None:
        """Persist the account-wide maximum without ever lowering it."""

        row = cursor.execute(
            """
            SELECT watermark_order_ref, cutover_established, last_trading_day
            FROM ctp_order_ref_account_watermarks WHERE account_key = ?
            """,
            (account_key,),
        ).fetchone()
        if row is None:
            cursor.execute(
                """
                INSERT INTO ctp_order_ref_account_watermarks(
                    account_key, watermark_order_ref, cutover_established,
                    last_trading_day, last_cutover_evidence_sha256, updated_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    account_key,
                    order_ref,
                    int(bool(cutover_established)),
                    trading_day,
                    evidence_sha256,
                    now_ns,
                ),
            )
            return
        active_trading_day = row["last_trading_day"]
        if (
            trading_day is not None
            and active_trading_day is not None
            and trading_day < str(active_trading_day)
        ):
            raise IntentConflictError("CTP OrderRef active trading day cannot move backward")
        current = str(row["watermark_order_ref"])
        advanced = f"{max(int(current), int(order_ref)):012d}"
        established = bool(row["cutover_established"]) or bool(cutover_established)
        cursor.execute(
            """
            UPDATE ctp_order_ref_account_watermarks
            SET watermark_order_ref = ?, cutover_established = ?,
                last_trading_day = COALESCE(?, last_trading_day),
                last_cutover_evidence_sha256 = COALESCE(?, last_cutover_evidence_sha256),
                updated_at_ns = ?
            WHERE account_key = ?
            """,
            (
                advanced,
                int(established),
                trading_day,
                evidence_sha256,
                now_ns,
                account_key,
            ),
        )

    @staticmethod
    def _assert_ctp_order_ref_evidence_is_current_or_new(
        cursor: sqlite3.Cursor,
        proof: CtpOrderRefSeedProof,
        evidence_sha256: str,
    ) -> None:
        """Reject superseded session proofs and account-day rollback attempts."""

        account_watermark = cursor.execute(
            """
            SELECT last_trading_day, last_cutover_evidence_sha256
            FROM ctp_order_ref_account_watermarks WHERE account_key = ?
            """,
            (proof.account_key,),
        ).fetchone()
        if account_watermark is None:
            return
        active_day = account_watermark["last_trading_day"]
        active_evidence = account_watermark["last_cutover_evidence_sha256"]
        if active_day is not None and proof.trading_day < str(active_day):
            raise IntentConflictError("CTP OrderRef active trading day cannot move backward")

        prior_session = cursor.execute(
            """
            SELECT evidence_sha256 FROM ctp_order_ref_cutover_sessions
            WHERE account_key = ? AND trading_day = ? AND scope_key = ?
              AND session_generation_id = ?
            """,
            (
                proof.account_key,
                proof.trading_day,
                proof.scope_key,
                proof.session_generation_id,
            ),
        ).fetchone()
        if prior_session is None:
            return
        if str(prior_session["evidence_sha256"]) != evidence_sha256:
            raise IntentConflictError("CTP OrderRef session proof conflicts with stored evidence")
        if active_day is None or str(active_day) != proof.trading_day or str(
            active_evidence
        ) != evidence_sha256:
            raise IntentConflictError("CTP OrderRef session evidence was superseded")

    @staticmethod
    def _require_ctp_order_ref_cutover_session(
        cursor: sqlite3.Cursor,
        *,
        account_key: str,
        trading_day: str,
        scope_key: str,
        session_generation_id: str,
        native_front_id: int,
        native_session_id: int,
        order_ref: str,
    ) -> None:
        account_watermark = cursor.execute(
            """
            SELECT watermark_order_ref, cutover_established, last_trading_day,
                   last_cutover_evidence_sha256
            FROM ctp_order_ref_account_watermarks WHERE account_key = ?
            """,
            (account_key,),
        ).fetchone()
        evidence = cursor.execute(
            """
            SELECT evidence_sha256 FROM ctp_order_ref_cutover_sessions
            WHERE account_key = ? AND trading_day = ? AND scope_key = ?
              AND session_generation_id = ? AND native_front_id = ?
              AND native_session_id = ?
            """,
            (
                account_key,
                trading_day,
                scope_key,
                session_generation_id,
                native_front_id,
                native_session_id,
            ),
        ).fetchone()
        if (
            account_watermark is None
            or not bool(account_watermark["cutover_established"])
            or evidence is None
            or str(account_watermark["last_trading_day"]) != trading_day
            or str(account_watermark["last_cutover_evidence_sha256"])
            != str(evidence["evidence_sha256"])
            or int(order_ref) > int(str(account_watermark["watermark_order_ref"]))
        ):
            raise ContractValidationError(
                "CTP dispatch requires the exact current OrderRef cutover session"
            )

    @staticmethod
    def _validate_sha256(value: str, field_name: str) -> str:
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
        ):
            raise ContractValidationError("invalid " + field_name)
        return value

    @staticmethod
    def _validate_command_identifier(value: str, field_name: str) -> str:
        if (
            not isinstance(value, str)
            or value != value.strip()
            or not value
            or len(value) > 128
            or not value.isascii()
            or any(not (char.isalnum() or char in "._:-") for char in value)
        ):
            raise ContractValidationError("invalid " + field_name)
        return value

    def _verify_ctp_dispatch_authority(
        self,
        verifier: CtpDispatchAuthorityVerifier,
        command: CtpDispatchCommand,
        *,
        now_ns: int,
    ) -> CtpDispatchAuthority:
        """Invoke and structurally validate the required fresh action verifier."""

        verify_action = getattr(verifier, "verify_action", None)
        if not callable(verify_action):
            raise ContractValidationError("fresh CTP dispatch authority verifier is required")
        try:
            authority = verify_action(command, now_ns=now_ns)
        except Exception:
            raise ContractValidationError(
                "fresh CTP dispatch authority verification failed"
            ) from None
        if type(authority) is not CtpDispatchAuthority:
            raise ContractValidationError("invalid typed CTP dispatch authority")
        if authority.authority_type != "ctp_dispatch_authority.v1":
            raise ContractValidationError("invalid CTP dispatch authority type")
        self._validate_sha256(authority.command_binding_sha256, "authority command binding")
        self._validate_sha256(authority.approval_digest, "authority approval digest")
        self._validate_sha256(authority.source_digest_sha256, "authority source digest")
        self._validate_command_identifier(authority.approval_use_id, "authority approval use id")
        self._validate_command_identifier(authority.verifier_id, "authority verifier id")
        if (
            authority.command_binding_sha256 != command.authority_binding_sha256
            or authority.approval_use_id != command.approval_use_id
            or authority.approval_digest != command.approval_digest
        ):
            raise ContractValidationError("CTP dispatch authority binding does not match command")
        if type(authority.verified_at_ns) is not int or authority.verified_at_ns != now_ns:
            raise ContractValidationError("CTP dispatch authority was not freshly verified")
        if type(authority.expires_at_ns) is not int or authority.expires_at_ns <= now_ns:
            raise ContractValidationError("CTP dispatch authority is expired")
        return authority

    @staticmethod
    def _reject_sensitive_command_fields(value: Any) -> None:
        """Reject credential-shaped keys before canonical command persistence."""

        forbidden = (
            "password",
            "passwd",
            "auth",
            "secret",
            "credential",
            "token",
            "privatekey",
            "private",
            "apikey",
            "accesskey",
            "approvalkey",
        )
        if isinstance(value, dict):
            for key, child in value.items():
                normalized = "".join(char.lower() for char in str(key) if char.isalnum())
                if any(part in normalized for part in forbidden):
                    raise ContractValidationError(
                        "credential-shaped CTP command field is forbidden"
                    )
                SqliteExecutionStore._reject_sensitive_command_fields(child)
        elif isinstance(value, list):
            for child in value:
                SqliteExecutionStore._reject_sensitive_command_fields(child)

    @classmethod
    def _validate_ctp_cancel_target(
        cls, target: CtpCancelTarget, request_payload: Mapping[str, Any]
    ) -> CtpCancelTarget:
        if type(target) is not CtpCancelTarget:
            raise ContractValidationError("CANCEL requires a typed exact CTP target")
        cls._validate_ctp_order_ref(target.order_ref, "cancel target OrderRef")
        if (
            not isinstance(target.exchange_id, str)
            or not target.exchange_id
            or len(target.exchange_id) > 9
            or not target.exchange_id.isascii()
            or not target.exchange_id.isalnum()
            or not isinstance(target.order_sys_id, str)
            or not target.order_sys_id
            or len(target.order_sys_id) > 21
            or not target.order_sys_id.isascii()
            or target.order_sys_id != target.order_sys_id.strip()
            or type(target.front_id) is not int
            or target.front_id <= 0
            or type(target.session_id) is not int
            or target.session_id <= 0
        ):
            raise ContractValidationError("invalid exact CTP cancel target")
        expected = {
            "OrderRef": target.order_ref,
            "ExchangeID": target.exchange_id,
            "OrderSysID": target.order_sys_id,
            "FrontID": target.front_id,
            "SessionID": target.session_id,
        }
        for key, value in expected.items():
            if key not in request_payload or type(request_payload[key]) is not type(value):
                raise ContractValidationError("CTP cancel request lacks exact target echo: " + key)
            if request_payload[key] != value:
                raise ContractValidationError("CTP cancel request target mismatch: " + key)
        # CTP encodes delete as '0' and modify as '3'. A command whose
        # operation is CANCEL must never carry the valid modify action.
        if (
            type(request_payload.get("ActionFlag")) is not str
            or request_payload["ActionFlag"] != "0"
        ):
            raise ContractValidationError(
                "CTP cancel request requires native delete ActionFlag '0'"
            )
        # These fields on CThostFtdcInputOrderActionField are meaningful for
        # modify actions. Leave them at their native zero defaults for deletes.
        limit_price = request_payload.get("LimitPrice", 0.0)
        if type(limit_price) not in (int, float) or limit_price != 0:
            raise ContractValidationError("CTP cancel request must not change LimitPrice")
        volume_change = request_payload.get("VolumeChange", 0)
        if type(volume_change) is not int or volume_change != 0:
            raise ContractValidationError("CTP cancel request must not change VolumeChange")
        return target

    def record_ctp_order_ref_seed(
        self,
        scope: ExecutionScope,
        proof: CtpOrderRefSeedProof,
        *,
        writer_lease: WriterLease,
    ) -> None:
        """Record a later session's cutover evidence without allocating a ref.

        The initial cutover must remain atomic with the first new reservation.
        All evidence is caller supplied and non-authorizing.
        """

        account_key, trading_day, _ = self._validate_ctp_order_identity_scope(scope)
        mappings_sha256, evidence_sha256, _ = self._validate_ctp_order_ref_seed_proof(
            scope, proof
        )
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            self._assert_ctp_order_ref_evidence_is_current_or_new(
                cursor, proof, evidence_sha256
            )
            existing = cursor.execute(
                """
                SELECT 1 FROM ctp_order_ref_watermarks
                WHERE account_key = ? AND trading_day = ?
                """,
                (account_key, trading_day),
            ).fetchone()
            account_watermark = cursor.execute(
                """
                SELECT watermark_order_ref, cutover_established
                FROM ctp_order_ref_account_watermarks WHERE account_key = ?
                """,
                (account_key,),
            ).fetchone()
            if existing is None or account_watermark is None or not bool(
                account_watermark["cutover_established"]
            ):
                raise ContractValidationError(
                    "initial CTP OrderRef seed must commit atomically with a new reservation"
                )
            self._import_ctp_legacy_order_ref_mappings(cursor, proof, now_ns=now_ns)
            self._store_ctp_order_ref_cutover_evidence(
                cursor,
                proof,
                mappings_sha256=mappings_sha256,
                evidence_sha256=evidence_sha256,
                now_ns=now_ns,
            )
            local_latest = cursor.execute(
                """
                SELECT order_ref FROM ctp_order_identity_reservations
                WHERE account_key = ? ORDER BY order_ref DESC LIMIT 1
                """,
                (account_key,),
            ).fetchone()
            floor = max(
                int(str(account_watermark["watermark_order_ref"])),
                int(proof.native_max_order_ref),
                int(proof.legacy_ledger_max_order_ref),
                max(map(int, proof.existing_native_order_refs), default=0),
                0 if local_latest is None else int(str(local_latest["order_ref"])),
            )
            self._advance_ctp_account_order_ref_watermark(
                cursor,
                account_key,
                f"{floor:012d}",
                now_ns=now_ns,
                trading_day=trading_day,
                evidence_sha256=evidence_sha256,
                cutover_established=True,
            )

    def seed_ctp_order_ref_and_reserve_identity(
        self,
        scope: ExecutionScope,
        proof: CtpOrderRefSeedProof,
        managed_intent_id: str,
        runtime_order_id: str,
        *,
        writer_lease: WriterLease,
    ) -> CtpOrderIdentityReservation:
        """Atomically record the first/updated watermark and reserve a fresh ref.

        The proof remains caller-supplied and non-authorizing. This transaction
        only ensures that initial seeding cannot be separated from the first
        local reservation and that allocated refs advance past both local and
        reported native/legacy maxima.
        """

        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        mappings_sha256, evidence_sha256, _ = self._validate_ctp_order_ref_seed_proof(
            scope, proof
        )
        if (
            not isinstance(managed_intent_id, str)
            or managed_intent_id != managed_intent_id.strip()
            or not managed_intent_id
            or not managed_intent_id.isascii()
            or len(managed_intent_id) > 128
            or any(not (char.isalnum() or char in "._:-") for char in managed_intent_id)
        ):
            raise ContractValidationError("invalid managed_intent_id")
        runtime_prefix = "bt-managed-v1:"
        if (
            not isinstance(runtime_order_id, str)
            or len(runtime_order_id) != len(runtime_prefix) + 64
            or not runtime_order_id.startswith(runtime_prefix)
            or any(
                char not in "0123456789abcdef" for char in runtime_order_id[len(runtime_prefix) :]
            )
        ):
            raise ContractValidationError("invalid runtime_order_id")

        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            self._assert_ctp_order_ref_evidence_is_current_or_new(
                cursor, proof, evidence_sha256
            )
            existing_identity = cursor.execute(
                """
                SELECT account_key, trading_day, scope_key, managed_intent_id,
                       runtime_order_id, order_ref, created_at_ns
                FROM ctp_order_identity_reservations
                WHERE account_key = ? AND scope_key = ? AND managed_intent_id = ?
                """,
                (account_key, scope_key, managed_intent_id),
            ).fetchone()
            watermark = cursor.execute(
                """
                SELECT native_max_order_ref, legacy_ledger_max_order_ref,
                       legacy_ledger_sha256
                FROM ctp_order_ref_watermarks
                WHERE account_key = ? AND trading_day = ?
                """,
                (account_key, trading_day),
            ).fetchone()
            account_watermark = cursor.execute(
                """
                SELECT watermark_order_ref, cutover_established
                FROM ctp_order_ref_account_watermarks WHERE account_key = ?
                """,
                (account_key,),
            ).fetchone()
            if existing_identity is not None:
                session_evidence = cursor.execute(
                    """
                    SELECT evidence_sha256 FROM ctp_order_ref_cutover_sessions
                    WHERE account_key = ? AND trading_day = ? AND scope_key = ?
                      AND session_generation_id = ?
                    """,
                    (account_key, trading_day, scope_key, proof.session_generation_id),
                ).fetchone()
                if (
                    watermark is not None
                    and account_watermark is not None
                    and bool(account_watermark["cutover_established"])
                    and str(watermark["native_max_order_ref"]) == proof.native_max_order_ref
                    and str(watermark["legacy_ledger_max_order_ref"])
                    == proof.legacy_ledger_max_order_ref
                    and str(watermark["legacy_ledger_sha256"]) == proof.legacy_ledger_sha256
                    and session_evidence is not None
                    and str(session_evidence["evidence_sha256"]) == evidence_sha256
                    and str(existing_identity["trading_day"]) == trading_day
                    and str(existing_identity["runtime_order_id"]) == runtime_order_id
                ):
                    return self._ctp_order_identity_from_row(existing_identity)
                raise IntentConflictError(
                    "managed CTP intent identity conflicts with seed reservation"
                )

            if watermark is not None and (
                int(proof.native_max_order_ref) < int(watermark["native_max_order_ref"])
                or int(proof.legacy_ledger_max_order_ref)
                < int(watermark["legacy_ledger_max_order_ref"])
            ):
                raise IntentConflictError("CTP OrderRef seed watermark cannot move backward")
            self._import_ctp_legacy_order_ref_mappings(cursor, proof, now_ns=now_ns)
            self._store_ctp_order_ref_cutover_evidence(
                cursor,
                proof,
                mappings_sha256=mappings_sha256,
                evidence_sha256=evidence_sha256,
                now_ns=now_ns,
            )
            account_watermark = cursor.execute(
                """
                SELECT watermark_order_ref, cutover_established
                FROM ctp_order_ref_account_watermarks WHERE account_key = ?
                """,
                (account_key,),
            ).fetchone()
            runtime_owner = cursor.execute(
                """
                SELECT 1 FROM ctp_order_identity_reservations
                WHERE account_key = ? AND runtime_order_id = ?
                """,
                (account_key, runtime_order_id),
            ).fetchone()
            if runtime_owner is not None:
                raise IntentConflictError("managed CTP runtime identity is already reserved")
            latest = cursor.execute(
                "SELECT order_ref FROM ctp_order_identity_reservations "
                "WHERE account_key = ? ORDER BY order_ref DESC LIMIT 1",
                (account_key,),
            ).fetchone()
            floor = max(
                int(proof.native_max_order_ref),
                int(proof.legacy_ledger_max_order_ref),
                max(map(int, proof.existing_native_order_refs), default=0),
                0 if latest is None else int(str(latest["order_ref"])),
                0
                if account_watermark is None
                else int(str(account_watermark["watermark_order_ref"])),
            )
            next_value = floor + 1
            if next_value > 999_999_999_999:
                raise DurableStoreError("CTP OrderRef sequence is exhausted")
            order_ref = f"{next_value:012d}"
            try:
                cursor.execute(
                    """
                    INSERT INTO ctp_order_identity_reservations(
                        account_key, trading_day, scope_key, managed_intent_id,
                        runtime_order_id, order_ref, created_at_ns
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_key,
                        trading_day,
                        scope_key,
                        managed_intent_id,
                        runtime_order_id,
                        order_ref,
                        now_ns,
                    ),
                )
            except sqlite3.IntegrityError as error:
                error_name = getattr(error, "sqlite_errorname", "")
                if error_name in {
                    "SQLITE_CONSTRAINT_PRIMARYKEY",
                    "SQLITE_CONSTRAINT_UNIQUE",
                }:
                    raise IntentConflictError(
                        "managed CTP order identity is already reserved"
                    ) from error
                raise DurableStoreError("unable to persist CTP order identity") from error
            result = cursor.execute(
                """
                SELECT account_key, trading_day, scope_key, managed_intent_id,
                       runtime_order_id, order_ref, created_at_ns
                FROM ctp_order_identity_reservations
                WHERE account_key = ? AND scope_key = ? AND managed_intent_id = ?
                """,
                (account_key, scope_key, managed_intent_id),
            ).fetchone()
            assert result is not None
            self._advance_ctp_account_order_ref_watermark(
                cursor,
                account_key,
                order_ref,
                now_ns=now_ns,
                trading_day=trading_day,
                evidence_sha256=evidence_sha256,
                cutover_established=True,
            )
            return self._ctp_order_identity_from_row(result)

    @staticmethod
    def _ctp_dispatch_command_from_row(row: sqlite3.Row) -> CtpDispatchCommand:
        try:
            request_payload = json.loads(str(row["request_payload_json"]))
            session_binding = json.loads(str(row["session_binding_json"]))
            native_receipt_payload = (
                None
                if row["native_receipt_payload_json"] is None
                else json.loads(str(row["native_receipt_payload_json"]))
            )
            completion_echo = (
                None
                if row["completion_echo_json"] is None
                else json.loads(str(row["completion_echo_json"]))
            )
            local_queue_receipt_id = (
                None
                if row["local_queue_receipt_id"] is None
                else str(row["local_queue_receipt_id"])
            )
            queued_value = row["local_queue_receipt_queued"]
            if queued_value not in (None, 0, 1):
                raise ValueError("stored CTP queue disposition is invalid")
            local_queue_receipt_queued = None if queued_value is None else bool(queued_value)
            if local_queue_receipt_id is not None and not _is_local_queue_receipt_id(
                local_queue_receipt_id
            ):
                raise ValueError("stored CTP local queue receipt id is invalid")
            if local_queue_receipt_id is None and local_queue_receipt_queued is not None:
                raise ValueError("stored CTP queue state has no receipt id")
            correlation_version = int(row["correlation_version"])
            correlation_key = None
            if correlation_version == 1:
                action_order_ref = (
                    str(row["order_ref"])
                    if row["order_ref"] is not None
                    else str(row["cancel_target_order_ref"])
                )
                correlation_key = CtpDispatchCorrelationKey(
                    version=1,
                    account_key=str(row["account_key"]),
                    scope_key=str(row["scope_key"]),
                    trading_day=str(row["trading_day"]),
                    operation=str(row["operation"]),
                    command_id=str(row["command_id"]),
                    request_payload_sha256=str(row["request_payload_sha256"]),
                    reservation_managed_intent_id=str(row["reservation_managed_intent_id"]),
                    managed_action_id=str(row["managed_action_id"]),
                    runtime_order_id=str(row["runtime_order_id"]),
                    order_ref=action_order_ref,
                    cancel_target_exchange_id=(
                        None
                        if row["cancel_target_exchange_id"] is None
                        else str(row["cancel_target_exchange_id"])
                    ),
                    cancel_target_order_sys_id=(
                        None
                        if row["cancel_target_order_sys_id"] is None
                        else str(row["cancel_target_order_sys_id"])
                    ),
                    cancel_target_front_id=(
                        None
                        if row["cancel_target_front_id"] is None
                        else int(row["cancel_target_front_id"])
                    ),
                    cancel_target_session_id=(
                        None
                        if row["cancel_target_session_id"] is None
                        else int(row["cancel_target_session_id"])
                    ),
                    approval_use_id=str(row["approval_use_id"]),
                    approval_digest=str(row["approval_digest"]),
                    session_binding_sha256=str(row["session_binding_sha256"]),
                    session_generation_id=str(row["session_generation_id"]),
                    dispatch_front_id=int(row["dispatch_front_id"]),
                    dispatch_session_id=int(row["dispatch_session_id"]),
                    native_request_id=int(row["native_request_id"]),
                    native_action_ref=(
                        None if row["native_action_ref"] is None else str(row["native_action_ref"])
                    ),
                )
                if (
                    session_binding.get("session_generation_id")
                    != correlation_key.session_generation_id
                    or session_binding.get("dispatch_front_id") != correlation_key.dispatch_front_id
                    or session_binding.get("dispatch_session_id")
                    != correlation_key.dispatch_session_id
                ):
                    raise ValueError("stored CTP session binding differs from correlation")
            elif correlation_version == 0:
                if any(
                    row[name] is not None
                    for name in (
                        "runtime_order_id",
                        "managed_action_id",
                        "session_generation_id",
                        "dispatch_front_id",
                        "dispatch_session_id",
                        "native_request_id",
                        "native_action_ref",
                    )
                ):
                    raise ValueError("legacy CTP command has partial correlation keys")
            else:
                raise ValueError("unsupported stored CTP correlation version")
            if (
                not isinstance(request_payload, dict)
                or not isinstance(session_binding, dict)
                or (
                    native_receipt_payload is not None
                    and not isinstance(native_receipt_payload, dict)
                )
                or (completion_echo is not None and not isinstance(completion_echo, dict))
                or canonical_json(request_payload) != str(row["request_payload_json"])
                or payload_sha256(request_payload) != str(row["request_payload_sha256"])
                or canonical_json(session_binding) != str(row["session_binding_json"])
                or payload_sha256(session_binding) != str(row["session_binding_sha256"])
                or (
                    native_receipt_payload is not None
                    and payload_sha256(native_receipt_payload) != str(row["native_receipt_sha256"])
                )
                or (native_receipt_payload is None and row["native_receipt_sha256"] is not None)
                or (
                    completion_echo is not None
                    and (
                        canonical_json(completion_echo) != str(row["completion_echo_json"])
                        or payload_sha256(completion_echo) != str(row["completion_echo_sha256"])
                        or completion_echo.get("native_receipt_payload") != native_receipt_payload
                        or completion_echo.get("local_queue_receipt_id")
                        != local_queue_receipt_id
                    )
                )
                or (completion_echo is None and row["completion_echo_sha256"] is not None)
                or (
                    str(row["status"]) == "COMPLETED"
                    and (
                        completion_echo is None
                        or native_receipt_payload is None
                        or row["completed_at_ns"] is None
                    )
                )
                or (
                    str(row["status"]) in {"READY", "CLAIMED"}
                    and (
                        native_receipt_payload is not None
                        or completion_echo is not None
                        or row["completed_at_ns"] is not None
                    )
                )
            ):
                raise ValueError("stored CTP command digest mismatch")
            if local_queue_receipt_id is not None:
                status = str(row["status"])
                if status in {"CLAIMED", "UNKNOWN"} and local_queue_receipt_queued is not True:
                    raise ValueError("claimed CTP command lacks a committed queue receipt")
                if local_queue_receipt_queued is False and status != "COMPLETED":
                    raise ValueError("rejected CTP queue receipt is not terminal")
                if status == "COMPLETED":
                    outcome = completion_echo.get("outcome") if completion_echo else None
                    if local_queue_receipt_queued is False and outcome != "REJECTED":
                        raise ValueError("rejected queue receipt has a non-rejected outcome")
                    if local_queue_receipt_queued is True and outcome not in {"QUEUED", "REJECTED"}:
                        raise ValueError("queued CTP command has an invalid completion outcome")
            return CtpDispatchCommand(
                account_key=str(row["account_key"]),
                scope_key=str(row["scope_key"]),
                trading_day=str(row["trading_day"]),
                operation=str(row["operation"]),
                command_id=str(row["command_id"]),
                request_payload=request_payload,
                request_payload_sha256=str(row["request_payload_sha256"]),
                reservation_managed_intent_id=str(row["reservation_managed_intent_id"]),
                order_ref=None if row["order_ref"] is None else str(row["order_ref"]),
                cancel_target_order_ref=(
                    None
                    if row["cancel_target_order_ref"] is None
                    else str(row["cancel_target_order_ref"])
                ),
                cancel_target_exchange_id=(
                    None
                    if row["cancel_target_exchange_id"] is None
                    else str(row["cancel_target_exchange_id"])
                ),
                cancel_target_order_sys_id=(
                    None
                    if row["cancel_target_order_sys_id"] is None
                    else str(row["cancel_target_order_sys_id"])
                ),
                cancel_target_front_id=(
                    None
                    if row["cancel_target_front_id"] is None
                    else int(row["cancel_target_front_id"])
                ),
                cancel_target_session_id=(
                    None
                    if row["cancel_target_session_id"] is None
                    else int(row["cancel_target_session_id"])
                ),
                approval_use_id=str(row["approval_use_id"]),
                approval_digest=str(row["approval_digest"]),
                session_binding=session_binding,
                session_binding_sha256=str(row["session_binding_sha256"]),
                status=str(row["status"]),
                created_at_ns=int(row["created_at_ns"]),
                updated_at_ns=int(row["updated_at_ns"]),
                claimed_at_ns=(None if row["claimed_at_ns"] is None else int(row["claimed_at_ns"])),
                claimed_owner_id=(
                    None if row["claimed_owner_id"] is None else str(row["claimed_owner_id"])
                ),
                claimed_fencing_token=(
                    None
                    if row["claimed_fencing_token"] is None
                    else int(row["claimed_fencing_token"])
                ),
                completed_at_ns=(
                    None if row["completed_at_ns"] is None else int(row["completed_at_ns"])
                ),
                unknown_at_ns=(None if row["unknown_at_ns"] is None else int(row["unknown_at_ns"])),
                unknown_reason=(
                    None if row["unknown_reason"] is None else str(row["unknown_reason"])
                ),
                native_receipt_payload=native_receipt_payload,
                native_receipt_sha256=(
                    None
                    if row["native_receipt_sha256"] is None
                    else str(row["native_receipt_sha256"])
                ),
                completion_echo_sha256=(
                    None
                    if row["completion_echo_sha256"] is None
                    else str(row["completion_echo_sha256"])
                ),
                correlation_key=correlation_key,
                local_queue_receipt_id=local_queue_receipt_id,
                local_queue_receipt_queued=local_queue_receipt_queued,
            )
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise DurableStoreError("stored CTP command is unreadable") from error

    def stage_ctp_dispatch_command(
        self,
        scope: ExecutionScope,
        command_id: str,
        operation: str,
        request_payload: Mapping[str, Any],
        *,
        approval_use_id: str,
        approval_digest: str,
        session_binding: Mapping[str, Any],
        writer_lease: WriterLease,
        managed_intent_id: str | None = None,
        order_ref: str | None = None,
        cancel_target: CtpCancelTarget | None = None,
        managed_action_id: str | None = None,
        session_generation_id: str | None = None,
        dispatch_front_id: int | None = None,
        dispatch_session_id: int | None = None,
        native_request_id: int | None = None,
        native_action_ref: str | None = None,
        local_queue_receipt_id: str | None = None,
    ) -> CtpDispatchCommand:
        """Persist one immutable CTP command; this does not enable dispatch.

        SUBMIT binds the exact previously reserved intent/OrderRef. CANCEL
        binds an exact reserved OrderRef in this scope and trading day. A
        command can become claimable only after a per-day OrderRef seed proof
        and only while the account writer lease remains active.
        """

        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        self._validate_command_identifier(command_id, "command_id")
        self._validate_command_identifier(approval_use_id, "approval_use_id")
        self._validate_sha256(approval_digest, "approval digest")
        if not isinstance(operation, str) or operation not in {"SUBMIT", "CANCEL"}:
            raise ContractValidationError("invalid CTP dispatch operation")
        if session_generation_id is None:
            raise ContractValidationError("typed CTP session generation is required")
        _validate_correlation_text(session_generation_id, "session generation id")
        if type(dispatch_front_id) is not int or dispatch_front_id <= 0:
            raise ContractValidationError("typed CTP dispatch FrontID is required")
        if type(dispatch_session_id) is not int or dispatch_session_id <= 0:
            raise ContractValidationError("typed CTP dispatch SessionID is required")
        if (
            type(native_request_id) is not int
            or native_request_id <= 0
            or native_request_id > 2_147_483_647
        ):
            raise ContractValidationError("typed CTP native RequestID is required")
        if native_action_ref is not None:
            _validate_correlation_text(native_action_ref, "native ActionRef")
        if local_queue_receipt_id is not None and not _is_local_queue_receipt_id(
            local_queue_receipt_id
        ):
            raise ContractValidationError("invalid CTP local queue receipt id")
        if not isinstance(request_payload, Mapping) or not isinstance(session_binding, Mapping):
            raise ContractValidationError(
                "CTP command payload and session binding must be mappings"
            )
        try:
            request_json = canonical_json(dict(request_payload))
            request_value = json.loads(request_json)
            session_json = canonical_json(dict(session_binding))
            session_value = json.loads(session_json)
        except (TypeError, ValueError) as error:
            raise ContractValidationError("CTP command payload is not canonical JSON") from error
        if not request_value or not isinstance(request_value, dict):
            raise ContractValidationError("CTP command request payload must be a non-empty object")
        if not session_value or not isinstance(session_value, dict):
            raise ContractValidationError("CTP session binding must be a non-empty object")
        if (
            session_value.get("session_generation_id") != session_generation_id
            or type(session_value.get("dispatch_front_id")) is not int
            or session_value.get("dispatch_front_id") != dispatch_front_id
            or type(session_value.get("dispatch_session_id")) is not int
            or session_value.get("dispatch_session_id") != dispatch_session_id
        ):
            raise ContractValidationError("typed CTP session keys do not match session binding")
        self._reject_sensitive_command_fields(request_value)
        self._reject_sensitive_command_fields(session_value)
        request_digest = payload_sha256(request_value)
        session_digest = payload_sha256(session_value)

        reservation_managed_intent_id: str
        persisted_order_ref: str | None
        persisted_cancel_target: str | None
        cancel_exchange_id: str | None = None
        cancel_order_sys_id: str | None = None
        cancel_front_id: int | None = None
        cancel_session_id: int | None = None
        if operation == "SUBMIT":
            if managed_intent_id is None or order_ref is None or cancel_target is not None:
                raise ContractValidationError("SUBMIT requires its reserved intent and OrderRef")
            self._validate_command_identifier(managed_intent_id, "managed_intent_id")
            if managed_action_id not in (None, managed_intent_id):
                raise ContractValidationError("submit action id must equal its managed intent id")
            managed_action_id = managed_intent_id
            if native_action_ref is not None:
                raise ContractValidationError("SUBMIT cannot carry a native cancel ActionRef")
            self._validate_ctp_order_ref(order_ref, "OrderRef")
            if (
                type(request_value.get("OrderRef")) is not str
                or request_value.get("OrderRef") != order_ref
            ):
                raise ContractValidationError(
                    "CTP submit payload must echo its exact reserved OrderRef"
                )
            reservation_managed_intent_id = managed_intent_id
            persisted_order_ref = order_ref
            persisted_cancel_target = None
        else:
            if managed_intent_id is not None or order_ref is not None or cancel_target is None:
                raise ContractValidationError("CANCEL requires an exact typed cancel target")
            if managed_action_id is None:
                raise ContractValidationError("CANCEL requires a distinct managed action id")
            self._validate_command_identifier(managed_action_id, "managed_action_id")
            self._validate_ctp_cancel_target(cancel_target, request_value)
            persisted_order_ref = None
            persisted_cancel_target = cancel_target.order_ref
            cancel_exchange_id = cancel_target.exchange_id
            cancel_order_sys_id = cancel_target.order_sys_id
            cancel_front_id = cancel_target.front_id
            cancel_session_id = cancel_target.session_id
            reservation_managed_intent_id = ""

        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            if operation == "SUBMIT":
                reservation = cursor.execute(
                    """
                    SELECT managed_intent_id, runtime_order_id
                    FROM ctp_order_identity_reservations
                    WHERE account_key = ? AND trading_day = ? AND scope_key = ?
                      AND managed_intent_id = ? AND order_ref = ?
                    """,
                    (account_key, trading_day, scope_key, reservation_managed_intent_id, order_ref),
                ).fetchone()
            else:
                reservation = cursor.execute(
                    """
                    SELECT managed_intent_id, runtime_order_id
                    FROM ctp_order_identity_reservations
                    WHERE account_key = ? AND trading_day = ? AND scope_key = ?
                      AND order_ref = ?
                    """,
                    (account_key, trading_day, scope_key, persisted_cancel_target),
                ).fetchone()
            if reservation is None:
                raise ContractValidationError("CTP command has no exact OrderRef reservation")
            reservation_managed_intent_id = str(reservation["managed_intent_id"])
            runtime_order_id = str(reservation["runtime_order_id"])
            if operation == "CANCEL" and managed_action_id == reservation_managed_intent_id:
                raise ContractValidationError(
                    "cancel action id must be distinct from target intent"
                )
            action_order_ref = persisted_order_ref or persisted_cancel_target
            assert action_order_ref is not None
            CtpDispatchCorrelationKey(
                version=1,
                account_key=account_key,
                scope_key=scope_key,
                trading_day=trading_day,
                operation=operation,
                command_id=command_id,
                request_payload_sha256=request_digest,
                reservation_managed_intent_id=reservation_managed_intent_id,
                managed_action_id=str(managed_action_id),
                runtime_order_id=runtime_order_id,
                order_ref=action_order_ref,
                cancel_target_exchange_id=cancel_exchange_id,
                cancel_target_order_sys_id=cancel_order_sys_id,
                cancel_target_front_id=cancel_front_id,
                cancel_target_session_id=cancel_session_id,
                approval_use_id=approval_use_id,
                approval_digest=approval_digest,
                session_binding_sha256=session_digest,
                session_generation_id=str(session_generation_id),
                dispatch_front_id=dispatch_front_id,
                dispatch_session_id=dispatch_session_id,
                native_request_id=native_request_id,
                native_action_ref=native_action_ref,
            )

            existing = cursor.execute(
                "SELECT * FROM ctp_dispatch_commands WHERE account_key = ? AND command_id = ?",
                (account_key, command_id),
            ).fetchone()
            immutable = (
                scope_key,
                trading_day,
                operation,
                request_json,
                request_digest,
                reservation_managed_intent_id,
                persisted_order_ref,
                persisted_cancel_target,
                cancel_exchange_id,
                cancel_order_sys_id,
                cancel_front_id,
                cancel_session_id,
                approval_use_id,
                approval_digest,
                session_json,
                session_digest,
                1,
                runtime_order_id,
                str(managed_action_id),
                str(session_generation_id),
                dispatch_front_id,
                dispatch_session_id,
                native_request_id,
                native_action_ref,
                local_queue_receipt_id,
            )
            if existing is not None:
                stored = (
                    str(existing["scope_key"]),
                    str(existing["trading_day"]),
                    str(existing["operation"]),
                    str(existing["request_payload_json"]),
                    str(existing["request_payload_sha256"]),
                    str(existing["reservation_managed_intent_id"]),
                    None if existing["order_ref"] is None else str(existing["order_ref"]),
                    None
                    if existing["cancel_target_order_ref"] is None
                    else str(existing["cancel_target_order_ref"]),
                    None
                    if existing["cancel_target_exchange_id"] is None
                    else str(existing["cancel_target_exchange_id"]),
                    None
                    if existing["cancel_target_order_sys_id"] is None
                    else str(existing["cancel_target_order_sys_id"]),
                    None
                    if existing["cancel_target_front_id"] is None
                    else int(existing["cancel_target_front_id"]),
                    None
                    if existing["cancel_target_session_id"] is None
                    else int(existing["cancel_target_session_id"]),
                    str(existing["approval_use_id"]),
                    str(existing["approval_digest"]),
                    str(existing["session_binding_json"]),
                    str(existing["session_binding_sha256"]),
                    int(existing["correlation_version"]),
                    None
                    if existing["runtime_order_id"] is None
                    else str(existing["runtime_order_id"]),
                    None
                    if existing["managed_action_id"] is None
                    else str(existing["managed_action_id"]),
                    None
                    if existing["session_generation_id"] is None
                    else str(existing["session_generation_id"]),
                    None
                    if existing["dispatch_front_id"] is None
                    else int(existing["dispatch_front_id"]),
                    None
                    if existing["dispatch_session_id"] is None
                    else int(existing["dispatch_session_id"]),
                    None
                    if existing["native_request_id"] is None
                    else int(existing["native_request_id"]),
                    None
                    if existing["native_action_ref"] is None
                    else str(existing["native_action_ref"]),
                    None
                    if existing["local_queue_receipt_id"] is None
                    else str(existing["local_queue_receipt_id"]),
                )
                if stored != immutable:
                    raise IntentConflictError("CTP command_id conflicts with staged command")
                return self._ctp_dispatch_command_from_row(existing)

            generation_owner = cursor.execute(
                """
                SELECT command_id, session_binding_sha256, dispatch_front_id, dispatch_session_id
                FROM ctp_dispatch_commands
                WHERE account_key = ? AND session_generation_id = ?
                  AND correlation_version = 1
                ORDER BY created_at_ns, command_id LIMIT 1
                """,
                (account_key, session_generation_id),
            ).fetchone()
            if generation_owner is not None and (
                str(generation_owner["session_binding_sha256"]) != session_digest
                or int(generation_owner["dispatch_front_id"]) != dispatch_front_id
                or int(generation_owner["dispatch_session_id"]) != dispatch_session_id
            ):
                raise ContractValidationError(
                    "CTP session generation was reused with another binding"
                )
            action_owner = cursor.execute(
                """
                SELECT command_id FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND managed_action_id = ?
                  AND correlation_version = 1
                """,
                (account_key, scope_key, managed_action_id),
            ).fetchone()
            if action_owner is not None:
                raise IntentConflictError(
                    "CTP managed action id is already bound to another command"
                )
            request_owner = cursor.execute(
                """
                SELECT command_id FROM ctp_dispatch_commands
                WHERE account_key = ? AND session_generation_id = ? AND native_request_id = ?
                  AND correlation_version = 1
                """,
                (account_key, session_generation_id, native_request_id),
            ).fetchone()
            if request_owner is not None:
                raise IntentConflictError(
                    "CTP RequestID is already bound in this session generation"
                )

            approval_owner = cursor.execute(
                """
                SELECT command_id FROM ctp_dispatch_commands
                WHERE account_key = ? AND approval_use_id = ?
                """,
                (account_key, approval_use_id),
            ).fetchone()
            if approval_owner is not None:
                raise IntentConflictError("CTP approval use is already bound to another command")

            cursor.execute(
                """
                INSERT INTO ctp_dispatch_commands(
                    account_key, scope_key, trading_day, operation, command_id,
                    request_payload_json, request_payload_sha256,
                    reservation_managed_intent_id, order_ref, cancel_target_order_ref,
                    cancel_target_exchange_id, cancel_target_order_sys_id,
                    cancel_target_front_id, cancel_target_session_id,
                    approval_use_id, approval_digest, session_binding_json,
                    session_binding_sha256, correlation_version, runtime_order_id,
                    managed_action_id, session_generation_id, dispatch_front_id,
                    dispatch_session_id, native_request_id, native_action_ref,
                    local_queue_receipt_id,
                    status, created_at_ns, updated_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'READY', ?, ?)
                """,
                (
                    account_key,
                    scope_key,
                    trading_day,
                    operation,
                    command_id,
                    request_json,
                    request_digest,
                    reservation_managed_intent_id,
                    persisted_order_ref,
                    persisted_cancel_target,
                    cancel_exchange_id,
                    cancel_order_sys_id,
                    cancel_front_id,
                    cancel_session_id,
                    approval_use_id,
                    approval_digest,
                    session_json,
                    session_digest,
                    1,
                    runtime_order_id,
                    str(managed_action_id),
                    str(session_generation_id),
                    dispatch_front_id,
                    dispatch_session_id,
                    native_request_id,
                    native_action_ref,
                    local_queue_receipt_id,
                    now_ns,
                    now_ns,
                ),
            )
            row = cursor.execute(
                "SELECT * FROM ctp_dispatch_commands WHERE account_key = ? AND command_id = ?",
                (account_key, command_id),
            ).fetchone()
            assert row is not None
            return self._ctp_dispatch_command_from_row(row)

    def read_ctp_dispatch_command(
        self, scope: ExecutionScope, command_id: str
    ) -> CtpDispatchCommand | None:
        """Read a canonical CTP command without changing its dispatch state."""

        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        self._validate_command_identifier(command_id, "command_id")
        with self._lock:
            try:
                row = self._connection.execute(
                    """
                    SELECT * FROM ctp_dispatch_commands
                    WHERE account_key = ? AND scope_key = ? AND command_id = ?
                    """,
                    (account_key, scope_key, command_id),
                ).fetchone()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read CTP dispatch command") from error
        return None if row is None else self._ctp_dispatch_command_from_row(row)

    def read_ctp_dispatch_projection(
        self, scope: ExecutionScope, command_id: str
    ) -> CtpDispatchProjection | None:
        """Read committed callback/reconciliation projections for one command.

        This query is scoped by the durable execution scope and exact command
        ID. It exposes neither the staged request nor session/approval data,
        invokes no verifier, and does not change the outbox. A command resolved
        through reconciliation retains its historical ``UNKNOWN`` status.
        """

        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        self._validate_command_identifier(command_id, "command_id")
        with self._lock:
            try:
                row = self._connection.execute(
                    """
                    SELECT
                        command.*,
                        order_projection.runtime_order_id AS op_runtime_order_id,
                        order_projection.scope_key AS op_scope_key,
                        order_projection.trading_day AS op_trading_day,
                        order_projection.managed_intent_id AS op_managed_intent_id,
                        order_projection.order_ref AS op_order_ref,
                        order_projection.provider_state AS op_provider_state,
                        order_projection.terminal AS op_terminal,
                        order_projection.last_source_kind AS op_source_kind,
                        order_projection.updated_at_ns AS op_updated_at_ns,
                        cancel_projection.managed_action_id AS cp_managed_action_id,
                        cancel_projection.runtime_order_id AS cp_runtime_order_id,
                        cancel_projection.order_ref AS cp_order_ref,
                        cancel_projection.target_exchange_id AS cp_target_exchange_id,
                        cancel_projection.target_order_sys_id AS cp_target_order_sys_id,
                        cancel_projection.target_front_id AS cp_target_front_id,
                        cancel_projection.target_session_id AS cp_target_session_id,
                        cancel_projection.provider_state AS cp_provider_state,
                        cancel_projection.terminal AS cp_terminal,
                        cancel_projection.last_source_kind AS cp_source_kind,
                        cancel_projection.updated_at_ns AS cp_updated_at_ns,
                        resolution.command_id AS resolution_command_id,
                        resolution.operation AS resolution_operation,
                        resolution.correlation_key_sha256 AS resolution_correlation_sha256,
                        resolution.order_terminal_state AS resolution_order_state,
                        resolution.cancel_action_terminal_state AS resolution_cancel_state,
                        resolution.verified_at_ns AS resolution_verified_at_ns,
                        resolution.resolved_at_ns AS resolution_resolved_at_ns
                    FROM ctp_dispatch_commands AS command
                    LEFT JOIN ctp_dispatch_order_projection AS order_projection
                        ON order_projection.account_key = command.account_key
                        AND order_projection.runtime_order_id = command.runtime_order_id
                    LEFT JOIN ctp_dispatch_cancel_projection AS cancel_projection
                        ON cancel_projection.account_key = command.account_key
                        AND cancel_projection.scope_key = command.scope_key
                        AND cancel_projection.managed_action_id = command.managed_action_id
                    LEFT JOIN ctp_dispatch_unknown_resolutions AS resolution
                        ON resolution.account_key = command.account_key
                        AND resolution.scope_key = command.scope_key
                        AND resolution.command_id = command.command_id
                    WHERE command.account_key = ?
                        AND command.scope_key = ?
                        AND command.command_id = ?
                    """,
                    (account_key, scope_key, command_id),
                ).fetchone()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read CTP dispatch projection") from error
        if row is None:
            return None

        command = self._ctp_dispatch_command_from_row(row)
        correlation = command.correlation_key
        try:
            completion_echo_json = row["completion_echo_json"]
            if completion_echo_json is None:
                local_dispatch_outcome = "UNKNOWN" if command.status == "UNKNOWN" else None
            else:
                # The command loader has already checked canonical JSON, digest,
                # and agreement with the persisted native receipt payload.
                completion_echo = json.loads(str(completion_echo_json))
                outcome = completion_echo.get("outcome") if isinstance(completion_echo, dict) else None
                if command.status == "COMPLETED":
                    allowed_outcomes = {"QUEUED", "REJECTED"}
                elif command.status == "UNKNOWN":
                    allowed_outcomes = {"UNKNOWN"}
                else:
                    allowed_outcomes = set()
                if type(outcome) is not str or outcome not in allowed_outcomes:
                    raise ValueError("stored CTP receipt outcome differs from command status")
                local_dispatch_outcome = outcome
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise DurableStoreError("stored CTP local dispatch outcome is unreadable") from error
        if correlation is None:
            if row["resolution_command_id"] is not None:
                raise DurableStoreError("stored CTP dispatch projection is unreadable")
            return CtpDispatchProjection(
                command_id=command.command_id,
                operation=command.operation,
                command_status=command.status,
                local_dispatch_outcome=local_dispatch_outcome,
                unknown_reason=command.unknown_reason,
                submit_action=None,
                cancel_action=None,
                unknown_resolution=None,
                local_queue_receipt_id=command.local_queue_receipt_id,
                local_queue_receipt_queued=command.local_queue_receipt_queued,
            )

        try:
            order_state = CtpProjectedOrderState(
                provider_state=(
                    None if row["op_provider_state"] is None else str(row["op_provider_state"])
                ),
                terminal=None if row["op_terminal"] is None else bool(row["op_terminal"]),
                source_kind=(None if row["op_source_kind"] is None else str(row["op_source_kind"])),
                updated_at_ns=(
                    None if row["op_updated_at_ns"] is None else int(row["op_updated_at_ns"])
                ),
            )
            if row["op_runtime_order_id"] is not None:
                stored_order_identity = (
                    str(row["op_runtime_order_id"]),
                    str(row["op_scope_key"]),
                    str(row["op_trading_day"]),
                    str(row["op_managed_intent_id"]),
                    str(row["op_order_ref"]),
                )
                expected_order_identity = (
                    correlation.runtime_order_id,
                    correlation.scope_key,
                    correlation.trading_day,
                    correlation.reservation_managed_intent_id,
                    correlation.order_ref,
                )
                if stored_order_identity != expected_order_identity:
                    raise ValueError("stored CTP order projection identity differs from command")

            resolution = None
            if row["resolution_command_id"] is not None:
                if (
                    command.status != "UNKNOWN"
                    or str(row["resolution_operation"]) != command.operation
                    or str(row["resolution_correlation_sha256"])
                    != payload_sha256(correlation.to_payload())
                ):
                    raise ValueError("CTP reconciliation resolution differs from command")
                resolution = CtpUnknownResolutionProjection(
                    order_terminal_state=str(row["resolution_order_state"]),
                    cancel_action_terminal_state=(
                        None
                        if row["resolution_cancel_state"] is None
                        else str(row["resolution_cancel_state"])
                    ),
                    verified_at_ns=int(row["resolution_verified_at_ns"]),
                    resolved_at_ns=int(row["resolution_resolved_at_ns"]),
                )
                if order_state.provider_state != resolution.order_terminal_state:
                    raise ValueError("CTP UNKNOWN order resolution lacks its projection")

            if command.operation == "SUBMIT":
                submit_action = CtpSubmitActionProjection(
                    managed_intent_id=correlation.reservation_managed_intent_id,
                    runtime_order_id=correlation.runtime_order_id,
                    order_ref=correlation.order_ref,
                    order_state=order_state,
                )
                cancel_action = None
            else:
                cancel_action_state = row["cp_provider_state"]
                cancel_action = CtpCancelActionProjection(
                    managed_action_id=correlation.managed_action_id,
                    action_state=(
                        None if cancel_action_state is None else str(cancel_action_state)
                    ),
                    terminal=(
                        None if row["cp_terminal"] is None else bool(row["cp_terminal"])
                    ),
                    source_kind=(
                        None if row["cp_source_kind"] is None else str(row["cp_source_kind"])
                    ),
                    updated_at_ns=(
                        None if row["cp_updated_at_ns"] is None else int(row["cp_updated_at_ns"])
                    ),
                    target_order=CtpTargetOrderProjection(
                        managed_intent_id=correlation.reservation_managed_intent_id,
                        runtime_order_id=correlation.runtime_order_id,
                        order_ref=correlation.order_ref,
                        exchange_id=correlation.cancel_target_exchange_id,
                        order_sys_id=correlation.cancel_target_order_sys_id,
                        front_id=correlation.cancel_target_front_id,
                        session_id=correlation.cancel_target_session_id,
                        order_state=order_state,
                    ),
                )
                if row["cp_managed_action_id"] is not None:
                    stored_cancel_identity = (
                        str(row["cp_managed_action_id"]),
                        str(row["cp_runtime_order_id"]),
                        str(row["cp_order_ref"]),
                        str(row["cp_target_exchange_id"]),
                        str(row["cp_target_order_sys_id"]),
                        int(row["cp_target_front_id"]),
                        int(row["cp_target_session_id"]),
                    )
                    expected_cancel_identity = (
                        correlation.managed_action_id,
                        correlation.runtime_order_id,
                        correlation.order_ref,
                        correlation.cancel_target_exchange_id,
                        correlation.cancel_target_order_sys_id,
                        correlation.cancel_target_front_id,
                        correlation.cancel_target_session_id,
                    )
                    if stored_cancel_identity != expected_cancel_identity:
                        raise ValueError("stored CTP cancel projection identity differs from action")
                if resolution is not None and (
                    cancel_action.action_state != resolution.cancel_action_terminal_state
                ):
                    raise ValueError("CTP UNKNOWN cancel resolution lacks its projection")
                submit_action = None

            return CtpDispatchProjection(
                command_id=command.command_id,
                operation=command.operation,
                command_status=command.status,
                local_dispatch_outcome=local_dispatch_outcome,
                unknown_reason=command.unknown_reason,
                submit_action=submit_action,
                cancel_action=cancel_action,
                unknown_resolution=resolution,
                local_queue_receipt_id=command.local_queue_receipt_id,
                local_queue_receipt_queued=command.local_queue_receipt_queued,
            )
        except (ContractValidationError, KeyError, TypeError, ValueError) as error:
            raise DurableStoreError("stored CTP dispatch projection is unreadable") from error

    def record_ctp_dispatch_queue_receipt(
        self,
        scope: ExecutionScope,
        command_id: str,
        queue_receipt: Mapping[str, Any],
        *,
        writer_lease: WriterLease,
    ) -> CtpDispatchCommand:
        """Persist the exact local queue ID before a worker can claim the row.

        The caller must invoke this before publishing an accepted command to
        its worker queue. A rejected queue receipt is completed locally without
        a sender call; a queued receipt makes the same command row eligible for
        the single sender claim. The receipt ID is supplied by the queue owner
        and is never derived from ``command_id``.
        """

        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        self._validate_command_identifier(command_id, "command_id")
        if not isinstance(queue_receipt, Mapping):
            raise ContractValidationError("CTP local queue receipt must be a mapping")
        receipt_id = queue_receipt.get("receipt_id")
        queued = queue_receipt.get("queued")
        if (
            queue_receipt.get("kind") != "command_receipt"
            or type(receipt_id) is not str
            or not _is_local_queue_receipt_id(receipt_id)
            or type(queued) is not bool
        ):
            raise ContractValidationError("CTP local queue receipt is malformed")
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, command_id),
            ).fetchone()
            if row is None:
                raise ContractValidationError("unknown CTP dispatch command")
            command = self._ctp_dispatch_command_from_row(row)
            expected_queue_command = "submit" if command.operation == "SUBMIT" else "cancel"
            if (
                command.local_queue_receipt_id is None
                or receipt_id != command.local_queue_receipt_id
                or queue_receipt.get("command") != expected_queue_command
                or command.correlation_key is None
            ):
                raise ContractValidationError("CTP queue receipt does not match staged command")
            if command.local_queue_receipt_queued is not None:
                if command.local_queue_receipt_queued is queued:
                    return command
                raise IntentConflictError("CTP command already has another queue disposition")
            if command.status != "READY":
                raise InvalidStateTransition("CTP queue receipt requires a READY command")

            if queued:
                cursor.execute(
                    """
                    UPDATE ctp_dispatch_commands
                    SET local_queue_receipt_queued = 1, updated_at_ns = ?
                    WHERE account_key = ? AND scope_key = ? AND command_id = ?
                      AND status = 'READY' AND local_queue_receipt_queued IS NULL
                    """,
                    (now_ns, account_key, scope_key, command_id),
                )
                if cursor.rowcount != 1:
                    raise InvalidStateTransition("CTP queue-ready transition changed")
            else:
                # Retain only the four public queue fields. Queue diagnostics
                # may contain operator/provider data and are not command facts.
                safe_queue_receipt = {
                    "kind": "command_receipt",
                    "command": expected_queue_command,
                    "receipt_id": receipt_id,
                    "queued": False,
                }
                receipt = CtpDispatchReceipt(
                    receipt_type="ctp_dispatch_receipt.v2",
                    command_id=command.command_id,
                    account_key=command.account_key,
                    scope_key=command.scope_key,
                    trading_day=command.trading_day,
                    operation=command.operation,
                    request_payload_sha256=command.request_payload_sha256,
                    reservation_managed_intent_id=command.reservation_managed_intent_id,
                    order_ref=command.order_ref,
                    cancel_target_order_ref=command.cancel_target_order_ref,
                    cancel_target_exchange_id=command.cancel_target_exchange_id,
                    cancel_target_order_sys_id=command.cancel_target_order_sys_id,
                    cancel_target_front_id=command.cancel_target_front_id,
                    cancel_target_session_id=command.cancel_target_session_id,
                    approval_use_id=command.approval_use_id,
                    approval_digest=command.approval_digest,
                    session_binding_sha256=command.session_binding_sha256,
                    outcome="REJECTED",
                    native_receipt_payload=safe_queue_receipt,
                    correlation_key=command.correlation_key,
                    local_queue_receipt_id=receipt_id,
                )
                native_json, native_digest, echo_json = self._ctp_dispatch_receipt_payload(receipt)
                echo_digest = payload_sha256(json.loads(echo_json))
                cursor.execute(
                    """
                    UPDATE ctp_dispatch_commands
                    SET local_queue_receipt_queued = 0, status = 'COMPLETED',
                        updated_at_ns = ?, completed_at_ns = ?,
                        native_receipt_payload_json = ?, native_receipt_sha256 = ?,
                        completion_echo_json = ?, completion_echo_sha256 = ?
                    WHERE account_key = ? AND scope_key = ? AND command_id = ?
                      AND status = 'READY' AND local_queue_receipt_queued IS NULL
                    """,
                    (
                        now_ns,
                        now_ns,
                        native_json,
                        native_digest,
                        echo_json,
                        echo_digest,
                        account_key,
                        scope_key,
                        command_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise InvalidStateTransition("CTP local rejection transition changed")
            updated = cursor.execute(
                "SELECT * FROM ctp_dispatch_commands WHERE account_key = ? AND command_id = ?",
                (account_key, command_id),
            ).fetchone()
            assert updated is not None
            return self._ctp_dispatch_command_from_row(updated)

    def claim_ctp_dispatch_command(
        self,
        scope: ExecutionScope,
        command_id: str,
        *,
        writer_lease: WriterLease,
        authority_verifier: CtpDispatchAuthorityVerifier,
        required_local_queue_receipt_id: str | None = None,
    ) -> CtpDispatchCommand | None:
        """Verify and condition-claim one READY command after the seed gate.

        The verifier must freshly bind the exact staged action and its current
        approval/source evidence. Its callback, one-use consumption row, and
        READY-to-CLAIMED transition share one durable transaction. Approval
        digests stored on commands or echoed by receipts are not authority.

        This remains a local admission contract only: the verifier is injected,
        and no provider/SDK is called here. The caller-supplied watermark is a
        local collision floor, not external account-writer fencing or native
        login evidence.
        """

        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        self._validate_command_identifier(command_id, "command_id")
        if required_local_queue_receipt_id is not None and not _is_local_queue_receipt_id(
            required_local_queue_receipt_id
        ):
            raise ContractValidationError("invalid required CTP local queue receipt id")
        with self._transaction() as cursor:
            verification_started_ns = time.time_ns()
            self._assert_active_writer_lease(
                cursor, scope, writer_lease, now_ns=verification_started_ns
            )
            row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, command_id),
            ).fetchone()
            if row is None or str(row["status"]) != "READY":
                return None
            if row["local_queue_receipt_id"] is not None and row[
                "local_queue_receipt_queued"
            ] != 1:
                return None
            if required_local_queue_receipt_id is not None and (
                row["local_queue_receipt_id"] != required_local_queue_receipt_id
                or row["local_queue_receipt_queued"] != 1
            ):
                return None
            unresolved = cursor.execute(
                """
                SELECT command_id, status,
                       EXISTS (
                           SELECT 1 FROM ctp_dispatch_callback_source_lifecycle_fences AS fence
                           WHERE fence.account_key = ctp_dispatch_commands.account_key
                       ) AS callback_source_fenced
                FROM ctp_dispatch_commands
                WHERE account_key = ? AND (
                    status = 'CLAIMED'
                    OR (status = 'UNKNOWN' AND NOT EXISTS (
                        SELECT 1 FROM ctp_dispatch_unknown_resolutions AS resolution
                        WHERE resolution.account_key = ctp_dispatch_commands.account_key
                          AND resolution.command_id = ctp_dispatch_commands.command_id
                    ))
                    OR EXISTS (
                        SELECT 1 FROM ctp_dispatch_callback_source_lifecycle_fences AS fence
                        WHERE fence.account_key = ctp_dispatch_commands.account_key
                    )
                )
                ORDER BY created_at_ns, command_id LIMIT 1
                """,
                (account_key,),
            ).fetchone()
            if unresolved is not None:
                if bool(unresolved["callback_source_fenced"]):
                    raise InvalidStateTransition(
                        "account has CTP callback source lifecycle fence: "
                        + str(unresolved["command_id"])
                    )
                raise InvalidStateTransition(
                    "account has unresolved CTP dispatch command: " + str(unresolved["command_id"])
                )
            watermark = cursor.execute(
                """
                SELECT native_max_order_ref, legacy_ledger_max_order_ref, updated_at_ns
                FROM ctp_order_ref_watermarks
                WHERE account_key = ? AND trading_day = ?
                """,
                (account_key, trading_day),
            ).fetchone()
            if watermark is None:
                raise ContractValidationError("CTP dispatch is staged-only until OrderRef seed")
            command_ref = (
                str(row["order_ref"])
                if row["order_ref"] is not None
                else str(row["cancel_target_order_ref"])
            )
            if (
                type(row["dispatch_front_id"]) is not int
                or type(row["dispatch_session_id"]) is not int
                or not isinstance(row["session_generation_id"], str)
            ):
                raise ContractValidationError("CTP command lacks exact session generation keys")
            self._require_ctp_order_ref_cutover_session(
                cursor,
                account_key=account_key,
                trading_day=trading_day,
                scope_key=scope_key,
                session_generation_id=str(row["session_generation_id"]),
                native_front_id=int(row["dispatch_front_id"]),
                native_session_id=int(row["dispatch_session_id"]),
                order_ref=command_ref,
            )
            reservation = cursor.execute(
                """
                SELECT created_at_ns FROM ctp_order_identity_reservations
                WHERE account_key = ? AND scope_key = ? AND trading_day = ?
                  AND managed_intent_id = ? AND order_ref = ?
                """,
                (
                    account_key,
                    scope_key,
                    trading_day,
                    str(row["reservation_managed_intent_id"]),
                    command_ref,
                ),
            ).fetchone()
            seeded_floor = max(
                int(str(watermark["native_max_order_ref"])),
                int(str(watermark["legacy_ledger_max_order_ref"])),
            )
            if (
                reservation is None
                or int(command_ref) <= seeded_floor
                or int(reservation["created_at_ns"]) < int(watermark["updated_at_ns"])
            ):
                raise ContractValidationError(
                    "CTP OrderRef predates or conflicts with the seeded watermark"
                )
            command = self._ctp_dispatch_command_from_row(row)
            if command.correlation_key is None:
                raise ContractValidationError("CTP command lacks versioned correlation keys")
            authority = self._verify_ctp_dispatch_authority(
                authority_verifier, command, now_ns=verification_started_ns
            )
            claim_now_ns = time.time_ns()
            self._assert_active_writer_lease(cursor, scope, writer_lease, now_ns=claim_now_ns)
            if authority.expires_at_ns <= claim_now_ns:
                raise ContractValidationError("CTP dispatch authority is expired")
            if claim_now_ns < authority.verified_at_ns:
                raise ContractValidationError("CTP dispatch authority clock moved backwards")
            cursor.execute(
                """
                UPDATE ctp_dispatch_commands
                SET status = 'CLAIMED', updated_at_ns = ?, claimed_at_ns = ?,
                    claimed_owner_id = ?, claimed_fencing_token = ?
                WHERE account_key = ? AND scope_key = ? AND command_id = ? AND status = 'READY'
                """,
                (
                    claim_now_ns,
                    claim_now_ns,
                    writer_lease.owner_id,
                    writer_lease.fencing_token,
                    account_key,
                    scope_key,
                    command_id,
                ),
            )
            if cursor.rowcount != 1:
                return None
            cursor.execute(
                """
                INSERT INTO ctp_dispatch_authority_uses (
                    account_key, approval_use_id, command_id, command_binding_sha256,
                    approval_digest, source_digest_sha256, verifier_id, verified_at_ns,
                    expires_at_ns, writer_owner_id, fencing_token
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_key,
                    authority.approval_use_id,
                    command.command_id,
                    authority.command_binding_sha256,
                    authority.approval_digest,
                    authority.source_digest_sha256,
                    authority.verifier_id,
                    authority.verified_at_ns,
                    authority.expires_at_ns,
                    writer_lease.owner_id,
                    writer_lease.fencing_token,
                ),
            )
            claimed = cursor.execute(
                "SELECT * FROM ctp_dispatch_commands WHERE account_key = ? AND command_id = ?",
                (account_key, command_id),
            ).fetchone()
            assert claimed is not None
            return self._ctp_dispatch_command_from_row(claimed)

    @staticmethod
    def _ctp_dispatch_receipt_payload(receipt: CtpDispatchReceipt) -> tuple[str, str, str]:
        if (
            type(receipt) is not CtpDispatchReceipt
            or receipt.receipt_type != "ctp_dispatch_receipt.v2"
        ):
            raise ContractValidationError("invalid typed CTP dispatch receipt")
        if not isinstance(receipt.outcome, str) or receipt.outcome not in {
            "QUEUED",
            "REJECTED",
            "UNKNOWN",
        }:
            raise ContractValidationError("invalid CTP dispatch receipt outcome")
        if not isinstance(receipt.native_receipt_payload, Mapping):
            raise ContractValidationError("native CTP receipt payload must be a mapping")
        if receipt.local_queue_receipt_id is not None and not _is_local_queue_receipt_id(
            receipt.local_queue_receipt_id
        ):
            raise ContractValidationError("invalid CTP local queue receipt echo")
        key = receipt.correlation_key
        if type(key) is not CtpDispatchCorrelationKey:
            raise ContractValidationError("typed CTP dispatch correlation echo is required")
        if (
            receipt.command_id != key.command_id
            or receipt.account_key != key.account_key
            or receipt.scope_key != key.scope_key
            or receipt.trading_day != key.trading_day
            or receipt.operation != key.operation
            or receipt.request_payload_sha256 != key.request_payload_sha256
            or receipt.reservation_managed_intent_id != key.reservation_managed_intent_id
            or receipt.order_ref != (key.order_ref if key.operation == "SUBMIT" else None)
            or receipt.cancel_target_order_ref
            != (key.order_ref if key.operation == "CANCEL" else None)
            or receipt.cancel_target_exchange_id != key.cancel_target_exchange_id
            or receipt.cancel_target_order_sys_id != key.cancel_target_order_sys_id
            or receipt.cancel_target_front_id != key.cancel_target_front_id
            or receipt.cancel_target_session_id != key.cancel_target_session_id
            or receipt.approval_use_id != key.approval_use_id
            or receipt.approval_digest != key.approval_digest
            or receipt.session_binding_sha256 != key.session_binding_sha256
        ):
            raise ContractValidationError("CTP dispatch receipt correlation echo is inconsistent")
        try:
            native_json = canonical_json(dict(receipt.native_receipt_payload))
            native_value = json.loads(native_json)
            if not isinstance(native_value, dict) or not native_value:
                raise ContractValidationError("native CTP receipt payload must be non-empty")
            SqliteExecutionStore._reject_sensitive_command_fields(native_value)
            echo = {
                "receipt_type": receipt.receipt_type,
                "command_id": receipt.command_id,
                "account_key": receipt.account_key,
                "scope_key": receipt.scope_key,
                "trading_day": receipt.trading_day,
                "operation": receipt.operation,
                "request_payload_sha256": receipt.request_payload_sha256,
                "reservation_managed_intent_id": receipt.reservation_managed_intent_id,
                "order_ref": receipt.order_ref,
                "cancel_target_order_ref": receipt.cancel_target_order_ref,
                "cancel_target_exchange_id": receipt.cancel_target_exchange_id,
                "cancel_target_order_sys_id": receipt.cancel_target_order_sys_id,
                "cancel_target_front_id": receipt.cancel_target_front_id,
                "cancel_target_session_id": receipt.cancel_target_session_id,
                "approval_use_id": receipt.approval_use_id,
                "approval_digest": receipt.approval_digest,
                "session_binding_sha256": receipt.session_binding_sha256,
                "local_queue_receipt_id": receipt.local_queue_receipt_id,
                "correlation_key": key.to_payload(),
                "outcome": receipt.outcome,
                "native_receipt_payload": native_value,
            }
            echo_json = canonical_json(echo)
        except (TypeError, ValueError) as error:
            raise ContractValidationError("CTP dispatch receipt is not canonical JSON") from error
        return native_json, payload_sha256(native_value), echo_json

    def complete_ctp_dispatch_command(
        self,
        scope: ExecutionScope,
        receipt: CtpDispatchReceipt,
        *,
        writer_lease: WriterLease,
    ) -> CtpDispatchCommand:
        """Persist a local receipt only when every command binding echoes.

        A ``QUEUED`` receipt completes the local dispatch receipt step only. It
        never advances a provider order or cancel-action state projection.
        """

        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        native_json, native_digest, echo_json = self._ctp_dispatch_receipt_payload(receipt)
        echo_digest = payload_sha256(json.loads(echo_json))
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, receipt.command_id),
            ).fetchone()
            if row is None:
                raise ContractValidationError("unknown CTP dispatch command")
            command = self._ctp_dispatch_command_from_row(row)
            expected = (
                command.command_id,
                command.account_key,
                command.scope_key,
                command.trading_day,
                command.operation,
                command.request_payload_sha256,
                command.reservation_managed_intent_id,
                command.order_ref,
                command.cancel_target_order_ref,
                command.cancel_target_exchange_id,
                command.cancel_target_order_sys_id,
                command.cancel_target_front_id,
                command.cancel_target_session_id,
                command.approval_use_id,
                command.approval_digest,
                command.session_binding_sha256,
                command.correlation_key,
                command.local_queue_receipt_id,
            )
            actual = (
                receipt.command_id,
                receipt.account_key,
                receipt.scope_key,
                receipt.trading_day,
                receipt.operation,
                receipt.request_payload_sha256,
                receipt.reservation_managed_intent_id,
                receipt.order_ref,
                receipt.cancel_target_order_ref,
                receipt.cancel_target_exchange_id,
                receipt.cancel_target_order_sys_id,
                receipt.cancel_target_front_id,
                receipt.cancel_target_session_id,
                receipt.approval_use_id,
                receipt.approval_digest,
                receipt.session_binding_sha256,
                receipt.correlation_key,
                receipt.local_queue_receipt_id,
            )
            if actual != expected:
                raise ContractValidationError("CTP dispatch receipt echo does not match command")
            if command.local_queue_receipt_id is not None and (
                command.local_queue_receipt_queued is not True
            ):
                raise InvalidStateTransition("CTP local queue receipt is not ready for dispatch")
            if command.status in {"COMPLETED", "UNKNOWN"} and command.completion_echo_sha256:
                if command.completion_echo_sha256 != echo_digest:
                    raise IntentConflictError("CTP dispatch command already has another receipt")
                return command
            if command.status != "CLAIMED":
                raise InvalidStateTransition("CTP dispatch command is not CLAIMED")
            if (
                str(row["claimed_owner_id"]) != writer_lease.owner_id
                or int(row["claimed_fencing_token"]) != writer_lease.fencing_token
            ):
                raise WriterLeaseUnavailable()
            if receipt.outcome == "UNKNOWN":
                next_state = "UNKNOWN"
                cursor.execute(
                    """
                    UPDATE ctp_dispatch_commands
                    SET status = ?, updated_at_ns = ?, unknown_at_ns = ?,
                        unknown_reason = ?, native_receipt_payload_json = ?,
                        native_receipt_sha256 = ?, completion_echo_json = ?,
                        completion_echo_sha256 = ?
                    WHERE account_key = ? AND command_id = ? AND status = 'CLAIMED'
                    """,
                    (
                        next_state,
                        now_ns,
                        now_ns,
                        "native_receipt_unknown",
                        native_json,
                        native_digest,
                        echo_json,
                        echo_digest,
                        account_key,
                        receipt.command_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    UPDATE ctp_dispatch_commands
                    SET status = 'COMPLETED', updated_at_ns = ?, completed_at_ns = ?,
                        native_receipt_payload_json = ?, native_receipt_sha256 = ?,
                        completion_echo_json = ?, completion_echo_sha256 = ?
                    WHERE account_key = ? AND command_id = ? AND status = 'CLAIMED'
                    """,
                    (
                        now_ns,
                        now_ns,
                        native_json,
                        native_digest,
                        echo_json,
                        echo_digest,
                        account_key,
                        receipt.command_id,
                    ),
                )
            if cursor.rowcount != 1:
                raise InvalidStateTransition("CTP dispatch command claim changed")
            completed = cursor.execute(
                "SELECT * FROM ctp_dispatch_commands WHERE account_key = ? AND command_id = ?",
                (account_key, receipt.command_id),
            ).fetchone()
            assert completed is not None
            return self._ctp_dispatch_command_from_row(completed)

    @staticmethod
    def _ctp_dispatch_has_open_account_fence(cursor: sqlite3.Cursor, account_key: str) -> bool:
        row = cursor.execute(
            """
            SELECT 1 FROM ctp_dispatch_commands
            WHERE account_key = ? AND (
                status = 'CLAIMED'
                OR (status = 'UNKNOWN' AND NOT EXISTS (
                    SELECT 1 FROM ctp_dispatch_unknown_resolutions AS resolution
                    WHERE resolution.account_key = ctp_dispatch_commands.account_key
                      AND resolution.command_id = ctp_dispatch_commands.command_id
                ))
                OR EXISTS (
                    SELECT 1 FROM ctp_dispatch_callback_source_lifecycle_fences AS fence
                    WHERE fence.account_key = ctp_dispatch_commands.account_key
                )
            )
            LIMIT 1
            """,
            (account_key,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _verify_ctp_dispatch_callback(
        verifier: CtpDispatchCallbackVerifier,
        command: CtpDispatchCommand,
        callback: CtpDispatchCallbackKey,
        callback_payload: Mapping[str, Any],
        callback_payload_sha256: str,
        *,
        now_ns: int,
    ) -> CtpVerifiedCallbackEvidence:
        verify_callback = getattr(verifier, "verify_callback", None)
        if not callable(verify_callback):
            raise ContractValidationError("trusted CTP callback verifier is required")
        verification_failed = False
        evidence: CtpVerifiedCallbackEvidence | None = None
        try:
            evidence = verify_callback(command, callback, callback_payload, now_ns=now_ns)
        except Exception:
            # Raise outside the except block so verifier exceptions and payloads
            # are absent from both the public message and implicit context.
            verification_failed = True
        if verification_failed:
            raise ContractValidationError("trusted CTP callback verification failed")
        if type(evidence) is not CtpVerifiedCallbackEvidence:
            raise ContractValidationError("invalid typed CTP callback verification result")
        if (
            evidence.callback_key != callback
            or evidence.callback_payload_sha256 != callback_payload_sha256
            or evidence.callback_key.correlation_key != command.correlation_key
        ):
            raise ContractValidationError("verified CTP callback binding does not match command")
        SqliteExecutionStore._validate_ctp_callback_projection_state(callback, evidence)
        if evidence.verified_at_ns != now_ns:
            raise ContractValidationError("CTP callback source was not freshly verified")
        return evidence

    @staticmethod
    def _validate_ctp_callback_projection_state(
        callback: CtpDispatchCallbackKey, evidence: CtpVerifiedCallbackEvidence
    ) -> None:
        if callback.correlation_key.operation == "SUBMIT":
            if evidence.projection_state not in _CTP_ORDER_PROJECTION_STATES:
                raise ContractValidationError("invalid submit callback projection state")
            if callback.callback_family == "TRADE" and evidence.projection_state not in {
                "PARTIALLY_FILLED",
                "FILLED",
            }:
                raise ContractValidationError("trade callback has a non-trade projection state")
        elif evidence.projection_state not in _CTP_CANCEL_ACTION_STATES:
            raise ContractValidationError("invalid cancel-action projection state")

    @staticmethod
    def _upsert_ctp_order_projection(
        cursor: sqlite3.Cursor,
        key: CtpDispatchCorrelationKey,
        state: str,
        source_digest_sha256: str,
        *,
        source_kind: str,
        updated_at_ns: int,
        callback_key: CtpDispatchCallbackKey | None = None,
    ) -> None:
        if key.operation not in {"SUBMIT", "CANCEL"} or state not in _CTP_ORDER_PROJECTION_STATES:
            raise ContractValidationError("invalid CTP order projection")
        current = cursor.execute(
            """
            SELECT * FROM ctp_dispatch_order_projection
            WHERE account_key = ? AND runtime_order_id = ?
            """,
            (key.account_key, key.runtime_order_id),
        ).fetchone()
        if current is not None:
            identity = (
                str(current["scope_key"]),
                str(current["trading_day"]),
                str(current["managed_intent_id"]),
                str(current["order_ref"]),
            )
            expected_identity = (
                key.scope_key,
                key.trading_day,
                key.reservation_managed_intent_id,
                key.order_ref,
            )
            if identity != expected_identity:
                raise IntentConflictError(
                    "CTP order projection identity conflicts with correlation"
                )
            current_state = str(current["provider_state"])
            allowed = {
                "ACKNOWLEDGED": _CTP_ORDER_PROJECTION_STATES,
                "PARTIALLY_FILLED": {"PARTIALLY_FILLED", "FILLED", "CANCELLED"},
                "FILLED": {"FILLED"},
                "CANCELLED": {"CANCELLED"},
                "REJECTED": {"REJECTED"},
            }
            if state not in allowed[current_state]:
                raise InvalidStateTransition(
                    "CTP order projection cannot regress or replace terminal"
                )
        is_terminal = int(state in _CTP_ORDER_TERMINAL_STATES)
        cursor.execute(
            """
            INSERT INTO ctp_dispatch_order_projection(
                account_key, runtime_order_id, scope_key, trading_day,
                managed_intent_id, order_ref, provider_state, terminal,
                correlation_key_sha256, source_digest_sha256, last_source_kind,
                last_event_generation_id, last_event_stream_id, last_event_id, updated_at_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_key, runtime_order_id) DO UPDATE SET
                provider_state = excluded.provider_state,
                terminal = excluded.terminal,
                correlation_key_sha256 = excluded.correlation_key_sha256,
                source_digest_sha256 = excluded.source_digest_sha256,
                last_source_kind = excluded.last_source_kind,
                last_event_generation_id = excluded.last_event_generation_id,
                last_event_stream_id = excluded.last_event_stream_id,
                last_event_id = excluded.last_event_id,
                updated_at_ns = excluded.updated_at_ns
            """,
            (
                key.account_key,
                key.runtime_order_id,
                key.scope_key,
                key.trading_day,
                key.reservation_managed_intent_id,
                key.order_ref,
                state,
                is_terminal,
                payload_sha256(key.to_payload()),
                source_digest_sha256,
                source_kind,
                None if callback_key is None else key.session_generation_id,
                None if callback_key is None else callback_key.stream_id,
                None if callback_key is None else callback_key.event_id,
                updated_at_ns,
            ),
        )

    @staticmethod
    def _upsert_ctp_cancel_projection(
        cursor: sqlite3.Cursor,
        key: CtpDispatchCorrelationKey,
        state: str,
        source_digest_sha256: str,
        *,
        source_kind: str,
        updated_at_ns: int,
        callback_key: CtpDispatchCallbackKey | None = None,
    ) -> None:
        if key.operation != "CANCEL" or state not in _CTP_CANCEL_ACTION_STATES:
            raise ContractValidationError("invalid CTP cancel-action projection")
        current = cursor.execute(
            """
            SELECT * FROM ctp_dispatch_cancel_projection
            WHERE account_key = ? AND scope_key = ? AND managed_action_id = ?
            """,
            (key.account_key, key.scope_key, key.managed_action_id),
        ).fetchone()
        if current is not None:
            identity = (
                str(current["runtime_order_id"]),
                str(current["managed_action_id"]),
                str(current["order_ref"]),
                str(current["target_exchange_id"]),
                str(current["target_order_sys_id"]),
                int(current["target_front_id"]),
                int(current["target_session_id"]),
            )
            expected = (
                key.runtime_order_id,
                key.managed_action_id,
                key.order_ref,
                key.cancel_target_exchange_id,
                key.cancel_target_order_sys_id,
                key.cancel_target_front_id,
                key.cancel_target_session_id,
            )
            if identity != expected:
                raise IntentConflictError("CTP cancel projection identity conflicts with action")
            current_state = str(current["provider_state"])
            allowed = {
                "ACKNOWLEDGED": _CTP_CANCEL_ACTION_STATES,
                "REJECTED": {"REJECTED"},
                "TERMINAL": {"TERMINAL"},
            }
            if state not in allowed[current_state]:
                raise InvalidStateTransition(
                    "CTP cancel-action projection cannot regress or replace terminal"
                )
        is_terminal = int(state in _CTP_CANCEL_ACTION_TERMINAL_STATES)
        cursor.execute(
            """
            INSERT INTO ctp_dispatch_cancel_projection(
                account_key, scope_key, managed_action_id, runtime_order_id,
                order_ref, target_exchange_id, target_order_sys_id,
                target_front_id, target_session_id, provider_state, terminal,
                correlation_key_sha256, source_digest_sha256, last_source_kind,
                last_event_generation_id, last_event_stream_id, last_event_id, updated_at_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_key, scope_key, managed_action_id) DO UPDATE SET
                provider_state = excluded.provider_state,
                terminal = excluded.terminal,
                correlation_key_sha256 = excluded.correlation_key_sha256,
                source_digest_sha256 = excluded.source_digest_sha256,
                last_source_kind = excluded.last_source_kind,
                last_event_generation_id = excluded.last_event_generation_id,
                last_event_stream_id = excluded.last_event_stream_id,
                last_event_id = excluded.last_event_id,
                updated_at_ns = excluded.updated_at_ns
            """,
            (
                key.account_key,
                key.scope_key,
                key.managed_action_id,
                key.runtime_order_id,
                key.order_ref,
                key.cancel_target_exchange_id,
                key.cancel_target_order_sys_id,
                key.cancel_target_front_id,
                key.cancel_target_session_id,
                state,
                is_terminal,
                payload_sha256(key.to_payload()),
                source_digest_sha256,
                source_kind,
                None if callback_key is None else key.session_generation_id,
                None if callback_key is None else callback_key.stream_id,
                None if callback_key is None else callback_key.event_id,
                updated_at_ns,
            ),
        )

    def create_ctp_callback_source_lifecycle_fence(
        self,
        scope: ExecutionScope,
        command_id: str,
        *,
        writer_lease: WriterLease,
    ) -> str:
        """Persist a fail-closed fence before polling the callback source.

        This immutable account fence covers the whole source-consumer
        lifecycle. There is no resolution API: callbacks, timeout, close,
        process exit, or verifier results cannot prove that no later source
        event exists. The fence blocks future account claims permanently.
        It does not grant callback trust.
        """

        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        self._validate_command_identifier(command_id, "command_id")
        source_lifecycle_fence_id = uuid.uuid4().hex
        with self._transaction() as cursor:
            now_ns = time.time_ns()
            self._assert_active_writer_lease(cursor, scope, writer_lease, now_ns=now_ns)
            row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, command_id),
            ).fetchone()
            if row is None:
                raise ContractValidationError("unknown CTP dispatch command")
            command = self._ctp_dispatch_command_from_row(row)
            if command.status not in {"CLAIMED", "COMPLETED", "UNKNOWN"}:
                raise InvalidStateTransition(
                    "callback ingestion cannot begin for an undispatched CTP command"
                )
            if command.correlation_key is None:
                raise ContractValidationError("CTP command lacks versioned correlation keys")
            existing = cursor.execute(
                """
                SELECT fence.source_lifecycle_fence_id
                FROM ctp_dispatch_callback_source_lifecycle_fences AS fence
                WHERE fence.account_key = ?
                LIMIT 1
                """,
                (account_key,),
            ).fetchone()
            if existing is not None:
                raise InvalidStateTransition(
                    "account has an existing CTP callback source lifecycle fence"
                )
            cursor.execute(
                """
                INSERT INTO ctp_dispatch_callback_source_lifecycle_fences(
                    account_key, scope_key, command_id, source_lifecycle_fence_id,
                    correlation_key_sha256, session_binding_sha256, created_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_key,
                    scope_key,
                    command_id,
                    source_lifecycle_fence_id,
                    payload_sha256(command.correlation_key.to_payload()),
                    command.session_binding_sha256,
                    now_ns,
                ),
            )
        return source_lifecycle_fence_id

    def apply_ctp_verified_dispatch_callback(
        self,
        scope: ExecutionScope,
        command_id: str,
        callback: CtpDispatchCallbackKey,
        callback_payload: Mapping[str, Any],
        *,
        writer_lease: WriterLease,
        callback_verifier: CtpDispatchCallbackVerifier | None = None,
        source_lifecycle_fence_id: str | None = None,
    ) -> CtpDispatchCallbackApplyResult:
        """Verify one callback, then atomically append, dedupe, and project it.

        The verifier must authenticate the callback's local/native source and
        return a fresh exact binding. The default verifier always rejects.
        Structural matching and digest equality alone never confer trust. A
        callback on an UNKNOWN command updates only its typed projection; the
        UNKNOWN account fence remains until reconciliation is separately
        attested by :meth:`resolve_unknown_ctp_dispatch_command`.
        """

        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        self._validate_command_identifier(command_id, "command_id")
        if source_lifecycle_fence_id is not None and not _is_local_queue_receipt_id(
            source_lifecycle_fence_id
        ):
            raise ContractValidationError("invalid CTP callback source lifecycle fence id")
        if type(callback) is not CtpDispatchCallbackKey:
            raise ContractValidationError("typed CTP callback correlation key is required")
        if not isinstance(callback_payload, Mapping):
            raise ContractValidationError("CTP callback payload must be a mapping")
        callback_payload_invalid = False
        callback_payload_json = ""
        callback_payload_value: Any = None
        try:
            callback_payload_json = canonical_json(dict(callback_payload))
            callback_payload_value = json.loads(callback_payload_json)
        except (TypeError, ValueError):
            callback_payload_invalid = True
        if callback_payload_invalid:
            raise ContractValidationError("CTP callback payload is not canonical JSON")
        if not isinstance(callback_payload_value, dict) or not callback_payload_value:
            raise ContractValidationError("CTP callback payload must be a non-empty object")
        self._reject_sensitive_command_fields(callback_payload_value)
        callback_payload_digest = payload_sha256(callback_payload_value)
        staged = self.read_ctp_dispatch_command(scope, command_id)
        if staged is None:
            raise ContractValidationError("unknown CTP dispatch command")
        if staged.status not in {"CLAIMED", "COMPLETED", "UNKNOWN"}:
            raise InvalidStateTransition("callback cannot apply to an undispatched CTP command")
        require_ctp_dispatch_callback_match(staged, callback)
        verification_started_ns = time.time_ns()
        evidence = self._verify_ctp_dispatch_callback(
            _REJECT_CTP_DISPATCH_VERIFIER if callback_verifier is None else callback_verifier,
            staged,
            callback,
            callback_payload_value,
            callback_payload_digest,
            now_ns=verification_started_ns,
        )
        callback_json = canonical_json(callback.to_payload())
        callback_digest = payload_sha256(json.loads(callback_json))
        correlation_digest = payload_sha256(staged.correlation_key.to_payload())
        with self._transaction() as cursor:
            applied_at_ns = time.time_ns()
            self._assert_active_writer_lease(cursor, scope, writer_lease, now_ns=applied_at_ns)
            if evidence.expires_at_ns <= applied_at_ns:
                raise ContractValidationError(
                    "verified CTP callback evidence expired before commit"
                )
            row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, command_id),
            ).fetchone()
            if row is None:
                raise ContractValidationError("unknown CTP dispatch command")
            current = self._ctp_dispatch_command_from_row(row)
            if (
                current.status not in {"CLAIMED", "COMPLETED", "UNKNOWN"}
                or current.correlation_key != staged.correlation_key
            ):
                raise InvalidStateTransition("CTP callback command changed during verification")
            if source_lifecycle_fence_id is not None:
                guard = cursor.execute(
                    """
                    SELECT * FROM ctp_dispatch_callback_source_lifecycle_fences
                    WHERE account_key = ? AND scope_key = ? AND command_id = ?
                      AND source_lifecycle_fence_id = ?
                    """,
                    (account_key, scope_key, command_id, source_lifecycle_fence_id),
                ).fetchone()
                if (
                    guard is None
                    or str(guard["correlation_key_sha256"]) != correlation_digest
                    or str(guard["session_binding_sha256"])
                    != staged.session_binding_sha256
                ):
                    raise ContractValidationError(
                        "CTP callback source lifecycle fence does not match command"
                    )
            duplicate = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_callback_ledger
                WHERE account_key = ? AND session_generation_id = ?
                  AND callback_stream_id = ? AND callback_event_id = ?
                """,
                (
                    account_key,
                    callback.correlation_key.session_generation_id,
                    callback.stream_id,
                    callback.event_id,
                ),
            ).fetchone()
            if duplicate is not None:
                exact = (
                    str(duplicate["command_id"]) == command_id
                    and str(duplicate["callback_key_json"]) == callback_json
                    and str(duplicate["callback_key_sha256"]) == callback_digest
                    and str(duplicate["callback_payload_sha256"]) == callback_payload_digest
                    and str(duplicate["correlation_key_sha256"]) == correlation_digest
                    and str(duplicate["projection_state"]) == evidence.projection_state
                    and str(duplicate["source_digest_sha256"]) == evidence.source_digest_sha256
                    and str(duplicate["verifier_id"]) == evidence.verifier_id
                )
                if not exact:
                    raise IntentConflictError(
                        "CTP callback event id conflicts with durable evidence"
                    )
                fence_open = self._ctp_dispatch_has_open_account_fence(cursor, account_key)
                return CtpDispatchCallbackApplyResult(
                    callback_key=callback,
                    projection_state=evidence.projection_state,
                    duplicate=True,
                    account_fence_open=fence_open,
                )

            cursor.execute(
                """
                INSERT INTO ctp_dispatch_callback_ledger(
                    account_key, scope_key, trading_day, command_id, operation,
                    managed_action_id, runtime_order_id, order_ref,
                    session_generation_id, callback_stream_id, callback_event_id,
                    callback_family, callback_key_json, callback_key_sha256,
                    callback_payload_sha256, correlation_key_sha256,
                    projection_state, source_digest_sha256,
                    verifier_id, verified_at_ns, applied_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_key,
                    scope_key,
                    staged.trading_day,
                    command_id,
                    staged.operation,
                    staged.correlation_key.managed_action_id,
                    staged.correlation_key.runtime_order_id,
                    staged.correlation_key.order_ref,
                    staged.correlation_key.session_generation_id,
                    callback.stream_id,
                    callback.event_id,
                    callback.callback_family,
                    callback_json,
                    callback_digest,
                    callback_payload_digest,
                    correlation_digest,
                    evidence.projection_state,
                    evidence.source_digest_sha256,
                    evidence.verifier_id,
                    evidence.verified_at_ns,
                    applied_at_ns,
                ),
            )
            if staged.operation == "SUBMIT":
                self._upsert_ctp_order_projection(
                    cursor,
                    staged.correlation_key,
                    evidence.projection_state,
                    evidence.source_digest_sha256,
                    source_kind="CALLBACK",
                    updated_at_ns=applied_at_ns,
                    callback_key=callback,
                )
            else:
                self._upsert_ctp_cancel_projection(
                    cursor,
                    staged.correlation_key,
                    evidence.projection_state,
                    evidence.source_digest_sha256,
                    source_kind="CALLBACK",
                    updated_at_ns=applied_at_ns,
                    callback_key=callback,
                )
            fence_open = self._ctp_dispatch_has_open_account_fence(cursor, account_key)
            return CtpDispatchCallbackApplyResult(
                callback_key=callback,
                projection_state=evidence.projection_state,
                duplicate=False,
                account_fence_open=fence_open,
            )

    def resolve_unknown_ctp_dispatch_command(
        self,
        scope: ExecutionScope,
        command_id: str,
        *,
        writer_lease: WriterLease,
        reconciliation_verifier: CtpDispatchReconciliationVerifier | None = None,
    ) -> CtpDispatchUnknownResolutionResult:
        """Resolve only with a fresh exact external reconciliation attestation.

        The command remains historically ``UNKNOWN``. This atomically records
        an immutable terminal-evidence decision, updates the separate submit or
        cancel projection, and removes this command from the local account
        fence. Submit needs terminal order evidence; cancel needs terminal
        cancel-action evidence *and* terminal target-order evidence. Real
        verification must reconcile exact order/trade/position/cancel sources;
        caller digests or a matching callback are insufficient.
        """

        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        self._validate_command_identifier(command_id, "command_id")
        staged = self.read_ctp_dispatch_command(scope, command_id)
        if staged is None or staged.status != "UNKNOWN":
            raise InvalidStateTransition("only an UNKNOWN CTP dispatch command can be resolved")
        if staged.correlation_key is None:
            raise ContractValidationError(
                "UNKNOWN command lacks typed correlation and cannot resolve"
            )
        verifier = (
            _REJECT_CTP_DISPATCH_VERIFIER
            if reconciliation_verifier is None
            else reconciliation_verifier
        )
        verify_unknown = getattr(verifier, "verify_unknown", None)
        if not callable(verify_unknown):
            raise ContractValidationError("trusted CTP reconciliation verifier is required")
        verification_started_ns = time.time_ns()
        verification_failed = False
        attestation: CtpUnknownResolutionAttestation | None = None
        try:
            attestation = verify_unknown(staged, now_ns=verification_started_ns)
        except Exception:
            verification_failed = True
        if verification_failed:
            raise ContractValidationError("trusted CTP reconciliation verification failed")
        if type(attestation) is not CtpUnknownResolutionAttestation:
            raise ContractValidationError("invalid typed CTP reconciliation attestation")
        if (
            attestation.correlation_key != staged.correlation_key
            or attestation.verified_at_ns != verification_started_ns
        ):
            raise ContractValidationError("CTP reconciliation evidence does not match UNKNOWN")
        resolved_at_ns = time.time_ns()
        if attestation.expires_at_ns <= resolved_at_ns:
            raise ContractValidationError("CTP reconciliation evidence expired before commit")
        correlation_digest = payload_sha256(staged.correlation_key.to_payload())
        with self._transaction() as cursor:
            resolved_at_ns = time.time_ns()
            self._assert_active_writer_lease(cursor, scope, writer_lease, now_ns=resolved_at_ns)
            if attestation.expires_at_ns <= resolved_at_ns:
                raise ContractValidationError("CTP reconciliation evidence expired before commit")
            row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, command_id),
            ).fetchone()
            if row is None:
                raise ContractValidationError("unknown CTP dispatch command")
            current = self._ctp_dispatch_command_from_row(row)
            if current.status != "UNKNOWN" or current.correlation_key != staged.correlation_key:
                raise InvalidStateTransition("CTP UNKNOWN command changed during reconciliation")
            existing = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_unknown_resolutions
                WHERE account_key = ? AND command_id = ?
                """,
                (account_key, command_id),
            ).fetchone()
            if existing is not None:
                exact = (
                    str(existing["correlation_key_sha256"]) == correlation_digest
                    and str(existing["order_terminal_state"]) == attestation.order_terminal_state
                    and (
                        None
                        if existing["cancel_action_terminal_state"] is None
                        else str(existing["cancel_action_terminal_state"])
                    )
                    == attestation.cancel_action_terminal_state
                    and str(existing["source_digest_sha256"]) == attestation.source_digest_sha256
                    and str(existing["verifier_id"]) == attestation.verifier_id
                )
                if not exact:
                    raise IntentConflictError(
                        "CTP UNKNOWN resolution conflicts with durable evidence"
                    )
                return CtpDispatchUnknownResolutionResult(
                    command_id=command_id,
                    correlation_key=staged.correlation_key,
                    order_terminal_state=attestation.order_terminal_state,
                    cancel_action_terminal_state=attestation.cancel_action_terminal_state,
                    account_fence_open=self._ctp_dispatch_has_open_account_fence(
                        cursor, account_key
                    ),
                    duplicate=True,
                )
            cursor.execute(
                """
                INSERT INTO ctp_dispatch_unknown_resolutions(
                    account_key, scope_key, command_id, operation,
                    correlation_key_sha256, order_terminal_state,
                    cancel_action_terminal_state, source_digest_sha256,
                    verifier_id, verified_at_ns, resolved_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_key,
                    scope_key,
                    command_id,
                    staged.operation,
                    correlation_digest,
                    attestation.order_terminal_state,
                    attestation.cancel_action_terminal_state,
                    attestation.source_digest_sha256,
                    attestation.verifier_id,
                    attestation.verified_at_ns,
                    resolved_at_ns,
                ),
            )
            self._upsert_ctp_order_projection(
                cursor,
                staged.correlation_key,
                attestation.order_terminal_state,
                attestation.source_digest_sha256,
                source_kind="RECONCILIATION",
                updated_at_ns=resolved_at_ns,
            )
            if staged.operation == "CANCEL":
                assert attestation.cancel_action_terminal_state is not None
                self._upsert_ctp_cancel_projection(
                    cursor,
                    staged.correlation_key,
                    attestation.cancel_action_terminal_state,
                    attestation.source_digest_sha256,
                    source_kind="RECONCILIATION",
                    updated_at_ns=resolved_at_ns,
                )
            return CtpDispatchUnknownResolutionResult(
                command_id=command_id,
                correlation_key=staged.correlation_key,
                order_terminal_state=attestation.order_terminal_state,
                cancel_action_terminal_state=attestation.cancel_action_terminal_state,
                account_fence_open=self._ctp_dispatch_has_open_account_fence(cursor, account_key),
                duplicate=False,
            )

    def recover_claimed_ctp_dispatch_commands(
        self,
        scope: ExecutionScope,
        *,
        writer_lease: WriterLease,
    ) -> tuple[CtpDispatchCommand, ...]:
        """Recover prior-generation CLAIMED commands account-wide; never replay.

        One account writer lease covers every strategy scope and trading day,
        so unresolved commands from any scope are recovered before a new scope
        can proceed.
        """

        account_key, _, _ = self._validate_ctp_order_identity_scope(scope)
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            rows = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND status = 'CLAIMED'
                ORDER BY created_at_ns, command_id
                """,
                (account_key,),
            ).fetchall()
            recovered = []
            for row in rows:
                if (
                    str(row["claimed_owner_id"]) == writer_lease.owner_id
                    and int(row["claimed_fencing_token"]) == writer_lease.fencing_token
                ):
                    continue
                if row["native_receipt_payload_json"] is not None:
                    raise DurableStoreError("claimed CTP command unexpectedly has a receipt")
                cursor.execute(
                    """
                    UPDATE ctp_dispatch_commands
                    SET status = 'UNKNOWN', updated_at_ns = ?, unknown_at_ns = ?,
                        unknown_reason = ?
                    WHERE account_key = ? AND command_id = ? AND status = 'CLAIMED'
                      AND native_receipt_payload_json IS NULL
                    """,
                    (
                        now_ns,
                        now_ns,
                        "claimed_without_receipt_after_writer_change",
                        account_key,
                        str(row["command_id"]),
                    ),
                )
                updated = cursor.execute(
                    "SELECT * FROM ctp_dispatch_commands WHERE account_key = ? AND command_id = ?",
                    (account_key, str(row["command_id"])),
                ).fetchone()
                if updated is not None and str(updated["status"]) == "UNKNOWN":
                    recovered.append(self._ctp_dispatch_command_from_row(updated))
            return tuple(recovered)

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cursor = self._connection.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                yield cursor
            except sqlite3.Error as error:
                self._connection.rollback()
                raise DurableStoreError("execution store transaction failed") from error
            except BaseException:
                self._connection.rollback()
                raise
            else:
                try:
                    self._connection.commit()
                except sqlite3.Error as error:
                    self._connection.rollback()
                    raise DurableStoreError("execution store commit failed") from error
            finally:
                cursor.close()

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> ExecutionRecord:
        return ExecutionRecord(
            intent_id=row["intent_id"],
            scope_key=row["scope_key"],
            payload_sha256=row["payload_sha256"],
            state=ExecutionState(row["state"]),
            provider_order_id=row["provider_order_id"],
            filled_quantity=Decimal(row["filled_quantity"]),
            average_price=None if row["average_price"] is None else Decimal(row["average_price"]),
            cumulative_commission=(
                None
                if row["cumulative_commission"] is None
                else Decimal(row["cumulative_commission"])
            ),
            permit_reference=row["permit_reference"],
            dispatch_attempts=row["dispatch_attempts"],
            unknown_reason=row["unknown_reason"],
            review_required=bool(row["review_required"]),
            created_at_ns=row["created_at_ns"],
            updated_at_ns=row["updated_at_ns"],
        )

    @staticmethod
    def _cancel_record_from_row(row: sqlite3.Row) -> CancelRecord:
        return CancelRecord(
            cancel_id=row["cancel_id"],
            scope_key=row["scope_key"],
            target_intent_id=row["target_intent_id"],
            provider_order_id=row["provider_order_id"],
            payload_sha256=row["payload_sha256"],
            state=ExecutionState(row["state"]),
            permit_reference=row["permit_reference"],
            dispatch_attempts=row["dispatch_attempts"],
            unknown_reason=row["unknown_reason"],
            review_required=bool(row["review_required"]),
            created_at_ns=row["created_at_ns"],
            updated_at_ns=row["updated_at_ns"],
        )

    @staticmethod
    def _event_payload(**values: Any) -> str:
        return canonical_json(values)

    def _append_event(
        self,
        cursor: sqlite3.Cursor,
        *,
        intent_id: str,
        scope_key: str,
        event_type: str,
        state: ExecutionState,
        payload: dict[str, Any],
        now_ns: int,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO execution_outbox(
                event_id, intent_id, scope_key, event_type, state, payload_json, created_at_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                intent_id,
                scope_key,
                event_type,
                state.value,
                self._event_payload(**payload),
                now_ns,
            ),
        )

    def _append_cancel_event(
        self,
        cursor: sqlite3.Cursor,
        *,
        cancel_id: str,
        target_intent_id: str,
        scope_key: str,
        event_type: str,
        state: ExecutionState,
        payload: dict[str, Any],
        now_ns: int,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO cancellation_outbox(
                event_id, cancel_id, target_intent_id, scope_key, event_type, state,
                payload_json, created_at_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                cancel_id,
                target_intent_id,
                scope_key,
                event_type,
                state.value,
                self._event_payload(**payload),
                now_ns,
            ),
        )

    @staticmethod
    def _fetch_record(cursor: sqlite3.Cursor, scope_key: str, intent_id: str) -> sqlite3.Row | None:
        return cursor.execute(
            "SELECT * FROM execution_records WHERE scope_key = ? AND intent_id = ?",
            (scope_key, intent_id),
        ).fetchone()

    @staticmethod
    def _fetch_cancel_record(
        cursor: sqlite3.Cursor, scope_key: str, cancel_id: str
    ) -> sqlite3.Row | None:
        return cursor.execute(
            "SELECT * FROM cancellation_records WHERE scope_key = ? AND cancel_id = ?",
            (scope_key, cancel_id),
        ).fetchone()

    @staticmethod
    def _assert_active_writer_lease(
        cursor: sqlite3.Cursor,
        scope: ExecutionScope,
        writer_lease: WriterLease | None,
        *,
        now_ns: int | None = None,
    ) -> None:
        """Fence a facade mutation inside its SQLite transaction.

        Every execution/cancellation state mutation must carry the exact lease
        generation acquired by its facade.  This keeps a direct low-level
        caller from silently bypassing the writer fence after a lease expiry.
        ``now_ns`` lets a caller recheck expiry against the transaction
        timestamp it will persist for the mutation.
        """

        if writer_lease is None:
            raise WriterLeaseUnavailable()
        if (
            writer_lease.scope_key != scope.account_key
            or not writer_lease.owner_id
            or type(writer_lease.fencing_token) is not int
            or writer_lease.fencing_token <= 0
        ):
            raise ContractValidationError("invalid writer lease")
        row = cursor.execute(
            "SELECT owner_id, fencing_token, expires_at_ns FROM execution_writer_leases "
            "WHERE scope_key = ?",
            (scope.account_key,),
        ).fetchone()
        lease_check_ns = time.time_ns() if now_ns is None else now_ns
        if (
            row is None
            or str(row["owner_id"]) != writer_lease.owner_id
            or int(row["fencing_token"]) != writer_lease.fencing_token
            or int(row["expires_at_ns"]) <= lease_check_ns
        ):
            raise WriterLeaseUnavailable()

    def get(self, intent_id: str, *, scope: ExecutionScope | None = None) -> ExecutionRecord | None:
        """Read one durable record without mutating state.

        An intent id is unique only within its strategy scope.  Supplying a
        scope is therefore mandatory once multiple scopes share a journal.
        """

        with self._lock:
            try:
                if scope is not None:
                    row = self._connection.execute(
                        "SELECT * FROM execution_records WHERE scope_key = ? AND intent_id = ?",
                        (scope.key, intent_id),
                    ).fetchone()
                else:
                    rows = self._connection.execute(
                        "SELECT * FROM execution_records WHERE intent_id = ? LIMIT 2", (intent_id,)
                    ).fetchall()
                    if len(rows) > 1:
                        raise ContractValidationError("ambiguous intent_id; scope is required")
                    row = rows[0] if rows else None
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read execution record") from error
        return None if row is None else self._record_from_row(row)

    def list_dispatching(self, scope: ExecutionScope) -> tuple[ExecutionRecord, ...]:
        """Return in-flight records in one scope without provider activity.

        A composition-level pre-dispatch hook runs after this store durably
        enters ``DISPATCHING``.  A process can die in that narrow interval,
        leaving no composition work row.  Recovery needs this exact read to
        turn such records into ``UNKNOWN`` rather than leaving them stranded
        forever.
        """

        with self._lock:
            try:
                rows = self._connection.execute(
                    """
                    SELECT * FROM execution_records
                    WHERE scope_key = ? AND state = ?
                    ORDER BY created_at_ns ASC, intent_id ASC
                    """,
                    (scope.key, ExecutionState.DISPATCHING.value),
                ).fetchall()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to list dispatching execution records") from error
        return tuple(self._record_from_row(row) for row in rows)

    def assert_writer_lease(self, scope: ExecutionScope, writer_lease: WriterLease) -> None:
        """Fail if this exact facade generation has lost its writer fence."""

        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)

    def get_intent(
        self, intent_id: str, *, scope: ExecutionScope | None = None
    ) -> OrderIntent | None:
        """Read and validate the immutable version-1 intent payload."""

        with self._lock:
            try:
                if scope is not None:
                    row = self._connection.execute(
                        """
                        SELECT payload_json FROM execution_records
                        WHERE scope_key = ? AND intent_id = ?
                        """,
                        (scope.key, intent_id),
                    ).fetchone()
                else:
                    rows = self._connection.execute(
                        "SELECT payload_json FROM execution_records WHERE intent_id = ? LIMIT 2",
                        (intent_id,),
                    ).fetchall()
                    if len(rows) > 1:
                        raise ContractValidationError("ambiguous intent_id; scope is required")
                    row = rows[0] if rows else None
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read execution intent") from error
        if row is None:
            return None
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError) as error:
            raise DurableStoreError("stored intent payload is unreadable") from error
        return order_intent_from_payload(payload)

    def get_cancel(
        self, cancel_id: str, *, scope: ExecutionScope | None = None
    ) -> CancelRecord | None:
        """Read one cancellation record without creating provider activity.

        As with order intents, the caller must supply a scope whenever a
        shared journal could contain the same cancellation id in more than one
        strategy scope.
        """

        with self._lock:
            try:
                if scope is not None:
                    row = self._connection.execute(
                        "SELECT * FROM cancellation_records WHERE scope_key = ? AND cancel_id = ?",
                        (scope.key, cancel_id),
                    ).fetchone()
                else:
                    rows = self._connection.execute(
                        "SELECT * FROM cancellation_records WHERE cancel_id = ? LIMIT 2",
                        (cancel_id,),
                    ).fetchall()
                    if len(rows) > 1:
                        raise ContractValidationError("ambiguous cancel_id; scope is required")
                    row = rows[0] if rows else None
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read cancellation record") from error
        return None if row is None else self._cancel_record_from_row(row)

    def list_unknown_cancellations(self, scope: ExecutionScope) -> list[CancelRecord]:
        """Return only durable unknown cancellations in one exact execution scope.

        This recovery read performs no provider I/O and makes no state change.
        Composition layers use it at startup to rebuild an independent account
        freeze after a crash between the execution-journal UNKNOWN commit and a
        separate risk-journal freeze commit.  It is deliberately scoped so one
        strategy cannot discover or re-freeze another strategy's cancellation.
        """

        if not isinstance(scope, ExecutionScope):
            raise ContractValidationError("execution scope is required")
        with self._lock:
            try:
                rows = self._connection.execute(
                    """
                    SELECT * FROM cancellation_records
                    WHERE scope_key = ? AND state = ?
                    ORDER BY created_at_ns ASC, cancel_id ASC
                    """,
                    (scope.key, ExecutionState.UNKNOWN.value),
                ).fetchall()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to list unknown cancellation records") from error
        return [self._cancel_record_from_row(row) for row in rows]

    def recover_interrupted_cancellations(
        self,
        scope: ExecutionScope,
        *,
        writer_lease: WriterLease | None = None,
    ) -> tuple[CancelRecord, ...]:
        """Convert claimed but unobserved cancellations into durable UNKNOWN.

        A process may stop after the durable dispatch claim and before the
        provider observation is committed.  Since the provider request may
        have reached the venue, this recovery operation never retries it and
        never infers success from order status.  The state transition and
        outbox event are committed together under the current writer fence.
        """

        if not isinstance(scope, ExecutionScope):
            raise ContractValidationError("execution scope is required")
        now_ns = time.time_ns()
        reason_code = "interrupted_cancel_dispatch"
        recovered: list[CancelRecord] = []
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            rows = cursor.execute(
                """
                SELECT * FROM cancellation_records
                WHERE scope_key = ? AND state = ?
                ORDER BY created_at_ns ASC, cancel_id ASC
                """,
                (scope.key, ExecutionState.DISPATCHING.value),
            ).fetchall()
            for row in rows:
                record = self._cancel_record_from_row(row)
                cursor.execute(
                    """
                    UPDATE cancellation_records
                    SET state = ?, unknown_reason = ?, review_required = 1, updated_at_ns = ?
                    WHERE scope_key = ? AND cancel_id = ? AND state = ?
                    """,
                    (
                        ExecutionState.UNKNOWN.value,
                        reason_code,
                        now_ns,
                        scope.key,
                        record.cancel_id,
                        ExecutionState.DISPATCHING.value,
                    ),
                )
                self._append_cancel_event(
                    cursor,
                    cancel_id=record.cancel_id,
                    target_intent_id=record.target_intent_id,
                    scope_key=scope.key,
                    event_type="cancel_dispatch_recovered_unknown",
                    state=ExecutionState.UNKNOWN,
                    payload={"reason_code": reason_code},
                    now_ns=now_ns,
                )
                updated = self._fetch_cancel_record(cursor, scope.key, record.cancel_id)
                assert updated is not None
                recovered.append(self._cancel_record_from_row(updated))
        return tuple(recovered)

    def get_cancel_intent(
        self, cancel_id: str, *, scope: ExecutionScope | None = None
    ) -> CancelIntent | None:
        """Read and validate one immutable cancellation payload."""

        with self._lock:
            try:
                if scope is not None:
                    row = self._connection.execute(
                        """
                        SELECT payload_json FROM cancellation_records
                        WHERE scope_key = ? AND cancel_id = ?
                        """,
                        (scope.key, cancel_id),
                    ).fetchone()
                else:
                    rows = self._connection.execute(
                        "SELECT payload_json FROM cancellation_records WHERE cancel_id = ? LIMIT 2",
                        (cancel_id,),
                    ).fetchall()
                    if len(rows) > 1:
                        raise ContractValidationError("ambiguous cancel_id; scope is required")
                    row = rows[0] if rows else None
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read cancellation intent") from error
        if row is None:
            return None
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError) as error:
            raise DurableStoreError("stored cancellation payload is unreadable") from error
        return cancel_intent_from_payload(payload)

    def admit_cancel(
        self, intent: CancelIntent, *, writer_lease: WriterLease | None = None
    ) -> CancelRecord:
        """Persist a cancellation only for an already evidenced open target.

        The target identity is checked against the original execution journal
        before the cancellation record can become dispatchable.  This keeps a
        bare framework order reference from authorizing a provider cancel.
        """

        now_ns = time.time_ns()
        payload_json = canonical_json(intent.to_payload())
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, intent.scope, writer_lease)
            existing = self._fetch_cancel_record(cursor, intent.scope.key, intent.cancel_id)
            if existing is not None:
                if (
                    existing["payload_sha256"] != intent.fingerprint
                    or existing["scope_key"] != intent.scope.key
                ):
                    raise IntentConflictError("cancel_id was reused with another payload")
                return self._cancel_record_from_row(existing)
            target_row = self._fetch_record(cursor, intent.scope.key, intent.target_intent_id)
            if target_row is None:
                raise ContractValidationError("cancellation target intent is unknown")
            target = self._record_from_row(target_row)
            if (
                target.provider_order_id is None
                or target.provider_order_id != intent.provider_order_id
            ):
                raise ContractValidationError(
                    "cancellation target provider identity is not confirmed"
                )
            if target.state not in {ExecutionState.ACKED, ExecutionState.PARTIALLY_FILLED}:
                raise ContractValidationError("cancellation target is not safely cancellable")
            cursor.execute(
                """
                INSERT INTO cancellation_records(
                    scope_key, cancel_id, target_intent_id, provider_order_id, payload_sha256,
                    payload_json, state, created_at_ns, updated_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent.scope.key,
                    intent.cancel_id,
                    intent.target_intent_id,
                    intent.provider_order_id,
                    intent.fingerprint,
                    payload_json,
                    ExecutionState.PENDING_ADMISSION.value,
                    now_ns,
                    now_ns,
                ),
            )
            self._append_cancel_event(
                cursor,
                cancel_id=intent.cancel_id,
                target_intent_id=intent.target_intent_id,
                scope_key=intent.scope.key,
                event_type="cancel_intent_recorded",
                state=ExecutionState.PENDING_ADMISSION,
                payload={"payload_sha256": intent.fingerprint},
                now_ns=now_ns,
            )
            inserted = self._fetch_cancel_record(cursor, intent.scope.key, intent.cancel_id)
            assert inserted is not None
            return self._cancel_record_from_row(inserted)

    def activate_cancel(
        self,
        cancel_id: str,
        scope: ExecutionScope,
        permit_reference: str | None = None,
        *,
        writer_lease: WriterLease | None = None,
    ) -> CancelRecord:
        """Record that one durable cancellation may claim a single dispatch."""

        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            if row is None:
                raise ContractValidationError("unknown cancel_id")
            record = self._cancel_record_from_row(row)
            if record.state is not ExecutionState.PENDING_ADMISSION:
                return record
            cursor.execute(
                """
                UPDATE cancellation_records
                SET state = ?, permit_reference = ?, updated_at_ns = ?
                WHERE scope_key = ? AND cancel_id = ?
                """,
                (
                    ExecutionState.PENDING_DISPATCH.value,
                    permit_reference,
                    now_ns,
                    scope.key,
                    cancel_id,
                ),
            )
            self._append_cancel_event(
                cursor,
                cancel_id=cancel_id,
                target_intent_id=record.target_intent_id,
                scope_key=scope.key,
                event_type="cancel_intent_admitted",
                state=ExecutionState.PENDING_DISPATCH,
                payload={"permit_reference": permit_reference},
                now_ns=now_ns,
            )
            activated = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            assert activated is not None
            return self._cancel_record_from_row(activated)

    def reject_cancel(
        self,
        cancel_id: str,
        scope: ExecutionScope,
        reason_code: str,
        *,
        blocked: bool = False,
        writer_lease: WriterLease | None = None,
    ) -> CancelRecord:
        """Durably reject a cancellation before any provider dispatch."""

        if not reason_code or not reason_code.replace("_", "").isalnum():
            raise ContractValidationError("invalid cancellation reason_code")
        now_ns = time.time_ns()
        target = ExecutionState.BLOCKED if blocked else ExecutionState.REJECTED
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            if row is None:
                raise ContractValidationError("unknown cancel_id")
            record = self._cancel_record_from_row(row)
            if record.state is not ExecutionState.PENDING_ADMISSION:
                return record
            cursor.execute(
                """
                UPDATE cancellation_records
                SET state = ?, unknown_reason = ?, review_required = ?, updated_at_ns = ?
                WHERE scope_key = ? AND cancel_id = ?
                """,
                (target.value, reason_code, 1 if blocked else 0, now_ns, scope.key, cancel_id),
            )
            self._append_cancel_event(
                cursor,
                cancel_id=cancel_id,
                target_intent_id=record.target_intent_id,
                scope_key=scope.key,
                event_type="cancel_intent_blocked" if blocked else "cancel_intent_rejected",
                state=target,
                payload={"reason_code": reason_code},
                now_ns=now_ns,
            )
            rejected = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            assert rejected is not None
            return self._cancel_record_from_row(rejected)

    def claim_cancel_for_dispatch(
        self,
        cancel_id: str,
        scope: ExecutionScope,
        *,
        writer_lease: WriterLease | None = None,
    ) -> tuple[CancelRecord, bool]:
        """Atomically claim the only provider cancellation attempt.

        The target is checked again in the same transaction, so a target that
        filled or changed identity after admission cannot be cancelled through
        a stale local request.
        """

        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            if row is None:
                raise ContractValidationError("unknown cancel_id")
            record = self._cancel_record_from_row(row)
            if record.state is not ExecutionState.PENDING_DISPATCH:
                return record, False
            target_row = self._fetch_record(cursor, scope.key, record.target_intent_id)
            target = None if target_row is None else self._record_from_row(target_row)
            if (
                target is None
                or target.provider_order_id != record.provider_order_id
                or target.state not in {ExecutionState.ACKED, ExecutionState.PARTIALLY_FILLED}
            ):
                reason_code = "cancellation_target_changed"
                cursor.execute(
                    """
                    UPDATE cancellation_records
                    SET state = ?, unknown_reason = ?, review_required = 1, updated_at_ns = ?
                    WHERE scope_key = ? AND cancel_id = ?
                    """,
                    (ExecutionState.BLOCKED.value, reason_code, now_ns, scope.key, cancel_id),
                )
                self._append_cancel_event(
                    cursor,
                    cancel_id=cancel_id,
                    target_intent_id=record.target_intent_id,
                    scope_key=scope.key,
                    event_type="cancel_dispatch_blocked",
                    state=ExecutionState.BLOCKED,
                    payload={"reason_code": reason_code},
                    now_ns=now_ns,
                )
                blocked = self._fetch_cancel_record(cursor, scope.key, cancel_id)
                assert blocked is not None
                return self._cancel_record_from_row(blocked), False
            cursor.execute(
                """
                UPDATE cancellation_records
                SET state = ?, dispatch_attempts = dispatch_attempts + 1, updated_at_ns = ?
                WHERE scope_key = ? AND cancel_id = ?
                """,
                (ExecutionState.DISPATCHING.value, now_ns, scope.key, cancel_id),
            )
            self._append_cancel_event(
                cursor,
                cancel_id=cancel_id,
                target_intent_id=record.target_intent_id,
                scope_key=scope.key,
                event_type="cancel_dispatch_claimed",
                state=ExecutionState.DISPATCHING,
                payload={"dispatch_attempt": record.dispatch_attempts + 1},
                now_ns=now_ns,
            )
            claimed = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            assert claimed is not None
            return self._cancel_record_from_row(claimed), True

    def mark_cancel_unknown(
        self,
        cancel_id: str,
        scope: ExecutionScope,
        reason_code: str = "cancel_outcome_unknown",
        *,
        writer_lease: WriterLease | None = None,
    ) -> CancelRecord:
        """Latch an uncertain cancellation result; automatic replay is impossible."""

        if not reason_code or not reason_code.replace("_", "").isalnum():
            raise ContractValidationError("invalid cancellation reason_code")
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            if row is None:
                raise ContractValidationError("unknown cancel_id")
            record = self._cancel_record_from_row(row)
            if record.is_terminal or record.state is ExecutionState.UNKNOWN:
                return record
            if not _can_cancel_transition(record.state, ExecutionState.UNKNOWN):
                raise InvalidStateTransition()
            cursor.execute(
                """
                UPDATE cancellation_records
                SET state = ?, unknown_reason = ?, review_required = 1, updated_at_ns = ?
                WHERE scope_key = ? AND cancel_id = ?
                """,
                (ExecutionState.UNKNOWN.value, reason_code, now_ns, scope.key, cancel_id),
            )
            self._append_cancel_event(
                cursor,
                cancel_id=cancel_id,
                target_intent_id=record.target_intent_id,
                scope_key=scope.key,
                event_type="cancel_dispatch_unknown",
                state=ExecutionState.UNKNOWN,
                payload={"reason_code": reason_code},
                now_ns=now_ns,
            )
            unknown = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            assert unknown is not None
            return self._cancel_record_from_row(unknown)

    def block_claimed_cancel_dispatch(
        self,
        cancel_id: str,
        scope: ExecutionScope,
        reason_code: str,
        *,
        writer_lease: WriterLease | None = None,
    ) -> CancelRecord:
        """Stop a cancellation after a local claim but before provider I/O."""

        if not reason_code or not reason_code.replace("_", "").isalnum():
            raise ContractValidationError("invalid cancellation reason_code")
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            if row is None:
                raise ContractValidationError("unknown cancel_id")
            record = self._cancel_record_from_row(row)
            if record.state is not ExecutionState.DISPATCHING:
                return record
            cursor.execute(
                """
                UPDATE cancellation_records
                SET state = ?, unknown_reason = ?, review_required = 1, updated_at_ns = ?
                WHERE scope_key = ? AND cancel_id = ?
                """,
                (ExecutionState.BLOCKED.value, reason_code, now_ns, scope.key, cancel_id),
            )
            self._append_cancel_event(
                cursor,
                cancel_id=cancel_id,
                target_intent_id=record.target_intent_id,
                scope_key=scope.key,
                event_type="cancel_dispatch_blocked",
                state=ExecutionState.BLOCKED,
                payload={"reason_code": reason_code},
                now_ns=now_ns,
            )
            blocked = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            assert blocked is not None
            return self._cancel_record_from_row(blocked)

    def record_cancel_observation(
        self,
        observation: CancelObservation,
        *,
        scope: ExecutionScope,
        source: str = "provider",
        writer_lease: WriterLease | None = None,
    ) -> CancelRecord:
        """Apply typed cancellation evidence and advance the target if cancelled."""

        if source not in {"provider", "reconcile"}:
            raise ContractValidationError("invalid cancellation observation source")
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_cancel_record(cursor, scope.key, observation.cancel_id)
            if row is None:
                raise ContractValidationError("unknown cancel_id")
            record = self._cancel_record_from_row(row)
            if (
                record.target_intent_id != observation.target_intent_id
                or record.provider_order_id != observation.provider_order_id
            ):
                raise InvalidStateTransition("cancellation observation identity mismatch")
            target_state = observation.state
            if record.state is target_state:
                return record
            if record.is_terminal:
                raise InvalidStateTransition("conflicting terminal cancellation observation")
            if not _can_cancel_transition(record.state, target_state):
                raise InvalidStateTransition()
            if target_state is ExecutionState.CANCELLED:
                target_row = self._fetch_record(cursor, scope.key, record.target_intent_id)
                if target_row is None:
                    raise ContractValidationError("cancellation target intent is unknown")
                target_record = self._record_from_row(target_row)
                if target_record.provider_order_id != record.provider_order_id:
                    raise InvalidStateTransition("cancellation target provider identity changed")
                if target_record.state is not ExecutionState.CANCELLED:
                    if target_record.is_terminal or not can_transition(
                        target_record.state, ExecutionState.CANCELLED
                    ):
                        raise InvalidStateTransition("cancellation target cannot advance")
                    cursor.execute(
                        """
                        UPDATE execution_records
                        SET state = ?, unknown_reason = NULL, review_required = 0, updated_at_ns = ?
                        WHERE scope_key = ? AND intent_id = ?
                        """,
                        (
                            ExecutionState.CANCELLED.value,
                            now_ns,
                            scope.key,
                            record.target_intent_id,
                        ),
                    )
                    self._append_event(
                        cursor,
                        intent_id=record.target_intent_id,
                        scope_key=scope.key,
                        event_type="cancelled_by_cancel_intent",
                        state=ExecutionState.CANCELLED,
                        payload={
                            "cancel_id": record.cancel_id,
                            "provider_order_id": record.provider_order_id,
                            "source": source,
                        },
                        now_ns=now_ns,
                    )
            cursor.execute(
                """
                UPDATE cancellation_records
                SET state = ?, unknown_reason = NULL, review_required = ?, updated_at_ns = ?
                WHERE scope_key = ? AND cancel_id = ?
                """,
                (
                    target_state.value,
                    0
                    if target_state in {ExecutionState.CANCELLED, ExecutionState.REJECTED}
                    else record.review_required,
                    now_ns,
                    scope.key,
                    observation.cancel_id,
                ),
            )
            self._append_cancel_event(
                cursor,
                cancel_id=observation.cancel_id,
                target_intent_id=record.target_intent_id,
                scope_key=scope.key,
                event_type="cancel_provider_observation"
                if source == "provider"
                else "cancel_reconciled_observation",
                state=target_state,
                payload={
                    "provider_order_id": record.provider_order_id,
                    "reason_code": observation.reason_code,
                    "source": source,
                },
                now_ns=now_ns,
            )
            updated = self._fetch_cancel_record(cursor, scope.key, observation.cancel_id)
            assert updated is not None
            return self._cancel_record_from_row(updated)

    def mark_cancel_review_required(
        self,
        cancel_id: str,
        scope: ExecutionScope,
        reason_code: str,
        *,
        writer_lease: WriterLease | None = None,
    ) -> CancelRecord:
        """Persist a cancellation manual-review latch without fabricating evidence."""

        if not reason_code or not reason_code.replace("_", "").isalnum():
            raise ContractValidationError("invalid cancellation reason_code")
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            if row is None:
                raise ContractValidationError("unknown cancel_id")
            record = self._cancel_record_from_row(row)
            cursor.execute(
                """
                UPDATE cancellation_records SET review_required = 1, updated_at_ns = ?
                WHERE scope_key = ? AND cancel_id = ?
                """,
                (now_ns, scope.key, cancel_id),
            )
            self._append_cancel_event(
                cursor,
                cancel_id=cancel_id,
                target_intent_id=record.target_intent_id,
                scope_key=scope.key,
                event_type="cancel_manual_review_required",
                state=record.state,
                payload={"reason_code": reason_code},
                now_ns=now_ns,
            )
            updated = self._fetch_cancel_record(cursor, scope.key, cancel_id)
            assert updated is not None
            return self._cancel_record_from_row(updated)

    def admit_intent(
        self, intent: OrderIntent, *, writer_lease: WriterLease | None = None
    ) -> ExecutionRecord:
        """Persist an immutable intent before admission or provider dispatch.

        Reusing an id with the identical payload is idempotent.  Reusing it
        with a different payload is a terminal local contract error.
        """

        now_ns = time.time_ns()
        payload_json = canonical_json(intent.to_payload())
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, intent.scope, writer_lease)
            existing = self._fetch_record(cursor, intent.scope.key, intent.intent_id)
            if existing is not None:
                if (
                    existing["payload_sha256"] != intent.fingerprint
                    or existing["scope_key"] != intent.scope.key
                ):
                    raise IntentConflictError()
                return self._record_from_row(existing)
            cursor.execute(
                """
                INSERT INTO execution_records(
                    scope_key, intent_id, payload_sha256, payload_json, state, filled_quantity,
                    created_at_ns, updated_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent.scope.key,
                    intent.intent_id,
                    intent.fingerprint,
                    payload_json,
                    ExecutionState.PENDING_ADMISSION.value,
                    "0",
                    now_ns,
                    now_ns,
                ),
            )
            self._append_event(
                cursor,
                intent_id=intent.intent_id,
                scope_key=intent.scope.key,
                event_type="intent_recorded",
                state=ExecutionState.PENDING_ADMISSION,
                payload={"payload_sha256": intent.fingerprint},
                now_ns=now_ns,
            )
            inserted = self._fetch_record(cursor, intent.scope.key, intent.intent_id)
            assert inserted is not None
            return self._record_from_row(inserted)

    def activate_intent(
        self,
        intent_id: str,
        scope: ExecutionScope,
        permit_reference: str | None = None,
        *,
        writer_lease: WriterLease | None = None,
    ) -> ExecutionRecord:
        """Record that a durable intent is eligible for a single dispatch claim."""

        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_record(cursor, scope.key, intent_id)
            if row is None:
                raise ContractValidationError("unknown intent_id")
            record = self._record_from_row(row)
            if record.state is not ExecutionState.PENDING_ADMISSION:
                return record
            cursor.execute(
                """
                UPDATE execution_records
                SET state = ?, permit_reference = ?, updated_at_ns = ?
                WHERE scope_key = ? AND intent_id = ?
                """,
                (
                    ExecutionState.PENDING_DISPATCH.value,
                    permit_reference,
                    now_ns,
                    scope.key,
                    intent_id,
                ),
            )
            self._append_event(
                cursor,
                intent_id=intent_id,
                scope_key=scope.key,
                event_type="intent_admitted",
                state=ExecutionState.PENDING_DISPATCH,
                payload={"permit_reference": permit_reference},
                now_ns=now_ns,
            )
            activated = self._fetch_record(cursor, scope.key, intent_id)
            assert activated is not None
            return self._record_from_row(activated)

    def reject_intent(
        self,
        intent_id: str,
        scope: ExecutionScope,
        reason_code: str,
        *,
        blocked: bool = False,
        writer_lease: WriterLease | None = None,
    ) -> ExecutionRecord:
        """Durably reject an intent before any provider dispatch."""

        if not reason_code or not reason_code.replace("_", "").isalnum():
            raise ContractValidationError("invalid reason_code")
        now_ns = time.time_ns()
        target = ExecutionState.BLOCKED if blocked else ExecutionState.REJECTED
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_record(cursor, scope.key, intent_id)
            if row is None:
                raise ContractValidationError("unknown intent_id")
            record = self._record_from_row(row)
            if record.state is not ExecutionState.PENDING_ADMISSION:
                return record
            cursor.execute(
                """
                UPDATE execution_records
                SET state = ?, unknown_reason = ?, updated_at_ns = ?
                WHERE scope_key = ? AND intent_id = ?
                """,
                (target.value, reason_code, now_ns, scope.key, intent_id),
            )
            self._append_event(
                cursor,
                intent_id=intent_id,
                scope_key=scope.key,
                event_type="intent_blocked" if blocked else "intent_rejected",
                state=target,
                payload={"reason_code": reason_code},
                now_ns=now_ns,
            )
            rejected = self._fetch_record(cursor, scope.key, intent_id)
            assert rejected is not None
            return self._record_from_row(rejected)

    def claim_for_dispatch(
        self,
        intent_id: str,
        scope: ExecutionScope,
        *,
        writer_lease: WriterLease | None = None,
    ) -> tuple[ExecutionRecord, bool]:
        """Atomically claim the only allowed provider dispatch attempt.

        A returned ``False`` means the record was already claimed or terminal;
        callers must never infer permission to resend from that result.
        """

        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_record(cursor, scope.key, intent_id)
            if row is None:
                raise ContractValidationError("unknown intent_id")
            record = self._record_from_row(row)
            if record.state is not ExecutionState.PENDING_DISPATCH:
                return record, False
            cursor.execute(
                """
                UPDATE execution_records
                SET state = ?, dispatch_attempts = dispatch_attempts + 1, updated_at_ns = ?
                WHERE scope_key = ? AND intent_id = ?
                """,
                (ExecutionState.DISPATCHING.value, now_ns, scope.key, intent_id),
            )
            self._append_event(
                cursor,
                intent_id=intent_id,
                scope_key=scope.key,
                event_type="dispatch_claimed",
                state=ExecutionState.DISPATCHING,
                payload={"dispatch_attempt": record.dispatch_attempts + 1},
                now_ns=now_ns,
            )
            claimed = self._fetch_record(cursor, scope.key, intent_id)
            assert claimed is not None
            return self._record_from_row(claimed), True

    def mark_unknown(
        self,
        intent_id: str,
        scope: ExecutionScope,
        reason_code: str = "dispatch_outcome_unknown",
        *,
        writer_lease: WriterLease | None = None,
    ) -> ExecutionRecord:
        """Latch an uncertain provider result; automatic replay is impossible afterwards."""

        if not reason_code or not reason_code.replace("_", "").isalnum():
            raise ContractValidationError("invalid reason_code")
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_record(cursor, scope.key, intent_id)
            if row is None:
                raise ContractValidationError("unknown intent_id")
            record = self._record_from_row(row)
            if record.is_terminal or record.state is ExecutionState.UNKNOWN:
                return record
            if not can_transition(record.state, ExecutionState.UNKNOWN):
                raise InvalidStateTransition()
            cursor.execute(
                """
                UPDATE execution_records
                SET state = ?, unknown_reason = ?, review_required = 1, updated_at_ns = ?
                WHERE scope_key = ? AND intent_id = ?
                """,
                (ExecutionState.UNKNOWN.value, reason_code, now_ns, scope.key, intent_id),
            )
            self._append_event(
                cursor,
                intent_id=intent_id,
                scope_key=scope.key,
                event_type="dispatch_unknown",
                state=ExecutionState.UNKNOWN,
                payload={"reason_code": reason_code},
                now_ns=now_ns,
            )
            unknown = self._fetch_record(cursor, scope.key, intent_id)
            assert unknown is not None
            return self._record_from_row(unknown)

    def block_claimed_dispatch(
        self,
        intent_id: str,
        scope: ExecutionScope,
        reason_code: str,
        *,
        writer_lease: WriterLease | None = None,
    ) -> ExecutionRecord:
        """Stop a claimed dispatch before a provider port has been invoked."""

        if not reason_code or not reason_code.replace("_", "").isalnum():
            raise ContractValidationError("invalid reason_code")
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_record(cursor, scope.key, intent_id)
            if row is None:
                raise ContractValidationError("unknown intent_id")
            record = self._record_from_row(row)
            if record.state is not ExecutionState.DISPATCHING:
                return record
            cursor.execute(
                """
                UPDATE execution_records
                SET state = ?, unknown_reason = ?, review_required = 1, updated_at_ns = ?
                WHERE scope_key = ? AND intent_id = ?
                """,
                (ExecutionState.BLOCKED.value, reason_code, now_ns, scope.key, intent_id),
            )
            self._append_event(
                cursor,
                intent_id=intent_id,
                scope_key=scope.key,
                event_type="dispatch_blocked",
                state=ExecutionState.BLOCKED,
                payload={"reason_code": reason_code},
                now_ns=now_ns,
            )
            blocked = self._fetch_record(cursor, scope.key, intent_id)
            assert blocked is not None
            return self._record_from_row(blocked)

    def record_observation(
        self,
        observation: ProviderObservation,
        *,
        scope: ExecutionScope,
        source: str = "provider",
        writer_lease: WriterLease | None = None,
    ) -> ExecutionRecord:
        """Apply monotonic provider evidence to a dispatch or unknown record."""

        if source not in {"provider", "reconcile"}:
            raise ContractValidationError("invalid observation source")
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_record(cursor, scope.key, observation.intent_id)
            if row is None:
                raise ContractValidationError("unknown intent_id")
            record = self._record_from_row(row)
            target = observation.state
            if record.state is target:
                if observation.filled_quantity == record.filled_quantity and (
                    observation.provider_order_id is None
                    or observation.provider_order_id == record.provider_order_id
                ):
                    if observation.cumulative_commission is None:
                        return record
                    if record.cumulative_commission == observation.cumulative_commission:
                        return record
                    if record.cumulative_commission is None:
                        cursor.execute(
                            """
                            UPDATE execution_records
                            SET cumulative_commission = ?, updated_at_ns = ?
                            WHERE scope_key = ? AND intent_id = ?
                            """,
                            (
                                format(observation.cumulative_commission, "f"),
                                now_ns,
                                scope.key,
                                observation.intent_id,
                            ),
                        )
                        self._append_event(
                            cursor,
                            intent_id=observation.intent_id,
                            scope_key=scope.key,
                            event_type="provider_commission_evidence",
                            state=target,
                            payload={
                                "cumulative_commission": format(
                                    observation.cumulative_commission, "f"
                                ),
                                "source": source,
                            },
                            now_ns=now_ns,
                        )
                        updated = self._fetch_record(cursor, scope.key, observation.intent_id)
                        assert updated is not None
                        return self._record_from_row(updated)
                raise InvalidStateTransition("conflicting duplicate provider observation")
            if record.is_terminal:
                if target in {ExecutionState.ACKED, ExecutionState.PARTIALLY_FILLED}:
                    return record
                raise InvalidStateTransition("conflicting terminal provider observation")
            if not can_transition(record.state, target):
                raise InvalidStateTransition()
            payload = json.loads(row["payload_json"])
            intent_quantity = Decimal(payload["quantity"])
            self._validate_fill_progress(record, observation, intent_quantity)
            if (
                record.provider_order_id is not None
                and observation.provider_order_id is not None
                and record.provider_order_id != observation.provider_order_id
            ):
                raise InvalidStateTransition("provider order identity changed")
            provider_order_id = observation.provider_order_id or record.provider_order_id
            cursor.execute(
                """
                UPDATE execution_records
                SET state = ?, provider_order_id = ?, filled_quantity = ?, average_price = ?,
                    cumulative_commission = ?, unknown_reason = NULL, review_required = ?,
                    updated_at_ns = ?
                WHERE scope_key = ? AND intent_id = ?
                """,
                (
                    target.value,
                    provider_order_id,
                    format(observation.filled_quantity, "f"),
                    None
                    if observation.average_price is None
                    else format(observation.average_price, "f"),
                    (
                        format(observation.cumulative_commission, "f")
                        if observation.cumulative_commission is not None
                        else row["cumulative_commission"]
                    ),
                    0 if target in TERMINAL_STATES else record.review_required,
                    now_ns,
                    scope.key,
                    observation.intent_id,
                ),
            )
            self._append_event(
                cursor,
                intent_id=observation.intent_id,
                scope_key=scope.key,
                event_type="provider_observation"
                if source == "provider"
                else "reconciled_observation",
                state=target,
                payload={
                    "provider_order_id": provider_order_id,
                    "filled_quantity": format(observation.filled_quantity, "f"),
                    "cumulative_commission": (
                        None
                        if observation.cumulative_commission is None
                        else format(observation.cumulative_commission, "f")
                    ),
                    "reason_code": observation.reason_code,
                    "source": source,
                },
                now_ns=now_ns,
            )
            updated = self._fetch_record(cursor, scope.key, observation.intent_id)
            assert updated is not None
            return self._record_from_row(updated)

    @staticmethod
    def _validate_fill_progress(
        record: ExecutionRecord,
        observation: ProviderObservation,
        intent_quantity: Decimal,
    ) -> None:
        filled = observation.filled_quantity
        if filled < record.filled_quantity or filled > intent_quantity:
            raise InvalidStateTransition("non-monotonic filled_quantity")
        if observation.state is ExecutionState.ACKED and filled != 0:
            raise InvalidStateTransition("acknowledged order cannot include fills")
        if observation.state is ExecutionState.PARTIALLY_FILLED and not (
            Decimal("0") < filled < intent_quantity
        ):
            raise InvalidStateTransition("partial fill must be within intent quantity")
        if observation.state is ExecutionState.FILLED and filled != intent_quantity:
            raise InvalidStateTransition("filled quantity must equal intent quantity")
        if observation.state is ExecutionState.REJECTED and filled != 0:
            raise InvalidStateTransition("rejected order cannot include fills")

    def mark_review_required(
        self,
        intent_id: str,
        scope: ExecutionScope,
        reason_code: str,
        *,
        writer_lease: WriterLease | None = None,
    ) -> ExecutionRecord:
        """Persist a manual-review latch without fabricating a provider outcome."""

        if not reason_code or not reason_code.replace("_", "").isalnum():
            raise ContractValidationError("invalid reason_code")
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = self._fetch_record(cursor, scope.key, intent_id)
            if row is None:
                raise ContractValidationError("unknown intent_id")
            record = self._record_from_row(row)
            cursor.execute(
                """
                UPDATE execution_records
                SET review_required = 1, updated_at_ns = ?
                WHERE scope_key = ? AND intent_id = ?
                """,
                (now_ns, scope.key, intent_id),
            )
            self._append_event(
                cursor,
                intent_id=intent_id,
                scope_key=scope.key,
                event_type="manual_review_required",
                state=record.state,
                payload={"reason_code": reason_code},
                now_ns=now_ns,
            )
            updated = self._fetch_record(cursor, scope.key, intent_id)
            assert updated is not None
            return self._record_from_row(updated)

    def read_outbox(
        self,
        *,
        after_sequence: int = 0,
        limit: int = 100,
        scope: ExecutionScope | None = None,
    ) -> tuple[ExecutionEvent, ...]:
        """Read an ordered, replayable outbox page without advancing a consumer cursor."""

        if type(after_sequence) is not int or after_sequence < 0:
            raise ContractValidationError("invalid after_sequence")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ContractValidationError("invalid outbox limit")
        with self._lock:
            try:
                if scope is None:
                    rows = self._connection.execute(
                        """
                        SELECT sequence, event_id, intent_id, scope_key, event_type, state, payload_json,
                               created_at_ns
                        FROM execution_outbox WHERE sequence > ? ORDER BY sequence ASC LIMIT ?
                        """,
                        (after_sequence, limit),
                    ).fetchall()
                else:
                    rows = self._connection.execute(
                        """
                        SELECT sequence, event_id, intent_id, scope_key, event_type, state, payload_json,
                               created_at_ns
                        FROM execution_outbox
                        WHERE sequence > ? AND scope_key = ?
                        ORDER BY sequence ASC LIMIT ?
                        """,
                        (after_sequence, scope.key, limit),
                    ).fetchall()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read execution outbox") from error
        try:
            return tuple(
                ExecutionEvent(
                    sequence=row["sequence"],
                    event_id=row["event_id"],
                    intent_id=row["intent_id"],
                    scope_key=row["scope_key"],
                    event_type=row["event_type"],
                    state=ExecutionState(row["state"]),
                    payload=json.loads(row["payload_json"]),
                    created_at_ns=row["created_at_ns"],
                )
                for row in rows
            )
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise DurableStoreError("stored execution outbox is unreadable") from error

    def read_cancel_outbox(
        self,
        *,
        after_sequence: int = 0,
        limit: int = 100,
        scope: ExecutionScope | None = None,
    ) -> tuple[CancelEvent, ...]:
        """Read an ordered cancellation lifecycle page without advancing a cursor."""

        if type(after_sequence) is not int or after_sequence < 0:
            raise ContractValidationError("invalid cancellation outbox after_sequence")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ContractValidationError("invalid cancellation outbox limit")
        with self._lock:
            try:
                if scope is None:
                    rows = self._connection.execute(
                        """
                        SELECT sequence, event_id, cancel_id, target_intent_id, scope_key, event_type,
                               state, payload_json, created_at_ns
                        FROM cancellation_outbox WHERE sequence > ? ORDER BY sequence ASC LIMIT ?
                        """,
                        (after_sequence, limit),
                    ).fetchall()
                else:
                    rows = self._connection.execute(
                        """
                        SELECT sequence, event_id, cancel_id, target_intent_id, scope_key, event_type,
                               state, payload_json, created_at_ns
                        FROM cancellation_outbox
                        WHERE sequence > ? AND scope_key = ?
                        ORDER BY sequence ASC LIMIT ?
                        """,
                        (after_sequence, scope.key, limit),
                    ).fetchall()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read cancellation outbox") from error
        try:
            return tuple(
                CancelEvent(
                    sequence=row["sequence"],
                    event_id=row["event_id"],
                    cancel_id=row["cancel_id"],
                    target_intent_id=row["target_intent_id"],
                    scope_key=row["scope_key"],
                    event_type=row["event_type"],
                    state=ExecutionState(row["state"]),
                    payload=json.loads(row["payload_json"]),
                    created_at_ns=row["created_at_ns"],
                )
                for row in rows
            )
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise DurableStoreError("stored cancellation outbox is unreadable") from error

    def acquire_or_renew_lease(
        self,
        scope: ExecutionScope,
        owner_id: str,
        *,
        ttl_ns: int = 5_000_000_000,
    ) -> WriterLease:
        """Acquire or renew the sole dispatch writer lease for one account scope."""

        if not owner_id or owner_id != owner_id.strip() or len(owner_id) > 128:
            raise ContractValidationError("invalid writer owner_id")
        if type(ttl_ns) is not int or not 1_000_000 <= ttl_ns <= 300_000_000_000:
            raise ContractValidationError("invalid lease ttl_ns")
        now_ns = time.time_ns()
        expires_at_ns = now_ns + ttl_ns
        scope_key = scope.account_key
        with self._transaction() as cursor:
            row = cursor.execute(
                "SELECT * FROM execution_writer_leases WHERE scope_key = ?", (scope_key,)
            ).fetchone()
            if row is None:
                token = 1
                cursor.execute(
                    """
                    INSERT INTO execution_writer_leases(scope_key, owner_id, fencing_token, expires_at_ns)
                    VALUES (?, ?, ?, ?)
                    """,
                    (scope_key, owner_id, token, expires_at_ns),
                )
            elif row["owner_id"] == owner_id and row["expires_at_ns"] > now_ns:
                token = row["fencing_token"]
                cursor.execute(
                    """
                    UPDATE execution_writer_leases SET expires_at_ns = ? WHERE scope_key = ?
                    """,
                    (expires_at_ns, scope_key),
                )
            elif row["expires_at_ns"] <= now_ns:
                # An expired lease is a new generation even if a restarted
                # process reuses the same writer_id.  Otherwise a stale owner
                # could retain the old token and mutate after a timeout.
                token = row["fencing_token"] + 1
                cursor.execute(
                    """
                    UPDATE execution_writer_leases
                    SET owner_id = ?, fencing_token = ?, expires_at_ns = ?
                    WHERE scope_key = ?
                    """,
                    (owner_id, token, expires_at_ns, scope_key),
                )
            else:
                raise WriterLeaseUnavailable()
        return WriterLease(scope_key, owner_id, token, expires_at_ns)

    def release_lease(
        self,
        scope: ExecutionScope,
        owner_id: str,
        *,
        fencing_token: int,
    ) -> bool:
        """Release only the exact current owner/token lease generation."""

        if type(fencing_token) is not int or fencing_token <= 0:
            raise ContractValidationError("invalid writer fencing_token")

        with self._transaction() as cursor:
            result = cursor.execute(
                """
                DELETE FROM execution_writer_leases
                WHERE scope_key = ? AND owner_id = ? AND fencing_token = ?
                """,
                (scope.account_key, owner_id, fencing_token),
            )
            return result.rowcount == 1
