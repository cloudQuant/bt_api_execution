"""SQLite-backed durable state for the first public execution spine.

This store owns only execution facts: immutable intent identity, child progress,
the replayable outbox, and the execution writer lease.  It intentionally does
not contain a risk ledger or provider transport.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from collections.abc import Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from threading import RLock
from types import MappingProxyType
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


_CTP_ACCOUNT_REF_PREFIX = "ctp-account-ref.v1:"
_CTP_ACCOUNT_FAMILY_PREFIX = "ctp-account-family.v1:"
_CTP_ACCOUNT_FAMILY_DOMAIN = b"bt-api-execution/ctp-account-family/v1\0"
_CTP_ACCOUNT_STORE_TABLES = frozenset(
    {
        "execution_meta",
        "execution_records",
        "execution_outbox",
        "cancellation_records",
        "cancellation_outbox",
        "execution_writer_leases",
        "ctp_account_family_owners",
        "ctp_account_store_identity",
        "ctp_account_family_legacy_fences",
        "ctp_order_identity_reservations",
        "ctp_order_target_projections",
        "ctp_order_target_projection_consumptions",
        "ctp_order_ref_watermarks",
        "ctp_order_ref_account_watermarks",
        "ctp_order_ref_cutover_sessions",
        "ctp_order_ref_legacy_imports",
        "ctp_dispatch_commands",
        "ctp_native_action_ref_counters",
        "ctp_native_action_ref_allocations",
        "ctp_dispatch_callback_ledger",
        "ctp_dispatch_trade_fact_ledger",
        "ctp_dispatch_order_cumulative_ledger",
        "ctp_dispatch_callback_source_lifecycle_fences",
        "ctp_dispatch_callback_session_owners",
        "ctp_dispatch_callback_ingress",
        "ctp_dispatch_callback_ingress_applications",
        "ctp_dispatch_order_projection",
        "ctp_dispatch_cancel_projection",
        "ctp_dispatch_unknown_resolutions",
        "ctp_dispatch_authority_uses",
        "ctp_dispatch_cancel_postconditions",
        "ctp_dispatch_cancel_terminal_observations",
        "ctp_dispatch_cancel_postcondition_resolutions",
    }
)
_CTP_ACCOUNT_STORE_INDEXES = frozenset(
    {
        "execution_outbox_intent_sequence",
        "cancellation_records_target",
        "cancellation_outbox_cancel_sequence",
        "ctp_order_identity_scope_day",
        "ctp_order_target_projection_latest",
        "ctp_order_target_query_request_once",
        "ctp_order_target_consumption_command",
        "ctp_dispatch_commands_scope_status",
        "ctp_dispatch_commands_account_status",
        "ctp_dispatch_callback_command",
        "ctp_dispatch_trade_command",
        "ctp_dispatch_order_cumulative_command",
        "ctp_dispatch_callback_source_lifecycle_account",
        "ctp_dispatch_cancel_postconditions_target",
        "ctp_dispatch_cancel_terminal_by_target",
        "ctp_dispatch_session_request_unique",
        "ctp_dispatch_managed_action_unique",
        "ctp_dispatch_native_action_ref_unique",
        "ctp_dispatch_local_queue_receipt_unique",
    }
)
_CTP_ACCOUNT_STORE_TRIGGERS = frozenset(
    {
        "ctp_account_family_owner_immutable_identity",
        "ctp_account_family_owner_no_delete",
        "ctp_account_store_identity_immutable_update",
        "ctp_account_store_identity_immutable_delete",
        "ctp_account_family_legacy_fence_immutable_update",
        "ctp_account_family_legacy_fence_immutable_delete",
        "ctp_order_ref_cutover_immutable_update",
        "ctp_order_ref_cutover_immutable_delete",
        "ctp_order_ref_legacy_import_immutable_update",
        "ctp_order_ref_legacy_import_immutable_delete",
        "ctp_native_action_ref_counter_monotonic",
        "ctp_native_action_ref_counter_no_delete",
        "ctp_native_action_ref_allocations_immutable_update",
        "ctp_native_action_ref_allocations_immutable_delete",
        "ctp_dispatch_session_owner_write_once",
        "ctp_dispatch_trade_fact_immutable_update",
        "ctp_dispatch_trade_fact_immutable_delete",
        "ctp_dispatch_order_cumulative_immutable_update",
        "ctp_dispatch_order_cumulative_immutable_delete",
        "ctp_dispatch_callback_source_lifecycle_immutable_update",
        "ctp_dispatch_callback_source_lifecycle_immutable_delete",
        "ctp_callback_session_owner_immutable_identity",
        "ctp_callback_session_owner_no_delete",
        "ctp_callback_ingress_immutable_update",
        "ctp_callback_ingress_immutable_delete",
        "ctp_callback_ingress_applications_immutable_update",
        "ctp_callback_ingress_applications_immutable_delete",
        "ctp_dispatch_callback_immutable_update",
        "ctp_dispatch_callback_immutable_delete",
        "ctp_dispatch_resolution_immutable_update",
        "ctp_dispatch_resolution_immutable_delete",
        "ctp_dispatch_authority_uses_immutable_update",
        "ctp_dispatch_authority_uses_immutable_delete",
        "ctp_order_target_projections_immutable_update",
        "ctp_order_target_projections_immutable_delete",
        "ctp_order_target_consumptions_immutable_update",
        "ctp_order_target_consumptions_immutable_delete",
        "ctp_dispatch_cancel_postconditions_immutable_update",
        "ctp_dispatch_cancel_postconditions_immutable_delete",
        "ctp_dispatch_cancel_terminal_observations_immutable_update",
        "ctp_dispatch_cancel_terminal_observations_immutable_delete",
        "ctp_dispatch_cancel_postcondition_resolutions_immutable_update",
        "ctp_dispatch_cancel_postcondition_resolutions_immutable_delete",
        "ctp_dispatch_commands_immutable",
        "ctp_dispatch_queue_receipt_once",
    }
)


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
_CTP_ORDER_TARGET_MAX_TTL_NS = 5_000_000_000
_CTP_CALLBACK_OWNER_POISON_CODES = frozenset(
    {
        "candidate_composition_failure",
        "dispatch_stage_ambiguous",
        "dispatch_claim_failure",
        "owner_stop",
        "source_replaced",
        "disconnect",
        "lifecycle_transition",
        "source_gap",
        "queue_overflow",
        "unknown_source",
        "unsupported_financial",
        "capture_incomplete",
        "callback_handler_error",
        "append_failure",
        "append_commit_unknown",
        "native_call_ambiguous",
        "native_call_receipt_mismatch",
        "native_call_lease_failure",
        "source_identity_mismatch",
        "owner_binding_mismatch",
        "callback_route_mismatch",
        "callback_apply_failure",
    }
)


class _CtpCancelPostconditionConflictError(ContractValidationError):
    """A verified callback contradicts an already-resolved cancel target."""

    def __init__(self, owner_intent_id: str):
        super().__init__("verified CTP callback contradicts a resolved cancel target")
        self.owner_intent_id = owner_intent_id


@dataclass(frozen=True, slots=True)
class WriterLease:
    """A scope-local writer lease with a monotonically increasing fence."""

    scope_key: str
    owner_id: str
    fencing_token: int
    expires_at_ns: int
    family_key: str | None = None
    family_owner_intent_id: str | None = None


@dataclass(frozen=True, slots=True)
class CtpAccountFamilyOwnerHandle:
    """Same-Store handle for the one durable owner of a canonical CTP account family."""

    family_key: str
    owner_intent_id: str
    account_key: str
    scope_key: str

    def __post_init__(self) -> None:
        if not _is_prefixed_digest(self.family_key, "ctp-account-family.v1:"):
            raise ContractValidationError("invalid CTP account family identity")
        if not _is_local_queue_receipt_id(self.owner_intent_id):
            raise ContractValidationError("invalid CTP account family owner identity")
        if not _is_prefixed_digest(self.account_key, "account:"):
            raise ContractValidationError("invalid CTP account family account key")
        if not _is_prefixed_digest(self.scope_key, "scope:"):
            raise ContractValidationError("invalid CTP account family scope key")


@dataclass(frozen=True, slots=True)
class CtpAccountStoreIdentity:
    """Read-only identity facts for a Store opened through the CTP factory.

    This describes the opened local database. It is not a writer lease, source
    proof, or defense against a hostile process replacing or copying files.
    """

    ledger_kind: str
    account_ref: str
    family_key: str
    journal_incarnation_id: str
    family_owner_state: str | None
    family_owner_intent_id: str | None
    database_filename: str
    file_device: int | None
    file_id: int | None


@dataclass(frozen=True, slots=True)
class CtpAccountStoreFileInspection:
    """Read-only file classification used before either journal opens SQLite for writes."""

    kind: str
    identity: CtpAccountStoreIdentity | None

    def __post_init__(self) -> None:
        if self.kind not in {"MISSING", "LEGACY_CTP_JOURNAL", "CTP_EXECUTION_STORE"}:
            raise ContractValidationError("invalid CTP account store file kind")
        if (self.kind == "CTP_EXECUTION_STORE") != (self.identity is not None):
            raise ContractValidationError("CTP account store inspection identity is inconsistent")


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
class CtpVerifiedOrderTargetProjection:
    """Verifier output for one fresh, uniquely matched native CTP order row.

    This value is accepted only as the return from an injected
    ``CtpOrderTargetProjectionVerifier``. Its digest fields are references to
    source evidence; they do not authenticate a caller-constructed value.
    The verifier must establish the exact native query source, current-result
    readback, scope, registration and single OPEN/PARTIAL match.
    """

    account_key: str
    scope_key: str
    trading_day: str
    managed_intent_id: str
    runtime_order_id: str
    order_ref: str
    account_fingerprint_sha256: str
    registration_digest: str
    instrument_id: str
    exchange_id: str
    session_generation_id: str
    connection_generation: int
    query_front_id: int
    query_session_id: int
    query_request_id: int
    query_filters_sha256: str
    query_records_sha256: str
    source_evidence_sha256: str
    query_record_count: int
    query_match_count: int
    query_complete: bool
    query_terminal: bool
    query_timed_out: bool
    query_error_id: int | None
    late_callback_count: int
    order_sys_id: str
    front_id: int
    session_id: int
    provider_state: str
    quantity: int
    traded_quantity: int
    remaining_quantity: int
    verifier_id: str
    verified_at_ns: int
    expires_at_ns: int

    def __post_init__(self) -> None:
        for name in (
            "account_key",
            "scope_key",
            "managed_intent_id",
            "runtime_order_id",
            "session_generation_id",
            "instrument_id",
            "exchange_id",
            "order_sys_id",
            "verifier_id",
        ):
            _validate_correlation_text(getattr(self, name), "CTP target " + name)
        if (
            type(self.trading_day) is not str
            or len(self.trading_day) != 8
            or not self.trading_day.isdigit()
        ):
            raise ContractValidationError("invalid CTP target trading day")
        if (
            type(self.order_ref) is not str
            or len(self.order_ref) != 12
            or not self.order_ref.isascii()
            or not self.order_ref.isdigit()
        ):
            raise ContractValidationError("invalid CTP target OrderRef")
        for name in (
            "account_fingerprint_sha256",
            "registration_digest",
            "query_filters_sha256",
            "query_records_sha256",
            "source_evidence_sha256",
        ):
            if not _is_sha256(getattr(self, name)):
                raise ContractValidationError("invalid CTP target " + name)
        for name in (
            "connection_generation",
            "query_front_id",
            "query_session_id",
            "query_request_id",
            "front_id",
            "session_id",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ContractValidationError("invalid CTP target " + name)
        if self.query_request_id > 2_147_483_647:
            raise ContractValidationError("invalid CTP target query request id")
        if (
            type(self.query_record_count) is not int
            or self.query_record_count <= 0
            or type(self.query_match_count) is not int
            or self.query_match_count != 1
        ):
            raise ContractValidationError("CTP target query is ambiguous or empty")
        if (
            self.query_complete is not True
            or self.query_terminal is not True
            or self.query_timed_out is not False
            or self.query_error_id not in (None, 0)
            or type(self.query_error_id) not in (int, type(None))
            or type(self.late_callback_count) is not int
            or self.late_callback_count != 0
        ):
            raise ContractValidationError("CTP target query is incomplete or unstable")
        if self.provider_state not in {"OPEN", "PARTIAL"}:
            raise ContractValidationError("CTP cancel target is not currently open")
        if (
            type(self.quantity) is not int
            or self.quantity <= 0
            or type(self.traded_quantity) is not int
            or not 0 <= self.traded_quantity < self.quantity
            or type(self.remaining_quantity) is not int
            or self.remaining_quantity != self.quantity - self.traded_quantity
            or self.remaining_quantity <= 0
            or (self.provider_state == "OPEN" and self.traded_quantity != 0)
            or (self.provider_state == "PARTIAL" and self.traded_quantity == 0)
        ):
            raise ContractValidationError("CTP target open quantity is inconsistent")
        if (
            type(self.verified_at_ns) is not int
            or self.verified_at_ns <= 0
            or type(self.expires_at_ns) is not int
            or not self.verified_at_ns < self.expires_at_ns
            or self.expires_at_ns - self.verified_at_ns > _CTP_ORDER_TARGET_MAX_TTL_NS
        ):
            raise ContractValidationError("invalid CTP target freshness interval")

    def to_payload(self) -> dict[str, Any]:
        return {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
        }


@dataclass(frozen=True, slots=True)
class CtpOrderTargetProjectionHandle:
    """Ephemeral same-store handle returned only after immutable row readback."""

    projection_id: str
    projection_sha256: str
    projection: CtpVerifiedOrderTargetProjection


@dataclass(frozen=True, slots=True)
class CtpCallbackSessionOwnerHandle:
    """Exact Store-instance handle for one durable CTP callback session owner."""

    owner_intent_id: str
    account_key: str
    scope_key: str

    def __post_init__(self) -> None:
        if not _is_local_queue_receipt_id(self.owner_intent_id):
            raise ContractValidationError("invalid CTP callback owner intent id")
        if not _is_prefixed_digest(self.account_key, "account:") or not _is_prefixed_digest(
            self.scope_key, "scope:"
        ):
            raise ContractValidationError("invalid CTP callback owner scope")


@dataclass(frozen=True, slots=True)
class CtpCallbackIngressCommit:
    """Readback of one synchronously committed SDK callback inbox append."""

    owner_intent_id: str
    source_sequence: int
    record_digest_sha256: str
    committed_high_watermark: int
    status: str = "COMMITTED"


@dataclass(frozen=True, slots=True)
class CtpCallbackIngressEventV1:
    """Exact Store-issued readback of one durable but unapplied ingress row."""

    owner_handle: CtpCallbackSessionOwnerHandle
    source_sequence: int
    record_digest_sha256: str
    callback_name: str
    callback_class: str
    source_phase: str
    source_tags: tuple[str, str, str, str, int, int]
    connection_generation: int
    record_payload_json: str

    def __post_init__(self) -> None:
        if type(self.owner_handle) is not CtpCallbackSessionOwnerHandle:
            raise ContractValidationError("typed CTP callback session owner is required")
        if (
            type(self.source_sequence) is not int
            or self.source_sequence <= 0
            or not _is_sha256(self.record_digest_sha256)
            or type(self.callback_name) is not str
            or not self.callback_name
            or type(self.callback_class) is not str
            or not self.callback_class
            or self.source_phase not in {"PRE_LOGIN", "ACTIVE", "POISONED"}
            or type(self.source_tags) is not tuple
            or len(self.source_tags) != 6
            or any(type(value) is not str or not value for value in self.source_tags[:4])
            or type(self.source_tags[4]) is not int
            or self.source_tags[4] <= 0
            or type(self.source_tags[5]) is not int
            or self.source_tags[5] < 0
            or type(self.connection_generation) is not int
            or self.connection_generation < 0
            or type(self.record_payload_json) is not str
        ):
            raise ContractValidationError("invalid Store-issued CTP callback ingress event")


@dataclass(frozen=True, slots=True)
class CtpCallbackSessionPoisonCommit:
    """Durable readback after a monotonic session-owner poison transition."""

    owner_intent_id: str
    durable_state: str
    last_source_sequence: int
    poison_code: str
    committed: bool = True


@dataclass(frozen=True, slots=True)
class CtpCallbackSessionBindingV1:
    """Exact Store readback of the SDK-accepted login/session identity."""

    owner_intent_id: str
    account_key: str
    scope_key: str
    trading_day: str
    session_generation_id: str
    dispatch_front_id: int
    dispatch_session_id: int
    source_instance_id: str
    native_client_epoch: str
    native_api_source_id: str
    native_spi_source_id: str
    native_api_generation: int
    source_connection_generation: int
    connection_generation: int
    source_high_watermark: int
    session_binding_sha256: str


@dataclass(frozen=True, slots=True)
class CtpTraderCallbackIngressPoisonAckV2:
    """SDK-shaped acknowledgement for one durable monotonic poison write."""

    owner_intent_id: str
    durable_state: str
    last_source_sequence: int
    poison_code: str
    committed: bool = True


@dataclass(frozen=True, slots=True)
class CtpManagedNativeCallBindingV2:
    """One-use, Store-issued binding for one claimed native CTP request.

    This is an in-process source bridge contract. Its object identity is
    registered by one live Store instance and checked by the install-time SDK
    verifier; the fields alone are not authority or hostile-process isolation.
    """

    binding_id: str
    owner_intent_id: str
    account_key: str
    scope_key: str
    command_id: str
    operation: str
    trading_day: str
    request_payload_json: str
    request_payload_sha256: str
    reservation_managed_intent_id: str
    managed_action_id: str
    runtime_order_id: str
    order_ref: str
    native_request_id: int
    native_action_ref: int | None
    native_request_payload_json: str
    native_request_payload_sha256: str
    cancel_target_order_ref: str | None
    cancel_target_exchange_id: str | None
    cancel_target_order_sys_id: str | None
    cancel_target_front_id: int | None
    cancel_target_session_id: int | None
    session_binding_sha256: str
    session_generation_id: str
    dispatch_front_id: int
    dispatch_session_id: int
    writer_owner_id: str
    writer_fencing_token: int
    expires_at_ns: int

    def __post_init__(self) -> None:
        if not _is_local_queue_receipt_id(self.binding_id):
            raise ContractValidationError("invalid CTP native-call binding id")
        for value, name in (
            (self.owner_intent_id, "owner intent id"),
            (self.command_id, "command id"),
            (self.reservation_managed_intent_id, "managed intent id"),
            (self.managed_action_id, "managed action id"),
            (self.runtime_order_id, "runtime order id"),
            (self.order_ref, "OrderRef"),
            (self.session_generation_id, "session generation"),
            (self.writer_owner_id, "writer owner id"),
        ):
            SqliteExecutionStore._validate_command_identifier(value, name)
        if self.operation not in {"SUBMIT", "CANCEL"}:
            raise ContractValidationError("invalid CTP native-call binding operation")
        if not _is_prefixed_digest(self.account_key, "account:") or not _is_prefixed_digest(
            self.scope_key, "scope:"
        ):
            raise ContractValidationError("invalid CTP native-call binding scope")
        if (
            not _is_sha256(self.request_payload_sha256)
            or not _is_sha256(self.native_request_payload_sha256)
            or not _is_sha256(self.session_binding_sha256)
        ):
            raise ContractValidationError("invalid CTP native-call binding digest")
        if self.request_payload_json != canonical_json(
            json.loads(self.request_payload_json)
        ) or payload_sha256(json.loads(self.request_payload_json)) != self.request_payload_sha256:
            raise ContractValidationError("invalid CTP native-call binding payload")
        try:
            logical_payload = json.loads(self.request_payload_json)
            native_payload = json.loads(self.native_request_payload_json)
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid CTP native-call binding native payload") from error
        expected_native_payload = dict(logical_payload)
        if "OrderActionRef" in logical_payload:
            raise ContractValidationError("logical native-call payload contains ActionRef")
        if self.operation == "CANCEL":
            expected_native_payload["OrderActionRef"] = self.native_action_ref
        if (
            not isinstance(native_payload, dict)
            or canonical_json(native_payload) != self.native_request_payload_json
            or payload_sha256(native_payload) != self.native_request_payload_sha256
            or native_payload != expected_native_payload
        ):
            raise ContractValidationError("invalid CTP native-call binding native payload")
        if (
            type(self.native_request_id) is not int
            or self.native_request_id <= 0
            or self.native_request_id > 2_147_483_647
            or type(self.dispatch_front_id) is not int
            or self.dispatch_front_id <= 0
            or type(self.dispatch_session_id) is not int
            or self.dispatch_session_id <= 0
            or type(self.writer_fencing_token) is not int
            or self.writer_fencing_token <= 0
            or type(self.expires_at_ns) is not int
            or self.expires_at_ns <= 0
        ):
            raise ContractValidationError("invalid CTP native-call binding numeric identity")
        if self.operation == "SUBMIT":
            if any(
                value is not None
                for value in (
                    self.native_action_ref,
                    self.cancel_target_order_ref,
                    self.cancel_target_exchange_id,
                    self.cancel_target_order_sys_id,
                    self.cancel_target_front_id,
                    self.cancel_target_session_id,
                )
            ):
                raise ContractValidationError("submit native-call binding has cancel identity")
        elif (
            type(self.native_action_ref) is not int
            or not 1 <= self.native_action_ref <= 2_147_483_647
            or self.cancel_target_order_ref != self.order_ref
            or not self.cancel_target_exchange_id
            or not self.cancel_target_order_sys_id
            or type(self.cancel_target_front_id) is not int
            or self.cancel_target_front_id <= 0
            or type(self.cancel_target_session_id) is not int
            or self.cancel_target_session_id <= 0
        ):
            raise ContractValidationError("cancel native-call binding lacks complete target")

    def to_payload(self) -> dict[str, Any]:
        """Return the frozen SDK verifier handoff shape."""

        return {
            "binding_type": "ctp_managed_native_call_binding.v2",
            "owner_intent_id": self.owner_intent_id,
            "account_key": self.account_key,
            "scope_key": self.scope_key,
            "command_id": self.command_id,
            "operation": self.operation,
            "trading_day": self.trading_day,
            "request_payload_json": self.request_payload_json,
            "request_payload_sha256": self.request_payload_sha256,
            "native_request_payload_json": self.native_request_payload_json,
            "native_request_payload_sha256": self.native_request_payload_sha256,
            "reservation_managed_intent_id": self.reservation_managed_intent_id,
            "managed_action_id": self.managed_action_id,
            "runtime_order_id": self.runtime_order_id,
            "order_ref": self.order_ref,
            "native_request_id": self.native_request_id,
            "native_action_ref": self.native_action_ref,
            "cancel_target_order_ref": self.cancel_target_order_ref,
            "cancel_target_exchange_id": self.cancel_target_exchange_id,
            "cancel_target_order_sys_id": self.cancel_target_order_sys_id,
            "cancel_target_front_id": self.cancel_target_front_id,
            "cancel_target_session_id": self.cancel_target_session_id,
            "session_binding_sha256": self.session_binding_sha256,
            "session_generation_id": self.session_generation_id,
            "dispatch_front_id": self.dispatch_front_id,
            "dispatch_session_id": self.dispatch_session_id,
            "writer_owner_id": self.writer_owner_id,
            "writer_fencing_token": self.writer_fencing_token,
            "expires_at_ns": self.expires_at_ns,
        }


# Kept as an import alias for callers that only need to name the in-process
# binding protocol. Store claim APIs issue V2 objects exclusively.
CtpManagedNativeCallBindingV1 = CtpManagedNativeCallBindingV2


@dataclass(frozen=True, slots=True)
class CtpSessionNativeCallClaim:
    """One claimed command and its exact Store-issued SDK send binding."""

    command: CtpDispatchCommand
    binding: CtpManagedNativeCallBindingV2


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
    native_action_ref: str | int | None
    native_request_payload_sha256: str | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version not in {1, 2}:
            raise ContractValidationError("unsupported CTP dispatch correlation version")
        if self.version == 1:
            if self.native_request_payload_sha256 is not None:
                raise ContractValidationError("legacy CTP correlation cannot bind native payload")
        elif not _is_sha256(self.native_request_payload_sha256):
            raise ContractValidationError("CTP correlation requires native payload digest")
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
            if self.version == 1:
                _validate_correlation_text(self.native_action_ref, "native action reference")
            elif (
                type(self.native_action_ref) is not int
                or not 1 <= self.native_action_ref <= 2_147_483_647
            ):
                raise ContractValidationError("invalid native ActionRef integer")
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
            if self.version == 2 and (
                type(self.native_action_ref) is not int
                or not 1 <= self.native_action_ref <= 2_147_483_647
            ):
                raise ContractValidationError("cancel correlation requires Store ActionRef integer")
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

        payload = {
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
        if self.version == 2:
            payload["native_request_payload_sha256"] = self.native_request_payload_sha256
        return payload


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
    native_action_ref: str | int | None
    order_ref: str
    exchange_id: str | None = None
    order_sys_id: str | None = None
    target_front_id: int | None = None
    target_session_id: int | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version not in {1, 2}:
            raise ContractValidationError("unsupported CTP callback key version")
        if type(self.correlation_key) is not CtpDispatchCorrelationKey:
            raise ContractValidationError("typed CTP dispatch correlation key is required")
        if self.version != self.correlation_key.version:
            raise ContractValidationError("CTP callback and correlation versions differ")
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
            if self.version == 1:
                _validate_correlation_text(self.native_action_ref, "callback action reference")
            elif (
                type(self.native_action_ref) is not int
                or not 1 <= self.native_action_ref <= 2_147_483_647
            ):
                raise ContractValidationError("invalid callback ActionRef integer")
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
            if self.version == 2 and (
                type(self.native_action_ref) is not int
                or self.native_action_ref != self.correlation_key.native_action_ref
            ):
                raise ContractValidationError("cancel callback ActionRef does not match action")
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


@dataclass(frozen=True, slots=True)
class CtpVerifiedTradeFactEvidence:
    """Injected verifier result for one native TradeField without fabricated IDs."""

    evidence_type: str
    owner_intent_id: str
    source_sequence: int
    ingress_record_digest_sha256: str
    event_id: str
    trade_fact_sha256: str
    source_digest_sha256: str
    verifier_id: str
    verified_at_ns: int
    expires_at_ns: int

    def __post_init__(self) -> None:
        if self.evidence_type != "ctp_verified_trade_fact.v2":
            raise ContractValidationError("invalid verified CTP trade evidence type")
        if not _is_local_queue_receipt_id(self.owner_intent_id):
            raise ContractValidationError("invalid verified CTP trade owner")
        if type(self.source_sequence) is not int or self.source_sequence <= 0:
            raise ContractValidationError("invalid verified CTP trade sequence")
        for value in (
            self.ingress_record_digest_sha256,
            self.trade_fact_sha256,
            self.source_digest_sha256,
        ):
            if not _is_sha256(value):
                raise ContractValidationError("invalid verified CTP trade digest")
        _validate_correlation_text(self.event_id, "trade event id")
        _validate_correlation_text(self.verifier_id, "trade verifier id")
        if (
            type(self.verified_at_ns) is not int
            or type(self.expires_at_ns) is not int
            or self.expires_at_ns <= self.verified_at_ns
        ):
            raise ContractValidationError("invalid CTP trade verification interval")


class CtpDispatchTradeFactVerifier(Protocol):
    """Trusted adapter for one exact durable SDK ingress TradeField."""

    def verify_trade_fact(
        self,
        command: CtpDispatchCommand,
        trade_fact: Any,
        source_event: CtpCallbackIngressEventV1,
        *,
        now_ns: int,
    ) -> CtpVerifiedTradeFactEvidence: ...


@dataclass(frozen=True, slots=True)
class CtpDispatchTradeFactApplyResult:
    command_id: str
    event_id: str
    trade_quantity: int
    cumulative_trade_quantity: int
    projection_state: str | None
    duplicate: bool
    account_fence_open: bool


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


class CtpOrderTargetProjectionVerifier(Protocol):
    """Trusted adapter for one fresh, exact native CTP order query.

    The adapter must verify the native query result and its current SDK-owned
    readback, then bind exactly one OPEN/PARTIAL row to the supplied I9
    reservation. This package does not implement a native verifier. The
    durable contract and rejecting default do not authorize production
    cancellation; a fake verifier is suitable for local tests only.
    """

    def verify_order_target(
        self,
        scope: ExecutionScope,
        reservation: CtpOrderIdentityReservation,
        native_query_evidence: Any,
        *,
        now_ns: int,
    ) -> CtpVerifiedOrderTargetProjection: ...


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


class _RejectCtpOrderTargetProjectionVerifier:
    def verify_order_target(
        self,
        scope: ExecutionScope,
        reservation: CtpOrderIdentityReservation,
        native_query_evidence: Any,
        *,
        now_ns: int,
    ) -> CtpVerifiedOrderTargetProjection:
        raise ContractValidationError("trusted CTP order-target query verifier is required")


_REJECT_CTP_DISPATCH_VERIFIER = _RejectCtpDispatchVerifier()
_REJECT_CTP_ORDER_TARGET_PROJECTION_VERIFIER = _RejectCtpOrderTargetProjectionVerifier()


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
    native_request_payload: Mapping[str, Any] | None = None
    native_request_payload_sha256: str | None = None

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
                "native_request_payload_sha256": self.native_request_payload_sha256,
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
    # Version 11 adds immutable, same-process-fresh CTP order-target evidence.
    # Version 12 introduced a permanent callback source-lifecycle fence in a
    # separate candidate. Version 13 combines that fence with order-target
    # projections while migrating the distinct v11 lineages by old table facts.
    # Version 14 adds local journal lineage to new outbox rows only.
    # Version 15 adds a one-shot account callback session owner and durable
    # all-SPI ingress inbox, plus exact owner-bound native-call state.
    # Version 18 records account-wide cancel postconditions at the native
    # claim boundary and settles them only from verified terminal-order and
    # reconciled callback-inbox evidence.
    # Version 19 adds a mode-independent CTP account-family owner. V18 stored
    # only provider/environment/account and full-scope hashes, so an existing
    # journal with execution history receives a permanent unmapped-history
    # fence instead of guessing cross-mode aliases.
    # Version 20 adds immutable exact account identity for the code-owned CTP
    # journal factory. Existing V19 databases without this identity are not
    # implicitly bound by migration.
    _SCHEMA_VERSION = 20

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
            self._issued_ctp_order_target_projections: dict[
                str, CtpOrderTargetProjectionHandle
            ] = {}
            self._issued_ctp_account_family_owners: dict[
                str, CtpAccountFamilyOwnerHandle
            ] = {}
            self._issued_ctp_callback_session_owners: dict[
                str, CtpCallbackSessionOwnerHandle
            ] = {}
            self._issued_ctp_callback_session_adapters: dict[str, object] = {}
            self._ctp_callback_ingress_record_types: dict[str, type] = {}
            self._issued_ctp_staged_commands: dict[tuple[str, str], CtpDispatchCommand] = {}
            self._active_ctp_callback_sessions: dict[
                str, CtpCallbackSessionBindingV1
            ] = {}
            self._issued_ctp_callback_ingress_events: dict[
                tuple[str, int], CtpCallbackIngressEventV1
            ] = {}
            self._issued_ctp_native_call_bindings: dict[
                str, CtpManagedNativeCallBindingV2
            ] = {}
            self._consumed_ctp_native_call_bindings: set[str] = set()
            self._create_schema()
        except sqlite3.Error as error:
            raise DurableStoreError("unable to initialize execution store") from error

    def close(self) -> None:
        """Close the SQLite connection; no background thread is owned here."""

        with self._lock:
            self._connection.close()

    def journal_source_identity(self) -> dict[str, str | int]:
        """Return the persisted local journal lineage, not provider authority."""

        with self._lock:
            row = self._connection.execute(
                "SELECT value FROM execution_meta WHERE key = ?",
                ("journal_incarnation_id",),
            ).fetchone()
        if row is None:
            raise DurableStoreError("execution journal identity is unavailable")
        value = str(row["value"])
        if len(value) != 32 or any(character not in "0123456789abcdef" for character in value):
            raise DurableStoreError("execution journal identity is invalid")
        return {"generation_kind": "EXECUTION_JOURNAL", "generation": value, "epoch": 1}

    @classmethod
    def _ctp_account_store_path(
        cls, path: str | Path, scope: ExecutionScope | None
    ) -> Path:
        if scope is not None:
            cls._ctp_account_family_key(scope)
        try:
            raw_path = os.fspath(path)
        except TypeError as error:
            raise ContractValidationError("CTP execution store path must be a file path") from error
        if (
            type(raw_path) is not str
            or not raw_path
            or raw_path != raw_path.strip()
            or raw_path == ":memory:"
            or raw_path.lower().startswith("file:")
        ):
            raise ContractValidationError("CTP execution store requires a regular file path")
        candidate = Path(raw_path).expanduser()
        if not candidate.is_absolute():
            raise ContractValidationError("CTP execution store path must be absolute")
        try:
            if candidate.is_symlink():
                raise ContractValidationError("CTP execution store path cannot be a symlink")
            normalized = candidate.resolve(strict=False)
            if not normalized.parent.is_dir():
                raise ContractValidationError("CTP execution store parent directory is missing")
            if normalized.exists() and not normalized.is_file():
                raise ContractValidationError("CTP execution store path is not a regular file")
        except OSError as error:
            raise DurableStoreError("unable to inspect CTP execution store path") from error
        return normalized

    @staticmethod
    def _ctp_store_sidecar_state(path: Path) -> tuple[tuple[int, int] | None, ...]:
        signatures: list[tuple[int, int] | None] = []
        for suffix in ("", "-journal", "-wal", "-shm"):
            candidate = path if not suffix else Path(f"{path}{suffix}")
            if candidate.is_symlink():
                raise DurableStoreError("CTP execution store sidecar cannot be a symlink")
            try:
                stat_result = candidate.stat()
            except FileNotFoundError:
                signatures.append(None)
                continue
            except OSError as error:
                raise DurableStoreError("unable to inspect CTP execution store sidecars") from error
            # SQLite's shared-memory lock bytes can change during a read-only
            # WAL connection; the main DB and WAL content signatures remain
            # stable while their size/mtime pair is checked.
            signatures.append(
                (stat_result.st_size, 0 if suffix == "-shm" else stat_result.st_mtime_ns)
            )
        return tuple(signatures)

    @staticmethod
    def _ctp_preflight_state_is_stable(
        before: tuple[tuple[int, int] | None, ...],
        after: tuple[tuple[int, int] | None, ...],
    ) -> bool:
        if before[0] != after[0] or before[1] is not None or after[1] is not None:
            return False
        before_wal, after_wal = before[2], after[2]
        if before_wal is None:
            if after_wal is not None and after_wal[0] != 0:
                return False
        elif before_wal[0] == 0 and after_wal is not None and after_wal[0] == 0:
            pass
        elif before_wal != after_wal:
            return False
        before_shm, after_shm = before[3], after[3]
        if before_shm is None:
            # A read-only SQLite connection can create the 32 KiB WAL shared
            # memory file, and an empty WAL file, to coordinate its read lock.
            if after_shm is not None and after_shm[0] != 32_768:
                return False
        elif after_shm is None or before_shm[0] != after_shm[0]:
            return False
        return True

    @classmethod
    def _read_only_ctp_store_tables(cls, path: Path) -> set[str]:
        if not path.is_file():
            raise DurableStoreError("existing CTP journal is not a regular file")
        before = cls._ctp_store_sidecar_state(path)
        _, journal_signature, wal_signature, shm_signature = before
        if journal_signature is not None or (wal_signature is None) != (shm_signature is None):
            raise DurableStoreError("CTP journal has uncertain SQLite journal state")
        connection: sqlite3.Connection | None = None
        opened_path: Path | None = None
        try:
            connection = sqlite3.connect(
                f"{path.as_uri()}?mode=ro", uri=True, isolation_level=None, timeout=1.0
            )
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            database_row = connection.execute("PRAGMA database_list").fetchone()
            if database_row is None or database_row[1] != "main" or not database_row[2]:
                raise DurableStoreError("CTP journal database filename is unavailable")
            opened_path = Path(str(database_row[2])).resolve(strict=True)
            if os.path.normcase(str(opened_path)) != os.path.normcase(str(path)):
                raise DurableStoreError("CTP journal opened an unexpected database file")
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            legacy_table_names = {
                "ctp_sim_orders",
                "ctp_sim_journal_metadata",
                "ctp_sim_journal_scopes",
            }
            if tables & legacy_table_names:
                object_rows = connection.execute(
                    "SELECT type FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'"
                ).fetchall()
                if any(str(row[0]) != "table" for row in object_rows):
                    raise DurableStoreError("legacy CTP journal contains unsupported schema objects")
            # The legacy journal used to be allowed to start from an empty
            # orders-only table. Its constructor adds the scope column and
            # creates the metadata tables, but only when there are no rows to
            # bind to an unknown historical scope. Recognize that precise
            # pre-migration shape here so the legacy owner can preflight it
            # before enabling WAL. Do not classify partial or populated
            # layouts as legacy; the writable Store must reject those before
            # it can mutate them.
            if tables == {"ctp_sim_orders"}:
                legacy_order_columns = frozenset(
                    {
                        "client_order_id",
                        "approval_id",
                        "request_digest",
                        "request_json",
                        "state",
                        "order_ref",
                        "order_sys_id",
                        "front_id",
                        "session_id",
                        "status",
                        "traded_quantity",
                        "position_digest",
                        "updated_at",
                    }
                )
                columns = frozenset(
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(ctp_sim_orders)")
                )
                allowed_columns = {
                    legacy_order_columns,
                    legacy_order_columns | {"execution_scope_sha256"},
                }
                order_count = int(
                    connection.execute("SELECT COUNT(*) FROM ctp_sim_orders").fetchone()[0]
                )
                if columns not in allowed_columns or order_count != 0:
                    raise DurableStoreError(
                        "orders-only legacy CTP journal is populated or has an unknown schema"
                    )
            connection.execute("COMMIT")
        except DurableStoreError:
            if connection is not None:
                with suppress(sqlite3.Error):
                    connection.execute("ROLLBACK")
            raise
        except (sqlite3.Error, OSError, ValueError) as error:
            if connection is not None:
                with suppress(sqlite3.Error):
                    connection.execute("ROLLBACK")
            raise DurableStoreError("read-only CTP journal inspection failed") from error
        finally:
            if connection is not None:
                connection.close()
        if not cls._ctp_preflight_state_is_stable(
            before, cls._ctp_store_sidecar_state(path)
        ):
            raise DurableStoreError("CTP journal changed during read-only inspection")
        if opened_path is None:
            raise DurableStoreError("CTP journal database identity is unavailable")
        return tables

    @staticmethod
    def _ctp_store_schema_objects(connection: sqlite3.Connection) -> tuple[tuple[str, ...], ...]:
        rows = connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name NOT GLOB 'sqlite_*'"
        ).fetchall()
        return tuple(
            sorted(
                (
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    "" if row[3] is None else str(row[3]),
                )
                for row in rows
            )
        )

    @classmethod
    @lru_cache(maxsize=1)
    def _code_owned_ctp_store_schema_objects(cls) -> tuple[tuple[str, ...], ...]:
        """Build the exact current schema from code in an isolated memory DB."""

        reference = cls(":memory:")
        try:
            return cls._ctp_store_schema_objects(reference._connection)
        finally:
            reference.close()

    @classmethod
    def inspect_ctp_account_store_file(
        cls, path: str | Path, scope: ExecutionScope | None = None
    ) -> CtpAccountStoreFileInspection:
        """Classify an existing CTP journal before any writable SQLite open.

        A known legacy `_ExecutionJournal` layout is reported so its existing
        owner can continue using it. A V20 Store is returned only after exact
        identity validation. Unknown, partial, mismatched, or uncertain files
        fail closed; this inspection does not grant a writer lease.
        """

        target = cls._ctp_account_store_path(path, scope)
        if not os.path.lexists(str(target)):
            if any(
                os.path.lexists(str(Path(f"{target}{suffix}")))
                for suffix in ("-journal", "-wal", "-shm")
            ):
                raise DurableStoreError("CTP journal sidecars exist without a database")
            return CtpAccountStoreFileInspection("MISSING", None)
        tables = cls._read_only_ctp_store_tables(target)
        legacy_tables = {
            "ctp_sim_orders",
            "ctp_sim_journal_metadata",
            "ctp_sim_journal_scopes",
        }
        found_legacy_tables = tables & legacy_tables
        if found_legacy_tables:
            if found_legacy_tables == {"ctp_sim_orders"} and tables == {"ctp_sim_orders"}:
                # _read_only_ctp_store_tables has already validated the exact
                # supported empty orders-only pre-migration signature.
                return CtpAccountStoreFileInspection("LEGACY_CTP_JOURNAL", None)
            if found_legacy_tables != legacy_tables or tables != legacy_tables:
                raise DurableStoreError("CTP journal layout is partial or ambiguous")
            return CtpAccountStoreFileInspection("LEGACY_CTP_JOURNAL", None)
        identity = cls._preflight_existing_ctp_account_store(target, scope)
        return CtpAccountStoreFileInspection("CTP_EXECUTION_STORE", identity)

    @classmethod
    def _preflight_existing_ctp_account_store(
        cls, path: Path, scope: ExecutionScope | None
    ) -> CtpAccountStoreIdentity:
        if not path.is_file():
            raise DurableStoreError("existing CTP execution store is not a regular file")
        before = cls._ctp_store_sidecar_state(path)
        _, journal_signature, wal_signature, shm_signature = before
        if journal_signature is not None or (wal_signature is None) != (shm_signature is None):
            raise DurableStoreError("CTP execution store has uncertain SQLite journal state")

        uri = f"{path.as_uri()}?mode=ro"
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=1.0)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            database_row = connection.execute("PRAGMA database_list").fetchone()
            if database_row is None or database_row[1] != "main" or not database_row[2]:
                raise DurableStoreError("CTP execution store database identity is unavailable")
            opened_path = Path(str(database_row[2])).resolve(strict=True)
            if os.path.normcase(str(opened_path)) != os.path.normcase(str(path)):
                raise DurableStoreError("CTP execution store opened an unexpected database file")

            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            legacy_tables = {
                "ctp_sim_orders",
                "ctp_sim_journal_metadata",
                "ctp_sim_journal_scopes",
            }
            if tables & legacy_tables:
                raise DurableStoreError("legacy CTP execution journal cannot be opened as a Store")
            required_tables = {
                "execution_meta",
                "execution_records",
                "execution_writer_leases",
                "ctp_dispatch_commands",
                "ctp_account_family_owners",
            }
            if not required_tables.issubset(tables):
                raise DurableStoreError("existing CTP execution store schema is unknown")

            metadata = {
                str(row["key"]): str(row["value"])
                for row in connection.execute(
                    "SELECT key, value FROM execution_meta"
                ).fetchall()
            }
            if metadata.get("schema_version") != str(cls._SCHEMA_VERSION):
                raise DurableStoreError("existing CTP execution store schema is not current")
            non_internal_tables = tables - {"sqlite_sequence"}
            if non_internal_tables != _CTP_ACCOUNT_STORE_TABLES:
                raise DurableStoreError("existing CTP execution store schema is incomplete or unknown")
            if cls._ctp_store_schema_objects(connection) != cls._code_owned_ctp_store_schema_objects():
                raise DurableStoreError(
                    "existing CTP execution store schema definitions do not match code"
                )
            if "ctp_account_store_identity" not in tables:
                raise DurableStoreError("CTP execution store identity is missing or ambiguous")
            journal_incarnation_id = metadata.get("journal_incarnation_id", "")
            if len(journal_incarnation_id) != 32 or any(
                character not in "0123456789abcdef" for character in journal_incarnation_id
            ):
                raise DurableStoreError("CTP execution store journal identity is invalid")

            triggers = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                ).fetchall()
            }
            if triggers != _CTP_ACCOUNT_STORE_TRIGGERS:
                raise DurableStoreError("CTP execution store trigger schema is incomplete or unknown")
            indexes = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index'"
                ).fetchall()
            }
            explicit_indexes = {
                name for name in indexes if not name.startswith("sqlite_autoindex_")
            }
            if explicit_indexes != _CTP_ACCOUNT_STORE_INDEXES:
                raise DurableStoreError("CTP execution store index schema is incomplete or unknown")
            identity_rows = connection.execute(
                "SELECT * FROM ctp_account_store_identity"
            ).fetchall()
            if len(identity_rows) != 1 or int(identity_rows[0]["singleton"]) != 1:
                raise DurableStoreError("CTP execution store identity is missing or ambiguous")
            identity_row = identity_rows[0]
            account_ref = str(identity_row["account_ref"])
            if not _is_prefixed_digest(account_ref, _CTP_ACCOUNT_REF_PREFIX):
                raise DurableStoreError("CTP execution store account identity is malformed")
            persisted_family_key = _CTP_ACCOUNT_FAMILY_PREFIX + hashlib.sha256(
                _CTP_ACCOUNT_FAMILY_DOMAIN + account_ref.encode("ascii")
            ).hexdigest()
            if (
                str(identity_row["ledger_kind"]) != "CTP_EXECUTION_V1"
                or str(identity_row["family_key"]) != persisted_family_key
                or (scope is not None and account_ref != scope.account_ref)
                or str(identity_row["journal_incarnation_id"]) != journal_incarnation_id
            ):
                raise DurableStoreError("CTP execution store account identity does not match")
            owner_row = connection.execute(
                "SELECT owner_state, owner_intent_id FROM ctp_account_family_owners "
                "WHERE family_key = ?",
                (persisted_family_key,),
            ).fetchone()
            connection.execute("COMMIT")
        except DurableStoreError:
            if connection is not None:
                with suppress(sqlite3.Error):
                    connection.execute("ROLLBACK")
            raise
        except (sqlite3.Error, OSError, ValueError) as error:
            if connection is not None:
                with suppress(sqlite3.Error):
                    connection.execute("ROLLBACK")
            raise DurableStoreError("read-only CTP execution store preflight failed") from error
        finally:
            if connection is not None:
                connection.close()

        after = cls._ctp_store_sidecar_state(path)
        if not cls._ctp_preflight_state_is_stable(before, after):
            raise DurableStoreError("CTP execution store changed during read-only preflight")
        stat_result = path.stat()
        return CtpAccountStoreIdentity(
            ledger_kind="CTP_EXECUTION_V1",
            account_ref=account_ref,
            family_key=persisted_family_key,
            journal_incarnation_id=journal_incarnation_id,
            family_owner_state=None if owner_row is None else str(owner_row["owner_state"]),
            family_owner_intent_id=(
                None if owner_row is None else str(owner_row["owner_intent_id"])
            ),
            database_filename=str(opened_path),
            file_device=int(stat_result.st_dev),
            file_id=int(stat_result.st_ino),
        )

    @classmethod
    def open_ctp_account_store(
        cls,
        path: str | Path,
        scope: ExecutionScope,
        *,
        busy_timeout_ms: int = 5_000,
    ) -> SqliteExecutionStore:
        """Open the one factory-bound CTP account ledger at a caller-owned path.

        The main composition owns the path and flow lock. This factory rejects
        existing legacy/unknown/unbound journals before the regular constructor
        can set WAL mode or run schema DDL. A missing path is exclusively
        created here, then bound to the exact canonical account reference.
        """

        if type(busy_timeout_ms) is not int or busy_timeout_ms <= 0:
            raise ContractValidationError("invalid busy_timeout_ms")
        family_key = cls._ctp_account_family_key(scope)
        target = cls._ctp_account_store_path(path, scope)
        sidecars = tuple(Path(f"{target}{suffix}") for suffix in ("-journal", "-wal", "-shm"))
        if not os.path.lexists(str(target)):
            if any(os.path.lexists(str(sidecar)) for sidecar in sidecars):
                raise DurableStoreError("CTP execution store sidecars exist without a database")
            try:
                descriptor = os.open(str(target), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                created_by_factory = False
            except OSError as error:
                raise DurableStoreError("unable to create CTP execution store") from error
            else:
                os.close(descriptor)
                created_by_factory = True
        else:
            created_by_factory = False

        if created_by_factory:
            store: SqliteExecutionStore | None = None
            try:
                if any(os.path.lexists(str(sidecar)) for sidecar in sidecars):
                    raise DurableStoreError("CTP execution store sidecars appeared during creation")
                store = cls(target, busy_timeout_ms=busy_timeout_ms)
                store._bind_new_ctp_account_store_identity(scope, family_key)
                store.read_ctp_account_store_identity(scope)
                return store
            except BaseException:
                if store is not None:
                    store.close()
                raise

        preflight_identity = cls._preflight_existing_ctp_account_store(
            target, scope
        )
        store = cls(target, busy_timeout_ms=busy_timeout_ms)
        try:
            identity = store.read_ctp_account_store_identity(scope)
            if (
                identity.journal_incarnation_id != preflight_identity.journal_incarnation_id
                or identity.family_key != preflight_identity.family_key
                or identity.account_ref != preflight_identity.account_ref
                or identity.database_filename != preflight_identity.database_filename
                or identity.file_device != preflight_identity.file_device
                or identity.file_id != preflight_identity.file_id
            ):
                raise DurableStoreError("CTP execution store changed after preflight")
            return store
        except BaseException:
            store.close()
            raise

    def _bind_new_ctp_account_store_identity(
        self, scope: ExecutionScope, family_key: str
    ) -> None:
        with self._transaction() as cursor:
            existing = cursor.execute(
                "SELECT 1 FROM ctp_account_store_identity LIMIT 1"
            ).fetchone()
            if existing is not None:
                raise DurableStoreError("new CTP execution store identity is already bound")
            journal_row = cursor.execute(
                "SELECT value FROM execution_meta WHERE key = 'journal_incarnation_id'"
            ).fetchone()
            if journal_row is None:
                raise DurableStoreError("new CTP execution store has no journal identity")
            cursor.execute(
                """
                INSERT INTO ctp_account_store_identity(
                    singleton, ledger_kind, account_ref, family_key,
                    journal_incarnation_id, created_at_ns
                ) VALUES (1, 'CTP_EXECUTION_V1', ?, ?, ?, ?)
                """,
                (scope.account_ref, family_key, str(journal_row["value"]), time.time_ns()),
            )

    def read_ctp_account_store_identity(
        self, scope: ExecutionScope
    ) -> CtpAccountStoreIdentity:
        """Read the exact CTP account identity bound to this opened database."""

        family_key = self._ctp_account_family_key(scope)
        try:
            with self._read_transaction() as cursor:
                identity_rows = cursor.execute(
                    "SELECT * FROM ctp_account_store_identity"
                ).fetchall()
                if len(identity_rows) != 1:
                    raise DurableStoreError("CTP execution store identity is missing or ambiguous")
                identity_row = identity_rows[0]
                journal_row = cursor.execute(
                    "SELECT value FROM execution_meta WHERE key = 'journal_incarnation_id'"
                ).fetchone()
                if journal_row is None:
                    raise DurableStoreError("CTP execution store journal identity is missing")
                journal_incarnation_id = str(journal_row["value"])
                if (
                    str(identity_row["ledger_kind"]) != "CTP_EXECUTION_V1"
                    or str(identity_row["account_ref"]) != scope.account_ref
                    or str(identity_row["family_key"]) != family_key
                    or str(identity_row["journal_incarnation_id"]) != journal_incarnation_id
                ):
                    raise DurableStoreError("CTP execution store account identity does not match")
                owner_row = cursor.execute(
                    "SELECT owner_state, owner_intent_id FROM ctp_account_family_owners "
                    "WHERE family_key = ?",
                    (family_key,),
                ).fetchone()
                database_row = self._connection.execute("PRAGMA database_list").fetchone()
        except sqlite3.Error as error:
            raise DurableStoreError("unable to read CTP execution store identity") from error
        if database_row is None or database_row[1] != "main" or not database_row[2]:
            raise DurableStoreError("CTP execution store database filename is unavailable")
        try:
            opened_path = Path(str(database_row[2])).resolve(strict=True)
            stat_result = opened_path.stat()
        except OSError as error:
            raise DurableStoreError("unable to identify opened CTP execution store") from error
        return CtpAccountStoreIdentity(
            ledger_kind="CTP_EXECUTION_V1",
            account_ref=scope.account_ref,
            family_key=family_key,
            journal_incarnation_id=journal_incarnation_id,
            family_owner_state=None if owner_row is None else str(owner_row["owner_state"]),
            family_owner_intent_id=(
                None if owner_row is None else str(owner_row["owner_intent_id"])
            ),
            database_filename=str(opened_path),
            file_device=int(stat_result.st_dev),
            file_id=int(stat_result.st_ino),
        )

    @staticmethod
    def _ctp_account_family_key(scope: ExecutionScope) -> str:
        if (
            type(scope) is not ExecutionScope
            or scope.provider != "ctp"
            or not _is_prefixed_digest(scope.account_ref, _CTP_ACCOUNT_REF_PREFIX)
        ):
            raise ContractValidationError(
                "canonical lowercase CTP account scope is required for family ownership"
            )
        digest = hashlib.sha256(
            _CTP_ACCOUNT_FAMILY_DOMAIN + scope.account_ref.encode("ascii")
        ).hexdigest()
        return _CTP_ACCOUNT_FAMILY_PREFIX + digest

    @staticmethod
    def _assert_no_unmapped_ctp_account_family_history(cursor: sqlite3.Cursor) -> None:
        row = cursor.execute(
            "SELECT 1 FROM ctp_account_family_legacy_fences WHERE fence_id = 1"
        ).fetchone()
        if row is not None:
            raise InvalidStateTransition(
                "CTP account family is blocked by unmapped legacy execution history; "
                "a reviewed offline migration is required"
            )

    def acquire_ctp_account_family_owner(
        self, scope: ExecutionScope
    ) -> CtpAccountFamilyOwnerHandle:
        """Persist the one mode-independent owner for a canonical CTP account.

        This must precede the ordinary writer lease and native API creation.
        The family identity is derived inside the Store from ``scope.account_ref``;
        there is no caller-supplied fingerprint. Owners survive close/restart and
        this candidate deliberately exposes no release or handoff operation.
        """

        family_key = self._ctp_account_family_key(scope)
        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        owner_intent_id = uuid.uuid4().hex
        now_ns = time.time_ns()
        legacy_blocked = False
        existing_handle: CtpAccountFamilyOwnerHandle | None = None
        with self._lock, self._transaction() as cursor:
            existing = cursor.execute(
                "SELECT * FROM ctp_account_family_owners WHERE family_key = ?",
                (family_key,),
            ).fetchone()
            if existing is not None:
                candidate = self._issued_ctp_account_family_owners.get(
                    str(existing["owner_intent_id"])
                )
                if (
                    candidate is not None
                    and str(existing["owner_state"]) == "ACTIVE"
                    and str(existing["family_key"]) == family_key
                    and str(existing["account_key"]) == account_key
                    and str(existing["environment"]) == scope.environment
                ):
                    existing_handle = candidate
                else:
                    raise InvalidStateTransition(
                        "canonical CTP account family already has a persistent owner"
                    )
            else:
                legacy = cursor.execute(
                    "SELECT 1 FROM ctp_account_family_legacy_fences WHERE fence_id = 1"
                ).fetchone()
                if legacy is not None:
                    legacy_blocked = True
                else:
                    cursor.execute(
                        """
                        INSERT INTO ctp_account_family_owners(
                            family_key, owner_intent_id, account_key, scope_key,
                            environment, trading_day, owner_state, poison_code,
                            created_at_ns, updated_at_ns
                        ) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', NULL, ?, ?)
                        """,
                        (
                            family_key,
                            owner_intent_id,
                            account_key,
                            scope_key,
                            scope.environment,
                            trading_day,
                            now_ns,
                            now_ns,
                        ),
                    )
        if legacy_blocked:
            raise InvalidStateTransition(
                "CTP account family is blocked by unmapped legacy execution history; "
                "a reviewed offline migration is required"
            )
        if existing_handle is not None:
            return existing_handle
        handle = CtpAccountFamilyOwnerHandle(
            family_key=family_key,
            owner_intent_id=owner_intent_id,
            account_key=account_key,
            scope_key=scope_key,
        )
        with self._lock:
            self._issued_ctp_account_family_owners[owner_intent_id] = handle
        return handle

    def _require_ctp_account_family_owner(
        self,
        cursor: sqlite3.Cursor,
        scope: ExecutionScope,
        owner_handle: CtpAccountFamilyOwnerHandle,
    ) -> sqlite3.Row:
        family_key = self._ctp_account_family_key(scope)
        account_key, _, _ = self._validate_ctp_order_identity_scope(scope)
        if (
            type(owner_handle) is not CtpAccountFamilyOwnerHandle
            or self._issued_ctp_account_family_owners.get(owner_handle.owner_intent_id)
            is not owner_handle
            or owner_handle.family_key != family_key
            or owner_handle.account_key != account_key
        ):
            raise ContractValidationError(
                "exact same-Store CTP account family owner handle is required"
            )
        self._assert_no_unmapped_ctp_account_family_history(cursor)
        row = cursor.execute(
            """
            SELECT * FROM ctp_account_family_owners
            WHERE family_key = ? AND owner_intent_id = ?
              AND account_key = ?
            """,
            (family_key, owner_handle.owner_intent_id, account_key),
        ).fetchone()
        if (
            row is None
            or str(row["owner_state"]) != "ACTIVE"
            or str(row["environment"]) != scope.environment
        ):
            raise InvalidStateTransition("durable CTP account family owner is not active")
        return row

    @staticmethod
    def _poison_ctp_account_family_owner(
        cursor: sqlite3.Cursor,
        *,
        account_key: str,
        reason_code: str,
        now_ns: int,
    ) -> None:
        """Make callback-source uncertainty block every later CTP mutation."""

        cursor.execute(
            """
            UPDATE ctp_account_family_owners
            SET owner_state = 'POISONED', poison_code = ?, updated_at_ns = ?
            WHERE account_key = ? AND owner_state = 'ACTIVE'
            """,
            (reason_code, now_ns, account_key),
        )

    def create_ctp_callback_session_owner(
        self,
        scope: ExecutionScope,
        *,
        writer_lease: WriterLease,
    ) -> CtpCallbackSessionOwnerHandle:
        """Persist a one-shot account owner before native API construction.

        This creates no native identity and grants no send authority. A
        persisted owner is never recreated after Store/process restart. Prior
        dispatched commands, callback ledgers, and lifecycle fences require a
        separate recovery design and reject this candidate owner.
        """

        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        owner_intent_id = uuid.uuid4().hex
        now_ns = time.time_ns()
        with self._lock:
            with self._transaction() as cursor:
                self._assert_active_writer_lease(cursor, scope, writer_lease, now_ns=now_ns)
                fenced = cursor.execute(
                    "SELECT 1 FROM ctp_dispatch_callback_source_lifecycle_fences "
                    "WHERE account_key = ? LIMIT 1",
                    (account_key,),
                ).fetchone()
                if fenced is not None:
                    raise InvalidStateTransition(
                        "account has a permanent CTP callback source lifecycle fence"
                    )
                previous_owner = cursor.execute(
                    "SELECT 1 FROM ctp_dispatch_callback_session_owners "
                    "WHERE account_key = ? LIMIT 1",
                    (account_key,),
                ).fetchone()
                if previous_owner is not None:
                    raise InvalidStateTransition(
                        "account already has a durable CTP callback session owner"
                    )
                previous_dispatch = cursor.execute(
                    """
                    SELECT command_id FROM ctp_dispatch_commands
                    WHERE account_key = ? AND status != 'READY'
                    ORDER BY created_at_ns, command_id LIMIT 1
                    """,
                    (account_key,),
                ).fetchone()
                previous_callback = cursor.execute(
                    "SELECT 1 FROM ctp_dispatch_callback_ledger "
                    "WHERE account_key = ? LIMIT 1",
                    (account_key,),
                ).fetchone()
                if previous_dispatch is not None or previous_callback is not None:
                    raise InvalidStateTransition(
                        "prior CTP dispatch history prevents a new callback session owner"
                    )
                cursor.execute(
                    """
                    INSERT INTO ctp_dispatch_callback_session_owners(
                        owner_intent_id, account_key, scope_key, owner_state,
                        writer_owner_id, writer_fencing_token,
                        last_source_sequence, created_at_ns, updated_at_ns
                    ) VALUES (?, ?, ?, 'PREPARED', ?, ?, 0, ?, ?)
                    """,
                    (
                        owner_intent_id,
                        account_key,
                        scope_key,
                        writer_lease.owner_id,
                        writer_lease.fencing_token,
                        now_ns,
                        now_ns,
                    ),
                )
            handle = CtpCallbackSessionOwnerHandle(
                owner_intent_id=owner_intent_id,
                account_key=account_key,
                scope_key=scope_key,
            )
            self._issued_ctp_callback_session_owners[owner_intent_id] = handle
        return handle

    def _register_ctp_callback_session_adapter(
        self,
        owner_handle: CtpCallbackSessionOwnerHandle,
        adapter: object,
        record_type: type,
    ) -> None:
        """Pin the one in-process adapter and exact SDK wire class per owner.

        This is a trusted-process composition contract, not an isolation
        boundary against arbitrary Python code in the same process.
        """

        if (
            type(owner_handle) is not CtpCallbackSessionOwnerHandle
            or type(record_type) is not type
            or adapter is None
            or self._issued_ctp_callback_session_owners.get(owner_handle.owner_intent_id)
            is not owner_handle
        ):
            raise ContractValidationError("exact Store-owned callback adapter inputs are required")
        with self._lock:
            prior = self._issued_ctp_callback_session_adapters.get(owner_handle.owner_intent_id)
            if prior is not None:
                raise InvalidStateTransition("CTP callback owner already has its single adapter")
            self._issued_ctp_callback_session_adapters[owner_handle.owner_intent_id] = adapter
            self._ctp_callback_ingress_record_types[owner_handle.owner_intent_id] = record_type

    @staticmethod
    def _ctp_callback_payload_from_record(
        record: Any, *, expected_record_type: type
    ) -> tuple[dict[str, Any], str, str]:
        """Validate the immutable callback snapshot without importing the SDK."""

        if type(expected_record_type) is not type or type(record) is not expected_record_type:
            raise ContractValidationError("typed CTP callback ingress record is required")
        to_payload = getattr(record, "to_payload", None)
        if not callable(to_payload):
            raise ContractValidationError("typed CTP callback ingress record is malformed")
        try:
            payload = to_payload()
            if type(payload) is not dict:
                raise ValueError
            expected = {
                "schema",
                "owner_intent_id",
                "callback_name",
                "callback_class",
                "phase",
                "source_instance_id",
                "native_client_epoch",
                "native_api_source_id",
                "native_spi_source_id",
                "native_api_generation",
                "source_connection_generation",
                "connection_generation",
                "sequence",
                "monotonic_ns",
                "named_args",
                "flattened_fields",
                "capture_complete",
                "missing_getters",
                "digest",
            }
            if set(payload) != expected or payload["schema"] != "ctp_trader_callback_ingress.v2":
                raise ValueError
            digest = payload["digest"]
            if not _is_sha256(digest):
                raise ValueError
            unsigned = dict(payload)
            del unsigned["digest"]
            digest_bytes = json.dumps(
                unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            if hashlib.sha256(digest_bytes).hexdigest() != digest:
                raise ValueError
            canonical_bytes = json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            if len(canonical_bytes) > 32 * 1024:
                raise ValueError
            if (
                payload["capture_complete"] is not True
                or payload["missing_getters"] != []
                or type(payload["sequence"]) is not int
                or payload["sequence"] <= 0
                or type(payload["monotonic_ns"]) is not int
                or payload["monotonic_ns"] <= 0
                or type(payload["native_api_generation"]) is not int
                or payload["native_api_generation"] <= 0
                or type(payload["source_connection_generation"]) is not int
                or payload["source_connection_generation"] < 0
                or type(payload["connection_generation"]) is not int
                or payload["connection_generation"] < 0
                or type(payload["named_args"]) is not list
                or type(payload["flattened_fields"]) is not list
            ):
                raise ValueError
            for key in (
                "owner_intent_id",
                "callback_name",
                "callback_class",
                "phase",
                "source_instance_id",
                "native_client_epoch",
                "native_api_source_id",
                "native_spi_source_id",
            ):
                if type(payload[key]) is not str or not payload[key] or len(payload[key]) > 128:
                    raise ValueError
            if payload["callback_class"] not in {
                "PRE_LOGIN",
                "ROUTEABLE",
                "AUDIT_QUERY",
                "AUDIT_INFORMATIONAL",
                "LIFECYCLE_POISON",
                "UNSUPPORTED_FINANCIAL",
            }:
                raise ValueError
            if payload["phase"] not in {"PRE_LOGIN", "ACTIVE", "POISONED"}:
                raise ValueError
            # The SDK snapshot format is scalar-only. Recanonicalization also
            # rejects arbitrary objects, bytes, non-finite floats and NaN.
            encoded = json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            self_digest = str(digest)
            return payload, encoded, self_digest
        except (AttributeError, TypeError, ValueError, KeyError, UnicodeError):
            raise ContractValidationError("CTP callback ingress record is invalid") from None

    @staticmethod
    def _ctp_callback_field(
        payload: Mapping[str, Any], argument_slot: int, field_name: str
    ) -> Any:
        for item in payload["flattened_fields"]:
            if (
                type(item) is dict
                and item.get("argument_slot") == argument_slot
                and item.get("field_name") == field_name
            ):
                if item.get("present") is not True or item.get("scalar_captured") is not True:
                    return None
                return item.get("value")
        return None

    @staticmethod
    def _ctp_callback_named_arg(payload: Mapping[str, Any], slot: int) -> Any:
        if slot < 0 or slot >= len(payload["named_args"]):
            return None
        item = payload["named_args"][slot]
        if type(item) is not dict or item.get("present") is not True:
            return None
        if item.get("scalar_captured") is not True:
            return None
        return item.get("value")

    def append_ctp_callback_ingress(
        self,
        owner_handle: CtpCallbackSessionOwnerHandle,
        record: Any,
        *,
        _adapter: object | None = None,
    ) -> CtpCallbackIngressCommit:
        """Synchronously append one source-owned SDK callback snapshot.

        The callback source invokes this before it publishes the event to any
        queue or legacy callback. A gap, replay, changed source tuple, malformed
        snapshot, or persistence ambiguity poisons the durable owner.
        """

        payload: dict[str, Any] | None = None
        try:
            expected_adapter = self._issued_ctp_callback_session_adapters.get(
                owner_handle.owner_intent_id
            )
            expected_record_type = self._ctp_callback_ingress_record_types.get(
                owner_handle.owner_intent_id
            )
            if expected_adapter is None or (
                _adapter is not None and _adapter is not expected_adapter
            ):
                raise ContractValidationError("fixed CTP callback session adapter is required")
            if expected_record_type is None:
                raise ContractValidationError("fixed CTP callback record type is unavailable")
            payload, payload_json, digest = self._ctp_callback_payload_from_record(
                record, expected_record_type=expected_record_type
            )
            if payload["owner_intent_id"] != owner_handle.owner_intent_id:
                raise ContractValidationError("CTP callback owner does not match sink")
            source_tags = {
                "source_instance_id": payload["source_instance_id"],
                "native_client_epoch": payload["native_client_epoch"],
                "native_api_source_id": payload["native_api_source_id"],
                "native_spi_source_id": payload["native_spi_source_id"],
                "native_api_generation": payload["native_api_generation"],
                "connection_generation": payload["source_connection_generation"],
            }
            event_generation = payload["connection_generation"]
            if any(
                type(value) is not str or not value or len(value) > 128
                for key, value in source_tags.items()
                if key.endswith("_id") or key in {"source_instance_id", "native_client_epoch"}
            ):
                raise ContractValidationError("CTP callback source identity is invalid")
            if type(source_tags["native_api_generation"]) is not int or source_tags[
                "native_api_generation"
            ] <= 0:
                raise ContractValidationError("CTP callback source generation is invalid")
            if type(source_tags["connection_generation"]) is not int:
                raise ContractValidationError("CTP callback source generation is invalid")
            if type(event_generation) is not int or event_generation < 0:
                raise ContractValidationError("CTP callback event generation is invalid")

            now_ns = time.time_ns()
            with self._lock, self._transaction() as cursor:
                    owner = cursor.execute(
                        """
                        SELECT * FROM ctp_dispatch_callback_session_owners
                        WHERE owner_intent_id = ? AND account_key = ? AND scope_key = ?
                        """,
                        (
                            owner_handle.owner_intent_id,
                            owner_handle.account_key,
                            owner_handle.scope_key,
                        ),
                    ).fetchone()
                    if (
                        type(owner_handle) is not CtpCallbackSessionOwnerHandle
                        or self._issued_ctp_callback_session_owners.get(
                            owner_handle.owner_intent_id
                        )
                        is not owner_handle
                        or owner is None
                        or str(owner["owner_state"]) not in {"PREPARED", "ACTIVE"}
                    ):
                        raise ContractValidationError("CTP callback owner is not live")
                    self._assert_no_unmapped_ctp_account_family_history(cursor)
                    family_rows = cursor.execute(
                        """
                        SELECT owner_state FROM ctp_account_family_owners
                        WHERE account_key = ?
                        """,
                        (owner_handle.account_key,),
                    ).fetchall()
                    if len(family_rows) != 1 or str(family_rows[0]["owner_state"]) != "ACTIVE":
                        raise ContractValidationError("CTP callback account family is not active")
                    seq = payload["sequence"]
                    expected_seq = int(owner["last_source_sequence"]) + 1
                    if seq != expected_seq:
                        raise ContractValidationError("CTP callback source sequence gap or replay")
                    owner_tags = (
                        owner["source_instance_id"],
                        owner["native_client_epoch"],
                        owner["native_api_source_id"],
                        owner["native_spi_source_id"],
                        owner["native_api_generation"],
                        owner["connection_generation"],
                    )
                    incoming_tags = (
                        source_tags["source_instance_id"],
                        source_tags["native_client_epoch"],
                        source_tags["native_api_source_id"],
                        source_tags["native_spi_source_id"],
                        source_tags["native_api_generation"],
                        source_tags["connection_generation"],
                    )
                    if all(value is None for value in owner_tags):
                        if str(owner["owner_state"]) != "PREPARED" or payload["phase"] != "PRE_LOGIN":
                            raise ContractValidationError("CTP callback source did not start pre-login")
                        if seq != 1 or payload["callback_name"] != "OnFrontConnected":
                            raise ContractValidationError("CTP callback source lacks its first front event")
                    elif owner_tags != incoming_tags:
                        raise ContractValidationError("CTP callback source identity changed")
                    owner_state = str(owner["owner_state"])
                    if (
                        (owner_state == "PREPARED" and payload["phase"] != "PRE_LOGIN")
                        or (owner_state == "ACTIVE" and payload["phase"] != "ACTIVE")
                    ):
                        raise ContractValidationError("CTP callback phase differs from durable owner")

                    previous = cursor.execute(
                        """
                        SELECT callback_name FROM ctp_dispatch_callback_ingress
                        WHERE owner_intent_id = ? ORDER BY source_sequence DESC LIMIT 1
                        """,
                        (owner_handle.owner_intent_id,),
                    ).fetchone()
                    callback_name = payload["callback_name"]
                    class_name = payload["callback_class"]
                    if owner_state == "PREPARED":
                        allowed_next = {
                            None: "OnFrontConnected",
                            "OnFrontConnected": "OnRspAuthenticate",
                            "OnRspAuthenticate": "OnRspUserLogin",
                        }
                        if allowed_next.get(None if previous is None else str(previous["callback_name"])) != callback_name:
                            raise ContractValidationError("unexpected CTP pre-login callback order")
                        if class_name != "PRE_LOGIN":
                            raise ContractValidationError("pre-login callback has the wrong class")
                        if callback_name in {"OnRspAuthenticate", "OnRspUserLogin"}:
                            error_id = self._ctp_callback_field(payload, 1, "ErrorID")
                            if type(error_id) is not int or error_id != 0:
                                raise ContractValidationError("CTP pre-login response is not successful")
                    poison_code = None
                    if class_name == "LIFECYCLE_POISON":
                        poison_code = "lifecycle_transition"
                    elif class_name == "UNSUPPORTED_FINANCIAL":
                        poison_code = "unsupported_financial"
                    elif class_name == "PRE_LOGIN" and owner_state == "ACTIVE":
                        poison_code = "lifecycle_transition"
                    elif payload["phase"] == "POISONED":
                        poison_code = "source_gap"

                    # Queries that can reveal account activity are retained as
                    # audit facts but block future sends until an exact typed
                    # query evaluator exists. This flag is one-way in v1.
                    economic_query = callback_name in {
                        "OnRspQryOrder",
                        "OnRspQryTrade",
                        "OnRspQryInvestorPosition",
                        "OnRspQryTradingAccount",
                    }
                    cursor.execute(
                        """
                        INSERT INTO ctp_dispatch_callback_ingress(
                            owner_intent_id, source_sequence, record_digest_sha256,
                            callback_name, callback_class, source_phase,
                            source_instance_id, native_client_epoch,
                            native_api_source_id, native_spi_source_id,
                            native_api_generation, source_connection_generation,
                            connection_generation, record_payload_json, captured_at_ns
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            owner_handle.owner_intent_id,
                            seq,
                            digest,
                            callback_name,
                            class_name,
                            payload["phase"],
                            source_tags["source_instance_id"],
                            source_tags["native_client_epoch"],
                            source_tags["native_api_source_id"],
                            source_tags["native_spi_source_id"],
                            source_tags["native_api_generation"],
                            source_tags["connection_generation"],
                            event_generation,
                            payload_json,
                            now_ns,
                        ),
                    )
                    new_state = "POISONED" if poison_code else owner_state
                    cursor.execute(
                        """
                        UPDATE ctp_dispatch_callback_session_owners
                        SET source_instance_id = ?, native_client_epoch = ?,
                            native_api_source_id = ?, native_spi_source_id = ?,
                            native_api_generation = ?, connection_generation = ?,
                            last_source_sequence = ?,
                            economic_query_observed = CASE WHEN ? = 1 THEN 1
                                ELSE economic_query_observed END,
                            owner_state = ?, poison_code = CASE WHEN ? IS NULL
                                THEN poison_code ELSE ? END,
                            updated_at_ns = ?
                        WHERE owner_intent_id = ? AND owner_state IN ('PREPARED', 'ACTIVE')
                        """,
                        (
                            source_tags["source_instance_id"],
                            source_tags["native_client_epoch"],
                            source_tags["native_api_source_id"],
                            source_tags["native_spi_source_id"],
                            source_tags["native_api_generation"],
                            source_tags["connection_generation"],
                            seq,
                            int(economic_query),
                            new_state,
                            poison_code,
                            poison_code,
                            now_ns,
                            owner_handle.owner_intent_id,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise InvalidStateTransition("CTP callback owner changed during append")
                    if poison_code is not None:
                        self._poison_ctp_account_family_owner(
                            cursor,
                            account_key=owner_handle.account_key,
                            reason_code=poison_code,
                            now_ns=now_ns,
                        )
            return CtpCallbackIngressCommit(
                owner_intent_id=owner_handle.owner_intent_id,
                source_sequence=payload["sequence"],
                record_digest_sha256=digest,
                committed_high_watermark=payload["sequence"],
            )
        except Exception as error:
            with suppress(Exception):
                self.poison_ctp_callback_session_owner(
                    owner_handle,
                    "capture_incomplete" if payload is None else "source_gap",
                    last_sequence=(
                        payload.get("sequence") if payload is not None else None
                    ),
                )
            if isinstance(error, ContractValidationError):
                raise
            raise ContractValidationError("CTP callback inbox append failed") from None

    @staticmethod
    def _ctp_callback_applied_sequence(
        cursor: sqlite3.Cursor, owner_intent_id: str, last_source_sequence: int
    ) -> int:
        row = cursor.execute(
            """
            SELECT COUNT(*) AS application_count, COALESCE(MAX(source_sequence), 0) AS highwater
            FROM ctp_dispatch_callback_ingress_applications
            WHERE owner_intent_id = ?
            """,
            (owner_intent_id,),
        ).fetchone()
        count = int(row["application_count"])
        highwater = int(row["highwater"])
        if count != highwater or highwater > last_source_sequence:
            raise DurableStoreError("CTP callback inbox application sequence is inconsistent")
        return highwater

    def _require_current_ctp_callback_ingress_event(
        self,
        cursor: sqlite3.Cursor,
        scope: ExecutionScope,
        event: CtpCallbackIngressEventV1,
        writer_lease: WriterLease,
        *,
        expected_class: str = "ROUTEABLE",
    ) -> tuple[sqlite3.Row, sqlite3.Row, Mapping[str, Any], CtpCallbackSessionBindingV1]:
        if (
            type(event) is not CtpCallbackIngressEventV1
            or type(event.owner_handle) is not CtpCallbackSessionOwnerHandle
            or self._issued_ctp_callback_ingress_events.get(
                (event.owner_handle.owner_intent_id, event.source_sequence)
            )
            is not event
        ):
            raise ContractValidationError("Store-issued CTP callback ingress event is required")
        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        if (
            event.owner_handle.account_key != account_key
            or event.owner_handle.scope_key != scope_key
            or self._issued_ctp_callback_session_owners.get(event.owner_handle.owner_intent_id)
            is not event.owner_handle
        ):
            raise ContractValidationError("CTP callback event does not belong to the execution scope")
        binding = self._active_ctp_callback_sessions.get(event.owner_handle.owner_intent_id)
        if binding is None:
            raise InvalidStateTransition("active CTP callback session binding is unavailable")
        self._assert_active_writer_lease(cursor, scope, writer_lease)
        owner = cursor.execute(
            """
            SELECT * FROM ctp_dispatch_callback_session_owners
            WHERE owner_intent_id = ? AND account_key = ? AND scope_key = ?
            """,
            (event.owner_handle.owner_intent_id, account_key, scope_key),
        ).fetchone()
        if (
            owner is None
            or str(owner["owner_state"]) != "ACTIVE"
            or str(owner["trading_day"]) != trading_day
            or str(owner["trading_day"]) != binding.trading_day
            or int(owner["front_id"]) != binding.dispatch_front_id
            or int(owner["session_id"]) != binding.dispatch_session_id
            or int(owner["active_connection_generation"]) != binding.connection_generation
            or binding.owner_intent_id != event.owner_handle.owner_intent_id
        ):
            raise InvalidStateTransition("CTP callback owner changed before ledger commit")
        row = cursor.execute(
            """
            SELECT * FROM ctp_dispatch_callback_ingress
            WHERE owner_intent_id = ? AND source_sequence = ?
            """,
            (event.owner_handle.owner_intent_id, event.source_sequence),
        ).fetchone()
        if (
            row is None
            or str(row["record_digest_sha256"]) != event.record_digest_sha256
            or str(row["record_payload_json"]) != event.record_payload_json
            or str(row["callback_name"]) != event.callback_name
            or str(row["callback_class"]) != expected_class
            or str(row["source_phase"]) != "ACTIVE"
            or event.callback_class != expected_class
            or int(row["connection_generation"]) != binding.connection_generation
            or (
                str(row["source_instance_id"]),
                str(row["native_client_epoch"]),
                str(row["native_api_source_id"]),
                str(row["native_spi_source_id"]),
                int(row["native_api_generation"]),
                int(row["source_connection_generation"]),
            )
            != (
                binding.source_instance_id,
                binding.native_client_epoch,
                binding.native_api_source_id,
                binding.native_spi_source_id,
                binding.native_api_generation,
                binding.source_connection_generation,
            )
        ):
            raise ContractValidationError("CTP callback ingress row differs from the source event")
        payload = json.loads(str(row["record_payload_json"]))
        if (
            payload.get("digest") != event.record_digest_sha256
            or payload.get("sequence") != event.source_sequence
            or payload.get("capture_complete") is not True
            or payload.get("phase") != "ACTIVE"
        ):
            raise ContractValidationError("CTP callback ingress payload failed readback")
        applied = self._ctp_callback_applied_sequence(
            cursor, event.owner_handle.owner_intent_id, int(owner["last_source_sequence"])
        )
        if event.source_sequence != applied + 1:
            raise InvalidStateTransition("CTP callback source event is not the next sequence")
        return owner, row, payload, binding

    @staticmethod
    def _insert_ctp_callback_ingress_application(
        cursor: sqlite3.Cursor,
        event: CtpCallbackIngressEventV1,
        command_id: str,
        outcome: str,
        evidence_payload: Mapping[str, Any],
        *,
        applied_at_ns: int,
    ) -> None:
        if outcome != "APPLIED":
            raise ContractValidationError("invalid routed CTP callback application outcome")
        digest = payload_sha256(
            {
                "application_type": "ctp_callback_ingress_routed.v1",
                "owner_intent_id": event.owner_handle.owner_intent_id,
                "source_sequence": event.source_sequence,
                "record_digest_sha256": event.record_digest_sha256,
                "command_id": command_id,
                "outcome": outcome,
                "evidence": dict(evidence_payload),
            }
        )
        cursor.execute(
            """
            INSERT INTO ctp_dispatch_callback_ingress_applications(
                owner_intent_id, source_sequence, outcome, command_id,
                application_digest_sha256, applied_at_ns
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.owner_handle.owner_intent_id,
                event.source_sequence,
                outcome,
                command_id,
                digest,
                applied_at_ns,
            ),
        )

    @staticmethod
    def _insert_ctp_order_cumulative_observation(
        cursor: sqlite3.Cursor,
        event: CtpCallbackIngressEventV1,
        command: CtpDispatchCommand,
        binding: CtpCallbackSessionBindingV1,
        native_volume_traded: object,
        callback_key_sha256: str,
        *,
        observed_at_ns: int,
    ) -> tuple[int, str] | None:
        if native_volume_traded is None:
            return None
        if type(native_volume_traded) is not int or native_volume_traded < 0:
            raise ContractValidationError("native CTP VolumeTraded is invalid")
        payload = dict(command.request_payload)
        order_quantity = payload.get("VolumeTotalOriginal")
        if type(order_quantity) is not int or order_quantity <= 0:
            raise ContractValidationError("durable CTP submit quantity is invalid")
        if native_volume_traded > order_quantity:
            raise ContractValidationError("native CTP cumulative volume exceeds submit quantity")
        prior_order = cursor.execute(
            """
            SELECT native_volume_traded FROM ctp_dispatch_order_cumulative_ledger
            WHERE owner_intent_id = ? AND command_id = ? AND source_sequence < ?
            ORDER BY source_sequence DESC LIMIT 1
            """,
            (event.owner_handle.owner_intent_id, command.command_id, event.source_sequence),
        ).fetchone()
        if prior_order is not None and native_volume_traded < int(
            prior_order["native_volume_traded"]
        ):
            raise InvalidStateTransition("native CTP order cumulative volume moved backwards")
        prior = cursor.execute(
            """
            SELECT COALESCE(SUM(trade_volume), 0) AS total
            FROM ctp_dispatch_trade_fact_ledger
            WHERE owner_intent_id = ? AND command_id = ? AND source_sequence < ?
            """,
            (event.owner_handle.owner_intent_id, command.command_id, event.source_sequence),
        ).fetchone()
        trade_volume = int(prior["total"])
        if native_volume_traded == trade_volume:
            consistency = "MATCHED"
        elif native_volume_traded > trade_volume:
            consistency = "ORDER_CUMULATIVE_AHEAD"
        else:
            consistency = "TRADE_CUMULATIVE_AHEAD"
        cursor.execute(
            """
            INSERT INTO ctp_dispatch_order_cumulative_ledger(
                owner_intent_id, source_sequence, account_key, scope_key, trading_day,
                command_id, runtime_order_id, order_ref, native_volume_traded,
                trade_volume_at_prefix, prefix_consistency,
                ingress_record_digest_sha256, callback_key_sha256, observed_at_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.owner_handle.owner_intent_id,
                event.source_sequence,
                command.account_key,
                command.scope_key,
                command.trading_day,
                command.command_id,
                command.correlation_key.runtime_order_id,
                command.order_ref,
                native_volume_traded,
                trade_volume,
                consistency,
                event.record_digest_sha256,
                callback_key_sha256,
                observed_at_ns,
            ),
        )
        return trade_volume, consistency

    def read_next_ctp_callback_ingress(
        self, owner_handle: CtpCallbackSessionOwnerHandle
    ) -> CtpCallbackIngressEventV1 | None:
        """Read the next exact durable source event not yet routed or audited.

        This read does not consume the event. A routing path must commit its
        ledger/projection and application marker together; an audit-only path
        must call :meth:`mark_ctp_callback_ingress_audit`.
        """

        if type(owner_handle) is not CtpCallbackSessionOwnerHandle:
            raise ContractValidationError("typed CTP callback owner is required")
        if self._issued_ctp_callback_session_owners.get(owner_handle.owner_intent_id) is not owner_handle:
            raise ContractValidationError("exact same-Store CTP callback owner is required")
        with self._lock:
            try:
                owner = self._connection.execute(
                    """
                    SELECT * FROM ctp_dispatch_callback_session_owners
                    WHERE owner_intent_id = ? AND account_key = ? AND scope_key = ?
                    """,
                    (
                        owner_handle.owner_intent_id,
                        owner_handle.account_key,
                        owner_handle.scope_key,
                    ),
                ).fetchone()
                if owner is None or str(owner["owner_state"]) != "ACTIVE":
                    raise InvalidStateTransition("CTP callback session owner is not active")
                last_source = int(owner["last_source_sequence"])
                applied = self._ctp_callback_applied_sequence(
                    self._connection.cursor(), owner_handle.owner_intent_id, last_source
                )
                if applied >= last_source:
                    return None
                sequence = applied + 1
                row = self._connection.execute(
                    """
                    SELECT * FROM ctp_dispatch_callback_ingress
                    WHERE owner_intent_id = ? AND source_sequence = ?
                    """,
                    (owner_handle.owner_intent_id, sequence),
                ).fetchone()
                if row is None:
                    raise DurableStoreError("CTP callback inbox has a missing source event")
                event = CtpCallbackIngressEventV1(
                    owner_handle=owner_handle,
                    source_sequence=sequence,
                    record_digest_sha256=str(row["record_digest_sha256"]),
                    callback_name=str(row["callback_name"]),
                    callback_class=str(row["callback_class"]),
                    source_phase=str(row["source_phase"]),
                    source_tags=(
                        str(row["source_instance_id"]),
                        str(row["native_client_epoch"]),
                        str(row["native_api_source_id"]),
                        str(row["native_spi_source_id"]),
                        int(row["native_api_generation"]),
                        int(row["source_connection_generation"]),
                    ),
                    connection_generation=int(row["connection_generation"]),
                    record_payload_json=str(row["record_payload_json"]),
                )
                issued_key = (owner_handle.owner_intent_id, sequence)
                previous = self._issued_ctp_callback_ingress_events.get(issued_key)
                if previous is not None:
                    if previous != event:
                        raise DurableStoreError("CTP callback inbox readback changed")
                    return previous
                self._issued_ctp_callback_ingress_events[issued_key] = event
                return event
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read CTP callback inbox") from error

    def read_ctp_callback_session_context_facts(
        self, owner_handle: CtpCallbackSessionOwnerHandle
    ) -> tuple[CtpCallbackSessionBindingV1, str, str]:
        """Return Store-readback login account scalars for a live in-process owner.

        Broker/User identity is read from the immutable successful login inbox
        row; it is never accepted from the routing caller.
        """

        if (
            type(owner_handle) is not CtpCallbackSessionOwnerHandle
            or self._issued_ctp_callback_session_owners.get(owner_handle.owner_intent_id)
            is not owner_handle
        ):
            raise ContractValidationError("exact same-Store CTP callback owner is required")
        with self._lock:
            binding = self._active_ctp_callback_sessions.get(owner_handle.owner_intent_id)
            owner = self._connection.execute(
                """
                SELECT * FROM ctp_dispatch_callback_session_owners
                WHERE owner_intent_id = ? AND account_key = ? AND scope_key = ?
                """,
                (owner_handle.owner_intent_id, owner_handle.account_key, owner_handle.scope_key),
            ).fetchone()
            if (
                binding is None
                or owner is None
                or str(owner["owner_state"]) != "ACTIVE"
                or binding.owner_intent_id != owner_handle.owner_intent_id
                or binding.account_key != owner_handle.account_key
                or binding.scope_key != owner_handle.scope_key
                or binding.trading_day != str(owner["trading_day"])
                or binding.dispatch_front_id != int(owner["front_id"])
                or binding.dispatch_session_id != int(owner["session_id"])
                or binding.connection_generation != int(owner["active_connection_generation"])
            ):
                raise InvalidStateTransition("active CTP callback session binding is unavailable")
            login = self._connection.execute(
                """
                SELECT * FROM ctp_dispatch_callback_ingress
                WHERE owner_intent_id = ? AND callback_name = 'OnRspUserLogin'
                ORDER BY source_sequence DESC LIMIT 1
                """,
                (owner_handle.owner_intent_id,),
            ).fetchone()
            if (
                login is None
                or int(login["source_sequence"]) != binding.source_high_watermark
                or str(login["callback_name"]) != "OnRspUserLogin"
                or str(login["callback_class"]) != "PRE_LOGIN"
            ):
                raise DurableStoreError("durable CTP login identity row is unavailable")
            payload = json.loads(str(login["record_payload_json"]))
            broker_id = self._ctp_callback_field(payload, 0, "BrokerID")
            user_id = self._ctp_callback_field(payload, 0, "UserID")
            if (
                type(broker_id) is not str
                or not broker_id
                or type(user_id) is not str
                or not user_id
                or self._ctp_callback_field(payload, 0, "TradingDay") != binding.trading_day
            ):
                raise DurableStoreError("durable CTP login identity is invalid")
            return binding, broker_id, user_id

    def find_ctp_dispatch_command_for_ingress(
        self, event: CtpCallbackIngressEventV1
    ) -> CtpDispatchCommand | None:
        """Correlate a routeable event from its native fields, never caller IDs.

        ``None`` means that the uniquely correlated command is still inside
        its native Req call; the event must stay in the inbox until its receipt
        transaction completes. Unmatched, ambiguous, or wrong-session events
        raise and must poison the source owner.
        """

        if (
            type(event) is not CtpCallbackIngressEventV1
            or self._issued_ctp_callback_ingress_events.get(
                (event.owner_handle.owner_intent_id, event.source_sequence)
            )
            is not event
        ):
            raise ContractValidationError("Store-issued CTP callback ingress event is required")
        payload = json.loads(event.record_payload_json)
        if (
            payload.get("digest") != event.record_digest_sha256
            or payload.get("sequence") != event.source_sequence
            or payload.get("callback_name") != event.callback_name
            or payload.get("callback_class") != "ROUTEABLE"
            or payload.get("phase") != "ACTIVE"
            or payload.get("capture_complete") is not True
        ):
            raise ContractValidationError("CTP routeable callback record is not exact")
        name = event.callback_name
        field = {
            item["field_name"]: item["value"]
            for item in payload["flattened_fields"]
            if item.get("argument_slot") == 0
            and item.get("present") is True
            and item.get("scalar_captured") is True
        }
        if name not in {
            "OnRtnOrder",
            "OnRtnTrade",
            "OnRspOrderInsert",
            "OnErrRtnOrderInsert",
            "OnRspOrderAction",
            "OnErrRtnOrderAction",
        }:
            raise ContractValidationError("CTP routeable callback has no code-owned router")
        with self._lock:
            owner = self._connection.execute(
                """
                SELECT * FROM ctp_dispatch_callback_session_owners
                WHERE owner_intent_id = ? AND account_key = ? AND scope_key = ?
                """,
                (
                    event.owner_handle.owner_intent_id,
                    event.owner_handle.account_key,
                    event.owner_handle.scope_key,
                ),
            ).fetchone()
            binding = self._active_ctp_callback_sessions.get(
                event.owner_handle.owner_intent_id
            )
            if (
                owner is None
                or str(owner["owner_state"]) != "ACTIVE"
                or binding is None
                or event.source_tags
                != (
                    str(owner["source_instance_id"]),
                    str(owner["native_client_epoch"]),
                    str(owner["native_api_source_id"]),
                    str(owner["native_spi_source_id"]),
                    int(owner["native_api_generation"]),
                    int(owner["connection_generation"]),
                )
                or event.connection_generation != int(owner["active_connection_generation"])
                or event.connection_generation != binding.connection_generation
                or binding.source_high_watermark >= event.source_sequence
            ):
                raise ContractValidationError("CTP routeable callback source differs from owner")

            applied = self._ctp_callback_applied_sequence(
                self._connection.cursor(),
                event.owner_handle.owner_intent_id,
                int(owner["last_source_sequence"]),
            )
            if event.source_sequence != applied + 1:
                raise InvalidStateTransition("CTP routeable callback is out of sequence")

            submit_names = {"OnRtnOrder", "OnRtnTrade", "OnRspOrderInsert", "OnErrRtnOrderInsert"}
            operation = "SUBMIT" if name in submit_names else "CANCEL"
            # A staged CANCEL deliberately has no `order_ref` column value;
            # its target OrderRef is persisted separately. Keep the remaining
            # RequestID, ActionRef, target tuple, and session checks below so
            # two cancel actions on one order cannot be confused.
            rows = self._connection.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND trading_day = ?
                  AND operation = ?
                  AND CASE WHEN operation = 'SUBMIT'
                      THEN order_ref ELSE cancel_target_order_ref END = ?
                ORDER BY created_at_ns, command_id
                """,
                (
                    event.owner_handle.account_key,
                    event.owner_handle.scope_key,
                    binding.trading_day,
                    operation,
                    field.get("OrderRef"),
                ),
            ).fetchall()
            matches = []
            for row in rows:
                correlation = self._ctp_dispatch_command_from_row(row).correlation_key
                if correlation is None:
                    continue
                if (
                    correlation.session_generation_id != binding.session_generation_id
                    or correlation.dispatch_front_id != binding.dispatch_front_id
                    or correlation.dispatch_session_id != binding.dispatch_session_id
                ):
                    continue
                if name == "OnRtnTrade":
                    matches.append(row)
                    continue
                if field.get("RequestID") != correlation.native_request_id:
                    continue
                if name in {"OnRspOrderAction", "OnErrRtnOrderAction"} and (
                    field.get("OrderActionRef") != correlation.native_action_ref
                    or field.get("ExchangeID") != correlation.cancel_target_exchange_id
                    or field.get("OrderSysID") != correlation.cancel_target_order_sys_id
                    or field.get("FrontID") != correlation.cancel_target_front_id
                    or field.get("SessionID") != correlation.cancel_target_session_id
                ):
                    continue
                matches.append(row)
            if len(matches) != 1:
                raise ContractValidationError("CTP callback command correlation is not unique")
            row = matches[0]
            if row["callback_owner_intent_id"] != event.owner_handle.owner_intent_id:
                raise ContractValidationError("CTP callback command is not bound to this owner")
            if str(row["status"]) == "CLAIMED" and int(row["native_call_inflight"]) == 1:
                return None
            if (
                str(row["status"]) not in {"COMPLETED", "UNKNOWN"}
                or int(row["native_call_inflight"]) != 0
                or row["local_queue_receipt_id"] is None
                or row["local_queue_receipt_queued"] != 1
            ):
                raise InvalidStateTransition("CTP callback arrived outside completed native send")
            return self._ctp_dispatch_command_from_row(row)

    def mark_ctp_callback_ingress_audit(
        self,
        scope: ExecutionScope,
        event: CtpCallbackIngressEventV1,
        *,
        writer_lease: WriterLease,
    ) -> None:
        """Atomically mark one informational/query event as audit-only.

        Routeable and unsupported/lifecycle events cannot be consumed through
        this path. Economic query callbacks remain a permanent send fence even
        after this audit marker is written.
        """

        if (
            type(event) is not CtpCallbackIngressEventV1
            or self._issued_ctp_callback_ingress_events.get(
                (event.owner_handle.owner_intent_id, event.source_sequence)
            )
            is not event
        ):
            raise ContractValidationError("Store-issued CTP callback ingress event is required")
        if event.callback_class not in {"AUDIT_QUERY", "AUDIT_INFORMATIONAL"}:
            raise ContractValidationError("CTP callback event is not audit-only")
        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        if (
            account_key != event.owner_handle.account_key
            or scope_key != event.owner_handle.scope_key
        ):
            raise ContractValidationError("CTP callback audit scope differs from owner")
        now_ns = time.time_ns()
        application_digest = payload_sha256(
            {
                "application_type": "ctp_callback_ingress_audit.v1",
                "owner_intent_id": event.owner_handle.owner_intent_id,
                "source_sequence": event.source_sequence,
                "record_digest_sha256": event.record_digest_sha256,
                "callback_class": event.callback_class,
            }
        )
        with self._transaction() as cursor:
            owner = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_callback_session_owners
                WHERE owner_intent_id = ? AND account_key = ? AND scope_key = ?
                """,
                (event.owner_handle.owner_intent_id, account_key, scope_key),
            ).fetchone()
            if owner is None or str(owner["owner_state"]) != "ACTIVE":
                raise InvalidStateTransition("CTP callback session owner is not active")
            self._assert_active_writer_lease(
                cursor,
                scope,
                writer_lease,
                now_ns=now_ns,
            )
            if (
                str(owner["writer_owner_id"]) != writer_lease.owner_id
                or int(owner["writer_fencing_token"]) != writer_lease.fencing_token
            ):
                raise WriterLeaseUnavailable()
            applied = self._ctp_callback_applied_sequence(
                cursor, event.owner_handle.owner_intent_id, int(owner["last_source_sequence"])
            )
            if event.source_sequence != applied + 1:
                raise InvalidStateTransition("CTP callback audit event is out of sequence")
            row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_callback_ingress
                WHERE owner_intent_id = ? AND source_sequence = ?
                """,
                (event.owner_handle.owner_intent_id, event.source_sequence),
            ).fetchone()
            if (
                row is None
                or str(row["record_digest_sha256"]) != event.record_digest_sha256
                or str(row["record_payload_json"]) != event.record_payload_json
                or str(row["callback_name"]) != event.callback_name
                or str(row["callback_class"]) != event.callback_class
            ):
                raise ContractValidationError("CTP callback audit row differs from source event")
            cursor.execute(
                """
                INSERT INTO ctp_dispatch_callback_ingress_applications(
                    owner_intent_id, source_sequence, outcome, command_id,
                    application_digest_sha256, applied_at_ns
                ) VALUES (?, ?, 'AUDIT_ONLY', NULL, ?, ?)
                """,
                (
                    event.owner_handle.owner_intent_id,
                    event.source_sequence,
                    application_digest,
                    now_ns,
                ),
            )

    @staticmethod
    def _ctp_callback_session_binding_payload(
        binding: CtpCallbackSessionBindingV1,
    ) -> dict[str, Any]:
        if type(binding) is not CtpCallbackSessionBindingV1:
            raise ContractValidationError("typed active CTP callback binding is required")
        return {
            "binding_type": "ctp_callback_session_binding.v1",
            "owner_intent_id": binding.owner_intent_id,
            "account_key": binding.account_key,
            "scope_key": binding.scope_key,
            "trading_day": binding.trading_day,
            "session_generation_id": binding.session_generation_id,
            "dispatch_front_id": binding.dispatch_front_id,
            "dispatch_session_id": binding.dispatch_session_id,
            "source_tags": {
                "source_instance_id": binding.source_instance_id,
                "native_client_epoch": binding.native_client_epoch,
                "native_api_source_id": binding.native_api_source_id,
                "native_spi_source_id": binding.native_spi_source_id,
                "native_api_generation": binding.native_api_generation,
                "connection_generation": binding.source_connection_generation,
            },
            "source_high_watermark": binding.source_high_watermark,
        }

    @classmethod
    def _require_command_session_binding(
        cls,
        row: Mapping[str, Any],
        binding: CtpCallbackSessionBindingV1,
    ) -> None:
        expected_payload = cls._ctp_callback_session_binding_payload(binding)
        if (
            str(row["session_binding_json"]) != canonical_json(expected_payload)
            or str(row["session_binding_sha256"]) != binding.session_binding_sha256
            or payload_sha256(expected_payload) != binding.session_binding_sha256
        ):
            raise ContractValidationError(
                "CTP command session binding differs from active Store session"
            )

    def bind_ctp_callback_session(
        self,
        scope: ExecutionScope,
        owner_handle: CtpCallbackSessionOwnerHandle,
        observation: Any,
        source_tags: Any,
        high_watermark: int,
        *,
        writer_lease: WriterLease,
    ) -> CtpCallbackSessionBindingV1:
        """Bind one SDK-accepted terminal login to this permanent owner."""

        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        if type(high_watermark) is not int or high_watermark <= 0:
            raise ContractValidationError("invalid CTP login source watermark")
        try:
            tag_values = {
                name: getattr(source_tags, name)
                for name in (
                    "source_instance_id",
                    "native_client_epoch",
                    "native_api_source_id",
                    "native_spi_source_id",
                    "native_api_generation",
                    "connection_generation",
                )
            }
            observation_values = {
                "broker_id": observation.broker_id,
                "user_id": observation.user_id,
                "trading_day": observation.trading_day,
                "connection_generation": observation.connection_generation,
                "request_id": observation.request_id,
            }
        except Exception:
            raise ContractValidationError("typed SDK login observation is required") from None
        if (
            any(type(tag_values[name]) is not str or not tag_values[name] for name in (
                "source_instance_id", "native_client_epoch", "native_api_source_id",
                "native_spi_source_id"
            ))
            or type(tag_values["native_api_generation"]) is not int
            or tag_values["native_api_generation"] <= 0
            or type(tag_values["connection_generation"]) is not int
            or tag_values["connection_generation"] < 0
            or type(observation_values["broker_id"]) is not str
            or not observation_values["broker_id"]
            or type(observation_values["user_id"]) is not str
            or not observation_values["user_id"]
            or observation_values["trading_day"] != trading_day
            or type(observation_values["connection_generation"]) is not int
            or observation_values["connection_generation"] <= 0
            or type(observation_values["request_id"]) is not int
            or observation_values["request_id"] <= 0
        ):
            raise ContractValidationError("SDK login identity does not match CTP scope")
        epoch = tag_values["native_client_epoch"]
        if len(epoch) != 32 or any(char not in "0123456789abcdef" for char in epoch):
            raise ContractValidationError("invalid native client epoch")
        now_ns = time.time_ns()
        with self._lock, self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease, now_ns=now_ns)
            owner = self._require_ctp_callback_session_owner_handle(
                cursor,
                scope,
                owner_handle,
                allowed_states=frozenset({"PREPARED"}),
                writer_lease=writer_lease,
                now_ns=now_ns,
            )
            if high_watermark != int(owner["last_source_sequence"]):
                raise ContractValidationError("SDK login watermark changed before binding")
            login = cursor.execute(
                """
                    SELECT * FROM ctp_dispatch_callback_ingress
                    WHERE owner_intent_id = ? AND source_sequence = ?
                    """,
                (owner_handle.owner_intent_id, high_watermark),
            ).fetchone()
            if (
                login is None
                or str(login["callback_name"]) != "OnRspUserLogin"
                or str(login["callback_class"]) != "PRE_LOGIN"
                or str(login["source_phase"]) != "PRE_LOGIN"
                or str(login["source_instance_id"]) != tag_values["source_instance_id"]
                or str(login["native_client_epoch"]) != epoch
                or str(login["native_api_source_id"]) != tag_values["native_api_source_id"]
                or str(login["native_spi_source_id"]) != tag_values["native_spi_source_id"]
                or int(login["native_api_generation"]) != tag_values["native_api_generation"]
                or int(login["source_connection_generation"])
                != tag_values["connection_generation"]
                or int(login["connection_generation"])
                != observation_values["connection_generation"]
            ):
                raise ContractValidationError("durable CTP login source does not match SDK")
            login_payload = json.loads(str(login["record_payload_json"]))
            if (
                self._ctp_callback_named_arg(login_payload, 2)
                != observation_values["request_id"]
                or self._ctp_callback_field(login_payload, 0, "BrokerID")
                != observation_values["broker_id"]
                or self._ctp_callback_field(login_payload, 0, "UserID")
                != observation_values["user_id"]
                or self._ctp_callback_field(login_payload, 0, "TradingDay")
                != observation_values["trading_day"]
                or type(self._ctp_callback_field(login_payload, 0, "FrontID")) is not int
                or type(self._ctp_callback_field(login_payload, 0, "SessionID")) is not int
            ):
                raise ContractValidationError("durable CTP login identity differs from SDK")
            front_id = self._ctp_callback_field(login_payload, 0, "FrontID")
            session_id = self._ctp_callback_field(login_payload, 0, "SessionID")
            if front_id <= 0 or session_id <= 0:
                raise ContractValidationError("invalid native CTP login front/session")
            session_generation_id = (
                f"ctp-native-v2:{epoch}:{tag_values['native_api_generation']}:"
                f"{observation_values['connection_generation']}:{front_id}:{session_id}"
            )
            session_digest = payload_sha256(
                {
                    "binding_type": "ctp_callback_session_binding.v1",
                    "owner_intent_id": owner_handle.owner_intent_id,
                    "account_key": account_key,
                    "scope_key": scope_key,
                    "trading_day": trading_day,
                    "session_generation_id": session_generation_id,
                    "dispatch_front_id": front_id,
                    "dispatch_session_id": session_id,
                    "source_tags": tag_values,
                    "source_high_watermark": high_watermark,
                }
            )
            cursor.execute(
                """
                    UPDATE ctp_dispatch_callback_session_owners
                    SET owner_state = 'ACTIVE', trading_day = ?, front_id = ?,
                        session_id = ?, active_connection_generation = ?,
                        updated_at_ns = ?
                    WHERE owner_intent_id = ? AND owner_state = 'PREPARED'
                    """,
                (
                    trading_day,
                    front_id,
                    session_id,
                    observation_values["connection_generation"],
                    now_ns,
                    owner_handle.owner_intent_id,
                ),
            )
            if cursor.rowcount != 1:
                raise InvalidStateTransition("CTP callback owner activation changed")
            prior_events = cursor.execute(
                """
                    SELECT source_sequence, record_digest_sha256, callback_class
                    FROM ctp_dispatch_callback_ingress
                    WHERE owner_intent_id = ? AND source_sequence <= ?
                    ORDER BY source_sequence
                    """,
                (owner_handle.owner_intent_id, high_watermark),
            ).fetchall()
            if len(prior_events) != high_watermark:
                raise DurableStoreError("CTP login inbox sequence is incomplete")
            if self._ctp_callback_applied_sequence(
                cursor, owner_handle.owner_intent_id, high_watermark
            ) != 0:
                raise DurableStoreError("CTP login inbox was unexpectedly consumed")
            for prior_event in prior_events:
                sequence = int(prior_event["source_sequence"])
                application_digest = payload_sha256(
                    {
                        "application_type": "ctp_callback_prelogin_bound.v1",
                        "owner_intent_id": owner_handle.owner_intent_id,
                        "source_sequence": sequence,
                        "record_digest_sha256": str(
                            prior_event["record_digest_sha256"]
                        ),
                        "callback_class": str(prior_event["callback_class"]),
                    }
                )
                cursor.execute(
                    """
                        INSERT INTO ctp_dispatch_callback_ingress_applications(
                            owner_intent_id, source_sequence, outcome, command_id,
                            application_digest_sha256, applied_at_ns
                        ) VALUES (?, ?, 'AUDIT_ONLY', NULL, ?, ?)
                        """,
                    (
                        owner_handle.owner_intent_id,
                        sequence,
                        application_digest,
                        now_ns,
                    ),
                )
        binding = CtpCallbackSessionBindingV1(
            owner_intent_id=owner_handle.owner_intent_id,
            account_key=account_key,
            scope_key=scope_key,
            trading_day=trading_day,
            session_generation_id=session_generation_id,
            dispatch_front_id=front_id,
            dispatch_session_id=session_id,
            source_instance_id=tag_values["source_instance_id"],
            native_client_epoch=epoch,
            native_api_source_id=tag_values["native_api_source_id"],
            native_spi_source_id=tag_values["native_spi_source_id"],
            native_api_generation=tag_values["native_api_generation"],
            source_connection_generation=tag_values["connection_generation"],
            connection_generation=observation_values["connection_generation"],
            source_high_watermark=high_watermark,
            session_binding_sha256=session_digest,
        )
        self._active_ctp_callback_sessions[owner_handle.owner_intent_id] = binding
        return binding

    @staticmethod
    def _ctp_callback_session_generation_id(owner_row: Mapping[str, Any]) -> str:
        epoch = owner_row["native_client_epoch"]
        api_generation = owner_row["native_api_generation"]
        connection_generation = owner_row["active_connection_generation"]
        front_id = owner_row["front_id"]
        session_id = owner_row["session_id"]
        if (
            type(epoch) is not str
            or len(epoch) != 32
            or any(char not in "0123456789abcdef" for char in epoch)
            or type(api_generation) is not int
            or api_generation <= 0
            or type(connection_generation) is not int
            or connection_generation <= 0
            or type(front_id) is not int
            or front_id <= 0
            or type(session_id) is not int
            or session_id <= 0
        ):
            raise ContractValidationError("active CTP callback session identity is invalid")
        return (
            f"ctp-native-v2:{epoch}:{api_generation}:{connection_generation}:"
            f"{front_id}:{session_id}"
        )

    def _require_ctp_callback_session_owner_handle(
        self,
        cursor: sqlite3.Cursor,
        scope: ExecutionScope,
        owner_handle: CtpCallbackSessionOwnerHandle,
        *,
        allowed_states: frozenset[str] | None = None,
        writer_lease: WriterLease | None = None,
        now_ns: int | None = None,
    ) -> sqlite3.Row:
        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        if (
            type(owner_handle) is not CtpCallbackSessionOwnerHandle
            or self._issued_ctp_callback_session_owners.get(owner_handle.owner_intent_id)
            is not owner_handle
            or owner_handle.account_key != account_key
            or owner_handle.scope_key != scope_key
        ):
            raise ContractValidationError(
                "exact same-Store CTP callback session owner handle is required"
            )
        row = cursor.execute(
            """
            SELECT * FROM ctp_dispatch_callback_session_owners
            WHERE owner_intent_id = ? AND account_key = ? AND scope_key = ?
            """,
            (owner_handle.owner_intent_id, account_key, scope_key),
        ).fetchone()
        if row is None:
            raise ContractValidationError("durable CTP callback session owner is missing")
        if allowed_states is not None and str(row["owner_state"]) not in allowed_states:
            raise InvalidStateTransition("CTP callback session owner is not active")
        if writer_lease is not None:
            self._assert_active_writer_lease(
                cursor, scope, writer_lease, now_ns=now_ns
            )
            if (
                str(row["writer_owner_id"]) != writer_lease.owner_id
                or int(row["writer_fencing_token"]) != writer_lease.fencing_token
            ):
                raise WriterLeaseUnavailable()
        return row

    def poison_ctp_callback_session_owner(
        self,
        owner_handle: CtpCallbackSessionOwnerHandle,
        reason_code: str,
        *,
        source_tags: Mapping[str, Any] | None = None,
        last_sequence: int | None = None,
    ) -> CtpCallbackSessionPoisonCommit:
        """Permanently poison a one-shot owner after source uncertainty.

        Poisoning is monotonic and fail-safe, so it does not require a currently
        live writer lease. Callback/source detail is never copied into the
        durable poison marker. The permanent owner row itself remains an
        account fence if this best-effort poison transaction cannot commit.
        """

        if type(owner_handle) is not CtpCallbackSessionOwnerHandle or (
            self._issued_ctp_callback_session_owners.get(owner_handle.owner_intent_id)
            is not owner_handle
        ):
            raise ContractValidationError(
                "exact same-Store CTP callback session owner handle is required"
            )
        if type(reason_code) is not str or reason_code not in _CTP_CALLBACK_OWNER_POISON_CODES:
            raise ContractValidationError("invalid CTP callback owner poison code")
        if last_sequence is not None and (type(last_sequence) is not int or last_sequence < 0):
            raise ContractValidationError("invalid CTP callback owner sequence")
        effective_reason = reason_code
        if source_tags is not None:
            expected = {
                "source_instance_id",
                "native_client_epoch",
                "native_api_source_id",
                "native_spi_source_id",
                "native_api_generation",
                "connection_generation",
            }
            if not isinstance(source_tags, Mapping) or set(source_tags) != expected or any(
                type(source_tags[name]) not in (str, int)
                or (type(source_tags[name]) is str and len(source_tags[name]) > 128)
                for name in expected
            ):
                effective_reason = "source_identity_mismatch"

        with self._transaction() as cursor:
            row = cursor.execute(
                "SELECT * FROM ctp_dispatch_callback_session_owners "
                "WHERE owner_intent_id = ? AND account_key = ? AND scope_key = ?",
                (
                    owner_handle.owner_intent_id,
                    owner_handle.account_key,
                    owner_handle.scope_key,
                ),
            ).fetchone()
            if row is None:
                raise ContractValidationError("durable CTP callback session owner is missing")
            if source_tags is not None and set(source_tags) == {
                "source_instance_id",
                "native_client_epoch",
                "native_api_source_id",
                "native_spi_source_id",
                "native_api_generation",
                "connection_generation",
            }:
                for incoming, column in (
                    ("source_instance_id", "source_instance_id"),
                    ("native_client_epoch", "native_client_epoch"),
                    ("native_api_source_id", "native_api_source_id"),
                    ("native_spi_source_id", "native_spi_source_id"),
                    ("native_api_generation", "native_api_generation"),
                    ("connection_generation", "connection_generation"),
                ):
                    stored = row[column]
                    if stored is not None and stored != source_tags[incoming]:
                        effective_reason = "source_identity_mismatch"
                        break
            current_state = str(row["owner_state"])
            if current_state != "POISONED":
                cursor.execute(
                    """
                    UPDATE ctp_dispatch_callback_session_owners
                    SET owner_state = 'POISONED', poison_code = ?,
                        poison_observed_sequence = MAX(last_source_sequence, COALESCE(?, 0)),
                        updated_at_ns = ?
                    WHERE owner_intent_id = ? AND owner_state IN ('PREPARED', 'ACTIVE')
                    """,
                    (
                        effective_reason,
                        last_sequence,
                        time.time_ns(),
                        owner_handle.owner_intent_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise InvalidStateTransition("CTP callback session owner changed")
                self._poison_ctp_account_family_owner(
                    cursor,
                    account_key=owner_handle.account_key,
                    reason_code=effective_reason,
                    now_ns=time.time_ns(),
                )
                row = cursor.execute(
                    "SELECT * FROM ctp_dispatch_callback_session_owners "
                    "WHERE owner_intent_id = ?",
                    (owner_handle.owner_intent_id,),
                ).fetchone()
                assert row is not None
            persisted_sequence = int(
                row["poison_observed_sequence"]
                if row["poison_observed_sequence"] is not None
                else row["last_source_sequence"]
            )
            persisted_code = str(row["poison_code"])
        return CtpCallbackSessionPoisonCommit(
            owner_intent_id=owner_handle.owner_intent_id,
            durable_state="POISONED",
            last_source_sequence=persisted_sequence,
            poison_code=persisted_code,
        )

    def _migrate_legacy_ctp_callback_source_lifecycle_fences(
        self, cursor: sqlite3.Cursor, *, previous_tables: set[str]
    ) -> None:
        """Fail closed while migrating callback facts from pre-v13 schemas."""

        fenced_accounts = {
            str(row["account_key"])
            for row in cursor.execute(
                "SELECT account_key FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchall()
        }

        legacy_guard_table = "ctp_dispatch_callback_ingestion_guards"
        if legacy_guard_table in previous_tables:
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

    def _migrate_legacy_ctp_action_ref_accounts(self, cursor: sqlite3.Cursor) -> None:
        """Validate historical dispatch identity and fence pre-V2 cancel history.

        V1 ActionRef values may have been sent to a native provider and cannot
        safely seed the independent integer allocator. Preserve their command
        bytes and audit rows; the account fence prevents a new V2 allocation.
        Every historical command must also retain a canonical account and
        scope key. An unidentifiable completed SUBMIT is enough to make the
        account boundary unknowable, so reject the whole migration rather than
        treating a malformed key as a separate account.
        """

        historical_rows = cursor.execute(
            "SELECT account_key, scope_key FROM ctp_dispatch_commands"
        ).fetchall()
        for row in historical_rows:
            if not _is_prefixed_digest(str(row["account_key"]), "account:"):
                raise DurableStoreError("historical CTP dispatch account identity is unreadable")
            if not _is_prefixed_digest(str(row["scope_key"]), "scope:"):
                raise DurableStoreError("historical CTP dispatch scope identity is unreadable")
        if cursor.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise DurableStoreError("historical CTP foreign key integrity is inconsistent")

        fenced_accounts = {
            str(row["account_key"])
            for row in cursor.execute(
                "SELECT account_key FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchall()
        }
        rows = cursor.execute(
            """
            SELECT account_key, scope_key, command_id, request_payload_sha256,
                   session_binding_sha256, correlation_version, updated_at_ns
            FROM ctp_dispatch_commands
            WHERE operation = 'CANCEL' AND correlation_version < 2
            ORDER BY account_key, created_at_ns, command_id
            """
        ).fetchall()
        for row in rows:
            account_key = str(row["account_key"])
            if not _is_prefixed_digest(account_key, "account:"):
                raise DurableStoreError("legacy CTP cancel account identity is unreadable")
            if account_key in fenced_accounts:
                continue
            if not _is_prefixed_digest(str(row["scope_key"]), "scope:"):
                raise DurableStoreError("legacy CTP cancel scope identity is unreadable")
            command_id = str(row["command_id"])
            if int(row["correlation_version"]) == 1:
                command_row = cursor.execute(
                    "SELECT * FROM ctp_dispatch_commands WHERE account_key = ? AND command_id = ?",
                    (account_key, command_id),
                ).fetchone()
                if command_row is None:
                    raise DurableStoreError("legacy CTP cancel command disappeared during migration")
                command = self._ctp_dispatch_command_from_row(command_row)
                if command.correlation_key is None:
                    raise DurableStoreError("legacy CTP cancel has no exact correlation")
                correlation_digest = payload_sha256(command.correlation_key.to_payload())
            else:
                # Correlation version zero has no typed source identity. Bind
                # the permanent fence to its immutable command digest instead
                # of inventing a native ActionRef from an old payload.
                correlation_digest = payload_sha256(
                    {
                        "migration": "ctp_legacy_cancel_fence.v1",
                        "account_key": account_key,
                        "command_id": command_id,
                        "request_payload_sha256": str(row["request_payload_sha256"]),
                    }
                )
            session_digest = str(row["session_binding_sha256"])
            if not _is_sha256(correlation_digest) or not _is_sha256(session_digest):
                raise DurableStoreError("legacy CTP cancel fence digest is invalid")
            cursor.execute(
                """
                INSERT INTO ctp_dispatch_callback_source_lifecycle_fences(
                    account_key, scope_key, command_id, source_lifecycle_fence_id,
                    correlation_key_sha256, session_binding_sha256, created_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_key,
                    str(row["scope_key"]),
                    command_id,
                    uuid.uuid4().hex,
                    correlation_digest,
                    session_digest,
                    int(row["updated_at_ns"]),
                ),
            )
            fenced_accounts.add(account_key)

    def _migrate_ctp_cancel_postconditions(self, cursor: sqlite3.Cursor) -> None:
        """Keep every possibly-sent historical cancel behind an open obligation.

        The old command receipt describes a local call result, not terminal
        order state. Migration therefore never resolves a cancel. A V2 cancel
        is bound only when the original submit, owner, session and independent
        native identities can all be read back exactly; otherwise an
        UNMAPPABLE account obligation remains permanently open.
        """

        rows = cursor.execute(
            """
            SELECT * FROM ctp_dispatch_commands
            WHERE operation = 'CANCEL'
              AND (status != 'READY' OR claimed_at_ns IS NOT NULL
                   OR native_call_inflight = 1)
            ORDER BY account_key, created_at_ns, command_id
            """
        ).fetchall()
        for row in rows:
            account_key = str(row["account_key"])
            cancel_command_id = str(row["command_id"])
            existing = cursor.execute(
                "SELECT 1 FROM ctp_dispatch_cancel_postconditions "
                "WHERE account_key = ? AND cancel_command_id = ?",
                (account_key, cancel_command_id),
            ).fetchone()
            if existing is not None:
                continue

            target_submit = None
            owner_intent_id = row["callback_owner_intent_id"]
            if (
                int(row["correlation_version"]) == 2
                and type(owner_intent_id) is str
                and row["native_action_ref_int"] is not None
                and row["native_request_id"] is not None
                and row["runtime_order_id"] is not None
                and row["cancel_target_order_ref"] is not None
            ):
                candidates = cursor.execute(
                    """
                    SELECT * FROM ctp_dispatch_commands
                    WHERE account_key = ? AND operation = 'SUBMIT'
                      AND runtime_order_id = ? AND order_ref = ?
                    ORDER BY created_at_ns, command_id
                    """,
                    (
                        account_key,
                        str(row["runtime_order_id"]),
                        str(row["cancel_target_order_ref"]),
                    ),
                ).fetchall()
                if len(candidates) == 1:
                    candidate = candidates[0]
                    same_session = (
                        str(candidate["trading_day"]) == str(row["trading_day"])
                        and str(candidate["session_generation_id"])
                        == str(row["session_generation_id"])
                        and candidate["dispatch_front_id"] == row["dispatch_front_id"]
                        and candidate["dispatch_session_id"] == row["dispatch_session_id"]
                        and candidate["callback_owner_intent_id"] == owner_intent_id
                        and candidate["session_binding_sha256"]
                        == row["session_binding_sha256"]
                        and str(candidate["status"]) in {"COMPLETED", "UNKNOWN"}
                    )
                    if same_session:
                        target_submit = candidate

            identity_state = "EXACT" if target_submit is not None else "UNMAPPABLE"
            if target_submit is None:
                correlation_digest = payload_sha256(
                    {
                        "migration": "ctp_cancel_postcondition.v1",
                        "account_key": account_key,
                        "cancel_command_id": cancel_command_id,
                        "request_payload_sha256": str(row["request_payload_sha256"]),
                    }
                )
                owner_intent_id = None
                fields = (None,) * 14
            else:
                correlation = self._ctp_dispatch_command_from_row(row).correlation_key
                assert correlation is not None
                correlation_digest = payload_sha256(correlation.to_payload())
                fields = (
                    str(target_submit["command_id"]),
                    str(target_submit["runtime_order_id"]),
                    str(row["cancel_target_order_ref"]),
                    str(row["cancel_target_exchange_id"]),
                    str(row["cancel_target_order_sys_id"]),
                    int(row["cancel_target_front_id"]),
                    int(row["cancel_target_session_id"]),
                    str(row["session_generation_id"]),
                    int(row["dispatch_front_id"]),
                    int(row["dispatch_session_id"]),
                    int(row["native_request_id"]),
                    int(target_submit["native_request_id"]),
                    int(row["native_action_ref_int"]),
                    str(owner_intent_id),
                )
            cursor.execute(
                """
                INSERT INTO ctp_dispatch_cancel_postconditions(
                    account_key, cancel_command_id, scope_key, trading_day,
                    target_submit_command_id, runtime_order_id, target_order_ref,
                    target_exchange_id, target_order_sys_id, target_front_id,
                    target_session_id, session_generation_id, dispatch_front_id,
                    dispatch_session_id, native_request_id, target_native_request_id,
                    native_action_ref_int,
                    owner_intent_id, identity_state, correlation_key_sha256,
                    session_binding_sha256, created_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_key,
                    cancel_command_id,
                    str(row["scope_key"]),
                    str(row["trading_day"]),
                    *fields,
                    identity_state,
                    correlation_digest,
                    str(row["session_binding_sha256"]),
                    int(row["updated_at_ns"]),
                ),
            )

    @staticmethod
    def _has_unmapped_execution_history(
        cursor: sqlite3.Cursor, existing_tables: set[str] | frozenset[str]
    ) -> bool:
        """Return whether prior journal rows lack a V19 account-family binding.

        Before V19, the durable identity retained only account/scope hashes.
        Even generic execution rows cannot be attributed to a canonical CTP
        account family without the original scope payload, so any such history
        must block a new CTP family owner.
        """

        history_tables = (
            "execution_records",
            "execution_outbox",
            "cancellation_records",
            "cancellation_outbox",
            "execution_writer_leases",
            "ctp_order_identity_reservations",
            "ctp_order_target_projections",
            "ctp_order_target_projection_consumptions",
            "ctp_order_ref_cutover_sessions",
            "ctp_order_ref_legacy_imports",
            "ctp_order_ref_watermarks",
            "ctp_order_ref_account_watermarks",
            "ctp_native_action_ref_counters",
            "ctp_native_action_ref_allocations",
            "ctp_dispatch_commands",
            "ctp_dispatch_callback_ledger",
            "ctp_dispatch_callback_source_lifecycle_fences",
            "ctp_dispatch_callback_session_owners",
            "ctp_dispatch_callback_ingress",
            "ctp_dispatch_callback_ingress_applications",
            "ctp_dispatch_trade_fact_ledger",
            "ctp_dispatch_order_cumulative_ledger",
            "ctp_dispatch_order_projection",
            "ctp_dispatch_cancel_projection",
            "ctp_dispatch_cancel_postconditions",
            "ctp_dispatch_cancel_terminal_observations",
            "ctp_dispatch_cancel_postcondition_resolutions",
        )
        for table in history_tables:
            if table not in existing_tables:
                continue
            # ``table`` comes only from the fixed source-code tuple above and
            # is first confirmed present in sqlite_master; identifiers cannot
            # be bound as SQL parameters. Keep the migration probe narrow.
            if cursor.execute(
                f"SELECT 1 FROM {table} LIMIT 1"  # noqa: S608
            ).fetchone() is not None:
                return True
        return False

    def _migrate_ctp_account_family_legacy_fence(
        self,
        cursor: sqlite3.Cursor,
        *,
        previous_tables: set[str],
        previous_version: str,
    ) -> None:
        """Fence un-mappable pre-V19 journal history without rewriting it."""

        if previous_version == str(self._SCHEMA_VERSION):
            return
        if not self._has_unmapped_execution_history(cursor, previous_tables):
            return
        cursor.execute(
            """
            INSERT OR IGNORE INTO ctp_account_family_legacy_fences(
                fence_id, reason_code, source_schema_version, created_at_ns
            ) VALUES (1, 'unmapped_legacy_execution_history', ?, ?)
            """,
            (previous_version, time.time_ns()),
        )

    def _create_schema(self) -> None:
        with self._transaction() as cursor:
            # Capture lineage facts before CREATE TABLE IF NOT EXISTS adds the
            # current v13 tables. In particular, schema number 11 was used by
            # two isolated candidates: one had order-target projections, the
            # other had per-event callback guards. Never infer the old shape
            # from tables created by this migration.
            previous_tables = {
                str(row["name"])
                for row in cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            if "ctp_dispatch_commands" in previous_tables:
                existing_command_columns = {
                    str(item["name"])
                    for item in cursor.execute(
                        "PRAGMA table_info(ctp_dispatch_commands)"
                    ).fetchall()
                }
                for name, declaration in (
                    ("callback_owner_intent_id", "TEXT"),
                    (
                        "native_call_inflight",
                        "INTEGER NOT NULL DEFAULT 0 CHECK(native_call_inflight IN (0, 1))",
                    ),
                ):
                    if name not in existing_command_columns:
                        cursor.execute(
                            f"ALTER TABLE ctp_dispatch_commands ADD COLUMN {name} {declaration}"
                        )
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
                    journal_incarnation_id TEXT,
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
                CREATE TABLE IF NOT EXISTS ctp_account_family_owners (
                    family_key TEXT PRIMARY KEY
                        CHECK(length(family_key) = 86
                              AND family_key GLOB 'ctp-account-family.v1:*'
                              AND substr(family_key, 23) NOT GLOB '*[^0-9a-f]*'),
                    owner_intent_id TEXT NOT NULL UNIQUE
                        CHECK(length(owner_intent_id) = 32
                              AND owner_intent_id NOT GLOB '*[^0-9a-f]*'),
                    account_key TEXT NOT NULL
                        CHECK(length(account_key) = 72
                              AND account_key GLOB 'account:*'
                              AND substr(account_key, 9) NOT GLOB '*[^0-9a-f]*'),
                    scope_key TEXT NOT NULL
                        CHECK(length(scope_key) = 70
                              AND scope_key GLOB 'scope:*'
                              AND substr(scope_key, 7) NOT GLOB '*[^0-9a-f]*'),
                    environment TEXT NOT NULL,
                    trading_day TEXT NOT NULL
                        CHECK(length(trading_day) = 8
                              AND trading_day NOT GLOB '*[^0-9]*'),
                    owner_state TEXT NOT NULL CHECK(owner_state IN ('ACTIVE', 'POISONED')),
                    poison_code TEXT,
                    created_at_ns INTEGER NOT NULL CHECK(created_at_ns > 0),
                    updated_at_ns INTEGER NOT NULL CHECK(updated_at_ns > 0),
                    CHECK((owner_state = 'POISONED' AND poison_code IS NOT NULL)
                       OR (owner_state = 'ACTIVE' AND poison_code IS NULL))
                );
                CREATE TRIGGER IF NOT EXISTS ctp_account_family_owner_immutable_identity
                BEFORE UPDATE ON ctp_account_family_owners
                WHEN NEW.family_key != OLD.family_key
                  OR NEW.owner_intent_id != OLD.owner_intent_id
                  OR NEW.account_key != OLD.account_key
                  OR NEW.scope_key != OLD.scope_key
                  OR NEW.environment != OLD.environment
                  OR NEW.trading_day != OLD.trading_day
                  OR (OLD.owner_state = 'POISONED' AND NEW.owner_state != 'POISONED')
                BEGIN
                    SELECT RAISE(ABORT, 'CTP account family owner identity is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_account_family_owner_no_delete
                BEFORE DELETE ON ctp_account_family_owners
                BEGIN
                    SELECT RAISE(ABORT, 'CTP account family owner is permanent');
                END;
                CREATE TABLE IF NOT EXISTS ctp_account_store_identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    ledger_kind TEXT NOT NULL
                        CHECK(ledger_kind = 'CTP_EXECUTION_V1'),
                    account_ref TEXT NOT NULL
                        CHECK(length(account_ref) = 83
                              AND account_ref GLOB 'ctp-account-ref.v1:*'
                              AND substr(account_ref, 20) NOT GLOB '*[^0-9a-f]*'),
                    family_key TEXT NOT NULL
                        CHECK(length(family_key) = 86
                              AND family_key GLOB 'ctp-account-family.v1:*'
                              AND substr(family_key, 23) NOT GLOB '*[^0-9a-f]*'),
                    journal_incarnation_id TEXT NOT NULL
                        CHECK(length(journal_incarnation_id) = 32
                              AND journal_incarnation_id NOT GLOB '*[^0-9a-f]*'),
                    created_at_ns INTEGER NOT NULL CHECK(created_at_ns > 0)
                );
                CREATE TRIGGER IF NOT EXISTS ctp_account_store_identity_immutable_update
                BEFORE UPDATE ON ctp_account_store_identity
                BEGIN
                    SELECT RAISE(ABORT, 'CTP account store identity is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_account_store_identity_immutable_delete
                BEFORE DELETE ON ctp_account_store_identity
                BEGIN
                    SELECT RAISE(ABORT, 'CTP account store identity is immutable');
                END;
                CREATE TABLE IF NOT EXISTS ctp_account_family_legacy_fences (
                    fence_id INTEGER PRIMARY KEY CHECK(fence_id = 1),
                    reason_code TEXT NOT NULL
                        CHECK(reason_code = 'unmapped_legacy_execution_history'),
                    source_schema_version TEXT NOT NULL,
                    created_at_ns INTEGER NOT NULL CHECK(created_at_ns > 0)
                );
                CREATE TRIGGER IF NOT EXISTS ctp_account_family_legacy_fence_immutable_update
                BEFORE UPDATE ON ctp_account_family_legacy_fences
                BEGIN
                    SELECT RAISE(ABORT, 'CTP account family migration fence is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_account_family_legacy_fence_immutable_delete
                BEFORE DELETE ON ctp_account_family_legacy_fences
                BEGIN
                    SELECT RAISE(ABORT, 'CTP account family migration fence is permanent');
                END;
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
                CREATE TABLE IF NOT EXISTS ctp_order_target_projections (
                    account_key TEXT NOT NULL,
                    projection_id TEXT NOT NULL
                        CHECK(length(projection_id) = 32 AND projection_id NOT GLOB '*[^0-9a-f]*'),
                    runtime_order_id TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    trading_day TEXT NOT NULL,
                    managed_intent_id TEXT NOT NULL,
                    order_ref TEXT NOT NULL
                        CHECK(length(order_ref) = 12 AND order_ref NOT GLOB '*[^0-9]*'),
                    session_generation_id TEXT NOT NULL,
                    connection_generation INTEGER NOT NULL CHECK(connection_generation > 0),
                    query_front_id INTEGER NOT NULL CHECK(query_front_id > 0),
                    query_session_id INTEGER NOT NULL CHECK(query_session_id > 0),
                    query_request_id INTEGER NOT NULL CHECK(query_request_id > 0),
                    projection_payload_json TEXT NOT NULL,
                    projection_sha256 TEXT NOT NULL CHECK(length(projection_sha256) = 64),
                    verified_at_ns INTEGER NOT NULL CHECK(verified_at_ns > 0),
                    expires_at_ns INTEGER NOT NULL CHECK(expires_at_ns > verified_at_ns),
                    created_at_ns INTEGER NOT NULL CHECK(created_at_ns > 0),
                    PRIMARY KEY(account_key, projection_id),
                    FOREIGN KEY(account_key, runtime_order_id)
                        REFERENCES ctp_order_identity_reservations(account_key, runtime_order_id)
                );
                CREATE INDEX IF NOT EXISTS ctp_order_target_projection_latest
                    ON ctp_order_target_projections(
                        account_key, scope_key, trading_day, runtime_order_id,
                        created_at_ns DESC
                    );
                CREATE UNIQUE INDEX IF NOT EXISTS ctp_order_target_query_request_once
                    ON ctp_order_target_projections(
                        account_key, scope_key, trading_day, session_generation_id,
                        connection_generation, query_request_id
                    );
                CREATE TABLE IF NOT EXISTS ctp_order_target_projection_consumptions (
                    account_key TEXT NOT NULL,
                    projection_id TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    consumed_at_ns INTEGER NOT NULL CHECK(consumed_at_ns > 0),
                    PRIMARY KEY(account_key, projection_id),
                    FOREIGN KEY(account_key, projection_id)
                        REFERENCES ctp_order_target_projections(account_key, projection_id),
                    FOREIGN KEY(account_key, command_id)
                        REFERENCES ctp_dispatch_commands(account_key, command_id)
                );
                CREATE INDEX IF NOT EXISTS ctp_order_target_consumption_command
                    ON ctp_order_target_projection_consumptions(
                        account_key, command_id, consumed_at_ns DESC
                    );
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
                    native_action_ref_int INTEGER
                        CHECK(native_action_ref_int IS NULL OR
                              (typeof(native_action_ref_int) = 'integer' AND
                               native_action_ref_int BETWEEN 1 AND 2147483647)),
                    native_request_payload_json TEXT,
                    native_request_payload_sha256 TEXT
                        CHECK(native_request_payload_sha256 IS NULL OR
                              length(native_request_payload_sha256) = 64),
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
                    callback_owner_intent_id TEXT,
                    native_call_inflight INTEGER NOT NULL DEFAULT 0
                        CHECK(native_call_inflight IN (0, 1)),
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
                CREATE TABLE IF NOT EXISTS ctp_native_action_ref_counters (
                    account_key TEXT PRIMARY KEY,
                    last_action_ref INTEGER NOT NULL
                        CHECK(typeof(last_action_ref) = 'integer' AND
                              last_action_ref BETWEEN 0 AND 2147483647),
                    updated_at_ns INTEGER NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS ctp_native_action_ref_counter_monotonic
                BEFORE UPDATE OF last_action_ref ON ctp_native_action_ref_counters
                WHEN NEW.last_action_ref != OLD.last_action_ref + 1
                     OR NEW.last_action_ref > 2147483647
                BEGIN
                    SELECT RAISE(ABORT, 'CTP native ActionRef counter must increment by one');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_native_action_ref_counter_no_delete
                BEFORE DELETE ON ctp_native_action_ref_counters
                BEGIN
                    SELECT RAISE(ABORT, 'CTP native ActionRef counter is durable');
                END;
                CREATE TABLE IF NOT EXISTS ctp_native_action_ref_allocations (
                    account_key TEXT NOT NULL,
                    native_action_ref INTEGER NOT NULL
                        CHECK(typeof(native_action_ref) = 'integer' AND
                              native_action_ref BETWEEN 1 AND 2147483647),
                    scope_key TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    managed_action_id TEXT NOT NULL,
                    allocated_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, native_action_ref),
                    UNIQUE(account_key, command_id),
                    UNIQUE(account_key, scope_key, managed_action_id),
                    FOREIGN KEY(account_key, command_id)
                        REFERENCES ctp_dispatch_commands(account_key, command_id)
                );
                CREATE TRIGGER IF NOT EXISTS ctp_native_action_ref_allocations_immutable_update
                BEFORE UPDATE ON ctp_native_action_ref_allocations
                BEGIN
                    SELECT RAISE(ABORT, 'CTP native ActionRef allocation is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_native_action_ref_allocations_immutable_delete
                BEFORE DELETE ON ctp_native_action_ref_allocations
                BEGIN
                    SELECT RAISE(ABORT, 'CTP native ActionRef allocation is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_session_owner_write_once
                BEFORE UPDATE ON ctp_dispatch_commands
                WHEN
                    (OLD.callback_owner_intent_id IS NOT NULL
                     AND NEW.callback_owner_intent_id != OLD.callback_owner_intent_id)
                    OR (OLD.callback_owner_intent_id IS NULL
                        AND NEW.callback_owner_intent_id IS NOT NULL
                        AND NOT (OLD.status = 'READY' AND NEW.status = 'CLAIMED'
                                 AND NEW.native_call_inflight = 1))
                    OR (OLD.native_call_inflight != NEW.native_call_inflight
                        AND NOT ((OLD.native_call_inflight = 0
                                  AND NEW.native_call_inflight = 1
                                  AND OLD.status = 'READY' AND NEW.status = 'CLAIMED'
                                  AND NEW.callback_owner_intent_id IS NOT NULL)
                              OR (OLD.native_call_inflight = 1
                                  AND NEW.native_call_inflight = 0
                                  AND OLD.status = 'CLAIMED'
                                  AND NEW.status IN ('COMPLETED', 'UNKNOWN'))))
                BEGIN
                    SELECT RAISE(ABORT, 'CTP session owner command binding is immutable');
                END;
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
                CREATE TABLE IF NOT EXISTS ctp_dispatch_trade_fact_ledger (
                    account_key TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    trading_day TEXT NOT NULL,
                    owner_intent_id TEXT NOT NULL,
                    source_sequence INTEGER NOT NULL CHECK(source_sequence > 0),
                    ingress_record_digest_sha256 TEXT NOT NULL
                        CHECK(length(ingress_record_digest_sha256) = 64),
                    command_id TEXT NOT NULL,
                    managed_intent_id TEXT NOT NULL,
                    runtime_order_id TEXT NOT NULL,
                    order_ref TEXT NOT NULL,
                    session_generation_id TEXT NOT NULL,
                    callback_stream_id TEXT NOT NULL,
                    callback_event_id TEXT NOT NULL,
                    exchange_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL,
                    order_sys_id TEXT NOT NULL,
                    instrument_id TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    offset_flag TEXT NOT NULL,
                    hedge_flag TEXT NOT NULL,
                    trade_price TEXT NOT NULL,
                    trade_volume INTEGER NOT NULL CHECK(trade_volume > 0),
                    cumulative_trade_volume INTEGER NOT NULL CHECK(cumulative_trade_volume > 0),
                    cumulative_order_volume INTEGER,
                    prefix_consistency TEXT NOT NULL CHECK(prefix_consistency IN (
                        'NO_ORDER_CUMULATIVE', 'MATCHED',
                        'ORDER_CUMULATIVE_AHEAD', 'TRADE_CUMULATIVE_AHEAD'
                    )),
                    cumulative_commission TEXT CHECK(cumulative_commission IS NULL),
                    commission_quality TEXT NOT NULL CHECK(commission_quality = 'INCOMPLETE'),
                    fact_payload_json TEXT NOT NULL,
                    fact_payload_sha256 TEXT NOT NULL CHECK(length(fact_payload_sha256) = 64),
                    source_digest_sha256 TEXT NOT NULL CHECK(length(source_digest_sha256) = 64),
                    verifier_id TEXT NOT NULL,
                    verified_at_ns INTEGER NOT NULL,
                    applied_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, trading_day, exchange_id, trade_id),
                    UNIQUE(owner_intent_id, source_sequence),
                    FOREIGN KEY(account_key, command_id)
                        REFERENCES ctp_dispatch_commands(account_key, command_id),
                    FOREIGN KEY(owner_intent_id, source_sequence)
                        REFERENCES ctp_dispatch_callback_ingress(owner_intent_id, source_sequence)
                );
                CREATE INDEX IF NOT EXISTS ctp_dispatch_trade_command
                    ON ctp_dispatch_trade_fact_ledger(account_key, command_id, source_sequence);
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_trade_fact_immutable_update
                BEFORE UPDATE ON ctp_dispatch_trade_fact_ledger
                BEGIN
                    SELECT RAISE(ABORT, 'CTP trade fact ledger is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_trade_fact_immutable_delete
                BEFORE DELETE ON ctp_dispatch_trade_fact_ledger
                BEGIN
                    SELECT RAISE(ABORT, 'CTP trade fact ledger is immutable');
                END;
                CREATE TABLE IF NOT EXISTS ctp_dispatch_order_cumulative_ledger (
                    owner_intent_id TEXT NOT NULL,
                    source_sequence INTEGER NOT NULL CHECK(source_sequence > 0),
                    account_key TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    trading_day TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    runtime_order_id TEXT NOT NULL,
                    order_ref TEXT NOT NULL,
                    native_volume_traded INTEGER NOT NULL CHECK(native_volume_traded >= 0),
                    trade_volume_at_prefix INTEGER NOT NULL CHECK(trade_volume_at_prefix >= 0),
                    prefix_consistency TEXT NOT NULL CHECK(prefix_consistency IN (
                        'MATCHED', 'ORDER_CUMULATIVE_AHEAD', 'TRADE_CUMULATIVE_AHEAD'
                    )),
                    ingress_record_digest_sha256 TEXT NOT NULL
                        CHECK(length(ingress_record_digest_sha256) = 64),
                    callback_key_sha256 TEXT NOT NULL CHECK(length(callback_key_sha256) = 64),
                    observed_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(owner_intent_id, source_sequence),
                    FOREIGN KEY(account_key, command_id)
                        REFERENCES ctp_dispatch_commands(account_key, command_id),
                    FOREIGN KEY(owner_intent_id, source_sequence)
                        REFERENCES ctp_dispatch_callback_ingress(owner_intent_id, source_sequence)
                );
                CREATE INDEX IF NOT EXISTS ctp_dispatch_order_cumulative_command
                    ON ctp_dispatch_order_cumulative_ledger(account_key, command_id, source_sequence);
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_order_cumulative_immutable_update
                BEFORE UPDATE ON ctp_dispatch_order_cumulative_ledger
                BEGIN
                    SELECT RAISE(ABORT, 'CTP order cumulative ledger is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_order_cumulative_immutable_delete
                BEFORE DELETE ON ctp_dispatch_order_cumulative_ledger
                BEGIN
                    SELECT RAISE(ABORT, 'CTP order cumulative ledger is immutable');
                END;
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
                CREATE TABLE IF NOT EXISTS ctp_dispatch_callback_session_owners (
                    owner_intent_id TEXT PRIMARY KEY
                        CHECK(length(owner_intent_id) = 32
                              AND owner_intent_id NOT GLOB '*[^0-9a-f]*'),
                    account_key TEXT NOT NULL UNIQUE,
                    scope_key TEXT NOT NULL,
                    owner_state TEXT NOT NULL
                        CHECK(owner_state IN ('PREPARED', 'ACTIVE', 'POISONED')),
                    writer_owner_id TEXT NOT NULL,
                    writer_fencing_token INTEGER NOT NULL CHECK(writer_fencing_token > 0),
                    source_instance_id TEXT,
                    native_client_epoch TEXT,
                    native_api_source_id TEXT,
                    native_spi_source_id TEXT,
                    native_api_generation INTEGER,
                    connection_generation INTEGER,
                    trading_day TEXT,
                    front_id INTEGER,
                    session_id INTEGER,
                    active_connection_generation INTEGER,
                    last_source_sequence INTEGER NOT NULL DEFAULT 0
                        CHECK(last_source_sequence >= 0),
                    economic_query_observed INTEGER NOT NULL DEFAULT 0
                        CHECK(economic_query_observed IN (0, 1)),
                    poison_observed_sequence INTEGER CHECK(
                        poison_observed_sequence IS NULL OR poison_observed_sequence >= 0
                    ),
                    poison_code TEXT,
                    created_at_ns INTEGER NOT NULL,
                    updated_at_ns INTEGER NOT NULL,
                    CHECK((owner_state = 'POISONED' AND poison_code IS NOT NULL)
                       OR (owner_state != 'POISONED' AND poison_code IS NULL)),
                    CHECK((owner_state = 'PREPARED' AND trading_day IS NULL
                           AND front_id IS NULL AND session_id IS NULL
                           AND active_connection_generation IS NULL)
                       OR (owner_state = 'ACTIVE'
                           AND trading_day IS NOT NULL AND front_id IS NOT NULL
                           AND session_id IS NOT NULL AND active_connection_generation > 0)
                       OR (owner_state = 'POISONED'
                           AND ((trading_day IS NULL AND front_id IS NULL AND session_id IS NULL)
                             OR (trading_day IS NOT NULL AND front_id IS NOT NULL
                                 AND session_id IS NOT NULL
                                 AND active_connection_generation > 0))))
                );
                CREATE TRIGGER IF NOT EXISTS ctp_callback_session_owner_immutable_identity
                BEFORE UPDATE ON ctp_dispatch_callback_session_owners
                WHEN NEW.owner_intent_id != OLD.owner_intent_id
                  OR NEW.account_key != OLD.account_key
                  OR NEW.scope_key != OLD.scope_key
                  OR NEW.writer_owner_id != OLD.writer_owner_id
                  OR NEW.writer_fencing_token != OLD.writer_fencing_token
                  OR NEW.last_source_sequence < OLD.last_source_sequence
                  OR (OLD.economic_query_observed = 1 AND NEW.economic_query_observed != 1)
                  OR (OLD.owner_state = 'POISONED' AND NEW.owner_state != 'POISONED')
                  OR (OLD.owner_state = 'ACTIVE' AND NEW.owner_state = 'PREPARED')
                  OR (OLD.owner_state != 'PREPARED' AND (
                        NEW.source_instance_id != OLD.source_instance_id
                     OR NEW.native_client_epoch != OLD.native_client_epoch
                     OR NEW.native_api_source_id != OLD.native_api_source_id
                     OR NEW.native_spi_source_id != OLD.native_spi_source_id
                     OR NEW.native_api_generation != OLD.native_api_generation
                     OR NEW.connection_generation != OLD.connection_generation
                     OR NEW.trading_day != OLD.trading_day
                     OR NEW.front_id != OLD.front_id
                     OR NEW.session_id != OLD.session_id
                     OR NEW.active_connection_generation != OLD.active_connection_generation
                  ))
                BEGIN
                    SELECT RAISE(ABORT, 'CTP callback session owner identity is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_callback_session_owner_no_delete
                BEFORE DELETE ON ctp_dispatch_callback_session_owners
                BEGIN
                    SELECT RAISE(ABORT, 'CTP callback session owner is permanent');
                END;
                CREATE TABLE IF NOT EXISTS ctp_dispatch_callback_ingress (
                    owner_intent_id TEXT NOT NULL,
                    source_sequence INTEGER NOT NULL CHECK(source_sequence > 0),
                    record_digest_sha256 TEXT NOT NULL
                        CHECK(length(record_digest_sha256) = 64),
                    callback_name TEXT NOT NULL,
                    callback_class TEXT NOT NULL,
                    source_phase TEXT NOT NULL,
                    source_instance_id TEXT NOT NULL,
                    native_client_epoch TEXT NOT NULL,
                    native_api_source_id TEXT NOT NULL,
                    native_spi_source_id TEXT NOT NULL,
                    native_api_generation INTEGER NOT NULL CHECK(native_api_generation > 0),
                    source_connection_generation INTEGER NOT NULL
                        CHECK(source_connection_generation >= 0),
                    connection_generation INTEGER NOT NULL CHECK(connection_generation >= 0),
                    record_payload_json TEXT NOT NULL,
                    captured_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(owner_intent_id, source_sequence),
                    FOREIGN KEY(owner_intent_id)
                        REFERENCES ctp_dispatch_callback_session_owners(owner_intent_id)
                );
                CREATE TRIGGER IF NOT EXISTS ctp_callback_ingress_immutable_update
                BEFORE UPDATE ON ctp_dispatch_callback_ingress
                BEGIN
                    SELECT RAISE(ABORT, 'CTP callback ingress record is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_callback_ingress_immutable_delete
                BEFORE DELETE ON ctp_dispatch_callback_ingress
                BEGIN
                    SELECT RAISE(ABORT, 'CTP callback ingress record is immutable');
                END;
                CREATE TABLE IF NOT EXISTS ctp_dispatch_callback_ingress_applications (
                    owner_intent_id TEXT NOT NULL,
                    source_sequence INTEGER NOT NULL CHECK(source_sequence > 0),
                    outcome TEXT NOT NULL CHECK(outcome IN ('AUDIT_ONLY', 'APPLIED', 'POISONED')),
                    command_id TEXT,
                    application_digest_sha256 TEXT NOT NULL
                        CHECK(length(application_digest_sha256) = 64),
                    applied_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(owner_intent_id, source_sequence),
                    FOREIGN KEY(owner_intent_id, source_sequence)
                        REFERENCES ctp_dispatch_callback_ingress(owner_intent_id, source_sequence)
                );
                CREATE TRIGGER IF NOT EXISTS ctp_callback_ingress_applications_immutable_update
                BEFORE UPDATE ON ctp_dispatch_callback_ingress_applications
                BEGIN
                    SELECT RAISE(ABORT, 'CTP callback ingress application is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_callback_ingress_applications_immutable_delete
                BEFORE DELETE ON ctp_dispatch_callback_ingress_applications
                BEGIN
                    SELECT RAISE(ABORT, 'CTP callback ingress application is immutable');
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
                CREATE TRIGGER IF NOT EXISTS ctp_order_target_projections_immutable_update
                BEFORE UPDATE ON ctp_order_target_projections
                BEGIN
                    SELECT RAISE(ABORT, 'CTP order-target projection is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_order_target_projections_immutable_delete
                BEFORE DELETE ON ctp_order_target_projections
                BEGIN
                    SELECT RAISE(ABORT, 'CTP order-target projection is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_order_target_consumptions_immutable_update
                BEFORE UPDATE ON ctp_order_target_projection_consumptions
                BEGIN
                    SELECT RAISE(ABORT, 'CTP order-target consumption is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_order_target_consumptions_immutable_delete
                BEFORE DELETE ON ctp_order_target_projection_consumptions
                BEGIN
                    SELECT RAISE(ABORT, 'CTP order-target consumption is immutable');
                END;
                """,
            )
            self._execute_schema_statements(
                cursor,
                """
                CREATE TABLE IF NOT EXISTS ctp_dispatch_cancel_postconditions (
                    account_key TEXT NOT NULL,
                    cancel_command_id TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    trading_day TEXT NOT NULL,
                    target_submit_command_id TEXT,
                    runtime_order_id TEXT,
                    target_order_ref TEXT,
                    target_exchange_id TEXT,
                    target_order_sys_id TEXT,
                    target_front_id INTEGER,
                    target_session_id INTEGER,
                    session_generation_id TEXT,
                    dispatch_front_id INTEGER,
                    dispatch_session_id INTEGER,
                    native_request_id INTEGER,
                    target_native_request_id INTEGER,
                    native_action_ref_int INTEGER,
                    owner_intent_id TEXT,
                    identity_state TEXT NOT NULL CHECK(identity_state IN ('EXACT', 'UNMAPPABLE')),
                    correlation_key_sha256 TEXT NOT NULL CHECK(length(correlation_key_sha256) = 64),
                    session_binding_sha256 TEXT NOT NULL CHECK(length(session_binding_sha256) = 64),
                    created_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, cancel_command_id),
                    CHECK(identity_state != 'EXACT' OR (
                        target_submit_command_id IS NOT NULL
                        AND runtime_order_id IS NOT NULL
                        AND target_order_ref IS NOT NULL
                        AND target_exchange_id IS NOT NULL
                        AND target_order_sys_id IS NOT NULL
                        AND target_front_id IS NOT NULL
                        AND target_session_id IS NOT NULL
                        AND session_generation_id IS NOT NULL
                        AND dispatch_front_id IS NOT NULL
                        AND dispatch_session_id IS NOT NULL
                        AND native_request_id IS NOT NULL
                        AND target_native_request_id IS NOT NULL
                        AND native_action_ref_int IS NOT NULL
                        AND owner_intent_id IS NOT NULL
                    )),
                    FOREIGN KEY(account_key, cancel_command_id)
                        REFERENCES ctp_dispatch_commands(account_key, command_id),
                    FOREIGN KEY(account_key, target_submit_command_id)
                        REFERENCES ctp_dispatch_commands(account_key, command_id),
                    FOREIGN KEY(owner_intent_id)
                        REFERENCES ctp_dispatch_callback_session_owners(owner_intent_id)
                );
                CREATE INDEX IF NOT EXISTS ctp_dispatch_cancel_postconditions_target
                    ON ctp_dispatch_cancel_postconditions(
                        account_key, target_submit_command_id, identity_state
                    );
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_cancel_postconditions_immutable_update
                BEFORE UPDATE ON ctp_dispatch_cancel_postconditions
                BEGIN
                    SELECT RAISE(ABORT, 'CTP cancel postcondition is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_cancel_postconditions_immutable_delete
                BEFORE DELETE ON ctp_dispatch_cancel_postconditions
                BEGIN
                    SELECT RAISE(ABORT, 'CTP cancel postcondition is immutable');
                END;
                CREATE TABLE IF NOT EXISTS ctp_dispatch_cancel_terminal_observations (
                    account_key TEXT NOT NULL,
                    cancel_command_id TEXT NOT NULL,
                    owner_intent_id TEXT NOT NULL,
                    source_sequence INTEGER NOT NULL CHECK(source_sequence > 0),
                    submit_command_id TEXT NOT NULL,
                    terminal_state TEXT NOT NULL CHECK(terminal_state IN ('FILLED', 'CANCELLED', 'REJECTED')),
                    order_volume_traded INTEGER NOT NULL CHECK(order_volume_traded >= 0),
                    trade_volume_at_prefix INTEGER NOT NULL CHECK(trade_volume_at_prefix >= 0),
                    callback_key_sha256 TEXT NOT NULL CHECK(length(callback_key_sha256) = 64),
                    ingress_record_digest_sha256 TEXT NOT NULL
                        CHECK(length(ingress_record_digest_sha256) = 64),
                    source_digest_sha256 TEXT NOT NULL CHECK(length(source_digest_sha256) = 64),
                    identity_sha256 TEXT NOT NULL CHECK(length(identity_sha256) = 64),
                    observed_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, cancel_command_id, owner_intent_id, source_sequence),
                    FOREIGN KEY(account_key, cancel_command_id)
                        REFERENCES ctp_dispatch_cancel_postconditions(account_key, cancel_command_id),
                    FOREIGN KEY(account_key, submit_command_id)
                        REFERENCES ctp_dispatch_commands(account_key, command_id),
                    FOREIGN KEY(owner_intent_id, source_sequence)
                        REFERENCES ctp_dispatch_callback_ingress(owner_intent_id, source_sequence)
                );
                CREATE INDEX IF NOT EXISTS ctp_dispatch_cancel_terminal_by_target
                    ON ctp_dispatch_cancel_terminal_observations(
                        account_key, submit_command_id, source_sequence
                    );
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_cancel_terminal_observations_immutable_update
                BEFORE UPDATE ON ctp_dispatch_cancel_terminal_observations
                BEGIN
                    SELECT RAISE(ABORT, 'CTP cancel terminal observation is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_cancel_terminal_observations_immutable_delete
                BEFORE DELETE ON ctp_dispatch_cancel_terminal_observations
                BEGIN
                    SELECT RAISE(ABORT, 'CTP cancel terminal observation is immutable');
                END;
                CREATE TABLE IF NOT EXISTS ctp_dispatch_cancel_postcondition_resolutions (
                    account_key TEXT NOT NULL,
                    cancel_command_id TEXT NOT NULL,
                    terminal_owner_intent_id TEXT NOT NULL,
                    terminal_source_sequence INTEGER NOT NULL CHECK(terminal_source_sequence > 0),
                    reconciliation_owner_intent_id TEXT NOT NULL,
                    reconciliation_source_sequence INTEGER NOT NULL CHECK(reconciliation_source_sequence > 0),
                    order_volume_traded INTEGER NOT NULL CHECK(order_volume_traded >= 0),
                    trade_volume INTEGER NOT NULL CHECK(trade_volume >= 0),
                    terminal_record_digest_sha256 TEXT NOT NULL
                        CHECK(length(terminal_record_digest_sha256) = 64),
                    reconciliation_record_digest_sha256 TEXT NOT NULL
                        CHECK(length(reconciliation_record_digest_sha256) = 64),
                    resolution_digest_sha256 TEXT NOT NULL CHECK(length(resolution_digest_sha256) = 64),
                    resolved_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_key, cancel_command_id),
                    CHECK(order_volume_traded = trade_volume),
                    FOREIGN KEY(account_key, cancel_command_id)
                        REFERENCES ctp_dispatch_cancel_postconditions(account_key, cancel_command_id),
                    FOREIGN KEY(account_key, cancel_command_id, terminal_owner_intent_id,
                                terminal_source_sequence)
                        REFERENCES ctp_dispatch_cancel_terminal_observations(
                            account_key, cancel_command_id, owner_intent_id, source_sequence
                        ),
                    FOREIGN KEY(terminal_owner_intent_id, terminal_source_sequence)
                        REFERENCES ctp_dispatch_callback_ingress(owner_intent_id, source_sequence),
                    FOREIGN KEY(reconciliation_owner_intent_id, reconciliation_source_sequence)
                        REFERENCES ctp_dispatch_callback_ingress(owner_intent_id, source_sequence)
                );
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_cancel_postcondition_resolutions_immutable_update
                BEFORE UPDATE ON ctp_dispatch_cancel_postcondition_resolutions
                BEGIN
                    SELECT RAISE(ABORT, 'CTP cancel postcondition resolution is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_cancel_postcondition_resolutions_immutable_delete
                BEFORE DELETE ON ctp_dispatch_cancel_postcondition_resolutions
                BEGIN
                    SELECT RAISE(ABORT, 'CTP cancel postcondition resolution is immutable');
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
            outbox_columns = {
                str(item["name"])
                for item in cursor.execute("PRAGMA table_info(execution_outbox)").fetchall()
            }
            if "journal_incarnation_id" not in outbox_columns:
                cursor.execute(
                    "ALTER TABLE execution_outbox ADD COLUMN journal_incarnation_id TEXT"
                )
            cursor.execute(
                "INSERT OR IGNORE INTO execution_meta(key, value) VALUES (?, ?)",
                ("journal_incarnation_id", uuid.uuid4().hex),
            )
            target_projection_tables = {
                "ctp_order_target_projections",
                "ctp_order_target_projection_consumptions",
            }
            legacy_guard_tables = {
                "ctp_dispatch_callback_ingestion_guards",
                "ctp_dispatch_callback_ingestion_resolutions",
            }
            if version == "11":
                if "ctp_dispatch_callback_ledger" not in previous_tables:
                    raise DurableStoreError(
                        "schema 11 callback ledger table is missing"
                    )
                had_target_projection_tables = target_projection_tables.issubset(
                    previous_tables
                )
                had_legacy_guard_tables = legacy_guard_tables.issubset(previous_tables)
                has_partial_target_tables = bool(
                    target_projection_tables & previous_tables
                ) and not had_target_projection_tables
                has_partial_guard_tables = bool(legacy_guard_tables & previous_tables) and not (
                    had_legacy_guard_tables
                )
                if (
                    had_target_projection_tables == had_legacy_guard_tables
                    or has_partial_target_tables
                    or has_partial_guard_tables
                ):
                    raise DurableStoreError(
                        "ambiguous schema 11 execution store lineage"
                    )
            elif version == "12":
                if "ctp_dispatch_callback_ledger" not in previous_tables:
                    raise DurableStoreError(
                        "schema 12 callback ledger table is missing"
                    )
                if "ctp_dispatch_callback_source_lifecycle_fences" not in previous_tables:
                    raise DurableStoreError(
                        "schema 12 callback lifecycle fence table is missing"
                    )
                if target_projection_tables & previous_tables:
                    raise DurableStoreError(
                        "inconsistent schema 12 execution store lineage"
                    )
                had_legacy_guard_tables = legacy_guard_tables.issubset(previous_tables)
                has_partial_guard_tables = bool(legacy_guard_tables & previous_tables) and not (
                    had_legacy_guard_tables
                )
                if has_partial_guard_tables:
                    raise DurableStoreError(
                        "inconsistent schema 12 callback guard lineage"
                    )
            elif version == "15":
                required_v15_tables = {
                    "ctp_dispatch_callback_ledger",
                    "ctp_dispatch_callback_source_lifecycle_fences",
                    "ctp_dispatch_callback_session_owners",
                    "ctp_dispatch_callback_ingress",
                    "ctp_dispatch_callback_ingress_applications",
                    "ctp_order_target_projections",
                    "ctp_order_target_projection_consumptions",
                }
                if not required_v15_tables.issubset(previous_tables):
                    raise DurableStoreError("incomplete schema 15 CTP callback lineage")
            elif version == "16":
                required_v16_tables = {
                    "ctp_dispatch_commands",
                    "ctp_dispatch_callback_ledger",
                    "ctp_dispatch_callback_source_lifecycle_fences",
                    "ctp_dispatch_callback_session_owners",
                    "ctp_dispatch_callback_ingress",
                    "ctp_dispatch_callback_ingress_applications",
                    "ctp_dispatch_trade_fact_ledger",
                    "ctp_order_target_projections",
                    "ctp_order_target_projection_consumptions",
                }
                if not required_v16_tables.issubset(previous_tables):
                    raise DurableStoreError("incomplete schema 16 CTP execution lineage")
            elif version == "17":
                required_v17_tables = {
                    "ctp_dispatch_commands",
                    "ctp_dispatch_callback_ledger",
                    "ctp_dispatch_callback_source_lifecycle_fences",
                    "ctp_dispatch_callback_session_owners",
                    "ctp_dispatch_callback_ingress",
                    "ctp_dispatch_callback_ingress_applications",
                    "ctp_dispatch_trade_fact_ledger",
                    "ctp_dispatch_order_cumulative_ledger",
                    "ctp_order_target_projections",
                    "ctp_order_target_projection_consumptions",
                }
                if not required_v17_tables.issubset(previous_tables):
                    raise DurableStoreError("incomplete schema 17 CTP execution lineage")
            elif version == "18":
                required_v18_tables = {
                    "execution_records",
                    "execution_outbox",
                    "execution_writer_leases",
                    "ctp_dispatch_commands",
                    "ctp_dispatch_callback_ledger",
                    "ctp_dispatch_callback_source_lifecycle_fences",
                    "ctp_dispatch_callback_session_owners",
                    "ctp_dispatch_callback_ingress",
                    "ctp_dispatch_callback_ingress_applications",
                    "ctp_dispatch_trade_fact_ledger",
                    "ctp_dispatch_order_cumulative_ledger",
                    "ctp_dispatch_cancel_postconditions",
                    "ctp_dispatch_cancel_terminal_observations",
                    "ctp_dispatch_cancel_postcondition_resolutions",
                    "ctp_order_target_projections",
                    "ctp_order_target_projection_consumptions",
                }
                if not required_v18_tables.issubset(previous_tables):
                    raise DurableStoreError("incomplete schema 18 CTP execution lineage")
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
                "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18",
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
                "native_action_ref_int": "INTEGER",
                "native_request_payload_json": "TEXT",
                "native_request_payload_sha256": "TEXT",
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
            session_owner_columns = {
                "callback_owner_intent_id": "TEXT",
                "native_call_inflight": "INTEGER NOT NULL DEFAULT 0 CHECK(native_call_inflight IN (0, 1))",
            }
            for name, declaration in session_owner_columns.items():
                if name not in columns:
                    cursor.execute(
                        f"ALTER TABLE ctp_dispatch_commands ADD COLUMN {name} {declaration}"
                    )
            if version != str(self._SCHEMA_VERSION):
                self._migrate_ctp_account_family_legacy_fence(
                    cursor,
                    previous_tables=previous_tables,
                    previous_version=version,
                )
                self._migrate_legacy_ctp_callback_source_lifecycle_fences(
                    cursor, previous_tables=previous_tables
                )
                self._migrate_legacy_ctp_action_ref_accounts(cursor)
                self._migrate_ctp_cancel_postconditions(cursor)
                # Old rows have no exact OrderRef cutover-session evidence.
                # Never let migration make those commands dispatchable.
                cursor.execute(
                    """
                    UPDATE ctp_dispatch_commands
                    SET status = 'UNKNOWN', unknown_at_ns = COALESCE(unknown_at_ns, updated_at_ns),
                        unknown_reason = COALESCE(
                            unknown_reason, 'schema_upgrade_requires_ctp_v2_dispatch_cutover'
                        )
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
                WHERE correlation_version IN (1, 2)
                """
            )
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ctp_dispatch_managed_action_unique
                ON ctp_dispatch_commands(account_key, scope_key, managed_action_id)
                WHERE correlation_version IN (1, 2)
                """
            )
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ctp_dispatch_native_action_ref_unique
                ON ctp_dispatch_commands(account_key, native_action_ref_int)
                WHERE correlation_version = 2 AND native_action_ref_int IS NOT NULL
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
                    native_action_ref_int, native_request_payload_json,
                    native_request_payload_sha256,
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
        if not isinstance(scope, ExecutionScope) or scope.provider != "ctp":
            raise ContractValidationError(
                "CTP order identity requires canonical lowercase CTP execution scope"
            )
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
    def _ctp_order_target_projection_from_payload(
        payload: Any,
    ) -> CtpVerifiedOrderTargetProjection:
        if (
            not isinstance(payload, dict)
            or set(payload) != set(CtpVerifiedOrderTargetProjection.__dataclass_fields__)
        ):
            raise ContractValidationError("stored CTP target projection payload is invalid")
        try:
            return CtpVerifiedOrderTargetProjection(**payload)
        except (TypeError, ValueError) as error:
            raise ContractValidationError("stored CTP target projection is invalid") from error

    @staticmethod
    def _validate_ctp_order_target_projection_binding(
        scope: ExecutionScope,
        reservation: CtpOrderIdentityReservation,
        projection: CtpVerifiedOrderTargetProjection,
    ) -> None:
        if type(projection) is not CtpVerifiedOrderTargetProjection:
            raise ContractValidationError("typed verified CTP order target is required")
        expected = (
            reservation.account_key,
            reservation.scope_key,
            reservation.trading_day,
            reservation.managed_intent_id,
            reservation.runtime_order_id,
            reservation.order_ref,
        )
        actual = (
            projection.account_key,
            projection.scope_key,
            projection.trading_day,
            projection.managed_intent_id,
            projection.runtime_order_id,
            projection.order_ref,
        )
        if expected != actual or (
            reservation.account_key != scope.account_key
            or reservation.scope_key != scope.key
            or reservation.trading_day != scope.trading_day
        ):
            raise ContractValidationError("verified CTP order target differs from I9 reservation")

    def issue_ctp_order_target_projection(
        self,
        scope: ExecutionScope,
        managed_intent_id: str,
        native_query_evidence: Any,
        *,
        verifier: CtpOrderTargetProjectionVerifier | None = None,
    ) -> CtpOrderTargetProjectionHandle:
        """Verify, append and read back one current CTP target for a reservation.

        The query adapter is injected and defaults to reject. A successful
        readback returns an ephemeral handle held by this exact store instance.
        Persisted OPEN/PARTIAL rows from an earlier process cannot be recovered
        as fresh handles after restart; a new native query and verifier result
        are required before a cancel may be staged or claimed. No native query
        verifier ships in this package, and this contract grants no production
        cancel authority.
        """

        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        reservation = self.read_ctp_order_identity(scope, managed_intent_id)
        if type(reservation) is not CtpOrderIdentityReservation:
            raise ContractValidationError("CTP target has no same-store OrderRef reservation")
        check_started_ns = time.monotonic_ns()
        authority = (
            _REJECT_CTP_ORDER_TARGET_PROJECTION_VERIFIER if verifier is None else verifier
        )
        verify_target = getattr(authority, "verify_order_target", None)
        if not callable(verify_target):
            raise ContractValidationError("trusted CTP order-target query verifier is required")
        verification_failed = False
        projection: CtpVerifiedOrderTargetProjection | None = None
        try:
            projection = verify_target(
                scope,
                reservation,
                native_query_evidence,
                now_ns=check_started_ns,
            )
        except Exception:
            verification_failed = True
        if verification_failed:
            raise ContractValidationError("CTP order-target query verification failed")
        if type(projection) is not CtpVerifiedOrderTargetProjection:
            raise ContractValidationError("invalid typed CTP order-target verification result")
        self._validate_ctp_order_target_projection_binding(scope, reservation, projection)
        if (
            projection.verified_at_ns != check_started_ns
            or projection.expires_at_ns <= check_started_ns
            or projection.session_generation_id == ""
        ):
            raise ContractValidationError("CTP order-target query is stale or unbound")
        projection_payload = projection.to_payload()
        projection_json = canonical_json(projection_payload)
        projection_digest = payload_sha256(projection_payload)
        projection_id = uuid.uuid4().hex
        persistence_check_ns = time.monotonic_ns()
        if projection.expires_at_ns <= persistence_check_ns:
            raise ContractValidationError("CTP order-target query expired before persistence")
        created_at_ns = time.time_ns()
        try:
            with self._transaction() as cursor:
                current = cursor.execute(
                    """
                    SELECT account_key, trading_day, scope_key, managed_intent_id,
                           runtime_order_id, order_ref, created_at_ns
                    FROM ctp_order_identity_reservations
                    WHERE account_key = ? AND trading_day = ? AND scope_key = ?
                      AND managed_intent_id = ?
                    """,
                    (account_key, trading_day, scope_key, managed_intent_id),
                ).fetchone()
                current_reservation = (
                    None if current is None else self._ctp_order_identity_from_row(current)
                )
                if current_reservation != reservation:
                    raise ContractValidationError(
                        "CTP target reservation changed during verification"
                    )
                cursor.execute(
                    """
                    INSERT INTO ctp_order_target_projections(
                        account_key, projection_id, runtime_order_id, scope_key, trading_day,
                        managed_intent_id, order_ref, session_generation_id,
                        connection_generation, query_front_id, query_session_id,
                        query_request_id, projection_payload_json, projection_sha256,
                        verified_at_ns, expires_at_ns, created_at_ns
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_key,
                        projection_id,
                        reservation.runtime_order_id,
                        scope_key,
                        trading_day,
                        managed_intent_id,
                        reservation.order_ref,
                        projection.session_generation_id,
                        projection.connection_generation,
                        projection.query_front_id,
                        projection.query_session_id,
                        projection.query_request_id,
                        projection_json,
                        projection_digest,
                        projection.verified_at_ns,
                        projection.expires_at_ns,
                        created_at_ns,
                    ),
                )
        except DurableStoreError as error:
            if isinstance(error.__cause__, sqlite3.IntegrityError):
                raise ContractValidationError(
                    "CTP native query identity is duplicate or already persisted"
                ) from error
            raise
        handle = CtpOrderTargetProjectionHandle(
            projection_id=projection_id,
            projection_sha256=projection_digest,
            projection=projection,
        )
        self._issued_ctp_order_target_projections[projection_id] = handle
        try:
            self.read_ctp_order_target_projection(scope, handle)
        except Exception:
            self._issued_ctp_order_target_projections.pop(projection_id, None)
            raise
        return handle

    def _read_ctp_order_target_projection_row(
        self,
        scope: ExecutionScope,
        handle: CtpOrderTargetProjectionHandle,
        *,
        cursor: sqlite3.Cursor | None = None,
        now_ns: int | None = None,
    ) -> CtpVerifiedOrderTargetProjection:
        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        if (
            type(handle) is not CtpOrderTargetProjectionHandle
            or self._issued_ctp_order_target_projections.get(handle.projection_id) is not handle
        ):
            raise ContractValidationError("fresh same-store CTP target handle is required")
        projection = handle.projection
        if (
            projection.account_key != account_key
            or projection.scope_key != scope_key
            or projection.trading_day != trading_day
        ):
            raise ContractValidationError("CTP target handle belongs to another scope")
        now = time.monotonic_ns() if now_ns is None else now_ns
        if type(now) is not int or now >= projection.expires_at_ns:
            raise ContractValidationError("CTP target handle is stale")
        if handle.projection_sha256 != payload_sha256(projection.to_payload()):
            raise ContractValidationError("CTP target handle digest differs")
        if cursor is None:
            with self._lock:
                row = self._connection.execute(
                    """
                    SELECT runtime_order_id, scope_key, trading_day, managed_intent_id,
                           order_ref, projection_payload_json, projection_sha256,
                           session_generation_id, connection_generation,
                           query_front_id, query_session_id, query_request_id,
                           verified_at_ns, expires_at_ns
                    FROM ctp_order_target_projections
                    WHERE account_key = ? AND projection_id = ?
                    """,
                    (account_key, handle.projection_id),
                ).fetchone()
        else:
            row = cursor.execute(
                """
                SELECT runtime_order_id, scope_key, trading_day, managed_intent_id,
                       order_ref, projection_payload_json, projection_sha256,
                       session_generation_id, connection_generation,
                       query_front_id, query_session_id, query_request_id,
                       verified_at_ns, expires_at_ns
                FROM ctp_order_target_projections
                WHERE account_key = ? AND projection_id = ?
                """,
                (account_key, handle.projection_id),
            ).fetchone()
        if row is None:
            raise ContractValidationError("persisted CTP target projection is missing")
        stored_json = str(row["projection_payload_json"])
        stored_digest = str(row["projection_sha256"])
        try:
            payload = json.loads(stored_json)
            stored_projection = self._ctp_order_target_projection_from_payload(payload)
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise ContractValidationError("persisted CTP target projection is invalid") from error
        if (
            canonical_json(stored_projection.to_payload()) != stored_json
            or stored_digest != payload_sha256(stored_projection.to_payload())
            or stored_digest != handle.projection_sha256
            or stored_projection != projection
            or str(row["runtime_order_id"]) != projection.runtime_order_id
            or str(row["scope_key"]) != projection.scope_key
            or str(row["trading_day"]) != projection.trading_day
            or str(row["managed_intent_id"]) != projection.managed_intent_id
            or str(row["order_ref"]) != projection.order_ref
            or str(row["session_generation_id"]) != projection.session_generation_id
            or int(row["connection_generation"]) != projection.connection_generation
            or int(row["query_front_id"]) != projection.query_front_id
            or int(row["query_session_id"]) != projection.query_session_id
            or int(row["query_request_id"]) != projection.query_request_id
            or int(row["verified_at_ns"]) != projection.verified_at_ns
            or int(row["expires_at_ns"]) != projection.expires_at_ns
        ):
            raise ContractValidationError("persisted CTP target projection readback differs")
        return stored_projection

    def read_ctp_order_target_projection(
        self,
        scope: ExecutionScope,
        handle: CtpOrderTargetProjectionHandle,
    ) -> CtpVerifiedOrderTargetProjection:
        """Read back only a handle issued by this live store instance."""

        return self._read_ctp_order_target_projection_row(scope, handle)

    def _record_ctp_order_target_projection_consumption(
        self,
        cursor: sqlite3.Cursor,
        *,
        account_key: str,
        projection_id: str,
        command_id: str,
        consumed_at_ns: int,
    ) -> None:
        existing = cursor.execute(
            """
            SELECT command_id FROM ctp_order_target_projection_consumptions
            WHERE account_key = ? AND projection_id = ?
            """,
            (account_key, projection_id),
        ).fetchone()
        if existing is not None:
            if str(existing["command_id"]) != command_id:
                raise IntentConflictError(
                    "CTP target projection is already bound to another action"
                )
            return
        command = cursor.execute(
            """
            SELECT operation FROM ctp_dispatch_commands
            WHERE account_key = ? AND command_id = ?
            """,
            (account_key, command_id),
        ).fetchone()
        if command is None or str(command["operation"]) != "CANCEL":
            raise ContractValidationError(
                "CTP target projection requires a persisted cancel action"
            )
        cursor.execute(
            """
            INSERT INTO ctp_order_target_projection_consumptions(
                account_key, projection_id, command_id, consumed_at_ns
            ) VALUES (?, ?, ?, ?)
            """,
            (account_key, projection_id, command_id, consumed_at_ns),
        )

    def _require_fresh_ctp_cancel_target_row(
        self,
        cursor: sqlite3.Cursor,
        scope: ExecutionScope,
        command_row: sqlite3.Row,
        *,
        now_ns: int,
    ) -> CtpVerifiedOrderTargetProjection:
        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        if (
            str(command_row["operation"]) != "CANCEL"
            or str(command_row["account_key"]) != account_key
            or str(command_row["scope_key"]) != scope_key
            or str(command_row["trading_day"]) != trading_day
        ):
            raise ContractValidationError("CTP cancel target is outside the exact I9 scope")
        consumption = cursor.execute(
            """
            SELECT projection_id FROM ctp_order_target_projection_consumptions
            WHERE account_key = ? AND command_id = ?
            ORDER BY rowid DESC LIMIT 1
            """,
            (account_key, str(command_row["command_id"])),
        ).fetchone()
        if consumption is None:
            raise ContractValidationError("CTP cancel action has no persisted target projection")
        projection_id = str(consumption["projection_id"])
        matching_consumption = cursor.execute(
            """
            SELECT command_id FROM ctp_order_target_projection_consumptions
            WHERE account_key = ? AND projection_id = ?
            """,
            (account_key, projection_id),
        ).fetchall()
        if (
            len(matching_consumption) != 1
            or str(matching_consumption[0]["command_id"])
            != str(command_row["command_id"])
        ):
            raise ContractValidationError("CTP target projection is not singly consumed by this action")
        handle = self._issued_ctp_order_target_projections.get(projection_id)
        if handle is None:
            raise ContractValidationError("fresh CTP target query is required after store restart")
        projection = self._read_ctp_order_target_projection_row(
            scope, handle, cursor=cursor, now_ns=now_ns
        )
        reservation = cursor.execute(
            """
            SELECT account_key, trading_day, scope_key, managed_intent_id,
                   runtime_order_id, order_ref, created_at_ns
            FROM ctp_order_identity_reservations
            WHERE account_key = ? AND trading_day = ? AND scope_key = ?
              AND managed_intent_id = ? AND runtime_order_id = ? AND order_ref = ?
            """,
            (
                account_key,
                trading_day,
                scope_key,
                str(command_row["reservation_managed_intent_id"]),
                str(command_row["runtime_order_id"]),
                str(command_row["cancel_target_order_ref"]),
            ),
        ).fetchone()
        if reservation is None:
            raise ContractValidationError(
                "persisted CTP cancel target lost its OrderRef reservation"
            )
        identity = self._ctp_order_identity_from_row(reservation)
        self._validate_ctp_order_target_projection_binding(scope, identity, projection)
        try:
            request_payload = json.loads(str(command_row["request_payload_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise ContractValidationError("persisted CTP cancel request is unreadable") from error
        if (
            projection.session_generation_id != str(command_row["session_generation_id"])
            or projection.query_front_id != int(command_row["dispatch_front_id"])
            or projection.query_session_id != int(command_row["dispatch_session_id"])
            or not isinstance(request_payload, dict)
            or request_payload.get("InstrumentID") != projection.instrument_id
            or projection.exchange_id != str(command_row["cancel_target_exchange_id"])
            or projection.order_sys_id != str(command_row["cancel_target_order_sys_id"])
            or projection.front_id != int(command_row["cancel_target_front_id"])
            or projection.session_id != int(command_row["cancel_target_session_id"])
        ):
            raise ContractValidationError(
                "persisted CTP cancel target differs from its query projection"
            )
        return projection

    def require_fresh_ctp_cancel_target_for_command(
        self, scope: ExecutionScope, command_id: str
    ) -> CtpVerifiedOrderTargetProjection:
        """Recheck a claimed cancel's same-process query target immediately before dispatch."""

        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        self._validate_command_identifier(command_id, "command_id")
        with self._transaction() as cursor:
            row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, command_id),
            ).fetchone()
            if row is None or str(row["status"]) != "CLAIMED":
                raise ContractValidationError("fresh CTP cancel target requires a claimed command")
            return self._require_fresh_ctp_cancel_target_row(
                cursor, scope, row, now_ns=time.monotonic_ns()
            )

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
    def _allocate_ctp_native_action_ref(
        cursor: sqlite3.Cursor,
        *,
        account_key: str,
        scope_key: str,
        command_id: str,
        managed_action_id: str,
        allocated_at_ns: int,
    ) -> int:
        """Allocate one durable account-lifetime CTP int32 OrderActionRef."""

        existing = cursor.execute(
            """
            SELECT native_action_ref FROM ctp_native_action_ref_allocations
            WHERE account_key = ? AND command_id = ?
            """,
            (account_key, command_id),
        ).fetchone()
        if existing is not None:
            raise IntentConflictError("CTP native ActionRef was already allocated to this command")
        counter = cursor.execute(
            "SELECT last_action_ref FROM ctp_native_action_ref_counters WHERE account_key = ?",
            (account_key,),
        ).fetchone()
        if counter is None:
            next_value = 1
            cursor.execute(
                """
                INSERT INTO ctp_native_action_ref_counters(
                    account_key, last_action_ref, updated_at_ns
                ) VALUES (?, ?, ?)
                """,
                (account_key, next_value, allocated_at_ns),
            )
        else:
            previous = int(counter["last_action_ref"])
            if previous >= 2_147_483_647:
                raise DurableStoreError("CTP native ActionRef int32 sequence is exhausted")
            next_value = previous + 1
            cursor.execute(
                """
                UPDATE ctp_native_action_ref_counters
                SET last_action_ref = ?, updated_at_ns = ?
                WHERE account_key = ? AND last_action_ref = ?
                """,
                (next_value, allocated_at_ns, account_key, previous),
            )
            if cursor.rowcount != 1:
                raise DurableStoreError("CTP native ActionRef counter changed unexpectedly")
        if type(next_value) is not int or not 1 <= next_value <= 2_147_483_647:
            raise DurableStoreError("CTP native ActionRef allocator produced an invalid value")
        return next_value

    @staticmethod
    def _require_ctp_native_action_ref_allocation(
        cursor: sqlite3.Cursor,
        row: sqlite3.Row,
        correlation: CtpDispatchCorrelationKey,
    ) -> None:
        """Match a V2 cancel command to its immutable allocator row and counter."""

        if correlation.version != 2:
            raise InvalidStateTransition("legacy CTP command is audit-only")
        if correlation.operation == "SUBMIT":
            if correlation.native_action_ref is not None or row["native_action_ref_int"] is not None:
                raise DurableStoreError("CTP submit unexpectedly carries a native ActionRef")
            allocated = cursor.execute(
                "SELECT 1 FROM ctp_native_action_ref_allocations WHERE account_key = ? AND command_id = ?",
                (str(row["account_key"]), str(row["command_id"])),
            ).fetchone()
            if allocated is not None:
                raise DurableStoreError("CTP submit has a native ActionRef allocation")
            return
        action_ref = correlation.native_action_ref
        if type(action_ref) is not int or not 1 <= action_ref <= 2_147_483_647:
            raise DurableStoreError("CTP cancel lacks a native ActionRef integer")
        allocation = cursor.execute(
            """
            SELECT native_action_ref, scope_key, managed_action_id
            FROM ctp_native_action_ref_allocations
            WHERE account_key = ? AND command_id = ?
            """,
            (str(row["account_key"]), str(row["command_id"])),
        ).fetchone()
        counter = cursor.execute(
            "SELECT last_action_ref FROM ctp_native_action_ref_counters WHERE account_key = ?",
            (str(row["account_key"]),),
        ).fetchone()
        if (
            allocation is None
            or int(allocation["native_action_ref"]) != action_ref
            or str(allocation["scope_key"]) != str(row["scope_key"])
            or str(allocation["managed_action_id"]) != correlation.managed_action_id
            or row["native_action_ref_int"] is None
            or int(row["native_action_ref_int"]) != action_ref
            or counter is None
            or int(counter["last_action_ref"]) < action_ref
        ):
            raise DurableStoreError("CTP cancel ActionRef allocation is missing or mismatched")

    @staticmethod
    def _ctp_dispatch_command_from_row(row: sqlite3.Row) -> CtpDispatchCommand:
        try:
            request_payload = json.loads(str(row["request_payload_json"]))
            native_request_payload = (
                None
                if row["native_request_payload_json"] is None
                else json.loads(str(row["native_request_payload_json"]))
            )
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
            if correlation_version in {1, 2}:
                action_order_ref = (
                    str(row["order_ref"])
                    if row["order_ref"] is not None
                    else str(row["cancel_target_order_ref"])
                )
                native_action_ref: str | int | None
                if correlation_version == 1:
                    native_action_ref = (
                        None
                        if row["native_action_ref"] is None
                        else str(row["native_action_ref"])
                    )
                else:
                    native_action_ref = (
                        None
                        if row["native_action_ref_int"] is None
                        else int(row["native_action_ref_int"])
                    )
                native_payload_digest = (
                    None
                    if row["native_request_payload_sha256"] is None
                    else str(row["native_request_payload_sha256"])
                )
                correlation_key = CtpDispatchCorrelationKey(
                    version=correlation_version,
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
                    native_action_ref=native_action_ref,
                    native_request_payload_sha256=native_payload_digest,
                )
                if (
                    session_binding.get("session_generation_id")
                    != correlation_key.session_generation_id
                    or session_binding.get("dispatch_front_id") != correlation_key.dispatch_front_id
                    or session_binding.get("dispatch_session_id")
                    != correlation_key.dispatch_session_id
                ):
                    raise ValueError("stored CTP session binding differs from correlation")
                if correlation_version == 1:
                    if (
                        row["native_action_ref_int"] is not None
                        or native_request_payload is not None
                        or row["native_request_payload_sha256"] is not None
                    ):
                        raise ValueError("legacy CTP command has V2 native request fields")
                else:
                    expected_native_payload = dict(request_payload)
                    if correlation_key.operation == "CANCEL":
                        expected_native_payload["OrderActionRef"] = correlation_key.native_action_ref
                    if (
                        row["native_action_ref"] is not None
                        or not isinstance(native_request_payload, dict)
                        or canonical_json(native_request_payload)
                        != str(row["native_request_payload_json"])
                        or payload_sha256(native_request_payload) != native_payload_digest
                        or native_request_payload != expected_native_payload
                    ):
                        raise ValueError("stored CTP V2 native request payload is inconsistent")
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
                        "native_action_ref_int",
                        "native_request_payload_json",
                        "native_request_payload_sha256",
                    )
                ):
                    raise ValueError("legacy CTP command has partial correlation keys")
            else:
                raise ValueError("unsupported stored CTP correlation version")
            if (
                not isinstance(request_payload, dict)
                or not isinstance(session_binding, dict)
                or (native_request_payload is not None and not isinstance(native_request_payload, dict))
                or (
                    native_receipt_payload is not None
                    and not isinstance(native_receipt_payload, dict)
                )
                or (completion_echo is not None and not isinstance(completion_echo, dict))
                or canonical_json(request_payload) != str(row["request_payload_json"])
                or payload_sha256(request_payload) != str(row["request_payload_sha256"])
                or (
                    native_request_payload is not None
                    and payload_sha256(native_request_payload)
                    != str(row["native_request_payload_sha256"])
                )
                or (native_request_payload is None and row["native_request_payload_sha256"] is not None)
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
                    pre_native_unknown = (
                        status == "UNKNOWN"
                        and row["unknown_reason"]
                        in {
                            "session_pre_native_failure:post_stage_failure",
                            "session_pre_native_failure:queue_receipt_failure",
                            "session_pre_native_failure:queue_publish_failure",
                        }
                        and local_queue_receipt_queued is None
                    )
                    if not pre_native_unknown:
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
                native_request_payload=(
                    None
                    if native_request_payload is None
                    else MappingProxyType(native_request_payload)
                ),
                native_request_payload_sha256=(
                    None
                    if row["native_request_payload_sha256"] is None
                    else str(row["native_request_payload_sha256"])
                ),
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
        native_action_ref: Any = None,
        local_queue_receipt_id: str | None = None,
        cancel_target_projection: CtpOrderTargetProjectionHandle | None = None,
    ) -> CtpDispatchCommand:
        """Persist one immutable CTP command; this does not enable dispatch.

        SUBMIT binds the exact previously reserved intent/OrderRef. CANCEL
        additionally requires a fresh, same-store readback handle minted from
        verified native query evidence for the exact reserved order. A command
        can become claimable only after a per-day OrderRef seed proof and only
        while the account writer lease remains active.
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
            raise ContractValidationError("native ActionRef is allocated by the execution Store")
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
        if "OrderActionRef" in request_value:
            raise ContractValidationError("logical CTP request cannot supply native OrderActionRef")
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
        target_projection: CtpVerifiedOrderTargetProjection | None = None
        cancel_exchange_id: str | None = None
        cancel_order_sys_id: str | None = None
        cancel_front_id: int | None = None
        cancel_session_id: int | None = None
        if operation == "SUBMIT":
            if cancel_target_projection is not None:
                raise ContractValidationError("SUBMIT cannot carry a CTP cancel-target projection")
            if managed_intent_id is None or order_ref is None or cancel_target is not None:
                raise ContractValidationError("SUBMIT requires its reserved intent and OrderRef")
            self._validate_command_identifier(managed_intent_id, "managed_intent_id")
            if managed_action_id not in (None, managed_intent_id):
                raise ContractValidationError("submit action id must equal its managed intent id")
            managed_action_id = managed_intent_id
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
            if type(cancel_target_projection) is not CtpOrderTargetProjectionHandle:
                raise ContractValidationError("CANCEL requires a fresh verified CTP order target")
            if type(cancel_target_projection.projection) is not CtpVerifiedOrderTargetProjection:
                raise ContractValidationError("invalid typed CTP cancel target projection")
            target_projection = cancel_target_projection.projection

        with self._transaction() as cursor:
            target_check_now_ns = time.monotonic_ns()
            now_ns = time.time_ns()
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            if cursor.execute(
                "SELECT 1 FROM ctp_dispatch_callback_source_lifecycle_fences WHERE account_key = ?",
                (account_key,),
            ).fetchone() is not None:
                raise ContractValidationError("CTP account has a permanent callback lifecycle fence")
            session_owner = cursor.execute(
                "SELECT * FROM ctp_dispatch_callback_session_owners WHERE account_key = ?",
                (account_key,),
            ).fetchone()
            if session_owner is not None:
                owner_intent_id = str(session_owner["owner_intent_id"])
                active_binding = self._active_ctp_callback_sessions.get(owner_intent_id)
                if (
                    active_binding is None
                    or str(session_owner["owner_state"]) != "ACTIVE"
                    or session_value.get("binding_type") != "ctp_callback_session_binding.v1"
                    or session_value.get("owner_intent_id") != owner_intent_id
                    or active_binding.account_key != account_key
                    or active_binding.scope_key != scope_key
                    or active_binding.trading_day != trading_day
                    or session_digest != active_binding.session_binding_sha256
                    or session_json
                    != canonical_json(self._ctp_callback_session_binding_payload(active_binding))
                    or payload_sha256(
                        self._ctp_callback_session_binding_payload(active_binding)
                    )
                    != active_binding.session_binding_sha256
                ):
                    raise ContractValidationError(
                        "CTP staged session binding differs from active Store session"
                    )
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
                    SELECT managed_intent_id, runtime_order_id, created_at_ns
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
            if operation == "CANCEL":
                assert cancel_target_projection is not None and target_projection is not None
                target_reservation = CtpOrderIdentityReservation(
                    account_key=account_key,
                    trading_day=trading_day,
                    scope_key=scope_key,
                    managed_intent_id=reservation_managed_intent_id,
                    runtime_order_id=runtime_order_id,
                    order_ref=str(persisted_cancel_target),
                    created_at_ns=int(reservation["created_at_ns"]),
                )
                self._validate_ctp_order_target_projection_binding(
                    scope, target_reservation, target_projection
                )
                if (
                    target_projection.session_generation_id != session_generation_id
                    or target_projection.query_front_id != dispatch_front_id
                    or target_projection.query_session_id != dispatch_session_id
                    or request_value.get("InstrumentID") != target_projection.instrument_id
                    or target_projection.exchange_id != cancel_exchange_id
                    or target_projection.order_sys_id != cancel_order_sys_id
                    or target_projection.front_id != cancel_front_id
                    or target_projection.session_id != cancel_session_id
                ):
                    raise ContractValidationError(
                        "CTP cancel target differs from its verified order projection"
                    )
                target_projection = self._read_ctp_order_target_projection_row(
                    scope,
                    cancel_target_projection,
                    cursor=cursor,
                    now_ns=target_check_now_ns,
                )
            if operation == "CANCEL" and managed_action_id == reservation_managed_intent_id:
                raise ContractValidationError(
                    "cancel action id must be distinct from target intent"
                )
            action_order_ref = persisted_order_ref or persisted_cancel_target
            assert action_order_ref is not None
            existing = cursor.execute(
                "SELECT * FROM ctp_dispatch_commands WHERE account_key = ? AND command_id = ?",
                (account_key, command_id),
            ).fetchone()
            if existing is not None and int(existing["correlation_version"]) != 2:
                raise IntentConflictError("legacy CTP command cannot be restaged for native dispatch")
            if operation == "CANCEL":
                if existing is None:
                    native_action_ref = self._allocate_ctp_native_action_ref(
                        cursor,
                        account_key=account_key,
                        scope_key=scope_key,
                        command_id=command_id,
                        managed_action_id=str(managed_action_id),
                        allocated_at_ns=now_ns,
                    )
                else:
                    native_action_ref = int(existing["native_action_ref_int"])
            else:
                native_action_ref = None
            native_request_value = dict(request_value)
            if operation == "CANCEL":
                native_request_value["OrderActionRef"] = native_action_ref
            native_request_json = canonical_json(native_request_value)
            native_request_digest = payload_sha256(native_request_value)
            CtpDispatchCorrelationKey(
                version=2,
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
                native_request_payload_sha256=native_request_digest,
            )
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
                2,
                runtime_order_id,
                str(managed_action_id),
                str(session_generation_id),
                dispatch_front_id,
                dispatch_session_id,
                native_request_id,
                None,
                native_action_ref,
                native_request_json,
                native_request_digest,
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
                    if existing["native_action_ref_int"] is None
                    else int(existing["native_action_ref_int"]),
                    None
                    if existing["native_request_payload_json"] is None
                    else str(existing["native_request_payload_json"]),
                    None
                    if existing["native_request_payload_sha256"] is None
                    else str(existing["native_request_payload_sha256"]),
                    None
                    if existing["local_queue_receipt_id"] is None
                    else str(existing["local_queue_receipt_id"]),
                )
                if stored != immutable:
                    raise IntentConflictError("CTP command_id conflicts with staged command")
                command = self._ctp_dispatch_command_from_row(existing)
                assert command.correlation_key is not None
                self._require_ctp_native_action_ref_allocation(
                    cursor, existing, command.correlation_key
                )
                if operation == "CANCEL":
                    assert cancel_target_projection is not None
                    self._record_ctp_order_target_projection_consumption(
                        cursor,
                        account_key=account_key,
                        projection_id=cancel_target_projection.projection_id,
                        command_id=command_id,
                        consumed_at_ns=now_ns,
                    )
                    self._require_fresh_ctp_cancel_target_row(
                        cursor, scope, existing, now_ns=time.monotonic_ns()
                    )
                with self._lock:
                    self._issued_ctp_staged_commands[(account_key, command_id)] = command
                return command

            generation_owner = cursor.execute(
                """
                SELECT command_id, session_binding_sha256, dispatch_front_id, dispatch_session_id
                FROM ctp_dispatch_commands
                WHERE account_key = ? AND session_generation_id = ?
                  AND correlation_version IN (1, 2)
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
                  AND correlation_version IN (1, 2)
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
                  AND correlation_version IN (1, 2)
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
                    native_action_ref_int, native_request_payload_json,
                    native_request_payload_sha256,
                    local_queue_receipt_id,
                    status, created_at_ns, updated_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'READY', ?, ?)
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
                    2,
                    runtime_order_id,
                    str(managed_action_id),
                    str(session_generation_id),
                    dispatch_front_id,
                    dispatch_session_id,
                    native_request_id,
                    None,
                    native_action_ref,
                    native_request_json,
                    native_request_digest,
                    local_queue_receipt_id,
                    now_ns,
                    now_ns,
                ),
            )
            if operation == "CANCEL":
                cursor.execute(
                    """
                    INSERT INTO ctp_native_action_ref_allocations(
                        account_key, native_action_ref, scope_key, command_id,
                        managed_action_id, allocated_at_ns
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_key,
                        native_action_ref,
                        scope_key,
                        command_id,
                        str(managed_action_id),
                        now_ns,
                    ),
                )
            if operation == "CANCEL":
                assert cancel_target_projection is not None
                self._record_ctp_order_target_projection_consumption(
                    cursor,
                    account_key=account_key,
                    projection_id=cancel_target_projection.projection_id,
                    command_id=command_id,
                    consumed_at_ns=now_ns,
                )
            row = cursor.execute(
                "SELECT * FROM ctp_dispatch_commands WHERE account_key = ? AND command_id = ?",
                (account_key, command_id),
            ).fetchone()
            assert row is not None
            command = self._ctp_dispatch_command_from_row(row)
            if operation == "CANCEL":
                self._require_fresh_ctp_cancel_target_row(
                    cursor, scope, row, now_ns=time.monotonic_ns()
                )
            with self._lock:
                self._issued_ctp_staged_commands[(account_key, command_id)] = command
            return command

    def read_ctp_staged_command_for_session(
        self,
        scope: ExecutionScope,
        command_id: str,
        *,
        owner_handle: CtpCallbackSessionOwnerHandle,
        writer_lease: WriterLease,
    ) -> CtpDispatchCommand:
        """Issue the exact READY command identity for a pre-native failure.

        A caller cannot forge a command from readback JSON and pass it to the
        fail-before-native transition. This narrow read issues a same-Store
        object only while the exact active owner and writer lease still hold.
        Reading it grants no claim or provider authority.
        """

        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        self._validate_command_identifier(command_id, "command_id")
        with self._transaction() as cursor:
            now_ns = time.time_ns()
            self._assert_active_writer_lease(cursor, scope, writer_lease, now_ns=now_ns)
            owner = self._require_ctp_callback_session_owner_handle(
                cursor,
                scope,
                owner_handle,
                allowed_states=frozenset({"ACTIVE"}),
                writer_lease=writer_lease,
                now_ns=now_ns,
            )
            session = self._active_ctp_callback_sessions.get(owner_handle.owner_intent_id)
            if session is None or bool(owner["economic_query_observed"]):
                raise InvalidStateTransition("active CTP callback session is unavailable")
            row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, command_id),
            ).fetchone()
            if row is None or str(row["status"]) != "READY":
                raise InvalidStateTransition("CTP command is not staged before native claim")
            if (
                row["callback_owner_intent_id"]
                not in (None, owner_handle.owner_intent_id)
                or int(row["native_call_inflight"]) != 0
                or row["local_queue_receipt_queued"] == 0
            ):
                raise ContractValidationError("CTP staged command differs from active owner")
            self._require_command_session_binding(row, session)
            command = self._ctp_dispatch_command_from_row(row)
        with self._lock:
            self._issued_ctp_staged_commands[(account_key, command_id)] = command
        return command

    def fail_ctp_dispatch_command_before_native(
        self,
        scope: ExecutionScope,
        staged_command: CtpDispatchCommand,
        *,
        owner_handle: CtpCallbackSessionOwnerHandle,
        writer_lease: WriterLease,
        failure_code: str,
    ) -> CtpDispatchCommand:
        """Permanently fence a staged command after local handoff ambiguity.

        This API accepts only a same-Store issued command object, the exact
        active callback owner, and the active writer lease. It atomically marks
        a READY command UNKNOWN and poisons its owner. It cannot be used after
        a native claim, for a known local queue rejection, or to release its
        reserved OrderRef.
        """

        allowed_failure_codes = frozenset(
            {"post_stage_failure", "queue_receipt_failure", "queue_publish_failure"}
        )
        if type(staged_command) is not CtpDispatchCommand:
            raise ContractValidationError("Store-issued staged CTP command is required")
        if type(owner_handle) is not CtpCallbackSessionOwnerHandle:
            raise ContractValidationError("exact callback session owner is required")
        if type(failure_code) is not str or failure_code not in allowed_failure_codes:
            raise ContractValidationError("invalid pre-native CTP failure code")
        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        key = (account_key, staged_command.command_id)
        with self._lock:
            if self._issued_ctp_staged_commands.get(key) is not staged_command:
                raise ContractValidationError("CTP staged command is not fresh for this Store")

        with self._transaction() as cursor:
            now_ns = time.time_ns()
            self._assert_active_writer_lease(cursor, scope, writer_lease, now_ns=now_ns)
            owner = self._require_ctp_callback_session_owner_handle(
                cursor,
                scope,
                owner_handle,
                allowed_states=frozenset({"ACTIVE"}),
                writer_lease=writer_lease,
                now_ns=now_ns,
            )
            session = self._active_ctp_callback_sessions.get(owner_handle.owner_intent_id)
            row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, staged_command.command_id),
            ).fetchone()
            if row is None:
                raise ContractValidationError("staged CTP command is missing")
            current = self._ctp_dispatch_command_from_row(row)
            staged_payload_matches = (
                type(staged_command.request_payload) is dict
                and canonical_json(staged_command.request_payload)
                == canonical_json(dict(current.request_payload))
                and payload_sha256(staged_command.request_payload)
                == staged_command.request_payload_sha256
                and type(staged_command.session_binding) is dict
                and canonical_json(staged_command.session_binding)
                == canonical_json(dict(current.session_binding))
                and payload_sha256(staged_command.session_binding)
                == staged_command.session_binding_sha256
            )
            immutable_identity_matches = all(
                getattr(current, name) == getattr(staged_command, name)
                for name in (
                    "account_key",
                    "scope_key",
                    "trading_day",
                    "operation",
                    "command_id",
                    "request_payload_sha256",
                    "reservation_managed_intent_id",
                    "order_ref",
                    "cancel_target_order_ref",
                    "cancel_target_exchange_id",
                    "cancel_target_order_sys_id",
                    "cancel_target_front_id",
                    "cancel_target_session_id",
                    "approval_use_id",
                    "approval_digest",
                    "session_binding_sha256",
                    "correlation_key",
                    "local_queue_receipt_id",
                )
            )
            if (
                session is None
                or bool(owner["economic_query_observed"])
                or staged_command.status != "READY"
                or not staged_payload_matches
                or not immutable_identity_matches
                or str(row["status"]) != "READY"
                or row["callback_owner_intent_id"]
                not in (None, owner_handle.owner_intent_id)
                or int(row["native_call_inflight"]) != 0
                or row["local_queue_receipt_queued"] == 0
            ):
                raise InvalidStateTransition("CTP command is no longer pre-native and READY")
            self._require_command_session_binding(row, session)
            cursor.execute(
                """
                UPDATE ctp_dispatch_commands
                SET status = 'UNKNOWN', unknown_at_ns = ?, updated_at_ns = ?,
                    unknown_reason = ?
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                  AND status = 'READY' AND native_call_inflight = 0
                """,
                (
                    now_ns,
                    now_ns,
                    "session_pre_native_failure:" + failure_code,
                    account_key,
                    scope_key,
                    staged_command.command_id,
                ),
            )
            if cursor.rowcount != 1:
                raise InvalidStateTransition("CTP pre-native failure transition changed")
            cursor.execute(
                """
                UPDATE ctp_dispatch_callback_session_owners
                SET owner_state = 'POISONED', poison_code = 'dispatch_stage_ambiguous',
                    poison_observed_sequence = last_source_sequence, updated_at_ns = ?
                WHERE owner_intent_id = ? AND owner_state = 'ACTIVE'
                """,
                (now_ns, owner_handle.owner_intent_id),
            )
            if cursor.rowcount != 1:
                raise InvalidStateTransition("CTP owner poison transition changed")
            self._poison_ctp_account_family_owner(
                cursor,
                account_key=account_key,
                reason_code="dispatch_stage_ambiguous",
                now_ns=now_ns,
            )
            updated = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, staged_command.command_id),
            ).fetchone()
            assert updated is not None
            result = self._ctp_dispatch_command_from_row(updated)
        with self._lock:
            self._issued_ctp_staged_commands.pop(key, None)
        return result

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
        _callback_session_owner: CtpCallbackSessionOwnerHandle | None = None,
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
        # Authority providers are injected and may consult SDK-owned state.
        # Never invoke one while holding the SQLite transaction/Store lock:
        # callback ingress takes SDK lifecycle lock before synchronously
        # appending to this Store. The final transaction below re-reads and
        # compares the exact command and all mutable admission facts.
        verification_command = self.read_ctp_dispatch_command(scope, command_id)
        if verification_command is None or verification_command.status != "READY":
            if _callback_session_owner is not None:
                with self._lock:
                    owner_row = self._connection.execute(
                        "SELECT last_source_sequence FROM "
                        "ctp_dispatch_callback_session_owners WHERE account_key = ?",
                        (account_key,),
                    ).fetchone()
                    if owner_row is not None:
                        cursor = self._connection.cursor()
                        try:
                            applied = self._ctp_callback_applied_sequence(
                                cursor,
                                _callback_session_owner.owner_intent_id,
                                int(owner_row["last_source_sequence"]),
                            )
                        finally:
                            cursor.close()
                        if applied != int(owner_row["last_source_sequence"]):
                            raise InvalidStateTransition(
                                "CTP callback inbox has unapplied source events"
                            )
            return None
        if (
            verification_command.correlation_key is None
            or verification_command.correlation_key.version != 2
            or verification_command.native_request_payload is None
            or verification_command.native_request_payload_sha256 is None
        ):
            raise InvalidStateTransition("legacy CTP command is audit-only")
        # Avoid invoking an injected verifier for an account that already has
        # a permanent source-lifecycle fence. This is only a read-only early
        # rejection; the claim transaction repeats the fence check after the
        # verifier to close the race without holding Store locks across it.
        with self._lock:
            existing_source_fence = self._connection.execute(
                """
                SELECT command_id FROM ctp_dispatch_callback_source_lifecycle_fences
                WHERE account_key = ? ORDER BY created_at_ns, command_id LIMIT 1
                """,
                (account_key,),
            ).fetchone()
        if existing_source_fence is not None:
            raise InvalidStateTransition(
                "account has CTP callback source lifecycle fence: "
                + str(existing_source_fence["command_id"])
            )
        verification_started_ns = time.time_ns()
        authority = self._verify_ctp_dispatch_authority(
            authority_verifier,
            verification_command,
            now_ns=verification_started_ns,
        )
        with self._transaction() as cursor:
            target_check_started_ns = time.monotonic_ns()
            self._assert_active_writer_lease(
                cursor, scope, writer_lease, now_ns=verification_started_ns
            )
            session_owner = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_callback_session_owners
                WHERE account_key = ?
                """,
                (account_key,),
            ).fetchone()
            if session_owner is None:
                if _callback_session_owner is not None:
                    raise ContractValidationError(
                        "CTP callback session owner is not persisted for this account"
                    )
            else:
                if _callback_session_owner is None:
                    raise InvalidStateTransition(
                        "ordinary CTP claim is blocked by the callback session owner"
                    )
                self._require_ctp_callback_session_owner_handle(
                    cursor,
                    scope,
                    _callback_session_owner,
                    allowed_states=frozenset({"ACTIVE"}),
                    writer_lease=writer_lease,
                    now_ns=verification_started_ns,
                )
                active_binding = self._active_ctp_callback_sessions.get(
                    _callback_session_owner.owner_intent_id
                )
                if (
                    active_binding is None
                    or active_binding.account_key != account_key
                    or active_binding.scope_key != scope_key
                    or active_binding.trading_day != trading_day
                    or active_binding.session_generation_id
                    != self._ctp_callback_session_generation_id(session_owner)
                ):
                    raise InvalidStateTransition(
                        "active CTP callback session binding is not live in this Store"
                    )
                if bool(session_owner["economic_query_observed"]):
                    raise InvalidStateTransition(
                        "CTP economic query results require a typed evaluator before send"
                    )
                if self._ctp_callback_applied_sequence(
                    cursor,
                    _callback_session_owner.owner_intent_id,
                    int(session_owner["last_source_sequence"]),
                ) != int(session_owner["last_source_sequence"]):
                    raise InvalidStateTransition(
                        "CTP callback inbox has unapplied source events"
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
            if int(row["correlation_version"]) != 2:
                raise InvalidStateTransition("legacy CTP command version is audit-only")
            if session_owner is not None:
                if (
                    str(session_owner["scope_key"]) != scope_key
                    or str(row["trading_day"]) != str(session_owner["trading_day"])
                    or str(row["session_generation_id"])
                    != self._ctp_callback_session_generation_id(session_owner)
                    or int(row["dispatch_front_id"]) != int(session_owner["front_id"])
                    or int(row["dispatch_session_id"]) != int(session_owner["session_id"])
                ):
                    raise ContractValidationError(
                        "CTP command session differs from active callback owner"
                    )
                assert active_binding is not None
                self._require_command_session_binding(row, active_binding)
                if row["local_queue_receipt_id"] is None or row[
                    "local_queue_receipt_queued"
                ] != 1:
                    raise InvalidStateTransition(
                        "callback-owner claim requires a committed queued receipt"
                    )
            if row["local_queue_receipt_id"] is not None and row[
                "local_queue_receipt_queued"
            ] != 1:
                return None
            if required_local_queue_receipt_id is not None and (
                row["local_queue_receipt_id"] != required_local_queue_receipt_id
                or row["local_queue_receipt_queued"] != 1
            ):
                return None
            if str(row["operation"]) == "CANCEL":
                self._require_fresh_ctp_cancel_target_row(
                    cursor, scope, row, now_ns=target_check_started_ns
                )
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
            if str(row["operation"]) == "SUBMIT" and self._ctp_has_pending_cancel_postcondition(
                cursor, account_key
            ):
                raise InvalidStateTransition(
                    "account has a cancel awaiting verified terminal-order evidence"
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
            if command.correlation_key is None or command != verification_command:
                raise ContractValidationError(
                    "CTP command changed while authority was verified"
                )
            self._require_ctp_native_action_ref_allocation(cursor, row, command.correlation_key)
            claim_now_ns = time.time_ns()
            self._assert_active_writer_lease(cursor, scope, writer_lease, now_ns=claim_now_ns)
            if authority.expires_at_ns <= claim_now_ns:
                raise ContractValidationError("CTP dispatch authority is expired")
            if claim_now_ns < authority.verified_at_ns:
                raise ContractValidationError("CTP dispatch authority clock moved backwards")
            row_after_verification = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, command_id),
            ).fetchone()
            if row_after_verification is None or str(row_after_verification["status"]) != "READY":
                return None
            if session_owner is not None:
                fresh_owner = cursor.execute(
                    "SELECT * FROM ctp_dispatch_callback_session_owners WHERE account_key = ?",
                    (account_key,),
                ).fetchone()
                fresh_binding = self._active_ctp_callback_sessions.get(
                    _callback_session_owner.owner_intent_id
                )
                if (
                    fresh_owner is None
                    or str(fresh_owner["owner_state"]) != "ACTIVE"
                    or bool(fresh_owner["economic_query_observed"])
                    or fresh_binding is None
                    or fresh_binding != active_binding
                    or str(fresh_owner["owner_intent_id"])
                    != _callback_session_owner.owner_intent_id
                ):
                    raise InvalidStateTransition(
                        "active CTP callback session changed during claim verification"
                    )
                self._require_command_session_binding(row_after_verification, fresh_binding)
            if str(row_after_verification["operation"]) == "CANCEL":
                fresh_target = self._require_fresh_ctp_cancel_target_row(
                    cursor,
                    scope,
                    row_after_verification,
                    now_ns=time.monotonic_ns(),
                )
                self._require_ctp_cancel_target_not_terminal(
                    cursor, account_key, fresh_target
                )
            elif self._unresolved_cancellations_for_account_cursor(
                cursor,
                account_key,
                ctp_family_key=self._ctp_account_family_key(scope),
            ):
                raise InvalidStateTransition(
                    "account has an unresolved managed cancellation"
                )
            cursor.execute(
                """
                UPDATE ctp_dispatch_commands
                SET status = 'CLAIMED', updated_at_ns = ?, claimed_at_ns = ?,
                    claimed_owner_id = ?, claimed_fencing_token = ?,
                    callback_owner_intent_id = ?, native_call_inflight = ?
                WHERE account_key = ? AND scope_key = ? AND command_id = ? AND status = 'READY'
                """,
                (
                    claim_now_ns,
                    claim_now_ns,
                    writer_lease.owner_id,
                    writer_lease.fencing_token,
                    None
                    if session_owner is None
                    else _callback_session_owner.owner_intent_id,
                    0 if session_owner is None else 1,
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
            if str(claimed["operation"]) == "CANCEL":
                self._insert_ctp_cancel_postcondition_for_claim(
                    cursor,
                    scope,
                    claimed,
                    owner_intent_id=(
                        None
                        if session_owner is None
                        else _callback_session_owner.owner_intent_id
                    ),
                    created_at_ns=claim_now_ns,
                )
            return self._ctp_dispatch_command_from_row(claimed)

    def claim_ctp_dispatch_command_for_session(
        self,
        scope: ExecutionScope,
        command_id: str,
        *,
        owner_handle: CtpCallbackSessionOwnerHandle,
        writer_lease: WriterLease,
        authority_verifier: CtpDispatchAuthorityVerifier,
        required_local_queue_receipt_id: str,
        binding_ttl_ns: int = 5_000_000_000,
    ) -> CtpSessionNativeCallClaim | None:
        """Claim one queued command under the exact active inbox owner.

        The READY-to-CLAIMED transition and ``native_call_inflight`` marker are
        committed by the ordinary claim transaction before this method issues
        its ephemeral binding. If binding creation or verifier handoff is
        interrupted, the durable command remains non-retryable.
        """

        if type(binding_ttl_ns) is not int or not 0 < binding_ttl_ns <= 30_000_000_000:
            raise ContractValidationError("invalid CTP native-call binding lifetime")
        account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
        if type(owner_handle) is not CtpCallbackSessionOwnerHandle:
            raise ContractValidationError("typed CTP callback owner is required")
        if self._active_ctp_callback_sessions.get(owner_handle.owner_intent_id) is None:
            raise InvalidStateTransition("active CTP callback session binding is unavailable")
        command = self.claim_ctp_dispatch_command(
            scope,
            command_id,
            writer_lease=writer_lease,
            authority_verifier=authority_verifier,
            required_local_queue_receipt_id=required_local_queue_receipt_id,
            _callback_session_owner=owner_handle,
        )
        if command is None:
            return None
        try:
            correlation = command.correlation_key
            if correlation is None:
                raise ContractValidationError("claimed CTP command lacks correlation")
            session = self._active_ctp_callback_sessions.get(owner_handle.owner_intent_id)
            if session is None:
                raise InvalidStateTransition("active CTP callback session binding was lost")
            if (
                command.account_key != account_key
                or command.scope_key != scope_key
                or command.trading_day != session.trading_day
                or correlation.session_generation_id != session.session_generation_id
                or correlation.dispatch_front_id != session.dispatch_front_id
                or correlation.dispatch_session_id != session.dispatch_session_id
            ):
                raise ContractValidationError("claimed CTP command differs from source session")
            if (
                correlation.version != 2
                or command.native_request_payload is None
                or command.native_request_payload_sha256 is None
            ):
                raise ContractValidationError("claimed command lacks V2 native request payload")
            binding = CtpManagedNativeCallBindingV2(
                binding_id=uuid.uuid4().hex,
                owner_intent_id=owner_handle.owner_intent_id,
                account_key=command.account_key,
                scope_key=command.scope_key,
                command_id=command.command_id,
                operation=command.operation,
                trading_day=command.trading_day,
                request_payload_json=canonical_json(dict(command.request_payload)),
                request_payload_sha256=command.request_payload_sha256,
                reservation_managed_intent_id=command.reservation_managed_intent_id,
                managed_action_id=correlation.managed_action_id,
                runtime_order_id=correlation.runtime_order_id,
                order_ref=correlation.order_ref,
                native_request_id=correlation.native_request_id,
                native_action_ref=correlation.native_action_ref,
                native_request_payload_json=canonical_json(
                    dict(command.native_request_payload)
                ),
                native_request_payload_sha256=command.native_request_payload_sha256,
                cancel_target_order_ref=(
                    command.cancel_target_order_ref
                    if command.operation == "CANCEL"
                    else None
                ),
                cancel_target_exchange_id=command.cancel_target_exchange_id,
                cancel_target_order_sys_id=command.cancel_target_order_sys_id,
                cancel_target_front_id=command.cancel_target_front_id,
                cancel_target_session_id=command.cancel_target_session_id,
                session_binding_sha256=command.session_binding_sha256,
                session_generation_id=correlation.session_generation_id,
                dispatch_front_id=correlation.dispatch_front_id,
                dispatch_session_id=correlation.dispatch_session_id,
                writer_owner_id=writer_lease.owner_id,
                writer_fencing_token=writer_lease.fencing_token,
                expires_at_ns=time.time_ns() + binding_ttl_ns,
            )
            with self._lock:
                self._issued_ctp_native_call_bindings[binding.binding_id] = binding
            return CtpSessionNativeCallClaim(command=command, binding=binding)
        except Exception:
            with suppress(Exception):
                self.poison_ctp_callback_session_owner(
                    owner_handle, "owner_binding_mismatch"
                )
            raise

    def verify_ctp_managed_native_call_binding(
        self,
        scope: ExecutionScope,
        owner_handle: CtpCallbackSessionOwnerHandle,
        binding: CtpManagedNativeCallBindingV2,
        *,
        writer_lease: WriterLease,
    ) -> CtpManagedNativeCallBindingV2:
        """Consume once and recheck the exact binding under Store transaction.

        The SDK calls this while holding its lifecycle lock, then invokes the
        pinned API after releasing every SDK and Store lock. This method never
        acquires an SDK lock.
        """

        if type(owner_handle) is not CtpCallbackSessionOwnerHandle:
            raise ContractValidationError("exact callback session owner is required")
        if type(binding) is not CtpManagedNativeCallBindingV2:
            with suppress(Exception):
                self.poison_ctp_callback_session_owner(owner_handle, "owner_binding_mismatch")
            raise ContractValidationError("Store-issued CTP native-call binding is required")
        with self._lock:
            issued = self._issued_ctp_native_call_bindings.get(binding.binding_id)
            if issued is not binding or binding.binding_id in self._consumed_ctp_native_call_bindings:
                with suppress(Exception):
                    self.poison_ctp_callback_session_owner(
                        owner_handle, "owner_binding_mismatch"
                    )
                raise ContractValidationError("CTP native-call binding is not fresh")
            # Consume before the DB read. Any lock, lease, or commit ambiguity
            # must leave this object unusable in this process.
            self._consumed_ctp_native_call_bindings.add(binding.binding_id)
            try:
                account_key, _, scope_key = self._validate_ctp_order_identity_scope(scope)
                now_ns = time.time_ns()
                with self._transaction() as cursor:
                    self._assert_active_writer_lease(cursor, scope, writer_lease, now_ns=now_ns)
                    owner = self._require_ctp_callback_session_owner_handle(
                        cursor,
                        scope,
                        owner_handle,
                        allowed_states=frozenset({"ACTIVE"}),
                        writer_lease=writer_lease,
                        now_ns=now_ns,
                    )
                    session = self._active_ctp_callback_sessions.get(
                        owner_handle.owner_intent_id
                    )
                    if (
                        session is None
                        or bool(owner["economic_query_observed"])
                        or binding.expires_at_ns <= now_ns
                        or binding.owner_intent_id != owner_handle.owner_intent_id
                        or binding.account_key != account_key
                        or binding.scope_key != scope_key
                        or binding.trading_day != str(owner["trading_day"])
                        or binding.session_generation_id
                        != self._ctp_callback_session_generation_id(owner)
                        or binding.dispatch_front_id != int(owner["front_id"])
                        or binding.dispatch_session_id != int(owner["session_id"])
                        or binding.session_binding_sha256 != session.session_binding_sha256
                        or binding.writer_owner_id != writer_lease.owner_id
                        or binding.writer_fencing_token != writer_lease.fencing_token
                    ):
                        raise ContractValidationError("CTP native-call binding is stale")
                    row = cursor.execute(
                        """
                        SELECT * FROM ctp_dispatch_commands
                        WHERE account_key = ? AND scope_key = ? AND command_id = ?
                        """,
                        (account_key, scope_key, binding.command_id),
                    ).fetchone()
                    if row is None or str(row["status"]) != "CLAIMED":
                        raise InvalidStateTransition("bound CTP command is no longer CLAIMED")
                    command = self._ctp_dispatch_command_from_row(row)
                    correlation = command.correlation_key
                    if (
                        correlation is None
                        or correlation.version != 2
                        or command.native_request_payload is None
                        or command.native_request_payload_sha256 is None
                    ):
                        raise ContractValidationError("bound CTP command lacks correlation")
                    self._require_ctp_native_action_ref_allocation(cursor, row, correlation)
                    expected_payload = canonical_json(dict(command.request_payload))
                    expected_native_payload = canonical_json(
                        dict(command.native_request_payload)
                    )
                    if (
                        row["callback_owner_intent_id"] != owner_handle.owner_intent_id
                        or int(row["native_call_inflight"]) != 1
                        or row["local_queue_receipt_id"] is None
                        or row["local_queue_receipt_queued"] != 1
                        or str(row["claimed_owner_id"]) != writer_lease.owner_id
                        or int(row["claimed_fencing_token"]) != writer_lease.fencing_token
                        or binding.operation != command.operation
                        or binding.trading_day != command.trading_day
                        or binding.request_payload_json != expected_payload
                        or binding.request_payload_sha256 != command.request_payload_sha256
                        or binding.native_request_payload_json != expected_native_payload
                        or binding.native_request_payload_sha256
                        != command.native_request_payload_sha256
                        or binding.reservation_managed_intent_id
                        != command.reservation_managed_intent_id
                        or binding.managed_action_id != correlation.managed_action_id
                        or binding.runtime_order_id != correlation.runtime_order_id
                        or binding.order_ref != correlation.order_ref
                        or binding.native_request_id != correlation.native_request_id
                        or binding.native_action_ref != correlation.native_action_ref
                        or binding.cancel_target_order_ref
                        != (command.cancel_target_order_ref if command.operation == "CANCEL" else None)
                        or binding.cancel_target_exchange_id != command.cancel_target_exchange_id
                        or binding.cancel_target_order_sys_id != command.cancel_target_order_sys_id
                        or binding.cancel_target_front_id != command.cancel_target_front_id
                        or binding.cancel_target_session_id != command.cancel_target_session_id
                        or binding.session_binding_sha256 != command.session_binding_sha256
                        or binding.session_generation_id != correlation.session_generation_id
                        or binding.dispatch_front_id != correlation.dispatch_front_id
                        or binding.dispatch_session_id != correlation.dispatch_session_id
                    ):
                        raise ContractValidationError("CTP native-call binding differs from command")
                    self._require_command_session_binding(row, session)
                    if command.operation == "CANCEL":
                        fresh_target = self._require_fresh_ctp_cancel_target_row(
                            cursor, scope, row, now_ns=time.monotonic_ns()
                        )
                        self._require_ctp_cancel_target_not_terminal(
                            cursor, account_key, fresh_target
                        )
                return binding
            except Exception as error:
                with suppress(Exception):
                    self.poison_ctp_callback_session_owner(
                        owner_handle, "owner_binding_mismatch"
                    )
                if isinstance(error, ContractValidationError):
                    raise
                raise ContractValidationError("CTP native-call binding verification failed") from None

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
        _callback_session_owner: CtpCallbackSessionOwnerHandle | None = None,
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
            callback_owner_intent_id = row["callback_owner_intent_id"]
            owner_state = None
            if callback_owner_intent_id is None:
                if _callback_session_owner is not None:
                    raise ContractValidationError(
                        "receipt owner was not bound to this CTP command"
                    )
            else:
                if (
                    _callback_session_owner is None
                    or _callback_session_owner.owner_intent_id
                    != str(callback_owner_intent_id)
                ):
                    raise InvalidStateTransition(
                        "ordinary receipt completion is blocked for session-owned command"
                    )
                owner_row = self._require_ctp_callback_session_owner_handle(
                    cursor,
                    scope,
                    _callback_session_owner,
                    allowed_states=frozenset({"ACTIVE", "POISONED"}),
                    writer_lease=writer_lease,
                    now_ns=now_ns,
                )
                owner_state = str(owner_row["owner_state"])
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
            if callback_owner_intent_id is not None and int(row["native_call_inflight"]) != 1:
                raise InvalidStateTransition("session-owned native call is not in flight")
            if owner_state == "POISONED" and receipt.outcome != "UNKNOWN":
                raise InvalidStateTransition(
                    "poisoned CTP callback owner requires an UNKNOWN native receipt"
                )
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
                        completion_echo_sha256 = ?, native_call_inflight = 0
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
                        completion_echo_json = ?, completion_echo_sha256 = ?,
                        native_call_inflight = 0
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

    def complete_ctp_dispatch_command_for_session(
        self,
        scope: ExecutionScope,
        receipt: CtpDispatchReceipt,
        *,
        owner_handle: CtpCallbackSessionOwnerHandle,
        writer_lease: WriterLease,
    ) -> CtpDispatchCommand:
        """Persist the native request receipt for an owner-bound claim."""

        if (
            type(owner_handle) is not CtpCallbackSessionOwnerHandle
            or self._issued_ctp_callback_session_owners.get(owner_handle.owner_intent_id)
            is not owner_handle
        ):
            raise ContractValidationError("exact same-Store CTP callback owner is required")
        return self.complete_ctp_dispatch_command(
            scope,
            receipt,
            writer_lease=writer_lease,
            _callback_session_owner=owner_handle,
        )

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
                OR EXISTS (
                    SELECT 1 FROM ctp_dispatch_cancel_postconditions AS postcondition
                    WHERE postcondition.account_key = ctp_dispatch_commands.account_key
                      AND NOT EXISTS (
                          SELECT 1 FROM ctp_dispatch_cancel_postcondition_resolutions AS resolution
                          WHERE resolution.account_key = postcondition.account_key
                            AND resolution.cancel_command_id = postcondition.cancel_command_id
                      )
                )
                OR EXISTS (
                    SELECT 1 FROM ctp_dispatch_callback_session_owners AS owner
                    WHERE owner.account_key = ctp_dispatch_commands.account_key
                      AND owner.owner_state = 'POISONED'
                )
            )
            LIMIT 1
            """,
            (account_key,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _require_ctp_cancel_target_not_terminal(
        cursor: sqlite3.Cursor, account_key: str, target: CtpVerifiedOrderTargetProjection
    ) -> None:
        row = cursor.execute(
            """
            SELECT provider_state, terminal FROM ctp_dispatch_order_projection
            WHERE account_key = ? AND runtime_order_id = ?
            """,
            (account_key, target.runtime_order_id),
        ).fetchone()
        if row is not None and (int(row["terminal"]) == 1 or str(row["provider_state"]) in _CTP_ORDER_TERMINAL_STATES):
            raise InvalidStateTransition("CTP cancel target is already terminal")

    @staticmethod
    def _ctp_has_pending_cancel_postcondition(cursor: sqlite3.Cursor, account_key: str) -> bool:
        return cursor.execute(
            """
            SELECT 1 FROM ctp_dispatch_cancel_postconditions AS postcondition
            WHERE postcondition.account_key = ?
              AND NOT EXISTS (
                  SELECT 1 FROM ctp_dispatch_cancel_postcondition_resolutions AS resolution
                  WHERE resolution.account_key = postcondition.account_key
                    AND resolution.cancel_command_id = postcondition.cancel_command_id
              )
            LIMIT 1
            """,
            (account_key,),
        ).fetchone() is not None

    def _insert_ctp_cancel_postcondition_for_claim(
        self,
        cursor: sqlite3.Cursor,
        scope: ExecutionScope,
        cancel_row: sqlite3.Row,
        *,
        owner_intent_id: str | None,
        created_at_ns: int,
    ) -> None:
        """Record one exact cancel obligation in the READY→CLAIMED transaction."""

        command = self._ctp_dispatch_command_from_row(cancel_row)
        correlation = command.correlation_key
        if command.operation != "CANCEL" or correlation is None or correlation.version != 2:
            raise ContractValidationError("CTP cancel claim lacks exact V2 identity")
        target = self._require_fresh_ctp_cancel_target_row(
            cursor, scope, cancel_row, now_ns=time.monotonic_ns()
        )
        submit_rows = cursor.execute(
            """
            SELECT * FROM ctp_dispatch_commands
            WHERE account_key = ? AND operation = 'SUBMIT'
              AND runtime_order_id = ? AND order_ref = ?
              AND trading_day = ? AND session_generation_id = ?
              AND dispatch_front_id = ? AND dispatch_session_id = ?
              AND callback_owner_intent_id = ?
              AND correlation_version = 2
              AND status IN ('COMPLETED', 'UNKNOWN')
            ORDER BY created_at_ns, command_id
            """,
            (
                command.account_key,
                target.runtime_order_id,
                target.order_ref,
                command.trading_day,
                correlation.session_generation_id,
                correlation.dispatch_front_id,
                correlation.dispatch_session_id,
                owner_intent_id,
            ),
        ).fetchall()
        if (
            owner_intent_id is None
            or len(submit_rows) != 1
            or target.exchange_id != command.cancel_target_exchange_id
            or target.order_sys_id != command.cancel_target_order_sys_id
            or target.front_id != command.cancel_target_front_id
            or target.session_id != command.cancel_target_session_id
        ):
            # A V2 cancel without a uniquely linked source-owned submit is not
            # safe to release with a callback. Preserve it as an unmappable
            # account obligation rather than manufacturing a relation.
            cursor.execute(
                """
                INSERT INTO ctp_dispatch_cancel_postconditions(
                    account_key, cancel_command_id, scope_key, trading_day,
                    identity_state, correlation_key_sha256, session_binding_sha256,
                    created_at_ns
                ) VALUES (?, ?, ?, ?, 'UNMAPPABLE', ?, ?, ?)
                """,
                (
                    command.account_key,
                    command.command_id,
                    command.scope_key,
                    command.trading_day,
                    payload_sha256(correlation.to_payload()),
                    command.session_binding_sha256,
                    created_at_ns,
                ),
            )
            return

        submit_row = submit_rows[0]
        submit_command = self._ctp_dispatch_command_from_row(submit_row)
        submit_correlation = submit_command.correlation_key
        if (
            submit_correlation is None
            or submit_correlation.version != 2
            or submit_correlation.runtime_order_id != target.runtime_order_id
            or submit_correlation.order_ref != target.order_ref
            or submit_command.session_binding_sha256 != command.session_binding_sha256
        ):
            raise ContractValidationError("CTP cancel target submit binding is inconsistent")
        cursor.execute(
            """
            INSERT INTO ctp_dispatch_cancel_postconditions(
                account_key, cancel_command_id, scope_key, trading_day,
                target_submit_command_id, runtime_order_id, target_order_ref,
                target_exchange_id, target_order_sys_id, target_front_id,
                target_session_id, session_generation_id, dispatch_front_id,
                dispatch_session_id, native_request_id, target_native_request_id,
                native_action_ref_int,
                owner_intent_id, identity_state, correlation_key_sha256,
                session_binding_sha256, created_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'EXACT', ?, ?, ?)
            """,
            (
                command.account_key,
                command.command_id,
                command.scope_key,
                command.trading_day,
                submit_command.command_id,
                target.runtime_order_id,
                target.order_ref,
                target.exchange_id,
                target.order_sys_id,
                target.front_id,
                target.session_id,
                correlation.session_generation_id,
                correlation.dispatch_front_id,
                correlation.dispatch_session_id,
                correlation.native_request_id,
                submit_correlation.native_request_id,
                correlation.native_action_ref,
                owner_intent_id,
                payload_sha256(correlation.to_payload()),
                command.session_binding_sha256,
                created_at_ns,
            ),
        )

    def _record_ctp_cancel_terminal_observations(
        self,
        cursor: sqlite3.Cursor,
        command: CtpDispatchCommand,
        event: CtpCallbackIngressEventV1,
        callback: CtpDispatchCallbackKey,
        callback_payload: Mapping[str, Any],
        evidence: CtpVerifiedCallbackEvidence,
        *,
        applied_at_ns: int,
    ) -> None:
        if (
            event.callback_name != "OnRtnOrder"
            or command.operation != "SUBMIT"
        ):
            return
        is_terminal = evidence.projection_state in _CTP_ORDER_TERMINAL_STATES
        native_fields = callback_payload.get("native_fields")
        if not isinstance(native_fields, Mapping):
            raise ContractValidationError("verified terminal order lacks native fields")
        required = (
            "RequestID", "OrderRef", "ExchangeID", "OrderSysID", "FrontID",
            "SessionID", "TradingDay", "VolumeTraded",
        )
        if any(name not in native_fields for name in required):
            raise ContractValidationError("verified terminal order identity is incomplete")
        correlation = command.correlation_key
        if correlation is None or correlation.version != 2:
            raise ContractValidationError("terminal order command lacks V2 correlation")
        if (
            type(native_fields["RequestID"]) is not int
            or native_fields["RequestID"] != correlation.native_request_id
            or native_fields["OrderRef"] != correlation.order_ref
            or type(native_fields["FrontID"]) is not int
            or native_fields["FrontID"] != correlation.dispatch_front_id
            or type(native_fields["SessionID"]) is not int
            or native_fields["SessionID"] != correlation.dispatch_session_id
            or native_fields["TradingDay"] != command.trading_day
            or type(native_fields["VolumeTraded"]) is not int
            or native_fields["VolumeTraded"] < 0
        ):
            raise ContractValidationError("verified terminal order differs from submit identity")
        owner_intent_id = event.owner_handle.owner_intent_id
        order_cumulative = cursor.execute(
            """
            SELECT native_volume_traded, trade_volume_at_prefix
            FROM ctp_dispatch_order_cumulative_ledger
            WHERE owner_intent_id = ? AND source_sequence = ? AND command_id = ?
            """,
            (owner_intent_id, event.source_sequence, command.command_id),
        ).fetchone()
        if (
            order_cumulative is None
            or int(order_cumulative["native_volume_traded"]) != native_fields["VolumeTraded"]
        ):
            raise ContractValidationError("terminal order cumulative evidence is unavailable")

        obligations = cursor.execute(
            """
            SELECT postcondition.* FROM ctp_dispatch_cancel_postconditions AS postcondition
            WHERE postcondition.account_key = ?
              AND postcondition.target_submit_command_id = ?
              AND postcondition.identity_state = 'EXACT'
              AND postcondition.owner_intent_id = ?
              AND NOT EXISTS (
                  SELECT 1 FROM ctp_dispatch_cancel_postcondition_resolutions AS resolution
                  WHERE resolution.account_key = postcondition.account_key
                    AND resolution.cancel_command_id = postcondition.cancel_command_id
              )
            ORDER BY postcondition.cancel_command_id
            """,
            (command.account_key, command.command_id, owner_intent_id),
        ).fetchall() if is_terminal else ()
        identity = {
            "account_key": command.account_key,
            "trading_day": command.trading_day,
            "owner_intent_id": owner_intent_id,
            "session_generation_id": correlation.session_generation_id,
            "dispatch_front_id": correlation.dispatch_front_id,
            "dispatch_session_id": correlation.dispatch_session_id,
            "native_request_id": correlation.native_request_id,
            "order_ref": correlation.order_ref,
            "exchange_id": native_fields["ExchangeID"],
            "order_sys_id": native_fields["OrderSysID"],
            "front_id": native_fields["FrontID"],
            "session_id": native_fields["SessionID"],
        }
        identity_digest = payload_sha256(identity)
        for obligation in obligations:
            if (
                str(obligation["runtime_order_id"]) != correlation.runtime_order_id
                or str(obligation["target_order_ref"]) != str(native_fields["OrderRef"])
                or str(obligation["target_exchange_id"]) != str(native_fields["ExchangeID"])
                or str(obligation["target_order_sys_id"]) != str(native_fields["OrderSysID"])
                or int(obligation["target_front_id"]) != native_fields["FrontID"]
                or int(obligation["target_session_id"]) != native_fields["SessionID"]
                or str(obligation["session_generation_id"]) != correlation.session_generation_id
                or int(obligation["dispatch_front_id"]) != correlation.dispatch_front_id
                or int(obligation["dispatch_session_id"]) != correlation.dispatch_session_id
                or int(obligation["target_native_request_id"])
                != correlation.native_request_id
                or str(obligation["owner_intent_id"]) != owner_intent_id
            ):
                raise ContractValidationError("terminal order does not match cancel obligation")
            cursor.execute(
                """
                INSERT OR IGNORE INTO ctp_dispatch_cancel_terminal_observations(
                    account_key, cancel_command_id, owner_intent_id, source_sequence,
                    submit_command_id, terminal_state, order_volume_traded,
                    trade_volume_at_prefix, callback_key_sha256,
                    ingress_record_digest_sha256, source_digest_sha256,
                    identity_sha256, observed_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    command.account_key,
                    str(obligation["cancel_command_id"]),
                    owner_intent_id,
                    event.source_sequence,
                    command.command_id,
                    evidence.projection_state,
                    int(native_fields["VolumeTraded"]),
                    int(order_cumulative["trade_volume_at_prefix"]),
                    payload_sha256(callback.to_payload()),
                    event.record_digest_sha256,
                    evidence.source_digest_sha256,
                    identity_digest,
                    applied_at_ns,
                ),
            )
            self._require_same_ctp_cancel_terminal_observation(
                cursor,
                command.account_key,
                str(obligation["cancel_command_id"]),
                owner_intent_id,
                event.source_sequence,
                evidence.projection_state,
                int(native_fields["VolumeTraded"]),
                event.record_digest_sha256,
                identity_digest,
            )
        self._reconcile_ctp_cancel_postconditions(
            cursor,
            command.account_key,
            command.command_id,
            owner_intent_id,
            event.source_sequence,
            event.record_digest_sha256,
            applied_at_ns=applied_at_ns,
        )

    @staticmethod
    def _require_same_ctp_cancel_terminal_observation(
        cursor: sqlite3.Cursor,
        account_key: str,
        cancel_command_id: str,
        owner_intent_id: str,
        source_sequence: int,
        terminal_state: str,
        order_volume_traded: int,
        record_digest: str,
        identity_digest: str,
    ) -> None:
        row = cursor.execute(
            """
            SELECT terminal_state, order_volume_traded,
                   ingress_record_digest_sha256, identity_sha256
            FROM ctp_dispatch_cancel_terminal_observations
            WHERE account_key = ? AND cancel_command_id = ?
              AND owner_intent_id = ? AND source_sequence = ?
            """,
            (account_key, cancel_command_id, owner_intent_id, source_sequence),
        ).fetchone()
        if row is None or (
            str(row["terminal_state"]) != terminal_state
            or int(row["order_volume_traded"]) != order_volume_traded
            or str(row["ingress_record_digest_sha256"]) != record_digest
            or str(row["identity_sha256"]) != identity_digest
        ):
            raise IntentConflictError("CTP terminal order observation changed")

    @staticmethod
    def _poison_if_ctp_cancel_resolution_contradicted(
        cursor: sqlite3.Cursor,
        account_key: str,
        submit_command_id: str,
        owner_intent_id: str,
        through_sequence: int,
    ) -> None:
        resolutions = cursor.execute(
            """
            SELECT resolution.cancel_command_id, resolution.order_volume_traded,
                   resolution.trade_volume, postcondition.target_submit_command_id
            FROM ctp_dispatch_cancel_postcondition_resolutions AS resolution
            JOIN ctp_dispatch_cancel_postconditions AS postcondition
              ON postcondition.account_key = resolution.account_key
             AND postcondition.cancel_command_id = resolution.cancel_command_id
            WHERE resolution.account_key = ?
              AND postcondition.target_submit_command_id = ?
            """,
            (account_key, submit_command_id),
        ).fetchall()
        for resolution in resolutions:
            order_row = cursor.execute(
                """
                SELECT native_volume_traded FROM ctp_dispatch_order_cumulative_ledger
                WHERE account_key = ? AND command_id = ? AND owner_intent_id = ?
                  AND source_sequence <= ?
                ORDER BY source_sequence DESC LIMIT 1
                """,
                (account_key, submit_command_id, owner_intent_id, through_sequence),
            ).fetchone()
            trades = cursor.execute(
                """
                SELECT COALESCE(SUM(trade_volume), 0) AS total
                FROM ctp_dispatch_trade_fact_ledger
                WHERE account_key = ? AND command_id = ? AND owner_intent_id = ?
                  AND source_sequence <= ?
                """,
                (account_key, submit_command_id, owner_intent_id, through_sequence),
            ).fetchone()
            if (
                order_row is not None
                and int(order_row["native_volume_traded"]) != int(resolution["order_volume_traded"])
            ) or int(trades["total"]) != int(resolution["trade_volume"]):
                raise _CtpCancelPostconditionConflictError(owner_intent_id)

    def _reconcile_ctp_cancel_postconditions(
        self,
        cursor: sqlite3.Cursor,
        account_key: str,
        submit_command_id: str,
        owner_intent_id: str,
        through_sequence: int,
        reconciliation_record_digest: str,
        *,
        applied_at_ns: int,
    ) -> None:
        self._poison_if_ctp_cancel_resolution_contradicted(
            cursor,
            account_key,
            submit_command_id,
            owner_intent_id,
            through_sequence,
        )
        obligations = cursor.execute(
            """
            SELECT postcondition.* FROM ctp_dispatch_cancel_postconditions AS postcondition
            WHERE postcondition.account_key = ?
              AND postcondition.target_submit_command_id = ?
              AND postcondition.owner_intent_id = ?
              AND postcondition.identity_state = 'EXACT'
              AND NOT EXISTS (
                  SELECT 1 FROM ctp_dispatch_cancel_postcondition_resolutions AS resolution
                  WHERE resolution.account_key = postcondition.account_key
                    AND resolution.cancel_command_id = postcondition.cancel_command_id
              )
            ORDER BY postcondition.cancel_command_id
            """,
            (account_key, submit_command_id, owner_intent_id),
        ).fetchall()
        for obligation in obligations:
            terminal = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_cancel_terminal_observations
                WHERE account_key = ? AND cancel_command_id = ?
                  AND owner_intent_id = ? AND source_sequence <= ?
                ORDER BY source_sequence DESC LIMIT 1
                """,
                (
                    account_key,
                    str(obligation["cancel_command_id"]),
                    owner_intent_id,
                    through_sequence,
                ),
            ).fetchone()
            if terminal is None:
                continue
            latest_order = cursor.execute(
                """
                SELECT source_sequence, native_volume_traded FROM ctp_dispatch_order_cumulative_ledger
                WHERE account_key = ? AND command_id = ? AND owner_intent_id = ?
                  AND source_sequence <= ?
                ORDER BY source_sequence DESC LIMIT 1
                """,
                (account_key, submit_command_id, owner_intent_id, through_sequence),
            ).fetchone()
            if latest_order is None:
                continue
            terminal_volume = int(terminal["order_volume_traded"])
            latest_order_volume = int(latest_order["native_volume_traded"])
            if latest_order_volume != terminal_volume:
                # A verified order report after terminal evidence cannot
                # silently change that terminal order's cumulative amount.
                if int(latest_order["source_sequence"]) > int(terminal["source_sequence"]):
                    raise _CtpCancelPostconditionConflictError(owner_intent_id)
                continue
            trade_row = cursor.execute(
                """
                SELECT COALESCE(SUM(trade_volume), 0) AS total
                FROM ctp_dispatch_trade_fact_ledger
                WHERE account_key = ? AND command_id = ? AND owner_intent_id = ?
                  AND source_sequence <= ?
                """,
                (account_key, submit_command_id, owner_intent_id, through_sequence),
            ).fetchone()
            trade_volume = int(trade_row["total"])
            if trade_volume > terminal_volume:
                raise _CtpCancelPostconditionConflictError(owner_intent_id)
            if trade_volume != terminal_volume:
                continue
            terminal_record_digest = str(terminal["ingress_record_digest_sha256"])
            resolution_digest = payload_sha256(
                {
                    "schema": "ctp_cancel_postcondition_resolution.v1",
                    "account_key": account_key,
                    "cancel_command_id": str(obligation["cancel_command_id"]),
                    "submit_command_id": submit_command_id,
                    "terminal_owner_intent_id": owner_intent_id,
                    "terminal_source_sequence": int(terminal["source_sequence"]),
                    "reconciliation_owner_intent_id": owner_intent_id,
                    "reconciliation_source_sequence": through_sequence,
                    "order_volume_traded": terminal_volume,
                    "trade_volume": trade_volume,
                    "terminal_record_digest_sha256": terminal_record_digest,
                    "reconciliation_record_digest_sha256": reconciliation_record_digest,
                }
            )
            cursor.execute(
                """
                INSERT INTO ctp_dispatch_cancel_postcondition_resolutions(
                    account_key, cancel_command_id,
                    terminal_owner_intent_id, terminal_source_sequence,
                    reconciliation_owner_intent_id, reconciliation_source_sequence,
                    order_volume_traded, trade_volume,
                    terminal_record_digest_sha256, reconciliation_record_digest_sha256,
                    resolution_digest_sha256, resolved_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_key,
                    str(obligation["cancel_command_id"]),
                    owner_intent_id,
                    int(terminal["source_sequence"]),
                    owner_intent_id,
                    through_sequence,
                    terminal_volume,
                    trade_volume,
                    terminal_record_digest,
                    reconciliation_record_digest,
                    resolution_digest,
                    applied_at_ns,
                ),
            )

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
        source_event_identity: tuple[str, str, str] | None = None,
    ) -> None:
        if key.operation not in {"SUBMIT", "CANCEL"} or state not in _CTP_ORDER_PROJECTION_STATES:
            raise ContractValidationError("invalid CTP order projection")
        if source_event_identity is not None and (
            type(source_event_identity) is not tuple
            or len(source_event_identity) != 3
            or any(type(value) is not str or not value for value in source_event_identity)
        ):
            raise ContractValidationError("invalid CTP projection source event identity")
        event_generation_id = (
            source_event_identity[0]
            if source_event_identity is not None
            else None if callback_key is None else key.session_generation_id
        )
        event_stream_id = (
            source_event_identity[1]
            if source_event_identity is not None
            else None if callback_key is None else callback_key.stream_id
        )
        event_id = (
            source_event_identity[2]
            if source_event_identity is not None
            else None if callback_key is None else callback_key.event_id
        )
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
                event_generation_id,
                event_stream_id,
                event_id,
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
        _callback_session_owner: CtpCallbackSessionOwnerHandle | None = None,
        _callback_ingress_event: CtpCallbackIngressEventV1 | None = None,
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
        if (_callback_session_owner is None) != (_callback_ingress_event is None):
            raise ContractValidationError("CTP callback session event binding is incomplete")
        if _callback_ingress_event is not None and (
                type(_callback_session_owner) is not CtpCallbackSessionOwnerHandle
                or type(_callback_ingress_event) is not CtpCallbackIngressEventV1
                or _callback_ingress_event.owner_handle is not _callback_session_owner
                or self._issued_ctp_callback_session_owners.get(
                    _callback_session_owner.owner_intent_id
                )
                is not _callback_session_owner
                or self._issued_ctp_callback_ingress_events.get(
                    (
                        _callback_session_owner.owner_intent_id,
                        _callback_ingress_event.source_sequence,
                    )
                )
                is not _callback_ingress_event
                or _callback_ingress_event.callback_class != "ROUTEABLE"
                or callback_payload_value.get("envelope_type")
                != "ctp_native_callback_envelope.v2"
                or _callback_ingress_event.callback_name
                != callback_payload_value.get("source_callback")
                or callback_payload_value.get("ingress_source")
                != {
                    "owner_intent_id": _callback_session_owner.owner_intent_id,
                    "source_sequence": _callback_ingress_event.source_sequence,
                    "record_digest_sha256": _callback_ingress_event.record_digest_sha256,
                }
        ):
            raise ContractValidationError("CTP callback payload is not bound to its ingress row")
        staged = self.read_ctp_dispatch_command(scope, command_id)
        if staged is None:
            raise ContractValidationError("unknown CTP dispatch command")
        if staged.status not in {"CLAIMED", "COMPLETED", "UNKNOWN"}:
            raise InvalidStateTransition("callback cannot apply to an undispatched CTP command")
        if staged.correlation_key is None or staged.correlation_key.version != 2:
            raise InvalidStateTransition("legacy CTP command is audit-only")
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
        ingress_payload: Mapping[str, Any] | None = None
        ingress_binding: CtpCallbackSessionBindingV1 | None = None
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
            assert current.correlation_key is not None
            self._require_ctp_native_action_ref_allocation(cursor, row, current.correlation_key)
            if _callback_ingress_event is not None:
                assert _callback_session_owner is not None
                owner_row, _, ingress_payload, ingress_binding = (
                    self._require_current_ctp_callback_ingress_event(
                    cursor,
                    scope,
                    _callback_ingress_event,
                    writer_lease,
                    )
                )
                if (
                    row["callback_owner_intent_id"] != _callback_session_owner.owner_intent_id
                    or str(owner_row["owner_state"]) != "ACTIVE"
                    or int(row["native_call_inflight"]) != 0
                    or str(row["status"]) not in {"COMPLETED", "UNKNOWN"}
                    or row["local_queue_receipt_id"] is None
                    or row["local_queue_receipt_queued"] != 1
                    or ingress_binding is None
                    or ingress_binding.session_generation_id
                    != staged.correlation_key.session_generation_id
                ):
                    raise InvalidStateTransition(
                        "CTP session callback command is not ready for ledger commit"
                    )
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
                if _callback_ingress_event is not None:
                    if (
                        _callback_ingress_event.callback_name == "OnRtnOrder"
                        and ingress_payload is not None
                        and ingress_binding is not None
                    ):
                        self._insert_ctp_order_cumulative_observation(
                            cursor,
                            _callback_ingress_event,
                            staged,
                            ingress_binding,
                            self._ctp_callback_field(ingress_payload, 0, "VolumeTraded"),
                            callback_digest,
                            observed_at_ns=applied_at_ns,
                        )
                        self._record_ctp_cancel_terminal_observations(
                            cursor,
                            staged,
                            _callback_ingress_event,
                            callback,
                            callback_payload_value,
                            evidence,
                            applied_at_ns=applied_at_ns,
                        )
                    self._insert_ctp_callback_ingress_application(
                        cursor,
                        _callback_ingress_event,
                        command_id,
                        "APPLIED",
                        {
                            "callback_key_sha256": callback_digest,
                            "callback_payload_sha256": callback_payload_digest,
                            "source_digest_sha256": evidence.source_digest_sha256,
                            "duplicate": True,
                        },
                        applied_at_ns=applied_at_ns,
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
            if _callback_ingress_event is not None:
                if (
                    _callback_ingress_event.callback_name == "OnRtnOrder"
                    and ingress_payload is not None
                    and ingress_binding is not None
                ):
                    self._insert_ctp_order_cumulative_observation(
                        cursor,
                        _callback_ingress_event,
                        staged,
                        ingress_binding,
                        self._ctp_callback_field(ingress_payload, 0, "VolumeTraded"),
                        callback_digest,
                        observed_at_ns=applied_at_ns,
                    )
                    self._record_ctp_cancel_terminal_observations(
                        cursor,
                        staged,
                        _callback_ingress_event,
                        callback,
                        callback_payload_value,
                        evidence,
                        applied_at_ns=applied_at_ns,
                    )
                self._insert_ctp_callback_ingress_application(
                    cursor,
                    _callback_ingress_event,
                    command_id,
                    "APPLIED",
                    {
                        "callback_key_sha256": callback_digest,
                        "callback_payload_sha256": callback_payload_digest,
                        "source_digest_sha256": evidence.source_digest_sha256,
                        "duplicate": False,
                    },
                    applied_at_ns=applied_at_ns,
                )
            fence_open = self._ctp_dispatch_has_open_account_fence(cursor, account_key)
            return CtpDispatchCallbackApplyResult(
                callback_key=callback,
                projection_state=evidence.projection_state,
                duplicate=False,
                account_fence_open=fence_open,
            )

    def apply_ctp_verified_session_trade_fact(
        self,
        scope: ExecutionScope,
        command_id: str,
        event: CtpCallbackIngressEventV1,
        trade_fact: Any,
        *,
        writer_lease: WriterLease,
        trade_verifier: CtpDispatchTradeFactVerifier,
    ) -> CtpDispatchTradeFactApplyResult:
        """Commit one verified TradeField, inbox cursor, and fill projection.

        CTP TradeField has no RequestID, FrontID, or SessionID. This V2 path
        correlates by one exact prior SUBMIT OrderRef within the bound account,
        day, and session, and persists no invented native request/session IDs.
        Trade quantities aggregate only other unique trade facts; order
        ``VolumeTraded`` snapshots are stored separately for prefix comparison.
        Fees remain absent/incomplete.
        """

        from .ctp_native_callbacks import CtpNativeTradeFactV2

        if type(trade_fact) is not CtpNativeTradeFactV2:
            raise ContractValidationError("typed V2 native CTP trade fact is required")
        # The protocol is structural. Its typed result and the exact event
        # binding checked here are the contract, not a concrete class check.
        verify_trade = getattr(trade_verifier, "verify_trade_fact", None)
        if not callable(verify_trade):
            raise ContractValidationError("trusted CTP trade verifier is required")
        if (
            type(event) is not CtpCallbackIngressEventV1
            or event.owner_handle is None
            or event.callback_class != "ROUTEABLE"
            or event.callback_name != "OnRtnTrade"
            or trade_fact.source_callback != "OnRtnTrade"
        ):
            raise ContractValidationError("CTP trade fact is not bound to a routeable event")
        account_key, trading_day, scope_key = self._validate_ctp_order_identity_scope(scope)
        if (
            event.owner_handle.account_key != account_key
            or event.owner_handle.scope_key != scope_key
            or trade_fact.source_scope.account_key != account_key
            or trade_fact.source_scope.scope_key != scope_key
            or trade_fact.source_scope.trading_day != trading_day
            or self._issued_ctp_callback_ingress_events.get(
                (event.owner_handle.owner_intent_id, event.source_sequence)
            )
            is not event
        ):
            raise ContractValidationError("CTP trade source differs from execution scope")
        fact_payload = trade_fact.to_payload()
        fact_payload_json = canonical_json(fact_payload)
        fact_payload_sha256 = payload_sha256(fact_payload)
        native_fields = dict(trade_fact.native_fields)
        binding, broker_id, user_id = self.read_ctp_callback_session_context_facts(
            event.owner_handle
        )
        session = trade_fact.source_scope
        expected_fingerprint = "acct_" + hashlib.sha256(
            f"{broker_id}:{user_id}".encode("ascii")
        ).hexdigest()[:16]
        if (
            session.session_epoch != binding.native_client_epoch
            or session.session_generation_id != binding.session_generation_id
            or session.native_api_generation != binding.native_api_generation
            or session.connection_generation != binding.connection_generation
            or session.dispatch_front_id != binding.dispatch_front_id
            or session.dispatch_session_id != binding.dispatch_session_id
            or session.account_fingerprint != expected_fingerprint
            or native_fields.get("BrokerID") != broker_id
            or native_fields.get("UserID") != user_id
            or native_fields.get("TradingDay") != trading_day
        ):
            raise ContractValidationError("CTP trade fact differs from accepted source session")

        record_payload = json.loads(event.record_payload_json)
        if (
            record_payload.get("digest") != event.record_digest_sha256
            or record_payload.get("sequence") != event.source_sequence
            or record_payload.get("callback_name") != "OnRtnTrade"
            or record_payload.get("callback_class") != "ROUTEABLE"
            or record_payload.get("phase") != "ACTIVE"
            or record_payload.get("capture_complete") is not True
        ):
            raise ContractValidationError("CTP trade ingress record is not complete")
        for name, value in native_fields.items():
            source_value = self._ctp_callback_field(record_payload, 0, name)
            if name == "Price":
                try:
                    if Decimal(str(source_value)) != Decimal(str(value)):
                        raise ContractValidationError("CTP trade price differs from ingress row")
                except Exception:
                    raise ContractValidationError("CTP trade price differs from ingress row") from None
            elif source_value != value:
                raise ContractValidationError("CTP trade field differs from ingress row")

        staged = self.read_ctp_dispatch_command(scope, command_id)
        if staged is None or staged.correlation_key is None:
            raise ContractValidationError("CTP trade command is unavailable")
        correlation = staged.correlation_key
        if (
            staged.operation != "SUBMIT"
            or staged.trading_day != trading_day
            or staged.order_ref != native_fields.get("OrderRef")
            or correlation.order_ref != native_fields.get("OrderRef")
            or correlation.session_generation_id != binding.session_generation_id
            or correlation.dispatch_front_id != binding.dispatch_front_id
            or correlation.dispatch_session_id != binding.dispatch_session_id
            or staged.session_binding_sha256 != binding.session_binding_sha256
        ):
            raise ContractValidationError("CTP trade does not match the reserved submit")
        request_payload = dict(staged.request_payload)
        required_match = {
            "OrderRef": "OrderRef",
            "InstrumentID": "InstrumentID",
            "ExchangeID": "ExchangeID",
            "Direction": "Direction",
        }
        if any(request_payload.get(request_name) != native_fields.get(fact_name)
               for request_name, fact_name in required_match.items()):
            raise ContractValidationError("CTP trade identity differs from submit request")
        for request_name, fact_name in (
            ("CombOffsetFlag", "OffsetFlag"),
            ("CombHedgeFlag", "HedgeFlag"),
        ):
            requested_flag = request_payload.get(request_name)
            native_flag = native_fields.get(fact_name)
            if (
                type(requested_flag) is not str
                or not requested_flag
                or native_flag != requested_flag[0]
            ):
                raise ContractValidationError("CTP trade side flags differ from submit request")
        for identity_name in ("BrokerID", "InvestorID", "UserID"):
            if (
                identity_name in request_payload
                and request_payload[identity_name] != native_fields.get(identity_name)
            ):
                raise ContractValidationError(
                    "CTP trade account identity differs from submit request"
                )
        order_quantity = request_payload.get("VolumeTotalOriginal")
        trade_quantity = native_fields.get("Volume")
        if (
            type(order_quantity) is not int
            or order_quantity <= 0
            or type(trade_quantity) is not int
            or trade_quantity <= 0
        ):
            raise ContractValidationError("CTP trade or order quantity is invalid")
        try:
            price = Decimal(str(native_fields.get("Price")))
            limit_price = Decimal(str(request_payload.get("LimitPrice")))
        except Exception:
            raise ContractValidationError("CTP trade price does not match a valid limit") from None
        if (
            not price.is_finite()
            or not limit_price.is_finite()
            or price <= 0
            or limit_price <= 0
            or (native_fields.get("Direction") == "0" and price > limit_price)
            or (native_fields.get("Direction") == "1" and price < limit_price)
            or native_fields.get("Direction") not in {"0", "1"}
        ):
            raise ContractValidationError("CTP trade price is outside the bound submit limit")

        verification_started_ns = time.time_ns()
        verification_failed = False
        evidence: CtpVerifiedTradeFactEvidence | None = None
        try:
            evidence = verify_trade(
                staged,
                trade_fact,
                event,
                now_ns=verification_started_ns,
            )
        except Exception:
            verification_failed = True
        if verification_failed:
            raise ContractValidationError("trusted CTP trade verification failed")
        if type(evidence) is not CtpVerifiedTradeFactEvidence:
            raise ContractValidationError("invalid typed CTP trade verification result")
        if (
            evidence.owner_intent_id != event.owner_handle.owner_intent_id
            or evidence.source_sequence != event.source_sequence
            or evidence.ingress_record_digest_sha256 != event.record_digest_sha256
            or evidence.event_id != trade_fact.event_id
            or evidence.trade_fact_sha256 != fact_payload_sha256
            or evidence.verified_at_ns != verification_started_ns
        ):
            raise ContractValidationError("verified CTP trade binding does not match source event")

        with self._transaction() as cursor:
            applied_at_ns = time.time_ns()
            owner_row, _, _, current_binding = self._require_current_ctp_callback_ingress_event(
                cursor, scope, event, writer_lease
            )
            command_row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, command_id),
            ).fetchone()
            if command_row is None:
                raise ContractValidationError("CTP trade command is unavailable")
            current = self._ctp_dispatch_command_from_row(command_row)
            if (
                str(owner_row["owner_state"]) != "ACTIVE"
                or current.status not in {"COMPLETED", "UNKNOWN"}
                or current.correlation_key != correlation
                or command_row["callback_owner_intent_id"] != event.owner_handle.owner_intent_id
                or int(command_row["native_call_inflight"]) != 0
                or command_row["local_queue_receipt_id"] is None
                or command_row["local_queue_receipt_queued"] != 1
                or current_binding.session_binding_sha256 != binding.session_binding_sha256
                or evidence.expires_at_ns <= applied_at_ns
            ):
                raise InvalidStateTransition("CTP trade command or session changed before commit")
            if current.status == "COMPLETED" and (
                current.native_receipt_payload is None
                or current.native_receipt_payload.get("outcome") == "REJECTED"
            ):
                raise InvalidStateTransition("CTP trade conflicts with local native receipt")

            identity = native_fields
            exchange_id = str(identity["ExchangeID"])
            trade_id = str(identity["TradeID"])
            order_sys_id = str(identity["OrderSysID"])
            existing = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_trade_fact_ledger
                WHERE account_key = ? AND trading_day = ? AND exchange_id = ? AND trade_id = ?
                """,
                (account_key, trading_day, exchange_id, trade_id),
            ).fetchone()
            if existing is not None:
                exact = (
                    str(existing["command_id"]) == command_id
                    and str(existing["fact_payload_json"]) == fact_payload_json
                    and str(existing["fact_payload_sha256"]) == fact_payload_sha256
                    and str(existing["order_sys_id"]) == order_sys_id
                    and str(existing["order_ref"]) == str(identity["OrderRef"])
                )
                if not exact:
                    raise IntentConflictError("CTP TradeID conflicts with durable trade fact")
                self._insert_ctp_callback_ingress_application(
                    cursor,
                    event,
                    command_id,
                    "APPLIED",
                    {
                        "trade_event_id": trade_fact.event_id,
                        "trade_fact_sha256": fact_payload_sha256,
                        "duplicate": True,
                    },
                    applied_at_ns=applied_at_ns,
                )
                self._reconcile_ctp_cancel_postconditions(
                    cursor,
                    account_key,
                    command_id,
                    event.owner_handle.owner_intent_id,
                    event.source_sequence,
                    event.record_digest_sha256,
                    applied_at_ns=applied_at_ns,
                )
                projection = cursor.execute(
                    """
                    SELECT provider_state FROM ctp_dispatch_order_projection
                    WHERE account_key = ? AND runtime_order_id = ?
                    """,
                    (account_key, current.correlation_key.runtime_order_id),
                ).fetchone()
                state = None if projection is None else str(projection["provider_state"])
                return CtpDispatchTradeFactApplyResult(
                    command_id=command_id,
                    event_id=trade_fact.event_id,
                    trade_quantity=trade_quantity,
                    cumulative_trade_quantity=int(existing["cumulative_trade_volume"]),
                    projection_state=state,
                    duplicate=True,
                    account_fence_open=self._ctp_dispatch_has_open_account_fence(
                        cursor, account_key
                    ),
                )

            same_order = cursor.execute(
                """
                SELECT exchange_id, order_sys_id, instrument_id
                FROM ctp_dispatch_trade_fact_ledger
                WHERE account_key = ? AND trading_day = ? AND order_ref = ?
                LIMIT 1
                """,
                (account_key, trading_day, str(identity["OrderRef"])),
            ).fetchone()
            if same_order is not None and (
                str(same_order["exchange_id"]) != exchange_id
                or str(same_order["order_sys_id"]) != order_sys_id
                or str(same_order["instrument_id"]) != str(identity["InstrumentID"])
            ):
                raise IntentConflictError("CTP trade identity changed within one OrderRef")
            prior = cursor.execute(
                """
                SELECT COALESCE(SUM(trade_volume), 0) AS total
                FROM ctp_dispatch_trade_fact_ledger
                WHERE account_key = ? AND command_id = ?
                """,
                (account_key, command_id),
            ).fetchone()
            cumulative_trade_quantity = int(prior["total"]) + trade_quantity
            if cumulative_trade_quantity > order_quantity:
                raise ContractValidationError("CTP trade facts exceed reserved order quantity")
            latest_order = cursor.execute(
                """
                SELECT native_volume_traded FROM ctp_dispatch_order_cumulative_ledger
                WHERE owner_intent_id = ? AND command_id = ? AND source_sequence < ?
                ORDER BY source_sequence DESC LIMIT 1
                """,
                (event.owner_handle.owner_intent_id, command_id, event.source_sequence),
            ).fetchone()
            cumulative_order_quantity = (
                None if latest_order is None else int(latest_order["native_volume_traded"])
            )
            if cumulative_order_quantity is None:
                prefix_consistency = "NO_ORDER_CUMULATIVE"
            elif cumulative_order_quantity == cumulative_trade_quantity:
                prefix_consistency = "MATCHED"
            elif cumulative_order_quantity > cumulative_trade_quantity:
                prefix_consistency = "ORDER_CUMULATIVE_AHEAD"
            else:
                prefix_consistency = "TRADE_CUMULATIVE_AHEAD"

            cursor.execute(
                """
                INSERT INTO ctp_dispatch_trade_fact_ledger(
                    account_key, scope_key, trading_day, owner_intent_id,
                    source_sequence, ingress_record_digest_sha256, command_id,
                    managed_intent_id, runtime_order_id, order_ref,
                    session_generation_id, callback_stream_id, callback_event_id,
                    exchange_id, trade_id, order_sys_id, instrument_id, direction,
                    offset_flag, hedge_flag, trade_price, trade_volume,
                    cumulative_trade_volume, cumulative_order_volume,
                    prefix_consistency, cumulative_commission, commission_quality,
                    fact_payload_json, fact_payload_sha256, source_digest_sha256,
                    verifier_id, verified_at_ns, applied_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'INCOMPLETE', ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_key,
                    scope_key,
                    trading_day,
                    event.owner_handle.owner_intent_id,
                    event.source_sequence,
                    event.record_digest_sha256,
                    command_id,
                    current.reservation_managed_intent_id,
                    correlation.runtime_order_id,
                    str(identity["OrderRef"]),
                    binding.session_generation_id,
                    session.stream_id,
                    trade_fact.event_id,
                    exchange_id,
                    trade_id,
                    order_sys_id,
                    str(identity["InstrumentID"]),
                    str(identity["Direction"]),
                    str(identity["OffsetFlag"]),
                    str(identity["HedgeFlag"]),
                    str(identity["Price"]),
                    trade_quantity,
                    cumulative_trade_quantity,
                    cumulative_order_quantity,
                    prefix_consistency,
                    fact_payload_json,
                    fact_payload_sha256,
                    evidence.source_digest_sha256,
                    evidence.verifier_id,
                    evidence.verified_at_ns,
                    applied_at_ns,
                ),
            )
            projection_row = cursor.execute(
                """
                SELECT provider_state FROM ctp_dispatch_order_projection
                WHERE account_key = ? AND runtime_order_id = ?
                """,
                (account_key, correlation.runtime_order_id),
            ).fetchone()
            current_state = None if projection_row is None else str(projection_row["provider_state"])
            if current_state == "REJECTED":
                raise InvalidStateTransition("CTP trade conflicts with rejected order projection")
            if current_state == "CANCELLED":
                projection_state = "CANCELLED"
            elif current_state == "FILLED":
                projection_state = "FILLED"
            else:
                projection_state = (
                    "FILLED" if cumulative_trade_quantity == order_quantity else "PARTIALLY_FILLED"
                )
                self._upsert_ctp_order_projection(
                    cursor,
                    correlation,
                    projection_state,
                    evidence.source_digest_sha256,
                    source_kind="CALLBACK",
                    updated_at_ns=applied_at_ns,
                    source_event_identity=(
                        binding.session_generation_id,
                        session.stream_id,
                        trade_fact.event_id,
                    ),
                )
            self._insert_ctp_callback_ingress_application(
                cursor,
                event,
                command_id,
                "APPLIED",
                {
                    "trade_event_id": trade_fact.event_id,
                    "trade_fact_sha256": fact_payload_sha256,
                    "source_digest_sha256": evidence.source_digest_sha256,
                    "cumulative_trade_quantity": cumulative_trade_quantity,
                    "cumulative_order_quantity": cumulative_order_quantity,
                    "prefix_consistency": prefix_consistency,
                    "commission_quality": "INCOMPLETE",
                    "duplicate": False,
                },
                applied_at_ns=applied_at_ns,
            )
            self._reconcile_ctp_cancel_postconditions(
                cursor,
                account_key,
                command_id,
                event.owner_handle.owner_intent_id,
                event.source_sequence,
                event.record_digest_sha256,
                applied_at_ns=applied_at_ns,
            )
            return CtpDispatchTradeFactApplyResult(
                command_id=command_id,
                event_id=trade_fact.event_id,
                trade_quantity=trade_quantity,
                cumulative_trade_quantity=cumulative_trade_quantity,
                projection_state=projection_state,
                duplicate=False,
                account_fence_open=self._ctp_dispatch_has_open_account_fence(
                    cursor, account_key
                ),
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
    def _read_transaction(self) -> Iterator[sqlite3.Cursor]:
        """Keep a consistent read snapshot without reserving SQLite's writer lock."""

        with self._lock:
            cursor = self._connection.cursor()
            try:
                cursor.execute("BEGIN")
                yield cursor
            except sqlite3.Error as error:
                self._connection.rollback()
                raise DurableStoreError("execution store read transaction failed") from error
            except BaseException:
                self._connection.rollback()
                raise
            else:
                try:
                    self._connection.commit()
                except sqlite3.Error as error:
                    self._connection.rollback()
                    raise DurableStoreError(
                        "execution store read transaction commit failed"
                    ) from error
            finally:
                cursor.close()

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
            except BaseException as error:
                self._connection.rollback()
                if isinstance(error, _CtpCancelPostconditionConflictError):
                    poison_cursor = self._connection.cursor()
                    try:
                        poison_cursor.execute("BEGIN IMMEDIATE")
                        poison_cursor.execute(
                            """
                            UPDATE ctp_dispatch_callback_session_owners
                            SET owner_state = 'POISONED', poison_code = 'callback_apply_failure',
                                poison_observed_sequence = MAX(
                                    last_source_sequence, COALESCE(poison_observed_sequence, 0)
                                ),
                                updated_at_ns = ?
                            WHERE owner_intent_id = ?
                              AND owner_state IN ('PREPARED', 'ACTIVE')
                            """,
                            (time.time_ns(), error.owner_intent_id),
                        )
                        poisoned_owner = poison_cursor.execute(
                            """
                            SELECT account_key FROM ctp_dispatch_callback_session_owners
                            WHERE owner_intent_id = ?
                            """,
                            (error.owner_intent_id,),
                        ).fetchone()
                        if poisoned_owner is not None:
                            self._poison_ctp_account_family_owner(
                                poison_cursor,
                                account_key=str(poisoned_owner["account_key"]),
                                reason_code="callback_apply_failure",
                                now_ns=time.time_ns(),
                            )
                        self._connection.commit()
                    except sqlite3.Error:
                        self._connection.rollback()
                    finally:
                        poison_cursor.close()
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
    ) -> ExecutionEvent:
        identity_row = cursor.execute(
            "SELECT value FROM execution_meta WHERE key = ?",
            ("journal_incarnation_id",),
        ).fetchone()
        if identity_row is None:
            raise DurableStoreError("execution journal identity is unavailable")
        event_id = uuid.uuid4().hex
        payload_json = self._event_payload(**payload)
        cursor.execute(
            """
            INSERT INTO execution_outbox(
                event_id, intent_id, scope_key, event_type, state, payload_json, created_at_ns,
                journal_incarnation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                intent_id,
                scope_key,
                event_type,
                state.value,
                payload_json,
                now_ns,
                identity_row["value"],
            ),
        )
        return ExecutionEvent(
            sequence=int(cursor.lastrowid),
            event_id=event_id,
            intent_id=intent_id,
            scope_key=scope_key,
            event_type=event_type,
            state=state,
            payload=json.loads(payload_json),
            created_at_ns=now_ns,
            journal_incarnation_id=str(identity_row["value"]),
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
        is_ctp_scope = (
            type(scope) is ExecutionScope and scope.provider.upper() == "CTP"
        )
        if is_ctp_scope:
            family_key = SqliteExecutionStore._ctp_account_family_key(scope)
            if (
                writer_lease.family_key != family_key
                or not _is_local_queue_receipt_id(writer_lease.family_owner_intent_id)
            ):
                raise WriterLeaseUnavailable()
            SqliteExecutionStore._assert_no_unmapped_ctp_account_family_history(cursor)
            family_row = cursor.execute(
                """
                SELECT owner_state FROM ctp_account_family_owners
                WHERE family_key = ? AND owner_intent_id = ?
                  AND account_key = ?
                """,
                (
                    family_key,
                    writer_lease.family_owner_intent_id,
                    scope.account_key,
                ),
            ).fetchone()
            if family_row is None or str(family_row["owner_state"]) != "ACTIVE":
                raise WriterLeaseUnavailable()
        elif (
            writer_lease.family_key is not None
            or writer_lease.family_owner_intent_id is not None
        ):
            raise ContractValidationError("non-CTP writer lease has a CTP account family")
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

    @staticmethod
    def _unresolved_cancellations_for_account_cursor(
        cursor: sqlite3.Cursor,
        account_key: str,
        *,
        ctp_family_key: str | None = None,
    ) -> tuple[tuple[ExecutionScope, CancelRecord], ...]:
        rows = cursor.execute(
            """
            SELECT * FROM cancellation_records
            WHERE state IN (?, ?)
            ORDER BY scope_key, created_at_ns, cancel_id
            """,
            (ExecutionState.DISPATCHING.value, ExecutionState.UNKNOWN.value),
        ).fetchall()
        matches: list[tuple[ExecutionScope, CancelRecord]] = []
        for row in rows:
            try:
                payload_json = str(row["payload_json"])
                payload = json.loads(payload_json)
                intent = cancel_intent_from_payload(payload)
                if (
                    canonical_json(payload) != payload_json
                    or intent.fingerprint != str(row["payload_sha256"])
                    or intent.cancel_id != str(row["cancel_id"])
                    or intent.scope.key != str(row["scope_key"])
                    or intent.target_intent_id != str(row["target_intent_id"])
                    or intent.provider_order_id != str(row["provider_order_id"])
                ):
                    raise ValueError("cancellation row does not match its payload")
            except (ContractValidationError, TypeError, ValueError, json.JSONDecodeError):
                raise DurableStoreError(
                    "stored account-wide cancellation identity is unreadable"
                ) from None
            if ctp_family_key is not None:
                if intent.scope.provider.lower() == "ctp":
                    try:
                        row_family_key = SqliteExecutionStore._ctp_account_family_key(
                            intent.scope
                        )
                    except ContractValidationError:
                        raise DurableStoreError(
                            "stored account-wide cancellation identity is unreadable"
                        ) from None
                    if row_family_key == ctp_family_key:
                        matches.append(
                            (intent.scope, SqliteExecutionStore._cancel_record_from_row(row))
                        )
                elif intent.scope.account_key == account_key:
                    matches.append(
                        (intent.scope, SqliteExecutionStore._cancel_record_from_row(row))
                    )
            elif intent.scope.account_key == account_key:
                matches.append((intent.scope, SqliteExecutionStore._cancel_record_from_row(row)))
        return tuple(matches)

    def list_unresolved_cancellations_for_account(
        self,
        scope: ExecutionScope,
        *,
        writer_lease: WriterLease,
    ) -> tuple[tuple[ExecutionScope, CancelRecord], ...]:
        """Read nonterminal cancellation facts across this exact account.

        The account key deliberately excludes strategy and trading day, so the
        returned scope alongside each record preserves its original identity.
        Persisted payloads are reconstructed and checked before their account
        is considered; malformed rows fail closed rather than disappearing
        from an account-wide recovery scan.
        """

        if type(scope) is not ExecutionScope:
            raise ContractValidationError("execution scope is required")
        try:
            with self._read_transaction() as cursor:
                self._assert_active_writer_lease(cursor, scope, writer_lease)
                unresolved = self._unresolved_cancellations_for_account_cursor(
                    cursor,
                    scope.account_key,
                    ctp_family_key=(
                        self._ctp_account_family_key(scope)
                        if scope.provider == "ctp"
                        else None
                    ),
                )
        except sqlite3.Error as error:
            raise DurableStoreError(
                "unable to list unresolved cancellation records for account"
            ) from error
        return unresolved

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
                            "filled_quantity": format(target_record.filled_quantity, "f"),
                            "average_price": (
                                None
                                if target_record.average_price is None
                                else format(target_record.average_price, "f")
                            ),
                            "cumulative_commission": (
                                None
                                if target_record.cumulative_commission is None
                                else format(target_record.cumulative_commission, "f")
                            ),
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
        """Durably reject an intent before any provider dispatch.

        The ``intent_rejected``/``intent_blocked`` events are no-dispatch
        lifecycle facts. They are emitted only from ``PENDING_ADMISSION`` and
        only while the record has no provider identity, fill, price, fee, or
        dispatch attempt. Their payload contains only the local reason code.
        """

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
            if (
                record.dispatch_attempts != 0
                or record.provider_order_id is not None
                or record.filled_quantity != Decimal("0")
                or record.average_price is not None
                or record.cumulative_commission is not None
            ):
                raise InvalidStateTransition("pending admission has dispatch evidence")
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
            if self._unresolved_cancellations_for_account_cursor(
                cursor,
                scope.account_key,
                ctp_family_key=(
                    self._ctp_account_family_key(scope)
                    if scope.provider.lower() == "ctp"
                    else None
                ),
            ):
                raise InvalidStateTransition(
                    "account has an unresolved managed cancellation"
                )
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
        """Apply provider evidence while preserving the record-returning API."""

        record, _event = self.record_observation_with_event(
            observation, scope=scope, source=source, writer_lease=writer_lease
        )
        return record

    def record_observation_with_event(
        self,
        observation: ProviderObservation,
        *,
        scope: ExecutionScope,
        source: str = "provider",
        writer_lease: WriterLease | None = None,
    ) -> tuple[ExecutionRecord, ExecutionEvent | None]:
        """Apply evidence and return the exact new event from the same transaction."""

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
            same_state_partial_growth = (
                record.state is ExecutionState.PARTIALLY_FILLED
                and target is ExecutionState.PARTIALLY_FILLED
                and observation.filled_quantity > record.filled_quantity
            )
            if record.state is target and not same_state_partial_growth:
                if (
                    observation.filled_quantity != record.filled_quantity
                    or (
                        observation.provider_order_id is not None
                        and observation.provider_order_id != record.provider_order_id
                    )
                    or (
                        observation.average_price is not None
                        and observation.average_price != record.average_price
                    )
                ):
                    raise InvalidStateTransition("conflicting duplicate provider observation")
                if observation.cumulative_commission is None:
                    return record, None
                if record.cumulative_commission == observation.cumulative_commission:
                    return record, None
                if record.cumulative_commission is not None:
                    raise InvalidStateTransition("conflicting duplicate provider commission")
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
                appended_event = self._append_event(
                    cursor,
                    intent_id=observation.intent_id,
                    scope_key=scope.key,
                    event_type="provider_commission_evidence",
                    state=target,
                    payload={
                        "provider_order_id": record.provider_order_id,
                        "filled_quantity": format(record.filled_quantity, "f"),
                        "average_price": (
                            None
                            if record.average_price is None
                            else format(record.average_price, "f")
                        ),
                        "cumulative_commission": format(
                            observation.cumulative_commission, "f"
                        ),
                        "source": source,
                    },
                    now_ns=now_ns,
                )
                updated = self._fetch_record(cursor, scope.key, observation.intent_id)
                assert updated is not None
                return self._record_from_row(updated), appended_event
            if record.is_terminal:
                if target in {ExecutionState.ACKED, ExecutionState.PARTIALLY_FILLED}:
                    return record, None
                raise InvalidStateTransition("conflicting terminal provider observation")
            if not same_state_partial_growth and not can_transition(record.state, target):
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
                        else (
                            None
                            if observation.filled_quantity > record.filled_quantity
                            else row["cumulative_commission"]
                        )
                    ),
                    0 if target in TERMINAL_STATES else record.review_required,
                    now_ns,
                    scope.key,
                    observation.intent_id,
                ),
            )
            appended_event = self._append_event(
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
                    "average_price": (
                        None
                        if observation.average_price is None
                        else format(observation.average_price, "f")
                    ),
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
            return self._record_from_row(updated), appended_event

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
                               created_at_ns, journal_incarnation_id
                        FROM execution_outbox WHERE sequence > ? ORDER BY sequence ASC LIMIT ?
                        """,
                        (after_sequence, limit),
                    ).fetchall()
                else:
                    rows = self._connection.execute(
                        """
                        SELECT sequence, event_id, intent_id, scope_key, event_type, state, payload_json,
                               created_at_ns, journal_incarnation_id
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
                    journal_incarnation_id=row["journal_incarnation_id"],
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
        ctp_account_family_owner: CtpAccountFamilyOwnerHandle | None = None,
    ) -> WriterLease:
        """Acquire or renew the sole dispatch writer lease for one account scope."""

        if not owner_id or owner_id != owner_id.strip() or len(owner_id) > 128:
            raise ContractValidationError("invalid writer owner_id")
        if type(ttl_ns) is not int or not 1_000_000 <= ttl_ns <= 300_000_000_000:
            raise ContractValidationError("invalid lease ttl_ns")
        now_ns = time.time_ns()
        expires_at_ns = now_ns + ttl_ns
        scope_key = scope.account_key
        is_ctp_scope = (
            type(scope) is ExecutionScope and scope.provider.upper() == "CTP"
        )
        if not is_ctp_scope and ctp_account_family_owner is not None:
            raise ContractValidationError(
                "CTP account family owner cannot be used for another provider"
            )
        with self._transaction() as cursor:
            if is_ctp_scope:
                if ctp_account_family_owner is None:
                    raise InvalidStateTransition(
                        "CTP account family owner must precede the writer lease"
                    )
                self._require_ctp_account_family_owner(
                    cursor, scope, ctp_account_family_owner
                )
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
        return WriterLease(
            scope_key,
            owner_id,
            token,
            expires_at_ns,
            None if not is_ctp_scope else ctp_account_family_owner.family_key,
            None if not is_ctp_scope else ctp_account_family_owner.owner_intent_id,
        )

    def release_lease(
        self,
        scope: ExecutionScope,
        owner_id: str,
        *,
        fencing_token: int,
    ) -> bool:
        """Expire only the exact owner/token generation without reusing its token.

        The row is a durable fencing-generation tombstone. Deleting it would
        reset the next acquisition to token 1 and could make a stale lease
        object valid again when the same owner_id is reused.
        """

        if type(fencing_token) is not int or fencing_token <= 0:
            raise ContractValidationError("invalid writer fencing_token")

        with self._transaction() as cursor:
            result = cursor.execute(
                """
                UPDATE execution_writer_leases
                SET expires_at_ns = 0
                WHERE scope_key = ? AND owner_id = ? AND fencing_token = ?
                  AND expires_at_ns != 0
                """,
                (scope.account_key, owner_id, fencing_token),
            )
            return result.rowcount == 1
