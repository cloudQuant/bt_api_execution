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
from typing import TYPE_CHECKING, Any, Optional, Protocol, Tuple

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


@dataclass(frozen=True)
class CtpOrderRefSeedProof:
    """Explicit offline evidence used to fence new CTP OrderRefs.

    This records caller-supplied observations of native MaxOrderRef and the
    legacy reservation ledger. It is not provider verification or write
    authorization; no registered runtime currently creates this proof.
    """

    trading_day: str
    native_max_order_ref: str
    legacy_ledger_max_order_ref: str
    legacy_ledger_sha256: str


@dataclass(frozen=True)
class CtpDispatchCommand:
    """Immutable staged CTP request plus its durable local dispatch state."""

    account_key: str
    scope_key: str
    trading_day: str
    operation: str
    command_id: str
    request_payload: Mapping[str, Any]
    request_payload_sha256: str
    reservation_managed_intent_id: str
    order_ref: Optional[str]
    cancel_target_order_ref: Optional[str]
    cancel_target_exchange_id: Optional[str]
    cancel_target_order_sys_id: Optional[str]
    cancel_target_front_id: Optional[int]
    cancel_target_session_id: Optional[int]
    approval_use_id: str
    approval_digest: str
    session_binding: Mapping[str, Any]
    session_binding_sha256: str
    status: str
    created_at_ns: int
    updated_at_ns: int
    claimed_at_ns: Optional[int]
    claimed_owner_id: Optional[str]
    claimed_fencing_token: Optional[int]
    completed_at_ns: Optional[int]
    unknown_at_ns: Optional[int]
    unknown_reason: Optional[str]
    native_receipt_payload: Optional[Mapping[str, Any]]
    native_receipt_sha256: Optional[str]
    completion_echo_sha256: Optional[str]

    @property
    def authority_binding_sha256(self) -> str:
        """Return the canonical digest of every immutable dispatch binding.

        This is an input to a trusted action verifier, not authorization by
        itself. In particular, the stored approval digest is only an echo.
        """

        return payload_sha256(
            {
                "binding_type": "ctp_dispatch_action_binding.v1",
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
    local queue disposition and are not provider acknowledgements.
    """

    receipt_type: str
    command_id: str
    account_key: str
    scope_key: str
    trading_day: str
    operation: str
    request_payload_sha256: str
    reservation_managed_intent_id: str
    order_ref: Optional[str]
    cancel_target_order_ref: Optional[str]
    cancel_target_exchange_id: Optional[str]
    cancel_target_order_sys_id: Optional[str]
    cancel_target_front_id: Optional[int]
    cancel_target_session_id: Optional[int]
    approval_use_id: str
    approval_digest: str
    session_binding_sha256: str
    outcome: str
    native_receipt_payload: Mapping[str, Any]


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
    _SCHEMA_VERSION = 6

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

    def _create_schema(self) -> None:
        with self._transaction() as cursor:
            cursor.executescript(
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
                CREATE TRIGGER IF NOT EXISTS ctp_dispatch_commands_immutable
                BEFORE UPDATE OF account_key, scope_key, trading_day, operation,
                    command_id, request_payload_json, request_payload_sha256,
                    reservation_managed_intent_id, order_ref, cancel_target_order_ref,
                    cancel_target_exchange_id, cancel_target_order_sys_id,
                    cancel_target_front_id, cancel_target_session_id,
                    approval_use_id, approval_digest, session_binding_json,
                    session_binding_sha256
                ON ctp_dispatch_commands
                BEGIN
                    SELECT RAISE(ABORT, 'CTP dispatch command identity is immutable');
                END;
                """
            )
            row = cursor.execute(
                "SELECT value FROM execution_meta WHERE key = ?", ("schema_version",)
            ).fetchone()
            if row is None:
                cursor.execute(
                    "INSERT INTO execution_meta(key, value) VALUES (?, ?)",
                    ("schema_version", str(self._SCHEMA_VERSION)),
                )
                return
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
                cursor.execute(
                    "UPDATE execution_meta SET value = ? WHERE key = ?",
                    (str(self._SCHEMA_VERSION), "schema_version"),
                )
                return
            if version == "3":
                cursor.execute(
                    "UPDATE execution_meta SET value = ? WHERE key = ?",
                    (str(self._SCHEMA_VERSION), "schema_version"),
                )
                return
            if version == "4":
                cursor.execute(
                    "UPDATE execution_meta SET value = ? WHERE key = ?",
                    (str(self._SCHEMA_VERSION), "schema_version"),
                )
                return
            if version == "5":
                cursor.execute(
                    "UPDATE execution_meta SET value = ? WHERE key = ?",
                    (str(self._SCHEMA_VERSION), "schema_version"),
                )
                return
            if version != str(self._SCHEMA_VERSION):
                raise DurableStoreError("unsupported execution store schema")

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

        The sequence is account-wide across trading days, so a committed or
        reservation-only reference is never reused after restart or when the
        scope's trading day changes.  ``BEGIN IMMEDIATE`` serializes allocation
        across independent store instances.  Repeating the identical mapping
        is idempotent; reusing either identity in a conflicting mapping fails.

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
            watermark = cursor.execute(
                """
                SELECT native_max_order_ref, legacy_ledger_max_order_ref,
                       legacy_ledger_sha256
                FROM ctp_order_ref_watermarks
                WHERE account_key = ? AND trading_day = ?
                """,
                (account_key, trading_day),
            ).fetchone()
            if watermark is not None:
                seeded_floor = max(
                    int(str(watermark["native_max_order_ref"])),
                    int(str(watermark["legacy_ledger_max_order_ref"])),
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
        """Record explicit MaxOrderRef and legacy-ledger seed evidence.

        The proof is an offline caller observation. This store does not verify
        a native session, import the legacy ledger, or authorize dispatch.
        Commands remain unclaimable until this per-account/day seed exists.
        """

        account_key, trading_day, _ = self._validate_ctp_order_identity_scope(scope)
        if type(proof) is not CtpOrderRefSeedProof or proof.trading_day != trading_day:
            raise ContractValidationError("invalid CTP OrderRef seed proof")
        self._validate_ctp_order_ref(proof.native_max_order_ref, "native MaxOrderRef")
        self._validate_ctp_order_ref(proof.legacy_ledger_max_order_ref, "legacy ledger MaxOrderRef")
        self._validate_sha256(proof.legacy_ledger_sha256, "legacy ledger digest")
        now_ns = time.time_ns()
        with self._transaction() as cursor:
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            existing = cursor.execute(
                """
                SELECT native_max_order_ref, legacy_ledger_max_order_ref,
                       legacy_ledger_sha256
                FROM ctp_order_ref_watermarks
                WHERE account_key = ? AND trading_day = ?
                """,
                (account_key, trading_day),
            ).fetchone()
            if existing is not None:
                if int(proof.native_max_order_ref) < int(existing["native_max_order_ref"]) or int(
                    proof.legacy_ledger_max_order_ref
                ) < int(existing["legacy_ledger_max_order_ref"]):
                    raise IntentConflictError("CTP OrderRef seed watermark cannot move backward")
                if (
                    proof.native_max_order_ref == str(existing["native_max_order_ref"])
                    and proof.legacy_ledger_max_order_ref
                    == str(existing["legacy_ledger_max_order_ref"])
                    and proof.legacy_ledger_sha256 == str(existing["legacy_ledger_sha256"])
                ):
                    return
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
                        account_key,
                        trading_day,
                    ),
                )
                return
            raise ContractValidationError(
                "initial CTP OrderRef seed must commit atomically with a new reservation"
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
        if type(proof) is not CtpOrderRefSeedProof or proof.trading_day != trading_day:
            raise ContractValidationError("invalid CTP OrderRef seed proof")
        self._validate_ctp_order_ref(proof.native_max_order_ref, "native MaxOrderRef")
        self._validate_ctp_order_ref(proof.legacy_ledger_max_order_ref, "legacy ledger MaxOrderRef")
        self._validate_sha256(proof.legacy_ledger_sha256, "legacy ledger digest")
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
            if existing_identity is not None:
                proof_matches = watermark is not None and (
                    str(watermark["native_max_order_ref"]) == proof.native_max_order_ref
                    and str(watermark["legacy_ledger_max_order_ref"])
                    == proof.legacy_ledger_max_order_ref
                    and str(watermark["legacy_ledger_sha256"]) == proof.legacy_ledger_sha256
                )
                if (
                    proof_matches
                    and str(existing_identity["trading_day"]) == trading_day
                    and str(existing_identity["runtime_order_id"]) == runtime_order_id
                    and int(existing_identity["created_at_ns"])
                    >= int(
                        cursor.execute(
                            """
                            SELECT updated_at_ns FROM ctp_order_ref_watermarks
                            WHERE account_key = ? AND trading_day = ?
                            """,
                            (account_key, trading_day),
                        ).fetchone()["updated_at_ns"]
                    )
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
                0 if latest is None else int(str(latest["order_ref"])),
            )
            next_value = floor + 1
            if next_value > 999_999_999_999:
                raise DurableStoreError("CTP OrderRef sequence is exhausted")
            order_ref = f"{next_value:012d}"
            watermark_matches = watermark is not None and (
                str(watermark["native_max_order_ref"]) == proof.native_max_order_ref
                and str(watermark["legacy_ledger_max_order_ref"])
                == proof.legacy_ledger_max_order_ref
                and str(watermark["legacy_ledger_sha256"]) == proof.legacy_ledger_sha256
            )
            if watermark is None:
                cursor.execute(
                    """
                    INSERT INTO ctp_order_ref_watermarks(
                        account_key, trading_day, native_max_order_ref,
                        legacy_ledger_max_order_ref, legacy_ledger_sha256, updated_at_ns
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_key,
                        trading_day,
                        proof.native_max_order_ref,
                        proof.legacy_ledger_max_order_ref,
                        proof.legacy_ledger_sha256,
                        now_ns,
                    ),
                )
            elif not watermark_matches:
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
                        account_key,
                        trading_day,
                    ),
                )
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
        managed_intent_id: Optional[str] = None,
        order_ref: Optional[str] = None,
        cancel_target: Optional[CtpCancelTarget] = None,
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
        self._reject_sensitive_command_fields(request_value)
        self._reject_sensitive_command_fields(session_value)
        request_digest = payload_sha256(request_value)
        session_digest = payload_sha256(session_value)

        reservation_managed_intent_id: str
        persisted_order_ref: Optional[str]
        persisted_cancel_target: Optional[str]
        cancel_exchange_id: Optional[str] = None
        cancel_order_sys_id: Optional[str] = None
        cancel_front_id: Optional[int] = None
        cancel_session_id: Optional[int] = None
        if operation == "SUBMIT":
            if managed_intent_id is None or order_ref is None or cancel_target is not None:
                raise ContractValidationError("SUBMIT requires its reserved intent and OrderRef")
            self._validate_command_identifier(managed_intent_id, "managed_intent_id")
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
                    SELECT managed_intent_id FROM ctp_order_identity_reservations
                    WHERE account_key = ? AND trading_day = ? AND scope_key = ?
                      AND managed_intent_id = ? AND order_ref = ?
                    """,
                    (account_key, trading_day, scope_key, reservation_managed_intent_id, order_ref),
                ).fetchone()
            else:
                reservation = cursor.execute(
                    """
                    SELECT managed_intent_id FROM ctp_order_identity_reservations
                    WHERE account_key = ? AND trading_day = ? AND scope_key = ?
                      AND order_ref = ?
                    """,
                    (account_key, trading_day, scope_key, persisted_cancel_target),
                ).fetchone()
            if reservation is None:
                raise ContractValidationError("CTP command has no exact OrderRef reservation")
            reservation_managed_intent_id = str(reservation["managed_intent_id"])

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
                )
                if stored != immutable:
                    raise IntentConflictError("CTP command_id conflicts with staged command")
                return self._ctp_dispatch_command_from_row(existing)

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
                    session_binding_sha256, status, created_at_ns, updated_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'READY', ?, ?)
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
    ) -> Optional[CtpDispatchCommand]:
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

    def claim_ctp_dispatch_command(
        self,
        scope: ExecutionScope,
        command_id: str,
        *,
        writer_lease: WriterLease,
        authority_verifier: CtpDispatchAuthorityVerifier,
    ) -> Optional[CtpDispatchCommand]:
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
        with self._transaction() as cursor:
            now_ns = time.time_ns()
            self._assert_active_writer_lease(cursor, scope, writer_lease)
            row = cursor.execute(
                """
                SELECT * FROM ctp_dispatch_commands
                WHERE account_key = ? AND scope_key = ? AND command_id = ?
                """,
                (account_key, scope_key, command_id),
            ).fetchone()
            if row is None or str(row["status"]) != "READY":
                return None
            unresolved = cursor.execute(
                """
                SELECT command_id, status FROM ctp_dispatch_commands
                WHERE account_key = ? AND status IN ('CLAIMED', 'UNKNOWN')
                ORDER BY created_at_ns, command_id LIMIT 1
                """,
                (account_key,),
            ).fetchone()
            if unresolved is not None:
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
            authority = self._verify_ctp_dispatch_authority(
                authority_verifier, command, now_ns=now_ns
            )
            cursor.execute(
                """
                UPDATE ctp_dispatch_commands
                SET status = 'CLAIMED', updated_at_ns = ?, claimed_at_ns = ?,
                    claimed_owner_id = ?, claimed_fencing_token = ?
                WHERE account_key = ? AND scope_key = ? AND command_id = ? AND status = 'READY'
                """,
                (
                    now_ns,
                    now_ns,
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
    def _ctp_dispatch_receipt_payload(receipt: CtpDispatchReceipt) -> Tuple[str, str, str]:
        if (
            type(receipt) is not CtpDispatchReceipt
            or receipt.receipt_type != "ctp_dispatch_receipt.v1"
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
        """Persist a typed receipt only when every command binding echoes."""

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
            )
            if actual != expected:
                raise ContractValidationError("CTP dispatch receipt echo does not match command")
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

    def recover_claimed_ctp_dispatch_commands(
        self,
        scope: ExecutionScope,
        *,
        writer_lease: WriterLease,
    ) -> Tuple[CtpDispatchCommand, ...]:
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
    ) -> None:
        """Fence a facade mutation inside its SQLite transaction.

        Every execution/cancellation state mutation must carry the exact lease
        generation acquired by its facade.  This keeps a direct low-level
        caller from silently bypassing the writer fence after a lease expiry.
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
        if (
            row is None
            or str(row["owner_id"]) != writer_lease.owner_id
            or int(row["fencing_token"]) != writer_lease.fencing_token
            or int(row["expires_at_ns"]) <= time.time_ns()
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
