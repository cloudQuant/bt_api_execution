"""Durable local journal for ordered multi-leg execution coordination."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

from .contracts import canonical_json, payload_sha256
from .errors import ContractValidationError, DurableStoreError, IntentConflictError
from .group_order_contracts import (
    GroupAccountReservationSnapshot,
    GroupLegProgress,
    GroupOrderEvent,
    GroupOrderIntent,
    GroupOrderSnapshot,
    GroupOrderState,
)

_TERMINAL_STATES = frozenset(
    {
        GroupOrderState.PAIRED,
        GroupOrderState.CLOSED,
        GroupOrderState.REJECTED_BEFORE_DISPATCH,
    }
)
_ALLOWED_STATE_TRANSITIONS = {
    GroupOrderState.PREPARING: frozenset(
        {
            GroupOrderState.FIRST_PENDING,
            GroupOrderState.REJECTED_BEFORE_DISPATCH,
            GroupOrderState.RECOVERY_REQUIRED,
        }
    ),
    GroupOrderState.FIRST_PENDING: frozenset(
        {
            GroupOrderState.HEDGE_PENDING,
            GroupOrderState.CLOSED,
            GroupOrderState.RECOVERY_REQUIRED,
        }
    ),
    GroupOrderState.HEDGE_PENDING: frozenset(
        {GroupOrderState.PAIRED, GroupOrderState.RECOVERY_REQUIRED}
    ),
    GroupOrderState.RECOVERY_REQUIRED: frozenset(
        {
            GroupOrderState.FIRST_PENDING,
            GroupOrderState.HEDGE_PENDING,
            GroupOrderState.PAIRED,
            GroupOrderState.CLOSED,
            GroupOrderState.REJECTED_BEFORE_DISPATCH,
        }
    ),
    GroupOrderState.PAIRED: frozenset({GroupOrderState.UNWINDING, GroupOrderState.CLOSED}),
    GroupOrderState.UNWINDING: frozenset(
        {GroupOrderState.CLOSED, GroupOrderState.RECOVERY_REQUIRED}
    ),
    GroupOrderState.CLOSED: frozenset(),
    GroupOrderState.REJECTED_BEFORE_DISPATCH: frozenset(),
}
_EVENT_TYPE = frozenset(
    {
        "GROUP_ACCEPTED",
        "ACCOUNT_TARGETS_BOUND",
        "RESERVATION_QUERY_STARTED",
        "RESERVATION_CONFIRMED",
        "RESERVATION_DENIED",
        "RESERVATION_UNKNOWN",
        "RESERVATION_RELEASE_STARTED",
        "RESERVATION_RELEASED",
        "FIRST_LEG_QUERY_STARTED",
        "FIRST_LEG_SUBMIT_STARTED",
        "FIRST_LEG_STATUS_OBSERVED",
        "GROUP_DISPATCH_RESOLUTION_RECORDED",
        "HEDGE_LEG_SIZED",
        "HEDGE_LEG_QUERY_STARTED",
        "HEDGE_LEG_SUBMIT_STARTED",
        "HEDGE_LEG_STATUS_OBSERVED",
        "GROUP_PAIRED",
        "GROUP_REJECTED_BEFORE_DISPATCH",
        "GROUP_CLOSED_NO_EXPOSURE",
        "GROUP_RECOVERY_REQUIRED",
    }
)


class SqliteOrderGroupStore:
    """Persist immutable group requests and revisioned coordinator snapshots.

    Remote calls never occur inside a SQLite transaction. The caller writes a
    progress event before a side effect and recovers by querying the original
    group, reservation, and child-order identities.
    """

    _SCHEMA_VERSION = 1

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 5_000) -> None:
        if type(busy_timeout_ms) is not int or busy_timeout_ms <= 0:
            raise ContractValidationError("invalid busy_timeout_ms")
        self._path = str(path)
        self._lock = threading.RLock()
        try:
            self._connection = sqlite3.connect(
                self._path,
                timeout=busy_timeout_ms / 1000,
                isolation_level=None,
                check_same_thread=False,
            )
            self._connection.row_factory = sqlite3.Row
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = FULL")
            self._initialize_schema()
        except sqlite3.Error as error:
            raise DurableStoreError("unable to open group journal") from error

    def _initialize_schema(self) -> None:
        with self._lock:
            try:
                self._connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS execution_group_meta (
                        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                        schema_version INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS order_group_snapshots (
                        portfolio_ref TEXT NOT NULL,
                        group_id TEXT NOT NULL,
                        request_fingerprint TEXT NOT NULL CHECK(length(request_fingerprint) = 64),
                        request_json TEXT NOT NULL,
                        snapshot_json TEXT NOT NULL,
                        snapshot_sha256 TEXT NOT NULL CHECK(length(snapshot_sha256) = 64),
                        state TEXT NOT NULL,
                        revision INTEGER NOT NULL CHECK(revision >= 0),
                        deadline_ns INTEGER NOT NULL CHECK(deadline_ns > 0),
                        created_at_ns INTEGER NOT NULL CHECK(created_at_ns > 0),
                        updated_at_ns INTEGER NOT NULL CHECK(updated_at_ns > 0),
                        PRIMARY KEY(portfolio_ref, group_id)
                    );
                    CREATE TABLE IF NOT EXISTS order_group_events (
                        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                        portfolio_ref TEXT NOT NULL,
                        group_id TEXT NOT NULL,
                        revision INTEGER NOT NULL CHECK(revision >= 0),
                        event_type TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        event_sha256 TEXT NOT NULL CHECK(length(event_sha256) = 64),
                        created_at_ns INTEGER NOT NULL CHECK(created_at_ns > 0),
                        UNIQUE(portfolio_ref, group_id, revision),
                        FOREIGN KEY(portfolio_ref, group_id)
                            REFERENCES order_group_snapshots(portfolio_ref, group_id)
                    );
                    CREATE INDEX IF NOT EXISTS order_group_events_group_sequence
                        ON order_group_events(portfolio_ref, group_id, sequence);
                    CREATE INDEX IF NOT EXISTS order_group_snapshots_state
                        ON order_group_snapshots(state, updated_at_ns);
                    CREATE TRIGGER IF NOT EXISTS order_group_events_immutable_update
                    BEFORE UPDATE ON order_group_events
                    BEGIN
                        SELECT RAISE(ABORT, 'order group events are immutable');
                    END;
                    CREATE TRIGGER IF NOT EXISTS order_group_events_immutable_delete
                    BEFORE DELETE ON order_group_events
                    BEGIN
                        SELECT RAISE(ABORT, 'order group events are immutable');
                    END;
                    """
                )
                row = self._connection.execute(
                    "SELECT schema_version FROM execution_group_meta WHERE singleton = 1"
                ).fetchone()
                if row is None:
                    self._connection.execute(
                        "INSERT INTO execution_group_meta(singleton, schema_version) VALUES(1, ?)",
                        (self._SCHEMA_VERSION,),
                    )
                elif int(row["schema_version"]) != self._SCHEMA_VERSION:
                    raise DurableStoreError("unsupported group journal schema")
            except sqlite3.Error as error:
                raise DurableStoreError("unable to initialize group journal") from error

    def create_or_get(
        self, request: GroupOrderIntent, *, now_ns: int | None = None
    ) -> GroupOrderSnapshot:
        if type(request) is not GroupOrderIntent:
            raise ContractValidationError("normalized group order intent is required")
        created_at_ns = time.time_ns() if now_ns is None else now_ns
        if type(created_at_ns) is not int or created_at_ns <= 0:
            raise ContractValidationError("invalid group creation time")
        reservations = tuple(
            GroupAccountReservationSnapshot(
                group_id=request.group_id,
                account_ref=account_ref,
                scope_fingerprint="0" * 64,
                phase="NOT_STARTED",
            )
            for account_ref in request.account_refs
        )
        # The zero fingerprint is only a placeholder in the request-independent
        # initial state. `bind_account_scopes` replaces it before any RPC.
        legs = tuple(GroupLegProgress(leg_id=plan.leg_id) for plan in request.legs)
        initial = GroupOrderSnapshot(
            request=request,
            state=GroupOrderState.PREPARING,
            revision=0,
            reservations=reservations,
            legs=legs,
            residual_base_units=None,
            error_code=None,
            created_at_ns=created_at_ns,
            updated_at_ns=created_at_ns,
        )
        request_json = canonical_json(request.to_payload())
        snapshot_json = canonical_json(initial.to_payload())
        event_payload = {
            "request_fingerprint": request.fingerprint,
            "account_refs": list(request.account_refs),
            "leg_ids": [leg.leg_id for leg in request.legs],
            "deadline_ns": request.deadline_ns,
        }
        self._validate_event("GROUP_ACCEPTED", event_payload, created_at_ns)
        with self._transaction() as cursor:
            row = cursor.execute(
                """SELECT * FROM order_group_snapshots
                   WHERE portfolio_ref = ? AND group_id = ?""",
                (request.portfolio_ref, request.group_id),
            ).fetchone()
            if row is not None:
                existing = self._snapshot_from_row(row)
                if existing.request.fingerprint != request.fingerprint:
                    raise IntentConflictError()
                return existing
            cursor.execute(
                """INSERT INTO order_group_snapshots(
                       portfolio_ref, group_id, request_fingerprint, request_json,
                       snapshot_json, snapshot_sha256, state, revision, deadline_ns,
                       created_at_ns, updated_at_ns
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    request.portfolio_ref,
                    request.group_id,
                    request.fingerprint,
                    request_json,
                    snapshot_json,
                    payload_sha256(initial.to_payload()),
                    initial.state.value,
                    initial.revision,
                    request.deadline_ns,
                    created_at_ns,
                    created_at_ns,
                ),
            )
            self._insert_event(
                cursor,
                request.portfolio_ref,
                request.group_id,
                initial.revision,
                "GROUP_ACCEPTED",
                event_payload,
                created_at_ns,
            )
            return initial

    def bind_account_scopes(
        self,
        snapshot: GroupOrderSnapshot,
        scope_fingerprints: Mapping[str, str],
        *,
        now_ns: int | None = None,
    ) -> GroupOrderSnapshot:
        """Bind configured account-target fingerprints before first reserve."""

        if set(scope_fingerprints) != set(snapshot.request.account_refs):
            raise ContractValidationError("group account target set is incomplete")
        updated_reservations: list[GroupAccountReservationSnapshot] = []
        for reservation in snapshot.reservations:
            fingerprint = scope_fingerprints[reservation.account_ref]
            if (
                type(fingerprint) is not str
                or len(fingerprint) != 64
                or any(char not in "0123456789abcdef" for char in fingerprint)
            ):
                raise ContractValidationError("invalid account target fingerprint")
            if reservation.scope_fingerprint not in {"0" * 64, fingerprint}:
                raise IntentConflictError()
            updated_reservations.append(replace(reservation, scope_fingerprint=fingerprint))
        if tuple(updated_reservations) == snapshot.reservations:
            return snapshot
        return self.update(
            snapshot,
            event_type="ACCOUNT_TARGETS_BOUND",
            event_payload={"phase": "account_targets_bound"},
            reservations=tuple(updated_reservations),
            now_ns=now_ns,
        )

    def update(
        self,
        snapshot: GroupOrderSnapshot,
        *,
        event_type: str,
        event_payload: Mapping[str, Any],
        state: GroupOrderState | None = None,
        reservations: tuple[GroupAccountReservationSnapshot, ...] | None = None,
        legs: tuple[GroupLegProgress, ...] | None = None,
        residual_base_units: Any = ...,
        error_code: str | None | Any = ...,
        now_ns: int | None = None,
    ) -> GroupOrderSnapshot:
        if type(snapshot) is not GroupOrderSnapshot:
            raise ContractValidationError("group snapshot is required")
        if event_type not in _EVENT_TYPE:
            raise ContractValidationError("unsupported group event type")
        occurred_at_ns = time.time_ns() if now_ns is None else now_ns
        if type(occurred_at_ns) is not int or occurred_at_ns <= 0:
            raise ContractValidationError("invalid group event time")
        target_state = snapshot.state if state is None else GroupOrderState(state)
        if (
            target_state != snapshot.state
            and target_state not in _ALLOWED_STATE_TRANSITIONS[snapshot.state]
        ):
            raise ContractValidationError("invalid group state transition")
        new_snapshot = replace(
            snapshot,
            state=target_state,
            revision=snapshot.revision + 1,
            reservations=snapshot.reservations if reservations is None else reservations,
            legs=snapshot.legs if legs is None else legs,
            residual_base_units=(
                snapshot.residual_base_units if residual_base_units is ... else residual_base_units
            ),
            error_code=snapshot.error_code if error_code is ... else error_code,
            updated_at_ns=occurred_at_ns,
        )
        self._validate_event(event_type, event_payload, occurred_at_ns)
        request_json = canonical_json(new_snapshot.request.to_payload())
        snapshot_payload = new_snapshot.to_payload()
        snapshot_json = canonical_json(snapshot_payload)
        with self._transaction() as cursor:
            row = cursor.execute(
                """SELECT * FROM order_group_snapshots
                   WHERE portfolio_ref = ? AND group_id = ?""",
                (snapshot.request.portfolio_ref, snapshot.request.group_id),
            ).fetchone()
            if row is None:
                raise DurableStoreError("group record disappeared")
            persisted = self._snapshot_from_row(row)
            if (
                int(row["revision"]) != snapshot.revision
                or persisted.request.fingerprint != snapshot.request.fingerprint
            ):
                raise IntentConflictError()
            cursor.execute(
                """UPDATE order_group_snapshots
                   SET request_json = ?, snapshot_json = ?, snapshot_sha256 = ?, state = ?,
                       revision = ?, updated_at_ns = ?
                   WHERE portfolio_ref = ? AND group_id = ? AND revision = ?""",
                (
                    request_json,
                    snapshot_json,
                    payload_sha256(snapshot_payload),
                    target_state.value,
                    new_snapshot.revision,
                    occurred_at_ns,
                    snapshot.request.portfolio_ref,
                    snapshot.request.group_id,
                    snapshot.revision,
                ),
            )
            if cursor.rowcount != 1:
                raise IntentConflictError()
            self._insert_event(
                cursor,
                snapshot.request.portfolio_ref,
                snapshot.request.group_id,
                new_snapshot.revision,
                event_type,
                event_payload,
                occurred_at_ns,
            )
        return new_snapshot

    def get(self, portfolio_ref: str, group_id: str) -> GroupOrderSnapshot | None:
        with self._lock:
            try:
                row = self._connection.execute(
                    """SELECT * FROM order_group_snapshots
                       WHERE portfolio_ref = ? AND group_id = ?""",
                    (portfolio_ref, group_id),
                ).fetchone()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read group journal") from error
        return None if row is None else self._snapshot_from_row(row)

    def list_recoverable(self) -> tuple[GroupOrderSnapshot, ...]:
        states = tuple(state.value for state in GroupOrderState if state not in _TERMINAL_STATES)
        placeholders = ",".join("?" for _ in states)
        with self._lock:
            try:
                rows = self._connection.execute(
                    f"""SELECT * FROM order_group_snapshots
                        WHERE state IN ({placeholders})
                        ORDER BY created_at_ns, portfolio_ref, group_id""",  # noqa: S608
                    states,
                ).fetchall()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to list recoverable groups") from error
        return tuple(self._snapshot_from_row(row) for row in rows)

    def events(
        self, portfolio_ref: str, group_id: str, *, after_sequence: int = 0
    ) -> tuple[GroupOrderEvent, ...]:
        if type(after_sequence) is not int or after_sequence < 0:
            raise ContractValidationError("invalid event cursor")
        with self._lock:
            try:
                rows = self._connection.execute(
                    """SELECT sequence, group_id, event_type, payload_json,
                              event_sha256, created_at_ns
                       FROM order_group_events
                       WHERE portfolio_ref = ? AND group_id = ? AND sequence > ?
                       ORDER BY sequence""",
                    (portfolio_ref, group_id, after_sequence),
                ).fetchall()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read group events") from error
        result: list[GroupOrderEvent] = []
        for row in rows:
            try:
                payload_json = str(row["payload_json"])
                payload = json.loads(payload_json)
                event_type = str(row["event_type"])
                created_at_ns = int(row["created_at_ns"])
                expected = payload_sha256(
                    {
                        "group_id": group_id,
                        "event_type": event_type,
                        "payload": payload,
                        "created_at_ns": created_at_ns,
                    }
                )
                if canonical_json(payload) != payload_json or expected != str(row["event_sha256"]):
                    raise ValueError("event digest mismatch")
                result.append(
                    GroupOrderEvent(
                        sequence=int(row["sequence"]),
                        group_id=group_id,
                        event_type=event_type,
                        created_at_ns=created_at_ns,
                        payload=payload,
                    )
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                raise DurableStoreError("stored group event is unreadable") from None
        return tuple(result)

    def close(self) -> None:
        with self._lock:
            try:
                self._connection.close()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to close group journal") from error

    def _snapshot_from_row(self, row: sqlite3.Row) -> GroupOrderSnapshot:
        try:
            request_json = str(row["request_json"])
            snapshot_json = str(row["snapshot_json"])
            request_payload = json.loads(request_json)
            snapshot_payload = json.loads(snapshot_json)
            request = GroupOrderIntent.from_payload(request_payload)
            snapshot = GroupOrderSnapshot.from_payload(snapshot_payload)
            if (
                canonical_json(request_payload) != request_json
                or request.fingerprint != str(row["request_fingerprint"])
                or request.fingerprint != snapshot.request.fingerprint
                or canonical_json(snapshot_payload) != snapshot_json
                or payload_sha256(snapshot_payload) != str(row["snapshot_sha256"])
                or snapshot.state.value != str(row["state"])
                or snapshot.revision != int(row["revision"])
                or request.deadline_ns != int(row["deadline_ns"])
                or snapshot.created_at_ns != int(row["created_at_ns"])
                or snapshot.updated_at_ns != int(row["updated_at_ns"])
            ):
                raise ValueError("group snapshot digest mismatch")
            return snapshot
        except (ContractValidationError, TypeError, ValueError, json.JSONDecodeError):
            raise DurableStoreError("stored group snapshot is unreadable") from None

    @staticmethod
    def _validate_event(event_type: str, payload: Mapping[str, Any], created_at_ns: int) -> None:
        if event_type not in _EVENT_TYPE or not isinstance(payload, Mapping):
            raise ContractValidationError("invalid group event")
        if type(created_at_ns) is not int or created_at_ns <= 0:
            raise ContractValidationError("invalid group event time")
        try:
            canonical_json(dict(payload))
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid group event payload") from error

    @staticmethod
    def _insert_event(
        cursor: sqlite3.Cursor,
        portfolio_ref: str,
        group_id: str,
        revision: int,
        event_type: str,
        payload: Mapping[str, Any],
        created_at_ns: int,
    ) -> None:
        payload_json = canonical_json(dict(payload))
        digest = payload_sha256(
            {
                "group_id": group_id,
                "event_type": event_type,
                "payload": dict(payload),
                "created_at_ns": created_at_ns,
            }
        )
        cursor.execute(
            """INSERT INTO order_group_events(
                   portfolio_ref, group_id, revision, event_type, payload_json,
                   event_sha256, created_at_ns
               ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                portfolio_ref,
                group_id,
                revision,
                event_type,
                payload_json,
                digest,
                created_at_ns,
            ),
        )

    class _Transaction:
        def __init__(self, store: SqliteOrderGroupStore) -> None:
            self._store = store
            self._cursor: sqlite3.Cursor | None = None

        def __enter__(self) -> sqlite3.Cursor:
            self._store._lock.acquire()
            try:
                cursor = self._store._connection.cursor()
                cursor.execute("BEGIN IMMEDIATE")
                self._cursor = cursor
                return cursor
            except sqlite3.Error as error:
                self._store._lock.release()
                raise DurableStoreError("unable to begin group journal transaction") from error

        def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
            assert self._cursor is not None
            try:
                self._cursor.execute("ROLLBACK" if exc_type else "COMMIT")
            except sqlite3.Error as error:
                raise DurableStoreError("group journal transaction failed") from error
            finally:
                self._cursor.close()
                self._store._lock.release()
            return False

    def _transaction(self) -> SqliteOrderGroupStore._Transaction:
        return self._Transaction(self)


__all__ = ["SqliteOrderGroupStore"]
