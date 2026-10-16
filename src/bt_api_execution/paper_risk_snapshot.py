"""Durable, owner-fenced Risk mapping snapshots for managed Paper orders.

The snapshot is written into the same SQLite file as the immutable Execution
intent. It preserves the reserve-time Risk mapping and quote across a service
restart; it is never an authority by itself. Every read/write also verifies
the exact current Execution writer lease and the corresponding durable order
state.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from time import time_ns
from typing import Any

from .contracts import ExecutionScope, OrderIntent, canonical_json
from .errors import ContractValidationError
from .store import SqliteExecutionStore, WriterLease


class PaperRiskSnapshotStoreError(ValueError):
    """Finite fail-closed error for Paper admission snapshot persistence."""

    def __init__(self, code: str) -> None:
        if type(code) is not str or not code.startswith("PAPER_ADMISSION_"):
            raise ValueError("invalid Paper admission snapshot error code")
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class PaperRiskAdmissionSnapshotV1:
    """Exact immutable reserve-time mapping, quote and trusted composition."""

    body: Mapping[str, Any]
    snapshot_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.body, Mapping):
            raise ContractValidationError("invalid Paper Risk snapshot body")
        if (
            type(self.snapshot_sha256) is not str
            or len(self.snapshot_sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.snapshot_sha256)
        ):
            raise ContractValidationError("invalid Paper Risk snapshot digest")
        actual = hashlib.sha256(canonical_json(dict(self.body)).encode("utf-8")).hexdigest()
        if actual != self.snapshot_sha256:
            raise ContractValidationError("Paper Risk snapshot digest mismatch")


class PaperRiskAdmissionSnapshotStore:
    """Append-only Paper/Risk snapshots fenced by the live Execution lease."""

    _TABLE = "execution_paper_risk_admission_snapshots"
    _SELECT_SQL = (
        "SELECT intent_fingerprint, snapshot_sha256, snapshot_json "
        "FROM execution_paper_risk_admission_snapshots WHERE scope_key=? AND intent_id=?"
    )
    _INSERT_SQL = (
        "INSERT INTO execution_paper_risk_admission_snapshots "
        "(scope_key, intent_id, intent_fingerprint, snapshot_sha256, snapshot_json, created_at_ns) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )

    def __init__(self, store: SqliteExecutionStore, scope: ExecutionScope) -> None:
        if type(store) is not SqliteExecutionStore or type(scope) is not ExecutionScope:
            raise ContractValidationError("Paper Risk snapshot requires a typed Execution store")
        path = store.path
        if type(path) is not str or not path or path == ":memory:":
            raise ContractValidationError("Paper Risk snapshots require a durable SQLite file")
        self._path = path
        self._store = store
        self._scope = scope

    def is_bound_to(self, store: object, scope: object) -> bool:
        return store is self._store and scope is self._scope

    def load_for_reservation(
        self,
        intent: OrderIntent,
        writer_lease: WriterLease,
    ) -> PaperRiskAdmissionSnapshotV1 | None:
        return self._load(intent, writer_lease, expected_state="PENDING_ADMISSION")

    def create_for_reservation(
        self,
        intent: OrderIntent,
        writer_lease: WriterLease,
        body: Mapping[str, Any],
    ) -> PaperRiskAdmissionSnapshotV1:
        return self._create(
            intent,
            writer_lease,
            body,
            expected_state="PENDING_ADMISSION",
        )

    def load_for_claim(
        self,
        intent: OrderIntent,
        writer_lease: WriterLease,
        permit_reference: str,
    ) -> PaperRiskAdmissionSnapshotV1:
        snapshot = self._load(
            intent,
            writer_lease,
            expected_state="DISPATCHING",
            permit_reference=permit_reference,
        )
        if snapshot is None:
            raise PaperRiskSnapshotStoreError("PAPER_ADMISSION_SNAPSHOT_MISSING")
        return snapshot

    def _load(
        self,
        intent: OrderIntent,
        writer_lease: WriterLease,
        *,
        expected_state: str,
        permit_reference: str | None = None,
    ) -> PaperRiskAdmissionSnapshotV1 | None:
        with self._transaction(intent, writer_lease, expected_state, permit_reference) as cursor:
            row = cursor.execute(
                self._SELECT_SQL,
                (self._scope.key, intent.intent_id),
            ).fetchone()
            if row is None:
                return None
            return self._decode_row(row, intent)

    def _create(
        self,
        intent: OrderIntent,
        writer_lease: WriterLease,
        body: Mapping[str, Any],
        *,
        expected_state: str,
    ) -> PaperRiskAdmissionSnapshotV1:
        payload = dict(body)
        serialized = canonical_json(payload)
        digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        with self._transaction(intent, writer_lease, expected_state, None) as cursor:
            row = cursor.execute(
                self._SELECT_SQL,
                (self._scope.key, intent.intent_id),
            ).fetchone()
            if row is not None:
                existing = self._decode_row(row, intent)
                if existing.snapshot_sha256 != digest or canonical_json(dict(existing.body)) != serialized:
                    raise PaperRiskSnapshotStoreError("PAPER_ADMISSION_SNAPSHOT_CONFLICT")
                return existing
            cursor.execute(
                self._INSERT_SQL,
                (
                    self._scope.key,
                    intent.intent_id,
                    intent.fingerprint,
                    digest,
                    serialized,
                    time_ns(),
                ),
            )
        return PaperRiskAdmissionSnapshotV1(payload, digest)

    def _decode_row(self, row: sqlite3.Row, intent: OrderIntent) -> PaperRiskAdmissionSnapshotV1:
        raw = str(row["snapshot_json"])
        try:
            value = json.loads(raw)
            if type(value) is not dict or canonical_json(value) != raw:
                raise ValueError("noncanonical snapshot")
            snapshot = PaperRiskAdmissionSnapshotV1(value, str(row["snapshot_sha256"]))
        except (ContractValidationError, TypeError, ValueError, json.JSONDecodeError):
            raise PaperRiskSnapshotStoreError("PAPER_ADMISSION_SNAPSHOT_CORRUPT") from None
        if (
            row["intent_fingerprint"] != intent.fingerprint
            or value.get("intent_id") != intent.intent_id
            or value.get("execution_intent_fingerprint") != intent.fingerprint
            or value.get("execution_scope_key") != self._scope.key
            or value.get("execution_scope") != self._scope.to_payload()
        ):
            raise PaperRiskSnapshotStoreError("PAPER_ADMISSION_SNAPSHOT_CONFLICT")
        return snapshot

    def _transaction(
        self,
        intent: OrderIntent,
        writer_lease: WriterLease,
        expected_state: str,
        permit_reference: str | None,
    ):
        if (
            type(intent) is not OrderIntent
            or intent.scope != self._scope
            or type(writer_lease) is not WriterLease
            or writer_lease.scope_key != self._scope.account_key
            or not writer_lease.owner_id
            or type(writer_lease.fencing_token) is not int
            or writer_lease.fencing_token <= 0
            or expected_state not in {"PENDING_ADMISSION", "DISPATCHING"}
        ):
            raise PaperRiskSnapshotStoreError("PAPER_ADMISSION_SOURCE_INVALID")
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(self._path, timeout=5.0, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                f"CREATE TABLE IF NOT EXISTS {self._TABLE} ("
                "scope_key TEXT NOT NULL, intent_id TEXT NOT NULL, "
                "intent_fingerprint TEXT NOT NULL, snapshot_sha256 TEXT NOT NULL, "
                "snapshot_json TEXT NOT NULL, created_at_ns INTEGER NOT NULL, "
                "PRIMARY KEY(scope_key, intent_id), "
                "FOREIGN KEY(scope_key, intent_id) REFERENCES execution_records(scope_key, intent_id))"
            )
            now = time_ns()
            lease = connection.execute(
                "SELECT owner_id, fencing_token, expires_at_ns FROM execution_writer_leases "
                "WHERE scope_key=?",
                (self._scope.account_key,),
            ).fetchone()
            if (
                lease is None
                or lease["owner_id"] != writer_lease.owner_id
                or lease["fencing_token"] != writer_lease.fencing_token
                or type(lease["expires_at_ns"]) is not int
                or lease["expires_at_ns"] <= now
            ):
                connection.rollback()
                connection.close()
                raise PaperRiskSnapshotStoreError("PAPER_ADMISSION_WRITER_LEASE_INVALID")
            record = connection.execute(
                "SELECT payload_sha256, payload_json, state, permit_reference, dispatch_attempts "
                "FROM execution_records WHERE scope_key=? AND intent_id=?",
                (self._scope.key, intent.intent_id),
            ).fetchone()
            if (
                record is None
                or record["payload_sha256"] != intent.fingerprint
                or record["payload_json"] != canonical_json(intent.to_payload())
                or record["state"] != expected_state
                or (
                    expected_state == "PENDING_ADMISSION"
                    and record["permit_reference"] is not None
                )
                or (
                    expected_state == "DISPATCHING"
                    and (
                        permit_reference is None
                        or record["permit_reference"] != permit_reference
                        or record["dispatch_attempts"] != 1
                    )
                )
            ):
                connection.rollback()
                connection.close()
                raise PaperRiskSnapshotStoreError("PAPER_ADMISSION_SOURCE_STATE_INVALID")
            return _SqliteSnapshotTransaction(connection)
        except PaperRiskSnapshotStoreError:
            raise
        except (sqlite3.Error, OSError):
            if connection is not None:
                from contextlib import suppress

                with suppress(sqlite3.Error):
                    connection.close()
            raise PaperRiskSnapshotStoreError("PAPER_ADMISSION_SNAPSHOT_STORE_UNAVAILABLE") from None


class _SqliteSnapshotTransaction:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self._cursor = connection.cursor()

    def __enter__(self) -> sqlite3.Cursor:
        return self._cursor

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            if exc_type is None:
                self._connection.commit()
            else:
                self._connection.rollback()
        finally:
            self._cursor.close()
            self._connection.close()


__all__ = (
    "PaperRiskAdmissionSnapshotStore",
    "PaperRiskAdmissionSnapshotV1",
    "PaperRiskSnapshotStoreError",
)
