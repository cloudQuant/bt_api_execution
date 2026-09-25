from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256

import pytest

from bt_api_execution import (
    ContractValidationError,
    CtpOrderRefSeedProof,
    DurableStoreError,
    ExecutionScope,
    IntentConflictError,
    SqliteExecutionStore,
    payload_sha256,
)


@pytest.mark.unit
def test_older_scope_ambiguous_schema_is_rejected_before_use(tmp_path) -> None:
    path = tmp_path / "execution.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE execution_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        connection.execute("INSERT INTO execution_meta(key, value) VALUES ('schema_version', '1')")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(DurableStoreError, match="unsupported execution store schema"):
        SqliteExecutionStore(path)


@pytest.mark.unit
def test_v2_execution_store_adds_nullable_cumulative_commission_column(tmp_path) -> None:
    path = tmp_path / "execution.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE execution_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO execution_meta(key, value) VALUES ('schema_version', '2');
            CREATE TABLE execution_records (
                scope_key TEXT NOT NULL,
                intent_id TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                state TEXT NOT NULL,
                provider_order_id TEXT,
                filled_quantity TEXT NOT NULL,
                average_price TEXT,
                permit_reference TEXT,
                dispatch_attempts INTEGER NOT NULL DEFAULT 0,
                unknown_reason TEXT,
                review_required INTEGER NOT NULL DEFAULT 0,
                created_at_ns INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL,
                PRIMARY KEY(scope_key, intent_id)
            );
            """
        )
        connection.commit()
    finally:
        connection.close()

    store = SqliteExecutionStore(path)
    try:
        columns = {
            row["name"]
            for row in store._connection.execute("PRAGMA table_info(execution_records)").fetchall()
        }
        version = store._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()["value"]
        assert "cumulative_commission" in columns
        assert version == "9"
    finally:
        store.close()


@pytest.mark.unit
def test_v3_execution_store_adds_ctp_order_identity_reservations(tmp_path) -> None:
    path = tmp_path / "execution.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE execution_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO execution_meta(key, value) VALUES ('schema_version', '3');
            """
        )
        connection.commit()
    finally:
        connection.close()

    store = SqliteExecutionStore(path)
    try:
        version = store._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()["value"]
        tables = {
            row["name"]
            for row in store._connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        assert version == "9"
        assert "ctp_order_identity_reservations" in tables
        assert "ctp_dispatch_commands" in tables
        assert "ctp_dispatch_authority_uses" in tables
    finally:
        store.close()


def _ctp_scope(*, day: str = "20260925", strategy: str = "strategy.demo") -> ExecutionScope:
    return ExecutionScope("CTP", "simulation", "acct_ref", strategy, trading_day=day)


def _runtime_order_id(token: str) -> str:
    return "bt-managed-v1:" + sha256(token.encode("ascii")).hexdigest()


@pytest.mark.unit
def test_ctp_order_ref_seed_proof_rejects_legacy_four_field_constructor() -> None:
    with pytest.raises(TypeError):
        CtpOrderRefSeedProof(
            trading_day="20260925",
            native_max_order_ref="000000000000",
            legacy_ledger_max_order_ref="000000000000",
            legacy_ledger_sha256="0" * 64,
        )


def _seed_proof(scope: ExecutionScope, session: str = "test-seed-session") -> CtpOrderRefSeedProof:
    sources = (
        ("backtrader_prototype", sha256(b"empty backtrader prototype").hexdigest()),
        ("sdk_jsonl", sha256(b"empty sdk jsonl").hexdigest()),
    )
    return CtpOrderRefSeedProof(
        trading_day=scope.trading_day,
        native_max_order_ref="000000000000",
        legacy_ledger_max_order_ref="000000000000",
        legacy_ledger_sha256=payload_sha256(dict(sources)),
        account_key=scope.account_key,
        scope_key=scope.key,
        session_generation_id=session,
        native_front_id=4,
        native_session_id=91,
        existing_native_order_refs=(),
        legacy_source_sha256=sources,
        legacy_mappings=(),
    )


def _reserve_seeded(store, scope, managed_intent_id, runtime_order_id, *, session="test-seed-session"):
    lease = store.acquire_or_renew_lease(scope, "execution-store-test", ttl_ns=30_000_000_000)
    return store.seed_ctp_order_ref_and_reserve_identity(
        scope,
        _seed_proof(scope, session),
        managed_intent_id,
        runtime_order_id,
        writer_lease=lease,
    )


@pytest.mark.unit
def test_ctp_order_identity_reservation_is_idempotent_and_survives_restart(tmp_path) -> None:
    path = tmp_path / "execution.sqlite3"
    scope = _ctp_scope()
    runtime_id = _runtime_order_id("first")
    store = SqliteExecutionStore(path)
    try:
        first = _reserve_seeded(store, scope, "intent-1", runtime_id)
        assert first.order_ref == "000000000001"
        assert first.account_key == scope.account_key
        assert first.trading_day == "20260925"
        assert first.scope_key == scope.key
        assert _reserve_seeded(store, scope, "intent-1", runtime_id) == first
    finally:
        store.close()

    reopened = SqliteExecutionStore(path)
    try:
        assert reopened.read_ctp_order_identity(scope, "intent-1") == first
        second = _reserve_seeded(reopened, scope, "intent-2", _runtime_order_id("second"))
        assert second.order_ref == "000000000002"

        # A new trading day and another strategy share the same account-level
        # allocation history; previously committed refs are never reused.
        next_day = _ctp_scope(day="20260926", strategy="strategy.other")
        third = _reserve_seeded(
            reopened,
            next_day,
            "intent-1",
            _runtime_order_id("next-day"),
            session="next-day-session",
        )
        assert third.order_ref == "000000000003"
        assert third.account_key == first.account_key
        assert reopened.read_ctp_order_identity(scope, "missing") is None
    finally:
        reopened.close()


@pytest.mark.unit
def test_ctp_order_identity_conflicts_do_not_consume_a_reference(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _ctp_scope()
    first_runtime_id = _runtime_order_id("first")
    try:
        first = _reserve_seeded(store, scope, "intent-1", first_runtime_id)
        with pytest.raises(IntentConflictError, match="intent identity conflicts"):
            _reserve_seeded(store, scope, "intent-1", _runtime_order_id("changed"))
        with pytest.raises(IntentConflictError, match="runtime identity is already reserved"):
            _reserve_seeded(store, scope, "intent-2", first_runtime_id)
        second = _reserve_seeded(store, scope, "intent-2", _runtime_order_id("second"))
        assert first.order_ref == "000000000001"
        assert second.order_ref == "000000000002"
    finally:
        store.close()


@pytest.mark.unit
def test_new_orderref_reservation_rejects_an_unseeded_empty_store(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _ctp_scope()
    try:
        with pytest.raises(ContractValidationError, match="cutover is required"):
            store.reserve_ctp_order_identity(
                scope, "intent-1", _runtime_order_id("no-implicit-one")
            )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_identity_reservations"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("scope", "intent_id", "runtime_id"),
    [
        (_ctp_scope(day="2026-09-25"), "intent-1", _runtime_order_id("bad-day")),
        (_ctp_scope(day="20260230"), "intent-1", _runtime_order_id("invalid-day")),
        (
            ExecutionScope("FAKE", "simulation", "acct_ref", "strategy.demo", "20260925"),
            "intent-1",
            _runtime_order_id("wrong-provider"),
        ),
        (_ctp_scope(), "intent-1", "123456789012"),
    ],
)
def test_ctp_order_identity_rejects_incomplete_or_invalid_scope(
    tmp_path, scope, intent_id, runtime_id
) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    try:
        with pytest.raises(ContractValidationError):
            store.reserve_ctp_order_identity(scope, intent_id, runtime_id)
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_order_identity_reservations"
        ).fetchone()[0] == 0
    finally:
        store.close()


@pytest.mark.unit
def test_ctp_order_identity_sqlite_rollback_does_not_leave_a_burned_ref(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _ctp_scope()
    try:
        store._connection.execute(
            """
            CREATE TRIGGER reject_ctp_identity_insert
            BEFORE INSERT ON ctp_order_identity_reservations
            BEGIN
                SELECT RAISE(ABORT, 'injected reservation failure');
            END;
            """
        )
        with pytest.raises(DurableStoreError, match="unable to persist CTP order identity"):
            _reserve_seeded(store, scope, "intent-1", _runtime_order_id("failed"))
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_order_identity_reservations"
        ).fetchone()[0] == 0

        store._connection.execute("DROP TRIGGER reject_ctp_identity_insert")
        reservation = _reserve_seeded(store, scope, "intent-1", _runtime_order_id("retry"))
        assert reservation.order_ref == "000000000001"
    finally:
        store.close()


@pytest.mark.unit
def test_ctp_order_identity_allocation_serializes_independent_store_connections(tmp_path) -> None:
    path = tmp_path / "execution.sqlite3"
    stores = [SqliteExecutionStore(path) for _ in range(8)]
    scope = _ctp_scope()
    lease = stores[0].acquire_or_renew_lease(
        scope, "execution-store-concurrency-test", ttl_ns=30_000_000_000
    )
    proof = _seed_proof(scope)
    try:
        def reserve(index: int) -> str:
            result = stores[index].seed_ctp_order_ref_and_reserve_identity(
                scope,
                proof,
                f"intent-{index}",
                _runtime_order_id(f"concurrent-{index}"),
                writer_lease=lease,
            )
            return result.order_ref

        with ThreadPoolExecutor(max_workers=len(stores)) as pool:
            order_refs = tuple(pool.map(reserve, range(len(stores))))
        assert sorted(order_refs) == [f"{value:012d}" for value in range(1, 9)]
    finally:
        for store in stores:
            store.close()
