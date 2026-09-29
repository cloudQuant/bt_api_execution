from __future__ import annotations

import hashlib
import sqlite3

import pytest

from bt_api_execution import (
    ContractValidationError,
    DurableStoreError,
    ExecutionScope,
    InvalidStateTransition,
    SqliteExecutionStore,
)


def _scope(account_label: str = "account.one", *, environment: str = "simulation"):
    account_ref = "ctp-account-ref.v1:" + hashlib.sha256(account_label.encode("ascii")).hexdigest()
    return ExecutionScope("ctp", environment, account_ref, "strategy.one", "20260926")


@pytest.mark.unit
def test_factory_creates_and_reopens_exact_account_identity_through_wal(tmp_path) -> None:
    path = tmp_path / "journals" / "one.sqlite3"
    path.parent.mkdir()
    scope = _scope()
    first = SqliteExecutionStore.open_ctp_account_store(path, scope)
    try:
        assert (
            SqliteExecutionStore.inspect_ctp_account_store_file(path).kind
            == "CTP_EXECUTION_STORE"
        )
        owner = first.acquire_ctp_account_family_owner(scope)
        identity = first.read_ctp_account_store_identity(scope)
        assert identity.ledger_kind == "CTP_EXECUTION_V1"
        assert identity.account_ref == scope.account_ref
        assert identity.family_key == owner.family_key
        assert identity.journal_incarnation_id == first.journal_source_identity()["generation"]
        assert identity.family_owner_state == "ACTIVE"
        assert identity.family_owner_intent_id == owner.owner_intent_id
        assert identity.database_filename == str(path.resolve())
        assert identity.file_device is not None
        assert identity.file_id is not None
        assert (path.with_name(path.name + "-wal")).exists()
        assert (path.with_name(path.name + "-shm")).exists()

        # The second open sees the committed identity in the still-live WAL.
        second = SqliteExecutionStore.open_ctp_account_store(path, scope)
        try:
            reopened_identity = second.read_ctp_account_store_identity(scope)
            assert reopened_identity == identity
        finally:
            second.close()
    finally:
        first.close()


@pytest.mark.unit
def test_factory_rejects_different_account_without_mutating_existing_store(tmp_path) -> None:
    path = tmp_path / "account.sqlite3"
    first_scope = _scope("account.one")
    store = SqliteExecutionStore.open_ctp_account_store(path, first_scope)
    try:
        store.acquire_ctp_account_family_owner(first_scope)
        data_files = [path, path.with_name(path.name + "-wal")]
        before = {candidate: candidate.read_bytes() for candidate in data_files}
        assert path.with_name(path.name + "-shm").exists()
        with pytest.raises(DurableStoreError, match="account identity does not match"):
            SqliteExecutionStore.open_ctp_account_store(path, _scope("account.two"))
        after = {candidate: candidate.read_bytes() for candidate in data_files}
        assert after == before
        assert path.with_name(path.name + "-shm").exists()
    finally:
        store.close()


@pytest.mark.unit
def test_factory_requires_identity_even_for_existing_empty_v20_store(tmp_path) -> None:
    path = tmp_path / "unbound.sqlite3"
    unbound = SqliteExecutionStore(path)
    unbound.close()
    before_tables = sqlite3.connect(path)
    try:
        tables_before = {
            row[0]
            for row in before_tables.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    finally:
        before_tables.close()

    with pytest.raises(DurableStoreError, match="identity is missing or ambiguous"):
        SqliteExecutionStore.open_ctp_account_store(path, _scope())
    after = sqlite3.connect(path)
    try:
        tables_after = {
            row[0]
            for row in after.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        assert tables_after == tables_before
        assert "ctp_account_store_identity" in tables_after
    finally:
        after.close()


@pytest.mark.unit
def test_factory_rejects_malformed_persisted_account_identity_before_writes(tmp_path) -> None:
    path = tmp_path / "malformed-identity.sqlite3"
    store = SqliteExecutionStore.open_ctp_account_store(path, _scope())
    store.close()
    connection = sqlite3.connect(path)
    try:
        trigger_sql = connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='trigger' AND name='ctp_account_store_identity_immutable_update'"
        ).fetchone()[0]
        connection.execute("DROP TRIGGER ctp_account_store_identity_immutable_update")
        connection.execute(
            "UPDATE ctp_account_store_identity SET family_key = ?",
            ("ctp-account-family.v1:" + "0" * 64,),
        )
        connection.execute(trigger_sql)
        connection.commit()
    finally:
        connection.close()
    before = path.read_bytes()
    with pytest.raises(DurableStoreError, match="account identity does not match"):
        SqliteExecutionStore.inspect_ctp_account_store_file(path, _scope())
    assert path.read_bytes() == before


@pytest.mark.unit
def test_factory_rejects_incomplete_v20_schema_before_recreating_guard(tmp_path) -> None:
    path = tmp_path / "missing-guard.sqlite3"
    store = SqliteExecutionStore.open_ctp_account_store(path, _scope())
    store.close()
    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TRIGGER ctp_dispatch_trade_fact_immutable_update")
        connection.commit()
    finally:
        connection.close()
    before = path.read_bytes()

    with pytest.raises(DurableStoreError, match="schema definitions do not match"):
        SqliteExecutionStore.open_ctp_account_store(path, _scope())
    assert path.read_bytes() == before
    check = sqlite3.connect(path)
    try:
        assert check.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' "
            "AND name='ctp_dispatch_trade_fact_immutable_update'"
        ).fetchone()[0] == 0
    finally:
        check.close()


@pytest.mark.unit
@pytest.mark.parametrize("tamper", ["trigger_body", "identity_column"])
def test_factory_rejects_same_name_but_malformed_v20_schema_before_writes(
    tmp_path, tamper
) -> None:
    path = tmp_path / f"malformed-schema-{tamper}.sqlite3"
    store = SqliteExecutionStore.open_ctp_account_store(path, _scope())
    store.close()
    connection = sqlite3.connect(path)
    try:
        if tamper == "trigger_body":
            connection.execute("DROP TRIGGER ctp_dispatch_trade_fact_immutable_update")
            connection.execute(
                "CREATE TRIGGER ctp_dispatch_trade_fact_immutable_update "
                "BEFORE UPDATE ON ctp_dispatch_trade_fact_ledger BEGIN SELECT 1; END"
            )
        else:
            connection.execute(
                "ALTER TABLE ctp_account_store_identity ADD COLUMN unexpected TEXT"
            )
        connection.commit()
    finally:
        connection.close()

    before = path.read_bytes()
    with pytest.raises(DurableStoreError, match="schema definitions do not match"):
        SqliteExecutionStore.inspect_ctp_account_store_file(path, _scope())
    assert path.read_bytes() == before
    with pytest.raises(DurableStoreError, match="schema definitions do not match"):
        SqliteExecutionStore.open_ctp_account_store(path, _scope())
    assert path.read_bytes() == before


@pytest.mark.unit
def test_factory_rejects_v19_without_identity_before_schema_upgrade(tmp_path) -> None:
    path = tmp_path / "old-v19.sqlite3"
    store = SqliteExecutionStore(path)
    store.close()
    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TRIGGER ctp_account_store_identity_immutable_update")
        connection.execute("DROP TRIGGER ctp_account_store_identity_immutable_delete")
        connection.execute("DROP TABLE ctp_account_store_identity")
        connection.execute("UPDATE execution_meta SET value='19' WHERE key='schema_version'")
        connection.commit()
        before_tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    finally:
        connection.close()

    before = path.read_bytes()
    with pytest.raises(DurableStoreError, match="schema is not current"):
        SqliteExecutionStore.open_ctp_account_store(path, _scope())
    assert path.read_bytes() == before
    check = sqlite3.connect(path)
    try:
        assert {
            row[0]
            for row in check.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        } == before_tables
        assert check.execute(
            "SELECT value FROM execution_meta WHERE key='schema_version'"
        ).fetchone()[0] == "19"
    finally:
        check.close()


@pytest.mark.unit
def test_factory_rejects_legacy_and_unknown_schemas_without_adding_tables(tmp_path) -> None:
    missing_path = tmp_path / "missing.sqlite3"
    assert (
        SqliteExecutionStore.inspect_ctp_account_store_file(missing_path).kind == "MISSING"
    )

    legacy_path = tmp_path / "legacy.sqlite3"
    legacy = sqlite3.connect(legacy_path)
    try:
        legacy.execute("CREATE TABLE ctp_sim_orders (client_order_id TEXT PRIMARY KEY)")
        legacy.execute(
            "CREATE TABLE ctp_sim_journal_metadata (singleton INTEGER PRIMARY KEY)"
        )
        legacy.execute("CREATE TABLE ctp_sim_journal_scopes (scope_sha256 TEXT PRIMARY KEY)")
        legacy.commit()
    finally:
        legacy.close()
    legacy_before = legacy_path.read_bytes()
    assert (
        SqliteExecutionStore.inspect_ctp_account_store_file(legacy_path).kind
        == "LEGACY_CTP_JOURNAL"
    )
    with pytest.raises(DurableStoreError, match="legacy CTP execution journal"):
        SqliteExecutionStore.open_ctp_account_store(legacy_path, _scope())
    assert legacy_path.read_bytes() == legacy_before

    unknown_path = tmp_path / "unknown.sqlite3"
    unknown = sqlite3.connect(unknown_path)
    try:
        unknown.execute("CREATE TABLE unrelated_history (value TEXT)")
        unknown.commit()
    finally:
        unknown.close()
    unknown_before = unknown_path.read_bytes()
    with pytest.raises(DurableStoreError, match="schema is unknown"):
        SqliteExecutionStore.open_ctp_account_store(unknown_path, _scope())
    assert unknown_path.read_bytes() == unknown_before


@pytest.mark.unit
def test_factory_inspector_recognizes_only_empty_exact_orders_only_legacy_layout(tmp_path) -> None:
    legacy_columns = """(
        client_order_id TEXT PRIMARY KEY,
        approval_id TEXT NOT NULL UNIQUE,
        request_digest TEXT NOT NULL,
        request_json TEXT NOT NULL,
        state TEXT NOT NULL,
        order_ref TEXT,
        order_sys_id TEXT,
        front_id INTEGER,
        session_id INTEGER,
        status TEXT,
        traded_quantity INTEGER NOT NULL DEFAULT 0,
        position_digest TEXT,
        updated_at REAL NOT NULL
    )"""

    valid_path = tmp_path / "empty-orders-only.sqlite3"
    valid = sqlite3.connect(valid_path)
    try:
        valid.execute(f"CREATE TABLE ctp_sim_orders {legacy_columns}")
        valid.commit()
    finally:
        valid.close()
    assert (
        SqliteExecutionStore.inspect_ctp_account_store_file(valid_path).kind
        == "LEGACY_CTP_JOURNAL"
    )

    scoped_empty_path = tmp_path / "empty-orders-only-with-scope.sqlite3"
    scoped_empty = sqlite3.connect(scoped_empty_path)
    try:
        scoped_columns = legacy_columns.replace(
            "updated_at REAL NOT NULL", "updated_at REAL NOT NULL, execution_scope_sha256 TEXT"
        )
        scoped_empty.execute(f"CREATE TABLE ctp_sim_orders {scoped_columns}")
        scoped_empty.commit()
    finally:
        scoped_empty.close()
    assert (
        SqliteExecutionStore.inspect_ctp_account_store_file(scoped_empty_path).kind
        == "LEGACY_CTP_JOURNAL"
    )

    incomplete_path = tmp_path / "incomplete-orders-only.sqlite3"
    incomplete = sqlite3.connect(incomplete_path)
    try:
        incomplete.execute("CREATE TABLE ctp_sim_orders (client_order_id TEXT PRIMARY KEY)")
        incomplete.commit()
    finally:
        incomplete.close()
    with pytest.raises(DurableStoreError, match="orders-only legacy CTP journal"):
        SqliteExecutionStore.inspect_ctp_account_store_file(incomplete_path)

    extra_column_path = tmp_path / "extra-column-orders-only.sqlite3"
    extra_column = sqlite3.connect(extra_column_path)
    try:
        extra_column.execute(
            f"CREATE TABLE ctp_sim_orders {legacy_columns[:-1]}, surprise TEXT)"
        )
        extra_column.commit()
    finally:
        extra_column.close()
    with pytest.raises(DurableStoreError, match="orders-only legacy CTP journal"):
        SqliteExecutionStore.inspect_ctp_account_store_file(extra_column_path)

    extra_object_path = tmp_path / "extra-object-orders-only.sqlite3"
    extra_object = sqlite3.connect(extra_object_path)
    try:
        extra_object.execute(f"CREATE TABLE ctp_sim_orders {legacy_columns}")
        extra_object.execute(
            "CREATE VIEW ctp_sim_orders_view AS SELECT client_order_id FROM ctp_sim_orders"
        )
        extra_object.commit()
    finally:
        extra_object.close()
    with pytest.raises(DurableStoreError, match="unsupported schema objects"):
        SqliteExecutionStore.inspect_ctp_account_store_file(extra_object_path)

    populated_path = tmp_path / "populated-orders-only.sqlite3"
    populated = sqlite3.connect(populated_path)
    try:
        populated.execute(f"CREATE TABLE ctp_sim_orders {legacy_columns}")
        populated.execute(
            "INSERT INTO ctp_sim_orders "
            "(client_order_id, approval_id, request_digest, request_json, state, updated_at) "
            "VALUES ('order.1', 'approval.1', 'digest', '{}', 'UNKNOWN', 1.0)"
        )
        populated.commit()
    finally:
        populated.close()
    with pytest.raises(DurableStoreError, match="orders-only legacy CTP journal"):
        SqliteExecutionStore.inspect_ctp_account_store_file(populated_path)

    for table in ("ctp_sim_journal_metadata", "ctp_sim_journal_scopes"):
        partial_path = tmp_path / f"partial-{table}.sqlite3"
        partial = sqlite3.connect(partial_path)
        try:
            partial.execute(f"CREATE TABLE {table} (value TEXT)")
            partial.commit()
        finally:
            partial.close()
        with pytest.raises(DurableStoreError, match="partial or ambiguous"):
            SqliteExecutionStore.inspect_ctp_account_store_file(partial_path)


@pytest.mark.unit
def test_factory_rejects_existing_zero_byte_file_and_sidecar_only_path(tmp_path) -> None:
    empty_path = tmp_path / "empty.sqlite3"
    empty_path.write_bytes(b"")
    with pytest.raises(DurableStoreError, match="schema is unknown"):
        SqliteExecutionStore.open_ctp_account_store(empty_path, _scope())
    assert empty_path.read_bytes() == b""

    sidecar_path = tmp_path / "orphan.sqlite3"
    wal_path = sidecar_path.with_name(sidecar_path.name + "-wal")
    wal_path.write_bytes(b"unowned wal")
    with pytest.raises(DurableStoreError, match="sidecars exist without a database"):
        SqliteExecutionStore.open_ctp_account_store(sidecar_path, _scope())
    assert not sidecar_path.exists()
    assert wal_path.read_bytes() == b"unowned wal"


@pytest.mark.unit
def test_factory_rejects_hot_journal_before_opening_existing_store(tmp_path) -> None:
    path = tmp_path / "journal.sqlite3"
    store = SqliteExecutionStore.open_ctp_account_store(path, _scope())
    store.close()
    journal_path = path.with_name(path.name + "-journal")
    journal_path.write_bytes(b"uncertain journal")
    before = path.read_bytes()
    with pytest.raises(DurableStoreError, match="uncertain SQLite journal state"):
        SqliteExecutionStore.open_ctp_account_store(path, _scope())
    assert path.read_bytes() == before
    assert journal_path.read_bytes() == b"uncertain journal"
    assert not path.with_name(path.name + "-wal").exists()
    assert not path.with_name(path.name + "-shm").exists()


@pytest.mark.unit
@pytest.mark.parametrize("invalid_path", [":memory:", "file:memorydb?mode=memory&cache=shared", "relative.sqlite3"])
def test_factory_rejects_memory_uri_and_relative_paths(tmp_path, invalid_path) -> None:
    with pytest.raises(ContractValidationError):
        SqliteExecutionStore.open_ctp_account_store(invalid_path, _scope())


@pytest.mark.unit
def test_same_account_reference_does_not_bypass_persistent_environment_owner(tmp_path) -> None:
    path = tmp_path / "same-account.sqlite3"
    simulation_scope = _scope(environment="simulation")
    live_scope = _scope(environment="live")
    store = SqliteExecutionStore.open_ctp_account_store(path, simulation_scope)
    try:
        owner = store.acquire_ctp_account_family_owner(simulation_scope)
        live_identity = store.read_ctp_account_store_identity(live_scope)
        assert live_identity.family_key == owner.family_key
        with pytest.raises(InvalidStateTransition, match="persistent owner"):
            store.acquire_ctp_account_family_owner(live_scope)
    finally:
        store.close()
