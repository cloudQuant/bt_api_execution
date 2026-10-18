from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace

import pytest

from bt_api_execution import (
    ContractValidationError,
    CtpOrderRefLegacyMapping,
    CtpOrderRefSeedProof,
    ExecutionScope,
    InvalidStateTransition,
    SqliteExecutionStore,
    payload_sha256,
)


_TRADING_DAY = "20260925"
_SESSION_GENERATION = "scope-set-session-generation"


def _account_ref(label: str = "callback-owner-scope-set") -> str:
    return "ctp-account-ref.v1:" + hashlib.sha256(label.encode("ascii")).hexdigest()


def _scope(
    strategy_id: str = "strategy.owner.source",
    *,
    account_ref: str | None = None,
    provider: str = "ctp",
    environment: str = "simulation",
    trading_day: str | None = _TRADING_DAY,
) -> ExecutionScope:
    return ExecutionScope(
        provider,
        environment,
        account_ref or _account_ref(),
        strategy_id,
        trading_day,
    )


def _writer_lease(store: SqliteExecutionStore, scope: ExecutionScope):
    family_owner = store.acquire_ctp_account_family_owner(scope)
    return store.acquire_or_renew_lease(
        scope,
        "callback-owner-scope-set-test",
        ttl_ns=30_000_000_000,
        ctp_account_family_owner=family_owner,
    )


def _runtime_order_id(value: str) -> str:
    return "bt-managed-v1:" + hashlib.sha256(value.encode("ascii")).hexdigest()


def _seed_proof(scope: ExecutionScope) -> CtpOrderRefSeedProof:
    legacy_scope = ExecutionScope(
        "ctp", "simulation", scope.account_ref, "strategy.legacy", "20260924"
    )
    source_digests = (
        ("backtrader_prototype", hashlib.sha256(b"prototype fixture").hexdigest()),
        ("sdk_jsonl", hashlib.sha256(b"empty journal fixture").hexdigest()),
    )
    mappings = (
        CtpOrderRefLegacyMapping(
            source_name="backtrader_prototype",
            account_key=scope.account_key,
            trading_day="20260924",
            scope_key=legacy_scope.key,
            managed_intent_id="legacy-managed-intent",
            runtime_order_id="legacy-runtime-order",
            order_ref="000000000012",
        ),
    )
    return CtpOrderRefSeedProof(
        trading_day=scope.trading_day,
        native_max_order_ref="000000000010",
        legacy_ledger_max_order_ref="000000000012",
        legacy_ledger_sha256=payload_sha256(dict(source_digests)),
        account_key=scope.account_key,
        scope_key=scope.key,
        session_generation_id=_SESSION_GENERATION,
        native_front_id=4,
        native_session_id=91,
        existing_native_order_refs=("000000000009", "000000000010"),
        legacy_source_sha256=source_digests,
        legacy_mappings=mappings,
    )


def _insert_ready_submit(store: SqliteExecutionStore, scope: ExecutionScope, lease) -> str:
    intent_id = "intent-ready-before-callback-owner"
    reservation = store.seed_ctp_order_ref_and_reserve_identity(
        scope,
        _seed_proof(scope),
        intent_id,
        _runtime_order_id(intent_id),
        writer_lease=lease,
    )
    command_id = "command-ready-before-callback-owner"
    command = store.stage_ctp_dispatch_command(
        scope,
        command_id,
        "SUBMIT",
        {
            "InstrumentID": "rb2710",
            "LimitPrice": "3510.5",
            "VolumeTotalOriginal": 1,
            "OrderRef": reservation.order_ref,
        },
        approval_use_id="approval-use-ready-before-callback-owner",
        approval_digest=hashlib.sha256(b"approved test command").hexdigest(),
        session_binding={
            "session_identity": "opaque-session-v1",
            "session_generation_id": _SESSION_GENERATION,
            "dispatch_front_id": 4,
            "dispatch_session_id": 91,
        },
        writer_lease=lease,
        managed_intent_id=reservation.managed_intent_id,
        order_ref=reservation.order_ref,
        managed_action_id=reservation.managed_intent_id,
        session_generation_id=_SESSION_GENERATION,
        dispatch_front_id=4,
        dispatch_session_id=91,
        native_request_id=17,
    )
    assert command.status == "READY"
    return command_id


def _row_dict(connection: sqlite3.Connection, table: str, where: str, values: tuple):
    row = connection.execute(f"SELECT * FROM {table} WHERE {where}", values).fetchone()
    assert row is not None
    return dict(row)


def _schema_signature(connection: sqlite3.Connection) -> tuple[tuple[str, str, str, str | None], ...]:
    return tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT type, name, tbl_name, sql FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name
            """
        ).fetchall()
    )


def _assert_source_only_owner_scope(store, source_scope, owner) -> None:
    with store._transaction() as cursor:
        store._require_ctp_callback_session_owner_handle(cursor, source_scope, owner)


@pytest.mark.unit
def test_callback_owner_persists_multiple_keys_without_widening_source_owner(tmp_path):
    store = SqliteExecutionStore(tmp_path / "owner-scope-set.sqlite3")
    source = _scope("strategy.z-source")
    second = _scope("strategy.a-second")
    third = _scope("strategy.m-third")
    unregistered = _scope("strategy.zz-unregistered")
    lease = _writer_lease(store, source)
    try:
        owner = store.create_ctp_callback_session_owner(
            source, writer_lease=lease, authorized_scopes=(second, source, third)
        )

        expected = (source.key, *sorted((second.key, third.key)))
        assert store.ctp_callback_session_owner_scope_keys(owner) == expected
        _assert_source_only_owner_scope(store, source, owner)
        with store._transaction() as cursor:
            with pytest.raises(ContractValidationError, match="same-Store"):
                store._require_ctp_callback_session_owner_handle(cursor, second, owner)
            with pytest.raises(ContractValidationError, match="same-Store"):
                store._require_ctp_callback_session_owner_handle(cursor, third, owner)
            with pytest.raises(ContractValidationError, match="same-Store"):
                store._require_ctp_callback_session_owner_handle(cursor, unregistered, owner)

        with pytest.raises(ContractValidationError, match="same-Store"):
            store.ctp_callback_session_owner_scope_keys(replace(owner))
        foreign = SqliteExecutionStore(tmp_path / "foreign-owner-scope-set.sqlite3")
        try:
            with pytest.raises(ContractValidationError, match="same-Store"):
                foreign.ctp_callback_session_owner_scope_keys(owner)
        finally:
            foreign.close()

        before = store.ctp_callback_session_owner_scope_keys(owner)
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            store._connection.execute(
                "UPDATE ctp_dispatch_callback_session_owner_scopes "
                "SET scope_count = scope_count WHERE owner_intent_id = ?",
                (owner.owner_intent_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="permanent"):
            store._connection.execute(
                "DELETE FROM ctp_dispatch_callback_session_owner_scopes "
                "WHERE owner_intent_id = ?",
                (owner.owner_intent_id,),
            )
        with pytest.raises(InvalidStateTransition, match="already has a durable"):
            store.create_ctp_callback_session_owner(
                source,
                writer_lease=lease,
                authorized_scopes=(source, _scope("strategy.unapproved")),
            )
        assert store.ctp_callback_session_owner_scope_keys(owner) == before
    finally:
        store.close()


@pytest.mark.unit
def test_callback_owner_scope_set_rejects_invalid_or_cross_account_sets(tmp_path):
    source = _scope("strategy.validation-source")
    invalid_sets = (
        (),
        [],
        (_scope("strategy.other-account", account_ref=_account_ref("other")), source),
        (source, _scope("strategy.other-environment", environment="live")),
        (source, _scope("strategy.other-day", trading_day="20260926")),
        (source, _scope("strategy.other-provider", provider="binance")),
        (source, source),
        (_scope("strategy.missing-source"),),
        (source, object()),
        tuple([source, *(_scope(f"strategy.extra-{index:03d}") for index in range(256))]),
    )
    store = SqliteExecutionStore(tmp_path / "invalid-owner-scope-sets.sqlite3")
    lease = _writer_lease(store, source)
    try:
        for invalid in invalid_sets:
            with pytest.raises(ContractValidationError):
                store.create_ctp_callback_session_owner(
                    source, writer_lease=lease, authorized_scopes=invalid
                )
            assert store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_session_owners"
            ).fetchone()[0] == 0
            assert store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_session_owner_scopes"
            ).fetchone()[0] == 0
    finally:
        store.close()


@pytest.mark.unit
def test_callback_owner_default_scope_is_singleton_and_persisted_handle_is_not_reissued(
    tmp_path,
):
    path = tmp_path / "owner-scope-singleton.sqlite3"
    store = SqliteExecutionStore(path)
    source = _scope("strategy.singleton")
    lease = _writer_lease(store, source)
    owner = store.create_ctp_callback_session_owner(source, writer_lease=lease)
    try:
        assert store.ctp_callback_session_owner_scope_keys(owner) == (source.key,)
        persisted = store._connection.execute(
            "SELECT scope_count, source_scope_key, scope_keys_json "
            "FROM ctp_dispatch_callback_session_owner_scopes WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert persisted is not None
        assert persisted["scope_count"] == 1
        assert persisted["source_scope_key"] == source.key
        assert json.loads(persisted["scope_keys_json"])["scope_keys"] == [source.key]
    finally:
        store.close()

    reopened = SqliteExecutionStore(path)
    try:
        assert reopened._connection.execute(
            "SELECT scope_count, source_scope_key FROM ctp_dispatch_callback_session_owner_scopes "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone() is not None
        with pytest.raises(ContractValidationError, match="same-Store"):
            reopened.ctp_callback_session_owner_scope_keys(owner)
    finally:
        reopened.close()


@pytest.mark.unit
def test_schema_23_upgrade_preserves_ready_dispatch_and_only_adds_source_scope(tmp_path):
    path = tmp_path / "owner-scope-schema-23.sqlite3"
    source = _scope("strategy.migration-source")
    store = SqliteExecutionStore.open_ctp_account_store(path, source)
    lease = _writer_lease(store, source)
    command_id = _insert_ready_submit(store, source, lease)
    owner = store.create_ctp_callback_session_owner(source, writer_lease=lease)
    owner_id = owner.owner_intent_id
    before_owner = _row_dict(
        store._connection,
        "ctp_dispatch_callback_session_owners",
        "owner_intent_id = ?",
        (owner_id,),
    )
    before_command = _row_dict(
        store._connection,
        "ctp_dispatch_commands",
        "command_id = ?",
        (command_id,),
    )
    assert before_command["status"] == "READY"
    v24_schema = _schema_signature(store._connection)
    store.close()

    scope_objects = {
        "ctp_dispatch_callback_session_owner_scopes",
        "ctp_callback_session_owner_scopes_owner_match",
        "ctp_callback_session_owner_scopes_immutable_update",
        "ctp_callback_session_owner_scopes_immutable_delete",
    }
    legacy = sqlite3.connect(path)
    legacy.row_factory = sqlite3.Row
    try:
        existing_names = {
            row["name"]
            for row in legacy.execute(
                "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        assert existing_names == {name for _, name, _, _ in v24_schema}
        assert scope_objects <= existing_names
        for trigger in sorted(scope_objects - {"ctp_dispatch_callback_session_owner_scopes"}):
            legacy.execute(f'DROP TRIGGER "{trigger}"')
        legacy.execute("DROP TABLE ctp_dispatch_callback_session_owner_scopes")
        legacy.execute("UPDATE execution_meta SET value = '23' WHERE key = 'schema_version'")
        expected_v23_schema = tuple(
            row for row in v24_schema if row[1] not in scope_objects
        )
        assert _schema_signature(legacy) == expected_v23_schema
        assert legacy.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == "23"
        legacy.commit()
    finally:
        legacy.close()

    inspection = SqliteExecutionStore.inspect_ctp_account_store_file(path, source)
    assert inspection.kind == "CTP_EXECUTION_STORE"
    migrated = SqliteExecutionStore.open_ctp_account_store(path, source)
    try:
        assert _schema_signature(migrated._connection) == v24_schema
        assert _row_dict(
            migrated._connection,
            "ctp_dispatch_callback_session_owners",
            "owner_intent_id = ?",
            (owner_id,),
        ) == before_owner
        assert _row_dict(
            migrated._connection,
            "ctp_dispatch_commands",
            "command_id = ?",
            (command_id,),
        ) == before_command
        scope_row = migrated._connection.execute(
            "SELECT scope_count, source_scope_key, scope_set_sha256, scope_keys_json "
            "FROM ctp_dispatch_callback_session_owner_scopes WHERE owner_intent_id = ?",
            (owner_id,),
        ).fetchone()
        assert scope_row is not None
        scope_keys_json = json.dumps(
            {"schema": "ctp-callback-owner-scope-set.v1", "scope_keys": [source.key]},
            sort_keys=True,
            separators=(",", ":"),
        )
        assert tuple(scope_row) == (
            1,
            source.key,
            hashlib.sha256(scope_keys_json.encode("utf-8")).hexdigest(),
            scope_keys_json,
        )
        assert migrated._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == "24"
        with pytest.raises(ContractValidationError, match="same-Store"):
            migrated.ctp_callback_session_owner_scope_keys(owner)
    finally:
        migrated.close()
