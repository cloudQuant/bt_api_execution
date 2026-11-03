"""Offline schema and restart boundaries for Paper admission snapshots."""

from __future__ import annotations

import sqlite3

import pytest
import test_paper_risk_snapshot as fixtures
from bt_api_risk import RiskAdmissionDeniedError

from bt_api_execution import SqliteExecutionStore
from bt_api_execution.errors import DurableStoreError


def _persist(tmp_path):
    intent = fixtures._order()
    store, gate, adapter, _matcher, source = fixtures._composition(tmp_path, fixtures._quote())
    lease = fixtures._lease_and_record(store, intent)
    try:
        permit = adapter.reserve_for_execution(intent, writer_lease=lease)
        assert source.calls == ["BTC-USDT"]
    finally:
        fixtures._close(store, gate, lease)
    return intent, permit


def test_snapshot_reopens_and_reuses_original_quote_and_permit(tmp_path):
    intent, permit = _persist(tmp_path)
    store, gate, adapter, _matcher, source = fixtures._composition(tmp_path, fixtures._quote())
    lease = fixtures._lease_and_record(store, intent)
    try:
        replayed = adapter.reserve_for_execution(intent, writer_lease=lease)
        assert replayed.permit_id == permit.permit_id
        assert source.calls == []
    finally:
        fixtures._close(store, gate, lease)


@pytest.mark.parametrize(
    "damage", ["table", "insert_trigger", "update_trigger", "delete_trigger", "marker"]
)
def test_marked_snapshot_schema_damage_is_rejected_before_write(tmp_path, damage):
    _persist(tmp_path)
    path = tmp_path / "paper-execution.sqlite3"
    with sqlite3.connect(path) as connection:
        if damage == "table":
            connection.execute("DROP TABLE execution_paper_risk_admission_snapshots")
        elif damage == "marker":
            connection.execute("DROP TRIGGER execution_paper_risk_marker_update")
            connection.execute(
                "UPDATE execution_meta SET value='invalid' "
                "WHERE key='paper_risk_snapshot_schema_version'"
            )
        else:
            connection.execute(
                f"DROP TRIGGER execution_paper_risk_snapshot_immutable_{damage.split('_')[0]}"
            )
    before = path.read_bytes()
    with pytest.raises(DurableStoreError, match="schema"):
        SqliteExecutionStore(path)
    assert path.read_bytes() == before


def test_current_snapshot_row_cannot_be_updated_or_deleted(tmp_path):
    _persist(tmp_path)
    path = tmp_path / "paper-execution.sqlite3"
    with sqlite3.connect(path) as connection:
        for statement in (
            "UPDATE execution_paper_risk_admission_snapshots SET snapshot_json='{}'",
            "DELETE FROM execution_paper_risk_admission_snapshots",
            "INSERT OR REPLACE INTO execution_paper_risk_admission_snapshots "
            "SELECT * FROM execution_paper_risk_admission_snapshots",
        ):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(statement)


@pytest.mark.parametrize("keep_table", [False, True])
def test_unmarked_legacy_missing_snapshot_never_reads_new_quote(tmp_path, keep_table):
    intent, _permit = _persist(tmp_path)
    path = tmp_path / "paper-execution.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER execution_paper_risk_snapshot_immutable_update")
        connection.execute("DROP TRIGGER execution_paper_risk_snapshot_immutable_delete")
        connection.execute("DROP TRIGGER execution_paper_risk_snapshot_immutable_insert")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_insert")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_update")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_delete")
        if keep_table:
            connection.execute("DELETE FROM execution_paper_risk_admission_snapshots")
        else:
            connection.execute("DROP TABLE execution_paper_risk_admission_snapshots")
        connection.execute(
            "DELETE FROM execution_meta WHERE key='paper_risk_snapshot_schema_version'"
        )
    store, gate, adapter, _matcher, source = fixtures._composition(tmp_path, fixtures._quote())
    lease = fixtures._lease_and_record(store, intent)
    try:
        with pytest.raises(RiskAdmissionDeniedError) as denied:
            adapter.reserve_for_execution(intent, writer_lease=lease)
        assert denied.value.code in {
            "PAPER_ADMISSION_SNAPSHOT_LEGACY_UNVERIFIED",
            "PAPER_ADMISSION_SNAPSHOT_SCHEMA_INVALID",
        }
        assert source.calls == []
    finally:
        fixtures._close(store, gate, lease)


def test_unmarked_legacy_present_snapshot_is_read_without_new_quote(tmp_path):
    intent, permit = _persist(tmp_path)
    path = tmp_path / "paper-execution.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER execution_paper_risk_snapshot_immutable_update")
        connection.execute("DROP TRIGGER execution_paper_risk_snapshot_immutable_delete")
        connection.execute("DROP TRIGGER execution_paper_risk_snapshot_immutable_insert")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_insert")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_update")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_delete")
        connection.execute(
            "DELETE FROM execution_meta WHERE key='paper_risk_snapshot_schema_version'"
        )
    store, gate, adapter, _matcher, source = fixtures._composition(tmp_path, fixtures._quote())
    lease = fixtures._lease_and_record(store, intent)
    try:
        replayed = adapter.reserve_for_execution(intent, writer_lease=lease)
        assert replayed.permit_id == permit.permit_id
        assert source.calls == []
    finally:
        fixtures._close(store, gate, lease)


def test_unmarked_legacy_changed_table_is_rejected_before_write(tmp_path):
    _persist(tmp_path)
    path = tmp_path / "paper-execution.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE execution_paper_risk_admission_snapshots")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_insert")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_update")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_delete")
        connection.execute(
            "CREATE TABLE execution_paper_risk_admission_snapshots "
            "(scope_key TEXT, intent_id TEXT, snapshot_json TEXT)"
        )
        connection.execute(
            "DELETE FROM execution_meta WHERE key='paper_risk_snapshot_schema_version'"
        )
    before = path.read_bytes()
    with pytest.raises(DurableStoreError, match="schema"):
        SqliteExecutionStore(path)
    assert path.read_bytes() == before


def test_unmarked_legacy_corrupt_row_never_reads_new_quote(tmp_path):
    intent, _permit = _persist(tmp_path)
    path = tmp_path / "paper-execution.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER execution_paper_risk_snapshot_immutable_update")
        connection.execute("DROP TRIGGER execution_paper_risk_snapshot_immutable_delete")
        connection.execute("DROP TRIGGER execution_paper_risk_snapshot_immutable_insert")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_insert")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_update")
        connection.execute("DROP TRIGGER execution_paper_risk_marker_delete")
        connection.execute("UPDATE execution_paper_risk_admission_snapshots SET snapshot_json='{}'")
        connection.execute(
            "DELETE FROM execution_meta WHERE key='paper_risk_snapshot_schema_version'"
        )
    store, gate, adapter, _matcher, source = fixtures._composition(tmp_path, fixtures._quote())
    lease = fixtures._lease_and_record(store, intent)
    try:
        with pytest.raises(RiskAdmissionDeniedError) as denied:
            adapter.reserve_for_execution(intent, writer_lease=lease)
        assert denied.value.code == "PAPER_ADMISSION_SNAPSHOT_CORRUPT"
        assert source.calls == []
    finally:
        fixtures._close(store, gate, lease)


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE execution_meta SET value='0' WHERE key='paper_risk_snapshot_schema_version'",
        "DELETE FROM execution_meta WHERE key='paper_risk_snapshot_schema_version'",
        "UPDATE execution_meta SET key='changed' WHERE key='paper_risk_snapshot_schema_version'",
        "INSERT OR REPLACE INTO execution_meta(key,value) "
        "VALUES('paper_risk_snapshot_schema_version','0')",
    ],
)
def test_marker_cannot_be_reverted_or_deleted_after_table_loss(tmp_path, mutation):
    _persist(tmp_path)
    path = tmp_path / "paper-execution.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE execution_paper_risk_admission_snapshots")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(mutation)
    before = path.read_bytes()
    with pytest.raises(DurableStoreError, match="schema"):
        SqliteExecutionStore(path)
    assert path.read_bytes() == before


@pytest.mark.parametrize("trigger", ["insert", "update", "delete"])
def test_missing_marker_trigger_is_rejected_before_reopen(tmp_path, trigger):
    _persist(tmp_path)
    path = tmp_path / "paper-execution.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(f"DROP TRIGGER execution_paper_risk_marker_{trigger}")
    before = path.read_bytes()
    with pytest.raises(DurableStoreError, match="schema"):
        SqliteExecutionStore(path)
    assert path.read_bytes() == before


def test_case_insensitive_table_collision_does_not_create_snapshot_schema(tmp_path):
    intent = fixtures._order()
    store, gate, adapter, _matcher, source = fixtures._composition(tmp_path, fixtures._quote())
    lease = fixtures._lease_and_record(store, intent)
    try:
        # The store is already open: SQLite table names are case insensitive.
        with sqlite3.connect(tmp_path / "paper-execution.sqlite3") as connection:
            connection.execute(
                "CREATE TABLE EXECUTION_PAPER_RISK_ADMISSION_SNAPSHOTS (payload TEXT)"
            )
        with pytest.raises(RiskAdmissionDeniedError) as denied:
            adapter.reserve_for_execution(intent, writer_lease=lease)
        assert denied.value.code == "PAPER_ADMISSION_SNAPSHOT_STORE_UNAVAILABLE"
        assert source.calls == []
        assert (
            store._connection.execute(
                "SELECT value FROM execution_meta WHERE key='paper_risk_snapshot_schema_version'"
            ).fetchone()[0]
            == "0"
        )
    finally:
        fixtures._close(store, gate, lease)


def test_open_store_rechecks_marker_guard_before_replaying_snapshot(tmp_path):
    intent, _permit = _persist(tmp_path)
    store, gate, adapter, _matcher, source = fixtures._composition(tmp_path, fixtures._quote())
    lease = fixtures._lease_and_record(store, intent)
    try:
        with sqlite3.connect(tmp_path / "paper-execution.sqlite3") as connection:
            connection.execute("DROP TRIGGER execution_paper_risk_marker_insert")
        with pytest.raises(RiskAdmissionDeniedError) as denied:
            adapter.reserve_for_execution(intent, writer_lease=lease)
        assert denied.value.code == "PAPER_ADMISSION_SNAPSHOT_SCHEMA_INVALID"
        assert source.calls == []
    finally:
        fixtures._close(store, gate, lease)
