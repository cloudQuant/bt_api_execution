from __future__ import annotations

import os
import sqlite3
from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from bt_api_execution import (
    CancelIntent,
    CancelObservation,
    ExecutionScope,
    ManagedCancellationFacade,
    ManagedExecutionFacade,
    OrderIntent,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
)
from bt_api_execution.journal_reader import (
    ExecutionJournalReader,
    JournalReaderError,
)


def _scope_a() -> ExecutionScope:
    return ExecutionScope("FAKE", "simulation", "acct_a", "strategy.a", "20261015")


def _scope_b() -> ExecutionScope:
    return ExecutionScope("FAKE", "simulation", "acct_b", "strategy.b", "20261015")


def _intent(scope: ExecutionScope, intent_id: str) -> OrderIntent:
    return OrderIntent.limit(
        intent_id=intent_id,
        scope=scope,
        signal_id=f"signal.{intent_id}",
        instrument="FAKE-PERP",
        side=Side.BUY,
        quantity=Decimal("1"),
        price=Decimal("100"),
        metadata_version="metadata.1",
    )


def _build_journals(database_path) -> None:
    store = SqliteExecutionStore(database_path)
    try:
        facade_a = ManagedExecutionFacade(
            store,
            _scope_a(),
            writer_id="writer.a",
            allow_unprotected=True,
        )
        facade_b = ManagedExecutionFacade(
            store,
            _scope_b(),
            writer_id="writer.b",
            allow_unprotected=True,
        )
        for facade, intent_id in (
            (facade_a, "order.a1"),
            (facade_b, "order.b1"),
            (facade_a, "order.a2"),
        ):
            intent = _intent(facade.scope, intent_id)
            facade.submit(
                intent,
                lambda submitted: ProviderObservation.accepted(
                    submitted.intent_id, f"provider.{submitted.intent_id}"
                ),
            )

        cancel_facade_a = ManagedCancellationFacade(
            store,
            _scope_a(),
            acquire_writer_lease=facade_a.acquire_writer_lease,
            allow_unprotected=True,
        )
        cancel_facade_b = ManagedCancellationFacade(
            store,
            _scope_b(),
            acquire_writer_lease=facade_b.acquire_writer_lease,
            allow_unprotected=True,
        )
        for facade, cancel_id, intent_id in (
            (cancel_facade_a, "cancel.a1", "order.a1"),
            (cancel_facade_b, "cancel.b1", "order.b1"),
            (cancel_facade_a, "cancel.a2", "order.a2"),
        ):
            cancel = CancelIntent(
                cancel_id=cancel_id,
                scope=facade.scope,
                target_intent_id=intent_id,
                provider_order_id=f"provider.{intent_id}",
                metadata_version="metadata.1",
            )
            facade.cancel(
                cancel,
                lambda submitted: CancelObservation.cancelled(
                    submitted.cancel_id,
                    submitted.target_intent_id,
                    submitted.provider_order_id,
                ),
            )
    finally:
        store.close()


def _snapshot(database_path) -> tuple[tuple[tuple[object, ...], ...], ...]:
    uri = f"{database_path.resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        connection.execute("PRAGMA query_only = ON")
        queries = (
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name",
            "SELECT key, value FROM execution_meta ORDER BY key",
            "SELECT * FROM execution_records ORDER BY scope_key, intent_id",
            "SELECT * FROM execution_outbox ORDER BY sequence",
            "SELECT * FROM cancellation_records ORDER BY scope_key, cancel_id",
            "SELECT * FROM cancellation_outbox ORDER BY sequence",
            "SELECT * FROM execution_writer_leases ORDER BY scope_key",
        )
        return tuple(tuple(tuple(row) for row in connection.execute(query)) for query in queries)
    finally:
        connection.close()


def _replace_payload(database_path, stream: str, sentinel: str) -> None:
    table = "execution_outbox" if stream == "execution" else "cancellation_outbox"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            f"UPDATE {table} SET payload_json = ? WHERE sequence = (SELECT MIN(sequence) FROM {table})",  # noqa: S608 -- two fixed test table names
            (sentinel,),
        )
        connection.commit()


def test_reads_both_scoped_streams_with_gaps_and_without_payload_fields(tmp_path) -> None:
    database_path = tmp_path / "execution.sqlite3"
    _build_journals(database_path)
    _replace_payload(database_path, "execution", "raw-provider-secret:not-json")
    _replace_payload(database_path, "cancellation", "raw-cancellation-secret:not-json")
    before = _snapshot(database_path)

    reader = ExecutionJournalReader(database_path, scope=_scope_a())
    try:
        assert reader.database_path == database_path.resolve()
        assert reader.scope == _scope_a()
        assert reader.source_identity == (database_path.stat().st_dev, database_path.stat().st_ino)

        execution_page = reader.read_page("execution", limit=100)
        execution_events = execution_page.events
        assert execution_page.generation == reader.generation
        assert execution_events
        assert [event.sequence for event in execution_events] == sorted(
            event.sequence for event in execution_events
        )
        assert any(
            later.sequence > earlier.sequence + 1
            for earlier, later in zip(execution_events, execution_events[1:])  # noqa: B905 -- adjacent pairs intentionally differ in length
        )
        assert {event.intent_id for event in execution_events} <= {"order.a1", "order.a2"}
        assert all(
            event.cancel_id is None and event.target_intent_id is None for event in execution_events
        )
        assert all(event.scope_key == _scope_a().key for event in execution_events)
        assert all(event.journal_generation == reader.generation for event in execution_events)

        first = reader.read_page("execution", after_sequence=0, limit=1)
        second = reader.read_page("execution", after_sequence=first.events[-1].sequence, limit=1)
        assert len(first.events) == len(second.events) == 1
        assert second.events[0].sequence > first.events[0].sequence

        cancellation_page = reader.read_page("cancellation", limit=100)
        assert cancellation_page.generation == reader.generation
        assert {event.cancel_id for event in cancellation_page.events} == {"cancel.a1", "cancel.a2"}
        assert {event.target_intent_id for event in cancellation_page.events} == {
            "order.a1",
            "order.a2",
        }
        assert all(event.intent_id is None for event in cancellation_page.events)
        assert any(
            later.sequence > earlier.sequence + 1
            for earlier, later in zip(cancellation_page.events, cancellation_page.events[1:])  # noqa: B905 -- adjacent pairs intentionally differ in length
        )
        cancellation_first = reader.read_page("cancellation", limit=1)
        cancellation_second = reader.read_page(
            "cancellation", after_sequence=cancellation_first.events[-1].sequence, limit=1
        )
        assert len(cancellation_first.events) == len(cancellation_second.events) == 1
        assert cancellation_second.events[0].sequence > cancellation_first.events[0].sequence

        for event in (*execution_events, *cancellation_page.events):
            assert not hasattr(event, "payload")
            assert not hasattr(event, "payload_json")
            with pytest.raises(FrozenInstanceError):
                event.event_id = "changed"  # type: ignore[misc]

        other_scope_reader = ExecutionJournalReader(database_path, scope=_scope_b())
        try:
            other_scope_events = other_scope_reader.read_page("execution", limit=100).events
            assert {event.intent_id for event in other_scope_events} == {"order.b1"}
            empty_scope_reader = ExecutionJournalReader(
                database_path,
                scope=ExecutionScope("FAKE", "simulation", "acct_empty", "strategy.empty"),
            )
            try:
                empty_page = empty_scope_reader.read_page("execution")
                assert empty_page.generation == reader.generation
                assert empty_page.events == ()
            finally:
                empty_scope_reader.close()
        finally:
            other_scope_reader.close()
    finally:
        reader.close()

    assert _snapshot(database_path) == before
    with sqlite3.connect(database_path) as connection:
        execution_payload = connection.execute(
            "SELECT payload_json FROM execution_outbox ORDER BY sequence LIMIT 1"
        ).fetchone()[0]
        cancellation_payload = connection.execute(
            "SELECT payload_json FROM cancellation_outbox ORDER BY sequence LIMIT 1"
        ).fetchone()[0]
    assert execution_payload == "raw-provider-secret:not-json"
    assert cancellation_payload == "raw-cancellation-secret:not-json"


def test_missing_database_is_not_created_and_path_is_required(tmp_path) -> None:
    missing = tmp_path / "missing.sqlite3"
    with pytest.raises(JournalReaderError) as error:
        ExecutionJournalReader(missing, scope=_scope_a())
    assert error.value.code == "database_missing"
    assert not missing.exists()

    database_path = tmp_path / "execution.sqlite3"
    _build_journals(database_path)
    with pytest.raises(JournalReaderError) as error:
        ExecutionJournalReader(database_path, scope=None)  # type: ignore[arg-type]
    assert error.value.code == "invalid_scope"


def test_invalid_page_arguments_and_closed_reader_have_finite_codes(tmp_path) -> None:
    database_path = tmp_path / "execution.sqlite3"
    _build_journals(database_path)
    reader = ExecutionJournalReader(database_path, scope=_scope_a())
    with pytest.raises(JournalReaderError) as error:
        reader.read_page("unknown")  # type: ignore[arg-type]
    assert error.value.code == "invalid_stream"
    with pytest.raises(JournalReaderError) as error:
        reader.read_page("execution", after_sequence=True)
    assert error.value.code == "invalid_cursor"
    with pytest.raises(JournalReaderError) as error:
        reader.read_page("execution", after_sequence=2**63)
    assert error.value.code == "invalid_cursor"
    with pytest.raises(JournalReaderError) as error:
        reader.read_page("execution", limit=1_001)
    assert error.value.code == "invalid_limit"
    reader.close()
    with pytest.raises(JournalReaderError) as error:
        reader.read_page("execution")
    assert error.value.code == "reader_closed"


def test_malformed_metadata_is_rejected_with_bounded_selected_columns(tmp_path) -> None:
    database_path = tmp_path / "execution.sqlite3"
    _build_journals(database_path)
    with sqlite3.connect(database_path) as connection:
        first_execution_sequence = connection.execute(
            "SELECT MIN(sequence) FROM execution_outbox WHERE scope_key = ?",
            (_scope_a().key,),
        ).fetchone()[0]
        second_execution_sequence = connection.execute(
            "SELECT sequence FROM execution_outbox WHERE scope_key = ? "
            "ORDER BY sequence LIMIT 1 OFFSET 1",
            (_scope_a().key,),
        ).fetchone()[0]
        first_cancellation_sequence = connection.execute(
            "SELECT MIN(sequence) FROM cancellation_outbox WHERE scope_key = ?",
            (_scope_a().key,),
        ).fetchone()[0]
        connection.execute(
            "UPDATE execution_outbox SET event_type = ? WHERE sequence = ?",
            ("x" * 10_000, first_execution_sequence),
        )
        connection.execute(
            "UPDATE execution_outbox SET created_at_ns = ? WHERE sequence = ?",
            ("bad-time:" + "x" * 10_000, second_execution_sequence),
        )
        connection.execute(
            "UPDATE cancellation_outbox SET created_at_ns = ? WHERE sequence = ?",
            ("bad-time:" + "x" * 10_000, first_cancellation_sequence),
        )
        assert (
            connection.execute(
                "SELECT typeof(created_at_ns) FROM execution_outbox WHERE sequence = ?",
                (second_execution_sequence,),
            ).fetchone()[0]
            == "text"
        )
        assert (
            connection.execute(
                "SELECT typeof(created_at_ns) FROM cancellation_outbox WHERE sequence = ?",
                (first_cancellation_sequence,),
            ).fetchone()[0]
            == "text"
        )
        connection.commit()

    reader = ExecutionJournalReader(database_path, scope=_scope_a())
    try:
        with pytest.raises(JournalReaderError) as error:
            reader.read_page("execution", limit=1)
        assert error.value.code == "malformed_event"
        with pytest.raises(JournalReaderError) as error:
            reader.read_page("execution", after_sequence=first_execution_sequence, limit=1)
        assert error.value.code == "malformed_event"
        with pytest.raises(JournalReaderError) as error:
            reader.read_page("cancellation", limit=1)
        assert error.value.code == "malformed_event"
    finally:
        reader.close()


def test_changed_generation_and_database_replacement_do_not_reuse_old_reader(tmp_path) -> None:
    database_path = tmp_path / "execution.sqlite3"
    _build_journals(database_path)
    reader = ExecutionJournalReader(database_path, scope=_scope_a())
    old_generation = reader.generation
    replacement_generation = ("0" if old_generation[0] != "0" else "1") + old_generation[1:]
    try:
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                "UPDATE execution_meta SET value = ? WHERE key = 'journal_incarnation_id'",
                (replacement_generation,),
            )
            connection.commit()
        with pytest.raises(JournalReaderError) as error:
            reader.read_page("execution")
        assert error.value.code == "generation_changed"
        assert reader.generation == old_generation
    finally:
        reader.close()

    replaced_path = tmp_path / "replaced.sqlite3"
    _build_journals(replaced_path)
    replaced_reader = ExecutionJournalReader(replaced_path, scope=_scope_a())
    replacement_path = tmp_path / "new.sqlite3"
    _build_journals(replacement_path)
    os.replace(replacement_path, replaced_path)
    try:
        with pytest.raises(JournalReaderError) as error:
            replaced_reader.read_page("execution")
        assert error.value.code == "source_changed"
    finally:
        replaced_reader.close()
