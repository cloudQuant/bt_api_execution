"""Focused durable-journal recovery snapshot and page tests."""

from __future__ import annotations

from decimal import Decimal

import pytest

from bt_api_execution import (
    ExecutionScope,
    ExecutionState,
    OrderIntent,
    PositionEffect,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
)
from bt_api_execution.errors import ContractValidationError, DurableStoreError


def _scope(strategy: str = "strategy-a") -> ExecutionScope:
    return ExecutionScope("PAPER", "simulation", "account-a", strategy, "20261016")


def _intent(scope: ExecutionScope, intent_id: str = "intent-1") -> OrderIntent:
    from bt_api_execution import OrderType

    return OrderIntent(
        intent_id=intent_id,
        scope=scope,
        signal_id=f"signal-{intent_id}",
        instrument="BTC-USDT",
        side=Side.BUY,
        position_effect=PositionEffect.OPEN,
        order_type=OrderType.LIMIT,
        quantity=Decimal("2"),
        price=Decimal("10"),
    )


def _acked_store(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = store.acquire_or_renew_lease(scope, "recovery-test")
    intent = _intent(scope)
    store.admit_intent(intent, writer_lease=lease)
    store.activate_intent(intent.intent_id, scope, writer_lease=lease)
    claimed, dispatch = store.claim_for_dispatch(intent.intent_id, scope, writer_lease=lease)
    assert dispatch is True and claimed.state is ExecutionState.DISPATCHING
    store.record_observation(
        ProviderObservation.accepted(intent.intent_id, "provider-1"),
        scope=scope,
        writer_lease=lease,
    )
    return store, scope


@pytest.mark.unit
def test_recovery_snapshot_and_sparse_scope_pages_share_the_global_cut(tmp_path) -> None:
    store, scope = _acked_store(tmp_path)
    other_scope = _scope("strategy-b")
    lease = store.acquire_or_renew_lease(other_scope, "recovery-test")
    other = _intent(other_scope)
    store.admit_intent(other, writer_lease=lease)
    try:
        snapshot = store.read_execution_recovery_snapshot((scope,))
        assert snapshot.cut.row_count == snapshot.cut.high_water_sequence == 5
        assert snapshot.cut.first_sequence == 1
        assert len(snapshot.orders) == 1
        order = snapshot.orders[0]
        assert order.intent.scope == scope
        assert order.record.scope_key == scope.key
        assert other_scope.key != order.record.scope_key
        assert order.record.state is ExecutionState.ACKED
        assert order.latest_event.event_type == "provider_observation"
        assert order.latest_observation_event == order.latest_event
        assert tuple(event.sequence for event in snapshot.events) == (1, 2, 3, 4)

        first = store.read_execution_outbox_page((scope,), after_sequence=0, limit=2)
        assert tuple(event.sequence for event in first.events) == (1, 2)
        assert first.has_more is True
        assert first.scanned_through == 2
        assert first.cut == snapshot.cut
        second = store.read_execution_outbox_page(
            (scope,), after_sequence=first.scanned_through, limit=2
        )
        assert tuple(event.sequence for event in second.events) == (3, 4)
        assert second.has_more is False
        assert second.scanned_through == second.cut.high_water_sequence == 5
        assert second.cut == snapshot.cut
    finally:
        store.close()


@pytest.mark.unit
def test_recovery_reads_fail_closed_on_global_outbox_gap(tmp_path) -> None:
    store, scope = _acked_store(tmp_path)
    try:
        with store._lock:
            store._connection.execute("DELETE FROM execution_outbox WHERE sequence = 2")

        with pytest.raises(DurableStoreError, match="contiguous journal"):
            store.read_execution_recovery_snapshot((scope,))
        with pytest.raises(DurableStoreError, match="contiguous journal"):
            store.read_execution_outbox_page((scope,), after_sequence=0, limit=2)
    finally:
        store.close()


@pytest.mark.unit
def test_recovery_reads_fail_closed_when_selected_event_lineage_is_missing(tmp_path) -> None:
    store, scope = _acked_store(tmp_path)
    try:
        with store._lock:
            store._connection.execute(
                "UPDATE execution_outbox SET journal_incarnation_id = NULL WHERE sequence = 4"
            )

        with pytest.raises(DurableStoreError, match="lineage or sequence"):
            store.read_execution_recovery_snapshot((scope,))
        with pytest.raises(DurableStoreError, match="lineage or sequence"):
            store.read_execution_outbox_page((scope,), after_sequence=2, limit=2)
    finally:
        store.close()


@pytest.mark.unit
def test_recovery_page_rejects_cursor_past_pinned_high_water(tmp_path) -> None:
    store, scope = _acked_store(tmp_path)
    try:
        with pytest.raises(ContractValidationError, match="exceeds journal high-water"):
            store.read_execution_outbox_page((scope,), after_sequence=99, limit=2)
    finally:
        store.close()
