from __future__ import annotations

from decimal import Decimal

import pytest

from bt_api_execution import (
    AdmissionDenied,
    CancelIntent,
    CancelObservation,
    ExecutionScope,
    ExecutionState,
    InvalidStateTransition,
    ManagedExecutionFacade,
    OrderIntent,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
)


def _scope() -> ExecutionScope:
    return ExecutionScope("FAKE", "simulation", "acct_demo", "strategy.demo", "20260922")


def _intent(intent_id: str = "order.snapshot") -> OrderIntent:
    return OrderIntent.limit(
        intent_id=intent_id,
        scope=_scope(),
        signal_id=f"signal.{intent_id}",
        instrument="FAKE-PERP",
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("50000"),
        metadata_version="metadata.1",
    )


def _writer_lease(store: SqliteExecutionStore):
    return store.acquire_or_renew_lease(_scope(), "writer.snapshot", ttl_ns=30_000_000_000)


def _partial_target(
    store: SqliteExecutionStore,
    *,
    average_price: Decimal | None,
    cumulative_commission: Decimal | None,
) -> None:
    intent = _intent()
    lease = _writer_lease(store)
    store.admit_intent(intent, writer_lease=lease)
    store.activate_intent(intent.intent_id, intent.scope, writer_lease=lease)
    _claimed, claimed = store.claim_for_dispatch(intent.intent_id, intent.scope, writer_lease=lease)
    assert claimed is True
    store.record_observation(
        ProviderObservation.accepted(intent.intent_id, "provider.order.1"),
        scope=intent.scope,
        writer_lease=lease,
    )
    store.record_observation(
        ProviderObservation(
            intent.intent_id,
            ExecutionState.PARTIALLY_FILLED,
            provider_order_id="provider.order.1",
            filled_quantity=Decimal("0.75"),
            average_price=average_price,
            cumulative_commission=cumulative_commission,
        ),
        scope=intent.scope,
        writer_lease=lease,
    )
    return lease


@pytest.mark.parametrize(
    (
        "average_price",
        "commission",
        "expected_average",
        "expected_commission",
        "source",
    ),
    [
        (Decimal("50001.25"), Decimal("-0.125"), "50001.25", "-0.125", "provider"),
        (Decimal("50001.25"), Decimal("-0.125"), "50001.25", "-0.125", "reconcile"),
        (None, None, None, None, "provider"),
    ],
)
def test_cancelled_event_persists_atomic_cumulative_target_snapshot(
    tmp_path,
    average_price,
    commission,
    expected_average,
    expected_commission,
    source,
):
    database = tmp_path / "execution.sqlite3"
    store = SqliteExecutionStore(database)
    try:
        lease = _partial_target(
            store,
            average_price=average_price,
            cumulative_commission=commission,
        )
        cancel = CancelIntent(
            cancel_id="cancel.snapshot",
            scope=_scope(),
            target_intent_id="order.snapshot",
            provider_order_id="provider.order.1",
            metadata_version="metadata.1",
        )
        store.admit_cancel(cancel, writer_lease=lease)
        store.activate_cancel(cancel.cancel_id, cancel.scope, "permit.cancel", writer_lease=lease)
        _cancel_record, claimed = store.claim_cancel_for_dispatch(
            cancel.cancel_id, cancel.scope, writer_lease=lease
        )
        assert claimed is True
        store.record_cancel_observation(
            CancelObservation.cancelled(
                cancel.cancel_id, cancel.target_intent_id, cancel.provider_order_id
            ),
            scope=cancel.scope,
            source=source,
            writer_lease=lease,
        )

        event = next(
            item for item in store.read_outbox() if item.event_type == "cancelled_by_cancel_intent"
        )
        assert event.state is ExecutionState.CANCELLED
        assert event.payload == {
            "cancel_id": "cancel.snapshot",
            "provider_order_id": "provider.order.1",
            "source": source,
            "filled_quantity": "0.75",
            "average_price": expected_average,
            "cumulative_commission": expected_commission,
        }
        cancelled_target = store.get("order.snapshot", scope=_scope())
        assert cancelled_target.state is ExecutionState.CANCELLED
        assert cancelled_target.filled_quantity == Decimal("0.75")
        assert cancelled_target.average_price == average_price
        assert cancelled_target.cumulative_commission == commission
    finally:
        store.close()

    reopened = SqliteExecutionStore(database)
    try:
        event = next(
            item
            for item in reopened.read_outbox()
            if item.event_type == "cancelled_by_cancel_intent"
        )
        assert event.payload["filled_quantity"] == "0.75"
        assert event.payload["average_price"] == expected_average
        assert event.payload["cumulative_commission"] == expected_commission
    finally:
        reopened.close()


@pytest.mark.parametrize(
    ("expected_state", "expected_event", "gate_result", "expected_reason"),
    [
        (ExecutionState.REJECTED, "intent_rejected", "denied", "admission_denied"),
        (ExecutionState.BLOCKED, "intent_blocked", "invalid", "invalid_admission_permit"),
    ],
)
def test_admission_reject_and_block_are_reason_only_no_dispatch_events(
    tmp_path, expected_state, expected_event, gate_result, expected_reason
):
    class Gate:
        def reserve(self, _intent):
            if gate_result == "denied":
                raise AdmissionDenied()
            return None

    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    facade = ManagedExecutionFacade(
        store,
        _scope(),
        writer_id="writer.test",
        admission_gate=Gate(),
    )
    provider_calls = []
    try:
        record = facade.submit(
            _intent(),
            lambda intent: (
                provider_calls.append(intent.intent_id)
                or ProviderObservation.accepted(intent.intent_id, "provider.order.1")
            ),
        )

        assert record.state is expected_state
        assert record.filled_quantity == Decimal("0")
        assert record.provider_order_id is None
        assert record.average_price is None
        assert record.cumulative_commission is None
        assert record.dispatch_attempts == 0
        assert provider_calls == []
        events = store.read_outbox()
        assert events[-1].event_type == expected_event
        assert events[-1].state is expected_state
        assert events[-1].payload == {"reason_code": expected_reason}
        assert not any(
            event.event_type in {"intent_admitted", "dispatch_claimed", "provider_observation"}
            for event in events
        )
    finally:
        store.close()


def test_reject_intent_refuses_pending_row_with_dispatch_evidence(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    try:
        intent = _intent("order.corrupt-pending")
        lease = _writer_lease(store)
        store.admit_intent(intent, writer_lease=lease)
        with store._transaction() as cursor:
            cursor.execute(
                "UPDATE execution_records SET filled_quantity = ? WHERE scope_key = ? AND intent_id = ?",
                ("0.25", intent.scope.key, intent.intent_id),
            )

        with pytest.raises(InvalidStateTransition, match="pending admission has dispatch evidence"):
            store.reject_intent(
                intent.intent_id,
                intent.scope,
                "admission_denied",
                writer_lease=lease,
            )

        events = store.read_outbox()
        assert [event.event_type for event in events] == ["intent_recorded"]
    finally:
        store.close()


def test_missing_fee_after_partial_growth_stays_unknown_through_cancel(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    try:
        lease = _partial_target(
            store,
            average_price=Decimal("50001.25"),
            cumulative_commission=Decimal("-0.125"),
        )
        second_partial = ProviderObservation(
            "order.snapshot",
            ExecutionState.PARTIALLY_FILLED,
            provider_order_id="provider.order.1",
            filled_quantity=Decimal("1.25"),
            average_price=Decimal("50002.50"),
            cumulative_commission=None,
        )
        store.record_observation(
            second_partial,
            scope=_scope(),
            source="reconcile",
            writer_lease=lease,
        )
        current = store.get("order.snapshot", scope=_scope())
        assert current.filled_quantity == Decimal("1.25")
        assert current.average_price == Decimal("50002.50")
        assert current.cumulative_commission is None

        partial_events = [
            event
            for event in store.read_outbox()
            if event.event_type in {"provider_observation", "reconciled_observation"}
        ]
        assert partial_events[-1].payload["filled_quantity"] == "1.25"
        assert partial_events[-1].payload["cumulative_commission"] is None
        assert partial_events[-2].payload["cumulative_commission"] == "-0.125"

        cancel = CancelIntent(
            cancel_id="cancel.unknown-fee",
            scope=_scope(),
            target_intent_id="order.snapshot",
            provider_order_id="provider.order.1",
            metadata_version="metadata.1",
        )
        store.admit_cancel(cancel, writer_lease=lease)
        store.activate_cancel(cancel.cancel_id, cancel.scope, "permit.cancel", writer_lease=lease)
        store.claim_cancel_for_dispatch(cancel.cancel_id, cancel.scope, writer_lease=lease)
        store.record_cancel_observation(
            CancelObservation.cancelled(
                cancel.cancel_id, cancel.target_intent_id, cancel.provider_order_id
            ),
            scope=cancel.scope,
            writer_lease=lease,
        )

        cancel_event = next(
            event
            for event in store.read_outbox()
            if event.event_type == "cancelled_by_cancel_intent"
        )
        assert cancel_event.payload["filled_quantity"] == "1.25"
        assert cancel_event.payload["average_price"] == "50002.50"
        assert cancel_event.payload["cumulative_commission"] is None
    finally:
        store.close()


def test_cancel_snapshot_and_target_transition_roll_back_together(tmp_path, monkeypatch):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    try:
        lease = _partial_target(
            store,
            average_price=Decimal("50001.25"),
            cumulative_commission=Decimal("-0.125"),
        )
        cancel = CancelIntent(
            cancel_id="cancel.atomic",
            scope=_scope(),
            target_intent_id="order.snapshot",
            provider_order_id="provider.order.1",
            metadata_version="metadata.1",
        )
        store.admit_cancel(cancel, writer_lease=lease)
        store.activate_cancel(cancel.cancel_id, cancel.scope, "permit.cancel", writer_lease=lease)
        store.claim_cancel_for_dispatch(cancel.cancel_id, cancel.scope, writer_lease=lease)

        append_event = store._append_event

        def fail_cancel_snapshot(*args, **kwargs):
            if kwargs.get("event_type") == "cancelled_by_cancel_intent":
                raise RuntimeError("injected outbox failure")
            return append_event(*args, **kwargs)

        monkeypatch.setattr(store, "_append_event", fail_cancel_snapshot)
        with pytest.raises(RuntimeError, match="injected outbox failure"):
            store.record_cancel_observation(
                CancelObservation.cancelled(
                    cancel.cancel_id, cancel.target_intent_id, cancel.provider_order_id
                ),
                scope=cancel.scope,
                writer_lease=lease,
            )

        target = store.get("order.snapshot", scope=_scope())
        assert target.state is ExecutionState.PARTIALLY_FILLED
        assert target.filled_quantity == Decimal("0.75")
        assert target.average_price == Decimal("50001.25")
        assert target.cumulative_commission == Decimal("-0.125")
        assert (
            store.get_cancel(cancel.cancel_id, scope=cancel.scope).state
            is ExecutionState.DISPATCHING
        )
        assert not any(
            event.event_type == "cancelled_by_cancel_intent" for event in store.read_outbox()
        )
    finally:
        store.close()
