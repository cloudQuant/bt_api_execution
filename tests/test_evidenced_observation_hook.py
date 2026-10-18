from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest

from bt_api_execution import (
    ExecutionScope,
    ExecutionState,
    InvalidStateTransition,
    ManagedExecutionFacade,
    OrderIntent,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
)


@dataclass(frozen=True)
class _Permit:
    permit_id: str


class _ObservationGate:
    def __init__(self, *, fail_event_type: str | None = None) -> None:
        self.fail_event_type = fail_event_type
        self.settled: list[str] = []
        self.legacy_outcomes: list[tuple[str, ExecutionState]] = []
        self.observations: list[tuple[str, str, ExecutionState]] = []
        self.observation_events: list[object] = []

    def reserve(self, intent: OrderIntent) -> _Permit:
        return _Permit(f"permit-{intent.intent_id}")

    def validate(self, permit_reference: str, intent: OrderIntent) -> _Permit:
        assert permit_reference == f"permit-{intent.intent_id}"
        return _Permit(permit_reference)

    def claim_for_dispatch(self, permit_reference: str, intent: OrderIntent) -> _Permit:
        assert permit_reference == f"permit-{intent.intent_id}"
        return _Permit(permit_reference)

    def release(self, permit_reference: str, reason: str) -> None:
        return None

    def settle(self, permit_reference: str) -> None:
        self.settled.append(permit_reference)

    def after_evidenced_outcome(self, record, *, writer_lease) -> None:
        self.legacy_outcomes.append((record.intent_id, record.state))

    def after_evidenced_observation(self, record, event, *, writer_lease) -> None:
        self.observations.append((event.event_id, event.event_type, event.state))
        self.observation_events.append(event)
        if event.event_type == self.fail_event_type:
            raise RuntimeError("observation reconciliation failed")


def _scope() -> ExecutionScope:
    return ExecutionScope("FAKE", "simulation", "acct_demo", "strategy.demo", "20261018")


def _intent() -> OrderIntent:
    return OrderIntent.limit(
        intent_id="signal-1",
        scope=_scope(),
        signal_id="signal-1",
        instrument="BTC-USDT",
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("50000"),
        metadata_version="metadata-v1",
    )


def _cancelled() -> ProviderObservation:
    return ProviderObservation(
        "signal-1",
        ExecutionState.CANCELLED,
        provider_order_id="provider-order-1",
    )


@pytest.mark.unit
def test_new_observation_hook_sees_initial_and_later_events_once(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    gate = _ObservationGate()
    facade = ManagedExecutionFacade(
        store,
        _scope(),
        writer_id="writer-a",
        admission_gate=gate,
    )
    provider_calls: list[str] = []

    def provider(intent: OrderIntent) -> ProviderObservation:
        provider_calls.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

    try:
        first = facade.submit(_intent(), provider)
        assert first.state is ExecutionState.ACKED
        assert len(gate.observations) == 1
        assert gate.observations[0][1:] == (
            "provider_observation",
            ExecutionState.ACKED,
        )
        first_event_id = gate.observations[0][0]

        terminal_event = facade.record_provider_observation_event(_cancelled())
        assert terminal_event is not None
        assert gate.observation_events[1] is terminal_event
        assert gate.observations == [
            (first_event_id, "provider_observation", ExecutionState.ACKED),
            (terminal_event.event_id, "reconciled_observation", ExecutionState.CANCELLED),
        ]
        assert gate.settled == ["permit-signal-1"]
        assert gate.legacy_outcomes == [("signal-1", ExecutionState.ACKED)]

        assert facade.record_provider_observation_event(_cancelled()) is None
        duplicate = facade.submit(_intent(), lambda intent: provider_calls.append("duplicate"))
        assert duplicate.state is ExecutionState.CANCELLED
        assert provider_calls == ["signal-1"]
        assert len(gate.observations) == 2
        assert gate.settled == ["permit-signal-1"]
    finally:
        facade.close()
        store.close()


@pytest.mark.unit
def test_observation_hook_failure_latches_durable_event_without_redispatch(tmp_path) -> None:
    path = tmp_path / "execution.sqlite3"
    store = SqliteExecutionStore(path)
    gate = _ObservationGate(fail_event_type="reconciled_observation")
    facade = ManagedExecutionFacade(
        store,
        _scope(),
        writer_id="writer-a",
        admission_gate=gate,
    )
    provider_calls: list[str] = []

    def provider(intent: OrderIntent) -> ProviderObservation:
        provider_calls.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

    try:
        assert facade.submit(_intent(), provider).state is ExecutionState.ACKED
        with pytest.raises(InvalidStateTransition, match="requires manual review"):
            facade.record_provider_observation_event(_cancelled())

        durable = store.get("signal-1", scope=_scope())
        assert durable is not None
        assert durable.state is ExecutionState.CANCELLED
        assert durable.review_required is True
        events = store.read_outbox()
        assert [event.event_type for event in events][-2:] == [
            "reconciled_observation",
            "manual_review_required",
        ]
        assert events[-2].state is ExecutionState.CANCELLED
        assert events[-1].payload["reason_code"] == (
            "evidenced_observation_reconciliation_failed"
        )
        assert len(gate.observations) == 2
        assert gate.settled == ["permit-signal-1"]
        assert provider_calls == ["signal-1"]
    finally:
        facade.close()
        store.close()

    reopened = SqliteExecutionStore(path)
    try:
        durable = reopened.get("signal-1", scope=_scope())
        assert durable is not None
        assert durable.state is ExecutionState.CANCELLED
        assert durable.review_required is True
        assert reopened.read_outbox()[-2].event_type == "reconciled_observation"
        assert reopened.read_outbox()[-1].event_type == "manual_review_required"
    finally:
        reopened.close()
