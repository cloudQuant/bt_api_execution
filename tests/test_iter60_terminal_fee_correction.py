"""Iteration 60 R3: late fees after a terminal order, and the public observation entry.

A terminal execution record stays immutable except for one thing: a later
fee-only report (same terminal state and cumulative quantity, explicit fee
currency) is journaled as ``provider_commission_evidence`` with the new
cumulative fee. Duplicates are idempotent; a lower total, another currency, a
fee for a record whose earlier fee had no currency, or changed execution facts
are refused and write nothing. ``ExecutionOrderEntryService.record_provider_observation``
is the public host entry for such later observations. UNIT/FAKE (temp SQLite).
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from bt_api_base.service_protocol import WireScope

from bt_api_execution import (
    ContractValidationError,
    ExecutionScope,
    ExecutionState,
    InvalidStateTransition,
    ManagedExecutionFacade,
    OrderIntent,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
)
from bt_api_execution.service_order_entry import ExecutionOrderEntryService


def _scope() -> ExecutionScope:
    return ExecutionScope("FAKE", "simulation", "acct_demo", "strategy.demo", "20261104")


def _intent(intent_id: str = "fee-1") -> OrderIntent:
    return OrderIntent.limit(
        intent_id=intent_id,
        scope=_scope(),
        signal_id="signal-" + intent_id,
        instrument="BTC-USDT",
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("50000"),
        metadata_version="metadata-v1",
    )


def _filled(fee=None, currency=None, *, state=ExecutionState.FILLED, filled="2", intent="fee-1"):
    return ProviderObservation(
        intent,
        state,
        provider_order_id="provider-" + intent,
        filled_quantity=Decimal(filled),
        average_price=Decimal("50000"),
        cumulative_commission=None if fee is None else Decimal(fee),
        cumulative_commission_currency=currency,
    )


def _fee_events(store: SqliteExecutionStore, intent_id: str = "fee-1"):
    return [
        event
        for event in store.read_outbox(after_sequence=0, limit=500)
        if event.intent_id == intent_id and event.event_type == "provider_commission_evidence"
    ]


@pytest.fixture
def filled(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    facade = ManagedExecutionFacade(store, _scope(), writer_id="writer-a", allow_unprotected=True)
    facade.submit(
        _intent(), lambda intent: ProviderObservation.accepted(intent.intent_id, "provider-fee-1")
    )
    record = facade.record_provider_observation(_filled("1", "USDT"))
    assert record.state is ExecutionState.FILLED and record.cumulative_commission == 1
    try:
        yield store, facade
    finally:
        store.close()


@pytest.mark.unit
def test_late_cumulative_fee_on_a_terminal_record_is_journaled_once(filled) -> None:
    store, facade = filled
    record = facade.record_provider_observation(_filled("1.5", "USDT"))
    assert record.state is ExecutionState.FILLED
    assert record.cumulative_commission == Decimal("1.5")
    (event,) = _fee_events(store)
    assert event.state is ExecutionState.FILLED or str(event.state) == "FILLED"
    assert event.payload["cumulative_commission"] == "1.5"
    assert event.payload["cumulative_commission_currency"] == "USDT"
    assert event.payload["filled_quantity"] == "2"
    # Idempotent: the same cumulative total writes nothing.
    assert facade.record_provider_observation_event(_filled("1.5", "USDT")) is None
    assert len(_fee_events(store)) == 1
    # A further correction is a second event.
    assert facade.record_provider_observation_event(_filled("2", "USDT")) is not None
    assert [e.payload["cumulative_commission"] for e in _fee_events(store)] == ["1.5", "2"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("observation", "message"),
    [
        (_filled("0.5", "USDT"), "regressed"),
        (_filled("3", "BTC"), "currency changed"),
        (_filled("3", None), "requires a fee and its currency"),
    ],
)
def test_terminal_fee_regression_or_currency_change_is_refused(filled, observation, message):
    store, facade = filled
    before = store.read_outbox(after_sequence=0, limit=500)
    with pytest.raises(InvalidStateTransition, match=message):
        facade.record_provider_observation(observation)
    assert store.read_outbox(after_sequence=0, limit=500) == before
    record = facade.get("fee-1")
    assert record.cumulative_commission == 1 and record.review_required is False


@pytest.mark.unit
def test_fee_without_recorded_currency_cannot_be_corrected(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    try:
        facade = ManagedExecutionFacade(
            store, _scope(), writer_id="writer-a", allow_unprotected=True
        )
        facade.submit(
            _intent(),
            lambda intent: ProviderObservation.accepted(intent.intent_id, "provider-fee-1"),
        )
        facade.record_provider_observation(_filled("1", None))
        with pytest.raises(InvalidStateTransition, match="no currency"):
            facade.record_provider_observation(_filled("2", "USDT"))
        # A terminal record without any fee accepts its first late fee.
        facade.submit(
            _intent("fee-2"),
            lambda intent: ProviderObservation.accepted(intent.intent_id, "provider-fee-2"),
        )
        facade.record_provider_observation(_filled(intent="fee-2"))
        record = facade.record_provider_observation(_filled("0.7", "USDT", intent="fee-2"))
        assert record.cumulative_commission == Decimal("0.7")
        assert len(_fee_events(store, "fee-2")) == 1
    finally:
        store.close()


@pytest.mark.unit
def test_changed_execution_facts_on_a_terminal_record_stay_ignored(filled) -> None:
    store, facade = filled
    before = store.read_outbox(after_sequence=0, limit=500)
    # Another quantity is not a fee-only report: the terminal record is kept.
    record = facade.record_provider_observation(
        _filled("9", "USDT", state=ExecutionState.PARTIALLY_FILLED, filled="1")
    )
    assert record.state is ExecutionState.FILLED and record.cumulative_commission == 1
    assert store.read_outbox(after_sequence=0, limit=500) == before


@pytest.mark.unit
def test_order_entry_public_observation_entry_uses_the_served_scope(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    template = WireScope("fee-domain", account_ref="acct_demo", strategy_ref="strategy.demo")

    class _Provider:
        def submit(self, intent):
            return ProviderObservation.accepted(intent.intent_id, "provider-" + intent.intent_id)

        def cancel(self, intent):  # pragma: no cover - not used
            raise AssertionError

    try:
        service = ExecutionOrderEntryService(
            store, {template: _scope()}, _Provider(), allow_unprotected=True
        )
        facade = service._facade_pair(_scope())[0]  # the order entry's own facade
        facade.submit(
            _intent(),
            lambda intent: ProviderObservation.accepted(intent.intent_id, "provider-fee-1"),
        )
        run_scope = WireScope(
            "fee-domain", account_ref="acct_demo", strategy_ref="strategy.demo", run_id="run-1"
        )
        record = service.record_provider_observation(run_scope, _filled("1", "USDT"))
        assert record.state is ExecutionState.FILLED
        record = service.record_provider_observation(template, _filled("2", "USDT"))
        assert record.cumulative_commission == 2
        other = WireScope("fee-domain", account_ref="acct_demo", strategy_ref="other")
        with pytest.raises(ContractValidationError, match="not served"):
            service.record_provider_observation(other, _filled("3", "USDT"))
        with pytest.raises(ContractValidationError, match="invalid provider observation"):
            service.record_provider_observation(template, {"intent_id": "fee-1"})
    finally:
        store.close()
