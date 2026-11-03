"""Failure boundaries for the current optional Paper recovery contract.

This is an isolated SQLite/fake-source test, not a real account or a
replacement for legacy migration and original permit/quote positive coverage.
"""

from dataclasses import replace
from decimal import Decimal

import pytest
import test_paper_risk_snapshot as fixtures
from bt_api_risk import RiskAdmissionDeniedError

from bt_api_execution import ExecutionState


def test_expired_persisted_paper_quote_stays_blocked_after_reopen(tmp_path, monkeypatch):
    """An expired saved quote must not be replaced by a new quote on restart."""
    import bt_api_execution.paper_execution as paper_execution_module
    import bt_api_execution.paper_risk as paper_risk_module

    now = [950]
    monkeypatch.setattr(paper_execution_module, "time_ns", lambda: now[0])
    monkeypatch.setattr(paper_risk_module, "time_ns", lambda: now[0])
    intent = fixtures._order()
    store, gate, adapter, _matcher, source = fixtures._composition(
        tmp_path, fixtures._quote(as_of_ns=900), max_quote_age_ns=100
    )
    lease = fixtures._lease_and_record(store, intent)
    try:
        permit = adapter.reserve_for_execution(intent, writer_lease=lease)
        assert source.calls == ["BTC-USDT"]
        assert (
            store.get(intent.intent_id, scope=intent.scope).state
            is ExecutionState.PENDING_ADMISSION
        )
    finally:
        fixtures._close(store, gate, lease)

    # A new live quote is offered, but it cannot substitute for the old
    # admission evidence or silently extend that evidence's validity.
    now[0] = 1000
    newer_quote = replace(fixtures._quote(as_of_ns=999, ask="10.20"), cursor=2)
    reopened, risk, restarted, _new_matcher, new_source = fixtures._composition(
        tmp_path, newer_quote, max_quote_age_ns=100
    )
    new_lease = fixtures._lease_and_record(reopened, intent)
    try:
        with pytest.raises(RiskAdmissionDeniedError) as denied:
            restarted.reserve_for_execution(intent, writer_lease=new_lease)
        assert denied.value.code == "PAPER_ADMISSION_SNAPSHOT_EXPIRED"
        assert new_source.calls == []
        assert (
            reopened.get(intent.intent_id, scope=intent.scope).state
            is ExecutionState.PENDING_ADMISSION
        )
        assert risk.get_strategy_allocation(
            fixtures._RISK_SCOPE, fixtures._STRATEGY
        ).reserved_notional == Decimal("12.5375")
        assert permit.permit_id
    finally:
        fixtures._close(reopened, risk, new_lease)
