"""Iteration 60, step 60.07: per-strategy budgets on one shared account.

Example from the plan: the account holds 1,000,000; strategy A is allocated
200,000 and strategy B 300,000. Budgets are internal limits, not exchange
balances. Policy v1 (gross exposure at cost) is enforced by
``StrategyBudgetGate`` chained with ``StrategyCloseCapacityGate``.
"""

from __future__ import annotations

import dataclasses
import threading
from decimal import Decimal

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    CancelObservation,
    ExecutionScope,
    ExecutionState,
    ProviderObservation,
    SqliteExecutionStore,
    StrategyBudgetGate,
    StrategyCloseCapacityGate,
)
from bt_api_execution.service_order_entry import SUBMIT_OPERATION, ExecutionOrderEntryService

_ACCOUNT = "account-budget"
_DOMAIN = "paper:budget"
_INSTRUMENT = "PAPER:SIM:SA2701"
_MULTIPLIER = Decimal("20")  # 20 units per lot


class _Provider:
    def __init__(self):
        self.submitted = []
        self.fail = set()

    def submit(self, intent):
        self.submitted.append(intent.intent_id)
        if intent.intent_id in self.fail:
            raise TimeoutError("provider response lost")
        return ProviderObservation.accepted(intent.intent_id, f"provider-{intent.intent_id}")

    def cancel(self, intent):
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )


class _Account:
    def __init__(self, path, *, budgets=None):
        self.store = SqliteExecutionStore(path)
        self.provider = _Provider()
        self.scopes = {
            n: ExecutionScope("PAPER", "simulation", _ACCOUNT, f"strategy-{n}", "20261102")
            for n in ("a", "b", "c")
        }
        self.wires = {
            n: WireScope(_DOMAIN, account_ref=_ACCOUNT, strategy_ref=f"strategy-{n}")
            for n in ("a", "b", "c")
        }
        if budgets is None:
            budgets = {"a": Decimal("200000"), "b": Decimal("300000")}
        self.budget = StrategyBudgetGate(
            self.store,
            budgets={self.scopes[n].key: v for n, v in budgets.items()},
            multipliers={_INSTRUMENT: _MULTIPLIER},
        )
        self.gate = StrategyCloseCapacityGate(self.store, inner=self.budget)
        self.service = ExecutionOrderEntryService(
            self.store,
            {self.wires[n]: self.scopes[n] for n in self.scopes},
            self.provider,
            admission_gate=self.gate,
        )

    def submit(self, name, intent_id, *, side="BUY", quantity, price="1500", effect="OPEN",
               order_type="LIMIT"):
        arguments = {
            "intent_id": intent_id,
            "signal_id": f"signal-{intent_id}",
            "instrument": _INSTRUMENT,
            "side": side,
            "position_effect": effect,
            "order_type": order_type,
            "quantity": quantity,
            "price": price,
            "time_in_force": "GTC",
            "reduce_only": effect != "OPEN",
            "metadata_version": "metadata-v1",
            "deadline_ns": None,
            "tags": {},
        }
        return self.service.handle(
            SUBMIT_OPERATION,
            principal_ref="client",
            identity=WireIdentity(WireRole.STRATEGY_CLIENT, "client", f"strategy-{name}"),
            scope=self.wires[name],
            arguments=arguments,
        )

    def observe(self, name, intent_id, state, filled, price="1500"):
        return self.service._facade_pair(self.scopes[name])[0].reconcile(
            ProviderObservation(
                intent_id,
                state,
                provider_order_id=f"provider-{intent_id}",
                filled_quantity=Decimal(filled),
                average_price=Decimal(price) if Decimal(filled) else None,
            )
        )

    def close(self):
        for scope in self.scopes.values():
            self.service._facade_pair(scope)[0].close()
        self.store.close()


def _admitted(reply):
    record = (reply.get("result") or {}).get("record") or {}
    return reply.get("ok") is True and record.get("state") != "REJECTED"


def _probe(account, intent_id):
    """A not-yet-submitted intent of the same strategy, used to read committed capital."""
    stored = account.store.get_intent(intent_id, scope=account.scopes["a"])
    return dataclasses.replace(stored, intent_id="probe")


@pytest.fixture
def account(tmp_path):
    item = _Account(tmp_path / "budget.sqlite3")
    try:
        yield item
    finally:
        item.close()


# 1500 * 20 = 30,000 per lot: A (200,000) can hold 6 lots, B (300,000) 10.


@pytest.mark.unit
def test_each_strategy_is_limited_by_its_own_budget(account):
    assert _admitted(account.submit("a", "a1", quantity="6"))
    assert not _admitted(account.submit("a", "a2", quantity="1"))  # 210,000 > 200,000
    # B's budget is independent of A's usage.
    assert _admitted(account.submit("b", "b1", quantity="10"))
    assert not _admitted(account.submit("b", "b2", quantity="1"))
    assert account.budget.committed(_probe(account, "a1")) == Decimal("180000")


@pytest.mark.unit
def test_budget_is_not_the_account_balance(account):
    # The account may hold 1,000,000, but A is capped at 200,000.
    assert not _admitted(account.submit("a", "a-big", quantity="7"))
    # A strategy without a configured budget is refused (fail closed).
    assert not _admitted(account.submit("c", "c1", quantity="1"))


@pytest.mark.unit
def test_concurrent_opens_cannot_double_spend_one_budget(account):
    replies = {}
    barrier = threading.Barrier(2)

    def worker(intent_id):
        barrier.wait()
        replies[intent_id] = account.submit("a", intent_id, quantity="4")  # 120,000 each

    threads = [threading.Thread(target=worker, args=(f"a-{i}",)) for i in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(_admitted(r) for r in replies.values()) == 1, replies


@pytest.mark.unit
def test_cancel_releases_unknown_keeps_and_fills_move_to_position(account):
    account.provider.fail.add("a-lost")
    account.submit("a", "a-lost", quantity="6")
    assert account.store.get("a-lost", scope=account.scopes["a"]).state is ExecutionState.UNKNOWN
    # UNKNOWN keeps the whole reservation.
    assert not _admitted(account.submit("a", "a-next", quantity="1"))
    # Reconciliation: 2 lots filled at 1500, the rest cancelled.
    account.observe("a", "a-lost", ExecutionState.CANCELLED, "2")
    assert account.budget.committed(_probe(account, "a-lost")) == Decimal("60000")
    assert _admitted(account.submit("a", "a-more", quantity="4"))  # 60k + 120k = 180k
    assert not _admitted(account.submit("a", "a-over", quantity="1"))


@pytest.mark.unit
def test_closing_frees_budget_at_average_cost(account):
    assert _admitted(account.submit("a", "a-open", quantity="6"))
    account.observe("a", "a-open", ExecutionState.FILLED, "6")
    assert not _admitted(account.submit("a", "a-blocked", quantity="1"))
    # Closing is limited by close capacity, not by budget.
    assert _admitted(account.submit("a", "a-close", side="SELL", quantity="2", effect="CLOSE"))
    account.observe("a", "a-close", ExecutionState.FILLED, "2", price="1600")
    assert _admitted(account.submit("a", "a-reopen", quantity="2"))  # 4 held + 2 = 6 lots
    assert not _admitted(account.submit("a", "a-reopen-2", quantity="1"))


@pytest.mark.unit
def test_opening_without_a_knowable_notional_is_refused(account):
    reply = account.submit("a", "a-market", quantity="1", price=None, order_type="MARKET")
    assert not _admitted(reply)
    assert "a-market" not in account.provider.submitted


@pytest.mark.unit
def test_budget_survives_restart(tmp_path):
    path = tmp_path / "restart.sqlite3"
    first = _Account(path)
    assert _admitted(first.submit("a", "a1", quantity="5"))
    first.close()
    second = _Account(path)
    try:
        assert not _admitted(second.submit("a", "a2", quantity="2"))  # 150k + 60k > 200k
        assert _admitted(second.submit("a", "a3", quantity="1"))
    finally:
        second.close()


@pytest.mark.unit
def test_unconfigured_multiplier_is_unknown_not_one(tmp_path):
    item = _Account(tmp_path / "no-multiplier.sqlite3")
    item.budget._multipliers = {}  # the instrument's multiplier is not configured
    try:
        reply = item.submit("a", "a1", quantity="1")
        assert not _admitted(reply)
        assert "a1" not in item.provider.submitted
    finally:
        item.close()


@pytest.mark.unit
def test_a_fill_without_average_price_makes_the_budget_unknown(account):
    assert _admitted(account.submit("a", "a1", quantity="1"))
    account.observe("a", "a1", ExecutionState.FILLED, "1")
    # Force the persisted fill to carry no price, as an incomplete report would.
    original = account.store.list_scope_intents

    def without_price(scope):
        return [
            (dataclasses.replace(record, average_price=None), stored)
            for record, stored in original(scope)
        ]

    account.store.list_scope_intents = without_price
    assert not _admitted(account.submit("a", "a2", quantity="1"))
    assert "a2" not in account.provider.submitted
