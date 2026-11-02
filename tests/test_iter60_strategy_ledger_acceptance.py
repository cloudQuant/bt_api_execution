"""Iteration 60, step 60.06: one account, several strategies, one instrument.

Acceptance matrix for the single-process strategy ledger built from the
existing execution store, order entry and account-book reconciliation, plus
the owner-scoped close-capacity gate:

* zero baseline: A buys 2, B buys 3 -> books 2 / 3, account 5, BALANCED;
* A cannot close more than A owns and cannot consume B's position;
* concurrent closes of one holding cannot oversell;
* cancel/reject release the reservation, UNKNOWN keeps it;
* duplicate fill observations are idempotent;
* external fills stay UNALLOCATED, a missing allocation baseline is PARTIAL;
* the books and close capacity survive a store reopen (restart);
* one strategy scope admits a single writer.
"""

from __future__ import annotations

import dataclasses
import threading
import time
from decimal import Decimal

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    AllocationOwnerKind,
    CancelObservation,
    CurrencyAmount,
    ExecutionAccountBookService,
    ExecutionAccountSnapshot,
    ExecutionAllocationBaseline,
    ExecutionOwnerScope,
    ExecutionScope,
    ExecutionScopeDirectory,
    ExecutionState,
    ExternalActivitySnapshot,
    ExternalFillFact,
    ManagedExecutionFacade,
    PositionFact,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
    StrategyCloseCapacityGate,
    WriterLeaseUnavailable,
)
from bt_api_execution.contracts import PositionEffect
from bt_api_execution.service_order_entry import SUBMIT_OPERATION, ExecutionOrderEntryService

_DOMAIN = "paper:iter60"
_ACCOUNT = "account-iter60"
_PRINCIPAL = "iter60-client"
_INSTRUMENT = "PAPER:SIM:SA2701"
_DAY = "20261102"


class _Provider:
    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.fail: set[str] = set()

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
    """One account composition with strategies A and B."""

    def __init__(self, path, *, baseline_positions=None) -> None:
        self.path = path
        self.store = SqliteExecutionStore(path)
        self.provider = _Provider()
        self.scopes = {
            name: ExecutionScope("PAPER", "simulation", _ACCOUNT, f"strategy-{name}", _DAY)
            for name in ("a", "b")
        }
        self.wires = {
            name: WireScope(_DOMAIN, account_ref=_ACCOUNT, strategy_ref=f"strategy-{name}")
            for name in ("a", "b")
        }
        self.identities = {
            name: WireIdentity(WireRole.STRATEGY_CLIENT, _PRINCIPAL, f"strategy-{name}")
            for name in ("a", "b")
        }
        self.gate = StrategyCloseCapacityGate(self.store, baseline_positions=baseline_positions)
        self.service = ExecutionOrderEntryService(
            self.store,
            {self.wires[name]: self.scopes[name] for name in ("a", "b")},
            self.provider,
            admission_gate=self.gate,
        )

    def submit(self, name, intent_id, *, side, quantity, effect="OPEN"):
        return self.service.handle(
            SUBMIT_OPERATION,
            principal_ref=_PRINCIPAL,
            identity=self.identities[name],
            scope=self.wires[name],
            arguments={
                "intent_id": intent_id,
                "signal_id": f"signal-{intent_id}",
                "instrument": _INSTRUMENT,
                "side": side,
                "position_effect": effect,
                "order_type": "LIMIT",
                "quantity": quantity,
                "price": "1500",
                "time_in_force": "GTC",
                "reduce_only": effect != "OPEN",
                "metadata_version": "metadata-v1",
                "deadline_ns": None,
                "tags": {},
            },
        )

    def observe(self, name, intent_id, state, filled, *, fee="0"):
        return self.service._facade_pair(self.scopes[name])[0].reconcile(
            ProviderObservation(
                intent_id,
                state,
                provider_order_id=f"provider-{intent_id}",
                filled_quantity=Decimal(filled),
                average_price=Decimal("1500") if Decimal(filled) else None,
                cumulative_commission=Decimal(fee),
                cumulative_commission_currency="CNY",
            )
        )

    def fill(self, name, intent_id, quantity):
        record = self.observe(name, intent_id, ExecutionState.FILLED, quantity, fee="1")
        assert record.state is ExecutionState.FILLED

    def directory(self, start_ns):
        return ExecutionScopeDirectory(
            account_key=self.scopes["a"].account_key,
            source="trusted-owner-registry",
            source_ref="registry-1",
            revision="owner-revision-1",
            covers_from_ns=start_ns,
            as_of_ns=start_ns + 3_600_000_000_000,
            scopes=tuple(
                ExecutionOwnerScope(
                    self.scopes[name],
                    AllocationOwnerKind.STRATEGY,
                    f"strategy-{name}",
                    self.wires[name],
                )
                for name in ("a", "b")
            ),
        )

    def close(self):
        for scope in self.scopes.values():
            self.service._facade_pair(scope)[0].close()
        self.store.close()


def _ok(reply):
    assert reply["ok"] is True and reply["code"] == "OK", reply
    return reply


def _denied(reply):
    assert reply["ok"] is not True or reply["code"] != "OK" or _state(reply) == "REJECTED", reply
    return reply


def _state(reply):
    record = (reply.get("result") or {}).get("record") or {}
    return record.get("state")


def _snapshots(account, *, start_ns, current_position, external=None, with_allocation=True):
    journal_generation = str(account.store.journal_source_identity()["generation"])
    baseline = ExecutionAccountSnapshot(
        account_key=account.scopes["a"].account_key,
        source="synthetic-provider",
        source_ref="positions-baseline",
        generation="provider-session-1",
        journal_generation=journal_generation,
        as_of_ns=start_ns,
        execution_cursor=0,
        position_mode="NET",
        positions=(),
        cumulative_fees=(),
        fee_coverage_start_ns=start_ns - 1,
    )
    allocation = (
        ExecutionAllocationBaseline(
            account_key=baseline.account_key,
            source="trusted-virtual-position-ledger",
            source_ref="allocation-zero",
            revision="allocation-revision-1",
            scope_directory_revision="owner-revision-1",
            provider_generation=baseline.generation,
            journal_generation=journal_generation,
            as_of_ns=baseline.as_of_ns,
            execution_cursor=0,
            positions=(),
        )
        if with_allocation
        else None
    )
    _rows, _first, head, _high = account.store.execution_outbox_bounds()
    now = max(time.time_ns(), start_ns + 1)
    current = ExecutionAccountSnapshot(
        account_key=baseline.account_key,
        source=baseline.source,
        source_ref="positions-current",
        generation=baseline.generation,
        journal_generation=journal_generation,
        as_of_ns=now,
        execution_cursor=head or 0,
        position_mode="NET",
        positions=(PositionFact(_INSTRUMENT, Decimal(current_position)),),
        cumulative_fees=(CurrencyAmount("CNY", Decimal("2")),),
        fee_coverage_start_ns=baseline.fee_coverage_start_ns,
    )
    activity = ExternalActivitySnapshot(
        account_key=baseline.account_key,
        source="synthetic-trade-feed",
        source_ref="activity-window",
        generation=baseline.generation,
        coverage_start_ns=baseline.as_of_ns,
        coverage_end_ns=now,
        fills=tuple(external or ()),
        orders=(),
    )
    return baseline, allocation, current, activity


def _book_quantity(result, owner_kind, owner_ref):
    book = next(
        (b for b in result.books if (b.owner_kind, b.owner_ref) == (owner_kind, owner_ref)),
        None,
    )
    if book is None:
        return Decimal("0")
    return sum(
        (p.net_quantity for p in book.current_positions if p.instrument == _INSTRUMENT),
        Decimal("0"),
    )


def _open_a2_b3(account):
    _ok(account.submit("a", "a-open", side="BUY", quantity="2"))
    _ok(account.submit("b", "b-open", side="BUY", quantity="3"))
    account.fill("a", "a-open", "2")
    account.fill("b", "b-open", "3")


@pytest.fixture
def account(tmp_path):
    item = _Account(tmp_path / "iter60.sqlite3")
    try:
        yield item
    finally:
        item.close()


@pytest.mark.unit
def test_two_strategies_zero_baseline_books_are_2_and_3_account_5(account):
    start = time.time_ns()
    _open_a2_b3(account)
    baseline, allocation, current, activity = _snapshots(
        account, start_ns=start, current_position="5"
    )
    result = ExecutionAccountBookService(account.store).reconcile(
        account.directory(start),
        baseline,
        current,
        allocation_baseline=allocation,
        external_activity=activity,
    )
    assert result.coverage.complete is True
    assert result.state == "BALANCED", result.state
    assert _book_quantity(result, AllocationOwnerKind.STRATEGY, "strategy-a") == Decimal("2")
    assert _book_quantity(result, AllocationOwnerKind.STRATEGY, "strategy-b") == Decimal("3")
    assert result.position_residual in ((), (PositionFact(_INSTRUMENT, Decimal("0")),))


@pytest.mark.unit
def test_strategy_cannot_close_more_than_it_owns_or_use_another_strategys_position(account):
    _open_a2_b3(account)
    intent = account.store.get_intent("a-open", scope=account.scopes["a"])
    probe = dataclasses.replace(
        intent,
        intent_id="probe",
        side=Side.SELL,
        position_effect=PositionEffect.CLOSE,
        reduce_only=True,
    )
    assert account.gate.closable_quantity(probe) == Decimal("2")
    # A owns 2; the account holds 5. Closing 3 would consume B's position.
    reply = account.submit("a", "a-close-3", side="SELL", quantity="3", effect="CLOSE")
    assert _state(reply) == "REJECTED", reply
    assert "a-close-3" not in account.provider.submitted
    # Closing exactly A's own 2 is admitted; B can still close its 3.
    _ok(account.submit("a", "a-close-2", side="SELL", quantity="2", effect="CLOSE"))
    _ok(account.submit("b", "b-close-3", side="SELL", quantity="3", effect="CLOSE"))
    assert {"a-close-2", "b-close-3"} <= set(account.provider.submitted)


@pytest.mark.unit
def test_concurrent_closes_cannot_oversell_one_holding(account):
    _open_a2_b3(account)
    replies = {}
    barrier = threading.Barrier(2)

    def worker(intent_id):
        barrier.wait()
        replies[intent_id] = account.submit(
            "a", intent_id, side="SELL", quantity="2", effect="CLOSE"
        )

    threads = [threading.Thread(target=worker, args=(f"a-close-{i}",)) for i in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    admitted = [k for k, v in replies.items() if v["ok"] and _state(v) != "REJECTED"]
    assert len(admitted) == 1, replies
    dispatched = [i for i in account.provider.submitted if i.startswith("a-close-")]
    assert dispatched == admitted


@pytest.mark.unit
def test_unknown_close_keeps_reservation_and_cancel_releases_it(account):
    _open_a2_b3(account)
    # The provider response is lost: the close is latched UNKNOWN and keeps
    # its reservation, so nothing else can close A's 2.
    account.provider.fail.add("a-close-1")
    account.submit("a", "a-close-1", side="SELL", quantity="2", effect="CLOSE")
    record = account.store.get("a-close-1", scope=account.scopes["a"])
    assert record.state is ExecutionState.UNKNOWN
    reply = account.submit("a", "a-close-2", side="SELL", quantity="1", effect="CLOSE")
    assert _state(reply) == "REJECTED", reply
    # Reconciliation proves 1 filled and the rest cancelled: 1 lot is freed.
    account.observe("a", "a-close-1", ExecutionState.CANCELLED, "1")
    record = account.store.get("a-close-1", scope=account.scopes["a"])
    assert record.state is ExecutionState.CANCELLED
    assert record.filled_quantity == Decimal("1")
    _ok(account.submit("a", "a-close-3", side="SELL", quantity="1", effect="CLOSE"))
    reply = account.submit("a", "a-close-4", side="SELL", quantity="1", effect="CLOSE")
    assert _state(reply) == "REJECTED", reply


@pytest.mark.unit
def test_duplicate_fill_observation_is_idempotent(account):
    _open_a2_b3(account)
    bounds = account.store.execution_outbox_bounds()
    account.fill("a", "a-open", "2")
    assert account.store.execution_outbox_bounds() == bounds
    record = account.store.get("a-open", scope=account.scopes["a"])
    assert record.filled_quantity == Decimal("2")


@pytest.mark.unit
def test_external_fill_stays_unallocated(account):
    start = time.time_ns()
    _open_a2_b3(account)
    external = ExternalFillFact(
        "manual-terminal-fill-1",
        _INSTRUMENT,
        Side.BUY,
        Decimal("1"),
        Decimal("1500"),
        occurred_at_ns=start + 1,
        fee=Decimal("0"),
        fee_currency="CNY",
        provider_order_ref="manual-order-1",
    )
    baseline, allocation, current, activity = _snapshots(
        account, start_ns=start, current_position="6", external=[external]
    )
    result = ExecutionAccountBookService(account.store).reconcile(
        account.directory(start),
        baseline,
        current,
        allocation_baseline=allocation,
        external_activity=activity,
    )
    # The unattributed fill explains the extra lot, but only in the
    # UNALLOCATED book: it is never pushed into a strategy.
    assert result.state == "BALANCED"
    assert len(result.external_fills) == 1
    assert _book_quantity(result, AllocationOwnerKind.STRATEGY, "strategy-a") == Decimal("2")
    assert _book_quantity(result, AllocationOwnerKind.STRATEGY, "strategy-b") == Decimal("3")
    assert _book_quantity(result, AllocationOwnerKind.UNALLOCATED, "UNALLOCATED") == Decimal("1")


@pytest.mark.unit
def test_missing_allocation_baseline_is_partial_not_balanced(account):
    start = time.time_ns()
    _open_a2_b3(account)
    baseline, _allocation, current, activity = _snapshots(
        account, start_ns=start, current_position="5", with_allocation=False
    )
    result = ExecutionAccountBookService(account.store).reconcile(
        account.directory(start),
        baseline,
        current,
        allocation_baseline=None,
        external_activity=activity,
    )
    assert result.state == "PARTIAL"
    assert result.coverage.complete is False


@pytest.mark.unit
def test_books_and_close_capacity_survive_restart(tmp_path):
    path = tmp_path / "restart.sqlite3"
    first = _Account(path)
    _open_a2_b3(first)
    _ok(first.submit("a", "a-close-1", side="SELL", quantity="1", effect="CLOSE"))
    first.close()

    second = _Account(path)
    try:
        # Capacity is recomputed from durable facts: A owns 2, 1 is in flight.
        reply = second.submit("a", "a-close-2", side="SELL", quantity="2", effect="CLOSE")
        assert _state(reply) == "REJECTED", reply
        _ok(second.submit("a", "a-close-3", side="SELL", quantity="1", effect="CLOSE"))
        record = second.store.get("b-open", scope=second.scopes["b"])
        assert record.filled_quantity == Decimal("3")
    finally:
        second.close()


@pytest.mark.unit
def test_baseline_allocation_is_closable(tmp_path):
    scope = ExecutionScope("PAPER", "simulation", _ACCOUNT, "strategy-a", _DAY)
    item = _Account(
        tmp_path / "baseline.sqlite3",
        baseline_positions={(scope.key, _INSTRUMENT, "NET"): Decimal("4")},
    )
    try:
        _ok(item.submit("a", "a-close-4", side="SELL", quantity="4", effect="CLOSE"))
        reply = item.submit("b", "b-close-1", side="SELL", quantity="1", effect="CLOSE")
        assert _state(reply) == "REJECTED", reply
    finally:
        item.close()


@pytest.mark.unit
def test_one_strategy_scope_admits_a_single_writer(tmp_path):
    store = SqliteExecutionStore(tmp_path / "writer.sqlite3")
    scope = ExecutionScope("PAPER", "simulation", _ACCOUNT, "strategy-a", _DAY)
    gate = StrategyCloseCapacityGate(store)
    first = ManagedExecutionFacade(store, scope, writer_id="run-1", admission_gate=gate)
    second = ManagedExecutionFacade(store, scope, writer_id="run-2", admission_gate=gate)
    try:
        first.acquire_writer_lease()
        with pytest.raises(WriterLeaseUnavailable):
            second.acquire_writer_lease()
    finally:
        first.close()
        store.close()
