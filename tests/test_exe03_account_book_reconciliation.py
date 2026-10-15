"""EXE03 account-book reconciliation through the execution order entry."""

from __future__ import annotations

import time
from decimal import Decimal

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    AccountBookReconciliation,
    AllocationOwnerKind,
    CancelObservation,
    ContractValidationError,
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
    ExternalOrderFact,
    OwnerPosition,
    PositionFact,
    ProviderObservation,
    QuarantinedExecutionIntent,
    Side,
    SqliteExecutionStore,
)
from bt_api_execution.service_order_entry import SUBMIT_OPERATION, ExecutionOrderEntryService

_DOMAIN = "paper:demo"
_ACCOUNT_REF = "account-exe03"
_PRINCIPAL = "curve-exe03-client"
_INSTRUMENT = "PAPER:SIM:BTC-USDT"


class _SyntheticProvider:
    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.cancelled: list[str] = []

    def submit(self, intent):
        self.submitted.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, f"provider-{intent.intent_id}")

    def cancel(self, intent):
        self.cancelled.append(intent.cancel_id)
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )


def _order_arguments(intent_id: str, *, side: str, quantity: str) -> dict[str, object]:
    return {
        "intent_id": intent_id,
        "signal_id": f"signal-{intent_id}",
        "instrument": _INSTRUMENT,
        "side": side,
        "position_effect": "OPEN",
        "order_type": "LIMIT",
        "quantity": quantity,
        "price": "10",
        "time_in_force": "GTC",
        "reduce_only": False,
        "metadata_version": "metadata-v1",
        "deadline_ns": None,
        "tags": {},
    }


def _submit(
    service: ExecutionOrderEntryService,
    *,
    scope: WireScope,
    identity: WireIdentity,
    intent_id: str,
    side: str,
    quantity: str,
) -> dict[str, object]:
    reply = service.handle(
        SUBMIT_OPERATION,
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=scope,
        arguments=_order_arguments(intent_id, side=side, quantity=quantity),
    )
    assert reply["ok"] is True and reply["code"] == "OK"
    return reply


def _observe_partial(
    service: ExecutionOrderEntryService,
    *,
    scope: ExecutionScope,
    intent_id: str,
    quantity: str,
    price: str,
    fee: str,
) -> None:
    record = service._facade_pair(scope)[0].reconcile(
        ProviderObservation(
            intent_id,
            ExecutionState.PARTIALLY_FILLED,
            provider_order_id=f"provider-{intent_id}",
            filled_quantity=Decimal(quantity),
            average_price=Decimal(price),
            cumulative_commission=Decimal(fee),
            cumulative_commission_currency="USD",
        )
    )
    assert record.state is ExecutionState.PARTIALLY_FILLED
    assert record.filled_quantity == Decimal(quantity)


def _book_for(result: AccountBookReconciliation, kind: AllocationOwnerKind, ref: str):
    return next(book for book in result.books if (book.owner_kind, book.owner_ref) == (kind, ref))


def _quantity(book, instrument: str = _INSTRUMENT) -> Decimal:
    return next(
        (
            position.net_quantity
            for position in book.current_positions
            if position.instrument == instrument
        ),
        Decimal("0"),
    )


@pytest.mark.unit
def test_exe03_entry_observations_allocate_strategy_manual_and_unallocated_residual(
    tmp_path,
) -> None:
    store = SqliteExecutionStore(tmp_path / "exe03.sqlite3")
    provider = _SyntheticProvider()
    strategy_a = ExecutionScope("PAPER", "simulation", _ACCOUNT_REF, "strategy-a", "20261015")
    strategy_b = ExecutionScope("PAPER", "simulation", _ACCOUNT_REF, "strategy-b", "20261015")
    manual_scope = ExecutionScope(
        "PAPER", "simulation", _ACCOUNT_REF, "manual-execution", "20261015"
    )
    wire_a = WireScope(_DOMAIN, account_ref=_ACCOUNT_REF, strategy_ref="strategy-a")
    wire_b = WireScope(_DOMAIN, account_ref=_ACCOUNT_REF, strategy_ref="strategy-b")
    wire_manual = WireScope(_DOMAIN, account_ref=_ACCOUNT_REF, manual_book_ref="manual-book-a")
    identity_a = WireIdentity(WireRole.STRATEGY_CLIENT, _PRINCIPAL, "strategy-a")
    identity_b = WireIdentity(WireRole.STRATEGY_CLIENT, _PRINCIPAL, "strategy-b")
    identity_manual = WireIdentity(WireRole.APPLICATION_CLIENT, _PRINCIPAL, "operator")
    service = ExecutionOrderEntryService(
        store,
        {wire_a: strategy_a, wire_b: strategy_b, wire_manual: manual_scope},
        provider,
        allow_unprotected=True,
    )
    journal_generation = str(store.journal_source_identity()["generation"])
    directory_start_ns = time.time_ns()
    directory = ExecutionScopeDirectory(
        account_key=strategy_a.account_key,
        source="trusted-owner-registry",
        source_ref="registry-snapshot-1",
        revision="owner-revision-1",
        covers_from_ns=directory_start_ns,
        as_of_ns=directory_start_ns + 10_000_000_000,
        scopes=(
            ExecutionOwnerScope(strategy_a, AllocationOwnerKind.STRATEGY, "strategy-a", wire_a),
            ExecutionOwnerScope(strategy_b, AllocationOwnerKind.STRATEGY, "strategy-b", wire_b),
            ExecutionOwnerScope(
                manual_scope, AllocationOwnerKind.MANUAL_BOOK, "manual-book-a", wire_manual
            ),
        ),
    )
    try:
        _submit(
            service,
            scope=wire_a,
            identity=identity_a,
            intent_id="intent-strategy-a",
            side="BUY",
            quantity="10",
        )
        _submit(
            service,
            scope=wire_b,
            identity=identity_b,
            intent_id="intent-strategy-a",
            side="BUY",
            quantity="5",
        )
        _submit(
            service,
            scope=wire_manual,
            identity=identity_manual,
            intent_id="intent-manual-book",
            side="SELL",
            quantity="3",
        )
        _observe_partial(
            service,
            scope=strategy_a,
            intent_id="intent-strategy-a",
            quantity="2",
            price="10",
            fee="0.2",
        )
        _count, _first_sequence, baseline_cursor, _high_water = store.execution_outbox_bounds()
        assert baseline_cursor is not None
        baseline_as_of = time.time_ns()
        baseline = ExecutionAccountSnapshot(
            account_key=strategy_a.account_key,
            source="synthetic-provider",
            source_ref="positions-baseline-1",
            generation="provider-session-1",
            journal_generation=journal_generation,
            as_of_ns=baseline_as_of,
            execution_cursor=baseline_cursor,
            position_mode="NET",
            positions=(PositionFact(_INSTRUMENT, Decimal("5")),),
            cumulative_fees=(CurrencyAmount("USD", Decimal("1.2")),),
            fee_coverage_start_ns=baseline_as_of - 1_000_000,
        )
        allocation_baseline = ExecutionAllocationBaseline(
            account_key=strategy_a.account_key,
            source="trusted-virtual-position-ledger",
            source_ref="allocation-baseline-1",
            revision="allocation-revision-1",
            scope_directory_revision=directory.revision,
            provider_generation=baseline.generation,
            journal_generation=journal_generation,
            as_of_ns=baseline.as_of_ns,
            execution_cursor=baseline.execution_cursor,
            positions=(
                OwnerPosition(
                    AllocationOwnerKind.STRATEGY, "strategy-a", _INSTRUMENT, Decimal("4")
                ),
                OwnerPosition(
                    AllocationOwnerKind.STRATEGY, "strategy-b", _INSTRUMENT, Decimal("1")
                ),
            ),
        )
        _observe_partial(
            service,
            scope=strategy_a,
            intent_id="intent-strategy-a",
            quantity="4",
            price="11",
            fee="0.4",
        )
        _observe_partial(
            service,
            scope=strategy_b,
            intent_id="intent-strategy-a",
            quantity="2",
            price="12",
            fee="0.2",
        )
        _observe_partial(
            service,
            scope=manual_scope,
            intent_id="intent-manual-book",
            quantity="1",
            price="8",
            fee="0.1",
        )
        prior_bounds = store.execution_outbox_bounds()
        _observe_partial(
            service,
            scope=strategy_a,
            intent_id="intent-strategy-a",
            quantity="4",
            price="11",
            fee="0.4",
        )
        assert store.execution_outbox_bounds() == prior_bounds
        assert provider.submitted == [
            "intent-strategy-a",
            "intent-strategy-a",
            "intent-manual-book",
        ]

        current_as_of = max(time.time_ns(), baseline_as_of + 1)
        row_count, _first, outbox_head, _high_water = store.execution_outbox_bounds()
        assert row_count > 0 and outbox_head is not None
        external = ExternalActivitySnapshot(
            account_key=strategy_a.account_key,
            source="synthetic-provider-trade-feed",
            source_ref="complete-activity-window-1",
            generation=baseline.generation,
            coverage_start_ns=baseline.as_of_ns,
            coverage_end_ns=current_as_of,
            fills=(
                ExternalFillFact(
                    "provider-trade-manual-1",
                    _INSTRUMENT,
                    Side.BUY,
                    Decimal("2"),
                    Decimal("11"),
                    occurred_at_ns=baseline_as_of + 1,
                    fee=Decimal("0.15"),
                    fee_currency="USD",
                    provider_order_ref="provider-manual-order-17",
                ),
                ExternalFillFact(
                    "provider-trade-unattributed-1",
                    _INSTRUMENT,
                    Side.SELL,
                    Decimal("1"),
                    Decimal("9"),
                    occurred_at_ns=baseline_as_of + 2,
                    fee=Decimal("0.05"),
                    fee_currency="USD",
                    provider_order_ref="provider-unattributed-order-18",
                ),
            ),
            orders=(
                ExternalOrderFact(
                    "provider-manual-order-17",
                    _INSTRUMENT,
                    Side.BUY,
                    Decimal("2"),
                    Decimal("2"),
                    "FILLED",
                    observed_at_ns=baseline_as_of + 1,
                    owner_kind=AllocationOwnerKind.MANUAL_BOOK,
                    owner_ref="manual-book-a",
                ),
                ExternalOrderFact(
                    "provider-unattributed-order-18",
                    _INSTRUMENT,
                    Side.SELL,
                    Decimal("1"),
                    Decimal("1"),
                    "FILLED",
                    observed_at_ns=baseline_as_of + 2,
                ),
            ),
        )
        current = ExecutionAccountSnapshot(
            account_key=strategy_a.account_key,
            source=baseline.source,
            source_ref="positions-current-1",
            generation=baseline.generation,
            journal_generation=journal_generation,
            as_of_ns=current_as_of,
            execution_cursor=outbox_head,
            position_mode="NET",
            positions=(PositionFact(_INSTRUMENT, Decimal("11")),),
            cumulative_fees=(CurrencyAmount("USD", Decimal("2.2")),),
            fee_coverage_start_ns=baseline.fee_coverage_start_ns,
        )

        result = ExecutionAccountBookService(store, page_size=2).reconcile(
            directory,
            baseline,
            current,
            allocation_baseline=allocation_baseline,
            external_activity=external,
        )

        assert result.coverage.complete is True
        assert result.coverage.outbox_scan_complete is True
        assert result.coverage.outbox_history_contiguous is True
        assert result.state == "RESIDUAL"
        assert _quantity(_book_for(result, AllocationOwnerKind.STRATEGY, "strategy-a")) == Decimal(
            "6"
        )
        assert _quantity(_book_for(result, AllocationOwnerKind.STRATEGY, "strategy-b")) == Decimal(
            "3"
        )
        assert _quantity(
            _book_for(result, AllocationOwnerKind.MANUAL_BOOK, "manual-book-a")
        ) == Decimal("1")
        assert _quantity(
            _book_for(result, AllocationOwnerKind.UNALLOCATED, "UNALLOCATED")
        ) == Decimal("1")
        assert result.position_residual == (PositionFact(_INSTRUMENT, Decimal("2")),)
        assert result.fee_residual == (CurrencyAmount("USD", Decimal("0.3")),)
        assert len(result.managed_fills) == 3
        assert len(result.managed_orders) == 3
        assert sum(item.requested_quantity for item in result.managed_orders) == Decimal("18")
        assert sum(item.filled_quantity for item in result.managed_orders) == Decimal("7")
        assert sum(item.fee_delta or Decimal("0") for item in result.managed_fills) == Decimal(
            "0.5"
        )
        strategy_a_fill = next(
            item
            for item in result.managed_fills
            if item.intent_id == "intent-strategy-a" and item.owner_ref == "strategy-a"
        )
        assert strategy_a_fill.quantity == Decimal("2")
        assert strategy_a_fill.interval_vwap == Decimal("12")
        assert strategy_a_fill.notional_delta == Decimal("24")
        assert strategy_a_fill.fee_delta == Decimal("0.2")
        assert len(result.external_orders) == 2
        assert len(result.external_fills) == 2
        assert provider.cancelled == []
    finally:
        service._facade_pair(strategy_a)[0].close()
        service._facade_pair(strategy_b)[0].close()
        service._facade_pair(manual_scope)[0].close()
        store.close()


@pytest.mark.unit
def test_exe03_incomplete_pages_and_missing_baseline_are_visible_as_partial(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "exe03-partial.sqlite3")
    provider = _SyntheticProvider()
    scope = ExecutionScope("PAPER", "simulation", _ACCOUNT_REF, "strategy-a", "20261015")
    wire = WireScope(_DOMAIN, account_ref=_ACCOUNT_REF, strategy_ref="strategy-a")
    identity = WireIdentity(WireRole.STRATEGY_CLIENT, _PRINCIPAL, "strategy-a")
    service = ExecutionOrderEntryService(store, {wire: scope}, provider, allow_unprotected=True)
    journal_generation = str(store.journal_source_identity()["generation"])
    baseline_as_of = time.time_ns()
    directory = ExecutionScopeDirectory(
        account_key=scope.account_key,
        source="owner-registry",
        source_ref="registry-partial-1",
        revision="revision-partial-1",
        covers_from_ns=baseline_as_of,
        as_of_ns=baseline_as_of + 10_000_000_000,
        scopes=(ExecutionOwnerScope(scope, AllocationOwnerKind.STRATEGY, "strategy-a", wire),),
        completeness="PARTIAL",
        page_exhausted=False,
        continuation_token="owner-page-next",
    )
    baseline = ExecutionAccountSnapshot(
        account_key=scope.account_key,
        source="synthetic-provider",
        source_ref="positions-base",
        generation="session-1",
        journal_generation=journal_generation,
        as_of_ns=baseline_as_of,
        execution_cursor=0,
        position_mode="NET",
        positions=(PositionFact(_INSTRUMENT, Decimal("2")),),
        cumulative_fees=(CurrencyAmount("USD", Decimal("0")),),
        fee_coverage_start_ns=baseline_as_of - 1,
    )
    try:
        _submit(
            service,
            scope=wire,
            identity=identity,
            intent_id="intent-partial-page",
            side="BUY",
            quantity="2",
        )
        current_time = max(time.time_ns(), baseline_as_of + 1)
        _observe_partial(
            service,
            scope=scope,
            intent_id="intent-partial-page",
            quantity="1",
            price="10",
            fee="0.1",
        )
        row_count, _first, outbox_head, _high_water = store.execution_outbox_bounds()
        assert row_count > 1 and outbox_head is not None
        current = ExecutionAccountSnapshot(
            account_key=scope.account_key,
            source=baseline.source,
            source_ref="positions-current",
            generation=baseline.generation,
            journal_generation=journal_generation,
            as_of_ns=current_time,
            execution_cursor=outbox_head,
            position_mode="NET",
            positions=(PositionFact(_INSTRUMENT, Decimal("3")),),
            cumulative_fees=(CurrencyAmount("USD", Decimal("0.1")),),
            fee_coverage_start_ns=baseline.fee_coverage_start_ns,
            completeness="PARTIAL",
            page_exhausted=False,
            position_universe_complete=False,
            continuation_token="positions-page-next",
        )
        incomplete_external = ExternalActivitySnapshot(
            account_key=scope.account_key,
            source="synthetic-provider-trade-feed",
            source_ref="truncated-activity-window",
            generation=baseline.generation,
            coverage_start_ns=baseline.as_of_ns,
            coverage_end_ns=current_time,
            fills=(),
            completeness="PARTIAL",
            page_exhausted=False,
            continuation_token="activity-page-next",
        )

        result = ExecutionAccountBookService(store, page_size=1, max_outbox_events=1).reconcile(
            directory,
            baseline,
            current,
            allocation_baseline=None,
            external_activity=incomplete_external,
        )

        assert result.coverage.complete is False
        assert result.state == "PARTIAL"
        assert "scope_directory_incomplete" in result.coverage.reasons
        assert "allocation_baseline_missing" in result.coverage.reasons
        assert "external_activity_incomplete" in result.coverage.reasons
        assert result.coverage.outbox_scan_complete is False
        assert len(result.coverage.continuations) == 1
        # The whole current account position stays residual because the
        # outbox scan stopped before it could prove the strategy's fill.
        assert _quantity(
            _book_for(result, AllocationOwnerKind.UNALLOCATED, "UNALLOCATED")
        ) == Decimal("3")
        assert {item.source_kind for item in result.coverage.input_continuations} == {
            "scope_directory",
            "current_account_snapshot",
            "external_activity",
        }
    finally:
        service._facade_pair(scope)[0].close()
        store.close()


@pytest.mark.unit
def test_exe03_rejects_account_mismatch_duplicate_external_ids_and_fee_currency_without_amount(
    tmp_path,
) -> None:
    with pytest.raises(ContractValidationError):
        ExternalFillFact(
            "bad-fee",
            _INSTRUMENT,
            Side.BUY,
            Decimal("1"),
            Decimal("10"),
            occurred_at_ns=10,
            fee=None,
            fee_currency="USD",
        )
    with pytest.raises(ContractValidationError):
        ProviderObservation(
            "intent-fee-currency",
            ExecutionState.ACKED,
            cumulative_commission_currency="USD",
        )
    fill = ExternalFillFact(
        "duplicate-id",
        _INSTRUMENT,
        Side.BUY,
        Decimal("1"),
        Decimal("10"),
        occurred_at_ns=10,
    )
    with pytest.raises(ContractValidationError):
        ExternalActivitySnapshot(
            account_key="account-a",
            source="provider",
            source_ref="source-1",
            generation="generation-1",
            coverage_start_ns=1,
            coverage_end_ns=20,
            fills=(fill, fill),
        )
    provider_order = ExternalOrderFact(
        "provider-order-a",
        _INSTRUMENT,
        Side.BUY,
        Decimal("1"),
        Decimal("1"),
        "FILLED",
        observed_at_ns=15,
    )
    wrong_instrument_fill = ExternalFillFact(
        "wrong-instrument-fill",
        "PAPER:SIM:ETH-USDT",
        Side.BUY,
        Decimal("1"),
        Decimal("10"),
        occurred_at_ns=12,
        provider_order_ref=provider_order.provider_order_ref,
    )
    with pytest.raises(ContractValidationError, match="does not match its provider order"):
        ExternalActivitySnapshot(
            account_key="account-a",
            source="provider",
            source_ref="source-1",
            generation="generation-1",
            coverage_start_ns=10,
            coverage_end_ns=20,
            fills=(wrong_instrument_fill,),
            orders=(provider_order,),
        )
    wrong_side_fill = ExternalFillFact(
        "wrong-side-fill",
        _INSTRUMENT,
        Side.SELL,
        Decimal("1"),
        Decimal("10"),
        occurred_at_ns=12,
        provider_order_ref=provider_order.provider_order_ref,
    )
    with pytest.raises(ContractValidationError, match="does not match its provider order"):
        ExternalActivitySnapshot(
            account_key="account-a",
            source="provider",
            source_ref="source-1",
            generation="generation-1",
            coverage_start_ns=10,
            coverage_end_ns=20,
            fills=(wrong_side_fill,),
            orders=(provider_order,),
        )
    excessive_fills = tuple(
        ExternalFillFact(
            f"excessive-fill-{index}",
            _INSTRUMENT,
            Side.BUY,
            Decimal("0.75"),
            Decimal("10"),
            occurred_at_ns=12 + index,
            provider_order_ref=provider_order.provider_order_ref,
        )
        for index in (1, 2)
    )
    with pytest.raises(ContractValidationError, match="exceed their provider order quantity"):
        ExternalActivitySnapshot(
            account_key="account-a",
            source="provider",
            source_ref="source-1",
            generation="generation-1",
            coverage_start_ns=10,
            coverage_end_ns=20,
            fills=excessive_fills,
            orders=(provider_order,),
        )

    store = SqliteExecutionStore(tmp_path / "exe03-mismatch.sqlite3")
    scope = ExecutionScope("PAPER", "simulation", _ACCOUNT_REF, "strategy-a", "20261015")
    journal_generation = str(store.journal_source_identity()["generation"])
    snapshot = ExecutionAccountSnapshot(
        account_key=scope.account_key,
        source="synthetic-provider",
        source_ref="snapshot-1",
        generation="session-1",
        journal_generation=journal_generation,
        as_of_ns=10,
        execution_cursor=0,
        position_mode="NET",
        positions=(),
        cumulative_fees=(),
        fee_coverage_start_ns=1,
    )
    directory = ExecutionScopeDirectory(
        account_key=scope.account_key,
        source="owner-registry",
        source_ref="registry-1",
        revision="revision-1",
        covers_from_ns=10,
        as_of_ns=20,
        scopes=(
            ExecutionOwnerScope(
                scope,
                AllocationOwnerKind.STRATEGY,
                "strategy-a",
                WireScope(_DOMAIN, account_ref=_ACCOUNT_REF, strategy_ref="strategy-a"),
            ),
        ),
    )
    wrong_account = ExecutionAccountSnapshot(
        account_key="different-account",
        source=snapshot.source,
        source_ref="snapshot-wrong-account",
        generation=snapshot.generation,
        journal_generation=journal_generation,
        as_of_ns=20,
        execution_cursor=0,
        position_mode="NET",
        positions=(),
        cumulative_fees=(),
        fee_coverage_start_ns=1,
    )
    try:
        with pytest.raises(ContractValidationError, match="account snapshots"):
            ExecutionAccountBookService(store).reconcile(
                directory,
                snapshot,
                wrong_account,
                allocation_baseline=None,
                external_activity=None,
            )
    finally:
        store.close()


@pytest.mark.unit
def test_exe03_fee_currency_conflict_latches_review_and_downgrades_coverage(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "exe03-fee-currency.sqlite3")
    provider = _SyntheticProvider()
    scope = ExecutionScope("PAPER", "simulation", _ACCOUNT_REF, "strategy-fee", "20261015")
    wire = WireScope(_DOMAIN, account_ref=_ACCOUNT_REF, strategy_ref="strategy-fee")
    identity = WireIdentity(WireRole.STRATEGY_CLIENT, _PRINCIPAL, "strategy-fee")
    service = ExecutionOrderEntryService(store, {wire: scope}, provider, allow_unprotected=True)
    journal_generation = str(store.journal_source_identity()["generation"])
    baseline_as_of = time.time_ns()
    directory = ExecutionScopeDirectory(
        account_key=scope.account_key,
        source="owner-registry",
        source_ref="registry-fee-1",
        revision="revision-fee-1",
        covers_from_ns=baseline_as_of,
        as_of_ns=baseline_as_of + 10_000_000_000,
        scopes=(
            ExecutionOwnerScope(
                scope,
                AllocationOwnerKind.STRATEGY,
                "strategy-fee",
                wire,
            ),
        ),
    )
    baseline = ExecutionAccountSnapshot(
        account_key=scope.account_key,
        source="synthetic-provider",
        source_ref="fee-position-baseline",
        generation="fee-session-1",
        journal_generation=journal_generation,
        as_of_ns=baseline_as_of,
        execution_cursor=0,
        position_mode="NET",
        positions=(),
        cumulative_fees=(CurrencyAmount("USD", Decimal("0")),),
        fee_coverage_start_ns=baseline_as_of - 1,
    )
    try:
        _submit(
            service,
            scope=wire,
            identity=identity,
            intent_id="intent-fee-currency",
            side="BUY",
            quantity="2",
        )
        facade = service._facade_pair(scope)[0]
        first = ProviderObservation(
            "intent-fee-currency",
            ExecutionState.PARTIALLY_FILLED,
            provider_order_id="provider-intent-fee-currency",
            filled_quantity=Decimal("1"),
            average_price=Decimal("10"),
            cumulative_commission=Decimal("0.1"),
            cumulative_commission_currency="USD",
        )
        assert facade.reconcile(first).review_required is False
        conflicting = ProviderObservation(
            "intent-fee-currency",
            ExecutionState.PARTIALLY_FILLED,
            provider_order_id="provider-intent-fee-currency",
            filled_quantity=Decimal("1"),
            average_price=Decimal("10"),
            cumulative_commission=Decimal("0.1"),
            cumulative_commission_currency="EUR",
        )
        assert facade.reconcile(conflicting).review_required is True
        assert provider.submitted == ["intent-fee-currency"]

        current_as_of = max(time.time_ns(), baseline_as_of + 1)
        _count, _first_sequence, outbox_head, _high_water = store.execution_outbox_bounds()
        assert outbox_head is not None
        current = ExecutionAccountSnapshot(
            account_key=scope.account_key,
            source=baseline.source,
            source_ref="fee-position-current",
            generation=baseline.generation,
            journal_generation=journal_generation,
            as_of_ns=current_as_of,
            execution_cursor=outbox_head,
            position_mode="NET",
            positions=(PositionFact(_INSTRUMENT, Decimal("1")),),
            cumulative_fees=(CurrencyAmount("USD", Decimal("0.1")),),
            fee_coverage_start_ns=baseline.fee_coverage_start_ns,
        )
        allocation_baseline = ExecutionAllocationBaseline(
            account_key=scope.account_key,
            source="virtual-position-ledger",
            source_ref="fee-allocation-base",
            revision="allocation-fee-1",
            scope_directory_revision=directory.revision,
            provider_generation=baseline.generation,
            journal_generation=journal_generation,
            as_of_ns=baseline.as_of_ns,
            execution_cursor=baseline.execution_cursor,
            positions=(),
        )
        external = ExternalActivitySnapshot(
            account_key=scope.account_key,
            source="synthetic-provider-trade-feed",
            source_ref="fee-external-1",
            generation=baseline.generation,
            coverage_start_ns=baseline.as_of_ns,
            coverage_end_ns=current_as_of,
            fills=(),
            orders=(
                ExternalOrderFact(
                    "provider-order-unknown",
                    _INSTRUMENT,
                    Side.BUY,
                    Decimal("2"),
                    Decimal("1"),
                    "UNKNOWN",
                    observed_at_ns=baseline.as_of_ns + 1,
                ),
            ),
        )
        result = ExecutionAccountBookService(store).reconcile(
            directory,
            baseline,
            current,
            allocation_baseline=allocation_baseline,
            external_activity=external,
        )
        assert result.coverage.complete is False
        assert "execution_intent_requires_review" in result.coverage.reasons
        assert "external_order_requires_reconciliation" in result.coverage.reasons
    finally:
        service._facade_pair(scope)[0].close()
        store.close()


@pytest.mark.unit
def test_exe03_quarantines_legacy_outbox_lineage_to_unallocated_residual(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "exe03-lineage.sqlite3")
    provider = _SyntheticProvider()
    scope = ExecutionScope("PAPER", "simulation", _ACCOUNT_REF, "strategy-lineage", "20261015")
    other_scope = ExecutionScope(
        "PAPER", "simulation", _ACCOUNT_REF, "strategy-lineage-b", "20261015"
    )
    wire = WireScope(_DOMAIN, account_ref=_ACCOUNT_REF, strategy_ref="strategy-lineage")
    other_wire = WireScope(_DOMAIN, account_ref=_ACCOUNT_REF, strategy_ref="strategy-lineage-b")
    identity = WireIdentity(WireRole.STRATEGY_CLIENT, _PRINCIPAL, "strategy-lineage")
    other_identity = WireIdentity(WireRole.STRATEGY_CLIENT, _PRINCIPAL, "strategy-lineage-b")
    service = ExecutionOrderEntryService(
        store,
        {wire: scope, other_wire: other_scope},
        provider,
        allow_unprotected=True,
    )
    journal_generation = str(store.journal_source_identity()["generation"])
    baseline_as_of = time.time_ns()
    directory = ExecutionScopeDirectory(
        account_key=scope.account_key,
        source="trusted-owner-registry",
        source_ref="registry-lineage-1",
        revision="registry-lineage-revision-1",
        covers_from_ns=baseline_as_of,
        as_of_ns=baseline_as_of + 10_000_000_000,
        scopes=(
            ExecutionOwnerScope(scope, AllocationOwnerKind.STRATEGY, "strategy-lineage", wire),
            ExecutionOwnerScope(
                other_scope,
                AllocationOwnerKind.STRATEGY,
                "strategy-lineage-b",
                other_wire,
            ),
        ),
    )
    baseline = ExecutionAccountSnapshot(
        account_key=scope.account_key,
        source="synthetic-provider",
        source_ref="lineage-position-baseline",
        generation="lineage-session-1",
        journal_generation=journal_generation,
        as_of_ns=baseline_as_of,
        execution_cursor=0,
        position_mode="NET",
        positions=(),
        cumulative_fees=(CurrencyAmount("USD", Decimal("0")),),
        fee_coverage_start_ns=baseline_as_of,
    )
    try:
        _submit(
            service,
            scope=wire,
            identity=identity,
            intent_id="intent-legacy-lineage",
            side="BUY",
            quantity="2",
        )
        _submit(
            service,
            scope=other_wire,
            identity=other_identity,
            intent_id="intent-legacy-lineage",
            side="BUY",
            quantity="3",
        )
        _observe_partial(
            service,
            scope=scope,
            intent_id="intent-legacy-lineage",
            quantity="1",
            price="10",
            fee="0.1",
        )
        _observe_partial(
            service,
            scope=other_scope,
            intent_id="intent-legacy-lineage",
            quantity="2",
            price="11",
            fee="0.2",
        )
        with store._transaction() as cursor:
            cursor.execute(
                "UPDATE execution_outbox SET journal_incarnation_id = NULL "
                "WHERE scope_key = ? AND intent_id = ? AND event_type = 'provider_observation'",
                (scope.key, "intent-legacy-lineage"),
            )
        current_as_of = max(time.time_ns(), baseline_as_of + 1)
        _count, _first_sequence, outbox_head, _high_water = store.execution_outbox_bounds()
        assert outbox_head is not None
        current = ExecutionAccountSnapshot(
            account_key=scope.account_key,
            source=baseline.source,
            source_ref="lineage-position-current",
            generation=baseline.generation,
            journal_generation=journal_generation,
            as_of_ns=current_as_of,
            execution_cursor=outbox_head,
            position_mode="NET",
            positions=(PositionFact(_INSTRUMENT, Decimal("3")),),
            cumulative_fees=(CurrencyAmount("USD", Decimal("0.3")),),
            fee_coverage_start_ns=baseline.fee_coverage_start_ns,
        )
        allocation_baseline = ExecutionAllocationBaseline(
            account_key=scope.account_key,
            source="virtual-position-ledger",
            source_ref="lineage-allocation-baseline",
            revision="lineage-allocation-revision",
            scope_directory_revision=directory.revision,
            provider_generation=baseline.generation,
            journal_generation=journal_generation,
            as_of_ns=baseline.as_of_ns,
            execution_cursor=baseline.execution_cursor,
            positions=(),
        )
        external = ExternalActivitySnapshot(
            account_key=scope.account_key,
            source="synthetic-provider-trade-feed",
            source_ref="lineage-external-activity",
            generation=baseline.generation,
            coverage_start_ns=baseline.as_of_ns,
            coverage_end_ns=current_as_of,
            fills=(),
        )

        result = ExecutionAccountBookService(store).reconcile(
            directory,
            baseline,
            current,
            allocation_baseline=allocation_baseline,
            external_activity=external,
        )

        assert result.coverage.complete is False
        assert "execution_outbox_lineage_mismatch" in result.coverage.reasons
        assert result.coverage.quarantined_intents == (
            QuarantinedExecutionIntent(scope.key, "intent-legacy-lineage"),
        )
        assert len(result.managed_fills) == 1
        assert result.managed_fills[0].owner_ref == "strategy-lineage-b"
        assert result.managed_fills[0].quantity == Decimal("2")
        assert _quantity(
            _book_for(result, AllocationOwnerKind.STRATEGY, "strategy-lineage")
        ) == Decimal("0")
        assert _quantity(
            _book_for(result, AllocationOwnerKind.STRATEGY, "strategy-lineage-b")
        ) == Decimal("2")
        assert _quantity(
            _book_for(result, AllocationOwnerKind.UNALLOCATED, "UNALLOCATED")
        ) == Decimal("1")
    finally:
        service._facade_pair(scope)[0].close()
        service._facade_pair(other_scope)[0].close()
        store.close()
