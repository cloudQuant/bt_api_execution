"""Source-owned BT01/BT02 financial ledger component tests.

These tests exercise deterministic accounting contracts only.  They do not
claim provider, account, ServiceHost, or durable-store acceptance.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from bt_api_execution import ExecutionEvent, ExecutionScope, ExecutionState, OrderIntent, Side
from bt_api_execution.account_financial_ledger import (
    AccountFinancialBaselineV1,
    CurrencyAmountV1,
    FinancialActivityBatchV1,
    FinancialActivityEventV1,
    FinancialDataUnavailableError,
    FinancialEventKind,
    FinancialExecutionBatchV1,
    FinancialExecutionCursorSnapshotV1,
    FinancialExecutionCursorV1,
    FinancialFxRateV1,
    FinancialInstrumentSpecV1,
    FinancialLedgerCutV1,
    FinancialLedgerError,
    FinancialLedgerPolicyV1,
    FinancialMarkV1,
    FinancialOrderBaselineCursorV1,
    FinancialOwnerBaselineV1,
    FinancialOwnerDirectoryV1,
    FinancialOwnerKind,
    FinancialOwnerV1,
    FinancialProductModel,
    FinancialSourceKind,
    FinancialValuationPolicyV1,
    FinancialValuationSnapshotV1,
    FinancialViewCutV1,
    FinancialWatermarkV1,
    OwnerPositionCostV1,
    ProviderAccountFinancialSnapshotV1,
    advance_execution_cursor_v1,
    assert_same_financial_cut,
    derive_execution_posting_v1,
    execution_cursor_from_observation_v1,
    owner_cash_v1,
    owner_nav_v1,
    project_account_financials_v1,
)

_INSTRUMENT = "SPOT:BTC-USD"
_ACCOUNT_SCOPE_A = ExecutionScope("synthetic", "test", "account-1", "strategy-a")
_ACCOUNT_SCOPE_B = ExecutionScope("synthetic", "test", "account-1", "strategy-b")
_STRATEGY_A = FinancialOwnerV1(FinancialOwnerKind.STRATEGY, "strategy-a", _ACCOUNT_SCOPE_A.key)
_STRATEGY_B = FinancialOwnerV1(FinancialOwnerKind.STRATEGY, "strategy-b", _ACCOUNT_SCOPE_B.key)
_MANUAL = FinancialOwnerV1(FinancialOwnerKind.MANUAL_BOOK, "manual-book-1", None)
_OWNERS = (_STRATEGY_A, _STRATEGY_B, _MANUAL)
_DIRECTORY = FinancialOwnerDirectoryV1(
    _ACCOUNT_SCOPE_A.account_key, "owners-v1", "trusted-owner-registry", _OWNERS
)
_SPEC = FinancialInstrumentSpecV1(
    _INSTRUMENT,
    "instrument-v1",
    FinancialProductModel.SPOT,
    "BTC",
    "USD",
)
_SPECS = {_INSTRUMENT: _SPEC}
_SPEC_ETH = FinancialInstrumentSpecV1(
    "SPOT:ETH-USD",
    "instrument-v1",
    FinancialProductModel.SPOT,
    "ETH",
    "USD",
)
_POLICY = FinancialLedgerPolicyV1(
    cash_currency="USD",
    reporting_currency="USD",
    max_account_fact_age_ns=10**12,
    max_activity_age_ns=10**12,
    max_cut_skew_ns=10**12,
    max_mark_age_ns=10**12,
    max_fx_age_ns=10**12,
)


def _cut(
    *,
    captured: int,
    execution_cursor: int = 0,
    facts_cursor: int = 0,
    activity_cursor: int = 0,
    facts_generation: str = "provider-gen-1",
    activity_generation: str = "activity-gen-1",
    directory: FinancialOwnerDirectoryV1 = _DIRECTORY,
) -> FinancialLedgerCutV1:
    as_of = captured - 1
    return FinancialLedgerCutV1(
        account_key=directory.account_key,
        owner_directory_revision=directory.revision,
        captured_at_ns=captured,
        watermarks=(
            FinancialWatermarkV1(
                FinancialSourceKind.EXECUTION_OUTBOX,
                "execution-outbox",
                "1" * 32,
                execution_cursor,
                as_of,
            ),
            FinancialWatermarkV1(
                FinancialSourceKind.ACCOUNT_FACTS,
                "provider-account-facts",
                facts_generation,
                facts_cursor,
                as_of,
            ),
            FinancialWatermarkV1(
                FinancialSourceKind.ACCOUNT_ACTIVITY,
                "provider-account-activity",
                activity_generation,
                activity_cursor,
                as_of,
            ),
        ),
    )


def _provider_snapshot(
    cut: FinancialLedgerCutV1,
    *,
    cash: tuple[CurrencyAmountV1, ...] = (CurrencyAmountV1("USD", Decimal("1000")),),
    positions: tuple[tuple[str, Decimal], ...] = (),
    fees: tuple[CurrencyAmountV1, ...] = (CurrencyAmountV1("USD", Decimal("0")),),
) -> ProviderAccountFinancialSnapshotV1:
    watermark = cut.watermark(FinancialSourceKind.ACCOUNT_FACTS)
    return ProviderAccountFinancialSnapshotV1(
        account_key=cut.account_key,
        source_ref=watermark.source_ref,
        generation=watermark.generation,
        cursor=watermark.cursor,
        as_of_ns=watermark.as_of_ns,
        cash=cash,
        positions=positions,
        cumulative_fees=fees,
    )


def _baseline(
    *,
    owner_cash: dict[tuple[str, str], Decimal] | None = None,
    owner_positions: dict[tuple[str, str], tuple[OwnerPositionCostV1, ...]] | None = None,
    account_cash: Decimal = Decimal("1000"),
    account_positions: tuple[tuple[str, Decimal], ...] = (),
) -> AccountFinancialBaselineV1:
    cut = _cut(captured=1000)
    provider = _provider_snapshot(
        cut,
        cash=(CurrencyAmountV1("USD", account_cash),),
        positions=account_positions,
    )
    cash_by_owner = owner_cash or {
        _STRATEGY_A.key: Decimal("500"),
        _STRATEGY_B.key: Decimal("500"),
        _MANUAL.key: Decimal("0"),
    }
    positions_by_owner = owner_positions or {}
    rows = tuple(
        FinancialOwnerBaselineV1(
            owner,
            (CurrencyAmountV1("USD", cash_by_owner[owner.key]),),
            positions_by_owner.get(owner.key, ()),
            (CurrencyAmountV1("USD", Decimal("0")),),
        )
        for owner in _OWNERS
    )
    return AccountFinancialBaselineV1(
        account_key=_DIRECTORY.account_key,
        baseline_ref="baseline-v1",
        version=1,
        authority_ref="trusted-owner-baseline-issuer",
        source_ref="trusted-financial-baseline",
        owner_directory_revision=_DIRECTORY.revision,
        cut=cut,
        provider_snapshot=provider,
        owners=rows,
    )


def _empty_activity(cut: FinancialLedgerCutV1, *, from_cursor: int = 0) -> FinancialActivityBatchV1:
    watermark = cut.watermark(FinancialSourceKind.ACCOUNT_ACTIVITY)
    return FinancialActivityBatchV1(
        source_ref=watermark.source_ref,
        generation=watermark.generation,
        from_cursor=from_cursor,
        through_cursor=watermark.cursor,
        as_of_ns=watermark.as_of_ns,
        events=(),
    )


def _execution_batch(
    cut: FinancialLedgerCutV1,
    postings=(),
    *,
    from_cursor: int = 0,
) -> FinancialExecutionBatchV1:
    watermark = cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX)
    return FinancialExecutionBatchV1(
        journal_generation=watermark.generation,
        from_cursor=from_cursor,
        through_cursor=watermark.cursor,
        scanned_row_count=watermark.cursor - from_cursor,
        postings=tuple(postings),
    )


def _intent(
    *, intent_id: str, scope: ExecutionScope = _ACCOUNT_SCOPE_A, quantity: str = "2"
) -> OrderIntent:
    return OrderIntent.limit(
        intent_id=intent_id,
        scope=scope,
        signal_id=f"signal-{intent_id}",
        instrument=_INSTRUMENT,
        side=Side.BUY,
        quantity=Decimal(quantity),
        price=Decimal("10"),
        metadata_version=_SPEC.metadata_version,
    )


def _observation(
    *,
    intent: OrderIntent,
    event_id: str,
    sequence: int,
    filled: str,
    average: str,
    commission: str,
    provider_order_id: str = "venue-order-1",
    state: ExecutionState = ExecutionState.PARTIALLY_FILLED,
    commission_currency: str = "USD",
) -> ExecutionEvent:
    payload = {
        "provider_order_id": provider_order_id,
        "filled_quantity": filled,
        "average_price": average,
        "cumulative_commission": commission,
        "cumulative_commission_currency": commission_currency,
        "reason_code": None,
        "source": "provider",
    }
    return ExecutionEvent(
        sequence=sequence,
        event_id=event_id,
        intent_id=intent.intent_id,
        scope_key=intent.scope.key,
        event_type="provider_observation",
        state=state,
        payload=payload,
        created_at_ns=2000 + sequence,
        journal_incarnation_id="1" * 32,
    )


def test_managed_partial_fills_split_strategy_books_and_post_only_incremental_fee() -> None:
    intent = _intent(intent_id="intent-a")
    first = _observation(
        intent=intent,
        event_id="a" * 32,
        sequence=1,
        filled="1",
        average="10",
        commission="0.1",
    )
    first_posting = derive_execution_posting_v1(first, intent, None, _STRATEGY_A, _SPECS)
    second = _observation(
        intent=intent,
        event_id="b" * 32,
        sequence=2,
        filled="2",
        average="11",
        commission="0.3",
    )
    second_posting = derive_execution_posting_v1(
        second,
        intent,
        FinancialExecutionCursorV1(
            Decimal("1"), Decimal("10"), Decimal("0.1"), "USD", "venue-order-1"
        ),
        _STRATEGY_A,
        _SPECS,
    )
    intent_b = _intent(intent_id="intent-b", scope=_ACCOUNT_SCOPE_B, quantity="1")
    strategy_b_posting = derive_execution_posting_v1(
        _observation(
            intent=intent_b,
            event_id="c" * 32,
            sequence=3,
            filled="1",
            average="20",
            commission="0.1",
            provider_order_id="venue-order-b",
        ),
        intent_b,
        None,
        _STRATEGY_B,
        _SPECS,
    )

    assert first_posting.signed_quantity == Decimal("1")
    assert first_posting.principal_cash_delta == Decimal("-10")
    assert first_posting.fee_delta == Decimal("0.1")
    assert second_posting.signed_quantity == Decimal("1")
    assert second_posting.price == Decimal("12")
    assert second_posting.principal_cash_delta == Decimal("-12")
    assert second_posting.fee_delta == Decimal("0.2")
    assert second_posting.source_payload_sha256

    current_cut = _cut(captured=3000, execution_cursor=3, facts_cursor=1)
    current = _provider_snapshot(
        current_cut,
        cash=(CurrencyAmountV1("USD", Decimal("957.6")),),
        positions=((_INSTRUMENT, Decimal("3")),),
        fees=(CurrencyAmountV1("USD", Decimal("0.4")),),
    )
    projected = project_account_financials_v1(
        baseline=_baseline(),
        directory=_DIRECTORY,
        current_account=current,
        current_cut=current_cut,
        execution_batch=_execution_batch(
            current_cut, (first_posting, second_posting, strategy_b_posting)
        ),
        activity_batch=_empty_activity(current_cut),
        instrument_specs=_SPECS,
        policy=_POLICY,
        now_ns=3001,
    )

    strategy_a = projected.owner_book(_STRATEGY_A.key)
    strategy_b = projected.owner_book(_STRATEGY_B.key)
    assert owner_cash_v1(strategy_a, "USD") == Decimal("477.7")
    assert strategy_a.positions[0].quantity == Decimal("2")
    assert strategy_a.positions[0].cost_basis == Decimal("22")
    assert strategy_a.cumulative_fees == (CurrencyAmountV1("USD", Decimal("0.3")),)
    assert owner_cash_v1(strategy_b, "USD") == Decimal("479.9")
    assert strategy_b.positions[0].quantity == Decimal("1")
    assert strategy_b.positions[0].cost_basis == Decimal("20")
    assert projected.state == "BALANCED"
    assert projected.complete is True


def test_baseline_cumulative_order_cursor_prevents_reposting_prior_fill_and_fee() -> None:
    intent = _intent(intent_id="intent-baseline-fill")
    baseline_cut = _cut(captured=1000, execution_cursor=1)
    exec_wm = baseline_cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX)
    baseline = AccountFinancialBaselineV1(
        account_key=_DIRECTORY.account_key,
        baseline_ref="baseline-with-order-cursor",
        version=1,
        authority_ref="trusted-owner-baseline-issuer",
        source_ref="trusted-financial-baseline",
        owner_directory_revision=_DIRECTORY.revision,
        cut=baseline_cut,
        provider_snapshot=_provider_snapshot(
            baseline_cut,
            cash=(CurrencyAmountV1("USD", Decimal("989.9")),),
            positions=((_INSTRUMENT, Decimal("1")),),
            fees=(CurrencyAmountV1("USD", Decimal("0.1")),),
        ),
        owners=(
            FinancialOwnerBaselineV1(
                _STRATEGY_A,
                (CurrencyAmountV1("USD", Decimal("489.9")),),
                (
                    OwnerPositionCostV1(
                        _INSTRUMENT,
                        Decimal("1"),
                        Decimal("10"),
                        "USD",
                        metadata_version=_SPEC.metadata_version,
                    ),
                ),
                (CurrencyAmountV1("USD", Decimal("0.1")),),
            ),
            FinancialOwnerBaselineV1(
                _STRATEGY_B,
                (CurrencyAmountV1("USD", Decimal("500")),),
                (),
                (CurrencyAmountV1("USD", Decimal("0")),),
            ),
            FinancialOwnerBaselineV1(
                _MANUAL,
                (CurrencyAmountV1("USD", Decimal("0")),),
                (),
                (CurrencyAmountV1("USD", Decimal("0")),),
            ),
        ),
        execution_cursors=FinancialExecutionCursorSnapshotV1(
            journal_generation=exec_wm.generation,
            cursor=exec_wm.cursor,
            as_of_ns=exec_wm.as_of_ns,
            orders=(
                FinancialOrderBaselineCursorV1(
                    owner=_STRATEGY_A,
                    scope_key=intent.scope.key,
                    intent_id=intent.intent_id,
                    intent_payload_sha256=intent.fingerprint,
                    cursor=FinancialExecutionCursorV1(
                        filled_quantity=Decimal("1"),
                        average_price=Decimal("10"),
                        cumulative_commission=Decimal("0.10"),
                        commission_currency="USD",
                        provider_order_id="venue-order-1",
                    ),
                ),
            ),
        ),
    )

    event = _observation(
        intent=intent,
        event_id="d" * 32,
        sequence=2,
        filled="1.5",
        average="12",
        commission="0.15",
    )
    prior = baseline.execution_cursors.orders[0].cursor
    observed_cursor, observed_quantity, observed_fee = execution_cursor_from_observation_v1(
        event, intent, prior, _STRATEGY_A
    )
    assert observed_cursor.filled_quantity == Decimal("1.5")
    assert observed_quantity == Decimal("0.5")
    assert observed_fee == Decimal("0.05")
    cursor, posting = advance_execution_cursor_v1(event, intent, prior, _STRATEGY_A, _SPECS)
    assert cursor.filled_quantity == Decimal("1.5")
    assert cursor.cumulative_commission == Decimal("0.15")
    assert posting is not None
    assert posting.signed_quantity == Decimal("0.5")
    assert posting.price == Decimal("16")
    assert posting.fee_delta == Decimal("0.05")

    current_cut = _cut(captured=3000, execution_cursor=2)
    current_account = _provider_snapshot(
        current_cut,
        cash=(CurrencyAmountV1("USD", Decimal("981.85")),),
        positions=((_INSTRUMENT, Decimal("1.5")),),
        fees=(CurrencyAmountV1("USD", Decimal("0.15")),),
    )
    projection = project_account_financials_v1(
        baseline=baseline,
        directory=_DIRECTORY,
        current_account=current_account,
        current_cut=current_cut,
        execution_batch=_execution_batch(current_cut, (posting,), from_cursor=1),
        activity_batch=_empty_activity(current_cut),
        instrument_specs=_SPECS,
        policy=_POLICY,
        now_ns=3000,
    )
    book = projection.owner_book(_STRATEGY_A.key)
    assert book.cash == (CurrencyAmountV1("USD", Decimal("481.85")),)
    assert book.positions[0].quantity == Decimal("1.5")
    assert book.positions[0].cost_basis == Decimal("18")
    assert book.cumulative_fees == (CurrencyAmountV1("USD", Decimal("0.15")),)


def test_nonzero_execution_baseline_requires_exact_cursor_snapshot() -> None:
    cut = _cut(captured=1000, execution_cursor=1)
    owner_rows = tuple(
        FinancialOwnerBaselineV1(
            owner,
            (CurrencyAmountV1("USD", Decimal("0")),),
            (),
            (CurrencyAmountV1("USD", Decimal("0")),),
        )
        for owner in _OWNERS
    )
    arguments = {
        "account_key": _DIRECTORY.account_key,
        "authority_ref": "trusted-owner-baseline-issuer",
        "source_ref": "trusted-financial-baseline",
        "owner_directory_revision": _DIRECTORY.revision,
        "cut": cut,
        "provider_snapshot": _provider_snapshot(cut),
        "owners": owner_rows,
    }
    with pytest.raises(FinancialDataUnavailableError, match="cumulative execution cursor snapshot"):
        AccountFinancialBaselineV1(baseline_ref="missing-cursor-snapshot", version=1, **arguments)

    exec_wm = cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX)
    stale_snapshot = FinancialExecutionCursorSnapshotV1(
        journal_generation=exec_wm.generation,
        cursor=0,
        as_of_ns=exec_wm.as_of_ns,
        orders=(),
    )
    with pytest.raises(FinancialDataUnavailableError, match="exact source cut"):
        AccountFinancialBaselineV1(
            baseline_ref="stale-cursor-snapshot",
            version=1,
            execution_cursors=stale_snapshot,
            **arguments,
        )


def test_activity_manual_book_fill_applies_principal_fee_and_cost_basis() -> None:
    baseline = _baseline(
        owner_cash={
            _STRATEGY_A.key: Decimal("500"),
            _STRATEGY_B.key: Decimal("0"),
            _MANUAL.key: Decimal("100"),
        },
        account_cash=Decimal("600"),
    )
    current_cut = _cut(captured=3000, facts_cursor=1, activity_cursor=1)
    event = FinancialActivityEventV1(
        source_ref="provider-account-activity",
        generation="activity-gen-1",
        sequence=1,
        event_id="venue-trade-1",
        occurred_at_ns=2500,
        owner_key=_MANUAL.key,
        kind=FinancialEventKind.SPOT_FILL,
        cash_movements=(CurrencyAmountV1("USD", Decimal("-20")),),
        fee=CurrencyAmountV1("USD", Decimal("0.2")),
        instrument=_INSTRUMENT,
        signed_quantity=Decimal("2"),
        price=Decimal("10"),
        provider_order_ref="manual-provider-order-1",
    )
    activity = FinancialActivityBatchV1(
        source_ref="provider-account-activity",
        generation="activity-gen-1",
        from_cursor=0,
        through_cursor=1,
        as_of_ns=2999,
        events=(event,),
    )
    current = _provider_snapshot(
        current_cut,
        cash=(CurrencyAmountV1("USD", Decimal("579.8")),),
        positions=((_INSTRUMENT, Decimal("2")),),
        fees=(CurrencyAmountV1("USD", Decimal("0.2")),),
    )
    projected = project_account_financials_v1(
        baseline=baseline,
        directory=_DIRECTORY,
        current_account=current,
        current_cut=current_cut,
        execution_batch=_execution_batch(current_cut),
        activity_batch=activity,
        instrument_specs=_SPECS,
        policy=_POLICY,
        now_ns=3001,
    )
    manual = projected.owner_book(_MANUAL.key)
    assert owner_cash_v1(manual, "USD") == Decimal("79.8")
    assert manual.positions[0].quantity == Decimal("2")
    assert manual.positions[0].cost_basis == Decimal("20")
    assert manual.cumulative_fees == (CurrencyAmountV1("USD", Decimal("0.2")),)
    assert projected.state == "BALANCED"


def test_unallocated_residual_includes_account_only_cash_positions_and_fees() -> None:
    current_cut = _cut(captured=3000, facts_cursor=1)
    current = _provider_snapshot(
        current_cut,
        cash=(
            CurrencyAmountV1("USD", Decimal("1001")),
            CurrencyAmountV1("EUR", Decimal("0")),
        ),
        positions=(("SPOT:ETH-USD", Decimal("1")),),
        fees=(CurrencyAmountV1("USD", Decimal("0.25")),),
    )
    projected = project_account_financials_v1(
        baseline=_baseline(),
        directory=_DIRECTORY,
        current_account=current,
        current_cut=current_cut,
        execution_batch=_execution_batch(current_cut),
        activity_batch=_empty_activity(current_cut),
        instrument_specs={**_SPECS, "SPOT:ETH-USD": _SPEC_ETH},
        policy=_POLICY,
        now_ns=3001,
    )
    assert projected.state == "RESIDUAL"
    assert projected.unallocated_cash_residual == (CurrencyAmountV1("USD", Decimal("1")),)
    assert projected.unallocated_position_residual == (("SPOT:ETH-USD", Decimal("1")),)
    assert projected.unallocated_fee_residual == (CurrencyAmountV1("USD", Decimal("0.25")),)


def test_missing_provider_currency_does_not_hide_owner_only_cash_residual() -> None:
    baseline_cut = _cut(captured=1000)
    baseline_provider = _provider_snapshot(
        baseline_cut,
        cash=(CurrencyAmountV1("USD", Decimal("1000")), CurrencyAmountV1("EUR", Decimal("10"))),
    )
    owner_cash = {
        _STRATEGY_A.key: (
            CurrencyAmountV1("USD", Decimal("500")),
            CurrencyAmountV1("EUR", Decimal("10")),
        ),
        _STRATEGY_B.key: (
            CurrencyAmountV1("USD", Decimal("500")),
            CurrencyAmountV1("EUR", Decimal("0")),
        ),
        _MANUAL.key: (CurrencyAmountV1("USD", Decimal("0")), CurrencyAmountV1("EUR", Decimal("0"))),
    }
    baseline = AccountFinancialBaselineV1(
        account_key=_DIRECTORY.account_key,
        baseline_ref="baseline-multicurrency",
        version=1,
        authority_ref="trusted-owner-baseline-issuer",
        source_ref="trusted-financial-baseline",
        owner_directory_revision=_DIRECTORY.revision,
        cut=baseline_cut,
        provider_snapshot=baseline_provider,
        owners=tuple(
            FinancialOwnerBaselineV1(
                owner,
                owner_cash[owner.key],
                (),
                (CurrencyAmountV1("USD", Decimal("0")), CurrencyAmountV1("EUR", Decimal("0"))),
            )
            for owner in _OWNERS
        ),
    )
    current_cut = _cut(captured=3000, facts_cursor=1)
    current = _provider_snapshot(current_cut, cash=(CurrencyAmountV1("USD", Decimal("1000")),))
    projected = project_account_financials_v1(
        baseline=baseline,
        directory=_DIRECTORY,
        current_account=current,
        current_cut=current_cut,
        execution_batch=_execution_batch(current_cut),
        activity_batch=_empty_activity(current_cut),
        instrument_specs=_SPECS,
        policy=_POLICY,
        now_ns=3001,
    )
    assert projected.unallocated_cash_residual == (CurrencyAmountV1("EUR", Decimal("-10")),)


def test_fill_payload_must_bind_scope_state_fee_currency_and_instrument_version() -> None:
    intent = _intent(intent_id="intent-a")
    event = _observation(
        intent=intent,
        event_id="c" * 32,
        sequence=1,
        filled="1",
        average="10",
        commission="0.1",
    )
    with pytest.raises(FinancialLedgerError, match="owner do not match"):
        derive_execution_posting_v1(event, intent, None, _STRATEGY_B, _SPECS)
    with pytest.raises(FinancialDataUnavailableError, match="state does not authorize"):
        derive_execution_posting_v1(
            _observation(
                intent=intent,
                event_id="d" * 32,
                sequence=1,
                filled="1",
                average="10",
                commission="0.1",
                state=ExecutionState.ACKED,
            ),
            intent,
            None,
            _STRATEGY_A,
            _SPECS,
        )
    with pytest.raises(FinancialDataUnavailableError, match="base-currency"):
        derive_execution_posting_v1(
            _observation(
                intent=intent,
                event_id="e" * 32,
                sequence=1,
                filled="1",
                average="10",
                commission="0.1",
                commission_currency="BTC",
            ),
            intent,
            None,
            _STRATEGY_A,
            _SPECS,
        )


def test_managed_fill_duplicate_in_activity_by_provider_order_ref_is_rejected() -> None:
    intent = _intent(intent_id="intent-a")
    posting = derive_execution_posting_v1(
        _observation(
            intent=intent,
            event_id="f" * 32,
            sequence=1,
            filled="1",
            average="10",
            commission="0",
        ),
        intent,
        None,
        _STRATEGY_A,
        _SPECS,
    )
    current_cut = _cut(captured=3000, execution_cursor=1, facts_cursor=1, activity_cursor=1)
    duplicate = FinancialActivityEventV1(
        source_ref="provider-account-activity",
        generation="activity-gen-1",
        sequence=1,
        event_id="activity-copy-of-managed-fill",
        occurred_at_ns=2500,
        owner_key=_MANUAL.key,
        kind=FinancialEventKind.SPOT_FILL,
        cash_movements=(CurrencyAmountV1("USD", Decimal("-10")),),
        instrument=_INSTRUMENT,
        signed_quantity=Decimal("1"),
        price=Decimal("10"),
        provider_order_ref="venue-order-1",
    )
    activity = FinancialActivityBatchV1(
        source_ref="provider-account-activity",
        generation="activity-gen-1",
        from_cursor=0,
        through_cursor=1,
        as_of_ns=2999,
        events=(duplicate,),
    )
    current = _provider_snapshot(current_cut)
    with pytest.raises(FinancialDataUnavailableError, match="duplicates a managed"):
        project_account_financials_v1(
            baseline=_baseline(),
            directory=_DIRECTORY,
            current_account=current,
            current_cut=current_cut,
            execution_batch=_execution_batch(current_cut, (posting,)),
            activity_batch=activity,
            instrument_specs=_SPECS,
            policy=_POLICY,
            now_ns=3001,
        )


def test_late_activity_for_prebaseline_managed_order_is_not_assigned_to_manual_book() -> None:
    intent = _intent(intent_id="intent-before-financial-baseline")
    baseline_cut = _cut(captured=1000, execution_cursor=1)
    execution_wm = baseline_cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX)
    baseline_cursor = FinancialOrderBaselineCursorV1(
        owner=_STRATEGY_A,
        scope_key=intent.scope.key,
        intent_id=intent.intent_id,
        intent_payload_sha256=intent.fingerprint,
        cursor=FinancialExecutionCursorV1(provider_order_id="historical-managed-order"),
    )
    baseline = replace(
        _baseline(),
        cut=baseline_cut,
        provider_snapshot=_provider_snapshot(baseline_cut),
        execution_cursors=FinancialExecutionCursorSnapshotV1(
            journal_generation=execution_wm.generation,
            cursor=execution_wm.cursor,
            as_of_ns=execution_wm.as_of_ns,
            orders=(baseline_cursor,),
        ),
    )
    current_cut = _cut(
        captured=3000,
        execution_cursor=1,
        facts_cursor=1,
        activity_cursor=1,
    )
    activity_watermark = current_cut.watermark(FinancialSourceKind.ACCOUNT_ACTIVITY)
    activity_event = FinancialActivityEventV1(
        source_ref=activity_watermark.source_ref,
        generation=activity_watermark.generation,
        sequence=1,
        event_id="late-managed-fill-copy",
        occurred_at_ns=2000,
        owner_key=_MANUAL.key,
        kind=FinancialEventKind.SPOT_FILL,
        cash_movements=(CurrencyAmountV1("USD", Decimal("-10")),),
        instrument=_INSTRUMENT,
        signed_quantity=Decimal("1"),
        price=Decimal("10"),
        provider_order_ref="historical-managed-order",
    )
    activity = FinancialActivityBatchV1(
        source_ref=activity_watermark.source_ref,
        generation=activity_watermark.generation,
        from_cursor=0,
        through_cursor=1,
        as_of_ns=activity_watermark.as_of_ns,
        events=(activity_event,),
    )
    with pytest.raises(FinancialDataUnavailableError, match="duplicates a managed"):
        project_account_financials_v1(
            baseline=baseline,
            directory=_DIRECTORY,
            current_account=_provider_snapshot(
                current_cut,
                cash=(CurrencyAmountV1("USD", Decimal("990")),),
                positions=((_INSTRUMENT, Decimal("1")),),
            ),
            current_cut=current_cut,
            execution_batch=_execution_batch(current_cut, from_cursor=1),
            activity_batch=activity,
            instrument_specs=_SPECS,
            policy=_POLICY,
            now_ns=3001,
        )


def test_stale_cut_or_activity_gap_fails_closed() -> None:
    baseline = _baseline()
    stale = _cut(captured=3000, facts_cursor=1, facts_generation="replacement-provider-gen")
    with pytest.raises(FinancialDataUnavailableError, match="generation or cursor changed"):
        project_account_financials_v1(
            baseline=baseline,
            directory=_DIRECTORY,
            current_account=_provider_snapshot(stale),
            current_cut=stale,
            execution_batch=_execution_batch(stale),
            activity_batch=_empty_activity(stale),
            instrument_specs=_SPECS,
            policy=_POLICY,
            now_ns=3001,
        )
    current = _cut(captured=3000, facts_cursor=1, activity_cursor=1)
    with pytest.raises(FinancialDataUnavailableError, match="cursor has a gap"):
        gap = FinancialActivityBatchV1(
            source_ref="provider-account-activity",
            generation="activity-gen-1",
            from_cursor=0,
            through_cursor=1,
            as_of_ns=2999,
            events=(),
        )
        project_account_financials_v1(
            baseline=baseline,
            directory=_DIRECTORY,
            current_account=_provider_snapshot(current),
            current_cut=current,
            execution_batch=_execution_batch(current),
            activity_batch=gap,
            instrument_specs=_SPECS,
            policy=_POLICY,
            now_ns=3001,
        )


def test_execution_scan_rejects_filtered_or_truncated_global_outbox_pages() -> None:
    with pytest.raises(FinancialDataUnavailableError, match="gap or truncated page"):
        FinancialExecutionBatchV1(
            journal_generation="1" * 32,
            from_cursor=10,
            through_cursor=12,
            scanned_row_count=1,
            postings=(),
        )
    with pytest.raises(FinancialDataUnavailableError, match="incomplete or filtered"):
        FinancialExecutionBatchV1(
            journal_generation="1" * 32,
            from_cursor=10,
            through_cursor=12,
            scanned_row_count=2,
            postings=(),
            scope_filter_applied=True,
        )


def test_cost_basis_unknown_and_unsupported_products_are_not_fabricated() -> None:
    baseline = _baseline(
        owner_cash={
            _STRATEGY_A.key: Decimal("500"),
            _STRATEGY_B.key: Decimal("500"),
            _MANUAL.key: Decimal("0"),
        },
        owner_positions={
            _STRATEGY_A.key: (
                OwnerPositionCostV1(
                    _INSTRUMENT,
                    Decimal("1"),
                    None,
                    None,
                    metadata_version="instrument-v1",
                ),
            )
        },
        account_positions=((_INSTRUMENT, Decimal("1")),),
    )
    current_cut = _cut(captured=3000, facts_cursor=1)
    current = _provider_snapshot(
        current_cut,
        cash=(CurrencyAmountV1("USD", Decimal("1000")),),
        positions=((_INSTRUMENT, Decimal("1")),),
    )
    projection = project_account_financials_v1(
        baseline=baseline,
        directory=_DIRECTORY,
        current_account=current,
        current_cut=current_cut,
        execution_batch=_execution_batch(current_cut),
        activity_batch=_empty_activity(current_cut),
        instrument_specs=_SPECS,
        policy=_POLICY,
        now_ns=3001,
    )
    book = projection.owner_book(_STRATEGY_A.key)
    assert book.cost_basis_complete is False
    assert book.positions[0].cost_basis is None
    with pytest.raises(FinancialLedgerError, match="zero position requires"):
        OwnerPositionCostV1(_INSTRUMENT, Decimal("0"), None, None)
    with pytest.raises(FinancialDataUnavailableError, match="unit quantity multiplier"):
        FinancialInstrumentSpecV1(
            _INSTRUMENT,
            "instrument-v2",
            FinancialProductModel.SPOT,
            "BTC",
            "USD",
            multiplier=Decimal("100"),
        )


def test_nav_requires_exact_book_cut_fresh_mark_and_fx() -> None:
    intent = _intent(intent_id="intent-a", quantity="1")
    posting = derive_execution_posting_v1(
        _observation(
            intent=intent,
            event_id="1" * 32,
            sequence=1,
            filled="1",
            average="10",
            commission="0",
        ),
        intent,
        None,
        _STRATEGY_A,
        _SPECS,
    )
    current_cut = _cut(captured=3000, execution_cursor=1, facts_cursor=1)
    current = _provider_snapshot(
        current_cut,
        cash=(CurrencyAmountV1("USD", Decimal("990")),),
        positions=((_INSTRUMENT, Decimal("1")),),
    )
    projection = project_account_financials_v1(
        baseline=_baseline(),
        directory=_DIRECTORY,
        current_account=current,
        current_cut=current_cut,
        execution_batch=_execution_batch(current_cut, (posting,)),
        activity_batch=_empty_activity(current_cut),
        instrument_specs=_SPECS,
        policy=_POLICY,
        now_ns=3001,
    )
    book = projection.owner_book(_STRATEGY_A.key)
    mark_wm = FinancialWatermarkV1(
        FinancialSourceKind.MARKET_MARKS, "mark-feed", "mark-gen-1", 4, 2999
    )
    fx_wm = FinancialWatermarkV1(FinancialSourceKind.FX_RATES, "fx-feed", "fx-gen-1", 9, 2999)
    view_cut = FinancialViewCutV1(current_cut, (mark_wm, fx_wm))
    valuation = FinancialValuationSnapshotV1(
        view_cut,
        (
            FinancialMarkV1(
                _INSTRUMENT,
                Decimal("12"),
                "USD",
                "instrument-v1",
                "mark-feed",
                "mark-gen-1",
                4,
                2999,
            ),
        ),
        (),
        "USD",
        "spot-nav-v1",
    )
    valuation_policy = FinancialValuationPolicyV1(
        cash_currency="USD",
        reporting_currency="USD",
        max_account_fact_age_ns=10**12,
        max_activity_age_ns=10**12,
        max_cut_skew_ns=10**12,
        max_mark_age_ns=10**12,
        max_fx_age_ns=10**12,
    )
    assert owner_nav_v1(book, valuation, _SPECS, valuation_policy, now_ns=3001) == Decimal("502")

    wrong_mark_cut = FinancialViewCutV1(
        current_cut,
        (
            FinancialWatermarkV1(
                FinancialSourceKind.MARKET_MARKS, "mark-feed", "replacement-gen", 4, 2999
            ),
            fx_wm,
        ),
    )
    wrong_mark = FinancialValuationSnapshotV1(
        wrong_mark_cut,
        valuation.marks,
        (),
        "USD",
        "spot-nav-v1",
    )
    with pytest.raises(FinancialDataUnavailableError, match="mark facts do not match"):
        owner_nav_v1(book, wrong_mark, _SPECS, valuation_policy, now_ns=3001)

    changed_ledger = _cut(
        captured=3000, execution_cursor=1, facts_cursor=1, facts_generation="new-gen"
    )
    changed_cut = FinancialViewCutV1(changed_ledger, (mark_wm, fx_wm))
    assert_same_financial_cut(view_cut, view_cut)
    with pytest.raises(FinancialDataUnavailableError, match="different ledger cuts"):
        owner_nav_v1(
            book,
            FinancialValuationSnapshotV1(changed_cut, valuation.marks, (), "USD", "spot-nav-v1"),
            _SPECS,
            valuation_policy,
            now_ns=3001,
        )

    eur_cash_book = type(book)(
        owner=book.owner,
        ledger_cut=book.ledger_cut,
        cash=(CurrencyAmountV1("EUR", Decimal("10")),),
        positions=book.positions,
        cumulative_fees=book.cumulative_fees,
        realized_pnl=book.realized_pnl,
        cost_basis_complete=book.cost_basis_complete,
    )
    euro_fx = FinancialFxRateV1("EUR", "USD", Decimal("1.2"), "fx-feed", "fx-gen-1", 9, 2999)
    euro_valuation = FinancialValuationSnapshotV1(
        view_cut,
        valuation.marks,
        (euro_fx,),
        "USD",
        "spot-nav-v1",
    )
    assert owner_nav_v1(
        eur_cash_book, euro_valuation, _SPECS, valuation_policy, now_ns=3001
    ) == Decimal("24")
    with pytest.raises(FinancialDataUnavailableError, match="required FX rate is missing"):
        owner_nav_v1(eur_cash_book, valuation, _SPECS, valuation_policy, now_ns=3001)
    stale_fx_policy = FinancialValuationPolicyV1(
        cash_currency="USD",
        reporting_currency="USD",
        max_account_fact_age_ns=10**12,
        max_activity_age_ns=10**12,
        max_mark_age_ns=10**12,
        max_fx_age_ns=1,
        max_cut_skew_ns=10**12,
    )
    with pytest.raises(FinancialDataUnavailableError, match="valuation source is stale"):
        owner_nav_v1(eur_cash_book, euro_valuation, _SPECS, stale_fx_policy, now_ns=3001)
