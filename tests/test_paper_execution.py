"""Pure spot-V1 paper matching contracts and balance conservation."""

from __future__ import annotations

from decimal import Decimal
from time import time_ns

import pytest

from bt_api_execution import (
    ExecutionScope,
    ExecutionState,
    OrderIntent,
    OrderType,
    PaperDispatchResultV1,
    PaperInstrumentV1,
    PaperOwnerDomainV1,
    PaperQuoteV1,
    PositionEffect,
    Side,
    TimeInForce,
    apply_paper_decision,
    evaluate_paper_match,
    initial_paper_state,
)


def _domain(*, initial_cash: Decimal = Decimal("100"), positions=()) -> PaperOwnerDomainV1:
    return PaperOwnerDomainV1(
        execution_domain_ref="paper-domain-test",
        simulation_account_ref="sim-account-test",
        experiment_ref="experiment-test",
        strategy_ref="strategy-test",
        model_ref="spot_market_top_of_book",
        model_version="1",
        quote_source_ref="quote-feed-test",
        cash_currency="USDT",
        initial_cash=initial_cash,
        initial_fees=Decimal("0"),
        fee_rate=Decimal("0.001"),
        fee_quantum=Decimal("0.0001"),
        max_quote_age_ns=10_000_000_000,
        instruments=(
            PaperInstrumentV1(
                "BTC-USDT", "instrument-v1", "BTC", "USDT", Decimal("0.25"), Decimal("0.01")
            ),
        ),
        opening_positions=tuple(positions),
    )


def _intent(
    *, side: Side = Side.BUY, quantity: Decimal = Decimal("1.25"), intent_id: str = "paper-pure"
) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        scope=ExecutionScope("paper", "paper", "sim-account-test", "strategy-test", "20261016"),
        signal_id=f"signal-{intent_id}",
        instrument="BTC-USDT",
        side=side,
        position_effect=PositionEffect.OPEN if side is Side.BUY else PositionEffect.CLOSE,
        order_type=OrderType.MARKET,
        quantity=quantity,
        price=None,
        time_in_force=TimeInForce.FOK,
        reduce_only=side is Side.SELL,
        metadata_version="instrument-v1",
    )


def _quote(
    *, cursor: int = 1, as_of_ns: int | None = None, quote_currency: str = "USDT"
) -> PaperQuoteV1:
    return PaperQuoteV1(
        source_ref="quote-feed-test",
        generation="quote-generation-1",
        cursor=cursor,
        instrument="BTC-USDT",
        metadata_version="instrument-v1",
        quote_currency=quote_currency,
        bid=Decimal("9.98"),
        ask=Decimal("10.02"),
        bid_quantity=Decimal("5"),
        ask_quantity=Decimal("5"),
        as_of_ns=time_ns() if as_of_ns is None else as_of_ns,
    )


def _propose(intent: OrderIntent, quote: PaperQuoteV1) -> PaperDispatchResultV1:
    return PaperDispatchResultV1(intent.intent_id, intent.fingerprint, quote, None)


@pytest.mark.unit
def test_spot_buy_and_partial_sell_preserve_cash_inventory_and_fees() -> None:
    domain = _domain()
    opening = initial_paper_state(domain)
    buy_intent = _intent()
    buy = evaluate_paper_match(
        domain, buy_intent, _propose(buy_intent, _quote()), opening, now_ns=time_ns()
    )

    assert buy.observation.state is ExecutionState.FILLED
    assert buy.fill_price == Decimal("10.02")
    assert buy.fee_amount == Decimal("0.0125")
    after_buy = apply_paper_decision(domain, opening, buy)
    assert after_buy.cash == Decimal("87.4625")
    assert after_buy.cumulative_fees == Decimal("0.0125")
    assert after_buy.positions[0].quantity == Decimal("1.25")
    assert after_buy.positions[0].average_cost == Decimal("10.02")

    sell_intent = _intent(side=Side.SELL, quantity=Decimal("0.50"), intent_id="paper-sell")
    sell = evaluate_paper_match(
        domain,
        sell_intent,
        _propose(sell_intent, _quote(cursor=2)),
        after_buy,
        now_ns=time_ns(),
    )
    after_sell = apply_paper_decision(domain, after_buy, sell)

    assert sell.observation.state is ExecutionState.FILLED
    assert sell.fill_price == Decimal("9.98")
    assert sell.fee_amount == Decimal("0.0050")
    assert after_sell.cash == Decimal("92.4475")
    assert after_sell.cumulative_fees == Decimal("0.0175")
    assert after_sell.positions[0].quantity == Decimal("0.75")
    assert after_sell.positions[0].average_cost == Decimal("10.02")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("quote_kind", "quantity", "expected_reason"),
    [
        ("missing", Decimal("1.25"), "paper_quote_unavailable"),
        ("stale", Decimal("1.25"), "paper_quote_stale"),
        ("shallow", Decimal("6"), "paper_top_of_book_depth_insufficient"),
        ("wrong-currency", Decimal("1.25"), "paper_quote_invalid"),
    ],
)
def test_unavailable_stale_or_insufficient_quote_has_no_economic_effect(
    quote_kind: str, quantity: Decimal, expected_reason: str
) -> None:
    domain = _domain()
    state = initial_paper_state(domain)
    intent = _intent(quantity=quantity, intent_id=f"refuse-{expected_reason}")
    quote = {
        "missing": None,
        "stale": _quote(as_of_ns=1),
        "shallow": _quote(),
        "wrong-currency": _quote(quote_currency="EUR"),
    }[quote_kind]
    proposal = PaperDispatchResultV1(
        intent.intent_id,
        intent.fingerprint,
        quote,
        "paper_quote_unavailable" if quote is None else None,
    )

    decision = evaluate_paper_match(domain, intent, proposal, state, now_ns=time_ns())
    after = apply_paper_decision(domain, state, decision)

    assert decision.observation.state is ExecutionState.REJECTED
    assert decision.reason_code == expected_reason
    assert decision.filled_quantity == 0
    assert after == state
