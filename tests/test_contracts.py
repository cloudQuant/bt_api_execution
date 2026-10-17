from __future__ import annotations

from decimal import Decimal

import pytest

from bt_api_execution import (
    ContractValidationError,
    ExecutionPlan,
    ExecutionScope,
    ExecutionState,
    OrderIntent,
    PositionEffect,
    ProviderObservation,
    Side,
    can_transition,
    order_intent_from_payload,
)


def _scope() -> ExecutionScope:
    return ExecutionScope("FAKE", "simulation", "acct_demo", "strategy.demo", "20260922")


def _intent(*, intent_id: str = "signal-1") -> OrderIntent:
    return OrderIntent.limit(
        intent_id=intent_id,
        scope=_scope(),
        signal_id="signal-1",
        instrument="BTC-USDT",
        side=Side.BUY,
        quantity=Decimal("1.2500"),
        price=Decimal("50000.10"),
        metadata_version="metadata-v1",
        tags={"candidate": "candidate-1"},
    )


@pytest.mark.unit
def test_intent_fingerprint_is_decimal_stable_and_payload_round_trips() -> None:
    intent = _intent()

    rebuilt = order_intent_from_payload(intent.to_payload())

    assert rebuilt == intent
    assert rebuilt.fingerprint == intent.fingerprint
    assert rebuilt.to_payload()["quantity"] == "1.2500"
    assert rebuilt.to_payload()["price"] == "50000.10"


@pytest.mark.unit
def test_close_intent_requires_reduce_only_and_limit_requires_price() -> None:
    with pytest.raises(ContractValidationError, match="close intent"):
        OrderIntent.limit(
            intent_id="close-1",
            scope=_scope(),
            signal_id="signal-close",
            instrument="BTC-USDT",
            side=Side.SELL,
            quantity=Decimal("1"),
            price=Decimal("50000"),
            position_effect=PositionEffect.CLOSE,
        )

    with pytest.raises(ContractValidationError, match="limit intent requires price"):
        OrderIntent(
            intent_id="limit-no-price",
            scope=_scope(),
            signal_id="signal-2",
            instrument="BTC-USDT",
            side=Side.BUY,
            position_effect=PositionEffect.OPEN,
            order_type="LIMIT",
            quantity=Decimal("1"),
        )


@pytest.mark.unit
def test_legacy_dated_close_payloads_keep_their_existing_fingerprint_shape() -> None:
    payload = {
        "intent_id": "close-today-1",
        "scope": _scope().to_payload(),
        "signal_id": "signal-close-today",
        "instrument": "BTC-USDT",
        "side": "SELL",
        "position_effect": "CLOSE_TODAY",
        "order_type": "LIMIT",
        "quantity": "1",
        "price": "50000",
        "time_in_force": "GTC",
        "reduce_only": True,
        "metadata_version": "metadata-v1",
        "deadline_ns": None,
        "tags": {},
    }

    intent = order_intent_from_payload(payload)

    assert intent.position_effect is PositionEffect.CLOSE_TODAY
    assert intent.position_side == "NET"
    assert intent.position_bucket == "TODAY"
    assert intent.to_payload() == payload


@pytest.mark.unit
def test_explicit_hedge_bucket_round_trips_without_rewriting_effect() -> None:
    intent = OrderIntent.limit(
        intent_id="close-long-yesterday-1",
        scope=_scope(),
        signal_id="signal-close-long-yesterday",
        instrument="BTC-USDT",
        side=Side.SELL,
        quantity=Decimal("1"),
        price=Decimal("50000"),
        position_effect=PositionEffect.CLOSE,
        reduce_only=True,
        position_side="LONG",
        position_bucket="YESTERDAY",
    )

    rebuilt = order_intent_from_payload(intent.to_payload())

    assert rebuilt == intent
    assert rebuilt.fingerprint == intent.fingerprint
    assert rebuilt.position_effect is PositionEffect.CLOSE
    assert rebuilt.position_side == "LONG"
    assert rebuilt.position_bucket == "YESTERDAY"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("position_effect", "position_side", "position_bucket", "reduce_only", "message"),
    [
        (PositionEffect.OPEN, "BOTH", "ALL", False, "invalid position_side"),
        (PositionEffect.OPEN, "NET", "TOMORROW", False, "invalid position_bucket"),
        (PositionEffect.OPEN, "NET", "TODAY", False, "opening intent cannot select a close bucket"),
        (PositionEffect.CLOSE_TODAY, "NET", "YESTERDAY", True, "position effect and bucket disagree"),
        (PositionEffect.CLOSE_YESTERDAY, "NET", "TODAY", True, "position effect and bucket disagree"),
    ],
)
def test_invalid_position_dimensions_are_rejected(
    position_effect: PositionEffect,
    position_side: str,
    position_bucket: str,
    reduce_only: bool,
    message: str,
) -> None:
    with pytest.raises(ContractValidationError, match=message):
        OrderIntent.limit(
            intent_id="invalid-position-dimensions",
            scope=_scope(),
            signal_id="signal-invalid-position-dimensions",
            instrument="BTC-USDT",
            side=Side.SELL,
            quantity=Decimal("1"),
            price=Decimal("50000"),
            position_effect=position_effect,
            reduce_only=reduce_only,
            position_side=position_side,
            position_bucket=position_bucket,
        )



@pytest.mark.unit
def test_plan_rejects_duplicate_intent_ids_and_transitions_are_closed() -> None:
    with pytest.raises(ContractValidationError, match="duplicate"):
        ExecutionPlan("plan-1", _scope(), ("intent-1", "intent-1"))

    assert can_transition(ExecutionState.DISPATCHING, ExecutionState.UNKNOWN)
    assert can_transition(ExecutionState.UNKNOWN, ExecutionState.FILLED)
    assert not can_transition(ExecutionState.FILLED, ExecutionState.ACKED)


@pytest.mark.unit
def test_provider_observation_rejects_non_evidence_state() -> None:
    with pytest.raises(ContractValidationError, match="evidenced"):
        ProviderObservation("signal-1", ExecutionState.UNKNOWN)


@pytest.mark.unit
def test_provider_observation_keeps_a_finite_cumulative_commission() -> None:
    observation = ProviderObservation(
        "signal-1",
        ExecutionState.FILLED,
        provider_order_id="provider-order-1",
        filled_quantity=Decimal("1.25"),
        average_price=Decimal("50000"),
        # A maker rebate is a valid observed venue fee fact.
        cumulative_commission=Decimal("-0.03"),
    )

    assert observation.cumulative_commission == Decimal("-0.03")

    with pytest.raises(ContractValidationError, match="cumulative_commission"):
        ProviderObservation(
            "signal-1",
            ExecutionState.FILLED,
            provider_order_id="provider-order-1",
            filled_quantity=Decimal("1.25"),
            average_price=Decimal("50000"),
            cumulative_commission=Decimal("NaN"),
        )


@pytest.mark.unit
def test_scope_keys_are_unambiguous_and_binary_float_amounts_are_rejected() -> None:
    # The text fields permit ':' for provider conventions, so keys must be a
    # canonical digest rather than an ambiguous delimiter join.
    first = ExecutionScope("A:B", "C", "account", "strategy")
    second = ExecutionScope("A", "B:C", "account", "strategy")
    assert first.account_key != second.account_key

    with pytest.raises(ContractValidationError, match="quantity"):
        OrderIntent.limit(
            intent_id="float-amount",
            scope=_scope(),
            signal_id="signal-float",
            instrument="BTC-USDT",
            side=Side.BUY,
            quantity=1.25,
            price=Decimal("1"),
        )
