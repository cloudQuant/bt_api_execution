"""Focused v3 three-leg coverage for the production group budget quote builder.

``build_instrument_risk_group_budget_quote`` originally refused every
``request.version != 1``, so the real v3 three-leg request path failed closed
as ``GROUP_BUDGET_QUOTE_INVALID`` and host tests had to synthesize quotes.
These tests pin the widening: the production builder quotes an unquoted
version-3 request leg by leg (per-leg instrument multipliers/fees, exact leg
sum, ratio-carrying plan binding) and the coordinator persists that quote, the
legacy version-1 behavior is unchanged, and every malformed or already-quoted
request stays fail-closed.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from functools import partial

import pytest

from bt_api_execution import (
    ExecutionScope,
    GroupBudgetQuote,
    GroupBudgetReservationState,
    GroupBudgetUnsupportedOrderError,
    GroupOrderCoordinator,
    GroupOrderIntent,
    GroupOrderLegPlan,
    GroupOrderState,
    OrderIntent,
    OrderType,
    PortfolioGroupBudgetPolicy,
    Side,
    SqliteOrderGroupStore,
    TimeInForce,
    build_instrument_risk_group_budget_quote,
    group_leg_intent_id,
    order_intent_from_payload,
    payload_sha256,
)
from bt_api_execution.errors import ContractValidationError

_NOW = 1_800_000_000_000_000_000
_DEADLINE = _NOW + 10_000_000_000
_GROUP = "group-quotewiden"
_ACCOUNT = "account-a"
_PORTFOLIO = "portfolio-quotewiden"
_STRATEGY = "strategy-quotewiden"

# Same-account put -> future -> call staging order with one distinct
# instrument per leg, each carrying its own reviewed multiplier, taker fee,
# fixed fee and slippage bound.
_LEG_SPECS = (
    {
        "leg_id": "leg-put",
        "instrument": "PUT-USD",
        "side": Side.BUY,
        "quantity": "2",
        "price": "10",
        "ratio": "1",
        "step": "0.5",
        "multiplier": "2",
        "taker_bps": "25",
        "fixed_fee": "0.4",
        "slippage_bps": "100",
    },
    {
        "leg_id": "leg-future",
        "instrument": "FUT-USD",
        "side": Side.SELL,
        "quantity": "4",
        "price": "20",
        "ratio": "2",
        "step": "0.25",
        "multiplier": "1",
        "taker_bps": "10",
        "fixed_fee": "0.1",
        "slippage_bps": "50",
    },
    {
        "leg_id": "leg-call",
        "instrument": "CALL-USD",
        "side": Side.BUY,
        "quantity": "3",
        "price": "5",
        "ratio": "3",
        "step": "0.5",
        "multiplier": "3",
        "taker_bps": "25",
        "fixed_fee": "0.2",
        "slippage_bps": "100",
    },
)


def _metadata_records(risk):
    return tuple(
        risk.InstrumentRiskMetadata(
            instrument=spec["instrument"],
            metadata_version="fixture-metadata-v1",
            as_of_ns=_NOW - 1_000_000,
            expires_at_ns=_DEADLINE,
            tick_size=Decimal("0.01"),
            quantity_step=Decimal(spec["step"]),
            contract_multiplier=Decimal(spec["multiplier"]),
            max_gross_notional=Decimal("100000"),
            taker_fee_bps=Decimal(spec["taker_bps"]),
            fixed_fee=Decimal(spec["fixed_fee"]),
            max_slippage_bps=Decimal(spec["slippage_bps"]),
            quantity_unit="contract",
            valuation_unit="USD",
        )
        for spec in _LEG_SPECS
    )


def _policy(
    portfolio: str = _PORTFOLIO,
    *,
    unit: str = "USD",
) -> PortfolioGroupBudgetPolicy:
    return PortfolioGroupBudgetPolicy(
        portfolio_ref=portfolio,
        policy_id="quotewiden-budget-policy",
        policy_version=1,
        valuation_unit=unit,
        max_group_gross_notional=Decimal("1000000"),
        max_active_gross_notional=Decimal("10000000"),
        max_active_groups=100,
    )


def _leg_intent(meta, spec: dict[str, object], scope: ExecutionScope) -> OrderIntent:
    return OrderIntent.limit(
        intent_id=group_leg_intent_id(_GROUP, spec["leg_id"]),
        scope=scope,
        signal_id=f"signal-{spec['leg_id']}",
        instrument=meta.instrument,
        side=spec["side"],
        quantity=Decimal(spec["quantity"]),
        price=Decimal(spec["price"]),
        time_in_force=TimeInForce.IOC,
        metadata_version=meta.metadata_version,
        deadline_ns=_DEADLINE,
        tags={"instrument_metadata_digest": meta.digest},
    )


def _build_v3_request(metadata) -> GroupOrderIntent:
    scope = ExecutionScope("paper", "demo", _ACCOUNT, _STRATEGY)
    plans = tuple(
        GroupOrderLegPlan(
            leg_id=spec["leg_id"],
            account_ref=_ACCOUNT,
            maximum_intent=_leg_intent(meta, spec, scope),
            base_units_per_order_unit=Decimal("1"),
            quantity_step=meta.quantity_step,
            metadata_valid_until_ns=meta.expires_at_ns,
            ratio=Decimal(spec["ratio"]),
        )
        for spec, meta in zip(_LEG_SPECS, metadata, strict=True)
    )
    return GroupOrderIntent(
        group_id=_GROUP,
        portfolio_ref=_PORTFOLIO,
        strategy_id=_STRATEGY,
        coordinator_principal_ref="coordinator-quotewiden",
        received_at_ns=_NOW,
        deadline_ns=_DEADLINE,
        first_leg_id="leg-put",
        legs=plans,
        version=3,
    )


def _build_v1_request(metadata) -> GroupOrderIntent:
    """The legacy two-leg body: no version key, no ratios."""

    scope = ExecutionScope("paper", "demo", _ACCOUNT, _STRATEGY)
    plans = tuple(
        GroupOrderLegPlan(
            leg_id=spec["leg_id"],
            account_ref=_ACCOUNT,
            maximum_intent=_leg_intent(meta, spec, scope),
            base_units_per_order_unit=Decimal("1"),
            quantity_step=meta.quantity_step,
            metadata_valid_until_ns=meta.expires_at_ns,
        )
        for spec, meta in zip(_LEG_SPECS[:2], metadata[:2], strict=True)
    )
    return GroupOrderIntent(
        group_id=_GROUP,
        portfolio_ref=_PORTFOLIO,
        strategy_id=_STRATEGY,
        coordinator_principal_ref="coordinator-quotewiden",
        received_at_ns=_NOW,
        deadline_ns=_DEADLINE,
        first_leg_id="leg-put",
        legs=plans,
    )


def _assess(risk, registry, plan: GroupOrderLegPlan):
    return registry.assess(
        risk.InstrumentRiskOrder(
            instrument=plan.maximum_intent.instrument,
            quantity=plan.maximum_intent.quantity,
            limit_price=plan.maximum_intent.price,
            metadata_version=plan.maximum_intent.metadata_version,
            metadata_digest=plan.maximum_intent.tags["instrument_metadata_digest"],
        ),
        _NOW + 1,
    )


def test_v3_three_leg_quote_builds_with_per_leg_multipliers_fees_and_ratios() -> None:
    risk = pytest.importorskip("bt_api_risk")
    metadata = _metadata_records(risk)
    request = _build_v3_request(metadata)
    assert GroupOrderIntent.from_payload(request.to_payload()) == request
    policy = _policy()
    registry = risk.InstrumentRiskRegistry(metadata)

    quote = build_instrument_risk_group_budget_quote(request, policy, registry, now_ns=_NOW + 1)

    assert type(quote) is GroupBudgetQuote
    assert quote.policy_fingerprint == policy.fingerprint
    assert quote.valuation_unit == "USD"
    assert tuple(leg.leg_id for leg in quote.legs) == ("leg-put", "leg-future", "leg-call")
    # Three distinct trusted metadata digests prove per-leg instrument facts.
    assert len({leg.metadata_digest for leg in quote.legs}) == 3
    for quote_leg, plan, meta, spec in zip(
        quote.legs, request.legs, metadata, _LEG_SPECS, strict=True
    ):
        assessment = _assess(risk, registry, plan)
        assert assessment.metadata is meta
        assert assessment.metadata.contract_multiplier == Decimal(spec["multiplier"])
        assert assessment.metadata.taker_fee_bps == Decimal(spec["taker_bps"])
        assert assessment.metadata.fixed_fee == Decimal(spec["fixed_fee"])
        assert quote_leg.account_ref == _ACCOUNT
        assert quote_leg.intent_fingerprint == plan.maximum_intent.fingerprint
        assert quote_leg.metadata_digest == meta.digest
        assert quote_leg.assessment_fingerprint == assessment.bound_payload_fingerprint(
            plan.maximum_intent.fingerprint
        )
        assert quote_leg.valuation_unit == "USD"
        assert quote_leg.gross_notional == assessment.gross_notional
        assert quote_leg.assessed_at_ns == _NOW + 1
        assert quote_leg.valid_until_ns == _DEADLINE
        # Multiplier, adverse-price envelope and fees push the bound strictly
        # past the raw quote notional of that leg.
        assert quote_leg.gross_notional > plan.maximum_intent.quantity * plan.maximum_intent.price
    assert quote.gross_notional == sum(
        (leg.gross_notional for leg in quote.legs), Decimal("0")
    )
    # The per-leg ratios stay on the request and the quote binds to them.
    assert tuple(plan.ratio for plan in request.legs) == (
        Decimal("1"),
        Decimal("2"),
        Decimal("3"),
    )
    bound = replace(request, budget_quote=quote)
    assert GroupOrderIntent.from_payload(bound.to_payload()) == bound
    changed = replace(
        request,
        legs=(request.legs[0], replace(request.legs[1], ratio=Decimal("4")), request.legs[2]),
    )
    assert changed.fingerprint != request.fingerprint
    assert changed.unquoted_fingerprint != request.unquoted_fingerprint


def test_v3_quote_matches_the_declared_sdk_three_leg_wire_request_shape() -> None:
    """Mirror the SDK ``GroupCoordinatorService._parse_request`` v3 body.

    The execution test tree cannot import the SDK repository, so this test
    rebuilds the exact wire body that service accepts and translates it the
    same way: the five-key submit body (``version``, ``group_id``,
    ``deadline_ns``, ``first_leg_id``, ``legs``) with the four-key leg body
    (``leg_id``, ``account_ref``, ``intent``, ``ratio``), exact positive
    ratios, and plans built from trusted route facts.  The production quote
    must then equal the expectations the DIM09 fixture-side
    ``_replicated_v3_quote`` pinned leg by leg.
    """

    risk = pytest.importorskip("bt_api_risk")
    metadata = _metadata_records(risk)
    policy = _policy()
    registry = risk.InstrumentRiskRegistry(metadata)
    # The DIM09 host fixture routes all three legs of one account to a single
    # instrument; this mirror keeps that shape with non-trivial ratios.
    meta = metadata[0]
    scope = ExecutionScope("paper", "demo", _ACCOUNT, _STRATEGY)
    ratios = ("1", "2", "3")
    sides = (Side.BUY, Side.SELL, Side.BUY)
    quantities = ("2", "4", "3")
    prices = ("10", "20", "5")
    body = {
        "version": 3,
        "group_id": _GROUP,
        "deadline_ns": _DEADLINE,
        "first_leg_id": _LEG_SPECS[0]["leg_id"],
        "legs": [
            {
                "leg_id": spec["leg_id"],
                "account_ref": _ACCOUNT,
                "ratio": ratio,
                "intent": OrderIntent.limit(
                    intent_id=group_leg_intent_id(_GROUP, spec["leg_id"]),
                    scope=scope,
                    signal_id=f"signal-{spec['leg_id']}",
                    instrument=meta.instrument,
                    side=side,
                    quantity=Decimal(quantity),
                    price=Decimal(price),
                    time_in_force=TimeInForce.IOC,
                    metadata_version=meta.metadata_version,
                    deadline_ns=_DEADLINE,
                    tags={"instrument_metadata_digest": meta.digest},
                ).to_payload(),
            }
            for spec, ratio, side, quantity, price in zip(
                _LEG_SPECS, ratios, sides, quantities, prices, strict=True
            )
        ],
    }
    # The SDK declares exactly these key sets; anything else fails closed there.
    assert set(body) == {"version", "group_id", "deadline_ns", "first_leg_id", "legs"}
    assert all(
        set(raw_leg) == {"leg_id", "account_ref", "intent", "ratio"} for raw_leg in body["legs"]
    )

    plans = []
    for raw_leg in body["legs"]:
        ratio = Decimal(raw_leg["ratio"])
        assert ratio > 0
        plans.append(
            GroupOrderLegPlan(
                leg_id=raw_leg["leg_id"],
                account_ref=raw_leg["account_ref"],
                maximum_intent=order_intent_from_payload(raw_leg["intent"]),
                base_units_per_order_unit=Decimal("1"),
                quantity_step=meta.quantity_step,
                metadata_valid_until_ns=meta.expires_at_ns,
                ratio=ratio,
            )
        )
    request = GroupOrderIntent(
        group_id=body["group_id"],
        portfolio_ref=_PORTFOLIO,
        strategy_id=_STRATEGY,
        coordinator_principal_ref="coordinator-quotewiden",
        received_at_ns=_NOW,
        deadline_ns=body["deadline_ns"],
        first_leg_id=body["first_leg_id"],
        legs=tuple(plans),
        version=body["version"],
    )

    quote = build_instrument_risk_group_budget_quote(request, policy, registry, now_ns=_NOW + 1)

    for quote_leg, plan in zip(quote.legs, request.legs, strict=True):
        assessment = _assess(risk, registry, plan)
        assert quote_leg.leg_id == plan.leg_id
        assert quote_leg.account_ref == plan.account_ref
        assert quote_leg.intent_fingerprint == plan.maximum_intent.fingerprint
        assert quote_leg.metadata_digest == assessment.metadata.digest
        assert quote_leg.valuation_unit == policy.valuation_unit
        assert quote_leg.gross_notional == assessment.gross_notional
        assert quote_leg.assessed_at_ns == _NOW + 1
        assert quote_leg.valid_until_ns == min(
            assessment.metadata.expires_at_ns, plan.metadata_valid_until_ns
        )
    assert quote.gross_notional == sum(
        (leg.gross_notional for leg in quote.legs), Decimal("0")
    )
    assert tuple(plan.ratio for plan in request.legs) == tuple(Decimal(item) for item in ratios)
    bound = replace(request, budget_quote=quote)
    assert bound.version == 3
    assert GroupOrderIntent.from_payload(bound.to_payload()) == bound


def test_v3_quote_fails_closed_on_malformed_or_quoted_requests() -> None:
    risk = pytest.importorskip("bt_api_risk")
    metadata = _metadata_records(risk)
    request = _build_v3_request(metadata)
    policy = _policy()
    registry = risk.InstrumentRiskRegistry(metadata)
    quote = build_instrument_risk_group_budget_quote(request, policy, registry, now_ns=_NOW + 1)

    # A request that already carries the trusted quote must never be re-quoted.
    with pytest.raises(ContractValidationError, match="unquoted request"):
        build_instrument_risk_group_budget_quote(
            replace(request, budget_quote=quote), policy, registry, now_ns=_NOW + 1
        )
    # The legacy two-leg path still builds, and its quoted v2 form is refused
    # by the new-request builder exactly as before the widening.
    legacy = _build_v1_request(metadata)
    legacy_quote = build_instrument_risk_group_budget_quote(
        legacy, policy, registry, now_ns=_NOW + 1
    )
    assert tuple(leg.leg_id for leg in legacy_quote.legs) == ("leg-put", "leg-future")
    assert legacy_quote.gross_notional == sum(
        (leg.gross_notional for leg in legacy_quote.legs), Decimal("0")
    )
    with pytest.raises(ContractValidationError, match="unquoted request"):
        build_instrument_risk_group_budget_quote(
            replace(legacy, version=2, budget_quote=legacy_quote),
            policy,
            registry,
            now_ns=_NOW + 1,
        )
    # An assessment time outside the request window fails closed.
    with pytest.raises(ContractValidationError, match="outside the request window"):
        build_instrument_risk_group_budget_quote(request, policy, registry, now_ns=_DEADLINE)
    # The policy must belong to the request portfolio.
    with pytest.raises(ContractValidationError, match="portfolio mismatch"):
        build_instrument_risk_group_budget_quote(
            request, _policy(portfolio="other-portfolio"), registry, now_ns=_NOW + 1
        )
    # Cross-currency valuation can never mix into one quote.
    with pytest.raises(ContractValidationError, match="units or validity"):
        build_instrument_risk_group_budget_quote(
            request, _policy(unit="EUR"), registry, now_ns=_NOW + 1
        )
    # Wrong leg count cannot even form a normalized v3 request.
    with pytest.raises(ContractValidationError, match="exactly three legs"):
        replace(request, legs=request.legs[:2])
    # Bad ratios fail closed at the plan/contract boundary.
    with pytest.raises(ContractValidationError, match="invalid ratio"):
        replace(request.legs[1], ratio=Decimal("0"))
    with pytest.raises(ContractValidationError, match="positive ratio"):
        replace(
            request,
            legs=(request.legs[0], replace(request.legs[1], ratio=None), request.legs[2]),
        )
    with pytest.raises(ContractValidationError, match="first-leg ratio"):
        replace(request, legs=(replace(request.legs[0], ratio=Decimal("2")), *request.legs[1:]))
    # A MARKET leg still has no trusted price bound: refuse before any assess.
    market_leg = replace(
        request.legs[0],
        maximum_intent=replace(
            request.legs[0].maximum_intent, order_type=OrderType.MARKET, price=None
        ),
    )
    market_request = replace(request, legs=(market_leg, request.legs[1], request.legs[2]))
    registry.assess = lambda *_args: pytest.fail(
        "Risk assess must not receive an unbounded market order"
    )
    with pytest.raises(GroupBudgetUnsupportedOrderError) as caught:
        build_instrument_risk_group_budget_quote(
            market_request, policy, registry, now_ns=_NOW + 1
        )
    assert caught.value.code == "group_budget_market_price_unbounded"


class _InertPort:
    """Account port that ``accept`` must never touch (it is quote-only)."""

    risk_scope_fingerprint = payload_sha256({"scope": "quotewiden-account-a"})

    @staticmethod
    def _never(*_args, **_kwargs):
        raise AssertionError("group accept must not perform account-owner RPCs")

    query_group_vector = _never
    reserve_group_vector = _never
    release_group_unclaimed = _never
    submit_group_leg = _never
    query = _never
    resolve_group_leg = _never


def test_v3_accept_persists_the_production_quote_and_budget_hold(tmp_path) -> None:
    risk = pytest.importorskip("bt_api_risk")
    metadata = _metadata_records(risk)
    request = _build_v3_request(metadata)
    policy = _policy()
    registry = risk.InstrumentRiskRegistry(metadata)
    db_path = tmp_path / "quotewiden.sqlite3"
    store = SqliteOrderGroupStore(db_path)
    port = _InertPort()
    coordinator = GroupOrderCoordinator(
        store,
        reservations={_ACCOUNT: port},
        orders={_ACCOUNT: port},
        portfolio_budget_policies={_PORTFOLIO: policy},
        budget_quote_builder=partial(build_instrument_risk_group_budget_quote, registry=registry),
        clock_ns=lambda: _NOW + 1,
    )

    accepted = coordinator.accept(request)

    assert accepted.state is GroupOrderState.PREPARING
    assert accepted.request.version == 3
    quote = accepted.request.budget_quote
    assert type(quote) is GroupBudgetQuote
    assert tuple(leg.leg_id for leg in quote.legs) == ("leg-put", "leg-future", "leg-call")
    assert accepted.budget_reservation is not None
    assert accepted.budget_reservation.state is GroupBudgetReservationState.RESERVED
    assert accepted.budget_reservation.gross_notional == quote.gross_notional
    store.close()

    reopened = SqliteOrderGroupStore(db_path)
    assert reopened.get(_PORTFOLIO, _GROUP) == accepted
    reopened.close()
