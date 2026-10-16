"""Trusted fee-inclusive quotes for the coordinator-local P5A group budget.

The Risk package is imported only when a quote is built.  Importing
``bt_api_execution`` therefore remains independent of the optional instrument
Risk package, while a writable group coordinator can require its real
``InstrumentRiskRegistry`` at the quote boundary.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from .contracts import OrderType
from .errors import ContractValidationError
from .group_order_contracts import (
    GroupBudgetLegQuote,
    GroupBudgetQuote,
    GroupOrderIntent,
    PortfolioGroupBudgetPolicy,
)

if TYPE_CHECKING:
    from collections.abc import Iterable


class GroupBudgetUnsupportedOrderError(ContractValidationError):
    """The built-in Risk quote path lacks a trusted price bound for this order."""

    code = "group_budget_market_price_unbounded"


def build_instrument_risk_group_budget_quote(
    request: GroupOrderIntent,
    policy: PortfolioGroupBudgetPolicy,
    registry: object,
    *,
    now_ns: int,
) -> GroupBudgetQuote:
    """Build the maximum unquoted group budget from trusted Risk assessments.

    ``InstrumentRiskRegistry.assess(order, now_ns)`` is the sole estimator.
    It applies the reviewed adverse-price envelope, multiplier, taker fee and
    fixed fee.  This builder rejects absent quantity/valuation units and any
    cross-currency conversion need; it never trusts a caller-supplied amount.
    Version-1 legacy requests keep their original two-leg behavior; version-3
    three-leg requests are quoted leg by leg with the same estimator, so each
    leg's own instrument multiplier and fees are reflected and the quote binds
    to the ratio-carrying plan fingerprint.  Anything else, including a request
    that already carries a trusted quote, fails closed.
    """

    if (
        type(request) is not GroupOrderIntent
        or request.version not in {1, 3}
        or request.budget_quote is not None
    ):
        raise ContractValidationError("group budget quote source must be an unquoted request")
    if type(policy) is not PortfolioGroupBudgetPolicy:
        raise ContractValidationError("trusted portfolio group budget policy is required")
    if policy.portfolio_ref != request.portfolio_ref:
        raise ContractValidationError("group budget policy portfolio mismatch")
    if type(now_ns) is not int or now_ns <= 0 or now_ns >= request.deadline_ns:
        raise ContractValidationError("group budget assessment time is outside the request window")
    if any(plan.maximum_intent.order_type is OrderType.MARKET for plan in request.legs):
        # The current Risk registry deliberately requires a bounded limit price.
        # Do not invent a market estimate from metadata or caller-provided values.
        raise GroupBudgetUnsupportedOrderError(
            "the built-in group budget estimator has no trusted market-price bound"
        )

    try:
        from bt_api_risk import (
            InstrumentRiskAssessment,
            InstrumentRiskOrder,
            InstrumentRiskRegistry,
        )
    except ImportError as error:
        raise ContractValidationError("instrument Risk package is unavailable") from error
    if type(registry) is not InstrumentRiskRegistry:
        raise ContractValidationError("exact trusted InstrumentRiskRegistry is required")

    quotes: list[GroupBudgetLegQuote] = []
    for plan in request.legs:
        intent = plan.maximum_intent
        metadata_digest = intent.tags.get("instrument_metadata_digest")
        metadata_version = intent.metadata_version
        if type(metadata_digest) is not str or type(metadata_version) is not str:
            raise ContractValidationError("group leg lacks trusted instrument metadata binding")
        risk_order = InstrumentRiskOrder(
            instrument=intent.instrument,
            quantity=intent.quantity,
            limit_price=intent.price,
            metadata_version=metadata_version,
            metadata_digest=metadata_digest,
        )
        # Do not reconstruct or approximate this calculation here.  The public
        # Risk registry validates freshness/lattices and includes multiplier,
        # worst-case slippage, taker bps and fixed fees in gross_notional.
        assessment = registry.assess(risk_order, now_ns)
        if type(assessment) is not InstrumentRiskAssessment:
            raise ContractValidationError("Risk registry returned an untyped assessment")
        metadata = assessment.metadata
        if (
            metadata.instrument != intent.instrument
            or metadata.metadata_version != metadata_version
            or metadata.digest != metadata_digest
            or metadata.quantity_unit is None
            or metadata.valuation_unit != policy.valuation_unit
            or metadata.quantity_step != plan.quantity_step
            or metadata.expires_at_ns < request.deadline_ns
        ):
            raise ContractValidationError(
                "group leg metadata units or validity do not match policy"
            )
        if assessment.order != risk_order:
            raise ContractValidationError("Risk assessment changed the exact group order")
        quotes.append(
            GroupBudgetLegQuote(
                leg_id=plan.leg_id,
                account_ref=plan.account_ref,
                intent_fingerprint=intent.fingerprint,
                metadata_digest=metadata.digest,
                assessment_fingerprint=assessment.bound_payload_fingerprint(intent.fingerprint),
                valuation_unit=metadata.valuation_unit,
                gross_notional=assessment.gross_notional,
                assessed_at_ns=now_ns,
                valid_until_ns=min(metadata.expires_at_ns, plan.metadata_valid_until_ns),
            )
        )
    gross_notional = _exact_decimal_sum(item.gross_notional for item in quotes)
    return GroupBudgetQuote(
        policy_fingerprint=policy.fingerprint,
        valuation_unit=policy.valuation_unit,
        gross_notional=gross_notional,
        legs=tuple(quotes),
    )


def _exact_decimal_sum(values: Iterable[Decimal]) -> Decimal:
    items = tuple(values)
    exponent = min(item.as_tuple().exponent for item in items)
    total = 0
    for item in items:
        parts = item.as_tuple()
        coefficient = int("".join(str(digit) for digit in parts.digits))
        coefficient *= 10 ** (parts.exponent - exponent)
        total += -coefficient if parts.sign else coefficient
    digits = tuple(int(digit) for digit in str(abs(total))) if total else (0,)
    return Decimal((int(total < 0), digits, exponent))


__all__ = ["GroupBudgetUnsupportedOrderError", "build_instrument_risk_group_budget_quote"]
