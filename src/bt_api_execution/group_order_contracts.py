"""Durable two-leg order-group contracts for the coordinator role.

These contracts are server-side normalized values. A strategy request is not
itself trusted sizing, account, Risk, or coordinator authority.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from fractions import Fraction
from typing import Any

from .contracts import (
    TERMINAL_STATES,
    ExecutionState,
    OrderIntent,
    OrderType,
    PositionEffect,
    TimeInForce,
    order_intent_from_payload,
    payload_sha256,
)
from .errors import ContractValidationError
from .group_contracts import GroupPermitBinding, group_leg_intent_id

_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_DECIMAL = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_SAFE_CODE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_NOTIONAL_UNIT = re.compile(r"^[A-Z][A-Z0-9._-]{0,15}$")


def _reference(value: object, name: str) -> str:
    if type(value) is not str or not _REFERENCE.fullmatch(value):
        raise ContractValidationError(f"invalid {name}")
    return value


def _positive_decimal(value: object, name: str) -> Decimal:
    if type(value) is not Decimal or not value.is_finite() or value <= 0:
        raise ContractValidationError(f"invalid {name}")
    return value


class GroupOrderState(StrEnum):
    PREPARING = "PREPARING"
    FIRST_PENDING = "FIRST_PENDING"
    HEDGE_PENDING = "HEDGE_PENDING"
    PAIRED = "PAIRED"
    UNWINDING = "UNWINDING"
    CLOSED = "CLOSED"
    REJECTED_BEFORE_DISPATCH = "REJECTED_BEFORE_DISPATCH"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


class GroupReservationPhase(StrEnum):
    NOT_STARTED = "NOT_STARTED"
    REQUESTING = "REQUESTING"
    ACTIVE = "ACTIVE"
    UNKNOWN = "UNKNOWN"
    DENIED = "DENIED"
    RELEASE_REQUESTED = "RELEASE_REQUESTED"
    RELEASED = "RELEASED"


class GroupBudgetReservationState(StrEnum):
    """Durable state of the coordinator-local gross group-budget hold."""

    RESERVED = "RESERVED"
    HELD = "HELD"
    RELEASED = "RELEASED"
    DENIED = "DENIED"


class GroupLegPhase(StrEnum):
    READY = "READY"
    REQUESTING = "REQUESTING"
    UNKNOWN = "UNKNOWN"
    OBSERVED = "OBSERVED"


@dataclass(frozen=True, slots=True)
class PortfolioGroupBudgetPolicy:
    """Trusted coordinator policy for conservative gross group reservations.

    Amounts are fee- and slippage-inclusive gross notionals in one exact unit.
    This policy never nets across legs/accounts and does not replace either
    account owner's Risk policy.
    """

    portfolio_ref: str
    policy_id: str
    policy_version: int
    valuation_unit: str
    max_group_gross_notional: Decimal
    max_active_gross_notional: Decimal
    max_active_groups: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "portfolio_ref", _reference(self.portfolio_ref, "portfolio_ref"))
        object.__setattr__(self, "policy_id", _reference(self.policy_id, "policy_id"))
        if type(self.policy_version) is not int or self.policy_version <= 0:
            raise ContractValidationError("invalid group budget policy version")
        if type(self.valuation_unit) is not str or not _NOTIONAL_UNIT.fullmatch(
            self.valuation_unit
        ):
            raise ContractValidationError("invalid group budget valuation unit")
        object.__setattr__(
            self,
            "max_group_gross_notional",
            _positive_decimal(self.max_group_gross_notional, "max_group_gross_notional"),
        )
        object.__setattr__(
            self,
            "max_active_gross_notional",
            _positive_decimal(self.max_active_gross_notional, "max_active_gross_notional"),
        )
        if self.max_group_gross_notional > self.max_active_gross_notional:
            raise ContractValidationError("single group cap exceeds active portfolio cap")
        if (
            type(self.max_active_groups) is not int
            or self.max_active_groups <= 0
            or self.max_active_groups > 100_000
        ):
            raise ContractValidationError("invalid active group count limit")

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema": "portfolio-group-budget-policy-v1",
            "semantics": "FEE_INCLUSIVE_GROSS_NO_FX_NO_NETTING_V1",
            "portfolio_ref": self.portfolio_ref,
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "valuation_unit": self.valuation_unit,
            "max_group_gross_notional": format(self.max_group_gross_notional, "f"),
            "max_active_gross_notional": format(self.max_active_gross_notional, "f"),
            "max_active_groups": self.max_active_groups,
        }

    @property
    def fingerprint(self) -> str:
        return payload_sha256(self.to_payload())

    @classmethod
    def from_payload(cls, value: object) -> PortfolioGroupBudgetPolicy:
        expected = {
            "schema",
            "semantics",
            "portfolio_ref",
            "policy_id",
            "policy_version",
            "valuation_unit",
            "max_group_gross_notional",
            "max_active_gross_notional",
            "max_active_groups",
        }
        if (
            not isinstance(value, Mapping)
            or set(value) != expected
            or value["schema"] != "portfolio-group-budget-policy-v1"
            or value["semantics"] != "FEE_INCLUSIVE_GROSS_NO_FX_NO_NETTING_V1"
        ):
            raise ContractValidationError("invalid portfolio group budget policy payload")
        return cls(
            portfolio_ref=value["portfolio_ref"],
            policy_id=value["policy_id"],
            policy_version=value["policy_version"],
            valuation_unit=value["valuation_unit"],
            max_group_gross_notional=_parse_decimal(
                value["max_group_gross_notional"], "max_group_gross_notional"
            ),
            max_active_gross_notional=_parse_decimal(
                value["max_active_gross_notional"], "max_active_gross_notional"
            ),
            max_active_groups=value["max_active_groups"],
        )


@dataclass(frozen=True, slots=True)
class GroupBudgetLegQuote:
    """Server-computed conservative gross quote for one exact maximum leg."""

    leg_id: str
    account_ref: str
    intent_fingerprint: str
    metadata_digest: str
    assessment_fingerprint: str
    valuation_unit: str
    gross_notional: Decimal
    assessed_at_ns: int
    valid_until_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "leg_id", _reference(self.leg_id, "leg_id"))
        object.__setattr__(self, "account_ref", _reference(self.account_ref, "account_ref"))
        for name in ("intent_fingerprint", "metadata_digest", "assessment_fingerprint"):
            _require_digest(getattr(self, name), name)
        if type(self.valuation_unit) is not str or not _NOTIONAL_UNIT.fullmatch(
            self.valuation_unit
        ):
            raise ContractValidationError("invalid group leg valuation unit")
        object.__setattr__(
            self, "gross_notional", _positive_decimal(self.gross_notional, "gross_notional")
        )
        if (
            type(self.assessed_at_ns) is not int
            or self.assessed_at_ns <= 0
            or type(self.valid_until_ns) is not int
            or self.valid_until_ns <= self.assessed_at_ns
        ):
            raise ContractValidationError("invalid group leg assessment interval")

    def to_payload(self) -> dict[str, Any]:
        return {
            "leg_id": self.leg_id,
            "account_ref": self.account_ref,
            "intent_fingerprint": self.intent_fingerprint,
            "metadata_digest": self.metadata_digest,
            "assessment_fingerprint": self.assessment_fingerprint,
            "valuation_unit": self.valuation_unit,
            "gross_notional": format(self.gross_notional, "f"),
            "assessed_at_ns": self.assessed_at_ns,
            "valid_until_ns": self.valid_until_ns,
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupBudgetLegQuote:
        expected = {
            "leg_id",
            "account_ref",
            "intent_fingerprint",
            "metadata_digest",
            "assessment_fingerprint",
            "valuation_unit",
            "gross_notional",
            "assessed_at_ns",
            "valid_until_ns",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise ContractValidationError("invalid group budget leg quote payload")
        return cls(
            leg_id=value["leg_id"],
            account_ref=value["account_ref"],
            intent_fingerprint=value["intent_fingerprint"],
            metadata_digest=value["metadata_digest"],
            assessment_fingerprint=value["assessment_fingerprint"],
            valuation_unit=value["valuation_unit"],
            gross_notional=_parse_decimal(value["gross_notional"], "gross_notional"),
            assessed_at_ns=value["assessed_at_ns"],
            valid_until_ns=value["valid_until_ns"],
        )


@dataclass(frozen=True, slots=True)
class GroupBudgetQuote:
    """Immutable trusted server quote bound to the exact v2 group legs."""

    policy_fingerprint: str
    valuation_unit: str
    gross_notional: Decimal
    legs: tuple[GroupBudgetLegQuote, GroupBudgetLegQuote]

    def __post_init__(self) -> None:
        _require_digest(self.policy_fingerprint, "policy_fingerprint")
        if type(self.valuation_unit) is not str or not _NOTIONAL_UNIT.fullmatch(
            self.valuation_unit
        ):
            raise ContractValidationError("invalid group quote valuation unit")
        object.__setattr__(
            self, "gross_notional", _positive_decimal(self.gross_notional, "gross_notional")
        )
        if (
            type(self.legs) is not tuple
            or len(self.legs) not in {2, 3}
            or any(type(item) is not GroupBudgetLegQuote for item in self.legs)
        ):
            raise ContractValidationError("P5A group quote requires two or three leg facts")
        if len({item.leg_id for item in self.legs}) != len(self.legs) or any(
            item.valuation_unit != self.valuation_unit for item in self.legs
        ):
            raise ContractValidationError("group quote legs do not share one valuation unit")
        if sum((Fraction(item.gross_notional) for item in self.legs), Fraction(0)) != Fraction(
            self.gross_notional
        ):
            raise ContractValidationError("group gross quote does not equal its leg vector")

    @property
    def fingerprint(self) -> str:
        return payload_sha256(self.to_payload())

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema": "group-budget-quote-v1",
            "policy_fingerprint": self.policy_fingerprint,
            "valuation_unit": self.valuation_unit,
            "gross_notional": format(self.gross_notional, "f"),
            "legs": [item.to_payload() for item in self.legs],
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupBudgetQuote:
        expected = {"schema", "policy_fingerprint", "valuation_unit", "gross_notional", "legs"}
        if (
            not isinstance(value, Mapping)
            or set(value) != expected
            or value["schema"] != "group-budget-quote-v1"
            or not isinstance(value["legs"], list)
        ):
            raise ContractValidationError("invalid group budget quote payload")
        return cls(
            policy_fingerprint=value["policy_fingerprint"],
            valuation_unit=value["valuation_unit"],
            gross_notional=_parse_decimal(value["gross_notional"], "gross_notional"),
            legs=tuple(GroupBudgetLegQuote.from_payload(item) for item in value["legs"]),
        )


@dataclass(frozen=True, slots=True)
class GroupBudgetReservation:
    """Persisted local budget hold; status changes share the group transaction."""

    policy_fingerprint: str
    policy: PortfolioGroupBudgetPolicy
    quote_fingerprint: str
    valuation_unit: str
    gross_notional: Decimal
    deadline_ns: int
    state: GroupBudgetReservationState

    def __post_init__(self) -> None:
        _require_digest(self.policy_fingerprint, "policy_fingerprint")
        if (
            type(self.policy) is not PortfolioGroupBudgetPolicy
            or self.policy.fingerprint != self.policy_fingerprint
            or self.policy.valuation_unit != self.valuation_unit
        ):
            raise ContractValidationError("stored group budget policy does not match its binding")
        _require_digest(self.quote_fingerprint, "quote_fingerprint")
        if type(self.valuation_unit) is not str or not _NOTIONAL_UNIT.fullmatch(
            self.valuation_unit
        ):
            raise ContractValidationError("invalid group reservation valuation unit")
        object.__setattr__(
            self, "gross_notional", _positive_decimal(self.gross_notional, "gross_notional")
        )
        if type(self.deadline_ns) is not int or self.deadline_ns <= 0:
            raise ContractValidationError("invalid group budget deadline")
        try:
            object.__setattr__(self, "state", GroupBudgetReservationState(self.state))
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid group budget state") from error

    def to_payload(self) -> dict[str, Any]:
        return {
            "policy_fingerprint": self.policy_fingerprint,
            "policy": self.policy.to_payload(),
            "quote_fingerprint": self.quote_fingerprint,
            "valuation_unit": self.valuation_unit,
            "gross_notional": format(self.gross_notional, "f"),
            "deadline_ns": self.deadline_ns,
            "state": self.state.value,
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupBudgetReservation:
        expected = {
            "policy_fingerprint",
            "policy",
            "quote_fingerprint",
            "valuation_unit",
            "gross_notional",
            "deadline_ns",
            "state",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise ContractValidationError("invalid group budget reservation payload")
        return cls(
            policy_fingerprint=value["policy_fingerprint"],
            policy=PortfolioGroupBudgetPolicy.from_payload(value["policy"]),
            quote_fingerprint=value["quote_fingerprint"],
            valuation_unit=value["valuation_unit"],
            gross_notional=_parse_decimal(value["gross_notional"], "gross_notional"),
            deadline_ns=value["deadline_ns"],
            state=value["state"],
        )


@dataclass(frozen=True, slots=True)
class GroupOrderLegPlan:
    """One maximum reserved child and trusted normalized sizing facts."""

    leg_id: str
    account_ref: str
    maximum_intent: OrderIntent
    base_units_per_order_unit: Decimal
    quantity_step: Decimal
    metadata_valid_until_ns: int
    ratio: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "leg_id", _reference(self.leg_id, "leg_id"))
        object.__setattr__(self, "account_ref", _reference(self.account_ref, "account_ref"))
        if type(self.maximum_intent) is not OrderIntent:
            raise ContractValidationError("group leg requires a normalized order intent")
        object.__setattr__(
            self,
            "base_units_per_order_unit",
            _positive_decimal(self.base_units_per_order_unit, "base_units_per_order_unit"),
        )
        object.__setattr__(
            self, "quantity_step", _positive_decimal(self.quantity_step, "quantity_step")
        )
        if self.maximum_intent.quantity % self.quantity_step != 0:
            raise ContractValidationError("maximum quantity is not aligned to its step")
        if type(self.metadata_valid_until_ns) is not int or self.metadata_valid_until_ns <= 0:
            raise ContractValidationError("invalid metadata_valid_until_ns")
        if self.ratio is not None:
            object.__setattr__(self, "ratio", _positive_decimal(self.ratio, "ratio"))

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "leg_id": self.leg_id,
            "account_ref": self.account_ref,
            "maximum_intent": self.maximum_intent.to_payload(),
            "base_units_per_order_unit": format(self.base_units_per_order_unit, "f"),
            "quantity_step": format(self.quantity_step, "f"),
            "metadata_valid_until_ns": self.metadata_valid_until_ns,
        }
        if self.ratio is not None:
            payload["ratio"] = format(self.ratio, "f")
        return payload

    @classmethod
    def from_payload(cls, value: object) -> GroupOrderLegPlan:
        legacy_keys = {
            "leg_id",
            "account_ref",
            "maximum_intent",
            "base_units_per_order_unit",
            "quantity_step",
            "metadata_valid_until_ns",
        }
        if not isinstance(value, Mapping) or frozenset(value) not in {
            frozenset(legacy_keys),
            frozenset(legacy_keys | {"ratio"}),
        }:
            raise ContractValidationError("invalid group leg plan payload")
        return cls(
            leg_id=value["leg_id"],
            account_ref=value["account_ref"],
            maximum_intent=order_intent_from_payload(value["maximum_intent"]),
            base_units_per_order_unit=_parse_decimal(
                value["base_units_per_order_unit"], "base_units_per_order_unit"
            ),
            quantity_step=_parse_decimal(value["quantity_step"], "quantity_step"),
            metadata_valid_until_ns=value["metadata_valid_until_ns"],
            ratio=None if "ratio" not in value else _parse_decimal(value["ratio"], "ratio"),
        )


@dataclass(frozen=True, slots=True)
class GroupOrderIntent:
    """Immutable, service-normalized two-leg plan with an absolute deadline."""

    group_id: str
    portfolio_ref: str
    strategy_id: str
    coordinator_principal_ref: str
    received_at_ns: int
    deadline_ns: int
    first_leg_id: str
    legs: tuple[GroupOrderLegPlan, GroupOrderLegPlan]
    budget_quote: GroupBudgetQuote | None = None
    recovery_policy: str = "HOLD_AND_QUERY_ORIGINAL_IDS"
    version: int = 1

    def __post_init__(self) -> None:
        for name in (
            "group_id",
            "portfolio_ref",
            "strategy_id",
            "coordinator_principal_ref",
            "first_leg_id",
            "recovery_policy",
        ):
            object.__setattr__(self, name, _reference(getattr(self, name), name))
        if self.recovery_policy != "HOLD_AND_QUERY_ORIGINAL_IDS":
            raise ContractValidationError("unsupported group recovery policy")
        if type(self.received_at_ns) is not int or self.received_at_ns <= 0:
            raise ContractValidationError("invalid received_at_ns")
        if type(self.deadline_ns) is not int or self.deadline_ns <= self.received_at_ns:
            raise ContractValidationError("invalid absolute group deadline")
        if type(self.version) is not int or self.version not in {1, 2, 3}:
            raise ContractValidationError("unsupported group order contract version")
        expected_legs = 3 if self.version == 3 else 2
        if not isinstance(self.legs, tuple) or len(self.legs) != expected_legs:
            raise ContractValidationError(
                "v3 group intent requires exactly three legs"
                if self.version == 3
                else "P5A requires exactly two legs"
            )
        if any(type(leg) is not GroupOrderLegPlan for leg in self.legs):
            raise ContractValidationError("invalid group leg plan")
        ids = tuple(leg.leg_id for leg in self.legs)
        if len(set(ids)) != len(self.legs) or self.first_leg_id not in ids:
            raise ContractValidationError("invalid ordered group legs")
        if self.version == 3:
            # P5B: one same-account Call/Put/Future chain executed strictly in
            # legs tuple order, with exact per-leg ratio semantics.
            if self.legs[0].leg_id != self.first_leg_id:
                raise ContractValidationError("v3 group legs must start at the first leg")
            if len({leg.account_ref for leg in self.legs}) != 1:
                raise ContractValidationError("v3 group legs must share one account")
            if self.legs[0].ratio != Decimal(1):
                raise ContractValidationError("v3 first-leg ratio must equal one")
            for leg in self.legs[1:]:
                if leg.ratio is None or leg.ratio <= 0:
                    raise ContractValidationError("v3 non-first legs require a positive ratio")
        elif any(leg.ratio is not None for leg in self.legs):
            raise ContractValidationError("legacy group legs cannot carry a ratio")
        for leg in self.legs:
            intent = leg.maximum_intent
            if (
                intent.intent_id != group_leg_intent_id(self.group_id, leg.leg_id)
                or intent.scope.account_ref != leg.account_ref
                or intent.scope.strategy_id != self.strategy_id
                or intent.deadline_ns != self.deadline_ns
                or intent.time_in_force is not TimeInForce.IOC
                or intent.position_effect is not PositionEffect.OPEN
                or intent.reduce_only
                or intent.order_type not in {OrderType.LIMIT, OrderType.MARKET}
                or leg.metadata_valid_until_ns < self.deadline_ns
            ):
                raise ContractValidationError("group leg violates the normalized P5A policy")
        quote = self.budget_quote
        if self.version == 1:
            if quote is not None:
                raise ContractValidationError("legacy group intent cannot carry a budget quote")
        elif quote is not None or self.version == 2:
            # Version 2 always requires the trusted quote. A version-3 strategy
            # request may arrive unquoted (the coordinator's trusted builder
            # quotes it); any quote that is present must bind exactly.
            if type(quote) is not GroupBudgetQuote:
                raise ContractValidationError("new group intent requires a trusted budget quote")
            plans = {plan.leg_id: plan for plan in self.legs}
            if {item.leg_id for item in quote.legs} != set(plans):
                raise ContractValidationError("group budget quote membership mismatch")
            for item in quote.legs:
                plan = plans[item.leg_id]
                if (
                    item.account_ref != plan.account_ref
                    or item.intent_fingerprint != plan.maximum_intent.fingerprint
                    or item.metadata_digest
                    != plan.maximum_intent.tags.get("instrument_metadata_digest")
                    or item.valuation_unit != quote.valuation_unit
                    or item.valid_until_ns < self.deadline_ns
                ):
                    raise ContractValidationError("group budget quote is not bound to its leg")
        ordered = self.ordered_legs
        if self.version == 3:
            if len({leg.maximum_intent.side for leg in self.legs}) < 2:
                raise ContractValidationError("v3 group legs cannot all use one side")
            first_plan = ordered[0]
            first_base = Fraction(first_plan.base_units_per_order_unit) * Fraction(
                first_plan.maximum_intent.quantity
            )
            for plan in ordered[1:]:
                reachable = Fraction(plan.ratio) * first_base / Fraction(
                    plan.base_units_per_order_unit
                )
                if reachable < Fraction(plan.quantity_step):
                    raise ContractValidationError(
                        "v3 leg ratio cannot be expressed in its step"
                    )
        elif (
            ordered[0].maximum_intent.side is ordered[1].maximum_intent.side
        ):
            raise ContractValidationError("P5A legs must have opposing sides")

    @property
    def fingerprint(self) -> str:
        return payload_sha256(self.to_payload())

    @property
    def unquoted_fingerprint(self) -> str:
        """Fingerprint request content independently of trusted quote facts."""

        payload = self.to_payload()
        payload["version"] = 1
        payload.pop("budget_quote", None)
        return payload_sha256(payload)

    @property
    def ordered_legs(self) -> tuple[GroupOrderLegPlan, ...]:
        first = self.leg(self.first_leg_id)
        rest = tuple(leg for leg in self.legs if leg.leg_id != self.first_leg_id)
        return (first, *rest)

    def leg(self, leg_id: str) -> GroupOrderLegPlan:
        for leg in self.legs:
            if leg.leg_id == leg_id:
                return leg
        raise ContractValidationError("group leg does not exist")

    @property
    def account_refs(self) -> tuple[str, ...]:
        return tuple(sorted({leg.account_ref for leg in self.legs}))

    def legs_for_account(self, account_ref: str) -> tuple[GroupOrderLegPlan, ...]:
        return tuple(leg for leg in self.legs if leg.account_ref == account_ref)

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "version": self.version,
            "group_id": self.group_id,
            "portfolio_ref": self.portfolio_ref,
            "strategy_id": self.strategy_id,
            "coordinator_principal_ref": self.coordinator_principal_ref,
            "received_at_ns": self.received_at_ns,
            "deadline_ns": self.deadline_ns,
            "first_leg_id": self.first_leg_id,
            "legs": [leg.to_payload() for leg in self.legs],
            "recovery_policy": self.recovery_policy,
        }
        if self.version != 1 and self.budget_quote is not None:
            payload["budget_quote"] = self.budget_quote.to_payload()
        return payload

    @classmethod
    def from_payload(cls, value: object) -> GroupOrderIntent:
        legacy_keys = {
            "version",
            "group_id",
            "portfolio_ref",
            "strategy_id",
            "coordinator_principal_ref",
            "received_at_ns",
            "deadline_ns",
            "first_leg_id",
            "legs",
            "recovery_policy",
        }
        current_keys = legacy_keys | {"budget_quote"}
        if not isinstance(value, Mapping) or frozenset(value) not in {
            frozenset(legacy_keys),
            frozenset(current_keys),
        }:
            raise ContractValidationError("invalid group order intent payload")
        if not isinstance(value["legs"], list):
            raise ContractValidationError("invalid group legs payload")
        return cls(
            version=value["version"],
            group_id=value["group_id"],
            portfolio_ref=value["portfolio_ref"],
            strategy_id=value["strategy_id"],
            coordinator_principal_ref=value["coordinator_principal_ref"],
            received_at_ns=value["received_at_ns"],
            deadline_ns=value["deadline_ns"],
            first_leg_id=value["first_leg_id"],
            legs=tuple(GroupOrderLegPlan.from_payload(leg) for leg in value["legs"]),
            budget_quote=None
            if "budget_quote" not in value
            else GroupBudgetQuote.from_payload(value["budget_quote"]),
            recovery_policy=value["recovery_policy"],
        )


@dataclass(frozen=True, slots=True)
class GroupRiskLegSnapshot:
    leg_id: str
    permit_id: str
    intent_id: str
    reserved_intent_hash: str
    exposure_profile_fingerprint: str
    actual_intent_hash: str | None
    state: str
    claimed: bool
    resolved: bool
    expires_at_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "leg_id", _reference(self.leg_id, "leg_id"))
        for name in ("permit_id", "intent_id"):
            object.__setattr__(self, name, _reference(getattr(self, name), name))
        for name in ("reserved_intent_hash", "exposure_profile_fingerprint"):
            _require_digest(getattr(self, name), name)
        if self.actual_intent_hash is not None:
            _require_digest(self.actual_intent_hash, "actual_intent_hash")
        if self.state not in {"ACTIVE", "SETTLED", "RELEASED", "EXPIRED", "NO_FILL_TERMINAL"}:
            raise ContractValidationError("invalid Risk group leg state")
        if type(self.claimed) is not bool or type(self.resolved) is not bool:
            raise ContractValidationError("invalid Risk group claim flags")
        if type(self.expires_at_ns) is not int or self.expires_at_ns <= 0:
            raise ContractValidationError("invalid Risk leg expiry")
        if self.resolved and not self.claimed:
            raise ContractValidationError("an unclaimed Risk group leg cannot be resolved")
        if self.claimed != (self.actual_intent_hash is not None):
            raise ContractValidationError("Risk group claim hash does not match claim state")
        if self.state == "ACTIVE" and self.resolved:
            raise ContractValidationError("active Risk group leg cannot be resolved")
        if self.state == "SETTLED" and not self.claimed:
            raise ContractValidationError("settled Risk group leg must be claimed")
        if self.state == "NO_FILL_TERMINAL" and not (self.claimed and self.resolved):
            raise ContractValidationError("terminal no-fill Risk leg must be resolved")
        if self.state in {"RELEASED", "EXPIRED"} and (self.claimed or self.resolved):
            raise ContractValidationError("claimed Risk group leg cannot be released or expired")

    def to_payload(self) -> dict[str, Any]:
        return {
            "leg_id": self.leg_id,
            "permit_id": self.permit_id,
            "intent_id": self.intent_id,
            "reserved_intent_hash": self.reserved_intent_hash,
            "exposure_profile_fingerprint": self.exposure_profile_fingerprint,
            "actual_intent_hash": self.actual_intent_hash,
            "state": self.state,
            "claimed": self.claimed,
            "resolved": self.resolved,
            "expires_at_ns": self.expires_at_ns,
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupRiskLegSnapshot:
        if not isinstance(value, Mapping) or set(value) != {
            "leg_id",
            "permit_id",
            "intent_id",
            "reserved_intent_hash",
            "exposure_profile_fingerprint",
            "actual_intent_hash",
            "state",
            "claimed",
            "resolved",
            "expires_at_ns",
        }:
            raise ContractValidationError("invalid stored Risk group leg")
        return cls(**value)


@dataclass(frozen=True, slots=True)
class GroupAccountReservationSnapshot:
    group_id: str
    account_ref: str
    scope_fingerprint: str
    phase: GroupReservationPhase
    group_fingerprint: str | None = None
    policy_fingerprint: str | None = None
    deadline_ns: int | None = None
    legs: tuple[GroupRiskLegSnapshot, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "group_id", _reference(self.group_id, "group_id"))
        object.__setattr__(self, "account_ref", _reference(self.account_ref, "account_ref"))
        _require_digest(self.scope_fingerprint, "scope_fingerprint")
        if type(self.phase) is not GroupReservationPhase:
            object.__setattr__(self, "phase", GroupReservationPhase(self.phase))
        for name in ("group_fingerprint", "policy_fingerprint"):
            value = getattr(self, name)
            if value is not None:
                _require_digest(value, name)
        if self.deadline_ns is not None and (
            type(self.deadline_ns) is not int or self.deadline_ns <= 0
        ):
            raise ContractValidationError("invalid Risk group deadline")
        if not isinstance(self.legs, tuple) or any(
            type(leg) is not GroupRiskLegSnapshot for leg in self.legs
        ):
            raise ContractValidationError("invalid Risk group reservation legs")

    def to_payload(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "account_ref": self.account_ref,
            "scope_fingerprint": self.scope_fingerprint,
            "phase": self.phase.value,
            "group_fingerprint": self.group_fingerprint,
            "policy_fingerprint": self.policy_fingerprint,
            "deadline_ns": self.deadline_ns,
            "legs": [leg.to_payload() for leg in self.legs],
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupAccountReservationSnapshot:
        if (
            not isinstance(value, Mapping)
            or set(value)
            != {
                "group_id",
                "account_ref",
                "scope_fingerprint",
                "phase",
                "group_fingerprint",
                "policy_fingerprint",
                "deadline_ns",
                "legs",
            }
            or not isinstance(value["legs"], list)
        ):
            raise ContractValidationError("invalid stored account reservation")
        return cls(
            group_id=value["group_id"],
            account_ref=value["account_ref"],
            scope_fingerprint=value["scope_fingerprint"],
            phase=GroupReservationPhase(value["phase"]),
            group_fingerprint=value["group_fingerprint"],
            policy_fingerprint=value["policy_fingerprint"],
            deadline_ns=value["deadline_ns"],
            legs=tuple(GroupRiskLegSnapshot.from_payload(leg) for leg in value["legs"]),
        )


@dataclass(frozen=True, slots=True)
class GroupChildOrderStatus:
    intent_id: str
    state: ExecutionState
    filled_quantity: Decimal
    provider_order_id: str | None = None
    average_price: Decimal | None = None
    cumulative_commission: Decimal | None = None
    cumulative_commission_currency: str | None = None
    reason_code: str | None = None
    order_revision: int | None = None
    trade_count: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "intent_id", _reference(self.intent_id, "intent_id"))
        if type(self.state) is not ExecutionState:
            object.__setattr__(self, "state", ExecutionState(self.state))
        if type(self.filled_quantity) is not Decimal or not self.filled_quantity.is_finite():
            raise ContractValidationError("invalid child filled quantity")
        if self.filled_quantity < 0:
            raise ContractValidationError("invalid child filled quantity")
        for name in (
            "provider_order_id",
            "cumulative_commission_currency",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _reference(value, name))
        if self.reason_code is not None and (
            type(self.reason_code) is not str or not _SAFE_CODE.fullmatch(self.reason_code)
        ):
            raise ContractValidationError("invalid child reason_code")
        if self.order_revision is not None and (
            type(self.order_revision) is not int or self.order_revision <= 0
        ):
            raise ContractValidationError("invalid child order_revision")
        if self.trade_count is not None and (
            type(self.trade_count) is not int or self.trade_count < 0
        ):
            raise ContractValidationError("invalid child trade_count")
        for name in ("average_price", "cumulative_commission"):
            value = getattr(self, name)
            if value is not None and (type(value) is not Decimal or not value.is_finite()):
                raise ContractValidationError(f"invalid {name}")

    def to_payload(self) -> dict[str, Any]:
        return {
            "intent_id": self.intent_id,
            "state": self.state.value,
            "filled_quantity": format(self.filled_quantity, "f"),
            "provider_order_id": self.provider_order_id,
            "average_price": None
            if self.average_price is None
            else format(self.average_price, "f"),
            "cumulative_commission": None
            if self.cumulative_commission is None
            else format(self.cumulative_commission, "f"),
            "cumulative_commission_currency": self.cumulative_commission_currency,
            "reason_code": self.reason_code,
            "order_revision": self.order_revision,
            "trade_count": self.trade_count,
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupChildOrderStatus:
        if not isinstance(value, Mapping) or set(value) != {
            "intent_id",
            "state",
            "filled_quantity",
            "provider_order_id",
            "average_price",
            "cumulative_commission",
            "cumulative_commission_currency",
            "reason_code",
            "order_revision",
            "trade_count",
        }:
            raise ContractValidationError("invalid stored child status")
        return cls(
            intent_id=value["intent_id"],
            state=ExecutionState(value["state"]),
            filled_quantity=_parse_decimal(value["filled_quantity"], "filled_quantity"),
            provider_order_id=value["provider_order_id"],
            average_price=None
            if value["average_price"] is None
            else _parse_decimal(value["average_price"], "average_price"),
            cumulative_commission=None
            if value["cumulative_commission"] is None
            else _parse_decimal(value["cumulative_commission"], "cumulative_commission"),
            cumulative_commission_currency=value["cumulative_commission_currency"],
            reason_code=value["reason_code"],
            order_revision=value["order_revision"],
            trade_count=value["trade_count"],
        )


@dataclass(frozen=True, slots=True)
class GroupLegPermitReference:
    """Risk reservation identity sent to an account owner for one child.

    The reference deliberately omits the mapper-produced actual Risk intent
    hash. The receiving account owner must map the immutable ``OrderIntent``
    with its trusted mapper and construct the final ``GroupPermitBinding``
    locally before claim or dispatch.
    """

    group_id: str
    leg_id: str
    portfolio_ref: str
    strategy_id: str
    account_ref: str
    coordinator_principal_ref: str
    risk_scope_fingerprint: str
    permit_id: str
    group_fingerprint: str
    policy_fingerprint: str
    reserved_intent_hash: str
    exposure_profile_fingerprint: str
    order_intent_hash: str
    deadline_ns: int

    def __post_init__(self) -> None:
        for name in (
            "group_id",
            "leg_id",
            "portfolio_ref",
            "strategy_id",
            "account_ref",
            "coordinator_principal_ref",
            "permit_id",
        ):
            object.__setattr__(self, name, _reference(getattr(self, name), name))
        for name in (
            "risk_scope_fingerprint",
            "group_fingerprint",
            "policy_fingerprint",
            "reserved_intent_hash",
            "exposure_profile_fingerprint",
            "order_intent_hash",
        ):
            _require_digest(getattr(self, name), name)
        if type(self.deadline_ns) is not int or self.deadline_ns <= 0:
            raise ContractValidationError("invalid group leg deadline")

    def validate_order(self, intent: OrderIntent) -> None:
        if type(intent) is not OrderIntent or (
            intent.intent_id != group_leg_intent_id(self.group_id, self.leg_id)
            or intent.scope.strategy_id != self.strategy_id
            or intent.scope.account_ref != self.account_ref
            or intent.deadline_ns != self.deadline_ns
            or intent.fingerprint != self.order_intent_hash
        ):
            raise ContractValidationError("group permit reference does not match the order intent")

    @property
    def intent_id(self) -> str:
        return group_leg_intent_id(self.group_id, self.leg_id)

    def bind_actual_child(self, actual_child_intent_hash: str) -> GroupPermitBinding:
        """Build the final binding only after the account owner maps the child."""

        _require_digest(actual_child_intent_hash, "actual_child_intent_hash")
        return GroupPermitBinding(
            group_id=self.group_id,
            leg_id=self.leg_id,
            portfolio_ref=self.portfolio_ref,
            strategy_id=self.strategy_id,
            account_ref=self.account_ref,
            coordinator_principal_ref=self.coordinator_principal_ref,
            risk_scope_fingerprint=self.risk_scope_fingerprint,
            permit_id=self.permit_id,
            group_fingerprint=self.group_fingerprint,
            policy_fingerprint=self.policy_fingerprint,
            reserved_intent_hash=self.reserved_intent_hash,
            exposure_profile_fingerprint=self.exposure_profile_fingerprint,
            actual_child_intent_hash=actual_child_intent_hash,
            order_intent_hash=self.order_intent_hash,
            deadline_ns=self.deadline_ns,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "leg_id": self.leg_id,
            "portfolio_ref": self.portfolio_ref,
            "strategy_id": self.strategy_id,
            "account_ref": self.account_ref,
            "coordinator_principal_ref": self.coordinator_principal_ref,
            "risk_scope_fingerprint": self.risk_scope_fingerprint,
            "permit_id": self.permit_id,
            "group_fingerprint": self.group_fingerprint,
            "policy_fingerprint": self.policy_fingerprint,
            "reserved_intent_hash": self.reserved_intent_hash,
            "exposure_profile_fingerprint": self.exposure_profile_fingerprint,
            "order_intent_hash": self.order_intent_hash,
            "deadline_ns": self.deadline_ns,
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupLegPermitReference:
        if not isinstance(value, Mapping) or set(value) != {
            "group_id",
            "leg_id",
            "portfolio_ref",
            "strategy_id",
            "account_ref",
            "coordinator_principal_ref",
            "risk_scope_fingerprint",
            "permit_id",
            "group_fingerprint",
            "policy_fingerprint",
            "reserved_intent_hash",
            "exposure_profile_fingerprint",
            "order_intent_hash",
            "deadline_ns",
        }:
            raise ContractValidationError("invalid group leg permit reference")
        return cls(**value)


@dataclass(frozen=True, slots=True)
class GroupLegClaimReceipt:
    """Account-owner projection of the exact Risk one-shot group claim."""

    permit: GroupLegPermitReference
    actual_child_intent_hash: str
    claim_digest: str
    actual_quantity: Decimal
    actual_notional: Decimal

    def __post_init__(self) -> None:
        if type(self.permit) is not GroupLegPermitReference:
            raise ContractValidationError("group claim requires its permit reference")
        _require_digest(self.actual_child_intent_hash, "actual_child_intent_hash")
        _require_digest(self.claim_digest, "claim_digest")
        object.__setattr__(
            self, "actual_quantity", _positive_decimal(self.actual_quantity, "actual_quantity")
        )
        object.__setattr__(
            self, "actual_notional", _positive_decimal(self.actual_notional, "actual_notional")
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "permit": self.permit.to_payload(),
            "actual_child_intent_hash": self.actual_child_intent_hash,
            "claim_digest": self.claim_digest,
            "actual_quantity": format(self.actual_quantity, "f"),
            "actual_notional": format(self.actual_notional, "f"),
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupLegClaimReceipt:
        if not isinstance(value, Mapping) or set(value) != {
            "permit",
            "actual_child_intent_hash",
            "claim_digest",
            "actual_quantity",
            "actual_notional",
        }:
            raise ContractValidationError("invalid group leg claim receipt")
        return cls(
            permit=GroupLegPermitReference.from_payload(value["permit"]),
            actual_child_intent_hash=value["actual_child_intent_hash"],
            claim_digest=value["claim_digest"],
            actual_quantity=_parse_decimal(value["actual_quantity"], "actual_quantity"),
            actual_notional=_parse_decimal(value["actual_notional"], "actual_notional"),
        )


@dataclass(frozen=True, slots=True)
class GroupDispatchResolutionReceipt:
    """Exact Risk result for terminal reconciliation of one claimed child."""

    claim: GroupLegClaimReceipt
    resolution_kind: str
    proof_sha256: str
    final_state: ExecutionState
    requested_quantity: Decimal
    filled_quantity: Decimal
    trade_count: int
    order_revision: int | None
    journal_revision: int
    exposure_proof_sha256: str | None
    exposure_reservation_id: str | None
    exposure_reservation_sha256: str | None
    dispatch_latch_active: bool
    has_other_active_freezes: bool

    def __post_init__(self) -> None:
        if type(self.claim) is not GroupLegClaimReceipt:
            raise ContractValidationError("dispatch resolution requires the exact claim")
        _require_digest(self.proof_sha256, "proof_sha256")
        if type(self.resolution_kind) is not str or not re.fullmatch(
            r"[A-Z][A-Z0-9_]{0,63}", self.resolution_kind
        ):
            raise ContractValidationError("invalid dispatch resolution kind")
        if type(self.final_state) is not ExecutionState or self.final_state not in TERMINAL_STATES:
            raise ContractValidationError("dispatch resolution requires a terminal final state")
        object.__setattr__(
            self,
            "requested_quantity",
            _positive_decimal(self.requested_quantity, "requested_quantity"),
        )
        if type(self.filled_quantity) is not Decimal or not self.filled_quantity.is_finite():
            raise ContractValidationError("invalid resolved filled quantity")
        if self.filled_quantity < 0 or self.filled_quantity > self.requested_quantity:
            raise ContractValidationError("invalid resolved filled quantity")
        if type(self.trade_count) is not int or self.trade_count < 0:
            raise ContractValidationError("invalid resolved trade count")
        if type(self.journal_revision) is not int or self.journal_revision <= 0:
            raise ContractValidationError("invalid resolved journal revision")
        if self.order_revision is not None and (
            type(self.order_revision) is not int or self.order_revision <= 0
        ):
            raise ContractValidationError("invalid resolved order revision")
        if (
            type(self.dispatch_latch_active) is not bool
            or type(self.has_other_active_freezes) is not bool
        ):
            raise ContractValidationError("invalid dispatch resolution freeze state")
        for name in (
            "exposure_proof_sha256",
            "exposure_reservation_sha256",
        ):
            value = getattr(self, name)
            if value is not None:
                _require_digest(value, name)
        if self.exposure_reservation_id is not None:
            object.__setattr__(
                self,
                "exposure_reservation_id",
                _reference(self.exposure_reservation_id, "exposure_reservation_id"),
            )
        if self.dispatch_latch_active:
            raise ContractValidationError("resolved dispatch latch must be inactive")
        if self.resolution_kind == "TERMINAL_EXPOSURE_RESOLVED":
            if (
                self.final_state not in {ExecutionState.FILLED, ExecutionState.CANCELLED}
                or self.filled_quantity <= 0
                or self.trade_count <= 0
                or self.order_revision is None
                or self.exposure_proof_sha256 is None
                or self.exposure_reservation_id is None
                or self.exposure_reservation_sha256 is None
            ):
                raise ContractValidationError("terminal exposure receipt is incomplete")
        elif self.resolution_kind == "TERMINAL_NO_FILL_RESOLVED":
            if (
                self.final_state
                not in {ExecutionState.CANCELLED, ExecutionState.REJECTED, ExecutionState.BLOCKED}
                or self.filled_quantity != 0
                or self.trade_count != 0
                or self.exposure_proof_sha256 is not None
                or self.exposure_reservation_id is not None
                or self.exposure_reservation_sha256 is not None
            ):
                raise ContractValidationError("terminal no-fill receipt is inconsistent")
        else:
            raise ContractValidationError("unsupported group dispatch resolution kind")

    def to_payload(self) -> dict[str, Any]:
        return {
            "claim": self.claim.to_payload(),
            "resolution_kind": self.resolution_kind,
            "proof_sha256": self.proof_sha256,
            "final_state": self.final_state.value,
            "requested_quantity": format(self.requested_quantity, "f"),
            "filled_quantity": format(self.filled_quantity, "f"),
            "trade_count": self.trade_count,
            "order_revision": self.order_revision,
            "journal_revision": self.journal_revision,
            "exposure_proof_sha256": self.exposure_proof_sha256,
            "exposure_reservation_id": self.exposure_reservation_id,
            "exposure_reservation_sha256": self.exposure_reservation_sha256,
            "dispatch_latch_active": self.dispatch_latch_active,
            "has_other_active_freezes": self.has_other_active_freezes,
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupDispatchResolutionReceipt:
        if not isinstance(value, Mapping) or set(value) != {
            "claim",
            "resolution_kind",
            "proof_sha256",
            "final_state",
            "requested_quantity",
            "filled_quantity",
            "trade_count",
            "order_revision",
            "journal_revision",
            "exposure_proof_sha256",
            "exposure_reservation_id",
            "exposure_reservation_sha256",
            "dispatch_latch_active",
            "has_other_active_freezes",
        }:
            raise ContractValidationError("invalid group dispatch resolution receipt")
        return cls(
            claim=GroupLegClaimReceipt.from_payload(value["claim"]),
            resolution_kind=value["resolution_kind"],
            proof_sha256=value["proof_sha256"],
            final_state=ExecutionState(value["final_state"]),
            requested_quantity=_parse_decimal(value["requested_quantity"], "requested_quantity"),
            filled_quantity=_parse_decimal(value["filled_quantity"], "filled_quantity"),
            trade_count=value["trade_count"],
            order_revision=value["order_revision"],
            journal_revision=value["journal_revision"],
            exposure_proof_sha256=value["exposure_proof_sha256"],
            exposure_reservation_id=value["exposure_reservation_id"],
            exposure_reservation_sha256=value["exposure_reservation_sha256"],
            dispatch_latch_active=value["dispatch_latch_active"],
            has_other_active_freezes=value["has_other_active_freezes"],
        )


@dataclass(frozen=True, slots=True)
class GroupLegOrderReceipt:
    """Account-owner query/submit projection from its existing orders journal."""

    status: GroupChildOrderStatus
    claim: GroupLegClaimReceipt | None = None
    resolution: GroupDispatchResolutionReceipt | None = None

    def __post_init__(self) -> None:
        if type(self.status) is not GroupChildOrderStatus:
            raise ContractValidationError("group order receipt requires a child status")
        if self.claim is not None and type(self.claim) is not GroupLegClaimReceipt:
            raise ContractValidationError("invalid group order claim receipt")
        if (
            self.resolution is not None
            and type(self.resolution) is not GroupDispatchResolutionReceipt
        ):
            raise ContractValidationError("invalid group order resolution receipt")
        if self.claim is not None and self.claim.permit.intent_id != self.status.intent_id:
            raise ContractValidationError("group order claim does not match child status")
        if self.resolution is not None and (
            self.claim is None or self.resolution.claim != self.claim
        ):
            raise ContractValidationError("group order resolution does not match claim")

    def to_payload(self) -> dict[str, Any]:
        return {
            "status": self.status.to_payload(),
            "claim": None if self.claim is None else self.claim.to_payload(),
            "resolution": None if self.resolution is None else self.resolution.to_payload(),
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupLegOrderReceipt:
        if not isinstance(value, Mapping) or set(value) != {"status", "claim", "resolution"}:
            raise ContractValidationError("invalid group order receipt")
        return cls(
            status=GroupChildOrderStatus.from_payload(value["status"]),
            claim=None
            if value["claim"] is None
            else GroupLegClaimReceipt.from_payload(value["claim"]),
            resolution=None
            if value["resolution"] is None
            else GroupDispatchResolutionReceipt.from_payload(value["resolution"]),
        )


@dataclass(frozen=True, slots=True)
class GroupLegProgress:
    leg_id: str
    phase: GroupLegPhase = GroupLegPhase.READY
    submitted_intent: OrderIntent | None = None
    child_status: GroupChildOrderStatus | None = None
    claim_receipt: GroupLegClaimReceipt | None = None
    resolution_receipt: GroupDispatchResolutionReceipt | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "leg_id", _reference(self.leg_id, "leg_id"))
        if type(self.phase) is not GroupLegPhase:
            object.__setattr__(self, "phase", GroupLegPhase(self.phase))
        if self.submitted_intent is not None and type(self.submitted_intent) is not OrderIntent:
            raise ContractValidationError("invalid submitted group leg intent")
        if self.child_status is not None and type(self.child_status) is not GroupChildOrderStatus:
            raise ContractValidationError("invalid group child status")
        if self.child_status is not None and (
            self.submitted_intent is None
            or self.child_status.intent_id != self.submitted_intent.intent_id
        ):
            raise ContractValidationError("group child status does not match submitted intent")
        if self.claim_receipt is not None and (
            self.submitted_intent is None
            or self.claim_receipt.permit.order_intent_hash != self.submitted_intent.fingerprint
        ):
            raise ContractValidationError("group claim receipt does not match submitted intent")
        if self.resolution_receipt is not None and (
            self.claim_receipt is None or self.resolution_receipt.claim != self.claim_receipt
        ):
            raise ContractValidationError("group resolution receipt does not match the claim")

    def to_payload(self) -> dict[str, Any]:
        return {
            "leg_id": self.leg_id,
            "phase": self.phase.value,
            "submitted_intent": None
            if self.submitted_intent is None
            else self.submitted_intent.to_payload(),
            "child_status": None if self.child_status is None else self.child_status.to_payload(),
            "claim_receipt": None
            if self.claim_receipt is None
            else self.claim_receipt.to_payload(),
            "resolution_receipt": None
            if self.resolution_receipt is None
            else self.resolution_receipt.to_payload(),
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupLegProgress:
        if not isinstance(value, Mapping) or set(value) != {
            "leg_id",
            "phase",
            "submitted_intent",
            "child_status",
            "claim_receipt",
            "resolution_receipt",
        }:
            raise ContractValidationError("invalid stored group leg progress")
        return cls(
            leg_id=value["leg_id"],
            phase=GroupLegPhase(value["phase"]),
            submitted_intent=None
            if value["submitted_intent"] is None
            else order_intent_from_payload(value["submitted_intent"]),
            child_status=None
            if value["child_status"] is None
            else GroupChildOrderStatus.from_payload(value["child_status"]),
            claim_receipt=None
            if value["claim_receipt"] is None
            else GroupLegClaimReceipt.from_payload(value["claim_receipt"]),
            resolution_receipt=None
            if value["resolution_receipt"] is None
            else GroupDispatchResolutionReceipt.from_payload(value["resolution_receipt"]),
        )


@dataclass(frozen=True, slots=True)
class GroupOrderSnapshot:
    request: GroupOrderIntent
    state: GroupOrderState
    revision: int
    reservations: tuple[GroupAccountReservationSnapshot, ...]
    legs: tuple[GroupLegProgress, ...]
    residual_base_units: Decimal | None
    error_code: str | None
    created_at_ns: int
    updated_at_ns: int
    budget_reservation: GroupBudgetReservation | None = None
    residual_vector: tuple[Decimal, ...] | None = None

    def __post_init__(self) -> None:
        if type(self.request) is not GroupOrderIntent:
            raise ContractValidationError("invalid group order request")
        if type(self.state) is not GroupOrderState:
            object.__setattr__(self, "state", GroupOrderState(self.state))
        if type(self.revision) is not int or self.revision < 0:
            raise ContractValidationError("invalid group revision")
        if type(self.reservations) is not tuple or any(
            type(value) is not GroupAccountReservationSnapshot for value in self.reservations
        ):
            raise ContractValidationError("invalid group reservation snapshot")
        if type(self.legs) is not tuple or any(
            type(value) is not GroupLegProgress for value in self.legs
        ):
            raise ContractValidationError("invalid group leg snapshot")
        if tuple(item.account_ref for item in self.reservations) != self.request.account_refs:
            raise ContractValidationError("group reservation accounts are inconsistent")
        if tuple(item.leg_id for item in self.legs) != tuple(
            leg.leg_id for leg in self.request.legs
        ):
            raise ContractValidationError("group leg progress is inconsistent")
        if self.residual_base_units is not None and (
            type(self.residual_base_units) is not Decimal
            or not self.residual_base_units.is_finite()
        ):
            raise ContractValidationError("invalid group residual")
        if self.residual_vector is not None:
            if (
                type(self.residual_vector) is not tuple
                or len(self.residual_vector) != len(self.request.legs)
                or any(
                    type(item) is not Decimal or not item.is_finite()
                    for item in self.residual_vector
                )
            ):
                raise ContractValidationError("invalid group residual vector")
            if self.request.version != 3:
                raise ContractValidationError(
                    "only a version-3 group can carry a residual vector"
                )
        if self.error_code is not None:
            object.__setattr__(self, "error_code", _reference(self.error_code, "error_code"))
        for name in ("created_at_ns", "updated_at_ns"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ContractValidationError(f"invalid {name}")
        if self.request.version == 1:
            if self.budget_reservation is not None:
                raise ContractValidationError("legacy group snapshot cannot carry a budget hold")
        else:
            reservation = self.budget_reservation
            quote = self.request.budget_quote
            if (
                type(reservation) is not GroupBudgetReservation
                or type(quote) is not GroupBudgetQuote
            ):
                raise ContractValidationError("version-2 group snapshot requires a budget receipt")
            if (
                reservation.policy_fingerprint != quote.policy_fingerprint
                or reservation.policy.portfolio_ref != self.request.portfolio_ref
                or reservation.policy.valuation_unit != quote.valuation_unit
                or reservation.quote_fingerprint != quote.fingerprint
                or reservation.valuation_unit != quote.valuation_unit
                or reservation.gross_notional != quote.gross_notional
                or reservation.deadline_ns != self.request.deadline_ns
            ):
                raise ContractValidationError("stored group budget receipt does not match request")
            if self.state is GroupOrderState.PAIRED and (
                reservation.state is not GroupBudgetReservationState.HELD
            ):
                raise ContractValidationError("paired group must retain its gross budget")
            if self.state is GroupOrderState.CLOSED and (
                reservation.state is not GroupBudgetReservationState.RELEASED
            ):
                raise ContractValidationError("closed group must have released budget evidence")
            if self.state is GroupOrderState.REJECTED_BEFORE_DISPATCH and reservation.state not in {
                GroupBudgetReservationState.DENIED,
                GroupBudgetReservationState.RELEASED,
            }:
                raise ContractValidationError("rejected group has an active budget hold")
            if self.state not in {
                GroupOrderState.CLOSED,
                GroupOrderState.REJECTED_BEFORE_DISPATCH,
            } and reservation.state in {
                GroupBudgetReservationState.RELEASED,
                GroupBudgetReservationState.DENIED,
            }:
                raise ContractValidationError("active group does not have an active budget hold")

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "request": self.request.to_payload(),
            "state": self.state.value,
            "revision": self.revision,
            "reservations": [item.to_payload() for item in self.reservations],
            "legs": [item.to_payload() for item in self.legs],
            "residual_base_units": None
            if self.residual_base_units is None
            else format(self.residual_base_units, "f"),
            "error_code": self.error_code,
            "created_at_ns": self.created_at_ns,
            "updated_at_ns": self.updated_at_ns,
        }
        if self.request.version != 1:
            assert self.budget_reservation is not None
            payload["budget_reservation"] = self.budget_reservation.to_payload()
        if self.request.version == 3:
            payload["residual_vector"] = (
                None
                if self.residual_vector is None
                else [format(item, "f") for item in self.residual_vector]
            )
        return payload

    @classmethod
    def from_payload(cls, value: object) -> GroupOrderSnapshot:
        legacy_keys = {
            "request",
            "state",
            "revision",
            "reservations",
            "legs",
            "residual_base_units",
            "error_code",
            "created_at_ns",
            "updated_at_ns",
        }
        current_keys = legacy_keys | {"budget_reservation"}
        vector_keys = current_keys | {"residual_vector"}
        if not isinstance(value, Mapping) or frozenset(value) not in {
            frozenset(legacy_keys),
            frozenset(current_keys),
            frozenset(vector_keys),
        }:
            raise ContractValidationError("invalid group snapshot payload")
        if not isinstance(value["reservations"], list) or not isinstance(value["legs"], list):
            raise ContractValidationError("invalid group snapshot collections")
        residual_vector = value.get("residual_vector")
        if residual_vector is not None and not isinstance(residual_vector, list):
            raise ContractValidationError("invalid group residual vector payload")
        return cls(
            request=GroupOrderIntent.from_payload(value["request"]),
            state=GroupOrderState(value["state"]),
            revision=value["revision"],
            reservations=tuple(
                GroupAccountReservationSnapshot.from_payload(item) for item in value["reservations"]
            ),
            legs=tuple(GroupLegProgress.from_payload(item) for item in value["legs"]),
            residual_base_units=None
            if value["residual_base_units"] is None
            else _parse_decimal(value["residual_base_units"], "residual_base_units"),
            error_code=value["error_code"],
            created_at_ns=value["created_at_ns"],
            updated_at_ns=value["updated_at_ns"],
            budget_reservation=None
            if "budget_reservation" not in value
            else GroupBudgetReservation.from_payload(value["budget_reservation"]),
            residual_vector=None
            if residual_vector is None
            else tuple(
                _parse_decimal(item, "residual_vector") for item in residual_vector
            ),
        )


@dataclass(frozen=True, slots=True)
class GroupOrderEvent:
    sequence: int
    group_id: str
    event_type: str
    created_at_ns: int
    payload: Mapping[str, Any]


def _require_digest(value: object, name: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ContractValidationError(f"invalid {name}")


def _parse_decimal(value: object, name: str) -> Decimal:
    if type(value) is not str or not _DECIMAL.fullmatch(value):
        raise ContractValidationError(f"invalid {name}")
    try:
        result = Decimal(value)
    except Exception as error:
        raise ContractValidationError(f"invalid {name}") from error
    if not result.is_finite():
        raise ContractValidationError(f"invalid {name}")
    return result


__all__ = [
    "GroupAccountReservationSnapshot",
    "GroupBudgetLegQuote",
    "GroupBudgetQuote",
    "GroupBudgetReservation",
    "GroupBudgetReservationState",
    "GroupChildOrderStatus",
    "GroupDispatchResolutionReceipt",
    "GroupLegClaimReceipt",
    "GroupLegPhase",
    "GroupLegOrderReceipt",
    "GroupLegPermitReference",
    "GroupLegProgress",
    "GroupOrderEvent",
    "GroupOrderIntent",
    "GroupOrderLegPlan",
    "GroupOrderSnapshot",
    "GroupOrderState",
    "PortfolioGroupBudgetPolicy",
    "GroupReservationPhase",
    "GroupRiskLegSnapshot",
]
