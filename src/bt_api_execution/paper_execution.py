"""Code-owned spot paper execution contracts and deterministic top-of-book model.

The module contains no client-controlled account setup and no live-provider
fallback.  The store owns durable match receipts; this module only validates
the immutable paper-domain configuration and builds a proposal from a trusted
quote leaf.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from time import time_ns
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Mapping

from bt_api_base.service_protocol import WireScope

from .basic_admission import (
    ExecutionAdmissionAuthorizationV1,
    ExecutionProviderOrderParametersV1,
)
from .contracts import (
    CancelIntent,
    CancelObservation,
    ExecutionScope,
    ExecutionState,
    OrderIntent,
    OrderType,
    PositionEffect,
    ProviderObservation,
    Side,
    TimeInForce,
    payload_sha256,
)
from .errors import ContractValidationError

PAPER_PROVIDER = "paper"
PAPER_ENVIRONMENT = "paper"
PAPER_MODEL_REF = "spot_market_top_of_book"
PAPER_MODEL_VERSION = "1"
PAPER_FEE_MODEL_REF = "quote_notional_rate"
PAPER_FEE_MODEL_VERSION = "1"
PAPER_MATCH_RECEIPT_SCHEMA_V2 = "managed_paper_fill.v2"
PAPER_MATCH_TIME_SOURCE_V2 = "MANAGED_PAPER_MATCH"

_PAPER_REFUSALS = frozenset(
    {
        "paper_instrument_unsupported",
        "paper_order_type_unsupported",
        "paper_position_effect_unsupported",
        "paper_quote_unavailable",
        "paper_quote_invalid",
        "paper_quote_stale",
        "paper_quote_cursor_regressed",
        "paper_quote_cursor_conflict",
        "paper_quote_cursor_reused",
        "paper_time_in_force_unsupported",
        "paper_top_of_book_depth_insufficient",
        "paper_order_quantity_invalid",
        "paper_instrument_price_invalid",
        "paper_admission_expired",
        "paper_insufficient_cash",
        "paper_insufficient_inventory",
    }
)


def _text(value: object, name: str, *, max_length: int = 256) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > max_length
        or not value.isascii()
        or any(ord(char) < 0x21 for char in value)
    ):
        raise ContractValidationError(f"invalid paper {name}")
    return value


def _decimal(value: object, name: str, *, positive: bool = False) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise ContractValidationError(f"invalid paper {name}")
    if positive and value <= 0:
        raise ContractValidationError(f"invalid paper {name}")
    return value


@dataclass(frozen=True, slots=True)
class PaperInstrumentV1:
    """Trusted, versioned spot instrument units used by the paper matcher."""

    instrument: str
    metadata_version: str
    base_currency: str
    quote_currency: str
    quantity_step: Decimal
    price_tick: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument", _text(self.instrument, "instrument"))
        object.__setattr__(
            self, "metadata_version", _text(self.metadata_version, "metadata version")
        )
        object.__setattr__(self, "base_currency", _text(self.base_currency, "base currency"))
        object.__setattr__(self, "quote_currency", _text(self.quote_currency, "quote currency"))
        object.__setattr__(
            self, "quantity_step", _decimal(self.quantity_step, "quantity step", positive=True)
        )
        object.__setattr__(
            self, "price_tick", _decimal(self.price_tick, "price tick", positive=True)
        )

    def to_payload(self) -> dict[str, str]:
        return {
            "instrument": self.instrument,
            "metadata_version": self.metadata_version,
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
            "quantity_step": format(self.quantity_step, "f"),
            "price_tick": format(self.price_tick, "f"),
        }


@dataclass(frozen=True, slots=True)
class PaperOpeningPositionV1:
    """Explicit opening spot inventory and quote-currency average cost."""

    instrument: str
    quantity: Decimal
    average_cost: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument", _text(self.instrument, "opening instrument"))
        object.__setattr__(
            self, "quantity", _decimal(self.quantity, "opening quantity", positive=True)
        )
        object.__setattr__(
            self, "average_cost", _decimal(self.average_cost, "opening average cost", positive=True)
        )

    def to_payload(self) -> dict[str, str]:
        return {
            "instrument": self.instrument,
            "quantity": format(self.quantity, "f"),
            "average_cost": format(self.average_cost, "f"),
        }


@dataclass(frozen=True, slots=True)
class PaperOwnerDomainV1:
    """Immutable, code-owned identity and explicit opening state for one paper owner.

    This first model supports one strategy book, spot instruments sharing one
    quote currency, and immediate top-of-book MARKET fills.  `initial_cash`,
    `initial_fees`, and `opening_positions` are required configuration facts;
    no value is inferred from a budget or account summary.
    """

    execution_domain_ref: str
    simulation_account_ref: str
    experiment_ref: str
    strategy_ref: str
    model_ref: str
    model_version: str
    quote_source_ref: str
    cash_currency: str
    initial_cash: Decimal
    initial_fees: Decimal
    fee_rate: Decimal
    fee_quantum: Decimal
    max_quote_age_ns: int
    instruments: tuple[PaperInstrumentV1, ...]
    opening_positions: tuple[PaperOpeningPositionV1, ...]

    def __post_init__(self) -> None:
        for name in (
            "execution_domain_ref",
            "simulation_account_ref",
            "experiment_ref",
            "strategy_ref",
            "model_ref",
            "model_version",
            "quote_source_ref",
            "cash_currency",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name.replace("_", " ")))
        if self.model_ref != PAPER_MODEL_REF or self.model_version != PAPER_MODEL_VERSION:
            raise ContractValidationError("unsupported paper matching model")
        if not self.model_version:
            raise ContractValidationError("paper model version is required")
        object.__setattr__(self, "initial_cash", _decimal(self.initial_cash, "initial cash"))
        if self.initial_cash < 0:
            raise ContractValidationError("paper initial cash cannot be negative")
        object.__setattr__(self, "initial_fees", _decimal(self.initial_fees, "initial fees"))
        if self.initial_fees < 0:
            raise ContractValidationError("paper initial fees cannot be negative")
        object.__setattr__(self, "fee_rate", _decimal(self.fee_rate, "fee rate"))
        if self.fee_rate < 0 or self.fee_rate >= 1:
            raise ContractValidationError("paper fee rate is outside the supported range")
        object.__setattr__(
            self, "fee_quantum", _decimal(self.fee_quantum, "fee quantum", positive=True)
        )
        if type(self.max_quote_age_ns) is not int or self.max_quote_age_ns <= 0:
            raise ContractValidationError("paper quote age limit must be positive")
        instruments = tuple(self.instruments)
        positions = tuple(self.opening_positions)
        if not instruments or any(type(item) is not PaperInstrumentV1 for item in instruments):
            raise ContractValidationError("paper domain requires trusted spot instruments")
        if any(type(item) is not PaperOpeningPositionV1 for item in positions):
            raise ContractValidationError("paper opening positions are invalid")
        if len({item.instrument for item in instruments}) != len(instruments):
            raise ContractValidationError("paper instrument is duplicated")
        if len({item.instrument for item in positions}) != len(positions):
            raise ContractValidationError("paper opening position is duplicated")
        configured = {item.instrument: item for item in instruments}
        for spec in instruments:
            if (
                spec.quote_currency != self.cash_currency
                or spec.base_currency == spec.quote_currency
            ):
                raise ContractValidationError(
                    "paper spot quote currency must match cash and differ from base"
                )
        for position in positions:
            spec = configured.get(position.instrument)
            if spec is None or position.quantity % spec.quantity_step != 0:
                raise ContractValidationError("paper opening position has no instrument")
        object.__setattr__(self, "instruments", instruments)
        object.__setattr__(self, "opening_positions", positions)

    @property
    def fingerprint(self) -> str:
        return payload_sha256(self.to_payload())

    def instrument(self, instrument: str) -> PaperInstrumentV1 | None:
        return next((item for item in self.instruments if item.instrument == instrument), None)

    def validate_binding(self, wire_scope: WireScope, execution_scope: ExecutionScope) -> None:
        if type(wire_scope) is not WireScope or type(execution_scope) is not ExecutionScope:
            raise ContractValidationError("paper owner requires typed wire and execution scopes")
        if (
            wire_scope.execution_domain_ref != self.execution_domain_ref
            or wire_scope.account_ref != self.simulation_account_ref
            or wire_scope.account_refs
            or wire_scope.strategy_ref != self.strategy_ref
            or wire_scope.manual_book_ref is not None
            or wire_scope.portfolio_ref is not None
            or wire_scope.group_id is not None
            or wire_scope.leg_id is not None
            or execution_scope.provider != PAPER_PROVIDER
            or execution_scope.environment != PAPER_ENVIRONMENT
            or execution_scope.account_ref != self.simulation_account_ref
            or execution_scope.strategy_id != self.strategy_ref
        ):
            raise ContractValidationError("paper owner domain does not match trusted scope mapping")

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "execution_domain_ref": self.execution_domain_ref,
            "simulation_account_ref": self.simulation_account_ref,
            "experiment_ref": self.experiment_ref,
            "strategy_ref": self.strategy_ref,
            "model_ref": self.model_ref,
            "model_version": self.model_version,
            "quote_source_ref": self.quote_source_ref,
            "cash_currency": self.cash_currency,
            "initial_cash": format(self.initial_cash, "f"),
            "initial_fees": format(self.initial_fees, "f"),
            "fee_model_ref": PAPER_FEE_MODEL_REF,
            "fee_model_version": PAPER_FEE_MODEL_VERSION,
            "fee_rate": format(self.fee_rate, "f"),
            "fee_quantum": format(self.fee_quantum, "f"),
            "max_quote_age_ns": self.max_quote_age_ns,
            "instruments": [item.to_payload() for item in self.instruments],
            "opening_positions": [item.to_payload() for item in self.opening_positions],
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> PaperOwnerDomainV1:
        expected = {
            "schema_version",
            "execution_domain_ref",
            "simulation_account_ref",
            "experiment_ref",
            "strategy_ref",
            "model_ref",
            "model_version",
            "quote_source_ref",
            "cash_currency",
            "initial_cash",
            "initial_fees",
            "fee_model_ref",
            "fee_model_version",
            "fee_rate",
            "fee_quantum",
            "max_quote_age_ns",
            "instruments",
            "opening_positions",
        }
        if type(payload) is not dict or set(payload) != expected:
            raise ContractValidationError("stored paper owner domain has an invalid shape")
        if (
            type(payload.get("schema_version")) is not int
            or payload.get("schema_version") != 1
            or payload.get("fee_model_ref") != PAPER_FEE_MODEL_REF
            or payload.get("fee_model_version") != PAPER_FEE_MODEL_VERSION
        ):
            raise ContractValidationError("stored paper owner model version is unsupported")
        instrument_rows = payload.get("instruments")
        position_rows = payload.get("opening_positions")
        if type(instrument_rows) is not list or type(position_rows) is not list:
            raise ContractValidationError("stored paper owner rows are invalid")
        instruments = tuple(
            PaperInstrumentV1(
                instrument=row["instrument"],
                metadata_version=row["metadata_version"],
                base_currency=row["base_currency"],
                quote_currency=row["quote_currency"],
                quantity_step=Decimal(row["quantity_step"]),
                price_tick=Decimal(row["price_tick"]),
            )
            for row in instrument_rows
            if type(row) is dict
            and set(row)
            == {
                "instrument",
                "metadata_version",
                "base_currency",
                "quote_currency",
                "quantity_step",
                "price_tick",
            }
        )
        positions = tuple(
            PaperOpeningPositionV1(
                instrument=row["instrument"],
                quantity=Decimal(row["quantity"]),
                average_cost=Decimal(row["average_cost"]),
            )
            for row in position_rows
            if type(row) is dict and set(row) == {"instrument", "quantity", "average_cost"}
        )
        if len(instruments) != len(instrument_rows) or len(positions) != len(position_rows):
            raise ContractValidationError("stored paper owner rows have an invalid shape")
        return cls(
            execution_domain_ref=payload["execution_domain_ref"],
            simulation_account_ref=payload["simulation_account_ref"],
            experiment_ref=payload["experiment_ref"],
            strategy_ref=payload["strategy_ref"],
            model_ref=payload["model_ref"],
            model_version=payload["model_version"],
            quote_source_ref=payload["quote_source_ref"],
            cash_currency=payload["cash_currency"],
            initial_cash=Decimal(payload["initial_cash"]),
            initial_fees=Decimal(payload["initial_fees"]),
            fee_rate=Decimal(payload["fee_rate"]),
            fee_quantum=Decimal(payload["fee_quantum"]),
            max_quote_age_ns=payload["max_quote_age_ns"],
            instruments=instruments,
            opening_positions=positions,
        )


@dataclass(frozen=True, slots=True)
class PaperQuoteV1:
    """One trusted, source-versioned best-bid/best-ask observation."""

    source_ref: str
    generation: str
    cursor: int
    instrument: str
    metadata_version: str
    quote_currency: str
    bid: Decimal
    ask: Decimal
    bid_quantity: Decimal
    ask_quantity: Decimal
    as_of_ns: int

    def __post_init__(self) -> None:
        for name in (
            "source_ref",
            "generation",
            "instrument",
            "metadata_version",
            "quote_currency",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name.replace("_", " ")))
        if type(self.cursor) is not int or self.cursor <= 0:
            raise ContractValidationError("invalid paper quote cursor")
        object.__setattr__(self, "bid", _decimal(self.bid, "bid", positive=True))
        object.__setattr__(self, "ask", _decimal(self.ask, "ask", positive=True))
        object.__setattr__(
            self, "bid_quantity", _decimal(self.bid_quantity, "bid quantity", positive=True)
        )
        object.__setattr__(
            self, "ask_quantity", _decimal(self.ask_quantity, "ask quantity", positive=True)
        )
        if self.ask < self.bid:
            raise ContractValidationError("paper ask is below bid")
        if type(self.as_of_ns) is not int or self.as_of_ns <= 0:
            raise ContractValidationError("invalid paper quote time")

    def to_payload(self) -> dict[str, object]:
        return {
            "source_ref": self.source_ref,
            "generation": self.generation,
            "cursor": self.cursor,
            "instrument": self.instrument,
            "metadata_version": self.metadata_version,
            "quote_currency": self.quote_currency,
            "bid": format(self.bid, "f"),
            "ask": format(self.ask, "f"),
            "bid_quantity": format(self.bid_quantity, "f"),
            "ask_quantity": format(self.ask_quantity, "f"),
            "as_of_ns": self.as_of_ns,
        }


class PaperQuoteSource(Protocol):
    """Trusted read-only source for current versioned spot quotes."""

    def read_quote(self, instrument: str) -> PaperQuoteV1 | None: ...


@dataclass(frozen=True, slots=True)
class PaperDispatchResultV1:
    """In-process proposal; the SQLite store decides and commits its outcome."""

    intent_id: str
    intent_fingerprint: str
    quote: PaperQuoteV1 | None
    refusal_code: str | None
    authorization: ExecutionAdmissionAuthorizationV1 | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "intent_id", _text(self.intent_id, "intent id"))
        fingerprint = _text(self.intent_fingerprint, "intent fingerprint", max_length=64)
        if len(fingerprint) != 64 or any(char not in "0123456789abcdef" for char in fingerprint):
            raise ContractValidationError("invalid paper intent fingerprint")
        if self.quote is not None and type(self.quote) is not PaperQuoteV1:
            raise ContractValidationError("invalid paper quote result")
        if self.refusal_code is not None and self.refusal_code not in _PAPER_REFUSALS:
            raise ContractValidationError("invalid paper refusal code")
        if (self.quote is None) == (self.refusal_code is None):
            raise ContractValidationError("paper result requires one quote or refusal")
        if (
            self.authorization is not None
            and type(self.authorization) is not ExecutionAdmissionAuthorizationV1
        ):
            raise ContractValidationError("invalid paper admission authorization")


@dataclass(frozen=True, slots=True)
class PaperEconomicStateV1:
    cash: Decimal
    cumulative_fees: Decimal
    positions: tuple[PaperPositionBalanceV1, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "cash", _decimal(self.cash, "cash"))
        object.__setattr__(
            self, "cumulative_fees", _decimal(self.cumulative_fees, "cumulative fees")
        )
        rows = tuple(self.positions)
        if any(type(item) is not PaperPositionBalanceV1 for item in rows):
            raise ContractValidationError("paper positions are invalid")
        if len({item.instrument for item in rows}) != len(rows):
            raise ContractValidationError("paper positions contain a duplicate instrument")
        if any(item.quantity <= 0 or item.average_cost <= 0 for item in rows):
            raise ContractValidationError("paper position values must be positive")
        object.__setattr__(self, "positions", tuple(sorted(rows, key=lambda item: item.instrument)))


@dataclass(frozen=True, slots=True)
class PaperMatchDecisionV1:
    observation: ProviderObservation
    quote: PaperQuoteV1 | None
    evaluated_at_ns: int
    instrument: str
    side: Side
    filled_quantity: Decimal
    fill_price: Decimal | None
    fee_amount: Decimal
    cash_delta: Decimal
    position_delta: Decimal
    reason_code: str | None


def initial_paper_state(domain: PaperOwnerDomainV1) -> PaperEconomicStateV1:
    if type(domain) is not PaperOwnerDomainV1:
        raise ContractValidationError("paper owner domain is required")
    return PaperEconomicStateV1(
        cash=domain.initial_cash,
        cumulative_fees=domain.initial_fees,
        positions=tuple(
            PaperPositionBalanceV1(item.instrument, item.quantity, item.average_cost)
            for item in sorted(domain.opening_positions, key=lambda row: row.instrument)
        ),
    )


def paper_state_digest(domain: PaperOwnerDomainV1, state: PaperEconomicStateV1) -> str:
    if type(domain) is not PaperOwnerDomainV1 or type(state) is not PaperEconomicStateV1:
        raise ContractValidationError("paper state digest inputs are invalid")
    return payload_sha256(
        {
            "domain_sha256": domain.fingerprint,
            "cash_currency": domain.cash_currency,
            "cash": format(state.cash, "f"),
            "cumulative_fees": format(state.cumulative_fees, "f"),
            "positions": [
                {
                    "instrument": row.instrument,
                    "quantity": format(row.quantity, "f"),
                    "average_cost": format(row.average_cost, "f"),
                }
                for row in state.positions
            ],
        }
    )


def evaluate_paper_match(
    domain: PaperOwnerDomainV1,
    intent: OrderIntent,
    proposal: PaperDispatchResultV1,
    state: PaperEconomicStateV1,
    *,
    now_ns: int,
    cursor_refusal: str | None = None,
    authorization_expired: bool = False,
) -> PaperMatchDecisionV1:
    """Evaluate one market order against the exact trusted quote and current book."""

    if (
        type(domain) is not PaperOwnerDomainV1
        or type(intent) is not OrderIntent
        or type(proposal) is not PaperDispatchResultV1
        or type(state) is not PaperEconomicStateV1
        or type(now_ns) is not int
        or now_ns <= 0
    ):
        raise ContractValidationError("paper match inputs are invalid")
    if proposal.intent_id != intent.intent_id or proposal.intent_fingerprint != intent.fingerprint:
        raise ContractValidationError("paper proposal does not bind the durable order")
    if cursor_refusal is not None and cursor_refusal not in {
        "paper_quote_cursor_regressed",
        "paper_quote_cursor_conflict",
        "paper_quote_cursor_reused",
    }:
        raise ContractValidationError("paper quote cursor refusal is invalid")

    reason = proposal.refusal_code
    quote = proposal.quote
    spec = domain.instrument(intent.instrument)
    if cursor_refusal is not None:
        reason = cursor_refusal
    elif authorization_expired:
        reason = "paper_admission_expired"
    elif quote is not None:
        if (
            spec is None
            or quote.source_ref != domain.quote_source_ref
            or quote.instrument != intent.instrument
            or quote.metadata_version != spec.metadata_version
            or quote.quote_currency != spec.quote_currency
        ) or now_ns < quote.as_of_ns:
            reason = "paper_quote_invalid"
        elif now_ns - quote.as_of_ns > domain.max_quote_age_ns:
            reason = "paper_quote_stale"
        elif intent.quantity % spec.quantity_step != 0:
            reason = "paper_order_quantity_invalid"
        elif not (
            (
                intent.side is Side.BUY
                and intent.position_effect is PositionEffect.OPEN
                and not intent.reduce_only
            )
            or (
                intent.side is Side.SELL
                and intent.position_effect is PositionEffect.CLOSE
                and intent.reduce_only
            )
        ):
            reason = "paper_position_effect_unsupported"
        elif intent.order_type is not OrderType.MARKET:
            reason = "paper_order_type_unsupported"
        elif intent.time_in_force is not TimeInForce.FOK:
            reason = "paper_time_in_force_unsupported"
        elif quote.bid % spec.price_tick != 0 or quote.ask % spec.price_tick != 0:
            reason = "paper_instrument_price_invalid"
        else:
            fill_price = quote.ask if intent.side is Side.BUY else quote.bid
            top_quantity = quote.ask_quantity if intent.side is Side.BUY else quote.bid_quantity
            if intent.quantity > top_quantity:
                reason = "paper_top_of_book_depth_insufficient"
            else:
                notional = intent.quantity * fill_price
                fee = (notional * domain.fee_rate).quantize(
                    domain.fee_quantum, rounding=ROUND_HALF_EVEN
                )
                current = {row.instrument: row for row in state.positions}
                position = current.get(intent.instrument)
                if intent.side is Side.BUY:
                    cash_delta = -(notional + fee)
                    if state.cash + cash_delta < 0:
                        reason = "paper_insufficient_cash"
                else:
                    if position is None or position.quantity < intent.quantity:
                        reason = "paper_insufficient_inventory"
                    else:
                        cash_delta = notional - fee
                if reason is None:
                    position_delta = (
                        intent.quantity if intent.side is Side.BUY else -intent.quantity
                    )
                    provider_order_id = (
                        "paper-"
                        + payload_sha256(
                            {
                                "execution_domain_ref": domain.execution_domain_ref,
                                "simulation_account_ref": domain.simulation_account_ref,
                                "intent_id": intent.intent_id,
                            }
                        )[:32]
                    )
                    return PaperMatchDecisionV1(
                        observation=ProviderObservation(
                            intent_id=intent.intent_id,
                            state=ExecutionState.FILLED,
                            provider_order_id=provider_order_id,
                            filled_quantity=intent.quantity,
                            average_price=fill_price,
                            cumulative_commission=fee,
                            cumulative_commission_currency=domain.cash_currency,
                        ),
                        quote=quote,
                        evaluated_at_ns=now_ns,
                        instrument=intent.instrument,
                        side=intent.side,
                        filled_quantity=intent.quantity,
                        fill_price=fill_price,
                        fee_amount=fee,
                        cash_delta=cash_delta,
                        position_delta=position_delta,
                        reason_code=None,
                    )
    if reason is None:
        reason = "paper_quote_unavailable"
    if reason not in _PAPER_REFUSALS:
        raise ContractValidationError("paper decision has an unsupported refusal")
    return PaperMatchDecisionV1(
        observation=ProviderObservation.rejected(intent.intent_id, reason),
        quote=quote,
        evaluated_at_ns=now_ns,
        instrument=intent.instrument,
        side=intent.side,
        filled_quantity=Decimal("0"),
        fill_price=None,
        fee_amount=Decimal("0"),
        cash_delta=Decimal("0"),
        position_delta=Decimal("0"),
        reason_code=reason,
    )


def apply_paper_decision(
    domain: PaperOwnerDomainV1,
    state: PaperEconomicStateV1,
    decision: PaperMatchDecisionV1,
) -> PaperEconomicStateV1:
    """Apply one already validated immutable paper decision to a replay state."""

    if (
        type(domain) is not PaperOwnerDomainV1
        or type(state) is not PaperEconomicStateV1
        or type(decision) is not PaperMatchDecisionV1
    ):
        raise ContractValidationError("paper state transition inputs are invalid")
    if decision.observation.state is ExecutionState.REJECTED:
        if (
            decision.filled_quantity != 0
            or decision.fill_price is not None
            or decision.fee_amount != 0
            or decision.cash_delta != 0
            or decision.position_delta != 0
        ):
            raise ContractValidationError("rejected paper receipt contains economic effects")
        return state
    if (
        decision.observation.state is not ExecutionState.FILLED
        or decision.quote is None
        or decision.fill_price is None
        or decision.filled_quantity <= 0
        or decision.reason_code is not None
    ):
        raise ContractValidationError("paper receipt is not a complete immediate fill")
    spec = domain.instrument(decision.instrument)
    if spec is None or decision.quote.metadata_version != spec.metadata_version:
        raise ContractValidationError("paper receipt instrument metadata changed")
    current = {row.instrument: row for row in state.positions}
    existing = current.get(decision.instrument)
    new_quantity: Decimal
    if decision.side is Side.BUY:
        old_quantity = Decimal("0") if existing is None else existing.quantity
        old_cost = Decimal("0") if existing is None else existing.average_cost * old_quantity
        new_quantity = old_quantity + decision.filled_quantity
        current[decision.instrument] = PaperPositionBalanceV1(
            decision.instrument,
            new_quantity,
            (old_cost + decision.filled_quantity * decision.fill_price) / new_quantity,
        )
    else:
        if existing is None or existing.quantity < decision.filled_quantity:
            raise ContractValidationError("paper receipt oversells opening inventory")
        new_quantity = existing.quantity - decision.filled_quantity
        if new_quantity == 0:
            del current[decision.instrument]
        else:
            current[decision.instrument] = PaperPositionBalanceV1(
                decision.instrument, new_quantity, existing.average_cost
            )
    cash = state.cash + decision.cash_delta
    fees = state.cumulative_fees + decision.fee_amount
    if cash < 0:
        raise ContractValidationError("paper receipt creates negative cash")
    return PaperEconomicStateV1(
        cash=cash,
        cumulative_fees=fees,
        positions=tuple(current[key] for key in sorted(current)),
    )


class PaperMatchingPort:
    """Trusted adapter that reads quotes and returns proposals, never cash facts."""

    def __init__(self, owner_domain: PaperOwnerDomainV1, quote_source: PaperQuoteSource) -> None:
        if type(owner_domain) is not PaperOwnerDomainV1:
            raise ContractValidationError("paper matching port requires a typed owner domain")
        if not callable(getattr(quote_source, "read_quote", None)):
            raise ContractValidationError("paper matching port requires a trusted quote source")
        self.owner_domain = owner_domain
        self._quote_source = quote_source
        self._risk_quote_lock = threading.RLock()
        self._risk_quotes: dict[str, PaperQuoteV1] = {}
        self._risk_quote_required: set[str] = set()

    def prepare_risk_quote(self, intent: OrderIntent) -> PaperQuoteV1:
        """Bind Risk valuation to the exact quote later used for this paper match.

        This method is only for the trusted Paper/Risk composition. It does not
        accept a client quote and it never changes the economic book. Repeated
        Risk validation of one durable intent reuses its first quote; a stale
        quote fails closed instead of being silently repriced under an existing
        permit.
        """

        if type(intent) is not OrderIntent or intent.scope.provider != PAPER_PROVIDER:
            raise ContractValidationError("paper risk quote requires a paper order intent")
        spec = self.owner_domain.instrument(intent.instrument)
        if (
            spec is None
            or intent.metadata_version != spec.metadata_version
            or intent.order_type is not OrderType.MARKET
            or intent.time_in_force is not TimeInForce.FOK
            or not (
                (
                    intent.side is Side.BUY
                    and intent.position_effect is PositionEffect.OPEN
                    and not intent.reduce_only
                )
                or (
                    intent.side is Side.SELL
                    and intent.position_effect is PositionEffect.CLOSE
                    and intent.reduce_only
                )
            )
        ):
            raise ContractValidationError("paper risk order is outside the supported matching domain")
        with self._risk_quote_lock:
            quote = self._risk_quotes.get(intent.fingerprint)
            if quote is None:
                try:
                    quote = self._quote_source.read_quote(intent.instrument)
                except Exception:
                    raise ContractValidationError("paper risk quote is unavailable") from None
                self._validate_risk_quote(quote, intent, spec)
                if len(self._risk_quotes) >= 1024:
                    now = time_ns()
                    stale = [
                        key
                        for key, value in self._risk_quotes.items()
                        if now >= value.as_of_ns
                        and now - value.as_of_ns > self.owner_domain.max_quote_age_ns
                    ]
                    for key in stale:
                        self._risk_quotes.pop(key, None)
                        self._risk_quote_required.discard(key)
                    if len(self._risk_quotes) >= 1024:
                        raise ContractValidationError("paper risk quote capacity is full")
                self._risk_quotes[intent.fingerprint] = quote
                self._risk_quote_required.add(intent.fingerprint)
            self._validate_risk_quote(quote, intent, spec)
            self._risk_quote_required.add(intent.fingerprint)
            return quote

    def restore_risk_quote(self, intent: OrderIntent, quote: PaperQuoteV1) -> None:
        """Restore an exact durable admission quote without consulting its source."""

        if type(intent) is not OrderIntent:
            raise ContractValidationError("paper risk quote restore requires a typed order")
        spec = self.owner_domain.instrument(intent.instrument)
        if spec is None:
            raise ContractValidationError("paper risk quote instrument is not configured")
        self._validate_risk_quote(quote, intent, spec)
        with self._risk_quote_lock:
            prior = self._risk_quotes.get(intent.fingerprint)
            if prior is not None and prior != quote:
                raise ContractValidationError("paper risk quote conflicts with its durable snapshot")
            self._risk_quotes[intent.fingerprint] = quote
            self._risk_quote_required.add(intent.fingerprint)

    def discard_risk_quote(self, intent_fingerprint: str) -> None:
        """Discard an unused quote after Risk definitively refuses an intent."""

        if type(intent_fingerprint) is not str or len(intent_fingerprint) != 64:
            raise ContractValidationError("invalid paper risk intent fingerprint")
        with self._risk_quote_lock:
            self._risk_quotes.pop(intent_fingerprint, None)
            self._risk_quote_required.discard(intent_fingerprint)

    def _validate_risk_quote(
        self,
        quote: PaperQuoteV1 | None,
        intent: OrderIntent,
        spec: PaperInstrumentV1,
    ) -> None:
        now = time_ns()
        if (
            type(quote) is not PaperQuoteV1
            or quote.source_ref != self.owner_domain.quote_source_ref
            or quote.instrument != intent.instrument
            or quote.metadata_version != spec.metadata_version
            or quote.quote_currency != spec.quote_currency
            or quote.bid % spec.price_tick != 0
            or quote.ask % spec.price_tick != 0
            or now < quote.as_of_ns
            or now - quote.as_of_ns > self.owner_domain.max_quote_age_ns
        ):
            raise ContractValidationError("paper risk quote is invalid or stale")

    def submit(self, intent: OrderIntent) -> PaperDispatchResultV1:
        raise ContractValidationError("paper matching requires execution-only admission")

    def submit_authorized(
        self,
        intent: OrderIntent,
        authorization: ExecutionAdmissionAuthorizationV1,
    ) -> PaperDispatchResultV1:
        if type(authorization) is not ExecutionAdmissionAuthorizationV1:
            raise ContractValidationError("paper dispatch requires typed basic-admission authority")
        parameters = authorization.parameters
        if (
            type(parameters) is not ExecutionProviderOrderParametersV1
            or authorization.intent_fingerprint != intent.fingerprint
            or parameters.intent_fingerprint != intent.fingerprint
            or parameters.provider_id != PAPER_PROVIDER
            or parameters.environment != PAPER_ENVIRONMENT
            or parameters.provider_instrument != intent.instrument
            or parameters.account_ref != self.owner_domain.simulation_account_ref
            or parameters.strategy_id != self.owner_domain.strategy_ref
            or parameters.intent_id != intent.intent_id
            or parameters.signal_id != intent.signal_id
            or parameters.trading_day != intent.scope.trading_day
            or parameters.side is not intent.side
            or parameters.position_effect is not intent.position_effect
            or parameters.order_type is not intent.order_type
            or parameters.quantity != intent.quantity
            or parameters.price != intent.price
            or parameters.time_in_force is not intent.time_in_force
            or parameters.reduce_only is not intent.reduce_only
            or parameters.metadata_version != intent.metadata_version
            or parameters.deadline_ns != intent.deadline_ns
            or dict(parameters.tags) != dict(intent.tags)
            or authorization.owner_id == ""
            or authorization.fencing_token <= 0
            or authorization.issued_at_ns <= 0
            or authorization.expires_at_ns <= authorization.issued_at_ns
        ):
            raise ContractValidationError("paper dispatch authorization does not bind this owner")
        return self._propose(intent, authorization=authorization)

    def cancel(self, intent: CancelIntent) -> CancelObservation:
        if type(intent) is not CancelIntent:
            raise ContractValidationError("paper cancellation intent is invalid")
        return CancelObservation.rejected(
            intent.cancel_id,
            intent.target_intent_id,
            intent.provider_order_id,
            "paper_immediate_order_terminal",
        )

    def _propose(
        self,
        intent: OrderIntent,
        *,
        authorization: ExecutionAdmissionAuthorizationV1 | None,
    ) -> PaperDispatchResultV1:
        if type(intent) is not OrderIntent:
            raise ContractValidationError("paper order intent is invalid")
        spec = self.owner_domain.instrument(intent.instrument)
        if spec is None or intent.metadata_version != spec.metadata_version:
            return PaperDispatchResultV1(
                intent.intent_id,
                intent.fingerprint,
                None,
                "paper_instrument_unsupported",
                authorization,
            )
        if intent.order_type is not OrderType.MARKET:
            return PaperDispatchResultV1(
                intent.intent_id,
                intent.fingerprint,
                None,
                "paper_order_type_unsupported",
                authorization,
            )
        if not (
            (
                intent.side is Side.BUY
                and intent.position_effect is PositionEffect.OPEN
                and not intent.reduce_only
            )
            or (
                intent.side is Side.SELL
                and intent.position_effect is PositionEffect.CLOSE
                and intent.reduce_only
            )
        ):
            return PaperDispatchResultV1(
                intent.intent_id,
                intent.fingerprint,
                None,
                "paper_position_effect_unsupported",
                authorization,
            )
        if intent.time_in_force is not TimeInForce.FOK:
            return PaperDispatchResultV1(
                intent.intent_id,
                intent.fingerprint,
                None,
                "paper_time_in_force_unsupported",
                authorization,
            )
        with self._risk_quote_lock:
            quote = self._risk_quotes.pop(intent.fingerprint, None)
            risk_quote_required = intent.fingerprint in self._risk_quote_required
            self._risk_quote_required.discard(intent.fingerprint)
        if quote is None:
            if risk_quote_required:
                return PaperDispatchResultV1(
                    intent.intent_id,
                    intent.fingerprint,
                    None,
                    "paper_quote_unavailable",
                    authorization,
                )
            try:
                quote = self._quote_source.read_quote(intent.instrument)
            except Exception:
                return PaperDispatchResultV1(
                    intent.intent_id,
                    intent.fingerprint,
                    None,
                    "paper_quote_unavailable",
                    authorization,
                )
        if quote is None:
            return PaperDispatchResultV1(
                intent.intent_id,
                intent.fingerprint,
                None,
                "paper_quote_unavailable",
                authorization,
            )
        if (
            type(quote) is not PaperQuoteV1
            or quote.instrument != intent.instrument
            or quote.metadata_version != spec.metadata_version
            or quote.quote_currency != spec.quote_currency
        ):
            return PaperDispatchResultV1(
                intent.intent_id,
                intent.fingerprint,
                None,
                "paper_quote_invalid",
                authorization,
            )
        if risk_quote_required:
            try:
                self._validate_risk_quote(quote, intent, spec)
            except ContractValidationError:
                return PaperDispatchResultV1(
                    intent.intent_id,
                    intent.fingerprint,
                    None,
                    "paper_quote_invalid",
                    authorization,
                )
        return PaperDispatchResultV1(
            intent.intent_id,
            intent.fingerprint,
            quote,
            None,
            authorization,
        )


@dataclass(frozen=True, slots=True)
class PaperMatchReceiptV1:
    """Immutable receipt linking one order event to its exact paper economics."""

    account_key: str
    scope_key: str
    journal_generation: str
    execution_domain_ref: str
    simulation_account_ref: str
    experiment_ref: str
    model_ref: str
    model_version: str
    intent_id: str
    intent_fingerprint: str
    event_id: str
    event_sequence: int
    state: ExecutionState
    reason_code: str | None
    evaluated_at_ns: int
    instrument: str
    side: Side
    quantity: Decimal
    fill_price: Decimal | None
    fee_amount: Decimal
    fee_currency: str
    quote: PaperQuoteV1 | None
    cash_delta: Decimal
    position_delta: Decimal
    before_book_sha256: str
    after_book_sha256: str
    receipt_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "account_key",
            "scope_key",
            "execution_domain_ref",
            "simulation_account_ref",
            "experiment_ref",
            "model_ref",
            "model_version",
            "intent_id",
            "event_id",
            "instrument",
            "fee_currency",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name.replace("_", " ")))
        if (
            type(self.journal_generation) is not str
            or len(self.journal_generation) != 32
            or any(char not in "0123456789abcdef" for char in self.journal_generation)
        ):
            raise ContractValidationError("invalid paper receipt journal generation")
        for name in (
            "intent_fingerprint",
            "before_book_sha256",
            "after_book_sha256",
            "receipt_sha256",
        ):
            value = _text(getattr(self, name), name.replace("_", " "), max_length=64)
            if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
                raise ContractValidationError("invalid paper receipt digest")
        if type(self.event_sequence) is not int or self.event_sequence <= 0:
            raise ContractValidationError("invalid paper receipt event sequence")
        if type(self.evaluated_at_ns) is not int or self.evaluated_at_ns <= 0:
            raise ContractValidationError("invalid paper receipt time")
        try:
            state = ExecutionState(self.state)
            side = Side(self.side)
        except (TypeError, ValueError):
            raise ContractValidationError("invalid paper receipt state or side") from None
        if state not in {ExecutionState.FILLED, ExecutionState.REJECTED}:
            raise ContractValidationError("paper receipt must be filled or rejected")
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "quantity", _decimal(self.quantity, "receipt quantity"))
        object.__setattr__(self, "fee_amount", _decimal(self.fee_amount, "receipt fee"))
        object.__setattr__(self, "cash_delta", _decimal(self.cash_delta, "receipt cash delta"))
        object.__setattr__(
            self, "position_delta", _decimal(self.position_delta, "receipt position delta")
        )
        if self.fill_price is not None:
            object.__setattr__(
                self, "fill_price", _decimal(self.fill_price, "receipt fill price", positive=True)
            )
        if self.quote is not None and type(self.quote) is not PaperQuoteV1:
            raise ContractValidationError("invalid paper receipt quote")
        if self.reason_code is not None and self.reason_code not in _PAPER_REFUSALS:
            raise ContractValidationError("invalid paper receipt refusal")
        if state is ExecutionState.FILLED:
            if (
                self.reason_code is not None
                or self.quote is None
                or self.fill_price is None
                or self.quantity <= 0
                or self.fee_amount < 0
                or self.position_delta != (self.quantity if side is Side.BUY else -self.quantity)
            ):
                raise ContractValidationError("paper fill receipt is incomplete")
        elif (
            self.reason_code is None
            or self.quantity != 0
            or self.fill_price is not None
            or self.fee_amount != 0
            or self.cash_delta != 0
            or self.position_delta != 0
        ):
            raise ContractValidationError("paper rejection receipt contains fill effects")
        if self.receipt_sha256 != payload_sha256(self.payload_without_digest()):
            raise ContractValidationError("paper receipt digest does not match its contents")

    def payload_without_digest(self) -> dict[str, object]:
        return {
            "account_key": self.account_key,
            "scope_key": self.scope_key,
            "journal_generation": self.journal_generation,
            "execution_domain_ref": self.execution_domain_ref,
            "simulation_account_ref": self.simulation_account_ref,
            "experiment_ref": self.experiment_ref,
            "model_ref": self.model_ref,
            "model_version": self.model_version,
            "intent_id": self.intent_id,
            "intent_fingerprint": self.intent_fingerprint,
            "event_id": self.event_id,
            "event_sequence": self.event_sequence,
            "state": self.state.value,
            "reason_code": self.reason_code,
            "evaluated_at_ns": self.evaluated_at_ns,
            "instrument": self.instrument,
            "side": self.side.value,
            "quantity": format(self.quantity, "f"),
            "fill_price": None if self.fill_price is None else format(self.fill_price, "f"),
            "fee_amount": format(self.fee_amount, "f"),
            "fee_currency": self.fee_currency,
            "quote": None if self.quote is None else self.quote.to_payload(),
            "cash_delta": format(self.cash_delta, "f"),
            "position_delta": format(self.position_delta, "f"),
            "before_book_sha256": self.before_book_sha256,
            "after_book_sha256": self.after_book_sha256,
        }

    def to_payload(self) -> dict[str, object]:
        return {**self.payload_without_digest(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def create(cls, **values: object) -> PaperMatchReceiptV1:
        digest = payload_sha256(values)
        return cls(**values, receipt_sha256=digest)  # type: ignore[arg-type]

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> PaperMatchReceiptV1:
        expected = {
            "account_key",
            "scope_key",
            "journal_generation",
            "execution_domain_ref",
            "simulation_account_ref",
            "experiment_ref",
            "model_ref",
            "model_version",
            "intent_id",
            "intent_fingerprint",
            "event_id",
            "event_sequence",
            "state",
            "reason_code",
            "evaluated_at_ns",
            "instrument",
            "side",
            "quantity",
            "fill_price",
            "fee_amount",
            "fee_currency",
            "quote",
            "cash_delta",
            "position_delta",
            "before_book_sha256",
            "after_book_sha256",
            "receipt_sha256",
        }
        if type(payload) is not dict or set(payload) != expected:
            raise ContractValidationError("stored paper receipt has an invalid shape")
        quote_payload = payload["quote"]
        quote = None
        if quote_payload is not None:
            if type(quote_payload) is not dict or set(quote_payload) != {
                "source_ref",
                "generation",
                "cursor",
                "instrument",
                "metadata_version",
                "quote_currency",
                "bid",
                "ask",
                "bid_quantity",
                "ask_quantity",
                "as_of_ns",
            }:
                raise ContractValidationError("stored paper quote has an invalid shape")
            quote = PaperQuoteV1(
                source_ref=quote_payload["source_ref"],
                generation=quote_payload["generation"],
                cursor=quote_payload["cursor"],
                instrument=quote_payload["instrument"],
                metadata_version=quote_payload["metadata_version"],
                quote_currency=quote_payload["quote_currency"],
                bid=Decimal(quote_payload["bid"]),
                ask=Decimal(quote_payload["ask"]),
                bid_quantity=Decimal(quote_payload["bid_quantity"]),
                ask_quantity=Decimal(quote_payload["ask_quantity"]),
                as_of_ns=quote_payload["as_of_ns"],
            )
        return cls(
            account_key=payload["account_key"],
            scope_key=payload["scope_key"],
            journal_generation=payload["journal_generation"],
            execution_domain_ref=payload["execution_domain_ref"],
            simulation_account_ref=payload["simulation_account_ref"],
            experiment_ref=payload["experiment_ref"],
            model_ref=payload["model_ref"],
            model_version=payload["model_version"],
            intent_id=payload["intent_id"],
            intent_fingerprint=payload["intent_fingerprint"],
            event_id=payload["event_id"],
            event_sequence=payload["event_sequence"],
            state=payload["state"],
            reason_code=payload["reason_code"],
            evaluated_at_ns=payload["evaluated_at_ns"],
            instrument=payload["instrument"],
            side=payload["side"],
            quantity=Decimal(payload["quantity"]),
            fill_price=None if payload["fill_price"] is None else Decimal(payload["fill_price"]),
            fee_amount=Decimal(payload["fee_amount"]),
            fee_currency=payload["fee_currency"],
            quote=quote,
            cash_delta=Decimal(payload["cash_delta"]),
            position_delta=Decimal(payload["position_delta"]),
            before_book_sha256=payload["before_book_sha256"],
            after_book_sha256=payload["after_book_sha256"],
            receipt_sha256=payload["receipt_sha256"],
        )


@dataclass(frozen=True, slots=True)
class PaperMatchReceiptV2(PaperMatchReceiptV1):
    """One durable synthetic managed-paper fill with an explicit match clock.

    V2 is emitted only for a successful managed-paper MARKET/FOK full fill.
    Its match timestamp is the single clock sample used to evaluate and replay
    the synthetic match. ``event_created_at_ns`` remains local journal commit
    metadata and is never an execution-time fallback.
    """

    schema: str
    time_source: str
    match_time_ns: int
    event_created_at_ns: int
    fill_id: str
    strategy_ref: str
    commission_model_id: str
    commission_model_version: str

    def __post_init__(self) -> None:
        PaperMatchReceiptV1.__post_init__(self)
        for name in (
            "schema",
            "time_source",
            "fill_id",
            "strategy_ref",
            "commission_model_id",
            "commission_model_version",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name.replace("_", " ")))
        if (
            self.schema != PAPER_MATCH_RECEIPT_SCHEMA_V2
            or self.time_source != PAPER_MATCH_TIME_SOURCE_V2
            or type(self.match_time_ns) is not int
            or self.match_time_ns <= 0
            or self.match_time_ns != self.evaluated_at_ns
            or type(self.event_created_at_ns) is not int
            or self.event_created_at_ns <= 0
            or self.state is not ExecutionState.FILLED
            or self.fill_id != f"paper:{self.journal_generation}:{self.event_id}"
            or self.commission_model_id != PAPER_FEE_MODEL_REF
            or self.commission_model_version != PAPER_FEE_MODEL_VERSION
        ):
            raise ContractValidationError("managed-paper V2 fill facts are invalid")

    def payload_without_digest(self) -> dict[str, object]:
        return {
            **PaperMatchReceiptV1.payload_without_digest(self),
            "schema": self.schema,
            "time_source": self.time_source,
            "match_time_ns": self.match_time_ns,
            "event_created_at_ns": self.event_created_at_ns,
            "fill_id": self.fill_id,
            "strategy_ref": self.strategy_ref,
            "commission_model_id": self.commission_model_id,
            "commission_model_version": self.commission_model_version,
        }

    def to_fill_fact_payload(self) -> dict[str, object]:
        """Return the strict wire projection for one verified V2 paper fill."""

        if self.quote is None or self.fill_price is None or self.state is not ExecutionState.FILLED:
            raise ContractValidationError("managed-paper V2 receipt has no fill fact")
        return {
            "schema": self.schema,
            "time_source": self.time_source,
            "fill_id": self.fill_id,
            "match_time_ns": self.match_time_ns,
            "event_created_at_ns": self.event_created_at_ns,
            "quantity": format(self.quantity, "f"),
            "price": format(self.fill_price, "f"),
            "commission": format(self.fee_amount, "f"),
            "commission_currency": self.fee_currency,
            "commission_model_id": self.commission_model_id,
            "commission_model_version": self.commission_model_version,
            "event_id": self.event_id,
            "event_sequence": self.event_sequence,
            "journal_generation": self.journal_generation,
            "scope_key": self.scope_key,
            "intent_id": self.intent_id,
            "intent_fingerprint": self.intent_fingerprint,
            "account_ref": self.simulation_account_ref,
            "strategy_ref": self.strategy_ref,
            "quote_source_ref": self.quote.source_ref,
            "quote_generation": self.quote.generation,
            "quote_cursor": self.quote.cursor,
            "quote_instrument": self.quote.instrument,
            "receipt_sha256": self.receipt_sha256,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> PaperMatchReceiptV2:
        extra_fields = {
            "schema",
            "time_source",
            "match_time_ns",
            "event_created_at_ns",
            "fill_id",
            "strategy_ref",
            "commission_model_id",
            "commission_model_version",
        }
        if type(payload) is not dict or "schema" not in payload:
            raise ContractValidationError("stored managed-paper V2 receipt is malformed")
        base_payload = {key: value for key, value in payload.items() if key not in extra_fields}
        if set(payload) != set(base_payload) | extra_fields:
            raise ContractValidationError("stored managed-paper V2 receipt has an invalid shape")
        # Reuse the strict V1 parser for all shared receipt and quote fields;
        # the temporary digest is only for parsing, while this V2 instance
        # verifies the original V2 digest over the complete payload below.
        base_without_digest = {
            key: value for key, value in base_payload.items() if key != "receipt_sha256"
        }
        base_payload["receipt_sha256"] = payload_sha256(base_without_digest)
        base = PaperMatchReceiptV1.from_payload(base_payload)
        return cls(
            account_key=base.account_key,
            scope_key=base.scope_key,
            journal_generation=base.journal_generation,
            execution_domain_ref=base.execution_domain_ref,
            simulation_account_ref=base.simulation_account_ref,
            experiment_ref=base.experiment_ref,
            model_ref=base.model_ref,
            model_version=base.model_version,
            intent_id=base.intent_id,
            intent_fingerprint=base.intent_fingerprint,
            event_id=base.event_id,
            event_sequence=base.event_sequence,
            state=base.state,
            reason_code=base.reason_code,
            evaluated_at_ns=base.evaluated_at_ns,
            instrument=base.instrument,
            side=base.side,
            quantity=base.quantity,
            fill_price=base.fill_price,
            fee_amount=base.fee_amount,
            fee_currency=base.fee_currency,
            quote=base.quote,
            cash_delta=base.cash_delta,
            position_delta=base.position_delta,
            before_book_sha256=base.before_book_sha256,
            after_book_sha256=base.after_book_sha256,
            schema=payload["schema"],
            time_source=payload["time_source"],
            match_time_ns=payload["match_time_ns"],
            event_created_at_ns=payload["event_created_at_ns"],
            fill_id=payload["fill_id"],
            strategy_ref=payload["strategy_ref"],
            commission_model_id=payload["commission_model_id"],
            commission_model_version=payload["commission_model_version"],
            receipt_sha256=payload["receipt_sha256"],
        )


@dataclass(frozen=True, slots=True)
class PaperPositionBalanceV1:
    instrument: str
    quantity: Decimal
    average_cost: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument", _text(self.instrument, "position instrument"))
        object.__setattr__(
            self, "quantity", _decimal(self.quantity, "position quantity", positive=True)
        )
        object.__setattr__(
            self, "average_cost", _decimal(self.average_cost, "average cost", positive=True)
        )


@dataclass(frozen=True, slots=True)
class PaperBookSnapshotV1:
    account_key: str
    scope_key: str
    execution_domain_ref: str
    simulation_account_ref: str
    experiment_ref: str
    model_ref: str
    model_version: str
    cash_currency: str
    cash: Decimal
    cumulative_fees: Decimal
    positions: tuple[PaperPositionBalanceV1, ...]
    execution_journal_generation: str
    execution_cursor: int
    receipt_count: int
    quote_watermarks: tuple[tuple[str, str, str, int, int], ...]

    def __post_init__(self) -> None:
        for name in (
            "account_key",
            "scope_key",
            "execution_domain_ref",
            "simulation_account_ref",
            "experiment_ref",
            "model_ref",
            "model_version",
            "cash_currency",
            "execution_journal_generation",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name.replace("_", " ")))
        object.__setattr__(self, "cash", _decimal(self.cash, "book cash"))
        object.__setattr__(self, "cumulative_fees", _decimal(self.cumulative_fees, "book fees"))
        if len(self.execution_journal_generation) != 32 or any(
            character not in "0123456789abcdef" for character in self.execution_journal_generation
        ):
            raise ContractValidationError("invalid paper book journal generation")
        if type(self.execution_cursor) is not int or self.execution_cursor < 0:
            raise ContractValidationError("invalid paper book execution cursor")
        if type(self.receipt_count) is not int or self.receipt_count < 0:
            raise ContractValidationError("invalid paper book receipt count")
        if any(type(row) is not PaperPositionBalanceV1 for row in self.positions):
            raise ContractValidationError("paper book positions are invalid")
        if any(
            type(item) is not tuple
            or len(item) != 5
            or any(type(value) is not str for value in item[:3])
            or type(item[3]) is not int
            or type(item[4]) is not int
            for item in self.quote_watermarks
        ):
            raise ContractValidationError("paper book quote watermarks are invalid")


__all__ = (
    "PAPER_PROVIDER",
    "PAPER_ENVIRONMENT",
    "PAPER_MODEL_REF",
    "PAPER_MODEL_VERSION",
    "PAPER_FEE_MODEL_REF",
    "PAPER_FEE_MODEL_VERSION",
    "PAPER_MATCH_RECEIPT_SCHEMA_V2",
    "PAPER_MATCH_TIME_SOURCE_V2",
    "PaperInstrumentV1",
    "PaperOpeningPositionV1",
    "PaperOwnerDomainV1",
    "PaperQuoteV1",
    "PaperQuoteSource",
    "PaperDispatchResultV1",
    "PaperMatchingPort",
    "PaperEconomicStateV1",
    "PaperMatchDecisionV1",
    "PaperMatchReceiptV1",
    "PaperMatchReceiptV2",
    "PaperPositionBalanceV1",
    "PaperBookSnapshotV1",
    "initial_paper_state",
    "paper_state_digest",
    "evaluate_paper_match",
    "apply_paper_decision",
)
