"""Immutable provider-neutral contracts owned by :mod:`bt_api_execution`.

The module contains no I/O.  Values crossing a provider boundary are normalized
before they reach these contracts, and amounts stay as :class:`~decimal.Decimal`
until an explicit provider adapter performs its own wire conversion.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, is_dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum, StrEnum
from typing import Any, cast

from .errors import ContractValidationError

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_INSTRUMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,191}$")
_SAFE_REASON = re.compile(r"^[a-z][a-z0-9_]{0,127}$")


def _text(value: str, field_name: str, *, pattern: re.Pattern[str] = _IDENTIFIER) -> str:
    if not isinstance(value, str) or value != value.strip() or not pattern.fullmatch(value):
        raise ContractValidationError(f"invalid {field_name}")
    return value


def _decimal(value: Decimal, field_name: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, float):
        raise ContractValidationError(f"invalid {field_name}")
    if not isinstance(value, Decimal):
        try:
            value = Decimal(value)
        except (InvalidOperation, TypeError, ValueError) as error:
            raise ContractValidationError(f"invalid {field_name}") from error
    if not value.is_finite() or (positive and value <= 0):
        raise ContractValidationError(f"invalid {field_name}")
    return value


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(cast("Any", value))
    raise TypeError(f"cannot canonicalize {type(value).__name__}")


def canonical_json(value: Any) -> str:
    """Return deterministic JSON suitable for payload fingerprints."""

    return json.dumps(
        value,
        default=_json_default,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def payload_sha256(value: Any) -> str:
    """Return the SHA-256 of a deterministic, non-secret contract payload."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class PositionEffect(StrEnum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    CLOSE_TODAY = "CLOSE_TODAY"
    CLOSE_YESTERDAY = "CLOSE_YESTERDAY"


class OrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class TimeInForce(StrEnum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    DAY = "DAY"


class ExecutionState(StrEnum):
    PENDING_ADMISSION = "PENDING_ADMISSION"
    PENDING_DISPATCH = "PENDING_DISPATCH"
    DISPATCHING = "DISPATCHING"
    ACKED = "ACKED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    BLOCKED = "BLOCKED"
    UNKNOWN = "UNKNOWN"


TERMINAL_STATES = frozenset(
    {
        ExecutionState.FILLED,
        ExecutionState.CANCELLED,
        ExecutionState.REJECTED,
        ExecutionState.BLOCKED,
    }
)

_ALLOWED_TRANSITIONS: Mapping[ExecutionState, frozenset[ExecutionState]] = {
    ExecutionState.PENDING_ADMISSION: frozenset(
        {ExecutionState.PENDING_DISPATCH, ExecutionState.REJECTED, ExecutionState.BLOCKED}
    ),
    ExecutionState.PENDING_DISPATCH: frozenset(
        {ExecutionState.DISPATCHING, ExecutionState.CANCELLED, ExecutionState.BLOCKED}
    ),
    ExecutionState.DISPATCHING: frozenset(
        {
            ExecutionState.ACKED,
            ExecutionState.PARTIALLY_FILLED,
            ExecutionState.FILLED,
            ExecutionState.REJECTED,
            ExecutionState.BLOCKED,
            ExecutionState.UNKNOWN,
        }
    ),
    ExecutionState.ACKED: frozenset(
        {
            ExecutionState.PARTIALLY_FILLED,
            ExecutionState.FILLED,
            ExecutionState.CANCELLED,
            ExecutionState.UNKNOWN,
        }
    ),
    ExecutionState.PARTIALLY_FILLED: frozenset(
        {ExecutionState.FILLED, ExecutionState.CANCELLED, ExecutionState.UNKNOWN}
    ),
    ExecutionState.UNKNOWN: frozenset(
        {
            ExecutionState.ACKED,
            ExecutionState.PARTIALLY_FILLED,
            ExecutionState.FILLED,
            ExecutionState.CANCELLED,
            ExecutionState.REJECTED,
        }
    ),
    ExecutionState.BLOCKED: frozenset(),
    ExecutionState.FILLED: frozenset(),
    ExecutionState.CANCELLED: frozenset(),
    ExecutionState.REJECTED: frozenset(),
}


def can_transition(source: ExecutionState, target: ExecutionState) -> bool:
    """Return whether a managed child may move from ``source`` to ``target``."""

    return target in _ALLOWED_TRANSITIONS[source]


@dataclass(frozen=True, slots=True)
class ExecutionScope:
    """Opaque account/strategy scope used for journal and lease isolation."""

    provider: str
    environment: str
    account_ref: str
    strategy_id: str
    trading_day: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider", _text(self.provider, "provider"))
        object.__setattr__(self, "environment", _text(self.environment, "environment"))
        object.__setattr__(self, "account_ref", _text(self.account_ref, "account_ref"))
        object.__setattr__(self, "strategy_id", _text(self.strategy_id, "strategy_id"))
        if self.trading_day is not None:
            object.__setattr__(self, "trading_day", _text(self.trading_day, "trading_day"))

    @property
    def account_key(self) -> str:
        """Return the account authority key, excluding strategy allocation."""

        return "account:" + payload_sha256(
            {
                "provider": self.provider,
                "environment": self.environment,
                "account_ref": self.account_ref,
            }
        )

    @property
    def key(self) -> str:
        """Return the full durable scope key."""

        return "scope:" + payload_sha256(self.to_payload())

    def to_payload(self) -> dict[str, str | None]:
        return {
            "provider": self.provider,
            "environment": self.environment,
            "account_ref": self.account_ref,
            "strategy_id": self.strategy_id,
            "trading_day": self.trading_day,
        }


@dataclass(frozen=True, slots=True)
class OrderIntent:
    """One immutable request to create a managed provider child order."""

    intent_id: str
    scope: ExecutionScope
    signal_id: str
    instrument: str
    side: Side
    position_effect: PositionEffect
    order_type: OrderType
    quantity: Decimal
    price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.GTC
    reduce_only: bool = False
    metadata_version: str = "unknown"
    deadline_ns: int | None = None
    tags: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.scope, ExecutionScope):
            raise ContractValidationError("invalid execution scope")
        object.__setattr__(self, "intent_id", _text(self.intent_id, "intent_id"))
        object.__setattr__(self, "signal_id", _text(self.signal_id, "signal_id"))
        object.__setattr__(
            self, "instrument", _text(self.instrument, "instrument", pattern=_INSTRUMENT)
        )
        for name, enum_type in (
            ("side", Side),
            ("position_effect", PositionEffect),
            ("order_type", OrderType),
            ("time_in_force", TimeInForce),
        ):
            try:
                object.__setattr__(self, name, enum_type(getattr(self, name)))
            except (TypeError, ValueError) as error:
                raise ContractValidationError(f"invalid {name}") from error
        object.__setattr__(self, "quantity", _decimal(self.quantity, "quantity", positive=True))
        object.__setattr__(
            self, "metadata_version", _text(self.metadata_version, "metadata_version")
        )
        if self.price is not None:
            object.__setattr__(self, "price", _decimal(self.price, "price", positive=True))
        if self.order_type is OrderType.LIMIT and self.price is None:
            raise ContractValidationError("limit intent requires price")
        if self.order_type is OrderType.MARKET and self.price is not None:
            raise ContractValidationError("market intent must not include price")
        if self.position_effect is not PositionEffect.OPEN and not self.reduce_only:
            raise ContractValidationError("close intent must be reduce_only")
        if self.deadline_ns is not None and (
            type(self.deadline_ns) is not int or self.deadline_ns <= 0
        ):
            raise ContractValidationError("invalid deadline_ns")
        if type(self.reduce_only) is not bool:
            raise ContractValidationError("invalid reduce_only")
        if not isinstance(self.tags, Mapping):
            raise ContractValidationError("invalid tags")
        normalized_tags: dict[str, str] = {}
        for key, value in self.tags.items():
            normalized_tags[_text(key, "tag key")] = _text(value, "tag value")
        object.__setattr__(self, "tags", normalized_tags)

    @classmethod
    def limit(
        cls,
        *,
        intent_id: str,
        scope: ExecutionScope,
        signal_id: str,
        instrument: str,
        side: Side,
        quantity: Decimal,
        price: Decimal,
        position_effect: PositionEffect = PositionEffect.OPEN,
        time_in_force: TimeInForce = TimeInForce.GTC,
        reduce_only: bool = False,
        metadata_version: str = "unknown",
        deadline_ns: int | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> OrderIntent:
        return cls(
            intent_id=intent_id,
            scope=scope,
            signal_id=signal_id,
            instrument=instrument,
            side=side,
            position_effect=position_effect,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=price,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            metadata_version=metadata_version,
            deadline_ns=deadline_ns,
            tags={} if tags is None else tags,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "intent_id": self.intent_id,
            "scope": self.scope.to_payload(),
            "signal_id": self.signal_id,
            "instrument": self.instrument,
            "side": self.side.value,
            "position_effect": self.position_effect.value,
            "order_type": self.order_type.value,
            "quantity": format(self.quantity, "f"),
            "price": None if self.price is None else format(self.price, "f"),
            "time_in_force": self.time_in_force.value,
            "reduce_only": self.reduce_only,
            "metadata_version": self.metadata_version,
            "deadline_ns": self.deadline_ns,
            "tags": dict(self.tags),
        }

    @property
    def fingerprint(self) -> str:
        return payload_sha256(self.to_payload())


def order_intent_from_payload(payload: Mapping[str, Any]) -> OrderIntent:
    """Recreate a version-1 immutable intent from its canonical journal form."""

    if not isinstance(payload, Mapping) or not isinstance(payload.get("scope"), Mapping):
        raise ContractValidationError("invalid persisted intent payload")
    try:
        return OrderIntent(
            intent_id=payload["intent_id"],
            scope=ExecutionScope(**payload["scope"]),
            signal_id=payload["signal_id"],
            instrument=payload["instrument"],
            side=Side(payload["side"]),
            position_effect=PositionEffect(payload["position_effect"]),
            order_type=OrderType(payload["order_type"]),
            quantity=Decimal(payload["quantity"]),
            price=None if payload["price"] is None else Decimal(payload["price"]),
            time_in_force=TimeInForce(payload["time_in_force"]),
            reduce_only=payload["reduce_only"],
            metadata_version=payload["metadata_version"],
            deadline_ns=payload["deadline_ns"],
            tags=payload["tags"],
        )
    except (KeyError, TypeError, ValueError, InvalidOperation) as error:
        raise ContractValidationError("invalid persisted intent payload") from error


@dataclass(frozen=True, slots=True)
class CancelIntent:
    """One immutable request to cancel a previously evidenced provider order.

    A cancellation is never identified only by a framework order reference.
    It is bound to both the original managed intent and the provider order id
    that the execution journal had already confirmed.  ``cancel_id`` is a
    durable idempotency key for this one cancellation attempt.
    """

    cancel_id: str
    scope: ExecutionScope
    target_intent_id: str
    provider_order_id: str
    metadata_version: str = "unknown"
    tags: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.scope, ExecutionScope):
            raise ContractValidationError("invalid cancellation scope")
        object.__setattr__(self, "cancel_id", _text(self.cancel_id, "cancel_id"))
        object.__setattr__(
            self, "target_intent_id", _text(self.target_intent_id, "target_intent_id")
        )
        object.__setattr__(
            self, "provider_order_id", _text(self.provider_order_id, "provider_order_id")
        )
        object.__setattr__(
            self, "metadata_version", _text(self.metadata_version, "metadata_version")
        )
        if not isinstance(self.tags, Mapping):
            raise ContractValidationError("invalid cancellation tags")
        normalized_tags: dict[str, str] = {}
        for key, value in self.tags.items():
            normalized_tags[_text(key, "cancellation tag key")] = _text(
                value, "cancellation tag value"
            )
        object.__setattr__(self, "tags", normalized_tags)

    def to_payload(self) -> dict[str, Any]:
        """Return the canonical, non-secret durable cancellation payload."""

        return {
            "cancel_id": self.cancel_id,
            "scope": self.scope.to_payload(),
            "target_intent_id": self.target_intent_id,
            "provider_order_id": self.provider_order_id,
            "metadata_version": self.metadata_version,
            "tags": dict(self.tags),
        }

    @property
    def fingerprint(self) -> str:
        """Return the immutable cancellation payload fingerprint."""

        return payload_sha256(self.to_payload())


def cancel_intent_from_payload(payload: Mapping[str, Any]) -> CancelIntent:
    """Rebuild a version-1 cancellation intent from its durable representation."""

    if not isinstance(payload, Mapping) or not isinstance(payload.get("scope"), Mapping):
        raise ContractValidationError("invalid persisted cancellation payload")
    try:
        return CancelIntent(
            cancel_id=payload["cancel_id"],
            scope=ExecutionScope(**payload["scope"]),
            target_intent_id=payload["target_intent_id"],
            provider_order_id=payload["provider_order_id"],
            metadata_version=payload["metadata_version"],
            tags=payload["tags"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ContractValidationError("invalid persisted cancellation payload") from error


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """A plan groups compatible immutable intents without claiming atomicity."""

    plan_id: str
    scope: ExecutionScope
    intent_ids: tuple[str, ...]
    execution_kind: str = "single"

    def __post_init__(self) -> None:
        object.__setattr__(self, "plan_id", _text(self.plan_id, "plan_id"))
        if not self.intent_ids:
            raise ContractValidationError("execution plan requires intent_ids")
        ids = tuple(_text(item, "intent_id") for item in self.intent_ids)
        if len(set(ids)) != len(ids):
            raise ContractValidationError("execution plan has duplicate intent_ids")
        object.__setattr__(self, "intent_ids", ids)
        allowed = {
            "single",
            "native_atomic",
            "native_algo",
            "sequential_non_atomic",
            "basket_best_effort",
        }
        if self.execution_kind not in allowed:
            raise ContractValidationError("invalid execution_kind")


@dataclass(frozen=True, slots=True)
class ParentExecution:
    """Stable parent identity for an execution plan and its child orders."""

    parent_id: str
    plan_id: str
    scope: ExecutionScope
    intent_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "parent_id", _text(self.parent_id, "parent_id"))
        object.__setattr__(self, "plan_id", _text(self.plan_id, "plan_id"))
        ids = tuple(_text(item, "intent_id") for item in self.intent_ids)
        if not ids or len(set(ids)) != len(ids):
            raise ContractValidationError("invalid parent intent_ids")
        object.__setattr__(self, "intent_ids", ids)


@dataclass(frozen=True, slots=True)
class ChildIntent:
    """A provider-facing child derived from one immutable parent intent."""

    child_id: str
    parent_id: str
    intent_id: str
    scope: ExecutionScope
    quantity: Decimal
    provider_capability_receipt: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "child_id", _text(self.child_id, "child_id"))
        object.__setattr__(self, "parent_id", _text(self.parent_id, "parent_id"))
        object.__setattr__(self, "intent_id", _text(self.intent_id, "intent_id"))
        object.__setattr__(self, "quantity", _decimal(self.quantity, "quantity", positive=True))
        if self.provider_capability_receipt is not None:
            object.__setattr__(
                self,
                "provider_capability_receipt",
                _text(self.provider_capability_receipt, "provider_capability_receipt"),
            )


@dataclass(frozen=True, slots=True)
class FillAllocation:
    """One monotonic allocation of a provider fill to a strategy intent."""

    allocation_id: str
    child_id: str
    intent_id: str
    quantity: Decimal
    price: Decimal
    fee: Decimal = Decimal("0")
    currency: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "allocation_id", _text(self.allocation_id, "allocation_id"))
        object.__setattr__(self, "child_id", _text(self.child_id, "child_id"))
        object.__setattr__(self, "intent_id", _text(self.intent_id, "intent_id"))
        object.__setattr__(self, "quantity", _decimal(self.quantity, "quantity", positive=True))
        object.__setattr__(self, "price", _decimal(self.price, "price", positive=True))
        object.__setattr__(self, "fee", _decimal(self.fee, "fee"))
        if self.currency is not None:
            object.__setattr__(self, "currency", _text(self.currency, "currency"))


@dataclass(frozen=True, slots=True)
class StrategyAllocation:
    """Read-only account allocation used to further tighten strategy sizing."""

    scope: ExecutionScope
    allocation_version: str
    max_notional: Decimal | None = None
    max_position: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "allocation_version", _text(self.allocation_version, "allocation_version")
        )
        if self.max_notional is not None:
            object.__setattr__(
                self, "max_notional", _decimal(self.max_notional, "max_notional", positive=True)
            )
        if self.max_position is not None:
            object.__setattr__(
                self, "max_position", _decimal(self.max_position, "max_position", positive=True)
            )


@dataclass(frozen=True, slots=True)
class InstrumentMetadataSnapshot:
    """Versioned, time-bound market metadata used by a later admission policy."""

    scope: ExecutionScope
    instrument: str
    metadata_version: str
    as_of_ns: int
    tick_size: Decimal
    quantity_step: Decimal
    contract_multiplier: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "instrument", _text(self.instrument, "instrument", pattern=_INSTRUMENT)
        )
        object.__setattr__(
            self, "metadata_version", _text(self.metadata_version, "metadata_version")
        )
        if type(self.as_of_ns) is not int or self.as_of_ns <= 0:
            raise ContractValidationError("invalid as_of_ns")
        object.__setattr__(self, "tick_size", _decimal(self.tick_size, "tick_size", positive=True))
        object.__setattr__(
            self, "quantity_step", _decimal(self.quantity_step, "quantity_step", positive=True)
        )
        object.__setattr__(
            self,
            "contract_multiplier",
            _decimal(self.contract_multiplier, "contract_multiplier", positive=True),
        )


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    """Read-only, explicitly complete-or-incomplete account evidence."""

    scope: ExecutionScope
    as_of_ns: int
    source: str
    completeness: str
    equity: Decimal | None = None
    available_margin: Decimal | None = None
    currency: str | None = None

    def __post_init__(self) -> None:
        if type(self.as_of_ns) is not int or self.as_of_ns <= 0:
            raise ContractValidationError("invalid as_of_ns")
        object.__setattr__(self, "source", _text(self.source, "source"))
        if self.completeness not in {"COMPLETE", "INCOMPLETE", "UNAVAILABLE"}:
            raise ContractValidationError("invalid completeness")
        if self.equity is not None:
            object.__setattr__(self, "equity", _decimal(self.equity, "equity"))
        if self.available_margin is not None:
            object.__setattr__(
                self, "available_margin", _decimal(self.available_margin, "available_margin")
            )
        if self.currency is not None:
            object.__setattr__(self, "currency", _text(self.currency, "currency"))


@dataclass(frozen=True, slots=True)
class ExecutionQualityRecord:
    """Provider-result quality facts; absent values remain absent rather than inferred."""

    intent_id: str
    as_of_ns: int
    source: str
    completeness: str
    latency_ms: Decimal | None = None
    fee: Decimal | None = None
    slippage: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "intent_id", _text(self.intent_id, "intent_id"))
        if type(self.as_of_ns) is not int or self.as_of_ns <= 0:
            raise ContractValidationError("invalid as_of_ns")
        object.__setattr__(self, "source", _text(self.source, "source"))
        if self.completeness not in {"COMPLETE", "INCOMPLETE", "UNAVAILABLE"}:
            raise ContractValidationError("invalid completeness")
        for name in ("latency_ms", "fee", "slippage"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _decimal(value, name))


@dataclass(frozen=True, slots=True)
class StrategyCheckpoint:
    """A strategy-owned recovery cursor that never replaces execution facts."""

    checkpoint_id: str
    scope: ExecutionScope
    signal_cursor: str
    state_version: str
    execution_cursor: int
    actor_id: str

    def __post_init__(self) -> None:
        for name in ("checkpoint_id", "signal_cursor", "state_version", "actor_id"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        if type(self.execution_cursor) is not int or self.execution_cursor < 0:
            raise ContractValidationError("invalid execution_cursor")


@dataclass(frozen=True, slots=True)
class ProviderObservation:
    """Normalized provider evidence applied after dispatch or reconciliation."""

    intent_id: str
    state: ExecutionState
    provider_order_id: str | None = None
    filled_quantity: Decimal = Decimal("0")
    average_price: Decimal | None = None
    reason_code: str | None = None
    cumulative_commission: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "intent_id", _text(self.intent_id, "intent_id"))
        try:
            object.__setattr__(self, "state", ExecutionState(self.state))
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid provider observation state") from error
        if self.state not in {
            ExecutionState.ACKED,
            ExecutionState.PARTIALLY_FILLED,
            ExecutionState.FILLED,
            ExecutionState.CANCELLED,
            ExecutionState.REJECTED,
        }:
            raise ContractValidationError("provider observation requires an evidenced state")
        object.__setattr__(
            self, "filled_quantity", _decimal(self.filled_quantity, "filled_quantity")
        )
        if self.average_price is not None:
            object.__setattr__(
                self, "average_price", _decimal(self.average_price, "average_price", positive=True)
            )
        # Commission is an observed cumulative venue fact.  It may be
        # negative for maker rebates, so unlike quantity and price it must not
        # be constrained to a positive value.  Keeping it on the normalized
        # observation lets a durable execution record later reconstruct a
        # framework fill without guessing a local commission rate.
        if self.cumulative_commission is not None:
            object.__setattr__(
                self,
                "cumulative_commission",
                _decimal(self.cumulative_commission, "cumulative_commission"),
            )
        if self.provider_order_id is not None:
            object.__setattr__(
                self, "provider_order_id", _text(self.provider_order_id, "provider_order_id")
            )
        if self.reason_code is not None and not _SAFE_REASON.fullmatch(self.reason_code):
            raise ContractValidationError("invalid reason_code")

    @classmethod
    def accepted(cls, intent_id: str, provider_order_id: str) -> ProviderObservation:
        return cls(intent_id, ExecutionState.ACKED, provider_order_id=provider_order_id)

    @classmethod
    def rejected(cls, intent_id: str, reason_code: str) -> ProviderObservation:
        return cls(intent_id, ExecutionState.REJECTED, reason_code=reason_code)


@dataclass(frozen=True, slots=True)
class CancelObservation:
    """Typed provider evidence for one durable :class:`CancelIntent`.

    ``ACKED`` means the provider confirmed the cancellation request but has
    not yet proved that the target order is cancelled.  ``CANCELLED`` is the
    only observation that advances the target execution record to its terminal
    cancellation state.  An absent, malformed, or mismatched identity is not
    representable here and must be recorded as ``UNKNOWN`` by the facade.
    """

    cancel_id: str
    target_intent_id: str
    provider_order_id: str
    state: ExecutionState
    reason_code: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "cancel_id", _text(self.cancel_id, "cancel_id"))
        object.__setattr__(
            self, "target_intent_id", _text(self.target_intent_id, "target_intent_id")
        )
        object.__setattr__(
            self, "provider_order_id", _text(self.provider_order_id, "provider_order_id")
        )
        try:
            object.__setattr__(self, "state", ExecutionState(self.state))
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid cancellation observation state") from error
        if self.state not in {
            ExecutionState.ACKED,
            ExecutionState.CANCELLED,
            ExecutionState.REJECTED,
        }:
            raise ContractValidationError("cancellation observation requires an evidenced state")
        if self.reason_code is not None and not _SAFE_REASON.fullmatch(self.reason_code):
            raise ContractValidationError("invalid cancellation reason_code")

    @classmethod
    def accepted(
        cls, cancel_id: str, target_intent_id: str, provider_order_id: str
    ) -> CancelObservation:
        """Build acknowledged cancellation evidence."""

        return cls(cancel_id, target_intent_id, provider_order_id, ExecutionState.ACKED)

    @classmethod
    def cancelled(
        cls, cancel_id: str, target_intent_id: str, provider_order_id: str
    ) -> CancelObservation:
        """Build terminal cancellation evidence."""

        return cls(cancel_id, target_intent_id, provider_order_id, ExecutionState.CANCELLED)

    @classmethod
    def rejected(
        cls, cancel_id: str, target_intent_id: str, provider_order_id: str, reason_code: str
    ) -> CancelObservation:
        """Build provider rejection evidence for one cancellation request."""

        return cls(
            cancel_id,
            target_intent_id,
            provider_order_id,
            ExecutionState.REJECTED,
            reason_code=reason_code,
        )


@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    """A replayable, non-secret outbox event."""

    sequence: int
    event_id: str
    intent_id: str
    scope_key: str
    event_type: str
    state: ExecutionState
    payload: Mapping[str, Any]
    created_at_ns: int

    def __post_init__(self) -> None:
        if type(self.sequence) is not int or self.sequence <= 0:
            raise ContractValidationError("invalid event sequence")
        object.__setattr__(self, "event_id", _text(self.event_id, "event_id"))
        object.__setattr__(self, "intent_id", _text(self.intent_id, "intent_id"))
        object.__setattr__(self, "scope_key", _text(self.scope_key, "scope_key"))
        object.__setattr__(self, "event_type", _text(self.event_type, "event_type"))
        if type(self.created_at_ns) is not int or self.created_at_ns <= 0:
            raise ContractValidationError("invalid event timestamp")


@dataclass(frozen=True, slots=True)
class CancelEvent:
    """A replayable, non-secret event emitted by the cancellation journal."""

    sequence: int
    event_id: str
    cancel_id: str
    target_intent_id: str
    scope_key: str
    event_type: str
    state: ExecutionState
    payload: Mapping[str, Any]
    created_at_ns: int

    def __post_init__(self) -> None:
        if type(self.sequence) is not int or self.sequence <= 0:
            raise ContractValidationError("invalid cancellation event sequence")
        object.__setattr__(self, "event_id", _text(self.event_id, "event_id"))
        object.__setattr__(self, "cancel_id", _text(self.cancel_id, "cancel_id"))
        object.__setattr__(
            self, "target_intent_id", _text(self.target_intent_id, "target_intent_id")
        )
        object.__setattr__(self, "scope_key", _text(self.scope_key, "scope_key"))
        object.__setattr__(self, "event_type", _text(self.event_type, "event_type"))
        try:
            object.__setattr__(self, "state", ExecutionState(self.state))
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid cancellation event state") from error
        if type(self.created_at_ns) is not int or self.created_at_ns <= 0:
            raise ContractValidationError("invalid cancellation event timestamp")
