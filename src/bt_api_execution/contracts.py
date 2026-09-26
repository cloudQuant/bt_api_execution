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
from types import MappingProxyType
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


_FACT_COMPLETENESS = frozenset({"COMPLETE", "PARTIAL", "INCOMPLETE", "UNAVAILABLE", "UNKNOWN"})
_ACCOUNT_VALUE_FIELDS = (
    "equity",
    "cash",
    "available_margin",
    "margin",
    "realized_pnl",
    "unrealized_pnl",
    "fee",
    "funding",
    "net_cashflow",
    "fx_rate",
    "settlement_price",
)
_QUALITY_VALUE_FIELDS = (
    "arrival_bid",
    "arrival_ask",
    "arrival_mid",
    "native_quantity",
    "contract_multiplier",
    "vwap",
    "fee",
    "slippage_amount",
    "slippage_bps",
    "stage_durations_ns",
)


def _economic_decimal(value: Decimal | int | str | None, field_name: str) -> Decimal | None:
    """Validate exact economic scalar input without accepting bool or float."""

    if value is None:
        return None
    if type(value) is bool or isinstance(value, float):
        raise ContractValidationError(f"invalid {field_name}")
    return _decimal(value, field_name)


def _optional_timestamp_ns(value: int | None, field_name: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value <= 0:
        raise ContractValidationError(f"invalid {field_name}")
    return value


def _fact_field_metadata(
    fields: tuple[str, ...],
    values: Mapping[str, object | None],
    completeness: Mapping[str, str],
    coverage_ns: Mapping[str, tuple[int, int]],
    source_refs: Mapping[str, tuple[str, ...]],
) -> tuple[
    Mapping[str, str],
    Mapping[str, tuple[int, int]],
    Mapping[str, tuple[str, ...]],
]:
    """Freeze and validate per-field provenance while keeping absent facts absent."""

    if not all(isinstance(value, Mapping) for value in (completeness, coverage_ns, source_refs)):
        raise ContractValidationError("invalid economic field metadata")
    allowed = set(fields)
    if any(set(mapping) - allowed for mapping in (completeness, coverage_ns, source_refs)):
        raise ContractValidationError("unknown economic field metadata")

    normalized_completeness: dict[str, str] = {}
    normalized_coverage: dict[str, tuple[int, int]] = {}
    normalized_source_refs: dict[str, tuple[str, ...]] = {}
    for name in fields:
        status = completeness.get(name, "INCOMPLETE" if values[name] is None else "UNKNOWN")
        if not isinstance(status, str) or status not in _FACT_COMPLETENESS:
            raise ContractValidationError(f"invalid {name} completeness")
        interval = coverage_ns.get(name)
        if interval is not None:
            if (
                not isinstance(interval, (tuple, list))
                or len(interval) != 2
                or type(interval[0]) is not int
                or type(interval[1]) is not int
                or interval[0] <= 0
                or interval[1] < interval[0]
            ):
                raise ContractValidationError(f"invalid {name} coverage")
            normalized_coverage[name] = (interval[0], interval[1])
        refs = source_refs.get(name, ())
        if not isinstance(refs, (tuple, list)) or isinstance(refs, str):
            raise ContractValidationError(f"invalid {name} source references")
        normalized_refs = tuple(_text(ref, f"{name} source reference") for ref in refs)
        if len(set(normalized_refs)) != len(normalized_refs):
            raise ContractValidationError(f"duplicate {name} source reference")
        if status == "COMPLETE":
            if values[name] is None:
                raise ContractValidationError(f"complete {name} is missing a value")
            if name not in normalized_coverage or not normalized_refs:
                raise ContractValidationError(f"complete {name} is missing coverage or source")
        normalized_completeness[name] = status
        if normalized_refs:
            normalized_source_refs[name] = normalized_refs

    return (
        MappingProxyType(normalized_completeness),
        MappingProxyType(normalized_coverage),
        MappingProxyType(normalized_source_refs),
    )


def _economic_scope_wire(
    scope: ExecutionScope,
    generation: str | None,
    epoch: int | None,
    *,
    strategy_attribution: bool,
    generation_kind: str | None,
) -> dict[str, Any]:
    if not isinstance(scope, ExecutionScope):
        raise ContractValidationError("economic fact scope is required")
    if generation_kind is not None:
        generation_kind = _text(generation_kind, "generation_kind")
        if generation_kind not in {"EXECUTION_JOURNAL", "PROVIDER_SESSION"}:
            raise ContractValidationError("invalid generation_kind")
    if generation is not None:
        generation = _text(generation, "generation")
    if (generation_kind is None) != (generation is None):
        raise ContractValidationError("generation kind and identity must be supplied together")
    if epoch is not None and (type(epoch) is not int or epoch <= 0):
        raise ContractValidationError("invalid economic fact epoch")
    if generation is None and epoch is not None:
        raise ContractValidationError("an unscoped generation cannot carry an epoch")
    if generation is not None and epoch is None:
        raise ContractValidationError("a scoped generation requires an epoch")
    return {
        "provider": scope.provider,
        "environment": scope.environment,
        "account_fingerprint": scope.account_key.partition(":")[2],
        "generation_kind": generation_kind,
        "generation": generation,
        "trading_day": scope.trading_day,
        "epoch": epoch,
        "strategy_id": scope.strategy_id if strategy_attribution else None,
    }


def _fact_effective_completeness(
    requested: str,
    field_completeness: Mapping[str, str],
    required_fields: tuple[str, ...],
    *,
    required_context_complete: bool,
) -> str:
    if requested == "UNAVAILABLE":
        return "UNAVAILABLE"
    if (
        requested == "COMPLETE"
        and required_context_complete
        and all(field_completeness[name] == "COMPLETE" for name in required_fields)
    ):
        return "COMPLETE"
    return "INCOMPLETE"


def _field_metadata_wire(
    fields: tuple[str, ...],
    completeness: Mapping[str, str],
    coverage_ns: Mapping[str, tuple[int, int]],
    source_refs: Mapping[str, tuple[str, ...]],
) -> dict[str, dict[str, Any]]:
    return {
        name: {
            "completeness": completeness[name],
            "coverage_start_ns": coverage_ns[name][0] if name in coverage_ns else None,
            "coverage_end_ns": coverage_ns[name][1] if name in coverage_ns else None,
            "source_refs": list(source_refs.get(name, ())),
        }
        for name in fields
    }


def _coverage_interval(value: tuple[int, int] | None, field_name: str) -> tuple[int, int] | None:
    if value is None:
        return None
    if (
        not isinstance(value, (tuple, list))
        or len(value) != 2
        or type(value[0]) is not int
        or type(value[1]) is not int
        or value[0] <= 0
        or value[1] < value[0]
    ):
        raise ContractValidationError(f"invalid {field_name} coverage")
    return (value[0], value[1])


def _source_references(value: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    if not isinstance(value, (tuple, list)) or isinstance(value, str):
        raise ContractValidationError(f"invalid {field_name} source references")
    normalized = tuple(_text(item, f"{field_name} source reference") for item in value)
    if len(normalized) != len(set(normalized)):
        raise ContractValidationError(f"duplicate {field_name} source reference")
    return normalized


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
    """Read-only account facts with an explicit versioned scalar wire form.

    The original positional fields remain first.  New provenance fields are
    additive so existing callers can keep constructing snapshots, while
    :meth:`to_wire` refuses to invent a generation or epoch and downgrades
    caller-supplied ``COMPLETE`` when any required field is unsupported.
    """

    scope: ExecutionScope
    as_of_ns: int
    source: str
    completeness: str
    equity: Decimal | None = None
    available_margin: Decimal | None = None
    currency: str | None = None
    generation: str | None = None
    epoch: int | None = None
    cash: Decimal | None = None
    margin: Decimal | None = None
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    fee: Decimal | None = None
    funding: Decimal | None = None
    net_cashflow: Decimal | None = None
    reporting_currency: str | None = None
    fx_rate: Decimal | None = None
    settlement_price: Decimal | None = None
    external_activity_attribution: str = "INCOMPLETE"
    equity_includes_fees: bool | None = None
    field_completeness: Mapping[str, str] = field(default_factory=dict)
    field_coverage_ns: Mapping[str, tuple[int, int]] = field(default_factory=dict)
    field_source_refs: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    external_activity_coverage_ns: tuple[int, int] | None = None
    external_activity_source_refs: tuple[str, ...] = ()
    generation_kind: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.scope, ExecutionScope):
            raise ContractValidationError("invalid account snapshot scope")
        if type(self.as_of_ns) is not int or self.as_of_ns <= 0:
            raise ContractValidationError("invalid as_of_ns")
        object.__setattr__(self, "source", _text(self.source, "source"))
        if self.completeness not in {"COMPLETE", "INCOMPLETE", "UNAVAILABLE"}:
            raise ContractValidationError("invalid completeness")
        for name in _ACCOUNT_VALUE_FIELDS:
            object.__setattr__(self, name, _economic_decimal(getattr(self, name), name))
        for name in ("currency", "reporting_currency"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _text(value, name))
        if self.generation is not None:
            object.__setattr__(self, "generation", _text(self.generation, "generation"))
        if self.generation_kind is not None:
            generation_kind = _text(self.generation_kind, "generation_kind")
            if generation_kind not in {"EXECUTION_JOURNAL", "PROVIDER_SESSION"}:
                raise ContractValidationError("invalid generation_kind")
            object.__setattr__(self, "generation_kind", generation_kind)
        object.__setattr__(self, "epoch", _optional_timestamp_ns(self.epoch, "epoch"))
        if self.external_activity_attribution not in _FACT_COMPLETENESS:
            raise ContractValidationError("invalid external activity attribution completeness")
        if self.equity_includes_fees is not None and type(self.equity_includes_fees) is not bool:
            raise ContractValidationError("invalid equity_includes_fees")
        values = {name: getattr(self, name) for name in _ACCOUNT_VALUE_FIELDS}
        completeness, coverage, source_refs = _fact_field_metadata(
            _ACCOUNT_VALUE_FIELDS,
            values,
            self.field_completeness,
            self.field_coverage_ns,
            self.field_source_refs,
        )
        object.__setattr__(self, "field_completeness", completeness)
        object.__setattr__(self, "field_coverage_ns", coverage)
        object.__setattr__(self, "field_source_refs", source_refs)
        if any(end > self.as_of_ns for _, end in coverage.values()):
            raise ContractValidationError("economic field coverage must not follow as_of_ns")
        external_coverage = _coverage_interval(
            self.external_activity_coverage_ns, "external activity"
        )
        external_refs = _source_references(self.external_activity_source_refs, "external activity")
        if self.external_activity_attribution == "COMPLETE" and (
            external_coverage is None or external_coverage[1] != self.as_of_ns or not external_refs
        ):
            raise ContractValidationError(
                "complete external activity attribution requires as_of coverage and source refs"
            )
        if external_coverage is not None and external_coverage[1] > self.as_of_ns:
            raise ContractValidationError("external activity coverage must not follow as_of_ns")
        object.__setattr__(self, "external_activity_coverage_ns", external_coverage)
        object.__setattr__(self, "external_activity_source_refs", external_refs)

    def to_wire(self) -> dict[str, Any]:
        """Return the strict ``bt_api.execution.account_snapshot.v2`` mapping."""

        scope = _economic_scope_wire(
            self.scope,
            self.generation,
            self.epoch,
            strategy_attribution=False,
            generation_kind=self.generation_kind,
        )
        values = {name: getattr(self, name) for name in _ACCOUNT_VALUE_FIELDS}
        scope_complete = scope["trading_day"] is not None
        context_complete = (
            scope_complete
            and scope["generation"] is not None
            and self.currency is not None
            and self.reporting_currency is not None
            and self.equity_includes_fees is not None
            and self.external_activity_attribution == "COMPLETE"
        )
        effective = _fact_effective_completeness(
            self.completeness,
            self.field_completeness,
            _ACCOUNT_VALUE_FIELDS,
            required_context_complete=context_complete,
        )
        return {
            "schema": "bt_api.execution.account_snapshot.v2",
            "fact_type": "account_snapshot",
            "scope": scope,
            "as_of_ns": self.as_of_ns,
            "source": self.source,
            "currency": self.currency,
            "reporting_currency": self.reporting_currency,
            "fx_rate_direction": (
                "reporting_currency_units_per_currency_unit"
                if self.currency is not None and self.reporting_currency is not None
                else None
            ),
            "completeness": effective,
            "values": {
                name: format(values[name], "f") if values[name] is not None else None
                for name in _ACCOUNT_VALUE_FIELDS
            },
            "field_evidence": _field_metadata_wire(
                _ACCOUNT_VALUE_FIELDS,
                self.field_completeness,
                self.field_coverage_ns,
                self.field_source_refs,
            ),
            "external_activity_attribution": self.external_activity_attribution,
            "external_activity_evidence": {
                "coverage_start_ns": (
                    self.external_activity_coverage_ns[0]
                    if self.external_activity_coverage_ns is not None
                    else None
                ),
                "coverage_end_ns": (
                    self.external_activity_coverage_ns[1]
                    if self.external_activity_coverage_ns is not None
                    else None
                ),
                "source_refs": list(self.external_activity_source_refs),
            },
            "equity_includes_fees": self.equity_includes_fees,
        }


@dataclass(frozen=True, slots=True)
class ExecutionQualityRecord:
    """Per-execution quality facts; absent values stay null and incomplete."""

    intent_id: str
    as_of_ns: int
    source: str
    completeness: str
    latency_ms: Decimal | None = None
    fee: Decimal | None = None
    slippage: Decimal | None = None
    scope: ExecutionScope | None = None
    generation: str | None = None
    epoch: int | None = None
    currency: str | None = None
    fee_currency: str | None = None
    signal_id: str | None = None
    child_id: str | None = None
    order_id: str | None = None
    trade_id: str | None = None
    arrival_bid: Decimal | None = None
    arrival_ask: Decimal | None = None
    arrival_mid: Decimal | None = None
    arrival_as_of_ns: int | None = None
    arrival_freshness_ns: int | None = None
    arrival_source: str | None = None
    side: str | Side | None = None
    native_quantity: Decimal | None = None
    contract_multiplier: Decimal | None = None
    vwap: Decimal | None = None
    slippage_amount: Decimal | None = None
    slippage_bps: Decimal | None = None
    stage_durations_ns: Mapping[str, int] = field(default_factory=dict)
    rejection_reason: str | None = None
    field_completeness: Mapping[str, str] = field(default_factory=dict)
    field_coverage_ns: Mapping[str, tuple[int, int]] = field(default_factory=dict)
    field_source_refs: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    generation_kind: str | None = None
    native_quantity_basis: str | None = None
    vwap_basis: str | None = None
    fee_basis: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "intent_id", _text(self.intent_id, "intent_id"))
        if type(self.as_of_ns) is not int or self.as_of_ns <= 0:
            raise ContractValidationError("invalid as_of_ns")
        object.__setattr__(self, "source", _text(self.source, "source"))
        if self.completeness not in {"COMPLETE", "INCOMPLETE", "UNAVAILABLE"}:
            raise ContractValidationError("invalid completeness")
        for name in (
            "latency_ms",
            "fee",
            "slippage",
            "arrival_bid",
            "arrival_ask",
            "arrival_mid",
            "native_quantity",
            "contract_multiplier",
            "vwap",
            "slippage_amount",
            "slippage_bps",
        ):
            value = _economic_decimal(getattr(self, name), name)
            if (
                value is not None
                and name
                in {
                    "arrival_bid",
                    "arrival_ask",
                    "arrival_mid",
                    "native_quantity",
                    "contract_multiplier",
                    "vwap",
                }
                and value <= 0
            ):
                raise ContractValidationError(f"invalid {name}")
            object.__setattr__(self, name, value)
        if self.scope is not None and not isinstance(self.scope, ExecutionScope):
            raise ContractValidationError("invalid execution quality scope")
        if self.generation is not None:
            object.__setattr__(self, "generation", _text(self.generation, "generation"))
        if self.generation_kind is not None:
            generation_kind = _text(self.generation_kind, "generation_kind")
            if generation_kind not in {"EXECUTION_JOURNAL", "PROVIDER_SESSION"}:
                raise ContractValidationError("invalid generation_kind")
            object.__setattr__(self, "generation_kind", generation_kind)
        for name in ("native_quantity_basis", "vwap_basis", "fee_basis"):
            basis = getattr(self, name)
            if basis is not None:
                basis = _text(basis, name)
                if basis not in {"TRADE", "ORDER_CUMULATIVE"}:
                    raise ContractValidationError(f"invalid {name}")
                object.__setattr__(self, name, basis)
        object.__setattr__(self, "epoch", _optional_timestamp_ns(self.epoch, "epoch"))
        for name in ("currency", "fee_currency", "arrival_source"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _text(value, name))
        for name in ("signal_id", "child_id", "order_id", "trade_id"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _text(value, name))
        if (
            any(
                basis == "TRADE"
                for basis in (self.native_quantity_basis, self.vwap_basis, self.fee_basis)
            )
            and self.trade_id is None
        ):
            raise ContractValidationError("TRADE measurement basis requires trade_id")
        for name in ("arrival_as_of_ns",):
            object.__setattr__(self, name, _optional_timestamp_ns(getattr(self, name), name))
        if self.arrival_as_of_ns is not None and self.arrival_as_of_ns > self.as_of_ns:
            raise ContractValidationError("arrival_as_of_ns must not be after the quality record")
        if self.arrival_freshness_ns is not None and (
            type(self.arrival_freshness_ns) is not int or self.arrival_freshness_ns < 0
        ):
            raise ContractValidationError("invalid arrival_freshness_ns")
        if self.side is not None:
            side = self.side.value if isinstance(self.side, Side) else self.side
            if side not in {Side.BUY.value, Side.SELL.value}:
                raise ContractValidationError("invalid execution quality side")
            object.__setattr__(self, "side", side)
        if self.rejection_reason is not None and not _SAFE_REASON.fullmatch(self.rejection_reason):
            raise ContractValidationError("invalid rejection_reason")
        if (self.slippage_amount is not None or self.slippage_bps is not None) and (
            self.arrival_mid is None or self.vwap is None
        ):
            raise ContractValidationError("slippage requires arrival_mid and vwap evidence")
        if not isinstance(self.stage_durations_ns, Mapping):
            raise ContractValidationError("invalid stage_durations_ns")
        durations: dict[str, int] = {}
        for name, duration in self.stage_durations_ns.items():
            normalized_name = _text(name, "stage duration name")
            if type(duration) is not int or duration < 0:
                raise ContractValidationError("invalid stage duration")
            durations[normalized_name] = duration
        if len(durations) != len(self.stage_durations_ns):
            raise ContractValidationError("duplicate stage duration name")
        object.__setattr__(self, "stage_durations_ns", MappingProxyType(durations))
        values: dict[str, object | None] = {
            name: getattr(self, name)
            for name in _QUALITY_VALUE_FIELDS
            if name != "stage_durations_ns"
        }
        values["stage_durations_ns"] = durations if durations else None
        field_completeness = dict(self.field_completeness)
        basis_for_field = {
            "native_quantity": self.native_quantity_basis,
            "vwap": self.vwap_basis,
            "fee": self.fee_basis,
        }
        for name, basis in basis_for_field.items():
            if (
                values[name] is not None
                and basis is None
                and field_completeness.get(name) == "COMPLETE"
            ):
                # A scalar without its measurement basis is not a complete
                # economic fact: consumers cannot tell whether to replace or
                # accumulate it. Keep the value and provenance for audit, but
                # prevent field-level COMPLETE from overstating its meaning.
                field_completeness[name] = "INCOMPLETE"
        completeness, coverage, source_refs = _fact_field_metadata(
            _QUALITY_VALUE_FIELDS,
            values,
            field_completeness,
            self.field_coverage_ns,
            self.field_source_refs,
        )
        object.__setattr__(self, "field_completeness", completeness)
        object.__setattr__(self, "field_coverage_ns", coverage)
        object.__setattr__(self, "field_source_refs", source_refs)
        if any(end > self.as_of_ns for _, end in coverage.values()):
            raise ContractValidationError("economic field coverage must not follow as_of_ns")

    def to_wire(self) -> dict[str, Any]:
        """Return the strict ``bt_api.execution.execution_quality.v2`` mapping."""

        if self.scope is None:
            raise ContractValidationError("execution quality scope is required for wire export")
        scope = _economic_scope_wire(
            self.scope,
            self.generation,
            self.epoch,
            strategy_attribution=True,
            generation_kind=self.generation_kind,
        )
        values = {
            name: getattr(self, name)
            for name in _QUALITY_VALUE_FIELDS
            if name != "stage_durations_ns"
        }
        values["stage_durations_ns"] = dict(self.stage_durations_ns) or None
        scope_complete = scope["trading_day"] is not None
        context_complete = (
            scope_complete
            and self.currency is not None
            and (self.fee is None or self.fee_currency is not None)
            and self.arrival_as_of_ns is not None
            and self.arrival_freshness_ns is not None
            and self.arrival_source is not None
            and self.side is not None
            and scope["generation"] is not None
            and self.native_quantity_basis is not None
            and self.vwap_basis is not None
            and (self.fee is None or self.fee_basis is not None)
            and bool(self.signal_id and self.child_id and self.order_id)
            and (self.trade_id is not None or self.rejection_reason is not None)
            and bool(self.stage_durations_ns)
            and (self.latency_ms is None and self.slippage is None)
        )
        effective = _fact_effective_completeness(
            self.completeness,
            self.field_completeness,
            _QUALITY_VALUE_FIELDS,
            required_context_complete=context_complete,
        )
        return {
            "schema": "bt_api.execution.execution_quality.v2",
            "fact_type": "execution_quality",
            "scope": scope,
            "intent_id": self.intent_id,
            "as_of_ns": self.as_of_ns,
            "source": self.source,
            "currency": self.currency,
            "completeness": effective,
            "lineage": {
                "signal_id": self.signal_id,
                "intent_id": self.intent_id,
                "child_id": self.child_id,
                "order_id": self.order_id,
                "trade_id": self.trade_id,
            },
            "arrival": {
                "bid": format(self.arrival_bid, "f") if self.arrival_bid is not None else None,
                "ask": format(self.arrival_ask, "f") if self.arrival_ask is not None else None,
                "mid": format(self.arrival_mid, "f") if self.arrival_mid is not None else None,
                "as_of_ns": self.arrival_as_of_ns,
                "freshness_ns": self.arrival_freshness_ns,
                "source": self.arrival_source,
            },
            "execution": {
                "side": self.side,
                "native_quantity": (
                    format(self.native_quantity, "f") if self.native_quantity is not None else None
                ),
                "native_quantity_basis": self.native_quantity_basis,
                "contract_multiplier": (
                    format(self.contract_multiplier, "f")
                    if self.contract_multiplier is not None
                    else None
                ),
                "vwap": format(self.vwap, "f") if self.vwap is not None else None,
                "vwap_basis": self.vwap_basis,
                "fee": format(self.fee, "f") if self.fee is not None else None,
                "fee_basis": self.fee_basis,
                "fee_currency": self.fee_currency,
                "slippage_amount": (
                    format(self.slippage_amount, "f") if self.slippage_amount is not None else None
                ),
                "slippage_bps": (
                    format(self.slippage_bps, "f") if self.slippage_bps is not None else None
                ),
                "slippage_sign_convention": "positive_is_adverse",
                "stage_durations_ns": dict(self.stage_durations_ns),
                "stage_durations_clock": "monotonic",
                "rejection_reason": self.rejection_reason,
                "legacy_metrics": {
                    "latency_ms_unscoped": (
                        format(self.latency_ms, "f") if self.latency_ms is not None else None
                    ),
                    "slippage_untyped": (
                        format(self.slippage, "f") if self.slippage is not None else None
                    ),
                },
            },
            "field_evidence": _field_metadata_wire(
                _QUALITY_VALUE_FIELDS,
                self.field_completeness,
                self.field_coverage_ns,
                self.field_source_refs,
            ),
        }


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
    journal_incarnation_id: str | None = None

    def __post_init__(self) -> None:
        if type(self.sequence) is not int or self.sequence <= 0:
            raise ContractValidationError("invalid event sequence")
        object.__setattr__(self, "event_id", _text(self.event_id, "event_id"))
        object.__setattr__(self, "intent_id", _text(self.intent_id, "intent_id"))
        object.__setattr__(self, "scope_key", _text(self.scope_key, "scope_key"))
        object.__setattr__(self, "event_type", _text(self.event_type, "event_type"))
        if self.journal_incarnation_id is not None and (
            not isinstance(self.journal_incarnation_id, str)
            or len(self.journal_incarnation_id) != 32
            or any(character not in "0123456789abcdef" for character in self.journal_incarnation_id)
        ):
            raise ContractValidationError("invalid journal_incarnation_id")
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
