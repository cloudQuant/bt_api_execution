"""Versioned, owner-scoped cash, cost-basis, and valuation facts.

The account book is derived from an explicit trusted baseline plus immutable
financial events and an independent provider account snapshot.  This module
contains the wire/domain contracts and deterministic projection rules; it does
not treat its typed inputs as proof that an upstream source was trusted.  The
account-owner composition must obtain these records from code-owned resources
and keep mutations off client RPCs.

V1 valuation deliberately supports spot assets only.  Other products require
their own settlement/equity model before they can produce a complete value.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum

from .contracts import ExecutionEvent, ExecutionState, OrderIntent, Side, payload_sha256
from .errors import ContractValidationError

_HEX = frozenset("0123456789abcdef")
_UNALLOCATED_REF = "UNALLOCATED"
_REQUIRED_LEDGER_SOURCES = frozenset({"execution_outbox", "account_facts", "account_activity"})
_REQUIRED_VIEW_SOURCES = frozenset({"market_marks", "fx_rates"})
_FINANCIAL_EXECUTION_EVENTS = frozenset(
    {"provider_observation", "reconciled_observation", "provider_commission_evidence"}
)


class FinancialLedgerError(ContractValidationError):
    """Invalid or conflicting trusted financial facts."""


class FinancialDataUnavailableError(FinancialLedgerError):
    """Required source facts, provenance, or freshness are missing."""


class FinancialOwnerKind(StrEnum):
    STRATEGY = "STRATEGY"
    MANUAL_BOOK = "MANUAL_BOOK"
    UNALLOCATED = "UNALLOCATED"


class FinancialSourceKind(StrEnum):
    EXECUTION_OUTBOX = "execution_outbox"
    ACCOUNT_FACTS = "account_facts"
    ACCOUNT_ACTIVITY = "account_activity"
    MARKET_MARKS = "market_marks"
    FX_RATES = "fx_rates"


class FinancialEventKind(StrEnum):
    CASH_FLOW = "CASH_FLOW"
    SPOT_FILL = "SPOT_FILL"


class FinancialCostMethod(StrEnum):
    AVERAGE_COST = "AVERAGE_COST"


class FinancialProductModel(StrEnum):
    SPOT = "SPOT"


def _text(value: object, name: str, *, max_length: int = 256) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > max_length
        or not value.isascii()
        or any(ord(char) < 0x21 for char in value)
    ):
        raise FinancialLedgerError(f"invalid {name}")
    return value


def _digest_id(value: object, name: str) -> str:
    text = _text(value, name, max_length=64)
    if len(text) != 32 or any(char not in _HEX for char in text):
        raise FinancialLedgerError(f"invalid {name}")
    return text


def _sha256(value: object, name: str) -> str:
    text = _text(value, name, max_length=64)
    if len(text) != 64 or any(char not in _HEX for char in text):
        raise FinancialLedgerError(f"invalid {name}")
    return text


def _decimal(value: object, name: str, *, positive: bool = False) -> Decimal:
    if type(value) is bool or not isinstance(value, (Decimal, int, str)):
        raise FinancialLedgerError(f"invalid {name}")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except (InvalidOperation, ValueError):
        raise FinancialLedgerError(f"invalid {name}") from None
    if not result.is_finite() or (positive and result <= 0):
        raise FinancialLedgerError(f"invalid {name}")
    return result


def _timestamp(value: object, name: str, *, allow_zero: bool = False) -> int:
    if type(value) is not int or value < (0 if allow_zero else 1):
        raise FinancialLedgerError(f"invalid {name}")
    return value


def _cursor(value: object, name: str) -> int:
    return _timestamp(value, name, allow_zero=True)


@dataclass(frozen=True, slots=True)
class FinancialOwnerV1:
    """One trusted registered owner or the explicit account residual bucket."""

    kind: FinancialOwnerKind
    owner_ref: str
    execution_scope_key: str | None

    def __post_init__(self) -> None:
        try:
            kind = FinancialOwnerKind(self.kind)
        except (TypeError, ValueError):
            raise FinancialLedgerError("invalid financial owner kind") from None
        owner_ref = _text(self.owner_ref, "financial owner ref")
        if kind is FinancialOwnerKind.UNALLOCATED:
            if owner_ref != _UNALLOCATED_REF or self.execution_scope_key is not None:
                raise FinancialLedgerError("invalid UNALLOCATED owner")
        elif kind is FinancialOwnerKind.STRATEGY:
            _text(self.execution_scope_key, "financial owner execution scope")
            if owner_ref == _UNALLOCATED_REF:
                raise FinancialLedgerError("registered owner uses reserved ref")
        else:
            if self.execution_scope_key is not None:
                _text(self.execution_scope_key, "manual book source scope")
            if owner_ref == _UNALLOCATED_REF:
                raise FinancialLedgerError("registered owner uses reserved ref")
        object.__setattr__(self, "kind", kind)

    @property
    def key(self) -> tuple[str, str]:
        return self.kind.value, self.owner_ref


@dataclass(frozen=True, slots=True)
class FinancialOwnerDirectoryV1:
    """Full, unfiltered owner directory supplied by trusted composition."""

    account_key: str
    revision: str
    source_ref: str
    owners: tuple[FinancialOwnerV1, ...]
    page_exhausted: bool = True
    filters_applied: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_key", _text(self.account_key, "account key"))
        object.__setattr__(self, "revision", _text(self.revision, "owner directory revision"))
        object.__setattr__(self, "source_ref", _text(self.source_ref, "owner directory source"))
        if type(self.page_exhausted) is not bool or type(self.filters_applied) is not bool:
            raise FinancialLedgerError("invalid owner directory coverage")
        owners = tuple(self.owners)
        if not owners or any(type(item) is not FinancialOwnerV1 for item in owners):
            raise FinancialLedgerError("trusted owner directory is empty or invalid")
        if any(item.kind is FinancialOwnerKind.UNALLOCATED for item in owners):
            raise FinancialLedgerError("UNALLOCATED is not a registered owner")
        if len({item.key for item in owners}) != len(owners):
            raise FinancialLedgerError("duplicate financial owner")
        scopes = [
            item.execution_scope_key for item in owners if item.execution_scope_key is not None
        ]
        if len(set(scopes)) != len(scopes):
            raise FinancialLedgerError("duplicate financial owner scope")
        object.__setattr__(self, "owners", owners)

    @property
    def complete(self) -> bool:
        return self.page_exhausted and not self.filters_applied

    def owner(self, key: tuple[str, str]) -> FinancialOwnerV1 | None:
        return next((item for item in self.owners if item.key == key), None)


@dataclass(frozen=True, slots=True)
class FinancialWatermarkV1:
    """One source generation and inclusive sequence cursor in a read cut."""

    source_kind: FinancialSourceKind
    source_ref: str
    generation: str
    cursor: int
    as_of_ns: int

    def __post_init__(self) -> None:
        try:
            kind = FinancialSourceKind(self.source_kind)
        except (TypeError, ValueError):
            raise FinancialLedgerError("invalid financial source kind") from None
        object.__setattr__(self, "source_kind", kind)
        object.__setattr__(self, "source_ref", _text(self.source_ref, "financial source ref"))
        object.__setattr__(self, "generation", _text(self.generation, "source generation"))
        object.__setattr__(self, "cursor", _cursor(self.cursor, "source cursor"))
        object.__setattr__(self, "as_of_ns", _timestamp(self.as_of_ns, "source as_of"))


@dataclass(frozen=True, slots=True)
class FinancialLedgerCutV1:
    """Exact owner-directory, execution, provider, and activity read vector."""

    account_key: str
    owner_directory_revision: str
    captured_at_ns: int
    watermarks: tuple[FinancialWatermarkV1, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_key", _text(self.account_key, "account key"))
        object.__setattr__(
            self,
            "owner_directory_revision",
            _text(self.owner_directory_revision, "owner directory revision"),
        )
        object.__setattr__(self, "captured_at_ns", _timestamp(self.captured_at_ns, "cut time"))
        watermarks = tuple(self.watermarks)
        if any(type(item) is not FinancialWatermarkV1 for item in watermarks):
            raise FinancialLedgerError("invalid source vector")
        kinds = [item.source_kind.value for item in watermarks]
        if len(set(kinds)) != len(kinds) or set(kinds) != _REQUIRED_LEDGER_SOURCES:
            raise FinancialDataUnavailableError("required ledger source vector is incomplete")
        if any(item.as_of_ns > self.captured_at_ns for item in watermarks):
            raise FinancialLedgerError("source cut follows its capture time")
        object.__setattr__(self, "watermarks", watermarks)

    def watermark(self, kind: FinancialSourceKind) -> FinancialWatermarkV1:
        for item in self.watermarks:
            if item.source_kind is kind:
                return item
        raise FinancialDataUnavailableError("required source watermark is missing")


@dataclass(frozen=True, slots=True)
class FinancialViewCutV1:
    """Ledger vector plus the exact mark and FX source vector for one value."""

    ledger: FinancialLedgerCutV1
    valuation_watermarks: tuple[FinancialWatermarkV1, ...]

    def __post_init__(self) -> None:
        if type(self.ledger) is not FinancialLedgerCutV1:
            raise FinancialLedgerError("invalid financial ledger cut")
        values = tuple(self.valuation_watermarks)
        if any(type(item) is not FinancialWatermarkV1 for item in values):
            raise FinancialLedgerError("invalid valuation source vector")
        kinds = [item.source_kind.value for item in values]
        if len(set(kinds)) != len(kinds) or set(kinds) != _REQUIRED_VIEW_SOURCES:
            raise FinancialDataUnavailableError("required valuation vector is incomplete")
        if any(item.as_of_ns > self.ledger.captured_at_ns for item in values):
            raise FinancialLedgerError("valuation source cut follows capture time")
        object.__setattr__(self, "valuation_watermarks", values)

    def watermark(self, kind: FinancialSourceKind) -> FinancialWatermarkV1:
        if kind in {FinancialSourceKind.MARKET_MARKS, FinancialSourceKind.FX_RATES}:
            for item in self.valuation_watermarks:
                if item.source_kind is kind:
                    return item
        return self.ledger.watermark(kind)


@dataclass(frozen=True, slots=True)
class CurrencyAmountV1:
    currency: str
    amount: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "currency", _text(self.currency, "currency", max_length=16))
        object.__setattr__(self, "amount", _decimal(self.amount, "currency amount"))


@dataclass(frozen=True, slots=True)
class OwnerPositionCostV1:
    """One explicit average-cost baseline; unknown cost is represented as None."""

    instrument: str
    quantity: Decimal
    cost_basis: Decimal | None
    cost_currency: str | None
    cost_method: FinancialCostMethod = FinancialCostMethod.AVERAGE_COST
    product_model: FinancialProductModel = FinancialProductModel.SPOT
    metadata_version: str = "v1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument", _text(self.instrument, "instrument"))
        quantity = _decimal(self.quantity, "position quantity")
        if quantity < 0:
            raise FinancialLedgerError("spot baseline quantity cannot be short")
        object.__setattr__(self, "quantity", quantity)
        try:
            method = FinancialCostMethod(self.cost_method)
            model = FinancialProductModel(self.product_model)
        except (TypeError, ValueError):
            raise FinancialLedgerError("unsupported cost or product model") from None
        object.__setattr__(self, "cost_method", method)
        object.__setattr__(self, "product_model", model)
        object.__setattr__(
            self, "metadata_version", _text(self.metadata_version, "metadata version")
        )
        if self.cost_basis is None:
            if quantity == 0:
                raise FinancialLedgerError("zero position requires an explicit zero cost basis")
            if self.cost_currency is not None:
                raise FinancialLedgerError("unknown cost basis cannot declare a currency")
        else:
            basis = _decimal(self.cost_basis, "position cost basis")
            if basis < 0 or (quantity == 0 and basis != 0) or (quantity > 0 and basis == 0):
                raise FinancialLedgerError("position cost basis does not match quantity")
            object.__setattr__(self, "cost_basis", basis)
            object.__setattr__(
                self, "cost_currency", _text(self.cost_currency, "cost currency", max_length=16)
            )


@dataclass(frozen=True, slots=True)
class FinancialOwnerBaselineV1:
    owner: FinancialOwnerV1
    cash: tuple[CurrencyAmountV1, ...]
    positions: tuple[OwnerPositionCostV1, ...]
    cumulative_fees: tuple[CurrencyAmountV1, ...] = ()

    def __post_init__(self) -> None:
        if (
            type(self.owner) is not FinancialOwnerV1
            or self.owner.kind is FinancialOwnerKind.UNALLOCATED
        ):
            raise FinancialLedgerError("baseline owner must be registered")
        cash = tuple(self.cash)
        positions = tuple(self.positions)
        fees = tuple(self.cumulative_fees)
        if not cash or any(type(item) is not CurrencyAmountV1 for item in cash):
            raise FinancialLedgerError("baseline must explicitly declare cash balances")
        if any(type(item) is not OwnerPositionCostV1 for item in positions):
            raise FinancialLedgerError("invalid baseline position cost")
        if any(type(item) is not CurrencyAmountV1 for item in fees):
            raise FinancialLedgerError("invalid baseline fee total")
        if len({item.currency for item in cash}) != len(cash):
            raise FinancialLedgerError("duplicate owner baseline cash currency")
        if len({item.currency for item in fees}) != len(fees):
            raise FinancialLedgerError("duplicate owner baseline fee currency")
        if len({item.instrument for item in positions}) != len(positions):
            raise FinancialLedgerError("duplicate owner baseline position")
        object.__setattr__(self, "cash", cash)
        object.__setattr__(self, "positions", positions)
        object.__setattr__(self, "cumulative_fees", fees)


@dataclass(frozen=True, slots=True)
class ProviderAccountFinancialSnapshotV1:
    """Independent complete provider totals used to calculate the residual."""

    account_key: str
    source_ref: str
    generation: str
    cursor: int
    as_of_ns: int
    cash: tuple[CurrencyAmountV1, ...]
    positions: tuple[tuple[str, Decimal], ...]
    cumulative_fees: tuple[CurrencyAmountV1, ...]
    cash_universe_complete: bool = True
    positions_complete: bool = True
    fees_complete: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_key", _text(self.account_key, "account key"))
        object.__setattr__(self, "source_ref", _text(self.source_ref, "account snapshot source"))
        object.__setattr__(
            self, "generation", _text(self.generation, "account snapshot generation")
        )
        object.__setattr__(self, "cursor", _cursor(self.cursor, "account snapshot cursor"))
        object.__setattr__(self, "as_of_ns", _timestamp(self.as_of_ns, "account snapshot as_of"))
        cash = tuple(self.cash)
        positions = tuple(
            ((_text(row[0], "instrument"), _decimal(row[1], "account position")))
            for row in self.positions
        )
        fees = tuple(self.cumulative_fees)
        if any(type(item) is not CurrencyAmountV1 for item in cash + fees):
            raise FinancialLedgerError("invalid provider account money facts")
        for flag in (self.cash_universe_complete, self.positions_complete, self.fees_complete):
            if type(flag) is not bool:
                raise FinancialLedgerError("invalid provider account completeness")
        if len({item.currency for item in cash}) != len(cash):
            raise FinancialLedgerError("duplicate provider cash currency")
        if len({item.currency for item in fees}) != len(fees):
            raise FinancialLedgerError("duplicate provider fee currency")
        if len({instrument for instrument, _quantity in positions}) != len(positions):
            raise FinancialLedgerError("duplicate provider account position")
        object.__setattr__(self, "cash", cash)
        object.__setattr__(self, "positions", positions)
        object.__setattr__(self, "cumulative_fees", fees)

    @property
    def complete(self) -> bool:
        return self.cash_universe_complete and self.positions_complete and self.fees_complete


@dataclass(frozen=True, slots=True)
class AccountFinancialBaselineV1:
    """Immutable authorized owner seed at an exact account facts cut."""

    account_key: str
    baseline_ref: str
    version: int
    authority_ref: str
    source_ref: str
    owner_directory_revision: str
    cut: FinancialLedgerCutV1
    provider_snapshot: ProviderAccountFinancialSnapshotV1
    owners: tuple[FinancialOwnerBaselineV1, ...]
    execution_cursors: FinancialExecutionCursorSnapshotV1 | None = None

    def __post_init__(self) -> None:
        for name in (
            "account_key",
            "baseline_ref",
            "authority_ref",
            "source_ref",
            "owner_directory_revision",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        if type(self.version) is not int or self.version <= 0:
            raise FinancialLedgerError("invalid baseline version")
        if (
            type(self.cut) is not FinancialLedgerCutV1
            or type(self.provider_snapshot) is not ProviderAccountFinancialSnapshotV1
        ):
            raise FinancialLedgerError("invalid financial baseline cut")
        owners = tuple(self.owners)
        if not owners or any(type(item) is not FinancialOwnerBaselineV1 for item in owners):
            raise FinancialLedgerError("financial baseline requires explicit owner rows")
        if len({item.owner.key for item in owners}) != len(owners):
            raise FinancialLedgerError("duplicate financial baseline owner")
        object.__setattr__(self, "owners", owners)
        execution_watermark = self.cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX)
        cursor_snapshot = self.execution_cursors
        if cursor_snapshot is None and execution_watermark.cursor == 0:
            cursor_snapshot = FinancialExecutionCursorSnapshotV1(
                journal_generation=execution_watermark.generation,
                cursor=0,
                as_of_ns=execution_watermark.as_of_ns,
                orders=(),
            )
            object.__setattr__(self, "execution_cursors", cursor_snapshot)
        if type(cursor_snapshot) is not FinancialExecutionCursorSnapshotV1:
            raise FinancialDataUnavailableError(
                "baseline requires a complete cumulative execution cursor snapshot"
            )
        if (
            cursor_snapshot.journal_generation != execution_watermark.generation
            or cursor_snapshot.cursor != execution_watermark.cursor
            or cursor_snapshot.as_of_ns != execution_watermark.as_of_ns
            or not cursor_snapshot.complete
        ):
            raise FinancialDataUnavailableError(
                "baseline execution cursors do not bind the exact source cut"
            )
        self.validate_against_directory(
            FinancialOwnerDirectoryV1(
                self.account_key,
                self.owner_directory_revision,
                self.source_ref,
                tuple(item.owner for item in owners),
            )
        )

    def validate_against_directory(self, directory: FinancialOwnerDirectoryV1) -> None:
        if (
            type(directory) is not FinancialOwnerDirectoryV1
            or not directory.complete
            or directory.account_key != self.account_key
            or directory.revision != self.owner_directory_revision
            or {item.key for item in directory.owners} != {item.owner.key for item in self.owners}
        ):
            raise FinancialDataUnavailableError(
                "baseline owner directory is not authoritative or complete"
            )
        if (
            self.cut.account_key != self.account_key
            or self.cut.owner_directory_revision != directory.revision
            or self.provider_snapshot.account_key != self.account_key
            or self.provider_snapshot.generation
            != self.cut.watermark(FinancialSourceKind.ACCOUNT_FACTS).generation
            or self.provider_snapshot.cursor
            != self.cut.watermark(FinancialSourceKind.ACCOUNT_FACTS).cursor
            or self.provider_snapshot.as_of_ns
            != self.cut.watermark(FinancialSourceKind.ACCOUNT_FACTS).as_of_ns
        ):
            raise FinancialDataUnavailableError("baseline does not bind the current provider cut")
        if not self.provider_snapshot.complete:
            raise FinancialDataUnavailableError("baseline provider totals are incomplete")
        cursor_snapshot = self.execution_cursors
        assert cursor_snapshot is not None
        if any(directory.owner(row.owner.key) != row.owner for row in cursor_snapshot.orders):
            raise FinancialDataUnavailableError(
                "baseline execution cursor names an unregistered owner"
            )


@dataclass(frozen=True, slots=True)
class FinancialInstrumentSpecV1:
    """Trusted unit/currency metadata for the intentionally narrow spot model."""

    instrument: str
    metadata_version: str
    product_model: FinancialProductModel
    base_currency: str
    quote_currency: str
    multiplier: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument", _text(self.instrument, "instrument"))
        object.__setattr__(
            self, "metadata_version", _text(self.metadata_version, "metadata version")
        )
        try:
            model = FinancialProductModel(self.product_model)
        except (TypeError, ValueError):
            raise FinancialDataUnavailableError("unsupported instrument accounting model") from None
        object.__setattr__(self, "product_model", model)
        object.__setattr__(
            self, "base_currency", _text(self.base_currency, "base currency", max_length=16)
        )
        object.__setattr__(
            self, "quote_currency", _text(self.quote_currency, "quote currency", max_length=16)
        )
        multiplier = _decimal(self.multiplier, "instrument multiplier", positive=True)
        if multiplier != Decimal("1"):
            raise FinancialDataUnavailableError("spot V1 requires unit quantity multiplier")
        object.__setattr__(self, "multiplier", multiplier)


@dataclass(frozen=True, slots=True)
class FinancialExecutionCursorV1:
    filled_quantity: Decimal = Decimal("0")
    average_price: Decimal | None = None
    cumulative_commission: Decimal | None = None
    commission_currency: str | None = None
    provider_order_id: str | None = None

    def __post_init__(self) -> None:
        quantity = _decimal(self.filled_quantity, "execution filled quantity")
        if quantity < 0:
            raise FinancialLedgerError("execution filled quantity cannot be negative")
        object.__setattr__(self, "filled_quantity", quantity)
        if self.average_price is not None:
            object.__setattr__(
                self, "average_price", _decimal(self.average_price, "average price", positive=True)
            )
        if self.cumulative_commission is not None:
            object.__setattr__(
                self,
                "cumulative_commission",
                _decimal(self.cumulative_commission, "cumulative commission"),
            )
        if self.commission_currency is not None:
            if self.cumulative_commission is None:
                raise FinancialLedgerError("commission currency has no amount")
            object.__setattr__(
                self,
                "commission_currency",
                _text(self.commission_currency, "commission currency", max_length=16),
            )
        if self.provider_order_id is not None:
            object.__setattr__(
                self, "provider_order_id", _text(self.provider_order_id, "provider order id")
            )


@dataclass(frozen=True, slots=True)
class FinancialOrderBaselineCursorV1:
    """Trusted latest cumulative observation for one immutable managed order."""

    owner: FinancialOwnerV1
    scope_key: str
    intent_id: str
    intent_payload_sha256: str
    cursor: FinancialExecutionCursorV1

    def __post_init__(self) -> None:
        if (
            type(self.owner) is not FinancialOwnerV1
            or self.owner.kind is not FinancialOwnerKind.STRATEGY
            or self.owner.execution_scope_key != self.scope_key
        ):
            raise FinancialLedgerError("baseline execution cursor owner is invalid")
        object.__setattr__(self, "scope_key", _text(self.scope_key, "execution scope key"))
        object.__setattr__(self, "intent_id", _text(self.intent_id, "execution intent id"))
        object.__setattr__(
            self,
            "intent_payload_sha256",
            _sha256(self.intent_payload_sha256, "baseline intent payload fingerprint"),
        )
        if type(self.cursor) is not FinancialExecutionCursorV1:
            raise FinancialLedgerError("baseline order cursor is invalid")


@dataclass(frozen=True, slots=True)
class FinancialExecutionCursorSnapshotV1:
    """Complete account-wide cumulative order state at one outbox high-water."""

    journal_generation: str
    cursor: int
    as_of_ns: int
    orders: tuple[FinancialOrderBaselineCursorV1, ...]
    page_exhausted: bool = True
    scope_filter_applied: bool = False

    def __post_init__(self) -> None:
        _digest_id(self.journal_generation, "execution cursor snapshot generation")
        object.__setattr__(self, "cursor", _cursor(self.cursor, "execution cursor snapshot cursor"))
        object.__setattr__(
            self, "as_of_ns", _timestamp(self.as_of_ns, "execution cursor snapshot time")
        )
        orders = tuple(self.orders)
        if any(type(item) is not FinancialOrderBaselineCursorV1 for item in orders):
            raise FinancialLedgerError("invalid baseline execution cursor row")
        if len({(item.scope_key, item.intent_id) for item in orders}) != len(orders):
            raise FinancialLedgerError("duplicate baseline execution cursor row")
        if type(self.page_exhausted) is not bool or type(self.scope_filter_applied) is not bool:
            raise FinancialLedgerError("invalid baseline execution cursor coverage")
        object.__setattr__(
            self, "orders", tuple(sorted(orders, key=lambda row: (row.scope_key, row.intent_id)))
        )

    @property
    def complete(self) -> bool:
        return self.page_exhausted and not self.scope_filter_applied


@dataclass(frozen=True, slots=True)
class FinancialExecutionPostingV1:
    """Money/position delta derived from one durable Execution outbox event."""

    event_id: str
    event_sequence: int
    journal_generation: str
    scope_key: str
    intent_id: str
    provider_order_id: str
    source_payload_sha256: str
    owner: FinancialOwnerV1
    occurred_at_ns: int
    kind: FinancialEventKind
    instrument: str | None
    signed_quantity: Decimal
    price: Decimal | None
    principal_currency: str | None
    principal_cash_delta: Decimal
    fee_currency: str | None
    fee_delta: Decimal

    def __post_init__(self) -> None:
        _digest_id(self.event_id, "execution event id")
        _cursor(self.event_sequence, "execution event sequence")
        _digest_id(self.journal_generation, "execution journal generation")
        object.__setattr__(self, "scope_key", _text(self.scope_key, "execution scope key"))
        object.__setattr__(self, "intent_id", _text(self.intent_id, "intent id"))
        object.__setattr__(
            self, "provider_order_id", _text(self.provider_order_id, "provider order id")
        )
        object.__setattr__(
            self,
            "source_payload_sha256",
            _sha256(self.source_payload_sha256, "execution payload fingerprint"),
        )
        if (
            type(self.owner) is not FinancialOwnerV1
            or self.owner.kind is FinancialOwnerKind.UNALLOCATED
        ):
            raise FinancialLedgerError("managed fill requires a registered owner")
        _timestamp(self.occurred_at_ns, "execution event time")
        try:
            kind = FinancialEventKind(self.kind)
        except (TypeError, ValueError):
            raise FinancialLedgerError("invalid execution financial event") from None
        object.__setattr__(self, "kind", kind)
        signed_quantity = _decimal(self.signed_quantity, "signed fill quantity")
        principal_delta = _decimal(self.principal_cash_delta, "fill principal cash delta")
        fee_delta = _decimal(self.fee_delta, "fee delta")
        object.__setattr__(self, "signed_quantity", signed_quantity)
        object.__setattr__(self, "principal_cash_delta", principal_delta)
        object.__setattr__(self, "fee_delta", fee_delta)
        if kind is FinancialEventKind.CASH_FLOW:
            if self.instrument is not None or self.price is not None or signed_quantity != 0:
                raise FinancialLedgerError("cash-flow posting includes fill data")
        else:
            if self.instrument is None or self.price is None or self.principal_currency is None:
                raise FinancialLedgerError("fill posting omits price or principal currency")
            object.__setattr__(self, "instrument", _text(self.instrument, "instrument"))
            object.__setattr__(self, "price", _decimal(self.price, "fill price", positive=True))
            object.__setattr__(
                self,
                "principal_currency",
                _text(self.principal_currency, "principal currency", max_length=16),
            )
            if signed_quantity == 0:
                raise FinancialLedgerError("fill posting has zero quantity")
        if self.fee_currency is not None:
            object.__setattr__(
                self, "fee_currency", _text(self.fee_currency, "fee currency", max_length=16)
            )
        if fee_delta != 0 and self.fee_currency is None:
            raise FinancialDataUnavailableError("fee amount has no trusted currency")


@dataclass(frozen=True, slots=True)
class FinancialExecutionBatchV1:
    """Complete, unfiltered outbox scan bound to one journal cursor interval.

    ``scanned_row_count`` counts every outbox row, including non-economic and
    other-account rows.  The durable store constructs this value while it
    holds its transaction and checks the journal's global AUTOINCREMENT
    bounds; callers must not derive completeness from a filtered page.
    """

    journal_generation: str
    from_cursor: int
    through_cursor: int
    scanned_row_count: int
    postings: tuple[FinancialExecutionPostingV1, ...]
    page_exhausted: bool = True
    scope_filter_applied: bool = False

    def __post_init__(self) -> None:
        _digest_id(self.journal_generation, "execution batch journal generation")
        object.__setattr__(self, "from_cursor", _cursor(self.from_cursor, "execution batch start"))
        object.__setattr__(
            self, "through_cursor", _cursor(self.through_cursor, "execution batch end")
        )
        if self.through_cursor < self.from_cursor:
            raise FinancialLedgerError("execution batch cursor range is reversed")
        if type(self.scanned_row_count) is not int or self.scanned_row_count < 0:
            raise FinancialLedgerError("invalid execution scan row count")
        if type(self.page_exhausted) is not bool or type(self.scope_filter_applied) is not bool:
            raise FinancialLedgerError("invalid execution scan coverage flags")
        if not self.page_exhausted or self.scope_filter_applied:
            raise FinancialDataUnavailableError("execution outbox scan is incomplete or filtered")
        if self.scanned_row_count != self.through_cursor - self.from_cursor:
            raise FinancialDataUnavailableError("execution outbox scan has a gap or truncated page")
        postings = tuple(self.postings)
        if any(type(item) is not FinancialExecutionPostingV1 for item in postings):
            raise FinancialLedgerError("invalid financial posting in execution scan")
        if len({item.event_id for item in postings}) != len(postings):
            raise FinancialLedgerError("duplicate event identity in execution scan")
        if len({item.event_sequence for item in postings}) != len(postings):
            raise FinancialLedgerError("duplicate execution sequence in financial scan")
        if any(
            item.journal_generation != self.journal_generation
            or not self.from_cursor < item.event_sequence <= self.through_cursor
            for item in postings
        ):
            raise FinancialDataUnavailableError("financial posting is outside its execution scan")
        object.__setattr__(self, "postings", postings)


def execution_cursor_from_observation_v1(
    event: ExecutionEvent,
    intent: OrderIntent,
    previous: FinancialExecutionCursorV1 | None,
    owner: FinancialOwnerV1,
) -> tuple[FinancialExecutionCursorV1, Decimal, Decimal]:
    """Advance cumulative execution facts without assigning cash or cost."""

    if type(event) is not ExecutionEvent or type(intent) is not OrderIntent:
        raise FinancialLedgerError("execution cursor requires typed durable journal facts")
    if event.event_type not in _FINANCIAL_EXECUTION_EVENTS or event.journal_incarnation_id is None:
        raise FinancialDataUnavailableError("execution cursor event lacks observation lineage")
    if (
        event.intent_id != intent.intent_id
        or event.scope_key != intent.scope.key
        or event.scope_key != owner.execution_scope_key
        or intent.scope.strategy_id != owner.owner_ref
        or owner.kind is not FinancialOwnerKind.STRATEGY
    ):
        raise FinancialLedgerError("execution event, intent, and trusted owner do not match")
    if previous is not None and type(previous) is not FinancialExecutionCursorV1:
        raise FinancialLedgerError("invalid previous execution financial cursor")
    try:
        event_state = ExecutionState(event.state)
    except (TypeError, ValueError):
        raise FinancialLedgerError("execution event state is invalid") from None
    payload = event.payload
    if not isinstance(payload, Mapping):
        raise FinancialLedgerError("execution observation payload is not a mapping")
    expected = (
        {"provider_order_id", "filled_quantity", "average_price", "cumulative_commission", "source"}
        if event.event_type == "provider_commission_evidence"
        else {
            "provider_order_id",
            "filled_quantity",
            "average_price",
            "cumulative_commission",
            "reason_code",
            "source",
        }
    )
    optional = {"cumulative_commission_currency"}
    if set(payload) - optional != expected or set(payload) - expected - optional:
        raise FinancialLedgerError("execution observation payload shape changed")
    if payload.get("source") not in {"provider", "reconcile"}:
        raise FinancialLedgerError("execution observation source is invalid")
    order_id = payload.get("provider_order_id")
    if order_id is not None:
        order_id = _text(order_id, "provider order id")
    prior_order_id = None if previous is None else previous.provider_order_id
    if prior_order_id not in {None, order_id}:
        raise FinancialLedgerError("provider order identity changed across observations")
    if order_id is None:
        order_id = prior_order_id
    filled = _decimal(payload.get("filled_quantity"), "cumulative filled quantity")
    if filled < 0 or filled > intent.quantity:
        raise FinancialLedgerError("cumulative fill is outside the immutable order quantity")
    prior_filled = Decimal("0") if previous is None else previous.filled_quantity
    if filled < prior_filled:
        raise FinancialLedgerError("execution fill quantity regressed")
    raw_average = payload.get("average_price")
    average = (
        None
        if raw_average is None
        else _decimal(raw_average, "cumulative average price", positive=True)
    )
    prior_average = None if previous is None else previous.average_price
    if filled > prior_filled and average is None:
        raise FinancialDataUnavailableError("filled order has no trusted average price")
    if prior_filled > 0 and prior_average is None:
        raise FinancialDataUnavailableError("prior filled order has no trusted cost price")
    delta_quantity = filled - prior_filled
    if delta_quantity and event_state not in {
        ExecutionState.PARTIALLY_FILLED,
        ExecutionState.FILLED,
        ExecutionState.CANCELLED,
    }:
        raise FinancialDataUnavailableError("execution state does not authorize the reported fill")
    if delta_quantity and order_id is None:
        raise FinancialDataUnavailableError("filled execution has no provider order identity")
    if delta_quantity:
        current_notional = filled * average  # type: ignore[operator]
        previous_notional = prior_filled * (prior_average or Decimal("0"))
        if current_notional <= previous_notional:
            raise FinancialDataUnavailableError("cumulative fill notional is not monotonic")
    elif filled > 0 and average is not None and average != prior_average:
        raise FinancialDataUnavailableError(
            "cumulative average price changed without a quantity delta"
        )

    prior_fee = None if previous is None else previous.cumulative_commission
    raw_fee = payload.get("cumulative_commission")
    fee = None if raw_fee is None else _decimal(raw_fee, "cumulative commission")
    prior_currency = None if previous is None else previous.commission_currency
    raw_currency = payload.get("cumulative_commission_currency")
    currency = (
        None if raw_currency is None else _text(raw_currency, "commission currency", max_length=16)
    )
    if fee is not None and currency is None:
        currency = prior_currency
    if fee is not None and prior_fee is not None and currency != prior_currency:
        raise FinancialDataUnavailableError("cumulative commission currency changed or is unknown")
    if fee is not None and currency is None:
        raise FinancialDataUnavailableError("cumulative commission has no trusted currency")
    if fee is not None and fee < (prior_fee or Decimal("0")):
        raise FinancialDataUnavailableError("cumulative commission regressed")
    fee_delta = Decimal("0") if fee is None else fee - (prior_fee or Decimal("0"))
    next_cursor = FinancialExecutionCursorV1(
        filled_quantity=filled,
        average_price=average if average is not None else prior_average,
        cumulative_commission=fee if fee is not None else prior_fee,
        commission_currency=currency if fee is not None else prior_currency,
        provider_order_id=order_id,
    )
    return next_cursor, delta_quantity, fee_delta


def derive_execution_posting_v1(
    event: ExecutionEvent,
    intent: OrderIntent,
    previous: FinancialExecutionCursorV1 | None,
    owner: FinancialOwnerV1,
    instrument_specs: Mapping[str, FinancialInstrumentSpecV1],
) -> FinancialExecutionPostingV1:
    """Derive one new cash/position delta from a durable cumulative observation."""

    cursor, delta_quantity, fee_delta = execution_cursor_from_observation_v1(
        event, intent, previous, owner
    )
    if delta_quantity == 0 and fee_delta == 0:
        raise FinancialDataUnavailableError("execution observation contains no new financial fact")
    spec = instrument_specs.get(intent.instrument)
    if type(spec) is not FinancialInstrumentSpecV1 or spec.instrument != intent.instrument:
        raise FinancialDataUnavailableError("trusted spot instrument metadata is missing")
    if spec.product_model is not FinancialProductModel.SPOT:
        raise FinancialDataUnavailableError("execution fill product model is unsupported")
    if spec.metadata_version != intent.metadata_version:
        raise FinancialDataUnavailableError(
            "execution intent is bound to another instrument metadata version"
        )
    if fee_delta and cursor.commission_currency == spec.base_currency:
        raise FinancialDataUnavailableError("spot V1 cannot value a base-currency execution fee")
    notional_delta = Decimal("0")
    interval_price: Decimal | None = None
    if delta_quantity:
        prior_filled = Decimal("0") if previous is None else previous.filled_quantity
        prior_average = None if previous is None else previous.average_price
        notional_delta = (
            cursor.filled_quantity * cursor.average_price  # type: ignore[operator]
        ) - prior_filled * (prior_average or Decimal("0"))
        interval_price = notional_delta / delta_quantity
    side = Side(intent.side)
    signed_quantity = delta_quantity if side is Side.BUY else -delta_quantity
    return FinancialExecutionPostingV1(
        event_id=event.event_id,
        event_sequence=event.sequence,
        journal_generation=event.journal_incarnation_id or "",
        scope_key=event.scope_key,
        intent_id=event.intent_id,
        provider_order_id=cursor.provider_order_id or "",
        source_payload_sha256=payload_sha256(dict(event.payload)),
        owner=owner,
        occurred_at_ns=event.created_at_ns,
        kind=FinancialEventKind.SPOT_FILL if delta_quantity else FinancialEventKind.CASH_FLOW,
        instrument=intent.instrument if delta_quantity else None,
        signed_quantity=signed_quantity,
        price=interval_price,
        principal_currency=spec.quote_currency if delta_quantity else None,
        principal_cash_delta=(-notional_delta if side is Side.BUY else notional_delta)
        if delta_quantity
        else Decimal("0"),
        fee_currency=cursor.commission_currency if fee_delta else None,
        fee_delta=fee_delta,
    )


def advance_execution_cursor_v1(
    event: ExecutionEvent,
    intent: OrderIntent,
    previous: FinancialExecutionCursorV1 | None,
    owner: FinancialOwnerV1,
    instrument_specs: Mapping[str, FinancialInstrumentSpecV1],
) -> tuple[FinancialExecutionCursorV1, FinancialExecutionPostingV1 | None]:
    """Advance one durable observation and derive its new economic posting."""

    cursor, delta_quantity, fee_delta = execution_cursor_from_observation_v1(
        event, intent, previous, owner
    )
    if delta_quantity == 0 and fee_delta == 0:
        return cursor, None
    return cursor, derive_execution_posting_v1(event, intent, previous, owner, instrument_specs)


@dataclass(frozen=True, slots=True)
class FinancialActivityEventV1:
    """One source-ordered trusted external cash event or attributed spot fill."""

    source_ref: str
    generation: str
    sequence: int
    event_id: str
    occurred_at_ns: int
    owner_key: tuple[str, str] | None
    kind: FinancialEventKind
    cash_movements: tuple[CurrencyAmountV1, ...]
    fee: CurrencyAmountV1 | None = None
    instrument: str | None = None
    signed_quantity: Decimal = Decimal("0")
    price: Decimal | None = None
    provider_order_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_ref", _text(self.source_ref, "activity source ref"))
        object.__setattr__(self, "generation", _text(self.generation, "activity generation"))
        object.__setattr__(self, "sequence", _cursor(self.sequence, "activity sequence"))
        object.__setattr__(self, "event_id", _text(self.event_id, "activity event id"))
        _timestamp(self.occurred_at_ns, "activity event time")
        try:
            kind = FinancialEventKind(self.kind)
        except (TypeError, ValueError):
            raise FinancialLedgerError("invalid account activity kind") from None
        object.__setattr__(self, "kind", kind)
        movements = tuple(self.cash_movements)
        if any(type(item) is not CurrencyAmountV1 for item in movements):
            raise FinancialLedgerError("invalid account activity cash movements")
        if len({item.currency for item in movements}) != len(movements):
            raise FinancialLedgerError("duplicate currency movement in one activity")
        object.__setattr__(self, "cash_movements", movements)
        if self.owner_key is not None:
            if (
                type(self.owner_key) is not tuple
                or len(self.owner_key) != 2
                or any(type(item) is not str for item in self.owner_key)
            ):
                raise FinancialLedgerError("invalid attributed activity owner")
            object.__setattr__(
                self,
                "owner_key",
                (_text(self.owner_key[0], "owner kind"), _text(self.owner_key[1], "owner ref")),
            )
        if self.fee is not None and type(self.fee) is not CurrencyAmountV1:
            raise FinancialLedgerError("invalid attributed activity fee")
        quantity = _decimal(self.signed_quantity, "activity signed quantity")
        object.__setattr__(self, "signed_quantity", quantity)
        if kind is FinancialEventKind.CASH_FLOW:
            if self.instrument is not None or quantity != 0 or self.price is not None:
                raise FinancialLedgerError("cash flow includes fill data")
        else:
            if self.instrument is None or self.price is None or quantity == 0:
                raise FinancialLedgerError("activity fill lacks quantity, price, or instrument")
            object.__setattr__(self, "instrument", _text(self.instrument, "activity instrument"))
            object.__setattr__(
                self, "price", _decimal(self.price, "activity fill price", positive=True)
            )
        if self.provider_order_ref is not None:
            object.__setattr__(
                self, "provider_order_ref", _text(self.provider_order_ref, "provider order ref")
            )


@dataclass(frozen=True, slots=True)
class FinancialActivityBatchV1:
    source_ref: str
    generation: str
    from_cursor: int
    through_cursor: int
    as_of_ns: int
    events: tuple[FinancialActivityEventV1, ...]
    page_exhausted: bool = True
    filters_applied: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_ref", _text(self.source_ref, "activity source ref"))
        object.__setattr__(self, "generation", _text(self.generation, "activity generation"))
        object.__setattr__(self, "from_cursor", _cursor(self.from_cursor, "activity from cursor"))
        object.__setattr__(
            self, "through_cursor", _cursor(self.through_cursor, "activity through cursor")
        )
        object.__setattr__(self, "as_of_ns", _timestamp(self.as_of_ns, "activity as_of"))
        events = tuple(self.events)
        if any(type(item) is not FinancialActivityEventV1 for item in events):
            raise FinancialLedgerError("invalid activity page")
        if type(self.page_exhausted) is not bool or type(self.filters_applied) is not bool:
            raise FinancialLedgerError("invalid activity coverage")
        if not self.page_exhausted or self.filters_applied:
            raise FinancialDataUnavailableError("account activity page is incomplete or filtered")
        if self.through_cursor < self.from_cursor:
            raise FinancialLedgerError("activity cursor range is reversed")
        if len(events) != self.through_cursor - self.from_cursor:
            raise FinancialDataUnavailableError("account activity cursor has a gap")
        expected = self.from_cursor + 1
        for event in events:
            if (
                event.source_ref != self.source_ref
                or event.generation != self.generation
                or event.sequence != expected
                or event.occurred_at_ns > self.as_of_ns
            ):
                raise FinancialDataUnavailableError(
                    "account activity does not match its declared cursor"
                )
            expected += 1
        if len({item.event_id for item in events}) != len(events):
            raise FinancialLedgerError("duplicate account activity identity")
        object.__setattr__(self, "events", events)


@dataclass(frozen=True, slots=True)
class FinancialLedgerPolicyV1:
    cash_currency: str
    max_account_fact_age_ns: int
    max_activity_age_ns: int
    max_cut_skew_ns: int
    reporting_currency: str
    max_mark_age_ns: int
    max_fx_age_ns: int

    def __post_init__(self) -> None:
        for name in ("cash_currency", "reporting_currency"):
            object.__setattr__(self, name, _text(getattr(self, name), name, max_length=16))
        for name in (
            "max_account_fact_age_ns",
            "max_activity_age_ns",
            "max_cut_skew_ns",
            "max_mark_age_ns",
            "max_fx_age_ns",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise FinancialLedgerError(f"invalid {name}")


@dataclass(frozen=True, slots=True)
class OwnerFinancialBookV1:
    owner: FinancialOwnerV1
    ledger_cut: FinancialLedgerCutV1
    cash: tuple[CurrencyAmountV1, ...]
    positions: tuple[OwnerPositionCostV1, ...]
    cumulative_fees: tuple[CurrencyAmountV1, ...]
    realized_pnl: tuple[CurrencyAmountV1, ...]
    cost_basis_complete: bool


@dataclass(frozen=True, slots=True)
class AccountFinancialProjectionV1:
    """One complete source cut and its owner books plus explicit residuals.

    ``complete`` describes source coverage.  A consumer may treat the owner
    allocation as reconciled only when ``state`` is also ``BALANCED``; a
    complete cut can still expose a non-zero ``UNALLOCATED`` residual.
    """

    cut: FinancialLedgerCutV1
    books: tuple[OwnerFinancialBookV1, ...]
    unallocated_cash_residual: tuple[CurrencyAmountV1, ...]
    unallocated_position_residual: tuple[tuple[str, Decimal], ...]
    unallocated_fee_residual: tuple[CurrencyAmountV1, ...]
    complete: bool
    state: str

    def owner_book(self, key: tuple[str, str]) -> OwnerFinancialBookV1:
        for book in self.books:
            if book.owner.key == key:
                return book
        raise FinancialDataUnavailableError("owner financial book is not in this projection")


def project_account_financials_v1(
    *,
    baseline: AccountFinancialBaselineV1,
    directory: FinancialOwnerDirectoryV1,
    current_account: ProviderAccountFinancialSnapshotV1,
    current_cut: FinancialLedgerCutV1,
    execution_batch: FinancialExecutionBatchV1,
    activity_batch: FinancialActivityBatchV1,
    instrument_specs: Mapping[str, FinancialInstrumentSpecV1],
    policy: FinancialLedgerPolicyV1,
    now_ns: int,
    managed_provider_order_ids: tuple[str, ...] = (),
) -> AccountFinancialProjectionV1:
    """Apply trusted journal deltas and reconcile them to provider totals.

    A non-zero difference is retained as an explicit UNALLOCATED residual.
    Incomplete sources, unsupported products, owner mismatches, or gaps are
    errors; callers must not turn them into zero balances.
    """

    if any(
        type(value) is not expected
        for value, expected in (
            (baseline, AccountFinancialBaselineV1),
            (directory, FinancialOwnerDirectoryV1),
            (current_account, ProviderAccountFinancialSnapshotV1),
            (current_cut, FinancialLedgerCutV1),
            (execution_batch, FinancialExecutionBatchV1),
            (activity_batch, FinancialActivityBatchV1),
            (policy, FinancialLedgerPolicyV1),
        )
    ):
        raise FinancialLedgerError("invalid financial projection inputs")
    if type(now_ns) is not int or now_ns <= 0:
        raise FinancialLedgerError("invalid financial projection time")
    if type(managed_provider_order_ids) is not tuple or any(
        type(item) is not str or not item or item != item.strip()
        for item in managed_provider_order_ids
    ):
        raise FinancialLedgerError("invalid managed provider order identity set")
    baseline.validate_against_directory(directory)
    if not directory.complete or not current_account.complete:
        raise FinancialDataUnavailableError(
            "owner directory or provider account totals are incomplete"
        )
    if (
        current_cut.account_key != baseline.account_key
        or current_cut.owner_directory_revision != directory.revision
        or current_account.account_key != baseline.account_key
        or current_account.generation
        != current_cut.watermark(FinancialSourceKind.ACCOUNT_FACTS).generation
        or current_account.cursor != current_cut.watermark(FinancialSourceKind.ACCOUNT_FACTS).cursor
        or current_account.as_of_ns
        != current_cut.watermark(FinancialSourceKind.ACCOUNT_FACTS).as_of_ns
    ):
        raise FinancialDataUnavailableError(
            "provider totals do not bind to the requested source cut"
        )
    _validate_cut_progress(baseline.cut, current_cut)
    _validate_freshness(current_cut, policy, now_ns)
    activity_wm = current_cut.watermark(FinancialSourceKind.ACCOUNT_ACTIVITY)
    baseline_activity_wm = baseline.cut.watermark(FinancialSourceKind.ACCOUNT_ACTIVITY)
    if (
        activity_batch.source_ref != activity_wm.source_ref
        or activity_batch.generation != activity_wm.generation
        or activity_batch.from_cursor != baseline_activity_wm.cursor
        or activity_batch.through_cursor != activity_wm.cursor
    ):
        raise FinancialDataUnavailableError(
            "account activity batch does not span the baseline-to-current cut"
        )
    execution_wm = current_cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX)
    baseline_execution_wm = baseline.cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX)
    if (
        execution_batch.journal_generation != execution_wm.generation
        or execution_batch.from_cursor != baseline_execution_wm.cursor
        or execution_batch.through_cursor != execution_wm.cursor
    ):
        raise FinancialDataUnavailableError(
            "execution scan does not span the baseline-to-current cut"
        )
    postings = execution_batch.postings
    for posting in postings:
        if (
            posting.journal_generation != execution_wm.generation
            or not baseline_execution_wm.cursor < posting.event_sequence <= execution_wm.cursor
            or directory.owner(posting.owner.key) != posting.owner
        ):
            raise FinancialDataUnavailableError(
                "execution fill posting is outside the authorized source cut"
            )
    if len({item.event_sequence for item in postings}) != len(postings):
        raise FinancialLedgerError("multiple financial facts claim one execution outbox row")
    baseline_cursor_snapshot = baseline.execution_cursors
    if baseline_cursor_snapshot is None:
        raise FinancialDataUnavailableError(
            "financial baseline has no durable cumulative execution cursors"
        )
    baseline_order_ids = tuple(
        row.cursor.provider_order_id
        for row in baseline_cursor_snapshot.orders
        if row.cursor.provider_order_id is not None
    )
    _assert_managed_activity_not_duplicated(
        postings,
        activity_batch.events,
        known_managed_order_ids=baseline_order_ids + managed_provider_order_ids,
    )

    for owner_baseline in baseline.owners:
        for position in owner_baseline.positions:
            spec = instrument_specs.get(position.instrument)
            if (
                type(spec) is not FinancialInstrumentSpecV1
                or spec.product_model is not FinancialProductModel.SPOT
                or spec.metadata_version != position.metadata_version
                or position.cost_currency not in {None, spec.quote_currency}
            ):
                raise FinancialDataUnavailableError(
                    "baseline position lacks matching supported spot cost metadata"
                )

    books = {item.owner.key: _MutableBook.from_baseline(item) for item in baseline.owners}
    if {item.owner.key for item in baseline.owners} != {item.key for item in directory.owners}:
        raise FinancialDataUnavailableError("baseline does not cover the trusted owner directory")
    for posting in sorted(postings, key=lambda item: item.event_sequence):
        _apply_execution_posting(books[posting.owner.key], posting, instrument_specs)
    for event in activity_batch.events:
        owner = directory.owner(event.owner_key) if event.owner_key is not None else None
        if event.owner_key is not None and owner is None:
            raise FinancialDataUnavailableError("activity event names an unregistered owner")
        if event.kind is FinancialEventKind.SPOT_FILL and event.owner_key is None:
            # Unknown ownership is represented in the independent account
            # residual; never guess a book from its instrument or side.
            continue
        if event.kind is FinancialEventKind.SPOT_FILL and event.provider_order_ref is None:
            raise FinancialDataUnavailableError(
                "attributed external fill has no provider order identity"
            )
        if (
            event.kind is FinancialEventKind.SPOT_FILL
            and owner.kind is not FinancialOwnerKind.MANUAL_BOOK
        ):
            raise FinancialDataUnavailableError(
                "external spot fills must be assigned to a manual book"
            )
        if event.owner_key is None:
            continue
        _apply_activity_event(books[owner.key], event, instrument_specs)

    snapshot_cash = {item.currency: item.amount for item in current_account.cash}
    snapshot_positions = dict(current_account.positions)
    snapshot_fees = {item.currency: item.amount for item in current_account.cumulative_fees}
    if policy.cash_currency not in snapshot_cash:
        raise FinancialDataUnavailableError(
            "provider snapshot does not cover configured cash currency"
        )
    for instrument, quantity in snapshot_positions.items():
        spec = instrument_specs.get(instrument)
        if (
            type(spec) is not FinancialInstrumentSpecV1
            or spec.product_model is not FinancialProductModel.SPOT
        ):
            raise FinancialDataUnavailableError(
                "provider position has no supported spot accounting model"
            )
        if quantity < 0:
            raise FinancialDataUnavailableError("spot provider position is short or invalid")
    cash_sum: dict[str, Decimal] = defaultdict(Decimal)
    fee_sum: dict[str, Decimal] = defaultdict(Decimal)
    position_sum: dict[str, Decimal] = defaultdict(Decimal)
    final_books: list[OwnerFinancialBookV1] = []
    for key in sorted(books):
        mutable = books[key]
        for currency, amount in mutable.cash.items():
            cash_sum[currency] += amount
        for currency, amount in mutable.fees.items():
            fee_sum[currency] += amount
        for instrument, position in mutable.positions.items():
            position_sum[instrument] += position.quantity
        final_books.append(mutable.freeze(current_cut))
    cash_residual = {
        currency: snapshot_cash.get(currency, Decimal("0")) - cash_sum.get(currency, Decimal("0"))
        for currency in set(snapshot_cash) | set(cash_sum)
        if snapshot_cash.get(currency, Decimal("0")) != cash_sum.get(currency, Decimal("0"))
    }
    position_residual = {
        instrument: snapshot_positions.get(instrument, Decimal("0"))
        - position_sum.get(instrument, Decimal("0"))
        for instrument in set(snapshot_positions) | set(position_sum)
        if snapshot_positions.get(instrument, Decimal("0"))
        != position_sum.get(instrument, Decimal("0"))
    }
    fee_residual = {
        currency: snapshot_fees.get(currency, Decimal("0")) - fee_sum.get(currency, Decimal("0"))
        for currency in set(snapshot_fees) | set(fee_sum)
        if snapshot_fees.get(currency, Decimal("0")) != fee_sum.get(currency, Decimal("0"))
    }
    state = "RESIDUAL" if cash_residual or position_residual or fee_residual else "BALANCED"
    return AccountFinancialProjectionV1(
        cut=current_cut,
        books=tuple(final_books),
        unallocated_cash_residual=tuple(
            CurrencyAmountV1(currency, amount) for currency, amount in sorted(cash_residual.items())
        ),
        unallocated_position_residual=tuple(sorted(position_residual.items())),
        unallocated_fee_residual=tuple(
            CurrencyAmountV1(currency, amount) for currency, amount in sorted(fee_residual.items())
        ),
        complete=True,
        state=state,
    )


@dataclass(slots=True)
class _MutablePosition:
    quantity: Decimal
    cost_basis: Decimal | None
    cost_currency: str | None
    metadata_version: str


@dataclass(slots=True)
class _MutableBook:
    owner: FinancialOwnerV1
    cash: dict[str, Decimal]
    positions: dict[str, _MutablePosition]
    fees: dict[str, Decimal]
    realized: dict[str, Decimal]
    cost_basis_complete: bool

    @classmethod
    def from_baseline(cls, baseline: FinancialOwnerBaselineV1) -> _MutableBook:
        return cls(
            owner=baseline.owner,
            cash={item.currency: item.amount for item in baseline.cash},
            positions={
                item.instrument: _MutablePosition(
                    item.quantity, item.cost_basis, item.cost_currency, item.metadata_version
                )
                for item in baseline.positions
            },
            fees={item.currency: item.amount for item in baseline.cumulative_fees},
            realized={},
            cost_basis_complete=all(item.cost_basis is not None for item in baseline.positions),
        )

    def freeze(self, ledger_cut: FinancialLedgerCutV1) -> OwnerFinancialBookV1:
        return OwnerFinancialBookV1(
            owner=self.owner,
            ledger_cut=ledger_cut,
            cash=tuple(CurrencyAmountV1(key, value) for key, value in sorted(self.cash.items())),
            positions=tuple(
                OwnerPositionCostV1(
                    instrument=key,
                    quantity=value.quantity,
                    cost_basis=value.cost_basis,
                    cost_currency=value.cost_currency,
                    metadata_version=value.metadata_version,
                )
                for key, value in sorted(self.positions.items())
                if value.quantity != 0
            ),
            cumulative_fees=tuple(
                CurrencyAmountV1(key, value) for key, value in sorted(self.fees.items())
            ),
            realized_pnl=tuple(
                CurrencyAmountV1(key, value) for key, value in sorted(self.realized.items())
            ),
            cost_basis_complete=self.cost_basis_complete,
        )


def _apply_execution_posting(
    book: _MutableBook,
    posting: FinancialExecutionPostingV1,
    specs: Mapping[str, FinancialInstrumentSpecV1],
) -> None:
    if posting.kind is FinancialEventKind.SPOT_FILL:
        spec = specs.get(posting.instrument or "")
        if (
            type(spec) is not FinancialInstrumentSpecV1
            or spec.product_model is not FinancialProductModel.SPOT
        ):
            raise FinancialDataUnavailableError("fill lacks registered spot accounting metadata")
        position = book.positions.setdefault(
            posting.instrument or "",
            _MutablePosition(
                Decimal("0"), Decimal("0"), spec.quote_currency, spec.metadata_version
            ),
        )
        if position.metadata_version != spec.metadata_version:
            raise FinancialDataUnavailableError("instrument metadata changed across cost basis")
        _apply_average_cost_fill(
            book, position, posting.signed_quantity, posting.price, spec.quote_currency
        )
        book.cash[spec.quote_currency] = (
            book.cash.get(spec.quote_currency, Decimal("0")) + posting.principal_cash_delta
        )
    if posting.fee_delta != 0:
        currency = posting.fee_currency
        if currency is None:
            raise FinancialDataUnavailableError("fee posting has no currency")
        book.cash[currency] = book.cash.get(currency, Decimal("0")) - posting.fee_delta
        book.fees[currency] = book.fees.get(currency, Decimal("0")) + posting.fee_delta


def _apply_activity_event(
    book: _MutableBook,
    event: FinancialActivityEventV1,
    specs: Mapping[str, FinancialInstrumentSpecV1],
) -> None:
    if event.kind is FinancialEventKind.SPOT_FILL:
        spec = specs.get(event.instrument or "")
        if (
            type(spec) is not FinancialInstrumentSpecV1
            or spec.product_model is not FinancialProductModel.SPOT
        ):
            raise FinancialDataUnavailableError(
                "external fill lacks registered spot accounting metadata"
            )
        if event.price is None:
            raise FinancialDataUnavailableError("external spot fill has no trusted price")
        expected_principal = -event.signed_quantity * event.price
        if event.cash_movements != (CurrencyAmountV1(spec.quote_currency, expected_principal),):
            raise FinancialDataUnavailableError(
                "external fill cash does not match its quote-currency principal"
            )
        if event.fee is not None and event.fee.currency == spec.base_currency:
            raise FinancialDataUnavailableError("spot V1 cannot value a base-currency fill fee")
    for movement in event.cash_movements:
        book.cash[movement.currency] = (
            book.cash.get(movement.currency, Decimal("0")) + movement.amount
        )
    if event.fee is not None:
        book.cash[event.fee.currency] = (
            book.cash.get(event.fee.currency, Decimal("0")) - event.fee.amount
        )
        book.fees[event.fee.currency] = (
            book.fees.get(event.fee.currency, Decimal("0")) + event.fee.amount
        )
    if event.kind is FinancialEventKind.SPOT_FILL:
        spec = specs.get(event.instrument or "")
        if (
            type(spec) is not FinancialInstrumentSpecV1
            or spec.product_model is not FinancialProductModel.SPOT
        ):
            raise FinancialDataUnavailableError(
                "external fill lacks registered spot accounting metadata"
            )
        position = book.positions.setdefault(
            event.instrument or "",
            _MutablePosition(
                Decimal("0"), Decimal("0"), spec.quote_currency, spec.metadata_version
            ),
        )
        if position.metadata_version != spec.metadata_version:
            raise FinancialDataUnavailableError("instrument metadata changed across cost basis")
        _apply_average_cost_fill(
            book, position, event.signed_quantity, event.price, spec.quote_currency
        )


def _apply_average_cost_fill(
    book: _MutableBook,
    position: _MutablePosition,
    signed_quantity: Decimal,
    price: Decimal | None,
    quote_currency: str,
) -> None:
    if price is None or price <= 0:
        raise FinancialDataUnavailableError("spot fill has no usable price")
    if position.cost_currency not in {None, quote_currency}:
        raise FinancialDataUnavailableError("cost basis currency changed")
    if signed_quantity > 0:
        add_cost = signed_quantity * price
        position.quantity += signed_quantity
        if position.cost_basis is not None:
            position.cost_basis += add_cost
        position.cost_currency = quote_currency
        return
    close_quantity = -signed_quantity
    if close_quantity > position.quantity:
        raise FinancialDataUnavailableError("spot owner fill exceeds its allocated position")
    if position.cost_basis is None:
        book.cost_basis_complete = False
    else:
        average_cost = (
            position.cost_basis / position.quantity if position.quantity else Decimal("0")
        )
        realized = close_quantity * (price - average_cost)
        book.realized[quote_currency] = book.realized.get(quote_currency, Decimal("0")) + realized
        position.cost_basis -= close_quantity * average_cost
    position.quantity -= close_quantity
    if position.quantity == 0:
        position.cost_basis = Decimal("0")
        position.cost_currency = quote_currency


@dataclass(frozen=True, slots=True)
class FinancialMarkV1:
    instrument: str
    price: Decimal
    currency: str
    metadata_version: str
    source_ref: str
    generation: str
    cursor: int
    as_of_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument", _text(self.instrument, "mark instrument"))
        object.__setattr__(self, "price", _decimal(self.price, "mark price", positive=True))
        object.__setattr__(self, "currency", _text(self.currency, "mark currency", max_length=16))
        object.__setattr__(
            self, "metadata_version", _text(self.metadata_version, "mark metadata version")
        )
        object.__setattr__(self, "source_ref", _text(self.source_ref, "mark source"))
        object.__setattr__(self, "generation", _text(self.generation, "mark source generation"))
        object.__setattr__(self, "cursor", _cursor(self.cursor, "mark source cursor"))
        object.__setattr__(self, "as_of_ns", _timestamp(self.as_of_ns, "mark as_of"))


@dataclass(frozen=True, slots=True)
class FinancialFxRateV1:
    from_currency: str
    to_currency: str
    rate: Decimal
    source_ref: str
    generation: str
    cursor: int
    as_of_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "from_currency", _text(self.from_currency, "FX from currency", max_length=16)
        )
        object.__setattr__(
            self, "to_currency", _text(self.to_currency, "FX to currency", max_length=16)
        )
        object.__setattr__(self, "rate", _decimal(self.rate, "FX rate", positive=True))
        object.__setattr__(self, "source_ref", _text(self.source_ref, "FX source"))
        object.__setattr__(self, "generation", _text(self.generation, "FX source generation"))
        object.__setattr__(self, "cursor", _cursor(self.cursor, "FX source cursor"))
        object.__setattr__(self, "as_of_ns", _timestamp(self.as_of_ns, "FX as_of"))


@dataclass(frozen=True, slots=True)
class FinancialValuationSnapshotV1:
    cut: FinancialViewCutV1
    marks: tuple[FinancialMarkV1, ...]
    fx_rates: tuple[FinancialFxRateV1, ...]
    reporting_currency: str
    valuation_model_version: str

    def __post_init__(self) -> None:
        if type(self.cut) is not FinancialViewCutV1:
            raise FinancialLedgerError("invalid valuation cut")
        marks = tuple(self.marks)
        rates = tuple(self.fx_rates)
        if any(type(item) is not FinancialMarkV1 for item in marks) or any(
            type(item) is not FinancialFxRateV1 for item in rates
        ):
            raise FinancialLedgerError("invalid valuation facts")
        if len({item.instrument for item in marks}) != len(marks):
            raise FinancialLedgerError("duplicate valuation mark")
        if len({(item.from_currency, item.to_currency) for item in rates}) != len(rates):
            raise FinancialLedgerError("duplicate FX rate")
        object.__setattr__(self, "marks", marks)
        object.__setattr__(self, "fx_rates", rates)
        object.__setattr__(
            self,
            "reporting_currency",
            _text(self.reporting_currency, "reporting currency", max_length=16),
        )
        object.__setattr__(
            self,
            "valuation_model_version",
            _text(self.valuation_model_version, "valuation model version"),
        )


@dataclass(frozen=True, slots=True)
class FinancialValuationPolicyV1:
    cash_currency: str
    reporting_currency: str
    max_account_fact_age_ns: int
    max_activity_age_ns: int
    max_mark_age_ns: int
    max_fx_age_ns: int
    max_cut_skew_ns: int

    def __post_init__(self) -> None:
        for name in ("cash_currency", "reporting_currency"):
            object.__setattr__(self, name, _text(getattr(self, name), name, max_length=16))
        for name in (
            "max_account_fact_age_ns",
            "max_activity_age_ns",
            "max_mark_age_ns",
            "max_fx_age_ns",
            "max_cut_skew_ns",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise FinancialLedgerError(f"invalid {name}")


def owner_cash_v1(book: OwnerFinancialBookV1, currency: str) -> Decimal:
    """Return one explicit native-currency owner cash balance."""

    if type(book) is not OwnerFinancialBookV1:
        raise FinancialLedgerError("invalid owner financial book")
    currency = _text(currency, "cash currency", max_length=16)
    row = next((item for item in book.cash if item.currency == currency), None)
    if row is None:
        raise FinancialDataUnavailableError(
            "configured owner cash currency has no explicit balance"
        )
    return row.amount


def owner_nav_v1(
    book: OwnerFinancialBookV1,
    valuation: FinancialValuationSnapshotV1,
    specs: Mapping[str, FinancialInstrumentSpecV1],
    policy: FinancialValuationPolicyV1,
    *,
    now_ns: int,
) -> Decimal:
    """Value a complete spot book in one configured report currency."""

    if any(
        type(value) is not expected
        for value, expected in (
            (book, OwnerFinancialBookV1),
            (valuation, FinancialValuationSnapshotV1),
            (policy, FinancialValuationPolicyV1),
        )
    ):
        raise FinancialLedgerError("invalid owner NAV inputs")
    if type(now_ns) is not int or now_ns <= 0:
        raise FinancialLedgerError("invalid valuation clock")
    if valuation.cut.ledger != book.ledger_cut:
        raise FinancialDataUnavailableError("valuation and owner book use different ledger cuts")
    if valuation.reporting_currency != policy.reporting_currency:
        raise FinancialDataUnavailableError(
            "valuation reporting currency differs from trusted policy"
        )
    _validate_freshness(valuation.cut.ledger, policy, now_ns)
    for watermark in valuation.cut.valuation_watermarks:
        age_limit = (
            policy.max_mark_age_ns
            if watermark.source_kind is FinancialSourceKind.MARKET_MARKS
            else policy.max_fx_age_ns
        )
        if now_ns - watermark.as_of_ns > age_limit:
            raise FinancialDataUnavailableError("valuation source is stale")
    if (
        max(item.as_of_ns for item in valuation.cut.valuation_watermarks)
        - min(item.as_of_ns for item in valuation.cut.valuation_watermarks)
        > policy.max_cut_skew_ns
    ):
        raise FinancialDataUnavailableError("market and FX cuts exceed allowed skew")
    source_times = [
        item.as_of_ns
        for item in valuation.cut.ledger.watermarks + valuation.cut.valuation_watermarks
    ]
    if max(source_times) - min(source_times) > policy.max_cut_skew_ns:
        raise FinancialDataUnavailableError("financial and valuation cuts exceed allowed skew")
    cut_marks = valuation.cut.watermark(FinancialSourceKind.MARKET_MARKS)
    cut_fx = valuation.cut.watermark(FinancialSourceKind.FX_RATES)
    marks = {item.instrument: item for item in valuation.marks}
    rates = {(item.from_currency, item.to_currency): item for item in valuation.fx_rates}
    if any(
        item.source_ref != cut_marks.source_ref
        or item.generation != cut_marks.generation
        or item.cursor != cut_marks.cursor
        or item.as_of_ns != cut_marks.as_of_ns
        for item in valuation.marks
    ):
        raise FinancialDataUnavailableError("mark facts do not match their source cut")
    if any(
        item.source_ref != cut_fx.source_ref
        or item.generation != cut_fx.generation
        or item.cursor != cut_fx.cursor
        or item.as_of_ns != cut_fx.as_of_ns
        for item in valuation.fx_rates
    ):
        raise FinancialDataUnavailableError("FX facts do not match their source cut")

    def convert(amount: Decimal, currency: str) -> Decimal:
        if currency == policy.reporting_currency:
            return amount
        rate = rates.get((currency, policy.reporting_currency))
        if rate is None:
            raise FinancialDataUnavailableError("required FX rate is missing")
        if now_ns - rate.as_of_ns > policy.max_fx_age_ns:
            raise FinancialDataUnavailableError("required FX rate is stale")
        return amount * rate.rate

    value = Decimal("0")
    for cash in book.cash:
        value += convert(cash.amount, cash.currency)
    for position in book.positions:
        if position.quantity == 0:
            continue
        if position.quantity < 0:
            raise FinancialDataUnavailableError("spot V1 does not value short positions")
        spec = specs.get(position.instrument)
        mark = marks.get(position.instrument)
        if (
            type(spec) is not FinancialInstrumentSpecV1
            or spec.product_model is not FinancialProductModel.SPOT
        ):
            raise FinancialDataUnavailableError("position has no supported spot model")
        if (
            mark is None
            or mark.currency != spec.quote_currency
            or mark.metadata_version != spec.metadata_version
        ):
            raise FinancialDataUnavailableError(
                "position mark is missing or uses another contract version"
            )
        if now_ns - mark.as_of_ns > policy.max_mark_age_ns:
            raise FinancialDataUnavailableError("position mark is stale")
        value += convert(position.quantity * mark.price, mark.currency)
    return value


def assert_same_financial_cut(before: FinancialViewCutV1, after: FinancialViewCutV1) -> None:
    """Reject any source generation/cursor/time change during a composed read."""

    if type(before) is not FinancialViewCutV1 or type(after) is not FinancialViewCutV1:
        raise FinancialLedgerError("invalid financial cut comparison")
    if before != after:
        raise FinancialDataUnavailableError("financial source vector changed during read")


def _validate_cut_progress(baseline: FinancialLedgerCutV1, current: FinancialLedgerCutV1) -> None:
    if (
        baseline.account_key != current.account_key
        or baseline.owner_directory_revision != current.owner_directory_revision
    ):
        raise FinancialDataUnavailableError(
            "account or owner directory changed across financial cuts"
        )
    for kind in (
        FinancialSourceKind.EXECUTION_OUTBOX,
        FinancialSourceKind.ACCOUNT_FACTS,
        FinancialSourceKind.ACCOUNT_ACTIVITY,
    ):
        old = baseline.watermark(kind)
        new = current.watermark(kind)
        if (
            old.source_ref != new.source_ref
            or old.generation != new.generation
            or new.cursor < old.cursor
        ):
            raise FinancialDataUnavailableError("financial source generation or cursor changed")
    if current.captured_at_ns < baseline.captured_at_ns:
        raise FinancialDataUnavailableError("financial cut time regressed")


def _validate_freshness(
    cut: FinancialLedgerCutV1,
    policy: FinancialLedgerPolicyV1 | FinancialValuationPolicyV1,
    now_ns: int,
) -> None:
    if now_ns < cut.captured_at_ns:
        raise FinancialDataUnavailableError("financial cut is in the future")
    for kind, age_limit in (
        (FinancialSourceKind.ACCOUNT_FACTS, policy.max_account_fact_age_ns),
        (FinancialSourceKind.ACCOUNT_ACTIVITY, policy.max_activity_age_ns),
    ):
        if now_ns - cut.watermark(kind).as_of_ns > age_limit:
            raise FinancialDataUnavailableError(f"{kind.value} source is stale")
    source_times = [item.as_of_ns for item in cut.watermarks]
    if max(source_times) - min(source_times) > policy.max_cut_skew_ns:
        raise FinancialDataUnavailableError("account financial cut exceeds allowed source skew")


def _assert_managed_activity_not_duplicated(
    execution_postings: tuple[FinancialExecutionPostingV1, ...],
    activity_events: tuple[FinancialActivityEventV1, ...],
    *,
    known_managed_order_ids: tuple[str, ...] = (),
) -> None:
    managed_order_ids = set(known_managed_order_ids)
    managed_order_ids.update(item.provider_order_id for item in execution_postings)
    if managed_order_ids and any(
        event.provider_order_ref in managed_order_ids
        for event in activity_events
        if event.provider_order_ref is not None
    ):
        raise FinancialDataUnavailableError("account activity duplicates a managed execution fact")


__all__ = [
    "AccountFinancialBaselineV1",
    "AccountFinancialProjectionV1",
    "FinancialActivityBatchV1",
    "FinancialActivityEventV1",
    "FinancialCostMethod",
    "CurrencyAmountV1",
    "FinancialDataUnavailableError",
    "FinancialEventKind",
    "FinancialExecutionBatchV1",
    "FinancialExecutionCursorV1",
    "FinancialExecutionCursorSnapshotV1",
    "FinancialExecutionPostingV1",
    "FinancialFxRateV1",
    "FinancialInstrumentSpecV1",
    "FinancialLedgerCutV1",
    "FinancialLedgerError",
    "FinancialLedgerPolicyV1",
    "FinancialMarkV1",
    "FinancialOwnerBaselineV1",
    "FinancialOwnerDirectoryV1",
    "FinancialOwnerKind",
    "FinancialOwnerV1",
    "FinancialOrderBaselineCursorV1",
    "FinancialProductModel",
    "FinancialValuationPolicyV1",
    "FinancialValuationSnapshotV1",
    "FinancialViewCutV1",
    "FinancialWatermarkV1",
    "OwnerFinancialBookV1",
    "OwnerPositionCostV1",
    "ProviderAccountFinancialSnapshotV1",
    "assert_same_financial_cut",
    "advance_execution_cursor_v1",
    "derive_execution_posting_v1",
    "execution_cursor_from_observation_v1",
    "owner_cash_v1",
    "owner_nav_v1",
    "project_account_financials_v1",
]
