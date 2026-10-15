"""Execution-owned virtual books reconciled to versioned account facts.

This module consumes the existing execution outbox; it does not create a
second fill journal.  Scope directories, provider snapshots, allocation
baselines, and external activity must be assembled by trusted composition
code.  Caller supplied coverage flags are evidence claims, not authority by
themselves.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum

from bt_api_base.service_protocol import WireScope

from .contracts import (
    _INSTRUMENT,
    ExecutionEvent,
    ExecutionScope,
    ExecutionState,
    Side,
    _decimal,
    _text,
)
from .errors import ContractValidationError, DurableStoreError
from .store import SqliteExecutionStore

_COMPLETE_VALUES = frozenset({"COMPLETE", "PARTIAL", "INCOMPLETE", "UNAVAILABLE"})
_OBSERVATION_EVENTS = frozenset(
    {"provider_observation", "reconciled_observation", "provider_commission_evidence"}
)
_UNALLOCATED = ("UNALLOCATED", "UNALLOCATED")


class AllocationOwnerKind(StrEnum):
    STRATEGY = "STRATEGY"
    MANUAL_BOOK = "MANUAL_BOOK"
    UNALLOCATED = "UNALLOCATED"


def _timestamp(value: int, field_name: str, *, allow_zero: bool = False) -> int:
    if type(value) is not int or value < (0 if allow_zero else 1):
        raise ContractValidationError(f"invalid {field_name}")
    return value


def _currency(value: str) -> str:
    return _text(value, "currency")


def _freeze_tuple(value: object, field_name: str) -> tuple[object, ...]:
    if not isinstance(value, (tuple, list)):
        raise ContractValidationError(f"invalid {field_name}")
    return tuple(value)


@dataclass(frozen=True, slots=True)
class ExecutionOwnerScope:
    """One trusted journal scope and its stable virtual-book owner."""

    scope: ExecutionScope
    owner_kind: AllocationOwnerKind
    owner_ref: str
    wire_scope: WireScope

    def __post_init__(self) -> None:
        if type(self.scope) is not ExecutionScope:
            raise ContractValidationError("invalid execution owner scope")
        if type(self.wire_scope) is not WireScope:
            raise ContractValidationError("owner directory requires a trusted wire-scope binding")
        try:
            kind = AllocationOwnerKind(self.owner_kind)
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid allocation owner kind") from error
        if kind is AllocationOwnerKind.UNALLOCATED:
            raise ContractValidationError("UNALLOCATED is a result bucket, not a registered owner")
        owner_ref = _text(self.owner_ref, "allocation owner ref")
        if self.wire_scope.account_ref != self.scope.account_ref:
            raise ContractValidationError("wire scope does not match its execution account")
        if kind is AllocationOwnerKind.STRATEGY:
            if (
                owner_ref != self.scope.strategy_id
                or self.wire_scope.strategy_ref != owner_ref
                or self.wire_scope.manual_book_ref is not None
            ):
                raise ContractValidationError("strategy book does not match its execution scope")
        elif (
            self.wire_scope.manual_book_ref != owner_ref or self.wire_scope.strategy_ref is not None
        ):
            raise ContractValidationError("manual book does not match its wire scope")
        object.__setattr__(self, "owner_kind", kind)
        object.__setattr__(self, "owner_ref", owner_ref)

    @property
    def owner_key(self) -> tuple[str, str]:
        return self.owner_kind.value, self.owner_ref


@dataclass(frozen=True, slots=True)
class ExecutionScopeDirectory:
    """Versioned, unfiltered directory of all allocation scopes for an account."""

    account_key: str
    source: str
    source_ref: str
    revision: str
    covers_from_ns: int
    as_of_ns: int
    scopes: tuple[ExecutionOwnerScope, ...]
    completeness: str = "COMPLETE"
    page_exhausted: bool = True
    filters_applied: bool = False
    continuation_token: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_key", _text(self.account_key, "account key"))
        for name in ("source", "source_ref", "revision"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        _timestamp(self.covers_from_ns, "scope directory coverage start")
        _timestamp(self.as_of_ns, "scope directory as_of")
        if self.as_of_ns < self.covers_from_ns:
            raise ContractValidationError("scope directory interval is reversed")
        if self.completeness not in _COMPLETE_VALUES:
            raise ContractValidationError("invalid scope directory completeness")
        if type(self.page_exhausted) is not bool or type(self.filters_applied) is not bool:
            raise ContractValidationError("invalid scope directory coverage flags")
        token = self.continuation_token
        if token is not None:
            object.__setattr__(self, "continuation_token", _text(token, "continuation token"))
        if self.page_exhausted == (token is not None):
            raise ContractValidationError("scope directory continuation does not match page state")
        scopes = _freeze_tuple(self.scopes, "scope directory entries")
        if not scopes or any(type(item) is not ExecutionOwnerScope for item in scopes):
            raise ContractValidationError("scope directory entries are required")
        if len({item.scope.key for item in scopes}) != len(scopes):
            raise ContractValidationError("duplicate execution scope directory entry")
        if len({item.wire_scope for item in scopes}) != len(scopes):
            raise ContractValidationError("duplicate wire scope directory entry")
        for item in scopes:
            if item.scope.account_key != self.account_key:
                raise ContractValidationError("scope directory crosses account boundaries")
        object.__setattr__(self, "scopes", scopes)

    @property
    def complete(self) -> bool:
        return self.completeness == "COMPLETE" and self.page_exhausted and not self.filters_applied


@dataclass(frozen=True, slots=True)
class PositionFact:
    instrument: str
    net_quantity: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "instrument", _text(self.instrument, "instrument", pattern=_INSTRUMENT)
        )
        object.__setattr__(self, "net_quantity", _decimal(self.net_quantity, "net quantity"))


@dataclass(frozen=True, slots=True)
class CurrencyAmount:
    currency: str
    amount: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "currency", _currency(self.currency))
        object.__setattr__(self, "amount", _decimal(self.amount, "currency amount"))


@dataclass(frozen=True, slots=True)
class ExecutionAccountSnapshot:
    """Complete-or-explicitly-partial provider net positions and fee counters.

    ``execution_cursor`` declares which execution outbox sequence is reflected
    in the account snapshot. Fee totals are cumulative from
    ``fee_coverage_start_ns`` within ``generation``; omitted currencies mean
    zero only when ``fees_complete`` is true.
    """

    account_key: str
    source: str
    source_ref: str
    generation: str
    journal_generation: str
    as_of_ns: int
    execution_cursor: int
    position_mode: str
    positions: tuple[PositionFact, ...]
    cumulative_fees: tuple[CurrencyAmount, ...]
    fee_coverage_start_ns: int
    completeness: str = "COMPLETE"
    page_exhausted: bool = True
    filters_applied: bool = False
    position_universe_complete: bool = True
    fees_complete: bool = True
    continuation_token: str | None = None

    def __post_init__(self) -> None:
        for name in ("account_key", "source", "source_ref", "generation", "journal_generation"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        _timestamp(self.as_of_ns, "account snapshot as_of")
        _timestamp(self.execution_cursor, "account snapshot execution cursor", allow_zero=True)
        _timestamp(self.fee_coverage_start_ns, "fee coverage start")
        if self.fee_coverage_start_ns > self.as_of_ns:
            raise ContractValidationError("fee coverage begins after account snapshot")
        if self.position_mode != "NET":
            raise ContractValidationError("only explicit NET position snapshots are supported")
        if self.completeness not in _COMPLETE_VALUES:
            raise ContractValidationError("invalid account snapshot completeness")
        for name in (
            "page_exhausted",
            "filters_applied",
            "position_universe_complete",
            "fees_complete",
        ):
            if type(getattr(self, name)) is not bool:
                raise ContractValidationError(f"invalid {name}")
        token = self.continuation_token
        if token is not None:
            object.__setattr__(self, "continuation_token", _text(token, "continuation token"))
        if self.page_exhausted == (token is not None):
            raise ContractValidationError("account snapshot continuation does not match page state")
        positions = _freeze_tuple(self.positions, "positions")
        fees = _freeze_tuple(self.cumulative_fees, "cumulative fees")
        if any(type(item) is not PositionFact for item in positions):
            raise ContractValidationError("invalid position fact")
        if any(type(item) is not CurrencyAmount for item in fees):
            raise ContractValidationError("invalid fee total")
        if len({item.instrument for item in positions}) != len(positions):
            raise ContractValidationError("duplicate account position")
        if len({item.currency for item in fees}) != len(fees):
            raise ContractValidationError("duplicate account fee currency")
        object.__setattr__(self, "positions", positions)
        object.__setattr__(self, "cumulative_fees", fees)

    @property
    def coverage_complete(self) -> bool:
        return (
            self.completeness == "COMPLETE"
            and self.page_exhausted
            and not self.filters_applied
            and self.position_universe_complete
            and self.fees_complete
        )


@dataclass(frozen=True, slots=True)
class OwnerPosition:
    owner_kind: AllocationOwnerKind
    owner_ref: str
    instrument: str
    net_quantity: Decimal

    def __post_init__(self) -> None:
        try:
            kind = AllocationOwnerKind(self.owner_kind)
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid baseline owner kind") from error
        if kind is AllocationOwnerKind.UNALLOCATED:
            raise ContractValidationError(
                "baseline owners must be explicit strategy or manual books"
            )
        object.__setattr__(self, "owner_kind", kind)
        object.__setattr__(self, "owner_ref", _text(self.owner_ref, "owner ref"))
        object.__setattr__(
            self, "instrument", _text(self.instrument, "instrument", pattern=_INSTRUMENT)
        )
        object.__setattr__(self, "net_quantity", _decimal(self.net_quantity, "owner net quantity"))

    @property
    def owner_key(self) -> tuple[str, str]:
        return self.owner_kind.value, self.owner_ref


@dataclass(frozen=True, slots=True)
class ExecutionAllocationBaseline:
    """Prior virtual positions tied to the provider and journal snapshot cut."""

    account_key: str
    source: str
    source_ref: str
    revision: str
    scope_directory_revision: str
    provider_generation: str
    journal_generation: str
    as_of_ns: int
    execution_cursor: int
    positions: tuple[OwnerPosition, ...]
    completeness: str = "COMPLETE"
    page_exhausted: bool = True
    filters_applied: bool = False
    continuation_token: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "account_key",
            "source",
            "source_ref",
            "revision",
            "scope_directory_revision",
            "provider_generation",
            "journal_generation",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        _timestamp(self.as_of_ns, "allocation baseline as_of")
        _timestamp(self.execution_cursor, "allocation baseline cursor", allow_zero=True)
        if self.completeness not in _COMPLETE_VALUES:
            raise ContractValidationError("invalid allocation baseline completeness")
        if type(self.page_exhausted) is not bool or type(self.filters_applied) is not bool:
            raise ContractValidationError("invalid allocation baseline coverage flags")
        token = self.continuation_token
        if token is not None:
            object.__setattr__(self, "continuation_token", _text(token, "continuation token"))
        if self.page_exhausted == (token is not None):
            raise ContractValidationError(
                "allocation baseline continuation does not match page state"
            )
        positions = _freeze_tuple(self.positions, "owner baseline positions")
        if any(type(item) is not OwnerPosition for item in positions):
            raise ContractValidationError("invalid owner baseline position")
        if len({(item.owner_key, item.instrument) for item in positions}) != len(positions):
            raise ContractValidationError("duplicate owner baseline position")
        object.__setattr__(self, "positions", positions)

    @property
    def complete(self) -> bool:
        return self.completeness == "COMPLETE" and self.page_exhausted and not self.filters_applied


@dataclass(frozen=True, slots=True)
class ExternalFillFact:
    """One provider trade with optional trusted owner attribution."""

    fill_ref: str
    instrument: str
    side: Side
    quantity: Decimal
    price: Decimal
    occurred_at_ns: int
    owner_kind: AllocationOwnerKind | None = None
    owner_ref: str | None = None
    fee: Decimal | None = None
    fee_currency: str | None = None
    provider_order_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "fill_ref", _text(self.fill_ref, "external fill ref"))
        object.__setattr__(
            self, "instrument", _text(self.instrument, "instrument", pattern=_INSTRUMENT)
        )
        try:
            object.__setattr__(self, "side", Side(self.side))
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid external fill side") from error
        object.__setattr__(
            self, "quantity", _decimal(self.quantity, "external fill quantity", positive=True)
        )
        object.__setattr__(
            self, "price", _decimal(self.price, "external fill price", positive=True)
        )
        _timestamp(self.occurred_at_ns, "external fill time")
        if self.owner_kind is None:
            if self.owner_ref is not None:
                raise ContractValidationError("external owner ref requires owner kind")
        else:
            try:
                kind = AllocationOwnerKind(self.owner_kind)
            except (TypeError, ValueError) as error:
                raise ContractValidationError("invalid external owner kind") from error
            if kind is AllocationOwnerKind.UNALLOCATED:
                raise ContractValidationError("unattributed external fills omit owner attribution")
            if self.owner_ref is None:
                raise ContractValidationError("external owner kind requires owner ref")
            if self.provider_order_ref is None:
                raise ContractValidationError(
                    "attributed external fill requires its provider order"
                )
            object.__setattr__(self, "owner_kind", kind)
            object.__setattr__(self, "owner_ref", _text(self.owner_ref, "external owner ref"))
        if self.provider_order_ref is not None:
            object.__setattr__(
                self, "provider_order_ref", _text(self.provider_order_ref, "provider order ref")
            )
        if self.fee is not None:
            object.__setattr__(self, "fee", _decimal(self.fee, "external fill fee"))
        if self.fee_currency is not None:
            if self.fee is None:
                raise ContractValidationError("external fee currency requires fee amount")
            object.__setattr__(self, "fee_currency", _currency(self.fee_currency))


@dataclass(frozen=True, slots=True)
class ExternalOrderFact:
    """One account-wide provider order observation with optional trusted owner."""

    provider_order_ref: str
    instrument: str
    side: Side
    quantity: Decimal
    filled_quantity: Decimal
    state: str
    observed_at_ns: int
    owner_kind: AllocationOwnerKind | None = None
    owner_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "provider_order_ref", _text(self.provider_order_ref, "provider order ref")
        )
        object.__setattr__(
            self, "instrument", _text(self.instrument, "instrument", pattern=_INSTRUMENT)
        )
        try:
            object.__setattr__(self, "side", Side(self.side))
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid external order side") from error
        quantity = _decimal(self.quantity, "external order quantity", positive=True)
        filled = _decimal(self.filled_quantity, "external order filled quantity")
        if filled < 0 or filled > quantity:
            raise ContractValidationError("invalid external order filled quantity")
        if self.state not in {
            "OPEN",
            "PARTIALLY_FILLED",
            "FILLED",
            "CANCELLED",
            "REJECTED",
            "UNKNOWN",
        }:
            raise ContractValidationError("invalid external order state")
        if self.state == "OPEN" and filled != 0:
            raise ContractValidationError("open external order cannot include fills")
        if self.state == "PARTIALLY_FILLED" and not (0 < filled < quantity):
            raise ContractValidationError("partial external order fill is out of range")
        if self.state == "FILLED" and filled != quantity:
            raise ContractValidationError("filled external order quantity is incomplete")
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "filled_quantity", filled)
        _timestamp(self.observed_at_ns, "external order observation time")
        if self.owner_kind is None:
            if self.owner_ref is not None:
                raise ContractValidationError("external order owner ref requires owner kind")
        else:
            try:
                kind = AllocationOwnerKind(self.owner_kind)
            except (TypeError, ValueError) as error:
                raise ContractValidationError("invalid external order owner kind") from error
            if kind is AllocationOwnerKind.UNALLOCATED:
                raise ContractValidationError("unattributed external orders omit owner attribution")
            if self.owner_ref is None:
                raise ContractValidationError("external order owner kind requires owner ref")
            object.__setattr__(self, "owner_kind", kind)
            object.__setattr__(self, "owner_ref", _text(self.owner_ref, "external order owner ref"))


@dataclass(frozen=True, slots=True)
class ExternalActivitySnapshot:
    """Page-qualified trade facts from the provider's account-wide activity feed."""

    account_key: str
    source: str
    source_ref: str
    generation: str
    coverage_start_ns: int
    coverage_end_ns: int
    fills: tuple[ExternalFillFact, ...]
    orders: tuple[ExternalOrderFact, ...] = ()
    completeness: str = "COMPLETE"
    page_exhausted: bool = True
    filters_applied: bool = False
    continuation_token: str | None = None

    def __post_init__(self) -> None:
        for name in ("account_key", "source", "source_ref", "generation"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        _timestamp(self.coverage_start_ns, "external activity coverage start")
        _timestamp(self.coverage_end_ns, "external activity coverage end")
        if self.coverage_end_ns < self.coverage_start_ns:
            raise ContractValidationError("external activity interval is reversed")
        if self.completeness not in _COMPLETE_VALUES:
            raise ContractValidationError("invalid external activity completeness")
        if type(self.page_exhausted) is not bool or type(self.filters_applied) is not bool:
            raise ContractValidationError("invalid external activity coverage flags")
        token = self.continuation_token
        if token is not None:
            object.__setattr__(self, "continuation_token", _text(token, "continuation token"))
        if self.page_exhausted == (token is not None):
            raise ContractValidationError("external continuation does not match page state")
        fills = _freeze_tuple(self.fills, "external fills")
        orders = _freeze_tuple(self.orders, "external orders")
        if any(type(item) is not ExternalFillFact for item in fills):
            raise ContractValidationError("invalid external fill fact")
        if any(type(item) is not ExternalOrderFact for item in orders):
            raise ContractValidationError("invalid external order fact")
        if len({item.fill_ref for item in fills}) != len(fills):
            raise ContractValidationError("duplicate external fill reference")
        order_map = {item.provider_order_ref: item for item in orders}
        if len(order_map) != len(orders):
            raise ContractValidationError("duplicate provider order reference")
        if any(
            item.occurred_at_ns <= self.coverage_start_ns
            or item.occurred_at_ns > self.coverage_end_ns
            for item in fills
        ):
            raise ContractValidationError("external fill lies outside its declared coverage")
        if any(
            item.observed_at_ns < self.coverage_start_ns
            or item.observed_at_ns > self.coverage_end_ns
            for item in orders
        ):
            raise ContractValidationError("external order lies outside its declared coverage")
        for fill in fills:
            if fill.provider_order_ref is None:
                continue
            order = order_map.get(fill.provider_order_ref)
            if order is None:
                if fill.owner_kind is not None:
                    raise ContractValidationError(
                        "attributed external fill lacks its provider order"
                    )
                continue
            if fill.instrument != order.instrument or fill.side is not order.side:
                raise ContractValidationError("external fill does not match its provider order")
            if fill.owner_kind is not None and (
                order.owner_kind is not fill.owner_kind or order.owner_ref != fill.owner_ref
            ):
                raise ContractValidationError(
                    "attributed external fill lacks a matching attributed provider order"
                )
        fill_quantity_by_order: dict[str, Decimal] = defaultdict(Decimal)
        for fill in fills:
            if fill.provider_order_ref is not None:
                fill_quantity_by_order[fill.provider_order_ref] += fill.quantity
        if any(
            quantity > order_map[provider_order_ref].filled_quantity
            for provider_order_ref, quantity in fill_quantity_by_order.items()
            if provider_order_ref in order_map
        ):
            raise ContractValidationError("external fills exceed their provider order quantity")
        object.__setattr__(self, "fills", fills)
        object.__setattr__(self, "orders", orders)

    @property
    def complete(self) -> bool:
        return self.completeness == "COMPLETE" and self.page_exhausted and not self.filters_applied


@dataclass(frozen=True, slots=True)
class ManagedFillAllocation:
    owner_kind: AllocationOwnerKind
    owner_ref: str
    intent_id: str
    provider_order_id: str | None
    instrument: str
    side: Side
    quantity: Decimal
    signed_quantity: Decimal
    interval_vwap: Decimal | None
    notional_delta: Decimal | None
    fee_delta: Decimal | None
    fee_currency: str | None
    baseline_sequence: int | None
    current_sequence: int | None


@dataclass(frozen=True, slots=True)
class ManagedOrderBookEntry:
    owner_kind: AllocationOwnerKind
    owner_ref: str
    intent_id: str
    provider_order_id: str | None
    instrument: str
    side: Side
    state: ExecutionState
    requested_quantity: Decimal
    filled_quantity: Decimal
    remaining_quantity: Decimal


@dataclass(frozen=True, slots=True)
class OwnerBook:
    owner_kind: AllocationOwnerKind
    owner_ref: str
    current_positions: tuple[PositionFact, ...]
    fees_since_baseline: tuple[CurrencyAmount, ...]


@dataclass(frozen=True, slots=True)
class OutboxContinuation:
    scope_key: str
    owner_kind: AllocationOwnerKind
    owner_ref: str
    after_sequence: int
    through_sequence: int


@dataclass(frozen=True, slots=True)
class InputContinuation:
    source_kind: str
    source_ref: str
    token: str


@dataclass(frozen=True, slots=True)
class QuarantinedExecutionIntent:
    scope_key: str
    intent_id: str


@dataclass(frozen=True, slots=True)
class AccountBookCoverage:
    account_key: str
    scope_directory_revision: str
    journal_generation: str
    baseline_execution_cursor: int
    current_execution_cursor: int
    complete: bool
    reasons: tuple[str, ...]
    scanned_outbox_events: int
    outbox_history_contiguous: bool
    outbox_scan_complete: bool
    continuations: tuple[OutboxContinuation, ...]
    input_continuations: tuple[InputContinuation, ...]
    quarantined_intents: tuple[QuarantinedExecutionIntent, ...] = ()


@dataclass(frozen=True, slots=True)
class AccountBookReconciliation:
    coverage: AccountBookCoverage
    state: str
    books: tuple[OwnerBook, ...]
    managed_orders: tuple[ManagedOrderBookEntry, ...]
    managed_fills: tuple[ManagedFillAllocation, ...]
    external_orders: tuple[ExternalOrderFact, ...]
    external_fills: tuple[ExternalFillFact, ...]
    provider_position_change: tuple[PositionFact, ...]
    position_residual: tuple[PositionFact, ...]
    fee_residual: tuple[CurrencyAmount, ...]


@dataclass(slots=True)
class _CumulativeObservation:
    sequence: int
    state: ExecutionState
    filled_quantity: Decimal
    average_price: Decimal | None
    commission: Decimal | None
    commission_currency: str | None


@dataclass(slots=True)
class _OrderTimeline:
    owner: ExecutionOwnerScope
    baseline: _CumulativeObservation | None = None
    current: _CumulativeObservation | None = None
    baseline_state: ExecutionState | None = None
    current_state: ExecutionState | None = None
    changed_since_baseline: bool = False
    review_required: bool = False


class ExecutionAccountBookService:
    """Reconcile trusted virtual books to an account-wide provider snapshot."""

    def __init__(
        self,
        store: SqliteExecutionStore,
        *,
        page_size: int = 1_000,
        max_outbox_events: int = 100_000,
    ) -> None:
        if type(store) is not SqliteExecutionStore:
            raise ContractValidationError("account book requires the execution SQLite store")
        if type(page_size) is not int or not 1 <= page_size <= 10_000:
            raise ContractValidationError("invalid account book page size")
        if type(max_outbox_events) is not int or max_outbox_events <= 0:
            raise ContractValidationError("invalid account book outbox bound")
        self._store = store
        self._page_size = page_size
        self._max_outbox_events = max_outbox_events

    def reconcile(
        self,
        directory: ExecutionScopeDirectory,
        baseline_snapshot: ExecutionAccountSnapshot,
        current_snapshot: ExecutionAccountSnapshot,
        *,
        allocation_baseline: ExecutionAllocationBaseline | None,
        external_activity: ExternalActivitySnapshot | None,
    ) -> AccountBookReconciliation:
        """Build current named books and the exact account residual.

        Missing/filtered/truncated sources reduce coverage to PARTIAL; they do
        not turn omitted positions or fees into zero. Unattributed provider
        trades and unexplained net position/fee changes remain UNALLOCATED.
        """

        if any(
            type(value) is not expected
            for value, expected in (
                (directory, ExecutionScopeDirectory),
                (baseline_snapshot, ExecutionAccountSnapshot),
                (current_snapshot, ExecutionAccountSnapshot),
            )
        ):
            raise ContractValidationError("invalid account book reconciliation inputs")
        account_key = directory.account_key
        if (
            baseline_snapshot.account_key != account_key
            or current_snapshot.account_key != account_key
        ):
            raise ContractValidationError("account snapshots do not match the owner directory")
        if (
            baseline_snapshot.source != current_snapshot.source
            or baseline_snapshot.generation != current_snapshot.generation
            or baseline_snapshot.position_mode != current_snapshot.position_mode
            or baseline_snapshot.fee_coverage_start_ns != current_snapshot.fee_coverage_start_ns
        ):
            raise ContractValidationError(
                "account snapshots do not share one comparable generation"
            )
        if (
            current_snapshot.as_of_ns < baseline_snapshot.as_of_ns
            or current_snapshot.execution_cursor < baseline_snapshot.execution_cursor
        ):
            raise ContractValidationError("account snapshot interval is reversed")
        if not (
            directory.covers_from_ns <= baseline_snapshot.as_of_ns
            and directory.as_of_ns >= current_snapshot.as_of_ns
        ):
            raise ContractValidationError("scope directory does not cover the snapshot interval")

        journal = self._store.journal_source_identity()
        journal_generation = str(journal["generation"])
        if any(
            snapshot.journal_generation != journal_generation
            for snapshot in (baseline_snapshot, current_snapshot)
        ):
            raise ContractValidationError("account snapshots reference another execution journal")
        row_count, first_sequence, last_sequence, sequence_high_water = (
            self._store.execution_outbox_bounds()
        )
        head = 0 if last_sequence is None else last_sequence
        if current_snapshot.execution_cursor > head:
            raise ContractValidationError(
                "account snapshot cursor is ahead of the execution outbox"
            )
        history_contiguous = (
            row_count == 0
            and first_sequence is None
            and last_sequence is None
            and sequence_high_water == 0
        ) or (
            row_count > 0
            and first_sequence == 1
            and last_sequence == row_count
            and sequence_high_water == row_count
        )

        reasons: set[str] = set()
        input_continuations: list[InputContinuation] = []
        if not history_contiguous:
            reasons.add("execution_outbox_history_gap")
        if not directory.complete:
            reasons.add("scope_directory_incomplete")
        if directory.continuation_token is not None:
            input_continuations.append(
                InputContinuation(
                    "scope_directory", directory.source_ref, directory.continuation_token
                )
            )
        for label, snapshot in (("baseline", baseline_snapshot), ("current", current_snapshot)):
            if not snapshot.coverage_complete:
                reasons.add(f"{label}_account_snapshot_incomplete")
            if snapshot.continuation_token is not None:
                input_continuations.append(
                    InputContinuation(
                        label + "_account_snapshot",
                        snapshot.source_ref,
                        snapshot.continuation_token,
                    )
                )

        base_positions: dict[tuple[str, str, str], Decimal] = {}
        if allocation_baseline is None:
            reasons.add("allocation_baseline_missing")
        else:
            self._validate_allocation_baseline(
                allocation_baseline,
                directory,
                baseline_snapshot,
                journal_generation,
            )
            if not allocation_baseline.complete:
                reasons.add("allocation_baseline_incomplete")
            for fact in allocation_baseline.positions:
                base_positions[(fact.owner_kind.value, fact.owner_ref, fact.instrument)] = (
                    fact.net_quantity
                )

        if external_activity is None:
            reasons.add("external_activity_snapshot_missing")
            external_fills: tuple[ExternalFillFact, ...] = ()
            external_orders: tuple[ExternalOrderFact, ...] = ()
        else:
            self._validate_external_activity(
                external_activity, directory, baseline_snapshot, current_snapshot
            )
            external_fills = external_activity.fills
            external_orders = external_activity.orders
            if not external_activity.complete:
                reasons.add("external_activity_incomplete")
            if any(order.state == "UNKNOWN" for order in external_orders):
                reasons.add("external_order_requires_reconciliation")
            if external_activity.continuation_token is not None:
                input_continuations.append(
                    InputContinuation(
                        "external_activity",
                        external_activity.source_ref,
                        external_activity.continuation_token,
                    )
                )

        if not baseline_snapshot.fees_complete or not current_snapshot.fees_complete:
            reasons.add("account_fee_totals_incomplete")

        owner_by_scope = {item.scope.key: item for item in directory.scopes}
        timelines, scanned, continuations, quarantined_intents = self._read_timelines(
            directory,
            baseline_snapshot.execution_cursor,
            current_snapshot.execution_cursor,
            journal_generation,
        )
        if quarantined_intents:
            reasons.add("execution_outbox_lineage_mismatch")
        if self._store.execution_outbox_bounds() != (
            row_count,
            first_sequence,
            last_sequence,
            sequence_high_water,
        ):
            reasons.add("execution_outbox_changed_during_reconciliation")
        if continuations:
            reasons.add("execution_outbox_scan_limited")

        positions: dict[tuple[str, str, str], Decimal] = defaultdict(Decimal)
        positions.update(base_positions)
        fees: dict[tuple[str, str, str], Decimal] = defaultdict(Decimal)
        managed_allocations: list[ManagedFillAllocation] = []
        managed_orders: list[ManagedOrderBookEntry] = []
        managed_position_changes: dict[str, Decimal] = defaultdict(Decimal)
        managed_fee_changes: dict[str, Decimal] = defaultdict(Decimal)
        instruments: set[str] = {item.instrument for item in baseline_snapshot.positions} | {
            item.instrument for item in current_snapshot.positions
        }

        for (_scope_key, intent_id), timeline in timelines.items():
            owner = owner_by_scope.get(timeline.owner.scope.key)
            if owner is None:
                # The directory itself is the authority boundary. Never assign
                # a durable order from an unregistered scope by guesswork.
                reasons.add("execution_scope_missing_from_directory")
                continue
            intent = self._store.get_intent(intent_id, scope=owner.scope)
            if intent is None or intent.scope != owner.scope:
                raise DurableStoreError("execution outbox observation has no scoped intent")
            base = timeline.baseline
            current = timeline.current or base
            q0 = Decimal("0") if base is None else base.filled_quantity
            q1 = q0 if current is None else current.filled_quantity
            if q1 < q0:
                reasons.add("non_monotonic_execution_fill")
                continue
            if q1 > intent.quantity:
                raise DurableStoreError("execution outbox fill exceeds its immutable intent")
            order_state = timeline.current_state or timeline.baseline_state
            if order_state is None:
                raise DurableStoreError("execution outbox order state is unavailable")
            managed_orders.append(
                ManagedOrderBookEntry(
                    owner_kind=owner.owner_kind,
                    owner_ref=owner.owner_ref,
                    intent_id=intent_id,
                    provider_order_id=self._provider_order_id(intent_id, owner.scope),
                    instrument=intent.instrument,
                    side=intent.side,
                    state=order_state,
                    requested_quantity=intent.quantity,
                    filled_quantity=q1,
                    remaining_quantity=intent.quantity - q1,
                )
            )
            quantity_delta = q1 - q0
            if timeline.current_state is ExecutionState.UNKNOWN:
                reasons.add("unresolved_unknown_order")
            if timeline.baseline_state is ExecutionState.UNKNOWN and q1 > 0:
                reasons.add("unknown_order_crosses_baseline")
            if timeline.review_required:
                reasons.add("execution_intent_requires_review")
            if not timeline.changed_since_baseline:
                quantity_delta = Decimal("0")
            signed_delta = quantity_delta if intent.side is Side.BUY else -quantity_delta
            instruments.add(intent.instrument)
            if quantity_delta:
                key = (owner.owner_kind.value, owner.owner_ref, intent.instrument)
                positions[key] += signed_delta
                managed_position_changes[intent.instrument] += signed_delta

            interval_vwap: Decimal | None = None
            notional_delta: Decimal | None = None
            if quantity_delta:
                current_price = None if current is None else current.average_price
                base_price = None if base is None else base.average_price
                if current_price is None or (q0 > 0 and base_price is None):
                    reasons.add("managed_fill_price_incomplete")
                else:
                    current_notional = q1 * current_price
                    base_notional = Decimal("0") if q0 == 0 else q0 * base_price
                    notional_delta = current_notional - base_notional
                    if notional_delta <= 0:
                        notional_delta = None
                        reasons.add("managed_fill_notional_invalid")
                    else:
                        interval_vwap = notional_delta / quantity_delta

            fee_delta: Decimal | None = Decimal("0")
            fee_currency: str | None = None
            if timeline.changed_since_baseline:
                c0 = None if base is None else base.commission
                c1 = None if current is None else current.commission
                currency0 = None if base is None else base.commission_currency
                currency1 = None if current is None else current.commission_currency
                if c1 is None:
                    if quantity_delta:
                        fee_delta = None
                        reasons.add("managed_fill_fee_incomplete")
                elif c0 is None:
                    if q0 == 0:
                        fee_delta = c1
                        fee_currency = currency1
                    else:
                        fee_delta = None
                        reasons.add("managed_fee_baseline_missing")
                else:
                    fee_delta = c1 - c0
                    if currency0 is not None and currency1 is not None and currency0 != currency1:
                        fee_delta = None
                        reasons.add("managed_fee_currency_changed")
                    else:
                        fee_currency = currency1 or currency0
                if fee_delta is not None and fee_currency is None and (quantity_delta or fee_delta):
                    fee_delta = None
                    reasons.add("managed_fee_currency_missing")
                if fee_delta is not None and fee_currency is not None:
                    fees[(owner.owner_kind.value, owner.owner_ref, fee_currency)] += fee_delta
                    managed_fee_changes[fee_currency] += fee_delta

            if quantity_delta or (fee_delta not in (None, Decimal("0"))):
                managed_allocations.append(
                    ManagedFillAllocation(
                        owner_kind=owner.owner_kind,
                        owner_ref=owner.owner_ref,
                        intent_id=intent_id,
                        provider_order_id=(
                            None
                            if current is None
                            else self._provider_order_id(intent_id, owner.scope)
                        ),
                        instrument=intent.instrument,
                        side=intent.side,
                        quantity=quantity_delta,
                        signed_quantity=signed_delta,
                        interval_vwap=interval_vwap,
                        notional_delta=notional_delta,
                        fee_delta=fee_delta,
                        fee_currency=fee_currency,
                        baseline_sequence=None if base is None else base.sequence,
                        current_sequence=None if current is None else current.sequence,
                    )
                )

        external_position_changes: dict[str, Decimal] = defaultdict(Decimal)
        external_order_by_ref = {item.provider_order_ref: item for item in external_orders}
        external_owner_by_fill: dict[str, tuple[str, str]] = {}
        for fill in external_fills:
            signed = fill.quantity if fill.side is Side.BUY else -fill.quantity
            instruments.add(fill.instrument)
            external_position_changes[fill.instrument] += signed
            order = (
                None
                if fill.provider_order_ref is None
                else external_order_by_ref.get(fill.provider_order_ref)
            )
            effective_kind = fill.owner_kind or (None if order is None else order.owner_kind)
            effective_ref = fill.owner_ref or (None if order is None else order.owner_ref)
            key = (
                _UNALLOCATED
                if effective_kind is None
                else (effective_kind.value, effective_ref or "")
            )
            if fill.owner_kind is not None and key not in {
                owner.owner_key for owner in directory.scopes
            }:
                raise ContractValidationError(
                    "external fill attribution is outside the owner directory"
                )
            if effective_kind is not None and key not in {
                owner.owner_key for owner in directory.scopes
            }:
                raise ContractValidationError(
                    "provider order attribution is outside the owner directory"
                )
            external_owner_by_fill[fill.fill_ref] = key
            positions[(key[0], key[1], fill.instrument)] += signed
            if fill.fee is None or fill.fee_currency is None:
                reasons.add("external_fill_fee_incomplete")
            else:
                fees[(key[0], key[1], fill.fee_currency)] += fill.fee

        baseline_account_positions = self._position_map(baseline_snapshot.positions)
        current_account_positions = self._position_map(current_snapshot.positions)
        provider_position_change: dict[str, Decimal] = {}
        position_residual: dict[str, Decimal] = {}
        usable_current_positions: dict[str, Decimal] = {}
        for instrument in instruments:
            before = self._position_value(baseline_snapshot, baseline_account_positions, instrument)
            after = self._position_value(current_snapshot, current_account_positions, instrument)
            if after is not None:
                usable_current_positions[instrument] = after
            if before is None or after is None:
                reasons.add("position_universe_incomplete_for_instrument")
                continue
            provider_position_change[instrument] = after - before
            position_residual[instrument] = (
                (after - before)
                - managed_position_changes[instrument]
                - external_position_changes[instrument]
            )

        # The account's current net position is authoritative. Named books keep
        # only explicit baselines/fills; everything else remains UNALLOCATED.
        named_keys = {owner.owner_key for owner in directory.scopes}
        unallocated_positions: dict[str, Decimal] = {}
        for instrument in instruments:
            if instrument not in usable_current_positions:
                continue
            named_quantity = sum(
                quantity
                for (kind, _ref, candidate), quantity in positions.items()
                if candidate == instrument and (kind, _ref) in named_keys
            )
            unallocated_positions[instrument] = (
                usable_current_positions[instrument] - named_quantity
            )
            positions[(*_UNALLOCATED, instrument)] = unallocated_positions[instrument]

        baseline_fees = self._fee_map(baseline_snapshot.cumulative_fees)
        current_fees = self._fee_map(current_snapshot.cumulative_fees)
        currencies = set(baseline_fees) | set(current_fees) | set(managed_fee_changes)
        currencies.update(
            fill.fee_currency for fill in external_fills if fill.fee_currency is not None
        )
        account_fee_change: dict[str, Decimal] = {}
        for currency in currencies:
            if currency not in baseline_fees and not baseline_snapshot.fees_complete:
                reasons.add("account_fee_currency_uncovered")
                continue
            if currency not in current_fees and not current_snapshot.fees_complete:
                reasons.add("account_fee_currency_uncovered")
                continue
            account_fee_change[currency] = current_fees.get(
                currency, Decimal("0")
            ) - baseline_fees.get(currency, Decimal("0"))
        all_external_fee_changes: dict[str, Decimal] = defaultdict(Decimal)
        named_fee_changes: dict[str, Decimal] = defaultdict(Decimal)
        for fill in external_fills:
            if fill.fee is None or fill.fee_currency is None:
                continue
            all_external_fee_changes[fill.fee_currency] += fill.fee
            if external_owner_by_fill.get(fill.fill_ref, _UNALLOCATED) != _UNALLOCATED:
                named_fee_changes[fill.fee_currency] += fill.fee
        fee_residual = {
            currency: amount
            - managed_fee_changes.get(currency, Decimal("0"))
            - all_external_fee_changes.get(currency, Decimal("0"))
            for currency, amount in account_fee_change.items()
        }
        for currency in currencies:
            if currency not in account_fee_change:
                continue
            # Preserve the provider's total on the residual book; fee facts
            # without trusted owner attribution cannot be assigned to a strategy.
            named_total = managed_fee_changes.get(currency, Decimal("0")) + named_fee_changes.get(
                currency, Decimal("0")
            )
            unallocated_fee = account_fee_change.get(currency, Decimal("0")) - named_total
            fees[(*_UNALLOCATED, currency)] = unallocated_fee

        books = self._build_books(positions, fees, directory)
        nonzero_position_residual = any(value != 0 for value in position_residual.values())
        nonzero_fee_residual = any(value != 0 for value in fee_residual.values())
        coverage = AccountBookCoverage(
            account_key=account_key,
            scope_directory_revision=directory.revision,
            journal_generation=journal_generation,
            baseline_execution_cursor=baseline_snapshot.execution_cursor,
            current_execution_cursor=current_snapshot.execution_cursor,
            complete=not reasons,
            reasons=tuple(sorted(reasons)),
            scanned_outbox_events=scanned,
            outbox_history_contiguous=history_contiguous,
            outbox_scan_complete=not continuations,
            continuations=continuations,
            input_continuations=tuple(input_continuations),
            quarantined_intents=quarantined_intents,
        )
        return AccountBookReconciliation(
            coverage=coverage,
            state=(
                "PARTIAL"
                if reasons
                else (
                    "RESIDUAL" if nonzero_position_residual or nonzero_fee_residual else "BALANCED"
                )
            ),
            books=books,
            managed_orders=tuple(
                sorted(
                    managed_orders,
                    key=lambda item: (item.owner_kind.value, item.owner_ref, item.intent_id),
                )
            ),
            managed_fills=tuple(
                sorted(
                    managed_allocations,
                    key=lambda item: (item.owner_kind.value, item.owner_ref, item.intent_id),
                )
            ),
            external_orders=external_orders,
            external_fills=external_fills,
            provider_position_change=tuple(
                PositionFact(key, value) for key, value in sorted(provider_position_change.items())
            ),
            position_residual=tuple(
                PositionFact(key, value)
                for key, value in sorted(position_residual.items())
                if value != 0
            ),
            fee_residual=tuple(
                CurrencyAmount(key, value)
                for key, value in sorted(fee_residual.items())
                if value != 0
            ),
        )

    def _read_timelines(
        self,
        directory: ExecutionScopeDirectory,
        baseline_cursor: int,
        current_cursor: int,
        journal_generation: str,
    ) -> tuple[
        dict[tuple[str, str], _OrderTimeline],
        int,
        tuple[OutboxContinuation, ...],
        tuple[QuarantinedExecutionIntent, ...],
    ]:
        timelines: dict[tuple[str, str], _OrderTimeline] = {}
        quarantined_intents: set[tuple[str, str]] = set()
        scanned = 0
        continuations: list[OutboxContinuation] = []
        stop = False
        for owner in directory.scopes:
            after = 0
            while True:
                remaining = self._max_outbox_events - scanned
                request_limit = min(self._page_size, max(1, remaining + 1))
                page = self._store.read_outbox(
                    scope=owner.scope,
                    after_sequence=after,
                    limit=request_limit,
                )
                eligible = tuple(event for event in page if event.sequence <= current_cursor)
                process_count = min(len(eligible), remaining)
                for event in eligible[:process_count]:
                    if event.journal_incarnation_id != journal_generation:
                        quarantined_intents.add((owner.scope.key, event.intent_id))
                    else:
                        self._apply_event(timelines, owner, event, baseline_cursor)
                scanned += process_count
                if process_count < len(eligible):
                    continuations.append(
                        OutboxContinuation(
                            owner.scope.key,
                            owner.owner_kind,
                            owner.owner_ref,
                            eligible[process_count - 1].sequence if process_count else after,
                            current_cursor,
                        )
                    )
                    stop = True
                    break
                if not page or len(page) < request_limit or page[-1].sequence > current_cursor:
                    break
                if remaining <= 0:
                    continuations.append(
                        OutboxContinuation(
                            owner.scope.key,
                            owner.owner_kind,
                            owner.owner_ref,
                            after,
                            current_cursor,
                        )
                    )
                    stop = True
                    break
                after = page[-1].sequence
            if stop:
                owner_index = directory.scopes.index(owner)
                continuations.extend(
                    OutboxContinuation(
                        pending.scope.key,
                        pending.owner_kind,
                        pending.owner_ref,
                        0,
                        current_cursor,
                    )
                    for pending in directory.scopes[owner_index + 1 :]
                )
                break
        for intent_key in quarantined_intents:
            timelines.pop(intent_key, None)
        return (
            timelines,
            scanned,
            tuple(continuations),
            tuple(
                QuarantinedExecutionIntent(scope_key, intent_id)
                for scope_key, intent_id in sorted(quarantined_intents)
            ),
        )

    @staticmethod
    def _position_value(
        snapshot: ExecutionAccountSnapshot,
        positions: dict[str, Decimal],
        instrument: str,
    ) -> Decimal | None:
        if instrument in positions:
            return positions[instrument]
        if (
            snapshot.position_universe_complete
            and snapshot.page_exhausted
            and not snapshot.filters_applied
        ):
            return Decimal("0")
        return None

    @staticmethod
    def _apply_event(
        timelines: dict[tuple[str, str], _OrderTimeline],
        owner: ExecutionOwnerScope,
        event: ExecutionEvent,
        baseline_cursor: int,
    ) -> None:
        if event.scope_key != owner.scope.key:
            raise DurableStoreError("scoped outbox read returned another execution scope")
        intent_key = (event.scope_key, event.intent_id)
        timeline = timelines.setdefault(intent_key, _OrderTimeline(owner=owner))
        if timeline.owner != owner:
            raise DurableStoreError("execution intent appears under conflicting owner scopes")
        is_baseline = event.sequence <= baseline_cursor
        if is_baseline:
            timeline.baseline_state = event.state
            timeline.current_state = event.state
        else:
            timeline.current_state = event.state
            timeline.changed_since_baseline = True
        if event.event_type == "manual_review_required":
            timeline.review_required = True
        if event.event_type not in _OBSERVATION_EVENTS:
            return
        payload = event.payload
        try:
            quantity = _decimal(payload["filled_quantity"], "outbox filled quantity")
            price = (
                None
                if payload.get("average_price") is None
                else _decimal(payload["average_price"], "outbox average price", positive=True)
            )
            commission = (
                None
                if payload.get("cumulative_commission") is None
                else _decimal(payload["cumulative_commission"], "outbox cumulative commission")
            )
            currency = payload.get("cumulative_commission_currency")
            if currency is not None:
                currency = _currency(currency)
        except (KeyError, TypeError, InvalidOperation, ContractValidationError) as error:
            raise DurableStoreError("execution observation outbox payload is invalid") from error
        previous = timeline.baseline if is_baseline else timeline.current or timeline.baseline
        if commission is not None and currency is None and previous is not None:
            currency = previous.commission_currency
        observation = _CumulativeObservation(
            sequence=event.sequence,
            state=event.state,
            filled_quantity=quantity,
            average_price=price,
            commission=commission,
            commission_currency=currency,
        )
        if is_baseline:
            timeline.baseline = observation
            if timeline.current is None:
                timeline.current = observation
                timeline.current_state = event.state
        else:
            timeline.current = observation

    def _validate_allocation_baseline(
        self,
        baseline: ExecutionAllocationBaseline,
        directory: ExecutionScopeDirectory,
        snapshot: ExecutionAccountSnapshot,
        journal_generation: str,
    ) -> None:
        if type(baseline) is not ExecutionAllocationBaseline:
            raise ContractValidationError("invalid allocation baseline")
        if (
            baseline.account_key != directory.account_key
            or baseline.scope_directory_revision != directory.revision
            or baseline.provider_generation != snapshot.generation
            or baseline.journal_generation != journal_generation
            or baseline.as_of_ns != snapshot.as_of_ns
            or baseline.execution_cursor != snapshot.execution_cursor
        ):
            raise ContractValidationError(
                "allocation baseline is not bound to the baseline snapshot"
            )
        known = {owner.owner_key for owner in directory.scopes}
        if any(item.owner_key not in known for item in baseline.positions):
            raise ContractValidationError("allocation baseline contains an unregistered owner")

    @staticmethod
    def _validate_external_activity(
        activity: ExternalActivitySnapshot,
        directory: ExecutionScopeDirectory,
        baseline: ExecutionAccountSnapshot,
        current: ExecutionAccountSnapshot,
    ) -> None:
        if type(activity) is not ExternalActivitySnapshot:
            raise ContractValidationError("invalid external activity snapshot")
        if (
            activity.account_key != directory.account_key
            or activity.generation != current.generation
            or activity.coverage_start_ns != baseline.as_of_ns
            or activity.coverage_end_ns < current.as_of_ns
        ):
            raise ContractValidationError("external activity does not cover the account interval")
        known = {owner.owner_key for owner in directory.scopes}
        if any(
            item.owner_kind is not None
            and (item.owner_kind.value, item.owner_ref or "") not in known
            for item in (*activity.fills, *activity.orders)
        ):
            raise ContractValidationError("external activity has an owner outside the directory")

    def _provider_order_id(self, intent_id: str, scope: ExecutionScope) -> str | None:
        record = self._store.get(intent_id, scope=scope)
        return None if record is None else record.provider_order_id

    @staticmethod
    def _position_map(values: tuple[PositionFact, ...]) -> dict[str, Decimal]:
        return {item.instrument: item.net_quantity for item in values}

    @staticmethod
    def _fee_map(values: tuple[CurrencyAmount, ...]) -> dict[str, Decimal]:
        return {item.currency: item.amount for item in values}

    @staticmethod
    def _build_books(
        positions: dict[tuple[str, str, str], Decimal],
        fees: dict[tuple[str, str, str], Decimal],
        directory: ExecutionScopeDirectory,
    ) -> tuple[OwnerBook, ...]:
        owners = {owner.owner_key for owner in directory.scopes}
        owners.add(_UNALLOCATED)
        books: list[OwnerBook] = []
        for kind, owner_ref in sorted(owners):
            kind_value = AllocationOwnerKind(kind)
            books.append(
                OwnerBook(
                    owner_kind=kind_value,
                    owner_ref=owner_ref,
                    current_positions=tuple(
                        PositionFact(instrument, quantity)
                        for (_kind, _ref, instrument), quantity in sorted(positions.items())
                        if _kind == kind and _ref == owner_ref and quantity != 0
                    ),
                    fees_since_baseline=tuple(
                        CurrencyAmount(currency, amount)
                        for (_kind, _ref, currency), amount in sorted(fees.items())
                        if _kind == kind and _ref == owner_ref and amount != 0
                    ),
                )
            )
        return tuple(books)
