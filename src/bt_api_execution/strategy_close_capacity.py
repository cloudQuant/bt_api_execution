"""Owner-scoped close capacity: a strategy can only close what it owns.

Iteration 60, step 60.06. Several strategies share one account and trade the
same instrument. Each strategy is one ``ExecutionScope``. This admission gate
denies a closing intent (``CLOSE*`` effect or ``reduce_only``) whose quantity
exceeds the strategy's own closable quantity, so strategy A can never consume
strategy B's position and two concurrent closes cannot oversell one holding.

Closable quantity is recomputed from durable facts on every reservation::

    closable = baseline + confirmed fills of this scope (per position bucket)
               - remaining quantity of this scope's live closing orders
               - reservations granted in this process but not yet persisted

* Live closing orders include ``UNKNOWN`` ones: an unresolved close keeps its
  reservation until it is reconciled; only terminal states release it.
* Nothing is cached across reservations, so a restarted process derives the
  same capacity from the store. ``baseline_positions`` carries an approved
  initial allocation (for example positions claimed from the account before
  this journal existed); without it a strategy starts from zero.
* Opening intents are not limited here. An optional ``inner`` gate (such as a
  shared Risk gate) is consulted after the capacity check for every intent.

The gate is one object per account composition. All reservations are
serialized by one lock, which is the account coordinator's single logical
writer for close capacity in a single process. It does not prove single
writership across processes or hosts; that requires the deployment's own
fencing (plan INV-08).
"""

from __future__ import annotations

import itertools
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .contracts import ExecutionScope, ExecutionState, OrderIntent, PositionEffect, Side
from .errors import AdmissionDenied, ContractValidationError

__all__ = [
    "StrategyCloseCapacityGate",
    "StrategyClosePermit",
]

_CLOSING_EFFECTS = frozenset(
    {PositionEffect.CLOSE, PositionEffect.CLOSE_TODAY, PositionEffect.CLOSE_YESTERDAY}
)
_TERMINAL = frozenset(
    {
        ExecutionState.FILLED,
        ExecutionState.CANCELLED,
        ExecutionState.REJECTED,
        ExecutionState.BLOCKED,
    }
)
_ZERO = Decimal("0")

# (scope_key, instrument, position_side) -> quantity
BucketKey = tuple[str, str, str]


@dataclass(frozen=True)
class StrategyClosePermit:
    """One granted reservation; ``permit_id`` is what the facade persists."""

    permit_id: str
    intent_id: str
    scope_key: str
    instrument: str
    position_side: str
    closing: bool
    quantity: Decimal
    inner_permit: Any = None


def _is_closing(intent: OrderIntent) -> bool:
    return intent.position_effect in _CLOSING_EFFECTS or bool(intent.reduce_only)


def _signed_fill(intent: OrderIntent, filled: Decimal) -> Decimal:
    """Contribution of a fill to the owner's position bucket.

    NET buckets hold a signed net quantity (BUY +, SELL -). LONG/SHORT
    buckets hold the leg size: opening adds, closing subtracts.
    """
    if intent.position_side == "NET":
        return filled if intent.side is Side.BUY else -filled
    return -filled if _is_closing(intent) else filled


def _closable(intent: OrderIntent, bucket_quantity: Decimal) -> Decimal:
    if intent.position_side == "NET":
        # Selling closes a long net position, buying closes a short one.
        if intent.side is Side.SELL:
            return max(bucket_quantity, _ZERO)
        return max(-bucket_quantity, _ZERO)
    return max(bucket_quantity, _ZERO)


class StrategyCloseCapacityGate:
    """Execution admission gate enforcing per-strategy close capacity."""

    def __init__(
        self,
        store: Any,
        *,
        baseline_positions: Mapping[BucketKey, Decimal] | None = None,
        inner: Any = None,
    ) -> None:
        if not callable(getattr(store, "list_scope_intents", None)):
            raise ContractValidationError("close capacity requires a scope intent reader")
        baseline: dict[BucketKey, Decimal] = {}
        for key, quantity in dict(baseline_positions or {}).items():
            if (
                type(key) is not tuple
                or len(key) != 3
                or not all(type(part) is str and part for part in key)
                or type(quantity) is not Decimal
                or not quantity.is_finite()
            ):
                raise ContractValidationError("invalid close-capacity baseline position")
            baseline[key] = quantity
        self._store = store
        self._baseline = baseline
        self._inner = inner
        self._lock = threading.RLock()
        self._permits: dict[str, StrategyClosePermit] = {}
        self._sequence = itertools.count(1)

    # ------------------------------------------------------------------
    # Capacity
    # ------------------------------------------------------------------

    def closable_quantity(self, intent: OrderIntent) -> Decimal:
        """Return the closable quantity this intent may use right now."""
        with self._lock:
            return self._available(intent)

    def _available(self, intent: OrderIntent) -> Decimal:
        scope: ExecutionScope = intent.scope
        bucket = (scope.key, intent.instrument, intent.position_side)
        position = self._baseline.get(bucket, _ZERO)
        outstanding = _ZERO
        persisted: dict[str, ExecutionState] = {}
        for record, stored in self._store.list_scope_intents(scope):
            persisted[record.intent_id] = record.state
            if (stored.instrument, stored.position_side) != bucket[1:]:
                continue
            position += _signed_fill(stored, record.filled_quantity)
            if (
                record.intent_id != intent.intent_id
                and _is_closing(stored)
                and stored.side is intent.side
                and record.state not in _TERMINAL
                and record.state is not ExecutionState.PENDING_ADMISSION
            ):
                outstanding += max(stored.quantity - record.filled_quantity, _ZERO)
        for permit in self._permits.values():
            if (
                permit.closing
                and permit.intent_id != intent.intent_id
                and (permit.scope_key, permit.instrument, permit.position_side) == bucket
                and persisted.get(permit.intent_id, ExecutionState.PENDING_ADMISSION)
                is ExecutionState.PENDING_ADMISSION
            ):
                # Granted here but not yet visible as a live durable order.
                outstanding += permit.quantity
        return max(_closable(intent, position) - outstanding, _ZERO)

    # ------------------------------------------------------------------
    # Admission gate protocol used by ExecutionFacade
    # ------------------------------------------------------------------

    def reserve(self, intent: OrderIntent) -> StrategyClosePermit:
        with self._lock:
            closing = _is_closing(intent)
            if closing and intent.quantity > self._available(intent):
                raise AdmissionDenied("strategy close quantity exceeds its own position")
            inner_permit = self._inner.reserve(intent) if self._inner is not None else None
            permit = StrategyClosePermit(
                permit_id=f"close-capacity-{next(self._sequence)}",
                intent_id=intent.intent_id,
                scope_key=intent.scope.key,
                instrument=intent.instrument,
                position_side=intent.position_side,
                closing=closing,
                quantity=intent.quantity,
                inner_permit=inner_permit,
            )
            self._permits[permit.permit_id] = permit
            return permit

    def validate(self, permit_reference: str, intent: OrderIntent) -> StrategyClosePermit:
        permit = self._permit(permit_reference, intent)
        if self._inner is not None:
            self._inner.validate(_inner_reference(permit), intent)
        return permit

    def claim_for_dispatch(self, permit_reference: str, intent: OrderIntent) -> StrategyClosePermit:
        permit = self._permit(permit_reference, intent)
        if self._inner is not None:
            self._inner.claim_for_dispatch(_inner_reference(permit), intent)
        return permit

    def settle(self, permit_reference: str) -> None:
        # Once the order is durable its remaining quantity is counted from the
        # store, so the in-process reservation is no longer needed.
        with self._lock:
            permit = self._permits.pop(permit_reference, None)
        if permit is not None and self._inner is not None:
            self._inner.settle(_inner_reference(permit))

    def release(self, permit_reference: str, reason: str) -> None:
        with self._lock:
            permit = self._permits.pop(permit_reference, None)
        if permit is not None and self._inner is not None:
            self._inner.release(_inner_reference(permit), reason)

    def _permit(self, permit_reference: str, intent: OrderIntent) -> StrategyClosePermit:
        with self._lock:
            permit = self._permits.get(permit_reference)
        if permit is None or permit.intent_id != intent.intent_id:
            raise AdmissionDenied("unknown close-capacity permit")
        return permit


def _inner_reference(permit: StrategyClosePermit) -> Any:
    inner = permit.inner_permit
    if isinstance(inner, Mapping):
        return inner.get("permit_id") or inner.get("reference")
    return getattr(inner, "permit_id", None) or getattr(inner, "reference", None) or inner
