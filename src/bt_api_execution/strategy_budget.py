"""Per-strategy budget admission (iteration 60, step 60.07).

Several strategies share one account; each gets an internal budget that is a
bookkeeping limit, not an exchange balance (plan INV-10). This admission gate
denies an opening intent whose notional would push the strategy's committed
capital above its budget. Budget policy v1 ("gross exposure at cost"):

    committed = sum(|net position| * average entry price * multiplier)
              + sum(remaining quantity of live opening orders * limit price * multiplier)
              + notional reserved in this process but not yet persisted
    admit an opening intent only if committed + its notional <= budget

* Everything is recomputed from durable facts on each reservation, so the
  result is the same after a restart. Live includes ``UNKNOWN``: an
  unresolved order keeps its reservation until reconciled.
* An opening intent without a limit price (market order) has no knowable
  notional and is denied unless the caller supplies a reference price
  provider; unknown never counts as zero.
* A strategy without a configured budget is denied (fail closed).
* Closing intents are not limited by budget; chain this gate with
  ``StrategyCloseCapacityGate`` (``inner=``) to enforce both.
* Fees, margin and FX are not modelled by policy v1; budgets are in the
  instrument's quote currency.
"""

from __future__ import annotations

import itertools
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .contracts import ExecutionState, OrderIntent, PositionEffect, Side
from .errors import AdmissionDenied, ContractValidationError

__all__ = ["STRATEGY_BUDGET_POLICY_V1", "StrategyBudgetGate", "StrategyBudgetPermit"]

STRATEGY_BUDGET_POLICY_V1 = "gross_exposure_at_cost.v1"
_CLOSING = frozenset(
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


@dataclass(frozen=True)
class StrategyBudgetPermit:
    permit_id: str
    intent_id: str
    scope_key: str
    opening: bool
    notional: Decimal
    inner_permit: Any = None


def _is_closing(intent: OrderIntent) -> bool:
    return intent.position_effect in _CLOSING or bool(intent.reduce_only)


def _check_decimal_map(values: Mapping[str, Decimal] | None, code: str) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for key, value in dict(values or {}).items():
        if (
            type(key) is not str
            or not key
            or type(value) is not Decimal
            or not value.is_finite()
            or value < 0
        ):
            raise ContractValidationError(code)
        result[key] = value
    return result


class StrategyBudgetGate:
    """Execution admission gate enforcing per-strategy budgets (policy v1)."""

    policy = STRATEGY_BUDGET_POLICY_V1

    def __init__(
        self,
        store: Any,
        *,
        budgets: Mapping[str, Decimal],
        multipliers: Mapping[str, Decimal] | None = None,
        reference_price: Callable[[OrderIntent], Decimal | None] | None = None,
        inner: Any = None,
    ) -> None:
        """``budgets`` maps ``ExecutionScope.key`` to the strategy budget."""

        if not callable(getattr(store, "list_scope_intents", None)):
            raise ContractValidationError("budget gate requires a scope intent reader")
        if reference_price is not None and not callable(reference_price):
            raise ContractValidationError("reference_price must be callable")
        self._store = store
        self._budgets = _check_decimal_map(budgets, "invalid strategy budget")
        self._multipliers = _check_decimal_map(multipliers, "invalid contract multiplier")
        self._reference_price = reference_price
        self._inner = inner
        self._lock = threading.RLock()
        self._permits: dict[str, StrategyBudgetPermit] = {}
        self._sequence = itertools.count(1)

    # ------------------------------------------------------------------

    def _multiplier(self, instrument: str) -> Decimal:
        # Acceptance report P1-1: an unconfigured multiplier is unknown, never 1.
        multiplier = self._multipliers.get(instrument)
        if multiplier is None:
            raise AdmissionDenied("strategy budget unknown: contract multiplier not configured")
        return multiplier

    def _intent_notional(self, intent: OrderIntent) -> Decimal | None:
        price = intent.price
        if price is None and self._reference_price is not None:
            price = self._reference_price(intent)
        if price is None or not price.is_finite() or price <= 0:
            return None
        return intent.quantity * price * self._multiplier(intent.instrument)

    def committed(self, intent: OrderIntent) -> Decimal:
        """Capital the intent's strategy has committed right now (policy v1)."""
        with self._lock:
            return self._committed(intent)

    def _committed(self, intent: OrderIntent) -> Decimal:
        scope = intent.scope
        positions: dict[tuple[str, str], tuple[Decimal, Decimal]] = {}
        committed = _ZERO
        persisted: dict[str, ExecutionState] = {}
        for record, stored in self._store.list_scope_intents(scope):
            persisted[record.intent_id] = record.state
            filled = record.filled_quantity
            if filled and record.average_price is None:
                # Acceptance report P1-1: a fill without a price has unknown cost.
                raise AdmissionDenied("strategy budget unknown: fill without average price")
            if filled:
                key = (stored.instrument, stored.position_side)
                quantity, cost = positions.get(key, (_ZERO, _ZERO))
                signed = filled if stored.side is Side.BUY else -filled
                if _is_closing(stored) and quantity != 0:
                    # Closing releases cost at the running average.
                    average = cost / abs(quantity)
                    remaining = abs(quantity) - min(filled, abs(quantity))
                    positions[key] = (quantity + signed, average * remaining)
                else:
                    positions[key] = (quantity + signed, cost + filled * record.average_price)
            if (
                record.intent_id != intent.intent_id
                and not _is_closing(stored)
                and record.state not in _TERMINAL
                and record.state is not ExecutionState.PENDING_ADMISSION
            ):
                remaining = max(stored.quantity - record.filled_quantity, _ZERO)
                notional = self._intent_notional(stored)
                if remaining and notional is None:
                    # A live order of unknown notional keeps the budget unknown.
                    raise AdmissionDenied("strategy budget unknown: live order without price")
                if remaining:
                    committed += notional * remaining / stored.quantity
        for (instrument, _side), (_quantity, cost) in positions.items():
            committed += cost * self._multiplier(instrument)
        for permit in self._permits.values():
            if (
                permit.opening
                and permit.scope_key == scope.key
                and permit.intent_id != intent.intent_id
                and persisted.get(permit.intent_id, ExecutionState.PENDING_ADMISSION)
                is ExecutionState.PENDING_ADMISSION
            ):
                committed += permit.notional
        return committed

    # ------------------------------------------------------------------
    # Admission gate protocol
    # ------------------------------------------------------------------

    def reserve(self, intent: OrderIntent) -> StrategyBudgetPermit:
        with self._lock:
            opening = not _is_closing(intent)
            notional = _ZERO
            if opening:
                budget = self._budgets.get(intent.scope.key)
                if budget is None:
                    raise AdmissionDenied("strategy has no configured budget")
                notional = self._intent_notional(intent)
                if notional is None:
                    raise AdmissionDenied("opening notional is unknown")
                if self._committed(intent) + notional > budget:
                    raise AdmissionDenied("strategy budget exceeded")
            inner_permit = self._inner.reserve(intent) if self._inner is not None else None
            permit = StrategyBudgetPermit(
                permit_id=f"budget-{next(self._sequence)}",
                intent_id=intent.intent_id,
                scope_key=intent.scope.key,
                opening=opening,
                notional=notional,
                inner_permit=inner_permit,
            )
            self._permits[permit.permit_id] = permit
            return permit

    def validate(self, permit_reference: str, intent: OrderIntent) -> StrategyBudgetPermit:
        permit = self._permit(permit_reference, intent)
        if self._inner is not None:
            self._inner.validate(_inner_reference(permit), intent)
        return permit

    def claim_for_dispatch(
        self, permit_reference: str, intent: OrderIntent
    ) -> StrategyBudgetPermit:
        permit = self._permit(permit_reference, intent)
        if self._inner is not None:
            self._inner.claim_for_dispatch(_inner_reference(permit), intent)
        return permit

    def settle(self, permit_reference: str) -> None:
        with self._lock:
            permit = self._permits.pop(permit_reference, None)
        if permit is not None and self._inner is not None:
            self._inner.settle(_inner_reference(permit))

    def release(self, permit_reference: str, reason: str) -> None:
        with self._lock:
            permit = self._permits.pop(permit_reference, None)
        if permit is not None and self._inner is not None:
            self._inner.release(_inner_reference(permit), reason)

    def _permit(self, permit_reference: str, intent: OrderIntent) -> StrategyBudgetPermit:
        with self._lock:
            permit = self._permits.get(permit_reference)
        if permit is None or permit.intent_id != intent.intent_id:
            raise AdmissionDenied("unknown budget permit")
        return permit


def _inner_reference(permit: StrategyBudgetPermit) -> Any:
    inner = permit.inner_permit
    if isinstance(inner, Mapping):
        return inner.get("permit_id") or inner.get("reference")
    return getattr(inner, "permit_id", None) or getattr(inner, "reference", None) or inner
