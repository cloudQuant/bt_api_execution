"""Adapters for shared admission implementations without importing a risk package.

The execution package owns neither a risk DTO nor a risk ledger.  This small
adapter lets an SDK composition root translate an :class:`OrderIntent` into its
shared ``RiskIntent`` and hand an independently installed risk gate to the
execution facade without creating an import edge from execution to risk.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from .errors import ContractValidationError

if TYPE_CHECKING:
    from collections.abc import Callable

    from .contracts import OrderIntent


class SharedRiskGatePort(Protocol):
    """The narrow structural surface implemented by a shared risk gate."""

    def reserve(self, intent: Any) -> Any: ...

    def validate_permit(self, permit_id: str, intent: Any) -> Any: ...

    def claim_for_dispatch(self, permit_id: str, intent: Any) -> Any: ...

    def settle(self, permit_id: str) -> Any: ...

    def release(self, permit_id: str, reason: str) -> None: ...


class SharedRiskAdmissionAdapter:
    """Adapt a shared risk gate through an injected execution-to-risk mapper.

    The mapper belongs in the SDK composition root because it supplies account
    scope, intent action, and reliable notional metadata.  No provider call,
    credential, or risk model is loaded by this adapter.
    """

    def __init__(
        self,
        risk_gate: SharedRiskGatePort,
        map_intent: Callable[[OrderIntent], Any],
    ) -> None:
        if not callable(map_intent):
            raise ContractValidationError("risk intent mapper is required")
        self._risk_gate = risk_gate
        self._map_intent = map_intent

    def reserve(self, intent: OrderIntent) -> Any:
        return self._risk_gate.reserve(self._map_intent(intent))

    def validate(self, permit_reference: str, intent: OrderIntent) -> Any:
        return self._risk_gate.validate_permit(permit_reference, self._map_intent(intent))

    def claim_for_dispatch(self, permit_reference: str, intent: OrderIntent) -> Any:
        """Atomically bind a validated permit to the provider dispatch boundary.

        The shared risk owner defines the durable claim and any account-wide
        latch it needs.  Falling back to a non-atomic validation here would
        reopen a cross-process check-then-freeze race, so an older gate is a
        hard composition error rather than a compatibility downgrade.
        """

        claim = getattr(self._risk_gate, "claim_for_dispatch", None)
        if not callable(claim):
            raise ContractValidationError("shared risk gate lacks atomic dispatch claim")
        return claim(permit_reference, self._map_intent(intent))

    def settle(self, permit_reference: str) -> Any:
        return self._risk_gate.settle(permit_reference)

    def ensure_settled(self, permit_reference: str) -> Any:
        """Use the risk owner's idempotent recovery-settlement proof."""

        ensure = getattr(self._risk_gate, "ensure_settled", None)
        if not callable(ensure):
            raise ContractValidationError("shared risk gate lacks recovery settlement proof")
        return ensure(permit_reference)

    def release(self, permit_reference: str, reason: str) -> None:
        self._risk_gate.release(permit_reference, reason)
