"""Adapters for shared admission implementations without importing a risk package.

The execution package owns neither a risk DTO nor a risk ledger.  This small
adapter lets an SDK composition root translate an :class:`OrderIntent` into its
shared ``RiskIntent`` and hand an independently installed risk gate to the
execution facade without creating an import edge from execution to risk.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from .errors import ContractValidationError
from .group_contracts import risk_account_scope_fingerprint

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


class AccountScopedGroupAdmissionAdapter:
    """Bind the neutral Execution gate to one trusted Risk account scope.

    The composition root constructs this adapter with the account scope chosen
    by its trusted order-to-Risk mapper. The scope is never accepted from a
    group request. Execution calls the ordinary neutral
    ``claim_group_leg(intent, ...)`` surface; this adapter injects the captured
    Risk scope into the stricter account-owner Risk API and verifies that the
    group binding names the same canonical scope.

    Construct one instance per account-owned service boundary. This class does
    not import Risk or derive account identity from ``OrderIntent.scope``.
    """

    def __init__(self, admission_gate: Any, *, account_scope: Any) -> None:
        if admission_gate is None:
            raise ContractValidationError("account-scoped admission gate is required")
        if not callable(getattr(admission_gate, "claim_group_leg", None)):
            raise ContractValidationError("account Risk adapter lacks group claim")
        scope_key = getattr(account_scope, "key", None)
        if type(scope_key) is not str:
            raise ContractValidationError("trusted Risk AccountScope is required")
        scope_fingerprint = risk_account_scope_fingerprint(scope_key)
        self._admission_gate = admission_gate
        self._account_scope = account_scope
        self._scope_key = scope_key
        self._scope_fingerprint = scope_fingerprint

    @property
    def risk_scope_fingerprint(self) -> str:
        return self._scope_fingerprint

    def reserve(self, intent: Any) -> Any:
        return self._admission_gate.reserve(intent)

    def validate(self, permit_reference: str, intent: Any) -> Any:
        return self._admission_gate.validate(permit_reference, intent)

    def claim_for_dispatch(self, permit_reference: str, intent: Any) -> Any:
        return self._admission_gate.claim_for_dispatch(permit_reference, intent)

    def claim_group_leg(
        self,
        intent: Any,
        *,
        risk_scope_fingerprint: str,
        group_id: str,
        leg_id: str,
        permit_reference: str,
        group_fingerprint: str,
        deadline_ns: int,
    ) -> Any:
        current_key = getattr(self._account_scope, "key", None)
        if (
            type(current_key) is not str
            or current_key != self._scope_key
            or risk_scope_fingerprint != self._scope_fingerprint
        ):
            raise ContractValidationError("group binding does not match trusted Risk account")
        return self._admission_gate.claim_group_leg(
            intent,
            scope=self._account_scope,
            group_id=group_id,
            leg_id=leg_id,
            permit_reference=permit_reference,
            group_fingerprint=group_fingerprint,
            deadline_ns=deadline_ns,
        )

    def settle(self, permit_reference: str) -> Any:
        return self._admission_gate.settle(permit_reference)

    def release(self, permit_reference: str, reason: str) -> None:
        self._admission_gate.release(permit_reference, reason)

    def ensure_settled(self, permit_reference: str) -> Any:
        ensure_settled = getattr(self._admission_gate, "ensure_settled", None)
        if not callable(ensure_settled):
            raise ContractValidationError("account Risk adapter lacks recovery settlement")
        return ensure_settled(permit_reference)
