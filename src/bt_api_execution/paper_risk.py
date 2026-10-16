"""Trusted Risk admission for the managed spot-paper matching port.

This is an optional composition module: importing the base execution package
does not require ``bt_api_risk``.  A configured Paper owner may construct this
adapter with its already-selected durable Risk gate and exact matching port.
The mapper prices BUY/OPEN intents from the quote cached by
``PaperMatchingPort.prepare_risk_quote`` so Risk reservation and the eventual
Paper fill use the same source observation.
"""

from __future__ import annotations

from decimal import ROUND_HALF_EVEN
from typing import TYPE_CHECKING, Any

from .contracts import (
    OrderIntent,
    OrderType,
    PositionEffect,
    Side,
    TimeInForce,
    payload_sha256,
)
from .errors import ContractValidationError
from .paper_execution import (
    PAPER_ENVIRONMENT,
    PAPER_PROVIDER,
    PaperMatchingPort,
    PaperOwnerDomainV1,
)

if TYPE_CHECKING:
    from bt_api_risk.core.admission import AccountScope, RiskIntent
    from bt_api_risk.order_admission import RiskAdmissionPolicyBinding


class PaperRiskCompositionError(ValueError):
    """Finite error raised when a trusted Paper/Risk composition is invalid."""

    def __init__(self, code: str) -> None:
        if type(code) is not str or not code or not code.replace("_", "").isalnum():
            raise ValueError("invalid Paper Risk composition error code")
        self.code = code
        super().__init__(code)


class PaperRiskAdmissionAdapter:
    """Bind one exact Paper domain and matcher to a durable Risk gate.

    The Paper simulation account and strategy select the Risk account scope;
    no order field can choose or override that scope.  The current supported
    composition admits BUY/OPEN only.  A SELL/CLOSE requires the future
    cursor-bound PaperBook-to-Risk close rebase contract and is rejected here
    until that proof path is installed.
    """

    def __init__(
        self,
        risk_gate: object,
        owner_domain: PaperOwnerDomainV1,
        matching_port: PaperMatchingPort,
    ) -> None:
        try:
            from bt_api_risk import (
                AccountScope,
                DurableRiskAdmissionAdapter,
                DurableRiskGate,
                DurableRiskPolicyBinding,
                RiskAdmissionDeniedError,
                RiskDeniedError,
                StrategyAllocationSnapshot,
            )
            from bt_api_risk.core.exposure import ExposureAccountingMode
        except ImportError:
            raise PaperRiskCompositionError("RISK_PACKAGE_UNAVAILABLE") from None

        if type(risk_gate) is not DurableRiskGate:
            raise PaperRiskCompositionError("DURABLE_RISK_GATE_REQUIRED")
        if (
            type(owner_domain) is not PaperOwnerDomainV1
            or type(matching_port) is not PaperMatchingPort
            or matching_port.owner_domain is not owner_domain
        ):
            raise PaperRiskCompositionError("PAPER_OWNER_BINDING_INVALID")

        policy = risk_gate.policy
        if (
            policy.exposure_accounting_mode is not ExposureAccountingMode.RESERVATIONS_ONLY
            or policy.notional_unit != owner_domain.cash_currency
            or not policy.require_strategy_allocation
            or policy.max_increase_notional <= 0
            or policy.max_increase_count <= 0
        ):
            raise PaperRiskCompositionError("PAPER_RISK_POLICY_INCOMPATIBLE")

        scope = AccountScope(
            PAPER_PROVIDER,
            owner_domain.simulation_account_ref,
            PAPER_ENVIRONMENT,
        )
        try:
            binding = risk_gate.bind_policy(scope)
            if (
                type(binding) is not DurableRiskPolicyBinding
                or binding.scope != scope
                or binding.policy_id != policy.policy_id
                or binding.policy_fingerprint != policy.fingerprint
                or binding.ledger_identity_ref != risk_gate.ledger_identity_ref
            ):
                raise PaperRiskCompositionError("PAPER_RISK_POLICY_BINDING_MISMATCH")
            self._require_allocation(
                risk_gate.get_strategy_allocation(scope, owner_domain.strategy_ref),
                scope=scope,
                strategy_ref=owner_domain.strategy_ref,
                cash_currency=owner_domain.cash_currency,
                allocation_type=StrategyAllocationSnapshot,
            )
        except RiskDeniedError as error:
            raise PaperRiskCompositionError(error.code) from None
        except PaperRiskCompositionError:
            raise
        except Exception:
            raise PaperRiskCompositionError("PAPER_RISK_BINDING_UNAVAILABLE") from None

        self._scope = scope
        self._risk_gate = risk_gate
        self._owner_domain = owner_domain
        self._matching_port = matching_port
        self._risk_denied_error = RiskAdmissionDeniedError
        self._risk = DurableRiskAdmissionAdapter(risk_gate, self._map_intent)

    @staticmethod
    def _require_allocation(
        allocation: object,
        *,
        scope: AccountScope,
        strategy_ref: str,
        cash_currency: str,
        allocation_type: type,
    ) -> Any:
        if (
            type(allocation) is not allocation_type
            or allocation.scope != scope
            or allocation.strategy_id != strategy_ref
            or allocation.completeness != "LOCAL_LEDGER_COMPLETE"
            or allocation.notional_unit != cash_currency
            or allocation.available_notional is None
            or allocation.revision == ""
        ):
            raise PaperRiskCompositionError("PAPER_STRATEGY_ALLOCATION_UNAVAILABLE")
        return allocation

    @property
    def risk_scope(self) -> AccountScope:
        """Return the server-derived Risk scope bound to this Paper owner."""

        return self._scope

    @property
    def policy_binding(self) -> RiskAdmissionPolicyBinding:
        """Return the durable adapter's read-only policy projection."""

        return self._risk.policy_binding

    def is_bound_to(self, gate: object) -> bool:
        """Check that the account owner uses this exact Risk ledger."""

        return self._risk.is_bound_to(gate)

    def reserve(self, intent: OrderIntent) -> object:
        """Reserve Risk capacity before Paper's durable dispatch boundary."""

        try:
            return self._risk.reserve(intent)
        except self._risk_denied_error:
            if type(intent) is OrderIntent:
                self._matching_port.discard_risk_quote(intent.fingerprint)
            raise

    def validate(self, permit_reference: str, intent: OrderIntent) -> object:
        return self._risk.validate(permit_reference, intent)

    def claim_for_dispatch(self, permit_reference: str, intent: OrderIntent) -> object:
        return self._risk.claim_for_dispatch(permit_reference, intent)

    def settle(self, permit_reference: str) -> object:
        return self._risk.settle(permit_reference)

    def ensure_settled(self, permit_reference: str) -> object:
        return self._risk.ensure_settled(permit_reference)

    def release(self, permit_reference: str, reason: str) -> None:
        self._risk.release(permit_reference, reason)

    def _map_intent(self, intent: Any) -> RiskIntent:
        from bt_api_risk import IntentAction, RiskDeniedError, RiskIntent

        if type(intent) is not OrderIntent:
            raise RiskDeniedError("PAPER_RISK_ORDER_INVALID", "a typed Paper order is required")
        if (
            intent.scope.provider != PAPER_PROVIDER
            or intent.scope.environment != PAPER_ENVIRONMENT
            or intent.scope.account_ref != self._owner_domain.simulation_account_ref
            or intent.scope.strategy_id != self._owner_domain.strategy_ref
        ):
            raise RiskDeniedError(
                "PAPER_RISK_SCOPE_MISMATCH", "order does not match the trusted Paper owner"
            )
        if not (
            intent.side is Side.BUY
            and intent.position_effect is PositionEffect.OPEN
            and not intent.reduce_only
            and intent.position_side == "NET"
            and intent.position_bucket == "ALL"
        ):
            raise RiskDeniedError(
                "PAPER_RISK_CLOSE_REBASE_REQUIRED",
                "Paper close requires a cursor-bound Risk rebase",
            )
        if (
            intent.order_type is not OrderType.MARKET
            or intent.time_in_force is not TimeInForce.FOK
        ):
            raise RiskDeniedError(
                "PAPER_RISK_ORDER_UNSUPPORTED", "only Paper MARKET/FOK is supported"
            )

        spec = self._owner_domain.instrument(intent.instrument)
        if spec is None or intent.metadata_version != spec.metadata_version:
            raise RiskDeniedError(
                "PAPER_RISK_INSTRUMENT_UNSUPPORTED", "instrument metadata is not configured"
            )
        if intent.quantity % spec.quantity_step != 0:
            raise RiskDeniedError(
                "PAPER_RISK_QUANTITY_INVALID", "quantity does not match Paper instrument units"
            )

        try:
            quote = self._matching_port.prepare_risk_quote(intent)
        except ContractValidationError:
            raise RiskDeniedError(
                "PAPER_RISK_QUOTE_UNAVAILABLE", "a current trusted Paper quote is required"
            ) from None
        if intent.quantity > quote.ask_quantity:
            self._matching_port.discard_risk_quote(intent.fingerprint)
            raise RiskDeniedError(
                "PAPER_RISK_QUOTE_DEPTH_INSUFFICIENT",
                "the exact quote cannot fill the complete Paper order",
            )

        allocation = self._risk_gate.get_strategy_allocation(
            self._scope, self._owner_domain.strategy_ref
        )
        from bt_api_risk import StrategyAllocationSnapshot

        try:
            allocation = self._require_allocation(
                allocation,
                scope=self._scope,
                strategy_ref=self._owner_domain.strategy_ref,
                cash_currency=self._owner_domain.cash_currency,
                allocation_type=StrategyAllocationSnapshot,
            )
        except PaperRiskCompositionError:
            raise RiskDeniedError(
                "PAPER_STRATEGY_ALLOCATION_UNAVAILABLE",
                "the exact Paper strategy allocation is unavailable",
            ) from None
        gross = intent.quantity * quote.ask
        fee = (gross * self._owner_domain.fee_rate).quantize(
            self._owner_domain.fee_quantum,
            rounding=ROUND_HALF_EVEN,
        )
        risk_payload = payload_sha256(
            {
                "allocation_revision": allocation.revision,
                "domain_fingerprint": self._owner_domain.fingerprint,
                "instrument": spec.to_payload(),
                "intent": intent.to_payload(),
                "intent_fingerprint": intent.fingerprint,
                "paper_quote": quote.to_payload(),
                "paper_risk_schema": "paper-risk-quote.v1",
                "risk_notional": format(gross + fee, "f"),
                "simulation_account_ref": self._owner_domain.simulation_account_ref,
                "strategy_ref": self._owner_domain.strategy_ref,
            }
        )
        return RiskIntent(
            intent_id=intent.intent_id,
            scope=self._scope,
            action=IntentAction.INCREASE,
            notional=gross + fee,
            payload_fingerprint=risk_payload,
            strategy_id=self._owner_domain.strategy_ref,
            allocation_version=allocation.revision,
            instrument=intent.instrument,
            quantity=intent.quantity,
            quantity_unit=spec.base_currency,
            notional_unit=self._owner_domain.cash_currency,
            position_side="NET",
            position_bucket="ALL",
        )


__all__ = ("PaperRiskAdmissionAdapter", "PaperRiskCompositionError")
