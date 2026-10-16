"""Trusted Risk admission for the managed spot-paper matching port.

This is an optional composition module: importing the base execution package
does not require ``bt_api_risk``.  A configured Paper owner may construct this
adapter with its already-selected durable Risk gate and exact matching port.
The mapper prices BUY/OPEN intents from the quote cached by
``PaperMatchingPort.prepare_risk_quote`` so Risk reservation and the eventual
Paper fill use the same source observation.
"""

from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Decimal
from math import isfinite
from threading import RLock, get_ident
from time import time_ns
from typing import TYPE_CHECKING, Any

from .contracts import (
    ExecutionScope,
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
    PaperQuoteV1,
)
from .paper_risk_snapshot import (
    PaperRiskAdmissionSnapshotStore,
    PaperRiskAdmissionSnapshotV1,
    PaperRiskSnapshotStoreError,
)

if TYPE_CHECKING:
    from bt_api_risk.core.admission import AccountScope
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
    no order field can choose or override that scope. BUY/OPEN is reserved as
    an increase. SELL/CLOSE is enabled only when the exact trusted E-backed
    close authority is installed and keeps the Risk position cut in sync.
    """

    def __init__(
        self,
        risk_gate: object,
        owner_domain: PaperOwnerDomainV1,
        matching_port: PaperMatchingPort,
        *,
        close_authority: object | None = None,
        execution_store: object | None = None,
        execution_scope: ExecutionScope | None = None,
    ) -> None:
        try:
            from bt_api_risk import (
                AccountScope,
                DurableRiskAdmissionAdapter,
                DurableRiskGate,
                DurableRiskPolicyBinding,
                RiskAdmissionAdapterError,
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
        snapshot_store = None
        if execution_store is not None or execution_scope is not None:
            from .store import SqliteExecutionStore

            if (
                type(execution_store) is not SqliteExecutionStore
                or type(execution_scope) is not ExecutionScope
                or execution_scope.provider != PAPER_PROVIDER
                or execution_scope.environment != PAPER_ENVIRONMENT
                or execution_scope.account_ref != owner_domain.simulation_account_ref
                or execution_scope.strategy_id != owner_domain.strategy_ref
            ):
                raise PaperRiskCompositionError("PAPER_EXECUTION_SNAPSHOT_BINDING_INVALID")
            try:
                snapshot_store = PaperRiskAdmissionSnapshotStore(
                    execution_store, execution_scope
                )
            except (ContractValidationError, ValueError):
                raise PaperRiskCompositionError(
                    "PAPER_EXECUTION_SNAPSHOT_STORE_INVALID"
                ) from None
        elif close_authority is not None:
            raise PaperRiskCompositionError("PAPER_EXECUTION_SNAPSHOT_STORE_REQUIRED")

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
        if close_authority is not None:
            from .paper_execution_risk import PaperExecutionCloseAuthority

            if (
                type(close_authority) is not PaperExecutionCloseAuthority
                or snapshot_store is None
                or close_authority.store is not execution_store
                or close_authority.execution_scope is not execution_scope
                or not close_authority.is_bound_to_composition(
                    risk_gate,
                    store=close_authority.store,
                    execution_scope=close_authority.execution_scope,
                    owner_domain=owner_domain,
                    matching_port=matching_port,
                    risk_scope=scope,
                    instrument_registry=close_authority.instrument_registry,
                )
            ):
                raise PaperRiskCompositionError("PAPER_CLOSE_AUTHORITY_MISMATCH")
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
        self._close_authority = close_authority
        self._execution_store = execution_store
        self._execution_scope = execution_scope
        self._snapshot_store = snapshot_store
        self._managed_mapping_lock = RLock()
        self._managed_mapping_context: tuple[int, OrderIntent, object] | None = None
        self._risk_denied_error = RiskAdmissionDeniedError
        self._risk_adapter_error = RiskAdmissionAdapterError
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

    @property
    def risk_gate(self) -> object:
        """Return the exact trusted gate used by this Paper admission adapter."""

        return self._risk_gate

    def is_bound_to_composition(
        self,
        gate: object,
        owner_domain: PaperOwnerDomainV1,
        matching_port: PaperMatchingPort,
        *,
        store: object | None = None,
        execution_scope: object | None = None,
        instrument_registry: object | None = None,
    ) -> bool:
        """Check exact local identity across the selected Paper/Risk composition."""

        if (
            not self.is_bound_to(gate)
            or self._owner_domain is not owner_domain
            or self._matching_port is not matching_port
            or self._snapshot_store is None
            or (store is not None and not self._snapshot_store.is_bound_to(store, self._execution_scope))
            or (
                execution_scope is not None
                and not self._snapshot_store.is_bound_to(self._execution_store, execution_scope)
            )
        ):
            return False
        if self._close_authority is None:
            return True
        return self._close_authority.is_bound_to_composition(
            gate,
            store=self._close_authority.store if store is None else store,
            execution_scope=(
                self._close_authority.execution_scope
                if execution_scope is None
                else execution_scope
            ),
            owner_domain=owner_domain,
            matching_port=matching_port,
            risk_scope=self._scope,
            instrument_registry=(
                self._close_authority.instrument_registry
                if instrument_registry is None
                else instrument_registry
            ),
        )

    def recover_paper_scope(self, writer_lease: object) -> None:
        """Reconcile durable E fills and the full-book Risk cut before ready."""

        if self._close_authority is None:
            return
        self._close_authority.recover_scope(self._risk_gate, writer_lease)

    def after_evidenced_outcome(self, record: object, *, writer_lease: object) -> None:
        """Resolve a Paper fill's Risk dispatch claim from committed E facts."""

        if self._close_authority is None:
            return
        self._close_authority.resolve_evidenced_outcome(
            self._risk_gate, record, writer_lease
        )

    def reserve_for_execution(self, intent: OrderIntent, *, writer_lease: object) -> object:
        """Reserve from an immutable, owner-fenced mapping snapshot."""

        if self._snapshot_store is None:
            raise self._risk_denied_error("PAPER_EXECUTION_SNAPSHOT_STORE_REQUIRED")
        risk_reserve_started = False

        def reserve_risk_mapping() -> object:
            nonlocal risk_reserve_started
            risk_reserve_started = True
            return self._risk.reserve(intent)

        try:
            snapshot = self._snapshot_store.load_for_reservation(intent, writer_lease)
            if snapshot is None:
                mapped, quote, allocation = self._build_risk_mapping(intent)
                snapshot = self._snapshot_store.create_for_reservation(
                    intent,
                    writer_lease,
                    self._snapshot_body(intent, mapped, quote, allocation),
                )
            mapped, quote = self._restore_snapshot(intent, snapshot)
            self._matching_port.restore_risk_quote(intent, quote)
            return self._with_managed_mapping(
                intent, mapped, reserve_risk_mapping
            )
        except PaperRiskSnapshotStoreError as error:
            self._discard_risk_quote(intent)
            raise self._risk_denied_error(error.code) from None
        except ContractValidationError as error:
            if risk_reserve_started:
                self._project_risk_denial(error)
                raise
            self._discard_risk_quote(intent)
            raise self._risk_denied_error("PAPER_ADMISSION_SNAPSHOT_INVALID") from None
        except (ArithmeticError, KeyError, TypeError, ValueError) as error:
            if type(error) is self._risk_adapter_error:
                self._discard_risk_quote(intent)
                raise self._risk_denied_error(
                    "PAPER_ADMISSION_MAPPING_INVALID"
                ) from None
            if risk_reserve_started:
                self._project_risk_denial(error)
                raise
            self._discard_risk_quote(intent)
            raise self._risk_denied_error("PAPER_ADMISSION_SNAPSHOT_INVALID") from None
        except Exception as error:
            self._discard_if_definitive_risk_denial(intent, error)
            self._project_risk_denial(error)
            raise

    def claim_for_dispatch_for_execution(
        self,
        permit_reference: str,
        intent: OrderIntent,
        *,
        writer_lease: object,
    ) -> object:
        """Claim the exact persisted mapping and quote for one dispatch attempt."""

        if self._snapshot_store is None:
            raise self._risk_denied_error("PAPER_EXECUTION_SNAPSHOT_STORE_REQUIRED")
        risk_claim_started = False

        def claim_risk_mapping() -> object:
            nonlocal risk_claim_started
            risk_claim_started = True
            return self._risk.claim_for_dispatch(permit_reference, intent)

        try:
            snapshot = self._snapshot_store.load_for_claim(
                intent, writer_lease, permit_reference
            )
            mapped, quote = self._restore_snapshot(intent, snapshot)
            self._matching_port.restore_risk_quote(intent, quote)
            return self._with_managed_mapping(
                intent,
                mapped,
                claim_risk_mapping,
            )
        except PaperRiskSnapshotStoreError as error:
            self._discard_risk_quote(intent)
            raise self._risk_denied_error(error.code) from None
        except ContractValidationError as error:
            if risk_claim_started:
                self._project_risk_denial(error)
                raise
            self._discard_risk_quote(intent)
            raise self._risk_denied_error("PAPER_ADMISSION_SNAPSHOT_INVALID") from None
        except (ArithmeticError, KeyError, TypeError, ValueError) as error:
            if type(error) is self._risk_adapter_error:
                self._discard_risk_quote(intent)
                raise self._risk_denied_error(
                    "PAPER_ADMISSION_MAPPING_INVALID"
                ) from None
            if risk_claim_started:
                self._project_risk_denial(error)
                raise
            self._discard_risk_quote(intent)
            raise self._risk_denied_error("PAPER_ADMISSION_SNAPSHOT_INVALID") from None
        except Exception as error:
            self._discard_if_definitive_risk_denial(intent, error)
            self._project_risk_denial(error)
            raise

    def _discard_risk_quote(self, intent: object) -> None:
        if type(intent) is OrderIntent:
            self._matching_port.discard_risk_quote(intent.fingerprint)

    def _discard_if_definitive_risk_denial(self, intent: object, error: Exception) -> None:
        from bt_api_risk import PermitInvalidError, RiskDeniedError

        if type(error) in (self._risk_denied_error, PermitInvalidError, RiskDeniedError):
            self._discard_risk_quote(intent)

    def _with_managed_mapping(self, intent: OrderIntent, mapped: object, call: Any) -> Any:
        with self._managed_mapping_lock:
            if self._managed_mapping_context is not None:
                raise self._risk_denied_error("PAPER_ADMISSION_MAPPING_REENTRY")
            self._managed_mapping_context = (get_ident(), intent, mapped)
            try:
                return call()
            finally:
                self._managed_mapping_context = None

    def _project_risk_denial(self, error: Exception) -> None:
        from bt_api_risk import PermitInvalidError, RiskDeniedError

        if type(error) in (PermitInvalidError, RiskDeniedError):
            code = getattr(error, "code", None)
            if type(code) is str and code and code.replace("_", "").isalnum():
                raise self._risk_denied_error(code) from None

    def _snapshot_body(
        self,
        intent: OrderIntent,
        mapped: object,
        quote: PaperQuoteV1,
        allocation: object,
    ) -> dict[str, Any]:
        from bt_api_risk import RiskIntent, StrategyAllocationSnapshot

        if (
            type(mapped) is not RiskIntent
            or type(quote) is not PaperQuoteV1
            or type(allocation) is not StrategyAllocationSnapshot
        ):
            raise self._risk_denied_error("PAPER_ADMISSION_MAPPING_INVALID")
        policy = self.policy_binding
        return {
            "schema": "paper-risk-admission.snapshot.v1",
            "intent_id": intent.intent_id,
            "execution_intent_fingerprint": intent.fingerprint,
            "execution_scope": self._execution_scope.to_payload(),
            "execution_scope_key": self._execution_scope.key,
            "execution_journal_generation": self._execution_store.journal_source_identity()[
                "generation"
            ],
            "risk_scope": {
                "provider": self._scope.provider,
                "account_id": self._scope.account_id,
                "environment": self._scope.environment,
                "key": self._scope.key,
            },
            "owner_domain_ref": self._owner_domain.execution_domain_ref,
            "owner_domain_fingerprint": self._owner_domain.fingerprint,
            "risk_policy": {
                "policy_id": policy.policy_id,
                "policy_fingerprint": policy.policy_fingerprint,
                "exposure_accounting_mode": policy.exposure_accounting_mode.value,
                "ledger_identity_ref": self._risk_gate.ledger_identity_ref,
            },
            "allocation_snapshot": self._allocation_snapshot_payload(allocation),
            "allocation_revision": mapped.allocation_version,
            "paper_quote": quote.to_payload(),
            "quote_expires_at_ns": quote.as_of_ns + self._owner_domain.max_quote_age_ns,
            "risk_intent": self._risk_intent_payload(mapped),
            "risk_intent_fingerprint": mapped.fingerprint,
        }

    def _allocation_snapshot_payload(self, allocation: object) -> dict[str, Any]:
        from bt_api_risk import StrategyAllocationSnapshot

        if (
            type(allocation) is not StrategyAllocationSnapshot
            or allocation.scope != self._scope
            or allocation.strategy_id != self._owner_domain.strategy_ref
            or allocation.notional_unit != self._owner_domain.cash_currency
            or allocation.completeness != "LOCAL_LEDGER_COMPLETE"
            or allocation.available_notional is None
        ):
            raise self._risk_denied_error("PAPER_STRATEGY_ALLOCATION_UNAVAILABLE")

        def decimal_text(value: object) -> str | None:
            if value is None:
                return None
            if type(value) is not Decimal or not value.is_finite():
                raise self._risk_denied_error("PAPER_STRATEGY_ALLOCATION_UNAVAILABLE")
            return format(value, "f")

        return {
            "scope": {
                "provider": allocation.scope.provider,
                "account_id": allocation.scope.account_id,
                "environment": allocation.scope.environment,
                "key": allocation.scope.key,
            },
            "strategy_id": allocation.strategy_id,
            "revision": allocation.revision,
            "allocated_notional": decimal_text(allocation.allocated_notional),
            "used_notional": decimal_text(allocation.used_notional),
            "reserved_notional": decimal_text(allocation.reserved_notional),
            "available_notional": decimal_text(allocation.available_notional),
            "notional_unit": allocation.notional_unit,
            "max_position": decimal_text(allocation.max_position),
            "position_instrument": allocation.position_instrument,
            "position_unit": allocation.position_unit,
            "as_of": allocation.as_of,
            "max_gross_occupancy_notional": decimal_text(
                allocation.max_gross_occupancy_notional
            ),
            "max_gross_occupancy_intent_count": allocation.max_gross_occupancy_intent_count,
            "source": allocation.source,
            "completeness": allocation.completeness,
            "reason": allocation.reason,
        }

    @staticmethod
    def _risk_intent_payload(mapped: object) -> dict[str, Any]:
        return {
            "intent_id": mapped.intent_id,
            "scope": {
                "provider": mapped.scope.provider,
                "account_id": mapped.scope.account_id,
                "environment": mapped.scope.environment,
            },
            "action": mapped.action.value,
            "notional": format(mapped.notional, "f"),
            "payload_fingerprint": mapped.payload_fingerprint,
            "strategy_id": mapped.strategy_id,
            "allocation_version": mapped.allocation_version,
            "instrument": mapped.instrument,
            "quantity": None if mapped.quantity is None else format(mapped.quantity, "f"),
            "quantity_unit": mapped.quantity_unit,
            "notional_unit": mapped.notional_unit,
            "exposure_profile_fingerprint": mapped.exposure_profile_fingerprint,
            "instrument_facts_observed_at_ns": mapped.instrument_facts_observed_at_ns,
            "instrument_facts_valid_until_ns": mapped.instrument_facts_valid_until_ns,
            "resource": None,
            "exposure_dimensions": [],
            "position_side": mapped.position_side,
            "position_bucket": mapped.position_bucket,
        }

    def _restore_snapshot(
        self,
        intent: OrderIntent,
        snapshot: PaperRiskAdmissionSnapshotV1,
    ) -> tuple[object, PaperQuoteV1]:
        from bt_api_risk import AccountScope, IntentAction, RiskIntent

        body = dict(snapshot.body)
        # The adapter is the only normal producer of these objects, but
        # re-check the content digest at the point of use as the returned
        # mapping is intentionally a public Mapping rather than a deep-frozen
        # JSON tree.
        PaperRiskAdmissionSnapshotV1(body, snapshot.snapshot_sha256)
        expected_keys = {
            "schema",
            "intent_id",
            "execution_intent_fingerprint",
            "execution_scope",
            "execution_scope_key",
            "execution_journal_generation",
            "risk_scope",
            "owner_domain_ref",
            "owner_domain_fingerprint",
            "risk_policy",
            "allocation_snapshot",
            "allocation_revision",
            "paper_quote",
            "quote_expires_at_ns",
            "risk_intent",
            "risk_intent_fingerprint",
        }
        expected_policy = {
            "policy_id": self.policy_binding.policy_id,
            "policy_fingerprint": self.policy_binding.policy_fingerprint,
            "exposure_accounting_mode": self.policy_binding.exposure_accounting_mode.value,
            "ledger_identity_ref": self._risk_gate.ledger_identity_ref,
        }
        expected_risk_scope = {
            "provider": self._scope.provider,
            "account_id": self._scope.account_id,
            "environment": self._scope.environment,
            "key": self._scope.key,
        }
        allocation = body.get("allocation_snapshot")
        allocation_keys = {
            "scope",
            "strategy_id",
            "revision",
            "allocated_notional",
            "used_notional",
            "reserved_notional",
            "available_notional",
            "notional_unit",
            "max_position",
            "position_instrument",
            "position_unit",
            "as_of",
            "max_gross_occupancy_notional",
            "max_gross_occupancy_intent_count",
            "source",
            "completeness",
            "reason",
        }
        if (
            type(allocation) is not dict
            or set(allocation) != allocation_keys
            or allocation.get("scope") != expected_risk_scope
            or allocation.get("strategy_id") != self._owner_domain.strategy_ref
            or allocation.get("revision") != body.get("allocation_revision")
            or allocation.get("notional_unit") != self._owner_domain.cash_currency
            or allocation.get("available_notional") is None
            or allocation.get("completeness") != "LOCAL_LEDGER_COMPLETE"
            or allocation.get("source") != "local_risk_reservation_ledger"
            or type(allocation.get("as_of")) not in {int, float}
            or not isfinite(allocation["as_of"])
        ):
            raise ContractValidationError("invalid persisted strategy allocation snapshot")
        for name in (
            "allocated_notional",
            "used_notional",
            "reserved_notional",
            "available_notional",
            "max_position",
            "max_gross_occupancy_notional",
        ):
            amount = allocation[name]
            if amount is not None:
                if type(amount) is not str:
                    raise ContractValidationError("invalid persisted allocation amount")
                parsed = Decimal(amount)
                if not parsed.is_finite() or format(parsed, "f") != amount:
                    raise ContractValidationError("invalid persisted allocation amount")
        if allocation["max_gross_occupancy_intent_count"] is not None and type(
            allocation["max_gross_occupancy_intent_count"]
        ) is not int:
            raise ContractValidationError("invalid persisted allocation count")
        if allocation["position_instrument"] is not None and type(
            allocation["position_instrument"]
        ) is not str:
            raise ContractValidationError("invalid persisted allocation instrument")
        if allocation["position_unit"] is not None and type(allocation["position_unit"]) is not str:
            raise ContractValidationError("invalid persisted allocation unit")
        if allocation["reason"] is not None and type(allocation["reason"]) is not str:
            raise ContractValidationError("invalid persisted allocation reason")
        risk_payload = body.get("risk_intent")
        if type(risk_payload) is not dict:
            raise ContractValidationError("invalid persisted Risk intent mapping")
        if (
            set(body) != expected_keys
            or body.get("schema") != "paper-risk-admission.snapshot.v1"
            or body.get("intent_id") != intent.intent_id
            or body.get("execution_intent_fingerprint") != intent.fingerprint
            or body.get("execution_scope") != self._execution_scope.to_payload()
            or body.get("execution_scope_key") != self._execution_scope.key
            or body.get("execution_journal_generation")
            != self._execution_store.journal_source_identity()["generation"]
            or body.get("risk_scope") != expected_risk_scope
            or body.get("owner_domain_ref") != self._owner_domain.execution_domain_ref
            or body.get("owner_domain_fingerprint") != self._owner_domain.fingerprint
            or body.get("risk_policy") != expected_policy
            or body.get("allocation_revision") != risk_payload.get("allocation_version")
            or type(body.get("quote_expires_at_ns")) is not int
        ):
            raise ContractValidationError("Paper Risk snapshot no longer matches its owner")
        quote_payload = body["paper_quote"]
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
            raise ContractValidationError("invalid persisted Paper quote")
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
        if (
            body["quote_expires_at_ns"] != quote.as_of_ns + self._owner_domain.max_quote_age_ns
            or time_ns() >= body["quote_expires_at_ns"]
        ):
            raise PaperRiskSnapshotStoreError("PAPER_ADMISSION_SNAPSHOT_EXPIRED")
        risk_keys = {
            "intent_id",
            "scope",
            "action",
            "notional",
            "payload_fingerprint",
            "strategy_id",
            "allocation_version",
            "instrument",
            "quantity",
            "quantity_unit",
            "notional_unit",
            "exposure_profile_fingerprint",
            "instrument_facts_observed_at_ns",
            "instrument_facts_valid_until_ns",
            "resource",
            "exposure_dimensions",
            "position_side",
            "position_bucket",
        }
        scope_payload = risk_payload.get("scope")
        if (
            set(risk_payload) != risk_keys
            or type(scope_payload) is not dict
            or scope_payload
            != {
                "provider": self._scope.provider,
                "account_id": self._scope.account_id,
                "environment": self._scope.environment,
            }
            or risk_payload.get("resource") is not None
            or risk_payload.get("exposure_dimensions") != []
        ):
            raise ContractValidationError("invalid persisted Risk intent mapping")
        mapped = RiskIntent(
            intent_id=risk_payload["intent_id"],
            scope=AccountScope(
                scope_payload["provider"],
                scope_payload["account_id"],
                scope_payload["environment"],
            ),
            action=IntentAction(risk_payload["action"]),
            notional=Decimal(risk_payload["notional"]),
            payload_fingerprint=risk_payload["payload_fingerprint"],
            strategy_id=risk_payload["strategy_id"],
            allocation_version=risk_payload["allocation_version"],
            instrument=risk_payload["instrument"],
            quantity=(
                None
                if risk_payload["quantity"] is None
                else Decimal(risk_payload["quantity"])
            ),
            quantity_unit=risk_payload["quantity_unit"],
            notional_unit=risk_payload["notional_unit"],
            exposure_profile_fingerprint=risk_payload["exposure_profile_fingerprint"],
            instrument_facts_observed_at_ns=risk_payload[
                "instrument_facts_observed_at_ns"
            ],
            instrument_facts_valid_until_ns=risk_payload[
                "instrument_facts_valid_until_ns"
            ],
            resource=None,
            exposure_dimensions=(),
            position_side=risk_payload["position_side"],
            position_bucket=risk_payload["position_bucket"],
        )
        spec = self._owner_domain.instrument(intent.instrument)
        expected_action = "increase" if intent.side is Side.BUY else "reduce"
        if (
            type(mapped) is not RiskIntent
            or mapped.fingerprint != body.get("risk_intent_fingerprint")
            or mapped.intent_id != intent.intent_id
            or mapped.scope != self._scope
            or mapped.strategy_id != self._owner_domain.strategy_ref
            or mapped.allocation_version != body.get("allocation_revision")
            or mapped.instrument != intent.instrument
            or mapped.quantity != intent.quantity
            or spec is None
            or mapped.quantity_unit != spec.base_currency
            or mapped.notional_unit != self._owner_domain.cash_currency
            or mapped.position_side != "NET"
            or mapped.position_bucket != "ALL"
            or mapped.action.value != expected_action
            or quote.instrument != intent.instrument
            or quote.source_ref != self._owner_domain.quote_source_ref
        ):
            raise ContractValidationError("persisted Paper Risk mapping is not exact")
        return mapped, quote

    def _map_intent(self, intent: Any) -> Any:
        from bt_api_risk import RiskDeniedError

        context = self._managed_mapping_context
        if context is not None and context[0] == get_ident():
            if (
                context[1] is not intent
                or context[1].fingerprint != getattr(intent, "fingerprint", None)
            ):
                raise RiskDeniedError(
                    "PAPER_ADMISSION_MAPPING_CONFLICT",
                    "managed Risk mapping does not match the dispatched order",
                )
            return context[2]
        if self._snapshot_store is not None:
            raise RiskDeniedError(
                "PAPER_EXECUTION_CONTEXT_REQUIRED",
                "managed Paper Risk calls require the held Execution writer lease",
            )
        return self._build_risk_mapping(intent)[0]

    def reserve(self, intent: OrderIntent) -> object:
        """Reserve Risk capacity before Paper's durable dispatch boundary."""

        if self._snapshot_store is not None:
            raise self._risk_denied_error("PAPER_EXECUTION_CONTEXT_REQUIRED")
        try:
            return self._risk.reserve(intent)
        except self._risk_denied_error:
            if type(intent) is OrderIntent:
                self._matching_port.discard_risk_quote(intent.fingerprint)
            raise

    def validate(self, permit_reference: str, intent: OrderIntent) -> object:
        if self._snapshot_store is not None:
            raise self._risk_denied_error("PAPER_EXECUTION_CONTEXT_REQUIRED")
        return self._risk.validate(permit_reference, intent)

    def claim_for_dispatch(self, permit_reference: str, intent: OrderIntent) -> object:
        if self._snapshot_store is not None:
            raise self._risk_denied_error("PAPER_EXECUTION_CONTEXT_REQUIRED")
        return self._risk.claim_for_dispatch(permit_reference, intent)

    def settle(self, permit_reference: str) -> object:
        return self._risk.settle(permit_reference)

    def ensure_settled(self, permit_reference: str) -> object:
        return self._risk.ensure_settled(permit_reference)

    def release(self, permit_reference: str, reason: str) -> None:
        self._risk.release(permit_reference, reason)

    def _build_risk_mapping(self, intent: Any) -> tuple[Any, PaperQuoteV1, Any]:
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
        is_open = (
            intent.side is Side.BUY
            and intent.position_effect is PositionEffect.OPEN
            and not intent.reduce_only
        )
        is_close = (
            intent.side is Side.SELL
            and intent.position_effect is PositionEffect.CLOSE
            and intent.reduce_only
        )
        if not (is_open or is_close) or intent.position_side != "NET" or intent.position_bucket != "ALL":
            raise RiskDeniedError(
                "PAPER_RISK_ORDER_UNSUPPORTED",
                "Paper supports only BUY/OPEN and bound SELL/CLOSE NET/ALL orders",
            )
        if is_close and self._close_authority is None:
            raise RiskDeniedError(
                "PAPER_RISK_CLOSE_REBASE_REQUIRED",
                "Paper close requires the trusted E-backed cursor rebase authority",
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

        if is_close:
            try:
                closable = self._risk_gate.get_closable_position(
                    self._scope,
                    intent.instrument,
                    self._owner_domain.strategy_ref,
                    position_side="NET",
                    position_bucket="ALL",
                )
            except RiskDeniedError:
                raise RiskDeniedError(
                    "PAPER_CLOSE_POSITION_UNAVAILABLE",
                    "the exact PaperBook closeable position is not initialized",
                ) from None
            if (
                closable.completeness != "LOCAL_LEDGER_COMPLETE"
                or closable.quantity_unit != spec.base_currency
                or closable.available_quantity is None
                or closable.available_quantity < intent.quantity
            ):
                raise RiskDeniedError(
                    "PAPER_CLOSE_QUANTITY_UNAVAILABLE",
                    "the current PaperBook cut does not prove enough closeable quantity",
                )

        try:
            quote = self._matching_port.prepare_risk_quote(intent)
        except ContractValidationError:
            raise RiskDeniedError(
                "PAPER_RISK_QUOTE_UNAVAILABLE", "a current trusted Paper quote is required"
            ) from None
        quote_price = quote.ask if is_open else quote.bid
        quote_depth = quote.ask_quantity if is_open else quote.bid_quantity
        if intent.quantity > quote_depth:
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
        gross = intent.quantity * quote_price
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
                "risk_action": "increase" if is_open else "reduce",
                "risk_notional": format(gross + fee if is_open else Decimal("0"), "f"),
                "simulation_account_ref": self._owner_domain.simulation_account_ref,
                "strategy_ref": self._owner_domain.strategy_ref,
            }
        )
        mapped = RiskIntent(
            intent_id=intent.intent_id,
            scope=self._scope,
            action=IntentAction.INCREASE if is_open else IntentAction.REDUCE,
            notional=gross + fee if is_open else Decimal("0"),
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
        return mapped, quote, allocation


__all__ = ("PaperRiskAdmissionAdapter", "PaperRiskCompositionError")
