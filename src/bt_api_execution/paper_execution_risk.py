"""E-backed terminal resolution and closable-position rebases for Paper.

This module is deliberately separate from the optional Risk package's base
Execution APIs.  A trusted service composition constructs one exact authority
from its SQLite Paper owner, immutable domain, matcher, Risk scope, instrument
registry and existing RiskOrderJournal fallback.  It is the only source
authority that the Paper Risk adapter will accept.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterator, Mapping

from .contracts import (
    ExecutionEvent,
    ExecutionScope,
    OrderIntent,
    PositionEffect,
    Side,
    payload_sha256,
)
from .errors import ContractValidationError, DurableStoreError
from .paper_execution import (
    PAPER_ENVIRONMENT,
    PAPER_PROVIDER,
    PaperBookSnapshotV1,
    PaperMatchReceiptV2,
    PaperMatchingPort,
    PaperOwnerDomainV1,
)
from .store import ExecutionRecord, ExecutionState, SqliteExecutionStore, WriterLease


@dataclass(frozen=True, slots=True)
class _PaperTerminalSource:
    record: ExecutionRecord
    intent: OrderIntent
    event: ExecutionEvent
    receipt: PaperMatchReceiptV2
    source_sha256: str


class PaperExecutionCloseAuthority:
    """Canonical E source authority for one exact Paper/Risk composition.

    The authority is placed in ``DurableRiskGate.gross_exposure_authority``
    while the same existing RiskOrderJournal remains installed as the normal
    execution-journal authority.  Paper scopes are verified from immutable E
    records/events/V2 fills; every non-Paper scope delegates to that exact
    journal object.  A client cannot construct the Risk proof accepted here.
    """

    def __init__(
        self,
        store: SqliteExecutionStore,
        execution_scope: ExecutionScope,
        owner_domain: PaperOwnerDomainV1,
        matching_port: PaperMatchingPort,
        risk_scope: object,
        instrument_registry: object,
        fallback_authority: object,
    ) -> None:
        try:
            from bt_api_risk import AccountScope
            from bt_api_risk.core.instrument import InstrumentRiskRegistry
        except ImportError:
            raise ContractValidationError("paper close authority requires bt_api_risk") from None
        if (
            type(store) is not SqliteExecutionStore
            or type(execution_scope) is not ExecutionScope
            or type(owner_domain) is not PaperOwnerDomainV1
            or type(matching_port) is not PaperMatchingPort
            or matching_port.owner_domain is not owner_domain
            or type(risk_scope) is not AccountScope
            or type(instrument_registry) is not InstrumentRiskRegistry
            or not callable(
                getattr(fallback_authority, "dispatch_exposure_resolution_guard", None)
            )
            or execution_scope.provider != PAPER_PROVIDER
            or execution_scope.environment != PAPER_ENVIRONMENT
            or execution_scope.account_ref != owner_domain.simulation_account_ref
            or execution_scope.strategy_id != owner_domain.strategy_ref
            or risk_scope.provider != PAPER_PROVIDER
            or risk_scope.environment != PAPER_ENVIRONMENT
            or risk_scope.account_id != owner_domain.simulation_account_ref
        ):
            raise ContractValidationError("paper close authority binding is invalid")
        owner_domain.validate_binding(
            __import__("bt_api_base.service_protocol", fromlist=["WireScope"]).WireScope(
                owner_domain.execution_domain_ref,
                account_ref=owner_domain.simulation_account_ref,
                strategy_ref=owner_domain.strategy_ref,
            ),
            execution_scope,
        )
        self._store = store
        self._execution_scope = execution_scope
        self._owner_domain = owner_domain
        self._matching_port = matching_port
        self._risk_scope = risk_scope
        self._instrument_registry = instrument_registry
        self._fallback_authority = fallback_authority
        self._lock = threading.RLock()
        self._lease_context = threading.local()
        self._resolution_context = threading.local()

    @property
    def fallback_authority(self) -> object:
        """The exact pre-existing RiskOrderJournal fallback object."""

        return self._fallback_authority

    @property
    def risk_scope(self) -> object:
        return self._risk_scope

    @property
    def execution_scope(self) -> ExecutionScope:
        return self._execution_scope

    @property
    def store(self) -> SqliteExecutionStore:
        return self._store

    @property
    def owner_domain(self) -> PaperOwnerDomainV1:
        return self._owner_domain

    @property
    def matching_port(self) -> PaperMatchingPort:
        return self._matching_port

    @property
    def instrument_registry(self) -> object:
        return self._instrument_registry

    def is_bound_to_composition(
        self,
        gate: object,
        *,
        store: object,
        execution_scope: object,
        owner_domain: object,
        matching_port: object,
        risk_scope: object,
        instrument_registry: object,
    ) -> bool:
        """Require exact local identity for every server-owned composition edge."""

        try:
            from bt_api_risk import DurableRiskGate
            from bt_api_risk.core.exposure import ExposureAccountingMode
        except ImportError:
            return False
        return bool(
            type(gate) is DurableRiskGate
            and gate.policy.exposure_accounting_mode is ExposureAccountingMode.RESERVATIONS_ONLY
            and gate.policy.notional_unit == self._owner_domain.cash_currency
            and getattr(gate, "_gross_exposure_authority", None) is self
            and getattr(gate, "_execution_journal_authority", None)
            is self._fallback_authority
            and store is self._store
            and execution_scope is self._execution_scope
            and owner_domain is self._owner_domain
            and matching_port is self._matching_port
            and risk_scope == self._risk_scope
            and instrument_registry is self._instrument_registry
        )

    @contextmanager
    def _writer_lease(self, writer_lease: WriterLease) -> Iterator[None]:
        if (
            type(writer_lease) is not WriterLease
            or writer_lease.scope_key != self._execution_scope.account_key
        ):
            raise DurableStoreError("paper risk resolution has no exact execution writer lease")
        with self._lock:
            if getattr(self._lease_context, "lease", None) is not None:
                raise DurableStoreError("nested paper execution writer lease context")
            self._store.assert_writer_lease(self._execution_scope, writer_lease)
            self._lease_context.lease = writer_lease
            self._lease_context.thread_id = threading.get_ident()
            try:
                yield
            finally:
                self._lease_context.lease = None
                self._lease_context.thread_id = None

    def recover_scope(self, gate: object, writer_lease: WriterLease) -> None:
        """Finish E-backed Risk resolutions and full-book cuts before readiness."""

        self._require_gate(gate)
        with self._writer_lease(writer_lease):
            snapshot = self._store.read_execution_recovery_snapshot(
                (self._execution_scope,)
            )
            for item in snapshot.orders:
                if item.record.state is ExecutionState.FILLED:
                    if item.record.permit_reference is None:
                        raise DurableStoreError("filled paper order has no Risk permit")
                    self._resolve_order(gate, item.record, writer_lease)
            self._rebase_book(gate, writer_lease)

    def resolve_evidenced_outcome(
        self,
        gate: object,
        record: ExecutionRecord,
        writer_lease: WriterLease,
    ) -> None:
        """Resolve an atomic E Paper fill through its exact durable Risk claim."""

        self._require_gate(gate)
        if type(record) is not ExecutionRecord or record.state is not ExecutionState.FILLED:
            raise DurableStoreError("paper Risk resolution requires a filled E record")
        with self._writer_lease(writer_lease):
            self._resolve_order(gate, record, writer_lease)
            self._rebase_book(gate, writer_lease)

    def _require_gate(self, gate: object) -> None:
        if not self.is_bound_to_composition(
            gate,
            store=self._store,
            execution_scope=self._execution_scope,
            owner_domain=self._owner_domain,
            matching_port=self._matching_port,
            risk_scope=self._risk_scope,
            instrument_registry=self._instrument_registry,
        ):
            raise DurableStoreError("paper Risk gate lost its canonical E authority binding")

    def _resolve_order(
        self, gate: object, record: ExecutionRecord, writer_lease: WriterLease
    ) -> None:
        source = self._terminal_source(record.intent_id, writer_lease)
        intent = source.intent
        if intent.position_effect not in (PositionEffect.OPEN, PositionEffect.CLOSE):
            raise DurableStoreError("paper fill has an unsupported position effect")
        permit_id = record.permit_reference
        if permit_id is None:
            raise DurableStoreError("paper fill has no Risk permit reference")
        existing = self._read_resolution_receipt(gate, intent.position_effect, permit_id)
        claim = self._claim_for_resolution(gate, permit_id, intent, source, existing)
        if existing is not None:
            self._verify_existing_resolution(existing, claim, source)
            return
        gate.ensure_settled(permit_id)
        observation = self._observation(source, claim, writer_lease)
        from bt_api_risk.core.admission import (
            DispatchEvidenceClass,
            DispatchExposureResolutionProof,
        )

        proof = DispatchExposureResolutionProof(
            claim=claim,
            evidence_class=DispatchEvidenceClass.SIMULATION_JOURNAL,
            dispatch_attempt_count=record.dispatch_attempts,
            reconciliation_evidence_sha256=source.source_sha256,
            observation=observation,
        )
        self._store.assert_writer_lease(self._execution_scope, writer_lease)
        self._resolution_context.proof = proof
        self._resolution_context.source_sha256 = source.source_sha256
        self._resolution_context.thread_id = threading.get_ident()
        try:
            if intent.position_effect is PositionEffect.CLOSE:
                gate.resolve_dispatch_close(proof)
            else:
                gate.resolve_dispatch_exposure(proof)
        finally:
            self._resolution_context.proof = None
            self._resolution_context.source_sha256 = None
            self._resolution_context.thread_id = None
        self._store.assert_writer_lease(self._execution_scope, writer_lease)
        committed = self._read_resolution_receipt(gate, intent.position_effect, permit_id)
        if committed is None:
            raise DurableStoreError("paper Risk resolution did not persist a receipt")
        self._verify_existing_resolution(committed, claim, source, proof=proof)

    def _claim_for_resolution(
        self,
        gate: object,
        permit_id: str,
        intent: OrderIntent,
        source: _PaperTerminalSource,
        receipt: object | None,
    ) -> object:
        from bt_api_risk.core.admission import DispatchClaimBinding

        claim = receipt.claim if receipt is not None else gate.dispatch_claim_binding(permit_id)
        # Risk hashes the trusted mapped RiskIntent; E separately retains the
        # submitted OrderIntent fingerprint in its immutable record.  The
        # observation binds both values in their respective fields.
        if (
            type(claim) is not DispatchClaimBinding
            or claim.scope != self._risk_scope
            or claim.intent_id != intent.intent_id
            or claim.permit_id != permit_id
            or source.record.payload_sha256 != intent.fingerprint
        ):
            raise DurableStoreError("paper E intent does not match its exact Risk dispatch claim")
        return claim

    @staticmethod
    def _read_resolution_receipt(gate: object, effect: PositionEffect, permit_id: str):
        from bt_api_risk.core.admission import (
            DispatchExposureResolutionReceiptStatus,
            PermitInvalidError,
        )

        getter = (
            gate.dispatch_close_resolution_receipt
            if effect is PositionEffect.CLOSE
            else gate.dispatch_exposure_resolution_receipt
        )
        try:
            receipt = getter(permit_id)
        except PermitInvalidError as error:
            if error.code in {
                "PERMIT_NOT_DISPATCH_CLAIMED",
                "DISPATCH_CLOSE_STATE_CONFLICT",
                "DISPATCH_EXPOSURE_STATE_CONFLICT",
            }:
                return None
            raise
        if receipt.status is DispatchExposureResolutionReceiptStatus.UNRESOLVED_ACTIVE:
            return None
        expected = (
            DispatchExposureResolutionReceiptStatus.TERMINAL_CLOSE_RESOLVED
            if effect is PositionEffect.CLOSE
            else DispatchExposureResolutionReceiptStatus.TERMINAL_EXPOSURE_RESOLVED
        )
        if receipt.status is not expected:
            raise DurableStoreError("paper Risk resolution receipt has the wrong terminal type")
        return receipt

    @staticmethod
    def _verify_existing_resolution(
        receipt: object,
        claim: object,
        source: _PaperTerminalSource,
        *,
        proof: object | None = None,
    ) -> None:
        from bt_api_risk.core.admission import (
            DispatchExposureResolutionProof,
            DispatchExposureResolutionReceipt,
            OrderExposureObservationProof,
            OrderExposureState,
        )

        if type(receipt) is not DispatchExposureResolutionReceipt:
            raise DurableStoreError("persisted Risk receipt has an unsupported type")
        if not all(
            (
                receipt.claim == claim,
                receipt.position_effect == source.intent.position_effect.value,
                receipt.journal_revision == source.event.sequence,
                receipt.exposure_reservation_id == source.receipt.fill_id,
                receipt.exposure_reservation_sha256 == source.receipt.receipt_sha256,
                receipt.final_state is OrderExposureState.FILLED,
                receipt.requested_quantity == source.intent.quantity,
                receipt.filled_quantity == source.receipt.quantity,
                receipt.trade_count == 1,
                receipt.order_revision == source.event.sequence,
                receipt.dispatch_latch_active is False,
            )
        ):
            raise DurableStoreError("persisted Risk receipt differs from exact Paper fill evidence")
        if proof is not None:
            if (
                type(proof) is not DispatchExposureResolutionProof
                or type(proof.observation) is not OrderExposureObservationProof
            ):
                raise DurableStoreError("submitted Paper proof has an unsupported type")
            observation = proof.observation
            if not all(
                (
                    proof.claim == claim,
                    proof.evidence_class.value == "SIMULATION_JOURNAL",
                    proof.dispatch_attempt_count == source.record.dispatch_attempts,
                    proof.reconciliation_evidence_sha256 == source.source_sha256,
                    proof.journal_revision == source.event.sequence,
                    proof.journal_record_sha256 == source.source_sha256,
                    observation.scope == claim.scope,
                    observation.permit_id == claim.permit_id,
                    observation.intent_id == claim.intent_id,
                    observation.intent_hash == claim.intent_hash,
                    observation.provider_order_id == source.record.provider_order_id,
                    observation.accepted_request_sha256 == source.intent.fingerprint,
                    observation.exposure_reservation_id == source.receipt.fill_id,
                    observation.exposure_reservation_sha256 == source.receipt.receipt_sha256,
                    observation.state is OrderExposureState.FILLED,
                    observation.requested_quantity == source.intent.quantity,
                    observation.cumulative_filled_quantity == source.receipt.quantity,
                    observation.fill_trade_count == 1,
                    observation.order_revision == source.event.sequence,
                    observation.journal_revision == source.event.sequence,
                    observation.journal_record_sha256 == source.source_sha256,
                    receipt.proof_sha256 == proof.fingerprint,
                    receipt.exposure_proof_sha256 == observation.fingerprint,
                )
            ):
                raise DurableStoreError("new Risk receipt does not bind the submitted Paper proof")

    @contextmanager
    def dispatch_exposure_resolution_guard(self, proof: object, *, claim: object) -> Iterator[object]:
        """Verify the current Paper E source or delegate non-Paper scopes exactly."""

        from bt_api_risk.core.admission import (
            DispatchClaimBinding,
            DispatchExposureResolutionProof,
            VerifiedDispatchExposureResolution,
        )

        if type(proof) is not DispatchExposureResolutionProof or type(claim) is not DispatchClaimBinding:
            yield None
            return
        if proof.scope.provider != PAPER_PROVIDER:
            guard = self._fallback_authority.dispatch_exposure_resolution_guard
            with guard(proof, claim=claim) as verified:
                yield verified
            return
        if proof.scope != self._risk_scope or claim != proof.claim:
            yield None
            return
        expected = getattr(self._resolution_context, "proof", None)
        expected_source = getattr(self._resolution_context, "source_sha256", None)
        current_lease = getattr(self._lease_context, "lease", None)
        thread_id = threading.get_ident()
        if (
            proof is not expected
            or expected_source is None
            or current_lease is None
            or getattr(self._resolution_context, "thread_id", None) != thread_id
            or getattr(self._lease_context, "thread_id", None) != thread_id
        ):
            yield None
            return
        try:
            self._store.assert_writer_lease(self._execution_scope, current_lease)
            source = self._terminal_source(proof.intent_id, current_lease)
            if (
                source.source_sha256 != expected_source
                or proof.reconciliation_evidence_sha256 != source.source_sha256
                or proof.journal_record_sha256 != source.source_sha256
                # ``proof.intent_hash`` is the exact Risk claim hash.  The
                # accepted request hash is the raw E OrderIntent fingerprint.
                or source.intent.fingerprint != proof.observation.accepted_request_sha256
                or source.record.permit_reference != proof.permit_id
                or source.record.dispatch_attempts != proof.dispatch_attempt_count
                or source.event.sequence != proof.journal_revision
                or source.receipt.receipt_sha256
                != proof.observation.exposure_reservation_sha256
                or proof.observation.fingerprint != expected.observation.fingerprint
            ):
                yield None
                return
            observation = proof.observation
            verified = VerifiedDispatchExposureResolution(
                scope=proof.scope,
                permit_id=proof.permit_id,
                intent_id=proof.intent_id,
                intent_hash=proof.intent_hash,
                cause_id=proof.cause_id,
                claim_digest=proof.claim_digest,
                proof_sha256=proof.fingerprint,
                exposure_proof_sha256=observation.fingerprint,
                provider_order_id=observation.provider_order_id,
                accepted_request_sha256=observation.accepted_request_sha256,
                exposure_reservation_id=observation.exposure_reservation_id,
                exposure_reservation_sha256=observation.exposure_reservation_sha256,
                final_state=observation.state,
                requested_quantity=observation.requested_quantity,
                filled_quantity=observation.cumulative_filled_quantity,
                trade_count=observation.fill_trade_count,
                order_revision=observation.order_revision,
                journal_revision=observation.journal_revision,
                journal_record_sha256=observation.journal_record_sha256,
                writer_owner_id=observation.writer_owner_id,
                writer_fencing_token=observation.writer_fencing_token,
                writer_fence_sha256=observation.writer_fence_sha256,
            )
            yield verified
        finally:
            self._store.assert_writer_lease(self._execution_scope, current_lease)

    def _terminal_source(self, intent_id: str, writer_lease: WriterLease) -> _PaperTerminalSource:
        if type(intent_id) is not str or not intent_id:
            raise DurableStoreError("paper source intent selector is invalid")
        self._store.assert_writer_lease(self._execution_scope, writer_lease)
        snapshot = self._store.read_execution_recovery_snapshot((self._execution_scope,))
        matches = tuple(
            item
            for item in snapshot.orders
            if item.record.intent_id == intent_id
            and item.record.scope_key == self._execution_scope.key
        )
        if len(matches) != 1:
            raise DurableStoreError("paper source has no unique durable order")
        item = matches[0]
        record, intent = item.record, item.intent
        if (
            record.state is not ExecutionState.FILLED
            or record.dispatch_attempts != 1
            or record.review_required
            or record.provider_order_id is None
            or record.permit_reference is None
            or record.payload_sha256 != intent.fingerprint
            or intent.scope != self._execution_scope
        ):
            raise DurableStoreError("paper order is not one complete durable filled dispatch")
        events = tuple(
            event
            for event in snapshot.events
            if event.intent_id == intent_id
            and event.scope_key == self._execution_scope.key
            and event.event_type == "provider_observation"
            and event.state is ExecutionState.FILLED
        )
        if len(events) != 1:
            raise DurableStoreError("paper fill has no unique immutable provider event")
        event = events[0]
        if (
            event.sequence <= 0
            or event.sequence > snapshot.cut.high_water_sequence
            or event.journal_incarnation_id != snapshot.cut.journal_generation
            or not isinstance(event.payload, Mapping)
            or event.payload.get("source") != "provider"
            or event.payload.get("provider_order_id") != record.provider_order_id
        ):
            raise DurableStoreError("paper provider event is outside its exact journal cut")
        receipt = self._store.get_managed_paper_fill_v2_for_event(
            event.event_id,
            event_sequence=event.sequence,
            journal_generation=snapshot.cut.journal_generation,
            scope=self._execution_scope,
        )
        if type(receipt) is not PaperMatchReceiptV2:
            raise DurableStoreError("paper order lacks an exact managed V2 fill receipt")
        expected_effect = (
            PositionEffect.OPEN
            if intent.side is Side.BUY
            else PositionEffect.CLOSE
            if intent.side is Side.SELL
            else None
        )
        expected_reduce_only = expected_effect is PositionEffect.CLOSE
        if (
            expected_effect is None
            or intent.position_effect is not expected_effect
            or intent.reduce_only is not expected_reduce_only
            or intent.position_side != "NET"
            or intent.position_bucket != "ALL"
            or intent.order_type.value != "MARKET"
            or intent.time_in_force.value != "FOK"
            or intent.instrument != receipt.instrument
            or intent.side is not receipt.side
            or intent.quantity != receipt.quantity
            or intent.fingerprint != receipt.intent_fingerprint
            or receipt.scope_key != self._execution_scope.key
            or receipt.journal_generation != snapshot.cut.journal_generation
            or receipt.event_id != event.event_id
            or receipt.event_sequence != event.sequence
            or receipt.intent_id != intent.intent_id
            or receipt.execution_domain_ref != self._owner_domain.execution_domain_ref
            or receipt.simulation_account_ref != self._owner_domain.simulation_account_ref
            or receipt.strategy_ref != self._owner_domain.strategy_ref
            or receipt.experiment_ref != self._owner_domain.experiment_ref
            or receipt.model_ref != self._owner_domain.model_ref
            or receipt.model_version != self._owner_domain.model_version
            or receipt.state is not ExecutionState.FILLED
            or receipt.fill_price is None
            or record.filled_quantity != receipt.quantity
            or record.average_price != receipt.fill_price
            or record.cumulative_commission != receipt.fee_amount
            or event.payload.get("filled_quantity") != format(receipt.quantity, "f")
            or event.payload.get("average_price") != format(receipt.fill_price, "f")
            or event.payload.get("cumulative_commission") != format(receipt.fee_amount, "f")
        ):
            raise DurableStoreError("paper V2 fill does not prove the exact E OrderIntent")
        source_payload = {
            "domain_fingerprint": self._owner_domain.fingerprint,
            "event": {
                "created_at_ns": event.created_at_ns,
                "event_id": event.event_id,
                "event_type": event.event_type,
                "intent_id": event.intent_id,
                "journal_incarnation_id": event.journal_incarnation_id,
                "payload": dict(event.payload),
                "scope_key": event.scope_key,
                "sequence": event.sequence,
                "state": event.state.value,
            },
            "intent": intent.to_payload(),
            "record": {
                "average_price": None
                if record.average_price is None
                else format(record.average_price, "f"),
                "cumulative_commission": None
                if record.cumulative_commission is None
                else format(record.cumulative_commission, "f"),
                "dispatch_attempts": record.dispatch_attempts,
                "intent_id": record.intent_id,
                "payload_sha256": record.payload_sha256,
                "permit_reference": record.permit_reference,
                "provider_order_id": record.provider_order_id,
                "review_required": record.review_required,
                "scope_key": record.scope_key,
                "state": record.state.value,
                "filled_quantity": format(record.filled_quantity, "f"),
            },
            "receipt": receipt.to_payload(),
            "schema": "bt-api-paper-risk-source.v1",
        }
        return _PaperTerminalSource(
            record=record,
            intent=intent,
            event=event,
            receipt=receipt,
            source_sha256=payload_sha256(source_payload),
        )

    def _observation(self, source: _PaperTerminalSource, claim: object, lease: WriterLease):
        from bt_api_risk.core.exposure import (
            OrderExposureObservationProof,
            OrderExposureState,
            OrderFillFact,
            fill_ids_sha256,
        )

        intent = source.intent
        spec = self._owner_domain.instrument(intent.instrument)
        metadata = self._instrument_registry.get(intent.instrument)
        if (
            spec is None
            or metadata is None
            or metadata.instrument != intent.instrument
            or metadata.metadata_version != intent.metadata_version
            or metadata.quantity_unit != spec.base_currency
            or metadata.quantity_step != spec.quantity_step
            or metadata.tick_size != spec.price_tick
            or metadata.quote_currency != spec.quote_currency
            or metadata.valuation_unit != self._owner_domain.cash_currency
            or metadata.contract_multiplier != Decimal("1")
        ):
            raise DurableStoreError("Paper instrument metadata is not bound to the Risk source")
        now_ns = time.time_ns()
        match_time_ns = source.receipt.match_time_ns
        if (
            type(match_time_ns) is not int
            or match_time_ns > now_ns
            or not metadata.as_of_ns <= match_time_ns < metadata.expires_at_ns
            or not metadata.as_of_ns <= now_ns < metadata.expires_at_ns
        ):
            raise DurableStoreError(
                "Paper instrument metadata is not valid at the exact fill and observation times"
            )
        valid_until_ns = metadata.expires_at_ns
        metadata_digest = payload_sha256(metadata.to_payload())
        fill = OrderFillFact(
            trade_id=source.receipt.fill_id,
            quantity=source.receipt.quantity,
            price=source.receipt.fill_price,
            side=intent.side.value,
            position_effect=intent.position_effect.value,
            fee_amount=source.receipt.fee_amount,
            fee_currency=source.receipt.fee_currency,
            source_record_sha256=source.receipt.receipt_sha256,
            source_revision=source.event.sequence,
        )
        fill_tuple = (fill,)
        quote_payload = source.receipt.quote.to_payload()
        metadata_payload = spec.to_payload()
        valuation_facts = {
            "currency": self._owner_domain.cash_currency,
            "fee_amount": format(source.receipt.fee_amount, "f"),
            "fill_price": format(source.receipt.fill_price, "f"),
            "fill_quantity": format(source.receipt.quantity, "f"),
            "quote": quote_payload,
            "schema": "bt-api-paper-risk-valuation.v1",
        }
        observation_facts = {
            "execution_source_sha256": source.source_sha256,
            "fill_history_sha256": payload_sha256(
                {"fills": [fill.fingerprint], "complete": True}
            ),
            "observed_at_ns": now_ns,
            "provider_order_id": source.record.provider_order_id,
            "schema": "bt-api-paper-risk-observation.v1",
        }
        from .group_contracts import writer_lease_fingerprint

        return OrderExposureObservationProof(
            scope=claim.scope,
            permit_id=claim.permit_id,
            intent_id=claim.intent_id,
            intent_hash=claim.intent_hash,
            dispatch_claim_digest=claim.claim_digest,
            dispatch_proof_sha256=None,
            provider_order_id=source.record.provider_order_id,
            accepted_request_sha256=intent.fingerprint,
            exposure_reservation_id=source.receipt.fill_id,
            exposure_reservation_sha256=source.receipt.receipt_sha256,
            order_revision=source.event.sequence,
            journal_revision=source.event.sequence,
            journal_record_sha256=source.source_sha256,
            observation_sha256=payload_sha256(observation_facts),
            fill_history_sha256=payload_sha256(
                {"fills": [fill.fingerprint], "complete": True}
            ),
            source_alias_map_sha256=payload_sha256(
                {
                    "intent_id": intent.intent_id,
                    "provider_order_id": source.record.provider_order_id,
                    "schema": "bt-api-paper-alias-map.v1",
                }
            ),
            state=OrderExposureState.FILLED,
            requested_quantity=intent.quantity,
            cumulative_filled_quantity=source.receipt.quantity,
            leaves_quantity=Decimal("0"),
            filled_gross_notional=(
                source.receipt.quantity
                * source.receipt.fill_price
                * metadata.contract_multiplier
            ),
            leaves_gross_notional=Decimal("0"),
            leaves_valuation_price=None,
            notional_unit=self._owner_domain.cash_currency,
            complete_order_snapshot=True,
            complete_fill_history=True,
            fill_watermark_revision=source.event.sequence,
            source_contract_ref="bt-api-execution.managed-paper-fill",
            source_contract_version=source.receipt.schema,
            provider_profile_version=self._owner_domain.fingerprint,
            normalizer_schema_version="bt-api-paper-v2",
            order_side=intent.side.value,
            position_effect=intent.position_effect.value,
            fills=fill_tuple,
            fill_trade_count=1,
            fill_ids_sha256=fill_ids_sha256(fill_tuple),
            instrument_id=intent.instrument,
            instrument_version=spec.metadata_version,
            instrument_multiplier=metadata.contract_multiplier,
            quote_currency=spec.quote_currency,
            valuation_unit=self._owner_domain.cash_currency,
            instrument_metadata_sha256=metadata_digest,
            instrument_metadata_observed_at_ns=now_ns,
            instrument_metadata_valid_until_ns=valid_until_ns,
            valuation_facts_sha256=payload_sha256(valuation_facts),
            valuation_observed_at_ns=now_ns,
            valuation_valid_until_ns=valid_until_ns,
            fx_pair=None,
            fx_rate=None,
            fx_observed_at_ns=None,
            fx_valid_until_ns=None,
            observed_at_ns=now_ns,
            valid_until_ns=valid_until_ns,
            writer_owner_id=lease.owner_id,
            writer_fencing_token=lease.fencing_token,
            writer_fence_sha256=writer_lease_fingerprint(lease),
            position_side=intent.position_side,
            position_bucket=intent.position_bucket,
        )

    def _rebase_book(self, gate: object, writer_lease: WriterLease) -> None:
        from bt_api_risk import RiskDeniedError
        from bt_api_risk.core.closable_position_rebase import ClosablePositionRebaseV1

        self._store.assert_writer_lease(self._execution_scope, writer_lease)
        recovery = self._store.read_execution_recovery_snapshot((self._execution_scope,))
        book = self._store.read_paper_book(self._execution_scope)
        self._validate_book_cut(recovery, book)
        event_payloads = [
            self._event_payload(event)
            for event in recovery.events
            if event.sequence <= book.execution_cursor
        ]
        quantities = {row.instrument: row.quantity for row in book.positions}
        for spec in self._owner_domain.instruments:
            quantity = quantities.get(spec.instrument, Decimal("0"))
            latest_cut = gate.get_closable_position_rebase(
                self._risk_scope,
                spec.instrument,
                self._owner_domain.strategy_ref,
                position_side="NET",
                position_bucket="ALL",
            )
            if latest_cut is not None and type(latest_cut) is not ClosablePositionRebaseV1:
                raise DurableStoreError("Risk returned an invalid persisted PaperBook cut")
            if latest_cut is not None and (
                latest_cut.scope != self._risk_scope
                or latest_cut.instrument != spec.instrument
                or latest_cut.strategy_id != self._owner_domain.strategy_ref
                or latest_cut.position_side != "NET"
                or latest_cut.position_bucket != "ALL"
                or latest_cut.quantity_unit != spec.base_currency
            ):
                raise DurableStoreError("persisted Risk cut is outside the Paper NET/ALL key")
            if latest_cut is not None and (
                latest_cut.execution_generation != book.execution_journal_generation
            ):
                raise DurableStoreError("Paper execution generation changed after its Risk cut")
            if latest_cut is not None and latest_cut.execution_cursor > book.execution_cursor:
                raise DurableStoreError("PaperBook cursor is older than its persisted Risk cut")

            unabsorbed_refs = gate.get_unabsorbed_settled_close_receipts(
                self._risk_scope,
                spec.instrument,
                self._owner_domain.strategy_ref,
                position_side="NET",
                position_bucket="ALL",
            )
            if type(unabsorbed_refs) is not tuple:
                raise DurableStoreError("Risk returned an invalid terminal close receipt set")
            same_cut = (
                latest_cut is not None
                and latest_cut.execution_cursor == book.execution_cursor
            )
            if same_cut:
                # The Risk cut already absorbed these terminal receipts. Re-read
                # their exact refs from that durable cut and prove the same E
                # source again; an empty "unabsorbed" set alone is not a proof
                # that the previous cut matches the current PaperBook.
                if unabsorbed_refs:
                    raise DurableStoreError(
                        "Risk has an unabsorbed close at an already-bound PaperBook cut"
                    )
                refs = latest_cut.absorbed_close_receipts
            else:
                refs = unabsorbed_refs
            ref_sources = self._verify_absorbed_close_sources(
                refs,
                recovery,
                book,
                spec.instrument,
                writer_lease,
            )
            try:
                current = gate.get_closable_position(
                    self._risk_scope,
                    spec.instrument,
                    self._owner_domain.strategy_ref,
                    position_side="NET",
                    position_bucket="ALL",
                )
            except RiskDeniedError as error:
                if error.code != "CLOSE_POSITION_UNCONFIGURED":
                    raise
                if latest_cut is not None:
                    raise DurableStoreError(
                        "persisted Risk cut has no matching closable declaration"
                    ) from error
                gate.set_closable_position(
                    self._risk_scope,
                    spec.instrument,
                    self._owner_domain.strategy_ref,
                    "paper-book-initial-declaration-v1",
                    closable_quantity=Decimal("0"),
                    quantity_unit=spec.base_currency,
                    position_side="NET",
                    position_bucket="ALL",
                )
                current = gate.get_closable_position(
                    self._risk_scope,
                    spec.instrument,
                    self._owner_domain.strategy_ref,
                    position_side="NET",
                    position_bucket="ALL",
                )
            if (
                current.scope != self._risk_scope
                or current.instrument != spec.instrument
                or current.strategy_id != self._owner_domain.strategy_ref
                or current.position_side != "NET"
                or current.position_bucket != "ALL"
                or current.quantity_unit != spec.base_currency
                or current.completeness != "LOCAL_LEDGER_COMPLETE"
            ):
                raise DurableStoreError("Risk closable declaration is outside the Paper NET/ALL key")
            if latest_cut is not None and (
                current.declared_quantity != latest_cut.closable_quantity
            ):
                raise DurableStoreError(
                    "Risk closable declaration diverged from its persisted PaperBook cut"
                )
            snapshot_sha256 = payload_sha256(
                {
                    "absorbed_close_source": ref_sources,
                    "book": self._book_payload(book),
                    "domain_fingerprint": self._owner_domain.fingerprint,
                    "events": event_payloads,
                    "instrument": spec.to_payload(),
                    "schema": "bt-api-paper-book-risk-rebase.v1",
                }
            )
            proof = ClosablePositionRebaseV1(
                scope=self._risk_scope,
                instrument=spec.instrument,
                strategy_id=self._owner_domain.strategy_ref,
                closable_quantity=quantity,
                quantity_unit=spec.base_currency,
                execution_generation=book.execution_journal_generation,
                execution_cursor=book.execution_cursor,
                snapshot_sha256=snapshot_sha256,
                absorbed_close_receipts=refs,
                position_side="NET",
                position_bucket="ALL",
            )
            if same_cut:
                if (
                    proof.closable_quantity != latest_cut.closable_quantity
                    or proof.quantity_unit != latest_cut.quantity_unit
                    or proof.execution_generation != latest_cut.execution_generation
                    or proof.execution_cursor != latest_cut.execution_cursor
                    or proof.snapshot_sha256 != latest_cut.snapshot_sha256
                    or proof.absorbed_close_receipts != latest_cut.absorbed_close_receipts
                ):
                    raise DurableStoreError(
                        "current PaperBook does not match its persisted Risk cut"
                    )
                # Exact same-cut recovery is a verified read-only replay. Calling
                # rebase here would be safe if byte-identical, but skipping the
                # write makes restart recovery explicitly idempotent.
                continue
            rebased = gate.rebase_closable_position(proof)
            if (
                rebased.declared_quantity != quantity
                or rebased.quantity_unit != spec.base_currency
                or rebased.position_side != "NET"
                or rebased.position_bucket != "ALL"
                or rebased.available_quantity is None
                or rebased.available_quantity < 0
            ):
                raise DurableStoreError("Risk did not read back the exact full PaperBook rebase")
        self._store.assert_writer_lease(self._execution_scope, writer_lease)

    def _verify_absorbed_close_sources(
        self,
        refs: tuple[object, ...],
        recovery: object,
        book: PaperBookSnapshotV1,
        instrument: str,
        writer_lease: WriterLease,
    ) -> list[dict[str, object]]:
        from bt_api_risk.core.closable_position_rebase import ClosablePositionCloseReceiptV1

        if type(refs) is not tuple or any(type(item) is not ClosablePositionCloseReceiptV1 for item in refs):
            raise DurableStoreError("Risk returned an invalid terminal close receipt set")
        sources: list[dict[str, object]] = []
        seen_events: set[str] = set()
        for ref in refs:
            matching_orders = tuple(
                item
                for item in recovery.orders
                if item.record.intent_id == ref.intent_id
                and item.record.scope_key == self._execution_scope.key
            )
            if len(matching_orders) != 1:
                raise DurableStoreError("Risk close receipt has no unique E order")
            item = matching_orders[0]
            record, intent = item.record, item.intent
            matching_events = tuple(
                event
                for event in recovery.events
                if event.intent_id == ref.intent_id
                and event.scope_key == self._execution_scope.key
                and event.event_type == "provider_observation"
                and event.state is ExecutionState.FILLED
                and event.sequence <= book.execution_cursor
                and event.journal_incarnation_id == book.execution_journal_generation
            )
            if len(matching_events) != 1:
                raise DurableStoreError("Risk close receipt has no unique in-cut E fill event")
            event = matching_events[0]
            if event.event_id in seen_events:
                raise DurableStoreError("Risk close receipts alias one E event")
            seen_events.add(event.event_id)
            if (
                record.permit_reference != ref.permit_id
                or record.state is not ExecutionState.FILLED
                or record.review_required
                or intent.side is not Side.SELL
                or intent.position_effect is not PositionEffect.CLOSE
                or not intent.reduce_only
                or intent.position_side != "NET"
                or intent.position_bucket != "ALL"
                or intent.instrument != instrument
            ):
                raise DurableStoreError("Risk close receipt does not bind a NET/ALL SELL close")
            source = self._terminal_source(ref.intent_id, writer_lease)
            if source.event.event_id != event.event_id:
                raise DurableStoreError("Risk close receipt source event changed during verification")
            sources.append(
                {
                    "execution_event_id": event.event_id,
                    "execution_event_sequence": event.sequence,
                    "execution_fill_sha256": source.receipt.receipt_sha256,
                    "intent_id": ref.intent_id,
                    "permit_id": ref.permit_id,
                    "risk_close_receipt_sha256": ref.close_receipt_sha256,
                    "source_sha256": source.source_sha256,
                }
            )
        if sources != sorted(sources, key=lambda item: str(item["permit_id"])):
            raise DurableStoreError("Risk close receipt source set is not canonical")
        return sources

    def _validate_book_cut(self, recovery: object, book: object) -> None:
        if (
            type(book) is not PaperBookSnapshotV1
            or book.scope_key != self._execution_scope.key
            or book.execution_domain_ref != self._owner_domain.execution_domain_ref
            or book.simulation_account_ref != self._owner_domain.simulation_account_ref
            or book.experiment_ref != self._owner_domain.experiment_ref
            or book.model_ref != self._owner_domain.model_ref
            or book.model_version != self._owner_domain.model_version
            or book.cash_currency != self._owner_domain.cash_currency
            or book.execution_journal_generation != recovery.cut.journal_generation
            or book.execution_cursor > recovery.cut.high_water_sequence
            or any(
                event.scope_key != self._execution_scope.key
                or event.sequence > book.execution_cursor
                or event.journal_incarnation_id != book.execution_journal_generation
                for event in recovery.events
            )
        ):
            raise DurableStoreError("PaperBook and selected E recovery snapshot do not share a valid cut")
        if any(row.instrument not in {item.instrument for item in self._owner_domain.instruments} for row in book.positions):
            raise DurableStoreError("PaperBook contains an instrument outside its trusted domain")

    @staticmethod
    def _event_payload(event: ExecutionEvent) -> dict[str, object]:
        return {
            "created_at_ns": event.created_at_ns,
            "event_id": event.event_id,
            "event_type": event.event_type,
            "intent_id": event.intent_id,
            "journal_incarnation_id": event.journal_incarnation_id,
            "payload": dict(event.payload),
            "scope_key": event.scope_key,
            "sequence": event.sequence,
            "state": event.state.value,
        }

    @staticmethod
    def _book_payload(book: PaperBookSnapshotV1) -> dict[str, object]:
        return {
            "account_key": book.account_key,
            "cash": format(book.cash, "f"),
            "cash_currency": book.cash_currency,
            "cumulative_fees": format(book.cumulative_fees, "f"),
            "execution_cursor": book.execution_cursor,
            "execution_domain_ref": book.execution_domain_ref,
            "execution_journal_generation": book.execution_journal_generation,
            "experiment_ref": book.experiment_ref,
            "model_ref": book.model_ref,
            "model_version": book.model_version,
            "positions": [
                {
                    "average_cost": format(row.average_cost, "f"),
                    "instrument": row.instrument,
                    "quantity": format(row.quantity, "f"),
                }
                for row in book.positions
            ],
            "quote_watermarks": [list(item) for item in book.quote_watermarks],
            "receipt_count": book.receipt_count,
            "schema": "bt-api-paper-book-snapshot.v1",
            "scope_key": book.scope_key,
            "simulation_account_ref": book.simulation_account_ref,
        }


__all__ = ("PaperExecutionCloseAuthority",)
