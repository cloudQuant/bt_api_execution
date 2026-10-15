"""Synthetic provider flow through the real durable Risk group APIs."""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from decimal import Decimal

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    AccountScopedGroupAdmissionAdapter,
    ExecutionScope,
    ExecutionState,
    GroupAccountReservationSnapshot,
    GroupChildOrderStatus,
    GroupDispatchResolutionReceipt,
    GroupLegAuthorization,
    GroupLegClaimReceipt,
    GroupLegOrderReceipt,
    GroupOrderCoordinator,
    GroupOrderIntent,
    GroupOrderLegPlan,
    GroupOrderState,
    GroupPermitBinding,
    GroupReservationPhase,
    GroupRiskLegSnapshot,
    OrderIntent,
    OrderType,
    PortfolioGroupBudgetPolicy,
    PositionEffect,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
    SqliteOrderGroupStore,
    TimeInForce,
    build_instrument_risk_group_budget_quote,
    group_leg_intent_id,
    payload_sha256,
    risk_account_scope_fingerprint,
    writer_lease_fingerprint,
)
from bt_api_execution.service_order_entry import ExecutionOrderEntryService

_NOW_NS = 1_800_000_000_000_000_000
_DEADLINE_NS = _NOW_NS + 10_000_000_000
_WIRE_ACCOUNT = "wire-account-a"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class _RiskIntentMapper:
    """Trusted test mapper with a Risk account ID distinct from the wire alias."""

    def __init__(self, risk_api, scope) -> None:
        self._risk = risk_api
        self.scope = scope
        self.wire_account_ref = _WIRE_ACCOUNT

    def __call__(self, intent: OrderIntent):
        return self.map_group_intent(intent)

    def map_group_intent(self, intent: OrderIntent):
        profile = payload_sha256(
            {
                "instrument": intent.instrument,
                "side": intent.side.value,
                "position_effect": "OPEN",
                "quantity_unit": "contract",
                "notional_unit": "USD",
            }
        )
        return self._risk.RiskIntent(
            intent_id=intent.intent_id,
            scope=self.scope,
            action=self._risk.IntentAction.INCREASE,
            notional=intent.quantity * intent.price,
            payload_fingerprint=payload_sha256(
                {"order_fingerprint": intent.fingerprint, "risk_mapper": "fixture-v1"}
            ),
            strategy_id=intent.scope.strategy_id,
            allocation_version="allocation-v1",
            instrument=intent.instrument,
            quantity=intent.quantity,
            quantity_unit="contract",
            notional_unit="USD",
            exposure_profile_fingerprint=profile,
            instrument_facts_observed_at_ns=_NOW_NS - 1_000_000,
            instrument_facts_valid_until_ns=_DEADLINE_NS,
        )


class _RiskReservationOwner:
    def __init__(self, account_ref: str, scope, adapter) -> None:
        self.account_ref = account_ref
        self.scope = scope
        self.adapter = adapter
        self.risk_scope_fingerprint = payload_sha256({"scope_key": scope.key})

    def reserve_group_vector(self, group_id, members, *, deadline_ns):
        return self._snapshot(
            self.adapter.reserve_group_vector(
                self.scope, group_id, members, deadline_ns=deadline_ns
            )
        )

    def query_group_vector(self, group_id):
        return self._snapshot(self.adapter.query_group_vector(self.scope, group_id))

    def release_group_unclaimed(self, group_id, reason):
        return self._snapshot(self.adapter.release_group_unclaimed(self.scope, group_id, reason))

    def _snapshot(self, receipt):
        terminal = {"RELEASED", "EXPIRED", "NO_FILL_TERMINAL"}
        all_terminal = all(leg.state.value in terminal for leg in receipt.legs)
        phase = GroupReservationPhase.RELEASED if all_terminal else GroupReservationPhase.ACTIVE
        return GroupAccountReservationSnapshot(
            group_id=receipt.group_id,
            account_ref=self.account_ref,
            scope_fingerprint=self.risk_scope_fingerprint,
            phase=phase,
            group_fingerprint=receipt.group_fingerprint,
            policy_fingerprint=receipt.policy_fingerprint,
            deadline_ns=receipt.deadline_ns,
            legs=tuple(
                GroupRiskLegSnapshot(
                    leg_id=leg.leg_id,
                    permit_id=leg.permit_id,
                    intent_id=leg.intent_id,
                    reserved_intent_hash=leg.reserved_intent_hash,
                    exposure_profile_fingerprint=leg.exposure_profile_fingerprint,
                    actual_intent_hash=leg.actual_intent_hash,
                    # The account-reservations resource's wire DTO uses the
                    # stable uppercase lifecycle labels; Risk enum values are
                    # intentionally lowercase for its own public API.
                    state=leg.state.name,
                    claimed=leg.claimed,
                    resolved=leg.resolved,
                    expires_at_ns=leg.expires_at_ns,
                )
                for leg in receipt.legs
            ),
        )


class _SyntheticExposureAuthority:
    """Models exact source evidence from an in-memory synthetic order journal."""

    def __init__(self, risk_api) -> None:
        self._risk = risk_api

    @contextmanager
    def dispatch_exposure_resolution_guard(self, proof, *, claim):
        observation = proof.observation
        yield self._risk.VerifiedDispatchExposureResolution(
            scope=claim.scope,
            permit_id=claim.permit_id,
            intent_id=claim.intent_id,
            intent_hash=claim.intent_hash,
            cause_id=claim.cause_id,
            claim_digest=claim.claim_digest,
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


class _RiskBackedOrdersOwner:
    def __init__(self, risk_api, scope, adapter, reservations, *, lose_first_response=False):
        self._risk = risk_api
        self.scope = scope
        self.adapter = adapter
        self.reservations = reservations
        self.lose_first_response = lose_first_response
        self.submissions: list[OrderIntent] = []
        self.statuses: dict[str, GroupChildOrderStatus] = {}
        self.risk_claims: dict[str, object] = {}
        self.claim_receipts: dict[str, GroupLegClaimReceipt] = {}
        self.resolution_receipts: dict[str, GroupDispatchResolutionReceipt] = {}
        self.query_ids: list[str] = []
        self.resolution_errors: list[str] = []

    def query(self, intent_id: str):
        self.query_ids.append(intent_id)
        status = self.statuses.get(intent_id)
        if status is None:
            return None
        return GroupLegOrderReceipt(
            status=status,
            claim=self.claim_receipts.get(intent_id),
            resolution=self.resolution_receipts.get(intent_id),
        )

    def submit_group_leg(self, intent: OrderIntent, permit):
        permit.validate_order(intent)
        risk_claim = self.adapter.claim_group_leg(
            intent,
            scope=self.scope,
            group_id=permit.group_id,
            leg_id=permit.leg_id,
            permit_reference=permit.permit_id,
            group_fingerprint=permit.group_fingerprint,
            deadline_ns=permit.deadline_ns,
        )
        self.risk_claims[intent.intent_id] = risk_claim
        claim = GroupLegClaimReceipt(
            permit=permit,
            actual_child_intent_hash=risk_claim.actual_intent_hash,
            claim_digest=risk_claim.claim.claim_digest,
            actual_quantity=risk_claim.actual_quantity,
            actual_notional=risk_claim.actual_notional,
        )
        self.claim_receipts[intent.intent_id] = claim
        self.submissions.append(intent)

        if permit.leg_id == "leg-first":
            state, filled, trades, revision = (
                ExecutionState.CANCELLED,
                Decimal("1.5"),
                1,
                3,
            )
        else:
            assert (
                self.reservations.adapter.query_group_vector(self.scope, permit.group_id)
                .legs[0]
                .resolved
            )
            state, filled, trades, revision = ExecutionState.FILLED, intent.quantity, 1, 5
        status = GroupChildOrderStatus(
            intent_id=intent.intent_id,
            state=state,
            filled_quantity=filled,
            provider_order_id=f"synthetic-order-{permit.leg_id}",
            order_revision=revision,
            trade_count=trades,
        )
        self.statuses[intent.intent_id] = status
        if self.lose_first_response and permit.leg_id == "leg-first":
            self.lose_first_response = False
            raise TimeoutError("synthetic provider response was lost")

        self.adapter.settle(risk_claim.permit_reference)
        return GroupLegOrderReceipt(status=status, claim=claim)

    def resolve_group_leg(self, intent, permit, claim, status):
        risk_claim = self.risk_claims[intent.intent_id]
        self.adapter.ensure_settled(risk_claim.permit_reference)
        proof = self._proof(intent, risk_claim, status)
        try:
            receipt = self.adapter.resolve_dispatch_exposure(proof)
        except Exception as error:
            self.resolution_errors.append(f"{type(error).__name__}: {error}")
            raise
        assert (
            receipt.status
            is self._risk.DispatchExposureResolutionReceiptStatus.TERMINAL_EXPOSURE_RESOLVED
        )
        assert not receipt.dispatch_latch_active
        resolution = GroupDispatchResolutionReceipt(
            claim=claim,
            resolution_kind=receipt.status.value,
            proof_sha256=receipt.proof_sha256,
            final_state=ExecutionState(receipt.final_state.value),
            requested_quantity=receipt.requested_quantity,
            filled_quantity=receipt.filled_quantity,
            trade_count=receipt.trade_count,
            order_revision=receipt.order_revision,
            journal_revision=receipt.journal_revision,
            exposure_proof_sha256=receipt.exposure_proof_sha256,
            exposure_reservation_id=receipt.exposure_reservation_id,
            exposure_reservation_sha256=receipt.exposure_reservation_sha256,
            dispatch_latch_active=receipt.dispatch_latch_active,
            has_other_active_freezes=receipt.has_other_active_freezes,
        )
        self.resolution_receipts[intent.intent_id] = resolution
        return resolution

    def _proof(self, intent, risk_claim, status):
        risk = self._risk
        scope = risk_claim.claim.scope
        risk_state = risk.OrderExposureState(status.state.value)
        order_revision = status.order_revision
        journal_revision = order_revision + 1
        # The synthetic Risk clock is fixed at _NOW_NS; use the exact current
        # instant so the source freshness predicate is exercised, not bypassed.
        now_ns = _NOW_NS
        fill = risk.OrderFillFact(
            trade_id=f"{status.provider_order_id}-fill-1",
            quantity=status.filled_quantity,
            price=intent.price,
            side=intent.side.value,
            position_effect="OPEN",
            fee_amount=Decimal("0"),
            fee_currency="USD",
            source_record_sha256=_digest(f"fill-source:{intent.intent_id}"),
            source_revision=journal_revision,
        )
        fills = (fill,)
        observation = risk.OrderExposureObservationProof(
            scope=scope,
            permit_id=risk_claim.permit_reference,
            intent_id=intent.intent_id,
            intent_hash=risk_claim.claim.intent_hash,
            dispatch_claim_digest=risk_claim.claim.claim_digest,
            dispatch_proof_sha256=None,
            provider_order_id=status.provider_order_id,
            accepted_request_sha256=_digest(f"accepted:{intent.fingerprint}"),
            exposure_reservation_id=f"exposure:{intent.intent_id}",
            exposure_reservation_sha256=_digest(f"exposure-reservation:{intent.intent_id}"),
            order_revision=order_revision,
            journal_revision=journal_revision,
            journal_record_sha256=_digest(f"journal:{intent.intent_id}:{journal_revision}"),
            observation_sha256=_digest(f"order-observation:{intent.intent_id}"),
            fill_history_sha256=_digest(f"fill-history:{intent.intent_id}"),
            source_alias_map_sha256=_digest("synthetic-source-alias-map-v1"),
            state=risk_state,
            requested_quantity=intent.quantity,
            cumulative_filled_quantity=status.filled_quantity,
            leaves_quantity=Decimal("0"),
            filled_gross_notional=status.filled_quantity * intent.price,
            leaves_gross_notional=Decimal("0"),
            leaves_valuation_price=None,
            notional_unit="USD",
            complete_order_snapshot=True,
            complete_fill_history=True,
            fill_watermark_revision=journal_revision,
            source_contract_ref="fixture.order.v1",
            source_contract_version="1",
            provider_profile_version="fixture-provider-v1",
            normalizer_schema_version="fixture-normalizer-v1",
            order_side=intent.side.value,
            position_effect="OPEN",
            fills=fills,
            fill_trade_count=len(fills),
            fill_ids_sha256=risk.fill_ids_sha256(fills),
            instrument_id=intent.instrument,
            instrument_version="fixture-instrument-v1",
            instrument_multiplier=Decimal("1"),
            quote_currency="USD",
            valuation_unit="USD",
            instrument_metadata_sha256=_digest(f"metadata:{intent.instrument}"),
            instrument_metadata_observed_at_ns=_NOW_NS - 1_000_000,
            instrument_metadata_valid_until_ns=_DEADLINE_NS,
            valuation_facts_sha256=_digest(f"valuation:{intent.instrument}"),
            valuation_observed_at_ns=_NOW_NS - 1_000_000,
            valuation_valid_until_ns=_DEADLINE_NS,
            fx_pair=None,
            fx_rate=None,
            fx_observed_at_ns=None,
            fx_valid_until_ns=None,
            observed_at_ns=now_ns,
            valid_until_ns=_DEADLINE_NS,
            writer_owner_id="synthetic-account-owner",
            writer_fencing_token=1,
            writer_fence_sha256=_digest("synthetic-writer-fence-v1"),
        )
        return risk.DispatchExposureResolutionProof(
            claim=risk_claim.claim,
            evidence_class=risk.DispatchEvidenceClass.SIMULATION_JOURNAL,
            dispatch_attempt_count=1,
            reconciliation_evidence_sha256=_digest(f"synthetic-reconciliation:{intent.intent_id}"),
            observation=observation,
        )


class _GroupExecutionAuthority:
    """Trusted synthetic authority guard used by the real service-path test."""

    @contextmanager
    def hold_group_leg_authorization(
        self,
        *,
        principal_ref,
        identity,
        wire_scope,
        execution_scope,
        intent,
        binding,
        writer_lease,
    ):
        assert identity.principal_ref == principal_ref
        assert wire_scope.group_id == binding.group_id
        binding.validate_order(intent)
        yield GroupLegAuthorization(
            authorization_attempt_id=f"auth-{intent.intent_id}",
            principal_ref=principal_ref,
            portfolio_ref=binding.portfolio_ref,
            strategy_id=binding.strategy_id,
            account_ref=binding.account_ref,
            group_id=binding.group_id,
            leg_id=binding.leg_id,
            coordinator_epoch=1,
            group_fingerprint=binding.group_fingerprint,
            binding_fingerprint=binding.fingerprint,
            deadline_ns=binding.deadline_ns,
            account_writer_epoch=1,
            risk_scope_fingerprint=binding.risk_scope_fingerprint,
            execution_writer_lease_fingerprint=writer_lease_fingerprint(writer_lease),
            action="orders.submit_group_leg",
        )

    def assert_current_group_leg_authorization(
        self,
        *,
        principal_ref,
        identity,
        wire_scope,
        execution_scope,
        intent,
        binding,
        authorization,
        writer_lease,
    ):
        assert principal_ref == identity.principal_ref == authorization.principal_ref
        assert wire_scope.group_id == binding.group_id == authorization.group_id
        assert execution_scope == intent.scope
        authorization.validate_binding(binding)
        authorization.validate_writer_lease(writer_lease)


class _ExecutionProvider:
    def __init__(self) -> None:
        self.submitted: list[str] = []

    def submit(self, intent: OrderIntent) -> ProviderObservation:
        self.submitted.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, "synthetic-provider-order")

    def cancel(self, intent):
        raise AssertionError("group-submit integration test must not cancel")


def _instrument_registry(risk):
    records = {}
    for instrument in ("FOO-USD", "BAR-USD"):
        records[instrument] = risk.InstrumentRiskMetadata(
            instrument=instrument,
            metadata_version="instrument-v1",
            as_of_ns=_NOW_NS - 1_000_000,
            expires_at_ns=_DEADLINE_NS,
            tick_size=Decimal("0.01"),
            quantity_step=Decimal("0.25"),
            contract_multiplier=Decimal("1"),
            max_gross_notional=Decimal("1000000"),
            taker_fee_bps=Decimal("10"),
            fixed_fee=Decimal("0.1"),
            max_slippage_bps=Decimal("100"),
            quantity_unit="contract",
            valuation_unit="USD",
        )
    return records, risk.InstrumentRiskRegistry(records.values())


def _request(records) -> GroupOrderIntent:
    scopes = (
        ExecutionScope("fixture", "sandbox", _WIRE_ACCOUNT, "strategy-p5a"),
        ExecutionScope("fixture", "sandbox", _WIRE_ACCOUNT, "strategy-p5a"),
    )
    first = OrderIntent(
        intent_id=group_leg_intent_id("risk-backed-group", "leg-first"),
        scope=scopes[0],
        signal_id="synthetic-signal",
        instrument="FOO-USD",
        side=Side.BUY,
        position_effect=PositionEffect.OPEN,
        order_type=OrderType.LIMIT,
        quantity=Decimal("2"),
        price=Decimal("10"),
        metadata_version=records["FOO-USD"].metadata_version,
        time_in_force=TimeInForce.IOC,
        deadline_ns=_DEADLINE_NS,
        tags={"instrument_metadata_digest": records["FOO-USD"].digest},
    )
    second = OrderIntent(
        intent_id=group_leg_intent_id("risk-backed-group", "leg-hedge"),
        scope=scopes[1],
        signal_id="synthetic-signal",
        instrument="BAR-USD",
        side=Side.SELL,
        position_effect=PositionEffect.OPEN,
        order_type=OrderType.LIMIT,
        quantity=Decimal("1.5"),
        price=Decimal("20"),
        metadata_version=records["BAR-USD"].metadata_version,
        time_in_force=TimeInForce.IOC,
        deadline_ns=_DEADLINE_NS,
        tags={"instrument_metadata_digest": records["BAR-USD"].digest},
    )
    return GroupOrderIntent(
        group_id="risk-backed-group",
        portfolio_ref="portfolio-p5a",
        strategy_id="strategy-p5a",
        coordinator_principal_ref="coordinator-p5a",
        received_at_ns=_NOW_NS,
        deadline_ns=_DEADLINE_NS,
        first_leg_id="leg-first",
        legs=(
            GroupOrderLegPlan(
                "leg-first", _WIRE_ACCOUNT, first, Decimal("1"), Decimal("0.25"), _DEADLINE_NS
            ),
            GroupOrderLegPlan(
                "leg-hedge", _WIRE_ACCOUNT, second, Decimal("2"), Decimal("0.25"), _DEADLINE_NS
            ),
        ),
    )


def test_real_risk_group_submit_uses_account_scope_through_order_entry(tmp_path) -> None:
    """Exercise the actual facade call path, not a coordinator-side Risk call."""

    risk = pytest.importorskip("bt_api_risk")
    records, _ = _instrument_registry(risk)
    trusted_scope = risk.AccountScope("fixture", "mapped-risk-account-a", "sandbox")
    mapper = _RiskIntentMapper(risk, trusted_scope)
    resolution_authority = _SyntheticExposureAuthority(risk)
    gate = risk.DurableRiskGate(
        tmp_path / "service-path-risk.sqlite3",
        risk.RiskPolicy(
            "p5a-service-path",
            Decimal("1000"),
            8,
            permit_ttl_seconds=30,
            require_strategy_allocation=True,
            notional_unit="USD",
        ),
        clock=lambda: _NOW_NS / 1_000_000_000,
        execution_journal_authority=resolution_authority,
    )
    gate.set_strategy_allocation(
        trusted_scope,
        "strategy-p5a",
        "allocation-v1",
        max_notional=Decimal("1000"),
        notional_unit="USD",
    )
    adapter = risk.DurableRiskAdmissionAdapter(gate, mapper)
    request = _request(records)
    first_plan = request.ordered_legs[0]
    reservation = adapter.reserve_group_vector(
        trusted_scope,
        request.group_id,
        tuple((leg.leg_id, leg.maximum_intent) for leg in request.ordered_legs),
        deadline_ns=request.deadline_ns,
    )
    reserved_leg = next(leg for leg in reservation.legs if leg.leg_id == first_plan.leg_id)
    actual_risk_intent = mapper.map_group_intent(first_plan.maximum_intent)
    binding = GroupPermitBinding(
        group_id=request.group_id,
        leg_id=first_plan.leg_id,
        portfolio_ref=request.portfolio_ref,
        strategy_id=request.strategy_id,
        account_ref=first_plan.account_ref,
        coordinator_principal_ref=request.coordinator_principal_ref,
        risk_scope_fingerprint=risk_account_scope_fingerprint(trusted_scope.key),
        permit_id=reserved_leg.permit_id,
        group_fingerprint=reservation.group_fingerprint,
        policy_fingerprint=reservation.policy_fingerprint,
        reserved_intent_hash=reserved_leg.reserved_intent_hash,
        exposure_profile_fingerprint=reserved_leg.exposure_profile_fingerprint,
        actual_child_intent_hash=actual_risk_intent.fingerprint,
        order_intent_hash=first_plan.maximum_intent.fingerprint,
        deadline_ns=request.deadline_ns,
    )

    base_scope = WireScope(
        execution_domain_ref="fixture-sandbox",
        account_ref=_WIRE_ACCOUNT,
        strategy_ref=request.strategy_id,
    )
    group_base_scope = WireScope(
        execution_domain_ref="fixture-sandbox",
        account_ref=_WIRE_ACCOUNT,
        strategy_ref=request.strategy_id,
        portfolio_ref=request.portfolio_ref,
    )
    group_scope = WireScope(
        execution_domain_ref="fixture-sandbox",
        account_ref=_WIRE_ACCOUNT,
        strategy_ref=request.strategy_id,
        portfolio_ref=request.portfolio_ref,
        group_id=request.group_id,
        leg_id=first_plan.leg_id,
    )
    identity = WireIdentity(
        WireRole.SERVICE,
        request.coordinator_principal_ref,
        "synthetic-group-owner",
    )
    provider = _ExecutionProvider()
    execution_store = SqliteExecutionStore(tmp_path / "service-path-execution.sqlite3")
    service = ExecutionOrderEntryService(
        execution_store,
        {
            base_scope: first_plan.maximum_intent.scope,
            group_base_scope: first_plan.maximum_intent.scope,
        },
        provider,
        admission_gate=AccountScopedGroupAdmissionAdapter(adapter, account_scope=trusted_scope),
        group_coordinator_authority=_GroupExecutionAuthority(),
    )

    result = service.submit_group_leg(
        principal_ref=request.coordinator_principal_ref,
        identity=identity,
        scope=group_scope,
        intent=first_plan.maximum_intent,
        binding=binding,
    )

    assert result["ok"] is True
    assert result["result"]["record"]["state"] == ExecutionState.ACKED.value
    assert provider.submitted == [first_plan.maximum_intent.intent_id]
    settled_group = adapter.query_group_vector(trusted_scope, request.group_id)
    settled_leg = next(leg for leg in settled_group.legs if leg.leg_id == first_plan.leg_id)
    assert settled_leg.claimed and not settled_leg.resolved
    assert settled_leg.actual_intent_hash == actual_risk_intent.fingerprint
    assert settled_leg.state.name == "SETTLED"

    execution_store.close()
    gate.close()


def test_real_risk_vector_partial_fill_resolution_then_hedge(tmp_path) -> None:
    risk = pytest.importorskip("bt_api_risk")
    records, instrument_registry = _instrument_registry(risk)
    scope = risk.AccountScope("fixture", "mapped-risk-account-a", "sandbox")
    mapper = _RiskIntentMapper(risk, scope)
    authority = _SyntheticExposureAuthority(risk)
    gate = risk.DurableRiskGate(
        tmp_path / "real-risk.sqlite3",
        risk.RiskPolicy(
            "p5a-integration",
            Decimal("1000"),
            8,
            permit_ttl_seconds=30,
            require_strategy_allocation=True,
            notional_unit="USD",
        ),
        clock=lambda: _NOW_NS / 1_000_000_000,
        execution_journal_authority=authority,
    )
    gate.set_strategy_allocation(
        scope,
        "strategy-p5a",
        "allocation-v1",
        max_notional=Decimal("1000"),
        notional_unit="USD",
    )
    adapter = risk.DurableRiskAdmissionAdapter(gate, mapper)
    reservations = _RiskReservationOwner(_WIRE_ACCOUNT, scope, adapter)
    orders = _RiskBackedOrdersOwner(risk, scope, adapter, reservations)
    request = _request(records)
    store = SqliteOrderGroupStore(tmp_path / "groups.sqlite")
    budget_policy = PortfolioGroupBudgetPolicy(
        portfolio_ref=request.portfolio_ref,
        policy_id="synthetic-p5a-budget",
        policy_version=1,
        valuation_unit="USD",
        max_group_gross_notional=Decimal("1000"),
        max_active_gross_notional=Decimal("1000"),
        max_active_groups=3,
    )
    coordinator = GroupOrderCoordinator(
        store,
        reservations={_WIRE_ACCOUNT: reservations},
        orders={_WIRE_ACCOUNT: orders},
        portfolio_budget_policies={request.portfolio_ref: budget_policy},
        budget_quote_builder=lambda group, policy, *, now_ns: (
            build_instrument_risk_group_budget_quote(
                group, policy, instrument_registry, now_ns=now_ns
            )
        ),
        clock_ns=lambda: _NOW_NS + 1,
    )

    coordinator.accept(request)
    result = coordinator.advance_group(request.portfolio_ref, request.group_id)

    assert result.state is GroupOrderState.PAIRED, orders.resolution_errors
    assert [intent.quantity for intent in orders.submissions] == [
        Decimal("2"),
        Decimal("0.75"),
    ]
    vector = adapter.query_group_vector(scope, request.group_id)
    assert [leg.state.name for leg in vector.legs] == ["SETTLED", "SETTLED"]
    assert all(leg.claimed and leg.resolved for leg in vector.legs)
    assert all(leg.actual_intent_hash is not None for leg in vector.legs)
    assert all(
        not adapter.dispatch_exposure_resolution_receipt(leg.permit_id).dispatch_latch_active
        for leg in vector.legs
    )
    assert (
        vector.legs[0].actual_intent_hash
        == orders.risk_claims[request.ordered_legs[0].maximum_intent.intent_id].actual_intent_hash
    )
    gate.close()
    store.close()


def test_real_risk_unknown_response_recovers_original_intent_without_reclaim(tmp_path) -> None:
    risk = pytest.importorskip("bt_api_risk")
    records, instrument_registry = _instrument_registry(risk)
    scope = risk.AccountScope("fixture", "mapped-risk-account-a", "sandbox")
    mapper = _RiskIntentMapper(risk, scope)
    authority = _SyntheticExposureAuthority(risk)
    gate = risk.DurableRiskGate(
        tmp_path / "real-risk-recovery.sqlite3",
        risk.RiskPolicy(
            "p5a-recovery",
            Decimal("1000"),
            8,
            permit_ttl_seconds=30,
            require_strategy_allocation=True,
            notional_unit="USD",
        ),
        clock=lambda: _NOW_NS / 1_000_000_000,
        execution_journal_authority=authority,
    )
    gate.set_strategy_allocation(
        scope,
        "strategy-p5a",
        "allocation-v1",
        max_notional=Decimal("1000"),
        notional_unit="USD",
    )
    adapter = risk.DurableRiskAdmissionAdapter(gate, mapper)
    reservations = _RiskReservationOwner(_WIRE_ACCOUNT, scope, adapter)
    orders = _RiskBackedOrdersOwner(risk, scope, adapter, reservations, lose_first_response=True)
    request = _request(records)
    store_path = tmp_path / "groups-recovery.sqlite"
    store = SqliteOrderGroupStore(store_path)
    budget_policy = PortfolioGroupBudgetPolicy(
        portfolio_ref=request.portfolio_ref,
        policy_id="synthetic-p5a-budget-recovery",
        policy_version=1,
        valuation_unit="USD",
        max_group_gross_notional=Decimal("1000"),
        max_active_gross_notional=Decimal("1000"),
        max_active_groups=3,
    )
    coordinator = GroupOrderCoordinator(
        store,
        reservations={_WIRE_ACCOUNT: reservations},
        orders={_WIRE_ACCOUNT: orders},
        portfolio_budget_policies={request.portfolio_ref: budget_policy},
        budget_quote_builder=lambda group, policy, *, now_ns: (
            build_instrument_risk_group_budget_quote(
                group, policy, instrument_registry, now_ns=now_ns
            )
        ),
        clock_ns=lambda: _NOW_NS + 1,
    )

    coordinator.accept(request)
    interrupted = coordinator.advance_group(request.portfolio_ref, request.group_id)
    assert interrupted.state is GroupOrderState.RECOVERY_REQUIRED
    first_id = request.ordered_legs[0].maximum_intent.intent_id
    original_claim = orders.risk_claims[first_id]
    assert len(orders.submissions) == 1
    in_flight = adapter.query_group_vector(scope, request.group_id).legs[0]
    assert in_flight.claimed and not in_flight.resolved

    store.close()
    reopened_store = SqliteOrderGroupStore(store_path)
    recovered = GroupOrderCoordinator(
        reopened_store,
        reservations={_WIRE_ACCOUNT: reservations},
        orders={_WIRE_ACCOUNT: orders},
        portfolio_budget_policies={request.portfolio_ref: budget_policy},
        budget_quote_builder=lambda group, policy, *, now_ns: (
            build_instrument_risk_group_budget_quote(
                group, policy, instrument_registry, now_ns=now_ns
            )
        ),
        clock_ns=lambda: _NOW_NS + 2,
    )
    result = recovered.advance_group(request.portfolio_ref, request.group_id)

    assert result.state is GroupOrderState.PAIRED, orders.resolution_errors
    assert len(orders.submissions) == 2
    assert orders.submissions.count(request.ordered_legs[0].maximum_intent) == 1
    assert first_id in orders.query_ids
    assert orders.risk_claims[first_id] is original_claim
    vector = adapter.query_group_vector(scope, request.group_id)
    assert all(leg.claimed and leg.resolved for leg in vector.legs)
    gate.close()
    reopened_store.close()
