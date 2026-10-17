"""SQLite regressions for the typed, whole-group roll-exit close proof."""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from bt_api_execution import (
    ExecutionScope,
    ExecutionState,
    GroupAccountReservationSnapshot,
    GroupBudgetLegQuote,
    GroupBudgetQuote,
    GroupChildOrderStatus,
    GroupDispatchResolutionReceipt,
    GroupLegClaimReceipt,
    GroupLegOrderReceipt,
    GroupLegPermitReference,
    GroupOrderCoordinator,
    GroupOrderIntent,
    GroupOrderLegPlan,
    GroupOrderState,
    GroupReservationPhase,
    GroupRiskLegSnapshot,
    OrderIntent,
    OrderType,
    PortfolioGroupBudgetPolicy,
    PositionEffect,
    Side,
    SqliteOrderGroupStore,
    TimeInForce,
    group_leg_intent_id,
    payload_sha256,
)
from bt_api_execution.errors import ContractValidationError, IntentConflictError
from bt_api_execution.group_order_contracts import GroupBudgetReservationState
from bt_api_execution.group_roll_exit_contracts import (
    GroupRollExitBudgetReleaseV1,
    GroupRollExitCloseActionV1,
    GroupRollExitCloseFeeEvidenceV1,
    GroupRollExitCloseFeeFillV1,
    GroupRollExitClosedProofV1,
    GroupRollExitLegClosedV1,
    GroupRollExitRiskCloseClaimV1,
    GroupRollExitRiskCloseReceiptV1,
    GroupRollExitSourceFeeEvidenceV1,
)
from bt_api_execution.group_roll_exit_validation import (
    _risk_close_fee_from_payload,
    _validate_released_reservations,
    group_event_chain_sha256,
)


_NOW_NS = 1_900_000_000_000_000_000
_DEADLINE_NS = _NOW_NS + 60_000_000_000
_ACCOUNT = "account-roll-01"
_STRATEGY = "strategy-roll-01"
_RUN = "run-roll-01"
_PRINCIPAL = "principal-roll-01"
_INSTANCE = "instance-roll-01"
_SCOPE_REF = "scope-roll-01"
_REGISTRATION = "registration-roll-01"
_METADATA_VERSION = "metadata-roll-v1"
_METADATA_DIGEST = payload_sha256(
    {"instrument": "BTC-USD", "quantity_step": "0.25", "tick_size": "0.01"}
)


def _missing_group_error() -> LookupError:
    error = LookupError("group reservation does not exist")
    error.code = "GROUP_NOT_FOUND"  # type: ignore[attr-defined]
    return error


@dataclass(frozen=True)
class _RiskScope:
    key: str


class _Mapper:
    def __init__(self, scope: _RiskScope) -> None:
        self.scope = scope

    def map_group_intent(self, intent: OrderIntent) -> Any:
        return SimpleNamespace(
            scope=self.scope,
            intent_id=intent.intent_id,
            fingerprint=payload_sha256({"intent": intent.to_payload(), "risk": "mapped"}),
            exposure_profile_fingerprint=payload_sha256(
                {"instrument": intent.instrument, "side": intent.side.value, "effect": "OPEN"}
            ),
            quantity=intent.quantity,
            strategy_id=intent.scope.strategy_id,
            instrument=intent.instrument,
        )


class _ReservationPort:
    """A deterministic owner port that keeps the real coordinator/store path."""

    def __init__(
        self, scope: ExecutionScope, *, group_fingerprint: str, policy_fingerprint: str
    ) -> None:
        self.account_ref = _ACCOUNT
        self.scope = _RiskScope(
            f'{{"account_id":"risk-{self.account_ref}","environment":"demo","provider":"paper"}}'
        )
        self.mapper = _Mapper(self.scope)
        self.risk_scope_fingerprint = payload_sha256({"scope_key": self.scope.key})
        self.group_fingerprint = group_fingerprint
        self.policy_fingerprint = policy_fingerprint
        self.groups: dict[str, Any] = {}

    def query_group_vector(self, group_id: str) -> GroupAccountReservationSnapshot:
        if group_id not in self.groups:
            raise _missing_group_error()
        return self._receipt(group_id)

    def reserve_group_vector(
        self, group_id: str, members: tuple[tuple[str, OrderIntent], ...], *, deadline_ns: int
    ) -> GroupAccountReservationSnapshot:
        if group_id in self.groups:
            raise RuntimeError("test reservation already exists")
        legs: dict[str, dict[str, Any]] = {}
        for leg_id, intent in sorted(members):
            mapped = self.mapper.map_group_intent(intent)
            legs[leg_id] = {
                "permit_id": f"permit-{group_id}-{leg_id}",
                "intent_id": intent.intent_id,
                "reserved_intent_hash": mapped.fingerprint,
                "exposure_profile_fingerprint": mapped.exposure_profile_fingerprint,
                "actual_intent_hash": None,
                "state": "ACTIVE",
                "claimed": False,
                "resolved": False,
                "expires_at_ns": deadline_ns - 1,
            }
        self.groups[group_id] = SimpleNamespace(
            scope_fingerprint=self.risk_scope_fingerprint,
            group_id=group_id,
            account_ref=self.account_ref,
            group_fingerprint=self.group_fingerprint,
            policy_fingerprint=self.policy_fingerprint,
            deadline_ns=deadline_ns,
            legs=legs,
        )

        return self._receipt(group_id)

    def set_claim(
        self, group_id: str, leg_id: str, intent: OrderIntent, claim: GroupLegClaimReceipt
    ) -> None:
        leg = self.groups[group_id].legs[leg_id]
        mapped = self.mapper.map_group_intent(intent)
        if mapped.fingerprint != claim.actual_child_intent_hash:
            raise RuntimeError("test claim does not match the mapped Risk intent")
        leg.update(
            actual_intent_hash=mapped.fingerprint,
            claimed=True,
            resolved=False,
            state="SETTLED",
        )

    def resolve_claim(self, claim: GroupLegClaimReceipt) -> None:
        self.groups[claim.permit.group_id].legs[claim.permit.leg_id]["resolved"] = True

    def release_group_unclaimed(
        self, group_id: str, reason: str
    ) -> GroupAccountReservationSnapshot:
        for leg in self.groups[group_id].legs.values():
            if not leg["claimed"]:
                leg["state"] = "RELEASED"
        return self._receipt(group_id)

    def _receipt(self, group_id: str) -> GroupAccountReservationSnapshot:
        receipt = self.groups[group_id]
        return GroupAccountReservationSnapshot(
            group_id=receipt.group_id,
            account_ref=receipt.account_ref,
            scope_fingerprint=receipt.scope_fingerprint,
            phase=GroupReservationPhase.ACTIVE,
            group_fingerprint=receipt.group_fingerprint,
            policy_fingerprint=receipt.policy_fingerprint,
            deadline_ns=receipt.deadline_ns,
            legs=tuple(
                GroupRiskLegSnapshot(leg_id=leg_id, **value)
                for leg_id, value in sorted(receipt.legs.items())
            ),
        )


class _OrdersPort:
    def __init__(
        self, reservations: _ReservationPort, *, partial_open_leg_id: str | None = None
    ) -> None:
        self._reservations = reservations
        self._partial_open_leg_id = partial_open_leg_id
        self._receipts: dict[str, GroupLegOrderReceipt] = {}

    def query(self, intent_id: str) -> GroupLegOrderReceipt | None:
        return self._receipts.get(intent_id)

    def submit_group_leg(self, intent: OrderIntent, permit: Any) -> GroupLegOrderReceipt:
        permit.validate_order(intent)
        mapped = self._reservations.mapper.map_group_intent(intent)
        claim = GroupLegClaimReceipt(
            permit=permit,
            actual_child_intent_hash=mapped.fingerprint,
            claim_digest=payload_sha256({"claim": permit.permit_id, "order": intent.fingerprint}),
            actual_quantity=intent.quantity,
            actual_notional=Decimal("1"),
        )
        self._reservations.set_claim(permit.group_id, permit.leg_id, intent, claim)
        partial_open = permit.leg_id == self._partial_open_leg_id
        filled_quantity = intent.quantity * Decimal("0.75") if partial_open else intent.quantity
        status = GroupChildOrderStatus(
            intent_id=intent.intent_id,
            state=ExecutionState.CANCELLED if partial_open else ExecutionState.FILLED,
            filled_quantity=filled_quantity,
            provider_order_id=f"order-{permit.leg_id}",
            average_price=intent.price,
            cumulative_commission=Decimal("0.20"),
            cumulative_commission_currency="USD",
            order_revision=1,
            trade_count=1,
        )
        resolution = GroupDispatchResolutionReceipt(
            claim=claim,
            resolution_kind="TERMINAL_EXPOSURE_RESOLVED",
            proof_sha256=payload_sha256(
                {"kind": "synthetic-terminal-open-proof", "intent_id": intent.intent_id}
            ),
            final_state=status.state,
            requested_quantity=intent.quantity,
            filled_quantity=status.filled_quantity,
            trade_count=1,
            order_revision=1,
            journal_revision=2,
            exposure_proof_sha256=payload_sha256(
                {"kind": "synthetic-open-exposure-proof", "intent_id": intent.intent_id}
            ),
            exposure_reservation_id=f"open-exposure-{permit.leg_id}",
            exposure_reservation_sha256=payload_sha256(
                {"kind": "synthetic-open-reservation", "intent_id": intent.intent_id}
            ),
            dispatch_latch_active=False,
            has_other_active_freezes=False,
        )
        self._reservations.resolve_claim(claim)
        receipt = GroupLegOrderReceipt(status=status, claim=claim, resolution=resolution)
        self._receipts[intent.intent_id] = receipt
        return receipt

    def resolve_group_leg(
        self,
        intent: OrderIntent,
        permit: GroupLegPermitReference,
        claim: GroupLegClaimReceipt,
        status: GroupChildOrderStatus,
    ) -> GroupDispatchResolutionReceipt:
        if (
            intent.intent_id != permit.intent_id
            or status.intent_id != intent.intent_id
            or claim.permit != permit
        ):
            raise RuntimeError("test resolution request does not match its order binding")
        receipt = self._receipts.get(intent.intent_id)
        if receipt is None or receipt.claim != claim or receipt.resolution is None:
            raise RuntimeError("test leg has no terminal resolution")
        return receipt.resolution


@dataclass(frozen=True)
class _RollAction:
    leg_id: str
    source: dict[str, Any]
    source_event_sha256: str
    source_status: GroupChildOrderStatus
    source_claim: Any
    source_resolution: GroupDispatchResolutionReceipt
    request: dict[str, Any]
    intent: OrderIntent
    action_id: str
    risk_intent_id: str
    authorization: dict[str, Any]
    authorization_sha256: str
    risk_receipt: GroupRollExitRiskCloseReceiptV1
    fee_evidence: GroupRollExitCloseFeeEvidenceV1
    risk_fee_wire: dict[str, Any]
    close_state: str = "FILLED"


@dataclass
class _Scenario:
    database: Path
    store: SqliteOrderGroupStore
    snapshot: Any
    proof: GroupRollExitClosedProofV1
    actions: tuple[_RollAction, ...]


def _make_request() -> GroupOrderIntent:
    scope = ExecutionScope(
        provider="paper",
        environment="test",
        account_ref=_ACCOUNT,
        strategy_id=_STRATEGY,
    )

    def intent(leg_id: str, instrument: str, side: Side, price: str) -> OrderIntent:
        return OrderIntent(
            intent_id=group_leg_intent_id("group-roll-01", leg_id),
            signal_id=f"signal-{leg_id}",
            scope=scope,
            instrument=instrument,
            side=side,
            position_effect=PositionEffect.OPEN,
            order_type=OrderType.LIMIT,
            quantity=Decimal("2"),
            price=Decimal(price),
            time_in_force=TimeInForce.IOC,
            deadline_ns=_DEADLINE_NS,
            metadata_version=_METADATA_VERSION,
            tags={"instrument_metadata_digest": _METADATA_DIGEST},
        )

    return GroupOrderIntent(
        group_id="group-roll-01",
        portfolio_ref="portfolio-roll-01",
        strategy_id=_STRATEGY,
        coordinator_principal_ref=_PRINCIPAL,
        received_at_ns=_NOW_NS,
        deadline_ns=_DEADLINE_NS,
        first_leg_id="leg-long",
        legs=(
            GroupOrderLegPlan(
                leg_id="leg-long",
                account_ref=_ACCOUNT,
                maximum_intent=intent("leg-long", "BTC-USD", Side.BUY, "100"),
                base_units_per_order_unit=Decimal("1"),
                quantity_step=Decimal("0.25"),
                metadata_valid_until_ns=_DEADLINE_NS + 1_000_000_000,
            ),
            GroupOrderLegPlan(
                leg_id="leg-short",
                account_ref=_ACCOUNT,
                maximum_intent=intent("leg-short", "ETH-USD", Side.SELL, "100"),
                base_units_per_order_unit=Decimal("1"),
                quantity_step=Decimal("0.25"),
                metadata_valid_until_ns=_DEADLINE_NS + 1_000_000_000,
            ),
        ),
    )


def _budget_policy() -> PortfolioGroupBudgetPolicy:
    return PortfolioGroupBudgetPolicy(
        portfolio_ref="portfolio-roll-01",
        policy_id="roll-exit-policy",
        policy_version=1,
        valuation_unit="USD",
        max_group_gross_notional=Decimal("100000"),
        max_active_gross_notional=Decimal("100000"),
        max_active_groups=10,
    )


def _budget_quote(
    request: GroupOrderIntent,
    policy: PortfolioGroupBudgetPolicy,
    *,
    now_ns: int,
) -> GroupBudgetQuote:
    legs = []
    for plan in request.legs:
        intent = plan.maximum_intent
        gross = intent.quantity * (intent.price or Decimal("0")) + Decimal("0.5")
        legs.append(
            GroupBudgetLegQuote(
                leg_id=plan.leg_id,
                account_ref=plan.account_ref,
                intent_fingerprint=intent.fingerprint,
                metadata_digest=str(intent.tags["instrument_metadata_digest"]),
                assessment_fingerprint=payload_sha256(
                    {"leg": plan.leg_id, "intent": intent.fingerprint, "gross": format(gross, "f")}
                ),
                valuation_unit="USD",
                gross_notional=gross,
                assessed_at_ns=now_ns,
                valid_until_ns=plan.metadata_valid_until_ns,
            )
        )
    return GroupBudgetQuote(
        policy_fingerprint=policy.fingerprint,
        valuation_unit=policy.valuation_unit,
        gross_notional=sum((leg.gross_notional for leg in legs), Decimal("0")),
        legs=tuple(legs),
    )


def _paired_group(
    database: Path,
    *,
    partial_open_leg_id: str | None = None,
    risk_policy_fingerprint: str | None = None,
) -> tuple[SqliteOrderGroupStore, Any]:
    request = _make_request()
    scope = request.legs[0].maximum_intent.scope
    policy = _budget_policy()
    accepted_request = replace(
        request,
        version=2,
        budget_quote=_budget_quote(request, policy, now_ns=_NOW_NS + 1),
    )
    risk_group_fingerprint = payload_sha256(
        {"accepted_execution_group": accepted_request.fingerprint, "risk": "mapped"}
    )
    assert risk_group_fingerprint != accepted_request.fingerprint
    reservations = _ReservationPort(
        scope,
        group_fingerprint=risk_group_fingerprint,
        policy_fingerprint=risk_policy_fingerprint or policy.fingerprint,
    )
    orders = _OrdersPort(reservations, partial_open_leg_id=partial_open_leg_id)
    store = SqliteOrderGroupStore(database)
    coordinator = GroupOrderCoordinator(
        store,
        reservations={_ACCOUNT: reservations},
        orders={_ACCOUNT: orders},
        portfolio_budget_policies={request.portfolio_ref: policy},
        budget_quote_builder=_budget_quote,
        clock_ns=lambda: _NOW_NS + 1,
    )
    coordinator.accept(request)
    paired = coordinator.advance_group(request.portfolio_ref, request.group_id)
    assert paired.state is GroupOrderState.PAIRED
    assert paired.request.version == 2
    assert paired.residual_base_units == Decimal("0")
    assert paired.budget_reservation.state is GroupBudgetReservationState.HELD
    assert len(paired.legs) == 2
    for leg in paired.legs:
        assert leg.submitted_intent is not None
        assert leg.child_status is not None
        assert leg.child_status.state in {ExecutionState.CANCELLED, ExecutionState.FILLED}
        assert leg.child_status.filled_quantity <= leg.submitted_intent.quantity
        assert leg.child_status.cumulative_commission == Decimal("0.20")
        assert leg.claim_receipt is not None
        assert leg.resolution_receipt is not None
        assert leg.resolution_receipt.final_state is leg.child_status.state
        assert leg.resolution_receipt.requested_quantity == leg.submitted_intent.quantity
        assert leg.resolution_receipt.filled_quantity == leg.child_status.filled_quantity
    return store, paired


def _synthetic_source_event_digest(store: SqliteOrderGroupStore, leg_id: str) -> str:
    """Return a test-only digest placeholder, not an execution proof-journal digest."""

    event = next(
        event
        for event in store.events("portfolio-roll-01", "group-roll-01")
        if event.event_type == "GROUP_DISPATCH_RESOLUTION_RECORDED"
        and event.payload.get("leg_id") == leg_id
    )
    return payload_sha256(
        {
            "group_id": event.group_id,
            "event_type": event.event_type,
            "payload": dict(event.payload),
            "created_at_ns": event.created_at_ns,
        }
    )


def _roll_action(
    snapshot: Any, store: SqliteOrderGroupStore, leg_id: str
) -> _RollAction:
    leg = next(item for item in snapshot.legs if item.leg_id == leg_id)
    plan = snapshot.request.leg(leg_id)
    assert (
        leg.submitted_intent is not None
        and leg.child_status is not None
        and leg.claim_receipt is not None
        and leg.resolution_receipt is not None
    )
    # This E-store unit fixture has no external execution proof journal. The digest
    # is deliberately synthetic and must not be read as production source evidence.
    source_event_sha256 = _synthetic_source_event_digest(store, leg_id)
    source_intent = leg.submitted_intent
    assert source_intent.scope == plan.maximum_intent.scope
    assert source_intent.instrument == plan.maximum_intent.instrument
    assert source_intent.side is plan.maximum_intent.side
    assert source_intent.position_effect is PositionEffect.OPEN
    assert source_intent.quantity <= plan.maximum_intent.quantity
    assert source_intent.quantity % plan.quantity_step == 0
    assert leg.resolution_receipt.requested_quantity == source_intent.quantity
    assert leg.resolution_receipt.filled_quantity == leg.child_status.filled_quantity
    account_reservation = next(
        item for item in snapshot.reservations if item.account_ref == plan.account_ref
    )
    risk_leg = next(item for item in account_reservation.legs if item.leg_id == leg_id)
    assert account_reservation.group_fingerprint != snapshot.request.fingerprint
    assert risk_leg.actual_intent_hash != source_intent.fingerprint
    assert leg.claim_receipt.actual_child_intent_hash == risk_leg.actual_intent_hash
    assert leg.claim_receipt.permit.order_intent_hash == source_intent.fingerprint
    request_payload = {
        "schema": "group-roll-exit-request-v1",
        "portfolio_ref": snapshot.request.portfolio_ref,
        "group_id": snapshot.request.group_id,
        "source_leg_id": leg_id,
        "limit_price": "101.00",
    }
    source = {
        "schema": "group-roll-exit-source-facts-v1",
        "portfolio_ref": snapshot.request.portfolio_ref,
        "origin_run_id": _RUN,
        "group_id": snapshot.request.group_id,
        "group_version": snapshot.request.version,
        "group_revision": snapshot.revision,
        "group_fingerprint": snapshot.request.fingerprint,
        "source_risk_reservation_fingerprint": account_reservation.group_fingerprint,
        "source_risk_intent_fingerprint": risk_leg.actual_intent_hash,
        "group_request": snapshot.request.to_payload(),
        "account_ref": plan.account_ref,
        "strategy_id": snapshot.request.strategy_id,
        "source_leg_id": leg_id,
        "source_intent_id": leg.child_status.intent_id,
        "source_request_fingerprint": source_intent.fingerprint,
        "source_child_intent": source_intent.to_payload(),
        "source_execution_scope": source_intent.scope.to_payload(),
        "source_terminal_status": leg.child_status.to_payload(),
        "source_requested_quantity": format(plan.maximum_intent.quantity, "f"),
        "source_claim": leg.claim_receipt.to_payload(),
        "source_risk_resolution": leg.resolution_receipt.to_payload(),
        "source_event_digest": source_event_sha256,
        "source_side": source_intent.side.value,
        "instrument": source_intent.instrument,
        "source_metadata_version": source_intent.metadata_version,
        "close_metadata_version": _METADATA_VERSION,
        "close_metadata_digest": _METADATA_DIGEST,
        "close_quantity_step": "0.25",
        "close_tick_size": "0.01",
        "close_metadata_valid_until_ns": _DEADLINE_NS + 1_000_000_000,
        "source_position_side": source_intent.position_side,
        "source_position_bucket": source_intent.position_bucket,
        "source_filled_quantity": format(leg.child_status.filled_quantity, "f"),
        "prior_resolved_closes": [],
        "remaining_quantity": format(leg.child_status.filled_quantity, "f"),
        "closable_position_version": payload_sha256(
            {
                "account_ref": plan.account_ref,
                "instrument": source_intent.instrument,
                "position_side": source_intent.position_side,
                "position_bucket": source_intent.position_bucket,
                "revision": 1,
            }
        ),
        "service_principal_ref": _PRINCIPAL,
        "service_instance_id": _INSTANCE,
        "service_scope_ref": _SCOPE_REF,
        "service_registration_id": _REGISTRATION,
        "service_operation": "orders.submit_group_roll_exit",
        "service_port": "service.order_entry",
        "service_registration_digest": payload_sha256(
            {"registration": _REGISTRATION, "scope": _RUN}
        ),
        "service_epoch": "epoch-roll-01",
        "service_policy_version": "policy-roll-01",
        "service_requirements_version": "requirements-roll-01",
        "service_write_run_id": _RUN,
    }
    request_fingerprint = payload_sha256(request_payload)
    source_digest = payload_sha256(source)
    action_id = "dim05-roll-" + payload_sha256(
        {
            "schema": "group-roll-exit-authorization-v1",
            "source_digest": source_digest,
            "request_fingerprint": request_fingerprint,
        }
    )
    risk_intent_id = "risk-close-" + leg_id
    close_side = Side.SELL if source_intent.side is Side.BUY else Side.BUY
    close_intent = OrderIntent(
        intent_id=action_id,
        signal_id=action_id,
        scope=source_intent.scope,
        instrument=source_intent.instrument,
        side=close_side,
        order_type=OrderType.LIMIT,
        quantity=leg.child_status.filled_quantity,
        price=Decimal("101.00"),
        time_in_force=TimeInForce.GTC,
        deadline_ns=snapshot.request.deadline_ns,
        position_effect=PositionEffect.CLOSE,
        reduce_only=True,
        metadata_version=_METADATA_VERSION,
        tags={},
        position_side=source_intent.position_side,
        position_bucket=source_intent.position_bucket,
    )
    authorization = {
        "schema": "group-roll-exit-authorization-v1",
        "request": request_payload,
        "source": source,
        "action_id": action_id,
        "client_order_id": "dim05-client-"
        + payload_sha256(
            {"schema": "group-roll-exit-authorization-v1", "action_id": action_id}
        )[:40],
        "intent": close_intent.to_payload(),
        "deadline_ns": close_intent.deadline_ns,
        "risk_intent_id": risk_intent_id,
    }
    authorization_sha256 = payload_sha256(authorization)
    authorization["authorization_sha256"] = authorization_sha256
    fee_amount = Decimal("0.03") if leg_id == "leg-long" else Decimal("0.04")
    claim = GroupRollExitRiskCloseClaimV1(
        cause_id=f"dispatch-inflight:{risk_intent_id}",
        claim_digest=payload_sha256({"claim": risk_intent_id}),
        claimed_at=_NOW_NS / 1_000_000_000,
        intent_hash=close_intent.fingerprint,
        intent_id=risk_intent_id,
        permit_id=f"risk-permit-{leg_id}",
        scope_key=payload_sha256({"scope": close_intent.scope.to_payload()}),
    )
    risk_receipt = GroupRollExitRiskCloseReceiptV1(
        claim=claim,
        proof_sha256=payload_sha256({"risk-proof": risk_intent_id}),
        exposure_proof_sha256=payload_sha256({"exposure-proof": risk_intent_id}),
        requested_quantity=close_intent.quantity,
        filled_quantity=close_intent.quantity,
        trade_count=1,
        order_revision=2,
        journal_revision=23,
        exposure_reservation_id=f"risk-exposure-{leg_id}",
        exposure_reservation_sha256=payload_sha256({"risk-reservation": risk_intent_id}),
    )
    fill = GroupRollExitCloseFeeFillV1(
        fill_id=f"fill-{leg_id}",
        fill_fingerprint=payload_sha256({"fill": leg_id}),
        source_row_sha256=payload_sha256({"source-row": leg_id}),
        amount=fee_amount,
        currency="USD",
    )
    fee_evidence = GroupRollExitCloseFeeEvidenceV1(
        intent_id=action_id,
        journal_revision=risk_receipt.journal_revision,
        fill_observation_sha256=payload_sha256({"fill-observation": leg_id}),
        fee_amount=fee_amount,
        fee_currency="USD",
        fills=(fill,),
    )
    risk_fee_wire = {
        "schema": "group-roll-exit-close-fee-evidence-v1",
        "intent_id": fee_evidence.intent_id,
        "journal_revision": fee_evidence.journal_revision,
        "fill_observation_sha256": fee_evidence.fill_observation_sha256,
        "fee_currency": fee_evidence.fee_currency,
        "fee_amount": format(fee_evidence.fee_amount, "f"),
        "fills": [item.to_payload() for item in fee_evidence.fills],
    }
    return _RollAction(
        leg_id=leg_id,
        source=source,
        source_event_sha256=source_event_sha256,
        source_status=leg.child_status,
        source_claim=leg.claim_receipt,
        source_resolution=leg.resolution_receipt,
        request=request_payload,
        intent=close_intent,
        action_id=action_id,
        risk_intent_id=risk_intent_id,
        authorization=authorization,
        authorization_sha256=authorization_sha256,
        risk_receipt=risk_receipt,
        fee_evidence=fee_evidence,
        risk_fee_wire=risk_fee_wire,
    )


def _status_observation(action: _RollAction, *, phase: str) -> dict[str, Any]:
    if phase == "RESOLVED":
        return {
            "schema": "group-roll-exit-observation-v1",
            "action_id": action.action_id,
            "phase": "RESOLVED",
            "source_digest": payload_sha256(action.source),
            "request_fingerprint": payload_sha256(action.request),
            "authorization_sha256": action.authorization_sha256,
            "intent_id": action.action_id,
            "close_state": "FILLED",
            "requested_quantity": format(action.intent.quantity, "f"),
            "filled_quantity": format(action.intent.quantity, "f"),
            "order_revision": 2,
            "close_receipt_sha256": action.risk_receipt.receipt_sha256,
            "detail_code": None,
            "risk_intent_id": action.risk_intent_id,
            "close_receipt": action.risk_receipt.to_payload(),
            "close_fee_evidence": action.risk_fee_wire,
        }
    if phase == "UNKNOWN":
        close_state = "UNKNOWN"
        filled_quantity = "0"
        order_revision = 1
        detail_code = "PROVIDER_OUTCOME_UNKNOWN"
    elif action.close_state == "PARTIALLY_FILLED":
        close_state = "PARTIALLY_FILLED"
        filled_quantity = "0.50"
        order_revision = 2
        detail_code = "PARTIAL_CLOSE_OBSERVED"
    else:
        close_state = "ACKED"
        filled_quantity = "0"
        order_revision = 1
        detail_code = "ORDER_ACKNOWLEDGED"
    return {
        "schema": "group-roll-exit-observation-v1",
        "action_id": action.action_id,
        "phase": phase,
        "source_digest": payload_sha256(action.source),
        "request_fingerprint": payload_sha256(action.request),
        "authorization_sha256": action.authorization_sha256,
        "intent_id": action.action_id,
        "close_state": close_state,
        "requested_quantity": format(action.intent.quantity, "f"),
        "filled_quantity": filled_quantity,
        "order_revision": order_revision,
        "close_receipt_sha256": None,
        "detail_code": detail_code,
        "risk_intent_id": action.risk_intent_id,
        "close_receipt": None,
        "close_fee_evidence": None,
    }


def _append_action_lifecycle(
    store: SqliteOrderGroupStore,
    snapshot: Any,
    action: _RollAction,
    *,
    unresolved: bool = False,
    clock: int,
) -> Any:
    plan_payload = {
        "action_id": action.action_id,
        "authorization_sha256": action.authorization_sha256,
        "authorization": action.authorization,
    }
    snapshot = store.update(
        snapshot,
        event_type="GROUP_ROLL_EXIT_PLAN_RECORDED",
        event_payload=plan_payload,
        state=GroupOrderState.UNWINDING,
        now_ns=clock,
    )
    snapshot = store.update(
        snapshot,
        event_type="GROUP_ROLL_EXIT_SUBMIT_STARTED",
        event_payload={
            "action_id": action.action_id,
            "authorization_sha256": action.authorization_sha256,
            "risk_intent_id": action.risk_intent_id,
            "intent_id": action.action_id,
            "client_order_id": action.authorization["client_order_id"],
        },
        state=GroupOrderState.UNWINDING,
        now_ns=clock + 1,
    )
    if unresolved:
        return store.update(
            snapshot,
            event_type="GROUP_ROLL_EXIT_STATUS_OBSERVED",
            event_payload={
                "action_id": action.action_id,
                "authorization_sha256": action.authorization_sha256,
                "risk_intent_id": action.risk_intent_id,
                "observation": {
                    **_status_observation(action, phase="UNKNOWN"),
                    "detail_code": "PROVIDER_OUTCOME_UNKNOWN",
                },
            },
            state=GroupOrderState.RECOVERY_REQUIRED,
            error_code="PROVIDER_OUTCOME_UNKNOWN",
            now_ns=clock + 2,
        )
    if action.close_state == "PARTIALLY_FILLED":
        return store.update(
            snapshot,
            event_type="GROUP_ROLL_EXIT_STATUS_OBSERVED",
            event_payload={
                "action_id": action.action_id,
                "authorization_sha256": action.authorization_sha256,
                "risk_intent_id": action.risk_intent_id,
                "observation": _status_observation(action, phase="STATUS_OBSERVED"),
            },
            state=GroupOrderState.UNWINDING,
            now_ns=clock + 2,
        )
    snapshot = store.update(
        snapshot,
        event_type="GROUP_ROLL_EXIT_STATUS_OBSERVED",
        event_payload={
            "action_id": action.action_id,
            "authorization_sha256": action.authorization_sha256,
            "risk_intent_id": action.risk_intent_id,
            "observation": _status_observation(action, phase="STATUS_OBSERVED"),
        },
        state=GroupOrderState.UNWINDING,
        now_ns=clock + 2,
    )
    return store.update(
        snapshot,
        event_type="GROUP_ROLL_EXIT_RESOLVED",
        event_payload={
            "action_id": action.action_id,
            "authorization_sha256": action.authorization_sha256,
            "risk_intent_id": action.risk_intent_id,
            "observation": _status_observation(action, phase="RESOLVED"),
            "risk_receipt": action.risk_receipt.to_payload(),
            "close_fee_evidence": action.risk_fee_wire,
        },
        state=GroupOrderState.UNWINDING,
        now_ns=clock + 3,
    )


def _append_action_plan_and_start(
    store: SqliteOrderGroupStore,
    snapshot: Any,
    action: _RollAction,
    *,
    clock: int,
) -> Any:
    snapshot = store.update(
        snapshot,
        event_type="GROUP_ROLL_EXIT_PLAN_RECORDED",
        event_payload={
            "action_id": action.action_id,
            "authorization_sha256": action.authorization_sha256,
            "authorization": action.authorization,
        },
        state=GroupOrderState.UNWINDING,
        now_ns=clock,
    )
    return store.update(
        snapshot,
        event_type="GROUP_ROLL_EXIT_SUBMIT_STARTED",
        event_payload={
            "action_id": action.action_id,
            "authorization_sha256": action.authorization_sha256,
            "risk_intent_id": action.risk_intent_id,
            "intent_id": action.action_id,
            "client_order_id": action.authorization["client_order_id"],
        },
        state=GroupOrderState.UNWINDING,
        now_ns=clock + 1,
    )


def _release_reservations(store: SqliteOrderGroupStore, snapshot: Any) -> Any:
    for index, account_ref in enumerate(snapshot.request.account_refs):
        current = next(item for item in snapshot.reservations if item.account_ref == account_ref)
        released = replace(current, phase=GroupReservationPhase.RELEASED)
        reservations = tuple(
            released if item.account_ref == account_ref else item
            for item in snapshot.reservations
        )
        snapshot = store.update(
            snapshot,
            event_type="RESERVATION_RELEASED",
            event_payload={"account_ref": account_ref, "roll_exit_settled": True},
            reservations=reservations,
            now_ns=_NOW_NS + 500 + index,
        )
    return snapshot


def _proof(snapshot: Any, store: SqliteOrderGroupStore, actions: tuple[_RollAction, ...]):
    action_by_leg = {action.leg_id: action for action in actions}
    closed_legs: list[GroupRollExitLegClosedV1] = []
    for requested_leg in snapshot.request.legs:
        leg_id = requested_leg.leg_id
        leg = next(item for item in snapshot.legs if item.leg_id == leg_id)
        action = action_by_leg[leg_id]
        assert (
            leg.child_status is not None
            and leg.claim_receipt is not None
            and leg.resolution_receipt is not None
        )
        assert leg.child_status.cumulative_commission_currency == "USD"
        close_action = GroupRollExitCloseActionV1(
            action_id=action.action_id,
            authorization_sha256=action.authorization_sha256,
            risk_intent_id=action.risk_intent_id,
            receipt_sha256=action.risk_receipt.receipt_sha256,
            close_fee_evidence_sha256=action.fee_evidence.evidence_sha256,
            close_quantity=action.intent.quantity,
            close_fee_amount=action.fee_evidence.fee_amount,
            close_fee_currency=action.fee_evidence.fee_currency,
        )
        closed_legs.append(
            GroupRollExitLegClosedV1(
                leg_id=leg_id,
                source_intent_id=leg.child_status.intent_id,
                source_event_sha256=action.source_event_sha256,
                source_claim_digest=leg.claim_receipt.claim_digest,
                source_resolution_sha256=payload_sha256(leg.resolution_receipt.to_payload()),
                source_filled_quantity=leg.child_status.filled_quantity,
                source_fee=GroupRollExitSourceFeeEvidenceV1(
                    intent_id=leg.child_status.intent_id,
                    amount=leg.child_status.cumulative_commission,
                    currency=leg.child_status.cumulative_commission_currency,
                    source_event_sha256=action.source_event_sha256,
                ),
                close_actions=(close_action,),
            )
        )
    budget = snapshot.budget_reservation
    budget_release = GroupRollExitBudgetReleaseV1(
        policy_fingerprint=budget.policy_fingerprint,
        quote_fingerprint=budget.quote_fingerprint,
        valuation_unit=budget.valuation_unit,
        gross_notional=budget.gross_notional,
        deadline_ns=budget.deadline_ns,
    )
    return GroupRollExitClosedProofV1(
        portfolio_ref=snapshot.request.portfolio_ref,
        group_id=snapshot.request.group_id,
        group_version=snapshot.request.version,
        group_fingerprint=snapshot.request.fingerprint,
        expected_revision=snapshot.revision,
        prior_group_event_chain_sha256=group_event_chain_sha256(
            snapshot,
            store.events(snapshot.request.portfolio_ref, snapshot.request.group_id),
        ),
        legs=tuple(closed_legs),
        reservation_sha256s=tuple(
            payload_sha256(item.to_payload()) for item in snapshot.reservations
        ),
        budget_release=budget_release,
    )


def _full_scenario(
    database: Path,
    *,
    partial_open_leg_id: str | None = None,
    risk_policy_fingerprint: str | None = None,
) -> _Scenario:
    store, snapshot = _paired_group(
        database,
        partial_open_leg_id=partial_open_leg_id,
        risk_policy_fingerprint=risk_policy_fingerprint,
    )
    actions: list[_RollAction] = []
    for index, plan in enumerate(snapshot.request.legs):
        action = _roll_action(snapshot, store, plan.leg_id)
        actions.append(action)
        snapshot = _append_action_lifecycle(
            store, snapshot, action, clock=_NOW_NS + 100 + index * 10
        )
    snapshot = _release_reservations(store, snapshot)
    proof = _proof(snapshot, store, tuple(actions))
    return _Scenario(database, store, snapshot, proof, tuple(actions))


def test_roll_exit_keeps_risk_and_portfolio_budget_policies_independent(
    tmp_path: Path,
) -> None:
    risk_policy_fingerprint = payload_sha256({"risk_policy": "account-risk-policy-v1"})
    scenario = _full_scenario(
        tmp_path / "distinct-risk-and-budget-policies.sqlite",
        risk_policy_fingerprint=risk_policy_fingerprint,
    )
    snapshot, store, proof = scenario.snapshot, scenario.store, scenario.proof
    budget_fingerprint = snapshot.budget_reservation.policy_fingerprint

    assert risk_policy_fingerprint != budget_fingerprint
    assert all(
        reservation.policy_fingerprint == risk_policy_fingerprint
        for reservation in snapshot.reservations
    )
    assert all(
        leg.claim_receipt is not None
        and leg.claim_receipt.permit.policy_fingerprint == risk_policy_fingerprint
        for leg in snapshot.legs
    )
    assert proof.budget_release is not None
    assert proof.budget_release.policy_fingerprint == budget_fingerprint
    assert proof.budget_release.policy_fingerprint != risk_policy_fingerprint

    closed = store.close_roll_exit(snapshot, proof, now_ns=_NOW_NS + 701)

    assert closed.state is GroupOrderState.CLOSED
    assert closed.budget_reservation.policy_fingerprint == budget_fingerprint
    assert all(
        reservation.policy_fingerprint == risk_policy_fingerprint
        for reservation in closed.reservations
    )


def test_closed_proof_requires_two_fully_resolved_legs_and_releases_v2_budget(
    tmp_path: Path,
) -> None:
    scenario = _full_scenario(tmp_path / "roll-exit.sqlite")
    snapshot, store, proof = scenario.snapshot, scenario.store, scenario.proof
    before_events = store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)
    assert snapshot.state is GroupOrderState.UNWINDING
    assert snapshot.residual_base_units == Decimal("0")
    assert snapshot.budget_reservation.state is GroupBudgetReservationState.HELD
    assert tuple(item.phase for item in snapshot.reservations) == (
        GroupReservationPhase.RELEASED,
    )
    assert all(leg.child_status.state is ExecutionState.FILLED for leg in snapshot.legs)
    assert all(
        leg.resolution_receipt.filled_quantity
        == snapshot.request.leg(leg.leg_id).maximum_intent.quantity
        for leg in snapshot.legs
    )
    assert all(
        action.risk_receipt.filled_quantity == action.intent.quantity
        and action.risk_receipt.trade_count == 1
        and action.fee_evidence.fee_amount == action.fee_evidence.fills[0].amount
        and _risk_close_fee_from_payload(action.risk_fee_wire) == action.fee_evidence
        for action in scenario.actions
    )
    event_types = [event.event_type for event in before_events]
    assert event_types.count("GROUP_ROLL_EXIT_PLAN_RECORDED") == 2
    assert event_types.count("GROUP_ROLL_EXIT_SUBMIT_STARTED") == 2
    assert event_types.count("GROUP_ROLL_EXIT_STATUS_OBSERVED") == 2
    assert event_types.count("GROUP_ROLL_EXIT_RESOLVED") == 2
    resolved_events = [
        event for event in before_events if event.event_type == "GROUP_ROLL_EXIT_RESOLVED"
    ]
    assert len(resolved_events) == 2
    for action in scenario.actions:
        resolved = next(
            event for event in resolved_events if event.payload["action_id"] == action.action_id
        )
        assert resolved.payload["observation"]["close_fee_evidence"] == action.risk_fee_wire
        assert resolved.payload["close_fee_evidence"] == action.risk_fee_wire
    release_events = [event for event in before_events if event.event_type == "RESERVATION_RELEASED"]
    assert len(release_events) == 1
    assert dict(release_events[0].payload) == {
        "account_ref": _ACCOUNT,
        "roll_exit_settled": True,
        "budget_state": "HELD",
    }
    assert proof.reservation_sha256s == tuple(
        payload_sha256(item.to_payload()) for item in snapshot.reservations
    )
    assert proof.budget_release is not None

    with pytest.raises(ContractValidationError):
        store.update(
            snapshot,
            event_type="GROUP_ROLL_EXIT_CLOSED",
            event_payload={
                "schema": "group-roll-exit-closed-event-v1",
                "proof": proof.to_payload(),
                "proof_sha256": proof.proof_sha256,
            },
            state=GroupOrderState.CLOSED,
            now_ns=_NOW_NS + 700,
        )
    assert store.get(snapshot.request.portfolio_ref, snapshot.request.group_id) == snapshot
    assert store.events(snapshot.request.portfolio_ref, snapshot.request.group_id) == before_events

    closed = store.close_roll_exit(snapshot, proof, now_ns=_NOW_NS + 701)
    assert closed.state is GroupOrderState.CLOSED
    assert closed.residual_base_units == Decimal("0")
    assert closed.budget_reservation.state is GroupBudgetReservationState.RELEASED
    assert all(item.phase is GroupReservationPhase.RELEASED for item in closed.reservations)
    assert all(
        leg.resolution_receipt.filled_quantity
        == closed.request.leg(leg.leg_id).maximum_intent.quantity
        for leg in closed.legs
    )
    closed_events = store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)
    close_events = [event for event in closed_events if event.event_type == "GROUP_ROLL_EXIT_CLOSED"]
    assert len(close_events) == 1
    assert dict(close_events[0].payload) == {
        "schema": "group-roll-exit-closed-event-v1",
        "proof": proof.to_payload(),
        "proof_sha256": proof.proof_sha256,
    }

    reopened = SqliteOrderGroupStore(scenario.database)
    try:
        assert reopened.get(snapshot.request.portfolio_ref, snapshot.request.group_id) == closed
        assert reopened.events(snapshot.request.portfolio_ref, snapshot.request.group_id) == closed_events
        with pytest.raises(IntentConflictError):
            reopened.close_roll_exit(snapshot, proof, now_ns=_NOW_NS + 702)
        assert reopened.get(snapshot.request.portfolio_ref, snapshot.request.group_id) == closed
        assert reopened.events(snapshot.request.portfolio_ref, snapshot.request.group_id) == closed_events
    finally:
        reopened.close()


def test_partial_open_hedge_closes_only_the_persisted_filled_quantities(
    tmp_path: Path,
) -> None:
    scenario = _full_scenario(
        tmp_path / "partial-open-hedge.sqlite", partial_open_leg_id="leg-long"
    )
    snapshot, store = scenario.snapshot, scenario.store
    first = next(leg for leg in snapshot.legs if leg.leg_id == "leg-long")
    second = next(leg for leg in snapshot.legs if leg.leg_id == "leg-short")
    assert first.submitted_intent is not None
    assert first.child_status is not None
    assert first.child_status.state is ExecutionState.CANCELLED
    assert first.child_status.filled_quantity == Decimal("1.50")
    assert first.submitted_intent.quantity == Decimal("2")
    assert second.submitted_intent is not None
    assert second.child_status is not None
    assert second.child_status.state is ExecutionState.FILLED
    assert second.submitted_intent.quantity == Decimal("1.50")
    assert second.child_status.filled_quantity == Decimal("1.50")
    assert {action.leg_id: action.intent.quantity for action in scenario.actions} == {
        "leg-long": Decimal("1.50"),
        "leg-short": Decimal("1.50"),
    }

    closed = store.close_roll_exit(snapshot, scenario.proof, now_ns=_NOW_NS + 701)

    assert closed.state is GroupOrderState.CLOSED
    assert closed.residual_base_units == Decimal("0")
    assert all(
        action.risk_receipt.filled_quantity == action.intent.quantity
        for action in scenario.actions
    )
    assert len(
        [
            event
            for event in store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)
            if event.event_type == "GROUP_ROLL_EXIT_CLOSED"
        ]
    ) == 1


def test_forged_close_fee_proof_is_rejected_without_partial_event_or_state_change(
    tmp_path: Path,
) -> None:
    scenario = _full_scenario(tmp_path / "forged-fee.sqlite")
    snapshot, store, proof = scenario.snapshot, scenario.store, scenario.proof
    payload = proof.to_payload()
    payload["legs"][0]["close_actions"][0]["close_fee_amount"] = "999.00"
    unsigned = dict(payload)
    unsigned.pop("proof_sha256")
    payload["proof_sha256"] = payload_sha256(unsigned)
    forged = GroupRollExitClosedProofV1.from_payload(payload)
    before_events = store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)

    with pytest.raises(ContractValidationError):
        store.close_roll_exit(snapshot, forged, now_ns=_NOW_NS + 800)

    assert store.get(snapshot.request.portfolio_ref, snapshot.request.group_id) == snapshot
    assert store.events(snapshot.request.portfolio_ref, snapshot.request.group_id) == before_events
    assert not any(event.event_type == "GROUP_ROLL_EXIT_CLOSED" for event in before_events)


@pytest.mark.parametrize("tamper_nested", [False, True], ids=["top-level", "nested"])
def test_resolved_event_rejects_mismatched_valid_risk_fee_wire(
    tmp_path: Path, tamper_nested: bool
) -> None:
    scope_name = "nested" if tamper_nested else "top-level"
    store, snapshot = _paired_group(tmp_path / f"tampered-{scope_name}-fee.sqlite")
    action = _roll_action(snapshot, store, snapshot.request.legs[0].leg_id)
    snapshot = _append_action_plan_and_start(
        store, snapshot, action, clock=_NOW_NS + 200
    )
    snapshot = store.update(
        snapshot,
        event_type="GROUP_ROLL_EXIT_STATUS_OBSERVED",
        event_payload={
            "action_id": action.action_id,
            "authorization_sha256": action.authorization_sha256,
            "risk_intent_id": action.risk_intent_id,
            "observation": _status_observation(action, phase="STATUS_OBSERVED"),
        },
        state=GroupOrderState.UNWINDING,
        now_ns=_NOW_NS + 202,
    )
    before_events = store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)
    tampered_fee = {
        **action.risk_fee_wire,
        "fee_amount": "1.03",
        "fills": [
            {**row, "amount": "1.03"}
            for row in action.risk_fee_wire["fills"]
        ],
    }
    assert _risk_close_fee_from_payload(tampered_fee) != action.fee_evidence
    observation = _status_observation(action, phase="RESOLVED")
    event_fee = action.risk_fee_wire
    if tamper_nested:
        observation["close_fee_evidence"] = tampered_fee
    else:
        event_fee = tampered_fee

    with pytest.raises(ContractValidationError):
        store.update(
            snapshot,
            event_type="GROUP_ROLL_EXIT_RESOLVED",
            event_payload={
                "action_id": action.action_id,
                "authorization_sha256": action.authorization_sha256,
                "risk_intent_id": action.risk_intent_id,
                "observation": observation,
                "risk_receipt": action.risk_receipt.to_payload(),
                "close_fee_evidence": event_fee,
            },
            state=GroupOrderState.UNWINDING,
            now_ns=_NOW_NS + 203,
        )

    assert store.get(snapshot.request.portfolio_ref, snapshot.request.group_id) == snapshot
    assert store.events(snapshot.request.portfolio_ref, snapshot.request.group_id) == before_events


def test_unknown_close_action_cannot_close_group_even_with_optimistic_proof(
    tmp_path: Path,
) -> None:
    store, snapshot = _paired_group(tmp_path / "unknown.sqlite")
    first = snapshot.request.legs[0]
    unknown_action = _roll_action(snapshot, store, first.leg_id)
    snapshot = _append_action_lifecycle(
        store,
        snapshot,
        unknown_action,
        unresolved=True,
        clock=_NOW_NS + 200,
    )
    assert snapshot.state is GroupOrderState.RECOVERY_REQUIRED
    assert snapshot.budget_reservation.state is GroupBudgetReservationState.HELD
    assert not any(
        event.event_type == "GROUP_ROLL_EXIT_RESOLVED"
        for event in store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)
    )

    # The second proof entry is well-typed but has no corresponding persisted action;
    # the unresolved first action must be rejected before a caller proof can close it.
    optimistic_second = _roll_action(snapshot, store, snapshot.request.legs[1].leg_id)
    proof = _proof(snapshot, store, (unknown_action, optimistic_second))
    before_events = store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)
    with pytest.raises(ContractValidationError):
        store.close_roll_exit(snapshot, proof, now_ns=_NOW_NS + 900)
    assert store.get(snapshot.request.portfolio_ref, snapshot.request.group_id) == snapshot
    assert store.events(snapshot.request.portfolio_ref, snapshot.request.group_id) == before_events
    assert not any(event.event_type == "GROUP_ROLL_EXIT_CLOSED" for event in before_events)
    assert all(item.phase is GroupReservationPhase.ACTIVE for item in snapshot.reservations)


def test_opposite_leg_plan_is_rejected_while_first_close_is_pending(tmp_path: Path) -> None:
    store, snapshot = _paired_group(tmp_path / "pending-opposite-leg.sqlite")
    first_action = _roll_action(snapshot, store, snapshot.request.legs[0].leg_id)
    snapshot = _append_action_plan_and_start(
        store, snapshot, first_action, clock=_NOW_NS + 200
    )
    before_events = store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)
    before_revision = snapshot.revision
    second_action = _roll_action(snapshot, store, snapshot.request.legs[1].leg_id)

    with pytest.raises(ContractValidationError):
        store.update(
            snapshot,
            event_type="GROUP_ROLL_EXIT_PLAN_RECORDED",
            event_payload={
                "action_id": second_action.action_id,
                "authorization_sha256": second_action.authorization_sha256,
                "authorization": second_action.authorization,
            },
            state=GroupOrderState.UNWINDING,
            now_ns=_NOW_NS + 210,
        )

    assert store.get(snapshot.request.portfolio_ref, snapshot.request.group_id) == snapshot
    assert snapshot.revision == before_revision
    assert store.events(snapshot.request.portfolio_ref, snapshot.request.group_id) == before_events
    assert sum(
        event.event_type == "GROUP_ROLL_EXIT_PLAN_RECORDED" for event in before_events
    ) == 1


def test_recovery_close_clears_stale_residual_only_after_full_proof(tmp_path: Path) -> None:
    scenario = _full_scenario(tmp_path / "recovery-residual.sqlite")
    snapshot, store = scenario.snapshot, scenario.store
    snapshot = store.update(
        snapshot,
        event_type="GROUP_RECOVERY_REQUIRED",
        event_payload={"code": "stale-residual-after-terminal-close"},
        state=GroupOrderState.RECOVERY_REQUIRED,
        residual_base_units=Decimal("0.25"),
        error_code="STALE_RESIDUAL_REQUIRES_RECONCILIATION",
        now_ns=_NOW_NS + 650,
    )
    assert snapshot.residual_base_units == Decimal("0.25")
    assert snapshot.error_code == "STALE_RESIDUAL_REQUIRES_RECONCILIATION"
    proof = _proof(snapshot, store, scenario.actions)
    before_events = store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)

    closed = store.close_roll_exit(snapshot, proof, now_ns=_NOW_NS + 651)

    assert closed.state is GroupOrderState.CLOSED
    assert closed.residual_base_units == Decimal("0")
    assert closed.error_code is None
    assert closed.budget_reservation.state is GroupBudgetReservationState.RELEASED
    close_events = store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)
    assert len(
        [event for event in close_events if event.event_type == "GROUP_ROLL_EXIT_CLOSED"]
    ) == 1
    assert len(close_events) == len(before_events) + 1


def test_same_revision_forged_snapshot_error_code_cannot_close_group(tmp_path: Path) -> None:
    scenario = _full_scenario(tmp_path / "forged-snapshot.sqlite")
    persisted = scenario.snapshot
    before_events = scenario.store.events(
        persisted.request.portfolio_ref, persisted.request.group_id
    )
    forged_snapshot = replace(persisted, error_code="FORGED_SNAPSHOT_PROJECTION")
    # Rebuild the proof from the forged projection so the rejection exercises
    # the store's persisted-snapshot fence, not a stale proof digest.
    forged_proof = _proof(forged_snapshot, scenario.store, scenario.actions)

    assert forged_snapshot.revision == persisted.revision
    assert forged_snapshot.request == persisted.request
    assert forged_snapshot.error_code != persisted.error_code
    with pytest.raises(IntentConflictError):
        scenario.store.close_roll_exit(forged_snapshot, forged_proof, now_ns=_NOW_NS + 1_100)

    assert scenario.store.get(persisted.request.portfolio_ref, persisted.request.group_id) == persisted
    assert scenario.store.events(persisted.request.portfolio_ref, persisted.request.group_id) == before_events


def test_v2_release_history_requires_held_budget_marker(tmp_path: Path) -> None:
    scenario = _full_scenario(tmp_path / "missing-v2-budget-marker.sqlite")
    snapshot, store = scenario.snapshot, scenario.store
    events = store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)
    release_index = next(
        index for index, event in enumerate(events) if event.event_type == "RESERVATION_RELEASED"
    )
    release = events[release_index]
    malformed_payload = dict(release.payload)
    assert malformed_payload.pop("budget_state") == GroupBudgetReservationState.HELD.value
    malformed_history = (
        events[:release_index]
        + (replace(release, payload=malformed_payload),)
        + events[release_index + 1 :]
    )

    with pytest.raises(ContractValidationError, match="budget marker"):
        _validate_released_reservations(snapshot, malformed_history)

    assert store.get(snapshot.request.portfolio_ref, snapshot.request.group_id) == snapshot
    assert store.events(snapshot.request.portfolio_ref, snapshot.request.group_id) == events


def test_stale_revision_and_stale_snapshot_fail_atomically(tmp_path: Path) -> None:
    scenario = _full_scenario(tmp_path / "stale.sqlite")
    snapshot, store, proof = scenario.snapshot, scenario.store, scenario.proof

    stale_payload = proof.to_payload()
    stale_payload["expected_revision"] = proof.expected_revision - 1
    unsigned = dict(stale_payload)
    unsigned.pop("proof_sha256")
    stale_payload["proof_sha256"] = payload_sha256(unsigned)
    stale_proof = GroupRollExitClosedProofV1.from_payload(stale_payload)
    before_events = store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)
    with pytest.raises(ContractValidationError):
        store.close_roll_exit(snapshot, stale_proof, now_ns=_NOW_NS + 1_000)
    assert store.get(snapshot.request.portfolio_ref, snapshot.request.group_id) == snapshot
    assert store.events(snapshot.request.portfolio_ref, snapshot.request.group_id) == before_events

    newer = store.update(
        snapshot,
        event_type="GROUP_RECOVERY_REQUIRED",
        event_payload={"code": "newer-snapshot"},
        state=GroupOrderState.RECOVERY_REQUIRED,
        error_code="newer-snapshot",
        now_ns=_NOW_NS + 1_001,
    )
    newer_events = store.events(snapshot.request.portfolio_ref, snapshot.request.group_id)
    with pytest.raises(IntentConflictError):
        store.close_roll_exit(snapshot, proof, now_ns=_NOW_NS + 1_002)
    assert store.get(snapshot.request.portfolio_ref, snapshot.request.group_id) == newer
    assert store.events(snapshot.request.portfolio_ref, snapshot.request.group_id) == newer_events
    assert not any(event.event_type == "GROUP_ROLL_EXIT_CLOSED" for event in newer_events)


def test_close_proof_parser_rejects_bool_group_version(tmp_path: Path) -> None:
    scenario = _full_scenario(tmp_path / "bool-version.sqlite")
    payload = scenario.proof.to_payload()
    payload["group_version"] = True
    unsigned = dict(payload)
    unsigned.pop("proof_sha256")
    payload["proof_sha256"] = payload_sha256(unsigned)

    with pytest.raises(ContractValidationError):
        GroupRollExitClosedProofV1.from_payload(payload)
