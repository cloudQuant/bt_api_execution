"""In-process tests of the durable P5B three-leg C/P/F staged chain.

Harness adapted from tests/test_group_coordinator.py (that file is unchanged):
the typed fake ports generalize to any leg count, a v3 request builder pins the
put -> future -> call staging order, and every scenario asserts the exact v3
durable evidence (error codes, residual vectors, budget hold bytes).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from bt_api_execution import (
    ExecutionScope,
    ExecutionState,
    GroupAccountReservationSnapshot,
    GroupBudgetLegQuote,
    GroupBudgetQuote,
    GroupBudgetReservationState,
    GroupChildOrderStatus,
    GroupDispatchResolutionReceipt,
    GroupLegClaimReceipt,
    GroupLegPhase,
    GroupLegOrderReceipt,
    GroupLegPermitReference,
    GroupOrderCoordinator,
    GroupOrderIntent,
    GroupOrderLegPlan,
    GroupOrderSnapshot,
    GroupOrderState,
    GroupReservationDeniedError,
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
from bt_api_execution.errors import ContractValidationError

_NOW = 1_800_000_000_000_000_000
_DEADLINE = _NOW + 10_000_000_000

_GROUP = "group-cpf"


class _BusinessOutcomeUnknownError(RuntimeError):
    code = "BUSINESS_OUTCOME_UNKNOWN"


@dataclass(frozen=True)
class _Scope:
    key: str


class _Mapper:
    def __init__(self, scope: _Scope) -> None:
        self.scope = scope

    def map_group_intent(self, intent: OrderIntent) -> object:
        profile = payload_sha256(
            {"instrument": intent.instrument, "side": intent.side.value, "effect": "OPEN"}
        )
        return SimpleNamespace(
            scope=self.scope,
            intent_id=intent.intent_id,
            fingerprint=payload_sha256({"intent": intent.to_payload(), "risk": "mapped"}),
            exposure_profile_fingerprint=profile,
            quantity=intent.quantity,
            strategy_id=intent.scope.strategy_id,
            instrument=intent.instrument,
            action=SimpleNamespace(value="INCREASE"),
        )


class _ReservationPort:
    def __init__(self, scope: _Scope, mapper: _Mapper, *, deny: str | None = None) -> None:
        self.scope = scope
        self.mapper = mapper
        self.deny = deny
        self.groups: dict[str, object] = {}
        self.reserve_calls: list[tuple[str, tuple[str, ...]]] = []
        self.query_calls: list[str] = []
        self.release_calls: list[str] = []
        self.lose_release_response = False
        self.lose_reserve_response = False

    @property
    def risk_scope_fingerprint(self) -> str:
        return payload_sha256({"scope_key": self.scope.key})

    def query_group_vector(self, group_id: str) -> GroupAccountReservationSnapshot:
        self.query_calls.append(group_id)
        receipt = self.groups.get(group_id)
        if receipt is None:
            error = LookupError("missing")
            error.code = "GROUP_NOT_FOUND"
            raise error
        return self._receipt(group_id)

    def reserve_group_vector(
        self, group_id: str, members: tuple[tuple[str, OrderIntent], ...], *, deadline_ns: int
    ) -> GroupAccountReservationSnapshot:
        if self.deny is not None:
            raise GroupReservationDeniedError(self.deny)
        self.reserve_calls.append((group_id, tuple(leg_id for leg_id, _ in members)))
        legs = {}
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
            account_ref=members[0][1].scope.account_ref,
            group_fingerprint=payload_sha256({"scope": self.scope.key, "group": group_id}),
            policy_fingerprint=payload_sha256({"policy": "unit"}),
            deadline_ns=deadline_ns,
            legs=legs,
        )
        if self.lose_reserve_response:
            self.lose_reserve_response = False
            raise _BusinessOutcomeUnknownError("reservation committed but reply was lost")
        return self._receipt(group_id)

    def release_group_unclaimed(
        self, group_id: str, reason: str
    ) -> GroupAccountReservationSnapshot:
        assert reason in {"group_rejected_before_dispatch", "group_first_leg_no_fill"}
        self.release_calls.append(group_id)
        receipt = self.groups[group_id]
        for leg in receipt.legs.values():
            if not leg["claimed"]:
                leg["state"] = "RELEASED"
        if self.lose_release_response:
            self.lose_release_response = False
            raise TimeoutError("release committed but reply was lost")
        return self._receipt(group_id)

    def set_claim(
        self, group_id: str, leg_id: str, intent: OrderIntent, claim: GroupLegClaimReceipt
    ) -> None:
        receipt = self.groups[group_id]
        leg = receipt.legs[leg_id]
        mapped = self.mapper.map_group_intent(intent)
        assert mapped.fingerprint == claim.actual_child_intent_hash
        leg["actual_intent_hash"] = mapped.fingerprint
        leg["claimed"] = True
        leg["resolved"] = False
        leg["state"] = "SETTLED"

    def resolve_claim(self, claim: GroupLegClaimReceipt) -> None:
        self.groups[claim.permit.group_id].legs[claim.permit.leg_id]["resolved"] = True

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
        self,
        reservations: _ReservationPort,
        *,
        group_id: str,
        leg_order: tuple[str, ...],
    ) -> None:
        self.reservations = reservations
        self.group_id = group_id
        self.leg_order = leg_order
        self.submissions: list[OrderIntent] = []
        self.statuses: dict[str, GroupChildOrderStatus] = {}
        self.query_ids: list[str] = []
        self.terminal_on_submit: dict[str, tuple[ExecutionState, Decimal, int, int]] = {}
        self.lose_first_response = False
        self.lose_response_leg_id: str | None = None
        self.fail_resolution = False
        self.claims: dict[str, GroupLegClaimReceipt] = {}
        self.resolutions: dict[str, GroupDispatchResolutionReceipt] = {}
        self.resolution_calls: list[str] = []

    def query(self, intent_id: str) -> GroupLegOrderReceipt | None:
        self.query_ids.append(intent_id)
        status = self.statuses.get(intent_id)
        if status is None:
            return None
        return GroupLegOrderReceipt(
            status=status,
            claim=self.claims.get(intent_id),
            resolution=self.resolutions.get(intent_id),
        )

    def submit_group_leg(
        self, intent: OrderIntent, permit: GroupLegPermitReference
    ) -> GroupLegOrderReceipt:
        self.submissions.append(intent)
        permit.validate_order(intent)
        assert permit.group_id == self.group_id
        # The staged chain may dispatch a leg only after every earlier leg in
        # the put/future/call order reached a terminal fill and resolved.
        position = self.leg_order.index(permit.leg_id)
        for prior_id in self.leg_order[:position]:
            prior_status = self.statuses.get(group_leg_intent_id(self.group_id, prior_id))
            assert prior_status is not None
            assert prior_status.state in {ExecutionState.FILLED, ExecutionState.CANCELLED}
            assert prior_status.filled_quantity > 0
            assert self.reservations.groups[permit.group_id].legs[prior_id]["resolved"]
        state, filled, trade_count, order_revision = self.terminal_on_submit[intent.intent_id]
        mapped = self.reservations.mapper.map_group_intent(intent)
        claim = GroupLegClaimReceipt(
            permit=permit,
            actual_child_intent_hash=mapped.fingerprint,
            claim_digest=payload_sha256({"claim": permit.permit_id, "order": intent.fingerprint}),
            actual_quantity=intent.quantity,
            actual_notional=Decimal("1"),
        )
        self.claims[intent.intent_id] = claim
        self.reservations.set_claim(permit.group_id, permit.leg_id, intent, claim)
        status = GroupChildOrderStatus(
            intent_id=intent.intent_id,
            state=state,
            filled_quantity=filled,
            provider_order_id=f"order-{permit.leg_id}",
            order_revision=order_revision,
            trade_count=trade_count,
        )
        self.statuses[intent.intent_id] = status
        if (self.lose_first_response and permit.leg_id == self.leg_order[0]) or (
            self.lose_response_leg_id == permit.leg_id
        ):
            status = GroupChildOrderStatus(
                intent_id=intent.intent_id,
                state=ExecutionState.UNKNOWN,
                filled_quantity=Decimal("0"),
            )
            self.statuses[intent.intent_id] = status
            raise TimeoutError("lost response")
        return GroupLegOrderReceipt(status=status, claim=claim)

    def resolve_group_leg(
        self,
        intent: OrderIntent,
        permit: GroupLegPermitReference,
        claim: GroupLegClaimReceipt,
        status: GroupChildOrderStatus,
    ) -> GroupDispatchResolutionReceipt:
        assert claim.permit == permit
        assert status.intent_id == intent.intent_id
        assert status.state in {
            ExecutionState.CANCELLED,
            ExecutionState.REJECTED,
            ExecutionState.BLOCKED,
            ExecutionState.FILLED,
        }
        if self.fail_resolution:
            raise TimeoutError("risk exposure resolution unavailable")
        self.resolution_calls.append(intent.intent_id)
        no_fill = status.filled_quantity == 0
        receipt = GroupDispatchResolutionReceipt(
            claim=claim,
            resolution_kind=(
                "TERMINAL_NO_FILL_RESOLVED" if no_fill else "TERMINAL_EXPOSURE_RESOLVED"
            ),
            proof_sha256=payload_sha256(
                {"proof": claim.claim_digest, "fill": str(status.filled_quantity)}
            ),
            final_state=status.state,
            requested_quantity=claim.actual_quantity,
            filled_quantity=status.filled_quantity,
            trade_count=status.trade_count or 0,
            order_revision=None if no_fill else status.order_revision,
            journal_revision=status.order_revision or 1,
            exposure_proof_sha256=None
            if no_fill
            else payload_sha256({"observation": claim.claim_digest}),
            exposure_reservation_id=None if no_fill else f"exposure-{claim.permit.leg_id}",
            exposure_reservation_sha256=None
            if no_fill
            else payload_sha256({"exposure": claim.claim_digest}),
            dispatch_latch_active=False,
            has_other_active_freezes=False,
        )
        self.resolutions[intent.intent_id] = receipt
        self.reservations.resolve_claim(claim)
        return receipt


def _make_request_v3() -> GroupOrderIntent:
    scope = ExecutionScope("paper", "demo", "account-a", "strategy-cpf")
    plans = []
    for leg_id, instrument, side, quantity, price, ratio, step in (
        ("leg-put", "PUT-USD", Side.BUY, "2", "10", Decimal("1"), Decimal("0.1")),
        ("leg-future", "FUT-USD", Side.SELL, "4", "20", Decimal("1"), Decimal("0.25")),
        ("leg-call", "CALL-USD", Side.BUY, "4", "5", Decimal("1"), Decimal("0.5")),
    ):
        intent = OrderIntent(
            intent_id=group_leg_intent_id(_GROUP, leg_id),
            scope=scope,
            signal_id="signal-cpf",
            instrument=instrument,
            side=side,
            position_effect=PositionEffect.OPEN,
            order_type=OrderType.LIMIT,
            quantity=Decimal(quantity),
            price=Decimal(price),
            metadata_version="fixture-metadata-v1",
            time_in_force=TimeInForce.IOC,
            deadline_ns=_DEADLINE,
            tags={"instrument_metadata_digest": payload_sha256({"instrument": instrument})},
        )
        plans.append(
            GroupOrderLegPlan(
                leg_id, "account-a", intent, Decimal("10"), step, _DEADLINE, ratio=ratio
            )
        )
    return GroupOrderIntent(
        group_id=_GROUP,
        portfolio_ref="portfolio-cpf",
        strategy_id="strategy-cpf",
        coordinator_principal_ref="coordinator-cpf",
        received_at_ns=_NOW,
        deadline_ns=_DEADLINE,
        first_leg_id="leg-put",
        legs=tuple(plans),
        version=3,
    )


def _make_request_v2() -> GroupOrderIntent:
    scopes = (
        ExecutionScope("paper", "demo", "account-a", "strategy-p5a"),
        ExecutionScope("paper", "demo", "account-a", "strategy-p5a"),
    )
    first = OrderIntent(
        intent_id=group_leg_intent_id("group-p5a", "leg-first"),
        scope=scopes[0],
        signal_id="signal-p5a",
        instrument="FOO-USD",
        side=Side.BUY,
        position_effect=PositionEffect.OPEN,
        order_type=OrderType.LIMIT,
        quantity=Decimal("2"),
        price=Decimal("10"),
        metadata_version="fixture-metadata-v1",
        time_in_force=TimeInForce.IOC,
        deadline_ns=_DEADLINE,
        tags={"instrument_metadata_digest": payload_sha256({"instrument": "FOO-USD"})},
    )
    second = OrderIntent(
        intent_id=group_leg_intent_id("group-p5a", "leg-hedge"),
        scope=scopes[1],
        signal_id="signal-p5a",
        instrument="BAR-USD",
        side=Side.SELL,
        position_effect=PositionEffect.OPEN,
        order_type=OrderType.LIMIT,
        quantity=Decimal("1.5"),
        price=Decimal("20"),
        metadata_version="fixture-metadata-v1",
        time_in_force=TimeInForce.IOC,
        deadline_ns=_DEADLINE,
        tags={"instrument_metadata_digest": payload_sha256({"instrument": "BAR-USD"})},
    )
    return GroupOrderIntent(
        group_id="group-p5a",
        portfolio_ref="portfolio-p5a",
        strategy_id="strategy-p5a",
        coordinator_principal_ref="coordinator-p5a",
        received_at_ns=_NOW,
        deadline_ns=_DEADLINE,
        first_leg_id="leg-first",
        legs=(
            GroupOrderLegPlan(
                "leg-first", "account-a", first, Decimal("1"), Decimal("0.25"), _DEADLINE
            ),
            GroupOrderLegPlan(
                "leg-hedge", "account-a", second, Decimal("2"), Decimal("0.25"), _DEADLINE
            ),
        ),
    )


def _coordinator(tmp_path, request: GroupOrderIntent, *, clock_ns=None, budget_policy=None):
    reservations = {}
    orders = {}
    leg_order = tuple(plan.leg_id for plan in request.legs)
    for account_ref in request.account_refs:
        scope = _Scope(
            f'{{"account_id":"risk-{account_ref}","environment":"demo","provider":"paper"}}'
        )
        mapper = _Mapper(scope)
        reservation = _ReservationPort(scope, mapper)
        reservations[account_ref] = reservation
        orders[account_ref] = _OrdersPort(
            reservation, group_id=request.group_id, leg_order=leg_order
        )
    store = SqliteOrderGroupStore(tmp_path / "groups.sqlite")
    policy = budget_policy or PortfolioGroupBudgetPolicy(
        portfolio_ref=request.portfolio_ref,
        policy_id="test-portfolio-budget",
        policy_version=1,
        valuation_unit="USD",
        max_group_gross_notional=Decimal("100000"),
        max_active_gross_notional=Decimal("100000"),
        max_active_groups=10,
    )
    coordinator = GroupOrderCoordinator(
        store,
        reservations=reservations,
        orders=orders,
        portfolio_budget_policies={request.portfolio_ref: policy},
        budget_quote_builder=_test_budget_quote,
        clock_ns=(lambda: _NOW + 1) if clock_ns is None else clock_ns,
    )
    return coordinator, store, reservations, orders


def _test_budget_quote(request, policy, *, now_ns):
    legs = []
    for plan in request.legs:
        intent = plan.maximum_intent
        digest = intent.tags["instrument_metadata_digest"]
        # Component tests bind a deterministic synthetic quote. Production
        # composition uses build_instrument_risk_group_budget_quote.
        gross = intent.quantity * intent.price + Decimal("0.5")
        legs.append(
            GroupBudgetLegQuote(
                leg_id=plan.leg_id,
                account_ref=plan.account_ref,
                intent_fingerprint=intent.fingerprint,
                metadata_digest=digest,
                assessment_fingerprint=payload_sha256(
                    {"synthetic-assessment": intent.fingerprint, "gross": format(gross, "f")}
                ),
                valuation_unit="USD",
                gross_notional=gross,
                assessed_at_ns=now_ns,
                valid_until_ns=plan.metadata_valid_until_ns,
            )
        )
    total = sum((item.gross_notional for item in legs), Decimal("0"))
    return GroupBudgetQuote(
        policy_fingerprint=policy.fingerprint,
        valuation_unit="USD",
        gross_notional=total,
        legs=tuple(legs),
    )


def _chain_intent_ids(request: GroupOrderIntent) -> tuple[str, str, str]:
    return tuple(plan.maximum_intent.intent_id for plan in request.legs)


def _recovery_payloads(store, request):
    return [
        event.payload
        for event in store.events(request.portfolio_ref, request.group_id)
        if event.event_type == "GROUP_RECOVERY_REQUIRED"
    ]


def test_v3_accept_quotes_three_legs_without_any_remote_calls(tmp_path) -> None:
    request = _make_request_v3()
    assert GroupOrderIntent.from_payload(request.to_payload()) == request
    coordinator, store, reservations, orders = _coordinator(tmp_path, request)

    accepted = coordinator.accept(request)

    assert accepted.state is GroupOrderState.PREPARING
    assert accepted.request.version == 3
    quote = accepted.request.budget_quote
    assert quote is not None
    assert len(quote.legs) == 3
    assert [leg.leg_id for leg in quote.legs] == ["leg-put", "leg-future", "leg-call"]
    assert quote.gross_notional == sum((leg.gross_notional for leg in quote.legs), Decimal("0"))
    assert quote.gross_notional == Decimal("121.5")
    assert accepted.budget_reservation.state is GroupBudgetReservationState.RESERVED
    assert accepted.residual_vector is None
    assert [leg.leg_id for leg in accepted.legs] == ["leg-put", "leg-future", "leg-call"]
    assert all(leg.submitted_intent is None for leg in accepted.legs)
    assert GroupOrderSnapshot.from_payload(accepted.to_payload()) == accepted
    assert all(not port.reserve_calls and not port.query_calls for port in reservations.values())
    assert all(not port.submissions and not port.query_ids for port in orders.values())
    store.close()


def test_v3_partial_first_fill_sizes_staged_chain_and_pairs(tmp_path) -> None:
    request = _make_request_v3()
    coordinator, store, reservations, orders = _coordinator(tmp_path, request)
    puts, futures, calls = _chain_intent_ids(request)
    orders["account-a"].terminal_on_submit[puts] = (
        ExecutionState.CANCELLED,
        Decimal("1.5"),
        1,
        3,
    )
    orders["account-a"].terminal_on_submit[futures] = (
        ExecutionState.FILLED,
        Decimal("1.5"),
        1,
        5,
    )
    orders["account-a"].terminal_on_submit[calls] = (
        ExecutionState.FILLED,
        Decimal("1.5"),
        1,
        7,
    )
    coordinator.accept(request)

    paired = coordinator.advance_group(request.portfolio_ref, request.group_id)

    assert paired.state is GroupOrderState.PAIRED
    assert paired.error_code is None
    assert [item.intent_id for item in orders["account-a"].submissions] == [
        puts,
        futures,
        calls,
    ]
    assert [item.quantity for item in orders["account-a"].submissions] == [
        Decimal("2"),
        Decimal("1.5"),
        Decimal("1.5"),
    ]
    assert reservations["account-a"].reserve_calls == [
        (request.group_id, ("leg-call", "leg-future", "leg-put"))
    ]
    assert paired.residual_base_units == Decimal("0")
    assert paired.residual_vector == (Decimal("0"), Decimal("0"), Decimal("0"))
    assert paired.legs[0].child_status.filled_quantity == Decimal("1.5")
    assert paired.legs[1].submitted_intent.quantity == Decimal("1.5")
    assert paired.legs[2].submitted_intent.quantity == Decimal("1.5")
    assert paired.budget_reservation.state is GroupBudgetReservationState.HELD
    # Each leg is always queried by its original intent id before any submit;
    # the first leg query is the idempotency check that returns None.
    assert orders["account-a"].query_ids == [puts, futures, calls]
    assert orders["account-a"].resolution_calls == [puts, futures, calls]
    paired_events = [
        event
        for event in store.events(request.portfolio_ref, request.group_id)
        if event.event_type == "GROUP_PAIRED"
    ]
    assert len(paired_events) == 1
    assert paired_events[0].payload == {
        "leg_ids": ["leg-put", "leg-future", "leg-call"],
        "filled_quantities": ["1.5", "1.5", "1.5"],
        "residual_vector": ["0", "0", "0"],
        "residual_base_units": "0",
        "budget_state": "HELD",
    }
    assert GroupOrderSnapshot.from_payload(paired.to_payload()) == paired
    store.close()


def test_v3_whole_group_reduction_sizes_both_remaining_legs_from_one_base(
    tmp_path,
) -> None:
    request = _make_request_v3()
    coordinator, store, reservations, orders = _coordinator(tmp_path, request)
    puts, futures, calls = _chain_intent_ids(request)
    orders["account-a"].terminal_on_submit[puts] = (
        ExecutionState.CANCELLED,
        Decimal("1.75"),
        1,
        3,
    )
    orders["account-a"].terminal_on_submit[futures] = (
        ExecutionState.FILLED,
        Decimal("1.5"),
        1,
        5,
    )
    orders["account-a"].terminal_on_submit[calls] = (
        ExecutionState.FILLED,
        Decimal("1.5"),
        1,
        7,
    )
    coordinator.accept(request)

    result = coordinator.advance_group(request.portfolio_ref, request.group_id)

    # The first fill is 17.5 base; the future leg alone would size to 1.75,
    # but the whole-group reduction resolves the common fillable base B* = 15
    # (call leg floors to 1.5 per its step), so both remaining legs submit 1.5.
    assert [item.quantity for item in orders["account-a"].submissions] == [
        Decimal("2"),
        Decimal("1.5"),
        Decimal("1.5"),
    ]
    assert result.legs[1].submitted_intent.quantity == Decimal("1.5")
    assert result.legs[2].submitted_intent.quantity == Decimal("1.5")
    assert result.state is GroupOrderState.RECOVERY_REQUIRED
    assert result.error_code == "chain_residual_unresolved"
    assert result.residual_vector == (Decimal("0"), Decimal("2.5"), Decimal("2.5"))
    assert result.residual_base_units == Decimal("5")
    assert result.budget_reservation.state is GroupBudgetReservationState.HELD
    assert _recovery_payloads(store, request)[-1] == {
        "leg_ids": ["leg-put", "leg-future", "leg-call"],
        "filled_quantities": ["1.75", "1.5", "1.5"],
        "residual_vector": ["0", "2.5", "2.5"],
        "residual_base_units": "5",
        "budget_state": "HELD",
    }
    store.close()


def test_v3_contract_rejects_ratio_not_expressible_in_step() -> None:
    request = _make_request_v3()
    unreachable = replace(request.legs[2], ratio=Decimal("0.01"))

    with pytest.raises(ContractValidationError, match="cannot be expressed in its step"):
        replace(request, legs=(request.legs[0], request.legs[1], unreachable))


def test_v3_first_fill_below_smallest_step_fails_closed_before_any_hedge(
    tmp_path,
) -> None:
    request = _make_request_v3()
    coordinator, store, reservations, orders = _coordinator(tmp_path, request)
    puts, _, _ = _chain_intent_ids(request)
    orders["account-a"].terminal_on_submit[puts] = (
        ExecutionState.CANCELLED,
        Decimal("0.05"),
        1,
        3,
    )
    coordinator.accept(request)

    result = coordinator.advance_group(request.portfolio_ref, request.group_id)

    assert result.state is GroupOrderState.RECOVERY_REQUIRED
    assert result.error_code == "chain_quantity_rounds_to_zero"
    assert [item.intent_id for item in orders["account-a"].submissions] == [puts]
    assert result.legs[1].submitted_intent is None
    assert result.legs[2].submitted_intent is None
    assert result.residual_vector is None
    assert result.budget_reservation.state is GroupBudgetReservationState.HELD
    store.close()


def test_v3_mid_chain_zero_fill_interrupts_and_records_residual_vector(tmp_path) -> None:
    request = _make_request_v3()
    coordinator, store, reservations, orders = _coordinator(tmp_path, request)
    puts, futures, calls = _chain_intent_ids(request)
    orders["account-a"].terminal_on_submit[puts] = (
        ExecutionState.CANCELLED,
        Decimal("1.5"),
        1,
        3,
    )
    orders["account-a"].terminal_on_submit[futures] = (
        ExecutionState.REJECTED,
        Decimal("0"),
        0,
        None,
    )
    coordinator.accept(request)

    interrupted = coordinator.advance_group(request.portfolio_ref, request.group_id)

    assert interrupted.state is GroupOrderState.RECOVERY_REQUIRED
    assert interrupted.error_code == "chain_interrupted_zero_fill"
    assert [item.intent_id for item in orders["account-a"].submissions] == [puts, futures]
    assert calls not in [item.intent_id for item in orders["account-a"].submissions]
    assert interrupted.legs[1].child_status.filled_quantity == Decimal("0")
    assert interrupted.legs[2].submitted_intent is None
    assert interrupted.residual_vector == (Decimal("0"), Decimal("15"), Decimal("15"))
    assert interrupted.residual_base_units == Decimal("30")
    assert interrupted.budget_reservation.state is GroupBudgetReservationState.HELD
    assert _recovery_payloads(store, request)[-1] == {
        "code": "CHAIN_INTERRUPTED_ZERO_FILL",
        "budget_state": "HELD",
    }
    store.close()


def test_v3_zero_fill_first_leg_closes_and_releases_without_sizing_leg_two(
    tmp_path,
) -> None:
    request = _make_request_v3()
    coordinator, store, reservations, orders = _coordinator(tmp_path, request)
    puts, _, _ = _chain_intent_ids(request)
    orders["account-a"].terminal_on_submit[puts] = (
        ExecutionState.REJECTED,
        Decimal("0"),
        0,
        None,
    )
    coordinator.accept(request)

    closed = coordinator.advance_group(request.portfolio_ref, request.group_id)

    assert closed.state is GroupOrderState.CLOSED
    assert closed.error_code is None
    assert [item.intent_id for item in orders["account-a"].submissions] == [puts]
    assert closed.legs[1].submitted_intent is None
    assert closed.legs[2].submitted_intent is None
    assert closed.residual_base_units == Decimal("0")
    assert closed.residual_vector is None
    assert reservations["account-a"].release_calls == [request.group_id]
    assert closed.budget_reservation.state is GroupBudgetReservationState.RELEASED
    assert all(item.phase is GroupReservationPhase.RELEASED for item in closed.reservations)
    store.close()


def test_v3_lost_final_leg_reply_restarts_by_original_id_without_resubmit(
    tmp_path,
) -> None:
    request = _make_request_v3()
    coordinator, store, reservations, orders = _coordinator(tmp_path, request)
    puts, futures, calls = _chain_intent_ids(request)
    orders["account-a"].terminal_on_submit[puts] = (
        ExecutionState.CANCELLED,
        Decimal("1.5"),
        1,
        3,
    )
    orders["account-a"].terminal_on_submit[futures] = (
        ExecutionState.FILLED,
        Decimal("1.5"),
        1,
        5,
    )
    orders["account-a"].terminal_on_submit[calls] = (
        ExecutionState.FILLED,
        Decimal("1.5"),
        1,
        7,
    )
    orders["account-a"].lose_response_leg_id = "leg-call"
    coordinator.accept(request)

    interrupted = coordinator.advance_group(request.portfolio_ref, request.group_id)

    # v2-identical UNKNOWN handling: the leg flips to UNKNOWN/ RECOVERY_REQUIRED
    # without a new error code (it lands on the next reconciliation tick) and
    # the staged chain never submits a later leg.
    assert interrupted.state is GroupOrderState.RECOVERY_REQUIRED
    assert interrupted.error_code is None
    assert interrupted.legs[2].phase is GroupLegPhase.UNKNOWN
    assert interrupted.budget_reservation.state is GroupBudgetReservationState.HELD
    assert [item.intent_id for item in orders["account-a"].submissions] == [
        puts,
        futures,
        calls,
    ]
    assert orders["account-a"].statuses[calls].state is ExecutionState.UNKNOWN
    store.close()

    restarted_store = SqliteOrderGroupStore(tmp_path / "groups.sqlite")
    persisted = restarted_store.get(request.portfolio_ref, request.group_id)
    assert persisted.budget_reservation.state is GroupBudgetReservationState.HELD
    orders["account-a"].statuses[calls] = GroupChildOrderStatus(
        intent_id=calls,
        state=ExecutionState.FILLED,
        filled_quantity=Decimal("1.5"),
        provider_order_id="order-leg-call",
        order_revision=7,
        trade_count=1,
    )
    restarted = GroupOrderCoordinator(
        restarted_store,
        reservations=reservations,
        orders=orders,
        clock_ns=lambda: _NOW + 2,
    )

    result = restarted.advance_group(request.portfolio_ref, request.group_id)

    assert result.state is GroupOrderState.PAIRED
    assert result.error_code is None
    assert result.budget_reservation.state is GroupBudgetReservationState.HELD
    assert result.residual_vector == (Decimal("0"), Decimal("0"), Decimal("0"))
    assert calls in orders["account-a"].query_ids
    assert sum(item.intent_id == calls for item in orders["account-a"].submissions) == 1
    assert len(orders["account-a"].submissions) == 3
    assert len(orders["account-a"].statuses) == 3
    restarted_store.close()


def test_v3_partial_mid_leg_fill_finishes_chain_with_exact_recovery_vector(
    tmp_path,
) -> None:
    request = _make_request_v3()
    coordinator, store, reservations, orders = _coordinator(tmp_path, request)
    puts, futures, calls = _chain_intent_ids(request)
    orders["account-a"].terminal_on_submit[puts] = (
        ExecutionState.CANCELLED,
        Decimal("1.5"),
        1,
        3,
    )
    orders["account-a"].terminal_on_submit[futures] = (
        ExecutionState.FILLED,
        Decimal("1.25"),
        1,
        5,
    )
    orders["account-a"].terminal_on_submit[calls] = (
        ExecutionState.FILLED,
        Decimal("1.5"),
        1,
        7,
    )
    coordinator.accept(request)

    result = coordinator.advance_group(request.portfolio_ref, request.group_id)

    assert result.state is GroupOrderState.RECOVERY_REQUIRED
    assert result.error_code == "chain_residual_unresolved"
    assert result.residual_vector == (Decimal("0"), Decimal("2.5"), Decimal("0"))
    assert result.residual_base_units == Decimal("2.5")
    assert [item.quantity for item in orders["account-a"].submissions] == [
        Decimal("2"),
        Decimal("1.5"),
        Decimal("1.5"),
    ]
    assert _recovery_payloads(store, request)[-1] == {
        "leg_ids": ["leg-put", "leg-future", "leg-call"],
        "filled_quantities": ["1.5", "1.25", "1.5"],
        "residual_vector": ["0", "2.5", "0"],
        "residual_base_units": "2.5",
        "budget_state": "HELD",
    }
    store.close()


def test_v3_budget_hold_is_quoted_once_and_never_mutated_by_staging(tmp_path) -> None:
    request = _make_request_v3()
    coordinator, store, reservations, orders = _coordinator(tmp_path, request)
    puts, futures, calls = _chain_intent_ids(request)
    orders["account-a"].terminal_on_submit[puts] = (
        ExecutionState.CANCELLED,
        Decimal("1.5"),
        1,
        3,
    )
    orders["account-a"].terminal_on_submit[futures] = (
        ExecutionState.FILLED,
        Decimal("1.5"),
        1,
        5,
    )
    orders["account-a"].terminal_on_submit[calls] = (
        ExecutionState.FILLED,
        Decimal("1.5"),
        1,
        7,
    )
    accepted = coordinator.accept(request)
    assert accepted.budget_reservation.state is GroupBudgetReservationState.RESERVED
    quote_sha256 = payload_sha256(accepted.request.budget_quote.to_payload())
    request_sha256 = payload_sha256(accepted.request.to_payload())

    paired = coordinator.advance_group(request.portfolio_ref, request.group_id)

    assert paired.state is GroupOrderState.PAIRED
    assert paired.budget_reservation.state is GroupBudgetReservationState.HELD
    assert payload_sha256(paired.request.budget_quote.to_payload()) == quote_sha256
    assert payload_sha256(paired.request.to_payload()) == request_sha256
    assert paired.request.budget_quote.gross_notional == Decimal("121.5")
    # One three-leg hold is quoted once: staged sizing of leg two and leg
    # three never re-reserves, re-quotes, or duplicates the budget vector.
    assert reservations["account-a"].reserve_calls == [
        (request.group_id, ("leg-call", "leg-future", "leg-put"))
    ]
    store.close()


def test_v3_contract_negatives_are_exact() -> None:
    request = _make_request_v3()

    with pytest.raises(ContractValidationError, match="requires exactly three legs"):
        replace(request, legs=request.legs[:2])
    with pytest.raises(ContractValidationError, match="must start at the first leg"):
        replace(request, first_leg_id="leg-future")
    with pytest.raises(ContractValidationError, match="first-leg ratio must equal one"):
        replace(
            request,
            legs=(replace(request.legs[0], ratio=Decimal("2")), request.legs[1], request.legs[2]),
        )
    with pytest.raises(ContractValidationError, match="non-first legs require a positive ratio"):
        replace(
            request,
            legs=(request.legs[0], replace(request.legs[1], ratio=None), request.legs[2]),
        )
    with pytest.raises(ContractValidationError, match="share one account"):
        replace(
            request,
            legs=(
                request.legs[0],
                request.legs[1],
                replace(request.legs[2], account_ref="account-b"),
            ),
        )
    with pytest.raises(ContractValidationError, match="cannot all use one side"):
        replace(
            request,
            legs=(
                request.legs[0],
                replace(
                    request.legs[1],
                    maximum_intent=replace(request.legs[1].maximum_intent, side=Side.BUY),
                ),
                request.legs[2],
            ),
        )

    future = request.legs[1]
    with pytest.raises(ContractValidationError, match="invalid ratio"):
        GroupOrderLegPlan(
            "leg-future",
            "account-a",
            future.maximum_intent,
            Decimal("10"),
            Decimal("0.25"),
            _DEADLINE,
            ratio=Decimal("0"),
        )
    with pytest.raises(ContractValidationError, match="invalid ratio"):
        GroupOrderLegPlan(
            "leg-future",
            "account-a",
            future.maximum_intent,
            Decimal("10"),
            Decimal("0.25"),
            _DEADLINE,
            ratio=Decimal("-0.5"),
        )

    legacy = _make_request_v2()
    with pytest.raises(ContractValidationError, match="cannot carry a ratio"):
        replace(legacy, legs=(legacy.legs[0], replace(legacy.legs[1], ratio=Decimal("1"))))


def test_v2_two_leg_intent_still_accepts_sizes_and_pairs(tmp_path) -> None:
    request = _make_request_v2()
    coordinator, store, reservations, orders = _coordinator(tmp_path, request)
    first, hedge = request.ordered_legs
    orders["account-a"].terminal_on_submit[first.maximum_intent.intent_id] = (
        ExecutionState.CANCELLED,
        Decimal("1.5"),
        1,
        3,
    )
    orders["account-a"].terminal_on_submit[hedge.maximum_intent.intent_id] = (
        ExecutionState.FILLED,
        Decimal("0.75"),
        1,
        5,
    )

    accepted = coordinator.accept(request)
    assert accepted.request.version == 2
    assert all("ratio" not in leg for leg in accepted.request.to_payload()["legs"])
    assert "residual_vector" not in accepted.to_payload()

    paired = coordinator.advance_group(request.portfolio_ref, request.group_id)

    assert paired.state is GroupOrderState.PAIRED
    assert paired.error_code is None
    assert [item.quantity for item in orders["account-a"].submissions] == [
        Decimal("2"),
        Decimal("0.75"),
    ]
    assert paired.residual_base_units == Decimal("0")
    assert paired.residual_vector is None
    assert "residual_vector" not in paired.to_payload()
    assert paired.budget_reservation.state is GroupBudgetReservationState.HELD
    store.close()


def test_v2_partial_hedge_fill_keeps_legacy_residual_error_code(tmp_path) -> None:
    request = _make_request_v2()
    coordinator, store, reservations, orders = _coordinator(tmp_path, request)
    first, hedge = request.ordered_legs
    orders["account-a"].terminal_on_submit[first.maximum_intent.intent_id] = (
        ExecutionState.CANCELLED,
        Decimal("1.5"),
        1,
        3,
    )
    orders["account-a"].terminal_on_submit[hedge.maximum_intent.intent_id] = (
        ExecutionState.FILLED,
        Decimal("0.5"),
        1,
        5,
    )
    coordinator.accept(request)

    result = coordinator.advance_group(request.portfolio_ref, request.group_id)

    assert result.state is GroupOrderState.RECOVERY_REQUIRED
    assert result.error_code == "hedge_residual_unresolved"
    assert result.residual_base_units == Decimal("0.5")
    assert result.residual_vector is None
    assert [item.quantity for item in orders["account-a"].submissions] == [
        Decimal("2"),
        Decimal("0.75"),
    ]
    assert result.budget_reservation.state is GroupBudgetReservationState.HELD
    store.close()
