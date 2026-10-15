"""Durable portfolio-group budget admission and quote-boundary tests."""

from __future__ import annotations

import multiprocessing
import sqlite3
from dataclasses import replace
from decimal import Decimal

import pytest

from bt_api_execution import (
    ExecutionScope,
    GroupAccountReservationSnapshot,
    GroupBudgetLegQuote,
    GroupBudgetQuote,
    GroupBudgetReservationState,
    GroupBudgetUnsupportedOrderError,
    GroupLegPhase,
    GroupLegProgress,
    GroupOrderCoordinator,
    GroupOrderIntent,
    GroupOrderLegPlan,
    GroupOrderSnapshot,
    GroupOrderState,
    IntentConflictError,
    OrderIntent,
    OrderType,
    PortfolioGroupBudgetPolicy,
    PositionEffect,
    Side,
    SqliteOrderGroupStore,
    TimeInForce,
    build_instrument_risk_group_budget_quote,
    canonical_json,
    group_leg_intent_id,
    payload_sha256,
)
from bt_api_execution.errors import ContractValidationError

_NOW = 1_800_000_000_000_000_000
_DEADLINE = _NOW + 10_000_000_000


def _policy(
    portfolio: str = "portfolio-budget",
    *,
    max_group: Decimal = Decimal("100"),
    max_active: Decimal = Decimal("100"),
    max_groups: int = 8,
    unit: str = "USD",
    version: int = 1,
) -> PortfolioGroupBudgetPolicy:
    return PortfolioGroupBudgetPolicy(
        portfolio_ref=portfolio,
        policy_id="budget-policy",
        policy_version=version,
        valuation_unit=unit,
        max_group_gross_notional=max_group,
        max_active_gross_notional=max_active,
        max_active_groups=max_groups,
    )


def _request(group_id: str, *, portfolio: str = "portfolio-budget") -> GroupOrderIntent:
    scopes = (
        ExecutionScope("synthetic", "paper", "account-a", "strategy-a"),
        ExecutionScope("synthetic", "paper", "account-b", "strategy-a"),
    )
    plans = []
    for leg_id, account, scope, instrument, side, quantity, price in (
        ("leg-a", "account-a", scopes[0], "FOO-USD", Side.BUY, "2", "10"),
        ("leg-b", "account-b", scopes[1], "BAR-USD", Side.SELL, "1", "20"),
    ):
        metadata_digest = payload_sha256({"trusted-metadata": instrument})
        intent = OrderIntent(
            intent_id=group_leg_intent_id(group_id, leg_id),
            scope=scope,
            signal_id="synthetic-signal",
            instrument=instrument,
            side=side,
            position_effect=PositionEffect.OPEN,
            order_type=OrderType.LIMIT,
            quantity=Decimal(quantity),
            price=Decimal(price),
            time_in_force=TimeInForce.IOC,
            metadata_version="metadata-v1",
            deadline_ns=_DEADLINE,
            tags={"instrument_metadata_digest": metadata_digest},
        )
        plans.append(
            GroupOrderLegPlan(
                leg_id,
                account,
                intent,
                Decimal("1"),
                Decimal("1"),
                _DEADLINE,
            )
        )
    return GroupOrderIntent(
        group_id=group_id,
        portfolio_ref=portfolio,
        strategy_id="strategy-a",
        coordinator_principal_ref="coordinator-a",
        received_at_ns=_NOW,
        deadline_ns=_DEADLINE,
        first_leg_id="leg-a",
        legs=tuple(plans),
    )


def _quote(
    request: GroupOrderIntent,
    policy: PortfolioGroupBudgetPolicy,
    *,
    now_ns: int = _NOW + 1,
    gross_total: Decimal = Decimal("60"),
) -> GroupBudgetQuote:
    per_leg = gross_total / Decimal(len(request.legs))
    legs = tuple(
        GroupBudgetLegQuote(
            leg_id=plan.leg_id,
            account_ref=plan.account_ref,
            intent_fingerprint=plan.maximum_intent.fingerprint,
            metadata_digest=plan.maximum_intent.tags["instrument_metadata_digest"],
            assessment_fingerprint=payload_sha256(
                {"synthetic-assessment": plan.maximum_intent.fingerprint, "gross": str(per_leg)}
            ),
            valuation_unit=policy.valuation_unit,
            gross_notional=per_leg,
            assessed_at_ns=now_ns,
            valid_until_ns=plan.metadata_valid_until_ns,
        )
        for plan in request.legs
    )
    return GroupBudgetQuote(
        policy_fingerprint=policy.fingerprint,
        valuation_unit=policy.valuation_unit,
        gross_notional=gross_total,
        legs=legs,
    )


def _as_budgeted(request: GroupOrderIntent, policy: PortfolioGroupBudgetPolicy) -> GroupOrderIntent:
    return replace(request, version=2, budget_quote=_quote(request, policy))


def _reserve_worker(db_path, request_payload, policy_payload, barrier, result_queue) -> None:
    request = GroupOrderIntent.from_payload(request_payload)
    policy = PortfolioGroupBudgetPolicy.from_payload(policy_payload)
    store = SqliteOrderGroupStore(db_path, busy_timeout_ms=10_000)
    barrier.wait(timeout=15)
    snapshot = store.create_or_get(
        request,
        budget_policy=policy,
        now_ns=_NOW + 1,
        clock_ns=lambda: _NOW + 2,
    )
    result_queue.put(
        (
            snapshot.request.group_id,
            snapshot.state.value,
            snapshot.budget_reservation.state.value,
        )
    )
    store.close()


def test_real_risk_registry_quote_is_fee_and_slippage_inclusive() -> None:
    risk = pytest.importorskip("bt_api_risk")
    metadata = tuple(
        risk.InstrumentRiskMetadata(
            instrument=symbol,
            metadata_version="metadata-v1",
            as_of_ns=_NOW - 1_000_000,
            expires_at_ns=_DEADLINE,
            tick_size=Decimal("0.01"),
            quantity_step=Decimal("1"),
            contract_multiplier=Decimal("2"),
            max_gross_notional=Decimal("100000"),
            taker_fee_bps=Decimal("25"),
            fixed_fee=Decimal("0.4"),
            max_slippage_bps=Decimal("100"),
            quantity_unit="contract",
            valuation_unit="USD",
        )
        for symbol in ("FOO-USD", "BAR-USD")
    )
    # Rebuild exact intent digests using the actual public metadata records.
    request = _request("risk-assessed")
    plans = tuple(
        replace(
            plan,
            maximum_intent=replace(
                plan.maximum_intent,
                tags={
                    "instrument_metadata_digest": next(
                        item.digest
                        for item in metadata
                        if item.instrument == plan.maximum_intent.instrument
                    )
                },
            ),
        )
        for plan in request.legs
    )
    request = replace(request, legs=plans)
    policy = _policy(max_group=Decimal("1000"), max_active=Decimal("1000"))
    registry = risk.InstrumentRiskRegistry(metadata)

    quote = build_instrument_risk_group_budget_quote(request, policy, registry, now_ns=_NOW + 1)

    first = request.leg("leg-a").maximum_intent
    assessment = registry.assess(
        risk.InstrumentRiskOrder(
            instrument=first.instrument,
            quantity=first.quantity,
            limit_price=first.price,
            metadata_version=first.metadata_version,
            metadata_digest=first.tags["instrument_metadata_digest"],
        ),
        _NOW + 1,
    )
    quoted = next(leg for leg in quote.legs if leg.leg_id == "leg-a")
    assert quoted.gross_notional == assessment.gross_notional
    assert quoted.gross_notional > first.quantity * first.price
    assert quoted.valuation_unit == "USD"
    assert quoted.valid_until_ns == _DEADLINE


def test_builtin_budget_quote_rejects_market_without_trusted_price_bound() -> None:
    risk = pytest.importorskip("bt_api_risk")
    request = _request("market-no-bound")
    first = request.leg("leg-a")
    market_intent = replace(
        first.maximum_intent,
        order_type=OrderType.MARKET,
        price=None,
    )
    request = replace(
        request,
        legs=tuple(
            replace(plan, maximum_intent=market_intent) if plan.leg_id == first.leg_id else plan
            for plan in request.legs
        ),
    )
    assert GroupOrderIntent.from_payload(request.to_payload()) == request

    metadata = risk.InstrumentRiskMetadata(
        instrument=market_intent.instrument,
        metadata_version=market_intent.metadata_version,
        as_of_ns=_NOW - 1_000_000,
        expires_at_ns=_DEADLINE,
        tick_size=Decimal("0.01"),
        quantity_step=Decimal("1"),
        contract_multiplier=Decimal("1"),
        max_gross_notional=Decimal("100000"),
        taker_fee_bps=Decimal("10"),
        fixed_fee=Decimal("0"),
        max_slippage_bps=Decimal("0"),
        quantity_unit="contract",
        valuation_unit="USD",
    )
    registry = risk.InstrumentRiskRegistry((metadata,))
    registry.assess = lambda *_args: pytest.fail("Risk assess must not receive unbounded market")

    with pytest.raises(GroupBudgetUnsupportedOrderError) as caught:
        build_instrument_risk_group_budget_quote(
            request,
            _policy(max_group=Decimal("1000"), max_active=Decimal("1000")),
            registry,
            now_ns=_NOW + 1,
        )
    assert caught.value.code == "group_budget_market_price_unbounded"


@pytest.mark.parametrize("metadata_unit", [None, "EUR"])
def test_group_quote_rejects_missing_or_cross_currency_valuation(metadata_unit) -> None:
    risk = pytest.importorskip("bt_api_risk")
    metadata = tuple(
        risk.InstrumentRiskMetadata(
            instrument=symbol,
            metadata_version="metadata-v1",
            as_of_ns=_NOW - 1_000_000,
            expires_at_ns=_DEADLINE,
            tick_size=Decimal("0.01"),
            quantity_step=Decimal("1"),
            contract_multiplier=Decimal("1"),
            max_gross_notional=Decimal("100000"),
            taker_fee_bps=Decimal("10"),
            fixed_fee=Decimal("0"),
            max_slippage_bps=Decimal("0"),
            quantity_unit="contract",
            valuation_unit=metadata_unit,
        )
        for symbol in ("FOO-USD", "BAR-USD")
    )
    request = _request(f"unit-{metadata_unit}")
    request = replace(
        request,
        legs=tuple(
            replace(
                plan,
                maximum_intent=replace(
                    plan.maximum_intent,
                    tags={
                        "instrument_metadata_digest": next(
                            item.digest
                            for item in metadata
                            if item.instrument == plan.maximum_intent.instrument
                        )
                    },
                ),
            )
            for plan in request.legs
        ),
    )
    with pytest.raises(ContractValidationError, match="units or validity"):
        build_instrument_risk_group_budget_quote(
            request,
            _policy(max_group=Decimal("1000"), max_active=Decimal("1000")),
            risk.InstrumentRiskRegistry(metadata),
            now_ns=_NOW + 1,
        )


def test_same_id_replay_returns_original_policy_quote_without_reassessment(tmp_path) -> None:
    request = _request("idempotent-budget")
    original_policy = _policy(max_group=Decimal("100"), max_active=Decimal("100"))
    first_store = SqliteOrderGroupStore(tmp_path / "budget-idempotency.sqlite3")
    first = first_store.create_or_get(
        _as_budgeted(request, original_policy),
        budget_policy=original_policy,
        now_ns=_NOW + 1,
        clock_ns=lambda: _NOW + 2,
    )
    first_store.close()

    changed_policy = _policy(max_group=Decimal("10"), max_active=Decimal("10"), version=2)
    changed_quote_request = _as_budgeted(request, changed_policy)
    reopened = SqliteOrderGroupStore(tmp_path / "budget-idempotency.sqlite3")
    replay = reopened.create_or_get(
        changed_quote_request,
        budget_policy=changed_policy,
        now_ns=_NOW + 3,
        clock_ns=lambda: _NOW + 4,
    )
    assert replay.request.fingerprint == first.request.fingerprint
    assert replay.budget_reservation == first.budget_reservation
    assert replay.budget_reservation.policy == original_policy
    assert replay.budget_reservation.gross_notional == Decimal("60")

    changed_content = replace(request, received_at_ns=_NOW + 5)
    with pytest.raises(IntentConflictError):
        reopened.create_or_get(
            _as_budgeted(changed_content, changed_policy),
            budget_policy=changed_policy,
            now_ns=_NOW + 6,
        )
    reopened.close()


def test_new_group_cannot_apply_a_different_policy_while_old_hold_is_active(tmp_path) -> None:
    store = SqliteOrderGroupStore(tmp_path / "policy-generation.sqlite3")
    original_policy = _policy(max_group=Decimal("100"), max_active=Decimal("100"))
    first_request = _request("policy-generation-first")
    first = store.create_or_get(
        _as_budgeted(first_request, original_policy),
        budget_policy=original_policy,
        now_ns=_NOW + 1,
        clock_ns=lambda: _NOW + 2,
    )
    started_leg = replace(
        first.legs[0],
        phase=GroupLegPhase.REQUESTING,
        submitted_intent=first_request.ordered_legs[0].maximum_intent,
    )
    held = store.update(
        first,
        event_type="FIRST_LEG_SUBMIT_STARTED",
        event_payload={"leg_id": started_leg.leg_id},
        state=GroupOrderState.FIRST_PENDING,
        legs=(started_leg, first.legs[1]),
        now_ns=_NOW + 3,
    )
    assert held.budget_reservation.state is GroupBudgetReservationState.HELD

    looser_policy = _policy(
        max_group=Decimal("100"),
        max_active=Decimal("200"),
        version=original_policy.policy_version + 1,
    )
    second_request = _as_budgeted(_request("policy-generation-second"), looser_policy)
    denied = store.create_or_get(
        second_request,
        budget_policy=looser_policy,
        now_ns=_NOW + 4,
        clock_ns=lambda: _NOW + 5,
    )

    assert denied.state is GroupOrderState.REJECTED_BEFORE_DISPATCH
    assert denied.error_code == "group_budget_policy_mismatch"
    assert denied.budget_reservation.state is GroupBudgetReservationState.DENIED
    assert store.get(
        first_request.portfolio_ref, first_request.group_id
    ).budget_reservation.state is (GroupBudgetReservationState.HELD)
    store.close()


def test_unknown_budget_hold_is_not_ttl_released_on_replay_or_reopen(tmp_path) -> None:
    request = _request("held-budget-after-deadline")
    policy = _policy(max_group=Decimal("100"), max_active=Decimal("100"))
    budgeted = _as_budgeted(request, policy)
    db_path = tmp_path / "held-budget-expiry.sqlite3"
    store = SqliteOrderGroupStore(db_path)
    initial = store.create_or_get(
        budgeted,
        budget_policy=policy,
        now_ns=_NOW + 1,
        clock_ns=lambda: _NOW + 2,
    )
    submitted_leg = replace(
        initial.legs[0],
        phase=GroupLegPhase.REQUESTING,
        submitted_intent=request.ordered_legs[0].maximum_intent,
    )
    attempted = store.update(
        initial,
        event_type="FIRST_LEG_SUBMIT_STARTED",
        event_payload={"leg_id": submitted_leg.leg_id},
        state=GroupOrderState.FIRST_PENDING,
        legs=(submitted_leg, initial.legs[1]),
        now_ns=_NOW + 3,
    )
    unknown_leg = replace(submitted_leg, phase=GroupLegPhase.UNKNOWN)
    unknown = store.update(
        attempted,
        event_type="FIRST_LEG_STATUS_OBSERVED",
        event_payload={"leg_id": unknown_leg.leg_id, "state": "UNKNOWN"},
        legs=(unknown_leg, attempted.legs[1]),
        now_ns=_NOW + 4,
    )
    unknown = store.update(
        unknown,
        event_type="GROUP_RECOVERY_REQUIRED",
        event_payload={"code": "provider_outcome_unknown"},
        state=GroupOrderState.RECOVERY_REQUIRED,
        error_code="provider_outcome_unknown",
        now_ns=_NOW + 5,
    )
    assert unknown.budget_reservation.state is GroupBudgetReservationState.HELD
    store.close()

    reopened = SqliteOrderGroupStore(db_path)
    replay = reopened.create_or_get(
        budgeted,
        budget_policy=policy,
        now_ns=_DEADLINE + 1,
        clock_ns=lambda: _DEADLINE + 2,
    )
    assert replay == unknown
    assert replay.budget_reservation.state is GroupBudgetReservationState.HELD
    reopened.close()


def test_two_processes_atomically_contend_for_last_portfolio_budget(tmp_path) -> None:
    db_path = tmp_path / "portfolio-budget-contest.sqlite3"
    bootstrap = SqliteOrderGroupStore(db_path)
    bootstrap.close()
    policy = _policy(max_group=Decimal("100"), max_active=Decimal("100"))
    requests = tuple(_as_budgeted(_request(group), policy) for group in ("contest-a", "contest-b"))
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    result_queue = context.Queue()
    workers = [
        context.Process(
            target=_reserve_worker,
            args=(
                str(db_path),
                request.to_payload(),
                policy.to_payload(),
                barrier,
                result_queue,
            ),
        )
        for request in requests
    ]
    for worker in workers:
        worker.start()
    results = [result_queue.get(timeout=20) for _ in workers]
    for worker in workers:
        worker.join(timeout=20)
        assert worker.exitcode == 0
    assert sorted(item[1] for item in results) == sorted(
        [GroupOrderState.PREPARING.value, GroupOrderState.REJECTED_BEFORE_DISPATCH.value]
    )
    assert sorted(item[2] for item in results) == sorted(
        [GroupBudgetReservationState.RESERVED.value, GroupBudgetReservationState.DENIED.value]
    )

    reopened = SqliteOrderGroupStore(db_path)
    snapshots = [reopened.get(request.portfolio_ref, request.group_id) for request in requests]
    assert all(snapshot is not None for snapshot in snapshots)
    holds = [
        snapshot.budget_reservation
        for snapshot in snapshots
        if snapshot.budget_reservation.state is GroupBudgetReservationState.RESERVED
    ]
    assert len(holds) == 1
    assert holds[0].gross_notional == Decimal("60")
    assert sum(
        (snapshot.budget_reservation.gross_notional for snapshot in snapshots), Decimal("0")
    ) == Decimal("120")  # One durable DENIED quote, one active $60 hold.
    reopened.close()


def test_version_one_rows_migrate_read_only_and_cannot_start_unbudgeted_dispatch(tmp_path) -> None:
    request = _request("legacy-v1")
    initial_reservations = tuple(
        GroupAccountReservationSnapshot(
            group_id=request.group_id,
            account_ref=account,
            scope_fingerprint="0" * 64,
            phase="NOT_STARTED",
        )
        for account in request.account_refs
    )
    snapshot = GroupOrderSnapshot(
        request=request,
        state=GroupOrderState.PREPARING,
        revision=0,
        reservations=initial_reservations,
        legs=tuple(GroupLegProgress(plan.leg_id) for plan in request.legs),
        residual_base_units=None,
        error_code=None,
        created_at_ns=_NOW,
        updated_at_ns=_NOW,
    )
    db_path = tmp_path / "legacy-v1.sqlite3"
    connection = sqlite3.connect(db_path)
    connection.executescript(
        """
        CREATE TABLE execution_group_meta(singleton INTEGER PRIMARY KEY CHECK(singleton = 1), schema_version INTEGER NOT NULL);
        INSERT INTO execution_group_meta(singleton, schema_version) VALUES(1, 1);
        CREATE TABLE order_group_snapshots(
            portfolio_ref TEXT NOT NULL, group_id TEXT NOT NULL,
            request_fingerprint TEXT NOT NULL, request_json TEXT NOT NULL,
            snapshot_json TEXT NOT NULL, snapshot_sha256 TEXT NOT NULL,
            state TEXT NOT NULL, revision INTEGER NOT NULL, deadline_ns INTEGER NOT NULL,
            created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL,
            PRIMARY KEY(portfolio_ref, group_id)
        );
        """
    )
    connection.execute(
        """INSERT INTO order_group_snapshots(
               portfolio_ref, group_id, request_fingerprint, request_json,
               snapshot_json, snapshot_sha256, state, revision, deadline_ns,
               created_at_ns, updated_at_ns
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            request.portfolio_ref,
            request.group_id,
            request.fingerprint,
            canonical_json(request.to_payload()),
            canonical_json(snapshot.to_payload()),
            payload_sha256(snapshot.to_payload()),
            snapshot.state.value,
            snapshot.revision,
            request.deadline_ns,
            _NOW,
            _NOW,
        ),
    )
    connection.commit()
    connection.close()

    store = SqliteOrderGroupStore(db_path)
    assert store.get(request.portfolio_ref, request.group_id) == snapshot
    readback = sqlite3.connect(db_path)
    assert (
        readback.execute(
            "SELECT schema_version FROM execution_group_meta WHERE singleton = 1"
        ).fetchone()[0]
        == 2
    )
    readback.close()

    new_policy = _policy(portfolio=request.portfolio_ref)
    new_request = _as_budgeted(
        _request("new-after-legacy", portfolio=request.portfolio_ref), new_policy
    )
    blocked_new = store.create_or_get(
        new_request,
        budget_policy=new_policy,
        now_ns=_NOW + 1,
        clock_ns=lambda: _NOW + 2,
    )
    assert blocked_new.state is GroupOrderState.REJECTED_BEFORE_DISPATCH
    assert blocked_new.error_code == "group_budget_legacy_hold_unproven"
    assert blocked_new.budget_reservation.state is GroupBudgetReservationState.DENIED
    assert store.get(request.portfolio_ref, request.group_id) == snapshot

    class _Port:
        risk_scope_fingerprint = payload_sha256({"scope": "legacy"})

        def __init__(self) -> None:
            self.reserve_calls = []
            self.query_calls = []

        def reserve_group_vector(self, *args, **kwargs):
            self.reserve_calls.append(args)
            raise AssertionError("legacy group must not reserve or dispatch")

        def query_group_vector(self, *args, **kwargs):
            self.query_calls.append(args)
            raise AssertionError("legacy group must remain query-only in this migration")

        def release_group_unclaimed(self, *args, **kwargs):
            raise AssertionError("migration must not release an old hold")

        def submit_group_leg(self, *args, **kwargs):
            raise AssertionError("legacy group must not dispatch")

        def query(self, *args, **kwargs):
            raise AssertionError("legacy group cannot create follow-on orders")

        def resolve_group_leg(self, *args, **kwargs):
            raise AssertionError("legacy group cannot create follow-on orders")

    port = _Port()
    coordinator = GroupOrderCoordinator(
        store,
        reservations=dict.fromkeys(request.account_refs, port),
        orders=dict.fromkeys(request.account_refs, port),
    )
    blocked = coordinator.advance_group(request.portfolio_ref, request.group_id)
    assert blocked.state is GroupOrderState.RECOVERY_REQUIRED
    assert blocked.budget_reservation is None
    assert port.reserve_calls == [] and port.query_calls == []
    store.close()
