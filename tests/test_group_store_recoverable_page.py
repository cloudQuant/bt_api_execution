"""Bounded paging for service-owned recovery scans."""

from __future__ import annotations

from decimal import Decimal

import pytest

from bt_api_execution import (
    ExecutionScope,
    GroupOrderIntent,
    GroupOrderLegPlan,
    GroupOrderSnapshot,
    GroupOrderState,
    OrderIntent,
    OrderType,
    PositionEffect,
    Side,
    SqliteOrderGroupStore,
    TimeInForce,
    group_leg_intent_id,
    payload_sha256,
)
from bt_api_execution.errors import ContractValidationError

_CREATED_AT_NS = 1_800_000_000_000_000_000
_DEADLINE_NS = _CREATED_AT_NS + 10_000_000_000
_PORTFOLIO = "portfolio-recovery-page"


def _request(group_id: str) -> GroupOrderIntent:
    account_ref = "account-recovery-page"
    scope = ExecutionScope("paper", "demo", account_ref, "strategy-recovery-page")
    digest = payload_sha256({"instrument": "FOO-USD"})
    plans = []
    for leg_id, side, quantity, price in (
        ("leg-first", Side.BUY, Decimal("2"), Decimal("10")),
        ("leg-hedge", Side.SELL, Decimal("2"), Decimal("10")),
    ):
        intent = OrderIntent(
            intent_id=group_leg_intent_id(group_id, leg_id),
            scope=scope,
            signal_id=f"{group_id}-{leg_id}",
            instrument="FOO-USD",
            side=side,
            position_effect=PositionEffect.OPEN,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=price,
            metadata_version="recovery-page-test-v1",
            time_in_force=TimeInForce.IOC,
            deadline_ns=_DEADLINE_NS,
            tags={"instrument_metadata_digest": digest},
        )
        plans.append(
            GroupOrderLegPlan(
                leg_id=leg_id,
                account_ref=account_ref,
                maximum_intent=intent,
                base_units_per_order_unit=Decimal("1"),
                quantity_step=Decimal("0.25"),
                metadata_valid_until_ns=_DEADLINE_NS,
            )
        )
    return GroupOrderIntent(
        group_id=group_id,
        portfolio_ref=_PORTFOLIO,
        strategy_id="strategy-recovery-page",
        coordinator_principal_ref="coordinator-recovery-page",
        received_at_ns=_CREATED_AT_NS,
        deadline_ns=_DEADLINE_NS,
        first_leg_id="leg-first",
        legs=tuple(plans),
    )


def _key(snapshot: GroupOrderSnapshot) -> tuple[int, str, str]:
    return (snapshot.created_at_ns, snapshot.request.portfolio_ref, snapshot.request.group_id)


def test_recoverable_pages_use_stable_cursor_and_current_state(tmp_path) -> None:
    store = SqliteOrderGroupStore(tmp_path / "groups.sqlite")
    try:
        inserted = tuple(
            store.create_or_get(
                _request(group_id),
                now_ns=_CREATED_AT_NS,
                clock_ns=lambda: _CREATED_AT_NS + 1,
            )
            for group_id in ("page-a", "page-b", "page-c", "page-d", "page-e")
        )
        assert all(item.created_at_ns == _CREATED_AT_NS for item in inserted)

        statements: list[str] = []
        store._connection.set_trace_callback(statements.append)
        first = store.list_recoverable_page(limit=2)
        store._connection.set_trace_callback(None)
        assert tuple(item.request.group_id for item in first) == ("page-a", "page-b")
        assert any(
            "ORDER_GROUP_SNAPSHOTS" in statement.upper()
            and "LIMIT 2" in " ".join(statement.upper().split())
            for statement in statements
        )

        rejected = store.get(_PORTFOLIO, "page-c")
        assert rejected is not None
        store.update(
            rejected,
            event_type="GROUP_REJECTED_BEFORE_DISPATCH",
            event_payload={"reason": "bounded-page-state-change"},
            state=GroupOrderState.REJECTED_BEFORE_DISPATCH,
            error_code="PAGE_TEST_REJECTED",
            now_ns=_CREATED_AT_NS + 2,
        )
        held = store.get(_PORTFOLIO, "page-d")
        assert held is not None
        held = store.update(
            held,
            event_type="GROUP_RECOVERY_REQUIRED",
            event_payload={"reason": "bounded-page-state-change"},
            state=GroupOrderState.RECOVERY_REQUIRED,
            error_code="PAGE_TEST_RECOVERY",
            now_ns=_CREATED_AT_NS + 3,
        )

        second = store.list_recoverable_page(after=_key(first[-1]), limit=2)
        assert tuple(item.request.group_id for item in second) == ("page-d", "page-e")
        assert second[0].state is GroupOrderState.RECOVERY_REQUIRED
        assert store.list_recoverable_page(after=_key(second[-1]), limit=2) == ()
        assert len(store.list_recoverable_page(limit=256)) == 4
        assert tuple(item.request.group_id for item in store.list_recoverable()) == (
            "page-a",
            "page-b",
            "page-d",
            "page-e",
        )
    finally:
        store.close()


@pytest.mark.parametrize("limit", [True, 0, -1, 257, 1.0, "2", None])
def test_recoverable_page_rejects_invalid_limits(tmp_path, limit) -> None:
    store = SqliteOrderGroupStore(tmp_path / "groups.sqlite")
    try:
        with pytest.raises(ContractValidationError):
            store.list_recoverable_page(limit=limit)
    finally:
        store.close()


@pytest.mark.parametrize(
    "after",
    [
        [],
        (True, "portfolio-recovery-page", "page-a"),
        (0, "portfolio-recovery-page", "page-a"),
        ((1 << 63), "portfolio-recovery-page", "page-a"),
        (1, "portfolio-recovery-page", None),
        (1, "", "page-a"),
        (1, "pörtfolio", "page-a"),
        (1, "x" * 129, "page-a"),
        (1, "portfolio-recovery-page", "bad/group"),
    ],
)
def test_recoverable_page_rejects_invalid_cursors(tmp_path, after) -> None:
    store = SqliteOrderGroupStore(tmp_path / "groups.sqlite")
    try:
        with pytest.raises(ContractValidationError):
            store.list_recoverable_page(after=after)
    finally:
        store.close()
