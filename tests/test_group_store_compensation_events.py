"""Durable event vocabulary for service-owned group compensation."""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal

import pytest

from bt_api_execution import (
    ExecutionScope,
    GroupOrderIntent,
    GroupOrderLegPlan,
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
from bt_api_execution.errors import ContractValidationError, DurableStoreError


_CREATED_AT_NS = 1_800_000_000_000_000_000
_DEADLINE_NS = _CREATED_AT_NS + 10_000_000_000
_PORTFOLIO = "portfolio-compensation-events"


def _request(group_id: str) -> GroupOrderIntent:
    account_ref = "account-compensation-events"
    scope = ExecutionScope("paper", "demo", account_ref, "strategy-compensation-events")
    digest = payload_sha256({"instrument": "FOO-USD"})
    plans = []
    for leg_id, side in (("leg-first", Side.BUY), ("leg-hedge", Side.SELL)):
        intent = OrderIntent(
            intent_id=group_leg_intent_id(group_id, leg_id),
            scope=scope,
            signal_id=f"{group_id}-{leg_id}",
            instrument="FOO-USD",
            side=side,
            position_effect=PositionEffect.OPEN,
            order_type=OrderType.LIMIT,
            quantity=Decimal("2"),
            price=Decimal("10"),
            metadata_version="compensation-events-test-v1",
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
        strategy_id="strategy-compensation-events",
        coordinator_principal_ref="coordinator-compensation-events",
        received_at_ns=_CREATED_AT_NS,
        deadline_ns=_DEADLINE_NS,
        first_leg_id="leg-first",
        legs=tuple(plans),
    )


def test_compensation_events_are_durable_and_recovery_can_enter_unwinding(tmp_path) -> None:
    path = tmp_path / "group-compensation.sqlite"
    store = SqliteOrderGroupStore(path)
    request = _request("group-compensation-events-01")
    try:
        snapshot = store.create_or_get(request, now_ns=_CREATED_AT_NS)
        snapshot = store.update(
            snapshot,
            event_type="GROUP_RECOVERY_REQUIRED",
            event_payload={"code": "chain_residual_unresolved"},
            state=GroupOrderState.RECOVERY_REQUIRED,
            now_ns=_CREATED_AT_NS + 1,
        )
        action = {
            "action_id": "compensation-action-0001",
            "ordinal": 1,
            "authorization_digest": payload_sha256({"authorization": "fixed"}),
        }
        snapshot = store.update(
            snapshot,
            event_type="GROUP_COMPENSATION_PLAN_RECORDED",
            event_payload={"actions": [action]},
            state=GroupOrderState.UNWINDING,
            now_ns=_CREATED_AT_NS + 2,
        )
        snapshot = store.update(
            snapshot,
            event_type="GROUP_COMPENSATION_SUBMIT_STARTED",
            event_payload={**action, "client_order_id": "client-comp-0001"},
            now_ns=_CREATED_AT_NS + 3,
        )
        snapshot = store.update(
            snapshot,
            event_type="GROUP_COMPENSATION_STATUS_OBSERVED",
            event_payload={**action, "state": "UNKNOWN"},
            state=GroupOrderState.RECOVERY_REQUIRED,
            now_ns=_CREATED_AT_NS + 4,
        )
        # Recovery can resume the durable unwind without returning to the
        # opening-leg progression or changing the accepted group intent.
        snapshot = store.update(
            snapshot,
            event_type="GROUP_COMPENSATION_STATUS_OBSERVED",
            event_payload={**action, "state": "FILLED"},
            state=GroupOrderState.UNWINDING,
            now_ns=_CREATED_AT_NS + 5,
        )
        snapshot = store.update(
            snapshot,
            event_type="GROUP_COMPENSATION_RESOLVED",
            event_payload={**action, "resolution_kind": "TERMINAL_CLOSE_RESOLVED"},
            now_ns=_CREATED_AT_NS + 6,
        )
        expected_revision = snapshot.revision
        expected_request = request.to_payload()
    finally:
        store.close()

    reopened = SqliteOrderGroupStore(path)
    try:
        recovered = reopened.get(_PORTFOLIO, request.group_id)
        assert recovered is not None
        assert recovered.request.to_payload() == expected_request
        assert recovered.revision == expected_revision
        assert recovered.state is GroupOrderState.UNWINDING
        events = reopened.events(_PORTFOLIO, request.group_id)
        assert tuple(item.event_type for item in events[-5:]) == (
            "GROUP_COMPENSATION_PLAN_RECORDED",
            "GROUP_COMPENSATION_SUBMIT_STARTED",
            "GROUP_COMPENSATION_STATUS_OBSERVED",
            "GROUP_COMPENSATION_STATUS_OBSERVED",
            "GROUP_COMPENSATION_RESOLVED",
        )
        assert events[-1].payload["resolution_kind"] == "TERMINAL_CLOSE_RESOLVED"
    finally:
        reopened.close()


@pytest.mark.parametrize(
    ("event_type", "target_state"),
    (
        ("GROUP_COMPENSATION_SUBMIT_STARTED", GroupOrderState.UNWINDING),
        ("GROUP_COMPENSATION_RESOLVED", GroupOrderState.RECOVERY_REQUIRED),
        ("FIRST_LEG_QUERY_STARTED", GroupOrderState.UNWINDING),
    ),
)
def test_recovery_unwinding_requires_the_matching_compensation_phase(
    tmp_path, event_type, target_state
) -> None:
    store = SqliteOrderGroupStore(tmp_path / f"{event_type}.sqlite")
    request = _request(f"group-invalid-{event_type.lower().replace('_', '-')}")
    try:
        snapshot = store.create_or_get(request, now_ns=_CREATED_AT_NS)
        snapshot = store.update(
            snapshot,
            event_type="GROUP_RECOVERY_REQUIRED",
            event_payload={"code": "chain_residual_unresolved"},
            state=GroupOrderState.RECOVERY_REQUIRED,
            now_ns=_CREATED_AT_NS + 1,
        )
        with pytest.raises(ContractValidationError):
            store.update(
                snapshot,
                event_type=event_type,
                event_payload={"action_id": "action-invalid-01"},
                state=target_state,
                now_ns=_CREATED_AT_NS + 2,
            )

        persisted = store.get(_PORTFOLIO, request.group_id)
        assert persisted is not None
        assert persisted.state is GroupOrderState.RECOVERY_REQUIRED
        assert persisted.revision == snapshot.revision
        assert tuple(event.event_type for event in store.events(_PORTFOLIO, request.group_id)) == (
            "GROUP_ACCEPTED",
            "GROUP_RECOVERY_REQUIRED",
        )
    finally:
        store.close()


def test_compensation_event_digest_is_checked_after_reopen(tmp_path) -> None:
    path = tmp_path / "group-compensation-tamper.sqlite"
    store = SqliteOrderGroupStore(path)
    request = _request("group-compensation-tamper-01")
    try:
        snapshot = store.create_or_get(request, now_ns=_CREATED_AT_NS)
        store.update(
            snapshot,
            event_type="GROUP_RECOVERY_REQUIRED",
            event_payload={"code": "chain_residual_unresolved"},
            state=GroupOrderState.RECOVERY_REQUIRED,
            now_ns=_CREATED_AT_NS + 1,
        )
    finally:
        store.close()

    # Simulate an out-of-band edit to a persisted event while leaving its
    # immutable digest untouched. Reopening must not trust the altered row.
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER order_group_events_immutable_update")
        connection.execute(
            "UPDATE order_group_events SET payload_json = ? WHERE group_id = ? AND revision = ?",
            (json.dumps({"code": "tampered"}, separators=(",", ":")), request.group_id, 1),
        )

    reopened = SqliteOrderGroupStore(path)
    try:
        with pytest.raises(DurableStoreError, match="stored group event is unreadable"):
            reopened.events(_PORTFOLIO, request.group_id)
    finally:
        reopened.close()
