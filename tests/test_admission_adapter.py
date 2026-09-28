from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest

from bt_api_execution import ExecutionScope, OrderIntent, SharedRiskAdmissionAdapter, Side


@dataclass(frozen=True)
class _Permit:
    permit_id: str


class _ExternalRiskGate:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def reserve(self, intent: object) -> _Permit:
        self.calls.append(("reserve", intent))
        return _Permit("permit-1")

    def validate_permit(self, permit_id: str, intent: object) -> _Permit:
        self.calls.append(("validate", (permit_id, intent)))
        return _Permit(permit_id)

    def claim_for_dispatch(self, permit_id: str, intent: object) -> _Permit:
        self.calls.append(("claim", (permit_id, intent)))
        return _Permit(permit_id)

    def settle(self, permit_id: str) -> _Permit:
        self.calls.append(("settle", permit_id))
        return _Permit(permit_id)

    def release(self, permit_id: str, reason: str) -> None:
        self.calls.append(("release", (permit_id, reason)))


@pytest.mark.unit
def test_shared_risk_adapter_keeps_risk_dto_mapping_outside_execution() -> None:
    scope = ExecutionScope("FAKE", "simulation", "acct_demo", "strategy.demo")
    intent = OrderIntent.limit(
        intent_id="signal-1",
        scope=scope,
        signal_id="signal-1",
        instrument="BTC-USDT",
        side=Side.BUY,
        quantity=Decimal("1"),
        price=Decimal("50000"),
    )
    gate = _ExternalRiskGate()
    adapter = SharedRiskAdmissionAdapter(
        gate,
        lambda value: {"risk_id": value.intent_id, "account": value.scope.account_ref},
    )

    permit = adapter.reserve(intent)
    adapter.validate(permit.permit_id, intent)
    adapter.claim_for_dispatch(permit.permit_id, intent)
    adapter.settle(permit.permit_id)
    adapter.release(permit.permit_id, "not_dispatched")

    assert gate.calls == [
        ("reserve", {"risk_id": "signal-1", "account": "acct_demo"}),
        ("validate", ("permit-1", {"risk_id": "signal-1", "account": "acct_demo"})),
        ("claim", ("permit-1", {"risk_id": "signal-1", "account": "acct_demo"})),
        ("settle", "permit-1"),
        ("release", ("permit-1", "not_dispatched")),
    ]
