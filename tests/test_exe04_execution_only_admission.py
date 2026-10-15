"""Execution-only admission boundary coverage for EXE04.

These tests exercise the execution package's structural admission port.  The
fixture deliberately models only the permit facts needed at this boundary;
it does not import or claim to test a risk policy implementation.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    CancelObservation,
    ExecutionScope,
    ExecutionState,
    OrderIntent,
    ProviderObservation,
    SqliteExecutionStore,
)
from bt_api_execution.service_order_entry import SUBMIT_OPERATION, ExecutionOrderEntryService

_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
_EXECUTION_SRC = _PACKAGE_ROOT / "src"
_BASE_SRC = _PACKAGE_ROOT.parent / "bt_api_base" / "src"
_WIRE_SCOPE = WireScope("paper:exe04", account_ref="account-a", strategy_ref="strategy-a")
_EXECUTION_SCOPE = ExecutionScope("PAPER", "simulation", "account-a", "strategy-a", "20261015")
_IDENTITY = WireIdentity(WireRole.STRATEGY_CLIENT, "exe04-principal", "strategy-a")

_COLD_IMPORT = r"""
import importlib.abc
import sys

class RejectRiskPackage(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "bt_api_risk" or fullname.startswith("bt_api_risk."):
            raise AssertionError("execution import attempted to load bt_api_risk")
        return None

sys.meta_path.insert(0, RejectRiskPackage())
sys.path[:0] = sys.argv[1:3]
import bt_api_execution
from bt_api_execution.service_order_entry import ExecutionOrderEntryService, SUBMIT_OPERATION

assert not any(
    name == "bt_api_risk" or name.startswith("bt_api_risk.")
    for name in sys.modules
)
"""


@dataclass(frozen=True)
class _Permit:
    permit_id: str
    intent_id: str
    scope: ExecutionScope
    action: tuple[str, str]
    expires_at_ns: int
    intent_fingerprint: str


class _Clock:
    def __init__(self, now_ns: int = 1_000_000_000) -> None:
        self.now_ns = now_ns


class _StructuralAdmissionGate:
    """Deterministic structural port that enforces the EXE04 permit facts."""

    def __init__(
        self,
        store: SqliteExecutionStore,
        clock: _Clock,
        *,
        permit_scope: ExecutionScope | None = None,
        permit_action: tuple[str, str] | None = None,
        expire_during_claim: bool = False,
    ) -> None:
        self.store = store
        self.clock = clock
        self.permit_scope = permit_scope
        self.permit_action = permit_action
        self.expire_during_claim = expire_during_claim
        self.permits: dict[str, _Permit] = {}
        self.calls: list[tuple[str, str, ExecutionState]] = []
        self.released: list[tuple[str, str]] = []
        self.settled: list[str] = []

    def reserve(self, intent: OrderIntent) -> _Permit:
        record = self.store.get(intent.intent_id, scope=intent.scope)
        assert record is not None and record.state is ExecutionState.PENDING_ADMISSION
        self.calls.append(("reserve", intent.intent_id, record.state))
        permit_id = f"exe04-permit-{intent.intent_id}"
        permit = _Permit(
            permit_id=permit_id,
            intent_id=intent.intent_id,
            scope=self.permit_scope or intent.scope,
            action=self.permit_action or (intent.side.value, intent.position_effect.value),
            expires_at_ns=self.clock.now_ns + 100,
            intent_fingerprint=intent.fingerprint,
        )
        self.permits[permit_id] = permit
        return permit

    def claim_for_dispatch(self, permit_id: str, intent: OrderIntent) -> _Permit:
        record = self.store.get(intent.intent_id, scope=intent.scope)
        assert record is not None and record.state is ExecutionState.DISPATCHING
        self.calls.append(("claim", intent.intent_id, record.state))
        if self.expire_during_claim:
            self.clock.now_ns = self.permits[permit_id].expires_at_ns
        return self.validate(permit_id, intent)

    def validate(self, permit_id: str, intent: OrderIntent) -> _Permit:
        record = self.store.get(intent.intent_id, scope=intent.scope)
        assert record is not None and record.state is ExecutionState.DISPATCHING
        self.calls.append(("validate", intent.intent_id, record.state))
        permit = self.permits[permit_id]
        if permit.intent_id != intent.intent_id:
            raise PermissionError("PERMIT_INTENT_MISMATCH")
        if permit.intent_fingerprint != intent.fingerprint:
            raise PermissionError("PERMIT_PAYLOAD_MISMATCH")
        if permit.scope.account_key != intent.scope.account_key:
            raise PermissionError("PERMIT_ACCOUNT_MISMATCH")
        if permit.action != (intent.side.value, intent.position_effect.value):
            raise PermissionError("PERMIT_ACTION_MISMATCH")
        if permit.expires_at_ns <= self.clock.now_ns:
            raise PermissionError("PERMIT_EXPIRED")
        return permit

    def release(self, permit_id: str, reason: str) -> None:
        self.released.append((permit_id, reason))

    def settle(self, permit_id: str) -> None:
        self.settled.append(permit_id)


class _SyntheticProvider:
    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.cancelled: list[str] = []

    def submit(self, intent: OrderIntent) -> ProviderObservation:
        self.submitted.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, f"provider-{intent.intent_id}")

    def cancel(self, intent) -> CancelObservation:
        self.cancelled.append(intent.cancel_id)
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )


def _arguments(intent_id: str = "exe04-order") -> dict[str, object]:
    return {
        "intent_id": intent_id,
        "signal_id": f"signal-{intent_id}",
        "instrument": "BTC-USDT",
        "side": "BUY",
        "position_effect": "OPEN",
        "order_type": "LIMIT",
        "quantity": "2",
        "price": "50000",
        "time_in_force": "GTC",
        "reduce_only": False,
        "metadata_version": "metadata-v1",
        "deadline_ns": None,
        "tags": {},
    }


def _service(
    store: SqliteExecutionStore,
    provider: _SyntheticProvider,
    *,
    gate: _StructuralAdmissionGate | None,
    allow_unprotected: bool = False,
    writer_id: str = "exe04-order-entry",
    lease_ttl_ns: int = 5_000_000_000,
) -> ExecutionOrderEntryService:
    return ExecutionOrderEntryService(
        store,
        {
            WireScope(
                "paper:exe04", account_ref="account-a", strategy_ref="strategy-a", run_id=None
            ): _EXECUTION_SCOPE
        },
        provider,
        admission_gate=gate,
        allow_unprotected=allow_unprotected,
        writer_id=writer_id,
        lease_ttl_ns=lease_ttl_ns,
    )


def _submit(service: ExecutionOrderEntryService, arguments: dict[str, object]):
    return service.handle(
        SUBMIT_OPERATION,
        principal_ref="exe04-principal",
        identity=_IDENTITY,
        scope=_WIRE_SCOPE,
        arguments=arguments,
    )


@pytest.mark.unit
def test_cold_execution_import_does_not_load_risk_package() -> None:
    result = subprocess.run(  # noqa: S603 - fixed interpreter and source roots
        [
            sys.executable,
            "-c",
            _COLD_IMPORT,
            str(_EXECUTION_SRC),
            str(_BASE_SRC),
        ],
        cwd=_PACKAGE_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.unit
def test_execution_only_valid_admission_dispatches_through_real_entry_and_store(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _SyntheticProvider()
    gate = _StructuralAdmissionGate(store, _Clock())
    service = _service(store, provider, gate=gate)
    try:
        reply = _submit(service, _arguments())
        record = store.get("exe04-order", scope=_EXECUTION_SCOPE)

        assert reply["ok"] is True and reply["code"] == "OK"
        assert record is not None and record.state is ExecutionState.ACKED
        assert record.permit_reference == "exe04-permit-exe04-order"
        assert gate.calls == [
            ("reserve", "exe04-order", ExecutionState.PENDING_ADMISSION),
            ("claim", "exe04-order", ExecutionState.DISPATCHING),
            ("validate", "exe04-order", ExecutionState.DISPATCHING),
        ]
        assert provider.submitted == ["exe04-order"]
        assert gate.settled == ["exe04-permit-exe04-order"]
        assert provider.cancelled == []
    finally:
        store.close()


@pytest.mark.unit
def test_missing_admission_gate_fails_closed_and_replay_never_dispatches(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _SyntheticProvider()
    service = _service(store, provider, gate=None)
    try:
        first = _submit(service, _arguments())
        replay = _submit(service, _arguments())
        record = store.get("exe04-order", scope=_EXECUTION_SCOPE)

        assert first["ok"] is False and first["code"] == "ADMISSION_REQUIRED"
        assert replay["ok"] is False and replay["code"] == "ADMISSION_REQUIRED"
        assert record is not None and record.state is ExecutionState.PENDING_ADMISSION
        assert provider.submitted == []
        assert provider.cancelled == []
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize("failure", ["expired", "wrong_account", "wrong_action"])
def test_invalid_structural_permit_is_blocked_before_provider_and_not_retried(
    tmp_path, failure: str
) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _SyntheticProvider()
    clock = _Clock()
    if failure == "wrong_account":
        permit_scope = ExecutionScope("PAPER", "simulation", "account-b", "strategy-a", "20261015")
        gate = _StructuralAdmissionGate(store, clock, permit_scope=permit_scope)
    elif failure == "wrong_action":
        gate = _StructuralAdmissionGate(store, clock, permit_action=("SELL", "CLOSE"))
    else:
        gate = _StructuralAdmissionGate(store, clock, expire_during_claim=True)
    service = _service(store, provider, gate=gate)
    try:
        first = _submit(service, _arguments())
        first_events = list(gate.calls)
        replay = _submit(service, _arguments())
        record = store.get("exe04-order", scope=_EXECUTION_SCOPE)

        assert first["ok"] is True
        assert first["result"]["record"]["state"] == ExecutionState.BLOCKED.value
        assert replay == first
        assert record is not None and record.state is ExecutionState.BLOCKED
        assert first_events == [
            ("reserve", "exe04-order", ExecutionState.PENDING_ADMISSION),
            ("claim", "exe04-order", ExecutionState.DISPATCHING),
            ("validate", "exe04-order", ExecutionState.DISPATCHING),
        ]
        assert gate.calls == first_events
        assert gate.released == [("exe04-permit-exe04-order", "admission_claim_failed")]
        assert provider.submitted == []
        assert provider.cancelled == []
    finally:
        store.close()


@pytest.mark.unit
def test_stale_owner_generation_cannot_activate_or_dispatch_after_handoff(
    tmp_path, monkeypatch
) -> None:
    from bt_api_execution import store as store_module

    clock = _Clock()
    monkeypatch.setattr(store_module.time, "time_ns", lambda: clock.now_ns)
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _SyntheticProvider()

    class HandoffAdmissionGate(_StructuralAdmissionGate):
        contender_lease = None

        def reserve(self, intent: OrderIntent) -> _Permit:
            permit = super().reserve(intent)
            clock.now_ns += 5_000_000
            self.contender_lease = store.acquire_or_renew_lease(
                intent.scope, "exe04-new-owner", ttl_ns=5_000_000_000
            )
            return permit

    gate = HandoffAdmissionGate(store, clock)
    service = _service(
        store,
        provider,
        gate=gate,
        writer_id="exe04-old-owner",
        lease_ttl_ns=5_000_000,
    )
    try:
        first = _submit(service, _arguments())
        replay = _submit(service, _arguments())
        record = store.get("exe04-order", scope=_EXECUTION_SCOPE)

        assert gate.contender_lease is not None
        assert gate.contender_lease.owner_id == "exe04-new-owner"
        assert gate.contender_lease.fencing_token == 2
        assert first["ok"] is False and first["code"] == "OWNER_LEASE_UNAVAILABLE"
        assert replay["ok"] is False and replay["code"] == "OWNER_LEASE_UNAVAILABLE"
        assert record is not None and record.state is ExecutionState.PENDING_ADMISSION
        assert gate.calls == [("reserve", "exe04-order", ExecutionState.PENDING_ADMISSION)]
        assert provider.submitted == []
        assert provider.cancelled == []
    finally:
        store.close()
