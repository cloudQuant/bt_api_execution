from __future__ import annotations

from decimal import Decimal

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    AdmissionRequired,
    ExecutionAdmissionContextV1,
    ExecutionAdmissionValidatorRegistry,
    ExecutionBasicAdmissionGate,
    ExecutionProviderValidatorV1,
    ExecutionScope,
    ExecutionState,
    ManagedExecutionFacade,
    OrderIntent,
    OrderType,
    PositionEffect,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
    TimeInForce,
)
from bt_api_execution.service_order_entry import SUBMIT_OPERATION
from bt_api_execution.store import WriterLease


_SCOPE = ExecutionScope("PAPER", "simulation", "account-a", "strategy-a", "20261018")
_WIRE_SCOPE = WireScope(
    "paper:exe04", account_ref="account-a", strategy_ref="strategy-a"
)
_IDENTITY = WireIdentity(WireRole.STRATEGY_CLIENT, "strategy-a-principal", "strategy-a")


class _DispatchPort:
    def __init__(self) -> None:
        self.authorized_calls: list[tuple[OrderIntent, object]] = []
        self.ordinary_calls: list[OrderIntent] = []

    def submit_authorized(self, intent: OrderIntent, authorization: object):
        self.authorized_calls.append((intent, authorization))
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

    def __call__(self, intent: OrderIntent):
        self.ordinary_calls.append(intent)
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")


class _AuthenticatedRiskGate:
    def __init__(self) -> None:
        self.authenticated_calls: list[
            tuple[OrderIntent, ExecutionAdmissionContextV1, WriterLease, object]
        ] = []
        self.legacy_calls: list[OrderIntent] = []
        self.claimed: list[tuple[str, OrderIntent]] = []
        self.settled: list[str] = []

    def reserve_for_authenticated_execution(
        self,
        intent: OrderIntent,
        *,
        admission_context: ExecutionAdmissionContextV1,
        writer_lease: WriterLease,
        dispatch_port: object,
    ) -> str:
        self.authenticated_calls.append(
            (intent, admission_context, writer_lease, dispatch_port)
        )
        return "risk-permit-1"

    def reserve(self, intent: OrderIntent) -> str:
        self.legacy_calls.append(intent)
        return "legacy-risk-permit"

    def claim_for_dispatch(self, permit_reference: str, intent: OrderIntent) -> None:
        self.claimed.append((permit_reference, intent))

    def validate(self, permit_reference: str, intent: OrderIntent) -> None:
        assert permit_reference == "legacy-risk-permit"

    def settle(self, permit_reference: str) -> None:
        self.settled.append(permit_reference)

    def release(self, permit_reference: str, reason: str) -> None:
        del permit_reference, reason


class _LegacyRiskGate:
    def __init__(self) -> None:
        self.reserved: list[OrderIntent] = []
        self.validated: list[tuple[str, OrderIntent]] = []
        self.settled: list[str] = []

    def reserve(self, intent: OrderIntent) -> str:
        self.reserved.append(intent)
        return "legacy-permit-1"

    def validate(self, permit_reference: str, intent: OrderIntent) -> None:
        self.validated.append((permit_reference, intent))

    def settle(self, permit_reference: str) -> None:
        self.settled.append(permit_reference)

    def release(self, permit_reference: str, reason: str) -> None:
        del permit_reference, reason


def _intent(*, instrument: str = "BTC-USDT", intent_id: str = "authenticated-order-1") -> OrderIntent:
    return OrderIntent.limit(
        intent_id=intent_id,
        scope=_SCOPE,
        signal_id=intent_id,
        instrument=instrument,
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("50000"),
        position_effect=PositionEffect.OPEN,
        time_in_force=TimeInForce.GTC,
        metadata_version="metadata-v1",
    )


def _context() -> ExecutionAdmissionContextV1:
    return ExecutionAdmissionContextV1(
        _IDENTITY.principal_ref,
        _IDENTITY,
        _WIRE_SCOPE,
        _SCOPE,
        SUBMIT_OPERATION,
    )


def _basic_gate(store: SqliteExecutionStore, port: _DispatchPort) -> ExecutionBasicAdmissionGate:
    validator = ExecutionProviderValidatorV1(
        provider_id="PAPER",
        validator_id="paper-policy",
        version="v1",
        dispatch_port=port,
        instrument_map={"BTC-USDT": "BTCUSDT"},
        quantity_steps={"BTC-USDT": Decimal("1")},
        max_quantities={"BTC-USDT": Decimal("100")},
        price_ticks={"BTC-USDT": Decimal("0.5")},
        supported_order_types=(OrderType.LIMIT,),
        supported_time_in_force=(TimeInForce.GTC,),
        supported_actions=((Side.BUY, PositionEffect.OPEN),),
        supports_reduce_only=False,
    )
    return ExecutionBasicAdmissionGate(
        store,
        ExecutionAdmissionValidatorRegistry((validator,)),
        clock_ns=lambda: 1_000_000_000,
    )


@pytest.mark.unit
def test_authenticated_risk_reservation_receives_exact_basic_context_and_owner_objects(
    tmp_path,
) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    port = _DispatchPort()
    basic_gate = _basic_gate(store, port)
    risk_gate = _AuthenticatedRiskGate()
    facade = ManagedExecutionFacade(
        store,
        _SCOPE,
        writer_id="authenticated-writer",
        admission_gate=risk_gate,
        basic_admission_gate=basic_gate,
    )
    context = _context()
    intent = _intent()
    try:
        result = facade.submit(intent, port, admission_context=context)

        assert result.state is ExecutionState.ACKED
        assert len(risk_gate.authenticated_calls) == 1
        received_intent, received_context, writer_lease, received_port = (
            risk_gate.authenticated_calls[0]
        )
        assert received_intent is intent
        assert received_context is context
        assert writer_lease is facade._last_writer_lease
        assert received_port is port
        assert risk_gate.legacy_calls == []
        assert risk_gate.claimed == [("risk-permit-1", intent)]
        assert len(port.authorized_calls) == 1
        assert port.authorized_calls[0][0] is intent
    finally:
        facade.close()
        store.close()


@pytest.mark.unit
def test_authenticated_risk_reservation_is_not_called_without_basic_context(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    port = _DispatchPort()
    risk_gate = _AuthenticatedRiskGate()
    facade = ManagedExecutionFacade(
        store,
        _SCOPE,
        writer_id="missing-context-writer",
        admission_gate=risk_gate,
        basic_admission_gate=_basic_gate(store, port),
    )
    try:
        intent = _intent()
        with pytest.raises(AdmissionRequired):
            facade.submit(intent, port)

        assert risk_gate.authenticated_calls == []
        assert risk_gate.legacy_calls == []
        assert port.authorized_calls == []
        assert store.get_intent(intent.intent_id, scope=intent.scope) is None
    finally:
        facade.close()
        store.close()


@pytest.mark.unit
def test_basic_admission_denial_precedes_authenticated_risk_reservation(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    port = _DispatchPort()
    risk_gate = _AuthenticatedRiskGate()
    facade = ManagedExecutionFacade(
        store,
        _SCOPE,
        writer_id="denied-basic-writer",
        admission_gate=risk_gate,
        basic_admission_gate=_basic_gate(store, port),
    )
    try:
        result = facade.submit(
            _intent(instrument="ETH-USDT", intent_id="unsupported-instrument"),
            port,
            admission_context=_context(),
        )

        assert result.state is ExecutionState.BLOCKED
        assert risk_gate.authenticated_calls == []
        assert risk_gate.legacy_calls == []
        assert port.authorized_calls == []
    finally:
        facade.close()
        store.close()


@pytest.mark.unit
def test_authenticated_reservation_gate_fails_closed_without_basic_gate(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    port = _DispatchPort()
    risk_gate = _AuthenticatedRiskGate()
    facade = ManagedExecutionFacade(
        store,
        _SCOPE,
        writer_id="missing-basic-gate-writer",
        admission_gate=risk_gate,
    )
    try:
        result = facade.submit(_intent(), port, admission_context=_context())

        assert result.state is ExecutionState.BLOCKED
        assert risk_gate.authenticated_calls == []
        assert risk_gate.legacy_calls == []
        assert port.ordinary_calls == []
    finally:
        facade.close()
        store.close()


@pytest.mark.unit
def test_gate_without_authenticated_seam_keeps_legacy_reserve_path(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    port = _DispatchPort()
    risk_gate = _LegacyRiskGate()
    facade = ManagedExecutionFacade(
        store,
        _SCOPE,
        writer_id="legacy-risk-writer",
        admission_gate=risk_gate,
    )
    intent = _intent()
    try:
        result = facade.submit(intent, port)

        assert result.state is ExecutionState.ACKED
        assert risk_gate.reserved == [intent]
        assert risk_gate.validated == [("legacy-permit-1", intent)]
        assert risk_gate.settled == ["legacy-permit-1"]
        assert len(port.ordinary_calls) == 1
    finally:
        facade.close()
        store.close()
