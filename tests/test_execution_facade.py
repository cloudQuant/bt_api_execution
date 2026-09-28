from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest

from bt_api_execution import (
    AdmissionDenied,
    AdmissionRequired,
    ContractValidationError,
    ExecutionScope,
    ExecutionState,
    IntentConflictError,
    ManagedExecutionFacade,
    OrderIntent,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
    WriterLeaseUnavailable,
)


@dataclass
class _Permit:
    permit_id: str


class _Gate:
    def __init__(self, *, deny: bool = False, fail_validate: bool = False) -> None:
        self.deny = deny
        self.fail_validate = fail_validate
        self.reserved: list[str] = []
        self.settled: list[str] = []
        self.released: list[tuple[str, str]] = []

    def reserve(self, intent: OrderIntent) -> _Permit:
        if self.deny:
            raise AdmissionDenied()
        self.reserved.append(intent.intent_id)
        return _Permit(f"permit-{intent.intent_id}")

    def validate(self, permit_id: str, intent: OrderIntent) -> _Permit:
        if self.fail_validate:
            raise PermissionError(permit_id)
        assert permit_id == f"permit-{intent.intent_id}"
        return _Permit(permit_id)

    def release(self, permit_id: str, reason: str) -> None:
        self.released.append((permit_id, reason))

    def settle(self, permit_id: str) -> None:
        self.settled.append(permit_id)


class _AtomicGate(_Gate):
    def __init__(self, *, fail_claim: bool = False) -> None:
        super().__init__()
        self.fail_claim = fail_claim
        self.claimed: list[tuple[str, str]] = []

    def claim_for_dispatch(self, permit_id: str, intent: OrderIntent) -> _Permit:
        if self.fail_claim:
            raise PermissionError(permit_id)
        self.claimed.append((permit_id, intent.intent_id))
        return _Permit(permit_id)


def _scope() -> ExecutionScope:
    return ExecutionScope("FAKE", "simulation", "acct_demo", "strategy.demo", "20260922")


def _intent(*, intent_id: str = "signal-1") -> OrderIntent:
    return OrderIntent.limit(
        intent_id=intent_id,
        scope=_scope(),
        signal_id="signal-1",
        instrument="BTC-USDT",
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("50000"),
        metadata_version="metadata-v1",
    )


def _intent_for_scope(scope: ExecutionScope, *, intent_id: str = "signal-1") -> OrderIntent:
    return OrderIntent.limit(
        intent_id=intent_id,
        scope=scope,
        signal_id="signal-1",
        instrument="BTC-USDT",
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("50000"),
        metadata_version="metadata-v1",
    )


def _facade(tmp_path, gate: _Gate | None = None, *, writer_id: str = "writer-a"):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    return store, ManagedExecutionFacade(store, _scope(), writer_id=writer_id, admission_gate=gate)


@pytest.mark.unit
def test_submit_persists_intent_and_claim_before_local_provider_port(tmp_path) -> None:
    gate = _Gate()
    store, facade = _facade(tmp_path, gate)
    seen_states: list[ExecutionState] = []

    def provider(intent: OrderIntent) -> ProviderObservation:
        record = store.get(intent.intent_id)
        assert record is not None
        seen_states.append(record.state)
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

    try:
        record = facade.submit(_intent(), provider)

        assert record.state is ExecutionState.ACKED
        assert seen_states == [ExecutionState.DISPATCHING]
        assert gate.reserved == ["signal-1"]
        assert [event.event_type for event in store.read_outbox()] == [
            "intent_recorded",
            "intent_admitted",
            "dispatch_claimed",
            "provider_observation",
        ]
    finally:
        store.close()


@pytest.mark.unit
def test_duplicate_identical_intent_never_dispatches_twice(tmp_path) -> None:
    gate = _Gate()
    store, facade = _facade(tmp_path, gate)
    calls: list[str] = []

    def provider(intent: OrderIntent) -> ProviderObservation:
        calls.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

    try:
        assert facade.submit(_intent(), provider).state is ExecutionState.ACKED
        assert facade.submit(_intent(), provider).state is ExecutionState.ACKED

        assert calls == ["signal-1"]
        assert gate.reserved == ["signal-1"]
        assert len(store.read_outbox()) == 4
    finally:
        store.close()


@pytest.mark.unit
def test_same_intent_id_with_different_payload_is_rejected_before_provider(tmp_path) -> None:
    gate = _Gate()
    store, facade = _facade(tmp_path, gate)
    calls = 0

    def provider(intent: OrderIntent) -> ProviderObservation:
        nonlocal calls
        calls += 1
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

    try:
        facade.submit(_intent(), provider)
        changed = OrderIntent.limit(
            intent_id="signal-1",
            scope=_scope(),
            signal_id="signal-1",
            instrument="BTC-USDT",
            side=Side.BUY,
            quantity=Decimal("3"),
            price=Decimal("50000"),
            metadata_version="metadata-v1",
        )

        with pytest.raises(IntentConflictError):
            facade.submit(changed, provider)
        assert calls == 1
    finally:
        store.close()


@pytest.mark.unit
def test_dispatch_failure_latches_unknown_and_reconcile_does_not_replay(tmp_path) -> None:
    gate = _Gate()
    store, facade = _facade(tmp_path, gate)
    calls = 0

    def failing_provider(intent: OrderIntent) -> ProviderObservation:
        nonlocal calls
        calls += 1
        raise TimeoutError(intent.intent_id)

    try:
        assert facade.submit(_intent(), failing_provider).state is ExecutionState.UNKNOWN
        assert facade.submit(_intent(), failing_provider).state is ExecutionState.UNKNOWN
        assert calls == 1

        resolved = facade.reconcile(
            ProviderObservation(
                "signal-1",
                ExecutionState.FILLED,
                provider_order_id="provider-order-1",
                filled_quantity=Decimal("2"),
                average_price=Decimal("49999"),
            )
        )
        assert resolved.state is ExecutionState.FILLED
        assert gate.settled == ["permit-signal-1"]
    finally:
        store.close()


@pytest.mark.unit
def test_invalid_provider_evidence_latches_unknown_without_second_dispatch(tmp_path) -> None:
    gate = _Gate()
    store, facade = _facade(tmp_path, gate)
    calls: list[str] = []

    def invalid_provider(intent: OrderIntent) -> ProviderObservation:
        calls.append(intent.intent_id)
        return ProviderObservation(
            intent.intent_id,
            ExecutionState.FILLED,
            provider_order_id="provider-order-1",
            filled_quantity=Decimal("1"),
        )

    try:
        assert facade.submit(_intent(), invalid_provider).state is ExecutionState.UNKNOWN
        assert facade.submit(_intent(), invalid_provider).state is ExecutionState.UNKNOWN
        assert calls == ["signal-1"]
        assert gate.settled == []
    finally:
        store.close()


@pytest.mark.unit
def test_admission_denial_and_missing_gate_both_prevent_provider_dispatch(tmp_path) -> None:
    denied_store, denied_facade = _facade(tmp_path / "denied", _Gate(deny=True))
    plain_store = SqliteExecutionStore(tmp_path / "plain" / "execution.sqlite3")
    plain_facade = ManagedExecutionFacade(plain_store, _scope(), writer_id="writer-b")
    calls: list[str] = []

    def provider(intent: OrderIntent) -> ProviderObservation:
        calls.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

    try:
        assert denied_facade.submit(_intent(), provider).state is ExecutionState.REJECTED
        with pytest.raises(AdmissionRequired):
            plain_facade.submit(_intent(intent_id="signal-2"), provider)

        assert calls == []
        record = plain_store.get("signal-2")
        assert record is not None and record.state is ExecutionState.PENDING_ADMISSION
    finally:
        denied_store.close()
        plain_store.close()


@pytest.mark.unit
def test_final_admission_validation_failure_blocks_before_provider_port(tmp_path) -> None:
    gate = _Gate(fail_validate=True)
    store, facade = _facade(tmp_path, gate)
    calls: list[str] = []

    def provider(intent: OrderIntent) -> ProviderObservation:
        calls.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

    try:
        record = facade.submit(_intent(), provider)

        assert record.state is ExecutionState.BLOCKED
        assert record.review_required is True
        assert calls == []
        assert gate.released == [("permit-signal-1", "admission_validation_failed")]
        assert [event.event_type for event in store.read_outbox()] == [
            "intent_recorded",
            "intent_admitted",
            "dispatch_claimed",
            "dispatch_blocked",
        ]
    finally:
        store.close()


@pytest.mark.unit
def test_atomic_admission_claim_precedes_the_provider_port(tmp_path) -> None:
    gate = _AtomicGate()
    store, facade = _facade(tmp_path, gate)
    calls: list[str] = []

    def provider(intent: OrderIntent) -> ProviderObservation:
        assert gate.claimed == [("permit-signal-1", "signal-1")]
        calls.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

    try:
        record = facade.submit(_intent(), provider)

        assert record.state is ExecutionState.ACKED
        assert calls == ["signal-1"]
        assert gate.claimed == [("permit-signal-1", "signal-1")]
    finally:
        store.close()


@pytest.mark.unit
def test_atomic_admission_claim_failure_blocks_before_provider_port(tmp_path) -> None:
    gate = _AtomicGate(fail_claim=True)
    store, facade = _facade(tmp_path, gate)
    calls: list[str] = []

    try:
        record = facade.submit(
            _intent(),
            lambda intent: (
                calls.append(intent.intent_id)
                or ProviderObservation.accepted(intent.intent_id, "provider-order-1")
            ),
        )

        assert record.state is ExecutionState.BLOCKED
        assert record.review_required is True
        assert calls == []
        assert gate.released == [("permit-signal-1", "admission_claim_failed")]
    finally:
        store.close()


@pytest.mark.unit
def test_pre_dispatch_guard_failure_blocks_before_provider_port(tmp_path) -> None:
    gate = _Gate()
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    calls: list[str] = []

    def guard(intent: OrderIntent) -> None:
        record = store.get(intent.intent_id, scope=_scope())
        assert record is not None and record.state is ExecutionState.DISPATCHING
        raise RuntimeError("risk latch unavailable")

    facade = ManagedExecutionFacade(
        store,
        _scope(),
        writer_id="writer-a",
        admission_gate=gate,
        pre_dispatch_guard=guard,
    )

    def provider(intent: OrderIntent) -> ProviderObservation:
        calls.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

    try:
        record = facade.submit(_intent(), provider)

        assert record.state is ExecutionState.BLOCKED
        assert record.review_required is True
        assert calls == []
        assert gate.released == [("permit-signal-1", "pre_dispatch_guard_failed")]
        assert [event.event_type for event in store.read_outbox()] == [
            "intent_recorded",
            "intent_admitted",
            "dispatch_claimed",
            "dispatch_blocked",
        ]
    finally:
        store.close()


@pytest.mark.unit
def test_observation_advances_acknowledged_order_without_second_permit_settlement(tmp_path) -> None:
    gate = _Gate()
    store, facade = _facade(tmp_path, gate)

    try:
        assert (
            facade.submit(
                _intent(),
                lambda intent: ProviderObservation.accepted(intent.intent_id, "provider-order-1"),
            ).state
            is ExecutionState.ACKED
        )
        partial = facade.record_provider_observation(
            ProviderObservation(
                "signal-1",
                ExecutionState.PARTIALLY_FILLED,
                provider_order_id="provider-order-1",
                filled_quantity=Decimal("1"),
                average_price=Decimal("50000"),
            )
        )
        filled = facade.reconcile(
            ProviderObservation(
                "signal-1",
                ExecutionState.FILLED,
                provider_order_id="provider-order-1",
                filled_quantity=Decimal("2"),
                average_price=Decimal("49999"),
            )
        )

        assert partial.state is ExecutionState.PARTIALLY_FILLED
        assert filled.state is ExecutionState.FILLED
        assert gate.settled == ["permit-signal-1"]
    finally:
        store.close()


@pytest.mark.unit
def test_scope_writer_lease_excludes_a_second_facade(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    first = ManagedExecutionFacade(store, _scope(), writer_id="writer-a", admission_gate=_Gate())
    second = ManagedExecutionFacade(store, _scope(), writer_id="writer-b", admission_gate=_Gate())
    try:
        first.acquire_writer_lease()
        with pytest.raises(WriterLeaseUnavailable):
            second.submit(
                _intent(), lambda intent: ProviderObservation.accepted(intent.intent_id, "order-1")
            )
        assert store.get("signal-1") is None
    finally:
        store.close()


@pytest.mark.unit
def test_execution_store_rejects_unleased_state_mutation(tmp_path) -> None:
    """The public store cannot bypass facade fencing by omitting a token."""

    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    try:
        with pytest.raises(WriterLeaseUnavailable):
            store.admit_intent(_intent())

        lease = store.acquire_or_renew_lease(_scope(), "writer-a")
        record = store.admit_intent(_intent(), writer_lease=lease)
        assert record.state is ExecutionState.PENDING_ADMISSION
    finally:
        store.close()


@pytest.mark.unit
def test_replayable_outbox_survives_reopen(tmp_path) -> None:
    path = tmp_path / "execution.sqlite3"
    gate = _Gate()
    store = SqliteExecutionStore(path)
    facade = ManagedExecutionFacade(store, _scope(), writer_id="writer-a", admission_gate=gate)
    try:
        facade.submit(
            _intent(),
            lambda intent: ProviderObservation.accepted(intent.intent_id, "provider-order-1"),
        )
        last_sequence = store.read_outbox()[-1].sequence
    finally:
        store.close()

    reopened = SqliteExecutionStore(path)
    try:
        page = reopened.read_outbox(after_sequence=0)
        assert [event.sequence for event in page] == list(range(1, last_sequence + 1))
        assert reopened.read_outbox(after_sequence=last_sequence) == ()
    finally:
        reopened.close()


@pytest.mark.unit
def test_fill_commission_evidence_survives_execution_store_reopen(tmp_path) -> None:
    path = tmp_path / "execution.sqlite3"
    store = SqliteExecutionStore(path)
    facade = ManagedExecutionFacade(store, _scope(), writer_id="writer-a", admission_gate=_Gate())
    try:
        record = facade.submit(
            _intent(),
            lambda intent: ProviderObservation(
                intent.intent_id,
                ExecutionState.FILLED,
                provider_order_id="provider-order-1",
                filled_quantity=Decimal("2"),
                average_price=Decimal("49999"),
                cumulative_commission=Decimal("0.42"),
            ),
        )
        assert record.cumulative_commission == Decimal("0.42")
    finally:
        facade.close()
        store.close()

    reopened = SqliteExecutionStore(path)
    try:
        recovered = reopened.get("signal-1", scope=_scope())
        assert recovered is not None
        assert recovered.state is ExecutionState.FILLED
        assert recovered.cumulative_commission == Decimal("0.42")
    finally:
        reopened.close()


@pytest.mark.unit
def test_same_intent_id_is_isolated_by_execution_scope(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    first_scope = _scope()
    second_scope = ExecutionScope("FAKE", "simulation", "acct_demo", "strategy.another", "20260922")
    first = ManagedExecutionFacade(store, first_scope, writer_id="writer-a", admission_gate=_Gate())
    second = ManagedExecutionFacade(
        store, second_scope, writer_id="writer-b", admission_gate=_Gate()
    )
    try:
        assert (
            first.submit(
                _intent_for_scope(first_scope),
                lambda intent: ProviderObservation.accepted(intent.intent_id, "provider-order-1"),
            ).state
            is ExecutionState.ACKED
        )
        assert first.close()
        assert (
            second.submit(
                _intent_for_scope(second_scope),
                lambda intent: ProviderObservation.accepted(intent.intent_id, "provider-order-2"),
            ).state
            is ExecutionState.ACKED
        )

        assert store.get("signal-1", scope=first_scope).provider_order_id == "provider-order-1"
        assert store.get("signal-1", scope=second_scope).provider_order_id == "provider-order-2"
        with pytest.raises(ContractValidationError, match="ambiguous"):
            store.get("signal-1")
    finally:
        store.close()
