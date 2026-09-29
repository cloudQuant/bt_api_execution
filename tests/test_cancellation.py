from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from decimal import Decimal

import pytest

from bt_api_execution import (
    CancelDispatchClaimReceiptV1,
    CancelIntent,
    CancelObservation,
    ContractValidationError,
    ExecutionScope,
    ExecutionState,
    ManagedCancellationFacade,
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


class _OrderGate:
    def reserve(self, intent: OrderIntent) -> _Permit:
        return _Permit("order-permit-" + intent.intent_id)

    def validate(self, permit_id: str, intent: OrderIntent) -> _Permit:
        assert permit_id == "order-permit-" + intent.intent_id
        return _Permit(permit_id)

    def settle(self, permit_id: str) -> None:
        assert permit_id.startswith("order-permit-")

    def release(self, permit_id: str, reason: str) -> None:
        assert permit_id.startswith("order-permit-")
        assert reason


class _CancelGate:
    def __init__(self) -> None:
        self.reserved: list[str] = []
        self.validated: list[str] = []
        self.claimed: list[str] = []
        self.settled: list[str] = []
        self.released: list[tuple[str, str]] = []

    def reserve(self, intent: CancelIntent) -> _Permit:
        self.reserved.append(intent.cancel_id)
        return _Permit("cancel-permit-" + intent.cancel_id)

    def validate(self, permit_id: str, intent: CancelIntent) -> _Permit:
        assert permit_id == "cancel-permit-" + intent.cancel_id
        self.validated.append(intent.cancel_id)
        return _Permit(permit_id)

    def claim_before_dispatch(
        self, permit_id: str, intent: CancelIntent
    ) -> CancelDispatchClaimReceiptV1:
        assert permit_id == "cancel-permit-" + intent.cancel_id
        self.claimed.append(intent.cancel_id)
        return CancelDispatchClaimReceiptV1(
            permit_reference=permit_id,
            scope_key=intent.scope.key,
            cancel_id=intent.cancel_id,
            intent_fingerprint=intent.fingerprint,
        )

    def settle(self, permit_id: str) -> None:
        self.settled.append(permit_id)

    def release(self, permit_id: str, reason: str) -> None:
        self.released.append((permit_id, reason))


class _DurableCancelGate(_CancelGate):
    """Tiny SQLite-backed risk fake for the provider-call ordering boundary."""

    def __init__(self, path) -> None:
        super().__init__()
        self._path = path
        self._connection = sqlite3.connect(path)
        self._connection.execute("CREATE TABLE risk_dispatch_claims (permit_id TEXT PRIMARY KEY)")
        self._connection.commit()

    def claim_before_dispatch(
        self, permit_id: str, intent: CancelIntent
    ) -> CancelDispatchClaimReceiptV1:
        receipt = super().claim_before_dispatch(permit_id, intent)
        with self._connection:
            self._connection.execute(
                "INSERT INTO risk_dispatch_claims (permit_id) VALUES (?)", (permit_id,)
            )
        return receipt

    def has_durable_claim(self, permit_id: str) -> bool:
        with sqlite3.connect(self._path) as db:
            row = db.execute(
                "SELECT 1 FROM risk_dispatch_claims WHERE permit_id = ?", (permit_id,)
            ).fetchone()
        return row is not None

    def close(self) -> None:
        self._connection.close()


def _scope() -> ExecutionScope:
    return ExecutionScope("FAKE", "simulation", "acct_demo", "strategy.demo", "20260922")


def _scope_for(strategy_id: str) -> ExecutionScope:
    """Build a sibling strategy scope on the same simulated account."""

    return ExecutionScope("FAKE", "simulation", "acct_demo", strategy_id, "20260922")


def _order_intent() -> OrderIntent:
    return OrderIntent.limit(
        intent_id="order.1",
        scope=_scope(),
        signal_id="signal.1",
        instrument="BTC-USDT",
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("50000"),
        metadata_version="metadata.1",
    )


def _cancel_intent(
    *, cancel_id: str = "cancel.1", provider_order_id: str = "provider.1"
) -> CancelIntent:
    return CancelIntent(
        cancel_id=cancel_id,
        scope=_scope(),
        target_intent_id="order.1",
        provider_order_id=provider_order_id,
        metadata_version="metadata.1",
    )


def _ready_facades(tmp_path, *, cancel_gate: _CancelGate | None = None):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    order_facade = ManagedExecutionFacade(
        store, _scope(), writer_id="writer.1", admission_gate=_OrderGate()
    )
    order_facade.submit(
        _order_intent(),
        lambda intent: ProviderObservation.accepted(intent.intent_id, "provider.1"),
    )
    gate = cancel_gate if cancel_gate is not None else _CancelGate()
    cancel_facade = ManagedCancellationFacade(
        store,
        _scope(),
        acquire_writer_lease=order_facade.acquire_writer_lease,
        admission_gate=gate,
    )
    return store, cancel_facade, gate


@pytest.mark.unit
def test_cancel_is_durable_idempotent_and_keeps_target_identity(tmp_path) -> None:
    store, facade, gate = _ready_facades(tmp_path)
    calls: list[str] = []

    call_order: list[str] = []

    def provider(intent: CancelIntent) -> CancelObservation:
        call_order.append("provider")
        calls.append(intent.provider_order_id)
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )

    original_claim = gate.claim_before_dispatch

    def claim_before_dispatch(permit_id: str, intent: CancelIntent) -> CancelDispatchClaimReceiptV1:
        call_order.append("risk_claim")
        return original_claim(permit_id, intent)

    gate.claim_before_dispatch = claim_before_dispatch  # type: ignore[method-assign]

    try:
        first = facade.cancel(_cancel_intent(), provider)
        repeated = facade.cancel(_cancel_intent(), provider)
        target = store.get("order.1", scope=_scope())

        assert first.state is ExecutionState.ACKED
        assert repeated.state is ExecutionState.ACKED
        assert target is not None and target.state is ExecutionState.ACKED
        assert calls == ["provider.1"]
        assert gate.reserved == ["cancel.1"]
        assert gate.validated == ["cancel.1"]
        assert gate.claimed == ["cancel.1"]
        assert call_order == ["risk_claim", "provider"]
        assert gate.settled == ["cancel-permit-cancel.1"]
        assert [event.event_type for event in store.read_cancel_outbox()] == [
            "cancel_intent_recorded",
            "cancel_intent_admitted",
            "cancel_dispatch_claimed",
            "cancel_provider_observation",
        ]
    finally:
        store.close()


@pytest.mark.unit
def test_provider_is_called_only_after_risk_claim_is_committed(tmp_path) -> None:
    gate = _DurableCancelGate(tmp_path / "risk.sqlite3")
    store, facade, gate = _ready_facades(tmp_path, cancel_gate=gate)
    provider_calls: list[str] = []

    def provider(intent: CancelIntent) -> CancelObservation:
        assert gate.has_durable_claim("cancel-permit-" + intent.cancel_id)
        provider_calls.append(intent.cancel_id)
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )

    try:
        result = facade.cancel(_cancel_intent(), provider)

        assert result.state is ExecutionState.ACKED
        assert gate.claimed == ["cancel.1"]
        assert provider_calls == ["cancel.1"]
    finally:
        gate.close()
        store.close()


@pytest.mark.unit
def test_cancel_pre_dispatch_guard_runs_before_risk_claim_and_provider(tmp_path) -> None:
    store, facade, gate = _ready_facades(tmp_path)
    provider_calls: list[str] = []

    def guard(intent: CancelIntent) -> None:
        raise RuntimeError("guard denied")

    try:
        record = facade.cancel(
            _cancel_intent(),
            lambda intent: provider_calls.append(intent.cancel_id),
            before_dispatch=guard,
        )

        assert record.state is ExecutionState.BLOCKED
        assert provider_calls == []
        assert gate.claimed == []
        assert gate.released == [("cancel-permit-cancel.1", "cancel_pre_dispatch_guard_failed")]
    finally:
        store.close()


@pytest.mark.unit
def test_cancel_risk_claim_failure_is_unknown_without_release_or_provider_retry(tmp_path) -> None:
    store, facade, gate = _ready_facades(tmp_path)
    provider_calls: list[str] = []

    def ambiguous_claim(permit_id: str, intent: CancelIntent) -> CancelDispatchClaimReceiptV1:
        gate.claimed.append(intent.cancel_id)
        # Model a risk store commit whose acknowledgement was lost.
        raise RuntimeError("claim acknowledgement lost")

    gate.claim_before_dispatch = ambiguous_claim  # type: ignore[method-assign]

    def provider(intent: CancelIntent) -> None:
        provider_calls.append(intent.cancel_id)

    try:
        first = facade.cancel(_cancel_intent(), provider)
        second = facade.cancel(_cancel_intent(), provider)

        assert first.state is ExecutionState.UNKNOWN
        assert first.unknown_reason == "cancel_admission_claim_outcome_unknown"
        assert second.state is ExecutionState.UNKNOWN
        assert gate.claimed == ["cancel.1"]
        assert gate.released == []
        assert provider_calls == []
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "invalid_kind",
    [
        "zero",
        "empty_string",
        "string",
        "plain_object",
        "wrong_permit",
        "wrong_scope",
        "wrong_cancel",
        "wrong_fingerprint",
    ],
)
def test_cancel_invalid_risk_claim_receipt_is_unknown_and_never_dispatches(
    tmp_path, invalid_kind: str
) -> None:
    store, facade, gate = _ready_facades(tmp_path)
    provider_calls: list[str] = []

    def invalid_claim(permit_id: str, intent: CancelIntent):
        gate.claimed.append(intent.cancel_id)
        valid_fields = {
            "permit_reference": permit_id,
            "scope_key": intent.scope.key,
            "cancel_id": intent.cancel_id,
            "intent_fingerprint": intent.fingerprint,
        }
        if invalid_kind == "zero":
            return 0
        if invalid_kind == "empty_string":
            return ""
        if invalid_kind == "string":
            return permit_id
        if invalid_kind == "plain_object":
            return object()
        if invalid_kind == "wrong_permit":
            valid_fields["permit_reference"] = "other-permit"
        elif invalid_kind == "wrong_scope":
            valid_fields["scope_key"] = "other-scope"
        elif invalid_kind == "wrong_cancel":
            valid_fields["cancel_id"] = "other-cancel"
        elif invalid_kind == "wrong_fingerprint":
            valid_fields["intent_fingerprint"] = "0" * 64
        return CancelDispatchClaimReceiptV1(**valid_fields)

    gate.claim_before_dispatch = invalid_claim  # type: ignore[method-assign]

    def provider(intent: CancelIntent) -> None:
        provider_calls.append(intent.cancel_id)

    try:
        result = facade.cancel(_cancel_intent(), provider)
        replayed = facade.cancel(_cancel_intent(), provider)

        assert result.state is ExecutionState.UNKNOWN
        assert result.unknown_reason == "cancel_admission_claim_outcome_unknown"
        assert replayed.state is ExecutionState.UNKNOWN
        assert gate.claimed == ["cancel.1"]
        assert gate.released == []
        assert provider_calls == []
    finally:
        store.close()


@pytest.mark.unit
def test_cancel_provider_failure_after_risk_claim_keeps_permit_fenced(tmp_path) -> None:
    store, facade, gate = _ready_facades(tmp_path)
    provider_calls: list[str] = []

    def provider(intent: CancelIntent) -> CancelObservation:
        provider_calls.append(intent.cancel_id)
        raise TimeoutError("provider outcome unknown")

    try:
        first = facade.cancel(_cancel_intent(), provider)
        second = facade.cancel(_cancel_intent(), provider)

        assert first.state is ExecutionState.UNKNOWN
        assert second.state is ExecutionState.UNKNOWN
        assert gate.claimed == ["cancel.1"]
        assert gate.released == []
        assert provider_calls == ["cancel.1"]
    finally:
        store.close()


@pytest.mark.unit
def test_configured_cancel_gate_without_dispatch_claim_method_fails_closed(tmp_path) -> None:
    class _LegacyGateWithoutClaim:
        def reserve(self, intent: CancelIntent) -> _Permit:
            return _Permit("permit")

        def validate(self, permit_id: str, intent: CancelIntent) -> None:
            return None

        def settle(self, permit_id: str) -> None:
            return None

        def release(self, permit_id: str, reason: str) -> None:
            return None

    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    with pytest.raises(ContractValidationError, match="claim_before_dispatch"):
        ManagedCancellationFacade(
            store,
            _scope(),
            acquire_writer_lease=lambda: store.acquire_or_renew_lease(_scope(), "writer.legacy"),
            admission_gate=_LegacyGateWithoutClaim(),  # type: ignore[arg-type]
        )
    store.close()


@pytest.mark.unit
def test_explicit_unprotected_legacy_cancel_path_remains_available_without_gate(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    orders = ManagedExecutionFacade(
        store, _scope(), writer_id="writer.legacy", admission_gate=_OrderGate()
    )
    orders.submit(
        _order_intent(),
        lambda intent: ProviderObservation.accepted(intent.intent_id, "provider.1"),
    )
    cancellation = ManagedCancellationFacade(
        store,
        _scope(),
        acquire_writer_lease=orders.acquire_writer_lease,
        admission_gate=None,
        allow_unprotected=True,
    )
    calls: list[str] = []
    try:
        result = cancellation.cancel(
            _cancel_intent(),
            lambda intent: (
                calls.append(intent.cancel_id)
                or CancelObservation.accepted(
                    intent.cancel_id, intent.target_intent_id, intent.provider_order_id
                )
            ),
        )

        assert result.state is ExecutionState.ACKED
        assert calls == ["cancel.1"]
    finally:
        orders.close()
        store.close()


@pytest.mark.unit
def test_cancel_rejects_missing_or_mismatched_confirmed_provider_identity_before_dispatch(
    tmp_path,
) -> None:
    store, facade, _gate = _ready_facades(tmp_path)
    calls: list[str] = []

    try:
        with pytest.raises(ContractValidationError, match="provider identity"):
            facade.cancel(
                _cancel_intent(provider_order_id="another.provider"),
                lambda intent: calls.append(intent),
            )

        assert calls == []
        assert store.get_cancel("cancel.1", scope=_scope()) is None
    finally:
        store.close()


@pytest.mark.unit
def test_unknown_cancellation_never_retries_and_only_typed_reconciliation_can_close_target(
    tmp_path,
) -> None:
    store, facade, gate = _ready_facades(tmp_path)
    calls: list[str] = []

    def timeout_provider(intent: CancelIntent) -> CancelObservation:
        calls.append(intent.cancel_id)
        raise TimeoutError(intent.cancel_id)

    try:
        first = facade.cancel(_cancel_intent(), timeout_provider)
        repeated = facade.cancel(_cancel_intent(), timeout_provider)
        reconciled = facade.reconcile(
            CancelObservation.cancelled("cancel.1", "order.1", "provider.1")
        )
        target = store.get("order.1", scope=_scope())

        assert first.state is ExecutionState.UNKNOWN
        assert first.review_required is True
        assert repeated.state is ExecutionState.UNKNOWN
        assert calls == ["cancel.1"]
        assert reconciled.state is ExecutionState.CANCELLED
        assert target is not None and target.state is ExecutionState.CANCELLED
        assert gate.settled == ["cancel-permit-cancel.1"]
    finally:
        store.close()


@pytest.mark.unit
def test_expired_cancellation_writer_cannot_project_provider_result_after_fencing(tmp_path) -> None:
    """Cancellation observation writes use the same token fence as orders."""

    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    order_facade = ManagedExecutionFacade(
        store,
        _scope(),
        writer_id="writer.initial",
        admission_gate=_OrderGate(),
        lease_ttl_ns=100_000_000,
    )
    order_facade.submit(
        _order_intent(),
        lambda intent: ProviderObservation.accepted(intent.intent_id, "provider.1"),
    )
    gate = _CancelGate()
    cancellation = ManagedCancellationFacade(
        store,
        _scope(),
        acquire_writer_lease=order_facade.acquire_writer_lease,
        admission_gate=gate,
    )
    entered = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def blocking_provider(intent: CancelIntent) -> CancelObservation:
        entered.set()
        assert release.wait(timeout=5)
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )

    def cancel() -> None:
        try:
            cancellation.cancel(_cancel_intent(), blocking_provider)
        except BaseException as error:  # asserted after the stale provider returns
            errors.append(error)

    thread = threading.Thread(target=cancel)
    thread.start()
    assert entered.wait(timeout=5)
    try:
        time.sleep(0.15)
        contender = store.acquire_or_renew_lease(_scope(), "writer.contender", ttl_ns=1_000_000_000)
        assert contender.fencing_token > order_facade._last_writer_lease.fencing_token
        release.set()
        thread.join(timeout=5)

        assert not thread.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], WriterLeaseUnavailable)
        record = store.get_cancel("cancel.1", scope=_scope())
        assert record is not None and record.state is ExecutionState.DISPATCHING
        assert gate.settled == []
        # A stale facade must not delete the new owner generation while closing.
        assert order_facade.close() is False
        assert store.release_lease(
            _scope(), "writer.contender", fencing_token=contender.fencing_token
        )
    finally:
        release.set()
        thread.join(timeout=5)
        store.close()


@pytest.mark.unit
def test_cancellation_idempotency_and_outbox_survive_journal_reopen(tmp_path) -> None:
    path = tmp_path / "execution.sqlite3"
    store = SqliteExecutionStore(path)
    order_facade = ManagedExecutionFacade(
        store, _scope(), writer_id="writer.1", admission_gate=_OrderGate()
    )
    order_facade.submit(
        _order_intent(),
        lambda intent: ProviderObservation.accepted(intent.intent_id, "provider.1"),
    )
    first_gate = _CancelGate()
    first = ManagedCancellationFacade(
        store,
        _scope(),
        acquire_writer_lease=order_facade.acquire_writer_lease,
        admission_gate=first_gate,
    )
    calls: list[str] = []
    try:
        record = first.cancel(
            _cancel_intent(),
            lambda intent: (
                calls.append(intent.cancel_id)
                or CancelObservation.accepted(
                    intent.cancel_id, intent.target_intent_id, intent.provider_order_id
                )
            ),
        )
        assert record.state is ExecutionState.ACKED
    finally:
        order_facade.close()
        store.close()

    reopened = SqliteExecutionStore(path)
    second = ManagedCancellationFacade(
        reopened,
        _scope(),
        acquire_writer_lease=lambda: reopened.acquire_or_renew_lease(_scope(), "writer.1"),
        admission_gate=_CancelGate(),
    )
    try:
        replayed = second.cancel(
            _cancel_intent(),
            lambda intent: (
                calls.append("unexpected." + intent.cancel_id)
                or CancelObservation.accepted(
                    intent.cancel_id, intent.target_intent_id, intent.provider_order_id
                )
            ),
        )

        assert replayed.state is ExecutionState.ACKED
        assert calls == ["cancel.1"]
        assert [event.sequence for event in reopened.read_cancel_outbox()] == [1, 2, 3, 4]
    finally:
        reopened.close()


@pytest.mark.unit
def test_restart_recovery_latches_claimed_cancel_unknown_without_retry(tmp_path) -> None:
    """A crash after the durable claim must not strand or replay a cancel."""

    path = tmp_path / "execution.sqlite3"
    first_store = SqliteExecutionStore(path)
    first_orders = ManagedExecutionFacade(
        first_store, _scope(), writer_id="writer.before_restart", admission_gate=_OrderGate()
    )
    first_orders.submit(
        _order_intent(),
        lambda intent: ProviderObservation.accepted(intent.intent_id, "provider.1"),
    )
    first_cancellation = ManagedCancellationFacade(
        first_store,
        _scope(),
        acquire_writer_lease=first_orders.acquire_writer_lease,
        admission_gate=_CancelGate(),
    )
    calls: list[str] = []

    class _SimulatedProcessCrash(BaseException):
        pass

    def crash_after_claim(intent: CancelIntent) -> CancelObservation:
        calls.append(intent.cancel_id)
        # BaseException models process death after the provider call may have
        # started; the facade cannot turn this into a fabricated response.
        raise _SimulatedProcessCrash()

    try:
        with pytest.raises(_SimulatedProcessCrash):
            first_cancellation.cancel(_cancel_intent(), crash_after_claim)
        interrupted = first_cancellation.get("cancel.1")
        assert interrupted is not None
        assert interrupted.state is ExecutionState.DISPATCHING
    finally:
        assert first_orders.close()
        first_store.close()

    reopened = SqliteExecutionStore(path)
    second_orders = ManagedExecutionFacade(
        reopened, _scope(), writer_id="writer.after_restart", admission_gate=_OrderGate()
    )
    second_cancellation = ManagedCancellationFacade(
        reopened,
        _scope(),
        acquire_writer_lease=second_orders.acquire_writer_lease,
        admission_gate=_CancelGate(),
    )
    try:
        recovered = second_cancellation.recover_interrupted_dispatches()
        assert len(recovered) == 1
        assert recovered[0].cancel_id == "cancel.1"
        assert recovered[0].state is ExecutionState.UNKNOWN
        assert recovered[0].unknown_reason == "interrupted_cancel_dispatch"
        assert recovered[0].review_required is True
        assert recovered[0].dispatch_attempts == 1

        # The submitted order is still open in the journal. Recovery does not
        # reinterpret order status as proof that the cancel succeeded.
        target = reopened.get("order.1", scope=_scope())
        assert target is not None and target.state is ExecutionState.ACKED
        assert [record.cancel_id for record in reopened.list_unknown_cancellations(_scope())] == [
            "cancel.1"
        ]

        assert second_cancellation.recover_interrupted_dispatches() == ()
        replayed = second_cancellation.cancel(
            _cancel_intent(),
            lambda intent: (_ for _ in ()).throw(AssertionError("cancel must not be retried")),
        )
        assert replayed.state is ExecutionState.UNKNOWN
        assert calls == ["cancel.1"]

        events = reopened.read_cancel_outbox(scope=_scope())
        assert events[-1].event_type == "cancel_dispatch_recovered_unknown"
        assert events[-1].state is ExecutionState.UNKNOWN
        assert events[-1].payload == {"reason_code": "interrupted_cancel_dispatch"}
    finally:
        assert second_orders.close()
        reopened.close()


@pytest.mark.unit
def test_unknown_cancellation_recovery_scan_is_exact_scope_even_for_shared_cancel_id(
    tmp_path,
) -> None:
    """A startup recovery reader keeps same-ID cancellations in separate scopes."""

    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    first_scope = _scope_for("strategy.one")
    second_scope = ExecutionScope(
        "FAKE", "simulation", "acct_demo.second", "strategy.two", "20260922"
    )
    provider_calls: list[tuple[str, str]] = []
    try:
        for scope, intent_id, provider_order_id in (
            (first_scope, "order.one", "provider.one"),
            (second_scope, "order.two", "provider.two"),
        ):
            order_facade = ManagedExecutionFacade(
                store,
                scope,
                writer_id="writer." + scope.strategy_id,
                admission_gate=_OrderGate(),
            )
            try:
                order_facade.submit(
                    OrderIntent.limit(
                        intent_id=intent_id,
                        scope=scope,
                        signal_id="signal." + intent_id,
                        instrument="BTC-USDT",
                        side=Side.BUY,
                        quantity=Decimal("1"),
                        price=Decimal("50000"),
                        metadata_version="metadata.1",
                    ),
                    lambda intent, provider_order_id=provider_order_id: (
                        ProviderObservation.accepted(intent.intent_id, provider_order_id)
                    ),
                )
                cancellation = ManagedCancellationFacade(
                    store,
                    scope,
                    acquire_writer_lease=order_facade.acquire_writer_lease,
                    admission_gate=_CancelGate(),
                )
                record = cancellation.cancel(
                    CancelIntent(
                        # Scope, not cancel_id alone, owns cancellation idempotency.
                        cancel_id="cancel.shared",
                        scope=scope,
                        target_intent_id=intent_id,
                        provider_order_id=provider_order_id,
                        metadata_version="metadata.1",
                    ),
                    lambda intent: (
                        provider_calls.append((intent.scope.strategy_id, intent.cancel_id))
                        or (_ for _ in ()).throw(TimeoutError("local test timeout"))
                    ),
                )
                assert record.state is ExecutionState.UNKNOWN
            finally:
                # Each independent account has its own dispatch writer.
                order_facade.close()

        first_unknown = store.list_unknown_cancellations(first_scope)
        second_unknown = store.list_unknown_cancellations(second_scope)

        assert [(record.scope_key, record.cancel_id) for record in first_unknown] == [
            (first_scope.key, "cancel.shared")
        ]
        assert [(record.scope_key, record.cancel_id) for record in second_unknown] == [
            (second_scope.key, "cancel.shared")
        ]
        assert provider_calls == [
            ("strategy.one", "cancel.shared"),
            ("strategy.two", "cancel.shared"),
        ]
    finally:
        store.close()
