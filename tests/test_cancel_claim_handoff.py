"""Store/Facade contract for passing a validated cancel claim to its port.

The admission gate and dispatch ports here are local test doubles. These tests
exercise the real SQLite-backed ManagedCancellationFacade boundary; they do
not establish CTP authority or provider acknowledgement.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from bt_api_execution import (
    CancelDispatchClaimReceiptV1,
    CancelObservation,
    ExecutionState,
    ManagedCancellationFacade,
    ManagedExecutionFacade,
    ProviderObservation,
    SqliteExecutionStore,
)
from test_cancellation import (
    _CancelGate,
    _OrderGate,
    _cancel_intent,
    _order_intent,
    _ready_facades,
    _scope,
)


def _accepted(intent):
    return CancelObservation.accepted(
        intent.cancel_id, intent.target_intent_id, intent.provider_order_id
    )


class _ClaimAwarePort:
    def __init__(self) -> None:
        self.claim_receipts: list[object] = []
        self.claim_aware_calls: list[str] = []
        self.legacy_calls: list[str] = []

    def cancel_with_claim(self, intent, claim_receipt):
        self.claim_aware_calls.append(intent.cancel_id)
        self.claim_receipts.append(claim_receipt)
        return _accepted(intent)

    def cancel(self, intent):
        self.legacy_calls.append(intent.cancel_id)
        return _accepted(intent)


class _LegacyObjectPort:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def cancel(self, intent):
        self.calls.append(intent.cancel_id)
        return _accepted(intent)


@pytest.mark.unit
def test_claim_aware_port_receives_exact_claim_and_replay_does_not_dispatch_again(tmp_path):
    store, facade, gate = _ready_facades(tmp_path)
    issued: list[CancelDispatchClaimReceiptV1] = []
    original_claim = gate.claim_before_dispatch

    def capture_claim(permit_reference, intent):
        receipt = original_claim(permit_reference, intent)
        issued.append(receipt)
        return receipt

    gate.claim_before_dispatch = capture_claim  # type: ignore[method-assign]
    port = _ClaimAwarePort()
    intent = _cancel_intent()
    try:
        first = facade.cancel(intent, port)
        replay = facade.cancel(intent, port)

        assert first.state is ExecutionState.ACKED
        assert replay.state is ExecutionState.ACKED
        assert len(issued) == 1
        assert len(port.claim_receipts) == 1
        assert port.claim_receipts[0] is issued[0]
        assert port.claim_aware_calls == [intent.cancel_id]
        assert port.legacy_calls == []
        assert gate.claimed == [intent.cancel_id]
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize("invalid_kind", ["missing", "wrong_identity"])
def test_invalid_claim_never_calls_claim_aware_or_legacy_dispatch(tmp_path, invalid_kind):
    store, facade, gate = _ready_facades(tmp_path)
    original_claim = gate.claim_before_dispatch

    def invalid_claim(permit_reference, intent):
        receipt = original_claim(permit_reference, intent)
        if invalid_kind == "missing":
            return None
        return replace(receipt, cancel_id="other-cancel")

    gate.claim_before_dispatch = invalid_claim  # type: ignore[method-assign]
    port = _ClaimAwarePort()
    intent = _cancel_intent()
    try:
        first = facade.cancel(intent, port)
        replay = facade.cancel(intent, port)

        assert first.state is ExecutionState.UNKNOWN
        assert first.unknown_reason == "cancel_admission_claim_outcome_unknown"
        assert replay.state is ExecutionState.UNKNOWN
        assert gate.claimed == [intent.cancel_id]
        assert port.claim_aware_calls == []
        assert port.legacy_calls == []
        assert port.claim_receipts == []
    finally:
        store.close()


@pytest.mark.unit
def test_unprotected_claim_aware_port_without_receipt_fails_before_both_methods(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    orders = ManagedExecutionFacade(
        store, _scope(), writer_id="writer.unprotected", admission_gate=_OrderGate()
    )
    orders.submit(
        _order_intent(),
        lambda intent: ProviderObservation.accepted(intent.intent_id, "provider.1"),
    )
    facade = ManagedCancellationFacade(
        store,
        _scope(),
        acquire_writer_lease=orders.acquire_writer_lease,
        admission_gate=None,
        allow_unprotected=True,
    )
    port = _ClaimAwarePort()
    intent = _cancel_intent()
    try:
        record = facade.cancel(intent, port)
        replay = facade.cancel(intent, port)

        assert record.state is ExecutionState.UNKNOWN
        assert replay.state is ExecutionState.UNKNOWN
        assert port.claim_aware_calls == []
        assert port.legacy_calls == []
    finally:
        orders.close()
        store.close()


@pytest.mark.unit
def test_postclaim_provider_guard_failure_never_calls_either_dispatch_method(tmp_path):
    store, facade, gate = _ready_facades(tmp_path)
    port = _ClaimAwarePort()
    intent = _cancel_intent()

    def deny_provider_dispatch(_intent):
        raise RuntimeError("local provider-dispatch guard denied")

    try:
        record = facade.cancel(
            intent, port, before_provider_dispatch=deny_provider_dispatch
        )

        assert record.state is ExecutionState.BLOCKED
        assert gate.claimed == [intent.cancel_id]
        assert gate.released == [
            ("cancel-permit-" + intent.cancel_id, "cancel_provider_dispatch_guard_failed")
        ]
        assert port.claim_aware_calls == []
        assert port.legacy_calls == []
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize("legacy_kind", ["object", "callable"])
def test_dispatchers_without_claim_method_keep_legacy_compatibility(tmp_path, legacy_kind):
    store, facade, gate = _ready_facades(tmp_path)
    intent = _cancel_intent()
    calls: list[str] = []
    if legacy_kind == "object":
        port = _LegacyObjectPort()
        expected_calls = port.calls
    else:
        def port(value):
            calls.append(value.cancel_id)
            return _accepted(value)

        expected_calls = calls
    try:
        record = facade.cancel(intent, port)

        assert record.state is ExecutionState.ACKED
        assert expected_calls == [intent.cancel_id]
        assert gate.claimed == [intent.cancel_id]
        assert not hasattr(port, "cancel_with_claim")
    finally:
        store.close()
