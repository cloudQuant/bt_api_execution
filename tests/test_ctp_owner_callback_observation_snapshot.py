from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace
from decimal import Decimal, Inexact, Rounded, localcontext

import pytest
import test_ctp_callback_session_router as router_helpers

from bt_api_execution import (
    CtpOwnerCallbackObservationNotReadyV1,
    CtpOwnerCallbackObservationSnapshotV1,
    CtpVerifiedCallbackEvidence,
    SqliteExecutionStore,
    payload_sha256,
)
from test_ctp_callback_session_router import (
    _TerminalOrderCallbackVerifier,
    _TradeVerifier,
    _OrderTargetVerifier,
    _active_owner,
    _adapter_for,
    _lease,
    _order_fields,
    _queue_and_complete,
    _record,
    _reserve,
    _scope,
    _stage_cancel_and_complete,
    _trade_fields,
)


class _SnapshotOrderCallbackVerifier(_TerminalOrderCallbackVerifier):
    def verify_callback(self, command, callback, callback_payload, *, now_ns):
        native = callback_payload.get("native_fields", {})
        if (
            command.operation == "SUBMIT"
            and callback_payload.get("source_callback") == "OnRtnOrder"
            and native.get("OrderStatus") == "3"
        ):
            return CtpVerifiedCallbackEvidence(
                evidence_type="ctp_verified_callback.v1",
                callback_key=callback,
                callback_payload_sha256=payload_sha256(callback_payload),
                projection_state="ACKNOWLEDGED",
                source_digest_sha256=hashlib.sha256(b"snapshot order callback").hexdigest(),
                verifier_id="fake-owner-observation-order-verifier",
                verified_at_ns=now_ns,
                expires_at_ns=now_ns + 5_000_000_000,
            )
        return super().verify_callback(
            command, callback, callback_payload, now_ns=now_ns
        )


class _TwoUnitOrderTargetVerifier(_OrderTargetVerifier):
    def verify_order_target(self, scope, reservation, native_query_evidence, *, now_ns):
        projection = super().verify_order_target(
            scope, reservation, native_query_evidence, now_ns=now_ns
        )
        return replace(
            projection,
            quantity=2,
            remaining_quantity=2 - projection.traded_quantity,
        )


def _submit(store, scope, lease, owner, *, intent_id, command_id, request_id=71, quantity=1):
    session = store._active_ctp_callback_sessions[owner.owner_intent_id]
    reservation = _reserve(store, scope, lease, session.session_generation_id, intent_id)
    payload = {
        "BrokerID": "broker",
        "InvestorID": "user",
        "UserID": "user",
        "InstrumentID": "rb2710",
        "ExchangeID": "SHFE",
        "OrderRef": reservation.order_ref,
        "Direction": "0",
        "CombOffsetFlag": "0",
        "CombHedgeFlag": "1",
        "LimitPrice": 100.0,
        "VolumeTotalOriginal": quantity,
        "OrderPriceType": "2",
        "TimeCondition": "3",
    }
    staged = _queue_and_complete(
        store,
        scope,
        lease,
        owner,
        reservation,
        command_id,
        payload,
        request_id,
    )
    return reservation, staged


def _append_order(
    store,
    owner,
    scope,
    reservation,
    *,
    source_sequence,
    request_id=71,
    quantity=1,
    volume_traded=0,
    status="3",
    missing_identity=(),
):
    fields = []
    for slot, name, value in _order_fields(
        scope, reservation, request_id, volume_traded, source_sequence, status=status
    ):
        if name in missing_identity:
            continue
        if name == "VolumeTotal":
            value = quantity - volume_traded
        fields.append((slot, name, value))
    store.append_ctp_callback_ingress(
        owner,
        _record(
            owner,
            "OnRtnOrder",
            source_sequence,
            phase="ACTIVE",
            callback_class="ROUTEABLE",
            fields=tuple(fields),
        ),
    )
    return _adapter_for(store, owner).apply_next_ingress(
        callback_verifier=_SnapshotOrderCallbackVerifier(),
        trade_fact_verifier=_TradeVerifier(),
    )


def _append_trade(store, owner, reservation, *, source_sequence, trade_id, volume, price):
    store.append_ctp_callback_ingress(
        owner,
        _record(
            owner,
            "OnRtnTrade",
            source_sequence,
            phase="ACTIVE",
            callback_class="ROUTEABLE",
            fields=_trade_fields(
                order_ref=reservation.order_ref,
                trade_id=trade_id,
                volume=volume,
                price=price,
            ),
        ),
    )
    return _adapter_for(store, owner).apply_next_ingress(
        callback_verifier=_SnapshotOrderCallbackVerifier(),
        trade_fact_verifier=_TradeVerifier(),
    )


def _read(store, scope, command_id, *, cancel_command_id=None):
    return store.read_ctp_owner_callback_observation_snapshot(
        scope, command_id, cancel_command_id=cancel_command_id
    )


def _assert_not_ready(value, reason_code):
    assert isinstance(value, CtpOwnerCallbackObservationNotReadyV1)
    assert value.reason_code == reason_code


@pytest.mark.unit
def test_owner_observation_requires_applied_native_order_identity_and_is_read_only(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "owner-observation-incomplete.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        reservation, _ = _submit(
            store,
            scope,
            lease,
            owner,
            intent_id="observation-incomplete",
            command_id="command-observation-incomplete",
        )

        before = store._connection.total_changes
        _assert_not_ready(
            _read(store, scope, "command-observation-incomplete"),
            "PROVIDER_ORDER_NOT_READY",
        )
        assert store._connection.total_changes == before

        _append_order(
            store,
            owner,
            scope,
            reservation,
            source_sequence=4,
            missing_identity=("ExchangeID", "OrderSysID"),
        )
        before = store._connection.total_changes
        _assert_not_ready(
            _read(store, scope, "command-observation-incomplete"),
            "ORDER_CALLBACK_INCOMPLETE",
        )
        assert store._connection.total_changes == before

        # A trade can arrive between the identity-incomplete and complete
        # OrderRtn. Its cumulative order prefix still includes the earlier
        # explicit zero; dropping that history would reject this valid chain.
        _append_trade(
            store, owner, reservation, source_sequence=5,
            trade_id="late-native-identity-trade", volume=1, price=99.125,
        )
        _append_order(
            store,
            owner,
            scope,
            reservation,
            source_sequence=6,
            volume_traded=1,
            status="0",
        )
        before = store._connection.total_changes
        snapshot = _read(store, scope, "command-observation-incomplete")
        assert isinstance(snapshot, CtpOwnerCallbackObservationSnapshotV1)
        assert snapshot.exchange_id == "SHFE"
        assert snapshot.order_sys_id == "sys-" + reservation.order_ref
        assert snapshot.order_source_sequence == 6
        assert snapshot.filled_quantity == 1
        assert snapshot.trade_notional == Decimal("99.125")
        assert store._connection.total_changes == before
    finally:
        store.close()


@pytest.mark.unit
def test_owner_observation_accepts_only_explicit_zero_order_callback_with_empty_ledger(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "owner-observation-zero.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        reservation, _ = _submit(
            store,
            scope,
            lease,
            owner,
            intent_id="observation-zero",
            command_id="command-observation-zero",
        )
        _append_order(
            store,
            owner,
            scope,
            reservation,
            source_sequence=4,
            volume_traded=0,
            status="3",
        )

        before = store._connection.total_changes
        snapshot = _read(store, scope, "command-observation-zero")
        assert isinstance(snapshot, CtpOwnerCallbackObservationSnapshotV1)
        assert snapshot.command_status == "COMPLETED"
        assert snapshot.local_dispatch_outcome == "QUEUED"
        assert snapshot.provider_state == "ACKNOWLEDGED"
        assert snapshot.volume_traded == 0
        assert snapshot.filled_quantity == 0
        assert snapshot.trade_fact_count == 0
        assert snapshot.trade_notional == Decimal(0)
        assert snapshot.exchange_id == "SHFE"
        assert snapshot.order_sys_id == "sys-" + reservation.order_ref
        assert snapshot.cumulative_commission is None
        assert snapshot.commission_quality == "INCOMPLETE"
        assert store._connection.total_changes == before
    finally:
        store.close()


@pytest.mark.unit
def test_owner_observation_requires_deduplicated_trade_prefix_and_exact_notional(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "owner-observation-trades.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        reservation, _ = _submit(
            store,
            scope,
            lease,
            owner,
            intent_id="observation-trades",
            command_id="command-observation-trades",
            quantity=3,
        )
        _append_order(
            store,
            owner,
            scope,
            reservation,
            source_sequence=4,
            quantity=3,
            volume_traded=2,
            status="1",
        )
        _assert_not_ready(
            _read(store, scope, "command-observation-trades"),
            "TRADE_PREFIX_MISMATCH",
        )

        _append_trade(
            store,
            owner,
            reservation,
            source_sequence=5,
            trade_id="observation-trade-1",
            volume=1,
            price=99.123456789,
        )
        _assert_not_ready(
            _read(store, scope, "command-observation-trades"),
            "TRADE_PREFIX_MISMATCH",
        )
        duplicate = _append_trade(
            store,
            owner,
            reservation,
            source_sequence=6,
            trade_id="observation-trade-1",
            volume=1,
            price=99.123456789,
        )
        assert duplicate.duplicate is True
        _append_trade(
            store,
            owner,
            reservation,
            source_sequence=7,
            trade_id="observation-trade-2",
            volume=1,
            price=99.000000001,
        )

        before = store._connection.total_changes
        with localcontext() as context:
            context.prec = 6
            snapshot = _read(store, scope, "command-observation-trades")
        assert isinstance(snapshot, CtpOwnerCallbackObservationSnapshotV1)
        assert snapshot.volume_traded == 2
        assert snapshot.filled_quantity == 2
        assert snapshot.trade_fact_count == 2
        assert snapshot.trade_notional == Decimal("198.123456790")
        assert store._connection.total_changes == before
    finally:
        store.close()


@pytest.mark.unit
def test_owner_observation_cancel_ack_stays_fenced_until_terminal_order_matches_trade_prefix(
    tmp_path, monkeypatch
):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "owner-observation-cancel.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        reservation, _ = _submit(
            store,
            scope,
            lease,
            owner,
            intent_id="observation-cancel",
            command_id="command-observation-cancel",
            quantity=2,
        )
        monkeypatch.setattr(
            router_helpers,
            "_OrderTargetVerifier",
            _TwoUnitOrderTargetVerifier,
        )
        _stage_cancel_and_complete(
            store,
            scope,
            lease,
            owner,
            reservation,
            "cancel-observation-cancel",
            501,
        )
        session = store._active_ctp_callback_sessions[owner.owner_intent_id]
        cancel = store.read_ctp_dispatch_command(scope, "cancel-observation-cancel")
        action = cancel.correlation_key
        store.append_ctp_callback_ingress(
            owner,
            _record(
                owner,
                "OnRspOrderAction",
                4,
                phase="ACTIVE",
                callback_class="ROUTEABLE",
                request_id=action.native_request_id,
                is_last=True,
                fields=(
                    (0, "BrokerID", "broker"),
                    (0, "InvestorID", "user"),
                    (0, "UserID", "user"),
                    (0, "InstrumentID", "rb2710"),
                    (0, "OrderRef", reservation.order_ref),
                    (0, "ExchangeID", "SHFE"),
                    (0, "OrderSysID", "sys-" + reservation.order_ref),
                    (0, "FrontID", session.dispatch_front_id),
                    (0, "SessionID", session.dispatch_session_id),
                    (0, "RequestID", action.native_request_id),
                    (0, "OrderActionRef", action.native_action_ref),
                    (0, "ActionFlag", "0"),
                    (1, "ErrorID", 0),
                ),
            ),
        )
        _adapter_for(store, owner).apply_next_ingress(
            callback_verifier=_SnapshotOrderCallbackVerifier(),
            trade_fact_verifier=_TradeVerifier(),
        )
        cancel_projection = store.read_ctp_dispatch_projection(
            scope, "cancel-observation-cancel"
        )
        assert cancel_projection.cancel_action.action_state == "ACKNOWLEDGED"
        assert cancel_projection.cancel_action.terminal is False

        _append_trade(
            store,
            owner,
            reservation,
            source_sequence=5,
            trade_id="observation-cancel-fill",
            volume=1,
            price=99.25,
        )

        before = store._connection.total_changes
        _assert_not_ready(
            _read(
                store,
                scope,
                "command-observation-cancel",
                cancel_command_id="cancel-observation-cancel",
            ),
            "ACCOUNT_FENCED",
        )
        assert store._connection.total_changes == before

        _append_order(
            store,
            owner,
            scope,
            reservation,
            source_sequence=6,
            quantity=2,
            volume_traded=1,
            status="5",
        )
        snapshot = _read(
            store,
            scope,
            "command-observation-cancel",
            cancel_command_id="cancel-observation-cancel",
        )
        assert isinstance(snapshot, CtpOwnerCallbackObservationSnapshotV1)
        assert snapshot.provider_state == "CANCELLED"
        assert snapshot.volume_traded == 1
        assert snapshot.trade_fact_count == 1
        assert snapshot.trade_notional == Decimal("99.25")
        assert snapshot.cancel_command_id == "cancel-observation-cancel"
        assert snapshot.cancel_action_state == "ACKNOWLEDGED"
    finally:
        store.close()


@pytest.mark.unit
def test_owner_observation_unknown_is_read_only_and_keeps_poisoned_owner_fence(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "owner-observation-unknown.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        session = store._active_ctp_callback_sessions[owner.owner_intent_id]
        reservation = _reserve(
            store, scope, lease, session.session_generation_id, "observation-unknown"
        )
        command_id = "command-observation-unknown"
        payload = {
            "BrokerID": "broker",
            "InvestorID": "user",
            "UserID": "user",
            "InstrumentID": "rb2710",
            "ExchangeID": "SHFE",
            "OrderRef": reservation.order_ref,
            "Direction": "0",
            "CombOffsetFlag": "0",
            "CombHedgeFlag": "1",
            "LimitPrice": 100.0,
            "VolumeTotalOriginal": 1,
            "OrderPriceType": "2",
            "TimeCondition": "3",
        }
        unclaimed, claim = _queue_and_complete(
            store,
            scope,
            lease,
            owner,
            reservation,
            command_id,
            payload,
            72,
            claim_command=False,
        )
        assert claim is None
        store.fail_ctp_dispatch_command_before_native(
            scope,
            unclaimed,
            owner_handle=owner,
            writer_lease=lease,
            failure_code="queue_publish_failure",
        )
        command_before = store.read_ctp_dispatch_command(scope, command_id)
        owner_before = store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        before_changes = store._connection.total_changes
        _assert_not_ready(_read(store, scope, command_id), "COMMAND_UNKNOWN")
        assert store._connection.total_changes == before_changes
        command_after = store.read_ctp_dispatch_command(scope, command_id)
        owner_after = store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert command_after == command_before
        assert tuple(owner_after) == tuple(owner_before)
        assert command_after.status == "UNKNOWN"
    finally:
        store.close()


@pytest.mark.unit
def test_owner_observation_uses_one_wal_snapshot_during_second_connection_write(tmp_path, monkeypatch):
    path = tmp_path / "owner-observation-wal.sqlite3"
    scope = _scope()
    store = SqliteExecutionStore(path)
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        command_id = "command-observation-wal"
        reservation, _ = _submit(
            store, scope, lease, owner, intent_id="observation-wal", command_id=command_id,
        )
        _append_order(store, owner, scope, reservation, source_sequence=4)
        original = store.read_ctp_dispatch_projection
        writes = []

        def read_then_write(*args, **kwargs):
            projection = original(*args, **kwargs)
            if not writes:
                # Inject a deterministic committed competing projection after
                # the first SELECT. No thread or sleep is needed in WAL mode.
                with sqlite3.connect(path) as writer:
                    assert writer.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
                    changed = writer.execute(
                        "UPDATE ctp_dispatch_order_projection SET provider_state = 'CANCELLED', "
                        "terminal = 1 WHERE account_key = ? AND runtime_order_id = ?",
                        (scope.account_key, reservation.runtime_order_id),
                    )
                    assert changed.rowcount == 1
                writes.append(True)
            return projection

        monkeypatch.setattr(store, "read_ctp_dispatch_projection", read_then_write)
        snapshot = _read(store, scope, command_id)
        assert writes == [True]
        assert isinstance(snapshot, CtpOwnerCallbackObservationSnapshotV1)
        assert snapshot.provider_state == "ACKNOWLEDGED"
        assert not store._connection.in_transaction
        # A fresh snapshot sees the competing row and rejects its unmatched
        # callback anchor instead of reporting a fabricated cancellation.
        _assert_not_ready(_read(store, scope, command_id), "PROVIDER_ORDER_NOT_READY")
        assert not store._connection.in_transaction
    finally:
        store.close()


@pytest.mark.unit
def test_native_trade_price_identity_ignores_decimal_precision_and_traps():
    from bt_api_execution import map_ctp_native_trade_fact_v2
    from test_ctp_native_callbacks import _correlation, _session, _trade_field

    session = _session(_correlation("SUBMIT"))
    cases = (
        (100, "100"),
        (Decimal("1.2300"), "1.23"),
        (Decimal("100.000"), "100"),
        (Decimal("99.123456789"), "99.123456789"),
        (99.000000001, "99.000000001"),
        (Decimal("12345678901234567890123456789012"), "12345678901234567890123456789012"),
        (Decimal("1E-32"), "0." + "0" * 31 + "1"),
    )
    for price, expected in cases:
        facts = []
        for precision in (1, 6, 28):
            with localcontext() as context:
                context.prec = precision
                context.traps[Inexact] = True
                context.traps[Rounded] = True
                context.clear_flags()
                fact = map_ctp_native_trade_fact_v2(session, _trade_field(Price=price))
                assert dict(fact.native_fields)["Price"] == expected
                assert not context.flags[Inexact]
                assert not context.flags[Rounded]
                facts.append(fact)
        assert facts[0] == facts[1] == facts[2]
