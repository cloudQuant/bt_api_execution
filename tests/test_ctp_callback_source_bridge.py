from __future__ import annotations

import hashlib
import queue
import threading
import uuid
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from bt_api_execution import (
    ContractValidationError,
    CtpCancelTarget,
    CtpDispatchAuthority,
    CtpNativeCallbackSourceBridge,
    CtpOrderRefSeedProof,
    ExecutionScope,
    SqliteExecutionStore,
    ctp_native_callback_source_facts,
    ctp_native_session_generation_id,
)


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class _FakeTraderClient:
    """Small fake for the pinned TraderClient lifecycle and callback queue."""

    def __init__(self) -> None:
        self._query_state_lock = threading.RLock()
        self._api = object()
        self._spi = SimpleNamespace(
            _c=self,
            _native_api=self._api,
            _native_spi_source_id=uuid.uuid4().hex,
        )
        self._session_native_api = self._api
        self._callback_source_instance_id = uuid.uuid4().hex
        self._native_client_epoch = uuid.uuid4().hex
        self._native_api_source_id = uuid.uuid4().hex
        self._native_api_generation = 3
        self._connection_generation = 7
        self._bound_broker_id = "9999"
        self._bound_user_id = "investor"
        self._bound_front = "tcp://fake-front"
        self._session_native_front = self._bound_front
        self._trading_day = "20260925"
        self._front_id = 4
        self._session_id = 91
        self._connected = True
        self._login_state = "logged_in"
        self._callback_source_sequence = 0
        self._events: queue.Queue[object] = queue.Queue()

    def _bound_identity_is_current(self, *, require_active_front: bool = False) -> bool:
        return require_active_front and self._session_native_front == self._bound_front

    def wait_native_callback_event(self, timeout: float = 5.0):
        try:
            return self._events.get(timeout=timeout)
        except queue.Empty:
            return None


class _FakeActionAuthorityVerifier:
    def verify_action(self, command, *, now_ns):
        return CtpDispatchAuthority(
            authority_type="ctp_dispatch_authority.v1",
            command_binding_sha256=command.authority_binding_sha256,
            approval_use_id=command.approval_use_id,
            approval_digest=command.approval_digest,
            source_digest_sha256=_sha(b"fake local action source"),
            verifier_id="fake-callback-source-test",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + 5_000_000_000,
        )


@dataclass
class _BoundDispatch:
    store: SqliteExecutionStore
    scope: ExecutionScope
    lease: object
    command_id: str
    client: _FakeTraderClient
    source_facts: dict[str, str | int | bool]
    operation: str


def _stage_dispatched_command(
    tmp_path,
    *,
    operation: str = "SUBMIT",
    include_source_facts: bool = True,
    source_binding_override: dict[str, object] | None = None,
) -> _BoundDispatch:
    client = _FakeTraderClient()
    source_facts = ctp_native_callback_source_facts(client)
    generation = ctp_native_session_generation_id(
        source_facts["native_api_generation"],
        source_facts["connection_generation"],
        source_facts["native_client_epoch"],
        source_facts["login_front_id"],
        source_facts["login_session_id"],
    )
    scope = ExecutionScope(
        "CTP", "simulation", "source-bridge-account", "source-bridge", "20260925"
    )
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    lease = store.acquire_or_renew_lease(scope, "source-bridge-test", ttl_ns=30_000_000_000)
    reservation = store.seed_ctp_order_ref_and_reserve_identity(
        scope,
        CtpOrderRefSeedProof(
            trading_day=scope.trading_day,
            native_max_order_ref="000000000010",
            legacy_ledger_max_order_ref="000000000012",
            legacy_ledger_sha256=_sha(b"fake legacy seed"),
        ),
        "source-bridge-intent",
        "bt-managed-v1:" + _sha(b"source-bridge-intent"),
        writer_lease=lease,
    )
    session_binding = {
        "session_identity": "opaque-session-v1",
        "session_generation_id": generation,
        "dispatch_front_id": source_facts["login_front_id"],
        "dispatch_session_id": source_facts["login_session_id"],
    }
    if include_source_facts:
        persisted_source_facts = dict(source_facts)
        persisted_source_facts.update(source_binding_override or {})
        session_binding["native_callback_source"] = persisted_source_facts
    command_id = "source-bridge-command"
    if operation == "SUBMIT":
        command = store.stage_ctp_dispatch_command(
            scope,
            command_id,
            "SUBMIT",
            {"InstrumentID": "rb2710", "OrderRef": reservation.order_ref},
            approval_use_id="source-bridge-approval-use",
            approval_digest=_sha(b"fake approval"),
            session_binding=session_binding,
            writer_lease=lease,
            managed_intent_id=reservation.managed_intent_id,
            order_ref=reservation.order_ref,
            session_generation_id=generation,
            dispatch_front_id=source_facts["login_front_id"],
            dispatch_session_id=source_facts["login_session_id"],
            native_request_id=17,
        )
    else:
        target = CtpCancelTarget(
            order_ref=reservation.order_ref,
            exchange_id="SHFE",
            order_sys_id="sys-order-17",
            front_id=source_facts["login_front_id"],
            session_id=source_facts["login_session_id"],
        )
        command = store.stage_ctp_dispatch_command(
            scope,
            command_id,
            "CANCEL",
            {
                "OrderRef": target.order_ref,
                "ExchangeID": target.exchange_id,
                "OrderSysID": target.order_sys_id,
                "FrontID": target.front_id,
                "SessionID": target.session_id,
                "ActionFlag": "0",
            },
            approval_use_id="source-bridge-approval-use",
            approval_digest=_sha(b"fake approval"),
            session_binding=session_binding,
            writer_lease=lease,
            cancel_target=target,
            managed_action_id="source-bridge-cancel-action",
            session_generation_id=generation,
            dispatch_front_id=source_facts["login_front_id"],
            dispatch_session_id=source_facts["login_session_id"],
            native_request_id=17,
            native_action_ref="action-ref-29",
        )
    claimed = store.claim_ctp_dispatch_command(
        scope,
        command.command_id,
        writer_lease=lease,
        authority_verifier=_FakeActionAuthorityVerifier(),
    )
    assert claimed is not None and claimed.status == "CLAIMED"
    return _BoundDispatch(
        store=store,
        scope=scope,
        lease=lease,
        command_id=command_id,
        client=client,
        source_facts=source_facts,
        operation=operation,
    )


def _source_event(
    client: _FakeTraderClient,
    *,
    event_type: str = "OnRtnOrder",
    sequence: int = 1,
    overrides: dict[str, object] | None = None,
):
    if event_type == "OnRtnOrder":
        raw_fields: dict[str, object] = {
            "BrokerID": client._bound_broker_id,
            "InvestorID": client._bound_user_id,
            "UserID": client._bound_user_id,
            "InstrumentID": "rb2710",
            "RequestID": 17,
            "OrderRef": "000000000013",
            "ExchangeID": "SHFE",
            "OrderSysID": "sys-order-17",
            "FrontID": client._front_id,
            "SessionID": client._session_id,
            "TradingDay": client._trading_day,
            "NotifySequence": 381,
        }
    else:
        raw_fields = {
            "BrokerID": client._bound_broker_id,
            "InvestorID": client._bound_user_id,
            "UserID": client._bound_user_id,
            "InstrumentID": "rb2710",
            "RequestID": 17,
            "OrderActionRef": "action-ref-29",
            "OrderRef": "000000000013",
            "ExchangeID": "SHFE",
            "OrderSysID": "sys-order-17",
            "FrontID": client._front_id,
            "SessionID": client._session_id,
            "ActionFlag": "0",
            "ActionDate": "20260925",
            "ActionTime": "09:32:10",
            "OrderActionStatus": "3",
            "nRequestID": 17,
            "bIsLast": True,
            "ErrorID": 0,
        }
    raw_fields.update(overrides or {})
    spi = client._spi
    stable_source_key = (
        "ctp-trader-callback-v1",
        client._callback_source_instance_id,
        client._native_client_epoch,
        client._native_api_source_id,
        spi._native_spi_source_id,
        client._native_api_generation,
        client._connection_generation,
        sequence,
    )
    event = SimpleNamespace(
        event_type=event_type,
        source_instance_id=client._callback_source_instance_id,
        native_client_epoch=client._native_client_epoch,
        native_api_source_id=client._native_api_source_id,
        native_spi_source_id=spi._native_spi_source_id,
        source_sequence=sequence,
        callback_monotonic_ns=123456,
        native_api_generation=client._native_api_generation,
        connection_generation=client._connection_generation,
        login_verified=True,
        login_broker_id=client._bound_broker_id,
        login_investor_id=client._bound_user_id,
        login_front=client._session_native_front,
        login_trading_day=client._trading_day,
        login_front_id=client._front_id,
        login_session_id=client._session_id,
        callback_session_matches_login=True,
        raw_correlation_fields=tuple(raw_fields.items()),
        managed_session_epoch=None,
        managed_session_epoch_bound=False,
        scope_binding="unbound",
        trust_boundary="source_facts_only",
        stable_source_key=stable_source_key,
    )
    return event


@pytest.mark.unit
def test_bridge_derives_context_and_maps_source_queue_event_with_exact_lifecycle_facts(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    try:
        bound.client._events.put(_source_event(bound.client))
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )

        result = bridge.next_envelope(timeout=0)

        assert result is not None
        assert result.callback_key.correlation_key.command_id == bound.command_id
        assert (
            result.callback_envelope.source_scope.session_generation_id
            == result.callback_key.correlation_key.session_generation_id
        )
        payload = result.to_payload()
        assert payload["envelope_type"] == "ctp_lifecycle_bound_native_callback_envelope.v1"
        assert payload["callback_envelope"]["envelope_type"] == "ctp_native_callback_envelope.v1"
        source = payload["lifecycle_binding"]["source_facts"]
        assert source == bound.source_facts
        assert source["native_api_source_id"] == bound.client._native_api_source_id
        assert source["native_spi_source_id"] == bound.client._spi._native_spi_source_id
        assert source["native_api_generation"] == bound.client._native_api_generation
        assert source["connection_generation"] == bound.client._connection_generation
        assert source["login_verified"] is True
        assert source["login_front_id"] == 4
        assert source["login_session_id"] == 91
        source_event = payload["source_event"]
        assert source_event["source_sequence"] == 1
        assert source_event["native_api_source_id"] == bound.client._native_api_source_id
        assert source_event["native_spi_source_id"] == bound.client._spi._native_spi_source_id
        assert source_event["native_api_generation"] == bound.client._native_api_generation
        assert source_event["connection_generation"] == bound.client._connection_generation
        assert source_event["login_verified"] is True
        assert source_event["callback_session_matches_login"] is True
        assert source_event["scope_binding"] == "unbound"
        assert payload["callback_envelope"]["native_fields"]["RequestID"] == 17
        with pytest.raises(
            ContractValidationError, match="trusted CTP callback verification failed"
        ):
            bound.store.apply_ctp_verified_dispatch_callback(
                bound.scope,
                bound.command_id,
                result.callback_key,
                payload,
                writer_lease=bound.lease,
            )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 0
        )
    finally:
        bound.store.close()


@pytest.mark.unit
def test_bridge_maps_terminal_order_action_source_event(tmp_path):
    bound = _stage_dispatched_command(tmp_path, operation="CANCEL")
    try:
        bound.client._events.put(_source_event(bound.client, event_type="OnRspOrderAction"))
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )

        result = bridge.next_envelope(timeout=0)

        assert result is not None
        assert result.callback_envelope.source_callback == "OnRspOrderAction"
        assert result.callback_envelope.response_is_last is True
        assert result.callback_key.native_action_ref == "action-ref-29"
        assert result.to_payload()["callback_envelope"]["response_error_id"] == 0
    finally:
        bound.store.close()


@pytest.mark.unit
def test_bridge_rejects_non_integer_terminal_action_request_id(tmp_path):
    bound = _stage_dispatched_command(tmp_path, operation="CANCEL")
    try:
        bound.client._events.put(
            _source_event(
                bound.client,
                event_type="OnRspOrderAction",
                overrides={"nRequestID": "17"},
            )
        )
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )

        with pytest.raises(ContractValidationError, match="request ID is invalid"):
            bridge.next_envelope(timeout=0)
    finally:
        bound.store.close()


@pytest.mark.unit
def test_bridge_denies_missing_persisted_source_binding_without_legacy_fallback(tmp_path):
    bound = _stage_dispatched_command(tmp_path, include_source_facts=False)
    try:
        with pytest.raises(ContractValidationError, match="no callback source lifecycle binding"):
            CtpNativeCallbackSourceBridge.bind_after_login(
                store=bound.store,
                scope=bound.scope,
                command_id=bound.command_id,
                native_trader_client=bound.client,
            )
        assert bound.client._events.empty()
        assert (
            bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id).status == "CLAIMED"
        )
    finally:
        bound.store.close()


@pytest.mark.unit
def test_bridge_rejects_caller_supplied_source_facts_that_do_not_echo_live_source(tmp_path):
    bound = _stage_dispatched_command(
        tmp_path,
        source_binding_override={"native_api_source_id": uuid.uuid4().hex},
    )
    try:
        with pytest.raises(ContractValidationError, match="source binding differs from live login"):
            CtpNativeCallbackSourceBridge.bind_after_login(
                store=bound.store,
                scope=bound.scope,
                command_id=bound.command_id,
                native_trader_client=bound.client,
            )
    finally:
        bound.store.close()


@pytest.mark.unit
@pytest.mark.parametrize("field_name", ["native_api_source_id", "native_spi_source_id"])
def test_bridge_rejects_forged_source_id_and_poisons_itself(tmp_path, field_name: str):
    bound = _stage_dispatched_command(tmp_path)
    try:
        event = _source_event(bound.client)
        setattr(event, field_name, uuid.uuid4().hex)
        bound.client._events.put(event)
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )

        with pytest.raises(ContractValidationError, match="origin facts differ"):
            bridge.next_envelope(timeout=0)
        with pytest.raises(ContractValidationError, match="bridge is closed"):
            bridge.next_envelope(timeout=0)
    finally:
        bound.store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("native_api_generation", 4),
        ("connection_generation", 8),
        ("login_trading_day", "20260926"),
        ("callback_session_matches_login", False),
    ],
)
def test_bridge_rejects_event_generation_or_login_fact_drift(
    tmp_path, field_name: str, value: object
):
    bound = _stage_dispatched_command(tmp_path)
    try:
        event = _source_event(bound.client)
        setattr(event, field_name, value)
        bound.client._events.put(event)
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )

        with pytest.raises(ContractValidationError, match="origin facts differ"):
            bridge.next_envelope(timeout=0)
    finally:
        bound.store.close()


@pytest.mark.unit
@pytest.mark.parametrize("event_type", ["OnRtnTrade", "OnRspOrderInsert", "OnErrRtnOrderInsert"])
def test_bridge_rejects_trade_and_insert_callbacks_without_fallback(tmp_path, event_type: str):
    bound = _stage_dispatched_command(tmp_path)
    try:
        event = _source_event(bound.client)
        event.event_type = event_type
        bound.client._events.put(event)
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )

        with pytest.raises(ContractValidationError, match="unsupported native callback event type"):
            bridge.next_envelope(timeout=0)
    finally:
        bound.store.close()


@pytest.mark.unit
def test_bridge_rejects_unbound_or_changed_live_login_before_polling(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    try:
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )
        bound.client._login_state = "disconnected"
        bound.client._events.put(_source_event(bound.client))

        with pytest.raises(ContractValidationError, match="not logged in"):
            bridge.next_envelope(timeout=0)
        assert bound.client._events.qsize() == 1
    finally:
        bound.store.close()


@pytest.mark.unit
def test_bridge_requires_source_event_sequence_after_persisted_baseline(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    try:
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )
        bound.client._callback_source_sequence = 1
        bound.client._events.put(_source_event(bound.client, sequence=2))

        with pytest.raises(ContractValidationError, match="source event identity or sequence"):
            bridge.next_envelope(timeout=0)
    finally:
        bound.store.close()
