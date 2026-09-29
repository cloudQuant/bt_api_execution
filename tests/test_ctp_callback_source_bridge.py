from __future__ import annotations

import hashlib
import queue
import sqlite3
import threading
import time
import traceback
import uuid
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

import pytest

from bt_api_execution import (
    ContractValidationError,
    CtpCancelTarget,
    CtpDispatchAuthority,
    CtpDispatchReceipt,
    CtpNativeCallbackLedgerAdapter,
    CtpNativeCallbackSourceBridge,
    CtpOrderRefLegacyMapping,
    CtpOrderRefSeedProof,
    CtpVerifiedCallbackEvidence,
    CtpVerifiedOrderTargetProjection,
    DurableStoreError,
    ExecutionScope,
    InvalidStateTransition,
    SqliteExecutionStore,
    ctp_native_callback_source_facts,
    ctp_native_session_generation_id,
    payload_sha256,
)


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _ctp_account_ref(label: bytes) -> str:
    return "ctp-account-ref.v1:" + _sha(label)


def _lease(store, scope, owner: str):
    handle = getattr(store, "_test_ctp_account_family_owner", None)
    if handle is None:
        handle = store.acquire_ctp_account_family_owner(scope)
        store._test_ctp_account_family_owner = handle
    return store.acquire_or_renew_lease(
        scope,
        owner,
        ttl_ns=30_000_000_000,
        ctp_account_family_owner=handle,
    )


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
        self._callback_consumer_lock = threading.Lock()
        self._callback_consumer_token: object | None = None
        self._callback_consumer_generation: tuple[object, object, object] | None = None
        self._callback_consumer_waiting: object | None = None
        self._callback_consumer_last_released: object | None = None
        self._legacy_callback_waiters = 0

    def _bound_identity_is_current(self, *, require_active_front: bool = False) -> bool:
        return require_active_front and self._session_native_front == self._bound_front

    def wait_native_callback_event(self, timeout: float = 5.0):
        with self._callback_consumer_lock:
            if self._callback_consumer_token is not None:
                raise RuntimeError("exclusive callback consumer owns the source queue")
            self._legacy_callback_waiters += 1
        try:
            return self._events.get(timeout=timeout)
        except queue.Empty:
            return None
        finally:
            with self._callback_consumer_lock:
                self._legacy_callback_waiters -= 1

    def _claim_native_callback_event_consumer(self) -> object:
        with self._callback_consumer_lock:
            if self._callback_consumer_token is not None or self._legacy_callback_waiters:
                raise RuntimeError("callback source queue already has a consumer")
            token = object()
            self._callback_consumer_token = token
            self._callback_consumer_generation = (
                self._callback_source_instance_id,
                self._native_client_epoch,
                self._native_api_generation,
            )
            return token

    def _wait_native_callback_event_for_consumer(self, token: object, timeout: float = 5.0):
        with self._callback_consumer_lock:
            if (
                self._callback_consumer_token is not token
                or self._callback_consumer_waiting is not None
                or self._callback_consumer_generation
                != (
                    self._callback_source_instance_id,
                    self._native_client_epoch,
                    self._native_api_generation,
                )
            ):
                raise RuntimeError("callback source queue consumer lease is stale")
            self._callback_consumer_waiting = token
        try:
            try:
                event = self._events.get(timeout=timeout)
            except queue.Empty:
                event = None
        finally:
            with self._callback_consumer_lock:
                self._callback_consumer_waiting = None
                if (
                    self._callback_consumer_token is not token
                    or self._callback_consumer_generation
                    != (
                        self._callback_source_instance_id,
                        self._native_client_epoch,
                        self._native_api_generation,
                    )
                ):
                    raise RuntimeError("callback source queue consumer lease was revoked")
        return event

    def _release_native_callback_event_consumer(self, token: object) -> None:
        with self._callback_consumer_lock:
            if token is self._callback_consumer_last_released:
                return
            if self._callback_consumer_token is not token:
                self._callback_consumer_last_released = token
                return
            if self._callback_consumer_waiting is token:
                raise RuntimeError("callback source queue consumer still has an active waiter")
            self._callback_consumer_token = None
            self._callback_consumer_generation = None
            self._callback_consumer_last_released = token

    def _revoke_native_callback_event_consumer(self) -> None:
        """Test hook for source-side lease loss while the client login stays current."""

        with self._callback_consumer_lock:
            self._callback_consumer_token = None
            self._callback_consumer_generation = None


def _logged_in_sdk_client_with_fake_api(client_module: Any) -> tuple[Any, Any]:
    client = client_module.TraderClient("tcp://fake-front", "9999", "fake-user", "fake-secret")

    class _FakeNativeApi:
        def __init__(self) -> None:
            self.authenticate_request_ids: list[int] = []
            self.login_request_ids: list[int] = []

        def ReqAuthenticate(self, _field, request_id: int) -> int:  # noqa: N802
            self.authenticate_request_ids.append(request_id)
            return 0

        def ReqUserLogin(self, _field, request_id: int) -> int:  # noqa: N802
            self.login_request_ids.append(request_id)
            return 0

        def RegisterSpi(self, _spi: object) -> None:  # noqa: N802
            return None

        def Release(self) -> None:  # noqa: N802
            return None

    api = _FakeNativeApi()
    client._api = api
    spi = client_module._TraderSpi(client, api)
    client._spi = spi
    spi.OnFrontConnected()
    spi.OnRspAuthenticate(
        None,
        SimpleNamespace(ErrorID=0, ErrorMsg=""),
        api.authenticate_request_ids[-1],
        True,
    )
    spi.OnRspUserLogin(
        SimpleNamespace(FrontID=7, SessionID=19, TradingDay="20260925", MaxOrderRef="90"),
        SimpleNamespace(ErrorID=0, ErrorMsg=""),
        api.login_request_ids[-1],
        True,
    )
    return client, spi


def _supports_source_queue_consumer_lease(client: object) -> bool:
    return all(
        callable(getattr(client, name, None))
        for name in (
            "_claim_native_callback_event_consumer",
            "_wait_native_callback_event_for_consumer",
            "_release_native_callback_event_consumer",
        )
    )


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


class _FakeCallbackLedgerVerifier:
    """Test-only verifier for a lifecycle-bound callback envelope."""

    def __init__(self, *, error: Exception | None = None, during_verify=None) -> None:
        self.error = error
        self.during_verify = during_verify
        self.calls = []

    def verify_callback(self, command, callback, callback_payload, *, now_ns):
        self.calls.append((command, callback, dict(callback_payload), now_ns))
        if self.error is not None:
            raise self.error
        if self.during_verify is not None:
            self.during_verify()
        assert callback_payload["envelope_type"] == (
            "ctp_lifecycle_bound_native_callback_envelope.v1"
        )
        source = callback_payload["source_event"]
        assert source["login_verified"] is True
        assert source["callback_session_matches_login"] is True
        return CtpVerifiedCallbackEvidence(
            evidence_type="ctp_verified_callback.v1",
            callback_key=callback,
            callback_payload_sha256=payload_sha256(callback_payload),
            projection_state="ACKNOWLEDGED",
            source_digest_sha256=payload_sha256(source),
            verifier_id="fake-lifecycle-callback-verifier",
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
    source_client: object | None = None,
) -> _BoundDispatch:
    client = (
        _FakeTraderClient() if source_client is None else cast("_FakeTraderClient", source_client)
    )
    source_facts = ctp_native_callback_source_facts(client)
    generation = ctp_native_session_generation_id(
        source_facts["native_api_generation"],
        source_facts["connection_generation"],
        source_facts["native_client_epoch"],
        source_facts["login_front_id"],
        source_facts["login_session_id"],
    )
    scope = ExecutionScope(
        "ctp",
        "simulation",
        _ctp_account_ref(b"source-bridge-account"),
        "source-bridge",
        "20260925",
    )
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    lease = _lease(store, scope, "source-bridge-test")
    source_digests = (
        ("backtrader_prototype", _sha(b"source bridge legacy prototype fixture")),
        ("sdk_jsonl", _sha(b"source bridge empty SDK ledger fixture")),
    )
    legacy_scope = ExecutionScope(
        "ctp", "simulation", scope.account_ref, "source-bridge-legacy", "20260924"
    )
    legacy_mapping = CtpOrderRefLegacyMapping(
        source_name="backtrader_prototype",
        account_key=scope.account_key,
        trading_day="20260924",
        scope_key=legacy_scope.key,
        managed_intent_id="source-bridge-legacy-intent",
        runtime_order_id="bt-managed-v1:" + _sha(b"source-bridge-legacy-intent"),
        order_ref="000000000012",
    )
    reservation = store.seed_ctp_order_ref_and_reserve_identity(
        scope,
        CtpOrderRefSeedProof(
            trading_day=scope.trading_day,
            native_max_order_ref="000000000010",
            legacy_ledger_max_order_ref="000000000012",
            legacy_ledger_sha256=payload_sha256(dict(source_digests)),
            account_key=scope.account_key,
            scope_key=scope.key,
            session_generation_id=generation,
            native_front_id=source_facts["login_front_id"],
            native_session_id=source_facts["login_session_id"],
            existing_native_order_refs=("000000000009", "000000000010"),
            legacy_source_sha256=source_digests,
            legacy_mappings=(legacy_mapping,),
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

        class _FakeTargetVerifier:
            def verify_order_target(self, exact_scope, exact_reservation, evidence, *, now_ns):
                return CtpVerifiedOrderTargetProjection(
                    account_key=exact_reservation.account_key,
                    scope_key=exact_reservation.scope_key,
                    trading_day=exact_reservation.trading_day,
                    managed_intent_id=exact_reservation.managed_intent_id,
                    runtime_order_id=exact_reservation.runtime_order_id,
                    order_ref=exact_reservation.order_ref,
                    account_fingerprint_sha256=_sha(b"fake account"),
                    registration_digest=_sha(b"fake registration"),
                    instrument_id="rb2710",
                    exchange_id=target.exchange_id,
                    session_generation_id=generation,
                    connection_generation=7,
                    query_front_id=source_facts["login_front_id"],
                    query_session_id=source_facts["login_session_id"],
                    query_request_id=81,
                    query_filters_sha256=_sha(b"fake filters"),
                    query_records_sha256=_sha(b"fake rows"),
                    source_evidence_sha256=_sha(b"fake native source"),
                    query_record_count=1,
                    query_match_count=1,
                    query_complete=True,
                    query_terminal=True,
                    query_timed_out=False,
                    query_error_id=None,
                    late_callback_count=0,
                    order_sys_id=target.order_sys_id,
                    front_id=target.front_id,
                    session_id=target.session_id,
                    provider_state="OPEN",
                    quantity=1,
                    traded_quantity=0,
                    remaining_quantity=1,
                    verifier_id="fake-source-bridge-target-verifier",
                    verified_at_ns=now_ns,
                    expires_at_ns=now_ns + 2_000_000_000,
                )

        target_projection = store.issue_ctp_order_target_projection(
            scope,
            reservation.managed_intent_id,
            {"fake_only": True},
            verifier=_FakeTargetVerifier(),
        )
        command = store.stage_ctp_dispatch_command(
            scope,
            command_id,
            "CANCEL",
            {
                "InstrumentID": "rb2710",
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
            cancel_target_projection=target_projection,
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


def _stage_followup_ready_command(bound, store, *, writer_lease=None):
    if writer_lease is None:
        writer_lease = bound.lease
    intent_id = "source-bridge-followup-intent"
    runtime_order_id = "bt-managed-v1:" + _sha(b"source-bridge-followup-runtime")
    reservation = store.reserve_ctp_order_identity(bound.scope, intent_id, runtime_order_id)
    original = store.read_ctp_dispatch_command(bound.scope, bound.command_id)
    assert original is not None and original.correlation_key is not None
    generation = original.correlation_key.session_generation_id
    session_binding = {
        "session_identity": "opaque-session-v1",
        "session_generation_id": generation,
        "dispatch_front_id": original.correlation_key.dispatch_front_id,
        "dispatch_session_id": original.correlation_key.dispatch_session_id,
        "native_callback_source": dict(bound.source_facts),
    }
    return store.stage_ctp_dispatch_command(
        bound.scope,
        "source-bridge-followup-command",
        "SUBMIT",
        {"InstrumentID": "rb2710", "OrderRef": reservation.order_ref},
        approval_use_id="source-bridge-followup-approval",
        approval_digest=_sha(b"source-bridge-followup-approval"),
        session_binding=session_binding,
        writer_lease=writer_lease,
        managed_intent_id=reservation.managed_intent_id,
        order_ref=reservation.order_ref,
        session_generation_id=generation,
        dispatch_front_id=original.correlation_key.dispatch_front_id,
        dispatch_session_id=original.correlation_key.dispatch_session_id,
        native_request_id=29,
    )


def _write_legacy_v11_resolved_guard(bound, *, correlation_digest=None):
    command = bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id)
    assert command is not None and command.correlation_key is not None
    guard_id = "f" * 32
    correlation_digest = (
        payload_sha256(command.correlation_key.to_payload())
        if correlation_digest is None
        else correlation_digest
    )
    # Recreate the guard-only schema-11 candidate shape. The final Store will
    # create projection tables after it snapshots these preexisting tables.
    bound.store._connection.execute("DROP TABLE ctp_order_target_projection_consumptions")
    bound.store._connection.execute("DROP TABLE ctp_order_target_projections")
    bound.store._connection.execute("DROP TABLE ctp_dispatch_callback_source_lifecycle_fences")
    bound.store._connection.executescript(
        """
        CREATE TABLE ctp_dispatch_callback_ingestion_guards (
            account_key TEXT NOT NULL,
            scope_key TEXT NOT NULL,
            command_id TEXT NOT NULL,
            guard_id TEXT NOT NULL,
            correlation_key_sha256 TEXT NOT NULL,
            session_binding_sha256 TEXT NOT NULL,
            created_at_ns INTEGER NOT NULL,
            PRIMARY KEY(account_key, command_id, guard_id)
        );
        CREATE TABLE ctp_dispatch_callback_ingestion_resolutions (
            account_key TEXT NOT NULL,
            scope_key TEXT NOT NULL,
            command_id TEXT NOT NULL,
            guard_id TEXT NOT NULL,
            correlation_key_sha256 TEXT NOT NULL,
            callback_key_sha256 TEXT NOT NULL,
            callback_payload_sha256 TEXT NOT NULL,
            resolved_at_ns INTEGER NOT NULL,
            PRIMARY KEY(account_key, command_id, guard_id)
        );
        """
    )
    bound.store._connection.execute(
        """
        INSERT INTO ctp_dispatch_callback_ingestion_guards(
            account_key, scope_key, command_id, guard_id,
            correlation_key_sha256, session_binding_sha256, created_at_ns
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            command.account_key,
            command.scope_key,
            command.command_id,
            guard_id,
            correlation_digest,
            command.session_binding_sha256,
            1,
        ),
    )
    bound.store._connection.execute(
        """
        INSERT INTO ctp_dispatch_callback_ingestion_resolutions(
            account_key, scope_key, command_id, guard_id,
            correlation_key_sha256, callback_key_sha256,
            callback_payload_sha256, resolved_at_ns
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            command.account_key,
            command.scope_key,
            command.command_id,
            guard_id,
            correlation_digest,
            _sha(b"legacy callback key"),
            _sha(b"legacy callback payload"),
            2,
        ),
    )
    bound.store._connection.execute(
        "UPDATE execution_meta SET value = '11' WHERE key = 'schema_version'"
    )
    return command, guard_id


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
            "OrderActionRef": 1,
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


def _unknown_receipt(command, *, outcome="UNKNOWN"):
    return CtpDispatchReceipt(
        receipt_type="ctp_dispatch_receipt.v2",
        command_id=command.command_id,
        account_key=command.account_key,
        scope_key=command.scope_key,
        trading_day=command.trading_day,
        operation=command.operation,
        request_payload_sha256=command.request_payload_sha256,
        reservation_managed_intent_id=command.reservation_managed_intent_id,
        order_ref=command.order_ref,
        cancel_target_order_ref=command.cancel_target_order_ref,
        cancel_target_exchange_id=command.cancel_target_exchange_id,
        cancel_target_order_sys_id=command.cancel_target_order_sys_id,
        cancel_target_front_id=command.cancel_target_front_id,
        cancel_target_session_id=command.cancel_target_session_id,
        approval_use_id=command.approval_use_id,
        approval_digest=command.approval_digest,
        session_binding_sha256=command.session_binding_sha256,
        outcome=outcome,
        native_receipt_payload={"kind": "fake-send", "outcome": outcome},
        correlation_key=command.correlation_key,
        local_queue_receipt_id=command.local_queue_receipt_id,
    )


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
        assert payload["callback_envelope"]["envelope_type"] == "ctp_native_callback_envelope.v2"
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
def test_bridge_requires_source_queue_exclusive_consumer_capability(tmp_path, monkeypatch):
    bound = _stage_dispatched_command(tmp_path)
    try:
        monkeypatch.setattr(bound.client, "_claim_native_callback_event_consumer", None)
        with pytest.raises(ContractValidationError, match="no exclusive queue consumer lease"):
            CtpNativeCallbackSourceBridge.bind_after_login(
                store=bound.store,
                scope=bound.scope,
                command_id=bound.command_id,
                native_trader_client=bound.client,
            )
        assert bound.client._events.empty()
    finally:
        bound.store.close()


@pytest.mark.unit
def test_bridge_exclusive_consumer_lease_rejects_competing_bridge_and_releases_on_close(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    try:
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )

        with pytest.raises(ContractValidationError, match="lease claim failed"):
            CtpNativeCallbackSourceBridge.bind_after_login(
                store=bound.store,
                scope=bound.scope,
                command_id=bound.command_id,
                native_trader_client=bound.client,
            )
        assert bound.client._callback_consumer_token is bridge._consumer_token
        with pytest.raises(RuntimeError, match="exclusive callback consumer"):
            bound.client.wait_native_callback_event(timeout=0)

        bridge.close()
        bridge.close()
        assert bound.client._callback_consumer_token is None

        replacement = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )
        replacement.close()
        assert bound.client._callback_consumer_token is None
    finally:
        bound.store.close()


@pytest.mark.unit
def test_bridge_poisons_and_releases_a_lost_consumer_lease(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    try:
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )
        bound.client._events.put(_source_event(bound.client))
        bound.client._revoke_native_callback_event_consumer()

        with pytest.raises(ContractValidationError, match="source queue or lifecycle check failed"):
            bridge.next_envelope(timeout=0)
        with pytest.raises(ContractValidationError, match="bridge is closed"):
            bridge.next_envelope(timeout=0)
        assert bound.client._callback_consumer_token is None
        assert not bound.client._events.empty()
        replacement = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=bound.client,
        )
        replacement.close()
    finally:
        bound.store.close()


@pytest.mark.unit
def test_close_wakes_fake_source_poll_and_discards_its_result(tmp_path, monkeypatch):
    bound = _stage_dispatched_command(tmp_path)
    client = bound.client
    bridge = CtpNativeCallbackSourceBridge.bind_after_login(
        store=bound.store,
        scope=bound.scope,
        command_id=bound.command_id,
        native_trader_client=client,
    )
    poll_waiting = threading.Event()
    release_wakeup = threading.Event()
    poll_finished = threading.Event()
    poll_errors: list[BaseException] = []

    def wait_for_lease_release(token: object, timeout: float = 5.0) -> object | None:
        with client._callback_consumer_lock:
            if client._callback_consumer_token is not token:
                raise RuntimeError("callback source queue consumer lease is stale")
            if client._callback_consumer_waiting is not None:
                raise RuntimeError("callback source queue already has a waiter")
            client._callback_consumer_waiting = token
        poll_waiting.set()
        release_wakeup.wait(timeout=timeout)
        with client._callback_consumer_lock:
            client._callback_consumer_waiting = None
            if client._callback_consumer_token is not token:
                raise RuntimeError("callback source queue consumer lease was revoked")
        return None

    def release_and_wake(token: object) -> None:
        with client._callback_consumer_lock:
            if client._callback_consumer_token is not token:
                raise RuntimeError("callback source queue consumer lease is stale")
            client._callback_consumer_token = None
            client._callback_consumer_generation = None
            client._callback_consumer_last_released = token
        release_wakeup.set()

    monkeypatch.setattr(client, "_wait_native_callback_event_for_consumer", wait_for_lease_release)
    monkeypatch.setattr(client, "_release_native_callback_event_consumer", release_and_wake)

    def poll() -> None:
        try:
            bridge.next_envelope(timeout=5.0)
        except BaseException as exc:  # retain the background failure for assertions
            poll_errors.append(exc)
        finally:
            poll_finished.set()

    poll_thread = threading.Thread(target=poll, daemon=True)
    poll_thread.start()
    try:
        assert poll_waiting.wait(timeout=1)
        close_started = time.monotonic()
        bridge.close()
        assert time.monotonic() - close_started < 0.5
        assert poll_finished.wait(timeout=1)
        poll_thread.join(timeout=1)
        assert not poll_thread.is_alive()
        assert len(poll_errors) == 1
        assert isinstance(poll_errors[0], ContractValidationError)
        assert "callback may have been consumed and discarded" in str(poll_errors[0])
        assert client._callback_consumer_token is None
        assert client._callback_consumer_waiting is None
    finally:
        release_wakeup.set()
        bridge.close()
        poll_thread.join(timeout=1)
        bound.store.close()


@pytest.mark.unit
def test_close_wins_after_fake_sdk_dequeue_and_discards_callback(tmp_path, monkeypatch):
    bound = _stage_dispatched_command(tmp_path)
    bridge = CtpNativeCallbackSourceBridge.bind_after_login(
        store=bound.store,
        scope=bound.scope,
        command_id=bound.command_id,
        native_trader_client=bound.client,
    )
    bound.client._events.put(_source_event(bound.client))
    original_wait = bound.client._wait_native_callback_event_for_consumer
    dequeue_finished = threading.Event()
    resume_poll = threading.Event()
    poll_finished = threading.Event()
    poll_errors: list[BaseException] = []

    def pause_after_dequeue(token: object, timeout: float = 5.0) -> object | None:
        event = original_wait(token, timeout=timeout)
        dequeue_finished.set()
        if not resume_poll.wait(timeout=1):
            raise RuntimeError("test did not release the post-dequeue wait")
        return event

    monkeypatch.setattr(
        bound.client, "_wait_native_callback_event_for_consumer", pause_after_dequeue
    )

    def poll() -> None:
        try:
            bridge.next_envelope(timeout=0.6)
        except BaseException as exc:  # retain the background failure for assertions
            poll_errors.append(exc)
        finally:
            poll_finished.set()

    poll_thread = threading.Thread(target=poll, daemon=True)
    poll_thread.start()
    try:
        assert dequeue_finished.wait(timeout=1)
        bridge.close()
        assert bound.client._callback_consumer_token is None
        resume_poll.set()
        assert poll_finished.wait(timeout=1)
        poll_thread.join(timeout=1)
        assert not poll_thread.is_alive()
        assert len(poll_errors) == 1
        assert isinstance(poll_errors[0], ContractValidationError)
        assert "callback may have been consumed and discarded" in str(poll_errors[0])
        assert bound.client._events.empty()
        with pytest.raises(ContractValidationError, match="bridge is closed"):
            bridge.next_envelope(timeout=0)
    finally:
        resume_poll.set()
        bridge.close()
        poll_thread.join(timeout=1)
        bound.store.close()


@pytest.mark.unit
def test_close_wakes_real_trader_client_poll_and_discards_its_result(tmp_path):
    client_module = pytest.importorskip("bt_api_ctp.ctp.client")
    client, _spi = _logged_in_sdk_client_with_fake_api(client_module)
    if not _supports_source_queue_consumer_lease(client):
        pytest.skip("installed bt_api_ctp has no callback queue consumer lease")

    bound = _stage_dispatched_command(tmp_path, source_client=client)
    bridge = None
    poll_thread = None
    poll_errors: list[BaseException] = []
    poll_finished = threading.Event()
    try:
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=client,
        )

        def poll() -> None:
            try:
                bridge.next_envelope(timeout=0.6)
            except BaseException as exc:  # retain the background failure for assertions
                poll_errors.append(exc)
            finally:
                poll_finished.set()

        poll_thread = threading.Thread(target=poll, daemon=True)
        poll_thread.start()
        with client._query_state_lock:
            waiting = client._native_callback_event_condition.wait_for(
                lambda: (
                    client._native_callback_consumer_lease is not None
                    and client._native_callback_consumer_lease.waiting
                ),
                timeout=1,
            )
        assert waiting

        with pytest.raises(ContractValidationError, match="bridge already has a poll in progress"):
            bridge.next_envelope(timeout=0)

        close_started = time.monotonic()
        bridge.close()
        close_elapsed = time.monotonic() - close_started
        assert close_elapsed < 0.5
        assert poll_finished.wait(timeout=1)
        poll_thread.join(timeout=1)
        assert not poll_thread.is_alive()
        assert len(poll_errors) == 1
        assert isinstance(poll_errors[0], ContractValidationError)
        assert "callback may have been consumed and discarded" in str(poll_errors[0])
        assert client._native_callback_consumer_lease is None
        with pytest.raises(ContractValidationError, match="bridge is closed"):
            bridge.next_envelope(timeout=0)
    finally:
        if bridge is not None:
            bridge.close()
        if poll_thread is not None:
            poll_thread.join(timeout=1)
        bound.store.close()
        client.stop()


@pytest.mark.unit
def test_close_discards_real_sdk_event_dequeued_before_close_wins(tmp_path, monkeypatch):
    client_module = pytest.importorskip("bt_api_ctp.ctp.client")
    client, spi = _logged_in_sdk_client_with_fake_api(client_module)
    if not _supports_source_queue_consumer_lease(client):
        pytest.skip("installed bt_api_ctp has no callback queue consumer lease")

    bound = _stage_dispatched_command(tmp_path, source_client=client)
    bridge = None
    poll_thread = None
    poll_errors: list[BaseException] = []
    poll_finished = threading.Event()
    dequeue_finished = threading.Event()
    resume_poll = threading.Event()
    try:
        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            native_trader_client=client,
        )
        source = bound.source_facts
        spi.OnRtnOrder(
            SimpleNamespace(
                BrokerID=source["login_broker_id"],
                InvestorID=source["login_investor_id"],
                UserID=source["login_investor_id"],
                InstrumentID="rb2710",
                RequestID=17,
                OrderRef="000000000013",
                ExchangeID="SHFE",
                OrderSysID="sys-order-17",
                FrontID=source["login_front_id"],
                SessionID=source["login_session_id"],
                TradingDay=source["login_trading_day"],
                NotifySequence=381,
            )
        )
        original_wait = client._wait_native_callback_event_for_consumer

        def pause_after_dequeue(token: object, timeout: float = 5.0) -> object | None:
            event = original_wait(token, timeout=timeout)
            dequeue_finished.set()
            if not resume_poll.wait(timeout=1):
                raise RuntimeError("test did not release the post-dequeue wait")
            return event

        monkeypatch.setattr(client, "_wait_native_callback_event_for_consumer", pause_after_dequeue)

        def poll() -> None:
            try:
                bridge.next_envelope(timeout=0.6)
            except BaseException as exc:  # retain the background failure for assertions
                poll_errors.append(exc)
            finally:
                poll_finished.set()

        poll_thread = threading.Thread(target=poll, daemon=True)
        poll_thread.start()
        assert dequeue_finished.wait(timeout=1)
        assert client._native_callback_events.empty()

        bridge.close()
        resume_poll.set()
        assert poll_finished.wait(timeout=1)
        poll_thread.join(timeout=1)
        assert not poll_thread.is_alive()
        assert len(poll_errors) == 1
        assert isinstance(poll_errors[0], ContractValidationError)
        assert "callback may have been consumed and discarded" in str(poll_errors[0])
        assert client._native_callback_consumer_lease is None
    finally:
        resume_poll.set()
        if bridge is not None:
            bridge.close()
        if poll_thread is not None:
            poll_thread.join(timeout=1)
        bound.store.close()
        client.stop()


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
        assert result.callback_key.native_action_ref == 1
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


@pytest.mark.unit
def test_callback_ledger_adapter_applies_same_send_event_and_keeps_unknown_fenced(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    adapter = None
    try:
        reservation = bound.store.read_ctp_order_identity(bound.scope, "source-bridge-intent")
        command = bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id)
        assert reservation is not None and command is not None
        assert command.status == "CLAIMED"
        verifier = _FakeCallbackLedgerVerifier()
        adapter = CtpNativeCallbackLedgerAdapter.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            writer_lease=bound.lease,
            native_trader_client=bound.client,
            callback_verifier=verifier,
        )

        sent: list[str] = []

        def fake_native_send(claimed_command):
            assert claimed_command.status == "CLAIMED"
            assert claimed_command.request_payload["OrderRef"] == reservation.order_ref
            assert claimed_command.correlation_key.order_ref == reservation.order_ref
            sent.append(claimed_command.command_id)
            # This fake models the SDK source queue being written by its native
            # callback handler after the send call. No caller callback fields
            # are passed to the ledger adapter.
            bound.client._events.put(_source_event(bound.client))

        fake_native_send(command)
        bound.store.complete_ctp_dispatch_command(
            bound.scope,
            _unknown_receipt(command),
            writer_lease=bound.lease,
        )

        applied = adapter.apply_next(timeout=0)

        assert applied is not None and applied.duplicate is False
        assert sent == [bound.command_id]
        assert len(verifier.calls) == 1
        assert verifier.calls[0][0].correlation_key == command.correlation_key
        assert bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id).status == (
            "UNKNOWN"
        )
        projection = bound.store.read_ctp_dispatch_projection(bound.scope, bound.command_id)
        assert projection is not None
        assert projection.command_status == "UNKNOWN"
        assert projection.submit_action.order_state.provider_state == "ACKNOWLEDGED"
        assert applied.account_fence_open is True
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger WHERE command_id = ?",
                (bound.command_id,),
            ).fetchone()[0]
            == 1
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )
        assert (
            bound.store._connection.execute(
                """
            SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences
            WHERE account_key = ?
            """,
                (bound.scope.account_key,),
            ).fetchone()[0]
            == 1
        )
    finally:
        if adapter is not None:
            adapter.close()
        bound.store.close()


@pytest.mark.unit
def test_source_lifecycle_fence_blocks_new_claim_after_queued_receipt_and_restart(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    adapter = None
    store_path = tmp_path / "execution.sqlite3"
    reopened = None
    try:
        command = bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id)
        assert command is not None
        adapter = CtpNativeCallbackLedgerAdapter.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            writer_lease=bound.lease,
            native_trader_client=bound.client,
            callback_verifier=_FakeCallbackLedgerVerifier(
                error=RuntimeError("fake verifier rejection")
            ),
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )

        # Fake native send is complete: its local QUEUED receipt is immutable,
        # but callback ingestion is still pending.
        bound.client._events.put(_source_event(bound.client))
        completed = bound.store.complete_ctp_dispatch_command(
            bound.scope,
            _unknown_receipt(command, outcome="QUEUED"),
            writer_lease=bound.lease,
        )
        assert completed.status == "COMPLETED"
        with pytest.raises(ContractValidationError, match="may have been consumed"):
            adapter.apply_next(timeout=0)
        adapter.close()
        adapter = None
        bound.store.close()
        # A process restart cannot lose the uncertainty fence or rewrite the
        # already durable QUEUED receipt.
        reopened = SqliteExecutionStore(store_path)
        assert reopened.read_ctp_dispatch_command(bound.scope, bound.command_id).status == (
            "COMPLETED"
        )
        with pytest.raises(ContractValidationError, match="permanent callback lifecycle fence"):
            _stage_followup_ready_command(bound, reopened)
        assert (
            reopened._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences WHERE account_key = ?",
                (bound.scope.account_key,),
            ).fetchone()[0]
            == 1
        )
    finally:
        if adapter is not None:
            adapter.close()
        if reopened is not None:
            reopened.close()
        bound.store.close()


@pytest.mark.unit
def test_callback_poll_timeout_keeps_lifecycle_fence_for_same_bridge_retry(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    adapter = None
    try:
        adapter = CtpNativeCallbackLedgerAdapter.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            writer_lease=bound.lease,
            native_trader_client=bound.client,
            callback_verifier=_FakeCallbackLedgerVerifier(),
        )
        assert adapter.apply_next(timeout=0) is None
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )

        bound.client._events.put(_source_event(bound.client))
        assert adapter.apply_next(timeout=0) is not None
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )
    finally:
        if adapter is not None:
            adapter.close()
        bound.store.close()


@pytest.mark.unit
def test_source_lifecycle_fence_covers_event_queued_between_polls_and_restart(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    adapter = None
    reopened = None
    try:
        command = bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id)
        assert command is not None
        adapter = CtpNativeCallbackLedgerAdapter.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            writer_lease=bound.lease,
            native_trader_client=bound.client,
            callback_verifier=_FakeCallbackLedgerVerifier(),
        )
        bound.store.complete_ctp_dispatch_command(
            bound.scope,
            _unknown_receipt(command, outcome="QUEUED"),
            writer_lease=bound.lease,
        )
        bound.client._events.put(_source_event(bound.client, sequence=1))
        assert adapter.apply_next(timeout=0) is not None
        assert bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id).status == (
            "COMPLETED"
        )

        # A later callback arrives after event 1's commit but before another
        # poll. A crash at this point must still leave the account fenced.
        bound.client._events.put(_source_event(bound.client, sequence=2))
        assert bound.client._events.qsize() == 1
        adapter.close()
        adapter = None
        bound.store.close()

        reopened = SqliteExecutionStore(tmp_path / "execution.sqlite3")
        with pytest.raises(ContractValidationError, match="permanent callback lifecycle fence"):
            _stage_followup_ready_command(bound, reopened)
        assert (
            reopened._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences WHERE account_key = ?",
                (bound.scope.account_key,),
            ).fetchone()[0]
            == 1
        )
        assert bound.client._events.qsize() == 1
    finally:
        if adapter is not None:
            adapter.close()
        if reopened is not None:
            reopened.close()
        bound.store.close()


@pytest.mark.unit
def test_v11_resolved_guard_migrates_to_permanent_fence_and_stays_after_reopen(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    migrated = None
    reopened = None
    try:
        command = bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id)
        assert command is not None
        bound.store.complete_ctp_dispatch_command(
            bound.scope,
            _unknown_receipt(command, outcome="QUEUED"),
            writer_lease=bound.lease,
        )
        command, legacy_guard_id = _write_legacy_v11_resolved_guard(bound)
        bound.store.close()

        migrated = SqliteExecutionStore(tmp_path / "execution.sqlite3")
        version = migrated._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()["value"]
        assert version == "20"
        assert migrated.read_ctp_dispatch_command(bound.scope, bound.command_id).status == (
            "COMPLETED"
        )
        fence = migrated._connection.execute(
            """
            SELECT * FROM ctp_dispatch_callback_source_lifecycle_fences
            WHERE account_key = ?
            """,
            (bound.scope.account_key,),
        ).fetchone()
        assert fence is not None
        assert fence["command_id"] == bound.command_id
        assert fence["source_lifecycle_fence_id"] == legacy_guard_id
        assert fence["correlation_key_sha256"] == payload_sha256(
            command.correlation_key.to_payload()
        )

        with pytest.raises(InvalidStateTransition, match="unmapped legacy execution history"):
            _stage_followup_ready_command(bound, migrated)
        migrated.close()
        migrated = None

        # The combined v19 upgrade is idempotent and the old resolved bit never removes
        # the newly migrated account source-lifecycle fence.
        reopened = SqliteExecutionStore(tmp_path / "execution.sqlite3")
        assert (
            reopened._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences WHERE account_key = ?",
                (bound.scope.account_key,),
            ).fetchone()[0]
            == 1
        )
        with pytest.raises(InvalidStateTransition, match="unmapped legacy execution history"):
            _stage_followup_ready_command(bound, reopened)
    finally:
        if migrated is not None:
            migrated.close()
        if reopened is not None:
            reopened.close()
        bound.store.close()


@pytest.mark.unit
def test_v11_unmappable_resolved_guard_refuses_store_upgrade(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    try:
        _write_legacy_v11_resolved_guard(bound, correlation_digest="0" * 64)
        bound.store.close()
        with pytest.raises(
            DurableStoreError, match="legacy CTP callback lifecycle fence binding is inconsistent"
        ):
            SqliteExecutionStore(tmp_path / "execution.sqlite3")
    finally:
        bound.store.close()


@pytest.mark.unit
def test_v12_permanent_fence_and_projectionless_cancel_migrate_to_v19(tmp_path):
    bound = _stage_dispatched_command(tmp_path, operation="CANCEL")
    migrated = None
    reopened = None
    try:
        fence_id = bound.store.create_ctp_callback_source_lifecycle_fence(
            bound.scope, bound.command_id, writer_lease=bound.lease
        )
        bound.store.close()

        # A real v12 callback-fence candidate predates target projections. Keep
        # its dispatched CANCEL command and permanent callback fence while
        # removing only the later v13 projection structures.
        connection = sqlite3.connect(tmp_path / "execution.sqlite3")
        try:
            connection.execute("DROP TABLE ctp_order_target_projection_consumptions")
            connection.execute("DROP TABLE ctp_order_target_projections")
            connection.execute(
                "UPDATE execution_meta SET value = '12' WHERE key = 'schema_version'"
            )
            connection.commit()
        finally:
            connection.close()

        migrated = SqliteExecutionStore(tmp_path / "execution.sqlite3")
        assert (
            migrated._connection.execute(
                "SELECT value FROM execution_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            == "20"
        )
        command = migrated.read_ctp_dispatch_command(bound.scope, bound.command_id)
        assert command is not None
        assert command.status == "UNKNOWN"
        assert command.unknown_reason == "schema_upgrade_requires_ctp_v2_dispatch_cutover"
        assert (
            migrated._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_target_projections"
            ).fetchone()[0]
            == 0
        )
        persisted_fence = migrated._connection.execute(
            "SELECT source_lifecycle_fence_id FROM ctp_dispatch_callback_source_lifecycle_fences "
            "WHERE account_key = ?",
            (bound.scope.account_key,),
        ).fetchone()
        assert persisted_fence is not None
        assert persisted_fence[0] == fence_id

        with pytest.raises(InvalidStateTransition, match="unmapped legacy execution history"):
            _stage_followup_ready_command(bound, migrated)
        migrated.close()
        migrated = None

        reopened = SqliteExecutionStore(tmp_path / "execution.sqlite3")
        assert reopened.read_ctp_dispatch_command(bound.scope, bound.command_id).status == (
            "UNKNOWN"
        )
        assert (
            reopened._connection.execute(
                "SELECT source_lifecycle_fence_id FROM ctp_dispatch_callback_source_lifecycle_fences "
                "WHERE account_key = ?",
                (bound.scope.account_key,),
            ).fetchone()[0]
            == fence_id
        )
        assert (
            reopened._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_target_projections"
            ).fetchone()[0]
            == 0
        )
    finally:
        if migrated is not None:
            migrated.close()
        if reopened is not None:
            reopened.close()
        bound.store.close()


@pytest.mark.unit
def test_callback_projection_failure_rolls_back_ledger_but_keeps_lifecycle_fence(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    adapter = None
    try:
        command = bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id)
        assert command is not None
        bound.client._events.put(_source_event(bound.client))
        adapter = CtpNativeCallbackLedgerAdapter.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            writer_lease=bound.lease,
            native_trader_client=bound.client,
            callback_verifier=_FakeCallbackLedgerVerifier(),
        )
        bound.store.complete_ctp_dispatch_command(
            bound.scope,
            _unknown_receipt(command, outcome="QUEUED"),
            writer_lease=bound.lease,
        )
        bound.store._connection.execute(
            """
            CREATE TRIGGER reject_callback_projection
            BEFORE INSERT ON ctp_dispatch_order_projection
            BEGIN
                SELECT RAISE(ABORT, 'test projection storage failure');
            END
            """
        )

        with pytest.raises(ContractValidationError, match="may have been consumed"):
            adapter.apply_next(timeout=0)
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 0
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_order_projection"
            ).fetchone()[0]
            == 0
        )
    finally:
        if adapter is not None:
            adapter.close()
        bound.store.close()


@pytest.mark.unit
def test_callback_commit_lease_loss_leaves_durable_ingestion_fence(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    adapter = None
    reopened = None
    try:
        command = bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id)
        assert command is not None
        bound.client._events.put(_source_event(bound.client))
        adapter = CtpNativeCallbackLedgerAdapter.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            writer_lease=bound.lease,
            native_trader_client=bound.client,
            callback_verifier=_FakeCallbackLedgerVerifier(),
        )
        bound.store.complete_ctp_dispatch_command(
            bound.scope,
            _unknown_receipt(command, outcome="QUEUED"),
            writer_lease=bound.lease,
        )
        bound.store._connection.execute(
            "UPDATE execution_writer_leases SET expires_at_ns = ? WHERE scope_key = ?",
            (time.time_ns() - 1, bound.scope.account_key),
        )

        with pytest.raises(ContractValidationError, match="may have been consumed"):
            adapter.apply_next(timeout=0)
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 0
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )
        adapter.close()
        adapter = None
        bound.store.close()

        reopened = SqliteExecutionStore(tmp_path / "execution.sqlite3")
        with pytest.raises(InvalidStateTransition, match="already has a persistent owner"):
            reopened.acquire_ctp_account_family_owner(bound.scope)
    finally:
        if adapter is not None:
            adapter.close()
        if reopened is not None:
            reopened.close()
        bound.store.close()


@pytest.mark.unit
def test_adapter_cleanup_failure_does_not_leak_callback_source_exception(tmp_path, monkeypatch):
    bound = _stage_dispatched_command(tmp_path)
    adapter = None
    try:
        bound.client._events.put(_source_event(bound.client))
        adapter = CtpNativeCallbackLedgerAdapter.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            writer_lease=bound.lease,
            native_trader_client=bound.client,
            callback_verifier=_FakeCallbackLedgerVerifier(
                error=RuntimeError("fake callback verifier failure")
            ),
        )
        consumer_token = bound.client._callback_consumer_token

        def fail_consumer_release(token):
            assert token is consumer_token
            raise RuntimeError("SENTINEL_CALLBACK_QUEUE_RELEASE_FAILURE")

        monkeypatch.setattr(
            bound.client, "_release_native_callback_event_consumer", fail_consumer_release
        )
        with pytest.raises(
            ContractValidationError,
            match="CTP callback may have been consumed without a durable verified ledger commit",
        ) as failure:
            adapter.apply_next(timeout=0)

        rendered = "".join(traceback.format_exception(failure.value))
        assert "SENTINEL_CALLBACK_QUEUE_RELEASE_FAILURE" not in rendered
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 0
        )
    finally:
        bound.client._revoke_native_callback_event_consumer()
        if adapter is not None:
            adapter.close()
        bound.store.close()


@pytest.mark.unit
@pytest.mark.parametrize("mismatch", ["target", "source"])
def test_callback_ledger_adapter_poison_closes_on_target_or_source_mismatch(
    tmp_path, mismatch: str
):
    bound = _stage_dispatched_command(tmp_path)
    adapter = None
    try:
        event = _source_event(
            bound.client,
            overrides=({"OrderRef": "000000000099"} if mismatch == "target" else None),
        )
        if mismatch == "source":
            event.callback_session_matches_login = False
        bound.client._events.put(event)
        adapter = CtpNativeCallbackLedgerAdapter.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            writer_lease=bound.lease,
            native_trader_client=bound.client,
            callback_verifier=_FakeCallbackLedgerVerifier(),
        )

        with pytest.raises(ContractValidationError, match="may have been consumed"):
            adapter.apply_next(timeout=0)
        assert bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id).status == (
            "CLAIMED"
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 0
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )
        with pytest.raises(ContractValidationError, match="adapter is closed"):
            adapter.apply_next(timeout=0)
    finally:
        if adapter is not None:
            adapter.close()
        bound.store.close()


@pytest.mark.unit
def test_callback_ledger_adapter_rejects_wrong_durable_scope_before_queue_claim(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    try:
        wrong_scope = ExecutionScope(
            "ctp",
            "simulation",
            _ctp_account_ref(b"different-account"),
            "source-bridge",
            "20260925",
        )
        with pytest.raises(ContractValidationError, match="command is missing"):
            CtpNativeCallbackLedgerAdapter.bind_after_login(
                store=bound.store,
                scope=wrong_scope,
                command_id=bound.command_id,
                writer_lease=bound.lease,
                native_trader_client=bound.client,
                callback_verifier=_FakeCallbackLedgerVerifier(),
            )
        assert bound.client._callback_consumer_token is None
        assert bound.client._events.empty()
    finally:
        bound.store.close()


@pytest.mark.unit
def test_callback_ledger_adapter_replay_poison_does_not_append_second_row(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    adapter = None
    try:
        event = _source_event(bound.client)
        bound.client._events.put(event)
        adapter = CtpNativeCallbackLedgerAdapter.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            writer_lease=bound.lease,
            native_trader_client=bound.client,
            callback_verifier=_FakeCallbackLedgerVerifier(),
        )
        assert adapter.apply_next(timeout=0) is not None

        bound.client._events.put(event)
        with pytest.raises(ContractValidationError, match="may have been consumed"):
            adapter.apply_next(timeout=0)
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 1
        )
        assert bound.store.read_ctp_dispatch_projection(bound.scope, bound.command_id) is not None
    finally:
        if adapter is not None:
            adapter.close()
        bound.store.close()


@pytest.mark.unit
def test_callback_ledger_adapter_rechecks_reentrant_lifecycle_change(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    adapter = None
    try:
        bound.client._events.put(_source_event(bound.client))

        def mutate_lifecycle_during_verification():
            # The source lock is reentrant on this thread. The adapter must
            # detect this after the injected verifier returns, before commit.
            with bound.client._query_state_lock:
                bound.client._connection_generation += 1

        adapter = CtpNativeCallbackLedgerAdapter.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            writer_lease=bound.lease,
            native_trader_client=bound.client,
            callback_verifier=_FakeCallbackLedgerVerifier(
                during_verify=mutate_lifecycle_during_verification
            ),
        )

        with pytest.raises(ContractValidationError, match="may have been consumed"):
            adapter.apply_next(timeout=0)
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 0
        )
        assert bound.store.read_ctp_dispatch_command(bound.scope, bound.command_id).status == (
            "CLAIMED"
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )
    finally:
        if adapter is not None:
            adapter.close()
        bound.store.close()


@pytest.mark.unit
def test_callback_ledger_commit_precedes_cross_thread_source_disconnect(tmp_path):
    bound = _stage_dispatched_command(tmp_path)
    adapter = None
    disconnect_thread = None
    try:
        bound.client._events.put(_source_event(bound.client))
        disconnect_attempted = threading.Event()
        disconnected = threading.Event()

        def disconnect():
            disconnect_attempted.set()
            with bound.client._query_state_lock:
                bound.client._connected = False
                bound.client._login_state = "disconnected"
                disconnected.set()

        def start_disconnect_during_verification():
            nonlocal disconnect_thread
            disconnect_thread = threading.Thread(target=disconnect)
            disconnect_thread.start()
            assert disconnect_attempted.wait(1.0)
            assert not disconnected.is_set()

        adapter = CtpNativeCallbackLedgerAdapter.bind_after_login(
            store=bound.store,
            scope=bound.scope,
            command_id=bound.command_id,
            writer_lease=bound.lease,
            native_trader_client=bound.client,
            callback_verifier=_FakeCallbackLedgerVerifier(
                during_verify=start_disconnect_during_verification
            ),
        )

        applied = adapter.apply_next(timeout=0)
        assert applied is not None
        assert disconnect_thread is not None
        disconnect_thread.join(timeout=1.0)
        assert not disconnect_thread.is_alive()
        assert disconnected.is_set()
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_source_lifecycle_fences"
            ).fetchone()[0]
            == 1
        )
        assert (
            bound.store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 1
        )
    finally:
        if adapter is not None:
            adapter.close()
        if disconnect_thread is not None and disconnect_thread.is_alive():
            disconnect_thread.join(timeout=1.0)
        bound.store.close()
