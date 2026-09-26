from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace

import pytest

from bt_api_execution import (
    ContractValidationError,
    CtpCallbackSessionStoreAdapter,
    CtpCancelTarget,
    CtpDispatchAuthority,
    CtpDispatchReceipt,
    CtpOrderRefLegacyMapping,
    CtpOrderRefSeedProof,
    CtpVerifiedCallbackEvidence,
    CtpVerifiedOrderTargetProjection,
    CtpVerifiedTradeFactEvidence,
    ExecutionScope,
    InvalidStateTransition,
    SqliteExecutionStore,
    payload_sha256,
)


@dataclass(frozen=True)
class _SourceTags:
    source_instance_id: str = "source-instance-router"
    native_client_epoch: str = "a" * 32
    native_api_source_id: str = "api-source-router"
    native_spi_source_id: str = "spi-source-router"
    native_api_generation: int = 1
    connection_generation: int = 0


@dataclass(frozen=True)
class _LoginObservation:
    broker_id: str = "broker"
    user_id: str = "user"
    trading_day: str = "20260925"
    connection_generation: int = 1
    request_id: int = 17


class CtpTraderCallbackIngressRecordV2:
    """Test double for the exact SDK wire type; no SDK/native import occurs."""

    def __init__(self, payload):
        self._payload = payload

    def to_payload(self):
        return dict(self._payload)


def _record(
    owner,
    name,
    sequence,
    *,
    phase,
    callback_class,
    fields=(),
    generation=1,
    request_id=None,
    is_last=None,
):
    if name == "OnFrontConnected":
        named_args = []
    elif name in {"OnRspAuthenticate", "OnRspUserLogin"}:
        named_args = [
            {"argument_slot": 0, "name": "pRspField", "present": True,
             "scalar_captured": False, "value": None},
            {"argument_slot": 1, "name": "pRspInfo", "present": True,
             "scalar_captured": False, "value": None},
            {"argument_slot": 2, "name": "nRequestID", "present": True,
             "scalar_captured": True, "value": sequence + 14},
            {"argument_slot": 3, "name": "bIsLast", "present": True,
             "scalar_captured": True, "value": True},
        ]
    elif name == "OnRspOrderAction":
        named_args = [
            {"argument_slot": 0, "name": "pInputOrderAction", "present": True,
             "scalar_captured": False, "value": None},
            {"argument_slot": 1, "name": "pRspInfo", "present": True,
             "scalar_captured": False, "value": None},
            {"argument_slot": 2, "name": "nRequestID", "present": True,
             "scalar_captured": True, "value": request_id},
            {"argument_slot": 3, "name": "bIsLast", "present": True,
             "scalar_captured": True, "value": is_last},
        ]
    else:
        argument_name = {
            "OnRtnOrder": "pOrder",
            "OnRtnTrade": "pTrade",
            "OnRtnInstrumentStatus": "pInstrumentStatus",
        }.get(name, "pField")
        named_args = [
            {"argument_slot": 0, "name": argument_name, "present": True,
             "scalar_captured": False, "value": None}
        ]
    flattened = [
        {
            "argument_slot": slot,
            "field_name": field_name,
            "present": True,
            "scalar_captured": True,
            "value": value,
        }
        for slot, field_name, value in fields
    ]
    unsigned = {
        "schema": "ctp_trader_callback_ingress.v2",
        "owner_intent_id": owner.owner_intent_id,
        "callback_name": name,
        "callback_class": callback_class,
        "phase": phase,
        "source_instance_id": _SourceTags.source_instance_id,
        "native_client_epoch": _SourceTags.native_client_epoch,
        "native_api_source_id": _SourceTags.native_api_source_id,
        "native_spi_source_id": _SourceTags.native_spi_source_id,
        "native_api_generation": 1,
        "source_connection_generation": 0,
        "connection_generation": generation,
        "sequence": sequence,
        "monotonic_ns": sequence + 100,
        "named_args": named_args,
        "flattened_fields": flattened,
        "capture_complete": True,
        "missing_getters": [],
    }
    encoded = json.dumps(
        unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    unsigned["digest"] = hashlib.sha256(encoded).hexdigest()
    return CtpTraderCallbackIngressRecordV2(unsigned)


def _scope():
    return ExecutionScope("CTP", "simulation", "acct-router", "strategy.router", "20260925")


def _lease(store, scope):
    return store.acquire_or_renew_lease(scope, "router-test-owner", ttl_ns=60_000_000_000)


def _runtime_id(value):
    return "bt-managed-v1:" + hashlib.sha256(value.encode("ascii")).hexdigest()


def _active_owner(store, scope, lease):
    owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)
    CtpCallbackSessionStoreAdapter(
        store,
        scope,
        owner,
        lease,
        lambda **kwargs: kwargs,
        CtpTraderCallbackIngressRecordV2,
    )
    store.append_ctp_callback_ingress(
        owner,
        _record(owner, "OnFrontConnected", 1, phase="PRE_LOGIN", callback_class="PRE_LOGIN", generation=0),
    )
    store.append_ctp_callback_ingress(
        owner,
        _record(
            owner,
            "OnRspAuthenticate",
            2,
            phase="PRE_LOGIN",
            callback_class="PRE_LOGIN",
            fields=((0, "BrokerID", "broker"), (0, "UserID", "user"), (1, "ErrorID", 0)),
        ),
    )
    login = _record(
        owner,
        "OnRspUserLogin",
        3,
        phase="PRE_LOGIN",
        callback_class="PRE_LOGIN",
        fields=(
            (0, "BrokerID", "broker"),
            (0, "UserID", "user"),
            (0, "TradingDay", scope.trading_day),
            (0, "FrontID", 31),
            (0, "SessionID", 41),
            (1, "ErrorID", 0),
        ),
    )
    store.append_ctp_callback_ingress(owner, login)
    store.bind_ctp_callback_session(
        scope, owner, _LoginObservation(), _SourceTags(), 3, writer_lease=lease
    )
    return owner


def _proof(scope, session_generation_id):
    legacy_scope = ExecutionScope(
        "CTP", "simulation", scope.account_ref, "strategy.legacy", "20260924"
    )
    source_digests = (
        ("backtrader_prototype", hashlib.sha256(b"prototype fixture").hexdigest()),
        ("sdk_jsonl", hashlib.sha256(b"sdk fixture").hexdigest()),
    )
    mappings = (
        CtpOrderRefLegacyMapping(
            source_name="backtrader_prototype",
            account_key=scope.account_key,
            trading_day="20260924",
            scope_key=legacy_scope.key,
            managed_intent_id="legacy-intent",
            runtime_order_id="legacy-runtime-order",
            order_ref="000000000012",
        ),
    )
    return CtpOrderRefSeedProof(
        trading_day=scope.trading_day,
        native_max_order_ref="000000000010",
        legacy_ledger_max_order_ref="000000000012",
        legacy_ledger_sha256=payload_sha256(dict(source_digests)),
        account_key=scope.account_key,
        scope_key=scope.key,
        session_generation_id=session_generation_id,
        native_front_id=31,
        native_session_id=41,
        existing_native_order_refs=("000000000009", "000000000010"),
        legacy_source_sha256=source_digests,
        legacy_mappings=mappings,
    )


class _Authority:
    def verify_action(self, command, *, now_ns):
        return CtpDispatchAuthority(
            authority_type="ctp_dispatch_authority.v1",
            command_binding_sha256=command.authority_binding_sha256,
            approval_use_id=command.approval_use_id,
            approval_digest=command.approval_digest,
            source_digest_sha256=hashlib.sha256(b"fake authority source").hexdigest(),
            verifier_id="fake-router-authority",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + 5_000_000_000,
        )


class _CallbackVerifier:
    def verify_callback(self, command, callback, callback_payload, *, now_ns):
        return CtpVerifiedCallbackEvidence(
            evidence_type="ctp_verified_callback.v1",
            callback_key=callback,
            callback_payload_sha256=payload_sha256(callback_payload),
            projection_state="PARTIALLY_FILLED",
            source_digest_sha256=hashlib.sha256(b"fake callback verifier").hexdigest(),
            verifier_id="fake-router-callback",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + 5_000_000_000,
        )


class _TradeVerifier:
    def verify_trade_fact(self, command, trade_fact, source_event, *, now_ns):
        return CtpVerifiedTradeFactEvidence(
            evidence_type="ctp_verified_trade_fact.v2",
            owner_intent_id=source_event.owner_handle.owner_intent_id,
            source_sequence=source_event.source_sequence,
            ingress_record_digest_sha256=source_event.record_digest_sha256,
            event_id=trade_fact.event_id,
            trade_fact_sha256=payload_sha256(trade_fact.to_payload()),
            source_digest_sha256=hashlib.sha256(b"fake trade verifier").hexdigest(),
            verifier_id="fake-router-trade",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + 5_000_000_000,
        )


class _OrderTargetVerifier:
    def __init__(self):
        self._request_id = 100

    def verify_order_target(self, scope, reservation, native_query_evidence, *, now_ns):
        self._request_id += 1
        return CtpVerifiedOrderTargetProjection(
            account_key=reservation.account_key,
            scope_key=reservation.scope_key,
            trading_day=reservation.trading_day,
            managed_intent_id=reservation.managed_intent_id,
            runtime_order_id=reservation.runtime_order_id,
            order_ref=reservation.order_ref,
            account_fingerprint_sha256=hashlib.sha256(b"fake account").hexdigest(),
            registration_digest=hashlib.sha256(b"fake registration").hexdigest(),
            instrument_id="rb2710",
            exchange_id="SHFE",
            session_generation_id=native_query_evidence["session_generation_id"],
            connection_generation=1,
            query_front_id=31,
            query_session_id=41,
            query_request_id=self._request_id,
            query_filters_sha256=hashlib.sha256(b"fake filters").hexdigest(),
            query_records_sha256=hashlib.sha256(b"fake records").hexdigest(),
            source_evidence_sha256=hashlib.sha256(b"fake source").hexdigest(),
            query_record_count=1,
            query_match_count=1,
            query_complete=True,
            query_terminal=True,
            query_timed_out=False,
            query_error_id=None,
            late_callback_count=0,
            order_sys_id="sys-" + reservation.order_ref,
            front_id=31,
            session_id=41,
            provider_state="OPEN",
            quantity=1,
            traded_quantity=0,
            remaining_quantity=1,
            verifier_id="fake-session-router-order-target",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + 4_000_000_000,
        )


def _queue_and_complete(
    store,
    scope,
    lease,
    owner,
    reservation,
    command_id,
    payload,
    request_id,
    *,
    complete=True,
    record_queue_receipt=True,
    claim_command=True,
):
    binding = store._active_ctp_callback_sessions[owner.owner_intent_id]
    receipt_id = hashlib.sha256(command_id.encode("ascii")).hexdigest()[:32]
    staged = store.stage_ctp_dispatch_command(
        scope,
        command_id,
        "SUBMIT",
        payload,
        approval_use_id="approval-" + command_id,
        approval_digest=hashlib.sha256(("approval-" + command_id).encode("ascii")).hexdigest(),
        session_binding={
            "binding_type": "ctp_callback_session_binding.v1",
            "owner_intent_id": binding.owner_intent_id,
            "account_key": binding.account_key,
            "scope_key": binding.scope_key,
            "trading_day": binding.trading_day,
            "session_generation_id": binding.session_generation_id,
            "dispatch_front_id": binding.dispatch_front_id,
            "dispatch_session_id": binding.dispatch_session_id,
            "source_tags": {
                "source_instance_id": binding.source_instance_id,
                "native_client_epoch": binding.native_client_epoch,
                "native_api_source_id": binding.native_api_source_id,
                "native_spi_source_id": binding.native_spi_source_id,
                "native_api_generation": binding.native_api_generation,
                "connection_generation": binding.source_connection_generation,
            },
            "source_high_watermark": binding.source_high_watermark,
        },
        writer_lease=lease,
        managed_intent_id=reservation.managed_intent_id,
        order_ref=reservation.order_ref,
        managed_action_id=reservation.managed_intent_id,
        session_generation_id=binding.session_generation_id,
        dispatch_front_id=binding.dispatch_front_id,
        dispatch_session_id=binding.dispatch_session_id,
        native_request_id=request_id,
        local_queue_receipt_id=receipt_id,
    )
    if record_queue_receipt:
        store.record_ctp_dispatch_queue_receipt(
            scope,
            command_id,
            {
                "kind": "command_receipt",
                "command": "submit",
                "receipt_id": receipt_id,
                "queued": True,
            },
            writer_lease=lease,
        )
    if not claim_command:
        return staged, None
    claim = store.claim_ctp_dispatch_command_for_session(
        scope,
        command_id,
        owner_handle=owner,
        writer_lease=lease,
        authority_verifier=_Authority(),
        required_local_queue_receipt_id=receipt_id,
    )
    assert claim is not None
    if not complete:
        return staged, claim
    command = claim.command
    receipt = CtpDispatchReceipt(
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
        outcome="QUEUED",
        native_receipt_payload={"request_id": request_id, "queue_code": 0},
        correlation_key=command.correlation_key,
        local_queue_receipt_id=receipt_id,
    )
    store.complete_ctp_dispatch_command_for_session(
        scope, receipt, owner_handle=owner, writer_lease=lease
    )
    return staged


@pytest.mark.unit
def test_session_native_call_binding_is_store_exact_and_consumed_once(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-router-binding-once.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        session = store._active_ctp_callback_sessions[owner.owner_intent_id]
        reservation = _reserve(store, scope, lease, session.session_generation_id, "bound-intent")
        request = {
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
        staged, claim = _queue_and_complete(
            store,
            scope,
            lease,
            owner,
            reservation,
            "command-bound-intent",
            request,
            81,
            complete=False,
        )
        adapter = _adapter_for(store, owner)
        assert adapter.verify_native_call(owner, claim.binding) is claim.binding
        forged = replace(claim.binding)
        with pytest.raises(ContractValidationError, match="not fresh"):
            adapter.verify_native_call(owner, forged)
        with pytest.raises(ContractValidationError, match="not fresh"):
            adapter.verify_native_call(owner, claim.binding)
        current = store.read_ctp_dispatch_command(scope, staged.command_id)
        assert current is not None and current.status == "CLAIMED"
        owner_row = store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert tuple(owner_row) == ("POISONED", "owner_binding_mismatch")
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutation",
    ("owner", "source", "highwater"),
)
def test_session_command_binding_cannot_be_caller_relabelled(tmp_path, mutation):
    scope = _scope()
    store = SqliteExecutionStore(
        tmp_path / f"session-router-binding-mismatch-{mutation}.sqlite3"
    )
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        binding = store._active_ctp_callback_sessions[owner.owner_intent_id]
        reservation = _reserve(
            store,
            scope,
            lease,
            binding.session_generation_id,
            "mismatch-intent-" + mutation,
        )
        session_payload = store._ctp_callback_session_binding_payload(binding)
        if mutation == "owner":
            session_payload["owner_intent_id"] = "other-owner"
        elif mutation == "source":
            session_payload["source_tags"]["native_api_source_id"] = "other-api-source"
        else:
            session_payload["source_high_watermark"] += 1
        request = {
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
        with pytest.raises(ContractValidationError, match="differs from active Store session"):
            store.stage_ctp_dispatch_command(
                scope,
                "mismatch-command-" + mutation,
                "SUBMIT",
                request,
                approval_use_id="mismatch-approval-" + mutation,
                approval_digest=hashlib.sha256(mutation.encode()).hexdigest(),
                session_binding=session_payload,
                writer_lease=lease,
                managed_intent_id=reservation.managed_intent_id,
                order_ref=reservation.order_ref,
                managed_action_id=reservation.managed_intent_id,
                session_generation_id=binding.session_generation_id,
                dispatch_front_id=binding.dispatch_front_id,
                dispatch_session_id=binding.dispatch_session_id,
                native_request_id=91,
            )
        assert store.read_ctp_dispatch_command(scope, "mismatch-command-" + mutation) is None
        owner_row = store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert tuple(owner_row) == ("ACTIVE", None)
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("record_queue_receipt", "failure_code"),
    ((False, "queue_receipt_failure"), (True, "queue_publish_failure")),
)
def test_session_pre_native_failure_atomically_unknowns_and_poisons_across_restart(
    tmp_path, record_queue_receipt, failure_code
):
    scope = _scope()
    path = tmp_path / f"session-pre-native-{failure_code}.sqlite3"
    store = SqliteExecutionStore(path)
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        session = store._active_ctp_callback_sessions[owner.owner_intent_id]
        reservation = _reserve(
            store, scope, lease, session.session_generation_id, "pre-native-" + failure_code
        )
        request = {
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
        staged, claim = _queue_and_complete(
            store,
            scope,
            lease,
            owner,
            reservation,
            "pre-native-command-" + failure_code,
            request,
            111,
            record_queue_receipt=record_queue_receipt,
            claim_command=False,
        )
        assert claim is None
        failed = store.fail_ctp_dispatch_command_before_native(
            scope,
            staged,
            owner_handle=owner,
            writer_lease=lease,
            failure_code=failure_code,
        )
        assert failed.status == "UNKNOWN"
        assert failed.unknown_reason == "session_pre_native_failure:" + failure_code
        owner_row = store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert tuple(owner_row) == ("POISONED", "dispatch_stage_ambiguous")
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            store.fail_ctp_dispatch_command_before_native(
                scope,
                staged,
                owner_handle=owner,
                writer_lease=lease,
                failure_code=failure_code,
            )
    finally:
        store.close()

    reopened = SqliteExecutionStore(path)
    try:
        command = reopened.read_ctp_dispatch_command(
            scope, "pre-native-command-" + failure_code
        )
        assert command is not None and command.status == "UNKNOWN"
        assert command.unknown_reason == "session_pre_native_failure:" + failure_code
        owner_row = reopened._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners"
        ).fetchone()
        assert tuple(owner_row) == ("POISONED", "dispatch_stage_ambiguous")
        with pytest.raises(InvalidStateTransition, match="already has a durable"):
            reopened.create_ctp_callback_session_owner(scope, writer_lease=_lease(reopened, scope))
        assert (
            reopened.claim_ctp_dispatch_command(
                scope,
                "pre-native-command-" + failure_code,
                writer_lease=_lease(reopened, scope),
                authority_verifier=_Authority(),
            )
            is None
        )
    finally:
        reopened.close()


@pytest.mark.unit
def test_session_pre_native_failure_rejects_forged_readback_and_claimed_command(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-pre-native-forgery.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        session = store._active_ctp_callback_sessions[owner.owner_intent_id]
        reservation = _reserve(store, scope, lease, session.session_generation_id, "claim-before")
        request = {
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
        staged, claim = _queue_and_complete(
            store,
            scope,
            lease,
            owner,
            reservation,
            "claim-before-command",
            request,
            112,
            complete=False,
        )
        assert claim is not None
        readback = store.read_ctp_dispatch_command(scope, staged.command_id)
        assert readback is not None and readback.status == "CLAIMED"
        with pytest.raises(ContractValidationError, match="not fresh for this Store"):
            store.fail_ctp_dispatch_command_before_native(
                scope,
                readback,
                owner_handle=owner,
                writer_lease=lease,
                failure_code="post_stage_failure",
            )
        with pytest.raises(InvalidStateTransition, match="no longer pre-native"):
            store.fail_ctp_dispatch_command_before_native(
                scope,
                staged,
                owner_handle=owner,
                writer_lease=lease,
                failure_code="post_stage_failure",
            )
        current = store.read_ctp_dispatch_command(scope, staged.command_id)
        assert current is not None and current.status == "CLAIMED"
    finally:
        store.close()


def _adapter_for(store, owner):
    return store._issued_ctp_callback_session_adapters[owner.owner_intent_id]


def _trade_fields(*, order_ref, exchange="SHFE", trade_id="trade-1", volume=1, price=99.0):
    return (
        (0, "BrokerID", "broker"),
        (0, "InvestorID", "user"),
        (0, "UserID", "user"),
        (0, "InstrumentID", "rb2710"),
        (0, "OrderRef", order_ref),
        (0, "ExchangeID", exchange),
        (0, "TradeID", trade_id),
        (0, "OrderSysID", "sys-" + order_ref),
        (0, "TradingDay", "20260925"),
        (0, "Direction", "0"),
        (0, "OffsetFlag", "0"),
        (0, "HedgeFlag", "1"),
        (0, "Price", price),
        (0, "Volume", volume),
        (0, "TradeDate", "20260925"),
        (0, "TradeTime", "09:31:00"),
        (0, "SequenceNo", 1),
        (0, "BrokerOrderSeq", 1),
        (0, "TradeSource", "0"),
    )


def _reserve(store, scope, lease, session_generation_id, intent):
    return store.seed_ctp_order_ref_and_reserve_identity(
        scope,
        _proof(scope, session_generation_id),
        intent,
        _runtime_id(intent),
        writer_lease=lease,
    )


@pytest.mark.unit
def test_session_router_applies_interleaved_trade_facts_and_order_cumulative_without_double_count(
    tmp_path,
):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-router-trades.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        binding = store._active_ctp_callback_sessions[owner.owner_intent_id]
        first = _reserve(store, scope, lease, binding.session_generation_id, "intent-one")
        second = _reserve(store, scope, lease, binding.session_generation_id, "intent-two")
        third = _reserve(store, scope, lease, binding.session_generation_id, "intent-three")
        def payload(reservation, exchange="SHFE"):
            return {
                "BrokerID": "broker",
                "InvestorID": "user",
                "UserID": "user",
                "InstrumentID": "rb2710",
                "ExchangeID": exchange,
                "OrderRef": reservation.order_ref,
                "Direction": "0",
                "CombOffsetFlag": "0",
                "CombHedgeFlag": "1",
                "LimitPrice": 100.0,
                "VolumeTotalOriginal": 3,
                "OrderPriceType": "2",
                "TimeCondition": "3",
            }
        _queue_and_complete(store, scope, lease, owner, first, "command-one", payload(first), 71)
        _queue_and_complete(store, scope, lease, owner, second, "command-two", payload(second), 72)
        _queue_and_complete(
            store,
            scope,
            lease,
            owner,
            third,
            "command-three",
            payload(third, exchange="DCE"),
            73,
        )
        adapter = _adapter_for(store, owner)

        for sequence, reservation, trade_id in (
            (4, first, "trade-1"),
            (5, second, "trade-2"),
            (6, first, "trade-3"),
        ):
            store.append_ctp_callback_ingress(
                owner,
                _record(
                    owner,
                    "OnRtnTrade",
                    sequence,
                    phase="ACTIVE",
                    callback_class="ROUTEABLE",
                    fields=_trade_fields(
                        order_ref=reservation.order_ref,
                        trade_id=trade_id,
                        price=99.0,
                    ),
                ),
            )
            result = adapter.apply_next_ingress(
                callback_verifier=_CallbackVerifier(), trade_fact_verifier=_TradeVerifier()
            )
            assert result.command_id == ("command-one" if reservation is first else "command-two")
            assert result.trade_quantity == 1
            assert result.duplicate is False

        # The exact same native TradeID/payload is idempotent in this durable
        # source stream and still advances its separate inbox sequence.
        store.append_ctp_callback_ingress(
            owner,
            _record(
                owner,
                "OnRtnTrade",
                7,
                phase="ACTIVE",
                callback_class="ROUTEABLE",
                fields=_trade_fields(order_ref=first.order_ref, trade_id="trade-3"),
            ),
        )
        duplicate = adapter.apply_next_ingress(
            callback_verifier=_CallbackVerifier(), trade_fact_verifier=_TradeVerifier()
        )
        assert duplicate.duplicate is True
        assert duplicate.cumulative_trade_quantity == 2

        # Native TradeID uniqueness includes ExchangeID; the same ID on a
        # distinct exchange is an independent durable fact.
        store.append_ctp_callback_ingress(
            owner,
            _record(
                owner,
                "OnRtnTrade",
                8,
                phase="ACTIVE",
                callback_class="ROUTEABLE",
                fields=_trade_fields(
                    order_ref=third.order_ref,
                    exchange="DCE",
                    trade_id="trade-1",
                ),
            ),
        )
        exchange_scoped = adapter.apply_next_ingress(
            callback_verifier=_CallbackVerifier(), trade_fact_verifier=_TradeVerifier()
        )
        assert exchange_scoped.command_id == "command-three"
        assert exchange_scoped.cumulative_trade_quantity == 1

        order_fields = (
            (0, "BrokerID", "broker"),
            (0, "InvestorID", "user"),
            (0, "UserID", "user"),
            (0, "InstrumentID", "rb2710"),
            (0, "RequestID", 71),
            (0, "OrderRef", first.order_ref),
            (0, "ExchangeID", "SHFE"),
            (0, "OrderSysID", "sys-" + first.order_ref),
            (0, "FrontID", 31),
            (0, "SessionID", 41),
            (0, "TradingDay", scope.trading_day),
            (0, "OrderStatus", "1"),
            (0, "OrderSubmitStatus", "3"),
            (0, "VolumeTraded", 2),
            (0, "VolumeTotal", 1),
            (0, "NotifySequence", 381),
            (0, "SequenceNo", 729),
        )
        store.append_ctp_callback_ingress(
            owner,
            _record(
                owner,
                "OnRtnOrder",
                9,
                phase="ACTIVE",
                callback_class="ROUTEABLE",
                fields=order_fields,
            ),
        )
        applied = adapter.apply_next_ingress(
            callback_verifier=_CallbackVerifier(), trade_fact_verifier=_TradeVerifier()
        )
        assert applied.projection_state == "PARTIALLY_FILLED"

        trade_rows = store._connection.execute(
            "SELECT command_id, trade_id, trade_volume, cumulative_trade_volume, "
            "cumulative_order_volume, prefix_consistency, cumulative_commission, "
            "commission_quality FROM ctp_dispatch_trade_fact_ledger "
            "ORDER BY source_sequence"
        ).fetchall()
        assert [(row["command_id"], row["trade_id"], row["cumulative_trade_volume"]) for row in trade_rows] == [
            ("command-one", "trade-1", 1),
            ("command-two", "trade-2", 1),
            ("command-one", "trade-3", 2),
            ("command-three", "trade-1", 1),
        ]
        assert all(row["cumulative_commission"] is None for row in trade_rows)
        assert all(row["commission_quality"] == "INCOMPLETE" for row in trade_rows)
        cumulative = store._connection.execute(
            "SELECT native_volume_traded, trade_volume_at_prefix, prefix_consistency "
            "FROM ctp_dispatch_order_cumulative_ledger"
        ).fetchone()
        assert tuple(cumulative) == (2, 2, "MATCHED")
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_trade_fact_ledger WHERE trade_id = 'trade-1'"
        ).fetchone()[0] == 2
        assert store.read_next_ctp_callback_ingress(owner) is None
    finally:
        store.close()


@pytest.mark.unit
def test_session_router_routes_cancel_by_target_then_exact_request_and_action_ref(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-router-cancel-correlation.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        session = store._active_ctp_callback_sessions[owner.owner_intent_id]
        reservation = _reserve(store, scope, lease, session.session_generation_id, "cancel-target")
        target = CtpCancelTarget(
            order_ref=reservation.order_ref,
            exchange_id="SHFE",
            order_sys_id="sys-" + reservation.order_ref,
            front_id=session.dispatch_front_id,
            session_id=session.dispatch_session_id,
        )
        target_verifier = _OrderTargetVerifier()

        def stage_cancel(command_id, request_id):
            projection = store.issue_ctp_order_target_projection(
                scope,
                reservation.managed_intent_id,
                {"session_generation_id": session.session_generation_id},
                verifier=target_verifier,
            )
            receipt_id = hashlib.sha256(command_id.encode("ascii")).hexdigest()[:32]
            store.stage_ctp_dispatch_command(
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
                    "LimitPrice": 0.0,
                    "VolumeChange": 0,
                },
                approval_use_id="approval-" + command_id,
                approval_digest=hashlib.sha256(
                    ("approval-" + command_id).encode("ascii")
                ).hexdigest(),
                session_binding={
                    "binding_type": "ctp_callback_session_binding.v1",
                    "owner_intent_id": session.owner_intent_id,
                    "account_key": session.account_key,
                    "scope_key": session.scope_key,
                    "trading_day": session.trading_day,
                    "session_generation_id": session.session_generation_id,
                    "dispatch_front_id": session.dispatch_front_id,
                    "dispatch_session_id": session.dispatch_session_id,
                    "source_tags": {
                        "source_instance_id": session.source_instance_id,
                        "native_client_epoch": session.native_client_epoch,
                        "native_api_source_id": session.native_api_source_id,
                        "native_spi_source_id": session.native_spi_source_id,
                        "native_api_generation": session.native_api_generation,
                        "connection_generation": session.source_connection_generation,
                    },
                    "source_high_watermark": session.source_high_watermark,
                },
                writer_lease=lease,
                managed_action_id="cancel-action-" + command_id,
                cancel_target=target,
                session_generation_id=session.session_generation_id,
                dispatch_front_id=session.dispatch_front_id,
                dispatch_session_id=session.dispatch_session_id,
                native_request_id=request_id,
                local_queue_receipt_id=receipt_id,
                cancel_target_projection=projection,
            )
            store.record_ctp_dispatch_queue_receipt(
                scope,
                command_id,
                {
                    "kind": "command_receipt",
                    "command": "cancel",
                    "receipt_id": receipt_id,
                    "queued": True,
                },
                writer_lease=lease,
            )
            claim = store.claim_ctp_dispatch_command_for_session(
                scope,
                command_id,
                owner_handle=owner,
                writer_lease=lease,
                authority_verifier=_Authority(),
                required_local_queue_receipt_id=receipt_id,
            )
            assert claim is not None
            command = claim.command
            receipt = CtpDispatchReceipt(
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
                outcome="QUEUED",
                native_receipt_payload={"request_id": request_id, "queue_code": 0},
                correlation_key=command.correlation_key,
                local_queue_receipt_id=receipt_id,
            )
            return store.complete_ctp_dispatch_command_for_session(
                scope, receipt, owner_handle=owner, writer_lease=lease
            )

        first = stage_cancel("cancel-same-target-one", 501)
        second = stage_cancel("cancel-same-target-two", 502)
        assert first.cancel_target_order_ref == second.cancel_target_order_ref == target.order_ref
        assert first.correlation_key.native_action_ref != second.correlation_key.native_action_ref

        store.append_ctp_callback_ingress(
            owner,
            _record(
                owner,
                "OnRspOrderAction",
                4,
                phase="ACTIVE",
                callback_class="ROUTEABLE",
                request_id=502,
                is_last=True,
                fields=(
                    (0, "BrokerID", "broker"),
                    (0, "InvestorID", "user"),
                    (0, "UserID", "user"),
                    (0, "InstrumentID", "rb2710"),
                    (0, "OrderRef", target.order_ref),
                    (0, "ExchangeID", target.exchange_id),
                    (0, "OrderSysID", target.order_sys_id),
                    (0, "FrontID", target.front_id),
                    (0, "SessionID", target.session_id),
                    (0, "RequestID", 502),
                    (0, "OrderActionRef", second.correlation_key.native_action_ref),
                    (0, "ActionFlag", "0"),
                    (1, "ErrorID", 0),
                ),
            ),
        )
        event = _adapter_for(store, owner).read_next_ingress()
        assert event is not None
        matched = store.find_ctp_dispatch_command_for_ingress(event)
        assert matched is not None
        assert matched.command_id == second.command_id
    finally:
        store.close()


@pytest.mark.unit
def test_session_router_poison_on_changed_duplicate_trade_and_wrong_day(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-router-trade-conflict.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        binding = store._active_ctp_callback_sessions[owner.owner_intent_id]
        reservation = _reserve(store, scope, lease, binding.session_generation_id, "intent-one")
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
            "VolumeTotalOriginal": 3,
            "OrderPriceType": "2",
            "TimeCondition": "3",
        }
        _queue_and_complete(store, scope, lease, owner, reservation, "command-one", payload, 71)
        adapter = _adapter_for(store, owner)
        store.append_ctp_callback_ingress(
            owner,
            _record(owner, "OnRtnTrade", 4, phase="ACTIVE", callback_class="ROUTEABLE",
                    fields=_trade_fields(order_ref=reservation.order_ref, trade_id="trade-repeat")),
        )
        assert adapter.apply_next_ingress(
            callback_verifier=_CallbackVerifier(), trade_fact_verifier=_TradeVerifier()
        ).trade_quantity == 1
        store.append_ctp_callback_ingress(
            owner,
            _record(owner, "OnRtnTrade", 5, phase="ACTIVE", callback_class="ROUTEABLE",
                    fields=_trade_fields(order_ref=reservation.order_ref,
                                         trade_id="trade-repeat", price=98.0)),
        )
        with pytest.raises(ContractValidationError, match="application failed"):
            adapter.apply_next_ingress(
                callback_verifier=_CallbackVerifier(), trade_fact_verifier=_TradeVerifier()
            )
        state = store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert tuple(state) == ("POISONED", "callback_apply_failure")
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_trade_fact_ledger"
        ).fetchone()[0] == 1
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_callback_ingress_applications "
            "WHERE owner_intent_id = ? AND source_sequence = 5",
            (owner.owner_intent_id,),
        ).fetchone()[0] == 0
    finally:
        store.close()


@pytest.mark.unit
def test_session_router_rejects_wrong_trading_day_without_fabricated_trade_request_ids(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-router-trade-day.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        binding = store._active_ctp_callback_sessions[owner.owner_intent_id]
        reservation = _reserve(store, scope, lease, binding.session_generation_id, "intent-one")
        payload = {
            "BrokerID": "broker", "InvestorID": "user", "UserID": "user",
            "InstrumentID": "rb2710", "ExchangeID": "SHFE", "OrderRef": reservation.order_ref,
            "Direction": "0", "CombOffsetFlag": "0", "CombHedgeFlag": "1",
            "LimitPrice": 100.0, "VolumeTotalOriginal": 3,
            "OrderPriceType": "2", "TimeCondition": "3",
        }
        _queue_and_complete(store, scope, lease, owner, reservation, "command-one", payload, 71)
        adapter = _adapter_for(store, owner)
        fields = tuple(
            (slot, name, "20260926" if name == "TradingDay" else value)
            for slot, name, value in _trade_fields(order_ref=reservation.order_ref)
        )
        assert not {"RequestID", "FrontID", "SessionID"} & {name for _, name, _ in fields}
        store.append_ctp_callback_ingress(
            owner,
            _record(owner, "OnRtnTrade", 4, phase="ACTIVE", callback_class="ROUTEABLE", fields=fields),
        )
        with pytest.raises(ContractValidationError, match="application failed"):
            adapter.apply_next_ingress(
                callback_verifier=_CallbackVerifier(), trade_fact_verifier=_TradeVerifier()
            )
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_trade_fact_ledger"
        ).fetchone()[0] == 0
        assert store._connection.execute(
            "SELECT owner_state FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()[0] == "POISONED"
    finally:
        store.close()


@pytest.mark.unit
def test_session_router_poison_on_order_cumulative_rollback(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-router-order-rollback.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        binding = store._active_ctp_callback_sessions[owner.owner_intent_id]
        reservation = _reserve(store, scope, lease, binding.session_generation_id, "intent-one")
        request = {
            "BrokerID": "broker", "InvestorID": "user", "UserID": "user",
            "InstrumentID": "rb2710", "ExchangeID": "SHFE", "OrderRef": reservation.order_ref,
            "Direction": "0", "CombOffsetFlag": "0", "CombHedgeFlag": "1",
            "LimitPrice": 100.0, "VolumeTotalOriginal": 3,
            "OrderPriceType": "2", "TimeCondition": "3",
        }
        _queue_and_complete(store, scope, lease, owner, reservation, "command-one", request, 71)
        adapter = _adapter_for(store, owner)

        def append_order(sequence, volume_traded, notify_sequence):
            fields = (
                (0, "BrokerID", "broker"),
                (0, "InvestorID", "user"),
                (0, "UserID", "user"),
                (0, "InstrumentID", "rb2710"),
                (0, "RequestID", 71),
                (0, "OrderRef", reservation.order_ref),
                (0, "ExchangeID", "SHFE"),
                (0, "OrderSysID", "sys-" + reservation.order_ref),
                (0, "FrontID", 31),
                (0, "SessionID", 41),
                (0, "TradingDay", scope.trading_day),
                (0, "OrderStatus", "1"),
                (0, "OrderSubmitStatus", "3"),
                (0, "VolumeTraded", volume_traded),
                (0, "VolumeTotal", 3 - volume_traded),
                (0, "NotifySequence", notify_sequence),
                (0, "SequenceNo", 800 + notify_sequence),
            )
            store.append_ctp_callback_ingress(
                owner,
                _record(
                    owner,
                    "OnRtnOrder",
                    sequence,
                    phase="ACTIVE",
                    callback_class="ROUTEABLE",
                    fields=fields,
                ),
            )

        append_order(4, 2, 381)
        adapter.apply_next_ingress(
            callback_verifier=_CallbackVerifier(), trade_fact_verifier=_TradeVerifier()
        )
        append_order(5, 1, 382)
        with pytest.raises(ContractValidationError, match="application failed"):
            adapter.apply_next_ingress(
                callback_verifier=_CallbackVerifier(), trade_fact_verifier=_TradeVerifier()
            )
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_order_cumulative_ledger"
        ).fetchone()[0] == 1
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_callback_ingress_applications "
            "WHERE owner_intent_id = ? AND source_sequence = 5",
            (owner.owner_intent_id,),
        ).fetchone()[0] == 0
        assert store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()[0:2] == ("POISONED", "callback_apply_failure")
    finally:
        store.close()


@pytest.mark.unit
def test_session_router_rolls_back_trade_projection_and_marker_as_one_transaction(
    tmp_path, monkeypatch
):
    scope = _scope()
    path = tmp_path / "session-router-trade-atomic-rollback.sqlite3"
    store = SqliteExecutionStore(path)
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        binding = store._active_ctp_callback_sessions[owner.owner_intent_id]
        reservation = _reserve(
            store, scope, lease, binding.session_generation_id, "atomic-trade-intent"
        )
        request = {
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
            "VolumeTotalOriginal": 3,
            "OrderPriceType": "2",
            "TimeCondition": "3",
        }
        _queue_and_complete(store, scope, lease, owner, reservation, "atomic-command", request, 121)
        adapter = _adapter_for(store, owner)
        store.append_ctp_callback_ingress(
            owner,
            _record(
                owner,
                "OnRtnTrade",
                4,
                phase="ACTIVE",
                callback_class="ROUTEABLE",
                fields=_trade_fields(
                    order_ref=reservation.order_ref, trade_id="atomic-trade"
                ),
            ),
        )

        def fail_after_ledger_and_projection(*args, **kwargs):
            raise RuntimeError("injected marker write failure")

        monkeypatch.setattr(
            store, "_insert_ctp_callback_ingress_application", fail_after_ledger_and_projection
        )
        with pytest.raises(ContractValidationError, match="application failed"):
            adapter.apply_next_ingress(
                callback_verifier=_CallbackVerifier(), trade_fact_verifier=_TradeVerifier()
            )

        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_trade_fact_ledger"
        ).fetchone()[0] == 0
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_order_projection"
        ).fetchone()[0] == 0
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_callback_ingress_applications "
            "WHERE owner_intent_id = ? AND source_sequence = 4",
            (owner.owner_intent_id,),
        ).fetchone()[0] == 0
        owner_row = store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert tuple(owner_row) == ("POISONED", "callback_apply_failure")
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_callback_ingress WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()[0] == 4
    finally:
        store.close()

    reopened = SqliteExecutionStore(path)
    try:
        assert reopened._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_trade_fact_ledger"
        ).fetchone()[0] == 0
        owner_row = reopened._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners"
        ).fetchone()
        assert tuple(owner_row) == ("POISONED", "callback_apply_failure")
        with pytest.raises(InvalidStateTransition, match="already has a durable"):
            reopened.create_ctp_callback_session_owner(scope, writer_lease=_lease(reopened, scope))
    finally:
        reopened.close()


@pytest.mark.unit
def test_session_router_serializes_one_adapter_consuming_multiple_audit_events(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-router-single-consumer.sqlite3")
    lease = _lease(store, scope)
    try:
        owner = _active_owner(store, scope, lease)
        adapter = _adapter_for(store, owner)
        for sequence in (4, 5):
            store.append_ctp_callback_ingress(
                owner,
                _record(
                    owner,
                    "OnRtnInstrumentStatus",
                    sequence,
                    phase="ACTIVE",
                    callback_class="AUDIT_INFORMATIONAL",
                    fields=((0, "InstrumentID", "rb2710"),),
                ),
            )

        def apply_one(_):
            return adapter.apply_next_ingress(
                callback_verifier=_CallbackVerifier(), trade_fact_verifier=_TradeVerifier()
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = tuple(executor.map(apply_one, (1, 2)))
        assert {result.source_sequence for result in results} == {4, 5}
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_callback_ingress_applications "
            "WHERE owner_intent_id = ? AND outcome = 'AUDIT_ONLY'",
            (owner.owner_intent_id,),
        ).fetchone()[0] == 5  # three login records and two informational records
        owner_row = store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert tuple(owner_row) == ("ACTIVE", None)
    finally:
        store.close()
