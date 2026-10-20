"""Store-contract tests for query-proven CTP cancellation targets.

The ingress events are built with the pinned callback-record builder, but the
order-target verifier below is deliberately a trusted fake. These tests cover
the Execution Store boundary only; they do not prove production SDK authority,
native dispatch, or provider acknowledgement.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, replace
from types import SimpleNamespace

import pytest

from bt_api_ctp.ctp.callback_ingress import (
    CtpTraderCallbackPhase,
    CtpTraderCallbackSourceTagsV2,
    build_callback_ingress_record_v2,
)
from bt_api_execution import (
    ContractValidationError,
    IntentConflictError,
    CtpCallbackSessionStoreAdapter,
    CtpCancelTarget,
    CtpDispatchAuthority,
    CtpOrderRefLegacyMapping,
    CtpOrderRefSeedProof,
    CtpVerifiedOrderTargetProjection,
    ExecutionScope,
    InvalidStateTransition,
    SqliteExecutionStore,
    WriterLeaseUnavailable,
    payload_sha256,
)
from bt_api_execution import store as execution_store_module


_REQUEST_ID = 1701


@dataclass(frozen=True)
class _LoginObservation:
    broker_id: str = "broker"
    user_id: str = "user"
    trading_day: str = "20260925"
    connection_generation: int = 1
    request_id: int = 17


class _Authority:
    """Trusted local approval leaf for the Store-contract boundary only."""

    def verify_action(self, command, *, now_ns):
        return CtpDispatchAuthority(
            authority_type="ctp_dispatch_authority.v1",
            command_binding_sha256=command.authority_binding_sha256,
            approval_use_id=command.approval_use_id,
            approval_digest=command.approval_digest,
            source_digest_sha256=hashlib.sha256(b"test-only trusted authority").hexdigest(),
            verifier_id="test-only-ctp-query-cancel-authority",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + 5_000_000_000,
        )


class _QueryWindowVerifier:
    """A deterministic fake oracle that checks the Store-issued query window."""

    def __init__(self, *, expected_order_ref: str):
        self.expected_order_ref = expected_order_ref
        self.seen_window = None

    def verify_order_target(
        self, scope, reservation, native_query_evidence, *, now_ns, query_window=None
    ):
        self.seen_window = query_window
        if query_window is None or query_window.query_request_id != _REQUEST_ID:
            raise ContractValidationError("test verifier requires the exact query window")

        rows = []
        request_ids = []
        last_flags = []
        for event in query_window.events:
            if event.callback_name != "OnRspQryOrder" or event.callback_class != "AUDIT_QUERY":
                raise ContractValidationError("query window contains a non-query event")
            payload = json.loads(event.record_payload_json)
            args = payload["named_args"]
            request_ids.append(args[2]["value"])
            last_flags.append(args[3]["value"])
            fields = {
                row["field_name"]: row["value"]
                for row in payload["flattened_fields"]
                if row["argument_slot"] == 0
                and row["present"] is True
                and row["scalar_captured"] is True
            }
            if fields:
                rows.append(fields)

        if request_ids != [_REQUEST_ID, _REQUEST_ID] or last_flags != [False, True]:
            raise ContractValidationError("query callbacks are not one complete terminal response")
        matches = [
            row
            for row in rows
            if row.get("OrderRef") == self.expected_order_ref
            and row.get("ExchangeID") == "SHFE"
            and row.get("OrderSysID")
        ]
        if len(rows) != 1 or len(matches) != 1:
            raise ContractValidationError("query did not uniquely identify the reserved order")
        row = matches[0]
        if row.get("FrontID") != 31 or row.get("SessionID") != 41:
            raise ContractValidationError("query result is not bound to the active session")
        if row.get("OrderStatus") != "3" or row.get("VolumeTraded") != 0:
            raise ContractValidationError("test query row is not currently open")

        records_digest = payload_sha256(
            [json.loads(event.record_payload_json) for event in query_window.events]
        )
        source_digest = payload_sha256(
            [event.record_digest_sha256 for event in query_window.events]
        )
        return CtpVerifiedOrderTargetProjection(
            account_key=reservation.account_key,
            scope_key=reservation.scope_key,
            trading_day=reservation.trading_day,
            managed_intent_id=reservation.managed_intent_id,
            runtime_order_id=reservation.runtime_order_id,
            order_ref=reservation.order_ref,
            account_fingerprint_sha256=hashlib.sha256(
                reservation.account_key.encode("ascii")
            ).hexdigest(),
            registration_digest=hashlib.sha256(b"test-only registration binding").hexdigest(),
            instrument_id=row["InstrumentID"],
            exchange_id=row["ExchangeID"],
            session_generation_id=native_query_evidence["session_generation_id"],
            connection_generation=query_window.events[-1].connection_generation,
            query_front_id=31,
            query_session_id=41,
            query_request_id=query_window.query_request_id,
            query_filters_sha256=payload_sha256(
                {"OrderRef": reservation.order_ref, "ExchangeID": "SHFE"}
            ),
            query_records_sha256=records_digest,
            source_evidence_sha256=source_digest,
            query_record_count=1,
            query_match_count=1,
            query_complete=True,
            query_terminal=True,
            query_timed_out=False,
            query_error_id=None,
            late_callback_count=0,
            order_sys_id=row["OrderSysID"],
            front_id=row["FrontID"],
            session_id=row["SessionID"],
            provider_state="OPEN",
            quantity=row["VolumeTotalOriginal"],
            traded_quantity=row["VolumeTraded"],
            remaining_quantity=row["VolumeTotal"],
            verifier_id="test-only-query-window-verifier",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + 2_000_000_000,
        )


def _scope() -> ExecutionScope:
    account_ref = "ctp-account-ref.v1:" + hashlib.sha256(b"query-fence-account").hexdigest()
    return ExecutionScope("ctp", "simulation", account_ref, "strategy.query-fence", "20260925")


def _new_store_and_session(tmp_path, name="query-fence.sqlite3"):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / name)
    family_owner = store.acquire_ctp_account_family_owner(scope)
    lease = store.acquire_or_renew_lease(
        scope,
        "query-fence-writer",
        ttl_ns=60_000_000_000,
        ctp_account_family_owner=family_owner,
    )
    owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)
    tags = CtpTraderCallbackSourceTagsV2(
        source_instance_id="query-fence-source-instance",
        native_client_epoch="a" * 32,
        native_api_source_id="query-fence-api-source",
        native_spi_source_id="query-fence-spi-source",
        native_api_generation=1,
        connection_generation=0,
    )
    adapter = CtpCallbackSessionStoreAdapter(
        store,
        scope,
        owner,
        lease,
        lambda **kwargs: kwargs,
        type(
            build_callback_ingress_record_v2(
                owner_intent_id=owner.owner_intent_id,
                callback_name="OnFrontConnected",
                source_phase=CtpTraderCallbackPhase.PRE_LOGIN,
                source_tags=tags,
                connection_generation=0,
                source_sequence=1,
                callback_monotonic_ns=time.monotonic_ns(),
                args=(),
                kwargs=None,
            )
        ),
    )

    _append_callback(
        adapter,
        owner,
        tags,
        "OnFrontConnected",
        1,
        (),
        source_generation=0,
        phase=CtpTraderCallbackPhase.PRE_LOGIN,
    )
    _append_callback(
        adapter,
        owner,
        tags,
        "OnRspAuthenticate",
        2,
        (
            SimpleNamespace(BrokerID="broker", UserID="user"),
            SimpleNamespace(ErrorID=0, ErrorMsg=""),
            17,
            True,
        ),
        source_generation=1,
        phase=CtpTraderCallbackPhase.PRE_LOGIN,
    )
    _append_callback(
        adapter,
        owner,
        tags,
        "OnRspUserLogin",
        3,
        (
            SimpleNamespace(
                BrokerID="broker",
                UserID="user",
                TradingDay=scope.trading_day,
                FrontID=31,
                SessionID=41,
                MaxOrderRef="000000000010",
            ),
            SimpleNamespace(ErrorID=0, ErrorMsg=""),
            17,
            True,
        ),
        source_generation=1,
        phase=CtpTraderCallbackPhase.PRE_LOGIN,
    )
    binding = adapter.bind_session(
        owner,
        observation=_LoginObservation(trading_day=scope.trading_day),
        source_tags=tags,
        high_watermark=3,
    )
    return store, scope, family_owner, lease, owner, adapter, binding, tags


def _append_callback(
    adapter,
    owner,
    tags,
    callback_name,
    sequence,
    args,
    *,
    source_generation,
    phase,
):
    record = build_callback_ingress_record_v2(
        owner_intent_id=owner.owner_intent_id,
        callback_name=callback_name,
        source_phase=phase,
        source_tags=tags,
        connection_generation=source_generation,
        source_sequence=sequence,
        callback_monotonic_ns=time.monotonic_ns(),
        args=args,
        kwargs=None,
    )
    acknowledgement = adapter.sink.append(record)
    assert acknowledgement["commit_state"] == "COMMITTED"
    assert acknowledgement["sequence"] == sequence


def _seed_proof(scope, session_generation_id):
    source_digests = (
        ("backtrader_prototype", hashlib.sha256(b"prototype fixture").hexdigest()),
        ("sdk_jsonl", hashlib.sha256(b"sdk fixture").hexdigest()),
    )
    legacy_scope = ExecutionScope(
        "ctp", "simulation", scope.account_ref, "strategy.legacy", "20260924"
    )
    mapping = CtpOrderRefLegacyMapping(
        source_name="backtrader_prototype",
        account_key=scope.account_key,
        trading_day="20260924",
        scope_key=legacy_scope.key,
        managed_intent_id="legacy-intent",
        runtime_order_id="legacy-runtime-order",
        order_ref="000000000012",
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
        legacy_mappings=(mapping,),
    )


def _reserve(store, scope, lease, binding, managed_intent_id="query-intent"):
    runtime_order_id = "bt-managed-v1:" + hashlib.sha256(
        managed_intent_id.encode("ascii")
    ).hexdigest()
    return store.seed_ctp_order_ref_and_reserve_identity(
        scope,
        _seed_proof(scope, binding.session_generation_id),
        managed_intent_id,
        runtime_order_id,
        writer_lease=lease,
    )


def _session_payload(binding):
    return {
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
    }


def _order_row(reservation, *, order_ref=None):
    return SimpleNamespace(
        BrokerID="broker",
        InvestorID="user",
        UserID="user",
        InstrumentID="rb2710",
        ExchangeID="SHFE",
        OrderRef=reservation.order_ref if order_ref is None else order_ref,
        OrderSysID="sys-" + reservation.order_ref,
        RequestID=_REQUEST_ID,
        FrontID=31,
        SessionID=41,
        TradingDay=reservation.trading_day,
        SequenceNo=801,
        NotifySequence=1,
        Direction="0",
        CombOffsetFlag="0",
        CombHedgeFlag="1",
        OrderStatus="3",
        OrderSubmitStatus="3",
        VolumeTotalOriginal=1,
        VolumeTotal=1,
        VolumeTraded=0,
        LimitPrice=100.0,
    )


def _append_query_pair(
    adapter,
    owner,
    binding,
    tags,
    reservation,
    *,
    request_id=_REQUEST_ID,
    row_order_ref=None,
    start_sequence=None,
):
    sequence = (
        binding.source_high_watermark + 1
        if start_sequence is None
        else start_sequence
    )
    _append_callback(
        adapter,
        owner,
        tags,
        "OnRspQryOrder",
        sequence,
        (
            _order_row(reservation, order_ref=row_order_ref),
            SimpleNamespace(ErrorID=0, ErrorMsg=""),
            request_id,
            False,
        ),
        source_generation=binding.connection_generation,
        phase=CtpTraderCallbackPhase.ACTIVE,
    )
    _append_callback(
        adapter,
        owner,
        tags,
        "OnRspQryOrder",
        sequence + 1,
        (
            None,
            SimpleNamespace(ErrorID=0, ErrorMsg=""),
            request_id,
            True,
        ),
        source_generation=binding.connection_generation,
        phase=CtpTraderCallbackPhase.ACTIVE,
    )
    return sequence + 1


def _apply_query_pair(adapter):
    first = adapter.apply_next_ingress(callback_verifier=None, trade_fact_verifier=None)
    second = adapter.apply_next_ingress(callback_verifier=None, trade_fact_verifier=None)
    assert first is not None and first.callback_name == "OnRspQryOrder"
    assert second is not None and second.callback_name == "OnRspQryOrder"
    assert first.economic_query_observed is True
    assert second.economic_query_observed is True


def _append_trade_query_terminal(
    adapter,
    owner,
    binding,
    tags,
    *,
    request_id=_REQUEST_ID,
    sequence=None,
):
    if sequence is None:
        sequence = binding.source_high_watermark + 1
    _append_callback(
        adapter,
        owner,
        tags,
        "OnRspQryTrade",
        sequence,
        (None, SimpleNamespace(ErrorID=0, ErrorMsg=""), request_id, True),
        source_generation=binding.connection_generation,
        phase=CtpTraderCallbackPhase.ACTIVE,
    )
    result = adapter.apply_next_ingress(
        callback_verifier=None, trade_fact_verifier=None
    )
    assert result is not None and result.callback_name == "OnRspQryTrade"
    assert result.economic_query_observed is True
    return sequence


@pytest.mark.unit
def test_trade_query_window_distinguishes_empty_terminal_from_absent_or_invalid_callback(
    tmp_path,
):
    store, scope, _family_owner, lease, owner, adapter, binding, tags = _new_store_and_session(
        tmp_path
    )
    try:
        reservation = _reserve(store, scope, lease, binding, "trade-window-empty")
        fence = store.begin_ctp_order_target_query(
            scope,
            reservation.managed_intent_id,
            owner_handle=owner,
            writer_lease=lease,
        )
        sequence = _append_trade_query_terminal(
            adapter, owner, binding, tags
        )

        window = store.read_ctp_order_target_trade_query_window(
            scope,
            fence,
            query_request_id=_REQUEST_ID,
            owner_handle=owner,
            writer_lease=lease,
        )
        assert window.fence_id == fence.fence_id
        assert len(window.events) == 1
        assert window.events[0].callback_name == "OnRspQryTrade"
        assert window.events[0].source_sequence == sequence
        assert store._active_ctp_order_target_query_fences.get(
            owner.owner_intent_id
        ) is None
        assert store.read_ctp_order_target_trade_query_window(
            scope,
            fence,
            query_request_id=_REQUEST_ID,
            owner_handle=owner,
            writer_lease=lease,
        ) is window

        # A completed trade audit no longer holds the per-owner query fence.
        later_fence = store.begin_ctp_order_target_query(
            scope,
            reservation.managed_intent_id,
            owner_handle=owner,
            writer_lease=lease,
        )
        assert later_fence is not fence
    finally:
        store.close()

    for variant in ("absent", "wrong-request", "missing-terminal", "unapplied"):
        store, scope, _family_owner, lease, owner, adapter, binding, tags = _new_store_and_session(
            tmp_path, f"trade-window-{variant}.sqlite3"
        )
        try:
            reservation = _reserve(
                store, scope, lease, binding, "trade-window-" + variant
            )
            fence = store.begin_ctp_order_target_query(
                scope,
                reservation.managed_intent_id,
                owner_handle=owner,
                writer_lease=lease,
            )
            if variant != "absent":
                sequence = binding.source_high_watermark + 1
                request_id = _REQUEST_ID + 1 if variant == "wrong-request" else _REQUEST_ID
                is_last = variant != "missing-terminal"
                _append_callback(
                    adapter,
                    owner,
                    tags,
                    "OnRspQryTrade",
                    sequence,
                    (None, SimpleNamespace(ErrorID=0, ErrorMsg=""), request_id, is_last),
                    source_generation=binding.connection_generation,
                    phase=CtpTraderCallbackPhase.ACTIVE,
                )
                if variant != "unapplied":
                    result = adapter.apply_next_ingress(
                        callback_verifier=None, trade_fact_verifier=None
                    )
                    assert result is not None
            with pytest.raises((ContractValidationError, InvalidStateTransition)):
                store.read_ctp_order_target_trade_query_window(
                    scope,
                    fence,
                    query_request_id=_REQUEST_ID,
                    owner_handle=owner,
                    writer_lease=lease,
                )
        finally:
            store.close()


def _begin_window_and_projection(
    store,
    scope,
    lease,
    owner,
    adapter,
    binding,
    tags,
    reservation,
    *,
    row_order_ref=None,
    apply=True,
    request_id=_REQUEST_ID,
    start_sequence=None,
):
    fence = store.begin_ctp_order_target_query(
        scope,
        reservation.managed_intent_id,
        owner_handle=owner,
        writer_lease=lease,
    )
    if row_order_ref is None:
        _append_query_pair(
            adapter,
            owner,
            binding,
            tags,
            reservation,
            request_id=request_id,
            start_sequence=start_sequence,
        )
    else:
        sequence = (
            binding.source_high_watermark + 1
            if start_sequence is None
            else start_sequence
        )
        _append_callback(
            adapter,
            owner,
            tags,
            "OnRspQryOrder",
            sequence,
            (
                _order_row(reservation, order_ref=row_order_ref),
                SimpleNamespace(ErrorID=0, ErrorMsg=""),
                request_id,
                False,
            ),
            source_generation=binding.connection_generation,
            phase=CtpTraderCallbackPhase.ACTIVE,
        )
        _append_callback(
            adapter,
            owner,
            tags,
            "OnRspQryOrder",
            sequence + 1,
            (None, SimpleNamespace(ErrorID=0, ErrorMsg=""), request_id, True),
            source_generation=binding.connection_generation,
            phase=CtpTraderCallbackPhase.ACTIVE,
        )
    if apply:
        _apply_query_pair(adapter)
    window = store.read_ctp_order_target_query_window(
        scope,
        fence,
        query_request_id=request_id,
        owner_handle=owner,
        writer_lease=lease,
    )
    verifier = _QueryWindowVerifier(expected_order_ref=reservation.order_ref)
    handle = store.issue_ctp_order_target_projection(
        scope,
        reservation.managed_intent_id,
        {
            "test_only": "trusted-fake-query-verifier",
            "session_generation_id": binding.session_generation_id,
        },
        verifier=verifier,
        owner_handle=owner,
        writer_lease=lease,
        query_fence=fence,
        query_window=window,
    )
    return fence, window, handle, verifier


def _stage_cancel(
    store,
    scope,
    lease,
    owner,
    binding,
    reservation,
    handle,
    command_id,
    *,
    native_request_id=2001,
):
    receipt_id = hashlib.sha256(command_id.encode("ascii")).hexdigest()[:32]
    target = CtpCancelTarget(
        order_ref=handle.projection.order_ref,
        exchange_id=handle.projection.exchange_id,
        order_sys_id=handle.projection.order_sys_id,
        front_id=handle.projection.front_id,
        session_id=handle.projection.session_id,
    )
    staged = store.stage_ctp_dispatch_command(
        scope,
        command_id,
        "CANCEL",
        {
            "InstrumentID": handle.projection.instrument_id,
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
        approval_digest=hashlib.sha256(("approval-" + command_id).encode("ascii")).hexdigest(),
        session_binding=_session_payload(binding),
        writer_lease=lease,
        managed_action_id="cancel-action-" + command_id,
        session_generation_id=binding.session_generation_id,
        dispatch_front_id=binding.dispatch_front_id,
        dispatch_session_id=binding.dispatch_session_id,
        native_request_id=native_request_id,
        local_queue_receipt_id=receipt_id,
        cancel_target=target,
        cancel_target_projection=handle,
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
    return staged, receipt_id


def _durable_query_bit(store, owner):
    row = store._connection.execute(
        """SELECT economic_query_observed
           FROM ctp_dispatch_callback_session_owners WHERE owner_intent_id = ?""",
        (owner.owner_intent_id,),
    ).fetchone()
    assert row is not None
    return int(row["economic_query_observed"])


@pytest.mark.unit
def test_applied_exact_query_allows_one_cancel_binding_and_preserves_query_fence(tmp_path):
    store, scope, _family_owner, lease, owner, adapter, binding, tags = _new_store_and_session(tmp_path)
    try:
        reservation = _reserve(store, scope, lease, binding)
        _append_query_pair(adapter, owner, binding, tags, reservation)
        _apply_query_pair(adapter)
        assert _durable_query_bit(store, owner) == 1

        fence, window, handle, verifier = _begin_window_and_projection(
            store,
            scope,
            lease,
            owner,
            adapter,
            binding,
            tags,
            reservation,
            start_sequence=binding.source_high_watermark + 3,
        )
        assert verifier.seen_window is window
        assert handle.projection.query_provenance.fence_id == fence.fence_id
        assert handle.projection.query_provenance.query_request_id == _REQUEST_ID
        assert handle.projection.query_provenance.end_source_sequence == window.end_source_sequence
        assert _durable_query_bit(store, owner) == 1

        staged, receipt_id = _stage_cancel(
            store, scope, lease, owner, binding, reservation, handle, "query-cancel-one"
        )
        assert staged.status == "READY"
        assert _durable_query_bit(store, owner) == 1
        with pytest.raises(
            IntentConflictError,
            match="^CTP target projection is already bound to another action$",
        ):
            _stage_cancel(
                store,
                scope,
                lease,
                owner,
                binding,
                reservation,
                handle,
                "query-cancel-replay",
                native_request_id=2002,
            )
        assert store.read_ctp_dispatch_command(scope, "query-cancel-replay") is None

        claim = store.claim_ctp_dispatch_command_for_session(
            scope,
            "query-cancel-one",
            owner_handle=owner,
            writer_lease=lease,
            authority_verifier=_Authority(),
            required_local_queue_receipt_id=receipt_id,
        )
        assert claim is not None
        assert claim.command.operation == "CANCEL"
        native_binding = claim.binding
        assert native_binding.operation == "CANCEL"
        assert native_binding.cancel_target_order_ref == reservation.order_ref
        assert store.verify_ctp_managed_native_call_binding(
            scope, owner, native_binding, writer_lease=lease
        )
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            store.verify_ctp_managed_native_call_binding(
                scope, owner, native_binding, writer_lease=lease
            )
        assert _durable_query_bit(store, owner) == 1
    finally:
        store.close()


@pytest.mark.unit
def test_query_read_window_rejects_unapplied_wrong_request_and_missing_terminal(tmp_path):
    for variant in ("unapplied", "wrong-request", "missing-terminal"):
        store, scope, _family_owner, lease, owner, adapter, binding, tags = _new_store_and_session(
            tmp_path, f"query-window-{variant}.sqlite3"
        )
        try:
            reservation = _reserve(store, scope, lease, binding, "query-intent-" + variant)
            fence = store.begin_ctp_order_target_query(
                scope,
                reservation.managed_intent_id,
                owner_handle=owner,
                writer_lease=lease,
            )
            sequence = binding.source_high_watermark + 1
            request_id = _REQUEST_ID + (1 if variant == "wrong-request" else 0)
            _append_callback(
                adapter,
                owner,
                tags,
                "OnRspQryOrder",
                sequence,
                (
                    _order_row(reservation),
                    SimpleNamespace(ErrorID=0, ErrorMsg=""),
                    _REQUEST_ID,
                    False,
                ),
                source_generation=binding.connection_generation,
                phase=CtpTraderCallbackPhase.ACTIVE,
            )
            if variant != "missing-terminal":
                _append_callback(
                    adapter,
                    owner,
                    tags,
                    "OnRspQryOrder",
                    sequence + 1,
                    (
                        None,
                        SimpleNamespace(ErrorID=0, ErrorMsg=""),
                        _REQUEST_ID,
                        True,
                    ),
                    source_generation=binding.connection_generation,
                    phase=CtpTraderCallbackPhase.ACTIVE,
                )
            if variant != "unapplied":
                count = 1 if variant == "missing-terminal" else 2
                for _ in range(count):
                    result = adapter.apply_next_ingress(
                        callback_verifier=None, trade_fact_verifier=None
                    )
                    assert result is not None
            with pytest.raises((ContractValidationError, InvalidStateTransition)):
                store.read_ctp_order_target_query_window(
                    scope,
                    fence,
                    query_request_id=request_id,
                    owner_handle=owner,
                    writer_lease=lease,
                )
        finally:
            store.close()


@pytest.mark.unit
def test_query_miss_consumes_fence_and_equivalent_forged_owner_is_rejected(tmp_path):
    store, scope, _family_owner, lease, owner, adapter, binding, tags = _new_store_and_session(tmp_path)
    try:
        reservation = _reserve(store, scope, lease, binding)
        fence = store.begin_ctp_order_target_query(
            scope,
            reservation.managed_intent_id,
            owner_handle=owner,
            writer_lease=lease,
        )
        forged_owner = replace(owner)
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            store.read_ctp_order_target_query_window(
                scope,
                fence,
                query_request_id=_REQUEST_ID,
                owner_handle=forged_owner,
                writer_lease=lease,
            )

        _append_query_pair(
            adapter,
            owner,
            binding,
            tags,
            reservation,
            row_order_ref="000000000099",
        )
        _apply_query_pair(adapter)
        window = store.read_ctp_order_target_query_window(
            scope,
            fence,
            query_request_id=_REQUEST_ID,
            owner_handle=owner,
            writer_lease=lease,
        )
        verifier = _QueryWindowVerifier(expected_order_ref=reservation.order_ref)
        with pytest.raises(ContractValidationError):
            store.issue_ctp_order_target_projection(
                scope,
                reservation.managed_intent_id,
                {"test_only": True, "session_generation_id": binding.session_generation_id},
                verifier=verifier,
                owner_handle=owner,
                writer_lease=lease,
                query_fence=fence,
                query_window=window,
            )
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            store.issue_ctp_order_target_projection(
                scope,
                reservation.managed_intent_id,
                {"test_only": True, "session_generation_id": binding.session_generation_id},
                verifier=verifier,
                owner_handle=owner,
                writer_lease=lease,
                query_fence=fence,
                query_window=window,
            )
    finally:
        store.close()


@pytest.mark.unit
def test_expired_source_drift_and_writer_generation_fences_fail_closed(tmp_path, monkeypatch):
    store, scope, family_owner, lease, owner, adapter, binding, tags = _new_store_and_session(tmp_path)
    try:
        reservation = _reserve(store, scope, lease, binding)

        expired_fence = store.begin_ctp_order_target_query(
            scope,
            reservation.managed_intent_id,
            owner_handle=owner,
            writer_lease=lease,
        )
        expired_pair_end = _append_query_pair(adapter, owner, binding, tags, reservation)
        _apply_query_pair(adapter)
        expired_now = expired_fence.expires_at_ns + 1
        monkeypatch.setattr(execution_store_module.time, "monotonic_ns", lambda: expired_now)
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            store.read_ctp_order_target_query_window(
                scope,
                expired_fence,
                query_request_id=_REQUEST_ID,
                owner_handle=owner,
                writer_lease=lease,
            )
        # A new query establishes a fresh fence after the previous expired
        # fence was consumed/rejected. Keep the monotonic clock at the advanced
        # value so the expired fence cannot become live again.
        # Any later contiguous source sequence invalidates the read window.
        source_fence = store.begin_ctp_order_target_query(
            scope,
            reservation.managed_intent_id,
            owner_handle=owner,
            writer_lease=lease,
        )
        _append_query_pair(
            adapter,
            owner,
            binding,
            tags,
            reservation,
            start_sequence=expired_pair_end + 1,
        )
        _apply_query_pair(adapter)
        source_window = store.read_ctp_order_target_query_window(
            scope,
            source_fence,
            query_request_id=_REQUEST_ID,
            owner_handle=owner,
            writer_lease=lease,
        )
        next_sequence = source_window.end_source_sequence + 1
        _append_callback(
            adapter,
            owner,
            tags,
            "OnRspQryOrder",
            next_sequence,
            (None, SimpleNamespace(ErrorID=0, ErrorMsg=""), _REQUEST_ID + 1, True),
            source_generation=binding.connection_generation,
            phase=CtpTraderCallbackPhase.ACTIVE,
        )
        result = adapter.apply_next_ingress(callback_verifier=None, trade_fact_verifier=None)
        assert result is not None
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            store.issue_ctp_order_target_projection(
                scope,
                reservation.managed_intent_id,
                {"test_only": True, "session_generation_id": binding.session_generation_id},
                verifier=_QueryWindowVerifier(expected_order_ref=reservation.order_ref),
                owner_handle=owner,
                writer_lease=lease,
                query_fence=source_fence,
                query_window=source_window,
            )

        # A fresh Store-issued writer generation also invalidates existing
        # owner/query authority; stale owner leases cannot read a window.
        drift_fence = store.begin_ctp_order_target_query(
            scope,
            reservation.managed_intent_id,
            owner_handle=owner,
            writer_lease=lease,
        )
        store.release_lease(scope, lease.owner_id, fencing_token=lease.fencing_token)
        new_lease = store.acquire_or_renew_lease(
            scope,
            lease.owner_id,
            ttl_ns=60_000_000_000,
            ctp_account_family_owner=family_owner,
        )
        assert new_lease.fencing_token > lease.fencing_token
        with pytest.raises((ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable)):
            store.read_ctp_order_target_query_window(
                scope,
                drift_fence,
                query_request_id=_REQUEST_ID,
                owner_handle=owner,
                writer_lease=new_lease,
            )
    finally:
        store.close()


@pytest.mark.unit
def test_query_window_readback_preserves_timestamp_but_rejects_source_change(
    tmp_path, monkeypatch
):
    store, scope, _family_owner, lease, owner, adapter, binding, tags = _new_store_and_session(
        tmp_path
    )
    try:
        reservation = _reserve(store, scope, lease, binding)
        fence = store.begin_ctp_order_target_query(
            scope,
            reservation.managed_intent_id,
            owner_handle=owner,
            writer_lease=lease,
        )
        _append_query_pair(adapter, owner, binding, tags, reservation)
        _apply_query_pair(adapter)

        clock = {"now": max(fence.created_at_ns + 1, time.monotonic_ns())}
        monkeypatch.setattr(execution_store_module.time, "monotonic_ns", lambda: clock["now"])
        first_window = store.read_ctp_order_target_query_window(
            scope,
            fence,
            query_request_id=_REQUEST_ID,
            owner_handle=owner,
            writer_lease=lease,
        )
        first_observed_at_ns = first_window.observed_at_ns
        first_events = first_window.events

        clock["now"] += 1_000_000
        repeated_window = store.read_ctp_order_target_query_window(
            scope,
            fence,
            query_request_id=_REQUEST_ID,
            owner_handle=owner,
            writer_lease=lease,
        )
        assert repeated_window is first_window
        assert repeated_window.observed_at_ns == first_observed_at_ns
        assert repeated_window.events is first_events
        assert all(
            store._issued_ctp_callback_ingress_events[
                (owner.owner_intent_id, event.source_sequence)
            ] is event
            for event in first_events
        )

        _append_query_pair(
            adapter,
            owner,
            binding,
            tags,
            reservation,
            start_sequence=first_window.end_source_sequence + 1,
        )
        _apply_query_pair(adapter)
        clock["now"] += 1_000_000
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            store.read_ctp_order_target_query_window(
                scope,
                fence,
                query_request_id=_REQUEST_ID,
                owner_handle=owner,
                writer_lease=lease,
            )
    finally:
        store.close()


@pytest.mark.unit
def test_permanent_query_observation_never_reopens_submit(tmp_path):
    store, scope, _family_owner, lease, owner, adapter, binding, tags = _new_store_and_session(tmp_path)
    try:
        reservation = _reserve(store, scope, lease, binding)
        _begin_window_and_projection(store, scope, lease, owner, adapter, binding, tags, reservation)
        assert _durable_query_bit(store, owner) == 1

        command_id = "query-fenced-submit"
        receipt_id = hashlib.sha256(command_id.encode("ascii")).hexdigest()[:32]
        try:
            store.stage_ctp_dispatch_command(
                scope,
                command_id,
                "SUBMIT",
                {
                    "InstrumentID": "rb2710",
                    "ExchangeID": "SHFE",
                    "Direction": "0",
                    "CombOffsetFlag": "0",
                    "CombHedgeFlag": "1",
                    "LimitPrice": 100.0,
                    "VolumeTotalOriginal": 1,
                    "OrderPriceType": "2",
                    "TimeCondition": "3",
                    "VolumeCondition": "1",
                    "ContingentCondition": "1",
                    "ForceCloseReason": "0",
                    "IsAutoSuspend": 0,
                    "UserForceClose": 0,
                    "OrderRef": reservation.order_ref,
                },
                approval_use_id="approval-query-fenced-submit",
                approval_digest=hashlib.sha256(b"approval-query-fenced-submit").hexdigest(),
                session_binding=_session_payload(binding),
                writer_lease=lease,
                managed_intent_id=reservation.managed_intent_id,
                order_ref=reservation.order_ref,
                session_generation_id=binding.session_generation_id,
                dispatch_front_id=binding.dispatch_front_id,
                dispatch_session_id=binding.dispatch_session_id,
                native_request_id=2999,
                local_queue_receipt_id=receipt_id,
            )
        except (ContractValidationError, InvalidStateTransition):
            assert _durable_query_bit(store, owner) == 1
            return
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
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            store.claim_ctp_dispatch_command_for_session(
                scope,
                command_id,
                owner_handle=owner,
                writer_lease=lease,
                authority_verifier=_Authority(),
                required_local_queue_receipt_id=receipt_id,
            )
        assert _durable_query_bit(store, owner) == 1
    finally:
        store.close()
