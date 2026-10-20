"""Source-fake Store tests for CTP position-based dispatch authorization.

These tests exercise the real SQLite Execution Store and callback journal. The
native API and the query evidence issuer are deterministic fakes; no CTP
provider, credentials, native process, or account is contacted.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

import bt_api_execution.store as execution_store_module
from bt_api_ctp.containers.ctp.ctp_position_evidence import build_ctp_position_evidence
from bt_api_ctp.ctp.callback_ingress import CtpTraderCallbackPhase
from bt_api_ctp.ctp.client import (
    TraderClient,
    _TRADER_LOGIN_IDENTITY_SEAL,
    _TraderLoginIdentityObservation,
)
from bt_api_execution import (
    CancelIntent,
    CancelObservation,
    ContractValidationError,
    CtpDispatchAuthority,
    CtpDispatchReceipt,
    CtpVerifiedCallbackEvidence,
    CtpVerifiedTradeFactEvidence,
    DurableStoreError,
    ExecutionState,
    InvalidStateTransition,
    OrderIntent,
    OrderType,
    PositionEffect,
    ProviderObservation,
    SqliteExecutionStore,
    ExecutionScope,
    Side,
    WriterLeaseUnavailable,
)
from test_ctp_order_query_cancel_fence import (
    _append_callback,
    _new_store_and_session,
    _reserve,
    _session_payload,
)
from bt_api_risk import (
    AccountScope,
    DispatchEvidenceClass,
    DispatchTrackedOrderProof,
    DurableRiskAdmissionAdapter,
    DurableRiskGate,
    IntentAction,
    PermitInvalidError,
    RiskIntent,
    RiskPolicy,
    VerifiedDispatchResolution,
)


class _Authority:
    """Trusted local approval leaf for a Store-contract test."""

    def verify_action(self, command, *, now_ns):
        return CtpDispatchAuthority(
            authority_type="ctp_dispatch_authority.v1",
            command_binding_sha256=command.authority_binding_sha256,
            approval_use_id=command.approval_use_id,
            approval_digest=command.approval_digest,
            source_digest_sha256=hashlib.sha256(b"sourcefake approval").hexdigest(),
            verifier_id="test-ctp-position-authorization",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + 5_000_000_000,
        )


class _TradeVerifier:
    """Synthetic provider evidence over an exact applied native TradeField."""

    def verify_trade_fact(self, _command, trade_fact, source_event, *, now_ns):
        from bt_api_execution import canonical_json

        return CtpVerifiedTradeFactEvidence(
            evidence_type="ctp_verified_trade_fact.v2",
            owner_intent_id=source_event.owner_handle.owner_intent_id,
            source_sequence=source_event.source_sequence,
            ingress_record_digest_sha256=source_event.record_digest_sha256,
            event_id=trade_fact.event_id,
            trade_fact_sha256=hashlib.sha256(
                canonical_json(trade_fact.to_payload()).encode("utf-8")
            ).hexdigest(),
            source_digest_sha256=hashlib.sha256(b"sourcefake verified trade").hexdigest(),
            verifier_id="test-ctp-position-trade-verifier",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + 5_000_000_000,
        )


class _OrderVerifier:
    """Synthetic provider evidence for the Store-correlated order callback."""

    def verify_callback(self, _command, callback, callback_payload, *, now_ns):
        from bt_api_execution import payload_sha256

        native = callback_payload.get("native_fields", {})
        projection = "FILLED" if native.get("OrderStatus") == "0" else "ACKNOWLEDGED"
        return CtpVerifiedCallbackEvidence(
            evidence_type="ctp_verified_callback.v1",
            callback_key=callback,
            callback_payload_sha256=payload_sha256(callback_payload),
            projection_state=projection,
            source_digest_sha256=hashlib.sha256(b"sourcefake verified order").hexdigest(),
            verifier_id="test-ctp-position-order-verifier",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + 5_000_000_000,
        )


class _StoreBackedRiskJournalAuthority:
    """Risk journal leaf that requires durable CTP command and trade facts."""

    def __init__(self, store, scope, command_id):
        self.store = store
        self.scope = scope
        self.command_id = command_id

    @contextmanager
    def dispatch_resolution_guard(self, proof, *, claim):
        command = self.store.read_ctp_dispatch_command(self.scope, self.command_id)
        trade_read = self.store.read_ctp_dispatch_trade_facts(
            self.scope, self.command_id, limit=100
        )
        assert command is not None and command.status == "COMPLETED"
        assert command.reservation_managed_intent_id == claim.intent_id
        assert command.order_ref is not None
        assert trade_read.trades
        assert sum(fact.trade_quantity for fact in trade_read.trades) > 0
        yield VerifiedDispatchResolution(
            scope=claim.scope,
            permit_id=claim.permit_id,
            intent_id=claim.intent_id,
            intent_hash=claim.intent_hash,
            claim_digest=claim.claim_digest,
            proof_sha256=proof.fingerprint,
            journal_revision=max(fact.source_sequence for fact in trade_read.trades),
            journal_record_sha256=trade_read.trades[-1].fact_payload_sha256,
            writer_fence_sha256=hashlib.sha256(
                (command.scope_key + command.command_id).encode("utf-8")
            ).hexdigest(),
        )


@pytest.mark.unit
def test_cancelled_positive_fill_preserves_review_latch_and_unknown_fee(tmp_path):
    scope = ExecutionScope(
        "FAKE", "simulation", "acct_demo", "strategy.cancel-review", "20261021"
    )
    store = SqliteExecutionStore(tmp_path / "cancel-review-latch.sqlite3")
    try:
        lease = store.acquire_or_renew_lease(
            scope, "cancel-review-writer", ttl_ns=30_000_000_000
        )
        target = OrderIntent.limit(
            intent_id="cancel-review-target",
            scope=scope,
            signal_id="signal-cancel-review-target",
            instrument="FAKE-PERP",
            side=Side.BUY,
            quantity=Decimal("2"),
            price=Decimal("100"),
            metadata_version="cancel-review-v1",
        )
        store.admit_intent(target, writer_lease=lease)
        store.activate_intent(target.intent_id, scope, writer_lease=lease)
        _claimed, claimed = store.claim_for_dispatch(
            target.intent_id, scope, writer_lease=lease
        )
        assert claimed is True
        store.record_observation(
            ProviderObservation.accepted(target.intent_id, "provider.cancel-review"),
            scope=scope,
            writer_lease=lease,
        )
        partial = store.record_observation(
            ProviderObservation(
                target.intent_id,
                ExecutionState.PARTIALLY_FILLED,
                provider_order_id="provider.cancel-review",
                filled_quantity=Decimal("0.75"),
                average_price=Decimal("99.5"),
                cumulative_commission=None,
            ),
            scope=scope,
            writer_lease=lease,
        )
        reviewed = store.mark_review_required(
            target.intent_id,
            scope,
            "unknown_fee_requires_review",
            writer_lease=lease,
        )
        assert reviewed.state is ExecutionState.PARTIALLY_FILLED
        assert reviewed.review_required is True
        assert reviewed.unknown_reason is None
        assert reviewed.cumulative_commission is None

        cancel = CancelIntent(
            cancel_id="cancel-review-action",
            scope=scope,
            target_intent_id=target.intent_id,
            provider_order_id="provider.cancel-review",
            metadata_version="cancel-review-v1",
        )
        store.admit_cancel(cancel, writer_lease=lease)
        store.activate_cancel(cancel.cancel_id, scope, "permit.cancel-review", writer_lease=lease)
        _cancel_record, cancel_claimed = store.claim_cancel_for_dispatch(
            cancel.cancel_id, scope, writer_lease=lease
        )
        assert cancel_claimed is True
        store.record_cancel_observation(
            CancelObservation.cancelled(
                cancel.cancel_id, target.intent_id, "provider.cancel-review"
            ),
            scope=scope,
            writer_lease=lease,
        )

        cancelled = store.get(target.intent_id, scope=scope)
        assert cancelled.state is ExecutionState.CANCELLED
        assert cancelled.filled_quantity == partial.filled_quantity == Decimal("0.75")
        assert cancelled.average_price == Decimal("99.5")
        assert cancelled.cumulative_commission is None
        assert cancelled.review_required is True
        assert cancelled.unknown_reason is None
    finally:
        store.close()


def _position_window(
    store,
    scope,
    lease,
    owner,
    adapter,
    binding,
    tags,
    managed_intent_id,
    *,
    positions=(),
    source_sequence_start=None,
    request_id_start=0,
    client=None,
    retain_client_api=False,
    terminal_pointer_present_without_fields=False,
    terminal_missing_response_info=False,
    after_terminal_callback=None,
):
    """Create a genuine Store-sealed query window from applied synthetic callbacks."""

    class _QueryApi:
        def __init__(self, client):
            self.client = client
            self.source_sequence = (
                binding.source_high_watermark
                if source_sequence_start is None
                else source_sequence_start
            )
            self.positions = tuple(positions)

        def ReqQryInvestorPosition(self, _field, request_id):
            response_rows = self.positions + (None,)
            for index, record in enumerate(response_rows):
                self.source_sequence += 1
                is_last = index == len(response_rows) - 1
                if record is None and is_last and terminal_pointer_present_without_fields:
                    # Exercise the Store boundary where the native argument is
                    # present but none of its allowlisted getters are captured.
                    # The synthetic QueryResult remains a genuine empty query
                    # so only the durable callback projection supplies this
                    # malformed terminal pointer.
                    field = SimpleNamespace()
                else:
                    field = None if record is None else SimpleNamespace(**dict(record))
                response_info = (
                    None
                    if is_last and terminal_missing_response_info
                    else SimpleNamespace(ErrorID=0)
                )
                _append_callback(
                    adapter,
                    owner,
                    tags,
                    "OnRspQryInvestorPosition",
                    self.source_sequence,
                    (field, response_info, request_id, is_last),
                    source_generation=binding.connection_generation,
                    phase=CtpTraderCallbackPhase.ACTIVE,
                )
                source_row = store._connection.execute(
                    """
                    SELECT record_payload_json FROM ctp_dispatch_callback_ingress
                    WHERE owner_intent_id = ? AND source_sequence = ?
                    """,
                    (owner.owner_intent_id, self.source_sequence),
                ).fetchone()
                assert source_row is not None
                source_payload = json.loads(source_row["record_payload_json"])
                pointer_projection = source_payload["named_args"][0]
                assert pointer_projection["pointer_is_null"] is (field is None)
                self.client._handle_query_callback(
                    "positions", record, response_info, request_id, is_last
                )
                applied = adapter.apply_next_ingress(
                    callback_verifier=None, trade_fact_verifier=None
                )
                assert applied is not None
            if after_terminal_callback is not None:
                self.source_sequence += 1
                after_terminal_callback(self.source_sequence)
            return 0

    if client is None:
        client = _new_position_query_client(scope, binding, tags)
    client._req_id = request_id_start
    client._connected = True
    client._authentication_state = "authenticated"
    client._login_state = "logged_in"
    client._query_interval = 0.0
    api = getattr(client, "_sourcefake_position_query_api", None)
    if api is None:
        api = _QueryApi(client)
        client._sourcefake_position_query_api = api
    else:
        api.client = client
        api.source_sequence = (
            binding.source_high_watermark
            if source_sequence_start is None
            else source_sequence_start
        )
        api.positions = tuple(positions)
    client._api = api
    # Match the exact source identities already owned by the Store callback
    # session. The API itself is only a synthetic local query leaf.
    client._native_client_epoch = tags.native_client_epoch
    client._native_api_generation = tags.native_api_generation
    client._callback_source_instance_id = tags.source_instance_id
    client._native_api_source_id = tags.native_api_source_id
    spi = getattr(client, "_sourcefake_position_query_spi", None)
    if spi is None:
        spi = SimpleNamespace()
        client._sourcefake_position_query_spi = spi
    spi._native_api = api
    spi._native_spi_source_id = tags.native_spi_source_id
    spi._callback_source_tags = tags
    client._spi = spi

    try:
        fence = store.begin_ctp_position_evidence_window(
            scope,
            managed_intent_id,
            owner_handle=owner,
            writer_lease=lease,
        )
        result = client.query_positions_result(timeout=1.0)
        assert result.complete is True
        query_source = result.query_source
        assert query_source is not None
        session_scope = client.get_query_session_scope()
        evidence = build_ctp_position_evidence(
            result,
            session_state=session_scope,
            now_utc=datetime.now(timezone.utc),
            expires_at_utc=query_source.completed_at_utc + timedelta(seconds=5),
            monotonic_now=time.monotonic(),
            monotonic_expires_at=query_source.completed_monotonic + 5.0,
        )
        handle = store.finalize_ctp_position_evidence_window(
            scope,
            fence,
            evidence,
            owner_handle=owner,
            writer_lease=lease,
        )
        return handle
    finally:
        if not retain_client_api:
            client._api = None
            client._spi = None


def _new_position_query_client(scope, binding, tags):
    client = TraderClient(
        "tcp://offline.invalid:0",
        "broker",
        "user",
        "",
        auto_settlement_confirm=False,
    )
    client._connected = True
    client._authentication_state = "authenticated"
    client._login_state = "logged_in"
    client._trading_day = scope.trading_day
    client._connection_generation = binding.connection_generation
    client._login_identity_observation = _TraderLoginIdentityObservation(
        _seal=_TRADER_LOGIN_IDENTITY_SEAL,
        broker_id="broker",
        user_id="user",
        trading_day=scope.trading_day,
        connection_generation=binding.connection_generation,
        request_id=17,
    )
    client._query_interval = 0.0
    client._native_client_epoch = tags.native_client_epoch
    client._native_api_generation = tags.native_api_generation
    client._callback_source_instance_id = tags.source_instance_id
    client._native_api_source_id = tags.native_api_source_id
    return client


def _intent(
    scope,
    intent_id,
    *,
    side=Side.BUY,
    position_effect=PositionEffect.OPEN,
    reduce_only=False,
    quantity=Decimal("1"),
    price=Decimal("100"),
    position_side="LONG",
    position_bucket="ALL",
):
    return OrderIntent(
        intent_id=intent_id,
        scope=scope,
        signal_id="signal-" + intent_id,
        instrument="rb2710",
        side=side,
        position_effect=position_effect,
        order_type=OrderType.LIMIT,
        quantity=quantity,
        price=price,
        reduce_only=reduce_only,
        metadata_version="position-sourcefake-v1",
        position_side=position_side,
        position_bucket=position_bucket,
    )


def _admit_pending_dispatch(
    store, scope, lease, intent, *, permit_reference=None, basic_reference=None
):
    if permit_reference is None:
        permit_reference = "permit-" + intent.intent_id
    if basic_reference is None:
        basic_reference = hashlib.sha256(
            ("basic-" + intent.intent_id).encode("ascii")
        ).hexdigest()[:32]
    store.admit_intent(intent, writer_lease=lease)
    return store.activate_intent(
        intent.intent_id,
        scope,
        permit_reference=permit_reference,
        basic_admission_reference=basic_reference,
        admission_context_sha256=hashlib.sha256(
            ("context-" + intent.intent_id).encode("ascii")
        ).hexdigest(),
        writer_lease=lease,
    )


def _risk_scope(scope):
    return AccountScope(
        provider=scope.provider,
        account_id=scope.account_ref,
        environment=scope.environment,
    )


def _risk_intent(order_intent, *, recovery=False):
    is_recovery = recovery or order_intent.position_effect is not PositionEffect.OPEN
    return RiskIntent(
        intent_id=order_intent.intent_id,
        scope=_risk_scope(order_intent.scope),
        action=IntentAction.REDUCE if is_recovery else IntentAction.INCREASE,
        notional=(
            Decimal("0")
            if is_recovery
            else order_intent.quantity * order_intent.price
        ),
        payload_fingerprint=order_intent.fingerprint,
        strategy_id=order_intent.scope.strategy_id,
        allocation_version="ctp-runtime-allocation-v1",
        instrument=order_intent.instrument,
        quantity=order_intent.quantity,
        quantity_unit="contracts",
        notional_unit="USD",
        position_side=order_intent.position_side,
        position_bucket=order_intent.position_bucket,
    )


def _make_recovery_child_source(parent, *, child_id="recovery-child"):
    reservation = _reserve(
        parent.store,
        parent.scope,
        parent.lease,
        parent.binding,
        child_id,
    )
    trade = parent.trade_facts.trades[-1]
    positions = (
        {
            "BrokerID": "broker",
            "InvestorID": "user",
            "InstrumentID": parent.parent_intent.instrument,
            "ExchangeID": "SHFE",
            "PosiDirection": "2",
            "HedgeFlag": "1",
            "PositionDate": "1",
            "TradingDay": parent.scope.trading_day,
            "Position": 1,
            "TodayPosition": 1,
            "YdPosition": 0,
            "LongFrozen": 0,
            "ShortFrozen": 0,
        },
    )
    window = _position_window(
        parent.store,
        parent.scope,
        parent.lease,
        parent.owner,
        parent.callback_store,
        parent.binding,
        parent.tags,
        child_id,
        positions=positions,
        source_sequence_start=trade.source_sequence,
        request_id_start=1,
        client=parent.query_client,
        retain_client_api=True,
    )
    facts = parent.store.read_ctp_recovery_close_source_facts(
        parent.scope,
        parent.parent_intent.intent_id,
        window,
        owner_handle=parent.owner,
        writer_lease=parent.lease,
        parent_permit=parent.parent_permit,
    )
    return reservation, window, facts


def _recovery_order_intent(parent, facts, *, side=Side.SELL, effect=None, bucket=None):
    selected_bucket = facts.position_bucket if bucket is None else bucket
    if effect is None:
        effect = {
            "TODAY": PositionEffect.CLOSE_TODAY,
            "YESTERDAY": PositionEffect.CLOSE_YESTERDAY,
            "ALL": PositionEffect.CLOSE,
        }[selected_bucket]
    return _intent(
        parent.scope,
        facts.recovery_intent_id,
        side=side,
        position_effect=effect,
        reduce_only=True,
        quantity=facts.attributable_position_quantity,
        position_side=facts.position_side,
        position_bucket=selected_bucket,
    )


def _admit_recovery_child(parent, child_intent, facts, window):
    child_risk_intent = parent.risk_adapter._map_order(child_intent)
    proof = parent.store.issue_ctp_recovery_close_source_proof(
        parent.scope,
        parent.parent_intent.intent_id,
        child_risk_intent,
        window,
        owner_handle=parent.owner,
        writer_lease=parent.lease,
        parent_permit=parent.parent_permit,
        parent_risk_intent=parent.parent_risk_intent,
    )
    allocation = parent.risk_gate.reserve_recovery_reduction(
        child_risk_intent,
        parent_permit_id=parent.parent_permit.permit_id,
        source_proof=proof,
    )
    basic_reference = hashlib.sha256(
        ("basic-admission:" + child_intent.intent_id).encode("ascii")
    ).hexdigest()[:32]
    parent.store.admit_intent(child_intent, writer_lease=parent.lease)
    parent.store.activate_intent(
        child_intent.intent_id,
        parent.scope,
        permit_reference=allocation.permit.permit_id,
        basic_admission_reference=basic_reference,
        admission_context_sha256=hashlib.sha256(
            ("context:" + child_intent.intent_id).encode("ascii")
        ).hexdigest(),
        writer_lease=parent.lease,
    )
    authorization = parent.store.authorize_ctp_recovery_close_dispatch(
        parent.scope,
        parent.parent_intent.intent_id,
        child_intent.intent_id,
        window,
        owner_handle=parent.owner,
        writer_lease=parent.lease,
        recovery_allocation=allocation,
        recovery_allocation_verifier=parent.risk_gate.verify_recovery_reduction_allocation,
        recovery_intent=child_risk_intent,
    )
    return child_risk_intent, proof, allocation, authorization


def _stage_recovery_child(
    parent,
    child_intent,
    reservation,
    authorization,
    *,
    command_id,
    scope=None,
    managed_intent_id=None,
    order_ref=None,
    session_binding=None,
    session_generation_id=None,
    direction=None,
    offset_flag=None,
    queue=True,
):
    receipt_id = hashlib.sha256(command_id.encode("ascii")).hexdigest()[:32]
    request_offset = (
        {
            PositionEffect.CLOSE: "1",
            PositionEffect.CLOSE_TODAY: "3",
            PositionEffect.CLOSE_YESTERDAY: "4",
        }[child_intent.position_effect]
        if offset_flag is None
        else offset_flag
    )
    request_order_ref = reservation.order_ref if order_ref is None else order_ref
    request = {
        "BrokerID": "broker",
        "InvestorID": "user",
        "UserID": "user",
        "InstrumentID": child_intent.instrument,
        "ExchangeID": "SHFE",
        "OrderRef": request_order_ref,
        "Direction": (
            ("0" if child_intent.side is Side.BUY else "1")
            if direction is None
            else direction
        ),
        "CombOffsetFlag": request_offset,
        "CombHedgeFlag": "1",
        "LimitPrice": 100.0,
        "VolumeTotalOriginal": int(child_intent.quantity),
        "OrderPriceType": "2",
        "TimeCondition": "3",
    }
    call_scope = parent.scope if scope is None else scope
    command = parent.store.stage_ctp_dispatch_command(
        call_scope,
        command_id,
        "SUBMIT",
        request,
        approval_use_id="approval-" + command_id,
        approval_digest=hashlib.sha256(
            ("approval-" + command_id).encode("ascii")
        ).hexdigest(),
        session_binding=(
            _session_payload(parent.binding)
            if session_binding is None
            else session_binding
        ),
        writer_lease=parent.lease,
        managed_intent_id=(
            child_intent.intent_id
            if managed_intent_id is None
            else managed_intent_id
        ),
        order_ref=request_order_ref,
        managed_action_id=(
            child_intent.intent_id
            if managed_intent_id is None
            else managed_intent_id
        ),
        session_generation_id=(
            parent.binding.session_generation_id
            if session_generation_id is None
            else session_generation_id
        ),
        dispatch_front_id=parent.binding.dispatch_front_id,
        dispatch_session_id=parent.binding.dispatch_session_id,
        native_request_id=202,
        local_queue_receipt_id=receipt_id,
        recovery_close_authorization=authorization,
    )
    if queue:
        parent.store.record_ctp_dispatch_queue_receipt(
            call_scope,
            command.command_id,
            {
                "kind": "command_receipt",
                "command": "submit",
                "receipt_id": receipt_id,
                "queued": True,
            },
            writer_lease=parent.lease,
            recovery_close_authorization=authorization,
        )
    return command, receipt_id


def _new_risk_gate(path, *, execution_authority, recovery_authority=None):
    gate = DurableRiskGate(
        path,
        RiskPolicy(
            policy_id="ctp-position-recovery-sourcefake",
            max_increase_notional=Decimal("1000"),
            max_increase_count=8,
            require_strategy_allocation=True,
            notional_unit="USD",
        ),
        execution_journal_authority=execution_authority,
        recovery_close_source_authority=recovery_authority,
    )
    return gate


def _authorize_baseline(store, scope, lease, owner, intent, window):
    return store.authorize_ctp_position_baseline_submit(
        scope,
        intent.intent_id,
        window,
        owner_handle=owner,
        writer_lease=lease,
    )


def _stage_baseline_submit(
    store,
    scope,
    lease,
    binding,
    reservation,
    authorization,
    *,
    command_id="baseline-submit-command",
    order_ref=None,
    session_generation_id=None,
    session_binding=None,
):
    receipt_id = hashlib.sha256(command_id.encode("ascii")).hexdigest()[:32]
    generation = (
        binding.session_generation_id
        if session_generation_id is None
        else session_generation_id
    )
    request = {
        "BrokerID": "broker",
        "InvestorID": "user",
        "UserID": "user",
        "InstrumentID": "rb2710",
        "ExchangeID": "SHFE",
        "OrderRef": reservation.order_ref if order_ref is None else order_ref,
        "Direction": "0",
        "CombOffsetFlag": "0",
        "CombHedgeFlag": "1",
        "LimitPrice": 100.0,
        "VolumeTotalOriginal": 1,
        "OrderPriceType": "2",
        "TimeCondition": "3",
    }
    command = store.stage_ctp_dispatch_command(
        scope,
        command_id,
        "SUBMIT",
        request,
        approval_use_id="approval-" + command_id,
        approval_digest=hashlib.sha256(
            ("approval-" + command_id).encode("ascii")
        ).hexdigest(),
        session_binding=(
            _session_payload(binding) if session_binding is None else session_binding
        ),
        writer_lease=lease,
        managed_intent_id=reservation.managed_intent_id,
        order_ref=reservation.order_ref if order_ref is None else order_ref,
        managed_action_id=reservation.managed_intent_id,
        session_generation_id=generation,
        dispatch_front_id=binding.dispatch_front_id,
        dispatch_session_id=binding.dispatch_session_id,
        native_request_id=101,
        local_queue_receipt_id=receipt_id,
        position_baseline_authorization=authorization,
    )
    return command, receipt_id


def _queue_baseline_submit(store, scope, lease, command, receipt_id, authorization):
    return store.record_ctp_dispatch_queue_receipt(
        scope,
        command.command_id,
        {
            "kind": "command_receipt",
            "command": "submit",
            "receipt_id": receipt_id,
            "queued": True,
        },
        writer_lease=lease,
        position_baseline_authorization=authorization,
    )


def _claim_and_complete_baseline_submit(
    store,
    scope,
    lease,
    owner,
    command,
    receipt_id,
    authorization,
):
    claim = store.claim_ctp_dispatch_command_for_session(
        scope,
        command.command_id,
        owner_handle=owner,
        writer_lease=lease,
        authority_verifier=_Authority(),
        required_local_queue_receipt_id=receipt_id,
        position_baseline_authorization=authorization,
    )
    assert claim is not None and claim.command.status == "CLAIMED"
    native_binding = store.verify_ctp_managed_native_call_binding(
        scope, owner, claim.binding, writer_lease=lease
    )
    assert native_binding is claim.binding
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
        native_receipt_payload={"request_id": 101, "queue_code": 0},
        correlation_key=command.correlation_key,
        local_queue_receipt_id=receipt_id,
    )
    store.complete_ctp_dispatch_command_for_session(
        scope, receipt, owner_handle=owner, writer_lease=lease
    )
    return claim


def _append_parent_order_and_trade(
    store,
    scope,
    owner,
    adapter,
    binding,
    tags,
    reservation,
    *,
    start_sequence,
    trade_id="sourcefake-parent-trade-1",
):
    order_fields = {
        "BrokerID": "broker",
        "InvestorID": "user",
        "UserID": "user",
        "InstrumentID": "rb2710",
        "RequestID": 101,
        "OrderRef": reservation.order_ref,
        "ExchangeID": "SHFE",
        "OrderSysID": "sys-" + reservation.order_ref,
        "FrontID": binding.dispatch_front_id,
        "SessionID": binding.dispatch_session_id,
        "TradingDay": scope.trading_day,
        "OrderStatus": "0",
        "OrderSubmitStatus": "3",
        "VolumeTraded": 1,
        "VolumeTotal": 0,
        "NotifySequence": 1,
        "SequenceNo": 1,
    }
    _append_callback(
        adapter,
        owner,
        tags,
        "OnRtnOrder",
        start_sequence,
        (SimpleNamespace(**order_fields),),
        source_generation=binding.connection_generation,
        phase=CtpTraderCallbackPhase.ACTIVE,
    )
    order_result = adapter.apply_next_ingress(
        callback_verifier=_OrderVerifier(), trade_fact_verifier=None
    )
    assert order_result is not None
    assert order_result.projection_state == "FILLED"

    trade_fields = {
        "BrokerID": "broker",
        "InvestorID": "user",
        "UserID": "user",
        "InstrumentID": "rb2710",
        "OrderRef": reservation.order_ref,
        "ExchangeID": "SHFE",
        "TradeID": trade_id,
        "OrderSysID": "sys-" + reservation.order_ref,
        "TradingDay": scope.trading_day,
        "Direction": "0",
        "OffsetFlag": "0",
        "HedgeFlag": "1",
        "Price": 99.0,
        "Volume": 1,
        "TradeDate": scope.trading_day,
        "TradeTime": "09:31:00",
        "SequenceNo": 1,
        "BrokerOrderSeq": 1,
        "TradeSource": "0",
    }
    _append_callback(
        adapter,
        owner,
        tags,
        "OnRtnTrade",
        start_sequence + 1,
        (SimpleNamespace(**trade_fields),),
        source_generation=binding.connection_generation,
        phase=CtpTraderCallbackPhase.ACTIVE,
    )
    trade_result = adapter.apply_next_ingress(
        callback_verifier=None, trade_fact_verifier=_TradeVerifier()
    )
    assert trade_result is not None and trade_result.trade_quantity == 1
    return trade_result


def _prepare_risk_parent(gate, order_intent, risk_order_adapter):
    risk_intent = risk_order_adapter._map_order(order_intent)
    gate.set_strategy_allocation(
        risk_intent.scope,
        risk_intent.strategy_id,
        risk_intent.allocation_version,
        max_notional=Decimal("10000"),
        notional_unit=risk_intent.notional_unit,
    )
    permit = gate.reserve(risk_intent)
    return risk_intent, permit


def _create_tracked_parent_source(tmp_path, *, name="recovery-parent"):
    """Create a real Store parent with a tracked fill and unknown commission.

    The mapper, native callback leaves, and approval verifier are deterministic
    test fixtures. Execution records, callback application, authorization
    consumption, and both SQLite journals are production implementations.
    """

    store, scope, _family_owner, lease, owner, callback_store, binding, tags = (
        _new_store_and_session(tmp_path, name + "-execution.sqlite3")
    )
    parent_intent = _intent(scope, name + "-open")
    command_id = name + "-submit"
    recovery_authority = store.bind_ctp_recovery_close_source_authority(
        scope, owner_handle=owner, writer_lease=lease
    )
    risk_authority = _StoreBackedRiskJournalAuthority(store, scope, command_id)
    risk_gate = _new_risk_gate(
        tmp_path / (name + "-risk.sqlite3"),
        execution_authority=risk_authority,
        recovery_authority=recovery_authority,
    )
    risk_adapter = DurableRiskAdmissionAdapter(risk_gate, _risk_intent)
    parent_risk_intent, parent_permit = _prepare_risk_parent(
        risk_gate, parent_intent, risk_adapter
    )

    basic_reference = hashlib.sha256(
        ("basic-admission:" + parent_intent.intent_id).encode("ascii")
    ).hexdigest()[:32]
    _admit_pending_dispatch(
        store,
        scope,
        lease,
        parent_intent,
        permit_reference=parent_permit.permit_id,
        basic_reference=basic_reference,
    )
    order_ref_reservation = _reserve(
        store, scope, lease, binding, parent_intent.intent_id
    )
    query_client = _new_position_query_client(scope, binding, tags)
    baseline_window = _position_window(
        store,
        scope,
        lease,
        owner,
        callback_store,
        binding,
        tags,
        parent_intent.intent_id,
        client=query_client,
        retain_client_api=True,
    )
    assert query_client.is_read_only_ready, query_client.get_session_state()
    baseline_authorization = _authorize_baseline(
        store, scope, lease, owner, parent_intent, baseline_window
    )
    command, local_receipt_id = _stage_baseline_submit(
        store,
        scope,
        lease,
        binding,
        order_ref_reservation,
        baseline_authorization,
        command_id=command_id,
    )
    _queue_baseline_submit(
        store, scope, lease, command, local_receipt_id, baseline_authorization
    )
    risk_gate.claim_for_dispatch(parent_permit.permit_id, parent_risk_intent)
    parent_claim = risk_gate.dispatch_claim_binding(parent_permit.permit_id)
    risk_gate.settle(parent_permit.permit_id)
    _claim_and_complete_baseline_submit(
        store,
        scope,
        lease,
        owner,
        command,
        local_receipt_id,
        baseline_authorization,
    )
    execution_claim, execution_claimed = store.claim_for_dispatch(
        parent_intent.intent_id, scope, writer_lease=lease
    )
    assert execution_claimed is True
    assert execution_claim.dispatch_attempts == 1
    trade_callback = _append_parent_order_and_trade(
        store,
        scope,
        owner,
        callback_store,
        binding,
        tags,
        order_ref_reservation,
        start_sequence=baseline_authorization.end_source_sequence + 1,
        trade_id=name + "-trade-1",
    )
    trade_facts = store.read_ctp_dispatch_trade_facts(
        scope, command.command_id, limit=100
    )
    assert trade_facts.trades
    assert trade_facts.trades[0].trade_quantity > 0
    assert trade_facts.trades[0].cumulative_commission is None
    stored_parent = store.record_observation(
        ProviderObservation(
            intent_id=parent_intent.intent_id,
            state=ExecutionState.FILLED,
            provider_order_id="ctp-order-ref:" + order_ref_reservation.order_ref,
            filled_quantity=Decimal(trade_facts.trades[0].trade_quantity),
            average_price=trade_facts.trades[0].trade_price,
            cumulative_commission=None,
        ),
        scope=scope,
        writer_lease=lease,
    )
    assert stored_parent.state is ExecutionState.FILLED
    reviewed_parent = store.mark_review_required(
        parent_intent.intent_id,
        scope,
        "unknown_fee_requires_review",
        writer_lease=lease,
    )
    tracked_proof, _ = _resolve_store_tracked_risk_parent(
        risk_gate,
        parent_risk_intent,
        parent_permit,
        parent_claim,
        command,
        trade_facts,
    )
    allocation = risk_gate.get_strategy_allocation(
        parent_risk_intent.scope, parent_risk_intent.strategy_id
    )
    return SimpleNamespace(
        store=store,
        scope=scope,
        lease=lease,
        owner=owner,
        callback_store=callback_store,
        binding=binding,
        tags=tags,
        query_client=query_client,
        parent_intent=parent_intent,
        parent_risk_intent=parent_risk_intent,
        parent_permit=parent_permit,
        parent_claim=parent_claim,
        parent_order_ref=order_ref_reservation,
        baseline_window=baseline_window,
        baseline_authorization=baseline_authorization,
        command=command,
        trade_callback=trade_callback,
        trade_facts=trade_facts,
        reviewed_parent=reviewed_parent,
        tracked_proof=tracked_proof,
        risk_gate=risk_gate,
        risk_adapter=risk_adapter,
        risk_allocation=allocation,
    )


def _resolve_store_tracked_risk_parent(
    gate,
    risk_order_intent,
    parent_permit,
    risk_claim,
    command,
    trade_read,
):
    from bt_api_risk import DispatchTrackedOrderProof

    trades = trade_read.trades
    assert trades and sum(item.trade_quantity for item in trades) > 0
    proof = DispatchTrackedOrderProof(
        scope=risk_claim.scope,
        permit_id=parent_permit.permit_id,
        intent_id=risk_order_intent.intent_id,
        intent_hash=risk_claim.intent_hash,
        cause_id=risk_claim.cause_id,
        claim_digest=risk_claim.claim_digest,
        evidence_class=DispatchEvidenceClass.NATIVE_PROVIDER_JOURNAL,
        dispatch_attempt_count=1,
        journal_revision=max(item.source_sequence for item in trades),
        journal_record_sha256=trades[-1].fact_payload_sha256,
        reconciliation_evidence_sha256=hashlib.sha256(
            "|".join(item.fact_payload_sha256 for item in trades).encode("ascii")
        ).hexdigest(),
        writer_fence_sha256=hashlib.sha256(
            (command.scope_key + command.command_id).encode("utf-8")
        ).hexdigest(),
        provider_order_id="ctp-order-ref:" + str(command.order_ref),
        accepted_request_sha256=command.request_payload_sha256,
        exposure_reservation_id=str(command.order_ref),
        exposure_reservation_sha256=hashlib.sha256(
            (
                command.reservation_managed_intent_id + str(command.order_ref)
            ).encode("utf-8")
        ).hexdigest(),
    )
    return gate.resolve_dispatch_freeze(proof), proof


def _authorization_use_count(store):
    return int(
        store._connection.execute(
            "SELECT COUNT(*) FROM ctp_position_baseline_submit_authorization_uses"
        ).fetchone()[0]
    )


def _recovery_authorization_use_count(store):
    return int(
        store._connection.execute(
            "SELECT COUNT(*) FROM ctp_position_recovery_close_authorization_uses"
        ).fetchone()[0]
    )


def _query_observed(store, owner):
    row = store._connection.execute(
        "SELECT economic_query_observed FROM ctp_dispatch_callback_session_owners "
        "WHERE owner_intent_id = ?",
        (owner.owner_intent_id,),
    ).fetchone()
    assert row is not None
    return int(row["economic_query_observed"])


@pytest.mark.unit
def test_position_empty_terminal_requires_absent_pointer(tmp_path):
    control = _new_store_and_session(
        tmp_path, "ctp-position-empty-terminal-control.sqlite3"
    )
    control_store, control_scope, _family, control_lease, control_owner, control_adapter, control_binding, control_tags = control
    try:
        control_reservation = _reserve(
            control_store,
            control_scope,
            control_lease,
            control_binding,
            "empty-terminal-control",
        )
        handle = _position_window(
            control_store,
            control_scope,
            control_lease,
            control_owner,
            control_adapter,
            control_binding,
            control_tags,
            control_reservation.managed_intent_id,
        )
        window = control_store.read_ctp_position_evidence_window(
            control_scope,
            handle,
            owner_handle=control_owner,
            writer_lease=control_lease,
        )
        assert window.positions == ()
    finally:
        control_store.close()

    malformed = _new_store_and_session(
        tmp_path, "ctp-position-empty-terminal-present-pointer.sqlite3"
    )
    store, scope, _family, lease, owner, adapter, binding, tags = malformed
    try:
        reservation = _reserve(
            store, scope, lease, binding, "empty-terminal-present-pointer"
        )
        with pytest.raises(ContractValidationError):
            _position_window(
                store,
                scope,
                lease,
                owner,
                adapter,
                binding,
                tags,
                reservation.managed_intent_id,
                terminal_pointer_present_without_fields=True,
            )
    finally:
        store.close()


@pytest.mark.unit
def test_position_null_terminal_without_response_info_is_rejected(tmp_path):
    store, scope, _family, lease, owner, adapter, binding, tags = _new_store_and_session(
        tmp_path, "ctp-position-null-terminal-missing-rsp-info.sqlite3"
    )
    try:
        reservation = _reserve(
            store, scope, lease, binding, "position-null-terminal-missing-rsp-info"
        )
        with pytest.raises(ContractValidationError):
            _position_window(
                store,
                scope,
                lease,
                owner,
                adapter,
                binding,
                tags,
                reservation.managed_intent_id,
                terminal_missing_response_info=True,
            )
    finally:
        store.close()


@pytest.mark.unit
def test_position_window_rejects_applied_callback_after_terminal_boundary(tmp_path):
    store, scope, _family, lease, owner, adapter, binding, tags = _new_store_and_session(
        tmp_path, "ctp-position-trailing-source-callback.sqlite3"
    )
    try:
        reservation = _reserve(
            store, scope, lease, binding, "position-trailing-source-callback"
        )
        trailing_applied = False

        def append_trailing_informational_callback(sequence):
            nonlocal trailing_applied
            _append_callback(
                adapter,
                owner,
                tags,
                "OnRtnInstrumentStatus",
                sequence,
                (
                    SimpleNamespace(
                        ExchangeID="SHFE",
                        ExchangeInstID="rb2710",
                        InstrumentID="rb2710",
                        InstrumentStatus="0",
                    ),
                ),
                source_generation=binding.connection_generation,
                phase=CtpTraderCallbackPhase.ACTIVE,
            )
            applied = adapter.apply_next_ingress(
                callback_verifier=None, trade_fact_verifier=None
            )
            assert applied is not None
            assert applied.callback_name == "OnRtnInstrumentStatus"
            trailing_applied = True

        with pytest.raises(InvalidStateTransition):
            _position_window(
                store,
                scope,
                lease,
                owner,
                adapter,
                binding,
                tags,
                reservation.managed_intent_id,
                after_terminal_callback=append_trailing_informational_callback,
            )
        assert trailing_applied is True
    finally:
        store.close()


@pytest.mark.unit
def test_baseline_claim_rejects_query_after_bound_position_window(tmp_path):
    store, scope, _family, lease, owner, adapter, binding, tags = _new_store_and_session(
        tmp_path, "ctp-position-baseline-query-high-water.sqlite3"
    )
    try:
        intent = _intent(scope, "baseline-query-high-water-intent")
        _admit_pending_dispatch(store, scope, lease, intent)
        reservation = _reserve(store, scope, lease, binding, intent.intent_id)
        query_client = _new_position_query_client(scope, binding, tags)
        baseline_window = _position_window(
            store,
            scope,
            lease,
            owner,
            adapter,
            binding,
            tags,
            intent.intent_id,
            client=query_client,
            retain_client_api=True,
        )
        authorization = _authorize_baseline(
            store, scope, lease, owner, intent, baseline_window
        )
        command, receipt_id = _stage_baseline_submit(
            store,
            scope,
            lease,
            binding,
            reservation,
            authorization,
            command_id="baseline-query-high-water-submit",
        )
        assert command.status == "READY"
        _queue_baseline_submit(store, scope, lease, command, receipt_id, authorization)
        assert _authorization_use_count(store) == 0

        api = query_client._sourcefake_position_query_api
        source_sequence = api.source_sequence + 1
        request_id = query_client._req_id + 1
        try:
            _append_callback(
                adapter,
                owner,
                tags,
                "OnRspQryInvestorPosition",
                source_sequence,
                (
                    SimpleNamespace(
                        InstrumentID="rb2710",
                        ExchangeID="SHFE",
                        TradingDay=scope.trading_day,
                        PositionDate="1",
                        YdPosition=0,
                        Position=0,
                        TodayPosition=0,
                        PosiDirection="2",
                        HedgeFlag="1",
                    ),
                    SimpleNamespace(ErrorID=0),
                    request_id,
                    True,
                ),
                source_generation=binding.connection_generation,
                phase=CtpTraderCallbackPhase.ACTIVE,
            )
        except (ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable):
            # Rejecting the query at durable ingress after the command binds
            # its baseline authorization is fail-closed.
            assert _authorization_use_count(store) == 0
            return

        api.source_sequence = source_sequence
        ingress_row = store._connection.execute(
            """
            SELECT record_payload_json FROM ctp_dispatch_callback_ingress
            WHERE owner_intent_id = ? AND source_sequence = ?
            """,
            (owner.owner_intent_id, source_sequence),
        ).fetchone()
        assert ingress_row is not None
        ingress_payload = json.loads(ingress_row["record_payload_json"])
        assert ingress_payload["callback_class"] == "AUDIT_QUERY"
        applied = adapter.apply_next_ingress(
            callback_verifier=None, trade_fact_verifier=None
        )
        if applied is not None:
            assert applied.callback_name == "OnRspQryInvestorPosition"
            assert applied.callback_class == "AUDIT_QUERY"
            assert applied.economic_query_observed is True

        # If the new query source was accepted, the original baseline auth is
        # stale at its high-water mark and may not claim native dispatch.
        with pytest.raises(
            (ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable)
        ):
            store.claim_ctp_dispatch_command_for_session(
                scope,
                command.command_id,
                owner_handle=owner,
                writer_lease=lease,
                authority_verifier=_Authority(),
                required_local_queue_receipt_id=receipt_id,
                position_baseline_authorization=authorization,
            )
        assert _authorization_use_count(store) == 0
    finally:
        store.close()


@pytest.mark.unit
def test_baseline_authority_is_one_use_at_ready_to_claim_and_keeps_query_latched(tmp_path):
    store, scope, _family_owner, lease, owner, adapter, binding, tags = _new_store_and_session(
        tmp_path, "ctp-position-baseline.sqlite3"
    )
    try:
        intent = _intent(scope, "baseline-intent")
        pending = _admit_pending_dispatch(store, scope, lease, intent)
        reservation = _reserve(store, scope, lease, binding, intent.intent_id)
        window = _position_window(
            store, scope, lease, owner, adapter, binding, tags, intent.intent_id
        )
        assert _query_observed(store, owner) == 1

        authorization = _authorize_baseline(store, scope, lease, owner, intent, window)
        assert authorization.managed_intent_id == pending.intent_id
        assert authorization.order_ref == reservation.order_ref
        assert authorization.scope_key == scope.key
        assert authorization.session_generation_id == binding.session_generation_id
        assert authorization.connection_generation == binding.connection_generation

        command, receipt_id = _stage_baseline_submit(
            store, scope, lease, binding, reservation, authorization
        )
        assert command.status == "READY"
        _queue_baseline_submit(store, scope, lease, command, receipt_id, authorization)
        assert _authorization_use_count(store) == 0
        assert _query_observed(store, owner) == 1

        def claim_once(_attempt):
            try:
                return store.claim_ctp_dispatch_command_for_session(
                    scope,
                    command.command_id,
                    owner_handle=owner,
                    writer_lease=lease,
                    authority_verifier=_Authority(),
                    required_local_queue_receipt_id=receipt_id,
                    position_baseline_authorization=authorization,
                )
            except (ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable):
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            claims = list(pool.map(claim_once, (1, 2)))
        successful_claims = [claim for claim in claims if claim is not None]
        assert len(successful_claims) == 1
        claim = successful_claims[0]
        assert claim.command.status == "CLAIMED"
        assert _authorization_use_count(store) == 1
        assert _query_observed(store, owner) == 1

        # The baseline waiver follows this exact one-use native-call binding;
        # consuming it does not reset the durable economic-query latch.
        native_binding = store.verify_ctp_managed_native_call_binding(
            scope,
            owner,
            claim.binding,
            writer_lease=lease,
        )
        assert native_binding is claim.binding
        assert _authorization_use_count(store) == 1
        assert _query_observed(store, owner) == 1

        assert claims.count(None) == 1
        assert claim.command.command_id == command.command_id
        assert _authorization_use_count(store) == 1
        assert _query_observed(store, owner) == 1
    finally:
        store.close()


@pytest.mark.unit
def test_baseline_authority_rejects_wrong_intent_scope_reference_and_generation(tmp_path):
    store, scope, _family_owner, lease, owner, adapter, binding, tags = _new_store_and_session(
        tmp_path, "ctp-position-baseline-bindings.sqlite3"
    )
    try:
        intent = _intent(scope, "baseline-bindings-intent")
        _admit_pending_dispatch(store, scope, lease, intent)
        reservation = _reserve(store, scope, lease, binding, intent.intent_id)
        window = _position_window(
            store, scope, lease, owner, adapter, binding, tags, intent.intent_id
        )
        authorization = _authorize_baseline(store, scope, lease, owner, intent, window)

        with pytest.raises(
            (ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable)
        ):
            store.authorize_ctp_position_baseline_submit(
                scope,
                "different-intent",
                window,
                owner_handle=owner,
                writer_lease=lease,
            )

        wrong_scope = type(scope)(
            scope.provider,
            scope.environment,
            scope.account_ref,
            "strategy.other",
            scope.trading_day,
        )
        with pytest.raises(
            (ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable)
        ):
            store.authorize_ctp_position_baseline_submit(
                wrong_scope,
                intent.intent_id,
                window,
                owner_handle=owner,
                writer_lease=lease,
            )

        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            _stage_baseline_submit(
                store,
                scope,
                lease,
                binding,
                reservation,
                authorization,
                command_id="baseline-wrong-order-ref",
                order_ref="999999999999",
            )

        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            _stage_baseline_submit(
                store,
                scope,
                lease,
                binding,
                reservation,
                authorization,
                command_id="baseline-wrong-generation",
                session_generation_id="different-session-generation",
            )

        wrong_session = _session_payload(binding)
        wrong_session["source_tags"] = dict(wrong_session["source_tags"])
        wrong_session["source_tags"]["source_instance_id"] = "other-source-instance"
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            _stage_baseline_submit(
                store,
                scope,
                lease,
                binding,
                reservation,
                authorization,
                command_id="baseline-wrong-session",
                session_binding=wrong_session,
            )

        assert _authorization_use_count(store) == 0
        assert store.read_ctp_dispatch_command(scope, "baseline-wrong-order-ref") is None
        assert store.read_ctp_dispatch_command(scope, "baseline-wrong-generation") is None
        assert store.read_ctp_dispatch_command(scope, "baseline-wrong-session") is None
    finally:
        store.close()


@pytest.mark.unit
def test_expired_baseline_authority_cannot_stage_a_ready_command(tmp_path, monkeypatch):
    store, scope, _family_owner, lease, owner, adapter, binding, tags = _new_store_and_session(
        tmp_path, "ctp-position-baseline-expired.sqlite3"
    )
    try:
        intent = _intent(scope, "baseline-expired-intent")
        _admit_pending_dispatch(store, scope, lease, intent)
        reservation = _reserve(store, scope, lease, binding, intent.intent_id)
        window = _position_window(
            store, scope, lease, owner, adapter, binding, tags, intent.intent_id
        )
        authorization = _authorize_baseline(store, scope, lease, owner, intent, window)
        real_time = time
        monkeypatch.setattr(
            execution_store_module,
            "time",
            SimpleNamespace(
                time_ns=lambda: authorization.expires_at_ns + 1,
                monotonic_ns=real_time.monotonic_ns,
                time=real_time.time,
                sleep=real_time.sleep,
            ),
        )
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            _stage_baseline_submit(
                store,
                scope,
                lease,
                binding,
                reservation,
                authorization,
                command_id="baseline-expired-command",
            )
        assert store.read_ctp_dispatch_command(scope, "baseline-expired-command") is None
        assert _authorization_use_count(store) == 0
    finally:
        store.close()


@pytest.mark.unit
def test_tracked_parent_keeps_unknown_fee_and_full_increase_reservation(tmp_path):
    parent = _create_tracked_parent_source(tmp_path)
    try:
        row = parent.store.get(parent.parent_intent.intent_id, scope=parent.scope)
        assert row.state is ExecutionState.FILLED
        assert row.filled_quantity == parent.parent_intent.quantity
        assert row.cumulative_commission is None
        assert row.review_required is True

        assert len(parent.trade_facts.trades) == 1
        tracked_trade = parent.trade_facts.trades[0]
        assert tracked_trade.trade_quantity == parent.parent_intent.quantity
        assert tracked_trade.cumulative_commission is None
        assert tracked_trade.commission_quality == "INCOMPLETE"

        allocation = parent.risk_gate.get_strategy_allocation(
            parent.parent_risk_intent.scope,
            parent.parent_risk_intent.strategy_id,
        )
        assert allocation.used_notional == parent.parent_risk_intent.notional
        assert allocation.reserved_notional == Decimal("0")
        assert allocation.available_notional < allocation.allocated_notional
    finally:
        parent.store.close()
        parent.risk_gate.close()


@pytest.mark.unit
def test_recovery_source_facts_and_proof_bind_store_scope_parent_fill_and_child(tmp_path):
    parent = _create_tracked_parent_source(tmp_path, name="recovery-proof")
    try:
        reservation, window, facts = _make_recovery_child_source(
            parent, child_id="recovery-proof-child"
        )
        stored_parent = parent.store.get(
            parent.parent_intent.intent_id, scope=parent.scope
        )
        assert facts.book_id == stored_parent.scope_key == parent.scope.key
        assert facts.parent_intent_id == parent.parent_intent.intent_id
        assert facts.parent_order_ref == parent.parent_order_ref.order_ref
        assert facts.parent_filled_quantity == stored_parent.filled_quantity
        assert facts.attributable_position_quantity == Decimal("1")
        assert facts.order_ref == reservation.order_ref
        assert facts.recovery_intent_id == reservation.managed_intent_id
        assert facts.session_generation_id == parent.binding.session_generation_id
        assert facts.connection_generation == parent.binding.connection_generation
        assert facts.journal_incarnation_id
        assert facts.position_window_id == window.window_id
        assert facts.position_window_digest_sha256 == window.row_digest
        assert facts.source_revision > parent.trade_facts.trades[-1].source_sequence
        assert facts.valid_until_ns > facts.observed_at_ns

        child = _intent(
            parent.scope,
            facts.recovery_intent_id,
            side=Side.SELL,
            position_effect=PositionEffect.CLOSE_TODAY,
            reduce_only=True,
            quantity=facts.attributable_position_quantity,
            position_side=facts.position_side,
            position_bucket=facts.position_bucket,
        )
        child_risk_intent = parent.risk_adapter._map_order(child)
        proof = parent.store.issue_ctp_recovery_close_source_proof(
            parent.scope,
            parent.parent_intent.intent_id,
            child_risk_intent,
            window,
            owner_handle=parent.owner,
            writer_lease=parent.lease,
            parent_permit=parent.parent_permit,
            parent_risk_intent=parent.parent_risk_intent,
        )
        assert proof.book_id == stored_parent.scope_key
        assert proof.parent_intent_hash == parent.parent_risk_intent.fingerprint
        assert proof.recovery_intent_hash == child_risk_intent.fingerprint
        assert proof.source_evidence_sha256 == facts.source_evidence_sha256
        assert proof.parent_filled_quantity == stored_parent.filled_quantity
        assert proof.attributable_position_quantity == facts.attributable_position_quantity

        wrong_scope = type(parent.scope)(
            parent.scope.provider,
            parent.scope.environment,
            parent.scope.account_ref,
            "strategy.other",
            parent.scope.trading_day,
        )
        with pytest.raises(
            (ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable)
        ):
            parent.store.read_ctp_recovery_close_source_facts(
                wrong_scope,
                parent.parent_intent.intent_id,
                window,
                owner_handle=parent.owner,
                writer_lease=parent.lease,
                parent_permit=parent.parent_permit,
            )
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            parent.store.read_ctp_recovery_close_source_facts(
                parent.scope,
                "different-parent-intent",
                window,
                owner_handle=parent.owner,
                writer_lease=parent.lease,
                parent_permit=parent.parent_permit,
            )
        wrong_child = _intent(
            parent.scope,
            "different-recovery-child",
            side=Side.SELL,
            position_effect=PositionEffect.CLOSE_TODAY,
            reduce_only=True,
            quantity=facts.attributable_position_quantity,
            position_side=facts.position_side,
            position_bucket=facts.position_bucket,
        )
        with pytest.raises(ContractValidationError):
            parent.store.issue_ctp_recovery_close_source_proof(
                parent.scope,
                parent.parent_intent.intent_id,
                parent.risk_adapter._map_order(wrong_child),
                window,
                owner_handle=parent.owner,
                writer_lease=parent.lease,
                parent_permit=parent.parent_permit,
                parent_risk_intent=parent.parent_risk_intent,
            )

        wrong_bucket = _intent(
            parent.scope,
            facts.recovery_intent_id,
            side=Side.SELL,
            position_effect=PositionEffect.CLOSE_YESTERDAY,
            reduce_only=True,
            quantity=facts.attributable_position_quantity,
            position_side=facts.position_side,
            position_bucket="YESTERDAY",
        )
        with pytest.raises(ContractValidationError):
            parent.store.issue_ctp_recovery_close_source_proof(
                parent.scope,
                parent.parent_intent.intent_id,
                parent.risk_adapter._map_order(wrong_bucket),
                window,
                owner_handle=parent.owner,
                writer_lease=parent.lease,
                parent_permit=parent.parent_permit,
                parent_risk_intent=parent.parent_risk_intent,
            )
    finally:
        parent.store.close()
        parent.risk_gate.close()


@pytest.mark.unit
def test_recovery_source_proof_stales_when_a_new_child_position_window_is_sealed(tmp_path):
    parent = _create_tracked_parent_source(tmp_path, name="recovery-window-revision")
    try:
        reservation, first_window, first_facts = _make_recovery_child_source(
            parent, child_id="recovery-window-revision-child"
        )
        child_intent = _recovery_order_intent(parent, first_facts)
        child_risk_intent = parent.risk_adapter._map_order(child_intent)
        first_window_row = parent.store.read_ctp_position_evidence_window(
            parent.scope,
            first_window,
            owner_handle=parent.owner,
            writer_lease=parent.lease,
        )
        stale_proof = parent.store.issue_ctp_recovery_close_source_proof(
            parent.scope,
            parent.parent_intent.intent_id,
            child_risk_intent,
            first_window,
            owner_handle=parent.owner,
            writer_lease=parent.lease,
            parent_permit=parent.parent_permit,
            parent_risk_intent=parent.parent_risk_intent,
        )

        # A later sealed query for the same reserved child is a new source
        # revision. It does not overwrite the earlier append-only window, but
        # it makes that window unusable as fresh dispatch authority.
        next_window = _position_window(
            parent.store,
            parent.scope,
            parent.lease,
            parent.owner,
            parent.callback_store,
            parent.binding,
            parent.tags,
            child_intent.intent_id,
            positions=(
                {
                    "BrokerID": "broker",
                    "InvestorID": "user",
                    "InstrumentID": parent.parent_intent.instrument,
                    "ExchangeID": "SHFE",
                    "PosiDirection": "2",
                    "HedgeFlag": "1",
                    "PositionDate": "1",
                    "TradingDay": parent.scope.trading_day,
                    "Position": 1,
                    "TodayPosition": 1,
                    "YdPosition": 0,
                    "LongFrozen": 0,
                    "ShortFrozen": 0,
                },
            ),
            source_sequence_start=first_window_row.end_source_sequence,
            request_id_start=17,
            client=parent.query_client,
            retain_client_api=True,
        )
        current_facts = parent.store.read_ctp_recovery_close_source_facts(
            parent.scope,
            parent.parent_intent.intent_id,
            next_window,
            owner_handle=parent.owner,
            writer_lease=parent.lease,
            parent_permit=parent.parent_permit,
        )
        assert next_window.window_id != first_window.window_id
        assert next_window.row_digest != first_window.row_digest
        historical_windows = parent.store.read_ctp_position_evidence_windows(
            parent.scope,
            child_intent.intent_id,
            owner_handle=parent.owner,
            writer_lease=parent.lease,
        )
        assert {item.window_id for item in historical_windows} == {
            first_window.window_id,
            next_window.window_id,
        }
        assert current_facts.source_revision > first_facts.source_revision
        assert current_facts.source_evidence_sha256 != first_facts.source_evidence_sha256
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            parent.store.read_ctp_recovery_close_source_facts(
                parent.scope,
                parent.parent_intent.intent_id,
                first_window,
                owner_handle=parent.owner,
                writer_lease=parent.lease,
                parent_permit=parent.parent_permit,
            )

        with pytest.raises(PermitInvalidError) as stale_error:
            parent.risk_gate.reserve_recovery_reduction(
                child_risk_intent,
                parent_permit_id=parent.parent_permit.permit_id,
                source_proof=stale_proof,
            )
        assert stale_error.value.code == "RECOVERY_SOURCE_PROOF_REJECTED"

        fresh_proof = parent.store.issue_ctp_recovery_close_source_proof(
            parent.scope,
            parent.parent_intent.intent_id,
            child_risk_intent,
            next_window,
            owner_handle=parent.owner,
            writer_lease=parent.lease,
            parent_permit=parent.parent_permit,
            parent_risk_intent=parent.parent_risk_intent,
        )
        allocation = parent.risk_gate.reserve_recovery_reduction(
            child_risk_intent,
            parent_permit_id=parent.parent_permit.permit_id,
            source_proof=fresh_proof,
        )
        assert allocation.source_revision == current_facts.source_revision
        assert allocation.source_evidence_sha256 == current_facts.source_evidence_sha256
        assert reservation.managed_intent_id == child_intent.intent_id
    finally:
        parent.store.close()
        parent.risk_gate.close()


@pytest.mark.unit
def test_expired_recovery_close_authorization_cannot_stage_a_ready_command(
    tmp_path, monkeypatch
):
    parent = _create_tracked_parent_source(tmp_path, name="recovery-auth-expired")
    try:
        reservation, window, facts = _make_recovery_child_source(
            parent, child_id="recovery-auth-expired-child"
        )
        child_intent = _recovery_order_intent(parent, facts)
        _child_risk_intent, _proof, _allocation, authorization = _admit_recovery_child(
            parent, child_intent, facts, window
        )
        real_time = time
        monkeypatch.setattr(
            execution_store_module,
            "time",
            SimpleNamespace(
                time_ns=lambda: authorization.expires_at_ns + 1,
                monotonic_ns=real_time.monotonic_ns,
                time=real_time.time,
                sleep=real_time.sleep,
            ),
        )
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            _stage_recovery_child(
                parent,
                child_intent,
                reservation,
                authorization,
                command_id="recovery-expired-command",
            )
        assert parent.store.read_ctp_dispatch_command(
            parent.scope, "recovery-expired-command"
        ) is None
        assert _recovery_authorization_use_count(parent.store) == 0
    finally:
        parent.store.close()
        parent.risk_gate.close()


@pytest.mark.unit
def test_recovery_close_authorization_rejects_wrong_scope_ref_session_generation_and_intent(
    tmp_path,
):
    parent = _create_tracked_parent_source(tmp_path, name="recovery-auth-bindings")
    try:
        reservation, window, facts = _make_recovery_child_source(
            parent, child_id="recovery-auth-bindings-child"
        )
        child_intent = _recovery_order_intent(parent, facts)
        _child_risk_intent, _proof, _allocation, authorization = _admit_recovery_child(
            parent, child_intent, facts, window
        )
        wrong_scope = type(parent.scope)(
            parent.scope.provider,
            parent.scope.environment,
            parent.scope.account_ref,
            "strategy.other",
            parent.scope.trading_day,
        )
        invalid_attempts = (
            (
                "recovery-wrong-scope",
                {
                    "scope": wrong_scope,
                },
            ),
            (
                "recovery-wrong-order-ref",
                {
                    "order_ref": "999999999999",
                },
            ),
            (
                "recovery-wrong-intent",
                {
                    "managed_intent_id": "different-child-intent",
                },
            ),
        )
        for command_id, overrides in invalid_attempts:
            with pytest.raises(
                (ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable)
            ):
                _stage_recovery_child(
                    parent,
                    child_intent,
                    reservation,
                    authorization,
                    command_id=command_id,
                    **overrides,
                )
            assert parent.store.read_ctp_dispatch_command(
                parent.scope, command_id
            ) is None

        wrong_generation = _session_payload(parent.binding)
        wrong_generation["session_generation_id"] = "another-session-generation"
        with pytest.raises(
            (ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable)
        ):
            _stage_recovery_child(
                parent,
                child_intent,
                reservation,
                authorization,
                command_id="recovery-wrong-session-generation",
                session_binding=wrong_generation,
                session_generation_id="another-session-generation",
            )

        wrong_session = _session_payload(parent.binding)
        wrong_session["source_tags"] = dict(wrong_session["source_tags"])
        wrong_session["source_tags"]["source_instance_id"] = "other-source-instance"
        with pytest.raises(
            (ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable)
        ):
            _stage_recovery_child(
                parent,
                child_intent,
                reservation,
                authorization,
                command_id="recovery-wrong-session-binding",
                session_binding=wrong_session,
            )

        assert _recovery_authorization_use_count(parent.store) == 0
    finally:
        parent.store.close()
        parent.risk_gate.close()


@pytest.mark.unit
def test_recovery_dispatch_authorization_rejects_child_order_side_that_opens_exposure(
    tmp_path,
):
    parent = _create_tracked_parent_source(tmp_path, name="recovery-child-side")
    try:
        reservation, window, facts = _make_recovery_child_source(
            parent, child_id="recovery-child-side-intent"
        )
        wrong_side_child = _recovery_order_intent(parent, facts, side=Side.BUY)
        wrong_side_risk_intent = parent.risk_adapter._map_order(wrong_side_child)
        proof = parent.store.issue_ctp_recovery_close_source_proof(
            parent.scope,
            parent.parent_intent.intent_id,
            wrong_side_risk_intent,
            window,
            owner_handle=parent.owner,
            writer_lease=parent.lease,
            parent_permit=parent.parent_permit,
            parent_risk_intent=parent.parent_risk_intent,
        )
        allocation = parent.risk_gate.reserve_recovery_reduction(
            wrong_side_risk_intent,
            parent_permit_id=parent.parent_permit.permit_id,
            source_proof=proof,
        )
        basic_reference = hashlib.sha256(
            ("basic-admission:" + wrong_side_child.intent_id).encode("ascii")
        ).hexdigest()[:32]
        parent.store.admit_intent(wrong_side_child, writer_lease=parent.lease)
        parent.store.activate_intent(
            wrong_side_child.intent_id,
            parent.scope,
            permit_reference=allocation.permit.permit_id,
            basic_admission_reference=basic_reference,
            admission_context_sha256=hashlib.sha256(
                ("context:" + wrong_side_child.intent_id).encode("ascii")
            ).hexdigest(),
            writer_lease=parent.lease,
        )

        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            parent.store.authorize_ctp_recovery_close_dispatch(
                parent.scope,
                parent.parent_intent.intent_id,
                wrong_side_child.intent_id,
                window,
                owner_handle=parent.owner,
                writer_lease=parent.lease,
                recovery_allocation=allocation,
                recovery_allocation_verifier=parent.risk_gate.verify_recovery_reduction_allocation,
                recovery_intent=wrong_side_risk_intent,
            )
        assert parent.store._connection.execute(
            "SELECT COUNT(*) FROM ctp_position_recovery_close_authorizations "
            "WHERE child_intent_id = ?",
            (wrong_side_child.intent_id,),
        ).fetchone()[0] == 0
        assert parent.store._connection.execute(
            "SELECT COUNT(*) FROM ctp_position_recovery_close_authorization_uses"
        ).fetchone()[0] == 0
        assert reservation.order_ref
    finally:
        parent.store.close()
        parent.risk_gate.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("request_override", "case_id"),
    [
        ({"direction": "0"}, "wrong-native-direction"),
        ({"offset_flag": "4"}, "wrong-native-offset"),
    ],
)
def test_recovery_close_stage_rejects_direction_or_offset_that_disagrees_with_source(
    tmp_path, request_override, case_id
):
    parent = _create_tracked_parent_source(tmp_path, name="recovery-native-fields-" + case_id)
    try:
        reservation, window, facts = _make_recovery_child_source(
            parent, child_id="recovery-native-fields-child-" + case_id
        )
        child_intent = _recovery_order_intent(parent, facts)
        _child_risk_intent, _proof, _allocation, authorization = _admit_recovery_child(
            parent, child_intent, facts, window
        )
        assert facts.recovery_order_side == "SELL"
        assert facts.position_bucket == "TODAY"
        with pytest.raises(
            ContractValidationError,
            match="recovery authorization differs from staged submit",
        ):
            _stage_recovery_child(
                parent,
                child_intent,
                reservation,
                authorization,
                command_id="recovery-native-fields-submit-" + case_id,
                **request_override,
            )
        command_id = "recovery-native-fields-submit-" + case_id
        assert parent.store.read_ctp_dispatch_command(parent.scope, command_id) is None
        assert _recovery_authorization_use_count(parent.store) == 0
    finally:
        parent.store.close()
        parent.risk_gate.close()


@pytest.mark.unit
def test_recovery_close_authorization_is_one_use_at_atomic_ready_to_claim(tmp_path):
    parent = _create_tracked_parent_source(tmp_path, name="recovery-claim")
    try:
        reservation, window, facts = _make_recovery_child_source(
            parent, child_id="recovery-claim-child"
        )
        child_intent = _recovery_order_intent(parent, facts)
        child_risk_intent, proof, allocation, authorization = _admit_recovery_child(
            parent, child_intent, facts, window
        )
        assert proof.book_id == parent.store.get(
            parent.parent_intent.intent_id, scope=parent.scope
        ).scope_key
        assert allocation.permit.intent_id == child_intent.intent_id
        assert allocation.permit.quantity == facts.attributable_position_quantity
        assert authorization.child_intent_id == child_intent.intent_id
        assert authorization.order_ref == reservation.order_ref
        assert authorization.position_window_id == window.window_id
        assert _recovery_authorization_use_count(parent.store) == 0

        command, receipt_id = _stage_recovery_child(
            parent,
            child_intent,
            reservation,
            authorization,
            command_id="recovery-claim-child-submit",
            queue=False,
        )
        assert command.status == "READY"
        assert command.local_queue_receipt_queued is None
        assert _recovery_authorization_use_count(parent.store) == 0
        assert _query_observed(parent.store, parent.owner) == 1

        wrong_scope = ExecutionScope(
            parent.scope.provider,
            parent.scope.environment,
            parent.scope.account_ref,
            parent.scope.strategy_id + ".other",
            parent.scope.trading_day,
        )
        with pytest.raises((ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable)):
            parent.store.read_ctp_staged_command_for_session(
                wrong_scope,
                command.command_id,
                owner_handle=parent.owner,
                writer_lease=parent.lease,
                recovery_close_authorization=authorization,
            )
        wrong_child_authorization = replace(
            authorization,
            child_intent_id=child_intent.intent_id + ".other",
        )
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            parent.store.read_ctp_staged_command_for_session(
                parent.scope,
                command.command_id,
                owner_handle=parent.owner,
                writer_lease=parent.lease,
                recovery_close_authorization=wrong_child_authorization,
            )

        read_command = parent.store.read_ctp_staged_command_for_session(
            parent.scope,
            command.command_id,
            owner_handle=parent.owner,
            writer_lease=parent.lease,
            recovery_close_authorization=authorization,
        )
        assert read_command.status == "READY"
        assert read_command.local_queue_receipt_queued is None
        assert read_command.command_id == command.command_id
        assert _recovery_authorization_use_count(parent.store) == 0

        parent.store.record_ctp_dispatch_queue_receipt(
            parent.scope,
            command.command_id,
            {
                "kind": "command_receipt",
                "command": "submit",
                "receipt_id": receipt_id,
                "queued": True,
            },
            writer_lease=parent.lease,
            recovery_close_authorization=authorization,
        )

        def claim_once(_attempt):
            try:
                return parent.store.claim_ctp_dispatch_command_for_session(
                    parent.scope,
                    command.command_id,
                    owner_handle=parent.owner,
                    writer_lease=parent.lease,
                    authority_verifier=_Authority(),
                    required_local_queue_receipt_id=receipt_id,
                    recovery_close_authorization=authorization,
                )
            except (ContractValidationError, InvalidStateTransition, WriterLeaseUnavailable):
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            claims = list(pool.map(claim_once, (1, 2)))
        successful_claims = [claim for claim in claims if claim is not None]
        assert len(successful_claims) == 1
        claim = successful_claims[0]
        assert claim.command.status == "CLAIMED"
        assert _recovery_authorization_use_count(parent.store) == 1
        assert _query_observed(parent.store, parent.owner) == 1
        assert claims.count(None) == 1

        binding = parent.store.verify_ctp_managed_native_call_binding(
            parent.scope,
            parent.owner,
            claim.binding,
            writer_lease=parent.lease,
        )
        assert binding is claim.binding
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            parent.store.read_ctp_staged_command_for_session(
                parent.scope,
                command.command_id,
                owner_handle=parent.owner,
                writer_lease=parent.lease,
                recovery_close_authorization=authorization,
            )
        with pytest.raises((ContractValidationError, InvalidStateTransition)):
            parent.store.claim_ctp_dispatch_command_for_session(
                parent.scope,
                command.command_id,
                owner_handle=parent.owner,
                writer_lease=parent.lease,
                authority_verifier=_Authority(),
                required_local_queue_receipt_id=receipt_id,
                recovery_close_authorization=authorization,
            )
        assert _recovery_authorization_use_count(parent.store) == 1

        child_row = parent.store.get(child_intent.intent_id, scope=parent.scope)
        assert child_row.state is ExecutionState.PENDING_DISPATCH
        assert child_row.filled_quantity == Decimal("0")
        assert child_row.cumulative_commission is None
        risk_allocation = parent.risk_gate.get_strategy_allocation(
            parent.parent_risk_intent.scope,
            parent.parent_risk_intent.strategy_id,
        )
        assert risk_allocation.used_notional == parent.parent_risk_intent.notional
        assert risk_allocation.reserved_notional == Decimal("0")
        assert child_risk_intent.action is IntentAction.REDUCE
    finally:
        parent.store.close()
        parent.risk_gate.close()


_COMMITTED_V24_STORE_COMMIT = "e7fe7e26ea0c703874e32e985f1b8a510fc01f92"


def _load_committed_v24_store_module():
    package_root = Path(__file__).resolve().parents[1]
    source_result = subprocess.run(
        [
            "git",
            "show",
            f"{_COMMITTED_V24_STORE_COMMIT}:src/bt_api_execution/store.py",
        ],
        cwd=package_root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=20,
        check=False,
    )
    assert source_result.returncode == 0, source_result.stderr
    source = source_result.stdout
    assert "    _SCHEMA_VERSION = 24" in source

    module_name = f"bt_api_execution._committed_store_v24_{uuid4().hex}"
    spec = importlib.util.spec_from_loader(module_name, loader=None)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "bt_api_execution"
    sys.modules[module_name] = module
    exec(
        compile(
            source,
            f"git:{_COMMITTED_V24_STORE_COMMIT}:src/bt_api_execution/store.py",
            "exec",
        ),
        module.__dict__,
    )
    return module_name, module


def _sqlite_schema_signature(connection):
    objects = tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT type, name, tbl_name, sql
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY type, name
            """
        ).fetchall()
    )
    tables = [row[1] for row in objects if row[0] == "table"]
    table_columns = tuple(
        (
            table,
            tuple(
                tuple(row)
                for row in connection.execute(
                    f'PRAGMA table_info("{table.replace(chr(34), chr(34) * 2)}")'
                ).fetchall()
            ),
        )
        for table in tables
    )
    return objects, table_columns


def _sqlite_table_content_signature(connection):
    table_names = [
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]
    result = []
    for table in sorted(table_names):
        quoted_table = '"' + table.replace('"', '""') + '"'
        rows = tuple(
            sorted(
                (tuple(row) for row in connection.execute(f"SELECT * FROM {quoted_table}").fetchall()),
                key=repr,
            )
        )
        result.append((table, rows))
    return tuple(result)


@pytest.mark.unit
def test_committed_v24_store_migrates_to_v26_with_fresh_schema_parity(tmp_path):
    legacy_module_name, legacy_module = _load_committed_v24_store_module()
    migrated_path = tmp_path / "execution-v24-migrated.sqlite3"
    fresh_path = tmp_path / "execution-v26-fresh.sqlite3"
    scope = ExecutionScope(
        "FAKE", "simulation", "acct-v24-migration", "strategy.v24-migration", "20261021"
    )
    intent = OrderIntent.limit(
        intent_id="v24-migration-preserved-intent",
        scope=scope,
        signal_id="signal-v24-migration-preserved-intent",
        instrument="FAKE-PERP",
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("100"),
        metadata_version="migration-v1",
    )
    legacy_store = None
    migrated_store = None
    fresh_store = None
    try:
        legacy_store = legacy_module.SqliteExecutionStore(migrated_path)
        assert legacy_store._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()["value"] == "24"
        assert legacy_store._connection.execute(
            """
            SELECT COUNT(*) FROM sqlite_master
            WHERE type = 'table' AND name = 'ctp_dispatch_position_evidence_windows'
            """
        ).fetchone()[0] == 0

        legacy_lease = legacy_store.acquire_or_renew_lease(
            scope, "v24-migration-writer", ttl_ns=30_000_000_000
        )
        legacy_store.admit_intent(intent, writer_lease=legacy_lease)
        legacy_store.activate_intent(
            intent.intent_id, scope, writer_lease=legacy_lease
        )
        legacy_record = legacy_store.get(intent.intent_id, scope=scope)
        assert legacy_record is not None
        legacy_record_fingerprint = (
            legacy_record.intent_id,
            legacy_record.payload_sha256,
            legacy_record.state,
            legacy_record.permit_reference,
            legacy_record.filled_quantity,
        )
        journal_incarnation_id = legacy_store._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'journal_incarnation_id'"
        ).fetchone()["value"]
        legacy_schema_version = legacy_store._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()["value"]
        legacy_store.close()
        legacy_store = None

        migrated_store = SqliteExecutionStore(migrated_path)
        fresh_store = SqliteExecutionStore(fresh_path)
        migrated_version = migrated_store._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()["value"]
        fresh_version = fresh_store._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()["value"]
        migrated_record = migrated_store.get(intent.intent_id, scope=scope)
        assert migrated_version == fresh_version == "26"
        assert legacy_schema_version == "24"
        assert migrated_record is not None
        assert (
            migrated_record.intent_id,
            migrated_record.payload_sha256,
            migrated_record.state,
            migrated_record.permit_reference,
            migrated_record.filled_quantity,
        ) == legacy_record_fingerprint
        assert migrated_store._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'journal_incarnation_id'"
        ).fetchone()["value"] == journal_incarnation_id
        assert _sqlite_schema_signature(migrated_store._connection) == (
            _sqlite_schema_signature(fresh_store._connection)
        )
    finally:
        if legacy_store is not None:
            legacy_store.close()
        if migrated_store is not None:
            migrated_store.close()
        if fresh_store is not None:
            fresh_store.close()
        sys.modules.pop(legacy_module_name, None)


@pytest.mark.unit
def test_current_v26_store_rejects_missing_recovery_use_table_without_repair(tmp_path):
    path = tmp_path / "execution-v26-missing-recovery-use.sqlite3"
    store = SqliteExecutionStore(path)
    store.close()

    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute(
            "DROP TABLE ctp_position_recovery_close_authorization_uses"
        )
        before_schema = _sqlite_schema_signature(connection)
        before_contents = _sqlite_table_content_signature(connection)
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == "26"
        assert "ctp_position_recovery_close_authorization_uses" not in {
            name for _type, name, _table, _sql in before_schema[0]
        }
    finally:
        connection.close()

    with pytest.raises(
        DurableStoreError,
        match="schema is incomplete|schema definitions do not match|scope-set schema is incomplete",
    ):
        SqliteExecutionStore(path)

    after = sqlite3.connect(path, isolation_level=None)
    try:
        assert _sqlite_schema_signature(after) == before_schema
        assert _sqlite_table_content_signature(after) == before_contents
        assert after.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert after.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == "26"
        assert after.execute(
            """
            SELECT COUNT(*) FROM sqlite_master
            WHERE type = 'table'
              AND name = 'ctp_position_recovery_close_authorization_uses'
            """
        ).fetchone()[0] == 0
    finally:
        after.close()
