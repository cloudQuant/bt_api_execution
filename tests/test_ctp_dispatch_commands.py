from __future__ import annotations

import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from hashlib import sha256

import pytest

from bt_api_execution import (
    ContractValidationError,
    CtpCancelTarget,
    CtpDispatchReceipt,
    CtpOrderRefSeedProof,
    DurableStoreError,
    ExecutionScope,
    ExecutionState,
    IntentConflictError,
    InvalidStateTransition,
    OrderIntent,
    Side,
    SqliteExecutionStore,
    WriterLeaseUnavailable,
)


def _scope() -> ExecutionScope:
    return ExecutionScope("CTP", "simulation", "acct-outbox", "strategy.outbox", "20260925")


def _runtime_id(value: str) -> str:
    return "bt-managed-v1:" + sha256(value.encode("ascii")).hexdigest()


def _proof(day: str = "20260925") -> CtpOrderRefSeedProof:
    return CtpOrderRefSeedProof(
        trading_day=day,
        native_max_order_ref="000000000010",
        legacy_ledger_max_order_ref="000000000012",
        legacy_ledger_sha256=sha256(b"offline legacy ledger fixture").hexdigest(),
    )


def _lease(store: SqliteExecutionStore, scope: ExecutionScope, owner: str = "outbox-owner"):
    return store.acquire_or_renew_lease(scope, owner, ttl_ns=30_000_000_000)


def _reserve_seeded(
    store: SqliteExecutionStore,
    scope: ExecutionScope,
    lease,
    intent_id: str = "intent-1",
):
    return store.seed_ctp_order_ref_and_reserve_identity(
        scope,
        _proof(scope.trading_day),
        intent_id,
        _runtime_id(intent_id),
        writer_lease=lease,
    )


def _stage_submit(
    store,
    scope,
    lease,
    reservation,
    command_id="command-1",
    payload=None,
    approval_use_id="approval-use-1",
):
    request = dict(
        payload or {"InstrumentID": "rb2710", "LimitPrice": "3510.5", "VolumeTotalOriginal": 1}
    )
    request.setdefault("OrderRef", reservation.order_ref)
    return store.stage_ctp_dispatch_command(
        scope,
        command_id,
        "SUBMIT",
        request,
        approval_use_id=approval_use_id,
        approval_digest=sha256(b"approval receipt digest").hexdigest(),
        session_binding={"session_identity": "opaque-session-v1"},
        writer_lease=lease,
        managed_intent_id=reservation.managed_intent_id,
        order_ref=reservation.order_ref,
    )


def _receipt(command, *, outcome="QUEUED", native=None):
    return CtpDispatchReceipt(
        receipt_type="ctp_dispatch_receipt.v1",
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
        native_receipt_payload=native or {"queue_code": 0},
    )


@pytest.mark.unit
def test_v4_execution_store_migrates_to_command_outbox_v5(tmp_path):
    path = tmp_path / "execution.sqlite3"
    scope = _scope()
    legacy_intent = OrderIntent.limit(
        intent_id="legacy-intent",
        scope=scope,
        signal_id="legacy-signal",
        instrument="rb2710",
        side=Side.BUY,
        quantity=Decimal("1"),
        price=Decimal("3510.5"),
        metadata_version="metadata-v1",
    )
    store = SqliteExecutionStore(path)
    try:
        lease = _lease(store, scope, "migration-owner")
        legacy_record = store.admit_intent(legacy_intent, writer_lease=lease)
        assert legacy_record.state is ExecutionState.PENDING_ADMISSION
        legacy_outbox = store.read_outbox(scope=scope)
        legacy_reservation = store.reserve_ctp_order_identity(
            scope, "legacy-managed-intent", _runtime_id("legacy-managed-intent")
        )
    finally:
        store.close()

    # A v4 database has the execution journal, event outbox, lease, and CTP
    # identity tables. The command and OrderRef watermark tables were added in
    # v5, so both must be absent from this fixture before migration.
    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TABLE ctp_dispatch_commands")
        connection.execute("DROP TABLE ctp_order_ref_watermarks")
        connection.execute("UPDATE execution_meta SET value = '4' WHERE key = 'schema_version'")
        connection.commit()
    finally:
        connection.close()

    migrated = SqliteExecutionStore(path)
    try:
        version = migrated._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()["value"]
        tables = {
            row["name"]
            for row in migrated._connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        assert version == "5"
        assert "ctp_dispatch_commands" in tables
        assert "ctp_order_ref_watermarks" in tables
        assert "execution_outbox" in tables
        assert migrated.get_intent(legacy_intent.intent_id, scope=scope) == legacy_intent
        migrated_record = migrated.get(legacy_intent.intent_id, scope=scope)
        assert migrated_record is not None
        assert migrated_record.state is ExecutionState.PENDING_ADMISSION
        assert migrated.read_outbox(scope=scope) == legacy_outbox
        migrated_reservation = migrated.read_ctp_order_identity(scope, "legacy-managed-intent")
        assert migrated_reservation == legacy_reservation
        assert (
            migrated._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_ref_watermarks"
            ).fetchone()[0]
            == 0
        )
    finally:
        migrated.close()


@pytest.mark.unit
def test_initial_seed_and_first_reservation_commit_atomically(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        with pytest.raises(ContractValidationError, match="must commit atomically"):
            store.record_ctp_order_ref_seed(scope, _proof(), writer_lease=lease)
        store._connection.execute(
            """
            CREATE TRIGGER reject_seeded_reservation
            BEFORE INSERT ON ctp_order_identity_reservations
            BEGIN
                SELECT RAISE(ABORT, 'injected reservation failure');
            END;
            """
        )
        with pytest.raises(DurableStoreError):
            _reserve_seeded(store, scope, lease)
        assert (
            store._connection.execute("SELECT COUNT(*) FROM ctp_order_ref_watermarks").fetchone()[0]
            == 0
        )

        store._connection.execute("DROP TRIGGER reject_seeded_reservation")
        reservation = _reserve_seeded(store, scope, lease)
        assert reservation.order_ref == "000000000013"
        assert _reserve_seeded(store, scope, lease) == reservation
    finally:
        store.close()


@pytest.mark.unit
def test_stage_is_idempotent_and_rejects_conflicting_command_identity(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        first = _stage_submit(store, scope, lease, reservation)
        assert first.status == "READY"
        assert first.order_ref == reservation.order_ref
        assert (
            first.request_payload_sha256
            == sha256(
                b'{"InstrumentID":"rb2710","LimitPrice":"3510.5","OrderRef":"000000000013","VolumeTotalOriginal":1}'
            ).hexdigest()
        )
        assert _stage_submit(store, scope, lease, reservation) == first
        with pytest.raises(IntentConflictError, match="command_id conflicts"):
            _stage_submit(
                store,
                scope,
                lease,
                reservation,
                payload={
                    "InstrumentID": "cu2710",
                    "VolumeTotalOriginal": 1,
                    "OrderRef": reservation.order_ref,
                },
            )
    finally:
        store.close()


@pytest.mark.unit
def test_approval_use_id_cannot_bind_two_account_commands(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        first_reservation = _reserve_seeded(store, scope, lease, "intent-1")
        second_reservation = _reserve_seeded(store, scope, lease, "intent-2")
        _stage_submit(
            store, scope, lease, first_reservation, "command-1", approval_use_id="approval-use-1"
        )
        with pytest.raises(IntentConflictError, match="approval use is already bound"):
            _stage_submit(
                store,
                scope,
                lease,
                second_reservation,
                "command-2",
                approval_use_id="approval-use-1",
            )
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "payload",
    [
        {"Order": {"Password": "never-store-this", "VolumeTotalOriginal": 1}},
        {"InstrumentID": "rb2710", "ApiToken": "never-store-this"},
        {"InstrumentID": "rb2710", "approval_private_key": "never-store-this"},
    ],
)
def test_command_payload_rejects_nested_credential_shaped_fields(tmp_path, payload):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        with pytest.raises(ContractValidationError, match="credential-shaped"):
            _stage_submit(store, scope, lease, reservation, payload=payload)
        with pytest.raises(ContractValidationError, match="credential-shaped"):
            store.stage_ctp_dispatch_command(
                scope,
                "command-session-secret",
                "SUBMIT",
                {"InstrumentID": "rb2710"},
                approval_use_id="approval-use-1",
                approval_digest=sha256(b"approval receipt digest").hexdigest(),
                session_binding={"nested": {"access_token": "never-store-this"}},
                writer_lease=lease,
                managed_intent_id=reservation.managed_intent_id,
                order_ref=reservation.order_ref,
            )
    finally:
        store.close()


@pytest.mark.unit
def test_claim_requires_seed_and_never_claims_a_preseed_reservation(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        old = store.reserve_ctp_order_identity(scope, "old-intent", _runtime_id("old-intent"))
        old_command = _stage_submit(store, scope, lease, old, command_id="old-command")
        with pytest.raises(ContractValidationError, match="staged-only"):
            store.claim_ctp_dispatch_command(scope, old_command.command_id, writer_lease=lease)

        _reserve_seeded(store, scope, lease, "new-intent")
        with pytest.raises(ContractValidationError, match="predates or conflicts"):
            store.claim_ctp_dispatch_command(scope, old_command.command_id, writer_lease=lease)
        assert store.read_ctp_dispatch_command(scope, old_command.command_id).status == "READY"
    finally:
        store.close()


@pytest.mark.unit
def test_receipt_requires_exact_typed_echo_and_is_idempotently_stored(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        staged = _stage_submit(store, scope, lease, reservation)
        claimed = store.claim_ctp_dispatch_command(scope, staged.command_id, writer_lease=lease)
        assert claimed is not None and claimed.status == "CLAIMED"

        wrong = _receipt(claimed)
        wrong = CtpDispatchReceipt(**{**wrong.__dict__, "session_binding_sha256": "0" * 64})
        with pytest.raises(ContractValidationError, match="echo does not match"):
            store.complete_ctp_dispatch_command(scope, wrong, writer_lease=lease)

        receipt = _receipt(claimed, native={"request_id": 0, "queue_code": 0})
        completed = store.complete_ctp_dispatch_command(scope, receipt, writer_lease=lease)
        assert completed.status == "COMPLETED"
        assert completed.native_receipt_payload == {"request_id": 0, "queue_code": 0}
        assert (
            completed.native_receipt_sha256
            == sha256(b'{"queue_code":0,"request_id":0}').hexdigest()
        )
        assert store.complete_ctp_dispatch_command(scope, receipt, writer_lease=lease) == completed
        assert (
            store.claim_ctp_dispatch_command(scope, staged.command_id, writer_lease=lease) is None
        )
    finally:
        store.close()


@pytest.mark.unit
def test_two_store_claim_race_has_one_local_claimant(tmp_path):
    path = tmp_path / "execution.sqlite3"
    first = SqliteExecutionStore(path)
    second = SqliteExecutionStore(path)
    scope = _scope()
    lease = _lease(first, scope)
    try:
        reservation = _reserve_seeded(first, scope, lease)
        staged = _stage_submit(first, scope, lease, reservation)

        def claim(store):
            return store.claim_ctp_dispatch_command(scope, staged.command_id, writer_lease=lease)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = tuple(pool.map(claim, (first, second)))
        assert sum(result is not None for result in results) == 1
        assert first.read_ctp_dispatch_command(scope, staged.command_id).status == "CLAIMED"
    finally:
        first.close()
        second.close()


@pytest.mark.unit
def test_restart_recovery_marks_prior_claim_unknown_without_replay(tmp_path):
    path = tmp_path / "execution.sqlite3"
    store = SqliteExecutionStore(path)
    scope = _scope()
    old_lease = store.acquire_or_renew_lease(scope, "outbox-before-crash")
    reservation = _reserve_seeded(store, scope, old_lease)
    staged = _stage_submit(store, scope, old_lease, reservation)
    assert store.claim_ctp_dispatch_command(scope, staged.command_id, writer_lease=old_lease)
    store._connection.execute(
        "UPDATE execution_writer_leases SET expires_at_ns = ? WHERE scope_key = ?",
        (time.time_ns() - 1, scope.account_key),
    )
    store.close()

    reopened = SqliteExecutionStore(path)
    new_lease = _lease(reopened, scope, "outbox-after-restart")
    try:
        with pytest.raises(WriterLeaseUnavailable):
            reopened.complete_ctp_dispatch_command(
                scope,
                _receipt(staged),
                writer_lease=new_lease,
            )
        recovered = reopened.recover_claimed_ctp_dispatch_commands(scope, writer_lease=new_lease)
        assert len(recovered) == 1
        assert recovered[0].status == "UNKNOWN"
        assert recovered[0].native_receipt_payload is None
        assert recovered[0].unknown_reason == "claimed_without_receipt_after_writer_change"
        assert (
            reopened.claim_ctp_dispatch_command(scope, staged.command_id, writer_lease=new_lease)
            is None
        )
        assert reopened.recover_claimed_ctp_dispatch_commands(scope, writer_lease=new_lease) == ()
    finally:
        reopened.close()


@pytest.mark.unit
def test_unresolved_command_fences_other_scopes_account_wide(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    first_scope = _scope()
    second_scope = ExecutionScope("CTP", "simulation", "acct-outbox", "strategy.other", "20260925")
    lease = _lease(store, first_scope, "account-writer-before-crash")
    try:
        first_reservation = _reserve_seeded(store, first_scope, lease, "intent-first")
        first_command = _stage_submit(
            store,
            first_scope,
            lease,
            first_reservation,
            "command-first",
            approval_use_id="approval-first",
        )
        assert store.claim_ctp_dispatch_command(
            first_scope, first_command.command_id, writer_lease=lease
        )

        second_reservation = _reserve_seeded(store, second_scope, lease, "intent-second")
        second_command = _stage_submit(
            store,
            second_scope,
            lease,
            second_reservation,
            "command-second",
            approval_use_id="approval-second",
        )
        store._connection.execute(
            "UPDATE execution_writer_leases SET expires_at_ns = ? WHERE scope_key = ?",
            (time.time_ns() - 1, first_scope.account_key),
        )
        new_lease = _lease(store, second_scope, "account-writer-after-crash")

        with pytest.raises(InvalidStateTransition, match="account has unresolved"):
            store.claim_ctp_dispatch_command(
                second_scope, second_command.command_id, writer_lease=new_lease
            )
        recovered = store.recover_claimed_ctp_dispatch_commands(
            second_scope, writer_lease=new_lease
        )
        assert len(recovered) == 1
        assert recovered[0].scope_key == first_scope.key
        assert recovered[0].status == "UNKNOWN"
        with pytest.raises(InvalidStateTransition, match="account has unresolved"):
            store.claim_ctp_dispatch_command(
                second_scope, second_command.command_id, writer_lease=new_lease
            )
    finally:
        store.close()


@pytest.mark.unit
def test_cancel_command_binds_orderref_exchange_system_order_and_session_ids(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        target = CtpCancelTarget(
            order_ref=reservation.order_ref,
            exchange_id="SHFE",
            order_sys_id="sys-order-17",
            front_id=4,
            session_id=91,
        )
        request = {
            "OrderRef": target.order_ref,
            "ExchangeID": target.exchange_id,
            "OrderSysID": target.order_sys_id,
            "FrontID": target.front_id,
            "SessionID": target.session_id,
            "ActionFlag": "0",
            "LimitPrice": 0.0,
            "VolumeChange": 0,
        }
        cancel = store.stage_ctp_dispatch_command(
            scope,
            "cancel-1",
            "CANCEL",
            request,
            approval_use_id="approval-use-cancel",
            approval_digest=sha256(b"cancel approval").hexdigest(),
            session_binding={"session_identity": "opaque-session-v1"},
            writer_lease=lease,
            cancel_target=target,
        )
        assert cancel.cancel_target_order_ref == target.order_ref
        assert cancel.cancel_target_exchange_id == "SHFE"
        assert cancel.cancel_target_order_sys_id == "sys-order-17"
        assert cancel.cancel_target_front_id == 4
        assert cancel.cancel_target_session_id == 91

        mismatched = dict(request, SessionID=92)
        with pytest.raises(ContractValidationError, match="target mismatch"):
            store.stage_ctp_dispatch_command(
                scope,
                "cancel-2",
                "CANCEL",
                mismatched,
                approval_use_id="approval-use-cancel",
                approval_digest=sha256(b"cancel approval").hexdigest(),
                session_binding={"session_identity": "opaque-session-v1"},
                writer_lease=lease,
                cancel_target=target,
            )
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("ActionFlag", "3", "requires native delete ActionFlag"),
        ("LimitPrice", 3510.5, "must not change LimitPrice"),
        ("VolumeChange", 1, "must not change VolumeChange"),
    ],
)
def test_cancel_command_rejects_modify_action_or_fields(tmp_path, field, value, message):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        target = CtpCancelTarget(
            order_ref=reservation.order_ref,
            exchange_id="SHFE",
            order_sys_id="sys-order-17",
            front_id=4,
            session_id=91,
        )
        request = {
            "OrderRef": target.order_ref,
            "ExchangeID": target.exchange_id,
            "OrderSysID": target.order_sys_id,
            "FrontID": target.front_id,
            "SessionID": target.session_id,
            "ActionFlag": "0",
        }
        request[field] = value
        with pytest.raises(ContractValidationError, match=message):
            store.stage_ctp_dispatch_command(
                scope,
                "cancel-invalid-" + field,
                "CANCEL",
                request,
                approval_use_id="approval-use-cancel-" + field,
                approval_digest=sha256(b"cancel approval").hexdigest(),
                session_binding={"session_identity": "opaque-session-v1"},
                writer_lease=lease,
                cancel_target=target,
            )
    finally:
        store.close()
