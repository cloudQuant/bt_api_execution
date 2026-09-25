from __future__ import annotations

import json
import sqlite3
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256

import pytest

import bt_api_execution.store as execution_store_module
from bt_api_execution import (
    ContractValidationError,
    CtpCancelTarget,
    CtpDispatchAuthority,
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


class _FakeCtpDispatchAuthorityVerifier:
    """Test-only fresh verifier; it never calls a provider or external source."""

    def __init__(self, *, overrides=None, error=None, connection=None, after_verify=None):
        self.overrides = dict(overrides or {})
        self.error = error
        self.connection = connection
        self.after_verify = after_verify
        self.calls = []
        self.in_transaction = None

    def verify_action(self, command, *, now_ns):
        self.calls.append((command, now_ns))
        if self.connection is not None:
            self.in_transaction = self.connection.in_transaction
        if self.error is not None:
            raise self.error
        values = {
            "authority_type": "ctp_dispatch_authority.v1",
            "command_binding_sha256": command.authority_binding_sha256,
            "approval_use_id": command.approval_use_id,
            "approval_digest": command.approval_digest,
            "source_digest_sha256": sha256(b"fake current source snapshot").hexdigest(),
            "verifier_id": "test-verifier.v1",
            "verified_at_ns": now_ns,
            "expires_at_ns": now_ns + 5_000_000_000,
        }
        values.update(self.overrides)
        authority = CtpDispatchAuthority(**values)
        if self.after_verify is not None:
            self.after_verify(command, now_ns, authority)
        return authority


def _authority_verifier(**overrides):
    return _FakeCtpDispatchAuthorityVerifier(overrides=overrides)


class _ControlledClock:
    def __init__(self, now_ns):
        self.now_ns = now_ns

    def time_ns(self):
        return self.now_ns


@pytest.mark.unit
def test_v4_execution_store_migrates_to_command_outbox_v6(tmp_path):
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
    # v5, then one-use authority consumption was added in v6.
    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TABLE ctp_dispatch_authority_uses")
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
        assert version == "6"
        assert "ctp_dispatch_commands" in tables
        assert "ctp_order_ref_watermarks" in tables
        assert "ctp_dispatch_authority_uses" in tables
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
def test_v5_execution_store_migrates_one_use_authority_table(tmp_path):
    path = tmp_path / "execution.sqlite3"
    initial = SqliteExecutionStore(path)
    initial.close()
    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TABLE ctp_dispatch_authority_uses")
        connection.execute("UPDATE execution_meta SET value = '5' WHERE key = 'schema_version'")
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
        assert version == "6"
        assert "ctp_dispatch_authority_uses" in tables
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
            store.claim_ctp_dispatch_command(
                scope,
                old_command.command_id,
                writer_lease=lease,
                authority_verifier=_authority_verifier(),
            )

        _reserve_seeded(store, scope, lease, "new-intent")
        with pytest.raises(ContractValidationError, match="predates or conflicts"):
            store.claim_ctp_dispatch_command(
                scope,
                old_command.command_id,
                writer_lease=lease,
                authority_verifier=_authority_verifier(),
            )
        assert store.read_ctp_dispatch_command(scope, old_command.command_id).status == "READY"
    finally:
        store.close()


@pytest.mark.unit
def test_stored_approval_digest_alone_cannot_claim_a_command(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        staged = _stage_submit(store, scope, lease, reservation)
        with pytest.raises(TypeError, match="authority_verifier"):
            store.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=lease,
            )
        assert store.read_ctp_dispatch_command(scope, staged.command_id).status == "READY"
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_authority_uses"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"command_binding_sha256": "0" * 64}, "binding does not match command"),
        ({"approval_use_id": "another-action"}, "binding does not match command"),
        ({"verified_at_ns": 0}, "not freshly verified"),
        ({"expires_at_ns": 0}, "is expired"),
        ({"source_digest_sha256": "g" * 64}, "invalid authority source digest"),
    ],
)
def test_claim_rejects_stale_or_mismatched_action_authority(tmp_path, override, message):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        staged = _stage_submit(store, scope, lease, reservation)
        with pytest.raises(ContractValidationError, match=message):
            store.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=lease,
                authority_verifier=_authority_verifier(**override),
            )
        assert store.read_ctp_dispatch_command(scope, staged.command_id).status == "READY"
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_authority_uses"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expiry_target", "expected_error", "message"),
    [
        ("authority", ContractValidationError, "authority is expired"),
        ("writer_lease", WriterLeaseUnavailable, None),
    ],
)
def test_slow_verifier_crossing_expiry_rolls_back_claim(
    tmp_path, monkeypatch, expiry_target, expected_error, message
):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        staged = _stage_submit(store, scope, lease, reservation)
        if expiry_target == "authority":
            verification_started_ns = lease.expires_at_ns - 20_000_000_000
            verifier_returned_ns = verification_started_ns + 5_000_000_001
        else:
            verification_started_ns = lease.expires_at_ns - 1_000_000_000
            verifier_returned_ns = lease.expires_at_ns + 1
        clock = _ControlledClock(verification_started_ns)
        monkeypatch.setattr(execution_store_module, "time", clock)

        def cross_expiry(_command, now_ns, _authority):
            assert now_ns == verification_started_ns
            clock.now_ns = verifier_returned_ns

        verifier = _FakeCtpDispatchAuthorityVerifier(after_verify=cross_expiry)
        with pytest.raises(expected_error, match=message):
            store.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=lease,
                authority_verifier=verifier,
            )

        assert len(verifier.calls) == 1
        assert store.read_ctp_dispatch_command(scope, staged.command_id).status == "READY"
        row = store._connection.execute(
            "SELECT claimed_at_ns FROM ctp_dispatch_commands WHERE command_id = ?",
            (staged.command_id,),
        ).fetchone()
        assert row["claimed_at_ns"] is None
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_authority_uses"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


@pytest.mark.unit
def test_authority_verifier_failure_leaves_command_and_use_ledger_untouched(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        staged = _stage_submit(store, scope, lease, reservation)
        sentinel = "private-source-secret-sentinel"
        verifier = _FakeCtpDispatchAuthorityVerifier(
            error=RuntimeError("source check failed: " + sentinel)
        )
        with pytest.raises(ContractValidationError, match="verification failed") as caught:
            store.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=lease,
                authority_verifier=verifier,
            )
        formatted_traceback = "".join(traceback.format_exception(caught.value))
        assert str(caught.value) == "fresh CTP dispatch authority verification failed"
        assert caught.value.__suppress_context__ is True
        assert sentinel not in str(caught.value)
        assert sentinel not in formatted_traceback
        assert len(verifier.calls) == 1
        assert store.read_ctp_dispatch_command(scope, staged.command_id).status == "READY"
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_authority_uses"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


@pytest.mark.unit
def test_fresh_verification_one_use_and_claim_share_one_durable_transaction(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        staged = _stage_submit(store, scope, lease, reservation)
        store._connection.execute(
            """
            CREATE TRIGGER reject_authority_use
            BEFORE INSERT ON ctp_dispatch_authority_uses
            BEGIN
                SELECT RAISE(ABORT, 'injected authority-use failure');
            END;
            """
        )
        failed_verifier = _FakeCtpDispatchAuthorityVerifier(connection=store._connection)
        with pytest.raises(DurableStoreError):
            store.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=lease,
                authority_verifier=failed_verifier,
            )
        assert len(failed_verifier.calls) == 1
        assert failed_verifier.in_transaction is True
        assert store.read_ctp_dispatch_command(scope, staged.command_id).status == "READY"
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_authority_uses"
            ).fetchone()[0]
            == 0
        )

        store._connection.execute("DROP TRIGGER reject_authority_use")
        verifier = _FakeCtpDispatchAuthorityVerifier(connection=store._connection)
        claimed = store.claim_ctp_dispatch_command(
            scope,
            staged.command_id,
            writer_lease=lease,
            authority_verifier=verifier,
        )
        assert claimed is not None and claimed.status == "CLAIMED"
        assert verifier.in_transaction is True
        use = store._connection.execute(
            "SELECT * FROM ctp_dispatch_authority_uses WHERE account_key = ?",
            (staged.account_key,),
        ).fetchone()
        assert use is not None
        assert use["approval_use_id"] == staged.approval_use_id
        assert use["command_id"] == staged.command_id
        assert use["command_binding_sha256"] == staged.authority_binding_sha256
        assert use["approval_digest"] == staged.approval_digest
        assert use["source_digest_sha256"] == sha256(b"fake current source snapshot").hexdigest()
        assert use["writer_owner_id"] == lease.owner_id
        assert use["fencing_token"] == lease.fencing_token
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            store._connection.execute(
                "DELETE FROM ctp_dispatch_authority_uses WHERE approval_use_id = ?",
                (staged.approval_use_id,),
            )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_authority_uses"
            ).fetchone()[0]
            == 1
        )
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
        claimed = store.claim_ctp_dispatch_command(
            scope,
            staged.command_id,
            writer_lease=lease,
            authority_verifier=_authority_verifier(),
        )
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
            store.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=lease,
                authority_verifier=_authority_verifier(),
            )
            is None
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_authority_uses"
            ).fetchone()[0]
            == 1
        )
    finally:
        store.close()


@pytest.mark.unit
def test_queued_receipt_is_local_dispatch_fact_not_provider_ack(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        staged = _stage_submit(store, scope, lease, reservation)
        claimed = store.claim_ctp_dispatch_command(
            scope,
            staged.command_id,
            writer_lease=lease,
            authority_verifier=_authority_verifier(),
        )
        assert claimed is not None
        completed = store.complete_ctp_dispatch_command(
            scope,
            _receipt(claimed, outcome="QUEUED", native={"request_id": 17, "queue_code": 0}),
            writer_lease=lease,
        )
        assert completed.status == "COMPLETED"
        assert completed.native_receipt_payload == {"request_id": 17, "queue_code": 0}
        persisted_echo = store._connection.execute(
            """
            SELECT completion_echo_json FROM ctp_dispatch_commands
            WHERE account_key = ? AND command_id = ?
            """,
            (completed.account_key, completed.command_id),
        ).fetchone()
        assert persisted_echo is not None
        assert json.loads(persisted_echo["completion_echo_json"])["outcome"] == "QUEUED"
        # The v6 local outbox has no provider order/cancel projection to advance.
        assert (
            store._connection.execute("SELECT COUNT(*) FROM execution_records").fetchone()[0] == 0
        )
        assert (
            store._connection.execute("SELECT COUNT(*) FROM cancellation_records").fetchone()[0]
            == 0
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
            return store.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=lease,
                authority_verifier=_authority_verifier(),
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = tuple(pool.map(claim, (first, second)))
        assert sum(result is not None for result in results) == 1
        assert first.read_ctp_dispatch_command(scope, staged.command_id).status == "CLAIMED"
        assert (
            first._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_authority_uses"
            ).fetchone()[0]
            == 1
        )
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
    assert store.claim_ctp_dispatch_command(
        scope,
        staged.command_id,
        writer_lease=old_lease,
        authority_verifier=_authority_verifier(),
    )
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
        # No callback/query evidence was supplied after restart; UNKNOWN remains durable.
        assert reopened.read_ctp_dispatch_command(scope, staged.command_id).status == "UNKNOWN"
        with pytest.raises(InvalidStateTransition, match="not CLAIMED"):
            reopened.complete_ctp_dispatch_command(
                scope,
                _receipt(recovered[0], outcome="QUEUED"),
                writer_lease=new_lease,
            )
        assert (
            reopened.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=new_lease,
                authority_verifier=_authority_verifier(),
            )
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
            first_scope,
            first_command.command_id,
            writer_lease=lease,
            authority_verifier=_authority_verifier(),
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
                second_scope,
                second_command.command_id,
                writer_lease=new_lease,
                authority_verifier=_authority_verifier(),
            )
        recovered = store.recover_claimed_ctp_dispatch_commands(
            second_scope, writer_lease=new_lease
        )
        assert len(recovered) == 1
        assert recovered[0].scope_key == first_scope.key
        assert recovered[0].status == "UNKNOWN"
        with pytest.raises(InvalidStateTransition, match="account has unresolved"):
            store.claim_ctp_dispatch_command(
                second_scope,
                second_command.command_id,
                writer_lease=new_lease,
                authority_verifier=_authority_verifier(),
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
def test_cancel_claim_authority_binds_exact_native_target(tmp_path):
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
        staged = store.stage_ctp_dispatch_command(
            scope,
            "cancel-authority-1",
            "CANCEL",
            request,
            approval_use_id="approval-use-cancel-authority",
            approval_digest=sha256(b"cancel action receipt digest").hexdigest(),
            session_binding={"session_identity": "opaque-session-v1"},
            writer_lease=lease,
            cancel_target=target,
        )
        changed_target = replace(
            staged, cancel_target_session_id=staged.cancel_target_session_id + 1
        )
        assert changed_target.authority_binding_sha256 != staged.authority_binding_sha256
        with pytest.raises(ContractValidationError, match="binding does not match command"):
            store.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=lease,
                authority_verifier=_authority_verifier(
                    command_binding_sha256=changed_target.authority_binding_sha256
                ),
            )
        assert store.read_ctp_dispatch_command(scope, staged.command_id).status == "READY"
        assert store.claim_ctp_dispatch_command(
            scope,
            staged.command_id,
            writer_lease=lease,
            authority_verifier=_authority_verifier(),
        )
        use = store._connection.execute(
            "SELECT command_binding_sha256 FROM ctp_dispatch_authority_uses WHERE command_id = ?",
            (staged.command_id,),
        ).fetchone()
        assert use["command_binding_sha256"] == staged.authority_binding_sha256
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
