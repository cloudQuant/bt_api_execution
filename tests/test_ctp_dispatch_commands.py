from __future__ import annotations

import asyncio
import json
import sqlite3
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from hashlib import sha256

import pytest

import bt_api_execution.store as execution_store_module
from bt_api_execution import (
    ContractValidationError,
    CtpCancelTarget,
    CtpDispatchAuthority,
    CtpDispatchCallbackKey,
    CtpDispatchCorrelationKey,
    CtpDispatchReceipt,
    CtpNativeSessionContext,
    CtpOrderRefLegacyMapping,
    CtpOrderRefSeedProof,
    CtpUnknownResolutionAttestation,
    CtpVerifiedCallbackEvidence,
    DurableStoreError,
    ExecutionScope,
    ExecutionState,
    IntentConflictError,
    InvalidStateTransition,
    OrderIntent,
    Side,
    SqliteExecutionStore,
    WriterLeaseUnavailable,
    ctp_native_session_epoch,
    ctp_native_session_generation_id,
    map_ctp_native_order_return,
    payload_sha256,
    require_ctp_dispatch_callback_match,
)
from bt_api_execution.ctp_single_worker_candidate import (
    CtpManagedPreparedDispatch,
    CtpManagedSingleWorkerCandidate,
    CtpNativeDispatchResult,
    stable_managed_command_id,
)


def _scope() -> ExecutionScope:
    return ExecutionScope("CTP", "simulation", "acct-outbox", "strategy.outbox", "20260925")


def _runtime_id(value: str) -> str:
    return "bt-managed-v1:" + sha256(value.encode("ascii")).hexdigest()


def _native_request_id(value: str) -> int:
    return int.from_bytes(sha256(value.encode("ascii")).digest()[:4], "big") % 2_147_483_646 + 1


def _session_binding(generation="test-session-generation", front_id=4, session_id=91):
    return {
        "session_identity": "opaque-session-v1",
        "session_generation_id": generation,
        "dispatch_front_id": front_id,
        "dispatch_session_id": session_id,
    }


def _proof(
    scope: ExecutionScope | None = None,
    *,
    session_generation_id: str = "test-session-generation",
) -> CtpOrderRefSeedProof:
    active_scope = scope or _scope()
    legacy_scope = ExecutionScope(
        "CTP", "simulation", "acct-outbox", "strategy.legacy", "20260924"
    )
    source_digests = (
        ("backtrader_prototype", sha256(b"backtrader prototype fixture").hexdigest()),
        ("sdk_jsonl", sha256(b"empty sdk jsonl fixture").hexdigest()),
    )
    mappings = (
        CtpOrderRefLegacyMapping(
            source_name="backtrader_prototype",
            account_key=active_scope.account_key,
            trading_day="20260924",
            scope_key=legacy_scope.key,
            managed_intent_id="legacy-managed-intent",
            runtime_order_id="legacy-runtime-order-1",
            order_ref="000000000012",
        ),
    )
    return CtpOrderRefSeedProof(
        trading_day=active_scope.trading_day,
        native_max_order_ref="000000000010",
        legacy_ledger_max_order_ref="000000000012",
        legacy_ledger_sha256=payload_sha256(dict(source_digests)),
        account_key=active_scope.account_key,
        scope_key=active_scope.key,
        session_generation_id=session_generation_id,
        native_front_id=4,
        native_session_id=91,
        existing_native_order_refs=("000000000009", "000000000010"),
        legacy_source_sha256=source_digests,
        legacy_mappings=mappings,
    )


def _lease(store: SqliteExecutionStore, scope: ExecutionScope, owner: str = "outbox-owner"):
    return store.acquire_or_renew_lease(scope, owner, ttl_ns=30_000_000_000)


def _reserve_seeded(
    store: SqliteExecutionStore,
    scope: ExecutionScope,
    lease,
    intent_id: str = "intent-1",
    *,
    session_generation_id: str = "test-session-generation",
):
    return store.seed_ctp_order_ref_and_reserve_identity(
        scope,
        _proof(scope, session_generation_id=session_generation_id),
        intent_id,
        _runtime_id(intent_id),
        writer_lease=lease,
    )


def _insert_preseed_identity(
    store: SqliteExecutionStore,
    scope: ExecutionScope,
    intent_id: str,
    runtime_id: str,
    order_ref: str = "000000000001",
):
    store._connection.execute(
        """
        INSERT INTO ctp_order_identity_reservations(
            account_key, trading_day, scope_key, managed_intent_id,
            runtime_order_id, order_ref, created_at_ns
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (scope.account_key, scope.trading_day, scope.key, intent_id, runtime_id, order_ref, 1),
    )
    return store.read_ctp_order_identity(scope, intent_id)


def _stage_submit(
    store,
    scope,
    lease,
    reservation,
    command_id="command-1",
    payload=None,
    approval_use_id="approval-use-1",
    session_generation_id="test-session-generation",
    dispatch_front_id=4,
    dispatch_session_id=91,
    native_request_id=None,
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
        session_binding=_session_binding(
            session_generation_id, dispatch_front_id, dispatch_session_id
        ),
        writer_lease=lease,
        managed_intent_id=reservation.managed_intent_id,
        order_ref=reservation.order_ref,
        managed_action_id=reservation.managed_intent_id,
        session_generation_id=session_generation_id,
        dispatch_front_id=dispatch_front_id,
        dispatch_session_id=dispatch_session_id,
        native_request_id=(
            _native_request_id(command_id) if native_request_id is None else native_request_id
        ),
    )


def _receipt(command, *, outcome="QUEUED", native=None):
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
        native_receipt_payload=native or {"queue_code": 0},
        correlation_key=command.correlation_key,
    )


def _callback(command, *, event_id="event-1", **overrides):
    key = command.correlation_key
    assert key is not None
    values = {
        "version": 1,
        "correlation_key": key,
        "callback_family": "ORDER" if command.operation == "SUBMIT" else "CANCEL_ACTION",
        "stream_id": "fake-td-callback-stream",
        "event_id": event_id,
        "native_request_id": key.native_request_id,
        "native_action_ref": key.native_action_ref,
        "order_ref": key.order_ref,
    }
    if command.operation == "CANCEL":
        values.update(
            exchange_id=key.cancel_target_exchange_id,
            order_sys_id=key.cancel_target_order_sys_id,
            target_front_id=key.cancel_target_front_id,
            target_session_id=key.cancel_target_session_id,
        )
    values.update(overrides)
    return CtpDispatchCallbackKey(**values)


def _stage_cancel(store, scope, lease, reservation, command_id="cancel-command-1"):
    target = CtpCancelTarget(
        order_ref=reservation.order_ref,
        exchange_id="SHFE",
        order_sys_id="sys-order-17",
        front_id=4,
        session_id=91,
    )
    return store.stage_ctp_dispatch_command(
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
        approval_use_id="approval-use-" + command_id,
        approval_digest=sha256(("approval-" + command_id).encode("ascii")).hexdigest(),
        session_binding=_session_binding("test-session-generation"),
        writer_lease=lease,
        cancel_target=target,
        managed_action_id="managed-action-" + command_id,
        session_generation_id="test-session-generation",
        dispatch_front_id=4,
        dispatch_session_id=91,
        native_request_id=_native_request_id(command_id),
        native_action_ref="native-action-" + command_id,
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


class _FakeCtpDispatchCallbackVerifier:
    """Test-only evidence source; it creates no provider or network evidence."""

    def __init__(
        self,
        state="ACKNOWLEDGED",
        *,
        source=b"fake callback",
        error=None,
        ttl_ns=5_000_000_000,
        payload_digest_override=None,
        verification_delay_s=0,
    ):
        self.state = state
        self.source_digest = sha256(source).hexdigest()
        self.error = error
        self.ttl_ns = ttl_ns
        self.payload_digest_override = payload_digest_override
        self.verification_delay_s = verification_delay_s
        self.calls = []

    def verify_callback(self, command, callback, callback_payload, *, now_ns):
        self.calls.append((command, callback, dict(callback_payload), now_ns))
        if self.error is not None:
            raise self.error
        evidence = CtpVerifiedCallbackEvidence(
            evidence_type="ctp_verified_callback.v1",
            callback_key=callback,
            callback_payload_sha256=(
                payload_sha256(callback_payload)
                if self.payload_digest_override is None
                else self.payload_digest_override
            ),
            projection_state=self.state,
            source_digest_sha256=self.source_digest,
            verifier_id="test-callback-verifier.v1",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + self.ttl_ns,
        )
        if self.verification_delay_s:
            time.sleep(self.verification_delay_s)
        return evidence


class _FakeCtpDispatchReconciliationVerifier:
    """Test-only reconciliation attester with no native/query access."""

    def __init__(
        self,
        order_state="FILLED",
        cancel_state=None,
        *,
        source=b"fake reconciliation bundle",
        error=None,
    ):
        self.order_state = order_state
        self.cancel_state = cancel_state
        self.source_digest = sha256(source).hexdigest()
        self.error = error
        self.calls = []

    def verify_unknown(self, command, *, now_ns):
        self.calls.append((command, now_ns))
        if self.error is not None:
            raise self.error
        return CtpUnknownResolutionAttestation(
            attestation_type="ctp_unknown_resolution.v1",
            correlation_key=command.correlation_key,
            order_terminal_state=self.order_state,
            cancel_action_terminal_state=self.cancel_state,
            source_digest_sha256=self.source_digest,
            verifier_id="test-reconciliation-verifier.v1",
            verified_at_ns=now_ns,
            expires_at_ns=now_ns + 5_000_000_000,
        )


def _authority_verifier(**overrides):
    return _FakeCtpDispatchAuthorityVerifier(overrides=overrides)


def _dispatch_fake(store, scope, lease, command, *, outcome="QUEUED", native=None):
    claimed = store.claim_ctp_dispatch_command(
        scope,
        command.command_id,
        writer_lease=lease,
        authority_verifier=_authority_verifier(),
    )
    assert claimed is not None and claimed.status == "CLAIMED"
    return store.complete_ctp_dispatch_command(
        scope, _receipt(claimed, outcome=outcome, native=native), writer_lease=lease
    )


def _apply_callback(
    store,
    scope,
    command,
    callback,
    lease,
    verifier=None,
    *,
    payload=None,
):
    callback_payload = payload or {
        "fake_event_family": callback.callback_family,
        "fake_event_id": callback.event_id,
    }
    return store.apply_ctp_verified_dispatch_callback(
        scope,
        command.command_id,
        callback,
        callback_payload,
        writer_lease=lease,
        callback_verifier=verifier,
    )


class _ControlledClock:
    def __init__(self, now_ns):
        self.now_ns = now_ns

    def time_ns(self):
        return self.now_ns


@pytest.mark.unit
def test_v4_execution_store_migrates_to_verified_callback_ledger_v8(tmp_path):
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
        legacy_reservation = _insert_preseed_identity(
            store,
            scope,
            "legacy-managed-intent",
            _runtime_id("legacy-managed-intent"),
        )
    finally:
        store.close()

    # A v4 database has the execution journal, event outbox, lease, and CTP
    # identity tables. The command and OrderRef watermark tables were added in
    # v5 added commands, v6 added one-use authority, v7 adds typed
    # per-action/session keys, and v8 adds callback ledger/projections.
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
        assert version == "10"
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
        migrated_account_watermark = migrated._connection.execute(
            """
            SELECT watermark_order_ref, cutover_established
            FROM ctp_order_ref_account_watermarks WHERE account_key = ?
            """,
            (scope.account_key,),
        ).fetchone()
        assert tuple(migrated_account_watermark) == ("000000000001", 0)
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
        assert version == "10"
        assert "ctp_dispatch_authority_uses" in tables
    finally:
        migrated.close()


@pytest.mark.unit
def test_v6_staged_command_without_typed_keys_migrates_to_unknown(tmp_path):
    path = tmp_path / "execution.sqlite3"
    store = SqliteExecutionStore(path)
    scope = _scope()
    lease = _lease(store, scope)
    reservation = _reserve_seeded(store, scope, lease)
    staged = _stage_submit(store, scope, lease, reservation)
    store.close()

    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP INDEX ctp_dispatch_session_request_unique")
        connection.execute("DROP INDEX ctp_dispatch_managed_action_unique")
        connection.execute("DROP TRIGGER ctp_dispatch_commands_immutable")
        for column in (
            "native_action_ref",
            "native_request_id",
            "dispatch_session_id",
            "dispatch_front_id",
            "session_generation_id",
            "managed_action_id",
            "runtime_order_id",
            "correlation_version",
        ):
            connection.execute(f"ALTER TABLE ctp_dispatch_commands DROP COLUMN {column}")
        connection.execute("UPDATE execution_meta SET value = '6' WHERE key = 'schema_version'")
        connection.commit()
    finally:
        connection.close()

    migrated = SqliteExecutionStore(path)
    try:
        row = migrated.read_ctp_dispatch_command(scope, staged.command_id)
        assert row is not None
        assert row.status == "UNKNOWN"
        assert row.correlation_key is None
        assert row.unknown_reason == "schema_upgrade_requires_ctp_orderref_cutover"
        assert (
            migrated.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=_lease(migrated, scope),
                authority_verifier=_authority_verifier(),
            )
            is None
        )
        assert migrated.read_ctp_dispatch_command(scope, staged.command_id).status == "UNKNOWN"
    finally:
        migrated.close()


@pytest.mark.unit
def test_v7_typed_command_migrates_to_callback_ledger_without_reopening_dispatch(tmp_path):
    path = tmp_path / "execution.sqlite3"
    store = SqliteExecutionStore(path)
    scope = _scope()
    lease = _lease(store, scope)
    reservation = _reserve_seeded(store, scope, lease)
    staged = _stage_submit(store, scope, lease, reservation)
    original_key = staged.correlation_key
    store.close()

    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            DROP TRIGGER ctp_dispatch_callback_immutable_update;
            DROP TRIGGER ctp_dispatch_callback_immutable_delete;
            DROP TRIGGER ctp_dispatch_resolution_immutable_update;
            DROP TRIGGER ctp_dispatch_resolution_immutable_delete;
            DROP TABLE ctp_dispatch_unknown_resolutions;
            DROP TABLE ctp_dispatch_cancel_projection;
            DROP TABLE ctp_dispatch_order_projection;
            DROP TABLE ctp_dispatch_callback_ledger;
            UPDATE execution_meta SET value = '7' WHERE key = 'schema_version';
            """
        )
        connection.commit()
    finally:
        connection.close()

    migrated = SqliteExecutionStore(path)
    try:
        current = migrated.read_ctp_dispatch_command(scope, staged.command_id)
        assert current is not None
        assert current.status == "UNKNOWN"
        assert current.correlation_key == original_key
        assert (
            migrated._connection.execute(
                "SELECT value FROM execution_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            == "10"
        )
        assert (
            migrated._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 0
        )
    finally:
        migrated.close()


@pytest.mark.unit
def test_v8_store_migrates_orderref_cutover_state_fail_closed(tmp_path):
    path = tmp_path / "execution.sqlite3"
    store = SqliteExecutionStore(path)
    scope = _scope()
    lease = _lease(store, scope)
    reservation = _reserve_seeded(store, scope, lease)
    staged = _stage_submit(store, scope, lease, reservation)
    store.close()

    # Remove only v9 cutover structures to model an existing v8 database.
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            DROP TABLE ctp_order_ref_legacy_imports;
            DROP TABLE ctp_order_ref_cutover_sessions;
            DROP TABLE ctp_order_ref_account_watermarks;
            UPDATE execution_meta SET value = '8' WHERE key = 'schema_version';
            """
        )
        connection.commit()
    finally:
        connection.close()

    migrated = SqliteExecutionStore(path)
    try:
        version = migrated._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()["value"]
        assert version == "10"
        command = migrated.read_ctp_dispatch_command(scope, staged.command_id)
        assert command is not None
        assert command.status == "UNKNOWN"
        assert command.unknown_reason == "schema_upgrade_requires_ctp_orderref_cutover"
        watermark = migrated._connection.execute(
            """
            SELECT watermark_order_ref, cutover_established, last_trading_day,
                   last_cutover_evidence_sha256
            FROM ctp_order_ref_account_watermarks WHERE account_key = ?
            """,
            (scope.account_key,),
        ).fetchone()
        assert tuple(watermark) == (reservation.order_ref, 0, None, None)
        assert (
            migrated._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_ref_cutover_sessions"
            ).fetchone()[0]
            == 0
        )
        assert (
            migrated.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=_lease(migrated, scope),
                authority_verifier=_authority_verifier(),
            )
            is None
        )
        assert migrated.read_ctp_dispatch_command(scope, staged.command_id).status == "UNKNOWN"
    finally:
        migrated.close()


@pytest.mark.unit
def test_v7_schema_migration_rolls_back_ddl_version_and_trigger_on_failure(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    connection = store._connection
    try:
        connection.executescript(
            """
            DROP TRIGGER ctp_dispatch_callback_immutable_update;
            DROP TRIGGER ctp_dispatch_callback_immutable_delete;
            DROP TRIGGER ctp_dispatch_resolution_immutable_update;
            DROP TRIGGER ctp_dispatch_resolution_immutable_delete;
            DROP TABLE ctp_dispatch_unknown_resolutions;
            DROP TABLE ctp_dispatch_cancel_projection;
            DROP TABLE ctp_dispatch_order_projection;
            DROP TABLE ctp_dispatch_callback_ledger;
            UPDATE execution_meta SET value = '7' WHERE key = 'schema_version';
            """
        )
        schema_before = tuple(
            tuple(row)
            for row in connection.execute(
                """
                SELECT type, name, tbl_name, sql FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name
                """
            ).fetchall()
        )
        denied_triggers = []

        def deny_command_trigger_recreate(action, name, _table, _database, _source):
            if (
                action == sqlite3.SQLITE_CREATE_TRIGGER
                and name == "ctp_dispatch_commands_immutable"
            ):
                denied_triggers.append(name)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(deny_command_trigger_recreate)
        with pytest.raises(DurableStoreError, match="transaction failed"):
            store._create_schema()
        connection.set_authorizer(None)

        schema_after = tuple(
            tuple(row)
            for row in connection.execute(
                """
                SELECT type, name, tbl_name, sql FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name
                """
            ).fetchall()
        )
        assert denied_triggers == ["ctp_dispatch_commands_immutable"]
        assert schema_after == schema_before
        assert (
            connection.execute(
                "SELECT value FROM execution_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            == "7"
        )
    finally:
        connection.set_authorizer(None)
        store.close()


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
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_ref_account_watermarks"
            ).fetchone()[0]
            == 0
        )
        assert (
            store._connection.execute("SELECT COUNT(*) FROM ctp_order_ref_legacy_imports").fetchone()[0]
            == 0
        )

        store._connection.execute("DROP TRIGGER reject_seeded_reservation")
        reservation = _reserve_seeded(store, scope, lease)
        assert reservation.order_ref == "000000000013"
        assert _reserve_seeded(store, scope, lease) == reservation
        imported = store.read_ctp_order_identity(
            ExecutionScope("CTP", "simulation", "acct-outbox", "strategy.legacy", "20260924"),
            "legacy-managed-intent",
        )
        assert imported is not None and imported.order_ref == "000000000012"
        account_watermark = store._connection.execute(
            """
            SELECT watermark_order_ref, cutover_established, last_trading_day
            FROM ctp_order_ref_account_watermarks WHERE account_key = ?
            """,
            (scope.account_key,),
        ).fetchone()
        assert tuple(account_watermark) == ("000000000013", 1, "20260925")
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_ref_cutover_sessions"
            ).fetchone()[0]
            == 1
        )
    finally:
        store.close()


def _prepared_managed_dispatch(reservation, *, operation="submit", receipt_id="a" * 32):
    managed_cancel_id = None if operation == "submit" else "cancel." + reservation.managed_intent_id
    command_id = stable_managed_command_id(
        operation,
        reservation.managed_intent_id,
        reservation.runtime_order_id,
        managed_cancel_id,
    )
    target = {
        "OrderRef": reservation.order_ref,
        "InstrumentID": "rb2710",
    }
    kwargs = {}
    if operation == "cancel":
        target.update(
            ExchangeID="SHFE",
            OrderSysID="fake-sys-order",
            FrontID=4,
            SessionID=91,
            ActionFlag="0",
        )
        kwargs = {
            "managed_cancel_intent_id": managed_cancel_id,
            "native_action_ref": "fake-action-ref",
            "cancel_target_exchange_id": "SHFE",
            "cancel_target_order_sys_id": "fake-sys-order",
            "cancel_target_front_id": 4,
            "cancel_target_session_id": 91,
        }
    return CtpManagedPreparedDispatch(
        operation=operation,
        command_id=command_id,
        managed_intent_id=reservation.managed_intent_id,
        runtime_order_id=reservation.runtime_order_id,
        order_ref=reservation.order_ref,
        request_payload=target,
        order_ref_reservation=reservation,
        approval_use_id="approval-use-" + operation,
        approval_digest=sha256(b"fake approval digest").hexdigest(),
        session_binding=_session_binding(),
        session_generation_id="test-session-generation",
        dispatch_front_id=4,
        dispatch_session_id=91,
        native_request_id=_native_request_id(command_id),
        local_queue_receipt_id=receipt_id,
        **kwargs,
    )


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["submit", "cancel"])
async def test_single_worker_requires_persisted_queue_receipt_and_sends_once(
    tmp_path, operation
):
    store = SqliteExecutionStore(tmp_path / (operation + "-single-worker.sqlite3"))
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        prepared = _prepared_managed_dispatch(reservation, operation=operation)
        worker = CtpManagedSingleWorkerCandidate(
            store, scope, lease, _authority_verifier()
        )
        staged_binding = worker.stage_prepared_dispatch(prepared)
        sent = []

        async def fake_sender(command):
            sent.append(command.command_id)
            return CtpNativeDispatchResult(
                "QUEUED", {"kind": "fake-native-return", "return_code": 0}
            )

        with pytest.raises(ContractValidationError, match="queue receipt is not committed"):
            await worker.dispatch_managed_command(
                prepared.command_id, staged_binding, fake_sender
            )
        assert sent == []
        with pytest.raises(ContractValidationError, match="does not match staged command"):
            worker.record_managed_queue_receipt(
                prepared.command_id,
                staged_binding,
                {
                    "kind": "command_receipt",
                    "command": operation,
                    "receipt_id": "f" * 32,
                    "queued": True,
                },
            )
        assert store.read_ctp_dispatch_command(scope, prepared.command_id).local_queue_receipt_queued is None

        ready_binding = worker.record_managed_queue_receipt(
            prepared.command_id,
            staged_binding,
            {
                "kind": "command_receipt",
                "command": operation,
                "receipt_id": prepared.local_queue_receipt_id,
                "queued": True,
            },
        )
        assert ready_binding.local_queue_receipt_id == prepared.local_queue_receipt_id
        assert ready_binding.local_queue_receipt_queued is True
        payload = ready_binding.to_payload()
        assert payload["request_payload"]["OrderRef"] == reservation.order_ref
        assert payload["session_binding"]["session_generation_id"] == prepared.session_generation_id
        assert payload["approval_use_id"] == prepared.approval_use_id
        assert payload["managed_action_id"] == (
            prepared.managed_cancel_intent_id or prepared.managed_intent_id
        )

        projection = await worker.dispatch_managed_command(
            prepared.command_id, ready_binding, fake_sender
        )
        assert sent == [prepared.command_id]
        assert projection.command_status == "COMPLETED"
        assert projection.local_dispatch_outcome == "QUEUED"
        assert projection.local_queue_receipt_id == prepared.local_queue_receipt_id
        assert projection.local_queue_receipt_queued is True
        assert projection.submit_action is None or projection.submit_action.order_state.provider_state is None
        replay = await worker.dispatch_managed_command(
            prepared.command_id, ready_binding, fake_sender
        )
        assert replay == projection
        assert sent == [prepared.command_id]
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_single_worker_rejects_orderref_mismatch_before_native_sender(tmp_path):
    store = SqliteExecutionStore(tmp_path / "mismatch-single-worker.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        prepared = _prepared_managed_dispatch(reservation)
        worker = CtpManagedSingleWorkerCandidate(
            store, scope, lease, _authority_verifier()
        )
        wrong_source_reservation = replace(
            reservation, created_at_ns=reservation.created_at_ns + 1
        )
        with pytest.raises(ContractValidationError, match="same-store reservation"):
            worker.stage_prepared_dispatch(
                replace(prepared, order_ref_reservation=wrong_source_reservation)
            )
        assert store.read_ctp_dispatch_command(scope, prepared.command_id) is None
        bad_request = replace(
            prepared,
            request_payload={**prepared.request_payload, "OrderRef": "999999999999"},
        )
        with pytest.raises(ContractValidationError, match="request OrderRef differs"):
            worker.stage_prepared_dispatch(bad_request)
        assert store.read_ctp_dispatch_command(scope, prepared.command_id) is None
        binding = worker.stage_prepared_dispatch(prepared)
        ready_binding = worker.record_managed_queue_receipt(
            prepared.command_id,
            binding,
            {
                "kind": "command_receipt",
                "command": "submit",
                "receipt_id": prepared.local_queue_receipt_id,
                "queued": True,
            },
        )
        sent = []

        async def fake_sender(command):
            sent.append(command.command_id)
            return CtpNativeDispatchResult("QUEUED", {"return_code": 0})

        mismatched_binding = replace(ready_binding, order_ref="999999999999")
        with pytest.raises(ContractValidationError, match="differs from durable command"):
            await worker.dispatch_managed_command(
                prepared.command_id, mismatched_binding, fake_sender
            )
        assert sent == []
        assert store.read_ctp_dispatch_command(scope, prepared.command_id).status == "READY"
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_single_worker_queue_rejection_never_calls_sender(tmp_path):
    store = SqliteExecutionStore(tmp_path / "queue-reject-single-worker.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        prepared = _prepared_managed_dispatch(reservation)
        worker = CtpManagedSingleWorkerCandidate(
            store, scope, lease, _authority_verifier()
        )
        binding = worker.stage_prepared_dispatch(prepared)
        binding = worker.record_managed_queue_receipt(
            prepared.command_id,
            binding,
            {
                "kind": "command_receipt",
                "command": "submit",
                "receipt_id": prepared.local_queue_receipt_id,
                "queued": False,
            },
        )
        sent = []

        async def fake_sender(command):
            sent.append(command.command_id)
            return CtpNativeDispatchResult("QUEUED", {"return_code": 0})

        projection = await worker.dispatch_managed_command(
            prepared.command_id, binding, fake_sender
        )
        assert projection.command_status == "COMPLETED"
        assert projection.local_dispatch_outcome == "REJECTED"
        assert projection.local_queue_receipt_id == prepared.local_queue_receipt_id
        assert projection.local_queue_receipt_queued is False
        assert sent == []
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_single_worker_concurrent_duplicate_has_one_sender_invocation(tmp_path):
    store = SqliteExecutionStore(tmp_path / "concurrent-single-worker.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        prepared = _prepared_managed_dispatch(reservation)
        worker = CtpManagedSingleWorkerCandidate(
            store, scope, lease, _authority_verifier()
        )
        binding = worker.stage_prepared_dispatch(prepared)
        binding = worker.record_managed_queue_receipt(
            prepared.command_id,
            binding,
            {
                "kind": "command_receipt",
                "command": "submit",
                "receipt_id": prepared.local_queue_receipt_id,
                "queued": True,
            },
        )
        send_started = asyncio.Event()
        release_send = asyncio.Event()
        sent = []

        async def fake_sender(command):
            sent.append(command.command_id)
            send_started.set()
            await release_send.wait()
            return CtpNativeDispatchResult("QUEUED", {"return_code": 0})

        first_task = asyncio.create_task(
            worker.dispatch_managed_command(prepared.command_id, binding, fake_sender)
        )
        await send_started.wait()
        duplicate = await worker.dispatch_managed_command(
            prepared.command_id, binding, fake_sender
        )
        assert duplicate.command_status == "CLAIMED"
        assert sent == [prepared.command_id]
        release_send.set()
        final = await first_task
        assert final.command_status == "COMPLETED"
        assert sent == [prepared.command_id]
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_single_worker_sender_exception_persists_unknown_and_is_not_replayed(tmp_path):
    store = SqliteExecutionStore(tmp_path / "unknown-single-worker.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        prepared = _prepared_managed_dispatch(reservation)
        worker = CtpManagedSingleWorkerCandidate(
            store, scope, lease, _authority_verifier()
        )
        binding = worker.stage_prepared_dispatch(prepared)
        ready_binding = worker.record_managed_queue_receipt(
            prepared.command_id,
            binding,
            {
                "kind": "command_receipt",
                "command": "submit",
                "receipt_id": prepared.local_queue_receipt_id,
                "queued": True,
            },
        )
        secret = "do-not-persist-this-native-exception"
        sent = []

        async def fake_sender(command):
            sent.append(command.command_id)
            raise RuntimeError(secret)

        projection = await worker.dispatch_managed_command(
            prepared.command_id, ready_binding, fake_sender
        )
        assert projection.command_status == "UNKNOWN"
        assert projection.local_dispatch_outcome == "UNKNOWN"
        assert projection.local_queue_receipt_id == prepared.local_queue_receipt_id
        command = store.read_ctp_dispatch_command(scope, prepared.command_id)
        assert command.native_receipt_payload == {
            "kind": "native_dispatch",
            "outcome": "UNKNOWN",
        }
        assert secret not in repr(command.native_receipt_payload)
        replay = await worker.dispatch_managed_command(
            prepared.command_id, ready_binding, fake_sender
        )
        assert replay == projection
        assert sent == [prepared.command_id]
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "untrusted_receipt",
    [
        {"kind": "native-rejected", "no_send": True, "callback_count": 0},
        {"native_return_code": -1},
    ],
)
async def test_single_worker_sender_rejection_without_trusted_proof_is_unknown_and_not_replayed(
    tmp_path, untrusted_receipt
):
    store = SqliteExecutionStore(tmp_path / "untrusted-reject-single-worker.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        prepared = _prepared_managed_dispatch(reservation)
        worker = CtpManagedSingleWorkerCandidate(store, scope, lease, _authority_verifier())
        binding = worker.stage_prepared_dispatch(prepared)
        ready_binding = worker.record_managed_queue_receipt(
            prepared.command_id,
            binding,
            {
                "kind": "command_receipt",
                "command": "submit",
                "receipt_id": prepared.local_queue_receipt_id,
                "queued": True,
            },
        )
        sent = []

        async def fake_sender(command):
            sent.append(command.command_id)
            return CtpNativeDispatchResult("REJECTED", untrusted_receipt)

        projection = await worker.dispatch_managed_command(
            prepared.command_id, ready_binding, fake_sender
        )
        assert projection.command_status == "UNKNOWN"
        assert projection.local_dispatch_outcome == "UNKNOWN"
        assert projection.local_queue_receipt_queued is True
        command = store.read_ctp_dispatch_command(scope, prepared.command_id)
        assert command.native_receipt_payload == {
            "kind": "native_dispatch",
            "outcome": "UNKNOWN",
        }

        replay = await worker.dispatch_managed_command(
            prepared.command_id, ready_binding, fake_sender
        )
        assert replay == projection
        assert sent == [prepared.command_id]
    finally:
        store.close()


@pytest.mark.unit
def test_cutover_import_and_account_watermark_survive_restart(tmp_path):
    path = tmp_path / "execution.sqlite3"
    scope = _scope()
    store = SqliteExecutionStore(path)
    try:
        lease = _lease(store, scope)
        first = _reserve_seeded(store, scope, lease, "first-cutover-intent")
        assert first.order_ref == "000000000013"
    finally:
        store.close()

    reopened = SqliteExecutionStore(path)
    try:
        lease = _lease(reopened, scope)
        second = _reserve_seeded(reopened, scope, lease, "second-cutover-intent")
        assert second.order_ref == "000000000014"
        row = reopened._connection.execute(
            "SELECT watermark_order_ref, cutover_established "
            "FROM ctp_order_ref_account_watermarks WHERE account_key = ?",
            (scope.account_key,),
        ).fetchone()
        assert tuple(row) == ("000000000014", 1)
        assert (
            reopened._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_ref_legacy_imports"
            ).fetchone()[0]
            == 1
        )
    finally:
        reopened.close()


@pytest.mark.unit
def test_cutover_requires_exact_scope_and_complete_legacy_mapping_manifest(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    proof = _proof(scope)
    try:
        wrong_scope_proof = _proof(
            ExecutionScope("CTP", "simulation", "acct-outbox", "strategy.other", "20260925")
        )
        with pytest.raises(ContractValidationError, match="exact account/scope/day"):
            store.seed_ctp_order_ref_and_reserve_identity(
                scope,
                wrong_scope_proof,
                "intent-1",
                _runtime_id("intent-1"),
                writer_lease=lease,
            )
        with pytest.raises(ContractValidationError, match="complete imported mapping set"):
            store.seed_ctp_order_ref_and_reserve_identity(
                scope,
                replace(proof, legacy_mappings=()),
                "intent-1",
                _runtime_id("intent-1"),
                writer_lease=lease,
            )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_identity_reservations"
            ).fetchone()[0]
            == 0
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_ref_cutover_sessions"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


@pytest.mark.unit
def test_cutover_rejects_stale_native_floor_and_omitted_imported_mapping(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        proof = _proof(scope)
        first_mapping = proof.legacy_mappings[0]
        older_mapping = CtpOrderRefLegacyMapping(
            source_name="backtrader_prototype",
            account_key=scope.account_key,
            trading_day="20260924",
            scope_key=first_mapping.scope_key,
            managed_intent_id="legacy-older-intent",
            runtime_order_id=_runtime_id("legacy-older-intent"),
            order_ref="000000000011",
        )
        complete_proof = replace(
            proof,
            legacy_mappings=(older_mapping, first_mapping),
        )
        first = store.seed_ctp_order_ref_and_reserve_identity(
            scope,
            complete_proof,
            "first-cutover-intent",
            _runtime_id("first-cutover-intent"),
            writer_lease=lease,
        )
        assert first.order_ref == "000000000013"

        stale_proof = replace(
            _proof(scope, session_generation_id="stale-native-session"),
            native_max_order_ref="000000000009",
            existing_native_order_refs=("000000000008", "000000000009"),
        )
        with pytest.raises(IntentConflictError, match="cannot move backward"):
            store.seed_ctp_order_ref_and_reserve_identity(
                scope,
                stale_proof,
                "stale-intent",
                _runtime_id("stale-intent"),
                writer_lease=lease,
            )

        incomplete_proof = replace(
            _proof(scope, session_generation_id="omitted-legacy-session"),
            legacy_mappings=(first_mapping,),
        )
        with pytest.raises(IntentConflictError, match="omits an imported mapping"):
            store.seed_ctp_order_ref_and_reserve_identity(
                scope,
                incomplete_proof,
                "omitted-intent",
                _runtime_id("omitted-intent"),
                writer_lease=lease,
            )

        assert (
            store._connection.execute(
                "SELECT watermark_order_ref FROM ctp_order_ref_account_watermarks "
                "WHERE account_key = ?",
                (scope.account_key,),
            ).fetchone()[0]
            == "000000000013"
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_ref_cutover_sessions"
            ).fetchone()[0]
            == 1
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_identity_reservations "
                "WHERE managed_intent_id IN ('stale-intent', 'omitted-intent')"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


@pytest.mark.unit
def test_claim_requires_the_cutover_native_session_to_match_command_session(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        staged = _stage_submit(
            store,
            scope,
            lease,
            reservation,
            session_generation_id="different-native-session",
        )
        with pytest.raises(ContractValidationError, match="exact current OrderRef cutover session"):
            store.claim_ctp_dispatch_command(
                scope,
                staged.command_id,
                writer_lease=lease,
                authority_verifier=_authority_verifier(),
            )
        assert store.read_ctp_dispatch_command(scope, staged.command_id).status == "READY"
    finally:
        store.close()


@pytest.mark.unit
def test_superseded_session_proof_cannot_reactivate_or_claim_old_session(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    original_proof = _proof(scope)
    try:
        reservation = store.seed_ctp_order_ref_and_reserve_identity(
            scope,
            original_proof,
            "session-a-intent",
            _runtime_id("session-a-intent"),
            writer_lease=lease,
        )
        old_session_command = _stage_submit(
            store,
            scope,
            lease,
            reservation,
            command_id="session-a-command",
        )
        replacement_proof = _proof(
            scope,
            session_generation_id="replacement-session-b",
        )
        store.record_ctp_order_ref_seed(scope, replacement_proof, writer_lease=lease)
        # The current proof remains idempotent, while the superseded one does not.
        store.record_ctp_order_ref_seed(scope, replacement_proof, writer_lease=lease)

        with pytest.raises(IntentConflictError, match="session evidence was superseded"):
            store.record_ctp_order_ref_seed(scope, original_proof, writer_lease=lease)

        with pytest.raises(IntentConflictError, match="session evidence was superseded"):
            store.seed_ctp_order_ref_and_reserve_identity(
                scope,
                original_proof,
                "session-a-replay-intent",
                _runtime_id("session-a-replay-intent"),
                writer_lease=lease,
            )
        with pytest.raises(
            ContractValidationError,
            match="exact current OrderRef cutover session",
        ):
            store.claim_ctp_dispatch_command(
                scope,
                old_session_command.command_id,
                writer_lease=lease,
                authority_verifier=_authority_verifier(),
            )

        assert (
            store.read_ctp_dispatch_command(scope, old_session_command.command_id).status
            == "READY"
        )
        active = store._connection.execute(
            """
            SELECT last_trading_day, last_cutover_evidence_sha256
            FROM ctp_order_ref_account_watermarks WHERE account_key = ?
            """,
            (scope.account_key,),
        ).fetchone()
        assert active["last_trading_day"] == scope.trading_day
        replacement = store._connection.execute(
            """
            SELECT evidence_sha256 FROM ctp_order_ref_cutover_sessions
            WHERE account_key = ? AND trading_day = ? AND scope_key = ?
              AND session_generation_id = ?
            """,
            (
                scope.account_key,
                scope.trading_day,
                scope.key,
                replacement_proof.session_generation_id,
            ),
        ).fetchone()
        assert active["last_cutover_evidence_sha256"] == replacement["evidence_sha256"]
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_identity_reservations "
                "WHERE managed_intent_id = 'session-a-replay-intent'"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


@pytest.mark.unit
def test_account_orderref_active_trading_day_never_moves_backwards(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    first_scope = _scope()
    first_lease = _lease(store, first_scope)
    first_proof = _proof(first_scope)
    second_scope = ExecutionScope(
        "CTP", "simulation", first_scope.account_ref, "strategy.next-day", "20260926"
    )
    try:
        first = store.seed_ctp_order_ref_and_reserve_identity(
            first_scope,
            first_proof,
            "first-day-intent",
            _runtime_id("first-day-intent"),
            writer_lease=first_lease,
        )
        second = store.seed_ctp_order_ref_and_reserve_identity(
            second_scope,
            _proof(second_scope, session_generation_id="next-trading-day-session"),
            "second-day-intent",
            _runtime_id("second-day-intent"),
            writer_lease=first_lease,
        )
        assert first.order_ref == "000000000013"
        assert second.order_ref == "000000000014"

        with pytest.raises(IntentConflictError, match="active trading day cannot move backward"):
            store.seed_ctp_order_ref_and_reserve_identity(
                first_scope,
                first_proof,
                "stale-first-day-intent",
                _runtime_id("stale-first-day-intent"),
                writer_lease=first_lease,
            )

        active_day = store._connection.execute(
            "SELECT last_trading_day FROM ctp_order_ref_account_watermarks "
            "WHERE account_key = ?",
            (first_scope.account_key,),
        ).fetchone()[0]
        assert active_day == second_scope.trading_day
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_order_identity_reservations "
                "WHERE managed_intent_id = 'stale-first-day-intent'"
            ).fetchone()[0]
            == 0
        )
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
def test_submit_correlation_binds_runtime_action_and_session_keys(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        command = _stage_submit(store, scope, lease, reservation)
        key = command.correlation_key
        assert isinstance(key, CtpDispatchCorrelationKey)
        assert key.runtime_order_id == reservation.runtime_order_id
        assert key.managed_action_id == reservation.managed_intent_id
        assert key.order_ref == reservation.order_ref
        assert key.session_generation_id == "test-session-generation"
        assert (key.dispatch_front_id, key.dispatch_session_id) == (4, 91)
        assert key.native_request_id == _native_request_id(command.command_id)
        assert command.status == "READY"
        assert require_ctp_dispatch_callback_match(command, _callback(command))
        assert store.read_ctp_dispatch_command(scope, command.command_id).correlation_key == key
    finally:
        store.close()


@pytest.mark.unit
def test_native_callback_envelope_keeps_default_store_verifier_fail_closed(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        session_epoch = ctp_native_session_epoch()
        generation = ctp_native_session_generation_id(3, 7, session_epoch, 4, 91)
        reservation = _reserve_seeded(
            store, scope, lease, session_generation_id=generation
        )
        command = _stage_submit(
            store,
            scope,
            lease,
            reservation,
            session_generation_id=generation,
        )
        _dispatch_fake(store, scope, lease, command)
        correlation = command.correlation_key
        assert correlation is not None
        session = CtpNativeSessionContext(
            account_key=correlation.account_key,
            scope_key=correlation.scope_key,
            trading_day=correlation.trading_day,
            session_epoch=session_epoch,
            session_generation_id=correlation.session_generation_id,
            account_fingerprint="acct_" + sha256(b"9999:investor").hexdigest()[:16],
            native_api_generation=3,
            connection_generation=7,
            dispatch_front_id=correlation.dispatch_front_id,
            dispatch_session_id=correlation.dispatch_session_id,
        )
        callback = map_ctp_native_order_return(
            correlation,
            session,
            {
                "BrokerID": "9999",
                "InvestorID": "investor",
                "UserID": "investor",
                "InstrumentID": "rb2710",
                "RequestID": correlation.native_request_id,
                "OrderRef": correlation.order_ref,
                "ExchangeID": "SHFE",
                "OrderSysID": "sys-order-17",
                "FrontID": correlation.dispatch_front_id,
                "SessionID": correlation.dispatch_session_id,
                "TradingDay": correlation.trading_day,
                "NotifySequence": 7,
            },
        )

        with pytest.raises(ContractValidationError, match="trusted CTP callback verification failed"):
            store.apply_ctp_verified_dispatch_callback(
                scope,
                command.command_id,
                callback.callback_key,
                callback.to_payload(),
                writer_lease=lease,
            )
        assert store.read_ctp_dispatch_command(scope, command.command_id).status == "COMPLETED"
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
        ).fetchone()[0] == 0
        assert store._connection.execute(
            "SELECT COUNT(*) FROM ctp_dispatch_order_projection"
        ).fetchone()[0] == 0
    finally:
        store.close()


@pytest.mark.unit
def test_callback_key_rejects_cross_scope_session_and_native_id_mismatches(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    first_scope = _scope()
    lease = _lease(store, first_scope)
    try:
        first_reservation = _reserve_seeded(store, first_scope, lease, "intent-first")
        first = _stage_submit(store, first_scope, lease, first_reservation, "command-first")
        callback = _callback(first)
        assert require_ctp_dispatch_callback_match(first, callback) == callback

        other_scope = ExecutionScope(
            "CTP", "simulation", "acct-outbox", "strategy.other", "20260925"
        )
        other_reservation = _reserve_seeded(store, other_scope, lease, "intent-other")
        other = _stage_submit(
            store,
            other_scope,
            lease,
            other_reservation,
            "command-other",
            approval_use_id="approval-use-other-scope",
        )
        with pytest.raises(ContractValidationError, match="correlation does not match"):
            require_ctp_dispatch_callback_match(first, _callback(other))

        wrong_generation = replace(
            callback,
            correlation_key=replace(
                first.correlation_key, session_generation_id="stale-session-generation"
            ),
        )
        with pytest.raises(ContractValidationError, match="correlation does not match"):
            require_ctp_dispatch_callback_match(first, wrong_generation)
        with pytest.raises(ContractValidationError, match="RequestID"):
            require_ctp_dispatch_callback_match(
                first, replace(callback, native_request_id=callback.native_request_id + 1)
            )
        with pytest.raises(ContractValidationError, match="OrderRef"):
            require_ctp_dispatch_callback_match(first, replace(callback, order_ref="000000000999"))
        # Matching is structural only and never changes local or provider state.
        assert store.read_ctp_dispatch_command(first_scope, first.command_id).status == "READY"
    finally:
        store.close()


@pytest.mark.unit
def test_session_generation_and_request_id_cannot_be_rebound_or_reused(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        first_reservation = _reserve_seeded(store, scope, lease, "intent-first")
        first = _stage_submit(store, scope, lease, first_reservation, "command-first")
        second_reservation = _reserve_seeded(store, scope, lease, "intent-second")

        with pytest.raises(ContractValidationError, match="session generation was reused"):
            _stage_submit(
                store,
                scope,
                lease,
                second_reservation,
                "command-other-session-binding",
                dispatch_front_id=5,
            )
        with pytest.raises(IntentConflictError, match="RequestID is already bound"):
            _stage_submit(
                store,
                scope,
                lease,
                second_reservation,
                "command-reused-request-id",
                native_request_id=first.correlation_key.native_request_id,
            )
        mismatched_binding = _session_binding()
        with pytest.raises(ContractValidationError, match="typed CTP session keys"):
            store.stage_ctp_dispatch_command(
                scope,
                "command-mismatch-session-echo",
                "SUBMIT",
                {"InstrumentID": "rb2710", "OrderRef": second_reservation.order_ref},
                approval_use_id="approval-use-mismatch-session-echo",
                approval_digest=sha256(b"mismatch session echo").hexdigest(),
                session_binding=mismatched_binding,
                writer_lease=lease,
                managed_intent_id=second_reservation.managed_intent_id,
                order_ref=second_reservation.order_ref,
                managed_action_id=second_reservation.managed_intent_id,
                session_generation_id="other-session-generation",
                dispatch_front_id=4,
                dispatch_session_id=91,
                native_request_id=_native_request_id("command-mismatch-session-echo"),
            )
        assert store.read_ctp_dispatch_command(scope, "command-first") == first
        assert store.read_ctp_dispatch_command(scope, "command-reused-request-id") is None
    finally:
        store.close()


@pytest.mark.unit
def test_cancel_correlation_keeps_action_separate_from_exact_order_target(tmp_path):
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
        command_id = "cancel-correlation-1"
        request = {
            "OrderRef": target.order_ref,
            "ExchangeID": target.exchange_id,
            "OrderSysID": target.order_sys_id,
            "FrontID": target.front_id,
            "SessionID": target.session_id,
            "ActionFlag": "0",
        }
        command = store.stage_ctp_dispatch_command(
            scope,
            command_id,
            "CANCEL",
            request,
            approval_use_id="approval-use-cancel-correlation",
            approval_digest=sha256(b"cancel correlation approval").hexdigest(),
            session_binding=_session_binding(),
            writer_lease=lease,
            cancel_target=target,
            managed_action_id="managed-cancel-action-1",
            session_generation_id="test-session-generation",
            dispatch_front_id=4,
            dispatch_session_id=91,
            native_request_id=_native_request_id(command_id),
            native_action_ref="native-action-ref-1",
        )
        key = command.correlation_key
        assert key is not None
        assert key.managed_action_id == "managed-cancel-action-1"
        assert key.managed_action_id != key.reservation_managed_intent_id
        assert key.runtime_order_id == reservation.runtime_order_id
        assert (key.order_ref, key.cancel_target_exchange_id, key.cancel_target_order_sys_id) == (
            target.order_ref,
            target.exchange_id,
            target.order_sys_id,
        )
        callback = _callback(command)
        assert require_ctp_dispatch_callback_match(command, callback) == callback
        with pytest.raises(ContractValidationError, match="target does not match"):
            _callback(command, target_session_id=callback.target_session_id + 1)
        with pytest.raises(ContractValidationError, match="ActionRef"):
            require_ctp_dispatch_callback_match(
                command, replace(callback, native_action_ref="different-action-ref")
            )
        assert store.read_ctp_dispatch_command(scope, command_id).status == "READY"
    finally:
        store.close()


@pytest.mark.unit
def test_local_rejection_does_not_become_provider_order_rejection(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        command = _stage_submit(store, scope, lease, reservation, "command-local-rejected")
        completed = _dispatch_fake(
            store,
            scope,
            lease,
            command,
            outcome="REJECTED",
            native={"queue_code": -1},
        )

        projected = store.read_ctp_dispatch_projection(scope, command.command_id)
        assert completed.status == "COMPLETED"
        assert projected is not None
        assert projected.command_status == "COMPLETED"
        assert projected.local_dispatch_outcome == "REJECTED"
        assert projected.submit_action is not None
        assert projected.submit_action.order_state.provider_state is None
        assert projected.cancel_action is None
    finally:
        store.close()


@pytest.mark.unit
def test_local_cancel_rejection_does_not_become_provider_cancel_rejection(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        submit = _stage_submit(store, scope, lease, reservation)
        _dispatch_fake(store, scope, lease, submit)
        _apply_callback(
            store,
            scope,
            submit,
            _callback(submit, event_id="submit-ack-for-local-cancel-reject"),
            lease,
            _FakeCtpDispatchCallbackVerifier("ACKNOWLEDGED"),
        )

        cancel = _stage_cancel(store, scope, lease, reservation, "cancel-local-rejected")
        completed = _dispatch_fake(
            store,
            scope,
            lease,
            cancel,
            outcome="REJECTED",
            native={"queue_code": -1},
        )
        projected = store.read_ctp_dispatch_projection(scope, cancel.command_id)

        assert completed.status == "COMPLETED"
        assert projected is not None
        assert projected.local_dispatch_outcome == "REJECTED"
        assert projected.cancel_action is not None
        assert projected.cancel_action.action_state is None
        assert projected.cancel_action.target_order.order_state.provider_state == "ACKNOWLEDGED"
    finally:
        store.close()


@pytest.mark.unit
def test_verified_submit_callback_is_durable_idempotent_and_restart_safe(tmp_path):
    path = tmp_path / "execution.sqlite3"
    store = SqliteExecutionStore(path)
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        command = _stage_submit(store, scope, lease, reservation)
        completed = _dispatch_fake(store, scope, lease, command)
        callback = _callback(command, event_id="order-event-1")
        before_verified_callback = store.read_ctp_dispatch_projection(scope, command.command_id)
        assert before_verified_callback is not None
        assert before_verified_callback.local_dispatch_outcome == "QUEUED"
        assert before_verified_callback.submit_action is not None
        assert before_verified_callback.submit_action.order_state.provider_state is None
        with pytest.raises(ContractValidationError, match="verification failed"):
            _apply_callback(store, scope, command, callback, lease)
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 0
        )
        after_unverified_callback = store.read_ctp_dispatch_projection(scope, command.command_id)
        assert after_unverified_callback is not None
        assert after_unverified_callback.local_dispatch_outcome == "QUEUED"
        assert after_unverified_callback.submit_action is not None
        assert after_unverified_callback.submit_action.order_state.provider_state is None

        applied = _apply_callback(
            store, scope, command, callback, lease, _FakeCtpDispatchCallbackVerifier()
        )
        assert not applied.duplicate
        assert applied.projection_state == "ACKNOWLEDGED"
        assert not applied.account_fence_open
        projection = store._connection.execute(
            "SELECT * FROM ctp_dispatch_order_projection WHERE account_key = ?",
            (command.account_key,),
        ).fetchone()
        ledger = store._connection.execute(
            "SELECT * FROM ctp_dispatch_callback_ledger WHERE account_key = ?",
            (command.account_key,),
        ).fetchone()
        assert projection["provider_state"] == "ACKNOWLEDGED"
        assert projection["terminal"] == 0
        changes_before_read = store._connection.total_changes
        projected = store.read_ctp_dispatch_projection(scope, command.command_id)
        assert projected is not None
        assert projected.operation == "SUBMIT"
        assert projected.command_status == "COMPLETED"
        assert projected.local_dispatch_outcome == "QUEUED"
        assert projected.submit_action is not None
        assert projected.submit_action.managed_intent_id == reservation.managed_intent_id
        assert projected.submit_action.runtime_order_id == reservation.runtime_order_id
        assert projected.submit_action.order_ref == reservation.order_ref
        assert projected.submit_action.order_state.provider_state == "ACKNOWLEDGED"
        assert projected.submit_action.order_state.source_kind == "CALLBACK"
        assert projected.cancel_action is None
        assert projected.unknown_resolution is None
        assert store._connection.total_changes == changes_before_read
        assert not hasattr(projected, "request_payload")
        assert not hasattr(projected, "session_binding")
        assert not hasattr(projected, "native_receipt_payload")
        with pytest.raises(FrozenInstanceError):
            projected.command_status = "UNKNOWN"
        assert ledger["callback_key_json"] == json.dumps(
            callback.to_payload(), sort_keys=True, separators=(",", ":")
        )
        assert ledger["source_digest_sha256"] == sha256(b"fake callback").hexdigest()
        assert completed.status == "COMPLETED"
        assert store.read_ctp_dispatch_command(scope, command.command_id).status == "COMPLETED"
    finally:
        store.close()

    reopened = SqliteExecutionStore(path)
    reopened_lease = _lease(reopened, scope)
    try:
        duplicate = _apply_callback(
            reopened,
            scope,
            command,
            callback,
            reopened_lease,
            _FakeCtpDispatchCallbackVerifier(),
        )
        assert duplicate.duplicate
        assert not duplicate.account_fence_open
        assert (
            reopened._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 1
        )
        assert (
            reopened._connection.execute(
                "SELECT provider_state FROM ctp_dispatch_order_projection"
            ).fetchone()[0]
            == "ACKNOWLEDGED"
        )
        reopened_projection = reopened.read_ctp_dispatch_projection(scope, command.command_id)
        assert reopened_projection is not None
        assert reopened_projection.local_dispatch_outcome == "QUEUED"
        assert reopened_projection.submit_action is not None
        assert reopened_projection.submit_action.order_state.provider_state == "ACKNOWLEDGED"
    finally:
        reopened.close()


@pytest.mark.unit
def test_callback_verifier_error_payload_is_not_exposed(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    secret = "SENTINEL-CALLBACK-VERIFIER-SECRET"
    try:
        reservation = _reserve_seeded(store, scope, lease)
        command = _stage_submit(store, scope, lease, reservation)
        _dispatch_fake(store, scope, lease, command)
        with pytest.raises(ContractValidationError) as caught:
            _apply_callback(
                store,
                scope,
                command,
                _callback(command),
                lease,
                _FakeCtpDispatchCallbackVerifier(error=RuntimeError(secret)),
            )
        assert secret not in str(caught.value)
        assert caught.value.__context__ is None
        assert secret not in "".join(traceback.format_exception(caught.value))
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


@pytest.mark.unit
def test_callback_verifier_must_bind_input_digest_and_fresh_expiry(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        command = _stage_submit(store, scope, lease, reservation)
        _dispatch_fake(store, scope, lease, command)
        callback = _callback(command, event_id="proof-binding-event")
        with pytest.raises(ContractValidationError, match="does not match command"):
            _apply_callback(
                store,
                scope,
                command,
                callback,
                lease,
                _FakeCtpDispatchCallbackVerifier(payload_digest_override="0" * 64),
            )
        with pytest.raises(ContractValidationError, match="expired before commit"):
            _apply_callback(
                store,
                scope,
                command,
                callback,
                lease,
                _FakeCtpDispatchCallbackVerifier(ttl_ns=1, verification_delay_s=0.002),
            )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 0
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_order_projection"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


@pytest.mark.unit
def test_callback_event_id_conflict_is_rejected_without_projection_change(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        command = _stage_submit(store, scope, lease, reservation)
        _dispatch_fake(store, scope, lease, command)
        callback = _callback(command, event_id="reused-event-id")
        _apply_callback(store, scope, command, callback, lease, _FakeCtpDispatchCallbackVerifier())
        with pytest.raises(IntentConflictError, match="event id conflicts"):
            _apply_callback(
                store,
                scope,
                command,
                callback,
                lease,
                _FakeCtpDispatchCallbackVerifier("REJECTED", source=b"different fake source"),
            )
        assert (
            store._connection.execute(
                "SELECT provider_state FROM ctp_dispatch_order_projection"
            ).fetchone()[0]
            == "ACKNOWLEDGED"
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 1
        )
    finally:
        store.close()


@pytest.mark.unit
def test_callback_ledger_and_projection_rollback_together(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        command = _stage_submit(store, scope, lease, reservation)
        _dispatch_fake(store, scope, lease, command)
        store._connection.execute(
            """
            CREATE TRIGGER fail_ctp_order_projection
            BEFORE INSERT ON ctp_dispatch_order_projection
            BEGIN
                SELECT RAISE(ABORT, 'fake projection failure');
            END
            """
        )
        callback = _callback(command, event_id="rollback-event")
        with pytest.raises(DurableStoreError, match="transaction failed"):
            _apply_callback(
                store, scope, command, callback, lease, _FakeCtpDispatchCallbackVerifier()
            )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_callback_ledger"
            ).fetchone()[0]
            == 0
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_order_projection"
            ).fetchone()[0]
            == 0
        )
        store._connection.execute("DROP TRIGGER fail_ctp_order_projection")
        assert not _apply_callback(
            store, scope, command, callback, lease, _FakeCtpDispatchCallbackVerifier()
        ).duplicate
    finally:
        store.close()


@pytest.mark.unit
def test_cancel_callback_projects_action_without_cancelling_target_order(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        submit = _stage_submit(store, scope, lease, reservation)
        _dispatch_fake(store, scope, lease, submit)
        _apply_callback(
            store,
            scope,
            submit,
            _callback(submit, event_id="submit-ack"),
            lease,
            _FakeCtpDispatchCallbackVerifier("ACKNOWLEDGED"),
        )
        cancel = _stage_cancel(store, scope, lease, reservation)
        _dispatch_fake(store, scope, lease, cancel)
        before_cancel_callback = store.read_ctp_dispatch_projection(scope, cancel.command_id)
        assert before_cancel_callback is not None
        assert before_cancel_callback.cancel_action is not None
        assert before_cancel_callback.cancel_action.action_state is None
        assert (
            before_cancel_callback.cancel_action.target_order.order_state.provider_state
            == "ACKNOWLEDGED"
        )
        applied = _apply_callback(
            store,
            scope,
            cancel,
            _callback(cancel, event_id="cancel-terminal"),
            lease,
            _FakeCtpDispatchCallbackVerifier("TERMINAL"),
        )
        assert applied.projection_state == "TERMINAL"
        order_projection = store._connection.execute(
            "SELECT provider_state FROM ctp_dispatch_order_projection WHERE account_key = ?",
            (cancel.account_key,),
        ).fetchone()
        cancel_projection = store._connection.execute(
            "SELECT provider_state, terminal FROM ctp_dispatch_cancel_projection "
            "WHERE account_key = ? AND managed_action_id = ?",
            (cancel.account_key, cancel.correlation_key.managed_action_id),
        ).fetchone()
        assert order_projection["provider_state"] == "ACKNOWLEDGED"
        assert cancel_projection["provider_state"] == "TERMINAL"
        assert cancel_projection["terminal"] == 1
        projected = store.read_ctp_dispatch_projection(scope, cancel.command_id)
        assert projected is not None
        assert projected.operation == "CANCEL"
        assert projected.command_status == "COMPLETED"
        assert projected.submit_action is None
        assert projected.cancel_action is not None
        assert projected.cancel_action.managed_action_id == cancel.correlation_key.managed_action_id
        assert projected.cancel_action.action_state == "TERMINAL"
        assert projected.cancel_action.terminal is True
        assert projected.cancel_action.target_order.managed_intent_id == reservation.managed_intent_id
        assert projected.cancel_action.target_order.runtime_order_id == reservation.runtime_order_id
        assert projected.cancel_action.target_order.order_ref == reservation.order_ref
        assert projected.cancel_action.target_order.exchange_id == "SHFE"
        assert projected.cancel_action.target_order.order_sys_id == "sys-order-17"
        assert projected.cancel_action.target_order.front_id == 4
        assert projected.cancel_action.target_order.session_id == 91
        assert projected.cancel_action.target_order.order_state.provider_state == "ACKNOWLEDGED"
        assert projected.cancel_action.target_order.order_state.source_kind == "CALLBACK"
        assert projected.cancel_action.managed_action_id != (
            projected.cancel_action.target_order.managed_intent_id
        )
    finally:
        store.close()


@pytest.mark.unit
def test_unknown_callback_stays_fenced_until_fresh_terminal_reconciliation(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    other_scope = ExecutionScope("CTP", "simulation", "acct-outbox", "strategy.other", "20260925")
    other_lease = _lease(store, other_scope)
    try:
        first_reservation = _reserve_seeded(store, scope, lease, "intent-unknown")
        unknown_command = _stage_submit(
            store,
            scope,
            lease,
            first_reservation,
            "command-unknown",
            approval_use_id="approval-unknown",
        )
        _dispatch_fake(store, scope, lease, unknown_command, outcome="UNKNOWN")
        callback_result = _apply_callback(
            store,
            scope,
            unknown_command,
            _callback(unknown_command, event_id="late-terminal-order"),
            lease,
            _FakeCtpDispatchCallbackVerifier("FILLED"),
        )
        assert callback_result.account_fence_open
        assert (
            store.read_ctp_dispatch_command(scope, unknown_command.command_id).status == "UNKNOWN"
        )
        unknown_projection = store.read_ctp_dispatch_projection(scope, unknown_command.command_id)
        assert unknown_projection is not None
        assert unknown_projection.command_status == "UNKNOWN"
        assert unknown_projection.local_dispatch_outcome == "UNKNOWN"
        assert unknown_projection.unknown_reason is not None
        assert unknown_projection.submit_action is not None
        assert unknown_projection.submit_action.order_state.provider_state == "FILLED"
        assert unknown_projection.submit_action.order_state.source_kind == "CALLBACK"
        assert unknown_projection.unknown_resolution is None
        assert store.read_ctp_dispatch_projection(other_scope, unknown_command.command_id) is None
        with pytest.raises(ContractValidationError, match="verification failed"):
            store.resolve_unknown_ctp_dispatch_command(
                scope, unknown_command.command_id, writer_lease=lease
            )

        second_reservation = _reserve_seeded(
            store,
            other_scope,
            other_lease,
            "intent-after-unknown",
            session_generation_id="new-session-generation",
        )
        next_command = _stage_submit(
            store,
            other_scope,
            other_lease,
            second_reservation,
            "command-after-unknown",
            approval_use_id="approval-after-unknown",
            session_generation_id="new-session-generation",
        )
        with pytest.raises(InvalidStateTransition, match="command-unknown"):
            store.claim_ctp_dispatch_command(
                other_scope,
                next_command.command_id,
                writer_lease=other_lease,
                authority_verifier=_authority_verifier(),
            )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_unknown_resolutions"
            ).fetchone()[0]
            == 0
        )

        resolved = store.resolve_unknown_ctp_dispatch_command(
            scope,
            unknown_command.command_id,
            writer_lease=lease,
            reconciliation_verifier=_FakeCtpDispatchReconciliationVerifier("FILLED"),
        )
        assert not resolved.account_fence_open
        assert not resolved.duplicate
        assert (
            store.read_ctp_dispatch_command(scope, unknown_command.command_id).status == "UNKNOWN"
        )
        reconciled_projection = store.read_ctp_dispatch_projection(
            scope, unknown_command.command_id
        )
        assert reconciled_projection is not None
        assert reconciled_projection.command_status == "UNKNOWN"
        assert reconciled_projection.local_dispatch_outcome == "UNKNOWN"
        assert reconciled_projection.unknown_reason == unknown_projection.unknown_reason
        assert reconciled_projection.submit_action is not None
        assert reconciled_projection.submit_action.order_state.provider_state == "FILLED"
        assert reconciled_projection.submit_action.order_state.source_kind == "RECONCILIATION"
        assert reconciled_projection.unknown_resolution is not None
        assert reconciled_projection.unknown_resolution.order_terminal_state == "FILLED"
        assert reconciled_projection.unknown_resolution.cancel_action_terminal_state is None
        assert (
            store._connection.execute(
                "SELECT provider_state FROM ctp_dispatch_order_projection WHERE account_key = ?",
                (unknown_command.account_key,),
            ).fetchone()[0]
            == "FILLED"
        )
        duplicate_resolution = store.resolve_unknown_ctp_dispatch_command(
            scope,
            unknown_command.command_id,
            writer_lease=lease,
            reconciliation_verifier=_FakeCtpDispatchReconciliationVerifier("FILLED"),
        )
        assert duplicate_resolution.duplicate
        assert not duplicate_resolution.account_fence_open
        with pytest.raises(IntentConflictError, match="resolution conflicts"):
            store.resolve_unknown_ctp_dispatch_command(
                scope,
                unknown_command.command_id,
                writer_lease=lease,
                reconciliation_verifier=_FakeCtpDispatchReconciliationVerifier(
                    "CANCELLED", source=b"different reconciliation bundle"
                ),
            )
        claimed = store.claim_ctp_dispatch_command(
            other_scope,
            next_command.command_id,
            writer_lease=other_lease,
            authority_verifier=_authority_verifier(),
        )
        assert claimed is not None and claimed.status == "CLAIMED"
    finally:
        store.close()


@pytest.mark.unit
def test_unknown_cancel_resolution_requires_terminal_action_and_target(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        reservation = _reserve_seeded(store, scope, lease)
        cancel = _stage_cancel(store, scope, lease, reservation, "cancel-unknown")
        _dispatch_fake(store, scope, lease, cancel, outcome="UNKNOWN")
        with pytest.raises(ContractValidationError, match="verification failed"):
            store.resolve_unknown_ctp_dispatch_command(
                scope,
                cancel.command_id,
                writer_lease=lease,
                reconciliation_verifier=_FakeCtpDispatchReconciliationVerifier(
                    "ACKNOWLEDGED", "TERMINAL"
                ),
            )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_unknown_resolutions"
            ).fetchone()[0]
            == 0
        )

        store._connection.execute(
            """
            CREATE TRIGGER fail_ctp_cancel_projection
            BEFORE INSERT ON ctp_dispatch_cancel_projection
            BEGIN
                SELECT RAISE(ABORT, 'fake cancel projection failure');
            END
            """
        )
        with pytest.raises(DurableStoreError, match="transaction failed"):
            store.resolve_unknown_ctp_dispatch_command(
                scope,
                cancel.command_id,
                writer_lease=lease,
                reconciliation_verifier=_FakeCtpDispatchReconciliationVerifier(
                    "CANCELLED", "TERMINAL"
                ),
            )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_unknown_resolutions"
            ).fetchone()[0]
            == 0
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_order_projection"
            ).fetchone()[0]
            == 0
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_cancel_projection"
            ).fetchone()[0]
            == 0
        )
        store._connection.execute("DROP TRIGGER fail_ctp_cancel_projection")

        resolved = store.resolve_unknown_ctp_dispatch_command(
            scope,
            cancel.command_id,
            writer_lease=lease,
            reconciliation_verifier=_FakeCtpDispatchReconciliationVerifier("CANCELLED", "TERMINAL"),
        )
        assert not resolved.account_fence_open
        assert resolved.order_terminal_state == "CANCELLED"
        assert resolved.cancel_action_terminal_state == "TERMINAL"
        order_projection = store._connection.execute(
            "SELECT provider_state, terminal FROM ctp_dispatch_order_projection"
        ).fetchone()
        cancel_projection = store._connection.execute(
            "SELECT provider_state, terminal FROM ctp_dispatch_cancel_projection"
        ).fetchone()
        assert order_projection["provider_state"] == "CANCELLED"
        assert order_projection["terminal"] == 1
        assert cancel_projection["provider_state"] == "TERMINAL"
        assert cancel_projection["terminal"] == 1
        assert store.read_ctp_dispatch_command(scope, cancel.command_id).status == "UNKNOWN"
        projected = store.read_ctp_dispatch_projection(scope, cancel.command_id)
        assert projected is not None
        assert projected.command_status == "UNKNOWN"
        assert projected.local_dispatch_outcome == "UNKNOWN"
        assert projected.cancel_action is not None
        assert projected.cancel_action.managed_action_id == cancel.correlation_key.managed_action_id
        assert projected.cancel_action.action_state == "TERMINAL"
        assert projected.cancel_action.source_kind == "RECONCILIATION"
        assert projected.cancel_action.target_order.managed_intent_id == reservation.managed_intent_id
        assert projected.cancel_action.target_order.runtime_order_id == reservation.runtime_order_id
        assert projected.cancel_action.target_order.exchange_id == "SHFE"
        assert projected.cancel_action.target_order.order_sys_id == "sys-order-17"
        assert projected.cancel_action.target_order.order_state.provider_state == "CANCELLED"
        assert projected.cancel_action.target_order.order_state.source_kind == "RECONCILIATION"
        assert projected.unknown_resolution is not None
        assert projected.unknown_resolution.order_terminal_state == "CANCELLED"
        assert projected.unknown_resolution.cancel_action_terminal_state == "TERMINAL"
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
                session_binding={
                    **_session_binding(),
                    "nested": {"access_token": "never-store-this"},
                },
                writer_lease=lease,
                managed_intent_id=reservation.managed_intent_id,
                order_ref=reservation.order_ref,
                managed_action_id=reservation.managed_intent_id,
                session_generation_id="test-session-generation",
                dispatch_front_id=4,
                dispatch_session_id=91,
                native_request_id=_native_request_id("command-session-secret"),
            )
    finally:
        store.close()


@pytest.mark.unit
def test_claim_requires_seed_and_never_claims_a_preseed_reservation(tmp_path):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope = _scope()
    lease = _lease(store, scope)
    try:
        old = _insert_preseed_identity(store, scope, "old-intent", _runtime_id("old-intent"))
        assert old is not None
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
        with pytest.raises(ContractValidationError, match="correlation echo is inconsistent"):
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
        # A queue receipt alone creates no callback-ledger or provider projection fact.
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_order_projection"
            ).fetchone()[0]
            == 0
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM ctp_dispatch_cancel_projection"
            ).fetchone()[0]
            == 0
        )
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
        assert recovered[0].correlation_key == staged.correlation_key
        assert recovered[0].native_receipt_payload is None
        assert recovered[0].unknown_reason == "claimed_without_receipt_after_writer_change"
        # The fake key matcher proves structure only; it neither resolves UNKNOWN
        # nor changes the outbox state after restart.
        assert require_ctp_dispatch_callback_match(recovered[0], _callback(recovered[0]))
        assert reopened.read_ctp_dispatch_command(scope, staged.command_id).status == "UNKNOWN"
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
            session_binding=_session_binding(),
            writer_lease=lease,
            cancel_target=target,
            managed_action_id="cancel-action-1",
            session_generation_id="test-session-generation",
            dispatch_front_id=4,
            dispatch_session_id=91,
            native_request_id=_native_request_id("cancel-1"),
            native_action_ref="native-action-1",
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
                session_binding=_session_binding(),
                writer_lease=lease,
                cancel_target=target,
                managed_action_id="cancel-action-2",
                session_generation_id="test-session-generation",
                dispatch_front_id=4,
                dispatch_session_id=91,
                native_request_id=_native_request_id("cancel-2"),
                native_action_ref="native-action-2",
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
            session_binding=_session_binding(),
            writer_lease=lease,
            cancel_target=target,
            managed_action_id="cancel-action-authority-1",
            session_generation_id="test-session-generation",
            dispatch_front_id=4,
            dispatch_session_id=91,
            native_request_id=_native_request_id("cancel-authority-1"),
            native_action_ref="native-action-authority-1",
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
                session_binding=_session_binding(),
                writer_lease=lease,
                cancel_target=target,
                managed_action_id="cancel-action-invalid-" + field,
                session_generation_id="test-session-generation",
                dispatch_front_id=4,
                dispatch_session_id=91,
                native_request_id=_native_request_id("cancel-invalid-" + field),
                native_action_ref="native-action-invalid-" + field,
            )
    finally:
        store.close()
