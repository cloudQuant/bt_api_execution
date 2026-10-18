
import pytest
import test_ctp_callback_session_owner as callback_fixtures

from bt_api_execution import (
    ContractValidationError,
    CtpCallbackIngressDrainStatus,
    CtpCallbackSessionOwnerHandle,
    InvalidStateTransition,
    SqliteExecutionStore,
    WriterLeaseUnavailable,
)


def _store_owner(tmp_path, name):
    scope = callback_fixtures._scope()
    store = SqliteExecutionStore(tmp_path / f"{name}.sqlite3")
    lease = callback_fixtures._lease(store, scope)
    owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)
    callback_fixtures._ensure_adapter(store, owner, scope, lease)
    return store, scope, lease, owner


@pytest.mark.unit
def test_callback_ingress_drain_status_covers_prepared_pending_and_active(tmp_path):
    store, scope, lease, owner = _store_owner(tmp_path, "callback-drain-empty")
    try:
        empty = store.read_ctp_callback_ingress_drain_status(owner, writer_lease=lease)
        assert type(empty) is CtpCallbackIngressDrainStatus
        assert empty.owner_handle is owner
        assert empty.owner_state == "PREPARED"
        assert (empty.last_source_sequence, empty.applied_source_sequence) == (0, 0)
        assert empty.pending_count == 0
        assert empty.drained is True

    finally:
        store.close()

    store, _scope, lease, owner = _store_owner(tmp_path, "callback-drain-pending")
    try:
        store.append_ctp_callback_ingress(
            owner,
            callback_fixtures._callback_record(
                owner.owner_intent_id,
                "OnFrontConnected",
                1,
                generation=0,
            ),
        )
        pending = store.read_ctp_callback_ingress_drain_status(owner, writer_lease=lease)
        assert pending.owner_state == "PREPARED"
        assert (pending.last_source_sequence, pending.applied_source_sequence) == (1, 0)
        assert pending.pending_count == 1
        assert pending.drained is False
    finally:
        store.close()

    store, scope, lease, owner = _store_owner(tmp_path, "callback-drain-active")
    try:
        callback_fixtures._append_login_facts(store, owner)
        store.bind_ctp_callback_session(
            scope,
            owner,
            callback_fixtures._LoginObservation(),
            callback_fixtures._SourceTags(),
            3,
            writer_lease=lease,
        )
        store.append_ctp_callback_ingress(
            owner,
            callback_fixtures._callback_record(
                owner.owner_intent_id,
                "OnRtnInstrumentStatus",
                4,
                generation=1,
                phase="ACTIVE",
                callback_class="AUDIT_INFORMATIONAL",
                fields=((0, "InstrumentID", "rb2710"), (0, "InstrumentStatus", 1)),
            ),
        )
        event = store.read_next_ctp_callback_ingress(owner)
        assert event is not None and event.source_sequence == 4
        store.mark_ctp_callback_ingress_audit(scope, event, writer_lease=lease)

        active = store.read_ctp_callback_ingress_drain_status(owner, writer_lease=lease)
        assert active.owner_handle is owner
        assert active.owner_state == "ACTIVE"
        assert (active.last_source_sequence, active.applied_source_sequence) == (4, 4)
        assert active.pending_count == 0
        assert active.drained is True
    finally:
        store.close()


@pytest.mark.unit
def test_callback_ingress_drain_status_rejects_foreign_handle_and_stale_lease(tmp_path):
    store, scope, lease, owner = _store_owner(tmp_path, "callback-drain-fence")
    try:
        foreign_handle = CtpCallbackSessionOwnerHandle(
            owner_intent_id=owner.owner_intent_id,
            account_key=owner.account_key,
            scope_key=owner.scope_key,
        )
        assert foreign_handle == owner and foreign_handle is not owner
        with pytest.raises(ContractValidationError, match="same-Store"):
            store.read_ctp_callback_ingress_drain_status(
                foreign_handle, writer_lease=lease
            )

        renewed_same_generation = store.acquire_or_renew_lease(
            scope,
            lease.owner_id,
            ttl_ns=120_000_000_000,
            ctp_account_family_owner=store._test_ctp_account_family_owner,
        )
        assert renewed_same_generation.fencing_token == lease.fencing_token
        assert renewed_same_generation.expires_at_ns > lease.expires_at_ns
        retained_runtime_status = store.read_ctp_callback_ingress_drain_status(
            owner, writer_lease=lease
        )
        assert retained_runtime_status.drained is True

        assert store.release_lease(
            scope,
            renewed_same_generation.owner_id,
            fencing_token=renewed_same_generation.fencing_token,
        )
        renewed = store.acquire_or_renew_lease(
            scope,
            lease.owner_id,
            ttl_ns=60_000_000_000,
            ctp_account_family_owner=store._test_ctp_account_family_owner,
        )
        assert renewed.fencing_token == lease.fencing_token + 1
        with pytest.raises(WriterLeaseUnavailable):
            store.read_ctp_callback_ingress_drain_status(owner, writer_lease=lease)
    finally:
        store.close()


@pytest.mark.unit
def test_callback_ingress_drain_status_reports_only_dual_owner_stop_poison(tmp_path):
    store, scope, lease, owner = _store_owner(tmp_path, "callback-drain-owner-stop")
    try:
        callback_fixtures._append_login_facts(store, owner)
        store.bind_ctp_callback_session(
            scope,
            owner,
            callback_fixtures._LoginObservation(),
            callback_fixtures._SourceTags(),
            3,
            writer_lease=lease,
        )

        committed = store.poison_ctp_callback_session_owner(owner, "owner_stop")
        assert committed.durable_state == "POISONED"
        assert committed.poison_code == "owner_stop"
        stopped = store.read_ctp_callback_ingress_drain_status(owner, writer_lease=lease)
        assert type(stopped) is CtpCallbackIngressDrainStatus
        assert stopped.owner_handle is owner
        assert stopped.owner_state == "POISONED"
        assert stopped.poison_code == "owner_stop"
        assert (stopped.last_source_sequence, stopped.applied_source_sequence) == (3, 3)
        assert stopped.pending_count == 0
        assert stopped.drained is True
    finally:
        store.close()


@pytest.mark.unit
def test_callback_ingress_drain_status_rejects_other_first_poison(tmp_path):
    store, _scope, lease, owner = _store_owner(tmp_path, "callback-drain-other-poison")
    try:
        first = store.poison_ctp_callback_session_owner(owner, "disconnect")
        later = store.poison_ctp_callback_session_owner(owner, "owner_stop")
        assert first.durable_state == later.durable_state == "POISONED"
        assert first.poison_code == later.poison_code == "disconnect"
        with pytest.raises(InvalidStateTransition, match="outside normal stop"):
            store.read_ctp_callback_ingress_drain_status(owner, writer_lease=lease)
    finally:
        store.close()
