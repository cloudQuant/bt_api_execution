from __future__ import annotations

import sqlite3
from decimal import Decimal
from hashlib import sha256

import pytest

from bt_api_execution import (
    CancelIntent,
    ContractValidationError,
    ExecutionScope,
    ExecutionState,
    InvalidStateTransition,
    ManagedExecutionFacade,
    OrderIntent,
    PositionEffect,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
    WriterLeaseUnavailable,
)


def _account_ref(label: str = "family-test") -> str:
    return "ctp-account-ref.v1:" + sha256(label.encode("ascii")).hexdigest()


def _ctp_scope(
    *, environment: str = "simulation", strategy: str = "strategy.one", day: str = "20260925"
) -> ExecutionScope:
    return ExecutionScope("ctp", environment, _account_ref(), strategy, trading_day=day)


def _lease(store: SqliteExecutionStore, scope: ExecutionScope, owner: str = "actor.one"):
    family_owner = store.acquire_ctp_account_family_owner(scope)
    return store.acquire_or_renew_lease(
        scope,
        owner,
        ttl_ns=30_000_000_000,
        ctp_account_family_owner=family_owner,
    )


def _order(scope: ExecutionScope, intent_id: str) -> OrderIntent:
    return OrderIntent.limit(
        intent_id=intent_id,
        scope=scope,
        signal_id="signal-" + intent_id,
        instrument="rb2710",
        side=Side.BUY,
        position_effect=PositionEffect.OPEN,
        quantity=Decimal("1"),
        price=Decimal("100"),
    )


def _persist_unknown_cancel(
    store: SqliteExecutionStore,
    scope: ExecutionScope,
    lease,
    *,
    intent_id: str = "cancel-target",
    cancel_id: str = "cancel-unknown",
) -> None:
    intent = _order(scope, intent_id)
    store.admit_intent(intent, writer_lease=lease)
    store.activate_intent(intent_id, scope, writer_lease=lease)
    _, claimed = store.claim_for_dispatch(intent_id, scope, writer_lease=lease)
    assert claimed
    store.record_observation(
        ProviderObservation.accepted(intent_id, "provider-order-1"),
        scope=scope,
        writer_lease=lease,
    )
    cancel = CancelIntent(
        cancel_id=cancel_id,
        scope=scope,
        target_intent_id=intent_id,
        provider_order_id="provider-order-1",
    )
    store.admit_cancel(cancel, writer_lease=lease)
    store.activate_cancel(cancel_id, scope, writer_lease=lease)
    _, cancel_claimed = store.claim_cancel_for_dispatch(cancel_id, scope, writer_lease=lease)
    assert cancel_claimed
    store.mark_cancel_unknown(cancel_id, scope, writer_lease=lease)


@pytest.mark.unit
def test_family_owner_reuses_only_exact_store_handle_for_sibling_scopes(tmp_path) -> None:
    path = tmp_path / "family-owner.sqlite3"
    first_scope = _ctp_scope()
    sibling_scope = _ctp_scope(strategy="strategy.two", day="20260926")
    other_mode = _ctp_scope(environment="live")
    store = SqliteExecutionStore(path)
    try:
        with pytest.raises(InvalidStateTransition, match="owner must precede the writer lease"):
            store.acquire_or_renew_lease(first_scope, "shared-actor", ttl_ns=30_000_000_000)
        owner = store.acquire_ctp_account_family_owner(first_scope)
        assert owner.family_key.startswith("ctp-account-family.v1:")
        assert store.acquire_ctp_account_family_owner(sibling_scope) is owner
        assert (
            store.acquire_or_renew_lease(
                first_scope,
                "shared-actor",
                ttl_ns=30_000_000_000,
                ctp_account_family_owner=owner,
            ).family_owner_intent_id
            == owner.owner_intent_id
        )
        assert (
            store.acquire_or_renew_lease(
                sibling_scope,
                "shared-actor",
                ttl_ns=30_000_000_000,
                ctp_account_family_owner=owner,
            ).family_owner_intent_id
            == owner.owner_intent_id
        )
        first_facade = ManagedExecutionFacade(store, first_scope, writer_id="shared-actor")
        sibling_facade = ManagedExecutionFacade(store, sibling_scope, writer_id="shared-actor")
        first_facade_lease = first_facade.acquire_writer_lease()
        sibling_facade_lease = sibling_facade.acquire_writer_lease()
        assert first_facade_lease.family_owner_intent_id == owner.owner_intent_id
        assert sibling_facade_lease.family_owner_intent_id == owner.owner_intent_id
        assert sibling_facade_lease.fencing_token == first_facade_lease.fencing_token
        with pytest.raises(InvalidStateTransition, match="already has a persistent owner"):
            store.acquire_ctp_account_family_owner(other_mode)
        with pytest.raises(ContractValidationError, match="canonical lowercase CTP"):
            store.acquire_ctp_account_family_owner(
                ExecutionScope("CTP", "simulation", _account_ref(), "strategy.one", "20260925")
            )
    finally:
        store.close()

    other_store = SqliteExecutionStore(path)
    try:
        with pytest.raises(ContractValidationError, match="exact same-Store"):
            other_store.acquire_or_renew_lease(
                first_scope,
                "actor.one",
                ttl_ns=30_000_000_000,
                ctp_account_family_owner=owner,
            )
        with pytest.raises(InvalidStateTransition, match="already has a persistent owner"):
            other_store.acquire_ctp_account_family_owner(first_scope)
    finally:
        other_store.close()

    reopened = SqliteExecutionStore(path)
    try:
        with pytest.raises(InvalidStateTransition, match="already has a persistent owner"):
            reopened.acquire_ctp_account_family_owner(first_scope)
    finally:
        reopened.close()


@pytest.mark.unit
def test_v18_history_migrates_to_permanent_unmapped_family_fence(tmp_path) -> None:
    path = tmp_path / "legacy-history.sqlite3"
    store = SqliteExecutionStore(path)
    scope = ExecutionScope("fake", "offline", "account", "strategy.one", "20260925")
    lease = store.acquire_or_renew_lease(scope, "legacy-writer", ttl_ns=30_000_000_000)
    intent = _order(scope, "legacy-intent")
    store.admit_intent(intent, writer_lease=lease)
    store.close()

    connection = sqlite3.connect(path)
    try:
        connection.execute("UPDATE execution_meta SET value = '18' WHERE key = 'schema_version'")
        connection.commit()
    finally:
        connection.close()

    migrated = SqliteExecutionStore(path)
    try:
        assert (
            migrated._connection.execute(
                "SELECT value FROM execution_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            == "20"
        )
        fence = migrated._connection.execute(
            "SELECT reason_code, source_schema_version FROM ctp_account_family_legacy_fences"
        ).fetchone()
        assert tuple(fence) == ("unmapped_legacy_execution_history", "18")
        with pytest.raises(InvalidStateTransition, match="unmapped legacy execution history"):
            migrated.acquire_ctp_account_family_owner(_ctp_scope())
    finally:
        migrated.close()


@pytest.mark.unit
def test_unknown_cancel_account_scan_and_claim_gate_are_atomic_across_strategy_day(
    tmp_path,
) -> None:
    store = SqliteExecutionStore(tmp_path / "account-cancel-gate.sqlite3")
    first_scope = ExecutionScope("fake", "offline", "same-account", "strategy.one", "20260925")
    sibling_scope = ExecutionScope("fake", "offline", "same-account", "strategy.two", "20260926")
    lease = store.acquire_or_renew_lease(first_scope, "actor.one", ttl_ns=30_000_000_000)
    try:
        _persist_unknown_cancel(store, first_scope, lease)
        second_intent = _order(sibling_scope, "fresh-submit")
        store.admit_intent(second_intent, writer_lease=lease)
        store.activate_intent(second_intent.intent_id, sibling_scope, writer_lease=lease)

        unresolved = store.list_unresolved_cancellations_for_account(
            sibling_scope, writer_lease=lease
        )
        assert len(unresolved) == 1
        assert unresolved[0][0] == first_scope
        assert unresolved[0][1].cancel_id == "cancel-unknown"
        assert unresolved[0][1].state is ExecutionState.UNKNOWN

        wrong_lease = type(lease)(
            lease.scope_key,
            lease.owner_id,
            lease.fencing_token + 1,
            lease.expires_at_ns,
        )
        with pytest.raises(WriterLeaseUnavailable):
            store.list_unresolved_cancellations_for_account(sibling_scope, writer_lease=wrong_lease)

        with pytest.raises(
            InvalidStateTransition, match="account has an unresolved managed cancellation"
        ):
            store.claim_for_dispatch(second_intent.intent_id, sibling_scope, writer_lease=lease)
        row = store._connection.execute(
            "SELECT state, dispatch_attempts FROM execution_records WHERE scope_key = ? AND intent_id = ?",
            (sibling_scope.key, second_intent.intent_id),
        ).fetchone()
        assert tuple(row) == ("PENDING_DISPATCH", 0)
    finally:
        store.close()


@pytest.mark.unit
def test_facade_acquires_ctp_family_owner_before_account_lease(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "facade-family.sqlite3")
    scope = _ctp_scope()
    facade = ManagedExecutionFacade(store, scope, writer_id="facade-owner")
    try:
        lease = facade.acquire_writer_lease()
        assert lease.family_key == store._ctp_account_family_key(scope)
        assert lease.family_owner_intent_id
        row = store._connection.execute(
            "SELECT owner_state FROM ctp_account_family_owners WHERE family_key = ?",
            (lease.family_key,),
        ).fetchone()
        assert row is not None and row["owner_state"] == "ACTIVE"
    finally:
        store.close()
