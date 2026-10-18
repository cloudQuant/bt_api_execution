"""Borrowed CTP leases stay owned by their runtime composition."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest
from bt_api_base.service_protocol import WireScope

import bt_api_execution.store as execution_store_module
from bt_api_execution import (
    ContractValidationError,
    ExecutionScope,
    ManagedExecutionFacade,
    SqliteExecutionStore,
    WriterLeaseUnavailable,
)
from bt_api_execution.service_order_entry import ExecutionOrderEntryService


def _scope(
    *,
    account: str = "borrowed-writer-test",
    provider: str = "ctp",
    strategy: str = "strategy.one",
    day: str = "20261018",
) -> ExecutionScope:
    account_ref = "ctp-account-ref.v1:" + hashlib.sha256(account.encode("ascii")).hexdigest()
    return ExecutionScope(provider, "simulation", account_ref, strategy, day)


def _wire_scope(scope: ExecutionScope) -> WireScope:
    return WireScope(
        "ctp",
        account_ref=scope.account_ref,
        strategy_ref=scope.strategy_id,
        run_id=f"run-{scope.trading_day}",
    )


def _lease(store: SqliteExecutionStore, scope: ExecutionScope, owner_id: str, ttl_ns: int):
    family_owner = store.acquire_ctp_account_family_owner(scope)
    lease = store.acquire_or_renew_lease(
        scope,
        owner_id,
        ttl_ns=ttl_ns,
        ctp_account_family_owner=family_owner,
    )
    return family_owner, lease


def _persisted_expiry(store: SqliteExecutionStore, scope: ExecutionScope) -> int:
    row = store._connection.execute(
        "SELECT expires_at_ns FROM execution_writer_leases WHERE scope_key = ?",
        (scope.account_key,),
    ).fetchone()
    assert row is not None
    return int(row[0])


class _NoopDispatch:
    def submit(self, intent):  # pragma: no cover - constructor-only fixture
        raise AssertionError("borrowed lease test must not dispatch")

    def cancel(self, intent):  # pragma: no cover - constructor-only fixture
        raise AssertionError("borrowed lease test must not dispatch")


@pytest.mark.unit
def test_borrowed_ctp_facades_reuse_original_lease_without_owning_renewal_or_release(
    tmp_path, monkeypatch
) -> None:
    store = SqliteExecutionStore(tmp_path / "borrowed-lease.sqlite3")
    scope = _scope()
    sibling = _scope(strategy="strategy.two", day="20261019")
    family_owner, lease = _lease(store, scope, "ctp-runtime-owner", 30_000_000_000)
    raw_acquire = store.acquire_or_renew_lease
    raw_assert = store.assert_writer_lease
    raw_release = store.release_lease
    raw_family_owner = store.acquire_ctp_account_family_owner
    acquire_calls = []
    assert_calls = []
    release_calls = []
    family_owner_calls = []

    def tracked_acquire(*args, **kwargs):
        acquire_calls.append((args, kwargs))
        return raw_acquire(*args, **kwargs)

    def tracked_assert(*args, **kwargs):
        assert_calls.append((args, kwargs))
        return raw_assert(*args, **kwargs)

    def tracked_release(*args, **kwargs):
        release_calls.append((args, kwargs))
        return raw_release(*args, **kwargs)

    def tracked_family_owner(*args, **kwargs):
        family_owner_calls.append((args, kwargs))
        return raw_family_owner(*args, **kwargs)

    monkeypatch.setattr(store, "acquire_or_renew_lease", tracked_acquire)
    monkeypatch.setattr(store, "assert_writer_lease", tracked_assert)
    monkeypatch.setattr(store, "release_lease", tracked_release)
    monkeypatch.setattr(store, "acquire_ctp_account_family_owner", tracked_family_owner)
    try:
        original_expiry = _persisted_expiry(store, scope)
        direct = ManagedExecutionFacade(
            store,
            scope,
            writer_id=lease.owner_id,
            borrowed_writer_lease=lease,
        )
        assert direct.acquire_writer_lease() is lease
        assert _persisted_expiry(store, scope) == original_expiry

        service = ExecutionOrderEntryService(
            store,
            {_wire_scope(scope): scope, _wire_scope(sibling): sibling},
            _NoopDispatch(),
            writer_id=lease.owner_id,
            allow_unprotected=True,
            borrowed_writer_lease=lease,
        )
        primary_order_facade, _ = service._facade_pair(scope)
        sibling_order_facade, _ = service._facade_pair(sibling)
        assert primary_order_facade._borrowed_writer_lease is lease
        assert sibling_order_facade._borrowed_writer_lease is lease
        assert primary_order_facade.acquire_writer_lease() is lease
        assert sibling_order_facade.acquire_writer_lease() is lease
        assert acquire_calls == []
        assert release_calls == []
        assert family_owner_calls == []
        assert _persisted_expiry(store, scope) == original_expiry
        assert any(call[0][0] is scope and call[0][1] is lease for call in assert_calls)
        assert any(call[0][0] is sibling and call[0][1] is lease for call in assert_calls)

        renewed = raw_acquire(
            scope,
            lease.owner_id,
            ttl_ns=60_000_000_000,
            ctp_account_family_owner=family_owner,
        )
        assert renewed.owner_id == lease.owner_id
        assert renewed.fencing_token == lease.fencing_token
        renewed_expiry = _persisted_expiry(store, scope)
        assert renewed_expiry > original_expiry
        raw_assert(scope, lease)

        assert direct.acquire_writer_lease() is lease
        assert sibling_order_facade.acquire_writer_lease() is lease
        assert acquire_calls == []
        assert family_owner_calls == []
        assert _persisted_expiry(store, scope) == renewed_expiry
        assert direct.close() is False
        assert primary_order_facade.close() is False
        assert sibling_order_facade.close() is False
        assert release_calls == []
        raw_assert(scope, lease)
        raw_assert(sibling, lease)
        assert _persisted_expiry(store, scope) == renewed_expiry
    finally:
        store.close()


@pytest.mark.unit
def test_borrowed_ctp_lease_rejects_wrong_writer_non_ctp_and_mixed_account_targets(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "borrowed-lease-invalid.sqlite3")
    scope = _scope()
    other_account = _scope(account="another-borrowed-writer-test", strategy="strategy.two")
    _, lease = _lease(store, scope, "ctp-runtime-owner", 30_000_000_000)
    try:
        with pytest.raises(ContractValidationError):
            ManagedExecutionFacade(
                store,
                scope,
                writer_id="different-writer",
                borrowed_writer_lease=lease,
            )

        with pytest.raises(ContractValidationError):
            ManagedExecutionFacade(
                store,
                _scope(provider="PAPER"),
                writer_id=lease.owner_id,
                borrowed_writer_lease=lease,
            )

        with pytest.raises(ContractValidationError):
            ExecutionOrderEntryService(
                store,
                {
                    _wire_scope(scope): scope,
                    _wire_scope(other_account): other_account,
                },
                _NoopDispatch(),
                writer_id=lease.owner_id,
                allow_unprotected=True,
                borrowed_writer_lease=lease,
            )
    finally:
        store.close()


@pytest.mark.unit
def test_borrowed_ctp_facade_rejects_expired_and_refenced_lease(tmp_path, monkeypatch) -> None:
    store = SqliteExecutionStore(tmp_path / "borrowed-lease-fenced.sqlite3")
    scope = _scope(account="fenced-borrowed-writer-test")
    original_time = execution_store_module.time
    wall_now = [1_800_000_000_000_000_000]
    monkeypatch.setattr(
        execution_store_module,
        "time",
        SimpleNamespace(time_ns=lambda: wall_now[0], monotonic_ns=original_time.monotonic_ns),
    )
    family_owner, lease = _lease(store, scope, "ctp-runtime-owner", 1_000_000)
    facade = ManagedExecutionFacade(
        store,
        scope,
        writer_id=lease.owner_id,
        borrowed_writer_lease=lease,
    )
    try:
        assert facade.acquire_writer_lease() is lease
        wall_now[0] = lease.expires_at_ns
        replacement = store.acquire_or_renew_lease(
            scope,
            "successor-runtime-owner",
            ttl_ns=30_000_000_000,
            ctp_account_family_owner=family_owner,
        )
        assert replacement.fencing_token == lease.fencing_token + 1
        store.assert_writer_lease(scope, replacement)
        with pytest.raises(WriterLeaseUnavailable):
            facade.acquire_writer_lease()
    finally:
        store.close()
