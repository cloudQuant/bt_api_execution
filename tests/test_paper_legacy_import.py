from __future__ import annotations

import hashlib
import json
from decimal import Decimal

import pytest

from bt_api_execution import (
    PAPER_ENVIRONMENT,
    PAPER_MODEL_REF,
    PAPER_PROVIDER,
    PaperInstrumentV1,
    PaperOwnerDomainV1,
    SqliteExecutionStore,
)
from bt_api_execution.contracts import ExecutionScope
from bt_api_execution.errors import (
    ContractValidationError,
    DurableStoreError,
    IntentConflictError,
    PaperImportResidualUnrepresentableError,
)
from bt_api_execution.paper_execution import PaperOpeningPositionV1
from bt_api_execution.paper_legacy_import import (
    LegacyPaperSymbolBindingV1,
    PaperLegacyImportV1,
)

_SOURCE_ACCOUNT_ID = "legacy-account-01"
_SOURCE_OWNER_ID = "legacy-user-01"


def _source_document(*, pending_order: bool = False) -> dict[str, object]:
    return {
        "schema": "afi.legacy-paper-inventory.v1",
        "account": {
            "id": _SOURCE_ACCOUNT_ID,
            "user_id": _SOURCE_OWNER_ID,
            "name": "Imported paper account",
            "initial_cash": 1000.0,
            "current_cash": 974.975,
            "total_equity": 1024.975,
            "profit_loss": 24.975,
            "profit_loss_pct": 2.4975,
            "commission_rate": 0.001,
            "slippage_rate": 0.0,
            "is_active": True,
            "created_at": "2026-10-01T00:00:00+00:00",
            "updated_at": "2026-10-15T00:00:00+00:00",
        },
        "orders": [
            {
                "id": "legacy-order-filled",
                "account_id": _SOURCE_ACCOUNT_ID,
                "symbol": "BTC/USDT",
                "order_type": "market",
                "side": "buy",
                "size": 2.5,
                "price": None,
                "stop_price": None,
                "limit_price": None,
                "filled_size": 2.5,
                "avg_fill_price": 10.0,
                "status": "filled",
                "rejected_reason": None,
                "commission": 0.025,
                "slippage": 0.0,
                "created_at": "2026-10-02T00:00:00+00:00",
                "updated_at": "2026-10-02T00:00:01+00:00",
                "filled_at": "2026-10-02T00:00:01+00:00",
            },
            {
                "id": "legacy-order-rejected",
                "account_id": _SOURCE_ACCOUNT_ID,
                "symbol": "NO-LONGER-CONFIGURED",
                "order_type": "market",
                "side": "buy",
                "size": 1.0,
                "price": None,
                "stop_price": None,
                "limit_price": None,
                "filled_size": 0.0,
                "avg_fill_price": 0.0,
                "status": "rejected",
                "rejected_reason": "source rejection fact",
                "commission": 0.0,
                "slippage": 0.0,
                "created_at": "2026-10-03T00:00:00+00:00",
                "updated_at": "2026-10-03T00:00:00+00:00",
                "filled_at": None,
            },
        ],
        "positions": [
            {
                "id": "legacy-position-btc",
                "account_id": _SOURCE_ACCOUNT_ID,
                "symbol": "BTC/USDT",
                "size": 2.5,
                "avg_price": 10.0,
                "market_value": 50.0,
                "margin_value": 0.0,
                "multiplier": 1.0,
                "margin_rate": 1.0,
                "commission_rate": 0.0,
                "commission_amount": 0.0,
                "unrealized_pnl": 0.0,
                "unrealized_pnl_pct": 0.0,
                "entry_price": 10.0,
                "entry_time": "2026-10-02T00:00:01+00:00",
                "updated_at": "2026-10-02T00:00:01+00:00",
            }
        ],
        "trades": [
            {
                "id": "legacy-trade-btc",
                "account_id": _SOURCE_ACCOUNT_ID,
                "order_id": "legacy-order-filled",
                "symbol": "BTC/USDT",
                "side": "buy",
                "size": 2.5,
                "price": 10.0,
                "commission": 0.025,
                "slippage": 0.0,
                "pnl": 0.0,
                "pnl_pct": 0.0,
                "created_at": "2026-10-02T00:00:01+00:00",
            }
        ],
    }


def _scope(strategy_id: str = "managed-strategy-01") -> ExecutionScope:
    return ExecutionScope(
        PAPER_PROVIDER,
        PAPER_ENVIRONMENT,
        "managed-sim-account-01",
        strategy_id,
        "20261016",
    )


def _domain(scope: ExecutionScope) -> PaperOwnerDomainV1:
    return PaperOwnerDomainV1(
        execution_domain_ref="managed-domain-01",
        simulation_account_ref=scope.account_ref,
        experiment_ref="managed-experiment-01",
        strategy_ref=scope.strategy_id,
        model_ref=PAPER_MODEL_REF,
        model_version="1",
        quote_source_ref="trusted-synthetic-quote-source",
        cash_currency="USDT",
        initial_cash=Decimal("974.975"),
        initial_fees=Decimal("0.025"),
        fee_rate=Decimal("0.001"),
        fee_quantum=Decimal("0.0001"),
        max_quote_age_ns=30_000_000_000,
        instruments=(
            PaperInstrumentV1(
                "BTC-USDT",
                "metadata-v1",
                "BTC",
                "USDT",
                Decimal("0.25"),
                Decimal("0.01"),
            ),
        ),
        opening_positions=(PaperOpeningPositionV1("BTC-USDT", Decimal("2.5"), Decimal("10")),),
    )


def _imported(*, pending_order: bool = False) -> PaperLegacyImportV1:
    document = _source_document(pending_order=pending_order)
    if pending_order:
        document["orders"][0]["status"] = "pending"  # type: ignore[index]
        document["orders"][0]["filled_size"] = 0.0  # type: ignore[index]
        document["orders"][0]["avg_fill_price"] = 0.0  # type: ignore[index]
        document["orders"][0]["commission"] = 0.0  # type: ignore[index]
        document["trades"] = []
        document["positions"] = []
        document["account"]["current_cash"] = 1000.0  # type: ignore[index]
        document["account"]["total_equity"] = 1000.0  # type: ignore[index]
        document["account"]["profit_loss"] = 0.0  # type: ignore[index]
        document["account"]["profit_loss_pct"] = 0.0  # type: ignore[index]
    return PaperLegacyImportV1.from_source_document(
        source_account_id=_SOURCE_ACCOUNT_ID,
        source_owner_id=_SOURCE_OWNER_ID,
        source_document=document,
        cash_currency="USDT",
        symbol_bindings=(LegacyPaperSymbolBindingV1("BTC/USDT", "BTC-USDT"),),
    )


def _lease(store: SqliteExecutionStore, scope: ExecutionScope):
    return store.acquire_or_renew_lease(scope, "legacy-import-writer", ttl_ns=60_000_000_000)


def test_account_status_fails_closed_when_schema_metadata_is_missing(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "paper-legacy-missing-meta.sqlite3")
    lease = _lease(store, scope)
    try:
        empty_status = store.inspect_paper_legacy_import_account(
            scope, source_account_id=_SOURCE_ACCOUNT_ID
        )
        assert empty_status.source_receipt is None
        assert empty_status.active_account_import_state is None
        assert empty_status.owner_domain_exists is False

        domain = _domain(scope)
        imported = _imported()
        store.prepare_paper_legacy_import(scope, domain, imported, writer_lease=lease)
        store.activate_paper_legacy_import(scope, writer_lease=lease)

        # Simulate loss/corruption of the metadata table while owner and import
        # facts remain.  Returning an empty account status here would let the
        # AFI source rollback path unfreeze a second writer.
        store._connection.execute("DROP TABLE execution_meta")
        with pytest.raises(DurableStoreError, match="partial or unversioned"):
            store.inspect_paper_legacy_import_account(
                scope, source_account_id=_SOURCE_ACCOUNT_ID
            )
    finally:
        store.close()


def test_import_prepares_without_owner_then_activates_the_exact_opening_snapshot(tmp_path):
    scope = _scope()
    domain = _domain(scope)
    imported = _imported()
    store = SqliteExecutionStore(tmp_path / "paper-legacy-import.sqlite3")
    lease = _lease(store, scope)
    try:
        before = store.inspect_paper_legacy_import_account(
            scope, source_account_id=_SOURCE_ACCOUNT_ID
        )
        assert before.source_receipt is None
        assert before.active_account_import_state is None
        assert before.owner_domain_exists is False

        prepared = store.prepare_paper_legacy_import(scope, domain, imported, writer_lease=lease)
        assert prepared.state == "PREPARED"
        assert prepared.import_sha256 == imported.fingerprint
        assert prepared.imported.origin == "imported_legacy"
        assert store.get_paper_legacy_import(scope) == prepared
        connection = store._connection
        changes_before_status_read = connection.total_changes
        prepared_status = store.inspect_paper_legacy_import_account(
            scope, source_account_id=_SOURCE_ACCOUNT_ID
        )
        assert prepared_status.source_receipt is not None
        assert prepared_status.source_receipt.state == "PREPARED"
        assert prepared_status.source_receipt.scope_key == scope.key
        assert prepared_status.source_receipt.account_key == scope.account_key
        assert prepared_status.active_account_import_state == "PREPARED"
        assert prepared_status.owner_domain_exists is False
        assert connection.total_changes == changes_before_status_read
        with pytest.raises(DurableStoreError, match="blocked by legacy import state"):
            store.register_paper_owner_domain(scope, domain, writer_lease=lease)

        activated = store.activate_paper_legacy_import(scope, writer_lease=lease)
        assert activated.state == "ACTIVE"
        assert (
            store.register_paper_owner_domain(scope, domain, writer_lease=lease)
            == domain.fingerprint
        )
        book = store.read_paper_book(scope)
        assert book.cash == Decimal("974.975")
        assert book.cumulative_fees == Decimal("0.025")
        assert [(row.instrument, row.quantity, row.average_cost) for row in book.positions] == [
            ("BTC-USDT", Decimal("2.5"), Decimal("10"))
        ]
        assert book.receipt_count == 0
        assert book.execution_cursor == 0

        other_strategy_scope = _scope("managed-strategy-02")
        connection = store._connection
        changes_before_status_read = connection.total_changes
        account_status = store.inspect_paper_legacy_import_account(
            other_strategy_scope, source_account_id=_SOURCE_ACCOUNT_ID
        )
        assert account_status.source_receipt is not None
        assert account_status.source_receipt.scope_key == scope.key
        assert account_status.source_receipt.state == "ACTIVE"
        assert account_status.active_account_import_state == "ACTIVE"
        assert account_status.owner_domain_exists is True
        assert connection.total_changes == changes_before_status_read

        changes_before = connection.total_changes
        reloaded = store.get_paper_legacy_import(scope)
        assert reloaded == activated
        assert connection.total_changes == changes_before
        assert (
            reloaded.imported.source_sha256
            == hashlib.sha256(imported.source_document_json.encode("utf-8")).hexdigest()
        )
        source_payload = json.loads(reloaded.imported.source_document_json)
        assert [row["id"] for row in source_payload["orders"]] == [
            "legacy-order-filled",
            "legacy-order-rejected",
        ]
        assert source_payload["orders"][1]["status"] == "rejected"

        assert (
            store.prepare_paper_legacy_import(scope, domain, imported, writer_lease=lease)
            == activated
        )
        assert store.activate_paper_legacy_import(scope, writer_lease=lease) == activated
    finally:
        store.close()


def test_import_reconciles_profit_loss_and_handles_zero_initial_cash() -> None:
    document = _source_document()
    account = document["account"]
    assert isinstance(account, dict)
    account["initial_cash"] = 0.0
    account["current_cash"] = 0.0
    account["total_equity"] = 1.0
    account["profit_loss"] = 1.0
    account["profit_loss_pct"] = 0.0
    document["orders"] = []
    document["positions"] = []
    document["trades"] = []

    imported = PaperLegacyImportV1.from_source_document(
        source_account_id=_SOURCE_ACCOUNT_ID,
        source_owner_id=_SOURCE_OWNER_ID,
        source_document=document,
        cash_currency="USDT",
        symbol_bindings=(LegacyPaperSymbolBindingV1("BTC/USDT", "BTC-USDT"),),
    )
    assert imported.unallocated_equity_residual == Decimal("1.0")

    account["profit_loss"] = 0.0
    with pytest.raises(ContractValidationError, match="profit and loss does not reconcile"):
        PaperLegacyImportV1.from_source_document(
            source_account_id=_SOURCE_ACCOUNT_ID,
            source_owner_id=_SOURCE_OWNER_ID,
            source_document=document,
            cash_currency="USDT",
            symbol_bindings=(LegacyPaperSymbolBindingV1("BTC/USDT", "BTC-USDT"),),
        )

    account["profit_loss"] = 1.0
    account["profit_loss_pct"] = 1.0
    with pytest.raises(
        ContractValidationError,
        match="profit and loss percent does not reconcile",
    ):
        PaperLegacyImportV1.from_source_document(
            source_account_id=_SOURCE_ACCOUNT_ID,
            source_owner_id=_SOURCE_OWNER_ID,
            source_document=document,
            cash_currency="USDT",
            symbol_bindings=(LegacyPaperSymbolBindingV1("BTC/USDT", "BTC-USDT"),),
        )


def test_stored_v1_receipt_keeps_historically_accepted_pnl_readable(tmp_path) -> None:
    scope = _scope()
    domain = _domain(scope)
    payload = _imported().to_payload()
    document = json.loads(payload["source_document_json"])
    # Earlier V1 receipts accepted finite P&L projections without enforcing
    # the newer ingress reconciliation.  Reading them must preserve the
    # receipt and owner rather than invalidating existing durable state.
    document["account"]["profit_loss"] = 99.0
    document["account"]["profit_loss_pct"] = -7.0
    source_json = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    payload["source_document_json"] = source_json
    payload["source_sha256"] = hashlib.sha256(source_json.encode("utf-8")).hexdigest()
    import_json = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    domain_json = json.dumps(
        domain.to_payload(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    row = {
        "import_json": import_json,
        "domain_json": domain_json,
        "source_system": "afi_legacy_paper",
        "source_account_id": _SOURCE_ACCOUNT_ID,
        "source_owner_id": _SOURCE_OWNER_ID,
        "source_sha256": payload["source_sha256"],
        "import_sha256": hashlib.sha256(import_json.encode("utf-8")).hexdigest(),
        "domain_sha256": hashlib.sha256(domain_json.encode("utf-8")).hexdigest(),
        "scope_key": scope.key,
        "account_key": scope.account_key,
        "state": "ACTIVE",
    }

    receipt = SqliteExecutionStore._paper_legacy_import_from_row(row, scope=scope)

    assert receipt.imported.to_payload() == payload
    with pytest.raises(ContractValidationError, match="profit and loss does not reconcile"):
        receipt.imported.validate_new_import()

    store = SqliteExecutionStore(tmp_path / "paper-legacy-v1-ingress.sqlite3")
    lease = _lease(store, scope)
    try:
        with pytest.raises(
            ContractValidationError,
            match="profit and loss does not reconcile",
        ):
            store.prepare_paper_legacy_import(
                scope,
                domain,
                receipt.imported,
                writer_lease=lease,
            )
        assert store.get_paper_legacy_import(scope) is None
    finally:
        store.close()


def test_nonzero_equity_residual_stays_prepared_and_never_creates_active_owner(tmp_path):
    scope = _scope()
    domain = _domain(scope)
    document = _source_document()
    account = document["account"]
    assert isinstance(account, dict)
    account["total_equity"] = 1025.975
    account["profit_loss"] = 25.975
    account["profit_loss_pct"] = 2.5975
    imported = PaperLegacyImportV1.from_source_document(
        source_account_id=_SOURCE_ACCOUNT_ID,
        source_owner_id=_SOURCE_OWNER_ID,
        source_document=document,
        cash_currency="USDT",
        symbol_bindings=(LegacyPaperSymbolBindingV1("BTC/USDT", "BTC-USDT"),),
    )
    assert imported.unallocated_equity_residual == Decimal("1.000")

    store = SqliteExecutionStore(tmp_path / "paper-legacy-residual.sqlite3")
    lease = _lease(store, scope)
    try:
        prepared = store.prepare_paper_legacy_import(
            scope, domain, imported, writer_lease=lease
        )
        assert prepared.state == "PREPARED"
        assert prepared.imported.unallocated_equity_residual == Decimal("1.000")

        with pytest.raises(PaperImportResidualUnrepresentableError):
            store.activate_paper_legacy_import(scope, writer_lease=lease)

        # The complete source facts and source fence remain durable, while the
        # active owner stays absent.  Callers cannot turn this refusal into an
        # empty-target success or silently discard the residual.
        assert store.get_paper_legacy_import(scope) == prepared
        status = store.inspect_paper_legacy_import_account(
            scope, source_account_id=_SOURCE_ACCOUNT_ID
        )
        assert status.source_receipt is not None
        assert status.source_receipt.state == "PREPARED"
        assert status.source_receipt.unallocated_equity_residual == Decimal("1.000")
        assert status.active_account_import_state == "PREPARED"
        assert status.owner_domain_exists is False
        assert (
            store._connection.execute(
                "SELECT 1 FROM paper_owner_domains WHERE scope_key = ?", (scope.key,)
            ).fetchone()
            is None
        )
    finally:
        store.close()


def test_unresolved_order_is_rejected_before_import_or_owner_creation(tmp_path):
    with pytest.raises(ContractValidationError, match="unresolved order"):
        _imported(pending_order=True)

    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "paper-legacy-pending.sqlite3")
    try:
        assert store.get_paper_legacy_import(scope) is None
        assert (
            store._connection.execute(
                "SELECT 1 FROM execution_meta WHERE key = 'paper_schema_version'"
            ).fetchone()
            is None
        )
    finally:
        store.close()


def test_prepared_import_can_be_aborted_and_retried_under_a_new_scope(tmp_path):
    first_scope = _scope()
    second_scope = _scope("managed-strategy-02")
    imported = _imported()
    store = SqliteExecutionStore(tmp_path / "paper-legacy-rollback.sqlite3")
    lease = _lease(store, first_scope)
    try:
        first_domain = _domain(first_scope)
        prepared = store.prepare_paper_legacy_import(
            first_scope, first_domain, imported, writer_lease=lease
        )
        aborted = store.abort_paper_legacy_import(
            first_scope, source_sha256=imported.source_sha256, writer_lease=lease
        )
        assert aborted.state == "ABORTED"
        assert store.get_paper_legacy_import(first_scope) == aborted
        aborted_status = store.inspect_paper_legacy_import_account(
            first_scope, source_account_id=_SOURCE_ACCOUNT_ID
        )
        assert aborted_status.source_receipt is not None
        assert aborted_status.source_receipt.state == "ABORTED"
        assert aborted_status.active_account_import_state is None
        assert aborted_status.owner_domain_exists is False
        with pytest.raises(DurableStoreError, match="cannot be activated"):
            store.activate_paper_legacy_import(first_scope, writer_lease=lease)
        with pytest.raises(IntentConflictError, match="prior receipt history"):
            store.prepare_paper_legacy_import(
                first_scope, first_domain, imported, writer_lease=lease
            )

        second_domain = _domain(second_scope)
        retried = store.prepare_paper_legacy_import(
            second_scope, second_domain, imported, writer_lease=lease
        )
        assert retried.state == "PREPARED"
        assert retried.import_sha256 == prepared.import_sha256
    finally:
        store.close()


def test_import_rejects_a_preexisting_owner_and_conflicting_source_facts(tmp_path):
    scope = _scope()
    domain = _domain(scope)
    store = SqliteExecutionStore(tmp_path / "paper-legacy-conflict.sqlite3")
    lease = _lease(store, scope)
    try:
        store.register_paper_owner_domain(scope, domain, writer_lease=lease)
        with pytest.raises(DurableStoreError, match="already has an owner domain"):
            store.prepare_paper_legacy_import(scope, domain, _imported(), writer_lease=lease)
    finally:
        store.close()

    conflict_store = SqliteExecutionStore(tmp_path / "paper-legacy-source-conflict.sqlite3")
    conflict_lease = _lease(conflict_store, scope)
    try:
        conflict_store.prepare_paper_legacy_import(
            scope, domain, _imported(), writer_lease=conflict_lease
        )
        changed_document = _source_document()
        changed_document["account"]["name"] = "different source account fact"  # type: ignore[index]
        changed = PaperLegacyImportV1.from_source_document(
            source_account_id=_SOURCE_ACCOUNT_ID,
            source_owner_id=_SOURCE_OWNER_ID,
            source_document=changed_document,
            cash_currency="USDT",
            symbol_bindings=(LegacyPaperSymbolBindingV1("BTC/USDT", "BTC-USDT"),),
        )
        with pytest.raises(IntentConflictError, match="different import"):
            conflict_store.prepare_paper_legacy_import(
                scope, domain, changed, writer_lease=conflict_lease
            )
    finally:
        conflict_store.close()


def test_prepare_transaction_rolls_back_schema_and_receipt_on_late_failure(tmp_path, monkeypatch):
    scope = _scope()
    domain = _domain(scope)
    imported = _imported()
    store = SqliteExecutionStore(tmp_path / "paper-legacy-atomic.sqlite3")
    lease = _lease(store, scope)
    original = SqliteExecutionStore._assert_active_writer_lease
    calls = 0

    def fail_after_receipt(cursor, checked_scope, checked_lease, *, now_ns=None):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise DurableStoreError("injected final writer-fence failure")
        return original(cursor, checked_scope, checked_lease, now_ns=now_ns)

    monkeypatch.setattr(
        SqliteExecutionStore, "_assert_active_writer_lease", staticmethod(fail_after_receipt)
    )
    try:
        with pytest.raises(DurableStoreError, match="injected final writer-fence failure"):
            store.prepare_paper_legacy_import(scope, domain, imported, writer_lease=lease)
        monkeypatch.setattr(
            SqliteExecutionStore, "_assert_active_writer_lease", staticmethod(original)
        )
        assert store.get_paper_legacy_import(scope) is None
        assert (
            store._connection.execute(
                "SELECT 1 FROM execution_meta WHERE key = 'paper_schema_version'"
            ).fetchone()
            is None
        )
        assert (
            store._connection.execute(
                "SELECT 1 FROM sqlite_master WHERE name = 'paper_legacy_import_receipts'"
            ).fetchone()
            is None
        )
    finally:
        monkeypatch.setattr(
            SqliteExecutionStore, "_assert_active_writer_lease", staticmethod(original)
        )
        store.close()
