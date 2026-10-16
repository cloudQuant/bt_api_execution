from __future__ import annotations

from decimal import Decimal

from bt_api_base.service_protocol import WireScope

from bt_api_execution import (
    PAPER_ENVIRONMENT,
    PAPER_MODEL_REF,
    PAPER_MODEL_VERSION,
    PAPER_PROVIDER,
    ExecutionScope,
    PaperInstrumentV1,
    PaperOpeningPositionV1,
    PaperOwnerDomainV1,
    SqliteExecutionStore,
)
from bt_api_execution.paper_legacy_import import (
    LegacyPaperSymbolBindingV1,
    PaperLegacyImportV1,
)
from bt_api_execution.service_paper_legacy_import import (
    ABORT_OPERATION,
    ACTIVATE_OPERATION,
    INSPECT_SOURCE_OPERATION,
    PREPARE_OPERATION,
    STATUS_OPERATION,
    PaperLegacyImportService,
)

_DOMAIN_REF = "afi12-import-domain-v1"
_ACCOUNT_REF = "afi12-import-account-v1"
_STRATEGY_REF = "afi12-import-strategy-v1"
_SOURCE_ACCOUNT_ID = "afi12-legacy-account-001"
_SOURCE_OWNER_ID = "afi12-legacy-owner-001"
_LEGACY_SYMBOL = "BTC/USDT"
_INSTRUMENT = "BTC-USDT"
_WIRE_SCOPE = WireScope(
    _DOMAIN_REF,
    account_ref=_ACCOUNT_REF,
    strategy_ref=_STRATEGY_REF,
)


def _domain() -> tuple[ExecutionScope, PaperOwnerDomainV1]:
    scope = ExecutionScope(PAPER_PROVIDER, PAPER_ENVIRONMENT, _ACCOUNT_REF, _STRATEGY_REF)
    domain = PaperOwnerDomainV1(
        execution_domain_ref=_DOMAIN_REF,
        simulation_account_ref=_ACCOUNT_REF,
        experiment_ref="afi12-import-experiment-v1",
        strategy_ref=_STRATEGY_REF,
        model_ref=PAPER_MODEL_REF,
        model_version=PAPER_MODEL_VERSION,
        quote_source_ref="afi12-import-quotes-v1",
        cash_currency="USDT",
        initial_cash=Decimal("89.9"),
        initial_fees=Decimal("0.1"),
        fee_rate=Decimal("0.01"),
        fee_quantum=Decimal("0.01"),
        max_quote_age_ns=30_000_000_000,
        instruments=(
            PaperInstrumentV1(
                _INSTRUMENT,
                "afi12-import-btc-usdt-v1",
                "BTC",
                "USDT",
                Decimal("0.001"),
                Decimal("0.01"),
            ),
        ),
        opening_positions=(
            PaperOpeningPositionV1(_INSTRUMENT, Decimal("1"), Decimal("10")),
        ),
    )
    return scope, domain


def _imported() -> PaperLegacyImportV1:
    document = {
        "schema": "afi.legacy-paper-inventory.v1",
        "account": {
            "id": _SOURCE_ACCOUNT_ID,
            "user_id": _SOURCE_OWNER_ID,
            "is_active": True,
            "initial_cash": 100.0,
            "current_cash": 89.9,
            "total_equity": 99.9,
            "profit_loss": -0.1,
            "profit_loss_pct": -0.1,
            "commission_rate": 0.01,
            "slippage_rate": 0.0,
        },
        "orders": [
            {
                "id": "afi12-legacy-order-001",
                "account_id": _SOURCE_ACCOUNT_ID,
                "symbol": _LEGACY_SYMBOL,
                "status": "filled",
                "side": "buy",
                "size": 1.0,
                "filled_size": 1.0,
                "avg_fill_price": 10.0,
                "commission": 0.1,
            }
        ],
        "positions": [
            {
                "id": "afi12-legacy-position-001",
                "account_id": _SOURCE_ACCOUNT_ID,
                "symbol": _LEGACY_SYMBOL,
                "size": 1.0,
                "avg_price": 10.0,
                "market_value": 10.0,
                "multiplier": 1.0,
                "margin_value": 0.0,
                "margin_rate": 1.0,
            }
        ],
        "trades": [
            {
                "id": "afi12-legacy-trade-001",
                "account_id": _SOURCE_ACCOUNT_ID,
                "order_id": "afi12-legacy-order-001",
                "symbol": _LEGACY_SYMBOL,
                "side": "buy",
                "size": 1.0,
                "price": 10.0,
                "commission": 0.1,
                "created_at": "2026-10-17T00:00:00+00:00",
            }
        ],
    }
    return PaperLegacyImportV1.from_source_document(
        source_account_id=_SOURCE_ACCOUNT_ID,
        source_owner_id=_SOURCE_OWNER_ID,
        source_document=document,
        cash_currency="USDT",
        symbol_bindings=(LegacyPaperSymbolBindingV1(_LEGACY_SYMBOL, _INSTRUMENT),),
    )


def test_service_uses_fixed_target_and_real_store_handoff_without_execution_facts(
    tmp_path,
) -> None:
    scope, domain = _domain()
    store = SqliteExecutionStore(tmp_path / "legacy-import.sqlite3")
    service = PaperLegacyImportService(
        store,
        {_WIRE_SCOPE: scope},
        domain,
        writer_id="afi12-import-host-v1",
    )
    try:
        denied = service.handle(
            STATUS_OPERATION,
            scope=WireScope("other-domain", account_ref=_ACCOUNT_REF, strategy_ref=_STRATEGY_REF),
            arguments={},
        )
        assert denied == {"ok": False, "code": "SCOPE_DENIED", "result": {}}

        before = service.handle(STATUS_OPERATION, scope=_WIRE_SCOPE, arguments={})
        assert before["ok"] is True
        assert before["result"] == {"found": False, "receipt": None}

        imported = _imported()
        prepared = service.handle(
            PREPARE_OPERATION,
            scope=_WIRE_SCOPE,
            arguments={"imported": imported.to_payload()},
        )
        assert prepared["ok"] is True
        prepared_receipt = prepared["result"]
        assert prepared_receipt["state"] == "PREPARED"
        assert prepared_receipt["origin"] == "imported_legacy"
        assert prepared_receipt["source_sha256"] == imported.source_sha256
        assert "source_document_json" not in prepared_receipt

        inspected = service.handle(
            INSPECT_SOURCE_OPERATION,
            scope=_WIRE_SCOPE,
            arguments={"source_account_id": _SOURCE_ACCOUNT_ID},
        )
        assert inspected["ok"] is True
        assert inspected["result"]["source_receipt"]["state"] == "PREPARED"
        assert inspected["result"]["owner_domain_exists"] is False

        activated = service.handle(ACTIVATE_OPERATION, scope=_WIRE_SCOPE, arguments={})
        assert activated["ok"] is True
        assert activated["result"]["state"] == "ACTIVE"
        status = store.inspect_paper_legacy_import_account(
            scope, source_account_id=_SOURCE_ACCOUNT_ID
        )
        assert status.owner_domain_exists is True
        assert status.source_receipt is not None
        assert status.source_receipt.state == "ACTIVE"
        assert store._connection.execute("SELECT COUNT(*) FROM execution_records").fetchone()[0] == 0
        assert store._connection.execute("SELECT COUNT(*) FROM paper_match_receipts").fetchone()[0] == 0

        aborted = service.handle(
            ABORT_OPERATION,
            scope=_WIRE_SCOPE,
            arguments={"source_sha256": imported.source_sha256},
        )
        assert aborted == {"ok": False, "code": "IMPORT_STATE_CONFLICT", "result": {}}
        after = service.handle(STATUS_OPERATION, scope=_WIRE_SCOPE, arguments={})
        assert after["ok"] is True
        assert after["result"]["receipt"]["state"] == "ACTIVE"
    finally:
        store.close()
