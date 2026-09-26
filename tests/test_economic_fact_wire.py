"""Versioned, lossless wire contracts for account and quality facts."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from bt_api_execution import (
    AccountSnapshot,
    ContractValidationError,
    ExecutionQualityRecord,
    ExecutionScope,
)

_ACCOUNT_FIELDS = (
    "equity",
    "cash",
    "available_margin",
    "margin",
    "realized_pnl",
    "unrealized_pnl",
    "fee",
    "funding",
    "net_cashflow",
    "fx_rate",
    "settlement_price",
)
_QUALITY_FIELDS = (
    "arrival_bid",
    "arrival_ask",
    "arrival_mid",
    "native_quantity",
    "contract_multiplier",
    "vwap",
    "fee",
    "slippage_amount",
    "slippage_bps",
    "stage_durations_ns",
)


def _scope() -> ExecutionScope:
    return ExecutionScope("SIM", "simulation", "private-account-ref", "strategy.alpha", "20260926")


def _metadata(
    fields: tuple[str, ...],
) -> tuple[dict[str, str], dict[str, tuple[int, int]], dict[str, tuple[str, ...]]]:
    return (
        dict.fromkeys(fields, "COMPLETE"),
        dict.fromkeys(fields, (100, 200)),
        {name: (f"source-{name}",) for name in fields},
    )


def _complete_account_snapshot(**overrides) -> AccountSnapshot:
    completeness, coverage, source_refs = _metadata(_ACCOUNT_FIELDS)
    completeness = overrides.pop("field_completeness", completeness)
    coverage = overrides.pop("field_coverage_ns", coverage)
    source_refs = overrides.pop("field_source_refs", source_refs)
    external_coverage = overrides.pop("external_activity_coverage_ns", (100, 200))
    external_refs = overrides.pop("external_activity_source_refs", ("source-external-activity",))
    values = {name: Decimal("1.2500") for name in _ACCOUNT_FIELDS}
    values.update(overrides.pop("values", {}))
    return AccountSnapshot(
        scope=_scope(),
        as_of_ns=200,
        source="provider.account.snapshot",
        completeness="COMPLETE",
        currency="USD",
        reporting_currency="USD",
        generation="generation-7",
        epoch=3,
        external_activity_attribution="COMPLETE",
        external_activity_coverage_ns=external_coverage,
        external_activity_source_refs=external_refs,
        equity_includes_fees=True,
        field_completeness=completeness,
        field_coverage_ns=coverage,
        field_source_refs=source_refs,
        **values,
        **overrides,
    )


def _complete_quality_record(**overrides) -> ExecutionQualityRecord:
    completeness, coverage, source_refs = _metadata(_QUALITY_FIELDS)
    completeness = overrides.pop("field_completeness", completeness)
    coverage = overrides.pop("field_coverage_ns", coverage)
    source_refs = overrides.pop("field_source_refs", source_refs)
    values = {
        "arrival_bid": Decimal("99.50"),
        "arrival_ask": Decimal("100.50"),
        "arrival_mid": Decimal("100.00"),
        "native_quantity": Decimal("2.00"),
        "contract_multiplier": Decimal("1"),
        "vwap": Decimal("100.10"),
        "fee": Decimal("0.03"),
        "slippage_amount": Decimal("0.20"),
        "slippage_bps": Decimal("10"),
    }
    values.update(overrides.pop("values", {}))
    return ExecutionQualityRecord(
        intent_id="intent-1",
        as_of_ns=200,
        source="provider.execution.observation",
        completeness="COMPLETE",
        scope=_scope(),
        generation="generation-7",
        epoch=3,
        currency="USD",
        fee_currency="USD",
        signal_id="signal-1",
        child_id="child-1",
        order_id="order-1",
        trade_id="trade-1",
        arrival_as_of_ns=190,
        arrival_freshness_ns=10,
        arrival_source="market.depth.snapshot",
        side="BUY",
        stage_durations_ns={"admission": 20, "dispatch": 50},
        field_completeness=completeness,
        field_coverage_ns=coverage,
        field_source_refs=source_refs,
        **values,
        **overrides,
    )


@pytest.mark.unit
def test_account_snapshot_wire_is_scoped_exact_and_never_attributes_strategy() -> None:
    snapshot = _complete_account_snapshot()

    wire = snapshot.to_wire()

    assert wire["schema"] == "bt_api.execution.account_snapshot.v1"
    assert wire["fact_type"] == "account_snapshot"
    assert wire["completeness"] == "COMPLETE"
    assert wire["scope"]["account_fingerprint"] == _scope().account_key.removeprefix("account:")
    assert wire["scope"]["strategy_id"] is None
    assert "private-account-ref" not in repr(wire)
    assert wire["values"]["equity"] == "1.2500"
    assert wire["values"]["funding"] == "1.2500"
    assert wire["fx_rate_direction"] == "reporting_currency_units_per_currency_unit"
    assert wire["equity_includes_fees"] is True
    assert wire["external_activity_evidence"] == {
        "coverage_start_ns": 100,
        "coverage_end_ns": 200,
        "source_refs": ["source-external-activity"],
    }
    assert set(wire) == {
        "schema",
        "fact_type",
        "scope",
        "as_of_ns",
        "source",
        "currency",
        "reporting_currency",
        "fx_rate_direction",
        "completeness",
        "values",
        "field_evidence",
        "external_activity_attribution",
        "external_activity_evidence",
        "equity_includes_fees",
    }


@pytest.mark.unit
def test_account_snapshot_missing_funding_cannot_be_marked_complete() -> None:
    completeness, coverage, source_refs = _metadata(_ACCOUNT_FIELDS)
    with pytest.raises(ContractValidationError, match="complete funding is missing"):
        _complete_account_snapshot(
            values={"funding": None},
            field_completeness=completeness,
            field_coverage_ns=coverage,
            field_source_refs=source_refs,
        )

    snapshot = AccountSnapshot(
        scope=_scope(),
        as_of_ns=200,
        source="provider.account.snapshot",
        completeness="COMPLETE",
        equity=Decimal("10"),
        available_margin=Decimal("5"),
        currency="USD",
        generation="generation-7",
        epoch=3,
    )
    wire = snapshot.to_wire()
    assert wire["completeness"] == "INCOMPLETE"
    assert wire["values"]["funding"] is None
    assert wire["field_evidence"]["funding"]["completeness"] == "INCOMPLETE"


@pytest.mark.unit
def test_complete_external_activity_requires_coverage_and_source_refs() -> None:
    with pytest.raises(ContractValidationError, match="external activity attribution"):
        AccountSnapshot(
            scope=_scope(),
            as_of_ns=200,
            source="provider.account.snapshot",
            completeness="INCOMPLETE",
            generation="generation-7",
            epoch=3,
            external_activity_attribution="COMPLETE",
        )


@pytest.mark.unit
def test_complete_external_activity_coverage_must_reach_snapshot_as_of() -> None:
    with pytest.raises(ContractValidationError, match="as_of coverage"):
        _complete_account_snapshot(external_activity_coverage_ns=(100, 199))


@pytest.mark.unit
def test_quality_wire_keeps_lineage_currency_and_monotonic_durations() -> None:
    wire = _complete_quality_record().to_wire()

    assert wire["schema"] == "bt_api.execution.execution_quality.v1"
    assert wire["completeness"] == "COMPLETE"
    assert wire["scope"]["strategy_id"] == "strategy.alpha"
    assert wire["lineage"] == {
        "signal_id": "signal-1",
        "intent_id": "intent-1",
        "child_id": "child-1",
        "order_id": "order-1",
        "trade_id": "trade-1",
    }
    assert wire["arrival"]["mid"] == "100.00"
    assert wire["execution"]["fee"] == "0.03"
    assert wire["execution"]["fee_currency"] == "USD"
    assert wire["execution"]["slippage_amount"] == "0.20"
    assert wire["execution"]["slippage_sign_convention"] == "positive_is_adverse"
    assert wire["execution"]["stage_durations_ns"] == {"admission": 20, "dispatch": 50}
    assert wire["execution"]["stage_durations_clock"] == "monotonic"


@pytest.mark.unit
def test_quality_missing_arrival_keeps_slippage_null_and_incomplete() -> None:
    completeness, coverage, source_refs = _metadata(_QUALITY_FIELDS)
    completeness["arrival_mid"] = "INCOMPLETE"
    completeness["slippage_amount"] = "INCOMPLETE"
    completeness["slippage_bps"] = "INCOMPLETE"
    record = _complete_quality_record(
        values={"arrival_mid": None, "slippage_amount": None, "slippage_bps": None},
        field_completeness=completeness,
        field_coverage_ns=coverage,
        field_source_refs=source_refs,
    )

    wire = record.to_wire()

    assert wire["completeness"] == "INCOMPLETE"
    assert wire["arrival"]["mid"] is None
    assert wire["execution"]["slippage_amount"] is None
    assert wire["execution"]["slippage_bps"] is None


@pytest.mark.unit
def test_quality_complete_missing_side_is_downgraded() -> None:
    record = replace(_complete_quality_record(), side=None)

    assert record.to_wire()["completeness"] == "INCOMPLETE"


@pytest.mark.unit
@pytest.mark.parametrize(
    "factory",
    [
        lambda: AccountSnapshot(
            _scope(), 200, "provider.account.snapshot", "COMPLETE", equity=True
        ),
        lambda: ExecutionQualityRecord(
            "intent-1", 200, "provider.execution.observation", "COMPLETE", fee=True
        ),
        lambda: AccountSnapshot(
            _scope(), 200, "provider.account.snapshot", "COMPLETE", equity=Decimal("NaN")
        ),
        lambda: ExecutionQualityRecord(
            "intent-1",
            200,
            "provider.execution.observation",
            "COMPLETE",
            epoch=True,
        ),
        lambda: ExecutionQualityRecord(
            "intent-1",
            200,
            "provider.execution.observation",
            "INCOMPLETE",
            scope=_scope(),
            generation="generation-7",
            epoch=3,
            field_coverage_ns={"arrival_mid": (100, 201)},
        ),
    ],
)
def test_economic_fact_scalars_reject_bool_and_nonfinite_values(factory) -> None:
    with pytest.raises(ContractValidationError):
        factory()


@pytest.mark.unit
def test_legacy_constructors_remain_valid_but_wire_export_requires_scope_generation_epoch() -> None:
    account = AccountSnapshot(
        _scope(), 200, "provider.account.snapshot", "COMPLETE", equity=Decimal("10")
    )
    quality = ExecutionQualityRecord("intent-1", 200, "provider.execution.observation", "COMPLETE")

    assert account.equity == Decimal("10")
    assert quality.intent_id == "intent-1"
    with pytest.raises(ContractValidationError, match="generation is required"):
        account.to_wire()
    with pytest.raises(ContractValidationError, match="scope is required"):
        quality.to_wire()
