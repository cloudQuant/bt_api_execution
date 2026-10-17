from __future__ import annotations

import sqlite3
from decimal import Decimal
from time import time_ns
from types import SimpleNamespace

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope
from bt_api_risk import AccountScope, DurableRiskGate, RiskPolicy
from bt_api_risk.core.instrument import InstrumentRiskMetadata, InstrumentRiskRegistry

from bt_api_execution import (
    PAPER_ENVIRONMENT,
    PAPER_MODEL_REF,
    PAPER_PROVIDER,
    ExecutionAdmissionValidatorRegistry,
    ExecutionBasicAdmissionGate,
    ExecutionProviderValidatorV1,
    ExecutionScope,
    OrderType,
    PaperInstrumentV1,
    PaperMatchingPort,
    PaperOpeningPositionV1,
    PaperOwnerDomainV1,
    PositionEffect,
    Side,
    SqliteExecutionStore,
    TimeInForce,
)
from bt_api_execution.errors import DurableStoreError
from bt_api_execution.paper_execution_risk import PaperExecutionCloseAuthority
from bt_api_execution.paper_risk import PaperRiskAdmissionAdapter
from bt_api_execution.service_order_entry import SUBMIT_OPERATION, ExecutionOrderEntryService

_DOMAIN_REF = "paper-recovery-domain-v1-test"
_ACCOUNT_REF = "paper-recovery-account-v1-test"
_STRATEGY_REF = "paper-recovery-strategy-v1-test"
_INSTRUMENT = "BTC-USDT"
_WIRE_SCOPE = WireScope(
    _DOMAIN_REF,
    account_ref=_ACCOUNT_REF,
    strategy_ref=_STRATEGY_REF,
)
_EXECUTION_SCOPE = ExecutionScope(
    PAPER_PROVIDER,
    PAPER_ENVIRONMENT,
    _ACCOUNT_REF,
    _STRATEGY_REF,
    "paper-recovery-day-v1",
)
_IDENTITY = WireIdentity(WireRole.STRATEGY_CLIENT, "paper-recovery-client-v1", _STRATEGY_REF)


class _QuoteSource:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def read_quote(self, instrument: str):
        self.calls.append(instrument)
        return None


class _FallbackJournal:
    def dispatch_exposure_resolution_guard(self, *_args, **_kwargs):
        raise AssertionError("Paper startup recovery must not use the Risk journal fallback")


def _domain() -> PaperOwnerDomainV1:
    return PaperOwnerDomainV1(
        execution_domain_ref=_DOMAIN_REF,
        simulation_account_ref=_ACCOUNT_REF,
        experiment_ref="paper-recovery-experiment-v1-test",
        strategy_ref=_STRATEGY_REF,
        model_ref=PAPER_MODEL_REF,
        model_version="1",
        quote_source_ref="paper-recovery-quotes-v1-test",
        cash_currency="USDT",
        initial_cash=Decimal("100"),
        initial_fees=Decimal("0"),
        fee_rate=Decimal("0.001"),
        fee_quantum=Decimal("0.0001"),
        max_quote_age_ns=30_000_000_000,
        instruments=(
            PaperInstrumentV1(
                _INSTRUMENT,
                "paper-recovery-instrument-v1",
                "BTC",
                "USDT",
                Decimal("0.25"),
                Decimal("0.01"),
            ),
        ),
        opening_positions=(
            PaperOpeningPositionV1(
                _INSTRUMENT,
                Decimal("1.00"),
                Decimal("10.00"),
            ),
        ),
    )


def _build_owner(tmp_path, *, initialize_allocation: bool = True):
    domain = _domain()
    store = SqliteExecutionStore(tmp_path / "paper-execution.sqlite3")
    quote_source = _QuoteSource()
    matcher = PaperMatchingPort(domain, quote_source)
    risk_scope = AccountScope(PAPER_PROVIDER, _ACCOUNT_REF, PAPER_ENVIRONMENT)
    fallback = _FallbackJournal()
    now_ns = time_ns()
    metadata = InstrumentRiskMetadata(
        instrument=_INSTRUMENT,
        metadata_version="paper-recovery-instrument-v1",
        as_of_ns=now_ns - 1_000_000,
        expires_at_ns=now_ns + 3_600_000_000_000,
        tick_size=Decimal("0.01"),
        quantity_step=Decimal("0.25"),
        contract_multiplier=Decimal("1"),
        max_gross_notional=Decimal("1000"),
        quantity_unit="BTC",
        valuation_unit="USDT",
        quote_currency="USDT",
    )
    instrument_registry = InstrumentRiskRegistry((metadata,))
    close_authority = PaperExecutionCloseAuthority(
        store,
        _EXECUTION_SCOPE,
        domain,
        matcher,
        risk_scope,
        instrument_registry,
        fallback,
    )
    risk_gate = DurableRiskGate(
        tmp_path / "paper-risk.sqlite3",
        RiskPolicy(
            policy_id="paper-recovery-policy-v1-test",
            max_increase_notional=Decimal("1000"),
            max_increase_count=10,
            require_strategy_allocation=True,
            notional_unit="USDT",
        ),
        execution_journal_authority=fallback,
        gross_exposure_authority=close_authority,
    )
    if initialize_allocation:
        risk_gate.set_strategy_allocation(
            risk_scope,
            _STRATEGY_REF,
            "paper-recovery-allocation-v1-test",
            max_notional=Decimal("500"),
            notional_unit="USDT",
        )
    admission = PaperRiskAdmissionAdapter(
        risk_gate,
        domain,
        matcher,
        close_authority=close_authority,
        execution_store=store,
        execution_scope=_EXECUTION_SCOPE,
    )
    validator = ExecutionProviderValidatorV1(
        provider_id=PAPER_PROVIDER,
        validator_id="paper-recovery-market-fok-v1-test",
        version="1",
        dispatch_port=matcher,
        instrument_map={_INSTRUMENT: _INSTRUMENT},
        quantity_steps={_INSTRUMENT: Decimal("0.25")},
        max_quantities={_INSTRUMENT: Decimal("10")},
        price_ticks={_INSTRUMENT: Decimal("0.01")},
        supported_order_types=(OrderType.MARKET,),
        supported_time_in_force=(TimeInForce.FOK,),
        supported_actions=(
            (Side.BUY, PositionEffect.OPEN),
            (Side.SELL, PositionEffect.CLOSE),
        ),
        supports_reduce_only=True,
    )
    basic_admission = ExecutionBasicAdmissionGate(
        store,
        ExecutionAdmissionValidatorRegistry((validator,)),
        permit_ttl_ns=60_000_000_000,
    )
    service = ExecutionOrderEntryService(
        store,
        {_WIRE_SCOPE: _EXECUTION_SCOPE},
        matcher,
        admission_gate=admission,
        basic_admission_gate=basic_admission,
        paper_owner_domain=domain,
        writer_id="paper-recovery-owner-v1-test",
        lease_ttl_ns=60_000_000_000,
    )
    return SimpleNamespace(
        store=store,
        risk_gate=risk_gate,
        service=service,
        admission=admission,
        quote_source=quote_source,
        risk_scope=risk_scope,
    )


def _close_owner(owner) -> None:
    for order_facade, _cancellation_facade in tuple(owner.service._facades.values()):
        order_facade.close()
    owner.risk_gate.close()
    owner.store.close()


def _submission_readiness(owner):
    return owner.service.handle(
        SUBMIT_OPERATION,
        principal_ref=_IDENTITY.principal_ref,
        identity=_IDENTITY,
        scope=_WIRE_SCOPE,
        arguments={},
    )


def _cut(owner):
    return owner.risk_gate.get_closable_position_rebase(
        owner.risk_scope,
        _INSTRUMENT,
        _STRATEGY_REF,
        position_side="NET",
        position_bucket="ALL",
    )


def _rebase_count(path) -> int:
    connection = sqlite3.connect(path)
    try:
        row = connection.execute(
            "SELECT COUNT(*) FROM risk_closable_position_rebases"
        ).fetchone()
        return int(row[0])
    finally:
        connection.close()


@pytest.mark.unit
def test_paper_startup_recovery_rebases_full_book_before_ready_and_is_restart_idempotent(
    tmp_path,
):
    owner = _build_owner(tmp_path)
    risk_path = tmp_path / "paper-risk.sqlite3"
    try:
        assert _submission_readiness(owner)["code"] == "SERVICE_NOT_READY"
        assert owner.quote_source.calls == []

        assert owner.service.recover_interrupted_dispatches() == ()
        book = owner.store.read_paper_book(_EXECUTION_SCOPE)
        assert len(book.positions) == 1
        assert book.positions[0].instrument == _INSTRUMENT
        assert book.positions[0].quantity == Decimal("1.00")
        first_cut = _cut(owner)
        assert first_cut is not None
        assert first_cut.closable_quantity == Decimal("1.00")
        assert first_cut.execution_generation == book.execution_journal_generation
        assert first_cut.execution_cursor == book.execution_cursor
        assert owner.risk_gate.get_closable_position(
            owner.risk_scope,
            _INSTRUMENT,
            _STRATEGY_REF,
            position_side="NET",
            position_bucket="ALL",
        ).available_quantity == Decimal("1.00")
        assert _rebase_count(risk_path) == 1
        assert _submission_readiness(owner)["code"] == "INVALID_REQUEST"
        assert owner.quote_source.calls == []

        # Re-running the same trusted hook must verify the persisted cut rather
        # than consume or publish another copy of the opening position.
        assert owner.service.recover_interrupted_dispatches() == ()
        assert _cut(owner) == first_cut
        assert _rebase_count(risk_path) == 1
        assert owner.risk_gate.get_closable_position(
            owner.risk_scope,
            _INSTRUMENT,
            _STRATEGY_REF,
            position_side="NET",
            position_bucket="ALL",
        ).available_quantity == Decimal("1.00")
    finally:
        _close_owner(owner)

    # A new service/store/gate instance must read the same E book and Risk cut.
    restarted = _build_owner(tmp_path, initialize_allocation=False)
    try:
        assert _submission_readiness(restarted)["code"] == "SERVICE_NOT_READY"
        assert restarted.service.recover_interrupted_dispatches() == ()
        assert _cut(restarted) == first_cut
        assert _rebase_count(risk_path) == 1
        assert restarted.risk_gate.get_closable_position(
            restarted.risk_scope,
            _INSTRUMENT,
            _STRATEGY_REF,
            position_side="NET",
            position_bucket="ALL",
        ).available_quantity == Decimal("1.00")
        assert _submission_readiness(restarted)["code"] == "INVALID_REQUEST"
        assert restarted.quote_source.calls == []
    finally:
        _close_owner(restarted)


@pytest.mark.unit
def test_failed_repeated_paper_recovery_clears_existing_ready_state(tmp_path, monkeypatch):
    owner = _build_owner(tmp_path)
    try:
        assert owner.service.recover_interrupted_dispatches() == ()
        assert _submission_readiness(owner)["code"] == "INVALID_REQUEST"
        previous_cut = _cut(owner)
        assert previous_cut is not None

        def reject_recovery(*, writer_lease):
            assert writer_lease.scope_key == _EXECUTION_SCOPE.account_key
            raise DurableStoreError("injected full-book recovery failure")

        original_recovery = owner.admission.recover_paper_scope
        monkeypatch.setattr(owner.admission, "recover_paper_scope", reject_recovery)
        with pytest.raises(DurableStoreError, match="injected full-book recovery failure"):
            owner.service.recover_interrupted_dispatches()

        # A failed second recovery cannot keep serving from the previously
        # published ready marker even though the older cut remains persisted.
        assert _cut(owner) == previous_cut
        assert _submission_readiness(owner)["code"] == "SERVICE_NOT_READY"
        assert owner.quote_source.calls == []

        monkeypatch.setattr(owner.admission, "recover_paper_scope", original_recovery)
        assert owner.service.recover_interrupted_dispatches() == ()
        assert _cut(owner) == previous_cut
        assert _submission_readiness(owner)["code"] == "INVALID_REQUEST"
        assert owner.quote_source.calls == []
    finally:
        _close_owner(owner)
