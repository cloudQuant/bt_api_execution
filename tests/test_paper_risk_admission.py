from __future__ import annotations

from decimal import Decimal
from time import time_ns

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope
from bt_api_risk import (
    AccountScope,
    DurableRiskGate,
    RiskAdmissionDeniedError,
    RiskPolicy,
)

from bt_api_execution import (
    PAPER_ENVIRONMENT,
    PAPER_MODEL_REF,
    PAPER_PROVIDER,
    ExecutionAdmissionValidatorRegistry,
    ExecutionBasicAdmissionGate,
    ExecutionProviderValidatorV1,
    ExecutionScope,
    ExecutionState,
    OrderIntent,
    OrderType,
    PaperInstrumentV1,
    PaperMatchingPort,
    PaperOwnerDomainV1,
    PaperQuoteV1,
    PositionEffect,
    Side,
    SqliteExecutionStore,
    TimeInForce,
)
from bt_api_execution.paper_risk import PaperRiskAdmissionAdapter
from bt_api_execution.service_order_entry import SUBMIT_OPERATION, ExecutionOrderEntryService

_DOMAIN_REF = "paper-risk-domain-v1-test"
_SIM_ACCOUNT = "paper-risk-account-v1-test"
_STRATEGY = "paper-risk-strategy-v1-test"
_WIRE_SCOPE = WireScope(_DOMAIN_REF, account_ref=_SIM_ACCOUNT, strategy_ref=_STRATEGY)
_EXECUTION_SCOPE = ExecutionScope(
    PAPER_PROVIDER, PAPER_ENVIRONMENT, _SIM_ACCOUNT, _STRATEGY, "20261017"
)
_IDENTITY = WireIdentity(WireRole.STRATEGY_CLIENT, "paper-risk-principal-v1-test", _STRATEGY)


class _QuoteSource:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.quote = PaperQuoteV1(
            source_ref="paper-risk-quote-source-v1-test",
            generation="quote-generation-v1-test",
            cursor=1,
            instrument="BTC-USDT",
            metadata_version="btc-usdt-v1",
            quote_currency="USDT",
            bid=Decimal("9.98"),
            ask=Decimal("10.02"),
            bid_quantity=Decimal("5"),
            ask_quantity=Decimal("5"),
            as_of_ns=time_ns() - 1_000_000,
        )

    def read_quote(self, instrument: str) -> PaperQuoteV1:
        self.calls.append(instrument)
        return self.quote


def _domain() -> PaperOwnerDomainV1:
    return PaperOwnerDomainV1(
        execution_domain_ref=_DOMAIN_REF,
        simulation_account_ref=_SIM_ACCOUNT,
        experiment_ref="paper-risk-experiment-v1-test",
        strategy_ref=_STRATEGY,
        model_ref=PAPER_MODEL_REF,
        model_version="1",
        quote_source_ref="paper-risk-quote-source-v1-test",
        cash_currency="USDT",
        initial_cash=Decimal("100"),
        initial_fees=Decimal("0"),
        fee_rate=Decimal("0.001"),
        fee_quantum=Decimal("0.0001"),
        max_quote_age_ns=30_000_000_000,
        instruments=(
            PaperInstrumentV1(
                "BTC-USDT", "btc-usdt-v1", "BTC", "USDT", Decimal("0.25"), Decimal("0.01")
            ),
        ),
        opening_positions=(),
    )


def _risk_adapter(path, domain, matcher, store):
    scope = AccountScope(PAPER_PROVIDER, _SIM_ACCOUNT, PAPER_ENVIRONMENT)
    gate = DurableRiskGate(
        path,
        RiskPolicy(
            policy_id="paper-risk-policy-v1-test",
            max_increase_notional=Decimal("1000"),
            max_increase_count=10,
            require_strategy_allocation=True,
            notional_unit="USDT",
        ),
    )
    gate.set_strategy_allocation(
        scope,
        _STRATEGY,
        "paper-allocation-v1",
        max_notional=Decimal("500"),
        notional_unit="USDT",
    )
    return (
        gate,
        PaperRiskAdmissionAdapter(
            gate,
            domain,
            matcher,
            execution_store=store,
            execution_scope=_EXECUTION_SCOPE,
        ),
        scope,
    )


def _order(intent_id: str, *, side: Side = Side.BUY) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        scope=_EXECUTION_SCOPE,
        signal_id=f"signal-{intent_id}",
        instrument="BTC-USDT",
        side=side,
        position_effect=PositionEffect.OPEN if side is Side.BUY else PositionEffect.CLOSE,
        order_type=OrderType.MARKET,
        quantity=Decimal("1.25"),
        price=None,
        time_in_force=TimeInForce.FOK,
        reduce_only=side is Side.SELL,
        metadata_version="btc-usdt-v1",
    )


@pytest.mark.unit
def test_managed_paper_risk_reserves_same_quote_used_by_durable_fill(tmp_path):
    domain = _domain()
    quotes = _QuoteSource()
    matcher = PaperMatchingPort(domain, quotes)
    store = SqliteExecutionStore(tmp_path / "paper-execution.sqlite3")
    risk_gate, risk_adapter, risk_scope = _risk_adapter(
        tmp_path / "paper-risk-ledger.sqlite3", domain, matcher, store
    )
    try:
        validator = ExecutionProviderValidatorV1(
            provider_id=PAPER_PROVIDER,
            validator_id="paper-risk-market-fok-v1",
            version="1",
            dispatch_port=matcher,
            instrument_map={"BTC-USDT": "BTC-USDT"},
            quantity_steps={"BTC-USDT": Decimal("0.25")},
            max_quantities={"BTC-USDT": Decimal("10")},
            price_ticks={"BTC-USDT": Decimal("0.01")},
            supported_order_types=(OrderType.MARKET,),
            supported_time_in_force=(TimeInForce.FOK,),
            supported_actions=((Side.BUY, PositionEffect.OPEN),),
            supports_reduce_only=False,
        )
        basic_gate = ExecutionBasicAdmissionGate(
            store,
            ExecutionAdmissionValidatorRegistry((validator,)),
            permit_ttl_ns=60_000_000_000,
        )
        service = ExecutionOrderEntryService(
            store,
            {_WIRE_SCOPE: _EXECUTION_SCOPE},
            matcher,
            admission_gate=risk_adapter,
            basic_admission_gate=basic_gate,
            paper_owner_domain=domain,
            writer_id="paper-risk-owner-v1-test",
            lease_ttl_ns=60_000_000_000,
        )
        service.recover_interrupted_dispatches()

        result = service.handle(
            SUBMIT_OPERATION,
            principal_ref=_IDENTITY.principal_ref,
            identity=_IDENTITY,
            scope=_WIRE_SCOPE,
            arguments={
                "intent_id": "paper-risk-buy-v1",
                "signal_id": "signal-paper-risk-buy-v1",
                "instrument": "BTC-USDT",
                "side": "BUY",
                "position_effect": "OPEN",
                "order_type": "MARKET",
                "quantity": "1.25",
                "price": None,
                "time_in_force": "FOK",
                "reduce_only": False,
                "metadata_version": "btc-usdt-v1",
                "deadline_ns": None,
                "tags": {},
            },
        )
        record = store.get("paper-risk-buy-v1", scope=_EXECUTION_SCOPE)
        receipt = store.get_paper_match_receipt("paper-risk-buy-v1", scope=_EXECUTION_SCOPE)
        allocation = risk_gate.get_strategy_allocation(risk_scope, _STRATEGY)

        assert result["ok"] is True and result["code"] == "OK"
        assert record is not None and record.state is ExecutionState.FILLED
        assert receipt is not None and receipt.quote == quotes.quote
        assert receipt.fill_price == Decimal("10.02")
        assert receipt.fee_amount == Decimal("0.0125")
        assert quotes.calls == ["BTC-USDT"]
        assert risk_adapter.is_bound_to(risk_gate)
        assert risk_adapter.risk_gate is risk_gate
        assert risk_adapter.is_bound_to_composition(risk_gate, domain, matcher)
        assert not risk_adapter.is_bound_to_composition(risk_gate, _domain(), matcher)
        assert not risk_adapter.is_bound_to_composition(risk_gate, domain, PaperMatchingPort(domain, quotes))
        assert risk_adapter.policy_binding.policy_id == "paper-risk-policy-v1-test"
        assert allocation.scope == risk_scope
        assert allocation.strategy_id == _STRATEGY
        assert allocation.used_notional == Decimal("12.5375")
        assert allocation.reserved_notional == Decimal("0")

        close = _order("paper-risk-close-v1", side=Side.SELL)
        with pytest.raises(RiskAdmissionDeniedError) as denied:
            risk_adapter.reserve(close)
        assert denied.value.code == "PAPER_EXECUTION_CONTEXT_REQUIRED"
        assert quotes.calls == ["BTC-USDT"]
        assert risk_gate.get_strategy_allocation(risk_scope, _STRATEGY).used_notional == Decimal(
            "12.5375"
        )
    finally:
        store.close()
        risk_gate.close()

