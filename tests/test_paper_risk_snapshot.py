from __future__ import annotations

import hashlib
from dataclasses import replace
from decimal import Decimal
from time import time_ns

import pytest
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
    canonical_json,
)
from bt_api_execution.errors import ContractValidationError
from bt_api_execution.paper_risk import PaperRiskAdmissionAdapter
from bt_api_execution.paper_risk_snapshot import (
    PaperRiskAdmissionSnapshotV1,
    PaperRiskSnapshotStoreError,
)

_DOMAIN_REF = "paper-risk-snapshot-domain-v1-test"
_SIM_ACCOUNT = "paper-risk-snapshot-account-v1-test"
_STRATEGY = "paper-risk-snapshot-strategy-v1-test"
_EXECUTION_SCOPE = ExecutionScope(
    PAPER_PROVIDER, PAPER_ENVIRONMENT, _SIM_ACCOUNT, _STRATEGY, "20261017"
)
_RISK_SCOPE = AccountScope(PAPER_PROVIDER, _SIM_ACCOUNT, PAPER_ENVIRONMENT)
_OWNER_ID = "paper-risk-snapshot-owner-v1-test"


class _QuoteSource:
    def __init__(self, quote: PaperQuoteV1) -> None:
        self.quote = quote
        self.calls: list[str] = []

    def read_quote(self, instrument: str) -> PaperQuoteV1:
        self.calls.append(instrument)
        return self.quote


def _quote(*, as_of_ns: int | None = None, ask: str = "10.02") -> PaperQuoteV1:
    return PaperQuoteV1(
        source_ref="paper-risk-snapshot-quote-source-v1-test",
        generation="paper-risk-snapshot-quote-generation-v1-test",
        cursor=1,
        instrument="BTC-USDT",
        metadata_version="btc-usdt-v1",
        quote_currency="USDT",
        bid=Decimal("9.98"),
        ask=Decimal(ask),
        bid_quantity=Decimal("5"),
        ask_quantity=Decimal("5"),
        as_of_ns=time_ns() - 1_000_000 if as_of_ns is None else as_of_ns,
    )


def _domain(*, max_quote_age_ns: int = 30_000_000_000) -> PaperOwnerDomainV1:
    return PaperOwnerDomainV1(
        execution_domain_ref=_DOMAIN_REF,
        simulation_account_ref=_SIM_ACCOUNT,
        experiment_ref="paper-risk-snapshot-experiment-v1-test",
        strategy_ref=_STRATEGY,
        model_ref=PAPER_MODEL_REF,
        model_version="1",
        quote_source_ref="paper-risk-snapshot-quote-source-v1-test",
        cash_currency="USDT",
        initial_cash=Decimal("100"),
        initial_fees=Decimal("0"),
        fee_rate=Decimal("0.001"),
        fee_quantum=Decimal("0.0001"),
        max_quote_age_ns=max_quote_age_ns,
        instruments=(
            PaperInstrumentV1(
                "BTC-USDT", "btc-usdt-v1", "BTC", "USDT", Decimal("0.25"), Decimal("0.01")
            ),
        ),
        opening_positions=(),
    )


def _order() -> OrderIntent:
    return OrderIntent(
        intent_id="paper-risk-snapshot-buy-v1",
        scope=_EXECUTION_SCOPE,
        signal_id="paper-risk-snapshot-signal-v1",
        instrument="BTC-USDT",
        side=Side.BUY,
        position_effect=PositionEffect.OPEN,
        order_type=OrderType.MARKET,
        quantity=Decimal("1.25"),
        price=None,
        time_in_force=TimeInForce.FOK,
        reduce_only=False,
        metadata_version="btc-usdt-v1",
    )


def _composition(tmp_path, quote: PaperQuoteV1, *, max_quote_age_ns: int = 30_000_000_000):
    domain = _domain(max_quote_age_ns=max_quote_age_ns)
    source = _QuoteSource(quote)
    matcher = PaperMatchingPort(domain, source)
    store = SqliteExecutionStore(tmp_path / "paper-execution.sqlite3")
    gate = DurableRiskGate(
        tmp_path / "paper-risk.sqlite3",
        RiskPolicy(
            policy_id="paper-risk-snapshot-policy-v1-test",
            max_increase_notional=Decimal("1000"),
            max_increase_count=10,
            require_strategy_allocation=True,
            notional_unit="USDT",
        ),
    )
    gate.set_strategy_allocation(
        _RISK_SCOPE,
        _STRATEGY,
        "paper-risk-snapshot-allocation-v1",
        max_notional=Decimal("500"),
        notional_unit="USDT",
    )
    adapter = PaperRiskAdmissionAdapter(
        gate,
        domain,
        matcher,
        execution_store=store,
        execution_scope=_EXECUTION_SCOPE,
    )
    return store, gate, adapter, matcher, source


def _lease_and_record(store: SqliteExecutionStore, intent: OrderIntent):
    lease = store.acquire_or_renew_lease(
        _EXECUTION_SCOPE, _OWNER_ID, ttl_ns=60_000_000_000
    )
    record = store.admit_intent(intent, writer_lease=lease)
    assert record.state is ExecutionState.PENDING_ADMISSION
    return lease


def _close(store, gate, lease) -> None:
    if lease is not None:
        store.release_lease(
            _EXECUTION_SCOPE,
            _OWNER_ID,
            fencing_token=lease.fencing_token,
        )
    store.close()
    gate.close()


@pytest.mark.unit
def test_paper_risk_snapshot_survives_restart_before_claim_without_requoting(tmp_path):
    intent = _order()
    store1, gate1, adapter1, _matcher1, source1 = _composition(tmp_path, _quote())
    lease1 = _lease_and_record(store1, intent)
    try:
        first_permit = adapter1.reserve_for_execution(intent, writer_lease=lease1)
        assert source1.calls == ["BTC-USDT"]
        first_allocation = gate1.get_strategy_allocation(_RISK_SCOPE, _STRATEGY)
        assert first_allocation.reserved_notional == Decimal("12.5375")
    finally:
        _close(store1, gate1, lease1)

    # The restarted owner is offered a different quote. The persisted mapping
    # must retain the first quote and idempotent Risk permit without reading it.
    changed_quote = replace(_quote(ask="10.20"), cursor=2)
    store2, gate2, adapter2, matcher2, source2 = _composition(tmp_path, changed_quote)
    lease2 = _lease_and_record(store2, intent)
    try:
        second_permit = adapter2.reserve_for_execution(intent, writer_lease=lease2)
        assert second_permit.permit_id == first_permit.permit_id
        assert source2.calls == []
        assert gate2.get_strategy_allocation(_RISK_SCOPE, _STRATEGY).reserved_notional == Decimal(
            "12.5375"
        )

        pending = store2.activate_intent(
            intent.intent_id,
            _EXECUTION_SCOPE,
            second_permit.permit_id,
            writer_lease=lease2,
        )
        assert pending.state is ExecutionState.PENDING_DISPATCH
        claimed, did_claim = store2.claim_for_dispatch(
            intent.intent_id, _EXECUTION_SCOPE, writer_lease=lease2
        )
        assert did_claim and claimed.state is ExecutionState.DISPATCHING
        second_claim = adapter2.claim_for_dispatch_for_execution(
            second_permit.permit_id,
            intent,
            writer_lease=lease2,
        )
        assert second_claim.permit_id == first_permit.permit_id

        proposal = matcher2._propose(intent, authorization=None)
        assert proposal.refusal_code is None
        assert proposal.quote == source1.quote
        assert proposal.quote.ask == Decimal("10.02")
        assert source2.calls == []
    finally:
        _close(store2, gate2, lease2)


@pytest.mark.unit
def test_paper_risk_snapshot_is_idempotent_and_rejects_changed_content(tmp_path):
    intent = _order()
    store, gate, adapter, _matcher, source = _composition(tmp_path, _quote())
    lease = _lease_and_record(store, intent)
    try:
        permit1 = adapter.reserve_for_execution(intent, writer_lease=lease)
        snapshot = adapter._snapshot_store.load_for_reservation(intent, lease)
        assert snapshot is not None
        same = adapter._snapshot_store.create_for_reservation(
            intent, lease, dict(snapshot.body)
        )
        assert same.snapshot_sha256 == snapshot.snapshot_sha256

        changed = dict(snapshot.body)
        changed["allocation_revision"] = "changed-allocation-revision"
        with pytest.raises(PaperRiskSnapshotStoreError) as conflict:
            adapter._snapshot_store.create_for_reservation(intent, lease, changed)
        assert conflict.value.code == "PAPER_ADMISSION_SNAPSHOT_CONFLICT"

        permit2 = adapter.reserve_for_execution(intent, writer_lease=lease)
        assert permit2.permit_id == permit1.permit_id
        assert source.calls == ["BTC-USDT"]
        assert gate.get_strategy_allocation(_RISK_SCOPE, _STRATEGY).reserved_notional == Decimal(
            "12.5375"
        )
    finally:
        _close(store, gate, lease)


@pytest.mark.unit
def test_paper_risk_snapshot_rejects_nonfinite_persisted_allocation(tmp_path):
    intent = _order()
    store, gate, adapter, _matcher, _source = _composition(tmp_path, _quote())
    lease = _lease_and_record(store, intent)
    try:
        adapter.reserve_for_execution(intent, writer_lease=lease)
        snapshot = adapter._snapshot_store.load_for_reservation(intent, lease)
        assert snapshot is not None
        body = dict(snapshot.body)
        allocation = dict(body["allocation_snapshot"])
        allocation["available_notional"] = "NaN"
        body["allocation_snapshot"] = allocation
        digest = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        malformed = PaperRiskAdmissionSnapshotV1(body, digest)

        with pytest.raises(ContractValidationError):
            adapter._restore_snapshot(intent, malformed)
    finally:
        _close(store, gate, lease)


@pytest.mark.unit
def test_paper_risk_reserve_denial_discards_quote_but_ambiguous_error_keeps_it(
    tmp_path, monkeypatch
):
    intent = _order()
    store, gate, adapter, matcher, source = _composition(tmp_path, _quote())
    lease = _lease_and_record(store, intent)

    def denied(_intent):
        raise RiskAdmissionDeniedError("PAPER_TEST_DENIED")

    try:
        with monkeypatch.context() as patcher:
            patcher.setattr(adapter._risk, "reserve", denied)
            with pytest.raises(RiskAdmissionDeniedError):
                adapter.reserve_for_execution(intent, writer_lease=lease)
            assert intent.fingerprint not in matcher._risk_quotes
            assert intent.fingerprint not in matcher._risk_quote_required

        def ambiguous(_intent):
            raise RuntimeError("risk reserve result is ambiguous")

        with monkeypatch.context() as patcher:
            patcher.setattr(adapter._risk, "reserve", ambiguous)
            with pytest.raises(RuntimeError):
                adapter.reserve_for_execution(intent, writer_lease=lease)
        assert matcher._risk_quotes[intent.fingerprint] == source.quote
        assert intent.fingerprint in matcher._risk_quote_required

        permit = adapter.reserve_for_execution(intent, writer_lease=lease)
        assert permit.permit_id
        assert source.calls == ["BTC-USDT"]
    finally:
        _close(store, gate, lease)


@pytest.mark.unit
def test_paper_risk_claim_denial_discards_quote_and_lost_response_keeps_it(
    tmp_path, monkeypatch
):
    intent = _order()
    store, gate, adapter, matcher, source = _composition(tmp_path, _quote())
    lease = _lease_and_record(store, intent)
    try:
        permit = adapter.reserve_for_execution(intent, writer_lease=lease)
        store.activate_intent(
            intent.intent_id,
            _EXECUTION_SCOPE,
            permit.permit_id,
            writer_lease=lease,
        )
        record, did_claim = store.claim_for_dispatch(
            intent.intent_id, _EXECUTION_SCOPE, writer_lease=lease
        )
        assert did_claim and record.state is ExecutionState.DISPATCHING

        def denied(_permit_reference, _intent):
            raise RiskAdmissionDeniedError("PAPER_TEST_CLAIM_DENIED")

        with monkeypatch.context() as patcher:
            patcher.setattr(adapter._risk, "claim_for_dispatch", denied)
            with pytest.raises(RiskAdmissionDeniedError):
                adapter.claim_for_dispatch_for_execution(
                    permit.permit_id, intent, writer_lease=lease
                )
        assert intent.fingerprint not in matcher._risk_quotes
        assert intent.fingerprint not in matcher._risk_quote_required

        original_claim = adapter._risk.claim_for_dispatch

        def claim_then_lose_response(permit_reference, order):
            original_claim(permit_reference, order)
            raise RuntimeError("risk claim response was lost")

        with monkeypatch.context() as patcher:
            patcher.setattr(adapter._risk, "claim_for_dispatch", claim_then_lose_response)
            with pytest.raises(RuntimeError):
                adapter.claim_for_dispatch_for_execution(
                    permit.permit_id, intent, writer_lease=lease
                )
        assert matcher._risk_quotes[intent.fingerprint] == source.quote
        assert intent.fingerprint in matcher._risk_quote_required
        assert store.get(intent.intent_id, scope=_EXECUTION_SCOPE).state is ExecutionState.DISPATCHING
        assert source.calls == ["BTC-USDT"]
    finally:
        _close(store, gate, lease)


@pytest.mark.unit
def test_paper_risk_mapping_errors_discard_quote_before_gate_effects(tmp_path, monkeypatch):
    intent = _order()
    store, gate, adapter, matcher, source = _composition(tmp_path, _quote())
    lease = _lease_and_record(store, intent)
    try:
        with monkeypatch.context() as patcher:
            patcher.setattr(adapter._risk, "_map_intent", lambda _order: object())
            with pytest.raises(RiskAdmissionDeniedError) as reserve_error:
                adapter.reserve_for_execution(intent, writer_lease=lease)
        assert reserve_error.value.code == "PAPER_ADMISSION_MAPPING_INVALID"
        assert intent.fingerprint not in matcher._risk_quotes
        assert intent.fingerprint not in matcher._risk_quote_required

        permit = adapter.reserve_for_execution(intent, writer_lease=lease)
        store.activate_intent(
            intent.intent_id,
            _EXECUTION_SCOPE,
            permit.permit_id,
            writer_lease=lease,
        )
        record, did_claim = store.claim_for_dispatch(
            intent.intent_id, _EXECUTION_SCOPE, writer_lease=lease
        )
        assert did_claim and record.state is ExecutionState.DISPATCHING

        with monkeypatch.context() as patcher:
            patcher.setattr(adapter._risk, "_map_intent", lambda _order: object())
            with pytest.raises(RiskAdmissionDeniedError) as claim_error:
                adapter.claim_for_dispatch_for_execution(
                    permit.permit_id, intent, writer_lease=lease
                )
        assert claim_error.value.code == "PAPER_ADMISSION_MAPPING_INVALID"
        assert intent.fingerprint not in matcher._risk_quotes
        assert intent.fingerprint not in matcher._risk_quote_required
        assert (
            store.get(intent.intent_id, scope=_EXECUTION_SCOPE).state
            is ExecutionState.DISPATCHING
        )

        original_claim = adapter.claim_for_dispatch_for_execution(
            permit.permit_id, intent, writer_lease=lease
        )
        assert original_claim
        assert source.calls == ["BTC-USDT"]
    finally:
        _close(store, gate, lease)


@pytest.mark.unit
def test_paper_risk_quote_stale_eviction_clears_required_markers(monkeypatch):
    import bt_api_execution.paper_execution as paper_execution_module

    monkeypatch.setattr(paper_execution_module, "time_ns", lambda: 10_000)
    domain = _domain(max_quote_age_ns=100)
    current_quote = _quote(as_of_ns=9_999)
    source = _QuoteSource(current_quote)
    matcher = PaperMatchingPort(domain, source)
    intent = _order()
    stale_keys = {f"{index:064x}" for index in range(1024)}
    stale_quote = _quote(as_of_ns=1)
    matcher._risk_quotes.update({key: stale_quote for key in stale_keys})
    matcher._risk_quote_required.update(stale_keys)

    assert matcher.prepare_risk_quote(intent) == current_quote
    assert matcher._risk_quote_required == {intent.fingerprint}
    assert set(matcher._risk_quotes) == {intent.fingerprint}


@pytest.mark.unit
def test_paper_risk_snapshot_expiry_blocks_claim_without_requoting(tmp_path, monkeypatch):
    import bt_api_execution.paper_execution as paper_execution_module
    import bt_api_execution.paper_risk as paper_risk_module

    monkeypatch.setattr(paper_execution_module, "time_ns", lambda: 950)
    monkeypatch.setattr(paper_risk_module, "time_ns", lambda: 950)
    intent = _order()
    store, gate, adapter, _matcher, source = _composition(
        tmp_path, _quote(as_of_ns=900), max_quote_age_ns=100
    )
    lease = _lease_and_record(store, intent)
    try:
        permit = adapter.reserve_for_execution(intent, writer_lease=lease)
        assert source.calls == ["BTC-USDT"]
        store.activate_intent(
            intent.intent_id,
            _EXECUTION_SCOPE,
            permit.permit_id,
            writer_lease=lease,
        )
        record, did_claim = store.claim_for_dispatch(
            intent.intent_id, _EXECUTION_SCOPE, writer_lease=lease
        )
        assert did_claim and record.state is ExecutionState.DISPATCHING

        monkeypatch.setattr(paper_execution_module, "time_ns", lambda: 1000)
        monkeypatch.setattr(paper_risk_module, "time_ns", lambda: 1000)
        with pytest.raises(RiskAdmissionDeniedError) as expired:
            adapter.claim_for_dispatch_for_execution(
                permit.permit_id,
                intent,
                writer_lease=lease,
            )
        assert expired.value.code == "PAPER_ADMISSION_SNAPSHOT_EXPIRED"
        assert source.calls == ["BTC-USDT"]
        assert store.get(intent.intent_id, scope=_EXECUTION_SCOPE).state is ExecutionState.DISPATCHING
    finally:
        _close(store, gate, lease)
