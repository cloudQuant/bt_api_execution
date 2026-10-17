from __future__ import annotations

import builtins
import json
import sqlite3
from decimal import Decimal
from time import time_ns
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

import bt_api_execution.store as store_module
from bt_api_execution import (
    PAPER_ENVIRONMENT,
    PAPER_MODEL_REF,
    PAPER_PROVIDER,
    ExecutionAdmissionValidatorRegistry,
    ExecutionBasicAdmissionGate,
    ExecutionProviderValidatorV1,
    ExecutionScope,
    ExecutionState,
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
from bt_api_execution.errors import ContractValidationError, DurableStoreError
from bt_api_execution.paper_execution import (
    PAPER_FEE_MODEL_REF,
    PAPER_FEE_MODEL_VERSION,
    PAPER_MATCH_RECEIPT_SCHEMA_V2,
    PAPER_MATCH_TIME_SOURCE_V2,
    PaperMatchReceiptV1,
    PaperMatchReceiptV2,
)
from bt_api_execution.service_order_entry import SUBMIT_OPERATION, ExecutionOrderEntryService

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

_DOMAIN_REF = "paper-domain-v2-test"
_SIM_ACCOUNT = "sim-account-v2-test"
_STRATEGY = "strategy-v2-test"
_WIRE_SCOPE = WireScope(_DOMAIN_REF, account_ref=_SIM_ACCOUNT, strategy_ref=_STRATEGY)
_EXECUTION_SCOPE = ExecutionScope(
    PAPER_PROVIDER, PAPER_ENVIRONMENT, _SIM_ACCOUNT, _STRATEGY, "20261016"
)
_IDENTITY = WireIdentity(WireRole.STRATEGY_CLIENT, "paper-v2-principal", _STRATEGY)


def _domain(*, available_quote_age_ns: int = 30_000_000_000) -> PaperOwnerDomainV1:
    return PaperOwnerDomainV1(
        execution_domain_ref=_DOMAIN_REF,
        simulation_account_ref=_SIM_ACCOUNT,
        experiment_ref="paper-experiment-v2-test",
        strategy_ref=_STRATEGY,
        model_ref=PAPER_MODEL_REF,
        model_version="1",
        quote_source_ref="synthetic-quote-v2-test",
        cash_currency="USDT",
        initial_cash=Decimal("100"),
        initial_fees=Decimal("0"),
        fee_rate=Decimal("0.001"),
        fee_quantum=Decimal("0.0001"),
        max_quote_age_ns=available_quote_age_ns,
        instruments=(
            PaperInstrumentV1(
                "BTC-USDT", "instrument-v1", "BTC", "USDT", Decimal("0.25"), Decimal("0.01")
            ),
        ),
        opening_positions=(),
    )


class _SyntheticQuoteSource:
    def __init__(
        self,
        *,
        available: bool = True,
        quote_time_ns: Callable[[], int] | None = None,
    ) -> None:
        self.available = available
        self.calls: list[str] = []
        self.quote_time_ns = quote_time_ns

    def read_quote(self, instrument: str) -> PaperQuoteV1 | None:
        self.calls.append(instrument)
        if not self.available:
            return None
        return PaperQuoteV1(
            source_ref="synthetic-quote-v2-test",
            generation="quote-generation-v2-test",
            cursor=len(self.calls),
            instrument=instrument,
            metadata_version="instrument-v1",
            quote_currency="USDT",
            bid=Decimal("9.98"),
            ask=Decimal("10.02"),
            bid_quantity=Decimal("5"),
            ask_quantity=Decimal("5"),
            # The injected store clock starts slightly ahead of wall clock.
            as_of_ns=(
                time_ns() - 1_000_000 if self.quote_time_ns is None else self.quote_time_ns()
            ),
        )


class _IncrementingClock:
    def __init__(self) -> None:
        self.value = time_ns() + 1_000_000_000

    def time_ns(self) -> int:
        self.value += 10_000
        return self.value


class _CommitExpiryClock:
    """Keep a quote fresh at match time, then age it before event commit."""

    def __init__(self) -> None:
        self.base_ns = time_ns() + 1_000_000_000
        self.last_ns = self.base_ns
        self._scheduled: list[int] = []
        self.after_quote_ns: list[int] = []
        self.quote_armed = False

    def time_ns(self) -> int:
        if self._scheduled:
            self.last_ns = self._scheduled.pop(0)
        if self.quote_armed:
            self.after_quote_ns.append(self.last_ns)
        return self.last_ns

    def quote_time_ns(self) -> int:
        # record_paper_match_with_event samples transaction time, match time,
        # then event creation time. The quote equals the match sample; by the
        # third sample it is outside the configured age window.
        self._scheduled.extend((self.base_ns, self.base_ns + 100, self.base_ns + 2_000))
        self.quote_armed = True
        return self.base_ns + 100


def _build_service(
    path: Path,
    *,
    quote_source: _SyntheticQuoteSource | None = None,
    domain: PaperOwnerDomainV1 | None = None,
    admission_gate_factory: Callable[
        [SqliteExecutionStore, PaperOwnerDomainV1, PaperMatchingPort], object
    ]
    | None = None,
) -> tuple[SqliteExecutionStore, ExecutionOrderEntryService, _SyntheticQuoteSource]:
    source = _SyntheticQuoteSource() if quote_source is None else quote_source
    domain = _domain() if domain is None else domain
    store = SqliteExecutionStore(path)
    try:
        port = PaperMatchingPort(domain, source)
        validator = ExecutionProviderValidatorV1(
            provider_id=PAPER_PROVIDER,
            validator_id="paper-v2-market-fok",
            version="1",
            dispatch_port=port,
            instrument_map={"BTC-USDT": "BTC-USDT"},
            quantity_steps={"BTC-USDT": Decimal("0.25")},
            max_quantities={"BTC-USDT": Decimal("10")},
            price_ticks={"BTC-USDT": Decimal("0.01")},
            supported_order_types=(OrderType.MARKET,),
            supported_time_in_force=(TimeInForce.FOK,),
            supported_actions=(
                (Side.BUY, PositionEffect.OPEN),
                (Side.SELL, PositionEffect.CLOSE),
            ),
            supports_reduce_only=True,
        )
        gate = ExecutionBasicAdmissionGate(
            store,
            ExecutionAdmissionValidatorRegistry((validator,)),
            permit_ttl_ns=60_000_000_000,
        )
        admission_gate = (
            None
            if admission_gate_factory is None
            else admission_gate_factory(store, domain, port)
        )
        service = ExecutionOrderEntryService(
            store,
            {_WIRE_SCOPE: _EXECUTION_SCOPE},
            port,
            admission_gate=admission_gate,
            basic_admission_gate=gate,
            paper_owner_domain=domain,
            writer_id="paper-v2-test-owner",
            lease_ttl_ns=60_000_000_000,
        )
        service.recover_interrupted_dispatches()
        return store, service, source
    except Exception:
        store.close()
        raise


def _make_risk_adapter(
    store: SqliteExecutionStore,
    domain: PaperOwnerDomainV1,
    dispatch: PaperMatchingPort,
    risk_path: Path,
    *,
    bound_domain: PaperOwnerDomainV1 | None = None,
    bound_dispatch: PaperMatchingPort | None = None,
    bound_store: SqliteExecutionStore | None = None,
    bound_scope: ExecutionScope | None = None,
) -> tuple[object, object, type[object]]:
    from bt_api_risk import AccountScope, DurableRiskGate, RiskPolicy

    from bt_api_execution.paper_risk import PaperRiskAdmissionAdapter

    selected_domain = domain if bound_domain is None else bound_domain
    selected_dispatch = dispatch if bound_dispatch is None else bound_dispatch
    selected_store = store if bound_store is None else bound_store
    selected_scope = _EXECUTION_SCOPE if bound_scope is None else bound_scope
    risk_scope = AccountScope(
        PAPER_PROVIDER,
        selected_domain.simulation_account_ref,
        PAPER_ENVIRONMENT,
    )
    risk_gate = DurableRiskGate(
        risk_path,
        RiskPolicy(
            policy_id=f"paper-v2-constructor-{risk_path.stem}",
            max_increase_notional=Decimal("1000"),
            max_increase_count=10,
            require_strategy_allocation=True,
            notional_unit=selected_domain.cash_currency,
        ),
    )
    risk_gate.set_strategy_allocation(
        risk_scope,
        selected_domain.strategy_ref,
        f"paper-v2-allocation-{risk_path.stem}",
        max_notional=Decimal("500"),
        notional_unit=selected_domain.cash_currency,
    )
    adapter = PaperRiskAdmissionAdapter(
        risk_gate,
        selected_domain,
        selected_dispatch,
        execution_store=selected_store,
        execution_scope=selected_scope,
    )
    return risk_gate, adapter, PaperRiskAdmissionAdapter


def test_managed_paper_accepts_exact_risk_adapter_with_basic_admission(tmp_path) -> None:
    risk_gates = []
    adapter_types = []

    def build_adapter(store, domain, dispatch):
        risk_gate, adapter, adapter_type = _make_risk_adapter(
            store,
            domain,
            dispatch,
            tmp_path / "paper-risk-owner.sqlite",
        )
        risk_gates.append(risk_gate)
        adapter_types.append(adapter_type)
        return adapter

    store, service, _source = _build_service(
        tmp_path / "paper-execution-owner.sqlite",
        admission_gate_factory=build_adapter,
    )
    try:
        assert type(service._admission_gate) is adapter_types[0]
        assert service._admission_gate.is_bound_to_composition(
            risk_gates[0],
            service._paper_owner_domain,
            service._dispatch,
            store=store,
            execution_scope=_EXECUTION_SCOPE,
        )
        assert service._basic_admission_gate is not None
        assert service._allow_unprotected is False
    finally:
        store.close()
        for risk_gate in risk_gates:
            risk_gate.close()


def test_managed_paper_rejects_generic_structural_risk_gate(tmp_path) -> None:
    with pytest.raises(ContractValidationError, match="canonical Risk adapter"):
        _build_service(
            tmp_path / "paper-generic-risk-owner.sqlite",
            admission_gate_factory=lambda *_: SimpleNamespace(reserve=lambda _intent: None),
        )


@pytest.mark.parametrize("mismatch", ("domain", "dispatch", "store", "scope"))
def test_managed_paper_rejects_risk_adapter_bound_to_other_composition(
    tmp_path, mismatch: str
) -> None:
    risk_gates = []
    extra_stores = []

    def build_adapter(store, domain, dispatch):
        bound_domain = domain
        bound_dispatch = dispatch
        bound_store = store
        bound_scope = _EXECUTION_SCOPE
        if mismatch == "domain":
            bound_domain = _domain()
            bound_dispatch = PaperMatchingPort(bound_domain, _SyntheticQuoteSource())
        elif mismatch == "dispatch":
            bound_dispatch = PaperMatchingPort(domain, _SyntheticQuoteSource())
        elif mismatch == "store":
            bound_store = SqliteExecutionStore(tmp_path / "other-execution-store.sqlite")
            extra_stores.append(bound_store)
        elif mismatch == "scope":
            bound_scope = ExecutionScope(
                PAPER_PROVIDER,
                PAPER_ENVIRONMENT,
                _SIM_ACCOUNT,
                _STRATEGY,
                "different-trading-day",
            )
        risk_gate, adapter, _adapter_type = _make_risk_adapter(
            store,
            domain,
            dispatch,
            tmp_path / f"paper-risk-{mismatch}.sqlite",
            bound_domain=bound_domain,
            bound_dispatch=bound_dispatch,
            bound_store=bound_store,
            bound_scope=bound_scope,
        )
        risk_gates.append(risk_gate)
        return adapter

    try:
        with pytest.raises(ContractValidationError, match="trusted owner composition"):
            _build_service(
                tmp_path / f"paper-mismatched-{mismatch}.sqlite",
                admission_gate_factory=build_adapter,
            )
    finally:
        for store in extra_stores:
            store.close()
        for risk_gate in risk_gates:
            risk_gate.close()


def test_managed_paper_e_only_does_not_import_optional_risk_adapter(tmp_path, monkeypatch) -> None:
    observed_optional_imports = []
    original_import = builtins.__import__

    def observe_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "bt_api_risk" or (name == "paper_risk" and level == 1):
            observed_optional_imports.append((name, level))
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", observe_import)
    store, service, _source = _build_service(tmp_path / "paper-e-only-owner.sqlite")
    try:
        assert service._admission_gate is None
        assert service._basic_admission_gate is not None
        assert service._paper_owner_domain is not None
        assert service._allow_unprotected is False
        assert observed_optional_imports == []
    finally:
        store.close()


def _arguments(intent_id: str = "paper-v2-buy") -> dict[str, object]:
    return {
        "intent_id": intent_id,
        "signal_id": f"signal-{intent_id}",
        "instrument": "BTC-USDT",
        "side": "BUY",
        "position_effect": "OPEN",
        "order_type": "MARKET",
        "quantity": "1.25",
        "price": None,
        "time_in_force": "FOK",
        "reduce_only": False,
        "metadata_version": "instrument-v1",
        "deadline_ns": None,
        "tags": {},
    }


def _submit(service: ExecutionOrderEntryService, intent_id: str) -> dict[str, object]:
    return service.handle(
        SUBMIT_OPERATION,
        principal_ref=_IDENTITY.principal_ref,
        identity=_IDENTITY,
        scope=_WIRE_SCOPE,
        arguments=_arguments(intent_id),
    )


def _only_provider_event(store: SqliteExecutionStore):
    events = tuple(
        event
        for event in store.read_outbox(scope=_EXECUTION_SCOPE, after_sequence=0, limit=100)
        if event.event_type == "provider_observation"
    )
    assert len(events) == 1
    return events[0]


@pytest.mark.unit
def test_managed_paper_v2_uses_one_match_clock_and_reads_exact_fill_after_reopen(
    tmp_path, monkeypatch
):
    database = tmp_path / "paper-v2.sqlite3"
    store, service, quotes = _build_service(database)
    try:
        monkeypatch.setattr(
            store_module, "time", SimpleNamespace(time_ns=_IncrementingClock().time_ns)
        )
        first = _submit(service, "paper-v2-buy")
        record = store.get("paper-v2-buy", scope=_EXECUTION_SCOPE)
        event = _only_provider_event(store)
        receipt = store.get_paper_match_receipt("paper-v2-buy", scope=_EXECUTION_SCOPE)

        assert first["ok"] is True and first["code"] == "OK"
        assert record is not None and record.state is ExecutionState.FILLED
        assert type(receipt) is PaperMatchReceiptV2
        assert receipt.schema == PAPER_MATCH_RECEIPT_SCHEMA_V2
        assert receipt.time_source == PAPER_MATCH_TIME_SOURCE_V2
        assert receipt.match_time_ns == receipt.evaluated_at_ns
        assert receipt.event_created_at_ns == event.created_at_ns
        assert receipt.match_time_ns < receipt.event_created_at_ns
        assert receipt.fill_id == f"paper:{receipt.journal_generation}:{event.event_id}"
        assert receipt.event_id == event.event_id
        assert receipt.event_sequence == event.sequence
        assert receipt.journal_generation == event.journal_incarnation_id
        assert receipt.intent_fingerprint == record.payload_sha256
        assert receipt.strategy_ref == _STRATEGY
        assert receipt.quantity == Decimal("1.25")
        assert receipt.fill_price == Decimal("10.02")
        assert receipt.fee_amount == Decimal("0.0125")
        assert receipt.fee_currency == "USDT"
        assert receipt.commission_model_id == PAPER_FEE_MODEL_REF
        assert receipt.commission_model_version == PAPER_FEE_MODEL_VERSION
        assert receipt.quote is not None
        assert receipt.quote.source_ref == "synthetic-quote-v2-test"
        assert receipt.quote.generation == "quote-generation-v2-test"
        assert receipt.quote.cursor == 1

        fill_fact = receipt.to_fill_fact_payload()
        assert fill_fact == {
            "schema": PAPER_MATCH_RECEIPT_SCHEMA_V2,
            "time_source": PAPER_MATCH_TIME_SOURCE_V2,
            "fill_id": receipt.fill_id,
            "match_time_ns": receipt.match_time_ns,
            "event_created_at_ns": event.created_at_ns,
            "quantity": "1.25",
            "price": "10.02",
            "commission": "0.0125",
            "commission_currency": "USDT",
            "commission_model_id": PAPER_FEE_MODEL_REF,
            "commission_model_version": PAPER_FEE_MODEL_VERSION,
            "event_id": event.event_id,
            "event_sequence": event.sequence,
            "journal_generation": receipt.journal_generation,
            "scope_key": _EXECUTION_SCOPE.key,
            "intent_id": "paper-v2-buy",
            "intent_fingerprint": record.payload_sha256,
            "account_ref": _SIM_ACCOUNT,
            "strategy_ref": _STRATEGY,
            "quote_source_ref": "synthetic-quote-v2-test",
            "quote_generation": "quote-generation-v2-test",
            "quote_cursor": 1,
            "quote_instrument": "BTC-USDT",
            "receipt_sha256": receipt.receipt_sha256,
        }

        changes_before_read = store._connection.total_changes
        for _ in range(2):
            verified = store.get_managed_paper_fill_v2_for_event(
                event.event_id,
                event_sequence=event.sequence,
                journal_generation=receipt.journal_generation,
                scope=_EXECUTION_SCOPE,
            )
            assert verified == receipt
        assert store._connection.total_changes == changes_before_read
        with pytest.raises(DurableStoreError, match="event binding"):
            store.get_managed_paper_fill_v2_for_event(
                event.event_id,
                event_sequence=event.sequence + 1,
                journal_generation=receipt.journal_generation,
                scope=_EXECUTION_SCOPE,
            )
        assert quotes.calls == ["BTC-USDT"]
        assert store.read_paper_book(_EXECUTION_SCOPE).receipt_count == 1
    finally:
        store.close()

    # A new owner can replay the persisted V2 receipt without a second quote
    # read or match, and the exact fill identity/time survive process restart.
    reopened, reopened_service, reopened_quotes = _build_service(database)
    try:
        replay = _submit(reopened_service, "paper-v2-buy")
        replayed_event = _only_provider_event(reopened)
        replayed_receipt = reopened.get_managed_paper_fill_v2_for_event(
            replayed_event.event_id,
            event_sequence=replayed_event.sequence,
            journal_generation=replayed_event.journal_incarnation_id,
            scope=_EXECUTION_SCOPE,
        )
        assert replay == first
        assert type(replayed_receipt) is PaperMatchReceiptV2
        assert replayed_receipt == receipt
        assert reopened_quotes.calls == []
        assert reopened.read_paper_book(_EXECUTION_SCOPE).receipt_count == 1
        assert (
            len(
                [
                    event
                    for event in reopened.read_outbox(
                        scope=_EXECUTION_SCOPE, after_sequence=0, limit=100
                    )
                    if event.event_type == "provider_observation"
                ]
            )
            == 1
        )
    finally:
        reopened.close()


@pytest.mark.unit
def test_managed_paper_v2_match_time_tampering_fails_closed(tmp_path):
    database = tmp_path / "paper-v2-tampered.sqlite3"
    store, _service, _quotes = _build_service(database)
    try:
        # The normal submit path is needed to create a receipt bound to a
        # real durable intent/event before the test corrupts its stored bytes.
        result = _submit(_service, "paper-v2-tamper")
        assert result["ok"] is True
        event = _only_provider_event(store)
        receipt = store.get_paper_match_receipt("paper-v2-tamper", scope=_EXECUTION_SCOPE)
        assert type(receipt) is PaperMatchReceiptV2
    finally:
        store.close()

    # Simulate out-of-band storage corruption while restoring the immutable
    # trigger before the production reader opens the database again.
    connection = sqlite3.connect(database)
    try:
        row = connection.execute(
            "SELECT receipt_json FROM paper_match_receipts WHERE event_id = ?",
            (event.event_id,),
        ).fetchone()
        payload = json.loads(row[0])
        payload["match_time_ns"] += 1
        connection.execute("DROP TRIGGER paper_match_receipts_immutable_update")
        connection.execute(
            "UPDATE paper_match_receipts SET receipt_json = ? WHERE event_id = ?",
            (json.dumps(payload, sort_keys=True, separators=(",", ":")), event.event_id),
        )
        connection.execute(
            """CREATE TRIGGER paper_match_receipts_immutable_update
               BEFORE UPDATE ON paper_match_receipts
               BEGIN SELECT RAISE(ABORT, 'paper match receipts are immutable'); END"""
        )
        connection.commit()
    finally:
        connection.close()

    corrupted, _service, _quotes = _build_service(database)
    try:
        with pytest.raises(DurableStoreError, match="stored paper match receipt is unreadable"):
            corrupted.get_managed_paper_fill_v2_for_event(
                event.event_id,
                event_sequence=event.sequence,
                journal_generation=event.journal_incarnation_id,
                scope=_EXECUTION_SCOPE,
            )
    finally:
        corrupted.close()


@pytest.mark.unit
def test_quote_expiring_before_event_commit_rolls_back_fill_and_latches_unknown(
    tmp_path, monkeypatch
):
    database = tmp_path / "paper-v2-quote-commit-expiry.sqlite3"
    clock = _CommitExpiryClock()
    quotes = _SyntheticQuoteSource(quote_time_ns=clock.quote_time_ns)
    store, service, _quotes = _build_service(
        database,
        quote_source=quotes,
        domain=_domain(available_quote_age_ns=1_000),
    )
    monkeypatch.setattr(store_module, "time", SimpleNamespace(time_ns=clock.time_ns))
    try:
        first = _submit(service, "paper-v2-expired-before-commit")
        record = store.get("paper-v2-expired-before-commit", scope=_EXECUTION_SCOPE)
        assert record is not None and record.state is ExecutionState.UNKNOWN
        assert record.dispatch_attempts == 1
        assert record.unknown_reason == "paper_match_commit_ambiguous"
        assert first["result"]["record"]["state"] == "UNKNOWN"
        assert quotes.calls == ["BTC-USDT"]
        assert clock.after_quote_ns[:3] == [
            clock.base_ns,
            clock.base_ns + 100,
            clock.base_ns + 2_000,
        ]

        receipt_count = store._connection.execute(
            "SELECT COUNT(*) FROM paper_match_receipts WHERE scope_key = ?",
            (_EXECUTION_SCOPE.key,),
        ).fetchone()[0]
        provider_events = [
            event
            for event in store.read_outbox(scope=_EXECUTION_SCOPE, after_sequence=0, limit=100)
            if event.event_type == "provider_observation"
        ]
        unknown_events = [
            event
            for event in store.read_outbox(scope=_EXECUTION_SCOPE, after_sequence=0, limit=100)
            if event.event_type == "dispatch_unknown"
        ]
        assert receipt_count == 0
        assert provider_events == []
        assert len(unknown_events) == 1
        with pytest.raises(DurableStoreError, match="unresolved order"):
            store.read_paper_book(_EXECUTION_SCOPE)

        replay = _submit(service, "paper-v2-expired-before-commit")
        assert replay["result"]["record"]["state"] == "UNKNOWN"
        assert quotes.calls == ["BTC-USDT"]
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM paper_match_receipts WHERE scope_key = ?",
                (_EXECUTION_SCOPE.key,),
            ).fetchone()[0]
            == 0
        )
        assert (
            len(
                [
                    event
                    for event in store.read_outbox(
                        scope=_EXECUTION_SCOPE, after_sequence=0, limit=100
                    )
                    if event.event_type == "provider_observation"
                ]
            )
            == 0
        )
    finally:
        store.close()


@pytest.mark.unit
def test_legacy_v1_rejection_stays_readable_without_a_synthetic_fill_time(tmp_path):
    database = tmp_path / "paper-v1-rejection.sqlite3"
    store, service, quotes = _build_service(
        database, quote_source=_SyntheticQuoteSource(available=False)
    )
    try:
        result = _submit(service, "paper-v1-no-quote")
        event = _only_provider_event(store)
        receipt = store.get_paper_match_receipt("paper-v1-no-quote", scope=_EXECUTION_SCOPE)

        assert result["ok"] is True and result["code"] == "OK"
        assert type(receipt) is PaperMatchReceiptV1
        assert receipt.state is ExecutionState.REJECTED
        assert receipt.reason_code == "paper_quote_unavailable"
        assert receipt.evaluated_at_ns == event.created_at_ns
        assert (
            store.get_managed_paper_fill_v2_for_event(
                event.event_id,
                event_sequence=event.sequence,
                journal_generation=receipt.journal_generation,
                scope=_EXECUTION_SCOPE,
            )
            is None
        )
        assert store.read_paper_book(_EXECUTION_SCOPE).receipt_count == 1
        assert quotes.calls == ["BTC-USDT"]
    finally:
        store.close()


@pytest.mark.unit
def test_persisted_legacy_v1_fill_is_readable_but_never_promoted_to_v2(tmp_path):
    database = tmp_path / "paper-v1-fill.sqlite3"
    store, service, quotes = _build_service(database)
    try:
        result = _submit(service, "paper-v1-fill")
        event = _only_provider_event(store)
        v2_receipt = store.get_paper_match_receipt("paper-v1-fill", scope=_EXECUTION_SCOPE)
        assert result["ok"] is True
        assert type(v2_receipt) is PaperMatchReceiptV2
        assert v2_receipt.event_created_at_ns == event.created_at_ns
        # Model a byte-stable legacy V1 receipt as it would have been stored
        # by the older producer: its timestamp is the event commit time and
        # the V1 schema contains no match-time or fill-ID claim.
        v1_receipt = PaperMatchReceiptV1.create(
            account_key=v2_receipt.account_key,
            scope_key=v2_receipt.scope_key,
            journal_generation=v2_receipt.journal_generation,
            execution_domain_ref=v2_receipt.execution_domain_ref,
            simulation_account_ref=v2_receipt.simulation_account_ref,
            experiment_ref=v2_receipt.experiment_ref,
            model_ref=v2_receipt.model_ref,
            model_version=v2_receipt.model_version,
            intent_id=v2_receipt.intent_id,
            intent_fingerprint=v2_receipt.intent_fingerprint,
            event_id=v2_receipt.event_id,
            event_sequence=v2_receipt.event_sequence,
            state=v2_receipt.state,
            reason_code=v2_receipt.reason_code,
            evaluated_at_ns=event.created_at_ns,
            instrument=v2_receipt.instrument,
            side=v2_receipt.side,
            quantity=v2_receipt.quantity,
            fill_price=v2_receipt.fill_price,
            fee_amount=v2_receipt.fee_amount,
            fee_currency=v2_receipt.fee_currency,
            quote=v2_receipt.quote,
            cash_delta=v2_receipt.cash_delta,
            position_delta=v2_receipt.position_delta,
            before_book_sha256=v2_receipt.before_book_sha256,
            after_book_sha256=v2_receipt.after_book_sha256,
        )
    finally:
        store.close()

    connection = sqlite3.connect(database)
    try:
        connection.execute("DROP TRIGGER paper_match_receipts_immutable_update")
        connection.execute(
            "UPDATE paper_match_receipts SET receipt_json = ?, receipt_sha256 = ? "
            "WHERE event_id = ?",
            (
                store_module.canonical_json(v1_receipt.to_payload()),
                v1_receipt.receipt_sha256,
                event.event_id,
            ),
        )
        connection.execute(
            """CREATE TRIGGER paper_match_receipts_immutable_update
               BEFORE UPDATE ON paper_match_receipts
               BEGIN SELECT RAISE(ABORT, 'paper match receipts are immutable'); END"""
        )
        connection.commit()
    finally:
        connection.close()

    reopened, _reopened_service, reopened_quotes = _build_service(database)
    try:
        restored = reopened.get_paper_match_receipt("paper-v1-fill", scope=_EXECUTION_SCOPE)
        assert type(restored) is PaperMatchReceiptV1
        assert restored.evaluated_at_ns == event.created_at_ns
        assert (
            reopened.get_managed_paper_fill_v2_for_event(
                event.event_id,
                event_sequence=event.sequence,
                journal_generation=event.journal_incarnation_id,
                scope=_EXECUTION_SCOPE,
            )
            is None
        )
        snapshot = reopened.read_paper_book(_EXECUTION_SCOPE)
        assert snapshot.receipt_count == 1
        assert snapshot.positions[0].quantity == Decimal("1.25")
        assert reopened_quotes.calls == []
        assert quotes.calls == ["BTC-USDT"]
    finally:
        reopened.close()
