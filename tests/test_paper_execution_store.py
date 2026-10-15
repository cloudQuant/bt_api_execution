"""Real SQLite/facade acceptance for the first managed paper execution slice."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from time import time_ns

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

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
    ProviderObservation,
    Side,
    SqliteExecutionStore,
    TimeInForce,
    WriterLeaseUnavailable,
)
from bt_api_execution.errors import ContractValidationError, DurableStoreError
from bt_api_execution.service_order_entry import SUBMIT_OPERATION, ExecutionOrderEntryService

_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
_EXECUTION_SRC = _PACKAGE_ROOT / "src"
_BASE_SRC = _PACKAGE_ROOT.parent / "bt_api_base" / "src"
_DOMAIN_REF = "paper-domain-integration-v1"
_SIM_ACCOUNT = "sim-account-integration"
_STRATEGY = "strategy-integration"
_WIRE_SCOPE = WireScope(
    _DOMAIN_REF,
    account_ref=_SIM_ACCOUNT,
    strategy_ref=_STRATEGY,
)
_EXECUTION_SCOPE = ExecutionScope(
    PAPER_PROVIDER, PAPER_ENVIRONMENT, _SIM_ACCOUNT, _STRATEGY, "20261016"
)
_IDENTITY = WireIdentity(WireRole.STRATEGY_CLIENT, "paper-principal", _STRATEGY)


def _domain(
    *,
    execution_domain_ref: str = _DOMAIN_REF,
    simulation_account_ref: str = _SIM_ACCOUNT,
    strategy_ref: str = _STRATEGY,
    experiment_ref: str = "paper-experiment-integration",
    quote_source_ref: str = "synthetic-quote-v1",
) -> PaperOwnerDomainV1:
    return PaperOwnerDomainV1(
        execution_domain_ref=execution_domain_ref,
        simulation_account_ref=simulation_account_ref,
        experiment_ref=experiment_ref,
        strategy_ref=strategy_ref,
        model_ref=PAPER_MODEL_REF,
        model_version="1",
        quote_source_ref=quote_source_ref,
        cash_currency="USDT",
        initial_cash=Decimal("100"),
        initial_fees=Decimal("0"),
        fee_rate=Decimal("0.001"),
        fee_quantum=Decimal("0.0001"),
        max_quote_age_ns=30_000_000_000,
        instruments=(
            PaperInstrumentV1(
                "BTC-USDT", "instrument-v1", "BTC", "USDT", Decimal("0.25"), Decimal("0.01")
            ),
        ),
        opening_positions=(),
    )


class _SyntheticQuoteSource:
    def __init__(
        self, *, available: bool = True, source_ref: str = "synthetic-quote-v1"
    ) -> None:
        self.available = available
        self.source_ref = source_ref
        self.calls: list[str] = []

    def read_quote(self, instrument: str) -> PaperQuoteV1 | None:
        self.calls.append(instrument)
        if not self.available:
            return None
        return PaperQuoteV1(
            source_ref=self.source_ref,
            generation="quote-generation-integration",
            cursor=len(self.calls),
            instrument=instrument,
            metadata_version="instrument-v1",
            quote_currency="USDT",
            bid=Decimal("9.98"),
            ask=Decimal("10.02"),
            bid_quantity=Decimal("5"),
            ask_quantity=Decimal("5"),
            as_of_ns=time_ns(),
        )


def _build_service(
    path: Path,
    *,
    domain: PaperOwnerDomainV1 | None = None,
    wire_scope: WireScope | None = None,
    execution_scope: ExecutionScope | None = None,
    quote_source: _SyntheticQuoteSource | None = None,
    writer_id: str = "paper-integration-owner",
) -> tuple[SqliteExecutionStore, ExecutionOrderEntryService, _SyntheticQuoteSource]:
    domain = _domain() if domain is None else domain
    wire_scope = _WIRE_SCOPE if wire_scope is None else wire_scope
    execution_scope = _EXECUTION_SCOPE if execution_scope is None else execution_scope
    quote_source = _SyntheticQuoteSource() if quote_source is None else quote_source
    store = SqliteExecutionStore(path)
    try:
        port = PaperMatchingPort(domain, quote_source)
        validator = ExecutionProviderValidatorV1(
            provider_id=PAPER_PROVIDER,
            validator_id="paper-spot-market-v1",
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
        service = ExecutionOrderEntryService(
            store,
            {wire_scope: execution_scope},
            port,
            basic_admission_gate=gate,
            paper_owner_domain=domain,
            writer_id=writer_id,
            lease_ttl_ns=60_000_000_000,
        )
        service.recover_interrupted_dispatches()
        return store, service, quote_source
    except Exception:
        store.close()
        raise


def _arguments(
    intent_id: str = "paper-buy-1",
    *,
    side: str = "BUY",
    quantity: str = "1.25",
) -> dict[str, object]:
    return {
        "intent_id": intent_id,
        "signal_id": f"signal-{intent_id}",
        "instrument": "BTC-USDT",
        "side": side,
        "position_effect": "OPEN" if side == "BUY" else "CLOSE",
        "order_type": "MARKET",
        "quantity": quantity,
        "price": None,
        "time_in_force": "FOK",
        "reduce_only": side == "SELL",
        "metadata_version": "instrument-v1",
        "deadline_ns": None,
        "tags": {},
    }


def _submit(
    service: ExecutionOrderEntryService,
    arguments: dict[str, object],
    *,
    scope: WireScope = _WIRE_SCOPE,
) -> dict[str, object]:
    return service.handle(
        SUBMIT_OPERATION,
        principal_ref=_IDENTITY.principal_ref,
        identity=_IDENTITY,
        scope=scope,
        arguments=arguments,
    )


@pytest.mark.unit
def test_managed_spot_match_posts_one_atomic_receipt_and_survives_reopen(tmp_path) -> None:
    database = tmp_path / "paper.sqlite3"
    store, service, quotes = _build_service(database)
    try:
        first = _submit(service, _arguments())
        record = store.get("paper-buy-1", scope=_EXECUTION_SCOPE)
        receipt = store.get_paper_match_receipt("paper-buy-1", scope=_EXECUTION_SCOPE)
        book = store.read_paper_book(_EXECUTION_SCOPE)
        provider_events = tuple(
            event
            for event in store.read_outbox(scope=_EXECUTION_SCOPE, after_sequence=0, limit=100)
            if event.event_type == "provider_observation"
        )

        assert first["ok"] is True and first["code"] == "OK"
        assert record is not None and record.state is ExecutionState.FILLED
        assert record.dispatch_attempts == 1
        assert receipt is not None and receipt.state is ExecutionState.FILLED
        assert receipt.event_sequence == provider_events[0].sequence
        assert receipt.event_id == provider_events[0].event_id
        assert receipt.execution_domain_ref == _DOMAIN_REF
        assert receipt.simulation_account_ref == _SIM_ACCOUNT
        assert receipt.quote is not None and receipt.quote.cursor == 1
        assert receipt.fill_price == Decimal("10.02")
        assert receipt.fee_amount == Decimal("0.0125")
        assert book.cash == Decimal("87.4625")
        assert book.cumulative_fees == Decimal("0.0125")
        assert book.positions[0].quantity == Decimal("1.25")
        assert book.positions[0].average_cost == Decimal("10.02")
        assert book.receipt_count == 1
        assert book.execution_journal_generation == receipt.journal_generation
        assert book.execution_cursor == store.execution_outbox_bounds()[3]
        assert quotes.calls == ["BTC-USDT"]

        replay = _submit(service, _arguments())
        assert replay == first
        assert quotes.calls == ["BTC-USDT"]
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
            == 1
        )
    finally:
        store.close()

    reopened, restarted, replay_quotes = _build_service(database)
    try:
        replay = _submit(restarted, _arguments())
        book = reopened.read_paper_book(_EXECUTION_SCOPE)
        assert replay["ok"] is True and replay["result"]["record"]["state"] == "FILLED"
        assert replay_quotes.calls == []
        assert book.cash == Decimal("87.4625")
        assert book.positions[0].quantity == Decimal("1.25")
        assert book.receipt_count == 1
    finally:
        reopened.close()


@pytest.mark.unit
def test_paper_generic_reconcile_cannot_create_unreceipted_provider_events(tmp_path) -> None:
    store, service, _quotes = _build_service(tmp_path / "paper-generic-reconcile.sqlite3")
    try:
        submitted = _submit(service, _arguments("paper-generic-reconcile"))
        assert submitted["result"]["record"]["state"] == "FILLED"
        before = store.read_outbox(scope=_EXECUTION_SCOPE, after_sequence=0, limit=100)
        observation = ProviderObservation(
            intent_id="paper-generic-reconcile",
            state=ExecutionState.FILLED,
            provider_order_id=submitted["result"]["record"]["provider_order_id"],
            filled_quantity=Decimal("1.25"),
            average_price=Decimal("10.02"),
            cumulative_commission=Decimal("0.0125"),
            cumulative_commission_currency="USDT",
        )
        facade = service._facade_pair(_EXECUTION_SCOPE)[0]

        class ReconcilePort:
            calls = 0

            def __call__(self, _intent):
                self.calls += 1
                return observation

        port = ReconcilePort()
        with pytest.raises(ContractValidationError, match="atomic match receipt"):
            facade.reconcile_with_port("paper-generic-reconcile", port)
        with pytest.raises(ContractValidationError, match="atomic match receipt"):
            facade.record_provider_observation(observation)
        with pytest.raises(ContractValidationError, match="atomic match receipt"):
            store.record_observation_with_event(
                observation,
                scope=_EXECUTION_SCOPE,
                writer_lease=facade.acquire_writer_lease(),
            )

        after = store.read_outbox(scope=_EXECUTION_SCOPE, after_sequence=0, limit=100)
        book = store.read_paper_book(_EXECUTION_SCOPE)
        assert port.calls == 0
        assert after == before
        assert book.receipt_count == 1
        assert book.cash == Decimal("87.4625")
    finally:
        store.close()


@pytest.mark.unit
def test_orphan_paper_provider_event_blocks_later_settlement(tmp_path) -> None:
    store, service, quotes = _build_service(tmp_path / "paper-orphan-event.sqlite3")
    try:
        first = _submit(service, _arguments("paper-before-orphan"))
        assert first["result"]["record"]["state"] == "FILLED"
        with store._transaction() as cursor:
            store._append_event(
                cursor,
                # Keep the foreign event attached to an existing intent so the
                # SQLite foreign key is valid; its new event ID has no receipt.
                intent_id="paper-before-orphan",
                scope_key=_EXECUTION_SCOPE.key,
                event_type="provider_observation",
                state=ExecutionState.FILLED,
                payload={
                    "provider_order_id": "synthetic-orphan",
                    "filled_quantity": "1",
                    "average_price": "10",
                    "cumulative_commission": None,
                    "reason_code": None,
                    "source": "provider",
                },
                now_ns=time_ns(),
            )

        provider_events_before = tuple(
            event
            for event in store.read_outbox(scope=_EXECUTION_SCOPE, after_sequence=0, limit=100)
            if event.event_type == "provider_observation"
        )
        with pytest.raises(DurableStoreError, match="no immutable match receipt"):
            store.read_paper_book(_EXECUTION_SCOPE)

        second = _submit(service, _arguments("paper-after-orphan"))
        second_record = store.get("paper-after-orphan", scope=_EXECUTION_SCOPE)
        receipts = store._connection.execute(
            "SELECT COUNT(*) FROM paper_match_receipts WHERE scope_key = ?",
            (_EXECUTION_SCOPE.key,),
        ).fetchone()[0]
        provider_events_after = tuple(
            event
            for event in store.read_outbox(scope=_EXECUTION_SCOPE, after_sequence=0, limit=100)
            if event.event_type == "provider_observation"
        )

        assert second["result"]["record"]["state"] == "UNKNOWN"
        assert second_record is not None and second_record.state is ExecutionState.UNKNOWN
        assert receipts == 1
        assert provider_events_after == provider_events_before
        assert quotes.calls == ["BTC-USDT", "BTC-USDT"]
        with pytest.raises(DurableStoreError, match="no immutable match receipt"):
            store.read_paper_book(_EXECUTION_SCOPE)
    finally:
        store.close()


@pytest.mark.unit
def test_prior_unknown_paper_intent_blocks_new_settlement(tmp_path) -> None:
    quotes = _SyntheticQuoteSource()
    store, service, _ = _build_service(
        tmp_path / "paper-prior-unknown.sqlite3", quote_source=quotes
    )
    try:
        # A trusted dispatch exception is an ambiguous outcome. A quote-source
        # exception is instead converted by PaperMatchingPort into a known
        # refusal and must not be used to synthesize UNKNOWN here.
        dispatch = service._dispatch
        original_submit_authorized = dispatch.submit_authorized
        first_dispatch = True

        def fail_first_dispatch(intent, authorization):
            nonlocal first_dispatch
            if first_dispatch:
                first_dispatch = False
                raise RuntimeError("synthetic ambiguous dispatch interruption")
            return original_submit_authorized(intent, authorization)

        dispatch.submit_authorized = fail_first_dispatch
        prior = _submit(service, _arguments("paper-prior-unknown"))
        prior_record = store.get("paper-prior-unknown", scope=_EXECUTION_SCOPE)
        assert prior["result"]["record"]["state"] == "UNKNOWN"
        assert prior_record is not None and prior_record.state is ExecutionState.UNKNOWN

        later = _submit(service, _arguments("paper-after-unknown"))
        later_record = store.get("paper-after-unknown", scope=_EXECUTION_SCOPE)
        receipts = store._connection.execute(
            "SELECT COUNT(*) FROM paper_match_receipts WHERE scope_key = ?",
            (_EXECUTION_SCOPE.key,),
        ).fetchone()[0]
        events = store.read_outbox(scope=_EXECUTION_SCOPE, after_sequence=0, limit=100)

        assert later["result"]["record"]["state"] == "UNKNOWN"
        assert later_record is not None and later_record.state is ExecutionState.UNKNOWN
        assert receipts == 0
        assert not [event for event in events if event.event_type == "provider_observation"]
        assert len([event for event in events if event.event_type == "dispatch_unknown"]) == 2
        assert quotes.calls == ["BTC-USDT"]
        with pytest.raises(DurableStoreError, match="unresolved order"):
            store.read_paper_book(_EXECUTION_SCOPE)
    finally:
        store.close()


@pytest.mark.unit
def test_managed_partial_sell_posts_quote_currency_fee_and_keeps_cost_basis(tmp_path) -> None:
    database = tmp_path / "paper-sell.sqlite3"
    store, service, quotes = _build_service(database)
    try:
        buy = _submit(service, _arguments("paper-buy", quantity="1.25"))
        sell = _submit(service, _arguments("paper-sell", side="SELL", quantity="0.50"))
        book = store.read_paper_book(_EXECUTION_SCOPE)

        assert buy["result"]["record"]["state"] == "FILLED"
        assert sell["result"]["record"]["state"] == "FILLED"
        assert book.cash == Decimal("92.4475")
        assert book.cumulative_fees == Decimal("0.0175")
        assert book.positions[0].quantity == Decimal("0.75")
        assert book.positions[0].average_cost == Decimal("10.02")
        assert quotes.calls == ["BTC-USDT", "BTC-USDT"]
    finally:
        store.close()


@pytest.mark.unit
def test_missing_quote_is_durably_rejected_without_cash_or_inventory_change(tmp_path) -> None:
    store, service, quotes = _build_service(
        tmp_path / "paper-no-quote.sqlite3", quote_source=_SyntheticQuoteSource(available=False)
    )
    try:
        reply = _submit(service, _arguments("paper-no-quote"))
        receipt = store.get_paper_match_receipt("paper-no-quote", scope=_EXECUTION_SCOPE)
        book = store.read_paper_book(_EXECUTION_SCOPE)

        assert reply["ok"] is True
        assert reply["result"]["record"]["state"] == "REJECTED"
        assert receipt is not None and receipt.reason_code == "paper_quote_unavailable"
        assert receipt.quote is None
        assert book.cash == Decimal("100")
        assert book.positions == ()
        assert quotes.calls == ["BTC-USDT"]
    finally:
        store.close()


@pytest.mark.unit
def test_a_quote_cursor_cannot_be_matched_twice_for_distinct_orders(tmp_path) -> None:
    class RepeatedQuoteSource:
        def __init__(self) -> None:
            self.calls = 0
            self.as_of_ns = time_ns()

        def read_quote(self, instrument: str) -> PaperQuoteV1:
            self.calls += 1
            return PaperQuoteV1(
                source_ref="synthetic-quote-v1",
                generation="quote-generation-repeated",
                cursor=1,
                instrument=instrument,
                metadata_version="instrument-v1",
                quote_currency="USDT",
                bid=Decimal("9.98"),
                ask=Decimal("10.02"),
                bid_quantity=Decimal("5"),
                ask_quantity=Decimal("5"),
                as_of_ns=self.as_of_ns,
            )

    quotes = RepeatedQuoteSource()
    store, service, _ = _build_service(tmp_path / "paper-reused-quote.sqlite3", quote_source=quotes)
    try:
        first = _submit(service, _arguments("paper-quote-first"))
        second = _submit(service, _arguments("paper-quote-second"))
        book = store.read_paper_book(_EXECUTION_SCOPE)
        second_receipt = store.get_paper_match_receipt("paper-quote-second", scope=_EXECUTION_SCOPE)

        assert first["result"]["record"]["state"] == "FILLED"
        assert second["result"]["record"]["state"] == "REJECTED"
        assert second_receipt is not None
        assert second_receipt.reason_code == "paper_quote_cursor_reused"
        assert quotes.calls == 2
        assert book.cash == Decimal("87.4625")
        assert book.positions[0].quantity == Decimal("1.25")
        assert book.cumulative_fees == Decimal("0.0125")
        assert book.receipt_count == 2
    finally:
        store.close()


@pytest.mark.unit
def test_domain_and_live_scope_mismatch_fail_closed_before_quote_or_registration(tmp_path) -> None:
    quotes = _SyntheticQuoteSource()
    store, service, _ = _build_service(tmp_path / "paper-scope.sqlite3", quote_source=quotes)
    try:
        mismatched_domain = WireScope(
            "other-paper-domain", account_ref=_SIM_ACCOUNT, strategy_ref=_STRATEGY
        )
        denied = _submit(service, _arguments("paper-domain-denied"), scope=mismatched_domain)
        assert denied["ok"] is False and denied["code"] == "SCOPE_DENIED"
        assert quotes.calls == []

        with pytest.raises(ContractValidationError):
            _build_service(
                tmp_path / "paper-live-mismatch.sqlite3",
                domain=_domain(execution_domain_ref="live-domain"),
                wire_scope=WireScope(
                    "live-domain", account_ref=_SIM_ACCOUNT, strategy_ref=_STRATEGY
                ),
                execution_scope=ExecutionScope(
                    "binance", "live", _SIM_ACCOUNT, _STRATEGY, "20261016"
                ),
            )
    finally:
        store.close()


@pytest.mark.unit
def test_second_writer_and_second_domain_cannot_take_paper_owner(tmp_path) -> None:
    database = tmp_path / "paper-owner-fence.sqlite3"
    first_store, first_service, first_quotes = _build_service(database)
    second_store = SqliteExecutionStore(database)
    second_quotes = _SyntheticQuoteSource()
    second_owner_domain = _domain()
    second_port = PaperMatchingPort(second_owner_domain, second_quotes)
    second_validator = ExecutionProviderValidatorV1(
        provider_id=PAPER_PROVIDER,
        validator_id="paper-spot-market-v1",
        version="1",
        dispatch_port=second_port,
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
    second_service = ExecutionOrderEntryService(
        second_store,
        {_WIRE_SCOPE: _EXECUTION_SCOPE},
        second_port,
        basic_admission_gate=ExecutionBasicAdmissionGate(
            second_store,
            ExecutionAdmissionValidatorRegistry((second_validator,)),
            permit_ttl_ns=60_000_000_000,
        ),
        paper_owner_domain=second_owner_domain,
        writer_id="different-paper-writer",
        lease_ttl_ns=60_000_000_000,
    )
    try:
        with pytest.raises(WriterLeaseUnavailable):
            second_service.recover_interrupted_dispatches()
        assert first_quotes.calls == []
        assert second_quotes.calls == []

        second_owner_domain = _domain(
            execution_domain_ref="paper-domain-other",
            strategy_ref="another-strategy",
        )
        second_owner_scope = ExecutionScope(
            PAPER_PROVIDER, PAPER_ENVIRONMENT, _SIM_ACCOUNT, "another-strategy", "20261016"
        )
        lease = first_service._facade_pair(_EXECUTION_SCOPE)[0].acquire_writer_lease()
        with pytest.raises(DurableStoreError, match="different owner"):
            first_store.register_paper_owner_domain(
                second_owner_scope, second_owner_domain, writer_lease=lease
            )
        assert first_quotes.calls == []
        assert second_quotes.calls == []
    finally:
        second_store.close()
        first_store.close()


_CRASH_CHILD = r"""
from __future__ import annotations

import os
import sqlite3
import sys
from decimal import Decimal
from pathlib import Path
from time import time_ns

sys.path[:0] = [sys.argv[4], sys.argv[5]]

from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope
from bt_api_execution import (
    ExecutionAdmissionValidatorRegistry, ExecutionBasicAdmissionGate,
    ExecutionProviderValidatorV1, ExecutionScope, OrderType, PAPER_ENVIRONMENT,
    PAPER_MODEL_REF, PAPER_PROVIDER, PaperInstrumentV1, PaperMatchingPort,
    PaperOwnerDomainV1, PaperQuoteV1, PositionEffect, Side, SqliteExecutionStore,
    TimeInForce,
)
from bt_api_execution.facade import ManagedExecutionFacade
from bt_api_execution.service_order_entry import ExecutionOrderEntryService, SUBMIT_OPERATION

mode, database_path, quote_journal_path = sys.argv[1:4]
domain = PaperOwnerDomainV1(
    execution_domain_ref="paper-domain-crash", simulation_account_ref="sim-account-crash",
    experiment_ref="experiment-crash", strategy_ref="strategy-crash",
    model_ref=PAPER_MODEL_REF, model_version="1", quote_source_ref="crash-quote-source",
    cash_currency="USDT", initial_cash=Decimal("100"), initial_fees=Decimal("0"),
    fee_rate=Decimal("0.001"), fee_quantum=Decimal("0.0001"), max_quote_age_ns=30_000_000_000,
    instruments=(
        PaperInstrumentV1(
            "BTC-USDT", "instrument-v1", "BTC", "USDT", Decimal("0.25"), Decimal("0.01")
        ),
    ),
    opening_positions=(),
)
wire_scope = WireScope("paper-domain-crash", account_ref="sim-account-crash", strategy_ref="strategy-crash")
execution_scope = ExecutionScope(PAPER_PROVIDER, PAPER_ENVIRONMENT, "sim-account-crash", "strategy-crash", "20261016")
identity = WireIdentity(WireRole.STRATEGY_CLIENT, "paper-crash-principal", "strategy-crash")

class QuoteSource:
    def read_quote(self, instrument):
        connection = sqlite3.connect(quote_journal_path)
        try:
            connection.execute("INSERT INTO quote_calls(instrument) VALUES (?)", (instrument,))
            connection.commit()
        finally:
            connection.close()
        if mode == "crash_after_claim_before_match_transaction":
            os._exit(71)
        return PaperQuoteV1(
            source_ref="crash-quote-source", generation="generation-crash", cursor=1,
            instrument=instrument, metadata_version="instrument-v1", bid=Decimal("9.98"),
            quote_currency="USDT",
            ask=Decimal("10.02"), bid_quantity=Decimal("5"), ask_quantity=Decimal("5"),
            as_of_ns=time_ns(),
        )

port = PaperMatchingPort(domain, QuoteSource())
validator = ExecutionProviderValidatorV1(
    provider_id=PAPER_PROVIDER, validator_id="paper-crash-validator", version="1",
    dispatch_port=port, instrument_map={"BTC-USDT": "BTC-USDT"},
    quantity_steps={"BTC-USDT": Decimal("0.25")}, max_quantities={"BTC-USDT": Decimal("10")},
    price_ticks={"BTC-USDT": Decimal("0.01")}, supported_order_types=(OrderType.MARKET,),
    supported_time_in_force=(TimeInForce.FOK,),
    supported_actions=((Side.BUY, PositionEffect.OPEN), (Side.SELL, PositionEffect.CLOSE)),
    supports_reduce_only=True,
)
store = SqliteExecutionStore(database_path)
service = ExecutionOrderEntryService(
    store, {wire_scope: execution_scope}, port,
    basic_admission_gate=ExecutionBasicAdmissionGate(
        store, ExecutionAdmissionValidatorRegistry((validator,)), permit_ttl_ns=60_000_000_000
    ),
    paper_owner_domain=domain, writer_id="paper-crash-writer", lease_ttl_ns=60_000_000_000,
)
service.recover_interrupted_dispatches()
arguments = {
    "intent_id": "paper-crash-order", "signal_id": "signal-paper-crash-order",
    "instrument": "BTC-USDT", "side": "BUY", "position_effect": "OPEN",
    "order_type": "MARKET", "quantity": "1.25", "price": None,
    "time_in_force": "FOK", "reduce_only": False, "metadata_version": "instrument-v1",
    "deadline_ns": None, "tags": {},
}

if mode == "crash_after_outbox_before_receipt":
    original = SqliteExecutionStore._append_event
    def crash_after_event(self, cursor, **values):
        event = original(self, cursor, **values)
        if values.get("event_type") == "provider_observation":
            os._exit(72)
        return event
    SqliteExecutionStore._append_event = crash_after_event
elif mode == "crash_after_commit_before_reply":
    def crash_before_reply(self, record, **kwargs):
        os._exit(73)
    ManagedExecutionFacade._settle_after_evidenced_outcome = crash_before_reply

service.handle(
    SUBMIT_OPERATION, principal_ref=identity.principal_ref, identity=identity,
    scope=wire_scope, arguments=arguments,
)
store.close()
"""


def _prepare_quote_journal(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE quote_calls(call_id INTEGER PRIMARY KEY, instrument TEXT NOT NULL)"
        )
        connection.commit()
    finally:
        connection.close()


def _quote_call_count(path: Path) -> int:
    connection = sqlite3.connect(path)
    try:
        return int(connection.execute("SELECT COUNT(*) FROM quote_calls").fetchone()[0])
    finally:
        connection.close()


def _run_crash_child(
    mode: str, database: Path, quote_journal: Path
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - test-owned child and temporary database only
        [
            sys.executable,
            "-c",
            _CRASH_CHILD,
            mode,
            str(database),
            str(quote_journal),
            str(_EXECUTION_SRC),
            str(_BASE_SRC),
        ],
        cwd=_PACKAGE_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("phase", "expected_exit", "committed"),
    [
        ("crash_after_claim_before_match_transaction", 71, False),
        ("crash_after_outbox_before_receipt", 72, False),
        ("crash_after_commit_before_reply", 73, True),
    ],
)
def test_process_crash_boundary_never_rematches_and_replays_only_committed_receipt(
    tmp_path, phase: str, expected_exit: int, committed: bool
) -> None:
    database = tmp_path / f"{phase}.sqlite3"
    quote_journal = tmp_path / f"{phase}-quotes.sqlite3"
    _prepare_quote_journal(quote_journal)
    child = _run_crash_child(phase, database, quote_journal)
    assert child.returncode == expected_exit, child.stderr
    assert _quote_call_count(quote_journal) == 1

    store, service, restarted_quotes = _build_service(
        database,
        domain=_domain(
            execution_domain_ref="paper-domain-crash",
            simulation_account_ref="sim-account-crash",
            strategy_ref="strategy-crash",
            experiment_ref="experiment-crash",
            quote_source_ref="crash-quote-source",
        ),
        wire_scope=WireScope(
            "paper-domain-crash", account_ref="sim-account-crash", strategy_ref="strategy-crash"
        ),
        execution_scope=ExecutionScope(
            PAPER_PROVIDER, PAPER_ENVIRONMENT, "sim-account-crash", "strategy-crash", "20261016"
        ),
        quote_source=_SyntheticQuoteSource(source_ref="crash-quote-source"),
        writer_id="paper-crash-writer",
    )
    try:
        crash_scope = ExecutionScope(
            PAPER_PROVIDER, PAPER_ENVIRONMENT, "sim-account-crash", "strategy-crash", "20261016"
        )
        record = store.get("paper-crash-order", scope=crash_scope)
        assert record is not None
        if committed:
            assert record.state is ExecutionState.FILLED
            assert store.get_paper_match_receipt("paper-crash-order", scope=crash_scope) is not None
        else:
            assert record.state is ExecutionState.UNKNOWN
            assert record.unknown_reason is not None
            receipt_count = store._connection.execute(
                "SELECT COUNT(*) FROM paper_match_receipts WHERE scope_key = ?",
                (crash_scope.key,),
            ).fetchone()[0]
            assert receipt_count == 0

        replay = service.handle(
            SUBMIT_OPERATION,
            principal_ref="paper-crash-principal",
            identity=WireIdentity(
                WireRole.STRATEGY_CLIENT, "paper-crash-principal", "strategy-crash"
            ),
            scope=WireScope(
                "paper-domain-crash", account_ref="sim-account-crash", strategy_ref="strategy-crash"
            ),
            arguments=_arguments("paper-crash-order"),
        )
        assert replay["result"]["record"]["state"] == ("FILLED" if committed else "UNKNOWN")
        assert restarted_quotes.calls == []
        assert _quote_call_count(quote_journal) == 1
        events = store.read_outbox(
            scope=ExecutionScope(
                PAPER_PROVIDER, PAPER_ENVIRONMENT, "sim-account-crash", "strategy-crash", "20261016"
            ),
            after_sequence=0,
            limit=100,
        )
        paper_observations = [
            event for event in events if event.event_type == "provider_observation"
        ]
        unknown_events = [event for event in events if event.event_type == "dispatch_unknown"]
        assert len(paper_observations) == (1 if committed else 0)
        assert len(unknown_events) == (0 if committed else 1)
        if committed:
            book = store.read_paper_book(
                ExecutionScope(
                    PAPER_PROVIDER,
                    PAPER_ENVIRONMENT,
                    "sim-account-crash",
                    "strategy-crash",
                    "20261016",
                )
            )
            assert book.cash == Decimal("87.4625")
            assert book.receipt_count == 1
        else:
            with pytest.raises(DurableStoreError, match="unresolved order"):
                store.read_paper_book(
                    ExecutionScope(
                        PAPER_PROVIDER,
                        PAPER_ENVIRONMENT,
                        "sim-account-crash",
                        "strategy-crash",
                        "20261016",
                    )
                )
    finally:
        store.close()
