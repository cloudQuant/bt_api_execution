"""Typed, versioned journal port injection and the journal's delegation points."""

from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from bt_api_execution.journal import (
    ExecutionDecisionJournal,
    ExecutionDecisionJournalError,
    _fee_pair_facts_source_before_commit,
    _journal_ports,
    configure_backtrader_contracts,
    configure_journal_ports,
    reset_journal_ports,
)
from bt_api_execution.journal_ports import (
    JOURNAL_PORT_SCHEMA_VERSION,
    AttestedCtpOrderIdentityQuery,
    JournalFeePairFactsPort,
    JournalPorts,
    JournalPositionAttributionPort,
    JournalRegisteredSimAttestationPort,
)

_ACCOUNT = "account-a"
_INSTRUMENT = "a2601"
_ROWS = ({"OrderRef": "1", "OrderSysID": "SYS-1"},)

#: Application-owned attribution tables the journal's capacity backfill counts.
_BOOK_TABLES = (
    "factor_position_grid_history",
    "factor_position_openings",
    "factor_position_fills",
    "factor_position_evidence_issues",
    "factor_position_dated_trade_facts",
    "factor_position_current",
    "factor_position_active_issues",
    "factor_position_fill_attributes",
    "factor_position_open_intent_totals",
)


def _rows_sha256(rows: tuple) -> str:
    encoded = json.dumps(
        [dict(row) for row in rows], ensure_ascii=True, separators=(",", ":"), sort_keys=True
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class _PositionPort(JournalPositionAttributionPort):
    """Minimal attribution port that really creates the book meta table."""

    port_version = JOURNAL_PORT_SCHEMA_VERSION

    def __init__(self, *, label: str = "global", stored_version: int = 6) -> None:
        self.label = label
        self.stored_version = stored_version
        self.expected_version_calls = 0
        self.ensure_schema_calls = 0
        self.recorded_grids: list[tuple] = []

    @property
    def expected_schema_version(self) -> int:
        self.expected_version_calls += 1
        return 6

    def ensure_schema(self, cursor: Any) -> None:
        self.ensure_schema_calls += 1
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS factor_position_book_meta (
                   singleton INTEGER PRIMARY KEY CHECK (singleton=1),
                   version INTEGER NOT NULL)"""
        )
        # The journal's durable-capacity backfill counts these application-owned
        # tables, so a port that wants an open journal must create them.
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS factor_position_grid_history (
                   account_scope TEXT NOT NULL,
                   strategy_scope TEXT NOT NULL,
                   cohort_id TEXT NOT NULL,
                   close_time TEXT NOT NULL)"""
        )
        for table in _BOOK_TABLES:
            if table != "factor_position_grid_history":
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} (row_id INTEGER)")
        cursor.execute(
            """INSERT INTO factor_position_book_meta(singleton, version)
               VALUES(1, ?)
               ON CONFLICT(singleton) DO UPDATE SET version=excluded.version""",
            (self.stored_version,),
        )

    def record_grid(
        self,
        cursor: Any,
        *,
        account_scope: str,
        strategy_scope: str,
        cohort_id: str,
        batch_id: str,
        close_time: str,
        ordinal: int,
        capacity_class: str = "BASE",
    ) -> bool:
        self.recorded_grids.append((account_scope, strategy_scope, cohort_id, ordinal))
        return True

    def record_flat_checkpoint(
        self, cursor: Any, *, intent_id: str, reconciliation_id: str
    ) -> None:
        return None

    def validate_close_owner(
        self,
        cursor: Any,
        *,
        account_scope: str,
        strategy_scope: str,
        instrument: str,
        exchange: str,
        owner_intent_id: str,
    ) -> None:
        return None


class _FactsPort(JournalFeePairFactsPort):
    port_version = JOURNAL_PORT_SCHEMA_VERSION

    def __init__(self, *, accepted: bool = True) -> None:
        self.accepted = accepted
        self.predicate_calls: list[Any] = []
        self.lease_calls: list[tuple] = []
        self.identifier_calls: list[tuple] = []

    def is_factor_execution_facts(self, facts: Any) -> bool:
        self.predicate_calls.append(facts)
        return self.accepted

    def query_set_identifier(
        self,
        account_fingerprint: str,
        generation: int,
        trading_day: str,
        started_at: datetime,
        completed_at: datetime,
        query_ids,
    ) -> str:
        self.identifier_calls.append((account_fingerprint, generation, trading_day, query_ids))
        return "query-set:test"

    def validate_source_lease(self, lease: Any, facts: Any, *, operation: str) -> bool:
        self.lease_calls.append((lease, facts, operation))
        return self.accepted


class _AttestationPort(JournalRegisteredSimAttestationPort):
    port_version = JOURNAL_PORT_SCHEMA_VERSION

    def __init__(self, *, reject: str | None = None) -> None:
        self.reject = reject
        self.calls: list[tuple] = []

    def attest_order_identity_query(
        self, receipt: Any, *, source_store: Any
    ) -> AttestedCtpOrderIdentityQuery:
        self.calls.append((receipt, source_store))
        if self.reject is not None:
            raise ExecutionDecisionJournalError(self.reject)
        return AttestedCtpOrderIdentityQuery(
            request_id=7,
            connection_generation=3,
            account_fingerprint=_ACCOUNT,
            trading_day="20261031",
            started_at_utc=datetime(2026, 10, 31, tzinfo=timezone.utc),
            completed_at_utc=datetime(2026, 10, 31, 0, 0, 1, tzinfo=timezone.utc),
            front_id=11,
            session_id=12,
            broker_id="9999",
            investor_id="investor",
            rows=_ROWS,
            rows_sha256=_rows_sha256(_ROWS),
            source_expires_monotonic=1e18,
            source_lock=threading.Lock(),
        )


class _NoPrivateSymbolReceipt:
    """A receipt the SDK must not introspect: any attribute read explodes."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError("the journal must read only the port projection, not " + name)


def _bind_kwargs(**changes: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "source_store": object(),
        "broker_order_ref": 1,
        "expected_side": "BUY",
        "expected_position_effect": "OPEN",
        "expected_remaining_quantity": Decimal(1),
        "expected_instrument": _INSTRUMENT,
        "expected_exchange": "CFFEX",
        "expected_front_id": 11,
        "expected_session_id": 12,
    }
    kwargs.update(changes)
    return kwargs


@pytest.fixture(autouse=True)
def _isolated_process_ports():
    reset_journal_ports()
    yield
    reset_journal_ports()


def _journal(**slots: Any) -> ExecutionDecisionJournal:
    slots.setdefault("position_attribution", _PositionPort())
    return ExecutionDecisionJournal(":memory:", journal_ports=JournalPorts(**slots))


def _code(error: BaseException) -> str:
    assert isinstance(error, ExecutionDecisionJournalError), repr(error)
    return error.code


# --- rule (a): each slot must hold a real implementation of the matching ABC ---


def test_non_abc_object_is_rejected():
    for name, bogus in (
        ("position_attribution", SimpleNamespace()),
        ("fee_pair_facts", object()),
        ("registered_sim_attestation", _PositionPort()),
    ):
        with pytest.raises(ExecutionDecisionJournalError) as captured:
            configure_journal_ports(**{name: bogus})
        assert _code(captured.value) == "JOURNAL_PORT_INVALID:" + name
    assert _journal_ports() == JournalPorts()


def test_abstract_class_itself_is_not_an_instance():
    with pytest.raises(ExecutionDecisionJournalError) as captured:
        configure_journal_ports(fee_pair_facts=JournalFeePairFactsPort)
    assert _code(captured.value) == "JOURNAL_PORT_INVALID:fee_pair_facts"


def test_incomplete_subclass_cannot_be_instantiated():
    class Incomplete(JournalPositionAttributionPort):
        port_version = JOURNAL_PORT_SCHEMA_VERSION

    with pytest.raises(TypeError):
        Incomplete()


def test_container_validates_a_hand_built_set():
    with pytest.raises(ExecutionDecisionJournalError) as captured:
        JournalPorts(fee_pair_facts=SimpleNamespace())
    assert _code(captured.value) == "JOURNAL_PORT_INVALID:fee_pair_facts"


# --- rule (b): port_version must equal the declared schema version ---


def test_missing_version_is_unsupported():
    class NoVersion(_PositionPort):
        port_version = None

    with pytest.raises(ExecutionDecisionJournalError) as captured:
        configure_journal_ports(position_attribution=NoVersion())
    assert _code(captured.value) == "JOURNAL_PORT_VERSION_UNSUPPORTED"


def test_stale_and_forward_versions_are_unsupported():
    for declared in (JOURNAL_PORT_SCHEMA_VERSION - 1, JOURNAL_PORT_SCHEMA_VERSION + 1, "1"):
        candidate = _PositionPort()
        candidate.__class__.port_version = declared
        try:
            with pytest.raises(ExecutionDecisionJournalError) as captured:
                configure_journal_ports(position_attribution=candidate)
            assert _code(captured.value) == "JOURNAL_PORT_VERSION_UNSUPPORTED"
        finally:
            candidate.__class__.port_version = JOURNAL_PORT_SCHEMA_VERSION


# --- rule (c): no silent overwrite; replace=True is explicit ---


def test_same_instance_reconfigure_is_a_no_op():
    port = _PositionPort()
    configure_journal_ports(position_attribution=port)
    configure_journal_ports(position_attribution=port)
    assert _journal_ports().position_attribution is port


def test_different_instance_needs_replace_true():
    first = _PositionPort(label="first")
    second = _PositionPort(label="second")
    configure_journal_ports(position_attribution=first)
    with pytest.raises(ExecutionDecisionJournalError) as captured:
        configure_journal_ports(position_attribution=second)
    assert _code(captured.value) == "JOURNAL_PORT_ALREADY_CONFIGURED:position_attribution"
    assert _journal_ports().position_attribution is first
    configure_journal_ports(position_attribution=second, replace=True)
    assert _journal_ports().position_attribution is second


def test_replace_must_be_a_bool():
    configure_journal_ports(position_attribution=_PositionPort())
    with pytest.raises(ExecutionDecisionJournalError) as captured:
        configure_journal_ports(
            position_attribution=_PositionPort(), replace="yes"  # type: ignore[arg-type]
        )
    assert _code(captured.value) == "JOURNAL_PORT_INVALID:replace"


def test_reset_clears_every_slot():
    configure_journal_ports(
        position_attribution=_PositionPort(),
        fee_pair_facts=_FactsPort(),
        registered_sim_attestation=_AttestationPort(),
    )
    reset_journal_ports()
    assert _journal_ports() == JournalPorts()


def test_legacy_module_injection_is_retired_not_fallback():
    with pytest.raises(ExecutionDecisionJournalError) as captured:
        configure_backtrader_contracts(factor_position_book=SimpleNamespace())
    assert _code(captured.value) == "JOURNAL_PORT_LEGACY_RETIRED"
    assert _journal_ports() == JournalPorts()


# --- constructor injection wins over the process-registered set ---


def test_constructor_ports_are_used_instead_of_the_registered_set():
    registered = _PositionPort(label="registered")
    injected = _PositionPort(label="injected")
    configure_journal_ports(position_attribution=registered)
    journal = ExecutionDecisionJournal(
        ":memory:", journal_ports=JournalPorts(position_attribution=injected)
    )
    assert injected.ensure_schema_calls == 1
    assert registered.ensure_schema_calls == 0
    assert injected.expected_version_calls >= 1
    assert _journal_ports().position_attribution is registered
    journal.close()


def test_registered_set_is_the_fallback():
    registered = _PositionPort()
    configure_journal_ports(position_attribution=registered)
    journal = ExecutionDecisionJournal(":memory:")
    assert registered.ensure_schema_calls == 1
    journal.close()


def test_invalid_container_is_rejected_by_the_constructor():
    with pytest.raises(ExecutionDecisionJournalError) as captured:
        ExecutionDecisionJournal(":memory:", journal_ports=SimpleNamespace())
    assert _code(captured.value) == "JOURNAL_PORT_INVALID:journal_ports"


def test_expected_schema_version_comes_from_the_port():
    # The stored book version disagrees with the port's expectation, so the
    # journal must still reject with the pre-existing code.
    mismatching = _PositionPort(stored_version=5)
    with pytest.raises(ExecutionDecisionJournalError) as captured:
        ExecutionDecisionJournal(
            ":memory:", journal_ports=JournalPorts(position_attribution=mismatching)
        )
    assert _code(captured.value) == "POSITION_BOOK_SCHEMA_UNAVAILABLE"
    assert mismatching.expected_version_calls >= 1


# --- a missing port keeps failing closed with the renamed code ---


def test_missing_port_raises_journal_port_unavailable():
    for name in ("position_attribution", "fee_pair_facts", "registered_sim_attestation"):
        with pytest.raises(ExecutionDecisionJournalError) as captured:
            JournalPorts().require(name)
        assert _code(captured.value) == "JOURNAL_PORT_UNAVAILABLE:" + name


def test_journal_path_without_attestation_port_fails_closed():
    journal = _journal(registered_sim_attestation=None)
    try:
        with pytest.raises(ExecutionDecisionJournalError) as captured:
            journal.bind_known_ctp_order_sysid_from_query(
                "intent-a", _NoPrivateSymbolReceipt(), **_bind_kwargs()
            )
        assert _code(captured.value) == "JOURNAL_PORT_UNAVAILABLE:registered_sim_attestation"
    finally:
        journal.close()


# --- the journal delegates instead of doing inline type or private checks ---


def test_attestation_port_is_called_and_the_journal_uses_only_the_projection():
    port = _AttestationPort()
    receipt = _NoPrivateSymbolReceipt()
    store = object()
    journal = _journal(registered_sim_attestation=port)
    try:
        with pytest.raises(ExecutionDecisionJournalError) as captured:
            journal.bind_known_ctp_order_sysid_from_query(
                "intent-a", receipt, **_bind_kwargs(source_store=store)
            )
    finally:
        journal.close()
    # Once for the pre-transaction attestation and once for the in-transaction
    # revalidation, both through the port and never through a receipt symbol.
    assert port.calls == [(receipt, store), (receipt, store)]
    # The projection then carries the durable identity the journal compares;
    # this intent row does not exist, so the journal's own work fails closed.
    assert _code(captured.value) == "KNOWN_CTP_ORDER_SYSID_PARENT_NOT_KNOWN"


def test_attestation_rejection_keeps_the_existing_error_code():
    for rejected in ("KNOWN_CTP_ORDER_SYSID_QUERY_UNTRUSTED", "KNOWN_CTP_ORDER_SYSID_QUERY_STALE"):
        port = _AttestationPort(reject=rejected)
        journal = _journal(registered_sim_attestation=port)
        try:
            with pytest.raises(ExecutionDecisionJournalError) as captured:
                journal.bind_known_ctp_order_sysid_from_query(
                    "intent-a", _NoPrivateSymbolReceipt(), **_bind_kwargs()
                )
            assert _code(captured.value) == rejected
        finally:
            journal.close()


def test_facts_type_predicate_replaces_the_exact_class_check():
    port = _FactsPort(accepted=False)
    journal = _journal(fee_pair_facts=port)
    facts = SimpleNamespace(account_scope=_ACCOUNT)
    try:
        with pytest.raises(ExecutionDecisionJournalError) as captured:
            journal._fee_pair_grid_facts_proof(
                SimpleNamespace(target_routes=None, key=None, target_ids=frozenset()),
                facts,
                current_account_session_id="session",
                current_account_session_generation="generation",
                max_query_age_seconds=1.0,
                max_quote_age_seconds=1.0,
                now=datetime(2026, 10, 31, tzinfo=timezone.utc),
                require_fresh=False,
            )
        assert _code(captured.value) == "FEE_PAIR_FACTS_INVALID"
    finally:
        journal.close()
    assert port.predicate_calls == [facts]


def test_source_lease_guard_prefers_the_constructor_set():
    registered = _FactsPort(accepted=True)
    injected = _FactsPort(accepted=True)
    configure_journal_ports(fee_pair_facts=registered)
    lease = SimpleNamespace(require_valid=lambda: None)
    facts = object()
    guard = _fee_pair_facts_source_before_commit(
        facts, lease, operation="OPEN", ports=JournalPorts(fee_pair_facts=injected)
    )
    guard()
    assert injected.lease_calls == [(lease, facts, "OPEN"), (lease, facts, "OPEN")]
    assert registered.lease_calls == []
    # With no explicit set the process-registered port answers instead.
    _fee_pair_facts_source_before_commit(facts, lease, operation="OPEN")()
    assert registered.lease_calls == [(lease, facts, "OPEN"), (lease, facts, "OPEN")]
    # A refused lease still fails closed with the pre-existing code.
    with pytest.raises(ExecutionDecisionJournalError) as captured:
        _fee_pair_facts_source_before_commit(
            facts,
            lease,
            operation="OPEN",
            ports=JournalPorts(fee_pair_facts=_FactsPort(accepted=False)),
        )
    assert _code(captured.value) == "FEE_PAIR_SOURCE_BINDING_REQUIRED"
    # And an absent global port fails closed with the renamed code.
    reset_journal_ports()
    with pytest.raises(ExecutionDecisionJournalError) as captured:
        _fee_pair_facts_source_before_commit(facts, lease, operation="OPEN")
    assert _code(captured.value) == "JOURNAL_PORT_UNAVAILABLE:fee_pair_facts"


def test_source_lease_guard_rejects_a_foreign_container():
    with pytest.raises(ExecutionDecisionJournalError) as captured:
        _fee_pair_facts_source_before_commit(
            object(),
            SimpleNamespace(require_valid=lambda: None),
            operation="OPEN",
            ports=SimpleNamespace(),  # type: ignore[arg-type]
        )
    assert _code(captured.value) == "JOURNAL_PORT_INVALID:ports"


def test_quantity_expectations_still_validate_before_the_durable_work():
    port = _AttestationPort()
    receipt = _NoPrivateSymbolReceipt()
    store = object()
    journal = _journal(registered_sim_attestation=port)
    try:
        with pytest.raises(ExecutionDecisionJournalError) as captured:
            journal.bind_known_ctp_order_sysid_from_query(
                "intent-a", receipt, **_bind_kwargs(source_store=store, expected_side="HOLD")
            )
        assert _code(captured.value) == "KNOWN_CTP_ORDER_SYSID_EXPECTED_IDENTITY_INVALID"
    finally:
        journal.close()
    # The attestation happened first and exactly once: the caller's identity
    # arguments are the journal's own check and never reach the port.
    assert port.calls == [(receipt, store)]
