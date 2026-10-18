"""Durable BT financial ledger integration with the SQLite execution journal.

The terminal provider is synthetic.  This verifies local persistence, source
identity, fencing, and deterministic replay; it is not account or host proof.
"""

from __future__ import annotations

import json
import sqlite3
import time
from decimal import Decimal
from unittest.mock import patch

import pytest

import bt_api_execution.store as execution_store_module
from bt_api_execution import (
    ExecutionScope,
    ExecutionState,
    ManagedExecutionFacade,
    OrderIntent,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
    WriterLeaseUnavailable,
)
from bt_api_execution.account_financial_ledger import (
    AccountFinancialBaselineV1,
    CurrencyAmountV1,
    FinancialActivityBatchV1,
    FinancialActivityEventV1,
    FinancialDataUnavailableError,
    FinancialEventKind,
    FinancialExecutionCursorSnapshotV1,
    FinancialInstrumentSpecV1,
    FinancialLedgerCutV1,
    FinancialLedgerPolicyV1,
    FinancialOwnerBaselineV1,
    FinancialOwnerDirectoryV1,
    FinancialOwnerKind,
    FinancialOwnerV1,
    FinancialSourceKind,
    FinancialWatermarkV1,
    OwnerPositionCostV1,
    ProviderAccountFinancialSnapshotV1,
)

_SCOPE = ExecutionScope("FAKE", "simulation", "acct-financial", "strategy.a")
_MANUAL = FinancialOwnerV1(FinancialOwnerKind.MANUAL_BOOK, "manual.a", None)
_OWNER = FinancialOwnerV1(FinancialOwnerKind.STRATEGY, _SCOPE.strategy_id, _SCOPE.key)
_DIRECTORY = FinancialOwnerDirectoryV1(
    _SCOPE.account_key,
    "owner-directory-v1",
    "trusted-owner-registry",
    (_OWNER, _MANUAL),
)
_INSTRUMENT = "SPOT:BTC-USD"
_INSTRUMENT_SPEC = FinancialInstrumentSpecV1(
    _INSTRUMENT,
    "spot-meta-v1",
    "SPOT",
    "BTC",
    "USD",
)
_POLICY = FinancialLedgerPolicyV1(
    cash_currency="USD",
    reporting_currency="USD",
    max_account_fact_age_ns=10**19,
    max_activity_age_ns=10**19,
    max_cut_skew_ns=10**19,
    max_mark_age_ns=10**19,
    max_fx_age_ns=10**19,
)


class _Permit:
    def __init__(self, permit_id: str) -> None:
        self.permit_id = permit_id


class _Admission:
    def reserve(self, intent: OrderIntent) -> _Permit:
        return _Permit(f"permit-{intent.intent_id}")

    def validate(self, permit_id: str, intent: OrderIntent) -> _Permit:
        assert permit_id == f"permit-{intent.intent_id}"
        return _Permit(permit_id)

    def settle(self, permit_id: str) -> None:
        assert permit_id.startswith("permit-")

    def release(self, permit_id: str, reason: str) -> None:
        assert permit_id.startswith("permit-")
        assert reason


def _intent() -> OrderIntent:
    return OrderIntent.limit(
        intent_id="financial-order-1",
        scope=_SCOPE,
        signal_id="financial-signal-1",
        instrument=_INSTRUMENT,
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("10"),
        metadata_version="spot-meta-v1",
    )


def _baseline(
    cursor_snapshot: FinancialExecutionCursorSnapshotV1,
    *,
    activity_cursor: int = 0,
    baseline_ref: str = "financial-baseline-v1",
    version: int = 1,
) -> AccountFinancialBaselineV1:
    captured = cursor_snapshot.as_of_ns
    execution = FinancialWatermarkV1(
        FinancialSourceKind.EXECUTION_OUTBOX,
        "sqlite-execution-outbox",
        cursor_snapshot.journal_generation,
        cursor_snapshot.cursor,
        captured,
    )
    facts = FinancialWatermarkV1(
        FinancialSourceKind.ACCOUNT_FACTS,
        "trusted-provider-account-facts",
        "provider-facts-v1",
        1,
        captured,
    )
    activity = FinancialWatermarkV1(
        FinancialSourceKind.ACCOUNT_ACTIVITY,
        "trusted-provider-account-activity",
        "provider-activity-v1",
        activity_cursor,
        captured,
    )
    cut = FinancialLedgerCutV1(
        _SCOPE.account_key,
        _DIRECTORY.revision,
        captured,
        (execution, facts, activity),
    )
    snapshot = ProviderAccountFinancialSnapshotV1(
        _SCOPE.account_key,
        facts.source_ref,
        facts.generation,
        facts.cursor,
        facts.as_of_ns,
        (CurrencyAmountV1("USD", Decimal("989.9")),),
        ((_INSTRUMENT, Decimal("1")),),
        (CurrencyAmountV1("USD", Decimal("0.1")),),
    )
    return AccountFinancialBaselineV1(
        account_key=_SCOPE.account_key,
        baseline_ref=baseline_ref,
        version=version,
        authority_ref="trusted-baseline-authority",
        source_ref="trusted-financial-baseline-source",
        owner_directory_revision=_DIRECTORY.revision,
        cut=cut,
        provider_snapshot=snapshot,
        owners=(
            FinancialOwnerBaselineV1(
                _OWNER,
                (CurrencyAmountV1("USD", Decimal("489.9")),),
                (
                    OwnerPositionCostV1(
                        _INSTRUMENT,
                        Decimal("1"),
                        Decimal("10"),
                        "USD",
                        metadata_version="spot-meta-v1",
                    ),
                ),
                (CurrencyAmountV1("USD", Decimal("0.1")),),
            ),
            FinancialOwnerBaselineV1(
                _MANUAL,
                (CurrencyAmountV1("USD", Decimal("500")),),
                (),
                (CurrencyAmountV1("USD", Decimal("0")),),
            ),
        ),
        execution_cursors=cursor_snapshot,
    )


def _current_inputs(
    store: SqliteExecutionStore,
    *,
    provider_cursor: int,
) -> tuple[ProviderAccountFinancialSnapshotV1, FinancialLedgerCutV1, FinancialActivityBatchV1, int]:
    now_ns = time.time_ns()
    generation = str(store.journal_source_identity()["generation"])
    _count, _first, _last, high_water = store.execution_outbox_bounds()
    execution = FinancialWatermarkV1(
        FinancialSourceKind.EXECUTION_OUTBOX,
        "sqlite-execution-outbox",
        generation,
        high_water,
        now_ns,
    )
    facts = FinancialWatermarkV1(
        FinancialSourceKind.ACCOUNT_FACTS,
        "trusted-provider-account-facts",
        "provider-facts-v1",
        provider_cursor,
        now_ns,
    )
    activity = FinancialWatermarkV1(
        FinancialSourceKind.ACCOUNT_ACTIVITY,
        "trusted-provider-account-activity",
        "provider-activity-v1",
        0,
        now_ns,
    )
    cut = FinancialLedgerCutV1(
        _SCOPE.account_key,
        _DIRECTORY.revision,
        now_ns,
        (execution, facts, activity),
    )
    snapshot = ProviderAccountFinancialSnapshotV1(
        _SCOPE.account_key,
        facts.source_ref,
        facts.generation,
        facts.cursor,
        facts.as_of_ns,
        (CurrencyAmountV1("USD", Decimal("981.85")),),
        ((_INSTRUMENT, Decimal("1.5")),),
        (CurrencyAmountV1("USD", Decimal("0.15")),),
    )
    activity_batch = FinancialActivityBatchV1(
        activity.source_ref,
        activity.generation,
        0,
        0,
        activity.as_of_ns,
        (),
    )
    return snapshot, cut, activity_batch, now_ns


def _sqlite_financial_rows(path) -> tuple[int, int, str | None, str | None]:
    connection = sqlite3.connect(path)
    try:
        return (
            int(connection.execute("SELECT COUNT(*) FROM account_financial_events").fetchone()[0]),
            int(
                connection.execute(
                    "SELECT scanned_sequence FROM account_financial_ledger_heads WHERE account_key = ?",
                    (_SCOPE.account_key,),
                ).fetchone()[0]
            ),
            connection.execute(
                "SELECT projection_sha256 FROM account_financial_ledger_heads WHERE account_key = ?",
                (_SCOPE.account_key,),
            ).fetchone()[0],
            connection.execute(
                "SELECT input_sha256 FROM account_financial_ledger_heads WHERE account_key = ?",
                (_SCOPE.account_key,),
            ).fetchone()[0],
        )
    finally:
        connection.close()


def _persist_projection_for_read_test(path, *, include_foreign_account_event: bool = False):
    store = SqliteExecutionStore(path)
    facade = ManagedExecutionFacade(
        store, _SCOPE, writer_id="financial-read-writer", admission_gate=_Admission()
    )
    lease = None
    try:
        intent = _intent()
        facade.submit(
            intent,
            lambda order: ProviderObservation.accepted(order.intent_id, "venue-order-read"),
        )
        facade.record_provider_observation_event(
            ProviderObservation(
                intent.intent_id,
                ExecutionState.PARTIALLY_FILLED,
                provider_order_id="venue-order-read",
                filled_quantity=Decimal("1"),
                average_price=Decimal("10"),
                cumulative_commission=Decimal("0.1"),
                cumulative_commission_currency="USD",
            )
        )
        lease = facade.acquire_writer_lease()
        baseline_cursors = store.capture_account_financial_execution_cursors(_SCOPE, lease)
        baseline = _baseline(baseline_cursors)
        store.install_account_financial_baseline(
            baseline,
            _DIRECTORY,
            writer_scope=_SCOPE,
            writer_lease=lease,
        )
        if include_foreign_account_event:
            foreign_scope = ExecutionScope(
                "FAKE", "simulation", "acct-financial-other", "strategy.other"
            )
            foreign_facade = ManagedExecutionFacade(
                store,
                foreign_scope,
                writer_id="foreign-financial-read-writer",
                admission_gate=_Admission(),
            )
            foreign_intent = OrderIntent.limit(
                intent_id="financial-order-foreign-account",
                scope=foreign_scope,
                signal_id="financial-signal-foreign-account",
                instrument=_INSTRUMENT,
                side=Side.BUY,
                quantity=Decimal("1"),
                price=Decimal("10"),
                metadata_version="spot-meta-v1",
            )
            try:
                foreign_facade.submit(
                    foreign_intent,
                    lambda order: ProviderObservation.accepted(
                        order.intent_id, "venue-order-foreign-account"
                    ),
                )
            finally:
                foreign_facade.close()
        facade.record_provider_observation_event(
            ProviderObservation(
                intent.intent_id,
                ExecutionState.PARTIALLY_FILLED,
                provider_order_id="venue-order-read",
                filled_quantity=Decimal("1.5"),
                average_price=Decimal("12"),
                cumulative_commission=Decimal("0.15"),
                cumulative_commission_currency="USD",
            )
        )
        current_account, current_cut, activity, now_ns = _current_inputs(
            store, provider_cursor=2
        )
        projection = store.reconcile_account_financials(
            baseline,
            _DIRECTORY,
            current_account=current_account,
            current_cut=current_cut,
            activity_batch=activity,
            instrument_specs={_INSTRUMENT: _INSTRUMENT_SPEC},
            policy=_POLICY,
            writer_scope=_SCOPE,
            writer_lease=lease,
            now_ns=now_ns,
        )
        return projection
    finally:
        if lease is not None:
            store.release_lease(
                _SCOPE,
                lease.owner_id,
                fencing_token=lease.fencing_token,
            )
        facade.close()
        store.close()


@pytest.mark.unit
def test_read_financial_projection_survives_reopen_is_read_only_and_rejects_stale_head(
    tmp_path,
) -> None:
    path = tmp_path / "financial-projection-read.sqlite3"
    expected = _persist_projection_for_read_test(path)

    store = SqliteExecutionStore(path)
    try:
        changes_before = store._connection.total_changes
        actual = store.read_account_financial_projection(
            _SCOPE.account_key,
            expected_directory=_DIRECTORY,
        )
        assert actual == expected
        assert store._connection.total_changes == changes_before
        assert actual.cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX).cursor > 0
        assert actual.cut.watermark(FinancialSourceKind.ACCOUNT_FACTS).cursor == 2

        wrong_directory = FinancialOwnerDirectoryV1(
            _DIRECTORY.account_key,
            "different-owner-directory-revision",
            _DIRECTORY.source_ref,
            _DIRECTORY.owners,
        )
        with pytest.raises(FinancialDataUnavailableError, match="another owner directory"):
            store.read_account_financial_projection(
                _SCOPE.account_key,
                expected_directory=wrong_directory,
            )
        assert store._connection.total_changes == changes_before

        facade = ManagedExecutionFacade(
            store,
            _SCOPE,
            writer_id="financial-read-after-head",
            admission_gate=_Admission(),
        )
        try:
            later_intent = OrderIntent.limit(
                intent_id="financial-order-after-projection",
                scope=_SCOPE,
                signal_id="financial-signal-after-projection",
                instrument=_INSTRUMENT,
                side=Side.BUY,
                quantity=Decimal("1"),
                price=Decimal("10"),
                metadata_version="spot-meta-v1",
            )
            facade.submit(
                later_intent,
                lambda order: ProviderObservation.accepted(
                    order.intent_id, "venue-order-after-projection"
                ),
            )
        finally:
            facade.close()

        changes_before_stale_read = store._connection.total_changes
        with pytest.raises(
            FinancialDataUnavailableError,
            match="does not cover the current execution journal",
        ):
            store.read_account_financial_projection(
                _SCOPE.account_key,
                expected_directory=_DIRECTORY,
            )
        assert store._connection.total_changes == changes_before_stale_read
    finally:
        store.close()


@pytest.mark.unit
def test_read_financial_projection_rejects_mutated_durable_execution_source(tmp_path) -> None:
    path = tmp_path / "financial-projection-mutated-source.sqlite3"
    _persist_projection_for_read_test(path)

    connection = sqlite3.connect(path)
    try:
        sequence, payload_json = connection.execute(
            "SELECT sequence, payload_json FROM execution_outbox "
            "WHERE scope_key = ? AND intent_id = ? AND event_type IN "
            "('provider_observation', 'reconciled_observation', "
            "'provider_commission_evidence') ORDER BY sequence DESC LIMIT 1",
            (_SCOPE.key, _intent().intent_id),
        ).fetchone()
        payload = json.loads(payload_json)
        payload["cumulative_commission"] = "0.16"
        connection.execute(
            "UPDATE execution_outbox SET payload_json = ? WHERE sequence = ?",
            (json.dumps(payload, separators=(",", ":"), sort_keys=True), sequence),
        )
        connection.commit()
    finally:
        connection.close()

    store = SqliteExecutionStore(path)
    try:
        changes_before = store._connection.total_changes
        with pytest.raises(
            FinancialDataUnavailableError,
            match="persisted financial event differs from its durable execution source",
        ):
            store.read_account_financial_projection(
                _SCOPE.account_key,
                expected_directory=_DIRECTORY,
            )
        assert store._connection.total_changes == changes_before
    finally:
        store.close()


@pytest.mark.unit
def test_read_financial_projection_accepts_foreign_account_outbox_rows(tmp_path) -> None:
    path = tmp_path / "financial-projection-read-foreign-account.sqlite3"
    expected = _persist_projection_for_read_test(
        path, include_foreign_account_event=True
    )

    store = SqliteExecutionStore(path)
    try:
        changes_before = store._connection.total_changes
        actual = store.read_account_financial_projection(
            _SCOPE.account_key,
            expected_directory=_DIRECTORY,
        )
        assert actual == expected
        assert store._connection.total_changes == changes_before
        assert actual.cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX).cursor > 0
    finally:
        store.close()


@pytest.mark.unit
def test_financial_baseline_lease_expiry_during_cursor_scan_rolls_back(tmp_path) -> None:
    path = tmp_path / "financial-baseline-expired.sqlite3"
    store = SqliteExecutionStore(path)
    lease = store.acquire_or_renew_lease(_SCOPE, "financial-baseline-writer")
    try:
        cursor_snapshot = store.capture_account_financial_execution_cursors(_SCOPE, lease)
        baseline = _baseline(cursor_snapshot)
        scan_finished = False
        clock_observations: list[tuple[bool, bool]] = []
        original_capture = store._capture_financial_cursor_snapshot_in_transaction

        def capture_then_mark_scan_complete(*args, **kwargs):
            nonlocal scan_finished
            snapshot = original_capture(*args, **kwargs)
            scan_finished = True
            return snapshot

        def lease_clock() -> int:
            clock_observations.append((store._connection.in_transaction, scan_finished))
            if len(clock_observations) == 1:
                return lease.expires_at_ns - 1
            assert len(clock_observations) == 2
            assert scan_finished is True
            return lease.expires_at_ns + 1

        with (
            patch.object(execution_store_module.time, "time_ns", side_effect=lease_clock),
            patch.object(
                store,
                "_capture_financial_cursor_snapshot_in_transaction",
                side_effect=capture_then_mark_scan_complete,
            ),
            pytest.raises(WriterLeaseUnavailable),
        ):
            store.install_account_financial_baseline(
                baseline,
                _DIRECTORY,
                writer_scope=_SCOPE,
                writer_lease=lease,
            )
        assert clock_observations == [(True, False), (True, True)]
        connection = sqlite3.connect(path)
        try:
            assert (
                connection.execute("SELECT COUNT(*) FROM account_financial_baselines").fetchone()[0]
                == 0
            )
            assert (
                connection.execute(
                    "SELECT COUNT(*) FROM account_financial_ledger_heads"
                ).fetchone()[0]
                == 0
            )
            assert (
                connection.execute("SELECT COUNT(*) FROM account_financial_events").fetchone()[0]
                == 0
            )
        finally:
            connection.close()
    finally:
        store.release_lease(
            _SCOPE,
            lease.owner_id,
            fencing_token=lease.fencing_token,
        )
        store.close()


@pytest.mark.unit
def test_financial_baseline_rejects_same_generation_activity_cursor_regression(tmp_path) -> None:
    path = tmp_path / "financial-baseline-activity-regression.sqlite3"
    store = SqliteExecutionStore(path)
    lease = store.acquire_or_renew_lease(_SCOPE, "financial-baseline-writer")
    try:
        cursor_snapshot = store.capture_account_financial_execution_cursors(_SCOPE, lease)
        installed_baseline = _baseline(cursor_snapshot, activity_cursor=3)
        assert (
            len(
                store.install_account_financial_baseline(
                    installed_baseline,
                    _DIRECTORY,
                    writer_scope=_SCOPE,
                    writer_lease=lease,
                )
            )
            == 64
        )

        regressed_baseline = _baseline(
            cursor_snapshot,
            activity_cursor=2,
            baseline_ref="financial-baseline-v2",
            version=2,
        )
        with pytest.raises(
            FinancialDataUnavailableError,
            match="new financial baseline regresses the current ledger head",
        ):
            store.install_account_financial_baseline(
                regressed_baseline,
                _DIRECTORY,
                writer_scope=_SCOPE,
                writer_lease=lease,
            )

        connection = sqlite3.connect(path)
        try:
            assert (
                connection.execute("SELECT COUNT(*) FROM account_financial_baselines").fetchone()[0]
                == 1
            )
            assert connection.execute(
                "SELECT baseline_version, activity_generation, activity_cursor "
                "FROM account_financial_ledger_heads WHERE account_key = ?",
                (_SCOPE.account_key,),
            ).fetchone() == (1, "provider-activity-v1", 3)
            assert (
                connection.execute("SELECT COUNT(*) FROM account_financial_events").fetchone()[0]
                == 0
            )
        finally:
            connection.close()
    finally:
        store.release_lease(
            _SCOPE,
            lease.owner_id,
            fencing_token=lease.fencing_token,
        )
        store.close()


@pytest.mark.unit
def test_sqlite_financial_posting_is_fenced_atomic_and_replays_after_reopen(tmp_path) -> None:
    path = tmp_path / "execution.sqlite3"
    store = SqliteExecutionStore(path)
    facade = ManagedExecutionFacade(
        store, _SCOPE, writer_id="financial-writer-a", admission_gate=_Admission()
    )
    intent = _intent()
    current_lease = None
    try:
        facade.submit(
            intent,
            lambda order: ProviderObservation.accepted(order.intent_id, "venue-order-1"),
        )
        assert (
            facade.record_provider_observation_event(
                ProviderObservation(
                    intent.intent_id,
                    ExecutionState.PARTIALLY_FILLED,
                    provider_order_id="venue-order-1",
                    filled_quantity=Decimal("1"),
                    average_price=Decimal("10"),
                    cumulative_commission=Decimal("0.1"),
                    cumulative_commission_currency="USD",
                )
            )
            is not None
        )
        original_lease = facade.acquire_writer_lease()
        baseline_cursors = store.capture_account_financial_execution_cursors(_SCOPE, original_lease)
        baseline = _baseline(baseline_cursors)
        baseline_sha = store.install_account_financial_baseline(
            baseline,
            _DIRECTORY,
            writer_scope=_SCOPE,
            writer_lease=original_lease,
        )
        assert len(baseline_sha) == 64
        assert (
            store.install_account_financial_baseline(
                baseline,
                _DIRECTORY,
                writer_scope=_SCOPE,
                writer_lease=original_lease,
            )
            == baseline_sha
        )

        quiet_intent = OrderIntent.limit(
            intent_id="financial-order-without-fill",
            scope=_SCOPE,
            signal_id="financial-signal-without-fill",
            instrument=_INSTRUMENT,
            side=Side.BUY,
            quantity=Decimal("1"),
            price=Decimal("10"),
            metadata_version="spot-meta-v1",
        )
        facade.submit(
            quiet_intent,
            lambda order: ProviderObservation.accepted(order.intent_id, "venue-order-2"),
        )
        assert (
            facade.record_provider_observation_event(
                ProviderObservation(
                    quiet_intent.intent_id,
                    ExecutionState.ACKED,
                    provider_order_id="venue-order-2",
                    filled_quantity=Decimal("0"),
                    average_price=None,
                    cumulative_commission=Decimal("0"),
                    cumulative_commission_currency="USD",
                )
            )
            is not None
        )

        assert (
            facade.record_provider_observation_event(
                ProviderObservation(
                    intent.intent_id,
                    ExecutionState.PARTIALLY_FILLED,
                    provider_order_id="venue-order-1",
                    filled_quantity=Decimal("1.5"),
                    average_price=Decimal("12"),
                    cumulative_commission=Decimal("0.15"),
                    cumulative_commission_currency="USD",
                )
            )
            is not None
        )
        current_account, current_cut, activity, now_ns = _current_inputs(store, provider_cursor=2)

        assert store.release_lease(
            _SCOPE,
            original_lease.owner_id,
            fencing_token=original_lease.fencing_token,
        )
        current_lease = store.acquire_or_renew_lease(_SCOPE, "financial-writer-b")

        rows_before_expired_recheck = _sqlite_financial_rows(path)
        clock_transaction_states: list[bool] = []

        def lease_check_clock() -> int:
            in_transaction = store._connection.in_transaction
            clock_transaction_states.append(in_transaction)
            sample = len(clock_transaction_states)
            return current_lease.expires_at_ns + (1 if sample == 4 else -1)

        with (
            patch.object(execution_store_module.time, "time_ns", side_effect=lease_check_clock),
            pytest.raises(WriterLeaseUnavailable),
        ):
            store.reconcile_account_financials(
                baseline,
                _DIRECTORY,
                current_account=current_account,
                current_cut=current_cut,
                activity_batch=activity,
                instrument_specs={_INSTRUMENT: _INSTRUMENT_SPEC},
                policy=_POLICY,
                writer_scope=_SCOPE,
                writer_lease=current_lease,
                now_ns=now_ns,
            )
        assert clock_transaction_states == [True, True, True, True]
        assert _sqlite_financial_rows(path) == rows_before_expired_recheck

        with pytest.raises(WriterLeaseUnavailable):
            store.reconcile_account_financials(
                baseline,
                _DIRECTORY,
                current_account=current_account,
                current_cut=current_cut,
                activity_batch=activity,
                instrument_specs={_INSTRUMENT: _INSTRUMENT_SPEC},
                policy=_POLICY,
                writer_scope=_SCOPE,
                writer_lease=original_lease,
                now_ns=now_ns,
            )
        assert _sqlite_financial_rows(path)[:2] == (
            0,
            baseline.cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX).cursor,
        )

        base_activity_watermark = current_cut.watermark(FinancialSourceKind.ACCOUNT_ACTIVITY)
        late_activity_watermark = FinancialWatermarkV1(
            FinancialSourceKind.ACCOUNT_ACTIVITY,
            base_activity_watermark.source_ref,
            base_activity_watermark.generation,
            1,
            base_activity_watermark.as_of_ns,
        )
        late_activity_cut = FinancialLedgerCutV1(
            current_cut.account_key,
            current_cut.owner_directory_revision,
            current_cut.captured_at_ns,
            tuple(
                late_activity_watermark
                if row.source_kind is FinancialSourceKind.ACCOUNT_ACTIVITY
                else row
                for row in current_cut.watermarks
            ),
        )
        late_activity = FinancialActivityBatchV1(
            late_activity_watermark.source_ref,
            late_activity_watermark.generation,
            0,
            1,
            late_activity_watermark.as_of_ns,
            (
                FinancialActivityEventV1(
                    source_ref=late_activity_watermark.source_ref,
                    generation=late_activity_watermark.generation,
                    sequence=1,
                    event_id="late-managed-order-fill",
                    occurred_at_ns=late_activity_watermark.as_of_ns - 1,
                    owner_key=_MANUAL.key,
                    kind=FinancialEventKind.SPOT_FILL,
                    cash_movements=(CurrencyAmountV1("USD", Decimal("-10")),),
                    fee=CurrencyAmountV1("USD", Decimal("0.02")),
                    instrument=_INSTRUMENT,
                    signed_quantity=Decimal("1"),
                    price=Decimal("10"),
                    provider_order_ref="venue-order-2",
                ),
            ),
        )
        late_activity_account = ProviderAccountFinancialSnapshotV1(
            current_account.account_key,
            current_account.source_ref,
            current_account.generation,
            current_account.cursor,
            current_account.as_of_ns,
            (CurrencyAmountV1("USD", Decimal("971.83")),),
            ((_INSTRUMENT, Decimal("2.5")),),
            (CurrencyAmountV1("USD", Decimal("0.17")),),
        )
        rows_before_duplicate_activity = _sqlite_financial_rows(path)
        with pytest.raises(FinancialDataUnavailableError, match="duplicates a managed"):
            store.reconcile_account_financials(
                baseline,
                _DIRECTORY,
                current_account=late_activity_account,
                current_cut=late_activity_cut,
                activity_batch=late_activity,
                instrument_specs={_INSTRUMENT: _INSTRUMENT_SPEC},
                policy=_POLICY,
                writer_scope=_SCOPE,
                writer_lease=current_lease,
                now_ns=now_ns,
            )
        assert _sqlite_financial_rows(path) == rows_before_duplicate_activity

        projection = store.reconcile_account_financials(
            baseline,
            _DIRECTORY,
            current_account=current_account,
            current_cut=current_cut,
            activity_batch=activity,
            instrument_specs={_INSTRUMENT: _INSTRUMENT_SPEC},
            policy=_POLICY,
            writer_scope=_SCOPE,
            writer_lease=current_lease,
            now_ns=now_ns,
        )
        owner_book = projection.owner_book(_OWNER.key)
        assert projection.complete is True
        assert projection.state == "BALANCED"
        assert owner_book.cash == (CurrencyAmountV1("USD", Decimal("481.85")),)
        assert owner_book.positions[0].quantity == Decimal("1.5")
        assert owner_book.positions[0].cost_basis == Decimal("18")
        assert owner_book.cumulative_fees == (CurrencyAmountV1("USD", Decimal("0.15")),)
        persisted_after_first = _sqlite_financial_rows(path)
        assert (
            persisted_after_first[0]
            == current_cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX).cursor
            - baseline.cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX).cursor
        )
        assert (
            persisted_after_first[1]
            == current_cut.watermark(FinancialSourceKind.EXECUTION_OUTBOX).cursor
        )
    finally:
        if current_lease is not None:
            store.release_lease(
                _SCOPE,
                current_lease.owner_id,
                fencing_token=current_lease.fencing_token,
            )
        facade.close()
        store.close()

    reopened = SqliteExecutionStore(path)
    reopened_lease = reopened.acquire_or_renew_lease(_SCOPE, "financial-writer-c")
    try:
        replay = reopened.reconcile_account_financials(
            baseline,
            _DIRECTORY,
            current_account=current_account,
            current_cut=current_cut,
            activity_batch=activity,
            instrument_specs={_INSTRUMENT: _INSTRUMENT_SPEC},
            policy=_POLICY,
            writer_scope=_SCOPE,
            writer_lease=reopened_lease,
            now_ns=time.time_ns(),
        )
        assert replay == projection
        assert _sqlite_financial_rows(path) == persisted_after_first

        connection = sqlite3.connect(path)
        try:
            with pytest.raises(sqlite3.IntegrityError, match="immutable"):
                connection.execute(
                    "UPDATE account_financial_baselines SET baseline_ref = 'rewritten' "
                    "WHERE account_key = ? AND version = 1",
                    (_SCOPE.account_key,),
                )
            sequence, payload_json = connection.execute(
                "SELECT sequence, payload_json FROM execution_outbox "
                "WHERE scope_key = ? AND intent_id = ? AND event_type = 'reconciled_observation' "
                "ORDER BY sequence DESC LIMIT 1",
                (_SCOPE.key, intent.intent_id),
            ).fetchone()
            changed_payload = json.loads(payload_json)
            changed_payload["cumulative_commission"] = "0.16"
            connection.execute(
                "UPDATE execution_outbox SET payload_json = ? WHERE sequence = ?",
                (
                    json.dumps(changed_payload, separators=(",", ":"), sort_keys=True),
                    sequence,
                ),
            )
            connection.commit()
        finally:
            connection.close()

        with pytest.raises(
            FinancialDataUnavailableError,
            match="durable execution source changed after financial ledger application",
        ):
            reopened.reconcile_account_financials(
                baseline,
                _DIRECTORY,
                current_account=current_account,
                current_cut=current_cut,
                activity_batch=activity,
                instrument_specs={_INSTRUMENT: _INSTRUMENT_SPEC},
                policy=_POLICY,
                writer_scope=_SCOPE,
                writer_lease=reopened_lease,
                now_ns=time.time_ns(),
            )
        assert _sqlite_financial_rows(path) == persisted_after_first
    finally:
        reopened.release_lease(
            _SCOPE,
            reopened_lease.owner_id,
            fencing_token=reopened_lease.fencing_token,
        )
        reopened.close()


@pytest.mark.unit
def test_v22_store_migration_adds_financial_schema_without_changing_journal(tmp_path) -> None:
    path = tmp_path / "execution-v22.sqlite3"
    store = SqliteExecutionStore(path)
    facade = ManagedExecutionFacade(
        store, _SCOPE, writer_id="migration-writer", admission_gate=_Admission()
    )
    try:
        facade.submit(
            _intent(),
            lambda order: ProviderObservation.accepted(order.intent_id, "venue-order-1"),
        )
        old_events = store.read_outbox()
    finally:
        facade.close()
        store.close()

    connection = sqlite3.connect(path)
    try:
        for trigger in (
            "account_financial_baselines_immutable_update",
            "account_financial_baselines_immutable_delete",
            "account_financial_events_immutable_update",
            "account_financial_events_immutable_delete",
            "ctp_callback_session_owner_scopes_owner_match",
            "ctp_callback_session_owner_scopes_immutable_update",
            "ctp_callback_session_owner_scopes_immutable_delete",
        ):
            connection.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')
        for table in (
            "account_financial_ledger_heads",
            "account_financial_events",
            "account_financial_baselines",
            "ctp_dispatch_callback_session_owner_scopes",
        ):
            connection.execute(f'DROP TABLE IF EXISTS "{table}"')
        connection.execute("UPDATE execution_meta SET value = '22' WHERE key = 'schema_version'")
        connection.commit()
    finally:
        connection.close()

    migrated = SqliteExecutionStore(path)
    try:
        assert migrated.read_outbox() == old_events
        connection = sqlite3.connect(path)
        try:
            schema_version = connection.execute(
                "SELECT value FROM execution_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            assert schema_version == "24"
            tables = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
            triggers = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                )
            }
            assert {
                "account_financial_baselines",
                "account_financial_events",
                "account_financial_ledger_heads",
            } <= tables
            assert {
                "account_financial_baselines_immutable_update",
                "account_financial_baselines_immutable_delete",
                "account_financial_events_immutable_update",
                "account_financial_events_immutable_delete",
            } <= triggers
        finally:
            connection.close()
    finally:
        migrated.close()
