"""Subprocess crash/restart coverage for the durable order-entry boundary."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
_EXECUTION_SRC = _PACKAGE_ROOT / "src"
_BASE_SRC = _PACKAGE_ROOT.parent / "bt_api_base" / "src"

_CHILD = r"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

sys.path[:0] = [sys.argv[5], sys.argv[6]]

from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope
from bt_api_execution import (
    CancelObservation,
    ExecutionScope,
    ExecutionState,
    ManagedExecutionFacade,
    ProviderObservation,
    SqliteExecutionStore,
)
from bt_api_execution.service_order_entry import ExecutionOrderEntryService, SUBMIT_OPERATION

mode, phase, database_path, journal_path = sys.argv[1:5]
business_id = "exe02-original-business-id"
wire_scope = WireScope("paper:exe02", account_ref="account-a", strategy_ref="strategy-a")
execution_scope = ExecutionScope(
    "PAPER", "simulation", "account-a", "strategy-a", "run-exe02"
)
identity = WireIdentity(WireRole.STRATEGY_CLIENT, "exe02-principal", "strategy-a")
arguments = {
    "intent_id": business_id,
    "signal_id": business_id,
    "instrument": "BTC-USDT",
    "side": "BUY",
    "position_effect": "OPEN",
    "order_type": "LIMIT",
    "quantity": "2",
    "price": "50000",
    "time_in_force": "GTC",
    "reduce_only": False,
    "metadata_version": "metadata-v1",
    "deadline_ns": None,
    "tags": {},
}


class SyntheticProvider:
    def submit(self, intent):
        connection = sqlite3.connect(journal_path, isolation_level=None)
        try:
            connection.execute(
                "INSERT INTO provider_calls (intent_id) VALUES (?)", (intent.intent_id,)
            )
        finally:
            connection.close()
        if mode == "crash" and phase == "during_provider":
            os._exit(73)
        return ProviderObservation.accepted(intent.intent_id, "synthetic-order-1")

    def cancel(self, intent):
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )


store = SqliteExecutionStore(database_path)
service = ExecutionOrderEntryService(
    store,
    {WireScope("paper:exe02", account_ref="account-a", strategy_ref="strategy-a"): execution_scope},
    SyntheticProvider(),
    allow_unprotected=True,
    writer_id="exe02-crash-recovery-writer",
)


def handle_original():
    return service.handle(
        SUBMIT_OPERATION,
        principal_ref="exe02-principal",
        identity=identity,
        scope=wire_scope,
        arguments=arguments,
    )


if mode == "crash":
    crash_codes = {
        "preclaim": 71,
        "before_provider": 72,
        "during_provider": 73,
        "before_report": 74,
        "after_report": 75,
    }
    expected_code = crash_codes[phase]

    if phase == "preclaim":
        original = SqliteExecutionStore.activate_intent

        def crash_after_preclaim(self, *args, **kwargs):
            record = original(self, *args, **kwargs)
            if record.state is ExecutionState.PENDING_DISPATCH:
                os._exit(expected_code)
            return record

        SqliteExecutionStore.activate_intent = crash_after_preclaim
    elif phase == "before_provider":
        original = SqliteExecutionStore.claim_for_dispatch

        def crash_after_claim(self, *args, **kwargs):
            record, claimed = original(self, *args, **kwargs)
            if claimed:
                os._exit(expected_code)
            return record, claimed

        SqliteExecutionStore.claim_for_dispatch = crash_after_claim
    elif phase == "before_report":
        def crash_before_observation_write(self, *args, **kwargs):
            os._exit(expected_code)

        SqliteExecutionStore.record_observation = crash_before_observation_write
    elif phase == "after_report":
        original = ManagedExecutionFacade._settle_after_evidenced_outcome

        def crash_after_observation_write(self, *args, **kwargs):
            record = original(self, *args, **kwargs)
            os._exit(expected_code)
            return record

        ManagedExecutionFacade._settle_after_evidenced_outcome = crash_after_observation_write

    handle_original()
    raise SystemExit("crash injection boundary was not reached")

if mode == "restart":
    at_restart = store.get(business_id, scope=execution_scope)
    recovered = service.recover_interrupted_dispatches()
    repeated_recovery = service.recover_interrupted_dispatches()
    before_replay = store.get(business_id, scope=execution_scope)
    reply = handle_original()
    after_replay = store.get(business_id, scope=execution_scope)
    events = store.read_outbox(scope=execution_scope, after_sequence=0, limit=10_000)
    print(
        json.dumps(
            {
                "business_id": business_id,
                "scope_key": execution_scope.key,
                "state_at_restart": at_restart.state.value,
                "before_replay_state": before_replay.state.value,
                "recovered_states": [record.state.value for record in recovered],
                "recovered_scope_keys": [record.scope_key for record in recovered],
                "repeated_recovery_count": len(repeated_recovery),
                "reply_code": reply["code"],
                "reply_state": reply["result"]["record"]["state"],
                "after_replay_state": after_replay.state.value,
                "unknown_event_count": sum(
                    event.event_type == "dispatch_unknown" for event in events
                ),
            },
            sort_keys=True,
        )
    )
    store.close()
    raise SystemExit(0)

raise SystemExit("unknown child mode")
"""


def _prepare_journal(path: Path) -> None:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute(
            "CREATE TABLE provider_calls (call_id INTEGER PRIMARY KEY, intent_id TEXT NOT NULL)"
        )
    finally:
        connection.close()


def _provider_call_count(path: Path) -> int:
    connection = sqlite3.connect(path)
    try:
        return int(connection.execute("SELECT COUNT(*) FROM provider_calls").fetchone()[0])
    finally:
        connection.close()


def _run_child(
    mode: str,
    phase: str,
    database_path: Path,
    journal_path: Path,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed child source and pytest-owned paths
        [
            sys.executable,
            "-c",
            _CHILD,
            mode,
            phase,
            str(database_path),
            str(journal_path),
            str(_EXECUTION_SRC),
            str(_BASE_SRC),
        ],
        cwd=_PACKAGE_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env=os.environ.copy(),
    )


@pytest.mark.parametrize(
    (
        "phase",
        "crash_code",
        "initial_state",
        "recovered_state",
        "final_state",
        "provider_calls",
        "unknown_events",
    ),
    [
        ("preclaim", 71, "PENDING_DISPATCH", None, "ACKED", 1, 0),
        ("before_provider", 72, "DISPATCHING", "UNKNOWN", "UNKNOWN", 0, 1),
        ("during_provider", 73, "DISPATCHING", "UNKNOWN", "UNKNOWN", 1, 1),
        ("before_report", 74, "DISPATCHING", "UNKNOWN", "UNKNOWN", 1, 1),
        ("after_report", 75, "ACKED", None, "ACKED", 1, 0),
    ],
)
def test_crash_restart_recovers_claims_without_duplicate_provider_dispatch(
    tmp_path: Path,
    phase: str,
    crash_code: int,
    initial_state: str,
    recovered_state: str | None,
    final_state: str,
    provider_calls: int,
    unknown_events: int,
) -> None:
    database_path = tmp_path / "execution.sqlite3"
    journal_path = tmp_path / "provider-calls.sqlite3"
    _prepare_journal(journal_path)

    crashed = _run_child("crash", phase, database_path, journal_path)
    assert crashed.returncode == crash_code, crashed.stderr
    assert crashed.stdout == ""
    calls_before_restart = 1 if phase in {"during_provider", "before_report", "after_report"} else 0
    assert _provider_call_count(journal_path) == calls_before_restart

    restarted = _run_child("restart", phase, database_path, journal_path)
    assert restarted.returncode == 0, restarted.stderr
    result = json.loads(restarted.stdout)

    assert result["business_id"] == "exe02-original-business-id"
    assert result["state_at_restart"] == initial_state
    assert result["recovered_states"] == ([] if recovered_state is None else [recovered_state])
    assert result["recovered_scope_keys"] == (
        [] if recovered_state is None else [result["scope_key"]]
    )
    assert result["before_replay_state"] == (recovered_state or initial_state)
    assert result["repeated_recovery_count"] == 0
    assert result["reply_code"] == "OK"
    assert result["reply_state"] == final_state
    assert result["reply_state"] == result["after_replay_state"]
    assert result["unknown_event_count"] == unknown_events
    assert _provider_call_count(journal_path) == provider_calls
