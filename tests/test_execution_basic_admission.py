"""Production BasicAdmission gate coverage through Execution service and SQLite."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    CancelObservation,
    ExecutionAdmissionContextV1,
    ExecutionAdmissionValidatorRegistry,
    ExecutionBasicAdmissionGate,
    ExecutionProviderValidatorV1,
    ExecutionScope,
    ExecutionState,
    OrderIntent,
    OrderType,
    PositionEffect,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
    TimeInForce,
)
from bt_api_execution.contracts import order_intent_from_payload
from bt_api_execution.service_order_entry import SUBMIT_OPERATION, ExecutionOrderEntryService

_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
_EXECUTION_SRC = _PACKAGE_ROOT / "src"
_BASE_SRC = _PACKAGE_ROOT.parent / "bt_api_base" / "src"
_WIRE_SCOPE = WireScope("paper:exe04", account_ref="account-a", strategy_ref="strategy-a")
_EXECUTION_SCOPE = ExecutionScope("PAPER", "simulation", "account-a", "strategy-a", "20261015")
_IDENTITY = WireIdentity(WireRole.STRATEGY_CLIENT, "exe04-principal", "strategy-a")

_CRASH_CHILD = r"""
from __future__ import annotations

import json
import os
import sqlite3
import sys

sys.path[:0] = [sys.argv[4], sys.argv[5]]

from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope
from bt_api_execution import (
    CancelObservation,
    ExecutionAdmissionValidatorRegistry,
    ExecutionBasicAdmissionGate,
    ExecutionProviderValidatorV1,
    ExecutionScope,
    ExecutionState,
    OrderType,
    PositionEffect,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
    TimeInForce,
)
from bt_api_execution.service_order_entry import ExecutionOrderEntryService, SUBMIT_OPERATION

mode, database_path, journal_path = sys.argv[1:4]
wire_scope = WireScope("paper:exe04", account_ref="account-a", strategy_ref="strategy-a")
execution_scope = ExecutionScope(
    "PAPER", "simulation", "account-a", "strategy-a", "20261015"
)
identity = WireIdentity(WireRole.STRATEGY_CLIENT, "exe04-principal", "strategy-a")
arguments = {
    "intent_id": "exe04-restart-business-id",
    "signal_id": "signal-exe04-restart",
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


class Provider:
    def submit_authorized(self, intent, authorization):
        assert authorization.intent_fingerprint == intent.fingerprint
        assert authorization.parameters.intent_fingerprint == intent.fingerprint
        assert authorization.parameters.provider_instrument == "BTCUSDT"
        connection = sqlite3.connect(journal_path, isolation_level=None)
        try:
            connection.execute(
                "INSERT INTO provider_calls (intent_id) VALUES (?)", (intent.intent_id,)
            )
        finally:
            connection.close()
        if mode == "crash_unknown":
            os._exit(82)
        return ProviderObservation.accepted(intent.intent_id, "synthetic-order")

    def submit(self, intent):
        raise AssertionError("basic admission must use authorized dispatch")

    def cancel(self, intent):
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )


provider = Provider()
store = SqliteExecutionStore(database_path)
validator = ExecutionProviderValidatorV1(
    provider_id="PAPER",
    validator_id="paper-order-policy",
    version="v1",
    dispatch_port=provider,
    instrument_map={"BTC-USDT": "BTCUSDT"},
    quantity_steps={"BTC-USDT": __import__("decimal").Decimal("1")},
    max_quantities={"BTC-USDT": __import__("decimal").Decimal("100")},
    price_ticks={"BTC-USDT": __import__("decimal").Decimal("0.5")},
    supported_order_types=(OrderType.LIMIT,),
    supported_time_in_force=(TimeInForce.GTC,),
    supported_actions=((Side.BUY, PositionEffect.OPEN),),
    supports_reduce_only=False,
)
gate = ExecutionBasicAdmissionGate(
    store,
    ExecutionAdmissionValidatorRegistry((validator,)),
    permit_ttl_ns=5_000_000_000,
)
service = ExecutionOrderEntryService(
    store,
    {wire_scope: execution_scope},
    provider,
    basic_admission_gate=gate,
    writer_id="exe04-process-writer",
    lease_ttl_ns=1_000_000_000,
)


def submit():
    return service.handle(
        SUBMIT_OPERATION,
        principal_ref="exe04-principal",
        identity=identity,
        scope=wire_scope,
        arguments=arguments,
    )


if mode == "crash_preclaim":
    original = SqliteExecutionStore.activate_intent

    def exit_after_activation(self, *args, **kwargs):
        record = original(self, *args, **kwargs)
        if record.state is ExecutionState.PENDING_DISPATCH:
            os._exit(81)
        return record

    SqliteExecutionStore.activate_intent = exit_after_activation
    reply = submit()
    record = store.get("exe04-restart-business-id", scope=execution_scope)
    raise SystemExit(
        f"preclaim crash point was not reached: reply={reply!r} "
        f"state={None if record is None else record.state.value}"
    )

if mode == "restart_preclaim":
    before = store.get("exe04-restart-business-id", scope=execution_scope)
    recovered = service.recover_interrupted_dispatches()
    reply = submit()
    after = store.get("exe04-restart-business-id", scope=execution_scope)
    events = store.read_outbox(scope=execution_scope, after_sequence=0, limit=1000)
    lease_token = store._connection.execute(
        "SELECT fencing_token FROM execution_writer_leases WHERE scope_key = ?",
        (execution_scope.account_key,),
    ).fetchone()[0]
    print(json.dumps({
        "before": before.state.value,
        "recovered": [record.state.value for record in recovered],
        "reply": reply["result"]["record"]["state"],
        "after": after.state.value,
        "dispatch_attempts": after.dispatch_attempts,
        "fencing_token": lease_token,
        "permit_replaced": before.basic_admission_reference != after.basic_admission_reference,
        "admission_events": sum(event.event_type == "pending_admission_reference_reauthorized" for event in events),
    }, sort_keys=True))
    store.close()
    raise SystemExit(0)

if mode == "crash_unknown":
    reply = submit()
    record = store.get("exe04-restart-business-id", scope=execution_scope)
    raise SystemExit(
        f"provider crash point was not reached: reply={reply!r} "
        f"state={None if record is None else record.state.value}"
    )

if mode == "restart_unknown":
    before = store.get("exe04-restart-business-id", scope=execution_scope)
    recovered = service.recover_interrupted_dispatches()
    reply = submit()
    after = store.get("exe04-restart-business-id", scope=execution_scope)
    events = store.read_outbox(scope=execution_scope, after_sequence=0, limit=1000)
    lease_token = store._connection.execute(
        "SELECT fencing_token FROM execution_writer_leases WHERE scope_key = ?",
        (execution_scope.account_key,),
    ).fetchone()[0]
    print(json.dumps({
        "before": before.state.value,
        "recovered": [record.state.value for record in recovered],
        "reply": reply["result"]["record"]["state"],
        "after": after.state.value,
        "dispatch_attempts": after.dispatch_attempts,
        "fencing_token": lease_token,
        "unknown_events": sum(event.event_type == "dispatch_unknown" for event in events),
    }, sort_keys=True))
    store.close()
    raise SystemExit(0)

raise SystemExit("unknown child mode")
"""


class _Clock:
    def __init__(self, now_ns: int = 1_000_000_000) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
        return self.now_ns


class _AuthorizedProvider:
    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.authorizations = []
        self.unauthorized_calls: list[str] = []

    def submit_authorized(self, intent: OrderIntent, authorization):
        self.submitted.append(intent.intent_id)
        self.authorizations.append(authorization)
        return ProviderObservation.accepted(intent.intent_id, f"provider-{intent.intent_id}")

    def submit(self, intent: OrderIntent):
        self.unauthorized_calls.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, f"provider-{intent.intent_id}")

    def cancel(self, intent):
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )


def _arguments(intent_id: str = "exe04-basic-order") -> dict[str, object]:
    return {
        "intent_id": intent_id,
        "signal_id": f"signal-{intent_id}",
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


def _validator(provider: _AuthorizedProvider, **overrides) -> ExecutionProviderValidatorV1:
    values = {
        "provider_id": "PAPER",
        "validator_id": "paper-order-policy",
        "version": "v1",
        "dispatch_port": provider,
        "instrument_map": {"BTC-USDT": "BTCUSDT"},
        "quantity_steps": {"BTC-USDT": Decimal("1")},
        "max_quantities": {"BTC-USDT": Decimal("100")},
        "price_ticks": {"BTC-USDT": Decimal("0.5")},
        "supported_order_types": (OrderType.LIMIT,),
        "supported_time_in_force": (TimeInForce.GTC,),
        "supported_actions": ((Side.BUY, PositionEffect.OPEN),),
        "supports_reduce_only": False,
    }
    values.update(overrides)
    return ExecutionProviderValidatorV1(**values)


def _service(
    store: SqliteExecutionStore,
    provider: _AuthorizedProvider,
    *,
    clock: _Clock | None = None,
    ttl_ns: int = 5_000_000_000,
    writer_id: str = "exe04-basic-writer",
    lease_ttl_ns: int = 5_000_000_000,
    validator: ExecutionProviderValidatorV1 | None = None,
    admission_gate: object | None = None,
    wire_scope: WireScope = _WIRE_SCOPE,
    execution_scope: ExecutionScope = _EXECUTION_SCOPE,
) -> tuple[ExecutionOrderEntryService, ExecutionBasicAdmissionGate]:
    gate = ExecutionBasicAdmissionGate(
        store,
        ExecutionAdmissionValidatorRegistry((validator or _validator(provider),)),
        permit_ttl_ns=ttl_ns,
        clock_ns=(clock or _Clock()),
    )
    service = ExecutionOrderEntryService(
        store,
        {wire_scope: execution_scope},
        provider,
        admission_gate=admission_gate,
        basic_admission_gate=gate,
        writer_id=writer_id,
        lease_ttl_ns=lease_ttl_ns,
    )
    return service, gate


def _submit(
    service: ExecutionOrderEntryService,
    arguments: dict[str, object],
    *,
    identity: WireIdentity = _IDENTITY,
    wire_scope: WireScope = _WIRE_SCOPE,
) -> dict[str, object]:
    return service.handle(
        SUBMIT_OPERATION,
        principal_ref=identity.principal_ref,
        identity=identity,
        scope=wire_scope,
        arguments=arguments,
    )


@pytest.mark.unit
def test_execution_basic_admission_uses_trusted_context_policy_and_authorized_port(
    tmp_path,
) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _AuthorizedProvider()
    service, _gate = _service(store, provider)
    try:
        request = _arguments()
        request["basic_admission_reference"] = "0" * 32
        request["admission_context_sha256"] = "f" * 64
        reply = _submit(service, request)
        record = store.get("exe04-basic-order", scope=_EXECUTION_SCOPE)

        assert reply["ok"] is True and reply["code"] == "OK"
        assert record is not None and record.state is ExecutionState.ACKED
        assert record.basic_admission_reference is not None
        assert len(record.basic_admission_reference) == 32
        assert record.basic_admission_reference != request["basic_admission_reference"]
        assert (
            record.admission_context_sha256
            == ExecutionAdmissionContextV1(
                _IDENTITY.principal_ref, _IDENTITY, _WIRE_SCOPE, _EXECUTION_SCOPE, SUBMIT_OPERATION
            ).fingerprint
        )
        assert record.permit_reference is None
        assert provider.submitted == ["exe04-basic-order"]
        assert provider.unauthorized_calls == []
        authorization = provider.authorizations[0]
        assert authorization.fencing_token == 1
        assert authorization.owner_id == "exe04-basic-writer"
        assert authorization.permit_reference == record.basic_admission_reference
        assert authorization.intent_fingerprint == record.payload_sha256
        assert authorization.parameters.account_ref == "account-a"
        assert authorization.parameters.strategy_id == "strategy-a"
        assert authorization.parameters.provider_instrument == "BTCUSDT"
        assert authorization.parameters.quantity == Decimal("2")
        assert authorization.parameters.price == Decimal("50000")
    finally:
        store.close()


@pytest.mark.unit
def test_basic_admission_composes_with_existing_risk_gate(tmp_path) -> None:
    class RiskGate:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def reserve(self, intent: OrderIntent):
            self.calls.append(("reserve", intent.intent_id))
            return {"permit_id": f"risk-{intent.intent_id}"}

        def claim_for_dispatch(self, reference: str, intent: OrderIntent):
            self.calls.append(("claim", reference))
            return reference

        def settle(self, reference: str):
            self.calls.append(("settle", reference))

        def release(self, reference: str, reason: str):
            self.calls.append(("release", reference))

    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _AuthorizedProvider()
    risk = RiskGate()
    service, _gate = _service(store, provider, admission_gate=risk)
    try:
        reply = _submit(service, _arguments())
        record = store.get("exe04-basic-order", scope=_EXECUTION_SCOPE)
        assert reply["ok"] is True
        assert record is not None and record.state is ExecutionState.ACKED
        assert record.basic_admission_reference is not None
        assert record.permit_reference == "risk-exe04-basic-order"
        assert provider.submitted == ["exe04-basic-order"]
        assert risk.calls == [
            ("reserve", "exe04-basic-order"),
            ("claim", "risk-exe04-basic-order"),
            ("settle", "risk-exe04-basic-order"),
        ]
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("failure", "arguments"),
    [
        ("action", {"side": "SELL"}),
        ("quantity_step", {"quantity": "1.5"}),
        ("quantity_maximum", {"quantity": "101"}),
        ("price_tick", {"price": "50000.1"}),
        ("unknown_instrument", {"instrument": "ETH-USDT"}),
        ("missing_validator", {}),
    ],
)
def test_registered_provider_policy_denials_never_reach_provider(
    tmp_path, failure: str, arguments: dict[str, str]
) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _AuthorizedProvider()
    if failure == "missing_validator":
        validator = _validator(provider, provider_id="OTHER")
    else:
        validator = _validator(provider)
    service, _gate = _service(store, provider, validator=validator)
    request = _arguments()
    request.update(arguments)
    try:
        reply = _submit(service, request)
        record = store.get("exe04-basic-order", scope=_EXECUTION_SCOPE)

        assert reply["ok"] is True
        assert reply["result"]["record"]["state"] == ExecutionState.BLOCKED.value
        assert record is not None and record.state is ExecutionState.BLOCKED
        assert provider.submitted == []
        assert provider.unauthorized_calls == []
    finally:
        store.close()


@pytest.mark.unit
def test_wrong_account_context_cannot_claim_an_existing_pending_permit(
    tmp_path, monkeypatch
) -> None:
    from bt_api_execution.basic_admission import ExecutionBasicAdmissionGate

    original_claim = ExecutionBasicAdmissionGate.claim_for_dispatch
    other_scope = ExecutionScope("PAPER", "simulation", "account-b", "strategy-a", "20261015")
    other_wire = WireScope("paper:exe04", account_ref="account-b", strategy_ref="strategy-a")
    other_context = ExecutionAdmissionContextV1(
        _IDENTITY.principal_ref, _IDENTITY, other_wire, other_scope, SUBMIT_OPERATION
    )

    def claim_with_wrong_account(self, reference, intent, context, dispatch_port, lease):
        return original_claim(self, reference, intent, other_context, dispatch_port, lease)

    monkeypatch.setattr(ExecutionBasicAdmissionGate, "claim_for_dispatch", claim_with_wrong_account)
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _AuthorizedProvider()
    service, _gate = _service(store, provider)
    try:
        reply = _submit(service, _arguments())
        record = store.get("exe04-basic-order", scope=_EXECUTION_SCOPE)

        assert reply["result"]["record"]["state"] == ExecutionState.BLOCKED.value
        assert record is not None and record.state is ExecutionState.BLOCKED
        assert record.dispatch_attempts == 1
        assert provider.submitted == []
        assert provider.unauthorized_calls == []
    finally:
        store.close()


@pytest.mark.unit
def test_expiry_is_checked_at_final_claim_and_blocks_provider(tmp_path, monkeypatch) -> None:

    clock = _Clock()
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _AuthorizedProvider()
    service, _gate = _service(store, provider, clock=clock, ttl_ns=100_000_000)
    original_claim = store.claim_for_dispatch

    def claim_then_expire(intent_id, scope, *, writer_lease=None):
        result = original_claim(intent_id, scope, writer_lease=writer_lease)
        if result[1]:
            clock.now_ns += 100_000_000
        return result

    monkeypatch.setattr(store, "claim_for_dispatch", claim_then_expire)
    try:
        reply = _submit(service, _arguments())
        record = store.get("exe04-basic-order", scope=_EXECUTION_SCOPE)
        assert reply["result"]["record"]["state"] == ExecutionState.BLOCKED.value
        assert record is not None and record.state is ExecutionState.BLOCKED
        assert provider.submitted == []
        assert provider.unauthorized_calls == []
    finally:
        store.close()


@pytest.mark.unit
def test_expiry_after_pre_dispatch_guard_blocks_provider(tmp_path) -> None:
    from bt_api_execution.facade import ManagedExecutionFacade

    clock = _Clock()
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _AuthorizedProvider()
    _service_for_gate, gate = _service(store, provider, clock=clock, ttl_ns=100_000_000)
    request = _arguments("exe04-guard-expiry")
    request["scope"] = _EXECUTION_SCOPE.to_payload()
    intent = order_intent_from_payload(request)
    context = ExecutionAdmissionContextV1(
        _IDENTITY.principal_ref,
        _IDENTITY,
        _WIRE_SCOPE,
        _EXECUTION_SCOPE,
        SUBMIT_OPERATION,
    )

    def expire_during_guard(_intent: OrderIntent) -> None:
        clock.now_ns += 100_000_000

    facade = ManagedExecutionFacade(
        store,
        _EXECUTION_SCOPE,
        writer_id="exe04-basic-writer",
        basic_admission_gate=gate,
        lease_ttl_ns=5_000_000_000,
        pre_dispatch_guard=expire_during_guard,
    )
    try:
        record = facade.submit(intent, provider, admission_context=context)
        assert record.state is ExecutionState.BLOCKED
        assert record.dispatch_attempts == 1
        assert provider.submitted == []
        assert provider.unauthorized_calls == []
    finally:
        store.close()


@pytest.mark.unit
def test_stale_writer_generation_fails_final_claim_before_provider(tmp_path, monkeypatch) -> None:
    from bt_api_execution import store as store_module

    clock = _Clock()
    monkeypatch.setattr(store_module.time, "time_ns", clock)
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _AuthorizedProvider()
    service, _gate = _service(
        store,
        provider,
        clock=clock,
        writer_id="exe04-old-owner",
        lease_ttl_ns=5_000_000,
    )
    original_claim = store.claim_for_dispatch
    new_leases = []

    def claim_then_handoff(intent_id, scope, *, writer_lease=None):
        result = original_claim(intent_id, scope, writer_lease=writer_lease)
        if result[1]:
            clock.now_ns += 6_000_000
            new_leases.append(store.acquire_or_renew_lease(scope, "exe04-new-owner"))
        return result

    monkeypatch.setattr(store, "claim_for_dispatch", claim_then_handoff)
    try:
        reply = _submit(service, _arguments())
        record = store.get("exe04-basic-order", scope=_EXECUTION_SCOPE)
        assert new_leases and new_leases[0].fencing_token == 2
        assert reply["code"] == "OWNER_LEASE_UNAVAILABLE"
        assert record is not None and record.state is ExecutionState.DISPATCHING
        assert record.dispatch_attempts == 1
        assert provider.submitted == []
        assert provider.unauthorized_calls == []

        restarted, _new_gate = _service(store, provider, clock=clock, writer_id="exe04-new-owner")
        recovered = restarted.recover_interrupted_dispatches()
        repeated = _submit(restarted, _arguments())
        final = store.get("exe04-basic-order", scope=_EXECUTION_SCOPE)
        assert [item.state for item in recovered] == [ExecutionState.UNKNOWN]
        assert repeated["result"]["record"]["state"] == ExecutionState.UNKNOWN.value
        assert final is not None and final.state is ExecutionState.UNKNOWN
        assert provider.submitted == []
    finally:
        store.close()


@pytest.mark.unit
def test_cold_basic_admission_path_imports_no_risk_or_gateway(tmp_path) -> None:
    source = r"""
import importlib.abc
import sys

class RejectOptionalPackage(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == name or fullname.startswith(name + '.') for name in ('bt_api_risk', 'bt_api_gateway')):
            raise AssertionError('execution basic admission imported an optional package')
        return None

sys.meta_path.insert(0, RejectOptionalPackage())
sys.path[:0] = sys.argv[1:3]
from bt_api_execution.basic_admission import ExecutionBasicAdmissionGate
from bt_api_execution.service_order_entry import ExecutionOrderEntryService
assert not any(
    name == 'bt_api_risk' or name.startswith('bt_api_risk.')
    or name == 'bt_api_gateway' or name.startswith('bt_api_gateway.')
    for name in sys.modules
)
"""
    result = subprocess.run(  # noqa: S603 - fixed interpreter and package source roots
        [
            sys.executable,
            "-c",
            source,
            str(_EXECUTION_SRC),
            str(_BASE_SRC),
        ],
        cwd=_PACKAGE_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.unit
def test_schema_v20_migration_adds_nullable_basic_admission_binding_columns(tmp_path) -> None:
    path = tmp_path / "execution-v20.sqlite3"
    store = SqliteExecutionStore(path)
    lease = store.acquire_or_renew_lease(_EXECUTION_SCOPE, "exe04-migration-writer")
    payload = _arguments("exe04-migration-order")
    payload["scope"] = _EXECUTION_SCOPE.to_payload()
    intent = order_intent_from_payload(payload)
    store.admit_intent(intent, writer_lease=lease)
    store.activate_intent(
        intent.intent_id,
        _EXECUTION_SCOPE,
        "risk-permit-migration",
        writer_lease=lease,
    )
    store.close()

    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute("ALTER TABLE execution_records DROP COLUMN basic_admission_reference")
        connection.execute("ALTER TABLE execution_records DROP COLUMN admission_context_sha256")
        connection.execute("UPDATE execution_meta SET value = '20' WHERE key = 'schema_version'")
        connection.execute(
            """
            INSERT INTO ctp_dispatch_commands(
                account_key, scope_key, trading_day, operation, command_id,
                request_payload_json, request_payload_sha256,
                reservation_managed_intent_id, order_ref, approval_use_id,
                approval_digest, session_binding_json, session_binding_sha256,
                status, created_at_ns, updated_at_ns, claimed_at_ns
            ) VALUES (
                'account:upgrade', 'scope:upgrade', '20261015', 'SUBMIT',
                'upgrade-claimed', '{}', ?, 'managed-upgrade', '000000000001',
                'approval-upgrade', ?, '{}', ?, 'CLAIMED', 1, 2, 2
            )
            """,
            ("a" * 64, "b" * 64, "c" * 64),
        )
    finally:
        connection.close()

    migrated = SqliteExecutionStore(path)
    try:
        record = migrated.get(intent.intent_id, scope=_EXECUTION_SCOPE)
        version = migrated._connection.execute(
            "SELECT value FROM execution_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        columns = {
            row["name"]
            for row in migrated._connection.execute(
                "PRAGMA table_info(execution_records)"
            ).fetchall()
        }
        assert version == "21"
        assert {"basic_admission_reference", "admission_context_sha256"}.issubset(columns)
        assert record is not None and record.state is ExecutionState.PENDING_DISPATCH
        assert record.permit_reference == "risk-permit-migration"
        assert record.basic_admission_reference is None
        assert record.admission_context_sha256 is None
        command = migrated._connection.execute(
            "SELECT status FROM ctp_dispatch_commands WHERE command_id = ?",
            ("upgrade-claimed",),
        ).fetchone()
        fence_count = migrated._connection.execute(
            "SELECT COUNT(*) FROM ctp_account_family_legacy_fences"
        ).fetchone()[0]
        assert command["status"] == "CLAIMED"
        assert fence_count == 0
    finally:
        migrated.close()


def _prepare_journal(path: Path) -> None:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
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


def _run_child(mode: str, database: Path, journal: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed child source and pytest-owned SQLite paths
        [
            sys.executable,
            "-c",
            _CRASH_CHILD,
            mode,
            str(database),
            str(journal),
            str(_EXECUTION_SRC),
            str(_BASE_SRC),
        ],
        cwd=_PACKAGE_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("crash_mode", "restart_mode", "exit_code", "initial_state", "final_state", "calls"),
    [
        (
            "crash_preclaim",
            "restart_preclaim",
            81,
            ExecutionState.PENDING_DISPATCH.value,
            ExecutionState.ACKED.value,
            1,
        ),
        (
            "crash_unknown",
            "restart_unknown",
            82,
            ExecutionState.DISPATCHING.value,
            ExecutionState.UNKNOWN.value,
            1,
        ),
    ],
)
def test_basic_admission_restart_reauthorizes_only_unclaimed_pending_rows(
    tmp_path: Path,
    crash_mode: str,
    restart_mode: str,
    exit_code: int,
    initial_state: str,
    final_state: str,
    calls: int,
) -> None:
    database = tmp_path / f"{crash_mode}.sqlite3"
    journal = tmp_path / f"{crash_mode}-provider.sqlite3"
    _prepare_journal(journal)

    crashed = _run_child(crash_mode, database, journal)
    assert crashed.returncode == exit_code, crashed.stderr
    assert crashed.stdout == ""
    assert _provider_call_count(journal) == (1 if crash_mode == "crash_unknown" else 0)

    # The writer lease expires before a separate process starts, so the new
    # child receives the next fencing token rather than inheriting a live lease.
    time.sleep(1.1)
    restarted = _run_child(restart_mode, database, journal)
    assert restarted.returncode == 0, restarted.stderr
    result = json.loads(restarted.stdout)
    assert result["before"] == initial_state
    assert result["fencing_token"] == 2
    assert result["after"] == final_state
    assert _provider_call_count(journal) == calls

    if restart_mode == "restart_preclaim":
        assert result["recovered"] == []
        assert result["reply"] == ExecutionState.ACKED.value
        assert result["permit_replaced"] is True
        assert result["admission_events"] == 1
        assert result["dispatch_attempts"] == 1
    else:
        assert result["recovered"] == [ExecutionState.UNKNOWN.value]
        assert result["reply"] == ExecutionState.UNKNOWN.value
        assert result["unknown_events"] == 1
        assert result["dispatch_attempts"] == 1
