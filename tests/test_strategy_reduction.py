"""Contract tests for one approved, strategy-scoped CLOSE target."""

from __future__ import annotations

import json
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    ApprovedTargetAuthorityV1,
    ExecutionAdmissionValidatorRegistry,
    ExecutionBasicAdmissionGate,
    ExecutionProviderValidatorV1,
    ExecutionState,
    OrderType,
    PositionEffect,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
    TimeInForce,
)
from bt_api_execution.approved_target import (
    ApprovedTargetOwnerBindingsV1,
    ApprovedTargetOwnerBindingV1,
    ProtectedApprovedTargetScopesV1,
)
from bt_api_execution.contracts import ExecutionScope
from bt_api_execution.errors import ContractValidationError
from bt_api_execution.service_order_entry import ExecutionOrderEntryService
from bt_api_execution.strategy_reduction import (
    OwnerStrategyReductionRequestV1,
    StrategyReductionHandlerV1,
    StrategyReductionStage,
    StrategyReductionStatus,
)

_PRINCIPAL = "afi-reduction-principal"
_IDENTITY = WireIdentity(WireRole.SERVICE, _PRINCIPAL, "owner-actor", "owner-delegate")
_WIRE_SCOPE = WireScope(
    "paper-domain",
    account_ref="account-a",
    strategy_ref="strategy-a",
    run_id="run-a",
    portfolio_ref="portfolio-a",
)
_EXECUTION_SCOPE = ExecutionScope(
    "PAPER", "simulation", "account-a", "strategy-a", "20261017"
)


def _target(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_id": "bt_api.approved_order_target",
        "schema_version": 1,
        "target_ref": "target-reduce-a",
        "target_version": 1,
        "owner_subject": "owner-a",
        "principal_ref": _PRINCIPAL,
        "actor_ref": "owner-actor",
        "delegate_ref": "owner-delegate",
        "scope_ref": "scope-a",
        "wire_scope": {
            "execution_domain_ref": "paper-domain",
            "account_ref": "account-a",
            "account_refs": [],
            "strategy_ref": "strategy-a",
            "run_id": "run-a",
            "manual_book_ref": None,
            "portfolio_ref": "portfolio-a",
            "group_id": None,
            "leg_id": None,
        },
        "strategy_version": 3,
        "membership_version": 4,
        "membership_sha256": "2" * 64,
        "instrument": "BTC-USDT",
        "metadata_version": "instrument-v1",
        "side": "SELL",
        "position_effect": "CLOSE",
        "order_type": "LIMIT",
        "quantity": "1.25",
        "price": "10.5",
        "time_in_force": "GTC",
        "reduce_only": True,
        "deadline_ns": None,
        "limits": {
            "max_quantity": "2",
            "max_notional": "20",
            "min_price": "5",
            "max_price": "15",
        },
        "policy_ref": "policy-a",
        "policy_version": 5,
        "policy_sha256": "3" * 64,
    }
    value.update(changes)
    return value


def _request(target: dict[str, object], *, intent_id: str = "reduction-intent-a"):
    target_hash = sha256(
        json.dumps(
            target, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
        ).encode("utf-8")
    ).hexdigest()
    return OwnerStrategyReductionRequestV1(
        approval_ref="approval-a",
        approval_version=7,
        target_sha256=target_hash,
        intent_id=intent_id,
    )


class _AuthorityPort:
    def __init__(self, target: dict[str, object]) -> None:
        self.target = target
        self.calls: list[str] = []

    def reserve_approved_target_operation(self, **arguments: object) -> object:
        self.calls.append("reserve")
        return {
            "approval_ref": arguments["approval_ref"],
            "approval_version": arguments["approval_version"],
            "target_sha256": arguments["target_sha256"],
            "intent_id": arguments["intent_id"],
            "operation_request_sha256": "4" * 64,
            "created": True,
            "record_version": 2,
        }

    def resolve_approved_target_v1(self, **arguments: object) -> object:
        self.calls.append("resolve")
        return deepcopy(self.target)

    def consume_approved_target_v1(self, **arguments: object) -> object:
        self.calls.append("consume")
        return {
            "disposition": "CONSUMED_NEW",
            "consumption_ref": "consume-reduction-a",
            "approval_ref": arguments["approval_ref"],
            "approval_version": arguments["approval_version"],
            "consumed_record_version": 4,
            "target_ref": "target-reduce-a",
            "target_version": 1,
            "target_sha256": arguments["target_sha256"],
            "intent_id": arguments["intent_id"],
            "intent_sha256": arguments["intent_sha256"],
            "consumed_at_ns": 123456789,
        }


class _RiskGate:
    def __init__(self) -> None:
        self.reserved = []
        self.claimed = []
        self.settled = []
        self.released = []

    def reserve(self, intent):
        self.reserved.append(intent)
        return {"permit_id": "risk-" + intent.intent_id}

    def claim_for_dispatch(self, reference, intent):
        self.claimed.append((reference, intent))
        return reference

    def settle(self, reference):
        self.settled.append(reference)

    def release(self, reference, reason):
        self.released.append((reference, reason))


class _Provider:
    def __init__(self, store: SqliteExecutionStore, result: str) -> None:
        self.store = store
        self.result = result
        self.submitted: list[str] = []
        self.unauthorized: list[str] = []

    def submit_authorized(self, intent, authorization):
        record = self.store.get(intent.intent_id, scope=intent.scope)
        assert record is not None and record.state is ExecutionState.DISPATCHING
        assert authorization.intent_fingerprint == intent.fingerprint
        self.submitted.append(intent.intent_id)
        if self.result == "unknown":
            raise TimeoutError("synthetic provider reply was lost")
        if self.result == "partial":
            return ProviderObservation(
                intent.intent_id,
                ExecutionState.PARTIALLY_FILLED,
                provider_order_id="provider-order-a",
                filled_quantity=Decimal("0.25"),
                average_price=Decimal("10.5"),
            )
        if self.result == "rejected":
            return ProviderObservation.rejected(intent.intent_id, "synthetic_reject")
        return ProviderObservation.accepted(intent.intent_id, "provider-order-a")

    def submit(self, intent):
        self.unauthorized.append(intent.intent_id)
        raise AssertionError("approved reduction must use authorized dispatch")

    def cancel(self, intent):
        raise AssertionError("cancellation is outside this reduction contract")


def _service(tmp_path, target: dict[str, object], *, provider_result: str = "accepted"):
    store = SqliteExecutionStore(tmp_path / "reduction-execution.sqlite3")
    risk = _RiskGate()
    provider = _Provider(store, provider_result)
    validator = ExecutionProviderValidatorV1(
        provider_id="PAPER",
        validator_id="strategy-reduction-policy",
        version="policy-v1",
        dispatch_port=provider,
        instrument_map={"BTC-USDT": "BTCUSDT"},
        quantity_steps={"BTC-USDT": Decimal("0.25")},
        max_quantities={"BTC-USDT": Decimal("100")},
        price_ticks={"BTC-USDT": Decimal("0.5")},
        supported_order_types=(OrderType.LIMIT,),
        supported_time_in_force=(TimeInForce.GTC,),
        supported_actions=((Side.SELL, PositionEffect.CLOSE),),
        supports_reduce_only=True,
    )
    basic = ExecutionBasicAdmissionGate(
        store, ExecutionAdmissionValidatorRegistry((validator,))
    )
    port = _AuthorityPort(target)
    authority = ApprovedTargetAuthorityV1(
        port,
        ApprovedTargetOwnerBindingsV1(
            (
                ApprovedTargetOwnerBindingV1(
                    principal_ref=_PRINCIPAL,
                    identity=_IDENTITY,
                    wire_scope=_WIRE_SCOPE,
                    owner_subject="owner-a",
                    expected_scope_ref="scope-a",
                ),
            )
        ),
        ProtectedApprovedTargetScopesV1((_WIRE_SCOPE,)),
    )
    service = ExecutionOrderEntryService(
        store,
        {_WIRE_SCOPE: _EXECUTION_SCOPE},
        provider,
        admission_gate=risk,
        basic_admission_gate=basic,
        approved_target_authority=authority,
        writer_id="strategy-reduction-writer",
    )
    return store, service, provider, risk, port, StrategyReductionHandlerV1(service)


def _run(handler, target: dict[str, object], **changes):
    request = _request(target)
    identity = changes.pop("identity", _IDENTITY)
    scope = changes.pop("scope", _WIRE_SCOPE)
    arguments = changes.pop("arguments", request.to_payload())
    assert not changes
    return handler.handle(
        principal_ref=identity.principal_ref,
        identity=identity,
        scope=scope,
        arguments=arguments,
    )


def test_reduction_uses_exact_approved_close_target_and_existing_risk_entry(tmp_path) -> None:
    target = _target()
    store, service, provider, risk, authority, handler = _service(tmp_path, target)
    try:
        outcome = _run(handler, target)

        assert outcome.status is StrategyReductionStatus.PENDING
        assert outcome.execution_state is ExecutionState.ACKED
        assert outcome.filled_quantity == "0"
        assert outcome.stage is StrategyReductionStage.EXECUTION
        assert outcome.is_terminal is False
        assert provider.submitted == ["reduction-intent-a"]
        assert provider.unauthorized == []
        assert len(risk.reserved) == 1
        intent = risk.reserved[0]
        assert intent.position_effect is PositionEffect.CLOSE
        assert intent.reduce_only is True
        assert intent.position_side == "NET" and intent.position_bucket == "ALL"
        assert intent.instrument == "BTC-USDT" and intent.quantity == Decimal("1.25")
        assert intent.scope is _EXECUTION_SCOPE
        assert authority.calls == ["reserve", "resolve", "reserve", "resolve", "consume"]
        assert risk.settled == ["risk-reduction-intent-a"]
    finally:
        store.close()


@pytest.mark.parametrize(
    ("changes", "expected_reason"),
    [
        ({"position_effect": "OPEN", "reduce_only": False}, "TARGET_NOT_REDUCTION"),
        ({"position_effect": "CLOSE_TODAY"}, "TARGET_NOT_REDUCTION"),
        ({"position_effect": "CLOSE_YESTERDAY"}, "TARGET_NOT_REDUCTION"),
    ],
)
def test_reduction_rejects_unapproved_action_or_nondefault_close_dimensions(
    tmp_path, changes, expected_reason
) -> None:
    target = _target(**changes)
    store, entry_service, provider, risk, authority, handler = _service(tmp_path, target)
    try:
        outcome = _run(handler, target)
        assert outcome.status is StrategyReductionStatus.NOT_DISPATCHED
        assert outcome.reason_code == expected_reason
        assert outcome.execution_state is None
        assert provider.submitted == []
        assert authority.calls == ["reserve", "resolve"]
        assert risk.reserved == []
    finally:
        store.close()


def test_reduction_rejects_scope_identity_and_request_overrides(tmp_path) -> None:
    target = _target()
    store, entry_service, provider, risk, authority, handler = _service(tmp_path, target)
    try:
        other_identity = WireIdentity(
            WireRole.SERVICE, _PRINCIPAL, "other-actor", "owner-delegate"
        )
        wrong_run = WireScope(
            "paper-domain",
            account_ref="account-a",
            strategy_ref="strategy-a",
            run_id="run-b",
            portfolio_ref="portfolio-a",
        )
        forged_arguments = {
            **_request(target).to_payload(),
            "quantity": "100",
            "position_side": "SHORT",
        }
        for identity, scope, arguments in (
            (other_identity, _WIRE_SCOPE, _request(target).to_payload()),
            (_IDENTITY, wrong_run, _request(target).to_payload()),
            (_IDENTITY, _WIRE_SCOPE, forged_arguments),
        ):
            outcome = handler.handle(
                principal_ref=identity.principal_ref,
                identity=identity,
                scope=scope,
                arguments=arguments,
            )
            assert outcome.status is StrategyReductionStatus.NOT_DISPATCHED
        assert provider.submitted == []
        assert authority.calls == []
        assert risk.reserved == []
    finally:
        store.close()


def test_partial_fill_is_projected_as_partial_not_complete_reduction(tmp_path) -> None:
    target = _target()
    store, entry_service, provider, risk, authority, handler = _service(
        tmp_path, target, provider_result="partial"
    )
    try:
        outcome = _run(handler, target)
        assert outcome.status is StrategyReductionStatus.PARTIAL
        assert outcome.execution_state is ExecutionState.PARTIALLY_FILLED
        assert outcome.filled_quantity == "0.25"
        assert outcome.is_terminal is False
        assert provider.submitted == ["reduction-intent-a"]
        assert len(risk.reserved) == 1
        assert authority.calls[-1] == "consume"
    finally:
        store.close()


def test_unknown_is_review_required_and_exact_replay_never_dispatches_twice(tmp_path) -> None:
    target = _target()
    store, entry_service, provider, risk, authority, handler = _service(
        tmp_path, target, provider_result="unknown"
    )
    try:
        first = _run(handler, target)
        second = _run(handler, target)
        record = store.get("reduction-intent-a", scope=_EXECUTION_SCOPE)
        assert first.status is StrategyReductionStatus.UNKNOWN
        assert first.execution_state is ExecutionState.UNKNOWN
        assert first.review_required is True
        assert second.status is StrategyReductionStatus.UNKNOWN
        assert record is not None and record.state is ExecutionState.UNKNOWN
        assert provider.submitted == ["reduction-intent-a"]
        assert len(risk.reserved) == 1
        assert risk.released == []
        assert authority.calls.count("consume") == 1
    finally:
        store.close()


def test_execution_definitive_no_record_denial_keeps_execution_stage(tmp_path) -> None:
    target = _target()
    store, entry_service, provider, risk, authority, handler = _service(tmp_path, target)
    entry_service.handle = lambda *args, **kwargs: {
        "ok": False,
        "code": "SCOPE_DENIED",
        "result": {"found": False, "record": None},
    }
    try:
        outcome = _run(handler, target)
        assert outcome.status is StrategyReductionStatus.NOT_DISPATCHED
        assert outcome.stage is StrategyReductionStage.EXECUTION
        assert outcome.reason_code == "SCOPE_DENIED"
        assert provider.submitted == []
        assert risk.reserved == []
        assert authority.calls == ["reserve", "resolve"]
    finally:
        store.close()


def test_handler_requires_canonical_entry_service_and_authority() -> None:
    with pytest.raises(ContractValidationError):
        StrategyReductionHandlerV1(object())  # type: ignore[arg-type]


def test_versioned_request_rejects_trade_terms_and_unknown_fields() -> None:
    target = _target()
    payload = _request(target).to_payload()
    assert OwnerStrategyReductionRequestV1.from_arguments(payload).intent_id == "reduction-intent-a"
    with pytest.raises(ContractValidationError):
        OwnerStrategyReductionRequestV1.from_arguments({**payload, "side": "SELL"})
