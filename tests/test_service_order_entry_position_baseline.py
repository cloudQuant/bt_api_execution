"""Approved-target dispatch must preserve the OPEN position baseline gate."""

from __future__ import annotations

import json
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    ApprovedTargetAuthorityV1,
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
from bt_api_execution.approved_target import (
    ApprovedTargetOwnerBindingV1,
    ApprovedTargetOwnerBindingsV1,
    ApprovedTargetSubmitRequestV1,
    ProtectedApprovedTargetScopesV1,
)
from bt_api_execution.service_order_entry import (
    APPROVED_TARGET_OPERATION,
    ExecutionOrderEntryService,
)

_PRINCIPAL = "baseline-approved-principal"
_IDENTITY = WireIdentity(
    WireRole.APPLICATION_CLIENT, _PRINCIPAL, "actor-a", "delegate-a"
)
_WIRE_SCOPE = WireScope(
    "paper-domain",
    account_ref="account-a",
    strategy_ref="strategy-a",
    portfolio_ref="portfolio-a",
)
_EXECUTION_SCOPE = ExecutionScope(
    "PAPER", "simulation", "account-a", "strategy-a", "20261021"
)


def _target(*, effect: str, intent_id: str) -> dict[str, object]:
    opening = effect == "OPEN"
    return {
        "schema_id": "bt_api.approved_order_target",
        "schema_version": 1,
        "target_ref": f"target-{intent_id}",
        "target_version": 1,
        "owner_subject": "owner-a",
        "principal_ref": _PRINCIPAL,
        "actor_ref": "actor-a",
        "delegate_ref": "delegate-a",
        "scope_ref": "scope-a",
        "wire_scope": {
            "execution_domain_ref": "paper-domain",
            "account_ref": "account-a",
            "account_refs": [],
            "strategy_ref": "strategy-a",
            "run_id": None,
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
        "side": "BUY" if opening else "SELL",
        "position_effect": effect,
        "order_type": "LIMIT",
        "quantity": "1.25",
        "price": "10.5",
        "time_in_force": "GTC",
        "reduce_only": not opening,
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


class _ApprovedTargetPort:
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

    def resolve_approved_target_v1(self, **_arguments: object) -> object:
        self.calls.append("resolve")
        return deepcopy(self.target)

    def consume_approved_target_v1(self, **arguments: object) -> object:
        self.calls.append("consume")
        return {
            "disposition": "CONSUMED_NEW",
            "consumption_ref": "approved-consumption",
            "approval_ref": arguments["approval_ref"],
            "approval_version": arguments["approval_version"],
            "consumed_record_version": 4,
            "target_ref": self.target["target_ref"],
            "target_version": self.target["target_version"],
            "target_sha256": arguments["target_sha256"],
            "intent_id": arguments["intent_id"],
            "intent_sha256": arguments["intent_sha256"],
            "consumed_at_ns": 123456789,
        }


class _RiskGate:
    def __init__(self) -> None:
        self.reserved: list[str] = []
        self.claimed: list[str] = []
        self.settled: list[str] = []
        self.released: list[str] = []

    def reserve(self, intent):
        self.reserved.append(intent.intent_id)
        return {"permit_id": f"risk-{intent.intent_id}"}

    def claim_for_dispatch(self, permit_id: str, intent):
        self.claimed.append(intent.intent_id)
        return permit_id

    def settle(self, permit_id: str) -> None:
        self.settled.append(permit_id)

    def release(self, permit_id: str, _reason: str) -> None:
        self.released.append(permit_id)


class _SyntheticDispatcher:
    def __init__(self, store: SqliteExecutionStore) -> None:
        self.store = store
        self.submitted: list[str] = []
        self.unauthorized: list[str] = []

    def submit_authorized(self, intent, authorization):
        record = self.store.get(intent.intent_id, scope=intent.scope)
        assert record is not None and record.state is ExecutionState.DISPATCHING
        assert authorization.intent_fingerprint == intent.fingerprint
        self.submitted.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, "synthetic-order")

    def submit(self, intent):
        self.unauthorized.append(intent.intent_id)
        raise AssertionError("approved target must use authorized dispatch")

    def cancel(self, intent):
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )


def _service(tmp_path, *, intent_id: str, effect: str):
    target = _target(effect=effect, intent_id=intent_id)
    target_digest = sha256(
        json.dumps(
            target,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    request = ApprovedTargetSubmitRequestV1("approval-a", 7, target_digest, intent_id)
    store = SqliteExecutionStore(tmp_path / f"{intent_id}.sqlite3")
    dispatcher = _SyntheticDispatcher(store)
    risk = _RiskGate()
    validator = ExecutionProviderValidatorV1(
        provider_id="PAPER",
        validator_id="approved-position-baseline-policy",
        version="policy-v1",
        dispatch_port=dispatcher,
        instrument_map={"BTC-USDT": "BTCUSDT"},
        quantity_steps={"BTC-USDT": Decimal("0.25")},
        max_quantities={"BTC-USDT": Decimal("100")},
        price_ticks={"BTC-USDT": Decimal("0.5")},
        supported_order_types=(OrderType.LIMIT,),
        supported_time_in_force=(TimeInForce.GTC,),
        supported_actions=(
            (Side.BUY, PositionEffect.OPEN),
            (Side.SELL, PositionEffect.CLOSE),
        ),
        supports_reduce_only=True,
    )
    basic = ExecutionBasicAdmissionGate(
        store, ExecutionAdmissionValidatorRegistry((validator,))
    )
    port = _ApprovedTargetPort(target)
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
    baseline_calls: list[tuple[object, object, object]] = []

    def position_baseline_authorizer(intent, pending_record, writer_lease):
        baseline_calls.append((intent, pending_record, writer_lease))
        return None

    service = ExecutionOrderEntryService(
        store,
        {_WIRE_SCOPE: _EXECUTION_SCOPE},
        dispatcher,
        admission_gate=risk,
        basic_admission_gate=basic,
        approved_target_authority=authority,
        position_baseline_authorizer=position_baseline_authorizer,
        writer_id="approved-baseline-test-writer",
    )
    return store, service, dispatcher, risk, port, request, baseline_calls


def _submit(service, request: ApprovedTargetSubmitRequestV1):
    return service.handle(
        APPROVED_TARGET_OPERATION,
        principal_ref=_PRINCIPAL,
        identity=_IDENTITY,
        scope=_WIRE_SCOPE,
        arguments=request.to_payload(),
    )


@pytest.mark.unit
def test_approved_open_refuses_dispatch_when_position_baseline_is_not_authorized(
    tmp_path,
):
    intent_id = "approved-open-baseline-refusal"
    store, service, dispatcher, risk, port, request, baseline_calls = _service(
        tmp_path, intent_id=intent_id, effect="OPEN"
    )
    try:
        reply = _submit(service, request)
        record = store.get(intent_id, scope=_EXECUTION_SCOPE)

        assert reply["ok"] is True and reply["code"] == "OK"
        assert record is not None and record.review_required is True
        assert record.state is ExecutionState.PENDING_DISPATCH
        assert len(baseline_calls) == 1
        intent, pending_record, _writer_lease = baseline_calls[0]
        assert intent.intent_id == intent_id
        assert pending_record.intent_id == intent_id
        assert pending_record.state is ExecutionState.PENDING_DISPATCH
        assert dispatcher.submitted == []
        assert dispatcher.unauthorized == []
        assert risk.reserved == [intent_id]
        assert risk.claimed == []
        assert port.calls == ["reserve", "resolve"]
    finally:
        store.close()


@pytest.mark.unit
def test_approved_close_does_not_call_open_position_baseline_authorizer(tmp_path):
    intent_id = "approved-close-no-open-baseline"
    store, service, dispatcher, risk, port, request, baseline_calls = _service(
        tmp_path, intent_id=intent_id, effect="CLOSE"
    )
    try:
        reply = _submit(service, request)
        record = store.get(intent_id, scope=_EXECUTION_SCOPE)

        assert reply["ok"] is True and reply["code"] == "OK"
        assert record is not None and record.state is ExecutionState.ACKED
        assert baseline_calls == []
        assert dispatcher.submitted == [intent_id]
        assert dispatcher.unauthorized == []
        assert risk.reserved == [intent_id]
        assert risk.claimed == [intent_id]
        assert port.calls == ["reserve", "resolve", "consume"]
    finally:
        store.close()
