"""Typed AFI approved-target facts accepted by the Execution boundary."""

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
    ApprovedTargetAuthorityError,
    ApprovedTargetDocumentV1,
    ApprovedTargetOwnerBindingsV1,
    ApprovedTargetOwnerBindingV1,
    ApprovedTargetSubmitRequestV1,
    ProtectedApprovedTargetScopesV1,
    approved_target_audit_tags,
    approved_target_operation_key,
    has_reserved_approval_tags,
)
from bt_api_execution.contracts import ExecutionScope
from bt_api_execution.errors import ContractValidationError
from bt_api_execution.group_contracts import group_leg_intent_id
from bt_api_execution.service_order_entry import (
    APPROVED_TARGET_OPERATION,
    SUBMIT_OPERATION,
    ExecutionOrderEntryService,
)

_HASH = "1" * 64
_PRINCIPAL = "afi-principal"
_IDENTITY = WireIdentity(WireRole.APPLICATION_CLIENT, _PRINCIPAL, "actor-a", "delegate-a")
_WIRE_SCOPE = WireScope(
    "paper-domain",
    account_ref="account-a",
    strategy_ref="strategy-a",
    portfolio_ref="portfolio-a",
)
_EXECUTION_SCOPE = ExecutionScope(
    "PAPER", "simulation", "account-a", "strategy-a", "20261016"
)


def _target(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_id": "bt_api.approved_order_target",
        "schema_version": 1,
        "target_ref": "target-a",
        "target_version": 2,
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
        "side": "BUY",
        "position_effect": "OPEN",
        "order_type": "LIMIT",
        "quantity": "1.25",
        "price": "10.5",
        "time_in_force": "GTC",
        "reduce_only": False,
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


def _request(target: dict[str, object]) -> ApprovedTargetSubmitRequestV1:
    canonical = json.dumps(
        target, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("utf-8")
    return ApprovedTargetSubmitRequestV1(
        "approval-a", 7, sha256(canonical).hexdigest(), "intent-a"
    )


def _binding() -> ApprovedTargetOwnerBindingV1:
    return ApprovedTargetOwnerBindingV1(
        principal_ref=_PRINCIPAL,
        identity=_IDENTITY,
        wire_scope=_WIRE_SCOPE,
        owner_subject="owner-a",
        expected_scope_ref="scope-a",
    )


def test_v1_target_digest_and_order_intent_are_canonical_and_server_bound() -> None:
    target_value = _target()
    target = ApprovedTargetDocumentV1.from_mapping(target_value)
    request = _request(target_value)
    target.validate_binding(
        _binding(), principal_ref=_PRINCIPAL, identity=_IDENTITY, wire_scope=_WIRE_SCOPE
    )

    intent = target.to_order_intent(
        intent_id=request.intent_id,
        execution_scope=_EXECUTION_SCOPE,
        request=request,
    )

    assert target.sha256 == request.target_sha256
    assert intent.intent_id == "intent-a"
    assert intent.scope is _EXECUTION_SCOPE
    assert intent.instrument == "BTC-USDT"
    assert str(intent.quantity) == "1.25"
    assert str(intent.price) == "10.5"
    assert intent.signal_id.startswith("approval.")
    assert dict(intent.tags) == dict(approved_target_audit_tags(request))
    assert has_reserved_approval_tags(intent.tags)


@pytest.mark.parametrize(
    "change",
    [
        {"unknown": "extra"},
        {"schema_version": True},
        {"target_version": True},
        {"strategy_version": 0},
        {"membership_version": 4.0},
        {"quantity": 1.25},
        {"quantity": "1.0"},
        {"price": "10.50"},
        {"policy_sha256": "not-a-digest"},
        {"reduce_only": 0},
    ],
)
def test_target_rejects_ambiguous_or_noncanonical_fields(change: dict[str, object]) -> None:
    target = _target()
    target.update(change)
    with pytest.raises(ContractValidationError):
        ApprovedTargetDocumentV1.from_mapping(target)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda wire: wire.update(account_ref=None, account_refs=["account-a"]),
        lambda wire: wire.update(manual_book_ref="manual-a"),
        lambda wire: wire.update(group_id="group-a", leg_id="leg-a"),
    ],
)
def test_target_wire_scope_rejects_manual_group_or_changed_domain(mutate) -> None:
    target = _target()
    target["wire_scope"] = deepcopy(target["wire_scope"])
    mutate(target["wire_scope"])
    with pytest.raises(ContractValidationError):
        ApprovedTargetDocumentV1.from_mapping(target)


def test_target_binding_requires_exact_identity_scope_subject_and_scope_ref() -> None:
    target_value = _target()
    target = ApprovedTargetDocumentV1.from_mapping(target_value)
    binding = _binding()
    other_identity = WireIdentity(
        WireRole.APPLICATION_CLIENT, _PRINCIPAL, "actor-a", "other-delegate"
    )
    other_scope = WireScope(
        "paper-domain",
        account_ref="account-a",
        strategy_ref="strategy-a",
        run_id="run-a",
        portfolio_ref="portfolio-a",
    )
    other_domain = WireScope(
        "live-domain",
        account_ref="account-a",
        strategy_ref="strategy-a",
        portfolio_ref="portfolio-a",
    )

    with pytest.raises(ContractValidationError):
        target.validate_binding(
            binding, principal_ref=_PRINCIPAL, identity=other_identity, wire_scope=_WIRE_SCOPE
        )
    with pytest.raises(ContractValidationError):
        target.validate_binding(
            binding, principal_ref=_PRINCIPAL, identity=_IDENTITY, wire_scope=other_scope
        )
    with pytest.raises(ContractValidationError):
        target.validate_binding(
            binding, principal_ref=_PRINCIPAL, identity=_IDENTITY, wire_scope=other_domain
        )
    wrong_owner = ApprovedTargetOwnerBindingV1(
        _PRINCIPAL, _IDENTITY, _WIRE_SCOPE, "other-owner", "scope-a"
    )
    with pytest.raises(ContractValidationError):
        target.validate_binding(
            wrong_owner, principal_ref=_PRINCIPAL, identity=_IDENTITY, wire_scope=_WIRE_SCOPE
        )
    wrong_scope_ref = ApprovedTargetOwnerBindingV1(
        _PRINCIPAL, _IDENTITY, _WIRE_SCOPE, "owner-a", "another-scope"
    )
    with pytest.raises(ContractValidationError):
        target.validate_binding(
            wrong_scope_ref,
            principal_ref=_PRINCIPAL,
            identity=_IDENTITY,
            wire_scope=_WIRE_SCOPE,
        )


def test_owner_bindings_reject_ambiguous_full_transport_tuple() -> None:
    with pytest.raises(ContractValidationError):
        ApprovedTargetOwnerBindingsV1((_binding(), _binding()))

    bindings = ApprovedTargetOwnerBindingsV1((_binding(),))
    assert bindings.resolve(_PRINCIPAL, _IDENTITY, _WIRE_SCOPE) is _binding() or (
        bindings.resolve(_PRINCIPAL, _IDENTITY, _WIRE_SCOPE) == _binding()
    )
    assert bindings.resolve(
        _PRINCIPAL,
        WireIdentity(WireRole.APPLICATION_CLIENT, _PRINCIPAL, "actor-b"),
        _WIRE_SCOPE,
    ) is None


def test_protected_scope_only_blocks_exact_strategy_template() -> None:
    protected = ProtectedApprovedTargetScopesV1((_WIRE_SCOPE,))
    run_scope = WireScope(
        "paper-domain",
        account_ref="account-a",
        strategy_ref="strategy-a",
        run_id="run-a",
        portfolio_ref="portfolio-a",
    )
    other_strategy = WireScope(
        "paper-domain",
        account_ref="account-a",
        strategy_ref="strategy-b",
        run_id="run-a",
        portfolio_ref="portfolio-a",
    )
    manual = WireScope("paper-domain", account_ref="account-a", manual_book_ref="manual-a")
    other_domain = WireScope(
        "live-domain",
        account_ref="account-a",
        strategy_ref="strategy-a",
        run_id="run-a",
        portfolio_ref="portfolio-a",
    )

    assert protected.requires_approval(_WIRE_SCOPE)
    assert protected.requires_approval(run_scope)
    assert not protected.requires_approval(other_strategy)
    assert not protected.requires_approval(manual)
    assert not protected.requires_approval(other_domain)
    with pytest.raises(ContractValidationError):
        ProtectedApprovedTargetScopesV1((_WIRE_SCOPE, run_scope))


def test_approved_operation_arguments_are_exact_and_audit_tags_are_reserved() -> None:
    parsed = ApprovedTargetSubmitRequestV1.from_arguments(
        {
            "approval_ref": "approval-a",
            "approval_version": 7,
            "target_sha256": _HASH,
            "intent_id": "intent-a",
        }
    )
    assert set(parsed.to_payload()) == {
        "approval_ref",
        "approval_version",
        "target_sha256",
        "intent_id",
    }
    with pytest.raises(ContractValidationError):
        ApprovedTargetSubmitRequestV1.from_arguments(
            {**parsed.to_payload(), "quantity": "1"}
        )
    with pytest.raises(ContractValidationError):
        ApprovedTargetSubmitRequestV1.from_arguments(
            {**parsed.to_payload(), "approval_version": True}
        )
    assert has_reserved_approval_tags({"execution.approval.ref": "forged"})
    assert not has_reserved_approval_tags({"strategy.note": "ordinary"})


class _AuthorityFailureError(Exception):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _AuthorityPort:
    def __init__(self, target: dict[str, object], *, consume_mode: str = "new") -> None:
        self.target = target
        self.consume_mode = consume_mode
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.fail_reserve: Exception | None = None

    def reserve_approved_target_operation(self, **arguments: object) -> object:
        copied = dict(arguments)
        self.calls.append(("reserve", copied))
        if self.fail_reserve is not None:
            raise self.fail_reserve
        return {
            "approval_ref": copied["approval_ref"],
            "approval_version": copied["approval_version"],
            "target_sha256": copied["target_sha256"],
            "intent_id": copied["intent_id"],
            "operation_request_sha256": "4" * 64,
            "created": True,
            "record_version": 2,
        }

    def resolve_approved_target_v1(self, **arguments: object) -> object:
        self.calls.append(("resolve", dict(arguments)))
        return deepcopy(self.target)

    def consume_approved_target_v1(self, **arguments: object) -> object:
        self.calls.append(("consume", dict(arguments)))
        if self.consume_mode == "denied":
            raise ApprovedTargetAuthorityError("APPROVAL_REVOKED")
        if self.consume_mode == "spoofed_denial":
            raise _AuthorityFailureError("APPROVAL_REVOKED")
        if self.consume_mode == "ambiguous":
            raise TimeoutError("approval transaction outcome is unknown")
        if self.consume_mode == "malformed":
            return {"disposition": "CONSUMED_NEW"}
        disposition = (
            "ALREADY_CONSUMED" if self.consume_mode == "already" else "CONSUMED_NEW"
        )
        return {
            "disposition": disposition,
            "consumption_ref": "consume-ref",
            "approval_ref": arguments["approval_ref"],
            "approval_version": arguments["approval_version"],
            "consumed_record_version": 4,
            "target_ref": "target-a",
            "target_version": 2,
            "target_sha256": arguments["target_sha256"],
            "intent_id": arguments["intent_id"],
            "intent_sha256": arguments["intent_sha256"],
            "consumed_at_ns": 123456789,
        }


class RiskAdmissionDeniedError(PermissionError):
    """Structural stand-in for Risk's public deterministic denial type."""

    __module__ = "bt_api_risk.order_admission"

    def __init__(self) -> None:
        self.code = "RISK_POLICY_DENIED"
        super().__init__(self.code)


class _RiskGate:
    def __init__(self, trace: list[str], *, claim_mode: str = "ok") -> None:
        self.trace = trace
        self.claim_mode = claim_mode
        self.reserved: list[str] = []
        self.claimed: list[str] = []
        self.released: list[str] = []
        self.settled: list[str] = []

    def reserve(self, intent) -> dict[str, str]:
        self.trace.append("risk.reserve")
        self.reserved.append(intent.intent_id)
        return {"permit_id": "risk-" + intent.intent_id}

    def claim_for_dispatch(self, reference: str, intent) -> str:
        self.trace.append("risk.claim")
        if self.claim_mode == "denied":
            raise RiskAdmissionDeniedError()
        if self.claim_mode == "ambiguous":
            raise TimeoutError("risk claim reply lost")
        self.claimed.append(intent.intent_id)
        return reference

    def release(self, reference: str, reason: str) -> None:
        self.released.append(reference)

    def settle(self, reference: str) -> None:
        self.settled.append(reference)


class _ApprovedProvider:
    def __init__(self, trace: list[str], store: SqliteExecutionStore) -> None:
        self.trace = trace
        self.store = store
        self.submitted: list[str] = []
        self.unauthorized: list[str] = []

    def submit_authorized(self, intent, authorization):
        self.trace.append("provider.submit_authorized")
        record = self.store.get(intent.intent_id, scope=intent.scope)
        assert record is not None and record.state is ExecutionState.DISPATCHING
        assert authorization.intent_fingerprint == intent.fingerprint
        assert authorization.parameters.intent_id == intent.intent_id
        self.submitted.append(intent.intent_id)
        return ProviderObservation.accepted(intent.intent_id, "provider-order")

    def submit(self, intent):
        self.unauthorized.append(intent.intent_id)
        raise AssertionError("approved target must use authorized dispatch")

    def cancel(self, intent):
        raise AssertionError("cancellation is outside this focused test")


def _approved_service(
    tmp_path,
    *,
    target: dict[str, object] | None = None,
    consume_mode: str = "new",
    risk_claim_mode: str = "ok",
    monkeypatch=None,
):
    store = SqliteExecutionStore(tmp_path / "approved-execution.sqlite3")
    trace: list[str] = []
    provider = _ApprovedProvider(trace, store)
    risk = _RiskGate(trace, claim_mode=risk_claim_mode)
    validator = ExecutionProviderValidatorV1(
        provider_id="PAPER",
        validator_id="approved-target-policy",
        version="policy-v1",
        dispatch_port=provider,
        instrument_map={"BTC-USDT": "BTCUSDT"},
        quantity_steps={"BTC-USDT": Decimal("0.25")},
        max_quantities={"BTC-USDT": Decimal("100")},
        price_ticks={"BTC-USDT": Decimal("0.5")},
        supported_order_types=(OrderType.LIMIT,),
        supported_time_in_force=(TimeInForce.GTC,),
        supported_actions=((Side.BUY, PositionEffect.OPEN),),
        supports_reduce_only=False,
    )
    basic = ExecutionBasicAdmissionGate(
        store, ExecutionAdmissionValidatorRegistry((validator,))
    )
    target_value = _target() if target is None else target
    port = _AuthorityPort(target_value, consume_mode=consume_mode)
    authority = ApprovedTargetAuthorityV1(
        port,
        ApprovedTargetOwnerBindingsV1((_binding(),)),
        ProtectedApprovedTargetScopesV1((_WIRE_SCOPE,)),
    )
    service = ExecutionOrderEntryService(
        store,
        {_WIRE_SCOPE: _EXECUTION_SCOPE},
        provider,
        admission_gate=risk,
        basic_admission_gate=basic,
        approved_target_authority=authority,
        writer_id="approved-target-writer",
    )
    if monkeypatch is not None:
        original_claim = ExecutionBasicAdmissionGate.claim_for_dispatch

        def traced_claim(self, *arguments, **keywords):
            trace.append("basic.claim")
            return original_claim(self, *arguments, **keywords)

        monkeypatch.setattr(ExecutionBasicAdmissionGate, "claim_for_dispatch", traced_claim)
    return store, service, provider, risk, basic, port, trace


def _approved_call(service, arguments: dict[str, object] | None = None):
    return service.handle(
        APPROVED_TARGET_OPERATION,
        principal_ref=_PRINCIPAL,
        identity=_IDENTITY,
        scope=_WIRE_SCOPE,
        arguments=_request(_target()).to_payload() if arguments is None else arguments,
    )


def test_approved_service_consumes_after_durable_claim_and_replay_never_consumes_twice(
    tmp_path, monkeypatch
) -> None:
    store, service, provider, risk, _basic, port, trace = _approved_service(
        tmp_path, monkeypatch=monkeypatch
    )
    try:
        arguments = _request(_target()).to_payload()
        first = service.handle(
            APPROVED_TARGET_OPERATION,
            principal_ref=_PRINCIPAL,
            identity=_IDENTITY,
            scope=_WIRE_SCOPE,
            arguments=arguments,
        )
        second = service.handle(
            APPROVED_TARGET_OPERATION,
            principal_ref=_PRINCIPAL,
            identity=_IDENTITY,
            scope=_WIRE_SCOPE,
            arguments=arguments,
        )

        record = store.get("intent-a", scope=_EXECUTION_SCOPE)
        assert first["ok"] is True and second["ok"] is True
        assert record is not None and record.state is ExecutionState.ACKED
        assert provider.submitted == ["intent-a"]
        assert provider.unauthorized == []
        assert [kind for kind, _ in port.calls] == [
            "reserve",
            "resolve",
            "consume",
            "reserve",
            "resolve",
        ]
        assert trace == [
            "risk.reserve",
            "basic.claim",
            "risk.claim",
            "provider.submit_authorized",
        ]
        assert risk.settled == ["risk-intent-a"]
        reserve_arguments = port.calls[0][1]
        consume_arguments = port.calls[2][1]
        assert reserve_arguments["operation_key"] == consume_arguments["operation_key"]
        assert reserve_arguments["operation_key"] == approved_target_operation_key(
            ApprovedTargetSubmitRequestV1.from_arguments(arguments), _binding()
        )
        assert reserve_arguments["identity"] is _IDENTITY
        assert reserve_arguments["wire_scope"] is _WIRE_SCOPE
        assert reserve_arguments["expected_scope_ref"] == "scope-a"
        assert consume_arguments["intent_sha256"] == record.payload_sha256
    finally:
        store.close()


@pytest.mark.parametrize(
    ("consume_mode", "expected_state", "expected_code", "released"),
    [
        ("denied", ExecutionState.BLOCKED, "APPROVAL_REVOKED", 1),
        ("spoofed_denial", ExecutionState.UNKNOWN, "APPROVAL_OUTCOME_UNKNOWN", 0),
        ("ambiguous", ExecutionState.UNKNOWN, "APPROVAL_OUTCOME_UNKNOWN", 0),
        ("malformed", ExecutionState.UNKNOWN, "APPROVAL_OUTCOME_UNKNOWN", 0),
        ("already", ExecutionState.UNKNOWN, "APPROVAL_OUTCOME_UNKNOWN", 0),
    ],
)
def test_approved_consume_outcomes_fail_closed_and_unknown_replay_is_read_only(
    tmp_path, consume_mode, expected_state, expected_code, released
) -> None:
    store, service, provider, risk, _basic, port, _trace = _approved_service(
        tmp_path, consume_mode=consume_mode
    )
    try:
        arguments = _request(_target()).to_payload()
        first = service.handle(
            APPROVED_TARGET_OPERATION,
            principal_ref=_PRINCIPAL,
            identity=_IDENTITY,
            scope=_WIRE_SCOPE,
            arguments=arguments,
        )
        record = store.get("intent-a", scope=_EXECUTION_SCOPE)
        assert record is not None and record.state is expected_state
        assert first["ok"] is False and first["code"] == expected_code
        assert provider.submitted == []
        assert len(risk.released) == released
        assert len([kind for kind, _ in port.calls if kind == "consume"]) == 1

        if expected_state is ExecutionState.UNKNOWN:
            replay = service.handle(
                APPROVED_TARGET_OPERATION,
                principal_ref=_PRINCIPAL,
                identity=_IDENTITY,
                scope=_WIRE_SCOPE,
                arguments=arguments,
            )
            assert replay["ok"] is False
            assert store.get("intent-a", scope=_EXECUTION_SCOPE) == record
            assert len([kind for kind, _ in port.calls if kind == "consume"]) == 1
            assert provider.submitted == []
            assert risk.released == []
    finally:
        store.close()


@pytest.mark.parametrize("risk_claim_mode", ["denied", "ambiguous"])
def test_postconsume_risk_denial_blocks_but_ambiguous_claim_stays_unknown(
    tmp_path, risk_claim_mode
) -> None:
    store, service, provider, risk, _basic, port, _trace = _approved_service(
        tmp_path, risk_claim_mode=risk_claim_mode
    )
    try:
        reply = _approved_call(service)
        record = store.get("intent-a", scope=_EXECUTION_SCOPE)
        assert record is not None
        assert len([kind for kind, _ in port.calls if kind == "consume"]) == 1
        assert provider.submitted == []
        if risk_claim_mode == "denied":
            assert reply["code"] == "APPROVAL_USED_NOT_DISPATCHED"
            assert record.state is ExecutionState.BLOCKED
            assert risk.released == ["risk-intent-a"]
        else:
            assert reply["code"] == "APPROVAL_OUTCOME_UNKNOWN"
            assert record.state is ExecutionState.UNKNOWN
            assert risk.released == []
    finally:
        store.close()


def test_approved_operation_and_protected_ordinary_submit_have_no_bypass(tmp_path) -> None:
    store, service, provider, risk, _basic, port, _trace = _approved_service(tmp_path)
    try:
        ordinary = service.handle(
            SUBMIT_OPERATION,
            principal_ref=_PRINCIPAL,
            identity=_IDENTITY,
            scope=_WIRE_SCOPE,
            arguments={
                "intent_id": "ordinary-protected",
                "signal_id": "signal-a",
                "instrument": "BTC-USDT",
                "side": "BUY",
                "position_effect": "OPEN",
                "order_type": "LIMIT",
                "quantity": "1.25",
                "price": "10.5",
                "time_in_force": "GTC",
                "reduce_only": False,
                "metadata_version": "instrument-v1",
                "deadline_ns": None,
                "tags": {},
            },
        )
        group_id = group_leg_intent_id("approved-group", "leg-a")
        group_arguments = _request(_target())
        group_arguments = {
            **group_arguments.to_payload(),
            "intent_id": group_id,
        }
        group_leg = service.handle(
            APPROVED_TARGET_OPERATION,
            principal_ref=_PRINCIPAL,
            identity=_IDENTITY,
            scope=_WIRE_SCOPE,
            arguments=group_arguments,
        )

        assert ordinary["ok"] is False and ordinary["code"] == "APPROVAL_REQUIRED"
        assert group_leg["ok"] is False and group_leg["code"] == "INVALID_REQUEST"
        assert port.calls == []
        assert risk.reserved == []
        assert provider.submitted == []
        assert store.get("ordinary-protected", scope=_EXECUTION_SCOPE) is None
    finally:
        store.close()


def test_approval_reservation_unknown_or_target_scope_mismatch_stops_before_admission(
    tmp_path,
) -> None:
    store, service, provider, risk, _basic, port, _trace = _approved_service(tmp_path)
    try:
        port.fail_reserve = TimeoutError("reservation reply lost")
        failed_reserve = _approved_call(service)
        assert failed_reserve["ok"] is False
        assert failed_reserve["code"] == "APPROVAL_STORE_UNAVAILABLE"
        assert store.get("intent-a", scope=_EXECUTION_SCOPE) is None
        assert risk.reserved == []
        assert provider.submitted == []
        port.calls.clear()
    finally:
        store.close()

    bad_target = _target()
    bad_target["wire_scope"] = {
        **bad_target["wire_scope"],
        "execution_domain_ref": "live-domain",
    }
    port.fail_reserve = None
    port.target = bad_target
    bad_request = _request(bad_target).to_payload()
    try:
        reply = _approved_call(service, bad_request)
        assert reply["ok"] is False and reply["code"] == "APPROVAL_TARGET_INVALID"
        assert [kind for kind, _ in port.calls] == ["reserve", "resolve"]
        assert risk.reserved == []
        assert provider.submitted == []
    finally:
        store.close()
