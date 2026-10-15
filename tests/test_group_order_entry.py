"""Focused source-fake tests for coordinator-authorized execution group legs."""

from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from dataclasses import dataclass, replace
from decimal import Decimal
from types import SimpleNamespace
from typing import TYPE_CHECKING

from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    AccountScopedGroupAdmissionAdapter,
    ExecutionScope,
    ExecutionState,
    GroupLegAuthorization,
    GroupPermitBinding,
    OrderIntent,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
    group_leg_intent_id,
    payload_sha256,
    risk_account_scope_fingerprint,
    writer_lease_fingerprint,
)
from bt_api_execution.errors import WriterLeaseUnavailable
from bt_api_execution.service_order_entry import (
    GROUP_SUBMIT_OPERATION,
    QUERY_OPERATION,
    SUBMIT_OPERATION,
    ExecutionOrderEntryService,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

_PRINCIPAL = "group-coordinator-a"
_PORTFOLIO = "portfolio-a"
_GROUP = "group-42"
_LEG = "leg-a"
_STRATEGY = "strategy-a"
_ACCOUNT = "account-a"
_DEADLINE_NS = 9_999_999_999_999_999_999
_RISK_SCOPE_KEY = json.dumps(
    {"account_id": "mapped-a", "environment": "demo", "provider": "paper"},
    sort_keys=True,
    separators=(",", ":"),
)
_RISK_SCOPE_FINGERPRINT = risk_account_scope_fingerprint(_RISK_SCOPE_KEY)


@dataclass(frozen=True)
class _RiskPermit:
    permit_id: str
    intent_id: str
    scope: object


@dataclass(frozen=True)
class _DispatchClaim:
    scope: object
    permit_id: str
    intent_id: str
    intent_hash: str


@dataclass(frozen=True)
class RiskGroupLegClaimReceipt:
    scope: object
    group_id: str
    group_fingerprint: str
    policy_fingerprint: str
    deadline_ns: int
    leg_id: str
    permit: _RiskPermit
    permit_reference: str
    reserved_intent_hash: str
    actual_intent_hash: str
    exposure_profile_fingerprint: str
    actual_quantity: Decimal
    actual_notional: Decimal
    claim: _DispatchClaim


class _GroupRiskAdapter:
    def __init__(self, intent: OrderIntent, binding: GroupPermitBinding) -> None:
        self.intent = intent
        self.binding = binding
        self.scope = SimpleNamespace(
            provider="paper",
            environment="demo",
            account_id="mapped-a",
            key=_RISK_SCOPE_KEY,
        )
        self.receipt_scope_override = None
        self.authority: _Authority | None = None
        self.group_claims = 0
        self.single_claims = 0
        self.reserves = 0
        self.releases = 0
        self.settlements = 0
        self.claim_error = False
        self.revoke_during_claim = False
        self.revocation_started = threading.Event()
        self.revocation_finished = threading.Event()
        self.revocation_thread: threading.Thread | None = None

    def reserve(self, intent: OrderIntent) -> object:
        self.reserves += 1
        raise AssertionError("group leg must consume its existing vector permit")

    def claim_for_dispatch(self, permit_id: str, intent: OrderIntent) -> object:
        self.single_claims += 1
        raise AssertionError("group leg must use group-aware Risk claim")

    def claim_group_leg(self, intent: OrderIntent, **kwargs: object) -> RiskGroupLegClaimReceipt:
        assert self.authority is not None
        if self.revoke_during_claim:

            def revoke() -> None:
                self.revocation_started.set()
                self.authority.revoke()
                self.revocation_finished.set()

            self.revocation_thread = threading.Thread(target=revoke)
            self.revocation_thread.start()
            assert self.revocation_started.wait(timeout=2)
            assert not self.revocation_finished.wait(timeout=0.05)
        self.authority.assert_current()
        self.group_claims += 1
        assert kwargs == {
            "scope": self.scope,
            "group_id": self.binding.group_id,
            "leg_id": self.binding.leg_id,
            "permit_reference": self.binding.permit_id,
            "group_fingerprint": self.binding.group_fingerprint,
            "deadline_ns": self.binding.deadline_ns,
        }
        if self.claim_error:
            raise TimeoutError("claim response lost after Risk commit")
        claim = _DispatchClaim(
            self.scope if self.receipt_scope_override is None else self.receipt_scope_override,
            self.binding.permit_id,
            self.intent.intent_id,
            self.binding.reserved_intent_hash,
        )
        receipt_scope = (
            self.scope if self.receipt_scope_override is None else self.receipt_scope_override
        )
        return RiskGroupLegClaimReceipt(
            scope=receipt_scope,
            group_id=self.binding.group_id,
            group_fingerprint=self.binding.group_fingerprint,
            policy_fingerprint=self.binding.policy_fingerprint,
            deadline_ns=self.binding.deadline_ns,
            leg_id=self.binding.leg_id,
            permit=_RiskPermit(self.binding.permit_id, self.intent.intent_id, receipt_scope),
            permit_reference=self.binding.permit_id,
            reserved_intent_hash=self.binding.reserved_intent_hash,
            actual_intent_hash=self.binding.actual_child_intent_hash,
            exposure_profile_fingerprint=self.binding.exposure_profile_fingerprint,
            actual_quantity=self.intent.quantity,
            actual_notional=Decimal("120"),
            claim=claim,
        )

    def settle(self, permit_id: str) -> None:
        assert permit_id == self.binding.permit_id
        self.settlements += 1

    def release(self, permit_id: str, reason: str) -> None:
        self.releases += 1
        raise AssertionError("group permits cannot be released through singleton release")


class _Authority:
    def __init__(self, store: SqliteExecutionStore) -> None:
        self.store = store
        self._lock = threading.RLock()
        self.calls: list[tuple[str, int]] = []
        self.authorizations: list[GroupLegAuthorization] = []
        self.authorization_active = True
        self.coordinator_epoch = 1
        self.account_writer_epoch = 18
        self.active_authorization: GroupLegAuthorization | None = None
        self.persisted_generation: int | None = None
        self.persisted_hold_id: str | None = None
        self._guard_thread_id: int | None = None
        self.risk_scope_fingerprint = _RISK_SCOPE_FINGERPRINT
        self.guard_entries = 0
        self.guard_exits = 0
        self.final_authorization_checks = 0
        self.final_check_observer = None

    @contextmanager
    def hold_group_leg_authorization(
        self,
        *,
        principal_ref: str,
        identity: WireIdentity,
        wire_scope: WireScope,
        execution_scope: ExecutionScope,
        intent: OrderIntent,
        binding: GroupPermitBinding,
        writer_lease: object,
    ) -> Iterator[GroupLegAuthorization]:
        # The lock models the trusted authority's exclusive current-grant and
        # epoch guard. It stays held through Risk claim and provider dispatch.
        with self._lock:
            self.store.assert_writer_lease(execution_scope, writer_lease)
            assert identity.role is WireRole.SERVICE
            assert wire_scope.group_id == binding.group_id
            assert execution_scope == intent.scope
            if not self.authorization_active:
                raise PermissionError("coordinator authorization was revoked")
            if binding.risk_scope_fingerprint != self.risk_scope_fingerprint:
                raise PermissionError("group binding targets a different Risk account")
            next_attempt = len(self.calls) + 1
            self.calls.append((principal_ref, next_attempt))
            authorization = GroupLegAuthorization(
                authorization_attempt_id=f"auth-{next_attempt}",
                principal_ref=principal_ref,
                portfolio_ref=binding.portfolio_ref,
                strategy_id=binding.strategy_id,
                account_ref=binding.account_ref,
                group_id=binding.group_id,
                leg_id=binding.leg_id,
                coordinator_epoch=self.coordinator_epoch,
                group_fingerprint=binding.group_fingerprint,
                binding_fingerprint=binding.fingerprint,
                deadline_ns=binding.deadline_ns,
                account_writer_epoch=self.account_writer_epoch,
                risk_scope_fingerprint=self.risk_scope_fingerprint,
                execution_writer_lease_fingerprint=writer_lease_fingerprint(writer_lease),
                action=GROUP_SUBMIT_OPERATION,
            )
            self.authorizations.append(authorization)
            self.active_authorization = authorization
            self.persisted_generation = authorization.coordinator_epoch
            self.persisted_hold_id = authorization.authorization_attempt_id
            self._guard_thread_id = threading.get_ident()
            self.guard_entries += 1
            guard_failed = False
            try:
                yield authorization
            except BaseException:
                guard_failed = True
                raise
            finally:
                self.guard_exits += 1
                self.active_authorization = None
                if not guard_failed:
                    self.persisted_generation = None
                    self.persisted_hold_id = None
                self._guard_thread_id = None

    def assert_current(self) -> None:
        assert self._guard_thread_id == threading.get_ident()
        assert self.authorization_active
        assert self.active_authorization is not None

    def assert_current_group_leg_authorization(
        self,
        *,
        principal_ref: str,
        identity: WireIdentity,
        wire_scope: WireScope,
        execution_scope: ExecutionScope,
        intent: OrderIntent,
        binding: GroupPermitBinding,
        authorization: GroupLegAuthorization,
        writer_lease: object,
    ) -> None:
        self.assert_current()
        assert identity.principal_ref == principal_ref == authorization.principal_ref
        assert wire_scope.group_id == binding.group_id == authorization.group_id
        assert execution_scope == intent.scope
        assert self.active_authorization is authorization
        if (
            self.persisted_generation != authorization.coordinator_epoch
            or self.persisted_hold_id != authorization.authorization_attempt_id
        ):
            raise PermissionError("durable authorization generation or hold changed")
        authorization.validate_binding(binding)
        authorization.validate_writer_lease(writer_lease)
        self.final_authorization_checks += 1
        if self.final_check_observer is not None:
            self.final_check_observer()

    def revoke(self) -> None:
        with self._lock:
            self.authorization_active = False
            self.coordinator_epoch += 1
            self.account_writer_epoch += 1

    def advance_epochs(self) -> None:
        with self._lock:
            self.coordinator_epoch += 1
            self.account_writer_epoch += 1


class _Provider:
    def __init__(self, *, raise_on_submit: bool = False) -> None:
        self.raise_on_submit = raise_on_submit
        self.submitted: list[str] = []
        self.authority: _Authority | None = None

    def submit(self, intent: OrderIntent) -> ProviderObservation:
        assert self.authority is not None
        self.authority.assert_current()
        self.submitted.append(intent.intent_id)
        if self.raise_on_submit:
            raise TimeoutError("provider outcome uncertain")
        return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

    def cancel(self, intent: object) -> object:
        raise AssertionError("group acceptance test must not cancel")


class _EvidenceDispatcher:
    def __init__(self, observation: ProviderObservation, *, fail: bool = False) -> None:
        self.observation = observation
        self.fail = fail
        self.claim_receipt: object | None = None
        self.submitted: list[str] = []

    def submit(self, intent: OrderIntent) -> ProviderObservation:
        assert self.claim_receipt is not None
        self.submitted.append(intent.intent_id)
        if self.fail:
            raise TimeoutError("provider outcome uncertain")
        assert intent.intent_id == self.observation.intent_id
        return self.observation


def _make_service(
    store_path: str,
    *,
    claim_error: bool = False,
    bind_account_scope: bool = True,
    run_id: str | None = None,
):
    execution_scope = ExecutionScope("paper", "demo", _ACCOUNT, _STRATEGY)
    intent = OrderIntent.limit(
        intent_id=group_leg_intent_id(_GROUP, _LEG),
        scope=execution_scope,
        signal_id="signal-a",
        instrument="BTC-USD",
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("60"),
        deadline_ns=_DEADLINE_NS,
    )
    binding = GroupPermitBinding(
        group_id=_GROUP,
        leg_id=_LEG,
        portfolio_ref=_PORTFOLIO,
        strategy_id=_STRATEGY,
        account_ref=_ACCOUNT,
        coordinator_principal_ref=_PRINCIPAL,
        risk_scope_fingerprint=_RISK_SCOPE_FINGERPRINT,
        permit_id="permit-group-42-leg-a",
        group_fingerprint=payload_sha256({"group": _GROUP}),
        policy_fingerprint=payload_sha256({"policy": "risk-v1"}),
        reserved_intent_hash=payload_sha256({"reserved": "maximum-risk-intent"}),
        exposure_profile_fingerprint=payload_sha256({"profile": "BTC-USD-long"}),
        actual_child_intent_hash=payload_sha256({"actual": "risk-child-intent"}),
        order_intent_hash=intent.fingerprint,
        deadline_ns=_DEADLINE_NS,
    )
    base_scope = WireScope(
        execution_domain_ref="paper-demo",
        account_ref=_ACCOUNT,
        strategy_ref=_STRATEGY,
        portfolio_ref=_PORTFOLIO,
    )
    group_scope = WireScope(
        execution_domain_ref="paper-demo",
        account_ref=_ACCOUNT,
        strategy_ref=_STRATEGY,
        run_id=run_id,
        portfolio_ref=_PORTFOLIO,
        group_id=_GROUP,
        leg_id=_LEG,
    )
    identity = WireIdentity(WireRole.SERVICE, _PRINCIPAL, "group-worker-a")
    store = SqliteExecutionStore(store_path)
    risk = _GroupRiskAdapter(intent, binding)
    risk.claim_error = claim_error
    provider = _Provider()
    authority = _Authority(store)
    risk.authority = authority
    provider.authority = authority
    service = ExecutionOrderEntryService(
        store,
        {base_scope: execution_scope},
        provider,
        admission_gate=(
            AccountScopedGroupAdmissionAdapter(risk, account_scope=risk.scope)
            if bind_account_scope
            else risk
        ),
        group_coordinator_authority=authority,
    )
    return (
        service,
        store,
        risk,
        provider,
        authority,
        identity,
        group_scope,
        base_scope,
        intent,
        binding,
    )


def test_group_leg_uses_existing_permit_and_exposes_authorized_binding(tmp_path) -> None:
    values = _make_service(str(tmp_path / "execution.sqlite"))
    (
        service,
        store,
        risk,
        provider,
        authority,
        identity,
        group_scope,
        base_scope,
        intent,
        binding,
    ) = values

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
    )

    assert result["ok"] is True
    assert result["result"]["record"]["state"] == ExecutionState.ACKED.value
    assert result["result"]["record"]["group_binding"] == binding.to_payload()
    assert result["result"]["record"]["group_authorization_attempts"][0]["coordinator_epoch"] == 1
    assert risk.group_claims == 1
    assert risk.single_claims == risk.reserves == risk.releases == 0
    assert risk.settlements == 1
    assert provider.submitted == [intent.intent_id]
    assert len(authority.calls) == 1

    store.close()
    reopened = SqliteExecutionStore(str(tmp_path / "execution.sqlite"))
    record = reopened.get(intent.intent_id, scope=intent.scope)
    assert record is not None and record.group_binding == binding
    assert (
        reopened.list_group_authorization_attempts(intent.intent_id, scope=intent.scope)[
            0
        ].account_writer_epoch
        == 18
    )
    reopened.close()


def test_group_leg_target_resolution_preserves_portfolio_with_same_run_scope(tmp_path) -> None:
    values = _make_service(str(tmp_path / "portfolio-scope.sqlite"), run_id="run-a")
    (
        service,
        store,
        risk,
        provider,
        authority,
        identity,
        group_scope,
        base_scope,
        intent,
        binding,
    ) = values
    other_portfolio_scope = replace(group_scope, portfolio_ref="portfolio-b")
    other_portfolio_binding = replace(binding, portfolio_ref="portfolio-b")

    assert len(service._targets) == 1
    assert next(iter(service._targets)) == base_scope
    assert base_scope.account_ref == other_portfolio_scope.account_ref == _ACCOUNT
    assert base_scope.strategy_ref == other_portfolio_scope.strategy_ref == _STRATEGY
    assert group_scope.run_id == other_portfolio_scope.run_id == "run-a"
    assert base_scope.run_id is None
    assert base_scope.portfolio_ref == _PORTFOLIO
    assert (
        other_portfolio_scope.portfolio_ref
        == other_portfolio_binding.portfolio_ref
        == "portfolio-b"
    )

    denied = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=other_portfolio_scope,
        intent=intent,
        binding=other_portfolio_binding,
    )

    assert denied["ok"] is False
    assert denied["code"] == "SCOPE_DENIED"
    assert denied["result"] == {"found": False, "record": None}
    assert risk.group_claims == risk.single_claims == risk.reserves == risk.releases == 0
    assert risk.settlements == 0
    assert authority.calls == []
    assert provider.submitted == []
    assert store.get(intent.intent_id, scope=intent.scope) is None

    allowed = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
    )

    assert allowed["ok"] is True
    assert allowed["result"]["record"]["state"] == ExecutionState.ACKED.value
    assert risk.group_claims == 1
    assert authority.calls == [(_PRINCIPAL, 1)]
    assert provider.submitted == [intent.intent_id]
    store.close()


def test_after_group_claim_hook_receives_exact_claim_before_provider(tmp_path) -> None:
    values = _make_service(str(tmp_path / "after-claim.sqlite"))
    service, store, risk, provider, authority, identity, group_scope, _, intent, binding = values
    sequence: list[str] = []
    provider_submit = provider.submit

    def submit_after_hook(order_intent: OrderIntent) -> ProviderObservation:
        sequence.append("provider")
        return provider_submit(order_intent)

    provider.submit = submit_after_hook
    claim_facts: list[tuple[object, ...]] = []

    def after_claim(
        received_intent: OrderIntent,
        received_binding: GroupPermitBinding,
        authorization: GroupLegAuthorization,
        writer_lease: object,
        receipt: object,
    ) -> bool:
        sequence.append("claim_hook")
        assert received_intent is intent
        assert received_binding is binding
        assert authorization is authority.authorizations[0]
        assert authorization.execution_writer_lease_fingerprint == writer_lease_fingerprint(
            writer_lease
        )
        assert type(receipt).__name__ == "RiskGroupLegClaimReceipt"
        assert receipt.permit_reference == binding.permit_id
        assert risk.group_claims == 1
        assert provider.submitted == []
        authority.assert_current()
        claim_facts.append(
            (received_intent, received_binding, authorization, writer_lease, receipt)
        )
        return False

    authority.final_check_observer = lambda: sequence.append("authority_check")

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=after_claim,
    )

    assert result["result"]["record"]["state"] == ExecutionState.ACKED.value
    assert sequence == ["claim_hook", "authority_check", "provider"]
    assert len(claim_facts) == 1
    assert authority.final_authorization_checks == 1
    assert risk.group_claims == 1
    assert provider.submitted == [intent.intent_id]
    store.close()


def test_evidence_dispatcher_result_callback_runs_after_durable_terminal_record(tmp_path) -> None:
    values = _make_service(str(tmp_path / "group-result.sqlite"))
    service, store, risk, provider, authority, identity, group_scope, _, intent, binding = values
    dispatcher = _EvidenceDispatcher(
        ProviderObservation(
            intent.intent_id,
            ExecutionState.FILLED,
            provider_order_id="provider-order-filled",
            filled_quantity=intent.quantity,
            average_price=Decimal("60"),
        )
    )
    callback_rows: list[tuple[object, object]] = []
    events: list[str] = []

    def after_claim(
        received_intent: OrderIntent,
        received_binding: GroupPermitBinding,
        authorization: GroupLegAuthorization,
        writer_lease: object,
        validated_claim: object,
    ) -> None:
        events.append("claim")
        assert received_intent is intent
        assert received_binding is binding
        assert authorization is authority.authorizations[-1]
        assert writer_lease is not None
        assert type(validated_claim).__name__ == "RiskGroupLegClaimReceipt"
        dispatcher.claim_receipt = validated_claim

    def after_result(
        received_intent: OrderIntent,
        received_binding: GroupPermitBinding,
        authorization: GroupLegAuthorization,
        writer_lease: object,
        validated_claim: object,
        record: object,
    ) -> None:
        events.append("result")
        assert received_intent is intent
        assert received_binding is binding
        assert authorization is authority.authorizations[-1]
        assert writer_lease is not None
        assert validated_claim is dispatcher.claim_receipt
        assert record.state is ExecutionState.FILLED
        assert record == store.get(intent.intent_id, scope=intent.scope)
        assert dispatcher.submitted == [intent.intent_id]
        assert provider.submitted == []
        callback_rows.append((validated_claim, record))

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=after_claim,
        provider_dispatcher=dispatcher,
        after_group_result=after_result,
    )

    assert result["ok"] is True
    assert result["result"]["record"]["state"] == ExecutionState.FILLED.value
    assert events == ["claim", "result"]
    assert len(callback_rows) == 1
    assert risk.group_claims == risk.settlements == 1
    assert authority.final_authorization_checks == 3
    assert authority.persisted_hold_id is None

    replay = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=after_claim,
        provider_dispatcher=dispatcher,
        after_group_result=after_result,
    )
    assert replay["ok"] is True
    assert replay["result"]["record"]["state"] == ExecutionState.FILLED.value
    assert dispatcher.submitted == [intent.intent_id]
    assert len(callback_rows) == 1
    assert risk.group_claims == risk.settlements == 1
    store.close()


def test_after_group_result_failure_preserves_terminal_record_and_authority_hold(tmp_path) -> None:
    values = _make_service(str(tmp_path / "group-result-failure.sqlite"))
    service, store, risk, provider, authority, identity, group_scope, _, intent, binding = values
    dispatcher = _EvidenceDispatcher(
        ProviderObservation(
            intent.intent_id,
            ExecutionState.FILLED,
            provider_order_id="provider-order-filled",
            filled_quantity=intent.quantity,
            average_price=Decimal("60"),
        )
    )

    def after_claim(*_arguments: object) -> None:
        dispatcher.claim_receipt = _arguments[-1]

    def fail_resolution(*_arguments: object) -> None:
        raise RuntimeError("Risk resolution receipt is unavailable")

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=after_claim,
        provider_dispatcher=dispatcher,
        after_group_result=fail_resolution,
    )

    durable = store.get(intent.intent_id, scope=intent.scope)
    assert result["ok"] is False
    assert result["code"] == "GROUP_RESOLUTION_UNPROVEN"
    assert result["result"]["record"]["state"] == ExecutionState.FILLED.value
    assert durable is not None and durable.state is ExecutionState.FILLED
    assert durable.dispatch_attempts == 1
    assert risk.group_claims == risk.settlements == 1
    assert dispatcher.submitted == [intent.intent_id]
    assert authority.persisted_hold_id == authority.authorizations[-1].authorization_attempt_id
    assert authority.persisted_generation == authority.authorizations[-1].coordinator_epoch
    store.close()


def test_acknowledged_group_result_is_not_treated_as_terminal_resolution(tmp_path) -> None:
    values = _make_service(str(tmp_path / "group-result-acked.sqlite"))
    service, store, risk, provider, authority, identity, group_scope, _, intent, binding = values
    dispatcher = _EvidenceDispatcher(ProviderObservation.accepted(intent.intent_id, "order-1"))
    resolution_calls: list[object] = []

    def after_claim(*_arguments: object) -> None:
        dispatcher.claim_receipt = _arguments[-1]

    def after_result(*arguments: object) -> None:
        resolution_calls.append(arguments)

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=after_claim,
        provider_dispatcher=dispatcher,
        after_group_result=after_result,
    )

    durable = store.get(intent.intent_id, scope=intent.scope)
    assert result["ok"] is False
    assert result["code"] == "GROUP_RESOLUTION_UNPROVEN"
    assert result["result"]["record"]["state"] == ExecutionState.ACKED.value
    assert durable is not None and durable.state is ExecutionState.ACKED
    assert dispatcher.submitted == [intent.intent_id]
    assert resolution_calls == []
    assert risk.group_claims == 1
    assert authority.persisted_hold_id == authority.authorizations[-1].authorization_attempt_id
    store.close()


def test_provider_rejection_with_no_order_id_reaches_resolution_callback(tmp_path) -> None:
    values = _make_service(str(tmp_path / "group-result-rejected.sqlite"))
    service, store, risk, provider, authority, identity, group_scope, _, intent, binding = values
    dispatcher = _EvidenceDispatcher(
        ProviderObservation.rejected(intent.intent_id, "provider_reject")
    )
    result_rows: list[object] = []

    def after_claim(*arguments: object) -> None:
        dispatcher.claim_receipt = arguments[-1]

    def after_result(*arguments: object) -> None:
        assert arguments[-2] is dispatcher.claim_receipt
        result_rows.append(arguments[-1])

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=after_claim,
        provider_dispatcher=dispatcher,
        after_group_result=after_result,
    )

    assert result["ok"] is True, result
    assert result["result"]["record"]["state"] == ExecutionState.REJECTED.value
    assert result["result"]["record"]["provider_order_id"] is None
    assert len(result_rows) == 1
    assert result_rows[0].dispatch_attempts == 1
    assert risk.group_claims == 1
    assert provider.submitted == []
    assert authority.persisted_hold_id is None
    store.close()


def test_evidence_override_requires_all_three_call_local_hooks(tmp_path) -> None:
    values = _make_service(str(tmp_path / "group-result-incomplete-hooks.sqlite"))
    service, store, risk, provider, authority, identity, group_scope, _, intent, binding = values
    dispatcher = _EvidenceDispatcher(ProviderObservation.accepted(intent.intent_id, "order-1"))

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=lambda *_arguments: None,
        provider_dispatcher=dispatcher,
    )

    assert result["ok"] is False
    assert result["code"] == "INVALID_REQUEST"
    assert authority.calls == []
    assert risk.group_claims == 0
    assert dispatcher.submitted == []
    assert provider.submitted == []
    store.close()


def test_unknown_evidence_dispatch_retains_hold_and_skips_result_callback(tmp_path) -> None:
    values = _make_service(str(tmp_path / "group-result-unknown.sqlite"))
    service, store, risk, _provider, authority, identity, group_scope, _, intent, binding = values
    dispatcher = _EvidenceDispatcher(
        ProviderObservation.accepted(intent.intent_id, "unused"), fail=True
    )
    resolution_calls: list[object] = []

    def after_claim(*_arguments: object) -> None:
        dispatcher.claim_receipt = _arguments[-1]

    def after_result(*arguments: object) -> None:
        resolution_calls.append(arguments)

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=after_claim,
        provider_dispatcher=dispatcher,
        after_group_result=after_result,
    )

    durable = store.get(intent.intent_id, scope=intent.scope)
    assert result["ok"] is False
    assert result["code"] == "GROUP_RESOLUTION_UNPROVEN"
    assert result["result"]["record"]["state"] == ExecutionState.UNKNOWN.value
    assert durable is not None and durable.state is ExecutionState.UNKNOWN
    assert dispatcher.submitted == [intent.intent_id]
    assert resolution_calls == []
    assert risk.group_claims == 1
    assert authority.persisted_hold_id == authority.authorizations[-1].authorization_attempt_id
    store.close()


def test_evidence_dispatchers_are_call_local_across_group_legs(tmp_path) -> None:
    values = _make_service(str(tmp_path / "group-result-two-calls.sqlite"))
    service, store, risk, provider, authority, identity, group_scope, _, intent, binding = values

    def execute_one(
        current_scope: WireScope,
        current_intent: OrderIntent,
        current_binding: GroupPermitBinding,
    ) -> tuple[dict[str, object], _EvidenceDispatcher]:
        dispatcher = _EvidenceDispatcher(
            ProviderObservation(
                current_intent.intent_id,
                ExecutionState.FILLED,
                provider_order_id=f"provider-{current_binding.leg_id}",
                filled_quantity=current_intent.quantity,
                average_price=Decimal("60"),
            )
        )

        def after_claim(*arguments: object) -> None:
            assert arguments[0] is current_intent
            assert arguments[1] is current_binding
            dispatcher.claim_receipt = arguments[-1]

        def after_result(*arguments: object) -> None:
            assert arguments[0] is current_intent
            assert arguments[1] is current_binding
            assert arguments[-2] is dispatcher.claim_receipt
            assert arguments[-1].intent_id == current_intent.intent_id
            assert arguments[-1].group_binding == current_binding

        response = service.submit_group_leg(
            principal_ref=_PRINCIPAL,
            identity=identity,
            scope=current_scope,
            intent=current_intent,
            binding=current_binding,
            after_group_claim=after_claim,
            provider_dispatcher=dispatcher,
            after_group_result=after_result,
        )
        return response, dispatcher

    first, first_dispatcher = execute_one(group_scope, intent, binding)
    second_group = "group-43"
    second_leg = "leg-b"
    second_intent = OrderIntent.limit(
        intent_id=group_leg_intent_id(second_group, second_leg),
        scope=intent.scope,
        signal_id="signal-b",
        instrument=intent.instrument,
        side=intent.side,
        quantity=intent.quantity,
        price=intent.price,
        deadline_ns=intent.deadline_ns,
    )
    second_binding = GroupPermitBinding(
        group_id=second_group,
        leg_id=second_leg,
        portfolio_ref=binding.portfolio_ref,
        strategy_id=binding.strategy_id,
        account_ref=binding.account_ref,
        coordinator_principal_ref=binding.coordinator_principal_ref,
        risk_scope_fingerprint=binding.risk_scope_fingerprint,
        permit_id="permit-group-43-leg-b",
        group_fingerprint=payload_sha256({"group": second_group}),
        policy_fingerprint=binding.policy_fingerprint,
        reserved_intent_hash=payload_sha256({"reserved": second_intent.intent_id}),
        exposure_profile_fingerprint=binding.exposure_profile_fingerprint,
        actual_child_intent_hash=payload_sha256({"actual": second_intent.intent_id}),
        order_intent_hash=second_intent.fingerprint,
        deadline_ns=second_intent.deadline_ns,
    )
    second_scope = WireScope(
        execution_domain_ref=group_scope.execution_domain_ref,
        account_ref=group_scope.account_ref,
        strategy_ref=group_scope.strategy_ref,
        portfolio_ref=group_scope.portfolio_ref,
        group_id=second_group,
        leg_id=second_leg,
    )
    risk.intent = second_intent
    risk.binding = second_binding
    second, second_dispatcher = execute_one(second_scope, second_intent, second_binding)

    assert first["ok"] is second["ok"] is True
    assert first["result"]["record"]["intent_id"] == intent.intent_id
    assert second["result"]["record"]["intent_id"] == second_intent.intent_id
    assert first_dispatcher.submitted == [intent.intent_id]
    assert second_dispatcher.submitted == [second_intent.intent_id]
    assert first_dispatcher.claim_receipt is not second_dispatcher.claim_receipt
    assert risk.group_claims == 2
    assert provider.submitted == []
    assert len(authority.authorizations) == 2
    store.close()


def test_after_group_claim_hook_exception_latches_unknown_without_provider(tmp_path) -> None:
    values = _make_service(str(tmp_path / "after-claim-failed.sqlite"))
    service, store, risk, provider, _authority, identity, group_scope, _, intent, binding = values

    def fail_after_claim(*_arguments: object) -> None:
        raise RuntimeError("evidence attempt could not be persisted")

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=fail_after_claim,
    )

    assert result["result"]["record"]["state"] == ExecutionState.UNKNOWN.value
    assert result["result"]["record"]["unknown_reason"] == "group_claim_hook_failed"
    assert risk.group_claims == 1
    assert risk.releases == risk.settlements == 0
    assert provider.submitted == []
    store.close()


def test_after_group_claim_hook_is_followed_by_final_writer_fence_check(tmp_path) -> None:
    values = _make_service(str(tmp_path / "after-claim-fence.sqlite"))
    service, store, risk, provider, _authority, identity, group_scope, _, intent, binding = values
    original_assert = store.assert_writer_lease
    hook_completed = False

    def assert_until_hook_returns(scope: object, writer_lease: object) -> None:
        if hook_completed:
            raise WriterLeaseUnavailable("writer fence changed while hook ran")
        original_assert(scope, writer_lease)

    store.assert_writer_lease = assert_until_hook_returns

    def finish_hook(*_arguments: object) -> None:
        nonlocal hook_completed
        hook_completed = True

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=finish_hook,
    )

    assert result["ok"] is False
    assert result["code"] == "OWNER_LEASE_UNAVAILABLE"
    assert result["result"]["record"]["state"] == ExecutionState.DISPATCHING.value
    assert risk.group_claims == 1
    assert provider.submitted == []
    store.assert_writer_lease = original_assert
    store.close()


def test_after_group_claim_hook_rejects_changed_durable_authority_generation(tmp_path) -> None:
    values = _make_service(str(tmp_path / "after-claim-authority-fence.sqlite"))
    service, store, risk, provider, authority, identity, group_scope, _, intent, binding = values

    def corrupt_held_generation(*_arguments: object) -> None:
        assert authority.persisted_generation is not None
        authority.persisted_generation += 1

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=corrupt_held_generation,
    )

    assert result["result"]["record"]["state"] == ExecutionState.UNKNOWN.value
    assert result["result"]["record"]["unknown_reason"] == "group_authority_fence_invalid"
    assert risk.group_claims == 1
    assert risk.releases == risk.settlements == 0
    assert provider.submitted == []
    store.close()


def test_after_group_claim_hook_cannot_extend_group_authorization_deadline(
    monkeypatch, tmp_path
) -> None:
    import bt_api_execution.facade as facade_module

    values = _make_service(str(tmp_path / "after-claim-expired.sqlite"))
    service, store, risk, provider, _authority, identity, group_scope, _, intent, binding = values

    def advance_to_deadline(*_arguments: object) -> None:
        monkeypatch.setattr(facade_module, "time_ns", lambda: binding.deadline_ns)

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
        after_group_claim=advance_to_deadline,
    )

    assert result["result"]["record"]["state"] == ExecutionState.UNKNOWN.value
    assert result["result"]["record"]["unknown_reason"] == (
        "group_authorization_expired_after_claim"
    )
    assert risk.group_claims == 1
    assert risk.releases == risk.settlements == 0
    assert provider.submitted == []
    store.close()


def test_group_entry_rejects_unscoped_risk_adapter_before_claim_or_dispatch(tmp_path) -> None:
    values = _make_service(str(tmp_path / "unscoped.sqlite"), bind_account_scope=False)
    service, store, risk, provider, authority, identity, group_scope, _, intent, binding = values

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
    )

    assert result["ok"] is False
    assert result["code"] == "GROUP_ACCOUNT_SCOPE_ADAPTER_REQUIRED"
    assert risk.group_claims == risk.single_claims == risk.reserves == risk.releases == 0
    assert provider.submitted == []
    assert authority.calls == []
    store.close()


def test_unknown_group_claim_never_retries_or_releases_permit(tmp_path) -> None:
    values = _make_service(str(tmp_path / "unknown.sqlite"), claim_error=True)
    (
        service,
        store,
        risk,
        provider,
        authority,
        identity,
        group_scope,
        _base_scope,
        intent,
        binding,
    ) = values

    first = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
    )
    second = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
    )

    assert first["result"]["record"]["state"] == ExecutionState.UNKNOWN.value
    assert second["result"]["record"]["state"] == ExecutionState.UNKNOWN.value
    assert risk.group_claims == 1
    assert risk.single_claims == risk.reserves == risk.releases == risk.settlements == 0
    assert provider.submitted == []
    assert len(authority.calls) == 2
    attempts = store.list_group_authorization_attempts(intent.intent_id, scope=intent.scope)
    assert len(attempts) == 1
    assert attempts[0].coordinator_epoch == 1
    store.close()


def test_fresh_fence_resumes_only_unclaimed_pending_group_leg(tmp_path) -> None:
    values = _make_service(str(tmp_path / "pending.sqlite"))
    (
        service,
        store,
        risk,
        provider,
        authority,
        identity,
        group_scope,
        _base_scope,
        intent,
        binding,
    ) = values
    original_activate = store.activate_intent
    fail_once = True

    def fail_before_activation(*args: object, **kwargs: object) -> object:
        nonlocal fail_once
        if fail_once:
            fail_once = False
            raise RuntimeError("simulated crash before activation")
        return original_activate(*args, **kwargs)

    store.activate_intent = fail_before_activation
    try:
        try:
            service.submit_group_leg(
                principal_ref=_PRINCIPAL,
                identity=identity,
                scope=group_scope,
                intent=intent,
                binding=binding,
            )
        except RuntimeError as error:
            assert str(error) == "simulated crash before activation"
        else:
            raise AssertionError("the injected pre-activation crash was not observed")
    finally:
        store.activate_intent = original_activate

    pending = store.get(intent.intent_id, scope=intent.scope)
    assert pending is not None and pending.state is ExecutionState.PENDING_ADMISSION
    assert risk.group_claims == risk.reserves == 0
    assert provider.submitted == []
    authority.advance_epochs()

    resumed = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
    )

    assert resumed["result"]["record"]["state"] == ExecutionState.ACKED.value
    attempts = store.list_group_authorization_attempts(intent.intent_id, scope=intent.scope)
    assert [attempt.coordinator_epoch for attempt in attempts] == [1, 2]
    assert [attempt.account_writer_epoch for attempt in attempts] == [18, 19]
    assert [attempt.execution_writer_lease_fingerprint for attempt in attempts] == [
        authorization.execution_writer_lease_fingerprint
        for authorization in authority.authorizations
    ]
    assert all(attempt.binding_fingerprint == binding.fingerprint for attempt in attempts)
    assert risk.group_claims == 1
    assert risk.reserves == risk.single_claims == risk.releases == 0
    assert provider.submitted == [intent.intent_id]
    store.close()


def test_revoked_group_authority_fails_before_risk_claim_or_provider(tmp_path) -> None:
    values = _make_service(str(tmp_path / "revoked.sqlite"))
    (
        service,
        store,
        risk,
        provider,
        authority,
        identity,
        group_scope,
        _base_scope,
        intent,
        binding,
    ) = values
    authority.revoke()

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
    )

    assert result["code"] == "GROUP_AUTHORIZATION_FAILED"
    assert risk.group_claims == risk.reserves == risk.single_claims == 0
    assert provider.submitted == []
    assert authority.guard_entries == authority.guard_exits == 0
    store.close()


def test_authority_guard_blocks_epoch_rotation_at_claim_and_provider(tmp_path) -> None:
    values = _make_service(str(tmp_path / "guard-race.sqlite"))
    (
        service,
        store,
        risk,
        provider,
        authority,
        identity,
        group_scope,
        _base_scope,
        intent,
        binding,
    ) = values
    risk.revoke_during_claim = True

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
    )

    assert result["result"]["record"]["state"] == ExecutionState.ACKED.value
    assert risk.group_claims == 1
    assert provider.submitted == [intent.intent_id]
    assert risk.revocation_thread is not None
    risk.revocation_thread.join(timeout=2)
    assert not risk.revocation_thread.is_alive()
    assert risk.revocation_finished.is_set()
    assert authority.authorization_active is False
    assert authority.guard_entries == authority.guard_exits == 1
    store.close()


def test_group_claim_receipt_must_match_trusted_risk_scope(tmp_path) -> None:
    values = _make_service(str(tmp_path / "wrong-risk-scope.sqlite"))
    (
        service,
        store,
        risk,
        provider,
        _authority,
        identity,
        group_scope,
        _base_scope,
        intent,
        _binding,
    ) = values
    risk.receipt_scope_override = SimpleNamespace(
        provider="paper",
        environment="demo",
        account_id="mapped-b",
        key=json.dumps(
            {"account_id": "mapped-b", "environment": "demo", "provider": "paper"},
            sort_keys=True,
            separators=(",", ":"),
        ),
    )

    result = service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=_binding,
    )

    assert result["result"]["record"]["state"] == ExecutionState.UNKNOWN.value
    assert risk.group_claims == 1
    assert provider.submitted == []
    assert risk.releases == risk.settlements == 0
    store.close()


def test_regular_submit_and_cancel_reject_group_namespace(tmp_path) -> None:
    values = _make_service(str(tmp_path / "ordinary.sqlite"))
    (
        service,
        store,
        risk,
        provider,
        _authority,
        identity,
        group_scope,
        base_scope,
        intent,
        _binding,
    ) = values
    ordinary_arguments = intent.to_payload()
    ordinary_arguments.pop("scope")

    result = service.handle(
        SUBMIT_OPERATION,
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=base_scope,
        arguments=ordinary_arguments,
    )
    cancel_result = service.handle(
        "orders.cancel",
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=base_scope,
        arguments={
            "cancel_id": "cancel-group",
            "target_intent_id": intent.intent_id,
            "provider_order_id": "provider-order-1",
        },
    )

    assert result["code"] == "INVALID_REQUEST"
    assert cancel_result["code"] == "INVALID_REQUEST"
    assert risk.reserves == risk.group_claims == risk.single_claims == 0
    assert provider.submitted == []
    assert group_scope.group_id == _GROUP
    store.close()


def test_query_returns_group_auth_attempt_audit(tmp_path) -> None:
    values = _make_service(str(tmp_path / "query.sqlite"))
    (
        service,
        store,
        risk,
        _provider,
        _authority,
        identity,
        group_scope,
        base_scope,
        intent,
        binding,
    ) = values
    service.submit_group_leg(
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=group_scope,
        intent=intent,
        binding=binding,
    )

    result = service.handle(
        QUERY_OPERATION,
        principal_ref=_PRINCIPAL,
        identity=identity,
        scope=base_scope,
        arguments={"intent_id": intent.intent_id},
    )

    dto = result["result"]["record"]
    assert dto["group_binding"] == binding.to_payload()
    assert len(dto["group_authorization_attempts"]) == 1
    assert dto["group_authorization_attempts"][0]["account_writer_epoch"] == 18
    assert risk.group_claims == 1
    store.close()
