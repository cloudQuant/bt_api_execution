"""Focused source-fake tests for coordinator-authorized execution group legs."""

from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from dataclasses import dataclass
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
        self._guard_thread_id: int | None = None
        self.risk_scope_fingerprint = _RISK_SCOPE_FINGERPRINT
        self.guard_entries = 0
        self.guard_exits = 0

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
            self._guard_thread_id = threading.get_ident()
            self.guard_entries += 1
            try:
                yield authorization
            finally:
                self.guard_exits += 1
                self.active_authorization = None
                self._guard_thread_id = None

    def assert_current(self) -> None:
        assert self._guard_thread_id == threading.get_ident()
        assert self.authorization_active
        assert self.active_authorization is not None

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


def _make_service(store_path: str, *, claim_error: bool = False, bind_account_scope: bool = True):
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
    )
    group_scope = WireScope(
        execution_domain_ref="paper-demo",
        account_ref=_ACCOUNT,
        strategy_ref=_STRATEGY,
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
