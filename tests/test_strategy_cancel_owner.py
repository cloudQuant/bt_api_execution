"""Durable, exact-run owner cancellation behavior with a synthetic provider."""

from __future__ import annotations

from decimal import Decimal

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    CancelObservation,
    CancelDispatchClaimReceiptV1,
    DurableStoreError,
    ExecutionScope,
    ExecutionState,
    OrderIntent,
    ProviderObservation,
    SqliteExecutionStore,
)
from bt_api_execution.service_order_entry import (
    OWNER_CANCEL_OPERATION,
    SUBMIT_OPERATION,
    ExecutionOrderEntryService,
)

_DOMAIN = "paper:owner-cancel-test"
_ACCOUNT = "account-owner-cancel"
_STRATEGY = "strategy-owner-cancel"
_PRINCIPAL = "strategy-principal-owner-cancel"
_OWNER_PRINCIPAL = "owner-service-principal"
_EXECUTION_SCOPE = ExecutionScope("PAPER", "simulation", _ACCOUNT, _STRATEGY, "20261017")


class _SyntheticProvider:
    def __init__(
        self,
        *,
        unknown_cancel: bool = False,
        unknown_submit: bool = False,
        terminal_cancel: bool = False,
    ) -> None:
        self.unknown_cancel = unknown_cancel
        self.unknown_submit = unknown_submit
        self.terminal_cancel = terminal_cancel
        self.submitted: list[str] = []
        self.cancelled: list[str] = []

    def submit(self, intent: OrderIntent) -> ProviderObservation:
        self.submitted.append(intent.intent_id)
        if self.unknown_submit:
            raise TimeoutError("synthetic submission timeout")
        return ProviderObservation.accepted(intent.intent_id, f"provider-{intent.intent_id}")

    def cancel(self, intent) -> CancelObservation:
        self.cancelled.append(intent.cancel_id)
        if self.unknown_cancel:
            raise TimeoutError("synthetic cancellation timeout")
        if self.terminal_cancel:
            return CancelObservation.cancelled(
                intent.cancel_id, intent.target_intent_id, intent.provider_order_id
            )
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )


class _TerminalCancelGate:
    """Test adapter that records final-proof callback identity and ordering."""

    def __init__(self) -> None:
        self.claims: list[tuple[str, str]] = []
        self.settled: list[str] = []
        self.resolutions: list[tuple[str, str, str, str, str]] = []

    def reserve(self, intent):
        from types import SimpleNamespace

        return SimpleNamespace(permit_id="cancel-permit-v1")

    def validate(self, permit_reference, intent):
        return None

    def claim_before_dispatch(self, permit_reference, intent):
        self.claims.append((permit_reference, intent.cancel_id))
        return CancelDispatchClaimReceiptV1(
            permit_reference=permit_reference,
            scope_key=intent.scope.key,
            cancel_id=intent.cancel_id,
            intent_fingerprint=intent.fingerprint,
        )

    def settle(self, permit_reference):
        self.settled.append(permit_reference)

    def resolve_terminal(self, permit_reference, intent, record, writer_lease):
        self.resolutions.append(
            (
                permit_reference,
                intent.cancel_id,
                intent.tags["owner_cancel_command_id"],
                intent.tags["owner_cancel_strategy_ref"],
                intent.tags["owner_cancel_run_id"],
            )
        )
        return object()


def _wire_scope(
    *, run_id: str | None, portfolio_ref: str | None = None, account_ref: str = _ACCOUNT
) -> WireScope:
    return WireScope(
        _DOMAIN,
        account_ref=account_ref,
        strategy_ref=_STRATEGY,
        run_id=run_id,
        portfolio_ref=portfolio_ref,
    )


def _order_arguments(intent_id: str) -> dict[str, object]:
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


def _service(
    tmp_path,
    *,
    provider=None,
    quota=True,
    portfolio_ref=None,
    additional_portfolios=(),
    cancel_gate=None,
):
    quota_suffix = "with-quota" if quota else "without-quota"
    store = SqliteExecutionStore(tmp_path / f"owner-cancel-{quota_suffix}.sqlite3")
    provider = _SyntheticProvider() if provider is None else provider
    quota_calls: list[tuple[object, ...]] = []

    def admit_quota(*args):
        quota_calls.append(args)
        return None

    templates = {
        _wire_scope(run_id=None, portfolio_ref=portfolio): _EXECUTION_SCOPE
        for portfolio in {portfolio_ref, *additional_portfolios}
    }
    service = ExecutionOrderEntryService(
        store,
        templates,
        provider,
        allow_unprotected=True,
        cancel_quota_admission=admit_quota if quota else None,
        cancel_admission_gate=cancel_gate,
    )
    return store, service, provider, quota_calls


def _call(
    service: ExecutionOrderEntryService,
    operation: str,
    arguments: dict[str, object],
    *,
    scope: WireScope,
    identity: WireIdentity | None = None,
):
    if identity is None:
        identity = WireIdentity(WireRole.STRATEGY_CLIENT, _PRINCIPAL, _STRATEGY)
    return service.handle(
        operation,
        principal_ref=identity.principal_ref,
        identity=identity,
        scope=scope,
        arguments=arguments,
    )


def _owner_identity(actor_ref: str = "portfolio-operator") -> WireIdentity:
    return WireIdentity(WireRole.SERVICE, _OWNER_PRINCIPAL, actor_ref)


def _submit(service, intent_id: str, *, scope: WireScope):
    return _call(service, SUBMIT_OPERATION, _order_arguments(intent_id), scope=scope)


def _cancel(service, command_id: str, *, scope: WireScope, identity=None):
    return _call(
        service,
        OWNER_CANCEL_OPERATION,
        {"command_id": command_id},
        scope=scope,
        identity=_owner_identity() if identity is None else identity,
    )


@pytest.mark.unit
def test_owner_cancel_is_exact_run_scoped_and_uses_trusted_quota_args(tmp_path) -> None:
    store, service, provider, quota_calls = _service(
        tmp_path,
        portfolio_ref="portfolio-a",
        additional_portfolios=("portfolio-b",),
    )
    scope = _wire_scope(run_id="run-1", portfolio_ref="portfolio-a")
    try:
        assert _submit(service, "run-1-order", scope=scope)["ok"] is True
        result = _cancel(service, "stop-run-1", scope=scope)

        assert result["ok"] is True
        receipt = result["result"]
        assert receipt["outcome"] == "ACKNOWLEDGED"
        assert (receipt["account_ref"], receipt["portfolio_ref"]) == (
            _ACCOUNT,
            "portfolio-a",
        )
        assert (receipt["strategy_ref"], receipt["run_id"]) == (_STRATEGY, "run-1")
        assert receipt["total_count"] == receipt["selected_count"] == 1
        assert receipt["omitted_count"] == 0
        assert receipt["items"][0]["cancel_state"] == ExecutionState.ACKED.value
        assert provider.cancelled == [receipt["items"][0]["cancel_id"]]
        assert len(quota_calls) == 1
        account, strategy, run_id, instrument, operation, deadline = quota_calls[0]
        assert (account, strategy, run_id, instrument, operation) == (
            _ACCOUNT,
            _STRATEGY,
            "run-1",
            "BTC-USDT",
            "CANCEL",
        )
        assert type(deadline) is int and deadline > 0

        other_run = _cancel(service, "stop-run-2", scope=_wire_scope(
            run_id="run-2", portfolio_ref="portfolio-a"
        ))
        assert other_run["ok"] is True
        assert other_run["result"]["outcome"] == "NO_EFFECT"
        assert other_run["result"]["total_count"] == 0
        assert provider.cancelled == [receipt["items"][0]["cancel_id"]]

        other_portfolio = _cancel(
            service,
            "stop-portfolio-b",
            scope=_wire_scope(run_id="run-1", portfolio_ref="portfolio-b"),
        )
        assert other_portfolio["ok"] is True
        assert other_portfolio["result"]["outcome"] == "NO_EFFECT"
        assert other_portfolio["result"]["total_count"] == 0
        assert provider.cancelled == [receipt["items"][0]["cancel_id"]]
    finally:
        store.close()


@pytest.mark.unit
def test_owner_cancel_terminal_proof_retry_keeps_exact_run_binding_and_never_redispatches(
    tmp_path,
) -> None:
    provider = _SyntheticProvider(terminal_cancel=True)
    gate = _TerminalCancelGate()
    store, service, provider, quota_calls = _service(
        tmp_path, provider=provider, cancel_gate=gate
    )
    scope = _wire_scope(run_id="run-proof")
    try:
        assert _submit(service, "proof-order", scope=scope)["ok"] is True

        first = _cancel(service, "proof-stop", scope=scope)
        cancel_id = first["result"]["items"][0]["cancel_id"]
        persisted_intent = store.get_cancel_intent(cancel_id, scope=_EXECUTION_SCOPE)
        replay = _cancel(service, "proof-stop", scope=scope)

        assert first["ok"] is True
        assert first["result"]["items"][0]["cancel_state"] == ExecutionState.CANCELLED.value
        assert replay["result"]["items"][0]["cancel_state"] == ExecutionState.CANCELLED.value
        assert persisted_intent is not None
        assert persisted_intent.tags["owner_cancel_binding_version"] == "1"
        assert persisted_intent.tags["owner_cancel_command_id"] == "proof-stop"
        assert persisted_intent.tags["owner_cancel_principal_ref"] == _OWNER_PRINCIPAL
        assert persisted_intent.tags["owner_cancel_actor_ref"] == "portfolio-operator"
        assert persisted_intent.tags["owner_cancel_strategy_ref"] == _STRATEGY
        assert persisted_intent.tags["owner_cancel_run_id"] == "run-proof"
        assert persisted_intent.tags["owner_cancel_batch_key"].startswith("owner_cancel_batch.v1:")
        assert provider.cancelled == [cancel_id]
        assert len(quota_calls) == 1
        assert gate.claims == [("cancel-permit-v1", cancel_id)]
        assert gate.resolutions == [
            ("cancel-permit-v1", cancel_id, "proof-stop", _STRATEGY, "run-proof"),
            ("cancel-permit-v1", cancel_id, "proof-stop", _STRATEGY, "run-proof"),
        ]
    finally:
        store.close()


@pytest.mark.unit
def test_owner_cancel_resolution_source_binds_terminal_result_and_exact_run(tmp_path) -> None:
    provider = _SyntheticProvider(terminal_cancel=True)
    store, service, provider, _quota_calls = _service(
        tmp_path, provider=provider, cancel_gate=_TerminalCancelGate()
    )
    scope = _wire_scope(run_id="run-source")
    try:
        assert _submit(service, "source-order", scope=scope)["ok"] is True
        result = _cancel(service, "source-stop", scope=scope)
        cancel_id = result["result"]["items"][0]["cancel_id"]

        source = store.read_cancel_resolution_source_by_key_v1(
            cancel_id, scope_key=_EXECUTION_SCOPE.key
        )

        assert source.cancel_record.state is ExecutionState.CANCELLED
        assert source.cancel_record.dispatch_attempts == 1
        assert source.cancel_intent.tags["owner_cancel_command_id"] == "source-stop"
        assert source.cancel_intent.tags["owner_cancel_run_id"] == "run-source"
        assert source.target_record.state is ExecutionState.CANCELLED
        assert source.target_events[-1].event_type == "cancelled_by_cancel_intent"
        assert source.target_events[-1].payload["cancel_id"] == cancel_id
        assert source.owner_batch_body["run_id"] == "run-source"

        with pytest.raises(DurableStoreError):
            store.read_cancel_resolution_source_by_key_v1(
                cancel_id, scope_key="scope:" + "0" * 64
            )
    finally:
        store.close()


@pytest.mark.unit
def test_owner_cancel_resolution_source_never_projects_unknown_as_terminal(tmp_path) -> None:
    provider = _SyntheticProvider(unknown_cancel=True)
    store, service, provider, _quota_calls = _service(
        tmp_path, provider=provider, cancel_gate=_TerminalCancelGate()
    )
    scope = _wire_scope(run_id="run-unknown-source")
    try:
        assert _submit(service, "unknown-source-order", scope=scope)["ok"] is True
        result = _cancel(service, "unknown-source-stop", scope=scope)
        cancel_id = result["result"]["items"][0]["cancel_id"]

        assert result["result"]["items"][0]["cancel_state"] == ExecutionState.UNKNOWN.value
        with pytest.raises(DurableStoreError):
            store.read_cancel_resolution_source_by_key_v1(
                cancel_id, scope_key=_EXECUTION_SCOPE.key
            )
        assert provider.cancelled == [cancel_id]
    finally:
        store.close()


@pytest.mark.unit
def test_owner_cancel_unknown_replay_never_attempts_terminal_proof_or_redispatches(
    tmp_path,
) -> None:
    provider = _SyntheticProvider(unknown_cancel=True)
    gate = _TerminalCancelGate()
    store, service, provider, _quota_calls = _service(
        tmp_path, provider=provider, cancel_gate=gate
    )
    scope = _wire_scope(run_id="run-unknown-proof")
    try:
        assert _submit(service, "unknown-proof-order", scope=scope)["ok"] is True

        first = _cancel(service, "unknown-proof-stop", scope=scope)
        replay = _cancel(service, "unknown-proof-stop", scope=scope)

        assert first["result"]["outcome"] == "UNKNOWN"
        assert replay["result"]["outcome"] == "UNKNOWN"
        assert provider.cancelled == [first["result"]["items"][0]["cancel_id"]]
        assert gate.resolutions == []
        assert gate.settled == []
    finally:
        store.close()


@pytest.mark.unit
def test_owner_cancel_deadline_expiry_after_quota_admission_prevents_provider_call(
    tmp_path, monkeypatch
) -> None:
    from bt_api_execution import service_order_entry as service_module

    store, service, provider, quota_calls = _service(tmp_path)
    scope = _wire_scope(run_id="run-deadline")
    try:
        assert _submit(service, "deadline-order", scope=scope)["ok"] is True
        # The quota callback returns successfully at the exact deadline. The
        # service must stop before provider dispatch and persist the claimed
        # cancellation as BLOCKED rather than report an applied action.
        clock_values = iter((100, 5_000_000_100))
        monkeypatch.setattr(service_module, "monotonic_ns", lambda: next(clock_values))

        result = _cancel(service, "deadline-stop", scope=scope)

        assert result["result"]["outcome"] == "NO_EFFECT"
        assert result["result"]["items"][0]["cancel_state"] == ExecutionState.BLOCKED.value
        assert provider.cancelled == []
        assert len(quota_calls) == 1
        assert quota_calls[0][-1] == 5_000_000_100
    finally:
        store.close()


@pytest.mark.unit
def test_owner_cancel_lease_loss_after_quota_admission_does_not_dispatch(tmp_path) -> None:
    store, service, provider, _quota_calls = _service(tmp_path)
    scope = _wire_scope(run_id="run-lease")
    try:
        assert _submit(service, "lease-order", scope=scope)["ok"] is True
        order_facade, _cancel_facade = service._facade_pair(_EXECUTION_SCOPE)

        def lose_lease(*_args) -> None:
            lease = order_facade._last_writer_lease
            assert lease is not None
            assert store.release_lease(
                _EXECUTION_SCOPE,
                service._writer_id,
                fencing_token=lease.fencing_token,
            )
            return None

        service._cancel_quota_admission = lose_lease
        result = _cancel(service, "lease-stop", scope=scope)

        assert result["result"]["outcome"] == "PENDING"
        assert result["result"]["items"][0]["cancel_state"] == ExecutionState.DISPATCHING.value
        assert provider.cancelled == []
    finally:
        store.close()


@pytest.mark.unit
def test_unknown_cancel_replay_and_new_command_are_query_only(tmp_path) -> None:
    provider = _SyntheticProvider(unknown_cancel=True)
    store, service, provider, _quota_calls = _service(tmp_path, provider=provider)
    scope = _wire_scope(run_id="run-1")
    try:
        assert _submit(service, "uncertain-order", scope=scope)["ok"] is True
        first = _cancel(service, "uncertain-stop", scope=scope)
        replay = _cancel(service, "uncertain-stop", scope=scope)
        second_command = _cancel(service, "later-stop", scope=scope)

        assert first["result"]["outcome"] == "UNKNOWN"
        assert replay["result"]["outcome"] == "UNKNOWN"
        assert second_command["result"]["outcome"] == "UNKNOWN"
        assert all(
            item["cancel_state"] == ExecutionState.UNKNOWN.value
            for response in (first, replay, second_command)
            for item in response["result"]["items"]
        )
        assert len(provider.cancelled) == 1
    finally:
        store.close()


@pytest.mark.unit
def test_unknown_submission_is_reported_without_attempting_cancel(tmp_path) -> None:
    provider = _SyntheticProvider(unknown_submit=True)
    store, service, provider, quota_calls = _service(tmp_path, provider=provider)
    scope = _wire_scope(run_id="run-1")
    try:
        submitted = _submit(service, "unknown-submit-order", scope=scope)
        assert submitted["result"]["record"]["state"] == ExecutionState.UNKNOWN.value

        result = _cancel(service, "unknown-submit-stop", scope=scope)
        assert result["ok"] is False
        assert result["result"]["outcome"] == "UNKNOWN"
        assert result["result"]["items"][0]["decision"] == "UNKNOWN"
        assert provider.cancelled == []
        assert quota_calls == []
    finally:
        store.close()


@pytest.mark.unit
def test_owner_cancel_rejects_nonservice_actor_and_crossuser_command_replay(tmp_path) -> None:
    store, service, provider, _quota_calls = _service(tmp_path)
    scope = _wire_scope(run_id="run-1")
    try:
        assert _submit(service, "cross-user-order", scope=scope)["ok"] is True
        denied = _cancel(
            service,
            "shared-command",
            scope=scope,
            identity=WireIdentity(WireRole.STRATEGY_CLIENT, _PRINCIPAL, _STRATEGY),
        )
        assert denied["code"] == "ROLE_NOT_AUTHORIZED"
        assert provider.cancelled == []

        first = _cancel(service, "shared-command", scope=scope, identity=_owner_identity("alice"))
        other_actor = _cancel(
            service, "shared-command", scope=scope, identity=_owner_identity("bob")
        )
        assert first["ok"] is True
        assert other_actor["code"] == "COMMAND_ID_CONFLICT"
        assert len(provider.cancelled) == 1
    finally:
        store.close()


@pytest.mark.unit
def test_owner_cancel_fails_closed_without_quota_and_rejects_extra_fields(tmp_path) -> None:
    store, service, provider, _quota_calls = _service(tmp_path, quota=False)
    scope = _wire_scope(run_id="run-1")
    try:
        assert _submit(service, "quota-order", scope=scope)["ok"] is True
        missing_quota = _cancel(service, "no-quota", scope=scope)
        assert missing_quota["code"] == "QUOTA_ADMISSION_UNAVAILABLE"
        assert missing_quota["result"]["outcome"] == "NO_EFFECT"
        assert provider.cancelled == []
    finally:
        store.close()

    quota_store, quota_service, quota_provider, _ = _service(tmp_path, quota=True)
    try:
        assert _submit(quota_service, "extra-field-order", scope=scope)["ok"] is True
        extra = _call(
            quota_service,
            OWNER_CANCEL_OPERATION,
            {"command_id": "extra", "account_ref": _ACCOUNT},
            scope=scope,
            identity=_owner_identity(),
        )
        assert extra["code"] == "INVALID_REQUEST"
        assert quota_provider.cancelled == []
    finally:
        quota_store.close()


@pytest.mark.unit
def test_historical_unattributed_order_is_never_assigned_to_a_run(tmp_path) -> None:
    from bt_api_execution.contracts import order_intent_from_payload

    store, service, provider, _quota_calls = _service(tmp_path)
    scope = _wire_scope(run_id="run-1")
    try:
        legacy_payload = _order_arguments("legacy-unbound-order")
        legacy_payload["scope"] = _EXECUTION_SCOPE.to_payload()
        legacy_intent = order_intent_from_payload(legacy_payload)
        order_facade, _ = service._facade_pair(_EXECUTION_SCOPE)
        legacy = order_facade.submit(legacy_intent, provider)
        assert legacy.state is ExecutionState.ACKED

        result = _cancel(service, "legacy-owner-stop", scope=scope)
        assert result["ok"] is True
        assert result["result"]["outcome"] == "NO_EFFECT"
        assert result["result"]["total_count"] == 0
        assert provider.cancelled == []
    finally:
        store.close()


@pytest.mark.unit
def test_owner_cancel_scope_scan_limit_fails_without_dispatch(tmp_path, monkeypatch) -> None:
    from bt_api_execution import store as store_module

    store, service, provider, _quota_calls = _service(tmp_path)
    scope = _wire_scope(run_id="run-1")
    try:
        assert _submit(service, "scan-order-1", scope=scope)["ok"] is True
        assert _submit(service, "scan-order-2", scope=scope)["ok"] is True
        monkeypatch.setattr(store_module, "OWNER_CANCEL_MAX_SCAN", 1)

        result = _cancel(service, "over-scan-limit", scope=scope)
        assert result["ok"] is False
        assert result["code"] == "OWNER_CANCEL_SCOPE_TOO_LARGE"
        assert result["result"]["outcome"] == "NO_EFFECT"
        assert provider.cancelled == []
    finally:
        store.close()


@pytest.mark.unit
def test_selection_counts_only_open_orders_and_reports_bounded_partial_result(
    tmp_path, monkeypatch
) -> None:
    from bt_api_execution import store as store_module

    store, service, provider, _quota_calls = _service(tmp_path)
    scope = _wire_scope(run_id="run-1")
    try:
        assert _submit(service, "filled-order", scope=scope)["ok"] is True
        assert _submit(service, "open-order", scope=scope)["ok"] is True
        facade, _ = service._facade_pair(_EXECUTION_SCOPE)
        facade.record_provider_observation(
            ProviderObservation(
                "filled-order",
                ExecutionState.FILLED,
                provider_order_id="provider-filled-order",
                filled_quantity=Decimal("2"),
                average_price=Decimal("50000"),
                cumulative_commission=Decimal("0"),
            )
        )

        result = _cancel(service, "open-only", scope=scope)
        assert result["result"]["outcome"] == "ACKNOWLEDGED"
        assert result["result"]["total_count"] == result["result"]["selected_count"] == 1
        assert [item["intent_id"] for item in result["result"]["items"]] == ["open-order"]
        assert len(provider.cancelled) == 1

        # Freeze a fresh bounded selection with a lower test cap to exercise
        # truthful omitted counts without creating a large synthetic dataset.
        assert _submit(service, "open-order-2", scope=scope)["ok"] is True
        monkeypatch.setattr(store_module, "OWNER_CANCEL_MAX_ITEMS", 1)
        partial = _cancel(service, "bounded-one", scope=scope)
        assert partial["result"]["outcome"] == "PARTIAL"
        assert partial["result"]["total_count"] == 2
        assert partial["result"]["selected_count"] == 1
        assert partial["result"]["omitted_count"] == 1
    finally:
        store.close()

