"""Focused behavior tests for the authenticated execution order-entry service."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest
from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from bt_api_execution import (
    CancelObservation,
    ContractValidationError,
    ExecutionScope,
    ExecutionState,
    ManagedExecutionFacade,
    OrderIntent,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
)
from bt_api_execution.service_order_entry import (
    CANCEL_OPERATION,
    QUERY_OPERATION,
    SUBMIT_OPERATION,
    SUPPORTED_OPERATIONS,
    ExecutionOrderEntryService,
)

_DOMAIN = "paper:demo"
_ACCOUNT = "account-a"
_STRATEGY = "strategy-a"
_PRINCIPAL = "curve-strategy-principal"


@dataclass
class _Permit:
    permit_id: str


class _Gate:
    """Minimal admission gate; denial is durable rather than exceptional."""

    def __init__(self, *, deny: bool = False) -> None:
        self.deny = deny
        self.reserved: list[str] = []
        self.claimed: list[tuple[str, str]] = []

    def reserve(self, intent: OrderIntent) -> _Permit:
        if self.deny:
            from bt_api_execution import AdmissionDenied

            raise AdmissionDenied()
        self.reserved.append(intent.intent_id)
        return _Permit(f"permit-{intent.intent_id}")

    def validate(self, permit_id: str, intent: OrderIntent) -> _Permit:
        return _Permit(permit_id)

    def claim_for_dispatch(self, permit_id: str, intent: OrderIntent) -> _Permit:
        self.claimed.append((permit_id, intent.intent_id))
        return _Permit(permit_id)

    def release(self, permit_id: str, reason: str) -> None:
        return None

    def settle(self, permit_id: str) -> None:
        return None


class _Provider:
    """Synthetic provider adapter exposing both managed dispatch ports."""

    def __init__(self, *, accept: bool = True, raise_on_submit: bool = False) -> None:
        self.accept = accept
        self.raise_on_submit = raise_on_submit
        self.submitted: list[str] = []
        self.cancelled: list[str] = []
        self.next_order_id = "provider-order-1"

    def submit(self, intent: OrderIntent) -> ProviderObservation:
        self.submitted.append(intent.intent_id)
        if self.raise_on_submit:
            raise TimeoutError("synthetic provider timeout")
        if not self.accept:
            return ProviderObservation.rejected(intent.intent_id, "synthetic_reject")
        return ProviderObservation.accepted(intent.intent_id, self.next_order_id)

    def cancel(self, intent) -> CancelObservation:
        self.cancelled.append(intent.cancel_id)
        return CancelObservation.accepted(
            intent.cancel_id, intent.target_intent_id, intent.provider_order_id
        )


class _FillDuringCancelProvider(_Provider):
    """Inject an order fill while the synthetic cancel call is in flight."""

    def __init__(
        self,
        order_facade: ManagedExecutionFacade,
        *,
        fill_state: ExecutionState,
        filled_quantity: Decimal,
        average_price: Decimal,
        cumulative_commission: Decimal,
        cancel_state: ExecutionState,
    ) -> None:
        super().__init__()
        self.order_facade = order_facade
        self.fill_state = fill_state
        self.filled_quantity = filled_quantity
        self.average_price = average_price
        self.cumulative_commission = cumulative_commission
        self.cancel_state = cancel_state

    def cancel(self, intent) -> CancelObservation:
        self.cancelled.append(intent.cancel_id)
        self.order_facade.record_provider_observation(
            ProviderObservation(
                intent.target_intent_id,
                self.fill_state,
                provider_order_id=intent.provider_order_id,
                filled_quantity=self.filled_quantity,
                average_price=self.average_price,
                cumulative_commission=self.cumulative_commission,
            )
        )
        if self.cancel_state is ExecutionState.CANCELLED:
            return CancelObservation.cancelled(
                intent.cancel_id, intent.target_intent_id, intent.provider_order_id
            )
        return CancelObservation.rejected(
            intent.cancel_id,
            intent.target_intent_id,
            intent.provider_order_id,
            "already_filled",
        )


def _execution_scope() -> ExecutionScope:
    return ExecutionScope("PAPER", "simulation", _ACCOUNT, _STRATEGY, "20261014")


def _wire_scope(*, run_id: str | None = "run-1", account: str = _ACCOUNT) -> WireScope:
    return WireScope(_DOMAIN, account_ref=account, strategy_ref=_STRATEGY, run_id=run_id)


def _identity(*, principal: str = _PRINCIPAL, strategy: str = _STRATEGY) -> WireIdentity:
    return WireIdentity(WireRole.STRATEGY_CLIENT, principal, strategy)


def _service(
    tmp_path,
    provider: _Provider,
    *,
    gate: _Gate | None = None,
    allow_unprotected: bool = True,
    writer_id: str = "execution-order-entry",
):
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    service = ExecutionOrderEntryService(
        store,
        {_wire_scope(run_id=None): _execution_scope()},
        provider,
        admission_gate=gate,
        allow_unprotected=allow_unprotected,
        writer_id=writer_id,
    )
    return store, service


def _submit_arguments(intent_id: str = "intent-1", *, price: str = "50000") -> dict[str, object]:
    return {
        "intent_id": intent_id,
        "signal_id": f"signal-{intent_id}",
        "instrument": "BTC-USDT",
        "side": "BUY",
        "position_effect": "OPEN",
        "order_type": "LIMIT",
        "quantity": "2",
        "price": price,
        "time_in_force": "GTC",
        "reduce_only": False,
        "metadata_version": "metadata-v1",
        "deadline_ns": None,
        "tags": {},
    }


def _call(service, operation: str, arguments: dict[str, object], *, scope: WireScope | None = None):
    return service.handle(
        operation,
        principal_ref=_PRINCIPAL,
        identity=_identity(),
        scope=_wire_scope() if scope is None else scope,
        arguments=arguments,
    )


@pytest.mark.unit
def test_operations_cover_the_account_owner_write_and_read_ports() -> None:
    assert frozenset({SUBMIT_OPERATION, CANCEL_OPERATION, QUERY_OPERATION}) == SUPPORTED_OPERATIONS


@pytest.mark.unit
def test_submit_dispatches_once_and_a_duplicate_returns_the_same_record(tmp_path) -> None:
    provider = _Provider()
    gate = _Gate()
    store, service = _service(tmp_path, provider, gate=gate)
    try:
        first = _call(service, SUBMIT_OPERATION, _submit_arguments())
        second = _call(service, SUBMIT_OPERATION, _submit_arguments())

        assert first["ok"] is True and first["code"] == "OK"
        assert first["result"]["record"]["state"] == ExecutionState.ACKED.value
        assert first["result"]["record"]["provider_order_id"] == "provider-order-1"
        assert second == first
        assert provider.submitted == ["intent-1"]
        assert gate.reserved == ["intent-1"]
        assert gate.claimed == [("permit-intent-1", "intent-1")]
    finally:
        store.close()


@pytest.mark.unit
def test_same_intent_id_with_different_content_conflicts_without_dispatch(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        _call(service, SUBMIT_OPERATION, _submit_arguments())
        conflict = _call(service, SUBMIT_OPERATION, _submit_arguments(price="51000"))

        assert conflict["ok"] is False
        assert conflict["code"] == "INTENT_CONFLICT"
        assert conflict["result"]["found"] is True
        assert conflict["result"]["record"]["state"] == ExecutionState.ACKED.value
        assert provider.submitted == ["intent-1"]
    finally:
        store.close()


@pytest.mark.unit
def test_unknown_provider_outcome_is_recorded_and_never_redispatched(tmp_path) -> None:
    provider = _Provider(raise_on_submit=True)
    store, service = _service(tmp_path, provider)
    try:
        first = _call(service, SUBMIT_OPERATION, _submit_arguments())

        assert first["ok"] is True
        assert first["result"]["record"]["state"] == ExecutionState.UNKNOWN.value
        assert first["result"]["record"]["unknown_reason"]

        repeat = _call(service, SUBMIT_OPERATION, _submit_arguments())
        assert repeat["result"]["record"]["state"] == ExecutionState.UNKNOWN.value
        assert provider.submitted == ["intent-1"]
    finally:
        store.close()


@pytest.mark.unit
def test_rejected_provider_evidence_is_reported_without_a_second_dispatch(tmp_path) -> None:
    provider = _Provider(accept=False)
    store, service = _service(tmp_path, provider)
    try:
        reply = _call(service, SUBMIT_OPERATION, _submit_arguments())

        assert reply["ok"] is True
        assert reply["result"]["record"]["state"] == ExecutionState.REJECTED.value
        assert provider.submitted == ["intent-1"]
    finally:
        store.close()


@pytest.mark.unit
def test_client_supplied_scope_is_refused_before_any_dispatch(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        arguments = _submit_arguments()
        arguments["scope"] = _execution_scope().to_payload()

        reply = _call(service, SUBMIT_OPERATION, arguments)

        assert reply == {
            "ok": False,
            "code": "INVALID_REQUEST",
            "result": {"found": False, "record": None},
        }
        assert provider.submitted == []
    finally:
        store.close()


@pytest.mark.unit
def test_unregistered_scope_is_denied_without_dispatch(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        reply = _call(
            service,
            SUBMIT_OPERATION,
            _submit_arguments(),
            scope=_wire_scope(account="account-b"),
        )

        assert reply["ok"] is False
        assert reply["code"] == "SCOPE_DENIED"
        assert provider.submitted == []
    finally:
        store.close()


@pytest.mark.unit
def test_run_wildcard_template_admits_another_run_of_the_same_strategy(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        reply = _call(
            service,
            SUBMIT_OPERATION,
            _submit_arguments(),
            scope=_wire_scope(run_id="run-2"),
        )

        assert reply["ok"] is True
        assert provider.submitted == ["intent-1"]
    finally:
        store.close()


@pytest.mark.unit
def test_missing_admission_gate_fails_closed_before_the_provider(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider, allow_unprotected=False)
    try:
        reply = _call(service, SUBMIT_OPERATION, _submit_arguments())

        assert reply["ok"] is False
        assert reply["code"] == "ADMISSION_REQUIRED"
        assert reply["result"]["record"]["state"] == ExecutionState.PENDING_ADMISSION.value
        assert provider.submitted == []
    finally:
        store.close()


@pytest.mark.unit
def test_denied_admission_is_durable_and_calls_no_provider(tmp_path) -> None:
    provider = _Provider()
    gate = _Gate(deny=True)
    store, service = _service(tmp_path, provider, gate=gate, allow_unprotected=False)
    try:
        reply = _call(service, SUBMIT_OPERATION, _submit_arguments())

        assert reply["ok"] is True
        assert reply["result"]["record"]["state"] == ExecutionState.REJECTED.value
        assert provider.submitted == []
        assert gate.reserved == []
    finally:
        store.close()


@pytest.mark.unit
def test_cancel_requires_an_evidenced_target_and_dispatches_once(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        unknown = _call(
            service,
            CANCEL_OPERATION,
            {
                "cancel_id": "cancel-1",
                "target_intent_id": "intent-1",
                "provider_order_id": "provider-order-1",
                "metadata_version": "metadata-v1",
                "tags": {},
            },
        )
        assert unknown["ok"] is False
        assert unknown["code"] == "CANCEL_TARGET_UNKNOWN"
        assert provider.cancelled == []

        _call(service, SUBMIT_OPERATION, _submit_arguments())
        first = _call(
            service,
            CANCEL_OPERATION,
            {
                "cancel_id": "cancel-1",
                "target_intent_id": "intent-1",
                "provider_order_id": "provider-order-1",
                "metadata_version": "metadata-v1",
                "tags": {},
            },
        )
        repeat = _call(
            service,
            CANCEL_OPERATION,
            {
                "cancel_id": "cancel-1",
                "target_intent_id": "intent-1",
                "provider_order_id": "provider-order-1",
                "metadata_version": "metadata-v1",
                "tags": {},
            },
        )

        assert first["ok"] is True
        assert first["result"]["record"]["state"] == ExecutionState.ACKED.value
        assert repeat == first
        assert provider.cancelled == ["cancel-1"]
    finally:
        store.close()


@pytest.mark.unit
def test_cancel_winning_after_a_partial_fill_preserves_fill_and_fee(tmp_path) -> None:
    scope = _execution_scope()
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    order_facade = ManagedExecutionFacade(
        store,
        scope,
        writer_id="execution-order-entry",
        allow_unprotected=True,
    )
    provider = _FillDuringCancelProvider(
        order_facade,
        fill_state=ExecutionState.PARTIALLY_FILLED,
        filled_quantity=Decimal("0.75"),
        average_price=Decimal("50010"),
        cumulative_commission=Decimal("0.25"),
        cancel_state=ExecutionState.CANCELLED,
    )
    service = ExecutionOrderEntryService(
        store,
        {_wire_scope(run_id=None): scope},
        provider,
        allow_unprotected=True,
        writer_id="execution-order-entry",
    )
    cancel_arguments = {
        "cancel_id": "cancel-race-partial",
        "target_intent_id": "intent-1",
        "provider_order_id": "provider-order-1",
        "metadata_version": "metadata-v1",
        "tags": {},
    }
    try:
        submitted = _call(service, SUBMIT_OPERATION, _submit_arguments())
        cancelled = _call(service, CANCEL_OPERATION, cancel_arguments)
        stored = store.get("intent-1", scope=scope)

        assert submitted["result"]["record"]["state"] == ExecutionState.ACKED.value
        assert cancelled["ok"] is True
        assert cancelled["result"]["record"]["state"] == ExecutionState.CANCELLED.value
        assert stored is not None
        assert stored.state is ExecutionState.CANCELLED
        assert stored.filled_quantity == Decimal("0.75")
        assert stored.average_price == Decimal("50010")
        assert stored.cumulative_commission == Decimal("0.25")

        cancel_replay = _call(service, CANCEL_OPERATION, cancel_arguments)
        submit_replay = _call(service, SUBMIT_OPERATION, _submit_arguments())
        assert cancel_replay == cancelled
        assert submit_replay["result"]["record"]["state"] == ExecutionState.CANCELLED.value
        assert provider.submitted == ["intent-1"]
        assert provider.cancelled == ["cancel-race-partial"]
    finally:
        store.close()


@pytest.mark.unit
def test_full_fill_during_cancel_rejects_cancel_without_downgrading_order(tmp_path) -> None:
    scope = _execution_scope()
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    order_facade = ManagedExecutionFacade(
        store,
        scope,
        writer_id="execution-order-entry",
        allow_unprotected=True,
    )
    provider = _FillDuringCancelProvider(
        order_facade,
        fill_state=ExecutionState.FILLED,
        filled_quantity=Decimal("2"),
        average_price=Decimal("50100"),
        cumulative_commission=Decimal("0.75"),
        cancel_state=ExecutionState.REJECTED,
    )
    service = ExecutionOrderEntryService(
        store,
        {_wire_scope(run_id=None): scope},
        provider,
        allow_unprotected=True,
        writer_id="execution-order-entry",
    )
    cancel_arguments = {
        "cancel_id": "cancel-race-filled",
        "target_intent_id": "intent-1",
        "provider_order_id": "provider-order-1",
        "metadata_version": "metadata-v1",
        "tags": {},
    }
    try:
        submitted = _call(service, SUBMIT_OPERATION, _submit_arguments())
        cancel_rejected = _call(service, CANCEL_OPERATION, cancel_arguments)
        stored = store.get("intent-1", scope=scope)

        assert submitted["result"]["record"]["state"] == ExecutionState.ACKED.value
        assert cancel_rejected["ok"] is True
        assert cancel_rejected["result"]["record"]["state"] == ExecutionState.REJECTED.value
        assert stored is not None
        assert stored.state is ExecutionState.FILLED
        assert stored.filled_quantity == Decimal("2")
        assert stored.average_price == Decimal("50100")
        assert stored.cumulative_commission == Decimal("0.75")

        cancel_replay = _call(service, CANCEL_OPERATION, cancel_arguments)
        submit_replay = _call(service, SUBMIT_OPERATION, _submit_arguments())
        assert cancel_replay == cancel_rejected
        assert submit_replay["result"]["record"]["state"] == ExecutionState.FILLED.value
        assert provider.submitted == ["intent-1"]
        assert provider.cancelled == ["cancel-race-filled"]
    finally:
        store.close()


@pytest.mark.unit
def test_cancel_with_a_mismatched_provider_order_id_is_refused(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        _call(service, SUBMIT_OPERATION, _submit_arguments())

        reply = _call(
            service,
            CANCEL_OPERATION,
            {
                "cancel_id": "cancel-1",
                "target_intent_id": "intent-1",
                "provider_order_id": "provider-order-9",
                "metadata_version": "metadata-v1",
                "tags": {},
            },
        )

        assert reply["ok"] is False
        assert reply["code"] == "CANCEL_TARGET_UNKNOWN"
        assert provider.cancelled == []
    finally:
        store.close()


@pytest.mark.unit
def test_cancel_cannot_target_another_scope(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        _call(service, SUBMIT_OPERATION, _submit_arguments())

        reply = _call(
            service,
            CANCEL_OPERATION,
            {
                "cancel_id": "cancel-1",
                "target_intent_id": "intent-1",
                "provider_order_id": "provider-order-1",
                "metadata_version": "metadata-v1",
                "tags": {},
            },
            scope=_wire_scope(account="account-b"),
        )

        assert reply["ok"] is False
        assert reply["code"] == "SCOPE_DENIED"
        assert provider.cancelled == []
    finally:
        store.close()


@pytest.mark.unit
def test_query_stays_inside_the_authorized_scope(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        _call(service, SUBMIT_OPERATION, _submit_arguments())

        found = _call(service, QUERY_OPERATION, {"intent_id": "intent-1"})
        assert found["ok"] is True
        assert found["result"]["found"] is True
        assert found["result"]["record"]["state"] == ExecutionState.ACKED.value

        denied = _call(
            service, QUERY_OPERATION, {"intent_id": "intent-1"}, scope=_wire_scope(account="b")
        )
        assert denied["ok"] is False
        assert denied["code"] == "SCOPE_DENIED"
    finally:
        store.close()


@pytest.mark.unit
def test_identity_principal_mismatch_is_a_contract_error(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        with pytest.raises(ContractValidationError):
            service.handle(
                SUBMIT_OPERATION,
                principal_ref=_PRINCIPAL,
                identity=_identity(principal="another-principal"),
                scope=_wire_scope(),
                arguments=_submit_arguments(),
            )
        assert provider.submitted == []
    finally:
        store.close()


@pytest.mark.unit
def test_unknown_operation_is_refused_without_touching_the_store(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        reply = _call(service, "orders.replace", {"intent_id": "intent-1"})

        assert reply == {"ok": False, "code": "UNSUPPORTED_OPERATION", "result": {}}
        assert provider.submitted == []
    finally:
        store.close()


@pytest.mark.unit
def test_foreign_writer_lease_reports_the_durable_state(tmp_path) -> None:
    provider = _Provider()
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    other = ManagedExecutionFacade(store, _execution_scope(), writer_id="other-owner")
    service = ExecutionOrderEntryService(
        store,
        {_wire_scope(run_id=None): _execution_scope()},
        provider,
        allow_unprotected=True,
        writer_id="execution-order-entry",
    )
    try:
        other.acquire_writer_lease()

        reply = _call(service, SUBMIT_OPERATION, _submit_arguments())

        assert reply["ok"] is False
        assert reply["code"] == "OWNER_LEASE_UNAVAILABLE"
        assert reply["result"]["found"] is False
        assert provider.submitted == []
    finally:
        other.close()
        store.close()


@pytest.mark.unit
def test_invalid_setup_is_rejected_before_serving(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _Provider()
    targets = {_wire_scope(run_id=None): _execution_scope()}
    try:
        with pytest.raises(ContractValidationError):
            ExecutionOrderEntryService("store", targets, provider)
        with pytest.raises(ContractValidationError):
            ExecutionOrderEntryService(store, {}, provider)
        with pytest.raises(ContractValidationError):
            ExecutionOrderEntryService(store, targets, object())
        with pytest.raises(ContractValidationError):
            ExecutionOrderEntryService(store, targets, lambda _intent: None)
        with pytest.raises(ContractValidationError):
            ExecutionOrderEntryService(store, targets, provider, writer_id="  ")
        with pytest.raises(ContractValidationError):
            ExecutionOrderEntryService(store, targets, provider, lease_ttl_ns=0)
        with pytest.raises(ContractValidationError):
            ExecutionOrderEntryService(store, targets, provider, allow_unprotected="yes")
    finally:
        store.close()


@pytest.mark.unit
def test_targets_must_use_canonical_wire_and_execution_scopes(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    provider = _Provider()
    try:
        with pytest.raises(ContractValidationError):
            ExecutionOrderEntryService(store, {"not-a-wire-scope": _execution_scope()}, provider)
        with pytest.raises(ContractValidationError):
            ExecutionOrderEntryService(store, {_wire_scope(): "not-an-execution-scope"}, provider)
    finally:
        store.close()


@pytest.mark.unit
def test_dispatch_port_may_be_a_plain_callable_only_when_it_can_cancel(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    targets = {_wire_scope(run_id=None): _execution_scope()}

    class _Both:
        def submit(self, intent: OrderIntent) -> ProviderObservation:
            return ProviderObservation.accepted(intent.intent_id, "provider-order-1")

        def cancel(self, intent) -> CancelObservation:
            return CancelObservation.accepted(
                intent.cancel_id, intent.target_intent_id, intent.provider_order_id
            )

    try:
        assert ExecutionOrderEntryService(store, targets, _Both()) is not None
        with pytest.raises(ContractValidationError):
            ExecutionOrderEntryService(store, targets, lambda _intent: None)
    finally:
        store.close()


@pytest.mark.unit
def test_submit_quantity_and_side_are_validated_by_the_contract(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        bad_side = _submit_arguments()
        bad_side["side"] = "sideways"
        assert _call(service, SUBMIT_OPERATION, bad_side)["code"] == "INVALID_REQUEST"

        bad_quantity = _submit_arguments()
        bad_quantity["quantity"] = "-1"
        assert _call(service, SUBMIT_OPERATION, bad_quantity)["code"] == "INVALID_REQUEST"

        market_with_price = _submit_arguments()
        market_with_price["order_type"] = "MARKET"
        assert _call(service, SUBMIT_OPERATION, market_with_price)["code"] == "INVALID_REQUEST"

        assert provider.submitted == []
    finally:
        store.close()


@pytest.mark.unit
def test_two_scopes_share_one_service_without_crossing_attribution(tmp_path) -> None:
    provider = _Provider()
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    scope_a = ExecutionScope("PAPER", "simulation", _ACCOUNT, _STRATEGY, "20261014")
    scope_b = ExecutionScope("PAPER", "simulation", _ACCOUNT, "strategy-b", "20261014")
    service = ExecutionOrderEntryService(
        store,
        {
            WireScope(_DOMAIN, account_ref=_ACCOUNT, strategy_ref=_STRATEGY): scope_a,
            WireScope(_DOMAIN, account_ref=_ACCOUNT, strategy_ref="strategy-b"): scope_b,
        },
        provider,
        allow_unprotected=True,
    )
    try:
        first = _call(service, SUBMIT_OPERATION, _submit_arguments("intent-a"))
        second = service.handle(
            SUBMIT_OPERATION,
            principal_ref=_PRINCIPAL,
            identity=WireIdentity(WireRole.STRATEGY_CLIENT, _PRINCIPAL, "strategy-b"),
            scope=WireScope(_DOMAIN, account_ref=_ACCOUNT, strategy_ref="strategy-b"),
            arguments=_submit_arguments("intent-a"),
        )

        assert first["result"]["record"]["scope_key"] == scope_a.key
        assert second["result"]["record"]["scope_key"] == scope_b.key
        assert provider.submitted == ["intent-a", "intent-a"]
    finally:
        store.close()


@pytest.mark.unit
def test_submitted_intent_keeps_trusted_attribution(tmp_path) -> None:
    provider = _Provider()
    store, service = _service(tmp_path, provider)
    try:
        _call(service, SUBMIT_OPERATION, _submit_arguments())

        intent = store.get_intent("intent-1", scope=_execution_scope())
        assert intent is not None
        assert intent.scope == _execution_scope()
        assert intent.quantity == Decimal("2")
        assert intent.side is Side.BUY
    finally:
        store.close()
