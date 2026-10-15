from __future__ import annotations

import json
from decimal import Decimal

import pytest
from bt_api_base.service_protocol import WireScope

from bt_api_execution import (
    ContractValidationError,
    ExecutionScope,
    OrderIntent,
    Side,
    SqliteExecutionStore,
)
from bt_api_execution.service_component import (
    SUPPORTED_OPERATIONS,
    ExecutionReadService,
    validate_service_options,
)


def _wire_scope(
    *,
    domain: str = "paper:demo",
    account: str = "account-a",
    strategy: str | None = "strategy-a",
    run_id: str | None = None,
    manual_book: str | None = None,
) -> WireScope:
    return WireScope(
        execution_domain_ref=domain,
        account_ref=account if manual_book is None else None,
        strategy_ref=strategy if manual_book is None else None,
        run_id=run_id,
        manual_book_ref=manual_book,
    )


def _execution_scope(*, account: str = "account-a", strategy: str = "strategy-a") -> ExecutionScope:
    return ExecutionScope("PAPER", "simulation", account, strategy, "20261014")


def _admit_record(
    store: SqliteExecutionStore,
    scope: ExecutionScope,
    intent_id: str,
    *,
    unknown: bool = False,
) -> None:
    lease = store.acquire_or_renew_lease(scope, f"writer-{scope.account_ref}")
    intent = OrderIntent.limit(
        intent_id=intent_id,
        scope=scope,
        signal_id=f"signal-{intent_id}",
        instrument="BTC-USDT",
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("50000"),
    )
    store.admit_intent(intent, writer_lease=lease)
    if not unknown:
        return
    store.activate_intent(intent_id, scope, writer_lease=lease)
    record, claimed = store.claim_for_dispatch(intent_id, scope, writer_lease=lease)
    assert record.state.value == "DISPATCHING"
    assert claimed is True
    store.mark_unknown(intent_id, scope, "provider_timeout", writer_lease=lease)


@pytest.mark.unit
def test_validate_service_options_matches_execution_roles_and_returns_immutable_copy() -> None:
    source = {"role": "account_owner", "provider_session_ref": "gateway-a"}

    normalized = validate_service_options(source, ("service.order_query", "service.owner_control"))
    source["provider_session_ref"] = "changed"

    assert dict(normalized) == {
        "role": "account_owner",
        "provider_session_ref": "gateway-a",
    }
    with pytest.raises(TypeError):
        normalized["role"] = "group_coordinator"  # type: ignore[index]

    owner_without_session = validate_service_options(
        {"role": "account_owner"}, ("service.order_entry",)
    )
    assert dict(owner_without_session) == {"role": "account_owner"}

    group = validate_service_options(
        {"role": "group_coordinator", "portfolio_ref": "portfolio-a"},
        ("service.order_group_entry",),
    )
    assert dict(group) == {"role": "group_coordinator", "portfolio_ref": "portfolio-a"}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("options", "ports"),
    [
        ({"role": "account_owner", "unexpected": True}, ("service.order_query",)),
        ({"role": "account_owner", "provider_session_ref": None}, ("service.order_query",)),
        ({"role": "account_owner"}, ("service.order_group_entry",)),
        ({"role": "group_coordinator"}, ("service.order_group_entry",)),
        (
            {"role": "group_coordinator", "portfolio_ref": "portfolio-a"},
            ("service.order_query",),
        ),
        (
            {"role": "group_coordinator", "portfolio_ref": "portfolio-a", "extra": "x"},
            ("service.order_group_entry",),
        ),
        ({"role": "account_owner"}, ()),
        ({"role": "account_owner"}, ("service.order_query", "service.order_query")),
        ({"role": "unknown_role"}, ("service.order_query",)),
    ],
)
def test_validate_service_options_rejects_unknown_keys_missing_refs_and_role_port_mismatch(
    options: dict[str, object], ports: tuple[str, ...]
) -> None:
    with pytest.raises(ContractValidationError):
        validate_service_options(options, ports)


@pytest.mark.unit
def test_approved_target_required_is_strict_server_option_and_write_only() -> None:
    enabled = validate_service_options(
        {"role": "account_owner", "approved_target_required": True},
        ("service.order_entry",),
    )
    disabled = validate_service_options(
        {"role": "account_owner", "approved_target_required": False},
        ("service.order_query",),
    )
    assert enabled["approved_target_required"] is True
    assert disabled["approved_target_required"] is False

    for invalid in (1, "true", None):
        with pytest.raises(ContractValidationError):
            validate_service_options(
                {"role": "account_owner", "approved_target_required": invalid},
                ("service.order_entry",),
            )

    with pytest.raises(ContractValidationError):
        validate_service_options(
            {"role": "account_owner", "approved_target_required": True},
            ("service.order_query",),
        )


@pytest.mark.unit
def test_orders_query_reads_json_dto_and_preserves_unknown_state_without_writes(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    execution_scope = _execution_scope()
    wire_scope = _wire_scope()
    _admit_record(store, execution_scope, "order-unknown", unknown=True)
    before = store.read_outbox(scope=execution_scope)
    service = ExecutionReadService(store, {wire_scope: execution_scope})

    try:
        response = service.handle(
            "orders.query",
            principal_ref="principal:" + ("p" * 180),
            scope=wire_scope,
            arguments={"intent_id": "order-unknown"},
        )

        assert frozenset({"orders.query"}) == SUPPORTED_OPERATIONS
        assert response["ok"] is True
        assert response["code"] == "OK"
        assert response["result"]["found"] is True
        dto = response["result"]["record"]
        assert dto["intent_id"] == "order-unknown"
        assert dto["scope_key"] == execution_scope.key
        assert dto["state"] == "UNKNOWN"
        assert dto["unknown_reason"] == "provider_timeout"
        assert dto["review_required"] is True
        assert dto["filled_quantity"] == "0"
        assert dto["average_price"] is None
        assert dto["is_terminal"] is False
        json.dumps(response)
        assert store.read_outbox(scope=execution_scope) == before
    finally:
        store.close()


@pytest.mark.unit
def test_cross_scope_intent_lookup_is_not_found_and_unmapped_scope_denial_is_constant(
    tmp_path,
) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    account_a = _execution_scope(account="account-a", strategy="strategy-a")
    account_b = _execution_scope(account="account-b", strategy="strategy-b")
    wire_a = _wire_scope(account="account-a", strategy="strategy-a")
    wire_b = _wire_scope(account="account-b", strategy="strategy-b")
    _admit_record(store, account_b, "private-order")
    service = ExecutionReadService(store, {wire_a: account_a})

    try:
        query_a = service.handle(
            "orders.query",
            principal_ref="principal-a",
            scope=wire_a,
            arguments={"intent_id": "private-order"},
        )
        assert query_a == {
            "ok": True,
            "code": "OK",
            "result": {"found": False, "record": None},
        }

        denied_existing = service.handle(
            "orders.query",
            principal_ref="principal-a",
            scope=wire_b,
            arguments={"intent_id": "private-order"},
        )
        denied_missing = service.handle(
            "orders.query",
            principal_ref="principal-a",
            scope=wire_b,
            arguments={"intent_id": "does-not-exist"},
        )
        assert (
            denied_existing
            == denied_missing
            == {
                "ok": False,
                "code": "SCOPE_DENIED",
                "result": {"found": False, "record": None},
            }
        )
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "scope",
    [
        _wire_scope(domain="paper:other"),
        _wire_scope(account="account-b"),
        _wire_scope(strategy="strategy-b"),
    ],
)
def test_scope_claim_injection_is_denied(scope: WireScope, tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    configured_scope = _execution_scope()
    service = ExecutionReadService(store, {_wire_scope(): configured_scope})
    try:
        response = service.handle(
            "orders.query",
            principal_ref="principal-a",
            scope=scope,
            arguments={"intent_id": "order-a"},
        )
        assert response["code"] == "SCOPE_DENIED"
        assert response["result"] == {"found": False, "record": None}
    finally:
        store.close()


@pytest.mark.unit
def test_run_wildcard_requires_template_none_and_same_strategy(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    execution_scope = _execution_scope()
    template = _wire_scope(strategy="strategy-a", run_id=None)
    _admit_record(store, execution_scope, "run-order")
    service = ExecutionReadService(store, {template: execution_scope})
    try:
        matching_run = service.handle(
            "orders.query",
            principal_ref="principal-a",
            scope=_wire_scope(strategy="strategy-a", run_id="run-42"),
            arguments={"intent_id": "run-order"},
        )
        other_strategy = service.handle(
            "orders.query",
            principal_ref="principal-a",
            scope=_wire_scope(strategy="strategy-b", run_id="run-42"),
            arguments={"intent_id": "run-order"},
        )
        assert matching_run["result"]["found"] is True
        assert other_strategy["code"] == "SCOPE_DENIED"
    finally:
        store.close()


@pytest.mark.unit
def test_manual_book_scope_requires_an_explicit_execution_scope_mapping(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    manual_scope = ExecutionScope("PAPER", "simulation", "account-a", "manual-book-owner")
    manual_template = _wire_scope(manual_book="book-a")
    _admit_record(store, manual_scope, "manual-order")
    service = ExecutionReadService(store, {manual_template: manual_scope})
    try:
        response = service.handle(
            "orders.query",
            principal_ref="principal-a",
            scope=manual_template,
            arguments={"intent_id": "manual-order"},
        )
        no_implicit_mapping = service.handle(
            "orders.query",
            principal_ref="principal-a",
            scope=_wire_scope(manual_book="book-b"),
            arguments={"intent_id": "manual-order"},
        )
        assert response["result"]["record"]["scope_key"] == manual_scope.key
        assert no_implicit_mapping["code"] == "SCOPE_DENIED"
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"intent_id": "order-a", "scope": {"account_ref": "account-b"}},
        {"intent_id": ""},
        {"intent_id": 1},
    ],
)
def test_orders_query_rejects_scope_or_identifier_injection(arguments, tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    service = ExecutionReadService(store, {_wire_scope(): _execution_scope()})
    try:
        with pytest.raises(ContractValidationError):
            service.handle(
                "orders.query",
                principal_ref="principal-a",
                scope=_wire_scope(),
                arguments=arguments,
            )
    finally:
        store.close()


@pytest.mark.unit
def test_service_rejects_noncanonical_wire_scope_objects(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    service = ExecutionReadService(store, {_wire_scope(): _execution_scope()})
    try:
        response = service.handle(
            "orders.query",
            principal_ref="principal-a",
            scope={"execution_domain_ref": "paper:demo"},  # type: ignore[arg-type]
            arguments={"intent_id": "order-a"},
        )
        assert response["code"] == "SCOPE_DENIED"
    finally:
        store.close()


@pytest.mark.unit
def test_service_constructor_requires_canonical_wire_scope_templates(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    try:
        with pytest.raises(ContractValidationError):
            ExecutionReadService(
                store,
                {"account-a": _execution_scope()},  # type: ignore[dict-item]
            )
    finally:
        store.close()


@pytest.mark.unit
def test_handle_rejects_invalid_trusted_principal_text(tmp_path) -> None:
    store = SqliteExecutionStore(tmp_path / "execution.sqlite3")
    service = ExecutionReadService(store, {_wire_scope(): _execution_scope()})
    try:
        with pytest.raises(ContractValidationError):
            service.handle(
                "orders.query",
                principal_ref=" principal-a",
                scope=_wire_scope(),
                arguments={"intent_id": "order-a"},
            )
    finally:
        store.close()
