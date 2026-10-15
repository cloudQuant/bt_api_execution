"""Read-only authenticated service boundary for managed execution records.

This adapter borrows an already-open execution store. It does not acquire a
writer lease, create the store, admit orders, or call provider code. Scope
claims are resolved only through an explicit trusted ``WireScope`` to
``ExecutionScope`` mapping supplied by the composition root.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from bt_api_base.service_protocol import WireScope

from .contracts import ExecutionScope
from .errors import ContractValidationError
from .store import ExecutionRecord, SqliteExecutionStore

if TYPE_CHECKING:
    from .group_contracts import GroupLegAuthorization

SUPPORTED_OPERATIONS = frozenset({"orders.query"})

_REFERENCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_ACCOUNT_OWNER_PORTS = frozenset(
    {"service.order_entry", "service.order_query", "service.owner_control"}
)
_GROUP_COORDINATOR_PORTS = frozenset({"service.order_group_entry"})
_WIRE_SCOPE_FIELDS = (
    "execution_domain_ref",
    "account_ref",
    "account_refs",
    "strategy_ref",
    "manual_book_ref",
    "portfolio_ref",
    "group_id",
    "leg_id",
)


def validate_service_options(
    options: Mapping[str, object], bind_ports: tuple[str, ...]
) -> Mapping[str, str]:
    """Validate the closed execution component role/port configuration.

    The accepted role and port pairs mirror the parent SDK service registry.
    The result is detached and immutable so later mutation of the supplied
    mapping cannot change the validated configuration.
    """

    if not isinstance(options, Mapping):
        raise ContractValidationError("execution service options must be a mapping")
    if type(bind_ports) is not tuple or not bind_ports:
        raise ContractValidationError("execution service bind_ports must be a non-empty tuple")
    if any(type(port) is not str for port in bind_ports) or len(set(bind_ports)) != len(bind_ports):
        raise ContractValidationError("invalid execution service bind_ports")

    keys = tuple(options.keys())
    if any(type(key) is not str for key in keys):
        raise ContractValidationError("invalid execution service option key")
    role = options.get("role")
    if type(role) is not str:
        raise ContractValidationError("execution service role is required")

    if role == "account_owner":
        if set(keys) - {"role", "provider_session_ref"}:
            raise ContractValidationError("unknown account_owner service option")
        if any(port not in _ACCOUNT_OWNER_PORTS for port in bind_ports):
            raise ContractValidationError("account_owner cannot bind the requested port")
        result = {"role": role}
        if "provider_session_ref" in options:
            provider_session_ref = options["provider_session_ref"]
            if not _is_reference(provider_session_ref):
                raise ContractValidationError("invalid provider_session_ref")
            result["provider_session_ref"] = provider_session_ref
        return MappingProxyType(result)

    if role == "group_coordinator":
        if set(keys) != {"role", "portfolio_ref"}:
            raise ContractValidationError("group_coordinator requires only role and portfolio_ref")
        if any(port not in _GROUP_COORDINATOR_PORTS for port in bind_ports):
            raise ContractValidationError("group_coordinator cannot bind the requested port")
        portfolio_ref = options["portfolio_ref"]
        if not _is_reference(portfolio_ref):
            raise ContractValidationError("invalid portfolio_ref")
        return MappingProxyType({"role": role, "portfolio_ref": portfolio_ref})

    raise ContractValidationError("unknown execution service role")


class ExecutionReadService:
    """Serve one exact ``orders.query`` read through trusted scope templates.

    ``store`` remains owned by the caller. ``targets`` is a trusted mapping
    from canonical base ``WireScope`` instances to durable execution scopes.
    Manual-book scopes therefore require an explicit target entry; the service
    never invents a strategy identity from a client claim.
    """

    SUPPORTED_OPERATIONS = SUPPORTED_OPERATIONS

    def __init__(
        self,
        store: SqliteExecutionStore,
        targets: Mapping[WireScope, ExecutionScope],
    ) -> None:
        if type(store) is not SqliteExecutionStore:
            raise ContractValidationError("execution read service requires a borrowed SQLite store")
        if not isinstance(targets, Mapping) or not targets:
            raise ContractValidationError("execution read service targets are required")

        copied: dict[WireScope, ExecutionScope] = {}
        for template, execution_scope in targets.items():
            if type(template) is not WireScope:
                raise ContractValidationError("targets must use canonical WireScope keys")
            if type(execution_scope) is not ExecutionScope:
                raise ContractValidationError("targets must map to ExecutionScope values")
            copied[template] = execution_scope

        self._store = store
        self._targets = MappingProxyType(copied)

    def handle(
        self,
        operation: str,
        *,
        principal_ref: str,
        scope: WireScope,
        arguments: Mapping[str, object],
    ) -> Mapping[str, Any]:
        """Handle a service request and return only JSON-compatible values."""

        if not _is_principal_ref(principal_ref):
            raise ContractValidationError("invalid trusted principal_ref")
        if type(operation) is not str or operation not in self.SUPPORTED_OPERATIONS:
            return _reply(False, "UNSUPPORTED_OPERATION", {})
        intent_id = _query_intent_id(arguments)
        if type(scope) is not WireScope:
            return _scope_denied()

        execution_scope = self._resolve_scope(scope)
        if execution_scope is None:
            return _scope_denied()

        record = self._store.get(intent_id, scope=execution_scope)
        if record is None:
            return _reply(True, "OK", {"found": False, "record": None})
        if type(record) is not ExecutionRecord or record.scope_key != execution_scope.key:
            raise ContractValidationError(
                "execution store returned a record outside the query scope"
            )
        return _reply(
            True,
            "OK",
            {"found": True, "record": _stored_record_dto(self._store, record, execution_scope)},
        )

    def _resolve_scope(self, requested: WireScope) -> ExecutionScope | None:
        exact = self._targets.get(requested)
        if exact is not None:
            return exact

        matches = [
            execution_scope
            for template, execution_scope in self._targets.items()
            if _matches_run_wildcard(template, requested)
        ]
        if len(matches) > 1:
            raise ContractValidationError("execution scope template is ambiguous")
        return matches[0] if matches else None


def _matches_run_wildcard(template: WireScope, requested: WireScope) -> bool:
    """Match only a template with no run id and the same concrete strategy."""

    if getattr(template, "run_id", object()) is not None:
        return False
    requested_run = getattr(requested, "run_id", None)
    strategy_ref = getattr(template, "strategy_ref", None)
    if requested_run is None or strategy_ref is None:
        return False
    if getattr(requested, "strategy_ref", None) != strategy_ref:
        return False
    return all(
        getattr(template, field, object()) == getattr(requested, field, None)
        for field in _WIRE_SCOPE_FIELDS
    )


def _query_intent_id(arguments: Mapping[str, object]) -> str:
    if not isinstance(arguments, Mapping):
        raise ContractValidationError("orders.query arguments must be a mapping")
    keys = tuple(arguments.keys())
    if any(type(key) is not str for key in keys) or set(keys) != {"intent_id"}:
        raise ContractValidationError("orders.query requires only intent_id")
    intent_id = arguments["intent_id"]
    if not _is_reference(intent_id):
        raise ContractValidationError("invalid intent_id")
    return intent_id


def _is_reference(value: object) -> bool:
    return type(value) is str and _REFERENCE_RE.fullmatch(value) is not None


def _is_principal_ref(value: object) -> bool:
    """Match the base protocol's bounded trusted-principal text contract."""

    if type(value) is not str:
        return False
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        return False
    return (
        bool(encoded)
        and len(encoded) <= 256
        and value == value.strip()
        and not any(ord(character) < 32 or 0x7F <= ord(character) <= 0x9F for character in value)
    )


def _record_dto(
    record: ExecutionRecord,
    *,
    group_authorization_attempts: tuple[GroupLegAuthorization, ...] = (),
) -> dict[str, object]:
    """Project the public immutable record fields to transport-safe scalars."""

    result: dict[str, object] = {
        "intent_id": record.intent_id,
        "scope_key": record.scope_key,
        "payload_sha256": record.payload_sha256,
        "state": record.state.value,
        "provider_order_id": record.provider_order_id,
        "filled_quantity": format(record.filled_quantity, "f"),
        "average_price": (
            None if record.average_price is None else format(record.average_price, "f")
        ),
        "cumulative_commission": (
            None
            if record.cumulative_commission is None
            else format(record.cumulative_commission, "f")
        ),
        "permit_reference": record.permit_reference,
        "dispatch_attempts": record.dispatch_attempts,
        "unknown_reason": record.unknown_reason,
        "review_required": record.review_required,
        "created_at_ns": record.created_at_ns,
        "updated_at_ns": record.updated_at_ns,
        "is_terminal": record.is_terminal,
    }
    if record.group_binding is not None:
        result["group_binding"] = record.group_binding.to_payload()
        result["group_authorization_attempts"] = [
            authorization.to_payload() for authorization in group_authorization_attempts
        ]
    return result


def _stored_record_dto(
    store: SqliteExecutionStore, record: ExecutionRecord, scope: ExecutionScope
) -> dict[str, object]:
    """Include the append-only current-fence audit for a grouped order."""

    attempts = (
        store.list_group_authorization_attempts(record.intent_id, scope=scope)
        if record.group_binding is not None
        else ()
    )
    return _record_dto(record, group_authorization_attempts=attempts)


def _scope_denied() -> Mapping[str, Any]:
    # Keep this response independent of whether an intent ID exists elsewhere.
    return _reply(False, "SCOPE_DENIED", {"found": False, "record": None})


def _reply(ok: bool, code: str, result: Mapping[str, object]) -> Mapping[str, Any]:
    return {"ok": ok, "code": code, "result": dict(result)}
