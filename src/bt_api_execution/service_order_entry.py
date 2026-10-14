"""Authenticated service boundary for the managed execution write path.

This adapter exposes the existing account-owner domain objects: it borrows an
already-open execution store and a trusted provider adapter, durably admits an
intent before any provider call, and never performs a second dispatch for the
same intent.  The client cannot choose the execution scope, the strategy
attribution, the admission policy, or the provider session; all of those come
from the composition root's trusted ``WireScope`` to ``ExecutionScope`` mapping
and from the borrowed provider adapter.

A reply always carries the durable record when one exists.  ``UNKNOWN`` means
the provider may have seen the request: it must be reconciled with provider
evidence and never resubmitted under a new identity.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from .cancellation import ManagedCancellationFacade
from .contracts import (
    CancelIntent,
    ExecutionScope,
    OrderIntent,
    cancel_intent_from_payload,
    order_intent_from_payload,
)
from .errors import (
    AdmissionRequired,
    ContractValidationError,
    DurableStoreError,
    IntentConflictError,
    InvalidStateTransition,
    WriterLeaseUnavailable,
)
from .facade import ManagedExecutionFacade
from .service_component import (
    ExecutionReadService,
    _is_principal_ref,
    _matches_run_wildcard,
    _record_dto,
    _reply,
    _scope_denied,
)
from .store import CancelRecord, ExecutionRecord, SqliteExecutionStore

SUBMIT_OPERATION = "orders.submit"
CANCEL_OPERATION = "orders.cancel"
QUERY_OPERATION = "orders.query"
SUPPORTED_OPERATIONS = frozenset({SUBMIT_OPERATION, CANCEL_OPERATION, QUERY_OPERATION})

#: Roles allowed to reach the write path.  The registry grant, not this set,
#: decides whether a concrete principal may actually submit or cancel.
_WRITE_ROLES = frozenset({WireRole.STRATEGY_CLIENT, WireRole.APPLICATION_CLIENT, WireRole.SERVICE})

_DEFAULT_WRITER_ID = "execution-order-entry"
_DEFAULT_LEASE_TTL_NS = 5_000_000_000


class ExecutionOrderEntryService:
    """Serve ``orders.submit``/``orders.cancel``/``orders.query`` for one store.

    ``store`` stays caller-owned.  ``targets`` maps canonical base
    ``WireScope`` templates to durable execution scopes; a request can only
    reach a scope that the composition root registered.  ``dispatch`` is the
    trusted provider adapter: an object exposing ``submit(intent)`` and
    ``cancel(intent)``, or a plain callable used for submission.

    ``allow_unprotected`` exists only for deployments that explicitly declare
    no risk admission gate.  It is never read from the service manifest.
    """

    SUPPORTED_OPERATIONS = SUPPORTED_OPERATIONS

    def __init__(
        self,
        store: SqliteExecutionStore,
        targets: Mapping[WireScope, ExecutionScope],
        dispatch: object,
        *,
        admission_gate: object | None = None,
        cancel_admission_gate: object | None = None,
        allow_unprotected: bool = False,
        writer_id: str = _DEFAULT_WRITER_ID,
        lease_ttl_ns: int = _DEFAULT_LEASE_TTL_NS,
    ) -> None:
        if type(store) is not SqliteExecutionStore:
            raise ContractValidationError("execution order entry requires a borrowed SQLite store")
        if not isinstance(targets, Mapping) or not targets:
            raise ContractValidationError("execution order entry targets are required")
        if not _has_dispatch(dispatch, "submit"):
            raise ContractValidationError("execution order entry requires a dispatch port")
        if not _has_dispatch(dispatch, "cancel"):
            raise ContractValidationError("execution order entry requires a cancel dispatch port")
        if type(allow_unprotected) is not bool:
            raise ContractValidationError("invalid allow_unprotected")
        if type(writer_id) is not str or not writer_id or writer_id != writer_id.strip():
            raise ContractValidationError("invalid writer_id")
        if len(writer_id) > 128:
            raise ContractValidationError("invalid writer_id")
        if type(lease_ttl_ns) is not int or lease_ttl_ns <= 0:
            raise ContractValidationError("invalid lease_ttl_ns")
        if admission_gate is not None and not callable(getattr(admission_gate, "reserve", None)):
            raise ContractValidationError("admission gate requires reserve")
        if cancel_admission_gate is not None and not callable(
            getattr(cancel_admission_gate, "claim_before_dispatch", None)
        ):
            # Fail at assembly time instead of on the first cancellation.
            raise ContractValidationError(
                "cancellation admission gate requires claim_before_dispatch"
            )

        copied: dict[WireScope, ExecutionScope] = {}
        for template, execution_scope in targets.items():
            if type(template) is not WireScope:
                raise ContractValidationError("targets must use canonical WireScope keys")
            if type(execution_scope) is not ExecutionScope:
                raise ContractValidationError("targets must map to ExecutionScope values")
            copied[template] = execution_scope

        self._store = store
        self._targets = MappingProxyType(copied)
        self._dispatch = dispatch
        self._admission_gate = admission_gate
        self._cancel_admission_gate = cancel_admission_gate
        self._allow_unprotected = allow_unprotected
        self._writer_id = writer_id
        self._lease_ttl_ns = lease_ttl_ns
        self._read_service = ExecutionReadService(store, copied)
        self._lock = threading.RLock()
        self._facades: dict[str, tuple[ManagedExecutionFacade, ManagedCancellationFacade]] = {}

    def handle(
        self,
        operation: str,
        *,
        principal_ref: str,
        identity: WireIdentity,
        scope: WireScope,
        arguments: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Handle one authenticated write or read request."""

        if not _is_principal_ref(principal_ref):
            raise ContractValidationError("invalid trusted principal_ref")
        if type(operation) is not str or operation not in SUPPORTED_OPERATIONS:
            return _reply(False, "UNSUPPORTED_OPERATION", {})

        if operation == QUERY_OPERATION:
            return self._read_service.handle(
                operation, principal_ref=principal_ref, scope=scope, arguments=arguments
            )

        if type(identity) is not WireIdentity or identity.principal_ref != principal_ref:
            raise ContractValidationError("trusted identity does not match the request principal")
        if identity.role not in _WRITE_ROLES:
            return _reply(False, "ROLE_NOT_AUTHORIZED", {})
        if type(scope) is not WireScope:
            return _scope_denied()
        execution_scope = self._resolve_scope(scope)
        if execution_scope is None:
            return _scope_denied()

        if operation == SUBMIT_OPERATION:
            return self._submit(arguments, execution_scope)
        return self._cancel(arguments, execution_scope)

    def _submit(
        self, arguments: Mapping[str, Any], execution_scope: ExecutionScope
    ) -> Mapping[str, Any]:
        try:
            intent = _order_intent(arguments, execution_scope)
        except ContractValidationError:
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})

        order_facade, _ = self._facade_pair(execution_scope)
        try:
            record = order_facade.submit(intent, self._dispatch)
        except IntentConflictError:
            return self._failure_with_record("INTENT_CONFLICT", intent.intent_id, execution_scope)
        except ContractValidationError:
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})
        except AdmissionRequired:
            return self._failure_with_record(
                "ADMISSION_REQUIRED", intent.intent_id, execution_scope
            )
        except WriterLeaseUnavailable:
            # A provider call may already have happened under the previous
            # owner; report the durable state instead of a bare failure.
            return self._failure_with_record(
                "OWNER_LEASE_UNAVAILABLE", intent.intent_id, execution_scope
            )
        except InvalidStateTransition:
            return self._failure_with_record("STATE_CONFLICT", intent.intent_id, execution_scope)
        except DurableStoreError:
            return _reply(False, "STORE_UNAVAILABLE", {"found": False, "record": None})
        return _reply(True, "OK", {"found": True, "record": _record_dto(record)})

    def _cancel(
        self, arguments: Mapping[str, Any], execution_scope: ExecutionScope
    ) -> Mapping[str, Any]:
        try:
            intent = _cancel_intent(arguments, execution_scope)
        except ContractValidationError:
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})

        # A cancellation is only meaningful for an order this journal already
        # evidenced.  Check before the durable transaction so the reply can
        # name the reason; the store re-checks inside its own transaction.
        target = self._store.get(intent.target_intent_id, scope=execution_scope)
        if target is None or target.provider_order_id != intent.provider_order_id:
            return _reply(False, "CANCEL_TARGET_UNKNOWN", {"found": False, "record": None})

        _, cancel_facade = self._facade_pair(execution_scope)
        try:
            record = cancel_facade.cancel(intent, self._dispatch)
        except IntentConflictError:
            return self._cancel_failure_with_record(
                "INTENT_CONFLICT", intent.cancel_id, execution_scope
            )
        except ContractValidationError:
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})
        except AdmissionRequired:
            return self._cancel_failure_with_record(
                "ADMISSION_REQUIRED", intent.cancel_id, execution_scope
            )
        except WriterLeaseUnavailable:
            return self._cancel_failure_with_record(
                "OWNER_LEASE_UNAVAILABLE", intent.cancel_id, execution_scope
            )
        except InvalidStateTransition:
            return self._cancel_failure_with_record(
                "STATE_CONFLICT", intent.cancel_id, execution_scope
            )
        except DurableStoreError:
            return _reply(False, "STORE_UNAVAILABLE", {"found": False, "record": None})
        return _reply(True, "OK", {"found": True, "record": _cancel_record_dto(record)})

    def _failure_with_record(
        self, code: str, intent_id: str, execution_scope: ExecutionScope
    ) -> Mapping[str, Any]:
        record = self._store.get(intent_id, scope=execution_scope)
        if type(record) is not ExecutionRecord or record.scope_key != execution_scope.key:
            return _reply(False, code, {"found": False, "record": None})
        return _reply(False, code, {"found": True, "record": _record_dto(record)})

    def _cancel_failure_with_record(
        self, code: str, cancel_id: str, execution_scope: ExecutionScope
    ) -> Mapping[str, Any]:
        record = self._store.get_cancel(cancel_id, scope=execution_scope)
        if type(record) is not CancelRecord or record.scope_key != execution_scope.key:
            return _reply(False, code, {"found": False, "record": None})
        return _reply(False, code, {"found": True, "record": _cancel_record_dto(record)})

    def _facade_pair(
        self, execution_scope: ExecutionScope
    ) -> tuple[ManagedExecutionFacade, ManagedCancellationFacade]:
        with self._lock:
            pair = self._facades.get(execution_scope.key)
            if pair is not None:
                return pair
            order_facade = ManagedExecutionFacade(
                self._store,
                execution_scope,
                writer_id=self._writer_id,
                admission_gate=self._admission_gate,
                lease_ttl_ns=self._lease_ttl_ns,
                allow_unprotected=self._allow_unprotected,
            )
            cancel_facade = ManagedCancellationFacade(
                self._store,
                execution_scope,
                acquire_writer_lease=order_facade.acquire_writer_lease,
                admission_gate=self._cancel_admission_gate,
                allow_unprotected=self._allow_unprotected,
            )
            pair = (order_facade, cancel_facade)
            self._facades[execution_scope.key] = pair
            return pair

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


def _has_dispatch(dispatch: object, method_name: str) -> bool:
    if callable(getattr(dispatch, method_name, None)):
        return True
    # A bare callable is accepted only for submission, where the port protocol
    # itself is a single call.
    return method_name == "submit" and callable(dispatch)


def _order_intent(arguments: Mapping[str, Any], execution_scope: ExecutionScope) -> OrderIntent:
    payload = _scopeless_payload(arguments, "orders.submit")
    payload["scope"] = execution_scope.to_payload()
    return order_intent_from_payload(payload)


def _cancel_intent(arguments: Mapping[str, Any], execution_scope: ExecutionScope) -> CancelIntent:
    payload = _scopeless_payload(arguments, "orders.cancel")
    payload["scope"] = execution_scope.to_payload()
    return cancel_intent_from_payload(payload)


def _scopeless_payload(arguments: Mapping[str, Any], operation: str) -> dict[str, Any]:
    """Reject a client-supplied scope so attribution stays server-side."""

    if not isinstance(arguments, Mapping):
        raise ContractValidationError(f"{operation} arguments must be a mapping")
    keys = tuple(arguments.keys())
    if any(type(key) is not str for key in keys):
        raise ContractValidationError(f"{operation} arguments must use text keys")
    if "scope" in keys:
        raise ContractValidationError(f"{operation} must not carry a client scope")
    return dict(arguments.items())


def _cancel_record_dto(record: CancelRecord) -> dict[str, object]:
    """Project the public immutable cancellation fields to transport-safe scalars."""

    return {
        "cancel_id": record.cancel_id,
        "scope_key": record.scope_key,
        "target_intent_id": record.target_intent_id,
        "provider_order_id": record.provider_order_id,
        "payload_sha256": record.payload_sha256,
        "state": record.state.value,
        "permit_reference": record.permit_reference,
        "dispatch_attempts": record.dispatch_attempts,
        "unknown_reason": record.unknown_reason,
        "review_required": record.review_required,
        "created_at_ns": record.created_at_ns,
        "updated_at_ns": record.updated_at_ns,
        "is_terminal": record.is_terminal,
    }


__all__ = [
    "CANCEL_OPERATION",
    "ExecutionOrderEntryService",
    "QUERY_OPERATION",
    "SUBMIT_OPERATION",
    "SUPPORTED_OPERATIONS",
]
