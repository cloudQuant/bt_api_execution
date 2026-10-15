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
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from sys import exc_info
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from .admission import AccountScopedGroupAdmissionAdapter
from .basic_admission import ExecutionAdmissionContextV1, ExecutionBasicAdmissionGate
from .cancellation import ManagedCancellationFacade
from .contracts import (
    CancelIntent,
    ExecutionScope,
    ExecutionState,
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
from .facade import AfterGroupClaimCallable, ManagedExecutionFacade
from .group_contracts import (
    GroupCoordinatorAuthority,
    GroupLegAuthorization,
    GroupPermitBinding,
    is_group_leg_intent_id,
)
from .service_component import (
    ExecutionReadService,
    _is_principal_ref,
    _matches_run_wildcard,
    _reply,
    _scope_denied,
    _stored_record_dto,
)
from .store import CancelRecord, ExecutionRecord, SqliteExecutionStore, WriterLease

if TYPE_CHECKING:
    from .ports import DispatchPort

SUBMIT_OPERATION = "orders.submit"
CANCEL_OPERATION = "orders.cancel"
QUERY_OPERATION = "orders.query"
SUPPORTED_OPERATIONS = frozenset({SUBMIT_OPERATION, CANCEL_OPERATION, QUERY_OPERATION})


class _GroupAuthorityGuardError(Exception):
    """The trusted current-authority guard could not be entered or exited."""


class _GroupResolutionUnprovenError(Exception):
    """A durable group result could not safely resolve its Risk claim."""


AfterGroupResultCallable = Callable[
    [OrderIntent, GroupPermitBinding, GroupLegAuthorization, WriterLease, object, ExecutionRecord],
    None,
]


@contextmanager
def _held_group_authority(
    authority: GroupCoordinatorAuthority, **arguments: Any
) -> Iterator[GroupLegAuthorization]:
    try:
        guard = authority.hold_group_leg_authorization(**arguments)
        if not callable(getattr(guard, "__enter__", None)) or not callable(
            getattr(guard, "__exit__", None)
        ):
            raise ContractValidationError("group authority must return a held guard")
        authorization = guard.__enter__()
    except Exception as error:
        raise _GroupAuthorityGuardError from error
    try:
        yield authorization
    except BaseException:
        with suppress(Exception):
            guard.__exit__(*exc_info())
            # Preserve the original execution failure. The caller reports its
            # durable state and never retries the group leg automatically.
        raise
    else:
        try:
            guard.__exit__(None, None, None)
        except Exception as error:
            raise _GroupAuthorityGuardError from error


#: Roles allowed to reach the write path.  The registry grant, not this set,
#: decides whether a concrete principal may actually submit or cancel.
_WRITE_ROLES = frozenset({WireRole.STRATEGY_CLIENT, WireRole.APPLICATION_CLIENT, WireRole.SERVICE})

_DEFAULT_WRITER_ID = "execution-order-entry"
_DEFAULT_LEASE_TTL_NS = 5_000_000_000
GROUP_SUBMIT_OPERATION = "orders.submit_group_leg"


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
        basic_admission_gate: ExecutionBasicAdmissionGate | None = None,
        cancel_admission_gate: object | None = None,
        group_coordinator_authority: GroupCoordinatorAuthority | None = None,
        allow_unprotected: bool = False,
        writer_id: str = _DEFAULT_WRITER_ID,
        lease_ttl_ns: int = _DEFAULT_LEASE_TTL_NS,
    ) -> None:
        if type(store) is not SqliteExecutionStore:
            raise ContractValidationError("execution order entry requires a borrowed SQLite store")
        if not isinstance(targets, Mapping) or not targets:
            raise ContractValidationError("execution order entry targets are required")
        if not _has_dispatch(dispatch, "submit") and not (
            basic_admission_gate is not None
            and callable(getattr(dispatch, "submit_authorized", None))
        ):
            raise ContractValidationError("execution order entry requires a dispatch port")
        if not _has_dispatch(dispatch, "cancel"):
            raise ContractValidationError("execution order entry requires a cancel dispatch port")
        if type(allow_unprotected) is not bool:
            raise ContractValidationError("invalid allow_unprotected")
        if (
            basic_admission_gate is not None
            and type(basic_admission_gate) is not ExecutionBasicAdmissionGate
        ):
            raise ContractValidationError("invalid execution basic admission gate")
        if basic_admission_gate is not None and allow_unprotected:
            raise ContractValidationError(
                "basic admission cannot be combined with unprotected dispatch"
            )
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
        if group_coordinator_authority is not None and not callable(
            getattr(group_coordinator_authority, "hold_group_leg_authorization", None)
        ):
            raise ContractValidationError(
                "group coordinator authority requires hold_group_leg_authorization"
            )
        if group_coordinator_authority is not None and not callable(
            getattr(group_coordinator_authority, "assert_current_group_leg_authorization", None)
        ):
            raise ContractValidationError(
                "group coordinator authority requires a current authorization check"
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
        self._basic_admission_gate = basic_admission_gate
        self._cancel_admission_gate = cancel_admission_gate
        self._group_coordinator_authority = group_coordinator_authority
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
            admission_context = None
            if self._basic_admission_gate is not None:
                try:
                    admission_context = ExecutionAdmissionContextV1(
                        principal_ref=principal_ref,
                        identity=identity,
                        wire_scope=scope,
                        execution_scope=execution_scope,
                        action=operation,
                    )
                except ContractValidationError:
                    return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})
            return self._submit(arguments, execution_scope, admission_context)
        return self._cancel(arguments, execution_scope)

    def submit_group_leg(
        self,
        *,
        principal_ref: str,
        identity: WireIdentity,
        scope: WireScope,
        intent: OrderIntent,
        binding: GroupPermitBinding,
        after_group_claim: AfterGroupClaimCallable | None = None,
        provider_dispatcher: DispatchPort | None = None,
        after_group_result: AfterGroupResultCallable | None = None,
    ) -> Mapping[str, Any]:
        """Authorize and submit one explicit group leg through this owner.

        The SDK wire adapter calls this for ``orders.submit_group_leg`` after
        parsing the typed request. An evidence-aware provider dispatcher is a
        trusted, call-local composition input. It must be paired with both
        evidence callbacks; no wire request can select any of them. Ordinary
        ``handle(orders.submit, ...)`` never accepts a group-leg binding.
        """

        if not _is_principal_ref(principal_ref):
            raise ContractValidationError("invalid trusted principal_ref")
        if (
            type(identity) is not WireIdentity
            or identity.principal_ref != principal_ref
            or identity.role is not WireRole.SERVICE
        ):
            return _reply(False, "ROLE_NOT_AUTHORIZED", {"found": False, "record": None})
        if type(scope) is not WireScope:
            return _scope_denied()
        if type(binding) is not GroupPermitBinding or type(intent) is not OrderIntent:
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})
        if after_group_claim is not None and not callable(after_group_claim):
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})
        if after_group_result is not None and not callable(after_group_result):
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})
        if (provider_dispatcher is None) != (after_group_result is None):
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})
        if provider_dispatcher is not None and (
            after_group_claim is None
            or not callable(after_group_claim)
            or not callable(getattr(provider_dispatcher, "submit", None))
        ):
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})
        if (
            scope.group_id != binding.group_id
            or scope.leg_id != binding.leg_id
            or scope.portfolio_ref != binding.portfolio_ref
            or scope.account_ref != binding.account_ref
            or scope.strategy_ref != binding.strategy_id
            or principal_ref != binding.coordinator_principal_ref
        ):
            return _reply(False, "SCOPE_DENIED", {"found": False, "record": None})
        if not isinstance(self._admission_gate, AccountScopedGroupAdmissionAdapter):
            # A raw Risk adapter requires a trusted AccountScope argument.
            # Reject this composition before creating an intent or entering
            # the provider path instead of turning a signature error into an
            # ambiguous Risk claim/UNKNOWN result.
            return _reply(
                False,
                "GROUP_ACCOUNT_SCOPE_ADAPTER_REQUIRED",
                {"found": False, "record": None},
            )

        # Group routing claims are resolved through the same trusted account
        # target after removing only the explicit group envelope.
        account_scope = WireScope(
            execution_domain_ref=scope.execution_domain_ref,
            account_ref=scope.account_ref,
            account_refs=scope.account_refs,
            strategy_ref=scope.strategy_ref,
            run_id=scope.run_id,
            manual_book_ref=scope.manual_book_ref,
        )
        execution_scope = self._resolve_scope(account_scope)
        if execution_scope is None or execution_scope != intent.scope:
            return _scope_denied()
        try:
            binding.validate_order(intent)
        except ContractValidationError:
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})

        authority = self._group_coordinator_authority
        if authority is None:
            return _reply(False, "GROUP_AUTHORITY_UNAVAILABLE", {"found": False, "record": None})
        order_facade, _ = self._facade_pair(execution_scope)
        try:
            # Acquire and verify the real execution writer lease before
            # entering trusted coordinator authority. Its guard remains held
            # across durable admission, the one-shot Risk claim, and provider
            # dispatch, so revocation cannot race the authorization boundary.
            writer_lease = order_facade.acquire_writer_lease()
            self._store.assert_writer_lease(execution_scope, writer_lease)
            with _held_group_authority(
                authority,
                principal_ref=principal_ref,
                identity=identity,
                wire_scope=scope,
                execution_scope=execution_scope,
                intent=intent,
                binding=binding,
                writer_lease=writer_lease,
            ) as authorization:
                if type(authorization) is not GroupLegAuthorization:
                    raise ContractValidationError("group guard did not yield typed authorization")
                authorization.validate_binding(binding)
                authorization.validate_writer_lease(writer_lease)
                self._store.assert_writer_lease(execution_scope, writer_lease)

                def assert_current_group_authorization(
                    checked_intent: OrderIntent,
                    checked_binding: GroupPermitBinding,
                    checked_authorization: GroupLegAuthorization,
                    checked_writer_lease: object,
                ) -> None:
                    authority.assert_current_group_leg_authorization(
                        principal_ref=principal_ref,
                        identity=identity,
                        wire_scope=scope,
                        execution_scope=execution_scope,
                        intent=checked_intent,
                        binding=checked_binding,
                        authorization=checked_authorization,
                        writer_lease=checked_writer_lease,
                    )

                claim_receipts: list[object] = []

                def capture_group_claim(
                    checked_intent: OrderIntent,
                    checked_binding: GroupPermitBinding,
                    checked_authorization: GroupLegAuthorization,
                    checked_writer_lease: WriterLease,
                    validated_claim: object,
                ) -> None:
                    if (
                        checked_intent is not intent
                        or checked_binding is not binding
                        or checked_authorization is not authorization
                        or checked_writer_lease is not writer_lease
                        or claim_receipts
                    ):
                        raise ContractValidationError(
                            "group claim callback does not match this dispatch attempt"
                        )
                    claim_receipts.append(validated_claim)
                    assert after_group_claim is not None
                    after_group_claim(
                        checked_intent,
                        checked_binding,
                        checked_authorization,
                        checked_writer_lease,
                        validated_claim,
                    )

                record = order_facade.submit_group_leg(
                    intent,
                    binding,
                    authorization,
                    self._dispatch if provider_dispatcher is None else provider_dispatcher,
                    writer_lease=writer_lease,
                    after_group_claim=(
                        capture_group_claim
                        if provider_dispatcher is not None
                        else after_group_claim
                    ),
                    assert_current_group_authorization=assert_current_group_authorization,
                )
                if provider_dispatcher is not None:
                    terminal_states = {
                        ExecutionState.FILLED,
                        ExecutionState.CANCELLED,
                        ExecutionState.REJECTED,
                    }
                    if record.state in {
                        ExecutionState.ACKED,
                        ExecutionState.PARTIALLY_FILLED,
                        ExecutionState.UNKNOWN,
                        ExecutionState.DISPATCHING,
                        ExecutionState.PENDING_ADMISSION,
                        ExecutionState.PENDING_DISPATCH,
                    }:
                        # These states either remain exposed, are ambiguous, or
                        # have not reached a terminal provider resolution.
                        # Raise through the authority guard so neither its
                        # dispatch hold nor the Risk latch is cleared.
                        raise _GroupResolutionUnprovenError
                    if claim_receipts and record.state not in terminal_states:
                        raise _GroupResolutionUnprovenError
                    if record.state in terminal_states and claim_receipts:
                        if (
                            len(claim_receipts) != 1
                            or record.dispatch_attempts != 1
                            or record.intent_id != intent.intent_id
                            or record.scope_key != execution_scope.key
                            or record.payload_sha256 != intent.fingerprint
                            or record.group_binding != binding
                            or record.unknown_reason is not None
                            or record.review_required
                        ):
                            raise _GroupResolutionUnprovenError
                        durable_record = self._store.get(intent.intent_id, scope=execution_scope)
                        if type(durable_record) is not ExecutionRecord or durable_record != record:
                            raise _GroupResolutionUnprovenError
                        try:
                            # The callback may write the durable Risk resolution.
                            # Recheck both live fences immediately before it,
                            # and check the store fence again before allowing
                            # the authority guard's normal-exit verifier to run.
                            self._store.assert_writer_lease(execution_scope, writer_lease)
                            assert_current_group_authorization(
                                intent, binding, authorization, writer_lease
                            )
                            assert after_group_result is not None
                            callback_result = after_group_result(
                                intent,
                                binding,
                                authorization,
                                writer_lease,
                                claim_receipts[0],
                                durable_record,
                            )
                            if callback_result is not None:
                                raise _GroupResolutionUnprovenError
                            self._store.assert_writer_lease(execution_scope, writer_lease)
                            assert_current_group_authorization(
                                intent, binding, authorization, writer_lease
                            )
                        except Exception as error:
                            raise _GroupResolutionUnprovenError from error
        except IntentConflictError:
            return self._failure_with_record("INTENT_CONFLICT", intent.intent_id, execution_scope)
        except ContractValidationError:
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})
        except AdmissionRequired:
            return self._failure_with_record(
                "GROUP_ADMISSION_REQUIRED", intent.intent_id, execution_scope
            )
        except WriterLeaseUnavailable:
            return self._failure_with_record(
                "OWNER_LEASE_UNAVAILABLE", intent.intent_id, execution_scope
            )
        except InvalidStateTransition:
            return self._failure_with_record("STATE_CONFLICT", intent.intent_id, execution_scope)
        except DurableStoreError:
            return _reply(False, "STORE_UNAVAILABLE", {"found": False, "record": None})
        except _GroupAuthorityGuardError:
            return self._failure_with_record(
                "GROUP_AUTHORIZATION_FAILED", intent.intent_id, execution_scope
            )
        except _GroupResolutionUnprovenError:
            return self._failure_with_record(
                "GROUP_RESOLUTION_UNPROVEN", intent.intent_id, execution_scope
            )
        return _reply(
            True,
            "OK",
            {"found": True, "record": _stored_record_dto(self._store, record, execution_scope)},
        )

    def recover_interrupted_dispatches(self) -> tuple[ExecutionRecord, ...]:
        """Recover configured scopes before this service accepts writes.

        The composition root calls this once during startup. Each scope's
        facade acquires the current writer lease and atomically latches its
        unresolved dispatch claims as ``UNKNOWN`` without provider I/O.
        """

        recovered: list[ExecutionRecord] = []
        seen_scope_keys: set[str] = set()
        for execution_scope in self._targets.values():
            if execution_scope.key in seen_scope_keys:
                continue
            seen_scope_keys.add(execution_scope.key)
            order_facade, _ = self._facade_pair(execution_scope)
            records = order_facade.recover_interrupted_dispatches()
            if type(records) is not tuple or any(
                type(record) is not ExecutionRecord
                or record.scope_key != execution_scope.key
                or record.state is not ExecutionState.UNKNOWN
                for record in records
            ):
                raise DurableStoreError(
                    "execution order recovery returned invalid persisted evidence"
                )
            recovered.extend(records)
        return tuple(recovered)

    def _submit(
        self,
        arguments: Mapping[str, Any],
        execution_scope: ExecutionScope,
        admission_context: ExecutionAdmissionContextV1 | None,
    ) -> Mapping[str, Any]:
        try:
            intent = _order_intent(arguments, execution_scope)
        except ContractValidationError:
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})

        order_facade, _ = self._facade_pair(execution_scope)
        try:
            record = order_facade.submit(
                intent,
                self._dispatch,
                admission_context=admission_context,
            )
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
        return _reply(
            True,
            "OK",
            {"found": True, "record": _stored_record_dto(self._store, record, execution_scope)},
        )

    def _cancel(
        self, arguments: Mapping[str, Any], execution_scope: ExecutionScope
    ) -> Mapping[str, Any]:
        try:
            intent = _cancel_intent(arguments, execution_scope)
        except ContractValidationError:
            return _reply(False, "INVALID_REQUEST", {"found": False, "record": None})
        if is_group_leg_intent_id(intent.cancel_id) or is_group_leg_intent_id(
            intent.target_intent_id
        ):
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
        return _reply(
            False,
            code,
            {"found": True, "record": _stored_record_dto(self._store, record, execution_scope)},
        )

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
                basic_admission_gate=self._basic_admission_gate,
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
    "GROUP_SUBMIT_OPERATION",
    "QUERY_OPERATION",
    "SUBMIT_OPERATION",
    "SUPPORTED_OPERATIONS",
]
