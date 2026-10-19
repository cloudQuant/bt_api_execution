"""Public managed execution facade with no concrete provider dependency."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from decimal import Decimal
from threading import RLock
from time import time_ns
from typing import TYPE_CHECKING, Any

from .admission import AccountScopedGroupAdmissionAdapter
from .approved_target import (
    APPROVED_TARGET_OPERATION,
    ApprovedTargetConsumeDisposition,
    ApprovedTargetConsumeResultV1,
    has_reserved_approval_tags,
)
from .basic_admission import (
    ExecutionAdmissionAuthorizationV1,
    ExecutionAdmissionContextV1,
    ExecutionBasicAdmissionGate,
)
from .contracts import (
    ExecutionEvent,
    ExecutionScope,
    ExecutionState,
    OrderIntent,
    ProviderObservation,
)
from .errors import (
    AdmissionDenied,
    AdmissionRequired,
    ContractValidationError,
    ExecutionBasicAdmissionDenied,
    InvalidStateTransition,
    WriterLeaseUnavailable,
)
from .group_contracts import (
    GroupLegAuthorization,
    GroupPermitBinding,
    is_group_leg_intent_id,
    risk_account_scope_fingerprint,
)
from .store import SqliteExecutionStore, WriterLease

if TYPE_CHECKING:
    from .ports import AdmissionGate, DispatchPort, ReconciliationPort
    from .store import (
        CtpAccountFamilyOwnerHandle,
        ExecutionRecord,
    )

DispatchCallable = Callable[[OrderIntent], ProviderObservation]
BeforeDispatchCallable = Callable[[OrderIntent], None]
ApprovedTargetConsumeCallable = Callable[[OrderIntent], ApprovedTargetConsumeResultV1]
AfterGroupClaimCallable = Callable[
    [OrderIntent, GroupPermitBinding, GroupLegAuthorization, "WriterLease", object], None
]
CurrentGroupAuthorizationCheck = Callable[
    [OrderIntent, GroupPermitBinding, GroupLegAuthorization, "WriterLease"], None
]

_OBSERVATION_RECORDING_STATES = frozenset(
    {
        ExecutionState.DISPATCHING,
        ExecutionState.UNKNOWN,
        ExecutionState.ACKED,
        ExecutionState.PARTIALLY_FILLED,
    }
)
_PERMIT_SETTLEMENT_SOURCE_STATES = frozenset({ExecutionState.DISPATCHING, ExecutionState.UNKNOWN})


def _validate_borrowed_ctp_writer_lease(
    store: SqliteExecutionStore,
    scope: ExecutionScope,
    writer_id: str,
    lease: object,
) -> WriterLease:
    """Validate a caller-owned CTP lease without acquiring a new generation."""

    if (
        type(store) is not SqliteExecutionStore
        or type(scope) is not ExecutionScope
        or scope.provider != "ctp"
        or type(lease) is not WriterLease
        or type(writer_id) is not str
        or not writer_id
        or lease.owner_id != writer_id
        or lease.scope_key != scope.account_key
        or type(lease.family_key) is not str
        or not lease.family_key
        or type(lease.family_owner_intent_id) is not str
        or not lease.family_owner_intent_id
    ):
        raise ContractValidationError("invalid borrowed CTP writer lease binding")
    store.assert_writer_lease(scope, lease)
    return lease


def _is_typed_risk_admission_denial(error: BaseException) -> bool:
    """Recognize the optional Risk adapter's exact pre-effect denial type.

    Execution must stay importable without Risk. The configured gate is trusted
    composition, so this narrow public exception identity distinguishes the
    Risk adapter's deterministic policy denial from an ambiguous claim failure.
    """

    error_type = type(error)
    code = getattr(error, "code", None)
    return (
        isinstance(error, PermissionError)
        and error_type.__module__ == "bt_api_risk.order_admission"
        and error_type.__name__ == "RiskAdmissionDeniedError"
        and type(code) is str
        and 1 <= len(code) <= 96
        and code[0].isupper()
        and code.replace("_", "").isalnum()
        and code == code.upper()
    )


def _is_paper_risk_admission_adapter(gate: object) -> bool:
    """Select only the canonical Paper adapter's lease-aware snapshot path."""

    try:
        from .paper_risk import PaperRiskAdmissionAdapter
    except ImportError:
        return False
    return type(gate) is PaperRiskAdmissionAdapter


class ManagedExecutionFacade:
    """Coordinate durable intent admission and one provider dispatch attempt.

    The facade never imports an exchange package and never performs network I/O
    by itself.  An injected dispatch callable is intentionally invoked *after*
    the intent, admission result, and dispatch claim are durable.  A failure
    from that callable is an uncertain provider effect, so the record becomes
    ``UNKNOWN`` and cannot be submitted again automatically.

    ``AdmissionGate`` is intentionally structural: the shared risk package owns
    the actual permit contract.  For an end-to-end managed deployment it must
    be configured.  ``allow_unprotected`` exists only for isolated, zero-I/O
    unit fixtures and must not be selected by a runtime preset.
    """

    def __init__(
        self,
        store: SqliteExecutionStore,
        scope: ExecutionScope,
        *,
        writer_id: str,
        admission_gate: AdmissionGate | None = None,
        basic_admission_gate: ExecutionBasicAdmissionGate | None = None,
        lease_ttl_ns: int = 5_000_000_000,
        allow_unprotected: bool = False,
        pre_dispatch_guard: BeforeDispatchCallable | None = None,
        borrowed_writer_lease: WriterLease | None = None,
    ) -> None:
        if not writer_id or writer_id != writer_id.strip() or len(writer_id) > 128:
            raise ContractValidationError("invalid writer_id")
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
        if pre_dispatch_guard is not None and not callable(pre_dispatch_guard):
            raise ContractValidationError("invalid pre-dispatch guard")
        if borrowed_writer_lease is not None:
            _validate_borrowed_ctp_writer_lease(store, scope, writer_id, borrowed_writer_lease)
        self._store = store
        self._scope = scope
        self._writer_id = writer_id
        self._admission_gate = admission_gate
        self._basic_admission_gate = basic_admission_gate
        self._lease_ttl_ns = lease_ttl_ns
        self._allow_unprotected = allow_unprotected
        self._pre_dispatch_guard = pre_dispatch_guard
        self._borrowed_writer_lease = borrowed_writer_lease
        self._last_writer_lease: WriterLease | None = None
        self._ctp_account_family_owner: CtpAccountFamilyOwnerHandle | None = None
        self._ctp_family_owner_lock = RLock()

    @property
    def scope(self) -> ExecutionScope:
        return self._scope

    @property
    def store(self) -> SqliteExecutionStore:
        return self._store

    def acquire_writer_lease(self) -> WriterLease:
        """Acquire or renew the account-level writer lease before a mutation."""
        if self._borrowed_writer_lease is not None:
            return _validate_borrowed_ctp_writer_lease(
                self._store,
                self._scope,
                self._writer_id,
                self._borrowed_writer_lease,
            )
        provider_is_ctp = self._scope.provider.lower() == "ctp"
        if provider_is_ctp and self._scope.provider != "ctp":
            raise ContractValidationError("CTP execution provider must use canonical lowercase")
        if provider_is_ctp:
            with self._ctp_family_owner_lock:
                if self._ctp_account_family_owner is None:
                    self._ctp_account_family_owner = self._store.acquire_ctp_account_family_owner(
                        self._scope
                    )
                lease = self._store.acquire_or_renew_lease(
                    self._scope,
                    self._writer_id,
                    ttl_ns=self._lease_ttl_ns,
                    ctp_account_family_owner=self._ctp_account_family_owner,
                )
        else:
            lease = self._store.acquire_or_renew_lease(
                self._scope,
                self._writer_id,
                ttl_ns=self._lease_ttl_ns,
            )
        self._last_writer_lease = lease
        return lease

    def recover_interrupted_dispatches(self) -> tuple[ExecutionRecord, ...]:
        """Fence this scope's persisted dispatch claims during service startup.

        The current writer lease is acquired before touching the journal. Any
        unresolved claim becomes ``UNKNOWN`` atomically, and provider I/O is
        never retried. A composition must finish this recovery before it
        accepts managed writes.
        """

        writer_lease = self.acquire_writer_lease()
        return self._store.recover_interrupted_dispatches(
            self._scope,
            writer_lease=writer_lease,
        )

    def close(self) -> bool:
        """Release an owned lease; borrowed leases and the store remain caller-owned."""

        if self._borrowed_writer_lease is not None:
            return False
        lease = self._last_writer_lease
        if lease is None:
            return False
        return self._store.release_lease(
            self._scope,
            self._writer_id,
            fencing_token=lease.fencing_token,
        )

    def submit(
        self,
        intent: OrderIntent,
        dispatcher: DispatchPort | DispatchCallable,
        *,
        before_dispatch: BeforeDispatchCallable | None = None,
        admission_context: ExecutionAdmissionContextV1 | None = None,
        owner_strategy_ref: str | None = None,
        owner_run_id: str | None = None,
        owner_wire_scope: Mapping[str, Any] | None = None,
    ) -> ExecutionRecord:
        """Submit an ordinary intent; group namespace IDs require the group path."""

        if is_group_leg_intent_id(getattr(intent, "intent_id", None)):
            raise ContractValidationError("group leg intent requires coordinator authorization")
        return self._submit(
            intent,
            dispatcher,
            before_dispatch=before_dispatch,
            admission_context=admission_context,
            group_binding=None,
            group_authorization=None,
            writer_lease=None,
            owner_strategy_ref=owner_strategy_ref,
            owner_run_id=owner_run_id,
            owner_wire_scope=owner_wire_scope,
        )

    def submit_approved_target(
        self,
        intent: OrderIntent,
        dispatcher: DispatchPort | DispatchCallable,
        *,
        admission_context: ExecutionAdmissionContextV1,
        consume_authorization: ApprovedTargetConsumeCallable,
        before_dispatch: BeforeDispatchCallable | None = None,
        owner_strategy_ref: str | None = None,
        owner_run_id: str | None = None,
        owner_wire_scope: Mapping[str, Any] | None = None,
    ) -> ExecutionRecord:
        """Submit one code-bound target after a one-use AFI consume decision.

        ``consume_authorization`` runs only after the exact intent has a durable
        dispatch claim, and before either final Basic or Risk claim. Its typed
        result distinguishes a definitive pre-claim denial from an uncertain
        consume; neither path calls the provider.
        """

        if not has_reserved_approval_tags(getattr(intent, "tags", None)):
            raise ContractValidationError("approved target audit tags are required")
        if (
            type(admission_context) is not ExecutionAdmissionContextV1
            or admission_context.action != APPROVED_TARGET_OPERATION
            or admission_context.execution_scope != getattr(intent, "scope", None)
        ):
            raise ContractValidationError("approved-target admission context is required")
        if not callable(consume_authorization):
            raise ContractValidationError("approved-target consume hook is required")
        if self._basic_admission_gate is None or self._admission_gate is None:
            raise AdmissionRequired("approved targets require Basic and Risk admission")
        if is_group_leg_intent_id(getattr(intent, "intent_id", None)):
            raise ContractValidationError("approved target cannot be a group leg")
        return self._submit(
            intent,
            dispatcher,
            before_dispatch=before_dispatch,
            admission_context=admission_context,
            group_binding=None,
            group_authorization=None,
            writer_lease=None,
            approved_target_consume=consume_authorization,
            owner_strategy_ref=owner_strategy_ref,
            owner_run_id=owner_run_id,
            owner_wire_scope=owner_wire_scope,
        )

    def submit_group_leg(
        self,
        intent: OrderIntent,
        binding: GroupPermitBinding,
        authorization: GroupLegAuthorization,
        dispatcher: DispatchPort | DispatchCallable,
        *,
        writer_lease: WriterLease,
        before_dispatch: BeforeDispatchCallable | None = None,
        after_group_claim: AfterGroupClaimCallable | None = None,
        assert_current_group_authorization: CurrentGroupAuthorizationCheck | None = None,
    ) -> ExecutionRecord:
        """Submit one child against its already-reserved group permit.

        This path deliberately does not call singleton ``reserve`` or
        ``claim_for_dispatch``. The account-owner admission adapter must
        implement the group-aware one-shot claim. A failed or ambiguous Risk
        claim is latched UNKNOWN and is never retried through provider I/O.
        """

        if type(binding) is not GroupPermitBinding:
            raise ContractValidationError("group permit binding is required")
        if type(authorization) is not GroupLegAuthorization:
            raise ContractValidationError("current group authorization is required")
        binding.validate_order(intent)
        authorization.validate_binding(binding)
        authorization.validate_writer_lease(writer_lease)
        if not isinstance(self._admission_gate, AccountScopedGroupAdmissionAdapter):
            raise AdmissionRequired("trusted account-scoped group Risk adapter is required")
        if self._basic_admission_gate is not None:
            raise AdmissionRequired("group legs require the pre-reserved group Risk path")
        return self._submit(
            intent,
            dispatcher,
            before_dispatch=before_dispatch,
            after_group_claim=after_group_claim,
            assert_current_group_authorization=assert_current_group_authorization,
            admission_context=None,
            group_binding=binding,
            group_authorization=authorization,
            writer_lease=writer_lease,
        )

    def _submit(
        self,
        intent: OrderIntent,
        dispatcher: DispatchPort | DispatchCallable,
        *,
        before_dispatch: BeforeDispatchCallable | None = None,
        after_group_claim: AfterGroupClaimCallable | None = None,
        assert_current_group_authorization: CurrentGroupAuthorizationCheck | None = None,
        admission_context: ExecutionAdmissionContextV1 | None = None,
        group_binding: GroupPermitBinding | None,
        group_authorization: GroupLegAuthorization | None,
        writer_lease: WriterLease | None,
        approved_target_consume: ApprovedTargetConsumeCallable | None = None,
        owner_strategy_ref: str | None = None,
        owner_run_id: str | None = None,
        owner_wire_scope: Mapping[str, Any] | None = None,
    ) -> ExecutionRecord:
        """Durably admit and dispatch an intent at most once.

        Repeating a durable `PENDING_DISPATCH`, `DISPATCHING`, terminal, or
        `UNKNOWN` intent returns its record without a second dispatch.  A caller
        may only resolve an unknown result through :meth:`reconcile`.

        The constructor-level ``pre_dispatch_guard`` runs for every submit
        after the durable dispatch claim and before final admission validation.
        ``before_dispatch`` is an optional per-call guard that runs after it.
        For a group leg, ``after_group_claim`` runs once after the exact
        one-shot Risk claim has been validated. Its return value is ignored;
        a failure leaves the durable dispatch UNKNOWN and never calls the
        provider. The caller keeps its trusted authority guard held through
        this method and the provider call.
        """

        if before_dispatch is not None and not callable(before_dispatch):
            raise ContractValidationError("invalid pre-dispatch guard")
        if approved_target_consume is not None and not callable(approved_target_consume):
            raise ContractValidationError("invalid approved-target consume hook")
        approved_target_path = approved_target_consume is not None
        if approved_target_path and (
            group_binding is not None
            or admission_context is None
            or admission_context.action != APPROVED_TARGET_OPERATION
            or not has_reserved_approval_tags(getattr(intent, "tags", None))
        ):
            raise ContractValidationError("invalid approved-target dispatch binding")
        if after_group_claim is not None and not callable(after_group_claim):
            raise ContractValidationError("invalid after-group-claim hook")
        if after_group_claim is not None and group_binding is None:
            raise ContractValidationError("after-group-claim hook requires a group leg")
        if (group_binding is None) != (assert_current_group_authorization is None):
            raise ContractValidationError(
                "current group authorization check must match group submission"
            )
        if assert_current_group_authorization is not None and not callable(
            assert_current_group_authorization
        ):
            raise ContractValidationError("invalid current group authorization check")
        if (group_binding is None) != (group_authorization is None):
            raise ContractValidationError("group binding and authorization must be paired")
        if group_binding is not None:
            group_binding.validate_order(intent)
            assert group_authorization is not None
            group_authorization.validate_binding(group_binding)
        elif is_group_leg_intent_id(getattr(intent, "intent_id", None)):
            raise ContractValidationError("group leg intent requires coordinator authorization")
        if (
            self._basic_admission_gate is not None
            and type(admission_context) is not ExecutionAdmissionContextV1
        ):
            raise AdmissionRequired("trusted execution admission context is required")
        if (
            self._basic_admission_gate is not None
            and admission_context.execution_scope != intent.scope
        ):
            raise ContractValidationError("admission context scope does not match intent")
        self._require_scope(intent)
        if writer_lease is None:
            if group_binding is not None:
                raise ContractValidationError("group leg requires a held writer lease")
            writer_lease = self.acquire_writer_lease()
        else:
            self._store.assert_writer_lease(self._scope, writer_lease)
        if group_authorization is not None:
            assert group_binding is not None
            group_authorization.validate_binding(group_binding)
            group_authorization.validate_writer_lease(writer_lease)
        record = self._store.admit_intent(
            intent,
            writer_lease=writer_lease,
            group_binding=group_binding,
            group_authorization=group_authorization,
            owner_strategy_ref=owner_strategy_ref,
            owner_run_id=owner_run_id,
            owner_wire_scope=owner_wire_scope,
        )
        if record.state is ExecutionState.PENDING_ADMISSION:
            if group_binding is None:
                record = self._admit(
                    intent,
                    writer_lease,
                    dispatcher,
                    admission_context,
                )
            else:
                record = self._admit_group_leg(intent, group_binding, writer_lease)
        if record.state is not ExecutionState.PENDING_DISPATCH:
            return record
        if self._basic_admission_gate is not None:
            assert admission_context is not None
            if record.admission_context_sha256 == admission_context.fingerprint and (
                self._basic_admission_gate.needs_reauthorization(
                    record.basic_admission_reference,
                    admission_context,
                    intent,
                    dispatcher,
                    writer_lease,
                )
            ):
                replacement = None
                try:
                    replacement = self._basic_admission_gate.reauthorize_pending_dispatch(
                        admission_context, intent, dispatcher, writer_lease
                    )
                    record, replaced = self._store.replace_pending_basic_admission_reference(
                        intent.intent_id,
                        self._scope,
                        expected_reference=record.basic_admission_reference,
                        new_reference=replacement,
                        expected_context_sha256=admission_context.fingerprint,
                        writer_lease=writer_lease,
                    )
                except WriterLeaseUnavailable:
                    if replacement is not None:
                        self._basic_admission_gate.revoke(replacement)
                    raise
                except Exception:
                    if replacement is not None:
                        self._basic_admission_gate.revoke(replacement)
                    # Continue through the one-shot final claim. The lost or
                    # rejected reference then fails closed and blocks before
                    # the provider; it is never reconstructed from SQLite.
                else:
                    if not replaced:
                        self._basic_admission_gate.revoke(replacement)
                        return record
            elif record.admission_context_sha256 is None:
                # Legacy pending rows have no original principal binding. A
                # new BasicGate cannot safely adopt them under a different
                # admission contract.
                pass
        record, claimed = self._store.claim_for_dispatch(
            intent.intent_id, self._scope, writer_lease=writer_lease
        )
        if not claimed:
            return record
        for guard in (self._pre_dispatch_guard, before_dispatch):
            if guard is None:
                continue
            try:
                guard(intent)
            except Exception:
                return self._block_before_dispatch(
                    intent, "pre_dispatch_guard_failed", writer_lease
                )
        if approved_target_consume is not None:
            try:
                consume_result = approved_target_consume(intent)
            except Exception:
                consume_result = ApprovedTargetConsumeResultV1(
                    ApprovedTargetConsumeDisposition.AMBIGUOUS,
                    "APPROVAL_CONSUME_OUTCOME_UNKNOWN",
                )
            if type(consume_result) is not ApprovedTargetConsumeResultV1:
                return self._store.mark_unknown(
                    intent.intent_id,
                    self._scope,
                    "approved_target_consume_unknown",
                    writer_lease=writer_lease,
                )
            if consume_result.disposition is ApprovedTargetConsumeDisposition.DEFINITIVE_DENIAL:
                assert consume_result.reason_code is not None
                return self._block_before_dispatch(
                    intent, consume_result.reason_code, writer_lease
                )
            if consume_result.disposition is not ApprovedTargetConsumeDisposition.CONSUMED_NEW:
                reason = (
                    "approved_target_already_consumed"
                    if consume_result.disposition
                    is ApprovedTargetConsumeDisposition.ALREADY_CONSUMED
                    else "approved_target_consume_unknown"
                )
                return self._store.mark_unknown(
                    intent.intent_id,
                    self._scope,
                    reason,
                    writer_lease=writer_lease,
                )
            # The approval consume is now irreversible. Reassert the current
            # execution owner before either one-shot admission claim.
            self._store.assert_writer_lease(self._scope, writer_lease)
        # Admission is the final application check after potentially slow
        # coordinator hooks. In particular, BasicAdmission expiry and owner
        # fencing are revalidated immediately before the authorized port.
        record, authorization = self._validate_and_claim_before_dispatch(
            intent,
            record,
            writer_lease,
            dispatcher,
            admission_context,
            group_binding=group_binding,
            group_authorization=group_authorization,
            after_group_claim=after_group_claim,
            approved_target_path=approved_target_path,
        )
        if record.state is not ExecutionState.DISPATCHING:
            return record
        if group_authorization is not None:
            assert group_binding is not None
            try:
                group_authorization.validate_binding(group_binding)
                group_authorization.validate_writer_lease(writer_lease)
            except Exception:
                return self._store.mark_unknown(
                    intent.intent_id,
                    self._scope,
                    "group_authorization_fence_invalid",
                    writer_lease=writer_lease,
                )
            assert assert_current_group_authorization is not None
            self._store.assert_writer_lease(self._scope, writer_lease)
            try:
                assert_current_group_authorization(
                    intent,
                    group_binding,
                    group_authorization,
                    writer_lease,
                )
            except Exception:
                return self._store.mark_unknown(
                    intent.intent_id,
                    self._scope,
                    "group_authority_fence_invalid",
                    writer_lease=writer_lease,
                )
        # Keep the store fence check adjacent to the provider call as well as
        # in each admission gate's final claim. For group work, the trusted
        # authority check above validates the active generation and durable
        # hold; the writer fence check below is repeated after that check so it
        # remains adjacent to the provider invocation.
        self._store.assert_writer_lease(self._scope, writer_lease)
        if group_authorization is not None and time_ns() >= group_authorization.deadline_ns:
            return self._store.mark_unknown(
                intent.intent_id,
                self._scope,
                "group_authorization_expired_after_claim",
                writer_lease=writer_lease,
            )
        try:
            dispatch_result = self._dispatch(dispatcher, intent, authorization=authorization)
        except Exception:
            return self._store.mark_unknown(
                intent.intent_id, self._scope, writer_lease=writer_lease
            )
        from .paper_execution import PaperDispatchResultV1

        if self._scope.provider == "paper":
            if type(dispatch_result) is not PaperDispatchResultV1:
                return self._store.mark_unknown(
                    intent.intent_id,
                    self._scope,
                    "paper_dispatch_result_invalid",
                    writer_lease=writer_lease,
                )
            try:
                record, _event, _receipt = self._store.record_paper_match_with_event(
                    dispatch_result,
                    scope=self._scope,
                    writer_lease=writer_lease,
                )
            except WriterLeaseUnavailable:
                raise
            except Exception:
                # The commit may have completed before a local error surfaced.
                # Only an exact durable receipt plus terminal execution record
                # resolves that ambiguity; otherwise this ID stays UNKNOWN.
                record = None
                receipt = None
                try:
                    record = self._store.get(intent.intent_id, scope=self._scope)
                    receipt = self._store.get_paper_match_receipt(
                        intent.intent_id, scope=self._scope
                    )
                except Exception:
                    record = None
                    receipt = None
                if (
                    record is None
                    or receipt is None
                    or record.state is not receipt.state
                    or receipt.intent_fingerprint != intent.fingerprint
                    or receipt.intent_id != intent.intent_id
                ):
                    return self._store.mark_unknown(
                        intent.intent_id,
                        self._scope,
                        "paper_match_commit_ambiguous",
                        writer_lease=writer_lease,
                    )
            return self._settle_after_evidenced_outcome(record, writer_lease=writer_lease)
        if type(dispatch_result) is PaperDispatchResultV1:
            return self._store.mark_unknown(
                intent.intent_id,
                self._scope,
                "paper_dispatch_outside_paper_scope",
                writer_lease=writer_lease,
            )
        observation = dispatch_result
        if observation.intent_id != intent.intent_id:
            return self._store.mark_unknown(
                intent.intent_id,
                self._scope,
                "dispatch_identity_mismatch",
                writer_lease=writer_lease,
            )
        try:
            record, event = self._store.record_observation_with_event(
                observation,
                scope=self._scope,
                source="provider",
                writer_lease=writer_lease,
            )
        except WriterLeaseUnavailable:
            # The provider may have seen the request, but this owner is no
            # longer allowed to alter the local projection.  The new lease
            # holder's no-provider recovery path must turn DISPATCHING into
            # UNKNOWN/reconcile instead.
            raise
        except Exception:
            return self._store.mark_unknown(
                intent.intent_id,
                self._scope,
                "invalid_provider_evidence",
                writer_lease=writer_lease,
            )
        settled_record, _settlement_ok = self._settle_after_evidenced_outcome_result(
            record, writer_lease=writer_lease
        )
        observed_record, _event, _observation_ok = (
            self._after_evidenced_observation_result(
                settled_record,
                event,
                writer_lease=writer_lease,
            )
        )
        return observed_record

    def reconcile(self, observation: ProviderObservation) -> ExecutionRecord:
        """Apply typed provider evidence without issuing another provider request.

        Reconciliation can advance an uncertain or crashed dispatch, and can
        also advance a previously acknowledged order through partial fill,
        fill, or cancellation evidence.  Terminal records remain immutable.
        """

        return self.record_provider_observation(observation)

    def record_provider_observation(self, observation: ProviderObservation) -> ExecutionRecord:
        """Persist one typed provider observation for a non-terminal order.

        This method performs no provider I/O.  It is for a caller that has
        already obtained normalized evidence from an injected port.  The first
        observed outcome for a claimed or unknown dispatch settles an active
        admission permit.  Later monotonic observations for ``ACKED`` and
        ``PARTIALLY_FILLED`` orders do not settle it a second time.
        """

        record, _event, _review_required = self._record_provider_observation_with_event(observation)
        return record

    def _admit_group_leg(
        self,
        intent: OrderIntent,
        binding: GroupPermitBinding,
        writer_lease: WriterLease,
    ) -> ExecutionRecord:
        """Activate from the coordinator's durable vector permit, never reserve anew."""

        self._store.assert_writer_lease(self._scope, writer_lease)
        return self._store.activate_intent(
            intent.intent_id,
            self._scope,
            binding.permit_id,
            writer_lease=writer_lease,
        )

    def record_provider_observation_event(
        self, observation: ProviderObservation
    ) -> ExecutionEvent | None:
        """Persist one observation and return only its newly appended outbox event.

        Exact retries return ``None``. Invalid evidence, permit-settlement,
        and optional observation-reconciliation failures first persist a
        review-required latch, then raise :class:`InvalidStateTransition` so
        callers cannot confuse them with a duplicate. The returned event is
        the same immutable row created by the store transaction; this method
        does not publish to monitor or call a provider port.
        """

        _record, event, review_required = self._record_provider_observation_with_event(observation)
        if review_required:
            raise InvalidStateTransition("provider observation requires manual review")
        return event

    def _record_provider_observation_with_event(
        self, observation: ProviderObservation
    ) -> tuple[ExecutionRecord, ExecutionEvent | None, bool]:
        if self._scope.provider == "paper":
            raise ContractValidationError(
                "paper outcomes must be persisted with their atomic match receipt"
            )
        writer_lease = self.acquire_writer_lease()
        record = self._require_owned_record(observation.intent_id)
        if record.state not in _OBSERVATION_RECORDING_STATES:
            return record, None, record.review_required
        previous_state = record.state
        try:
            record, event = self._store.record_observation_with_event(
                observation,
                scope=self._scope,
                source="reconcile",
                writer_lease=writer_lease,
            )
        except WriterLeaseUnavailable:
            raise
        except Exception:
            marked = self._store.mark_review_required(
                observation.intent_id,
                self._scope,
                "invalid_reconcile_evidence",
                writer_lease=writer_lease,
            )
            return marked, None, True
        settled_record, settlement_ok = self._settle_after_evidenced_outcome_result(
            record,
            settle_permit=previous_state in _PERMIT_SETTLEMENT_SOURCE_STATES,
            writer_lease=writer_lease,
        )
        observed_record, observed_event, observation_ok = (
            self._after_evidenced_observation_result(
                settled_record,
                event,
                writer_lease=writer_lease,
            )
        )
        operation_ok = settlement_ok and observation_ok
        return observed_record, observed_event if operation_ok else None, not operation_ok

    def reconcile_with_port(
        self,
        intent_id: str,
        reconciliation_port: ReconciliationPort
        | Callable[[OrderIntent], ProviderObservation | None],
    ) -> ExecutionRecord:
        """Query an injected evidence port without dispatching another provider order."""

        if self._scope.provider == "paper":
            raise ContractValidationError(
                "paper outcomes must be persisted with their atomic match receipt"
            )
        writer_lease = self.acquire_writer_lease()
        record = self._require_owned_record(intent_id)
        if record.state not in _OBSERVATION_RECORDING_STATES:
            return record
        intent = self._load_intent(intent_id)
        try:
            observation = self._query(reconciliation_port, intent)
        except Exception:
            return self._store.mark_review_required(
                intent_id,
                self._scope,
                "reconcile_query_failed",
                writer_lease=writer_lease,
            )
        if observation is None:
            return self._store.mark_review_required(
                intent_id,
                self._scope,
                "reconcile_no_evidence",
                writer_lease=writer_lease,
            )
        if observation.intent_id != intent_id:
            return self._store.mark_review_required(
                intent_id,
                self._scope,
                "reconcile_identity_mismatch",
                writer_lease=writer_lease,
            )
        return self.record_provider_observation(observation)

    def get(self, intent_id: str) -> ExecutionRecord | None:
        """Read a record in this facade's scope without creating provider activity."""

        record = self._store.get(intent_id, scope=self._scope)
        if record is not None and record.scope_key != self._scope.key:
            raise ContractValidationError("intent_id belongs to a different execution scope")
        return record

    def _admit(
        self,
        intent: OrderIntent,
        writer_lease: WriterLease,
        dispatcher: DispatchPort | DispatchCallable,
        admission_context: ExecutionAdmissionContextV1 | None,
    ) -> ExecutionRecord:
        gate = self._admission_gate
        basic_gate = self._basic_admission_gate
        if gate is None and basic_gate is None:
            if not self._allow_unprotected:
                raise AdmissionRequired()
            return self._store.activate_intent(
                intent.intent_id, self._scope, writer_lease=writer_lease
            )
        basic_reference: str | None = None
        context_fingerprint: str | None = None
        risk_reference: str | None = None
        risk_permit: Any = None
        try:
            self._store.assert_writer_lease(self._scope, writer_lease)
            if basic_gate is not None:
                if type(admission_context) is not ExecutionAdmissionContextV1:
                    raise AdmissionRequired("trusted execution admission context is required")
                basic_reference = basic_gate.reserve_for_dispatch(
                    admission_context, intent, dispatcher, writer_lease
                )
                context_fingerprint = admission_context.fingerprint
            if gate is not None:
                if _is_paper_risk_admission_adapter(gate):
                    risk_permit = gate.reserve_for_execution(
                        intent, writer_lease=writer_lease
                    )
                else:
                    authenticated_reserve = getattr(
                        gate, "reserve_for_authenticated_execution", None
                    )
                    if callable(authenticated_reserve):
                        if (
                            basic_gate is None
                            or type(admission_context) is not ExecutionAdmissionContextV1
                            or basic_reference is None
                        ):
                            raise AdmissionRequired(
                                "authenticated Risk reservation requires Basic admission"
                            )
                        risk_permit = authenticated_reserve(
                            intent,
                            admission_context=admission_context,
                            writer_lease=writer_lease,
                            dispatch_port=dispatcher,
                        )
                    else:
                        risk_permit = gate.reserve(intent)
        except ExecutionBasicAdmissionDenied:
            if basic_reference is not None and basic_gate is not None:
                basic_gate.revoke(basic_reference)
            return self._store.reject_intent(
                intent.intent_id,
                self._scope,
                "basic_admission_denied",
                blocked=True,
                writer_lease=writer_lease,
            )
        except AdmissionDenied:
            if basic_reference is not None and basic_gate is not None:
                basic_gate.revoke(basic_reference)
            return self._store.reject_intent(
                intent.intent_id, self._scope, AdmissionDenied.code, writer_lease=writer_lease
            )
        except PermissionError:
            if basic_reference is not None and basic_gate is not None:
                basic_gate.revoke(basic_reference)
            return self._store.reject_intent(
                intent.intent_id, self._scope, AdmissionDenied.code, writer_lease=writer_lease
            )
        except WriterLeaseUnavailable:
            if basic_reference is not None and basic_gate is not None:
                basic_gate.revoke(basic_reference)
            raise
        except Exception:
            if basic_reference is not None and basic_gate is not None:
                basic_gate.revoke(basic_reference)
            return self._store.reject_intent(
                intent.intent_id,
                self._scope,
                "admission_gate_error",
                blocked=True,
                writer_lease=writer_lease,
            )
        if gate is not None:
            if risk_permit is None:
                if basic_reference is not None and basic_gate is not None:
                    basic_gate.revoke(basic_reference)
                return self._store.reject_intent(
                    intent.intent_id,
                    self._scope,
                    "invalid_admission_permit",
                    blocked=True,
                    writer_lease=writer_lease,
                )
            try:
                risk_reference = self._permit_reference(risk_permit)
            except ContractValidationError:
                if basic_reference is not None and basic_gate is not None:
                    basic_gate.revoke(basic_reference)
                return self._reject_after_permit_reference_failure(
                    intent.intent_id, "invalid_admission_permit_reference", writer_lease
                )
            if risk_reference is None:
                if basic_reference is not None and basic_gate is not None:
                    basic_gate.revoke(basic_reference)
                return self._reject_after_permit_reference_failure(
                    intent.intent_id, "admission_permit_reference_required", writer_lease
                )
        try:
            record = self._store.activate_intent(
                intent.intent_id,
                self._scope,
                risk_reference,
                basic_admission_reference=basic_reference,
                admission_context_sha256=context_fingerprint,
                writer_lease=writer_lease,
            )
        except WriterLeaseUnavailable:
            # Never let an expired owner mutate the risk reservation after a
            # newer writer acquired the execution authority.
            raise
        except Exception:
            if risk_reference is not None:
                self._release_permit(risk_reference, "admission_activation_failed", writer_lease)
            if basic_reference is not None and basic_gate is not None:
                basic_gate.revoke(basic_reference)
            raise
        return record

    @staticmethod
    def _permit_reference(permit: Any) -> str | None:
        """Persist only an opaque, non-secret permit reference when exposed."""

        candidate: Any = None
        if isinstance(permit, str):
            candidate = permit
        elif isinstance(permit, Mapping):
            candidate = permit.get("permit_id") or permit.get("reference")
        else:
            candidate = getattr(permit, "permit_id", None) or getattr(permit, "reference", None)
        if candidate is None:
            return None
        if (
            not isinstance(candidate, str)
            or candidate != candidate.strip()
            or not 1 <= len(candidate) <= 128
            or not candidate.replace("-", "").replace("_", "").replace(".", "").isalnum()
        ):
            raise ContractValidationError("invalid admission permit reference")
        return candidate

    def _reject_after_permit_reference_failure(
        self,
        intent_id: str,
        reason_code: str,
        writer_lease: WriterLease,
    ) -> ExecutionRecord:
        self._store.reject_intent(
            intent_id,
            self._scope,
            reason_code,
            blocked=True,
            writer_lease=writer_lease,
        )
        return self._store.mark_review_required(
            intent_id,
            self._scope,
            "admission_release_required",
            writer_lease=writer_lease,
        )

    def _release_permit(
        self, permit_reference: str, reason_code: str, writer_lease: WriterLease
    ) -> bool:
        gate = self._admission_gate
        if gate is None:
            return True
        try:
            self._store.assert_writer_lease(self._scope, writer_lease)
            gate.release(permit_reference, reason_code)
        except Exception:
            return False
        return True

    def _release_basic_permit(self, permit_reference: str) -> None:
        gate = self._basic_admission_gate
        if gate is not None:
            gate.revoke(permit_reference)

    def _validate_and_claim_before_dispatch(
        self,
        intent: OrderIntent,
        record: ExecutionRecord,
        writer_lease: WriterLease,
        dispatcher: DispatchPort | DispatchCallable,
        admission_context: ExecutionAdmissionContextV1 | None,
        *,
        group_binding: GroupPermitBinding | None = None,
        group_authorization: GroupLegAuthorization | None = None,
        after_group_claim: AfterGroupClaimCallable | None = None,
        approved_target_path: bool = False,
    ) -> tuple[ExecutionRecord, ExecutionAdmissionAuthorizationV1 | None]:
        authorization: ExecutionAdmissionAuthorizationV1 | None = None
        gate = self._admission_gate
        if group_binding is not None:
            reference = record.permit_reference
            claimer = None if gate is None else getattr(gate, "claim_group_leg", None)
            if (
                reference != group_binding.permit_id
                or not callable(claimer)
                or group_authorization is None
            ):
                return (
                    self._store.mark_unknown(
                        intent.intent_id,
                        self._scope,
                        "group_risk_claim_unavailable",
                        writer_lease=writer_lease,
                    ),
                    None,
                )
            try:
                self._store.assert_writer_lease(self._scope, writer_lease)
                receipt = claimer(
                    intent,
                    risk_scope_fingerprint=group_binding.risk_scope_fingerprint,
                    group_id=group_binding.group_id,
                    leg_id=group_binding.leg_id,
                    permit_reference=group_binding.permit_id,
                    group_fingerprint=group_binding.group_fingerprint,
                    deadline_ns=group_binding.deadline_ns,
                )
            except WriterLeaseUnavailable:
                raise
            except Exception:
                # The account Risk owner may have committed the one-shot claim
                # before a response was lost. Preserve both ledgers for same-ID
                # reconciliation; neither this path nor recovery may retry it.
                return (
                    self._store.mark_unknown(
                        intent.intent_id,
                        self._scope,
                        "group_risk_claim_unknown",
                        writer_lease=writer_lease,
                    ),
                    None,
                )
            if not self._group_claim_receipt_matches(receipt, intent, group_binding):
                return (
                    self._store.mark_unknown(
                        intent.intent_id,
                        self._scope,
                        "group_risk_claim_mismatch",
                        writer_lease=writer_lease,
                    ),
                    None,
                )
            # The claim is one-shot and durable. The hook may record only the
            # exact validated claim facts needed by the provider evidence
            # boundary; its return value is deliberately ignored. The caller's
            # authority context remains held until this facade call returns.
            self._store.assert_writer_lease(self._scope, writer_lease)
            try:
                group_authorization.validate_binding(group_binding)
                group_authorization.validate_writer_lease(writer_lease)
                if after_group_claim is not None:
                    after_group_claim(
                        intent,
                        group_binding,
                        group_authorization,
                        writer_lease,
                        receipt,
                    )
            except Exception:
                return (
                    self._store.mark_unknown(
                        intent.intent_id,
                        self._scope,
                        "group_claim_hook_failed",
                        writer_lease=writer_lease,
                    ),
                    None,
                )
            return record, None
        if approved_target_path:
            record, authorization = self._claim_basic_admission(
                intent,
                record,
                writer_lease,
                dispatcher,
                admission_context,
                blocked_reason="APPROVAL_USED_NOT_DISPATCHED",
            )
            if record.state is not ExecutionState.DISPATCHING:
                return record, None
        if gate is not None:
            reference = record.permit_reference
            if reference is None:
                return (
                    self._block_before_dispatch(
                        intent,
                        "APPROVAL_USED_NOT_DISPATCHED"
                        if approved_target_path
                        else "admission_validation_unavailable",
                        writer_lease,
                    ),
                    None,
                )
            claimer = getattr(gate, "claim_for_dispatch", None)
            if approved_target_path and not callable(claimer):
                return (
                    self._block_before_dispatch(
                        intent, "APPROVAL_USED_NOT_DISPATCHED", writer_lease
                    ),
                    None,
                )
            if callable(claimer):
                try:
                    self._store.assert_writer_lease(self._scope, writer_lease)
                    if _is_paper_risk_admission_adapter(gate):
                        gate.claim_for_dispatch_for_execution(
                            reference,
                            intent,
                            writer_lease=writer_lease,
                        )
                    else:
                        claimer(reference, intent)
                except WriterLeaseUnavailable:
                    raise
                except AdmissionDenied:
                    if approved_target_path:
                        return (
                            self._block_before_dispatch(
                                intent, "APPROVAL_USED_NOT_DISPATCHED", writer_lease
                            ),
                            None,
                        )
                    return (
                        self._block_before_dispatch(intent, "admission_claim_failed", writer_lease),
                        None,
                    )
                except Exception as error:
                    if approved_target_path:
                        # The Risk adapter's exact typed policy denial proves
                        # its claim had no effect. Other exceptions may follow
                        # a committed claim and must remain unresolved.
                        if _is_typed_risk_admission_denial(error):
                            return (
                                self._block_before_dispatch(
                                    intent, "APPROVAL_USED_NOT_DISPATCHED", writer_lease
                                ),
                                None,
                            )
                        return (
                            self._store.mark_unknown(
                                intent.intent_id,
                                self._scope,
                                "approved_risk_claim_unknown",
                                writer_lease=writer_lease,
                            ),
                            None,
                        )
                    return (
                        self._block_before_dispatch(intent, "admission_claim_failed", writer_lease),
                        None,
                    )
            else:
                validator = getattr(gate, "validate", None)
                if not callable(validator):
                    return (
                        self._block_before_dispatch(
                            intent, "admission_validation_unavailable", writer_lease
                        ),
                        None,
                    )
                try:
                    self._store.assert_writer_lease(self._scope, writer_lease)
                    validator(reference, intent)
                except Exception:
                    return (
                        self._block_before_dispatch(
                            intent, "admission_validation_failed", writer_lease
                        ),
                        None,
                    )

        if approved_target_path:
            return record, authorization
        return self._claim_basic_admission(
            intent,
            record,
            writer_lease,
            dispatcher,
            admission_context,
            blocked_reason="basic_admission_claim_failed",
        )

    def _claim_basic_admission(
        self,
        intent: OrderIntent,
        record: ExecutionRecord,
        writer_lease: WriterLease,
        dispatcher: DispatchPort | DispatchCallable,
        admission_context: ExecutionAdmissionContextV1 | None,
        *,
        blocked_reason: str,
    ) -> tuple[ExecutionRecord, ExecutionAdmissionAuthorizationV1 | None]:
        authorization: ExecutionAdmissionAuthorizationV1 | None = None
        basic_gate = self._basic_admission_gate
        if basic_gate is not None:
            reference = record.basic_admission_reference
            if (
                reference is None
                or type(admission_context) is not ExecutionAdmissionContextV1
                or record.admission_context_sha256 != admission_context.fingerprint
            ):
                return (
                    self._block_before_dispatch(
                        intent, blocked_reason, writer_lease
                    ),
                    None,
                )
            try:
                self._store.assert_writer_lease(self._scope, writer_lease)
                authorization = basic_gate.claim_for_dispatch(
                    reference,
                    intent,
                    admission_context,
                    dispatcher,
                    writer_lease,
                )
            except Exception:
                return (
                    self._block_before_dispatch(
                        intent, blocked_reason, writer_lease
                    ),
                    None,
                )
        return record, authorization

    @staticmethod
    def _group_claim_receipt_matches(
        receipt: object, intent: OrderIntent, binding: GroupPermitBinding
    ) -> bool:
        """Verify the account Risk adapter's immutable one-shot claim receipt."""

        try:
            scope = receipt.scope
            permit = receipt.permit
            claim = receipt.claim
            quantity = receipt.actual_quantity
            notional = receipt.actual_notional
            return (
                type(receipt).__name__ == "RiskGroupLegClaimReceipt"
                and receipt.group_id == binding.group_id
                and receipt.leg_id == binding.leg_id
                and receipt.group_fingerprint == binding.group_fingerprint
                and receipt.policy_fingerprint == binding.policy_fingerprint
                and receipt.deadline_ns == binding.deadline_ns
                and receipt.permit_reference == binding.permit_id
                and receipt.reserved_intent_hash == binding.reserved_intent_hash
                and receipt.actual_intent_hash == binding.actual_child_intent_hash
                and receipt.exposure_profile_fingerprint == binding.exposure_profile_fingerprint
                and type(quantity) is Decimal
                and quantity == intent.quantity
                and type(notional) is Decimal
                and notional.is_finite()
                and notional > 0
                and scope.provider == intent.scope.provider
                and scope.environment == intent.scope.environment
                and risk_account_scope_fingerprint(scope.key) == binding.risk_scope_fingerprint
                and permit.permit_id == binding.permit_id
                and permit.intent_id == intent.intent_id
                and permit.scope == scope
                and claim.scope == scope
                and claim.permit_id == binding.permit_id
                and claim.intent_id == intent.intent_id
                and claim.intent_hash == binding.reserved_intent_hash
            )
        except (AttributeError, TypeError, ValueError):
            return False

    def _block_before_dispatch(
        self, intent: OrderIntent, reason_code: str, writer_lease: WriterLease
    ) -> ExecutionRecord:
        record = self._store.block_claimed_dispatch(
            intent.intent_id, self._scope, reason_code, writer_lease=writer_lease
        )
        if record.basic_admission_reference is not None:
            self._release_basic_permit(record.basic_admission_reference)
        reference = record.permit_reference
        if record.group_binding is not None:
            # Only the group vector owner can release an unclaimed reservation;
            # a singleton release would break its paired-leg accounting.
            return record
        if reference is None or self._release_permit(reference, reason_code, writer_lease):
            return record
        return self._store.mark_review_required(
            intent.intent_id,
            self._scope,
            "admission_release_failed",
            writer_lease=writer_lease,
        )

    def _settle_after_evidenced_outcome(
        self,
        record: ExecutionRecord,
        *,
        settle_permit: bool = True,
        writer_lease: WriterLease,
    ) -> ExecutionRecord:
        settled_record, _settlement_ok = self._settle_after_evidenced_outcome_result(
            record, settle_permit=settle_permit, writer_lease=writer_lease
        )
        return settled_record

    def _settle_after_evidenced_outcome_result(
        self,
        record: ExecutionRecord,
        *,
        settle_permit: bool = True,
        writer_lease: WriterLease,
    ) -> tuple[ExecutionRecord, bool]:
        if (
            not settle_permit
            or record.state
            in {
                ExecutionState.PENDING_ADMISSION,
                ExecutionState.PENDING_DISPATCH,
                ExecutionState.DISPATCHING,
                ExecutionState.UNKNOWN,
                ExecutionState.BLOCKED,
            }
            or self._admission_gate is None
        ):
            return record, True
        if record.permit_reference is None:
            return (
                self._store.mark_review_required(
                    record.intent_id,
                    self._scope,
                    "admission_settlement_unavailable",
                    writer_lease=writer_lease,
                ),
                False,
            )
        try:
            self._store.assert_writer_lease(self._scope, writer_lease)
            self._admission_gate.settle(record.permit_reference)
        except WriterLeaseUnavailable:
            raise
        except Exception:
            return (
                self._store.mark_review_required(
                    record.intent_id,
                    self._scope,
                    "admission_settlement_failed",
                    writer_lease=writer_lease,
                ),
                False,
            )
        resolve_paper_outcome = getattr(
            self._admission_gate, "after_evidenced_outcome", None
        )
        if callable(resolve_paper_outcome):
            try:
                self._store.assert_writer_lease(self._scope, writer_lease)
                resolve_paper_outcome(record, writer_lease=writer_lease)
            except WriterLeaseUnavailable:
                raise
            except Exception:
                return (
                    self._store.mark_review_required(
                        record.intent_id,
                        self._scope,
                        "paper_risk_resolution_failed",
                        writer_lease=writer_lease,
                    ),
                    False,
                )
        return record, True

    def _after_evidenced_observation_result(
        self,
        record: ExecutionRecord,
        event: ExecutionEvent | None,
        *,
        writer_lease: WriterLease,
    ) -> tuple[ExecutionRecord, ExecutionEvent | None, bool]:
        """Notify an optional gate hook for each newly persisted provider event.

        Unlike ``after_evidenced_outcome``, this hook is observation-scoped and
        may run for later callbacks after the admission permit was already
        settled. It receives the event returned by the same store transaction;
        exact replays have no event and are never notified. The hook must only
        reconcile durable evidence and must not dispatch another provider
        request.
        """

        if event is None:
            return record, None, True
        try:
            callback = getattr(self._admission_gate, "after_evidenced_observation", None)
            if not callable(callback):
                return record, event, True
            self._store.assert_writer_lease(self._scope, writer_lease)
            callback(record, event, writer_lease=writer_lease)
        except WriterLeaseUnavailable:
            raise
        except Exception:
            marked = self._store.mark_review_required(
                record.intent_id,
                self._scope,
                "evidenced_observation_reconciliation_failed",
                writer_lease=writer_lease,
            )
            return marked, None, False
        return record, event, True

    @staticmethod
    def _dispatch(
        dispatcher: DispatchPort | DispatchCallable,
        intent: OrderIntent,
        *,
        authorization: ExecutionAdmissionAuthorizationV1 | None = None,
    ) -> ProviderObservation | object:
        if authorization is not None:
            method = getattr(dispatcher, "submit_authorized", None)
            if not callable(method):
                raise ContractValidationError("authorized dispatch port is unavailable")
            result = method(intent, authorization)
        else:
            method = getattr(dispatcher, "submit", None)
            if callable(method):
                result = method(intent)
            elif callable(dispatcher):
                result = dispatcher(intent)
            else:
                raise ContractValidationError("invalid dispatch port")
        from .paper_execution import PaperDispatchResultV1

        if isinstance(result, PaperDispatchResultV1):
            return result
        if not isinstance(result, ProviderObservation):
            raise ContractValidationError("dispatch port returned invalid observation")
        return result

    @staticmethod
    def _query(
        reconciliation_port: ReconciliationPort
        | Callable[[OrderIntent], ProviderObservation | None],
        intent: OrderIntent,
    ) -> ProviderObservation | None:
        method = getattr(reconciliation_port, "query", None)
        if callable(method):
            result = method(intent)
        elif callable(reconciliation_port):
            result = reconciliation_port(intent)
        else:
            raise ContractValidationError("invalid reconciliation port")
        if result is not None and not isinstance(result, ProviderObservation):
            raise ContractValidationError("reconciliation port returned invalid observation")
        return result

    def _require_scope(self, intent: OrderIntent) -> None:
        if intent.scope != self._scope:
            raise ContractValidationError("intent scope does not match execution facade")

    def _require_owned_record(self, intent_id: str) -> ExecutionRecord:
        record = self.get(intent_id)
        if record is None:
            raise ContractValidationError("unknown intent_id")
        return record

    def _load_intent(self, intent_id: str) -> OrderIntent:
        """Rebuild the immutable local intent needed by a reconciliation port.

        The public store intentionally exposes no raw mutable dictionary API;
        rebuilding here keeps the port typed.  This reconstruction is limited to
        the package's own version-1 schema.
        """

        intent = self._store.get_intent(intent_id, scope=self._scope)
        if intent is None:
            raise ContractValidationError("unknown intent_id")
        self._require_scope(intent)
        return intent
