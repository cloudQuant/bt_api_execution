"""Public managed execution facade with no concrete provider dependency."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from threading import RLock
from typing import TYPE_CHECKING, Any

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
    InvalidStateTransition,
    WriterLeaseUnavailable,
)

if TYPE_CHECKING:
    from .ports import AdmissionGate, DispatchPort, ReconciliationPort
    from .store import (
        CtpAccountFamilyOwnerHandle,
        ExecutionRecord,
        SqliteExecutionStore,
        WriterLease,
    )

DispatchCallable = Callable[[OrderIntent], ProviderObservation]
BeforeDispatchCallable = Callable[[OrderIntent], None]

_OBSERVATION_RECORDING_STATES = frozenset(
    {
        ExecutionState.DISPATCHING,
        ExecutionState.UNKNOWN,
        ExecutionState.ACKED,
        ExecutionState.PARTIALLY_FILLED,
    }
)
_PERMIT_SETTLEMENT_SOURCE_STATES = frozenset({ExecutionState.DISPATCHING, ExecutionState.UNKNOWN})


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
        lease_ttl_ns: int = 5_000_000_000,
        allow_unprotected: bool = False,
        pre_dispatch_guard: BeforeDispatchCallable | None = None,
    ) -> None:
        if not writer_id or writer_id != writer_id.strip() or len(writer_id) > 128:
            raise ContractValidationError("invalid writer_id")
        if type(allow_unprotected) is not bool:
            raise ContractValidationError("invalid allow_unprotected")
        if pre_dispatch_guard is not None and not callable(pre_dispatch_guard):
            raise ContractValidationError("invalid pre-dispatch guard")
        self._store = store
        self._scope = scope
        self._writer_id = writer_id
        self._admission_gate = admission_gate
        self._lease_ttl_ns = lease_ttl_ns
        self._allow_unprotected = allow_unprotected
        self._pre_dispatch_guard = pre_dispatch_guard
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

    def close(self) -> bool:
        """Release only this facade's lease; the store remains caller-owned."""

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
    ) -> ExecutionRecord:
        """Durably admit and dispatch an intent at most once.

        Repeating a durable `PENDING_DISPATCH`, `DISPATCHING`, terminal, or
        `UNKNOWN` intent returns its record without a second dispatch.  A caller
        may only resolve an unknown result through :meth:`reconcile`.

        The constructor-level ``pre_dispatch_guard`` runs for every submit
        after the durable dispatch claim and final admission validation, but
        before the injected provider port.  ``before_dispatch`` is an optional
        per-call guard that runs after it.  A failing guard durably blocks the
        claimed dispatch and the port is not invoked.  This provides a narrow
        place for a composition root to persist an additional local safety
        latch without giving this package an import edge to a risk
        implementation.
        """

        if before_dispatch is not None and not callable(before_dispatch):
            raise ContractValidationError("invalid pre-dispatch guard")
        self._require_scope(intent)
        writer_lease = self.acquire_writer_lease()
        record = self._store.admit_intent(intent, writer_lease=writer_lease)
        if record.state is ExecutionState.PENDING_ADMISSION:
            record = self._admit(intent, writer_lease)
        if record.state is not ExecutionState.PENDING_DISPATCH:
            return record
        record, claimed = self._store.claim_for_dispatch(
            intent.intent_id, self._scope, writer_lease=writer_lease
        )
        if not claimed:
            return record
        record = self._validate_and_claim_before_dispatch(intent, record, writer_lease)
        if record.state is not ExecutionState.DISPATCHING:
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
        # A slow coordinator hook must not let an expired owner invoke the
        # provider with an old writer generation.
        self._store.assert_writer_lease(self._scope, writer_lease)
        try:
            observation = self._dispatch(dispatcher, intent)
        except Exception:
            return self._store.mark_unknown(
                intent.intent_id, self._scope, writer_lease=writer_lease
            )
        if observation.intent_id != intent.intent_id:
            return self._store.mark_unknown(
                intent.intent_id,
                self._scope,
                "dispatch_identity_mismatch",
                writer_lease=writer_lease,
            )
        try:
            record = self._store.record_observation(
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
        return self._settle_after_evidenced_outcome(record, writer_lease=writer_lease)

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

    def record_provider_observation_event(
        self, observation: ProviderObservation
    ) -> ExecutionEvent | None:
        """Persist one observation and return only its newly appended outbox event.

        Exact retries return ``None``. Invalid evidence and permit-settlement
        failures first persist a review-required latch, then raise
        :class:`InvalidStateTransition` so callers cannot confuse them with a
        duplicate. The returned event is the same immutable row created by the
        store transaction; this method does not publish to monitor or call a
        provider port.
        """

        _record, event, review_required = self._record_provider_observation_with_event(observation)
        if review_required:
            raise InvalidStateTransition("provider observation requires manual review")
        return event

    def _record_provider_observation_with_event(
        self, observation: ProviderObservation
    ) -> tuple[ExecutionRecord, ExecutionEvent | None, bool]:
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
        return settled_record, event if settlement_ok else None, not settlement_ok

    def reconcile_with_port(
        self,
        intent_id: str,
        reconciliation_port: ReconciliationPort
        | Callable[[OrderIntent], ProviderObservation | None],
    ) -> ExecutionRecord:
        """Query an injected evidence port without dispatching another provider order."""

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

    def _admit(self, intent: OrderIntent, writer_lease: WriterLease) -> ExecutionRecord:
        gate = self._admission_gate
        if gate is None:
            if not self._allow_unprotected:
                raise AdmissionRequired()
            return self._store.activate_intent(
                intent.intent_id, self._scope, writer_lease=writer_lease
            )
        try:
            self._store.assert_writer_lease(self._scope, writer_lease)
            permit = gate.reserve(intent)
        except AdmissionDenied:
            return self._store.reject_intent(
                intent.intent_id, self._scope, AdmissionDenied.code, writer_lease=writer_lease
            )
        except PermissionError:
            return self._store.reject_intent(
                intent.intent_id, self._scope, AdmissionDenied.code, writer_lease=writer_lease
            )
        except Exception:
            return self._store.reject_intent(
                intent.intent_id,
                self._scope,
                "admission_gate_error",
                blocked=True,
                writer_lease=writer_lease,
            )
        if permit is None:
            return self._store.reject_intent(
                intent.intent_id,
                self._scope,
                "invalid_admission_permit",
                blocked=True,
                writer_lease=writer_lease,
            )
        try:
            permit_reference = self._permit_reference(permit)
        except ContractValidationError:
            return self._reject_after_permit_reference_failure(
                intent.intent_id, "invalid_admission_permit_reference", writer_lease
            )
        if permit_reference is None:
            return self._reject_after_permit_reference_failure(
                intent.intent_id, "admission_permit_reference_required", writer_lease
            )
        try:
            record = self._store.activate_intent(
                intent.intent_id,
                self._scope,
                permit_reference,
                writer_lease=writer_lease,
            )
        except WriterLeaseUnavailable:
            # Never let an expired owner mutate the risk reservation after a
            # newer writer acquired the execution authority.
            raise
        except Exception:
            self._release_permit(permit_reference, "admission_activation_failed", writer_lease)
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

    def _validate_and_claim_before_dispatch(
        self,
        intent: OrderIntent,
        record: ExecutionRecord,
        writer_lease: WriterLease,
    ) -> ExecutionRecord:
        gate = self._admission_gate
        if gate is None:
            # This path is intentionally reachable only for an explicitly
            # isolated local fixture; runtime preset resolution must not expose
            # it for any provider-capable route.
            return record
        reference = record.permit_reference
        if reference is None:
            return self._block_before_dispatch(
                intent, "admission_validation_unavailable", writer_lease
            )
        claimer = getattr(gate, "claim_for_dispatch", None)
        if callable(claimer):
            try:
                self._store.assert_writer_lease(self._scope, writer_lease)
                claimer(reference, intent)
            except Exception:
                return self._block_before_dispatch(intent, "admission_claim_failed", writer_lease)
            return record
        validator = getattr(gate, "validate", None)
        if not callable(validator):
            return self._block_before_dispatch(
                intent, "admission_validation_unavailable", writer_lease
            )
        try:
            self._store.assert_writer_lease(self._scope, writer_lease)
            validator(reference, intent)
        except Exception:
            return self._block_before_dispatch(intent, "admission_validation_failed", writer_lease)
        return record

    def _block_before_dispatch(
        self, intent: OrderIntent, reason_code: str, writer_lease: WriterLease
    ) -> ExecutionRecord:
        record = self._store.block_claimed_dispatch(
            intent.intent_id, self._scope, reason_code, writer_lease=writer_lease
        )
        reference = record.permit_reference
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
        return record, True

    @staticmethod
    def _dispatch(
        dispatcher: DispatchPort | DispatchCallable,
        intent: OrderIntent,
    ) -> ProviderObservation:
        method = getattr(dispatcher, "submit", None)
        if callable(method):
            result = method(intent)
        elif callable(dispatcher):
            result = dispatcher(intent)
        else:
            raise ContractValidationError("invalid dispatch port")
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
