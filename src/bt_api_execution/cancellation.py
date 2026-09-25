"""Durable, provider-neutral cancellation lifecycle.

Cancellation is deliberately separate from order submission.  It shares the
same execution scope, SQLite writer lease, and admission boundary, but its
immutable identity is a ``CancelIntent`` tied to a previously evidenced target
order.  A provider call occurs only after a durable single-use dispatch claim.
Any exception or ambiguous response becomes ``UNKNOWN`` and cannot be replayed
automatically.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Protocol

from .contracts import CancelIntent, CancelObservation, ExecutionScope, ExecutionState
from .errors import AdmissionDenied, AdmissionRequired, ContractValidationError, WriterLeaseUnavailable
from .store import WriterLease

if TYPE_CHECKING:
    from .store import CancelRecord, SqliteExecutionStore

CancelDispatchCallable = Callable[[CancelIntent], CancelObservation]
CancelBeforeDispatchCallable = Callable[[CancelIntent], None]


class CancelAdmissionGate(Protocol):
    """Narrow admission surface for a ``CancelIntent``.

    The execution package owns no risk DTO.  A composition root supplies an
    adapter that maps a cancellation to its account-owned ``IntentAction.CANCEL``
    risk intent and persists only an opaque permit reference here.
    """

    def reserve(self, intent: CancelIntent) -> Any: ...

    def validate(self, permit_reference: str, intent: CancelIntent) -> Any: ...

    def settle(self, permit_reference: str) -> Any: ...

    def release(self, permit_reference: str, reason: str) -> None: ...


class ManagedCancellationFacade:
    """Durably admit, dispatch, and reconcile a single cancellation request."""

    def __init__(
        self,
        store: SqliteExecutionStore,
        scope: ExecutionScope,
        *,
        acquire_writer_lease: Callable[[], Any],
        admission_gate: CancelAdmissionGate | None = None,
        allow_unprotected: bool = False,
    ) -> None:
        if not callable(acquire_writer_lease):
            raise ContractValidationError("cancellation writer lease acquirer is required")
        if type(allow_unprotected) is not bool:
            raise ContractValidationError("invalid cancellation allow_unprotected")
        self._store = store
        self._scope = scope
        self._acquire_writer_lease = acquire_writer_lease
        self._admission_gate = admission_gate
        self._allow_unprotected = allow_unprotected

    @property
    def scope(self) -> ExecutionScope:
        """Return the scope this cancellation facade is allowed to mutate."""

        return self._scope

    @property
    def store(self) -> SqliteExecutionStore:
        """Return the caller-owned durable store."""

        return self._store

    def cancel(
        self,
        intent: CancelIntent,
        dispatcher: CancelDispatchCallable | Any,
        *,
        before_dispatch: CancelBeforeDispatchCallable | None = None,
    ) -> CancelRecord:
        """Dispatch a cancellation at most once after durable admission.

        Repeated calls return their durable record without another provider
        call.  Unknown results must be resolved only by :meth:`reconcile`.
        """

        if before_dispatch is not None and not callable(before_dispatch):
            raise ContractValidationError("invalid cancellation pre-dispatch guard")
        self._require_scope(intent)
        writer_lease = self._require_writer_lease()
        record = self._store.admit_cancel(intent, writer_lease=writer_lease)
        if record.state is ExecutionState.PENDING_ADMISSION:
            record = self._admit(intent, writer_lease)
        if record.state is not ExecutionState.PENDING_DISPATCH:
            return record
        record, claimed = self._store.claim_cancel_for_dispatch(
            intent.cancel_id, self._scope, writer_lease=writer_lease
        )
        if not claimed:
            return record
        record = self._validate_before_dispatch(intent, record, writer_lease)
        if record.state is not ExecutionState.DISPATCHING:
            return record
        if before_dispatch is not None:
            try:
                before_dispatch(intent)
            except Exception:
                return self._block_before_dispatch(
                    intent, "cancel_pre_dispatch_guard_failed", writer_lease
                )
        self._store.assert_writer_lease(self._scope, writer_lease)
        try:
            observation = self._dispatch(dispatcher, intent)
        except Exception:
            return self._store.mark_cancel_unknown(
                intent.cancel_id, self._scope, writer_lease=writer_lease
            )
        if (
            observation.cancel_id != intent.cancel_id
            or observation.target_intent_id != intent.target_intent_id
            or observation.provider_order_id != intent.provider_order_id
        ):
            return self._store.mark_cancel_unknown(
                intent.cancel_id,
                self._scope,
                "cancel_dispatch_identity_mismatch",
                writer_lease=writer_lease,
            )
        try:
            record = self._store.record_cancel_observation(
                observation,
                scope=self._scope,
                source="provider",
                writer_lease=writer_lease,
            )
        except WriterLeaseUnavailable:
            raise
        except Exception:
            return self._store.mark_cancel_unknown(
                intent.cancel_id,
                self._scope,
                "invalid_cancel_provider_evidence",
                writer_lease=writer_lease,
            )
        return self._settle_after_evidenced_outcome(record, writer_lease=writer_lease)

    def reconcile(self, observation: CancelObservation) -> CancelRecord:
        """Persist externally obtained cancellation evidence without provider I/O."""

        writer_lease = self._require_writer_lease()
        record = self._require_owned_record(observation.cancel_id)
        if record.is_terminal:
            return record
        previous_state = record.state
        try:
            record = self._store.record_cancel_observation(
                observation,
                scope=self._scope,
                source="reconcile",
                writer_lease=writer_lease,
            )
        except WriterLeaseUnavailable:
            raise
        except Exception:
            return self._store.mark_cancel_review_required(
                observation.cancel_id,
                self._scope,
                "invalid_cancel_reconcile_evidence",
                writer_lease=writer_lease,
            )
        return self._settle_after_evidenced_outcome(
            record,
            settle_permit=previous_state
            in {
                ExecutionState.PENDING_ADMISSION,
                ExecutionState.PENDING_DISPATCH,
                ExecutionState.DISPATCHING,
                ExecutionState.UNKNOWN,
            },
            writer_lease=writer_lease,
        )

    def recover_interrupted_dispatches(self) -> tuple[CancelRecord, ...]:
        """Latch pre-crash dispatch claims as unknown without provider I/O.

        Call during startup while holding this facade's newly acquired writer
        lease.  A native callback may have been lost with the old process, so
        recovery must not infer cancellation success from current order state
        or from the absence of a callback.
        """

        writer_lease = self._require_writer_lease()
        return self._store.recover_interrupted_cancellations(
            self._scope,
            writer_lease=writer_lease,
        )

    def get(self, cancel_id: str) -> CancelRecord | None:
        """Read one cancellation record without dispatching anything."""

        record = self._store.get_cancel(cancel_id, scope=self._scope)
        if record is not None and record.scope_key != self._scope.key:
            raise ContractValidationError("cancel_id belongs to a different execution scope")
        return record

    def _admit(self, intent: CancelIntent, writer_lease: WriterLease) -> CancelRecord:
        gate = self._admission_gate
        if gate is None:
            if not self._allow_unprotected:
                raise AdmissionRequired()
            return self._store.activate_cancel(
                intent.cancel_id, self._scope, writer_lease=writer_lease
            )
        try:
            self._store.assert_writer_lease(self._scope, writer_lease)
            permit = gate.reserve(intent)
        except AdmissionDenied:
            return self._store.reject_cancel(
                intent.cancel_id, self._scope, AdmissionDenied.code, writer_lease=writer_lease
            )
        except PermissionError:
            return self._store.reject_cancel(
                intent.cancel_id, self._scope, AdmissionDenied.code, writer_lease=writer_lease
            )
        except Exception:
            return self._store.reject_cancel(
                intent.cancel_id,
                self._scope,
                "cancel_admission_gate_error",
                blocked=True,
                writer_lease=writer_lease,
            )
        if permit is None:
            return self._store.reject_cancel(
                intent.cancel_id,
                self._scope,
                "invalid_cancel_admission_permit",
                blocked=True,
                writer_lease=writer_lease,
            )
        try:
            permit_reference = self._permit_reference(permit)
        except ContractValidationError:
            return self._reject_after_permit_reference_failure(
                intent.cancel_id, "invalid_cancel_admission_permit_reference", writer_lease
            )
        if permit_reference is None:
            return self._reject_after_permit_reference_failure(
                intent.cancel_id, "cancel_admission_permit_reference_required", writer_lease
            )
        try:
            return self._store.activate_cancel(
                intent.cancel_id,
                self._scope,
                permit_reference,
                writer_lease=writer_lease,
            )
        except WriterLeaseUnavailable:
            raise
        except Exception:
            self._release_permit(
                permit_reference, "cancel_admission_activation_failed", writer_lease
            )
            raise

    @staticmethod
    def _permit_reference(permit: Any) -> str | None:
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
            raise ContractValidationError("invalid cancellation admission permit reference")
        return candidate

    def _reject_after_permit_reference_failure(
        self, cancel_id: str, reason_code: str, writer_lease: WriterLease
    ) -> CancelRecord:
        self._store.reject_cancel(
            cancel_id,
            self._scope,
            reason_code,
            blocked=True,
            writer_lease=writer_lease,
        )
        return self._store.mark_cancel_review_required(
            cancel_id,
            self._scope,
            "cancel_admission_release_required",
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

    def _validate_before_dispatch(
        self, intent: CancelIntent, record: CancelRecord, writer_lease: WriterLease
    ) -> CancelRecord:
        gate = self._admission_gate
        if gate is None:
            return record
        reference = record.permit_reference
        if reference is None:
            return self._block_before_dispatch(
                intent, "cancel_admission_validation_unavailable", writer_lease
            )
        try:
            self._store.assert_writer_lease(self._scope, writer_lease)
            gate.validate(reference, intent)
        except Exception:
            return self._block_before_dispatch(
                intent, "cancel_admission_validation_failed", writer_lease
            )
        return record

    def _block_before_dispatch(
        self, intent: CancelIntent, reason_code: str, writer_lease: WriterLease
    ) -> CancelRecord:
        record = self._store.block_claimed_cancel_dispatch(
            intent.cancel_id, self._scope, reason_code, writer_lease=writer_lease
        )
        reference = record.permit_reference
        if reference is None or self._release_permit(reference, reason_code, writer_lease):
            return record
        return self._store.mark_cancel_review_required(
            intent.cancel_id,
            self._scope,
            "cancel_admission_release_failed",
            writer_lease=writer_lease,
        )

    def _settle_after_evidenced_outcome(
        self,
        record: CancelRecord,
        *,
        settle_permit: bool = True,
        writer_lease: WriterLease,
    ) -> CancelRecord:
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
            return record
        if record.permit_reference is None:
            return self._store.mark_cancel_review_required(
                record.cancel_id,
                self._scope,
                "cancel_admission_settlement_unavailable",
                writer_lease=writer_lease,
            )
        try:
            self._store.assert_writer_lease(self._scope, writer_lease)
            self._admission_gate.settle(record.permit_reference)
        except WriterLeaseUnavailable:
            raise
        except Exception:
            return self._store.mark_cancel_review_required(
                record.cancel_id,
                self._scope,
                "cancel_admission_settlement_failed",
                writer_lease=writer_lease,
            )
        return record

    @staticmethod
    def _dispatch(
        dispatcher: CancelDispatchCallable | Any, intent: CancelIntent
    ) -> CancelObservation:
        method = getattr(dispatcher, "cancel", None)
        if callable(method):
            result = method(intent)
        elif callable(dispatcher):
            result = dispatcher(intent)
        else:
            raise ContractValidationError("invalid cancellation dispatch port")
        if not isinstance(result, CancelObservation):
            raise ContractValidationError("cancellation dispatch port returned invalid observation")
        return result

    def _require_scope(self, intent: CancelIntent) -> None:
        if intent.scope != self._scope:
            raise ContractValidationError("cancellation scope does not match execution facade")

    def _require_writer_lease(self) -> WriterLease:
        """Require a token-bearing lease; old callback-only leases are unsafe."""

        lease = self._acquire_writer_lease()
        if not isinstance(lease, WriterLease):
            raise ContractValidationError("cancellation writer lease lacks fencing token")
        return lease

    def _require_owned_record(self, cancel_id: str) -> CancelRecord:
        record = self.get(cancel_id)
        if record is None:
            raise ContractValidationError("unknown cancel_id")
        return record


__all__ = [
    "CancelAdmissionGate",
    "CancelBeforeDispatchCallable",
    "CancelDispatchCallable",
    "ManagedCancellationFacade",
]
