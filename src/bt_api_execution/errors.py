"""Stable failure types for the provider-neutral execution boundary."""

from __future__ import annotations


class ExecutionError(RuntimeError):
    """Base class with a stable, non-secret reason code."""

    code = "execution_error"

    def __init__(self, detail: str | None = None) -> None:
        self.detail = detail or self.code
        super().__init__(self.detail)


class ContractValidationError(ExecutionError):
    """A public contract failed structural or semantic validation."""

    code = "invalid_execution_contract"


class IntentConflictError(ExecutionError):
    """One intent id was reused with different immutable payloads."""

    code = "intent_payload_conflict"


class InvalidStateTransitionError(ExecutionError):
    """A journal update would violate the managed child state machine."""

    code = "invalid_execution_state_transition"


class WriterLeaseUnavailableError(ExecutionError):
    """Another non-expired owner holds the scope's writer lease."""

    code = "writer_lease_unavailable"


class AdmissionRequiredError(ExecutionError):
    """Managed dispatch was attempted without a configured admission gate."""

    code = "admission_gate_required"


class AdmissionDeniedError(ExecutionError):
    """An injected admission gate denied a new risk-increasing intent."""

    code = "admission_denied"


class ExecutionUnknownError(ExecutionError):
    """A prior dispatch is uncertain and must be reconciled, never replayed."""

    code = "execution_unknown"


class DurableStoreError(ExecutionError):
    """The durable execution store could not safely complete an operation."""

    code = "durable_execution_store_error"


# Short aliases are part of the deliberately small public API.  The concrete
# classes keep an ``Error`` suffix for standard exception naming conventions.
InvalidStateTransition = InvalidStateTransitionError
WriterLeaseUnavailable = WriterLeaseUnavailableError
AdmissionRequired = AdmissionRequiredError
AdmissionDenied = AdmissionDeniedError
ExecutionUnknown = ExecutionUnknownError
