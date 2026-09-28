"""Narrow dependency-inversion ports; implementations live outside this package."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .contracts import OrderIntent, ProviderObservation


@runtime_checkable
class DispatchPort(Protocol):
    """Provider adapter port.  It is invoked only after durable admission."""

    def submit(self, intent: OrderIntent) -> ProviderObservation: ...


@runtime_checkable
class ReconciliationPort(Protocol):
    """Provider evidence port used to resolve a prior unknown outcome."""

    def query(self, intent: OrderIntent) -> ProviderObservation | None: ...


@runtime_checkable
class AdmissionGate(Protocol):
    """Duck-typed bridge to shared risk/admission contracts.

    The execution package does not own permit types.  ``reserve`` should be
    idempotent for an immutable intent id and return an object exposing an
    opaque ``permit_id`` or ``reference``.  Only that reference is persisted;
    the risk owner receives it again for settlement or release.
    """

    def reserve(self, intent: OrderIntent) -> Any: ...

    def validate(self, permit_reference: str, intent: OrderIntent) -> Any: ...

    def claim_for_dispatch(self, permit_reference: str, intent: OrderIntent) -> Any: ...

    def release(self, permit_reference: str, reason: str) -> None: ...

    def settle(self, permit_reference: str) -> Any: ...
