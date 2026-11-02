"""Typed, versioned attestation ports injected into the service recovery journal.

``bt_api_execution.service_recovery`` owns the durable logical-run recovery
manifest: immutable submit/cancel intents, committed event receipts, local
projections and at-least-once callback work.  Two of its validation steps bind
an *execution context* — the immutable feed, bar, commission and quote facts
captured when one intent was created — and rebind every projected paper fill to
that stored context.

Those two steps were written inside the durable journal and hard-coded one
application's own object layout: a context was assumed to be a mapping of
``feed``/``decision``/``comminfo``/``quote_binding`` whose feed carries
``feed_class``/``timeframe``/``compression``/``timezone`` and whose commission
carries ``class_module``/``class_name`` plus a ten-key Backtrader commission
parameter set.  None of that is a durable-execution fact; it is the shape of one
framework's objects.  Keeping it in the SDK made the durable journal depend on a
domain it must not know, and made any other host application unable to reuse it.

This module replaces that with one explicit ``abc.ABC`` port, in the same shape
as ``journal_ports``:

* the SDK depends only on this abstract signature;
* the injecting application depends on the SDK, never the reverse;
* the port carries a ``port_version`` class attribute checked against
  :data:`SERVICE_RECOVERY_PORT_SCHEMA_VERSION` at injection time, so an old or
  newer implementation fails closed instead of half-working;
* implementations are real subclasses, so ``isinstance`` is a genuine boundary
  check rather than a duck-typed guess;
* every method receives the *already flattened* values, never a framework
  object, so no framework attribute access happens below this line.

This package never imports ``backtrader`` and never imports ``bt_api_ctp``; the
ports stay provider- and application-neutral.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from threading import RLock
from types import MappingProxyType
from typing import Any, ClassVar, Mapping, Optional

#: Wire/schema version of the port contract below.  Every injected port must
#: declare ``port_version`` as exactly this value; anything else is refused by
#: ``configure_service_recovery_ports`` with ``RECOVERY_PORT_VERSION_UNSUPPORTED``.
SERVICE_RECOVERY_PORT_SCHEMA_VERSION = 1

#: Port slot names accepted by ``configure_service_recovery_ports`` and reported
#: by the ``RECOVERY_PORT_INVALID:<name>`` rejection path.
SERVICE_RECOVERY_PORT_NAMES = ("execution_context_attestation",)


@dataclass(frozen=True)
class AttestedRecoveryExecutionContext:
    """Neutral projection of one validated intent execution context.

    Produced only by
    :meth:`ServiceRecoveryExecutionContextAttestationPort.attest_intent_context`.
    ``facts`` is the mapping the journal persists verbatim after re-applying its
    own JSON-safety normalization, so an implementation must return the very
    mapping it was given, unchanged in content, once it is satisfied: the journal
    compares a re-read stored context against this payload byte for byte, and a
    resumed run's fill projection is only released when they still agree.
    """

    facts: Mapping[str, Any]


@dataclass(frozen=True)
class AttestedRecoveryEventContext:
    """Neutral projection of one fill-to-context binding.

    Produced only by
    :meth:`ServiceRecoveryExecutionContextAttestationPort.attest_event_context`.
    ``facts`` is the flat fill context the journal durably stores next to the
    projected fill and replays on restart.  ``data_name``, ``instrument`` and
    ``commission_currency`` are the three values the journal itself compares
    against the intent binding, its submit payload and the requested currency,
    and must equal the corresponding ``facts`` values.
    """

    facts: Mapping[str, Any]
    data_name: str
    instrument: str
    commission_currency: str


class ServiceRecoveryExecutionContextAttestationPort(abc.ABC):
    """Application-owned validation of one run's immutable execution context.

    The journal asks exactly two questions and never inspects an application
    object, an application key layout, or a private symbol of the injecting
    package to answer them.

    Invariants an implementation must enforce itself, and must reject by raising
    ``ServiceExecutionRecoveryError`` with the exact code shown:

    ``attest_intent_context``
        * ``INVALID_INTENT_RECOVERY_CONTEXT`` for any context that is not this
          implementation's own execution-context shape, for any missing, extra,
          non-identifier, negatively valued or wrongly typed component, for a
          feed instrument that differs from the submit payload instrument, for a
          bar index below one, for a non-finite/ non-positive decimal-encoded
          ``datetime_hex``, for a commission mapping whose parameter keys are not
          exactly the ten scalar parameters this implementation records, or for a
          quote binding whose data name, instrument, bar index or encoded
          datetime disagrees with the feed or decision components;
        * no database access and no mutation: it must return
          :class:`AttestedRecoveryExecutionContext` carrying the same content it
          was given, so a later re-read of the stored context still matches.

    ``attest_event_context``
        * ``INVALID_RECOVERY_EVENT_CONTEXT`` when the supplied fill context is
          not a mapping carrying exactly this implementation's required event
          context keys;
        * ``RECOVERY_EVENT_CONTEXT_MISMATCH`` when the fill context disagrees
          with the journaled manifest binding, the stored execution context, or
          the validated paper fill on any bound field, including a
          ``quote_as_of_ns`` later than the fill's ``match_time_ns``;
        * it must return :class:`AttestedRecoveryEventContext` carrying the same
          content it was given, because the journal re-verifies the stored
          projection against it.

    Normalization of the returned ``facts`` to the journal's own JSON-safe durable
    form is deliberately performed by the journal, not by the port.
    """

    port_version: ClassVar[int]

    @abc.abstractmethod
    def attest_intent_context(
        self,
        raw: Mapping[str, Any],
        submit_payload: Mapping[str, Any],
    ) -> AttestedRecoveryExecutionContext:
        """Validate and attest the execution context captured at submit time."""

    @abc.abstractmethod
    def attest_event_context(
        self,
        raw: Mapping[str, Any],
        manifest: Mapping[str, Any],
        execution_context: Mapping[str, Any],
        paper_fill: Mapping[str, Any],
    ) -> AttestedRecoveryEventContext:
        """Bind one paper fill to the attested submit-time context."""


#: Abstract type accepted in each slot, keyed by
#: :data:`SERVICE_RECOVERY_PORT_NAMES`.
SERVICE_RECOVERY_PORT_TYPES: Mapping[str, type] = MappingProxyType(
    {
        "execution_context_attestation": ServiceRecoveryExecutionContextAttestationPort,
    }
)


def validate_service_recovery_port(name: str, port: Any) -> Any:
    """Return *port* when it is a current-version implementation of slot *name*.

    ``isinstance`` here is a genuine subclass test, not duck typing: an
    application module, a look-alike object, or the abstract class itself is
    refused with ``RECOVERY_PORT_INVALID:<name>``.  ``port_version`` must be an
    ``int`` equal to :data:`SERVICE_RECOVERY_PORT_SCHEMA_VERSION`; the ABC carries
    only an annotation, so an implementation that forgets to declare it is
    refused with ``RECOVERY_PORT_VERSION_UNSUPPORTED`` instead of inheriting a
    permissive default.
    """

    from .service_recovery import ServiceExecutionRecoveryError

    if name not in SERVICE_RECOVERY_PORT_TYPES:
        raise ServiceExecutionRecoveryError("RECOVERY_PORT_INVALID:" + str(name))
    if not isinstance(port, SERVICE_RECOVERY_PORT_TYPES[name]):
        raise ServiceExecutionRecoveryError("RECOVERY_PORT_INVALID:" + name)
    declared_version = getattr(type(port), "port_version", None)
    if type(declared_version) is not int or declared_version != SERVICE_RECOVERY_PORT_SCHEMA_VERSION:
        raise ServiceExecutionRecoveryError("RECOVERY_PORT_VERSION_UNSUPPORTED")
    return port


@dataclass(frozen=True)
class ServiceRecoveryPorts:
    """Immutable recovery port set; ``None`` means "not supplied"."""

    execution_context_attestation: Optional[ServiceRecoveryExecutionContextAttestationPort] = None

    def __post_init__(self) -> None:
        # Validate on construction so a hand-built container cannot smuggle a
        # module, a look-alike object or a stale-version implementation past
        # ``configure_service_recovery_ports`` and reach a call site.
        for name in SERVICE_RECOVERY_PORT_NAMES:
            port = getattr(self, name)
            if port is not None:
                validate_service_recovery_port(name, port)

    def require(self, name: str) -> Any:
        """Return one port or fail closed; there is deliberately no fallback.

        A recovery journal whose host never supplied the attestation cannot
        validate an application-shaped execution context, and it must not guess
        one: the absence is reported with the single colon-free code
        ``RECOVERY_CONTEXT_ATTESTATION_UNAVAILABLE`` so every caller's finite
        rejection mapping keeps working.
        """

        from .service_recovery import ServiceExecutionRecoveryError

        if name not in SERVICE_RECOVERY_PORT_NAMES:
            raise ValueError("unknown service recovery port slot: " + str(name))
        if getattr(self, name) is None:
            raise ServiceExecutionRecoveryError("RECOVERY_CONTEXT_ATTESTATION_UNAVAILABLE")
        return getattr(self, name)


_SERVICE_RECOVERY_PORTS = ServiceRecoveryPorts()
_SERVICE_RECOVERY_PORTS_LOCK = RLock()


def configure_service_recovery_ports(
    *,
    execution_context_attestation: Any = None,
    replace: bool = False,
) -> None:
    """Inject the attestation ports for recovery journals without their own set.

    Each supplied port must be a current-version implementation of the matching
    abstract port.  An already-set port is never replaced by a *different*
    instance unless the caller passes ``replace=True``: a silent swap would let a
    later import quietly change how an already-open journal validates its stored
    contexts.  Re-configuring the identical instance is always allowed and is a
    no-op.
    """

    from .service_recovery import ServiceExecutionRecoveryError

    if type(replace) is not bool:
        raise ServiceExecutionRecoveryError("RECOVERY_PORT_INVALID:replace")
    supplied = {"execution_context_attestation": execution_context_attestation}
    with _SERVICE_RECOVERY_PORTS_LOCK:
        current = _SERVICE_RECOVERY_PORTS
        values: dict[str, Any] = {name: getattr(current, name) for name in SERVICE_RECOVERY_PORT_NAMES}
        changed = False
        for name in SERVICE_RECOVERY_PORT_NAMES:
            port = supplied[name]
            if port is None:
                continue
            validate_service_recovery_port(name, port)
            existing = values[name]
            if existing is not None and existing is not port:
                if not replace:
                    raise ServiceExecutionRecoveryError("RECOVERY_PORT_ALREADY_CONFIGURED:" + name)
                values[name] = None
            if existing is not port:
                values[name] = port
                changed = True
        if changed:
            globals()["_SERVICE_RECOVERY_PORTS"] = ServiceRecoveryPorts(**values)


def reset_service_recovery_ports() -> None:
    """Clear the process-registered ports; test isolation only."""

    with _SERVICE_RECOVERY_PORTS_LOCK:
        globals()["_SERVICE_RECOVERY_PORTS"] = ServiceRecoveryPorts()


def service_recovery_ports() -> ServiceRecoveryPorts:
    """Return the process-registered port set."""

    return _SERVICE_RECOVERY_PORTS


__all__ = [
    "AttestedRecoveryEventContext",
    "AttestedRecoveryExecutionContext",
    "SERVICE_RECOVERY_PORT_NAMES",
    "SERVICE_RECOVERY_PORT_SCHEMA_VERSION",
    "SERVICE_RECOVERY_PORT_TYPES",
    "ServiceRecoveryExecutionContextAttestationPort",
    "ServiceRecoveryPorts",
    "configure_service_recovery_ports",
    "reset_service_recovery_ports",
    "service_recovery_ports",
    "validate_service_recovery_port",
]
