"""Typed, versioned journal ports injected by the owning application.

``bt_api_execution.journal`` owns durable decision and dispatch evidence.  A few
of its code paths need facts that belong to another domain: an application-owned
strategy position projection, an application-owned fee-pair fact projection, and
an application-owned registered-sim query receipt.  Those domains used to be
supplied as whole Python *module objects* through an untyped, versionless,
silently rewritable process global.  That shape let the SDK reach private
symbols of the injecting package and perform exact cross-repo class identity
checks (``type(x) is SomeOtherRepoClass``), both of which break silently on any
module move.

This module replaces it with three explicit ``abc.ABC`` ports:

* the SDK depends only on these abstract signatures;
* the implementing application depends on the SDK, never the reverse;
* every port carries a ``port_version`` class attribute checked against
  :data:`JOURNAL_PORT_SCHEMA_VERSION` at injection time, so an old or newer
  implementation fails closed instead of half-working;
* implementations are real subclasses, so ``isinstance`` is a genuine boundary
  check rather than a duck-typed guess.

This package never imports ``backtrader`` and never imports ``bt_api_ctp``; the
ports stay provider- and application-neutral.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any, ClassVar, Mapping, Optional, Tuple

#: Wire/schema version of the port contract below.  Every injected port must
#: declare ``port_version`` as exactly this value; anything else is refused by
#: ``configure_journal_ports`` with ``JOURNAL_PORT_VERSION_UNSUPPORTED``.
JOURNAL_PORT_SCHEMA_VERSION = 1

#: Port slot names accepted by ``configure_journal_ports`` and reported by the
#: ``JOURNAL_PORT_INVALID:<name>`` / ``JOURNAL_PORT_UNAVAILABLE:<name>`` codes.
JOURNAL_PORT_NAMES = ("position_attribution", "fee_pair_facts", "registered_sim_attestation")


class JournalPositionAttributionPort(abc.ABC):
    """Application-owned strategy position attribution written in-transaction.

    Implementations own the ``factor_position_*`` tables that live inside the
    execution journal's own SQLite database.  Every method receives the
    *journal's* live cursor and must write on it inside the caller's
    transaction: the journal neither commits nor rolls back on the
    implementation's behalf, and an implementation failure must abort the whole
    decision transaction.

    Invariants an implementation must enforce itself:

    * schema creation/migration and the reported ``expected_schema_version``;
    * identifier validation of every scope/quantity argument;
    * append-only immutable fill/grid/issue evidence, rejecting identity or
      ordinal collisions instead of silently overwriting;
    * an exact idempotent replay returning ``False`` from ``record_grid``,
      while any conflicting replay raises;
    * ``record_flat_checkpoint`` consuming only evidence the journal already
      persisted (never a caller's claim that a position is flat), preserving
      callbacks received after an older checkpoint and never clearing
      missing-exchange issues with an exchange observation;
    * ``validate_close_owner`` requiring a reconciled, positively owned
      residual from journaled fills plus the prior order's terminal position
      reconciliation, and never trusting a Strategy-supplied quantity.
    """

    port_version: ClassVar[int]

    @property
    @abc.abstractmethod
    def expected_schema_version(self) -> int:
        """Current schema version an open journal must observe.

        The journal compares its durable ``position_book_meta`` row to this
        value during its capacity migration and rejects a mismatch with
        ``POSITION_BOOK_SCHEMA_UNAVAILABLE``.  Implementations must return a
        positive ``int``; the journal no longer hard-codes the number.
        """

    @abc.abstractmethod
    def ensure_schema(self, cursor: Any) -> None:
        """Create or migrate the attribution tables on the journal cursor."""

    @abc.abstractmethod
    def record_grid(
        self,
        cursor: Any,
        *,
        account_scope: str,
        strategy_scope: str,
        cohort_id: str,
        batch_id: str,
        close_time: str,
        ordinal: int,
        capacity_class: str = "BASE",
    ) -> bool:
        """Append one newly committed eligible grid in the journal transaction.

        ``True`` means a new attribution row was appended; ``False`` means an
        exact idempotent replay.  The journal calls this only for complete,
        LIVE_READY grids whose decision cursor advanced in the same
        transaction.
        """

    @abc.abstractmethod
    def record_flat_checkpoint(
        self, cursor: Any, *, intent_id: str, reconciliation_id: str
    ) -> None:
        """Apply one stored, terminal, authoritative-flat resolution atomically."""

    @abc.abstractmethod
    def validate_close_owner(
        self,
        cursor: Any,
        *,
        account_scope: str,
        strategy_scope: str,
        instrument: str,
        exchange: str,
        owner_intent_id: str,
    ) -> None:
        """Require a reconciled, positively owned residual before CLOSE prepares."""


class JournalFeePairFactsPort(abc.ABC):
    """Application-owned fee-pair fact projection checks.

    The facts object is an application projection, not an authenticity token.
    The journal uses this port for three narrow questions and never inspects a
    private symbol of the implementing package to answer them.

    Invariants an implementation must enforce itself:

    * ``is_factor_execution_facts`` must accept exactly the concrete projection
      type the implementation mints, rejecting subclasses it does not own and
      any look-alike object, so no cross-repo class identity is needed here;
    * ``validate_source_lease`` must bind the lease to that exact facts
      instance, require the lease to be currently active and un-exhausted, and
      check the operation-specific source dependencies, returning ``False``
      rather than raising for any mismatch;
    * ``query_set_identifier`` must be a pure deterministic digest of exactly
      the six arguments below, so a journal re-derivation of a stored
      identifier matches bit for bit.
    """

    port_version: ClassVar[int]

    @abc.abstractmethod
    def is_factor_execution_facts(self, facts: Any) -> bool:
        """Return ``True`` only for this implementation's own facts projection."""

    @abc.abstractmethod
    def query_set_identifier(
        self,
        account_fingerprint: str,
        generation: int,
        trading_day: str,
        started_at: datetime,
        completed_at: datetime,
        query_ids: Tuple[Tuple[str, int], ...],
    ) -> str:
        """Derive the public query-set identifier for one complete query group."""

    @abc.abstractmethod
    def validate_source_lease(self, lease: Any, facts: Any, *, operation: str) -> bool:
        """Validate one exact source lease against these facts and operation."""


class JournalRegisteredSimAttestationPort(abc.ABC):
    """Attestation of one registered-sim order-identity query receipt.

    The journal needs a Store-minted order-identity query receipt revalidated
    before it durably binds a CTP ``OrderSysID``.  The receipt type, its private
    issuer/store binding and its Store validation routine all belong to the
    injecting application, so the journal delegates instead of poking at them.

    Invariants an implementation must enforce, and must reject by raising
    ``ExecutionDecisionJournalError`` with the exact code shown:

    * ``KNOWN_CTP_ORDER_SYSID_QUERY_UNTRUSTED`` when ``receipt`` is not a
      receipt this implementation minted, when ``source_store`` is not its own
      concrete store type, when the receipt is not bound to that very store,
      or when the store's own receipt mint/validate pair is not the
      implementation's authentic methods (no substituted or rebound object);
    * ``KNOWN_CTP_ORDER_SYSID_QUERY_STALE`` for any other rejection: a missing
      issuer binding, a non-positive ``request_id`` or
      ``connection_generation``, a blank/untrimmed account fingerprint, a
      non-8-digit ``trading_day``, a naive or reversed
      ``started_at_utc``/``completed_at_utc``, a non-positive ``front_id`` or
      ``session_id``, a blank/untrimmed ``broker_id``/``investor_id``, rows
      that are not a tuple, a non-hex ``rows_sha256``, a non-finite or already
      expired ``source_expires_monotonic``, or a store-side validation that
      does not return ``True`` against the still-current query;
    * the projection must carry exactly the fields the journal persists, with
      ``rows`` a tuple of plain key-addressable mappings so the journal can
      match its own durable ``OrderRef`` against a unique source row, and
      ``source_lock`` the store's query lock the journal holds across its
      COMMIT so the validated query cannot be replaced mid-transaction.

    The journal keeps its own database work: parent-intent identity, the unique
    source-row match against the journal ``order_ref``, the row/digest
    comparison and the durable binding insert.  This method performs no
    database work and must not mutate anything.
    """

    port_version: ClassVar[int]

    @abc.abstractmethod
    def attest_order_identity_query(
        self, receipt: Any, *, source_store: Any
    ) -> AttestedCtpOrderIdentityQuery:
        """Return the validated order-identity projection for one receipt."""


#: Abstract type accepted in each slot, keyed by :data:`JOURNAL_PORT_NAMES`.
JOURNAL_PORT_TYPES: Mapping[str, type] = MappingProxyType(
    {
        "position_attribution": JournalPositionAttributionPort,
        "fee_pair_facts": JournalFeePairFactsPort,
        "registered_sim_attestation": JournalRegisteredSimAttestationPort,
    }
)


def validate_journal_port(name: str, port: Any) -> Any:
    """Return *port* when it is a current-version implementation of slot *name*.

    ``isinstance`` here is a genuine subclass test, not duck typing: an
    application module, a look-alike object, or the abstract class itself is
    refused with ``JOURNAL_PORT_INVALID:<name>``.  ``port_version`` must be an
    ``int`` equal to :data:`JOURNAL_PORT_SCHEMA_VERSION`; the ABCs carry only an
    annotation, so an implementation that forgets to declare it is refused with
    ``JOURNAL_PORT_VERSION_UNSUPPORTED`` instead of inheriting a permissive
    default.
    """

    from .journal import ExecutionDecisionJournalError

    if name not in JOURNAL_PORT_TYPES:
        raise ExecutionDecisionJournalError("JOURNAL_PORT_INVALID:" + str(name))
    if not isinstance(port, JOURNAL_PORT_TYPES[name]):
        raise ExecutionDecisionJournalError("JOURNAL_PORT_INVALID:" + name)
    declared_version = getattr(type(port), "port_version", None)
    if type(declared_version) is not int or declared_version != JOURNAL_PORT_SCHEMA_VERSION:
        raise ExecutionDecisionJournalError("JOURNAL_PORT_VERSION_UNSUPPORTED")
    return port


@dataclass(frozen=True)
class AttestedCtpOrderIdentityQuery:
    """Neutral projection of one attested registered-sim order-identity query.

    Produced only by :meth:`JournalRegisteredSimAttestationPort.attest_order_identity_query`.
    The journal reads these public fields and never touches the source receipt
    or store afterwards.  ``source_expires_monotonic`` is rechecked by the
    journal inside its own transaction, so a value that was current during
    attestation but expires before COMMIT still fails closed with
    ``KNOWN_CTP_ORDER_SYSID_QUERY_STALE``.
    """

    request_id: int
    connection_generation: int
    account_fingerprint: str
    trading_day: str
    started_at_utc: datetime
    completed_at_utc: datetime
    front_id: int
    session_id: int
    broker_id: str
    investor_id: str
    rows: Tuple[Mapping[str, Any], ...]
    rows_sha256: str
    source_expires_monotonic: float
    source_lock: Any


@dataclass(frozen=True)
class JournalPorts:
    """Immutable per-journal port set; ``None`` means "not supplied"."""

    position_attribution: Optional[JournalPositionAttributionPort] = None
    fee_pair_facts: Optional[JournalFeePairFactsPort] = None
    registered_sim_attestation: Optional[JournalRegisteredSimAttestationPort] = None

    def __post_init__(self) -> None:
        # Validate on construction so a hand-built container cannot smuggle a
        # module, a look-alike object or a stale-version implementation past
        # ``configure_journal_ports`` and reach a call site.
        for name in JOURNAL_PORT_NAMES:
            port = getattr(self, name)
            if port is not None:
                validate_journal_port(name, port)

    def require(self, name: str) -> Any:
        """Return one port or fail closed with ``JOURNAL_PORT_UNAVAILABLE:<name>``.

        Imported lazily so this module stays importable without the journal;
        the error class lives in this package and carries the journal's codes.
        """

        if name not in JOURNAL_PORT_NAMES:
            raise ValueError("unknown journal port slot: " + str(name))
        port = getattr(self, name)
        if port is None:
            from .journal import ExecutionDecisionJournalError

            raise ExecutionDecisionJournalError("JOURNAL_PORT_UNAVAILABLE:" + name)
        return port


__all__ = [
    "JOURNAL_PORT_NAMES",
    "JOURNAL_PORT_SCHEMA_VERSION",
    "JOURNAL_PORT_TYPES",
    "AttestedCtpOrderIdentityQuery",
    "JournalFeePairFactsPort",
    "JournalPorts",
    "JournalPositionAttributionPort",
    "JournalRegisteredSimAttestationPort",
    "validate_journal_port",
]
