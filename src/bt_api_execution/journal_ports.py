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
* every port carries a ``port_version`` class attribute checked against its
  slot's schema version at injection time, so an old or newer implementation
  fails closed instead of half-working;
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
from typing import Any, ClassVar, Iterable, Mapping, Optional, Tuple

#: Version retained by the fee-pair and registered-sim attestation port slots.
JOURNAL_PORT_SCHEMA_VERSION = 1

#: Position attribution v3 delegates reads and durable-capacity rows to the
#: application-owned schema implementation.
JOURNAL_POSITION_ATTRIBUTION_PORT_SCHEMA_VERSION = 3

#: Port slot names accepted by ``configure_journal_ports`` and reported by the
#: ``JOURNAL_PORT_INVALID:<name>`` / ``JOURNAL_PORT_UNAVAILABLE:<name>`` codes.
JOURNAL_PORT_NAMES = ("position_attribution", "fee_pair_facts", "registered_sim_attestation")

_POSITION_CAPABILITY_NAMES = frozenset(
    {
        "current_position",
        "fill",
        "evidence_issue",
        "capacity_rows",
        "capacity_row_size",
    }
)
_POSITION_CAPACITY_ROLES = frozenset({"GRID_HISTORY", "SETTLEMENT"})
_MAX_POSITION_ROW_SIZE = 9_223_372_036_854_775_807


class JournalPositionCapabilityUnavailable(RuntimeError):
    """One exact position-port capability is not implemented by its owner."""

    def __init__(self, capability: str) -> None:
        if type(capability) is not str or capability not in _POSITION_CAPABILITY_NAMES:
            raise ValueError("invalid position capability")
        self.capability = capability
        self.code = "JOURNAL_POSITION_CAPABILITY_UNAVAILABLE:" + capability
        super().__init__(self.code)


@dataclass(frozen=True)
class JournalPositionGridIdentity:
    """Neutral join key for classifying an owner's eligible grid history."""

    account_scope: str
    strategy_scope: str
    cohort_id: str
    close_time: str

    def __post_init__(self) -> None:
        if any(
            type(value) is not str or not value
            for value in (
                self.account_scope,
                self.strategy_scope,
                self.cohort_id,
                self.close_time,
            )
        ):
            raise ValueError("invalid position grid identity")


@dataclass(frozen=True)
class JournalPositionCapacityRow:
    """One owner row's canonical byte size and SDK-neutral accounting role."""

    capacity_role: str
    row_size_bytes: int
    grid_identity: Optional[JournalPositionGridIdentity] = None

    def __post_init__(self) -> None:
        if type(self.capacity_role) is not str or self.capacity_role not in _POSITION_CAPACITY_ROLES:
            raise ValueError("invalid position capacity role")
        if type(self.row_size_bytes) is not int or not 1 <= self.row_size_bytes <= _MAX_POSITION_ROW_SIZE:
            raise ValueError("invalid position capacity row size")
        if self.capacity_role == "GRID_HISTORY":
            if type(self.grid_identity) is not JournalPositionGridIdentity:
                raise ValueError("grid history row requires a grid identity")
        elif self.grid_identity is not None:
            raise ValueError("settlement row cannot carry a grid identity")


@dataclass(frozen=True)
class JournalPositionCurrent:
    """Typed application-owned current-position projection used by the journal."""

    state: str
    blocker_code: Optional[str]
    owner_intent_id: Optional[str]
    owner_strategy_scope: Optional[str]
    flat_reconciliation_id: Optional[str]
    flat_checkpoint_ns: Optional[int]
    last_callback_at_ns: Optional[int]
    open_quantity: str
    close_quantity: str
    open_notional: str
    opening_fee_unknown_count: int
    fill_time_unknown_count: int

    def __post_init__(self) -> None:
        if type(self.state) is not str or not self.state:
            raise ValueError("invalid current-position state")
        for value in (
            self.blocker_code,
            self.owner_intent_id,
            self.owner_strategy_scope,
            self.flat_reconciliation_id,
        ):
            if value is not None and (type(value) is not str or not value):
                raise ValueError("invalid current-position identifier")
        for value in (self.flat_checkpoint_ns, self.last_callback_at_ns):
            if value is not None and type(value) is not int:
                raise ValueError("invalid current-position timestamp")
        if any(
            type(value) is not str
            for value in (self.open_quantity, self.close_quantity, self.open_notional)
        ):
            raise ValueError("invalid current-position amount")
        if any(
            type(value) is not int or value < 0
            for value in (self.opening_fee_unknown_count, self.fill_time_unknown_count)
        ):
            raise ValueError("invalid current-position count")


@dataclass(frozen=True)
class JournalPositionFill:
    """Typed application-owned fill projection matched by a journal receipt."""

    intent_id: str
    strategy_scope: str
    position_effect: str
    order_side: str
    quantity: str
    price: str
    fee: Optional[str]
    filled_at_ns: int

    def __post_init__(self) -> None:
        for value in (
            self.intent_id,
            self.strategy_scope,
            self.position_effect,
            self.order_side,
            self.quantity,
            self.price,
        ):
            if type(value) is not str or not value:
                raise ValueError("invalid position fill field")
        if self.fee is not None and type(self.fee) is not str:
            raise ValueError("invalid position fill fee")
        if type(self.filled_at_ns) is not int:
            raise ValueError("invalid position fill timestamp")


@dataclass(frozen=True)
class JournalPositionIssue:
    """Typed issue blocker associated with an application-owned receipt."""

    blocker_code: str

    def __post_init__(self) -> None:
        if type(self.blocker_code) is not str or not self.blocker_code:
            raise ValueError("invalid position issue blocker")


class JournalPositionAttributionPort(abc.ABC):
    """Strategy position attribution exposed to the journal in-transaction.

    Implementations may use SDK-owned local attribution storage inside the
    execution journal's own SQLite database. Every method receives the
    *journal's* live cursor and must write on it inside the caller's
    transaction: the journal neither commits nor rolls back on the
    implementation's behalf, and an implementation failure must abort the whole
    decision transaction. When invoked by the journal, the transaction is
    already active; implementations must not begin, commit, or roll back it, or
    call ``executescript`` (which can implicitly commit). Execute schema
    statements individually through the supplied cursor.

    Invariants an implementation must enforce itself:

    * a write-free schema preflight before creation, creation only for an empty
      owner schema, exact validation of its metadata row, and the reported
      ``expected_schema_version``;
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
        """Current schema version the implementation validates on its tables.

        Implementations must return a positive ``int`` and use this value in
        ``validate_schema``.  The SDK journal does not interpret the port's
        metadata table.
        """

    @abc.abstractmethod
    def preflight_schema(self, cursor: Any) -> bool:
        """Return exact ``True`` before writes only for empty/current owned schema.

        ``False`` means existing owner data or schema is not proven compatible
        with this port version. The journal calls this before ``ensure_schema``
        so an unsupported legacy schema cannot be silently upgraded or altered.
        """

    @abc.abstractmethod
    def ensure_schema(self, cursor: Any) -> None:
        """Create the owned attribution schema only after a successful preflight."""

    @abc.abstractmethod
    def validate_schema(self, cursor: Any) -> bool:
        """Return exact ``True`` only when owned schema metadata is current.

        The journal passes its active cursor.  Implementations must validate
        their own metadata table against ``expected_schema_version`` and must
        not start, commit, or roll back a transaction.  A missing, malformed,
        or mismatched schema returns ``False``; the journal maps that result to
        ``POSITION_BOOK_SCHEMA_UNAVAILABLE``.
        """

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

    @abc.abstractmethod
    def read_current_position(
        self, cursor: Any, *, account_scope: str, instrument: str, exchange: str
    ) -> Optional[JournalPositionCurrent]:
        """Read one exact current projection; ``None`` means no matching row.

        Raise :class:`JournalPositionCapabilityUnavailable` when this owner
        does not store the requested fact family. Do not substitute a different
        local or provider projection.
        """

    @abc.abstractmethod
    def read_fill(
        self,
        cursor: Any,
        *,
        account_scope: str,
        instrument: str,
        exchange: str,
        execution_id: str,
    ) -> Optional[JournalPositionFill]:
        """Read one exact stored fill; ``None`` means no matching row."""

    @abc.abstractmethod
    def read_evidence_issue(
        self, cursor: Any, *, receipt_id: str
    ) -> Optional[JournalPositionIssue]:
        """Read one receipt issue; ``None`` means no matching issue row."""

    @abc.abstractmethod
    def iter_capacity_rows(
        self, cursor: Any
    ) -> Iterable[JournalPositionCapacityRow]:
        """Yield every owner row charged during capacity backfill.

        A complete empty iterator is valid only after this port has validated
        the complete, current owner schema. Raw row/column names never cross the
        port boundary.
        """

    @abc.abstractmethod
    def snapshot_capacity_row_size(
        self, cursor: Any, *, owner_table_ref: str, key_values: Tuple[Any, ...]
    ) -> Optional[int]:
        """Return a keyed owner row's canonical byte size, or ``None`` if absent.

        ``owner_table_ref`` and ``key_values`` are opaque to the SDK. The port
        validates and interprets them against its own schema and uses the SDK's
        canonical row-size helper; the SDK never constructs owner SQL.
        """


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

_JOURNAL_PORT_SCHEMA_VERSIONS: Mapping[str, int] = MappingProxyType(
    {
        "position_attribution": JOURNAL_POSITION_ATTRIBUTION_PORT_SCHEMA_VERSION,
        "fee_pair_facts": JOURNAL_PORT_SCHEMA_VERSION,
        "registered_sim_attestation": JOURNAL_PORT_SCHEMA_VERSION,
    }
)


def validate_journal_port(name: str, port: Any) -> Any:
    """Return *port* when it is a current-version implementation of slot *name*.

    ``isinstance`` here is a genuine subclass test, not duck typing: an
    application module, a look-alike object, or the abstract class itself is
    refused with ``JOURNAL_PORT_INVALID:<name>``.  ``port_version`` must be an
    exact ``int`` equal to the version for this slot; the ABCs carry only an
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
    expected_version = _JOURNAL_PORT_SCHEMA_VERSIONS[name]
    if type(declared_version) is not int or declared_version != expected_version:
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
    "JOURNAL_POSITION_ATTRIBUTION_PORT_SCHEMA_VERSION",
    "JOURNAL_PORT_SCHEMA_VERSION",
    "JOURNAL_PORT_TYPES",
    "AttestedCtpOrderIdentityQuery",
    "JournalFeePairFactsPort",
    "JournalPorts",
    "JournalPositionAttributionPort",
    "JournalPositionCapabilityUnavailable",
    "JournalPositionCapacityRow",
    "JournalPositionCurrent",
    "JournalPositionFill",
    "JournalPositionGridIdentity",
    "JournalPositionIssue",
    "JournalRegisteredSimAttestationPort",
    "validate_journal_port",
]
