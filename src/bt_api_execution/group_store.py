"""Durable local journal for ordered multi-leg execution coordination."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Mapping
from dataclasses import replace
from decimal import Decimal
from fractions import Fraction
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

from .contracts import canonical_json, payload_sha256
from .errors import ContractValidationError, DurableStoreError, IntentConflictError
from .group_order_contracts import (
    GroupAccountReservationSnapshot,
    GroupBudgetReservation,
    GroupBudgetReservationState,
    GroupLegProgress,
    GroupOrderEvent,
    GroupOrderIntent,
    GroupOrderSnapshot,
    GroupOrderState,
    PortfolioGroupBudgetPolicy,
)

_TERMINAL_STATES = frozenset(
    {
        GroupOrderState.PAIRED,
        GroupOrderState.CLOSED,
        GroupOrderState.REJECTED_BEFORE_DISPATCH,
    }
)
_ALLOWED_STATE_TRANSITIONS = {
    GroupOrderState.PREPARING: frozenset(
        {
            GroupOrderState.FIRST_PENDING,
            GroupOrderState.REJECTED_BEFORE_DISPATCH,
            GroupOrderState.RECOVERY_REQUIRED,
        }
    ),
    GroupOrderState.FIRST_PENDING: frozenset(
        {
            GroupOrderState.HEDGE_PENDING,
            GroupOrderState.CLOSED,
            GroupOrderState.RECOVERY_REQUIRED,
        }
    ),
    GroupOrderState.HEDGE_PENDING: frozenset(
        {GroupOrderState.PAIRED, GroupOrderState.RECOVERY_REQUIRED}
    ),
    GroupOrderState.RECOVERY_REQUIRED: frozenset(
        {
            GroupOrderState.FIRST_PENDING,
            GroupOrderState.HEDGE_PENDING,
            GroupOrderState.PAIRED,
            GroupOrderState.CLOSED,
            GroupOrderState.REJECTED_BEFORE_DISPATCH,
        }
    ),
    GroupOrderState.PAIRED: frozenset({GroupOrderState.UNWINDING, GroupOrderState.CLOSED}),
    GroupOrderState.UNWINDING: frozenset(
        {GroupOrderState.CLOSED, GroupOrderState.RECOVERY_REQUIRED}
    ),
    GroupOrderState.CLOSED: frozenset(),
    GroupOrderState.REJECTED_BEFORE_DISPATCH: frozenset(),
}
_EVENT_TYPE = frozenset(
    {
        "GROUP_ACCEPTED",
        "GROUP_BUDGET_DENIED",
        "ACCOUNT_TARGETS_BOUND",
        "RESERVATION_QUERY_STARTED",
        "RESERVATION_CONFIRMED",
        "RESERVATION_DENIED",
        "RESERVATION_UNKNOWN",
        "RESERVATION_RELEASE_STARTED",
        "RESERVATION_RELEASED",
        "FIRST_LEG_QUERY_STARTED",
        "FIRST_LEG_SUBMIT_STARTED",
        "FIRST_LEG_STATUS_OBSERVED",
        "GROUP_DISPATCH_RESOLUTION_RECORDED",
        "HEDGE_LEG_SIZED",
        "HEDGE_LEG_QUERY_STARTED",
        "HEDGE_LEG_SUBMIT_STARTED",
        "HEDGE_LEG_STATUS_OBSERVED",
        "GROUP_PAIRED",
        "GROUP_REJECTED_BEFORE_DISPATCH",
        "GROUP_CLOSED_NO_EXPOSURE",
        "GROUP_RECOVERY_REQUIRED",
    }
)


class SqliteOrderGroupStore:
    """Persist immutable group requests and revisioned coordinator snapshots.

    Remote calls never occur inside a SQLite transaction. The caller writes a
    progress event before a side effect and recovers by querying the original
    group, reservation, and child-order identities.
    """

    _SCHEMA_VERSION = 2

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 5_000) -> None:
        if type(busy_timeout_ms) is not int or busy_timeout_ms <= 0:
            raise ContractValidationError("invalid busy_timeout_ms")
        self._path = str(path)
        self._lock = threading.RLock()
        try:
            self._connection = sqlite3.connect(
                self._path,
                timeout=busy_timeout_ms / 1000,
                isolation_level=None,
                check_same_thread=False,
            )
            self._connection.row_factory = sqlite3.Row
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = FULL")
            self._initialize_schema()
        except sqlite3.Error as error:
            raise DurableStoreError("unable to open group journal") from error

    def _initialize_schema(self) -> None:
        with self._lock:
            try:
                self._connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS execution_group_meta (
                        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                        schema_version INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS order_group_snapshots (
                        portfolio_ref TEXT NOT NULL,
                        group_id TEXT NOT NULL,
                        request_fingerprint TEXT NOT NULL CHECK(length(request_fingerprint) = 64),
                        request_json TEXT NOT NULL,
                        snapshot_json TEXT NOT NULL,
                        snapshot_sha256 TEXT NOT NULL CHECK(length(snapshot_sha256) = 64),
                        state TEXT NOT NULL,
                        revision INTEGER NOT NULL CHECK(revision >= 0),
                        deadline_ns INTEGER NOT NULL CHECK(deadline_ns > 0),
                        budget_policy_fingerprint TEXT,
                        budget_quote_fingerprint TEXT,
                        budget_valuation_unit TEXT,
                        budget_gross_notional TEXT,
                        budget_deadline_ns INTEGER,
                        budget_state TEXT,
                        created_at_ns INTEGER NOT NULL CHECK(created_at_ns > 0),
                        updated_at_ns INTEGER NOT NULL CHECK(updated_at_ns > 0),
                        PRIMARY KEY(portfolio_ref, group_id)
                    );
                    CREATE TABLE IF NOT EXISTS order_group_events (
                        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                        portfolio_ref TEXT NOT NULL,
                        group_id TEXT NOT NULL,
                        revision INTEGER NOT NULL CHECK(revision >= 0),
                        event_type TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        event_sha256 TEXT NOT NULL CHECK(length(event_sha256) = 64),
                        created_at_ns INTEGER NOT NULL CHECK(created_at_ns > 0),
                        UNIQUE(portfolio_ref, group_id, revision),
                        FOREIGN KEY(portfolio_ref, group_id)
                            REFERENCES order_group_snapshots(portfolio_ref, group_id)
                    );
                    CREATE INDEX IF NOT EXISTS order_group_events_group_sequence
                        ON order_group_events(portfolio_ref, group_id, sequence);
                    CREATE INDEX IF NOT EXISTS order_group_snapshots_state
                        ON order_group_snapshots(state, updated_at_ns);
                    CREATE TRIGGER IF NOT EXISTS order_group_events_immutable_update
                    BEFORE UPDATE ON order_group_events
                    BEGIN
                        SELECT RAISE(ABORT, 'order group events are immutable');
                    END;
                    CREATE TRIGGER IF NOT EXISTS order_group_events_immutable_delete
                    BEFORE DELETE ON order_group_events
                    BEGIN
                        SELECT RAISE(ABORT, 'order group events are immutable');
                    END;
                    """
                )
                with self._transaction() as cursor:
                    columns = {
                        str(item["name"])
                        for item in cursor.execute("PRAGMA table_info(order_group_snapshots)")
                    }
                    additions = {
                        "budget_policy_fingerprint": "TEXT",
                        "budget_quote_fingerprint": "TEXT",
                        "budget_valuation_unit": "TEXT",
                        "budget_gross_notional": "TEXT",
                        "budget_deadline_ns": "INTEGER",
                        "budget_state": "TEXT",
                    }
                    for name, column_type in additions.items():
                        if name not in columns:
                            cursor.execute(
                                f"ALTER TABLE order_group_snapshots ADD COLUMN {name} {column_type}"
                            )
                    row = cursor.execute(
                        "SELECT schema_version FROM execution_group_meta WHERE singleton = 1"
                    ).fetchone()
                    if row is None:
                        cursor.execute(
                            "INSERT INTO execution_group_meta(singleton, schema_version) VALUES(1, ?)",
                            (self._SCHEMA_VERSION,),
                        )
                    elif int(row["schema_version"]) == 1:
                        cursor.execute(
                            "UPDATE execution_group_meta SET schema_version = ? WHERE singleton = 1",
                            (self._SCHEMA_VERSION,),
                        )
                    elif int(row["schema_version"]) != self._SCHEMA_VERSION:
                        raise DurableStoreError("unsupported group journal schema")
            except sqlite3.Error as error:
                raise DurableStoreError("unable to initialize group journal") from error

    def create_or_get(
        self,
        request: GroupOrderIntent,
        *,
        budget_policy: PortfolioGroupBudgetPolicy | None = None,
        now_ns: int | None = None,
        clock_ns: Any = None,
    ) -> GroupOrderSnapshot:
        """Atomically admit the first group ID and reserve its portfolio budget.

        An existing ID is resolved before today's policy or quote is examined.
        The immutable normalized request must match, after which the original
        stored assessment/policy receipt is returned unchanged.
        """

        if type(request) is not GroupOrderIntent:
            raise ContractValidationError("normalized group order intent is required")
        created_at_ns = time.time_ns() if now_ns is None else now_ns
        if type(created_at_ns) is not int or created_at_ns <= 0:
            raise ContractValidationError("invalid group creation time")
        if clock_ns is not None and not callable(clock_ns):
            raise ContractValidationError("group admission clock must be callable")
        request_json = canonical_json(request.to_payload())
        with self._transaction() as cursor:
            row = cursor.execute(
                """SELECT * FROM order_group_snapshots
                   WHERE portfolio_ref = ? AND group_id = ?""",
                (request.portfolio_ref, request.group_id),
            ).fetchone()
            if row is not None:
                existing = self._snapshot_from_row(row)
                if existing.request.unquoted_fingerprint != request.unquoted_fingerprint:
                    raise IntentConflictError()
                return existing

            if request.version in {2, 3}:
                if type(budget_policy) is not PortfolioGroupBudgetPolicy:
                    raise ContractValidationError(
                        "trusted portfolio group budget policy is required"
                    )
                quote = request.budget_quote
                if (
                    budget_policy.portfolio_ref != request.portfolio_ref
                    or quote is None
                    or quote.policy_fingerprint != budget_policy.fingerprint
                    or quote.valuation_unit != budget_policy.valuation_unit
                ):
                    raise ContractValidationError(
                        "group quote does not match trusted budget policy"
                    )
            elif budget_policy is not None:
                raise ContractValidationError("legacy group request cannot receive a budget policy")
            else:
                quote = None

            admission_now_ns = created_at_ns if clock_ns is None else clock_ns()
            if type(admission_now_ns) is not int or admission_now_ns <= 0:
                raise ContractValidationError("invalid group admission clock")
            if admission_now_ns >= request.deadline_ns:
                raise ContractValidationError("group deadline elapsed during admission")

            reservations = tuple(
                GroupAccountReservationSnapshot(
                    group_id=request.group_id,
                    account_ref=account_ref,
                    scope_fingerprint="0" * 64,
                    phase="NOT_STARTED",
                )
                for account_ref in request.account_refs
            )
            # The zero fingerprint is replaced before any account-owner RPC.
            legs = tuple(GroupLegProgress(leg_id=plan.leg_id) for plan in request.legs)
            budget_reservation = None
            initial_state = GroupOrderState.PREPARING
            error_code = None
            event_type = "GROUP_ACCEPTED"
            event_payload: dict[str, Any] = {
                "request_fingerprint": request.fingerprint,
                "account_refs": list(request.account_refs),
                "leg_ids": [leg.leg_id for leg in request.legs],
                "deadline_ns": request.deadline_ns,
            }
            if quote is not None:
                assert budget_policy is not None
                if any(
                    quote_leg.assessed_at_ns > admission_now_ns
                    or admission_now_ns >= quote_leg.valid_until_ns
                    for quote_leg in quote.legs
                ):
                    raise ContractValidationError("group quote facts expired before admission")
                portfolio_rows = cursor.execute(
                    """SELECT * FROM order_group_snapshots
                       WHERE portfolio_ref = ?""",
                    (request.portfolio_ref,),
                ).fetchall()
                active_notional = Fraction(0)
                active_groups = 0
                unpriced_legacy_groups = 0
                unit_mismatch = False
                policy_mismatch = False
                for portfolio_row in portfolio_rows:
                    active_snapshot = self._snapshot_from_row(portfolio_row)
                    if active_snapshot.request.version == 1:
                        if active_snapshot.state not in {
                            GroupOrderState.CLOSED,
                            GroupOrderState.REJECTED_BEFORE_DISPATCH,
                        }:
                            # A v1 row has no defensible gross amount. Keep
                            # unknown and PAIRED exposure as a portfolio-wide
                            # admission hold instead of assuming it is zero.
                            unpriced_legacy_groups += 1
                            active_groups += 1
                        continue
                    active_budget = active_snapshot.budget_reservation
                    if active_budget is None and active_snapshot.state not in {
                        GroupOrderState.CLOSED,
                        GroupOrderState.REJECTED_BEFORE_DISPATCH,
                    }:
                        raise DurableStoreError("active group budget row has no typed receipt")
                    if active_budget is None or active_budget.state not in {
                        GroupBudgetReservationState.RESERVED,
                        GroupBudgetReservationState.HELD,
                    }:
                        continue
                    if active_budget.valuation_unit != budget_policy.valuation_unit:
                        unit_mismatch = True
                    if active_budget.policy_fingerprint != budget_policy.fingerprint:
                        policy_mismatch = True
                    active_notional += Fraction(active_budget.gross_notional)
                    active_groups += 1
                denial = None
                if unpriced_legacy_groups:
                    denial = "GROUP_BUDGET_LEGACY_HOLD_UNPROVEN"
                elif unit_mismatch:
                    denial = "GROUP_BUDGET_UNIT_MISMATCH"
                elif policy_mismatch:
                    # Multiple owner processes must not apply different cap
                    # generations to the same active portfolio budget rows.
                    # The exact first policy remains effective until every
                    # RESERVED/HELD group has a trusted terminal release.
                    denial = "GROUP_BUDGET_POLICY_MISMATCH"
                elif quote.gross_notional > budget_policy.max_group_gross_notional:
                    denial = "GROUP_BUDGET_LIMIT_EXCEEDED"
                elif active_groups >= budget_policy.max_active_groups:
                    denial = "GROUP_BUDGET_ACTIVE_GROUP_LIMIT"
                elif active_notional + Fraction(quote.gross_notional) > Fraction(
                    budget_policy.max_active_gross_notional
                ):
                    denial = "GROUP_BUDGET_PORTFOLIO_LIMIT_EXCEEDED"
                budget_state = (
                    GroupBudgetReservationState.DENIED
                    if denial is not None
                    else GroupBudgetReservationState.RESERVED
                )
                budget_reservation = GroupBudgetReservation(
                    policy_fingerprint=budget_policy.fingerprint,
                    policy=budget_policy,
                    quote_fingerprint=quote.fingerprint,
                    valuation_unit=quote.valuation_unit,
                    gross_notional=quote.gross_notional,
                    deadline_ns=request.deadline_ns,
                    state=budget_state,
                )
                event_payload["budget"] = {
                    "policy_fingerprint": budget_policy.fingerprint,
                    "quote_fingerprint": quote.fingerprint,
                    "valuation_unit": quote.valuation_unit,
                    "gross_notional": format(quote.gross_notional, "f"),
                    "active_gross_notional_before": _fraction_text(active_notional),
                    "active_groups_before": active_groups,
                    "unpriced_legacy_groups": unpriced_legacy_groups,
                    "decision": budget_state.value,
                    "denial_code": denial,
                }
                if denial is not None:
                    initial_state = GroupOrderState.REJECTED_BEFORE_DISPATCH
                    error_code = denial.lower()
                    event_type = "GROUP_BUDGET_DENIED"

            initial = GroupOrderSnapshot(
                request=request,
                state=initial_state,
                revision=0,
                reservations=reservations,
                legs=legs,
                residual_base_units=None,
                error_code=error_code,
                created_at_ns=created_at_ns,
                updated_at_ns=created_at_ns,
                budget_reservation=budget_reservation,
            )
            snapshot_payload = initial.to_payload()
            snapshot_json = canonical_json(snapshot_payload)
            self._validate_event(event_type, event_payload, created_at_ns)
            cursor.execute(
                """INSERT INTO order_group_snapshots(
                       portfolio_ref, group_id, request_fingerprint, request_json,
                       snapshot_json, snapshot_sha256, state, revision, deadline_ns,
                       budget_policy_fingerprint, budget_quote_fingerprint,
                       budget_valuation_unit, budget_gross_notional, budget_deadline_ns,
                       budget_state, created_at_ns, updated_at_ns
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    request.portfolio_ref,
                    request.group_id,
                    request.fingerprint,
                    request_json,
                    snapshot_json,
                    payload_sha256(snapshot_payload),
                    initial.state.value,
                    initial.revision,
                    request.deadline_ns,
                    None if budget_reservation is None else budget_reservation.policy_fingerprint,
                    None if budget_reservation is None else budget_reservation.quote_fingerprint,
                    None if budget_reservation is None else budget_reservation.valuation_unit,
                    None
                    if budget_reservation is None
                    else format(budget_reservation.gross_notional, "f"),
                    None if budget_reservation is None else budget_reservation.deadline_ns,
                    None if budget_reservation is None else budget_reservation.state.value,
                    created_at_ns,
                    created_at_ns,
                ),
            )
            self._insert_event(
                cursor,
                request.portfolio_ref,
                request.group_id,
                initial.revision,
                event_type,
                event_payload,
                created_at_ns,
            )
            if quote is not None and event_type == "GROUP_ACCEPTED" and clock_ns is not None:
                commit_check_ns = clock_ns()
                if type(commit_check_ns) is not int or commit_check_ns <= 0:
                    raise ContractValidationError("invalid group budget commit clock")
                if commit_check_ns >= request.deadline_ns or any(
                    commit_check_ns >= quote_leg.valid_until_ns for quote_leg in quote.legs
                ):
                    raise ContractValidationError("group budget facts expired before commit")
            return initial

    def bind_account_scopes(
        self,
        snapshot: GroupOrderSnapshot,
        scope_fingerprints: Mapping[str, str],
        *,
        now_ns: int | None = None,
    ) -> GroupOrderSnapshot:
        """Bind configured account-target fingerprints before first reserve."""

        if set(scope_fingerprints) != set(snapshot.request.account_refs):
            raise ContractValidationError("group account target set is incomplete")
        updated_reservations: list[GroupAccountReservationSnapshot] = []
        for reservation in snapshot.reservations:
            fingerprint = scope_fingerprints[reservation.account_ref]
            if (
                type(fingerprint) is not str
                or len(fingerprint) != 64
                or any(char not in "0123456789abcdef" for char in fingerprint)
            ):
                raise ContractValidationError("invalid account target fingerprint")
            if reservation.scope_fingerprint not in {"0" * 64, fingerprint}:
                raise IntentConflictError()
            updated_reservations.append(replace(reservation, scope_fingerprint=fingerprint))
        if tuple(updated_reservations) == snapshot.reservations:
            return snapshot
        return self.update(
            snapshot,
            event_type="ACCOUNT_TARGETS_BOUND",
            event_payload={"phase": "account_targets_bound"},
            reservations=tuple(updated_reservations),
            now_ns=now_ns,
        )

    def update(
        self,
        snapshot: GroupOrderSnapshot,
        *,
        event_type: str,
        event_payload: Mapping[str, Any],
        state: GroupOrderState | None = None,
        reservations: tuple[GroupAccountReservationSnapshot, ...] | None = None,
        legs: tuple[GroupLegProgress, ...] | None = None,
        residual_base_units: Any = ...,
        residual_vector: Any = ...,
        error_code: str | None | Any = ...,
        now_ns: int | None = None,
    ) -> GroupOrderSnapshot:
        if type(snapshot) is not GroupOrderSnapshot:
            raise ContractValidationError("group snapshot is required")
        if event_type not in _EVENT_TYPE:
            raise ContractValidationError("unsupported group event type")
        occurred_at_ns = time.time_ns() if now_ns is None else now_ns
        if type(occurred_at_ns) is not int or occurred_at_ns <= 0:
            raise ContractValidationError("invalid group event time")
        target_state = snapshot.state if state is None else GroupOrderState(state)
        if (
            target_state != snapshot.state
            and target_state not in _ALLOWED_STATE_TRANSITIONS[snapshot.state]
        ):
            raise ContractValidationError("invalid group state transition")
        candidate = replace(
            snapshot,
            revision=snapshot.revision + 1,
            reservations=snapshot.reservations if reservations is None else reservations,
            legs=snapshot.legs if legs is None else legs,
            residual_base_units=(
                snapshot.residual_base_units if residual_base_units is ... else residual_base_units
            ),
            residual_vector=(
                snapshot.residual_vector if residual_vector is ... else residual_vector
            ),
            error_code=snapshot.error_code if error_code is ... else error_code,
            updated_at_ns=occurred_at_ns,
        )
        persisted_event_payload = dict(event_payload)
        current_budget = snapshot.budget_reservation
        target_budget = current_budget
        if current_budget is not None:
            target_budget_state = current_budget.state
            if event_type in {"FIRST_LEG_SUBMIT_STARTED", "HEDGE_LEG_SUBMIT_STARTED"}:
                if current_budget.state not in {
                    GroupBudgetReservationState.RESERVED,
                    GroupBudgetReservationState.HELD,
                }:
                    raise ContractValidationError("group budget is not dispatchable")
                target_budget_state = GroupBudgetReservationState.HELD
            elif (
                target_state is GroupOrderState.REJECTED_BEFORE_DISPATCH
                and current_budget.state is GroupBudgetReservationState.DENIED
            ):
                # A local policy denial created no budget hold or remote Risk
                # reservation. Preserve its durable DENIED receipt while
                # binding the read-only account target projection.
                target_budget_state = GroupBudgetReservationState.DENIED
            elif target_state is GroupOrderState.REJECTED_BEFORE_DISPATCH:
                if event_type != "GROUP_REJECTED_BEFORE_DISPATCH" or not _safe_pre_dispatch_release(
                    candidate
                ):
                    raise ContractValidationError(
                        "group budget cannot release before all remote reservations are safe"
                    )
                target_budget_state = GroupBudgetReservationState.RELEASED
            elif target_state is GroupOrderState.CLOSED:
                if event_type != "GROUP_CLOSED_NO_EXPOSURE" or not _proven_no_exposure_close(
                    candidate, persisted_event_payload
                ):
                    raise ContractValidationError(
                        "group budget release requires exact terminal no-fill evidence"
                    )
                target_budget_state = GroupBudgetReservationState.RELEASED
            if target_budget_state is not current_budget.state:
                target_budget = replace(current_budget, state=target_budget_state)
                persisted_event_payload["budget_transition"] = {
                    "from": current_budget.state.value,
                    "to": target_budget_state.value,
                    "policy_fingerprint": current_budget.policy_fingerprint,
                    "quote_fingerprint": current_budget.quote_fingerprint,
                    "valuation_unit": current_budget.valuation_unit,
                    "gross_notional": format(current_budget.gross_notional, "f"),
                }
            else:
                persisted_event_payload["budget_state"] = current_budget.state.value
        new_snapshot = replace(candidate, state=target_state, budget_reservation=target_budget)
        self._validate_event(event_type, persisted_event_payload, occurred_at_ns)
        request_json = canonical_json(new_snapshot.request.to_payload())
        snapshot_payload = new_snapshot.to_payload()
        snapshot_json = canonical_json(snapshot_payload)
        with self._transaction() as cursor:
            row = cursor.execute(
                """SELECT * FROM order_group_snapshots
                   WHERE portfolio_ref = ? AND group_id = ?""",
                (snapshot.request.portfolio_ref, snapshot.request.group_id),
            ).fetchone()
            if row is None:
                raise DurableStoreError("group record disappeared")
            persisted = self._snapshot_from_row(row)
            if (
                int(row["revision"]) != snapshot.revision
                or persisted.request.fingerprint != snapshot.request.fingerprint
            ):
                raise IntentConflictError()
            cursor.execute(
                """UPDATE order_group_snapshots
                   SET request_json = ?, snapshot_json = ?, snapshot_sha256 = ?, state = ?,
                       revision = ?, budget_policy_fingerprint = ?,
                       budget_quote_fingerprint = ?, budget_valuation_unit = ?,
                       budget_gross_notional = ?, budget_deadline_ns = ?, budget_state = ?,
                       updated_at_ns = ?
                   WHERE portfolio_ref = ? AND group_id = ? AND revision = ?""",
                (
                    request_json,
                    snapshot_json,
                    payload_sha256(snapshot_payload),
                    target_state.value,
                    new_snapshot.revision,
                    None
                    if new_snapshot.budget_reservation is None
                    else new_snapshot.budget_reservation.policy_fingerprint,
                    None
                    if new_snapshot.budget_reservation is None
                    else new_snapshot.budget_reservation.quote_fingerprint,
                    None
                    if new_snapshot.budget_reservation is None
                    else new_snapshot.budget_reservation.valuation_unit,
                    None
                    if new_snapshot.budget_reservation is None
                    else format(new_snapshot.budget_reservation.gross_notional, "f"),
                    None
                    if new_snapshot.budget_reservation is None
                    else new_snapshot.budget_reservation.deadline_ns,
                    None
                    if new_snapshot.budget_reservation is None
                    else new_snapshot.budget_reservation.state.value,
                    occurred_at_ns,
                    snapshot.request.portfolio_ref,
                    snapshot.request.group_id,
                    snapshot.revision,
                ),
            )
            if cursor.rowcount != 1:
                raise IntentConflictError()
            self._insert_event(
                cursor,
                snapshot.request.portfolio_ref,
                snapshot.request.group_id,
                new_snapshot.revision,
                event_type,
                persisted_event_payload,
                occurred_at_ns,
            )
        return new_snapshot

    def get(self, portfolio_ref: str, group_id: str) -> GroupOrderSnapshot | None:
        with self._lock:
            try:
                row = self._connection.execute(
                    """SELECT * FROM order_group_snapshots
                       WHERE portfolio_ref = ? AND group_id = ?""",
                    (portfolio_ref, group_id),
                ).fetchone()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read group journal") from error
        return None if row is None else self._snapshot_from_row(row)

    def list_recoverable(self) -> tuple[GroupOrderSnapshot, ...]:
        states = tuple(state.value for state in GroupOrderState if state not in _TERMINAL_STATES)
        placeholders = ",".join("?" for _ in states)
        with self._lock:
            try:
                rows = self._connection.execute(
                    f"""SELECT * FROM order_group_snapshots
                        WHERE state IN ({placeholders})
                        ORDER BY created_at_ns, portfolio_ref, group_id""",  # noqa: S608
                    states,
                ).fetchall()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to list recoverable groups") from error
        return tuple(self._snapshot_from_row(row) for row in rows)

    def events(
        self, portfolio_ref: str, group_id: str, *, after_sequence: int = 0
    ) -> tuple[GroupOrderEvent, ...]:
        if type(after_sequence) is not int or after_sequence < 0:
            raise ContractValidationError("invalid event cursor")
        with self._lock:
            try:
                rows = self._connection.execute(
                    """SELECT sequence, group_id, event_type, payload_json,
                              event_sha256, created_at_ns
                       FROM order_group_events
                       WHERE portfolio_ref = ? AND group_id = ? AND sequence > ?
                       ORDER BY sequence""",
                    (portfolio_ref, group_id, after_sequence),
                ).fetchall()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to read group events") from error
        result: list[GroupOrderEvent] = []
        for row in rows:
            try:
                payload_json = str(row["payload_json"])
                payload = json.loads(payload_json)
                event_type = str(row["event_type"])
                created_at_ns = int(row["created_at_ns"])
                expected = payload_sha256(
                    {
                        "group_id": group_id,
                        "event_type": event_type,
                        "payload": payload,
                        "created_at_ns": created_at_ns,
                    }
                )
                if canonical_json(payload) != payload_json or expected != str(row["event_sha256"]):
                    raise ValueError("event digest mismatch")
                result.append(
                    GroupOrderEvent(
                        sequence=int(row["sequence"]),
                        group_id=group_id,
                        event_type=event_type,
                        created_at_ns=created_at_ns,
                        payload=payload,
                    )
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                raise DurableStoreError("stored group event is unreadable") from None
        return tuple(result)

    def close(self) -> None:
        with self._lock:
            try:
                self._connection.close()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to close group journal") from error

    def _snapshot_from_row(self, row: sqlite3.Row) -> GroupOrderSnapshot:
        try:
            request_json = str(row["request_json"])
            snapshot_json = str(row["snapshot_json"])
            request_payload = json.loads(request_json)
            snapshot_payload = json.loads(snapshot_json)
            request = GroupOrderIntent.from_payload(request_payload)
            snapshot = GroupOrderSnapshot.from_payload(snapshot_payload)
            if (
                canonical_json(request_payload) != request_json
                or request.fingerprint != str(row["request_fingerprint"])
                or request.fingerprint != snapshot.request.fingerprint
                or canonical_json(snapshot_payload) != snapshot_json
                or payload_sha256(snapshot_payload) != str(row["snapshot_sha256"])
                or snapshot.state.value != str(row["state"])
                or snapshot.revision != int(row["revision"])
                or request.deadline_ns != int(row["deadline_ns"])
                or snapshot.created_at_ns != int(row["created_at_ns"])
                or snapshot.updated_at_ns != int(row["updated_at_ns"])
            ):
                raise ValueError("group snapshot digest mismatch")
            budget_columns = (
                row["budget_policy_fingerprint"],
                row["budget_quote_fingerprint"],
                row["budget_valuation_unit"],
                row["budget_gross_notional"],
                row["budget_deadline_ns"],
                row["budget_state"],
            )
            if request.version == 1:
                if any(value is not None for value in budget_columns):
                    raise ValueError("legacy group row unexpectedly has a budget hold")
            else:
                budget = snapshot.budget_reservation
                if budget is None or budget_columns != (
                    budget.policy_fingerprint,
                    budget.quote_fingerprint,
                    budget.valuation_unit,
                    format(budget.gross_notional, "f"),
                    budget.deadline_ns,
                    budget.state.value,
                ):
                    raise ValueError("group budget columns do not match the typed snapshot")
            return snapshot
        except (ContractValidationError, TypeError, ValueError, json.JSONDecodeError):
            raise DurableStoreError("stored group snapshot is unreadable") from None

    @staticmethod
    def _validate_event(event_type: str, payload: Mapping[str, Any], created_at_ns: int) -> None:
        if event_type not in _EVENT_TYPE or not isinstance(payload, Mapping):
            raise ContractValidationError("invalid group event")
        if type(created_at_ns) is not int or created_at_ns <= 0:
            raise ContractValidationError("invalid group event time")
        try:
            canonical_json(dict(payload))
        except (TypeError, ValueError) as error:
            raise ContractValidationError("invalid group event payload") from error

    @staticmethod
    def _insert_event(
        cursor: sqlite3.Cursor,
        portfolio_ref: str,
        group_id: str,
        revision: int,
        event_type: str,
        payload: Mapping[str, Any],
        created_at_ns: int,
    ) -> None:
        payload_json = canonical_json(dict(payload))
        digest = payload_sha256(
            {
                "group_id": group_id,
                "event_type": event_type,
                "payload": dict(payload),
                "created_at_ns": created_at_ns,
            }
        )
        cursor.execute(
            """INSERT INTO order_group_events(
                   portfolio_ref, group_id, revision, event_type, payload_json,
                   event_sha256, created_at_ns
               ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                portfolio_ref,
                group_id,
                revision,
                event_type,
                payload_json,
                digest,
                created_at_ns,
            ),
        )

    class _Transaction:
        def __init__(self, store: SqliteOrderGroupStore) -> None:
            self._store = store
            self._cursor: sqlite3.Cursor | None = None

        def __enter__(self) -> sqlite3.Cursor:
            self._store._lock.acquire()
            try:
                cursor = self._store._connection.cursor()
                cursor.execute("BEGIN IMMEDIATE")
                self._cursor = cursor
                return cursor
            except sqlite3.Error as error:
                self._store._lock.release()
                raise DurableStoreError("unable to begin group journal transaction") from error

        def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
            assert self._cursor is not None
            try:
                self._cursor.execute("ROLLBACK" if exc_type else "COMMIT")
            except sqlite3.Error as error:
                raise DurableStoreError("group journal transaction failed") from error
            finally:
                self._cursor.close()
                self._store._lock.release()
            return False

    def _transaction(self) -> SqliteOrderGroupStore._Transaction:
        return self._Transaction(self)


def _safe_pre_dispatch_release(snapshot: GroupOrderSnapshot) -> bool:
    if any(leg.submitted_intent is not None for leg in snapshot.legs):
        return False
    return all(
        reservation.phase.value in {"NOT_STARTED", "DENIED", "RELEASED"}
        for reservation in snapshot.reservations
    )


def _proven_no_exposure_close(
    snapshot: GroupOrderSnapshot, event_payload: Mapping[str, Any]
) -> bool:
    if event_payload.get("reason") != "terminal_no_fill":
        return False
    first_leg_id = event_payload.get("first_leg_id")
    try:
        snapshot.request.leg(first_leg_id)
    except (ContractValidationError, TypeError):
        return False
    if any(reservation.phase.value != "RELEASED" for reservation in snapshot.reservations):
        return False
    submitted = [leg for leg in snapshot.legs if leg.submitted_intent is not None]
    if len(submitted) != 1 or submitted[0].leg_id != first_leg_id:
        return False
    progress = submitted[0]
    status = progress.child_status
    resolution = progress.resolution_receipt
    if status is None or resolution is None:
        return False
    return all(
        (
            status.intent_id == progress.submitted_intent.intent_id,
            status.state.value in {"CANCELLED", "REJECTED", "BLOCKED"},
            status.filled_quantity == Decimal("0"),
            resolution.resolution_kind == "TERMINAL_NO_FILL_RESOLVED",
            resolution.final_state is status.state,
            resolution.filled_quantity == Decimal("0"),
            resolution.trade_count == 0,
            resolution.claim.permit.group_id == snapshot.request.group_id,
            resolution.claim.permit.leg_id == first_leg_id,
            resolution.claim.permit.order_intent_hash == progress.submitted_intent.fingerprint,
            not resolution.dispatch_latch_active,
        )
    )


def _fraction_text(value: Fraction) -> str:
    denominator = value.denominator
    twos = 0
    fives = 0
    while denominator % 2 == 0:
        twos += 1
        denominator //= 2
    while denominator % 5 == 0:
        fives += 1
        denominator //= 5
    if denominator != 1:
        raise ContractValidationError("group budget total is not a finite decimal")
    scale = max(twos, fives)
    coefficient = value.numerator * (2 ** (scale - twos)) * (5 ** (scale - fives))
    digits = tuple(int(digit) for digit in str(abs(coefficient))) if coefficient else (0,)
    return format(Decimal((int(coefficient < 0), digits, -scale)), "f")


__all__ = ["SqliteOrderGroupStore"]
