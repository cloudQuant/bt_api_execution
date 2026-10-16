"""Durable local journal for ordered multi-leg execution coordination."""

from __future__ import annotations

import json
import math
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

from .contracts import TERMINAL_STATES as _EXECUTION_TERMINAL_STATES
from .contracts import canonical_json, payload_sha256
from .errors import ContractValidationError, DurableStoreError, IntentConflictError
from .group_order_contracts import (
    GroupAccountReservationSnapshot,
    GroupBudgetReservation,
    GroupBudgetReservationState,
    GroupChildOrderStatus,
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
_MAX_RECOVERABLE_PAGE_SIZE = 256
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
            GroupOrderState.UNWINDING,
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
        "GROUP_COMPENSATION_PLAN_RECORDED",
        "GROUP_COMPENSATION_SUBMIT_STARTED",
        "GROUP_COMPENSATION_STATUS_OBSERVED",
        "GROUP_COMPENSATION_RESOLVED",
        "GROUP_COMPENSATION_CLOSED",
        "GROUP_REJECTED_BEFORE_DISPATCH",
        "GROUP_CLOSED_NO_EXPOSURE",
        "GROUP_RECOVERY_REQUIRED",
    }
)
_COMPENSATION_EVENT_TRANSITIONS = {
    "GROUP_COMPENSATION_PLAN_RECORDED": frozenset(
        {
            (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.UNWINDING),
            (GroupOrderState.UNWINDING, GroupOrderState.UNWINDING),
        }
    ),
    "GROUP_COMPENSATION_SUBMIT_STARTED": frozenset(
        {
            (GroupOrderState.UNWINDING, GroupOrderState.UNWINDING),
            (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.UNWINDING),
        }
    ),
    "GROUP_COMPENSATION_STATUS_OBSERVED": frozenset(
        {
            (GroupOrderState.UNWINDING, GroupOrderState.UNWINDING),
            (GroupOrderState.UNWINDING, GroupOrderState.RECOVERY_REQUIRED),
            (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.RECOVERY_REQUIRED),
            (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.UNWINDING),
        }
    ),
    "GROUP_COMPENSATION_RESOLVED": frozenset(
        {
            (GroupOrderState.UNWINDING, GroupOrderState.UNWINDING),
            (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.UNWINDING),
        }
    ),
    "GROUP_COMPENSATION_CLOSED": frozenset(
        {
            (GroupOrderState.UNWINDING, GroupOrderState.CLOSED),
            (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.CLOSED),
        }
    ),
}
_COMPENSATION_EVENT_TYPES = frozenset(_COMPENSATION_EVENT_TRANSITIONS)


def _valid_recoverable_cursor_ref(value: object) -> bool:
    if type(value) is not str or not 1 <= len(value) <= 128 or not value.isascii():
        return False
    return value[0].isalnum() and all(
        character.isalnum() or character in "._:-" for character in value
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
                    CREATE INDEX IF NOT EXISTS order_group_snapshots_recovery_page
                        ON order_group_snapshots(created_at_ns, portfolio_ref, group_id, state);
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
        if event_type in _COMPENSATION_EVENT_TRANSITIONS:
            if (snapshot.state, target_state) not in _COMPENSATION_EVENT_TRANSITIONS[event_type]:
                raise ContractValidationError("invalid compensation event state transition")
        elif (
            snapshot.state is GroupOrderState.RECOVERY_REQUIRED
            and target_state is GroupOrderState.UNWINDING
        ):
            raise ContractValidationError("recovery can enter unwinding only through compensation")
        compensation_history: tuple[GroupOrderEvent, ...] = ()
        if event_type in _COMPENSATION_EVENT_TYPES:
            compensation_history = self.events(
                snapshot.request.portfolio_ref, snapshot.request.group_id
            )
            if len(compensation_history) != snapshot.revision + 1:
                raise IntentConflictError()
            _validate_compensation_event_transition(
                snapshot,
                event_type,
                event_payload,
                compensation_history,
                target_state,
            )
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
                if event_type == "GROUP_CLOSED_NO_EXPOSURE":
                    proven = _proven_no_exposure_close(candidate, persisted_event_payload)
                elif event_type == "GROUP_COMPENSATION_CLOSED":
                    proven = _proven_group_compensation_close(
                        candidate, persisted_event_payload, compensation_history
                    )
                else:
                    proven = False
                if not proven:
                    raise ContractValidationError(
                        "group budget release requires exact terminal no-exposure evidence"
                    )
                target_budget_state = GroupBudgetReservationState.RELEASED
            if target_budget_state is not current_budget.state:
                target_budget = replace(current_budget, state=target_budget_state)
                if event_type not in _COMPENSATION_EVENT_TYPES:
                    persisted_event_payload["budget_transition"] = {
                        "from": current_budget.state.value,
                        "to": target_budget_state.value,
                        "policy_fingerprint": current_budget.policy_fingerprint,
                        "quote_fingerprint": current_budget.quote_fingerprint,
                        "valuation_unit": current_budget.valuation_unit,
                        "gross_notional": format(current_budget.gross_notional, "f"),
                    }
            elif event_type not in _COMPENSATION_EVENT_TYPES:
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

    def list_recoverable_page(
        self,
        *,
        after: tuple[int, str, str] | None = None,
        limit: int = 64,
    ) -> tuple[GroupOrderSnapshot, ...]:
        """Read a bounded page of nonterminal snapshots after a stable key.

        The key is ``(created_at_ns, portfolio_ref, group_id)`` and is
        exclusive, so callers can persist or retain it between owner ticks.
        State is evaluated on each page read: rows that become terminal are
        skipped and their immutable creation key never changes.
        """

        if type(limit) is not int or not 1 <= limit <= _MAX_RECOVERABLE_PAGE_SIZE:
            raise ContractValidationError("invalid recoverable group page limit")
        if after is not None and (
            type(after) is not tuple
            or len(after) != 3
            or type(after[0]) is not int
            or not 0 < after[0] <= (1 << 63) - 1
            or not _valid_recoverable_cursor_ref(after[1])
            or not _valid_recoverable_cursor_ref(after[2])
        ):
            raise ContractValidationError("invalid recoverable group page cursor")

        states = tuple(state.value for state in GroupOrderState if state not in _TERMINAL_STATES)
        placeholders = ",".join("?" for _ in states)
        query = (
            "SELECT * FROM order_group_snapshots "
            f"WHERE state IN ({placeholders})"
        )
        parameters: tuple[object, ...] = states
        if after is not None:
            query += " AND (created_at_ns, portfolio_ref, group_id) > (?, ?, ?)"
            parameters += after
        query += " ORDER BY created_at_ns, portfolio_ref, group_id LIMIT ?"
        parameters += (limit,)
        with self._lock:
            try:
                rows = self._connection.execute(query, parameters).fetchall()
            except sqlite3.Error as error:
                raise DurableStoreError("unable to page recoverable groups") from error
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


def _proven_group_compensation_close(
    snapshot: GroupOrderSnapshot,
    event_payload: Mapping[str, Any],
    history: tuple[GroupOrderEvent, ...],
) -> bool:
    """Require durable, full-fill CLOSE receipts for every actual source fill.

    The coordinator verifies every receipt against the account-owned Risk
    journal before recording ``GROUP_COMPENSATION_RESOLVED``.  This store
    independently binds that ordered evidence to the accepted group, checks
    each receipt digest and action phase, and recomputes the signed per-leg
    exposure from source fills minus verified CLOSE fills.  The historical
    chain-deficit vector is retained for diagnosis; it is never used as the
    quantity to close.
    """

    try:
        request = snapshot.request
        policy = request.compensation_budget_policy
        if (
            snapshot.state
            not in {GroupOrderState.UNWINDING, GroupOrderState.RECOVERY_REQUIRED}
            or request.version != 3
            or len(request.legs) != 3
            or policy is None
            or snapshot.residual_vector is None
            or snapshot.budget_reservation is None
            or snapshot.budget_reservation.state
            not in {GroupBudgetReservationState.RESERVED, GroupBudgetReservationState.HELD}
            # ``update`` passes the next-revision candidate while history
            # still contains only events through the previous revision.
            or len(history) != snapshot.revision
        ):
            return False
        if any(reservation.phase.value != "RELEASED" for reservation in snapshot.reservations):
            return False
        if len(snapshot.reservations) != 1:
            return False
        for reservation in snapshot.reservations:
            if reservation.phase.value != "RELEASED":
                return False
            for leg in reservation.legs:
                if leg.claimed:
                    if not leg.resolved or leg.state not in {"SETTLED", "NO_FILL_TERMINAL"}:
                        return False
                elif leg.state not in {"RELEASED", "EXPIRED"}:
                    return False

        plans_by_leg = {plan.leg_id: plan for plan in request.legs}
        progress_by_leg = {progress.leg_id: progress for progress in snapshot.legs}
        exposure: dict[str, Fraction] = {}
        for plan in request.legs:
            progress = progress_by_leg[plan.leg_id]
            intent = progress.submitted_intent
            status = progress.child_status
            claim = progress.claim_receipt
            resolution = progress.resolution_receipt
            if intent is None:
                if (
                    progress.phase.value != "READY"
                    or status is not None
                    or claim is not None
                    or resolution is not None
                ):
                    return False
                exposure[plan.leg_id] = Fraction(0)
                continue
            if (
                status is None
                or claim is None
                or resolution is None
                or progress.phase.value != "OBSERVED"
                or intent.intent_id != plan.maximum_intent.intent_id
                or intent.position_effect.value != "OPEN"
                or status.intent_id != intent.intent_id
                or status.state not in _EXECUTION_TERMINAL_STATES
                or Fraction(status.filled_quantity) > Fraction(intent.quantity)
                or claim.permit.group_id != request.group_id
                or claim.permit.leg_id != plan.leg_id
                or claim.permit.order_intent_hash != intent.fingerprint
                or resolution.claim != claim
                or resolution.final_state is not status.state
                or resolution.filled_quantity != status.filled_quantity
                or resolution.dispatch_latch_active
            ):
                return False
            if status.filled_quantity > 0:
                if (
                    resolution.resolution_kind != "TERMINAL_EXPOSURE_RESOLVED"
                    or resolution.trade_count <= 0
                ):
                    return False
            elif resolution.resolution_kind != "TERMINAL_NO_FILL_RESOLVED":
                return False
            sign = 1 if intent.side.value == "BUY" else -1
            exposure[plan.leg_id] = (
                sign
                * Fraction(status.filled_quantity)
                * Fraction(plan.base_units_per_order_unit)
            )

        expected_chain_vector = [
            [plan.leg_id, format(snapshot.residual_vector[index], "f")]
            for index, plan in enumerate(request.legs)
        ]
        if event_payload.get("schema") != "group-compensation-close-proof-v1":
            return False
        if event_payload.get("group_fingerprint") != request.fingerprint:
            return False
        if event_payload.get("chain_deficit_vector_base_units") != expected_chain_vector:
            return False

        planned: dict[str, dict[str, Any]] = {}
        started: dict[str, dict[str, Any]] = {}
        statuses: dict[str, list[dict[str, Any]]] = {}
        resolved: dict[str, dict[str, Any]] = {}
        for event in history:
            payload = event.payload
            if event.event_type == "GROUP_COMPENSATION_PLAN_RECORDED":
                action_id = payload.get("action_id")
                authorization = payload.get("authorization")
                if (
                    set(payload) != {"action_id", "authorization_digest", "authorization"}
                    or type(action_id) is not str
                    or action_id in planned
                    or not isinstance(authorization, Mapping)
                    or payload.get("authorization_digest")
                    != authorization.get("authorization_digest")
                ):
                    return False
                planned[action_id] = dict(payload)
            elif event.event_type == "GROUP_COMPENSATION_SUBMIT_STARTED":
                action_id = payload.get("action_id")
                if (
                    set(payload)
                    != {"action_id", "authorization_digest", "intent_id", "client_order_id"}
                    or type(action_id) is not str
                    or action_id not in planned
                    or action_id in started
                    or payload.get("authorization_digest")
                    != planned[action_id].get("authorization_digest")
                    or payload.get("intent_id")
                    != planned[action_id]["authorization"].get("intent_id")
                    or payload.get("client_order_id")
                    != planned[action_id]["authorization"].get("client_order_id")
                ):
                    return False
                started[action_id] = dict(payload)
            elif event.event_type == "GROUP_COMPENSATION_STATUS_OBSERVED":
                action_id = payload.get("action_id")
                status_payload = payload.get("status")
                if (
                    set(payload) != {"action_id", "authorization_digest", "status"}
                    or type(action_id) is not str
                    or action_id not in started
                    or not isinstance(status_payload, Mapping)
                    or payload.get("authorization_digest")
                    != planned[action_id].get("authorization_digest")
                ):
                    return False
                try:
                    status_value = GroupChildOrderStatus.from_payload(status_payload)
                except Exception:
                    return False
                close_quantity = _exact_event_fraction(
                    planned[action_id]["authorization"].get("close_quantity")
                )
                if (
                    status_value.intent_id
                    != planned[action_id]["authorization"].get("intent_id")
                    or Fraction(status_value.filled_quantity) > close_quantity
                ):
                    return False
                statuses.setdefault(action_id, []).append(dict(status_payload))
            elif event.event_type == "GROUP_COMPENSATION_RESOLVED":
                action_id = payload.get("action_id")
                if (
                    set(payload)
                    != {
                        "action_id",
                        "authorization_digest",
                        "authorization",
                        "close_receipt",
                    }
                    or type(action_id) is not str
                    or action_id not in started
                    or action_id in resolved
                    or payload.get("authorization_digest")
                    != planned[action_id].get("authorization_digest")
                    or payload.get("authorization")
                    != planned[action_id].get("authorization")
                ):
                    return False
                resolved[action_id] = dict(payload)

        if (
            not planned
            or len(planned) > 9
            or set(planned) != set(started)
            or set(planned) != set(resolved)
            or set(planned) != set(statuses)
        ):
            return False

        cap = Fraction(policy.cap_amount)
        consumed = Fraction(0)
        resolved_refs: list[dict[str, Any]] = []
        action_ordinals: dict[str, int] = {}
        for action_id in planned:
            plan_event = planned[action_id]
            authorization = plan_event["authorization"]
            digest = plan_event.get("authorization_digest")
            authorization_body = dict(authorization)
            embedded_digest = authorization_body.pop("authorization_digest", None)
            if (
                authorization.get("schema") != "group-compensation-close-auth-v4"
                or type(digest) is not str
                or embedded_digest != digest
                or payload_sha256(authorization_body) != digest
                or authorization.get("action_id") != action_id
                or authorization.get("group_id") != request.group_id
                or authorization.get("group_fingerprint") != request.fingerprint
                or authorization.get("source_request_fingerprint") != request.fingerprint
                or authorization.get("action_remaining_budget_policy_fingerprint")
                != policy.fingerprint
                or authorization.get("action_remaining_budget_policy_id") != policy.policy_id
                or authorization.get("action_remaining_budget_policy_version")
                != policy.policy_version
                or authorization.get("action_remaining_budget_metric") != policy.metric
                or authorization.get("action_remaining_budget_currency") != policy.currency
                or _exact_event_fraction(authorization.get("compensation_budget_cap_amount"))
                != Fraction(policy.cap_amount)
                or _exact_event_fraction(authorization.get("action_remaining_budget_amount"))
                != cap - consumed
            ):
                return False

            source_leg_id = authorization.get("source_leg_id")
            if source_leg_id not in plans_by_leg:
                return False
            source_plan = plans_by_leg[source_leg_id]
            source_progress = progress_by_leg[source_leg_id]
            source_intent = source_progress.submitted_intent
            source_status = source_progress.child_status
            if source_intent is None or source_status is None:
                return False
            coordinate = exposure[source_leg_id]
            source_side = source_intent.side.value
            closing_side = "SELL" if source_side == "BUY" else "BUY"
            if (
                authorization.get("source_intent_id") != source_intent.intent_id
                or authorization.get("source_terminal_status") != source_status.to_payload()
                or authorization.get("source_side") != source_side
                or authorization.get("closing_side") != closing_side
                or authorization.get("source_price")
                != (None if source_intent.price is None else format(source_intent.price, "f"))
                or source_intent.price is None
                or _exact_event_fraction(authorization.get("source_base_units_per_order_unit"))
                != Fraction(source_plan.base_units_per_order_unit)
                or _exact_event_fraction(authorization.get("source_requested_quantity"))
                != Fraction(source_intent.quantity)
                or _exact_event_fraction(authorization.get("source_uncompensated_position_base_units"))
                != coordinate
                or coordinate == 0
                or (source_side == "BUY" and coordinate < 0)
                or (source_side == "SELL" and coordinate > 0)
                or authorization.get("source_claim_digest")
                != payload_sha256(source_progress.claim_receipt.to_payload())
                or authorization.get("source_risk_resolution_digest")
                != payload_sha256(source_progress.resolution_receipt.to_payload())
                or authorization.get("instrument") != source_intent.instrument
                or authorization.get("metadata_version") != source_intent.metadata_version
            ):
                return False

            vector = authorization.get("source_filled_position_exposure_vector_base_units")
            expected_vector = [
                [leg.leg_id, _exact_event_fraction_text(exposure[leg.leg_id])]
                for leg in request.legs
            ]
            if vector != expected_vector:
                return False
            source_chain = authorization.get("source_chain_deficit_vector_base_units")
            if source_chain != expected_chain_vector:
                return False
            if authorization.get("prior_resolved_actions") != resolved_refs:
                return False

            ordinal = authorization.get("ordinal")
            if type(ordinal) is not int or ordinal <= 0:
                return False
            expected_ordinal = action_ordinals.get(source_leg_id, 0) + 1
            if ordinal != expected_ordinal:
                return False
            action_ordinals[source_leg_id] = ordinal
            source_event_digest = authorization.get("source_event_digest")
            if not _is_sha256_digest(source_event_digest):
                return False
            derived_action_id = "gca-" + payload_sha256(
                {
                    "group_id": request.group_id,
                    "group_fingerprint": request.fingerprint,
                    "source_leg_id": source_leg_id,
                    "source_event_digest": source_event_digest,
                    "ordinal": ordinal,
                }
            )[:48]
            expected_intent_id = "gclose-" + action_id
            risk_intent_id = authorization.get("risk_intent_id")
            expected_client_order_id = "gco-" + payload_sha256(
                {"schema": "group-compensation-client-order-id-v1", "action_id": action_id}
            )[:48]
            if (
                action_id != derived_action_id
                or authorization.get("intent_id") != expected_intent_id
                or not _is_risk_order_intent_id(risk_intent_id)
                or authorization.get("client_order_id") != expected_client_order_id
            ):
                return False

            close_quantity = _exact_event_fraction(authorization.get("close_quantity"))
            close_notional = _exact_event_fraction(
                authorization.get("close_reference_notional")
            )
            limit_price = _exact_event_fraction(authorization.get("source_price"))
            conversion = Fraction(source_plan.base_units_per_order_unit)
            if (
                close_quantity <= 0
                or close_quantity * conversion > abs(coordinate)
                or close_notional != close_quantity * limit_price * conversion
                or close_notional > cap - consumed
                or authorization.get("close_quantity_unit")
                != authorization.get("source_quantity_unit")
                or authorization.get("source_quote_currency") != policy.currency
            ):
                return False

            action_statuses = statuses[action_id]
            final_status = action_statuses[-1]
            if (
                any(item.get("intent_id") != expected_intent_id for item in action_statuses)
                or final_status.get("state") != "FILLED"
                or _exact_event_fraction(final_status.get("filled_quantity")) != close_quantity
            ):
                return False
            resolution_event = resolved[action_id]
            receipt = resolution_event.get("close_receipt")
            if not isinstance(receipt, Mapping):
                return False
            receipt_body = dict(receipt)
            receipt_digest = receipt_body.pop("receipt_sha256", None)
            if (
                not _is_sha256_digest(receipt_digest)
                or payload_sha256(receipt_body) != receipt_digest
                or receipt.get("status") != "TERMINAL_CLOSE_RESOLVED"
                or receipt.get("final_state") != "FILLED"
                or receipt.get("position_effect") != "CLOSE"
                or receipt.get("claim", {}).get("intent_id") != risk_intent_id
                or _exact_event_fraction(receipt.get("requested_quantity")) != close_quantity
                or _exact_event_fraction(receipt.get("filled_quantity")) != close_quantity
                or type(receipt.get("trade_count")) is not int
                or receipt.get("trade_count", 0) <= 0
                or receipt.get("dispatch_latch_active") is not False
                or receipt.get("has_other_active_freezes") is not False
                or not _is_sha256_digest(receipt.get("proof_sha256"))
                or not _is_sha256_digest(receipt.get("exposure_proof_sha256"))
                or not _is_sha256_digest(receipt.get("exposure_reservation_sha256"))
            ):
                return False
            claim = receipt.get("claim")
            if (
                type(claim) is not dict
                or set(claim)
                != {
                    "cause_id",
                    "claim_digest",
                    "claimed_at",
                    "intent_hash",
                    "intent_id",
                    "permit_id",
                    "scope_key",
                }
                or type(claim.get("claimed_at")) not in (int, float)
                or not math.isfinite(claim["claimed_at"])
                or claim["claimed_at"] <= 0
                or not all(
                type(claim.get(key)) is str and claim[key]
                for key in ("cause_id", "claim_digest", "intent_hash", "intent_id", "permit_id", "scope_key")
                )
            ):
                return False

            close_sign = 1 if closing_side == "BUY" else -1
            exposure[source_leg_id] += close_sign * close_quantity * conversion
            if not _is_sha256_digest(receipt_digest):
                return False
            resolved_refs.append(
                {
                    "schema": "group-compensation-resolved-action-ref-v3",
                    "action_id": action_id,
                    "source_leg_id": source_leg_id,
                    "intent_id": expected_intent_id,
                    "risk_intent_id": risk_intent_id,
                    "client_order_id": expected_client_order_id,
                    "permit_id": claim["permit_id"],
                    "authorization_digest": digest,
                    "receipt_sha256": receipt_digest,
                    "closing_side": closing_side,
                    "close_quantity": authorization["close_quantity"],
                    "close_quantity_unit": authorization["close_quantity_unit"],
                    "source_base_units_per_order_unit": authorization[
                        "source_base_units_per_order_unit"
                    ],
                    "instrument": authorization["instrument"],
                    "metadata_version": authorization["metadata_version"],
                    "close_limit_price": authorization["source_price"],
                    "close_reference_notional": authorization[
                        "close_reference_notional"
                    ],
                    "quote_currency": authorization["source_quote_currency"],
                    "budget_policy_fingerprint": policy.fingerprint,
                }
            )
            consumed += close_notional

        if any(exposure[plan.leg_id] != 0 for plan in request.legs):
            return False
        remaining = cap - consumed
        reservation_digest = payload_sha256(
            [reservation.to_payload() for reservation in snapshot.reservations]
        )
        expected_receipts = [item["receipt_sha256"] for item in resolved_refs]
        expected_payload = {
            "schema": "group-compensation-close-proof-v1",
            "group_fingerprint": request.fingerprint,
            "closure_facts_digest": event_payload.get("closure_facts_digest"),
            "chain_deficit_vector_base_units": expected_chain_vector,
            "signed_uncompensated_filled_exposure_vector_base_units": [
                [plan.leg_id, "0"] for plan in request.legs
            ],
            "resolved_action_receipt_sha256s": expected_receipts,
            "reservation_state_sha256": reservation_digest,
            "active_dispatch_latches": 0,
            "budget_policy_fingerprint": policy.fingerprint,
            "budget_cap_amount": format(policy.cap_amount, "f"),
            "budget_consumed_reference_notional": _exact_event_fraction_text(consumed),
            "budget_remaining_amount": _exact_event_fraction_text(remaining),
            "pending_budget_obligation_amount": "0",
        }
        return (
            _is_sha256_digest(expected_payload["closure_facts_digest"])
            and dict(event_payload) == expected_payload
        )
    except (AttributeError, KeyError, TypeError, ValueError, ZeroDivisionError):
        return False


def _is_sha256_digest(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _is_risk_order_intent_id(value: object) -> bool:
    return (
        type(value) is str
        and value.startswith("risk-order:")
        and _is_sha256_digest(value[len("risk-order:"):])
    )


def _exact_event_fraction(value: object) -> Fraction:
    if type(value) is not str or not 1 <= len(value) <= 512:
        raise ValueError("invalid exact compensation decimal")
    try:
        decimal_value = Decimal(value)
    except Exception:
        raise ValueError("invalid exact compensation decimal") from None
    if (
        not decimal_value.is_finite()
        or format(decimal_value, "f") != value
        or len(decimal_value.as_tuple().digits) > 256
        or abs(decimal_value.as_tuple().exponent) > 256
    ):
        raise ValueError("invalid exact compensation decimal")
    return Fraction(decimal_value)


def _exact_event_fraction_text(value: Fraction) -> str:
    if value == 0:
        return "0"
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
        raise ValueError("compensation exposure is not a finite decimal")
    scale = max(twos, fives)
    coefficient = value.numerator * (2 ** (scale - twos)) * (5 ** (scale - fives))
    digits = tuple(int(digit) for digit in str(abs(coefficient))) if coefficient else (0,)
    return format(Decimal((int(coefficient < 0), digits, -scale)), "f")


def _validate_compensation_event_transition(
    snapshot: GroupOrderSnapshot,
    event_type: str,
    payload: Mapping[str, Any],
    history: tuple[GroupOrderEvent, ...],
    target_state: GroupOrderState,
) -> None:
    """Reject malformed or regressing compensation events before SQLite writes.

    The event chain itself is hash-verified by ``events``. This validator adds
    the semantic state machine: each planned close has one durable start,
    status observations are monotonic, and only an exact full-fill typed CLOSE
    receipt can resolve an action. The writer still checks the snapshot
    revision inside its transaction after this read-only validation.
    """

    request = snapshot.request
    if request.version != 3 or len(request.legs) != 3 or request.compensation_budget_policy is None:
        raise ContractValidationError("compensation events require a budgeted v3 group")

    auth_keys = {
        "schema", "action_id", "ordinal", "group_id", "origin_run_id",
        "group_fingerprint", "execution_scope_key", "account_ref", "strategy_id",
        "source_leg_id", "source_intent_id", "source_request_fingerprint",
        "source_terminal_status", "source_requested_quantity", "source_claim_digest",
        "source_risk_resolution_digest", "source_event_digest", "source_side",
        "instrument", "metadata_version", "source_price",
        "source_base_units_per_order_unit", "source_quantity_unit",
        "source_quote_currency", "closing_side", "close_quantity",
        "close_quantity_unit", "close_reference_notional",
        "source_uncompensated_position_base_units",
        "source_chain_deficit_vector_base_units",
        "source_filled_position_exposure_vector_base_units",
        "prior_resolved_actions", "compensation_budget_cap_amount",
        "action_remaining_budget_amount", "action_remaining_budget_currency",
        "action_remaining_budget_metric", "action_remaining_budget_policy_id",
        "action_remaining_budget_policy_version",
        "action_remaining_budget_policy_fingerprint", "close_position_version",
        "close_allocation_available_quantity", "close_allocation_quantity_unit",
        "intent_id", "risk_intent_id", "client_order_id", "source_facts_digest",
        "authorization_digest",
    }
    actions: dict[str, dict[str, Any]] = {}
    leg_order = tuple(plan.leg_id for plan in request.legs)
    policy = request.compensation_budget_policy
    for event in history:
        kind = event.event_type
        item = event.payload
        if kind == "GROUP_COMPENSATION_PLAN_RECORDED":
            if set(item) != {"action_id", "authorization_digest", "authorization"}:
                raise ContractValidationError("invalid compensation plan event")
            auth = item["authorization"]
            if not isinstance(auth, Mapping) or set(auth) != auth_keys:
                raise ContractValidationError("invalid compensation authorization payload")
            body = dict(auth)
            auth_digest = body.pop("authorization_digest", None)
            action_id = auth.get("action_id")
            if (
                auth.get("schema") != "group-compensation-close-auth-v4"
                or type(action_id) is not str
                or action_id in actions
                or item.get("action_id") != action_id
                or item.get("authorization_digest") != auth_digest
                or not _is_sha256_digest(auth_digest)
                or payload_sha256(body) != auth_digest
                or auth.get("group_id") != request.group_id
                or auth.get("group_fingerprint") != request.fingerprint
                or auth.get("source_request_fingerprint") != request.fingerprint
                or auth.get("action_id") != "gca-" + payload_sha256(
                    {
                        "group_id": request.group_id,
                        "group_fingerprint": request.fingerprint,
                        "source_leg_id": auth.get("source_leg_id"),
                        "source_event_digest": auth.get("source_event_digest"),
                        "ordinal": auth.get("ordinal"),
                    }
                )[:48]
                or auth.get("intent_id") != "gclose-" + action_id
                or not _is_risk_order_intent_id(auth.get("risk_intent_id"))
                or auth.get("client_order_id") != "gco-" + payload_sha256(
                    {"schema": "group-compensation-client-order-id-v1", "action_id": action_id}
                )[:48]
                or auth.get("action_remaining_budget_policy_id") != policy.policy_id
                or auth.get("action_remaining_budget_policy_version") != policy.policy_version
                or auth.get("action_remaining_budget_policy_fingerprint") != policy.fingerprint
                or auth.get("action_remaining_budget_metric") != policy.metric
                or auth.get("action_remaining_budget_currency") != policy.currency
                or _exact_event_fraction(auth.get("compensation_budget_cap_amount"))
                != Fraction(policy.cap_amount)
                or auth.get("source_leg_id") not in leg_order
                or not _is_sha256_digest(auth.get("source_facts_digest"))
                or not _is_sha256_digest(auth.get("source_event_digest"))
            ):
                raise ContractValidationError("compensation plan binding is invalid")
            actions[action_id] = {
                "authorization": dict(auth),
                "digest": auth_digest,
                "phase": "PLAN_RECORDED",
                "statuses": [],
                "receipt": None,
            }
        elif kind == "GROUP_COMPENSATION_SUBMIT_STARTED":
            if set(item) != {"action_id", "authorization_digest", "intent_id", "client_order_id"}:
                raise ContractValidationError("invalid compensation start event")
            action = actions.get(item.get("action_id"))
            if (
                action is None
                or action["phase"] != "PLAN_RECORDED"
                or item.get("authorization_digest") != action["digest"]
                or item.get("intent_id") != action["authorization"]["intent_id"]
                or item.get("client_order_id") != action["authorization"]["client_order_id"]
            ):
                raise ContractValidationError("compensation start does not match its plan")
            action["phase"] = "SUBMIT_STARTED"
        elif kind == "GROUP_COMPENSATION_STATUS_OBSERVED":
            if set(item) != {"action_id", "authorization_digest", "status"}:
                raise ContractValidationError("invalid compensation status event")
            action = actions.get(item.get("action_id"))
            if (
                action is None
                or action["phase"] not in {"SUBMIT_STARTED", "STATUS_OBSERVED"}
                or item.get("authorization_digest") != action["digest"]
                or not isinstance(item.get("status"), Mapping)
            ):
                raise ContractValidationError("compensation status is not attached to a started action")
            try:
                status = GroupChildOrderStatus.from_payload(item["status"])
            except Exception:
                raise ContractValidationError("invalid compensation child status") from None
            auth = action["authorization"]
            previous = (
                None
                if not action["statuses"]
                else GroupChildOrderStatus.from_payload(action["statuses"][-1])
            )
            if (
                status.intent_id != auth["intent_id"]
                or Fraction(status.filled_quantity) > _exact_event_fraction(auth["close_quantity"])
                or (
                    previous is not None
                    and (
                        previous.state in _EXECUTION_TERMINAL_STATES
                        or Fraction(status.filled_quantity) < Fraction(previous.filled_quantity)
                        or not _valid_compensation_status_transition(previous.state.value, status.state.value)
                    )
                )
            ):
                raise ContractValidationError("compensation status regressed or exceeds its plan")
            action["statuses"].append(dict(item["status"]))
            action["phase"] = "STATUS_OBSERVED"
        elif kind == "GROUP_COMPENSATION_RESOLVED":
            if set(item) != {"action_id", "authorization_digest", "authorization", "close_receipt"}:
                raise ContractValidationError("invalid compensation resolution event")
            action = actions.get(item.get("action_id"))
            if (
                action is None
                or action["phase"] not in {"SUBMIT_STARTED", "STATUS_OBSERVED"}
                or item.get("authorization_digest") != action["digest"]
                or item.get("authorization") != action["authorization"]
                or not action["statuses"]
                or not isinstance(item.get("close_receipt"), Mapping)
            ):
                raise ContractValidationError("compensation resolution is not bound to an active action")
            status = GroupChildOrderStatus.from_payload(action["statuses"][-1])
            receipt = dict(item["close_receipt"])
            receipt_body = dict(receipt)
            receipt_digest = receipt_body.pop("receipt_sha256", None)
            quantity = _exact_event_fraction(action["authorization"]["close_quantity"])
            if (
                status.state.value != "FILLED"
                or Fraction(status.filled_quantity) != quantity
                or not _is_sha256_digest(receipt_digest)
                or payload_sha256(receipt_body) != receipt_digest
                or receipt.get("status") != "TERMINAL_CLOSE_RESOLVED"
                or receipt.get("final_state") != "FILLED"
                or receipt.get("position_effect") != "CLOSE"
                or receipt.get("claim", {}).get("intent_id") != action["authorization"]["risk_intent_id"]
                or _exact_event_fraction(receipt.get("requested_quantity")) != quantity
                or _exact_event_fraction(receipt.get("filled_quantity")) != quantity
                or receipt.get("dispatch_latch_active") is not False
                or receipt.get("has_other_active_freezes") is not False
            ):
                raise ContractValidationError("compensation resolution lacks exact terminal CLOSE receipt")
            action["phase"] = "RESOLVED"
            action["receipt"] = receipt

    pending = [item for item in actions.values() if item["phase"] != "RESOLVED"]
    if len(actions) > 9 or len(pending) > 1:
        raise ContractValidationError("compensation action history is not bounded or sequential")
    if event_type == "GROUP_COMPENSATION_PLAN_RECORDED":
        if (
            (snapshot.state, target_state) not in _COMPENSATION_EVENT_TRANSITIONS[event_type]
            or pending
            or set(payload) != {"action_id", "authorization_digest", "authorization"}
        ):
            raise ContractValidationError("a compensation plan requires no pending action")
        auth = payload.get("authorization")
        if (
            not isinstance(auth, Mapping)
            or set(auth) != auth_keys
            or payload.get("action_id") != auth.get("action_id")
            or payload.get("authorization_digest") != auth.get("authorization_digest")
            or not _is_sha256_digest(auth.get("authorization_digest"))
            or payload_sha256(
                {
                    key: value
                    for key, value in auth.items()
                    if key != "authorization_digest"
                }
            ) != auth.get("authorization_digest")
            or auth.get("schema") != "group-compensation-close-auth-v4"
            or auth.get("group_id") != request.group_id
            or auth.get("group_fingerprint") != request.fingerprint
            or auth.get("source_request_fingerprint") != request.fingerprint
            or auth.get("action_remaining_budget_policy_fingerprint") != policy.fingerprint
            or auth.get("action_remaining_budget_policy_id") != policy.policy_id
            or auth.get("action_remaining_budget_policy_version") != policy.policy_version
            or auth.get("action_remaining_budget_metric") != policy.metric
            or auth.get("action_remaining_budget_currency") != policy.currency
            or auth.get("compensation_budget_cap_amount") != format(policy.cap_amount, "f")
            or auth.get("source_leg_id") not in leg_order
            or auth.get("intent_id") != "gclose-" + str(auth.get("action_id"))
            or not _is_risk_order_intent_id(auth.get("risk_intent_id"))
            or auth.get("client_order_id") != "gco-" + payload_sha256(
                {
                    "schema": "group-compensation-client-order-id-v1",
                    "action_id": auth.get("action_id"),
                }
            )[:48]
            or auth.get("source_filled_position_exposure_vector_base_units")
            != [
                [leg_id, _exact_event_fraction_text(amount)]
                for leg_id, amount in _current_signed_source_exposure(snapshot, actions).items()
            ]
        ):
            raise ContractValidationError("compensation plan exposure does not match source facts")
    elif event_type == "GROUP_COMPENSATION_SUBMIT_STARTED":
        if set(payload) != {"action_id", "authorization_digest", "intent_id", "client_order_id"}:
            raise ContractValidationError("invalid compensation submit marker")
        action = actions.get(payload.get("action_id"))
        if (
            action is None
            or action["phase"] != "PLAN_RECORDED"
            or payload.get("authorization_digest") != action["digest"]
            or payload.get("intent_id") != action["authorization"]["intent_id"]
            or payload.get("client_order_id") != action["authorization"]["client_order_id"]
        ):
            raise ContractValidationError("submit marker must follow exactly one plan")
    elif event_type == "GROUP_COMPENSATION_STATUS_OBSERVED":
        if set(payload) != {"action_id", "authorization_digest", "status"}:
            raise ContractValidationError("invalid compensation status observation")
        action = actions.get(payload.get("action_id"))
        if (
            action is None
            or action["phase"] not in {"SUBMIT_STARTED", "STATUS_OBSERVED"}
            or payload.get("authorization_digest") != action["digest"]
            or not isinstance(payload.get("status"), Mapping)
        ):
            raise ContractValidationError("status marker requires a started action")
        try:
            observed = GroupChildOrderStatus.from_payload(payload["status"])
            previous = (
                None
                if not action["statuses"]
                else GroupChildOrderStatus.from_payload(action["statuses"][-1])
            )
        except Exception:
            raise ContractValidationError("invalid compensation status observation") from None
        if (
            observed.intent_id != action["authorization"]["intent_id"]
            or Fraction(observed.filled_quantity)
            > _exact_event_fraction(action["authorization"]["close_quantity"])
            or (
                previous is not None
                and (
                    previous.state in _EXECUTION_TERMINAL_STATES
                    or Fraction(observed.filled_quantity) < Fraction(previous.filled_quantity)
                    or not _valid_compensation_status_transition(
                        previous.state.value, observed.state.value
                    )
                )
            )
        ):
            raise ContractValidationError("compensation status observation regressed")
    elif event_type == "GROUP_COMPENSATION_RESOLVED":
        if set(payload) != {
            "action_id",
            "authorization_digest",
            "authorization",
            "close_receipt",
        }:
            raise ContractValidationError("invalid compensation resolution payload")
        action = actions.get(payload.get("action_id"))
        if (
            action is None
            or action["phase"] not in {"SUBMIT_STARTED", "STATUS_OBSERVED"}
            or payload.get("authorization_digest") != action["digest"]
            or payload.get("authorization") != action["authorization"]
            or not action["statuses"]
            or not isinstance(payload.get("close_receipt"), Mapping)
        ):
            raise ContractValidationError("resolution marker requires a fully filled close")
        try:
            final_status = GroupChildOrderStatus.from_payload(action["statuses"][-1])
            receipt = dict(payload["close_receipt"])
            receipt_body = dict(receipt)
            receipt_digest = receipt_body.pop("receipt_sha256", None)
            close_quantity = _exact_event_fraction(action["authorization"]["close_quantity"])
            valid = (
                final_status.state.value == "FILLED"
                and Fraction(final_status.filled_quantity) == close_quantity
                and _is_sha256_digest(receipt_digest)
                and payload_sha256(receipt_body) == receipt_digest
                and receipt.get("status") == "TERMINAL_CLOSE_RESOLVED"
                and receipt.get("final_state") == "FILLED"
                and receipt.get("position_effect") == "CLOSE"
                and receipt.get("claim", {}).get("intent_id") == action["authorization"]["risk_intent_id"]
                and _exact_event_fraction(receipt.get("requested_quantity")) == close_quantity
                and _exact_event_fraction(receipt.get("filled_quantity")) == close_quantity
                and receipt.get("dispatch_latch_active") is False
                and receipt.get("has_other_active_freezes") is False
            )
        except Exception:
            valid = False
        if not valid:
            raise ContractValidationError("resolution marker has no exact terminal CLOSE receipt")
    elif event_type == "GROUP_COMPENSATION_CLOSED":
        if pending or not actions:
            raise ContractValidationError("group close requires all actions resolved")


def _current_signed_source_exposure(
    snapshot: GroupOrderSnapshot,
    prior_actions: Mapping[str, Mapping[str, Any]],
) -> dict[str, Fraction]:
    request = snapshot.request
    progress = {item.leg_id: item for item in snapshot.legs}
    exposure: dict[str, Fraction] = {}
    for plan in request.legs:
        item = progress[plan.leg_id]
        intent = item.submitted_intent
        status = item.child_status
        if intent is None or status is None:
            exposure[plan.leg_id] = Fraction(0)
        else:
            if status.state not in _EXECUTION_TERMINAL_STATES:
                raise ContractValidationError("source group leg is not terminal")
            sign = 1 if intent.side.value == "BUY" else -1
            exposure[plan.leg_id] = (
                sign * Fraction(status.filled_quantity) * Fraction(plan.base_units_per_order_unit)
            )
    for action in prior_actions.values():
        if action["phase"] != "RESOLVED":
            raise ContractValidationError("a previous compensation action remains unresolved")
        auth = action["authorization"]
        leg_id = auth["source_leg_id"]
        plan = next((item for item in request.legs if item.leg_id == leg_id), None)
        if plan is None:
            raise ContractValidationError("compensation action references an unknown source leg")
        close_sign = 1 if auth["closing_side"] == "BUY" else -1
        exposure[leg_id] += (
            close_sign
            * _exact_event_fraction(auth["close_quantity"])
            * Fraction(plan.base_units_per_order_unit)
        )
    return exposure


def _valid_compensation_status_transition(previous: str, current: str) -> bool:
    allowed = {
        "PENDING_ADMISSION": {"PENDING_DISPATCH", "REJECTED", "BLOCKED"},
        "PENDING_DISPATCH": {"DISPATCHING", "CANCELLED", "BLOCKED"},
        "DISPATCHING": {"ACKED", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED", "UNKNOWN"},
        "ACKED": {"PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED", "UNKNOWN"},
        "PARTIALLY_FILLED": {"PARTIALLY_FILLED", "FILLED", "CANCELLED", "UNKNOWN"},
        "UNKNOWN": {"ACKED", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED", "BLOCKED"},
    }
    return current in allowed.get(previous, set())


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
