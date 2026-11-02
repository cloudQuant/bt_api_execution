"""Durable local fill attribution for an explicitly supplied execution journal.

Local order references and projected fills are not provider trade evidence.
Existing table names and fill identities remain stable during migration.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Tuple

from .durable_capacity import canonical_sql_row_size, track_durable_row_change
from .journal_catalog import is_execution_journal_catalog_object
from .journal_ports import (
    JournalPositionCapabilityUnavailable,
    JournalPositionCapacityRow,
    JournalPositionGridIdentity,
)


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
FACTOR_POSITION_BOOK_SCHEMA_VERSION = 1
_POSITION_BOOK_SCHEMA_VERSION = FACTOR_POSITION_BOOK_SCHEMA_VERSION
_MAX_DECIMAL_DIGITS = 128


class FactorPositionBookError(RuntimeError):
    """A local position attribution operation failed closed."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class FactorLocalExecutionFill:
    """One projected local fill, keyed by its local order reference and index."""

    account_scope: str
    strategy_scope: str
    logical_run_id: str
    cohort_id: str
    instrument: Any
    quantity_unit: str
    order_ref: int
    fill_index: int
    side: str
    position_effect: str
    owner_intent_id: Optional[str]
    quantity: Decimal
    price: Decimal
    fee: Optional[Decimal] = None
    filled_at_utc: Optional[datetime] = None
    entry_grid_ordinal: Optional[int] = None

    def __post_init__(self) -> None:
        for name in ("account_scope", "strategy_scope", "logical_run_id", "cohort_id", "quantity_unit"):
            _require_identifier(getattr(self, name), "INVALID_LOCAL_FILL_" + name.upper())
        try:
            from bt_api_base.market_protocol import MarketInstrumentKey
        except Exception as error:
            raise FactorPositionBookError("NEUTRAL_INSTRUMENT_SCHEMA_UNAVAILABLE") from error
        if type(self.instrument) is not MarketInstrumentKey:
            raise FactorPositionBookError("LOCAL_FILL_INSTRUMENT_INVALID")
        if (
            type(self.order_ref) is not int
            or self.order_ref <= 0
            or type(self.fill_index) is not int
            or self.fill_index < 0
        ):
            raise FactorPositionBookError("LOCAL_FILL_ID_INVALID")
        if self.side not in ("buy", "sell"):
            raise FactorPositionBookError("LOCAL_FILL_SIDE_INVALID")
        if self.position_effect not in ("OPEN", "CLOSE"):
            raise FactorPositionBookError("LOCAL_FILL_EFFECT_INVALID")
        if self.position_effect == "OPEN":
            _require_identifier(self.owner_intent_id, "LOCAL_FILL_OWNER_REQUIRED")
        else:
            _require_identifier(self.owner_intent_id, "LOCAL_CLOSE_OWNER_UNAVAILABLE")
        if type(self.quantity) is not Decimal or not self.quantity.is_finite() or self.quantity <= 0:
            raise FactorPositionBookError("LOCAL_FILL_QUANTITY_INVALID")
        if type(self.price) is not Decimal or not self.price.is_finite() or self.price <= 0:
            raise FactorPositionBookError("LOCAL_FILL_PRICE_INVALID")
        if self.fee is not None and (
            type(self.fee) is not Decimal or not self.fee.is_finite() or self.fee < 0
        ):
            raise FactorPositionBookError("LOCAL_FILL_FEE_INVALID")
        for value in (self.quantity, self.price, self.fee):
            if value is not None and len(value.as_tuple().digits) > _MAX_DECIMAL_DIGITS:
                raise FactorPositionBookError("LOCAL_FILL_DECIMAL_LIMIT")
        if self.filled_at_utc is not None and not _aware_utc(self.filled_at_utc):
            raise FactorPositionBookError("LOCAL_FILL_TIME_INVALID")
        if self.entry_grid_ordinal is not None and (
            type(self.entry_grid_ordinal) is not int or self.entry_grid_ordinal < 1
        ):
            raise FactorPositionBookError("LOCAL_FILL_GRID_INVALID")


@dataclass(frozen=True)
class FactorPositionApplyResult:
    """Disposition of one locally projected execution fill."""

    status: str
    fill_id: str
    intent_id: Optional[str] = None
    blocker_code: Optional[str] = None




def ensure_factor_position_book_schema(cursor: sqlite3.Cursor) -> None:
    """Create the local append-only fill and grid tables."""

    if type(cursor) is not sqlite3.Cursor:
        raise FactorPositionBookError("JOURNAL_CURSOR_REQUIRED")
    connection = cursor.connection
    owns_transaction = not connection.in_transaction
    try:
        if owns_transaction:
            cursor.execute("BEGIN IMMEDIATE")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS factor_position_book_meta (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                version INTEGER NOT NULL
            )"""
        )
        meta = cursor.execute(
            "SELECT version FROM factor_position_book_meta WHERE singleton=1"
        ).fetchone()
        if meta is not None and int(meta[0]) > _POSITION_BOOK_SCHEMA_VERSION:
            raise FactorPositionBookError("POSITION_BOOK_SCHEMA_NEWER")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS factor_local_position_grids (
                account_scope TEXT NOT NULL,
                strategy_scope TEXT NOT NULL,
                cohort_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                close_time TEXT NOT NULL,
                ordinal INTEGER NOT NULL CHECK(ordinal > 0),
                capacity_class TEXT NOT NULL,
                PRIMARY KEY(account_scope,strategy_scope,cohort_id,ordinal),
                UNIQUE(account_scope,strategy_scope,cohort_id,batch_id)
            )"""
        )
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS factor_local_execution_fills (
                account_scope TEXT NOT NULL,
                strategy_scope TEXT NOT NULL,
                logical_run_id TEXT NOT NULL,
                cohort_id TEXT NOT NULL,
                instrument_json TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                instrument_venue TEXT NOT NULL,
                quantity_unit TEXT NOT NULL,
                order_ref INTEGER NOT NULL CHECK(order_ref > 0),
                fill_index INTEGER NOT NULL CHECK(fill_index >= 0),
                fill_id TEXT NOT NULL,
                side TEXT NOT NULL CHECK(side IN ('buy','sell')),
                position_effect TEXT NOT NULL CHECK(position_effect IN ('OPEN','CLOSE')),
                owner_intent_id TEXT,
                quantity TEXT NOT NULL,
                price TEXT NOT NULL,
                fee TEXT,
                filled_at_utc TEXT,
                entry_grid_ordinal INTEGER,
                payload_sha256 TEXT NOT NULL,
                PRIMARY KEY(account_scope,logical_run_id,order_ref,fill_index)
            )"""
        )
        cursor.execute(
            """CREATE INDEX IF NOT EXISTS factor_local_fills_by_position
               ON factor_local_execution_fills
                 (account_scope,logical_run_id,instrument_id,instrument_venue,strategy_scope)"""
        )
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS factor_local_flat_checkpoints (
                intent_id TEXT NOT NULL,
                reconciliation_id TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL,
                PRIMARY KEY(intent_id,reconciliation_id)
            )"""
        )
        if meta is None:
            cursor.execute(
                "INSERT INTO factor_position_book_meta(singleton,version) VALUES(1,?)",
                (_POSITION_BOOK_SCHEMA_VERSION,),
            )
        elif int(meta[0]) < _POSITION_BOOK_SCHEMA_VERSION:
            cursor.execute(
                "UPDATE factor_position_book_meta SET version=? WHERE singleton=1",
                (_POSITION_BOOK_SCHEMA_VERSION,),
            )
        if owns_transaction:
            connection.commit()
    except Exception:
        if owns_transaction and connection.in_transaction:
            connection.rollback()
        raise


def record_factor_position_grid(
    cursor: sqlite3.Cursor,
    *,
    account_scope: str,
    strategy_scope: str,
    cohort_id: str,
    batch_id: str,
    close_time: str,
    ordinal: int,
    capacity_class: str = "BASE",
) -> bool:
    """Persist one local strategy grid cursor without provider timestamps."""

    ensure_factor_position_book_schema(cursor)
    for name, value in (
        ("account_scope", account_scope),
        ("strategy_scope", strategy_scope),
        ("cohort_id", cohort_id),
        ("batch_id", batch_id),
        ("capacity_class", capacity_class),
    ):
        _require_identifier(value, "INVALID_GRID_" + name.upper())
    if (
        type(close_time) is not str
        or not close_time
        or type(ordinal) is not int
        or ordinal < 1
    ):
        raise FactorPositionBookError("INVALID_GRID_BINDING")
    key = (account_scope, strategy_scope, cohort_id, ordinal)
    existing = cursor.execute(
        """SELECT batch_id,close_time,capacity_class FROM factor_local_position_grids
           WHERE account_scope=? AND strategy_scope=? AND cohort_id=? AND ordinal=?""",
        key,
    ).fetchone()
    expected = (batch_id, close_time, capacity_class)
    if existing is not None:
        if tuple(existing) != expected:
            raise FactorPositionBookError("POSITION_GRID_CONFLICT")
        return False
    with track_durable_row_change(
        cursor,
        "factor_local_position_grids",
        key,
        capacity_class,
    ):
        cursor.execute(
            """INSERT INTO factor_local_position_grids
               (account_scope,strategy_scope,cohort_id,batch_id,close_time,ordinal,capacity_class)
               VALUES(?,?,?,?,?,?,?)""",
            key[:3] + (batch_id, close_time, ordinal, capacity_class),
        )
    return True


def record_factor_position_flat_checkpoint(
    cursor: sqlite3.Cursor, *, intent_id: str, reconciliation_id: str
) -> None:
    """Record an opaque journal reference as an audit anchor only.

    This row does not prove provider-flatness or trade reconciliation.
    """

    ensure_factor_position_book_schema(cursor)
    _require_identifier(intent_id, "INVALID_FLAT_INTENT")
    _require_identifier(reconciliation_id, "INVALID_FLAT_RECONCILIATION")
    now_ns = time.time_ns()
    key = (intent_id, reconciliation_id)
    existing = cursor.execute(
        """SELECT created_at_ns FROM factor_local_flat_checkpoints
           WHERE intent_id=? AND reconciliation_id=?""",
        key,
    ).fetchone()
    if existing is not None:
        return
    with track_durable_row_change(
        cursor,
        "factor_local_flat_checkpoints",
        key,
        "SETTLEMENT",
    ):
        cursor.execute(
            """INSERT INTO factor_local_flat_checkpoints
               (intent_id,reconciliation_id,created_at_ns) VALUES(?,?,?)""",
            key + (now_ns,),
        )


def apply_factor_execution_fill(
    cursor: sqlite3.Cursor, *, fill: FactorLocalExecutionFill
) -> FactorPositionApplyResult:
    """Append one projected local fill; provider callback rows are not accepted."""

    ensure_factor_position_book_schema(cursor)
    if type(fill) is not FactorLocalExecutionFill:
        raise FactorPositionBookError("LOCAL_FILL_SCHEMA_REQUIRED")
    instrument = fill.instrument.to_dict()
    instrument_json = _canonical_json(instrument)
    payload = {
        "account_scope": fill.account_scope,
        "strategy_scope": fill.strategy_scope,
        "logical_run_id": fill.logical_run_id,
        "cohort_id": fill.cohort_id,
        "instrument": instrument,
        "quantity_unit": fill.quantity_unit,
        "order_ref": fill.order_ref,
        "fill_index": fill.fill_index,
        "side": fill.side,
        "position_effect": fill.position_effect,
        "owner_intent_id": fill.owner_intent_id,
        "quantity": _decimal_text(fill.quantity),
        "price": _decimal_text(fill.price),
        "fee": None if fill.fee is None else _decimal_text(fill.fee),
        "filled_at_utc": (
            None if fill.filled_at_utc is None else _utc_text(fill.filled_at_utc)
        ),
        "entry_grid_ordinal": fill.entry_grid_ordinal,
    }
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    fill_id = "bt-local-fill:" + hashlib.sha256(
        "{}:{}:{}".format(fill.logical_run_id, fill.order_ref, fill.fill_index).encode("utf-8")
    ).hexdigest()
    key = (fill.account_scope, fill.logical_run_id, fill.order_ref, fill.fill_index)
    existing = cursor.execute(
        """SELECT payload_sha256,fill_id FROM factor_local_execution_fills
           WHERE account_scope=? AND logical_run_id=? AND order_ref=? AND fill_index=?""",
        key,
    ).fetchone()
    if existing is not None:
        if existing["payload_sha256"] != digest or existing["fill_id"] != fill_id:
            raise FactorPositionBookError("LOCAL_FILL_ID_CONFLICT")
        return FactorPositionApplyResult(
            "DUPLICATE", fill_id, intent_id=fill.owner_intent_id
        )

    position = _position_from_cursor(
        cursor,
        account_scope=fill.account_scope,
        strategy_scope=fill.strategy_scope,
        logical_run_id=fill.logical_run_id,
        instrument_id=fill.instrument.instrument_id,
        instrument_venue=fill.instrument.venue,
        quantity_unit=fill.quantity_unit,
    )
    quantity_before = position["quantity"]
    current_owner = position["owner_intent_id"]
    if fill.position_effect == "OPEN":
        if fill.owner_intent_id is None:
            raise FactorPositionBookError("LOCAL_FILL_OWNER_REQUIRED")
        if quantity_before != 0 and current_owner != fill.owner_intent_id:
            raise FactorPositionBookError("LOCAL_POSITION_ALREADY_OWNED")
        if quantity_before != 0 and (
            (quantity_before > 0 and fill.side != "buy")
            or (quantity_before < 0 and fill.side != "sell")
        ):
            raise FactorPositionBookError("LOCAL_FILL_SIDE_CONFLICT")
    else:
        if (
            current_owner is None
            or fill.owner_intent_id != current_owner
            or quantity_before == 0
            or (quantity_before > 0 and fill.side != "sell")
            or (quantity_before < 0 and fill.side != "buy")
        ):
            raise FactorPositionBookError("LOCAL_CLOSE_OWNER_UNAVAILABLE")
        if fill.quantity > abs(quantity_before):
            raise FactorPositionBookError("LOCAL_CLOSE_EXCEEDS_OWNED_QUANTITY")

    sign = Decimal(1) if fill.side == "buy" else Decimal(-1)
    signed_delta = sign * fill.quantity
    if fill.position_effect == "CLOSE":
        signed_delta = signed_delta
    else:
        if quantity_before != 0 and (quantity_before + signed_delta) * quantity_before <= 0:
            raise FactorPositionBookError("LOCAL_OPEN_CROSSES_POSITION")
    if fill.position_effect == "CLOSE" and quantity_before + signed_delta != 0:
        if (quantity_before + signed_delta) * quantity_before <= 0:
            raise FactorPositionBookError("LOCAL_CLOSE_CROSSES_POSITION")

    values = (
        fill.account_scope,
        fill.strategy_scope,
        fill.logical_run_id,
        fill.cohort_id,
        instrument_json,
        fill.instrument.instrument_id,
        fill.instrument.venue,
        fill.quantity_unit,
        fill.order_ref,
        fill.fill_index,
        fill_id,
        fill.side,
        fill.position_effect,
        fill.owner_intent_id,
        _decimal_text(fill.quantity),
        _decimal_text(fill.price),
        None if fill.fee is None else _decimal_text(fill.fee),
        None if fill.filled_at_utc is None else _utc_text(fill.filled_at_utc),
        fill.entry_grid_ordinal,
        digest,
    )
    with track_durable_row_change(
        cursor,
        "factor_local_execution_fills",
        key,
        "SETTLEMENT",
    ):
        cursor.execute(
            """INSERT INTO factor_local_execution_fills
               (account_scope,strategy_scope,logical_run_id,cohort_id,instrument_json,
                instrument_id,instrument_venue,quantity_unit,order_ref,fill_index,fill_id,
                side,position_effect,owner_intent_id,quantity,price,fee,filled_at_utc,
                entry_grid_ordinal,payload_sha256)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            values,
        )
    return FactorPositionApplyResult("APPLIED", fill_id, intent_id=fill.owner_intent_id)


def validate_factor_position_close_owner(
    cursor: sqlite3.Cursor,
    *,
    account_scope: str,
    strategy_scope: str,
    logical_run_id: str,
    instrument: Any,
    quantity_unit: str,
    owner_intent_id: str,
) -> None:
    """Require a positive local residual before preparing a close."""

    ensure_factor_position_book_schema(cursor)
    try:
        from bt_api_base.market_protocol import MarketInstrumentKey
    except Exception as error:
        raise FactorPositionBookError("NEUTRAL_INSTRUMENT_SCHEMA_UNAVAILABLE") from error
    if type(instrument) is not MarketInstrumentKey:
        raise FactorPositionBookError("LOCAL_POSITION_INSTRUMENT_INVALID")
    for name, value in (
        ("account_scope", account_scope),
        ("strategy_scope", strategy_scope),
        ("logical_run_id", logical_run_id),
        ("quantity_unit", quantity_unit),
        ("owner_intent_id", owner_intent_id),
    ):
        _require_identifier(value, "INVALID_" + name.upper())
    position = _position_from_cursor(
        cursor,
        account_scope=account_scope,
        strategy_scope=strategy_scope,
        logical_run_id=logical_run_id,
        instrument_id=instrument.instrument_id,
        instrument_venue=instrument.venue,
        quantity_unit=quantity_unit,
    )
    if position["quantity"] == 0 or position["owner_intent_id"] != owner_intent_id:
        raise FactorPositionBookError("POSITION_OWNERSHIP_UNAVAILABLE")






def _position_from_cursor(
    cursor: sqlite3.Cursor,
    *,
    account_scope: str,
    strategy_scope: str,
    logical_run_id: str,
    instrument_id: str,
    instrument_venue: str,
    quantity_unit: str,
):
    rows = cursor.execute(
        """SELECT strategy_scope,position_effect,side,owner_intent_id,quantity,price,fee
           FROM factor_local_execution_fills
           WHERE account_scope=? AND logical_run_id=? AND instrument_id=?
             AND instrument_venue=? AND quantity_unit=?
            ORDER BY rowid""",
        (account_scope, logical_run_id, instrument_id, instrument_venue, quantity_unit),
    ).fetchall()
    positions_by_owner = {}
    for row in rows:
        owner = row["owner_intent_id"]
        if row["position_effect"] == "OPEN" and not owner:
            raise FactorPositionBookError("LOCAL_FILL_OWNER_REQUIRED")
        if row["position_effect"] == "CLOSE" and not owner:
            raise FactorPositionBookError("LOCAL_CLOSE_OWNER_UNAVAILABLE")
        amount = _decimal_or_none(row["quantity"])
        price = _decimal_or_none(row["price"])
        if amount is None or price is None or amount <= 0 or price <= 0:
            raise FactorPositionBookError("LOCAL_FILL_ROW_INVALID")
        side_sign = Decimal(1) if row["side"] == "buy" else Decimal(-1)
        signed = side_sign * amount
        previous = positions_by_owner.get(
            owner, {"quantity": Decimal(0), "average_price": None}
        )
        quantity = previous["quantity"]
        average_price = previous["average_price"]
        if row["position_effect"] == "OPEN":
            if quantity != 0 and (quantity > 0) != (signed > 0):
                raise FactorPositionBookError("LOCAL_OPEN_CROSSES_POSITION")
            old_amount = abs(quantity)
            combined_amount = old_amount + amount
            if average_price is None:
                average_price = price
            else:
                average_price = (
                    old_amount * average_price + amount * price
                ) / combined_amount
            quantity += signed
        else:
            if (
                quantity == 0
                or (quantity > 0 and row["side"] != "sell")
                or (quantity < 0 and row["side"] != "buy")
                or amount > abs(quantity)
            ):
                raise FactorPositionBookError("LOCAL_CLOSE_OWNER_UNAVAILABLE")
            quantity += signed
            if quantity == 0:
                average_price = None
        positions_by_owner[owner] = {
            "quantity": quantity,
            "average_price": average_price,
        }
    active = {
        owner: position
        for owner, position in positions_by_owner.items()
        if position["quantity"] != 0
    }
    if len(active) > 1:
        raise FactorPositionBookError("LOCAL_POSITION_OWNER_AMBIGUOUS")
    owner = next(iter(active), None)
    quantity = Decimal(0) if owner is None else active[owner]["quantity"]
    average = None if owner is None else active[owner]["average_price"]
    if owner is not None and any(
        row["strategy_scope"] != strategy_scope
        for row in rows
        if row["owner_intent_id"] == owner
    ):
        raise FactorPositionBookError("LOCAL_POSITION_STRATEGY_MISMATCH")
    return {
        "quantity": quantity,
        "owner_intent_id": owner,
        "average_price": average,
    }


def _require_identifier(value: Any, code: str) -> None:
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise FactorPositionBookError(code)


def _decimal_or_none(value: Any) -> Optional[Decimal]:
    if value is None or type(value) is bool:
        return None
    try:
        result = value if type(value) is Decimal else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise FactorPositionBookError("LOCAL_FILL_PAYLOAD_INVALID") from error


def _aware_utc(value: Any) -> bool:
    return (
        type(value) is datetime
        and value.tzinfo is not None
        and value.utcoffset() == timedelta(0)
    )


def _utc_text(value: datetime) -> str:
    if not _aware_utc(value):
        raise FactorPositionBookError("POSITION_SNAPSHOT_TIME_INVALID")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")



_POSITION_BOOK_COLUMNS = {
    "factor_position_book_meta": (
        ("singleton", "INTEGER", 0, 1),
        ("version", "INTEGER", 1, 0),
    ),
    "factor_local_position_grids": (
        ("account_scope", "TEXT", 1, 1),
        ("strategy_scope", "TEXT", 1, 2),
        ("cohort_id", "TEXT", 1, 3),
        ("batch_id", "TEXT", 1, 0),
        ("close_time", "TEXT", 1, 0),
        ("ordinal", "INTEGER", 1, 4),
        ("capacity_class", "TEXT", 1, 0),
    ),
    "factor_local_execution_fills": (
        ("account_scope", "TEXT", 1, 1),
        ("strategy_scope", "TEXT", 1, 0),
        ("logical_run_id", "TEXT", 1, 2),
        ("cohort_id", "TEXT", 1, 0),
        ("instrument_json", "TEXT", 1, 0),
        ("instrument_id", "TEXT", 1, 0),
        ("instrument_venue", "TEXT", 1, 0),
        ("quantity_unit", "TEXT", 1, 0),
        ("order_ref", "INTEGER", 1, 3),
        ("fill_index", "INTEGER", 1, 4),
        ("fill_id", "TEXT", 1, 0),
        ("side", "TEXT", 1, 0),
        ("position_effect", "TEXT", 1, 0),
        ("owner_intent_id", "TEXT", 0, 0),
        ("quantity", "TEXT", 1, 0),
        ("price", "TEXT", 1, 0),
        ("fee", "TEXT", 0, 0),
        ("filled_at_utc", "TEXT", 0, 0),
        ("entry_grid_ordinal", "INTEGER", 0, 0),
        ("payload_sha256", "TEXT", 1, 0),
    ),
    "factor_local_flat_checkpoints": (
        ("intent_id", "TEXT", 1, 1),
        ("reconciliation_id", "TEXT", 1, 2),
        ("created_at_ns", "INTEGER", 1, 0),
    ),
}

_POSITION_BOOK_INDEXES = {
    "factor_local_fills_by_position": (
        "factor_local_execution_fills",
        ("account_scope", "logical_run_id", "instrument_id", "instrument_venue", "strategy_scope"),
    ),
}

_POSITION_BOOK_EXPECTED_OBJECTS = frozenset(
    {("table", name) for name in _POSITION_BOOK_COLUMNS}
    | {("index", name) for name in _POSITION_BOOK_INDEXES}
)

_POSITION_BOOK_UNIQUE_KEYS = {
    "factor_position_book_meta": frozenset(),
    "factor_local_position_grids": frozenset(
        {
            ("account_scope", "strategy_scope", "cohort_id", "ordinal"),
            ("account_scope", "strategy_scope", "cohort_id", "batch_id"),
        }
    ),
    "factor_local_execution_fills": frozenset(
        {("account_scope", "logical_run_id", "order_ref", "fill_index")}
    ),
    "factor_local_flat_checkpoints": frozenset(
        {("intent_id", "reconciliation_id")}
    ),
}

_POSITION_BOOK_REQUIRED_SQL = {
    "factor_position_book_meta": ("check(singleton=1)",),
    "factor_local_position_grids": (
        "check(ordinal>0)",
        "unique(account_scope,strategy_scope,cohort_id,batch_id)",
    ),
    "factor_local_execution_fills": (
        "check(order_ref>0)",
        "check(fill_index>=0)",
        "check(sidein('buy','sell'))",
        "check(position_effectin('open','close'))",
    ),
    "factor_local_flat_checkpoints": (),
}

def _sqlite_user_objects(
    cursor: sqlite3.Cursor,
) -> Optional[frozenset[Tuple[str, str]]]:
    """Return catalog objects not recognized by the SDK or SQLite itself.

    Object names are not sufficient to identify the SDK schema. Use the SDK's
    exact type/name classifier so an unrecognized legacy object such as an
    unknown ``execution_*`` table remains visible to this owner preflight.
    """

    try:
        rows = cursor.execute(
            "SELECT type, name FROM sqlite_master "
            "WHERE type IN ('table','index','view','trigger')"
        ).fetchall()
    except sqlite3.Error:
        return None
    objects = set()
    for row in rows:
        try:
            kind, name = row[0], row[1]
        except (IndexError, TypeError):
            return None
        if type(kind) is not str or type(name) is not str:
            return None
        if name.startswith("sqlite_"):
            continue
        try:
            is_sdk_object = is_execution_journal_catalog_object(kind, name)
        except Exception:
            return None
        if type(is_sdk_object) is bool and is_sdk_object is True:
            continue
        objects.add((kind, name))
    return frozenset(objects)

def _has_exact_position_book_schema(cursor: sqlite3.Cursor, expected_version: int) -> bool:
    """Check the local attribution schema independently of journal catalog objects."""

    if type(cursor) is not sqlite3.Cursor:
        return False
    if _sqlite_user_objects(cursor) != _POSITION_BOOK_EXPECTED_OBJECTS:
        return False
    for table, expected_columns in _POSITION_BOOK_COLUMNS.items():
        try:
            rows = cursor.execute('PRAGMA table_info("{}")'.format(table)).fetchall()
            columns = tuple((row[1], row[2], row[3], row[5]) for row in rows)
        except (sqlite3.Error, IndexError, TypeError):
            return False
        if columns != expected_columns or any(row[4] is not None for row in rows):
            return False
        try:
            definition = cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
        except sqlite3.Error:
            return False
        if (
            definition is None
            or type(definition[0]) is not str
            or any(
                fragment not in "".join(definition[0].lower().split())
                for fragment in _POSITION_BOOK_REQUIRED_SQL[table]
            )
        ):
            return False
        try:
            unique_keys = set()
            for index in cursor.execute('PRAGMA index_list("{}")'.format(table)).fetchall():
                if len(index) < 3:
                    return False
                if index[2] != 1:
                    continue
                if len(index) > 4 and index[4] != 0:
                    return False
                index_name = index[1]
                key_columns = cursor.execute(
                    'PRAGMA index_info("{}")'.format(index_name)
                ).fetchall()
                unique_keys.add(tuple(row[2] for row in key_columns))
        except (sqlite3.Error, IndexError, TypeError):
            return False
        if frozenset(unique_keys) != _POSITION_BOOK_UNIQUE_KEYS[table]:
            return False
    for index_name, (table_name, expected_columns) in _POSITION_BOOK_INDEXES.items():
        try:
            rows = cursor.execute('PRAGMA index_info("{}")'.format(index_name)).fetchall()
            columns = tuple(row[2] for row in rows)
            listing = cursor.execute('PRAGMA index_list("{}")'.format(table_name)).fetchall()
            index_row = next((row for row in listing if row[1] == index_name), None)
            owner = cursor.execute(
                "SELECT tbl_name FROM sqlite_master WHERE type='index' AND name=?",
                (index_name,),
            ).fetchone()
        except (sqlite3.Error, IndexError, TypeError):
            return False
        if (
            columns != expected_columns
            or index_row is None
            or index_row[2] != 0
            or (len(index_row) > 4 and index_row[4] != 0)
            or owner is None
            or owner[0] != table_name
        ):
            return False
    try:
        metadata = cursor.execute(
            "SELECT singleton, version FROM factor_position_book_meta"
        ).fetchall()
    except sqlite3.Error:
        return False
    if len(metadata) != 1:
        return False
    try:
        singleton, version = metadata[0][0], metadata[0][1]
    except (IndexError, TypeError):
        return False
    return (
        type(singleton) is int
        and singleton == 1
        and type(version) is int
        and version == expected_version
    )


FACTOR_POSITION_BOOK_EXPECTED_OBJECTS = _POSITION_BOOK_EXPECTED_OBJECTS
factor_position_user_objects = _sqlite_user_objects
has_exact_factor_position_book_schema = _has_exact_position_book_schema


def factor_position_schema_version() -> int:
    """Return the schema version owned by the execution attribution book."""
    version = FACTOR_POSITION_BOOK_SCHEMA_VERSION
    if type(version) is not int or version <= 0:
        raise FactorPositionBookError("POSITION_BOOK_SCHEMA_VERSION_INVALID")
    return version


def preflight_factor_position_journal_schema(cursor: Any) -> bool:
    """Check an existing journal transaction before schema creation."""
    if type(cursor) is not sqlite3.Cursor or not cursor.connection.in_transaction:
        return False
    objects = _sqlite_user_objects(cursor)
    if objects is None:
        return False
    if not objects:
        return True
    if objects != _POSITION_BOOK_EXPECTED_OBJECTS:
        return False
    try:
        expected_version = factor_position_schema_version()
    except FactorPositionBookError:
        return False
    return _has_exact_position_book_schema(cursor, expected_version)


def ensure_factor_position_journal_schema(cursor: Any) -> None:
    """Create or verify attribution schema on the caller's journal transaction."""
    if type(cursor) is not sqlite3.Cursor or not cursor.connection.in_transaction:
        raise FactorPositionBookError("JOURNAL_POSITION_TRANSACTION_REQUIRED")
    objects = _sqlite_user_objects(cursor)
    if objects is None:
        raise FactorPositionBookError("POSITION_BOOK_SCHEMA_UNAVAILABLE")
    if objects:
        if (
            objects != _POSITION_BOOK_EXPECTED_OBJECTS
            or not _has_exact_position_book_schema(cursor, factor_position_schema_version())
        ):
            raise FactorPositionBookError("POSITION_BOOK_SCHEMA_UNAVAILABLE")
        return
    try:
        ensure_factor_position_book_schema(cursor)
    except Exception:
        raise FactorPositionBookError("POSITION_BOOK_SCHEMA_UNAVAILABLE") from None
    if not _has_exact_position_book_schema(cursor, factor_position_schema_version()):
        raise FactorPositionBookError("POSITION_BOOK_SCHEMA_UNAVAILABLE")


def validate_factor_position_journal_schema(cursor: Any) -> bool:
    """Validate the complete attribution schema against its current version."""
    try:
        expected_version = factor_position_schema_version()
    except FactorPositionBookError:
        return False
    return _has_exact_position_book_schema(cursor, expected_version)


def require_factor_close_reconciliation_inputs(
    *, instrument: Any, exchange: Optional[str], logical_run_id: Optional[str],
    quantity_unit: Optional[str],
) -> None:
    """Reject under-scoped closes until neutral terminal reconciliation exists."""
    try:
        from bt_api_base.market_protocol import MarketInstrumentKey
    except Exception as error:
        raise FactorPositionBookError("NEUTRAL_MARKET_INSTRUMENT_SCHEMA_UNAVAILABLE") from error
    if type(instrument) is not MarketInstrumentKey or exchange is not None:
        raise FactorPositionBookError("NEUTRAL_MARKET_INSTRUMENT_KEY_REQUIRED")
    if (
        type(logical_run_id) is not str
        or not logical_run_id
        or type(quantity_unit) is not str
        or not quantity_unit
    ):
        raise FactorPositionBookError("NEUTRAL_POSITION_SCOPE_AND_UNIT_REQUIRED")
    raise FactorPositionBookError("NEUTRAL_TERMINAL_POSITION_RECONCILIATION_UNAVAILABLE")


def provider_trade_reconciliation_unavailable(*, family: str = "trades") -> None:
    """Reject provider trade reconciliation until its neutral contract exists."""
    if type(family) is not str or not family or family != family.strip():
        raise FactorPositionBookError("PROVIDER_TRADE_SCHEMA_UNAVAILABLE")
    raise FactorPositionBookError("PROVIDER_TRADE_SCHEMA_UNAVAILABLE")


def reject_factor_provider_evidence(cursor: Any, capability: str) -> None:
    """Reject a provider fact that the local attribution book does not own."""
    if type(cursor) is not sqlite3.Cursor:
        raise FactorPositionBookError("JOURNAL_CURSOR_REQUIRED")
    raise JournalPositionCapabilityUnavailable(capability)


_POSITION_CAPACITY_KEYS = {
    "factor_local_position_grids": (
        ("account_scope", "strategy_scope", "cohort_id", "ordinal"),
        (str, str, str, int),
    ),
    "factor_local_execution_fills": (
        ("account_scope", "logical_run_id", "order_ref", "fill_index"),
        (str, str, int, int),
    ),
    "factor_local_flat_checkpoints": (
        ("intent_id", "reconciliation_id"),
        (str, str),
    ),
}


def _capacity_row_value(row: Any, key: str) -> Any:
    keys_method = getattr(row, "keys", None)
    if not callable(keys_method):
        raise FactorPositionBookError("POSITION_CAPACITY_ROW_INVALID")
    try:
        if key not in tuple(keys_method()):
            raise FactorPositionBookError("POSITION_CAPACITY_ROW_INVALID")
        return row[key]
    except FactorPositionBookError:
        raise
    except Exception:
        raise FactorPositionBookError("POSITION_CAPACITY_ROW_INVALID") from None


def _capacity_row_size(row: Any) -> int:
    try:
        size = canonical_sql_row_size(row)
    except Exception:
        raise FactorPositionBookError("POSITION_CAPACITY_ROW_INVALID") from None
    if type(size) is not int or size <= 0 or size > 9_223_372_036_854_775_807:
        raise FactorPositionBookError("POSITION_CAPACITY_ROW_INVALID")
    return size


def _require_capacity_schema(cursor: Any) -> None:
    if type(cursor) is not sqlite3.Cursor or not cursor.connection.in_transaction:
        raise FactorPositionBookError("JOURNAL_POSITION_TRANSACTION_REQUIRED")
    if not _has_exact_position_book_schema(cursor, FACTOR_POSITION_BOOK_SCHEMA_VERSION):
        raise FactorPositionBookError("POSITION_BOOK_SCHEMA_UNAVAILABLE")


def iter_factor_position_capacity_rows(cursor: Any):
    """Read capacity projections from the current journal transaction."""
    _require_capacity_schema(cursor)

    def project_rows():
        try:
            for row in cursor.execute(
                "SELECT * FROM factor_local_position_grids "
                "ORDER BY account_scope,strategy_scope,cohort_id,ordinal"
            ):
                identity = JournalPositionGridIdentity(
                    account_scope=_capacity_row_value(row, "account_scope"),
                    strategy_scope=_capacity_row_value(row, "strategy_scope"),
                    cohort_id=_capacity_row_value(row, "cohort_id"),
                    close_time=_capacity_row_value(row, "close_time"),
                )
                yield JournalPositionCapacityRow(
                    capacity_role="GRID_HISTORY",
                    row_size_bytes=_capacity_row_size(row),
                    grid_identity=identity,
                )
            for table, ordering in (
                (
                    "factor_local_execution_fills",
                    "account_scope,logical_run_id,order_ref,fill_index",
                ),
                ("factor_local_flat_checkpoints", "intent_id,reconciliation_id"),
            ):
                for row in cursor.execute("SELECT * FROM {} ORDER BY {}".format(table, ordering)):
                    yield JournalPositionCapacityRow(
                        capacity_role="SETTLEMENT",
                        row_size_bytes=_capacity_row_size(row),
                        grid_identity=None,
                    )
        except FactorPositionBookError:
            raise
        except Exception:
            raise FactorPositionBookError("POSITION_CAPACITY_ROWS_UNAVAILABLE") from None

    return project_rows()


def snapshot_factor_position_capacity_row_size(
    cursor: Any, *, owner_table_ref: str, key_values: tuple[Any, ...]
) -> Optional[int]:
    """Read one known attribution row's canonical durable-capacity size."""
    _require_capacity_schema(cursor)
    spec = _POSITION_CAPACITY_KEYS.get(owner_table_ref) if type(owner_table_ref) is str else None
    if spec is None or type(key_values) is not tuple:
        raise JournalPositionCapabilityUnavailable("capacity_row_size")
    columns, expected_types = spec
    if len(key_values) != len(columns) or any(
        type(value) is not expected_type for value, expected_type in zip(key_values, expected_types)
    ):
        raise JournalPositionCapabilityUnavailable("capacity_row_size")
    if owner_table_ref == "factor_local_position_grids" and key_values[3] < 1:
        raise JournalPositionCapabilityUnavailable("capacity_row_size")
    if owner_table_ref == "factor_local_execution_fills" and (
        key_values[2] <= 0 or key_values[3] < 0
    ):
        raise JournalPositionCapabilityUnavailable("capacity_row_size")
    predicate = " AND ".join("{}=?".format(column) for column in columns)
    try:
        row = cursor.execute(
            "SELECT * FROM {} WHERE {}".format(owner_table_ref, predicate), key_values
        ).fetchone()
    except sqlite3.Error:
        raise FactorPositionBookError("POSITION_CAPACITY_ROW_UNAVAILABLE") from None
    return None if row is None else _capacity_row_size(row)


def read_factor_position_snapshot_inputs(
    cursor: sqlite3.Cursor, *, account_scope: str, strategy_scope: str,
    logical_run_id: str, instrument: Any, quantity_unit: str,
    broker_quantity: Any, snapshot_id: str, available_at: datetime,
):
    """Validate neutral snapshot inputs and read local attribution facts.

    The caller compares its own position against the returned local facts;
    neither input nor result establishes provider account state.
    """
    ensure_factor_position_book_schema(cursor)
    try:
        from bt_api_base.market_protocol import MarketInstrumentKey
    except Exception as error:
        raise FactorPositionBookError("NEUTRAL_INSTRUMENT_SCHEMA_UNAVAILABLE") from error
    if type(instrument) is not MarketInstrumentKey:
        raise FactorPositionBookError("LOCAL_POSITION_INSTRUMENT_INVALID")
    for name, value in (
        ("account_scope", account_scope),
        ("strategy_scope", strategy_scope),
        ("logical_run_id", logical_run_id),
        ("quantity_unit", quantity_unit),
        ("snapshot_id", snapshot_id),
    ):
        _require_identifier(value, "INVALID_" + name.upper())
    if type(available_at) is not datetime or not _aware_utc(available_at):
        raise FactorPositionBookError("POSITION_SNAPSHOT_TIME_INVALID")
    quantity = _decimal_or_none(broker_quantity)
    if quantity is None:
        return None, None, "BROKER_POSITION_VALUE_INVALID"
    try:
        position = _position_from_cursor(
            cursor,
            account_scope=account_scope,
            strategy_scope=strategy_scope,
            logical_run_id=logical_run_id,
            instrument_id=instrument.instrument_id,
            instrument_venue=instrument.venue,
            quantity_unit=quantity_unit,
        )
    except FactorPositionBookError as error:
        return quantity, None, error.code
    return quantity, position, None


# Public neutral helpers for local attribution projection.
read_factor_local_position = _position_from_cursor
factor_decimal_or_none = _decimal_or_none
factor_aware_utc = _aware_utc
require_factor_identifier = _require_identifier

@dataclass(frozen=True)
class FactorPositionAttributionSnapshot:
    """Local fill attribution compared with a caller-supplied position quantity."""

    status: str
    blocker_code: Optional[str]
    account_scope: str
    strategy_scope: str
    logical_run_id: str
    instrument: Any
    snapshot_id: str
    available_at: datetime
    broker_quantity: Optional[Decimal]
    attributed_quantity: Optional[Decimal]
    quantity_unit: Optional[str]
    owner_intent_id: Optional[str]
    average_price: Optional[Decimal]

class FactorPositionBook:
    """Cursor-based local fill attribution and caller-position comparison."""

    def apply_fill(self, cursor: sqlite3.Cursor, fill: FactorLocalExecutionFill):
        return apply_factor_execution_fill(cursor, fill=fill)

    def snapshot(
        self,
        *,
        cursor: sqlite3.Cursor,
        account_scope: str,
        strategy_scope: str,
        logical_run_id: str,
        instrument: Any,
        quantity_unit: str,
        broker_quantity: Any,
        snapshot_id: str,
        available_at: datetime,
    ) -> FactorPositionAttributionSnapshot:
        broker, position, blocker = read_factor_position_snapshot_inputs(
            cursor,
            account_scope=account_scope,
            strategy_scope=strategy_scope,
            logical_run_id=logical_run_id,
            instrument=instrument,
            quantity_unit=quantity_unit,
            broker_quantity=broker_quantity,
            snapshot_id=snapshot_id,
            available_at=available_at,
        )
        base = dict(
            account_scope=account_scope,
            strategy_scope=strategy_scope,
            logical_run_id=logical_run_id,
            instrument=instrument,
            snapshot_id=snapshot_id,
            available_at=available_at,
            broker_quantity=broker,
        )
        if blocker is not None:
            return FactorPositionAttributionSnapshot(
                status="UNKNOWN",
                blocker_code=blocker,
                attributed_quantity=None,
                quantity_unit=quantity_unit,
                owner_intent_id=None,
                average_price=None,
                **base,
            )
        attributed = position["quantity"]
        if attributed != broker:
            return FactorPositionAttributionSnapshot(
                status="BROKER_MISMATCH",
                blocker_code="LOCAL_POSITION_BOOK_BROKER_MISMATCH",
                attributed_quantity=attributed,
                quantity_unit=quantity_unit,
                owner_intent_id=position["owner_intent_id"],
                average_price=position["average_price"],
                **base,
            )
        status = "FLAT" if attributed == 0 else "MATCHED"
        return FactorPositionAttributionSnapshot(
            status=status,
            blocker_code=None,
            attributed_quantity=attributed,
            quantity_unit=quantity_unit,
            owner_intent_id=position["owner_intent_id"],
            average_price=position["average_price"],
            **base,
        )

    def reconcile_provider_trades(self, *args: Any, **kwargs: Any) -> None:
        provider_trade_reconciliation_unavailable()


__all__ = (
    "FactorPositionAttributionSnapshot", "FactorPositionBook",
    "FactorLocalExecutionFill", "FactorPositionApplyResult", "FactorPositionBookError",
    "apply_factor_execution_fill", "ensure_factor_position_book_schema",
    "record_factor_position_flat_checkpoint", "record_factor_position_grid",
    "validate_factor_position_close_owner", "read_factor_local_position",
    "factor_decimal_or_none", "factor_aware_utc", "require_factor_identifier",
    "read_factor_position_snapshot_inputs",
    "FACTOR_POSITION_BOOK_SCHEMA_VERSION",
    "FACTOR_POSITION_BOOK_EXPECTED_OBJECTS",
    "factor_position_user_objects",
    "has_exact_factor_position_book_schema",
    "factor_position_schema_version",
    "preflight_factor_position_journal_schema",
    "ensure_factor_position_journal_schema",
    "validate_factor_position_journal_schema",
    "require_factor_close_reconciliation_inputs",
    "provider_trade_reconciliation_unavailable",
    "reject_factor_provider_evidence",
    "iter_factor_position_capacity_rows",
    "snapshot_factor_position_capacity_row_size",
)
