"""Read a bounded, scope-isolated projection of the execution journals.

This module deliberately does not construct :class:`SqliteExecutionStore`:
that class owns schema migration and writer behavior.  The reader opens an
existing SQLite database through a read-only URI and never selects event
payloads.
"""

from __future__ import annotations

import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .contracts import ExecutionScope, ExecutionState

JournalStream = Literal["execution", "cancellation"]
SourceIdentity = tuple[int, int]

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_EVENT_ID = re.compile(r"^[0-9a-f]{32}$")
_GENERATION = re.compile(r"^[0-9a-f]{32}$")
_MAX_PAGE_LIMIT = 1_000
_MAX_PAGE_FIELD_BYTES = 2 * 1024 * 1024


class JournalReaderError(RuntimeError):
    """A journal read failed with a finite, non-sensitive reason code."""

    _CODES = frozenset(
        {
            "invalid_scope",
            "invalid_database_path",
            "database_missing",
            "database_unavailable",
            "journal_unavailable",
            "malformed_journal",
            "malformed_event",
            "invalid_stream",
            "invalid_cursor",
            "invalid_limit",
            "reader_closed",
            "source_changed",
            "generation_changed",
            "page_too_large",
        }
    )

    def __init__(self, code: str) -> None:
        if code not in self._CODES:
            raise ValueError("unsupported journal reader error code")
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class JournalEvent:
    """One event's allow-listed metadata; payload data is never included."""

    sequence: int
    stream: JournalStream
    event_id: str
    intent_id: str | None
    cancel_id: str | None
    target_intent_id: str | None
    scope_key: str
    event_type: str
    state: str
    created_at_ns: int
    journal_generation: str


@dataclass(frozen=True, slots=True)
class JournalPage:
    """A page from one stream and one journal incarnation."""

    generation: str
    events: tuple[JournalEvent, ...]


_EXECUTION_PAGE_SQL = """
SELECT
    sequence,
    CASE WHEN typeof(event_id) = 'text' AND length(event_id) = 32
         THEN substr(event_id, 1, 32) END AS event_id,
    CASE WHEN typeof(intent_id) = 'text' AND length(intent_id) BETWEEN 1 AND 128
         THEN substr(intent_id, 1, 128) END AS intent_id,
    scope_key,
    CASE WHEN typeof(event_type) = 'text' AND length(event_type) BETWEEN 1 AND 128
         THEN substr(event_type, 1, 128) END AS event_type,
    CASE WHEN typeof(state) = 'text' AND length(state) BETWEEN 1 AND 32
         THEN substr(state, 1, 32) END AS state,
    CASE WHEN typeof(created_at_ns) = 'integer' THEN created_at_ns END AS created_at_ns,
    CASE WHEN typeof(journal_incarnation_id) = 'text'
                   AND length(journal_incarnation_id) = 32
         THEN substr(journal_incarnation_id, 1, 32) END AS journal_incarnation_id
FROM execution_outbox
WHERE sequence > ? AND scope_key = ?
ORDER BY sequence ASC
LIMIT ?
"""

_CANCELLATION_PAGE_SQL = """
SELECT
    sequence,
    CASE WHEN typeof(event_id) = 'text' AND length(event_id) = 32
         THEN substr(event_id, 1, 32) END AS event_id,
    CASE WHEN typeof(cancel_id) = 'text' AND length(cancel_id) BETWEEN 1 AND 128
         THEN substr(cancel_id, 1, 128) END AS cancel_id,
    CASE WHEN typeof(target_intent_id) = 'text'
                   AND length(target_intent_id) BETWEEN 1 AND 128
         THEN substr(target_intent_id, 1, 128) END AS target_intent_id,
    scope_key,
    CASE WHEN typeof(event_type) = 'text' AND length(event_type) BETWEEN 1 AND 128
         THEN substr(event_type, 1, 128) END AS event_type,
    CASE WHEN typeof(state) = 'text' AND length(state) BETWEEN 1 AND 32
         THEN substr(state, 1, 32) END AS state,
    CASE WHEN typeof(created_at_ns) = 'integer' THEN created_at_ns END AS created_at_ns
FROM cancellation_outbox
WHERE sequence > ? AND scope_key = ?
ORDER BY sequence ASC
LIMIT ?
"""


class ExecutionJournalReader:
    """Read execution and cancellation journal pages for exactly one scope.

    ``source_identity`` is the local ``(st_dev, st_ino)`` pair.  It can be
    compared with another local file identity (including a hard link); it is
    not a relay or wire identifier.  ``generation`` is the journal
    incarnation stored in ``execution_meta`` and must match on every page.
    """

    def __init__(self, database_path: str | os.PathLike[str], *, scope: ExecutionScope) -> None:
        if not isinstance(scope, ExecutionScope):
            raise JournalReaderError("invalid_scope")
        try:
            raw_path = os.fspath(database_path)
        except (TypeError, ValueError) as error:
            raise JournalReaderError("invalid_database_path") from error
        if type(raw_path) is not str or not raw_path:
            raise JournalReaderError("invalid_database_path")

        try:
            resolved_path = Path(raw_path).resolve(strict=True)
        except FileNotFoundError as error:
            raise JournalReaderError("database_missing") from error
        except (OSError, RuntimeError, ValueError) as error:
            raise JournalReaderError("invalid_database_path") from error
        try:
            initial_stat = resolved_path.stat()
        except FileNotFoundError as error:
            raise JournalReaderError("database_missing") from error
        except OSError as error:
            raise JournalReaderError("database_unavailable") from error
        if not resolved_path.is_file():
            raise JournalReaderError("database_unavailable")

        self._database_path = resolved_path
        self._scope = scope
        self._scope_key = scope.key
        self._source_identity = self._identity_from_stat(initial_stat)
        self._closed = False
        self._uri = f"{resolved_path.as_uri()}?mode=ro"
        self._generation = self._load_initial_generation()

    @property
    def database_path(self) -> Path:
        """Return the resolved path of the existing database file."""

        return self._database_path

    @property
    def scope(self) -> ExecutionScope:
        """Return the immutable scope this reader is restricted to."""

        return self._scope

    @property
    def source_identity(self) -> SourceIdentity:
        """Return the local device/inode identity captured at construction."""

        return self._source_identity

    @property
    def generation(self) -> str:
        """Return the journal generation captured at construction."""

        return self._generation

    def close(self) -> None:
        """Close the reader logically; each individual SQLite connection is short-lived."""

        self._closed = True

    def read_page(
        self,
        stream: JournalStream,
        after_sequence: int = 0,
        limit: int = 100,
    ) -> JournalPage:
        """Return one bounded page without persisting or advancing a cursor."""

        if self._closed:
            raise JournalReaderError("reader_closed")
        if type(stream) is not str or stream not in {"execution", "cancellation"}:
            raise JournalReaderError("invalid_stream")
        if type(after_sequence) is not int or not 0 <= after_sequence < 2**63:
            raise JournalReaderError("invalid_cursor")
        if type(limit) is not int or not 1 <= limit <= _MAX_PAGE_LIMIT:
            raise JournalReaderError("invalid_limit")

        if self._current_source_identity() != self._source_identity:
            raise JournalReaderError("source_changed")

        try:
            connection = self._connect()
        except sqlite3.Error as error:
            raise JournalReaderError("database_unavailable") from error

        events: tuple[JournalEvent, ...]
        try:
            connection.execute("BEGIN")
            generation = self._read_generation(connection)
            if generation != self._generation:
                raise JournalReaderError("generation_changed")

            sql = _EXECUTION_PAGE_SQL if stream == "execution" else _CANCELLATION_PAGE_SQL
            rows = connection.execute(sql, (after_sequence, self._scope_key, limit)).fetchall()
            events = tuple(self._event_from_row(row, stream, generation) for row in rows)

            if self._read_generation(connection) != self._generation:
                raise JournalReaderError("generation_changed")
            if self._page_field_bytes(events, generation) > _MAX_PAGE_FIELD_BYTES:
                raise JournalReaderError("page_too_large")
            connection.execute("COMMIT")
        except JournalReaderError:
            raise
        except (sqlite3.Error, UnicodeError) as error:
            raise JournalReaderError("journal_unavailable") from error
        finally:
            if connection.in_transaction:
                connection.rollback()
            connection.close()

        if self._current_source_identity() != self._source_identity:
            raise JournalReaderError("source_changed")
        return JournalPage(generation=self._generation, events=events)

    def _load_initial_generation(self) -> str:
        if self._current_source_identity() != self._source_identity:
            raise JournalReaderError("source_changed")
        try:
            connection = self._connect()
        except sqlite3.Error as error:
            raise JournalReaderError("journal_unavailable") from error
        try:
            connection.execute("BEGIN")
            generation = self._read_generation(connection)
            connection.execute("COMMIT")
        except JournalReaderError:
            raise
        except (sqlite3.Error, UnicodeError) as error:
            raise JournalReaderError("journal_unavailable") from error
        finally:
            if connection.in_transaction:
                connection.rollback()
            connection.close()
        if self._current_source_identity() != self._source_identity:
            raise JournalReaderError("source_changed")
        return generation

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._uri, uri=True, isolation_level=None, timeout=5.0)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            query_only = connection.execute("PRAGMA query_only").fetchone()
            if query_only is None or type(query_only[0]) is not int or query_only[0] != 1:
                raise sqlite3.OperationalError("query_only is unavailable")
            return connection
        except BaseException:
            connection.close()
            raise

    @staticmethod
    def _read_generation(connection: sqlite3.Connection) -> str:
        try:
            row = connection.execute(
                """
                SELECT CASE WHEN typeof(value) = 'text' AND length(value) = 32
                            THEN substr(value, 1, 32) END AS generation
                FROM execution_meta
                WHERE key = ?
                """,
                ("journal_incarnation_id",),
            ).fetchone()
        except sqlite3.Error as error:
            raise JournalReaderError("journal_unavailable") from error
        if row is None:
            raise JournalReaderError("journal_unavailable")
        generation = row["generation"]
        if type(generation) is not str or _GENERATION.fullmatch(generation) is None:
            raise JournalReaderError("malformed_journal")
        return generation

    @staticmethod
    def _identity_from_stat(stat_result: os.stat_result) -> SourceIdentity:
        return (int(stat_result.st_dev), int(stat_result.st_ino))

    def _current_source_identity(self) -> SourceIdentity:
        try:
            stat_result = self._database_path.stat()
        except FileNotFoundError as error:
            raise JournalReaderError("source_changed") from error
        except OSError as error:
            raise JournalReaderError("database_unavailable") from error
        if not self._database_path.is_file():
            raise JournalReaderError("source_changed")
        return self._identity_from_stat(stat_result)

    def _event_from_row(
        self,
        row: sqlite3.Row,
        stream: JournalStream,
        generation: str,
    ) -> JournalEvent:
        sequence = row["sequence"]
        event_id = row["event_id"]
        scope_key = row["scope_key"]
        event_type = row["event_type"]
        state = row["state"]
        created_at_ns = row["created_at_ns"]
        if (
            type(sequence) is not int
            or sequence <= 0
            or type(event_id) is not str
            or _EVENT_ID.fullmatch(event_id) is None
            or type(scope_key) is not str
            or type(event_type) is not str
            or _IDENTIFIER.fullmatch(event_type) is None
            or type(state) is not str
            or type(created_at_ns) is not int
            or created_at_ns <= 0
        ):
            raise JournalReaderError("malformed_event")
        if scope_key != self._scope_key:
            raise JournalReaderError("malformed_event")
        try:
            normalized_state = ExecutionState(state).value
        except ValueError as error:
            raise JournalReaderError("malformed_event") from error

        if stream == "execution":
            intent_id = row["intent_id"]
            row_generation = row["journal_incarnation_id"]
            if (
                not self._valid_identifier(intent_id)
                or type(row_generation) is not str
                or row_generation != generation
            ):
                raise JournalReaderError("malformed_event")
            return JournalEvent(
                sequence=sequence,
                stream=stream,
                event_id=event_id,
                intent_id=intent_id,
                cancel_id=None,
                target_intent_id=None,
                scope_key=scope_key,
                event_type=event_type,
                state=normalized_state,
                created_at_ns=created_at_ns,
                journal_generation=generation,
            )

        cancel_id = row["cancel_id"]
        target_intent_id = row["target_intent_id"]
        if not self._valid_identifier(cancel_id) or not self._valid_identifier(target_intent_id):
            raise JournalReaderError("malformed_event")
        return JournalEvent(
            sequence=sequence,
            stream=stream,
            event_id=event_id,
            intent_id=None,
            cancel_id=cancel_id,
            target_intent_id=target_intent_id,
            scope_key=scope_key,
            event_type=event_type,
            state=normalized_state,
            created_at_ns=created_at_ns,
            journal_generation=generation,
        )

    @staticmethod
    def _valid_identifier(value: object) -> bool:
        return type(value) is str and _IDENTIFIER.fullmatch(value) is not None

    @staticmethod
    def _page_field_bytes(events: tuple[JournalEvent, ...], generation: str) -> int:
        total = len(generation.encode("utf-8"))
        for event in events:
            values = (
                event.stream,
                event.event_id,
                event.intent_id,
                event.cancel_id,
                event.target_intent_id,
                event.scope_key,
                event.event_type,
                event.state,
                event.journal_generation,
            )
            total += sum(len(value.encode("utf-8")) for value in values if value is not None)
        return total
