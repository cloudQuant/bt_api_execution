"""Provider-neutral position attribution port for the execution journal.

The local attribution book never establishes provider position or fill facts.
"""
from __future__ import annotations

from typing import Any, Iterable, Optional, Tuple

from .journal_ports import (
    JOURNAL_POSITION_ATTRIBUTION_PORT_SCHEMA_VERSION,
    JournalPositionAttributionPort,
    JournalPositionCapacityRow,
    JournalPositionCurrent,
    JournalPositionFill,
    JournalPositionIssue,
)
from .factor_position_attribution import (
    FactorPositionBookError,
    FactorPositionBook,
    factor_position_schema_version,
    preflight_factor_position_journal_schema,
    ensure_factor_position_journal_schema,
    validate_factor_position_journal_schema,
    reject_factor_provider_evidence,
    iter_factor_position_capacity_rows,
    snapshot_factor_position_capacity_row_size,
    record_factor_position_grid,
    record_factor_position_flat_checkpoint,
    require_factor_close_reconciliation_inputs,
    validate_factor_position_close_owner,
)


class FactorPositionAttributionJournalPort(JournalPositionAttributionPort):
    """Execute local attribution operations on the journal's current cursor."""

    port_version = JOURNAL_POSITION_ATTRIBUTION_PORT_SCHEMA_VERSION

    def __init__(self, *, error_type=FactorPositionBookError):
        self._error_type = error_type

    @property
    def unavailable_capabilities(self) -> Tuple[str, ...]:
        """Provider evidence that the local attribution book does not own."""

        return (
            "terminal provider-order disposition bound to intent/client_order_id",
            "complete provider-position reconciliation bound to QueryBatchReceipt and lease",
            "provider position quantity-unit aggregation across account resources",
            "SDK current-position and execution-fill evidence rows",
            "SDK provider evidence-issue rows",
        )

    @property
    def expected_schema_version(self) -> int:
        """The attribution schema version the selected SDK currently writes."""

        try:
            return factor_position_schema_version()
        except FactorPositionBookError as error:
            raise self._error_type(error.code) from None

    def local_position_snapshot(
        self,
        cursor: Any,
        *,
        account_scope: str,
        strategy_scope: str,
        logical_run_id: str,
        instrument: Any,
        quantity_unit: str,
        broker_quantity: Any,
        snapshot_id: str,
        available_at: Any,
    ) -> Any:
        """Return local fill attribution for one neutral instrument and unit."""

        return FactorPositionBook().snapshot(
            cursor=cursor,
            account_scope=account_scope,
            strategy_scope=strategy_scope,
            logical_run_id=logical_run_id,
            instrument=instrument,
            quantity_unit=quantity_unit,
            broker_quantity=broker_quantity,
            snapshot_id=snapshot_id,
            available_at=available_at,
        )


    def preflight_schema(self, cursor: Any) -> bool:
        """Reject old or partial owner schemas before schema creation can mutate them."""

        return preflight_factor_position_journal_schema(cursor)

    def ensure_schema(self, cursor: Any) -> None:
        try:
            ensure_factor_position_journal_schema(cursor)
        except FactorPositionBookError as error:
            raise self._error_type(error.code) from None

    def validate_schema(self, cursor: Any) -> bool:
        """Validate only the exact attribution schema on the journal cursor."""

        return validate_factor_position_journal_schema(cursor)

    def read_current_position(
        self,
        cursor: Any,
        *,
        account_scope: str,
        instrument: str,
        exchange: str,
    ) -> Optional[JournalPositionCurrent]:
        """Refuse to promote the BT-local fill book into provider position truth."""

        del account_scope, instrument, exchange
        try:
            reject_factor_provider_evidence(cursor, "current_position")
        except FactorPositionBookError as error:
            raise self._error_type(error.code) from None

    def read_fill(
        self,
        cursor: Any,
        *,
        account_scope: str,
        instrument: str,
        exchange: str,
        execution_id: str,
    ) -> Optional[JournalPositionFill]:
        """Refuse to equate local execution bits with provider fill facts."""

        del account_scope, instrument, exchange, execution_id
        try:
            reject_factor_provider_evidence(cursor, "fill")
        except FactorPositionBookError as error:
            raise self._error_type(error.code) from None

    def read_evidence_issue(
        self, cursor: Any, *, receipt_id: str
    ) -> Optional[JournalPositionIssue]:
        """Keep provider receipt evidence separate from local flat checkpoints."""

        del receipt_id
        try:
            reject_factor_provider_evidence(cursor, "evidence_issue")
        except FactorPositionBookError as error:
            raise self._error_type(error.code) from None

    def iter_capacity_rows(self, cursor: Any) -> Iterable[JournalPositionCapacityRow]:
        """Return local attribution capacity rows through the journal port."""
        try:
            rows = iter_factor_position_capacity_rows(cursor)
        except FactorPositionBookError as error:
            raise self._error_type(error.code) from None

        def project_rows() -> Iterable[JournalPositionCapacityRow]:
            try:
                yield from rows
            except FactorPositionBookError as error:
                raise self._error_type(error.code) from None

        return project_rows()

    def snapshot_capacity_row_size(
        self,
        cursor: Any,
        *,
        owner_table_ref: str,
        key_values: tuple[Any, ...],
    ) -> Optional[int]:
        """Read an SDK-owned attribution row on the active journal cursor."""
        try:
            return snapshot_factor_position_capacity_row_size(
                cursor, owner_table_ref=owner_table_ref, key_values=key_values
            )
        except FactorPositionBookError as error:
            raise self._error_type(error.code) from None

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
        return bool(
            record_factor_position_grid(
                cursor,
                account_scope=account_scope,
                strategy_scope=strategy_scope,
                cohort_id=cohort_id,
                batch_id=batch_id,
                close_time=close_time,
                ordinal=ordinal,
                capacity_class=capacity_class,
            )
        )

    def record_flat_checkpoint(
        self, cursor: Any, *, intent_id: str, reconciliation_id: str
    ) -> None:
        """Persist the journal's opaque reconciliation reference as an audit anchor.

        The journal must validate the terminal reconciliation before this call;
        the local book stores no provider claim or native row.
        """

        record_factor_position_flat_checkpoint(
            cursor, intent_id=intent_id, reconciliation_id=reconciliation_id
        )

    def validate_close_owner(
        self,
        cursor: Any,
        *,
        account_scope: str,
        strategy_scope: str,
        instrument: Any,
        exchange: Optional[str] = None,
        owner_intent_id: str,
        logical_run_id: Optional[str] = None,
        quantity_unit: Optional[str] = None,
    ) -> None:
        """Reject the legacy under-scoped close contract.

        The SDK's current journal port supplies a symbol and exchange string,
        but not the neutral instrument key, run/unit, current position snapshot,
        or terminal-order proof.  Converting those strings locally would guess
        provider identity and would not establish a reconciled residual.
        """

        try:
            require_factor_close_reconciliation_inputs(
                instrument=instrument,
                exchange=exchange,
                logical_run_id=logical_run_id,
                quantity_unit=quantity_unit,
            )
        except FactorPositionBookError as error:
            raise self._error_type(error.code) from error.__cause__

    def validate_local_close_owner(
        self,
        cursor: Any,
        *,
        account_scope: str,
        strategy_scope: str,
        logical_run_id: str,
        instrument: Any,
        quantity_unit: str,
        owner_intent_id: str,
    ) -> None:
        """Check BT-local residual ownership without implying provider reconciliation."""

        validate_factor_position_close_owner(
            cursor,
            account_scope=account_scope,
            strategy_scope=strategy_scope,
            logical_run_id=logical_run_id,
            instrument=instrument,
            quantity_unit=quantity_unit,
            owner_intent_id=owner_intent_id,
        )


__all__ = ("FactorPositionAttributionJournalPort",)
