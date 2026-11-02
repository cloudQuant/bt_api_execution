"""Serialize exact Execution records for an external source comparison.

These projections do not establish journal authority, source truth or a fence.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any


class ExecutionSourceProjectionError(RuntimeError):
    """Finite projection failure without record contents."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def cancel_event_payload(event: object) -> dict[str, Any]:
    from .contracts import CancelEvent

    if type(event) is not CancelEvent:
        raise ExecutionSourceProjectionError("CANCEL_EVENT_SOURCE_INVALID")
    return {
        "cancel_id": event.cancel_id,
        "created_at_ns": event.created_at_ns,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "payload": dict(event.payload),
        "scope_key": event.scope_key,
        "sequence": event.sequence,
        "state": event.state.value,
        "target_intent_id": event.target_intent_id,
    }


def execution_event_payload(event: object) -> dict[str, Any]:
    from .contracts import ExecutionEvent

    if type(event) is not ExecutionEvent:
        raise ExecutionSourceProjectionError("CANCEL_TARGET_EVENT_SOURCE_INVALID")
    return {
        "created_at_ns": event.created_at_ns,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "intent_id": event.intent_id,
        "journal_incarnation_id": event.journal_incarnation_id,
        "payload": dict(event.payload),
        "scope_key": event.scope_key,
        "sequence": event.sequence,
        "state": event.state.value,
    }


def execution_record_payload(record: object) -> dict[str, Any]:
    from .store import ExecutionRecord

    if type(record) is not ExecutionRecord:
        raise ExecutionSourceProjectionError("CANCEL_TARGET_RECORD_SOURCE_INVALID")
    return {
        "admission_context_sha256": record.admission_context_sha256,
        "average_price": (
            None if record.average_price is None else cancel_decimal_text(record.average_price)
        ),
        "basic_admission_reference": record.basic_admission_reference,
        "cumulative_commission": (
            None
            if record.cumulative_commission is None
            else cancel_decimal_text(record.cumulative_commission)
        ),
        "dispatch_attempts": record.dispatch_attempts,
        "filled_quantity": cancel_decimal_text(record.filled_quantity),
        "group_binding": None,
        "intent_id": record.intent_id,
        "payload_sha256": record.payload_sha256,
        "permit_reference": record.permit_reference,
        "provider_order_id": record.provider_order_id,
        "review_required": record.review_required,
        "scope_key": record.scope_key,
        "state": record.state.value,
        "unknown_reason": record.unknown_reason,
        "updated_at_ns": record.updated_at_ns,
        "created_at_ns": record.created_at_ns,
    }


def cancel_record_payload(record: object) -> dict[str, Any]:
    from .store import CancelRecord

    if type(record) is not CancelRecord:
        raise ExecutionSourceProjectionError("CANCEL_RECORD_SOURCE_INVALID")
    return {
        "cancel_id": record.cancel_id,
        "created_at_ns": record.created_at_ns,
        "dispatch_attempts": record.dispatch_attempts,
        "payload_sha256": record.payload_sha256,
        "permit_reference": record.permit_reference,
        "provider_order_id": record.provider_order_id,
        "review_required": record.review_required,
        "scope_key": record.scope_key,
        "state": record.state.value,
        "target_intent_id": record.target_intent_id,
        "unknown_reason": record.unknown_reason,
        "updated_at_ns": record.updated_at_ns,
    }


def cancel_decimal_text(value: object) -> str:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ExecutionSourceProjectionError("CANCEL_DECIMAL_SOURCE_INVALID")
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


__all__ = ("ExecutionSourceProjectionError", "cancel_event_payload", "execution_event_payload", "execution_record_payload", "cancel_record_payload", "cancel_decimal_text")
