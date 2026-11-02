"""Project a terminal cancellation observation from Execution source events.

This validates event shape and sequence; it does not establish source authority.
"""
from __future__ import annotations

from .contracts import CancelObservation, ExecutionState


def terminal_cancel_observation(source, cancel_intent, *, error_type):
    """Return the terminal observation, using the caller's error type."""
    observation_events = [
        event
        for event in source.cancel_events
        if event.event_type in {"cancel_provider_observation", "cancel_reconciled_observation"}
    ]
    terminal_events = [
        event
        for event in observation_events
        if event.state in {ExecutionState.CANCELLED, ExecutionState.REJECTED}
    ]
    if (
        not observation_events
        or len(terminal_events) != 1
        or observation_events[-1] != terminal_events[0]
        or any(event.state is not ExecutionState.ACKED for event in observation_events[:-1])
        or sum(
            event.state is ExecutionState.ACKED
            for event in observation_events[:-1]
        ) > 1
        or (
            observation_events[:-1]
            and terminal_events[0].event_type != "cancel_reconciled_observation"
        )
        or any(
            event.cancel_id != cancel_intent.cancel_id
            or event.target_intent_id != cancel_intent.target_intent_id
            or event.scope_key != source.scope.key
            or set(event.payload) != {"provider_order_id", "reason_code", "source"}
            or event.payload.get("provider_order_id") != cancel_intent.provider_order_id
            or event.payload.get("source")
            != (
                "provider"
                if event.event_type == "cancel_provider_observation"
                else "reconcile"
            )
            for event in observation_events
        )
    ):
        raise error_type("CANCEL_DISPATCH_TERMINAL_EVENT_INVALID")
    try:
        for event in observation_events:
            CancelObservation(
                cancel_id=event.cancel_id,
                target_intent_id=event.target_intent_id,
                provider_order_id=event.payload["provider_order_id"],
                state=event.state,
                reason_code=event.payload["reason_code"],
            )
    except Exception:
        raise error_type("CANCEL_DISPATCH_TERMINAL_EVENT_INVALID") from None
    return terminal_events[0]


__all__ = ("terminal_cancel_observation",)
