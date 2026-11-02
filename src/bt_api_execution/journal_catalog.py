"""Exact SQLite object catalog for the execution decision journal.

This catalog is shared with application-owned journal ports so they can
distinguish SDK objects from their own schema without copying SDK table names
or trusting an ``execution_*`` prefix.  It covers declarations in the current
journal schema and named objects used by supported historical migrations.
SQLite's implicit ``sqlite_*`` objects are intentionally not included.
"""

from __future__ import annotations

from typing import FrozenSet, Tuple


_EXECUTION_JOURNAL_OBJECTS: FrozenSet[Tuple[str, str]] = frozenset(
    {
        # Current and historical tables declared by ExecutionDecisionJournal.
        ("table", "execution_callback_delivery_queue"),
        ("table", "execution_callback_query_dispositions"),
        ("table", "execution_decision_callbacks"),
        ("table", "execution_decision_capacity_usage"),
        ("table", "execution_decision_intents"),
        ("table", "execution_decision_slots"),
        ("table", "execution_decisions"),
        ("table", "execution_durable_capacity_limits"),
        ("table", "execution_durable_capacity_usage"),
        ("table", "execution_fee_pair_amount_active_holds"),
        ("table", "execution_fee_pair_amount_lifecycle_events"),
        ("table", "execution_fee_pair_amount_open_reservations"),
        ("table", "execution_fee_pair_amount_reservations"),
        ("table", "execution_fee_pair_candidate_capacity_reservations"),
        ("table", "execution_fee_pair_candidate_events"),
        ("table", "execution_fee_pair_candidates"),
        ("table", "execution_fee_pair_grid_single_row_tape_intents"),
        ("table", "execution_fee_pair_legacy_unresolved_v23"),
        ("table", "execution_fee_pair_required_scopes"),
        ("table", "execution_fee_pair_scope_events"),
        ("table", "execution_fee_pair_workflow_decision_reservations"),
        ("table", "execution_fee_pair_workflow_durable_reservations"),
        ("table", "execution_fee_pair_workflow_events"),
        ("table", "execution_fee_pair_workflow_events_v18"),
        ("table", "execution_fee_pair_workflow_events_v19"),
        ("table", "execution_fee_pair_workflow_grid_settlements"),
        ("table", "execution_fee_pair_workflows"),
        ("table", "execution_grid_cursors"),
        ("table", "execution_grid_highwaters"),
        ("table", "execution_instrument_owners"),
        ("table", "execution_intent_abandonments"),
        ("table", "execution_intent_reconciliations"),
        ("table", "execution_intent_resolutions"),
        ("table", "execution_journal_capacity"),
        ("table", "execution_known_ctp_order_sysid_bindings"),
        ("table", "execution_native_intent_identities"),
        ("table", "execution_registered_sim_callback_unknown_incidents"),
        ("table", "execution_registered_sim_cancel_dispatch_claims"),
        ("table", "execution_registered_sim_cancel_dispatch_tombstones"),
        ("table", "execution_run_loss_guards"),
        ("table", "execution_run_order_budgets"),
        ("table", "execution_slot_abandonments"),
        ("table", "execution_source_highwaters"),
        # Current indexes plus exact historical objects dropped by migrations.
        ("index", "execution_callback_delivery_order"),
        ("index", "execution_callback_disposition_startup"),
        ("index", "execution_decision_intents_recovery"),
        ("index", "execution_decisions_capacity"),
        ("index", "execution_fee_pair_grid_single_row_tape_position"),
        ("index", "execution_fee_pair_workflow_one_active"),
        ("index", "execution_fee_pair_workflow_tape_position"),
        ("index", "execution_intent_reconciliations_order"),
        ("index", "execution_native_intent_lookup"),
        ("index", "execution_registered_sim_callback_unknown_scope_sequence"),
        # Current triggers plus exact historical objects dropped by migrations.
        ("trigger", "execution_abandonment_no_delete"),
        ("trigger", "execution_abandonment_no_update"),
        ("trigger", "execution_callback_delivery_acknowledged"),
        ("trigger", "execution_callback_delivery_disposition"),
        ("trigger", "execution_callback_delivery_insert"),
        ("trigger", "execution_callback_delivery_state"),
        ("trigger", "execution_callback_disposition_no_ack"),
        ("trigger", "execution_callback_query_disposition_no_delete"),
        ("trigger", "execution_callback_query_disposition_no_update"),
        ("trigger", "execution_fee_pair_amount_active_hold_delete"),
        ("trigger", "execution_fee_pair_amount_active_hold_insert"),
        ("trigger", "execution_fee_pair_amount_active_hold_no_update"),
        ("trigger", "execution_fee_pair_amount_lifecycle_event_insert"),
        ("trigger", "execution_fee_pair_amount_lifecycle_event_no_delete"),
        ("trigger", "execution_fee_pair_amount_lifecycle_event_no_update"),
        ("trigger", "execution_fee_pair_amount_open_reservation_exclusive"),
        ("trigger", "execution_fee_pair_amount_open_reservation_no_delete"),
        ("trigger", "execution_fee_pair_amount_open_reservation_no_update"),
        ("trigger", "execution_fee_pair_amount_reservation_exclusive"),
        ("trigger", "execution_fee_pair_amount_reservation_no_delete"),
        ("trigger", "execution_fee_pair_amount_reservation_no_update"),
        ("trigger", "execution_fee_pair_candidate_capacity_no_delete"),
        ("trigger", "execution_fee_pair_candidate_capacity_state"),
        ("trigger", "execution_fee_pair_candidate_event_no_delete"),
        ("trigger", "execution_fee_pair_candidate_event_no_update"),
        ("trigger", "execution_fee_pair_candidate_no_delete"),
        ("trigger", "execution_fee_pair_candidate_state"),
        ("trigger", "execution_fee_pair_grid_single_row_tape_intent_no_delete"),
        ("trigger", "execution_fee_pair_grid_single_row_tape_intent_no_update"),
        ("trigger", "execution_fee_pair_legacy_unresolved_v23_no_delete"),
        ("trigger", "execution_fee_pair_legacy_unresolved_v23_no_update"),
        ("trigger", "execution_fee_pair_scope_event_no_delete"),
        ("trigger", "execution_fee_pair_scope_event_no_update"),
        ("trigger", "execution_fee_pair_scope_no_delete"),
        ("trigger", "execution_fee_pair_scope_no_update"),
        ("trigger", "execution_fee_pair_workflow_decision_reservation_no_delete"),
        ("trigger", "execution_fee_pair_workflow_decision_reservation_state"),
        ("trigger", "execution_fee_pair_workflow_durable_reservation_no_delete"),
        ("trigger", "execution_fee_pair_workflow_durable_reservation_state"),
        ("trigger", "execution_fee_pair_workflow_event_no_delete"),
        ("trigger", "execution_fee_pair_workflow_event_no_update"),
        ("trigger", "execution_fee_pair_workflow_grid_settlement_insert"),
        ("trigger", "execution_fee_pair_workflow_grid_settlement_no_delete"),
        ("trigger", "execution_fee_pair_workflow_grid_settlement_no_update"),
        ("trigger", "execution_fee_pair_workflow_no_delete"),
        ("trigger", "execution_fee_pair_workflow_state"),
        ("trigger", "execution_known_ctp_order_sysid_binding_no_delete"),
        ("trigger", "execution_known_ctp_order_sysid_binding_no_update"),
        ("trigger", "execution_reconciliation_no_delete"),
        ("trigger", "execution_reconciliation_no_update"),
        ("trigger", "execution_registered_sim_callback_unknown_dispatch_guard"),
        ("trigger", "execution_registered_sim_callback_unknown_no_delete"),
        ("trigger", "execution_registered_sim_callback_unknown_no_update"),
        ("trigger", "execution_registered_sim_cancel_dispatch_claim_no_delete"),
        ("trigger", "execution_registered_sim_cancel_dispatch_claim_no_update"),
        ("trigger", "execution_registered_sim_cancel_dispatch_tombstone_no_delete"),
        ("trigger", "execution_registered_sim_cancel_dispatch_tombstone_no_update"),
        ("trigger", "execution_slot_abandonment_no_delete"),
        ("trigger", "execution_slot_abandonment_no_update"),
    }
)


def is_execution_journal_catalog_object(object_type: str, name: str, /) -> bool:
    """Return exact ``True`` only for a declared SDK journal object identity.

    The catalog is an allowlist of exact SQLite ``type`` and ``name`` pairs,
    not a namespace-prefix check. Unsupported names, casing, object kinds, or
    malformed values return ``False`` so callers fail closed.
    """

    return (
        type(object_type) is str
        and type(name) is str
        and (object_type, name) in _EXECUTION_JOURNAL_OBJECTS
    )


__all__ = ("is_execution_journal_catalog_object",)
