"""Independent Execution-journal validation for DIM05 aggregate group close.

The Risk service is the authority for the close receipt and fee rows.  This
module checks that those exact, digest-bound outputs are durably attached to
the accepted v1/v2 group source before the local aggregate state can become
``CLOSED``.  It deliberately has no Risk client or provider dependency.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from decimal import Decimal
from fractions import Fraction
from typing import Any

from .contracts import (
    ExecutionState,
    OrderType,
    PositionEffect,
    Side,
    TimeInForce,
    order_intent_from_payload,
    payload_sha256,
)
from .errors import ContractValidationError
from .group_order_contracts import (
    GroupAccountReservationSnapshot,
    GroupBudgetReservationState,
    GroupChildOrderStatus,
    GroupDispatchResolutionReceipt,
    GroupLegClaimReceipt,
    GroupOrderEvent,
    GroupOrderIntent,
    GroupOrderSnapshot,
    GroupOrderState,
    GroupReservationPhase,
    _parse_decimal,
    _require_digest,
)
from .group_roll_exit_contracts import (
    GroupRollExitBudgetReleaseV1,
    GroupRollExitCloseActionV1,
    GroupRollExitCloseFeeEvidenceV1,
    GroupRollExitCloseFeeFillV1,
    GroupRollExitClosedProofV1,
    GroupRollExitLegClosedV1,
    GroupRollExitRiskCloseReceiptV1,
    GroupRollExitSourceFeeEvidenceV1,
)

_MAX_ACTIONS = 64
_AUTH_KEYS = {
    "schema", "request", "source", "action_id", "client_order_id", "intent",
    "deadline_ns", "authorization_sha256", "risk_intent_id",
}
_REQUEST_KEYS = {"schema", "portfolio_ref", "group_id", "source_leg_id", "limit_price"}
_SOURCE_KEYS = {
    "schema", "portfolio_ref", "origin_run_id", "group_id", "group_version",
    "group_revision", "group_fingerprint", "source_risk_reservation_fingerprint",
    "source_risk_intent_fingerprint", "group_request", "account_ref", "strategy_id",
    "source_leg_id", "source_intent_id", "source_request_fingerprint", "source_child_intent",
    "source_execution_scope", "source_terminal_status", "source_requested_quantity",
    "source_claim", "source_risk_resolution", "source_event_digest", "source_side",
    "instrument", "source_metadata_version", "close_metadata_version", "close_metadata_digest",
    "close_quantity_step", "close_tick_size", "close_metadata_valid_until_ns",
    "source_position_side", "source_position_bucket", "source_filled_quantity",
    "prior_resolved_closes", "remaining_quantity", "closable_position_version",
    "service_principal_ref", "service_instance_id", "service_scope_ref",
    "service_registration_id", "service_operation", "service_port",
    "service_registration_digest", "service_epoch", "service_policy_version",
    "service_requirements_version", "service_write_run_id",
}
_OBSERVATION_KEYS = {
    "schema", "action_id", "phase", "source_digest", "request_fingerprint",
    "authorization_sha256", "intent_id", "close_state", "requested_quantity",
    "filled_quantity", "order_revision", "close_receipt_sha256", "detail_code",
    "risk_intent_id", "close_receipt", "close_fee_evidence",
}
_OBSERVATION_PHASES = {"PREPARED", "SUBMIT_STARTED", "STATUS_OBSERVED", "UNKNOWN", "RESOLVED"}
_PENDING_STATES = {
    ExecutionState.PENDING_ADMISSION,
    ExecutionState.PENDING_DISPATCH,
    ExecutionState.DISPATCHING,
    ExecutionState.ACKED,
    ExecutionState.PARTIALLY_FILLED,
}
_TERMINAL = {ExecutionState.FILLED, ExecutionState.CANCELLED, ExecutionState.REJECTED}
_ROLL_EVENT_TRANSITIONS = {
    "GROUP_ROLL_EXIT_PLAN_RECORDED": {
        (GroupOrderState.PAIRED, GroupOrderState.UNWINDING),
        (GroupOrderState.UNWINDING, GroupOrderState.UNWINDING),
        (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.UNWINDING),
    },
    "GROUP_ROLL_EXIT_SUBMIT_STARTED": {
        (GroupOrderState.UNWINDING, GroupOrderState.UNWINDING),
        (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.UNWINDING),
    },
    "GROUP_ROLL_EXIT_STATUS_OBSERVED": {
        (GroupOrderState.UNWINDING, GroupOrderState.UNWINDING),
        (GroupOrderState.UNWINDING, GroupOrderState.RECOVERY_REQUIRED),
        (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.RECOVERY_REQUIRED),
        (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.UNWINDING),
    },
    "GROUP_ROLL_EXIT_RESOLVED": {
        (GroupOrderState.UNWINDING, GroupOrderState.UNWINDING),
        (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.UNWINDING),
    },
    "GROUP_ROLL_EXIT_CLOSED": {
        (GroupOrderState.UNWINDING, GroupOrderState.CLOSED),
        (GroupOrderState.RECOVERY_REQUIRED, GroupOrderState.CLOSED),
    },
}
ROLL_EXIT_EVENT_TYPES = frozenset(_ROLL_EVENT_TRANSITIONS)


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if type(value) in (list, tuple):
        return [_thaw(item) for item in value]
    return value


def _mapping(value: object, keys: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ContractValidationError(f"invalid {label}")
    return value


def _reference(value: object, label: str) -> str:
    if type(value) is not str or not value or len(value) > 128 or not value.isascii():
        raise ContractValidationError(f"invalid {label}")
    if not value[0].isalnum() or any(not (c.isalnum() or c in "._:-") for c in value):
        raise ContractValidationError(f"invalid {label}")
    return value


def _fraction_sum(values: tuple[Decimal, ...]) -> Fraction:
    return sum((Fraction(item) for item in values), Fraction(0))


def _exact_decimal_text(value: Fraction) -> str:
    denominator = value.denominator
    twos = fives = 0
    while denominator % 2 == 0:
        twos += 1
        denominator //= 2
    while denominator % 5 == 0:
        fives += 1
        denominator //= 5
    if denominator != 1:
        raise ContractValidationError("roll-exit quantity is not an exact decimal")
    places = max(twos, fives)
    coefficient = value.numerator * (2 ** (places - twos)) * (5 ** (places - fives))
    sign = "-" if coefficient < 0 else ""
    digits = str(abs(coefficient))
    if places:
        digits = digits.rjust(places + 1, "0")
        return f"{sign}{digits[:-places]}.{digits[-places:]}"
    return sign + digits


def group_event_chain_sha256(
    snapshot: GroupOrderSnapshot, history: tuple[GroupOrderEvent, ...]
) -> str:
    """Hash the exact current, hash-verified group event projection."""

    if type(snapshot) is not GroupOrderSnapshot or type(history) is not tuple:
        raise ContractValidationError("group event history is required")
    if len(history) != snapshot.revision + 1:
        raise ContractValidationError("group event history does not match snapshot revision")
    if any(
        type(item) is not GroupOrderEvent
        or item.group_id != snapshot.request.group_id
        or type(item.sequence) is not int
        or item.sequence <= 0
        for item in history
    ) or any(a.sequence >= b.sequence for a, b in zip(history, history[1:])):
        raise ContractValidationError("group event sequence is invalid")
    return payload_sha256(
        {
            "schema": "bt-api-dim05-group-event-chain-v1",
            "group_id": snapshot.request.group_id,
            "group_revision": snapshot.revision,
            "events": [
                {
                    "created_at_ns": item.created_at_ns,
                    "event_type": item.event_type,
                    "group_id": item.group_id,
                    "payload": dict(item.payload),
                    "sequence": item.sequence,
                }
                for item in history
            ],
        }
    )


@dataclass(slots=True)
class _Action:
    authorization: Mapping[str, Any]
    auth_digest: str
    source: Mapping[str, Any]
    request: Mapping[str, Any]
    intent: Any
    action_id: str
    risk_intent_id: str
    phase: str = "PLANNED"
    observation: Mapping[str, Any] | None = None
    receipt: GroupRollExitRiskCloseReceiptV1 | None = None
    fee: GroupRollExitCloseFeeEvidenceV1 | None = None
    resolved: bool = False


def _source_fee(source: Mapping[str, Any]) -> GroupRollExitSourceFeeEvidenceV1:
    status = GroupChildOrderStatus.from_payload(_thaw(source["source_terminal_status"]))
    if status.cumulative_commission is None or status.cumulative_commission_currency is None:
        raise ContractValidationError("source terminal fee evidence is missing")
    return GroupRollExitSourceFeeEvidenceV1(
        intent_id=status.intent_id,
        amount=status.cumulative_commission,
        currency=status.cumulative_commission_currency,
        source_event_sha256=source["source_event_digest"],
    )


def _risk_close_fee_from_payload(value: object) -> GroupRollExitCloseFeeEvidenceV1:
    """Normalize the exact Risk fee wire into the Execution proof DTO.

    Risk owns the journal-backed wire schema.  Execution adds its own digest
    over the normalized projection for the aggregate close proof; it does not
    pretend that Risk sent that digest or schema.
    """

    expected = {
        "schema", "intent_id", "journal_revision", "fill_observation_sha256",
        "fee_currency", "fee_amount", "fills",
    }
    if (
        not isinstance(value, Mapping)
        or set(value) != expected
        or value.get("schema") != "group-roll-exit-close-fee-evidence-v1"
        or type(value.get("fills")) not in (list, tuple)
        or len(value["fills"]) == 0
        or len(value["fills"]) > 4096
    ):
        raise ContractValidationError("invalid Risk roll-exit close fee wire")
    try:
        return GroupRollExitCloseFeeEvidenceV1(
            intent_id=value["intent_id"],
            journal_revision=value["journal_revision"],
            fill_observation_sha256=value["fill_observation_sha256"],
            fee_amount=_parse_decimal(value["fee_amount"], "close fee amount"),
            fee_currency=value["fee_currency"],
            fills=tuple(
                GroupRollExitCloseFeeFillV1.from_payload(_thaw(item))
                for item in value["fills"]
            ),
        )
    except (KeyError, TypeError, ValueError, ContractValidationError) as error:
        raise ContractValidationError("invalid Risk roll-exit close fee wire") from error


def _validate_authorization(
    snapshot: GroupOrderSnapshot,
    payload: object,
    *,
    plan_revision: int,
    previous_actions: tuple[_Action, ...],
) -> _Action:
    auth = _mapping(payload, _AUTH_KEYS, "roll-exit authorization")
    if auth.get("schema") != "group-roll-exit-authorization-v1":
        raise ContractValidationError("unsupported roll-exit authorization")
    body = dict(auth)
    auth_digest = body.pop("authorization_sha256", None)
    _require_digest(auth_digest, "authorization_sha256")
    if payload_sha256(body) != auth_digest:
        raise ContractValidationError("roll-exit authorization digest mismatch")

    request = _mapping(auth["request"], _REQUEST_KEYS, "roll-exit request")
    if request.get("schema") != "group-roll-exit-request-v1":
        raise ContractValidationError("unsupported roll-exit request")
    if (
        request.get("portfolio_ref") != snapshot.request.portfolio_ref
        or request.get("group_id") != snapshot.request.group_id
    ):
        raise ContractValidationError("roll-exit request is outside the accepted group")
    source = _mapping(auth["source"], _SOURCE_KEYS, "roll-exit source facts")
    if source.get("schema") != "group-roll-exit-source-facts-v1":
        raise ContractValidationError("unsupported roll-exit source facts")
    group_request = GroupOrderIntent.from_payload(_thaw(source["group_request"]))
    if (
        group_request.to_payload() != snapshot.request.to_payload()
        or type(source.get("group_version")) is not int
        or group_request.version not in {1, 2}
        or len(group_request.legs) != 2
        or source.get("group_revision") != plan_revision
        or source.get("group_version") != group_request.version
        or source.get("group_fingerprint") != snapshot.request.fingerprint
        or source.get("group_id") != snapshot.request.group_id
        or source.get("portfolio_ref") != snapshot.request.portfolio_ref
        or source.get("strategy_id") != snapshot.request.strategy_id
        or source.get("service_operation") != "orders.submit_group_roll_exit"
        or source.get("service_port") != "service.order_entry"
        or source.get("service_write_run_id") != source.get("origin_run_id")
        or type(source.get("prior_resolved_closes")) not in (list, tuple)
        or len(source["prior_resolved_closes"]) > _MAX_ACTIONS
    ):
        raise ContractValidationError("roll-exit source differs from accepted group")
    for digest_name in (
        "group_fingerprint", "source_risk_reservation_fingerprint",
        "source_risk_intent_fingerprint", "source_request_fingerprint", "source_event_digest",
        "close_metadata_digest", "closable_position_version", "service_registration_digest",
    ):
        _require_digest(source.get(digest_name), digest_name)
    for name in (
        "origin_run_id", "account_ref", "strategy_id", "source_leg_id", "source_intent_id",
        "instrument", "source_metadata_version", "close_metadata_version", "service_principal_ref",
        "service_instance_id", "service_scope_ref", "service_registration_id", "service_epoch",
        "service_policy_version", "service_requirements_version",
    ):
        _reference(source.get(name), name)
    if type(source.get("group_revision")) is not int or source["group_revision"] <= 0:
        raise ContractValidationError("invalid source group revision")
    close_quantity_step = _parse_decimal(source.get("close_quantity_step"), "close_quantity_step")
    close_tick_size = _parse_decimal(source.get("close_tick_size"), "close_tick_size")
    close_metadata_valid_until_ns = source.get("close_metadata_valid_until_ns")
    if (
        close_quantity_step <= 0
        or close_tick_size <= 0
        or type(close_metadata_valid_until_ns) is not int
        or close_metadata_valid_until_ns <= 0
    ):
        raise ContractValidationError("invalid close metadata bounds")

    try:
        source_leg = group_request.leg(source["source_leg_id"])
        source_intent = order_intent_from_payload(_thaw(source["source_child_intent"]))
        source_status = GroupChildOrderStatus.from_payload(_thaw(source["source_terminal_status"]))
        source_claim = GroupLegClaimReceipt.from_payload(_thaw(source["source_claim"]))
        source_resolution = GroupDispatchResolutionReceipt.from_payload(
            _thaw(source["source_risk_resolution"])
        )
    except (KeyError, ValueError, TypeError, ContractValidationError) as error:
        raise ContractValidationError("invalid persisted roll-exit source proof") from error
    try:
        progress = next(item for item in snapshot.legs if item.leg_id == source_leg.leg_id)
    except StopIteration:
        raise ContractValidationError("roll-exit source leg is absent") from None
    source_reservations = tuple(
        item for item in snapshot.reservations if item.account_ref == source_leg.account_ref
    )
    source_risk_reservation = source_reservations[0] if len(source_reservations) == 1 else None
    source_risk_legs = (
        ()
        if source_risk_reservation is None
        else tuple(item for item in source_risk_reservation.legs if item.leg_id == source_leg.leg_id)
    )
    source_risk_leg = source_risk_legs[0] if len(source_risk_legs) == 1 else None
    if (
        source_leg.account_ref != source.get("account_ref")
        or source_intent != progress.submitted_intent
        or progress.phase.value != "OBSERVED"
        or progress.child_status != source_status
        or progress.claim_receipt != source_claim
        or progress.resolution_receipt != source_resolution
        or source_intent.position_effect is not PositionEffect.OPEN
        or source_intent.intent_id != source.get("source_intent_id")
        or source_intent.fingerprint != source.get("source_request_fingerprint")
        or source_intent.scope.to_payload() != _thaw(source["source_execution_scope"])
        or source_intent.scope.account_ref != source.get("account_ref")
        or source_intent.scope.strategy_id != source.get("strategy_id")
        or source_intent.side.value != source.get("source_side")
        or source_intent.instrument != source.get("instrument")
        or source_intent.position_side != source.get("source_position_side")
        or source_intent.position_bucket != source.get("source_position_bucket")
        or source_status.state not in {ExecutionState.FILLED, ExecutionState.CANCELLED}
        or source_status.intent_id != source_intent.intent_id
        or source_status.filled_quantity <= 0
        or source_status.filled_quantity
        != _parse_decimal(source.get("source_filled_quantity"), "source_filled_quantity")
        or source_status.order_revision != source_resolution.order_revision
        or source_status.filled_quantity != source_resolution.filled_quantity
        or source_status.trade_count != source_resolution.trade_count
        or source_status.state is not source_resolution.final_state
        or source_resolution.resolution_kind != "TERMINAL_EXPOSURE_RESOLVED"
        or source_resolution.claim != source_claim
        or source_risk_reservation is None
        or source_risk_reservation.group_id != snapshot.request.group_id
        or source_risk_reservation.group_fingerprint != source.get("source_risk_reservation_fingerprint")
        or source_risk_reservation.scope_fingerprint != source_claim.permit.risk_scope_fingerprint
        or source_risk_reservation.policy_fingerprint != source_claim.permit.policy_fingerprint
        or source_risk_reservation.deadline_ns != source_claim.permit.deadline_ns
        or source_risk_reservation.deadline_ns != group_request.deadline_ns
        or source_risk_leg is None
        or source_risk_leg.permit_id != source_claim.permit.permit_id
        or source_risk_leg.intent_id != source_claim.permit.intent_id
        or source_risk_leg.reserved_intent_hash != source_claim.permit.reserved_intent_hash
        or source_risk_leg.exposure_profile_fingerprint != source_claim.permit.exposure_profile_fingerprint
        or source_risk_leg.actual_intent_hash != source.get("source_risk_intent_fingerprint")
        or not source_risk_leg.claimed
        or not source_risk_leg.resolved
        or source_claim.actual_child_intent_hash != source.get("source_risk_intent_fingerprint")
        or source_claim.actual_quantity != source_intent.quantity
        or source_claim.permit.order_intent_hash != source_intent.fingerprint
        or source_claim.permit.group_id != snapshot.request.group_id
        or source_claim.permit.intent_id != source_intent.intent_id
        or source_claim.permit.portfolio_ref != snapshot.request.portfolio_ref
        or source_claim.permit.coordinator_principal_ref != group_request.coordinator_principal_ref
        or source_claim.permit.group_fingerprint != source.get("source_risk_reservation_fingerprint")
        or source_claim.permit.leg_id != source_leg.leg_id
        or source_claim.permit.account_ref != source_leg.account_ref
        or source_claim.permit.strategy_id != snapshot.request.strategy_id
        or source_claim.permit.deadline_ns != group_request.deadline_ns
        or source_leg.maximum_intent.quantity
        != _parse_decimal(source["source_requested_quantity"], "source_requested_quantity")
        or Fraction(source_intent.quantity) > Fraction(source_leg.maximum_intent.quantity)
        or Fraction(source_intent.quantity) % Fraction(source_leg.quantity_step) != 0
        or replace(source_leg.maximum_intent, quantity=source_intent.quantity) != source_intent
        or source_resolution.requested_quantity != source_intent.quantity
        or source_status.state is ExecutionState.FILLED
        and source_status.filled_quantity != source_intent.quantity
    ):
        raise ContractValidationError("roll-exit source proof differs from persisted group leg")
    if (
        source_status.cumulative_commission is None
        or source_status.cumulative_commission_currency is None
    ):
        raise ContractValidationError("roll-exit source fee or terminal event proof is missing")

    req_fingerprint = payload_sha256(dict(request))
    source_digest = payload_sha256(dict(source))
    expected_action = "dim05-roll-" + payload_sha256(
        {
            "schema": "group-roll-exit-authorization-v1",
            "source_digest": source_digest,
            "request_fingerprint": req_fingerprint,
        }
    )
    expected_client = "dim05-client-" + payload_sha256(
        {"schema": "group-roll-exit-authorization-v1", "action_id": expected_action}
    )[:40]
    action_id = _reference(auth.get("action_id"), "action_id")
    risk_intent_id = _reference(auth.get("risk_intent_id"), "risk_intent_id")
    if (
        action_id != expected_action
        or auth.get("client_order_id") != expected_client
        or request.get("source_leg_id") != source_leg.leg_id
    ):
        raise ContractValidationError("roll-exit action identity is not source-derived")

    intent = order_intent_from_payload(_thaw(auth["intent"]))
    limit_price = _parse_decimal(request.get("limit_price"), "limit_price")
    remaining = _parse_decimal(source.get("remaining_quantity"), "remaining_quantity")
    if (
        remaining <= 0
        or intent.intent_id != action_id
        or intent.signal_id != action_id
        or intent.scope != source_intent.scope
        or intent.instrument != source_intent.instrument
        or intent.side is not (Side.SELL if source_intent.side is Side.BUY else Side.BUY)
        or intent.position_effect is not PositionEffect.CLOSE
        or intent.reduce_only is not True
        or intent.position_side != source_intent.position_side
        or intent.position_bucket != source_intent.position_bucket
        or intent.order_type is not OrderType.LIMIT
        or intent.quantity != remaining
        or intent.price != limit_price
        or Fraction(limit_price) % Fraction(close_tick_size) != 0
        or intent.metadata_version != source.get("close_metadata_version")
        or intent.time_in_force is not TimeInForce.GTC
        or bool(dict(intent.tags))
        or type(auth.get("deadline_ns")) is not int
        or auth["deadline_ns"] != intent.deadline_ns
        or auth["deadline_ns"] > close_metadata_valid_until_ns
        or Fraction(intent.quantity) % Fraction(close_quantity_step) != 0
    ):
        raise ContractValidationError("roll-exit CLOSE intent differs from source facts")

    prior_rows = tuple(_thaw(item) for item in source["prior_resolved_closes"])
    prior_actions = tuple(
        item
        for item in previous_actions
        if item.source.get("source_leg_id") == source_leg.leg_id and item.resolved
    )
    if len(prior_rows) != len(prior_actions):
        raise ContractValidationError("roll-exit prior CLOSE set is incomplete")
    expected_prior = []
    for prior in prior_actions:
        # Only a fully receipt-backed close can reduce the source exposure for
        # a later action.  Terminal cancel/reject and unresolved fee history
        # remain fail-closed; an assertion here would turn malformed durable
        # history into an implementation exception instead of a domain reject.
        if (
            prior.observation is None
            or prior.observation.get("phase") != "RESOLVED"
            or prior.observation.get("close_state") != ExecutionState.FILLED.value
            or prior.receipt is None
            or prior.fee is None
            or prior.receipt.claim.intent_id != prior.risk_intent_id
            or prior.receipt.requested_quantity != prior.intent.quantity
            or prior.receipt.filled_quantity != prior.intent.quantity
            or prior.fee.intent_id != prior.action_id
            or prior.fee.journal_revision != prior.receipt.journal_revision
        ):
            raise ContractValidationError(
                "prior roll-exit action lacks a complete terminal CLOSE receipt"
            )
        prior_source_digest = payload_sha256(dict(prior.source))
        prior_req_fingerprint = payload_sha256(dict(prior.request))
        expected_prior.append(
            {
                "schema": "group-roll-exit-prior-close-v1",
                "action_id": prior.action_id,
                "intent_id": prior.action_id,
                "client_order_id": prior.authorization["client_order_id"],
                "source_digest": prior_source_digest,
                "request_fingerprint": prior_req_fingerprint,
                "authorization_sha256": prior.auth_digest,
                "receipt_sha256": prior.receipt.receipt_sha256,
                "resolution_sha256": prior.receipt.exposure_proof_sha256,
                "closing_side": prior.intent.side.value,
                "instrument": prior.intent.instrument,
                "metadata_version": prior.intent.metadata_version,
                "requested_quantity": format(prior.intent.quantity, "f"),
                "filled_quantity": format(prior.receipt.filled_quantity, "f"),
                "terminal_state": ExecutionState.FILLED.value,
                "order_revision": prior.receipt.order_revision,
            }
        )
    if [_thaw(item) for item in prior_rows] != expected_prior:
        raise ContractValidationError("roll-exit prior CLOSE rows do not match durable actions")
    prior_filled = sum(
        (Fraction(item.receipt.filled_quantity) for item in prior_actions if item.receipt),
        Fraction(0),
    )
    if Fraction(source_status.filled_quantity) - prior_filled != Fraction(remaining):
        raise ContractValidationError("roll-exit remaining quantity is inconsistent")

    return _Action(
        authorization=auth,
        auth_digest=auth_digest,
        source=source,
        request=request,
        intent=intent,
        action_id=action_id,
        risk_intent_id=risk_intent_id,
    )


def _parse_observation(action: _Action, payload: object) -> tuple[Mapping[str, Any], GroupRollExitRiskCloseReceiptV1 | None, GroupRollExitCloseFeeEvidenceV1 | None]:
    observation = _mapping(payload, _OBSERVATION_KEYS, "roll-exit observation")
    if (
        observation.get("schema") != "group-roll-exit-observation-v1"
        or observation.get("action_id") != action.action_id
        or observation.get("intent_id") != action.intent.intent_id
        or observation.get("source_digest") != payload_sha256(dict(action.source))
        or observation.get("request_fingerprint") != payload_sha256(dict(action.request))
        or observation.get("authorization_sha256") != action.auth_digest
        or observation.get("risk_intent_id") != action.risk_intent_id
        or observation.get("phase") not in _OBSERVATION_PHASES
    ):
        raise ContractValidationError("roll-exit observation is not bound to its authorization")
    requested = _parse_decimal(observation.get("requested_quantity"), "requested_quantity")
    filled = _parse_decimal(observation.get("filled_quantity"), "filled_quantity")
    if requested != action.intent.quantity or filled < 0 or filled > requested:
        raise ContractValidationError("roll-exit observation quantity differs from its intent")
    state_raw = observation.get("close_state")
    try:
        state = None if state_raw is None else ExecutionState(state_raw)
    except (TypeError, ValueError):
        raise ContractValidationError("invalid roll-exit observation state") from None
    revision = observation.get("order_revision")
    if revision is not None and (type(revision) is not int or revision <= 0):
        raise ContractValidationError("invalid roll-exit observation revision")
    phase = observation["phase"]
    if phase == "PREPARED":
        valid = state is None and filled == 0 and revision is None
    elif phase == "SUBMIT_STARTED":
        valid = state in {None, ExecutionState.PENDING_ADMISSION, ExecutionState.PENDING_DISPATCH, ExecutionState.DISPATCHING} and filled == 0
    elif phase == "STATUS_OBSERVED":
        valid = state in _PENDING_STATES and revision is not None
        valid = valid and ((state is ExecutionState.PARTIALLY_FILLED and 0 < filled < requested) or (state is not ExecutionState.PARTIALLY_FILLED and filled == 0))
    elif phase == "UNKNOWN":
        valid = state is ExecutionState.UNKNOWN
    else:
        valid = state in _TERMINAL and revision is not None
    if not valid:
        raise ContractValidationError("roll-exit observation phase/state is inconsistent")
    receipt_raw = observation["close_receipt"]
    fee_raw = observation["close_fee_evidence"]
    receipt = None
    fee = None
    if phase == "RESOLVED" and state is ExecutionState.FILLED:
        if receipt_raw is None or fee_raw is None or filled != requested:
            raise ContractValidationError("filled CLOSE lacks receipt or fee evidence")
        receipt = GroupRollExitRiskCloseReceiptV1.from_payload(_thaw(receipt_raw))
        fee = _risk_close_fee_from_payload(_thaw(fee_raw))
        if (
            receipt.claim.intent_id != action.risk_intent_id
            or receipt.requested_quantity != requested
            or receipt.filled_quantity != filled
            or receipt.order_revision != revision
            or observation.get("close_receipt_sha256") != receipt.receipt_sha256
            or fee.intent_id != action.action_id
            or fee.journal_revision != receipt.journal_revision
            or len(fee.fills) != receipt.trade_count
        ):
            raise ContractValidationError("Risk close receipt or fill fees do not match observation")
    else:
        if receipt_raw is not None or fee_raw is not None:
            raise ContractValidationError("non-filled observation cannot carry close proof")
        if observation.get("close_receipt_sha256") is not None:
            raise ContractValidationError("observation without close proof carries receipt digest")
        if phase != "RESOLVED" and state in _TERMINAL:
            raise ContractValidationError("terminal order state requires a resolved event")
    return observation, receipt, fee


def _fold_roll_events(snapshot: GroupOrderSnapshot, events: tuple[GroupOrderEvent, ...]) -> dict[str, _Action]:
    actions: dict[str, _Action] = {}
    if len(events) > snapshot.revision + 1 or len(events) > 4097:
        raise ContractValidationError("roll-exit group event history is inconsistent")
    for index, event in enumerate(events):
        kind = event.event_type
        payload = event.payload
        if kind not in ROLL_EXIT_EVENT_TYPES:
            continue
        if kind == "GROUP_ROLL_EXIT_PLAN_RECORDED":
            if set(payload) != {"action_id", "authorization_sha256", "authorization"}:
                raise ContractValidationError("invalid roll-exit plan event")
            action = _validate_authorization(
                snapshot,
                payload["authorization"],
                plan_revision=index - 1,
                previous_actions=tuple(actions.values()),
            )
            if (
                payload.get("action_id") != action.action_id
                or payload.get("authorization_sha256") != action.auth_digest
                or action.action_id in actions
                or len(actions) >= _MAX_ACTIONS
                or any(not previous.resolved for previous in actions.values())
            ):
                raise ContractValidationError("duplicate, concurrent, or mismatched roll-exit plan")
            actions[action.action_id] = action
        elif kind == "GROUP_ROLL_EXIT_SUBMIT_STARTED":
            if set(payload) != {"action_id", "authorization_sha256", "risk_intent_id", "intent_id", "client_order_id"}:
                raise ContractValidationError("invalid roll-exit submit-start event")
            action = actions.get(payload.get("action_id"))
            if action is None or action.phase not in {"PLANNED", "PREPARED"} or action.resolved:
                raise ContractValidationError("roll-exit submit start has no unique plan")
            if (
                payload.get("authorization_sha256") != action.auth_digest
                or payload.get("risk_intent_id") != action.risk_intent_id
                or payload.get("intent_id") != action.intent.intent_id
                or payload.get("client_order_id") != action.authorization.get("client_order_id")
            ):
                raise ContractValidationError("roll-exit submit start differs from its plan")
            action.phase = "STARTED"
        elif kind == "GROUP_ROLL_EXIT_STATUS_OBSERVED":
            if set(payload) != {"action_id", "authorization_sha256", "risk_intent_id", "observation"}:
                raise ContractValidationError("invalid roll-exit status event")
            action = actions.get(payload.get("action_id"))
            if action is None or action.resolved:
                raise ContractValidationError("roll-exit status has no active plan")
            if payload.get("authorization_sha256") != action.auth_digest or payload.get("risk_intent_id") != action.risk_intent_id:
                raise ContractValidationError("roll-exit status identity mismatch")
            observation, receipt, fee = _parse_observation(action, payload["observation"])
            if observation["phase"] == "RESOLVED":
                raise ContractValidationError("terminal roll-exit result requires RESOLVED event")
            if observation["phase"] == "PREPARED":
                if action.phase not in {"PLANNED", "PREPARED"}:
                    raise ContractValidationError("prepared status regressed after submit start")
                action.phase = "PREPARED"
            else:
                if action.phase not in {"STARTED", "OBSERVED"}:
                    raise ContractValidationError("roll-exit status precedes durable submit start")
                previous = action.observation
                if previous is not None:
                    _validate_observation_progress(previous, observation)
                action.phase = "OBSERVED"
            action.observation = observation
            action.receipt = receipt
            action.fee = fee
        elif kind == "GROUP_ROLL_EXIT_RESOLVED":
            if set(payload) != {"action_id", "authorization_sha256", "risk_intent_id", "observation", "risk_receipt", "close_fee_evidence"}:
                raise ContractValidationError("invalid roll-exit resolved event")
            action = actions.get(payload.get("action_id"))
            if action is None or action.resolved or action.phase not in {"STARTED", "OBSERVED"}:
                raise ContractValidationError("roll-exit resolution has no started action")
            if payload.get("authorization_sha256") != action.auth_digest or payload.get("risk_intent_id") != action.risk_intent_id:
                raise ContractValidationError("roll-exit resolution identity mismatch")
            observation, receipt, fee = _parse_observation(action, payload["observation"])
            if observation["phase"] != "RESOLVED":
                raise ContractValidationError("roll-exit resolution requires terminal observation")
            if (None if receipt is None else receipt.to_payload()) != _thaw(payload["risk_receipt"]):
                raise ContractValidationError("roll-exit event Risk receipt differs from observation")
            event_fee_raw = payload["close_fee_evidence"]
            event_fee = (
                None
                if event_fee_raw is None
                else _risk_close_fee_from_payload(_thaw(event_fee_raw))
            )
            if (None if fee is None else fee.to_payload()) != (
                None if event_fee is None else event_fee.to_payload()
            ):
                raise ContractValidationError("roll-exit event fee evidence differs from observation")
            if action.observation is not None:
                _validate_observation_progress(action.observation, observation)
            action.phase = "RESOLVED"
            action.resolved = True
            action.observation = observation
            action.receipt = receipt
            action.fee = fee
    return actions


def _validate_observation_progress(previous: Mapping[str, Any], current: Mapping[str, Any]) -> None:
    prev_phase = previous["phase"]
    phase = current["phase"]
    phase_rank = {"PREPARED": 0, "SUBMIT_STARTED": 1, "STATUS_OBSERVED": 2, "UNKNOWN": 2, "RESOLVED": 3}
    if phase_rank.get(phase, -1) < phase_rank.get(prev_phase, -1):
        raise ContractValidationError("roll-exit observation phase regressed")
    prev_revision = previous.get("order_revision")
    revision = current.get("order_revision")
    if prev_revision is not None and revision is not None and revision < prev_revision:
        raise ContractValidationError("roll-exit order revision regressed")
    previous_fill = Fraction(_parse_decimal(previous["filled_quantity"], "filled_quantity"))
    current_fill = Fraction(_parse_decimal(current["filled_quantity"], "filled_quantity"))
    if current_fill < previous_fill:
        raise ContractValidationError("roll-exit fill quantity regressed")
    prev_state = previous.get("close_state")
    state = current.get("close_state")
    if prev_revision is not None and revision == prev_revision and (
        previous.get("close_state") != current.get("close_state")
        or previous.get("filled_quantity") != current.get("filled_quantity")
    ):
        raise ContractValidationError("roll-exit order changed without a new revision")
    allowed = {
        # A query after a durably started command may first observe a terminal
        # fill.  The terminal observation still needs the typed full-fill Risk
        # receipt and fee evidence enforced by _parse_observation.
        None: set(_PENDING_STATES) | _TERMINAL | {ExecutionState.UNKNOWN},
        ExecutionState.PENDING_ADMISSION: {
            ExecutionState.PENDING_ADMISSION,
            ExecutionState.PENDING_DISPATCH,
            ExecutionState.DISPATCHING,
            ExecutionState.UNKNOWN,
        },
        ExecutionState.PENDING_DISPATCH: {
            ExecutionState.PENDING_DISPATCH,
            ExecutionState.DISPATCHING,
            ExecutionState.ACKED,
            ExecutionState.PARTIALLY_FILLED,
            ExecutionState.FILLED,
            ExecutionState.CANCELLED,
            ExecutionState.REJECTED,
            ExecutionState.UNKNOWN,
        },
        ExecutionState.DISPATCHING: {
            ExecutionState.DISPATCHING,
            ExecutionState.ACKED,
            ExecutionState.PARTIALLY_FILLED,
            ExecutionState.FILLED,
            ExecutionState.CANCELLED,
            ExecutionState.REJECTED,
            ExecutionState.UNKNOWN,
        },
        ExecutionState.ACKED: {
            ExecutionState.ACKED,
            ExecutionState.PARTIALLY_FILLED,
            ExecutionState.FILLED,
            ExecutionState.CANCELLED,
            ExecutionState.REJECTED,
            ExecutionState.UNKNOWN,
        },
        ExecutionState.PARTIALLY_FILLED: {
            ExecutionState.PARTIALLY_FILLED,
            ExecutionState.FILLED,
            ExecutionState.CANCELLED,
            ExecutionState.UNKNOWN,
        },
        ExecutionState.UNKNOWN: {
            ExecutionState.UNKNOWN,
            ExecutionState.ACKED,
            ExecutionState.PARTIALLY_FILLED,
            ExecutionState.FILLED,
            ExecutionState.CANCELLED,
            ExecutionState.REJECTED,
        },
    }
    if prev_state is not None:
        try:
            previous_state = ExecutionState(prev_state)
            current_state = None if state is None else ExecutionState(state)
        except (TypeError, ValueError):
            raise ContractValidationError("invalid roll-exit state progression") from None
        if current_state not in allowed.get(previous_state, set()):
            raise ContractValidationError("roll-exit state regressed")
    elif state is not None and state not in {
        item.value for item in allowed[None]
    }:
        raise ContractValidationError("roll-exit state regressed after partial fill")


def validate_roll_exit_event_transition(
    snapshot: GroupOrderSnapshot,
    event_type: str,
    event_payload: Mapping[str, Any],
    history: tuple[GroupOrderEvent, ...],
    target_state: GroupOrderState,
) -> None:
    """Validate one lifecycle append before the store's revision CAS."""

    if event_type not in _ROLL_EVENT_TRANSITIONS:
        raise ContractValidationError("unsupported roll-exit event")
    if len(history) != snapshot.revision + 1:
        raise ContractValidationError("roll-exit event history is stale")
    if (snapshot.state, target_state) not in _ROLL_EVENT_TRANSITIONS[event_type]:
        raise ContractValidationError("invalid roll-exit state transition")
    if snapshot.request.version not in {1, 2} or len(snapshot.request.legs) != 2:
        raise ContractValidationError("roll exit only supports accepted v1/v2 groups")
    _fold_roll_events(snapshot, history)
    if event_type != "GROUP_ROLL_EXIT_CLOSED":
        validate_roll_exit_lifecycle_payload(snapshot, event_type, event_payload, history)


def validate_roll_exit_lifecycle_payload(
    snapshot: GroupOrderSnapshot,
    event_type: str,
    event_payload: Mapping[str, Any],
    history: tuple[GroupOrderEvent, ...],
) -> None:
    """Validate the newly appended plan/start/status/resolve event."""

    prefix = _fold_roll_events(snapshot, history)
    event_revision = snapshot.revision + 1
    # The pending event is folded using its exact revision index.  Construct a
    # light immutable record; sequence is irrelevant to action semantics.
    pending = GroupOrderEvent(
        sequence=(history[-1].sequence + 1),
        group_id=snapshot.request.group_id,
        event_type=event_type,
        created_at_ns=snapshot.updated_at_ns + 1,
        payload=dict(event_payload),
    )
    # Re-run the small phase machine with pending event so it is checked by the
    # same path as durable records. The snapshot revision bound is relaxed here
    # only for this in-memory event.
    _fold_roll_events_for_pending(snapshot, history, pending, event_revision, prefix)


def _fold_roll_events_for_pending(
    snapshot: GroupOrderSnapshot,
    history: tuple[GroupOrderEvent, ...],
    pending: GroupOrderEvent,
    event_revision: int,
    prior: dict[str, _Action],
) -> None:
    kind, payload = pending.event_type, pending.payload
    if kind == "GROUP_ROLL_EXIT_PLAN_RECORDED":
        if set(payload) != {"action_id", "authorization_sha256", "authorization"}:
            raise ContractValidationError("invalid roll-exit plan event")
        action = _validate_authorization(
            snapshot,
            payload["authorization"],
            plan_revision=event_revision - 1,
            previous_actions=tuple(prior.values()),
        )
        if (
            payload.get("action_id") != action.action_id
            or payload.get("authorization_sha256") != action.auth_digest
            or action.action_id in prior
            or len(prior) >= _MAX_ACTIONS
            or any(not previous.resolved for previous in prior.values())
        ):
            raise ContractValidationError("duplicate or mismatched roll-exit plan")
        return
    action_id = payload.get("action_id") if isinstance(payload, Mapping) else None
    action = prior.get(action_id)
    if action is None:
        raise ContractValidationError("roll-exit event has no durable action plan")
    # Use the same folding rules by placing a shallow copy of the current
    # action map plus the pending event into a tiny independent pass.
    pending_history = tuple(history) + (pending,)
    # The full history's revision count must be exact; the pending event is the
    # one extra row, so validate action semantics without changing snapshot.
    _fold_pending_tail(prior, kind, payload)


def _fold_pending_tail(actions: dict[str, _Action], kind: str, payload: Mapping[str, Any]) -> None:
    action = actions.get(payload.get("action_id"))
    if kind == "GROUP_ROLL_EXIT_SUBMIT_STARTED":
        if set(payload) != {"action_id", "authorization_sha256", "risk_intent_id", "intent_id", "client_order_id"} or action is None or action.phase not in {"PLANNED", "PREPARED"}:
            raise ContractValidationError("invalid or duplicate roll-exit submit start")
        if payload.get("authorization_sha256") != action.auth_digest or payload.get("risk_intent_id") != action.risk_intent_id or payload.get("intent_id") != action.intent.intent_id or payload.get("client_order_id") != action.authorization.get("client_order_id"):
            raise ContractValidationError("roll-exit submit start differs from its plan")
        return
    if kind == "GROUP_ROLL_EXIT_STATUS_OBSERVED":
        if set(payload) != {"action_id", "authorization_sha256", "risk_intent_id", "observation"} or action is None or action.resolved:
            raise ContractValidationError("invalid roll-exit status event")
        if payload.get("authorization_sha256") != action.auth_digest or payload.get("risk_intent_id") != action.risk_intent_id:
            raise ContractValidationError("roll-exit status identity mismatch")
        observation, _, _ = _parse_observation(action, payload["observation"])
        if observation["phase"] == "RESOLVED":
            raise ContractValidationError("terminal roll-exit result requires RESOLVED event")
        if observation["phase"] == "PREPARED":
            if action.phase not in {"PLANNED", "PREPARED"}:
                raise ContractValidationError("prepared status regressed after submit start")
        else:
            if action.phase not in {"STARTED", "OBSERVED"}:
                raise ContractValidationError("roll-exit status precedes durable submit start")
            if action.observation is not None:
                _validate_observation_progress(action.observation, observation)
        return
    if kind == "GROUP_ROLL_EXIT_RESOLVED":
        if set(payload) != {"action_id", "authorization_sha256", "risk_intent_id", "observation", "risk_receipt", "close_fee_evidence"} or action is None or action.resolved or action.phase not in {"STARTED", "OBSERVED"}:
            raise ContractValidationError("invalid roll-exit resolved event")
        if payload.get("authorization_sha256") != action.auth_digest or payload.get("risk_intent_id") != action.risk_intent_id:
            raise ContractValidationError("roll-exit resolution identity mismatch")
        observation, receipt, fee = _parse_observation(action, payload["observation"])
        if observation["phase"] != "RESOLVED":
            raise ContractValidationError("roll-exit resolution requires terminal observation")
        if (None if receipt is None else receipt.to_payload()) != _thaw(payload["risk_receipt"]):
            raise ContractValidationError("roll-exit event Risk receipt differs from observation")
        event_fee_raw = payload["close_fee_evidence"]
        event_fee = (
            None
            if event_fee_raw is None
            else _risk_close_fee_from_payload(_thaw(event_fee_raw))
        )
        if (None if fee is None else fee.to_payload()) != (
            None if event_fee is None else event_fee.to_payload()
        ):
            raise ContractValidationError("roll-exit event fee evidence differs from observation")
        if action.observation is not None:
            _validate_observation_progress(action.observation, observation)
        return
    raise ContractValidationError("unsupported roll-exit event")


def validate_roll_exit_close(
    snapshot: GroupOrderSnapshot,
    payload: Mapping[str, Any],
    history: tuple[GroupOrderEvent, ...],
) -> GroupRollExitClosedProofV1:
    """Validate a complete proof before atomic aggregate state closure."""

    if set(payload) != {"schema", "proof", "proof_sha256"} or payload.get("schema") != "group-roll-exit-closed-event-v1":
        raise ContractValidationError("invalid aggregate roll-exit closed event")
    proof = GroupRollExitClosedProofV1.from_payload(_thaw(payload["proof"]))
    if (
        payload.get("proof_sha256") != proof.proof_sha256
        or snapshot.state not in {GroupOrderState.UNWINDING, GroupOrderState.RECOVERY_REQUIRED}
        or snapshot.request.version not in {1, 2}
        or len(snapshot.request.legs) != 2
        or proof.portfolio_ref != snapshot.request.portfolio_ref
        or proof.group_id != snapshot.request.group_id
        or proof.group_version != snapshot.request.version
        or proof.group_fingerprint != snapshot.request.fingerprint
        or proof.expected_revision != snapshot.revision
        or proof.prior_group_event_chain_sha256 != group_event_chain_sha256(snapshot, history)
    ):
        raise ContractValidationError("aggregate roll-exit proof is stale or not group-bound")
    if any(
        item.event_type == "GROUP_ROLL_EXIT_CLOSED" for item in history
    ):
        raise ContractValidationError("group already has a roll-exit close event")
    actions = _fold_roll_events(snapshot, history)
    if not actions or any(not action.resolved for action in actions.values()):
        raise ContractValidationError("roll-exit actions remain active or unresolved")

    progress_by_leg = {item.leg_id: item for item in snapshot.legs}
    expected_legs: list[GroupRollExitLegClosedV1] = []
    for plan, supplied in zip(snapshot.request.legs, proof.legs, strict=True):
        progress = progress_by_leg[plan.leg_id]
        child = progress.submitted_intent
        status = progress.child_status
        claim = progress.claim_receipt
        resolution = progress.resolution_receipt
        if (
            supplied.leg_id != plan.leg_id
            or child is None
            or status is None
            or claim is None
            or resolution is None
            or progress.phase.value != "OBSERVED"
            or child.position_effect is not PositionEffect.OPEN
            or status.state not in {ExecutionState.FILLED, ExecutionState.CANCELLED}
            or status.filled_quantity <= 0
            or status.trade_count is None
            or status.trade_count <= 0
            or status.cumulative_commission is None
            or status.cumulative_commission_currency is None
            or status.state is not resolution.final_state
            or status.filled_quantity != resolution.filled_quantity
            or resolution.resolution_kind != "TERMINAL_EXPOSURE_RESOLVED"
            or resolution.claim != claim
            or resolution.dispatch_latch_active
        ):
            raise ContractValidationError("group has an unproven source leg or fee")
        leg_actions = [action for action in actions.values() if action.source.get("source_leg_id") == plan.leg_id]
        if not leg_actions or any(
            action.receipt is None
            or action.fee is None
            or action.observation is None
            or action.observation.get("phase") != "RESOLVED"
            or action.observation.get("close_state") != ExecutionState.FILLED.value
            for action in leg_actions
        ):
            raise ContractValidationError("every source leg requires full receipt-backed CLOSEs")
        source_payload = leg_actions[0].source
        if any(
            action.source.get("source_intent_id") != child.intent_id
            or action.source.get("source_event_digest") != source_payload.get("source_event_digest")
            or action.source.get("source_claim") != source_payload.get("source_claim")
            or action.source.get("source_risk_resolution") != source_payload.get("source_risk_resolution")
            or action.source.get("source_filled_quantity") != source_payload.get("source_filled_quantity")
            for action in leg_actions
        ):
            raise ContractValidationError("roll-exit actions disagree on source facts")
        expected_actions = tuple(
            GroupRollExitCloseActionV1(
                action_id=action.action_id,
                authorization_sha256=action.auth_digest,
                risk_intent_id=action.risk_intent_id,
                receipt_sha256=action.receipt.receipt_sha256,
                close_fee_evidence_sha256=action.fee.evidence_sha256,
                close_quantity=action.receipt.filled_quantity,
                close_fee_amount=action.fee.fee_amount,
                close_fee_currency=action.fee.fee_currency,
            )
            for action in leg_actions
        )
        expected_legs.append(
            GroupRollExitLegClosedV1(
                leg_id=plan.leg_id,
                source_intent_id=child.intent_id,
                source_event_sha256=source_payload["source_event_digest"],
                source_claim_digest=claim.claim_digest,
                source_resolution_sha256=payload_sha256(resolution.to_payload()),
                source_filled_quantity=status.filled_quantity,
                source_fee=_source_fee(source_payload),
                close_actions=expected_actions,
            )
        )
    if tuple(item.to_payload() for item in expected_legs) != tuple(item.to_payload() for item in proof.legs):
        raise ContractValidationError("aggregate proof omits or alters source/close fee evidence")

    budget = snapshot.budget_reservation
    if snapshot.request.version == 1:
        if budget is not None or proof.budget_release is not None:
            raise ContractValidationError("v1 aggregate close cannot carry budget release")
    else:
        if budget is None or budget.state is not GroupBudgetReservationState.HELD:
            raise ContractValidationError("v2 aggregate close requires the live held budget")
        expected_budget = GroupRollExitBudgetReleaseV1(
            policy_fingerprint=budget.policy_fingerprint,
            quote_fingerprint=budget.quote_fingerprint,
            valuation_unit=budget.valuation_unit,
            gross_notional=budget.gross_notional,
            deadline_ns=budget.deadline_ns,
        )
        if proof.budget_release != expected_budget:
            raise ContractValidationError("v2 budget release proof differs from held quote")
    _validate_released_reservations(snapshot, history)
    expected_reservation_hashes = tuple(
        payload_sha256(item.to_payload()) for item in snapshot.reservations
    )
    if proof.reservation_sha256s != expected_reservation_hashes:
        raise ContractValidationError("aggregate proof does not bind released reservations")
    return proof


def _validate_released_reservations(
    snapshot: GroupOrderSnapshot, history: tuple[GroupOrderEvent, ...]
) -> None:
    request = snapshot.request
    if len(snapshot.reservations) != len(request.account_refs):
        raise ContractValidationError("group reservation set is incomplete")
    group_paired_positions = [
        index for index, event in enumerate(history) if event.event_type == "GROUP_PAIRED"
    ]
    if not group_paired_positions:
        raise ContractValidationError("roll-exit source group has no paired event")
    paired_index = group_paired_positions[-1]
    release_positions: dict[str, int] = {}
    released_accounts: list[str] = []
    last_resolved_index = max(
        (
            index
            for index, event in enumerate(history)
            if event.event_type == "GROUP_ROLL_EXIT_RESOLVED"
        ),
        default=-1,
    )
    for index, event in enumerate(history):
        if event.event_type == "RESERVATION_RELEASED":
            account = event.payload.get("account_ref")
            if (
                type(account) is not str
                or account not in request.account_refs
                or account in release_positions
            ):
                raise ContractValidationError("roll-exit reservation release is duplicate or foreign")
            marker = dict(event.payload)
            settled_marker = {"account_ref": account, "roll_exit_settled": True}
            settled_v2_marker = {
                **settled_marker,
                "budget_state": GroupBudgetReservationState.HELD.value,
            }
            if marker == settled_marker:
                if request.version != 1 or snapshot.budget_reservation is not None:
                    raise ContractValidationError("roll-exit reservation release budget marker is missing")
                if index <= last_resolved_index:
                    raise ContractValidationError("roll-exit reservation release is premature")
            elif marker == settled_v2_marker:
                budget = snapshot.budget_reservation
                if (
                    request.version != 2
                    or budget is None
                    or budget.state is not GroupBudgetReservationState.HELD
                ):
                    raise ContractValidationError("roll-exit reservation release budget marker is invalid")
                if index <= last_resolved_index:
                    raise ContractValidationError("roll-exit reservation release is premature")
            elif marker in (
                {"account_ref": account},
                {"account_ref": account, "already_terminal": True},
            ):
                # An accepted group may already have a durable post-PAIRED
                # release event before roll-exit begins.  The typed snapshot
                # below must still show every claimed source leg SETTLED.
                if index <= paired_index:
                    raise ContractValidationError("group reservation released before pairing")
            else:
                raise ContractValidationError("roll-exit reservation release event is malformed")
            release_positions[account] = index
            released_accounts.append(account)
    if tuple(released_accounts) != request.account_refs:
        raise ContractValidationError("roll-exit reservation releases are incomplete or unordered")
    expected_legs: dict[str, str] = {}
    for leg in request.legs:
        expected_legs[leg.leg_id] = leg.account_ref
    for reservation in snapshot.reservations:
        if (
            reservation.phase is not GroupReservationPhase.RELEASED
            or reservation.group_id != request.group_id
            or reservation.group_fingerprint is None
            or reservation.deadline_ns != request.deadline_ns
            or reservation.account_ref not in request.account_refs
            or reservation.account_ref not in release_positions
        ):
            raise ContractValidationError("account reservation is not durably released")
        if request.version == 2 and reservation.policy_fingerprint != snapshot.budget_reservation.policy_fingerprint:
            raise ContractValidationError("account reservation policy differs from budget hold")
        expected_ids = {leg_id for leg_id, account in expected_legs.items() if account == reservation.account_ref}
        if {item.leg_id for item in reservation.legs} != expected_ids:
            raise ContractValidationError("released reservation leg membership is incomplete")
        for risk_leg in reservation.legs:
            if not risk_leg.claimed or not risk_leg.resolved or risk_leg.state != "SETTLED":
                raise ContractValidationError("source Risk reservation remains active or unresolved")
            progress = next(item for item in snapshot.legs if item.leg_id == risk_leg.leg_id)
            claim = progress.claim_receipt
            if (
                progress.submitted_intent is None
                or claim is None
                or risk_leg.intent_id != progress.submitted_intent.intent_id
                or risk_leg.actual_intent_hash != claim.actual_child_intent_hash
                or claim.permit.group_id != request.group_id
                or claim.permit.leg_id != risk_leg.leg_id
                or claim.permit.portfolio_ref != request.portfolio_ref
                or claim.permit.coordinator_principal_ref != request.coordinator_principal_ref
                or claim.permit.strategy_id != request.strategy_id
                or claim.permit.account_ref != reservation.account_ref
                or claim.permit.group_fingerprint != reservation.group_fingerprint
                or claim.permit.policy_fingerprint != reservation.policy_fingerprint
                or claim.permit.deadline_ns != request.deadline_ns
                or claim.permit.intent_id != progress.submitted_intent.intent_id
                or claim.permit.order_intent_hash != progress.submitted_intent.fingerprint
                or risk_leg.permit_id != claim.permit.permit_id
                or risk_leg.reserved_intent_hash != claim.permit.reserved_intent_hash
                or risk_leg.exposure_profile_fingerprint != claim.permit.exposure_profile_fingerprint
            ):
                raise ContractValidationError("released Risk reservation differs from submitted source")


def _proven_group_roll_exit_close(
    snapshot: GroupOrderSnapshot,
    payload: Mapping[str, Any],
    history: tuple[GroupOrderEvent, ...],
) -> bool:
    try:
        validate_roll_exit_close(snapshot, payload, history)
        return True
    except (ContractValidationError, TypeError, ValueError, KeyError, ArithmeticError):
        return False


__all__ = [
    "ROLL_EXIT_EVENT_TYPES",
    "group_event_chain_sha256",
    "validate_roll_exit_close",
    "validate_roll_exit_event_transition",
]
