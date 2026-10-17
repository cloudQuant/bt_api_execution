"""Durable proof contracts for a source-bound two-leg group roll exit.

These contracts deliberately describe the aggregate close boundary, not the
Risk-side command protocol.  The Risk receipt and its fill-fee evidence remain
separate values because the terminal Risk close receipt does not contain fees.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Any

from .contracts import payload_sha256
from .errors import ContractValidationError
from .group_order_contracts import _parse_decimal, _reference, _require_digest

_MAX_ROLL_EXIT_ACTIONS = 64
_MAX_ROLL_EXIT_FEE_FILLS = 4096


def _currency(value: object, name: str) -> str:
    if type(value) is not str or not value or len(value) > 16 or not value.isascii():
        raise ContractValidationError(f"invalid {name}")
    if not value[0].isalpha() or not value[0].isupper() or any(
        not (character.isupper() or character.isdigit() or character in "._-")
        for character in value
    ):
        raise ContractValidationError(f"invalid {name}")
    return value


def _signed_decimal(value: object, name: str) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise ContractValidationError(f"invalid {name}")
    return value


def _exact_decimal_sum(values: tuple[Decimal, ...]) -> Decimal:
    """Sum bounded decimal coefficients without ambient-context rounding."""

    total = sum((Fraction(value) for value in values), Fraction(0))
    if total.denominator == 1:
        return Decimal(total.numerator)
    denominator = total.denominator
    twos = fives = 0
    while denominator % 2 == 0:
        twos += 1
        denominator //= 2
    while denominator % 5 == 0:
        fives += 1
        denominator //= 5
    if denominator != 1:
        raise ContractValidationError("fee sum is not a finite decimal")
    places = max(twos, fives)
    scaled = total.numerator * (10**places) // total.denominator
    sign = "-" if scaled < 0 else ""
    digits = str(abs(scaled))
    if places:
        digits = digits.rjust(places + 1, "0")
        text = f"{sign}{digits[:-places]}.{digits[-places:]}"
    else:
        text = sign + digits
    result = Decimal(text)
    if Fraction(result) != total:
        raise ContractValidationError("fee sum cannot be represented exactly")
    return result


@dataclass(frozen=True, slots=True)
class GroupRollExitSourceFeeEvidenceV1:
    """Explicit source-order commission bound to its Risk group event."""

    intent_id: str
    amount: Decimal
    currency: str
    source_event_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "intent_id", _reference(self.intent_id, "intent_id"))
        object.__setattr__(self, "amount", _signed_decimal(self.amount, "source fee amount"))
        object.__setattr__(self, "currency", _currency(self.currency, "source fee currency"))
        _require_digest(self.source_event_sha256, "source_event_sha256")

    def _payload_without_digest(self) -> dict[str, Any]:
        return {
            "schema": "group-roll-exit-source-fee-v1",
            "intent_id": self.intent_id,
            "amount": format(self.amount, "f"),
            "currency": self.currency,
            "source_event_sha256": self.source_event_sha256,
        }

    @property
    def evidence_sha256(self) -> str:
        return payload_sha256(self._payload_without_digest())

    def to_payload(self) -> dict[str, Any]:
        return {**self._payload_without_digest(), "evidence_sha256": self.evidence_sha256}

    @classmethod
    def from_payload(cls, value: object) -> GroupRollExitSourceFeeEvidenceV1:
        expected = {
            "schema", "intent_id", "amount", "currency", "source_event_sha256", "evidence_sha256"
        }
        if (
            not isinstance(value, Mapping)
            or set(value) != expected
            or value.get("schema") != "group-roll-exit-source-fee-v1"
        ):
            raise ContractValidationError("invalid group roll-exit source fee evidence")
        result = cls(
            intent_id=value["intent_id"],
            amount=_parse_decimal(value["amount"], "source fee amount"),
            currency=value["currency"],
            source_event_sha256=value["source_event_sha256"],
        )
        if value["evidence_sha256"] != result.evidence_sha256:
            raise ContractValidationError("group roll-exit source fee digest mismatch")
        return result


@dataclass(frozen=True, slots=True)
class GroupRollExitCloseFeeFillV1:
    """One Risk-journal-verified fill row contributing a signed fee."""

    fill_id: str
    fill_fingerprint: str
    source_row_sha256: str
    amount: Decimal
    currency: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "fill_id", _reference(self.fill_id, "fill_id"))
        _require_digest(self.fill_fingerprint, "fill_fingerprint")
        _require_digest(self.source_row_sha256, "source_row_sha256")
        object.__setattr__(self, "amount", _signed_decimal(self.amount, "close fill fee"))
        object.__setattr__(self, "currency", _currency(self.currency, "close fill fee currency"))

    def to_payload(self) -> dict[str, Any]:
        return {
            "fill_id": self.fill_id,
            "fill_fingerprint": self.fill_fingerprint,
            "source_row_sha256": self.source_row_sha256,
            "amount": format(self.amount, "f"),
            "currency": self.currency,
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupRollExitCloseFeeFillV1:
        if not isinstance(value, Mapping) or set(value) != {
            "fill_id", "fill_fingerprint", "source_row_sha256", "amount", "currency"
        }:
            raise ContractValidationError("invalid group roll-exit close fee fill")
        return cls(
            fill_id=value["fill_id"],
            fill_fingerprint=value["fill_fingerprint"],
            source_row_sha256=value["source_row_sha256"],
            amount=_parse_decimal(value["amount"], "close fill fee"),
            currency=value["currency"],
        )


@dataclass(frozen=True, slots=True)
class GroupRollExitCloseFeeEvidenceV1:
    """Signed close commission reconstructed from a verified Risk fill event."""

    intent_id: str
    journal_revision: int
    fill_observation_sha256: str
    fee_amount: Decimal
    fee_currency: str
    fills: tuple[GroupRollExitCloseFeeFillV1, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "intent_id", _reference(self.intent_id, "intent_id"))
        if type(self.journal_revision) is not int or self.journal_revision <= 0:
            raise ContractValidationError("invalid close fee journal revision")
        _require_digest(self.fill_observation_sha256, "fill_observation_sha256")
        object.__setattr__(self, "fee_amount", _signed_decimal(self.fee_amount, "close fee amount"))
        object.__setattr__(self, "fee_currency", _currency(self.fee_currency, "close fee currency"))
        if (
            type(self.fills) is not tuple
            or not 1 <= len(self.fills) <= _MAX_ROLL_EXIT_FEE_FILLS
            or any(type(item) is not GroupRollExitCloseFeeFillV1 for item in self.fills)
            or len({item.fill_id for item in self.fills}) != len(self.fills)
            or any(item.currency != self.fee_currency for item in self.fills)
            or _exact_decimal_sum(tuple(item.amount for item in self.fills)) != self.fee_amount
        ):
            raise ContractValidationError("close fee evidence is incomplete or inconsistent")

    def _payload_without_digest(self) -> dict[str, Any]:
        return {
            "schema": "group-roll-exit-close-fee-v1",
            "intent_id": self.intent_id,
            "journal_revision": self.journal_revision,
            "fill_observation_sha256": self.fill_observation_sha256,
            "fee_amount": format(self.fee_amount, "f"),
            "fee_currency": self.fee_currency,
            "fills": [item.to_payload() for item in self.fills],
        }

    @property
    def evidence_sha256(self) -> str:
        return payload_sha256(self._payload_without_digest())

    def to_payload(self) -> dict[str, Any]:
        return {**self._payload_without_digest(), "evidence_sha256": self.evidence_sha256}

    @classmethod
    def from_payload(cls, value: object) -> GroupRollExitCloseFeeEvidenceV1:
        expected = {
            "schema", "intent_id", "journal_revision", "fill_observation_sha256",
            "fee_amount", "fee_currency", "fills", "evidence_sha256",
        }
        if (
            not isinstance(value, Mapping)
            or set(value) != expected
            or value.get("schema") != "group-roll-exit-close-fee-v1"
            or type(value["fills"]) not in (list, tuple)
            or len(value["fills"]) > _MAX_ROLL_EXIT_FEE_FILLS
        ):
            raise ContractValidationError("invalid group roll-exit close fee evidence")
        result = cls(
            intent_id=value["intent_id"],
            journal_revision=value["journal_revision"],
            fill_observation_sha256=value["fill_observation_sha256"],
            fee_amount=_parse_decimal(value["fee_amount"], "close fee amount"),
            fee_currency=value["fee_currency"],
            fills=tuple(GroupRollExitCloseFeeFillV1.from_payload(item) for item in value["fills"]),
        )
        if value["evidence_sha256"] != result.evidence_sha256:
            raise ContractValidationError("group roll-exit close fee digest mismatch")
        return result


@dataclass(frozen=True, slots=True)
class GroupRollExitRiskCloseClaimV1:
    """Exact claim projection embedded in a terminal Risk CLOSE receipt."""

    cause_id: str
    claim_digest: str
    claimed_at: float
    intent_hash: str
    intent_id: str
    permit_id: str
    scope_key: str

    def __post_init__(self) -> None:
        for name in ("cause_id", "intent_id", "permit_id"):
            value = getattr(self, name)
            if type(value) is not str or not value or value != value.strip():
                raise ContractValidationError(f"invalid Risk close claim {name}")
        if self.cause_id != "dispatch-inflight:" + self.intent_id:
            raise ContractValidationError("Risk close claim cause does not match intent")
        _require_digest(self.claim_digest, "claim_digest")
        _require_digest(self.intent_hash, "intent_hash")
        if (
            type(self.claimed_at) not in (int, float)
            or type(self.claimed_at) is bool
            or not math.isfinite(self.claimed_at)
        ):
            raise ContractValidationError("invalid Risk close claim timestamp")
        if (
            type(self.scope_key) is not str
            or not self.scope_key
            or len(self.scope_key) > 2048
            or not self.scope_key.isascii()
            or any(ord(character) < 0x20 or ord(character) == 0x7F for character in self.scope_key)
        ):
            raise ContractValidationError("invalid Risk close claim scope key")

    def to_payload(self) -> dict[str, Any]:
        return {
            "cause_id": self.cause_id,
            "claim_digest": self.claim_digest,
            "claimed_at": self.claimed_at,
            "intent_hash": self.intent_hash,
            "intent_id": self.intent_id,
            "permit_id": self.permit_id,
            "scope_key": self.scope_key,
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupRollExitRiskCloseClaimV1:
        if not isinstance(value, Mapping) or set(value) != {
            "cause_id", "claim_digest", "claimed_at", "intent_hash", "intent_id", "permit_id", "scope_key"
        }:
            raise ContractValidationError("invalid Risk close claim payload")
        return cls(**value)


@dataclass(frozen=True, slots=True)
class GroupRollExitRiskCloseReceiptV1:
    """Typed, digest-checked Risk terminal CLOSE receipt carried into closure proof."""

    claim: GroupRollExitRiskCloseClaimV1
    proof_sha256: str
    exposure_proof_sha256: str
    requested_quantity: Decimal
    filled_quantity: Decimal
    trade_count: int
    order_revision: int
    journal_revision: int
    exposure_reservation_id: str
    exposure_reservation_sha256: str

    def __post_init__(self) -> None:
        if type(self.claim) is not GroupRollExitRiskCloseClaimV1:
            raise ContractValidationError("Risk close receipt requires exact claim")
        for name in ("proof_sha256", "exposure_proof_sha256", "exposure_reservation_sha256"):
            _require_digest(getattr(self, name), name)
        for name in ("requested_quantity", "filled_quantity"):
            value = getattr(self, name)
            if type(value) is not Decimal or not value.is_finite():
                raise ContractValidationError(f"invalid Risk close receipt {name}")
        if (
            self.requested_quantity <= 0
            or self.filled_quantity != self.requested_quantity
            or type(self.trade_count) is not int
            or self.trade_count <= 0
            or type(self.order_revision) is not int
            or self.order_revision <= 0
            or type(self.journal_revision) is not int
            or self.journal_revision <= 0
        ):
            raise ContractValidationError("Risk close receipt is not a complete full-fill result")
        object.__setattr__(
            self,
            "exposure_reservation_id",
            _reference(self.exposure_reservation_id, "exposure_reservation_id"),
        )

    def _payload_without_digest(self) -> dict[str, Any]:
        return {
            "status": "TERMINAL_CLOSE_RESOLVED",
            "claim": self.claim.to_payload(),
            "proof_sha256": self.proof_sha256,
            "exposure_proof_sha256": self.exposure_proof_sha256,
            "final_state": "FILLED",
            "requested_quantity": format(self.requested_quantity, "f"),
            "filled_quantity": format(self.filled_quantity, "f"),
            "trade_count": self.trade_count,
            "order_revision": self.order_revision,
            "journal_revision": self.journal_revision,
            "exposure_reservation_id": self.exposure_reservation_id,
            "exposure_reservation_sha256": self.exposure_reservation_sha256,
            "dispatch_latch_active": False,
            "has_other_active_freezes": False,
            "position_effect": "CLOSE",
        }

    @property
    def receipt_sha256(self) -> str:
        return payload_sha256(self._payload_without_digest())

    def to_payload(self) -> dict[str, Any]:
        return {**self._payload_without_digest(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def from_payload(cls, value: object) -> GroupRollExitRiskCloseReceiptV1:
        expected = {
            "status", "claim", "proof_sha256", "exposure_proof_sha256", "final_state",
            "requested_quantity", "filled_quantity", "trade_count", "order_revision",
            "journal_revision", "exposure_reservation_id", "exposure_reservation_sha256",
            "dispatch_latch_active", "has_other_active_freezes", "position_effect", "receipt_sha256",
        }
        if (
            not isinstance(value, Mapping)
            or set(value) != expected
            or value.get("status") != "TERMINAL_CLOSE_RESOLVED"
            or value.get("final_state") != "FILLED"
            or value.get("position_effect") != "CLOSE"
            or value.get("dispatch_latch_active") is not False
            or value.get("has_other_active_freezes") is not False
        ):
            raise ContractValidationError("invalid terminal Risk CLOSE receipt")
        result = cls(
            claim=GroupRollExitRiskCloseClaimV1.from_payload(value["claim"]),
            proof_sha256=value["proof_sha256"],
            exposure_proof_sha256=value["exposure_proof_sha256"],
            requested_quantity=_parse_decimal(value["requested_quantity"], "requested_quantity"),
            filled_quantity=_parse_decimal(value["filled_quantity"], "filled_quantity"),
            trade_count=value["trade_count"],
            order_revision=value["order_revision"],
            journal_revision=value["journal_revision"],
            exposure_reservation_id=value["exposure_reservation_id"],
            exposure_reservation_sha256=value["exposure_reservation_sha256"],
        )
        if value["receipt_sha256"] != result.receipt_sha256:
            raise ContractValidationError("Risk close receipt digest mismatch")
        return result


@dataclass(frozen=True, slots=True)
class GroupRollExitCloseActionV1:
    """One resolved action reference in an aggregate group-close proof."""

    action_id: str
    authorization_sha256: str
    risk_intent_id: str
    receipt_sha256: str
    close_fee_evidence_sha256: str
    close_quantity: Decimal
    close_fee_amount: Decimal
    close_fee_currency: str

    def __post_init__(self) -> None:
        for name in ("action_id", "risk_intent_id"):
            object.__setattr__(self, name, _reference(getattr(self, name), name))
        for name in ("authorization_sha256", "receipt_sha256", "close_fee_evidence_sha256"):
            _require_digest(getattr(self, name), name)
        if type(self.close_quantity) is not Decimal or not self.close_quantity.is_finite() or self.close_quantity <= 0:
            raise ContractValidationError("invalid roll-exit close quantity")
        object.__setattr__(self, "close_fee_amount", _signed_decimal(self.close_fee_amount, "close fee amount"))
        object.__setattr__(self, "close_fee_currency", _currency(self.close_fee_currency, "close fee currency"))

    def to_payload(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "authorization_sha256": self.authorization_sha256,
            "risk_intent_id": self.risk_intent_id,
            "receipt_sha256": self.receipt_sha256,
            "close_fee_evidence_sha256": self.close_fee_evidence_sha256,
            "close_quantity": format(self.close_quantity, "f"),
            "close_fee_amount": format(self.close_fee_amount, "f"),
            "close_fee_currency": self.close_fee_currency,
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupRollExitCloseActionV1:
        if not isinstance(value, Mapping) or set(value) != {
            "action_id", "authorization_sha256", "risk_intent_id", "receipt_sha256",
            "close_fee_evidence_sha256", "close_quantity", "close_fee_amount", "close_fee_currency",
        }:
            raise ContractValidationError("invalid roll-exit close action reference")
        return cls(
            action_id=value["action_id"],
            authorization_sha256=value["authorization_sha256"],
            risk_intent_id=value["risk_intent_id"],
            receipt_sha256=value["receipt_sha256"],
            close_fee_evidence_sha256=value["close_fee_evidence_sha256"],
            close_quantity=_parse_decimal(value["close_quantity"], "close_quantity"),
            close_fee_amount=_parse_decimal(value["close_fee_amount"], "close_fee_amount"),
            close_fee_currency=value["close_fee_currency"],
        )


@dataclass(frozen=True, slots=True)
class GroupRollExitLegClosedV1:
    """Exact source fills, explicit fees, and resolved CLOSE actions for one leg."""

    leg_id: str
    source_intent_id: str
    source_event_sha256: str
    source_claim_digest: str
    source_resolution_sha256: str
    source_filled_quantity: Decimal
    source_fee: GroupRollExitSourceFeeEvidenceV1
    close_actions: tuple[GroupRollExitCloseActionV1, ...]

    def __post_init__(self) -> None:
        for name in ("leg_id", "source_intent_id"):
            object.__setattr__(self, name, _reference(getattr(self, name), name))
        for name in ("source_event_sha256", "source_claim_digest", "source_resolution_sha256"):
            _require_digest(getattr(self, name), name)
        if type(self.source_filled_quantity) is not Decimal or not self.source_filled_quantity.is_finite() or self.source_filled_quantity <= 0:
            raise ContractValidationError("roll-exit source exposure must be positive")
        if (
            type(self.source_fee) is not GroupRollExitSourceFeeEvidenceV1
            or self.source_fee.intent_id != self.source_intent_id
            or self.source_fee.source_event_sha256 != self.source_event_sha256
        ):
            raise ContractValidationError("roll-exit source fee does not match source event")
        if (
            type(self.close_actions) is not tuple
            or not 1 <= len(self.close_actions) <= _MAX_ROLL_EXIT_ACTIONS
            or any(type(item) is not GroupRollExitCloseActionV1 for item in self.close_actions)
            or len({item.action_id for item in self.close_actions}) != len(self.close_actions)
            or _exact_decimal_sum(tuple(item.close_quantity for item in self.close_actions))
            != self.source_filled_quantity
        ):
            raise ContractValidationError("roll-exit actions do not exactly close source exposure")

    def to_payload(self) -> dict[str, Any]:
        return {
            "leg_id": self.leg_id,
            "source_intent_id": self.source_intent_id,
            "source_event_sha256": self.source_event_sha256,
            "source_claim_digest": self.source_claim_digest,
            "source_resolution_sha256": self.source_resolution_sha256,
            "source_filled_quantity": format(self.source_filled_quantity, "f"),
            "source_fee": self.source_fee.to_payload(),
            "close_actions": [item.to_payload() for item in self.close_actions],
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupRollExitLegClosedV1:
        if not isinstance(value, Mapping) or set(value) != {
            "leg_id", "source_intent_id", "source_event_sha256", "source_claim_digest",
            "source_resolution_sha256", "source_filled_quantity", "source_fee", "close_actions",
        } or type(value["close_actions"]) not in (list, tuple):
            raise ContractValidationError("invalid closed roll-exit leg proof")
        return cls(
            leg_id=value["leg_id"],
            source_intent_id=value["source_intent_id"],
            source_event_sha256=value["source_event_sha256"],
            source_claim_digest=value["source_claim_digest"],
            source_resolution_sha256=value["source_resolution_sha256"],
            source_filled_quantity=_parse_decimal(value["source_filled_quantity"], "source_filled_quantity"),
            source_fee=GroupRollExitSourceFeeEvidenceV1.from_payload(value["source_fee"]),
            close_actions=tuple(GroupRollExitCloseActionV1.from_payload(item) for item in value["close_actions"]),
        )


@dataclass(frozen=True, slots=True)
class GroupRollExitBudgetReleaseV1:
    """The exact v2 gross hold released by the group-close transaction."""

    policy_fingerprint: str
    quote_fingerprint: str
    valuation_unit: str
    gross_notional: Decimal
    deadline_ns: int
    from_state: str = "HELD"
    to_state: str = "RELEASED"

    def __post_init__(self) -> None:
        _require_digest(self.policy_fingerprint, "policy_fingerprint")
        _require_digest(self.quote_fingerprint, "quote_fingerprint")
        object.__setattr__(self, "valuation_unit", _currency(self.valuation_unit, "valuation_unit"))
        if type(self.gross_notional) is not Decimal or not self.gross_notional.is_finite() or self.gross_notional <= 0:
            raise ContractValidationError("invalid group budget gross notional")
        if type(self.deadline_ns) is not int or self.deadline_ns <= 0:
            raise ContractValidationError("invalid group budget deadline")
        if self.from_state != "HELD" or self.to_state != "RELEASED":
            raise ContractValidationError("roll-exit budget release must be HELD to RELEASED")

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema": "group-roll-exit-budget-release-v1",
            "policy_fingerprint": self.policy_fingerprint,
            "quote_fingerprint": self.quote_fingerprint,
            "valuation_unit": self.valuation_unit,
            "gross_notional": format(self.gross_notional, "f"),
            "deadline_ns": self.deadline_ns,
            "from_state": self.from_state,
            "to_state": self.to_state,
        }

    @classmethod
    def from_payload(cls, value: object) -> GroupRollExitBudgetReleaseV1:
        if not isinstance(value, Mapping) or set(value) != {
            "schema", "policy_fingerprint", "quote_fingerprint", "valuation_unit",
            "gross_notional", "deadline_ns", "from_state", "to_state",
        } or value["schema"] != "group-roll-exit-budget-release-v1":
            raise ContractValidationError("invalid roll-exit budget release proof")
        return cls(
            policy_fingerprint=value["policy_fingerprint"],
            quote_fingerprint=value["quote_fingerprint"],
            valuation_unit=value["valuation_unit"],
            gross_notional=_parse_decimal(value["gross_notional"], "gross_notional"),
            deadline_ns=value["deadline_ns"],
            from_state=value["from_state"],
            to_state=value["to_state"],
        )


@dataclass(frozen=True, slots=True)
class GroupRollExitClosedProofV1:
    """Complete proof required for an atomic v1/v2 aggregate group close."""

    portfolio_ref: str
    group_id: str
    group_version: int
    group_fingerprint: str
    expected_revision: int
    prior_group_event_chain_sha256: str
    legs: tuple[GroupRollExitLegClosedV1, GroupRollExitLegClosedV1]
    reservation_sha256s: tuple[str, ...]
    budget_release: GroupRollExitBudgetReleaseV1 | None = None

    def __post_init__(self) -> None:
        for name in ("portfolio_ref", "group_id"):
            object.__setattr__(self, name, _reference(getattr(self, name), name))
        if type(self.group_version) is not int or self.group_version not in {1, 2}:
            raise ContractValidationError("roll-exit close supports only group v1/v2")
        _require_digest(self.group_fingerprint, "group_fingerprint")
        if type(self.expected_revision) is not int or self.expected_revision < 1:
            raise ContractValidationError("invalid pre-close group revision")
        _require_digest(self.prior_group_event_chain_sha256, "prior_group_event_chain_sha256")
        if (
            type(self.legs) is not tuple
            or len(self.legs) != 2
            or any(type(item) is not GroupRollExitLegClosedV1 for item in self.legs)
            or self.legs[0].leg_id == self.legs[1].leg_id
            or type(self.reservation_sha256s) is not tuple
            or not 1 <= len(self.reservation_sha256s) <= 2
        ):
            raise ContractValidationError("invalid aggregate roll-exit close proof shape")
        for digest in self.reservation_sha256s:
            _require_digest(digest, "reservation_sha256")
        if self.group_version == 1 and self.budget_release is not None:
            raise ContractValidationError("v1 roll-exit proof cannot carry a budget hold")
        if self.group_version == 2 and type(self.budget_release) is not GroupRollExitBudgetReleaseV1:
            raise ContractValidationError("v2 roll-exit proof requires exact budget release evidence")
        action_ids = [action.action_id for leg in self.legs for action in leg.close_actions]
        if len(action_ids) > _MAX_ROLL_EXIT_ACTIONS or len(set(action_ids)) != len(action_ids):
            raise ContractValidationError("roll-exit close actions are duplicate or unbounded")

    def _payload_without_digest(self) -> dict[str, Any]:
        return {
            "schema": "group-roll-exit-closed-proof-v1",
            "portfolio_ref": self.portfolio_ref,
            "group_id": self.group_id,
            "group_version": self.group_version,
            "group_fingerprint": self.group_fingerprint,
            "expected_revision": self.expected_revision,
            "prior_group_event_chain_sha256": self.prior_group_event_chain_sha256,
            "legs": [item.to_payload() for item in self.legs],
            "reservation_sha256s": list(self.reservation_sha256s),
            "budget_release": None if self.budget_release is None else self.budget_release.to_payload(),
        }

    @property
    def proof_sha256(self) -> str:
        return payload_sha256(self._payload_without_digest())

    def to_payload(self) -> dict[str, Any]:
        return {**self._payload_without_digest(), "proof_sha256": self.proof_sha256}

    @classmethod
    def from_payload(cls, value: object) -> GroupRollExitClosedProofV1:
        expected = {
            "schema", "portfolio_ref", "group_id", "group_version", "group_fingerprint",
            "expected_revision", "prior_group_event_chain_sha256", "legs", "reservation_sha256s",
            "budget_release", "proof_sha256",
        }
        if (
            not isinstance(value, Mapping)
            or set(value) != expected
            or value["schema"] != "group-roll-exit-closed-proof-v1"
            or type(value["legs"]) not in (list, tuple)
            or type(value["reservation_sha256s"]) not in (list, tuple)
        ):
            raise ContractValidationError("invalid aggregate roll-exit close proof")
        result = cls(
            portfolio_ref=value["portfolio_ref"],
            group_id=value["group_id"],
            group_version=value["group_version"],
            group_fingerprint=value["group_fingerprint"],
            expected_revision=value["expected_revision"],
            prior_group_event_chain_sha256=value["prior_group_event_chain_sha256"],
            legs=tuple(GroupRollExitLegClosedV1.from_payload(item) for item in value["legs"]),
            reservation_sha256s=tuple(value["reservation_sha256s"]),
            budget_release=None
            if value["budget_release"] is None
            else GroupRollExitBudgetReleaseV1.from_payload(value["budget_release"]),
        )
        if value["proof_sha256"] != result.proof_sha256:
            raise ContractValidationError("aggregate roll-exit close proof digest mismatch")
        return result


__all__ = [
    "GroupRollExitBudgetReleaseV1",
    "GroupRollExitCloseActionV1",
    "GroupRollExitCloseFeeEvidenceV1",
    "GroupRollExitCloseFeeFillV1",
    "GroupRollExitClosedProofV1",
    "GroupRollExitLegClosedV1",
    "GroupRollExitRiskCloseClaimV1",
    "GroupRollExitRiskCloseReceiptV1",
    "GroupRollExitSourceFeeEvidenceV1",
]
