"""Typed, execution-owned contracts for approved-target submission.

This module deliberately contains no AFI, Risk, Gateway, transport, or provider
imports.  It defines the narrow facts that the authenticated service adapter
may carry into the execution facade after resolving a code-owned owner binding.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from enum import StrEnum
from hashlib import sha256
from types import MappingProxyType
from typing import Protocol

from bt_api_base.service_protocol import WireIdentity, WireScope

from .contracts import (
    ExecutionScope,
    OrderIntent,
    OrderType,
    PositionEffect,
    Side,
    TimeInForce,
)
from .errors import ContractValidationError

APPROVED_TARGET_OPERATION = "orders.submit_approved_target"
APPROVAL_TAG_PREFIX = "execution.approval."

_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_APPROVED_TARGET_REQUEST_FIELDS = frozenset(
    {"approval_ref", "approval_version", "target_sha256", "intent_id"}
)
_TARGET_FIELDS = frozenset(
    {
        "schema_id",
        "schema_version",
        "target_ref",
        "target_version",
        "owner_subject",
        "principal_ref",
        "actor_ref",
        "delegate_ref",
        "scope_ref",
        "wire_scope",
        "strategy_version",
        "membership_version",
        "membership_sha256",
        "instrument",
        "metadata_version",
        "side",
        "position_effect",
        "order_type",
        "quantity",
        "price",
        "time_in_force",
        "reduce_only",
        "deadline_ns",
        "limits",
        "policy_ref",
        "policy_version",
        "policy_sha256",
    }
)
_WIRE_SCOPE_FIELDS = frozenset(
    {
        "execution_domain_ref",
        "account_ref",
        "account_refs",
        "strategy_ref",
        "run_id",
        "manual_book_ref",
        "portfolio_ref",
        "group_id",
        "leg_id",
    }
)
_LIMIT_FIELDS = frozenset({"max_quantity", "max_notional", "min_price", "max_price"})
_DECIMAL = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_INSTRUMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,191}$")
_DECIMAL_TEXT_MAX = 128


def _reference(value: object, field_name: str) -> str:
    if type(value) is not str or not _REFERENCE.fullmatch(value):
        raise ContractValidationError(f"invalid approved-target {field_name}")
    return value


def _required_text(value: object, field_name: str, *, maximum: int = 128) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > maximum
    ):
        raise ContractValidationError(f"invalid approved-target {field_name}")
    return value


def _positive_int(value: object, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ContractValidationError(f"invalid approved-target {field_name}")
    return value


def _positive_decimal_text(value: object, field_name: str) -> Decimal:
    if (
        type(value) is not str
        or len(value) > _DECIMAL_TEXT_MAX
        or not _DECIMAL.fullmatch(value)
    ):
        raise ContractValidationError(f"invalid approved-target {field_name}")
    try:
        number = Decimal(value)
    except InvalidOperation as error:
        raise ContractValidationError(f"invalid approved-target {field_name}") from error
    with localcontext() as context:
        context.prec = max(28, len(number.as_tuple().digits) + 2)
        canonical = format(number.normalize(), "f")
    if not number.is_finite() or number <= 0 or canonical != value:
        raise ContractValidationError(f"invalid approved-target {field_name}")
    return number


@dataclass(frozen=True, slots=True)
class ApprovedTargetWireScopeV1:
    execution_domain_ref: str
    account_ref: str
    strategy_ref: str
    run_id: str | None
    portfolio_ref: str | None

    def __post_init__(self) -> None:
        _reference(self.execution_domain_ref, "wire_scope.execution_domain_ref")
        _reference(self.account_ref, "wire_scope.account_ref")
        _reference(self.strategy_ref, "wire_scope.strategy_ref")
        if self.run_id is not None:
            _reference(self.run_id, "wire_scope.run_id")
        if self.portfolio_ref is not None:
            _reference(self.portfolio_ref, "wire_scope.portfolio_ref")

    @classmethod
    def from_mapping(cls, value: object) -> ApprovedTargetWireScopeV1:
        if not isinstance(value, Mapping) or set(value) != _WIRE_SCOPE_FIELDS:
            raise ContractValidationError("invalid approved-target wire_scope shape")
        if type(value["account_refs"]) is not list or value["account_refs"] != []:
            raise ContractValidationError("approved-target must bind one account")
        if any(value[key] is not None for key in ("manual_book_ref", "group_id", "leg_id")):
            raise ContractValidationError("approved-target cannot bind a manual book or group")
        return cls(
            execution_domain_ref=value["execution_domain_ref"],
            account_ref=value["account_ref"],
            strategy_ref=value["strategy_ref"],
            run_id=value["run_id"],
            portfolio_ref=value["portfolio_ref"],
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "execution_domain_ref": self.execution_domain_ref,
            "account_ref": self.account_ref,
            "account_refs": [],
            "strategy_ref": self.strategy_ref,
            "run_id": self.run_id,
            "manual_book_ref": None,
            "portfolio_ref": self.portfolio_ref,
            "group_id": None,
            "leg_id": None,
        }

    def matches(self, scope: WireScope) -> bool:
        return (
            type(scope) is WireScope
            and self.execution_domain_ref == scope.execution_domain_ref
            and self.account_ref == scope.account_ref
            and scope.account_refs == ()
            and self.strategy_ref == scope.strategy_ref
            and self.run_id == scope.run_id
            and scope.manual_book_ref is None
            and self.portfolio_ref == scope.portfolio_ref
            and scope.group_id is None
            and scope.leg_id is None
        )


@dataclass(frozen=True, slots=True)
class ApprovedTargetLimitsV1:
    max_quantity: str
    max_notional: str | None
    min_price: str | None
    max_price: str | None

    def __post_init__(self) -> None:
        max_quantity = _positive_decimal_text(self.max_quantity, "limits.max_quantity")
        optional = (
            ("max_notional", self.max_notional),
            ("min_price", self.min_price),
            ("max_price", self.max_price),
        )
        parsed: dict[str, Decimal | None] = {}
        for name, value in optional:
            parsed[name] = (
                None if value is None else _positive_decimal_text(value, f"limits.{name}")
            )
        if (
            parsed["min_price"] is not None
            and parsed["max_price"] is not None
            and parsed["min_price"] > parsed["max_price"]
        ):
            raise ContractValidationError("approved-target price limits are inverted")
        object.__setattr__(self, "max_quantity", format(max_quantity, "f"))

    @classmethod
    def from_mapping(cls, value: object) -> ApprovedTargetLimitsV1:
        if not isinstance(value, Mapping) or set(value) != _LIMIT_FIELDS:
            raise ContractValidationError("invalid approved-target limits shape")
        return cls(**dict(value))

    def to_payload(self) -> dict[str, str | None]:
        return {
            "max_quantity": self.max_quantity,
            "max_notional": self.max_notional,
            "min_price": self.min_price,
            "max_price": self.max_price,
        }


@dataclass(frozen=True, slots=True)
class ApprovedTargetDocumentV1:
    """Exact AFI v1 target, revalidated and fingerprinted by Execution."""

    target_ref: str
    target_version: int
    owner_subject: str
    principal_ref: str
    actor_ref: str
    delegate_ref: str | None
    scope_ref: str
    wire_scope: ApprovedTargetWireScopeV1
    strategy_version: int
    membership_version: int | None
    membership_sha256: str | None
    instrument: str
    metadata_version: str
    side: Side
    position_effect: PositionEffect
    order_type: OrderType
    quantity: str
    price: str | None
    time_in_force: TimeInForce
    reduce_only: bool
    deadline_ns: int | None
    limits: ApprovedTargetLimitsV1
    policy_ref: str
    policy_version: int
    policy_sha256: str

    def __post_init__(self) -> None:
        _reference(self.target_ref, "target_ref")
        _positive_int(self.target_version, "target_version")
        _required_text(self.owner_subject, "owner_subject", maximum=256)
        _reference(self.principal_ref, "principal_ref")
        _reference(self.actor_ref, "actor_ref")
        if self.delegate_ref is not None:
            _reference(self.delegate_ref, "delegate_ref")
        _reference(self.scope_ref, "scope_ref")
        if type(self.wire_scope) is not ApprovedTargetWireScopeV1:
            raise ContractValidationError("approved-target wire scope is required")
        _positive_int(self.strategy_version, "strategy_version")
        if self.membership_version is None:
            if self.membership_sha256 is not None:
                raise ContractValidationError("membership version and digest must be paired")
        else:
            _positive_int(self.membership_version, "membership_version")
            if type(self.membership_sha256) is not str or not _SHA256.fullmatch(
                self.membership_sha256
            ):
                raise ContractValidationError("invalid membership_sha256")
            if self.wire_scope.portfolio_ref is None:
                raise ContractValidationError("membership requires a portfolio scope")
        if type(self.instrument) is not str or not _INSTRUMENT.fullmatch(self.instrument):
            raise ContractValidationError("invalid approved-target instrument")
        _reference(self.metadata_version, "metadata_version")
        for name, enum_type in (
            ("side", Side),
            ("position_effect", PositionEffect),
            ("order_type", OrderType),
            ("time_in_force", TimeInForce),
        ):
            if type(getattr(self, name)) is not enum_type:
                raise ContractValidationError(f"invalid approved-target {name}")
        quantity = _positive_decimal_text(self.quantity, "quantity")
        if self.order_type is OrderType.MARKET:
            if self.price is not None or self.limits.max_notional is not None:
                raise ContractValidationError("market target cannot specify price/notional")
        else:
            if self.price is None:
                raise ContractValidationError("limit target requires price")
            price = _positive_decimal_text(self.price, "price")
            if self.limits.min_price is not None and price < Decimal(self.limits.min_price):
                raise ContractValidationError("target price is below its approved limit")
            if self.limits.max_price is not None and price > Decimal(self.limits.max_price):
                raise ContractValidationError("target price exceeds its approved limit")
        if self.limits.max_notional is not None:
            with localcontext() as context:
                context.prec = 2 * _DECIMAL_TEXT_MAX + 2
                if quantity * price > Decimal(self.limits.max_notional):
                    raise ContractValidationError("target notional exceeds its approved limit")
        if quantity > Decimal(self.limits.max_quantity):
            raise ContractValidationError("target quantity exceeds its approved limit")
        if type(self.reduce_only) is not bool:
            raise ContractValidationError("invalid approved-target reduce_only")
        if self.position_effect is not PositionEffect.OPEN and not self.reduce_only:
            raise ContractValidationError("close target must be reduce_only")
        if self.deadline_ns is not None:
            _positive_int(self.deadline_ns, "deadline_ns")
        _reference(self.policy_ref, "policy_ref")
        _positive_int(self.policy_version, "policy_version")
        if type(self.policy_sha256) is not str or not _SHA256.fullmatch(self.policy_sha256):
            raise ContractValidationError("invalid policy_sha256")

    @classmethod
    def from_mapping(cls, value: object) -> ApprovedTargetDocumentV1:
        if not isinstance(value, Mapping) or set(value) != _TARGET_FIELDS:
            raise ContractValidationError("approved-target document has an invalid shape")
        if (
            value["schema_id"] != "bt_api.approved_order_target"
            or type(value["schema_id"]) is not str
            or type(value["schema_version"]) is not int
            or value["schema_version"] != 1
        ):
            raise ContractValidationError("unsupported approved-target schema")
        try:
            return cls(
                target_ref=value["target_ref"],
                target_version=value["target_version"],
                owner_subject=value["owner_subject"],
                principal_ref=value["principal_ref"],
                actor_ref=value["actor_ref"],
                delegate_ref=value["delegate_ref"],
                scope_ref=value["scope_ref"],
                wire_scope=ApprovedTargetWireScopeV1.from_mapping(value["wire_scope"]),
                strategy_version=value["strategy_version"],
                membership_version=value["membership_version"],
                membership_sha256=value["membership_sha256"],
                instrument=value["instrument"],
                metadata_version=value["metadata_version"],
                side=Side(value["side"]),
                position_effect=PositionEffect(value["position_effect"]),
                order_type=OrderType(value["order_type"]),
                quantity=value["quantity"],
                price=value["price"],
                time_in_force=TimeInForce(value["time_in_force"]),
                reduce_only=value["reduce_only"],
                deadline_ns=value["deadline_ns"],
                limits=ApprovedTargetLimitsV1.from_mapping(value["limits"]),
                policy_ref=value["policy_ref"],
                policy_version=value["policy_version"],
                policy_sha256=value["policy_sha256"],
            )
        except (TypeError, ValueError) as error:
            if isinstance(error, ContractValidationError):
                raise
            raise ContractValidationError("invalid approved-target field value") from error

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_id": "bt_api.approved_order_target",
            "schema_version": 1,
            "target_ref": self.target_ref,
            "target_version": self.target_version,
            "owner_subject": self.owner_subject,
            "principal_ref": self.principal_ref,
            "actor_ref": self.actor_ref,
            "delegate_ref": self.delegate_ref,
            "scope_ref": self.scope_ref,
            "wire_scope": self.wire_scope.to_payload(),
            "strategy_version": self.strategy_version,
            "membership_version": self.membership_version,
            "membership_sha256": self.membership_sha256,
            "instrument": self.instrument,
            "metadata_version": self.metadata_version,
            "side": self.side.value,
            "position_effect": self.position_effect.value,
            "order_type": self.order_type.value,
            "quantity": self.quantity,
            "price": self.price,
            "time_in_force": self.time_in_force.value,
            "reduce_only": self.reduce_only,
            "deadline_ns": self.deadline_ns,
            "limits": self.limits.to_payload(),
            "policy_ref": self.policy_ref,
            "policy_version": self.policy_version,
            "policy_sha256": self.policy_sha256,
        }

    @property
    def sha256(self) -> str:
        canonical = json.dumps(
            self.to_payload(), sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
        return sha256(canonical).hexdigest()

    def validate_binding(
        self,
        binding: ApprovedTargetOwnerBindingV1,
        *,
        principal_ref: str,
        identity: WireIdentity,
        wire_scope: WireScope,
    ) -> None:
        if type(binding) is not ApprovedTargetOwnerBindingV1 or not binding.matches(
            principal_ref, identity, wire_scope
        ):
            raise ContractValidationError("approved-target owner binding mismatch")
        if (
            self.owner_subject != binding.owner_subject
            or self.principal_ref != binding.principal_ref
            or self.actor_ref != binding.identity.actor_ref
            or self.delegate_ref != binding.identity.delegate_ref
            or self.scope_ref != binding.expected_scope_ref
            or not self.wire_scope.matches(binding.wire_scope)
            or not self.wire_scope.matches(wire_scope)
        ):
            raise ContractValidationError("approved-target identity or scope mismatch")

    def to_order_intent(
        self, *, intent_id: str, execution_scope: ExecutionScope, request: ApprovedTargetSubmitRequestV1
    ) -> OrderIntent:
        if (
            type(execution_scope) is not ExecutionScope
            or type(request) is not ApprovedTargetSubmitRequestV1
            or self.sha256 != request.target_sha256
            or intent_id != request.intent_id
            or execution_scope.account_ref != self.wire_scope.account_ref
            or execution_scope.strategy_id != self.wire_scope.strategy_ref
        ):
            raise ContractValidationError("approved-target execution binding mismatch")
        signal_id = "approval." + sha256(
            f"{request.approval_ref}:{request.approval_version}:{request.intent_id}".encode()
        ).hexdigest()[:48]
        return OrderIntent(
            intent_id=intent_id,
            scope=execution_scope,
            signal_id=signal_id,
            instrument=self.instrument,
            side=self.side,
            position_effect=self.position_effect,
            order_type=self.order_type,
            quantity=Decimal(self.quantity),
            price=None if self.price is None else Decimal(self.price),
            time_in_force=self.time_in_force,
            reduce_only=self.reduce_only,
            metadata_version=self.metadata_version,
            deadline_ns=self.deadline_ns,
            tags=approved_target_audit_tags(request),
        )


def _strategy_scope(scope: object) -> bool:
    return (
        type(scope) is WireScope
        and scope.account_ref is not None
        and not scope.account_refs
        and scope.strategy_ref is not None
        and scope.manual_book_ref is None
        and scope.group_id is None
        and scope.leg_id is None
    )


@dataclass(frozen=True, slots=True)
class ApprovedTargetSubmitRequestV1:
    """The exact four claims accepted on the approved-target wire operation."""

    approval_ref: str
    approval_version: int
    target_sha256: str
    intent_id: str

    def __post_init__(self) -> None:
        _reference(self.approval_ref, "approval_ref")
        if type(self.approval_version) is not int or self.approval_version <= 0:
            raise ContractValidationError("invalid approved-target approval_version")
        if type(self.target_sha256) is not str or not _SHA256.fullmatch(self.target_sha256):
            raise ContractValidationError("invalid approved-target target_sha256")
        _reference(self.intent_id, "intent_id")

    @classmethod
    def from_arguments(cls, arguments: object) -> ApprovedTargetSubmitRequestV1:
        if not isinstance(arguments, Mapping):
            raise ContractValidationError("approved-target arguments must be a mapping")
        keys = tuple(arguments.keys())
        if any(type(key) is not str for key in keys) or frozenset(keys) != (
            _APPROVED_TARGET_REQUEST_FIELDS
        ):
            raise ContractValidationError("approved-target arguments have an invalid shape")
        return cls(
            approval_ref=arguments["approval_ref"],
            approval_version=arguments["approval_version"],
            target_sha256=arguments["target_sha256"],
            intent_id=arguments["intent_id"],
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "approval_ref": self.approval_ref,
            "approval_version": self.approval_version,
            "target_sha256": self.target_sha256,
            "intent_id": self.intent_id,
        }


@dataclass(frozen=True, slots=True)
class ApprovedTargetOwnerBindingV1:
    """Code-owned exact mapping from transport identity and WireScope to AFI owner.

    ``expected_scope_ref`` is supplied by trusted server configuration.  It is
    not taken from the request, nor inferred by comparing a target to itself.
    """

    principal_ref: str
    identity: WireIdentity
    wire_scope: WireScope
    owner_subject: str
    expected_scope_ref: str

    def __post_init__(self) -> None:
        _reference(self.principal_ref, "principal_ref")
        if type(self.identity) is not WireIdentity or self.identity.principal_ref != self.principal_ref:
            raise ContractValidationError("approved-target identity binding is inconsistent")
        if not _strategy_scope(self.wire_scope):
            raise ContractValidationError("approved-target binding requires one strategy account scope")
        _reference(self.owner_subject, "owner_subject")
        _reference(self.expected_scope_ref, "expected_scope_ref")

    def matches(
        self, principal_ref: object, identity: object, wire_scope: object
    ) -> bool:
        """Match the complete server-authenticated identity and scope tuple."""

        return (
            type(principal_ref) is str
            and principal_ref == self.principal_ref
            and type(identity) is WireIdentity
            and identity == self.identity
            and type(wire_scope) is WireScope
            and wire_scope == self.wire_scope
        )


class ApprovedTargetOwnerBindingsV1:
    """Immutable exact-match binding table; absent or duplicate bindings fail closed."""

    def __init__(self, bindings: tuple[ApprovedTargetOwnerBindingV1, ...]) -> None:
        if type(bindings) is not tuple or not bindings:
            raise ContractValidationError("approved-target owner bindings are required")
        table: dict[tuple[str, WireIdentity, WireScope], ApprovedTargetOwnerBindingV1] = {}
        for binding in bindings:
            if type(binding) is not ApprovedTargetOwnerBindingV1:
                raise ContractValidationError("approved-target owner binding is untyped")
            key = (binding.principal_ref, binding.identity, binding.wire_scope)
            if key in table:
                raise ContractValidationError("approved-target owner binding is ambiguous")
            table[key] = binding
        self._bindings = MappingProxyType(table)

    def resolve(
        self, principal_ref: object, identity: object, wire_scope: object
    ) -> ApprovedTargetOwnerBindingV1 | None:
        if (
            type(principal_ref) is not str
            or type(identity) is not WireIdentity
            or type(wire_scope) is not WireScope
        ):
            return None
        return self._bindings.get((principal_ref, identity, wire_scope))


class ProtectedApprovedTargetScopesV1:
    """Exact strategy scopes where ordinary submit is blocked pending approval.

    A configured ``run_id=None`` scope acts as a run template, matching the
    execution service's existing run-template behavior.  All other WireScope
    fields must match exactly; no string-prefix or account-only matching exists.
    """

    def __init__(self, scopes: tuple[WireScope, ...]) -> None:
        if type(scopes) is not tuple or not scopes:
            raise ContractValidationError("protected approved-target scopes are required")
        copied: list[WireScope] = []
        for scope in scopes:
            if not _strategy_scope(scope):
                raise ContractValidationError("only one-account strategy scopes may be protected")
            if scope in copied:
                raise ContractValidationError("protected approved-target scope is duplicated")
            for prior in copied:
                if _scope_templates_overlap(prior, scope):
                    raise ContractValidationError("protected approved-target scopes overlap")
            copied.append(scope)
        self._scopes = tuple(copied)

    @property
    def scopes(self) -> tuple[WireScope, ...]:
        return self._scopes

    def requires_approval(self, scope: object) -> bool:
        if not _strategy_scope(scope):
            return False
        return any(_scope_template_matches(template, scope) for template in self._scopes)


def _scope_template_matches(template: WireScope, requested: WireScope) -> bool:
    if template.run_id is not None:
        return template == requested
    return (
        template.execution_domain_ref == requested.execution_domain_ref
        and template.account_ref == requested.account_ref
        and template.account_refs == requested.account_refs
        and template.strategy_ref == requested.strategy_ref
        and requested.run_id is not None
        and template.manual_book_ref == requested.manual_book_ref
        and template.portfolio_ref == requested.portfolio_ref
        and template.group_id == requested.group_id
        and template.leg_id == requested.leg_id
    ) or template == requested


def _scope_templates_overlap(left: WireScope, right: WireScope) -> bool:
    if left == right:
        return True
    if left.run_id is not None and right.run_id is not None:
        return False
    left_without_run = (
        left.execution_domain_ref,
        left.account_ref,
        left.account_refs,
        left.strategy_ref,
        left.manual_book_ref,
        left.portfolio_ref,
        left.group_id,
        left.leg_id,
    )
    right_without_run = (
        right.execution_domain_ref,
        right.account_ref,
        right.account_refs,
        right.strategy_ref,
        right.manual_book_ref,
        right.portfolio_ref,
        right.group_id,
        right.leg_id,
    )
    return left_without_run == right_without_run


class ApprovedTargetConsumeDisposition(StrEnum):
    """Only a fresh consume authorizes the approved path to claim its gates."""

    CONSUMED_NEW = "CONSUMED_NEW"
    DEFINITIVE_DENIAL = "DEFINITIVE_DENIAL"
    AMBIGUOUS = "AMBIGUOUS"
    ALREADY_CONSUMED = "ALREADY_CONSUMED"


_DEFINITE_DENIAL_CODES = frozenset(
    {
        "APPROVAL_NOT_FOUND",
        "APPROVAL_OWNER_BINDING_MISMATCH",
        "APPROVAL_IDENTITY_MISMATCH",
        "APPROVAL_SCOPE_MISMATCH",
        "APPROVAL_NOT_APPROVED",
        "APPROVAL_REVOKED",
        "APPROVAL_EXPIRED",
        "APPROVAL_VERSION_MISMATCH",
        "APPROVAL_TARGET_DIGEST_MISMATCH",
        "APPROVAL_TARGET_INVALID",
        "APPROVAL_POLICY_STALE",
        "APPROVAL_OPERATION_CONFLICT",
        "APPROVAL_CONSUMED_CONFLICT",
    }
)
_AUTHORITY_ERROR_CODES = _DEFINITE_DENIAL_CODES | frozenset(
    {"APPROVAL_CONSUME_OUTCOME_UNKNOWN", "APPROVAL_STORE_UNAVAILABLE"}
)


class ApprovedTargetAuthorityError(RuntimeError):
    """Finite trusted-authority failure; ambiguous cases never authorize dispatch."""

    def __init__(self, code: str, *, ambiguous: bool = False) -> None:
        if code not in _AUTHORITY_ERROR_CODES:
            code = "APPROVAL_STORE_UNAVAILABLE"
            ambiguous = True
        if code in {"APPROVAL_CONSUME_OUTCOME_UNKNOWN", "APPROVAL_STORE_UNAVAILABLE"}:
            ambiguous = True
        elif code in _DEFINITE_DENIAL_CODES:
            ambiguous = False
        self.code = code
        self.ambiguous = ambiguous
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class ApprovedTargetOperationClaimV1:
    approval_ref: str
    approval_version: int
    target_sha256: str
    intent_id: str
    operation_request_sha256: str
    created: bool
    record_version: int

    def __post_init__(self) -> None:
        _reference(self.approval_ref, "claim.approval_ref")
        _positive_int(self.approval_version, "claim.approval_version")
        if type(self.target_sha256) is not str or not _SHA256.fullmatch(self.target_sha256):
            raise ContractValidationError("invalid claim.target_sha256")
        _reference(self.intent_id, "claim.intent_id")
        if type(self.operation_request_sha256) is not str or not _SHA256.fullmatch(
            self.operation_request_sha256
        ):
            raise ContractValidationError("invalid claim.operation_request_sha256")
        if type(self.created) is not bool:
            raise ContractValidationError("invalid claim.created")
        _positive_int(self.record_version, "claim.record_version")


@dataclass(frozen=True, slots=True)
class ApprovedTargetConsumptionReceiptV1:
    consumption_ref: str
    approval_ref: str
    approval_version: int
    target_ref: str
    target_version: int
    target_sha256: str
    intent_id: str
    intent_sha256: str
    consumed_record_version: int
    consumed_at_ns: int

    def __post_init__(self) -> None:
        _reference(self.consumption_ref, "receipt.consumption_ref")
        _reference(self.approval_ref, "receipt.approval_ref")
        _positive_int(self.approval_version, "receipt.approval_version")
        _reference(self.target_ref, "receipt.target_ref")
        _positive_int(self.target_version, "receipt.target_version")
        if type(self.target_sha256) is not str or not _SHA256.fullmatch(self.target_sha256):
            raise ContractValidationError("invalid receipt.target_sha256")
        _reference(self.intent_id, "receipt.intent_id")
        if type(self.intent_sha256) is not str or not _SHA256.fullmatch(self.intent_sha256):
            raise ContractValidationError("invalid receipt.intent_sha256")
        _positive_int(self.consumed_record_version, "receipt.consumed_record_version")
        _positive_int(self.consumed_at_ns, "receipt.consumed_at_ns")


@dataclass(frozen=True, slots=True)
class ApprovedTargetConsumeResultV1:
    """Finite typed result that distinguishes denial from a lost/uncertain reply."""

    disposition: ApprovedTargetConsumeDisposition
    reason_code: str | None = None
    receipt: ApprovedTargetConsumptionReceiptV1 | None = None

    def __post_init__(self) -> None:
        if type(self.disposition) is not ApprovedTargetConsumeDisposition:
            raise ContractValidationError("approved-target consume disposition is untyped")
        if self.disposition is ApprovedTargetConsumeDisposition.DEFINITIVE_DENIAL:
            if self.reason_code not in _DEFINITE_DENIAL_CODES:
                raise ContractValidationError("approved-target denial code is not recognized")
            if self.receipt is not None:
                raise ContractValidationError("denied consume cannot carry a receipt")
        elif self.disposition is ApprovedTargetConsumeDisposition.AMBIGUOUS:
            if self.reason_code != "APPROVAL_CONSUME_OUTCOME_UNKNOWN":
                raise ContractValidationError("approved-target ambiguity code is not recognized")
            if self.receipt is not None:
                raise ContractValidationError("ambiguous consume cannot carry a receipt")
        elif self.reason_code is not None:
            raise ContractValidationError("successful consume result cannot carry a denial code")
        if self.disposition in {
            ApprovedTargetConsumeDisposition.CONSUMED_NEW,
            ApprovedTargetConsumeDisposition.ALREADY_CONSUMED,
        } and type(self.receipt) is not ApprovedTargetConsumptionReceiptV1:
            raise ContractValidationError("successful consume disposition requires its receipt")


class ApprovedTargetAuthorityPort(Protocol):
    """Synchronous trusted adapter over AFI's durable approval authority."""

    def reserve_approved_target_operation(
        self,
        *,
        approval_ref: str,
        approval_version: int,
        target_sha256: str,
        intent_id: str,
        operation_key: str,
        owner_subject: str,
        principal_ref: str,
        expected_scope_ref: str,
        identity: WireIdentity,
        wire_scope: WireScope,
    ) -> Mapping[str, object]: ...

    def resolve_approved_target_v1(
        self,
        *,
        approval_ref: str,
        approval_version: int,
        target_sha256: str,
        intent_id: str,
        operation_key: str,
        owner_subject: str,
        principal_ref: str,
        expected_scope_ref: str,
        identity: WireIdentity,
        wire_scope: WireScope,
    ) -> Mapping[str, object]: ...

    def consume_approved_target_v1(
        self,
        *,
        approval_ref: str,
        approval_version: int,
        target_sha256: str,
        intent_id: str,
        operation_key: str,
        owner_subject: str,
        principal_ref: str,
        expected_scope_ref: str,
        identity: WireIdentity,
        wire_scope: WireScope,
        intent_sha256: str,
    ) -> Mapping[str, object]: ...


class ApprovedTargetAuthorityV1:
    """Authority bridge with exact code-owned transport-to-owner bindings.

    The configured port must perform current target/policy/membership checks
    and revoke-vs-consume serialization in AFI's persistence transaction.
    This adapter treats malformed, already-consumed and ambiguous results as
    non-dispatch outcomes.
    """

    def __init__(
        self,
        port: ApprovedTargetAuthorityPort,
        owner_bindings: ApprovedTargetOwnerBindingsV1,
        protected_scopes: ProtectedApprovedTargetScopesV1,
    ) -> None:
        if any(
            not callable(getattr(port, name, None))
            for name in (
                "reserve_approved_target_operation",
                "resolve_approved_target_v1",
                "consume_approved_target_v1",
            )
        ):
            raise ContractValidationError("approved-target authority port is incomplete")
        if type(owner_bindings) is not ApprovedTargetOwnerBindingsV1:
            raise ContractValidationError("approved-target owner bindings are required")
        if type(protected_scopes) is not ProtectedApprovedTargetScopesV1:
            raise ContractValidationError("protected approved-target scopes are required")
        self._port = port
        self._owner_bindings = owner_bindings
        self._protected_scopes = protected_scopes

    def binding_for(
        self, principal_ref: object, identity: object, wire_scope: object
    ) -> ApprovedTargetOwnerBindingV1 | None:
        return self._owner_bindings.resolve(principal_ref, identity, wire_scope)

    def requires_approval(self, wire_scope: object) -> bool:
        return self._protected_scopes.requires_approval(wire_scope)

    def reserve(
        self,
        request: ApprovedTargetSubmitRequestV1,
        binding: ApprovedTargetOwnerBindingV1,
    ) -> ApprovedTargetOperationClaimV1:
        arguments = self._bound_arguments(request, binding)
        arguments["operation_key"] = approved_target_operation_key(request, binding)
        try:
            raw_claim = self._port.reserve_approved_target_operation(**arguments)
        except Exception as error:
            raise _authority_error(error, ambiguous_default=True) from None
        claim = _operation_claim(raw_claim)
        if (
            claim.approval_ref != request.approval_ref
            or claim.approval_version != request.approval_version
            or claim.target_sha256 != request.target_sha256
            or claim.intent_id != request.intent_id
        ):
            raise ApprovedTargetAuthorityError("APPROVAL_OPERATION_CONFLICT")
        return claim

    def resolve_target(
        self,
        request: ApprovedTargetSubmitRequestV1,
        binding: ApprovedTargetOwnerBindingV1,
    ) -> ApprovedTargetDocumentV1:
        arguments = self._bound_arguments(request, binding)
        arguments["operation_key"] = approved_target_operation_key(request, binding)
        try:
            target = ApprovedTargetDocumentV1.from_mapping(
                self._port.resolve_approved_target_v1(**arguments)
            )
            target.validate_binding(
                binding,
                principal_ref=binding.principal_ref,
                identity=binding.identity,
                wire_scope=binding.wire_scope,
            )
        except ContractValidationError as error:
            raise ApprovedTargetAuthorityError("APPROVAL_TARGET_INVALID") from error
        except ApprovedTargetAuthorityError:
            raise
        except Exception as error:
            raise _authority_error(error, ambiguous_default=True) from None
        if target.sha256 != request.target_sha256:
            raise ApprovedTargetAuthorityError("APPROVAL_TARGET_DIGEST_MISMATCH")
        return target

    def consume(
        self,
        request: ApprovedTargetSubmitRequestV1,
        binding: ApprovedTargetOwnerBindingV1,
        target: ApprovedTargetDocumentV1,
        *,
        intent_sha256: str,
    ) -> ApprovedTargetConsumeResultV1:
        try:
            arguments = self._bound_arguments(request, binding)
            arguments["operation_key"] = approved_target_operation_key(request, binding)
            raw = self._port.consume_approved_target_v1(
                **arguments, intent_sha256=intent_sha256
            )
            result = _consume_result(raw)
        except Exception as error:
            # Only this package-owned typed denial proves consume did not
            # commit. A transport exception can expose any ``code`` value,
            # including a denial-looking one, after the durable CAS succeeded.
            if type(error) is ApprovedTargetAuthorityError and not error.ambiguous:
                return ApprovedTargetConsumeResultV1(
                    ApprovedTargetConsumeDisposition.DEFINITIVE_DENIAL,
                    error.code,
                )
            # Timeouts, adapter failures, subclasses, and malformed responses
            # may follow a durable consume. Preserve DISPATCHING and never retry.
            return ApprovedTargetConsumeResultV1(
                ApprovedTargetConsumeDisposition.AMBIGUOUS,
                "APPROVAL_CONSUME_OUTCOME_UNKNOWN",
            )
        if result.disposition in {
            ApprovedTargetConsumeDisposition.CONSUMED_NEW,
            ApprovedTargetConsumeDisposition.ALREADY_CONSUMED,
        }:
            receipt = result.receipt
            if (
                receipt is None
                or receipt.approval_ref != request.approval_ref
                or receipt.approval_version != request.approval_version
                or receipt.target_ref != target.target_ref
                or receipt.target_version != target.target_version
                or receipt.target_sha256 != request.target_sha256
                or receipt.intent_id != request.intent_id
                or receipt.intent_sha256 != intent_sha256
            ):
                return ApprovedTargetConsumeResultV1(
                    ApprovedTargetConsumeDisposition.AMBIGUOUS,
                    "APPROVAL_CONSUME_OUTCOME_UNKNOWN",
                )
        return result

    @staticmethod
    def _bound_arguments(
        request: ApprovedTargetSubmitRequestV1,
        binding: ApprovedTargetOwnerBindingV1,
    ) -> dict[str, object]:
        if type(request) is not ApprovedTargetSubmitRequestV1:
            raise ContractValidationError("approved-target request is required")
        if type(binding) is not ApprovedTargetOwnerBindingV1:
            raise ContractValidationError("approved-target owner binding is required")
        return {
            "approval_ref": request.approval_ref,
            "approval_version": request.approval_version,
            "target_sha256": request.target_sha256,
            "intent_id": request.intent_id,
            "owner_subject": binding.owner_subject,
            "principal_ref": binding.principal_ref,
            "expected_scope_ref": binding.expected_scope_ref,
            "identity": binding.identity,
            "wire_scope": binding.wire_scope,
        }


def approved_target_operation_key(
    request: ApprovedTargetSubmitRequestV1,
    binding: ApprovedTargetOwnerBindingV1,
) -> str:
    """Derive a stable operation key from request and the full trusted binding."""

    if type(request) is not ApprovedTargetSubmitRequestV1:
        raise ContractValidationError("approved-target request is required")
    if type(binding) is not ApprovedTargetOwnerBindingV1:
        raise ContractValidationError("approved-target owner binding is required")
    payload = {
        "schema_id": "bt_api.approved_target_operation_key",
        "schema_version": 1,
        "approval_ref": request.approval_ref,
        "approval_version": request.approval_version,
        "target_sha256": request.target_sha256,
        "intent_id": request.intent_id,
        "owner_subject": binding.owner_subject,
        "principal_ref": binding.principal_ref,
        "identity": {
            "role": binding.identity.role.value,
            "principal_ref": binding.identity.principal_ref,
            "actor_ref": binding.identity.actor_ref,
            "delegate_ref": binding.identity.delegate_ref,
        },
        "wire_scope": {
            "execution_domain_ref": binding.wire_scope.execution_domain_ref,
            "account_ref": binding.wire_scope.account_ref,
            "account_refs": list(binding.wire_scope.account_refs),
            "strategy_ref": binding.wire_scope.strategy_ref,
            "run_id": binding.wire_scope.run_id,
            "manual_book_ref": binding.wire_scope.manual_book_ref,
            "portfolio_ref": binding.wire_scope.portfolio_ref,
            "group_id": binding.wire_scope.group_id,
            "leg_id": binding.wire_scope.leg_id,
        },
        "expected_scope_ref": binding.expected_scope_ref,
    }
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("utf-8")
    return sha256(canonical).hexdigest()


def _operation_claim(value: object) -> ApprovedTargetOperationClaimV1:
    fields = {
        "approval_ref",
        "approval_version",
        "target_sha256",
        "intent_id",
        "operation_request_sha256",
        "created",
        "record_version",
    }
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ApprovedTargetAuthorityError("APPROVAL_STORE_UNAVAILABLE", ambiguous=True)
    try:
        return ApprovedTargetOperationClaimV1(**dict(value))
    except (ContractValidationError, TypeError):
        # The reservation may already be durable even if its reply is malformed.
        raise ApprovedTargetAuthorityError("APPROVAL_STORE_UNAVAILABLE", ambiguous=True) from None


def _authority_error(error: Exception, *, ambiguous_default: bool) -> ApprovedTargetAuthorityError:
    if type(error) is ApprovedTargetAuthorityError:
        return error
    code = getattr(error, "code", None)
    if type(code) is str and code in _DEFINITE_DENIAL_CODES:
        return ApprovedTargetAuthorityError(code)
    if type(code) is str and code in _AUTHORITY_ERROR_CODES:
        return ApprovedTargetAuthorityError(code, ambiguous=ambiguous_default)
    return ApprovedTargetAuthorityError("APPROVAL_STORE_UNAVAILABLE", ambiguous=True)


def _consume_result(value: object) -> ApprovedTargetConsumeResultV1:
    fields = {
        "disposition",
        "consumption_ref",
        "approval_ref",
        "approval_version",
        "consumed_record_version",
        "target_ref",
        "target_version",
        "target_sha256",
        "intent_id",
        "intent_sha256",
        "consumed_at_ns",
    }
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ContractValidationError("approved-target consume receipt has an invalid shape")
    try:
        disposition = ApprovedTargetConsumeDisposition(value["disposition"])
    except (TypeError, ValueError) as error:
        raise ContractValidationError("unknown approved-target consume disposition") from error
    if disposition not in {
        ApprovedTargetConsumeDisposition.CONSUMED_NEW,
        ApprovedTargetConsumeDisposition.ALREADY_CONSUMED,
    }:
        raise ContractValidationError("authority returned a non-receipt consume disposition")
    receipt = ApprovedTargetConsumptionReceiptV1(
        consumption_ref=value["consumption_ref"],
        approval_ref=value["approval_ref"],
        approval_version=value["approval_version"],
        target_ref=value["target_ref"],
        target_version=value["target_version"],
        target_sha256=value["target_sha256"],
        intent_id=value["intent_id"],
        intent_sha256=value["intent_sha256"],
        consumed_record_version=value["consumed_record_version"],
        consumed_at_ns=value["consumed_at_ns"],
    )
    return ApprovedTargetConsumeResultV1(disposition, receipt=receipt)


def has_reserved_approval_tags(tags: object) -> bool:
    """Return whether ordinary order tags attempt to claim the reserved namespace."""

    if not isinstance(tags, Mapping):
        return False
    return any(type(key) is str and key.startswith(APPROVAL_TAG_PREFIX) for key in tags)


def approved_target_audit_tags(request: ApprovedTargetSubmitRequestV1) -> Mapping[str, str]:
    """Build immutable journal audit tags only from the typed operation request."""

    if type(request) is not ApprovedTargetSubmitRequestV1:
        raise ContractValidationError("approved-target request is required")
    return MappingProxyType(
        {
            f"{APPROVAL_TAG_PREFIX}ref": request.approval_ref,
            f"{APPROVAL_TAG_PREFIX}version": str(request.approval_version),
            f"{APPROVAL_TAG_PREFIX}target_sha256": request.target_sha256,
        }
    )


__all__ = [
    "APPROVAL_TAG_PREFIX",
    "APPROVED_TARGET_OPERATION",
    "ApprovedTargetAuthorityError",
    "ApprovedTargetAuthorityPort",
    "ApprovedTargetAuthorityV1",
    "ApprovedTargetConsumeDisposition",
    "ApprovedTargetConsumeResultV1",
    "ApprovedTargetConsumptionReceiptV1",
    "ApprovedTargetDocumentV1",
    "ApprovedTargetOperationClaimV1",
    "ApprovedTargetOwnerBindingV1",
    "ApprovedTargetOwnerBindingsV1",
    "ApprovedTargetSubmitRequestV1",
    "ApprovedTargetWireScopeV1",
    "ProtectedApprovedTargetScopesV1",
    "approved_target_audit_tags",
    "has_reserved_approval_tags",
]
