"""Execution-owned, provider-specific basic admission without Risk/Gateway imports.

The host supplies the immutable caller context after its authenticated service
handler checks identity, role, and the registered WireScope mapping.  A
code-owned provider policy validates the complete typed order and creates the
provider parameter projection.  The gate keeps its one-shot permit only in
memory; SQLite stores its opaque reference plus the caller-context fingerprint
so a fresh process can safely reauthorize only an undispatched pending row.
"""

from __future__ import annotations

import re
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Protocol

from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope

from .contracts import (
    ExecutionScope,
    OrderIntent,
    OrderType,
    PositionEffect,
    Side,
    TimeInForce,
    payload_sha256,
)
from .errors import ContractValidationError, ExecutionBasicAdmissionDenied
from .store import SqliteExecutionStore, WriterLease

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_INSTRUMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,191}$")
_REFERENCE = re.compile(r"^[0-9a-f]{32}$")
_SUPPORTED_OPERATION = "orders.submit"
_APPROVED_TARGET_OPERATION = "orders.submit_approved_target"
RECOVERY_CLOSE_OPERATION = "orders.submit_recovery_close"
_SUPPORTED_OPERATIONS = frozenset(
    {_SUPPORTED_OPERATION, _APPROVED_TARGET_OPERATION, RECOVERY_CLOSE_OPERATION}
)


def _text(value: object, field_name: str, *, pattern: re.Pattern[str] = _IDENTIFIER) -> str:
    if type(value) is not str or value != value.strip() or not pattern.fullmatch(value):
        raise ContractValidationError(f"invalid {field_name}")
    return value


def _decimal_config(value: object, field_name: str) -> Decimal:
    if type(value) is not Decimal or not value.is_finite() or value <= 0:
        raise ContractValidationError(f"invalid {field_name}")
    return value


@dataclass(frozen=True, slots=True)
class ExecutionAdmissionContextV1:
    """Trusted identity and routing facts minted by the execution service handler."""

    principal_ref: str
    identity: WireIdentity
    wire_scope: WireScope
    execution_scope: ExecutionScope
    action: str
    recovery_target_intent_id: str | None = None

    def __post_init__(self) -> None:
        _text(self.principal_ref, "principal_ref")
        if (
            type(self.identity) is not WireIdentity
            or self.identity.principal_ref != self.principal_ref
        ):
            raise ContractValidationError("admission identity does not match principal")
        if self.identity.role not in {
            WireRole.STRATEGY_CLIENT,
            WireRole.APPLICATION_CLIENT,
            WireRole.SERVICE,
        }:
            raise ContractValidationError("admission identity has no write role")
        if (
            type(self.wire_scope) is not WireScope
            or type(self.execution_scope) is not ExecutionScope
        ):
            raise ContractValidationError("admission scopes are required")
        if type(self.action) is not str or self.action not in _SUPPORTED_OPERATIONS:
            raise ContractValidationError("unsupported execution admission action")
        if self.action == RECOVERY_CLOSE_OPERATION:
            _text(self.recovery_target_intent_id, "recovery target intent ID")
        elif self.recovery_target_intent_id is not None:
            raise ContractValidationError("recovery target is only valid for recovery close")
        if (
            self.wire_scope.account_ref is not None
            and self.wire_scope.account_ref != self.execution_scope.account_ref
        ):
            raise ContractValidationError("trusted account scope mapping is inconsistent")
        if (
            self.wire_scope.account_refs
            and self.execution_scope.account_ref not in self.wire_scope.account_refs
        ):
            raise ContractValidationError("trusted account set does not contain execution scope")
        if (
            self.wire_scope.strategy_ref is not None
            and self.wire_scope.strategy_ref != self.execution_scope.strategy_id
        ):
            raise ContractValidationError("trusted strategy scope mapping is inconsistent")

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "principal_ref": self.principal_ref,
            "identity": {
                "role": self.identity.role.value,
                "principal_ref": self.identity.principal_ref,
                "actor_ref": self.identity.actor_ref,
                "delegate_ref": self.identity.delegate_ref,
            },
            "wire_scope": {
                "execution_domain_ref": self.wire_scope.execution_domain_ref,
                "account_ref": self.wire_scope.account_ref,
                "account_refs": list(self.wire_scope.account_refs),
                "strategy_ref": self.wire_scope.strategy_ref,
                "run_id": self.wire_scope.run_id,
                "manual_book_ref": self.wire_scope.manual_book_ref,
                "portfolio_ref": self.wire_scope.portfolio_ref,
                "group_id": self.wire_scope.group_id,
                "leg_id": self.wire_scope.leg_id,
            },
            "execution_scope": self.execution_scope.to_payload(),
            "action": self.action,
        }
        if self.recovery_target_intent_id is not None:
            payload["recovery_target_intent_id"] = self.recovery_target_intent_id
        return payload

    @property
    def fingerprint(self) -> str:
        return payload_sha256(self.to_payload())


@dataclass(frozen=True, slots=True)
class ExecutionProviderOrderParametersV1:
    """Immutable provider parameters derived by a registered provider policy."""

    provider_id: str
    environment: str
    provider_instrument: str
    account_ref: str
    strategy_id: str
    trading_day: str | None
    intent_id: str
    signal_id: str
    intent_fingerprint: str
    side: Side
    position_effect: PositionEffect
    order_type: OrderType
    quantity: Decimal
    price: Decimal | None
    time_in_force: TimeInForce
    reduce_only: bool
    metadata_version: str
    deadline_ns: int | None
    tags: Mapping[str, str]

    def to_payload(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "environment": self.environment,
            "provider_instrument": self.provider_instrument,
            "account_ref": self.account_ref,
            "strategy_id": self.strategy_id,
            "trading_day": self.trading_day,
            "intent_id": self.intent_id,
            "signal_id": self.signal_id,
            "intent_fingerprint": self.intent_fingerprint,
            "side": self.side.value,
            "position_effect": self.position_effect.value,
            "order_type": self.order_type.value,
            "quantity": format(self.quantity, "f"),
            "price": None if self.price is None else format(self.price, "f"),
            "time_in_force": self.time_in_force.value,
            "reduce_only": self.reduce_only,
            "metadata_version": self.metadata_version,
            "deadline_ns": self.deadline_ns,
            "tags": dict(self.tags),
        }


class ExecutionAuthorizedDispatchPort(Protocol):
    """Provider port that consumes the policy-derived parameters at dispatch."""

    def submit_authorized(
        self,
        intent: OrderIntent,
        authorization: ExecutionAdmissionAuthorizationV1,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class ExecutionAdmissionAuthorizationV1:
    """In-process receipt passed only to the registered authorized provider port."""

    permit_reference: str
    context_fingerprint: str
    intent_fingerprint: str
    validator_id: str
    validator_version: str
    capability_fingerprint: str
    owner_id: str
    fencing_token: int
    issued_at_ns: int
    expires_at_ns: int
    parameters: ExecutionProviderOrderParametersV1


@dataclass(frozen=True, slots=True)
class ExecutionProviderValidatorV1:
    """Code-registered provider capability and exact order-parameter policy.

    Registration is a trusted composition-time operation.  It is deliberately
    data-bearing: allowed order/action/time-in-force sets, instrument mapping,
    quantity increments and maxima, and price ticks are required facts rather
    than a boolean allow callback.
    """

    provider_id: str
    validator_id: str
    version: str
    dispatch_port: ExecutionAuthorizedDispatchPort
    instrument_map: Mapping[str, str]
    quantity_steps: Mapping[str, Decimal]
    max_quantities: Mapping[str, Decimal]
    price_ticks: Mapping[str, Decimal]
    supported_order_types: tuple[OrderType, ...]
    supported_time_in_force: tuple[TimeInForce, ...]
    supported_actions: tuple[tuple[Side, PositionEffect], ...]
    supports_reduce_only: bool

    def __post_init__(self) -> None:
        _text(self.provider_id, "provider_id")
        _text(self.validator_id, "validator_id")
        _text(self.version, "validator version")
        if not callable(getattr(self.dispatch_port, "submit_authorized", None)):
            raise ContractValidationError("provider policy requires authorized dispatch port")
        if type(self.supports_reduce_only) is not bool:
            raise ContractValidationError("invalid reduce-only capability")
        mappings = (self.instrument_map, self.quantity_steps, self.max_quantities)
        if any(not isinstance(item, Mapping) or not item for item in mappings):
            raise ContractValidationError("provider order policy maps are required")

        instruments: dict[str, str] = {}
        for instrument, provider_instrument in self.instrument_map.items():
            instrument = _text(instrument, "instrument", pattern=_INSTRUMENT)
            provider_instrument = _text(
                provider_instrument, "provider instrument", pattern=_INSTRUMENT
            )
            instruments[instrument] = provider_instrument
        keys = set(instruments)
        steps = self._decimal_map(self.quantity_steps, "quantity step")
        maxima = self._decimal_map(self.max_quantities, "maximum quantity")
        ticks = self._decimal_map(self.price_ticks, "price tick", allow_empty=True)
        if set(steps) != keys or set(maxima) != keys or not set(ticks).issubset(keys):
            raise ContractValidationError("provider order policy instrument maps disagree")

        order_types = self._enum_tuple(self.supported_order_types, OrderType, "order types")
        time_in_force = self._enum_tuple(self.supported_time_in_force, TimeInForce, "time in force")
        if not order_types or not time_in_force:
            raise ContractValidationError("provider order capabilities are required")
        actions = tuple(self.supported_actions)
        if not actions or any(
            type(action) is not tuple
            or len(action) != 2
            or type(action[0]) is not Side
            or type(action[1]) is not PositionEffect
            for action in actions
        ):
            raise ContractValidationError("provider action capabilities are required")
        if len(set(actions)) != len(actions):
            raise ContractValidationError("duplicate provider action capability")
        if OrderType.LIMIT in order_types and set(ticks) != keys:
            raise ContractValidationError("limit policy requires a price tick for each instrument")

        object.__setattr__(self, "instrument_map", MappingProxyType(instruments))
        object.__setattr__(self, "quantity_steps", MappingProxyType(steps))
        object.__setattr__(self, "max_quantities", MappingProxyType(maxima))
        object.__setattr__(self, "price_ticks", MappingProxyType(ticks))
        object.__setattr__(self, "supported_order_types", order_types)
        object.__setattr__(self, "supported_time_in_force", time_in_force)
        object.__setattr__(self, "supported_actions", actions)

    @staticmethod
    def _enum_tuple(values: object, enum_type: type, field_name: str) -> tuple:
        if type(values) not in (tuple, list) or any(type(item) is not enum_type for item in values):
            raise ContractValidationError(f"invalid {field_name}")
        normalized = tuple(values)
        if len(set(normalized)) != len(normalized):
            raise ContractValidationError(f"duplicate {field_name}")
        return normalized

    @staticmethod
    def _decimal_map(
        values: Mapping[str, Decimal], field_name: str, *, allow_empty: bool = False
    ) -> dict[str, Decimal]:
        if not isinstance(values, Mapping) or (not values and not allow_empty):
            raise ContractValidationError(f"{field_name} map is required")
        return {
            _text(key, "instrument", pattern=_INSTRUMENT): _decimal_config(value, field_name)
            for key, value in values.items()
        }

    def capability_fingerprint(self) -> str:
        return payload_sha256(
            {
                "provider_id": self.provider_id,
                "validator_id": self.validator_id,
                "version": self.version,
                "instrument_map": dict(self.instrument_map),
                "quantity_steps": {
                    key: format(value, "f") for key, value in self.quantity_steps.items()
                },
                "max_quantities": {
                    key: format(value, "f") for key, value in self.max_quantities.items()
                },
                "price_ticks": {key: format(value, "f") for key, value in self.price_ticks.items()},
                "supported_order_types": [item.value for item in self.supported_order_types],
                "supported_time_in_force": [item.value for item in self.supported_time_in_force],
                "supported_actions": [
                    [side.value, effect.value] for side, effect in self.supported_actions
                ],
                "supports_reduce_only": self.supports_reduce_only,
            }
        )

    def validate(
        self,
        context: ExecutionAdmissionContextV1,
        intent: OrderIntent,
        dispatch_port: object,
        *,
        now_ns: int,
    ) -> tuple[ExecutionProviderOrderParametersV1, str]:
        if type(context) is not ExecutionAdmissionContextV1:
            raise ExecutionBasicAdmissionDenied("trusted admission context is missing")
        if type(intent) is not OrderIntent or intent.scope != context.execution_scope:
            raise ExecutionBasicAdmissionDenied("intent does not match trusted execution scope")
        if context.execution_scope.provider != self.provider_id:
            raise ExecutionBasicAdmissionDenied("provider policy does not match execution scope")
        if dispatch_port is not self.dispatch_port:
            raise ExecutionBasicAdmissionDenied("provider dispatch port is not code-registered")
        if not callable(getattr(dispatch_port, "submit_authorized", None)):
            raise ExecutionBasicAdmissionDenied("authorized provider dispatch is unavailable")
        if intent.instrument not in self.instrument_map:
            raise ExecutionBasicAdmissionDenied("instrument is not enabled by provider policy")
        if intent.order_type not in self.supported_order_types:
            raise ExecutionBasicAdmissionDenied("order type is not enabled by provider policy")
        if intent.time_in_force not in self.supported_time_in_force:
            raise ExecutionBasicAdmissionDenied("time in force is not enabled by provider policy")
        if (intent.side, intent.position_effect) not in self.supported_actions:
            raise ExecutionBasicAdmissionDenied("order action is not enabled by provider policy")
        if intent.reduce_only and not self.supports_reduce_only:
            raise ExecutionBasicAdmissionDenied("reduce-only is not enabled by provider policy")
        if intent.quantity % self.quantity_steps[intent.instrument] != 0:
            raise ExecutionBasicAdmissionDenied("quantity does not match provider increment")
        if intent.quantity > self.max_quantities[intent.instrument]:
            raise ExecutionBasicAdmissionDenied("quantity exceeds provider policy maximum")
        if intent.order_type is OrderType.LIMIT:
            tick = self.price_ticks.get(intent.instrument)
            if tick is None or intent.price is None or intent.price % tick != 0:
                raise ExecutionBasicAdmissionDenied("price does not match provider tick")
        if intent.deadline_ns is not None and intent.deadline_ns <= now_ns:
            raise ExecutionBasicAdmissionDenied("order intent has expired")

        parameters = ExecutionProviderOrderParametersV1(
            provider_id=self.provider_id,
            environment=context.execution_scope.environment,
            provider_instrument=self.instrument_map[intent.instrument],
            account_ref=context.execution_scope.account_ref,
            strategy_id=context.execution_scope.strategy_id,
            trading_day=context.execution_scope.trading_day,
            intent_id=intent.intent_id,
            signal_id=intent.signal_id,
            intent_fingerprint=intent.fingerprint,
            side=intent.side,
            position_effect=intent.position_effect,
            order_type=intent.order_type,
            quantity=intent.quantity,
            price=intent.price,
            time_in_force=intent.time_in_force,
            reduce_only=intent.reduce_only,
            metadata_version=intent.metadata_version,
            deadline_ns=intent.deadline_ns,
            tags=MappingProxyType(dict(intent.tags)),
        )
        binding = payload_sha256(
            {
                "capability_fingerprint": self.capability_fingerprint(),
                "parameters": parameters.to_payload(),
            }
        )
        return parameters, binding


class ExecutionAdmissionValidatorRegistry:
    """Immutable code-owned lookup for provider parameter and capability rules."""

    def __init__(self, validators: Iterable[ExecutionProviderValidatorV1]) -> None:
        copied: dict[str, ExecutionProviderValidatorV1] = {}
        for validator in validators:
            if type(validator) is not ExecutionProviderValidatorV1:
                raise ContractValidationError("execution admission requires typed code validators")
            if validator.provider_id in copied:
                raise ContractValidationError("duplicate provider admission validator")
            copied[validator.provider_id] = validator
        if not copied:
            raise ContractValidationError("execution admission validator registry is empty")
        self._validators = MappingProxyType(copied)

    def validate(
        self,
        context: ExecutionAdmissionContextV1,
        intent: OrderIntent,
        dispatch_port: object,
        *,
        now_ns: int,
    ) -> tuple[ExecutionProviderOrderParametersV1, str, ExecutionProviderValidatorV1]:
        validator = self._validators.get(context.execution_scope.provider)
        if validator is None:
            raise ExecutionBasicAdmissionDenied("provider has no code-registered validator")
        parameters, binding = validator.validate(context, intent, dispatch_port, now_ns=now_ns)
        return parameters, binding, validator


@dataclass(frozen=True, slots=True)
class _HeldPermit:
    context: ExecutionAdmissionContextV1
    context_fingerprint: str
    intent_fingerprint: str
    account_key: str
    strategy_id: str
    action: tuple[str, str, str]
    validator_id: str
    validator_version: str
    capability_fingerprint: str
    parameter_binding: str
    owner_id: str
    fencing_token: int
    issued_at_ns: int
    expires_at_ns: int
    parameters: ExecutionProviderOrderParametersV1
    dispatch_port: object


@dataclass(frozen=True, slots=True)
class _ClaimedAuthorization:
    authorization: ExecutionAdmissionAuthorizationV1
    held: _HeldPermit


class ExecutionBasicAdmissionGate:
    """Issue and consume context-, capability-, intent-, and owner-bound permits."""

    def __init__(
        self,
        store: SqliteExecutionStore,
        validators: ExecutionAdmissionValidatorRegistry,
        *,
        permit_ttl_ns: int = 5_000_000_000,
        clock_ns: Callable[[], int] = time.time_ns,
        max_active_permits: int = 10_000,
        retain_claimed_authorizations: bool = False,
    ) -> None:
        if type(store) is not SqliteExecutionStore:
            raise ContractValidationError("basic admission requires the execution SQLite store")
        if type(validators) is not ExecutionAdmissionValidatorRegistry:
            raise ContractValidationError("basic admission validator registry is required")
        if type(permit_ttl_ns) is not int or not 1_000_000 <= permit_ttl_ns <= 300_000_000_000:
            raise ContractValidationError("invalid execution admission permit TTL")
        if type(max_active_permits) is not int or not 1 <= max_active_permits <= 1_000_000:
            raise ContractValidationError("invalid execution admission capacity")
        if type(retain_claimed_authorizations) is not bool:
            raise ContractValidationError("invalid claimed authorization retention setting")
        if not callable(clock_ns):
            raise ContractValidationError("execution admission clock is required")
        self._store = store
        self._validators = validators
        self._permit_ttl_ns = permit_ttl_ns
        self._clock_ns = clock_ns
        self._max_active_permits = max_active_permits
        self._retain_claimed_authorizations = retain_claimed_authorizations
        self._lock = threading.RLock()
        self._permits: dict[str, _HeldPermit] = {}
        self._claiming: dict[str, object] = {}
        self._claimed_authorizations: dict[str, _ClaimedAuthorization] = {}

    def reserve_for_dispatch(
        self,
        context: ExecutionAdmissionContextV1,
        intent: OrderIntent,
        dispatch_port: object,
        writer_lease: WriterLease,
    ) -> str:
        """Validate current caller and provider facts, then issue an opaque ref."""

        now_ns = self._now()
        self._validate_request(context, intent, dispatch_port, writer_lease, now_ns=now_ns)
        parameters, binding, validator = self._validators.validate(
            context, intent, dispatch_port, now_ns=now_ns
        )
        reference = uuid.uuid4().hex
        held = _HeldPermit(
            context=context,
            context_fingerprint=context.fingerprint,
            intent_fingerprint=intent.fingerprint,
            account_key=intent.scope.account_key,
            strategy_id=intent.scope.strategy_id,
            action=(context.action, intent.side.value, intent.position_effect.value),
            validator_id=validator.validator_id,
            validator_version=validator.version,
            capability_fingerprint=validator.capability_fingerprint(),
            parameter_binding=binding,
            owner_id=writer_lease.owner_id,
            fencing_token=writer_lease.fencing_token,
            issued_at_ns=now_ns,
            expires_at_ns=now_ns + self._permit_ttl_ns,
            parameters=parameters,
            dispatch_port=dispatch_port,
        )
        with self._lock:
            self._purge_expired_locked(now_ns)
            active_count = len(self._permits)
            if self._retain_claimed_authorizations:
                active_count += len(self._claiming) + len(self._claimed_authorizations)
            if active_count >= self._max_active_permits:
                raise ExecutionBasicAdmissionDenied("execution admission capacity is full")
            self._permits[reference] = held
        return reference

    def needs_reauthorization(
        self,
        reference: str | None,
        context: ExecutionAdmissionContextV1,
        intent: OrderIntent,
        dispatch_port: object,
        writer_lease: WriterLease,
    ) -> bool:
        """Detect process-local registry loss or owner handoff for a pending row.

        Permit expiry is intentionally not treated as registry loss: an expired
        in-process token fails the final claim.  A missing token after process
        restart may be reissued only while the durable row is still PENDING.
        """

        if not isinstance(reference, str) or not _REFERENCE.fullmatch(reference):
            return True
        with self._lock:
            held = self._permits.get(reference)
            if held is None:
                return True
            return not (
                held.context_fingerprint == context.fingerprint
                and held.intent_fingerprint == intent.fingerprint
                and held.account_key == intent.scope.account_key
                and held.strategy_id == intent.scope.strategy_id
                and held.action == (context.action, intent.side.value, intent.position_effect.value)
                and held.owner_id == writer_lease.owner_id
                and held.fencing_token == writer_lease.fencing_token
                and held.dispatch_port is dispatch_port
            )

    def reauthorize_pending_dispatch(
        self,
        context: ExecutionAdmissionContextV1,
        intent: OrderIntent,
        dispatch_port: object,
        writer_lease: WriterLease,
    ) -> str:
        """Issue a fresh ref after the facade has proved exact pending row state."""

        return self.reserve_for_dispatch(context, intent, dispatch_port, writer_lease)

    def claim_for_dispatch(
        self,
        reference: str,
        intent: OrderIntent,
        context: ExecutionAdmissionContextV1,
        dispatch_port: object,
        writer_lease: WriterLease,
    ) -> ExecutionAdmissionAuthorizationV1:
        """Consume one permit and revalidate the same facts at final claim."""

        if type(reference) is not str or not _REFERENCE.fullmatch(reference):
            raise ExecutionBasicAdmissionDenied("basic admission reference is invalid")
        claim_token = object()
        with self._lock:
            held = self._permits.pop(reference, None)
            if held is not None:
                self._claiming[reference] = claim_token
        if held is None:
            raise ExecutionBasicAdmissionDenied("basic admission reference is unknown or consumed")

        try:
            now_ns = self._now()
            with self._lock:
                self._purge_expired_locked(now_ns)
            if now_ns >= held.expires_at_ns:
                raise ExecutionBasicAdmissionDenied("basic admission reference expired")
            self._validate_request(context, intent, dispatch_port, writer_lease, now_ns=now_ns)
            if (
                held.context_fingerprint != context.fingerprint
                or held.intent_fingerprint != intent.fingerprint
                or held.account_key != intent.scope.account_key
                or held.strategy_id != intent.scope.strategy_id
                or held.action != (context.action, intent.side.value, intent.position_effect.value)
                or held.owner_id != writer_lease.owner_id
                or held.fencing_token != writer_lease.fencing_token
                or held.dispatch_port is not dispatch_port
            ):
                raise ExecutionBasicAdmissionDenied("basic admission binding changed")

            parameters, binding, validator = self._validators.validate(
                context, intent, dispatch_port, now_ns=now_ns
            )
            capability_fingerprint = validator.capability_fingerprint()
            if (
                validator.validator_id != held.validator_id
                or validator.version != held.validator_version
                or capability_fingerprint != held.capability_fingerprint
                or binding != held.parameter_binding
                or payload_sha256(parameters.to_payload())
                != payload_sha256(held.parameters.to_payload())
            ):
                raise ExecutionBasicAdmissionDenied("provider capability changed after admission")
            # Recheck expiry and fencing after the pure policy evaluation as part
            # of the final dispatch claim. The facade calls this only after its
            # pre-dispatch guards, immediately before invoking the authorized port.
            final_now_ns = self._now()
            if final_now_ns >= held.expires_at_ns:
                raise ExecutionBasicAdmissionDenied("basic admission reference expired")
            self._store.assert_writer_lease(intent.scope, writer_lease)
            authorization = ExecutionAdmissionAuthorizationV1(
                permit_reference=reference,
                context_fingerprint=context.fingerprint,
                intent_fingerprint=intent.fingerprint,
                validator_id=validator.validator_id,
                validator_version=validator.version,
                capability_fingerprint=capability_fingerprint,
                owner_id=writer_lease.owner_id,
                fencing_token=writer_lease.fencing_token,
                issued_at_ns=held.issued_at_ns,
                expires_at_ns=held.expires_at_ns,
                parameters=parameters,
            )
            with self._lock:
                if self._claiming.get(reference) is not claim_token:
                    raise ExecutionBasicAdmissionDenied("basic admission claim is no longer current")
                self._claiming.pop(reference)
                if self._retain_claimed_authorizations:
                    self._claimed_authorizations[reference] = _ClaimedAuthorization(
                        authorization=authorization,
                        held=held,
                    )
            return authorization
        finally:
            with self._lock:
                if self._claiming.get(reference) is claim_token:
                    self._claiming.pop(reference)

    def verify_claimed_authorization(
        self,
        authorization: ExecutionAdmissionAuthorizationV1,
        intent: OrderIntent,
        dispatch_port: object,
        writer_lease: WriterLease,
    ) -> ExecutionAdmissionAuthorizationV1:
        """Revalidate an exact prior claim without consuming or renewing it."""

        if not self._retain_claimed_authorizations:
            raise ExecutionBasicAdmissionDenied("claimed authorization retention is disabled")
        if type(authorization) is not ExecutionAdmissionAuthorizationV1:
            raise ExecutionBasicAdmissionDenied("claimed authorization is invalid")
        reference = authorization.permit_reference
        if type(reference) is not str or not _REFERENCE.fullmatch(reference):
            raise ExecutionBasicAdmissionDenied("claimed authorization reference is invalid")
        now_ns = self._now()
        with self._lock:
            claimed = self._claimed_authorizations.get(reference)
        if claimed is None or claimed.authorization is not authorization:
            raise ExecutionBasicAdmissionDenied("claimed authorization is unknown or not exact")
        held = claimed.held
        context = held.context
        if now_ns >= held.expires_at_ns:
            raise ExecutionBasicAdmissionDenied("claimed authorization expired")
        if (
            authorization.context_fingerprint != held.context_fingerprint
            or authorization.intent_fingerprint != held.intent_fingerprint
            or authorization.validator_id != held.validator_id
            or authorization.validator_version != held.validator_version
            or authorization.capability_fingerprint != held.capability_fingerprint
            or authorization.owner_id != held.owner_id
            or authorization.fencing_token != held.fencing_token
            or authorization.issued_at_ns != held.issued_at_ns
            or authorization.expires_at_ns != held.expires_at_ns
            or payload_sha256(authorization.parameters.to_payload())
            != payload_sha256(held.parameters.to_payload())
            or dispatch_port is not held.dispatch_port
            or type(intent) is not OrderIntent
            or intent.scope != context.execution_scope
            or intent.fingerprint != held.intent_fingerprint
            or intent.scope.account_key != held.account_key
            or intent.scope.strategy_id != held.strategy_id
            or held.action != (context.action, intent.side.value, intent.position_effect.value)
            or type(writer_lease) is not WriterLease
            or writer_lease.owner_id != held.owner_id
            or writer_lease.fencing_token != held.fencing_token
        ):
            raise ExecutionBasicAdmissionDenied("claimed authorization binding changed")

        self._validate_request(context, intent, dispatch_port, writer_lease, now_ns=now_ns)
        parameters, binding, validator = self._validators.validate(
            context, intent, dispatch_port, now_ns=now_ns
        )
        if (
            validator.validator_id != held.validator_id
            or validator.version != held.validator_version
            or validator.capability_fingerprint() != held.capability_fingerprint
            or binding != held.parameter_binding
            or payload_sha256(parameters.to_payload())
            != payload_sha256(held.parameters.to_payload())
            or payload_sha256(parameters.to_payload())
            != payload_sha256(authorization.parameters.to_payload())
        ):
            raise ExecutionBasicAdmissionDenied("provider capability changed after admission")
        self._store.assert_writer_lease(intent.scope, writer_lease)
        final_now_ns = self._now()
        with self._lock:
            if (
                self._claimed_authorizations.get(reference) is not claimed
                or claimed.authorization is not authorization
            ):
                raise ExecutionBasicAdmissionDenied("claimed authorization is no longer current")
            if final_now_ns >= held.expires_at_ns:
                raise ExecutionBasicAdmissionDenied("claimed authorization expired")
        return authorization

    def revoke(self, reference: str) -> bool:
        """Drop an unclaimed in-memory reference; validators have no external reserve."""

        if type(reference) is not str or not _REFERENCE.fullmatch(reference):
            return False
        with self._lock:
            return self._permits.pop(reference, None) is not None

    def _validate_request(
        self,
        context: ExecutionAdmissionContextV1,
        intent: OrderIntent,
        dispatch_port: object,
        writer_lease: WriterLease,
        *,
        now_ns: int,
    ) -> None:
        if type(context) is not ExecutionAdmissionContextV1:
            raise ExecutionBasicAdmissionDenied("trusted admission context is missing")
        if type(intent) is not OrderIntent or intent.scope != context.execution_scope:
            raise ExecutionBasicAdmissionDenied("intent does not match trusted context")
        if context.action == RECOVERY_CLOSE_OPERATION and (
            context.recovery_target_intent_id is None
            or intent.position_effect.value not in {"CLOSE", "CLOSE_TODAY", "CLOSE_YESTERDAY"}
            or intent.reduce_only is not True
        ):
            raise ExecutionBasicAdmissionDenied(
                "recovery operation requires a bound reduce-only close intent"
            )
        if type(writer_lease) is not WriterLease:
            raise ExecutionBasicAdmissionDenied("current writer lease is required")
        self._store.assert_writer_lease(intent.scope, writer_lease)
        if type(now_ns) is not int or now_ns <= 0:
            raise ExecutionBasicAdmissionDenied("execution admission clock is invalid")

    def _now(self) -> int:
        try:
            value = self._clock_ns()
        except Exception as error:
            raise ExecutionBasicAdmissionDenied("execution admission clock failed") from error
        if type(value) is not int or value <= 0:
            raise ExecutionBasicAdmissionDenied("execution admission clock is invalid")
        return value

    def _purge_expired_locked(self, now_ns: int) -> None:
        expired = [
            reference
            for reference, permit in self._permits.items()
            if permit.expires_at_ns <= now_ns
        ]
        for reference in expired:
            del self._permits[reference]
        if self._retain_claimed_authorizations:
            expired_claims = [
                reference
                for reference, claimed in self._claimed_authorizations.items()
                if claimed.held.expires_at_ns <= now_ns
            ]
            for reference in expired_claims:
                del self._claimed_authorizations[reference]


__all__ = [
    "ExecutionAdmissionAuthorizationV1",
    "ExecutionAdmissionContextV1",
    "ExecutionAdmissionValidatorRegistry",
    "ExecutionAuthorizedDispatchPort",
    "ExecutionBasicAdmissionGate",
    "ExecutionProviderOrderParametersV1",
    "ExecutionProviderValidatorV1",
]
