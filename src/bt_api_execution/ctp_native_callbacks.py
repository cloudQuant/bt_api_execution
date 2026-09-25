"""Structural envelopes for exact native CTP order and cancel callbacks.

This module translates a small set of CTP callback fields into the durable
callback key used by :mod:`bt_api_execution.store`.  It is a mapping layer,
not a source verifier: callers must supply the session binding captured by the
native client, and the store's default callback verifier continues to reject.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import TypeAlias
from uuid import uuid4

from .errors import ContractValidationError
from .store import CtpDispatchCallbackKey, CtpDispatchCorrelationKey

_NativeValue: TypeAlias = str | int | bool | None
_ORDER_ACTION_SOURCES = {"OnRspOrderAction", "OnErrRtnOrderAction"}
_ORDER_RETURN_FIELDS = frozenset(
    {
        "BrokerID",
        "InvestorID",
        "UserID",
        "InstrumentID",
        "RequestID",
        "OrderRef",
        "ExchangeID",
        "OrderSysID",
        "FrontID",
        "SessionID",
        "TradingDay",
        "OrderStatus",
        "OrderSubmitStatus",
        "VolumeTraded",
        "VolumeTotal",
        "UpdateTime",
        "NotifySequence",
        "SequenceNo",
    }
)
_ORDER_ACTION_FIELDS = frozenset(
    {
        "BrokerID",
        "InvestorID",
        "UserID",
        "InstrumentID",
        "RequestID",
        "OrderActionRef",
        "OrderRef",
        "ExchangeID",
        "OrderSysID",
        "FrontID",
        "SessionID",
        "ActionFlag",
        "ActionDate",
        "ActionTime",
        "OrderActionStatus",
    }
)


def _validate_text(value: object, field_name: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > 128
        or not value.isascii()
        or any(not (char.isalnum() or char in "._:-") for char in value)
    ):
        raise ContractValidationError("invalid native CTP " + field_name)
    return value


def _read_field(field: object, name: str) -> object:
    try:
        if isinstance(field, Mapping):
            return field.get(name)
        return getattr(field, name)
    except Exception as exc:
        raise ContractValidationError("native CTP callback field is unavailable") from exc


def _native_text(field: object, name: str, *, required: bool = False) -> str | None:
    try:
        value = _read_field(field, name)
    except ContractValidationError:
        if required:
            raise
        return None
    if value is None:
        result = ""
    elif isinstance(value, bytes):
        try:
            result = value.decode("utf-8").rstrip("\x00").strip()
        except UnicodeDecodeError as exc:
            raise ContractValidationError("native CTP callback text is not UTF-8") from exc
    elif type(value) is str:
        result = value.rstrip("\x00").strip()
    elif type(value) is int and not isinstance(value, bool):
        result = str(value)
    else:
        raise ContractValidationError("native CTP callback text has an invalid type")
    if required and not result:
        raise ContractValidationError("native CTP callback field is required: " + name)
    return result or None


def _native_int(field: object, name: str, *, required: bool = False) -> int | None:
    try:
        value = _read_field(field, name)
    except ContractValidationError:
        if required:
            raise
        return None
    if value is None or value == "":
        if required:
            raise ContractValidationError("native CTP callback field is required: " + name)
        return None
    if type(value) is not int:
        raise ContractValidationError("native CTP callback integer has an invalid type")
    if required and value <= 0:
        raise ContractValidationError("native CTP callback integer must be positive")
    return value


def _native_bool(value: object, field_name: str) -> bool:
    if type(value) is bool:
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    raise ContractValidationError("native CTP callback flag is invalid: " + field_name)


def _error_id(response_info: object | None) -> int | None:
    if response_info is None:
        return None
    return _native_int(response_info, "ErrorID")


def _account_fingerprint(field: object) -> str:
    broker_id = _native_text(field, "BrokerID", required=True)
    user_id = _native_text(field, "UserID", required=True)
    digest = sha256(f"{broker_id}:{user_id}".encode()).hexdigest()[:16]
    return "acct_" + digest


def ctp_native_session_generation_id(
    native_api_generation: int,
    connection_generation: int,
    session_epoch: str,
    dispatch_front_id: int,
    dispatch_session_id: int,
) -> str:
    """Build the stable key for one non-reused native TraderClient session."""

    if (
        type(native_api_generation) is not int
        or native_api_generation <= 0
        or type(connection_generation) is not int
        or connection_generation <= 0
        or type(dispatch_front_id) is not int
        or dispatch_front_id <= 0
        or type(dispatch_session_id) is not int
        or dispatch_session_id <= 0
    ):
        raise ContractValidationError("invalid native CTP session generation")
    if (
        type(session_epoch) is not str
        or len(session_epoch) != 32
        or any(char not in "0123456789abcdef" for char in session_epoch)
    ):
        raise ContractValidationError("invalid native CTP session epoch")
    return (
        f"ctp-native-v2:{session_epoch}:{native_api_generation}:"
        f"{connection_generation}:{dispatch_front_id}:{dispatch_session_id}"
    )


def ctp_native_session_epoch() -> str:
    """Create a fresh opaque epoch for one code-owned native client session.

    The caller must generate this once when constructing a new native client,
    retain it for that client's lifetime, and never restore an old epoch after
    reconstructing a client. The value prevents local generation counters
    from aliasing across restarts; it is not source attestation.
    """

    return uuid4().hex


@dataclass(frozen=True, slots=True)
class CtpNativeSessionContext:
    """Session facts bound to the command before translating a native event.

    ``account_fingerprint`` and native generation fields are intended to be
    captured from the native TraderClient session. ``session_epoch`` must be
    generated once by code when constructing that client and retained for its
    lifetime. The account/scope/session keys are the execution-side binding.
    This caller-constructible value is not source provenance; a trusted
    verifier still has to establish that those two sides belong together.
    """

    account_key: str
    scope_key: str
    trading_day: str
    session_epoch: str
    session_generation_id: str
    account_fingerprint: str
    native_api_generation: int
    connection_generation: int
    dispatch_front_id: int
    dispatch_session_id: int

    def __post_init__(self) -> None:
        _validate_text(self.account_key, "account key")
        _validate_text(self.scope_key, "scope key")
        if (
            type(self.session_epoch) is not str
            or len(self.session_epoch) != 32
            or any(char not in "0123456789abcdef" for char in self.session_epoch)
        ):
            raise ContractValidationError("invalid native CTP session epoch")
        _validate_text(self.session_generation_id, "session generation")
        if self.session_generation_id != ctp_native_session_generation_id(
            self.native_api_generation,
            self.connection_generation,
            self.session_epoch,
            self.dispatch_front_id,
            self.dispatch_session_id,
        ):
            raise ContractValidationError(
                "native CTP session generation does not match its source generations"
            )
        if (
            type(self.trading_day) is not str
            or len(self.trading_day) != 8
            or not self.trading_day.isascii()
            or not self.trading_day.isdigit()
        ):
            raise ContractValidationError("invalid native CTP trading day")
        if (
            type(self.account_fingerprint) is not str
            or len(self.account_fingerprint) != 21
            or not self.account_fingerprint.startswith("acct_")
            or any(char not in "0123456789abcdef" for char in self.account_fingerprint[5:])
        ):
            raise ContractValidationError("invalid native CTP account fingerprint")
        for value, name in (
            (self.native_api_generation, "native API generation"),
            (self.connection_generation, "connection generation"),
            (self.dispatch_front_id, "dispatch front id"),
            (self.dispatch_session_id, "dispatch session id"),
        ):
            if type(value) is not int or value <= 0:
                raise ContractValidationError("invalid native CTP " + name)

    def require_correlation(self, correlation: CtpDispatchCorrelationKey) -> None:
        if type(correlation) is not CtpDispatchCorrelationKey:
            raise ContractValidationError("typed CTP dispatch correlation key is required")
        if (
            self.account_key != correlation.account_key
            or self.scope_key != correlation.scope_key
            or self.trading_day != correlation.trading_day
            or self.session_generation_id != correlation.session_generation_id
            or self.dispatch_front_id != correlation.dispatch_front_id
            or self.dispatch_session_id != correlation.dispatch_session_id
        ):
            raise ContractValidationError("native CTP callback session does not match command")

    @property
    def stream_id(self) -> str:
        material = ":".join(
            (
                self.account_key,
                self.scope_key,
                self.trading_day,
                self.session_epoch,
                self.session_generation_id,
                str(self.native_api_generation),
                str(self.connection_generation),
                str(self.dispatch_front_id),
                str(self.dispatch_session_id),
            )
        )
        return "ctp-native:" + sha256(material.encode("ascii")).hexdigest()

    def to_payload(self) -> dict[str, _NativeValue]:
        return {
            "account_key": self.account_key,
            "scope_key": self.scope_key,
            "trading_day": self.trading_day,
            "session_epoch": self.session_epoch,
            "session_generation_id": self.session_generation_id,
            "account_fingerprint": self.account_fingerprint,
            "native_api_generation": self.native_api_generation,
            "connection_generation": self.connection_generation,
            "dispatch_front_id": self.dispatch_front_id,
            "dispatch_session_id": self.dispatch_session_id,
        }


@dataclass(frozen=True, slots=True)
class CtpNativeCallbackEnvelope:
    """Mapped native callback facts ready for an injected source verifier.

    The deterministic event id is an idempotency key for the native event
    identity exposed by CTP; it does not authenticate the event.  The payload
    deliberately omits status-message text and credential fields.
    """

    callback_key: CtpDispatchCallbackKey
    source_callback: str
    source_scope: CtpNativeSessionContext
    event_identity_basis: str
    native_fields: tuple[tuple[str, _NativeValue], ...]
    response_error_id: int | None = None
    response_is_last: bool | None = None

    def __post_init__(self) -> None:
        if type(self.callback_key) is not CtpDispatchCallbackKey:
            raise ContractValidationError("typed CTP callback key is required")
        if type(self.source_scope) is not CtpNativeSessionContext:
            raise ContractValidationError("typed native CTP session context is required")
        _validate_text(self.source_callback, "callback source")
        _validate_text(self.event_identity_basis, "event identity basis")
        self.source_scope.require_correlation(self.callback_key.correlation_key)
        if self.source_callback == "OnRtnOrder":
            allowed_fields = _ORDER_RETURN_FIELDS
            allowed_bases = {
                "CThostFtdcOrderField.NotifySequence",
                "CThostFtdcOrderField.SequenceNo",
            }
            if self.callback_key.callback_family != "ORDER":
                raise ContractValidationError("native CTP order callback family is invalid")
        elif self.source_callback in _ORDER_ACTION_SOURCES:
            allowed_fields = _ORDER_ACTION_FIELDS
            allowed_bases = {"RequestID-OrderActionRef-callback-source"}
            if self.callback_key.callback_family != "CANCEL_ACTION":
                raise ContractValidationError("native CTP action callback family is invalid")
        else:
            raise ContractValidationError("unsupported native CTP callback source")
        if self.event_identity_basis not in allowed_bases:
            raise ContractValidationError("native CTP callback event identity basis is invalid")
        object.__setattr__(
            self,
            "native_fields",
            _freeze_native_fields(self.native_fields, allowed_fields),
        )
        if self.response_error_id is not None and type(self.response_error_id) is not int:
            raise ContractValidationError("native CTP response error id is invalid")
        if self.response_is_last is not None and type(self.response_is_last) is not bool:
            raise ContractValidationError("native CTP response terminal flag is invalid")
        if self.source_callback == "OnRspOrderAction":
            if self.response_is_last is not True:
                raise ContractValidationError("native CTP action response is not terminal")
        elif self.response_is_last is not None:
            raise ContractValidationError("native CTP action error has no response terminal flag")

    def to_payload(self) -> dict[str, object]:
        """Return detached JSON-safe facts for ``apply_ctp_verified_dispatch_callback``."""

        return {
            "envelope_type": "ctp_native_callback_envelope.v1",
            "source_callback": self.source_callback,
            "event_identity_basis": self.event_identity_basis,
            "source_scope": self.source_scope.to_payload(),
            "native_fields": dict(self.native_fields),
            "response_error_id": self.response_error_id,
            "response_is_last": self.response_is_last,
        }


def _freeze_native_fields(
    native_fields: object, allowed_fields: frozenset[str]
) -> tuple[tuple[str, _NativeValue], ...]:
    """Copy a native-field mapping into an allowlisted immutable tuple."""

    try:
        if isinstance(native_fields, Mapping):
            entries: Iterable[object] = tuple(native_fields.items())
        else:
            entries = tuple(native_fields)  # type: ignore[arg-type]
    except Exception as exc:
        raise ContractValidationError("native CTP callback fields are invalid") from exc

    frozen: list[tuple[str, _NativeValue]] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, (tuple, list)) or len(entry) != 2:
            raise ContractValidationError("native CTP callback field entry is invalid")
        name, value = entry
        if type(name) is not str or name not in allowed_fields:
            raise ContractValidationError("native CTP callback field is not allowlisted")
        if name in seen:
            raise ContractValidationError("native CTP callback field is duplicated")
        if type(value) not in (str, int, bool, type(None)):
            raise ContractValidationError("native CTP callback field value is not primitive")
        if type(value) is str and (len(value) > 256 or not value.isascii() or "\x00" in value):
            raise ContractValidationError("native CTP callback field text is invalid")
        seen.add(name)
        frozen.append((name, value))
    return tuple(frozen)


def _make_callback_key(
    correlation: CtpDispatchCorrelationKey,
    context: CtpNativeSessionContext,
    *,
    callback_family: str,
    event_id: str,
    native_request_id: int,
    native_action_ref: str | None,
    order_ref: str,
    exchange_id: str | None = None,
    order_sys_id: str | None = None,
    target_front_id: int | None = None,
    target_session_id: int | None = None,
) -> CtpDispatchCallbackKey:
    return CtpDispatchCallbackKey(
        version=1,
        correlation_key=correlation,
        callback_family=callback_family,
        stream_id=context.stream_id,
        event_id=event_id,
        native_request_id=native_request_id,
        native_action_ref=native_action_ref,
        order_ref=order_ref,
        exchange_id=exchange_id,
        order_sys_id=order_sys_id,
        target_front_id=target_front_id,
        target_session_id=target_session_id,
    )


def map_ctp_native_order_return(
    correlation: CtpDispatchCorrelationKey,
    session: CtpNativeSessionContext,
    order_field: object,
) -> CtpNativeCallbackEnvelope:
    """Map one ``OnRtnOrder`` field when it carries exact request and event ids.

    CTP 6.7.7's order field carries RequestID, OrderRef, TradingDay, FrontID,
    SessionID and native notification sequence fields.  Events without a
    positive ``NotifySequence`` or ``SequenceNo`` are rejected instead of
    receiving a generated timestamp or counter.
    """

    if type(correlation) is not CtpDispatchCorrelationKey:
        raise ContractValidationError("typed CTP dispatch correlation key is required")
    session.require_correlation(correlation)
    if correlation.operation != "SUBMIT":
        raise ContractValidationError("native CTP order return requires a submit command")

    order_ref = _native_text(order_field, "OrderRef", required=True)
    request_id = _native_int(order_field, "RequestID", required=True)
    front_id = _native_int(order_field, "FrontID", required=True)
    session_id = _native_int(order_field, "SessionID", required=True)
    trading_day = _native_text(order_field, "TradingDay", required=True)
    if order_ref != correlation.order_ref:
        raise ContractValidationError("native CTP order OrderRef does not match command")
    if request_id != correlation.native_request_id:
        raise ContractValidationError("native CTP order RequestID does not match command")
    if (front_id, session_id) != (correlation.dispatch_front_id, correlation.dispatch_session_id):
        raise ContractValidationError("native CTP order session does not match command")
    if trading_day != correlation.trading_day:
        raise ContractValidationError("native CTP order TradingDay does not match command")
    if _account_fingerprint(order_field) != session.account_fingerprint:
        raise ContractValidationError("native CTP order account does not match session")

    notify_sequence = _native_int(order_field, "NotifySequence")
    sequence_no = _native_int(order_field, "SequenceNo")
    if notify_sequence is not None and notify_sequence > 0:
        event_sequence = notify_sequence
        event_basis = "CThostFtdcOrderField.NotifySequence"
    elif sequence_no is not None and sequence_no > 0:
        event_sequence = sequence_no
        event_basis = "CThostFtdcOrderField.SequenceNo"
    else:
        raise ContractValidationError("native CTP order has no stable event sequence")

    exchange_id = _native_text(order_field, "ExchangeID")
    order_sys_id = _native_text(order_field, "OrderSysID")
    if not exchange_id or not order_sys_id:
        exchange_id = None
        order_sys_id = None
    key = _make_callback_key(
        correlation,
        session,
        callback_family="ORDER",
        event_id=f"on-rtn-order:{event_sequence}:{order_ref}",
        native_request_id=request_id,
        native_action_ref=None,
        order_ref=order_ref,
        exchange_id=exchange_id,
        order_sys_id=order_sys_id,
    )
    fields = (
        ("BrokerID", _native_text(order_field, "BrokerID", required=True)),
        ("InvestorID", _native_text(order_field, "InvestorID", required=True)),
        ("UserID", _native_text(order_field, "UserID", required=True)),
        ("InstrumentID", _native_text(order_field, "InstrumentID", required=True)),
        ("RequestID", request_id),
        ("OrderRef", order_ref),
        ("ExchangeID", _native_text(order_field, "ExchangeID")),
        ("OrderSysID", _native_text(order_field, "OrderSysID")),
        ("FrontID", front_id),
        ("SessionID", session_id),
        ("TradingDay", trading_day),
        ("OrderStatus", _native_text(order_field, "OrderStatus")),
        ("OrderSubmitStatus", _native_text(order_field, "OrderSubmitStatus")),
        ("VolumeTraded", _native_int(order_field, "VolumeTraded")),
        ("VolumeTotal", _native_int(order_field, "VolumeTotal")),
        ("UpdateTime", _native_text(order_field, "UpdateTime")),
        ("NotifySequence", notify_sequence),
        ("SequenceNo", sequence_no),
    )
    return CtpNativeCallbackEnvelope(
        callback_key=key,
        source_callback="OnRtnOrder",
        source_scope=session,
        event_identity_basis=event_basis,
        native_fields=fields,
    )


def map_ctp_native_order_action_callback(
    correlation: CtpDispatchCorrelationKey,
    session: CtpNativeSessionContext,
    source_callback: str,
    action_field: object,
    response_info: object | None = None,
    *,
    request_id: int | None = None,
    is_last: object | None = None,
) -> CtpNativeCallbackEnvelope:
    """Map one exact native cancel action response or error callback.

    ``OnRspOrderAction`` supplies RequestID as a callback argument and the
    action field; both must agree and the response must be terminal.
    ``OnErrRtnOrderAction`` has no separate RequestID argument, so its field
    must carry the exact request id.
    """

    if type(correlation) is not CtpDispatchCorrelationKey:
        raise ContractValidationError("typed CTP dispatch correlation key is required")
    session.require_correlation(correlation)
    if correlation.operation != "CANCEL":
        raise ContractValidationError("native CTP order action requires a cancel command")
    if source_callback not in _ORDER_ACTION_SOURCES:
        raise ContractValidationError("unsupported native CTP order action callback")

    field_request_id = _native_int(action_field, "RequestID", required=True)
    if source_callback == "OnRspOrderAction":
        if type(request_id) is not int or request_id <= 0 or request_id != field_request_id:
            raise ContractValidationError("native CTP action RequestID fields do not match")
        terminal = _native_bool(is_last, "bIsLast")
        if not terminal:
            raise ContractValidationError("native CTP action response is not terminal")
        native_request_id = request_id
        response_is_last: bool | None = terminal
    else:
        if request_id is not None or is_last is not None:
            raise ContractValidationError("native CTP action error has no response arguments")
        native_request_id = field_request_id
        response_is_last = None
    if native_request_id != correlation.native_request_id:
        raise ContractValidationError("native CTP action RequestID does not match command")

    order_ref = _native_text(action_field, "OrderRef", required=True)
    action_ref = _native_text(action_field, "OrderActionRef", required=True)
    exchange_id = _native_text(action_field, "ExchangeID", required=True)
    order_sys_id = _native_text(action_field, "OrderSysID", required=True)
    target_front_id = _native_int(action_field, "FrontID", required=True)
    target_session_id = _native_int(action_field, "SessionID", required=True)
    action_flag = _native_text(action_field, "ActionFlag", required=True)
    if (
        order_ref != correlation.order_ref
        or action_ref != correlation.native_action_ref
        or exchange_id != correlation.cancel_target_exchange_id
        or order_sys_id != correlation.cancel_target_order_sys_id
        or target_front_id != correlation.cancel_target_front_id
        or target_session_id != correlation.cancel_target_session_id
        or action_flag != "0"
    ):
        raise ContractValidationError("native CTP action target does not match command")
    if _account_fingerprint(action_field) != session.account_fingerprint:
        raise ContractValidationError("native CTP action account does not match session")

    error_id = _error_id(response_info)
    event_id = f"{source_callback.lower()}:{native_request_id}:{action_ref}"
    key = _make_callback_key(
        correlation,
        session,
        callback_family="CANCEL_ACTION",
        event_id=event_id,
        native_request_id=native_request_id,
        native_action_ref=action_ref,
        order_ref=order_ref,
        exchange_id=exchange_id,
        order_sys_id=order_sys_id,
        target_front_id=target_front_id,
        target_session_id=target_session_id,
    )
    fields = (
        ("BrokerID", _native_text(action_field, "BrokerID", required=True)),
        ("InvestorID", _native_text(action_field, "InvestorID", required=True)),
        ("UserID", _native_text(action_field, "UserID", required=True)),
        ("InstrumentID", _native_text(action_field, "InstrumentID", required=True)),
        ("RequestID", native_request_id),
        ("OrderActionRef", action_ref),
        ("OrderRef", order_ref),
        ("ExchangeID", exchange_id),
        ("OrderSysID", order_sys_id),
        ("FrontID", target_front_id),
        ("SessionID", target_session_id),
        ("ActionFlag", action_flag),
        ("ActionDate", _native_text(action_field, "ActionDate")),
        ("ActionTime", _native_text(action_field, "ActionTime")),
        ("OrderActionStatus", _native_text(action_field, "OrderActionStatus")),
    )
    return CtpNativeCallbackEnvelope(
        callback_key=key,
        source_callback=source_callback,
        source_scope=session,
        event_identity_basis="RequestID-OrderActionRef-callback-source",
        native_fields=fields,
        response_error_id=error_id,
        response_is_last=response_is_last,
    )


__all__ = [
    "CtpNativeCallbackEnvelope",
    "CtpNativeSessionContext",
    "ctp_native_session_epoch",
    "ctp_native_session_generation_id",
    "map_ctp_native_order_action_callback",
    "map_ctp_native_order_return",
]
