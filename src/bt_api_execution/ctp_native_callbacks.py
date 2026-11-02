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
_ORDER_INSERT_SOURCES = {"OnRspOrderInsert", "OnErrRtnOrderInsert"}
_RESPONSE_TERMINAL_SOURCES = {"OnRspOrderAction", "OnRspOrderInsert"}
_NO_RESPONSE_TERMINAL_SOURCES = {
    "OnRtnOrder",
    "OnErrRtnOrderAction",
    "OnErrRtnOrderInsert",
}
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
_ORDER_INSERT_FIELDS = frozenset(
    {
        "BrokerID",
        "InvestorID",
        "UserID",
        "InstrumentID",
        "ExchangeID",
        "OrderRef",
        "RequestID",
    }
)
_TRADE_FACT_FIELDS = frozenset(
    {
        "BrokerID",
        "InvestorID",
        "UserID",
        "InstrumentID",
        "OrderRef",
        "ExchangeID",
        "TradeID",
        "OrderSysID",
        "TradingDay",
        "Direction",
        "OffsetFlag",
        "HedgeFlag",
        "Price",
        "Volume",
        "TradeDate",
        "TradeTime",
        "SequenceNo",
        "BrokerOrderSeq",
        "TradeSource",
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
        if self.callback_key.version != 2:
            raise ContractValidationError("native CTP callback envelope requires V2 correlation")
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
        elif self.source_callback in _ORDER_INSERT_SOURCES:
            allowed_fields = _ORDER_INSERT_FIELDS
            allowed_bases = {"RequestID-OrderRef-callback-source"}
            if self.callback_key.callback_family != "ORDER":
                raise ContractValidationError("native CTP insert callback family is invalid")
        else:
            raise ContractValidationError("unsupported native CTP callback source")
        if self.event_identity_basis not in allowed_bases:
            raise ContractValidationError("native CTP callback event identity basis is invalid")
        object.__setattr__(
            self,
            "native_fields",
            _freeze_native_fields(self.native_fields, allowed_fields),
        )
        frozen_fields = dict(self.native_fields)
        if self.source_callback in _ORDER_ACTION_SOURCES and (
            type(frozen_fields.get("OrderActionRef")) is not int
            or frozen_fields.get("OrderActionRef") != self.callback_key.native_action_ref
            or type(frozen_fields.get("RequestID")) is not int
            or frozen_fields.get("RequestID") != self.callback_key.native_request_id
        ):
            raise ContractValidationError("native CTP action fields differ from callback key")
        if self.response_error_id is not None and type(self.response_error_id) is not int:
            raise ContractValidationError("native CTP response error id is invalid")
        if self.response_is_last is not None and type(self.response_is_last) is not bool:
            raise ContractValidationError("native CTP response terminal flag is invalid")
        if self.source_callback in _RESPONSE_TERMINAL_SOURCES:
            if self.response_is_last is not True:
                source_kind = "action" if self.source_callback == "OnRspOrderAction" else "insert"
                raise ContractValidationError(f"native CTP {source_kind} response is not terminal")
        elif self.source_callback in _NO_RESPONSE_TERMINAL_SOURCES:
            if self.response_is_last is not None:
                if self.source_callback == "OnErrRtnOrderAction":
                    raise ContractValidationError(
                        "native CTP action error has no response terminal flag"
                    )
                if self.source_callback == "OnErrRtnOrderInsert":
                    raise ContractValidationError(
                        "native CTP insert error has no response terminal flag"
                    )
                raise ContractValidationError(
                    "native CTP order return has no response terminal flag"
                )
        else:
            raise ContractValidationError("unsupported native CTP callback source")

    def to_payload(self) -> dict[str, object]:
        """Return detached JSON-safe facts for ``apply_ctp_verified_dispatch_callback``."""

        return {
            "envelope_type": "ctp_native_callback_envelope.v2",
            "source_callback": self.source_callback,
            "event_identity_basis": self.event_identity_basis,
            "source_scope": self.source_scope.to_payload(),
            "native_fields": dict(self.native_fields),
            "response_error_id": self.response_error_id,
            "response_is_last": self.response_is_last,
        }


@dataclass(frozen=True, slots=True)
class CtpNativeTradeFactV2:
    """Versioned raw trade fact without a command correlation key.

    A CTP ``CThostFtdcTradeField`` has no RequestID, FrontID, or SessionID.
    This value intentionally does not manufacture those fields from a command.
    It is only a structural projection: an SDK ingress record and an injected
    verifier must establish source provenance before a Store may use it.
    """

    source_scope: CtpNativeSessionContext
    event_id: str
    native_fields: tuple[tuple[str, _NativeValue], ...]

    def __post_init__(self) -> None:
        if type(self.source_scope) is not CtpNativeSessionContext:
            raise ContractValidationError("typed native CTP session context is required")
        _validate_text(self.event_id, "trade event id")
        object.__setattr__(
            self,
            "native_fields",
            _freeze_native_fields(self.native_fields, _TRADE_FACT_FIELDS),
        )

    @property
    def source_callback(self) -> str:
        return "OnRtnTrade"

    @property
    def event_identity_basis(self) -> str:
        return "TradingDay-ExchangeID-TradeID"

    def to_payload(self) -> dict[str, object]:
        return {
            "envelope_type": "ctp_native_trade_fact.v2",
            "source_callback": self.source_callback,
            "event_identity_basis": self.event_identity_basis,
            "event_id": self.event_id,
            "source_scope": self.source_scope.to_payload(),
            "native_fields": dict(self.native_fields),
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
    native_action_ref: int | None,
    order_ref: str,
    exchange_id: str | None = None,
    order_sys_id: str | None = None,
    target_front_id: int | None = None,
    target_session_id: int | None = None,
) -> CtpDispatchCallbackKey:
    return CtpDispatchCallbackKey(
        version=correlation.version,
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
    from bt_api_ctp.execution_callback_mapping import map_ctp_native_order_return as project

    return project(correlation, session, order_field)


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
    from bt_api_ctp.execution_callback_mapping import map_ctp_native_order_action_callback as project

    return project(
        correlation, session, source_callback, action_field, response_info,
        request_id=request_id, is_last=is_last,
    )


def map_ctp_native_order_insert_callback(
    correlation: CtpDispatchCorrelationKey,
    session: CtpNativeSessionContext,
    source_callback: str,
    order_field: object,
    response_info: object | None = None,
    *,
    request_id: int | None = None,
    is_last: object | None = None,
) -> CtpNativeCallbackEnvelope:
    from bt_api_ctp.execution_callback_mapping import map_ctp_native_order_insert_callback as project

    return project(
        correlation, session, source_callback, order_field, response_info,
        request_id=request_id, is_last=is_last,
    )


def map_ctp_native_trade_fact_v2(
    session: CtpNativeSessionContext,
    trade_field: object,
) -> CtpNativeTradeFactV2:
    from bt_api_ctp.execution_callback_mapping import map_ctp_native_trade_fact_v2 as project

    return project(session, trade_field)


__all__ = [
    "CtpNativeCallbackEnvelope",
    "CtpNativeSessionContext",
    "CtpNativeTradeFactV2",
    "ctp_native_session_epoch",
    "ctp_native_session_generation_id",
    "map_ctp_native_order_action_callback",
    "map_ctp_native_order_insert_callback",
    "map_ctp_native_order_return",
    "map_ctp_native_trade_fact_v2",
]
