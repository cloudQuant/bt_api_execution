from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import pytest

from bt_api_execution import (
    ContractValidationError,
    CtpDispatchCorrelationKey,
    CtpNativeCallbackEnvelope,
    CtpNativeSessionContext,
    CtpNativeTradeFactV2,
    ctp_native_session_epoch,
    ctp_native_session_generation_id,
    map_ctp_native_order_action_callback,
    map_ctp_native_order_return,
    map_ctp_native_trade_fact_v2,
)
from bt_api_execution.ctp_native_callbacks import map_ctp_native_order_insert_callback

_SESSION_EPOCH = "1234567890abcdef1234567890abcdef"


def _digest(value: str) -> str:
    return sha256(value.encode("ascii")).hexdigest()


def _correlation(
    operation: str, *, session_epoch: str = _SESSION_EPOCH
) -> CtpDispatchCorrelationKey:
    is_cancel = operation == "CANCEL"
    return CtpDispatchCorrelationKey(
        version=2,
        account_key="account:" + _digest("account"),
        scope_key="scope:" + _digest("scope"),
        trading_day="20260925",
        operation=operation,
        command_id="cancel-command" if is_cancel else "submit-command",
        request_payload_sha256=_digest("request"),
        reservation_managed_intent_id="managed-intent",
        managed_action_id="cancel-action" if is_cancel else "managed-intent",
        runtime_order_id="bt-managed-v1:" + _digest("runtime-order"),
        order_ref="000000000013",
        cancel_target_exchange_id="SHFE" if is_cancel else None,
        cancel_target_order_sys_id="sys-order-17" if is_cancel else None,
        cancel_target_front_id=4 if is_cancel else None,
        cancel_target_session_id=91 if is_cancel else None,
        approval_use_id="approval-use",
        approval_digest=_digest("approval"),
        session_binding_sha256=_digest("session-binding"),
        session_generation_id=ctp_native_session_generation_id(
            3, 7, session_epoch, 4, 91
        ),
        dispatch_front_id=4,
        dispatch_session_id=91,
        native_request_id=17,
        native_action_ref=29 if is_cancel else None,
        native_request_payload_sha256=_digest("native-request"),
    )


def _account_fingerprint() -> str:
    return "acct_" + _digest("9999:investor")[:16]


def _session(
    correlation: CtpDispatchCorrelationKey, *, session_epoch: str = _SESSION_EPOCH
) -> CtpNativeSessionContext:
    return CtpNativeSessionContext(
        account_key=correlation.account_key,
        scope_key=correlation.scope_key,
        trading_day=correlation.trading_day,
        session_epoch=session_epoch,
        session_generation_id=correlation.session_generation_id,
        account_fingerprint=_account_fingerprint(),
        native_api_generation=3,
        connection_generation=7,
        dispatch_front_id=correlation.dispatch_front_id,
        dispatch_session_id=correlation.dispatch_session_id,
    )


def _order_field(**overrides):
    values = {
        "BrokerID": "9999",
        "InvestorID": "investor",
        "UserID": "investor",
        "InstrumentID": "rb2710",
        "RequestID": 17,
        "OrderRef": "000000000013",
        "ExchangeID": "SHFE",
        "OrderSysID": "sys-order-17",
        "FrontID": 4,
        "SessionID": 91,
        "TradingDay": "20260925",
        "OrderStatus": "3",
        "OrderSubmitStatus": "3",
        "VolumeTraded": 0,
        "VolumeTotal": 1,
        "UpdateTime": "09:31:12",
        "NotifySequence": 381,
        "SequenceNo": 729,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _action_field(**overrides):
    values = {
        "BrokerID": "9999",
        "InvestorID": "investor",
        "UserID": "investor",
        "InstrumentID": "rb2710",
        "RequestID": 17,
        "OrderActionRef": 29,
        "OrderRef": "000000000013",
        "ExchangeID": "SHFE",
        "OrderSysID": "sys-order-17",
        "FrontID": 4,
        "SessionID": 91,
        "ActionFlag": "0",
        "ActionDate": "20260925",
        "ActionTime": "09:32:10",
        "OrderActionStatus": "3",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _trade_field(**overrides):
    values = {
        "BrokerID": "9999",
        "InvestorID": "investor",
        "UserID": "investor",
        "InstrumentID": "rb2710",
        "OrderRef": "000000000013",
        "ExchangeID": "SHFE",
        "TradeID": "trade-903",
        "OrderSysID": "sys-order-17",
        "Direction": "0",
        "OffsetFlag": "0",
        "HedgeFlag": "1",
        "Price": 3512.5,
        "Volume": 2,
        "TradeDate": "20260925",
        "TradeTime": "09:31:12",
        "TradingDay": "20260925",
        "SequenceNo": 0,
        "BrokerOrderSeq": 88,
        "TradeSource": "0",
        # These are deliberately present in the fake object even though the
        # native CThostFtdcTradeField has no such fields. The mapper must not
        # copy them or pretend they are native trade correlation.
        "RequestID": 17,
        "FrontID": 4,
        "SessionID": 91,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.unit
def test_native_order_return_maps_exact_scope_and_source_sequence():
    correlation = _correlation("SUBMIT")
    session = _session(correlation)

    first = map_ctp_native_order_return(correlation, session, _order_field())
    duplicate = map_ctp_native_order_return(
        correlation,
        session,
        _order_field(),
    )

    assert first.callback_key.callback_family == "ORDER"
    assert first.callback_key.native_request_id == 17
    assert first.callback_key.order_ref == "000000000013"
    assert first.callback_key.exchange_id == "SHFE"
    assert first.callback_key.order_sys_id == "sys-order-17"
    assert first.callback_key.stream_id == duplicate.callback_key.stream_id
    assert first.callback_key.event_id == "on-rtn-order:381:000000000013"
    assert first.event_identity_basis == "CThostFtdcOrderField.NotifySequence"
    assert first.to_payload()["source_scope"] == {
        "account_key": correlation.account_key,
        "scope_key": correlation.scope_key,
        "trading_day": "20260925",
        "session_epoch": _SESSION_EPOCH,
        "session_generation_id": ctp_native_session_generation_id(
            3, 7, _SESSION_EPOCH, 4, 91
        ),
        "account_fingerprint": _account_fingerprint(),
        "native_api_generation": 3,
        "connection_generation": 7,
        "dispatch_front_id": 4,
        "dispatch_session_id": 91,
    }
    assert first.to_payload()["native_fields"]["RequestID"] == 17


@pytest.mark.unit
@pytest.mark.parametrize(
    ("source_callback", "expected_terminal", "invalid_terminal"),
    [
        ("OnRtnOrder", None, True),
        ("OnRspOrderAction", True, None),
        ("OnErrRtnOrderAction", None, True),
        ("OnRspOrderInsert", True, None),
        ("OnErrRtnOrderInsert", None, True),
    ],
)
def test_native_callback_terminal_flag_matrix(source_callback, expected_terminal, invalid_terminal):
    is_cancel = "OrderAction" in source_callback
    correlation = _correlation("CANCEL" if is_cancel else "SUBMIT")
    session = _session(correlation)
    if source_callback == "OnRtnOrder":
        envelope = map_ctp_native_order_return(correlation, session, _order_field())
    elif source_callback in {"OnRspOrderAction", "OnErrRtnOrderAction"}:
        is_response = source_callback == "OnRspOrderAction"
        envelope = map_ctp_native_order_action_callback(
            correlation,
            session,
            source_callback,
            _action_field(),
            SimpleNamespace(ErrorID=0 if is_response else 32),
            request_id=17 if is_response else None,
            is_last=True if is_response else None,
        )
    else:
        is_response = source_callback == "OnRspOrderInsert"
        envelope = map_ctp_native_order_insert_callback(
            correlation,
            session,
            source_callback,
            _order_field(),
            SimpleNamespace(ErrorID=0 if is_response else 32),
            request_id=17 if is_response else None,
            is_last=True if is_response else None,
        )

    assert envelope.response_is_last is expected_terminal
    with pytest.raises(ContractValidationError, match="terminal flag|not terminal"):
        replace(envelope, response_is_last=invalid_terminal)


@pytest.mark.unit
def test_native_trade_fact_v2_has_no_fabricated_command_correlation():
    correlation = _correlation("SUBMIT")
    session = _session(correlation)

    fact = map_ctp_native_trade_fact_v2(session, _trade_field())
    payload = fact.to_payload()
    fields = payload["native_fields"]

    assert type(fact) is CtpNativeTradeFactV2
    assert payload["envelope_type"] == "ctp_native_trade_fact.v2"
    assert payload["source_callback"] == "OnRtnTrade"
    assert payload["event_identity_basis"] == "TradingDay-ExchangeID-TradeID"
    assert fact.event_id.startswith("ctp-trade-v2:")
    assert fields["OrderRef"] == "000000000013"
    assert fields["TradeID"] == "trade-903"
    assert fields["SequenceNo"] == 0
    assert fields["Price"] == "3512.5"
    assert fields["Volume"] == 2
    assert not {"RequestID", "FrontID", "SessionID", "command_id"}.intersection(fields)
    assert not {"RequestID", "FrontID", "SessionID", "command_id"}.intersection(payload)

    same_identity_changed_payload = map_ctp_native_trade_fact_v2(
        session, _trade_field(Price=3513.0)
    )
    assert same_identity_changed_payload.event_id == fact.event_id
    assert same_identity_changed_payload.to_payload() != payload


@pytest.mark.unit
def test_native_trade_fact_v2_rejects_wrong_account_day_and_incomplete_target():
    correlation = _correlation("SUBMIT")
    session = _session(correlation)

    with pytest.raises(ContractValidationError, match="TradingDay does not match"):
        map_ctp_native_trade_fact_v2(session, _trade_field(TradingDay="20260926"))
    with pytest.raises(ContractValidationError, match="account does not match"):
        map_ctp_native_trade_fact_v2(session, _trade_field(UserID="other-investor"))
    with pytest.raises(ContractValidationError, match="OrderSysID"):
        map_ctp_native_trade_fact_v2(session, _trade_field(OrderSysID=""))
    with pytest.raises(ContractValidationError, match="price must be positive"):
        map_ctp_native_trade_fact_v2(session, _trade_field(Price=0))
    with pytest.raises(ContractValidationError, match="volume must be positive"):
        map_ctp_native_trade_fact_v2(session, _trade_field(Volume=0))


@pytest.mark.unit
def test_envelope_freezes_native_fields_and_rejects_unallowlisted_secrets():
    correlation = _correlation("SUBMIT")
    envelope = map_ctp_native_order_return(
        correlation,
        _session(correlation),
        _order_field(),
    )
    mutable_fields = list(envelope.native_fields)
    copied = CtpNativeCallbackEnvelope(
        callback_key=envelope.callback_key,
        source_callback=envelope.source_callback,
        source_scope=envelope.source_scope,
        event_identity_basis=envelope.event_identity_basis,
        native_fields=mutable_fields,
        response_error_id=envelope.response_error_id,
        response_is_last=envelope.response_is_last,
    )
    frozen_fields = copied.native_fields

    mutable_fields[0] = ("BrokerID", "mutated-after-construction")
    mutable_fields.append(("StatusMsg", "STATUS_SENTINEL_SECRET"))
    assert copied.native_fields == frozen_fields
    assert isinstance(copied.native_fields, tuple)
    assert "STATUS_SENTINEL_SECRET" not in str(copied.to_payload())

    for forbidden_name in ("StatusMsg", "Password"):
        with pytest.raises(ContractValidationError, match="not allowlisted"):
            replace(
                envelope,
                native_fields=(
                    *envelope.native_fields,
                    (forbidden_name, "SENTINEL_SECRET"),
                ),
            )
    with pytest.raises(ContractValidationError, match="not primitive"):
        replace(envelope, native_fields=(("RequestID", [17]),))


@pytest.mark.unit
def test_native_order_return_requires_native_identity_sequence_and_matching_session():
    correlation = _correlation("SUBMIT")
    session = _session(correlation)

    with pytest.raises(ContractValidationError, match="RequestID does not match"):
        map_ctp_native_order_return(
            correlation,
            session,
            _order_field(RequestID=18),
        )
    with pytest.raises(ContractValidationError, match="TradingDay does not match"):
        map_ctp_native_order_return(
            correlation,
            session,
            _order_field(TradingDay="20260926"),
        )
    with pytest.raises(ContractValidationError, match="no stable event sequence"):
        map_ctp_native_order_return(
            correlation,
            session,
            _order_field(NotifySequence=0, SequenceNo=0),
        )
    with pytest.raises(ContractValidationError, match="account does not match"):
        map_ctp_native_order_return(
            correlation,
            session,
            _order_field(UserID="other-investor"),
        )
    with pytest.raises(ContractValidationError, match="session does not match command"):
        map_ctp_native_order_return(
            correlation,
            replace(
                session,
                dispatch_session_id=92,
                session_generation_id=ctp_native_session_generation_id(
                    3, 7, session.session_epoch, 4, 92
                ),
            ),
            _order_field(),
        )
    with pytest.raises(ContractValidationError, match="session generation"):
        replace(session, connection_generation=8)
    different_api_generation = replace(
        session,
        native_api_generation=4,
        session_generation_id=ctp_native_session_generation_id(
            4, 7, session.session_epoch, 4, 91
        ),
    )
    with pytest.raises(ContractValidationError, match="session does not match command"):
        map_ctp_native_order_return(correlation, different_api_generation, _order_field())

    with pytest.raises(ContractValidationError, match="RequestID"):
        map_ctp_native_order_return(
            correlation,
            session,
            {"OrderRef": "000000000013"},
        )


@pytest.mark.unit
def test_native_session_epoch_is_required_and_distinguishes_recreated_clients():
    with pytest.raises(ContractValidationError, match="session epoch"):
        ctp_native_session_generation_id(3, 7, "", 4, 91)

    epoch_one = ctp_native_session_epoch()
    epoch_two = ctp_native_session_epoch()
    assert epoch_one != epoch_two
    correlation_one = _correlation("CANCEL", session_epoch=epoch_one)
    correlation_two = _correlation("CANCEL", session_epoch=epoch_two)
    session_one = _session(correlation_one, session_epoch=epoch_one)
    session_two = _session(correlation_two, session_epoch=epoch_two)
    response_info = SimpleNamespace(ErrorID=0)

    first = map_ctp_native_order_action_callback(
        correlation_one,
        session_one,
        "OnRspOrderAction",
        _action_field(),
        response_info,
        request_id=17,
        is_last=True,
    )
    first_duplicate = map_ctp_native_order_action_callback(
        correlation_one,
        session_one,
        "OnRspOrderAction",
        _action_field(),
        response_info,
        request_id=17,
        is_last=True,
    )
    restarted_client_event = map_ctp_native_order_action_callback(
        correlation_two,
        session_two,
        "OnRspOrderAction",
        _action_field(),
        response_info,
        request_id=17,
        is_last=True,
    )

    assert first.callback_key == first_duplicate.callback_key
    assert first.callback_key.event_id == restarted_client_event.callback_key.event_id
    first_identity = (
        first.callback_key.correlation_key.session_generation_id,
        first.callback_key.stream_id,
        first.callback_key.event_id,
    )
    restarted_identity = (
        restarted_client_event.callback_key.correlation_key.session_generation_id,
        restarted_client_event.callback_key.stream_id,
        restarted_client_event.callback_key.event_id,
    )
    assert first_identity != restarted_identity

    with pytest.raises(ContractValidationError, match="session epoch"):
        replace(session_one, session_epoch="")


@pytest.mark.unit
def test_native_cancel_action_response_maps_request_action_and_target():
    correlation = _correlation("CANCEL")
    session = _session(correlation)
    response_info = SimpleNamespace(ErrorID=0, ErrorMsg="omitted from envelope")

    envelope = map_ctp_native_order_action_callback(
        correlation,
        session,
        "OnRspOrderAction",
        _action_field(),
        response_info,
        request_id=17,
        is_last=True,
    )

    callback = envelope.callback_key
    assert callback.callback_family == "CANCEL_ACTION"
    assert callback.native_request_id == 17
    assert callback.native_action_ref == 29
    assert callback.order_ref == "000000000013"
    assert callback.exchange_id == "SHFE"
    assert callback.order_sys_id == "sys-order-17"
    assert (callback.target_front_id, callback.target_session_id) == (4, 91)
    assert callback.event_id == "onrsporderaction:17:29"
    assert envelope.response_error_id == 0
    assert envelope.response_is_last is True
    payload = envelope.to_payload()
    assert payload["source_scope"]["trading_day"] == "20260925"
    assert payload["source_scope"]["session_generation_id"] == ctp_native_session_generation_id(
        3, 7, _SESSION_EPOCH, 4, 91
    )
    assert payload["native_fields"]["OrderActionRef"] == 29
    assert "ErrorMsg" not in str(payload)
    duplicate = map_ctp_native_order_action_callback(
        correlation,
        session,
        "OnRspOrderAction",
        _action_field(),
        response_info,
        request_id=17,
        is_last=True,
    )
    assert duplicate.callback_key == callback


@pytest.mark.unit
def test_native_cancel_action_response_rejects_mismatched_or_nonterminal_facts():
    correlation = _correlation("CANCEL")
    session = _session(correlation)
    response_info = SimpleNamespace(ErrorID=0)

    with pytest.raises(ContractValidationError, match="RequestID fields do not match"):
        map_ctp_native_order_action_callback(
            correlation,
            session,
            "OnRspOrderAction",
            _action_field(RequestID=18),
            response_info,
            request_id=17,
            is_last=True,
        )
    with pytest.raises(ContractValidationError, match="not terminal"):
        map_ctp_native_order_action_callback(
            correlation,
            session,
            "OnRspOrderAction",
            _action_field(),
            response_info,
            request_id=17,
            is_last=False,
        )
    with pytest.raises(ContractValidationError, match="target does not match"):
        map_ctp_native_order_action_callback(
            correlation,
            session,
            "OnRspOrderAction",
            _action_field(OrderSysID="different-order"),
            response_info,
            request_id=17,
            is_last=True,
        )
    with pytest.raises(ContractValidationError, match="field is required: OrderActionRef"):
        map_ctp_native_order_action_callback(
            correlation,
            session,
            "OnRspOrderAction",
            _action_field(OrderActionRef=""),
            response_info,
            request_id=17,
            is_last=True,
        )
    with pytest.raises(ContractValidationError, match="target does not match"):
        map_ctp_native_order_action_callback(
            correlation,
            session,
            "OnRspOrderAction",
            _action_field(FrontID=5),
            response_info,
            request_id=17,
            is_last=True,
        )
    with pytest.raises(ContractValidationError, match="target does not match"):
        map_ctp_native_order_action_callback(
            correlation,
            session,
            "OnRspOrderAction",
            _action_field(OrderActionRef=30),
            response_info,
            request_id=17,
            is_last=True,
        )


@pytest.mark.unit
def test_native_cancel_error_uses_field_request_id_without_inventing_callback_args():
    correlation = _correlation("CANCEL")
    session = _session(correlation)
    response_info = SimpleNamespace(ErrorID=32, ErrorMsg="rejected")

    envelope = map_ctp_native_order_action_callback(
        correlation,
        session,
        "OnErrRtnOrderAction",
        _action_field(),
        response_info,
    )

    assert envelope.callback_key.native_request_id == 17
    assert envelope.callback_key.native_action_ref == 29
    assert envelope.callback_key.event_id == "onerrrtnorderaction:17:29"
    assert envelope.response_error_id == 32
    assert envelope.response_is_last is None
    with pytest.raises(ContractValidationError, match="action error has no response terminal flag"):
        replace(envelope, response_is_last=True)


@pytest.mark.unit
def test_native_order_insert_response_keeps_terminal_flag_and_insert_error_rejects_one():
    correlation = _correlation("SUBMIT")
    session = _session(correlation)
    response_info = SimpleNamespace(ErrorID=0, ErrorMsg="omitted from envelope")

    response = map_ctp_native_order_insert_callback(
        correlation,
        session,
        "OnRspOrderInsert",
        _order_field(),
        response_info,
        request_id=17,
        is_last=True,
    )

    assert response.callback_key.callback_family == "ORDER"
    assert response.callback_key.native_request_id == 17
    assert response.response_error_id == 0
    assert response.response_is_last is True
    assert response.to_payload()["response_is_last"] is True

    insert_error = map_ctp_native_order_insert_callback(
        correlation,
        session,
        "OnErrRtnOrderInsert",
        _order_field(),
        SimpleNamespace(ErrorID=32, ErrorMsg="omitted from envelope"),
    )
    assert insert_error.response_is_last is None
    with pytest.raises(ContractValidationError, match="insert error has no response terminal flag"):
        replace(insert_error, response_is_last=True)
