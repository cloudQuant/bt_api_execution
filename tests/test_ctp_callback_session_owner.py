from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace

import pytest

from bt_api_execution import (
    ContractValidationError,
    CtpCallbackSessionBindingV1,
    CtpCallbackSessionOwnerHandle,
    CtpCallbackSessionStoreAdapter,
    ExecutionScope,
    InvalidStateTransition,
    SqliteExecutionStore,
)


def _scope() -> ExecutionScope:
    account_ref = "ctp-account-ref.v1:" + hashlib.sha256(b"acct-session-owner").hexdigest()
    return ExecutionScope("ctp", "simulation", account_ref, "strategy.owner", "20260925")


def _lease(store: SqliteExecutionStore, scope: ExecutionScope):
    handle = getattr(store, "_test_ctp_account_family_owner", None)
    if handle is None:
        handle = store.acquire_ctp_account_family_owner(scope)
        store._test_ctp_account_family_owner = handle
    return store.acquire_or_renew_lease(
        scope,
        "session-owner-test",
        ttl_ns=60_000_000_000,
        ctp_account_family_owner=handle,
    )


_SOURCE_TAGS = {
    "source_instance_id": "source-instance-01",
    "native_client_epoch": "a" * 32,
    "native_api_source_id": "api-source-01",
    "native_spi_source_id": "spi-source-01",
    "native_api_generation": 1,
    "source_connection_generation": 0,
}


class CtpTraderCallbackIngressRecordV2:
    """Exact SDK record type name, with the frozen JSON wire contract."""

    def __init__(self, payload):
        self._payload = payload

    def to_payload(self):
        return dict(self._payload)


def _ensure_adapter(store, owner, scope=None, lease=None):
    existing = store._issued_ctp_callback_session_adapters.get(owner.owner_intent_id)
    if existing is not None:
        return existing
    selected_scope = _scope() if scope is None else scope
    selected_lease = _lease(store, selected_scope) if lease is None else lease
    return CtpCallbackSessionStoreAdapter(
        store,
        selected_scope,
        owner,
        selected_lease,
        lambda **kwargs: kwargs,
        CtpTraderCallbackIngressRecordV2,
    )


def _callback_record(
    owner_intent_id,
    name,
    sequence,
    *,
    generation,
    fields=(),
    request_id=None,
    tags=None,
    phase="PRE_LOGIN",
    callback_class=None,
):
    source_tags = dict(_SOURCE_TAGS if tags is None else tags)
    if name == "OnFrontConnected":
        named_args = []
        callback_class = callback_class or "PRE_LOGIN"
    elif name.startswith("OnRsp"):
        names = ("pRspAuthenticateField", "pRspInfo", "nRequestID", "bIsLast")
        named_args = [
            {
                "argument_slot": index,
                "name": arg_name,
                "present": True,
                "scalar_captured": index >= 2,
                "value": (request_id if index == 2 else True) if index >= 2 else None,
            }
            for index, arg_name in enumerate(names)
        ]
        callback_class = callback_class or "PRE_LOGIN"
    else:
        named_args = [
            {
                "argument_slot": 0,
                "name": "pInstrumentStatus",
                "present": True,
                "scalar_captured": False,
                "value": None,
            }
        ]
        callback_class = callback_class or "AUDIT_INFORMATIONAL"
    flattened_fields = [
        {
            "argument_slot": slot,
            "field_name": field_name,
            "present": True,
            "scalar_captured": True,
            "value": value,
        }
        for slot, field_name, value in fields
    ]
    unsigned = {
        "schema": "ctp_trader_callback_ingress.v2",
        "owner_intent_id": owner_intent_id,
        "callback_name": name,
        "callback_class": callback_class,
        "phase": phase,
        **source_tags,
        "connection_generation": generation,
        "sequence": sequence,
        "monotonic_ns": sequence + 100,
        "named_args": named_args,
        "flattened_fields": flattened_fields,
        "capture_complete": True,
        "missing_getters": [],
    }
    digest = hashlib.sha256(
        json.dumps(
            unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    return CtpTraderCallbackIngressRecordV2({**unsigned, "digest": digest})


@dataclass(frozen=True)
class _SourceTags:
    source_instance_id: str = _SOURCE_TAGS["source_instance_id"]
    native_client_epoch: str = _SOURCE_TAGS["native_client_epoch"]
    native_api_source_id: str = _SOURCE_TAGS["native_api_source_id"]
    native_spi_source_id: str = _SOURCE_TAGS["native_spi_source_id"]
    native_api_generation: int = _SOURCE_TAGS["native_api_generation"]
    connection_generation: int = _SOURCE_TAGS["source_connection_generation"]


@dataclass(frozen=True)
class _LoginObservation:
    broker_id: str = "broker"
    user_id: str = "user"
    trading_day: str = "20260925"
    connection_generation: int = 1
    request_id: int = 17


def _append_login_facts(store, owner):
    _ensure_adapter(store, owner)
    store.append_ctp_callback_ingress(
        owner,
        _callback_record(owner.owner_intent_id, "OnFrontConnected", 1, generation=0),
    )
    store.append_ctp_callback_ingress(
        owner,
        _callback_record(
            owner.owner_intent_id,
            "OnRspAuthenticate",
            2,
            generation=1,
            request_id=16,
            fields=((0, "BrokerID", "broker"), (0, "UserID", "user"), (1, "ErrorID", 0)),
        ),
    )
    login = _callback_record(
        owner.owner_intent_id,
        "OnRspUserLogin",
        3,
        generation=1,
        request_id=17,
        fields=(
            (0, "BrokerID", "broker"),
            (0, "UserID", "user"),
            (0, "TradingDay", "20260925"),
            (0, "FrontID", 31),
            (0, "SessionID", 41),
            (1, "ErrorID", 0),
        ),
    )
    store.append_ctp_callback_ingress(owner, login)
    return login


@pytest.mark.unit
def test_callback_session_owner_is_persisted_before_start_and_cannot_be_recreated(tmp_path):
    database = tmp_path / "session-owner.sqlite3"
    scope = _scope()
    store = SqliteExecutionStore(database)
    try:
        lease = _lease(store, scope)
        owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)
        _ensure_adapter(store, owner, scope, lease)

        row = store._connection.execute(
            "SELECT owner_intent_id, account_key, scope_key, owner_state, last_source_sequence "
            "FROM ctp_dispatch_callback_session_owners"
        ).fetchone()
        assert row is not None
        assert row["owner_intent_id"] == owner.owner_intent_id
        assert row["account_key"] == scope.account_key
        assert row["scope_key"] == scope.key
        assert row["owner_state"] == "PREPARED"
        assert row["last_source_sequence"] == 0
        assert owner is store._issued_ctp_callback_session_owners[owner.owner_intent_id]

        with pytest.raises(InvalidStateTransition, match="already has a durable"):
            store.create_ctp_callback_session_owner(scope, writer_lease=lease)
    finally:
        store.close()

    reopened = SqliteExecutionStore(database)
    try:
        with pytest.raises(InvalidStateTransition, match="already has a persistent owner"):
            reopened.acquire_ctp_account_family_owner(scope)

        forged = CtpCallbackSessionOwnerHandle(
            owner_intent_id=owner.owner_intent_id,
            account_key=owner.account_key,
            scope_key=owner.scope_key,
        )
        with pytest.raises(ContractValidationError, match="same-Store"):
            reopened.poison_ctp_callback_session_owner(forged, "disconnect")
    finally:
        reopened.close()


@pytest.mark.unit
def test_callback_session_owner_poison_is_durable_and_never_resets(tmp_path):
    database = tmp_path / "session-owner-poison.sqlite3"
    scope = _scope()
    store = SqliteExecutionStore(database)
    lease = _lease(store, scope)
    owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)

    commit = store.poison_ctp_callback_session_owner(
        owner,
        "disconnect",
        last_sequence=0,
    )
    assert commit.owner_intent_id == owner.owner_intent_id
    assert commit.durable_state == "POISONED"
    assert commit.last_source_sequence == 0
    assert commit.poison_code == "disconnect"
    assert commit.committed is True

    repeated = store.poison_ctp_callback_session_owner(owner, "owner_stop")
    assert repeated == commit
    with pytest.raises(InvalidStateTransition, match="already has a persistent owner"):
        store.acquire_ctp_account_family_owner(scope)
    store.close()

    reopened = SqliteExecutionStore(database)
    try:
        row = reopened._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners"
        ).fetchone()
        assert row is not None
        assert tuple(row) == ("POISONED", "disconnect")
        with pytest.raises(InvalidStateTransition, match="already has a persistent owner"):
            reopened.acquire_ctp_account_family_owner(scope)
    finally:
        reopened.close()


@pytest.mark.unit
def test_first_ingress_generation_zero_binds_exact_positive_login_generation(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-owner-login.sqlite3")
    try:
        lease = _lease(store, scope)
        owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)
        _ensure_adapter(store, owner, scope, lease)
        _append_login_facts(store, owner)

        owner_row = store._connection.execute(
            "SELECT * FROM ctp_dispatch_callback_session_owners WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert owner_row["owner_state"] == "PREPARED"
        assert owner_row["last_source_sequence"] == 3
        assert owner_row["connection_generation"] == 0
        assert owner_row["active_connection_generation"] is None

        binding = store.bind_ctp_callback_session(
            scope,
            owner,
            _LoginObservation(),
            _SourceTags(),
            3,
            writer_lease=lease,
        )
        assert type(binding) is CtpCallbackSessionBindingV1
        assert binding.source_connection_generation == 0
        assert binding.connection_generation == 1
        assert binding.source_high_watermark == 3
        assert binding.dispatch_front_id == 31
        assert binding.dispatch_session_id == 41
        assert binding.session_generation_id == f"ctp-native-v2:{'a' * 32}:1:1:31:41"
        assert store._active_ctp_callback_sessions[owner.owner_intent_id] is binding
    finally:
        store.close()


@pytest.mark.unit
def test_callback_ingress_rejects_bool_generation_and_poisons_owner(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-owner-bool-generation.sqlite3")
    try:
        lease = _lease(store, scope)
        owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)
        _ensure_adapter(store, owner, scope, lease)
        tags = dict(_SOURCE_TAGS)
        tags["native_api_generation"] = True
        record = _callback_record(
            owner.owner_intent_id,
            "OnFrontConnected",
            1,
            generation=0,
            tags=tags,
        )
        with pytest.raises(ContractValidationError, match="record is invalid"):
            store.append_ctp_callback_ingress(owner, record)
        row = store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert tuple(row) == ("POISONED", "capture_incomplete")
    finally:
        store.close()


@pytest.mark.unit
def test_callback_ingress_cannot_relabel_old_source_generation(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-owner-old-generation.sqlite3")
    try:
        lease = _lease(store, scope)
        owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)
        _ensure_adapter(store, owner, scope, lease)
        store.append_ctp_callback_ingress(
            owner,
            _callback_record(owner.owner_intent_id, "OnFrontConnected", 1, generation=0),
        )
        changed = dict(_SOURCE_TAGS)
        changed["native_api_source_id"] = "replacement-api-source"
        with pytest.raises(ContractValidationError, match="source identity changed"):
            store.append_ctp_callback_ingress(
                owner,
                _callback_record(
                    owner.owner_intent_id,
                    "OnRspAuthenticate",
                    2,
                    generation=1,
                    request_id=16,
                    fields=((0, "BrokerID", "broker"), (0, "UserID", "user"), (1, "ErrorID", 0)),
                    tags=changed,
                ),
            )
        row = store._connection.execute(
            "SELECT owner_state, poison_code, last_source_sequence "
            "FROM ctp_dispatch_callback_session_owners WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert tuple(row) == ("POISONED", "source_gap", 1)
    finally:
        store.close()


@pytest.mark.unit
def test_callback_session_pins_one_exact_adapter_and_sdk_record_type(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-owner-adapter-pin.sqlite3")
    try:
        lease = _lease(store, scope)
        owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)
        adapter = _ensure_adapter(store, owner, scope, lease)
        assert store._issued_ctp_callback_session_adapters[owner.owner_intent_id] is adapter
        assert (
            store._ctp_callback_ingress_record_types[owner.owner_intent_id]
            is CtpTraderCallbackIngressRecordV2
        )

        with pytest.raises(InvalidStateTransition, match="single adapter"):
            CtpCallbackSessionStoreAdapter(
                store,
                scope,
                owner,
                lease,
                lambda **kwargs: kwargs,
                CtpTraderCallbackIngressRecordV2,
            )

        spoof_type = type(
            "CtpTraderCallbackIngressRecordV2",
            (),
            {
                "to_payload": lambda self: _callback_record(
                    owner.owner_intent_id, "OnFrontConnected", 1, generation=0
                ).to_payload()
            },
        )
        with pytest.raises(ContractValidationError, match="typed CTP callback ingress record"):
            store.append_ctp_callback_ingress(
                owner,
                spoof_type(),
            )
        row = store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (owner.owner_intent_id,),
        ).fetchone()
        assert tuple(row) == ("POISONED", "capture_incomplete")
    finally:
        store.close()


@pytest.mark.unit
def test_callback_inbox_reads_exact_source_row_and_marks_audit_contiguously(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-owner-inbox-audit.sqlite3")
    try:
        lease = _lease(store, scope)
        owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)
        _append_login_facts(store, owner)
        store.bind_ctp_callback_session(
            scope,
            owner,
            _LoginObservation(),
            _SourceTags(),
            3,
            writer_lease=lease,
        )
        store.append_ctp_callback_ingress(
            owner,
            _callback_record(
                owner.owner_intent_id,
                "OnRtnInstrumentStatus",
                4,
                generation=1,
                phase="ACTIVE",
                callback_class="AUDIT_INFORMATIONAL",
                fields=((0, "InstrumentID", "rb2710"), (0, "InstrumentStatus", 1)),
            ),
        )

        event = store.read_next_ctp_callback_ingress(owner)
        assert event is not None
        assert event.source_sequence == 4
        assert event.callback_name == "OnRtnInstrumentStatus"
        assert event.source_tags == (
            _SOURCE_TAGS["source_instance_id"],
            _SOURCE_TAGS["native_client_epoch"],
            _SOURCE_TAGS["native_api_source_id"],
            _SOURCE_TAGS["native_spi_source_id"],
            1,
            0,
        )
        assert store.read_next_ctp_callback_ingress(owner) is event
        with pytest.raises(InvalidStateTransition, match="unapplied source events"):
            store.claim_ctp_dispatch_command(
                scope,
                "not-yet-staged-command",
                writer_lease=lease,
                authority_verifier=object(),
                _callback_session_owner=owner,
            )
        store.mark_ctp_callback_ingress_audit(scope, event, writer_lease=lease)
        assert store.read_next_ctp_callback_ingress(owner) is None
        assert (
            store.claim_ctp_dispatch_command(
                scope,
                "not-yet-staged-command",
                writer_lease=lease,
                authority_verifier=object(),
                _callback_session_owner=owner,
            )
            is None
        )

        application = store._connection.execute(
            "SELECT outcome, command_id FROM ctp_dispatch_callback_ingress_applications "
            "WHERE owner_intent_id = ? AND source_sequence = 4",
            (owner.owner_intent_id,),
        ).fetchone()
        assert tuple(application) == ("AUDIT_ONLY", None)
    finally:
        store.close()


@pytest.mark.unit
def test_callback_inbox_cannot_audit_a_routeable_or_forged_event(tmp_path):
    scope = _scope()
    store = SqliteExecutionStore(tmp_path / "session-owner-inbox-forged.sqlite3")
    try:
        lease = _lease(store, scope)
        owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)
        _append_login_facts(store, owner)
        store.bind_ctp_callback_session(
            scope,
            owner,
            _LoginObservation(),
            _SourceTags(),
            3,
            writer_lease=lease,
        )
        store.append_ctp_callback_ingress(
            owner,
            _callback_record(
                owner.owner_intent_id,
                "OnRtnInstrumentStatus",
                4,
                generation=1,
                phase="ACTIVE",
                callback_class="AUDIT_INFORMATIONAL",
                fields=((0, "InstrumentID", "rb2710"), (0, "InstrumentStatus", 1)),
            ),
        )
        event = store.read_next_ctp_callback_ingress(owner)
        assert event is not None
        forged = replace(event)
        with pytest.raises(ContractValidationError, match="Store-issued"):
            store.mark_ctp_callback_ingress_audit(scope, forged, writer_lease=lease)
    finally:
        store.close()
