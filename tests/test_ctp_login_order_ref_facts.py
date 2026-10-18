"""Real Store readback of the CTP login OrderRef source fact.

Only the native CTP API leaf is synthetic. The TraderClient SPI, durable
callback ingress, login binding, and Store reader are the production paths.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest

import bt_api_ctp.ctp.client as client_module
from bt_api_ctp.ctp.callback_ingress import (
    CtpTraderCallbackIngressAckV2,
    CtpTraderCallbackIngressRecordV2,
)
from bt_api_execution import (
    ContractValidationError,
    CtpCallbackLoginOrderRefFactsV1,
    CtpCallbackSessionStoreAdapter,
    DurableStoreError,
    ExecutionScope,
    InvalidStateTransition,
    SqliteExecutionStore,
)
import test_ctp_callback_ingress_client as ctp_fixtures


class _SyntheticLoginApi(ctp_fixtures._FakeTraderApi):
    def __init__(self, max_order_ref: str) -> None:
        super().__init__()
        self._login_max_order_ref = max_order_ref

    def ReqUserLogin(self, field, request_id):  # noqa: N802 - CTP SPI name
        self.calls.append(("ReqUserLogin", request_id))
        response = SimpleNamespace(
            BrokerID=field.BrokerID,
            UserID=field.UserID,
            TradingDay="20260926",
            FrontID=7,
            SessionID=19,
            MaxOrderRef=self._login_max_order_ref,
        )
        self.spi.OnRspUserLogin(
            response,
            SimpleNamespace(ErrorID=0, ErrorMsg=""),
            request_id,
            True,
        )
        return 0


class _SyntheticLoginApiFactory:
    instances: list[_SyntheticLoginApi] = []
    max_order_ref = "17"

    @classmethod
    def CreateFtdcTraderApi(cls, _flow_path):  # noqa: N802 - CTP API name
        api = _SyntheticLoginApi(cls.max_order_ref)
        cls.instances.append(api)
        return api


def _scope() -> ExecutionScope:
    account_ref = "ctp-account-ref.v1:" + hashlib.sha256(
        b"synthetic-login-order-ref-facts"
    ).hexdigest()
    return ExecutionScope(
        "ctp", "simulation", account_ref, "strategy.login-order-ref", "20260926"
    )


@contextlib.contextmanager
def _active_login_case(monkeypatch, database, *, max_order_ref: str = "17"):
    """Create a fresh Store owner, then bind it only through real SPI login."""

    scope = _scope()
    store = SqliteExecutionStore(database)
    trader = None
    try:
        family_owner = store.acquire_ctp_account_family_owner(scope)
        lease = store.acquire_or_renew_lease(
            scope,
            "login-order-ref-test-owner",
            ttl_ns=60_000_000_000,
            ctp_account_family_owner=family_owner,
        )
        owner = store.create_ctp_callback_session_owner(scope, writer_lease=lease)
        adapter = CtpCallbackSessionStoreAdapter(
            store,
            scope,
            owner,
            lease,
            CtpTraderCallbackIngressAckV2,
            CtpTraderCallbackIngressRecordV2,
        )

        _SyntheticLoginApiFactory.instances = []
        _SyntheticLoginApiFactory.max_order_ref = max_order_ref
        monkeypatch.setattr(client_module, "_check_native_module", lambda: None)
        monkeypatch.setattr(client_module, "_flow_dir", lambda _name: "synthetic-flow")
        monkeypatch.setattr(client_module, "_register_ctp_native_api", lambda _api: None)
        monkeypatch.setattr(
            client_module, "CThostFtdcTraderApi", _SyntheticLoginApiFactory
        )

        trader = client_module.TraderClient(
            "tcp://synthetic.invalid", "9999", "investor-1", "synthetic-only-secret"
        )
        trader.install_callback_ingress_sink(
            owner, adapter.sink, adapter.bind_session, adapter.verify_native_call
        )
        trader.start()
        assert _SyntheticLoginApiFactory.instances
        api = _SyntheticLoginApiFactory.instances[-1]
        assert api.join_started.wait(1)
        assert trader._callback_ingress.phase == "ACTIVE"
        assert trader._callback_ingress.active_session.source_high_watermark == 3
        yield SimpleNamespace(
            scope=scope,
            store=store,
            lease=lease,
            owner=owner,
            adapter=adapter,
            trader=trader,
            api=api,
        )
    finally:
        try:
            if trader is not None and trader._api is not None:
                receipt = trader.stop_and_wait(timeout=2.0)
                assert receipt.complete, receipt
        finally:
            store.close()


def _cutover_row_counts(store) -> dict[str, int]:
    tables = (
        "ctp_order_ref_watermarks",
        "ctp_order_ref_account_watermarks",
        "ctp_order_ref_cutover_sessions",
        "ctp_order_ref_legacy_imports",
    )
    return {
        table: int(store._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in tables
    }


@pytest.mark.unit
def test_store_reads_login_max_order_ref_as_read_only_bound_source_fact(
    monkeypatch, tmp_path
):
    with _active_login_case(monkeypatch, tmp_path / "login-facts.sqlite3") as case:
        changes_before = case.store._connection.total_changes
        cutover_before = _cutover_row_counts(case.store)

        facts = case.store.read_ctp_callback_session_login_order_ref_facts(case.owner)
        repeated = case.store.read_ctp_callback_session_login_order_ref_facts(
            case.owner
        )

        assert type(facts) is CtpCallbackLoginOrderRefFactsV1
        assert facts.owner_handle is case.owner
        assert facts.binding is case.store._active_ctp_callback_sessions[
            case.owner.owner_intent_id
        ]
        assert facts.broker_id == "9999"
        assert facts.user_id == "investor-1"
        assert facts.native_max_order_ref == "17"
        assert facts.native_max_order_ref_padded == "000000000017"
        assert facts.login_source_sequence == facts.binding.source_high_watermark == 3
        assert len(facts.login_record_digest_sha256) == 64
        assert repeated == facts
        assert case.store._connection.total_changes == changes_before
        assert cutover_before == {
            "ctp_order_ref_watermarks": 0,
            "ctp_order_ref_account_watermarks": 0,
            "ctp_order_ref_cutover_sessions": 0,
            "ctp_order_ref_legacy_imports": 0,
        }
        assert _cutover_row_counts(case.store) == cutover_before


@pytest.mark.unit
def test_login_order_ref_read_rejects_cloned_foreign_and_disconnected_owners(
    monkeypatch, tmp_path
):
    with _active_login_case(monkeypatch, tmp_path / "source.sqlite3") as case:
        with pytest.raises(ContractValidationError):
            case.store.read_ctp_callback_session_login_order_ref_facts(
                replace(case.owner)
            )

        foreign_store = SqliteExecutionStore(tmp_path / "foreign.sqlite3")
        try:
            foreign_family = foreign_store.acquire_ctp_account_family_owner(case.scope)
            foreign_lease = foreign_store.acquire_or_renew_lease(
                case.scope,
                "foreign-login-order-ref-owner",
                ttl_ns=60_000_000_000,
                ctp_account_family_owner=foreign_family,
            )
            foreign_owner = foreign_store.create_ctp_callback_session_owner(
                case.scope, writer_lease=foreign_lease
            )
            with pytest.raises(ContractValidationError):
                case.store.read_ctp_callback_session_login_order_ref_facts(
                    foreign_owner
                )
        finally:
            foreign_store.close()

        case.api.spi.OnFrontDisconnected(1001)
        owner_row = case.store._connection.execute(
            "SELECT owner_state, poison_code FROM ctp_dispatch_callback_session_owners "
            "WHERE owner_intent_id = ?",
            (case.owner.owner_intent_id,),
        ).fetchone()
        assert owner_row["owner_state"] == "POISONED"
        assert owner_row["poison_code"] == "lifecycle_transition"
        with pytest.raises(InvalidStateTransition):
            case.store.read_ctp_callback_session_login_order_ref_facts(case.owner)


@pytest.mark.unit
@pytest.mark.parametrize("max_order_ref", ["", "1234567890123"])
def test_login_order_ref_reader_rejects_invalid_raw_boundary(
    monkeypatch, tmp_path, max_order_ref
):
    with _active_login_case(
        monkeypatch,
        tmp_path / f"invalid-max-order-ref-{len(max_order_ref)}.sqlite3",
        max_order_ref=max_order_ref,
    ) as case:
        with pytest.raises(DurableStoreError, match="invalid native MaxOrderRef"):
            case.store.read_ctp_callback_session_login_order_ref_facts(case.owner)


@pytest.mark.unit
def test_rehashed_login_row_cannot_rebind_the_existing_activation_anchor(
    monkeypatch, tmp_path
):
    """Fault injection is confined to this fresh disposable synthetic SQLite DB."""

    with _active_login_case(monkeypatch, tmp_path / "login-anchor.sqlite3") as case:
        connection = case.store._connection
        owner_id = case.owner.owner_intent_id
        login_row = connection.execute(
            "SELECT * FROM ctp_dispatch_callback_ingress "
            "WHERE owner_intent_id = ? AND callback_name = 'OnRspUserLogin'",
            (owner_id,),
        ).fetchone()
        assert login_row is not None
        application_before = connection.execute(
            "SELECT outcome, command_id, application_digest_sha256 "
            "FROM ctp_dispatch_callback_ingress_applications "
            "WHERE owner_intent_id = ? AND source_sequence = ?",
            (owner_id, login_row["source_sequence"]),
        ).fetchone()
        assert application_before["outcome"] == "AUDIT_ONLY"
        assert application_before["command_id"] is None

        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE ctp_dispatch_callback_ingress "
                "SET record_payload_json = record_payload_json "
                "WHERE owner_intent_id = ? AND source_sequence = ?",
                (owner_id, login_row["source_sequence"]),
            )
        connection.rollback()

        trigger_name = "ctp_callback_ingress_immutable_update"
        trigger_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
            (trigger_name,),
        ).fetchone()
        assert trigger_row is not None and type(trigger_row["sql"]) is str
        exact_trigger_sql = trigger_row["sql"]

        payload = json.loads(login_row["record_payload_json"])
        max_ref_fields = [
            item
            for item in payload["flattened_fields"]
            if item.get("argument_slot") == 0
            and item.get("field_name") == "MaxOrderRef"
        ]
        assert len(max_ref_fields) == 1
        max_ref_fields[0]["value"] = "18"
        unsigned = dict(payload)
        unsigned.pop("digest")
        row_digest = hashlib.sha256(
            json.dumps(
                unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        payload["digest"] = row_digest
        payload_json = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )

        # Deliberately bypass only this disposable DB's immutable ingress-row
        # trigger, then restore its exact SQL before invoking the public reader.
        connection.execute(f'DROP TRIGGER "{trigger_name}"')
        try:
            connection.execute(
                "UPDATE ctp_dispatch_callback_ingress "
                "SET record_payload_json = ?, record_digest_sha256 = ? "
                "WHERE owner_intent_id = ? AND source_sequence = ?",
                (
                    payload_json,
                    row_digest,
                    owner_id,
                    login_row["source_sequence"],
                ),
            )
            connection.commit()
        finally:
            connection.execute(exact_trigger_sql)
            connection.commit()

        changed_row = connection.execute(
            "SELECT record_digest_sha256 FROM ctp_dispatch_callback_ingress "
            "WHERE owner_intent_id = ? AND source_sequence = ?",
            (owner_id, login_row["source_sequence"]),
        ).fetchone()
        application_after = connection.execute(
            "SELECT application_digest_sha256 "
            "FROM ctp_dispatch_callback_ingress_applications "
            "WHERE owner_intent_id = ? AND source_sequence = ?",
            (owner_id, login_row["source_sequence"]),
        ).fetchone()
        assert changed_row["record_digest_sha256"] == row_digest
        assert row_digest != login_row["record_digest_sha256"]
        assert (
            application_after["application_digest_sha256"]
            == application_before["application_digest_sha256"]
        )
        with pytest.raises(DurableStoreError, match="activation receipt"):
            case.store.read_ctp_callback_session_login_order_ref_facts(case.owner)
