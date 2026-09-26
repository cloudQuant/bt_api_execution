"""Opt-in adapters for the Store-owned CTP callback session ingress.

Nothing in this module registers a default Store, imports the native SDK, or
constructs a provider. The embedding composition must supply the exact SDK
acknowledgement factory already loaded by its client integration.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from threading import Lock
from typing import Any

from .ctp_native_callbacks import (
    CtpNativeSessionContext,
    map_ctp_native_order_action_callback,
    map_ctp_native_order_insert_callback,
    map_ctp_native_order_return,
    map_ctp_native_trade_fact_v2,
)
from .errors import ContractValidationError
from .store import (
    CtpCallbackIngressEventV1,
    CtpCallbackSessionBindingV1,
    CtpCallbackSessionOwnerHandle,
    CtpDispatchCallbackApplyResult,
    CtpDispatchTradeFactApplyResult,
    CtpManagedNativeCallBindingV2,
    CtpTraderCallbackIngressPoisonAckV2,
    ExecutionScope,
    SqliteExecutionStore,
    WriterLease,
)


@dataclass(frozen=True, slots=True)
class CtpCallbackIngressAuditResultV1:
    """One persisted non-routeable event advanced on the source sequence."""

    owner_intent_id: str
    source_sequence: int
    callback_name: str
    callback_class: str
    economic_query_observed: bool


def _flattened_fields(payload: Mapping[str, Any], argument_slot: int) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for item in payload.get("flattened_fields", ()):
        if type(item) is not dict or item.get("argument_slot") != argument_slot:
            continue
        if item.get("present") is True and item.get("scalar_captured") is True:
            name = item.get("field_name")
            if type(name) is not str or not name or name in fields:
                raise ContractValidationError("CTP callback field snapshot is ambiguous")
            fields[name] = item.get("value")
    return fields


def _named_arg(payload: Mapping[str, Any], slot: int) -> Any:
    named_args = payload.get("named_args")
    if type(named_args) is not list or slot < 0 or slot >= len(named_args):
        return None
    item = named_args[slot]
    if type(item) is not dict or item.get("present") is not True:
        return None
    if item.get("scalar_captured") is not True:
        return None
    return item.get("value")


def _source_session(
    adapter: CtpCallbackSessionStoreAdapter,
) -> tuple[CtpNativeSessionContext, CtpCallbackSessionBindingV1]:
    binding, broker_id, user_id = adapter.store.read_ctp_callback_session_context_facts(
        adapter.owner_handle
    )
    import hashlib

    fingerprint = "acct_" + hashlib.sha256(
        f"{broker_id}:{user_id}".encode("ascii")
    ).hexdigest()[:16]
    session = CtpNativeSessionContext(
        account_key=binding.account_key,
        scope_key=binding.scope_key,
        trading_day=binding.trading_day,
        session_epoch=binding.native_client_epoch,
        session_generation_id=binding.session_generation_id,
        account_fingerprint=fingerprint,
        native_api_generation=binding.native_api_generation,
        connection_generation=binding.connection_generation,
        dispatch_front_id=binding.dispatch_front_id,
        dispatch_session_id=binding.dispatch_session_id,
    )
    return session, binding


class CtpCallbackSessionIngressSink:
    """Fixed Store/owner pair passed to the SDK before native start."""

    def __init__(
        self,
        store: SqliteExecutionStore,
        owner_handle: CtpCallbackSessionOwnerHandle,
        acknowledgement_factory: Callable[..., Any],
        callback_record_type: type,
        adapter: CtpCallbackSessionStoreAdapter,
    ) -> None:
        if type(store) is not SqliteExecutionStore:
            raise ContractValidationError("exact execution Store is required")
        if type(owner_handle) is not CtpCallbackSessionOwnerHandle:
            raise ContractValidationError("typed CTP callback owner is required")
        if not callable(acknowledgement_factory):
            raise ContractValidationError("typed SDK callback acknowledgement factory is required")
        if type(callback_record_type) is not type:
            raise ContractValidationError("exact SDK callback record class is required")
        if type(adapter) is not CtpCallbackSessionStoreAdapter:
            raise ContractValidationError("exact callback session adapter is required")
        if (
            store._issued_ctp_callback_session_owners.get(owner_handle.owner_intent_id)
            is not owner_handle
        ):
            raise ContractValidationError("callback owner does not belong to this Store")
        if (
            store._issued_ctp_callback_session_adapters.get(owner_handle.owner_intent_id)
            is not adapter
            or store._ctp_callback_ingress_record_types.get(owner_handle.owner_intent_id)
            is not callback_record_type
        ):
            raise ContractValidationError("callback adapter wire type is not Store-pinned")
        self._store = store
        self._owner_handle = owner_handle
        self._acknowledgement_factory = acknowledgement_factory
        self._callback_record_type = callback_record_type
        self._adapter = adapter

    def append(self, record: Any) -> Any:
        """Commit the exact callback row before constructing its SDK ack."""

        result = self._store.append_ctp_callback_ingress(
            self._owner_handle, record, _adapter=self._adapter
        )
        return self._acknowledgement_factory(
            owner_id=result.owner_intent_id,
            sequence=result.source_sequence,
            digest=result.record_digest_sha256,
            commit_state="COMMITTED",
            high_watermark=result.committed_high_watermark,
        )

    def poison_ingress(
        self,
        owner_handle: CtpCallbackSessionOwnerHandle,
        reason_code: str,
        *,
        source_tags: Any = None,
        last_sequence: int | None = None,
    ) -> CtpTraderCallbackIngressPoisonAckV2:
        """Persist an SDK lifecycle/callback failure as a permanent fence."""

        if owner_handle is not self._owner_handle:
            raise ContractValidationError("callback poison owner differs from fixed sink")
        tag_mapping = None
        if source_tags is not None:
            if type(source_tags) in (tuple, list) and len(source_tags) == 6:
                source_tags = {
                    "source_instance_id": source_tags[0],
                    "native_client_epoch": source_tags[1],
                    "native_api_source_id": source_tags[2],
                    "native_spi_source_id": source_tags[3],
                    "native_api_generation": source_tags[4],
                    "connection_generation": source_tags[5],
                }
            elif not isinstance(source_tags, Mapping):
                names = (
                    "source_instance_id",
                    "native_client_epoch",
                    "native_api_source_id",
                    "native_spi_source_id",
                    "native_api_generation",
                    "connection_generation",
                )
                try:
                    source_tags = {name: getattr(source_tags, name) for name in names}
                except Exception:
                    source_tags = None
            if isinstance(source_tags, Mapping):
                try:
                    # If the SDK supplied its compact legacy tuple, retain the
                    # source ID checks by using the persisted tag value for the
                    # omitted registration connection generation.
                    if "connection_generation" not in source_tags:
                        with self._store._lock:
                            row = self._store._connection.execute(
                                "SELECT connection_generation FROM "
                                "ctp_dispatch_callback_session_owners WHERE owner_intent_id = ?",
                                (self._owner_handle.owner_intent_id,),
                            ).fetchone()
                        if row is None:
                            source_tags = None
                        else:
                            source_tags = {
                                **source_tags,
                                "connection_generation": int(row[0]),
                            }
                    tag_mapping = source_tags if source_tags is not None else None
                except Exception:
                    tag_mapping = None
        result = self._store.poison_ctp_callback_session_owner(
            self._owner_handle,
            reason_code,
            source_tags=tag_mapping,
            last_sequence=last_sequence,
        )
        return CtpTraderCallbackIngressPoisonAckV2(
            owner_intent_id=result.owner_intent_id,
            durable_state=result.durable_state,
            last_source_sequence=result.last_source_sequence,
            poison_code=result.poison_code,
            committed=result.committed,
        )


class CtpCallbackSessionStoreAdapter:
    """Install-time fixed sink, login binder, and native-call verifier."""

    def __init__(
        self,
        store: SqliteExecutionStore,
        scope: ExecutionScope,
        owner_handle: CtpCallbackSessionOwnerHandle,
        writer_lease: WriterLease,
        acknowledgement_factory: Callable[..., Any],
        callback_record_type: type,
    ) -> None:
        if type(scope) is not ExecutionScope or type(writer_lease) is not WriterLease:
            raise ContractValidationError("typed CTP execution scope and writer lease are required")
        self.store = store
        self.scope = scope
        self.owner_handle = owner_handle
        self.writer_lease = writer_lease
        self._apply_lock = Lock()
        store._register_ctp_callback_session_adapter(
            owner_handle, self, callback_record_type
        )
        self.sink = CtpCallbackSessionIngressSink(
            store,
            owner_handle,
            acknowledgement_factory,
            callback_record_type,
            self,
        )

    def bind_session(
        self,
        owner_handle: CtpCallbackSessionOwnerHandle,
        *,
        observation: Any,
        source_tags: Any,
        high_watermark: int,
    ) -> Any:
        if owner_handle is not self.owner_handle:
            raise ContractValidationError("callback session owner differs from fixed adapter")
        return self.store.bind_ctp_callback_session(
            self.scope,
            self.owner_handle,
            observation,
            source_tags,
            high_watermark,
            writer_lease=self.writer_lease,
        )

    def verify_native_call(
        self,
        owner_handle: CtpCallbackSessionOwnerHandle,
        binding: CtpManagedNativeCallBindingV2,
    ) -> CtpManagedNativeCallBindingV2:
        if owner_handle is not self.owner_handle:
            raise ContractValidationError("callback session owner differs from fixed adapter")
        return self.store.verify_ctp_managed_native_call_binding(
            self.scope,
            self.owner_handle,
            binding,
            writer_lease=self.writer_lease,
        )

    def read_next_ingress(self) -> CtpCallbackIngressEventV1 | None:
        """Read the next exact persisted event for the single session worker."""

        return self.store.read_next_ctp_callback_ingress(self.owner_handle)

    def mark_audit_only(self, event: CtpCallbackIngressEventV1) -> None:
        """Advance only code-classified, non-routeable audit source events."""

        self.store.mark_ctp_callback_ingress_audit(
            self.scope,
            event,
            writer_lease=self.writer_lease,
        )

    def complete_native_receipt(self, receipt: Any) -> Any:
        """Complete the local native-call receipt under this exact owner."""

        return self.store.complete_ctp_dispatch_command_for_session(
            self.scope,
            receipt,
            owner_handle=self.owner_handle,
            writer_lease=self.writer_lease,
        )

    def apply_next_ingress(
        self,
        *,
        callback_verifier: Any,
        trade_fact_verifier: Any,
    ) -> (
        CtpDispatchCallbackApplyResult
        | CtpDispatchTradeFactApplyResult
        | CtpCallbackIngressAuditResultV1
        | None
    ):
        """Serialize one source consumer per durable account owner."""

        with self._apply_lock:
            return self._apply_next_ingress_locked(
                callback_verifier=callback_verifier,
                trade_fact_verifier=trade_fact_verifier,
            )

    def _apply_next_ingress_locked(
        self,
        *,
        callback_verifier: Any,
        trade_fact_verifier: Any,
    ) -> (
        CtpDispatchCallbackApplyResult
        | CtpDispatchTradeFactApplyResult
        | CtpCallbackIngressAuditResultV1
        | None
    ):
        """Apply the next durable SDK event without trusting caller routing IDs.

        Audit-only events advance the inbox cursor with no execution fact.
        Routeable order/action events are correlated from their native scalar
        fields against the uniquely staged command, then verified and
        committed with their inbox marker. ``OnRtnTrade`` uses its distinct V2
        key (account/day/exchange/TradeID), which intentionally has no native
        RequestID/FrontID/SessionID. A ``None`` command match is the narrow
        native-call-in-flight case: no cursor is advanced and the worker can
        retry after the request receipt transaction completes.

        Any unmatched/unknown/malformed/uncertain event permanently poisons
        this owner and leaves the durable inbox event unapplied.
        """

        event: CtpCallbackIngressEventV1 | None = None
        try:
            event = self.read_next_ingress()
            if event is None:
                return None
            if event.callback_class in {"AUDIT_QUERY", "AUDIT_INFORMATIONAL"}:
                self.mark_audit_only(event)
                return CtpCallbackIngressAuditResultV1(
                    owner_intent_id=self.owner_handle.owner_intent_id,
                    source_sequence=event.source_sequence,
                    callback_name=event.callback_name,
                    callback_class=event.callback_class,
                    economic_query_observed=event.callback_class == "AUDIT_QUERY",
                )
            if event.callback_class != "ROUTEABLE":
                raise ContractValidationError("CTP callback event is not routeable or audit-only")

            command = self.store.find_ctp_dispatch_command_for_ingress(event)
            if command is None:
                return None
            payload = json.loads(event.record_payload_json)
            native_field = _flattened_fields(payload, 0)
            response_info = _flattened_fields(payload, 1)
            session, _ = _source_session(self)
            callback_name = event.callback_name
            if callback_name == "OnRtnTrade":
                trade_fact = map_ctp_native_trade_fact_v2(session, native_field)
                return self.store.apply_ctp_verified_session_trade_fact(
                    self.scope,
                    command.command_id,
                    event,
                    trade_fact,
                    writer_lease=self.writer_lease,
                    trade_verifier=trade_fact_verifier,
                )

            if callback_name == "OnRtnOrder":
                envelope = map_ctp_native_order_return(
                    command.correlation_key,
                    session,
                    native_field,
                )
            elif callback_name in {"OnRspOrderInsert", "OnErrRtnOrderInsert"}:
                envelope = map_ctp_native_order_insert_callback(
                    command.correlation_key,
                    session,
                    callback_name,
                    native_field,
                    response_info if response_info else None,
                    request_id=_named_arg(payload, 2)
                    if callback_name == "OnRspOrderInsert"
                    else None,
                    is_last=_named_arg(payload, 3)
                    if callback_name == "OnRspOrderInsert"
                    else None,
                )
            elif callback_name in {"OnRspOrderAction", "OnErrRtnOrderAction"}:
                envelope = map_ctp_native_order_action_callback(
                    command.correlation_key,
                    session,
                    callback_name,
                    native_field,
                    response_info if response_info else None,
                    request_id=_named_arg(payload, 2)
                    if callback_name == "OnRspOrderAction"
                    else None,
                    is_last=_named_arg(payload, 3)
                    if callback_name == "OnRspOrderAction"
                    else None,
                )
            else:
                raise ContractValidationError("CTP callback has no typed native mapper")

            callback_payload = envelope.to_payload()
            callback_payload["ingress_source"] = {
                "owner_intent_id": self.owner_handle.owner_intent_id,
                "source_sequence": event.source_sequence,
                "record_digest_sha256": event.record_digest_sha256,
            }
            return self.store.apply_ctp_verified_dispatch_callback(
                self.scope,
                command.command_id,
                envelope.callback_key,
                callback_payload,
                writer_lease=self.writer_lease,
                callback_verifier=callback_verifier,
                _callback_session_owner=self.owner_handle,
                _callback_ingress_event=event,
            )
        except Exception:
            with suppress(Exception):
                self.sink.poison_ingress(
                    self.owner_handle,
                    "callback_apply_failure",
                    source_tags=None if event is None else event.source_tags,
                    last_sequence=None if event is None else event.source_sequence,
                )
            raise ContractValidationError("CTP session callback application failed") from None


__all__ = [
    "CtpCallbackIngressAuditResultV1",
    "CtpCallbackSessionIngressSink",
    "CtpCallbackSessionStoreAdapter",
]
