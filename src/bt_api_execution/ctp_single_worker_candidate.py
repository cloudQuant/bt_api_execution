"""CTP managed execution worker using provider-owned dispatch contracts.

Execution retains the durable claim, UNKNOWN and completion workflow. The CTP
provider owns request binding and never supplies execution authority itself.
"""
from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import Any

from .contracts import ExecutionScope, canonical_json
from .errors import ContractValidationError
from .store import (
    CtpCallbackSessionOwnerHandle,
    CtpDispatchAuthorityVerifier,
    CtpDispatchProjection,
    CtpDispatchReceipt,
    SqliteExecutionStore,
    WriterLease,
)
from bt_api_ctp.managed_dispatch_worker_binding import CtpManagedDispatchBindingMixin
from bt_api_ctp.managed_dispatch_contracts import (
    CtpManagedDispatchBinding,
    CtpManagedPreparedDispatch,
    CtpNativeDispatchResult,
    CtpNativeDispatchSender,
    stable_managed_command_id,
)



class CtpManagedSingleWorkerCandidate(CtpManagedDispatchBindingMixin):
    """Candidate adapter intended to run inside one persistent host worker.

    It owns no thread and starts no provider. The host's sole worker calls
    ``dispatch_managed_command`` with the already-published command ID and its
    typed binding. The SQLite claim is the at-most-once boundary; a crash after
    claim remains CLAIMED until recovery fences it to UNKNOWN.
    """

    def __init__(
        self,
        store: SqliteExecutionStore,
        scope: ExecutionScope,
        writer_lease: WriterLease,
        authority_verifier: CtpDispatchAuthorityVerifier,
    ) -> None:
        self._store = store
        self._scope = scope
        self._writer_lease = writer_lease
        self._authority_verifier = authority_verifier


    def record_managed_queue_receipt(
        self,
        command_id: str,
        binding: CtpManagedDispatchBinding,
        queue_receipt: Mapping[str, Any],
        *,
        owner_handle: CtpCallbackSessionOwnerHandle | None = None,
        position_baseline_authorization: object | None = None,
        recovery_close_authorization: object | None = None,
    ) -> CtpManagedDispatchBinding:
        if (
            position_baseline_authorization is not None
            and recovery_close_authorization is not None
        ):
            raise ContractValidationError("CTP position dispatch authorizations are exclusive")
        if (
            position_baseline_authorization is not None
            or recovery_close_authorization is not None
        ) and type(owner_handle) is not CtpCallbackSessionOwnerHandle:
            raise ContractValidationError(
                "position dispatch authorization requires the exact callback session owner"
            )
        _, reservation = self._read_and_require_binding(
            command_id,
            binding,
            owner_handle=owner_handle,
            position_baseline_authorization=position_baseline_authorization,
            recovery_close_authorization=recovery_close_authorization,
        )
        command = self._store.record_ctp_dispatch_queue_receipt(
            self._scope,
            command_id,
            queue_receipt,
            writer_lease=self._writer_lease,
            position_baseline_authorization=position_baseline_authorization,
            recovery_close_authorization=recovery_close_authorization,
        )
        return CtpManagedDispatchBinding.from_command(command, reservation)

    async def dispatch_managed_command(
        self,
        command_id: str,
        binding: CtpManagedDispatchBinding,
        sender: CtpNativeDispatchSender,
        *,
        owner_handle: CtpCallbackSessionOwnerHandle | None = None,
        position_baseline_authorization: object | None = None,
        recovery_close_authorization: object | None = None,
    ) -> CtpDispatchProjection:
        if (
            position_baseline_authorization is not None
            and recovery_close_authorization is not None
        ):
            raise ContractValidationError("CTP position dispatch authorizations are exclusive")
        if (
            position_baseline_authorization is not None
            or recovery_close_authorization is not None
        ) and type(owner_handle) is not CtpCallbackSessionOwnerHandle:
            raise ContractValidationError(
                "position dispatch authorization requires the exact callback session owner"
            )
        self._require_binding(command_id, binding)
        if not callable(sender):
            raise ContractValidationError("one managed CTP sender callable is required")
        command, _ = self._read_and_require_binding(
            command_id,
            binding,
            owner_handle=owner_handle,
            position_baseline_authorization=position_baseline_authorization,
            recovery_close_authorization=recovery_close_authorization,
        )
        if command.status in {"COMPLETED", "UNKNOWN", "CLAIMED"}:
            projection = self._store.read_ctp_dispatch_projection(self._scope, command_id)
            if projection is None:
                raise ContractValidationError("managed CTP durable projection is missing")
            return projection
        if command.local_queue_receipt_queued is not True:
            raise ContractValidationError("managed CTP queue receipt is not committed as queued")

        claimed = self._store.claim_ctp_dispatch_command(
            self._scope,
            command_id,
            writer_lease=self._writer_lease,
            authority_verifier=self._authority_verifier,
            required_local_queue_receipt_id=binding.local_queue_receipt_id,
            _callback_session_owner=owner_handle,
            position_baseline_authorization=position_baseline_authorization,
            recovery_close_authorization=recovery_close_authorization,
        )
        if claimed is None:
            projection = self._store.read_ctp_dispatch_projection(self._scope, command_id)
            if projection is None:
                raise ContractValidationError("managed CTP durable projection is missing")
            return projection

        try:
            if claimed.operation == "CANCEL":
                self._store.require_fresh_ctp_cancel_target_for_command(self._scope, command_id)
            result = sender(claimed)
            if inspect.isawaitable(result):
                result = await result
            if type(result) is not CtpNativeDispatchResult:
                raise ContractValidationError("managed CTP sender returned an untyped result")
            if result.outcome == "REJECTED":
                # Sender-provided data cannot prove that native send and every
                # callback were absent. There is no trusted no-send proof port
                # in this candidate, so a post-claim rejection is ambiguous.
                outcome = "UNKNOWN"
                receipt_payload = {"kind": "native_dispatch", "outcome": "UNKNOWN"}
            else:
                outcome = result.outcome
                receipt_payload = dict(result.receipt_payload)
                # A fake/native adapter may include an unsafe or non-canonical
                # payload. Treat it as ambiguous and persist only a fixed marker.
                canonical_json(receipt_payload)
                self._store._reject_sensitive_command_fields(receipt_payload)
        except Exception:
            # Deliberately discard exception text; it may contain credentials.
            outcome = "UNKNOWN"
            receipt_payload = {"kind": "native_dispatch", "outcome": "UNKNOWN"}

        receipt = CtpDispatchReceipt(
            receipt_type="ctp_dispatch_receipt.v2",
            command_id=claimed.command_id,
            account_key=claimed.account_key,
            scope_key=claimed.scope_key,
            trading_day=claimed.trading_day,
            operation=claimed.operation,
            request_payload_sha256=claimed.request_payload_sha256,
            reservation_managed_intent_id=claimed.reservation_managed_intent_id,
            order_ref=claimed.order_ref,
            cancel_target_order_ref=claimed.cancel_target_order_ref,
            cancel_target_exchange_id=claimed.cancel_target_exchange_id,
            cancel_target_order_sys_id=claimed.cancel_target_order_sys_id,
            cancel_target_front_id=claimed.cancel_target_front_id,
            cancel_target_session_id=claimed.cancel_target_session_id,
            approval_use_id=claimed.approval_use_id,
            approval_digest=claimed.approval_digest,
            session_binding_sha256=claimed.session_binding_sha256,
            outcome=outcome,
            native_receipt_payload=receipt_payload,
            correlation_key=claimed.correlation_key,
            local_queue_receipt_id=claimed.local_queue_receipt_id,
        )
        self._store.complete_ctp_dispatch_command(
            self._scope, receipt, writer_lease=self._writer_lease
        )
        projection = self._store.read_ctp_dispatch_projection(self._scope, command_id)
        if projection is None:
            raise ContractValidationError("managed CTP durable projection is missing")
        return projection




__all__ = [
    "CtpManagedDispatchBinding",
    "CtpManagedPreparedDispatch",
    "CtpManagedSingleWorkerCandidate",
    "CtpNativeDispatchResult",
    "CtpNativeDispatchSender",
    "stable_managed_command_id",
]
