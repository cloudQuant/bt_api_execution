"""Opt-in bridge from lifecycle-bound CTP callbacks to the durable ledger.

This adapter does not authenticate callback evidence itself. It requires an
explicit ``CtpDispatchCallbackVerifier`` and passes it the detached payload
from the live ``CtpNativeCallbackSourceBridge`` envelope. The Store remains
the only component that verifies the injected result and atomically appends
the callback ledger row and typed projection.
"""

from __future__ import annotations

import threading

from .contracts import canonical_json
from .ctp_callback_source_bridge import (
    CtpLifecycleBoundNativeCallbackEnvelope,
    CtpNativeCallbackSourceBridge,
)
from .errors import ContractValidationError
from .store import (
    CtpDispatchCallbackApplyResult,
    CtpDispatchCallbackVerifier,
    ExecutionScope,
    SqliteExecutionStore,
    WriterLease,
)


class _LifecycleBoundCallbackVerifier:
    """Bind the injected verifier to one bridge envelope and current source."""

    def __init__(self, bridge, envelope, payload, delegate) -> None:
        self._bridge = bridge
        self._envelope = envelope
        self._payload_json = canonical_json(payload)
        self._delegate = delegate

    def verify_callback(self, command, callback, callback_payload, *, now_ns):
        if (
            callback != self._envelope.callback_key
            or command.command_id != callback.correlation_key.command_id
            or canonical_json(dict(callback_payload)) != self._payload_json
        ):
            raise ContractValidationError("verified callback differs from consumed source event")
        # apply_next holds the SDK lifecycle lock through this callback and the
        # Store transaction. This final live-source check therefore remains
        # current through the ledger commit, including against API replacement.
        self._bridge._require_live_source_current()
        evidence = self._delegate.verify_callback(
            command, callback, callback_payload, now_ns=now_ns
        )
        # The lifecycle lock is an RLock in the pinned SDK. A same-thread
        # injected verifier can therefore reenter SDK methods and mutate the
        # session even while the adapter owns the lock. Re-read after the
        # verifier and reject before the Store starts its commit transaction.
        self._bridge._require_live_source_current()
        return evidence


class CtpNativeCallbackLedgerAdapter:
    """Apply one current SDK callback through an injected trusted verifier.

    Construct this adapter only after the durable command is ``CLAIMED`` and
    before native submission. ``bind_after_login`` binds the command, exact
    logged-in SDK lifecycle, exclusive callback queue lease, and an immutable
    account source-consumer fence. The fence is never removed by callback
    application or adapter close: there is no accepted source-drain/terminal
    protocol that can prove no later callback will arrive. The adapter latches
    closed after any consumed callback that cannot be durably verified and
    applied; it never retries a consumed source event.
    """

    def __init__(
        self,
        *,
        store: SqliteExecutionStore,
        scope: ExecutionScope,
        command_id: str,
        writer_lease: WriterLease,
        source_bridge: CtpNativeCallbackSourceBridge,
        callback_verifier: CtpDispatchCallbackVerifier,
        source_lifecycle_fence_id: str,
    ) -> None:
        if type(store) is not SqliteExecutionStore or type(scope) is not ExecutionScope:
            raise ContractValidationError("exact CTP execution store and scope are required")
        if type(command_id) is not str or not command_id:
            raise ContractValidationError("durable CTP command id is required")
        if type(source_bridge) is not CtpNativeCallbackSourceBridge:
            raise ContractValidationError("lifecycle-bound CTP callback source bridge is required")
        if (
            source_bridge._store is not store
            or source_bridge._scope != scope
            or source_bridge._command_id != command_id
        ):
            raise ContractValidationError("callback bridge does not match durable command scope")
        if not callable(getattr(callback_verifier, "verify_callback", None)):
            raise ContractValidationError("injected trusted CTP callback verifier is required")
        if (
            type(source_lifecycle_fence_id) is not str
            or len(source_lifecycle_fence_id) != 32
            or any(char not in "0123456789abcdef" for char in source_lifecycle_fence_id)
        ):
            raise ContractValidationError("durable CTP callback source lifecycle fence is required")

        self._store = store
        self._scope = scope
        self._command_id = command_id
        self._writer_lease = writer_lease
        self._source_bridge = source_bridge
        self._callback_verifier = callback_verifier
        self._source_lifecycle_fence_id = source_lifecycle_fence_id
        self._source_state_lock = getattr(
            source_bridge._native_trader_client, "_query_state_lock", None
        )
        if not callable(getattr(self._source_state_lock, "__enter__", None)):
            raise ContractValidationError("native CTP lifecycle lock is required")
        self._state_lock = threading.Lock()
        self._poll_in_flight = False
        self._closed = False
        self._poisoned = False

    @classmethod
    def bind_after_login(
        cls,
        *,
        store: SqliteExecutionStore,
        scope: ExecutionScope,
        command_id: str,
        writer_lease: WriterLease,
        native_trader_client: object,
        callback_verifier: CtpDispatchCallbackVerifier,
    ) -> CtpNativeCallbackLedgerAdapter:
        """Bind an exact claimed command and SDK login before native send."""

        bridge = CtpNativeCallbackSourceBridge.bind_after_login(
            store=store,
            scope=scope,
            command_id=command_id,
            native_trader_client=native_trader_client,
        )
        try:
            # The immutable database lifecycle fence is durable before this
            # adapter can poll or the caller can perform its native send.
            source_lifecycle_fence_id = store.create_ctp_callback_source_lifecycle_fence(
                scope, command_id, writer_lease=writer_lease
            )
            return cls(
                store=store,
                scope=scope,
                command_id=command_id,
                writer_lease=writer_lease,
                source_bridge=bridge,
                callback_verifier=callback_verifier,
                source_lifecycle_fence_id=source_lifecycle_fence_id,
            )
        except Exception:
            try:
                bridge.close()
            except Exception:
                # Preserve the binding failure; queue cleanup is best effort
                # and never clears the durable account lifecycle fence.
                pass
            raise

    def apply_next(
        self, *, timeout: float | None = 5.0
    ) -> CtpDispatchCallbackApplyResult | None:
        """Poll and durably apply one callback, or return ``None`` on timeout.

        The complete lifecycle-bound envelope is given to the injected
        verifier as callback payload. Caller-supplied callback fields are
        never accepted by this method. If source consumption or verification
        fails, the adapter closes permanently because the queue item may have
        been consumed without a matching ledger commit.
        """

        with self._state_lock:
            if self._closed or self._poisoned:
                raise ContractValidationError("CTP callback ledger adapter is closed")
            if self._poll_in_flight:
                raise ContractValidationError("CTP callback ledger adapter poll is already active")
            self._poll_in_flight = True

        try:
            envelope = self._source_bridge.next_envelope(timeout=timeout)
            if envelope is None:
                return None
            if type(envelope) is not CtpLifecycleBoundNativeCallbackEnvelope:
                raise ContractValidationError("typed lifecycle-bound CTP callback is required")
            with self._state_lock:
                if self._closed or self._poisoned:
                    raise ContractValidationError("CTP callback adapter closed during source poll")

            callback_payload = envelope.to_payload()
            if (
                type(callback_payload) is not dict
                or callback_payload.get("envelope_type")
                != "ctp_lifecycle_bound_native_callback_envelope.v1"
            ):
                raise ContractValidationError("invalid lifecycle-bound CTP callback payload")
            bound_verifier = _LifecycleBoundCallbackVerifier(
                self._source_bridge,
                envelope,
                callback_payload,
                self._callback_verifier,
            )
            # The source API/SPI generation cannot change between the final
            # source recapture, trusted verification, and SQLite commit. This
            # lock is the same lock used by the SDK callback writers/setters.
            with self._source_state_lock:
                self._source_bridge._require_live_source_current()
                applied = self._store.apply_ctp_verified_dispatch_callback(
                    self._scope,
                    self._command_id,
                    envelope.callback_key,
                    callback_payload,
                    writer_lease=self._writer_lease,
                    callback_verifier=bound_verifier,
                    source_lifecycle_fence_id=self._source_lifecycle_fence_id,
                )
            return applied
        except Exception:
            self._poison()
            # Source callbacks may contain provider-owned text. Keep the
            # public error fixed and omit exception chaining and payload data.
            raise ContractValidationError(
                "CTP callback may have been consumed without a durable verified ledger commit"
            ) from None
        finally:
            with self._state_lock:
                self._poll_in_flight = False

    def close(self) -> None:
        """Release the source queue lease; a blocked poll is revoked by bridge close."""

        with self._state_lock:
            if self._closed:
                return
            self._closed = True
        try:
            self._source_bridge.close()
        except Exception:
            # Cleanup diagnostics may contain provider-owned details. They do
            # not change the durable fence or the adapter's fail-closed state.
            pass

    def _poison(self) -> None:
        with self._state_lock:
            self._poisoned = True
            self._closed = True
        try:
            self._source_bridge.close()
        except Exception:
            # Keep the public adapter failure fixed and sanitized even if the
            # SDK queue lease cleanup itself fails.
            pass

    def __enter__(self) -> CtpNativeCallbackLedgerAdapter:
        with self._state_lock:
            if self._closed or self._poisoned:
                raise ContractValidationError("CTP callback ledger adapter is closed")
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


__all__ = ["CtpNativeCallbackLedgerAdapter"]
