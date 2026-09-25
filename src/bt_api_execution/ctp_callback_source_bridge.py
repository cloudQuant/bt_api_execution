"""Fail-closed bridge from the CTP TraderClient callback queue to v9 envelopes.

The source SDK's callback event is an immutable snapshot, not an attestation:
its dataclass can be constructed by a caller and it deliberately remains
unbound.  This bridge therefore never accepts an event or a
``CtpNativeSessionContext`` from its caller.  It reads a persisted dispatched
command, snapshots the current logged-in TraderClient lifecycle, and polls the
event from that exact client's queue.  The session context is derived inside
the bridge and is accepted only when the durable command's session-binding
echo matches the current source facts exactly.

Before returning, the bridge also claims the SDK queue's exclusive consumer lease;
clients that only expose the legacy bare queue wait are rejected.

The resulting envelope is still evidence for an injected verifier, not a
provider acknowledgement or write grant.  The store's default callback
verifier remains rejecting; this module is not registered by a runtime.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Protocol, cast

from .contracts import ExecutionScope, canonical_json, payload_sha256
from .ctp_native_callbacks import (
    CtpNativeCallbackEnvelope,
    CtpNativeSessionContext,
    ctp_native_session_generation_id,
    map_ctp_native_order_action_callback,
    map_ctp_native_order_return,
)
from .errors import ContractValidationError
from .store import (
    CtpDispatchCallbackKey,
    CtpDispatchCommand,
    CtpDispatchCorrelationKey,
    SqliteExecutionStore,
)

_SOURCE_BINDING_TYPE = "bt_api_ctp.trader_callback_source.v1"
_LIFECYCLE_BINDING_TYPE = "ctp_native_callback_lifecycle_binding.v1"
_BOUND_ENVELOPE_TYPE = "ctp_lifecycle_bound_native_callback_envelope.v1"


class _TraderSpiSource(Protocol):
    _c: object
    _native_api: object | None
    _native_spi_source_id: object


class _TraderClientSource(Protocol):
    _query_state_lock: Any
    _api: object | None
    _spi: _TraderSpiSource | None
    _session_native_api: object | None
    _callback_source_instance_id: object
    _native_client_epoch: object
    _native_api_source_id: object
    _native_api_generation: object
    _connection_generation: object
    _bound_broker_id: object
    _bound_user_id: object
    _bound_front: object
    _session_native_front: object
    _trading_day: object
    _front_id: object
    _session_id: object
    _connected: object
    _login_state: object
    _callback_source_sequence: object

    def _bound_identity_is_current(self, *, require_active_front: bool = False) -> bool: ...

    def _claim_native_callback_event_consumer(self) -> object: ...

    def _wait_native_callback_event_for_consumer(
        self, consumer_token: object, timeout: float = 5.0
    ) -> object | None: ...

    def _release_native_callback_event_consumer(self, consumer_token: object) -> None: ...


def _contract_error(detail: str) -> ContractValidationError:
    return ContractValidationError("CTP callback source bridge: " + detail)


def _source_text(value: object, name: str, *, maximum: int = 256) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum
        or not value.isascii()
        or "\x00" in value
    ):
        raise _contract_error("invalid source " + name)
    return value


def _source_uuid(value: object, name: str) -> str:
    if (
        type(value) is not str
        or len(value) != 32
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise _contract_error("invalid opaque source " + name)
    return value


def _positive_int(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise _contract_error("invalid source " + name)
    return value


@dataclass(frozen=True, slots=True)
class _NativeSourceFacts:
    source_instance_id: str
    native_client_epoch: str
    native_api_source_id: str
    native_spi_source_id: str
    native_api_generation: int
    connection_generation: int
    login_broker_id: str
    login_investor_id: str
    login_front: str
    login_trading_day: str
    login_front_id: int
    login_session_id: int
    callback_source_sequence_baseline: int

    @property
    def account_fingerprint(self) -> str:
        digest = sha256(f"{self.login_broker_id}:{self.login_investor_id}".encode())
        return "acct_" + digest.hexdigest()[:16]

    @property
    def session_generation_id(self) -> str:
        return ctp_native_session_generation_id(
            self.native_api_generation,
            self.connection_generation,
            self.native_client_epoch,
            self.login_front_id,
            self.login_session_id,
        )

    def to_payload(self) -> dict[str, str | int | bool]:
        return {
            "binding_type": _SOURCE_BINDING_TYPE,
            "source_instance_id": self.source_instance_id,
            "native_client_epoch": self.native_client_epoch,
            "native_api_source_id": self.native_api_source_id,
            "native_spi_source_id": self.native_spi_source_id,
            "native_api_generation": self.native_api_generation,
            "connection_generation": self.connection_generation,
            "login_verified": True,
            "login_broker_id": self.login_broker_id,
            "login_investor_id": self.login_investor_id,
            "login_front": self.login_front,
            "login_trading_day": self.login_trading_day,
            "login_front_id": self.login_front_id,
            "login_session_id": self.login_session_id,
            "account_fingerprint": self.account_fingerprint,
            "callback_source_sequence_baseline": self.callback_source_sequence_baseline,
        }

    def same_login_lifecycle(self, other: _NativeSourceFacts) -> bool:
        """Compare the session identity while allowing its queue sequence to advance."""

        return (
            self.source_instance_id,
            self.native_client_epoch,
            self.native_api_source_id,
            self.native_spi_source_id,
            self.native_api_generation,
            self.connection_generation,
            self.login_broker_id,
            self.login_investor_id,
            self.login_front,
            self.login_trading_day,
            self.login_front_id,
            self.login_session_id,
        ) == (
            other.source_instance_id,
            other.native_client_epoch,
            other.native_api_source_id,
            other.native_spi_source_id,
            other.native_api_generation,
            other.connection_generation,
            other.login_broker_id,
            other.login_investor_id,
            other.login_front,
            other.login_trading_day,
            other.login_front_id,
            other.login_session_id,
        )


def _capture_logged_in_source(native_trader_client: object) -> _NativeSourceFacts:
    """Read facts from the pinned SDK client under its own lifecycle lock.

    This intentionally binds to the private lifecycle fields of the source API
    introduced with ``CtpTraderCallbackSourceEvent``. Missing or renamed
    fields reject; there is no public-property or caller-context fallback.
    """

    lock = getattr(native_trader_client, "_query_state_lock", None)
    if lock is None or not callable(getattr(lock, "__enter__", None)):
        raise _contract_error("native TraderClient lifecycle lock is unavailable")

    try:
        client = cast("_TraderClientSource", native_trader_client)
        with lock:
            api = client._api
            spi = client._spi
            if (
                api is None
                or spi is None
                or spi._c is not client
                or spi._native_api is not api
                or client._session_native_api is not api
            ):
                raise _contract_error("native API/SPI lifecycle pair is not current")
            identity_check = getattr(client, "_bound_identity_is_current", None)
            if (
                not callable(identity_check)
                or identity_check(require_active_front=True) is not True
            ):
                raise _contract_error("logged-in source identity is not current")
            if client._connected is not True or client._login_state != "logged_in":
                raise _contract_error("native TraderClient is not logged in")
            if client._session_native_front != client._bound_front or not client._bound_front:
                raise _contract_error("logged-in native front is not current")

            source_instance_id = _source_uuid(client._callback_source_instance_id, "instance ID")
            native_client_epoch = _source_uuid(client._native_client_epoch, "client epoch")
            native_api_source_id = _source_uuid(client._native_api_source_id, "API source ID")
            native_spi_source_id = _source_uuid(spi._native_spi_source_id, "SPI source ID")
            native_api_generation = _positive_int(client._native_api_generation, "API generation")
            connection_generation = _positive_int(
                client._connection_generation, "connection generation"
            )
            broker_id = _source_text(client._bound_broker_id, "login broker ID")
            investor_id = _source_text(client._bound_user_id, "login investor ID")
            login_front = _source_text(client._session_native_front, "login front")
            trading_day = _source_text(client._trading_day, "login trading day", maximum=8)
            if (
                len(trading_day) != 8
                or not trading_day.isdigit()
                or client._session_native_front != client._bound_front
            ):
                raise _contract_error("invalid logged-in trading session facts")
            front_id = _positive_int(client._front_id, "login FrontID")
            session_id = _positive_int(client._session_id, "login SessionID")
            sequence = client._callback_source_sequence
            if type(sequence) is not int or sequence < 0:
                raise _contract_error("invalid callback source sequence baseline")
            return _NativeSourceFacts(
                source_instance_id=source_instance_id,
                native_client_epoch=native_client_epoch,
                native_api_source_id=native_api_source_id,
                native_spi_source_id=native_spi_source_id,
                native_api_generation=native_api_generation,
                connection_generation=connection_generation,
                login_broker_id=broker_id,
                login_investor_id=investor_id,
                login_front=login_front,
                login_trading_day=trading_day,
                login_front_id=front_id,
                login_session_id=session_id,
                callback_source_sequence_baseline=sequence,
            )
    except ContractValidationError:
        raise
    except Exception as exc:
        raise _contract_error("unable to read native TraderClient lifecycle") from exc


def ctp_native_callback_source_facts(native_trader_client: object) -> dict[str, str | int | bool]:
    """Return a staging-time snapshot; it is not a session grant or attestation.

    The bridge recaptures the live client and compares this exact payload with
    the durable command before issuing its in-memory lifecycle binding.
    """

    return _capture_logged_in_source(native_trader_client).to_payload()


def _claim_native_callback_consumer(native_trader_client: object) -> object:
    """Require the SDK's queue-level exclusive-consumer capability."""

    claim = getattr(native_trader_client, "_claim_native_callback_event_consumer", None)
    wait = getattr(native_trader_client, "_wait_native_callback_event_for_consumer", None)
    release = getattr(native_trader_client, "_release_native_callback_event_consumer", None)
    if not callable(claim) or not callable(wait) or not callable(release):
        raise _contract_error("native callback source has no exclusive queue consumer lease")
    try:
        token = claim()
    except ContractValidationError:
        raise
    except Exception as exc:
        raise _contract_error("native callback queue consumer lease claim failed") from exc
    if token is None:
        raise _contract_error("native callback queue consumer lease is unavailable")
    return token


def _release_native_callback_consumer(native_trader_client: object, token: object) -> None:
    """Release a lease acquired before a bridge instance could be returned."""

    release = getattr(native_trader_client, "_release_native_callback_event_consumer", None)
    if not callable(release):
        raise _contract_error("native callback queue consumer lease cleanup is unavailable")
    try:
        release(token)
    except Exception as exc:
        raise _contract_error("native callback queue consumer lease cleanup failed") from exc


@dataclass(frozen=True, slots=True)
class _LifecycleSessionBinding:
    session: CtpNativeSessionContext
    source_facts: _NativeSourceFacts
    session_binding_sha256: str
    lifecycle_binding_sha256: str

    def to_payload(self) -> dict[str, object]:
        return {
            "binding_type": _LIFECYCLE_BINDING_TYPE,
            "session_binding_sha256": self.session_binding_sha256,
            "lifecycle_binding_sha256": self.lifecycle_binding_sha256,
            "source_facts": self.source_facts.to_payload(),
            "source_scope": self.session.to_payload(),
        }


@dataclass(frozen=True, slots=True)
class CtpLifecycleBoundNativeCallbackEnvelope:
    """Envelope mapped by a live, lifecycle-bound source bridge.

    ``to_payload`` intentionally has a new outer version. Consumers must not
    silently treat it as the legacy, caller-context mapping envelope.
    """

    callback_envelope: CtpNativeCallbackEnvelope
    _session_binding: _LifecycleSessionBinding
    event_source_sequence: int
    callback_monotonic_ns: int

    @property
    def callback_key(self) -> CtpDispatchCallbackKey:
        return self.callback_envelope.callback_key

    def to_payload(self) -> dict[str, object]:
        facts = self._session_binding.source_facts
        event = {
            "event_type": self.callback_envelope.source_callback,
            "source_instance_id": facts.source_instance_id,
            "native_client_epoch": facts.native_client_epoch,
            "native_api_source_id": facts.native_api_source_id,
            "native_spi_source_id": facts.native_spi_source_id,
            "native_api_generation": facts.native_api_generation,
            "connection_generation": facts.connection_generation,
            "login_verified": True,
            "login_broker_id": facts.login_broker_id,
            "login_investor_id": facts.login_investor_id,
            "login_front": facts.login_front,
            "login_trading_day": facts.login_trading_day,
            "login_front_id": facts.login_front_id,
            "login_session_id": facts.login_session_id,
            "callback_session_matches_login": True,
            "managed_session_epoch": None,
            "managed_session_epoch_bound": False,
            "scope_binding": "unbound",
            "trust_boundary": "source_facts_only",
            "source_sequence": self.event_source_sequence,
            "callback_monotonic_ns": self.callback_monotonic_ns,
            "stable_source_key": [
                "ctp-trader-callback-v1",
                facts.source_instance_id,
                facts.native_client_epoch,
                facts.native_api_source_id,
                facts.native_spi_source_id,
                facts.native_api_generation,
                facts.connection_generation,
                self.event_source_sequence,
            ],
        }
        return {
            "envelope_type": _BOUND_ENVELOPE_TYPE,
            "callback_envelope": self.callback_envelope.to_payload(),
            "lifecycle_binding": self._session_binding.to_payload(),
            "source_event": event,
        }


def _read_event_fields(event: object) -> dict[str, object]:
    entries = getattr(event, "raw_correlation_fields", None)
    if type(entries) is not tuple:
        raise _contract_error("source callback raw fields are unavailable")
    fields: dict[str, object] = {}
    for item in entries:
        if type(item) is not tuple or len(item) != 2:
            raise _contract_error("source callback raw field entry is invalid")
        name, value = item
        if (
            type(name) is not str
            or type(value) not in (str, bytes, int, float, bool, type(None))
            or name in fields
        ):
            raise _contract_error("source callback raw fields are ambiguous")
        fields[name] = value
    return fields


class CtpNativeCallbackSourceBridge:
    """Poll one SDK client's native callback queue under a persisted binding and lease.

    The bridge binds while the exact command is ``CLAIMED`` and before its
    native call, while the source client reports a successful, current login.
    It never accepts source events or session contexts as method arguments.
    """

    def __init__(
        self,
        *,
        _token: object,
        store: SqliteExecutionStore,
        scope: ExecutionScope,
        command: CtpDispatchCommand,
        native_trader_client: object,
        binding: _LifecycleSessionBinding,
        consumer_token: object,
    ) -> None:
        if _token is not _BRIDGE_CONSTRUCTOR_TOKEN:
            raise _contract_error("bridge instances must be issued after login")
        self._store = store
        self._scope = scope
        self._command_id = command.command_id
        correlation = command.correlation_key
        if correlation is None:
            raise _contract_error("bridge command has no durable correlation")
        self._correlation = correlation
        self._native_trader_client = cast("_TraderClientSource", native_trader_client)
        self._binding = binding
        self._consumer_token: object | None = consumer_token
        self._last_source_sequence = binding.source_facts.callback_source_sequence_baseline
        self._closed = False
        self._state_generation = 0
        self._poll_in_flight = False
        self._lock = threading.Lock()

    @classmethod
    def bind_after_login(
        cls,
        *,
        store: SqliteExecutionStore,
        scope: ExecutionScope,
        command_id: str,
        native_trader_client: object,
    ) -> CtpNativeCallbackSourceBridge:
        """Issue an in-memory binding from current lifecycle and durable facts.

        ``command.session_binding['native_callback_source']`` must equal the
        live SDK source facts returned by :func:`ctp_native_callback_source_facts`.
        A missing or stale source binding or exclusive queue-consumer capability
        rejects. The caller cannot supply a session context or a callback event.
        """

        if type(store) is not SqliteExecutionStore or type(scope) is not ExecutionScope:
            raise _contract_error("exact SQLite store and execution scope types are required")
        try:
            command = store.read_ctp_dispatch_command(scope, command_id)
        except Exception as exc:
            raise _contract_error("durable dispatch command is unavailable") from exc
        if type(command) is not CtpDispatchCommand:
            raise _contract_error("durable dispatch command is missing")
        if command.status != "CLAIMED":
            raise _contract_error("bridge must bind while the command is CLAIMED")
        correlation = command.correlation_key
        if type(correlation) is not CtpDispatchCorrelationKey:
            raise _contract_error("typed durable command correlation is required")
        if command.session_binding_sha256 != correlation.session_binding_sha256:
            raise _contract_error("durable command session binding digest differs")

        facts = _capture_logged_in_source(native_trader_client)
        source_echo = command.session_binding.get("native_callback_source")
        if not isinstance(source_echo, Mapping):
            raise _contract_error("durable command has no callback source lifecycle binding")
        if canonical_json(dict(source_echo)) != canonical_json(facts.to_payload()):
            raise _contract_error("durable callback source binding differs from live login")

        session_generation_id = facts.session_generation_id
        if (
            session_generation_id != correlation.session_generation_id
            or facts.login_trading_day != correlation.trading_day
            or facts.login_front_id != correlation.dispatch_front_id
            or facts.login_session_id != correlation.dispatch_session_id
            or command.session_binding.get("session_generation_id") != session_generation_id
            or command.session_binding.get("dispatch_front_id") != facts.login_front_id
            or command.session_binding.get("dispatch_session_id") != facts.login_session_id
        ):
            raise _contract_error("live native login does not match durable command session")

        session = CtpNativeSessionContext(
            account_key=correlation.account_key,
            scope_key=correlation.scope_key,
            trading_day=facts.login_trading_day,
            session_epoch=facts.native_client_epoch,
            session_generation_id=session_generation_id,
            account_fingerprint=facts.account_fingerprint,
            native_api_generation=facts.native_api_generation,
            connection_generation=facts.connection_generation,
            dispatch_front_id=facts.login_front_id,
            dispatch_session_id=facts.login_session_id,
        )
        session.require_correlation(correlation)
        lifecycle_digest = payload_sha256(
            {
                "binding_type": _LIFECYCLE_BINDING_TYPE,
                "session_binding_sha256": command.session_binding_sha256,
                "correlation_key": correlation.to_payload(),
                "source_facts": facts.to_payload(),
            }
        )
        binding = _LifecycleSessionBinding(
            session=session,
            source_facts=facts,
            session_binding_sha256=command.session_binding_sha256,
            lifecycle_binding_sha256=lifecycle_digest,
        )
        consumer_token = _claim_native_callback_consumer(native_trader_client)
        try:
            post_claim_facts = _capture_logged_in_source(native_trader_client)
            if not facts.same_login_lifecycle(post_claim_facts):
                raise _contract_error("native source lifecycle changed while claiming queue owner")
            return cls(
                _token=_BRIDGE_CONSTRUCTOR_TOKEN,
                store=store,
                scope=scope,
                command=command,
                native_trader_client=native_trader_client,
                binding=binding,
                consumer_token=consumer_token,
            )
        except Exception:
            _release_native_callback_consumer(native_trader_client, consumer_token)
            raise

    def next_envelope(
        self, *, timeout: float = 5.0
    ) -> CtpLifecycleBoundNativeCallbackEnvelope | None:
        """Poll and map one exact source-queue event, or return ``None`` on timeout."""

        with self._lock:
            self._require_open()
            if self._poll_in_flight:
                raise _contract_error("native callback bridge already has a poll in progress")
            consumer_token = self._consumer_token
            if consumer_token is None:
                raise _contract_error("native callback queue consumer lease is unavailable")
            self._poll_in_flight = True
            poll_generation = self._state_generation

        event: object | None = None
        try:
            self._require_command_binding_current()
            self._require_live_source_current()
            with self._lock:
                self._require_poll_current(consumer_token, poll_generation)

            # The SDK wait can block. The bridge state lock stays free so close()
            # can revoke the queue lease and wake this wait immediately.
            event = self._native_trader_client._wait_native_callback_event_for_consumer(
                consumer_token, timeout=timeout
            )

            with self._lock:
                self._require_poll_current(consumer_token, poll_generation)
                self._require_command_binding_current()
                self._require_live_source_current()
                if event is None:
                    self._poll_in_flight = False
                    return None
                mapped = self._map_source_event(event)
                sequence = getattr(event, "source_sequence", None)
                monotonic_ns = getattr(event, "callback_monotonic_ns", None)
                if type(sequence) is not int or type(monotonic_ns) is not int:
                    raise _contract_error("source event sequence or time is invalid")
                self._last_source_sequence = sequence
                # Finalize while holding the state lock. A close that acquires
                # the lock after this point is ordered after this completed poll.
                self._poll_in_flight = False
                return CtpLifecycleBoundNativeCallbackEnvelope(
                    callback_envelope=mapped,
                    _session_binding=self._binding,
                    event_source_sequence=sequence,
                    callback_monotonic_ns=monotonic_ns,
                )
        except Exception as exc:
            if not self._poison(expected_generation=poll_generation):
                raise _contract_error(
                    "bridge closed during callback poll; callback may have been consumed and "
                    "discarded, so the outcome may require UNKNOWN reconciliation"
                ) from exc
            if isinstance(exc, ContractValidationError):
                raise
            raise _contract_error("native callback source queue or lifecycle check failed") from exc
        finally:
            with self._lock:
                self._poll_in_flight = False

    def _require_command_binding_current(self) -> None:
        try:
            command = self._store.read_ctp_dispatch_command(self._scope, self._command_id)
        except Exception as exc:
            raise _contract_error("durable command reread failed") from exc
        if (
            command is None
            or command.status not in {"CLAIMED", "COMPLETED", "UNKNOWN"}
            or command.correlation_key != self._correlation
            or command.session_binding_sha256 != self._binding.session_binding_sha256
        ):
            raise _contract_error("durable command changed after lifecycle binding")

    def _require_live_source_current(self) -> None:
        try:
            current = _capture_logged_in_source(self._native_trader_client)
        except ContractValidationError:
            raise
        if not self._binding.source_facts.same_login_lifecycle(current):
            raise _contract_error("native source lifecycle changed after binding")

    def _map_source_event(self, event: object) -> CtpNativeCallbackEnvelope:
        facts = self._binding.source_facts
        expected_stable_key = (
            "ctp-trader-callback-v1",
            facts.source_instance_id,
            facts.native_client_epoch,
            facts.native_api_source_id,
            facts.native_spi_source_id,
            facts.native_api_generation,
            facts.connection_generation,
        )
        event_source_key = getattr(event, "stable_source_key", None)
        source_sequence = getattr(event, "source_sequence", None)
        callback_monotonic_ns = getattr(event, "callback_monotonic_ns", None)
        if (
            type(source_sequence) is not int
            or source_sequence != self._last_source_sequence + 1
            or type(callback_monotonic_ns) is not int
            or callback_monotonic_ns < 0
            or type(event_source_key) is not tuple
            or event_source_key != (*expected_stable_key, source_sequence)
        ):
            raise _contract_error("source event identity or sequence does not match lifecycle")
        if (
            getattr(event, "source_instance_id", None) != facts.source_instance_id
            or getattr(event, "native_client_epoch", None) != facts.native_client_epoch
            or getattr(event, "native_api_source_id", None) != facts.native_api_source_id
            or getattr(event, "native_spi_source_id", None) != facts.native_spi_source_id
            or getattr(event, "native_api_generation", None) != facts.native_api_generation
            or getattr(event, "connection_generation", None) != facts.connection_generation
            or getattr(event, "login_verified", None) is not True
            or getattr(event, "login_broker_id", None) != facts.login_broker_id
            or getattr(event, "login_investor_id", None) != facts.login_investor_id
            or getattr(event, "login_front", None) != facts.login_front
            or getattr(event, "login_trading_day", None) != facts.login_trading_day
            or getattr(event, "login_front_id", None) != facts.login_front_id
            or getattr(event, "login_session_id", None) != facts.login_session_id
            or getattr(event, "callback_session_matches_login", None) is not True
            or getattr(event, "managed_session_epoch", None) is not None
            or getattr(event, "managed_session_epoch_bound", None) is not False
            or getattr(event, "scope_binding", None) != "unbound"
            or getattr(event, "trust_boundary", None) != "source_facts_only"
        ):
            raise _contract_error("source event login or origin facts differ from lifecycle")

        event_type = getattr(event, "event_type", None)
        if type(event_type) is not str or event_type not in {
            "OnRtnOrder",
            "OnRspOrderAction",
            "OnErrRtnOrderAction",
        }:
            raise _contract_error("unsupported native callback event type")
        fields = _read_event_fields(event)
        if event_type == "OnRtnOrder":
            if "nRequestID" in fields or "bIsLast" in fields:
                raise _contract_error("order return has action-only callback arguments")
            return map_ctp_native_order_return(
                self._correlation,
                self._binding.session,
                fields,
            )

        response_info = None
        if "ErrorID" in fields:
            response_info = {"ErrorID": fields["ErrorID"]}
        elif "ErrorMsg" in fields:
            # A captured message without its native error code is ambiguous.
            raise _contract_error("response message has no error code")
        if event_type == "OnRspOrderAction":
            if "nRequestID" not in fields or "bIsLast" not in fields:
                raise _contract_error("terminal action response arguments are missing")
            request_id = fields["nRequestID"]
            if type(request_id) is not int:
                raise _contract_error("terminal action response request ID is invalid")
            return map_ctp_native_order_action_callback(
                self._correlation,
                self._binding.session,
                event_type,
                fields,
                response_info,
                request_id=request_id,
                is_last=fields["bIsLast"],
            )
        if "nRequestID" in fields or "bIsLast" in fields:
            raise _contract_error("action error callback has response-only arguments")
        return map_ctp_native_order_action_callback(
            self._correlation,
            self._binding.session,
            event_type,
            fields,
            response_info,
        )

    def _require_open(self) -> None:
        if self._closed:
            raise _contract_error("bridge is closed")

    def _require_poll_current(self, token: object, generation: int) -> None:
        if (
            self._closed
            or self._state_generation != generation
            or self._consumer_token is not token
        ):
            raise _contract_error("bridge closed or queue lease changed during callback poll")

    def close(self) -> None:
        """Release exclusive callback-queue ownership after the final poll."""

        with self._lock:
            if not self._closed:
                self._closed = True
                self._state_generation += 1
            token = self._consumer_token
        self._release_consumer_token(token, suppress_errors=False)

    def _poison(self, *, expected_generation: int | None = None) -> bool:
        with self._lock:
            if expected_generation is not None and (
                self._closed or self._state_generation != expected_generation
            ):
                return False
            if not self._closed:
                self._closed = True
                self._state_generation += 1
            token = self._consumer_token
        self._release_consumer_token(token, suppress_errors=True)
        return True

    def _release_consumer_token(self, token: object | None, *, suppress_errors: bool) -> None:
        if token is None:
            return
        try:
            self._native_trader_client._release_native_callback_event_consumer(token)
        except Exception as exc:
            if not suppress_errors:
                raise _contract_error(
                    "native callback queue consumer lease cleanup failed"
                ) from exc
            return
        with self._lock:
            if self._consumer_token is token:
                self._consumer_token = None


_BRIDGE_CONSTRUCTOR_TOKEN = object()


__all__ = [
    "CtpLifecycleBoundNativeCallbackEnvelope",
    "CtpNativeCallbackSourceBridge",
    "ctp_native_callback_source_facts",
]
