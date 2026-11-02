"""Durable recovery orchestration for one service execution run.

``service_recovery`` owns the durable rows; this module owns the *sequence* that
keeps them consistent with the execution service: install the restore snapshot,
consume the committed event tail page by page, compare the owner book at both
ends of that window, revalidate the cut before admitting a new intent, and
claim/acknowledge at-least-once callback work.  That sequence is durable
execution truth, so it lives with the journal instead of in a framework adapter.

Two boundaries are deliberate:

* the service client and the owner-book reader are duck-typed callables supplied
  by the caller, because a client transport and an account-book projection belong
  to other owners; the flow only asks them for immutable mappings;
* every failure is reported with a *finite code* this module already knows, never
  with a framework exception type: :class:`ServiceRecoveryFlowError` carries the
  read-path code the caller must surface, :class:`ServiceRecoveryFlowArgumentError`
  carries an operator/configuration rejection, and the journal's own
  :class:`~bt_api_execution.service_recovery.ServiceExecutionRecoveryError`
  propagates unchanged so a caller's rejection vocabulary stays byte-identical.

This package never imports ``backtrader`` and never imports ``bt_api_ctp``.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable, Mapping, Optional

from .service_recovery import (
    ServiceExecutionRecoveryError,
    ServiceExecutionRecoveryNamespace,
)


class ServiceRecoveryFlowError(RuntimeError):
    """One durable recovery read or restore step failed.

    ``code`` is the finite diagnostic the caller must surface unchanged; there is
    no fallback path and no direct dispatch implied by it.
    """

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class ServiceRecoveryFlowArgumentError(RuntimeError):
    """An operator-supplied recovery argument or recovery state is invalid."""


class ServiceRecoveryFlow:
    """Drive one recovery journal against one injected service client.

    ``read_failure_code`` maps an unexpected client/snapshot failure onto the
    caller's finite read diagnostic; the flow never invents one, because the code
    vocabulary belongs to the caller's error contract.
    """

    def __init__(
        self,
        *,
        journal: Any,
        client: Any,
        event_version: int,
        require_owner_baseline: bool,
        read_failure_code: Callable[[BaseException], str],
    ) -> None:
        self._journal = journal
        self._client = client
        self._event_version = event_version
        self._require_owner_baseline = bool(require_owner_baseline)
        self._read_failure_code = read_failure_code
        self._namespace: Optional[ServiceExecutionRecoveryNamespace] = None
        self._consumer_token: Optional[str] = None
        self._event_context_resolver: Optional[Callable[..., Any]] = None
        self._owner_book_reader: Optional[Callable[[], Mapping[str, Any]]] = None
        self._owner_position_reconciliation: Optional[Mapping[str, Any]] = None
        self._initialized = False
        self._startup_cursor: Optional[int] = None

    # -- binding ------------------------------------------------------------

    @property
    def namespace(self) -> Optional[ServiceExecutionRecoveryNamespace]:
        return self._namespace

    @property
    def consumer_token(self) -> Optional[str]:
        return self._consumer_token

    @property
    def startup_cursor(self) -> Optional[int]:
        return self._startup_cursor

    @property
    def owner_position_reconciliation(self) -> Optional[Mapping[str, Any]]:
        """Return the latest local owner-book comparison, never an account projection."""

        value = self._owner_position_reconciliation
        return deepcopy(value) if isinstance(value, Mapping) else None

    def configure_event_context_resolver(self, resolver: Optional[Callable[..., Any]]) -> None:
        if not callable(resolver):
            raise ServiceRecoveryFlowArgumentError("invalid recovery event context resolver")
        current = self._event_context_resolver
        if current is not None and current is not resolver:
            raise ServiceRecoveryFlowArgumentError(
                "recovery event context resolver already configured"
            )
        self._event_context_resolver = resolver

    def bind(self, namespace: Any) -> None:
        """Bind recovery to the authenticated writer scope and stable run ID."""

        if namespace is None or type(namespace) is not ServiceExecutionRecoveryNamespace:
            raise ServiceRecoveryFlowArgumentError("recovery_namespace_unavailable")
        if self._namespace is not None and self._namespace != namespace:
            raise ServiceRecoveryFlowArgumentError("recovery_namespace_already_bound")
        try:
            self._journal.bind_event_version(namespace, self._event_version)
        except ServiceExecutionRecoveryError as error:
            raise ServiceRecoveryFlowArgumentError(
                f"recovery_{error.code.lower()}"
            ) from None
        self._namespace = namespace

    def _require_recovery(self) -> tuple[Any, ServiceExecutionRecoveryNamespace]:
        journal = self._journal
        namespace = self._namespace
        if journal is None or namespace is None:
            raise ServiceRecoveryFlowError("recovery_namespace_unavailable")
        return journal, namespace

    # -- lease and genesis ----------------------------------------------------

    def acquire_consumer_lease(self, owner_token: str) -> None:
        journal, namespace = self._require_recovery()
        try:
            journal.acquire_consumer_lease(namespace, owner_token)
        except ServiceExecutionRecoveryError as error:
            raise ServiceRecoveryFlowError(f"recovery_{error.code.lower()}") from None
        self._consumer_token = owner_token

    def renew_consumer_lease(self, owner_token: str) -> None:
        journal, namespace = self._require_recovery()
        if self._consumer_token != owner_token:
            raise ServiceRecoveryFlowError("recovery_consumer_lease_lost")
        try:
            journal.renew_consumer_lease(namespace, owner_token)
        except ServiceExecutionRecoveryError as error:
            raise ServiceRecoveryFlowError(f"recovery_{error.code.lower()}") from None

    def release_consumer_lease(self, owner_token: str) -> None:
        journal, namespace = self._require_recovery()
        try:
            journal.release_consumer_lease(namespace, owner_token)
        except ServiceExecutionRecoveryError as error:
            raise ServiceRecoveryFlowError(f"recovery_{error.code.lower()}") from None
        if self._consumer_token == owner_token:
            self._consumer_token = None

    def ensure_strategy_genesis(self, *, trade_ledger_empty: bool) -> Mapping[str, Any]:
        """Bind V2 recovery to the run's original cut and an empty host Trade ledger."""

        journal, namespace = self._require_recovery()
        if self._event_version != 2:
            raise ServiceRecoveryFlowError("strategy_genesis_requires_recovery_v2")
        try:
            baseline = journal.recovery_baseline(namespace)
            if baseline is None:
                raise ServiceExecutionRecoveryError("RECOVERY_BASELINE_REQUIRED")
            genesis = {
                "baseline_cursor": baseline["baseline_cursor"],
                "journal_generation": baseline["journal_generation"],
                "scope_directory_revision": baseline["scope_directory_revision"],
                "trade_ledger_empty": trade_ledger_empty,
            }
            journal.install_strategy_genesis(
                namespace,
                genesis,
                owner_token=self._consumer_token,
            )
            return journal.strategy_genesis(namespace)
        except ServiceExecutionRecoveryError as error:
            raise ServiceRecoveryFlowError(f"recovery_{error.code.lower()}") from None

    # -- restore and event tail ----------------------------------------------

    def initialize(
        self,
        *,
        owner_book_reader: Optional[Callable[[], Mapping[str, Any]]] = None,
        page_limit: int = 128,
        max_pages: int = 64,
    ) -> Mapping[str, Any]:
        """Install the authenticated restore snapshot and consume its event tail.

        Snapshot rows are stored as restore evidence only.  They do not create
        host fills or callbacks.  The durable page cursor starts at the snapshot
        cut for a new local run, or remains at its prior committed position for a
        resumed run.
        """

        journal, namespace = self._require_recovery()
        snapshot_reader = getattr(self._client, "query_recovery_snapshot", None)
        if not callable(snapshot_reader):
            raise ServiceRecoveryFlowError("recovery_snapshot_api_unavailable")
        if self._require_owner_baseline and not callable(owner_book_reader):
            raise ServiceRecoveryFlowError("recovery_owner_book_route_unavailable")
        if callable(owner_book_reader):
            self._owner_book_reader = owner_book_reader
            result = self._initialize_with_baseline(
                snapshot_reader,
                owner_book_reader,
                page_limit=page_limit,
                max_pages=max_pages,
            )
            self._mark_initialized(result)
            return result
        try:
            snapshot = snapshot_reader()
            snapshot_changed = journal.install_snapshot(
                namespace,
                snapshot,
                owner_token=self._consumer_token,
            )
        except ServiceExecutionRecoveryError as error:
            raise ServiceRecoveryFlowError(f"recovery_{error.code.lower()}") from None
        except Exception as error:  # noqa: BLE001 - service failure must not fall back
            raise ServiceRecoveryFlowError(self._read_failure_code(error)) from None
        pages_applied = self.consume_pages(page_limit=page_limit, max_pages=max_pages)
        cursor = journal.cursor(namespace)
        result = {
            "snapshot_changed": snapshot_changed,
            "pages_applied": pages_applied,
            "journal_generation": cursor["journal_generation"],
            "scope_directory_revision": cursor["scope_directory_revision"],
            "event_cursor": cursor["event_cursor"],
        }
        self._mark_initialized(result)
        return result

    def _mark_initialized(self, result: Mapping[str, Any]) -> None:
        cursor = result.get("event_cursor")
        if type(cursor) is not int or cursor < 0:
            raise ServiceRecoveryFlowError("recovery_event_cursor_invalid")
        if self._startup_cursor is None:
            self._startup_cursor = cursor
        self._initialized = True

    def refresh_cut(self) -> Mapping[str, Any]:
        """Revalidate the current service cut before admitting a new intent.

        The opening baseline stays immutable.  This performs a fresh snapshot,
        event-tail read and owner-book comparison so order admission does not
        rely on a stale startup reconciliation.
        """

        if not self._initialized:
            return self.initialize(owner_book_reader=self._owner_book_reader)
        if self._owner_book_reader is None:
            raise ServiceRecoveryFlowError("recovery_owner_book_route_unavailable")
        snapshot_reader = getattr(self._client, "query_recovery_snapshot", None)
        if not callable(snapshot_reader):
            raise ServiceRecoveryFlowError("recovery_snapshot_api_unavailable")
        result = self._initialize_with_baseline(
            snapshot_reader,
            self._owner_book_reader,
            page_limit=128,
            max_pages=64,
        )
        # A refresh may advance the durable cursor, but the startup boundary
        # remains fixed for the caller's restore-only/live-event distinction.
        self._mark_initialized(result)
        return result

    def _initialize_with_baseline(
        self,
        snapshot_reader: Callable[[], Any],
        owner_book_reader: Callable[[], Mapping[str, Any]],
        *,
        page_limit: int,
        max_pages: int,
    ) -> Mapping[str, Any]:
        journal, namespace = self._require_recovery()
        pages_applied = 0
        snapshot_changed = False
        for _ in range(4):
            try:
                snapshot = snapshot_reader()
                owner_book = owner_book_reader()
                self._require_matching_owner_cut(snapshot, owner_book, namespace)
                snapshot_changed = journal.install_recovery_baseline(
                    namespace,
                    snapshot,
                    owner_book,
                    owner_token=self._consumer_token,
                ) or snapshot_changed
                pages_applied += self.consume_pages(
                    page_limit=page_limit, max_pages=max_pages
                )
                end_snapshot = snapshot_reader()
                end_book = owner_book_reader()
                self._require_matching_owner_cut(end_snapshot, end_book, namespace)
                cursor = journal.cursor(namespace)
                coverage = (
                    end_snapshot.get("coverage") if isinstance(end_snapshot, Mapping) else None
                )
                if (
                    not isinstance(coverage, Mapping)
                    or coverage.get("high_water_sequence") != cursor["event_cursor"]
                ):
                    continue
                snapshot_changed = journal.install_snapshot(
                    namespace,
                    end_snapshot,
                    owner_token=self._consumer_token,
                ) or snapshot_changed
                self._owner_position_reconciliation = journal.reconcile_owner_positions(
                    namespace, end_book
                )
                return {
                    "snapshot_changed": snapshot_changed,
                    "pages_applied": pages_applied,
                    "journal_generation": cursor["journal_generation"],
                    "scope_directory_revision": cursor["scope_directory_revision"],
                    "event_cursor": cursor["event_cursor"],
                    "owner_positions_reconciled": True,
                    "trade_projection_available": self._owner_position_reconciliation[
                        "trade_projection_available"
                    ],
                }
            except ServiceExecutionRecoveryError as error:
                raise ServiceRecoveryFlowError(f"recovery_{error.code.lower()}") from None
            except ServiceRecoveryFlowError:
                raise
            except Exception as error:  # noqa: BLE001 - no direct fallback
                raise ServiceRecoveryFlowError(self._read_failure_code(error)) from None
        raise ServiceRecoveryFlowError("recovery_owner_cut_not_stable")

    @staticmethod
    def _require_matching_owner_cut(
        recovery_snapshot: Any,
        owner_book: Mapping[str, Any],
        namespace: ServiceExecutionRecoveryNamespace,
    ) -> None:
        coverage = (
            recovery_snapshot.get("coverage") if isinstance(recovery_snapshot, Mapping) else None
        )
        if not isinstance(coverage, Mapping):
            raise ServiceRecoveryFlowError("recovery_snapshot_coverage_invalid")
        if (
            coverage.get("owner_kind") != "STRATEGY"
            or coverage.get("owner_ref") != namespace.strategy_ref
            or coverage.get("account_key") != owner_book.get("account_key")
            or coverage.get("journal_generation") != owner_book.get("journal_generation")
            or coverage.get("scope_directory_revision")
            != owner_book.get("scope_directory_revision")
            or coverage.get("high_water_sequence") != owner_book.get("current_execution_cursor")
            or owner_book.get("account_ref") != namespace.account_ref
            or owner_book.get("strategy_ref") != namespace.strategy_ref
        ):
            raise ServiceRecoveryFlowError("recovery_owner_cut_mismatch")

    def refresh_events(
        self,
        *,
        callback: Optional[Callable[[Any], Any]] = None,
        page_limit: int = 128,
        max_pages: int = 64,
        callback_limit: int = 32,
    ) -> Mapping[str, Any]:
        """Read committed event pages and then deliver pending callbacks.

        Each page and the resulting local cursor/projection/receipt/callback
        queue commit atomically before caller code runs. Callback completion is
        at-least-once across interruption; it is not an exactly-once promise.
        """

        journal, namespace = self._require_recovery()
        pages_applied = self.consume_pages(page_limit=page_limit, max_pages=max_pages)
        delivered = 0
        if callback is not None:
            try:
                delivered = journal.deliver_pending_callbacks(
                    namespace,
                    callback,
                    limit=callback_limit,
                    owner_token=self._consumer_token,
                )
            except ServiceExecutionRecoveryError as error:
                raise ServiceRecoveryFlowError(f"recovery_{error.code.lower()}") from None
        cursor = journal.cursor(namespace)
        return {
            "pages_applied": pages_applied,
            "callbacks_delivered": delivered,
            "journal_generation": cursor["journal_generation"],
            "scope_directory_revision": cursor["scope_directory_revision"],
            "event_cursor": cursor["event_cursor"],
        }

    def consume_pages(self, *, page_limit: int, max_pages: int) -> int:
        if type(page_limit) is not int or not 1 <= page_limit <= 128:
            raise ServiceRecoveryFlowArgumentError("invalid recovery page limit")
        if type(max_pages) is not int or not 1 <= max_pages <= 1024:
            raise ServiceRecoveryFlowArgumentError("invalid recovery page budget")
        journal, namespace = self._require_recovery()
        page_reader = getattr(self._client, "query_recovery_events_after", None)
        if not callable(page_reader):
            raise ServiceRecoveryFlowError("recovery_events_api_unavailable")
        pages_applied = 0
        for _ in range(max_pages):
            cursor = journal.cursor(namespace)
            generation = cursor["journal_generation"]
            directory_revision = cursor["scope_directory_revision"]
            after_sequence = cursor["event_cursor"]
            if (
                type(generation) is not str
                or not generation
                or type(directory_revision) is not str
                or not directory_revision
                or type(after_sequence) is not int
            ):
                raise ServiceRecoveryFlowError("recovery_snapshot_required")
            try:
                request = {
                    "after_sequence": after_sequence,
                    "expected_journal_generation": generation,
                    "expected_directory_revision": directory_revision,
                    "limit": page_limit,
                }
                if self._event_version == 2:
                    request["version"] = 2
                page = page_reader(**request)
                page_changed = journal.apply_events_page(
                    namespace,
                    page,
                    event_context_resolver=self._event_context_resolver,
                    owner_token=self._consumer_token,
                )
            except ServiceExecutionRecoveryError as error:
                raise ServiceRecoveryFlowError(f"recovery_{error.code.lower()}") from None
            except Exception as error:  # noqa: BLE001 - never downgrade to direct calls
                raise ServiceRecoveryFlowError(self._read_failure_code(error)) from None

            pages_applied += int(page_changed)
            coverage = page.get("coverage") if isinstance(page, Mapping) else None
            if not isinstance(coverage, Mapping) or type(coverage.get("has_more")) is not bool:
                raise ServiceRecoveryFlowError("recovery_event_page_invalid")
            latest = journal.cursor(namespace)
            if latest["event_cursor"] != coverage.get("scanned_through"):
                raise ServiceRecoveryFlowError("recovery_event_cursor_mismatch")
            if coverage["has_more"] is False:
                return pages_applied
        raise ServiceRecoveryFlowError("recovery_page_budget_exhausted")

    # -- durable reads --------------------------------------------------------

    def history(self) -> tuple[Mapping[str, Any], ...]:
        journal, namespace = self._require_recovery()
        return journal.ordered_event_history(namespace)

    def baseline(self) -> Optional[Mapping[str, Any]]:
        journal, namespace = self._require_recovery()
        return journal.recovery_baseline(namespace)

    def manifest(self, intent_id: str) -> Optional[Mapping[str, Any]]:
        journal, namespace = self._require_recovery()
        return journal.intent_manifest(namespace, intent_id)

    def intent_execution_context(self, intent_id: str) -> Optional[Mapping[str, Any]]:
        journal, namespace = self._require_recovery()
        return journal.intent_execution_context(namespace, intent_id)

    def claim_callback(self, receipt_id: str) -> Any:
        journal, namespace = self._require_recovery()
        return journal.claim_callback(
            namespace, receipt_id, owner_token=self._consumer_token
        )

    def acknowledge_callback(self, receipt_id: str) -> None:
        journal, namespace = self._require_recovery()
        journal.acknowledge_callback(
            namespace, receipt_id, owner_token=self._consumer_token
        )

    def fill_projection(self, receipt_id: str) -> Optional[Mapping[str, Any]]:
        journal, namespace = self._require_recovery()
        return journal.recovery_fill_projection(namespace, receipt_id)

    def persist_owner_projection(
        self,
        receipt_id: str,
        *,
        payload: Mapping[str, Any],
        strategy_position_key_ref: Optional[str] = None,
        strategy_position_owner_payload: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        journal, namespace = self._require_recovery()
        return journal.persist_recovery_owner_projection(
            namespace,
            receipt_id,
            payload=payload,
            strategy_position_key_ref=strategy_position_key_ref,
            strategy_position_owner_payload=strategy_position_owner_payload,
            owner_token=self._consumer_token,
        )

    def owner_projection(self, receipt_id: str) -> Optional[Mapping[str, Any]]:
        journal, namespace = self._require_recovery()
        return journal.recovery_owner_projection(namespace, receipt_id)

    def owner_projections(self) -> tuple[Mapping[str, Any], ...]:
        journal, namespace = self._require_recovery()
        return journal.recovery_owner_projections(namespace)

    def persist_trade_projection(
        self,
        receipt_id: str,
        *,
        data_name: str,
        tradeid: int,
        trade_state: Mapping[str, Any],
        trade_notifications: Any,
    ) -> Mapping[str, Any]:
        journal, namespace = self._require_recovery()
        return journal.persist_recovery_trade_projection(
            namespace,
            receipt_id,
            data_name=data_name,
            tradeid=tradeid,
            trade_state=trade_state,
            trade_notifications=trade_notifications,
            owner_token=self._consumer_token,
        )

    def trade_checkpoints(self) -> tuple[Mapping[str, Any], ...]:
        journal, namespace = self._require_recovery()
        return journal.recovery_trade_checkpoints(namespace)

    # -- durable admission ----------------------------------------------------

    def prepare_submit(
        self,
        *,
        intent_id: str,
        submit_payload: Mapping[str, Any],
        recovery_binding: Mapping[str, Any],
        strategy_position_owner_payload: Optional[Mapping[str, Any]],
        intent_context: Optional[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        """Commit one intent's durable facts before the service RPC.

        Returns the prior durable recovery state when this logical run already
        journaled the intent (the caller then restores instead of dispatching),
        and ``None`` once the intent is freshly journaled and dispatch-safe.
        The optional recovery context is a neutral mapping prepared by the
        framework before crossing this boundary. Existing manifests continue
        to restore their persisted context without rebuilding it.
        """

        journal, namespace = self._require_recovery()
        execution_context = None
        if self._event_version == 2:
            prior_manifest = journal.intent_manifest(namespace, intent_id)
            if prior_manifest is not None:
                execution_context = journal.intent_execution_context(namespace, intent_id)
                if execution_context is None:
                    raise ServiceExecutionRecoveryError("RECOVERY_INTENT_CONTEXT_UNAVAILABLE")
            else:
                if not isinstance(intent_context, Mapping):
                    raise ServiceExecutionRecoveryError("RECOVERY_INTENT_CONTEXT_UNAVAILABLE")
                execution_context = deepcopy(dict(intent_context))
        if self._require_owner_baseline:
            if self._owner_book_reader is None:
                raise ServiceExecutionRecoveryError("RECOVERY_BASELINE_REQUIRED")
            self.refresh_cut()
        # The manifest is durable before the RPC. If the caller loses the reply,
        # the same logical run can retry these exact facts.
        journal.prepare_intent(
            namespace,
            intent_id=intent_id,
            submit_payload=submit_payload,
            recovery_binding=recovery_binding,
            strategy_position_owner_payload=strategy_position_owner_payload,
            execution_context=execution_context,
            owner_token=self._consumer_token,
        )
        manifest = journal.intent_manifest(namespace, intent_id)
        if not isinstance(manifest, Mapping) or not isinstance(
            manifest.get("recovery_binding"), Mapping
        ):
            raise ServiceExecutionRecoveryError("RECOVERY_INTENT_BINDING_UNAVAILABLE")
        prior_state = journal.intent_recovery_state(namespace, intent_id)
        if prior_state is not None:
            return {
                "recovery_binding": manifest["recovery_binding"],
                "prior_state": prior_state,
            }
        if self._require_owner_baseline:
            journal.validate_recovery_dispatch_safety(
                namespace,
                submit_payload,
                manifest["recovery_binding"],
                self._owner_position_reconciliation,
            )
        return {
            "recovery_binding": manifest["recovery_binding"],
            "prior_state": None,
        }

    def strategy_position_ref_for_key(
        self, local_position_key_ref: str
    ) -> Optional[str]:
        journal, namespace = self._require_recovery()
        return journal.strategy_position_ref_for_key(namespace, local_position_key_ref)

    def strategy_position_owner_payload(
        self, strategy_position_ref: str
    ) -> Optional[Mapping[str, Any]]:
        journal, namespace = self._require_recovery()
        return journal.strategy_position_owner_payload(namespace, strategy_position_ref)

    def intent_submit_fingerprint(self, intent_id: str) -> Any:
        journal, namespace = self._require_recovery()
        return journal.intent_manifest(namespace, intent_id)["submit_fingerprint"]

    def prepare_cancel(
        self,
        *,
        cancel_id: str,
        target_intent_id: str,
        provider_order_id: str,
        metadata_version: str,
    ) -> None:
        journal, namespace = self._require_recovery()
        journal.prepare_cancel(
            namespace,
            cancel_id=cancel_id,
            target_intent_id=target_intent_id,
            provider_order_id=provider_order_id,
            metadata_version=metadata_version,
            owner_token=self._consumer_token,
        )


__all__ = [
    "ServiceRecoveryFlow",
    "ServiceRecoveryFlowArgumentError",
    "ServiceRecoveryFlowError",
]
