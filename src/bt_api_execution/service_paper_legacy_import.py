"""Host-only RPC adapter for the typed legacy Paper import handoff.

The adapter borrows the Host-owned Execution store and trusts only the fixed
WireScope-to-ExecutionScope mapping supplied by the composition root.  Its
wire arguments contain legacy source facts, never a target path, store handle,
execution scope, or claim that imported history is exchange-generated.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from bt_api_base.service_protocol import WireScope

from .contracts import ExecutionScope
from .errors import (
    ContractValidationError,
    DurableStoreError,
    PaperImportResidualUnrepresentableError,
    WriterLeaseUnavailable,
)
from .paper_execution import PaperOwnerDomainV1
from .paper_legacy_import import (
    PaperLegacyImportAccountStatusV1,
    PaperLegacyImportReceiptV1,
    PaperLegacyImportV1,
)
from .store import SqliteExecutionStore, WriterLease

PORT = "service.paper_legacy_import"
STATUS_OPERATION = "paper.import.status"
INSPECT_SOURCE_OPERATION = "paper.import.inspect_source"
PREPARE_OPERATION = "paper.import.prepare"
ACTIVATE_OPERATION = "paper.import.activate"
ABORT_OPERATION = "paper.import.abort"
SUPPORTED_OPERATIONS = frozenset(
    {
        STATUS_OPERATION,
        INSPECT_SOURCE_OPERATION,
        PREPARE_OPERATION,
        ACTIVATE_OPERATION,
        ABORT_OPERATION,
    }
)
WRITE_OPERATIONS = frozenset({PREPARE_OPERATION, ACTIVATE_OPERATION, ABORT_OPERATION})


def validate_service_options(
    options: Mapping[str, object], bind_ports: tuple[str, ...]
) -> Mapping[str, object]:
    """Validate the closed, single-port import-only component role."""

    if (
        not isinstance(options, Mapping)
        or set(options) != {"role"}
        or options.get("role") != "legacy_import_owner"
        or type(bind_ports) is not tuple
        or bind_ports != (PORT,)
    ):
        raise ContractValidationError("invalid legacy import service configuration")
    return MappingProxyType({"role": "legacy_import_owner"})


def _reply(ok: bool, code: str, result: Mapping[str, object]) -> Mapping[str, Any]:
    return {"ok": ok, "code": code, "result": dict(result)}


def _arguments(arguments: Mapping[str, object], expected: frozenset[str]) -> bool:
    return isinstance(arguments, Mapping) and type(arguments) is dict and set(arguments) == expected


def _receipt_projection(receipt: PaperLegacyImportReceiptV1) -> dict[str, object]:
    imported = receipt.imported
    return {
        "state": receipt.state,
        "scope_key": receipt.scope_key,
        "account_key": receipt.account_key,
        "origin": imported.origin,
        "source_account_id": imported.source_account_id,
        "source_owner_id": imported.source_owner_id,
        "source_sha256": imported.source_sha256,
        "unallocated_equity_residual": format(imported.unallocated_equity_residual, "f"),
        "import_sha256": receipt.import_sha256,
        "domain_sha256": receipt.domain_sha256,
    }


def _account_status_projection(status: PaperLegacyImportAccountStatusV1) -> dict[str, object]:
    source = status.source_receipt
    source_projection: dict[str, object] | None = None
    if source is not None:
        source_projection = {
            "source_account_id": source.source_account_id,
            "source_owner_id": source.source_owner_id,
            "source_sha256": source.source_sha256,
            "state": source.state,
            "scope_key": source.scope_key,
            "account_key": source.account_key,
            "unallocated_equity_residual": format(
                source.unallocated_equity_residual, "f"
            ),
            "import_sha256": source.import_sha256,
            "domain_sha256": source.domain_sha256,
        }
    return {
        "source_receipt": source_projection,
        "active_account_import_state": status.active_account_import_state,
        "owner_domain_exists": status.owner_domain_exists,
    }


class PaperLegacyImportService:
    """Serve one fixed paper target through the real ServiceHost registry."""

    SUPPORTED_OPERATIONS = SUPPORTED_OPERATIONS
    WRITE_OPERATIONS = WRITE_OPERATIONS

    def __init__(
        self,
        store: SqliteExecutionStore,
        targets: Mapping[WireScope, ExecutionScope],
        domain: PaperOwnerDomainV1,
        *,
        writer_id: str,
        lease_ttl_ns: int = 30_000_000_000,
    ) -> None:
        if type(store) is not SqliteExecutionStore:
            raise ContractValidationError("legacy import requires a Host-owned Execution store")
        if type(domain) is not PaperOwnerDomainV1:
            raise ContractValidationError("legacy import requires a trusted Paper owner domain")
        if (
            type(writer_id) is not str
            or not writer_id
            or writer_id != writer_id.strip()
            or len(writer_id) > 128
        ):
            raise ContractValidationError("legacy import writer identity is invalid")
        if type(lease_ttl_ns) is not int or not 1_000_000 <= lease_ttl_ns <= 300_000_000_000:
            raise ContractValidationError("legacy import lease duration is invalid")
        if not isinstance(targets, Mapping) or len(targets) != 1:
            raise ContractValidationError("legacy import requires one trusted target mapping")
        copied: dict[WireScope, ExecutionScope] = {}
        for wire_scope, execution_scope in targets.items():
            if type(wire_scope) is not WireScope or type(execution_scope) is not ExecutionScope:
                raise ContractValidationError("legacy import target mapping is not typed")
            domain.validate_binding(wire_scope, execution_scope)
            copied[wire_scope] = execution_scope
        self._store = store
        self._targets = MappingProxyType(copied)
        self._domain = domain
        self._writer_id = writer_id
        self._lease_ttl_ns = lease_ttl_ns

    @property
    def domain(self) -> PaperOwnerDomainV1:
        return self._domain

    @property
    def targets(self) -> Mapping[WireScope, ExecutionScope]:
        return self._targets

    @property
    def supported_operations(self) -> frozenset[str]:
        return self.SUPPORTED_OPERATIONS

    def handle(
        self,
        operation: str,
        *,
        scope: WireScope,
        arguments: Mapping[str, object],
    ) -> Mapping[str, Any]:
        if type(operation) is not str or operation not in self.SUPPORTED_OPERATIONS:
            return _reply(False, "UNSUPPORTED_OPERATION", {})
        if type(scope) is not WireScope:
            return _reply(False, "SCOPE_DENIED", {})
        execution_scope = self._targets.get(scope)
        if execution_scope is None:
            return _reply(False, "SCOPE_DENIED", {})

        if operation == STATUS_OPERATION:
            if not _arguments(arguments, frozenset()):
                return _reply(False, "INVALID_ARGUMENTS", {})
            try:
                receipt = self._store.get_paper_legacy_import(execution_scope)
            except Exception:
                return _reply(False, "IMPORT_STORE_UNAVAILABLE", {})
            return _reply(
                True,
                "OK",
                {"found": receipt is not None, "receipt": None if receipt is None else _receipt_projection(receipt)},
            )

        if operation == INSPECT_SOURCE_OPERATION:
            if not _arguments(arguments, frozenset({"source_account_id"})):
                return _reply(False, "INVALID_ARGUMENTS", {})
            source_account_id = arguments.get("source_account_id")
            if (
                type(source_account_id) is not str
                or not source_account_id
                or source_account_id != source_account_id.strip()
                or len(source_account_id) > 256
                or not source_account_id.isprintable()
            ):
                return _reply(False, "INVALID_ARGUMENTS", {})
            try:
                status = self._store.inspect_paper_legacy_import_account(
                    execution_scope, source_account_id=source_account_id
                )
            except Exception:
                return _reply(False, "IMPORT_STORE_UNAVAILABLE", {})
            return _reply(True, "OK", _account_status_projection(status))

        if operation == PREPARE_OPERATION:
            if not _arguments(arguments, frozenset({"imported"})):
                return _reply(False, "INVALID_ARGUMENTS", {})
            try:
                imported = PaperLegacyImportV1.from_payload(arguments["imported"])
                imported.validate_new_import()
                imported.validate_domain(execution_scope, self._domain)
            except Exception:
                return _reply(False, "IMPORT_INVALID", {})
            return self._write(
                execution_scope,
                lambda lease: _receipt_projection(
                    self._store.prepare_paper_legacy_import(
                        execution_scope,
                        self._domain,
                        imported,
                        writer_lease=lease,
                    )
                ),
            )

        if operation == ACTIVATE_OPERATION:
            if not _arguments(arguments, frozenset()):
                return _reply(False, "INVALID_ARGUMENTS", {})
            return self._write(
                execution_scope,
                lambda lease: _receipt_projection(
                    self._store.activate_paper_legacy_import(execution_scope, writer_lease=lease)
                ),
            )

        if operation == ABORT_OPERATION:
            if not _arguments(arguments, frozenset({"source_sha256"})):
                return _reply(False, "INVALID_ARGUMENTS", {})
            source_sha256 = arguments.get("source_sha256")
            if (
                type(source_sha256) is not str
                or len(source_sha256) != 64
                or any(character not in "0123456789abcdef" for character in source_sha256)
            ):
                return _reply(False, "INVALID_ARGUMENTS", {})
            return self._write(
                execution_scope,
                lambda lease: _receipt_projection(
                    self._store.abort_paper_legacy_import(
                        execution_scope,
                        source_sha256=source_sha256,
                        writer_lease=lease,
                    )
                ),
            )
        return _reply(False, "UNSUPPORTED_OPERATION", {})

    def _write(self, scope: ExecutionScope, action) -> Mapping[str, Any]:
        try:
            lease = self._store.acquire_or_renew_lease(
                scope,
                self._writer_id,
                ttl_ns=self._lease_ttl_ns,
            )
        except WriterLeaseUnavailable:
            return _reply(False, "IMPORT_OWNER_BUSY", {})
        except Exception:
            return _reply(False, "IMPORT_STORE_UNAVAILABLE", {})
        if type(lease) is not WriterLease:
            return _reply(False, "IMPORT_STORE_UNAVAILABLE", {})

        value: Mapping[str, object] | None = None
        error_code: str | None = None
        try:
            value = action(lease)
        except ContractValidationError:
            error_code = "IMPORT_INVALID"
        except PaperImportResidualUnrepresentableError:
            error_code = "IMPORT_RESIDUAL_UNREPRESENTABLE"
        except DurableStoreError:
            error_code = "IMPORT_STATE_CONFLICT"
        except Exception:
            error_code = "IMPORT_STORE_UNAVAILABLE"
        try:
            released = self._store.release_lease(
                scope,
                self._writer_id,
                fencing_token=lease.fencing_token,
            )
        except Exception:
            released = False
        if released is not True:
            return _reply(False, "IMPORT_WRITE_UNCERTAIN", {})
        if error_code is not None:
            return _reply(False, error_code, {})
        if not isinstance(value, Mapping):
            return _reply(False, "IMPORT_STORE_UNAVAILABLE", {})
        return _reply(True, "OK", value)


__all__ = [
    "ABORT_OPERATION",
    "ACTIVATE_OPERATION",
    "INSPECT_SOURCE_OPERATION",
    "PaperLegacyImportService",
    "PORT",
    "PREPARE_OPERATION",
    "STATUS_OPERATION",
    "SUPPORTED_OPERATIONS",
    "WRITE_OPERATIONS",
    "validate_service_options",
]
