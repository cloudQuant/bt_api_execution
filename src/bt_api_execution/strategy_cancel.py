"""Typed request helpers for exact-run strategy cancellation.

The owner batch operation accepts only a command identifier.  Strategy, run,
account and service identity are supplied by the authenticated server context;
they are never read from the command payload.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass

from .contracts import canonical_json
from .errors import ContractValidationError

OWNER_CANCEL_OPERATION = "owner.cancel"
OWNER_CANCEL_MAX_ITEMS = 128
OWNER_CANCEL_MAX_SCAN = 10_000
_COMMAND_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class OwnerCancelSelectionLimitError(Exception):
    """The exact run has more attributed order rows than one bounded scan allows."""


@dataclass(frozen=True, slots=True)
class OwnerCancelCommandV1:
    """One idempotent request to cancel open orders for one exact strategy run."""

    command_id: str

    def __post_init__(self) -> None:
        if type(self.command_id) is not str or not _COMMAND_ID.fullmatch(self.command_id):
            raise ContractValidationError("invalid owner cancel command_id")

    @classmethod
    def from_arguments(cls, arguments: Mapping[str, object]) -> OwnerCancelCommandV1:
        if not isinstance(arguments, Mapping) or any(
            type(key) is not str for key in arguments
        ):
            raise ContractValidationError("invalid owner cancel arguments")
        if set(arguments) != {"command_id"}:
            raise ContractValidationError("owner cancel requires only command_id")
        return cls(arguments["command_id"])


def owner_cancel_batch_key(
    *, scope_key: str, wire_scope: Mapping[str, object], command_id: str
) -> str:
    """Return a bounded durable idempotency key for the exact owner scope."""

    payload = canonical_json([scope_key, wire_scope, command_id])
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"owner_cancel_batch.v1:{digest}"


def owner_cancel_id(*, command_id: str, intent_id: str) -> str:
    """Derive a deterministic per-order cancel id without exposing delimiters."""

    payload = canonical_json(["owner.cancel.v1", command_id, intent_id])
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"owner-cancel-v1:{digest}"


__all__ = [
    "OWNER_CANCEL_MAX_ITEMS",
    "OWNER_CANCEL_MAX_SCAN",
    "OWNER_CANCEL_OPERATION",
    "OwnerCancelCommandV1",
    "OwnerCancelSelectionLimitError",
    "owner_cancel_batch_key",
    "owner_cancel_id",
]
