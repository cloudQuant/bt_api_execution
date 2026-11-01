"""Project service owner-book snapshots for recovery journal inputs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def owner_book_payload(snapshot: Any, *, error_type: type[Exception]) -> dict[str, Any]:
    """Convert the immutable public book DTO to the journal's strict payload."""

    if isinstance(snapshot, Mapping):
        values = dict(snapshot)
    else:
        names = (
            "account_key",
            "account_ref",
            "strategy_ref",
            "scope_directory_revision",
            "journal_generation",
            "baseline_execution_cursor",
            "current_execution_cursor",
            "positions",
            "cash_by_currency",
            "position_costs",
            "financial_available",
            "cost_basis_complete",
        )
        try:
            values = {name: getattr(snapshot, name) for name in names}
        except Exception as error:  # noqa: BLE001 - strict recovery input
            raise error_type("recovery_owner_book_invalid") from error
    costs = values.get("position_costs")
    if isinstance(costs, Mapping):
        values["position_costs"] = {
            instrument: (
                dict(row)
                if isinstance(row, Mapping)
                else {
                    name: getattr(row, name, None)
                    for name in (
                        "quantity",
                        "cost_basis",
                        "cost_currency",
                        "product_model",
                        "metadata_version",
                        "average_cost",
                    )
                }
            )
            for instrument, row in costs.items()
        }
    return values


__all__ = ("owner_book_payload",)
