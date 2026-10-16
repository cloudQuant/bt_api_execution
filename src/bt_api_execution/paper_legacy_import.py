"""Typed, provenance-only import facts for a legacy paper ledger.

The legacy rows in this contract are historical application facts.  They are
never converted into provider observations or paper match receipts.  A target
paper owner is created only after the source snapshot reconciles completely
with an explicit opening baseline.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .contracts import ExecutionScope, canonical_json
from .errors import ContractValidationError
from .paper_execution import (
    PAPER_ENVIRONMENT,
    PAPER_PROVIDER,
    PaperOpeningPositionV1,
    PaperOwnerDomainV1,
)

_MAX_SOURCE_DOCUMENT_BYTES = 16 * 1024 * 1024
_TERMINAL_LEGACY_ORDER_STATES = frozenset({"filled", "cancelled", "rejected"})


def _identifier(value: Any, field: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > 256
        or not value.isprintable()
    ):
        raise ContractValidationError(f"invalid legacy paper {field}")
    return value


def _decimal(value: Any, field: str, *, minimum: Decimal | None = None) -> Decimal:
    if type(value) is bool or value is None:
        raise ContractValidationError(f"invalid legacy paper {field}")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ContractValidationError(f"invalid legacy paper {field}") from error
    if not result.is_finite() or (minimum is not None and result < minimum):
        raise ContractValidationError(f"invalid legacy paper {field}")
    return result


def _validate_import_profit_loss(account: Mapping[str, Any]) -> None:
    """Require a newly imported source row to reconcile its equity projection.

    The persisted V1 import receipt predates this stronger source-ingress
    invariant.  Keep decoding its historical finite P&L fields compatible,
    while requiring the reconciliation for all new imports and writes.
    """

    initial_cash = _decimal(account.get("initial_cash"), "initial cash", minimum=Decimal(0))
    total_equity = _decimal(account.get("total_equity"), "total equity")
    profit_loss = _decimal(account.get("profit_loss"), "profit and loss")
    profit_loss_pct = _decimal(account.get("profit_loss_pct"), "profit and loss percent")
    expected_profit_loss = total_equity - initial_cash
    pnl_tolerance = max(
        Decimal("1e-12"),
        max(abs(expected_profit_loss), abs(profit_loss)) * Decimal("1e-12"),
    )
    if abs(profit_loss - expected_profit_loss) > pnl_tolerance:
        raise ContractValidationError("legacy paper profit and loss does not reconcile")
    if initial_cash == 0:
        expected_profit_loss_pct = Decimal(0)
    else:
        expected_profit_loss_pct = (profit_loss / initial_cash) * Decimal(100)
    pct_tolerance = max(
        Decimal("1e-10"),
        max(abs(expected_profit_loss_pct), abs(profit_loss_pct)) * Decimal("1e-12"),
    )
    if abs(profit_loss_pct - expected_profit_loss_pct) > pct_tolerance:
        raise ContractValidationError("legacy paper profit and loss percent does not reconcile")


def _source_document(document_json: Any, expected_sha256: str) -> dict[str, Any]:
    if type(document_json) is not str:
        raise ContractValidationError("legacy paper source snapshot exceeds its bound")
    try:
        document_bytes = document_json.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ContractValidationError("legacy paper source snapshot is not valid UTF-8") from error
    if len(document_bytes) > _MAX_SOURCE_DOCUMENT_BYTES:
        raise ContractValidationError("legacy paper source snapshot exceeds its bound")
    try:
        document = json.loads(
            document_json,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("non-finite JSON")),
        )
    except (UnicodeError, ValueError, TypeError, json.JSONDecodeError) as error:
        raise ContractValidationError("legacy paper source snapshot is unreadable") from error
    if (
        type(document) is not dict
        or json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        != document_json
        or hashlib.sha256(document_bytes).hexdigest() != expected_sha256
        or set(document) != {"schema", "account", "orders", "positions", "trades"}
        or document.get("schema") != "afi.legacy-paper-inventory.v1"
        or type(document.get("account")) is not dict
        or any(type(document.get(name)) is not list for name in ("orders", "positions", "trades"))
    ):
        raise ContractValidationError("legacy paper source snapshot binding is invalid")
    return document


@dataclass(frozen=True, slots=True)
class LegacyPaperSymbolBindingV1:
    """Server-selected mapping from one legacy symbol to a managed spot instrument."""

    legacy_symbol: str
    target_instrument: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "legacy_symbol", _identifier(self.legacy_symbol, "symbol"))
        object.__setattr__(
            self, "target_instrument", _identifier(self.target_instrument, "target instrument")
        )

    def to_payload(self) -> dict[str, str]:
        return {
            "legacy_symbol": self.legacy_symbol,
            "target_instrument": self.target_instrument,
        }


@dataclass(frozen=True, slots=True)
class PaperLegacyImportV1:
    """A complete imported legacy snapshot plus its explicit target mapping.

    The source JSON is retained verbatim in canonical form.  The import is
    admissible only for terminal orders, fully linked trade history, spot-long
    holdings and exact cash/fee reconciliation.  Unsupported source facts are
    held in the legacy owner rather than silently dropped or synthesized.
    """

    source_account_id: str
    source_owner_id: str
    source_sha256: str
    source_document_json: str
    cash_currency: str
    symbol_bindings: tuple[LegacyPaperSymbolBindingV1, ...]
    unallocated_equity_residual: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "source_account_id", _identifier(self.source_account_id, "account id")
        )
        object.__setattr__(self, "source_owner_id", _identifier(self.source_owner_id, "owner id"))
        object.__setattr__(self, "cash_currency", _identifier(self.cash_currency, "cash currency"))
        if (
            type(self.source_sha256) is not str
            or len(self.source_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.source_sha256)
        ):
            raise ContractValidationError("invalid legacy paper source digest")
        residual = _decimal(self.unallocated_equity_residual, "unallocated equity residual")
        object.__setattr__(self, "unallocated_equity_residual", residual)
        document = _source_document(self.source_document_json, self.source_sha256)
        account = document["account"]
        if (
            account.get("id") != self.source_account_id
            or account.get("user_id") != self.source_owner_id
        ):
            raise ContractValidationError("legacy paper source owner binding changed")

        bindings = tuple(self.symbol_bindings)
        if any(type(item) is not LegacyPaperSymbolBindingV1 for item in bindings):
            raise ContractValidationError("legacy paper symbol bindings are invalid")
        if len({item.legacy_symbol for item in bindings}) != len(bindings):
            raise ContractValidationError("legacy paper symbol binding is duplicated")
        object.__setattr__(self, "symbol_bindings", bindings)
        self._validate_source_facts(document)

    @property
    def origin(self) -> str:
        return "imported_legacy"

    @property
    def opening_cash(self) -> Decimal:
        return _decimal(
            self._document()["account"].get("current_cash"), "opening cash", minimum=Decimal(0)
        )

    @property
    def cumulative_fees(self) -> Decimal:
        return sum(
            (
                _decimal(row.get("commission"), "trade commission", minimum=Decimal(0))
                for row in self._document()["trades"]
            ),
            Decimal(0),
        )

    @property
    def opening_positions(self) -> tuple[PaperOpeningPositionV1, ...]:
        document = self._document()
        bindings = {item.legacy_symbol: item.target_instrument for item in self.symbol_bindings}
        totals: dict[str, tuple[Decimal, Decimal]] = {}
        for row in document["positions"]:
            quantity = _decimal(row.get("size"), "position size")
            if quantity == 0:
                continue
            instrument = bindings.get(row.get("symbol"))
            if instrument is None:
                raise ContractValidationError(
                    "legacy paper position lacks a trusted symbol binding"
                )
            average_cost = _decimal(
                row.get("avg_price"), "position average cost", minimum=Decimal(0)
            )
            if quantity < 0 or average_cost <= 0:
                raise ContractValidationError("legacy paper position is not spot-long compatible")
            prior_quantity, prior_cost = totals.get(instrument, (Decimal(0), Decimal(0)))
            new_quantity = prior_quantity + quantity
            new_cost = (
                (prior_quantity * prior_cost + quantity * average_cost) / new_quantity
                if new_quantity
                else Decimal(0)
            )
            totals[instrument] = new_quantity, new_cost
        return tuple(
            PaperOpeningPositionV1(instrument, quantity, average_cost)
            for instrument, (quantity, average_cost) in sorted(totals.items())
        )

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(canonical_json(self.to_payload()).encode("utf-8")).hexdigest()

    def validate_domain(self, scope: ExecutionScope, domain: PaperOwnerDomainV1) -> None:
        if (
            type(scope) is not ExecutionScope
            or type(domain) is not PaperOwnerDomainV1
            or scope.provider != PAPER_PROVIDER
            or scope.environment != PAPER_ENVIRONMENT
            or scope.account_ref != domain.simulation_account_ref
            or scope.strategy_id != domain.strategy_ref
            or domain.cash_currency != self.cash_currency
            or domain.initial_cash != self.opening_cash
            or domain.initial_fees != self.cumulative_fees
            or domain.opening_positions != self.opening_positions
        ):
            raise ContractValidationError("legacy paper baseline differs from target owner domain")
        document = self._document()
        account = document["account"]
        legacy_fee_rate = _decimal(
            account.get("commission_rate"), "commission rate", minimum=Decimal(0)
        )
        legacy_slippage_rate = _decimal(
            account.get("slippage_rate"), "slippage rate", minimum=Decimal(0)
        )
        if domain.fee_rate != legacy_fee_rate or legacy_slippage_rate != 0:
            raise ContractValidationError("legacy paper fee or slippage model is unsupported")
        target_by_symbol = {
            item.legacy_symbol: item.target_instrument for item in self.symbol_bindings
        }
        for row in document["positions"]:
            quantity = _decimal(row.get("size"), "position size")
            if quantity == 0:
                continue
            target_instrument = target_by_symbol.get(row.get("symbol"))
            spec = domain.instrument(target_instrument) if target_instrument is not None else None
            if (
                spec is None
                or row.get("multiplier") is None
                or _decimal(row.get("multiplier"), "position multiplier") != 1
                or _decimal(row.get("margin_value"), "position margin") != 0
                or _decimal(row.get("margin_rate"), "position margin rate") != 1
            ):
                raise ContractValidationError("legacy paper position is not representable as spot")

    def validate_new_import(self) -> None:
        """Validate ingress-only invariants before persisting a new import."""

        _validate_import_profit_loss(self._document()["account"])

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "origin": self.origin,
            "source_account_id": self.source_account_id,
            "source_owner_id": self.source_owner_id,
            "source_sha256": self.source_sha256,
            "source_document_json": self.source_document_json,
            "cash_currency": self.cash_currency,
            "symbol_bindings": [item.to_payload() for item in self.symbol_bindings],
            "unallocated_equity_residual": format(self.unallocated_equity_residual, "f"),
            "opening_cash": format(self.opening_cash, "f"),
            "cumulative_fees": format(self.cumulative_fees, "f"),
            "opening_positions": [item.to_payload() for item in self.opening_positions],
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> PaperLegacyImportV1:
        expected = {
            "schema_version",
            "origin",
            "source_account_id",
            "source_owner_id",
            "source_sha256",
            "source_document_json",
            "cash_currency",
            "symbol_bindings",
            "unallocated_equity_residual",
            "opening_cash",
            "cumulative_fees",
            "opening_positions",
        }
        if (
            type(payload) is not dict
            or set(payload) != expected
            or type(payload.get("schema_version")) is not int
            or payload["schema_version"] != 1
            or payload.get("origin") != "imported_legacy"
            or type(payload.get("symbol_bindings")) is not list
        ):
            raise ContractValidationError("stored legacy paper import has an invalid shape")
        bindings = tuple(
            LegacyPaperSymbolBindingV1(
                legacy_symbol=row["legacy_symbol"], target_instrument=row["target_instrument"]
            )
            for row in payload["symbol_bindings"]
            if type(row) is dict and set(row) == {"legacy_symbol", "target_instrument"}
        )
        if len(bindings) != len(payload["symbol_bindings"]):
            raise ContractValidationError("stored legacy paper symbol bindings are invalid")
        imported = cls(
            source_account_id=payload["source_account_id"],
            source_owner_id=payload["source_owner_id"],
            source_sha256=payload["source_sha256"],
            source_document_json=payload["source_document_json"],
            cash_currency=payload["cash_currency"],
            symbol_bindings=bindings,
            unallocated_equity_residual=_decimal(
                payload["unallocated_equity_residual"], "unallocated equity residual"
            ),
        )
        opening_positions = payload.get("opening_positions")
        if (
            format(imported.opening_cash, "f") != payload["opening_cash"]
            or format(imported.cumulative_fees, "f") != payload["cumulative_fees"]
            or type(opening_positions) is not list
            or [item.to_payload() for item in imported.opening_positions] != opening_positions
        ):
            raise ContractValidationError("stored legacy paper baseline projection changed")
        return imported

    def _document(self) -> dict[str, Any]:
        return _source_document(self.source_document_json, self.source_sha256)

    def _validate_source_facts(self, document: dict[str, Any]) -> None:
        account = document["account"]
        if account.get("is_active") is not True:
            raise ContractValidationError("legacy paper account is not active")
        initial_cash = _decimal(account.get("initial_cash"), "initial cash", minimum=Decimal(0))
        current_cash = _decimal(account.get("current_cash"), "current cash", minimum=Decimal(0))
        _decimal(account.get("total_equity"), "total equity")
        _decimal(account.get("profit_loss"), "profit and loss")
        _decimal(account.get("profit_loss_pct"), "profit and loss percent")
        _decimal(account.get("commission_rate"), "commission rate", minimum=Decimal(0))
        if _decimal(account.get("slippage_rate"), "slippage rate", minimum=Decimal(0)) != 0:
            raise ContractValidationError("legacy paper slippage model is unsupported")

        row_ids: dict[str, dict[str, dict[str, Any]]] = {}
        for table_name in ("orders", "positions", "trades"):
            rows = document[table_name]
            ids: dict[str, dict[str, Any]] = {}
            for row in rows:
                if type(row) is not dict:
                    raise ContractValidationError("legacy paper source row is invalid")
                row_id = _identifier(row.get("id"), f"{table_name} row id")
                if row_id in ids or row.get("account_id") != self.source_account_id:
                    raise ContractValidationError("legacy paper source row identity is invalid")
                ids[row_id] = row
            row_ids[table_name] = ids

        orders = row_ids["orders"]
        order_trade_rows: dict[str, list[dict[str, Any]]] = {key: [] for key in orders}
        cash_delta = Decimal(0)
        fee_total = Decimal(0)
        for order in orders.values():
            status = order.get("status")
            if type(status) is not str or status.casefold() not in _TERMINAL_LEGACY_ORDER_STATES:
                raise ContractValidationError("legacy paper has an unresolved order")
            status = status.casefold()
            size = _decimal(order.get("size"), "order size", minimum=Decimal(0))
            filled_size = _decimal(order.get("filled_size"), "filled size", minimum=Decimal(0))
            order_commission = _decimal(
                order.get("commission"), "order commission", minimum=Decimal(0)
            )
            if status == "filled":
                if size <= 0 or filled_size != size:
                    raise ContractValidationError("legacy filled order is incomplete")
            elif filled_size != 0:
                raise ContractValidationError(
                    "legacy terminal order has an unsupported partial fill"
                )
            elif order_commission != 0:
                raise ContractValidationError("legacy non-filled order carries unexplained fees")

        for trade in row_ids["trades"].values():
            order_id = trade.get("order_id")
            if type(order_id) is not str or order_id not in orders:
                raise ContractValidationError("legacy paper trade has no source order")
            order = orders[order_id]
            if order.get("status", "").casefold() != "filled":
                raise ContractValidationError("legacy paper trade order is not fully filled")
            side = trade.get("side")
            if type(side) is not str or side.casefold() not in {"buy", "sell"}:
                raise ContractValidationError("legacy paper trade side is unsupported")
            if trade.get("symbol") != order.get("symbol"):
                raise ContractValidationError("legacy paper trade instrument changed")
            order_side = order.get("side")
            if (
                type(order_side) is not str
                or order_side.casefold() not in {"buy", "sell"}
                or side.casefold() != order_side.casefold()
            ):
                raise ContractValidationError("legacy paper trade side differs from order")
            quantity = _decimal(trade.get("size"), "trade size", minimum=Decimal(0))
            price = _decimal(trade.get("price"), "trade price", minimum=Decimal(0))
            commission = _decimal(trade.get("commission"), "trade commission", minimum=Decimal(0))
            if quantity <= 0 or price <= 0:
                raise ContractValidationError("legacy paper trade has an invalid size or price")
            order_trade_rows[order_id].append(trade)
            fee_total += commission
            notional = quantity * price
            cash_delta += (
                -(notional + commission) if side.casefold() == "buy" else notional - commission
            )

        for order_id, order in orders.items():
            trades = order_trade_rows[order_id]
            if order.get("status", "").casefold() == "filled":
                if not trades:
                    raise ContractValidationError("legacy filled order lacks imported trade facts")
                quantity = sum(
                    (_decimal(row.get("size"), "trade size") for row in trades), Decimal(0)
                )
                order_commission = _decimal(order.get("commission"), "order commission")
                trade_commission = sum(
                    (_decimal(row.get("commission"), "trade commission") for row in trades),
                    Decimal(0),
                )
                if (
                    quantity != _decimal(order.get("filled_size"), "filled size")
                    or order_commission != trade_commission
                ):
                    raise ContractValidationError("legacy order and trade totals do not reconcile")
                weighted_price = (
                    sum(
                        (
                            _decimal(row.get("size"), "trade size")
                            * _decimal(row.get("price"), "trade price")
                            for row in trades
                        ),
                        Decimal(0),
                    )
                    / quantity
                )
                if weighted_price != _decimal(order.get("avg_fill_price"), "average fill price"):
                    raise ContractValidationError("legacy order and trade prices do not reconcile")
            elif trades:
                raise ContractValidationError("legacy non-filled order has trade facts")

        if initial_cash + cash_delta != current_cash:
            raise ContractValidationError("legacy paper cash flows do not reconcile")

        position_bindings = {
            item.legacy_symbol: item.target_instrument for item in self.symbol_bindings
        }
        if len(position_bindings) != len(self.symbol_bindings):
            raise ContractValidationError("legacy paper symbol binding is duplicated")
        trade_quantities: dict[str, Decimal] = {}
        trade_costs: dict[str, Decimal] = {}
        for trade in sorted(
            row_ids["trades"].values(),
            key=lambda row: (str(row.get("created_at", "")), str(row.get("id", ""))),
        ):
            symbol = trade.get("symbol")
            if symbol not in position_bindings:
                raise ContractValidationError("legacy paper trade lacks a trusted symbol binding")
            instrument = position_bindings[symbol]
            quantity = _decimal(trade.get("size"), "trade size")
            price = _decimal(trade.get("price"), "trade price")
            side = trade.get("side", "").casefold()
            prior_quantity = trade_quantities.get(instrument, Decimal(0))
            prior_cost = trade_costs.get(instrument, Decimal(0))
            if side == "buy":
                next_quantity = prior_quantity + quantity
                trade_costs[instrument] = (
                    prior_quantity * prior_cost + quantity * price
                ) / next_quantity
                trade_quantities[instrument] = next_quantity
            else:
                if quantity > prior_quantity:
                    raise ContractValidationError(
                        "legacy paper trade history opens a short position"
                    )
                remaining = prior_quantity - quantity
                trade_quantities[instrument] = remaining
                trade_costs[instrument] = prior_cost if remaining else Decimal(0)

        source_positions: dict[str, tuple[Decimal, Decimal]] = {}
        for row in document["positions"]:
            quantity = _decimal(row.get("size"), "position size")
            symbol = row.get("symbol")
            if quantity == 0:
                continue
            if type(symbol) is not str or symbol not in position_bindings:
                raise ContractValidationError(
                    "legacy paper position lacks a trusted symbol binding"
                )
            if quantity < 0:
                raise ContractValidationError("legacy paper position is short and unsupported")
            instrument = position_bindings[symbol]
            average_cost = _decimal(
                row.get("avg_price"), "position average cost", minimum=Decimal(0)
            )
            if average_cost <= 0:
                raise ContractValidationError("legacy paper position has no average cost")
            prior_quantity, prior_cost = source_positions.get(instrument, (Decimal(0), Decimal(0)))
            total_quantity = prior_quantity + quantity
            source_positions[instrument] = (
                total_quantity,
                (prior_quantity * prior_cost + quantity * average_cost) / total_quantity,
            )
        if set(source_positions) != {
            instrument for instrument, quantity in trade_quantities.items() if quantity != 0
        }:
            raise ContractValidationError("legacy paper positions differ from imported trades")
        for instrument, (quantity, average_cost) in source_positions.items():
            if quantity != trade_quantities[instrument] or average_cost != trade_costs[instrument]:
                raise ContractValidationError("legacy paper position cost basis does not reconcile")

        marked_value = Decimal(0)
        for row in document["positions"]:
            marked_value += _decimal(row.get("market_value"), "position market value")
        residual = (
            _decimal(account.get("total_equity"), "total equity") - current_cash - marked_value
        )
        if residual != self.unallocated_equity_residual:
            raise ContractValidationError("legacy paper unallocated equity residual changed")

    @classmethod
    def from_source_document(
        cls,
        *,
        source_account_id: str,
        source_owner_id: str,
        source_document: Mapping[str, Any],
        cash_currency: str,
        symbol_bindings: tuple[LegacyPaperSymbolBindingV1, ...],
    ) -> PaperLegacyImportV1:
        if not isinstance(source_document, Mapping):
            raise ContractValidationError("legacy paper source document must be a mapping")
        try:
            document_json = json.dumps(
                dict(source_document),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
        except (TypeError, ValueError, UnicodeError) as error:
            raise ContractValidationError(
                "legacy paper source document is not canonical JSON"
            ) from error
        source_sha256 = hashlib.sha256(document_json.encode("utf-8")).hexdigest()
        document = _source_document(document_json, source_sha256)
        account = document["account"]
        residual = (
            _decimal(account.get("total_equity"), "total equity")
            - _decimal(account.get("current_cash"), "current cash")
            - sum(
                (
                    _decimal(row.get("market_value"), "position market value")
                    for row in document["positions"]
                ),
                Decimal(0),
            )
        )
        imported = cls(
            source_account_id=source_account_id,
            source_owner_id=source_owner_id,
            source_sha256=source_sha256,
            source_document_json=document_json,
            cash_currency=cash_currency,
            symbol_bindings=symbol_bindings,
            unallocated_equity_residual=residual,
        )
        imported.validate_new_import()
        return imported


@dataclass(frozen=True, slots=True)
class PaperLegacyImportReceiptV1:
    """Read-only target-side receipt for a prepared or activated legacy import."""

    state: str
    scope_key: str
    account_key: str
    import_sha256: str
    domain_sha256: str
    imported: PaperLegacyImportV1


@dataclass(frozen=True, slots=True)
class PaperLegacyImportSourceReceiptV1:
    """Safe source-keyed projection for cross-store handoff recovery."""

    source_account_id: str
    source_owner_id: str
    source_sha256: str
    state: str
    scope_key: str
    account_key: str
    import_sha256: str
    domain_sha256: str
    unallocated_equity_residual: Decimal


@dataclass(frozen=True, slots=True)
class PaperLegacyImportAccountStatusV1:
    """Read-only account-wide owner/import facts needed before source unfreeze."""

    source_receipt: PaperLegacyImportSourceReceiptV1 | None
    active_account_import_state: str | None
    owner_domain_exists: bool


__all__ = (
    "LegacyPaperSymbolBindingV1",
    "PaperLegacyImportAccountStatusV1",
    "PaperLegacyImportReceiptV1",
    "PaperLegacyImportSourceReceiptV1",
    "PaperLegacyImportV1",
)
