"""Lazy compatibility import for the provider-owned CTP integration."""

from importlib import import_module

__all__ = ("CtpNativeCallbackLedgerAdapter",)


def __getattr__(name):
    if name.startswith("__"):
        raise AttributeError(name)
    return getattr(import_module("bt_api_ctp.ctp_callback_ledger_adapter"), name)
