"""Lazy compatibility import for the provider-owned CTP integration."""

from importlib import import_module

__all__ = (
    "CtpLifecycleBoundNativeCallbackEnvelope",
    "CtpNativeCallbackSourceBridge",
    "ctp_native_callback_source_facts",
)


def __getattr__(name):
    if name.startswith("__"):
        raise AttributeError(name)
    return getattr(import_module("bt_api_ctp.ctp_callback_source_bridge"), name)
