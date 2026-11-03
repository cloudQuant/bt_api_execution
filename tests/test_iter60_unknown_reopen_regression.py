"""Current optional-Execution recovery contract with a local fake provider.

These tests do not exercise the deleted legacy migration or Paper fixtures.
They check that the current journal never resubmits an ambiguous intent when
its SQLite store and facade are reopened.
"""

from decimal import Decimal

import pytest

from bt_api_execution import (
    ExecutionScope,
    ExecutionState,
    ManagedExecutionFacade,
    OrderIntent,
    Side,
    SqliteExecutionStore,
)


def _intent():
    return OrderIntent.limit(
        intent_id="iter60-unknown-reopen",
        scope=ExecutionScope("FAKE", "simulation", "account-a", "strategy-a", "20261103"),
        signal_id="iter60-signal",
        instrument="SIM:SA2701",
        side=Side.BUY,
        quantity=Decimal("2"),
        price=Decimal("1500"),
        metadata_version="synthetic-terms-v1",
    )


@pytest.mark.parametrize("interrupted", [False, True], ids=["timeout", "interrupted-claim"])
def test_ambiguous_dispatch_remains_unknown_after_store_reopen(tmp_path, interrupted):
    """A lost response or interrupted dispatch never makes a second call."""
    path = tmp_path / "execution.sqlite"
    intent = _intent()
    calls = []

    def provider(submitted):
        calls.append(submitted.intent_id)
        if interrupted:
            raise KeyboardInterrupt("simulated interruption")
        raise TimeoutError("simulated lost response")

    store = SqliteExecutionStore(path)
    facade = ManagedExecutionFacade(
        store, intent.scope, writer_id="writer-one", allow_unprotected=True
    )
    try:
        if interrupted:
            with pytest.raises(KeyboardInterrupt):
                facade.submit(intent, provider)
            assert (
                store.get(intent.intent_id, scope=intent.scope).state is ExecutionState.DISPATCHING
            )
        else:
            assert facade.submit(intent, provider).state is ExecutionState.UNKNOWN
    finally:
        facade.close()
        store.close()

    reopened = SqliteExecutionStore(path)
    replacement = ManagedExecutionFacade(
        reopened, intent.scope, writer_id="writer-two", allow_unprotected=True
    )
    try:
        replacement.recover_interrupted_dispatches()
        record = reopened.get(intent.intent_id, scope=intent.scope)
        assert record.state is ExecutionState.UNKNOWN
        assert replacement.submit(intent, provider).state is ExecutionState.UNKNOWN
        assert calls == [intent.intent_id]
    finally:
        replacement.close()
        reopened.close()
