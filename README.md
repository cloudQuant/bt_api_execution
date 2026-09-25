# bt_api_execution

`bt_api_execution` is the provider-neutral execution boundary for managed
routes.  It deliberately contains no exchange SDK, HTTP client, socket, or
credential loader.  A caller injects a provider port only after the runtime
has completed its own configuration, admission, and preflight checks.

The first public release supplies a small, durable execution spine:

- immutable, Decimal-safe contracts for intents, plans, children, allocations,
  account snapshots, quality records, and strategy checkpoints;
- SQLite `FULL`-synchronous intent journal and replayable outbox;
- the same SQLite authority's account-wide, non-reusing CTP OrderRef mapping;
- a scoped writer lease and fencing token;
- intent-before-dispatch, idempotency conflict detection, explicit unknown
  outcomes, and evidence-based reconciliation; and
- an optional duck-typed admission gate.  The package does not define a second
  risk permit type; deployments inject an adapter for the shared risk contract.

The facade is intentionally fail-closed.  It needs an admission gate unless a
test explicitly enables `allow_unprotected=True`; that test-only escape does
not represent an approved managed provider route.  A managed adapter supplies
`reserve(intent) -> permit`, `validate(permit_reference, intent)`,
`settle(permit_reference)`, and `release(permit_reference, reason)`.  The
facade performs `validate` after its durable dispatch claim and immediately
before calling the provider port; a failed or missing validator causes a
durable block and zero provider calls.

`SharedRiskAdmissionAdapter` is available for a separately installed shared
risk gate that exposes `reserve`, `validate_permit`, `settle`, and `release`.
The SDK composition root supplies the `OrderIntent -> RiskIntent` mapper, so
this package does not import or duplicate the risk package's account, action,
or notional DTOs.

A composition may supply `pre_dispatch_guard=` to `ManagedExecutionFacade`.
The facade invokes this constructor-level guard for every `submit`, after its
durable claim and final admission validation but before the injected provider
port.  A guard failure durably blocks the claim and calls no provider.  A
per-call `before_dispatch=` guard can add a narrower caller check, but cannot
replace the composition guard.

`record_provider_observation()` persists normalized, already-obtained evidence
for `UNKNOWN`, `DISPATCHING`, `ACKED`, or `PARTIALLY_FILLED` records without
performing provider I/O.  It advances only through the closed state machine and
settles an admission permit only for the first evidenced outcome, avoiding a
second settlement when an acknowledged order later partially or fully fills.

## Install

```bash
python -m pip install ./bt_api_execution
```

## Minimal local example

```python
from decimal import Decimal
from pathlib import Path

from bt_api_execution import (
    ExecutionScope,
    ManagedExecutionFacade,
    OrderIntent,
    ProviderObservation,
    Side,
    SqliteExecutionStore,
)


scope = ExecutionScope("FAKE", "simulation", "acct_ref", "strategy.demo")
store = SqliteExecutionStore(Path("execution.sqlite3"))
execution = ManagedExecutionFacade(
    store,
    scope,
    writer_id="local-test-writer",
    allow_unprotected=True,
)
intent = OrderIntent.limit(
    intent_id="signal-42",
    scope=scope,
    signal_id="signal-42",
    instrument="BTC-USDT",
    side=Side.BUY,
    quantity=Decimal("0.01"),
    price=Decimal("50000"),
)

record = execution.submit(
    intent,
    lambda submitted: ProviderObservation.accepted(submitted.intent_id, "fake-order-1"),
)
assert record.state.value == "ACKED"
```

`ProviderObservation` is a normalized, non-secret result.  If the injected
port raises after a request may have been sent, the record becomes `UNKNOWN`.
The facade will never replay it automatically; a caller must reconcile it with
provider evidence.

## Scope of this release

This package is an execution foundation, not a provider adapter, risk engine,
or monitor.  Cross-package atomic reservation/journal commits, provider
capability receipts, aggregation policy, cancellation action state, and SDK
migration are deliberately left to their owning integration work packages.
The exposed contracts and outbox are designed to make those additions explicit
rather than silently falling back to an in-memory or direct route.

`reserve_ctp_order_identity()` binds a CTP execution scope, managed intent ID,
and stable runtime order ID to one account-wide 12-digit ASCII OrderRef in the
same SQLite database. It requires an explicit `YYYYMMDD` trading day, survives
restart, and never reuses a committed reference across strategies or trading
days for the same account. This primitive does not migrate the existing SDK or
Backtrader journals, dispatch an order, or authorize provider I/O; a managed
route must complete the separately reviewed single-authority cutover first.

The v6 CTP command outbox keeps that boundary closed while making its local
claim contract explicit. `claim_ctp_dispatch_command()` requires an injected
fresh action/source verifier, binds its result to the complete immutable
submit or cancel request, and records the one-use approval consumption in the
same SQLite transaction as `READY -> CLAIMED`. The command's approval digest
and any later receipt digest are evidence echoes only. This package supplies
the verifier interface and fake-only tests, not a deployed verifier, external
account-wide writer fence, native callback reconciliation, native SDK import,
or provider write route.
