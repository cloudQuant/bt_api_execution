# CTP command callback and UNKNOWN reconciliation boundary

**Status:** design-only contract; no CTP callback adapter or UNKNOWN resolver is
implemented by this package.

## Current facts

`ctp_dispatch_commands` is a local command outbox. `complete_ctp_dispatch_command()`
stores a typed native-call receipt, and `COMPLETED` means only that this local
receipt step finished. A `QUEUED` result means the SDK accepted a local queue
request; it is not a provider order acknowledgement, fill, cancel acknowledgement,
or terminal order state. The generic `execution_records` and
`cancellation_records` tables are not joined to CTP command IDs today.

On restart, `recover_claimed_ctp_dispatch_commands()` changes an interrupted
`CLAIMED` command to `UNKNOWN` and does not replay it. An account with a
`CLAIMED` or `UNKNOWN` command remains fenced. There is no API that resolves or
clears CTP command `UNKNOWN`, and this ADR does not add one.

This account fence is local to participants that use the same SQLite database
and lease contract. It cannot exclude another database, an uncoordinated
process, a manual trading client, or another native/provider writer. A real
account-wide writer exclusion mechanism remains an external blocker.

## Correlation keys required before a durable projection

Every normalized callback envelope must bind the immutable local action:

- `account_key`, `trading_day`, `scope_key`, `operation`, `command_id`, request
  payload digest, approval-use ID, and session-binding digest;
- a non-reused native session generation, callback stream, stable event ID or
  source cursor, and a digest of the exact callback evidence; and
- exact native identities, with absent or ambiguous fields rejected.

For **submit**, the minimum pre-acceptance correlation is the account and
trading day, reserved `OrderRef`, and exact TD `FrontID`/`SessionID` within one
session generation. The native `RequestID` must also be persisted and matched if
the SDK exposes it consistently through insert callbacks. Once the venue order
exists, exact `ExchangeID` and `OrderSysID` must be bound to that same
`OrderRef`. The current command schema has no typed `RequestID` or callback
cursor, and the package has not verified which callback families echo these
values. Do not infer a missing identifier from a queue receipt.

For **cancel**, the immutable target is already bound as
`OrderRef`/`ExchangeID`/`OrderSysID`/`FrontID`/`SessionID`. A cancel action also
needs its own native `ActionRef` and/or `RequestID`, exact operation and session
generation, plus the exact target echo. The current schema does not persist a
typed `ActionRef` or `RequestID`; raw receipt payload is not a correlation
contract. The SDK callback mapping for these fields is unverified. If the native
response does not identify exactly one cancel action, it must not be attached
to a command by target identity or arrival time alone.

`FrontID`/`SessionID` can be reused by a later login. The adapter must scope
callbacks to a durable, non-reused session generation and prove that a stopped
client has drained before accepting events from a replacement. A callback
without that provenance, or from an old/unknown generation, is not projected.
The native callback itself may not carry the generation; if so, only the
client-owned wrapper may attach it after proving callback ownership.

The future provider-neutral seams should look like this; these names are
illustrative and are not exported APIs yet:

```python
class CtpDispatchCallbackVerifier(Protocol):
    def verify_callback(
        self, command: CtpDispatchCommand, callback: NormalizedCtpCallback, *,
        session_generation: str,
    ) -> VerifiedCtpCallback: ...

class CtpDispatchUnknownRecoveryVerifier(Protocol):
    def verify_recovery(
        self, command: CtpDispatchCommand, evidence: CtpRecoveryEvidenceBundle, *,
        now_ns: int,
    ) -> VerifiedCtpRecovery | None: ...  # None means abstain; UNKNOWN stays fenced.
```

`VerifiedCtpCallback` must carry the command-binding digest, stable source
event/cursor, event family, native session generation, verifier identity, and
source digest. `CtpRecoveryEvidenceBundle` must keep order, trade, position,
and cancel-action evidence separate, each with exact filter and completeness
facts, while binding them to one reviewed snapshot/revision. A decision object
must bind the exact command/action and all evidence digests; no receipt digest
is a substitute. Missing fields or verifier errors deny projection/update.

## Projection split

When the native contract and artifact are reviewed, use separate append-only
ledgers for provider order/trade observations and cancel-action observations.
Persist the normalized event and advance its projection in one transaction,
deduplicated by a stable source-generation/stream/event identity.

- Submit order state advances only from exact provider order and trade
  observations. A successful local queue receipt creates no order ACK.
- A cancel-action response such as accepted or rejected belongs to that cancel
  action. An accepted cancel request does not prove that the target order is
  cancelled. Only exact terminal order evidence, with matching fills, can move
  the target order projection to cancelled.
- Local outbox receipt status, provider order state, and cancel-action state
  remain distinct. `COMPLETED` is never interpreted as provider terminal.

## UNKNOWN recovery contract

A future resolver must consume a verifier-produced evidence bundle bound to
the exact command and session generation. The verifier must establish exact
query filters and complete readbacks for orders, trades, positions, and cancel
action history, plus a consistent snapshot/revision or another reviewed proof
that the observations can be reconciled together. It must return an explicit
decision or abstain; absent, incomplete, evicted, stale, mismatched, or
cross-generation evidence leaves the command `UNKNOWN` and keeps the
account-wide fence.

The existing native query adapter issues separate queries, which do not prove
one atomic account snapshot, and the SDK has no accepted durable post-restart
cancel-action history. Therefore no positive UNKNOWN-resolution path is
currently implementable from the accepted evidence. Cold start without the
callback history needed to prove the action remains UNKNOWN. A queue receipt,
receipt digest, caller-supplied seed proof, TCP reachability, or a single order
query can never clear it.

If a future evidence contract passes review, recording the verified decision,
append-only evidence digests, order/cancel projections, and any outbox fence
transition must be atomic. Until then, recovery is conservative
`CLAIMED -> UNKNOWN` only; no automatic replay or operator bypass is in scope.

## Offline test boundary and blockers

Fake-only tests may prove exact-key matching, duplicate-event idempotency,
submit/cancel separation, stale-session rejection, and that an interrupted
command remains fenced after restart without evidence. They must not claim a
real provider ACK or a successful UNKNOWN resolution from invented evidence.

Implementation blockers are the clean pinned SDK artifact, verified native
callback field mapping (especially cancel `ActionRef`/`RequestID`), durable
session-generation/event history across restart, and a trusted source for
complete mutually consistent order/trade/position/cancel-action evidence.
