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

## Backtrader managed-handoff bridge (design only)

**Status:** unregistered interface design. No converter, provider adapter, or
default route is implemented by this package.

The Backtrader-side `backtrader_runtime/ctp_managed_handoff.py` and
`backtrader_runtime/managed_execution.py` contracts cannot currently be
adapted by returning an SDK outbox receipt from the existing synchronous
managed facade. `ManagedExecutionBridge` expects its dispatch callback to
return a provider observation that can be recorded in the generic execution
ledger. The typed `CtpManagedExecutionAdapterPlaceholder` rejects before
calling the SDK Store for this reason. By contrast, the SDK's
`CtpDispatchCommand` and `CtpDispatchReceipt` describe a local staged action
and local dispatch disposition. There is no SDK method that atomically records
an outbox transition and a provider order/cancel projection from a callback.

### Proposed immutable bridge envelope

A future adapter should carry a versioned, immutable envelope containing the
exact typed handoff and the exact outbox command binding. It must retain both
request digests because they have different domains:

- `handoff_request_digest` is the Backtrader handoff digest over operation,
  execution/account scope, trading day, managed identities, native target, and
  request fields;
- `outbox_request_payload_sha256` is the SDK digest over its canonical request
  payload only.

These digests must never be compared as if they were interchangeable. The
bridge must compare the canonical request fields themselves, with no implicit
field renaming or dropped fields. Any future field translation needs its own
versioned, reviewed mapping and exact round-trip tests.

The proposed DTO is two immutable records, not a wrapper that looks like a
provider observation:

```text
CtpManagedOutboxBindingV1(
    operation, command_id, managed_action_id,
    account_key, scope_key, trading_day,
    managed_intent_id, runtime_order_id, order_ref,
    canonical_request_fields, handoff_request_digest,
    outbox_request_payload_sha256,
    approval_use_id, approval_digest, session_binding_sha256,
    session_generation, selected_md_td_pair_digest,
    td_front_id, td_session_id,
    cancel_target_exchange_id, cancel_target_order_sys_id,
    cancel_target_front_id, cancel_target_session_id,
)

CtpManagedOutboxDispatchFact(
    binding_digest, local_state, local_outcome, local_receipt_digest,
)
```

Fields not applicable to submit are explicitly absent; cancel requires the
complete target tuple. `managed_action_id` is the submit intent ID for submit
or the distinct cancel-action ID for cancel; it is never inferred from the
generic `command_id`. Neither DTO implements `ProviderObservation`. A
separate future `VerifiedCtpCallback` must bind `binding_digest`, native
session generation, callback stream and stable event ID, event family,
verifier identity, source digest, and exact callback identifiers before any
provider projection is considered.

The currently shared identity must echo exactly between the Backtrader handoff
and staged command: operation, account key, scope key, trading day, reserved
`OrderRef`, reservation managed intent, and canonical request fields. The
outbox command and local receipt must additionally echo the approval-use ID,
approval digest, and session-binding digest exactly. The existing handoff
does not yet contain those latter fields, so a future envelope must carry them
alongside—not pretend they are handoff authority. Approval and receipt digests
are echo fields only. Fresh action approval must still be checked by the
trusted verifier during `claim_ctp_dispatch_command`; the one-use
authorization and claim stay in that same SQLite transaction.

The offline schema-v7 candidate now persists and returns a version-1 typed
`CtpDispatchCorrelationKey` for newly staged commands. It binds the exact
account, scope, trading day, operation, command ID, canonical request digest,
reservation intent, managed action ID, reserved runtime order ID and OrderRef,
approval-use/digest echoes, session-binding digest, non-reused session
generation ID, dispatch FrontID/SessionID, native RequestID, and optional
cancel ActionRef. The store obtains `runtime_order_id` from the exact durable
OrderRef reservation rather than caller input. Submit's action ID must equal
the reservation intent; cancel's action ID is explicit and distinct from the
target reservation intent. These key fields are persisted in immutable command
columns and echoed by the local receipt. Legacy READY/CLAIMED rows without
these keys migrate to `UNKNOWN`, preserving the fail-closed fence.

`CtpDispatchCallbackKey` and `require_ctp_dispatch_callback_match` are also
offline, pure DTO/matching seams. The callback key carries the full command
correlation key plus callback family, stream/event ID, native RequestID and
ActionRef, callback OrderRef, optional ExchangeID/OrderSysID, and exact cancel
target FrontID/SessionID. Matching rejects structural mismatches only. These
types do not verify native callback provenance, are not durably appended, and
cannot update order/cancel projections or clear `UNKNOWN`; caller-supplied IDs
or digests are not evidence. The ActionRef and RequestID fields are opaque
correlation values here, not a claim that a specific native callback family
echoes them.

Cancel matching also needs an explicit versioned target tuple:
`OrderRef`, `ExchangeID`, `OrderSysID`, `FrontID`, and `SessionID`. The SDK
command has typed exchange, system, front, and session target fields, while the
Backtrader cancel handoff types all of these except `ExchangeID`. Although
`request_fields` can carry arbitrary scalars, it does not establish that
`ExchangeID` is present or that the handoff validator checked it. A bridge
must reject until the handoff version types and validates the same target
tuple. It must also keep the cancel action's own native `ActionRef` and/or
`RequestID` distinct from the target order identifiers.

The new outbox key requires a caller-supplied session generation and exact TD
`FrontID`/`SessionID`, but does not establish that the generation is durable,
non-reused, or sourced from a verified native login. The `session_binding`
mapping remains an unconstrained input; its digest does not reveal or validate
its contents. The Backtrader handoff still lacks matching typed session
identity. No generation or login readiness may be inferred from a queue
receipt. MD and TD readiness must be independently typed, sourced, and fresh
before a future writer claim can reach native dispatch.

### Status and callback mapping

Keep the local outbox state, local native-call receipt, provider order state,
and cancel-action state as separate facts:

| SDK outbox fact | Backtrader bridge fact | Provider projection |
| --- | --- | --- |
| `READY` | staged only | none |
| `CLAIMED` | one-use action claim consumed; native effect may follow | none |
| receipt outcome `QUEUED` (outbox row becomes `COMPLETED`) | local dispatch/queue fact only | no ACK, fill, or cancel state change |
| receipt outcome `REJECTED` (outbox row becomes `COMPLETED`) | local refusal fact only, if exact no-native-effect evidence exists | no provider rejection unless a separate provider callback proves it |
| receipt outcome `UNKNOWN`, or a prior-generation `CLAIMED` recovered as `UNKNOWN` | uncertain action; keep action/account fences | remain `UNKNOWN` absent verified recovery |

In particular, SDK `COMPLETED` means that its local receipt was persisted; it
does not mean `ACKED`, `FILLED`, `CANCELLED`, or provider-terminal. The
Backtrader `CtpManagedLocalQueuedReceipt` and
`CtpManagedNativeSubmissionReceipt` also remain non-acknowledgement types.
The SDK receipt still has no typed queue-receipt ID/depth contract that can be
safely translated into the Backtrader local queued receipt, and its untyped
`native_receipt_payload` must not be mined for a request ID or callback
identity. The typed correlation echo preserves the command binding and local
outcome without implementing `ProviderObservation`.

The existing synchronous `ManagedExecutionFacade` cannot safely consume that
DTO as a provider observation. A future CTP-specific asynchronous port must
leave the execution record unresolved after `QUEUED`, and later accept only a
verified callback envelope. Submit callbacks need exact command/action,
account, trading day, `OrderRef`, `FrontID`/`SessionID`, session generation,
native `RequestID` where the callback contract guarantees it, and eventual
`ExchangeID`/`OrderSysID`. Cancel callbacks additionally need the separate
cancel action identity (`ActionRef` and/or `RequestID`) and exact target echo.
Missing, reused, or ambiguous keys leave the action `UNKNOWN`; arrival order or
target-only matching is insufficient.

Order/trade callbacks advance only the submit's provider order projection.
Cancel-action callbacks advance only that cancel action. An accepted cancel
request does not cancel its target; only exact terminal order evidence with
consistent trade evidence may move the target order to cancelled. Deduplicate
callbacks by a stable session-generation/stream/event identity. Callback-source
generation and verified evidence digests must be retained durably.

### Required transaction boundary and blockers

The outbox's `stage`, `claim`, and `complete` methods each persist only their
own command/receipt fact. The generic `execution_records` and cancellation
records have separate transitions, and the Backtrader framework projection
journal is another durable boundary. A callback adapter must not commit one
projection and then report success while another write fails. Before enabling
the bridge, a store-owned API must atomically append the verified callback,
deduplicate its stable event ID, advance the matching submit-order or
cancel-action projection, and update any same-database command fence. If the
generic execution ledger and outbox do not share that exact transaction, one
must be designated the canonical journal and the other made a deterministic,
rebuildable projection; independent writes are not an acceptable bridge.
External risk-permit settlement must be idempotently keyed to that committed
event and cannot be presented as part of a SQLite transaction unless it truly
shares it.

The first offline step now supplies typed action/runtime-order/native-session
keys, typed callback correlation DTOs, exact structural matching, fake submit
and cancel coverage, and fail-closed migration/restart behavior. The remaining
minimum sequence is: (1) bridge matching typed handoff IDs and validate the
exact cancel `ExchangeID`/ActionRef/RequestID contract; (2) add a CTP
asynchronous managed port that records local dispatch without fabricating a
`ProviderObservation`; (3) add a verified callback ledger and one transaction
boundary for callback deduplication plus provider projections; then (4) prove
with fake crash/restart tests that only exact verified evidence can resolve
`UNKNOWN`, and duplicate, stale-generation, mismatched-submit, and
mismatched-cancel callbacks cannot advance state. Until those seams and the
separately reviewed SDK artifact and external account-wide writer fence exist,
this bridge stays design-only and unregistered.
