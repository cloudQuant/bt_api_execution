# Paper startup recovery before readiness

Execution previously marked a managed Paper scope ready after recovering dispatches without rebuilding the Risk closable-position cut from its persisted book. A seeded opening position could therefore be rejected for close/reduce admission.

This round clears prior Paper readiness, runs the existing dispatch recovery, invokes the configured Paper Risk adapter's trusted recovery with the current scope writer lease, and publishes readiness only after all configured scopes succeed. A recovery failure keeps the scopes unavailable. It reuses the existing book, Risk ledger and idempotent receipts.

Scope on Execution `dev`, parent `3462d6d02a12e6d98d1ba47aee6f290d234c7dc7`: the recovery-method hunk in `service_order_entry.py`, the new `tests/test_paper_scope_recovery.py`, and this record. Unrelated changes in the source file remain uncommitted. Selected patch SHA-256: `0bf1c924f1f938e8e0d60654d6ac8631dec5b63d77dd9c9a8b99e6b1707a05af`; selected committed-source postimage SHA-256: `469cdf53c5805f54fb1669f4ef4df09d502ee8df58b4b890302b405b6409e9f7`.

Root tested the committed Base/Risk/Execution sources with only that method patch and the new test overlaid: **2 passed in 2.04 seconds**, pytest exit 0, no warnings, 164 source/support pins unchanged. The cases cover the opening-book cut, repeated recovery and logical restart without duplicate rebase, and failure clearing prior readiness. Independent static review returned GO. There is no direct concurrent or multi-instrument mid-batch failure test; this method is a startup operation.

Raw result directory: `D:/temp/paper-scope-recovery-fixed-source-root-qa-r1-20261017`. Spec SHA-256: `aebde2f6ae77e809db5f58a42da06057db96c146a9427a7c0fd32106de37d17e`; stdout SHA-256: `1966292f43c4bf2c156afe6045a64e488fa49aec9fc61bbe12cd55e91da65398`. The earlier author's mixed-worktree 2-pass run is supplementary only. No accepted test was rerun for delivery.

Evidence is local SQLite component acceptance. The failed AFI08 R15 result remains unchanged; HTTP/Host reduction and formal AFI08 acceptance still require their own checks. No provider/account operation occurred.
