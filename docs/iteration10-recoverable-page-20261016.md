# Iteration10: bounded recoverable-group pages

Scope: add `SqliteOrderGroupStore.list_recoverable_page` and its focused test. Stable exclusive key `(created_at_ns, portfolio_ref, group_id)`, validated limit 1..256, real SQL LIMIT and supporting index. Existing list_recoverable API is unchanged.

Root local SQLite QA: 17 passed in 1.38s, exit 0, no warnings, 139 source pins and Execution/base HEADs unchanged. Raw output: D:/temp/iteration10-execution-recoverable-page-root-20261016-r1. Independent static review GO_STATIC_STORE_API_ONLY. Tests cover paging/order, concurrent-between-pages state changes, terminal exclusion, legacy API agreement, invalid arguments and SQL LIMIT.

This is a storage API component, not DIM09 acceptance or proof of autonomous service recovery. Callers must wrap to after=None after an empty page so entries becoming nonterminal behind the cursor are eventually revisited. The existing recover_interrupted_groups method still uses the old unbounded API; the next SDK owner tick must use this new page directly and advance only the selected group. No provider/account action, Host or new worktree. Execution/dev parent f5c030644dea06ce91afbaacf94e11e353538186; accepted commit is recorded by Git and the parent SDK round.
