# Iteration10: approved-target strategy reduction

Scope: add the Execution-side `StrategyReductionHandlerV1` and focused contract tests. The handler consumes only an exact approved V1 CLOSE/reduce-only target through the existing admission and order-entry path. The caller cannot replace target terms; non-default close dimensions fail closed. Partial fills remain partial, and UNKNOWN outcomes require review and are query/replay only.

Evidence: the original focused suite reported 9 passed on the canonical Python 3.11.5 runtime. After the outcome-stage correction, the newly added definitive Execution no-record denial test reported 1 passed on the final source. These are two author runs on successive candidate pins, not a single 10-test run. Independent static review returned GO after the correction. No raw JUnit/result artifact was produced. Final source pin: `src/bt_api_execution/strategy_reduction.py`, 17,991 bytes, SHA-256 `197b8ef7542570c4e8b3bf9091c2ff40b72d3b983aa75a38f4fc9e571ba38cb6`; test pin: `tests/test_strategy_reduction.py`, 15,768 bytes, SHA-256 `4d087779b9b9afd4f1beb5246e883e6f71bfe923715bd784b9097c2a13e9b8c6`.

This is an Execution component only. It does not establish an HTTP-to-ServiceHost integration, venue/provider acceptance, account-level flatness, or a formal Iteration10 pass. UNKNOWN is never automatically redispatched as a new order.
