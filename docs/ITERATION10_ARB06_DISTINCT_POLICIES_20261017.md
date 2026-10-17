# ARB06 independent Risk and portfolio budget policies

The close validator incorrectly required the account Risk reservation policy fingerprint to equal the independently configured portfolio budget policy fingerprint. It now validates each against its own evidence: Risk reservations remain bound to their permits, and the budget release proof remains bound to the original budget hold. No other close predicate changed.

Repository: `D:/bt_api_py/bt_api/bt_api_execution`, branch `dev`, parent `723e8fec29bbf9abbaf49a877dc6517c930f0cfd`. Scope is the validator, the focused regression and this record. The unrelated service-order-entry and contract-test changes remain excluded.

The new single SQLite regression passed once: **1 passed in 1.13 seconds**, exit 0, empty stderr. All 135 fixed source pins and both Execution/Base HEADs were unchanged. It uses different policy fingerprints, verifies their separate bindings, and closes the group. The earlier 12-case R11 suite was not repeated. An initial runner preflight stopped before pytest on the old Execution HEAD; only the new spec's HEAD binding was corrected before execution.

Evidence: `D:/temp/iteration10-arb06-e-closed-proof-distinct-policy-r1-root-20261017/result.json`, SHA-256 `bd49fe6e4ac07de6b2ab0e051d2d25085571b4253bbfded2fcb8826a0a587e7d`. Spec SHA-256 `45ccdda0fba89e4d6bb581ba0086d86f8e7cca103c65ddd9a63065e2ddee425e`; stdout `630d18c301059220ebd508ce404e68724db722e51cc60e5095c7999185cc2e60`. Accepted validator: `eae4f630ea02c25a5ee085b2a1bec0ecaf9d03e7ff1a4df73be191139f74b840`; test: `d3c2eee83b211a0358d776b448f938d2a61725ae14ba33d64d5e897ca3685f8e`.

This is local component evidence with synthetic owner leaves. SDK integration and formal ARB06 acceptance remain pending; the Iteration10 formal total remains 59/68. The commit hash is recorded in the parent repository delivery record.
