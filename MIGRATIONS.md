# Migration notes

## `0.1.x` to `0.2.0`

`CtpOrderRefSeedProof` is a public constructor, and its four-field form is no
longer accepted. Callers must provide the original fields
(`trading_day`, `native_max_order_ref`, `legacy_ledger_max_order_ref`, and
`legacy_ledger_sha256`) plus all of these fields:

- `account_key` and `scope_key` for the exact account and execution scope;
- `session_generation_id`, `native_front_id`, and `native_session_id` for the
  observed native session;
- `existing_native_order_refs` as the complete native reference inventory;
- `legacy_source_sha256` with the separate source digests; and
- `legacy_mappings` with every imported legacy order identity.

Build the new proof from the caller's complete cutover inventory and pass the
exact scope and session values through unchanged. Do not add placeholder
defaults, infer omitted mappings, or reuse the old four-field object: the store
validates these values together and rejects incomplete or mismatched evidence.
This proof remains caller-supplied offline cutover input; it does not
authenticate a provider observation, establish an external writer fence, or
authorize provider I/O.

Consumers that pin this package by exact version must review the `0.2.0`
artifact and update their code-owned pin and artifact digest together. Broad
version ranges do not document or review the changed constructor contract.
