# Changelog

## 0.2.0 (unreleased)

- **Breaking:** `CtpOrderRefSeedProof` now requires all 12 cutover fields. The
  former four-field constructor is rejected; see [MIGRATIONS.md](MIGRATIONS.md)
  for the required caller changes. This keeps incomplete legacy inventories
  from being treated as valid cutover input.
- The package version is `0.2.0` so exact-version consumers can review and pin
  the changed public constructor separately from `0.1.0`.
