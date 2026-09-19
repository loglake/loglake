# Iceberg 0.10.1 candidate workspace

This workspace stages the fork rebase without changing the production
workspace's dependency selection. `candidate/` began from the pristine
published 0.10.1 sources for the three owned forks, without registry marker,
package lock or generated dependency-list files. `baseline/` names the forced
Arrow, Parquet, DataFusion, OpenDAL and reqsign graph. `adapter/` contains the
first caller ports and old/new process-level equivalence fixtures.

The candidate keeps upstream's declared Rust 1.94 minimum. The repository still
pins Rust 1.95.0 in `rust-toolchain.toml`; the candidate does not change it.

## Slice 1 ledger — schema and expiry

Recorded 2026-09-19:

- Pristine library compilation passed for `iceberg`, `iceberg-catalog-sql`,
  `iceberg-storage-opendal` and `iceberg-datafusion` 0.10.1 with Arrow/Parquet
  58.4, DataFusion 53.1, OpenDAL 0.57 and reqsign 3.
- The published `iceberg-storage-opendal` package's declared MinIO test targets
  do not compile outside the upstream repository because the package omits its
  workspace-only `iceberg_test_utils` development dependency. The candidate
  library compiles; a manifest-only feature gate keeps those named test targets
  outside this hermetic workspace.
- `adapter/` replaces the candidate schema action with upstream
  `AddColumn::optional`, after a name/type diff preserves replay safety.
- Count-only expiry sets `expire_older_than_ms(i64::MAX)` explicitly, removing
  upstream's implicit five-day boundary so `retain_last` controls selection.
  Age-plus-count passes its explicit cutoff separately.
- Expiry preview commits into an in-memory metadata-only catalog. The resulting
  snapshot/ref delta supplies the exact count without object deletion. An empty
  preview returns no transaction, so it cannot create an empty catalog commit.
- Separate 0.9 and 0.10 processes read the same Iceberg JSON fixtures and
  compare normalized JSON. They exchange no Arrow or Iceberg Rust types.

## Slice 2 ledger — commit and writer equivalence

Recorded 2026-09-19:

- Transactions pass their refreshed base to an opting-in catalog, refuse a
  recreated table UUID, retain retryable CAS conflicts, and avoid metadata
  commits when a replayed action becomes empty. A mock-catalog fixture proves
  the supplied-base path does one load.
- Atomic rewrites conserve the current live-file set for add-only, delete-only
  and replacement commits, reapply against a stale base, retain caller summary
  properties, and reject partial removals. Reserved snapshot IDs remain
  available for a same-transaction statistics registration.
- The SQL catalog applies a commit to the supplied base and conditions its
  pointer update on that base's metadata location. SQLite uniqueness failures
  retain distinct table and namespace creation-race kinds.
- Candidate Parquet output now carries row-group trigram blooms, compact typed
  group counts, time buckets for every timestamp unit, and the manifest sort
  order ID. The fixture compares decoded rows and footer payloads between the
  append and rewrite writer shapes, including null and capped dimensions.
- Snapshot summary parsing stays on upstream 0.10.1; only the underflow defense
  for an invalid rewrite is retained. Manifest validation still rejects that
  rewrite before persistence.
- Reader behavior and OpenDAL remain outside this slice. The root workspace's
  production dependency selection is unchanged.

## Slice 3 ledger — uploads and AWS credentials

Recorded 2026-09-19:

- Candidate OpenDAL writers retain the production multipart concurrency and
  chunk resolvers. Their defaults remain sequential writes and the service's
  chunk size; invalid and zero values preserve those defaults.
- Effective concurrency and chunk bytes remain gauges, and every opened writer
  increments the class-labelled counter.
- Drain and compaction keep separate permit pools. Memory-backed writer
  fixtures prove release on close, write failure, owner cancellation and drop,
  plus cancellation while waiting for a permit.
- Retry retains jitter, a 100 ms minimum and five attempts. OpenDAL 0.57's
  `TimeoutLayer` stays inside `RetryLayer`, so each attempt has its own timeout
  and a timed-out inner future cannot leave retry state half-consumed.
- The candidate adapter implements reqsign 3 `ProvideCredential` in the order
  static keys, IRSA, ECS and IMDSv2. A configured relative or full ECS URI
  suppresses IMDS fallback, including when ECS returns an error.
- Each metadata provider has a three-second bound. Provider errors and timeouts
  return to the caller instead of falling through to a lower-priority source.
- Hermetic contexts exercise real reqsign IRSA, ECS and IMDSv2 providers. They
  also prove rotating credentials and expiry pass through the adapter without
  process environment mutation or live AWS calls.
- Candidate `file`, `memory`, `s3` and `s3a` factories construct input and
  output paths without I/O. Unsupported and schemeless warehouse URLs fail.
- Production stays on Iceberg 0.9.1, OpenDAL 0.55 and reqsign 0.16 until the
  adoption slice.

## Refreshed divergence inventory

The candidate core now carries the slice-2 catalog, transaction and Parquet
writer behavior plus the slice-3 OpenDAL upload controls. Its packaging changes
also gate the four `iceberg-storage-opendal` external-service tests described
above. The schema and expiry caller differences remain in `adapter/src/lib.rs`;
the candidate credential adapter is in `adapter/src/aws_credential.rs`.

Upstream 0.10.1 now supplies schema evolution and snapshot expiry, so those two
local actions do not move forward. The remaining production divergences still
need later slices: reader pruning/caches/order/counters, segmented-index
publication and final dependency adoption. Refresh the file inventory
against the package sources with:

```sh
diff -rq --exclude=.cargo-ok --exclude=.cargo_vcs_info.json \
  "$CARGO_HOME/registry/src/index.crates.io-"*/iceberg-0.10.1 \
  bench/fork-rebase/candidate/iceberg
```

Commands:

```sh
CARGO_HOME="$TMPDIR/cargo-home" cargo check \
  --manifest-path bench/fork-rebase/Cargo.toml --workspace --all-targets
CARGO_HOME="$TMPDIR/cargo-home" cargo test \
  --manifest-path bench/fork-rebase/Cargo.toml -p fork-rebase-adapter
cargo fmt --manifest-path bench/fork-rebase/adapter/Cargo.toml -- --check
# Repeat the fmt check for baseline/ and each candidate manifest.
CARGO_HOME="$TMPDIR/cargo-home" cargo clippy \
  --manifest-path bench/fork-rebase/Cargo.toml \
  --workspace --all-targets -- -D warnings
```

Production remains on the root workspace's Arrow 57, DataFusion 52, OpenDAL
0.55 and Iceberg 0.9.1 selection. Its schema tests are the production-side gate
for this slice.
