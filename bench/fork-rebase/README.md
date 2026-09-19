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

## Refreshed divergence inventory

The candidate core carries no LogLake source behavior yet. Its only packaging
change gates the four `iceberg-storage-opendal` external-service tests described
above; rustfmt normalization is mechanical. The schema and expiry differences
are confined to `adapter/src/lib.rs` and its fixture runners.

Upstream 0.10.1 now supplies schema evolution and snapshot expiry, so those two
local actions do not move forward. The remaining production divergences still
need later slices: reader pruning/caches/order/counters, writer footers and
manifest stamping, atomic rewrite, catalog base threading/metrics, and OpenDAL
upload controls/permits. Refresh the file inventory against the package sources
with:

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
