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

## Slice 4 ledger — scan attribution and immutable read caches

Recorded 2026-09-19:

- `ScanMetrics` and caller-supplied `ScanCounters` share one counter set. Data
  and delete-file reads enter that set once, after a physical read succeeds.
- The modular Parquet reader labels footer, page-index and data ranges at the
  read site. Its existing coalescing and concurrency controls remain the only
  range controls; a coalesced range is charged as one read and its fetched
  length is charged once.
- Parsed Parquet metadata is cached by immutable path and page-index option
  set. The option set is part of the key so a footer opened without page
  indexes cannot satisfy a later positional-delete read that needs them.
- The raw immutable-object cache is byte-bounded, rejects entries larger than
  its budget and excludes mutable WAL paths. Concurrent identical misses share
  one population; failure or cancellation releases ownership for a retry.
- Whole-file and range-cache hits spend Tokio cooperative budget. A hand-poll
  fixture proves a warm loop parks on that budget and completes when the budget
  is lifted.
- Puffin metadata and decompressed blobs have separate bounded caches. Puffin
  misses use the same weak single-flight contract, and oversized blobs are
  refused before insertion.
- Cold, warm and parsed-cache-bypassed reads return the same rows. Counted
  readers cover coalesced data attribution and delete-file bytes; the candidate
  library suite passes 1,396 tests.
- Production remains on the 0.9.1 reader until the adoption slice. The private
  pre-squash rebase note is historical; this ledger is the public candidate
  evidence for the slice.

## Slice 5: pruning with exact filtering

Recorded 2026-09-19:

- The modular candidate reader accepts `RawPruneSpec` and
  `PromotedPruneSpec`. File and row-group trigram blooms, inline and Puffin v1
  inverted indexes, and promoted Utf8 min/max statistics now narrow reads.
- Scan planning attaches table-metadata statistics blobs to their data-file
  tasks. Puffin discovery checks the blob's data-file and column properties,
  and refuses a row-group-size stamp that does not describe the Parquet file.
- Every hint is conservative. A missing or malformed bloom/index, a missing
  promoted column in a pre-promotion file, and an index query the format cannot
  answer all fall back to the scan. The caller's predicate remains exact; the
  production boundary at `crates/loglake-storage/src/query_provider.rs` stays
  on the complete 0.9.1 reader until adoption.
- Bloom, predicate-statistics, inverted-index, page-index and positional-delete
  selections compose by intersection. The per-scan counters require a
  selective case to report file, row-group or row pruning, so an implementation
  that disables every hint does not pass the matrix.
- Puffin footer/blob caches honor the reader's measurement bypass. The v1 index
  load has the production concurrency bound, with the environment parsing
  covered through a pure resolver.
- The candidate library suite passes 1,400 unit tests and 85 doctests (12
  ignored).

The focused matrix covers:

| Shape | Equality check | Required pruning evidence |
| --- | --- | --- |
| token, substring and OR terms | enabled rows equal the exact filter over the disabled scan, cold, warm and bypassed | inverted selection drops rows |
| promoted equality | exact equality over the hinted scan equals the disabled scan | a disjoint row group is counted under statistics pruning |
| null/OR predicate | the modular predicate suite checks Kleene `IS NULL OR` filtering | filter-only fields are collected and evaluated |
| absent/malformed metadata and pre-promotion files | fall back to the disabled result | no false-negative selection is built |
| inline and Puffin indexes | cold, warm and bypassed lookups return the same postings | both storage paths build selective row selections |
| stale Puffin stamp | falls back to the exact scan | the stale index is not used |
| row-group and positional-delete intersection | the final selection contains only live indexed rows | both mechanisms remove rows |

Focused commands:

```sh
cargo test --manifest-path bench/fork-rebase/Cargo.toml -p iceberg \
  arrow::reader::pruning::tests
cargo test --manifest-path bench/fork-rebase/Cargo.toml -p iceberg \
  arrow::reader::row_filter::tests::test_kleene_logic_or_behaviour
```

## Slice 6 ledger — ordered and reverse streaming

Recorded 2026-09-19. This is the complete reader-extension checklist retained
in the public candidate tree; the pre-squash
`docs/internal/FORK_REBASE_2026-09.md` is historical and is not restored.

- [x] The candidate builder exposes task-order preservation, reverse traversal
  and a per-reader reverse-chunk bound. The candidate storage adapter carries
  the same three decisions used by production's query provider.
- [x] Concurrent file opens drain in task order. Non-front batches are charged
  to `LOGLAKE_ORDERED_DRAIN_BUFFER_BYTES`; the buffered-byte gauge returns to
  zero when the stream is dropped.
- [x] Reverse reads walk row groups from tail to head, split selections at row
  group boundaries and reverse rows across batch boundaries. The first chunk
  is one output batch and later chunks grow by four up to the configured cap.
- [x] Predicate selections and positional deletes compose before chunks are
  planned. Fixtures cover nulls, timestamp ties, multiple row groups, sparse
  selected rows and deleted rows against materialized references.
- [x] Filter-free decoded chunks use an immutable, byte-bounded cache. Bypass
  skips both lookup and population. A guard releases single-flight ownership
  on success, decode/open error, timeout and cancellation.
- [x] Cold, warm and bypassed reverse reads return equal ordered rows. A
  counter-based early-stop fixture drops a LIMIT-like stream after its first
  batch and proves it fetches fewer data bytes than a full drain.
- [x] The accumulated candidate library suite passes 1,410 unit tests and 85
  doctests (12 ignored).
- [x] Production selection remains on the complete 0.9.1 reader. Slice 7 must
  adopt the candidate dependency graph atomically; no partial reader becomes
  the default.

Focused commands:

```sh
cargo test --manifest-path bench/fork-rebase/Cargo.toml -p iceberg \
  arrow::reader::pipeline::tests::reverse_chunks
cargo test --manifest-path bench/fork-rebase/Cargo.toml -p iceberg \
  dropping_reverse_limit_stream_stops_before_full_decode
cargo test --manifest-path bench/fork-rebase/Cargo.toml -p iceberg \
  arrow::reader::reverse::tests
```

## Refreshed divergence inventory

The candidate core now carries the slice-2 catalog, transaction and Parquet
writer behavior, the slice-3 OpenDAL upload controls, slice-4 reader caches and
counters, slice-5 pruning, and slice-6 ordered/reverse streaming. Its packaging
changes also gate the four `iceberg-storage-opendal` external-service tests
described above. The schema and expiry caller differences remain in
`adapter/src/lib.rs`; the candidate credential adapter is in
`adapter/src/aws_credential.rs`.

Upstream 0.10.1 now supplies schema evolution and snapshot expiry, so those two
local actions do not move forward. The remaining production divergences still
need later slices: segmented-index publication and final dependency adoption.
Refresh the file inventory against the package sources with:

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
