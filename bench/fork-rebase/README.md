# Iceberg 0.10.1 rebase ledger

This ledger records the isolated qualification that preceded the atomic
production adoption. The temporary `candidate/`, `baseline/` and `adapter/`
trees were removed in slice 7 after their sources and regressions moved into
`third_party/` and the permanent workspace suites.

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

The permanent fork gate is `scripts/check-fork-tests.sh --fork iceberg`.

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
- [x] The complete reader was held outside production selection until slice 7
  adopted the dependency graph atomically.

The permanent fork gate is `scripts/check-fork-tests.sh --fork iceberg`.

## Slice 7 ledger — atomic adoption

Recorded 2026-09-19:

- The tested 0.10.1 sources replaced all three production forks together. The
  seven workspace consumers and root lockfile changed in the same adoption.
- The application resolves one Arrow/Parquet 58.4, DataFusion 53.1, OpenDAL
  0.57 and reqsign 3 graph. The query server's direct `arrow-json` dependency
  and storage's `iceberg-datafusion` dependency follow that graph.
- `LoglakeIcebergTableScan` uses `Arc<PlanProperties>` and the modular reader's
  runtime plus `ScanResult` contract at every call site. DataFusion SQL AST and
  OpenDAL credential APIs were adapted without changing read-only admission or
  credential fallback policy.
- Segmented-index publication, clipped lookup, cache bounds and metric label
  catalogs moved into the modular writer and reader before the switch.
- Schema, expiry and AWS credential adapter regressions now run in permanent
  storage/fork suites. The promoted fork sources retain the pruning,
  ordered/reverse, cache and transaction tests accumulated in slices 1–6.
- `third_party/*/.cargo_vcs_info.json` and `VENDORED.md` identify upstream
  commit `04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`. The temporary fork copies
  and their private lockfiles were removed.

Permanent gates:

```sh
cargo test --workspace
scripts/check-fork-tests.sh
scripts/ci-local.sh --strict
```

## Slice 8 ledger — performance acceptance (pre-registered)

Recorded 2026-09-23, before spending on the matched control round.

The candidate is LogLake `59ce561` (0.2.0), image
`loglake:bench-20260923-071516`, digest
`sha256:283ae38377746fc846e2853d24b6730b33dddbf4a373818d78de41715cada043`,
using `loglake-benchmarks` revision `590df579b917`. Its artifacts are retained as
`loglake-benchmarks/results/20260923-aws`.

The control source is `ea3c244`, the first parent of adoption merge `c3033f4`.
Its image is `loglake:bench-20260919-191232`, digest
`sha256:c4cf2627d26e433cf0f8a324207f194e7e5abe3041c3fe1bfd70f87a54ed5c91`.
Its only retained round, `loglake-benchmarks/results/20260919-aws`, used a
4 GiB query pod and two peers, so it is not configuration-matched evidence.
Round identity comes from `provenance.json`'s
`running_build.<role>.commit`, for both the query server and compactor, rather
than `repo_head`. In particular, `20260919-aws.191217` has `repo_head`
`ea3c244` but ran `5348199` binaries and is not control evidence.

### Matched contract

Both sides use an `m6i.4xlarge` and corpus `logs-50g`
(`s3://quickwit-bench-corpora-001520130581/corpora/v1/logs-50g/`), with an
unlimited query pod (`query_memory_limit_gb` 0), result cache off,
`peer_count` 0, `index_rebuild` 0, 10 core runs, 8 edge runs and
`previous_round` `20260916-aws.193232`. That previous round ran `66e2f11`; it
fixes the comparison input and is not a substitute for the `ea3c244` control.
The control's verbatim `aws_round` parameter set is:

```yaml
{skip_build: true, image_tag: "loglake:bench-20260919-191232", query_memory_limit_gb: 0, previous_round: "20260916-aws.193232"}
```

Each side archives:

- per-shape p50 and p95, with every cold and warm per-run sample;
- `served_by` for every run;
- object-store bytes by phase and the index/metadata remainder;
- files planned and read, and rows pruned by selection;
- ingest `accept_rows_per_s`, plus drain and commit counters from
  `compactor-metrics.txt`;
- cgroup `memory.peak`; and
- `loglake_query_memory_pool_reserved_bytes` beside
  `loglake_query_in_flight`.

The registered limits are the standing one-sided 2x bands and absolute
ceilings in `loglake-benchmarks/docs/predictions/50g-regression.json`. The
candidate must also equal the control's `served_by` value on all seven route
rows: `label_filter`, `keyword_and_label`, `label_filter_last25`,
`count_by_level_last25`, `count_by_level`, `group_by_service` and
`high_card_group_by_host`. After both suites,
`loglake_query_memory_pool_reserved_bytes` must be 0 whenever
`loglake_query_in_flight` is 0. A separate round with `peer_count` at least 1
must run a cross-shard `GROUP BY` whose counts for each group sum to `record_count`
98,466,115.

### Historical ceilings and current failures

The 2026-09-02 and 2026-09-03 baselines used LogLake `7c84634` and `4e03864`
with `loglake-benchmarks` revision `ac47b1f`. Those LogLake SHAs exist on
`private/main` only. The
rounds predate `provenance.json` and retain no image digest,
`ingest-summary.json` or pod size. They supplied the absolute ceilings; they
are not a comparison arm for this acceptance. The 2.57 ms
`count_by_status` result is an httplogs measurement outside the logs-50g
suite. Its logs-50g analogues are the three `tier1_inline` route rows named
above.

`results/20260923-aws` has four standing-prediction failures:

- `keyword`: 13.68 ms, with 19 files planned versus 23 in its selected
  previous-round baseline;
- `label_filter`: 70.35 ms, with 15 files planned versus 16;
- `label_filter_last25`: 52.05 ms, with six files planned on both sides but no
  candidate smallest-file measurement; and
- `multi_label_and`: 1,376 ms, with 15 files planned versus 16.

These failures are not attributable to the adopted stack before the matched
contract runs. They already have pre-adoption precedents: `multi_label_and`
was 1,205 ms in `results/20260916-aws.214836`, and `label_filter` was 55–75 ms
in every retained 2026-09-19 round, including the clean `ea3c244` round. The
file layouts above also differ from the selected previous-round baseline.
`loglake-benchmarks` tasks #5907 and #5920 own the controlled comparison and
its report.

Acceptance requires all four conditions at once: equality on the seven
`served_by` rows, no latency row beyond either its matched-control 2x band or
its absolute ceiling, pool reservation back at 0 while idle, and the exact
cross-shard sum. Otherwise the stack is not accepted and the report names
every breached row or invariant.

Three readings from the candidate round already hold: the typed Tier-1 routes
were retained in every run; the host rollup was `LOST`, as the negative
control; and after the 08:02:25Z restart the pool retained 11.6 GiB with
`loglake_query_in_flight` 0.

Verdict: not accepted yet
