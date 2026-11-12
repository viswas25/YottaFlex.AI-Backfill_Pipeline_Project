# Design & trade-off note

## Problem
Re-process (backfill) selected historical date partitions safely: estimate first, bound concurrency, never duplicate
data, show progress, and restart cleanly after any failure.

## Key decisions
| Decision | Why | Trade-off / alternative |
|---|---|---|
| **Partition overwrite** (delete partition rows + insert) in **one transaction**, instead of row-level upsert | Rerun = same result; corrected files remove stale rows; crash before COMMIT leaves old state | Slightly more write work than a pure upsert; needs a partition key on every fact row |
| **Event history table + derived current-state table** | Brief says preserve change history rather than overwrite; latest-wins makes late / out-of-order data harmless and processing **order-independent** | Extra table + rebuild step (measured: the largest part of load time) |
| **Checksum-based restart** (`partition_run`): skip if latest SUCCEEDED attempt has the same SHA-256 | "Safe restart" without a scheduler; changed source file is detected automatically | Whole-file granularity; `--force` overrides |
| **Parallel parse/validate, serialised load** | Parsing is CPU-bound and independent; SQLite has one writer anyway, so one writer = no lock contention and simple correctness | Throughput capped by the single writer (see performance) |
| **Bounded parallelism** = at most `--workers` partitions in flight (sliding window, not "submit everything") | Bounded memory/CPU, predictable load on the source and target | A slow partition can briefly idle other workers |
| **Retry only transient errors**, exponential backoff; `SchemaError` / missing file fail immediately | Retrying a bad schema is wasted time and hides the problem | Retries re-parse the file (simple > clever) |
| **One bad partition never stops the others**; run ends `PARTIAL` with exit code 2 | Operators see exactly what failed; restart only redoes the failed ones | Caller must check exit code / status |
| **Integer cents + integer half-up rounding** | Deterministic transformations, exact reconciliation | Needs conversion at the edges |
| **Reject, don't drop**: every rejected row is stored with file, line, reason and raw content | Traceability; reconciliation proves `source == accepted + rejected` | Rejects table grows with bad data |
| **Labels in a separate directory** computed from how rows were *constructed* | Independent ground truth: pipeline code is not grading itself | Generator and pipeline must agree on semantics (tests enforce it) |
| SQLite + stdlib only | Runs anywhere with Python, no credentials, zero setup | Not a warehouse; see next steps |

## Idempotency, retry, restart, backfill
* **Idempotent**: partition load = delete-then-insert of exactly that partition inside `BEGIN IMMEDIATE ... COMMIT`; fact rows re-derived only for touched orders. Reject ids are deterministic hashes.
* **Retry**: transient failures (anything not a `PermanentError`) retried up to `--max-retries` with `backoff * 2^(n-1)`; each attempt is a row in `partition_run`.
* **Restart**: just run the same command again. `SUCCEEDED + same checksum` partitions are `SKIPPED`; failed/never-run ones run; attempts left `RUNNING` by a crash are marked `ABANDONED`. Concurrent backfills against the same DB are **not a supported mode**: in a manual test (two simultaneous forced runs) the data stayed correct because each partition load is atomic and idempotent and SQLite serialises writers, but one run's stale-attempt cleanup could mislabel the other run's in-flight attempts in the audit tables. Run one backfill at a time (scheduler concurrency = 1); a lock/lease row is a listed next step.
* **Backfill**: pick partitions with `--start/--end` or `--dates`; `--dry-run` first; `--force` to recompute.

## Known limitations
* Single-node SQLite; one writer. No distributed lock/scheduler.
* A hard crash of a *worker process* (OOM kill) breaks the process pool and fails the run; the next run recovers (attempt abandoned), but there is no automatic pool rebuild.
* Conflicting duplicate across partitions: both versions are kept in history, latest `event_ts` wins; only *within a file* are conflicts rejected.
* Dimensions are SCD type 1 (overwritten); a product price change is not versioned.
* Late data arriving in a partition that was already loaded requires re-running that partition (detected via checksum).
* Throughput numbers come from one machine with 1 visible core, so parallel speed-up is *not demonstrated* here (honestly reported in `reports/performance.md`).

## Possible next steps
Single-run lock/lease row; Postgres/warehouse target with `INSERT ... ON CONFLICT` / partition swap; SCD2 for dimensions; incremental fact maintenance
instead of rebuild; Parquet input; Prometheus-style metrics; orchestration (Airflow/Dagster) calling the CLI; alerting on
reject-rate thresholds (the dry-run already estimates reject rate).
