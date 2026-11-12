# Interview walkthrough notes (study these; be ready to explain in your own words)

## 60-second pitch
"I built a backfill tool for daily CSV partitions. You first run `--dry-run` to see what would run, skip or fail. A bounded pool
of workers parses and validates partitions in parallel; a single writer loads each partition in one transaction by deleting that
partition's rows and re-inserting them, so reruns are idempotent. Every attempt is recorded, so restart is just 'run it again':
finished unchanged partitions are skipped by checksum. Bad rows go to a reject table with file and line. A reconciliation report
compares the target with the source and with independently generated labels."

## Questions to expect, and where the answer is
| Question | Answer / code |
|---|---|
| Why partition-overwrite and not upsert? | Replaces exactly one partition atomically; handles corrected files; `loader.py` |
| What if the process dies mid-load? | SQLite rollback leaves old state; attempt stays `RUNNING` -> next run marks `ABANDONED` (`db.abandon_stale`); test `test_crash_leaves_running...` |
| How is history preserved? | `order_event` keeps every accepted event; `fact_order` is derived (latest `event_ts`); test `test_status_history_is_preserved...` |
| Late / out-of-order data? | Latest-event-wins makes load order irrelevant; `test_partition_processing_order_does_not_change_result` |
| Why integer cents and half-up? | No float drift; deterministic; `validate.net_cents` |
| Why is parallelism only for parsing? | SQLite single writer; parse is the CPU part; see `reports/performance.md` |
| Did parallelism speed things up? | Not demonstrated: sandbox had 1 core. Say so. |
| Retry policy? | Only transient errors; `PermanentError` (schema, missing file) never retried; `orchestrator._record_failure` |
| How do you know it's correct? | 69 tests + reconciliation (source rows = events + rejects, FK/PK checks, aggregates, row-level match with labels) + a negative test proving checks can fail |
| Weaknesses? | Single node, one run at a time, SCD1, worker-process crash not auto-recovered, whole-file checksum |
| What would you change at 10x / 100x? | Warehouse target, bulk load/partition swap, incremental fact maintenance, orchestrator, lock lease |

## Things worth re-deriving by hand
1. Why `(source_partition, order_id, event_ts)` is the event PK, and how ties are broken.
2. The fact rebuild SQL (`db._REBUILD_FACT_SQL`): window function picks latest event per touched order.
3. Why bookkeeping statements autocommit outside the data transaction (so `FAILED` survives a rollback).
4. How the generator builds labels without calling pipeline code (`datagen.py`, label walk after shuffle).
