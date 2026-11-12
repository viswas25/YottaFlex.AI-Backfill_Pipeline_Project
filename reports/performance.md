# Performance measurement

- Dataset: **143,327 source rows** in 30 daily partitions (generated in 5.0s, seed 42)
- Machine: Linux-6.18.44-fc-v378-x86_64-with-glibc2.39, Python 3.12.3, 1 CPU core(s) visible
- Each configuration loads into a fresh SQLite database (WAL). Wall-clock, single run.

| Config | workers | batch_size | seconds | rows/s | speed-up vs A |
|---|---|---|---|---|---|
| A_baseline_row_at_a_time | 1 | 1 | 7.05 | 20,328 | 1.00x |
| B_batched_executemany | 1 | 5000 | 6.84 | 20,969 | 1.03x |
| C_parallel_2_workers | 2 | 5000 | 7.18 | 19,966 | 0.98x |

## Stage breakdown (config B, single process)

- parse + validate + transform: **2.71s** (52,977 rows/s)
- database load (delete, insert, rebuild fact): **4.07s** (35,245 rows/s)

## Reading the numbers

- **Optimisation implemented: batched `executemany` inside one transaction per partition** (A -> B). Its measured effect is small and can be within run-to-run noise (single runs, shared machine); load/rebuild cost dominates, not statement overhead.
- Parallelism only parallelises parse/validate (CPU-bound); SQLite has a single writer, so loads are serialised by design. Speed-up from C therefore depends on core count (this machine exposes 1 core(s)); on a 1-core box C cannot beat B.
- Tried and rejected (measured on a 15-partition run, no meaningful gain): `PRAGMA cache_size`/`temp_store=MEMORY` (+-2%, i.e. noise) and rewriting the fact rebuild from a window function to index seeks (~8% of that stage). Not worth the extra complexity.
- Next bottleneck: whichever stage above is larger. If it is the load, move to a server warehouse with bulk COPY/partition-swap; if parse, use `csv` -> `pyarrow` or compiled validation.
