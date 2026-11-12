"""Backfill orchestration: planning, dry-run, bounded parallelism, retry, restart, progress."""
from __future__ import annotations

import json
import logging
import math
import os
import time
import uuid
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from . import db, dimensions
from .common import file_checksum, partition_path
from .errors import PermanentError, SchemaError, TransientError
from .loader import load_partition
from .schemas import FAILED, SKIPPED, SUCCEEDED
from .worker import init_worker, process_partition

log = logging.getLogger("backfill")

DEFAULT_THROUGHPUT_ROWS_S = 12_000.0   # used by dry-run until real runs calibrate it


@dataclass
class BackfillConfig:
    db_path: Path
    source_dir: Path
    partitions: list[date]
    workers: int = 4                       # hard cap on concurrent partition workers
    max_retries: int = 2                   # retries AFTER the first attempt (transient errors only)
    retry_backoff_s: float = 0.5           # doubles each retry
    batch_size: int = 5000
    force: bool = False                    # re-run partitions even if already SUCCEEDED + unchanged
    run_id: Optional[str] = None
    simulate_failures: dict = field(default_factory=dict)   # {"YYYY-MM-DD": n_failing_attempts}
    report_dir: Optional[Path] = None
    dry_run_sample: int = 2000


@dataclass
class PartitionOutcome:
    partition: str
    status: str
    attempts: int = 0
    rows_read: int = 0
    rows_accepted: int = 0
    rows_rejected: int = 0
    duplicate_rows: int = 0
    late_rows: int = 0
    duration_s: float = 0.0
    warnings: list = field(default_factory=list)
    error_type: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class RunSummary:
    run_id: str
    status: str
    started_at: str
    ended_at: str
    duration_s: float
    partitions: list[PartitionOutcome]
    abandoned_attempts_cleaned: int = 0
    dimensions: dict = field(default_factory=dict)
    error: Optional[str] = None

    def count(self, status: str) -> int:
        return sum(1 for p in self.partitions if p.status == status)

    @property
    def exit_code(self) -> int:
        return {"SUCCEEDED": 0, "PARTIAL": 2, "FAILED": 2}.get(self.status, 1)


def resolve_partitions(start: Optional[date], end: Optional[date],
                       dates: Optional[list[date]]) -> list[date]:
    if dates:
        return sorted(set(dates))
    if start is None or end is None:
        raise ValueError("provide --start and --end, or --dates")
    if start > end:
        raise ValueError("--start must be <= --end")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def new_run_id() -> str:
    return "run-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:4]


class _InlineExecutor:
    """workers == 1: run synchronously in-process (simple, debuggable, no pickling)."""
    def submit(self, fn, *a, **kw) -> Future:
        fut: Future = Future()
        try:
            fut.set_result(fn(*a, **kw))
        except BaseException as exc:  # noqa: BLE001 - propagated through future
            fut.set_exception(exc)
        return fut
    def __enter__(self): return self
    def __exit__(self, *exc): return False


# --------------------------------------------------------------------------------------------
# dry run
# --------------------------------------------------------------------------------------------

def _count_lines(path: Path) -> int:
    n = 0
    with open(path, "rb") as fh:
        while block := fh.read(1 << 20):
            n += block.count(b"\n")
    return n


def dry_run(cfg: BackfillConfig) -> dict:
    """Estimate a backfill without writing anything (the target DB is opened read-only)."""
    conn = None
    if Path(cfg.db_path).exists():
        conn = db.connect(cfg.db_path, read_only=True)
    throughput, source = DEFAULT_THROUGHPUT_ROWS_S, "default"
    if conn is not None:
        try:
            observed = db.observed_throughput(conn)
        except Exception:  # schema not initialised yet
            observed = None
        if observed:
            throughput, source = observed, "calibrated from previous runs"

    cust = {r[0] for r in dimensions.parse_dimension(cfg.source_dir, "customers")[0]}
    prod = {r[0] for r in dimensions.parse_dimension(cfg.source_dir, "products")[0]}
    init_worker(cust, prod)

    plan, total_rows, total_bytes, total_cpu = [], 0, 0, 0.0
    for p in cfg.partitions:
        iso, path = p.isoformat(), partition_path(cfg.source_dir, p)
        entry = {"partition": iso, "source_file": str(path)}
        if not path.exists():
            entry.update(action="WOULD_FAIL", reason="SOURCE_MISSING", est_rows=0, bytes=0,
                         est_seconds=0.0)
            plan.append(entry)
            continue
        size = path.stat().st_size
        est_rows = max(_count_lines(path) - 1, 0)
        checksum = file_checksum(path)
        prior = None
        if conn is not None:
            try:
                prior = db.last_success_checksum(conn, iso)
            except Exception:
                prior = None
        entry.update(bytes=size, est_rows=est_rows)
        if prior == checksum and not cfg.force:
            entry.update(action="SKIP", reason="already loaded, source unchanged", est_seconds=0.0)
        else:
            try:
                sample = process_partition(iso, str(path), checksum, max_rows=cfg.dry_run_sample)
                rr = sample.rows_rejected / sample.rows_read if sample.rows_read else 0.0
                entry.update(action="RUN", est_reject_rate=round(rr, 4),
                             est_accepted=int(est_rows * (1 - rr)),
                             est_seconds=round(est_rows / throughput, 3),
                             warnings=sample.warnings)
                total_rows += est_rows
                total_cpu += est_rows / throughput
            except SchemaError as exc:
                entry.update(action="WOULD_FAIL", reason=f"SCHEMA_ERROR: {exc}", est_seconds=0.0)
        total_bytes += size
        plan.append(entry)
    workers = max(1, cfg.workers)
    longest = max((e.get("est_seconds", 0.0) for e in plan), default=0.0)
    report = {
        "mode": "dry-run (no writes)",
        "partitions_requested": len(plan),
        "would_run": sum(e["action"] == "RUN" for e in plan),
        "would_skip": sum(e["action"] == "SKIP" for e in plan),
        "would_fail": sum(e["action"] == "WOULD_FAIL" for e in plan),
        "est_source_rows_to_process": total_rows,
        "total_source_bytes": total_bytes,
        "assumed_throughput_rows_per_s": round(throughput, 1),
        "throughput_source": source,
        "workers": workers,
        "est_cpu_seconds": round(total_cpu, 2),
        "est_wall_seconds": round(max(longest, total_cpu / workers), 2),
        "partitions": plan,
    }
    if conn is not None:
        conn.close()
    return report


# --------------------------------------------------------------------------------------------
# real run
# --------------------------------------------------------------------------------------------

def run_backfill(cfg: BackfillConfig) -> RunSummary:
    t_run = time.perf_counter()
    started = db.utc_now()
    run_id = cfg.run_id or new_run_id()
    conn = db.connect(cfg.db_path)
    db.init_schema(conn)
    cleaned = db.abandon_stale(conn, run_id)
    if cleaned:
        log.warning("run=%s marked %d stale RUNNING attempt(s) from a crashed run as ABANDONED",
                    run_id, cleaned)
    params = {k: v for k, v in asdict(cfg).items() if k not in ("partitions",)}
    params["partitions"] = [p.isoformat() for p in cfg.partitions]
    db.start_run(conn, run_id, params, len(cfg.partitions))
    log.info("run=%s START partitions=%d workers=%d force=%s", run_id, len(cfg.partitions),
             cfg.workers, cfg.force)

    outcomes: dict[str, PartitionOutcome] = {}
    summary_error = None
    dim_stats: dict = {}
    status = "FAILED"
    try:
        try:
            dim_stats = dimensions.load_dimensions(conn, run_id, cfg.source_dir)
            log.info("run=%s dimensions loaded %s", run_id, dim_stats)
        except PermanentError as exc:
            summary_error = f"dimension load failed: {exc}"
            log.error("run=%s %s", run_id, summary_error)
            raise

        cust, prod = db.load_reference_keys(conn)
        todo = _plan(conn, cfg, run_id, outcomes)
        _execute(conn, cfg, run_id, todo, cust, prod, outcomes)
        failed = sum(o.status == FAILED for o in outcomes.values())
        ok = sum(o.status in (SUCCEEDED, SKIPPED) for o in outcomes.values())
        status = "SUCCEEDED" if failed == 0 else ("PARTIAL" if ok else "FAILED")
    except PermanentError:
        status = "FAILED"
    except BaseException as exc:  # includes KeyboardInterrupt: leave a trustworthy audit trail
        status = "INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "FAILED"
        summary_error = summary_error or f"{type(exc).__name__}: {exc}"
        conn.execute("UPDATE partition_run SET status='ABANDONED', ended_at=?, error_type=?, "
                     "error_message='run interrupted' WHERE run_id=? AND status='RUNNING'",
                     (db.utc_now(), type(exc).__name__, run_id))
        db.finish_run(conn, run_id, status, _totals(outcomes), summary_error)
        conn.close()
        raise
    totals = _totals(outcomes)
    db.finish_run(conn, run_id, status, totals, summary_error)
    conn.close()

    summary = RunSummary(
        run_id=run_id, status=status, started_at=started, ended_at=db.utc_now(),
        duration_s=round(time.perf_counter() - t_run, 3),
        partitions=[outcomes[k] for k in sorted(outcomes)], abandoned_attempts_cleaned=cleaned,
        dimensions=dim_stats, error=summary_error)
    log.info("run=%s END status=%s ok=%d skipped=%d failed=%d read=%d accepted=%d rejected=%d in %.2fs",
             run_id, status, summary.count(SUCCEEDED), summary.count(SKIPPED), summary.count(FAILED),
             totals["rows_read"], totals["rows_accepted"], totals["rows_rejected"], summary.duration_s)
    if cfg.report_dir:
        Path(cfg.report_dir).mkdir(parents=True, exist_ok=True)
        out = Path(cfg.report_dir) / f"run_{run_id}.json"
        out.write_text(json.dumps(asdict(summary), indent=2, default=str))
    return summary


def _totals(outcomes: dict) -> dict:
    ok = [o for o in outcomes.values() if o.status == SUCCEEDED]
    return {
        "succeeded": len(ok),
        "failed": sum(o.status == FAILED for o in outcomes.values()),
        "skipped": sum(o.status == SKIPPED for o in outcomes.values()),
        "rows_read": sum(o.rows_read for o in ok),
        "rows_accepted": sum(o.rows_accepted for o in ok),
        "rows_rejected": sum(o.rows_rejected for o in ok),
    }


def _plan(conn, cfg, run_id, outcomes) -> deque:
    """Decide per partition: SKIP (restart safety), FAIL (source missing) or RUN."""
    todo: deque = deque()
    for p in cfg.partitions:
        iso, path = p.isoformat(), partition_path(cfg.source_dir, p)
        if not path.exists():
            msg = f"source file not found: {path}"
            db.record_missing(conn, run_id, iso, str(path), msg)
            outcomes[iso] = PartitionOutcome(iso, FAILED, 1, error_type="SourceMissingError",
                                             error_message=msg)
            log.error("run=%s partition=%s FAILED %s", run_id, iso, msg)
            continue
        checksum = file_checksum(path)
        if not cfg.force and db.last_success_checksum(conn, iso) == checksum:
            db.record_skip(conn, run_id, iso, str(path), checksum)
            outcomes[iso] = PartitionOutcome(iso, SKIPPED)
            log.info("run=%s partition=%s SKIPPED (already loaded, source unchanged)", run_id, iso)
            continue
        todo.append((p, path, checksum))
    return todo


def _execute(conn, cfg, run_id, todo: deque, cust, prod, outcomes) -> None:
    workers = max(1, cfg.workers)
    total = len(todo)
    attempts: dict[str, int] = {}
    inflight: dict[Future, tuple] = {}
    done_count = 0
    if workers == 1:
        init_worker(cust, prod)
        executor = _InlineExecutor()
    else:
        executor = ProcessPoolExecutor(max_workers=workers, initializer=init_worker,
                                       initargs=(cust, prod))
    with executor as pool:
        while todo or inflight:
            # bounded parallelism: never more than `workers` partitions in flight
            while todo and len(inflight) < workers:
                task = todo.popleft()
                p, path, checksum = task
                iso = p.isoformat()
                attempts[iso] = attempts.get(iso, 0) + 1
                db.start_attempt(conn, run_id, iso, attempts[iso], str(path), checksum)
                t0 = time.perf_counter()
                fut = pool.submit(process_partition, iso, str(path), checksum)
                inflight[fut] = (task, attempts[iso], t0)
            done, _ = wait(inflight, return_when=FIRST_COMPLETED)
            for fut in done:
                task, attempt, t0 = inflight.pop(fut)
                p, path, checksum = task
                iso = p.isoformat()
                try:
                    res = fut.result()
                    fault = _make_fault(cfg, iso, attempt)
                    load_partition(conn, res, run_id, cfg.batch_size, fault)
                except Exception as exc:  # noqa: BLE001
                    retry = _record_failure(conn, cfg, run_id, iso, attempt, t0, exc, outcomes)
                    if retry:
                        todo.appendleft(task)
                        time.sleep(cfg.retry_backoff_s * (2 ** (attempt - 1)))
                    else:
                        done_count += 1
                        log.error("run=%s [%d/%d] partition=%s FAILED attempts=%d %s: %s", run_id,
                                  done_count, total, iso, attempt, type(exc).__name__, exc)
                    continue
                dur = time.perf_counter() - t0
                db.finish_attempt(conn, run_id, iso, attempt, SUCCEEDED, duration_s=dur,
                                  rows_read=res.rows_read, rows_accepted=res.rows_accepted,
                                  rows_rejected=res.rows_rejected, duplicate_rows=res.duplicate_rows,
                                  late_rows=res.late_rows, warnings=res.warnings)
                outcomes[iso] = PartitionOutcome(
                    iso, SUCCEEDED, attempt, res.rows_read, res.rows_accepted, res.rows_rejected,
                    res.duplicate_rows, res.late_rows, round(dur, 4), list(res.warnings))
                done_count += 1
                log.info("run=%s [%d/%d] partition=%s SUCCEEDED attempt=%d read=%d accepted=%d "
                         "rejected=%d dup=%d late=%d %.2fs%s", run_id, done_count, total, iso, attempt,
                         res.rows_read, res.rows_accepted, res.rows_rejected, res.duplicate_rows,
                         res.late_rows, dur, f" warnings={res.warnings}" if res.warnings else "")


def _make_fault(cfg: BackfillConfig, iso: str, attempt: int):
    n = int(cfg.simulate_failures.get(iso, 0))
    if attempt > n:
        return None
    def fault(stage: str) -> None:
        if stage == "after_events_insert":     # mid-transaction: proves rollback
            raise TransientError(f"simulated failure (attempt {attempt}/{n}) at {stage}")
    return fault


def _record_failure(conn, cfg, run_id, iso, attempt, t0, exc, outcomes) -> bool:
    """Persist the failed attempt; return True when the partition should be retried."""
    permanent = isinstance(exc, PermanentError)
    will_retry = (not permanent) and attempt <= cfg.max_retries
    dur = time.perf_counter() - t0
    db.finish_attempt(conn, run_id, iso, attempt, FAILED, duration_s=dur,
                      error_type=type(exc).__name__, error_message=str(exc)[:1000])
    if will_retry:
        log.warning("run=%s partition=%s attempt %d failed (%s: %s) -> retrying", run_id, iso,
                    attempt, type(exc).__name__, exc)
    else:
        outcomes[iso] = PartitionOutcome(iso, FAILED, attempt, duration_s=round(dur, 4),
                                         error_type=type(exc).__name__, error_message=str(exc)[:1000])
    return will_retry
