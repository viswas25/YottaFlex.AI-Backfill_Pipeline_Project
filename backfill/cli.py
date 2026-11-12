"""Command line interface:  python -m backfill <command> ..."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

from . import datagen, db
from .orchestrator import BackfillConfig, dry_run, resolve_partitions, run_backfill
from .reconcile import reconcile, render_markdown
from .reports import run_sample_queries


def _d(s: str) -> date:
    return date.fromisoformat(s)


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="backfill", description=__doc__)
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="generate synthetic source data + labels")
    g.add_argument("--out", required=True)
    g.add_argument("--labels-out")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--start", type=_d, default=date(2024, 3, 1))
    g.add_argument("--days", type=int, default=7)
    g.add_argument("--orders-per-day", type=int, default=120)
    g.add_argument("--customers", type=int, default=200)
    g.add_argument("--products", type=int, default=40)
    g.add_argument("--defect-rate", type=float, default=0.06)
    g.add_argument("--no-special-partitions", action="store_true")

    b = sub.add_parser("backfill", help="backfill selected date partitions (or --dry-run)")
    b.add_argument("--db", required=True, type=Path)
    b.add_argument("--source", required=True, type=Path)
    b.add_argument("--start", type=_d)
    b.add_argument("--end", type=_d)
    b.add_argument("--dates", help="comma separated list of dates, e.g. 2024-03-01,2024-03-05")
    b.add_argument("--workers", type=int, default=4, help="max concurrent partitions (bounded)")
    b.add_argument("--max-retries", type=int, default=2)
    b.add_argument("--retry-backoff", type=float, default=0.5)
    b.add_argument("--batch-size", type=int, default=5000)
    b.add_argument("--force", action="store_true", help="re-run even if already loaded and unchanged")
    b.add_argument("--dry-run", action="store_true", help="estimate only; write nothing")
    b.add_argument("--simulate-failure", action="append", default=[], metavar="DATE[:N]",
                   help="demo/testing: fail the first N attempts of DATE mid-transaction")
    b.add_argument("--report-dir", type=Path)
    b.add_argument("--run-id")

    s = sub.add_parser("status", help="show partition progress and recent runs")
    s.add_argument("--db", required=True, type=Path)

    r = sub.add_parser("reconcile", help="run reconciliation checks, write report")
    r.add_argument("--db", required=True, type=Path)
    r.add_argument("--source", required=True, type=Path)
    r.add_argument("--labels", type=Path)
    r.add_argument("--out", type=Path, default=Path("reports"))

    q = sub.add_parser("report", help="run sample queries into a markdown report")
    q.add_argument("--db", required=True, type=Path)
    q.add_argument("--sql", type=Path, default=Path("sql/sample_queries.sql"))
    q.add_argument("--out", type=Path, default=Path("reports/sample_query_results.md"))
    return ap


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    _setup_logging(args.verbose)

    if args.cmd == "generate":
        m = datagen.generate(Path(args.out), Path(args.labels_out or f"{args.out}_labels"), args.seed,
                             args.start, args.days, args.orders_per_day, args.customers, args.products,
                             args.defect_rate, not args.no_special_partitions)
        print(f"generated {m['days']} partitions, {m['total_source_rows']} source rows -> {args.out}")
        return 0

    if args.cmd == "backfill":
        dates = [_d(x) for x in args.dates.split(",")] if args.dates else None
        try:
            parts = resolve_partitions(args.start, args.end, dates)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        sim = {}
        for item in args.simulate_failure:
            d, _, n = item.partition(":")
            sim[d] = int(n or 1)
        cfg = BackfillConfig(db_path=args.db, source_dir=args.source, partitions=parts, workers=args.workers,
                             max_retries=args.max_retries, retry_backoff_s=args.retry_backoff,
                             batch_size=args.batch_size, force=args.force, run_id=args.run_id,
                             simulate_failures=sim, report_dir=args.report_dir)
        if args.dry_run:
            rep = dry_run(cfg)
            print(json.dumps(rep, indent=2))
            if args.report_dir:
                args.report_dir.mkdir(parents=True, exist_ok=True)
                (args.report_dir / "dry_run_estimate.json").write_text(json.dumps(rep, indent=2))
            return 0
        summary = run_backfill(cfg)
        print(f"\nrun {summary.run_id}: {summary.status}  succeeded={summary.count('SUCCEEDED')} "
              f"skipped={summary.count('SKIPPED')} failed={summary.count('FAILED')}")
        for p in summary.partitions:
            if p.status == "FAILED":
                print(f"  FAILED {p.partition}: {p.error_type}: {p.error_message}")
        return summary.exit_code

    conn = db.connect(args.db, read_only=True)
    if args.cmd == "status":
        print(f"{'partition':<12} {'status':<10} {'read':>7} {'accepted':>9} {'rejected':>9} {'dup':>5} {'late':>5}  note")
        for r in conn.execute("SELECT * FROM v_partition_progress ORDER BY source_partition"):
            note = r["error_message"] or r["warnings"] or ""
            print(f"{r['source_partition']:<12} {r['status']:<10} {r['rows_read'] or 0:>7} "
                  f"{r['rows_accepted'] or 0:>9} {r['rows_rejected'] or 0:>9} {r['duplicate_rows'] or 0:>5} "
                  f"{r['late_rows'] or 0:>5}  {note[:70]}")
        print("\nrecent runs:")
        for r in conn.execute("SELECT run_id, status, partitions_succeeded ok, partitions_failed failed, "
                              "partitions_skipped skipped FROM backfill_run ORDER BY started_at DESC LIMIT 5"):
            print(f"  {r['run_id']}  {r['status']:<11} ok={r['ok']} failed={r['failed']} skipped={r['skipped']}")
        return 0

    if args.cmd == "reconcile":
        checks = reconcile(conn, args.source, args.labels)
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "reconciliation.md").write_text(render_markdown(checks))
        (args.out / "reconciliation.json").write_text(
            json.dumps([c.__dict__ for c in checks], indent=2))
        for c in checks:
            print(f"[{'PASS' if c.passed else 'FAIL'}] {c.name}: {c.detail}")
        return 0 if all(c.passed for c in checks) else 3

    if args.cmd == "report":
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(run_sample_queries(conn, args.sql))
        print(f"wrote {args.out}")
        return 0
    return 1
