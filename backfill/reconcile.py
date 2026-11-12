"""Reconciliation / validation report: proves the target matches the source and the labels.

Three independent evidence sources are compared:
  1. the source files re-counted with a plain csv reader (not the pipeline's code path),
  2. the target tables (order_event, fact_order, reject_record, partition_run),
  3. optional ground-truth labels written by the generator in a separate directory.
"""
from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .common import partition_path
from .schemas import Reason


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


def _source_row_count(path: Path) -> int:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        r = csv.reader(fh)
        next(r, None)
        return sum(1 for row in r if row)


def _read_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def reconcile(conn, source_dir: Path, labels_dir: Optional[Path] = None) -> list[Check]:
    checks: list[Check] = []
    add = lambda n, ok, d: checks.append(Check(n, bool(ok), d))  # noqa: E731
    q = lambda sql, *a: conn.execute(sql, a).fetchall()           # noqa: E731

    # loaded partitions = latest SUCCEEDED attempt per partition
    succ = {r["source_partition"]: r for r in q(
        "SELECT p.* FROM partition_run p WHERE p.status='SUCCEEDED' AND p.rowid = ("
        " SELECT q.rowid FROM partition_run q WHERE q.source_partition=p.source_partition "
        " AND q.status='SUCCEEDED' ORDER BY q.started_at DESC, q.rowid DESC LIMIT 1)")}
    add("partitions_loaded", True, f"{len(succ)} partition(s) loaded: {sorted(succ)}")

    # 1. run bookkeeping is complete
    stuck = q("SELECT COUNT(*) c FROM partition_run WHERE status='RUNNING'")[0]["c"]
    add("no_attempt_left_running", stuck == 0, f"{stuck} attempt(s) still RUNNING")

    # 2. source -> target counts, per partition
    bad = []
    for p, r in sorted(succ.items()):
        src_rows = _source_row_count(partition_path(source_dir, p))
        n_ev = q("SELECT COUNT(*) c FROM order_event WHERE source_partition=?", p)[0]["c"]
        n_rej = q("SELECT COUNT(*) c FROM reject_record WHERE dataset='orders' AND source_partition=?", p)[0]["c"]
        if not (src_rows == r["rows_read"] == n_ev + n_rej and n_ev == r["rows_accepted"]
                and n_rej == r["rows_rejected"]):
            bad.append(f"{p}: source={src_rows} read={r['rows_read']} events={n_ev} rejects={n_rej}")
    add("source_rows_equal_events_plus_rejects", not bad,
        "every partition: source rows == accepted events + rejects" if not bad else "; ".join(bad))

    # 3. keys + referential integrity
    dup_evt = q("SELECT COUNT(*) c FROM (SELECT 1 FROM order_event GROUP BY source_partition, order_id, "
                "event_ts HAVING COUNT(*)>1)")[0]["c"]
    dup_fact = q("SELECT COUNT(*) c FROM (SELECT 1 FROM fact_order GROUP BY order_id HAVING COUNT(*)>1)")[0]["c"]
    add("primary_keys_unique", dup_evt == 0 and dup_fact == 0,
        f"duplicate event keys={dup_evt}, duplicate fact keys={dup_fact}")
    orphans = q("SELECT (SELECT COUNT(*) FROM order_event e LEFT JOIN dim_customer c USING(customer_id) "
                "WHERE c.customer_id IS NULL) + (SELECT COUNT(*) FROM order_event e LEFT JOIN dim_product p "
                "USING(product_id) WHERE p.product_id IS NULL) + (SELECT COUNT(*) FROM fact_order f LEFT JOIN "
                "dim_customer c USING(customer_id) WHERE c.customer_id IS NULL) + (SELECT COUNT(*) FROM "
                "fact_order f LEFT JOIN dim_product p USING(product_id) WHERE p.product_id IS NULL) AS n")[0]["n"]
    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    add("referential_integrity", orphans == 0 and not fk, f"orphan rows={orphans}, fk violations={len(fk)}")

    # 4. fact_order == latest event per order (computed through a different SQL path)
    n_orders = q("SELECT COUNT(DISTINCT order_id) c FROM order_event")[0]["c"]
    n_fact = q("SELECT COUNT(*) c FROM fact_order")[0]["c"]
    mism = q("SELECT COUNT(*) c FROM fact_order f JOIN (SELECT order_id, MAX(event_ts) m FROM order_event "
             "GROUP BY order_id) x ON x.order_id=f.order_id WHERE f.event_ts <> x.m")[0]["c"]
    add("fact_is_latest_event_per_order", n_orders == n_fact and mism == 0,
        f"distinct orders in history={n_orders}, fact rows={n_fact}, stale rows={mism}")

    # 5. derived-field determinism
    bad_amt = q("SELECT COUNT(*) c FROM order_event WHERE gross_cents <> quantity*unit_price_cents "
                "OR net_cents <> (gross_cents*(10000-discount_bp)+5000)/10000 OR net_cents > gross_cents")[0]["c"]
    add("amounts_recompute_exactly", bad_amt == 0, f"{bad_amt} event row(s) whose gross/net do not recompute")

    # 6. aggregate reconciliation (history -> current state), per currency
    a = {r["currency"]: r["n"] for r in q("SELECT currency, SUM(net_cents) n FROM fact_order GROUP BY currency")}
    b = {r["currency"]: r["n"] for r in q(
        "SELECT e.currency, SUM(e.net_cents) n FROM order_event e WHERE e.event_ts = ("
        "SELECT MAX(x.event_ts) FROM order_event x WHERE x.order_id=e.order_id) GROUP BY e.currency")}
    add("net_amount_by_currency_reconciles", a == b, f"fact={a} history_latest={b}")

    # 7. rejects traceable to source
    rej = q("SELECT dataset, source_partition, source_file, source_line, reason_code, raw_record FROM reject_record")
    untraceable = [r for r in rej if not r["source_file"] or r["source_line"] < 2 or not r["raw_record"]
                   or not Path(r["source_file"]).exists()]
    unknown = {r["reason_code"] for r in rej} - Reason.ALL
    add("rejects_traceable_to_source", not untraceable and not unknown,
        f"{len(rej)} rejects; untraceable={len(untraceable)}; unknown reason codes={sorted(unknown)}")
    by_reason = Counter(r["reason_code"] for r in rej if r["dataset"] == "orders")
    add("reject_breakdown", True, json.dumps(dict(sorted(by_reason.items()))))

    # 8. ground-truth labels (only when provided)
    if labels_dir and Path(labels_dir).exists():
        L = Path(labels_dir)
        exp_parts = {r["partition"]: r for r in _read_csv(L / "expected_partitions.csv")}
        failed_expected = {p for p, r in exp_parts.items() if r["expected_status"] == "FAILED"}
        loaded_expected = {p for p in succ if p in exp_parts and exp_parts[p]["expected_status"] == "SUCCEEDED"}
        wrongly_loaded = sorted(set(succ) & failed_expected)
        add("labels_expected_failures_not_loaded", not wrongly_loaded,
            f"partitions expected to FAIL: {sorted(failed_expected)}; wrongly loaded: {wrongly_loaded}")

        exp_events = {(r["partition"], r["order_id"], r["event_ts"]): r
                      for r in _read_csv(L / "expected_accepted_events.csv") if r["partition"] in loaded_expected}
        got_events = {(r["source_partition"], r["order_id"], r["event_ts"]): r for r in q(
            "SELECT source_partition, order_id, event_ts, status, quantity, gross_cents, net_cents, currency, "
            "is_late_arriving, source_line FROM order_event")}
        missing, extra = set(exp_events) - set(got_events), set(got_events) - set(exp_events)
        diff_fields = 0
        for k in set(exp_events) & set(got_events):
            e, g = exp_events[k], got_events[k]
            if (e["status"], int(e["quantity"]), int(e["gross_cents"]), int(e["net_cents"]), e["currency"],
                    int(e["is_late_arriving"]), int(e["source_line"])) != (
                    g["status"], g["quantity"], g["gross_cents"], g["net_cents"], g["currency"],
                    g["is_late_arriving"], g["source_line"]):
                diff_fields += 1
        add("labels_accepted_events_match", not (missing or extra or diff_fields),
            f"expected={len(exp_events)} loaded={len(got_events)} missing={len(missing)} "
            f"unexpected={len(extra)} field_mismatches={diff_fields}")

        exp_rej = Counter((r["partition"], int(r["source_line"]), r["reason_code"])
                          for r in _read_csv(L / "expected_rejects.csv") if r["partition"] in loaded_expected)
        got_rej = Counter((r["source_partition"], r["source_line"], r["reason_code"])
                          for r in rej if r["dataset"] == "orders")
        add("labels_rejects_match", exp_rej == got_rej,
            f"expected={sum(exp_rej.values())} loaded={sum(got_rej.values())} "
            f"missing={sum((exp_rej - got_rej).values())} unexpected={sum((got_rej - exp_rej).values())}")

        # expected current state from labels: latest event per order
        latest: dict = {}
        for (p, oid, ts), r in exp_events.items():
            if oid not in latest or (ts, p) > (latest[oid][0], latest[oid][1]):
                latest[oid] = (ts, p, r)
        exp_net = defaultdict(int)
        for ts, p, r in latest.values():
            exp_net[r["currency"]] += int(r["net_cents"])
        exp_status = Counter(r["status"] for _, _, r in latest.values())
        got_status = Counter(r["status"] for r in q("SELECT status FROM fact_order"))
        add("labels_fact_orders_and_net_match",
            len(latest) == n_fact and dict(exp_net) == a and exp_status == got_status,
            f"expected orders={len(latest)} fact={n_fact}; expected net={dict(exp_net)} fact={a}; "
            f"status mix equal={exp_status == got_status}")

        exp_dim = Counter((r["dataset"], int(r["source_line"]), r["reason_code"])
                          for r in _read_csv(L / "expected_dimension_rejects.csv"))
        got_dim = Counter((r["dataset"], r["source_line"], r["reason_code"]) for r in rej
                          if r["dataset"] in ("customers", "products"))
        add("labels_dimension_rejects_match", exp_dim == got_dim, f"expected={dict(exp_dim)} loaded={dict(got_dim)}")
    else:
        add("labels_comparison", True, "skipped (no labels directory supplied)")
    return checks


def render_markdown(checks: list[Check], title: str = "Reconciliation report") -> str:
    ok = all(c.passed for c in checks)
    lines = [f"# {title}", "", f"**Overall: {'PASS' if ok else 'FAIL'}** "
             f"({sum(c.passed for c in checks)}/{len(checks)} checks)", "",
             "| # | Check | Result | Detail |", "|---|-------|--------|--------|"]
    for i, c in enumerate(checks, 1):
        lines.append(f"| {i} | `{c.name}` | {'PASS' if c.passed else '**FAIL**'} | {c.detail} |")
    return "\n".join(lines) + "\n"
