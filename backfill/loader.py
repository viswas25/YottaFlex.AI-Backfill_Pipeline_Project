"""Idempotent per-partition load (partition overwrite) in ONE atomic transaction.

Why partition overwrite instead of plain INSERT/UPSERT:
  * re-running a partition replaces exactly that partition's rows -> no duplicates, ever;
  * a corrected source file removes events that no longer exist in it;
  * a crash before COMMIT leaves the previous state untouched (SQLite rollback).
The current-state table (fact_order) is then rebuilt only for the orders that were touched.
"""
from __future__ import annotations

from typing import Callable, Optional

from .common import reject_id
from .db import rebuild_fact_sql, utc_now
from .worker import PartitionResult

FaultInjector = Callable[[str], None]


def _chunks(seq, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def load_partition(conn, res: PartitionResult, run_id: str, batch_size: int = 5000,
                   fault: Optional[FaultInjector] = None) -> None:
    now = utc_now()
    p = res.partition
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_affected_orders(order_id TEXT PRIMARY KEY)")
        conn.execute("DELETE FROM temp_affected_orders")
        # orders previously loaded from this partition must be re-derived too
        conn.execute("INSERT OR IGNORE INTO temp_affected_orders "
                     "SELECT DISTINCT order_id FROM order_event WHERE source_partition=?", (p,))
        conn.execute("DELETE FROM order_event WHERE source_partition=?", (p,))
        conn.execute("DELETE FROM reject_record WHERE dataset='orders' AND source_partition=?", (p,))
        if fault:
            fault("after_delete")

        rows = [(p, e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[7], e[8], e[9], e[10], e[11], e[12],
                 res.source_file, e[13], run_id, now) for e in res.events]
        for chunk in _chunks(rows, batch_size):
            conn.executemany(
                "INSERT INTO order_event(source_partition, order_id, event_ts, customer_id, product_id,"
                " quantity, unit_price_cents, discount_bp, currency, status, order_ts, gross_cents,"
                " net_cents, is_late_arriving, source_file, source_line, run_id, loaded_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", chunk)
        if fault:
            fault("after_events_insert")

        rej_rows = [(reject_id("orders", p, res.source_file, line, reason, raw), "orders", p,
                     res.source_file, line, reason, detail, raw, run_id, now)
                    for reason, detail, line, raw in res.rejects]
        for chunk in _chunks(rej_rows, batch_size):
            conn.executemany(
                "INSERT INTO reject_record(reject_id, dataset, source_partition, source_file, "
                "source_line, reason_code, reason_detail, raw_record, run_id, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)", chunk)

        for chunk in _chunks([(e[0],) for e in res.events], batch_size):
            conn.executemany("INSERT OR IGNORE INTO temp_affected_orders VALUES (?)", chunk)
        conn.execute("DELETE FROM fact_order WHERE order_id IN (SELECT order_id FROM temp_affected_orders)")
        conn.execute(rebuild_fact_sql())
        if fault:
            fault("before_commit")
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
