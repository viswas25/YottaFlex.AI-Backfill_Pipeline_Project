"""SQLite target: DDL, connection helpers and run/partition bookkeeping."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

DDL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id   TEXT PRIMARY KEY CHECK (customer_id GLOB 'CUS-[0-9][0-9][0-9][0-9][0-9][0-9]'),
    customer_name TEXT NOT NULL,
    country       TEXT NOT NULL CHECK (length(country) = 2),
    signup_date   TEXT NOT NULL,
    source_file   TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id       TEXT PRIMARY KEY CHECK (product_id GLOB 'PRD-[0-9][0-9][0-9][0-9]'),
    product_name     TEXT NOT NULL,
    category         TEXT NOT NULL,
    list_price_cents INTEGER NOT NULL CHECK (list_price_cents >= 0),
    source_file      TEXT NOT NULL,
    run_id           TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

-- Append-only event history. One row per accepted source event; replaced per source partition.
CREATE TABLE IF NOT EXISTS order_event (
    source_partition TEXT    NOT NULL,
    order_id         TEXT    NOT NULL,
    event_ts         TEXT    NOT NULL,
    customer_id      TEXT    NOT NULL REFERENCES dim_customer(customer_id),
    product_id       TEXT    NOT NULL REFERENCES dim_product(product_id),
    quantity         INTEGER NOT NULL CHECK (quantity BETWEEN 1 AND 1000),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents > 0),
    discount_bp      INTEGER NOT NULL CHECK (discount_bp BETWEEN 0 AND 10000),
    currency         TEXT    NOT NULL CHECK (currency IN ('USD','EUR','GBP')),
    status           TEXT    NOT NULL CHECK (status IN ('CREATED','PAID','SHIPPED','CANCELLED','REFUNDED')),
    order_ts         TEXT    NOT NULL,
    gross_cents      INTEGER NOT NULL,
    net_cents        INTEGER NOT NULL,
    is_late_arriving INTEGER NOT NULL CHECK (is_late_arriving IN (0,1)),
    source_file      TEXT    NOT NULL,
    source_line      INTEGER NOT NULL,
    run_id           TEXT    NOT NULL,
    loaded_at        TEXT    NOT NULL,
    PRIMARY KEY (source_partition, order_id, event_ts)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_order_event_order ON order_event(order_id, event_ts);

-- Current state per order = latest event (ties broken by later source partition). Derived.
CREATE TABLE IF NOT EXISTS fact_order (
    order_id         TEXT PRIMARY KEY,
    customer_id      TEXT    NOT NULL REFERENCES dim_customer(customer_id),
    product_id       TEXT    NOT NULL REFERENCES dim_product(product_id),
    quantity         INTEGER NOT NULL,
    unit_price_cents INTEGER NOT NULL,
    discount_bp      INTEGER NOT NULL,
    currency         TEXT    NOT NULL,
    status           TEXT    NOT NULL,
    order_ts         TEXT    NOT NULL,
    order_date       TEXT    NOT NULL,
    event_ts         TEXT    NOT NULL,
    gross_cents      INTEGER NOT NULL,
    net_cents        INTEGER NOT NULL,
    version_count    INTEGER NOT NULL,
    source_partition TEXT    NOT NULL,
    source_file      TEXT    NOT NULL,
    run_id           TEXT    NOT NULL,
    loaded_at        TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fact_order_date ON fact_order(order_date);

CREATE TABLE IF NOT EXISTS reject_record (
    reject_id        TEXT PRIMARY KEY,
    dataset          TEXT NOT NULL,            -- orders | customers | products
    source_partition TEXT,                     -- NULL for dimension files
    source_file      TEXT NOT NULL,
    source_line      INTEGER NOT NULL,
    reason_code      TEXT NOT NULL,
    reason_detail    TEXT NOT NULL,
    raw_record       TEXT NOT NULL,
    run_id           TEXT NOT NULL,
    created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reject_partition ON reject_record(dataset, source_partition);

CREATE TABLE IF NOT EXISTS backfill_run (
    run_id                TEXT PRIMARY KEY,
    started_at            TEXT NOT NULL,
    ended_at              TEXT,
    status                TEXT NOT NULL,       -- RUNNING|SUCCEEDED|PARTIAL|FAILED|INTERRUPTED|ABANDONED
    params                TEXT NOT NULL,
    partitions_requested  INTEGER NOT NULL DEFAULT 0,
    partitions_succeeded  INTEGER NOT NULL DEFAULT 0,
    partitions_failed     INTEGER NOT NULL DEFAULT 0,
    partitions_skipped    INTEGER NOT NULL DEFAULT 0,
    rows_read             INTEGER NOT NULL DEFAULT 0,
    rows_accepted         INTEGER NOT NULL DEFAULT 0,
    rows_rejected         INTEGER NOT NULL DEFAULT 0,
    error_message         TEXT
);

-- One row per partition attempt (retries create new attempt numbers).
CREATE TABLE IF NOT EXISTS partition_run (
    run_id           TEXT NOT NULL,
    source_partition TEXT NOT NULL,
    attempt          INTEGER NOT NULL,
    status           TEXT NOT NULL,            -- RUNNING|SUCCEEDED|FAILED|SKIPPED|ABANDONED
    started_at       TEXT NOT NULL,
    ended_at         TEXT,
    duration_s       REAL,
    source_file      TEXT,
    source_checksum  TEXT,
    rows_read        INTEGER,
    rows_accepted    INTEGER,
    rows_rejected    INTEGER,
    duplicate_rows   INTEGER,
    late_rows        INTEGER,
    warnings         TEXT,
    error_type       TEXT,
    error_message    TEXT,
    PRIMARY KEY (run_id, source_partition, attempt)
);
CREATE INDEX IF NOT EXISTS idx_partition_run_part ON partition_run(source_partition, started_at);

-- Latest meaningful attempt per partition (SKIPPED / ABANDONED rows are bookkeeping only).
CREATE VIEW IF NOT EXISTS v_partition_progress AS
SELECT p.source_partition, p.status, p.run_id, p.attempt, p.rows_read, p.rows_accepted,
       p.rows_rejected, p.duplicate_rows, p.late_rows, p.duration_s, p.warnings,
       p.error_type, p.error_message, p.ended_at
FROM partition_run p
WHERE p.status IN ('RUNNING','SUCCEEDED','FAILED')
  AND p.rowid = (SELECT q.rowid FROM partition_run q
                 WHERE q.source_partition = p.source_partition
                   AND q.status IN ('RUNNING','SUCCEEDED','FAILED')
                 ORDER BY q.started_at DESC, q.rowid DESC LIMIT 1);

CREATE VIEW IF NOT EXISTS v_order_history AS
SELECT order_id, event_ts, status, net_cents, source_partition, is_late_arriving
FROM order_event;

CREATE VIEW IF NOT EXISTS v_daily_sales AS
SELECT order_date, currency, COUNT(*) AS orders, SUM(quantity) AS units,
       SUM(gross_cents) AS gross_cents, SUM(net_cents) AS net_cents
FROM fact_order
WHERE status NOT IN ('CANCELLED','REFUNDED')
GROUP BY order_date, currency;
"""

# columns copied from order_event into fact_order (latest event per order)
_REBUILD_FACT_SQL = """
INSERT INTO fact_order (order_id, customer_id, product_id, quantity, unit_price_cents,
    discount_bp, currency, status, order_ts, order_date, event_ts, gross_cents, net_cents,
    version_count, source_partition, source_file, run_id, loaded_at)
SELECT order_id, customer_id, product_id, quantity, unit_price_cents, discount_bp, currency,
       status, order_ts, substr(order_ts, 1, 10), event_ts, gross_cents, net_cents, cnt,
       source_partition, source_file, run_id, loaded_at
FROM (
    SELECT e.*,
           ROW_NUMBER() OVER (PARTITION BY order_id
                              ORDER BY event_ts DESC, source_partition DESC) AS rn,
           COUNT(*)     OVER (PARTITION BY order_id) AS cnt
    FROM order_event e
    WHERE e.order_id IN (SELECT order_id FROM temp_affected_orders)
)
WHERE rn = 1
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(path: Path | str, read_only: bool = False) -> sqlite3.Connection:
    """Autocommit connection (isolation_level=None): transactions are explicit BEGIN/COMMIT."""
    path = Path(path)
    if read_only:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, isolation_level=None)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, isolation_level=None, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)


def rebuild_fact_sql() -> str:
    return _REBUILD_FACT_SQL


# ---- run / partition bookkeeping (each statement autocommits on purpose, so FAILED/ABANDONED
# ---- states are durable even when the data transaction was rolled back) ------------------------

def start_run(conn, run_id: str, params: dict, partitions_requested: int) -> None:
    conn.execute(
        "INSERT INTO backfill_run(run_id, started_at, status, params, partitions_requested) "
        "VALUES (?,?,?,?,?)",
        (run_id, utc_now(), "RUNNING", json.dumps(params, default=str, sort_keys=True),
         partitions_requested))


def finish_run(conn, run_id: str, status: str, totals: dict, error: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE backfill_run SET ended_at=?, status=?, partitions_succeeded=?, partitions_failed=?,"
        " partitions_skipped=?, rows_read=?, rows_accepted=?, rows_rejected=?, error_message=? "
        "WHERE run_id=?",
        (utc_now(), status, totals.get("succeeded", 0), totals.get("failed", 0),
         totals.get("skipped", 0), totals.get("rows_read", 0), totals.get("rows_accepted", 0),
         totals.get("rows_rejected", 0), error, run_id))


def abandon_stale(conn, current_run_id: str) -> int:
    """Anything still RUNNING from an earlier process died mid-flight (crash/kill -9)."""
    n = conn.execute(
        "UPDATE partition_run SET status='ABANDONED', ended_at=?, "
        "error_type='Abandoned', error_message='process ended before attempt finished' "
        "WHERE status='RUNNING' AND run_id <> ?", (utc_now(), current_run_id)).rowcount
    conn.execute(
        "UPDATE backfill_run SET status='ABANDONED', ended_at=? WHERE status='RUNNING' AND run_id <> ?",
        (utc_now(), current_run_id))
    return n


def start_attempt(conn, run_id: str, partition: str, attempt: int, source_file: str, checksum: str):
    conn.execute(
        "INSERT INTO partition_run(run_id, source_partition, attempt, status, started_at, "
        "source_file, source_checksum) VALUES (?,?,?,?,?,?,?)",
        (run_id, partition, attempt, "RUNNING", utc_now(), source_file, checksum))


def finish_attempt(conn, run_id: str, partition: str, attempt: int, status: str, *,
                   duration_s: float, rows_read=None, rows_accepted=None, rows_rejected=None,
                   duplicate_rows=None, late_rows=None, warnings: Iterable[str] = (),
                   error_type: Optional[str] = None, error_message: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE partition_run SET status=?, ended_at=?, duration_s=?, rows_read=?, rows_accepted=?,"
        " rows_rejected=?, duplicate_rows=?, late_rows=?, warnings=?, error_type=?, error_message=? "
        "WHERE run_id=? AND source_partition=? AND attempt=?",
        (status, utc_now(), round(duration_s, 4), rows_read, rows_accepted, rows_rejected,
         duplicate_rows, late_rows, "; ".join(warnings) or None, error_type, error_message,
         run_id, partition, attempt))


def record_skip(conn, run_id: str, partition: str, source_file: str, checksum: str) -> None:
    now = utc_now()
    conn.execute(
        "INSERT INTO partition_run(run_id, source_partition, attempt, status, started_at, ended_at,"
        " duration_s, source_file, source_checksum) VALUES (?,?,?,?,?,?,?,?,?)",
        (run_id, partition, 0, "SKIPPED", now, now, 0.0, source_file, checksum))


def record_missing(conn, run_id: str, partition: str, source_file: str, message: str) -> None:
    now = utc_now()
    conn.execute(
        "INSERT INTO partition_run(run_id, source_partition, attempt, status, started_at, ended_at,"
        " duration_s, source_file, error_type, error_message) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (run_id, partition, 1, "FAILED", now, now, 0.0, source_file, "SourceMissingError", message))


def last_success_checksum(conn, partition: str) -> Optional[str]:
    row = conn.execute(
        "SELECT source_checksum FROM partition_run WHERE source_partition=? AND status='SUCCEEDED' "
        "ORDER BY started_at DESC, rowid DESC LIMIT 1", (partition,)).fetchone()
    return row[0] if row else None


def load_reference_keys(conn) -> tuple[frozenset, frozenset]:
    cust = frozenset(r[0] for r in conn.execute("SELECT customer_id FROM dim_customer"))
    prod = frozenset(r[0] for r in conn.execute("SELECT product_id FROM dim_product"))
    return cust, prod


def observed_throughput(conn, limit: int = 50) -> Optional[float]:
    """Median rows/sec from recent successful partition attempts (for dry-run estimates)."""
    rows = conn.execute(
        "SELECT rows_read * 1.0 / duration_s FROM partition_run WHERE status='SUCCEEDED' "
        "AND duration_s > 0 AND rows_read > 0 ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
    vals = sorted(r[0] for r in rows)
    return vals[len(vals) // 2] if vals else None
