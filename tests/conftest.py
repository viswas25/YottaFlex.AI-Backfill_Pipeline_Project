"""Shared helpers: tiny hand-written datasets so every test states its own expectations."""
from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pytest

from backfill import db
from backfill.orchestrator import BackfillConfig, run_backfill

HEADER = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "discount_pct",
          "currency", "status", "order_ts", "event_ts"]


def write_dims(src: Path, customers=("CUS-000001", "CUS-000002"), products=("PRD-0001", "PRD-0002")):
    src.mkdir(parents=True, exist_ok=True)
    with open(src / "customers.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["customer_id", "customer_name", "country", "signup_date"])
        for c in customers:
            w.writerow([c, f"Name {c}", "US", "2023-01-01"])
    with open(src / "products.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["product_id", "product_name", "category", "list_price"])
        for p in products:
            w.writerow([p, f"Prod {p}", "toys", "10.00"])


def order(oid="ORD-00000001", cust="CUS-000001", prod="PRD-0001", qty="2", price="10.00", disc="",
          cur="USD", status="CREATED", order_ts="2024-03-01T10:00:00Z", event_ts="2024-03-01T10:00:00Z"):
    return [oid, cust, prod, qty, price, disc, cur, status, order_ts, event_ts]


def write_partition(src: Path, day: str, rows, header=None, raw: str | None = None):
    d = src / "orders" / f"dt={day}"
    d.mkdir(parents=True, exist_ok=True)
    p = d / "orders.csv"
    if raw is not None:
        p.write_text(raw)
        return p
    with open(p, "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header or HEADER)
        w.writerows(rows)
    return p


def cfg_for(tmp: Path, days, **kw) -> BackfillConfig:
    parts = [date.fromisoformat(d) for d in days]
    kw.setdefault("workers", 1)
    kw.setdefault("retry_backoff_s", 0.0)
    return BackfillConfig(db_path=tmp / "wh.db", source_dir=tmp / "src", partitions=parts, **kw)


def run(tmp: Path, days, **kw):
    return run_backfill(cfg_for(tmp, days, **kw))


def conn_for(tmp: Path):
    return db.connect(tmp / "wh.db")


def business_hash(conn) -> list:
    """Fact content without audit columns (run_id/loaded_at) - used to prove idempotency."""
    return [tuple(r) for r in conn.execute(
        "SELECT order_id, customer_id, product_id, quantity, unit_price_cents, discount_bp, currency, "
        "status, order_ts, event_ts, gross_cents, net_cents, version_count, source_partition "
        "FROM fact_order ORDER BY order_id")]


@pytest.fixture
def env(tmp_path):
    write_dims(tmp_path / "src")
    return tmp_path
