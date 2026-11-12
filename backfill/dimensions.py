"""Customer / product reference data: parse+validate (pure) and upsert (SCD type 1)."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Callable

from .common import reject_id
from .db import utc_now
from .errors import SchemaError, SourceMissingError
from .schemas import CUSTOMER_COLUMNS, PRODUCT_COLUMNS, Reason
from .validate import validate_customer, validate_product

SPEC = {
    "customers": ("customers.csv", CUSTOMER_COLUMNS, validate_customer),
    "products": ("products.csv", PRODUCT_COLUMNS, validate_product),
}


def parse_dimension(source_dir: Path, dataset: str) -> tuple[list[tuple], list[tuple], str]:
    """Returns (valid_rows, rejects[(reason, detail, line, raw)], file_path). First key wins."""
    filename, columns, validator = SPEC[dataset]
    path = Path(source_dir) / filename
    if not path.exists():
        raise SourceMissingError(f"dimension file not found: {path}")
    valid, rejects, seen = [], [], set()
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header = [h.strip() for h in (next(reader, None) or [])]
        missing = [c for c in columns if c not in header]
        if missing:
            raise SchemaError(f"{filename}: missing required column(s) {missing}")
        for row in reader:
            if not row:
                continue
            line = reader.line_num
            if len(row) != len(header):
                rejects.append((Reason.MALFORMED_ROW, f"expected {len(header)} fields, got {len(row)}",
                                line, json.dumps(row)))
                continue
            rec = dict(zip(header, row))
            parsed, rej = validator(rec)
            if rej:
                rejects.append((rej[0], rej[1], line, json.dumps(rec)))
            elif parsed[0] in seen:
                rejects.append((Reason.DUPLICATE_KEY, f"key {parsed[0]} already defined earlier in file",
                                line, json.dumps(rec)))
            else:
                seen.add(parsed[0])
                valid.append(parsed)
    return valid, rejects, str(path)


def load_dimensions(conn, run_id: str, source_dir: Path) -> dict:
    """Upsert both dimensions (idempotent) and replace their reject rows. One transaction."""
    now = utc_now()
    stats = {}
    parsed = {ds: parse_dimension(source_dir, ds) for ds in SPEC}
    conn.execute("BEGIN IMMEDIATE")
    try:
        for dataset, (valid, rejects, path) in parsed.items():
            if dataset == "customers":
                conn.executemany(
                    "INSERT INTO dim_customer(customer_id, customer_name, country, signup_date, "
                    "source_file, run_id, updated_at) VALUES (?,?,?,?,?,?,?) "
                    "ON CONFLICT(customer_id) DO UPDATE SET customer_name=excluded.customer_name, "
                    "country=excluded.country, signup_date=excluded.signup_date, "
                    "source_file=excluded.source_file, run_id=excluded.run_id, "
                    "updated_at=excluded.updated_at "
                    "WHERE (customer_name, country, signup_date) IS NOT "
                    "(excluded.customer_name, excluded.country, excluded.signup_date)",
                    [r + (path, run_id, now) for r in valid])
            else:
                conn.executemany(
                    "INSERT INTO dim_product(product_id, product_name, category, list_price_cents, "
                    "source_file, run_id, updated_at) VALUES (?,?,?,?,?,?,?) "
                    "ON CONFLICT(product_id) DO UPDATE SET product_name=excluded.product_name, "
                    "category=excluded.category, list_price_cents=excluded.list_price_cents, "
                    "source_file=excluded.source_file, run_id=excluded.run_id, "
                    "updated_at=excluded.updated_at "
                    "WHERE (product_name, category, list_price_cents) IS NOT "
                    "(excluded.product_name, excluded.category, excluded.list_price_cents)",
                    [r + (path, run_id, now) for r in valid])
            conn.execute("DELETE FROM reject_record WHERE dataset=?", (dataset,))
            conn.executemany(
                "INSERT INTO reject_record(reject_id, dataset, source_partition, source_file, "
                "source_line, reason_code, reason_detail, raw_record, run_id, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                [(reject_id(dataset, None, path, line, reason, raw), dataset, None, path, line,
                  reason, detail, raw, run_id, now) for reason, detail, line, raw in rejects])
            stats[dataset] = {"valid": len(valid), "rejected": len(rejects)}
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return stats
