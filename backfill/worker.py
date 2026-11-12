"""Extract + validate + transform ONE partition file. Pure CPU/IO, no database writes.

Runs inside worker processes (bounded pool), so everything crossing the process boundary is
plain picklable data. Reference-key sets are shipped once per worker via the pool initializer.
"""
from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

from .errors import SchemaError
from .schemas import ORDER_KNOWN_COLUMNS, ORDER_REQUIRED_COLUMNS, Reason
from .validate import PAYLOAD_START, validate_order

_CUSTOMERS: frozenset = frozenset()
_PRODUCTS: frozenset = frozenset()


def init_worker(customer_ids, product_ids) -> None:
    global _CUSTOMERS, _PRODUCTS
    _CUSTOMERS, _PRODUCTS = frozenset(customer_ids), frozenset(product_ids)


@dataclass
class PartitionResult:
    partition: str
    source_file: str
    checksum: str
    rows_read: int = 0
    events: list = field(default_factory=list)    # validate_order() tuple + (source_line,)
    rejects: list = field(default_factory=list)   # (reason, detail, source_line, raw_json)
    warnings: list = field(default_factory=list)
    duplicate_rows: int = 0
    late_rows: int = 0
    elapsed_s: float = 0.0

    @property
    def rows_accepted(self) -> int:
        return len(self.events)

    @property
    def rows_rejected(self) -> int:
        return len(self.rejects)


def process_partition(partition: str, path: str, checksum: str,
                      max_rows: Optional[int] = None) -> PartitionResult:
    """Parse one orders CSV. `max_rows` limits work (used for dry-run sampling).

    Raises SchemaError when the header violates the contract; the partition is then not loaded.
    """
    t0 = time.perf_counter()
    pdate = date.fromisoformat(partition)
    res = PartitionResult(partition=partition, source_file=str(path), checksum=checksum)
    seen: dict[tuple, tuple] = {}

    try:
        with open(path, newline="", encoding="utf-8-sig", errors="strict") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            if header is None:
                res.warnings.append("EMPTY_BATCH: file has no header or rows")
                res.elapsed_s = time.perf_counter() - t0
                return res
            header = [h.strip() for h in header]
            _check_header(header, res)
            ncols = len(header)

            for row in reader:
                if not row:                     # truly blank line: ignored, not counted
                    continue
                line_no = reader.line_num
                res.rows_read += 1
                if len(row) != ncols:
                    res.rejects.append((Reason.MALFORMED_ROW,
                                        f"expected {ncols} fields, got {len(row)}", line_no,
                                        json.dumps(row, ensure_ascii=False)))
                else:
                    rec = dict(zip(header, row))
                    event, rejection = validate_order(rec, pdate, _CUSTOMERS, _PRODUCTS)
                    if rejection is not None:
                        res.rejects.append((rejection[0], rejection[1], line_no,
                                            json.dumps(rec, ensure_ascii=False)))
                    else:
                        key, payload = event[:PAYLOAD_START], event[PAYLOAD_START:]
                        prior = seen.get(key)
                        if prior is None:
                            seen[key] = payload
                            res.events.append(event + (line_no,))
                            res.late_rows += event[-1]
                        else:
                            same = prior == payload
                            res.duplicate_rows += 1
                            res.rejects.append((
                                Reason.DUPLICATE_EVENT if same else Reason.CONFLICTING_DUPLICATE,
                                f"(order_id, event_ts)={key} already seen in this partition",
                                line_no, json.dumps(rec, ensure_ascii=False)))
                if max_rows and res.rows_read >= max_rows:
                    break
    except UnicodeDecodeError as exc:
        raise SchemaError(f"file is not valid UTF-8: {exc}") from exc

    if res.rows_read == 0:
        res.warnings.append("EMPTY_BATCH: header only, no data rows")
    res.elapsed_s = time.perf_counter() - t0
    return res


def _check_header(header: list[str], res: PartitionResult) -> None:
    if len(set(header)) != len(header):
        raise SchemaError(f"duplicate column names in header: {header}")
    missing = [c for c in ORDER_REQUIRED_COLUMNS if c not in header]
    unexpected = [c for c in header if c not in ORDER_KNOWN_COLUMNS]
    if missing:
        raise SchemaError(f"missing required column(s) {missing}; unexpected column(s) {unexpected}")
    if unexpected:
        res.warnings.append(f"UNEXPECTED_COLUMNS ignored: {unexpected}")
    absent_optional = [c for c in ("discount_pct", "currency") if c not in header]
    if absent_optional:
        res.warnings.append(f"OPTIONAL_COLUMNS_ABSENT defaults applied: {absent_optional}")
