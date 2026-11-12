"""Deterministic synthetic data generator (fixed seed) + separate expected-answer labels.

Source files go to <out>/ ; the ground-truth labels go to a DIFFERENT directory so the labels
never sit next to the input. Labels are derived from how each row was *constructed* (and from a
tiny independent re-implementation of the money maths), not by running the pipeline.

Injected: normal rows, boundary values (qty 1/1000, price 0.01, discount 0/100), exact and
conflicting duplicates, 15 kinds of invalid rows, late-arriving + out-of-order events, mixed
timestamp formats/offsets, an empty partition, an unexpected extra column, absent optional
columns, and one schema-drift partition (renamed required column) that must fail.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

BASE_COLUMNS = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "discount_pct",
                "currency", "status", "order_ts", "event_ts"]

FIRST = ["Ava", "Liam", "Noah", "Mia", "Zoe", "Ethan", "Lucas", "Emma", "Ivy", "Omar", "Priya",
         "Chen", "Sofia", "Diego", "Aisha", "Kenji", "Lena", "Mateo", "Nina", "Raj"]
LAST = ["Smith", "Garcia", "Patel", "Kim", "Nguyen", "Muller", "Rossi", "Silva", "Khan", "Brown"]
COUNTRIES = ["US", "GB", "DE", "FR", "IN", "SG", "BR", "JP", "CA", "AU"]
CATEGORIES = ["books", "toys", "tools", "garden", "kitchen", "sports", "office"]
LIFECYCLES = [
    (["CREATED"], 15), (["CREATED", "PAID"], 20), (["CREATED", "PAID", "SHIPPED"], 38),
    (["CREATED", "CANCELLED"], 8), (["CREATED", "PAID", "CANCELLED"], 5),
    (["CREATED", "PAID", "REFUNDED"], 5), (["CREATED", "PAID", "SHIPPED", "REFUNDED"], 9),
]
INVALID_KINDS = [
    ("missing_required", "MISSING_REQUIRED"), ("bad_order_id", "INVALID_FORMAT"),
    ("bad_qty_type", "INVALID_TYPE"), ("bad_price_type", "INVALID_TYPE"),
    ("qty_range", "OUT_OF_RANGE"), ("price_range", "OUT_OF_RANGE"),
    ("discount_range", "OUT_OF_RANGE"), ("bad_status", "INVALID_ENUM"),
    ("bad_currency", "INVALID_ENUM"), ("bad_event_ts", "INVALID_TIMESTAMP"),
    ("future_ts", "FUTURE_TIMESTAMP"), ("event_before_order", "OUT_OF_RANGE"),
    ("bad_customer_ref", "INVALID_REFERENCE"), ("bad_product_ref", "INVALID_REFERENCE"),
    ("malformed_row", "MALFORMED_ROW"),
]


def _iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _render_ts(dt: datetime, rng: random.Random) -> str:
    r = rng.random()
    if r < 0.70:
        return _iso_z(dt)
    if r < 0.85:
        return dt.strftime("%Y-%m-%dT%H:%M:%S") + "+00:00"
    if r < 0.95:
        tz = timezone(timedelta(hours=5, minutes=30))
        return dt.astimezone(tz).isoformat()
    return dt.strftime("%Y-%m-%d %H:%M:%S")      # naive -> interpreted as UTC


def _money(cents: int, rng: random.Random) -> str:
    return f"{cents // 100}" if cents % 100 == 0 and rng.random() < 0.3 else f"{cents / 100:.2f}"


def _net(gross: int, bp: int) -> int:   # independent re-implementation (half-up, integers)
    return (gross * (10_000 - bp) + 5_000) // 10_000


@dataclass
class Entry:
    kind: str                      # valid | dup | conflict | invalid
    fields: dict
    key: tuple = ()
    payload: tuple = ()
    label: dict = field(default_factory=dict)
    reason: str = ""
    raw_row: list | None = None    # only for malformed rows


def generate(out_dir: Path, labels_dir: Path, seed: int = 42, start: date = date(2024, 3, 1),
             days: int = 7, orders_per_day: int = 120, n_customers: int = 200,
             n_products: int = 40, defect_rate: float = 0.06, special_partitions: bool = True) -> dict:
    rng = random.Random(seed)
    out_dir, labels_dir = Path(out_dir), Path(labels_dir)
    (out_dir / "orders").mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)

    # ---------------- dimensions (+ deliberately bad rows, labelled) ----------------
    cust_ids = [f"CUS-{i:06d}" for i in range(1, n_customers + 1)]
    prod_ids = [f"PRD-{i:04d}" for i in range(1, n_products + 1)]
    cust_rows = [[c, f"{rng.choice(FIRST)} {rng.choice(LAST)}", rng.choice(COUNTRIES),
                  (date(2020, 1, 1) + timedelta(days=rng.randrange(1400))).isoformat()]
                 for c in cust_ids]
    prices = {p: rng.randrange(199, 49_999) for p in prod_ids}
    prod_rows = [[p, f"Product {p[-4:]}", rng.choice(CATEGORIES), f"{prices[p] / 100:.2f}"]
                 for p in prod_ids]
    dim_rejects = []
    cust_rows.append([cust_ids[0], "Duplicate Person", "US", "2021-01-01"])
    dim_rejects.append(("customers", len(cust_rows) + 1, "DUPLICATE_KEY"))
    cust_rows.append(["CUS-900001", "Bad Country", "USA", "2021-01-01"])
    dim_rejects.append(("customers", len(cust_rows) + 1, "INVALID_FORMAT"))
    prod_rows.append([prod_ids[0], "Duplicate Product", "toys", "1.00"])
    dim_rejects.append(("products", len(prod_rows) + 1, "DUPLICATE_KEY"))
    prod_rows.append(["PRD-9001", "Free Thing", "toys", "free"])
    dim_rejects.append(("products", len(prod_rows) + 1, "INVALID_TYPE"))
    _write_csv(out_dir / "customers.csv", ["customer_id", "customer_name", "country", "signup_date"], cust_rows)
    _write_csv(out_dir / "products.csv", ["product_id", "product_name", "category", "list_price"], prod_rows)

    # ---------------- special partition roles ----------------
    roles = {}
    if special_partitions:
        for idx, role in ((1, "empty"), (3, "extra_column"), (4, "no_optional_columns"), (5, "schema_drift")):
            if idx < days:
                roles[idx] = role

    # ---------------- build per-partition entries ----------------
    seq = [0]
    def next_order_id() -> str:
        seq[0] += 1
        return f"ORD-{seq[0]:08d}"

    per_part: dict[int, list[Entry]] = {d: [] for d in range(days)}

    def new_spec(day: int) -> dict:
        t0 = datetime.combine(start + timedelta(days=day), datetime.min.time(), tzinfo=timezone.utc) \
            + timedelta(seconds=rng.randrange(0, 86_400 - 3600))
        q = rng.choice([1, 1, 2, 2, 3, 4, 5, 10]) if rng.random() > 0.01 else rng.choice([1, 1000])
        pid = rng.choice(prod_ids)
        price = prices[pid] if rng.random() > 0.01 else rng.choice([1, 9_999_999])
        r = rng.random()
        bp = 0 if r < 0.45 else rng.choice([500, 1000, 1050, 1500, 2000, 2500]) if r < 0.93 \
            else rng.choice([0, 10_000])
        cur = rng.choices(["USD", "EUR", "GBP", ""], weights=[70, 15, 10, 5])[0]
        return dict(customer=rng.choice(cust_ids), product=pid, qty=q, price=price, bp=bp,
                    bp_blank=(bp == 0 and rng.random() < 0.5), cur=cur, t0=t0)

    def make_valid(order_id: str, spec: dict, status: str, ev_dt: datetime, order_dt: datetime) -> Entry:
        cur_eff = spec["cur"] or "USD"
        gross = spec["qty"] * spec["price"]
        net = _net(gross, spec["bp"])
        disc = "" if spec["bp_blank"] else (f"{spec['bp'] // 100}" if spec["bp"] % 100 == 0 and rng.random() < 0.5
                                            else f"{spec['bp'] / 100:.2f}")
        f = {"order_id": order_id, "customer_id": spec["customer"], "product_id": spec["product"],
             "quantity": str(spec["qty"]), "unit_price": _money(spec["price"], rng),
             "discount_pct": disc, "currency": spec["cur"], "status": status,
             "order_ts": _render_ts(order_dt, rng), "event_ts": _render_ts(ev_dt, rng)}
        e = Entry("valid", f, key=(order_id, _iso_z(ev_dt)))
        e.payload = (spec["customer"], spec["product"], spec["qty"], spec["price"], spec["bp"],
                     cur_eff, status, _iso_z(order_dt), gross, net)
        e.label = {"order_id": order_id, "event_ts": _iso_z(ev_dt), "status": status,
                   "quantity": spec["qty"], "gross_cents": gross, "net_cents": net, "currency": cur_eff}
        return e

    for day in range(days):                       # create orders and their lifecycles
        for _ in range(orders_per_day):
            spec = new_spec(day)
            oid = next_order_id()
            statuses = rng.choices([l for l, _ in LIFECYCLES], weights=[w for _, w in LIFECYCLES])[0]
            ev = spec["t0"]
            for i, st in enumerate(statuses):
                if i > 0:
                    ev = ev + timedelta(seconds=rng.randrange(60, 30 * 3600))
                part = (ev.date() - start).days
                if part >= days:
                    break
                if rng.random() < 0.04 and part + 1 < days:          # late-arriving
                    part = min(days - 1, part + rng.randint(1, 3))
                per_part[part].append(make_valid(oid, spec, st, ev, spec["t0"]))

    manifest_parts, accepted_rows, reject_rows = [], [], []
    for day in range(days):
        pdate = start + timedelta(days=day)
        role = roles.get(day, "normal")
        entries = per_part[day]
        n_valid = len(entries)
        header = list(BASE_COLUMNS)
        if role == "extra_column":
            header.insert(5, "promo_code")
        elif role == "no_optional_columns":
            header = [c for c in header if c not in ("discount_pct", "currency")]
            for e in entries:                                         # defaults apply -> relabel
                spec_q, spec_p = e.payload[2], e.payload[3]
                e.fields["discount_pct"] = e.fields["currency"] = ""
                gross = spec_q * spec_p
                e.payload = e.payload[:4] + (0, "USD") + e.payload[6:8] + (gross, gross)
                e.label.update(currency="USD", net_cents=gross, gross_cents=gross)
        elif role == "schema_drift":
            header = ["qty" if c == "quantity" else c for c in header]

        # exact + conflicting duplicates
        for src in rng.sample(entries, k=min(len(entries), round(n_valid * 0.015))):
            d = Entry("dup", dict(src.fields), src.key, src.payload, dict(src.label))
            entries.append(d)
        for src in rng.sample(entries[:n_valid], k=min(n_valid, round(n_valid * 0.004))):
            f = dict(src.fields)
            q2 = int(f["quantity"]) + 1 if int(f["quantity"]) < 1000 else 999
            f["quantity"] = str(q2)
            gross = q2 * src.payload[3]
            p = list(src.payload)
            p[2], p[8], p[9] = q2, gross, _net(gross, src.payload[4])
            lbl = dict(src.label, quantity=q2, gross_cents=gross, net_cents=p[9])
            entries.append(Entry("conflict", f, src.key, tuple(p), lbl))

        # invalid rows
        for i in range(round(n_valid * defect_rate)):
            kind, reason = INVALID_KINDS[i % len(INVALID_KINDS)]
            if role == "no_optional_columns" and kind in ("discount_range", "bad_currency"):
                kind, reason = "bad_status", "INVALID_ENUM"   # those columns do not exist here
            spec = new_spec(day)
            ev = spec["t0"]
            e = make_valid(next_order_id(), spec, "CREATED", ev, ev)
            f = e.fields
            if kind == "missing_required":
                f[rng.choice(["order_id", "customer_id", "product_id", "quantity", "unit_price",
                              "status", "order_ts", "event_ts"])] = ""
            elif kind == "bad_order_id":
                f["order_id"] = rng.choice(["ORDER-1", "ord-00000001", "12345678"])
            elif kind == "bad_qty_type":
                f["quantity"] = rng.choice(["two", "1.5", "3 units"])
            elif kind == "bad_price_type":
                f["unit_price"] = rng.choice(["N/A", "12.345", "$9.99"])
            elif kind == "qty_range":
                f["quantity"] = rng.choice(["0", "-3", "1001"])
            elif kind == "price_range":
                f["unit_price"] = rng.choice(["0", "0.00", "100000.01"])
            elif kind == "discount_range":
                f["discount_pct"] = rng.choice(["120", "-5", "100.01"])
            elif kind == "bad_status":
                f["status"] = rng.choice(["DELIVERED", "unknown", "PENDING"])
            elif kind == "bad_currency":
                f["currency"] = rng.choice(["XXX", "JPY", "usdollar"])
            elif kind == "bad_event_ts":
                f["event_ts"] = rng.choice(["not-a-date", "2024-13-45T10:00:00Z", "2024-03-01"])
            elif kind == "future_ts":
                f["event_ts"] = _iso_z(datetime.combine(pdate + timedelta(days=2), datetime.min.time(),
                                                        tzinfo=timezone.utc) + timedelta(hours=10))
            elif kind == "event_before_order":
                f["order_ts"] = _iso_z(ev + timedelta(hours=1))
                f["event_ts"] = _iso_z(ev)
            elif kind == "bad_customer_ref":
                f["customer_id"] = "CUS-999999"
            elif kind == "bad_product_ref":
                f["product_id"] = "PRD-9999"
            # (re-render timestamps only for kinds that did not set them explicitly)
            ent = Entry("invalid", f, reason=reason)
            if kind == "malformed_row":
                ent.raw_row = [f[c] for c in BASE_COLUMNS][:len(header) - 2]
            entries.append(ent)

        rng.shuffle(entries)                                          # out-of-order within file

        # ---------------- write the file + derive labels ----------------
        pdir = out_dir / "orders" / f"dt={pdate.isoformat()}"
        pdir.mkdir(parents=True, exist_ok=True)
        if role == "empty":
            _write_csv(pdir / "orders.csv", header, [])
            manifest_parts.append(dict(partition=pdate.isoformat(), role=role, status="SUCCEEDED",
                                       rows=0, accepted=0, rejected=0))
            continue
        rows = []
        for e in entries:
            if e.raw_row is not None:
                rows.append(e.raw_row)
            else:
                f = dict(e.fields)
                if role == "extra_column":
                    f["promo_code"] = rng.choice(["SPRING10", "", "VIP"])
                if role == "schema_drift":
                    f["qty"] = f.pop("quantity")
                rows.append([f.get(c, "") for c in header])
        _write_csv(pdir / "orders.csv", header, rows)

        if role == "schema_drift":
            manifest_parts.append(dict(partition=pdate.isoformat(), role=role, status="FAILED",
                                       rows=len(rows), accepted=0, rejected=0))
            continue
        seen, acc, rej = {}, 0, 0
        for i, e in enumerate(entries):
            line = i + 2
            if e.kind == "invalid":
                reject_rows.append(("orders", pdate.isoformat(), line, e.reason)); rej += 1
            elif e.key in seen:
                reason = "DUPLICATE_EVENT" if seen[e.key] == e.payload else "CONFLICTING_DUPLICATE"
                reject_rows.append(("orders", pdate.isoformat(), line, reason)); rej += 1
            else:
                seen[e.key] = e.payload; acc += 1
                lab = e.label
                late = 1 if lab["event_ts"][:10] < pdate.isoformat() else 0
                accepted_rows.append([pdate.isoformat(), lab["order_id"], lab["event_ts"], lab["status"],
                                      lab["quantity"], lab["gross_cents"], lab["net_cents"],
                                      lab["currency"], late, line])
        manifest_parts.append(dict(partition=pdate.isoformat(), role=role, status="SUCCEEDED",
                                   rows=len(rows), accepted=acc, rejected=rej))

    # ---------------- labels (separate directory) ----------------
    _write_csv(labels_dir / "expected_accepted_events.csv",
               ["partition", "order_id", "event_ts", "status", "quantity", "gross_cents", "net_cents",
                "currency", "is_late_arriving", "source_line"], accepted_rows)
    _write_csv(labels_dir / "expected_rejects.csv", ["dataset", "partition", "source_line", "reason_code"],
               reject_rows)
    _write_csv(labels_dir / "expected_dimension_rejects.csv", ["dataset", "source_line", "reason_code"], dim_rejects)
    _write_csv(labels_dir / "expected_partitions.csv",
               ["partition", "role", "expected_status", "rows_read", "expected_accepted", "expected_rejected"],
               [[m["partition"], m["role"], m["status"], m["rows"], m["accepted"], m["rejected"]]
                for m in manifest_parts])
    manifest = {"seed": seed, "start": start.isoformat(), "days": days, "orders_per_day": orders_per_day,
                "customers": n_customers, "products": n_products, "partitions": manifest_parts,
                "total_source_rows": sum(m["rows"] for m in manifest_parts)}
    (labels_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def _write_csv(path: Path, header: list, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic orders data + separate labels")
    ap.add_argument("--out", required=True)
    ap.add_argument("--labels-out", default=None, help="default: <out>_labels")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--start", type=date.fromisoformat, default=date(2024, 3, 1))
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--orders-per-day", type=int, default=120)
    ap.add_argument("--customers", type=int, default=200)
    ap.add_argument("--products", type=int, default=40)
    ap.add_argument("--defect-rate", type=float, default=0.06)
    ap.add_argument("--no-special-partitions", action="store_true")
    a = ap.parse_args(argv)
    m = generate(Path(a.out), Path(a.labels_out or f"{a.out}_labels"), a.seed, a.start, a.days,
                 a.orders_per_day, a.customers, a.products, a.defect_rate, not a.no_special_partitions)
    print(f"generated {m['days']} partitions, {m['total_source_rows']} source rows -> {a.out}")


if __name__ == "__main__":
    main()
