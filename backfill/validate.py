"""Pure row-level validation + deterministic transformation (no I/O, easy to unit test)."""
from __future__ import annotations

import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

from .schemas import (
    ALLOWED_CURRENCIES, ALLOWED_STATUSES, COUNTRY_RE, CUSTOMER_ID_RE, DEFAULT_CURRENCY,
    MAX_QUANTITY, MAX_UNIT_PRICE_CENTS, MIN_QUANTITY, ORDER_ID_RE, PRODUCT_ID_RE,
    ORDER_REQUIRED_COLUMNS, Reason,
)

_INT_RE = re.compile(r"^[+-]?\d+$")
_TWO_PLACES = Decimal("0.01")

# Event tuple layout produced by validate_order() (source_line is appended by the worker):
EVENT_FIELDS = (
    "order_id", "event_ts", "customer_id", "product_id", "quantity", "unit_price_cents",
    "discount_bp", "currency", "status", "order_ts", "gross_cents", "net_cents",
    "is_late_arriving",
)
PAYLOAD_START = 2  # fields after (order_id, event_ts) form the "payload" used for dup detection

Rejection = tuple[str, str]  # (reason_code, detail)


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp (must contain a time part). Naive values are taken as UTC."""
    v = value.strip()
    if "T" not in v and " " not in v:
        raise ValueError("timestamp has no time component")
    if v.endswith(("Z", "z")):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def iso_utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_money_cents(value: str) -> int:
    """'12.34' -> 1234. Rejects NaN/inf and more than two decimals (no silent rounding)."""
    d = Decimal(value.strip())
    if not d.is_finite():
        raise ValueError("not a finite number")
    if d != d.quantize(_TWO_PLACES):
        raise ValueError("more than 2 decimal places")
    return int((d * 100).to_integral_value())


def net_cents(gross: int, discount_bp: int) -> int:
    """Half-up rounding in pure integer arithmetic (deterministic, no float drift)."""
    return (gross * (10_000 - discount_bp) + 5_000) // 10_000


def validate_order(
    row: dict, partition: date, customer_ids: frozenset | set, product_ids: frozenset | set,
) -> tuple[Optional[tuple], Optional[Rejection]]:
    """Validate and transform one order event.

    Returns (event_tuple, None) or (None, (reason_code, detail)). Checks run in a fixed order,
    so the first failing rule is the reported reason (deterministic).
    """
    g = lambda k: (row.get(k) or "").strip()  # noqa: E731

    for col in ORDER_REQUIRED_COLUMNS:
        if not g(col):
            return None, (Reason.MISSING_REQUIRED, f"{col} is empty")

    order_id, customer_id, product_id = g("order_id"), g("customer_id"), g("product_id")
    if not ORDER_ID_RE.match(order_id):
        return None, (Reason.INVALID_FORMAT, f"order_id={order_id!r}")
    if not CUSTOMER_ID_RE.match(customer_id):
        return None, (Reason.INVALID_FORMAT, f"customer_id={customer_id!r}")
    if not PRODUCT_ID_RE.match(product_id):
        return None, (Reason.INVALID_FORMAT, f"product_id={product_id!r}")

    q_raw = g("quantity")
    if not _INT_RE.match(q_raw):
        return None, (Reason.INVALID_TYPE, f"quantity={q_raw!r} is not an integer")
    quantity = int(q_raw)
    if not MIN_QUANTITY <= quantity <= MAX_QUANTITY:
        return None, (Reason.OUT_OF_RANGE, f"quantity={quantity} not in [{MIN_QUANTITY},{MAX_QUANTITY}]")

    try:
        unit_price_cents = parse_money_cents(g("unit_price"))
    except (InvalidOperation, ValueError) as exc:
        return None, (Reason.INVALID_TYPE, f"unit_price={g('unit_price')!r}: {exc}")
    if not 0 < unit_price_cents <= MAX_UNIT_PRICE_CENTS:
        return None, (Reason.OUT_OF_RANGE, f"unit_price_cents={unit_price_cents}")

    d_raw = g("discount_pct")
    if d_raw == "":
        discount_bp = 0  # nullable: no discount
    else:
        try:
            discount_bp = parse_money_cents(d_raw)  # pct with 2dp == basis points
        except (InvalidOperation, ValueError) as exc:
            return None, (Reason.INVALID_TYPE, f"discount_pct={d_raw!r}: {exc}")
        if not 0 <= discount_bp <= 10_000:
            return None, (Reason.OUT_OF_RANGE, f"discount_pct={d_raw} not in [0,100]")

    currency = g("currency").upper() or DEFAULT_CURRENCY
    if currency not in ALLOWED_CURRENCIES:
        return None, (Reason.INVALID_ENUM, f"currency={currency!r}")

    status = g("status").upper()
    if status not in ALLOWED_STATUSES:
        return None, (Reason.INVALID_ENUM, f"status={status!r}")

    try:
        order_dt = parse_timestamp(g("order_ts"))
        event_dt = parse_timestamp(g("event_ts"))
    except ValueError as exc:
        return None, (Reason.INVALID_TIMESTAMP, f"{exc}")
    if event_dt.date() > partition:
        return None, (Reason.FUTURE_TIMESTAMP,
                      f"event_ts={iso_utc(event_dt)} is after partition {partition.isoformat()}")
    if event_dt < order_dt:
        return None, (Reason.OUT_OF_RANGE, "event_ts is earlier than order_ts")

    if customer_id not in customer_ids:
        return None, (Reason.INVALID_REFERENCE, f"unknown customer_id={customer_id}")
    if product_id not in product_ids:
        return None, (Reason.INVALID_REFERENCE, f"unknown product_id={product_id}")

    gross = quantity * unit_price_cents
    event = (
        order_id, iso_utc(event_dt), customer_id, product_id, quantity, unit_price_cents,
        discount_bp, currency, status, iso_utc(order_dt), gross, net_cents(gross, discount_bp),
        1 if event_dt.date() < partition else 0,
    )
    return event, None


def validate_customer(row: dict) -> tuple[Optional[tuple], Optional[Rejection]]:
    g = lambda k: (row.get(k) or "").strip()  # noqa: E731
    for col in ("customer_id", "customer_name", "country", "signup_date"):
        if not g(col):
            return None, (Reason.MISSING_REQUIRED, f"{col} is empty")
    if not CUSTOMER_ID_RE.match(g("customer_id")):
        return None, (Reason.INVALID_FORMAT, f"customer_id={g('customer_id')!r}")
    if not COUNTRY_RE.match(g("country")):
        return None, (Reason.INVALID_FORMAT, f"country={g('country')!r} (expected ISO-3166 alpha-2)")
    try:
        sd = date.fromisoformat(g("signup_date"))
    except ValueError:
        return None, (Reason.INVALID_TIMESTAMP, f"signup_date={g('signup_date')!r}")
    return (g("customer_id"), g("customer_name"), g("country"), sd.isoformat()), None


def validate_product(row: dict) -> tuple[Optional[tuple], Optional[Rejection]]:
    g = lambda k: (row.get(k) or "").strip()  # noqa: E731
    for col in ("product_id", "product_name", "category", "list_price"):
        if not g(col):
            return None, (Reason.MISSING_REQUIRED, f"{col} is empty")
    if not PRODUCT_ID_RE.match(g("product_id")):
        return None, (Reason.INVALID_FORMAT, f"product_id={g('product_id')!r}")
    try:
        price = parse_money_cents(g("list_price"))
    except (InvalidOperation, ValueError) as exc:
        return None, (Reason.INVALID_TYPE, f"list_price={g('list_price')!r}: {exc}")
    if price < 0:
        return None, (Reason.OUT_OF_RANGE, "list_price is negative")
    return (g("product_id"), g("product_name"), g("category"), price), None
