"""Source/target contract constants shared by validation, loading and reconciliation."""
from __future__ import annotations

import re

# ---- source contract: orders/dt=YYYY-MM-DD/orders.csv -------------------------------------
ORDER_REQUIRED_COLUMNS = (
    "order_id", "customer_id", "product_id", "quantity", "unit_price",
    "status", "order_ts", "event_ts",
)
ORDER_OPTIONAL_COLUMNS = ("discount_pct", "currency")
ORDER_KNOWN_COLUMNS = ORDER_REQUIRED_COLUMNS + ORDER_OPTIONAL_COLUMNS

CUSTOMER_COLUMNS = ("customer_id", "customer_name", "country", "signup_date")
PRODUCT_COLUMNS = ("product_id", "product_name", "category", "list_price")

ALLOWED_STATUSES = ("CREATED", "PAID", "SHIPPED", "CANCELLED", "REFUNDED")
ALLOWED_CURRENCIES = ("USD", "EUR", "GBP")
DEFAULT_CURRENCY = "USD"

MIN_QUANTITY, MAX_QUANTITY = 1, 1000
MAX_UNIT_PRICE_CENTS = 10_000_000  # 100,000.00

ORDER_ID_RE = re.compile(r"^ORD-\d{8}$")
CUSTOMER_ID_RE = re.compile(r"^CUS-\d{6}$")
PRODUCT_ID_RE = re.compile(r"^PRD-\d{4}$")
COUNTRY_RE = re.compile(r"^[A-Z]{2}$")


class Reason:
    """Reject reason codes (closed vocabulary; reconciliation verifies it)."""
    MALFORMED_ROW = "MALFORMED_ROW"
    MISSING_REQUIRED = "MISSING_REQUIRED"
    INVALID_FORMAT = "INVALID_FORMAT"
    INVALID_TYPE = "INVALID_TYPE"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    INVALID_ENUM = "INVALID_ENUM"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
    INVALID_REFERENCE = "INVALID_REFERENCE"
    DUPLICATE_EVENT = "DUPLICATE_EVENT"
    CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"
    DUPLICATE_KEY = "DUPLICATE_KEY"  # dimensions

    ALL = frozenset({
        MALFORMED_ROW, MISSING_REQUIRED, INVALID_FORMAT, INVALID_TYPE, OUT_OF_RANGE,
        INVALID_ENUM, INVALID_TIMESTAMP, FUTURE_TIMESTAMP, INVALID_REFERENCE,
        DUPLICATE_EVENT, CONFLICTING_DUPLICATE, DUPLICATE_KEY,
    })


# Partition statuses
RUNNING, SUCCEEDED, FAILED, SKIPPED, ABANDONED = (
    "RUNNING", "SUCCEEDED", "FAILED", "SKIPPED", "ABANDONED")
