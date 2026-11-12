from datetime import date

import pytest

from backfill.schemas import Reason
from backfill.validate import net_cents, parse_money_cents, parse_timestamp, validate_order

CUST, PROD = {"CUS-000001"}, {"PRD-0001"}
P = date(2024, 3, 1)


def row(**over):
    base = dict(order_id="ORD-00000001", customer_id="CUS-000001", product_id="PRD-0001", quantity="2",
                unit_price="10.00", discount_pct="", currency="USD", status="CREATED",
                order_ts="2024-03-01T10:00:00Z", event_ts="2024-03-01T10:00:00Z")
    base.update(over)
    return base


def ok(**over):
    ev, rej = validate_order(row(**over), P, CUST, PROD)
    assert rej is None, rej
    return ev


def rejected(**over):
    ev, rej = validate_order(row(**over), P, CUST, PROD)
    assert ev is None
    return rej[0]


def test_happy_path_transformation():
    ev = ok(quantity="3", unit_price="19.99", discount_pct="10")
    # (order_id, event_ts, cust, prod, qty, unit_cents, disc_bp, cur, status, order_ts, gross, net, late)
    assert ev[4:7] == (3, 1999, 1000)
    assert ev[10] == 5997 and ev[11] == 5397          # 59.97 -> 10% off -> 53.973 -> 53.97
    assert ev[12] == 0


@pytest.mark.parametrize("field,value,reason", [
    ("customer_id", "", Reason.MISSING_REQUIRED),
    ("order_id", "ORDER-1", Reason.INVALID_FORMAT),
    ("quantity", "two", Reason.INVALID_TYPE),
    ("quantity", "1.5", Reason.INVALID_TYPE),
    ("quantity", "0", Reason.OUT_OF_RANGE),
    ("quantity", "1001", Reason.OUT_OF_RANGE),
    ("unit_price", "N/A", Reason.INVALID_TYPE),
    ("unit_price", "1.234", Reason.INVALID_TYPE),
    ("unit_price", "NaN", Reason.INVALID_TYPE),
    ("unit_price", "0", Reason.OUT_OF_RANGE),
    ("discount_pct", "100.01", Reason.OUT_OF_RANGE),
    ("discount_pct", "-1", Reason.OUT_OF_RANGE),
    ("currency", "XXX", Reason.INVALID_ENUM),
    ("status", "DELIVERED", Reason.INVALID_ENUM),
    ("event_ts", "not-a-date", Reason.INVALID_TIMESTAMP),
    ("event_ts", "2024-03-01", Reason.INVALID_TIMESTAMP),
    ("event_ts", "2024-03-05T00:00:00Z", Reason.FUTURE_TIMESTAMP),
    ("order_ts", "2024-03-01T11:00:00Z", Reason.OUT_OF_RANGE),      # event before order
    ("customer_id", "CUS-999999", Reason.INVALID_REFERENCE),
    ("product_id", "PRD-9999", Reason.INVALID_REFERENCE),
])
def test_rejection_reasons(field, value, reason):
    assert rejected(**{field: value}) == reason


# ---- boundary values ----------------------------------------------------------------------
@pytest.mark.parametrize("qty", ["1", "1000"])
def test_quantity_boundaries_accepted(qty):
    assert ok(quantity=qty)[4] == int(qty)


def test_price_and_discount_boundaries():
    assert ok(unit_price="0.01")[5] == 1
    assert ok(unit_price="100000.00")[5] == 10_000_000
    full = ok(discount_pct="100")
    assert full[11] == 0                      # 100% discount -> net 0
    assert ok(discount_pct="0")[11] == ok(discount_pct="")[11]   # nullable == 0


# ---- domain edge cases --------------------------------------------------------------------
def test_half_up_rounding_is_deterministic_not_bankers():
    assert net_cents(25, 1000) == 23          # 22.5 -> 23 (banker's rounding would give 22)
    assert net_cents(5, 5000) == 3            # 2.5 -> 3


def test_timezone_offsets_normalised_to_utc():
    ev = ok(event_ts="2024-03-01T15:30:00+05:30", order_ts="2024-03-01T10:00:00+00:00")
    assert ev[1] == "2024-03-01T10:00:00Z"
    assert parse_timestamp("2024-03-01 10:00:00") == parse_timestamp("2024-03-01T10:00:00Z")


def test_late_arriving_flag_when_event_date_before_partition():
    ev = ok(order_ts="2024-02-27T09:00:00Z", event_ts="2024-02-28T09:00:00Z")
    assert ev[12] == 1


def test_money_parsing_never_rounds_silently():
    assert parse_money_cents("12") == 1200
    with pytest.raises(ValueError):
        parse_money_cents("0.001")
