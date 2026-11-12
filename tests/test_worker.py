import pytest

from backfill.errors import SchemaError
from backfill.schemas import Reason
from backfill.worker import init_worker, process_partition

from conftest import HEADER, order, write_partition

init_worker({"CUS-000001"}, {"PRD-0001"})


def parse(tmp_path, rows=(), header=None, raw=None):
    p = write_partition(tmp_path, "2024-03-01", rows, header, raw)
    return process_partition("2024-03-01", str(p), "x")


def test_happy_path(tmp_path):
    r = parse(tmp_path, [order(), order(oid="ORD-00000002")])
    assert (r.rows_read, r.rows_accepted, r.rows_rejected) == (2, 2, 0)


def test_empty_batch_zero_byte_file(tmp_path):
    r = parse(tmp_path, raw="")
    assert r.rows_read == 0 and any("EMPTY_BATCH" in w for w in r.warnings)


def test_empty_batch_header_only(tmp_path):
    r = parse(tmp_path, [])
    assert r.rows_read == 0 and any("EMPTY_BATCH" in w for w in r.warnings)


def test_malformed_row_is_rejected_with_line_number(tmp_path):
    r = parse(tmp_path, [order(), ["ORD-00000009", "CUS-000001"], order(oid="ORD-00000003")])
    assert r.rows_accepted == 2
    reason, _, line, raw = r.rejects[0]
    assert reason == Reason.MALFORMED_ROW and line == 3 and "ORD-00000009" in raw


def test_exact_duplicate_vs_conflicting_duplicate(tmp_path):
    a = order()
    conflicting = order(qty="9")
    r = parse(tmp_path, [a, a, conflicting])
    assert r.rows_accepted == 1
    assert [x[0] for x in r.rejects] == [Reason.DUPLICATE_EVENT, Reason.CONFLICTING_DUPLICATE]
    assert r.duplicate_rows == 2


def test_null_in_required_field(tmp_path):
    r = parse(tmp_path, [order(cust="")])
    assert r.rejects[0][0] == Reason.MISSING_REQUIRED


def test_missing_required_column_is_schema_error(tmp_path):
    hdr = ["qty" if c == "quantity" else c for c in HEADER]
    with pytest.raises(SchemaError, match="quantity"):
        parse(tmp_path, [], header=hdr)


def test_unexpected_extra_column_is_tolerated_with_warning(tmp_path):
    hdr = HEADER + ["promo_code"]
    r = parse(tmp_path, [order() + ["X"]], header=hdr)
    assert r.rows_accepted == 1 and any("UNEXPECTED_COLUMNS" in w for w in r.warnings)


def test_absent_optional_columns_use_defaults(tmp_path):
    hdr = [c for c in HEADER if c not in ("discount_pct", "currency")]
    row = [v for h, v in zip(HEADER, order()) if h in hdr]
    r = parse(tmp_path, [row], header=hdr)
    ev = r.events[0]
    assert ev[7] == "USD" and ev[6] == 0


def test_blank_lines_ignored_and_bom_handled(tmp_path):
    raw = "\ufeff" + ",".join(HEADER) + "\n\n" + ",".join(order()) + "\n\n"
    r = parse(tmp_path, raw=raw)
    assert r.rows_read == 1 and r.rows_accepted == 1


def test_invalid_utf8_is_permanent_schema_error(tmp_path):
    p = write_partition(tmp_path, "2024-03-01", [], raw="x")
    p.write_bytes(b"order_id,customer_id\n\xff\xfe\n")
    with pytest.raises(SchemaError):
        process_partition("2024-03-01", str(p), "x")
