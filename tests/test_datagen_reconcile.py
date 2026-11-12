import hashlib
from datetime import date
from pathlib import Path

from backfill import db
from backfill.datagen import generate
from backfill.orchestrator import BackfillConfig, run_backfill
from backfill.reconcile import reconcile

from conftest import business_hash


def tree_hash(root: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        if p.is_file():
            h.update(p.relative_to(root).as_posix().encode())
            h.update(p.read_bytes())
    return h.hexdigest()


def test_generator_is_reproducible_with_fixed_seed(tmp_path):
    generate(tmp_path / "a", tmp_path / "al", seed=7, days=4, orders_per_day=40)
    generate(tmp_path / "b", tmp_path / "bl", seed=7, days=4, orders_per_day=40)
    generate(tmp_path / "c", tmp_path / "cl", seed=8, days=4, orders_per_day=40)
    assert tree_hash(tmp_path / "a") == tree_hash(tmp_path / "b")
    assert tree_hash(tmp_path / "al") == tree_hash(tmp_path / "bl")
    assert tree_hash(tmp_path / "a") != tree_hash(tmp_path / "c")


def test_labels_are_kept_outside_the_input_directory(tmp_path):
    generate(tmp_path / "src", tmp_path / "labels", days=3, orders_per_day=20)
    assert not any("expected" in p.name for p in (tmp_path / "src").rglob("*"))
    assert (tmp_path / "labels" / "expected_accepted_events.csv").exists()


def _full_run(tmp_path, workers):
    m = generate(tmp_path / "src", tmp_path / "labels", seed=11, days=7, orders_per_day=60)
    start = date.fromisoformat(m["start"])
    parts = [date.fromordinal(start.toordinal() + i) for i in range(m["days"])]
    cfg = BackfillConfig(db_path=tmp_path / "wh.db", source_dir=tmp_path / "src", partitions=parts,
                         workers=workers, retry_backoff_s=0)
    return run_backfill(cfg), cfg


def test_end_to_end_matches_independent_labels(tmp_path):
    summary, cfg = _full_run(tmp_path, workers=3)
    assert summary.status == "PARTIAL"                       # the deliberate schema-drift partition
    failed = [p.partition for p in summary.partitions if p.status == "FAILED"]
    assert failed == ["2024-03-06"]
    conn = db.connect(cfg.db_path)
    checks = reconcile(conn, cfg.source_dir, tmp_path / "labels")
    failing = [c for c in checks if not c.passed]
    assert not failing, failing
    assert any(c.name == "labels_accepted_events_match" for c in checks)


def test_full_rerun_with_force_gives_identical_target(tmp_path):
    summary, cfg = _full_run(tmp_path, workers=2)
    conn = db.connect(cfg.db_path)
    before = (business_hash(conn), conn.execute("SELECT COUNT(*) FROM order_event").fetchone()[0],
              conn.execute("SELECT COUNT(*) FROM reject_record").fetchone()[0])
    cfg.force = True
    run_backfill(cfg)
    conn = db.connect(cfg.db_path)
    after = (business_hash(conn), conn.execute("SELECT COUNT(*) FROM order_event").fetchone()[0],
             conn.execute("SELECT COUNT(*) FROM reject_record").fetchone()[0])
    assert before == after


def test_reconciliation_detects_tampering(tmp_path):
    """Negative test: the checks must be able to fail."""
    _, cfg = _full_run(tmp_path, workers=1)
    conn = db.connect(cfg.db_path)
    conn.execute("UPDATE fact_order SET net_cents = net_cents + 1 WHERE order_id = "
                 "(SELECT order_id FROM fact_order LIMIT 1)")
    conn.execute("DELETE FROM reject_record WHERE rowid = (SELECT MIN(rowid) FROM reject_record "
                 "WHERE dataset='orders')")
    failing = {c.name for c in reconcile(conn, cfg.source_dir, tmp_path / "labels") if not c.passed}
    assert "net_amount_by_currency_reconciles" in failing
    assert "source_rows_equal_events_plus_rejects" in failing
    assert "labels_rejects_match" in failing
