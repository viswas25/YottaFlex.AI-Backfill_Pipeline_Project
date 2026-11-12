"""Integration tests: the behaviours the business asked for (idempotency, restart, retry, ...)."""
import sqlite3
import shutil
from datetime import date

import pytest

from backfill import db
from backfill.orchestrator import BackfillConfig, dry_run, run_backfill

from conftest import HEADER, business_hash, cfg_for, conn_for, order, run, write_partition, write_dims

D1, D2, D3 = "2024-03-01", "2024-03-02", "2024-03-03"


def setup_three(env):
    src = env / "src"
    write_partition(src, D1, [order("ORD-00000001"), order("ORD-00000002", cust="CUS-000002")])
    write_partition(src, D2, [order("ORD-00000003", order_ts="2024-03-02T08:00:00Z", event_ts="2024-03-02T08:00:00Z")])
    write_partition(src, D3, [order("ORD-00000004", order_ts="2024-03-03T08:00:00Z", event_ts="2024-03-03T08:00:00Z")])


def count(env, table, where=""):
    return conn_for(env).execute(f"SELECT COUNT(*) FROM {table} {where}").fetchone()[0]


# ---------------------------------------------------------------- happy path + metrics
def test_happy_path_loads_and_records_metrics(env):
    setup_three(env)
    s = run(env, [D1, D2, D3])
    assert s.status == "SUCCEEDED" and s.exit_code == 0
    assert count(env, "fact_order") == 4
    c = conn_for(env)
    r = c.execute("SELECT * FROM backfill_run").fetchone()
    assert (r["partitions_succeeded"], r["rows_read"], r["rows_accepted"], r["rows_rejected"]) == (3, 4, 4, 0)
    assert r["started_at"] and r["ended_at"] and r["run_id"] == s.run_id
    pr = c.execute("SELECT * FROM partition_run WHERE source_partition=?", (D1,)).fetchone()
    assert pr["status"] == "SUCCEEDED" and pr["rows_read"] == 2 and pr["source_checksum"]


def test_selected_partitions_only(env):
    setup_three(env)
    run(env, [D2])
    assert count(env, "order_event", f"WHERE source_partition='{D2}'") == 1
    assert count(env, "order_event") == 1


# ---------------------------------------------------------------- idempotency / restart
def test_rerun_same_input_is_skipped_and_changes_nothing(env):
    setup_three(env)
    run(env, [D1, D2, D3])
    before = business_hash(conn_for(env))
    s = run(env, [D1, D2, D3])
    assert s.count("SKIPPED") == 3 and s.count("SUCCEEDED") == 0
    assert business_hash(conn_for(env)) == before
    assert count(env, "fact_order") == 4


def test_forced_rerun_is_idempotent(env):
    setup_three(env)
    run(env, [D1, D2, D3])
    before = business_hash(conn_for(env))
    ev_before = count(env, "order_event")
    s = run(env, [D1, D2, D3], force=True)
    assert s.count("SUCCEEDED") == 3
    assert business_hash(conn_for(env)) == before
    assert count(env, "order_event") == ev_before


def test_rerun_after_source_correction_replaces_partition(env):
    setup_three(env)
    run(env, [D1])
    write_partition(env / "src", D1, [order("ORD-00000001", qty="5")])   # ORD-2 removed, qty fixed
    s = run(env, [D1])
    assert s.count("SUCCEEDED") == 1                                     # checksum changed -> reloaded
    c = conn_for(env)
    assert c.execute("SELECT COUNT(*) FROM fact_order").fetchone()[0] == 1
    assert c.execute("SELECT quantity FROM fact_order").fetchone()[0] == 5


# ---------------------------------------------------------------- failure, rollback, retry
def test_partial_failure_rolls_back_and_other_partitions_survive(env):
    setup_three(env)
    s = run(env, [D1, D2, D3], simulate_failures={D2: 99}, max_retries=1)
    assert s.status == "PARTIAL" and s.exit_code == 2
    assert count(env, "order_event", f"WHERE source_partition='{D2}'") == 0     # rolled back
    assert count(env, "reject_record", f"WHERE source_partition='{D2}'") == 0
    assert count(env, "order_event") == 3                                       # D1 + D3 intact
    c = conn_for(env)
    attempts = c.execute("SELECT attempt, status, error_type FROM partition_run "
                         "WHERE source_partition=? ORDER BY attempt", (D2,)).fetchall()
    assert [(a["attempt"], a["status"], a["error_type"]) for a in attempts] == \
        [(1, "FAILED", "TransientError"), (2, "FAILED", "TransientError")]


def test_transient_failure_is_retried_then_succeeds(env):
    setup_three(env)
    s = run(env, [D1, D2, D3], simulate_failures={D2: 1}, max_retries=2)
    assert s.status == "SUCCEEDED"
    out = {p.partition: p for p in s.partitions}
    assert out[D2].attempts == 2
    assert count(env, "order_event") == 4                  # no duplicates from the failed attempt


def test_restart_after_failure_only_reprocesses_failed_partition(env):
    setup_three(env)
    run(env, [D1, D2, D3], simulate_failures={D2: 99}, max_retries=0)
    s = run(env, [D1, D2, D3])                             # operator simply re-runs
    states = {p.partition: p.status for p in s.partitions}
    assert states == {D1: "SKIPPED", D2: "SUCCEEDED", D3: "SKIPPED"}
    assert count(env, "fact_order") == 4


def test_permanent_error_is_not_retried(env):
    setup_three(env)
    write_partition(env / "src", D2, [], header=["qty" if c == "quantity" else c for c in HEADER])
    s = run(env, [D1, D2, D3], max_retries=5)
    bad = {p.partition: p for p in s.partitions}[D2]
    assert bad.status == "FAILED" and bad.attempts == 1 and bad.error_type == "SchemaError"
    assert count(env, "order_event") == 3
    # fix the file, restart: only the repaired partition runs
    write_partition(env / "src", D2, [order("ORD-00000003", order_ts="2024-03-02T08:00:00Z", event_ts="2024-03-02T08:00:00Z")])
    s2 = run(env, [D1, D2, D3])
    assert {p.partition: p.status for p in s2.partitions} == {D1: "SKIPPED", D2: "SUCCEEDED", D3: "SKIPPED"}


def test_missing_partition_file_is_reported_not_ignored(env):
    setup_three(env)
    s = run(env, [D1, "2024-03-09"])
    assert s.status == "PARTIAL"
    miss = {p.partition: p for p in s.partitions}["2024-03-09"]
    assert miss.status == "FAILED" and miss.error_type == "SourceMissingError"


def test_crash_leaves_running_attempt_which_next_run_marks_abandoned(env):
    setup_three(env)
    run(env, [D1])
    c = conn_for(env)
    db.start_attempt(c, "run-dead", D2, 1, "x", "y")        # simulate kill -9 mid-attempt
    c.execute("INSERT INTO backfill_run(run_id, started_at, status, params) VALUES ('run-dead','t','RUNNING','{}')")
    s = run(env, [D2])
    assert s.abandoned_attempts_cleaned == 1
    c = conn_for(env)
    assert c.execute("SELECT status FROM partition_run WHERE run_id='run-dead'").fetchone()[0] == "ABANDONED"
    assert c.execute("SELECT status FROM backfill_run WHERE run_id='run-dead'").fetchone()[0] == "ABANDONED"


# ---------------------------------------------------------------- history, late, out-of-order
def test_status_history_is_preserved_not_overwritten(env):
    src = env / "src"
    write_partition(src, D1, [order(status="CREATED")])
    write_partition(src, D2, [order(status="PAID", event_ts="2024-03-02T09:00:00Z")])
    write_partition(src, D3, [order(status="SHIPPED", event_ts="2024-03-03T09:00:00Z")])
    run(env, [D1, D2, D3])
    c = conn_for(env)
    f = c.execute("SELECT status, version_count FROM fact_order").fetchone()
    assert (f["status"], f["version_count"]) == ("SHIPPED", 3)
    hist = [r[0] for r in c.execute("SELECT status FROM v_order_history ORDER BY event_ts")]
    assert hist == ["CREATED", "PAID", "SHIPPED"]


def test_late_arriving_old_event_does_not_overwrite_newer_state(env):
    src = env / "src"
    write_partition(src, D1, [order(status="SHIPPED", order_ts="2024-03-01T08:00:00Z",
                                    event_ts="2024-03-01T20:00:00Z")])
    # D2 delivers an OLDER event (PAID at 09:00 on D1) late
    write_partition(src, D2, [order(status="PAID", order_ts="2024-03-01T08:00:00Z",
                                    event_ts="2024-03-01T09:00:00Z")])
    run(env, [D1, D2])
    c = conn_for(env)
    f = c.execute("SELECT status, version_count FROM fact_order").fetchone()
    assert (f["status"], f["version_count"]) == ("SHIPPED", 2)
    assert c.execute("SELECT is_late_arriving FROM order_event WHERE source_partition=?", (D2,)).fetchone()[0] == 1


def test_partition_processing_order_does_not_change_result(env):
    src = env / "src"
    write_partition(src, D1, [order(status="CREATED")])
    write_partition(src, D2, [order(status="PAID", event_ts="2024-03-02T09:00:00Z")])
    run(env, [D1, D2])
    forward = business_hash(conn_for(env))
    for f in (env / "wh.db", env / "wh.db-wal", env / "wh.db-shm"):
        f.unlink(missing_ok=True)
    run(env, [D2]); run(env, [D1])                         # reverse order, separate runs
    assert business_hash(conn_for(env)) == forward


def test_out_of_order_rows_inside_file(env):
    write_partition(env / "src", D1, [order(status="PAID", event_ts="2024-03-01T12:00:00Z"),
                                      order(status="CREATED", event_ts="2024-03-01T10:00:00Z")])
    run(env, [D1])
    assert conn_for(env).execute("SELECT status FROM fact_order").fetchone()[0] == "PAID"


# ---------------------------------------------------------------- data quality paths
def test_rejects_are_quarantined_with_source_traceability(env):
    write_partition(env / "src", D1, [order(), order("ORD-00000002", cust="CUS-777777"), order("ORD-00000003", qty="0")])
    s = run(env, [D1])
    assert s.status == "SUCCEEDED"
    rows = conn_for(env).execute("SELECT reason_code, source_line, source_file, raw_record "
                                 "FROM reject_record ORDER BY source_line").fetchall()
    assert [(r["reason_code"], r["source_line"]) for r in rows] == [("INVALID_REFERENCE", 3), ("OUT_OF_RANGE", 4)]
    assert all("dt=2024-03-01" in r["source_file"] and "ORD-0000000" in r["raw_record"] for r in rows)


def test_reject_ids_are_deterministic_across_reruns(env):
    write_partition(env / "src", D1, [order(), order("ORD-00000002", qty="0")])
    run(env, [D1])
    a = [r[0] for r in conn_for(env).execute("SELECT reject_id FROM reject_record")]
    run(env, [D1], force=True)
    b = [r[0] for r in conn_for(env).execute("SELECT reject_id FROM reject_record")]
    assert a == b and len(a) == 1


def test_empty_partition_succeeds_with_zero_rows(env):
    write_partition(env / "src", D1, [], raw="")
    s = run(env, [D1])
    assert s.status == "SUCCEEDED"
    out = s.partitions[0]
    assert out.rows_read == 0 and any("EMPTY_BATCH" in w for w in out.warnings)


def test_empty_partition_after_data_clears_previous_rows(env):
    write_partition(env / "src", D1, [order()])
    run(env, [D1])
    write_partition(env / "src", D1, [], raw="")
    run(env, [D1])
    assert count(env, "fact_order") == 0 and count(env, "order_event") == 0


def test_missing_dimension_file_fails_the_run_cleanly(env):
    setup_three(env)
    (env / "src" / "products.csv").unlink()
    s = run(env, [D1])
    assert s.status == "FAILED" and "dimension" in s.error
    assert count(env, "order_event") == 0


# ---------------------------------------------------------------- bounded parallelism
def test_parallel_run_matches_sequential_run(env, tmp_path_factory):
    setup_three(env)
    run(env, [D1, D2, D3], workers=1)
    seq = business_hash(conn_for(env))
    par_env = tmp_path_factory.mktemp("par")
    shutil.copytree(env / "src", par_env / "src")
    run(par_env, [D1, D2, D3], workers=3)
    assert business_hash(conn_for(par_env)) == seq


def test_worker_count_is_bounded(env, monkeypatch):
    """No more than `workers` partitions are ever in flight."""
    import backfill.orchestrator as orch
    setup_three(env)
    peak = {"now": 0, "max": 0}
    real_wait = orch.wait

    def spy(inflight, **kw):
        peak["max"] = max(peak["max"], len(inflight))
        return real_wait(inflight, **kw)
    monkeypatch.setattr(orch, "wait", spy)
    run(env, [D1, D2, D3], workers=2)
    assert peak["max"] <= 2


# ---------------------------------------------------------------- dry run
def test_dry_run_estimates_and_writes_nothing(env):
    setup_three(env)
    run(env, [D1])                                          # D1 already loaded
    write_partition(env / "src", "2024-03-04", [], header=["qty" if c == "quantity" else c for c in HEADER])
    cfg = cfg_for(env, [D1, D2, D3, "2024-03-04", "2024-03-09"])
    runs_before = count(env, "backfill_run")
    rep = dry_run(cfg)
    actions = {p["partition"]: p["action"] for p in rep["partitions"]}
    assert actions == {D1: "SKIP", D2: "RUN", D3: "RUN", "2024-03-04": "WOULD_FAIL", "2024-03-09": "WOULD_FAIL"}
    assert rep["would_run"] == 2 and rep["est_source_rows_to_process"] == 2
    assert rep["est_wall_seconds"] >= 0 and rep["assumed_throughput_rows_per_s"] > 0
    assert count(env, "backfill_run") == runs_before and count(env, "order_event") == 2


def test_dry_run_works_before_any_database_exists(env):
    setup_three(env)
    rep = dry_run(cfg_for(env, [D1, D2]))
    assert rep["would_run"] == 2 and not (env / "wh.db").exists()


# ---------------------------------------------------------------- dimensions
def test_dimension_duplicates_and_invalid_rows_are_rejected(tmp_path):
    src = tmp_path / "src"
    write_dims(src)
    with open(src / "customers.csv", "a") as fh:
        fh.write("CUS-000001,Dup,US,2023-01-01\nCUS-000009,Bad,USA,2023-01-01\n")
    write_partition(src, D1, [order()])
    run(tmp_path, [D1])
    rows = conn_for(tmp_path).execute("SELECT reason_code FROM reject_record WHERE dataset='customers' "
                                      "ORDER BY source_line").fetchall()
    assert [r[0] for r in rows] == ["DUPLICATE_KEY", "INVALID_FORMAT"]
