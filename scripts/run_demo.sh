#!/usr/bin/env bash
# End-to-end demonstration: generate -> dry-run -> backfill (with an injected transient failure)
# -> restart (skips finished partitions) -> forced rerun (idempotent) -> reconcile -> reports.
# Re-runnable from a clean checkout:  bash scripts/run_demo.sh
set -u
cd "$(dirname "$0")/.."
PY=${PYTHON:-python3}
DB=out/warehouse.db
SRC=data/sample
LABELS=data/sample_labels
START=2024-03-01; END=2024-03-07
mkdir -p reports out
rm -f out/warehouse.db out/warehouse.db-wal out/warehouse.db-shm reports/run_*.json
exec > >(tee reports/demo_run.log) 2>&1

echo "=== 1. generate reproducible sample (seed 42) ==="
rm -rf "$SRC" "$LABELS"
$PY -m backfill generate --out "$SRC" --labels-out "$LABELS" --seed 42 --days 7 --orders-per-day 120

echo; echo "=== 2. DRY RUN (estimates only, nothing is written) ==="
$PY -m backfill backfill --db "$DB" --source "$SRC" --start $START --end $END --dry-run --workers 3 --report-dir reports \
  | $PY -c "import sys,json; r=json.load(sys.stdin); [print(k,'=',v) for k,v in r.items() if k!='partitions']; [print(' ',p['partition'],p['action'],p.get('reason',''),'rows~',p.get('est_rows'),'reject_rate~',p.get('est_reject_rate','')) for p in r['partitions']]"

echo; echo "=== 3. BACKFILL with injected transient failure on 2024-03-03 (retried), 3 workers ==="
$PY -m backfill backfill --db "$DB" --source "$SRC" --start $START --end $END --workers 3 \
   --simulate-failure 2024-03-03:1 --retry-backoff 0.2 --report-dir reports
echo "exit code: $?  (2 = PARTIAL: the schema-drift partition 2024-03-06 failed, as designed)"

echo; echo "=== 4. RESTART: same command again - finished partitions are skipped ==="
$PY -m backfill backfill --db "$DB" --source "$SRC" --start $START --end $END --workers 3
echo "exit code: $?"

echo; echo "=== 5. FORCED RERUN of one partition (idempotent: counts do not change) ==="
$PY -m backfill reconcile --db "$DB" --source "$SRC" --labels "$LABELS" --out out/before > /dev/null
$PY -m backfill backfill --db "$DB" --source "$SRC" --dates 2024-03-03,2024-03-04 --force --workers 2
$PY -m backfill reconcile --db "$DB" --source "$SRC" --labels "$LABELS" --out out/after > /dev/null
diff <(sed 's/run-[^ ]*//' out/before/reconciliation.md) <(sed 's/run-[^ ]*//' out/after/reconciliation.md) \
  && echo "reconciliation identical before/after forced rerun -> no unintended duplicates"

echo; echo "=== 6. STATUS ==="
$PY -m backfill status --db "$DB" | tee reports/status.txt

echo; echo "=== 7. RECONCILIATION against independent labels ==="
$PY -m backfill reconcile --db "$DB" --source "$SRC" --labels "$LABELS" --out reports
echo "reconcile exit code: $?"

echo; echo "=== 8. SAMPLE QUERIES ==="
$PY -m backfill report --db "$DB" --sql sql/sample_queries.sql --out reports/sample_query_results.md
