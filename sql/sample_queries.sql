-- name: Backfill progress per partition (latest attempt)
SELECT source_partition, status, rows_read, rows_accepted, rows_rejected, duplicate_rows, late_rows, warnings, error_type
FROM v_partition_progress ORDER BY source_partition;

-- name: Run history
SELECT run_id, status, partitions_requested AS req, partitions_succeeded AS ok, partitions_failed AS failed, partitions_skipped AS skipped, rows_read, rows_accepted, rows_rejected
FROM backfill_run ORDER BY started_at, rowid;

-- name: Rejects by reason (orders)
SELECT reason_code, COUNT(*) AS rejects FROM reject_record WHERE dataset='orders' GROUP BY reason_code ORDER BY rejects DESC, reason_code;

-- name: Daily net sales by currency (excludes CANCELLED/REFUNDED)
SELECT order_date, currency, orders, units, gross_cents/100.0 AS gross, net_cents/100.0 AS net FROM v_daily_sales ORDER BY order_date, currency;

-- name: Current order status mix
SELECT status, COUNT(*) AS orders FROM fact_order GROUP BY status ORDER BY orders DESC;

-- name: Event history preserved - orders with the most status changes
SELECT order_id, version_count, status AS current_status FROM fact_order ORDER BY version_count DESC, order_id LIMIT 5;

-- name: Late-arriving events accepted per partition
SELECT source_partition, COUNT(*) AS late_events FROM order_event WHERE is_late_arriving=1 GROUP BY source_partition ORDER BY source_partition;

-- name: Sample rejects traced back to source file and line
SELECT source_partition, source_line, reason_code, reason_detail FROM reject_record WHERE dataset='orders' ORDER BY source_partition, source_line LIMIT 8;
