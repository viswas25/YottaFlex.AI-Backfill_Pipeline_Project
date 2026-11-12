# Sample query results

## Backfill progress per partition (latest attempt)

```sql
SELECT source_partition, status, rows_read, rows_accepted, rows_rejected, duplicate_rows, late_rows, warnings, error_type
FROM v_partition_progress ORDER BY source_partition
```

| source_partition | status | rows_read | rows_accepted | rows_rejected | duplicate_rows | late_rows | warnings | error_type |
|---|---|---|---|---|---|---|---|---|
| 2024-03-01 | SUCCEEDED | 174 | 161 | 13 | 3 | 0 |  |  |
| 2024-03-02 | SUCCEEDED | 0 | 0 | 0 | 0 | 0 | EMPTY_BATCH: header only, no data rows |  |
| 2024-03-03 | SUCCEEDED | 310 | 288 | 22 | 5 | 2 |  |  |
| 2024-03-04 | SUCCEEDED | 341 | 316 | 25 | 6 | 13 | UNEXPECTED_COLUMNS ignored: ['promo_code'] |  |
| 2024-03-05 | SUCCEEDED | 306 | 284 | 22 | 5 | 7 | OPTIONAL_COLUMNS_ABSENT defaults applied: ['discount_pct', 'currency'] |  |
| 2024-03-06 | FAILED |  |  |  |  |  |  | SchemaError |
| 2024-03-07 | SUCCEEDED | 327 | 303 | 24 | 6 | 19 |  |  |

## Run history

```sql
SELECT run_id, status, partitions_requested AS req, partitions_succeeded AS ok, partitions_failed AS failed, partitions_skipped AS skipped, rows_read, rows_accepted, rows_rejected
FROM backfill_run ORDER BY started_at, rowid
```

| run_id | status | req | ok | failed | skipped | rows_read | rows_accepted | rows_rejected |
|---|---|---|---|---|---|---|---|---|
| run-20261112T012753Z-cc79 | PARTIAL | 7 | 6 | 1 | 0 | 1458 | 1352 | 106 |
| run-20261112T012754Z-20f2 | PARTIAL | 7 | 0 | 1 | 6 | 0 | 0 | 0 |
| run-20261112T012754Z-7b20 | SUCCEEDED | 2 | 2 | 0 | 0 | 651 | 604 | 47 |

## Rejects by reason (orders)

```sql
SELECT reason_code, COUNT(*) AS rejects FROM reject_record WHERE dataset='orders' GROUP BY reason_code ORDER BY rejects DESC, reason_code
```

| reason_code | rejects |
|---|---|
| DUPLICATE_EVENT | 20 |
| OUT_OF_RANGE | 18 |
| INVALID_TYPE | 13 |
| INVALID_ENUM | 11 |
| INVALID_FORMAT | 9 |
| MISSING_REQUIRED | 9 |
| INVALID_REFERENCE | 8 |
| CONFLICTING_DUPLICATE | 5 |
| INVALID_TIMESTAMP | 5 |
| FUTURE_TIMESTAMP | 4 |
| MALFORMED_ROW | 4 |

## Daily net sales by currency (excludes CANCELLED/REFUNDED)

```sql
SELECT order_date, currency, orders, units, gross_cents/100.0 AS gross, net_cents/100.0 AS net FROM v_daily_sales ORDER BY order_date, currency
```

| order_date | currency | orders | units | gross | net |
|---|---|---|---|---|---|
| 2024-03-01 | EUR | 14 | 56 | 13281.59 | 13171.56 |
| 2024-03-01 | GBP | 10 | 39 | 11791.88 | 11168.0 |
| 2024-03-01 | USD | 79 | 1283 | 503280.91 | 484191.15 |
| 2024-03-02 | EUR | 5 | 9 | 2216.52 | 2043.28 |
| 2024-03-02 | GBP | 6 | 31 | 7476.01 | 7182.94 |
| 2024-03-02 | USD | 52 | 171 | 1030600.14 | 27183.27 |
| 2024-03-03 | EUR | 13 | 59 | 16629.6 | 16042.46 |
| 2024-03-03 | GBP | 6 | 25 | 3084.3 | 2548.67 |
| 2024-03-03 | USD | 75 | 266 | 68261.22 | 64547.91 |
| 2024-03-04 | EUR | 6 | 21 | 3273.48 | 2666.02 |
| 2024-03-04 | GBP | 4 | 16 | 1495.4 | 1173.78 |
| 2024-03-04 | USD | 93 | 1331 | 529429.03 | 528480.46 |
| 2024-03-05 | EUR | 3 | 12 | 2631.9 | 2609.27 |
| 2024-03-05 | USD | 104 | 407 | 86729.02 | 85824.8 |
| 2024-03-06 | EUR | 4 | 11 | 3589.07 | 2878.21 |
| 2024-03-06 | GBP | 4 | 5 | 896.35 | 874.84 |
| 2024-03-06 | USD | 52 | 1179 | 191577.53 | 31947.35 |
| 2024-03-07 | EUR | 12 | 47 | 12137.65 | 10299.51 |
| 2024-03-07 | GBP | 12 | 34 | 9759.55 | 8898.85 |
| 2024-03-07 | USD | 91 | 299 | 68327.16 | 57968.05 |

## Current order status mix

```sql
SELECT status, COUNT(*) AS orders FROM fact_order GROUP BY status ORDER BY orders DESC
```

| status | orders |
|---|---|
| CREATED | 239 |
| SHIPPED | 203 |
| PAID | 203 |
| CANCELLED | 59 |
| REFUNDED | 58 |

## Event history preserved - orders with the most status changes

```sql
SELECT order_id, version_count, status AS current_status FROM fact_order ORDER BY version_count DESC, order_id LIMIT 5
```

| order_id | version_count | current_status |
|---|---|---|
| ORD-00000244 | 4 | REFUNDED |
| ORD-00000245 | 4 | REFUNDED |
| ORD-00000251 | 4 | REFUNDED |
| ORD-00000284 | 4 | REFUNDED |
| ORD-00000290 | 4 | REFUNDED |

## Late-arriving events accepted per partition

```sql
SELECT source_partition, COUNT(*) AS late_events FROM order_event WHERE is_late_arriving=1 GROUP BY source_partition ORDER BY source_partition
```

| source_partition | late_events |
|---|---|
| 2024-03-03 | 2 |
| 2024-03-04 | 13 |
| 2024-03-05 | 7 |
| 2024-03-07 | 19 |

## Sample rejects traced back to source file and line

```sql
SELECT source_partition, source_line, reason_code, reason_detail FROM reject_record WHERE dataset='orders' ORDER BY source_partition, source_line LIMIT 8
```

| source_partition | source_line | reason_code | reason_detail |
|---|---|---|---|
| 2024-03-01 | 26 | OUT_OF_RANGE | quantity=0 not in [1,1000] |
| 2024-03-01 | 46 | INVALID_TIMESTAMP | month must be in 1..12 |
| 2024-03-01 | 52 | INVALID_ENUM | currency='USDOLLAR' |
| 2024-03-01 | 65 | INVALID_ENUM | status='DELIVERED' |
| 2024-03-01 | 68 | OUT_OF_RANGE | unit_price_cents=10000001 |
| 2024-03-01 | 73 | CONFLICTING_DUPLICATE | (order_id, event_ts)=('ORD-00000104', '2024-03-01T06:52:06Z') already seen in this partition |
| 2024-03-01 | 77 | OUT_OF_RANGE | discount_pct=120 not in [0,100] |
| 2024-03-01 | 78 | INVALID_FORMAT | order_id='ORDER-1' |
