# Data dictionary

Domain: e-commerce **order events** (an order moves through `CREATED -> PAID -> SHIPPED ...`).
Money is stored as **integer cents** (no floats), timestamps as **ISO-8601 UTC** (`YYYY-MM-DDTHH:MM:SSZ`).

## Naming conventions
| Thing | Convention | Example |
|---|---|---|
| tables / columns | `snake_case`; `dim_` dimensions, `fact_` current state, `*_event` history, `*_run` audit | `dim_customer`, `order_event` |
| business IDs | fixed, validated prefixes + zero-padded digits | `ORD-00000042`, `CUS-000007`, `PRD-0012` |
| source partition | Hive-style folder `orders/dt=YYYY-MM-DD/orders.csv`; column `source_partition` = `YYYY-MM-DD` | `dt=2024-03-01` |
| run id | `run-<UTC yyyymmddThhmmssZ>-<4 hex>` (unique per execution, overridable with `--run-id`) | `run-20261111T200216Z-aa39` |
| reject id | `REJ-` + first 16 hex of SHA-1 over (dataset, partition, file, line, reason, raw row) -> **deterministic**: re-processing the same bad row yields the same id | `REJ-3f9c...` |
| lineage fields | `source_file`, `source_line`, `source_partition`, `run_id`, `loaded_at` on every loaded/rejected row | |

## Source files (inputs)
### `orders/dt=YYYY-MM-DD/orders.csv` - grain: one **order event** (an order's state at a moment)
| column | type | required | rules |
|---|---|---|---|
| order_id | string | yes | `^ORD-\d{8}$` |
| customer_id | string | yes | `^CUS-\d{6}$`, must exist in customers |
| product_id | string | yes | `^PRD-\d{4}$`, must exist in products |
| quantity | integer | yes | 1..1000 |
| unit_price | decimal | yes | > 0, <= 100000.00, max 2 decimals (never silently rounded) |
| discount_pct | decimal | **no** (nullable) | 0..100, max 2 decimals; empty = 0 |
| currency | string | **no** (nullable) | `USD`/`EUR`/`GBP`; empty = `USD` |
| status | string | yes | `CREATED, PAID, SHIPPED, CANCELLED, REFUNDED` (case-insensitive) |
| order_ts | timestamp | yes | ISO-8601; offsets converted to UTC; naive = UTC |
| event_ts | timestamp | yes | ISO-8601; `order_ts <= event_ts`; UTC date `<=` partition date |

Natural key of an event: `(order_id, event_ts)`.
Header rules: missing required column -> partition **fails** (`SchemaError`, no retry); unknown extra column -> ignored + warning;
missing optional column -> defaults + warning.

### `customers.csv` (`customer_id, customer_name, country, signup_date`) and `products.csv` (`product_id, product_name, category, list_price`)
Full snapshots, upserted each run (SCD type 1). Duplicate keys: first wins, later rows rejected `DUPLICATE_KEY`.
`country` must be 2 upper-case letters, `signup_date` an ISO date, `list_price` a non-negative decimal.

## Target tables (SQLite, `out/warehouse.db`)
| table | grain | primary key | purpose |
|---|---|---|---|
| `dim_customer` | customer | `customer_id` | reference data |
| `dim_product` | product | `product_id` | reference data |
| `order_event` | accepted source event | `(source_partition, order_id, event_ts)` | **append-style history**; replaced per source partition; FKs to both dims |
| `fact_order` | order | `order_id` | **current state** = latest event per order (derived, rebuilt for touched orders) |
| `reject_record` | rejected source row | `reject_id` | quarantine; traceable via `source_file` + `source_line` + `raw_record` |
| `backfill_run` | pipeline execution | `run_id` | start/end, status, params, totals, error |
| `partition_run` | partition **attempt** | `(run_id, source_partition, attempt)` | progress tracking, counts, checksum, error; drives restart |
Views: `v_partition_progress` (latest attempt per partition), `v_order_history`, `v_daily_sales`.

### `order_event` columns
`source_partition` (NN) | `order_id` (NN) | `event_ts` (NN, UTC) | `customer_id` (NN, FK) | `product_id` (NN, FK) |
`quantity` int 1..1000 | `unit_price_cents` int > 0 | `discount_bp` int 0..10000 (basis points) | `currency` | `status` |
`order_ts` | **derived:** `gross_cents = quantity * unit_price_cents`, `net_cents = round_half_up(gross * (10000 - discount_bp) / 10000)`,
`is_late_arriving = 1` when the event's UTC date is before the partition date | lineage: `source_file`, `source_line`, `run_id`, `loaded_at`.
All columns NOT NULL (nullability is resolved during validation: optional inputs get documented defaults).

### `fact_order` extra derived columns
`order_date` (date of `order_ts`), `version_count` (number of history events for the order), plus the latest event's values and lineage.

### Uniqueness / integrity guarantees
* PK on every table; `foreign_keys=ON` so orphan customer/product references cannot be stored.
* `CHECK` constraints repeat the key rules (defence in depth): quantity range, enum values, positive price, discount range.
* Duplicates: exact duplicate event in a file -> `DUPLICATE_EVENT`; same key but different payload -> `CONFLICTING_DUPLICATE` (first wins, both traceable).

## Reject reason codes
`MALFORMED_ROW`, `MISSING_REQUIRED`, `INVALID_FORMAT`, `INVALID_TYPE`, `OUT_OF_RANGE`, `INVALID_ENUM`, `INVALID_TIMESTAMP`,
`FUTURE_TIMESTAMP`, `INVALID_REFERENCE`, `DUPLICATE_EVENT`, `CONFLICTING_DUPLICATE`, `DUPLICATE_KEY` (dimensions).
Checks run in a fixed order, so the first failing rule is the single recorded reason (deterministic).
