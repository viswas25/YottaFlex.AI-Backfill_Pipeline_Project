# Reconciliation report

**Overall: PASS** (15/15 checks)

| # | Check | Result | Detail |
|---|-------|--------|--------|
| 1 | `partitions_loaded` | PASS | 6 partition(s) loaded: ['2024-03-01', '2024-03-02', '2024-03-03', '2024-03-04', '2024-03-05', '2024-03-07'] |
| 2 | `no_attempt_left_running` | PASS | 0 attempt(s) still RUNNING |
| 3 | `source_rows_equal_events_plus_rejects` | PASS | every partition: source rows == accepted events + rejects |
| 4 | `primary_keys_unique` | PASS | duplicate event keys=0, duplicate fact keys=0 |
| 5 | `referential_integrity` | PASS | orphan rows=0, fk violations=0 |
| 6 | `fact_is_latest_event_per_order` | PASS | distinct orders in history=762, fact rows=762, stale rows=0 |
| 7 | `amounts_recompute_exactly` | PASS | 0 event row(s) whose gross/net do not recompute |
| 8 | `net_amount_by_currency_reconciles` | PASS | fact={'EUR': 5587124, 'GBP': 4378273, 'USD': 141317725} history_latest={'EUR': 5587124, 'GBP': 4378273, 'USD': 141317725} |
| 9 | `rejects_traceable_to_source` | PASS | 110 rejects; untraceable=0; unknown reason codes=[] |
| 10 | `reject_breakdown` | PASS | {"CONFLICTING_DUPLICATE": 5, "DUPLICATE_EVENT": 20, "FUTURE_TIMESTAMP": 4, "INVALID_ENUM": 11, "INVALID_FORMAT": 9, "INVALID_REFERENCE": 8, "INVALID_TIMESTAMP": 5, "INVALID_TYPE": 13, "MALFORMED_ROW": 4, "MISSING_REQUIRED": 9, "OUT_OF_RANGE": 18} |
| 11 | `labels_expected_failures_not_loaded` | PASS | partitions expected to FAIL: ['2024-03-06']; wrongly loaded: [] |
| 12 | `labels_accepted_events_match` | PASS | expected=1352 loaded=1352 missing=0 unexpected=0 field_mismatches=0 |
| 13 | `labels_rejects_match` | PASS | expected=106 loaded=106 missing=0 unexpected=0 |
| 14 | `labels_fact_orders_and_net_match` | PASS | expected orders=762 fact=762; expected net={'USD': 141317725, 'GBP': 4378273, 'EUR': 5587124} fact={'EUR': 5587124, 'GBP': 4378273, 'USD': 141317725}; status mix equal=True |
| 15 | `labels_dimension_rejects_match` | PASS | expected={('customers', 202, 'DUPLICATE_KEY'): 1, ('customers', 203, 'INVALID_FORMAT'): 1, ('products', 42, 'DUPLICATE_KEY'): 1, ('products', 43, 'INVALID_TYPE'): 1} loaded={('customers', 202, 'DUPLICATE_KEY'): 1, ('customers', 203, 'INVALID_FORMAT'): 1, ('products', 42, 'DUPLICATE_KEY'): 1, ('products', 43, 'INVALID_TYPE'): 1} |
