# Validation & Reconciliation Harness

## Validation & Reconciliation Harness: Live Dual-Execution

### Architecture

A Node.js (ESM) reconciliation harness orchestrates live execution on **both** the legacy CDH cluster (via Impala/beeline JDBC) and the BigQuery scratch dataset, then compares results row-by-row.

```
┌─────────────────┐     ┌──────────────────┐     ┌───────────────────┐
│  Seed Data Gen  │────▶│  Legacy Cluster   │     │  BQ Scratch DS    │
│  (shared JSON/  │     │  (Impala + Hive)  │     │  (project.test)   │
│   Parquet)      │────▶│                   │     │                   │
└─────────────────┘     └────────┬──────────┘     └────────┬──────────┘
                                 │                          │
                        Execute 66 legacy         Execute 66 BQ
                        scripts in dep order      scripts in dep order
                                 │                          │
                                 ▼                          ▼
                        ┌────────────────┐        ┌────────────────┐
                        │  Legacy Output │        │  BQ Output     │
                        │  (per table)   │        │  (per table)   │
                        └────────┬───────┘        └────────┬───────┘
                                 │                          │
                                 └──────────┬───────────────┘
                                            ▼
                                 ┌──────────────────┐
                                 │  Reconciliation  │
                                 │  Engine          │
                                 │  (row count +    │
                                 │   fingerprint +  │
                                 │   per-col agg)   │
                                 └────────┬─────────┘
                                          ▼
                                 ┌──────────────────┐
                                 │  Evidence Store  │
                                 │  (JSON + CSV)    │
                                 └──────────────────┘
```

### Execution Flow

**Phase 1: Seed** — Load deterministic test data into both platforms (see Seed Data Strategy decision).

**Phase 2: Legacy Run** — Execute all 66 legacy scripts on CDH via impala-shell / beeline JDBC in dependency order. Capture output tables.

**Phase 3: BQ Run** — Execute all 66 translated BQ scripts on the scratch dataset (`project.test`) via `BigQueryInsertJobOperator` or direct `bq query` in the same dependency order.

**Phase 4: Reconcile** — For each of the 66 output objects:

1. **Row count match** — `SELECT COUNT(*) FROM legacy_table` vs `SELECT COUNT(*) FROM bq_table`. Must be exact 0-delta for all 66.
2. **Order-independent MD5 fingerprint** — Compute `MD5(CONCAT(col1, col2, ...))` per row on both sides over deterministic columns only; compare the sorted set of hashes. 0 mismatches required.
3. **Per-column aggregate comparison** — `SUM` for numerics, `COUNT(DISTINCT)` for strings/timestamps. Compare at documented DECIMAL precision. 0 deviations.

**Phase 5: Construct-specific assertions** (AC2) — Run targeted sub-tests for each semantically-divergent rewrite:

| Construct | Test Script | Assertion |
|---|---|---|
| GROUPING__ID → GROUPING() bit math | 63 + vw_csat_rollup | grouping_id matches row-for-row across all 4 rollup levels |
| NDV() → APPROX_COUNT_DISTINCT() | vw_active_agents_ndv | ±5% tolerance per date_key/site_code |
| group_concat → STRING_AGG | 17-cleanse-ivr-session | menu_path_full matches after alphabetical sort of segments |
| UNNEST rewrite | 18 + 21 | message_count, agent_message_count, scored_points, overall_pct exact match |
| pmod → MOD | 34-scd2 join | Seed negative agent_id values; assert pmod(neg, 24) = MOD(MOD(neg,24)+24,24) |
| RLIKE → REGEXP_CONTAINS | vw_call_driver_regex | call_driver + embedded_ref_no match for all 5 CASE branches |
| trunc(MONDAY) → DATE_TRUNC(WEEK(MONDAY)) | 60-agg-agent-weekly | week_start_key exact match across Monday boundary |
| INSERT OVERWRITE DIRECTORY → EXPORT DATA | 94 + 95 | Pipe-delimited output content row-for-row identical |
| WITH RECURSIVE | vw_org_hierarchy | path_names + depth match at tree depth ≥4 |
| unix_timestamp arithmetic | vw_repeat_contact_window | repeat_within_72h flag on 259200-second boundary |

**Phase 6: Dirty-data parity** (AC3) — Re-run cleanses + DQ checks with injected dirty data (~0.5% dup PKs, ~0.2% orphan FKs, ~1% bad epochs, mixed-case CRM strings). Assert identical surviving/rejected row counts.

**Phase 7: Lossy-column round-trip** (AC4) — Seed edge values (DECIMAL(14,2) at 999999999999.99, epoch millis at 1704067199999 for issued_ts_sec LIE), run full chain, read back lossy columns, assert precision preserved.

### Technology

- **Harness runtime**: Node.js ESM (extends existing `/workspace/project/bigquery/validation/validate.mjs` framework)
- **Legacy connectivity**: impala-shell CLI (for Impala scripts) and beeline CLI (for Hive-routed scripts 32–40, 63) invoked via `child_process.execSync` from Node
- **BQ connectivity**: `@google-cloud/bigquery` client (already in workspace)
- **Evidence persistence**: JSON + CSV per table under `bigquery/validation/evidence/` and `bigquery/validation/reports/`

### Pass/Fail Criteria

- **AC1**: 66/66 tables pass with 0 row-count or fingerprint mismatches on deterministic columns
- **AC2**: All exercised constructs show 0 mismatches (exact or ±5% for approximate)
- **AC3**: All 15 cleanses + 6 DQ checks produce identical dirty-data outcomes
- **AC4**: All lossy columns survive at documented precision; issued_ts_sec LIE round-trip correct
- **AC5**: Determinism classification respected (never assert exact on non-deterministic)
- **AC6**: 66/66 scripts execute with 0 errors; all ACs executed live; evidence persisted

Any output table that fails ANY check = **HARD FAIL** naming the table + reason. An unparseable/NULL aggregate is NEVER a match. A 0-row count matches only when both sides are legitimately empty.
