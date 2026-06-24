# Locked Decisions for Story d585f811-156c-4fbc-b470-dfdd0a6a4c8c

## Validation & Reconciliation Harness
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

## Seed Data Strategy
## Seed Data Strategy: Deterministic Dual-Platform Test Data

### Approach

Generate a single deterministic seed dataset that is loaded into **both** the legacy CDH cluster and the BigQuery scratch dataset. The data is designed to exercise every transform path, every trap, and every dirty-data injection point.

### Data Sources

The existing `/workspace/source/data/` directory contains:
- **`expected-counts.json`** — row counts per staging table (45 tables, ranging from 10 rows for `stg_crm_client` to 301,500 for `stg_tel_agent_state_event`)
- **`rdbms-seeds/`** — per-source-system SQL seed scripts (crm/, finance/, hr/, telephony/, ticketing/, wfm/)
- **`parquet/`** and **`text/`** — pre-generated seed files

### Seed Dataset Design

The seed data is loaded in dependency order matching the transform chain:

**Layer 1: Staging tables** (all 45) — loaded from existing seed files using:
- CDH: `LOAD DATA INPATH` or direct Parquet copy to HDFS partition paths
- BQ: `bq load --source_format=PARQUET` / `CSV` / `NEWLINE_DELIMITED_JSON` per table format

**Layer 2: ODS tables** — populated by running the 15 cleanses + delta-merges + SCD-2 + ACID merges on both platforms

**Layer 3: DM tables** — populated by running dim loads → fact loads → agg builds on both platforms

### Dirty Data Injections (per EPOCH-POLICY.md and expected-counts.json)

Injected into the staging seed data before loading:

| Injection | Rate | Affected Tables | Purpose |
|---|---|---|---|
| Duplicate PKs | ~0.5% | All staging tables | Tests ROW_NUMBER dedup in cleanses |
| Orphan FKs | ~0.2% | `ods_call.queue_id`, `ods_call.agent_id`, `ods_interaction.agent_id` | Tests 77-dq-fk-orphan-check |
| Out-of-range epochs | ~1% | `stg_tel_call.start_epoch`, `stg_wfm_schedule.start_epoch`, `stg_tkt_ticket.created_ms`, `stg_fin_invoice.issued_ts_sec` | Tests 78-dq-epoch-range-sanity |
| Mixed-case + trailing whitespace | CRM string columns | `stg_crm_client.status`, `stg_crm_program.status` | Tests UPPER(TRIM()) normalization in cleanses |

### Construct-Exercise Seed Requirements

Specific data must be seeded to exercise every construct in AC2:

| Construct | Seed Requirement |
|---|---|
| GROUPING__ID bit math | Seed enough CSAT surveys across multiple client/program/site combinations to populate all 4 grouping sets |
| NDV / APPROX_COUNT_DISTINCT | Seed ≥100 distinct agent_sk values per site_code for meaningful approximation |
| group_concat / STRING_AGG | Seed IVR sessions with ≥3 menu hops to test ordering |
| UNNEST (arrays) | Seed chat transcripts with multi-element `messages` arrays (≥3 messages); QA forms with multi-element `sections` arrays |
| pmod with negatives | Seed at least 5 `stg_hr_agent` rows with negative `agent_id` values |
| RLIKE / REGEXP_CONTAINS | Seed `dim_disposition.disposition_desc` values triggering each CASE branch: BILLING pattern, ACCESS pattern, RETENTION pattern, bracket-code `[XX]` pattern, OTHER, and `ref#12345` embedded reference |
| trunc(MONDAY) | Seed `agg_agent_daily` rows spanning a Sunday→Monday boundary (e.g., date_keys 20240107 and 20240108) |
| INSERT OVERWRITE DIRECTORY | Ensure close scripts (94, 95) have non-trivial output (≥10 rows per site/queue) |
| WITH RECURSIVE | Seed `ods_org_unit` with a self-referencing tree of depth ≥4 |
| unix_timestamp arithmetic | Seed `ods_interaction` pairs for the same `customer_ref` spaced exactly 259200 seconds (72h) apart — one pair at boundary, one within, one outside |

### Lossy-Edge Seed Values (AC4)

| Column Type | Edge Value | Table.Column |
|---|---|---|
| DECIMAL(14,2) | 999999999999.99 | `fact_billing_line.line_amount` |
| DECIMAL(5,2) | 100.00, 0.01 | `ods_qa_evaluation.overall_pct` |
| DECIMAL(12,2) | 999999999.99 | `agg_billing_monthly.sla_credit_amount` |
| DECIMAL(8,2) | 99999.99 | `agg_agent_daily.avg_handle_seconds` |
| DECIMAL(7,2) | 99999.99 | `agg_queue_hourly.volume_variance_pct` |
| Epoch millis LIE | 1704067199000 (= 2023-12-31 23:59:59 UTC) | `stg_fin_invoice.issued_ts_sec` |
| Epoch millis boundary | 1704067199999 (ms precision boundary) | `stg_fin_invoice.issued_ts_sec` |

### Data Volume

For validation purposes, use the existing seed data volumes from `expected-counts.json` (total ~1M rows across staging). This is small enough for rapid dual-execution but large enough to exercise all code paths and generate meaningful aggregate comparisons.

### Loading Order

1. Load all 45 staging tables into both platforms
2. Run the 66 scripts in dependency order on both platforms
3. Reconcile outputs per the Validation Harness decision

## Determinism Classification & Tolerance Contract
## Determinism Classification & Tolerance Contract

### Column Classification

Every output column across the 66 objects is classified into one of three categories. The reconciliation harness applies the matching assertion type — never asserting exact match on a non-deterministic column.

### 1. Deterministic Columns (majority — exact 0-mismatch required)

All columns NOT listed below are deterministic. This includes:
- All primary keys, foreign keys, surrogate keys
- All `date_key`, `event_date`, `period_month`, `period_date` columns
- All integer counts (`interactions_handled`, `days_worked`, `hops`, `message_count`, `agent_message_count`, `customer_message_count`, `section_count`, `scored_points`, `max_points`, etc.)
- All TIMESTAMP columns derived from epoch conversion (`start_ts`, `end_ts`, `issued_ts`, `evaluated_ts`, `started_ts`, etc.)
- All string columns (`client_code`, `status`, `channel`, `call_driver`, `embedded_ref_no`, `path_names`, `recon_status`, etc.)
- All BOOLEAN flags (`resolved_flag`, `contained_flag`, `auto_fail`, `is_current`, `sla_breached_flag`, `repeat_within_72h`)
- All DECIMAL columns with fully deterministic computation paths (SUM, COUNT, exact division)
- `grouping_id` / `grouping_level` (GROUPING__ID rewrite must produce identical integer values)
- `week_start_key` (trunc(MONDAY) rewrite must produce identical integer)
- `depth` in vw_org_hierarchy
- EXPORT DATA output content (pipe-delimited files from 94, 95)

**Assertion**: Order-independent MD5 fingerprint equality. 0 mismatches.

### 2. Approximate Columns (tolerance-bounded)

| Column | Source | Tolerance | Rationale |
|---|---|---|---|
| `approx_active_agents` | vw_active_agents_ndv | ±5% per (date_key, site_code) | NDV() and APPROX_COUNT_DISTINCT() are both approximate with different algorithms |
| `approx_agent_channel_pairs` | vw_active_agents_ndv | ±5% per (date_key, site_code) | Same as above |
| `avg_csat` (in aggregates) | agg_csat_rollup_monthly, agg_program_monthly | ±0.01 | AVG over DECIMAL with float intermediate rounding may differ between engines |
| `pct_promoters`, `pct_detractors` | agg_csat_rollup_monthly | ±0.01 | Same — percentage computed from COUNT ratios with DECIMAL cast |
| `avg_handle_seconds` (in aggregates) | agg_agent_daily, agg_agent_weekly, agg_program_monthly, agg_site_daily | ±0.01 | AVG with DECIMAL(8,2) cast |
| `adherence_pct`, `occupancy_pct` (in aggregates) | agg_agent_daily, agg_agent_weekly, agg_site_daily | ±0.01 | AVG with DECIMAL(5,2) cast |
| `sl_pct` | agg_queue_hourly, agg_site_daily | ±0.01 | Division-based percentage |
| `volume_variance_pct` | agg_queue_hourly | ±0.01 | Division-based percentage with DECIMAL(7,2) |
| `avg_talk_seconds` | vw_call_driver_regex | ±0.01 | AVG aggregate |
| `occupancy_pct` | vw_occupancy_utilization | ±0.01 | Division-based |
| `shrinkage_pct` | vw_shrinkage_analysis | ±0.01 | Division-based |
| `aht_pctile` | vw_agent_scorecard | ±0.01 | PERCENT_RANK() may have float precision differences |
| `fcr_pct` | vw_first_contact_resolution | ±0.01 | Division-based percentage |
| `attrition_pct` | 94-close-headcount-snapshot | ±0.01 | DECIMAL(5,2) percentage |

**Assertion**: Absolute difference ≤ stated tolerance per row (after join on key columns). Exceeding tolerance = FAIL.

### 3. Order-Sensitive Columns (canonicalize before comparison)

| Column | Source | Canonicalization | Rationale |
|---|---|---|---|
| `menu_path_full` | 17-cleanse-ivr-session / ods_ivr_session | Sort segments by alphabetical order (split on ` > `, sort, rejoin) before MD5 | `group_concat` / `STRING_AGG` do not guarantee consistent ordering across engines |

**Assertion**: After canonicalization (alphabetical sort of ` > `-delimited segments), exact string match required. Divergence beyond ordering = HARD FAIL.

### Classification Completeness Rule

- Every column in every output table MUST be classified before the reconciliation runs
- The harness generates a `column_classification.json` manifest listing every column with its class (deterministic/approximate/order-sensitive)
- Any column present in output but NOT in the classification = **NOT-EXERCISED** (reported, not auto-passed)
- Any construct present in the estate but NOT exercised by the seed data = **NOT-EXERCISED** (reported in the final summary as `constructs-checked X/Y`)

### Evidence Output Format

The final reconciliation report prints:
```
reconciled N/N tables, C/C columns
constructs-checked X/Y
lossy-probed X/Y
NOT-EXERCISED constructs: [list]
NOT-EXERCISED lossy edges: [list]
```

## Determinism Classification & Tolerance Contract
## Determinism Classification & Tolerance Contract

Every output column across the 66 objects is classified into one of three categories. The reconciliation harness enforces the matching rule per classification — never asserting exact equality on a non-deterministic column.

### Classification: Deterministic (majority)

**Matching rule**: Exact 0-mismatch fingerprint equality.

All columns not listed below are deterministic. This includes:
- All surrogate keys (`*_sk`, `*_id`), natural keys, foreign keys
- All `date_key`, `event_date`, `period_month`, `period_date` partition columns
- All `TIMESTAMP` columns derived from epoch conversion (`start_ts`, `end_ts`, `issued_ts`, `evaluated_ts`, etc.)
- All `INT64` counts (`interactions_handled`, `hops`, `message_count`, `agent_message_count`, `customer_message_count`, `scored_points`, `max_points`, `section_count`, `headcount`, `leavers`, `surveys`, `days_worked`, `offered`, `answered`, `abandoned`, `answered_in_sl`, `open_tickets`, `sla_breached_tickets`, `touch_count`, `resolution_minutes`, etc.)
- All `BOOL` flags (`resolved_flag`, `contained_flag`, `auto_fail`, `is_current`, `adjustment_flag`, `sla_breached_flag`, `fcr_claimed`)
- All `STRING` columns (`channel`, `status`, `client_code`, `queue_code`, `site_code`, `call_driver`, `embedded_ref_no`, `recon_status`, `attrition_risk`, `role_on_program`, `path_names`, etc.)
- All `NUMERIC` columns that are direct sums or pass-throughs (not AVG-based): `line_amount`, `billed_amount`, `net_revenue`, `sla_credit_amount`, `telco_cost_amount`, `computed_penalty`, `posted_credits`, `delta_to_post`, `total_amount`, `qty`, `unit_rate`
- `GROUPING__ID` / `grouping_id` / `grouping_level` — exact match required (the bit-math rewrite must produce identical values)
- `repeat_within_72h` flag — exact match (boundary test at exactly 259200 seconds)
- `week_start_key` — exact match (Monday-anchor rewrite must be identical)
- `depth` in `vw_org_hierarchy` — exact match
- `first_response_seconds` — exact match (integer division from millis)
- `sl_pct` in `vw_queue_sla_attainment` — exact match (computed as integer ratio × 100)

### Classification: Approximate (±tolerance)

**Matching rule**: Value within stated tolerance band. Never exact-match.

| Column(s) | Tables/Views | Tolerance | Reason |
|---|---|---|---|
| `approx_active_agents` | `vw_active_agents_ndv` | ±5% per `date_key`/`site_code` | NDV→APPROX_COUNT_DISTINCT is inherently approximate |
| `approx_agent_channel_pairs` | `vw_active_agents_ndv` | ±5% per `date_key`/`site_code` | Same |
| `avg_csat` | `agg_csat_rollup_monthly`, `agg_program_monthly`, `vw_csat_rollup`, `vw_client_executive_summary` | ±0.01 | AVG over float intermediates; rounding path may differ |
| `avg_handle_seconds` | `agg_agent_daily`, `agg_agent_weekly`, `agg_program_monthly`, `vw_agent_scorecard`, `vw_client_executive_summary` | ±0.01 | AVG-based NUMERIC(8,2) |
| `adherence_pct` | `agg_agent_daily`, `agg_agent_weekly`, `vw_agent_scorecard` | ±0.01 | AVG-based NUMERIC(5,2) |
| `occupancy_pct` | `agg_agent_daily`, `agg_agent_weekly`, `vw_occupancy_utilization` | ±0.01 | Ratio-based NUMERIC(5,2) |
| `sl_pct` | `agg_queue_hourly`, `agg_site_daily` | ±0.01 | Ratio-based NUMERIC(5,2) |
| `attrition_pct` | 94-close-headcount-snapshot | ±0.01 | Ratio-based NUMERIC(5,2) |
| `pct_promoters`, `pct_detractors` | `agg_csat_rollup_monthly`, `vw_csat_rollup`, `vw_client_executive_summary` | ±0.01 | Ratio-based NUMERIC(5,2) |
| `volume_variance_pct` | `agg_queue_hourly` | ±0.01 | Ratio-based NUMERIC(7,2) |
| `shrinkage_pct` | `vw_shrinkage_analysis` | ±0.01 | Ratio-based |
| `fcr_pct` | `vw_first_contact_resolution` | ±0.01 | Ratio-based |
| `avg_talk_seconds` | `vw_call_driver_regex` | ±0.01 | AVG-based |
| `avg_qa_pct` | `vw_agent_scorecard` | ±0.01 | AVG of AVG |
| `adherence_90d` | `vw_attrition_risk` | ±0.01 | AVG-based |
| `aht_pctile` | `vw_agent_scorecard` | ±0.01 | PERCENT_RANK float |
| `est_labor_cost`, `est_margin` | `vw_program_margin` | ±0.01 | Float multiplication chain |
| `avg_speed_answer_sec`, `avg_handle_sec` | `fact_queue_interval` | ±0.01 | AVG-based NUMERIC(8,2) |

### Classification: Order-Sensitive

**Matching rule**: Canonicalize (sort segments alphabetically) before hashing. Match after canonicalization.

| Column | Table | Canonicalization |
|---|---|---|
| `menu_path_full` | `ods_ivr_session`, `fact_ivr_path` | Split on ` > `, sort segments alphabetically, rejoin with ` > `, then compare |

### DECIMAL Precision Contract

All DECIMAL/NUMERIC columns preserve their documented precision through the full compute path:

| Precision | Columns | Assertion |
|---|---|---|
| NUMERIC(5,2) | `overall_pct`, `adherence_pct`, `occupancy_pct`, `sl_pct`, `attrition_pct`, `pct_promoters`, `pct_detractors`, `avg_csat`, `penalty_pct` | Scale=2 preserved; no silent truncation |
| NUMERIC(14,2) | `billed_amount`, `net_revenue`, `line_amount` (summed), `total_amount` | Scale=2 preserved at max value 999999999999.99 |
| NUMERIC(12,2) | `sla_credit_amount`, `telco_cost_amount`, `computed_penalty` | Scale=2 preserved |
| NUMERIC(8,2) | `avg_handle_seconds`, `avg_speed_answer_sec`, `avg_handle_sec` | Scale=2 preserved |
| NUMERIC(7,2) | `volume_variance_pct` | Scale=2 preserved |
| NUMERIC(12,4) | `unit_rate` | Scale=4 preserved |

### Harness Enforcement

The reconciliation harness reads this classification to select the correct comparison function per column:
- **Deterministic** → `exactMatch(legacy_val, bq_val)`
- **Approximate** → `withinTolerance(legacy_val, bq_val, tolerance)`
- **Order-sensitive** → `canonicalMatch(legacy_val, bq_val, separator, sortFn)`

The classification is stored as a JSON manifest (`determinism_manifest.json`) consumed by the harness at runtime. Any column present in the output but NOT in the manifest defaults to **deterministic** (strictest).

### NOT-EXERCISED Tracking

If the seed data fails to exercise a construct (e.g., no negative agent_ids seeded for pmod test), the harness reports that construct as `NOT-EXERCISED` — never as `PASS`. The final report enumerates: `constructs-checked X/Y, lossy-probed X/Y` with explicit listing of any gap.
