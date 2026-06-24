# Seed Data Strategy

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
