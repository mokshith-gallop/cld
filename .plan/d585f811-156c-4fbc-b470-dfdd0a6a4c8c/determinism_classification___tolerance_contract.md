# Determinism Classification & Tolerance Contract

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
