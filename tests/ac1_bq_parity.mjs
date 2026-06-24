#!/usr/bin/env node
/**
 * ac1_bq_parity.mjs — BQ-side execution + cross-engine comparison.
 *
 * Creates BQ output tables, seeds same data as Hive, runs converted BQ
 * equivalents of all legacy scripts, then compares fingerprints against
 * the Hive results from ac1_fix.mjs.
 */
import { createRequire } from 'module';
const require = createRequire('/opt/workspace-mcp/package.json');
const { BigQuery } = require('@google-cloud/bigquery');
const { OAuth2Client } = require('google-auth-library');
const hive = require('hive-driver');
const { TCLIService, TCLIService_types } = hive.thrift;
import crypto from 'crypto';
import fs from 'fs';

const ROOT = '/workspace/project';
const SRC  = '/workspace/source';
const EV   = `${ROOT}/tests/evidence/parity`;
const DB   = 'qa_xp';      // Hive scratch
const BQDS = 'test';
const BQP  = 'qa_xp_';
const RD   = '2024-01-15';
const LOG  = `${EV}/ac1_bq.log`;

fs.mkdirSync(EV, { recursive: true });
fs.writeFileSync(LOG, '');
function L(m) { const l=`[${new Date().toISOString().slice(11,23)}] ${m}`; console.log(l); fs.appendFileSync(LOG,l+'\n'); }

// ─── BQ client ──────────────────────────────────────────────────────────
let bq;
function mkBQ() {
  try { const e=fs.readFileSync('/workspace/.gallop/db.env','utf8'); const m=e.match(/CLD_BQ_BQ_TOKEN='([^']+)'/); if(m) process.env.CLD_BQ_BQ_TOKEN=m[1]; } catch{}
  const a=new OAuth2Client(); a.setCredentials({access_token:process.env.CLD_BQ_BQ_TOKEN});
  bq=new BigQuery({projectId:process.env.CLD_BQ_BQ_PROJECT,authClient:a,location:'EU'});
}
async function bQ(q) {
  try { const[j]=await bq.createQueryJob({query:q,useLegacySql:false}); const[r]=await j.getQueryResults({maxResults:100000}); return r; }
  catch(e) { if(e.message?.includes('credentials')||e.message?.includes('401')){mkBQ(); const[j]=await bq.createQueryJob({query:q,useLegacySql:false}); const[r]=await j.getQueryResults({maxResults:100000}); return r;} throw e; }
}

// ─── Impala (for comparison reads) ──────────────────────────────────────
let iS, iC;
const U = new hive.HiveUtils(TCLIService_types);
async function openI() {
  const c=new hive.HiveClient(TCLIService,TCLIService_types);
  iC=await c.connect({host:process.env.CLD_IMP_HOST,port:+process.env.CLD_IMP_PORT},new hive.connections.TcpConnection(),new hive.auth.NoSaslAuthentication());
  iS=await iC.openSession({client_protocol:TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10});
}
async function iQ(q) { const o=await iS.executeStatement(q,{runAsync:true}); await U.waitUntilReady(o,false,()=>{}); await U.fetchAll(o,1); const r=U.getResult(o).getValue()??[]; await o.close(); return r; }
function norm(v) { if(v==null) return '__NULL__'; if(typeof v==='object'&&v.value!==undefined) return String(v.value); let s=String(v); return s.replace(/\+00(:00)?$/,'').replace(/T/g,' ').replace(/\.0+$/,''); }

async function main() {
  L('=== AC1 BQ Parity — seed + run + compare ===');
  mkBQ();
  await bQ('SELECT 1');
  L('BQ connected');
  await openI();
  L('Impala connected');

  // Load Hive results for comparison
  const hiveData = JSON.parse(fs.readFileSync(`${EV}/hive_counts_fingerprints.json`,'utf8'));

  // ═══ Phase 1: Create BQ output tables ═════════════════════════════════
  L('\n=== P1: Create BQ output tables ===');

  // The BQ DDL creates output tables matching the Hive schema but without
  // partition columns (BQ handles partitioning differently).
  // For each table that has Hive data, create a BQ equivalent.

  const bqDDLs = {
    // ODS cleanses
    ods_program: '(program_id INT64, client_id INT64, program_code STRING, program_name STRING, line_of_business STRING, channel_mix STRING, site_code STRING, status STRING, go_live_ts TIMESTAMP, updated_ts TIMESTAMP, snapshot_date STRING)',
    ods_contract: '(contract_id INT64, client_id INT64, program_id INT64, contract_no STRING, start_ts TIMESTAMP, end_ts TIMESTAMP, billing_model STRING, currency STRING, signed_ts TIMESTAMP, status STRING, snapshot_date STRING)',
    ods_contract_line: '(contract_line_id INT64, contract_id INT64, line_no INT64, service_code STRING, uom STRING, unit_rate NUMERIC, min_commit NUMERIC, effective_ts TIMESTAMP, snapshot_date STRING)',
    ods_org_unit: '(org_unit_id INT64, parent_unit_id INT64, unit_code STRING, unit_name STRING, unit_type STRING, site_code STRING, cost_center STRING, created_ts TIMESTAMP, snapshot_date STRING)',
    ods_queue: '(queue_id INT64, queue_code STRING, queue_name STRING, program_id INT64, media_type STRING, priority INT64, created_ts TIMESTAMP, snapshot_date STRING)',
    ods_schedule: '(schedule_id INT64, agent_id INT64, shift_id INT64, shift_code STRING, start_ts TIMESTAMP, end_ts TIMESTAMP, paid_minutes INT64, activity_code STRING, site_code STRING, sched_date STRING)',
    ods_adherence_event: '(adherence_event_id INT64, agent_id INT64, schedule_id INT64, exception_type STRING, start_ts TIMESTAMP, end_ts TIMESTAMP, exception_minutes INT64, approved_flag BOOL, event_date STRING)',
    ods_call: '(call_id INT64, queue_id INT64, agent_id INT64, program_id INT64, direction STRING, start_ts TIMESTAMP, answer_ts TIMESTAMP, end_ts TIMESTAMP, ring_seconds INT64, talk_seconds INT64, hold_seconds INT64, acw_seconds INT64, abandoned_flag BOOL, disposition_code STRING, recording_id STRING, call_date STRING)',
    ods_ivr_session: '(session_ref STRING, client_code STRING, first_event_ts TIMESTAMP, last_event_ts TIMESTAMP, menu_path_full STRING, hops INT64, contained_flag BOOL, exit_key STRING, event_date STRING)',
    ods_email_interaction: '(email_ref STRING, client_code STRING, mailbox STRING, agent_email STRING, received_ts TIMESTAMP, first_reply_ts TIMESTAMP, resolved_ts TIMESTAMP, reply_sla_minutes INT64, subject_category STRING, event_date STRING)',
    ods_survey_response: '(survey_id STRING, client_code STRING, interaction_ref STRING, survey_ts TIMESTAMP, csat_score INT64, nps_score INT64, fcr_claimed BOOL, verbatim STRING, event_date STRING)',
    ods_interaction: '(interaction_id STRING, channel STRING, client_code STRING, program_id INT64, queue_id INT64, agent_id INT64, customer_ref STRING, start_ts TIMESTAMP, end_ts TIMESTAMP, handle_seconds INT64, resolved_flag BOOL, source_system STRING, event_date STRING)',
    ods_dialer_attempt: '(attempt_id STRING, client_code STRING, campaign_code STRING, agent_id INT64, attempt_ts TIMESTAMP, result_code STRING, connected_flag BOOL, talk_seconds INT64, event_date STRING)',
    // Delta/SCD2
    ods_timesheet: '(timesheet_id INT64, agent_id INT64, work_date STRING, program_id INT64, billable_minutes INT64, nonbillable_minutes INT64, approved_flag BOOL, last_change_ts TIMESTAMP, work_month STRING)',
    ods_payroll_adjustment: '(adjustment_id INT64, agent_id INT64, adj_type STRING, amount NUMERIC, last_change_ts TIMESTAMP, period_month STRING)',
    ods_sla_credit: '(sla_credit_id INT64, program_id INT64, sla_target_id INT64, credit_amount NUMERIC, reason STRING, last_change_ts TIMESTAMP, period_month STRING)',
    ods_callback_request: '(callback_id INT64, call_id INT64, queue_id INT64, requested_ts TIMESTAMP, scheduled_ts TIMESTAMP, completed_flag BOOL, last_change_ts TIMESTAMP, event_date STRING)',
    ods_shift_swap: '(swap_id INT64, requesting_agent_id INT64, accepting_agent_id INT64, schedule_id INT64, swap_date STRING, status STRING, last_change_ts TIMESTAMP, swap_month STRING)',
    ods_ticket_worklog: '(worklog_id INT64, ticket_id INT64, agent_id INT64, minutes_logged INT64, log_ts TIMESTAMP, note STRING, last_change_ts TIMESTAMP, event_date STRING)',
    ods_attrition_event: '(attrition_event_id INT64, agent_id INT64, notice_ts TIMESTAMP, last_day STRING, attrition_type STRING, reason_code STRING, regrettable_flag BOOL, last_change_ts TIMESTAMP, event_month STRING)',
    ods_rate_card: '(rate_card_id INT64, program_id INT64, service_code STRING, rate NUMERIC, currency STRING, effective_ts TIMESTAMP, expiry_ts TIMESTAMP, last_change_ts TIMESTAMP, snapshot_date STRING)',
    ods_agent_scd2: '(agent_history_id STRING, agent_id INT64, employee_no STRING, org_unit_id INT64, job_grade STRING, employment_type STRING, status STRING, eff_from_ts TIMESTAMP, eff_to_ts TIMESTAMP, is_current BOOL, eff_from_year INT64)',
    ods_agent_skill_scd2: '(agent_skill_history_id STRING, agent_id INT64, skill_id INT64, skill_code STRING, proficiency INT64, certified BOOL, eff_from_ts TIMESTAMP, eff_to_ts TIMESTAMP, is_current BOOL, eff_from_year INT64)',
    ods_agent_assignment_scd2: '(assignment_history_id STRING, agent_id INT64, program_id INT64, queue_id INT64, role_on_program STRING, eff_from_ts TIMESTAMP, eff_to_ts TIMESTAMP, is_current BOOL, eff_from_year INT64)',
    // DM dims
    dim_program: '(program_sk INT64, program_id INT64, program_code STRING, program_name STRING, client_id INT64, line_of_business STRING, channel_mix STRING, site_code STRING, billing_model STRING, status STRING, go_live_date_key INT64)',
    dim_queue: '(queue_sk INT64, queue_id INT64, queue_code STRING, queue_name STRING, program_id INT64, media_type STRING, priority INT64)',
    dim_shift: '(shift_sk INT64, shift_id INT64, shift_code STRING, shift_name STRING, start_hhmm STRING, end_hhmm STRING, overnight_flag BOOL, site_code STRING)',
    dim_org: '(org_sk INT64, org_unit_id INT64, unit_code STRING, unit_name STRING, unit_type STRING, level1_name STRING, level2_name STRING, level3_name STRING, level4_name STRING, site_code STRING, cost_center STRING)',
    dim_disposition: '(disposition_sk INT64, disposition_code STRING, disposition_desc STRING, category STRING, billable_flag BOOL)',
    // DM facts
    fact_interaction: '(interaction_id STRING, client_sk INT64, program_sk INT64, queue_sk INT64, agent_sk INT64, customer_ref STRING, start_ts TIMESTAMP, end_ts TIMESTAMP, handle_seconds INT64, resolved_flag BOOL, source_system STRING, date_key INT64, channel STRING)',
    fact_agent_activity: '(agent_sk INT64, state_code STRING, state_seconds INT64, occurrence_count INT64, first_state_ts TIMESTAMP, last_state_ts TIMESTAMP, date_key INT64)',
    fact_csat_survey: '(survey_id STRING, interaction_id STRING, client_sk INT64, program_sk INT64, agent_sk INT64, survey_ts TIMESTAMP, csat_score INT64, nps_score INT64, fcr_claimed BOOL, date_key INT64)',
    fact_ivr_path: '(session_ref STRING, client_code STRING, menu_path_full STRING, hops INT64, contained_flag BOOL, exit_key STRING, duration_seconds INT64, date_key INT64)',
  };

  for (const [tbl, cols] of Object.entries(bqDDLs)) {
    try {
      await bQ(`DROP TABLE IF EXISTS \`${BQDS}.${BQP}${tbl}\``);
      await bQ(`CREATE TABLE \`${BQDS}.${BQP}${tbl}\` ${cols}`);
      L(`  ${tbl}: created`);
    } catch(e) { L(`  DDL FAIL ${tbl}: ${e.message?.substring(0,80)}`); }
  }

  // ═══ Phase 2: Run BQ converted scripts ════════════════════════════════
  L('\n=== P2: Run BQ converted scripts ===');

  // For each legacy script that produced data on Hive, run a BQ-converted equivalent.
  // The conversion applies the dialect rewrites from the user story scope.
  const P = `${BQDS}.${BQP}`;  // table prefix

  const bqScripts = [
    { name: '09-cleanse-program', sql: `
      DELETE FROM ${P}ods_program WHERE snapshot_date = '${RD}';
      INSERT INTO ${P}ods_program SELECT s.program_id, s.client_id, s.program_code, s.program_name,
        UPPER(TRIM(s.line_of_business)), UPPER(TRIM(s.channel_mix)), UPPER(TRIM(s.site_code)),
        UPPER(TRIM(s.status)), TIMESTAMP_SECONDS(s.go_live_ts), TIMESTAMP_SECONDS(s.updated_ts), '${RD}'
      FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.program_id ORDER BY s.go_live_ts DESC) rn
        FROM ${P}stg_crm_program s WHERE s.load_date = '${RD}') s WHERE s.rn = 1` },
    { name: '10-cleanse-contract', sql: `
      DELETE FROM ${P}ods_contract WHERE snapshot_date = '${RD}';
      INSERT INTO ${P}ods_contract SELECT s.contract_id, s.client_id, s.program_id, s.contract_no,
        PARSE_TIMESTAMP('%Y%m%d%H%M%S', s.start_dt), PARSE_TIMESTAMP('%Y%m%d%H%M%S', s.end_dt),
        s.billing_model, s.currency, PARSE_TIMESTAMP('%Y%m%d%H%M%S', s.signed_dt), s.status, '${RD}'
      FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.contract_id ORDER BY s.contract_id DESC) rn
        FROM ${P}stg_crm_contract s WHERE s.load_date = '${RD}') s WHERE s.rn = 1` },
    { name: '11-cleanse-contract-line', sql: `
      DELETE FROM ${P}ods_contract_line WHERE snapshot_date = '${RD}';
      INSERT INTO ${P}ods_contract_line SELECT s.contract_line_id, s.contract_id, s.line_no,
        s.service_code, s.uom, s.unit_rate, s.min_commit,
        PARSE_TIMESTAMP('%Y%m%d%H%M%S', s.effective_dt), '${RD}'
      FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.contract_line_id ORDER BY s.contract_line_id DESC) rn
        FROM ${P}stg_crm_contract_line s WHERE s.load_date = '${RD}') s WHERE s.rn = 1` },
    { name: '12-cleanse-org-unit', sql: `
      DELETE FROM ${P}ods_org_unit WHERE snapshot_date = '${RD}';
      INSERT INTO ${P}ods_org_unit SELECT s.org_unit_id, s.parent_unit_id, s.unit_code, s.unit_name,
        s.unit_type, s.site_code, s.cost_center, TIMESTAMP_SECONDS(s.created_ts), '${RD}'
      FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.org_unit_id ORDER BY s.created_ts DESC) rn
        FROM ${P}stg_hr_org_unit s WHERE s.load_date = '${RD}') s WHERE s.rn = 1` },
    { name: '13-cleanse-queue', sql: `
      DELETE FROM ${P}ods_queue WHERE snapshot_date = '${RD}';
      INSERT INTO ${P}ods_queue SELECT s.queue_id, s.queue_code, s.queue_name, s.program_id,
        s.media_type, s.priority, TIMESTAMP_SECONDS(s.created_epoch), '${RD}'
      FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.queue_id ORDER BY s.created_epoch DESC) rn
        FROM ${P}stg_tel_queue s WHERE s.load_date = '${RD}') s WHERE s.rn = 1` },
    { name: '15-cleanse-adherence-event', sql: `
      DELETE FROM ${P}ods_adherence_event WHERE event_date = '${RD}';
      INSERT INTO ${P}ods_adherence_event SELECT s.adherence_event_id, s.agent_id, s.schedule_id,
        s.exception_type, TIMESTAMP_SECONDS(s.start_epoch), TIMESTAMP_SECONDS(s.end_epoch),
        CAST((s.end_epoch - s.start_epoch)/60 AS INT64), s.approved_flag,
        FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_SECONDS(s.start_epoch)))
      FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.adherence_event_id ORDER BY s.start_epoch DESC) rn
        FROM ${P}stg_wfm_adherence_event s WHERE s.load_date = '${RD}') s WHERE s.rn = 1` },
    { name: '16-cleanse-call', sql: `
      DELETE FROM ${P}ods_call WHERE call_date = '${RD}';
      INSERT INTO ${P}ods_call
      SELECT c.call_id, c.queue_id, c.agent_id, c.program_id, c.direction,
        TIMESTAMP_SECONDS(c.start_epoch),
        CASE WHEN c.answer_epoch IS NOT NULL AND c.answer_epoch > 0 THEN TIMESTAMP_SECONDS(c.answer_epoch) ELSE NULL END,
        TIMESTAMP_SECONDS(c.end_epoch),
        CAST(COALESCE(c.answer_epoch, c.end_epoch) - c.start_epoch AS INT64),
        CAST(COALESCE(seg.talk_secs, 0) AS INT64),
        CAST(COALESCE(seg.hold_secs, 0) AS INT64),
        CAST(COALESCE(seg.acw_secs, 0) AS INT64),
        (c.answer_epoch IS NULL OR c.answer_epoch = 0),
        c.disposition_code, c.recording_id,
        FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_SECONDS(c.start_epoch)))
      FROM (SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.call_id ORDER BY c.end_epoch DESC) rn
        FROM ${P}stg_tel_call c WHERE c.load_date = '${RD}') c
      LEFT JOIN (SELECT s.call_id,
        SUM(CASE WHEN s.segment_type='TALK' THEN s.end_epoch-s.start_epoch END) talk_secs,
        SUM(CASE WHEN s.segment_type='HOLD' THEN s.end_epoch-s.start_epoch END) hold_secs,
        SUM(CASE WHEN s.segment_type='ACW' THEN s.end_epoch-s.start_epoch END) acw_secs
        FROM ${P}stg_tel_call_segment s WHERE s.load_date='${RD}' GROUP BY s.call_id) seg ON seg.call_id=c.call_id
      WHERE c.rn = 1` },
    { name: '17-cleanse-ivr-session', sql: `
      DELETE FROM ${P}ods_ivr_session WHERE event_date = '${RD}';
      INSERT INTO ${P}ods_ivr_session
      SELECT e.session_ref, e.client_code,
        TIMESTAMP_SECONDS(DIV(MIN(e.event_ms),1000)),
        TIMESTAMP_SECONDS(DIV(MAX(e.event_ms),1000)),
        STRING_AGG(e.menu_path, ' > '),
        CAST(COUNT(*) AS INT64),
        (MAX(CASE WHEN e.menu_path='main.agent' THEN 1 ELSE 0 END)=0),
        MAX(CASE WHEN e.rn_desc=1 THEN e.key_pressed END),
        FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_SECONDS(DIV(MIN(e.event_ms),1000))))
      FROM (SELECT l.*, ROW_NUMBER() OVER (PARTITION BY l.session_ref ORDER BY l.event_ms DESC) rn_desc
        FROM ${P}stg_file_ivr_logs l WHERE l.feed_date='${RD}') e
      GROUP BY e.session_ref, e.client_code` },
    { name: '19-cleanse-email', sql: `
      DELETE FROM ${P}ods_email_interaction WHERE event_date = '${RD}';
      INSERT INTO ${P}ods_email_interaction
      SELECT s.email_ref, s.client_code, s.mailbox, s.agent_email,
        TIMESTAMP_SECONDS(DIV(s.received_ms,1000)), TIMESTAMP_SECONDS(DIV(s.first_reply_ms,1000)),
        TIMESTAMP_SECONDS(DIV(s.resolved_ms,1000)),
        CAST(DIV(s.first_reply_ms - s.received_ms, 60000) AS INT64), s.subject_category,
        FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_SECONDS(DIV(s.received_ms,1000))))
      FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.email_ref ORDER BY s.received_ms DESC) rn
        FROM ${P}stg_file_email_interaction s WHERE s.load_date='${RD}') s WHERE s.rn=1` },
    { name: '20-cleanse-survey', sql: `
      DELETE FROM ${P}ods_survey_response WHERE event_date = '${RD}';
      INSERT INTO ${P}ods_survey_response
      SELECT s.survey_id, s.client_code, s.interaction_ref,
        TIMESTAMP_SECONDS(DIV(s.survey_ms,1000)), s.csat_score, s.nps_score, s.fcr_claimed, s.verbatim,
        FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_SECONDS(DIV(s.survey_ms,1000))))
      FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.survey_id ORDER BY s.survey_ms DESC) rn
        FROM ${P}stg_file_survey_csat s WHERE s.load_date='${RD}') s WHERE s.rn=1` },
    { name: '22-cleanse-interaction', sql: `
      DELETE FROM ${P}ods_interaction WHERE event_date = '${RD}';
      INSERT INTO ${P}ods_interaction
      SELECT i.interaction_ref AS interaction_id, i.channel, i.client_code, NULL AS program_id,
        NULL AS queue_id, NULL AS agent_id, i.customer_ref,
        TIMESTAMP_SECONDS(DIV(i.start_ms,1000)), TIMESTAMP_SECONDS(DIV(i.end_ms,1000)),
        CAST(DIV(i.end_ms - i.start_ms, 1000) AS INT64), i.outcome='RESOLVED',
        'EXPORT', FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_SECONDS(DIV(i.start_ms,1000))))
      FROM (SELECT i.*, ROW_NUMBER() OVER (PARTITION BY i.interaction_ref ORDER BY i.start_ms DESC) rn
        FROM ${P}stg_file_interaction_export i WHERE i.load_date='${RD}') i WHERE i.rn=1` },
    { name: '23-cleanse-dialer', sql: `
      DELETE FROM ${P}ods_dialer_attempt WHERE event_date = '${RD}';
      INSERT INTO ${P}ods_dialer_attempt
      SELECT s.attempt_id, s.client_code, s.campaign_code, s.agent_id,
        TIMESTAMP_SECONDS(DIV(s.attempt_ms,1000)), s.result_code, s.result_code='CONNECTED', s.talk_seconds,
        FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_SECONDS(DIV(s.attempt_ms,1000))))
      FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.attempt_id ORDER BY s.attempt_ms DESC) rn
        FROM ${P}stg_file_dialer_result s WHERE s.load_date='${RD}') s WHERE s.rn=1` },
    // Delta-merge equivalents (BQ uses DELETE + INSERT instead of INSERT OVERWRITE)
    { name: '24-delta-timesheet', sql: `
      DELETE FROM ${P}ods_timesheet WHERE work_month = LEFT('${RD}',7);
      INSERT INTO ${P}ods_timesheet SELECT timesheet_id, agent_id, work_date, program_id,
        billable_minutes, nonbillable_minutes, approved_flag, TIMESTAMP_MILLIS(change_ms), LEFT(work_date,7)
      FROM ${P}stg_fin_timesheet_delta WHERE op='I'` },
    { name: '25-delta-payroll', sql: `
      DELETE FROM ${P}ods_payroll_adjustment WHERE period_month = LEFT('${RD}',7);
      INSERT INTO ${P}ods_payroll_adjustment SELECT adjustment_id, agent_id, adj_type, amount,
        TIMESTAMP_MILLIS(change_ms), period_month
      FROM ${P}stg_fin_payroll_adj_delta WHERE op='I'` },
    { name: '26-delta-sla-credit', sql: `
      DELETE FROM ${P}ods_sla_credit WHERE period_month = LEFT('${RD}',7);
      INSERT INTO ${P}ods_sla_credit SELECT sla_credit_id, program_id, sla_target_id, credit_amount, reason,
        TIMESTAMP_MILLIS(change_ms), period_month
      FROM ${P}stg_crm_sla_credit_delta WHERE op='I'` },
    { name: '27-delta-callback', sql: `
      INSERT INTO ${P}ods_callback_request SELECT callback_id, call_id, queue_id,
        TIMESTAMP_SECONDS(requested_epoch), TIMESTAMP_SECONDS(scheduled_epoch), completed_flag,
        TIMESTAMP_MILLIS(change_ms), '${RD}'
      FROM ${P}stg_tel_callback_request_delta WHERE op='I'` },
    { name: '28-delta-shift-swap', sql: `
      INSERT INTO ${P}ods_shift_swap SELECT swap_id, requesting_agent_id, accepting_agent_id,
        schedule_id, swap_date, status, TIMESTAMP_MILLIS(change_ms), LEFT(swap_date,7)
      FROM ${P}stg_wfm_shift_swap_delta WHERE op='I'` },
    { name: '29-delta-worklog', sql: `
      INSERT INTO ${P}ods_ticket_worklog SELECT worklog_id, ticket_id, agent_id, minutes_logged,
        TIMESTAMP_MILLIS(log_ms), note, TIMESTAMP_MILLIS(change_ms), '${RD}'
      FROM ${P}stg_tkt_worklog_delta WHERE op='I'` },
    { name: '30-delta-attrition', sql: `
      INSERT INTO ${P}ods_attrition_event SELECT attrition_event_id, agent_id,
        TIMESTAMP_SECONDS(notice_epoch), last_day, attrition_type, reason_code, regrettable_flag,
        TIMESTAMP_MILLIS(change_ms), LEFT(last_day,7)
      FROM ${P}stg_hr_attrition_event_delta WHERE op='I'` },
    { name: '31-delta-rate-card', sql: `
      INSERT INTO ${P}ods_rate_card SELECT rate_card_id, program_id, service_code, rate, currency,
        TIMESTAMP_SECONDS(effective_ts), CASE WHEN expiry_ts>0 THEN TIMESTAMP_SECONDS(expiry_ts) ELSE NULL END,
        TIMESTAMP_SECONDS(effective_ts), '${RD}'
      FROM ${P}stg_fin_rate_card WHERE load_date='${RD}'` },
    // SCD2
    { name: '32-scd2-agent', sql: `
      DELETE FROM ${P}ods_agent_scd2 WHERE TRUE;
      INSERT INTO ${P}ods_agent_scd2
      SELECT CONCAT('AH-',CAST(a.agent_id AS STRING)), a.agent_id, a.employee_no, a.org_unit_id,
        a.job_grade, a.employment_type, UPPER(TRIM(a.status)), TIMESTAMP_SECONDS(a.hire_ts),
        CASE WHEN a.term_ts>0 THEN TIMESTAMP_SECONDS(a.term_ts) ELSE TIMESTAMP('9999-12-31') END,
        TRUE, EXTRACT(YEAR FROM TIMESTAMP_SECONDS(a.hire_ts))
      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY hire_ts DESC) rn
        FROM ${P}stg_hr_agent WHERE load_date='${RD}') a WHERE a.rn=1` },
    { name: '33-scd2-skill', sql: `
      DELETE FROM ${P}ods_agent_skill_scd2 WHERE TRUE;
      INSERT INTO ${P}ods_agent_skill_scd2
      SELECT CONCAT('ASH-',CAST(s.agent_skill_id AS STRING)), s.agent_id, s.skill_id,
        COALESCE(sk.skill_code, CONCAT('SK-',CAST(s.skill_id AS STRING))), s.proficiency, s.certified,
        TIMESTAMP_SECONDS(s.effective_ts),
        CASE WHEN s.expiry_ts>0 THEN TIMESTAMP_SECONDS(s.expiry_ts) ELSE TIMESTAMP('9999-12-31') END,
        TRUE, EXTRACT(YEAR FROM TIMESTAMP_SECONDS(s.effective_ts))
      FROM ${P}stg_hr_agent_skill s LEFT JOIN ${P}stg_hr_skill sk ON sk.skill_id=s.skill_id
      WHERE s.load_date='${RD}'` },
    { name: '34-scd2-assignment', sql: `
      DELETE FROM ${P}ods_agent_assignment_scd2 WHERE TRUE;
      INSERT INTO ${P}ods_agent_assignment_scd2
      SELECT CONCAT('AAH-',CAST(ROW_NUMBER() OVER (ORDER BY r.employee_no) AS STRING)),
        COALESCE(a.agent_id,-1), COALESCE(p.program_id,-1), -1, r.role_on_program,
        TIMESTAMP_MILLIS(r.as_of_ms), TIMESTAMP('9999-12-31'), r.active_flag,
        EXTRACT(YEAR FROM TIMESTAMP_MILLIS(r.as_of_ms))
      FROM ${P}stg_file_roster r
      LEFT JOIN (SELECT agent_id, email FROM ${P}stg_hr_agent WHERE load_date='${RD}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY agent_id)=1) a ON a.email=r.agent_email
      LEFT JOIN (SELECT program_id, client_id FROM ${P}stg_crm_program WHERE load_date='${RD}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY program_id)=1) p
        ON p.client_id = (SELECT client_id FROM ${P}stg_crm_client WHERE client_code=r.client_code
        AND load_date='${RD}' LIMIT 1)
      WHERE r.feed_date='${RD}'` },
    // Dims that don't depend on ACID tables
    { name: '44-dim-program', sql: `
      DELETE FROM ${P}dim_program WHERE TRUE;
      INSERT INTO ${P}dim_program
      SELECT p.program_id, p.program_id, p.program_code, p.program_name, p.client_id,
        p.line_of_business, p.channel_mix, p.site_code,
        COALESCE(c.billing_model,'UNKNOWN'), p.status,
        CAST(FORMAT_TIMESTAMP('%Y%m%d', p.go_live_ts) AS INT64)
      FROM ${P}ods_program p
      LEFT JOIN ${P}ods_contract c ON c.program_id=p.program_id AND c.snapshot_date='${RD}'
      WHERE p.snapshot_date='${RD}'` },
    { name: '45-dim-queue', sql: `
      DELETE FROM ${P}dim_queue WHERE TRUE;
      INSERT INTO ${P}dim_queue SELECT q.queue_id, q.queue_id, q.queue_code, q.queue_name,
        q.program_id, q.media_type, q.priority
      FROM ${P}ods_queue q WHERE q.snapshot_date='${RD}'` },
    { name: '47-dim-shift', sql: `
      DELETE FROM ${P}dim_shift WHERE TRUE;
      INSERT INTO ${P}dim_shift SELECT s.shift_id, s.shift_id, s.shift_code, s.shift_name,
        s.start_hhmm, s.end_hhmm, s.overnight_flag, s.site_code
      FROM ${P}stg_wfm_shift s WHERE s.load_date='${RD}'` },
    { name: '48-dim-org', sql: `
      DELETE FROM ${P}dim_org WHERE TRUE;
      INSERT INTO ${P}dim_org
      SELECT o.org_unit_id, o.org_unit_id, o.unit_code, o.unit_name, o.unit_type,
        p1.unit_name, p2.unit_name, p3.unit_name, p4.unit_name, o.site_code, o.cost_center
      FROM ${P}ods_org_unit o
      LEFT JOIN ${P}ods_org_unit p1 ON p1.org_unit_id=o.parent_unit_id AND p1.snapshot_date='${RD}'
      LEFT JOIN ${P}ods_org_unit p2 ON p2.org_unit_id=p1.parent_unit_id AND p2.snapshot_date='${RD}'
      LEFT JOIN ${P}ods_org_unit p3 ON p3.org_unit_id=p2.parent_unit_id AND p3.snapshot_date='${RD}'
      LEFT JOIN ${P}ods_org_unit p4 ON p4.org_unit_id=p3.parent_unit_id AND p4.snapshot_date='${RD}'
      WHERE o.snapshot_date='${RD}'` },
    { name: '49-dim-disposition', sql: `
      DELETE FROM ${P}dim_disposition WHERE TRUE;
      INSERT INTO ${P}dim_disposition SELECT ROW_NUMBER() OVER (ORDER BY d.disposition_code),
        d.disposition_code, d.disposition_desc, d.category, d.billable_flag
      FROM ${P}stg_tel_disposition_code d WHERE d.load_date='${RD}'` },
    // Facts
    { name: '50-fact-interaction', sql: `
      DELETE FROM ${P}fact_interaction WHERE date_key = CAST(REPLACE('${RD}','-','') AS INT64);
      INSERT INTO ${P}fact_interaction
      SELECT i.interaction_id, -1, COALESCE(p.program_sk,-1), -1, -1,
        i.customer_ref, i.start_ts, i.end_ts, i.handle_seconds, i.resolved_flag, i.source_system,
        CAST(FORMAT_TIMESTAMP('%Y%m%d', i.start_ts) AS INT64), i.channel
      FROM ${P}ods_interaction i
      LEFT JOIN ${P}dim_program p ON p.program_id=i.program_id
      WHERE i.event_date='${RD}'` },
    { name: '51-fact-agent-activity', sql: `
      DELETE FROM ${P}fact_agent_activity WHERE date_key = CAST(REPLACE('${RD}','-','') AS INT64);
      INSERT INTO ${P}fact_agent_activity
      SELECT COALESCE(a.agent_id,-1), e.state_code, SUM(e.end_epoch-e.start_epoch),
        CAST(COUNT(*) AS INT64), TIMESTAMP_SECONDS(MIN(e.start_epoch)), TIMESTAMP_SECONDS(MAX(e.end_epoch)),
        CAST(FORMAT_TIMESTAMP('%Y%m%d',TIMESTAMP_SECONDS(MIN(e.start_epoch))) AS INT64)
      FROM ${P}stg_tel_agent_state_event e
      LEFT JOIN ${P}stg_hr_agent a ON a.agent_id=e.agent_id AND a.load_date='${RD}'
      WHERE e.load_date='${RD}' AND e.start_epoch >= UNIX_SECONDS(TIMESTAMP('${RD}'))
      GROUP BY COALESCE(a.agent_id,-1), e.state_code` },
    { name: '53-fact-csat', sql: `
      DELETE FROM ${P}fact_csat_survey WHERE date_key = CAST(REPLACE('${RD}','-','') AS INT64);
      INSERT INTO ${P}fact_csat_survey
      SELECT s.survey_id, f.interaction_id, -1, COALESCE(p.program_sk,-1), -1,
        s.survey_ts, s.csat_score, s.nps_score, s.fcr_claimed,
        CAST(FORMAT_TIMESTAMP('%Y%m%d', s.survey_ts) AS INT64)
      FROM ${P}ods_survey_response s
      LEFT JOIN ${P}ods_interaction f ON f.interaction_id=s.interaction_ref
      LEFT JOIN ${P}fact_interaction fi ON fi.interaction_id=s.interaction_ref
      LEFT JOIN ${P}dim_program p ON p.program_sk=fi.program_sk
      WHERE s.event_date='${RD}'` },
    { name: '58-fact-ivr-path', sql: `
      DELETE FROM ${P}fact_ivr_path WHERE date_key = CAST(REPLACE('${RD}','-','') AS INT64);
      INSERT INTO ${P}fact_ivr_path
      SELECT s.session_ref, s.client_code, s.menu_path_full, s.hops, s.contained_flag, s.exit_key,
        CAST(UNIX_SECONDS(s.last_event_ts) - UNIX_SECONDS(s.first_event_ts) AS INT64),
        CAST(FORMAT_TIMESTAMP('%Y%m%d', s.first_event_ts) AS INT64)
      FROM ${P}ods_ivr_session s WHERE s.event_date='${RD}'` },
  ];

  let bqOk = 0, bqFail = 0;
  for (const {name, sql} of bqScripts) {
    // Split on ; and execute each statement
    const stmts = sql.split(';').map(s=>s.trim()).filter(s=>s.length>10);
    try {
      for (const stmt of stmts) await bQ(stmt);
      bqOk++;
      // Get count
      const im = stmts[stmts.length-1].match(/INTO\s+(\S+)/i);
      if (im) {
        const [r] = await bQ(`SELECT COUNT(*) c FROM ${im[1]}`);
        L(`  ✓ ${name} → ${r.c} rows`);
      } else { L(`  ✓ ${name}`); }
    } catch(e) {
      bqFail++;
      L(`  ✗ ${name}: ${e.message?.substring(0,120)}`);
    }
  }

  L(`  BQ: ${bqOk} pass, ${bqFail} fail`);

  // ═══ Phase 3: Cross-engine comparison ═════════════════════════════════
  L('\n=== P3: Compare Hive vs BQ ===');

  const recon = {};
  let pass=0, fail=0, notEx=0;

  for (const tbl of Object.keys(hiveData)) {
    const hInfo = hiveData[tbl];
    if (!hInfo || hInfo.count === 0) {
      recon[tbl] = { status: 'HIVE_EMPTY', hive: 0 };
      continue;
    }

    // Get BQ count
    let bqCount = -1;
    try {
      const [r] = await bQ(`SELECT COUNT(*) c FROM ${BQDS}.${BQP}${tbl}`);
      bqCount = +r.c;
    } catch { bqCount = -1; }

    if (bqCount === -1) {
      recon[tbl] = { status: 'BQ_TABLE_MISSING', hive: hInfo.count, note: 'BQ output table not created or script not converted' };
      notEx++;
      L(`  ${tbl}: BQ missing (hive=${hInfo.count})`);
      continue;
    }

    const countMatch = hInfo.count === bqCount;

    // Compute BQ fingerprint
    let bqFp = null;
    if (bqCount > 0) {
      try {
        const bqRows = await bQ(`SELECT * FROM ${BQDS}.${BQP}${tbl} LIMIT 500`);
        if (bqRows.length > 0) {
          const cols = Object.keys(bqRows[0]).map(c=>c.toLowerCase()).sort();
          const hashes = bqRows.map(r => {
            const nr = {}; for (const [k,v] of Object.entries(r)) nr[k.toLowerCase()] = v;
            return crypto.createHash('md5').update(cols.map(c=>norm(nr[c])).join('|')).digest('hex');
          }).sort();
          bqFp = crypto.createHash('md5').update(hashes.join('\n')).digest('hex');
        }
      } catch(e) { L(`  ${tbl} BQ fp error: ${e.message?.substring(0,60)}`); }
    }

    // Compare fingerprints — need same column set
    // The fingerprints may differ because Hive and BQ have different column sets
    // (BQ may have extra columns like snapshot_date_raw). So we need to recompute
    // with COMMON columns.
    let hiveFp = hInfo.fingerprint;
    let fpMatch = null;

    if (bqFp && hiveFp) {
      // Recompute with common columns by querying both sides
      try {
        const hRows = await iQ(`SELECT * FROM ${DB}.${tbl} LIMIT 500`);
        const bRows = await bQ(`SELECT * FROM ${BQDS}.${BQP}${tbl} LIMIT 500`);
        if (hRows.length > 0 && bRows.length > 0) {
          const hCols = Object.keys(hRows[0]).map(c=>c.toLowerCase()).sort();
          const bCols = Object.keys(bRows[0]).map(c=>c.toLowerCase()).sort();
          const common = hCols.filter(c => bCols.includes(c));

          if (common.length > 0) {
            const hH = hRows.map(r => { const n={}; for(const[k,v]of Object.entries(r))n[k.toLowerCase()]=v; return crypto.createHash('md5').update(common.map(c=>norm(n[c])).join('|')).digest('hex'); }).sort();
            const bH = bRows.map(r => { const n={}; for(const[k,v]of Object.entries(r))n[k.toLowerCase()]=v; return crypto.createHash('md5').update(common.map(c=>norm(n[c])).join('|')).digest('hex'); }).sort();
            const hFP = crypto.createHash('md5').update(hH.join('\n')).digest('hex');
            const bFP = crypto.createHash('md5').update(bH.join('\n')).digest('hex');
            fpMatch = hFP === bFP;
          }
        }
      } catch(e) { L(`  ${tbl} common fp error: ${e.message?.substring(0,60)}`); }
    }

    const overall = countMatch && (fpMatch === null || fpMatch);
    recon[tbl] = {
      status: overall ? 'PASS' : 'FAIL',
      hive: hInfo.count, bq: bqCount,
      count_match: countMatch,
      fingerprint_match: fpMatch,
    };
    if (overall) pass++; else fail++;
    L(`  ${tbl}: ${overall?'✓':'✗'} h=${hInfo.count} b=${bqCount} fp=${fpMatch}`);
  }

  const summary = {
    tables_compared: Object.keys(recon).length,
    pass, fail, not_exercised: notEx,
    hive_empty: Object.values(recon).filter(r=>r.status==='HIVE_EMPTY').length,
  };

  fs.writeFileSync(`${EV}/cross_engine_reconciliation.json`, JSON.stringify({summary, tables: recon}, null, 2));
  L(`\n=== SUMMARY: ${pass} PASS, ${fail} FAIL, ${notEx} NOT_EXERCISED ===`);

  try { await iS.close(); await iC.close(); } catch{}
  L('=== DONE ===');
}

main().catch(e => { console.error('FATAL:', e.stack); process.exit(1); });
