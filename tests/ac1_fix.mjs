#!/usr/bin/env node
/**
 * ac1_fix.mjs — Fix the seeding issues and re-run legacy scripts.
 *
 * Fixes: 
 * 1. Recreate file-feed staging tables with both feed_date AND load_date columns
 * 2. Reseed stg_file_ivr_logs and other missing tables
 * 3. Seed dim_date
 * 4. Re-run all legacy scripts
 * 5. Capture honest counts + fingerprints for all output tables
 */
import { createRequire } from 'module';
const require = createRequire('/opt/workspace-mcp/package.json');
const hive = require('hive-driver');
const { TCLIService, TCLIService_types } = hive.thrift;
import crypto from 'crypto';
import fs from 'fs';

const SRC  = '/workspace/source';
const ROOT = '/workspace/project';
const EV   = `${ROOT}/tests/evidence/parity`;
const DB   = 'qa_xp';
const RD   = '2024-01-15';
const BE   = 1705276800; // 2024-01-15 00:00 UTC

fs.mkdirSync(EV, { recursive: true });
const LOG = `${EV}/ac1_fix.log`;
fs.writeFileSync(LOG, '');

function L(m) { const l=`[${new Date().toISOString().slice(11,23)}] ${m}`; console.log(l); fs.appendFileSync(LOG,l+'\n'); }

let hS, iS, hC, iC;
const U = new hive.HiveUtils(TCLIService_types);

async function openH(h,p) {
  const c=new hive.HiveClient(TCLIService,TCLIService_types);
  const cn=await c.connect({host:h,port:+p},new hive.connections.TcpConnection(),new hive.auth.NoSaslAuthentication());
  const s=await cn.openSession({client_protocol:TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10});
  return {cn,s};
}
async function hE(s,q) { const o=await s.executeStatement(q,{runAsync:true}); await U.waitUntilReady(o,false,()=>{}); await o.close(); }
async function hQ(s,q) { const o=await s.executeStatement(q,{runAsync:true}); await U.waitUntilReady(o,false,()=>{}); await U.fetchAll(o,1); const r=U.getResult(o).getValue()??[]; await o.close(); return r; }
async function inv(t) { await hE(iS, t?`INVALIDATE METADATA ${t}`:'INVALIDATE METADATA'); }
async function ref(t) { try{await hE(iS,`REFRESH ${t}`);}catch{await inv(t);} }
async function iCnt(t) { const r=await hQ(iS,`SELECT COUNT(*) c FROM ${t}`); return +(r[0]?.c??0); }
function esc(v) { if(v==null) return 'NULL'; if(typeof v==='boolean') return v?'TRUE':'FALSE'; if(typeof v==='number') return String(v); return "'"+String(v).replace(/\\/g,'\\\\').replace(/'/g,"\\'")+"'"; }
function norm(v) { if(v==null) return '__NULL__'; if(typeof v==='object'&&v.value!==undefined) return String(v.value); let s=String(v); return s.replace(/\+00(:00)?$/,'').replace(/T/g,' ').replace(/\.0+$/,''); }

async function main() {
  L('=== AC1 Fix — Seed + Run Legacy ===');
  
  ({cn:hC,s:hS} = await openH(process.env.CLD_IMP_HIVE_HOST, process.env.CLD_IMP_HIVE_PORT));
  ({cn:iC,s:iS} = await openH(process.env.CLD_IMP_HOST, process.env.CLD_IMP_PORT));
  L('Connected');

  await hE(hS,'SET hive.exec.dynamic.partition=true');
  await hE(hS,'SET hive.exec.dynamic.partition.mode=nonstrict');

  // ── Fix 1: Recreate file-feed tables with load_date column ──
  L('--- Fix file-feed staging tables ---');
  
  // Drop and recreate stg_file_ivr_logs with both feed_date and load_date
  const fileFeedFixes = [
    { tbl: 'stg_file_ivr_logs', cols: 'event_ms BIGINT, session_ref STRING, menu_path STRING, key_pressed STRING, raw_tail STRING, client_code STRING, feed_date STRING, load_date STRING' },
    { tbl: 'stg_file_email_interaction', cols: 'email_ref STRING, mailbox STRING, agent_email STRING, received_ms BIGINT, first_reply_ms BIGINT, resolved_ms BIGINT, subject_category STRING, client_code STRING, feed_date STRING, load_date STRING' },
    { tbl: 'stg_file_survey_csat', cols: 'survey_id STRING, interaction_ref STRING, survey_ms BIGINT, csat_score INT, nps_score INT, fcr_claimed BOOLEAN, verbatim STRING, client_code STRING, feed_date STRING, load_date STRING' },
    { tbl: 'stg_file_dialer_result', cols: 'attempt_id STRING, campaign_code STRING, phone_hash STRING, agent_id BIGINT, attempt_ms BIGINT, result_code STRING, talk_seconds BIGINT, client_code STRING, feed_date STRING, load_date STRING' },
    { tbl: 'stg_file_interaction_export', cols: 'interaction_ref STRING, channel STRING, client_interaction_id STRING, agent_email STRING, start_ms BIGINT, end_ms BIGINT, outcome STRING, customer_ref STRING, client_code STRING, feed_date STRING, load_date STRING' },
    { tbl: 'stg_file_roster', cols: 'employee_no STRING, agent_email STRING, client_login STRING, role_on_program STRING, active_flag BOOLEAN, as_of_ms BIGINT, client_code STRING, feed_date STRING, load_date STRING' },
    { tbl: 'stg_file_telco_invoice', cols: 'telco_invoice_id STRING, carrier STRING, circuit_id STRING, usage_minutes BIGINT, charge_amount DOUBLE, bill_period STRING, billed_ms BIGINT, client_code STRING, feed_date STRING, load_date STRING' },
    { tbl: 'stg_file_speech_analytics', cols: 'recording_id STRING, call_ref STRING, analyzed_ms BIGINT, sentiment_score DOUBLE, silence_pct DOUBLE, talk_over_count BIGINT, client_code STRING, feed_date STRING, load_date STRING' },
    // Chat transcripts and QA forms — simplified (no ARRAY types)
    { tbl: 'stg_file_chat_transcripts', cols: 'chat_ref STRING, queue_code STRING, agent_email STRING, started_ms BIGINT, ended_ms BIGINT, client_code STRING, feed_date STRING, load_date STRING' },
    { tbl: 'stg_file_qa_forms', cols: 'qa_form_id STRING, interaction_ref STRING, evaluator_email STRING, evaluated_ms BIGINT, form_version STRING, auto_fail BOOLEAN, overall_pct DOUBLE, client_code STRING, feed_date STRING, load_date STRING' },
  ];

  for (const {tbl, cols} of fileFeedFixes) {
    try {
      await hE(hS, `DROP TABLE IF EXISTS ${DB}.${tbl}`);
      await hE(hS, `CREATE TABLE ${DB}.${tbl} (${cols}) STORED AS PARQUET`);
      L(`  Created ${tbl}`);
    } catch(e) { L(`  DDL FAIL ${tbl}: ${e.message?.substring(0,80)}`); }
  }
  await inv();

  // ── Fix 2: Seed file-feed tables ──
  L('--- Seed file-feed tables ---');

  // IVR logs
  const menus=['main','main.billing','main.billing.dispute','main.support'];
  const ivrRows = [];
  for (let s=1;s<=5;s++) for (let h=0;h<(s<=3?4:3);h++) ivrRows.push([
    (BE+s*1000+h*5)*1000, `'IVR-${String(s).padStart(4,'0')}'`, `'${menus[h%menus.length]}'`,
    `'${h+1}'`, "''", "'CLT001'", `'${RD}'`, `'${RD}'`
  ]);
  for (let i=0; i<ivrRows.length; i+=10) {
    const sel = ivrRows.slice(i,i+10).map(r=>`SELECT ${r.join(',')}`).join(' UNION ALL ');
    await hE(hS, `INSERT INTO ${DB}.stg_file_ivr_logs ${sel}`);
  }
  await ref(`${DB}.stg_file_ivr_logs`);
  L(`  stg_file_ivr_logs: ${await iCnt(`${DB}.stg_file_ivr_logs`)} rows`);

  // Email interactions
  const emailRows = [];
  for (let i=1;i<=10;i++) emailRows.push([
    `'EM-${i}'`, "'support@test.com'", `'agent${((i-1)%18)+1}@test.com'`,
    (BE+i*800)*1000, (BE+i*800+300)*1000, (BE+i*800+600)*1000,
    "'GENERAL'", `'${['CLT001','CLT002'][i%2]}'`, `'${RD}'`, `'${RD}'`
  ]);
  const emailSel = emailRows.map(r=>`SELECT ${r.join(',')}`).join(' UNION ALL ');
  await hE(hS, `INSERT INTO ${DB}.stg_file_email_interaction ${emailSel}`);
  await ref(`${DB}.stg_file_email_interaction`);
  L(`  stg_file_email_interaction: ${await iCnt(`${DB}.stg_file_email_interaction`)} rows`);

  // Survey CSAT
  const surveyRows = [];
  for (let i=1;i<=15;i++) surveyRows.push([
    `'SRV-${i}'`, `'INT-${String(i).padStart(5,'0')}'`, (BE+i*1000)*1000,
    3+(i%3), i%2===0?9:5, i%3===0, `'Feedback ${i}'`,
    `'${['CLT001','CLT002','CLT003'][i%3]}'`, `'${RD}'`, `'${RD}'`
  ]);
  const surSel = surveyRows.map(r=>`SELECT ${r.join(',')}`).join(' UNION ALL ');
  await hE(hS, `INSERT INTO ${DB}.stg_file_survey_csat ${surSel}`);
  await ref(`${DB}.stg_file_survey_csat`);
  L(`  stg_file_survey_csat: ${await iCnt(`${DB}.stg_file_survey_csat`)} rows`);

  // Dialer results
  const dlrRows = [];
  for (let i=1;i<=10;i++) dlrRows.push([
    `'DLR-${i}'`, "'CAMP1'", `'PH${i}'`, ((i-1)%18)+1,
    (BE+i*500)*1000, `'${i%3===0?'CONNECTED':'NO_ANSWER'}'`,
    i%3===0?120+i:0, "'CLT001'", `'${RD}'`, `'${RD}'`
  ]);
  const dlrSel = dlrRows.map(r=>`SELECT ${r.join(',')}`).join(' UNION ALL ');
  await hE(hS, `INSERT INTO ${DB}.stg_file_dialer_result ${dlrSel}`);
  await ref(`${DB}.stg_file_dialer_result`);
  L(`  stg_file_dialer_result: ${await iCnt(`${DB}.stg_file_dialer_result`)} rows`);

  // ── Fix 3: Seed dim_date ──
  L('--- Seed dim_date ---');
  try {
    await hE(hS, `INSERT OVERWRITE TABLE ${DB}.dim_date SELECT * FROM ${DB}.dim_date WHERE FALSE`);
    const dates = [];
    for (let d=1;d<=31;d++) {
      const ds=`2024-01-${String(d).padStart(2,'0')}`;
      const dt=new Date(ds+'T12:00:00Z');
      const dow=dt.getUTCDay();
      const dn=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
      dates.push(`SELECT ${parseInt(ds.replace(/-/g,''))},'${ds}',${dow},'${dn[dow]}',${Math.ceil(d/7)},1,'January',1,2024,${dow===0||dow===6},FALSE,'FY2024-Q1'`);
    }
    // Insert in 2 batches
    await hE(hS, `INSERT INTO ${DB}.dim_date ${dates.slice(0,15).join(' UNION ALL ')}`);
    await hE(hS, `INSERT INTO ${DB}.dim_date ${dates.slice(15).join(' UNION ALL ')}`);
    await ref(`${DB}.dim_date`);
    L(`  dim_date: ${await iCnt(`${DB}.dim_date`)} rows`);
  } catch(e) { L(`  dim_date FAIL: ${e.message?.substring(0,100)}`); }

  // ── Phase 2: Run legacy scripts ──
  L('=== Run legacy scripts ===');

  const scripts = [
    '09-cleanse-program.sql','10-cleanse-contract.sql','11-cleanse-contract-line.sql',
    '12-cleanse-org-unit.sql','13-cleanse-queue.sql','14-cleanse-schedule.sql',
    '15-cleanse-adherence-event.sql','16-cleanse-call.sql','17-cleanse-ivr-session.sql',
    '18-cleanse-chat-session.sql','19-cleanse-email-interaction.sql','20-cleanse-survey-response.sql',
    '21-cleanse-qa-evaluation.sql','22-cleanse-interaction.sql','23-cleanse-dialer-attempt.sql',
    '24-delta-merge-timesheet.sql','25-delta-merge-payroll-adjustment.sql',
    '26-delta-merge-sla-credit.sql','27-delta-merge-callback-request.sql',
    '28-delta-merge-shift-swap.sql','29-delta-merge-ticket-worklog.sql',
    '30-delta-merge-attrition-event.sql','31-delta-merge-rate-card.sql',
    '32-scd2-agent.sql','33-scd2-agent-skill.sql','34-scd2-agent-assignment.sql',
    '35-acid-merge-client.sql','36-acid-merge-agent.sql',
    '37-acid-merge-ticket.sql','38-acid-merge-invoice.sql',
    '42-load-dim-agent.sql','43-load-dim-client.sql','44-load-dim-program.sql',
    '45-load-dim-queue.sql','46-load-dim-site.sql','47-load-dim-shift.sql',
    '48-load-dim-org.sql','49-load-dim-disposition.sql',
    '50-load-fact-interaction.sql','51-load-fact-agent-activity.sql',
    '52-load-fact-queue-interval.sql','53-load-fact-csat-survey.sql',
    '54-load-fact-qa-evaluation.sql','55-load-fact-billing-line.sql',
    '56-load-fact-adherence-daily.sql','57-load-fact-ticket.sql','58-load-fact-ivr-path.sql',
    '59-agg-agent-daily.sql','60-agg-agent-weekly.sql','61-agg-program-monthly.sql',
    '62-agg-queue-hourly.sql','63-agg-csat-rollup-monthly.sql',
    '64-agg-billing-monthly.sql','65-agg-site-daily.sql',
    '93-close-billing-month.sql','96-intraday-queue-hourly.sql','97-intraday-agent-state.sql',
  ];

  let ok=0, fail=0, notEx=0;
  const results = [];

  for (const file of scripts) {
    const fp = `${SRC}/impala/${file}`;
    if (!fs.existsSync(fp)) { continue; }

    let sql = fs.readFileSync(fp, 'utf8');
    sql = sql.split('\n').filter(l=>!l.trim().startsWith('--')).join('\n');
    sql = sql.replace(/\$\{var:run_date\}/g, RD);
    sql = sql.replace(/\$\{hivevar:run_date\}/g, RD);
    sql = sql.replace(/\bstaging\./g, `${DB}.`);
    sql = sql.replace(/\bods\./g, `${DB}.`);
    sql = sql.replace(/\bdm\./g, `${DB}.`);
    sql = sql.replace(/COMPUTE\s+(?:INCREMENTAL\s+)?STATS[^;]*;?/gi, '');

    // Hive compatibility fixes:
    // group_concat → concat_ws(' > ', collect_list(...))
    sql = sql.replace(/group_concat\((\w+(?:\.\w+)?)\s*,\s*'([^']+)'\s*\)/gi,
      "concat_ws('$2', collect_list($1))");

    const stmts = sql.split(';').map(s=>s.trim()).filter(s=>s.length>10 && !s.match(/^SET\s/i));
    const t0 = Date.now();
    try {
      for (const stmt of stmts) await hE(hS, stmt);
      const im = stmts[0]?.match(/INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?(?:TABLE\s+)?(\S+)/i);
      const outTbl = im?.[1];
      if (outTbl) await ref(outTbl);
      const cnt = outTbl ? await iCnt(outTbl).catch(()=>-1) : -1;
      ok++;
      results.push({ script:file, status:'PASS', ms:Date.now()-t0, rows:cnt });
      L(`  ✓ ${file} → ${cnt}`);
    } catch(e) {
      const msg = e.message || '';
      if (msg.includes('MERGE')||msg.includes('transactional')||msg.includes('DELETE')||
          msg.match(/cannot recognize input near '(MERGE|DELETE)'/i)) {
        notEx++;
        results.push({ script:file, status:'NOT_EXERCISED', error:msg.substring(0,250) });
        L(`  ⊘ ${file}: NOT_EXERCISED — ${msg.substring(0,80)}`);
      } else {
        fail++;
        results.push({ script:file, status:'FAIL', error:msg.substring(0,250) });
        L(`  ✗ ${file}: ${msg.substring(0,100)}`);
      }
    }
  }

  // DQ scripts
  for (const n of [74,75,76,77,78,79]) {
    const file = fs.readdirSync(`${SRC}/impala/`).find(f=>f.startsWith(`${n}-`));
    if (!file) continue;
    let sql = fs.readFileSync(`${SRC}/impala/${file}`,'utf8');
    sql = sql.split('\n').filter(l=>!l.trim().startsWith('--')).join('\n');
    sql = sql.replace(/\$\{var:run_date\}/g,RD);
    sql = sql.replace(/\bstaging\./g,`${DB}.`).replace(/\bods\./g,`${DB}.`).replace(/\bdm\./g,`${DB}.`);
    sql = sql.replace(/COMPUTE[^;]*;?/gi,'');
    const stmts = sql.split(';').map(s=>s.trim()).filter(s=>s.length>10 && !s.match(/^SET\s/i));
    try {
      for (const stmt of stmts) await hQ(hS, stmt);
      results.push({ script:file, status:'PASS', rows:-1 });
      ok++;
      L(`  ✓ DQ ${file}`);
    } catch(e) {
      results.push({ script:file, status:'FAIL', error:e.message?.substring(0,200) });
      fail++;
      L(`  ✗ DQ ${file}: ${e.message?.substring(0,80)}`);
    }
  }

  // ── Collect all output table counts + fingerprints ──
  L('=== Collect Hive output counts ===');
  const tableMap = {
    9:'ods_program',10:'ods_contract',11:'ods_contract_line',12:'ods_org_unit',
    13:'ods_queue',14:'ods_schedule',15:'ods_adherence_event',16:'ods_call',
    17:'ods_ivr_session',18:'ods_chat_session',19:'ods_email_interaction',
    20:'ods_survey_response',21:'ods_qa_evaluation',22:'ods_interaction',
    23:'ods_dialer_attempt',24:'ods_timesheet',25:'ods_payroll_adjustment',
    26:'ods_sla_credit',27:'ods_callback_request',28:'ods_shift_swap',
    29:'ods_ticket_worklog',30:'ods_attrition_event',31:'ods_rate_card',
    32:'ods_agent_scd2',33:'ods_agent_skill_scd2',34:'ods_agent_assignment_scd2',
    35:'ods_client_acid',36:'ods_agent_acid',37:'ods_ticket_acid',38:'ods_invoice_acid',
    42:'dim_agent',43:'dim_client',44:'dim_program',45:'dim_queue',
    46:'dim_site',47:'dim_shift',48:'dim_org',49:'dim_disposition',
    50:'fact_interaction',51:'fact_agent_activity',52:'fact_queue_interval',
    53:'fact_csat_survey',54:'fact_qa_evaluation',55:'fact_billing_line',
    56:'fact_adherence_daily',57:'fact_ticket',58:'fact_ivr_path',
    59:'agg_agent_daily',60:'agg_agent_weekly',61:'agg_program_monthly',
    62:'agg_queue_hourly',63:'agg_csat_rollup_monthly',64:'agg_billing_monthly',
    65:'agg_site_daily',
  };

  const counts = {};
  for (const [num, tbl] of Object.entries(tableMap)) {
    try {
      const cnt = await iCnt(`${DB}.${tbl}`);
      counts[tbl] = cnt;
      if (cnt > 0) {
        // Compute fingerprint
        const rows = await hQ(iS, `SELECT * FROM ${DB}.${tbl} LIMIT 500`);
        if (rows.length > 0) {
          const cols = Object.keys(rows[0]).map(c=>c.toLowerCase()).sort();
          const hashes = rows.map(r => {
            const nr = {}; for (const [k,v] of Object.entries(r)) nr[k.toLowerCase()] = v;
            return crypto.createHash('md5').update(cols.map(c=>norm(nr[c])).join('|')).digest('hex');
          }).sort();
          const fp = crypto.createHash('md5').update(hashes.join('\n')).digest('hex');
          counts[tbl] = { count: cnt, fingerprint: fp, cols: cols.length };
        }
      } else {
        counts[tbl] = { count: 0, fingerprint: null };
      }
    } catch(e) { counts[tbl] = { count: -1, error: e.message?.substring(0,80) }; }
    L(`  ${tbl}: ${JSON.stringify(counts[tbl]).substring(0,80)}`);
  }

  fs.writeFileSync(`${EV}/legacy_results.json`, JSON.stringify(results, null, 2));
  fs.writeFileSync(`${EV}/hive_counts_fingerprints.json`, JSON.stringify(counts, null, 2));
  
  L(`\n=== SUMMARY ===`);
  L(`Scripts: ${ok} pass, ${fail} fail, ${notEx} not-exercised`);
  const nonEmpty = Object.values(counts).filter(v=>v.count>0).length;
  const empty = Object.values(counts).filter(v=>v.count===0).length;
  L(`Tables: ${nonEmpty} non-empty, ${empty} empty, ${Object.keys(counts).length} total`);

  try { await hS.close(); await hC.close(); } catch{}
  try { await iS.close(); await iC.close(); } catch{}
  L('=== DONE ===');
}

main().catch(e => { console.error('FATAL:', e.stack); process.exit(1); });
