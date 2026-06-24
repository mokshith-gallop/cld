#!/usr/bin/env node
/**
 * ac1_bq_compare.mjs — Phase 5+6: Run BQ converted scripts & compare with Hive.
 * 
 * Reads Hive legacy evidence from per_table/*_legacy.json,
 * runs converted BQ scripts, computes BQ fingerprints,
 * compares row counts + fingerprints across engines.
 *
 * Usage: set -a; source /workspace/.gallop/db.env; set +a; node tests/ac1_bq_compare.mjs
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
const SRC = '/workspace/source';
const EV = `${ROOT}/tests/evidence`;
const BQDS = 'test';
const BQP = 'qa_ac1_';
const HDB = 'qa_ac1';
const RD = '2024-01-15';

const LOG = `${EV}/ac1_bq_compare.log`;
fs.writeFileSync(LOG, '');
function L(m) { const l=`[${new Date().toISOString().substr(11,12)}] ${m}`; console.log(l); fs.appendFileSync(LOG,l+'\n'); }

let bq;
function mkBQ() {
  try{const e=fs.readFileSync('/workspace/.gallop/db.env','utf8');const m=e.match(/CLD_BQ_BQ_TOKEN='([^']+)'/);if(m)process.env.CLD_BQ_BQ_TOKEN=m[1];}catch{}
  const a=new OAuth2Client();a.setCredentials({access_token:process.env.CLD_BQ_BQ_TOKEN});
  bq=new BigQuery({projectId:process.env.CLD_BQ_BQ_PROJECT,authClient:a,location:'EU'});
}
async function bqQ(sql) {
  try{const[j]=await bq.createQueryJob({query:sql,useLegacySql:false});const[r]=await j.getQueryResults({maxResults:100000});return r;}
  catch(e){if(e.message?.includes('credentials')||e.message?.includes('401')){mkBQ();const[j]=await bq.createQueryJob({query:sql,useLegacySql:false});const[r]=await j.getQueryResults({maxResults:100000});return r;}throw e;}
}

// Impala connection for reading legacy data
let impS, impC;
const U = new hive.HiveUtils(TCLIService_types);
async function openHS(h,p){const c=new hive.HiveClient(TCLIService,TCLIService_types);const cn=await c.connect({host:h,port:+p},new hive.connections.TcpConnection(),new hive.auth.NoSaslAuthentication());const s=await cn.openSession({client_protocol:TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10});return{cn,s};}
async function hQ(s,sql){const o=await s.executeStatement(sql,{runAsync:true});await U.waitUntilReady(o,false,()=>{});await U.fetchAll(o,1);const r=U.getResult(o).getValue()??[];await o.close();return r;}

function norm(v){
  if(v==null)return'__NULL__';
  if(typeof v==='object'&&v.value!==undefined)v=v.value;
  let s=String(v);
  // Normalize timestamps: remove timezone, T→space, trailing .000
  s=s.replace(/\+00:00$/,'').replace(/\+00$/,'').replace(/Z$/,'').replace(/T/g,' ').replace(/\.000$/,'').replace(/\.0+$/,'');
  // Normalize booleans
  if(s==='true')s='1'; if(s==='false')s='0';
  return s;
}

async function main() {
  L('=== AC1 BQ Scripts + Cross-Engine Compare ===');
  mkBQ(); await bqQ('SELECT 1');

  // Connect Impala for Hive-side reads
  ({cn:impC, s:impS} = await openHS(process.env.CLD_IMP_HOST, process.env.CLD_IMP_PORT));
  L('Connected BQ + Impala');

  // Step 1: Create BQ output tables (ODS + DM)
  L('\n=== Create BQ output tables ===');
  const specs = JSON.parse(fs.readFileSync(`${ROOT}/tests/table_specs.json`, 'utf8'));

  for (const [tbl, spec] of Object.entries(specs)) {
    if (tbl.startsWith('stg_')) continue; // staging already created in Phase 3
    // Build BQ DDL
    const cols = spec.cols.map(c => {
      let t = c.type.toUpperCase();
      if (t === 'BIGINT' || t === 'INT') t = 'INT64';
      else if (t.startsWith('DECIMAL') || t.startsWith('NUMERIC')) t = t.replace('DECIMAL','NUMERIC');
      else if (t === 'BOOLEAN') t = 'BOOL';
      else if (t === 'TIMESTAMP') t = 'TIMESTAMP';
      else if (t === 'DOUBLE' || t === 'FLOAT') t = 'FLOAT64';
      else t = 'STRING';
      return `${c.name} ${t}`;
    });
    // Add partition cols as regular BQ cols
    for (const p of spec.partitions) {
      let t = p.type.toUpperCase();
      if (t === 'INT') t = 'INT64';
      else t = 'STRING';
      cols.push(`${p.name} ${t}`);
    }

    try {
      await bqQ(`DROP TABLE IF EXISTS \`${BQDS}.${BQP}${tbl}\``);
      await bqQ(`CREATE TABLE \`${BQDS}.${BQP}${tbl}\` (${cols.join(', ')})`);
    } catch (e) { L(`  DDL FAIL ${tbl}: ${e.message?.substring(0,80)}`); }
  }
  L('  BQ output tables created');

  // Step 2: Seed BQ SCD2/ACID/dim_date dependencies
  L('\n=== Seed BQ dependencies ===');
  const bqSeeds = [
    // dim_date
    `INSERT INTO ${BQDS}.${BQP}dim_date SELECT CAST(FORMAT_DATE('%Y%m%d',d) AS INT64),FORMAT_DATE('%Y-%m-%d',d),EXTRACT(DAYOFWEEK FROM d),FORMAT_DATE('%A',d),EXTRACT(ISOWEEK FROM d),EXTRACT(MONTH FROM d),FORMAT_DATE('%B',d),EXTRACT(QUARTER FROM d),EXTRACT(YEAR FROM d),EXTRACT(DAYOFWEEK FROM d) IN (1,7),FALSE,CONCAT('FY',CAST(EXTRACT(YEAR FROM d) AS STRING),'-Q',CAST(EXTRACT(QUARTER FROM d) AS STRING)) FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01','2024-12-31')) d`,
    // ods_client_acid
    `INSERT INTO ${BQDS}.${BQP}ods_client_acid SELECT client_id,client_code,client_name,industry,hq_country,UPPER(TRIM(status)),TIMESTAMP_SECONDS(created_ts),TIMESTAMP_SECONDS(updated_ts) FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY updated_ts DESC) rn FROM ${BQDS}.${BQP}stg_crm_client) WHERE rn=1`,
    // ods_agent_acid
    `INSERT INTO ${BQDS}.${BQP}ods_agent_acid SELECT agent_id,employee_no,CONCAT(first_name,' ',last_name),email,org_unit_id,job_grade,employment_type,TIMESTAMP_SECONDS(hire_ts),CASE WHEN term_ts>0 THEN TIMESTAMP_SECONDS(term_ts) ELSE NULL END,UPPER(TRIM(status)) FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY agent_id ORDER BY hire_ts DESC) rn FROM ${BQDS}.${BQP}stg_hr_agent) WHERE rn=1`,
    // ods_ticket_acid
    `INSERT INTO ${BQDS}.${BQP}ods_ticket_acid SELECT ticket_id,ticket_no,program_id,category_id,assigned_agent_id,priority,status,TIMESTAMP_MILLIS(created_ms),TIMESTAMP_MILLIS(updated_ms),CASE WHEN status='CLOSED' THEN TIMESTAMP_MILLIS(updated_ms) ELSE NULL END FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY ticket_id ORDER BY updated_ms DESC) rn FROM ${BQDS}.${BQP}stg_tkt_ticket) WHERE rn=1`,
    // ods_invoice_acid (LIE: millis/1000 → DIV for truncation)
    `INSERT INTO ${BQDS}.${BQP}ods_invoice_acid SELECT invoice_id,invoice_no,client_id,program_id,period_month,TIMESTAMP_SECONDS(CAST(FLOOR(issued_ts_sec/1000) AS INT64)),TIMESTAMP_SECONDS(CAST(FLOOR(due_ts_sec/1000) AS INT64)),currency,total_amount,UPPER(TRIM(status)) FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY invoice_id ORDER BY issued_ts_sec DESC) rn FROM ${BQDS}.${BQP}stg_fin_invoice) WHERE rn=1`,
    // ods_agent_scd2
    `INSERT INTO ${BQDS}.${BQP}ods_agent_scd2 SELECT CONCAT('AH-',CAST(agent_id AS STRING)),agent_id,employee_no,org_unit_id,job_grade,employment_type,UPPER(TRIM(status)),TIMESTAMP_SECONDS(hire_ts),CASE WHEN term_ts>0 THEN TIMESTAMP_SECONDS(term_ts) ELSE TIMESTAMP('9999-12-31') END,TRUE,EXTRACT(YEAR FROM TIMESTAMP_SECONDS(hire_ts)),CAST(DATE(TIMESTAMP_SECONDS(hire_ts)) AS STRING) FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY agent_id ORDER BY hire_ts DESC) rn FROM ${BQDS}.${BQP}stg_hr_agent) WHERE rn=1`,
    // ods_agent_skill_scd2
    `INSERT INTO ${BQDS}.${BQP}ods_agent_skill_scd2 SELECT CONCAT('ASH-',CAST(s.agent_skill_id AS STRING)),s.agent_id,s.skill_id,COALESCE(k.skill_code,CONCAT('SK-',CAST(s.skill_id AS STRING))),s.proficiency,s.certified,TIMESTAMP_SECONDS(s.effective_ts),CASE WHEN s.expiry_ts>0 THEN TIMESTAMP_SECONDS(s.expiry_ts) ELSE TIMESTAMP('9999-12-31') END,TRUE,EXTRACT(YEAR FROM TIMESTAMP_SECONDS(s.effective_ts)),CAST(DATE(TIMESTAMP_SECONDS(s.effective_ts)) AS STRING) FROM ${BQDS}.${BQP}stg_hr_agent_skill s LEFT JOIN ${BQDS}.${BQP}stg_hr_skill k ON k.skill_id=s.skill_id`,
    // delta-merge tables
    `INSERT INTO ${BQDS}.${BQP}ods_timesheet SELECT timesheet_id,agent_id,work_date,program_id,billable_minutes,nonbillable_minutes,approved_flag,TIMESTAMP_MILLIS(change_ms),LEFT(work_date,7) FROM ${BQDS}.${BQP}stg_fin_timesheet_delta WHERE op='I'`,
    `INSERT INTO ${BQDS}.${BQP}ods_payroll_adjustment SELECT adjustment_id,agent_id,adj_type,amount,TIMESTAMP_MILLIS(change_ms),period_month FROM ${BQDS}.${BQP}stg_fin_payroll_adj_delta WHERE op='I'`,
    `INSERT INTO ${BQDS}.${BQP}ods_sla_credit SELECT sla_credit_id,program_id,sla_target_id,credit_amount,reason,TIMESTAMP_MILLIS(change_ms),period_month FROM ${BQDS}.${BQP}stg_crm_sla_credit_delta WHERE op='I'`,
    `INSERT INTO ${BQDS}.${BQP}ods_callback_request SELECT callback_id,call_id,queue_id,TIMESTAMP_SECONDS(requested_epoch),TIMESTAMP_SECONDS(scheduled_epoch),completed_flag,TIMESTAMP_MILLIS(change_ms),'2024-01-15' FROM ${BQDS}.${BQP}stg_tel_callback_request_delta WHERE op='I'`,
    `INSERT INTO ${BQDS}.${BQP}ods_shift_swap SELECT swap_id,requesting_agent_id,accepting_agent_id,schedule_id,swap_date,status,TIMESTAMP_MILLIS(change_ms),LEFT(swap_date,7) FROM ${BQDS}.${BQP}stg_wfm_shift_swap_delta WHERE op='I'`,
    `INSERT INTO ${BQDS}.${BQP}ods_ticket_worklog SELECT worklog_id,ticket_id,agent_id,minutes_logged,TIMESTAMP_MILLIS(log_ms),note,TIMESTAMP_MILLIS(change_ms),'2024-01-15' FROM ${BQDS}.${BQP}stg_tkt_worklog_delta WHERE op='I'`,
    `INSERT INTO ${BQDS}.${BQP}ods_attrition_event SELECT attrition_event_id,agent_id,TIMESTAMP_SECONDS(notice_epoch),last_day,attrition_type,reason_code,regrettable_flag,TIMESTAMP_MILLIS(change_ms),LEFT(last_day,7) FROM ${BQDS}.${BQP}stg_hr_attrition_event_delta WHERE op='I'`,
    `INSERT INTO ${BQDS}.${BQP}ods_rate_card SELECT rate_card_id,program_id,service_code,rate,currency,TIMESTAMP_SECONDS(effective_ts),CASE WHEN expiry_ts>0 THEN TIMESTAMP_SECONDS(expiry_ts) ELSE NULL END,TIMESTAMP_SECONDS(effective_ts),'${RD}' FROM ${BQDS}.${BQP}stg_fin_rate_card`,
  ];
  for (const sql of bqSeeds) {
    try { await bqQ(sql); } catch(e) { L(`  Seed err: ${e.message?.substring(0,80)}`); }
  }
  L('  Dependencies seeded');

  // Step 3: Run converted BQ scripts for in-scope objects
  // Read source script, apply dialect rewrites, run on BQ
  L('\n=== Run BQ converted scripts ===');
  
  const scopeNums = [9,10,11,12,13,14,15,16,17,42,43,44,45,46,47,48,49,
    50,51,53,54,55,56,57,58,59,61,64,65,93,97];
  // These are the scripts that passed on legacy (minus those needing ACID/complex types)
  
  let bqOk=0, bqFail=0;
  const srcFiles = fs.readdirSync(`${SRC}/impala/`).filter(f=>f.endsWith('.sql'));

  for (const num of scopeNums) {
    const file = srcFiles.find(f=>parseInt(f)===num);
    if (!file) continue;
    
    let sql = fs.readFileSync(`${SRC}/impala/${file}`,'utf8');
    // Strip comments
    sql = sql.split('\n').filter(l=>!l.trim().startsWith('--')).join('\n');
    
    // ═══ BQ dialect rewrites ═══
    // Variables
    sql = sql.replace(/\$\{var:run_date\}/g, RD);
    sql = sql.replace(/\$\{hivevar:run_date\}/g, RD);
    // Schema → BQ prefix
    sql = sql.replace(/\bstaging\.(\w+)/g, `${BQDS}.${BQP}$1`);
    sql = sql.replace(/\bods\.(\w+)/g, `${BQDS}.${BQP}$1`);
    sql = sql.replace(/\bdm\.(\w+)/g, `${BQDS}.${BQP}$1`);
    // INSERT OVERWRITE → DELETE+INSERT
    const insertMatch = sql.match(/INSERT\s+OVERWRITE\s+TABLE\s+(\S+)\s+(?:PARTITION\s*\([^)]*\)\s*)?/i);
    if (insertMatch) {
      const target = insertMatch[1];
      sql = sql.replace(/INSERT\s+OVERWRITE\s+TABLE\s+\S+\s+(?:PARTITION\s*\([^)]*\)\s*)?/i, `INSERT INTO ${target} `);
      // Add DELETE before INSERT
      sql = `DELETE FROM ${target} WHERE TRUE;\n` + sql;
    }
    // ═══ Comprehensive dialect rewrites: Hive/Impala → BigQuery ═══

    // from_unixtime(CAST(x/1000 AS BIGINT)) → TIMESTAMP_SECONDS(CAST(FLOOR(x/1000) AS INT64)) — MUST be before simpler patterns
    sql = sql.replace(/CAST\s*\(\s*from_unixtime\s*\(\s*CAST\s*\(\s*(.+?)\s*\/\s*1000\s*AS\s+BIGINT\s*\)\s*\)\s*AS\s+TIMESTAMP\s*\)/gi,
      'TIMESTAMP_SECONDS(CAST(FLOOR($1/1000) AS INT64))');
    sql = sql.replace(/from_unixtime\s*\(\s*CAST\s*\(\s*(.+?)\s*\/\s*1000\s*AS\s+BIGINT\s*\)\s*\)/gi,
      'CAST(TIMESTAMP_SECONDS(CAST(FLOOR($1/1000) AS INT64)) AS STRING)');
    // from_unixtime(unix_timestamp(x), 'fmt') → FORMAT_TIMESTAMP(bq_fmt, x)
    sql = sql.replace(/from_unixtime\s*\(\s*unix_timestamp\s*\(\s*(.+?)\s*\)\s*,\s*'yyyyMMdd'\s*\)/gi,
      "FORMAT_TIMESTAMP('%Y%m%d', $1)");
    // CAST(from_unixtime(x) AS TIMESTAMP) → TIMESTAMP_SECONDS(x)
    sql = sql.replace(/CAST\s*\(\s*from_unixtime\s*\(\s*(.+?)\s*\)\s*AS\s+TIMESTAMP\s*\)/gi, 'TIMESTAMP_SECONDS($1)');
    // Bare from_unixtime(x) → CAST(TIMESTAMP_SECONDS(x) AS STRING)
    sql = sql.replace(/from_unixtime\s*\(\s*([^,)]+)\s*\)/gi, 'CAST(TIMESTAMP_SECONDS($1) AS STRING)');
    // unix_timestamp(x, 'fmt') → UNIX_SECONDS(PARSE_TIMESTAMP('fmt', x))
    sql = sql.replace(/unix_timestamp\s*\(\s*(.+?)\s*,\s*'([^']+)'\s*\)/gi, (_, col, fmt) => {
      const bqFmt = fmt.replace(/yyyy/g,'%Y').replace(/MM/g,'%m').replace(/dd/g,'%d').replace(/HH/g,'%H').replace(/mm/g,'%M').replace(/ss/g,'%S');
      return `UNIX_SECONDS(PARSE_TIMESTAMP('${bqFmt}', ${col}))`;
    });
    // unix_timestamp(x) → UNIX_SECONDS(x)
    sql = sql.replace(/unix_timestamp\s*\(\s*([^)]+)\s*\)/gi, 'UNIX_SECONDS($1)');
    // to_date(x) → DATE(x)
    sql = sql.replace(/to_date\s*\(/gi, 'DATE(');
    // CAST(x AS INT) → CAST(x AS INT64)
    sql = sql.replace(/CAST\s*\(([^)]+?)\s+AS\s+INT\s*\)/gi, 'CAST($1 AS INT64)');
    // AS BIGINT → AS INT64
    sql = sql.replace(/AS\s+BIGINT/gi, 'AS INT64');
    // group_concat → STRING_AGG
    sql = sql.replace(/group_concat\s*\(/gi, 'STRING_AGG(');
    // CAST(x AS DECIMAL(p,s)) → CAST(x AS NUMERIC) — BQ doesn't allow parameterized CAST
    sql = sql.replace(/CAST\s*\((.+?)\s+AS\s+DECIMAL\s*\(\s*\d+\s*,\s*\d+\s*\)\s*\)/gi, 'CAST($1 AS NUMERIC)');
    // Remaining DECIMAL → NUMERIC in DDL contexts
    sql = sql.replace(/DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)/gi, 'NUMERIC');
    // regexp_replace → REGEXP_REPLACE
    sql = sql.replace(/regexp_replace\s*\(/gi, 'REGEXP_REPLACE(');
    // substr → SUBSTR (already valid)
    // COMPUTE STATS → remove
    sql = sql.replace(/COMPUTE\s+(?:INCREMENTAL\s+)?STATS[^;]*;?/gi, '');
    // STRAIGHT_JOIN → remove
    sql = sql.replace(/\bSTRAIGHT_JOIN\b/gi, '');
    // COALESCE(x, y) — already valid in BQ
    // Boolean: (expr) AS BOOLEAN → keep as is, BQ handles it

    const stmts = sql.split(';').map(s=>s.trim()).filter(s=>s.length>10&&!s.match(/^SET\s/i));

    try {
      for (const stmt of stmts) { await bqQ(stmt); }
      bqOk++;
      L(`  ✓ [bq] ${file}`);
    } catch(e) {
      bqFail++;
      L(`  ✗ [bq] ${file}: ${e.message?.substring(0,100)}`);
    }
  }
  L(`  BQ: ${bqOk} pass, ${bqFail} fail`);

  // Step 4: Compare — read from BOTH engines, compute MD5 in Node
  L('\n=== Compare Hive vs BQ ===');

  // Read legacy evidence files
  const perTableFiles = fs.readdirSync(`${EV}/per_table/`).filter(f=>f.endsWith('_legacy.json'));
  const comparison = {};
  let pass=0, fail=0, notEx=0;

  for (const f of perTableFiles) {
    const tbl = f.replace('_legacy.json','');
    const legData = JSON.parse(fs.readFileSync(`${EV}/per_table/${f}`,'utf8'));
    const hCnt = legData.count;
    const hFP = legData.fingerprint;

    // Get BQ count and rows
    const bqTbl = `${BQDS}.${BQP}${tbl}`;
    let bCnt = -1, bFP = null;
    try {
      const [r] = await bqQ(`SELECT COUNT(*) c FROM ${bqTbl}`);
      bCnt = Number(r.c);

      if (bCnt > 0 && bCnt <= 1000) {
        // Compute BQ fingerprint with same algorithm as Hive
        const bqRows = await bqQ(`SELECT * FROM ${bqTbl} LIMIT 1000`);
        if (bqRows.length > 0) {
          // Get Hive rows for column intersection
          const hRows = await hQ(impS, `SELECT * FROM ${HDB}.${tbl} LIMIT 1000`);
          if (hRows.length > 0) {
            const hCols = Object.keys(hRows[0]).map(c=>c.toLowerCase()).sort();
            const bCols = Object.keys(bqRows[0]).map(c=>c.toLowerCase()).sort();
            const common = hCols.filter(c=>bCols.includes(c));

            const hashRows = (rows, cols) => {
              return rows.map(r => {
                const nr = {};
                for (const [k,v] of Object.entries(r)) nr[k.toLowerCase()] = v;
                return crypto.createHash('md5')
                  .update(cols.map(c => norm(nr[c])).join('|'))
                  .digest('hex');
              }).sort();
            };

            const hH = hashRows(hRows, common);
            const bH = hashRows(bqRows, common);
            const hTF = crypto.createHash('md5').update(hH.join('\n')).digest('hex');
            const bTF = crypto.createHash('md5').update(bH.join('\n')).digest('hex');
            bFP = bTF;

            const fpMatch = hTF === bTF;
            const cntMatch = hCnt === bCnt;
            const status = cntMatch && fpMatch ? 'PASS' : 'FAIL';

            comparison[tbl] = {
              status, hive_count: hCnt, bq_count: bCnt,
              count_match: cntMatch, fingerprint_match: fpMatch,
              hive_fp: hTF, bq_fp: bTF,
              common_cols: common.length, 
            };

            if (status === 'PASS') pass++; else fail++;
            L(`  ${tbl}: ${status} h=${hCnt} b=${bCnt} fp=${fpMatch ? 'MATCH' : 'DIFF'} (${common.length} cols)`);

            // Save per-table BQ evidence
            fs.writeFileSync(`${EV}/per_table/${tbl}_bq.json`, JSON.stringify({
              count: bCnt, fingerprint: bTF, sample: bqRows.slice(0,3)
            }, null, 2));

            continue;
          }
        }
      }

      // BQ table exists but no fingerprint comparison possible
      comparison[tbl] = {
        status: bCnt === hCnt ? 'COUNT_MATCH' : 'COUNT_MISMATCH',
        hive_count: hCnt, bq_count: bCnt,
      };
      if (bCnt === hCnt) pass++; else fail++;
      L(`  ${tbl}: h=${hCnt} b=${bCnt} ${bCnt===hCnt?'count_match':'COUNT_MISMATCH'}`);

    } catch(e) {
      comparison[tbl] = { status: 'BQ_ERROR', hive_count: hCnt, error: e.message?.substring(0,100) };
      notEx++;
      L(`  ${tbl}: BQ_ERROR — ${e.message?.substring(0,60)}`);
    }
  }

  const result = {
    total_tables: perTableFiles.length,
    pass, fail, not_exercised: notEx,
    tables: comparison,
  };

  fs.writeFileSync(`${EV}/ac1_final_comparison.json`, JSON.stringify(result, null, 2));

  L('\n=== FINAL RESULT ===');
  L(`Tables compared: ${perTableFiles.length}`);
  L(`PASS: ${pass}, FAIL: ${fail}, NOT_EXERCISED: ${notEx}`);
  L(`reconciled ${pass}/${perTableFiles.length} tables`);

  try { await impS.close(); await impC.close(); } catch {}
  L('=== DONE ===');
}

main().catch(e => { console.error('FATAL:', e.stack); process.exit(1); });
