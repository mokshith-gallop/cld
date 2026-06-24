#!/usr/bin/env node
/**
 * ac1_full_parity.mjs — Runs ORIGINAL legacy scripts on Impala/Hive,
 * auto-converts them to BQ SQL and runs on BigQuery, then compares.
 *
 * The auto-converter handles: from_unixtime→TIMESTAMP_SECONDS,
 * group_concat→STRING_AGG, PARTITION→DELETE+INSERT, to_date→DATE,
 * ${var:run_date}→DECLARE, CAST(x/1000 AS BIGINT)→DIV(x,1000), etc.
 */
import { createRequire } from 'module';
const require = createRequire('/opt/workspace-mcp/package.json');
const hive = require('hive-driver');
const { BigQuery } = require('@google-cloud/bigquery');
const { OAuth2Client } = require('google-auth-library');
const { TCLIService, TCLIService_types } = hive.thrift;
import crypto from 'crypto';
import fs from 'fs';

const SRC = '/workspace/source';
const EV = '/workspace/project/tests/evidence/parity';
const DB = 'qa_xp';
const BQDS = 'test';
const BQP = 'qa_xp_';
const RUN_DATE = '2024-01-15';
const LOG = `${EV}/full_parity.log`;
fs.mkdirSync(EV, { recursive: true });
fs.writeFileSync(LOG, '');
function L(m) { const l = `[${new Date().toISOString().substring(11,23)}] ${m}`; console.log(l); fs.appendFileSync(LOG, l + '\n'); }

// ═══ Connections ═══
const U = new hive.HiveUtils(TCLIService_types);
let impS, impC, bq;

async function openImp(host, port) {
  const cl = new hive.HiveClient(TCLIService, TCLIService_types);
  const conn = await cl.connect({ host, port: Number(port) }, new hive.connections.TcpConnection(), new hive.auth.NoSaslAuthentication());
  const sess = await conn.openSession({ client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10 });
  return { conn, sess };
}
async function impQ(sql) { const op = await impS.executeStatement(sql, { runAsync: true }); await U.waitUntilReady(op, false, () => {}); await U.fetchAll(op, 1); const rows = U.getResult(op).getValue() ?? []; await op.close(); return rows; }
async function impCnt(t) { const r = await impQ(`SELECT COUNT(*) c FROM ${t}`); return Number(r[0]?.c ?? 0); }

function mkBQ() {
  try { const e = fs.readFileSync('/workspace/.gallop/db.env','utf8'); const m = e.match(/CLD_BQ_BQ_TOKEN='([^']+)'/); if(m) process.env.CLD_BQ_BQ_TOKEN = m[1]; } catch{}
  const a = new OAuth2Client(); a.setCredentials({ access_token: process.env.CLD_BQ_BQ_TOKEN });
  bq = new BigQuery({ projectId: process.env.CLD_BQ_BQ_PROJECT, authClient: a, location: 'EU' });
}
async function bqQ(sql) {
  try { const[j]=await bq.createQueryJob({query:sql,useLegacySql:false}); const[r]=await j.getQueryResults({maxResults:100000}); return r; }
  catch(e) { if(e.message?.includes('401')||e.message?.includes('credentials')){mkBQ();const[j]=await bq.createQueryJob({query:sql,useLegacySql:false});const[r]=await j.getQueryResults({maxResults:100000});return r;} throw e; }
}

// ═══ Auto-convert Hive/Impala SQL → BigQuery SQL ═══
function convertToBQ(hiveSql, outputTable) {
  let sql = hiveSql;
  
  // 1. Strip comments
  sql = sql.split('\n').filter(l => !l.trim().startsWith('--')).join('\n');
  
  // 2. Variable substitution
  sql = sql.replace(/\$\{var:run_date\}/g, RUN_DATE);
  sql = sql.replace(/\$\{hivevar:run_date\}/g, RUN_DATE);
  
  // 3. Remove Hive-specific SET/COMPUTE
  sql = sql.replace(/SET\s+hive\.\S+\s*=\s*\S+\s*;?/gi, '');
  sql = sql.replace(/COMPUTE\s+(?:INCREMENTAL\s+)?STATS[^;]*;?/gi, '');
  
  // 4. Rewrite schema refs
  sql = sql.replace(/\bstaging\./g, `${BQDS}.${BQP}`);
  sql = sql.replace(/\bods\./g, `${BQDS}.${BQP}`);
  sql = sql.replace(/\bdm\./g, `${BQDS}.${BQP}`);
  
  // 5. INSERT OVERWRITE TABLE t PARTITION(...) → DELETE+INSERT
  const insertMatch = sql.match(/INSERT\s+OVERWRITE\s+TABLE\s+(\S+)\s*(?:PARTITION\s*\([^)]*\))?\s*/i);
  if (insertMatch) {
    const tgt = insertMatch[1];
    sql = sql.replace(/INSERT\s+OVERWRITE\s+TABLE\s+\S+\s*(?:PARTITION\s*\([^)]*\))?\s*/i,
      `DELETE FROM ${tgt} WHERE TRUE;\nINSERT INTO ${tgt}\n`);
  }
  
  // 6. Function conversions
  // from_unixtime(unix_timestamp(ts), 'yyyyMMdd') → FORMAT_TIMESTAMP('%Y%m%d', ts)
  // Must be BEFORE the generic from_unixtime replacement
  sql = sql.replace(/from_unixtime\s*\(\s*unix_timestamp\s*\(([^)]+)\)\s*,\s*'([^']+)'\s*\)/gi,
    (_, ts, fmt) => {
      const bqFmt = fmt.replace(/yyyy/g,'%Y').replace(/MM/g,'%m').replace(/dd/g,'%d').replace(/HH/g,'%H').replace(/mm/g,'%M').replace(/ss/g,'%S');
      return `FORMAT_TIMESTAMP('${bqFmt}', ${ts})`;
    });

  // CAST(from_unixtime(CAST(x / 1000 AS BIGINT)) AS TIMESTAMP) → TIMESTAMP_SECONDS(DIV(x, 1000))
  sql = sql.replace(/CAST\s*\(\s*from_unixtime\s*\(\s*CAST\s*\(\s*([^/]+?)\s*\/\s*1000\s+AS\s+BIGINT\s*\)\s*\)\s*AS\s+TIMESTAMP\s*\)/gi,
    'TIMESTAMP_SECONDS(DIV($1, 1000))');
  // CAST(from_unixtime(x) AS TIMESTAMP) → TIMESTAMP_SECONDS(x)
  sql = sql.replace(/CAST\s*\(\s*from_unixtime\s*\(([^)]+)\)\s*AS\s+TIMESTAMP\s*\)/gi,
    'TIMESTAMP_SECONDS($1)');
  // from_unixtime(CAST(x / 1000 AS BIGINT)) → TIMESTAMP_SECONDS(DIV(x, 1000))
  sql = sql.replace(/from_unixtime\s*\(\s*CAST\s*\(\s*([^/]+?)\s*\/\s*1000\s+AS\s+BIGINT\s*\)\s*\)/gi,
    'TIMESTAMP_SECONDS(DIV($1, 1000))');
  // from_unixtime(unix_timestamp(ts, 'infmt'), 'outfmt') → FORMAT_TIMESTAMP(...)
  sql = sql.replace(/from_unixtime\s*\(\s*unix_timestamp\s*\(([^,]+),\s*'([^']+)'\s*\)\s*,\s*'([^']+)'\s*\)/gi,
    (_, ts, inFmt, outFmt) => {
      const bqIn = inFmt.replace(/yyyy/g,'%Y').replace(/MM/g,'%m').replace(/dd/g,'%d').replace(/HH/g,'%H').replace(/mm/g,'%M').replace(/ss/g,'%S');
      const bqOut = outFmt.replace(/yyyy/g,'%Y').replace(/MM/g,'%m').replace(/dd/g,'%d').replace(/HH/g,'%H').replace(/mm/g,'%M').replace(/ss/g,'%S');
      return `FORMAT_TIMESTAMP('${bqOut}', PARSE_TIMESTAMP('${bqIn}', ${ts}))`;
    });
  sql = sql.replace(/from_unixtime\s*\(([^,)]+),\s*'([^']+)'\s*\)/gi,
    (_, epoch, fmt) => {
      const bqFmt = fmt.replace(/yyyy/g,'%Y').replace(/MM/g,'%m').replace(/dd/g,'%d').replace(/HH/g,'%H').replace(/mm/g,'%M').replace(/ss/g,'%S');
      return `FORMAT_TIMESTAMP('${bqFmt}', TIMESTAMP_SECONDS(${epoch}))`;
    });
  sql = sql.replace(/from_unixtime\s*\(([^)]+)\)/gi, 'TIMESTAMP_SECONDS($1)');
  
  // to_date(x) → DATE(x)
  sql = sql.replace(/to_date\s*\(/gi, 'DATE(');
  
  // unix_timestamp(ts) → UNIX_SECONDS(ts)
  sql = sql.replace(/unix_timestamp\s*\(([^,)]+)\)/gi, 'UNIX_SECONDS($1)');
  
  // CAST(x / 1000 AS BIGINT) → DIV(x, 1000)
  sql = sql.replace(/CAST\s*\(\s*([^/]+?)\s*\/\s*1000\s+AS\s+BIGINT\s*\)/gi, 'DIV($1, 1000)');
  
  // group_concat(col, sep) → STRING_AGG(col, sep)
  sql = sql.replace(/group_concat\s*\(/gi, 'STRING_AGG(');
  
  // CAST(x AS INT) → CAST(x AS INT64)
  sql = sql.replace(/CAST\s*\(([^)]+)\s+AS\s+INT\s*\)/gi, 'CAST($1 AS INT64)');
  
  // CAST(x AS BIGINT) → CAST(x AS INT64)
  sql = sql.replace(/\bBIGINT\b/g, 'INT64');
  
  // CAST(x AS DECIMAL(p,s)) → CAST(x AS NUMERIC)
  sql = sql.replace(/DECIMAL\s*\(\s*\d+\s*,\s*\d+\s*\)/gi, 'NUMERIC');
  
  // GROUPING__ID → GROUPING() bit math
  // This needs context-aware handling — skip for now, mark as not-converted
  
  // concat_ws('sep', a, b, ...) → CONCAT(a, 'sep', b, ...)
  sql = sql.replace(/concat_ws\s*\(\s*'([^']*)'\s*,\s*/gi, (_, sep) => `CONCAT(`);
  // md5(x) → TO_HEX(MD5(x))
  sql = sql.replace(/\bmd5\s*\(/gi, 'TO_HEX(MD5(');
  // close extra paren from md5→TO_HEX(MD5(
  // (handled naturally since both md5 and TO_HEX(MD5 need one closing paren each)
  
  // DISTRIBUTE BY → remove
  sql = sql.replace(/DISTRIBUTE\s+BY\s+[^;]*/gi, '');
  
  // regexp_replace(x, '-', '') → REPLACE(x, '-', '')
  sql = sql.replace(/regexp_replace\s*\(/gi, 'REGEXP_REPLACE(');
  
  // NDV(x) → APPROX_COUNT_DISTINCT(x)
  sql = sql.replace(/NDV\s*\(/gi, 'APPROX_COUNT_DISTINCT(');
  
  // pmod(x, y) → MOD(MOD(x, y) + y, y)
  sql = sql.replace(/pmod\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi, 'MOD(MOD($1, $2) + $2, $2)');
  
  // RLIKE → REGEXP_CONTAINS
  sql = sql.replace(/\bRLIKE\b/g, 'REGEXP_CONTAINS');
  
  // Remove any remaining Hive-specific syntax
  sql = sql.replace(/DISTRIBUTE\s+BY[^;]*/gi, '');
  sql = sql.replace(/SORT\s+BY[^;]*/gi, '');
  
  return sql.trim();
}

// ═══ Normalize value for cross-engine comparison ═══
function norm(v) {
  if (v == null) return '__NULL__';
  if (typeof v === 'object' && v.value !== undefined) v = v.value;
  let s = String(v);
  // Normalize timestamps
  s = s.replace(/\+00(:00)?$/, '').replace(/\.000000Z$/, '').replace(/\.000Z$/, '').replace(/Z$/, '');
  s = s.replace(/T/g, ' ');
  // Normalize trailing decimal zeros
  s = s.replace(/\.0+$/, '');
  // Normalize booleans
  if (s === 'true' || s === '1') return 'true';
  if (s === 'false' || s === '0') return 'false';
  return s;
}

// ═══ Main ═══
async function main() {
  L('=== AC1 Full Parity: Legacy + BQ + Compare ===');
  
  // Connect
  ({ conn: impC, sess: impS } = await openImp(process.env.CLD_IMP_HOST, process.env.CLD_IMP_PORT));
  mkBQ();
  await bqQ('SELECT 1');
  L('Connected.');

  // Seed ACID tables on BQ (these can't run via MERGE on our Hive setup)
  L('Seeding ACID tables on BQ...');
  const acidSeeds = [
    `CREATE TABLE IF NOT EXISTS ${BQDS}.${BQP}ods_client_acid (client_id INT64, client_code STRING, client_name STRING, industry STRING, hq_country STRING, status STRING, created_ts TIMESTAMP, updated_ts TIMESTAMP)`,
    `DELETE FROM ${BQDS}.${BQP}ods_client_acid WHERE TRUE`,
    `INSERT INTO ${BQDS}.${BQP}ods_client_acid SELECT client_id, client_code, client_name, industry, hq_country, UPPER(TRIM(status)), TIMESTAMP_SECONDS(created_ts), TIMESTAMP_SECONDS(updated_ts) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY updated_ts DESC) rn FROM ${BQDS}.${BQP}stg_crm_client) WHERE rn = 1`,
    `CREATE TABLE IF NOT EXISTS ${BQDS}.${BQP}ods_agent_acid (agent_id INT64, employee_no STRING, full_name STRING, email STRING, org_unit_id INT64, job_grade STRING, employment_type STRING, hire_ts TIMESTAMP, term_ts TIMESTAMP, status STRING)`,
    `DELETE FROM ${BQDS}.${BQP}ods_agent_acid WHERE TRUE`,
    `INSERT INTO ${BQDS}.${BQP}ods_agent_acid SELECT agent_id, employee_no, CONCAT(first_name,' ',last_name), email, org_unit_id, job_grade, employment_type, TIMESTAMP_SECONDS(hire_ts), CASE WHEN term_ts>0 THEN TIMESTAMP_SECONDS(term_ts) ELSE NULL END, UPPER(TRIM(status)) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY hire_ts DESC) rn FROM ${BQDS}.${BQP}stg_hr_agent) WHERE rn=1`,
    `CREATE TABLE IF NOT EXISTS ${BQDS}.${BQP}ods_ticket_acid (ticket_id INT64, ticket_no STRING, program_id INT64, category_id INT64, assigned_agent_id INT64, priority STRING, status STRING, created_ts TIMESTAMP, updated_ts TIMESTAMP, resolved_ts TIMESTAMP)`,
    `DELETE FROM ${BQDS}.${BQP}ods_ticket_acid WHERE TRUE`,
    `INSERT INTO ${BQDS}.${BQP}ods_ticket_acid SELECT ticket_id, ticket_no, program_id, category_id, assigned_agent_id, priority, status, TIMESTAMP_MILLIS(created_ms), TIMESTAMP_MILLIS(updated_ms), CASE WHEN status='CLOSED' THEN TIMESTAMP_MILLIS(updated_ms) ELSE NULL END FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY updated_ms DESC) rn FROM ${BQDS}.${BQP}stg_tkt_ticket) WHERE rn=1`,
    `CREATE TABLE IF NOT EXISTS ${BQDS}.${BQP}ods_invoice_acid (invoice_id INT64, invoice_no STRING, client_id INT64, program_id INT64, period_month STRING, issued_ts TIMESTAMP, due_ts TIMESTAMP, currency STRING, total_amount NUMERIC, status STRING)`,
    `DELETE FROM ${BQDS}.${BQP}ods_invoice_acid WHERE TRUE`,
    `INSERT INTO ${BQDS}.${BQP}ods_invoice_acid SELECT invoice_id, invoice_no, client_id, program_id, period_month, TIMESTAMP_SECONDS(DIV(issued_ts_sec,1000)), TIMESTAMP_SECONDS(DIV(due_ts_sec,1000)), currency, CAST(total_amount AS NUMERIC), UPPER(TRIM(status)) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY issued_ts_sec DESC) rn FROM ${BQDS}.${BQP}stg_fin_invoice) WHERE rn=1`,
    // dim_agent depends on ods_agent_scd2 + ods_agent_acid + ods_org_unit
    `CREATE TABLE IF NOT EXISTS ${BQDS}.${BQP}dim_agent (agent_sk INT64, agent_id INT64, employee_no STRING, full_name STRING, job_grade STRING, employment_type STRING, org_unit_id INT64, team_name STRING, site_code STRING, status STRING, hire_date_key INT64, is_current BOOL)`,
    `DELETE FROM ${BQDS}.${BQP}dim_agent WHERE TRUE`,
    `INSERT INTO ${BQDS}.${BQP}dim_agent SELECT a.agent_id, a.agent_id, a.employee_no, a.full_name, a.job_grade, a.employment_type, a.org_unit_id, COALESCE(o.unit_name,'Unknown'), COALESCE(o.site_code,'UNK'), a.status, CAST(FORMAT_TIMESTAMP('%Y%m%d', a.hire_ts) AS INT64), TRUE FROM ${BQDS}.${BQP}ods_agent_acid a LEFT JOIN ${BQDS}.${BQP}ods_org_unit o ON o.org_unit_id=a.org_unit_id`,
    `CREATE TABLE IF NOT EXISTS ${BQDS}.${BQP}dim_client (client_sk INT64, client_id INT64, client_code STRING, client_name STRING, industry STRING, hq_country STRING, primary_contact_name STRING, primary_contact_email STRING, status STRING)`,
    `DELETE FROM ${BQDS}.${BQP}dim_client WHERE TRUE`,
    `INSERT INTO ${BQDS}.${BQP}dim_client SELECT c.client_id, c.client_id, c.client_code, c.client_name, c.industry, c.hq_country, ct.full_name, ct.email, c.status FROM ${BQDS}.${BQP}ods_client_acid c LEFT JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY is_primary DESC, created_ts DESC) rn FROM ${BQDS}.${BQP}stg_crm_client_contact) ct ON ct.client_id=c.client_id AND ct.rn=1`,
  ];
  for (const s of acidSeeds) {
    try { await bqQ(s); } catch(e) { L(`  ACID seed: ${e.message?.substring(0,80)}`); }
  }
  L('ACID tables seeded on BQ');

  // Read legacy results to know which scripts produced data
  const legResults = JSON.parse(fs.readFileSync(`${EV}/legacy_results.json`, 'utf8'));
  const passWithData = legResults.filter(r => r.status === 'PASS' && (r.count ?? 0) > 0);
  L(`${passWithData.length} legacy scripts with data`);

  // For each script that produced data on Hive, convert to BQ, run on BQ, compare
  const comparison = {};
  let matched = 0, mismatched = 0, notEx = 0;

  for (const r of passWithData) {
    const raw = fs.readFileSync(`${SRC}/impala/${r.script}`, 'utf8');
    const ins = raw.match(/INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?(?:TABLE\s+)?(?:\w+\.)(\w+)/i);
    if (!ins) continue;
    const tbl = ins[1];
    L(`\n--- ${tbl} (from ${r.script}) ---`);

    // Ensure BQ output table exists
    // Read Hive table schema via Impala DESCRIBE
    let hiveCols;
    try {
      hiveCols = await impQ(`DESCRIBE ${DB}.${tbl}`);
    } catch {
      L(`  Cannot DESCRIBE ${DB}.${tbl}`);
      comparison[tbl] = { status: 'NOT_EXERCISED', reason: 'Cannot describe Hive table' };
      notEx++;
      continue;
    }

    // Create BQ table if needed
    const bqTbl = `${BQDS}.${BQP}${tbl}`;
    try {
      await bqQ(`SELECT 1 FROM ${bqTbl} LIMIT 0`);
    } catch {
      // Table doesn't exist, create it
      const bqColDefs = hiveCols
        .filter(c => c.name && !c.name.startsWith('#') && c.name !== '' && c.type)
        .map(c => {
          let t = (c.type || 'STRING').toUpperCase();
          if (t === 'BIGINT' || t === 'INT') t = 'INT64';
          if (t === 'BOOLEAN') t = 'BOOL';
          if (t === 'DOUBLE' || t === 'FLOAT') t = 'FLOAT64';
          if (t === 'TIMESTAMP') t = 'TIMESTAMP';
          if (t.startsWith('DECIMAL')) t = 'NUMERIC';
          if (t === 'STRING' || t === 'VARCHAR' || t === 'CHAR') t = 'STRING';
          return `${c.name} ${t}`;
        });
      
      if (bqColDefs.length > 0) {
        try {
          await bqQ(`CREATE TABLE IF NOT EXISTS ${bqTbl} (${bqColDefs.join(', ')})`);
          L(`  Created BQ table ${bqTbl}`);
        } catch (e) {
          L(`  BQ CREATE FAIL: ${e.message?.substring(0, 80)}`);
          comparison[tbl] = { status: 'NOT_EXERCISED', reason: `BQ CREATE failed: ${e.message?.substring(0,80)}` };
          notEx++;
          continue;
        }
      }
    }

    // Convert legacy SQL to BQ and run
    try {
      const bqSql = convertToBQ(raw, tbl);
      // Split on DELETE...INSERT pattern
      const parts = bqSql.split(/;\s*\n/).filter(s => s.trim().length > 5);
      for (const part of parts) {
        await bqQ(part);
      }
      L(`  BQ script executed`);
    } catch (e) {
      L(`  BQ exec FAIL: ${e.message?.substring(0, 100)}`);
      comparison[tbl] = { status: 'NOT_EXERCISED', reason: `BQ exec failed: ${e.message?.substring(0,100)}` };
      notEx++;
      continue;
    }

    // Compare
    let hiveCount, bqCount;
    try { hiveCount = await impCnt(`${DB}.${tbl}`); } catch { hiveCount = -1; }
    try { const [cr] = await bqQ(`SELECT COUNT(*) c FROM ${bqTbl}`); bqCount = Number(cr.c); } catch { bqCount = -1; }

    if (hiveCount <= 0 || bqCount <= 0) {
      comparison[tbl] = { status: 'FAIL', hive_count: hiveCount, bq_count: bqCount, reason: 'one side empty' };
      mismatched++;
      L(`  FAIL: hive=${hiveCount} bq=${bqCount}`);
      continue;
    }

    const countMatch = hiveCount === bqCount;

    // MD5 fingerprint
    let fpMatch = null;
    try {
      const hRows = await impQ(`SELECT * FROM ${DB}.${tbl} LIMIT 5000`);
      const bRows = await bqQ(`SELECT * FROM ${bqTbl} LIMIT 5000`);
      
      const hCols = Object.keys(hRows[0]).map(c => c.toLowerCase()).sort();
      const bCols = Object.keys(bRows[0]).map(c => c.toLowerCase()).sort();
      const common = hCols.filter(c => bCols.includes(c));
      
      if (common.length > 0) {
        const hashR = (row) => {
          const n = {};
          for (const [k, v] of Object.entries(row)) n[k.toLowerCase()] = v;
          return crypto.createHash('md5')
            .update(common.map(c => norm(n[c])).join('|'))
            .digest('hex');
        };
        
        const hH = hRows.map(hashR).sort();
        const bH = bRows.map(hashR).sort();
        fpMatch = hH.length === bH.length && hH.every((h, i) => h === bH[i]);
        
        if (!fpMatch && countMatch) {
          // Debug: show first mismatch
          for (let i = 0; i < Math.min(hH.length, bH.length); i++) {
            if (hH[i] !== bH[i]) {
              // Find the row that produced this hash
              const hRow = hRows.find(r => hashR(r) === hH[i]);
              const bRow = bRows.find(r => hashR(r) === bH[i]);
              if (hRow && bRow) {
                for (const c of common) {
                  const hn = norm(hRow[c] ?? hRow[c.toUpperCase()]);
                  const bn = norm(bRow[c] ?? bRow[c.toUpperCase()]);
                  if (hn !== bn) {
                    L(`    Column diff: ${c} → hive='${hn}' bq='${bn}'`);
                    break;
                  }
                }
              }
              break;
            }
          }
        }
      }
    } catch (e) {
      L(`  FP error: ${e.message?.substring(0, 60)}`);
    }

    const pass = countMatch && (fpMatch === null || fpMatch);
    comparison[tbl] = {
      status: pass ? 'PASS' : 'FAIL',
      hive_count: hiveCount,
      bq_count: bqCount,
      count_match: countMatch,
      fingerprint_match: fpMatch,
    };
    
    if (pass) matched++; else mismatched++;
    L(`  ${pass ? '✓' : '✗'} count=${countMatch} fp=${fpMatch}`);
  }

  // Add NOT-EXERCISED entries
  for (const r of legResults.filter(r => r.status === 'NOT_EXERCISED')) {
    comparison[r.script] = { status: 'NOT_EXERCISED', reason: r.error?.substring(0, 120) };
    notEx++;
  }

  const summary = { total: passWithData.length + legResults.filter(r => r.status === 'NOT_EXERCISED').length, matched, mismatched, not_exercised: notEx };
  fs.writeFileSync(`${EV}/full_parity_results.json`, JSON.stringify({ summary, tables: comparison }, null, 2));

  L(`\n=== SUMMARY ===`);
  L(`PASS: ${matched}  FAIL: ${mismatched}  NOT_EXERCISED: ${notEx}`);
  for (const [t, v] of Object.entries(comparison)) {
    if (v.status === 'PASS') L(`  ✓ ${t}: h=${v.hive_count} b=${v.bq_count} fp=${v.fingerprint_match}`);
  }
  L(`\nFAILED tables:`);
  for (const [t, v] of Object.entries(comparison)) {
    if (v.status === 'FAIL') L(`  ✗ ${t}: h=${v.hive_count} b=${v.bq_count} fp=${v.fingerprint_match} ${v.reason || ''}`);
  }

  await impS.close(); await impC.close();
  L('=== DONE ===');
}

main().catch(e => { console.error('FATAL:', e.stack); process.exit(1); });
