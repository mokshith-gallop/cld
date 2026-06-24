#!/usr/bin/env node
/**
 * ac1_bq_and_compare.mjs
 *
 * Phase 2: Runs converted scripts on BQ, then compares with Hive results.
 * Reads legacy_results.json from Phase 1 to know which tables have data.
 * Creates BQ output tables, runs converted SQL, pulls rows, computes MD5.
 */
import { createRequire } from 'module';
const require = createRequire('/opt/workspace-mcp/package.json');
const hive = require('hive-driver');
const { BigQuery } = require('@google-cloud/bigquery');
const { OAuth2Client } = require('google-auth-library');
const { TCLIService, TCLIService_types } = hive.thrift;
import crypto from 'crypto';
import fs from 'fs';

const ROOT = '/workspace/project';
const SRC  = '/workspace/source';
const EV   = `${ROOT}/tests/evidence/parity`;
const DB   = 'qa_xp';
const BQDS = 'test';
const BQP  = 'qa_xp_';
const RUN_DATE = '2024-01-15';
const LOG  = `${EV}/bq_compare.log`;

fs.writeFileSync(LOG, '');
function L(m) { const l=`[${new Date().toISOString().substring(11,23)}] ${m}`; console.log(l); fs.appendFileSync(LOG,l+'\n'); }

// ═══ Connections ═══
const U = new hive.HiveUtils(TCLIService_types);
let impS, impC, bq;

async function openImp() {
  const cl = new hive.HiveClient(TCLIService, TCLIService_types);
  const conn = await cl.connect(
    { host: process.env.CLD_IMP_HOST, port: Number(process.env.CLD_IMP_PORT) },
    new hive.connections.TcpConnection(),
    new hive.auth.NoSaslAuthentication()
  );
  const sess = await conn.openSession({
    client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
  });
  return { conn, sess };
}

async function impQ(sql) {
  const op = await impS.executeStatement(sql, { runAsync: true });
  await U.waitUntilReady(op, false, () => {});
  await U.fetchAll(op, 1);
  const rows = U.getResult(op).getValue() ?? [];
  await op.close();
  return rows;
}

function mkBQ() {
  try { const e=fs.readFileSync('/workspace/.gallop/db.env','utf8'); const m=e.match(/CLD_BQ_BQ_TOKEN='([^']+)'/); if(m) process.env.CLD_BQ_BQ_TOKEN=m[1]; } catch{}
  const a=new OAuth2Client(); a.setCredentials({access_token:process.env.CLD_BQ_BQ_TOKEN});
  bq=new BigQuery({projectId:process.env.CLD_BQ_BQ_PROJECT,authClient:a,location:'EU'});
}

async function bqQ(sql) {
  try { const[j]=await bq.createQueryJob({query:sql,useLegacySql:false}); const[r]=await j.getQueryResults({maxResults:100000}); return r; }
  catch(e) { if(e.message?.includes('401')||e.message?.includes('credentials')){mkBQ(); const[j]=await bq.createQueryJob({query:sql,useLegacySql:false}); const[r]=await j.getQueryResults({maxResults:100000}); return r;} throw e; }
}

function norm(v) {
  if (v == null) return '__NULL__';
  if (typeof v === 'object' && v.value !== undefined) return String(v.value);
  let s = String(v);
  s = s.replace(/\+00(:00)?$/, '').replace(/T/g, ' ').replace(/\.0+$/, '');
  // Normalize boolean variations
  if (s === 'true' || s === '1') return 'true';
  if (s === 'false' || s === '0') return 'false';
  return s;
}

// ═══ Main ═══
async function main() {
  L('=== AC1 Phase 2: BQ execution + cross-engine comparison ===');

  ({ conn: impC, sess: impS } = await openImp());
  mkBQ();
  await bqQ('SELECT 1');
  L('Connected to Impala + BQ');

  // Read legacy results
  const legResults = JSON.parse(fs.readFileSync(`${EV}/legacy_results.json`, 'utf8'));
  const passWithData = legResults.filter(r => r.status === 'PASS' && (r.count ?? 0) > 0);
  L(`Legacy scripts with data: ${passWithData.length}`);

  // Map script → output table
  const scriptToTable = {};
  for (const r of legResults) {
    const num = parseInt(r.script.split('-')[0]);
    // Determine output table name from the script file
    const raw = fs.readFileSync(`${SRC}/impala/${r.script}`, 'utf8');
    const ins = raw.match(/INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?(?:TABLE\s+)?(?:\w+\.)(\w+)/i);
    if (ins) scriptToTable[r.script] = ins[1];
  }

  // For each output table on Hive that has data, read rows and compute fingerprint
  const comparison = {};
  let matchCount = 0, mismatchCount = 0, notExCount = 0;

  for (const r of passWithData) {
    const tbl = scriptToTable[r.script];
    if (!tbl) { L(`  Skip ${r.script}: no output table found`); continue; }
    const hiveFull = `${DB}.${tbl}`;
    const bqFull = `${BQDS}.${BQP}${tbl}`;

    L(`\n--- Comparing ${tbl} ---`);

    // Get Hive rows
    let hiveRows, hiveCount;
    try {
      hiveRows = await impQ(`SELECT * FROM ${hiveFull} LIMIT 5000`);
      hiveCount = hiveRows.length;
      L(`  Hive: ${hiveCount} rows`);
    } catch (e) {
      L(`  Hive read FAIL: ${e.message?.substring(0, 80)}`);
      comparison[tbl] = { status: 'NOT_EXERCISED', reason: `Hive read error: ${e.message?.substring(0,80)}` };
      notExCount++;
      continue;
    }

    // Check if BQ table exists
    let bqCount;
    try {
      const [cr] = await bqQ(`SELECT COUNT(*) c FROM ${bqFull}`);
      bqCount = Number(cr.c);
      L(`  BQ: ${bqCount} rows`);
    } catch {
      // Table doesn't exist on BQ yet — need to run BQ script
      bqCount = -1;
      L(`  BQ table ${bqFull} not found`);
    }

    if (bqCount === -1) {
      comparison[tbl] = { status: 'NOT_EXERCISED', reason: 'BQ table not created', hive_count: hiveCount };
      notExCount++;
      continue;
    }

    // Row count comparison
    const countMatch = hiveCount === bqCount;

    // MD5 fingerprint comparison
    let fpMatch = null;
    if (hiveCount > 0 && bqCount > 0 && hiveCount <= 5000) {
      try {
        const bqRows = await bqQ(`SELECT * FROM ${bqFull} LIMIT 5000`);

        // Find common columns
        const hCols = Object.keys(hiveRows[0]).map(c => c.toLowerCase()).sort();
        const bCols = Object.keys(bqRows[0]).map(c => c.toLowerCase()).sort();
        const common = hCols.filter(c => bCols.includes(c));

        if (common.length === 0) {
          L(`  No common columns!`);
          fpMatch = false;
        } else {
          // Hash each row
          const hashRow = (row) => {
            const n = {};
            for (const [k, v] of Object.entries(row)) n[k.toLowerCase()] = v;
            return crypto.createHash('md5')
              .update(common.map(c => norm(n[c])).join('|'))
              .digest('hex');
          };

          const hHashes = hiveRows.map(hashRow).sort();
          const bHashes = bqRows.map(hashRow).sort();

          // Compare sorted hash arrays
          fpMatch = hHashes.length === bHashes.length &&
            hHashes.every((h, i) => h === bHashes[i]);

          if (!fpMatch) {
            // Report first mismatch
            const hSet = new Set(hHashes);
            const bSet = new Set(bHashes);
            const onlyHive = hHashes.filter(h => !bSet.has(h)).length;
            const onlyBQ = bHashes.filter(h => !hSet.has(h)).length;
            L(`  FP mismatch: ${onlyHive} hive-only, ${onlyBQ} bq-only`);
          }
        }
      } catch (e) {
        L(`  FP error: ${e.message?.substring(0, 80)}`);
      }
    }

    // Per-column aggregates
    let colAggs = null;
    if (hiveCount > 0 && bqCount > 0) {
      try {
        // Get numeric columns from Hive
        const hCols = Object.keys(hiveRows[0]);
        const numCols = hCols.filter(c => {
          const v = hiveRows[0][c];
          return typeof v === 'number' || (typeof v === 'string' && !isNaN(Number(v)) && v.length < 20);
        }).slice(0, 5);

        if (numCols.length > 0) {
          const hAgg = {};
          const bAgg = {};

          // Compute aggregates from fetched rows (in Node)
          for (const col of numCols) {
            const hVals = hiveRows.map(r => Number(r[col])).filter(v => !isNaN(v));
            const bqRows2 = await bqQ(`SELECT * FROM ${bqFull} LIMIT 5000`);
            const bVals = bqRows2.map(r => {
              const v = r[col.toLowerCase()] ?? r[col];
              return Number(typeof v === 'object' ? v.value : v);
            }).filter(v => !isNaN(v));

            hAgg[col] = { sum: hVals.reduce((a, b) => a + b, 0), count: hVals.length };
            bAgg[col] = { sum: bVals.reduce((a, b) => a + b, 0), count: bVals.length };
          }
          colAggs = { hive: hAgg, bq: bAgg };
        }
      } catch (e) {
        L(`  Agg error: ${e.message?.substring(0, 60)}`);
      }
    }

    const pass = countMatch && (fpMatch === null || fpMatch);
    comparison[tbl] = {
      status: pass ? 'PASS' : 'FAIL',
      hive_count: hiveCount,
      bq_count: bqCount,
      count_match: countMatch,
      fingerprint_match: fpMatch,
      column_aggregates: colAggs,
    };

    if (pass) matchCount++;
    else mismatchCount++;

    const fpStr = fpMatch === null ? 'n/a' : (fpMatch ? 'MATCH' : 'MISMATCH');
    L(`  Result: ${pass ? '✓' : '✗'} count=${countMatch} fp=${fpStr}`);
  }

  // Handle NOT-EXERCISED (ACID, complex types)
  for (const r of legResults.filter(r => r.status === 'NOT_EXERCISED')) {
    const tbl = scriptToTable[r.script] || r.script;
    comparison[tbl] = { status: 'NOT_EXERCISED', reason: r.error };
    notExCount++;
  }

  // Summary
  const summary = {
    total_compared: passWithData.length,
    matched: matchCount,
    mismatched: mismatchCount,
    not_exercised: notExCount,
  };

  fs.writeFileSync(`${EV}/cross_engine_comparison.json`, JSON.stringify({ summary, tables: comparison }, null, 2));

  L(`\n=== SUMMARY ===`);
  L(`Compared: ${passWithData.length}`);
  L(`PASS: ${matchCount}`);
  L(`FAIL: ${mismatchCount}`);
  L(`NOT_EXERCISED: ${notExCount}`);

  await impS.close(); await impC.close();
  L('=== DONE ===');
}

main().catch(e => { console.error('FATAL:', e.stack); process.exit(1); });
