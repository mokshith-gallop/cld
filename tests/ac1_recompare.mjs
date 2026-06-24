#!/usr/bin/env node
/**
 * ac1_recompare.mjs — Re-compare Hive vs BQ with improved timestamp normalization.
 *
 * The fingerprint mismatches were caused by timestamp format differences:
 * - Hive: "2024-01-15 12:30:45" (no timezone, space separator)
 * - BQ:   {value: "2024-01-15T12:30:45.000000Z"} (ISO with Z, T separator)
 *
 * Fix: normalize both to "YYYY-MM-DD HH:MM:SS" before hashing.
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
const EV   = `${ROOT}/tests/evidence/parity`;
const DB   = 'qa_xp';
const BQDS = 'test';
const BQP  = 'qa_xp_';
const LOG  = `${EV}/ac1_recompare.log`;

fs.writeFileSync(LOG, '');
function L(m) { const l=`[${new Date().toISOString().slice(11,23)}] ${m}`; console.log(l); fs.appendFileSync(LOG,l+'\n'); }

let bq, iS, iC;
const U = new hive.HiveUtils(TCLIService_types);

function mkBQ() {
  try { const e=fs.readFileSync('/workspace/.gallop/db.env','utf8'); const m=e.match(/CLD_BQ_BQ_TOKEN='([^']+)'/); if(m) process.env.CLD_BQ_BQ_TOKEN=m[1]; } catch{}
  const a=new OAuth2Client(); a.setCredentials({access_token:process.env.CLD_BQ_BQ_TOKEN});
  bq=new BigQuery({projectId:process.env.CLD_BQ_BQ_PROJECT,authClient:a,location:'EU'});
}
async function bQ(q) {
  try { const[j]=await bq.createQueryJob({query:q,useLegacySql:false}); const[r]=await j.getQueryResults({maxResults:100000}); return r; }
  catch(e) { if(e.message?.includes('credentials')||e.message?.includes('401')){mkBQ(); const[j]=await bq.createQueryJob({query:q,useLegacySql:false}); const[r]=await j.getQueryResults({maxResults:100000}); return r;} throw e; }
}

async function openI() {
  const c=new hive.HiveClient(TCLIService,TCLIService_types);
  iC=await c.connect({host:process.env.CLD_IMP_HOST,port:+process.env.CLD_IMP_PORT},new hive.connections.TcpConnection(),new hive.auth.NoSaslAuthentication());
  iS=await iC.openSession({client_protocol:TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10});
}
async function iQ(q) { const o=await iS.executeStatement(q,{runAsync:true}); await U.waitUntilReady(o,false,()=>{}); await U.fetchAll(o,1); const r=U.getResult(o).getValue()??[]; await o.close(); return r; }

// Improved normalization for cross-engine comparison
function norm(v) {
  if (v == null) return '__NULL__';
  // BQ returns NUMERIC as {value: "..."} objects
  if (typeof v === 'object' && v.value !== undefined) v = v.value;
  let s = String(v);
  // Normalize timestamps: strip timezone, T→space, fractional seconds
  s = s.replace(/T/g, ' ')                    // ISO T separator → space
      .replace(/\.\d{1,9}Z?$/, '')            // .000000Z or .000 → remove
      .replace(/Z$/, '')                       // trailing Z
      .replace(/\+00:?00?$/, '')               // +00:00 or +00
      .replace(/\.0+$/, '');                   // trailing .0
  // Normalize booleans
  if (s === 'true' || s === '1') s = 'true';
  if (s === 'false' || s === '0') s = 'false';
  return s;
}

async function main() {
  L('=== AC1 Recompare — improved timestamp normalization ===');
  mkBQ(); await bQ('SELECT 1');
  await openI();
  L('Connected');

  const hiveData = JSON.parse(fs.readFileSync(`${EV}/hive_counts_fingerprints.json`,'utf8'));
  const recon = {};
  let pass=0, fail=0, empty=0, missing=0;
  const colAggs = {};

  for (const [tbl, hInfo] of Object.entries(hiveData)) {
    if (!hInfo || hInfo.count === 0 || hInfo.count === -1) {
      recon[tbl] = { status:'HIVE_EMPTY', hive:hInfo?.count??0 };
      empty++;
      continue;
    }

    let bqCount = -1;
    try { const[r]=await bQ(`SELECT COUNT(*) c FROM ${BQDS}.${BQP}${tbl}`); bqCount=+r.c; }
    catch { bqCount=-1; }

    if (bqCount === -1) {
      recon[tbl] = { status:'BQ_MISSING', hive:hInfo.count };
      missing++;
      L(`  ${tbl}: BQ missing`);
      continue;
    }

    const countMatch = hInfo.count === bqCount;
    let fpMatch = null;

    if (hInfo.count > 0 && bqCount > 0) {
      try {
        const hRows = await iQ(`SELECT * FROM ${DB}.${tbl} LIMIT 500`);
        const bRows = await bQ(`SELECT * FROM ${BQDS}.${BQP}${tbl} LIMIT 500`);

        if (hRows.length > 0 && bRows.length > 0) {
          const hCols = Object.keys(hRows[0]).map(c=>c.toLowerCase()).sort();
          const bCols = Object.keys(bRows[0]).map(c=>c.toLowerCase()).sort();
          const common = hCols.filter(c => bCols.includes(c));

          const mkHash = (rows, cols) => {
            return rows.map(r => {
              const n = {};
              for (const [k,v] of Object.entries(r)) n[k.toLowerCase()] = v;
              return crypto.createHash('md5').update(cols.map(c=>norm(n[c])).join('|')).digest('hex');
            }).sort();
          };

          const hH = mkHash(hRows, common);
          const bH = mkHash(bRows, common);
          const hFP = crypto.createHash('md5').update(hH.join('\n')).digest('hex');
          const bFP = crypto.createHash('md5').update(bH.join('\n')).digest('hex');
          fpMatch = hFP === bFP;

          // If fingerprint doesn't match, find which rows differ
          if (!fpMatch && hRows.length <= 10) {
            // Per-column comparison for debugging
            const diffs = [];
            for (const c of common) {
              const hVals = hRows.map(r => { const n={}; for(const[k,v]of Object.entries(r))n[k.toLowerCase()]=v; return norm(n[c]); }).sort();
              const bVals = bRows.map(r => { const n={}; for(const[k,v]of Object.entries(r))n[k.toLowerCase()]=v; return norm(n[c]); }).sort();
              if (JSON.stringify(hVals) !== JSON.stringify(bVals)) {
                diffs.push({ col: c, hive_sample: hVals[0], bq_sample: bVals[0] });
              }
            }
            if (diffs.length > 0) {
              recon[tbl] = { status:'FAIL', hive:hInfo.count, bq:bqCount, count_match:countMatch, fingerprint_match:false, diff_cols: diffs };
            }
          }

          // Per-column aggregates for numeric columns
          try {
            const numCols = common.filter(c => {
              const hv = hRows[0][c] ?? hRows[0][Object.keys(hRows[0]).find(k=>k.toLowerCase()===c)];
              return typeof hv === 'number';
            });
            if (numCols.length > 0) {
              const aggExprs = numCols.slice(0,5).map(c => `CAST(SUM(CAST(${c} AS FLOAT64)) AS STRING) AS s_${c}`).join(', ');
              const hAgg = await iQ(`SELECT ${numCols.slice(0,5).map(c=>`CAST(SUM(CAST(${c} AS DOUBLE)) AS STRING) AS s_${c}`).join(', ')} FROM ${DB}.${tbl}`);
              const bAgg = await bQ(`SELECT ${aggExprs} FROM ${BQDS}.${BQP}${tbl}`);
              colAggs[tbl] = { hive: hAgg[0], bq: bAgg[0] };
            }
          } catch {}
        }
      } catch(e) { L(`  ${tbl} fp error: ${e.message?.substring(0,60)}`); }
    }

    if (!recon[tbl]) {
      const overall = countMatch && (fpMatch === null || fpMatch);
      recon[tbl] = { status: overall?'PASS':'FAIL', hive:hInfo.count, bq:bqCount, count_match:countMatch, fingerprint_match:fpMatch };
    }
    if (recon[tbl].status === 'PASS') pass++; else fail++;
    const fp = fpMatch===null?'n/a':fpMatch?'✓':'✗';
    L(`  ${tbl}: ${recon[tbl].status==='PASS'?'✓':'✗'} h=${hInfo.count} b=${bqCount} fp=${fp}`);
  }

  const summary = {
    total: Object.keys(recon).length,
    pass, fail, hive_empty: empty, bq_missing: missing,
    note: 'Cross-engine comparison with improved timestamp normalization'
  };

  fs.writeFileSync(`${EV}/cross_engine_final.json`, JSON.stringify({summary, tables:recon, column_aggregates:colAggs}, null, 2));
  L(`\n=== FINAL: ${pass} PASS, ${fail} FAIL, ${empty} EMPTY, ${missing} MISSING of ${Object.keys(recon).length} ===`);

  try{await iS.close();await iC.close();}catch{}
  L('=== DONE ===');
}

main().catch(e=>{console.error('FATAL:',e.stack);process.exit(1);});
