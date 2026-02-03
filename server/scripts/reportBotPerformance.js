// Simple performance report for bot decisions using decision_outcomes.
// Usage:
//   node server/scripts/reportBotPerformance.js --horizon 3600 --days 30
//   node server/scripts/reportBotPerformance.js --horizon 14400 --days 60

require('dotenv').config({ path: require('path').resolve(__dirname, '..', '.env') });
const { query } = require('../db');

function parseArgInt(flag, fallback) {
    const idx = process.argv.indexOf(flag);
    if (idx === -1) return fallback;
    const val = Number(process.argv[idx + 1]);
    return Number.isFinite(val) ? Math.trunc(val) : fallback;
}

async function main() {
    const horizonSec = parseArgInt('--horizon', 3600);
    const days = parseArgInt('--days', 30);
    const nowSec = Math.floor(Date.now() / 1000);
    const sinceSec = nowSec - days * 24 * 3600;

    const rows = await query(
        `SELECT
            o.horizon_sec,
            COUNT(*) AS n,
            AVG(o.forward_return) AS avg_forward_return,
            AVG(o.correct) AS hit_rate,
            d.prompt_version AS prompt_version,
            d.model AS model
         FROM decision_outcomes o
         JOIN bot_decisions d ON d.id = o.decision_id
         WHERE o.timestamp >= $1
           AND o.horizon_sec = $2
         GROUP BY o.horizon_sec, d.prompt_version, d.model
         ORDER BY n DESC`,
        [sinceSec, horizonSec]
    );

    console.log(`\nBot performance report (since ${days}d, horizon=${horizonSec}s)`);
    if (!rows.rows?.length) {
        console.log('No rows found.');
        return;
    }

    for (const r of rows.rows) {
        const n = Number(r.n);
        const avg = Number(r.avg_forward_return);
        const hit = Number(r.hit_rate);
        console.log(
            `- n=${n} avg=${Number.isFinite(avg) ? avg.toFixed(5) : 'N/A'} hit=${Number.isFinite(hit) ? (hit * 100).toFixed(1) + '%' : 'N/A'} ` +
            `prompt=${r.prompt_version || 'N/A'} model=${r.model || 'N/A'}`
        );
    }

    const overall = await query(
        `SELECT
            COUNT(*) AS n,
            AVG(forward_return) AS avg_forward_return,
            AVG(correct) AS hit_rate
         FROM decision_outcomes
         WHERE timestamp >= $1
           AND horizon_sec = $2`,
        [sinceSec, horizonSec]
    );

    const o = overall.rows?.[0];
    if (o) {
        const n = Number(o.n);
        const avg = Number(o.avg_forward_return);
        const hit = Number(o.hit_rate);
        console.log(`\nOverall: n=${n} avg=${Number.isFinite(avg) ? avg.toFixed(5) : 'N/A'} hit=${Number.isFinite(hit) ? (hit * 100).toFixed(1) + '%' : 'N/A'}`);
    }
}

main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
});
