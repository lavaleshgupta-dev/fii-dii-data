const express = require('express');
const cors = require('cors');
const path = require('path');
const http = require('http');
const axios = require('axios');
const cron = require('node-cron');
const { fetchAndProcessData, getDB } = require('./scripts/fetch_data');

const app = express();
const PORT = process.env.PORT || 3000;

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json());

// Static files
app.use(express.static(path.join(__dirname, '.'), {
    maxAge: '1h',
    setHeaders: (res, filePath) => {
        if (filePath.endsWith('sw.js') || filePath.endsWith('manifest.json')) {
            res.setHeader('Cache-Control', 'no-cache');
        }
    }
}));

// ── Routes ────────────────────────────────────────────────────────────────────

// Dashboard
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'fii_dii_india_flows_dashboard.html'));
});

// Latest FII/DII snapshot
app.get('/api/data', async (req, res) => {
    try {
        const db = await getDB();
        const data = await db.all('SELECT * FROM flows ORDER BY rowid DESC LIMIT 10');
        await db.close();
        if (!data || data.length === 0) return res.status(404).json({ error: 'No data found.' });
        const M = {Jan:0,Feb:1,Mar:2,Apr:3,May:4,Jun:5,Jul:6,Aug:7,Sep:8,Oct:9,Nov:10,Dec:11};
        data.sort((a,b) => {
            const pa = (a.date||'').split('-'), pb = (b.date||'').split('-');
            if(pa.length<3||pb.length<3) return 0;
            return new Date(pb[2],M[pb[1]],pb[0]) - new Date(pa[2],M[pa[1]],pa[0]);
        });
        res.json(data[0]);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Rolling history
app.get('/api/history', async (req, res) => {
    try {
        const db = await getDB();
        const history = await db.all('SELECT * FROM flows ORDER BY rowid DESC LIMIT 100');
        await db.close();
        const M = {Jan:0,Feb:1,Mar:2,Apr:3,May:4,Jun:5,Jul:6,Aug:7,Sep:8,Oct:9,Nov:10,Dec:11};
        history.sort((a,b) => {
            const pa = (a.date||'').split('-'), pb = (b.date||'').split('-');
            if(pa.length<3||pb.length<3) return 0;
            return new Date(pb[2],M[pb[1]],pb[0]) - new Date(pa[2],M[pa[1]],pa[0]);
        });
        res.json(history.slice(0,60));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Manual trigger
app.post('/api/refresh', async (req, res) => {
    try {
        const data = await fetchAndProcessData();
        res.json({ success: true, data });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// Status
app.get('/api/status', async (req, res) => {
    try {
        const db = await getDB();
        const logs = await db.all('SELECT * FROM fetch_logs ORDER BY id DESC LIMIT 5');
        await db.close();
        res.json({ status: 'ok', serverTime: new Date().toISOString(), recentLogs: logs });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Yahoo Finance proxy
app.get('/api/market', async (req, res) => {
    try {
        const fetchJSON = async (ticker) => {
            const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?interval=1d&range=1d`;
            const { data } = await axios.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 8000 });
            const m = data.chart.result[0].meta;
            const price = m.regularMarketPrice;
            const prev = m.previousClose || m.chartPreviousClose;
            return { price, change: price - prev, pct: ((price - prev) / prev) * 100 };
        };
        const [nifty, vix] = await Promise.all([fetchJSON('^NSEI'), fetchJSON('^INDIAVIX')]);
        res.json({ nifty, vix });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', ts: new Date().toISOString() });
});

// ── Scheduler ─────────────────────────────────────────────────────────────────
async function runFetchTask(label) {
    console.log(`[${new Date().toISOString()}] ${label} fetch starting…`);
    try {
        await fetchAndProcessData();
        console.log(`[${new Date().toISOString()}] ${label} fetch completed.`);
    } catch (err) {
        console.error(`[${new Date().toISOString()}] ${label} fetch failed:`, err.message);
    }
}

// Mon-Fri: Every 15 mins during market hours (9:15 AM - 3:30 PM IST = 3:45-10:00 UTC)
cron.schedule('*/15 3-10 * * 1-5', () => runFetchTask('Cron'));
cron.schedule('45 10 * * 1-5', () => runFetchTask('Post-market-1'));
cron.schedule('0 12 * * 1-5', () => runFetchTask('Post-market-2'));

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on port ${PORT}`);
});

module.exports = app;
