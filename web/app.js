// ── Auth ──────────────────────────────────────────────────────────────────────
function authFetch(url, opts = {}) {
    const tok = sessionStorage.getItem('greeks-auth');
    if (tok) {
        opts.headers = { ...(opts.headers || {}), 'X-Auth-Token': tok };
    }
    return fetch(url, opts).then(res => {
        if (res.status === 401) {
            sessionStorage.removeItem('greeks-auth');
            window.location.href = '/login';
        }
        return res;
    });
}


// ── Header greeting with time-of-day ─────────────────────────────────────────
(function setHeaderGreeting() {
    const h = new Date().getHours();
    const greeting = h < 12 ? 'Good Morning' : h < 18 ? 'Good Afternoon' : 'Good Evening';
    const el = document.getElementById('greeting-text');
    if (el) el.textContent = `${greeting}.`;

    const dateEl = document.getElementById('greeting-date');
    if (dateEl) {
        dateEl.textContent = new Date().toLocaleDateString('en-US', {
            weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
        });
    }
})();

function doLogout() {
    // No-op — auth removed
}


// ── Welcome transition screen ────────────────────────────────────────────────
(function setupWelcome() {
    const ws = document.getElementById('welcome-screen');
    if (!ws) return;

    // Set user name
    const email = localStorage.getItem('greeks-user') || '';
    const name = email ? email.split('@')[0].replace(/[._]/g, ' ')
        .replace(/\b\w/g, c => c.toUpperCase()) : 'Member';
    const nameEl = document.getElementById('ws-name');
    if (nameEl) nameEl.textContent = name;

    // Animate progress bar
    const bar = document.getElementById('ws-progress-bar');
    const status = document.getElementById('ws-status');
    const steps = [
        [15, 'DECRYPTING MARKET STREAM...'],
        [35, 'LOADING OPTIONS CHAIN DATA...'],
        [55, 'COMPUTING GREEK EXPOSURES...'],
        [75, 'BUILDING HEATMAP MATRICES...'],
        [90, 'INITIALIZING DASHBOARD...'],
    ];
    let step = 0;
    const iv = setInterval(() => {
        if (step < steps.length) {
            if (bar) bar.style.width = steps[step][0] + '%';
            // Typewriter effect for status text
            if (status) {
                const txt = steps[step][1];
                status.textContent = '';
                let ci = 0;
                const typeIt = () => {
                    if (ci < txt.length) { status.textContent += txt[ci]; ci++; setTimeout(typeIt, 18); }
                };
                typeIt();
            }
            step++;
        }
    }, 600);

    // Dismiss welcome screen — called after first data load
    window._dismissWelcome = function () {
        clearInterval(iv);
        if (bar) bar.style.width = '100%';
        if (status) status.textContent = 'DASHBOARD READY';
        setTimeout(() => {
            ws.classList.add('ws-hide');
            setTimeout(() => { ws.style.display = 'none'; }, 800);
        }, 400);
    };

    // Fallback: auto-dismiss after 30s in case data never comes
    setTimeout(() => { if (window._dismissWelcome) window._dismissWelcome(); }, 30000);
})();

// ── Sidebar navigation + user footer ─────────────────────────────────────────
(function setupSidebar() {
    // Tab switching
    const items = document.querySelectorAll('.sb-item[data-tab]');
    items.forEach(item => {
        item.addEventListener('click', () => {
            items.forEach(i => i.classList.remove('active'));
            item.classList.add('active');
            // Trigger same tab switching logic
            const tabId = item.dataset.tab;
            document.querySelectorAll('.tab-content').forEach(tc => {
                tc.classList.remove('active');
            });
            const target = document.getElementById('tab-' + tabId);
            if (target) target.classList.add('active');
            // Also update old tab buttons if any code references them
            document.querySelectorAll('.tab[data-tab]').forEach(t => {
                t.classList.toggle('active', t.dataset.tab === tabId);
            });
            // Lazy-load data for tabs that fetch on demand
            if (tabId === 'anomaly' && typeof updateAnomaly === 'function' && !charts?.['chart-anomaly']) updateAnomaly();
            if (tabId === 'anomaly' && typeof loadHIRO === 'function') loadHIRO();
            if (tabId === 'anomaly' && typeof loadTopology === 'function' && !topoLoaded) { topoLoaded = true; loadTopology(); }
            if (tabId === 'anomaly' && typeof loadEntropy === 'function' && !entropyLoaded) { entropyLoaded = true; loadEntropy(); }
            if (tabId === 'vol' && typeof updateVolatility === 'function') updateVolatility();
            if (tabId === 'macro' && typeof updateMacro === 'function') updateMacro();
            if (tabId === 'regime' && typeof updateRegime === 'function' && !charts?.['chart-regime-price']) updateRegime();
            if (tabId === 'settings' && typeof loadSettings === 'function') loadSettings();
            if (tabId === 'regime' && typeof updateRegime === 'function' && !charts?.['chart-regime-price']) updateRegime();
            if (tabId === 'oi' && typeof loadOI365 === 'function') loadOI365();
            if (tabId === 'prob' && typeof renderProb === 'function') renderProb();
            if (tabId === 'inference' && typeof loadInference === 'function') loadInference();
            if (tabId === 'crashrisk' && typeof loadCrashRisk === 'function') loadCrashRisk();
            if (tabId === 'flow' && typeof loadFlow === 'function') loadFlow();
        });
    });

    // User info in sidebar footer
    const email = localStorage.getItem('greeks-user') || '';
    if (email) {
        const name = email.split('@')[0].replace(/[._]/g, ' ')
            .replace(/\b\w/g, c => c.toUpperCase());
        const initials = name.split(' ').map(w => w[0]).join('').slice(0, 2);
        const nameEl = document.getElementById('sb-user-name');
        const avatarEl = document.getElementById('sb-avatar');
        if (nameEl) nameEl.textContent = name;
        if (avatarEl) avatarEl.textContent = initials;
    }

    // Sign out removed — no auth

})();

// â”€â”€ Config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const API_URL = "/api/data";
const ANOMALY_URL = "/api/anomalies";
const SETTINGS_URL = "/api/settings";
// Timer handles — declared at top to avoid TDZ errors when update() boots
let _refreshTimer = null;
let _barTimer = null;
let _barMs = 30000;

const VOL_URL = "/api/volatility";   // must be at top â€” used before line 1037
const HIRO_URL = "/api/hiro";
const TOPO_URL = "/api/topology";
const ENTROPY_URL = "/api/entropy";
const REFRESH_MS = 30_000;

let topoLoaded = false;
let entropyLoaded = false;


const GREEN = "#2ecc8a";
const RED = "#e8435a";
const CYAN = "#38bdf8";
const YELLOW = "#f5c542";
const PURPLE = "#a78bfa";
const BLUE = "#60a5fa";
const ORANGE = "#fb923c";

// Dynamic theme-aware color helpers — read from CSS vars so they update per theme
function _cssVar(v, fallback) {
    return getComputedStyle(document.documentElement).getPropertyValue(v).trim() || fallback;
}
function themeAccent() { return _cssVar("--accent", "#7c5af7"); }
function themeAccent2() { return _cssVar("--accent2", "#5435c2"); }
function themeBg1() { return _cssVar("--bg1", "#030305"); }
function themeBg2() { return _cssVar("--bg2", "#06070a"); }
function themeBg3() { return _cssVar("--bg3", "#0a0b10"); }
function themeBorder() { return _cssVar("--border", "rgba(255,255,255,.07)"); }
const GRID = "rgba(255,255,255,0.03)";
const TEXT = "#6b7a99";

// â”€â”€ Color system â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function hexToRgb(hex) {
    const m = hex.replace("#", "").match(/.{2}/g);
    return m ? m.map(v => parseInt(v, 16)) : [46, 180, 110];
}

// Multi-stop colormaps â€” each array is [r,g,b] stops from most-negative to most-positive
const CMAPS = {
    // OI: ice/steel blue (neutral — OI has no directional bias)
    viridis: [
        [8, 12, 30],     // -1.0  near-black
        [15, 40, 90],    // -0.66 deep navy
        [20, 80, 150],   // -0.33 steel blue
        [12, 14, 28],    //  0.0  dark neutral
        [0, 110, 160],   // +0.33 ocean blue
        [0, 170, 200],   // +0.66 ice cyan
        [160, 240, 255], // +1.0  bright ice
    ],
    // GEX semantic: crimson (destabilizing) → near-black → teal-green (stabilizing)
    gex: [
        [200, 30, 55],   // -1.0  vivid crimson    (max destabilizing gamma)
        [130, 15, 35],   // -0.66 dark crimson
        [55, 8, 20],   // -0.33 dim maroon
        [8, 10, 18],   //  0.0  near-black neutral
        [0, 100, 80],   // +0.33 dark teal
        [0, 175, 130],  // +0.66 medium teal-green
        [0, 230, 160],  // +1.0  vivid teal-green (max stabilizing gamma)
    ],
    // GEX alt: black→purple→crimson→orange→yellow (heat map scale)
    turbo: [
        [8, 4, 25],      //  min  near-black
        [55, 0, 120],    // 0.17 deep purple
        [130, 0, 180],   // 0.33 violet
        [210, 30, 100],  // 0.50 crimson-magenta
        [240, 90, 20],   // 0.67 orange
        [255, 190, 0],   // 0.83 amber
        [255, 255, 80],  //  max  electric yellow
    ],
    // DEX: deep crimson→dark→bright emerald (bearish→neutral→bullish delta)
    dex: [
        [200, 15, 35],   // -1.0  deep crimson
        [140, 20, 45],   // -0.66 dark red
        [65, 15, 30],    // -0.33 dim maroon
        [8, 10, 22],     //  0.0  near-black neutral
        [10, 80, 50],    // +0.33 dark emerald
        [18, 155, 75],   // +0.66 medium emerald
        [30, 215, 100],  // +1.0  bright emerald
    ],
    // VEX: navy→indigo→violet→lavender (vega exposure)
    vex: [
        [5, 5, 60],      // -1.0  deep navy
        [30, 10, 110],   // -0.66 indigo
        [70, 20, 150],   // -0.33 violet
        [10, 10, 25],    //  0.0  near-black
        [100, 30, 200],  // +0.33 purple
        [160, 60, 255],  // +0.66 bright violet
        [200, 130, 255], // +1.0  lavender
    ],
    // TEX: forest green→dark→bright gold (theta = time-value decay)
    tex: [
        [8, 30, 8],      // -1.0  dark forest (MMs collecting premium)
        [20, 90, 20],    // -0.66 deep green
        [40, 140, 30],   // -0.33 medium green
        [10, 12, 22],    //  0.0  near-black
        [100, 70, 0],    // +0.33 dark amber
        [200, 140, 0],   // +0.66 gold
        [255, 215, 30],  // +1.0  bright gold
    ],
    // VannaEX: dark ocean→teal→bright cyan (delta sensitivity to IV)
    vannex: [
        [0, 50, 80],     // -1.0  dark ocean
        [0, 100, 130],   // -0.66 deep teal
        [0, 160, 170],   // -0.33 teal
        [8, 10, 22],     //  0.0  near-black
        [20, 190, 180],  // +0.33 cyan-teal
        [80, 230, 215],  // +0.66 bright cyan
        [180, 255, 250], // +1.0  ice-white
    ],
    // CharmEX: deep plum→magenta→hot pink→pale rose (delta decay)
    cex: [
        [80, 0, 60],     // -1.0  deep plum
        [140, 10, 90],   // -0.66 dark magenta
        [190, 30, 110],  // -0.33 magenta-rose
        [10, 8, 22],     //  0.0  near-black
        [220, 50, 130],  // +0.33 hot pink
        [245, 100, 170], // +0.66 bright pink
        [255, 180, 220], // +1.0  pale rose
    ],
    // ── Extra palettes for picker ────────────────────────────────────────────
    solar: [
        [10, 0, 80], [80, 0, 130], [180, 20, 80],
        [12, 10, 28], [200, 60, 10], [240, 150, 0], [255, 230, 60],
    ],
    ice: [
        [0, 30, 80], [0, 80, 150], [0, 150, 210],
        [10, 12, 30], [0, 200, 230], [100, 230, 255], [200, 250, 255],
    ],
    lava: [
        [20, 0, 0], [80, 0, 0], [160, 10, 0],
        [12, 8, 8], [200, 60, 0], [255, 140, 20], [255, 240, 80],
    ],
    neon: [
        [60, 0, 120], [120, 0, 200], [200, 0, 200],
        [10, 10, 20], [0, 180, 200], [0, 240, 180], [180, 255, 220],
    ],
    copper: [
        [20, 8, 0], [80, 30, 5], [150, 70, 20],
        [12, 10, 8], [180, 100, 40], [220, 160, 80], [255, 220, 140],
    ],
    frost: [
        [5, 10, 60], [20, 60, 120], [60, 120, 180],
        [10, 12, 30], [120, 180, 220], [180, 220, 255], [230, 245, 255],
    ],
    forest: [
        [5, 30, 5], [20, 80, 15], [50, 140, 30],
        [10, 12, 10], [90, 160, 40], [160, 210, 60], [220, 255, 120],
    ],
    cherry: [
        [60, 0, 30], [130, 0, 60], [200, 10, 80],
        [12, 8, 18], [220, 50, 100], [255, 110, 150], [255, 200, 220],
    ],

    // ─── Extra palettes: each is a distinct visual archetype ───────────────────
    fire: [       // black → red → orange → gold → white  (pure flame)
        [4, 0, 0], [180, 5, 0], [255, 80, 0], [255, 190, 0], [255, 255, 200],
    ],
    rdbu: [       // deep red → white → deep blue  (classic quant diverging)
        [160, 20, 30], [220, 120, 100], [250, 240, 240],
        [200, 220, 255], [30, 60, 180], [5, 15, 90],
    ],
    twilight: [   // midnight navy → violet → pink → pale (night sky)
        [5, 5, 30], [40, 10, 100], [110, 20, 160],
        [200, 60, 160], [245, 150, 200], [255, 230, 245],
    ],
    lime: [       // soot black → dark green → electric lime  (terminal/matrix)
        [4, 4, 4], [0, 40, 10], [0, 100, 20],
        [0, 180, 50], [100, 240, 80], [220, 255, 150],
    ],
    spectral: [   // full rainbow: blue → cyan → green → yellow → red
        [30, 30, 200], [0, 170, 220], [0, 220, 130],
        [200, 230, 0], [255, 160, 0], [220, 20, 20],
    ],
    sand: [       // charcoal → brown → tan → pale cream  (earth/dune)
        [20, 10, 5], [80, 40, 15], [150, 100, 50],
        [200, 155, 90], [230, 200, 150], [255, 245, 220],
    ],
    cyberpunk: [  // black → electric violet → neon cyan  (synthwave)
        [4, 0, 12], [80, 0, 180], [160, 0, 255],
        [0, 200, 240], [0, 255, 220], [180, 255, 250],
    ],
    sunset: [     // deep navy → indigo → rose → amber → gold  (horizon)
        [5, 8, 40], [50, 20, 120], [160, 40, 120],
        [230, 90, 60], [255, 175, 30], [255, 240, 120],
    ],
    mono: [       // pure greyscale: black → charcoal → grey → white
        [8, 8, 8], [55, 55, 55], [120, 120, 120],
        [180, 180, 180], [225, 225, 225], [255, 255, 255],
    ],
    acid: [       // void black → acid green → bright yellow-white  (brutal contrast)
        [4, 4, 4], [0, 60, 0], [20, 160, 0],
        [140, 230, 0], [220, 255, 0], [255, 255, 200],
    ],
    // plasma / blueorange / inferno: legacy fallbacks
    plasma: [
        [13, 8, 135], [100, 10, 180], [185, 40, 130],
        [20, 20, 35], [220, 80, 50], [246, 180, 20], [240, 249, 33],
    ],
    blueorange: [
        [0, 80, 200], [60, 130, 210], [20, 20, 35], [220, 140, 40], [255, 200, 0],
    ],
    inferno: [
        [8, 0, 80], [120, 10, 80], [200, 30, 30],
        [20, 20, 35], [215, 90, 5], [248, 170, 10], [255, 240, 80],
    ],
};

// Map each panel to a unique colormap
const PANEL_CMAP = {
    gex: "turbo",   // black→purple→red→orange→yellow (heat map)
    dex: "dex",     // crimson→dark→emerald (bearish/bullish delta)
    vex: "vex",     // navy→violet→lavender (vega exposure)
    tex: "tex",     // green→dark→gold (theta decay)
    vannex: "vannex",  // ocean→teal→cyan (vanna)
    cex: "cex",     // plum→magenta→rose (charm)
    oi: "viridis", // ice/steel blue (neutral OI)
};

// Sign-preserving sqrt normalization â€” compresses huge outliers, spreads mid-range contrast
// Per-colormap normalization power (lower = more aggressive contrast for fat-tailed data)
const CMAP_POWER = {
    vannex: 0.30,   // vanna: very fat-tailed — boost mid-range contrast strongly
    cex: 0.30,   // charm: same distribution
    turbo: 0.40,   // GEX: moderately aggressive
    dex: 0.45,   // DEX: slight compression
    // others use default 0.50 (sqrt)
};

// Sign-preserving power normalization — compresses huge outliers, spreads mid-range contrast
function normT(value, maxAbs, power) {
    if (maxAbs === 0) return 0;
    const p = power || 0.50;
    const raw = Math.max(-1, Math.min(1, value / maxAbs));
    return Math.sign(raw) * Math.pow(Math.abs(raw), p);
}

// Interpolate through a multi-stop colormap for t in [-1, +1]
function cmapColor(t, stops) {
    // Map t from [-1,1] to [0, stops.length-1]
    const n = stops.length;
    const pos = (t + 1) / 2 * (n - 1);   // 0 â€¦ n-1
    const lo = Math.floor(pos);
    const hi = Math.min(lo + 1, n - 1);
    const f = pos - lo;
    const [r0, g0, b0] = stops[lo];
    const [r1, g1, b1] = stops[hi];
    return `rgb(${Math.round(r0 + (r1 - r0) * f)},${Math.round(g0 + (g1 - g0) * f)},${Math.round(b0 + (b1 - b0) * f)})`;
}

// Main cell colorizer (seqMode=true stretches positive-only data to full colormap range)
function cellColor(value, maxAbs, cmapKey, seqMode) {
    let t = normT(value, maxAbs, CMAP_POWER[cmapKey]);
    // Sequential mode: all-positive data â†’ stretch [0,+1] â†’ [-1,+1] to use full colormap
    if (seqMode && t >= 0) t = -1 + 2 * t;
    // Simple 5-stop: reads CSS vars + auto-computes 2 intermediate blends for smooth gradient
    if (cmapKey === "simple") {
        const style = getComputedStyle(document.documentElement);
        const neg = hexToRgb(style.getPropertyValue("--hm-neg").trim() || "#3b1278");
        const zero = hexToRgb(style.getPropertyValue("--hm-neu").trim() || "#20b79e");
        const pos = hexToRgb(style.getPropertyValue("--hm-pos").trim() || "#f5ff30");
        // Auto-blend 2 intermediate stops: gives viridis-like smooth transition
        const midNeg = neg.map((v, i) => Math.round(v * 0.5 + zero[i] * 0.5));
        const midPos = zero.map((v, i) => Math.round(v * 0.5 + pos[i] * 0.5));
        return cmapColor(t, [neg, midNeg, zero, midPos, pos]);
    }
    // Legacy gex_custom
    if (cmapKey === "gex_custom") {
        const style = getComputedStyle(document.documentElement);
        const pos = hexToRgb(style.getPropertyValue("--hm-pos").trim() || "#f5ff30");
        const neg = hexToRgb(style.getPropertyValue("--hm-neg").trim() || "#3b1278");
        const stops = [neg, [40, 40, 60], [20, 20, 35], [40, 40, 60], pos];
        return cmapColor(t, stops);
    }
    return cmapColor(t, CMAPS[cmapKey] || CMAPS.viridis);
}

// Build the legend gradient bar for a given cmap
function makeLegendGradient(cmapKey) {
    if (cmapKey === "simple") {
        const style = getComputedStyle(document.documentElement);
        const neg = style.getPropertyValue("--hm-neg").trim() || "#3b1278";
        const zero = style.getPropertyValue("--hm-neu").trim() || "#20b79e";
        const pos = style.getPropertyValue("--hm-pos").trim() || "#f5ff30";
        return `linear-gradient(to right,${neg},${zero},${pos})`;
    }
    const stops = CMAPS[cmapKey] || CMAPS.viridis;
    const colors = stops.map(([r, g, b]) => `rgb(${r},${g},${b})`).join(",");
    return `linear-gradient(to right,${colors})`;
}

// Luminance-based text colour for readability on colored cells
function textColor(bgRgb) {
    const m = bgRgb.match(/\d+/g);
    if (!m) return "#aaaaaa";
    const [r, g, b] = m.map(Number);
    const lum = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
    return lum > 0.45 ? "#111" : "#dde";
}

function sortedEntries(obj) {
    return Object.entries(obj).sort((a, b) => parseFloat(a[0]) - parseFloat(b[0]));
}

// â”€â”€ Formatters & state â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function fmt(v) {
    const a = Math.abs(v);
    if (a >= 1e12) return (v / 1e12).toFixed(1) + "T";
    if (a >= 1e9) return (v / 1e9).toFixed(1) + "B";
    if (a >= 1e6) return (v / 1e6).toFixed(1) + "M";
    if (a >= 1e3) return (v / 1e3).toFixed(0) + "K";
    return v.toFixed(0);
}

function fmtTime(iso) {
    return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

const charts = {};

// â”€â”€ Heatmap Builder â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function buildHeatmap(containerId, hmData, cmapKey, spot) {
    const container = document.getElementById(containerId);
    if (!container) return;

    const { strikes, expirations, rows } = hmData;
    if (!strikes || !strikes.length) { container.innerHTML = "<p style='color:#445566;padding:12px'>No data</p>"; return; }

    // Find max absolute value for color scaling (use 95th-percentile to resist outliers further)
    const allVals = [];
    for (const row of rows) for (const cell of row.cells) allVals.push(Math.abs(cell));
    allVals.sort((a, b) => a - b);
    // p99 cap: high enough that small cells stay near teal, only truly large ones go full yellow/purple
    const p99idx = Math.floor(allVals.length * 0.99);
    const maxAbs = allVals[p99idx] || allVals[allVals.length - 1] || 1;

    // Detect sequential (all-positive) data â€” will stretch full colormap range
    const seqMode = rows.every(r => r.cells.every(c => c >= 0));

    // Legend
    const legendId = containerId.replace("hm-", "") + "-legend";
    const legendEl = document.getElementById(legendId);
    if (legendEl) {
        const grad = cmapKey === "gex_custom"
            ? (() => {
                const style = getComputedStyle(document.documentElement);
                const posH = style.getPropertyValue("--hm-pos").trim() || "#f5ff30";
                const negH = style.getPropertyValue("--hm-neg").trim() || "#3b1278";
                return `linear-gradient(to right,${negH},#141423,${posH})`;
            })()
            : makeLegendGradient(cmapKey);
        const stops = CMAPS[cmapKey] || CMAPS.viridis;
        const negColor = `rgb(${stops[0].join(",")})`;
        const posColor = `rgb(${stops[stops.length - 1].join(",")})`;
        legendEl.innerHTML = `
          <div style="display:flex;align-items:center;gap:5px">
            <span style="font-size:.58rem;color:${negColor};font-weight:600">âˆ’</span>
            <div style="width:70px;height:8px;border-radius:4px;background:${grad};border:1px solid rgba(255,255,255,.08)"></div>
            <span style="font-size:.58rem;color:${posColor};font-weight:600">+</span>
          </div>`;
    }

    // Limit expirations shown to keep table readable (first 5)
    const maxExps = 5;
    const visExps = expirations.slice(0, maxExps);

    // Build table
    let html = '<table class="hm-table"><thead><tr>';
    html += '<th class="strike-col">Strike</th>';
    for (const e of visExps) {
        const isObj = typeof e === 'object' && e !== null;
        const dte = isObj ? e.dte : null;
        // Server provides label like "Mar 7 '25" — use directly, no re-parsing needed
        const label = isObj ? String(e.label) : String(e);
        // Shorten: strip the year part if present (e.g. "Mar 7 '25" → "Mar 7")
        const shortDate = label.replace(/\s+'?\d{2,4}$/, '');
        const dteTag = dte != null ? `<div class="hm-th-dte">${dte}d</div>` : '';
        html += `<th title="${label}">${dteTag}<div class="hm-th-date">${shortDate}</div></th>`;
    }
    html += '</tr></thead><tbody>';

    const nearestStrikeIdx = rows.reduce((best, row, i) => {
        return Math.abs(row.strike - spot) < Math.abs(rows[best].strike - spot) ? i : best;
    }, 0);

    for (let ri = 0; ri < rows.length; ri++) {
        const row = rows[ri];
        const isSpot = ri === nearestStrikeIdx;
        html += `<tr class="${isSpot ? 'spot-row' : ''}">`;
        html += `<td class="strike-cell">$${row.strike.toFixed(0)}</td>`;
        for (let ci = 0; ci < visExps.length; ci++) {
            const val = row.cells[ci] ?? 0;
            const bg = cellColor(val, maxAbs, cmapKey, seqMode);
            const fg = textColor(bg);
            html += `<td style="background:${bg};color:${fg}" title="${val.toFixed(2)}">${fmt(val)}</td>`;
        }
        html += '</tr>';
    }
    html += '</tbody></table>';
    container.innerHTML = html;
}

// â”€â”€ GEX Bar chart helper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function buildGexBar(hmData, spot) {
    // Convert GEX bar data to heatmap-like single-column display
    const entries = sortedEntries(hmData);
    const labels = entries.map(([k]) => "$" + parseFloat(k).toFixed(0));
    const values = entries.map(([, v]) => v);
    const colors = values.map(v => v >= 0 ? "rgba(34,197,94,.82)" : "rgba(239,68,68,.82)");

    const ctx = document.getElementById("hm-gex");
    if (!ctx) return;
    // Use canvas chart instead of heatmap table for GEX (it's net, not per-expiry)
    ctx.innerHTML = '<canvas id="canvas-gex"></canvas>';
    const canvas = document.getElementById("canvas-gex");
    canvas.style.height = "460px";
    if (charts["canvas-gex"]) { charts["canvas-gex"].destroy(); }
    charts["canvas-gex"] = new Chart(canvas.getContext("2d"), {
        type: "bar",
        data: {
            labels, datasets: [{
                data: values, backgroundColor: colors, borderWidth: 0, borderRadius: 5, borderSkipped: false, barPercentage: 0.75, categoryPercentage: 0.9
            }]
        },
        options: {
            indexAxis: "y", responsive: true, maintainAspectRatio: false,
            animation: { duration: 300 },
            plugins: {
                legend: { display: false }, tooltip: {
                    callbacks: { label: c => fmt(c.raw) },
                    backgroundColor: "#111", titleColor: CYAN, bodyColor: TEXT
                }
            },
            scales: {
                x: { ticks: { color: TEXT, font: { size: 9 }, callback: v => fmt(v) }, grid: { color: GRID }, border: { color: GRID } },
                y: { ticks: { color: TEXT, font: { family: "JetBrains Mono", size: 9 } }, grid: { color: GRID }, border: { color: GRID } },
            }
        }
    });
}

// â”€â”€ OI Bar Chart â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// ── makeStrikePlugins: shared Chart.js plugin factory for all strike bar charts ──
// Creates: ATM highlight band + spot-line marker + value labels on bars
function makeStrikePlugins({ spot, labels }) {
    const nearestIdx = labels.reduce((best, lbl, i) => {
        const s = parseFloat(lbl.replace('$', ''));
        const bS = parseFloat(labels[best].replace('$', ''));
        return Math.abs(s - spot) < Math.abs(bS - spot) ? i : best;
    }, 0);

    // ATM band highlight behind the nearest-to-spot bar
    const atmBandPlugin = {
        id: 'atmBand',
        beforeDraw(chart) {
            const { ctx, chartArea: ca, scales: { y } } = chart;
            if (!ca) return;
            const barH = y.getPixelForValue(nearestIdx + 0.5) - y.getPixelForValue(nearestIdx - 0.5);
            const cy = y.getPixelForValue(nearestIdx);
            ctx.save();
            const accent = getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#7c5af7';
            ctx.fillStyle = accent.includes('rgb') ? accent.replace(')', ',.07)').replace('rgb', 'rgba') : 'rgba(124,90,247,.07)';
            ctx.fillRect(ca.left, cy - Math.abs(barH) / 2, ca.right - ca.left, Math.abs(barH));
            ctx.restore();
        }
    };

    // Spot price vertical line
    const spotLinePlugin = {
        id: 'spotLine',
        afterDraw(chart) {
            const { ctx, chartArea: ca, scales: { x } } = chart;
            if (!ca || !x) return;
            ctx.save();
            const xPx = x.getPixelForValue(0);
            const accent = getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#7c5af7';
            ctx.strokeStyle = accent;
            ctx.lineWidth = 1.5;
            ctx.setLineDash([4, 3]);
            ctx.beginPath();
            ctx.moveTo(xPx, ca.top);
            ctx.lineTo(xPx, ca.bottom);
            ctx.stroke();
            ctx.restore();
        }
    };

    // Value labels on bars
    const valueLabelPlugin = {
        id: 'valueLabels',
        afterDatasetsDraw(chart) {
            const { ctx, scales: { x } } = chart;
            chart.data.datasets.forEach((ds, di) => {
                const meta = chart.getDatasetMeta(di);
                meta.data.forEach((bar, idx) => {
                    const val = ds.data[idx];
                    if (!val || Math.abs(val) < 1) return;
                    const label = Math.abs(val) >= 1e9 ? (val / 1e9).toFixed(1) + 'B'
                        : Math.abs(val) >= 1e6 ? (val / 1e6).toFixed(1) + 'M'
                            : Math.abs(val) >= 1e3 ? (val / 1e3).toFixed(1) + 'K'
                                : val.toFixed(0);
                    ctx.save();
                    ctx.font = '600 11px JetBrains Mono, monospace';
                    ctx.fillStyle = 'rgba(200,215,240,.75)';
                    ctx.textBaseline = 'middle';
                    const xPos = val >= 0 ? x.getPixelForValue(val) + 4 : x.getPixelForValue(val) - 4;
                    ctx.textAlign = val >= 0 ? 'left' : 'right';
                    ctx.fillText(label, xPos, bar.y);
                    ctx.restore();
                });
            });
        }
    };

    return [atmBandPlugin, spotLinePlugin, valueLabelPlugin];
}

function buildOI(oiBar, spot) {
    const entries = sortedEntries(oiBar);
    // Sort descending by strike (highest strike at top, matching heatmaps)
    entries.reverse();
    const labels = entries.map(([k]) => "$" + parseFloat(k).toFixed(0));
    const calls = entries.map(([, v]) => v.calls || 0);
    const puts = entries.map(([, v]) => -(v.puts || 0));  // negative = extends left

    // Put/Call ratio badge
    const totalCalls = calls.reduce((a, b) => a + b, 0);
    const totalPuts = entries.reduce((a, [, v]) => a + (v.puts || 0), 0);
    const pcRatio = totalCalls > 0 ? (totalPuts / totalCalls).toFixed(2) : "â€”";
    const pcEl = document.getElementById("oi-pc-ratio");
    if (pcEl) {
        pcEl.textContent = "P/C " + pcRatio;
        pcEl.style.color = pcRatio > 1 ? "rgba(255,40,90,.9)" : "rgba(0,200,255,.9)";
    }

    // Dynamic height: ~20px per strike
    const rowH = 20;
    const totalH = Math.max(500, entries.length * rowH + 80);
    const wrap = document.getElementById("chart-oi")?.parentElement;
    if (wrap) wrap.style.height = totalH + "px";

    if (charts["chart-oi"]) charts["chart-oi"].destroy();
    const canvas = document.getElementById("chart-oi");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");

    // â”€â”€ Glow plugin: draws a blurred shadow pass before each dataset â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const glowPlugin = {
        id: "barGlow",
        beforeDatasetsDraw(chart) {
            chart.ctx.save();
        },
        beforeDatasetDraw(chart, args) {
            const colors = ["rgba(0,210,255,.9)", "rgba(255,40,90,.9)"];  // Calls=cyan, Puts=crimson
            chart.ctx.shadowColor = colors[args.index] || "transparent";
            chart.ctx.shadowBlur = 14;
        },
        afterDatasetDraw(chart) {
            chart.ctx.shadowBlur = 0;
            chart.ctx.shadowColor = "transparent";
        },
        afterDatasetsDraw(chart) {
            chart.ctx.restore();
        },
    };

    // â”€â”€ Spot line plugin â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const spotPlugin = {
        id: "spotLine",
        afterDraw(chart) {
            if (!spot) return;
            const nearIdx = labels.reduce((b, lbl, i) =>
                Math.abs(parseFloat(lbl.replace("$", "")) - spot) <
                    Math.abs(parseFloat(labels[b].replace("$", "")) - spot) ? i : b, 0);
            const meta = chart.getDatasetMeta(0);
            if (!meta.data[nearIdx]) return;
            const y = meta.data[nearIdx].y;
            const { left, right } = chart.chartArea;
            const c2 = chart.ctx;
            c2.save();
            c2.shadowColor = "rgba(96,165,250,.6)";
            c2.shadowBlur = 8;
            c2.setLineDash([4, 3]);
            c2.strokeStyle = "rgba(96,165,250,.8)";
            c2.lineWidth = 1.5;
            c2.beginPath(); c2.moveTo(left, y); c2.lineTo(right, y); c2.stroke();
            c2.shadowBlur = 0;
            c2.fillStyle = "rgba(96,165,250,.9)";
            c2.font = "bold 11px 'JetBrains Mono', monospace";
            c2.fillText("ATM $" + spot.toFixed(0), left + 4, y - 4);
            c2.restore();
        }
    };

    charts["chart-oi"] = new Chart(ctx, {
        type: "bar",
        data: {
            labels,
            datasets: [
                {
                    label: "Calls",
                    data: calls,
                    backgroundColor: (ctx2) => {
                        const { ctx: c, chartArea } = ctx2.chart;
                        if (!chartArea) return 'rgba(0,200,255,.82)';
                        const g = c.createLinearGradient(chartArea.left, 0, chartArea.right, 0);
                        g.addColorStop(0, 'rgba(0,200,255,.05)');
                        g.addColorStop(0.5, 'rgba(0,200,255,.55)');
                        g.addColorStop(1, 'rgba(56,220,255,.92)');
                        return g;
                    },
                    hoverBackgroundColor: "rgba(56,220,255,1)",
                    borderRadius: 6,
                    borderSkipped: false,
                    borderWidth: 0,
                    barPercentage: 0.6,
                    categoryPercentage: 0.92,
                    order: 1,
                },
                {
                    label: "Puts",
                    data: puts,
                    backgroundColor: (ctx2) => {
                        const { ctx: c, chartArea } = ctx2.chart;
                        if (!chartArea) return 'rgba(255,40,90,.82)';
                        const g = c.createLinearGradient(chartArea.left, 0, chartArea.right, 0);
                        g.addColorStop(0, 'rgba(255,40,90,.92)');
                        g.addColorStop(0.5, 'rgba(255,40,90,.55)');
                        g.addColorStop(1, 'rgba(255,40,90,.05)');
                        return g;
                    },
                    hoverBackgroundColor: "rgba(255,60,100,1)",
                    borderRadius: 6,
                    borderSkipped: false,
                    borderWidth: 0,
                    barPercentage: 0.6,
                    categoryPercentage: 0.92,
                    order: 2,
                },
            ]
        },
        plugins: [...makeStrikePlugins({ spot, labels }), glowPlugin, spotPlugin],
        options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 400, easing: "easeOutExpo" },
            grouped: false,
            plugins: {
                legend: {
                    display: true,
                    position: "top",
                    align: "end",
                    labels: {
                        color: "rgba(180,190,220,.8)", boxWidth: 8, borderRadius: 3,
                        useBorderRadius: true, usePointStyle: false,
                        font: { size: 11, family: "'JetBrains Mono', monospace" }, padding: 14,
                    }
                },
                tooltip: {
                    backgroundColor: themeBg1(),
                    borderColor: themeAccent() + "44",
                    borderWidth: 1,
                    titleColor: "#60a5fa",
                    bodyColor: "#4a5a78",
                    padding: 12,
                    cornerRadius: 8,
                    callbacks: {
                        label: c => {
                            const side = c.datasetIndex === 0 ? "Calls" : "Puts";
                            return `  ${side}: ${fmt(Math.abs(c.raw))}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    stacked: false,
                    ticks: {
                        color: "rgba(100,130,170,.7)",
                        font: { size: 11, family: "'JetBrains Mono', monospace" },
                        callback: v => fmt(Math.abs(v)),
                        maxTicksLimit: 5,
                    },
                    grid: { color: GRID, lineWidth: 1 },
                    border: { color: "transparent" },
                },
                y: {
                    ticks: {
                        color: "rgba(140,160,200,.8)",
                        font: { family: "'JetBrains Mono', monospace", size: 11 },
                        padding: 6,
                    },
                    grid: { display: false },
                    border: { color: "transparent" },
                },
            }
        }
    });
}

// â”€â”€ 365 DTE OI Chart â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function dteColor(dte, maxDte) {
    // Thermal colormap: 0 DTE = hot red, 365 DTE = blue-purple
    const t = Math.min(1, dte / Math.max(maxDte, 1));
    // 7-stop thermal: redâ†’orangeâ†’yellowâ†’greenâ†’cyanâ†’blueâ†’purple
    const stops = [
        [255, 30, 30],   // 0.00  red
        [255, 140, 0],   // 0.17  orange
        [255, 215, 0],   // 0.33  yellow
        [0, 200, 80],   // 0.50  green
        [0, 210, 255],   // 0.67  cyan
        [60, 100, 255],   // 0.83  blue
        [160, 40, 255],   // 1.00  purple
    ];
    const n = stops.length;
    const pos = t * (n - 1);
    const lo = Math.floor(pos);
    const hi = Math.min(lo + 1, n - 1);
    const f = pos - lo;
    const [r0, g0, b0] = stops[lo];
    const [r1, g1, b1] = stops[hi];
    return [
        Math.round(r0 + (r1 - r0) * f),
        Math.round(g0 + (g1 - g0) * f),
        Math.round(b0 + (b1 - b0) * f),
    ];
}

async function loadOI365() {
    const loader = document.getElementById("oi365-loader");
    if (loader) loader.style.display = "block";
    try {
        const res = await authFetch("/api/oi365");
        const data = await res.json();
        if (data.error) return;
        buildOI365(data);
    } catch (e) { console.error("OI365 error:", e); }
    finally { if (loader) loader.style.display = "none"; }
}

function buildOI365(data) {
    const expirations = data.expirations || [];
    const spot = data.spot;
    if (!expirations.length) return;

    const maxDte = expirations[expirations.length - 1].dte;

    // Collect all unique strikes across all expirations (descending)
    const strikeSet = new Set();
    for (const exp of expirations)
        for (const k of Object.keys(exp.strikes)) strikeSet.add(parseFloat(k));
    const strikes = [...strikeSet].sort((a, b) => b - a);
    const labels = strikes.map(s => "$" + s.toFixed(0));

    // One dataset per expiration, colored by DTE
    const datasets = expirations.map(exp => {
        const [r, g, b] = dteColor(exp.dte, maxDte);
        const colorStr = `rgba(${r},${g},${b},.90)`;
        // calls extend right (positive), puts extend left (negative)
        const callData = strikes.map(s => exp.strikes[String(Math.round(s))]?.calls || 0);
        const putData = strikes.map(s => -(exp.strikes[String(Math.round(s))]?.puts || 0));
        return [
            {
                label: exp.label + " calls",
                data: callData,
                backgroundColor: colorStr,
                hoverBackgroundColor: `rgba(${r},${g},${b},1)`,
                borderRadius: 0,
                borderSkipped: false,
                borderWidth: 0,
                barPercentage: 0.14,
                categoryPercentage: 1.0,
                _color: [r, g, b],
                _exp: exp,
            },
            {
                label: exp.label + " puts",
                data: putData,
                backgroundColor: colorStr,
                hoverBackgroundColor: `rgba(${r},${g},${b},1)`,
                borderRadius: 0,
                borderSkipped: false,
                borderWidth: 0,
                barPercentage: 0.14,
                categoryPercentage: 1.0,
                _color: [r, g, b],
                _exp: exp,
            }
        ];
    }).flat();

    // Dynamic height
    const totalH = Math.max(500, strikes.length * 20 + 80);
    const wrap = document.getElementById("chart-oi365")?.parentElement;
    if (wrap) wrap.style.height = totalH + "px";

    if (charts["chart-oi365"]) charts["chart-oi365"].destroy();
    const canvas = document.getElementById("chart-oi365");
    if (!canvas) return;

    // â”€â”€ Glow plugin
    const glowPlugin365 = {
        id: "barGlow365",
        beforeDatasetDraw(chart, args) {
            const ds = chart.data.datasets[args.index];
            if (!ds._color) return;
            const [r, g, b] = ds._color;
            chart.ctx.shadowColor = `rgba(${r},${g},${b},.7)`;
            chart.ctx.shadowBlur = 10;
        },
        afterDatasetDraw(chart) {
            chart.ctx.shadowBlur = 0;
            chart.ctx.shadowColor = "transparent";
        },
    };

    // â”€â”€ Spot line plugin
    const spotPlugin365 = {
        id: "spotLine365",
        afterDraw(chart) {
            if (!spot) return;
            const nearIdx = labels.reduce((b, lbl, i) =>
                Math.abs(parseFloat(lbl.replace("$", "")) - spot) <
                    Math.abs(parseFloat(labels[b].replace("$", "")) - spot) ? i : b, 0);
            const meta = chart.getDatasetMeta(0);
            if (!meta.data[nearIdx]) return;
            const y = meta.data[nearIdx].y;
            const { left, right } = chart.chartArea;
            const c2 = chart.ctx;
            c2.save();
            c2.setLineDash([4, 3]);
            c2.strokeStyle = "rgba(96,165,250,.8)";
            c2.lineWidth = 1.5;
            c2.beginPath(); c2.moveTo(left, y); c2.lineTo(right, y); c2.stroke();
            c2.fillStyle = "rgba(96,165,250,.9)";
            c2.font = "bold 11px 'JetBrains Mono', monospace";
            c2.fillText("ATM $" + spot.toFixed(0), left + 4, y - 4);
            c2.restore();
        }
    };

    charts["chart-oi365"] = new Chart(canvas.getContext("2d"), {
        type: "bar",
        data: { labels, datasets },
        plugins: [glowPlugin365, spotPlugin365],
        options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 500, easing: "easeOutExpo" },
            grouped: false,
            plugins: {
                legend: { display: false },  // handled by custom HTML legend
                tooltip: {
                    backgroundColor: themeBg1(),
                    borderColor: "rgba(255,255,255,.10)",
                    borderWidth: 1,
                    titleColor: "#60a5fa",
                    bodyColor: "#8899aa",
                    padding: 10,
                    callbacks: {
                        title: items => items[0]?.label || "",
                        label: c => {
                            const ds = c.dataset;
                            const side = c.raw >= 0 ? "Calls" : "Puts";
                            return `  ${ds._exp?.label} ${side}: ${fmt(Math.abs(c.raw))}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    stacked: false,
                    ticks: { color: 'rgba(140,160,200,.8)', font: { size: 11, family: "'JetBrains Mono', monospace" }, callback: v => fmt(Math.abs(v)), maxTicksLimit: 7 },
                    grid: { color: GRID },
                    border: { color: "rgba(255,255,255,.05)" },
                },
                y: {
                    ticks: { color: "rgba(140,160,200,.85)", font: { family: "'JetBrains Mono', monospace", size: 11 }, padding: 6 },
                    grid: { display: false },
                    border: { color: "rgba(255,255,255,.05)" },
                },
            }
        }
    });

    // â”€â”€ Build legend panel
    const legendEl = document.getElementById("oi365-legend");
    if (legendEl) {
        legendEl.innerHTML = expirations.map(exp => {
            const [r, g, b] = dteColor(exp.dte, maxDte);
            return `<div class="oi365-legend-row">
                <span class="oi365-swatch" style="background:rgb(${r},${g},${b})"></span>
                <span class="oi365-label">${exp.label}</span>
                <span class="oi365-meta">${exp.dte}d</span>
                <span class="oi365-pc" style="color:${exp.pc > 1 ? '#3b1278' : '#f5ff30'}">P/C ${exp.pc.toFixed(2)}</span>
                <span class="oi365-oi">${fmt(exp.total_oi)}</span>
            </div>`;
        }).join("");
    }
}

// â”€â”€ Generic net-exposure bar (GEX / DEX / VEX strikes) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function buildNetBar(canvasId, barData, spot, colorPos, colorNeg) {
    const entries = Object.entries(barData)
        .map(([k, v]) => [parseFloat(k), v])
        .filter(([k]) => !spot || Math.abs(k - spot) <= 25)  // only show ±25 from spot
        .sort((a, b) => b[0] - a[0]);   // descending by strike

    if (!entries.length) return;

    const labels = entries.map(([k]) => "$" + k.toFixed(0));
    const values = entries.map(([, v]) => v);
    const cPos = colorPos || "rgba(0,220,150,.88)";
    const cNeg = colorNeg || "rgba(255,50,90,.88)";

    // Find nearest strike to spot â†’ highlight in cyan
    const nearestIdx = entries.reduce((best, [k], i) =>
        Math.abs(k - spot) < Math.abs(entries[best][0] - spot) ? i : best, 0);

    const colors = values.map((v, i) =>
        i === nearestIdx ? "rgba(56,189,248,.95)" : (v >= 0 ? cPos : cNeg));
    const hoverColors = colors.map(c => c.replace(/[\d.]+\)$/, "1)"));

    if (charts[canvasId]) charts[canvasId].destroy();
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;

    canvas.parentElement.style.height = Math.max(320, entries.length * 16 + 60) + "px";

    // Use shared strike plugins (bg, zero line, ATM glow band + value labels)
    const _sharedPlugins = makeStrikePlugins({ spot, labels });
    // ATM bar glow plugin (keep for bars themselves)
    const _atmGlow = {
        id: "atmGlow",
        beforeDatasetDraw(chart, args) {
            const ds = chart.data.datasets[args.index];
            const ctx = chart.ctx;
            ctx.save();
            ctx.shadowColor = "rgba(56,189,248,.7)";
            ctx.shadowBlur = 14;
        },
        afterDatasetDraw(chart) {
            chart.ctx.restore();
        }
    };
    // Zero-line & bg plugin
    const _bgPlugin = {
        id: "chartBg",
        beforeDraw(chart) {
            const { ctx, chartArea } = chart;
            if (!chartArea) return;
            ctx.save();
            ctx.fillStyle = "rgba(4,4,10,.7)";
            ctx.fillRect(chartArea.left, chartArea.top, chartArea.width, chartArea.height);
            ctx.restore();
        }
    };
    charts[canvasId] = new Chart(canvas.getContext("2d"), {
        type: "bar",
        data: {
            labels, datasets: [{
                data: values,
                backgroundColor: (ctx2) => {
                    const chart2 = ctx2.chart;
                    const { ctx: c, chartArea } = chart2;
                    if (!chartArea) return colors[ctx2.dataIndex];
                    const v = values[ctx2.dataIndex];
                    const i = ctx2.dataIndex;
                    if (i === nearestIdx) {
                        const g = c.createLinearGradient(chartArea.left, 0, chartArea.right, 0);
                        g.addColorStop(0, 'rgba(56,189,248,.15)');
                        g.addColorStop(0.5, 'rgba(56,189,248,.95)');
                        g.addColorStop(1, 'rgba(96,220,255,.7)');
                        return g;
                    }
                    const g = c.createLinearGradient(chartArea.left, 0, chartArea.right, 0);
                    if (v >= 0) {
                        g.addColorStop(0, 'rgba(34,197,94,.08)');
                        g.addColorStop(0.4, cPos.replace('.88', '.6'));
                        g.addColorStop(1, cPos);
                    } else {
                        g.addColorStop(0, 'rgba(239,68,68,.08)');
                        g.addColorStop(0.4, cNeg.replace('.88', '.6'));
                        g.addColorStop(1, cNeg);
                    }
                    return g;
                },
                hoverBackgroundColor: hoverColors,
                borderWidth: 0,
                borderRadius: 6,
                borderSkipped: false,
                barPercentage: 0.72,
                categoryPercentage: 0.92,
            }]
        },
        plugins: [..._sharedPlugins, _atmGlow],
        options: {
            indexAxis: "y", responsive: true, maintainAspectRatio: false,
            animation: { duration: 250, easing: "easeOutExpo" },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: themeBg1(),
                    borderColor: themeAccent() + "55",
                    borderWidth: 1,
                    cornerRadius: 8,
                    titleColor: "#38bdf8",
                    bodyColor: "#5a6a8a",
                    padding: 10,
                    callbacks: {
                        title: items => items[0]?.label || "",
                        label: c => "  " + fmt(c.raw)
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: "#2d3a50",
                        font: { size: 8, family: "'JetBrains Mono', monospace" },
                        callback: v => fmt(v),
                        maxTicksLimit: 4
                    },
                    grid: { color: GRID, lineWidth: 1 },
                    border: { color: "transparent" },
                },
                y: {
                    ticks: {
                        color: ctx => ctx.index === nearestIdx ? "#38bdf8" : "#2d3a50",
                        font: { family: "'JetBrains Mono', monospace", size: 8 },
                        padding: 4,
                    },
                    grid: { display: false },
                    border: { color: "rgba(255,255,255,.04)" },
                },
            }
        }
    });
}

// â”€â”€ IV Diagram (multi-expiry smile) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function buildIVDiagram() {
    try {
        const res = await authFetch("/api/vol_skew_multi");
        const d = await res.json();
        if (d.error || !d.expirations?.length) return;

        const colorStops = [
            [255, 30, 30], [255, 140, 0], [0, 200, 80], [60, 100, 255], [160, 40, 255]
        ];
        const ncol = colorStops.length;

        const datasets = d.expirations.map((exp, i) => {
            const t = i / Math.max(d.expirations.length - 1, 1);
            const pos = t * (ncol - 1);
            const lo = Math.floor(pos), hi = Math.min(lo + 1, ncol - 1), f = pos - lo;
            const [r0, g0, b0] = colorStops[lo], [r1, g1, b1] = colorStops[hi];
            const col = `rgb(${Math.round(r0 + (r1 - r0) * f)},${Math.round(g0 + (g1 - g0) * f)},${Math.round(b0 + (b1 - b0) * f)})`;
            return {
                label: exp.label,
                data: exp.data.map(p => ({ x: p.strike, y: p.iv })),
                borderColor: col,
                backgroundColor: "transparent",
                borderWidth: 1.5,
                pointRadius: 0,
                tension: 0.35,
            };
        });

        if (charts["chart-iv-diagram"]) charts["chart-iv-diagram"].destroy();
        const canvas = document.getElementById("chart-iv-diagram");
        if (!canvas) return;

        charts["chart-iv-diagram"] = new Chart(canvas.getContext("2d"), {
            type: "line",
            data: { datasets },
            options: {
                responsive: true, maintainAspectRatio: false,
                animation: { duration: 400 },
                parsing: false,
                plugins: {
                    legend: { display: true, labels: { color: TEXT, font: { size: 10 }, boxWidth: 20 } },
                    tooltip: {
                        backgroundColor: themeBg1(), titleColor: CYAN, bodyColor: TEXT,
                        callbacks: { label: c => ` ${c.dataset.label}: ${c.parsed.y?.toFixed(1)}%` }
                    }
                },
                scales: {
                    x: {
                        type: "linear", title: { display: true, text: "Strike", color: TEXT, font: { size: 10 } },
                        ticks: { color: TEXT, font: { size: 9 } }, grid: { color: GRID }, border: { color: GRID }
                    },
                    y: {
                        title: { display: true, text: "IV %", color: TEXT, font: { size: 10 } },
                        ticks: { color: TEXT, font: { size: 9 }, callback: v => v + "%" },
                        grid: { color: GRID }, border: { color: GRID }
                    },
                }
            }
        });
    } catch (e) { console.error("IV Diagram:", e); }
}

// â”€â”€ Render â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function render(data) {
    // Header
    try {
        document.getElementById("ticker").textContent = data.ticker;
        _updateActivePreset(data.ticker);
        document.getElementById("spot").textContent = "$" + data.spot.toFixed(2);
        document.getElementById("timestamp").textContent = fmtTime(data.timestamp);
        document.getElementById("callWall").textContent = "$" + data.call_wall.toFixed(0);
        document.getElementById("putWall").textContent = "$" + data.put_wall.toFixed(0);
        document.getElementById("majorWall").textContent = "$" + data.major_wall.toFixed(0);
        document.getElementById("maxPain").textContent = "$" + data.max_pain.toFixed(0);
    } catch (e) { console.error("Header render:", e); }

    const spot = data.spot;
    window._lastHmData = { oi_hm: data.oi_hm, dex_hm: data.dex_hm, gex_hm: data.gex_hm, spot };

    // 1 Â· OI Strike + Heatmap
    try { if (data.oi_bar) buildOI(data.oi_bar, spot); } catch (e) { console.error("OI bar:", e); }

    // 2 Â· DEX Strike + Heatmap
    try { if (data.dex_bar) buildNetBar("chart-dex-bar", data.dex_bar, spot, "rgba(0,180,255,.88)", "rgba(255,160,20,.88)"); } catch (e) { console.error("DEX bar:", e); }
    window._lastHmBuildData = data; window._lastHmSpot = spot; window._lastSpot = spot;
    // ── Update dashboard summary strip ──────────────────────────────────────
    const dsSpot = document.getElementById('ds-spot');
    if (dsSpot) dsSpot.textContent = '$' + spot.toFixed(2);
    const dsMp = document.getElementById('ds-mp');
    if (dsMp) dsMp.textContent = '$' + parseFloat(data.max_pain || 0).toFixed(0);
    const dsCw = document.getElementById('ds-cw');
    if (dsCw) dsCw.textContent = '$' + parseFloat(data.call_wall || 0).toFixed(0);
    const dsPw = document.getElementById('ds-pw');
    if (dsPw) dsPw.textContent = '$' + parseFloat(data.put_wall || 0).toFixed(0);
    // P/C ratio from OI bar
    if (data.oi_bar) {
        let totalC = 0, totalP = 0;
        for (const s in data.oi_bar) { totalC += data.oi_bar[s].calls; totalP += data.oi_bar[s].puts; }
        const pcr = totalP > 0 ? (totalC / totalP).toFixed(2) : '—';
        const dsPcr = document.getElementById('ds-pcr');
        if (dsPcr) dsPcr.textContent = pcr;
    }
    // Net DEX
    if (data.dex_bar) {
        let netDex = 0;
        for (const s in data.dex_bar) netDex += data.dex_bar[s];
        const dsNdex = document.getElementById('ds-ndex');
        if (dsNdex) {
            const fmt = Math.abs(netDex) >= 1e9 ? (netDex / 1e9).toFixed(1) + 'B' : Math.abs(netDex) >= 1e6 ? (netDex / 1e6).toFixed(1) + 'M' : Math.abs(netDex) >= 1e3 ? (netDex / 1e3).toFixed(0) + 'K' : netDex.toFixed(0);
            dsNdex.textContent = (netDex >= 0 ? '+' : '') + fmt;
            dsNdex.className = 'dash-metric-val ' + (netDex >= 0 ? 'up' : 'dn');
        }
    }
    // GEX flip (major wall)
    const dsGflip = document.getElementById('ds-gflip');
    if (dsGflip) dsGflip.textContent = '$' + parseFloat(data.major_wall || 0).toFixed(0);

    try { if (data.dex_hm) buildHeatmap("hm-dex", data.dex_hm, hmCmap("dex"), spot); } catch (e) { console.error("DEX hm:", e); }

    // 3 Â· GEX Strike + Heatmap
    try { if (data.gex_bar) buildNetBar("chart-gex-bar", data.gex_bar, spot, "rgba(0,220,150,.88)", "rgba(255,50,90,.88)"); } catch (e) { console.error("GEX bar:", e); }
    try { if (data.gex_hm) buildHeatmap("hm-gex", data.gex_hm, hmCmap("gex"), spot); } catch (e) { console.error("GEX hm:", e); }

    // 4 Â· VEX (Vega) Strike
    try { if (data.vex_bar) buildNetBar("chart-vex-bar", data.vex_bar, spot, "rgba(160,80,255,.88)", "rgba(255,140,20,.88)"); } catch (e) { console.error("VEX bar:", e); }
    try { if (data.vex_bar) buildNetBar("chart-vex-bar-main", data.vex_bar, spot, "rgba(160,80,255,.88)", "rgba(255,140,20,.88)"); } catch (e) { console.error("VEX bar:", e); }
    try { if (data.rex_bar) buildNetBar("chart-rex-bar", data.rex_bar, spot, "rgba(255,180,40,.88)", "rgba(80,200,255,.88)"); } catch (e) { console.error("REX bar:", e); }

    // 5+6 Â· Vanna + Charm gradient + OHLC overlay
    // 5+6 · VannaEX + CharmEX Heatmaps
    try { if (data.vannex_hm) buildHeatmap("hm-vannex", data.vannex_hm, hmCmap("vannex"), spot); } catch (e) { console.error("VannaEX hm:", e); }
    try { if (data.cex_hm) buildHeatmap("hm-cex", data.cex_hm, hmCmap("cex"), spot); } catch (e) { console.error("CharmEX hm:", e); }

    // Charm + Vanna intraday overlay (Plotly)
    buildCharmVannaOverlay();

    // 7 Â· IV Diagram (lazy â€” only on first render)
    try { buildIVDiagram(); } catch (e) { console.error("IV diagram:", e); }
}

// â”€â”€ Charm + Vanna Gradient + OHLC Overlay (Plotly) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function buildCharmVannaOverlay() {
    try {
        const res = await authFetch("/api/charm_overlay");
        const d = await res.json();
        if (d.error || !d.strikes?.length || !d.ohlc?.length) return;

        const spot = d.spot;
        const range = 40;  // show Â±40 strikes from spot

        // Filter to relevant strikes near spot price for better OHLC visibility
        const idxs = d.strikes.reduce((acc, s, i) => {
            if (Math.abs(s - spot) <= range) acc.push(i);
            return acc;
        }, []);
        const strikes = idxs.map(i => d.strikes[i]);
        const charmVals = idxs.map(i => d.charm_vals[i]);
        const vannaVals = idxs.map(i => d.vanna_vals[i]);

        const times = d.ohlc.map(c => c.time);

        // Build 2D z: rows=strikes, cols=times (same value across all times)
        function makeZ(vals) {
            return vals.map(v => new Array(times.length).fill(v));
        }

        // Compute y-axis bounds to keep OHLC visible
        const allPrices = d.ohlc.flatMap(c => [c.high, c.low]);
        const priceMin = Math.min(...allPrices);
        const priceMax = Math.max(...allPrices);
        const pad = (priceMax - priceMin) * 0.6;
        const yRange = [priceMin - pad, priceMax + pad];

        const baseLayout = {
            paper_bgcolor: "#0d0f14",
            plot_bgcolor: "#0d0f14",
            font: { color: "#6b7896", size: 10, family: "'JetBrains Mono', sans-serif" },
            margin: { t: 28, b: 44, l: 68, r: 60 },
            xaxis: {
                type: "date",
                title: { text: "Time", font: { size: 10, color: "#42506a" } },
                tickfont: { size: 8 },
                gridcolor: "rgba(255,255,255,.05)",
                color: "#42506a",
                linecolor: "transparent",
                rangeslider: { visible: false },
            },
            yaxis: {
                range: yRange,
                title: { text: "Price", font: { size: 10, color: "#42506a" } },
                tickfont: { size: 8, family: "'JetBrains Mono', monospace" },
                gridcolor: "rgba(255,255,255,.05)",
                color: "#42506a",
                linecolor: "transparent",
                tickprefix: "$",
                side: "right",
            },
            legend: {
                x: 0.01, y: 0.99,
                bgcolor: "rgba(10,12,20,.70)",
                bordercolor: "rgba(255,255,255,.12)", borderwidth: 1,
                font: { size: 9, family: "'JetBrains Mono', monospace" }
            },
        };

        const ohlcTrace = {
            type: "candlestick",
            name: "Price",
            x: times,
            open: d.ohlc.map(c => c.open),
            high: d.ohlc.map(c => c.high),
            low: d.ohlc.map(c => c.low),
            close: d.ohlc.map(c => c.close),
            increasing: { line: { color: "#22c77a", width: 1 }, fillcolor: "rgba(34,199,122,.75)" },
            decreasing: { line: { color: "#e0404f", width: 1 }, fillcolor: "rgba(224,64,79,.75)" },
        };

        function heatTrace(vals, colorscale, name, cbTitle) {
            return {
                type: "heatmap",
                name,
                z: makeZ(vals),
                x: times,
                y: strikes,
                colorscale,
                zmid: 0,
                zsmooth: "best",
                opacity: 0.84,
                showscale: true,
                colorbar: {
                    title: { text: cbTitle || "", font: { size: 9, color: "#8899aa" }, side: "right" },
                    thickness: 8, len: 0.65, x: 1.01,
                    tickfont: { size: 8, family: "'JetBrains Mono', monospace" },
                    outlinewidth: 0, tickcolor: "#42506a",
                },
                hovertemplate: "Strike $%{y}<br>%{z:.4f}<extra>" + name + "</extra>",
            };
        }

        // Blue <> white <> orange -- matches reference gradient
        const charmScale = [
            [0.00, "rgb(0,50,200)"],
            [0.25, "rgb(30,130,235)"],
            [0.46, "rgb(140,200,250)"],
            [0.50, "rgb(250,250,252)"],
            [0.54, "rgb(255,225,140)"],
            [0.75, "rgb(250,150,20)"],
            [1.00, "rgb(210,80,0)"],
        ];
        const vannaScale = [
            [0.00, "rgb(60,0,160)"],
            [0.35, "rgb(180,120,230)"],
            [0.50, "rgb(250,250,252)"],
            [0.65, "rgb(240,200,80)"],
            [1.00, "rgb(200,130,0)"],
        ];

        const cfg = { displayModeBar: false, responsive: true };

        const vannaEl = document.getElementById("plot-vanna-overlay");
        const charmEl = document.getElementById("plot-charm-overlay");

        if (vannaEl) Plotly.newPlot(vannaEl,
            [heatTrace(vannaVals, vannaScale, "Vanna"), ohlcTrace],
            { ...baseLayout }, cfg);

        if (charmEl) Plotly.newPlot(charmEl,
            [heatTrace(charmVals, charmScale, "Charm", "Charm\n(δ/5min)"), { ...ohlcTrace }],
            { ...baseLayout, title: { text: "Charm Gradient  +  OHLC", font: { size: 11, color: "#8899aa" }, x: 0.5 } }, cfg);

    } catch (e) { console.error("Charm/Vanna overlay:", e); }
}

/** Convert OI call_rows/put_rows heatmap to a net-style {strikes, expirations, rows} for buildHeatmap */
function _oiToNetHm(oiHm) {
    const { strikes, expirations, call_rows, put_rows } = oiHm;
    if (!strikes) return { strikes: [], expirations: [], rows: [] };
    const rows = call_rows.map((cr, i) => ({
        strike: cr.strike,
        cells: cr.cells.map((c, j) => c - (put_rows[i]?.cells[j] || 0))
    }));
    return { strikes, expirations, rows };
}

// â”€â”€ Fetch â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function update() {
    const badge = document.getElementById("badge");
    try {
        const res = await authFetch(API_URL);
        if (!res.ok) throw new Error(res.statusText);
        const data = await res.json();
        render(data);
        // Preload ALL tab data before dismissing welcome screen
        document.getElementById("loading-overlay")?.classList.add("hidden");
        if (document.getElementById("tab-prob")?.classList.contains("active")) renderProb();
        badge.textContent = "LIVE";
        badge.style.color = GREEN;
        badge.style.background = "rgba(0,221,119,.15)";
        badge.style.borderColor = "rgba(0,221,119,.3)";

        // Only preload on first load (welcome screen visible)
        if (window._dismissWelcome && !window._preloadDone) {
            window._preloadDone = true;
            const bar = document.getElementById('ws-progress-bar');
            const status = document.getElementById('ws-status');
            let loaded = 0;
            const modules = [
                { name: 'ANOMALIES', fn: () => typeof updateAnomaly === 'function' ? updateAnomaly() : Promise.resolve() },
                { name: 'VOLATILITY', fn: () => typeof updateVolatility === 'function' ? updateVolatility() : Promise.resolve() },
                { name: 'MACRO DATA', fn: () => typeof updateMacro === 'function' ? updateMacro() : Promise.resolve() },
                { name: 'REGIME', fn: () => typeof updateRegime === 'function' ? updateRegime() : Promise.resolve() },
                { name: 'OPEN INTEREST', fn: () => typeof loadOI365 === 'function' ? loadOI365() : Promise.resolve() },
                { name: 'PROBABILITY', fn: () => typeof renderProb === 'function' ? (renderProb(), Promise.resolve()) : Promise.resolve() },
                { name: 'SETTINGS', fn: () => typeof loadSettings === 'function' ? loadSettings() : Promise.resolve() },
                { name: 'HIRO FLOW', fn: () => typeof loadHIRO === 'function' ? loadHIRO() : Promise.resolve() },
            ];
            const total = modules.length;
            if (bar) bar.style.width = '60%';
            if (status) status.textContent = 'LOADING ALL MODULES...';

            // Fire all in parallel, update progress as each completes
            const promises = modules.map(m =>
                Promise.resolve().then(() => m.fn()).catch(() => { }).then(() => {
                    loaded++;
                    const pct = 60 + Math.round((loaded / total) * 38);
                    if (bar) bar.style.width = pct + '%';
                    if (status) status.textContent = `LOADED ${m.name} (${loaded}/${total})`;
                })
            );
            Promise.all(promises).then(() => {
                if (bar) bar.style.width = '100%';
                if (status) status.textContent = 'ALL SYSTEMS READY';
                setTimeout(() => { if (window._dismissWelcome) window._dismissWelcome(); }, 500);
            });
        }
        flashRefreshBar();
    } catch (e) {
        badge.textContent = "ERR";
        badge.style.color = RED;
        badge.style.background = "rgba(255,68,102,.15)";
        badge.style.borderColor = "rgba(255,68,102,.3)";
        console.error("Fetch error:", e);
        // Show user-friendly status — ERR means the data endpoint is unreachable
        // (market closed / API key missing / server restarting). Auto-retries each interval.
        const statusEl = document.getElementById("last-update");
        if (statusEl) statusEl.textContent = "Connection error — retrying…";
    }
}

// â”€â”€ Tabs â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
document.querySelectorAll(".tab").forEach(btn => {
    btn.addEventListener("click", () => {
        document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
        document.querySelectorAll(".tab-content").forEach(t => t.classList.remove("active"));
        btn.classList.add("active");
        document.getElementById("tab-" + btn.dataset.tab).classList.add("active");
        // Trigger anomaly/HIRO/regime load lazily
        if (btn.dataset.tab === "anomaly" && !charts["chart-anomaly"]) updateAnomaly();
        if (btn.dataset.tab === "anomaly") { loadHIRO(); }
        if (btn.dataset.tab === "anomaly" && !topoLoaded) { topoLoaded = true; loadTopology(); }
        if (btn.dataset.tab === "anomaly" && !entropyLoaded) { entropyLoaded = true; loadEntropy(); }
        if (btn.dataset.tab === "vol") updateVolatility();
        if (btn.dataset.tab === "macro") updateMacro();
        if (btn.dataset.tab === "chart") { charts["_onChartTab"] = true; updateCandleChart(); }
        if (btn.dataset.tab !== "chart") { charts["_onChartTab"] = false; }
        if (btn.dataset.tab === "regime" && !charts["chart-regime-price"]) updateRegime();
        if (btn.dataset.tab === "settings") loadSettings();
        if (btn.dataset.tab === "oi") loadOI365();
        if (btn.dataset.tab === "prob") renderProb();
    });
});

// â”€â”€ Anomaly Chart â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function updateAnomaly() {
    try {
        const res = await authFetch(ANOMALY_URL);
        const d = await res.json();
        if (d.error) { console.error("Anomaly:", d.error); return; }

        const labels = d.times.map(t => {
            const dt = new Date(t);
            return dt.toISOString().slice(0, 10) + " " +
                dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false });
        });

        // Determine today's date string so we can emphasise the current session
        const today = new Date().toISOString().slice(0, 10);
        const isToday = labels.map(l => l.startsWith(today));

        // Per-point colors: dim older sessions, bright for today
        const lineColors = labels.map(l => l.startsWith(today) ? "rgba(200,200,210,.90)" : "rgba(120,120,130,.22)");
        const upColors = d.log_returns.map((v, i) => d.z_scores[i] > d.threshold
            ? (isToday[i] ? "#ff4444" : "rgba(255,80,80,.25)") : "transparent");
        const downColors = d.log_returns.map((v, i) => d.z_scores[i] < -d.threshold
            ? (isToday[i] ? "#22ee55" : "rgba(40,200,80,.25)") : "transparent");
        const upRadius = d.log_returns.map((v, i) => d.z_scores[i] > d.threshold ? (isToday[i] ? 6 : 3) : 0);
        const downRadius = d.log_returns.map((v, i) => d.z_scores[i] < -d.threshold ? (isToday[i] ? 6 : 3) : 0);

        const upPoints = d.log_returns.map((v, i) => d.z_scores[i] > d.threshold ? v : null);
        const downPoints = d.log_returns.map((v, i) => d.z_scores[i] < -d.threshold ? v : null);

        // Find day-boundary indices for vertical separator lines
        const dayBounds = [];
        let prevDay = null;
        labels.forEach((l, i) => {
            const day = l.slice(0, 10);
            if (prevDay && day !== prevDay) dayBounds.push(i);
            prevDay = day;
        });

        if (charts["chart-anomaly"]) { charts["chart-anomaly"].destroy(); }

        const ctx = document.getElementById("chart-anomaly").getContext("2d");
        charts["chart-anomaly"] = new Chart(ctx, {
            type: "line",
            data: {
                labels, datasets: [
                    {
                        label: "Log Return", data: d.log_returns,
                        segment: {
                            borderColor: ctx2 => lineColors[ctx2.p0DataIndex] || "rgba(180,180,190,.3)"
                        },
                        borderColor: "rgba(180,180,190,.5)", borderWidth: 1,
                        pointRadius: 0, tension: 0, fill: false, order: 2
                    },
                    {
                        label: `Sell Imbalance (Z > ${d.threshold})`, data: upPoints,
                        borderColor: upColors, backgroundColor: upColors,
                        pointRadius: upRadius, pointHoverRadius: 8,
                        showLine: false, order: 1
                    },
                    {
                        label: `Buy Imbalance (Z < -${d.threshold})`, data: downPoints,
                        borderColor: downColors, backgroundColor: downColors,
                        pointRadius: downRadius, pointHoverRadius: 8,
                        showLine: false, order: 1
                    },
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false, animation: { duration: 200 },
                plugins: {
                    legend: {
                        display: true, position: "top", align: "end",
                        labels: { color: "#ccc", boxWidth: 10, padding: 10, usePointStyle: true, font: { size: 11 } }
                    },
                    title: {
                        display: true, text: "NASDAQ 100 | Statistical Anomalies (5m)",
                        color: "#ccc", font: { size: 12, weight: "normal" }, padding: { bottom: 8 }
                    },
                    tooltip: {
                        backgroundColor: "#111", titleColor: "#ccc", bodyColor: "#888",
                        callbacks: { label: c => c.raw !== null ? c.dataset.label + ": " + c.raw.toFixed(5) : "" }
                    },
                },
                scales: {
                    x: {
                        ticks: {
                            color: "#999", font: { size: 9 }, maxTicksLimit: 6,
                            callback(val, idx) {
                                const l = this.getLabelForValue(idx); const d = l.slice(0, 10);
                                const p = idx > 0 ? this.getLabelForValue(idx - 1).slice(0, 10) : null; return d !== p ? d : "";
                            }
                        },
                        grid: { color: "rgba(255,255,255,.05)" }
                    },
                    y: {
                        ticks: { color: "#999", font: { size: 9 }, callback: v => v.toFixed(3) },
                        grid: { color: "rgba(255,255,255,.05)" }
                    },
                }
            },
            plugins: [{
                id: "blackBg", beforeDraw(chart) {
                    const { ctx, chartArea } = chart; if (!chartArea) return;
                    ctx.save(); ctx.fillStyle = "#000";
                    ctx.fillRect(chartArea.left, chartArea.top, chartArea.width, chartArea.height);
                    ctx.restore();
                }
            }, {
                // Draw vertical day-separator lines and shade past sessions
                id: "daySep", afterDraw(chart) {
                    const { ctx, chartArea, scales } = chart; if (!chartArea) return;
                    ctx.save();
                    dayBounds.forEach(idx => {
                        const x = scales.x.getPixelForValue(idx);
                        if (x < chartArea.left || x > chartArea.right) return;
                        ctx.beginPath();
                        ctx.moveTo(x, chartArea.top);
                        ctx.lineTo(x, chartArea.bottom);
                        ctx.strokeStyle = "rgba(255,255,255,.18)";
                        ctx.lineWidth = 1;
                        ctx.setLineDash([4, 4]);
                        ctx.stroke();
                        ctx.setLineDash([]);
                    });
                    // Label each day at the top
                    ctx.fillStyle = "rgba(160,160,170,.55)";
                    ctx.font = "9px sans-serif";
                    ctx.textAlign = "left";
                    let prevBound = 0;
                    [...dayBounds, labels.length].forEach(bound => {
                        const midIdx = Math.round((prevBound + bound) / 2);
                        const midX = scales.x.getPixelForValue(midIdx);
                        const dayLabel = labels[prevBound]?.slice(0, 10) || "";
                        if (midX > chartArea.left && midX < chartArea.right)
                            ctx.fillText(dayLabel, midX - 20, chartArea.top + 12);
                        prevBound = bound;
                    });
                    ctx.restore();
                }
            }]
        });
    } catch (e) { console.error("Anomaly error:", e); }
}

// -- Regime Score Tab ----------------------------------------------------------
// Matches reference: Price+EMA with buy/sell signals, Score with zone fills, HUD
async function updateRegime() {
    try {
        const res = await authFetch("/api/regime_score");
        const d = await res.json();
        if (d.error) { console.error("Regime:", d.error); return; }

        // HUD badge
        const badge = document.getElementById("regime-badge");
        const scoreEl = document.getElementById("regime-score-val");
        const sub = document.getElementById("regime-ticker-sub");
        if (sub) sub.textContent = d.ticker + "  \u00b7  5-min bars";
        const score = d.current_score;
        if (badge) {
            badge.textContent = d.regime;
            badge.className = "regime-badge " +
                (score > 25 ? "bull" : score < -25 ? "bear" : "chop");
        }
        if (scoreEl) {
            scoreEl.textContent = score.toFixed(1);
            scoreEl.style.color = score > 25 ? "var(--green)" : score < -25 ? "var(--red)" : "var(--cyan)";
        }

        // Filter valid entries
        const times = d.times.filter((_, i) => d.prices[i] !== null);
        const prices = d.prices.filter(v => v !== null);
        const emas = d.emas.filter(v => v !== null);
        const scores = d.scores.filter(v => v !== null);

        // Short labels
        const labels = times.map(t => {
            const dt = new Date(t);
            return dt.toLocaleDateString("en", { month: "short", day: "numeric" }) +
                " " + dt.toLocaleTimeString("en", { hour: "2-digit", minute: "2-digit", hour12: false });
        });

        // Buy/sell signal indices
        const buyIdx = d.buy_times.map(bt => times.indexOf(bt)).filter(i => i >= 0);
        const sellIdx = d.sell_times.map(st => times.indexOf(st)).filter(i => i >= 0);

        // Buy signal scatter (green triangles)
        const buyData = labels.map((_, i) => buyIdx.includes(i) ? prices[i] : null);
        // Sell signal scatter (red triangles)  
        const sellData = labels.map((_, i) => sellIdx.includes(i) ? prices[i] : null);

        // Price + EMA chart with buy/sell signals
        const ctx1 = document.getElementById("chart-regime-price");
        if (ctx1 && !charts["chart-regime-price"]) {
            charts["chart-regime-price"] = new Chart(ctx1, {
                type: "line",
                data: {
                    labels,
                    datasets: [
                        { label: "Price", data: prices, borderColor: "rgba(255,255,255,.8)", borderWidth: 1.5, pointRadius: 0, tension: .3, fill: false },
                        { label: "EMA-50 (Fair Value)", data: emas, borderColor: "rgba(255,165,0,.7)", borderWidth: 1.2, pointRadius: 0, tension: .3, borderDash: [5, 3], fill: false },
                        {
                            label: "BUY BREAKOUT", data: buyData, borderColor: "transparent", borderWidth: 0, pointRadius: 8, pointStyle: "triangle",
                            pointBackgroundColor: "#00FF41", pointBorderColor: "#00FF41", showLine: false
                        },
                        {
                            label: "SELL BREAKDOWN", data: sellData, borderColor: "transparent", borderWidth: 0, pointRadius: 8, pointStyle: "triangle", pointRotation: 180,
                            pointBackgroundColor: "#FF3131", pointBorderColor: "#FF3131", showLine: false
                        },
                    ],
                },
                options: {
                    responsive: true, maintainAspectRatio: false,
                    plugins: { legend: { labels: { color: "#6b7a99", font: { size: 10, family: "JetBrains Mono" }, usePointStyle: true } } },
                    scales: {
                        x: { ticks: { color: "#4a5570", font: { size: 8 }, maxTicksLimit: 12 }, grid: { color: "rgba(255,255,255,.04)" } },
                        y: { ticks: { color: "#4a5570", font: { size: 9 }, callback: v => "$" + v.toFixed(0) }, grid: { color: "rgba(255,255,255,.04)" } },
                    },
                },
            });
        } else if (charts["chart-regime-price"]) {
            const c = charts["chart-regime-price"];
            c.data.labels = labels;
            c.data.datasets[0].data = prices;
            c.data.datasets[1].data = emas;
            c.data.datasets[2].data = buyData;
            c.data.datasets[3].data = sellData;
            c.update("none");
        }

        // Score chart with colored zone fills
        const ctx2 = document.getElementById("chart-regime-score");
        if (ctx2 && !charts["chart-regime-score"]) {
            charts["chart-regime-score"] = new Chart(ctx2, {
                type: "line",
                data: {
                    labels,
                    datasets: [
                        // Bullish zone fill (>25)
                        {
                            label: "Bull Zone", data: labels.map(() => 100), borderColor: "transparent", borderWidth: 0,
                            backgroundColor: "rgba(0,255,65,0.08)", fill: { target: { value: 25 }, above: "rgba(0,255,65,0.08)" }, pointRadius: 0
                        },
                        // Bearish zone fill (<-25)
                        {
                            label: "Bear Zone", data: labels.map(() => -100), borderColor: "transparent", borderWidth: 0,
                            backgroundColor: "rgba(255,49,49,0.08)", fill: { target: { value: -25 }, below: "rgba(255,49,49,0.08)" }, pointRadius: 0
                        },
                        // Score line (gray)
                        {
                            label: "Regime Score", data: scores,
                            borderColor: "rgba(180,180,180,0.8)", borderWidth: 2, pointRadius: 0, tension: .3, fill: false,
                        },
                    ],
                },
                options: {
                    responsive: true, maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                    },
                    scales: {
                        x: { ticks: { color: "#4a5570", font: { size: 8 }, maxTicksLimit: 12 }, grid: { color: "rgba(255,255,255,.04)" } },
                        y: {
                            min: -100, max: 100,
                            ticks: { color: "#4a5570", font: { size: 9 }, stepSize: 25 },
                            grid: {
                                color: ctx => {
                                    const v = ctx.tick.value;
                                    if (v === 25) return "rgba(0,255,65,0.3)";
                                    if (v === -25) return "rgba(255,49,49,0.3)";
                                    if (v === 0) return "rgba(255,255,255,0.3)";
                                    return "rgba(255,255,255,.04)";
                                }
                            },
                        },
                    },
                },
            });
        } else if (charts["chart-regime-score"]) {
            const c = charts["chart-regime-score"];
            c.data.labels = labels;
            c.data.datasets[2].data = scores;
            c.update("none");
        }

        // Update HUD checkmarks
        const hudEl = document.getElementById("regime-hud-checks");
        if (hudEl) {
            hudEl.innerHTML = `
                <div style="font-size:.7rem;color:var(--dim)">[${score > 25 ? "✓" : " "}] Trend Longs</div>
                <div style="font-size:.7rem;color:var(--dim)">[${score >= -25 && score <= 25 ? "✓" : " "}] Mean Reversion</div>
                <div style="font-size:.7rem;color:var(--dim)">[${score < -25 ? "✓" : " "}] Trend Shorts</div>`;
        }
    } catch (e) { console.error("Regime error:", e); }
}

// Refresh regime chart every 5 min when on the tab
setInterval(() => {
    if (charts["chart-regime-price"]) updateRegime();
}, 5 * 60 * 1000);


// ── Live spot price — polls every 5s for a fresh quote ──────────────────────
async function pollSpot() {
    try {
        const res = await authFetch("/api/spot");
        if (!res.ok) return;
        const d = await res.json();
        if (d.error) return;
        const el = document.getElementById("spot");
        if (!el) return;
        const prev = parseFloat(el.dataset.prev || d.spot);
        const cur = d.spot;
        el.textContent = "$" + cur.toFixed(2);
        // Flash green/red on change
        if (cur > prev) {
            el.style.color = "var(--green)";
        } else if (cur < prev) {
            el.style.color = "var(--red)";
        }
        el.dataset.prev = cur;
        // Fade back to accent after 1.5s
        setTimeout(() => { el.style.color = ""; }, 1500);
    } catch (_) { }
}

// Start live spot polling immediately and every 5 seconds
pollSpot();
setInterval(pollSpot, 5000);

// ── Ticker Switcher ──────────────────────────────────────────────────────────
async function switchTicker(sym) {
    sym = sym.trim().toUpperCase();
    if (!sym) return;
    // Update active state on preset buttons
    document.querySelectorAll(".ts-preset").forEach(b => {
        b.classList.toggle("active", b.dataset.sym === sym);
    });
    // Show switching state
    const badge = document.getElementById("badge");
    if (badge) { badge.textContent = "SWITCHING"; badge.style.color = YELLOW; }
    try {
        await authFetch(SETTINGS_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ticker: sym })
        });
        // Reset charts so they redraw with new data
        Object.keys(charts).forEach(k => {
            try { if (charts[k] && charts[k].destroy) charts[k].destroy(); } catch (_) { }
            delete charts[k];
        });
        // Force full refresh
        await update();
    } catch (e) {
        console.error("Ticker switch failed:", e);
    }
}

// Wire preset buttons
document.querySelectorAll(".ts-preset").forEach(btn => {
    btn.addEventListener("click", () => {
        const input = document.getElementById("ticker-input");
        if (input) input.value = "";
        switchTicker(btn.dataset.sym);
    });
});

// Wire free-type input
const _tickerInput = document.getElementById("ticker-input");
if (_tickerInput) {
    _tickerInput.addEventListener("keydown", e => {
        if (e.key === "Enter") {
            _tickerInput.blur();
            switchTicker(_tickerInput.value);
        }
    });
    _tickerInput.addEventListener("blur", () => {
        if (_tickerInput.value.trim()) switchTicker(_tickerInput.value);
    });
}

// Highlight active preset based on current ticker after first data load
function _updateActivePreset(sym) {
    document.querySelectorAll(".ts-preset").forEach(b => {
        b.classList.toggle("active", b.dataset.sym === sym.toUpperCase());
    });
}

// ── Candlestick Chart ────────────────────────────────────────────────────────
let _candleDays = 1;

async function updateCandleChart(days) {
    if (days !== undefined) _candleDays = days;
    const title = document.getElementById("candle-title");
    if (title) title.textContent = "⚫ Loading…";
    try {
        const res = await authFetch(`/api/candles?days=${_candleDays}`);
        const d = await res.json();
        if (d.error) { console.error("Candles:", d.error); return; }

        const times = d.candles.map(c => c.t);
        const opens = d.candles.map(c => c.o);
        const highs = d.candles.map(c => c.h);
        const lows = d.candles.map(c => c.l);
        const closes = d.candles.map(c => c.c);
        const vols = d.candles.map(c => c.v);
        const deltas = d.candles.map(c => c.d || 0);
        const ema20t = d.emas.map(e => e.t);
        const ema20v = d.emas.map(e => e.e20);
        const ema50v = d.emas.map(e => e.e50);
        const lvl = d.levels || {};
        const prof = d.delta_profile || [];

        // ── Reference style: gray background, white hollow candles ─────────
        const whiteCandle = "rgba(220,220,228,0.9)";   // hollow body
        const upFill = "rgba(220,220,228,0.08)";  // slight fill for up
        const downFill = "rgba(220,220,228,0.08)";  // same for down
        const buyBubble = "rgba(200,200,220,";
        const sellBubble = "rgba(200,200,220,";

        // ── 1. Candlestick trace ─────────────────────────────────────────────
        const candleTrace = {
            type: "candlestick",
            x: times,
            open: opens, high: highs, low: lows, close: closes,
            increasing: { line: { color: whiteCandle, width: 1 }, fillcolor: upFill },
            decreasing: { line: { color: whiteCandle, width: 1 }, fillcolor: downFill },
            name: d.ticker,
            xaxis: "x", yaxis: "y",
            hoverinfo: "x+y",
            whiskerwidth: 0,
        };

        // ── 2. Volume bubbles ────────────────────────────────────────────────
        // Size ~ sqrt(volume), capped for readability
        const maxVol = Math.max(...vols);
        const bubbleSizes = vols.map(v => Math.max(6, Math.sqrt(v / maxVol) * 48));
        const bubbleMidY = d.candles.map((c, i) => (c.h + c.l) / 2);
        const bubbleColors = d.candles.map((c, i) => {
            const b = c.c >= c.o ? buyBubble : sellBubble;
            return b + "0.35)";
        });
        const bubbleLines = d.candles.map((c) =>
            c.c >= c.o ? "rgba(31,209,122,.5)" : "rgba(224,48,96,.5)"
        );
        // Only show bubbles for top 25% volume bars (de-clutter)
        const volThresh = maxVol * 0.25;
        const bubbleY = d.candles.map((c, i) => vols[i] >= volThresh ? bubbleMidY[i] : null);

        const bubbleTrace = {
            type: "scatter", mode: "markers",
            x: times, y: bubbleY,
            marker: {
                size: bubbleSizes,
                color: bubbleColors,
                line: { color: bubbleLines, width: 1 },
                sizemode: "diameter",
            },
            name: "Volume Bubble",
            xaxis: "x", yaxis: "y",
            hovertemplate: "Vol: %{text}<extra></extra>",
            text: vols.map(v => v.toLocaleString()),
        };

        // ── 3. Volume bar panel ──────────────────────────────────────────────
        const volColors = d.candles.map((c, i) =>
            i === 0 ? upColor : (c.c >= d.candles[i - 1].c ? upColor : downColor)
        );
        const volTrace = {
            type: "bar", x: times, y: vols,
            marker: { color: volColors, opacity: 0.45 },
            name: "Volume", xaxis: "x", yaxis: "y2",
            hovertemplate: "Vol: %{y}<extra></extra>",
        };

        // ── 4. EMA overlays ──────────────────────────────────────────────────
        const ema20Trace = {
            type: "scatter", mode: "lines",
            x: ema20t, y: ema20v,
            line: { color: "rgba(40,196,248,.65)", width: 1.2 },
            name: "EMA-20", xaxis: "x", yaxis: "y", hoverinfo: "skip",
        };
        const ema50Trace = {
            type: "scatter", mode: "lines",
            x: ema20t, y: ema50v,
            line: { color: "rgba(251,120,40,.65)", width: 1.2, dash: "dot" },
            name: "EMA-50", xaxis: "x", yaxis: "y", hoverinfo: "skip",
        };

        // ── 5. Delta profile (right side — horizontal bars) ──────────────────
        const profPrices = prof.map(p => p.price);
        const profDeltas = prof.map(p => p.delta);
        const maxAbsDelta = Math.max(...profDeltas.map(Math.abs), 1);
        const profColors = profDeltas.map(d =>
            d >= 0 ? "rgba(100,120,255,.75)" : "rgba(224,48,96,.65)"
        );
        // Render as horizontal bars on xaxis3 (right panel)
        const deltaTrace = {
            type: "bar", orientation: "h",
            x: profDeltas, y: profPrices,
            marker: { color: profColors },
            name: "Delta Profile",
            xaxis: "x3", yaxis: "y",
            hovertemplate: "Price: $%{y}<br>Δ: %{x}<extra></extra>",
            width: 0.4,
        };

        // ── 6. Key level shapes ──────────────────────────────────────────────
        const shapes = [];
        const annotations = [];
        const levelDefs = [
            { key: "call_wall", color: "#7c5af7", label: "Call Wall" },
            { key: "put_wall", color: "#e03060", label: "Put Wall" },
            { key: "max_pain", color: "#e6b430", label: "Max Pain" },
        ];
        for (const { key, color, label } of levelDefs) {
            if (!lvl[key]) continue;
            shapes.push({
                type: "line", xref: "paper", yref: "y",
                x0: 0, x1: 0.78, y0: lvl[key], y1: lvl[key],
                line: { color, width: 1, dash: "dash" },
            });
            annotations.push({
                xref: "paper", yref: "y", x: 0.79, y: lvl[key],
                text: `${label}`, font: { color, size: 8, family: "JetBrains Mono" },
                showarrow: false, xanchor: "left", yanchor: "middle",
            });
        }

        // ── 7. Layout ────────────────────────────────────────────────────────
        const layout = {
            paper_bgcolor: "#1a1b22",
            plot_bgcolor: "#1a1b22",
            margin: { l: 55, r: 115, t: 10, b: 30 },
            dragmode: "pan",

            // Main time axis (70% width)
            xaxis: {
                type: "date", domain: [0, 0.78],
                rangeslider: { visible: false },
                showgrid: false,
                tickfont: { color: "#666", size: 9, family: "JetBrains Mono" },
                linecolor: "rgba(255,255,255,.06)",
                zeroline: false,
            },
            // Price y-axis (shared)
            yaxis: {
                domain: [0.22, 1],
                showgrid: false,
                tickfont: { color: "#666", size: 9, family: "JetBrains Mono" },
                tickprefix: "$",
                zeroline: false,
            },
            // Volume y-axis
            yaxis2: {
                domain: [0, 0.18],
                showgrid: false,
                tickfont: { color: "#555", size: 8, family: "JetBrains Mono" },
                zeroline: false,
            },
            // Delta profile x-axis (right 20% of width)
            xaxis3: {
                domain: [0.80, 1.0],
                anchor: "y",
                showgrid: false,
                tickfont: { color: "#555", size: 8, family: "JetBrains Mono" },
                zeroline: true, zerolinecolor: "rgba(255,255,255,.12)",
                title: { text: "Δ", font: { color: "#555", size: 9 } },
                showticklabels: false,
                range: [-maxAbsDelta * 1.15, maxAbsDelta * 1.15],
            },

            showlegend: false,
            shapes, annotations,
            font: { family: "JetBrains Mono", color: "#aaa" },
        };

        Plotly.react("candle-chart",
            [candleTrace, bubbleTrace, volTrace, ema20Trace, ema50Trace, deltaTrace],
            layout,
            { displayModeBar: false, responsive: true, scrollZoom: true }
        );

        if (title) title.textContent = `⬛ ${d.ticker}  ·  5-min  ·  ${_candleDays}D`;

    } catch (e) { console.error("Candle chart error:", e); }
}

// // Timeframe button wiring
document.querySelectorAll(".tf-btn").forEach(btn => {
    btn.addEventListener("click", () => {
        document.querySelectorAll(".tf-btn").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        updateCandleChart(parseInt(btn.dataset.days));
    });
});

// Live refresh every 10s when on the Chart tab
setInterval(() => {
    if (charts["_onChartTab"]) updateCandleChart();
}, 10_000);

// ── Theme Switcher ───────────────────────────────────────────────────────────
function applyTheme(themeId, cardEl) {
    // Set data-theme on <html>
    if (themeId === "void") {
        document.documentElement.removeAttribute("data-theme");
    } else {
        document.documentElement.setAttribute("data-theme", themeId);
    }
    // Update selected card
    document.querySelectorAll(".theme-card").forEach(c => c.classList.remove("selected"));
    if (cardEl) cardEl.classList.add("selected");
    else {
        const c = document.querySelector(`[data-theme-id="${themeId}"]`);
        if (c) c.classList.add("selected");
    }
    // Persist
    localStorage.setItem("dashTheme", themeId);
    // Sync header theme-toggle button label
    const _globalMap = { terminal: 'terminal', arctic: 'arctic' };
    const _globalTheme = _globalMap[themeId] || 'midnight';
    const _btnLabels = { midnight: '\u{1f319} Midnight', terminal: '\u{1f4bb} Terminal', arctic: '\u2600\ufe0f Arctic' };
    const _hBtn = document.getElementById('theme-btn');
    if (_hBtn) _hBtn.textContent = _btnLabels[_globalTheme] || _btnLabels.midnight;

    // Re-apply Chart.js default colors so all charts pick up the new theme
    if (typeof Chart !== "undefined") {
        Chart.defaults.color = TEXT;
        Chart.defaults.borderColor = GRID;
        // Destroy + rebuild all active charts from last data
        Object.keys(charts).forEach(id => {
            const ch = charts[id];
            if (ch && ch.canvas) {
                // Update tooltip bg and gridline color for all charts in-place
                try {
                    if (ch.options?.plugins?.tooltip) {
                        ch.options.plugins.tooltip.backgroundColor = themeBg1();
                        ch.options.plugins.tooltip.borderColor = themeAccent() + "44";
                    }
                    if (ch.options?.scales?.x?.grid) ch.options.scales.x.grid.color = GRID;
                    if (ch.options?.scales?.y?.grid) ch.options.scales.y.grid.color = GRID;
                    ch.update("none");
                } catch (e) { }
            }
        });
    }

    // Rebuild Plotly plots with new theme bg
    if (typeof Plotly !== "undefined") {
        const bg = themeBg1();
        const paper = themeBg2();
        ["iv-surface", "topology-3d", "entropy-3d"].forEach(id => {
            const el = document.getElementById(id);
            if (el && el._fullLayout) {
                Plotly.relayout(id, {
                    paper_bgcolor: paper,
                    plot_bgcolor: bg,
                    "scene.bgcolor": bg,
                }).catch(() => { });
            }
        });
    }

    // Rebuild heatmaps if data is loaded
    const d = window._lastHmBuildData;
    const spot = window._lastHmSpot;
    if (d && spot) {
        try { if (d.dex_hm) buildHeatmap("hm-dex", d.dex_hm, hmCmap("dex"), spot); } catch (e) { }
        try { if (d.gex_hm) buildHeatmap("hm-gex", d.gex_hm, hmCmap("gex"), spot); } catch (e) { }
        try { if (d.vannex_hm) buildHeatmap("hm-vannex", d.vannex_hm, hmCmap("vannex"), spot); } catch (e) { }
        try { if (d.cex_hm) buildHeatmap("hm-cex", d.cex_hm, hmCmap("cex"), spot); } catch (e) { }
    }
}

// Auto-restore theme on load
(function () {
    const saved = localStorage.getItem("dashTheme") || "void";
    applyTheme(saved, null);
})();


// ── IV Surface Colorscale Palette ─────────────────────────────────────────
const IV_COLORSCALES = {
    quant: [
        [0.00, 'rgb(2,2,20)'], [0.08, 'rgb(10,15,110)'], [0.18, 'rgb(0,60,200)'],
        [0.30, 'rgb(0,160,255)'], [0.42, 'rgb(0,230,220)'], [0.54, 'rgb(20,230,120)'],
        [0.65, 'rgb(180,255,0)'], [0.75, 'rgb(255,210,0)'], [0.86, 'rgb(255,90,10)'],
        [1.00, 'rgb(220,0,20)'],
    ],
    thermal: [
        [0.00, 'rgb(3,3,20)'], [0.15, 'rgb(30,10,80)'], [0.30, 'rgb(100,15,120)'],
        [0.45, 'rgb(180,30,60)'], [0.60, 'rgb(230,80,20)'], [0.75, 'rgb(250,160,0)'],
        [0.90, 'rgb(255,230,40)'], [1.00, 'rgb(255,255,200)'],
    ],
    ice: [
        [0.00, 'rgb(2,2,10)'], [0.15, 'rgb(5,20,60)'], [0.30, 'rgb(10,60,120)'],
        [0.45, 'rgb(20,110,170)'], [0.60, 'rgb(60,170,210)'], [0.75, 'rgb(140,210,230)'],
        [0.90, 'rgb(220,240,250)'], [1.00, 'rgb(255,255,255)'],
    ],
    magma: [
        [0.00, 'rgb(0,0,4)'], [0.13, 'rgb(28,16,68)'], [0.25, 'rgb(79,18,123)'],
        [0.38, 'rgb(129,37,129)'], [0.50, 'rgb(181,54,122)'], [0.63, 'rgb(229,89,100)'],
        [0.75, 'rgb(251,135,97)'], [0.88, 'rgb(254,194,140)'], [1.00, 'rgb(252,253,191)'],
    ],
    plasma: [
        [0.00, 'rgb(13,8,135)'], [0.13, 'rgb(75,3,161)'], [0.25, 'rgb(125,3,168)'],
        [0.38, 'rgb(168,34,150)'], [0.50, 'rgb(203,70,121)'], [0.63, 'rgb(229,107,93)'],
        [0.75, 'rgb(248,148,65)'], [0.88, 'rgb(253,195,40)'], [1.00, 'rgb(240,249,33)'],
    ],
    cividis: [
        [0.00, 'rgb(0,32,76)'], [0.13, 'rgb(0,51,96)'], [0.25, 'rgb(54,77,107)'],
        [0.38, 'rgb(90,100,109)'], [0.50, 'rgb(122,122,108)'], [0.63, 'rgb(156,146,98)'],
        [0.75, 'rgb(192,170,78)'], [0.88, 'rgb(228,197,45)'], [1.00, 'rgb(253,231,37)'],
    ],
    inferno: [
        [0.00, 'rgb(0,0,4)'], [0.13, 'rgb(31,12,72)'], [0.25, 'rgb(85,15,109)'],
        [0.38, 'rgb(136,34,106)'], [0.50, 'rgb(186,54,85)'], [0.63, 'rgb(227,89,51)'],
        [0.75, 'rgb(249,140,10)'], [0.88, 'rgb(249,201,50)'], [1.00, 'rgb(252,255,164)'],
    ],
    turbo: [
        [0.00, 'rgb(48,18,59)'], [0.13, 'rgb(65,68,180)'], [0.25, 'rgb(35,137,222)'],
        [0.38, 'rgb(15,192,187)'], [0.50, 'rgb(57,230,104)'], [0.63, 'rgb(163,241,31)'],
        [0.75, 'rgb(239,210,17)'], [0.88, 'rgb(249,138,13)'], [1.00, 'rgb(122,4,3)'],
    ],
    viridis: [
        [0.00, 'rgb(68,1,84)'], [0.13, 'rgb(72,36,117)'], [0.25, 'rgb(65,68,135)'],
        [0.38, 'rgb(53,95,141)'], [0.50, 'rgb(42,120,142)'], [0.63, 'rgb(33,145,140)'],
        [0.75, 'rgb(53,183,121)'], [0.88, 'rgb(109,205,89)'], [1.00, 'rgb(253,231,37)'],
    ],
    electric: [
        [0.00, 'rgb(0,0,0)'], [0.15, 'rgb(30,0,100)'], [0.30, 'rgb(120,0,200)'],
        [0.45, 'rgb(200,0,255)'], [0.60, 'rgb(255,30,200)'], [0.75, 'rgb(255,100,100)'],
        [0.90, 'rgb(255,200,50)'], [1.00, 'rgb(255,255,255)'],
    ],
};

const IV_PALETTE_NAMES = Object.keys(IV_COLORSCALES);
let _ivPaletteIdx = +(localStorage.getItem('ivSurfacePalette') || 0);

function getIVColorscale() {
    const name = IV_PALETTE_NAMES[_ivPaletteIdx % IV_PALETTE_NAMES.length];
    return { scale: IV_COLORSCALES[name], name };
}

function cycleIVPalette() {
    const existing = document.getElementById('iv-picker-popup');
    if (existing) { existing.remove(); return; }

    const btnEl = document.querySelector('[data-iv-palette]');
    if (!btnEl) return;

    const popup = document.createElement('div');
    popup.id = 'iv-picker-popup';
    popup.className = 'hm-picker-popup';

    IV_PALETTE_NAMES.forEach((name, idx) => {
        const row = document.createElement('div');
        row.className = 'hm-picker-row' + (_ivPaletteIdx === idx ? ' hm-picker-active' : '');

        const cv = document.createElement('canvas');
        cv.width = 80; cv.height = 12;
        cv.className = 'hm-picker-swatch';
        const ctx = cv.getContext('2d');
        const stops = IV_COLORSCALES[name];
        const grad = ctx.createLinearGradient(0, 0, 80, 0);
        stops.forEach(([pos, color]) => grad.addColorStop(pos, color));
        ctx.fillStyle = grad;
        ctx.fillRect(0, 0, 80, 12);

        const lbl = document.createElement('span');
        lbl.className = 'hm-picker-label';
        lbl.textContent = name;

        row.append(cv, lbl);
        row.onclick = () => {
            _ivPaletteIdx = idx;
            localStorage.setItem('ivSurfacePalette', idx);
            btnEl.textContent = '\u29e1 ' + name;
            popup.remove();
            const surfaceDiv = document.getElementById('iv-surface');
            if (surfaceDiv && surfaceDiv.data && surfaceDiv.data[0]) {
                Plotly.restyle('iv-surface', { colorscale: [IV_COLORSCALES[name]] }, [0]);
            }
        };
        popup.appendChild(row);
    });

    const rect = btnEl.getBoundingClientRect();
    const vpH = window.innerHeight, vpW = window.innerWidth;
    const estH = IV_PALETTE_NAMES.length * 28 + 12;
    const estW = 185;
    const top = (vpH - rect.bottom - 8) >= estH ? rect.bottom + 4 : rect.top - estH - 4;
    const left = Math.min(rect.left, vpW - estW - 8);
    popup.style.cssText = `position:fixed;top:${top}px;left:${left}px;z-index:9999`;
    document.body.appendChild(popup);

    setTimeout(() => {
        const outside = (e) => {
            if (!popup.contains(e.target) && !btnEl.contains(e.target)) {
                popup.remove();
                document.removeEventListener('mousedown', outside);
            }
        };
        document.addEventListener('mousedown', outside);
    }, 0);
}

// ── Heatmap per-panel colormap state ─────────────────────────────────────────
const HM_PALETTES = {
    dex: ["dex", "fire", "rdbu", "cyberpunk", "sunset", "spectral", "lime", "solar", "copper", "plasma"],
    gex: ["gex", "turbo", "fire", "sunset", "twilight", "rdbu", "lava", "spectral", "frost", "inferno"],
    vannex: ["vannex", "twilight", "cyberpunk", "spectral", "rdbu", "ice", "frost", "lime", "mono", "sand"],
    cex: ["cex", "twilight", "acid", "fire", "cyberpunk", "spectral", "rdbu", "plasma", "lava", "sunset"],
};
const HM_CMAP_STATE = { dex: 0, gex: 0, vannex: 0, cex: 0 };

// Restore saved state
Object.keys(HM_CMAP_STATE).forEach(k => {
    const saved = localStorage.getItem("hmCmap_" + k);
    if (saved !== null) HM_CMAP_STATE[k] = parseInt(saved) || 0;
});

// Open a visual palette picker popup with gradient swatches
function cycleHmPalette(hmId, data, spot) {
    // Toggle: if same popup already open, close it
    const existing = document.getElementById('hm-picker-popup');
    const samePanel = existing?.dataset.hmId === hmId;
    if (existing) existing.remove();
    if (samePanel) return;

    const btnEl = document.querySelector(`[data-hm-picker="${hmId}"]`);
    const palettes = HM_PALETTES[hmId];
    if (!palettes || !btnEl) return;

    const popup = document.createElement('div');
    popup.id = 'hm-picker-popup';
    popup.dataset.hmId = hmId;
    popup.className = 'hm-picker-popup';

    palettes.forEach((cmapKey, idx) => {
        const row = document.createElement('div');
        row.className = 'hm-picker-row' + (HM_CMAP_STATE[hmId] === idx ? ' hm-picker-active' : '');

        // Mini gradient swatch from CMAPS stops
        const cv = document.createElement('canvas');
        cv.width = 80; cv.height = 12;
        cv.className = 'hm-picker-swatch';
        const ctx2 = cv.getContext('2d');
        const stops = CMAPS[cmapKey] || CMAPS.viridis;
        const grad = ctx2.createLinearGradient(0, 0, 80, 0);
        stops.forEach((rgb, i) =>
            grad.addColorStop(i / (stops.length - 1), `rgb(${rgb[0]},${rgb[1]},${rgb[2]})`));
        ctx2.fillStyle = grad;
        ctx2.fillRect(0, 0, 80, 12);

        const lbl = document.createElement('span');
        lbl.className = 'hm-picker-label';
        lbl.textContent = cmapKey;

        row.append(cv, lbl);
        row.onclick = () => {
            HM_CMAP_STATE[hmId] = idx;
            localStorage.setItem('hmCmap_' + hmId, idx);
            if (data) buildHeatmap('hm-' + hmId, data, cmapKey, spot);
            if (btnEl) btnEl.textContent = '\u29e1 ' + cmapKey;
            popup.remove();
        };
        popup.appendChild(row);
    });

    // Smart position: flip up if not enough space below; clamp right edge
    const rect = btnEl.getBoundingClientRect();
    const vpH = window.innerHeight, vpW = window.innerWidth;
    const estH = palettes.length * 28 + 12;
    const estW = 185;
    const top = (vpH - rect.bottom - 8) >= estH ? rect.bottom + 4 : rect.top - estH - 4;
    const left = Math.min(rect.left, vpW - estW - 8);
    popup.style.cssText = `position:fixed;top:${top}px;left:${left}px;z-index:9999`;
    document.body.appendChild(popup);

    // Close on outside mousedown
    setTimeout(() => {
        const outside = (e) => {
            if (!popup.contains(e.target) && !btnEl.contains(e.target)) {
                popup.remove();
                document.removeEventListener('mousedown', outside);
            }
        };
        document.addEventListener('mousedown', outside);
    }, 0);
}


// Return current cmapKey for a heatmap
function hmCmap(hmId) {
    const p = HM_PALETTES[hmId];
    return p ? p[HM_CMAP_STATE[hmId] ?? 0] : hmId;
}

// Store last data for picker use
let _lastHmBuildData = null;
// ── Display Settings ─────────────────────────────────────────────────────────
function _setPillGroup(selector, value) {
    document.querySelectorAll(selector).forEach(b => {
        b.classList.toggle("active", b.dataset[Object.keys(b.dataset).find(k => k.endsWith("Opt"))] === value);
    });
}

function setDensity(val, _btn) {
    const html = document.documentElement;
    if (val === "comfortable") html.removeAttribute("data-density");
    else html.setAttribute("data-density", val === "compact" ? "compact" : "spacious");
    _setPillGroup("[data-density-opt]", val);
    localStorage.setItem("dashDensity", val);
}
function setAnim(val, _btn) {
    const html = document.documentElement;
    ["fast", "off"].forEach(v => html.removeAttribute("data-anim"));
    if (val !== "normal") html.setAttribute("data-anim", val);
    _setPillGroup("[data-anim-opt]", val);
    localStorage.setItem("dashAnim", val);
}
function setNumFmt(val, _btn) {
    _setPillGroup("[data-numfmt-opt]", val);
    localStorage.setItem("dashNumFmt", val);
    window._numFmt = val;
}
function setLineW(val, _btn) {
    document.documentElement.setAttribute("data-line-w", val);
    _setPillGroup("[data-linew-opt]", val);
    localStorage.setItem("dashLineW", val);
    window._lineW = parseInt(val);
}
function setAutoTab(val, _btn) {
    _setPillGroup("[data-autotab-opt]", val);
    localStorage.setItem("dashAutoTab", val);
}

// Restore display settings on load
(function restoreDisplaySettings() {
    const density = localStorage.getItem("dashDensity") || "comfortable";
    const anim = localStorage.getItem("dashAnim") || "normal";
    const numFmt = localStorage.getItem("dashNumFmt") || "auto";
    const lineW = localStorage.getItem("dashLineW") || "2";
    const autoTab = localStorage.getItem("dashAutoTab") || "greeks";
    setDensity(density);
    setAnim(anim);
    setNumFmt(numFmt);
    setLineW(lineW);
    // Auto-open tab
    const autoBtn = document.querySelector(`[data-tab="${autoTab}"]`);
    if (autoBtn && !document.querySelector(".tab.active")) autoBtn.click();
    _setPillGroup("[data-autotab-opt]", autoTab);
})();

// Boot â€” use tracked timer so loadSettings() can override with cfg interval
update();
setRefreshInterval(REFRESH_MS);
setInterval(() => { if (charts["chart-anomaly"]) updateAnomaly(); }, 5 * 60 * 1000);
setInterval(updateMacro, 5 * 60 * 1000); // refresh macro every 5 min

// â”€â”€ Volatility: IV Surface + IV Skew â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
var volLoaded = false;

async function updateVolatility() {
    try {
        const res = await authFetch(VOL_URL);
        const d = await res.json();
        if (d.error) { console.error("Vol:", d.error); return; }

        const strikes = d.strikes;
        const exps = d.expirations; // [{label, dte}]
        const surface = d.surface;     // [{label, dte, ivs:[]}]

        // ── IV Surface (Plotly 3D) ── Moneyness x DTE x IV%
        const spot = d.spot || 0;
        const ticker = d.ticker || 'QQQ';
        // X = Moneyness % (strike/spot*100), Y = numeric DTE, Z = IV%
        const x = strikes.map(s => +((s / spot) * 100).toFixed(1));
        const y = exps.map(e => e.dte);
        // Smoothed IV: replace nulls (< 1% or > 80%) with nearest valid value
        // to avoid Plotly rendering disconnected spiky faces
        const clamp = v => {
            const pct = +(v * 100).toFixed(2);
            return (pct > 1 && pct <= 75) ? pct : undefined;
        };
        const fillIVRow = row => {
            const raw = row.map(clamp);
            // Forward-fill then backward-fill undefined values
            let last = null;
            const fwd = raw.map(v => { if (v !== undefined) last = v; return last; });
            last = null;
            for (let i = fwd.length - 1; i >= 0; i--) {
                if (fwd[i] !== null) last = fwd[i];
                else if (last !== null) fwd[i] = last;
            }
            // Any remaining nulls → median of row
            const valid = fwd.filter(v => v !== null);
            const median = valid.sort((a, b) => a - b)[Math.floor(valid.length / 2)] || 20;
            return fwd.map(v => v !== null ? v : median);
        };
        const z = surface.map(s => fillIVRow(s.ivs));
        // Compute a sensible z-max: 95th percentile of all IV values
        const allZ = z.flat().filter(v => v > 0).sort((a, b) => a - b);
        const zMax95 = allZ[Math.floor(allZ.length * 0.95)] || 50;
        const zRangeTop = Math.min(75, Math.ceil(zMax95 / 5) * 5 + 5);

        // ── Quant terminal style IV surface ─────────────────────────────────
        // Read theme accent for ATM line color
        const _accent = getComputedStyle(document.documentElement)
            .getPropertyValue('--accent').trim() || '#7c5af7';
        const _bg1 = getComputedStyle(document.documentElement)
            .getPropertyValue('--bg1').trim() || '#030305';

        const surfaceTrace = {
            type: 'surface',
            x, y, z,
            colorscale: getIVColorscale().scale,
            // Surface contours: floor projection + Z-level lines for depth cues
            contours: {
                x: { show: false },
                y: { show: false },
                z: {
                    show: true,
                    usecolormap: true,
                    highlightcolor: 'rgba(255,255,255,.5)',
                    project: { z: true },   // project contours onto floor
                    width: 1.5,
                    start: 5, end: 60, size: 5,
                },
            },
            // Lighting: strong diffuse, low roughness for metallic terminal feel
            lighting: {
                ambient: 0.6,
                diffuse: 0.9,
                roughness: 0.25,
                specular: 0.8,
                fresnel: 0.15,
            },
            lightposition: { x: -1000, y: -500, z: 2000 },
            opacity: 1.0,   // must be 1.0 — Plotly alpha-blending at <1 causes see-through faces
            showscale: true,
            colorbar: {
                title: {
                    text: 'IV (%)',
                    font: { color: 'rgba(180,190,230,.9)', size: 11, family: 'JetBrains Mono' },
                    side: 'right',
                },
                tickfont: { color: 'rgba(140,155,195,.85)', size: 10, family: 'JetBrains Mono' },
                tickformat: '.0f',
                thickness: 10,
                len: 0.5,
                bgcolor: 'rgba(0,0,0,0)',
                bordercolor: 'rgba(255,255,255,.06)',
                borderwidth: 1,
                tickvals: [5, 10, 15, 20, 30, 40, 50],
                x: 0.97,
                outlinewidth: 0,
            },
            // hovertext: Surface3d does NOT support customdata indexing, use pre-built text
            hovertext: surface.map(s => strikes.map((k, ki) =>
                `Strike: $${k.toFixed(0)}\nMoneyness: ${((k / spot) * 100).toFixed(1)}%\nDTE: ${s.dte}d\nIV: ${(s.ivs[ki] != null ? s.ivs[ki].toFixed(1) : '—')}%`
            )),
            hoverinfo: 'text',
        };

        const surfaceLayout = {
            paper_bgcolor: _bg1,
            plot_bgcolor: _bg1,
            title: false,
            annotations: [{
                text: ticker + '  ·  SPOT $' + spot.toFixed(2),
                font: { size: 11, family: 'JetBrains Mono, monospace', color: 'rgba(140,160,220,.7)' },
                x: 0.5, y: 1.02, xref: 'paper', yref: 'paper',
                showarrow: false, xanchor: 'center', yanchor: 'bottom',
            }],
            scene: {
                bgcolor: _bg1,
                // Dark axis background planes — gives the "terminal grid" feel
                xaxis: {
                    title: { text: 'MONEYNESS (%)', font: { color: 'rgba(100,130,180,.8)', size: 11, family: 'JetBrains Mono' } },
                    tickfont: { color: 'rgba(100,130,180,.75)', size: 10, family: 'JetBrains Mono' },
                    gridcolor: 'rgba(50,70,120,.35)',
                    showgrid: true,
                    zeroline: true,
                    zerolinecolor: 'rgba(56,189,248,.6)',
                    zerolinewidth: 2,
                    showbackground: true,
                    backgroundcolor: 'rgba(2,5,15,.5)',
                    dtick: 5,
                    ticksuffix: '%',
                },
                yaxis: {
                    title: { text: 'DTE (days)', font: { color: 'rgba(100,130,180,.8)', size: 11, family: 'JetBrains Mono' } },
                    tickfont: { color: 'rgba(100,130,180,.75)', size: 10, family: 'JetBrains Mono' },
                    gridcolor: 'rgba(50,70,120,.35)',
                    showgrid: true,
                    showbackground: true,
                    backgroundcolor: 'rgba(2,5,15,.45)',
                },
                zaxis: {
                    title: { text: 'IV (%)', font: { color: 'rgba(100,130,180,.8)', size: 11, family: 'JetBrains Mono' } },
                    tickfont: { color: 'rgba(100,130,180,.75)', size: 10, family: 'JetBrains Mono' },
                    gridcolor: 'rgba(50,70,120,.35)',
                    showgrid: true,
                    showbackground: true,
                    backgroundcolor: 'rgba(2,5,15,.4)',
                    range: [0, zRangeTop],
                    dtick: 10,
                    ticksuffix: '%',
                },
                // Slightly elevated, angled for best vol surface readability
                camera: {
                    eye: { x: -1.55, y: -2.1, z: 0.85 },
                    center: { x: 0, y: 0, z: -0.1 },
                    up: { x: 0, y: 0, z: 1 },
                },
                aspectmode: 'manual',
                aspectratio: { x: 1.5, y: 1.6, z: 0.85 },
            },
            autosize: true,
            margin: { l: 0, r: 60, t: 42, b: 0 },
            font: { family: 'JetBrains Mono, monospace', color: 'rgba(180,190,230,.9)', size: 11 },
        };

        // ATM curtain — horizontal dashed line along DTE axis at moneyness=100 (floor plane)
        const atmColor = _accent || 'rgba(56,189,248,.9)';
        const yFull = [...y].sort((a, b) => a - b);
        const spotTrace = {
            type: 'scatter3d',
            mode: 'lines+text',
            // A dashed floor line spanning the full DTE range at moneyness=100
            x: [100, 100],
            y: [yFull[0], yFull[yFull.length - 1]],
            z: [0, 0],
            line: { color: atmColor, width: 6, dash: 'dash' },
            text: ['', `ATM  $${spot.toFixed(0)}`],
            textposition: 'top center',
            textfont: { color: atmColor, size: 11, family: 'JetBrains Mono, monospace' },
            name: 'ATM', showlegend: false, hoverinfo: 'skip',
        };

        Plotly.react('iv-surface', [surfaceTrace, spotTrace], surfaceLayout, {
            displayModeBar: true,
            modeBarButtonsToRemove: ['toImage', 'sendDataToCloud', 'select2d', 'lasso2d',
                'toggleSpikelines', 'hoverClosestCartesian', 'hoverCompareCartesian'],
            displaylogo: false,
            responsive: true,
            scrollZoom: true,
        });


        // â”€â”€ IV Skew (Chart.js, nearest expiration) â”€â”€
        const nearest = surface[0]; // first expiry = shortest dated
        const ivPct = nearest.ivs.map(v => +(v * 100).toFixed(2));

        // Detect skew signal
        const midIdx = Math.floor(strikes.length / 2);
        const otmPutIV = ivPct.slice(0, midIdx - 1);   // below ATM
        const otmCallIV = ivPct.slice(midIdx + 2);       // above ATM
        const putAvg = otmPutIV.reduce((a, b) => a + b, 0) / (otmPutIV.length || 1);
        const callAvg = otmCallIV.reduce((a, b) => a + b, 0) / (otmCallIV.length || 1);
        const skewRatio = putAvg / callAvg;
        let signal, sigColor;
        if (skewRatio > 1.5) { signal = "ðŸ”´  STRONG PUT SKEW"; sigColor = RED; }
        else if (skewRatio > 1.15) { signal = "ðŸŸ¡  MILD PUT SKEW"; sigColor = YELLOW; }
        else if (skewRatio > 0.85) { signal = "ðŸŸ¢  NORMAL VOL"; sigColor = GREEN; }
        else { signal = "ðŸ”µ  CALL SKEW"; sigColor = CYAN; }

        const sigEl = document.getElementById("skew-signal");
        const labelEl = document.getElementById("skew-label");
        if (sigEl) { sigEl.textContent = signal; sigEl.style.color = sigColor; }
        if (labelEl) labelEl.textContent = nearest.label + " (" + nearest.dte + "d)";

        // Spot line index
        const spotStrike = d.spot;
        const spotAnnot = strikes.reduce((best, s, i) =>
            Math.abs(s - spotStrike) < Math.abs(strikes[best] - spotStrike) ? i : best, 0);

        if (charts["chart-skew"]) { charts["chart-skew"].destroy(); }
        const skewCtx = document.getElementById("chart-skew");
        charts["chart-skew"] = new Chart(skewCtx.getContext("2d"), {
            type: "line",
            data: {
                labels: strikes.map(s => "$" + s.toFixed(0)),
                datasets: [
                    {
                        label: "IV %",
                        data: ivPct,
                        borderColor: "#e8e8f8",
                        borderWidth: 2,
                        pointRadius: 3,
                        pointBackgroundColor: ivPct.map((v, i) => {
                            if (i === spotAnnot) return CYAN;
                            return i < midIdx ? `rgba(232,67,90,${0.3 + 0.5 * (ivPct[0] - v) / (ivPct[0] - ivPct[midIdx] || 1)})`
                                : "rgba(255,255,255,0.2)";
                        }),
                        pointBorderColor: "transparent",
                        tension: 0.35,
                        fill: false,
                    },
                    {
                        // Spot vertical line as a scatter point
                        label: "Spot",
                        data: strikes.map((_, i) => i === spotAnnot ? ivPct[i] : null),
                        borderColor: RED,
                        backgroundColor: RED,
                        pointRadius: 7,
                        pointStyle: "line",
                        showLine: false,
                    }
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false, animation: { duration: 300 },
                plugins: {
                    legend: {
                        display: true, position: "top", align: "end",
                        labels: { color: TEXT, font: { size: 11 }, boxWidth: 10, padding: 12 }
                    },
                    tooltip: {
                        backgroundColor: "#131620",
                        borderColor: themeAccent() + "22", borderWidth: 1,
                        titleColor: CYAN, bodyColor: TEXT, padding: 10,
                        callbacks: { label: c => c.raw !== null ? `IV: ${c.raw.toFixed(2)}%` : "" }
                    },
                    annotation: {},  // placeholder
                },
                scales: {
                    x: {
                        ticks: { color: TEXT, font: { family: "JetBrains Mono", size: 9 }, maxTicksLimit: 14 },
                        grid: { color: GRID },
                        border: { color: "rgba(255,255,255,.06)" },
                    },
                    y: {
                        ticks: {
                            color: TEXT, font: { size: 10 },
                            callback: v => v.toFixed(0) + "%"
                        },
                        grid: { color: GRID },
                        border: { color: "rgba(255,255,255,.06)" },
                        title: { display: true, text: "IV %", color: TEXT, font: { size: 11 } },
                    }
                }
            },
            plugins: [{
                id: "spotLine",
                afterDraw(chart) {
                    const { ctx, chartArea, scales } = chart;
                    const xPos = scales.x.getPixelForValue(spotAnnot);
                    ctx.save();
                    ctx.beginPath();
                    ctx.moveTo(xPos, chartArea.top);
                    ctx.lineTo(xPos, chartArea.bottom);
                    ctx.strokeStyle = "rgba(232,67,90,0.65)";
                    ctx.lineWidth = 1.5;
                    ctx.setLineDash([4, 4]);
                    ctx.stroke();
                    ctx.restore();
                }
            }]
        });

        volLoaded = true;

        // Also fetch vol stats panel (non-blocking)
        buildVolStats();

    } catch (e) { console.error("Volatility error:", e); }
}

// â”€â”€ IVR + HV Stats Panel â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function buildVolStats() {
    try {
        const res = await authFetch("/api/vol_stats");
        const d = await res.json();
        if (d.error) return;

        const set = (id, txt, color) => {
            const el = document.getElementById(id);
            if (!el) return;
            el.textContent = txt;
            if (color) el.style.color = color;
        };

        // HV-Rank â€” purple = elevated IV, green = depressed IV
        const ivrColor = d.ivr >= 70 ? "#a78bfa" : d.ivr <= 30 ? "#2ecc8a" : "#f5c542";
        set("stat-ivr", d.ivr.toFixed(1), ivrColor);
        // sub-label is now static HTML: "ATM IV vs 1yr HV range"

        // ATM IV
        set("stat-atm-iv", d.atm_iv.toFixed(1) + "%", "#60a5fa");

        // HV trio
        set("stat-hvs", `${d.hv10}%  /  ${d.hv20}%  /  ${d.hv30}%`);

        // Vol premium (IV - HV30)
        const vpColor = d.vol_premium > 2 ? "#3b1278" : d.vol_premium < -2 ? "#f5ff30" : "#f5c542";
        set("stat-vp", (d.vol_premium >= 0 ? "+" : "") + d.vol_premium.toFixed(2) + "%", vpColor);

        // VIX term structure — always show a value, even when N/A
        const DIM = "#42506a";
        set("stat-vix9d", d.vix9d != null ? "9D " + d.vix9d : "9D N/A", d.vix9d != null ? null : DIM);
        set("stat-vix", d.vix != null ? "VIX " + d.vix : "VIX N/A", d.vix != null ? null : DIM);
        set("stat-vix3m", d.vix3m != null ? "3M " + d.vix3m : "3M N/A", d.vix3m != null ? null : DIM);
        const tsMap = {
            "CONTANGO": ["#2ecc8a", "Calm (contango)"],
            "BACKWARDATION": ["#e8435a", "Stressed (backwardation)"],
            "MIXED": ["#f5c542", "Mixed"],
        };
        if (d.ts_shape in tsMap) {
            set("stat-ts-shape", tsMap[d.ts_shape][1], tsMap[d.ts_shape][0]);
        } else {
            set("stat-ts-shape", d.ts_shape, DIM);  // e.g. "N/A (no VIX9D, VIX3M)"
        }

        // Regime badge
        const regMap = { "SELL VOL": "#a78bfa", "BUY VOL": "#2ecc8a", "NEUTRAL": "#f5c542" };
        set("stat-regime", d.regime, regMap[d.regime] || "#8899aa");

        // ── Realized Vol chart (HV10 / HV20 / HV30 + ATM IV) ─────────────────
        const hc = d.hv30_chart;
        if (!hc || !hc.dates.length) return;

        // Build lookup maps for HV10 / HV20 so we can align to HV30 date axis
        const hv20map = {};
        if (d.hv20_chart) d.hv20_chart.dates.forEach((dt, i) => hv20map[dt] = d.hv20_chart.values[i]);
        const hv10map = {};
        if (d.hv10_chart) d.hv10_chart.dates.forEach((dt, i) => hv10map[dt] = d.hv10_chart.values[i]);

        const hv20Aligned = hc.dates.map(dt => hv20map[dt] ?? null);
        const hv10Aligned = hc.dates.map(dt => hv10map[dt] ?? null);
        const ivLine = new Array(hc.dates.length).fill(d.atm_iv);

        if (charts["chart-hv"]) charts["chart-hv"].destroy();
        const canvas = document.getElementById("chart-hv");
        if (!canvas) return;

        charts["chart-hv"] = new Chart(canvas.getContext("2d"), {
            type: "line",
            data: {
                labels: hc.dates,
                datasets: [
                    {
                        label: "HV10",
                        data: hv10Aligned,
                        borderColor: "#fb923c",
                        backgroundColor: "transparent",
                        fill: false,
                        tension: 0.35,
                        pointRadius: 0,
                        borderWidth: 1.5,
                        spanGaps: true,
                    },
                    {
                        label: "HV20",
                        data: hv20Aligned,
                        borderColor: "#2ecc8a",
                        backgroundColor: "transparent",
                        fill: false,
                        tension: 0.35,
                        pointRadius: 0,
                        borderWidth: 1.5,
                        spanGaps: true,
                    },
                    {
                        label: "HV30",
                        data: hc.values,
                        borderColor: "#60a5fa",
                        backgroundColor: "rgba(96,165,250,.08)",
                        fill: true,
                        tension: 0.35,
                        pointRadius: 0,
                        borderWidth: 2,
                    },
                    {
                        label: "ATM IV",
                        data: ivLine,
                        borderColor: "#a78bfa",
                        borderDash: [5, 4],
                        borderWidth: 1.5,
                        pointRadius: 0,
                        fill: false,
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 400 },
                plugins: {
                    legend: { display: true, labels: { color: TEXT, font: { size: 11 }, boxWidth: 14 } },
                    tooltip: {
                        backgroundColor: themeBg1(),
                        borderColor: themeAccent() + "33",
                        borderWidth: 1,
                        titleColor: "#60a5fa",
                        bodyColor: "#8899aa",
                        callbacks: { label: c => ` ${c.dataset.label}: ${c.raw.toFixed(2)}%` }
                    }
                },
                scales: {
                    x: {
                        ticks: { color: "#6b7a99", font: { size: 9 }, maxTicksLimit: 8 },
                        grid: { color: "rgba(255,255,255,.03)" },
                    },
                    y: {
                        ticks: { color: TEXT, font: { size: 10 }, callback: v => v + "%" },
                        grid: { color: "rgba(255,255,255,.04)" },
                    }
                }
            }
        });
        // ── Kurtosis chart ─────────────────────────────────────────────────
        const kc = d.kurt_chart;
        if (kc && kc.dates.length) {
            const kNow = kc.current;
            const kBadge = document.getElementById("kurt-badge");
            if (kBadge) {
                const kLabel = kNow > 2 ? "FAT TAILS 🔴" : kNow > 0.5 ? "ELEVATED" : kNow < -0.5 ? "THIN TAILS" : "NORMAL";
                const kColor = kNow > 2 ? "#e8435a" : kNow > 0.5 ? "#f5c542" : kNow < -0.5 ? "#60a5fa" : "#2ecc8a";
                kBadge.textContent = `${kNow > 0 ? "+" : ""}${kNow}  ${kLabel}`;
                kBadge.style.color = kColor;
                kBadge.style.background = kColor + "22";
            }

            if (charts["chart-kurtosis"]) charts["chart-kurtosis"].destroy();
            const kCanvas = document.getElementById("chart-kurtosis");
            if (kCanvas) {
                charts["chart-kurtosis"] = new Chart(kCanvas.getContext("2d"), {
                    type: "line",
                    data: {
                        labels: kc.dates,
                        datasets: [
                            {
                                label: "5D Intraday Kurtosis (78-bar rolling)",
                                data: kc.values,
                                borderColor: "#f5c542",
                                borderWidth: 1.8,
                                pointRadius: 0,
                                fill: {
                                    target: { value: 0 },
                                    above: "rgba(245,197,66,0.18)",   // fat tails → amber
                                    below: "rgba(96,165,250,0.15)",   // thin tails → blue
                                },
                                tension: 0.35,
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        animation: { duration: 400 },
                        plugins: {
                            legend: { display: false },
                            tooltip: {
                                backgroundColor: themeBg1(),
                                borderColor: themeAccent() + "33",
                                borderWidth: 1,
                                titleColor: "#f5c542",
                                bodyColor: "#8899aa",
                                callbacks: { label: c => ` 5D Kurt: ${c.raw.toFixed(3)}` }
                            },
                            annotation: {
                                annotations: {
                                    zeroLine: {
                                        type: "line", yMin: 0, yMax: 0,
                                        borderColor: "rgba(255,255,255,0.25)",
                                        borderWidth: 1, borderDash: [4, 4],
                                    }
                                }
                            }
                        },
                        scales: {
                            x: {
                                ticks: { color: "#6b7a99", font: { size: 9 }, maxTicksLimit: 8 },
                                grid: { color: "rgba(255,255,255,.03)" },
                            },
                            y: {
                                ticks: { color: TEXT, font: { size: 10 }, callback: v => v.toFixed(1) },
                                grid: { color: "rgba(255,255,255,.04)" },
                            }
                        }
                    }
                });
            }
        }

    } catch (e) { console.error("vol_stats error:", e); }
}

// â”€â”€ Macro Economics â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const MACRO_URL = "/api/macro";

async function updateMacro() {
    try {
        const res = await authFetch(MACRO_URL);
        const d = await res.json();
        if (d.error) { console.error("Macro:", d.error); return; }

        // â”€â”€ News â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        const newsEl = document.getElementById("macro-news");
        if (newsEl) {
            newsEl.innerHTML = d.news.map(n => `
              <div class="news-item">
                <span class="news-label ${n.label}">${n.label === "BULL" ? "[BULL +]" :
                    n.label === "BEAR" ? "[BEAR -]" : "[NEUT]"}</span>
                <div style="flex:1;min-width:0">
                  <div class="news-text">${n.title}</div>
                  <div style="display:flex;gap:10px;margin-top:2px">
                    <span class="news-source">${n.source}</span>
                    <span class="news-source">${n.time_published}</span>
                    <span class="news-source" style="color:${n.score >= 0 ? "#f5ff30" : "#3b1278"}">${n.score >= 0 ? "+" : ""}${n.score.toFixed(3)}</span>
                  </div>
                </div>
              </div>`).join("");
        }

        // â”€â”€ Econ Indicators â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        const econEl = document.getElementById("macro-econ");
        if (econEl) {
            econEl.innerHTML = d.econ.map(e => {
                const isUp = e.trend === "â–²";
                const trendClass = e.bias === "BULL" ? (isUp ? "up" : "down")
                    : e.bias === "BEAR" ? (isUp ? "down" : "up") : "";
                return `
                <div class="econ-item">
                  <span class="econ-name">${e.name}</span>
                  <span class="econ-val">${e.value}</span>
                  <span class="econ-trend ${trendClass}">${e.trend}</span>
                  <span class="econ-bias ${e.bias}">${e.bias}</span>
                </div>`;
            }).join("");
        }

        // â”€â”€ Macro Bias â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        const bias = d.bias;
        const labelEl = document.getElementById("macro-bias-label");
        const scoreEl = document.getElementById("macro-bias-score");
        const bNews = document.getElementById("b-news");
        const bEcon = document.getElementById("b-econ");
        const bHtf = document.getElementById("b-htf");

        if (labelEl) { labelEl.textContent = bias.label; labelEl.style.color = bias.color; }
        if (scoreEl) scoreEl.textContent = `Score: ${bias.score >= 0 ? "+" : ""}${bias.score.toFixed(3)}`;
        if (bNews) { bNews.textContent = `${bias.news_score >= 0 ? "+" : ""}${bias.news_score.toFixed(2)}`; bNews.style.color = bias.news_score >= 0 ? GREEN : RED; }
        if (bEcon) { bEcon.textContent = `${bias.econ_score >= 0 ? "+" : ""}${bias.econ_score.toFixed(2)}`; bEcon.style.color = bias.econ_score >= 0 ? GREEN : RED; }
        if (bHtf) { bHtf.textContent = bias.htf; bHtf.style.color = bias.htf === "BULLISH" ? GREEN : bias.htf === "BEARISH" ? RED : YELLOW; }

        // ── Yields strip ──────────────────────────────────────────────────────
        const setEl = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
        const biasColor = b => b === "BULL" ? GREEN : b === "BEAR" ? RED : YELLOW;
        const setBias = (id, b) => { const el = document.getElementById(id); if (el) { el.textContent = b; el.style.color = biasColor(b); } };

        if (d.yields) {
            const y = d.yields;
            const setYield = (id, val) => {
                const el = document.getElementById(id);
                if (el) { el.textContent = val != null ? val.toFixed(2) : "—"; }
            };
            setYield("yield-2y", y.y2);
            setYield("yield-10y", y.y10);
            setYield("yield-tips", y.tips);
            const spreadEl = document.getElementById("yield-spread");
            if (spreadEl) {
                spreadEl.textContent = y.spread_2_10 != null ? (y.spread_2_10 >= 0 ? "+" : "") + y.spread_2_10.toFixed(2) : "—";
                spreadEl.style.color = y.curve_bias === "BULL" ? GREEN : y.curve_bias === "BEAR" ? RED : YELLOW;
            }
            setYield("yield-breakeven", y.inf_breakeven);
            setEl("yield-curve-label", y.curve_label || "");
            setEl("yields-src", `FRED · ${y.source === "simulated" ? "simulated" : "daily"}`);
        }

        // ── Liquidity strip ───────────────────────────────────────────────────
        if (d.liquidity) {
            const liq = d.liquidity;
            setEl("liq-fed-bs", liq.fed_bs_t != null ? `$${liq.fed_bs_t.toFixed(2)}` : "—");
            setEl("liq-repo", liq.repo_b != null ? `$${liq.repo_b.toFixed(0)}` : "—");
            setEl("liq-tga", liq.tga_b != null ? `$${liq.tga_b.toFixed(0)}` : "—");
            setBias("liq-fed-bs-bias", liq.fed_bs_bias);
            setBias("liq-repo-bias", liq.repo_bias);
            setBias("liq-tga-bias", liq.tga_bias);
            setEl("liq-src", `FRED · ${liq.source === "simulated" ? "simulated" : "weekly"}`);
        }

    } catch (e) { console.error("Macro error:", e); }
}

// â”€â”€ Customization helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function applyColors(pos, neg, neu) {
    document.documentElement.style.setProperty("--hm-pos", pos);
    document.documentElement.style.setProperty("--hm-neg", neg);
    document.documentElement.style.setProperty("--hm-neu", neu);
    // Sync color-picker label spans
    const setVal = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
    setVal("cfg-col-pos-val", pos);
    setVal("cfg-col-neg-val", neg);
    setVal("cfg-col-neu-val", neu);
}

// Map panel key â†’ container element ID
// Maps panel toggle key → a real element ID present in index.html.
// applyPanels walks up from this element to find the nearest .chart-card wrapper.
const PANEL_IDS = {
    gex: "hm-gex",          // GEX heatmap
    dex: "hm-dex",          // DEX heatmap
    vex: "chart-vex-bar",   // VEX bar chart
    tex: "chart-vex-bar",   // TEX shares VEX section (same card) - hide if unchecked
    vannex: "hm-vannex",       // VannaEX heatmap
    cex: "hm-cex",          // CharmEX heatmap
    oi: "chart-oi",        // OI bar chart (Greeks tab row)
    max_pain: "chart-oi",        // Max Pain lives in the same OI section
};

function applyPanels(pv) {
    for (const [key, chartId] of Object.entries(PANEL_IDS)) {
        const el = document.getElementById(chartId);
        if (!el) continue;
        // Walk up until we hit a .chart-card or .hm-card container
        let wrapper = el;
        for (let i = 0; i < 6; i++) {
            if (!wrapper.parentElement) break;
            wrapper = wrapper.parentElement;
            if (wrapper.classList.contains("chart-card") ||
                wrapper.classList.contains("hm-card") ||
                wrapper.classList.contains("card")) break;
        }
        const show = pv[key] !== false;
        wrapper.style.display = show ? "" : "none";
    }
}

// Live-preview color picker changes
["cfg-col-pos", "cfg-col-neg", "cfg-col-neu"].forEach(id => {
    document.getElementById(id)?.addEventListener("input", () => {
        const pos = document.getElementById("cfg-col-pos")?.value || "#f5ff30";
        const neg = document.getElementById("cfg-col-neg")?.value || "#3b1278";
        const neu = document.getElementById("cfg-col-neu")?.value || "#20b79e";
        applyColors(pos, neg, neu);
        // Instantly re-render heatmaps with new colors
        const d = window._lastHmData;
        if (d && d.spot) {
            try { if (d.dex_hm) buildHeatmap("hm-dex", d.dex_hm, hmCmap("dex"), d.spot); } catch (e) { }
            try { if (d.gex_hm) buildHeatmap("hm-gex", d.gex_hm, hmCmap("gex"), d.spot); } catch (e) { }
        }
    });
});

// _refreshTimer/_barTimer/_barMs hoisted to top of file

function startRefreshBar(ms) {
    const bar = document.getElementById("refresh-bar-inner");
    const wrap = document.getElementById("refresh-bar");
    if (!bar || !wrap) return;
    _barMs = Math.max(ms, 5000);
    // Clear any existing animation
    if (_barTimer) clearInterval(_barTimer);
    wrap.classList.remove("flash");
    bar.style.transition = "none";
    bar.style.width = "0%";
    // Force reflow then start sweeping
    void bar.offsetWidth;
    const steps = 200;
    const stepMs = _barMs / steps;
    let tick = 0;
    bar.style.transition = `width ${stepMs}ms linear`;
    _barTimer = setInterval(() => {
        tick++;
        bar.style.width = (tick / steps * 100) + "%";
        if (tick >= steps) clearInterval(_barTimer);
    }, stepMs);
}

function flashRefreshBar() {
    const bar = document.getElementById("refresh-bar-inner");
    const wrap = document.getElementById("refresh-bar");
    if (!bar || !wrap) return;
    // Flash green at 100%
    if (_barTimer) clearInterval(_barTimer);
    wrap.classList.add("flash");
    bar.style.width = "100%";
    setTimeout(() => {
        wrap.classList.remove("flash");
        // Restart countdown immediately
        startRefreshBar(_barMs);
    }, 400);
}

function setRefreshInterval(ms) {
    if (_refreshTimer) clearInterval(_refreshTimer);
    _refreshTimer = setInterval(update, Math.max(ms, 5000));
    startRefreshBar(ms);
}

function setKeyStatus(elId, isSet) {
    const el = document.getElementById(elId);
    if (!el) return;
    el.textContent = isSet ? "âœ“ Connected" : "Not set";
    el.style.color = isSet ? "#f5ff30" : "#3b1278";
}

async function loadSettings() {
    try {
        const res = await authFetch(SETTINGS_URL);
        const d = await res.json();

        // API / general
        const t = document.getElementById("cfg-ticker");
        const p = document.getElementById("cfg-opts-provider");
        if (t) t.value = d.ticker || "QQQ";
        if (p) p.value = d.options_api_provider || "simulated";
        setKeyStatus("opts-key-status", d.options_api_key_set);
        setKeyStatus("av-key-status", d.alpha_vantage_key_set);

        // Data settings
        const setNum = (id, v) => { const el = document.getElementById(id); if (el) el.value = v; };
        setNum("cfg-strike-range", d.strike_range ?? 30);
        setNum("cfg-max-exp", d.max_expirations ?? 3);
        setNum("cfg-refresh", d.refresh_interval ?? 30);
        setNum("cfg-rfr", d.risk_free_rate ?? 0.045);
        setNum("cfg-div", d.dividend_yield ?? 0.005);

        // Visual â€” colors
        const pos = d.heatmap_pos_color || "#f5ff30";
        const neg = d.heatmap_neg_color || "#3b1278";
        const neu = d.heatmap_neutral_color || "#20b79e";
        const setCol = (id, v) => { const el = document.getElementById(id); if (el) el.value = v; };
        setCol("cfg-col-pos", pos);
        setCol("cfg-col-neg", neg);
        setCol("cfg-col-neu", neu);
        applyColors(pos, neg, neu);

        // Layout â€” panel toggles
        const pv = d.panels_visible || {};
        for (const key of Object.keys(PANEL_IDS)) {
            const cb = document.getElementById(`panel-${key}`);
            if (cb) cb.checked = pv[key] !== false;
        }
        applyPanels(pv);

        // Update refresh timer
        setRefreshInterval((d.refresh_interval || 30) * 1000);

        // Wire panel toggle checkboxes to live-preview (safe here — PANEL_IDS is defined above)
        Object.keys(PANEL_IDS).forEach(key => {
            const cb = document.getElementById(`panel-${key}`);
            if (!cb || cb._panelWired) return;
            cb._panelWired = true;
            cb.addEventListener('change', () => {
                const pv = {};
                Object.keys(PANEL_IDS).forEach(k => {
                    const el = document.getElementById(`panel-${k}`);
                    pv[k] = el ? el.checked : true;
                });
                applyPanels(pv);
            });
        });

    } catch (e) { console.error("loadSettings:", e); }
}

async function saveSettings() {
    const btn = document.getElementById("settings-save-btn");
    const msg = document.getElementById("settings-msg");
    const ticker = (document.getElementById("cfg-ticker")?.value || "").trim().toUpperCase();
    const provider = document.getElementById("cfg-opts-provider")?.value || "simulated";
    const optsKey = document.getElementById("cfg-opts-key")?.value || "";
    const avKey = document.getElementById("cfg-av-key")?.value || "";

    // Ticker comes from hidden input (set by loadSettings). If for any reason
    // it's blank (e.g. first load before loadSettings completes), read from
    // the current URL or default to "QQQ" so Save still works.
    const effectiveTicker = ticker || (window._currentTicker || "QQQ");
    if (!effectiveTicker) { showMsg("Ticker cannot be empty", false); return; }

    // Gather new-setting values
    const strikeRange = parseFloat(document.getElementById("cfg-strike-range")?.value) || 30;
    const maxExp = parseInt(document.getElementById("cfg-max-exp")?.value) || 3;
    const refreshSec = parseInt(document.getElementById("cfg-refresh")?.value) || 30;
    const riskFreeRate = parseFloat(document.getElementById("cfg-rfr")?.value) || 0.045;
    const dividendYield = parseFloat(document.getElementById("cfg-div")?.value) || 0.005;
    // Color pickers removed from settings — read from CSS vars instead
    const style = getComputedStyle(document.documentElement);
    const posCol = style.getPropertyValue("--hm-pos").trim() || "#f5ff30";
    const negCol = style.getPropertyValue("--hm-neg").trim() || "#3b1278";
    const neuCol = style.getPropertyValue("--hm-neu").trim() || "#20b79e";

    const panels_visible = {};
    for (const key of Object.keys(PANEL_IDS)) {
        panels_visible[key] = document.getElementById(`panel-${key}`)?.checked !== false;
    }

    btn.disabled = true; btn.textContent = "Savingâ€¦";
    try {
        const res = await authFetch(SETTINGS_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                ticker: effectiveTicker, options_api_provider: provider,
                options_api_key: optsKey, alpha_vantage_key: avKey,
                strike_range: strikeRange, max_expirations: maxExp,
                refresh_interval: refreshSec, risk_free_rate: riskFreeRate,
                dividend_yield: dividendYield,
                heatmap_pos_color: posCol, heatmap_neg_color: negCol, heatmap_neutral_color: neuCol,
                panels_visible,
            })
        });
        const d = await res.json();
        if (d.ok) {
            showMsg("Saved! Refreshingâ€¦", true);
            if (optsKey) { const el = document.getElementById("cfg-opts-key"); if (el) el.value = ""; }
            if (avKey) { const el = document.getElementById("cfg-av-key"); if (el) el.value = ""; }
            setKeyStatus("opts-key-status", !!optsKey || d.options_api_key_set);
            setKeyStatus("av-key-status", !!avKey || d.alpha_vantage_key_set);
            applyColors(posCol, negCol, neuCol);
            applyPanels(panels_visible);
            setRefreshInterval(refreshSec * 1000);
            setTimeout(() => update(), 800);
        } else {
            showMsg("Error: " + (d.error || "unknown"), false);
        }
    } catch (e) {
        showMsg("Save failed: " + e.message, false);
    } finally {
        btn.disabled = false; btn.textContent = "Save & Apply";
    }
}

function showMsg(text, ok) {
    const el = document.getElementById("settings-msg");
    if (!el) return;
    el.textContent = text;
    el.className = "settings-msg " + (ok ? "ok" : "err");
    setTimeout(() => { el.textContent = ""; el.className = "settings-msg"; }, 4000);
}

document.getElementById("settings-save-btn")?.addEventListener("click", saveSettings);

async function testConnection(type) {
    const btnId = type === "alpha_vantage" ? "test-av-btn" : "test-opts-btn";
    const statusId = type === "alpha_vantage" ? "av-key-status" : "opts-key-status";
    const btn = document.getElementById(btnId);
    const statusEl = document.getElementById(statusId);
    if (btn) { btn.disabled = true; btn.textContent = "Testingâ€¦"; }
    if (statusEl) { statusEl.textContent = "Connectingâ€¦"; statusEl.className = "settings-key-status"; }
    try {
        const res = await authFetch(`/api/test-connection?type=${type}`);
        const d = await res.json();
        if (statusEl) {
            statusEl.textContent = d.message;
            statusEl.className = "settings-key-status " + (d.ok ? "set" : "notset");
        }
    } catch (e) {
        if (statusEl) { statusEl.textContent = "Connection error: " + e.message; statusEl.className = "settings-key-status notset"; }
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = "Test Connection"; }
    }
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// â”€â”€ HIRO: MM Hedging Flow â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

async function loadHIRO() {
    try {
        const res = await authFetch(HIRO_URL);
        const data = await res.json();
        if (!res.ok || data.error) { console.warn("HIRO:", data.error); return; }
        buildHIRO(data);
    } catch (e) { console.error("HIRO error:", e); }
}

function buildHIRO(data) {
    const series = data.series || [];
    if (!series.length) return;

    const labels = series.map(p => {
        const d = new Date(p.time);
        return d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false });
    });
    const prices = series.map(p => p.price);
    const hiros = series.map(p => p.hiro);

    const currentHiro = data.current_hiro_m ?? 0;
    const hiroColor = currentHiro >= 0 ? "#f5ff30" : "#3b1278";

    // â”€â”€ HUD stats
    const set = (id, txt, color) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.textContent = txt;
        if (color) el.style.color = color;
    };
    set("hiro-current", (currentHiro >= 0 ? "+" : "") + currentHiro.toFixed(1) + "M", hiroColor);
    set("hiro-gex", (data.total_gex_m >= 0 ? "+" : "") + data.total_gex_m.toFixed(1) + "M", "#8899aa");

    const dirBadge = document.getElementById("hiro-direction");
    if (dirBadge) {
        dirBadge.textContent = data.direction || "â€”";
        dirBadge.className = "regime-badge " + (currentHiro > 0 ? "buy-badge" : currentHiro < 0 ? "sell-badge" : "neutral-badge");
    }

    const interp = document.getElementById("hiro-interpret");
    if (interp) {
        interp.textContent = currentHiro > 0
            ? "MMs net long delta â€” hedging reinforces the current move. Structural backing confirmed."
            : currentHiro < 0
                ? "MMs net short delta â€” hedging creates a drag. Rallies may lack follow-through (divergence risk)."
                : "MM hedging neutral relative to open. No structural bias.";
    }

    // â”€â”€ Chart.js dual-axis
    if (charts["chart-hiro"]) charts["chart-hiro"].destroy();
    const canvas = document.getElementById("chart-hiro");
    if (!canvas) return;

    charts["chart-hiro"] = new Chart(canvas.getContext("2d"), {
        type: "line",
        data: {
            labels,
            datasets: [
                {
                    label: "Price",
                    data: prices,
                    borderColor: "rgba(225,230,245,.85)",
                    backgroundColor: "transparent",
                    borderWidth: 1.5,
                    pointRadius: 0,
                    yAxisID: "yPrice",
                    tension: 0.25,
                    order: 2,
                },
                {
                    label: "HIRO ($M)",
                    data: hiros,
                    borderColor: "#60a5fa",
                    backgroundColor: "rgba(96,165,250,.07)",
                    fill: true,
                    borderWidth: 2,
                    pointRadius: 0,
                    yAxisID: "yHiro",
                    tension: 0.35,
                    order: 1,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 400 },
            interaction: { mode: "index", intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: themeBg1(),
                    borderColor: themeAccent() + "33",
                    borderWidth: 1,
                    titleColor: "#60a5fa",
                    bodyColor: "#8899aa",
                    callbacks: {
                        label: c => c.dataset.yAxisID === "yPrice"
                            ? ` Price: $${c.raw.toFixed(2)}`
                            : ` HIRO: ${c.raw >= 0 ? "+" : ""}${c.raw.toFixed(1)}M`,
                    },
                },
            },
            scales: {
                x: {
                    ticks: { color: "#6b7a99", font: { size: 9 }, maxTicksLimit: 8 },
                    grid: { color: "rgba(255,255,255,.03)" },
                },
                yPrice: {
                    position: "left",
                    ticks: { color: TEXT, font: { size: 9 }, callback: v => "$" + v.toFixed(1) },
                    grid: { color: "rgba(255,255,255,.04)" },
                },
                yHiro: {
                    position: "right",
                    ticks: {
                        color: "#60a5fa",
                        font: { size: 9 },
                        callback: v => (v >= 0 ? "+" : "") + v.toFixed(0) + "M",
                    },
                    grid: { display: false },
                },
            },
        },
    });
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// â”€â”€ Market Topology â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

async function loadTopology() {
    try {
        const res = await authFetch(TOPO_URL);
        const data = await res.json();
        if (!res.ok || data.error) { console.warn("Topology:", data.error); return; }
        buildTopology(data);
    } catch (e) { console.error("Topology error:", e); }
}

function buildTopology(data) {
    const el = document.getElementById("topology-3d");
    if (!el) return;

    const CLUSTER_COLORS = ["#7c6fe0", "#4b79e0", "#38b0de", "#f5ff30", "#f5c542", "#3b1278"];

    const hist = data.history || [];
    const recent = data.recent || [];
    const current = data.current || [0, 0, 0];
    const base = data.base || [0, 0, 0];
    const regime = data.regime || "â€”";

    // History cloud â€” colored by cluster id
    const traceHistory = {
        type: "scatter3d", mode: "markers",
        x: hist.map(p => p[0]), y: hist.map(p => p[1]), z: hist.map(p => p[2]),
        name: "History",
        marker: {
            size: 2.5,
            color: hist.map(p => CLUSTER_COLORS[p[3] % 6]),
            opacity: 0.5,
        },
        hoverinfo: "skip",
    };

    // Recent breadcrumbs â€” orange gradient recent â†’ yellow now
    const recColors = recent.map((_, i) => {
        const t = i / Math.max(recent.length - 1, 1);
        return `rgb(255,${Math.round(80 + 130 * t)},0)`;
    });
    const traceRecent = {
        type: "scatter3d", mode: "markers+lines",
        x: recent.map(p => p[0]), y: recent.map(p => p[1]), z: recent.map(p => p[2]),
        name: "Recent Path",
        marker: { size: 3.5, color: recColors, opacity: 0.95 },
        line: { color: "#f5c542", width: 2 },
        hoverinfo: "skip",
    };

    // Current state â€” white cross/star
    const traceCurrent = {
        type: "scatter3d", mode: "markers+text",
        x: [current[0]], y: [current[1]], z: [current[2]],
        name: "CURRENT MARKET",
        text: ["âœ¦ NOW"], textposition: "top center",
        marker: {
            size: 10, color: "#ffffff", symbol: "cross",
            line: { width: 2, color: "#ffffff" }
        },
        textfont: { color: "#ffffff", size: 10, family: "JetBrains Mono, monospace" },
        hoverinfo: "text",
        hovertext: regime,
    };

    // Base anchor â€” yellow diamond
    const traceBase = {
        type: "scatter3d", mode: "markers",
        x: [base[0]], y: [base[1]], z: [base[2]],
        name: "Current (Base)",
        marker: {
            size: 8, color: "#f5c542", symbol: "diamond",
            line: { width: 1.5, color: "#fff" }
        },
        hoverinfo: "skip",
    };

    const axStyle = { color: "#6b7a99", gridcolor: "#1a2535", zerolinecolor: "#2a3545", backgroundcolor: "#050a10" };

    Plotly.react(el,
        [traceHistory, traceRecent, traceBase, traceCurrent],
        {
            paper_bgcolor: "#0d1117",
            margin: { l: 0, r: 0, t: 10, b: 0 },
            showlegend: true,
            legend: {
                x: 0, y: 1, bgcolor: "rgba(5,10,18,.80)",
                bordercolor: "rgba(255,255,255,.07)", borderwidth: 1,
                font: { color: "#8899aa", size: 10, family: "JetBrains Mono, monospace" },
            },
            scene: {
                bgcolor: "#050a10",
                xaxis: { ...axStyle, title: "Trend" },
                yaxis: { ...axStyle, title: "Momentum" },
                zaxis: { ...axStyle, title: "Volatility" },
                camera: { eye: { x: 1.5, y: 1.5, z: 0.7 } },
                aspectmode: "cube",
            },
        },
        { displayModeBar: false, responsive: true }
    );

    const badge = document.getElementById("topo-regime");
    if (badge) {
        badge.textContent = regime;
        badge.className = "regime-badge " + (
            regime.includes("OUTLIER") ? "critical-badge" :
                regime.includes("EXTREME") ? "sell-badge" :
                    regime.includes("STRONG") ? "warn-badge" : "buy-badge"
        );
    }
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// â”€â”€ Market Entropy Manifold â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

async function loadEntropy() {
    try {
        const res = await authFetch(ENTROPY_URL);
        const data = await res.json();
        if (!res.ok || data.error) { console.warn("Entropy:", data.error); return; }
        buildEntropyManifold(data);
    } catch (e) { console.error("Entropy error:", e); }
}

function buildEntropyManifold(data) {
    const el = document.getElementById("entropy-3d");
    if (!el) return;

    const path = data.path || [];
    if (!path.length) return;

    const xs = path.map(p => p[0]);  // PCA1 = STATE
    const ys = path.map(p => p[1]);  // PCA2 = MOMENTUM
    const zs = path.map(p => p[2]);  // Entropy_Smooth = CHAOS

    const current = data.current || [0, 0, 0];
    const isCrit = data.status === "CRITICAL";
    const curColor = isCrit ? "#ff4444" : "#00ff88";

    // Main trajectory colored by entropy (Turbo scale: blue â†’ cyan â†’ green â†’ yellow â†’ red)
    const tracePath = {
        type: "scatter3d", mode: "lines",
        x: xs, y: ys, z: zs,
        name: "Market Path",
        line: { width: 4, color: zs, colorscale: "Turbo" },
        hoverinfo: "skip",
    };

    // Current state â€” colored diamond (green=stable, red=critical)
    const traceCurrent = {
        type: "scatter3d", mode: "markers",
        x: [current[0]], y: [current[1]], z: [current[2]],
        name: "Current State",
        marker: {
            size: 12, color: curColor, symbol: "diamond",
            line: { width: 2, color: "#ffffff" },
        },
        hoverinfo: "text",
        hovertext: data.status,
    };

    const axStyle = { color: "#6b7a99", gridcolor: "#1a2535", zerolinecolor: "#2a3545", backgroundcolor: "#050a10" };

    Plotly.react(el, [tracePath, traceCurrent], {
        paper_bgcolor: "#0d1117",
        margin: { l: 0, r: 0, t: 10, b: 0 },
        showlegend: false,
        scene: {
            bgcolor: "#050a10",
            xaxis: { ...axStyle, title: "STATE (PCA1)" },
            yaxis: { ...axStyle, title: "MOMENTUM (PCA2)" },
            zaxis: { ...axStyle, title: "CHAOS (Entropy)" },
            camera: { eye: { x: 1.6, y: 1.6, z: 0.6 }, center: { x: 0, y: 0, z: -0.2 } },
            aspectmode: "cube",
        },
        annotations: [{
            x: 0.02, y: 0.96, xref: "paper", yref: "paper",
            text: `<b>SYSTEM STATUS:</b> <span style="color:${curColor}">${data.status}</span><br>`
                + `<span style="font-size:10px;color:#888">Entropy: ${(data.current_entropy).toExponential(3)}</span><br>`
                + `<span style="font-size:10px;color:#888">Threshold: ${(data.threshold).toExponential(3)}</span>`,
            showarrow: false,
            bgcolor: "rgba(0,0,0,0.65)",
            bordercolor: "#333", borderwidth: 1,
            font: { size: 13, color: "white", family: "JetBrains Mono, monospace" },
            align: "left",
        }],
    }, { displayModeBar: false, responsive: true });

    // Update status badge + HUD below card header
    const badge = document.getElementById("entropy-status");
    if (badge) {
        badge.textContent = data.status;
        badge.className = "regime-badge " + (isCrit ? "critical-badge" : "buy-badge");
    }
    const elEl = document.getElementById("entropy-level");
    const thrEl = document.getElementById("entropy-thresh");
    if (elEl) { elEl.textContent = data.current_entropy.toExponential(3); elEl.style.color = curColor; }
    if (thrEl) { thrEl.textContent = data.threshold.toExponential(3); }
}











// ── Probability Tab ─────────────────────────────────────────────────────────
// ── Probability Distribution Engine ─────────────────────────────────────────
// Computes lognormal price probability (PDF or CDF) across strikes × DTE
// Formula:  f(K,T) = 1/(K·σ·√T·√2π) · exp(-((ln(K/S)-(r-σ²/2)·T)²)/(2σ²T))
// P(>K) = N(-d2),  P(<K) = N(d2)  where d2 = (ln(S/K)+(r-σ²/2)·T)/(σ·√T)

let _probMode = 'pdf';

// fast rational approximation of Φ(x) accurate to ±1.5×10⁻⁷
function normalCDF(x) {
    const t = 1 / (1 + 0.2316419 * Math.abs(x));
    const d = 0.3989422820 * Math.exp(-0.5 * x * x);
    const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.7814779 + t * (-1.8212560 + t * 1.3302744))));
    return x >= 0 ? 1 - p : p;
}

const _probDesc = {
    pdf: 'Brighter = more likely price lands here at expiry (probability density)',
    cdf_above: 'Brighter = higher chance price finishes ABOVE this strike at expiry',
    cdf_below: 'Brighter = higher chance price finishes BELOW this strike at expiry',
};
function setProbMode(mode, btn) {
    _probMode = mode;
    document.querySelectorAll('[data-prob-mode]').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const desc = document.getElementById('prob-mode-desc');
    if (desc) desc.textContent = _probDesc[mode] || '';
    renderProb();
}

function renderProb() {
    const S = window._lastSpot || 500;
    const sigma = (parseFloat(document.getElementById('prob-iv')?.value) || 25) / 100;
    const r = (parseFloat(document.getElementById('prob-rf')?.value) || 5) / 100;
    const rangePct = (parseFloat(document.getElementById('prob-range')?.value) || 20) / 100;
    const maxDTE = parseInt(document.getElementById('prob-dte')?.value) || 90;

    // Build strike axis (80 points across ±rangePct from spot)
    const N_K = 80, N_T = 45;
    const Kmin = S * (1 - rangePct), Kmax = S * (1 + rangePct);
    const strikes = Array.from({ length: N_K }, (_, i) => Kmin + (Kmax - Kmin) * i / (N_K - 1));

    // Build DTE axis (1 to maxDTE, linear)
    const dtes = Array.from({ length: N_T }, (_, i) => Math.max(1, Math.round(1 + (maxDTE - 1) * i / (N_T - 1))));

    // Compute z matrix [N_T rows × N_K cols]
    const z = dtes.map(dte => {
        const T = dte / 365;
        const sqrtT = Math.sqrt(T);
        return strikes.map(K => {
            const d2 = (Math.log(S / K) + (r - 0.5 * sigma * sigma) * T) / (sigma * sqrtT);
            if (_probMode === 'pdf') {
                // lognormal PDF (probability density — peaks at ATM)
                const logArg = Math.log(K / S) - (r - 0.5 * sigma * sigma) * T;
                return (1 / (K * sigma * sqrtT * Math.sqrt(2 * Math.PI))) *
                    Math.exp(-0.5 * (logArg / (sigma * sqrtT)) ** 2);
            } else if (_probMode === 'cdf_above') {
                return normalCDF(-d2);       // P(S_T > K)
            } else {
                return normalCDF(d2);        // P(S_T < K)
            }
        });
    });

    const colorscale = [
        [0, '#04040a'],
        [0.2, '#1a0050'],
        [0.4, '#6200c8'],
        [0.6, '#e040fb'],
        [0.8, '#ff9800'],
        [1, '#ffffff'],
    ];

    const bgColor = '#070810';
    const textColor = 'rgba(180,195,230,.85)';
    const gridColor = 'rgba(255,255,255,.06)';

    // ── 2D Heatmap ─────────────────────────────────────────────────────────
    const hmTrace = {
        type: 'heatmap',
        x: strikes.map(k => parseFloat(k.toFixed(1))),
        y: dtes,
        z,
        colorscale,
        showscale: true,
        hovertemplate: 'Strike: $%{x}<br>DTE: %{y}d<br>Value: %{z:.4f}<extra></extra>',
        colorbar: { thickness: 10, outlinewidth: 0, tickfont: { color: textColor, size: 9 } },
    };

    const hmLayout = {
        paper_bgcolor: bgColor, plot_bgcolor: bgColor,
        margin: { t: 10, r: 60, b: 50, l: 60 },
        xaxis: {
            title: { text: 'Strike Price ($)', font: { color: textColor, size: 10 } },
            tickfont: { color: textColor, size: 9 }, gridcolor: gridColor, zeroline: false,
            // mark ATM spot
            shapes: [{
                type: 'line', x0: S.toFixed(1), x1: S.toFixed(1), y0: 0, y1: 1,
                xref: 'x', yref: 'paper', line: { color: '#00e5ff', width: 1.5, dash: 'dot' }
            }]
        },
        yaxis: {
            title: { text: 'DTE (days)', font: { color: textColor, size: 10 } },
            tickfont: { color: textColor, size: 9 }, gridcolor: gridColor, zeroline: false
        },
        font: { color: textColor },
    };

    // ── 3D Surface ─────────────────────────────────────────────────────────
    const surfTrace = {
        type: 'surface',
        x: strikes.map(k => parseFloat(k.toFixed(1))),
        y: dtes,
        z,
        colorscale,
        showscale: false,
        opacity: 0.92,
        hovertemplate: 'Strike: $%{x:.1f}<br>DTE: %{y}d<br>Value: %{z:.4f}<extra></extra>',
        contours: {
            z: { show: true, usecolormap: true, highlightcolor: 'rgba(255,255,255,.3)', project: { z: false } },
        },
    };

    const surfLayout = {
        paper_bgcolor: bgColor,
        margin: { t: 10, r: 10, b: 10, l: 10 },
        scene: {
            bgcolor: bgColor,
            xaxis: { title: 'Strike ($)', titlefont: { color: textColor, size: 9 }, tickfont: { color: textColor, size: 8 }, gridcolor: gridColor },
            yaxis: { title: 'DTE (days)', titlefont: { color: textColor, size: 9 }, tickfont: { color: textColor, size: 8 }, gridcolor: gridColor },
            zaxis: { title: _probMode === 'pdf' ? 'Density' : 'Probability', titlefont: { color: textColor, size: 9 }, tickfont: { color: textColor, size: 8 }, gridcolor: gridColor },
            camera: { eye: { x: 1.5, y: -1.5, z: 0.9 } },
        },
        font: { color: textColor },
    };

    const cfg = { responsive: true, displayModeBar: false };
    Plotly.react('plot-prob-hm', [hmTrace], hmLayout, cfg);
    Plotly.react('plot-prob-3d', [surfTrace], surfLayout, cfg);

    // ── Metrics panel ──────────────────────────────────────────────────────
    const T30 = 30 / 365, T7 = 7 / 365;
    const d2_30 = (Math.log(S / S) + (r - 0.5 * sigma * sigma) * T30) / (sigma * Math.sqrt(T30));
    const upK = S * 1.05, dnK = S * 0.95;
    const pUp5_30 = normalCDF(-(Math.log(S / upK) + (r - 0.5 * sigma * sigma) * T30) / (sigma * Math.sqrt(T30)));
    const pDn5_30 = normalCDF((Math.log(S / dnK) + (r - 0.5 * sigma * sigma) * T30) / (sigma * Math.sqrt(T30)));
    const pUp5_7 = normalCDF(-(Math.log(S / upK) + (r - 0.5 * sigma * sigma) * T7) / (sigma * Math.sqrt(T7)));
    const metrics = document.getElementById('prob-metrics');
    if (metrics) metrics.innerHTML = `
        <div class="prob-metric"><span>Spot</span><b>$${S.toFixed(2)}</b></div>
        <div class="prob-metric"><span>σ (IV)</span><b>${(sigma * 100).toFixed(0)}%</b></div>
        <div class="prob-metric"><span>P(+5% in 30d)</span><b>${(pUp5_30 * 100).toFixed(1)}%</b></div>
        <div class="prob-metric"><span>P(−5% in 30d)</span><b>${(pDn5_30 * 100).toFixed(1)}%</b></div>
        <div class="prob-metric"><span>P(+5% in 7d)</span><b>${(pUp5_7 * 100).toFixed(1)}%</b></div>`;
}


// ── Global Theme Switcher ─────────────────────────────────────────────────────
// Header button just cycles through 3 global themes by calling applyTheme
const _CYCLE_LIST = ['void', 'terminal', 'arctic'];
function cycleTheme() {
    const curr = localStorage.getItem('dashTheme') || 'void';
    const idx = _CYCLE_LIST.indexOf(curr);
    const next = _CYCLE_LIST[(idx + 1) % _CYCLE_LIST.length];
    applyTheme(next, null);
}

// Restore theme on load
(function () {
    const saved = localStorage.getItem('dashboard-theme') || 'midnight';
    if (saved !== 'midnight') document.documentElement.dataset.theme = saved;
    const btn = document.getElementById('theme-btn');
    if (btn) btn.textContent = THEME_LABELS[saved] || THEME_LABELS.midnight;
})();

// ── Header Settings button → switch to settings tab ─────────────────────────
document.getElementById('header-settings-btn')?.addEventListener('click', function () {
    // Deactivate all sidebar items
    document.querySelectorAll('.sb-item').forEach(i => i.classList.remove('active'));
    // Hide all tab content
    document.querySelectorAll('.tab-content').forEach(tc => tc.classList.remove('active'));
    // Show settings
    const target = document.getElementById('tab-settings');
    if (target) target.classList.add('active');
});

// Force Plotly charts to fill container width on window resize
window.addEventListener('resize', () => {
    document.querySelectorAll('.js-plotly-plot').forEach(el => {
        if (el.data) Plotly.Plots.resize(el);
    });
});

// ── Inference Engine Tab ─────────────────────────────────────────────────────
const INFERENCE_URL = '/api/inference';

function _alertColor(level) {
    const map = {
        elevated: '#f5c542', watch: '#fb923c', critical: '#e8435a',
        error: '#e8435a', inactive: '#555', stable: '#2ecc8a',
        normal: '#2ecc8a', structured: '#2ecc8a', chaotic: '#fb923c',
        random: '#6b7a99', laminar: '#38bdf8', transitional: '#f5c542',
        turbulent: '#e8435a', coupled: '#2ecc8a', decoupled: '#fb923c',
        thin_calm: '#38bdf8'
    };
    return map[level] || '#6b7a99';
}

function _alertIcon(level) {
    const map = {
        elevated: '⚠️', watch: '👁️', critical: '🔴', error: '❌',
        inactive: '⏸️', stable: '✅', normal: '✅', structured: '🎯',
        chaotic: '🌊', random: '🎲', laminar: '🏊', transitional: '⚡',
        turbulent: '🌪️', coupled: '🔗', decoupled: '🔓', thin_calm: '❄️'
    };
    return map[level] || '📊';
}

function _signalCard(s) {
    const name = (s.name || s.framework || '?').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    const alert = s.alert_level || s.regime || 'unknown';
    const color = _alertColor(alert);
    const icon = _alertIcon(alert);
    const val = typeof s.value === 'number' ? s.value.toFixed(4) : String(s.value || '—');
    const interp = s.interpretation || '';
    return `
      <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:14px 16px;border-left:3px solid ${color}">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
          <span style="font-weight:700;font-size:.82rem;color:var(--text)">${icon} ${name}</span>
          <span style="font-size:.65rem;font-weight:600;padding:2px 8px;border-radius:4px;background:${color}22;color:${color};text-transform:uppercase;letter-spacing:.5px">${alert}</span>
        </div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:1.1rem;font-weight:700;color:${color};margin-bottom:6px">${val}</div>
        <div style="font-size:.68rem;color:var(--dim);line-height:1.4">${interp}</div>
      </div>`;
}

function loadInference() {
    const grid = document.getElementById('inference-grid');
    if (!grid) return;
    grid.innerHTML = '<div style="color:var(--dim);padding:40px;text-align:center;grid-column:1/-1">⏳ Loading 8 frameworks...</div>';
    authFetch(INFERENCE_URL)
        .then(r => r.json())
        .then(data => {
            if (data.error) { grid.innerHTML = `<div style="color:#e8435a;padding:20px">${data.error}</div>`; return; }
            grid.innerHTML = data.signals.map(s => _signalCard(s)).join('');
        })
        .catch(e => { grid.innerHTML = `<div style="color:#e8435a;padding:20px">Error: ${e}</div>`; });
}

function loadCrashRisk() {
    const el = document.getElementById('crashrisk-content');
    if (!el) return;
    el.innerHTML = '<div style="color:var(--dim);padding:40px;text-align:center">⏳ Loading crash risk...</div>';
    authFetch(INFERENCE_URL)
        .then(r => r.json())
        .then(data => {
            if (data.error) { el.innerHTML = `<div style="color:#e8435a;padding:20px">${data.error}</div>`; return; }
            const sornette = data.signals.find(s => (s.name || s.framework) === 'lppl_sornette') || {};
            const powerlaw = data.signals.find(s => (s.name || s.framework) === 'powerlaw_tail') || {};
            el.innerHTML = `
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
                <div style="background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:20px">
                  <h3 style="color:var(--text);margin:0 0 12px;font-size:.9rem">🔮 LPPL Sornette — Crash Timing</h3>
                  <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-family:'JetBrains Mono',monospace;font-size:.75rem">
                    <div><span style="color:var(--dim)">Critical Date:</span><br><strong style="color:var(--text);font-size:1rem">${sornette.tc_date || '—'}</strong></div>
                    <div><span style="color:var(--dim)">Days to tc:</span><br><strong style="color:${(sornette.days_to_tc || 999) < 30 ? '#e8435a' : '#2ecc8a'};font-size:1rem">${sornette.days_to_tc ?? '—'}</strong></div>
                    <div><span style="color:var(--dim)">Confidence:</span><br><strong style="color:var(--text)">${sornette.confidence ? (sornette.confidence * 100).toFixed(1) + '%' : '—'}</strong></div>
                    <div><span style="color:var(--dim)">R²:</span><br><strong style="color:var(--text)">${sornette.r_squared ? sornette.r_squared.toFixed(4) : '—'}</strong></div>
                    <div><span style="color:var(--dim)">Bubble:</span><br><strong style="color:${sornette.is_bubble ? '#e8435a' : '#2ecc8a'}">${sornette.is_bubble ? 'YES' : 'NO'}</strong></div>
                    <div><span style="color:var(--dim)">Alert:</span><br><strong style="color:${_alertColor(sornette.alert_level || '')};text-transform:uppercase">${sornette.alert_level || '—'}</strong></div>
                  </div>
                  <div style="margin-top:12px;padding:10px;background:var(--bg3);border-radius:8px;font-size:.72rem;color:var(--dim)">${sornette.interpretation || 'No data'}</div>
                </div>
                <div style="background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:20px">
                  <h3 style="color:var(--text);margin:0 0 12px;font-size:.9rem">📊 Power-Law Tail α — Fat Tail Risk</h3>
                  <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-family:'JetBrains Mono',monospace;font-size:.75rem">
                    <div><span style="color:var(--dim)">α Combined:</span><br><strong style="color:${(powerlaw.alpha_combined || 4) < 3 ? '#e8435a' : '#2ecc8a'};font-size:1rem">${powerlaw.alpha_combined ? powerlaw.alpha_combined.toFixed(3) : '—'}</strong></div>
                    <div><span style="color:var(--dim)">α Left (crash):</span><br><strong style="color:var(--text)">${powerlaw.alpha_left ? powerlaw.alpha_left.toFixed(3) : '—'}</strong></div>
                    <div><span style="color:var(--dim)">α Right (melt):</span><br><strong style="color:var(--text)">${powerlaw.alpha_right ? powerlaw.alpha_right.toFixed(3) : '—'}</strong></div>
                    <div><span style="color:var(--dim)">Regime:</span><br><strong style="color:${_alertColor(powerlaw.regime || '')};text-transform:uppercase">${powerlaw.regime || '—'}</strong></div>
                    <div><span style="color:var(--dim)">Trend:</span><br><strong style="color:var(--text)">${powerlaw.trend || '—'}</strong></div>
                    <div><span style="color:var(--dim)">Alert:</span><br><strong style="color:${_alertColor(powerlaw.alert_level || '')};text-transform:uppercase">${powerlaw.alert_level || '—'}</strong></div>
                  </div>
                  <div style="margin-top:12px;padding:10px;background:var(--bg3);border-radius:8px;font-size:.72rem;color:var(--dim)">${powerlaw.interpretation || 'No data'}</div>
                </div>
              </div>`;
        })
        .catch(e => { el.innerHTML = `<div style="color:#e8435a;padding:20px">Error: ${e}</div>`; });
}

function loadFlow() {
    const el = document.getElementById('flow-content');
    if (!el) return;
    el.innerHTML = '<div style="color:var(--dim);padding:40px;text-align:center">⏳ Loading flow analysis...</div>';
    authFetch(INFERENCE_URL)
        .then(r => r.json())
        .then(data => {
            if (data.error) { el.innerHTML = `<div style="color:#e8435a;padding:20px">${data.error}</div>`; return; }
            const flowFrameworks = ['transfer_entropy', 'shannon_entropy', 'ising_magnetization', 'reynolds_number', 'mutual_information', 'percolation_threshold'];
            const flowSignals = data.signals.filter(s => flowFrameworks.includes(s.name || s.framework));
            el.innerHTML = `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px">
              ${flowSignals.map(s => _signalCard(s)).join('')}
            </div>`;
        })
        .catch(e => { el.innerHTML = `<div style="color:#e8435a;padding:20px">Error: ${e}</div>`; });
}

// ── Level 2 Dashboard ─────────────────────────────────────────────────────────

let _l2PollTimer = null;
let _l2CandleChart = null;
let _l2CandleSeries = null;
let _l2VolumeSeries = null;
let _l2ChartSymbol = 'NQ';
let _l2ChartTF = '1m';
let _l2ChartInitialized = false;
let _l2CandlePollTimer = null;
let _l2TapeAll = [];   // accumulated trades, newest first

const L2_SYMBOLS = ['NQ', 'ES', 'YM', 'RTY'];

const SIG_META = {
    shannon_entropy:     { label: 'Shannon Entropy',     unit: 'bits', hi: 8,    good: 'chaos', color: '#7c5af7' },
    ising_magnetization: { label: 'Ising Magnetization', unit: '',     hi: 1,    good: 'trend', color: '#28c4f8' },
    reynolds_number:     { label: 'Reynolds Number',     unit: '',     hi: 5000, good: 'flow',  color: '#1fd17a' },
    lppl_sornette:       { label: 'LPPL Bubble',         unit: '',     hi: 1,    good: 'risk',  color: '#e8435a' },
    powerlaw_tail:       { label: 'Power-Law α',         unit: '',     hi: 6,    good: 'tail',  color: '#e6b430' },
    transfer_entropy:    { label: 'Transfer Entropy',    unit: 'bits', hi: 4,    good: 'cause', color: '#f07828' },
    percolation_threshold: { label: 'Percolation θ',    unit: '',     hi: 1,    good: 'connect',color: '#9b7ef8' },
    mutual_information:  { label: 'Mutual Info',         unit: 'bits', hi: 3,    good: 'corr',  color: '#b06fff' },
};

function _l2FmtTime(ts) {
    if (!ts) return '—';
    try {
        const d = new Date(ts);
        const hh = d.getHours().toString().padStart(2,'0');
        const mm = d.getMinutes().toString().padStart(2,'0');
        const ss = d.getSeconds().toString().padStart(2,'0');
        return `${hh}:${mm}:${ss}`;
    } catch { return '—'; }
}

function _l2RenderImbalance(data) {
    const row = document.getElementById('l2-imbalance-row');
    if (!row) return;
    const dom = data.dom || {};
    const mid = data.mid_prices || {};
    row.innerHTML = L2_SYMBOLS.map(sym => {
        const snap = dom[sym] || {};
        const imb = snap.imbalance != null ? snap.imbalance : (data.imbalance || {})[sym];
        const midP = mid[sym] || 0;
        const pct = imb != null ? Math.abs(imb) * 50 : 0; // 0..50% from center
        const isBid = imb != null && imb > 0;
        const barClr = imb == null ? '#555' : (isBid ? 'var(--green)' : 'var(--red)');
        const side = imb == null ? '—' : (isBid ? 'BID HVY' : 'ASK HVY');
        const imbTxt = imb != null ? (imb * 100).toFixed(1) + '%' : '—';
        const midTxt = midP > 0 ? midP.toFixed(2) : '—';
        return `<div class="l2-imb-card">
          <div class="l2-imb-label">${sym} <span style="color:var(--text);font-size:.72rem">${midTxt}</span></div>
          <div class="l2-imb-bar-wrap">
            <div class="l2-imb-bar" style="
              width:${pct}%;
              background:${barClr};
              transform-origin:left;
              ${isBid ? 'right:50%;left:auto;transform:scaleX(-1)' : 'left:50%'};
            "></div>
          </div>
          <div class="l2-imb-val">
            <span>${imbTxt}</span>
            <span class="l2-imb-side" style="color:${barClr}">${side}</span>
          </div>
        </div>`;
    }).join('');
}

function _l2RenderDOM(dom) {
    const body = document.getElementById('l2-dom-body');
    const stats = document.getElementById('l2-dom-stats');
    if (!body) return;
    const nq = dom ? (dom['NQ'] || {}) : {};
    const bids = nq.bids || {};
    const asks = nq.asks || {};
    const bestBid = nq.best_bid || 0;
    const bestAsk = nq.best_ask || 0;
    if (stats) stats.textContent = `bids: ${Object.keys(bids).length}  asks: ${Object.keys(asks).length}  spread: ${bestAsk && bestBid ? (bestAsk - bestBid).toFixed(2) : '—'}`;

    // Sort: bids desc (highest first), asks asc (lowest = nearest first)
    const bidPrices = Object.keys(bids).map(Number).sort((a,b) => b - a).slice(0, 20);
    const askPrices = Object.keys(asks).map(Number).sort((a,b) => a - b).slice(0, 20);

    // Build interleaved DOM view: asks top (reversed so best ask closest to mid), then best bid, bids below
    const maxBid = bidPrices.reduce((m, p) => Math.max(m, bids[p] || 0), 1);
    const maxAsk = askPrices.reduce((m, p) => Math.max(m, asks[p] || 0), 1);

    let rows = '';
    // Asks (reversed so smallest ask is nearest mid, shown first = top of bid side)
    const askReversed = [...askPrices].reverse();
    askReversed.forEach(p => {
        const vol = asks[p] || 0;
        const barW = (vol / maxAsk * 100).toFixed(1);
        rows += `<div class="l2-dom-row">
          <span class="l2-dom-bid l2-dom-vol"></span>
          <span class="l2-dom-price" style="color:var(--red)">${p.toFixed(2)}</span>
          <span class="l2-dom-ask l2-dom-vol" style="color:var(--red)">
            <div class="l2-dom-bar ask-bar" style="width:${barW}%"></div>
            ${vol.toLocaleString()}
          </span>
        </div>`;
    });
    // Mid separator
    rows += `<div class="l2-dom-row l2-at-market">
      <span class="l2-dom-bid" style="font-size:.55rem;color:var(--cyan)">BID</span>
      <span class="l2-dom-price">— MID —</span>
      <span class="l2-dom-ask" style="font-size:.55rem;color:var(--cyan)">ASK</span>
    </div>`;
    // Bids
    bidPrices.forEach(p => {
        const vol = bids[p] || 0;
        const barW = (vol / maxBid * 100).toFixed(1);
        rows += `<div class="l2-dom-row">
          <span class="l2-dom-bid l2-dom-vol" style="color:var(--green)">
            <div class="l2-dom-bar bid-bar" style="width:${barW}%"></div>
            ${vol.toLocaleString()}
          </span>
          <span class="l2-dom-price" style="color:var(--green)">${p.toFixed(2)}</span>
          <span class="l2-dom-ask l2-dom-vol"></span>
        </div>`;
    });
    body.innerHTML = rows;
}

function _l2RenderTape(trades) {
    const body = document.getElementById('l2-tape-body');
    const cnt  = document.getElementById('l2-tape-count');
    if (!body) return;
    // Merge new trades into our accumulated list, newest first
    for (const sym of L2_SYMBOLS) {
        const arr = (trades[sym] || []);
        for (const t of [...arr].reverse()) {
            _l2TapeAll.unshift({ ...t, sym });
        }
    }
    // Deduplicate and cap
    _l2TapeAll = _l2TapeAll.slice(0, 300);
    if (cnt) cnt.textContent = `${_l2TapeAll.length} prints`;
    const top50 = _l2TapeAll.slice(0, 80);
    body.innerHTML = top50.map(t => {
        const side = t.side || (t.spin > 0 ? 'buy' : 'sell');
        const ts   = _l2FmtTime(t.timestamp);
        return `<div class="l2-tape-row ${side}">
          <span>${ts}</span>
          <span>${t.price != null ? t.price.toFixed(2) : '—'}</span>
          <span>${t.volume != null ? t.volume.toLocaleString() : '—'}</span>
          <span class="l2-tape-side">${side.toUpperCase()}</span>
        </div>`;
    }).join('');
}

function _l2RenderSignals(signals) {
    const grid = document.getElementById('l2-signals-grid');
    if (!grid) return;
    if (!signals || Object.values(signals).every(v => v == null)) {
        grid.innerHTML = '<div style="color:var(--dim);padding:30px;text-align:center;grid-column:1/-1">⏳ Signals compute after 60s of live data + backfill...</div>';
        return;
    }
    grid.innerHTML = Object.entries(SIG_META).map(([key, meta]) => {
        const raw = signals[key];
        let val = '—', fill = 0, fillClr = meta.color;
        if (raw != null && typeof raw === 'object') {
            // complex signal obj: try common fields
            const v = raw.value ?? raw.signal ?? raw.score ?? raw.magnetization ?? raw.entropy ?? raw.reynolds ?? null;
            if (v != null) { val = typeof v === 'number' ? v.toFixed(4) : String(v).slice(0,10); fill = Math.min(100, Math.abs(v) / meta.hi * 100); }
        } else if (raw != null) {
            val = typeof raw === 'number' ? raw.toFixed(4) : String(raw).slice(0,10);
            fill = Math.min(100, Math.abs(parseFloat(raw) || 0) / meta.hi * 100);
        }
        return `<div class="l2-signal-card">
          <div class="l2-signal-name">${meta.label}</div>
          <div class="l2-signal-val" style="color:${meta.color}">${val}${meta.unit ? '<span style="font-size:.65rem;opacity:.6;margin-left:4px">'+meta.unit+'</span>': ''}</div>
          <div class="l2-signal-bar"><div class="l2-signal-fill" style="width:${fill}%;background:${fillClr}"></div></div>
        </div>`;
    }).join('');
}

function _l2InitCandleChart() {
    if (_l2ChartInitialized) return;
    const container = document.getElementById('l2-candle-chart');
    if (!container || typeof LightweightCharts === 'undefined') return;
    _l2ChartInitialized = true;

    // Full container height (the CSS sets calc(100vh - 120px))
    const chartH = container.clientHeight || 700;

    _l2CandleChart = LightweightCharts.createChart(container, {
        width: container.clientWidth,
        height: chartH,
        layout: {
            background: { type: 'solid', color: 'transparent' },
            textColor: 'rgba(140,160,200,.65)',
            fontFamily: "'JetBrains Mono', 'SF Mono', monospace",
            fontSize: 11,
        },
        grid: {
            vertLines: { color: 'rgba(255,255,255,.025)', style: 1 },
            horzLines: { color: 'rgba(255,255,255,.025)', style: 1 },
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
            vertLine: {
                color: 'rgba(124,90,247,.5)',
                width: 1,
                style: 2,
                labelBackgroundColor: 'rgba(124,90,247,.85)',
            },
            horzLine: {
                color: 'rgba(124,90,247,.5)',
                width: 1,
                style: 2,
                labelBackgroundColor: 'rgba(124,90,247,.85)',
            },
        },
        rightPriceScale: {
            borderColor: 'rgba(255,255,255,.06)',
            scaleMargins: { top: 0.02, bottom: 0.18 },
            textColor: 'rgba(140,160,200,.55)',
            entireTextOnly: true,
        },
        timeScale: {
            borderColor: 'rgba(255,255,255,.06)',
            timeVisible: true,
            secondsVisible: true,
            rightOffset: 8,
            barSpacing: 7,
            minBarSpacing: 2,
            fixLeftEdge: false,
            fixRightEdge: false,
        },
        handleScroll: { mouseWheel: true, pressedMouseMove: true },
        handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
    });

    _l2CandleSeries = _l2CandleChart.addCandlestickSeries({
        upColor: '#1fd17a',
        downColor: '#e03060',
        borderUpColor: '#1fd17a',
        borderDownColor: '#e03060',
        wickUpColor: 'rgba(31,209,122,.7)',
        wickDownColor: 'rgba(224,48,96,.7)',
        priceFormat: { type: 'price', precision: 2, minMove: 0.25 },
    });

    _l2VolumeSeries = _l2CandleChart.addHistogramSeries({
        priceFormat: { type: 'volume' },
        priceScaleId: 'vol',
    });
    _l2CandleChart.priceScale('vol').applyOptions({
        scaleMargins: { top: 0.85, bottom: 0 },
        drawTicks: false,
    });

    // ResizeObserver for responsive chart
    const ro = new ResizeObserver(entries => {
        for (const entry of entries) {
            const { width, height } = entry.contentRect;
            if (_l2CandleChart && width > 0) {
                _l2CandleChart.applyOptions({ width, height: height || chartH });
            }
        }
    });
    ro.observe(container);

    // ── Symbol buttons ──
    document.querySelectorAll('#l2-chart-symbols .l2-tf-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('#l2-chart-symbols .l2-tf-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            _l2ChartSymbol = btn.dataset.sym;
            // Full reload: reset delta tracking, fetch all history
            _l2LastCandleTime = 0;
            _l2FetchCandles(true);
        });
    });

    // ── Timeframe buttons ──
    document.querySelectorAll('#l2-chart-tfs .l2-tf-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('#l2-chart-tfs .l2-tf-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            _l2ChartTF = btn.dataset.tf;
            // Full reload: reset delta tracking, fetch all history
            _l2LastCandleTime = 0;
            _l2FetchCandles(true);
        });
    });

    // Initial fetch — full history with setData()
    _l2FetchCandles(true);
    // Start chained polling (NOT setInterval — prevents request stacking)
    _l2ScheduleNextPoll();
}

// ── Delta tracking ──
// Tracks the timestamp of the newest candle we've received.
// Live polls send ?since=_l2LastCandleTime so the server returns only 1-3 candles.
let _l2LastCandleTime = 0;

// ── Chained setTimeout polling ──
// Unlike setInterval, this guarantees the next poll starts only AFTER the
// previous fetch completes. Prevents request stacking during server lag.
const _L2_POLL_INTERVAL = 1500; // 1.5s between polls

function _l2ScheduleNextPoll() {
    // Only schedule if we're still on the L2 tab and have a chart
    if (!_l2CandleSeries) return;
    _l2CandlePollTimer = setTimeout(() => {
        _l2FetchCandles(false).finally(() => {
            // Chain: schedule next poll after this one resolves (success or error)
            _l2ScheduleNextPoll();
        });
    }, _L2_POLL_INTERVAL);
}

function _l2FetchCandles(fullRedraw) {
    if (!_l2CandleSeries) return Promise.resolve();

    // Build URL: omit ?since= on full redraws to get all history
    let url = `/api/l2/candles?symbol=${_l2ChartSymbol}&tf=${_l2ChartTF}`;
    if (!fullRedraw && _l2LastCandleTime > 0) {
        url += `&since=${_l2LastCandleTime}`;
    }

    return authFetch(url)
        .then(r => r.json())
        .then(resp => {
            const candles = resp.candles;
            if (!Array.isArray(candles) || candles.length === 0) return;

            if (fullRedraw) {
                // ── FULL HISTORY: setData() once ──
                // Used on initial load and symbol/timeframe switches only.
                const ohlc = candles.map(c => ({
                    time: c.time,
                    open: c.open,
                    high: c.high,
                    low: c.low,
                    close: c.close,
                }));
                const vol = candles.map(c => ({
                    time: c.time,
                    value: c.volume || 0,
                    color: c.close >= c.open ? 'rgba(31,209,122,.25)' : 'rgba(224,48,96,.25)',
                }));
                _l2CandleSeries.setData(ohlc);
                _l2VolumeSeries.setData(vol);
                _l2CandleChart.timeScale().fitContent();
            } else {
                // ── DELTA UPDATE: update() only ──
                // Server returns only candles with time >= _l2LastCandleTime
                // (typically 1-2 candles). update() handles both:
                //   - Modifying the current bar (same timestamp)
                //   - Appending a new bar (new timestamp)
                for (const c of candles) {
                    _l2CandleSeries.update({
                        time: c.time,
                        open: c.open,
                        high: c.high,
                        low: c.low,
                        close: c.close,
                    });
                    _l2VolumeSeries.update({
                        time: c.time,
                        value: c.volume || 0,
                        color: c.close >= c.open ? 'rgba(31,209,122,.25)' : 'rgba(224,48,96,.25)',
                    });
                }
            }

            // Track the newest candle timestamp for next delta poll
            _l2LastCandleTime = candles[candles.length - 1].time;
        })
        .catch(() => {});
}

function _l2Render(data) {
    // Status dot
    const dot  = document.getElementById('l2-status-dot');
    const txt  = document.getElementById('l2-status-text');
    const conn = data.connected;
    if (dot) dot.className = 'l2-dot' + (conn ? ' live' : '');
    if (txt) txt.textContent = conn ? 'LIVE' : 'DISCONNECTED';

    // Symbol prices strip
    const strip = document.getElementById('l2-symbol-prices');
    if (strip) {
        const mid = data.mid_prices || {};
        strip.innerHTML = L2_SYMBOLS.map(s =>
            `<div class="l2-sym-price"><span class="l2-sym-label">${s}</span><span>${mid[s] ? mid[s].toFixed(2) : '—'}</span></div>`
        ).join('');
    }

    _l2RenderImbalance(data);
    _l2RenderDOM(data.dom);
    _l2RenderTape(data.trades || {});
    _l2RenderSignals(data.signals);
    _l2InitCandleChart();
}

// ── L2 DOM/Trade polling (also chained setTimeout, not setInterval) ──

function loadL2() {
    return authFetch('/api/l2')
        .then(r => r.json())
        .then(data => { if (data) _l2Render(data); })
        .catch(e => console.warn('L2 poll error:', e));
}

function _l2ScheduleDomPoll() {
    _l2PollTimer = setTimeout(() => {
        loadL2().finally(() => {
            _l2ScheduleDomPoll();
        });
    }, 500);
}

function _startL2Poll() {
    // Initial DOM fetch
    loadL2();
    // Start chained DOM polling (replaces setInterval)
    _l2ScheduleDomPoll();
    // Kickstart candle chart if not yet initialized
    _l2InitCandleChart();
}

function _stopL2Poll() {
    if (_l2PollTimer) { clearTimeout(_l2PollTimer); _l2PollTimer = null; }
    if (_l2CandlePollTimer) { clearTimeout(_l2CandlePollTimer); _l2CandlePollTimer = null; }
}

// ── Sidebar tab routing ────────────────────────────────────────────────────────
// Patch into the existing sidebar click handler by intercepting at DOMready
document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.sb-item[data-tab]').forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.getAttribute('data-tab');
            if (tab === 'l2') {
                _startL2Poll();
            } else {
                _stopL2Poll();
            }
        });
    });
});

