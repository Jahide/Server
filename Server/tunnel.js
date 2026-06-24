/**
 * SilentWatch — Cloud Tunnel Manager
 * 
 * Starts the WebSocket server + a Cloudflare Quick Tunnel,
 * then pushes the live wss:// URL to Firebase Realtime Database
 * so the desktop dashboard auto-connects without any manual input.
 *
 * Usage:
 *   node tunnel.js
 */

const { spawn, execSync } = require('child_process');
const https = require('https');
const http = require('http');
const path = require('path');
const fs = require('fs');

// ─── Firebase Config ──────────────────────────────────────────────────────────
const FIREBASE_DB_URL = 'raju-122f3-default-rtdb.firebaseio.com';
const FIREBASE_PATH   = '/config/serverUrl.json';

// ─── Local Server ─────────────────────────────────────────────────────────────
const LOCAL_PORT = process.env.PORT || 8080;

// ─── Colours ──────────────────────────────────────────────────────────────────
const C = {
    reset:  '\x1b[0m',
    green:  '\x1b[32m',
    yellow: '\x1b[33m',
    cyan:   '\x1b[36m',
    red:    '\x1b[31m',
    bold:   '\x1b[1m',
    dim:    '\x1b[2m',
};
const tag  = (col, label) => `${col}${C.bold}[${label}]${C.reset}`;
const INFO  = tag(C.cyan,   'TUNNEL');
const OK    = tag(C.green,  'OK    ');
const WARN  = tag(C.yellow, 'WARN  ');
const ERR   = tag(C.red,    'ERROR ');

// ─── Helpers ──────────────────────────────────────────────────────────────────
function pushToFirebase(tunnelUrl) {
    return new Promise((resolve, reject) => {
        const wsUrl = tunnelUrl.replace(/^https?:\/\//, match =>
            match === 'https://' ? 'wss://' : 'ws://'
        );
        const body = JSON.stringify(wsUrl);
        const options = {
            hostname: FIREBASE_DB_URL,
            path:     FIREBASE_PATH,
            method:   'PUT',
            headers:  {
                'Content-Type':   'application/json',
                'Content-Length': Buffer.byteLength(body),
            },
        };
        const req = https.request(options, res => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    resolve(wsUrl);
                } else {
                    reject(new Error(`Firebase PUT failed: ${res.statusCode} ${data}`));
                }
            });
        });
        req.on('error', reject);
        req.write(body);
        req.end();
    });
}

function cloudflaredBin() {
    // Check if cloudflared is in PATH
    try {
        execSync('cloudflared --version', { stdio: 'ignore' });
        return 'cloudflared';
    } catch (_) {}
    // Windows default install location
    const winPath = path.join(
        process.env.LOCALAPPDATA || 'C:\\Users\\Default\\AppData\\Local',
        'Microsoft', 'WinGet', 'Links', 'cloudflared.exe'
    );
    if (fs.existsSync(winPath)) return winPath;
    return null;
}

function installCloudflared() {
    console.log(`${WARN} cloudflared not found. Attempting install via winget…`);
    try {
        execSync('winget install --id Cloudflare.cloudflared -e --silent', { stdio: 'inherit' });
        console.log(`${OK} cloudflared installed. Please restart this script.`);
    } catch (e) {
        console.log(`${ERR} Auto-install failed.\n`);
        console.log(`  Please install cloudflared manually:`);
        console.log(`  ${C.cyan}https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/${C.reset}\n`);
    }
    process.exit(1);
}

// ─── Extract tunnel URL from cloudflared output ───────────────────────────────
// cloudflared prints: "https://random-words.trycloudflare.com"
const TUNNEL_URL_RE = /https:\/\/[\w-]+\.trycloudflare\.com/i;

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
    console.log('');
    console.log(`${C.bold}${C.cyan}══════════════════════════════════════════════════${C.reset}`);
    console.log(`${C.bold}   SILENTWATCH — CLOUD TUNNEL MANAGER                ${C.reset}`);
    console.log(`${C.bold}${C.cyan}══════════════════════════════════════════════════${C.reset}`);
    console.log('');

    // 1. Find or install cloudflared
    let bin = cloudflaredBin();
    if (!bin) installCloudflared();

    console.log(`${INFO} Using cloudflared: ${C.dim}${bin}${C.reset}`);
    console.log(`${INFO} Tunnelling local port ${C.bold}${LOCAL_PORT}${C.reset} → Cloudflare edge…`);
    console.log('');

    // 2. Start cloudflared quick tunnel
    const cf = spawn(bin, [
        'tunnel', '--url', `http://localhost:${LOCAL_PORT}`, '--no-autoupdate'
    ], { stdio: ['ignore', 'pipe', 'pipe'] });

    let tunnelUrl = null;

    function handleCfOutput(line) {
        const match = line.match(TUNNEL_URL_RE);
        if (match && !tunnelUrl) {
            tunnelUrl = match[0];
            onTunnelReady(tunnelUrl);
        }
    }

    cf.stdout.on('data', buf => buf.toString().split('\n').forEach(l => l.trim() && handleCfOutput(l)));
    cf.stderr.on('data',  buf => buf.toString().split('\n').forEach(l => l.trim() && handleCfOutput(l)));

    cf.on('close', code => {
        console.log(`\n${WARN} cloudflared exited (code ${code}). Tunnel is down.`);
    });

    // 3. Start the WebSocket broker server in the same process space
    console.log(`${INFO} Starting WebSocket broker on port ${C.bold}${LOCAL_PORT}${C.reset}…`);
    require('./server.js');

    // Wait for tunnel (timeout 30s)
    await new Promise((resolve, reject) => {
        let elapsed = 0;
        const check = setInterval(() => {
            if (tunnelUrl) { clearInterval(check); resolve(); return; }
            elapsed += 500;
            if (elapsed >= 30000) {
                clearInterval(check);
                reject(new Error('Timeout waiting for tunnel URL from cloudflared'));
            }
        }, 500);
    });

    // Graceful cleanup
    process.on('SIGINT',  () => { cf.kill(); process.exit(0); });
    process.on('SIGTERM', () => { cf.kill(); process.exit(0); });
}

async function onTunnelReady(tunnelUrl) {
    console.log(`${OK}  Tunnel is LIVE:`);
    console.log(`    HTTP  → ${C.green}${tunnelUrl}${C.reset}`);

    try {
        const wsUrl = await pushToFirebase(tunnelUrl);
        console.log('');
        console.log(`${OK}  Firebase updated → ${C.green}${C.bold}${wsUrl}${C.reset}`);
        console.log('');
        console.log(`${C.bold}${C.green}  ✅  Desktop dashboard will auto-connect now!${C.reset}`);
        console.log('');
        console.log(`  WebSocket URL: ${C.cyan}${wsUrl}${C.reset}`);
        console.log(`  Share this with the Android app (or it auto-reads from Firebase).`);
        console.log('');
        console.log(`${C.dim}  Press Ctrl+C to stop tunnel + server${C.reset}`);
        console.log('');
    } catch (err) {
        console.log(`${ERR} Could not push to Firebase: ${err.message}`);
        console.log(`${WARN} Dashboard will NOT auto-connect — set URL manually: ${tunnelUrl.replace('https://', 'wss://')}`);
    }
}

main().catch(err => {
    console.error(`${ERR} Fatal: ${err.message}`);
    process.exit(1);
});
