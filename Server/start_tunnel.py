import subprocess
import re
import sys
import os
import time
import socket

# ─── Config ───────────────────────────────────────────────────────────────────
FIREBASE_RTDB_URL = "https://raju-122f3-default-rtdb.firebaseio.com"
PORT = 8765

# ─── Telegram Config ─────────────────────────────────────────────────────────
# Change these two values to your Telegram bot token and chat ID.
# They will be automatically written to Firebase every time the server starts.
# The Android app reads them from Firebase — no need to rebuild the APK.
TELEGRAM_BOT_TOKEN = "8012742505:AAGACKj2xt-4Ph-waCvuMoOmLc-CxwMazB8"   # e.g. "7123456789:AAFxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
TELEGRAM_CHAT_ID   = "1975037313"   # e.g. "123456789"  (your personal chat ID or group ID)

# ─── Auto-install dependencies ────────────────────────────────────────────────
def ensure_deps():
    required = ["websockets", "requests"]
    for pkg in required:
        try:
            __import__(pkg)
        except ImportError:
            print(f"[*] Installing missing package: {pkg}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

# ─── Free port if already in use ──────────────────────────────────────────────
def free_port(port):
    """Kill any process already listening on the given port (Windows)."""
    try:
        result = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True, text=True
        )
        for line in result.stdout.splitlines():
            if f":{port} " in line and "LISTENING" in line:
                pid = line.strip().split()[-1]
                subprocess.run(["taskkill", "/F", "/PID", pid],
                               capture_output=True)
                print(f"[*] Freed port {port} (killed PID {pid})", flush=True)
                time.sleep(1)
                break
    except Exception as e:
        print(f"[!] Could not free port {port}: {e}", flush=True)

# ─── Wait until server is actually listening ──────────────────────────────────
def wait_for_server(port, timeout=15):
    print(f"[*] Waiting for server to be ready on port {port}...", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                print(f"[+] Server is UP on port {port}!", flush=True)
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.5)
    return False

# ─── Firebase helpers ─────────────────────────────────────────────────────────

def firebase_put(path, value):
    """
    Write a value to a Firebase RTDB path.
    Pass value=None to DELETE (clear) the key — Firebase rejects json=None (400 error).
    """
    import requests as req
    try:
        url = f"{FIREBASE_RTDB_URL}/{path}.json"
        if value is None:
            # DELETE removes the key entirely from Firebase
            r = req.delete(url, timeout=5)
        else:
            r = req.put(url, json=value, timeout=5)
        if r.status_code not in (200, 204):
            print(f"[-] Firebase {path} failed ({r.status_code}): {r.text}", flush=True)
        return r.status_code in (200, 204)
    except Exception as e:
        print(f"[-] Firebase error on {path}: {e}", flush=True)
        return False


def reset_firebase_on_startup():
    """
    Called at script start — puts Firebase into a clean 'server not ready' state.
    • serverUrl  = null  (app goes silent — no WebSocket attempts)
    • adminOnline = false (app won't connect even if a stale URL exists)
    """
    print("[*] Resetting Firebase state (serverUrl=null, adminOnline=false)...", flush=True)
    firebase_put("config/adminOnline", False)
    firebase_put("config/serverUrl", None)   # null clears the key in Firebase
    print("[+] Firebase reset complete.\n", flush=True)


def setup_telegram_config():
    """
    Write Telegram bot config to Firebase so the Android app can fetch it.
    Edit TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID at the top of this file.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[!] Telegram config not set — skipping Firebase write.", flush=True)
        print("[!] Edit TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID at the top of start_tunnel.py\n", flush=True)
        return

    print("[*] Writing Telegram config to Firebase...", flush=True)
    ok = firebase_put("config/telegram", {
        "botToken": TELEGRAM_BOT_TOKEN,
        "chatId":   TELEGRAM_CHAT_ID,
    })
    if ok:
        print(f"[+] Telegram config written to Firebase (chatId={TELEGRAM_CHAT_ID})\n", flush=True)
    else:
        print("[-] Failed to write Telegram config to Firebase.\n", flush=True)

def update_firebase(tunnel_url):
    """
    Push the new tunnel URL to Firebase.
    Uses individual field PUTs — never wipes other keys like adminOnline.
    adminOnline stays false here; server.py sets it true when an admin connects.
    """
    ws_url = tunnel_url.replace("https://", "wss://")
    print(f"\n[*] Publishing tunnel URL to Firebase: {ws_url}", flush=True)
    ok = firebase_put("config/serverUrl", ws_url)
    if ok:
        print("[+] Firebase serverUrl updated! App will connect when admin comes online.\n", flush=True)


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    ensure_deps()
    free_port(PORT)  # clear port 8765 if a previous run is still holding it

    # Reset Firebase so the app knows server isn't ready yet
    reset_firebase_on_startup()

    # Write Telegram bot config to Firebase (edit values at the top of this file)
    setup_telegram_config()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    server_script = os.path.join(script_dir, "server.py")

    print("[*] Starting WebSocket broker (server.py)...", flush=True)
    server_proc = subprocess.Popen(
        [sys.executable, "-u", server_script],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    # Verify server actually started
    if not wait_for_server(PORT, timeout=15):
        print("\n[!!!] SERVER FAILED TO START on port", PORT, flush=True)
        print("[!!!] Check for errors above (missing 'websockets' package, etc.)", flush=True)
        server_proc.terminate()
        sys.exit(1)

    # Use auto protocol only — Cloudflare auto-negotiates HTTP/1.1 with WebSocket upgrade.
    # http2 and quic both fail with WebSocket servers (400 Bad Request) because they
    # use binary framing that the Python `websockets` library cannot handle.
    url_found = False

    try:
        while True:
            print(f"\n[*] Starting Cloudflare Tunnel...", flush=True)

            tunnel_proc = subprocess.Popen(
                [
                    "cloudflared", "tunnel",
                    "--no-autoupdate",
                    "--url", f"http://127.0.0.1:{PORT}",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            # Real tunnel URLs always contain hyphens
            url_pattern   = re.compile(r"https://[a-zA-Z0-9][a-zA-Z0-9]*(?:-[a-zA-Z0-9]+)+\.trycloudflare\.com")
            # NOTE: "connection rejected" is intentionally removed — it happens when an upstream
            # WebSocket client disconnects mid-handshake and is NOT a fatal tunnel error.
            error_pattern = re.compile(
                r"(ERR.*registering|ERR.*edge|Tunnel server stopped|Initiating shutdown"
                r"|context deadline exceeded|failed to request quick Tunnel"
                r"|Unable to reach|TLS handshake timeout)",
                re.IGNORECASE
            )

            try:
                for line in tunnel_proc.stdout:
                    line = line.rstrip()
                    print(f"[Cloudflared] {line}", flush=True)

                    # Success — grab URL and publish to Firebase
                    if not url_found:
                        m = url_pattern.search(line)
                        if m:
                            url_found = True
                            print("[*] Tunnel URL found. Waiting 5s for DNS to propagate...", flush=True)
                            time.sleep(5)
                            update_firebase(m.group(0))

                    # Fatal error — break inner loop to restart tunnel
                    if error_pattern.search(line):
                        print(f"[!] Tunnel error detected: {line.strip()}", flush=True)
                        tunnel_proc.terminate()
                        break

            except Exception as e:
                print(f"[!] Exception reading tunnel stdout: {e}", flush=True)
                tunnel_proc.terminate()

            # Wait for tunnel process to terminate
            tunnel_proc.wait()
            print("[*] Tunnel disconnected. Retrying in 5 seconds...", flush=True)
            time.sleep(5)

            # Reset url_found so we re-publish the URL on reconnect
            url_found = False

    except KeyboardInterrupt:
        print("\n[*] Shutting down...", flush=True)
    finally:
        print("[*] Terminating WebSocket broker...", flush=True)
        server_proc.terminate()


if __name__ == "__main__":
    main()
