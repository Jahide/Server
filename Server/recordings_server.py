#!/usr/bin/env python3
"""
SilentWatch — Recordings Dashboard Server
==========================================
Run:   python recordings_server.py
Open:  http://localhost:8888

Routes
------
  GET /                        → serve recordings_dashboard.html
  GET /api/recordings          → Firebase RTDB recording list (JSON)
  GET /api/download?key=...    → proxy encrypted .aes file from R2
"""

import http.server
import json
import os
import socketserver
import sys
import urllib.parse
from pathlib import Path

# ── Auto-install deps ─────────────────────────────────────────────────────────
def _ensure(mod, pkg):
    try:
        __import__(mod)
    except ImportError:
        import subprocess
        print(f"[setup] Installing {pkg}…")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

_ensure("boto3",    "boto3")
_ensure("requests", "requests")

import boto3
import requests as req
from botocore.exceptions import ClientError

# ── Config ────────────────────────────────────────────────────────────────────
PORT          = 8888
FIREBASE_URL  = "https://raju-122f3-default-rtdb.firebaseio.com"
ACCOUNT_ID    = "b475dec0f0b18a58b62aa9adf38ef9c2"
BUCKET        = "pradip65"
ACCESS_KEY    = "c6d305828ec37b6c35f487651ae38c95"
SECRET_KEY    = "1c8fe9260f86c91baf668b648718d0485d68fef3e0cc2d7219d1a70f0806b151"

SCRIPT_DIR = Path(__file__).parent

s3 = boto3.client(
    "s3",
    endpoint_url   = f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id     = ACCESS_KEY,
    aws_secret_access_key = SECRET_KEY,
    region_name    = "auto",
)

# ── HTTP Handler ──────────────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path.rstrip("/") or "/"
        params = dict(urllib.parse.parse_qsl(parsed.query))

        try:
            if path in ("/", "/dashboard"):
                self._serve_file(SCRIPT_DIR / "recordings_dashboard.html", "text/html; charset=utf-8")

            elif path == "/api/recordings":
                self._serve_recordings()

            elif path == "/api/download":
                self._proxy_r2(params.get("key", ""))

            else:
                self._send(404, "text/plain", b"Not Found")

        except Exception as exc:
            print(f"[error] {exc}")
            self._send(500, "text/plain", str(exc).encode())

    # ── Routes ────────────────────────────────────────────────────────────────

    def _serve_file(self, path: Path, content_type: str):
        if not path.exists():
            self._send(404, "text/plain", f"File not found: {path.name}".encode())
            return
        self._send(200, content_type, path.read_bytes())

    def _serve_recordings(self):
        try:
            r = req.get(f"{FIREBASE_URL}/recordings.json", timeout=10)
            body = r.content if r.ok else b"null"
        except Exception as exc:
            body = json.dumps({"error": str(exc)}).encode()
        self._send(200, "application/json", body)

    def _proxy_r2(self, key: str):
        if not key:
            self._send(400, "text/plain", b"Missing ?key= parameter")
            return
        try:
            resp = s3.get_object(Bucket=BUCKET, Key=key)
            data = resp["Body"].read()
            self.send_response(200)
            self.send_header("Content-Type",              "application/octet-stream")
            self.send_header("Content-Length",            str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control",             "no-store")
            self.end_headers()
            self.wfile.write(data)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            self._send(403, "text/plain", f"R2 error: {code}".encode())

    # ── Helper ────────────────────────────────────────────────────────────────

    def _send(self, status: int, content_type: str, body: bytes):
        self.send_response(status)
        self.send_header("Content-Type",              content_type)
        self.send_header("Content-Length",            str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(f"  {self.client_address[0]}  {args[0]}")


class ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    server = ThreadedServer(("127.0.0.1", PORT), Handler)
    print(f"\n{'═'*52}")
    print(f"  SilentWatch — Recordings Dashboard")
    print(f"  Open → http://localhost:{PORT}")
    print(f"  Press Ctrl+C to stop")
    print(f"{'═'*52}\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
