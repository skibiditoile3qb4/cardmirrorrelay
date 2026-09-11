"""CardMirror card-sharing relay — standalone, self-hostable.
MongoDB storage backend (rewritten from the original Postgres/SQLAlchemy version).

A content-agnostic store-and-forward mailbox with live push:

 POST /relay/messages store one addressed (encrypted) bundle
 GET /relay/messages?recipient= pull everything addressed to a code
 GET /relay/stream?recipient= SSE push: live-delivers new bundles
 DELETE /relay/messages/{msg_id} acknowledge / remove one delivered bundle
 GET /relay/health liveness (no auth)

…plus durable ROOMS for collaboration sessions (opaque encrypted CRDT
update logs with server-assigned delivery cursors):

 POST /relay/rooms create → {roomId}
 POST /relay/rooms/{id}/updates append opaque blob → {seq}
 GET /relay/rooms/{id}/updates?after=N snapshot (if N predates it) + tail
 GET /relay/rooms/{id}/stream SSE: hello{lastSeq}, update/presence frames
 POST /relay/rooms/{id}/snapshot {blob, coversThroughSeq} → truncates ≤ seq
 POST /relay/rooms/{id}/presence ephemeral fan-out, never stored
 DELETE /relay/rooms/{id} end session (tombstone → 410)

This is the same wire contract CardMirror's official relay speaks, so
pointing the app at your own deployment is just Settings → Card Sharing →
Custom relay URL + Custom relay token. Everyone sharing cards with each
other must use the same relay.

WHAT CHANGED FROM THE POSTGRES VERSION:
 - Storage is MongoDB instead of Postgres. Every SQLAlchemy model became
 a Mongo collection; every `db.query(...)` became a pymongo find/
 update call. The wire contract (the HTTP routes, request/response
 shapes) is unchanged — this is a storage-layer rewrite only, not a
 behavior change.
 - `seq` (the room-update delivery cursor) can no longer come from a
 SQL autoincrement column, since Mongo has no such thing. It's now
 a manually-maintained atomic counter in a small `relay_counters`
 collection, incremented with an atomic `$inc` — still a single
 global monotonically-increasing integer, same semantics as before.
 - Message expiry (the 3-hour TTL) is now enforced by a MongoDB TTL
 index instead of a Python sweep loop — Mongo's background task
 deletes expired documents on its own every ~60 seconds. The GET
 /relay/messages query still also filters by cutoff itself, so a
 message never gets served late even if Mongo's cleanup lags.
 - Room idle-GC (tombstoning + eventually deleting rooms nobody's
 touched in a week) still needs custom logic — Mongo TTL indexes
 can't express "tombstone on day 7, then hard-delete on day 14" —
 so that part keeps its own sweep loop, just querying Mongo instead
 of Postgres.

Design notes (unchanged from the original):
 - Directed addressing: a sender POSTs to the recipient's routing code;
 the recipient receives only its own code and never sends to itself,
 so there is no self-echo.
 - Store-then-push: POST writes the row first (durability), then
 live-pushes to any open /relay/stream connections. Clients catch up
 via GET on every (re)connect, so delivery is at-least-once and the
 client's per-message dedupe absorbs overlap.
 - The in-process push registry requires a SINGLE worker process (run
 plain `uvicorn`, no --workers).
 - DB-touching handlers are sync `def` on purpose: Starlette runs them
 in its threadpool, keeping the blocking pymongo driver off the
 event loop (which must stay free to serve SSE streams and accept
 connections).

Rooms design notes (unchanged from the original):
 - `seq` is a delivery cursor, not a semantic order: CRDT updates are
 commutative, so the server only promises "give me everything after
 N" resumption. A global sequence shared across rooms is fine (gaps
 within a room are expected and harmless).
 - Compaction is the CLIENT's job (the server cannot read ciphertext):
 a client periodically uploads an encrypted snapshot covering
 everything through seq S; the server then deletes updates ≤ S.
 Joins fetch snapshot + tail, bounding join time on large docs.
 - Ended sessions tombstone (410, distinct from never-existed 404) so
 clients can tell "session over" from "bad room id". Idle rooms are
 garbage-collected after ROOM_IDLE_GC — generous by design: a
 session legitimately spans a travel day + tournament weekend with
 long fully-offline gaps.
 - At most MAX_STREAMS_PER_ROOM concurrent streams per room (409 on
 the next), which is also the participant ceiling.

PRIVACY: the card payload is end-to-end encrypted by the CardMirror
client. This server stores the bundle OPAQUELY (the `body` field) and
must never log or inspect it — only routing codes, ids, and counts are
ever touched here. Room update/snapshot/presence blobs are equally
opaque ciphertext: store, forward, count — never decode.

Env:
 RELAY_TOKEN required — the shared bearer your CardMirror clients
 configure as "Custom relay token".
 MONGODB_URI required — a MongoDB connection string, e.g.
 mongodb+srv://user:pass@cluster.mongodb.net
 (MongoDB Atlas's free tier gives you one of these.)
 MONGODB_DB_NAME optional (default "cardmirror_relay") — which
 database on that cluster to use.
 PORT optional (default 8000; the Dockerfile wires this up).
 RELAY_CORS_ORIGINS optional (default "*") — comma-separated allowed
 origins for browser/PWA collab clients. "*" is safe
 here (the auth is a bearer header, not a cookie).
"""
import asyncio
import base64
import gzip
import hmac
import json
import logging
import os
import re
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pymongo import ASCENDING, MongoClient, ReturnDocument
from pymongo.errors import PyMongoError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("relay")

# ── Limits / lifecycle (unchanged from the original) ────────────────

MAX_BYTES = 25 * 1024 * 1024 # decompressed payload cap
MAX_COMPRESSED_BYTES = 30 * 1024 * 1024 # gzip-bomb guard
TTL = timedelta(hours=3)
MAX_PER_POLL = 100
HEARTBEAT_SECONDS = 25
STREAM_QUEUE_MAX = 100

# Rooms (collaboration sessions)
MAX_UPDATE_BYTES = 5 * 1024 * 1024 # one appended blob (chunked client-side above 256 KiB)
ROOM_CAP_BYTES = 200 * 1024 * 1024 # total stored per room (updates + snapshot)
MAX_UPDATES_PER_PAGE = 200
MAX_STREAMS_PER_ROOM = 10 # participant ceiling, enforced at stream connect
ROOM_IDLE_GC = timedelta(days=7) # must exceed travel day + tournament weekend

# ── Storage (MongoDB) ────────────────────────────────────────────────

MONGODB_URI = os.getenv("MONGODB_URI")
if not MONGODB_URI:
 raise RuntimeError("MONGODB_URI environment variable is required")

MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "cardmirror_relay")

_client = MongoClient(MONGODB_URI)
mongo_db = _client[MONGODB_DB_NAME]

messages_col = mongo_db["relay_messages"]
rooms_col = mongo_db["relay_rooms"]
room_updates_col = mongo_db["relay_room_updates"]
room_snapshots_col = mongo_db["relay_room_snapshots"]
counters_col = mongo_db["relay_counters"]


def _init_indexes() -> None:
 """Called once at startup. Safe to call every restart — creating an
 index that already exists is a no-op."""
 # Messages: query index, plus a TTL index that makes Mongo delete
 # expired messages on its own (no manual sweep needed for these).
 messages_col.create_index([("recipient_code", ASCENDING), ("created_at", ASCENDING)])
 messages_col.create_index("created_at", expireAfterSeconds=int(TTL.total_seconds()))

 # Room updates: ordered lookup per room.
 room_updates_col.create_index([("room_id", ASCENDING), ("seq", ASCENDING)])

 # Rooms: idle-room sweep filters on this.
 rooms_col.create_index("last_activity")


def _next_seq() -> int:
 """Atomic global counter — replaces the Postgres autoincrement id.
 Still a single monotonically-increasing integer shared across all
 rooms, same semantics the original relied on (see module docstring)."""
 doc = counters_col.find_one_and_update(
 {"_id": "room_update_seq"},
 {"$inc": {"value": 1}},
 upsert=True,
 return_document=ReturnDocument.AFTER,
 )
 return int(doc["value"])


# routing code → open stream queues (single-worker only; see module doc)
_streams: dict[str, set["asyncio.Queue[dict]"]] = {}

# The server's one event loop, captured at startup. Sync (threadpool)
# handlers must never touch _streams or its asyncio.Queues directly —
# they are loop-owned and not thread-safe. All push fan-out is scheduled
# onto the loop via call_soon_threadsafe(_push_to_streams, …).
_loop: Optional[asyncio.AbstractEventLoop] = None


def _push_to_streams(recipient: str, message: dict) -> None:
 """Runs ON the event loop. A full queue sheds the push — the
 client's next catch-up poll covers it (at-least-once delivery)."""
 queues = _streams.get(recipient)
 if not queues:
 return
 for q in list(queues):
 try:
 q.put_nowait(message)
 except asyncio.QueueFull:
 pass


# room id → {queue: sid}. sid = client-minted stream nonce (?sid= at
# connect); presence POSTs carrying ?from=<same nonce> skip that queue
# (no self-echo). No sid = never skipped (old clients unchanged).
_room_streams: dict[str, dict["asyncio.Queue[dict]", Optional[str]]] = {}


def _push_to_room(room_id: str, frame: dict, skip_sid: Optional[str] = None) -> None:
 """Runs ON the loop; a full queue sheds the push — catch-up recovers.
 `skip_sid` (presence only): no self-echo to the sender's stream."""
 queues = _room_streams.get(room_id)
 if not queues:
 return
 for q, sid in list(queues.items()):
 if skip_sid is not None and sid == skip_sid:
 continue
 try:
 q.put_nowait(frame)
 except asyncio.QueueFull:
 pass


def _sweep_rooms() -> None:
 """Message expiry is handled by Mongo's TTL index (see _init_indexes).
 Rooms need custom logic: tombstone once idle, hard-delete once idle
 past a second period — that two-stage rule can't be expressed as a
 single TTL index."""
 idle_cutoff = datetime.utcnow() - ROOM_IDLE_GC
 idle_rooms = list(rooms_col.find({"last_activity": {"$lt": idle_cutoff}}))
 for room in idle_rooms:
 room_id = room["_id"]
 room_updates_col.delete_many({"room_id": room_id})
 room_snapshots_col.delete_many({"_id": room_id})
 if room.get("tombstoned"):
 rooms_col.delete_one({"_id": room_id})
 else:
 rooms_col.update_one(
 {"_id": room_id},
 {"$set": {"tombstoned": True, "bytes_used": 0}},
 )


def _sweeper_loop() -> None:
 while True:
 time.sleep(300)
 try:
 _sweep_rooms()
 except Exception as e: # never let the sweeper kill the thread
 logger.warning("[relay] sweep error: %s", e)


@asynccontextmanager
async def _lifespan(_app: FastAPI):
 global _loop
 _loop = asyncio.get_running_loop()
 _init_indexes()
 threading.Thread(target=_sweeper_loop, daemon=True).start()
 yield


app = FastAPI(title="CardMirror relay (MongoDB)", lifespan=_lifespan)

# CORS: collaboration sessions are driven from the browser/renderer via
# `fetch` (the mailbox card-sharing path runs in Electron's main process,
# which is not a browser and never triggers CORS — hence this was not
# needed before). A web/PWA client at a different origin needs the relay
# to answer CORS preflights. The bearer token is a header, not a cookie,
# so credential-less "*" is safe; lock it down with RELAY_CORS_ORIGINS
# (comma-separated) when serving a known front end.
_cors = os.getenv("RELAY_CORS_ORIGINS", "*").strip()
app.add_middleware(
 CORSMiddleware,
 allow_origins=["*"] if _cors == "*" else [o.strip() for o in _cors.split(",") if o.strip()],
 allow_credentials=False,
 allow_methods=["*"],
 allow_headers=["*"],
)


@app.exception_handler(PyMongoError)
async def _mongo_error(_request: Request, exc: PyMongoError) -> JSONResponse:
 # Any Mongo-side hiccup (connection blip, timeout, etc.) sheds with a
 # clean 503 rather than a 500 — clients retry (send is user-driven;
 # polls retry next interval; streams reconnect with backoff).
 logger.warning("[relay] mongo error: %s", exc)
 return JSONResponse({"detail": "relay busy, retry shortly"}, status_code=503)


# ── Auth (unchanged from the original) ──────────────────────────────


_VER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?$")
_PRE_RANK = {"alpha": 0, "beta": 1, "rc": 2}


def _parse_version(s: str):
 """Prerelease-aware key for CardMirror's version shapes, or None."""
 m = _VER_RE.match(s.strip())
 if not m:
 return None
 core = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
 pre = m.group(4)
 if pre is None:
 return core + (1, 0, 0)
 parts = pre.split(".")
 rank = _PRE_RANK.get(parts[0].lower(), 1)
 num = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
 return core + (0, rank, num)


@app.middleware("http")
async def _min_version_gate(request, call_next):
 """OPT-IN minimum-client-version gate (mirrors the official relay's).

 Dormant unless BOTH env vars are set:
 RELAY_MIN_CLIENT_VERSION e.g. "1.0.0"
 RELAY_MIN_VERSION_SCOPE "rooms" (block room CREATION only) or
 "all" (rooms + mailbox)

 Clients >= 0.1.0-beta.32 send X-CardMirror-Version; older builds
 send nothing and read as below any floor. Refusals are 426. This
 runs before token auth (it is a middleware), which is acceptable
 for an opt-in self-hosted policy knob; the official relay checks
 auth first. Unparseable floors fail open.
 """
 floor_s = os.getenv("RELAY_MIN_CLIENT_VERSION", "").strip()
 scope = os.getenv("RELAY_MIN_VERSION_SCOPE", "off").strip().lower()
 if floor_s and scope in ("rooms", "all"):
 path, method = request.url.path, request.method
 gated = path == "/relay/rooms" and method == "POST"
 if not gated and scope == "all":
 gated = (
 path.startswith("/relay/rooms/")
 or path == "/relay/messages"
 or path.startswith("/relay/messages/")
 or path == "/relay/stream"
 )
 if gated:
 floor = _parse_version(floor_s)
 if floor is not None:
 got = _parse_version(request.headers.get("x-cardmirror-version", ""))
 if got is None or got < floor:
 return JSONResponse(
 {"detail": {"error": "update-required", "minVersion": floor_s}},
 status_code=426,
 )
 return await call_next(request)


def require_relay_token(authorization: Optional[str] = Header(None)) -> None:
 """Shared bearer token. This stops the relay being an open public
 service; it is NOT the privacy mechanism (payloads are end-to-end
 encrypted, and the per-recipient routing code is the isolation
 boundary)."""
 expected = os.getenv("RELAY_TOKEN", "")
 if not expected:
 raise HTTPException(500, "RELAY_TOKEN not configured on server")
 if not authorization or not authorization.startswith("Bearer "):
 raise HTTPException(401, "Missing bearer token")
 if not hmac.compare_digest(authorization[len("Bearer "):], expected):
 raise HTTPException(401, "Invalid relay token")


def _epoch_ms(dt: datetime) -> int:
 return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


# ── Routes ───────────────────────────────────────────────────────────


@app.get("/relay/health")
def relay_health() -> dict:
 return {"ok": True}


# Plugin-install allowlist for CardMirror clients pointed at THIS relay
# (the client fetches it from whichever relay it is configured to use).
# Ungated like /health — it is public data, and the OPERATOR of a
# self-hosted relay is the right party to curate what their users'
# installers accept (plugins are full-trust code). Configure with
# RELAY_PLUGIN_ALLOWLIST (comma-separated owner/repo); the default
# matches the app's baked floor. Individuals who want arbitrary repos
# use the in-app console unlock instead: __plugins('community-on').
_DEFAULT_PLUGIN_ALLOWLIST = "shreerammodi/ebb,shreerammodi/cardmirror-ebb-plugin"


@app.get("/relay/plugin-allowlist")
def plugin_allowlist() -> dict:
 raw = os.getenv("RELAY_PLUGIN_ALLOWLIST", _DEFAULT_PLUGIN_ALLOWLIST)
 repos = sorted({r.strip().lower() for r in raw.split(",") if r.strip()})
 return {"schema": 1, "repos": repos}


async def _raw_body(request: Request) -> bytes:
 """Reads the request body on the event loop (a sync handler cannot
 await); everything after this runs on a worker thread."""
 return await request.body()


# Deliberately a sync `def`: Starlette runs it in the threadpool, so the
# blocking pymongo call never executes on the event loop.
@app.post("/relay/messages", status_code=202, dependencies=[Depends(require_relay_token)])
def post_message(
 raw: bytes = Depends(_raw_body),
 content_encoding: Optional[str] = Header(None),
) -> JSONResponse:
 if len(raw) > MAX_COMPRESSED_BYTES:
 raise HTTPException(413, "payload too large")

 if "gzip" in (content_encoding or "").lower():
 try:
 data = gzip.decompress(raw)
 except Exception:
 raise HTTPException(400, "invalid gzip body")
 else:
 data = raw

 if len(data) > MAX_BYTES:
 raise HTTPException(413, "payload too large")

 try:
 payload = json.loads(data) if data else {}
 except Exception:
 raise HTTPException(400, "invalid json")

 if not isinstance(payload, dict):
 raise HTTPException(400, "invalid payload")
 recipient = payload.get("recipientCode")
 if not isinstance(recipient, str) or not recipient:
 raise HTTPException(400, "missing recipientCode")

 msg_id = uuid.uuid4().hex
 created_at = datetime.utcnow()
 messages_col.insert_one(
 {
 "_id": msg_id,
 "recipient_code": recipient,
 "body": payload,
 "created_at": created_at,
 }
 )
 logger.info("[relay] POST recipient=%s… msgId=%s", recipient[:8], msg_id[:8])

 # Store-then-push. This runs on a worker thread; asyncio.Queues are
 # loop-owned and NOT thread-safe, so the fan-out is scheduled onto
 # the loop rather than touched here.
 if _loop is not None:
 message = {**payload, "msgId": msg_id, "receivedAt": _epoch_ms(created_at)}
 _loop.call_soon_threadsafe(_push_to_streams, recipient, message)
 return JSONResponse({"msgId": msg_id}, status_code=202)


def maybe_gzip_json(request: Request, payload: dict) -> Response:
 """Negotiated compression for the blob-heavy JSON endpoints. The
 ciphertext itself is incompressible, but its base64 EXPANSION gzips
 away (~-25% on blob bodies). Fires only when the client advertised
 gzip (every shipped client does); otherwise byte-identical to the
 uncompressed response. SSE never routes through here."""
 body = json.dumps(payload, separators=(",", ":")).encode()
 accepts = "gzip" in request.headers.get("accept-encoding", "").lower()
 if accepts and len(body) > 500:
 return Response(
 gzip.compress(body, 6),
 media_type="application/json",
 headers={"Content-Encoding": "gzip", "Vary": "Accept-Encoding"},
 )
 return Response(body, media_type="application/json", headers={"Vary": "Accept-Encoding"})


@app.get("/relay/messages", dependencies=[Depends(require_relay_token)])
def get_messages(
 request: Request,
 recipient: str = Query(..., min_length=1),
) -> Response:
 # Belt-and-suspenders: the TTL index already deletes expired
 # messages in the background, but this filter guarantees a stale
 # one is never served even if that background sweep hasn't run yet.
 cutoff = datetime.utcnow() - TTL
 cursor = (
 messages_col.find({"recipient_code": recipient, "created_at": {"$gte": cutoff}})
 .sort("created_at", ASCENDING)
 .limit(MAX_PER_POLL)
 )
 messages = [
 {**row["body"], "msgId": row["_id"], "receivedAt": _epoch_ms(row["created_at"])}
 for row in cursor
 ]
 return maybe_gzip_json(request, {"messages": messages})


@app.get("/relay/stream", dependencies=[Depends(require_relay_token)])
async def stream_messages(
 request: Request,
 recipient: str = Query(..., min_length=1),
) -> StreamingResponse:
 """SSE push channel: `event: hello` on connect, one `data:` frame per
 newly POSTed bundle, heartbeat comments while idle."""
 queue: "asyncio.Queue[dict]" = asyncio.Queue(maxsize=STREAM_QUEUE_MAX)
 _streams.setdefault(recipient, set()).add(queue)

 async def gen() -> AsyncIterator[str]:
 try:
 yield "event: hello\ndata: {}\n\n"
 while True:
 if await request.is_disconnected():
 return
 try:
 message = await asyncio.wait_for(
 queue.get(), timeout=HEARTBEAT_SECONDS
 )
 yield f"data: {json.dumps(message, separators=(',', ':'))}\n\n"
 except asyncio.TimeoutError:
 yield ": hb\n\n"
 finally:
 peers = _streams.get(recipient)
 if peers is not None:
 peers.discard(queue)
 if not peers:
 _streams.pop(recipient, None)

 return StreamingResponse(
 gen(),
 media_type="text/event-stream",
 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
 )


@app.delete(
 "/relay/messages/{msg_id}",
 status_code=204,
 dependencies=[Depends(require_relay_token)],
)
def delete_message(msg_id: str) -> Response:
 messages_col.delete_one({"_id": msg_id})
 return Response(status_code=204)


# ── Rooms (collaboration sessions) ───────────────────────────────────


def _room_or_error(room_id: str) -> dict:
 room = rooms_col.find_one({"_id": room_id})
 if room is None:
 raise HTTPException(404, "no such room")
 if room.get("tombstoned"):
 raise HTTPException(410, "session ended")
 return room


def _room_last_seq(room_id: str) -> int:
 last_update = room_updates_col.find_one({"room_id": room_id}, sort=[("seq", -1)])
 if last_update is not None:
 return int(last_update["seq"])
 snap = room_snapshots_col.find_one({"_id": room_id})
 return int(snap["covers_through_seq"]) if snap is not None else 0


@app.post("/relay/rooms", status_code=201, dependencies=[Depends(require_relay_token)])
def create_room() -> JSONResponse:
 room_id = uuid.uuid4().hex
 now = datetime.utcnow()
 rooms_col.insert_one(
 {
 "_id": room_id,
 "created_at": now,
 "last_activity": now,
 "bytes_used": 0,
 "tombstoned": False,
 }
 )
 logger.info("[relay] room created %s…", room_id[:8])
 return JSONResponse({"roomId": room_id}, status_code=201)


@app.post(
 "/relay/rooms/{room_id}/updates",
 status_code=202,
 dependencies=[Depends(require_relay_token)],
)
def post_room_update(room_id: str, raw: bytes = Depends(_raw_body)) -> JSONResponse:
 if not raw:
 raise HTTPException(400, "empty update")
 if len(raw) > MAX_UPDATE_BYTES:
 raise HTTPException(413, "update too large")
 room = _room_or_error(room_id)
 if room["bytes_used"] + len(raw) > ROOM_CAP_BYTES:
 raise HTTPException(413, "room storage cap reached")
 b64 = base64.b64encode(raw).decode("ascii")
 seq = _next_seq()
 now = datetime.utcnow()
 room_updates_col.insert_one(
 {"room_id": room_id, "seq": seq, "blob": b64, "created_at": now}
 )
 rooms_col.update_one(
 {"_id": room_id},
 {"$inc": {"bytes_used": len(raw)}, "$set": {"last_activity": now}},
 )
 if _loop is not None:
 _loop.call_soon_threadsafe(_push_to_room, room_id, {"t": "u", "seq": seq, "blob": b64})
 return JSONResponse({"seq": seq}, status_code=202)


@app.get("/relay/rooms/{room_id}/updates", dependencies=[Depends(require_relay_token)])
def get_room_updates(
 request: Request,
 room_id: str,
 after: int = Query(0, ge=0),
 have_snap: Optional[int] = Query(None, alias="haveSnap", ge=0),
) -> Response:
 _room_or_error(room_id)
 out: dict = {}
 snap = room_snapshots_col.find_one({"_id": room_id})
 floor = after
 if snap is not None and after < snap["covers_through_seq"]:
 if have_snap is not None and have_snap == int(snap["covers_through_seq"]):
 # Conditional snapshot: client already holds this exact one.
 out["snapshotUnchanged"] = True
 out["snapshotCovers"] = int(snap["covers_through_seq"])
 else:
 out["snapshot"] = {
 "blob": snap["blob"],
 "coversThroughSeq": int(snap["covers_through_seq"]),
 }
 floor = int(snap["covers_through_seq"])
 cursor = (
 room_updates_col.find({"room_id": room_id, "seq": {"$gt": floor}})
 .sort("seq", ASCENDING)
 .limit(MAX_UPDATES_PER_PAGE)
 )
 rows = list(cursor)
 # Compaction-epoch tag on every page (see the official relay: the
 # client's incremental audit keys off this).
 out["snapCovers"] = int(snap["covers_through_seq"]) if snap is not None else 0
 out["updates"] = [{"seq": int(r["seq"]), "blob": r["blob"]} for r in rows]
 out["more"] = len(rows) == MAX_UPDATES_PER_PAGE
 out["lastSeq"] = int(rows[-1]["seq"]) if rows else floor
 return maybe_gzip_json(request, out)


@app.post(
 "/relay/rooms/{room_id}/snapshot",
 status_code=204,
 dependencies=[Depends(require_relay_token)],
)
def post_room_snapshot(room_id: str, raw: bytes = Depends(_raw_body)) -> Response:
 try:
 payload = json.loads(raw)
 blob = payload["blob"]
 covers = int(payload["coversThroughSeq"])
 if not isinstance(blob, str) or not blob or covers < 0:
 raise ValueError
 except Exception:
 raise HTTPException(400, "expected {blob, coversThroughSeq}")
 if len(blob) > MAX_UPDATE_BYTES * 8:
 raise HTTPException(413, "snapshot too large")
 room = _room_or_error(room_id)
 existing = room_snapshots_col.find_one({"_id": room_id})
 if existing is not None and covers <= existing["covers_through_seq"]:
 # Stale or duplicate compaction (another client got there first).
 return Response(status_code=204)
 now = datetime.utcnow()
 room_snapshots_col.update_one(
 {"_id": room_id},
 {"$set": {"blob": blob, "covers_through_seq": covers, "created_at": now}},
 upsert=True,
 )
 room_updates_col.delete_many({"room_id": room_id, "seq": {"$lte": covers}})
 # Recompute stored size from what actually remains (base64 length is a
 # fine proxy for the cap's purpose).
 remaining_bytes = sum(
 len(r["blob"]) for r in room_updates_col.find({"room_id": room_id}, {"blob": 1})
 )
 rooms_col.update_one(
 {"_id": room_id},
 {"$set": {"bytes_used": remaining_bytes + len(blob), "last_activity": now}},
 )
 return Response(status_code=204)


@app.post(
 "/relay/rooms/{room_id}/presence",
 status_code=202,
 dependencies=[Depends(require_relay_token)],
)
async def post_room_presence(
 room_id: str,
 request: Request,
 sender: Optional[str] = Query(None, alias="from", max_length=64),
) -> JSONResponse:
 """Ephemeral fan-out only — never stored, never touches the DB (this
 is the hot path at cursor-move rates). An unknown room simply has no
 open streams, so the frame goes nowhere."""
 raw = await request.body()
 if not raw:
 raise HTTPException(400, "empty presence")
 if len(raw) > 64 * 1024:
 raise HTTPException(413, "presence too large")
 b64 = base64.b64encode(raw).decode("ascii")
 _push_to_room(room_id, {"t": "p", "blob": b64}, skip_sid=sender)
 return JSONResponse({}, status_code=202)


@app.get("/relay/rooms/{room_id}/stream", dependencies=[Depends(require_relay_token)])
async def stream_room(
 request: Request,
 room_id: str,
 sid: Optional[str] = Query(None, max_length=64),
) -> StreamingResponse:
 """SSE: `event: hello` with the current cursor, then update/presence
 frames. The participant cap is enforced here — holding a stream IS
 being in the room."""
 room = rooms_col.find_one({"_id": room_id})
 if room is None:
 raise HTTPException(404, "no such room")
 if room.get("tombstoned"):
 raise HTTPException(410, "session ended")
 open_count = len(_room_streams.get(room_id, {}))
 if open_count >= MAX_STREAMS_PER_ROOM:
 raise HTTPException(409, "room is full")
 last_seq = _room_last_seq(room_id)
 rooms_col.update_one({"_id": room_id}, {"$set": {"last_activity": datetime.utcnow()}})

 queue: "asyncio.Queue[dict]" = asyncio.Queue(maxsize=STREAM_QUEUE_MAX)
 _room_streams.setdefault(room_id, {})[queue] = sid

 async def gen() -> AsyncIterator[str]:
 try:
 yield f'event: hello\ndata: {{"lastSeq":{last_seq}}}\n\n'
 while True:
 if await request.is_disconnected():
 return
 try:
 frame = await asyncio.wait_for(queue.get(), timeout=HEARTBEAT_SECONDS)
 yield f"data: {json.dumps(frame, separators=(',', ':'))}\n\n"
 if frame.get("t") == "end":
 return
 except asyncio.TimeoutError:
 yield ": hb\n\n"
 finally:
 peers = _room_streams.get(room_id)
 if peers is not None:
 peers.pop(queue, None)
 if not peers:
 _room_streams.pop(room_id, None)

 return StreamingResponse(
 gen(),
 media_type="text/event-stream",
 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
 )


@app.delete(
 "/relay/rooms/{room_id}",
 status_code=204,
 dependencies=[Depends(require_relay_token)],
)
def delete_room(room_id: str) -> Response:
 room = rooms_col.find_one({"_id": room_id})
 if room is None:
 raise HTTPException(404, "no such room")
 if not room.get("tombstoned"):
 now = datetime.utcnow()
 rooms_col.update_one(
 {"_id": room_id},
 {"$set": {"tombstoned": True, "bytes_used": 0, "last_activity": now}},
 )
 room_updates_col.delete_many({"room_id": room_id})
 room_snapshots_col.delete_many({"_id": room_id})
 if _loop is not None:
 _loop.call_soon_threadsafe(_push_to_room, room_id, {"t": "end"})
 return Response(status_code=204)
