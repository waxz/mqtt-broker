#!/usr/bin/env python3
"""
High-Performance IoT MQTT Broker / HTTP-to-MQTT Bridge.

REDESIGNED: Thread-safe message queue + event-driven SSE.

Key safety guarantees:
  - threading.Lock protects every deque read/write
  - loop.call_soon_threadsafe bridges MQTT thread → async loop
  - Atomic drain prevents message loss during concurrent publish/poll
  - Event-driven SSE eliminates polling latency

Build:   python setup.py build_ext --inplace
Run:     uvicorn broker:app --host 0.0.0.0 --port 7860 --loop uvloop
"""

import asyncio
import base64
import json
import os
import socket
import threading                       # ← NEW: for TopicStore lock
import requests
from typing import Iterable
import time
from pathlib import Path
from sse_starlette.sse import EventSourceResponse
from starlette.responses import FileResponse
import logging
import urllib.parse
import concurrent.futures
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime

from functools import partial

from fastapi import FastAPI, Request
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from fastapi.responses import HTMLResponse


RETRY_TIMEOUT = 15000  # millisecond

# ========================================================================== #
# Import Cython core — full Python fallbacks inline
# ========================================================================== #
try:
    from broker_core import (
        fast_create_envelope,
        fast_timestamp,
        fast_build_poll_response,
        fast_json_dumps,
        fast_json_loads,
        json_engine_name,
        constant_time_compare,
        fast_client_id,
        prepare_batch_args,
        record_message_in,
        record_message_out,
        get_stats,
        reset_stats,
        optimized_ws_to_tcp,
        optimized_tcp_to_ws,
        fast_drain_multi,
    )
    _CYTHON = True
    _JSON_ENGINE = json_engine_name()
    print(f"🚀 broker_core loaded (Cython=True, JSON={_JSON_ENGINE})")

except ImportError:
    _CYTHON = False
    _JSON_ENGINE = "json"
    print("⚠️  broker_core not compiled — pure Python fallbacks active")

    def fast_timestamp() -> str:
        return datetime.now().isoformat()

    def fast_create_envelope(topic: str, payload) -> dict:
        return {"topic": topic, "payload": payload,
                "timestamp": datetime.now().isoformat()}

    def fast_build_poll_response(messages: list) -> dict:
        return {"success": True, "mode": "poll", "count": len(messages),
                "messages": messages, "stamp": datetime.now().isoformat()}

    def fast_json_dumps(obj) -> str:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

    def fast_json_loads(raw):
        return json.loads(raw)

    def json_engine_name() -> str:
        return "json"

    def constant_time_compare(a: str, b: str) -> bool:
        import hmac
        return hmac.compare_digest(a.encode(), b.encode())

    def fast_client_id(username: str, password: str) -> str:
        raw = f"{username}:{password}".encode("ascii")
        return urllib.parse.quote_plus(base64.b64encode(raw).decode("ascii"))

    def prepare_batch_args(messages) -> list:
        result = []
        for m in messages:
            if hasattr(m, "topic"):
                result.append((m.topic, m.payload, m.qos, m.retain))
            else:
                result.append((m.get("topic", ""), m.get("payload", ""),
                               m.get("qos", 0), m.get("retain", False)))
        return result

    def fast_drain_multi(topics_dict, topic_filter, limit):
        collected = []
        remaining = limit
        if topic_filter:
            store = topics_dict.get(topic_filter)
            if store:
                collected = store.drain(remaining)
        else:
            for store in list(topics_dict.values()):
                if remaining <= 0: break
                chunk = store.drain(remaining)
                collected.extend(chunk)
                remaining -= len(chunk)
        return collected

    _mi = _mo = _bi = _bo = 0

    def record_message_in(bc):
        global _mi, _bi
        _mi += 1; _bi += bc

    def record_message_out(bc):
        global _mo, _bo
        _mo += 1; _bo += bc

    def get_stats():
        return {"messages_in": _mi, "messages_out": _mo,
                "bytes_in": _bi, "bytes_out": _bo}

    def reset_stats():
        global _mi, _mo, _bi, _bo
        _mi = _mo = _bi = _bo = 0

    async def optimized_ws_to_tcp(ws, writer):
        pending = 0
        try:
            async for data in ws.iter_bytes():
                if data:
                    writer.write(data)
                    n = len(data)
                    pending += n
                    if n < 100 or pending > 524288:
                        await writer.drain()
                        pending = 0
            if pending:
                await writer.drain()
        except Exception:
            pass

    async def optimized_tcp_to_ws(reader, ws):
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                await ws.send_bytes(data)
        except Exception:
            pass


# ========================================================================== #
# uvloop
# ========================================================================== #
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    _UVLOOP = True
    print("🚀 High-Performance uvloop enabled")
except ImportError:
    _UVLOOP = False
    print("⚠️ uvloop not found. Falling back to standard asyncio (Slower)")

from iotcore import IotCore

# ========================================================================== #
# Configuration
# ========================================================================== #
BROKER_HOST   = os.environ.get("BROKER_HOST", "127.0.0.1")
BROKER_PORT   = int(os.environ.get("BROKER_PORT", "1883"))
BROKER_APIKEY = os.environ.get("BROKER_APIKEY", "BROKER_APIKEY")

MAX_QUEUED       = int(os.environ.get("MAX_QUEUED", "1000"))  # Reduced default for safety
POLL_TIMEOUT_MAX = float(os.environ.get("POLL_TIMEOUT_MAX", "30.0"))
REAPER_INTERVAL  = int(os.environ.get("REAPER_INTERVAL", "60"))
STALE_SECONDS    = int(os.environ.get("STALE_SECONDS", "300"))
MSG_TTL          = int(os.environ.get("MSG_TTL", "300"))      # Messages expire after 5 mins

SSE_HEARTBEAT_SEC = 15.0   # SSE keepalive interval
SSE_DRAIN_LIMIT   = 500    # max messages per SSE drain cycle

LOG_LEVEL = os.environ.get("BROKER_LOG_LEVEL", "WARNING").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.WARNING),
                    format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("broker")

iot = IotCore()
_executor: concurrent.futures.ThreadPoolExecutor | None = None


# ========================================================================== #
# REDESIGNED: Data Structures
# ========================================================================== #

class TopicStore:
    """
    Thread-safe per-topic message buffer with auto-eviction.
    """
    __slots__ = ("topic", "qos", "_buffer", "_lock",
                 "_notify", "_overflow_count")

    def __init__(self, topic: str, qos: int = 0):
        self.topic  = topic
        self.qos    = qos
        self._buffer: deque        = deque(maxlen=MAX_QUEUED)
        self._lock:  threading.Lock = threading.Lock()
        self._notify                = asyncio.Event()   # per-topic wakeup
        self._overflow_count: int   = 0

    # ── called from MQTT thread ──────────────────────────────────── #
    def push(self, envelope: dict):
        """
        Append one message. Thread-safe.
        """
        envelope["_ts"] = time.time()  # Internal high-res timestamp for TTL
        with self._lock:
            if len(self._buffer) == self._buffer.maxlen:
                self._overflow_count += 1
            self._buffer.append(envelope)

    # ── called from async-loop thread ────────────────────────────── #
    def drain(self, limit: int) -> list:
        """
        Atomically remove up to *limit* messages and return them.
        """
        with self._lock:
            n = len(self._buffer)
            if n == 0:
                return []
            take = min(n, limit)
            if take >= n:
                result = list(self._buffer)
                self._buffer.clear()
            else:
                result = [self._buffer.popleft() for _ in range(take)]
            return result

    def cleanup(self, ttl: int):
        """
        Remove messages older than TTL.
        """
        cutoff = time.time() - ttl
        with self._lock:
            while self._buffer and self._buffer[0].get("_ts", 0) < cutoff:
                self._buffer.popleft()

    @property
    def pending(self) -> int:
        """Pending message count (atomic on CPython)."""
        return len(self._buffer)


class ClientState:
    """
    Per-client state with cross-thread notification.
    """
    __slots__ = ("client_id", "created_at", "last_ping", "topics",
                 "_lock", "_notify", "_event_loop")

    def __init__(self, client_id: str):
        self.client_id  = client_id
        self.created_at = datetime.now()
        self.last_ping  = self.created_at
        self.topics: dict[str, TopicStore] = {}
        self._lock      = asyncio.Lock()          # guards self.topics dict
        self._notify    = asyncio.Event()          # client-wide wakeup
        self._event_loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop):
        """Attach the running event-loop for cross-thread signalling."""
        self._event_loop = loop

    def cleanup(self, ttl: int):
        """Prune expired messages from all topics."""
        for store in list(self.topics.values()):
            store.cleanup(ttl)

    @property
    def age_seconds(self) -> float:
        return (datetime.now() - self.last_ping).total_seconds()

    # ── called from MQTT thread ──────────────────────────────────── #
    def on_message(self, topic: str, payload):
        """
        Receive a message from the MQTT callback thread.
        Optimized with Cython and reduced notification overhead.
        """
        store = self.topics.get(topic)      # dict.get is GIL-atomic
        if store is None:
            return

        envelope = fast_create_envelope(topic, payload)
        store.push(envelope)

        loop = self._event_loop
        if loop is not None and not loop.is_closed():
            # Only schedule .set() if the event is not already set.
            if not self._notify.is_set():
                try:
                    loop.call_soon_threadsafe(self._notify.set)
                except RuntimeError:
                    pass
            if not store._notify.is_set():
                try:
                    loop.call_soon_threadsafe(store._notify.set)
                except RuntimeError:
                    pass

    # ── called from async-loop thread ONLY ───────────────────────── #
    def drain_messages(self, topic: str | None = None,
                       limit: int = 1000) -> list:
        """
        Drain buffered messages from one or all topic stores.
        """
        collected: list = []
        remaining = limit

        if topic is not None:
            store = self.topics.get(topic)
            if store:
                collected = store.drain(remaining)
                if not store.pending:
                    store._notify.clear()
        else:
            for store in list(self.topics.values()):
                if remaining <= 0:
                    break
                chunk = store.drain(remaining)
                collected.extend(chunk)
                remaining -= len(chunk)
                if not store.pending:
                    store._notify.clear()

        # Clear client-wide event only when ALL stores are empty
        if not any(s.pending for s in self.topics.values()):
            self._notify.clear()

        return collected

    async def wait_for_messages(self, timeout: float,
                                topic: str | None = None) -> bool:
        """
        Async wait: returns True when messages are available,
        False on timeout.
        """
        if topic is not None:
            store = self.topics.get(topic)
            if not store:
                return False
            if store.pending:
                return True
            store._notify.clear()
            try:
                await asyncio.wait_for(
                    store._notify.wait(), timeout=timeout)
                return True
            except asyncio.TimeoutError:
                return False
        else:
            if any(s.pending for s in self.topics.values()):
                return True
            self._notify.clear()
            try:
                await asyncio.wait_for(
                    self._notify.wait(), timeout=timeout)
                return True
            except asyncio.TimeoutError:
                return False


_clients: dict[str, ClientState] = {}
_clients_lock = asyncio.Lock()


async def _get_client(client_id: str) -> ClientState:
    c = _clients.get(client_id)
    if c is None:
        raise HTTPException(status_code=404, detail="Client not found")
    return c


# ========================================================================== #
# Thread-pool wrappers for blocking iot.* calls
# ========================================================================== #
async def _iot_publish(topic: str, payload: str, qos: int = 0,
                       retain: bool = False):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        _executor, partial(iot.publish, topic, payload))
    record_message_out(
        len(payload) if isinstance(payload, (str, bytes)) else 0)


async def _iot_subscribe(topic: str, callback, qos: int = 0):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        _executor, partial(iot.subscribe, topic, callback))


async def _iot_unsubscribe(topic: str):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_executor, partial(iot.unsubscribe, topic))


# ========================================================================== #
# REDESIGNED: MQTT Callback
# ========================================================================== #
def _mqtt_on_message(client_id: str, topic: str, msg):
    """
    Called from IotCore's background thread for EVERY message.
    """
    client = _clients.get(client_id)
    if client is None:
        return
    record_message_in(len(msg) if isinstance(msg, (str, bytes)) else 0)
    client.on_message(topic, msg)


# ========================================================================== #
# Background reaper
# ========================================================================== #
async def _reaper():
    while True:
        await asyncio.sleep(REAPER_INTERVAL)
        
        # 1. Clean up expired messages for ALL clients
        for client in list(_clients.values()):
            try:
                client.cleanup(MSG_TTL)
            except Exception as e:
                log.error("Error during client message cleanup: %s", e)

        # 2. Reap stale clients
        stale = [cid for cid, c in _clients.items()
                 if c.age_seconds > STALE_SECONDS]
        for cid in stale:
            log.info("Reaping stale client %s", cid)
            client = _clients.pop(cid, None)
            if client:
                for t in list(client.topics):
                    try:
                        iot.unsubscribe(t)
                    except Exception:
                        pass


# ========================================================================== #
# Lifespan
# ========================================================================== #
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _executor
    _executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=int(os.environ.get("IOT_WORKERS", "8")),
        thread_name_prefix="iot",
    )
    log.info("Starting broker (cython=%s, uvloop=%s, json=%s)",
             _CYTHON, _UVLOOP, _JSON_ENGINE)
    iot.background_loop_forever()

    try:
        _, w = await asyncio.open_connection(BROKER_HOST, BROKER_PORT)
        w.close(); await w.wait_closed()
        log.info("Internal broker UP at %s:%d", BROKER_HOST, BROKER_PORT)
    except Exception:
        log.warning("Internal broker not reachable yet")

    reaper_task = asyncio.create_task(_reaper())
    yield
    reaper_task.cancel()
    _executor.shutdown(wait=False)


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


# ========================================================================== #
# Pydantic Models
# ========================================================================== #
class ConnectBody(BaseModel):
    brokerUrl: str | None = f"mqtt://{BROKER_HOST}:{BROKER_PORT}"
    brokerUsername: str | None = "demo"
    brokerPassword: str | None = "demo"
    clientId: str | None = None
    keepalive: int = 60
    apiKey: str | None = None


class PublishBody(BaseModel):
    topic: str = "iot"
    payload: str = "test"
    qos: int = 0
    retain: bool = False


class BatchPublishBody(BaseModel):
    messages: list[PublishBody]


class SubscribeBody(BaseModel):
    topic: str = "iot"
    qos: int = 0


class UnsubscribeBody(BaseModel):
    topic: str = "iot"


class PollBody(BaseModel):
    topic: str | None = None
    limit: int = 1000
    timeout: float = Field(default=0.0, ge=0.0, le=POLL_TIMEOUT_MAX)


class BatchPollBody(BaseModel):
    topic: str | None = None
    limit: int = 10000
    timeout: float = Field(default=5.0, ge=0.0, le=30.0)


# ========================================================================== #
# Endpoints
# ========================================================================== #

@app.get("/")
async def root():
    return {"message": "IoT MQTT Bridge", "cython": _CYTHON,
            "uvloop": _UVLOOP, "json_engine": _JSON_ENGINE}


@app.get("/health")
async def health():
    return {"status": "ok", "activeClients": len(_clients),
            "cython": _CYTHON, "stats": get_stats()}


@app.get("/stats")
async def stats_endpoint():
    return {"cython": _CYTHON, "uvloop": _UVLOOP,
            "json_engine": _JSON_ENGINE,
            "clients": len(_clients), "counters": get_stats()}


@app.post("/stats/reset")
async def stats_reset():
    reset_stats()
    return {"success": True}


# ── Connect ─────────────────────────────────────────────────────────────── #
@app.post("/clients/connect")
async def connect_client(body: ConnectBody):
    if not constant_time_compare(body.apiKey or "", BROKER_APIKEY):
        raise HTTPException(status_code=401, detail="Invalid API Key")

    if body.clientId:
        client_id = body.clientId
    else:
        import uuid
        base = fast_client_id(body.brokerUsername or "demo",
                              body.brokerPassword or "demo")
        client_id = f"{base}-{uuid.uuid4().hex[:8]}"

    async with _clients_lock:
        if client_id not in _clients:
            client = ClientState(client_id)
            client.bind_loop(asyncio.get_running_loop())
            _clients[client_id] = client

    client = _clients[client_id]
    client.last_ping = datetime.now()
    return {"success": True, "client_id": client_id,
            "created_at": client.created_at.isoformat()}


# ── Ping ────────────────────────────────────────────────────────────────── #
@app.post("/clients/{client_id}/ping")
async def ping(client_id: str):
    client = await _get_client(client_id)
    now = datetime.now()
    timeout = (now - client.last_ping).total_seconds()
    client.last_ping = now
    return {"success": True, "timeout": timeout}


# ── Disconnect ──────────────────────────────────────────────────────────── #
@app.post("/clients/{client_id}/disconnect")
async def disconnect_client(client_id: str):
    await _get_client(client_id)
    async with _clients_lock:
        client = _clients.pop(client_id, None)
    if client:
        for topic in list(client.topics):
            try:
                await _iot_unsubscribe(topic)
            except Exception:
                pass
    return {"success": True}


# ── Subscribe ───────────────────────────────────────────────────────────── #
@app.post("/clients/{client_id}/subscribe")
async def subscribe(client_id: str, body: SubscribeBody):
    client = await _get_client(client_id)

    # Ensure event-loop is bound (idempotent)
    if client._event_loop is None:
        client.bind_loop(asyncio.get_running_loop())

    async with client._lock:
        if body.topic in client.topics:
            return {"success": True, "topic": body.topic,
                    "note": "already subscribed"}
        client.topics[body.topic] = TopicStore(
            body.topic, qos=body.qos)

    await _iot_subscribe(
        body.topic,
        lambda msg, _c=client_id, _t=body.topic:
            _mqtt_on_message(_c, _t, msg),
        qos=body.qos,
    )
    return {"success": True, "topic": body.topic}


# ── Unsubscribe ─────────────────────────────────────────────────────────── #
@app.post("/clients/{client_id}/unsubscribe")
async def unsubscribe(client_id: str, body: UnsubscribeBody):
    client = await _get_client(client_id)
    async with client._lock:
        if body.topic not in client.topics:
            raise HTTPException(status_code=404,
                                detail=f"Not subscribed to '{body.topic}'")
        del client.topics[body.topic]
    await _iot_unsubscribe(body.topic)
    return {"success": True}


# ── Publish (single) ────────────────────────────────────────────────────── #
@app.post("/clients/{client_id}/publish")
async def publish(client_id: str, body: PublishBody):
    await _get_client(client_id)
    await _iot_publish(body.topic, body.payload, qos=body.qos, retain=body.retain)
    return {"success": True}


@app.get("/clients/{client_id}/publish_get")
async def publish_get(client_id: str, topic: str, payload: str, qos: int = 0, retain: bool = False):
    await _get_client(client_id)
    await _iot_publish(topic, payload, qos=qos, retain=retain)
    return {"success": True}


# ── Publish (batch) ─────────────────────────────────────────────────────── #
@app.post("/clients/{client_id}/publish/batch")
async def publish_batch(client_id: str, body: BatchPublishBody):
    await _get_client(client_id)
    args = prepare_batch_args(body.messages)
    tasks = [_iot_publish(t, p, qos=q, retain=r) for t, p, q, r in args]
    await asyncio.gather(*tasks)
    return {"success": True, "published": len(args)}


# ========================================================================== #
# REDESIGNED: SSE Stream — event-driven, zero-loss
# ========================================================================== #

async def sse_event_generator(request: Request, client: ClientState):
    try:
        while True:
            # Exit if client was disconnected / reaped
            if _clients.get(client.client_id) is None:
                return

            # Activity update to prevent reaper from killing active connection
            client.last_ping = datetime.now()

            # ── drain all available messages ─────────────────── #
            messages = client.drain_messages(limit=SSE_DRAIN_LIMIT)

            if messages:
                yield {
                    "event": "messages",
                    "data":  fast_json_dumps(messages),
                }
                await asyncio.sleep(0)
                continue

            # ── nothing to send — wait for signal or heartbeat ── #
            got_data = await client.wait_for_messages(SSE_HEARTBEAT_SEC)

            if not got_data:
                # Heartbeat: keeps proxies / LBs from killing the conn
                if await request.is_disconnected():
                    return
                yield {"comment": "heartbeat"}

    except (asyncio.CancelledError, GeneratorExit):
        return


@app.post("/clients/{client_id}/messages")
async def message_stream(client_id: str, request: Request):
    """
    SSE endpoint.  Returns an infinite event stream of all
    messages for this client across every subscribed topic.
    """
    client = await _get_client(client_id)
    client.last_ping = datetime.now()

    if client._event_loop is None:
        client.bind_loop(asyncio.get_running_loop())

    return EventSourceResponse(
        sse_event_generator(request, client),
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


# ========================================================================== #
# REDESIGNED: Poll / Long-poll — uses same thread-safe drain
# ========================================================================== #

@app.post("/clients/{client_id}/messages/poll")
async def get_messages(client_id: str, body: PollBody):
    """
    Immediate or long-poll message retrieval (JSON response).
    """
    client = await _get_client(client_id)

    if client._event_loop is None:
        client.bind_loop(asyncio.get_running_loop())

    collected = client.drain_messages(
        topic=body.topic, limit=body.limit)

    if not collected and body.timeout > 0:
        got = await client.wait_for_messages(
            body.timeout, topic=body.topic)
        if got:
            collected = client.drain_messages(
                topic=body.topic, limit=body.limit)

    return fast_build_poll_response(collected)


@app.post("/clients/{client_id}/messages/batch")
async def poll_batch(client_id: str, body: BatchPollBody):
    """
    Batch long-poll: wait for first message then linger briefly
    to accumulate more before responding.
    """
    client = await _get_client(client_id)

    if client._event_loop is None:
        client.bind_loop(asyncio.get_running_loop())

    collected = client.drain_messages(
        topic=body.topic, limit=body.limit)

    if not collected and body.timeout > 0:
        got = await client.wait_for_messages(
            body.timeout, topic=body.topic)
        if got:
            await asyncio.sleep(min(0.5, body.timeout / 4))
            collected = client.drain_messages(
                topic=body.topic, limit=body.limit)

    return fast_build_poll_response(collected)


# ========================================================================== #
# WebSocket MQTT Proxy  (unchanged)
# ========================================================================== #

async def _proxy_ws(client_ws: WebSocket):
    try:
        reader, writer = await asyncio.open_connection(
            BROKER_HOST, BROKER_PORT)
        sock = writer.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception as e:
        log.error("Internal Broker Down: %s", e)
        await client_ws.close(code=1011)
        return

    requested_protocols = client_ws.scope.get('subprotocols', [])
    if "mqtt" in requested_protocols:
        await client_ws.accept(subprotocol="mqtt")
    elif not requested_protocols:
        await client_ws.accept()
    else:
        await client_ws.accept(subprotocol=requested_protocols[0])

    task_ws  = asyncio.create_task(optimized_ws_to_tcp(client_ws, writer))
    task_tcp = asyncio.create_task(optimized_tcp_to_ws(reader, client_ws))

    await asyncio.wait([task_ws, task_tcp],
                       return_when=asyncio.FIRST_COMPLETED)

    for task in [task_ws, task_tcp]:
        task.cancel()

    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


@app.websocket("/mqtt")
async def ws_mqtt(ws: WebSocket):
    await _proxy_ws(ws)


@app.websocket("/mqtt_normal")
async def ws_mqtt_normal(ws: WebSocket):
    await _proxy_ws(ws)


BRIDGE_HTML = Path('bridge.html').read_text()


@app.get("/bridge", response_class=HTMLResponse)
async def bridge_page():
    return HTMLResponse(
        content=BRIDGE_HTML,
        headers={
            "Content-Security-Policy":
                "default-src 'self' 'unsafe-inline' https://unpkg.com/ "
                "wss://broker.emqx.io blob: ; connect-src 'self' wss: ws:;",
            "X-Frame-Options": "ALLOWALL",
            "Cross-Origin-Embedder-Policy": "unsafe-none",
            "Cross-Origin-Opener-Policy": "unsafe-none",
            "Access-Control-Allow-Origin": "*",
        },
    )


# ========================================================================== #
# Entrypoint
# ========================================================================== #
def main():
    import uvicorn
    uvicorn.run(
        "broker_app:app",
        host="0.0.0.0",
        port=7860,
        loop="uvloop",
        ws_ping_interval=None,
        ws_ping_timeout=None,
        log_level=LOG_LEVEL.lower(),
        limit_concurrency=2000,
        backlog=2048,
        timeout_keep_alive=30,
    )
    


if __name__ == "__main__":
    main()
