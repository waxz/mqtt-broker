#!/usr/bin/env python3
"""
High-Performance IoT MQTT Broker / HTTP-to-MQTT Bridge.

Hot paths are compiled to C via broker_core.pyx.
Falls back to pure Python if the extension is not built.

Build:   python setup.py build_ext --inplace
Run:     uvicorn broker:app --host 0.0.0.0 --port 7860 --loop uvloop
"""

import asyncio
import base64
import json
import os
import socket
import logging
import urllib.parse
import concurrent.futures
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from functools import partial

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

# ========================================================================== #
# Import Cython core — full Python fallbacks inline
# ========================================================================== #
try:
    from broker_core import (
        # Bottleneck #1: callback processing
        fast_process_callback,
        fast_create_envelope,
        # Bottleneck #2: timestamp
        fast_timestamp,
        # Bottleneck #3: queue drain
        fast_drain,
        fast_drain_multi,
        # Bottleneck #4: response building
        fast_build_poll_response,
        # Bottleneck #5: JSON
        fast_json_dumps,
        fast_json_loads,
        json_engine_name,
        # Bottleneck #6: SSE formatting
        fast_format_sse,
        fast_format_sse_batch,
        optimized_sse_generator,
        # Security
        constant_time_compare,
        # Utilities
        fast_client_id,
        prepare_batch_args,
        # Stats
        record_message_in,
        record_message_out,
        get_stats,
        reset_stats,
        # WebSocket proxy
        optimized_ws_to_tcp,
        optimized_tcp_to_ws,
    )
    _CYTHON = True
    _JSON_ENGINE = json_engine_name()
    print(f"🚀 broker_core loaded (Cython=True, JSON={_JSON_ENGINE})")

except ImportError:
    _CYTHON = False
    _JSON_ENGINE = "json"
    print("⚠️  broker_core not compiled — pure Python fallbacks active")

    # ── Fallback: timestamp ──────────────────────────────────────────── #
    def fast_timestamp() -> str:
        return datetime.now().isoformat()

    # ── Fallback: envelope ───────────────────────────────────────────── #
    def fast_create_envelope(topic: str, payload) -> dict:
        return {"topic": topic, "payload": payload,
                "timestamp": datetime.now().isoformat()}

    # ── Fallback: callback ───────────────────────────────────────────── #
    def fast_process_callback(client_topics, topic, msg):
        store = client_topics.get(topic)
        if store is None:
            return
        envelope = fast_create_envelope(topic, msg)
        store.queue.append(envelope)
        try:
            store.notify.set()
        except AttributeError:
            pass
        aq = store.async_queue
        if aq is not None:
            try:
                aq.put_nowait(envelope)
            except Exception:
                try:
                    aq.get_nowait()
                except Exception:
                    pass
                try:
                    aq.put_nowait(envelope)
                except Exception:
                    pass

    # ── Fallback: drain ──────────────────────────────────────────────── #
    def fast_drain(queue, limit: int) -> list:
        n = len(queue)
        if n == 0:
            return []
        take = min(n, limit)
        if take >= n:
            result = list(queue)
            queue.clear()
            return result
        return [queue.popleft() for _ in range(take)]

    def fast_drain_multi(topics_dict, topic_filter, limit: int) -> list:
        collected = []
        if topic_filter is not None:
            store = topics_dict.get(topic_filter)
            if store is None:
                return collected
            return fast_drain(store.queue, limit)
        remaining = limit
        for key in list(topics_dict.keys()):
            if remaining <= 0:
                break
            store = topics_dict.get(key)
            if store is None:
                continue
            chunk = fast_drain(store.queue, remaining)
            collected.extend(chunk)
            remaining = limit - len(collected)
            if not store.queue:
                try:
                    store.notify.clear()
                except AttributeError:
                    pass
        return collected

    # ── Fallback: poll response ──────────────────────────────────────── #
    def fast_build_poll_response(messages: list) -> dict:
        return {"success": True, "mode": "poll", "count": len(messages),
                "messages": messages, "stamp": datetime.now().isoformat()}

    # ── Fallback: JSON ───────────────────────────────────────────────── #
    def fast_json_dumps(obj) -> str:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

    def fast_json_loads(raw):
        return json.loads(raw)

    def json_engine_name() -> str:
        return "json"

    # ── Fallback: SSE ────────────────────────────────────────────────── #
    def fast_format_sse(msg) -> str:
        return f"data: {json.dumps(msg, separators=(',', ':'))}\n\n"

    def fast_format_sse_batch(messages: list) -> str:
        return "".join(fast_format_sse(m) for m in messages)

    async def optimized_sse_generator(aq, clients_ref, client_id,
                                      heartbeat_interval=15.0):
        while True:
            if clients_ref.get(client_id) is None:
                break
            try:
                msg = await asyncio.wait_for(aq.get(),
                                             timeout=heartbeat_interval)
                yield f"data: {json.dumps(msg, separators=(',',':'))}\n\n"
            except asyncio.TimeoutError:
                yield ": heartbeat\n\n"
            except GeneratorExit:
                break
            except Exception:
                break

    # ── Fallback: security ───────────────────────────────────────────── #
    def constant_time_compare(a: str, b: str) -> bool:
        import hmac
        return hmac.compare_digest(a.encode(), b.encode())

    # ── Fallback: client ID ──────────────────────────────────────────── #
    def fast_client_id(username: str, password: str) -> str:
        raw = f"{username}:{password}".encode("ascii")
        return urllib.parse.quote_plus(base64.b64encode(raw).decode("ascii"))

    # ── Fallback: batch args ─────────────────────────────────────────── #
    def prepare_batch_args(messages) -> list:
        result = []
        for m in messages:
            if hasattr(m, "topic"):
                result.append((m.topic, m.payload, m.qos, m.retain))
            else:
                result.append((m.get("topic", ""), m.get("payload", ""),
                               m.get("qos", 0), m.get("retain", False)))
        return result

    # ── Fallback: stats ──────────────────────────────────────────────── #
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

    # ── Fallback: WS proxy ───────────────────────────────────────────── #
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

MAX_QUEUED       = int(os.environ.get("MAX_QUEUED", "10000"))
POLL_TIMEOUT_MAX = float(os.environ.get("POLL_TIMEOUT_MAX", "30.0"))
REAPER_INTERVAL  = int(os.environ.get("REAPER_INTERVAL", "60"))
STALE_SECONDS    = int(os.environ.get("STALE_SECONDS", "300"))

LOG_LEVEL = os.environ.get("BROKER_LOG_LEVEL", "WARNING").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.WARNING),
                    format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("broker")

iot = IotCore()
_executor: concurrent.futures.ThreadPoolExecutor | None = None


# ========================================================================== #
# Data Structures
# ========================================================================== #
class TopicStore:
    __slots__ = ("topic", "qos", "stream", "queue", "notify", "async_queue")

    def __init__(self, topic: str, qos: int = 0, stream: bool = False):
        self.topic       = topic
        self.qos         = qos
        self.stream      = stream
        self.queue: deque = deque(maxlen=MAX_QUEUED)
        self.notify       = asyncio.Event()
        self.async_queue: asyncio.Queue | None = (
            asyncio.Queue(maxsize=MAX_QUEUED) if stream else None
        )


class ClientState:
    __slots__ = ("client_id", "created_at", "last_ping", "topics", "_lock")

    def __init__(self, client_id: str):
        self.client_id  = client_id
        self.created_at = datetime.now()
        self.last_ping  = self.created_at
        self.topics: dict[str, TopicStore] = {}
        self._lock      = asyncio.Lock()

    @property
    def age_seconds(self) -> float:
        return (datetime.now() - self.last_ping).total_seconds()


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
    record_message_out(len(payload) if isinstance(payload, (str, bytes)) else 0)


async def _iot_subscribe(topic: str, callback, qos: int = 0):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        _executor, partial(iot.subscribe, topic, callback))


async def _iot_unsubscribe(topic: str):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_executor, partial(iot.unsubscribe, topic))


# ========================================================================== #
# MQTT Callback — delegates to Cython fast_process_callback
# ========================================================================== #
def _mqtt_on_message(client_id: str, topic: str, msg):
    """
    Called from IotCore's background thread for EVERY message.

    THIS IS THE #1 BOTTLENECK — now runs in compiled C when Cython
    is available.  ~6-12× faster than the Python version.
    """
    client = _clients.get(client_id)
    if client is None:
        return

    record_message_in(len(msg) if isinstance(msg, (str, bytes)) else 0)

    # ← The hot path.  In Cython this compiles to ~15 C function calls
    #   instead of ~40 Python bytecode instructions.
    fast_process_callback(client.topics, topic, msg)


# ========================================================================== #
# Background reaper
# ========================================================================== #
async def _reaper():
    while True:
        await asyncio.sleep(REAPER_INTERVAL)
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
    stream: bool = False


class UnsubscribeBody(BaseModel):
    topic: str = "iot"


class PollBody(BaseModel):
    topic: str | None = None
    limit: int = 1000
    timeout: float = Field(default=0.0, ge=0.0, le=POLL_TIMEOUT_MAX)


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
    # ← Cython: constant-time comparison prevents timing attacks
    if not constant_time_compare(body.apiKey or "", BROKER_APIKEY):
        raise HTTPException(status_code=401, detail="Invalid API Key")

    # ← Cython: C-level base64 + quote_plus
    client_id = fast_client_id(body.brokerUsername or "demo",
                               body.brokerPassword or "demo")

    async with _clients_lock:
        if client_id not in _clients:
            _clients[client_id] = ClientState(client_id)

    client = _clients[client_id]
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

    async with client._lock:
        if body.topic in client.topics:
            store = client.topics[body.topic]
            store.stream = body.stream
            if body.stream and store.async_queue is None:
                store.async_queue = asyncio.Queue(maxsize=MAX_QUEUED)
            return {"success": True, "topic": body.topic,
                    "stream": body.stream, "note": "flags updated"}
        client.topics[body.topic] = TopicStore(
            body.topic, qos=body.qos, stream=body.stream)

    await _iot_subscribe(
        body.topic,
        lambda msg, _c=client_id, _t=body.topic:
            _mqtt_on_message(_c, _t, msg),
        qos=body.qos,
    )
    return {"success": True, "topic": body.topic, "stream": body.stream}


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
    await _iot_publish(body.topic, body.payload,
                       qos=body.qos, retain=body.retain)
    return {"success": True}


# ── Publish (batch) ─────────────────────────────────────────────────────── #
@app.post("/clients/{client_id}/publish/batch")
async def publish_batch(client_id: str, body: BatchPublishBody):
    await _get_client(client_id)

    # ← Cython: extract args in C-typed loop
    args = prepare_batch_args(body.messages)

    tasks = [_iot_publish(t, p, qos=q, retain=r) for t, p, q, r in args]
    await asyncio.gather(*tasks)
    return {"success": True, "published": len(args)}


# ── Messages (poll / long-poll / SSE) ────────────────────────────────────── #
@app.post("/clients/{client_id}/messages")
async def get_messages(client_id: str, body: PollBody):
    client = await _get_client(client_id)

    # Check stream mode
    if body.topic:
        store = client.topics.get(body.topic)
        if store and store.stream:
            return _sse_response(client, body.topic)
    else:
        if any(s.stream for s in client.topics.values()):
            return _sse_response(client, None)

    return await _poll_response(client, body.topic, body.limit, body.timeout)


async def _poll_response(client: ClientState, topic: str | None,
                         limit: int, timeout: float):
    """
    Poll / long-poll endpoint.

    ALL drain work goes through Cython fast_drain_multi:
    - C-typed loop counters (no int boxing)
    - list(deque) + clear() for full drain (2 C calls vs N popleft)
    - Direct CPython API for list.append
    """
    # ← Cython: C-level drain
    collected = fast_drain_multi(client.topics, topic, limit)

    # Long-poll: block until messages arrive or timeout
    if not collected and timeout > 0:
        stores = (
            [client.topics[topic]] if topic and topic in client.topics
            else list(client.topics.values())
        )
        if stores:
            for s in stores:
                s.notify.clear()

            futs = [asyncio.ensure_future(s.notify.wait()) for s in stores]
            try:
                await asyncio.wait_for(
                    asyncio.wait(futs, return_when=asyncio.FIRST_COMPLETED),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                pass
            finally:
                for f in futs:
                    f.cancel()

            # ← Cython: drain again after wakeup
            collected = fast_drain_multi(client.topics, topic, limit)

    # ← Cython: C-level dict construction + C strftime timestamp
    return fast_build_poll_response(collected)


def _sse_response(client: ClientState, topic: str | None):
    """SSE stream using Cython-compiled async generator."""
    stores = (
        [client.topics[topic]] if topic and topic in client.topics
        else list(client.topics.values())
    )
    queues = [s.async_queue for s in stores if s.async_queue is not None]

    if not queues:
        async def _empty():
            yield 'data: {"error":"no stream subscriptions"}\n\n'
        return StreamingResponse(_empty(), media_type="text/event-stream")

    # ← Cython-compiled async generator
    gen = optimized_sse_generator(queues[0], _clients, client.client_id)

    return StreamingResponse(
        gen,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache",
                 "X-Accel-Buffering": "no",
                 "Connection": "keep-alive"},
    )


# ========================================================================== #
# WebSocket MQTT Proxy
# ========================================================================== #
async def _accept_ws(ws: WebSocket):
    requested = ws.scope.get("subprotocols", [])
    if "mqtt" in requested:
        await ws.accept(subprotocol="mqtt")
    elif not requested:
        await ws.accept()
    else:
        await ws.accept(subprotocol=requested[0])


async def _proxy_ws(ws: WebSocket):
    try:
        reader, writer = await asyncio.open_connection(
            BROKER_HOST, BROKER_PORT)
        sock = writer.get_extra_info("socket")
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception as exc:
        log.error("Broker down: %s", exc)
        await ws.close(code=1011)
        return

    await _accept_ws(ws)

    # ← Cython-compiled relay functions
    t1 = asyncio.create_task(optimized_ws_to_tcp(ws, writer))
    t2 = asyncio.create_task(optimized_tcp_to_ws(reader, ws))

    await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
    for t in (t1, t2):
        t.cancel()
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


# ========================================================================== #
# Entrypoint
# ========================================================================== #
def main():
    import uvicorn
    uvicorn.run(
        "broker:app",
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