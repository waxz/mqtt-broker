import asyncio
from datetime import datetime
import socket
import base64
from pydantic import BaseModel, Field
import os
import urllib.parse
# 1. IMPORT THE COMPILED CYTHON MODULE
try:
    from proxy_core import optimized_ws_to_tcp, optimized_tcp_to_ws
    print("🚀 Running with CYTHON optimizations")
except ImportError:
    print("⚠️ Cython module not found. Run 'python setup.py build_ext --inplace'")
    # Fallback to Python definitions if compilation failed
    async def optimized_ws_to_tcp(ws, writer): pass 
    async def optimized_tcp_to_ws(reader, ws): pass

from fastapi.middleware.cors import CORSMiddleware
import uuid
from concurrent_collections import ConcurrentDictionary

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager

# Assuming 'iotcore' is your custom library or wrapper around an MQTT broker
from iotcore import IotCore 

# --- CRITICAL: Install and use uvloop for high performance ---
# pip install uvloop
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("🚀 High-Performance uvloop enabled")
except ImportError:
    print("⚠️ uvloop not found. Falling back to standard asyncio (Slower)")




# --- MOCKING IotCore for the fix demonstration (Uncomment your import above) ---
# class IotCore:
#     def background_loop_forever(self): pass
#     def subscribe(self, topic, cb): pass
#     def publish(self, topic, payload): pass
#     def accept(self, topic):
#         def decorator(func): return func
#         return decorator
iot = IotCore()
# ---------------------------------------------------------------------------
# import logging
# logging.basicConfig(level=logging.INFO) # This will show Broker logs too

BROKER_PORT = 1883
BROKER_HOST = '127.0.0.1'

BROKER_APIKEY = os.environ.get("BROKER_APIKEY","BROKER_APIKEY")

CHUNK_SIZE = 65536  # 64KB Read Buffer
BUFFER_SIZE = 65536
MESSAGE_LOG_COUNT = 10000
MESSAGE_COUNT=0
MESSAGE_START_TIME = None

def on_message(msg):
    print(f"📩 Recieve [{msg}]")
def on_message_benchmark(msg):
    # Only print every 100th message to save CPU
    COUNT = int(msg.split()[-1])
    global MESSAGE_COUNT, MESSAGE_START_TIME
    if MESSAGE_START_TIME is None:
        MESSAGE_START_TIME = datetime.now()
    MESSAGE_COUNT += 1

    if MESSAGE_COUNT % MESSAGE_LOG_COUNT == 0:
        print(f"📩 Recieve [{msg}]")
        elapsed = (datetime.now() - MESSAGE_START_TIME).total_seconds()
        
        mps = float(MESSAGE_COUNT) / float(elapsed) 
        print(f"📊 Processed {MESSAGE_COUNT} messages in {elapsed:.2f} sec,  {mps:.2f} m/sec") 
        
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Starting IoT Broker...")
    iot.background_loop_forever()
    
    # Quick connectivity check
    try:
        _, w = await asyncio.open_connection(BROKER_HOST, BROKER_PORT)
        w.close()
        await w.wait_closed()
        print("✅ Internal Broker UP")
    except:
        pass

    iot.unsubscribe("iot")
    iot.subscribe("iot", on_message)
    # iot.unsubscribe("iot")
    yield
    print("🛑 Broker stopping...")


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Hello World"}

async def ws_to_tcp(ws: WebSocket, writer: asyncio.StreamWriter):
    pending_bytes = 0
    # Increase from 128KB to 512KB for Localhost testing
    # Try Increasing the Flush Threshold for localhost. Since localhost is extremely fast, flushing too often (CPU bound) is worse than flushing less often (Memory bound).
    FLUSH_THRESHOLD = 524288  # 512KB

    try:
        async for data in ws.iter_bytes():
            if data:
                writer.write(data)
                n = len(data)
                pending_bytes += n
                
                # LOGIC CHANGE:
                # If the packet is tiny (< 100 bytes), it is likely an MQTT ACK or PINGREQ.
                # Flush IMMEDIATELY to keep latency low.
                # If it is big (payload), buffer it to keep bandwidth high.
                if n < 100 or pending_bytes > FLUSH_THRESHOLD:
                    await writer.drain()
                    pending_bytes = 0
        
        if pending_bytes > 0:
            await writer.drain()

    except (WebSocketDisconnect, ConnectionResetError):
        pass 
    except Exception as e:
        print(f"⚠️ WS->TCP Error: {e}")

async def tcp_to_ws(reader: asyncio.StreamReader, ws: WebSocket):
    """
    Optimized Reader: Reads large chunks.
    """
    try:
        while True:
            data = await reader.read(BUFFER_SIZE)
            if not data:
                break # EOF from Broker
            await ws.send_bytes(data)
    except (RuntimeError, ConnectionResetError):
        pass # WS closed
    except Exception as e:
        print(f"⚠️ TCP->WS Error: {e}")


@app.websocket("/mqtt")
async def mqtt_websocket_proxy_opt(client_ws: WebSocket):
    
    # --- FIX FOR BROWSER COMPATIBILITY ---
    # 1. Connect to Internal Broker
    try:
        reader, writer = await asyncio.open_connection(BROKER_HOST, BROKER_PORT)
        # --- OPTIMIZATION ---
        # Get the raw socket object
        sock = writer.get_extra_info('socket')
        if sock:
            # Disable Nagle's Algorithm (No Delay)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            
    except Exception as e:
        print(f"❌ Internal Broker Down: {e}")
        await client_ws.close(code=1011)
        return

    # 2. Negotiate Subprotocol Smartly
    # Browsers might send: "mqtt", "mqttv3.1", or a list.
    requested_protocols = client_ws.scope.get('subprotocols', [])
    print(f"🔄 Client requested subprotocols: {requested_protocols}")
    
    # If client asks for 'mqtt', give it 'mqtt'.
    if "mqtt" in requested_protocols:
        await client_ws.accept(subprotocol="mqtt")
    # If client asks for nothing (some default browser behaviors), accept anyway.
    elif not requested_protocols:
        await client_ws.accept()
    else:
        # Fallback: Just accept the first one they asked for to keep connection alive
        await client_ws.accept(subprotocol=requested_protocols[0])



    # 2. USE THE CYTHON FUNCTIONS
    task_ws = asyncio.create_task(optimized_ws_to_tcp(client_ws, writer))
    task_tcp = asyncio.create_task(optimized_tcp_to_ws(reader, client_ws))

    await asyncio.wait([task_ws, task_tcp], return_when=asyncio.FIRST_COMPLETED)

    for task in [task_ws, task_tcp]:
        task.cancel()
    
    writer.close()
    try:
        await writer.wait_closed()
    except:
        pass

@app.websocket("/mqtt_normal")
async def mqtt_websocket_proxy(client_ws: WebSocket):
    
    # --- FIX FOR BROWSER COMPATIBILITY ---
    # 1. Connect to Internal Broker

    # get reader, writer from connection pool

    try:
        reader, writer = await asyncio.open_connection(BROKER_HOST, BROKER_PORT)
        # --- OPTIMIZATION ---
        # Get the raw socket object
        sock = writer.get_extra_info('socket')
        if sock:
            # Disable Nagle's Algorithm (No Delay)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception as e:
        print(f"❌ Internal Broker Down: {e}")
        await client_ws.close(code=1011)
        return

    print("✅ Connected to Internal Broker")
    

    # get id of the client
    client_id = client_ws.headers.get("sec-websocket-key","unknown")
    print(f"🔑 Client ID: {client_id}")


    # 2. Negotiate Subprotocol Smartly
    # Browsers might send: "mqtt", "mqttv3.1", or a list.
    requested_protocols = client_ws.scope.get('subprotocols', [])
    print(f"🔄 Client requested subprotocols: {requested_protocols}")
    
    # If client asks for 'mqtt', give it 'mqtt'.
    if "mqtt" in requested_protocols:
        await client_ws.accept(subprotocol="mqtt")
    # If client asks for nothing (some default browser behaviors), accept anyway.
    elif not requested_protocols:
        await client_ws.accept()
    else:
        # Fallback: Just accept the first one they asked for to keep connection alive
        await client_ws.accept(subprotocol=requested_protocols[0])



    # 3. Create Tasks
    task_ws_to_tcp = asyncio.create_task(ws_to_tcp(client_ws, writer))
    task_tcp_to_ws = asyncio.create_task(tcp_to_ws(reader, client_ws))

    # 4. CRITICAL: Wait for FIRST_COMPLETED
    # If WebSocket dies, we MUST kill the TCP task immediately.
    # If TCP dies, we MUST kill the WebSocket task immediately.
    done, pending = await asyncio.wait(
        [task_ws_to_tcp, task_tcp_to_ws],
        return_when=asyncio.FIRST_COMPLETED
    )

    # 5. AGGRESSIVE CLEANUP (Fixes the "Zombie" connection issue)
    for task in pending:
        task.cancel()
    
    # Close TCP socket immediately so Broker releases the Client ID
    writer.close()
    try:
        await writer.wait_closed()
    except:
        pass
    
    # print("🔌 Clean Disconnect")

# def main():
#     import uvicorn
#     # ws_ping_interval=None  -> Disables sending Pings (Prevents 20s disconnect)
#     # ws_ping_timeout=None   -> Disables waiting for Pongs (Prevents timeouts)
#     uvicorn.run(
#         "broker:app", 
#         host="127.0.0.1", 
#         port=8000, 
#         loop="uvloop",
#         ws_ping_interval=None, 
#         ws_ping_timeout=None,
#         log_level="info"
#     )

# if __name__ == "__main__":
#     main()

# --------------------------------------------------------------------------- #
# Imports added for the bridge fix
# --------------------------------------------------------------------------- #
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from datetime import datetime
from concurrent_collections import ConcurrentDictionary
import uuid, asyncio, json

# --------------------------------------------------------------------------- #
# Pydantic bodies
# --------------------------------------------------------------------------- #
class ConnectBody(BaseModel):
    brokerUrl: str | None = f"mqtt://{BROKER_HOST}:{BROKER_PORT}"
    brokerUsername: str | None = "demo"
    brokerPassword: str | None = "demo"
    password: str | None = None
    password: str | None = None

    keepalive: int = 60
    apiKey: str | None = None


class PublishBody(BaseModel):
    topic: str = "iot"
    payload: str = "test is send"
    qos: int = 0
    retain: bool = False


class SubscribeBody(BaseModel):
    topic: str = "iot"
    qos: int = 0
    stream: bool = False               # ← this controls /messages behavior


class UnsubscribeBody(BaseModel):
    topic: str = "iot"


class PollBody(BaseModel):
    topic: str | None = None            # None → all topics
    limit: int = 50


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #
MAX_QUEUED_MESSAGES = 1000

CONNECTION_CLIENTS = ConcurrentDictionary({})
SUBSCRIPTIONS      = ConcurrentDictionary({})


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _require_client(client_id: str):
    if CONNECTION_CLIENTS.get(client_id) is None:
        raise HTTPException(status_code=404, detail="Client not found")


def _mqtt_callback(client_id: str, topic: str, msg):
    """Thread-safe broker callback. Drops silently if client/topic gone."""
    print(f"✅ Callback: client={client_id}, topic={topic}, msg={msg}")
    try:
        if CONNECTION_CLIENTS.get(client_id) is None:
            print(f"⚠️ callback error (client={client_id}, topic={topic}): invalid clinet")
            return
        with CONNECTION_CLIENTS.get_locked(client_id) as v:
            store = v.get("messages", {}).get(topic)
            if store is None:
                print(f"⚠️ callback error (client={client_id}, topic={topic}): invalid store")
                return
            queue = store["queue"]
            if len(queue) >= MAX_QUEUED_MESSAGES:
                queue.pop(0)
            queue.append({
                "topic":     topic,
                "payload":   msg,
                "timestamp": datetime.now().isoformat(),
            })
    except Exception as e:
        print(f"⚠️ callback error (client={client_id}, topic={topic}): {e}")


def _is_stream_mode(client_id: str, topic: str | None) -> bool:
    """Check if the requested scope has ANY stream-enabled subscription."""
    with CONNECTION_CLIENTS.get_locked(client_id) as v:
        messages = v.get("messages", {})
        if topic:
            store = messages.get(topic)
            return store is not None and store.get("stream", False)
        return any(s.get("stream", False) for s in messages.values())


# --------------------------------------------------------------------------- #
# POST /clients/connect
# --------------------------------------------------------------------------- #
@app.post("/clients/connect")
async def connect_client(body: ConnectBody):
    if body.apiKey != BROKER_APIKEY:
        print("Invalid API Key")
        raise HTTPException(status_code=401, detail="Invalid API Key")
    
    client_id = urllib.parse.quote_plus(base64.b64encode(f'{body.brokerUsername}:{body.brokerPassword}'.encode('ascii')))
    stamp = datetime.now()
    if client_id not in CONNECTION_CLIENTS:
        CONNECTION_CLIENTS.assign_atomic(client_id, {
            "created_at":   stamp,
            "ping_at":      stamp,
            "ping_timeout": 0,
            "messages":     {},
        })

    return {
        "success":    True,
        "client_id":  client_id,
        "created_at": stamp.isoformat(),
    }


# --------------------------------------------------------------------------- #
# GET /health
# --------------------------------------------------------------------------- #
@app.get("/health")
async def health():
    return {"status": "ok", "activeClients": len(CONNECTION_CLIENTS)}


# --------------------------------------------------------------------------- #
# POST /clients/{client_id}/ping
# --------------------------------------------------------------------------- #
@app.post("/clients/{client_id}/ping")
async def ping(client_id: str):
    _require_client(client_id)
    with CONNECTION_CLIENTS.get_locked(client_id) as v:
        now = datetime.now()
        v["ping_timeout"] = (now - v["ping_at"]).total_seconds()
        v["ping_at"] = now
    return {"success": True}


# --------------------------------------------------------------------------- #
# POST /clients/{client_id}/disconnect
# --------------------------------------------------------------------------- #
@app.post("/clients/{client_id}/disconnect")
async def disconnect_client(client_id: str):
    _require_client(client_id)
    try:
        with CONNECTION_CLIENTS.get_locked(client_id) as v:
            for topic in list(v.get("messages", {})):
                try:
                    iot.unsubscribe(topic)
                except Exception:
                    pass
    except Exception:
        pass
    CONNECTION_CLIENTS.pop(client_id, None)
    SUBSCRIPTIONS.pop(client_id, None)
    return {"success": True}


# --------------------------------------------------------------------------- #
# POST /clients/{client_id}/publish
# --------------------------------------------------------------------------- #
@app.post("/clients/{client_id}/publish")
async def publish(client_id: str, body: PublishBody):
    _require_client(client_id)
    print(f"ℹ️ Publish {body}")
    iot.publish(body.topic, body.payload)
    return {"success": True}


# --------------------------------------------------------------------------- #
# POST /clients/{client_id}/subscribe
# --------------------------------------------------------------------------- #
@app.post("/clients/{client_id}/subscribe")
async def subscribe(client_id: str, body: SubscribeBody):
    _require_client(client_id)
    topic = body.topic

    with CONNECTION_CLIENTS.get_locked(client_id) as v:
        messages = v.setdefault("messages", {})
        if topic in messages:
            # update stream flag even if already subscribed
            messages[topic]["stream"] = body.stream
            return {"success": True, "topic": topic, "stream": body.stream,
                    "note": "already subscribed, stream flag updated"}
        messages[topic] = {"stream": body.stream, "queue": []}

    iot.subscribe(
        topic,
        lambda msg, _cid=client_id, _t=topic: _mqtt_callback(_cid, _t, msg),
    )
    print(f"ℹ️ Subscribe {body}")
    return {"success": True, "topic": topic, "stream": body.stream}


# --------------------------------------------------------------------------- #
# POST /clients/{client_id}/unsubscribe
# --------------------------------------------------------------------------- #
@app.post("/clients/{client_id}/unsubscribe")
async def unsubscribe(client_id: str, body: UnsubscribeBody):
    _require_client(client_id)
    topic = body.topic

    with CONNECTION_CLIENTS.get_locked(client_id) as v:
        messages = v.get("messages", {})
        if topic not in messages:
            raise HTTPException(status_code=404,
                                detail=f"Not subscribed to '{topic}'")
        del messages[topic]

    iot.unsubscribe(topic)
    return {"success": True}


# --------------------------------------------------------------------------- #
# POST /clients/{client_id}/messages   ← SINGLE ENDPOINT, DYNAMIC MODE
# --------------------------------------------------------------------------- #
@app.post("/clients/{client_id}/messages")
async def get_messages(client_id: str, body: PollBody):
    """
    • If the topic (or any topic) was subscribed with `stream: true`
      → returns an **SSE stream** that pushes messages in real-time.

    • Otherwise
      → returns a **JSON array** of drained messages (classic poll).
    """
    _require_client(client_id)
    print(f"ℹ️ Poll {body}")

    # ── decide mode from the flag set at subscribe time ──
    if body.topic and _is_stream_mode(client_id, body.topic):
        return _stream_response(client_id, body.topic)
    else:
        return _poll_response(client_id, body.topic, body.limit)


# ── poll path ──────────────────────────────────────────────────────────────── #
def _poll_response(client_id: str, topic: str | None, limit: int):
    collected: list[dict] = []

    print(f"ℹ️ _poll_response client_id:{client_id},topic:{topic}")

    with CONNECTION_CLIENTS.get_locked(client_id) as v:
        messages = v.get("messages", {})
        topics = [topic] if topic else list(messages)
        print(f"ℹ️ _poll_response client_id:{client_id},topics:{topics},messages:{messages}")

        for t in topics:
            store = messages.get(t)
            if store is None:
                print(f"⚠️ topic:{t}, store is none ")
                continue
            queue: list = store["queue"]
            take = min(len(queue), limit - len(collected))
            if take <= 0:
                print(f"⚠️ topic:{t}, take is {take} ")
                continue
            collected.extend(queue[:take])
            del queue[:take]
    print(f"ℹ️ _poll_response client_id:{client_id},collected:{collected}")

    return {
        "success":  True,
        "mode":     "poll",
        "count":    len(collected),
        "messages": collected,
    }


# ── stream path (SSE) ─────────────────────────────────────────────────────── #
def _stream_response(client_id: str, topic: str | None):

    async def _event_generator():
        while True:
            # client gone → stop
            if CONNECTION_CLIENTS.get(client_id) is None:
                break

            batch: list[dict] = []
            with CONNECTION_CLIENTS.get_locked(client_id) as v:
                messages = v.get("messages", {})
                targets = [topic] if topic else list(messages)
                for t in targets:
                    store = messages.get(t)
                    if store is None:
                        continue
                    # only drain stream-enabled topics
                    if not store.get("stream", False):
                        continue
                    batch.extend(store["queue"])
                    store["queue"].clear()

            for item in batch:
                yield f"data: {json.dumps(item)}\n\n"

            if not batch:
                await asyncio.sleep(0.1)        # 100ms idle backoff

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
        },
    )