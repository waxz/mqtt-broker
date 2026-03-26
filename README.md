---
title: High-Performance MQTT HTTP Bridge
emoji: 🚀
colorFrom: indigo
colorTo: blue
sdk: docker
pinned: false
license: mit
short_description: A high-concurrency, low-latency MQTT-HTTP bridge with WebSocket support.
---

# High-Performance MQTT HTTP Bridge

A robust, high-speed conduit between industrial MQTT and the modern Web. Achieves ~70,000+ msg/s MQTT throughput with Cython optimizations and supports HTTP, SSE, WebSocket, and NDJSON streaming.

## 🚀 Performance

### Localhost (Verified 2026-03-26)
| Protocol | Send Throughput | Recv Throughput | Latency |
|----------|-----------------|-----------------|---------|
| MQTT (TCP) | ~70,000 msg/s | ~800 msg/s | ~1.5ms |
| MQTT (WebSocket) | ~65,000 msg/s | ~800 msg/s | ~1.7ms |
| HTTP (SSE/NDJSON) | ~10,000 msg/s | ~5,000 msg/s | ~8ms |
| WebSocket API | ~15,000 msg/s | ~20-60 msg/s | ~8ms |

### Cloudflare/HuggingFace Space (Remote)
| Environment | Latency | Throughput |
|-------------|---------|------------|
| Cloudflare HTTP | ~43ms | ~1,000 msg/s |
| HuggingFace Space | ~100ms | ~500 msg/s |

**Note:** MQTT requires direct TCP access (port 1883) - not available over Cloudflare tunnels. Use WebSocket/MQTT or HTTP API instead.

## 🎯 Features

- **MQTT Broker** - Built-in high-performance MQTT broker (iotcore/Rust)
- **HTTP REST API** - Full CRUD operations for clients, topics, and messages
- **Server-Sent Events (SSE)** - Real-time message streaming with automatic batching
- **NDJSON Streaming** - Newline-delimited JSON for lower overhead than SSE
- **WebSocket** - Bidirectional communication for both MQTT proxy and HTTP API clients
- **Fire-and-Forget Publish** - Fastest publish mode via `/publish/fire`
- **GZip Compression** - Automatic compression for responses >256 bytes
- **Cython Optimizations** - Hot paths use orjson and compiled extensions

## 📦 Quick Start

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Build Cython Extensions (Optional but Recommended)
```bash
python pyx/setup.py build_ext --inplace
```

### Run Broker
```bash
python run_broker.py
```

### Run Benchmarks
```bash
# Python benchmark
python benchmark.py --test all

# Node.js benchmark (requires Node.js)
node benchmark.js --test all
```

## 🔌 API Reference

### Connect Client
```bash
curl -X POST http://localhost:7860/clients/connect \
  -H "Content-Type: application/json" \
  -d '{"apiKey": "BROKER_APIKEY", "clientId": "my-client"}'
```

### Subscribe to Topic
```bash
curl -X POST http://localhost:7860/clients/{client_id}/subscribe \
  -H "Content-Type: application/json" \
  -d '{"topic": "arena-ai/{session_id}/response", "qos": 0}'
```

### Publish Message
```bash
curl -X POST http://localhost:7860/clients/{client_id}/publish \
  -H "Content-Type: application/json" \
  -d '{"topic": "arena-ai/{session_id}/request", "payload": "Hello", "qos": 0}'
```

### Receive Messages (SSE)
```bash
curl -X POST http://localhost:7860/clients/{client_id}/messages \
  -H "Accept: text/event-stream"
```

### Receive Messages (NDJSON)
```bash
curl -X POST http://localhost:7860/clients/{client_id}/messages/stream \
  -H "Accept: application/x-ndjson"
```

### WebSocket (MQTT Proxy)
```javascript
const mqtt = require('mqtt');
const client = mqtt.connect('ws://localhost:7860/ws', {
  clientId: 'my-client',
  protocolVersion: 4,
  path: '/ws'
});
client.on('connect', () => {
  client.subscribe('topic/#');
});
client.on('message', (topic, msg) => {
  console.log(msg.toString());
});
```

### WebSocket (HTTP API Client)
```javascript
const WebSocket = require('ws');
const ws = new WebSocket('ws://localhost:7860/ws/{client_id}');
ws.onopen = () => console.log('Connected');
ws.onmessage = (event) => {
  const messages = JSON.parse(event.data);
  console.log(messages);
};
```

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Clients                                 │
│   (Browser, IoT Devices, External Services)                 │
└─────────────────────┬─────────────────────────────────────┘
                      │
         ┌────────────┼────────────┐
         │            │            │
         ▼            ▼            ▼
    ┌─────────┐  ┌─────────┐  ┌─────────┐
    │   HTTP  │  │  MQTT   │  │WebSocket│
    │   API   │  │  (TCP)  │  │  Proxy  │
    └────┬────┘  └────┬────┘  └────┬────┘
         │            │            │
         └────────────┼────────────┘
                      │
         ┌────────────▼────────────┐
         │     broker_app.py        │
         │   (FastAPI + uvloop)    │
         │                         │
         │  ┌─────────────────┐    │
         │  │ TopicMultiplexer│    │
         │  │ (Message Fan-out)│    │
         │  └────────┬────────┘    │
         │           │             │
         └───────────┼─────────────┘
                     │
         ┌───────────▼───────────┐
         │      iotcore          │
         │   (Rust MQTT Broker)   │
         └───────────────────────┘
```

## ⚙️ Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BROKER_HOST` | 127.0.0.1 | MQTT broker host |
| `BROKER_PORT` | 1883 | MQTT broker port |
| `BROKER_APIKEY` | BROKER_APIKEY | API authentication |
| `MAX_QUEUED` | 1000 | Max messages per topic buffer |
| `MSG_TTL` | 300 | Message TTL in seconds |
| `STALE_SECONDS` | 300 | Client stale timeout |
| `BROKER_LOG_LEVEL` | WARNING | Logging level |

## 🌐 Cloudflare/HuggingFace Deployment

### Performance Tips
1. **Use NDJSON Stream** (`POST /clients/{id}/messages/stream`) - lower overhead than SSE
2. **Enable GZip** - auto-enabled for responses >256 bytes
3. **Use batch publish** - `POST /clients/{id}/publish/batch` for bulk sends
4. **Use fire-and-forget** - `POST /clients/{id}/publish/fire` for fastest publish
5. **Use WebSocket MQTT** - Direct MQTT over WebSocket bypasses HTTP overhead

### Known Limitations
- MQTT requires direct TCP access (port 1883) - not available over Cloudflare
- Use WebSocket/MQTT or HTTP API over cloudflared tunnel
- Cloudflare QUIC adds ~40ms latency

## 📁 Project Structure

```
mqtt-broker/
├── broker_app.py       # Main MQTT broker/HTTP bridge
├── api_server.py       # OpenAI-compatible MQTT proxy
├── benchmark.py        # Python performance test suite
├── benchmark.js        # Node.js performance test suite
├── run_broker.py      # Broker startup script
├── pyx/               # Cython extension modules
├── requirements.txt   # Python dependencies
└── package.json      # Node.js dependencies
```

## 📜 License

MIT
