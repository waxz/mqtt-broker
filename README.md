---
title: High-Performance MQTT HTTP Bridge
emoji: 🚀
colorFrom: indigo
colorTo: blue
sdk: docker
pinned: false
license: mit
short_description: A high-concurrency, low-latency MQTT-HTTP bridge.
---

# High-Performance MQTT HTTP Bridge

This project provides a robust, high-speed conduit between the industrial MQTT world and the modern Web. Optimized for single-core efficiency and massive throughput, it enables seamless IoT data integration via HTTP, SSE, and WebSockets.

## 🚀 Performance & Reliability
On a single-core Intel(R) Xeon(R) CPU @ 2.20GHz, this bridge has achieved:
- **Throughput:** ~2,300 msg/s (HTTP-to-MQTT with SSE delivery).
- **Reliability:** 100% message delivery and 0% out-of-order delivery across all QoS levels.
- **Latency:** Average HTTP bridge latency of ~15ms (including SSE batching).

## 🛠 Design Principles (The Zen of the Bridge)
The architecture is guided by five core engineering mandates:
1. **Zero-Latency Mindset:** Hot-paths are optimized using **Cython** and **orjson** to eliminate Python interpreter overhead.
2. **Data Flow as Fluid Dynamics:** IO pressure is managed via **Batching** at every layer—from SSE event streams to bulk HTTP publishing.
3. **Non-Blocking Harmony:** Threaded MQTT callbacks and the Async loop communicate via quiet, lock-protected reservoirs to prevent "notification storms."
4. **Deterministic Reliability:** A **Handshake-First** lifecycle ensures client state is perfectly synchronized before data flows.
5. **Session Isolation & Safety:** Unique session IDs by default prevent cross-tab collisions, while a **dual-layered memory defense** (TTL + Buffer Cap) prevents memory exhaustion.

## 📦 Build & Run

### Dependency
- **iotcore:** High-performance MQTT core (Rust-based). [Source](https://github.com/waxz/iotcore)

### Build Extensions
```bash
python pyx/setup.py build_ext -b ./
```

### Run Server
```bash
python run_broker.py > /dev/null 2>&1
```
### Run benchmark
```bash
python ./run_broker.py > broker.log 2>&1 & pid=$!; for i in {1..20}; do grep -q "Uvicorn running" broker.log && echo "Broker ready" && break; sleep 1; done; python ./benchmark.py --test all; kill $pid
```

## 🗺 Roadmap (Phase 3)
- **Modern Web Client:** Production-ready `MqttSseClient` with WebWorker offloading support.
- **Multi-Process Pipelining:** Splitting the Rust MQTT loop and the FastAPI server into separate processes to fully bypass the GIL.
- **Rust Core Migration:** Porting internal state management into a dedicated Rust extension for near-native performance.

---
### Reference
- https://huggingface.co/docs/hub/spaces-config-reference
- https://discuss.huggingface.co/t/hf-space-stuck-at-starting/170911/2
- https://stackoverflow.com/questions/35244333/in-general-does-redirecting-outputs-to-dev-null-enhance-performance
- https://askubuntu.com/questions/350208/what-does-2-dev-null-mean
