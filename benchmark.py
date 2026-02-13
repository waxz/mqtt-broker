#!/usr/bin/env python3
"""
Benchmark & Correctness Test Suite for MQTT Broker.
"""

import requests
import threading
import time
import json
import sys
import os
import argparse
import hashlib
import uuid
import struct
import statistics
import traceback
import base64
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from requests_sse import EventSource

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
CONFIG = {
    "base_url":       "http://localhost:7860",
    "api_key":        "BROKER_APIKEY",
    "mqtt_host":      "localhost",
    "mqtt_port":      1883,
    "mqtt_ws_port":      7860,
    "mqtt_ws_path" : "/mqtt",
    "mqtt_username":  "",
    "mqtt_password":  "",
    "mqtt_transport": "websockets",        # "tcp" or "websockets"
    "tls": False
}

# Benchmark parameters
BENCH = {
    "bandwidth_msg_count":    1000,
    "bandwidth_payload_kb":   1,        # per-message payload size
    "latency_rounds":         100,
    "ordering_msg_count":     500,
    "correctness_msg_count":  200,
    "qos_levels":             [0, 1, 2],
    "timeout":                5.0,
}


def url(path: str) -> str:
    return f"{CONFIG['base_url']}{path}"


# --------------------------------------------------------------------------- #
# Pretty Printing
# --------------------------------------------------------------------------- #
class C:
    OK   = "\033[92m"
    FAIL = "\033[91m"
    WARN = "\033[93m"
    INFO = "\033[94m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    DIM  = "\033[2m"
    END  = "\033[0m"


def log_pass(m):   print(f"{C.OK}✅ PASS{C.END}  {m}")
def log_fail(m):   print(f"{C.FAIL}❌ FAIL{C.END}  {m}")
def log_info(m):   print(f"{C.INFO}ℹ️   {m}{C.END}")
def log_warn(m):   print(f"{C.WARN}⚠️   {m}{C.END}")
def log_metric(m): print(f"{C.CYAN}📊  {m}{C.END}")
def log_header(m): print(f"\n{C.BOLD}{'='*70}\n  {m}\n{'='*70}{C.END}")
def log_sub(m):    print(f"{C.DIM}    ↳ {m}{C.END}")


passed = 0
failed = 0


def check(cond: bool, label: str, detail: str = "") -> bool:
    global passed, failed
    if cond:
        log_pass(label)
        passed += 1
    else:
        log_fail(f"{label} → {detail}")
        failed += 1
    return cond


# --------------------------------------------------------------------------- #
# Result Containers
# --------------------------------------------------------------------------- #
@dataclass
class BandwidthResult:
    protocol:         str
    qos:              int
    messages_sent:    int   = 0
    messages_recv:    int   = 0
    bytes_sent:       int   = 0
    bytes_recv:       int   = 0
    send_duration_s:  float = 0.0
    recv_duration_s:  float = 0.0
    send_msg_per_s:   float = 0.0
    recv_msg_per_s:   float = 0.0
    send_mbps:        float = 0.0
    recv_mbps:        float = 0.0
    loss_pct:         float = 0.0


@dataclass
class LatencyResult:
    protocol:    str
    qos:         int
    samples:     List[float] = field(default_factory=list)
    min_ms:      float = 0.0
    max_ms:      float = 0.0
    mean_ms:     float = 0.0
    median_ms:   float = 0.0
    p95_ms:      float = 0.0
    p99_ms:      float = 0.0
    stddev_ms:   float = 0.0


@dataclass
class OrderingResult:
    protocol:       str
    qos:            int
    total_sent:     int   = 0
    total_recv:     int   = 0
    in_order:       bool  = True
    out_of_order:   int   = 0
    duplicates:     int   = 0
    missing:        int   = 0
    first_oo_index: int   = -1


@dataclass
class CorrectnessResult:
    protocol:       str
    qos:            int
    total_sent:     int   = 0
    total_recv:     int   = 0
    matched:        int   = 0
    corrupted:      int   = 0
    missing:        int   = 0
    extra:          int   = 0
    corrupt_details: List[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# MQTT Native Helper
# --------------------------------------------------------------------------- #
class MqttHelper:
    """Wraps a paho MQTT client."""

    def __init__(self, client_id: Optional[str] = None):
        cid = client_id or f"bench-{uuid.uuid4().hex[:8]}"
        self.use_ws = CONFIG["mqtt_transport"]=="websockets"
        
        if self.use_ws:
            self.client = mqtt.Client(
                callback_api_version=CallbackAPIVersion.VERSION2,
                client_id=cid,
                transport="websockets",
                protocol=mqtt.MQTTv311,
            )
            self.client.ws_set_options(path=CONFIG["mqtt_ws_path"], headers={"Sec-WebSocket-Protocol": "mqtt"})
        else:
            self.client = mqtt.Client(
                callback_api_version=CallbackAPIVersion.VERSION2,
                client_id=cid,
                transport="tcp",
            )

        if CONFIG["tls"]:
            self.client.tls_set()

        if CONFIG["mqtt_username"]:
            self.client.username_pw_set(
                CONFIG["mqtt_username"], CONFIG["mqtt_password"]
            )

        self._connected = threading.Event()
        self._subscribed = threading.Event()
        self._lock = threading.Lock()
        self.received: List[dict] = []
        self.recv_times: List[float] = []

        self.client.on_connect    = self._on_connect
        self.client.on_message    = self._on_message
        self.client.on_subscribe  = self._on_subscribe

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0 or (hasattr(rc, 'value') and rc.value == 0):
            self._connected.set()

    def _on_subscribe(self, client, userdata, mid, rc_list, properties=None):
        self._subscribed.set()

    def _on_message(self, client, userdata, msg):
        ts = time.perf_counter()
        with self._lock:
            self.received.append({
                "topic":   msg.topic,
                "payload": msg.payload,
                "qos":     msg.qos,
                "ts":      ts,
            })
            self.recv_times.append(ts)

    def connect(self, timeout=10):
        if self.use_ws:
            self.client.connect(CONFIG["mqtt_host"], CONFIG["mqtt_ws_port"])
        else:
            self.client.connect(CONFIG["mqtt_host"], CONFIG["mqtt_port"])
        self.client.loop_start()
        if not self._connected.wait(timeout):
            raise TimeoutError("MQTT connect timeout")

    def subscribe(self, topic, qos=0, timeout=10):
        self._subscribed.clear()
        self.client.subscribe(topic, qos)
        if not self._subscribed.wait(timeout):
            raise TimeoutError("MQTT subscribe timeout")

    def publish(self, topic, payload, qos=0):
        return self.client.publish(topic, payload, qos=qos)

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def wait_for(self, count, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if len(self.received) >= count:
                    return True
            time.sleep(0.01)
        return False


# --------------------------------------------------------------------------- #
# HTTP Bridge Helper
# --------------------------------------------------------------------------- #
class HttpHelper:
    """Wraps the HTTP-to-MQTT bridge."""

    def __init__(self):
        self.client_id: Optional[str] = None
        self._lock = threading.Lock()
        self.received: List[dict] = []
        self.recv_times: List[float] = []

        self._sse_thread: Optional[threading.Thread] = None
        self._sse_stop = threading.Event()
        self._buffer: List[dict] = []
        self._buffer_lock = threading.Lock()
        self._buffer_ready = threading.Event()

    def connect(self, start_sse=True):
        r = requests.post(
            url("/clients/connect"),
            json={"apiKey": CONFIG["api_key"]},
            timeout=10,
        )
        data = r.json()
        if not data.get("success"):
            raise RuntimeError(f"HTTP connect failed: {data}")
        self.client_id = data["client_id"]
        if start_sse:
            self._start_sse_listener()

    def disconnect(self):
        self._stop_sse_listener()
        if self.client_id:
            try:
                requests.post(
                    url(f"/clients/{self.client_id}/disconnect"), timeout=10
                )
            except Exception:
                pass
            self.client_id = None

    def _start_sse_listener(self):
        self._sse_stop.clear()
        self._sse_thread = threading.Thread(
            target=self._sse_loop,
            daemon=True,
            name=f"sse-{self.client_id[:8]}",
        )
        self._sse_thread.start()

    def _stop_sse_listener(self):
        self._sse_stop.set()
        self._buffer_ready.set()
        if self._sse_thread is not None and self._sse_thread.is_alive():
            self._sse_thread.join(timeout=5)
        self._sse_thread = None

    def _sse_loop(self):
        endpoint = url(f"/clients/{self.client_id}/messages")
        while not self._sse_stop.is_set():
            try:
                with EventSource(endpoint, timeout=60, method="POST") as source:
                    for event in source:
                        if self._sse_stop.is_set():
                            return
                        if event.data:
                            try:
                                data = json.loads(event.data)
                                with self._buffer_lock:
                                    if isinstance(data, list):
                                        self._buffer.extend(data)
                                    else:
                                        self._buffer.append(data)
                                    self._buffer_ready.set()
                            except:
                                pass
            except Exception:
                if self._sse_stop.is_set():
                    return
                time.sleep(0.5)

    def subscribe(self, topic: str, qos: int = 0):
        r = requests.post(
            url(f"/clients/{self.client_id}/subscribe"),
            json={"topic": topic, "qos": qos},
            timeout=10,
        )
        if r.status_code != 200:
            raise RuntimeError(f"HTTP subscribe failed: {r.text}")

    def publish(self, topic: str, payload, qos: int = 0):
        body: Dict = {"topic": topic, "qos": qos}
        if isinstance(payload, bytes):
            body["payload"] = base64.b64encode(payload).decode()
        else:
            body["payload"] = payload
        return requests.post(
            url(f"/clients/{self.client_id}/publish"),
            json=body,
            timeout=10,
        )

    def publish_batch(self, messages: list):
        return requests.post(
            url(f"/clients/{self.client_id}/publish/batch"),
            json={"messages": messages},
            timeout=10,
        )

    def _drain(self, topic: Optional[str] = None, limit: int = 1000) -> List[dict]:
        with self._buffer_lock:
            if topic is not None:
                matched = []
                remaining = []
                for m in self._buffer:
                    if len(matched) < limit and m.get("topic") == topic:
                        matched.append(m)
                    else:
                        remaining.append(m)
                self._buffer = remaining
            else:
                matched = self._buffer[:limit]
                self._buffer = self._buffer[limit:]
            if not self._buffer:
                self._buffer_ready.clear()
            return matched

    def poll(self, topic: Optional[str] = None, limit: int = 1000) -> List[dict]:
        return self._drain(topic, limit)

    def poll_until(self, topic: str, count: int, timeout: float = 30.0) -> List[dict]:
        collected: List[dict] = []
        deadline = time.time() + timeout
        while len(collected) < count:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            self._buffer_ready.wait(timeout=min(remaining, 0.5))
            batch = self._drain(topic)
            for m in batch:
                ts = time.perf_counter()
                collected.append(m)
                self.recv_times.append(ts)
        with self._lock:
            self.received.extend(collected)
        return collected


# --------------------------------------------------------------------------- #
# Payload Helpers
# --------------------------------------------------------------------------- #
def make_payload(size_bytes: int, seq: int = 0) -> bytes:
    header_len = 4 + 32
    pad_len = max(0, size_bytes - header_len)
    padding = os.urandom(pad_len)
    seq_bytes = struct.pack("!I", seq)
    raw = seq_bytes + padding
    sha = hashlib.sha256(raw).digest()
    return seq_bytes + sha + padding


def verify_payload(data: bytes) -> Tuple[int, bool]:
    if len(data) < 36: return -1, False
    seq = struct.unpack("!I", data[:4])[0]
    stored_sha = data[4:36]
    raw = data[:4] + data[36:]
    actual_sha = hashlib.sha256(raw).digest()
    return seq, (stored_sha == actual_sha)


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def bench_bandwidth_mqtt(qos: int = 0) -> BandwidthResult:
    result = BandwidthResult(protocol="MQTT", qos=qos)
    topic = f"bench/bw/mqtt/{qos}/{uuid.uuid4().hex[:6]}"
    payload_size = BENCH["bandwidth_payload_kb"] * 1024
    count = BENCH["bandwidth_msg_count"]
    pub = MqttHelper(); sub = MqttHelper()
    try:
        sub.connect(); sub.subscribe(topic, qos)
        time.sleep(0.2); pub.connect()
        payloads = [make_payload(payload_size, i) for i in range(count)]
        result.bytes_sent = sum(len(p) for p in payloads)
        result.messages_sent = count
        t0 = time.perf_counter()
        for p in payloads:
            info = pub.publish(topic, p, qos=qos)
            if qos > 0: info.wait_for_publish(timeout=10)
        result.send_duration_s = time.perf_counter() - t0
        sub.wait_for(count, timeout=BENCH["timeout"])
        with sub._lock:
            result.messages_recv = len(sub.received)
            result.bytes_recv = sum(len(m["payload"]) for m in sub.received)
            if sub.recv_times: result.recv_duration_s = sub.recv_times[-1] - t0
        if result.send_duration_s > 0:
            result.send_msg_per_s = count / result.send_duration_s
            result.send_mbps = (result.bytes_sent * 8) / (result.send_duration_s * 1e6)
        if result.recv_duration_s > 0:
            result.recv_msg_per_s = result.messages_recv / result.recv_duration_s
            result.recv_mbps = (result.bytes_recv * 8) / (result.recv_duration_s * 1e6)
        result.loss_pct = ((count - result.messages_recv) / count * 100) if count > 0 else 0
    finally:
        pub.disconnect(); sub.disconnect()
    return result

def bench_bandwidth_http(qos: int = 0) -> BandwidthResult:
    result = BandwidthResult(protocol="HTTP", qos=qos)
    topic = f"bench/bw/http/{qos}/{uuid.uuid4().hex[:6]}"
    count = BENCH["bandwidth_msg_count"]
    pub = HttpHelper(); sub = HttpHelper()
    try:
        pub.connect(start_sse=False); sub.connect(); sub.subscribe(topic, qos)
        time.sleep(0.3)
        # Use batch publish for true bandwidth test
        batch_size = 100
        result.messages_sent = count
        t0 = time.perf_counter()
        for i in range(0, count, batch_size):
            chunk = [{"topic": topic, "payload": f"msg-{j}", "qos": qos} for j in range(i, min(i+batch_size, count))]
            pub.publish_batch(chunk)
        result.send_duration_s = time.perf_counter() - t0
        collected = sub.poll_until(topic, count, timeout=BENCH["timeout"])
        result.recv_duration_s = time.perf_counter() - t0
        result.messages_recv = len(collected)
        if result.send_duration_s > 0: result.send_msg_per_s = count / result.send_duration_s
        if result.recv_duration_s > 0: result.recv_msg_per_s = len(collected) / result.recv_duration_s
        result.loss_pct = ((count - len(collected)) / count * 100) if count > 0 else 0
    finally:
        pub.disconnect(); sub.disconnect()
    return result

def test_bandwidth(avail):
    log_header("📶  BANDWIDTH BENCHMARK")
    for qos in BENCH["qos_levels"]:
        if "mqtt" in avail:
            r = bench_bandwidth_mqtt(qos)
            log_metric(f"[MQTT] QoS {qos}: {r.send_msg_per_s:.0f} msg/s send, {r.recv_msg_per_s:.0f} msg/s recv, {r.loss_pct:.2f}% loss")
        if "http" in avail:
            r = bench_bandwidth_http(qos)
            log_metric(f"[HTTP] QoS {qos}: {r.send_msg_per_s:.0f} msg/s send, {r.recv_msg_per_s:.0f} msg/s recv, {r.loss_pct:.2f}% loss")

def test_latency(avail):
    log_header("⏱  LATENCY BENCHMARK")
    for qos in BENCH["qos_levels"]:
        if "mqtt" in avail:
            h = MqttHelper(); h.connect(); h.subscribe("lat/mqtt", qos); time.sleep(0.2)
            lats = []
            for _ in range(BENCH["latency_rounds"]):
                t0 = time.perf_counter(); ev = threading.Event()
                h.client.on_message = lambda c,u,m: ev.set()
                h.publish("lat/mqtt", b"x", qos=qos)
                if ev.wait(timeout=2.0): lats.append((time.perf_counter() - t0)*1000)
            if lats: log_metric(f"[MQTT] QoS {qos}: Avg {statistics.mean(lats):.2f}ms, P95 {statistics.quantiles(lats, n=20)[18]:.2f}ms")
            h.disconnect()
        if "http" in avail:
            h = HttpHelper(); h.connect(); h.subscribe("lat/http", qos); time.sleep(0.2)
            lats = []
            for _ in range(BENCH["latency_rounds"]):
                t0 = time.perf_counter(); h.publish("lat/http", "x", qos=qos)
                while time.perf_counter()-t0 < 2.0:
                    if h.poll("lat/http", limit=1):
                        lats.append((time.perf_counter() - t0)*1000); break
                    time.sleep(0.001)
            if lats: log_metric(f"[HTTP] QoS {qos}: Avg {statistics.mean(lats):.2f}ms, P95 {statistics.quantiles(lats, n=20)[18]:.2f}ms")
            h.disconnect()

def test_ordering(avail):
    log_header("📐  ORDERING TEST")
    count = BENCH["ordering_msg_count"]
    if "mqtt" in avail:
        sub = MqttHelper(); sub.connect(); sub.subscribe("ord/mqtt", 1); time.sleep(0.2)
        pub = MqttHelper(); pub.connect()
        for i in range(count): pub.publish("ord/mqtt", struct.pack("!I", i), qos=1)
        sub.wait_for(count, timeout=5.0)
        seqs = [struct.unpack("!I", m["payload"])[0] for m in sub.received]
        ooo = sum(1 for i in range(1, len(seqs)) if seqs[i] < seqs[i-1])
        log_metric(f"[MQTT] Received {len(seqs)}/{count}, Out-of-order: {ooo}")
        sub.disconnect(); pub.disconnect()
    if "http" in avail:
        sub = HttpHelper(); sub.connect(); sub.subscribe("ord/http", 1); time.sleep(0.2)
        pub = HttpHelper(); pub.connect(start_sse=False)
        for i in range(count): pub.publish("ord/http", json.dumps({"s":i}), qos=1)
        msgs = sub.poll_until("ord/http", count, timeout=5.0)
        seqs = [m.get("payload",{}).get("s") if isinstance(m.get("payload"),dict) else -1 for m in msgs]
        ooo = sum(1 for i in range(1, len(seqs)) if seqs[i] is not None and seqs[i-1] is not None and seqs[i] < seqs[i-1])
        log_metric(f"[HTTP] Received {len(seqs)}/{count}, Out-of-order: {ooo}")
        sub.disconnect(); pub.disconnect()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", choices=["all", "bandwidth", "latency", "ordering", "health"], default="all")
    parser.add_argument("--protocol", choices=["mqtt", "http", "both"], default="both")
    args = parser.parse_args()
    
    log_info(f"Starting benchmark: test={args.test}, protocol={args.protocol}")
    
    avail = []
    try:
        h = MqttHelper(); h.connect(timeout=5); avail.append("mqtt"); h.disconnect()
        log_pass("MQTT broker reachable")
    except: log_fail("MQTT broker unreachable")
    
    try:
        r = requests.get(url("/health"), timeout=2)
        if r.status_code == 200: avail.append("http"); log_pass("HTTP bridge reachable")
    except: log_fail("HTTP bridge unreachable")
    
    if not avail: sys.exit(1)
    if args.protocol != "both": avail = [p for p in avail if p == args.protocol]
    if not avail: sys.exit(1)

    if args.test in ["all", "bandwidth"]: test_bandwidth(avail)
    if args.test in ["all", "latency"]: test_latency(avail)
    if args.test in ["all", "ordering"]: test_ordering(avail)

if __name__ == "__main__":
    main()
