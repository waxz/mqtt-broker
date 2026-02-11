#!/usr/bin/env python3
"""
Benchmark & Correctness Test Suite for MQTT Broker.

Tests both the HTTP-to-MQTT Bridge and native MQTT protocol for:
  • Bandwidth / throughput
  • Latency / delay
  • Message ordering
  • Payload correctness

Usage:
    pip install requests paho-mqtt
    python benchmark_broker.py
    python benchmark_broker.py --test bandwidth
    python benchmark_broker.py --test latency
    python benchmark_broker.py --test ordering
    python benchmark_broker.py --test correctness
    python benchmark_broker.py --test all
    python benchmark_broker.py --protocol mqtt        # MQTT-only
    python benchmark_broker.py --protocol http        # HTTP-only
    python benchmark_broker.py --protocol both        # default
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
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

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
    "mqtt_transport": "tcp",        # "tcp" or "websockets"
}

# Benchmark parameters
BENCH = {
    "bandwidth_msg_count":    1000,
    "bandwidth_payload_kb":   1,        # per-message payload size
    "latency_rounds":         100,
    "ordering_msg_count":     500,
    "correctness_msg_count":  200,
    "qos_levels":             [0, 1, 2],
    "timeout":                30.0,
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
    """Wraps a paho MQTT client with callbacks wired into collection lists."""

    def __init__(self, client_id: Optional[str] = None):
        cid = client_id or f"bench-{uuid.uuid4().hex[:8]}"
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=cid,
            transport=CONFIG["mqtt_transport"],
        )
        client_ws = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=cid,
            transport="websockets",
            protocol=mqtt.MQTTv311,
        )
        client_ws.ws_set_options(path=CONFIG["mqtt_ws_path"], headers={"Sec-WebSocket-Protocol": "mqtt"})
        self.use_ws = True

        if self.use_ws:
            self.client = client_ws
        else:
            self.client = client




        if CONFIG["mqtt_username"]:
            self.client.username_pw_set(
                CONFIG["mqtt_username"], CONFIG["mqtt_password"]
            )

        self._connected = threading.Event()
        self._subscribed = threading.Event()
        self._lock = threading.Lock()
        self.received: List[dict] = []
        self.recv_times: List[float] = []
        self.pub_complete: Dict[int, float] = {}

        self.client.on_connect    = self._on_connect
        self.client.on_message    = self._on_message
        self.client.on_subscribe  = self._on_subscribe
        self.client.on_publish    = self._on_publish

    # -- callbacks ---
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

    def _on_publish(self, client, userdata, mid, rc=None, properties=None):
        self.pub_complete[mid] = time.perf_counter()

    # -- helpers ---
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
        info = self.client.publish(topic, payload, qos=qos)
        return info

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def clear(self):
        with self._lock:
            self.received.clear()
            self.recv_times.clear()
        self.pub_complete.clear()

    def wait_for(self, count, timeout=30):
        """Block until we have >= count messages or timeout."""
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
    """Wraps the HTTP-to-MQTT bridge for benchmark tests."""

    def __init__(self):
        self.client_id: Optional[str] = None
        self._lock = threading.Lock()
        self.received: List[dict] = []
        self.recv_times: List[float] = []

    def connect(self):
        r = requests.post(url("/clients/connect"),
                          json={"apiKey": CONFIG["api_key"]}, timeout=10)
        data = r.json()
        if not data.get("success"):
            raise RuntimeError(f"HTTP connect failed: {data}")
        self.client_id = data["client_id"]

    def subscribe(self, topic: str, qos: int = 0, stream: bool = False):
        r = requests.post(url(f"/clients/{self.client_id}/subscribe"),
                          json={"topic": topic, "qos": qos, "stream": stream},
                          timeout=10)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP subscribe failed: {r.text}")

    def publish(self, topic: str, payload, qos: int = 0):
        body = {"topic": topic, "qos": qos}
        if isinstance(payload, bytes):
            import base64
            body["payload"] = base64.b64encode(payload).decode()
            body["encoding"] = "base64"
        else:
            body["payload"] = payload
        r = requests.post(url(f"/clients/{self.client_id}/publish"),
                          json=body, timeout=10)
        return r

    def poll(self, topic: Optional[str] = None, limit: int = 1000) -> list:
        body: dict = {"limit": limit}
        if topic:
            body["topic"] = topic
        r = requests.post(url(f"/clients/{self.client_id}/messages"),
                          json=body, timeout=10)
        data = r.json()
        return data.get("messages", [])

    def poll_until(self, topic: str, count: int,
                   timeout: float = 30.0) -> List[dict]:
        collected: List[dict] = []
        deadline = time.time() + timeout
        while time.time() < deadline and len(collected) < count:
            msgs = self.poll(topic)
            ts = time.perf_counter()
            for m in msgs:
                m["ts"] = ts
                collected.append(m)
                self.recv_times.append(ts)
            if len(collected) < count:
                time.sleep(0.05)
        with self._lock:
            self.received.extend(collected)
        return collected

    def unsubscribe(self, topic: str):
        requests.post(url(f"/clients/{self.client_id}/unsubscribe"),
                      json={"topic": topic}, timeout=10)

    def disconnect(self):
        if self.client_id:
            requests.post(url(f"/clients/{self.client_id}/disconnect"),
                          timeout=10)
            self.client_id = None

    def clear(self):
        with self._lock:
            self.received.clear()
            self.recv_times.clear()


# --------------------------------------------------------------------------- #
# Payload Generators
# --------------------------------------------------------------------------- #
def make_payload(size_bytes: int, seq: int = 0) -> bytes:
    """Create a payload of exact size with embedded sequence + checksum."""
    # Header: 4B seq | 32B sha256 | rest padding
    header_len = 4 + 32
    pad_len = max(0, size_bytes - header_len)
    padding = os.urandom(pad_len)
    seq_bytes = struct.pack("!I", seq)
    raw = seq_bytes + padding
    sha = hashlib.sha256(raw).digest()
    return seq_bytes + sha + padding


def verify_payload(data: bytes) -> Tuple[int, bool]:
    """Extract sequence and verify checksum. Returns (seq, ok)."""
    if len(data) < 36:
        return -1, False
    seq = struct.unpack("!I", data[:4])[0]
    stored_sha = data[4:36]
    raw = data[:4] + data[36:]
    actual_sha = hashlib.sha256(raw).digest()
    return seq, (stored_sha == actual_sha)


def make_json_payload(seq: int, extra_kb: int = 0) -> str:
    """Create a JSON payload with sequence number and optional padding."""
    obj = {
        "seq":       seq,
        "ts":        time.time(),
        "uid":       uuid.uuid4().hex,
        "checksum":  "",
    }
    if extra_kb > 0:
        obj["padding"] = "X" * (extra_kb * 1024)
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    obj["checksum"] = hashlib.md5(
        json.dumps({k: v for k, v in obj.items() if k != "checksum"},
                   sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def verify_json_payload(raw: str) -> Tuple[int, bool]:
    """Verify JSON payload checksum. Returns (seq, ok)."""
    try:
        obj = json.loads(raw)
        seq = obj.get("seq", -1)
        expected_checksum = obj.get("checksum", "")
        verify_obj = {k: v for k, v in obj.items() if k != "checksum"}
        actual = hashlib.md5(
            json.dumps(verify_obj, sort_keys=True,
                       separators=(",", ":")).encode()
        ).hexdigest()
        return seq, (expected_checksum == actual)
    except Exception:
        return -1, False


# --------------------------------------------------------------------------- #
# ██████  Bandwidth Tests
# --------------------------------------------------------------------------- #
def bench_bandwidth_mqtt(qos: int = 0) -> BandwidthResult:
    """Measure raw MQTT throughput."""
    result = BandwidthResult(protocol="MQTT", qos=qos)
    topic = f"bench/bandwidth/mqtt/{qos}/{uuid.uuid4().hex[:6]}"
    payload_size = BENCH["bandwidth_payload_kb"] * 1024
    count = BENCH["bandwidth_msg_count"]

    pub = MqttHelper(f"bench-bw-pub-{uuid.uuid4().hex[:6]}")
    sub = MqttHelper(f"bench-bw-sub-{uuid.uuid4().hex[:6]}")

    try:
        sub.connect()
        sub.subscribe(topic, qos)
        time.sleep(0.3)

        pub.connect()
        time.sleep(0.2)

        # --- send ---
        payloads = [make_payload(payload_size, i) for i in range(count)]
        result.bytes_sent = sum(len(p) for p in payloads)
        result.messages_sent = count

        t0 = time.perf_counter()
        for i, p in enumerate(payloads):
            info = pub.publish(topic, p, qos=qos)
            if qos > 0:
                info.wait_for_publish(timeout=10)
        send_done = time.perf_counter()
        result.send_duration_s = send_done - t0

        # --- receive ---
        sub.wait_for(count, timeout=BENCH["timeout"])
        recv_done = time.perf_counter()

        with sub._lock:
            result.messages_recv = len(sub.received)
            result.bytes_recv = sum(len(m["payload"]) for m in sub.received)
            if sub.recv_times:
                result.recv_duration_s = sub.recv_times[-1] - t0

        # --- metrics ---
        if result.send_duration_s > 0:
            result.send_msg_per_s = result.messages_sent / result.send_duration_s
            result.send_mbps = (result.bytes_sent * 8) / (result.send_duration_s * 1e6)
        if result.recv_duration_s > 0:
            result.recv_msg_per_s = result.messages_recv / result.recv_duration_s
            result.recv_mbps = (result.bytes_recv * 8) / (result.recv_duration_s * 1e6)
        if result.messages_sent > 0:
            result.loss_pct = (
                (result.messages_sent - result.messages_recv)
                / result.messages_sent * 100
            )

    finally:
        pub.disconnect()
        sub.disconnect()

    return result


def bench_bandwidth_http(qos: int = 0) -> BandwidthResult:
    """Measure HTTP bridge throughput."""
    result = BandwidthResult(protocol="HTTP", qos=qos)
    topic = f"bench/bandwidth/http/{qos}/{uuid.uuid4().hex[:6]}"
    count = BENCH["bandwidth_msg_count"]

    pub_h = HttpHelper()
    sub_h = HttpHelper()

    try:
        pub_h.connect()
        sub_h.connect()
        sub_h.subscribe(topic, qos=qos, stream=False)
        time.sleep(0.3)

        # --- send ---
        payloads = [make_json_payload(i, extra_kb=BENCH["bandwidth_payload_kb"])
                    for i in range(count)]
        result.bytes_sent = sum(len(p.encode()) for p in payloads)
        result.messages_sent = count

        t0 = time.perf_counter()
        for p in payloads:
            pub_h.publish(topic, p, qos=qos)
        send_done = time.perf_counter()
        result.send_duration_s = send_done - t0

        # --- receive ---
        collected = sub_h.poll_until(topic, count, timeout=BENCH["timeout"])
        recv_done = time.perf_counter()

        result.messages_recv = len(collected)
        result.bytes_recv = sum(
            len(m.get("payload", "").encode() if isinstance(m.get("payload"), str)
                else m.get("payload", b""))
            for m in collected
        )
        result.recv_duration_s = recv_done - t0

        if result.send_duration_s > 0:
            result.send_msg_per_s = result.messages_sent / result.send_duration_s
            result.send_mbps = (result.bytes_sent * 8) / (result.send_duration_s * 1e6)
        if result.recv_duration_s > 0:
            result.recv_msg_per_s = result.messages_recv / result.recv_duration_s
            result.recv_mbps = (result.bytes_recv * 8) / (result.recv_duration_s * 1e6)
        if result.messages_sent > 0:
            result.loss_pct = (
                (result.messages_sent - result.messages_recv)
                / result.messages_sent * 100
            )

    finally:
        sub_h.unsubscribe(topic)
        pub_h.disconnect()
        sub_h.disconnect()

    return result


def print_bandwidth(r: BandwidthResult):
    log_metric(f"[{r.protocol}] QoS {r.qos} Bandwidth")
    log_sub(f"Sent:       {r.messages_sent} msgs, {r.bytes_sent/1024:.1f} KB")
    log_sub(f"Received:   {r.messages_recv} msgs, {r.bytes_recv/1024:.1f} KB")
    log_sub(f"Send rate:  {r.send_msg_per_s:,.0f} msg/s  |  {r.send_mbps:.2f} Mbps")
    log_sub(f"Recv rate:  {r.recv_msg_per_s:,.0f} msg/s  |  {r.recv_mbps:.2f} Mbps")
    log_sub(f"Send time:  {r.send_duration_s:.3f}s  |  Recv time: {r.recv_duration_s:.3f}s")
    log_sub(f"Loss:       {r.loss_pct:.2f}%")


def test_bandwidth(protocols: list):
    log_header("📶  BANDWIDTH / THROUGHPUT BENCHMARK")
    results = []

    for qos in BENCH["qos_levels"]:
        if "mqtt" in protocols:
            log_info(f"Running MQTT bandwidth QoS={qos} "
                     f"({BENCH['bandwidth_msg_count']} msgs × "
                     f"{BENCH['bandwidth_payload_kb']} KB) ...")
            try:
                r = bench_bandwidth_mqtt(qos)
                print_bandwidth(r)
                check(r.loss_pct < 1.0,
                      f"MQTT QoS {qos}: loss < 1%", f"{r.loss_pct:.2f}%")
                check(r.messages_recv > 0,
                      f"MQTT QoS {qos}: received > 0", f"{r.messages_recv}")
                results.append(r)
            except Exception as e:
                log_fail(f"MQTT bandwidth QoS {qos}: {e}")
                traceback.print_exc()

        if "http" in protocols:
            log_info(f"Running HTTP bandwidth QoS={qos} "
                     f"({BENCH['bandwidth_msg_count']} msgs × "
                     f"{BENCH['bandwidth_payload_kb']} KB) ...")
            try:
                r = bench_bandwidth_http(qos)
                print_bandwidth(r)
                check(r.loss_pct < 5.0,
                      f"HTTP QoS {qos}: loss < 5%", f"{r.loss_pct:.2f}%")
                results.append(r)
            except Exception as e:
                log_fail(f"HTTP bandwidth QoS {qos}: {e}")
                traceback.print_exc()

    # --- comparison table ---
    if len(results) > 1:
        log_header("Bandwidth Comparison Table")
        print(f"  {'Proto':<6} {'QoS':>3} {'Sent':>6} {'Recv':>6} "
              f"{'Loss%':>6} {'Snd msg/s':>10} {'Rcv msg/s':>10} "
              f"{'Snd Mbps':>9} {'Rcv Mbps':>9}")
        print(f"  {'-'*5:<6} {'-'*3:>3} {'-'*5:>6} {'-'*5:>6} "
              f"{'-'*5:>6} {'-'*9:>10} {'-'*9:>10} "
              f"{'-'*8:>9} {'-'*8:>9}")
        for r in results:
            print(f"  {r.protocol:<6} {r.qos:>3} {r.messages_sent:>6} "
                  f"{r.messages_recv:>6} {r.loss_pct:>5.1f}% "
                  f"{r.send_msg_per_s:>10,.0f} {r.recv_msg_per_s:>10,.0f} "
                  f"{r.send_mbps:>8.2f} {r.recv_mbps:>8.2f}")

    return results


# --------------------------------------------------------------------------- #
# ⏱  Latency Tests
# --------------------------------------------------------------------------- #
def bench_latency_mqtt(qos: int = 0) -> LatencyResult:
    """Round-trip latency: publish → on_message callback."""
    result = LatencyResult(protocol="MQTT", qos=qos)
    topic = f"bench/latency/mqtt/{qos}/{uuid.uuid4().hex[:6]}"
    rounds = BENCH["latency_rounds"]

    helper = MqttHelper(f"bench-lat-{uuid.uuid4().hex[:6]}")
    latencies: List[float] = []

    event = threading.Event()
    recv_ts = [0.0]

    def _on_msg(client, userdata, msg):
        recv_ts[0] = time.perf_counter()
        event.set()

    try:
        helper.connect()
        helper.client.on_message = _on_msg
        helper.subscribe(topic, qos)
        time.sleep(0.3)

        for i in range(rounds):
            event.clear()
            payload = struct.pack("!Id", i, time.perf_counter())
            t_send = time.perf_counter()
            info = helper.publish(topic, payload, qos=qos)
            if qos > 0:
                info.wait_for_publish(timeout=5)

            if event.wait(timeout=5.0):
                rtt = (recv_ts[0] - t_send) * 1000.0  # ms
                latencies.append(rtt)
            else:
                log_warn(f"  Latency round {i} timed out")

        result.samples = latencies
        if latencies:
            latencies.sort()
            result.min_ms    = latencies[0]
            result.max_ms    = latencies[-1]
            result.mean_ms   = statistics.mean(latencies)
            result.median_ms = statistics.median(latencies)
            result.p95_ms    = latencies[int(len(latencies) * 0.95)]
            result.p99_ms    = latencies[int(len(latencies) * 0.99)]
            if len(latencies) > 1:
                result.stddev_ms = statistics.stdev(latencies)

    finally:
        helper.disconnect()

    return result


def bench_latency_http(qos: int = 0) -> LatencyResult:
    """Round-trip latency via HTTP bridge: publish → poll receive."""
    result = LatencyResult(protocol="HTTP", qos=qos)
    topic = f"bench/latency/http/{qos}/{uuid.uuid4().hex[:6]}"
    rounds = BENCH["latency_rounds"]

    h = HttpHelper()
    latencies: List[float] = []

    try:
        h.connect()
        h.subscribe(topic, qos=qos, stream=False)
        time.sleep(0.3)

        for i in range(rounds):
            payload = json.dumps({"seq": i, "t": time.perf_counter()})
            t_send = time.perf_counter()
            h.publish(topic, payload, qos=qos)

            # poll until we get it
            deadline = time.time() + 5.0
            got = False
            while time.time() < deadline:
                msgs = h.poll(topic, limit=10)
                t_recv = time.perf_counter()
                if msgs:
                    rtt = (t_recv - t_send) * 1000.0
                    latencies.append(rtt)
                    got = True
                    break
                time.sleep(0.005)

            if not got:
                log_warn(f"  HTTP latency round {i} timed out")

        result.samples = latencies
        if latencies:
            latencies.sort()
            result.min_ms    = latencies[0]
            result.max_ms    = latencies[-1]
            result.mean_ms   = statistics.mean(latencies)
            result.median_ms = statistics.median(latencies)
            result.p95_ms    = latencies[int(len(latencies) * 0.95)]
            result.p99_ms    = latencies[int(len(latencies) * 0.99)]
            if len(latencies) > 1:
                result.stddev_ms = statistics.stdev(latencies)

    finally:
        h.unsubscribe(topic)
        h.disconnect()

    return result


def print_latency(r: LatencyResult):
    log_metric(f"[{r.protocol}] QoS {r.qos} Latency ({len(r.samples)} samples)")
    log_sub(f"Min:    {r.min_ms:.3f} ms")
    log_sub(f"Mean:   {r.mean_ms:.3f} ms")
    log_sub(f"Median: {r.median_ms:.3f} ms")
    log_sub(f"P95:    {r.p95_ms:.3f} ms")
    log_sub(f"P99:    {r.p99_ms:.3f} ms")
    log_sub(f"Max:    {r.max_ms:.3f} ms")
    log_sub(f"Stddev: {r.stddev_ms:.3f} ms")


def test_latency(protocols: list):
    log_header("⏱  LATENCY / DELAY BENCHMARK")
    results = []

    for qos in BENCH["qos_levels"]:
        if "mqtt" in protocols:
            log_info(f"Running MQTT latency QoS={qos} "
                     f"({BENCH['latency_rounds']} rounds) ...")
            try:
                r = bench_latency_mqtt(qos)
                print_latency(r)
                check(len(r.samples) == BENCH["latency_rounds"],
                      f"MQTT QoS {qos}: all {BENCH['latency_rounds']} "
                      f"rounds complete",
                      f"got {len(r.samples)}")
                check(r.p99_ms < 1000,
                      f"MQTT QoS {qos}: P99 < 1 s",
                      f"{r.p99_ms:.1f} ms")
                results.append(r)
            except Exception as e:
                log_fail(f"MQTT latency QoS {qos}: {e}")
                traceback.print_exc()

        if "http" in protocols:
            log_info(f"Running HTTP latency QoS={qos} "
                     f"({BENCH['latency_rounds']} rounds) ...")
            try:
                r = bench_latency_http(qos)
                print_latency(r)
                check(len(r.samples) >= BENCH["latency_rounds"] * 0.9,
                      f"HTTP QoS {qos}: >= 90% rounds complete",
                      f"got {len(r.samples)}/{BENCH['latency_rounds']}")
                results.append(r)
            except Exception as e:
                log_fail(f"HTTP latency QoS {qos}: {e}")
                traceback.print_exc()

    if len(results) > 1:
        log_header("Latency Comparison Table")
        print(f"  {'Proto':<6} {'QoS':>3} {'N':>5} {'Min':>8} "
              f"{'Mean':>8} {'Med':>8} {'P95':>8} {'P99':>8} "
              f"{'Max':>8} {'Std':>8}")
        print(f"  {'-'*5:<6} {'-'*3:>3} {'-'*4:>5} {'-'*7:>8} "
              f"{'-'*7:>8} {'-'*7:>8} {'-'*7:>8} {'-'*7:>8} "
              f"{'-'*7:>8} {'-'*7:>8}")
        for r in results:
            print(f"  {r.protocol:<6} {r.qos:>3} {len(r.samples):>5} "
                  f"{r.min_ms:>7.2f} {r.mean_ms:>7.2f} {r.median_ms:>7.2f} "
                  f"{r.p95_ms:>7.2f} {r.p99_ms:>7.2f} "
                  f"{r.max_ms:>7.2f} {r.stddev_ms:>7.2f}")

    return results


# --------------------------------------------------------------------------- #
# 📐  Ordering Tests
# --------------------------------------------------------------------------- #
def bench_ordering_mqtt(qos: int = 0) -> OrderingResult:
    """Check message ordering over native MQTT."""
    result = OrderingResult(protocol="MQTT", qos=qos)
    topic = f"bench/order/mqtt/{qos}/{uuid.uuid4().hex[:6]}"
    count = BENCH["ordering_msg_count"]

    pub = MqttHelper(f"bench-ord-pub-{uuid.uuid4().hex[:6]}")
    sub = MqttHelper(f"bench-ord-sub-{uuid.uuid4().hex[:6]}")

    try:
        sub.connect()
        sub.subscribe(topic, qos)
        time.sleep(0.3)

        pub.connect()
        time.sleep(0.2)

        result.total_sent = count
        for i in range(count):
            payload = struct.pack("!I", i)
            info = pub.publish(topic, payload, qos=qos)
            if qos > 0:
                info.wait_for_publish(timeout=10)

        sub.wait_for(count, timeout=BENCH["timeout"])

        with sub._lock:
            received_seqs = []
            for m in sub.received:
                if len(m["payload"]) >= 4:
                    seq = struct.unpack("!I", m["payload"][:4])[0]
                    received_seqs.append(seq)

        result.total_recv = len(received_seqs)

        # check ordering
        seen = set()
        prev = -1
        for idx, seq in enumerate(received_seqs):
            if seq in seen:
                result.duplicates += 1
            seen.add(seq)
            if seq <= prev and prev != -1:
                result.out_of_order += 1
                if result.first_oo_index == -1:
                    result.first_oo_index = idx
            prev = seq

        expected = set(range(count))
        result.missing = len(expected - seen)
        result.in_order = (result.out_of_order == 0)

    finally:
        pub.disconnect()
        sub.disconnect()

    return result


def bench_ordering_http(qos: int = 0) -> OrderingResult:
    """Check message ordering over HTTP bridge."""
    result = OrderingResult(protocol="HTTP", qos=qos)
    topic = f"bench/order/http/{qos}/{uuid.uuid4().hex[:6]}"
    count = BENCH["ordering_msg_count"]

    pub_h = HttpHelper()
    sub_h = HttpHelper()

    try:
        pub_h.connect()
        sub_h.connect()
        sub_h.subscribe(topic, qos=qos, stream=False)
        time.sleep(0.3)

        result.total_sent = count
        for i in range(count):
            pub_h.publish(topic, json.dumps({"seq": i}), qos=qos)

        collected = sub_h.poll_until(topic, count, timeout=BENCH["timeout"])
        received_seqs = []
        for m in collected:
            try:
                p = m.get("payload", "{}")
                obj = json.loads(p) if isinstance(p, str) else json.loads(
                    p.decode() if isinstance(p, bytes) else "{}")
                received_seqs.append(obj.get("seq", -1))
            except Exception:
                pass

        result.total_recv = len(received_seqs)

        seen = set()
        prev = -1
        for idx, seq in enumerate(received_seqs):
            if seq in seen:
                result.duplicates += 1
            seen.add(seq)
            if seq <= prev and prev != -1:
                result.out_of_order += 1
                if result.first_oo_index == -1:
                    result.first_oo_index = idx
            prev = seq

        expected = set(range(count))
        result.missing = len(expected - seen)
        result.in_order = (result.out_of_order == 0)

    finally:
        sub_h.unsubscribe(topic)
        pub_h.disconnect()
        sub_h.disconnect()

    return result


def print_ordering(r: OrderingResult):
    log_metric(f"[{r.protocol}] QoS {r.qos} Ordering")
    log_sub(f"Sent:         {r.total_sent}")
    log_sub(f"Received:     {r.total_recv}")
    log_sub(f"In order:     {'YES ✓' if r.in_order else 'NO ✗'}")
    log_sub(f"Out of order: {r.out_of_order}")
    log_sub(f"Duplicates:   {r.duplicates}")
    log_sub(f"Missing:      {r.missing}")
    if not r.in_order:
        log_sub(f"First OoO at index: {r.first_oo_index}")


def test_ordering(protocols: list):
    log_header("📐  MESSAGE ORDERING TEST")
    results = []

    for qos in BENCH["qos_levels"]:
        if "mqtt" in protocols:
            log_info(f"Running MQTT ordering QoS={qos} "
                     f"({BENCH['ordering_msg_count']} msgs) ...")
            try:
                r = bench_ordering_mqtt(qos)
                print_ordering(r)
                check(r.in_order,
                      f"MQTT QoS {qos}: messages in order",
                      f"{r.out_of_order} out of order")
                check(r.duplicates == 0,
                      f"MQTT QoS {qos}: no duplicates",
                      f"{r.duplicates} duplicates")
                check(r.missing == 0,
                      f"MQTT QoS {qos}: no missing",
                      f"{r.missing} missing")
                results.append(r)
            except Exception as e:
                log_fail(f"MQTT ordering QoS {qos}: {e}")
                traceback.print_exc()

        if "http" in protocols:
            log_info(f"Running HTTP ordering QoS={qos} "
                     f"({BENCH['ordering_msg_count']} msgs) ...")
            try:
                r = bench_ordering_http(qos)
                print_ordering(r)
                check(r.in_order,
                      f"HTTP QoS {qos}: messages in order",
                      f"{r.out_of_order} out of order")
                check(r.missing <= r.total_sent * 0.01,
                      f"HTTP QoS {qos}: missing < 1%",
                      f"{r.missing} missing")
                results.append(r)
            except Exception as e:
                log_fail(f"HTTP ordering QoS {qos}: {e}")
                traceback.print_exc()

    if results:
        log_header("Ordering Comparison")
        print(f"  {'Proto':<6} {'QoS':>3} {'Sent':>6} {'Recv':>6} "
              f"{'OoO':>5} {'Dup':>5} {'Miss':>5} {'OK?':>5}")
        print(f"  {'-'*5:<6} {'-'*3:>3} {'-'*5:>6} {'-'*5:>6} "
              f"{'-'*4:>5} {'-'*4:>5} {'-'*4:>5} {'-'*4:>5}")
        for r in results:
            ok = "✓" if r.in_order and r.duplicates == 0 and r.missing == 0 else "✗"
            print(f"  {r.protocol:<6} {r.qos:>3} {r.total_sent:>6} "
                  f"{r.total_recv:>6} {r.out_of_order:>5} {r.duplicates:>5} "
                  f"{r.missing:>5} {ok:>5}")

    return results


# --------------------------------------------------------------------------- #
# ✅  Correctness Tests
# --------------------------------------------------------------------------- #
def bench_correctness_mqtt(qos: int = 0) -> CorrectnessResult:
    """Verify payload integrity over native MQTT."""
    result = CorrectnessResult(protocol="MQTT", qos=qos)
    topic = f"bench/correct/mqtt/{qos}/{uuid.uuid4().hex[:6]}"
    count = BENCH["correctness_msg_count"]
    payload_size = BENCH["bandwidth_payload_kb"] * 1024

    pub = MqttHelper(f"bench-cor-pub-{uuid.uuid4().hex[:6]}")
    sub = MqttHelper(f"bench-cor-sub-{uuid.uuid4().hex[:6]}")

    sent_payloads: Dict[int, bytes] = {}

    try:
        sub.connect()
        sub.subscribe(topic, qos)
        time.sleep(0.3)
        pub.connect()
        time.sleep(0.2)

        result.total_sent = count
        for i in range(count):
            payload = make_payload(payload_size, i)
            sent_payloads[i] = payload
            info = pub.publish(topic, payload, qos=qos)
            if qos > 0:
                info.wait_for_publish(timeout=10)

        sub.wait_for(count, timeout=BENCH["timeout"])

        with sub._lock:
            result.total_recv = len(sub.received)
            received_seqs = set()

            for m in sub.received:
                data = m["payload"]
                seq, ok = verify_payload(data)

                if seq in received_seqs:
                    result.extra += 1
                    continue
                received_seqs.add(seq)

                if not ok:
                    result.corrupted += 1
                    result.corrupt_details.append(
                        f"seq={seq}: checksum mismatch "
                        f"(len={len(data)})")
                elif seq in sent_payloads:
                    if data == sent_payloads[seq]:
                        result.matched += 1
                    else:
                        result.corrupted += 1
                        result.corrupt_details.append(
                            f"seq={seq}: byte mismatch "
                            f"(sent={len(sent_payloads[seq])}, "
                            f"recv={len(data)})")
                else:
                    result.extra += 1

        expected = set(range(count))
        result.missing = len(expected - received_seqs)

    finally:
        pub.disconnect()
        sub.disconnect()

    return result


def bench_correctness_http(qos: int = 0) -> CorrectnessResult:
    """Verify payload integrity over HTTP bridge."""
    result = CorrectnessResult(protocol="HTTP", qos=qos)
    topic = f"bench/correct/http/{qos}/{uuid.uuid4().hex[:6]}"
    count = BENCH["correctness_msg_count"]

    pub_h = HttpHelper()
    sub_h = HttpHelper()
    sent_payloads: Dict[int, str] = {}

    try:
        pub_h.connect()
        sub_h.connect()
        sub_h.subscribe(topic, qos=qos, stream=False)
        time.sleep(0.3)

        result.total_sent = count
        for i in range(count):
            payload = make_json_payload(i, extra_kb=BENCH["bandwidth_payload_kb"])
            sent_payloads[i] = payload
            pub_h.publish(topic, payload, qos=qos)

        collected = sub_h.poll_until(topic, count, timeout=BENCH["timeout"])
        result.total_recv = len(collected)

        received_seqs = set()
        for m in collected:
            raw = m.get("payload", "")
            if isinstance(raw, bytes):
                raw = raw.decode(errors="replace")

            seq, ok = verify_json_payload(raw)

            if seq in received_seqs:
                result.extra += 1
                continue
            received_seqs.add(seq)

            if not ok:
                result.corrupted += 1
                result.corrupt_details.append(
                    f"seq={seq}: checksum mismatch")
            elif seq in sent_payloads:
                if raw == sent_payloads[seq]:
                    result.matched += 1
                else:
                    result.corrupted += 1
                    result.corrupt_details.append(
                        f"seq={seq}: content mismatch "
                        f"(sent_len={len(sent_payloads[seq])}, "
                        f"recv_len={len(raw)})")
            else:
                result.extra += 1

        expected = set(range(count))
        result.missing = len(expected - received_seqs)

    finally:
        sub_h.unsubscribe(topic)
        pub_h.disconnect()
        sub_h.disconnect()

    return result


def print_correctness(r: CorrectnessResult):
    log_metric(f"[{r.protocol}] QoS {r.qos} Correctness")
    log_sub(f"Sent:      {r.total_sent}")
    log_sub(f"Received:  {r.total_recv}")
    log_sub(f"Matched:   {r.matched}")
    log_sub(f"Corrupted: {r.corrupted}")
    log_sub(f"Missing:   {r.missing}")
    log_sub(f"Extra/Dup: {r.extra}")
    if r.corrupt_details:
        for d in r.corrupt_details[:10]:
            log_sub(f"  ⚠ {d}")
        if len(r.corrupt_details) > 10:
            log_sub(f"  ... and {len(r.corrupt_details)-10} more")


def test_correctness(protocols: list):
    log_header("✅  DATA CORRECTNESS TEST")
    results = []

    for qos in BENCH["qos_levels"]:
        if "mqtt" in protocols:
            log_info(f"Running MQTT correctness QoS={qos} "
                     f"({BENCH['correctness_msg_count']} msgs) ...")
            try:
                r = bench_correctness_mqtt(qos)
                print_correctness(r)
                check(r.corrupted == 0,
                      f"MQTT QoS {qos}: zero corruption",
                      f"{r.corrupted} corrupted")
                check(r.missing == 0,
                      f"MQTT QoS {qos}: zero missing",
                      f"{r.missing} missing")
                check(r.matched == r.total_sent,
                      f"MQTT QoS {qos}: all matched",
                      f"{r.matched}/{r.total_sent}")
                results.append(r)
            except Exception as e:
                log_fail(f"MQTT correctness QoS {qos}: {e}")
                traceback.print_exc()

        if "http" in protocols:
            log_info(f"Running HTTP correctness QoS={qos} "
                     f"({BENCH['correctness_msg_count']} msgs) ...")
            try:
                r = bench_correctness_http(qos)
                print_correctness(r)
                check(r.corrupted == 0,
                      f"HTTP QoS {qos}: zero corruption",
                      f"{r.corrupted} corrupted")
                check(r.missing <= r.total_sent * 0.01,
                      f"HTTP QoS {qos}: missing < 1%",
                      f"{r.missing} missing")
                results.append(r)
            except Exception as e:
                log_fail(f"HTTP correctness QoS {qos}: {e}")
                traceback.print_exc()

    if results:
        log_header("Correctness Comparison")
        print(f"  {'Proto':<6} {'QoS':>3} {'Sent':>6} {'Recv':>6} "
              f"{'Match':>6} {'Corrupt':>7} {'Miss':>5} {'Extra':>5}")
        print(f"  {'-'*5:<6} {'-'*3:>3} {'-'*5:>6} {'-'*5:>6} "
              f"{'-'*5:>6} {'-'*6:>7} {'-'*4:>5} {'-'*4:>5}")
        for r in results:
            print(f"  {r.protocol:<6} {r.qos:>3} {r.total_sent:>6} "
                  f"{r.total_recv:>6} {r.matched:>6} {r.corrupted:>7} "
                  f"{r.missing:>5} {r.extra:>5}")

    return results


# --------------------------------------------------------------------------- #
# 🔀  Cross-Protocol Test (MQTT pub → HTTP sub, HTTP pub → MQTT sub)
# --------------------------------------------------------------------------- #
def test_cross_protocol():
    log_header("🔀  CROSS-PROTOCOL CORRECTNESS TEST")

    # --- MQTT publish → HTTP subscribe ---
    log_info("MQTT publish → HTTP poll receive ...")
    topic_m2h = f"bench/cross/m2h/{uuid.uuid4().hex[:6]}"
    count = min(50, BENCH["correctness_msg_count"])

    mqtt_pub = MqttHelper(f"cross-mpub-{uuid.uuid4().hex[:6]}")
    http_sub = HttpHelper()

    try:
        http_sub.connect()
        http_sub.subscribe(topic_m2h, qos=1, stream=False)
        time.sleep(0.5)

        mqtt_pub.connect()
        time.sleep(0.2)

        for i in range(count):
            payload = json.dumps({"cross": "m2h", "seq": i})
            info = mqtt_pub.publish(topic_m2h, payload.encode(), qos=1)
            info.wait_for_publish(timeout=5)

        collected = http_sub.poll_until(topic_m2h, count, timeout=BENCH["timeout"])
        seqs = []
        for m in collected:
            try:
                obj = json.loads(m.get("payload", "{}"))
                seqs.append(obj.get("seq", -1))
            except Exception:
                pass

        check(len(seqs) == count,
              f"M→H: received all {count} msgs",
              f"got {len(seqs)}")
        check(sorted(seqs) == list(range(count)),
              "M→H: all sequences present",
              f"got {len(seqs)} unique out of {count}")

    except Exception as e:
        log_fail(f"MQTT→HTTP: {e}")
        traceback.print_exc()
    finally:
        mqtt_pub.disconnect()
        http_sub.unsubscribe(topic_m2h)
        http_sub.disconnect()

    # --- HTTP publish → MQTT subscribe ---
    log_info("HTTP publish → MQTT subscribe ...")
    topic_h2m = f"bench/cross/h2m/{uuid.uuid4().hex[:6]}"

    http_pub = HttpHelper()
    mqtt_sub = MqttHelper(f"cross-msub-{uuid.uuid4().hex[:6]}")

    try:
        mqtt_sub.connect()
        mqtt_sub.subscribe(topic_h2m, 1)
        time.sleep(0.5)

        http_pub.connect()
        time.sleep(0.2)

        for i in range(count):
            payload = json.dumps({"cross": "h2m", "seq": i})
            http_pub.publish(topic_h2m, payload, qos=1)

        mqtt_sub.wait_for(count, timeout=BENCH["timeout"])

        with mqtt_sub._lock:
            seqs = []
            for m in mqtt_sub.received:
                try:
                    obj = json.loads(m["payload"].decode())
                    seqs.append(obj.get("seq", -1))
                except Exception:
                    pass

        check(len(seqs) == count,
              f"H→M: received all {count} msgs",
              f"got {len(seqs)}")
        check(sorted(seqs) == list(range(count)),
              "H→M: all sequences present",
              f"got {len(seqs)} unique out of {count}")

    except Exception as e:
        log_fail(f"HTTP→MQTT: {e}")
        traceback.print_exc()
    finally:
        mqtt_sub.disconnect()
        http_pub.disconnect()


# --------------------------------------------------------------------------- #
# 🏋  Stress / Burst Test
# --------------------------------------------------------------------------- #
def test_burst():
    log_header("🏋  BURST / STRESS TEST")

    topic = f"bench/burst/{uuid.uuid4().hex[:6]}"
    burst_size = BENCH["bandwidth_msg_count"]

    pub = MqttHelper(f"burst-pub-{uuid.uuid4().hex[:6]}")
    sub = MqttHelper(f"burst-sub-{uuid.uuid4().hex[:6]}")

    try:
        sub.connect()
        sub.subscribe(topic, 0)
        time.sleep(0.3)
        pub.connect()
        time.sleep(0.2)

        # Fire all at once, no waiting
        log_info(f"Sending {burst_size} messages in burst (QoS 0, no wait) ...")
        t0 = time.perf_counter()
        for i in range(burst_size):
            pub.publish(topic, struct.pack("!I", i), qos=0)
        send_elapsed = time.perf_counter() - t0

        log_metric(f"Burst send: {burst_size} msgs in {send_elapsed:.3f}s "
                   f"({burst_size/send_elapsed:,.0f} msg/s)")

        # Wait for delivery
        sub.wait_for(burst_size, timeout=BENCH["timeout"])
        recv_elapsed = time.perf_counter() - t0

        with sub._lock:
            recv_count = len(sub.received)

        log_metric(f"Burst recv: {recv_count}/{burst_size} msgs in "
                   f"{recv_elapsed:.3f}s")

        loss = (burst_size - recv_count) / burst_size * 100
        check(loss < 5.0,
              f"Burst loss < 5%", f"{loss:.2f}% lost")
        check(recv_count > 0,
              "Burst received > 0", f"got {recv_count}")

    finally:
        pub.disconnect()
        sub.disconnect()


# --------------------------------------------------------------------------- #
# 🔄  Multi-Subscriber Fan-Out Test
# --------------------------------------------------------------------------- #
def test_fanout():
    log_header("🔄  FAN-OUT TEST (1 publisher, N subscribers)")

    topic = f"bench/fanout/{uuid.uuid4().hex[:6]}"
    n_subs = 5
    msg_count = 50

    pub = MqttHelper(f"fanout-pub-{uuid.uuid4().hex[:6]}")
    subs = [MqttHelper(f"fanout-sub{i}-{uuid.uuid4().hex[:6]}")
            for i in range(n_subs)]

    try:
        for s in subs:
            s.connect()
            s.subscribe(topic, 1)
        time.sleep(0.5)

        pub.connect()
        time.sleep(0.2)

        for i in range(msg_count):
            payload = json.dumps({"fanout": True, "seq": i}).encode()
            info = pub.publish(topic, payload, qos=1)
            info.wait_for_publish(timeout=5)

        time.sleep(2)
        for s in subs:
            s.wait_for(msg_count, timeout=BENCH["timeout"])

        all_ok = True
        for idx, s in enumerate(subs):
            with s._lock:
                cnt = len(s.received)
            ok = cnt == msg_count
            if not ok:
                all_ok = False
            log_sub(f"Subscriber {idx}: {cnt}/{msg_count} msgs")

        check(all_ok, f"All {n_subs} subs received {msg_count} msgs",
              "some missed messages")

    finally:
        pub.disconnect()
        for s in subs:
            s.disconnect()


# --------------------------------------------------------------------------- #
# Connectivity Checks
# --------------------------------------------------------------------------- #
def check_http_health() -> bool:
    log_header("HTTP Bridge Health")
    try:
        r = requests.get(url("/health"), timeout=5)
        data = r.json()
        ok = r.status_code == 200 and data.get("status") == "ok"
        check(ok, "HTTP bridge reachable", str(data))
        return ok
    except Exception as e:
        log_fail(f"HTTP bridge unreachable: {e}")
        return False


def check_mqtt_health() -> bool:
    log_header("MQTT Broker Health")
    try:
        h = MqttHelper(f"healthcheck-{uuid.uuid4().hex[:6]}")
        h.connect(timeout=5)
        check(True, "MQTT broker reachable")
        h.disconnect()
        return True
    except Exception as e:
        log_fail(f"MQTT broker unreachable: {e}")
        return False


# --------------------------------------------------------------------------- #
# Runners
# --------------------------------------------------------------------------- #
def resolve_protocols(arg: str) -> list:
    if arg == "both":
        return ["mqtt", "http"]
    return [arg]


def run_all(protocols: list):
    avail = []
    if "mqtt" in protocols:
        if check_mqtt_health():
            avail.append("mqtt")
    if "http" in protocols:
        if check_http_health():
            avail.append("http")

    if not avail:
        log_fail("No reachable protocols. Aborting.")
        sys.exit(1)

    test_bandwidth(avail)
    test_latency(avail)
    test_ordering(avail)
    test_correctness(avail)

    if "mqtt" in avail:
        test_burst()
        test_fanout()

    if "mqtt" in avail and "http" in avail:
        test_cross_protocol()


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    p = argparse.ArgumentParser(
        description="Benchmark & correctness tests for MQTT broker")
    p.add_argument("--test",
                   choices=["all", "bandwidth", "latency",
                            "ordering", "correctness",
                            "cross", "burst", "fanout", "health"],
                   default="all")
    p.add_argument("--protocol", choices=["mqtt", "http", "both"],
                   default="both")
    p.add_argument("--url",        default=CONFIG["base_url"])
    p.add_argument("--apikey",     default=CONFIG["api_key"])
    p.add_argument("--mqtt-host",  default=CONFIG["mqtt_host"])
    p.add_argument("--mqtt-port",  type=int, default=CONFIG["mqtt_port"])
    p.add_argument("--mqtt-user",  default=CONFIG["mqtt_username"])
    p.add_argument("--mqtt-pass",  default=CONFIG["mqtt_password"])
    p.add_argument("--mqtt-transport", choices=["tcp", "websockets"],
                   default=CONFIG["mqtt_transport"])
    p.add_argument("--msg-count",  type=int, default=BENCH["bandwidth_msg_count"],
                   help="Messages per bandwidth / ordering test")
    p.add_argument("--payload-kb", type=int, default=BENCH["bandwidth_payload_kb"],
                   help="Payload size in KB")
    p.add_argument("--latency-rounds", type=int,
                   default=BENCH["latency_rounds"])
    p.add_argument("--timeout",    type=float, default=BENCH["timeout"])
    p.add_argument("--qos",        type=int, nargs="+",
                   default=BENCH["qos_levels"],
                   help="QoS levels to test (e.g. 0 1 2)")
    args = p.parse_args()

    CONFIG["base_url"]       = args.url
    CONFIG["api_key"]        = args.apikey
    CONFIG["mqtt_host"]      = args.mqtt_host
    CONFIG["mqtt_port"]      = args.mqtt_port
    CONFIG["mqtt_username"]  = args.mqtt_user
    CONFIG["mqtt_password"]  = args.mqtt_pass
    CONFIG["mqtt_transport"] = args.mqtt_transport

    BENCH["bandwidth_msg_count"]   = args.msg_count
    BENCH["ordering_msg_count"]    = args.msg_count
    BENCH["correctness_msg_count"] = min(args.msg_count, 500)
    BENCH["bandwidth_payload_kb"]  = args.payload_kb
    BENCH["latency_rounds"]        = args.latency_rounds
    BENCH["timeout"]               = args.timeout
    BENCH["qos_levels"]            = args.qos

    protos = resolve_protocols(args.protocol)

    print(f"\n{'='*70}")
    print(f"  🎯  MQTT Broker Benchmark Suite")
    print(f"{'='*70}")
    print(f"  HTTP bridge:  {CONFIG['base_url']}")
    print(f"  MQTT broker:  {CONFIG['mqtt_host']}:{CONFIG['mqtt_port']}")
    print(f"  Protocol(s):  {', '.join(protos)}")
    print(f"  QoS levels:   {BENCH['qos_levels']}")
    print(f"  Msg count:    {BENCH['bandwidth_msg_count']}")
    print(f"  Payload:      {BENCH['bandwidth_payload_kb']} KB")
    print(f"  Latency rds:  {BENCH['latency_rounds']}")
    print(f"  Timeout:      {BENCH['timeout']}s")
    print(f"{'='*70}\n")

    runners = {
        "health":      lambda: (check_mqtt_health() if "mqtt" in protos else None,
                                check_http_health() if "http" in protos else None),
        "bandwidth":   lambda: test_bandwidth(protos),
        "latency":     lambda: test_latency(protos),
        "ordering":    lambda: test_ordering(protos),
        "correctness": lambda: test_correctness(protos),
        "cross":       lambda: test_cross_protocol(),
        "burst":       lambda: test_burst(),
        "fanout":      lambda: test_fanout(),
        "all":         lambda: run_all(protos),
    }
    runners[args.test]()

    total = passed + failed
    print(f"\n{'='*70}")
    print(f"  {C.BOLD}RESULTS: {passed}/{total} checks passed", end="")
    if failed:
        print(f", {C.FAIL}{failed} FAILED{C.END}")
    else:
        print(f"  {C.OK}ALL PASSED ✨{C.END}")
    print(f"{'='*70}\n")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()