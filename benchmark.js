#!/usr/bin/env node
/**
 * Benchmark & Correctness Test Suite for MQTT Broker (Node.js)
 */

import mqtt from 'mqtt';
import axios from 'axios';
import crypto from 'crypto';
import WebSocket from 'ws';
import { URL } from 'url';

const C = {
  OK: '\x1b[92m',
  FAIL: '\x1b[91m',
  WARN: '\x1b[93m',
  INFO: '\x1b[94m',
  CYAN: '\x1b[96m',
  BOLD: '\x1b[1m',
  DIM: '\x1b[2m',
  END: '\x1b[0m'
};

const logPass = (m) => console.log(`${C.OK}✅ PASS${C.END}  ${m}`);
const logFail = (m) => console.log(`${C.FAIL}❌ FAIL${C.END}  ${m}`);
const logInfo = (m) => console.log(`${C.INFO}ℹ️   ${m}${C.END}`);
const logMetric = (m) => console.log(`${C.CYAN}📊  ${m}${C.END}`);
const logHeader = (m) => console.log(`\n${C.BOLD}${'='.repeat(70)}\n  ${m}\n${'='.repeat(70)}${C.END}`);

const CONFIG = {
  base_url: 'http://localhost:7860',
  api_key: 'BROKER_APIKEY',
  mqtt_host: 'localhost',
  mqtt_port: 1883,
  mqtt_ws_port: 7860,
  mqtt_ws_path: '/mqtt',
  mqtt_username: '',
  mqtt_password: '',
  mqtt_transport: 'websockets',
  tls: false
};

const BENCH = {
  bandwidth_msg_count: 1000,
  bandwidth_payload_kb: 1,
  latency_rounds: 100,
  ordering_msg_count: 500,
  qos_levels: [0, 1, 2],
  timeout: 5.0
};

const url = (path) => `${CONFIG.base_url}${path}`;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function generatePayload(sizeBytes, seq = 0) {
  const headerLen = 4 + 32;
  const padLen = Math.max(0, sizeBytes - headerLen);
  const padding = crypto.randomBytes(padLen);
  const seqBytes = Buffer.alloc(4);
  seqBytes.writeUInt32BE(seq, 0);
  const raw = Buffer.concat([seqBytes, padding]);
  const sha = crypto.createHash('sha256').update(raw).digest();
  return Buffer.concat([seqBytes, sha, padding]);
}

class MqttHelper {
  constructor(clientId = null) {
    const cid = clientId || `bench-${crypto.randomBytes(4).toString('hex')}`;
    this.useWs = CONFIG.mqtt_transport === 'websockets';
    this.connected = false;
    this.subscribed = false;
    this.received = [];
    this.recvTimes = [];
    this.client = null;
    this.connectResolve = null;
    this.subscribeResolve = null;
    this.messageResolve = null;

    const options = {
      clientId: cid,
      protocolVersion: 4,
      connectTimeout: 15000,
      reconnectPeriod: 0,
      clean: true,
      keepalive: 60
    };

    const brokerUrl = this.useWs 
      ? `ws://${CONFIG.mqtt_host}:${CONFIG.mqtt_ws_port}`
      : `mqtt://${CONFIG.mqtt_host}:${CONFIG.mqtt_port}`;

    // Add WebSocket path for mqtt.js
    if (this.useWs) {
      options.path = CONFIG.mqtt_ws_path;
    }

    this.client = mqtt.connect(brokerUrl, options);

    this.client.on('connect', () => {
      this.connected = true;
      if (this.connectResolve) this.connectResolve();
    });

    this.client.on('close', () => {
      this.connected = false;
    });

    this.client.on('message', (topic, payload, packet) => {
      const ts = performance.now();
      this.received.push({ topic, payload, qos: packet.qos, ts });
      this.recvTimes.push(ts);
      if (this.messageResolve) {
        this.messageResolve();
        this.messageResolve = null;
      }
    });

    this.client.on('subscribe', (topic, qos) => {
      this.subscribed = true;
      if (this.subscribeResolve) this.subscribeResolve();
    });

    this.client.on('error', (err) => {
      console.error('MQTT error:', err.message, err.code);
    });

    this.client.on('close', () => {
      this.connected = false;
    });
  }

  connect(timeout = 10000) {
    return new Promise((resolve, reject) => {
      if (this.connected) {
        resolve();
        return;
      }
      const timer = setTimeout(() => {
        reject(new Error('MQTT connect timeout'));
      }, timeout);
      this.connectResolve = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }

  subscribe(topic, qos = 0, timeout = 10000) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('MQTT subscribe timeout')), timeout);
      this.subscribeResolve = () => {
        clearTimeout(timer);
        resolve();
      };
      this.client.subscribe(topic, { qos }, (err, granted) => {
        if (err) {
          clearTimeout(timer);
          reject(err);
        } else {
          clearTimeout(timer);
          this.subscribed = true;
          if (this.subscribeResolve) this.subscribeResolve();
        }
      });
    });
  }

  publish(topic, payload, qos = 0) {
    return this.client.publish(topic, payload, { qos });
  }

  waitFor(count, timeout = 30000) {
    return new Promise((resolve) => {
      const deadline = Date.now() + timeout;
      const check = () => {
        if (this.received.length >= count) {
          resolve(true);
          return;
        }
        if (Date.now() > deadline) {
          resolve(false);
          return;
        }
        setTimeout(check, 10);
      };
      check();
    });
  }

  waitForMessage(timeout = 2000) {
    return new Promise((resolve) => {
      if (this.received.length > 0) {
        resolve(true);
        return;
      }
      const timer = setTimeout(() => resolve(false), timeout);
      this.messageResolve = () => {
        clearTimeout(timer);
        resolve(true);
      };
    });
  }

  disconnect() {
    this.client.end();
  }
}

class HttpHelper {
  constructor() {
    this.clientId = null;
    this.received = [];
    this.recvTimes = [];
    this.buffer = [];
    this.stopSse = false;
    this.sseRunning = false;
  }

  async connect(startSse = true) {
    const response = await axios.post(url('/clients/connect'), {
      apiKey: CONFIG.api_key
    }, { timeout: 10000 });
    
    const data = response.data;
    if (!data.success) {
      throw new Error(`HTTP connect failed: ${JSON.stringify(data)}`);
    }
    
    this.clientId = data.client_id;
    
    if (startSse) {
      this._startSseListener();
    }
  }

  async disconnect() {
    this.stopSse = true;
    
    if (this.clientId) {
      try {
        await axios.post(url(`/clients/${this.clientId}/disconnect`), {}, { timeout: 5000 });
      } catch (e) {
        // ignore
      }
      this.clientId = null;
    }
  }

  _startSseListener() {
    this.stopSse = false;
    this.sseRunning = true;
    this._sseLoop();
  }

  async _sseLoop() {
    while (!this.stopSse) {
      try {
        const response = await fetch(url(`/clients/${this.clientId}/messages`), {
          method: 'POST',
          headers: { 'Accept': 'text/event-stream' }
        });

        if (!response.ok) {
          await sleep(500);
          continue;
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (!this.stopSse) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';

          for (const line of lines) {
            if (line.startsWith('data: ')) {
              const raw = line.slice(6).trim();
              if (raw === '[DONE]' || !raw) continue;
              
              try {
                const data = JSON.parse(raw);
                if (Array.isArray(data)) {
                  this.buffer.push(...data);
                } else {
                  this.buffer.push(data);
                }
                this.recvTimes.push(performance.now());
              } catch (e) {
                // ignore parse errors
              }
            }
          }
        }
      } catch (e) {
        if (this.stopSse) break;
        await sleep(500);
      }
    }
    this.sseRunning = false;
  }

  async subscribe(topic, qos = 0) {
    const response = await axios.post(
      url(`/clients/${this.clientId}/subscribe`),
      { topic, qos },
      { timeout: 10000 }
    );
    if (response.status !== 200) {
      throw new Error(`HTTP subscribe failed: ${response.statusText}`);
    }
  }

  async publish(topic, payload, qos = 0) {
    return axios.post(
      url(`/clients/${this.clientId}/publish`),
      { topic, qos, payload: typeof payload === 'string' ? payload : payload.toString('base64') },
      { timeout: 10000 }
    );
  }

  async publishBatch(messages) {
    return axios.post(
      url(`/clients/${this.clientId}/publish/batch`),
      { messages },
      { timeout: 10000 }
    );
  }

  _drain(topic = null, limit = 1000) {
    const matched = [];
    const remaining = [];

    for (const m of this.buffer) {
      if (matched.length < limit && (topic === null || m.topic === topic)) {
        matched.push(m);
      } else {
        remaining.push(m);
      }
    }

    this.buffer = remaining;
    return matched;
  }

  poll(topic = null, limit = 1000) {
    const drained = this._drain(topic, limit);
    for (const m of drained) {
      this.received.push(m);
    }
    return drained;
  }

  async pollUntil(topic, count, timeout = 30000) {
    const collected = [];
    const deadline = Date.now() + timeout;

    while (collected.length < count) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) break;

      const waitTime = Math.min(remaining, 100);
      await sleep(waitTime);

      const batch = this._drain(topic);
      for (const m of batch) {
        collected.push(m);
        this.received.push(m);
      }
    }

    return collected;
  }
}

// WebSocket helper for HTTP API clients

async function benchBandwidthMqtt(qos = 0) {
  const result = { protocol: 'MQTT', qos, messagesSent: 0, messagesRecv: 0, sendDurationS: 0, recvDurationS: 0, sendMsgPerS: 0, recvMsgPerS: 0, lossPct: 0 };
  const topic = `bench/bw/mqtt/${qos}/${crypto.randomBytes(3).toString('hex')}`;
  const payloadSize = BENCH.bandwidth_payload_kb * 1024;
  const count = BENCH.bandwidth_msg_count;

  const pub = new MqttHelper();
  const sub = new MqttHelper();

  try {
    await sub.connect();
    await sub.subscribe(topic, qos);
    await sleep(200);
    await pub.connect();

    const payloads = [];
    for (let i = 0; i < count; i++) {
      payloads.push(generatePayload(payloadSize, i));
    }

    result.messagesSent = count;
    const t0 = performance.now();

    for (const p of payloads) {
      pub.publish(topic, p, qos);
    }

    result.sendDurationS = (performance.now() - t0) / 1000;
    await sub.waitFor(count, BENCH.timeout * 1000);
    result.recvDurationS = (sub.recvTimes[sub.recvTimes.length - 1] - t0) / 1000;
    result.messagesRecv = sub.received.length;

    if (result.sendDurationS > 0) {
      result.sendMsgPerS = count / result.sendDurationS;
    }
    if (result.recvDurationS > 0) {
      result.recvMsgPerS = result.messagesRecv / result.recvDurationS;
    }
    result.lossPct = count > 0 ? ((count - result.messagesRecv) / count * 100) : 0;
  } finally {
    pub.disconnect();
    sub.disconnect();
  }

  return result;
}

async function benchBandwidthHttp(qos = 0) {
  const result = { protocol: 'HTTP', qos, messagesSent: 0, messagesRecv: 0, sendDurationS: 0, recvDurationS: 0, sendMsgPerS: 0, recvMsgPerS: 0, lossPct: 0 };
  const topic = `bench/bw/http/${qos}/${crypto.randomBytes(3).toString('hex')}`;
  const count = BENCH.bandwidth_msg_count;

  const pub = new HttpHelper();
  const sub = new HttpHelper();

  try {
    await pub.connect(false);
    await sub.connect();
    await sub.subscribe(topic, qos);
    await sleep(300);

    const batchSize = 100;
    result.messagesSent = count;
    const t0 = performance.now();

    for (let i = 0; i < count; i += batchSize) {
      const chunk = [];
      for (let j = i; j < Math.min(i + batchSize, count); j++) {
        chunk.push({ topic, payload: `msg-${j}`, qos });
      }
      await pub.publishBatch(chunk);
    }

    result.sendDurationS = (performance.now() - t0) / 1000;
    const collected = await sub.pollUntil(topic, count, BENCH.timeout * 1000);
    result.recvDurationS = (performance.now() - t0) / 1000;
    result.messagesRecv = collected.length;

    if (result.sendDurationS > 0) {
      result.sendMsgPerS = count / result.sendDurationS;
    }
    if (result.recvDurationS > 0) {
      result.recvMsgPerS = result.messagesRecv / result.recvDurationS;
    }
    result.lossPct = count > 0 ? ((count - result.messagesRecv) / count * 100) : 0;
  } finally {
    await pub.disconnect();
    await sub.disconnect();
  }

  return result;
}


async function testBandwidth(avail) {
  logHeader('📶  BANDWIDTH BENCHMARK (Node.js)');
  
  for (const qos of BENCH.qos_levels) {
    if (avail.includes('mqtt')) {
      const r = await benchBandwidthMqtt(qos);
      logMetric(`[MQTT] QoS ${qos}: ${r.sendMsgPerS.toFixed(0)} msg/s send, ${r.recvMsgPerS.toFixed(0)} msg/s recv, ${r.lossPct.toFixed(2)}% loss`);
    }
    if (avail.includes('http')) {
      const r = await benchBandwidthHttp(qos);
      logMetric(`[HTTP] QoS ${qos}: ${r.sendMsgPerS.toFixed(0)} msg/s send, ${r.recvMsgPerS.toFixed(0)} msg/s recv, ${r.lossPct.toFixed(2)}% loss`);
    }
  }
}

async function testLatency(avail) {
  logHeader('⏱  LATENCY BENCHMARK (Node.js)');
  
  for (const qos of BENCH.qos_levels) {
    if (avail.includes('mqtt')) {
      const h = new MqttHelper();
      await h.connect();
      await h.subscribe('lat/mqtt', qos);
      await sleep(200);

      const lats = [];
      let rounds = Math.min(BENCH.latency_rounds, 50);
      for (let i = 0; i < rounds; i++) {
        const t0 = performance.now();
        h.publish('lat/mqtt', Buffer.from('x'), qos);
        
        const gotMsg = await h.waitForMessage(1000);
        
        if (gotMsg) {
          lats.push(performance.now() - t0);
          h.received = [];
        }
      }

      if (lats.length > 0) {
        const avg = lats.reduce((a, b) => a + b, 0) / lats.length;
        const sorted = [...lats].sort((a, b) => a - b);
        const p95Index = Math.floor(sorted.length * 0.95);
        const p95 = sorted[p95Index] || sorted[sorted.length - 1];
        logMetric(`[MQTT] QoS ${qos}: Avg ${avg.toFixed(2)}ms, P95 ${p95.toFixed(2)}ms`);
      }
      
      h.disconnect();
    }

    if (avail.includes('http')) {
      const h = new HttpHelper();
      await h.connect();
      await h.subscribe('lat/http', qos);
      await sleep(200);

      const lats = [];
      let rounds = Math.min(BENCH.latency_rounds, 50);
      for (let i = 0; i < rounds; i++) {
        const t0 = performance.now();
        await h.publish('lat/http', 'x', qos);
        
        const deadline = Date.now() + 1000;
        let gotIt = false;
        while (Date.now() < deadline) {
          const batch = h.poll('lat/http', 1);
          if (batch.length > 0) {
            lats.push(performance.now() - t0);
            gotIt = true;
            break;
          }
          await sleep(1);
        }
        
        if (!gotIt) {
          lats.push(1000);
        }
      }

      if (lats.length > 0) {
        const avg = lats.reduce((a, b) => a + b, 0) / lats.length;
        const sorted = [...lats].sort((a, b) => a - b);
        const p95Index = Math.floor(sorted.length * 0.95);
        const p95 = sorted[p95Index] || sorted[sorted.length - 1];
        logMetric(`[HTTP] QoS ${qos}: Avg ${avg.toFixed(2)}ms, P95 ${p95.toFixed(2)}ms`);
      }
      
      await h.disconnect();
    }
  }
}

async function testOrdering(avail) {
  logHeader('📐  ORDERING TEST (Node.js)');
  const count = BENCH.ordering_msg_count;

  if (avail.includes('mqtt')) {
    const sub = new MqttHelper();
    const pub = new MqttHelper();
    
    await sub.connect();
    await sub.subscribe('ord/mqtt', 1);
    await sleep(200);
    await pub.connect();

    for (let i = 0; i < count; i++) {
      const buf = Buffer.alloc(4);
      buf.writeUInt32BE(i, 0);
      pub.publish('ord/mqtt', buf, 1);
    }

    await sub.waitFor(count, 5000);
    
    const seqs = sub.received.map(m => m.payload.readUInt32BE(0));
    let ooo = 0;
    for (let i = 1; i < seqs.length; i++) {
      if (seqs[i] < seqs[i - 1]) ooo++;
    }
    
    logMetric(`[MQTT] Received ${seqs.length}/${count}, Out-of-order: ${ooo}`);
    
    pub.disconnect();
    sub.disconnect();
  }

  if (avail.includes('http')) {
    const sub = new HttpHelper();
    const pub = new HttpHelper();
    
    await sub.connect();
    await sub.subscribe('ord/http', 1);
    await sleep(200);
    await pub.connect(false);

    for (let i = 0; i < count; i++) {
      await pub.publish('ord/http', JSON.stringify({ s: i }), 1);
    }

    const msgs = await sub.pollUntil('ord/http', count, 5000);
    const seqs = msgs.map(m => m.payload?.s ?? -1);
    let ooo = 0;
    for (let i = 1; i < seqs.length; i++) {
      if (seqs[i] !== null && seqs[i - 1] !== null && seqs[i] < seqs[i - 1]) {
        ooo++;
      }
    }
    
    logMetric(`[HTTP] Received ${seqs.length}/${count}, Out-of-order: ${ooo}`);
    
    await pub.disconnect();
    await sub.disconnect();
  }
}

async function main() {
  const args = process.argv.slice(2);
  const testArg = args.find(a => a.startsWith('--test='))?.split('=')[1] || 'all';
  const protocolArg = args.find(a => a.startsWith('--protocol='))?.split('=')[1] || 'both';

  logInfo(`Starting Node.js benchmark: test=${testArg}, protocol=${protocolArg}`);

  const avail = [];

  // Check MQTT
  try {
    const mqttClient = new MqttHelper();
    await mqttClient.connect(5000);
    avail.push('mqtt');
    logPass('MQTT broker reachable');
    mqttClient.disconnect();
  } catch (e) {
    logFail('MQTT broker unreachable');
  }

  // Check HTTP
  try {
    const response = await axios.get(url('/health'), { timeout: 2000 });
    if (response.status === 200) {
      avail.push('http');
      avail.push('ws');  // WebSocket uses HTTP API
      logPass('HTTP/WebSocket bridge reachable');
    }
  } catch (e) {
    logFail('HTTP bridge unreachable');
  }

  if (avail.length === 0) {
    console.error('No protocols available');
    process.exit(1);
  }

  if (protocolArg !== 'both') {
    const filtered = avail.filter(p => p === protocolArg);
    if (filtered.length === 0) {
      console.error(`Protocol ${protocolArg} not available`);
      process.exit(1);
    }
    avail.length = 0;
    avail.push(...filtered);
  }

  if (testArg === 'all' || testArg === 'bandwidth') {
    await testBandwidth(avail);
  }
  if (testArg === 'all' || testArg === 'latency') {
    await testLatency(avail);
  }
  if (testArg === 'all' || testArg === 'ordering') {
    await testOrdering(avail);
  }
}

main().catch(console.error);
