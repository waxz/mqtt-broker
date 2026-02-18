// ==UserScript==
// @name        MQTT SSE Client + Resume + Benchmark
// @namespace   Violentmonkey Scripts
// @match       https://example.org/*
// @grant       GM_xmlhttpRequest
// @connect     *
// @run-at      document-start
// ==/UserScript==

const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ═══════════════════════════════════════════════
 *  Nano Event Emitter
 * ═══════════════════════════════════════════════ */
const createNanoEvents = () => ({
    events: {},
    emit(ev, ...a) { for (const cb of (this.events[ev] || [])) cb(...a); },
    on(ev, cb) {
        (this.events[ev] ||= []).push(cb);
        return () => { this.events[ev] = this.events[ev]?.filter(i => i !== cb); };
    }
});

/* ═══════════════════════════════════════════════
 *  Page Lifecycle Manager
 *
 *  Detects: tab hidden/visible, time-gap suspension,
 *           network resume, screen lock/unlock
 *
 *  Events: hidden, visible, suspend, resume, online, offline
 * ═══════════════════════════════════════════════ */
class PageLifecycle {
    constructor(opts = {}) {
        this._ev = createNanoEvents();
        this._heartbeatInterval = opts.heartbeatInterval || 5000;
        this._suspendThreshold = opts.suspendThreshold || 15000; // 15s gap = suspended
        this._lastHeartbeat = Date.now();
        this._lastVisibleAt = Date.now();
        this._lastHiddenAt = null;
        this._state = document.visibilityState || 'visible';
        this._suspended = false;
        this._heartbeatTimer = null;
        this._wakeLock = null;

        this._init();
    }

    _init() {
        // 1. Visibility API — primary detection
        document.addEventListener('visibilitychange', () => {
            const now = Date.now();
            if (document.visibilityState === 'hidden') {
                this._state = 'hidden';
                this._lastHiddenAt = now;
                this._emit('hidden', { at: now });
            } else {
                const hiddenFor = this._lastHiddenAt ? now - this._lastHiddenAt : 0;
                this._state = 'visible';
                this._lastVisibleAt = now;
                this._emit('visible', { hiddenFor, at: now });

                // Check if we were actually suspended (time gap)
                const gap = now - this._lastHeartbeat;
                if (gap > this._suspendThreshold) {
                    this._emitResume(gap, 'visibility');
                }
            }
        });

        // 2. Focus/blur as backup (some browsers don't fire visibilitychange reliably)
        window.addEventListener('focus', () => {
            const gap = Date.now() - this._lastHeartbeat;
            if (gap > this._suspendThreshold) {
                this._emitResume(gap, 'focus');
            }
        });

        // 3. Online/offline detection
        window.addEventListener('online', () => {
            this._emit('online');
            // Network back → likely need to reconnect
            const gap = Date.now() - this._lastHeartbeat;
            if (gap > this._suspendThreshold) {
                this._emitResume(gap, 'online');
            }
        });
        window.addEventListener('offline', () => this._emit('offline'));

        // 4. Page freeze/resume (Page Lifecycle API — Chrome 68+)
        document.addEventListener('freeze', () => this._emit('freeze'));
        document.addEventListener('resume', () => {
            const gap = Date.now() - this._lastHeartbeat;
            this._emitResume(gap, 'page-resume');
        });

        // 5. Heartbeat — detects suspension by time gap
        this._startHeartbeat();
    }

    _startHeartbeat() {
        // Use both setInterval and recursive setTimeout for robustness
        this._lastHeartbeat = Date.now();
        this._heartbeatTimer = setInterval(() => {
            const now = Date.now();
            const gap = now - this._lastHeartbeat;

            if (gap > this._suspendThreshold) {
                // Timer fired late → we were suspended/throttled
                this._emitResume(gap, 'heartbeat');
            }

            this._lastHeartbeat = now;
        }, this._heartbeatInterval);
    }
    _emitResume(gap, source) {
        // Debounce: skip if we already resumed recently
        if (this._lastResumeAt && Date.now() - this._lastResumeAt < 2000) return;
        this._lastResumeAt = Date.now();
        this._lastHeartbeat = Date.now();
        this._emit('resume', { suspendedFor: gap, source, at: Date.now() });
    }
    _emitResume1(gap, source) {
        if (this._suspended && Date.now() - this._lastResumeAt < 2000) return; // debounce
        this._suspended = false;
        this._lastResumeAt = Date.now();
        this._lastHeartbeat = Date.now();
        this._emit('resume', {
            suspendedFor: gap,
            source,
            at: Date.now()
        });
    }

    // Optional: request Wake Lock to prevent sleep (requires HTTPS, user gesture)
    async requestWakeLock() {
        if (!('wakeLock' in navigator)) return false;
        try {
            this._wakeLock = await navigator.wakeLock.request('screen');
            this._wakeLock.addEventListener('release', () => {
                this._wakeLock = null;
                this._emit('wakelock-released');
            });
            this._emit('wakelock-acquired');
            // Re-acquire on visibility change
            document.addEventListener('visibilitychange', async () => {
                if (document.visibilityState === 'visible' && !this._wakeLock) {
                    try { this._wakeLock = await navigator.wakeLock.request('screen'); }
                    catch { }
                }
            });
            return true;
        } catch { return false; }
    }

    getState() {
        return {
            visibility: this._state,
            lastHeartbeat: this._lastHeartbeat,
            lastVisibleAt: this._lastVisibleAt,
            lastHiddenAt: this._lastHiddenAt,
            online: navigator.onLine,
            heartbeatAge: ((Date.now() - this._lastHeartbeat) / 1000) | 0
        };
    }

    on(ev, cb) { return this._ev.on(ev, cb); }
    _emit(ev, ...a) { this._ev.emit(ev, ...a); }

    destroy() {
        if (this._heartbeatTimer) clearInterval(this._heartbeatTimer);
        if (this._wakeLock) { try { this._wakeLock.release(); } catch { } }
    }
}

/* ═══════════════════════════════════════════════
 *  SSE Streamer  (GM_xmlhttpRequest based)
 * ═══════════════════════════════════════════════ */
class Streamer {
    constructor() {
        this._buf = '';
        this._gen = 0;
        this._req = null;
        this._ev = createNanoEvents();
    }

    start(url, method = 'GET', { headers = {}, connectTimeout = 30000 } = {}) {
        this.stop();
        const gen = ++this._gen;
        this._buf = '';
        let lastLen = 0, connected = false;

        return new Promise((resolve, reject) => {
            const connTimer = connectTimeout > 0 ? setTimeout(() => {
                if (!connected && gen === this._gen) {
                    const e = new Error(`SSE connect timeout (${connectTimeout}ms)`);
                    this._emit('error', e);
                    this.stop();
                    reject(e);
                }
            }, connectTimeout) : null;
            const clearConn = () => { if (connTimer) clearTimeout(connTimer); };

            try {
                const req = GM_xmlhttpRequest({
                    method, url,
                    headers: { 'Accept': 'text/event-stream', 'Cache-Control': 'no-cache', ...headers },
                    onreadystatechange: (res) => {
                        if (gen !== this._gen) return;
                        if (res.readyState >= 3 && res.responseText) {
                            if (!connected) { connected = true; clearConn(); }
                            const chunk = res.responseText.slice(lastLen);
                            if (chunk.length > 0) {
                                lastLen = res.responseText.length;
                                this._parse(chunk);
                            }
                            if (res.responseText.length > 5 * 1024 * 1024) {
                                this._emit('error', new Error('Response buffer >5 MB'));
                                this.stop();
                            }
                        }
                    },
                    onload: () => { clearConn(); if (gen === this._gen) { this._emit('end'); resolve(); } },
                    onerror: (err) => { clearConn(); if (gen === this._gen) { const e = new Error(err?.error || 'Stream error'); this._emit('error', e); reject(e); } },
                    onabort: () => { clearConn(); if (gen === this._gen) { this._emit('abort'); resolve(); } }
                });
                this._req = req || null;
            } catch (e) { clearConn(); this._emit('error', e); reject(e); }
        });
    }

    stop() {
        this._gen++;
        if (this._req) { try { this._req.abort?.(); } catch { } this._req = null; }
        this._buf = '';
    }

    _parse(chunk) {
        this._buf += chunk;
        if (this._buf.length > 512 * 1024) { this._emit('error', new Error('Parse overflow')); this._buf = ''; return; }
        const lines = this._buf.split(/\r?\n/);
        this._buf = lines.pop() || '';
        for (const raw of lines) {
            const l = raw.trim();
            if (!l) continue;
            if (l.startsWith('data:')) { const v = l.slice(5).trim(); if (v === '[DONE]') this._emit('done'); else this._emit('data', v); }
            else if (l.startsWith('event:')) { this._emit('event', l.slice(6).trim()); }
        }
    }

    on(ev, cb) { return this._ev.on(ev, cb); }
    _emit(ev, ...a) { this._ev.emit(ev, ...a); }
}

/* ═══════════════════════════════════════════════
 *  MQTT-over-SSE Client
 *
 *  Now with PageLifecycle integration
 * ═══════════════════════════════════════════════ */
class MqttSseClient {
    constructor(apiBase, opts = {}) {
        this.apiBase = apiBase.replace(/\/$/, '');
        this.apiKey = opts.apiKey || '';
        this.username = opts.username || '';
        this.password = opts.password || '';
        this.pollMethod = opts.pollMethod || 'GET';

        this.clientId = null;
        this._topicHandlers = new Map();
        this._listeners = {};
        this._stream = null;
        this._streamUnsubs = [];
        this._isConnecting = false;
        this._stopping = false;
        this._sseActive = false;
        this._currentEvent = 'message';

        // Subscriptions tracking — for auto-resubscribe on reconnect
        this._subscribedTopics = new Map(); // topic → qos

        this.reconnectDelay = 1000;
        this.maxReconnectDelay = 30000;
        this._retryTimer = null;
        this._livenessTimer = null;
        this._lastDataAt = 0;
        this._lastActivityAt = 0;
        this.LIVENESS_TIMEOUT = 90_000;

        this.metrics = {
            msgIn: 0, msgOut: 0, parseErrors: 0,
            reconnects: 0, publishErrors: 0,
            suspendResumes: 0, startTime: Date.now()
        };

        // Page lifecycle integration
        this._lifecycle = new PageLifecycle({
            heartbeatInterval: 5000,
            suspendThreshold: 15000
        });
        this._lifecycleUnsubs = [];
        this._initLifecycle();
    }

    /* ── Page Lifecycle Integration ──────────── */
    _initLifecycle() {
        this._lifecycleUnsubs.push(
            this._lifecycle.on('resume', (info) => {
                this._onPageResume(info);
            }),
            this._lifecycle.on('hidden', () => {
                this._emit('debug', 'Page hidden — timers may throttle');
                this._emit('page-hidden');
            }),
            this._lifecycle.on('visible', (info) => {
                this._emit('debug', `Page visible (hidden ${(info.hiddenFor / 1000).toFixed(1)}s)`);
                this._emit('page-visible', info);
            }),
            this._lifecycle.on('offline', () => {
                this._emit('debug', 'Network offline');
                this._emit('offline');
            }),
            this._lifecycle.on('online', () => {
                this._emit('debug', 'Network online');
                this._emit('online');
            })
        );
    }
    _onPageResume(info) {
        const { suspendedFor, source } = info;
        this.metrics.suspendResumes++;
        this._emit('debug', `⏰ Page resumed after ${(suspendedFor / 1000).toFixed(1)}s (${source})`);
        this._emit('resume', info);

        if (this._stopping) return;

        if (this.clientId) {
            this._emit('debug', 'Force reconnect after suspend');
            this._forceReconnect('Page resumed after suspension');
        } else if (this._isConnecting) {
            // ── NEW: kill stale in-flight connect, start fresh ──
            this._emit('debug', 'Aborting stale connect after resume');
            this._killStream();
            this._isConnecting = false;     // allow new connect()
            this.clientId = null;
            this.reconnectDelay = 1000;
            this.connect();
        } else if (this._retryTimer) {
            clearTimeout(this._retryTimer);
            this._retryTimer = null;
            this.reconnectDelay = 1000;
            this._emit('debug', 'Immediate retry after resume');
            this.connect();
        } else {
            // ── NEW: no clientId, no retry, no connecting → restart ──
            this._emit('debug', 'No active state after resume, reconnecting');
            this.reconnectDelay = 1000;
            this.connect();
        }
    }
    _onPageResume1(info) {
        const { suspendedFor, source } = info;
        this.metrics.suspendResumes++;
        this._emit('debug',
            `⏰ Page resumed after ${(suspendedFor / 1000).toFixed(1)}s (${source})`);
        this._emit('resume', info);

        if (!this._stopping) {
            // Strategy: if we have a clientId, assume connection is stale → force reconnect
            if (this.clientId) {
                this._emit('debug', 'Force reconnect after suspend');
                this._forceReconnect('Page resumed after suspension');
            }
            // If we were in retry state, restart immediately
            else if (this._retryTimer) {
                clearTimeout(this._retryTimer);
                this._retryTimer = null;
                this.reconnectDelay = 1000; // reset backoff
                this._emit('debug', 'Immediate retry after resume');
                this.connect();
            }
        }
    }
    _forceReconnect(reason) {
        this._killStream();
        this.clientId = null;
        this.reconnectDelay = 1000;
        this.metrics.reconnects++;
        this._emit('disconnected', reason);
        if (!this._stopping) {
            this.connect().catch(e => {
                this._emit('debug', `Reconnect failed: ${e?.message}`);
            });
        }
    }
    _forceReconnect1(reason) {
        // Kill everything, but DON'T set _stopping
        this._killStream();
        this.clientId = null;
        this.reconnectDelay = 1000; // reset backoff — this is a resume, not repeated failure
        this.metrics.reconnects++;
        this._emit('disconnected', reason);
        // Immediate reconnect (no delay)
        if (!this._stopping) {
            this.connect();
        }
    }

    /* ── HTTP helper ─────────────────────────── */
    _request(method, path, body = null) {
        return new Promise((resolve, reject) => {
            try {
                GM_xmlhttpRequest({
                    method, url: `${this.apiBase}${path}`,
                    headers: { 'Content-Type': 'application/json' },
                    data: body ? JSON.stringify(body) : null,
                    timeout: 15000,
                    onload: (res) => {
                        if (res.status >= 200 && res.status < 300) {
                            try { resolve(JSON.parse(res.responseText)); } catch { resolve(res.responseText); }
                        } else reject(new Error(`HTTP ${res.status}: ${res.responseText?.slice(0, 200)}`));
                    },
                    onerror: (e) => reject(new Error(`Network: ${e?.error || 'unknown'}`)),
                    ontimeout: () => reject(new Error('Timeout'))
                });
            } catch (e) { reject(e); }
        });
    }

    _isSessionError(err) { return /^HTTP (404|410|401)/.test(err?.message || ''); }

    /* ── Public API ──────────────────────────── */
    async connect() {
        if (this._isConnecting || this.clientId || this._stopping) return false;
        if (this._retryTimer) { clearTimeout(this._retryTimer); this._retryTimer = null; }

        this._isConnecting = true;
        try {
            this._emit('connecting');
            const res = await this._request('POST', '/clients/connect', {
                apiKey: this.apiKey, brokerUsername: this.username, brokerPassword: this.password
            });
            if (this._stopping) return false;
            if (!res.success) throw new Error(res.error || 'Rejected');

            this.clientId = res.client_id;
            this.reconnectDelay = 1000;
            this._startStream();
            this._emit('connect', this.clientId);
            await this._resubscribeAll();

            // ── NEW: verify connection survived resubscribe ──
            if (!this.clientId) {
                this._emit('debug', 'Connection lost during resubscribe');
                return false;               // _handleDisconnect already scheduled retry
            }
            return true;
        } catch (e) {
            this._emit('error', e);
            if (!this._stopping) this._scheduleRetry();
            return false;
        } finally {
            this._isConnecting = false;
            // ── NEW: if disconnected while connecting, ensure retry exists ──
            if (!this.clientId && !this._stopping && !this._retryTimer) {
                this._scheduleRetry();
            }
        }
    }

    async connect1() {
        if (this._isConnecting || this.clientId || this._stopping) return false;
        if (this._retryTimer) { clearTimeout(this._retryTimer); this._retryTimer = null; }

        this._isConnecting = true;
        try {
            this._emit('connecting');
            const res = await this._request('POST', '/clients/connect', {
                apiKey: this.apiKey, brokerUsername: this.username, brokerPassword: this.password
            });
            if (this._stopping) { this._emit('debug', 'Connect aborted (stopping)'); return false; }
            if (!res.success) throw new Error(res.error || 'Rejected');
            this.clientId = res.client_id;
            this.reconnectDelay = 1000;
            this._startStream();
            this._emit('connect', this.clientId);

            // Auto-resubscribe to all tracked topics
            await this._resubscribeAll();

            return true;
        } catch (e) {
            this._emit('error', e);
            if (!this._stopping) this._scheduleRetry();
            return false;
        } finally {
            this._isConnecting = false;
        }
    }

    async subscribe(topic, qos = 0) {
        if (!this.clientId) throw new Error('Not connected');
        this._emit('debug', `Subscribe: ${topic}`);
        try {
            const result = await this._request('POST', `/clients/${this.clientId}/subscribe`, { topic, qos });
            // Track for auto-resubscribe
            this._subscribedTopics.set(topic, qos);
            return result;
        } catch (e) {
            if (this._isSessionError(e)) this._handleDisconnect('Session expired (subscribe)');
            throw e;
        }
    }

    async unsubscribe(topic) {
        this._subscribedTopics.delete(topic);
        // Optionally call server unsubscribe if API supports it
    }
    async _resubscribeAll() {
        if (this._subscribedTopics.size === 0) return;
        this._emit('debug', `Re-subscribing to ${this._subscribedTopics.size} topic(s)`);

        const failed = [];
        const promises = [];
        for (const [topic, qos] of this._subscribedTopics) {
            promises.push(
                this._request('POST', `/clients/${this.clientId}/subscribe`, { topic, qos })
                    .then(() => this._emit('debug', `Re-subscribed: ${topic}`))
                    .catch(e => {
                        this._emit('debug', `Re-sub failed ${topic}: ${e.message}`);
                        failed.push([topic, qos]);
                    })
            );
        }
        await Promise.allSettled(promises);

        // Retry failed subs once after a short delay
        if (failed.length > 0 && this.clientId) {
            this._emit('debug', `Retrying ${failed.length} failed subscription(s)…`);
            await sleep(1000);
            for (const [topic, qos] of failed) {
                if (!this.clientId) break;
                try {
                    await this._request('POST', `/clients/${this.clientId}/subscribe`, { topic, qos });
                    this._emit('debug', `Re-sub retry OK: ${topic}`);
                } catch (e) {
                    this._emit('error', new Error(`Re-sub retry failed ${topic}: ${e.message}`));
                }
            }
        }
    }
    async _resubscribeAll1() {
        if (this._subscribedTopics.size === 0) return;
        this._emit('debug', `Re-subscribing to ${this._subscribedTopics.size} topic(s)`);
        const promises = [];
        for (const [topic, qos] of this._subscribedTopics) {
            promises.push(
                this._request('POST', `/clients/${this.clientId}/subscribe`, { topic, qos })
                    .then(() => this._emit('debug', `Re-subscribed: ${topic}`))
                    .catch(e => this._emit('debug', `Re-sub failed ${topic}: ${e.message}`))
            );
        }
        await Promise.allSettled(promises);
    }

    async publish(topic, payload, qos = 0) {
        if (!this.clientId) { this.metrics.publishErrors++; throw new Error('Not connected'); }
        const data = typeof payload === 'object' ? JSON.stringify(payload) : String(payload);
        try {
            const r = await this._request('POST', `/clients/${this.clientId}/publish`, { topic, payload: data, qos });
            this.metrics.msgOut++;
            this._lastActivityAt = Date.now();
            return r;
        } catch (e) {
            this.metrics.publishErrors++;
            if (this._isSessionError(e)) {
                this._emit('debug', 'Session expired on publish, reconnecting');
                this._handleDisconnect('Session expired (publish)');
            }
            throw e;
        }
    }

    disconnect() {
        this._stopping = true;
        if (this._retryTimer) { clearTimeout(this._retryTimer); this._retryTimer = null; }
        this._killStream();
        const was = !!this.clientId;
        this.clientId = null;
        this.reconnectDelay = 1000;
        if (was) this._emit('disconnected', 'Manual');
    }

    enableReconnect() { this._stopping = false; }

    getStatus() {
        const now = Date.now();
        return {
            connected: !!this.clientId, clientId: this.clientId,
            sseActive: this._sseActive, stopping: this._stopping,
            subscribedTopics: [...this._subscribedTopics.keys()],
            lastDataAge: this._lastDataAt ? ((now - this._lastDataAt) / 1000) | 0 : null,
            lastActivityAge: this._lastActivityAt ? ((now - this._lastActivityAt) / 1000) | 0 : null,
            page: this._lifecycle.getState(),
            metrics: { ...this.metrics },
            uptime: ((now - this.metrics.startTime) / 1000) | 0
        };
    }

    /* ── SSE Stream ──────────────────────────── */
    _startStream() {
        this._killStream();
        this._sseActive = true;
        this._lastDataAt = Date.now();
        this._lastActivityAt = Date.now();
        this._currentEvent = 'message';

        const stream = new Streamer();
        this._stream = stream;
        const url = `${this.apiBase}/clients/${this.clientId}/messages`;
        this._emit('debug', `SSE → ${url}`);

        this._streamUnsubs = [
            stream.on('event', (name) => { this._currentEvent = name; }),
            stream.on('data', (raw) => {
                this._lastDataAt = Date.now();
                this._lastActivityAt = Date.now();
                if (!raw) return;
                try {
                    const payload = JSON.parse(raw);
                    if (this._currentEvent === 'messages' && Array.isArray(payload))
                        payload.forEach(m => this._dispatch(m));
                    else this._dispatch(payload);
                } catch (e) {
                    this.metrics.parseErrors++;
                    this._emit('parse_error', raw.slice(0, 120), e.message);
                }
                this._currentEvent = 'message';
            }),
            stream.on('end', () => { this._emit('debug', 'SSE ended'); this._handleDisconnect('Stream ended'); }),
            stream.on('error', (e) => { this._emit('debug', `SSE err: ${e?.message}`); this._handleDisconnect('Stream error'); })
        ];

        stream.start(url, this.pollMethod).catch(() => { });
        this._startLivenessCheck();
    }

    _killStream() {
        for (const u of this._streamUnsubs) u();
        this._streamUnsubs = [];
        if (this._stream) { this._stream.stop(); this._stream = null; }
        if (this._livenessTimer) { clearInterval(this._livenessTimer); this._livenessTimer = null; }
        this._sseActive = false;
    }

    _startLivenessCheck() {
        if (this._livenessTimer) clearInterval(this._livenessTimer);
        this._livenessTimer = setInterval(() => {
            if (!this.clientId || !this._sseActive) return;

            // Don't trigger liveness when page is hidden — browser throttles timers
            if (document.visibilityState === 'hidden') return;

            const silent = Date.now() - Math.max(this._lastDataAt, this._lastActivityAt);
            if (silent > this.LIVENESS_TIMEOUT) {
                this._emit('debug', `Stall: ${(silent / 1000) | 0}s no activity`);
                this._handleDisconnect('Liveness timeout');
            }
        }, 10_000);
    }

    /* ── Dispatch ────────────────────────────── */
    _dispatch(msg) {
        if (!msg?.topic) return;
        if (typeof msg.payload === 'string') { try { msg.payload = JSON.parse(msg.payload); } catch { } }
        this.metrics.msgIn++;
        const call = (set, ...a) => { for (const cb of set) { try { cb(...a); } catch (e) { this._emit('debug', `Handler: ${e.message}`); } } };
        const h = this._topicHandlers.get(msg.topic); if (h) call(h, msg.payload, msg);
        const w = this._topicHandlers.get('*'); if (w) call(w, msg.topic, msg.payload, msg);
        this._emit('message', msg.topic, msg.payload);
    }

    /* ── Reconnection ────────────────────────── */
    _handleDisconnect(reason) {
        const was = !!this.clientId;
        this.clientId = null;
        this._killStream();
        if (was) {
            this.metrics.reconnects++;
            this._emit('disconnected', reason);
            if (!this._stopping) this._scheduleRetry();
        }
    }

    _scheduleRetry() {
        if (this._retryTimer) clearTimeout(this._retryTimer);
        if (this._stopping) return;
        const delay = this.reconnectDelay;
        this.reconnectDelay = Math.min(delay * 1.5, this.maxReconnectDelay);
        this._emit('debug', `Retry in ${(delay / 1000).toFixed(1)}s`);
        // this._retryTimer = setTimeout(() => {
        //   this._retryTimer = null;
        //   if (!this.clientId && !this._isConnecting && !this._stopping) this.connect();
        // }, delay);
        this._retryTimer = setTimeout(() => {
            this._retryTimer = null;
            if (this._stopping) return;
            if (this._isConnecting) {
                // Another connect in progress — check back later
                this._emit('debug', 'Retry deferred (connect in progress)');
                this._scheduleRetry();
                return;
            }
            if (!this.clientId) this.connect();
        }, delay);
    }

    /* ── Event API ───────────────────────────── */
    onTopic(topic, cb) {
        if (!this._topicHandlers.has(topic)) this._topicHandlers.set(topic, new Set());
        this._topicHandlers.get(topic).add(cb);
        return () => this._topicHandlers.get(topic)?.delete(cb);
    }

    on(event, cb) {
        if (event.includes('/') || event === '*') return this.onTopic(event, cb);
        (this._listeners[event] ||= []).push(cb);
        return () => { this._listeners[event] = this._listeners[event]?.filter(f => f !== cb); };
    }

    off(event, cb) {
        if (event.includes('/') || event === '*') this._topicHandlers.get(event)?.delete(cb);
        else this._listeners[event] = this._listeners[event]?.filter(f => f !== cb);
    }

    _emit(ev, ...a) { for (const cb of (this._listeners[ev] || [])) { try { cb(...a); } catch { } } }

    destroy() {
        this.disconnect();
        for (const u of this._lifecycleUnsubs) u();
        this._lifecycleUnsubs = [];
        this._lifecycle.destroy();
        this._topicHandlers.clear();
        this._listeners = {};
    }
}

/* ═══════════════════════════════════════════════
 *  BENCHMARK
 * ═══════════════════════════════════════════════ */
class Benchmark {
    constructor(mqtt) { this.mqtt = mqtt; this.running = false; this._prog = null; }
    onProgress(cb) { this._prog = cb; }
    _p(msg) { this._prog?.(msg); }
    stop() { this.running = false; }

    async run(type, topic, count) {
        if (this.running) throw new Error('Already running');
        this.running = true;
        try {
            switch (type) {
                case 'throughput': return await this._throughput(topic, count);
                case 'latency': return await this._latency(topic, count);
                case 'burst': return await this._burst(topic, count);
                case 'reconnect': return await this._reconnect(count);
                default: throw new Error(`Unknown: ${type}`);
            }
        } finally { this.running = false; }
    }

    async _throughput(topic, count) {
        let ok = 0, fail = 0; const t0 = performance.now();
        for (let i = 0; i < count && this.running; i++) {
            try { await this.mqtt.publish(topic, { _bench: 'tp', _seq: i, ts: Date.now() }); ok++; } catch { fail++; }
            this._p(`Throughput: ${i + 1}/${count}  (${ok} ok, ${fail} err)`);
        }
        const ms = performance.now() - t0;
        return { type: 'throughput', total: count, ok, fail, ms: ms.toFixed(0), rate: (ok / (ms / 1000)).toFixed(1) };
    }

    async _latency(topic, count) {
        const id = Math.random().toString(36).slice(2, 8), pending = new Map(), results = [];
        try { await this.mqtt.subscribe(topic); } catch { }
        const unsub = this.mqtt.onTopic(topic, (p) => {
            if (p?._bid !== id) return;
            const sent = pending.get(p._seq);
            if (sent != null) { results.push(Date.now() - sent); pending.delete(p._seq); }
        });
        for (let i = 0; i < count && this.running; i++) {
            pending.set(i, Date.now());
            try { await this.mqtt.publish(topic, { _bid: id, _seq: i, _ts: Date.now() }); } catch { }
            this._p(`Latency: sent ${i + 1}/${count}, received ${results.length}`);
            await sleep(150);
        }
        const deadline = Date.now() + 5000;
        while (results.length < count && Date.now() < deadline && this.running) {
            this._p(`Latency: waiting… ${results.length}/${count}`); await sleep(200);
        }
        unsub();
        const s = [...results].sort((a, b) => a - b);
        const pct = p => s.length ? s[Math.min(Math.floor(s.length * p / 100), s.length - 1)] : null;
        return {
            type: 'latency', sent: count, received: s.length, lost: count - s.length,
            avg: s.length ? (s.reduce((a, b) => a + b, 0) / s.length).toFixed(0) : 'N/A',
            min: s[0] ?? 'N/A', max: s[s.length - 1] ?? 'N/A',
            p50: pct(50) ?? 'N/A', p95: pct(95) ?? 'N/A', p99: pct(99) ?? 'N/A'
        };
    }

    async _burst(topic, count) {
        this._p(`Burst: ${count} parallel…`); const t0 = performance.now();
        const all = Array.from({ length: count }, (_, i) => this.mqtt.publish(topic, { _bench: 'burst', _seq: i }));
        const res = await Promise.allSettled(all); const ms = performance.now() - t0;
        const ok = res.filter(r => r.status === 'fulfilled').length;
        return { type: 'burst', total: count, ok, fail: count - ok, ms: ms.toFixed(0), rate: (ok / (ms / 1000)).toFixed(1) };
    }

    async _reconnect(cycles) {
        const times = [];
        for (let i = 0; i < cycles && this.running; i++) {
            this._p(`Reconnect: cycle ${i + 1}/${cycles}…`); const t0 = performance.now();
            this.mqtt.disconnect(); this.mqtt.enableReconnect();
            try {
                await new Promise((resolve, reject) => {
                    let done = false;
                    const unsub = this.mqtt.on('connect', () => { if (done) return; done = true; unsub(); clearTimeout(tmr); resolve(); });
                    const tmr = setTimeout(() => { if (done) return; done = true; unsub(); reject(new Error('Timeout')); }, 30000);
                    this.mqtt.connect();
                });
                times.push(performance.now() - t0);
            } catch { times.push(-1); }
            await sleep(500);
        }
        const ok = times.filter(t => t > 0);
        return {
            type: 'reconnect', cycles, ok: ok.length, fail: cycles - ok.length,
            avg: ok.length ? (ok.reduce((a, b) => a + b, 0) / ok.length).toFixed(0) : 'N/A',
            min: ok.length ? Math.min(...ok).toFixed(0) : 'N/A', max: ok.length ? Math.max(...ok).toFixed(0) : 'N/A',
            each: times.map(t => t > 0 ? `${t.toFixed(0)}ms` : 'FAIL')
        };
    }
}

function formatResult(r) {
    const hr = '─'.repeat(40);
    switch (r.type) {
        case 'throughput': return [`THROUGHPUT`, hr, `OK ${r.ok}/${r.total}  Fail ${r.fail}`, `Time ${r.ms}ms  Rate ${r.rate} msg/s`].join('\n');
        case 'latency': return [`LATENCY`, hr, `Sent ${r.sent}  Recv ${r.received}  Lost ${r.lost}`,
            `Avg ${r.avg}ms  Min ${r.min}ms  Max ${r.max}ms`, `P50 ${r.p50}ms  P95 ${r.p95}ms  P99 ${r.p99}ms`].join('\n');
        case 'burst': return [`BURST`, hr, `OK ${r.ok}/${r.total}  Fail ${r.fail}`, `Time ${r.ms}ms  Rate ${r.rate} msg/s`].join('\n');
        case 'reconnect': return [`RECONNECT`, hr, `OK ${r.ok}/${r.cycles}  Fail ${r.fail}`,
            `Avg ${r.avg}ms  Min ${r.min}ms  Max ${r.max}ms`, `Each: ${r.each.join(', ')}`].join('\n');
        default: return JSON.stringify(r, null, 2);
    }
}

/* ═══════════════════════════════════════════════
 *  UI
 * ═══════════════════════════════════════════════ */
(async function main() {
    await new Promise(r => {
        if (document.readyState !== 'loading') r();
        else document.addEventListener('DOMContentLoaded', r, { once: true });
    });

    const style = document.createElement('style');
    style.textContent = `
    .mq-panel{position:fixed;top:12px;right:12px;width:420px;background:#1a1b26;color:#c0caf5;
      border:1px solid #3b4261;border-radius:10px;font:13px/1.4 system-ui,sans-serif;
      box-shadow:0 8px 32px #0006;z-index:99999;overflow:hidden}
    .mq-hdr{display:flex;justify-content:space-between;align-items:center;padding:10px 14px;
      background:#24283b;cursor:move;user-select:none}
    .mq-hdr b{font-size:14px}
    .mq-hdr button{background:none;border:none;color:#565f89;font-size:16px;cursor:pointer;padding:0 3px}
    .mq-hdr button:hover{color:#c0caf5}
    .mq-body{max-height:85vh;overflow-y:auto}
    .mq-status{padding:8px 14px;display:flex;align-items:center;gap:8px;border-bottom:1px solid #3b4261;font-size:12px}
    .mq-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
    .mq-dot.off{background:#f7768e}.mq-dot.on{background:#9ece6a}
    .mq-dot.wait{background:#e0af68;animation:mq-p 1s infinite}
    @keyframes mq-p{50%{opacity:.3}}
    .mq-sec{border-bottom:1px solid #3b4261}
    .mq-sh{padding:7px 14px;font:600 11px/1.4 system-ui;color:#565f89;text-transform:uppercase;
      letter-spacing:.5px;display:flex;justify-content:space-between;align-items:center;
      cursor:pointer;user-select:none}
    .mq-sh:hover{color:#c0caf5}
    .mq-sb{padding:4px 14px 10px}
    .mq-sb.h{display:none}
    .mq-row{display:flex;gap:6px;margin-bottom:6px}
    .mq input,.mq select{width:100%;padding:5px 8px;background:#292e42;
      border:1px solid #3b4261;border-radius:4px;color:#c0caf5;font-size:12px;
      box-sizing:border-box;margin-bottom:5px;outline:none;font-family:inherit}
    .mq input:focus,.mq select:focus{border-color:#7aa2f7}
    .mq label{font-size:11px;color:#565f89;display:block;margin-bottom:1px}
    .mq-btn{padding:5px 12px;border:1px solid #3b4261;border-radius:4px;background:#292e42;
      color:#c0caf5;cursor:pointer;font-size:12px;white-space:nowrap}
    .mq-btn:hover{background:#3b4261}.mq-btn:disabled{opacity:.35;cursor:not-allowed}
    .mq-btn.p{background:#7aa2f7;color:#1a1b26;border-color:#7aa2f7;font-weight:600}
    .mq-btn.p:hover{opacity:.85}
    .mq-btn.d{border-color:#f7768e;color:#f7768e}.mq-btn.d:hover{background:#f7768e;color:#1a1b26}
    .mq-btn.s{border-color:#9ece6a;color:#9ece6a}.mq-btn.s:hover{background:#9ece6a;color:#1a1b26}
    .mq-btn.w{border-color:#e0af68;color:#e0af68}.mq-btn.w:hover{background:#e0af68;color:#1a1b26}
    .mq-actions{padding:8px 14px;display:flex;gap:6px;flex-wrap:wrap;border-bottom:1px solid #3b4261}
    .mq-metrics{padding:8px 14px;font:11px/1.4 'Cascadia Code',monospace;color:#565f89;
      border-bottom:1px solid #3b4261;display:flex;gap:10px;flex-wrap:wrap}
    .mq-log{height:180px;overflow-y:auto;background:#16161e;border-radius:4px;
      font:11px/1.6 'Cascadia Code','Fira Code',monospace;padding:6px}
    .mq-le{padding:1px 0;word-break:break-all}
    .mq-le.msg{color:#9ece6a}.mq-le.err{color:#f7768e}.mq-le.dbg{color:#565f89}
    .mq-le.inf{color:#7dcfff}.mq-le.wrn{color:#e0af68}
    .mq-tags{display:flex;flex-wrap:wrap;gap:4px;margin-top:4px}
    .mq-tag{padding:2px 8px;background:#292e42;border-radius:10px;font-size:11px;
      display:inline-flex;align-items:center;gap:4px}
    .mq-tag button{background:none;border:none;color:#f7768e;cursor:pointer;font-size:13px;padding:0;line-height:1}
    .mq-bsm{padding:2px 6px;font-size:10px;background:none;border:1px solid #3b4261;
      border-radius:3px;color:#565f89;cursor:pointer}
    .mq-bsm:hover{color:#c0caf5;border-color:#c0caf5}
    .mq-bench-res{margin:6px 0 0;padding:8px;background:#16161e;border-radius:4px;
      font:11px/1.5 'Cascadia Code',monospace;color:#7dcfff;white-space:pre-wrap;
      max-height:160px;overflow-y:auto;border:1px solid #292e42}
    .mq-page-ind{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;
      background:#292e42;border-radius:10px;font-size:10px;margin-left:auto}
    .mq-page-ind .mq-pi-dot{width:6px;height:6px;border-radius:50%}
    .mq-page-ind .vis{background:#9ece6a}
    .mq-page-ind .hid{background:#e0af68;animation:mq-p 1.5s infinite}
    .mq-page-ind .off{background:#f7768e}
  `;
    document.head.appendChild(style);

    const wrap = document.createElement('div');
    wrap.className = 'mq';
    wrap.innerHTML = `
  <div class="mq-panel" id="mqP">
    <div class="mq-hdr" id="mqHdr">
      <b>🔌 MQTT SSE Client</b>
      <span><button id="mqMin" title="Minimize">−</button></span>
    </div>
    <div class="mq-body" id="mqBody">
      <div class="mq-status">
        <span class="mq-dot off" id="mqDot"></span>
        <span id="mqSt">Disconnected</span>
        <span class="mq-page-ind">
          <span class="mq-pi-dot vis" id="mqPDot"></span>
          <span id="mqPSt">visible</span>
        </span>
      </div>

      <div class="mq-sec">
        <div class="mq-sh" data-tog="mqCfg">▸ Configuration</div>
        <div class="mq-sb h" id="mqCfg">
          <label>API Base</label>
          <input id="mqBase" value="https://nxdev-org-mqtt-broker.hf.space"/>
          <label>API Key</label>
          <input id="mqKey" value="BROKER_APIKEY"/>
          <div class="mq-row">
            <div style="flex:1"><label>Username</label><input id="mqUsr" value="demo"/></div>
            <div style="flex:1"><label>Password</label><input id="mqPwd" type="password" value="demo"/></div>
          </div>
          <label>Poll Method</label>
          <select id="mqMtd"><option>GET</option><option>POST</option></select>
        </div>
      </div>

      <div class="mq-actions">
        <button class="mq-btn p" id="mqConn">🔗 Connect</button>
        <button class="mq-btn d" id="mqDisc" disabled>⛔ Disconnect</button>
        <button class="mq-btn w" id="mqWake" title="Request Wake Lock (prevents sleep)">🔒 Wake Lock</button>
      </div>

      <div class="mq-sec">
        <div class="mq-sh">Subscribe</div>
        <div class="mq-sb">
          <div class="mq-row">
            <input id="mqSTopic" placeholder="topic" value="test" style="flex:1"/>
            <button class="mq-btn" id="mqSub">Subscribe</button>
          </div>
          <div class="mq-tags" id="mqSubs"></div>
        </div>
      </div>

      <div class="mq-sec">
        <div class="mq-sh">Publish</div>
        <div class="mq-sb">
          <input id="mqPTopic" placeholder="topic" value="test"/>
          <input id="mqPData" placeholder='payload' value='{"hello":"world"}'/>
          <div class="mq-row">
            <button class="mq-btn p" id="mqPub">📤 Publish</button>
            <button class="mq-btn s" id="mqAuto">▶ Auto 1s</button>
            <button class="mq-btn d" id="mqStop" disabled>⏹ Stop</button>
          </div>
        </div>
      </div>

      <div class="mq-metrics" id="mqMet">
        <span>↓0</span><span>↑0</span><span>⚠0</span><span>↻0</span><span>⏱0s</span>
      </div>

      <div class="mq-sec">
        <div class="mq-sh" data-tog="mqBench">▸ Benchmark</div>
        <div class="mq-sb h" id="mqBench">
          <label>Test Type</label>
          <select id="mqBType">
            <option value="throughput">Throughput (sequential)</option>
            <option value="latency">Round-Trip Latency</option>
            <option value="burst">Burst (parallel)</option>
            <option value="reconnect">Reconnect Stress</option>
          </select>
          <div class="mq-row">
            <div style="flex:1"><label>Topic</label><input id="mqBTopic" value="bench/test"/></div>
            <div style="flex:0 0 80px"><label>Count</label><input id="mqBCount" type="number" value="50" min="1" max="1000"/></div>
          </div>
          <div class="mq-row">
            <button class="mq-btn p" id="mqBRun">▶ Run</button>
            <button class="mq-btn d" id="mqBStop" disabled>⏹ Stop</button>
          </div>
          <pre class="mq-bench-res" id="mqBRes">Connect first, then run benchmark.</pre>
        </div>
      </div>

      <div class="mq-sec">
        <div class="mq-sh">Log <button class="mq-bsm" id="mqClr">Clear</button></div>
        <div class="mq-sb">
          <div class="mq-log" id="mqLog"></div>
        </div>
      </div>
    </div>
  </div>`;
    document.body.appendChild(wrap);

    /* ── Refs ─────────────────────────────── */
    const $ = id => wrap.querySelector('#' + id);
    const panel = $('mqP'), body = $('mqBody'), dot = $('mqDot'), stText = $('mqSt'),
        pDot = $('mqPDot'), pSt = $('mqPSt'),
        baseIn = $('mqBase'), keyIn = $('mqKey'), usrIn = $('mqUsr'), pwdIn = $('mqPwd'), mtdIn = $('mqMtd'),
        connB = $('mqConn'), discB = $('mqDisc'), wakeB = $('mqWake'),
        sTopicIn = $('mqSTopic'), subB = $('mqSub'), subsEl = $('mqSubs'),
        pTopicIn = $('mqPTopic'), pDataIn = $('mqPData'),
        pubB = $('mqPub'), autoB = $('mqAuto'), stopB = $('mqStop'),
        metEl = $('mqMet'), logEl = $('mqLog'), clrB = $('mqClr'),
        bTypeIn = $('mqBType'), bTopicIn = $('mqBTopic'), bCountIn = $('mqBCount'),
        bRunB = $('mqBRun'), bStopB = $('mqBStop'), bResEl = $('mqBRes');

    /* ── State ────────────────────────────── */
    let mqtt = null, bench = null, unsubs = [], autoTimer = null, metricsTimer = null;
    const activeSubs = new Set();
    const MAX_LOG = 400;

    /* ── Helpers ──────────────────────────── */
    function log(type, text) {
        const cls = { message: 'msg', error: 'err', debug: 'dbg', info: 'inf', warn: 'wrn' }[type] || 'dbg';
        const pfx = { message: '◀', error: '✖', debug: '·', info: 'ℹ', warn: '⚠' }[type] || '·';
        const ts = new Date().toLocaleTimeString('en', { hour12: false });
        const div = document.createElement('div');
        div.className = 'mq-le ' + cls;
        div.textContent = `${ts} ${pfx} ${text}`;
        logEl.appendChild(div);
        while (logEl.children.length > MAX_LOG) logEl.firstChild.remove();
        logEl.scrollTop = logEl.scrollHeight;
    }

    function setStatus(state, detail = '') {
        dot.className = 'mq-dot ' + ({ connected: 'on', connecting: 'wait', disconnected: 'off' }[state] || 'off');
        const lab = { connected: '🟢 Connected', connecting: '🟡 Connecting…', disconnected: '🔴 Disconnected' };
        stText.textContent = (lab[state] || state) + (detail ? ` — ${detail}` : '');
        const on = state === 'connected';
        connB.disabled = on || state === 'connecting';
        discB.disabled = !on;
        subB.disabled = !on;
        pubB.disabled = !on;
        autoB.disabled = !on;
        bRunB.disabled = !on || (bench?.running ?? false);
    }

    function updatePageIndicator() {
        const online = navigator.onLine;
        const vis = document.visibilityState === 'visible';
        pDot.className = 'mq-pi-dot ' + (!online ? 'off' : vis ? 'vis' : 'hid');
        pSt.textContent = !online ? 'offline' : vis ? 'visible' : 'hidden';
    }

    function updateMetrics() {
        if (!mqtt) return;
        const s = mqtt.getStatus(), m = s.metrics;
        metEl.innerHTML =
            `<span>↓${m.msgIn}</span><span>↑${m.msgOut}</span>` +
            `<span>⚠${m.parseErrors + m.publishErrors}</span><span>↻${m.reconnects}</span>` +
            `<span>⏰${m.suspendResumes}</span>` +
            `<span>⏱${s.uptime}s</span>` +
            (s.lastDataAge != null ? `<span>📡${s.lastDataAge}s</span>` : '');
        updatePageIndicator();
    }

    function renderSubs() {
        subsEl.innerHTML = '';
        for (const t of activeSubs) {
            const tag = document.createElement('span');
            tag.className = 'mq-tag';
            tag.innerHTML = `${t} <button title="Remove">×</button>`;
            tag.querySelector('button').onclick = () => { activeSubs.delete(t); if (mqtt) mqtt.unsubscribe(t); renderSubs(); };
            subsEl.appendChild(tag);
        }
    }

    function stopAuto() {
        if (autoTimer) { clearInterval(autoTimer); autoTimer = null; }
        autoB.disabled = !mqtt?.clientId;
        stopB.disabled = true;
    }

    function wireEvents() {
        for (const u of unsubs) u();
        unsubs = [];
        unsubs.push(mqtt.on('connecting', () => { setStatus('connecting'); log('info', 'Connecting…'); }));
        unsubs.push(mqtt.on('connect', (id) => { setStatus('connected', id); log('info', `Connected: ${id}`); }));
        unsubs.push(mqtt.on('disconnected', (r) => { setStatus('disconnected', r); log('warn', `Disconnected: ${r}`); stopAuto(); }));
        unsubs.push(mqtt.on('error', (e) => log('error', e?.message || String(e))));
        unsubs.push(mqtt.on('debug', (m) => log('debug', m)));
        unsubs.push(mqtt.on('message', (t, p) => log('message', `${t}: ${typeof p === 'object' ? JSON.stringify(p) : p}`)));
        unsubs.push(mqtt.on('parse_error', (d, e) => log('error', `Parse: ${e} — ${d}`)));

        // Page lifecycle events in log
        unsubs.push(mqtt.on('resume', (info) => log('warn', `⏰ Resumed after ${(info.suspendedFor / 1000).toFixed(1)}s (${info.source})`)));
        unsubs.push(mqtt.on('page-hidden', () => log('debug', '👁 Page hidden')));
        unsubs.push(mqtt.on('page-visible', (info) => log('debug', `👁 Page visible (hidden ${(info.hiddenFor / 1000).toFixed(1)}s)`)));
        unsubs.push(mqtt.on('offline', () => log('warn', '📡 Network offline')));
        unsubs.push(mqtt.on('online', () => log('info', '📡 Network online')));
    }

    /* ── Drag ─────────────────────────────── */
    {
        let drag = false, dx = 0, dy = 0;
        $('mqHdr').addEventListener('mousedown', e => {
            if (e.target.tagName === 'BUTTON') return;
            drag = true; const r = panel.getBoundingClientRect();
            dx = e.clientX - r.left; dy = e.clientY - r.top; e.preventDefault();
        });
        document.addEventListener('mousemove', e => {
            if (!drag) return;
            panel.style.left = Math.max(0, e.clientX - dx) + 'px';
            panel.style.top = Math.max(0, e.clientY - dy) + 'px';
            panel.style.right = 'auto';
        });
        document.addEventListener('mouseup', () => drag = false);
    }

    /* ── Section toggles ──────────────────── */
    wrap.querySelectorAll('[data-tog]').forEach(el => {
        el.addEventListener('click', () => {
            const tgt = $(el.dataset.tog);
            tgt.classList.toggle('h');
            el.textContent = el.textContent.replace(/[▸▾]/, tgt.classList.contains('h') ? '▸' : '▾');
        });
    });

    /* ── Minimize ─────────────────────────── */
    $('mqMin').onclick = () => {
        const vis = body.style.display !== 'none';
        body.style.display = vis ? 'none' : '';
        $('mqMin').textContent = vis ? '+' : '−';
    };

    /* ── Connect ──────────────────────────── */
    connB.onclick = async () => {
        if (mqtt) mqtt.destroy();
        stopAuto();
        mqtt = new MqttSseClient(baseIn.value, {
            apiKey: keyIn.value, username: usrIn.value,
            password: pwdIn.value, pollMethod: mtdIn.value
        });
        wireEvents();
        bench = new Benchmark(mqtt);
        bench.onProgress(msg => { bResEl.textContent = msg; });
        if (metricsTimer) clearInterval(metricsTimer);
        metricsTimer = setInterval(updateMetrics, 1000);
        log('info', `→ ${baseIn.value}`);
        await mqtt.connect();
        // Track active subs for auto-resubscribe
        for (const t of activeSubs) {
            mqtt.subscribe(t).catch(e => log('error', `Sub ${t}: ${e.message}`));
        }
    };

    /* ── Disconnect ───────────────────────── */
    discB.onclick = () => {
        if (!mqtt) return;
        mqtt.disconnect();
        stopAuto();
        if (metricsTimer) { clearInterval(metricsTimer); metricsTimer = null; }
    };

    /* ── Wake Lock ────────────────────────── */
    wakeB.onclick = async () => {
        if (!mqtt?._lifecycle) { log('warn', 'Connect first'); return; }
        const ok = await mqtt._lifecycle.requestWakeLock();
        log(ok ? 'info' : 'warn', ok ? '🔒 Wake Lock acquired' : '🔒 Wake Lock not supported');
        if (ok) wakeB.textContent = '🔓 Locked';
    };

    /* ── Subscribe ────────────────────────── */
    subB.onclick = async () => {
        const t = sTopicIn.value.trim();
        if (!t || !mqtt) return;
        try { await mqtt.subscribe(t); activeSubs.add(t); renderSubs(); log('info', `Subscribed: ${t}`); }
        catch (e) { log('error', `Sub: ${e.message}`); }
    };
    sTopicIn.addEventListener('keydown', e => { if (e.key === 'Enter') subB.click(); });

    /* ── Publish ──────────────────────────── */
    async function doPublish() {
        if (!mqtt) return;
        const topic = pTopicIn.value.trim(), raw = pDataIn.value.trim();
        if (!topic) return;
        let payload; try { payload = JSON.parse(raw); } catch { payload = raw; }
        try { await mqtt.publish(topic, payload); log('info', `▶ ${topic}: ${raw}`); }
        catch (e) { log('error', `Pub: ${e.message}`); }
    }
    pubB.onclick = doPublish;
    pDataIn.addEventListener('keydown', e => { if (e.key === 'Enter') pubB.click(); });

    autoB.onclick = () => {
        if (autoTimer || !mqtt) return;
        let seq = 0;
        autoTimer = setInterval(async () => {
            if (!mqtt?.clientId) { stopAuto(); return; }
            const topic = pTopicIn.value.trim(); if (!topic) return;
            let p; try { p = JSON.parse(pDataIn.value.trim()); } catch { p = pDataIn.value.trim(); }
            if (typeof p === 'object' && p !== null) p._seq = seq;
            try { await mqtt.publish(topic, p); seq++; }
            catch (e) { log('error', `Auto #${seq}: ${e.message}`); }
        }, 1000);
        autoB.disabled = true; stopB.disabled = false;
        log('info', 'Auto-publish started');
    };
    stopB.onclick = () => { stopAuto(); log('info', 'Auto-publish stopped'); };

    /* ── Benchmark ────────────────────────── */
    bRunB.onclick = async () => {
        if (!bench || !mqtt?.clientId) { log('error', 'Connect first'); return; }
        if (bench.running) return;
        const type = bTypeIn.value, topic = bTopicIn.value.trim() || 'bench/test';
        const count = Math.max(1, Math.min(1000, parseInt(bCountIn.value) || 50));
        bRunB.disabled = true; bStopB.disabled = false;
        bResEl.textContent = `Starting ${type}…`;
        log('info', `🏁 Benchmark: ${type} × ${count}`);
        try {
            const r = await bench.run(type, topic, count);
            const t = formatResult(r);
            bResEl.textContent = t;
            log('info', `🏁 Done:\n${t}`);
        } catch (e) {
            bResEl.textContent = `ERROR: ${e.message}`;
            log('error', `Bench: ${e.message}`);
        } finally { bRunB.disabled = !mqtt?.clientId; bStopB.disabled = true; }
    };
    bStopB.onclick = () => { if (bench) bench.stop(); bStopB.disabled = true; bRunB.disabled = !mqtt?.clientId; };

    clrB.onclick = () => { logEl.innerHTML = ''; };

    // Page indicator updates
    document.addEventListener('visibilitychange', updatePageIndicator);
    window.addEventListener('online', updatePageIndicator);
    window.addEventListener('offline', updatePageIndicator);

    setStatus('disconnected');
    updatePageIndicator();
    log('info', 'UI ready. Configure and Connect.');
})();