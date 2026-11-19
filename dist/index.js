// index.ts
import { readFileSync as readFileSync2, writeFileSync as writeFileSync2, existsSync as existsSync2, readdirSync, lstatSync as lstatSync3, unlinkSync as unlinkSync2, chmodSync as chmodSync2 } from "node:fs";
import { resolve as resolve2, join, extname, isAbsolute as isAbsolute2 } from "node:path";
import { randomUUID as randomUUID2 } from "node:crypto";
import { fileURLToPath } from "node:url";

// src/safety.ts
import { openSync, closeSync, writeFileSync, fsyncSync, renameSync, unlinkSync, lstatSync, mkdirSync, chmodSync, realpathSync } from "node:fs";
import { dirname, resolve, relative, isAbsolute } from "node:path";
import { randomUUID } from "node:crypto";
import { root as openMediaRoot } from "openclaw/plugin-sdk/security-runtime";
import { fetchWithSsrFGuard } from "openclaw/plugin-sdk/ssrf-runtime";
var SPACE_RE = /^spaces\/[A-Za-z0-9_-]+$/;
var USER_RE = /^users\/[0-9]+$/;
var DEFAULT_MEDIA_BYTES = 20 * 1024 * 1024;
function privateDirectory(path) {
  mkdirSync(path, { recursive: true, mode: 448 });
  if (!lstatSync(path).isDirectory() || lstatSync(path).isSymbolicLink()) throw new Error("Unsafe state directory");
  chmodSync(path, 448);
}
function atomicJson(path, value) {
  const parent = dirname(path);
  const tmp = resolve(parent, `.${randomUUID()}.tmp`);
  let fd;
  try {
    fd = openSync(tmp, "wx", 384);
    writeFileSync(fd, JSON.stringify(value));
    fsyncSync(fd);
    closeSync(fd);
    fd = void 0;
    renameSync(tmp, path);
    const directory = openSync(parent, "r");
    try {
      fsyncSync(directory);
    } finally {
      closeSync(directory);
    }
  } finally {
    if (fd !== void 0) closeSync(fd);
    try {
      unlinkSync(tmp);
    } catch (e) {
      if (e.code !== "ENOENT") throw e;
    }
  }
}
var ApiError = class extends Error {
  code;
  constructor(operation, status) {
    super(`${operation} failed (${status})`);
    this.name = "ApiError";
    this.code = `HTTP_${status}`;
  }
};
function safeError(error) {
  const name = error instanceof Error ? error.name : "Error";
  const status = error?.response?.status;
  const code = Number.isInteger(status) && status >= 100 && status <= 599 ? `HTTP_${status}` : error?.code;
  return `${/^[A-Za-z0-9_]{1,60}$/.test(name) ? name : "Error"}${typeof code === "string" && /^[A-Z0-9_]{1,40}$/.test(code) ? ` (${code})` : ""}`;
}
async function readBoundedResponse(response, limit) {
  const length = response.headers.get("content-length");
  if (length && (!/^\d+$/.test(length) || Number(length) > limit)) {
    await response.body?.cancel();
    throw new Error("Response exceeds configured byte limit");
  }
  if (!response.body) return Buffer.alloc(0);
  const reader = response.body.getReader();
  const chunks = [];
  let size = 0;
  try {
    for (; ; ) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > limit) throw new Error("Response exceeds configured byte limit");
      chunks.push(Buffer.from(value));
    }
    return Buffer.concat(chunks, size);
  } finally {
    await reader.cancel().catch(() => {
    });
    reader.releaseLock();
  }
}
async function readMediaFile(path, roots, maxBytes) {
  for (const base of roots) {
    let rel = relative(resolve(base), resolve(path));
    if (rel === ".." || rel.startsWith("../") || isAbsolute(rel)) {
      rel = relative(realpathSync(base), resolve(path));
    }
    if (!rel || rel === ".." || rel.startsWith("../") || isAbsolute(rel)) continue;
    const directory = await openMediaRoot(resolve(base), { symlinks: "reject", hardlinks: "reject", maxBytes });
    return (await directory.read(rel)).buffer;
  }
  throw new Error("Media path is outside configured local roots");
}
async function fetchMedia(url, maxBytes, headers = {}, timeoutMs = 6e4) {
  const parsed = new URL(url);
  if (parsed.username || parsed.password) throw new Error("Media URL credentials are not allowed");
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let guarded;
  try {
    guarded = await fetchWithSsrFGuard({
      url,
      requireHttps: true,
      signal: controller.signal,
      maxRedirects: 3,
      mode: "strict",
      pinDns: true,
      capture: false,
      init: { headers }
    });
    if (!guarded.response.ok) throw new ApiError("Fetch media", guarded.response.status);
    return { buffer: await readBoundedResponse(guarded.response, maxBytes), headers: guarded.response.headers, url: guarded.finalUrl };
  } finally {
    clearTimeout(timer);
    await guarded?.release();
  }
}
function assertResource(space, value, kind) {
  if (value !== void 0 && !new RegExp(`^${space}/${kind}/[A-Za-z0-9_.-]+$`).test(value)) {
    throw new Error(`Invalid ${kind} resource for destination space`);
  }
}

// src/auth.ts
import { GoogleAuth } from "google-auth-library";
var AUTH_SCOPES = {
  chat: "https://www.googleapis.com/auth/chat.bot",
  events: "https://www.googleapis.com/auth/chat.app.messages.readonly",
  pubsub: "https://www.googleapis.com/auth/pubsub"
};
var ServiceAccountTokens = class {
  constructor(keyFile) {
    this.keyFile = keyFile;
  }
  keyFile;
  clients = /* @__PURE__ */ new Map();
  pending = /* @__PURE__ */ new Map();
  token(purpose) {
    const existing = this.pending.get(purpose);
    if (existing) return existing;
    let auth = this.clients.get(purpose);
    if (!auth) {
      auth = new GoogleAuth({
        keyFile: this.keyFile,
        scopes: [AUTH_SCOPES[purpose]],
        clientOptions: { transporterOptions: { timeout: 15e3, retryConfig: { retry: 2 } } }
      });
      this.clients.set(purpose, auth);
    }
    const pending = auth.getAccessToken().then((token) => {
      if (!token) throw new Error("Service account returned no access token");
      return token;
    }).finally(() => {
      this.pending.delete(purpose);
    });
    this.pending.set(purpose, pending);
    return pending;
  }
};

// src/inbox.ts
import { readFileSync, existsSync, lstatSync as lstatSync2 } from "node:fs";
import { acquireFileLock } from "openclaw/plugin-sdk/file-lock";
var DurableInbox = class {
  constructor(path, maxPending = 200, maxHistory = 2e3) {
    this.path = path;
    this.maxPending = maxPending;
    this.maxHistory = maxHistory;
    if (existsSync(path)) {
      const stat = lstatSync2(path);
      if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 128 * 1024 * 1024) throw new Error("Unsafe inbox file");
      const saved = JSON.parse(readFileSync(path, "utf8"));
      if (saved.version !== 1 || !Array.isArray(saved.records)) throw new Error("Unsupported inbox state; refusing to discard pending work");
      this.records = saved.records;
      const ids = /* @__PURE__ */ new Set();
      for (const r of this.records) {
        if (!r || typeof r.id !== "string" || !r.id || ids.has(r.id) || !["pending", "running", "failed", "done"].includes(r.state) || r.state !== "done" && (r.payload === void 0 || r.payload === null) || !Number.isInteger(r.attempts) || r.attempts < 0 || !Number.isFinite(r.nextAttempt) || !Number.isFinite(r.createdAt) || !Number.isFinite(r.updatedAt)) throw new Error("Invalid inbox record");
        ids.add(r.id);
        if (r.state === "running") {
          r.state = "failed";
          r.reason = "interrupted; delivery or tool side effects may have occurred";
        }
      }
      this.commit(this.records);
    } else {
      this.records = [];
    }
  }
  path;
  maxPending;
  maxHistory;
  records;
  commit(records) {
    atomicJson(this.path, { version: 1, records });
    this.records = records;
  }
  admit(jobs) {
    const next = structuredClone(this.records);
    const seen = new Set(next.map((r) => r.id));
    const added = [];
    for (const job of jobs) {
      if (seen.has(job.id)) continue;
      seen.add(job.id);
      added.push({ ...job, state: "pending", createdAt: Date.now(), updatedAt: Date.now(), attempts: 0, nextAttempt: 0 });
    }
    if (!added.length) return;
    if (next.filter((r) => r.state !== "done").length + added.length > this.maxPending) throw new Error("Inbox full; apply backpressure without ACK");
    const live = next.filter((r) => r.state !== "done");
    const done = next.filter((r) => r.state === "done").slice(-this.maxHistory);
    this.commit([...done, ...live, ...added]);
  }
  ready(now = Date.now()) {
    return structuredClone(this.records.find((r) => r.state === "pending" && r.nextAttempt <= now));
  }
  update(id, patch) {
    const next = structuredClone(this.records);
    const record = next.find((r) => r.id === id);
    if (!record) throw new Error("Unknown inbox record");
    Object.assign(record, patch, { updatedAt: Date.now() });
    if (record.state === "done") delete record.payload;
    this.commit(next);
  }
  snapshot() {
    return structuredClone(this.records);
  }
};
async function lockInbox(path) {
  const lock = await acquireFileLock(path, {
    retries: { retries: 0, factor: 1, minTimeout: 1, maxTimeout: 1 },
    stale: Number.MAX_SAFE_INTEGER,
    staleRecovery: "remove-if-unchanged"
  });
  return lock.release;
}

// src/subscriptions.ts
var MESSAGE_EVENT = "google.workspace.chat.message.v1.created";
var BASE = "https://workspaceevents.googleapis.com/v1/";
var validName = (name) => typeof name === "string" && /^subscriptions\/[A-Za-z0-9_-]+$/.test(name);
async function maintainSubscription(params) {
  const { space, topic, token, inspectionToken, request } = params;
  const now = params.now ?? Date.now() / 1e3;
  const headers = { Authorization: `Bearer ${token}` };
  const inspectHeaders = { Authorization: `Bearer ${inspectionToken}` };
  const matches = (sub2) => validName(sub2?.name) && sub2.targetResource === `//chat.googleapis.com/${space}` && sub2.notificationEndpoint?.pubsubTopic === topic && sub2.state !== "DELETED" && sub2.payloadOptions?.includeResource === true && !sub2.payloadOptions.fieldMask && sub2.eventTypes?.length === 1 && sub2.eventTypes[0] === MESSAGE_EVENT;
  async function operation(result) {
    if (result.status >= 400) throw new ApiError("Workspace Events request", result.status);
    let data = result.data;
    if (validName(data?.name)) return data;
    for (let i = 0; i < 30; i++) {
      if (data?.error) throw new Error("Workspace Events operation failed");
      if (data?.done) {
        if (!data.response) throw new Error("Workspace Events operation has no subscription");
        return data.response;
      }
      if (typeof data?.name !== "string" || !/^operations\/[A-Za-z0-9_-]+(?:\/[A-Za-z0-9_-]+)*$/.test(data.name)) throw new Error("Invalid Workspace Events operation");
      await new Promise((resolve3) => setTimeout(resolve3, 1e3));
      const polled = await request(BASE + data.name, { headers: inspectHeaders });
      if (polled.status >= 400) throw new ApiError("Workspace Events operation poll", polled.status);
      data = polled.data;
    }
    throw new Error("Workspace Events operation did not finish in time");
  }
  let sub;
  if (validName(params.current?.name)) {
    const result = await request(BASE + params.current.name, { headers: inspectHeaders });
    if (result.status >= 400 && result.status !== 404) throw new ApiError("Subscription inspection", result.status);
    if (result.status < 400 && matches(result.data)) sub = result.data;
  }
  async function discover() {
    let page = "";
    for (let i = 0; i < 20; i++) {
      const filter = encodeURIComponent(`event_types:"${MESSAGE_EVENT}" AND target_resource="//chat.googleapis.com/${space}"`);
      const result = await request(`${BASE}subscriptions?filter=${filter}&pageSize=100${page ? `&pageToken=${encodeURIComponent(page)}` : ""}`, { headers: inspectHeaders });
      if (result.status >= 400) throw new ApiError("Subscription discovery", result.status);
      const found = result.data?.subscriptions?.find(matches);
      if (found) return found;
      page = result.data?.nextPageToken || "";
      if (!page) return void 0;
    }
    throw new Error("Subscription discovery exceeded page limit");
  }
  if (!sub) sub = await discover();
  if (!sub) {
    const created = await request(BASE + "subscriptions", {
      method: "POST",
      headers,
      body: {
        targetResource: `//chat.googleapis.com/${space}`,
        eventTypes: [MESSAGE_EVENT],
        notificationEndpoint: { pubsubTopic: topic },
        payloadOptions: { includeResource: true },
        ttl: "14400s"
      }
    });
    sub = created.status === 409 ? await discover() : await operation(created);
    if (!sub) throw new Error("Conflicting subscription does not match configured target/topic");
  }
  if (!matches(sub)) throw new Error("Workspace subscription target/topic mismatch");
  if (sub.state === "SUSPENDED") {
    sub = await operation(await request(BASE + sub.name + ":reactivate", { method: "POST", headers }));
  }
  const expiry = Date.parse(sub.expireTime) / 1e3;
  if (!Number.isFinite(expiry) || expiry <= now + params.bufferSeconds) {
    sub = await operation(await request(BASE + sub.name + "?updateMask=ttl", {
      method: "PATCH",
      headers,
      body: { name: sub.name, ttl: "14400s" }
    }));
  }
  const expiresAt = Date.parse(sub?.expireTime) / 1e3;
  if (!matches(sub) || sub.state !== "ACTIVE" || !Number.isFinite(expiresAt) || expiresAt <= now) throw new Error("Workspace subscription is not active with a valid expiry");
  return { name: sub.name, expiresAt, space };
}

// src/progress.ts
function displayText(value) {
  if (typeof value !== "string") return;
  const text = value.replace(/-----BEGIN [\s\S]*?(?:-----END [^\n]+-----|$)/g, "[redacted]").replace(/\b(?:Bearer|Basic)\s+\S+/gi, "[redacted]").replace(/\b(?:password|passwd|secret|(?:access_|refresh_|api_)?token|api[_-]?key|authorization)\b["']?\s*[:=]\s*(?:"[^"]*"|'[^']*'|\S+)/gi, "[redacted]").replace(/\b(?:sk-[\w-]{16,}|gh[pousr]_[\w]{20,}|github_pat_[\w]{20,}|AIza[\w-]{20,}|ya29\.[\w.-]+)/g, "[redacted]").replace(/https?:\/\/\S+/gi, "[link]").replace(/<[^>]*>/g, "").replace(/[<>*_~`\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/g, " ").replace(/\s+/g, " ").trim();
  return text ? Array.from(text).slice(0, 160).join("") : void 0;
}
function toolLabel(name = "") {
  const short = name.split(/[.:/]/).pop().toLowerCase();
  if (["exec", "exec_command", "shell", "bash", "run_command"].includes(short)) return "Commands";
  const known = {
    memory_search: "Memory Search",
    memory_get: "Read Memory",
    read: "Read File",
    read_file: "Read File",
    write: "Write File",
    edit: "Edit File",
    apply_patch: "Edit File",
    web_search: "Web Search",
    web_fetch: "Fetch Page",
    tool_search: "Search Tools"
  };
  if (known[short]) return known[short];
  return /^[a-z][a-z0-9_-]{0,39}$/.test(short) ? short.replace(/[_-]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()) : "Other Tools";
}
var ActivityProgress = class {
  constructor(options) {
    this.options = options;
    if (options.signal?.aborted) this.stopped = true;
    else options.signal?.addEventListener("abort", this.abort, { once: true });
  }
  options;
  activities = /* @__PURE__ */ new Map();
  counts = /* @__PURE__ */ new Map();
  recent = [];
  sequence = 0;
  timer;
  inFlight;
  stopped = false;
  visible = false;
  lastText = "";
  lastEdit = Date.now();
  abort = () => {
    void this.stop();
  };
  onToolStart(event) {
    return this.record(event, false);
  }
  onItemEvent(event) {
    if (event.kind !== "tool" && event.kind !== "command") return false;
    return this.record(event, true);
  }
  record(event, item) {
    if (this.stopped) return false;
    const id = event.toolCallId || event.itemId?.replace(/^(?:tool|command):/, "");
    if (!id && (item || event.phase && event.phase !== "start")) return false;
    const key = id || `anonymous:${++this.sequence}`;
    let activity = this.activities.get(key);
    if (!activity) {
      if (this.activities.size >= 1e4) return false;
      let label = toolLabel(event.name || (event.kind === "command" ? "exec" : ""));
      if (!this.counts.has(label) && this.counts.size >= 8) label = "Other Tools";
      activity = { label, status: "running" };
      this.activities.set(key, activity);
      this.counts.set(label, (this.counts.get(label) || 0) + 1);
      this.recent.push(activity);
      if (this.recent.length > this.options.maxRecent) this.recent.shift();
    }
    if (!item && this.options.includeDetails) {
      const detail = displayText(event.args?.title) || (["Memory Search", "Web Search", "Search Tools"].includes(activity.label) ? displayText(event.args?.query) : void 0);
      if (detail) activity.detail = detail;
    }
    if (["failed", "error", "blocked", "cancelled", "canceled"].includes(event.status || "")) activity.status = "failed";
    else if (["completed", "complete", "done"].includes(event.status || "")) activity.status = "completed";
    this.schedule();
    return this.visible;
  }
  render() {
    const counts = [...this.counts].map(([label, count]) => `${label} \xD7${count}`).join(" \xB7 ");
    const lines = this.recent.map((a) => `${a.status === "completed" ? "\u2713" : a.status === "failed" ? "!" : "\u2026"} ${a.label}${a.detail ? ` \u2014 ${a.detail}` : ""}`);
    return ["_OpenClaw is working\u2026_", counts, ...lines].filter(Boolean).join("\n");
  }
  schedule() {
    if (this.stopped || this.timer || this.inFlight || this.lastText === this.render()) return;
    this.timer = setTimeout(() => {
      this.timer = void 0;
      if (this.stopped) return;
      const text = this.render();
      if (text === this.lastText) return;
      this.inFlight = (async () => {
        try {
          await this.options.update(text);
          this.lastText = text;
          this.visible = true;
        } catch (error) {
          this.stopped = true;
          this.options.onError(error);
        } finally {
          this.lastEdit = Date.now();
          this.inFlight = void 0;
          this.schedule();
        }
      })();
    }, Math.max(0, this.options.intervalMs - (Date.now() - this.lastEdit)));
    this.timer.unref();
  }
  async stop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = void 0;
    this.options.signal?.removeEventListener("abort", this.abort);
    await this.inFlight;
  }
};

// index.ts
import {
  resolveInboundRouteEnvelopeBuilderWithRuntime
} from "openclaw/plugin-sdk/inbound-envelope";
import {
  createReplyPrefixOptions
} from "openclaw/plugin-sdk/channel-outbound";
var config;
var serviceAccountFile;
var routingTable;
var targetSpaces;
var oauthCache = { token: null, expiresAt: 0 };
var processedMsgIds = /* @__PURE__ */ new Set();
var selfUserId = null;
var lastRenewalCheck = 0;
var subscriptionState = {};
var logger;
var pluginApi;
var pollTimer = null;
var authTokens;
var oauthRefresh;
var inbox;
var stopping = true;
var pollRunning = false;
var workerPromise;
var activeAbort;
var fatalInboxError = false;
var releaseInbox;
var MAX_DEDUP = 500;
function markProcessed(msgName) {
  if (!msgName) return;
  if (processedMsgIds.size > MAX_DEDUP) processedMsgIds.clear();
  processedMsgIds.add(msgName);
}
function isSelfMessage(sender) {
  return Boolean(selfUserId) && sender?.name === selfUserId;
}
function rememberSelfIdentity(created) {
  if (selfUserId) return;
  const name = created?.sender?.name;
  if (typeof name === "string" && name.startsWith("users/")) {
    selfUserId = name;
    logger?.info(`[self] Resolved own Chat identity: ${selfUserId}`);
  }
}
var RENEWAL_INTERVAL = 6e4;
var STATE_FILE_NAME = "gchat-pubsub-subscription-state.json";
var sessionBusy = /* @__PURE__ */ new Set();
var crossDispatchChains = /* @__PURE__ */ new Map();
var CROSS_DISPATCH_TTL_MS = 5 * 60 * 1e3;
function deriveSessionKey(agentId, space, threadName, threadSessionIsolation) {
  const threadId = threadName ? threadName.split("/").pop() : "";
  const peerId = threadSessionIsolation && threadId ? `${space}:thread:${threadId}` : space;
  return `agent:${agentId}:googlechatpubsub:group:${peerId}`.toLowerCase();
}
async function dispatchMessage(msg) {
  const sessionKey = deriveSessionKey(
    msg.agent.agentId,
    msg.space,
    msg.threadName,
    msg.threadSessionIsolation
  );
  if (sessionBusy.has(sessionKey)) throw new Error("Agent session is already busy");
  sessionBusy.add(sessionKey);
  try {
    await processMessageInPipeline({
      agentId: msg.agent.agentId,
      space: msg.space,
      spaceDisplayName: msg.spaceDisplayName,
      senderId: msg.senderId,
      senderName: msg.senderName,
      text: msg.text,
      messageName: msg.messageName,
      threadName: msg.threadName,
      eventTime: msg.eventTime,
      replyInThread: msg.replyInThread,
      threadSessionIsolation: msg.threadSessionIsolation,
      attachmentPaths: msg.attachmentPaths,
      crossDispatchChainKey: msg.crossDispatchChainKey
    });
    logger.info(`\u2705 [${msg.agent.agentId}] Pipeline complete for ${msg.space}`);
  } catch (err) {
    logger.error(`[${msg.agent.agentId}] Pipeline error: ${safeError(err)}`);
    throw err;
  } finally {
    sessionBusy.delete(sessionKey);
  }
}
async function crossAgentDispatch(params) {
  const {
    replyText,
    sourceAgentId,
    space,
    spaceDisplayName,
    senderName,
    messageName,
    threadName,
    replyInThread,
    threadSessionIsolation,
    chainKey
  } = params;
  const entry = routingTable.get(space);
  if (!entry || !entry.pattern) return;
  let chain = crossDispatchChains.get(chainKey);
  if (!chain) {
    chain = /* @__PURE__ */ new Set();
    crossDispatchChains.set(chainKey, chain);
    setTimeout(() => crossDispatchChains.delete(chainKey), CROSS_DISPATCH_TTL_MS).unref();
  }
  const matches = replyText.matchAll(new RegExp(entry.pattern.source, "gi"));
  const toDispatch = [];
  for (const m of matches) {
    const kw = m[1].toLowerCase();
    const agent = entry.keywordAgents.get(kw);
    if (!agent) continue;
    if (agent.agentId === sourceAgentId) continue;
    if (chain.has(agent.agentId)) continue;
    if (agent.alwaysListen) continue;
    toDispatch.push(agent);
    chain.add(agent.agentId);
    logger.info(`\u{1F500} Cross-dispatch: ${sourceAgentId} mentioned '${kw}' \u2192 dispatching to ${agent.agentId}`);
  }
  if (!toDispatch.length) return;
  chain.add(sourceAgentId);
  const contextText = `[via ${senderName}] ${replyText}`;
  const dispatchPromises = [];
  for (const agent of toDispatch) {
    logger.info(`\u{1F500} [${agent.agentId}] Cross-dispatch from ${sourceAgentId} in ${space}`);
    dispatchPromises.push(
      dispatchMessage({
        agent,
        space,
        spaceDisplayName,
        senderId: `bot:${sourceAgentId}`,
        senderName,
        text: contextText,
        messageName: messageName + ":xdispatch:" + agent.agentId,
        threadName,
        eventTime: (/* @__PURE__ */ new Date()).toISOString(),
        replyInThread,
        threadSessionIsolation,
        attachmentPaths: [],
        crossDispatchChainKey: chainKey
      })
    );
  }
  const outcomes = await Promise.allSettled(dispatchPromises);
  const failure = outcomes.find((outcome) => outcome.status === "rejected");
  if (failure) throw failure.reason;
}
async function httpJson(url, opts = {}) {
  const { method = "GET", headers = {}, body, timeoutMs = 15e3 } = opts;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method,
      headers: { "Content-Type": "application/json", ...headers },
      body: body === void 0 ? void 0 : JSON.stringify(body),
      signal: controller.signal,
      redirect: "error"
    });
    const text = (await readBoundedResponse(res, 2 * 1024 * 1024)).toString("utf8");
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = null;
    }
    return { status: res.status, data };
  } finally {
    clearTimeout(timer);
  }
}
async function httpForm(url, params) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15e3);
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams(params).toString(),
      signal: controller.signal,
      redirect: "error"
    });
    const data = JSON.parse((await readBoundedResponse(res, 64 * 1024)).toString("utf8"));
    if (!res.ok) throw new ApiError("OAuth token request", res.status);
    return data;
  } finally {
    clearTimeout(timer);
  }
}
function authMode() {
  return config.auth?.mode ?? (config.oauth ? "user-oauth" : "service-account");
}
async function getOAuthToken() {
  if (authMode() !== "user-oauth" || !config.oauth) throw new Error("Feature requires user-oauth authentication");
  if (oauthCache.token && Date.now() / 1e3 < oauthCache.expiresAt - 60) return oauthCache.token;
  if (oauthRefresh) return oauthRefresh;
  const oauth = config.oauth;
  oauthRefresh = (async () => {
    const st = lstatSync3(oauth.tokensFile);
    if (!st.isFile() || st.isSymbolicLink() || st.size > 64 * 1024) throw new Error("Unsafe OAuth token file");
    chmodSync2(oauth.tokensFile, 384);
    const tokens = JSON.parse(readFileSync2(oauth.tokensFile, "utf8"));
    if (typeof tokens.refresh_token !== "string" || !tokens.refresh_token) throw new Error("Missing OAuth refresh token");
    const result = await httpForm("https://oauth2.googleapis.com/token", {
      client_id: oauth.clientId,
      client_secret: oauth.clientSecret,
      refresh_token: tokens.refresh_token,
      grant_type: "refresh_token"
    });
    if (typeof result.access_token !== "string" || !result.access_token || !Number.isFinite(result.expires_in) || result.expires_in <= 0) throw new Error("Invalid OAuth access token response");
    if (typeof result.refresh_token === "string" && result.refresh_token) tokens.refresh_token = result.refresh_token;
    delete tokens.access_token;
    atomicJson(oauth.tokensFile, tokens);
    oauthCache = { token: result.access_token, expiresAt: Date.now() / 1e3 + result.expires_in };
    return result.access_token;
  })().finally(() => {
    oauthRefresh = void 0;
  });
  return oauthRefresh;
}
function serviceToken(purpose) {
  authTokens ??= new ServiceAccountTokens(serviceAccountFile);
  return authTokens.token(purpose);
}
async function getBotToken() {
  return serviceToken("chat");
}
async function getEventsToken() {
  return authMode() === "service-account" ? serviceToken("events") : getOAuthToken();
}
async function getPubSubToken() {
  return authMode() === "service-account" ? serviceToken("pubsub") : getOAuthToken();
}
function assertDestination(space, thread) {
  if (!SPACE_RE.test(space) || !config.bindings.some((b) => b.space === space)) throw new Error("Destination space is not configured");
  assertResource(space, thread || void 0, "threads");
}
function resolveReplyThread(space, thread) {
  assertDestination(space, thread);
  return config.bindings.find((binding) => binding.space === space)?.replyInThread ? thread : void 0;
}
function assertChatMessage(data, space) {
  if (typeof data?.name !== "string" || !data.name) throw new Error("Chat response has no message identity; delivery outcome uncertain");
  assertResource(space, data.name, "messages");
  return data.name;
}
function isSenderAllowed(space, sender) {
  const binding = config.bindings.find((b) => b.space === space);
  if (!binding || !USER_RE.test(sender)) return false;
  return (binding.allowAllSpaceMembers ?? config.allowAllSpaceMembers ?? false) || (binding.allowFrom ?? config.allowFrom ?? []).includes(sender);
}
function isCommandAuthorized(space, sender) {
  return isSenderAllowed(space, sender) && (config.bindings.find((b) => b.space === space)?.commandAllowFrom ?? config.commandAllowFrom ?? []).includes(sender);
}
function mediaLimit() {
  return config?.media?.maxBytes ?? DEFAULT_MEDIA_BYTES;
}
function requireOutboundMedia() {
  if (!config?.media?.outboundEnabled) throw new Error("Outbound media is disabled");
  if (authMode() !== "user-oauth") throw new Error("Outbound uploads require user-oauth authentication");
}
function requireReactions() {
  if (!config.reactionsEnabled || authMode() !== "user-oauth") throw new Error("Reactions require explicit enablement and user-oauth authentication");
}
function validateConfig() {
  for (const value of [config.projectId, config.topicId, config.subscriptionId]) {
    if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_.~+%-]{0,254}$/.test(value) || value.includes("%")) throw new Error("Invalid project/topic/subscription identifier");
  }
  if (!Array.isArray(config.bindings) || !config.bindings.length) throw new Error("At least one binding is required");
  const spaces = /* @__PURE__ */ new Set();
  for (const binding of config.bindings) {
    if (!SPACE_RE.test(binding.space) || spaces.has(binding.space)) throw new Error("Invalid or duplicate space binding");
    spaces.add(binding.space);
    if (!Array.isArray(binding.agents) || !binding.agents.length || binding.agents.length > 10 || new Set(binding.agents.map((a) => a.agentId)).size !== binding.agents.length || binding.agents.some((a) => !/^[a-zA-Z0-9_-]{1,80}$/.test(a.agentId))) throw new Error("Invalid agent binding");
    for (const list of [binding.allowFrom, binding.commandAllowFrom]) {
      if (list !== void 0 && (!Array.isArray(list) || list.some((v) => !USER_RE.test(v)))) throw new Error("Allowlists require stable numeric users/<id> identifiers");
    }
    if (!(binding.allowAllSpaceMembers ?? config.allowAllSpaceMembers) && !(binding.allowFrom ?? config.allowFrom)?.length) throw new Error("Configure allowFrom or explicitly allowAllSpaceMembers for every space");
  }
  if (config.media?.localRoots !== void 0 && (!Array.isArray(config.media.localRoots) || config.media.localRoots.some((root) => typeof root !== "string" || !isAbsolute2(root)))) throw new Error("Media roots must be absolute paths");
  for (const list of [config.allowFrom, config.commandAllowFrom]) {
    if (list !== void 0 && (!Array.isArray(list) || list.some((v) => !USER_RE.test(v)))) throw new Error("Allowlists require stable numeric users/<id> identifiers");
  }
  if (config.auth && !["service-account", "user-oauth"].includes(config.auth.mode)) throw new Error("Invalid authentication mode");
  if (authMode() === "user-oauth" && (!config.oauth?.clientId || !config.oauth.clientSecret || !config.oauth.tokensFile)) throw new Error("User OAuth configuration is incomplete");
  if (authMode() === "service-account" && (config.reactionsEnabled || config.media?.outboundEnabled)) throw new Error("App authentication does not support user-only reactions/uploads");
  for (const value of [config.progress?.enabled, config.progress?.includeDetails]) {
    if (value !== void 0 && typeof value !== "boolean") throw new Error("Invalid progress setting");
  }
  for (const [value, min, max] of [
    [config.pollIntervalSeconds ?? 3, 1, 300],
    [config.agentTimeoutSeconds ?? 60, 10, 3600],
    [config.maxPendingMessages ?? 200, 1, 1e3],
    [config.renewalBufferMinutes ?? 30, 1, 60],
    [mediaLimit(), 1, 200 * 1024 * 1024],
    [config.media?.maxAttachments ?? 5, 1, 10],
    [config.media?.retentionHours ?? 24, 1, 168],
    [config.progress?.updateIntervalSeconds ?? 5, 2, 30],
    [config.progress?.maxRecentActivities ?? 3, 1, 8]
  ]) {
    if (!Number.isInteger(value) || value < min || value > max) throw new Error("Invalid numeric limit in plugin configuration");
  }
}
function buildRoutingTable(bindings) {
  const table = /* @__PURE__ */ new Map();
  for (const binding of bindings) {
    const keywordAgents = /* @__PURE__ */ new Map();
    const alwaysListen = [];
    const keywords = [];
    for (const agent of binding.agents) {
      const kw = (agent.mentionKeyword || "").toLowerCase();
      if (kw) {
        keywordAgents.set(kw, agent);
        keywords.push(kw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
      }
      if (agent.alwaysListen) {
        alwaysListen.push(agent);
      }
    }
    let pattern = null;
    if (keywords.length) {
      pattern = new RegExp(
        `(?:^|[\\s@<])(${keywords.join("|")})(?=[\\s>,.:!?'")}]|$)`,
        "i"
      );
    }
    const replyInThread = binding.replyInThread ?? false;
    const threadSessionIsolation = binding.threadSessionIsolation ?? replyInThread;
    table.set(binding.space, { keywordAgents, alwaysListen, pattern, replyInThread, threadSessionIsolation });
  }
  return table;
}
function routeMessage(text, space) {
  const entry = routingTable.get(space);
  if (!entry) return [];
  const matched = [];
  const seen = /* @__PURE__ */ new Set();
  for (const agent of entry.alwaysListen) {
    if (!seen.has(agent.agentId)) {
      matched.push(agent);
      seen.add(agent.agentId);
    }
  }
  if (entry.pattern) {
    const matches = text.matchAll(new RegExp(entry.pattern.source, "gi"));
    for (const m of matches) {
      const kw = m[1].toLowerCase();
      const agent = entry.keywordAgents.get(kw);
      if (agent && !seen.has(agent.agentId)) {
        matched.push(agent);
        seen.add(agent.agentId);
        logger.info(`\u{1F3AF} Keyword '${kw}' \u2192 agent '${agent.agentId}'`);
      }
    }
  }
  if (matched.length) {
    logger.info(`\u{1F4E8} Routed to ${matched.length} agent(s): ${matched.map((a) => a.agentId).join(", ")}`);
  }
  return matched;
}
async function pullMessages(token) {
  const sub = `projects/${config.projectId}/subscriptions/${config.subscriptionId}`;
  const { status, data } = await httpJson(`https://pubsub.googleapis.com/v1/${sub}:pull`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` },
    body: { maxMessages: 10, returnImmediately: true }
  });
  if (status >= 400 || !data) throw new ApiError("Pub/Sub pull", status);
  return Array.isArray(data.receivedMessages) ? data.receivedMessages : [];
}
async function ackMessages(token, ackIds) {
  if (!ackIds.length) return;
  const sub = `projects/${config.projectId}/subscriptions/${config.subscriptionId}`;
  const { status } = await httpJson(`https://pubsub.googleapis.com/v1/${sub}:acknowledge`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` },
    body: { ackIds }
  });
  if (status >= 400) throw new ApiError("Pub/Sub acknowledgement", status);
}
function resolveStateDir() {
  const state = pluginApi?.runtime?.state?.resolveStateDir?.();
  if (!state) throw new Error("OpenClaw runtime state directory is unavailable");
  return state;
}
function pluginStateDir() {
  return join(resolveStateDir(), "googlechatpubsub");
}
function loadSubState() {
  const path = join(pluginStateDir(), STATE_FILE_NAME);
  if (!existsSync2(path)) return { subscriptions: {} };
  const stat = lstatSync3(path);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 1024 * 1024) throw new Error("Unsafe subscription state file");
  const data = JSON.parse(readFileSync2(path, "utf8"));
  if (!data || typeof data.subscriptions !== "object") throw new Error("Invalid subscription state");
  return data;
}
function saveSubState(state) {
  atomicJson(join(pluginStateDir(), STATE_FILE_NAME), state);
}
async function ensureSubscription(space, token) {
  const entry = await maintainSubscription({
    space,
    topic: `projects/${config.projectId}/topics/${config.topicId}`,
    current: subscriptionState.subscriptions?.[space],
    token,
    inspectionToken: authMode() === "service-account" ? await getBotToken() : token,
    bufferSeconds: (config.renewalBufferMinutes ?? 30) * 60,
    request: httpJson
  });
  subscriptionState.subscriptions ??= {};
  subscriptionState.subscriptions[space] = entry;
  saveSubState(subscriptionState);
}
async function checkAndRenewAll() {
  const token = await getEventsToken();
  for (const space of targetSpaces) await ensureSubscription(space, token);
}
function pruneInboundMedia() {
  const mediaDir = join(resolveStateDir(), "media", "inbound", "googlechatpubsub");
  if (!existsSync2(mediaDir)) return;
  const directory = lstatSync3(mediaDir);
  if (!directory.isDirectory() || directory.isSymbolicLink()) throw new Error("Unsafe media directory");
  const cutoff = Date.now() - (config.media?.retentionHours ?? 24) * 36e5;
  for (const name of readdirSync(mediaDir)) {
    if (!/^[0-9a-f-]{36}\.[A-Za-z0-9]{1,10}$/.test(name)) continue;
    const path = join(mediaDir, name);
    const st = lstatSync3(path);
    if (st.isFile() && !st.isSymbolicLink() && st.mtimeMs < cutoff) unlinkSync2(path);
  }
}
async function downloadAttachments(attachments, token) {
  if (!config.media?.inboundEnabled || !attachments?.length) return [];
  if (attachments.length > (config.media.maxAttachments ?? 5)) throw new Error("Too many inbound attachments");
  const mediaDir = join(resolveStateDir(), "media", "inbound", "googlechatpubsub");
  privateDirectory(mediaDir);
  pruneInboundMedia();
  const results = [];
  for (const att of attachments) {
    const resource = att.attachmentDataRef?.resourceName || att.name;
    if (typeof resource !== "string" || !/^[A-Za-z0-9_./=+-]+$/.test(resource) || resource.split("/").some((part) => part === "." || part === "..")) continue;
    let ext = extname(typeof att.contentName === "string" ? att.contentName : "");
    if (!/^\.[A-Za-z0-9]{1,10}$/.test(ext)) ext = ".bin";
    const filename = `${randomUUID2()}${ext}`;
    const localPath = join(mediaDir, filename);
    const url = `https://chat.googleapis.com/v1/media/${resource.split("/").map(encodeURIComponent).join("/")}?alt=media`;
    const downloaded = await fetchMedia(url, mediaLimit(), { Authorization: `Bearer ${token}` });
    writeFileSync2(localPath, downloaded.buffer, { flag: "wx", mode: 384 });
    results.push({ localPath, filename, mimeType: downloaded.headers.get("content-type") || "application/octet-stream" });
  }
  return results;
}
var MAX_UPLOAD_BYTES = 200 * 1024 * 1024;
var EXT_TO_MIME = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".svg": "image/svg+xml",
  ".bmp": "image/bmp",
  ".pdf": "application/pdf",
  ".txt": "text/plain",
  ".md": "text/markdown",
  ".csv": "text/csv",
  ".json": "application/json",
  ".zip": "application/zip",
  ".mp3": "audio/mpeg",
  ".ogg": "audio/ogg",
  ".wav": "audio/wav",
  ".mp4": "video/mp4",
  ".mov": "video/quicktime",
  ".webm": "video/webm",
  ".doc": "application/msword",
  ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  ".xls": "application/vnd.ms-excel",
  ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  ".ppt": "application/vnd.ms-powerpoint",
  ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation"
};
function guessMimeType(filename) {
  return EXT_TO_MIME[extname(filename).toLowerCase()] || "application/octet-stream";
}
async function resolveOutboundMedia(ref, opts = {}) {
  requireOutboundMedia();
  const trimmed = String(ref || "").trim();
  if (!trimmed) throw new Error("Empty media reference");
  if (/^https?:\/\//i.test(trimmed)) {
    if (!config.media?.allowRemoteUrls) throw new Error("Remote media URLs are disabled");
    const result = await fetchMedia(trimmed, mediaLimit());
    let filename = "";
    const disposition = result.headers.get("content-disposition") || "";
    const match = /filename\*?=(?:UTF-8'')?"?([^";]+)"?/i.exec(disposition);
    if (match) {
      try {
        filename = decodeURIComponent(match[1]);
      } catch {
      }
    }
    if (!filename) {
      try {
        filename = decodeURIComponent(new URL(result.url).pathname.split("/").pop() || "");
      } catch {
      }
    }
    filename = filename.replace(/[\\/\r\n\x00]/g, "_").slice(0, 200) || "attachment";
    const headerMime = (result.headers.get("content-type") || "").split(";")[0].trim();
    const mimeType = /^[A-Za-z0-9!#$&^_.+-]+\/[A-Za-z0-9!#$&^_.+-]+$/.test(headerMime) ? headerMime : guessMimeType(filename);
    if (!extname(filename)) {
      const ext = Object.entries(EXT_TO_MIME).find(([, mime]) => mime === mimeType)?.[0];
      if (ext) filename += ext;
    }
    return { buffer: result.buffer, filename, mimeType };
  }
  const localPath = trimmed.startsWith("file://") ? fileURLToPath(trimmed) : resolve2(trimmed);
  const buffer = opts.readFile ? await opts.readFile(localPath) : await readMediaFile(localPath, config.media?.localRoots ?? [], mediaLimit());
  if (buffer.length > mediaLimit()) throw new Error("Media exceeds configured byte limit");
  return { buffer, filename: localPath.split("/").pop() || "attachment", mimeType: guessMimeType(localPath) };
}
async function uploadAttachment(params) {
  const { space, media, token, timeoutMs = 12e4 } = params;
  requireOutboundMedia();
  assertDestination(space);
  if (media.buffer.length > mediaLimit()) throw new Error("Media exceeds configured byte limit");
  if (!/^[A-Za-z0-9!#$&^_.+-]+\/[A-Za-z0-9!#$&^_.+-]+$/.test(media.mimeType)) throw new Error("Invalid media content type");
  if (media.buffer.length === 0) {
    throw new Error(`Refusing to upload empty file: ${media.filename}`);
  }
  if (media.buffer.length > MAX_UPLOAD_BYTES) {
    throw new Error(
      `File ${media.filename} is ${(media.buffer.length / 1024 / 1024).toFixed(1)} MB, over the Google Chat 200 MB limit`
    );
  }
  const boundary = `openclaw-${randomUUID2()}`;
  const head = Buffer.from(
    `--${boundary}\r
Content-Type: application/json; charset=UTF-8\r
\r
${JSON.stringify({ filename: media.filename })}\r
--${boundary}\r
Content-Type: ${media.mimeType}\r
\r
`,
    "utf8"
  );
  const tail = Buffer.from(`\r
--${boundary}--\r
`, "utf8");
  const body = Buffer.concat([head, media.buffer, tail]);
  const url = `https://chat.googleapis.com/upload/v1/${space}/attachments:upload?uploadType=multipart`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let resp;
  let text;
  try {
    resp = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": `multipart/related; boundary=${boundary}`
      },
      body,
      signal: controller.signal,
      redirect: "error"
    });
    text = (await readBoundedResponse(resp, 64 * 1024)).toString("utf8");
  } finally {
    clearTimeout(timer);
  }
  if (!resp.ok) {
    throw new Error(`Attachment upload failed (${resp.status}): request rejected`);
  }
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`Attachment upload returned non-JSON: invalid response`);
  }
  const uploadToken = data?.attachmentDataRef?.attachmentUploadToken;
  if (!uploadToken) {
    throw new Error(`Attachment upload missing attachmentUploadToken: invalid response`);
  }
  logger.info(
    `[upload] Uploaded ${media.buffer.length} bytes`
  );
  return uploadToken;
}
async function sendMediaMessages(params) {
  const {
    space,
    mediaRefs,
    token,
    text,
    threadName,
    replyMessageOption = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD",
    readFile
  } = params;
  requireOutboundMedia();
  assertDestination(space, threadName);
  if (mediaRefs.length > (config.media?.maxAttachments ?? 5)) throw new Error("Too many outbound attachments");
  const failed = [];
  const messageIds = [];
  let sent = 0;
  let captionPending = text?.trim() || "";
  for (const ref of mediaRefs) {
    try {
      const media = await resolveOutboundMedia(ref, { readFile });
      const uploadToken = await uploadAttachment({ space, media, token });
      const msgBody = {
        attachment: [{ attachmentDataRef: { attachmentUploadToken: uploadToken } }]
      };
      if (captionPending) {
        msgBody.text = captionPending;
      }
      let url = `https://chat.googleapis.com/v1/${space}/messages`;
      if (threadName) {
        msgBody.thread = { name: threadName };
        url += `?messageReplyOption=${replyMessageOption}`;
      }
      const { status, data } = await httpJson(url, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
        body: msgBody,
        timeoutMs: 6e4
      });
      if (status >= 400) {
        throw new Error(`Message create failed (${status}): request rejected`);
      }
      captionPending = "";
      assertChatMessage(data, space);
      sent++;
      if (data?.name) messageIds.push(data.name);
      markProcessed(data?.name);
      rememberSelfIdentity(data);
      logger.info(`[upload] Sent attachment to ${space}`);
    } catch (err) {
      logger.error(`[upload] Failed: ${safeError(err)}`);
      failed.push(`Attachment delivery failed: ${safeError(err)}`);
    }
  }
  if (captionPending && sent === 0) {
    const msgBody = { text: captionPending };
    let url = `https://chat.googleapis.com/v1/${space}/messages`;
    if (threadName) {
      msgBody.thread = { name: threadName };
      url += `?messageReplyOption=${replyMessageOption}`;
    }
    await httpJson(url, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      body: msgBody
    }).then(({ status, data }) => {
      if (status >= 400) return;
      assertChatMessage(data, space);
      markProcessed(data?.name);
      rememberSelfIdentity(data);
    }).catch(() => {
    });
  }
  return { sent, failed, messageIds };
}
async function processMessageInPipeline(params) {
  const {
    agentId,
    space,
    spaceDisplayName,
    senderId,
    senderName,
    text,
    messageName,
    threadName,
    eventTime,
    replyInThread,
    threadSessionIsolation,
    attachmentPaths = [],
    crossDispatchChainKey
  } = params;
  const chainKey = crossDispatchChainKey || `${threadName || space}:${messageName}:${Date.now()}`;
  const deliveredChunks = [];
  assertDestination(space, threadName || void 0);
  const api = pluginApi;
  const cfg = api.config;
  const runtime = api.runtime;
  const effectiveThreadId = threadName || "";
  const peerId = threadSessionIsolation && effectiveThreadId ? `${space}:thread:${effectiveThreadId.split("/").pop()}` : space;
  const { route, buildEnvelope } = resolveInboundRouteEnvelopeBuilderWithRuntime({
    cfg,
    channel: "googlechatpubsub",
    accountId: agentId,
    peer: {
      kind: "group",
      id: peerId
    },
    runtime: runtime.channel,
    sessionStore: cfg.session?.store
  });
  logger.info(`\u{1F511} Session key: ${route.sessionKey} (agent=${agentId}, threadIsolation=${threadSessionIsolation}, thread=${effectiveThreadId || "none"})`);
  const fromLabel = spaceDisplayName || `space:${space}`;
  const { storePath, body } = buildEnvelope({
    channel: "Google Chat",
    from: fromLabel,
    timestamp: eventTime ? Date.parse(eventTime) : void 0,
    body: text
  });
  const ctxPayload = runtime.channel.reply.finalizeInboundContext({
    Body: body,
    BodyForAgent: text,
    RawBody: text,
    CommandBody: text,
    From: `googlechatpubsub:${senderId}`,
    To: `googlechatpubsub:${space}`,
    SessionKey: route.sessionKey,
    AccountId: agentId,
    ChatType: "channel",
    ConversationLabel: fromLabel,
    SenderName: senderName || void 0,
    SenderId: senderId,
    WasMentioned: false,
    CommandAuthorized: isCommandAuthorized(space, senderId),
    Provider: "googlechatpubsub",
    Surface: "googlechatpubsub",
    MessageSid: messageName,
    MessageSidFull: messageName,
    ReplyToId: replyInThread ? threadName || void 0 : void 0,
    ReplyToIdFull: replyInThread ? threadName || void 0 : void 0,
    GroupSpace: spaceDisplayName || void 0,
    OriginatingChannel: "googlechatpubsub",
    OriginatingTo: `googlechatpubsub:${space}`,
    ...attachmentPaths.length > 0 && {
      MediaPaths: attachmentPaths,
      // local file paths → sets path: value in normalizeAttachments
      MediaUrls: attachmentPaths
      // also set for compat/dedup logic
    }
  });
  void runtime.channel.session.recordSessionMetaFromInbound({
    storePath,
    sessionKey: ctxPayload.SessionKey ?? route.sessionKey,
    ctx: ctxPayload
  }).catch((err) => {
    logger.error(
      `googlechatpubsub: failed updating session meta: ${safeError(err)}`
    );
  });
  let replyThreadName = replyInThread ? threadName : "";
  let replyMessageOption = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD";
  if (replyInThread) {
    if (!threadName) {
      replyThreadName = messageName ? `${space}/threads/${messageName.split("/").pop()}` : "";
      replyMessageOption = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD";
      logger.info(`\u{1F9F5} replyInThread: creating new thread on message ${messageName}`);
    } else {
      replyMessageOption = "REPLY_MESSAGE_OR_FAIL";
      logger.info(`\u{1F9F5} replyInThread: continuing in existing thread ${threadName}`);
    }
  }
  const progressEnabled = config.progress?.enabled === true;
  const silentReply = config.silentReply !== false;
  let typingMessageName;
  if ((progressEnabled || !silentReply) && !activeAbort?.signal.aborted) {
    try {
      const botToken = await getBotToken();
      const typingBody = { text: progressEnabled ? "_OpenClaw is working\u2026_" : "_typing..._" };
      if (replyThreadName) typingBody.thread = { name: replyThreadName };
      const typingUrl = replyThreadName ? `https://chat.googleapis.com/v1/${space}/messages?messageReplyOption=${replyMessageOption}` : `https://chat.googleapis.com/v1/${space}/messages`;
      logger.info(`\u23F3 Sending typing indicator to ${space} (thread: ${replyThreadName || "none"}, replyInThread: ${replyInThread})`);
      const result = await httpJson(typingUrl, {
        method: "POST",
        headers: { Authorization: `Bearer ${botToken}` },
        body: typingBody
      });
      logger.info(`\u23F3 Typing indicator result: status=${result.status} name=${result.data?.name || "none"}`);
      if (result.status < 400 && result.data?.name) {
        assertChatMessage(result.data, space);
        typingMessageName = result.data.name;
        markProcessed(typingMessageName);
        if (replyInThread && result.data?.thread?.name && !replyThreadName) {
          replyThreadName = result.data.thread.name;
        }
      } else {
        logger.warn(`Typing indicator failed: ${result.status} request rejected`);
      }
    } catch (err) {
      logger.warn(`Typing indicator exception: ${safeError(err)}`);
    }
  } else {
    logger.info(`\u{1F507} silentReply=true \u2014 skipping typing indicator`);
  }
  const progress = progressEnabled && typingMessageName ? new ActivityProgress({
    intervalMs: (config.progress?.updateIntervalSeconds ?? 5) * 1e3,
    maxRecent: config.progress?.maxRecentActivities ?? 3,
    includeDetails: config.progress?.includeDetails !== false,
    signal: activeAbort?.signal,
    update: async (text2) => {
      const token = await getBotToken();
      if (activeAbort?.signal.aborted) throw new Error("Progress cancelled");
      const result = await httpJson(`https://chat.googleapis.com/v1/${typingMessageName}?updateMask=text`, {
        method: "PATCH",
        headers: { Authorization: `Bearer ${token}` },
        body: { text: text2 }
      });
      if (result.status >= 400) throw new ApiError("Chat progress", result.status);
      assertChatMessage(result.data, space);
    },
    onError: (error) => logger.warn(`Activity updates stopped: ${safeError(error)}`)
  }) : void 0;
  const { onModelSelected, ...prefixOptions } = createReplyPrefixOptions({
    cfg,
    agentId: route.agentId,
    channel: "googlechatpubsub",
    accountId: agentId
  });
  let pipelineError;
  try {
    await runtime.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
      ctx: ctxPayload,
      cfg,
      dispatcherOptions: {
        ...prefixOptions,
        deliver: async (payload, info) => {
          logger.debug(`Reply delivery: kind=${info?.kind || "unknown"}`);
          if (activeAbort?.signal.aborted) throw new Error("Reply cancelled");
          assertDestination(space, threadName || void 0);
          const botToken = await getBotToken();
          const replyText = payload.text?.trim();
          const mediaRefs = [
            ...payload.mediaUrls || [],
            ...payload.mediaUrl ? [payload.mediaUrl] : []
          ].filter((m) => Boolean(m && String(m).trim()));
          const uniqueMediaRefs = [...new Set(mediaRefs)];
          if (uniqueMediaRefs.length > 0) {
            await progress?.stop();
            if (activeAbort?.signal.aborted) throw new Error("Reply cancelled");
            requireOutboundMedia();
            const effectiveThread = replyThreadName || void 0;
            if (typingMessageName) {
              await httpJson(`https://chat.googleapis.com/v1/${typingMessageName}`, {
                method: "DELETE",
                headers: { Authorization: `Bearer ${botToken}` }
              }).catch(() => {
              });
              typingMessageName = void 0;
            }
            if (replyText) deliveredChunks.push(replyText);
            const result = await sendMediaMessages({
              space,
              mediaRefs: uniqueMediaRefs,
              token: await getOAuthToken(),
              text: replyText,
              threadName: effectiveThread,
              replyMessageOption
            });
            if (result.failed.length) throw new Error("One or more media deliveries failed");
            logger.info(
              `\u{1F4CE} Media delivery: ${result.sent}/${uniqueMediaRefs.length} sent` + (result.failed.length ? ` | failures: ${result.failed.join("; ").slice(0, 300)}` : "")
            );
            return;
          }
          if (!replyText) return;
          await progress?.stop();
          if (activeAbort?.signal.aborted) throw new Error("Reply cancelled");
          deliveredChunks.push(replyText);
          const chunkLimit = 4e3;
          const chunks = [];
          if (replyText.length <= chunkLimit) {
            chunks.push(replyText);
          } else {
            let remaining = replyText;
            while (remaining.length > 0) {
              if (remaining.length <= chunkLimit) {
                chunks.push(remaining);
                break;
              }
              let cut = remaining.lastIndexOf("\n", chunkLimit);
              if (cut <= 0) cut = chunkLimit;
              chunks.push(remaining.slice(0, cut));
              remaining = remaining.slice(cut).trimStart();
            }
          }
          for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];
            try {
              if (i === 0 && typingMessageName) {
                logger.info(`\u{1F4DD} PATCHing typing message: ${typingMessageName}`);
                const patchResult = await httpJson(
                  `https://chat.googleapis.com/v1/${typingMessageName}?updateMask=text`,
                  {
                    method: "PATCH",
                    headers: { Authorization: `Bearer ${botToken}` },
                    body: { text: chunk }
                  }
                );
                if (patchResult.status >= 400) {
                  logger.warn(`PATCH failed (${patchResult.status}), falling back to POST: request rejected`);
                  const msgBody = { text: chunk };
                  let url = `https://chat.googleapis.com/v1/${space}/messages`;
                  const effectiveThread = replyThreadName || void 0;
                  if (effectiveThread) {
                    msgBody.thread = { name: effectiveThread };
                    url += `?messageReplyOption=${replyMessageOption}`;
                  }
                  const postResult = await httpJson(url, {
                    method: "POST",
                    headers: { Authorization: `Bearer ${botToken}` },
                    body: msgBody
                  });
                  if (postResult.status >= 400) throw new ApiError("Chat reply", postResult.status);
                  assertChatMessage(postResult.data, space);
                  logger.info(`POST fallback result: ${postResult.status}`);
                  await httpJson(`https://chat.googleapis.com/v1/${typingMessageName}`, {
                    method: "DELETE",
                    headers: { Authorization: `Bearer ${botToken}` }
                  }).catch(() => {
                  });
                } else {
                  assertChatMessage(patchResult.data, space);
                  logger.info(`\u2705 PATCH succeeded`);
                }
                typingMessageName = void 0;
              } else {
                const msgBody = { text: chunk };
                let url = `https://chat.googleapis.com/v1/${space}/messages`;
                const effectiveThread = replyThreadName || void 0;
                if (effectiveThread) {
                  msgBody.thread = { name: effectiveThread };
                  url += `?messageReplyOption=${replyMessageOption}`;
                }
                const postResult = await httpJson(url, {
                  method: "POST",
                  headers: { Authorization: `Bearer ${botToken}` },
                  body: msgBody
                });
                if (postResult.status >= 400) throw new ApiError("Chat reply", postResult.status);
                assertChatMessage(postResult.data, space);
                logger.info(`\u{1F4E8} POST result: ${postResult.status}`);
              }
            } catch (err) {
              logger.error(`Chat API reply failed: ${safeError(err)}`);
              pipelineError = new Error("Chat API reply failed");
              throw pipelineError;
            }
          }
        },
        onSkip: (payload, info) => {
          logger.debug(`Reply skipped: kind=${info?.kind || "unknown"}`);
        },
        onHeartbeatStrip: () => {
          logger.info(`\u{1F493} Heartbeat strip triggered`);
        },
        onError: (err, info) => {
          pipelineError = new Error("Agent or reply delivery failed");
          logger.error(
            `googlechatpubsub reply ${info?.kind || "?"} failed: ${safeError(err)}`
          );
        }
      },
      replyOptions: {
        onModelSelected,
        ...progressEnabled ? {
          suppressDefaultToolProgressMessages: true,
          suppressToolProgressMessages: true,
          allowToolLifecycleWhenProgressHidden: true,
          commentaryProgressEnabled: false,
          progressPreambleEnabled: false,
          commentaryPayloadsEnabled: false,
          reasoningPayloadsEnabled: false,
          onToolStart: (event) => progress?.onToolStart(event) ?? false,
          onItemEvent: (event) => progress?.onItemEvent(event) ?? false
        } : {},
        abortSignal: activeAbort?.signal,
        onAgentRunTerminalOutcome: (outcome) => {
          if (outcome === "failed") pipelineError = new Error("Agent run failed");
        }
      }
    });
  } finally {
    await progress?.stop();
    if (typingMessageName) {
      logger.info(`\u{1F9F9} Cleaning up orphaned typing message: ${typingMessageName}`);
      try {
        const cleanupToken = await getBotToken();
        await httpJson(`https://chat.googleapis.com/v1/${typingMessageName}`, {
          method: "DELETE",
          headers: { Authorization: `Bearer ${cleanupToken}` }
        });
        logger.info(`\u2705 Orphaned typing message deleted`);
      } catch (err) {
        logger.error(`Failed to delete orphaned typing message: ${safeError(err)}`);
      }
    }
  }
  if (pipelineError) throw pipelineError;
  if (config.crossAgentDispatch && deliveredChunks.length > 0) {
    const fullReply = deliveredChunks.join("\n");
    try {
      await crossAgentDispatch({
        replyText: fullReply,
        sourceAgentId: agentId,
        space,
        spaceDisplayName,
        senderName: `${agentId} agent`,
        messageName,
        threadName: threadName || "",
        replyInThread,
        threadSessionIsolation,
        chainKey
      });
    } catch (err) {
      logger.error(`Cross-agent dispatch error: ${safeError(err)}`);
      throw err;
    }
  }
}
function admittedJobs(received) {
  if (received.message?.attributes?.["ce-type"] !== MESSAGE_EVENT) return [];
  const raw = received.message?.data;
  if (typeof raw !== "string" || raw.length > 9e4) return [];
  let data;
  try {
    data = JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
  } catch {
    return [];
  }
  const message = data.message;
  if (!message || message.sender?.type !== "HUMAN" || isSelfMessage(message.sender)) return [];
  const space = message.space?.name;
  const sender = message.sender?.name;
  if (typeof space !== "string" || typeof sender !== "string" || !isSenderAllowed(space, sender)) return [];
  const messageName = message.name;
  if (typeof messageName !== "string" || !messageName) return [];
  const threadName = message.thread?.name || "";
  try {
    assertResource(space, messageName, "messages");
    assertResource(space, threadName || void 0, "threads");
  } catch {
    return [];
  }
  if (processedMsgIds.has(messageName)) return [];
  const text = typeof message.text === "string" ? message.text.trim() : "";
  if (Buffer.byteLength(text) > 32 * 1024) return [];
  const rawAttachments = config.media?.inboundEnabled ? message.attachment || message.attachments || [] : [];
  if (!Array.isArray(rawAttachments) || rawAttachments.length > (config.media?.maxAttachments ?? 5)) return [];
  const attachments = rawAttachments.map((att) => ({
    name: typeof att?.name === "string" ? att.name.slice(0, 2048) : void 0,
    contentName: typeof att?.contentName === "string" ? att.contentName.slice(0, 200) : void 0,
    attachmentDataRef: { resourceName: typeof att?.attachmentDataRef?.resourceName === "string" ? att.attachmentDataRef.resourceName.slice(0, 2048) : void 0 }
  }));
  if (!text && !attachments.length) return [];
  const entry = routingTable.get(space);
  return (text ? routeMessage(text, space) : entry.alwaysListen).map((agent) => ({
    agent,
    space,
    spaceDisplayName: space,
    senderId: sender,
    senderName: typeof message.sender.displayName === "string" ? message.sender.displayName.slice(0, 200) : sender,
    text,
    messageName,
    threadName,
    eventTime: message.createTime || (/* @__PURE__ */ new Date()).toISOString(),
    replyInThread: entry.replyInThread,
    threadSessionIsolation: entry.threadSessionIsolation,
    attachmentPaths: [],
    rawAttachments: attachments
  }));
}
function startInboxWorker() {
  if (workerPromise || stopping || !inbox || fatalInboxError) return;
  workerPromise = (async () => {
    while (!stopping && !fatalInboxError) {
      const record = inbox.ready();
      if (!record?.payload) return;
      const msg = record.payload;
      let dispatched = false;
      const attempts = record.attempts + 1;
      try {
        const current = config.bindings.find((b) => b.space === msg.space);
        if (!isSenderAllowed(msg.space, msg.senderId) || !current?.agents.some((a) => a.agentId === msg.agent.agentId)) {
          inbox.update(record.id, { state: "done", reason: "admission revoked" });
          continue;
        }
        msg.replyInThread = current.replyInThread ?? false;
        msg.threadSessionIsolation = current.threadSessionIsolation ?? msg.replyInThread;
        inbox.update(record.id, { attempts });
        if (msg.rawAttachments?.length && config.media?.inboundEnabled) {
          const token = authMode() === "service-account" ? await getBotToken() : await getOAuthToken();
          msg.attachmentPaths = (await downloadAttachments(msg.rawAttachments, token)).map((item) => item.localPath);
        }
        inbox.update(record.id, { state: "running" });
        dispatched = true;
        const controller = new AbortController();
        activeAbort = controller;
        const timer = setTimeout(() => {
          controller.abort();
          logger.warn("Agent timeout: cancellation requested; waiting for the active run to settle");
        }, (config.agentTimeoutSeconds ?? 60) * 1e3);
        try {
          await dispatchMessage(msg);
          if (controller.signal.aborted) throw new Error("Agent run cancelled; inspect possible side effects");
        } finally {
          clearTimeout(timer);
          activeAbort = void 0;
        }
        inbox.update(record.id, { state: "done" });
        markProcessed(msg.messageName);
      } catch (error) {
        logger.error(`Inbox processing failed: ${safeError(error)}`);
        try {
          inbox.update(record.id, dispatched || attempts >= 3 ? { state: "failed", reason: dispatched ? "agent/delivery outcome uncertain; operator review required" : "pre-dispatch retries exhausted" } : { state: "pending", nextAttempt: Date.now() + Math.min(6e4, 1e3 * 2 ** attempts), reason: "pre-dispatch retry" });
        } catch (storageError) {
          fatalInboxError = true;
          logger.error(`Inbox persistence failed; listener paused: ${safeError(storageError)}`);
        }
      }
    }
  })().catch((error) => {
    fatalInboxError = true;
    logger.error(`Inbox worker stopped: ${safeError(error)}`);
  }).finally(() => {
    workerPromise = void 0;
  });
}
async function pollOnce() {
  if (pollRunning || stopping || fatalInboxError) return;
  pollRunning = true;
  try {
    if (Date.now() - lastRenewalCheck > RENEWAL_INTERVAL) {
      lastRenewalCheck = Date.now();
      try {
        pruneInboundMedia();
        await checkAndRenewAll();
      } catch (error) {
        logger.error(`Subscription maintenance failed: ${safeError(error)}`);
      }
    }
    const token = await getPubSubToken();
    const received = await pullMessages(token);
    for (const message of received) {
      if (stopping) break;
      if (typeof message.ackId !== "string" || !message.ackId) continue;
      const jobs = admittedJobs(message);
      inbox.admit(jobs.map((payload) => ({ id: `${payload.messageName}:${payload.agent.agentId}`, payload })));
      await ackMessages(token, [message.ackId]);
    }
  } catch (error) {
    logger.error(`Pub/Sub poll failed: ${safeError(error)}`);
  } finally {
    pollRunning = false;
    startInboxWorker();
  }
}
function register(api) {
  logger = api.logger ?? console;
  pluginApi = api;
  api.registerChannel({
    id: "googlechatpubsub",
    meta: {
      id: "googlechatpubsub",
      label: "Google Chat (Pub/Sub)",
      selectionLabel: "Google Chat Pub/Sub (no-mention listening)",
      docsPath: "/channels/googlechatpubsub",
      blurb: "Listen to Google Chat spaces via Workspace Events + Pub/Sub. No @mention required.",
      aliases: ["gchatpubsub", "gchat-pubsub"]
    },
    capabilities: { chatTypes: ["group"], reactions: true, media: true },
    describeMessageTool: () => ({
      actions: ["send", "react", "reactions", "upload-file"],
      capabilities: null,
      schema: null
    }),
    messaging: {
      targetResolver: {
        hint: "spaces/<SPACE_ID>",
        looksLikeId: (raw) => /^spaces\/[a-zA-Z0-9_-]+$/.test(raw.trim()),
        resolveTarget: async ({ normalized }) => {
          const to = normalized?.trim();
          if (!to || !/^spaces\/[a-zA-Z0-9_-]+$/.test(to)) return null;
          return { to, kind: "group", source: "normalized" };
        }
      }
    },
    config: {
      listAccountIds: () => ["default"],
      resolveAccount: (cfg) => {
        const pluginCfg = cfg.channels?.googlechatpubsub || cfg.plugins?.entries?.googlechatpubsub?.config || {};
        return { accountId: "default", ...pluginCfg };
      }
    },
    handleAction: async (ctx) => {
      const { action, params } = ctx;
      try {
        if (action === "react" || action === "reactions") {
          requireReactions();
          const message = params.messageId || params.message_id || params.target;
          const space = typeof message === "string" ? message.split("/").slice(0, 2).join("/") : "";
          assertDestination(space);
          assertResource(space, message, "messages");
        } else if (action === "send" || action === "upload-file") {
          assertDestination(params.target, params.threadId || params.replyTo);
          if (action === "upload-file") requireOutboundMedia();
        }
      } catch {
        return { ok: false, error: "Action denied by channel policy" };
      }
      if (action === "react") {
        const messageName = params.messageId || params.message_id || params.target;
        const emoji = params.emoji || "\u{1F440}";
        if (!messageName) {
          return { ok: false, error: "messageId (Chat message name) is required for react" };
        }
        try {
          const token = await getOAuthToken();
          const url = `https://chat.googleapis.com/v1/${messageName}/reactions`;
          const { status, data } = await httpJson(url, {
            method: "POST",
            headers: { Authorization: `Bearer ${token}` },
            body: {
              emoji: { unicode: emoji }
            }
          });
          if (status >= 400) {
            return { ok: false, error: `Chat API react failed (${status}): request rejected` };
          }
          return { ok: true, added: emoji };
        } catch (e) {
          return { ok: false, error: `React failed: ${safeError(e)}` };
        }
      }
      if (action === "reactions") {
        const messageName = params.messageId || params.message_id || params.target;
        if (!messageName) {
          return { ok: false, error: "messageId (Chat message name) is required for reactions" };
        }
        try {
          const token = await getOAuthToken();
          const url = `https://chat.googleapis.com/v1/${messageName}/reactions`;
          const { status, data } = await httpJson(url, {
            method: "GET",
            headers: { Authorization: `Bearer ${token}` }
          });
          if (status >= 400) return { ok: false, error: `Chat API reactions failed (${status})` };
          return { ok: true, reactions: data?.reactions || [] };
        } catch (e) {
          return { ok: false, error: `List reactions failed: ${safeError(e)}` };
        }
      }
      if (action === "send") {
        const text = params.message || params.text;
        const target = params.target;
        if (!text || !target) {
          return { ok: false, error: "message and target are required for send" };
        }
        try {
          const token = await getBotToken();
          const space = target;
          const replyToThread = resolveReplyThread(space, params.threadId || params.replyTo);
          const binding = config?.bindings?.find((b) => b.space === space);
          const bindingReplyInThread = binding?.replyInThread ?? false;
          const msgBody = { text };
          let url = `https://chat.googleapis.com/v1/${space}/messages`;
          if (replyToThread) {
            msgBody.thread = { name: replyToThread };
            url += `?messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD`;
          } else if (bindingReplyInThread) {
            logger.warn(`[handleAction.send] replyInThread=true for ${space} but no threadId provided \u2014 landing in main chat`);
          }
          const { status, data } = await httpJson(url, {
            method: "POST",
            headers: { Authorization: `Bearer ${token}` },
            body: msgBody
          });
          if (status >= 400) throw new ApiError("Chat send", status);
          const messageId = assertChatMessage(data, space);
          markProcessed(messageId);
          return { ok: true, messageId };
        } catch (e) {
          return { ok: false, error: `Send failed: ${safeError(e)}` };
        }
      }
      if (action === "upload-file") {
        const target = params.target;
        if (!target) {
          return { ok: false, error: "target (spaces/<SPACE_ID>) is required for upload-file" };
        }
        const mediaRefs = [
          ...Array.isArray(params.media) ? params.media : params.media ? [params.media] : [],
          ...params.path ? [params.path] : [],
          ...Array.isArray(params.paths) ? params.paths : [],
          ...Array.isArray(params.attachments) ? params.attachments.map((a) => a?.media).filter(Boolean) : []
        ].filter((m) => Boolean(m && String(m).trim()));
        if (mediaRefs.length === 0) {
          return { ok: false, error: "media (path or URL) is required for upload-file" };
        }
        try {
          const token = await getOAuthToken();
          const binding = config?.bindings?.find((b) => b.space === target);
          const replyToThread = resolveReplyThread(target, params.threadId || params.replyTo);
          const threadName = replyToThread ? String(replyToThread) : void 0;
          if (binding?.replyInThread && !replyToThread) {
            logger.warn(
              `[handleAction.upload-file] replyInThread=true for ${target} but no threadId provided \u2014 landing in main chat`
            );
          }
          const result = await sendMediaMessages({
            space: target,
            mediaRefs: [...new Set(mediaRefs)],
            token,
            text: params.message || params.text || params.caption,
            threadName
          });
          if (result.sent === 0) {
            return {
              ok: false,
              error: `All uploads failed: ${result.failed.join("; ").slice(0, 500)}`
            };
          }
          return {
            ok: true,
            sent: result.sent,
            messageIds: result.messageIds,
            ...result.failed.length ? { partialFailures: result.failed } : {}
          };
        } catch (e) {
          return { ok: false, error: `Upload failed: ${safeError(e)}` };
        }
      }
      return { ok: false, error: `Unsupported action: ${action}` };
    },
    outbound: {
      deliveryMode: "direct",
      sendText: async ({ text, target, threadId, replyTo }) => {
        try {
          const space = target || config?.bindings?.[0]?.space;
          if (!space) return { ok: false };
          assertDestination(space, threadId || replyTo);
          const token = await getBotToken();
          const replyToThread = resolveReplyThread(space, threadId || replyTo);
          const binding = config?.bindings?.find((b) => b.space === space);
          const bindingReplyInThread = binding?.replyInThread ?? false;
          const msgBody = { text };
          let url = `https://chat.googleapis.com/v1/${space}/messages`;
          if (replyToThread) {
            msgBody.thread = { name: replyToThread };
            url += `?messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD`;
          } else if (bindingReplyInThread) {
            logger.warn(`[outbound.sendText] replyInThread=true for ${space} but no threadId \u2014 landing in main chat`);
          }
          const { status, data } = await httpJson(url, {
            method: "POST",
            headers: { Authorization: `Bearer ${token}` },
            body: msgBody
          });
          if (status >= 400) throw new ApiError("Chat send", status);
          const messageId = assertChatMessage(data, space);
          markProcessed(messageId);
          return { ok: true, messageId, target: space };
        } catch (e) {
          logger.error(`outbound sendText error: ${safeError(e)}`);
          return { ok: false };
        }
      },
      sendMedia: async ({ text, target, threadId, replyTo, mediaUrl, mediaReadFile }) => {
        try {
          requireOutboundMedia();
          const space = target || config?.bindings?.[0]?.space;
          if (!space) return { ok: false };
          assertDestination(space, threadId || replyTo);
          const token = await getOAuthToken();
          if (!mediaUrl) return { ok: false };
          const replyToThread = resolveReplyThread(space, threadId || replyTo);
          const binding = config?.bindings?.find((b) => b.space === space);
          if (binding?.replyInThread && !replyToThread) {
            logger.warn(
              `[outbound.sendMedia] replyInThread=true for ${space} but no threadId \u2014 landing in main chat`
            );
          }
          const result = await sendMediaMessages({
            space,
            mediaRefs: [String(mediaUrl)],
            token,
            text,
            threadName: replyToThread ? String(replyToThread) : void 0,
            readFile: mediaReadFile
          });
          return {
            ok: result.sent > 0,
            messageId: result.messageIds[0],
            target: space
          };
        } catch (e) {
          logger.error(`outbound sendMedia error: ${safeError(e)}`);
          return { ok: false };
        }
      }
    }
  });
  api.registerService({
    id: "googlechatpubsub-listener",
    start: async () => {
      if (releaseInbox) throw new Error("Pub/Sub listener is already started");
      const cfg = api.config;
      const selected = cfg.channels?.googlechatpubsub || cfg.plugins?.entries?.googlechatpubsub?.config;
      if (!selected?.enabled) return;
      config = selected;
      serviceAccountFile = config.serviceAccountFile || cfg.channels?.googlechat?.serviceAccountFile || "";
      if (!serviceAccountFile) throw new Error("serviceAccountFile is required");
      const credentials = lstatSync3(serviceAccountFile);
      if (!credentials.isFile() || credentials.isSymbolicLink() || credentials.size > 64 * 1024) throw new Error("Unsafe service account file");
      if (process.platform !== "win32" && credentials.mode & 63) throw new Error("Service account file must have mode 0600");
      const key = JSON.parse(readFileSync2(serviceAccountFile, "utf8"));
      if (key.type !== "service_account" || !key.client_email || !key.private_key) throw new Error("Expected service_account credentials without user impersonation");
      validateConfig();
      privateDirectory(pluginStateDir());
      releaseInbox = await lockInbox(join(pluginStateDir(), "inbox.json"));
      try {
        authTokens = new ServiceAccountTokens(serviceAccountFile);
        oauthCache = { token: null, expiresAt: 0 };
        selfUserId = null;
        processedMsgIds.clear();
        routingTable = buildRoutingTable(config.bindings);
        targetSpaces = new Set(config.bindings.map((b) => b.space));
        subscriptionState = loadSubState();
        inbox = new DurableInbox(join(pluginStateDir(), "inbox.json"), config.maxPendingMessages ?? 200);
        fatalInboxError = false;
        await getPubSubToken();
        await getBotToken();
        await checkAndRenewAll();
        pruneInboundMedia();
        lastRenewalCheck = Date.now();
        stopping = false;
        const run = async () => {
          await pollOnce();
          if (!stopping) pollTimer = setTimeout(run, (config.pollIntervalSeconds ?? 3) * 1e3);
        };
        pollTimer = setTimeout(run, 0);
        const failed = inbox.snapshot().filter((item) => item.state === "failed").length;
        logger.info(`Pub/Sub listener started (${authMode()}); ${failed} inbox record(s) require operator review`);
      } catch (error) {
        stopping = true;
        await releaseInbox();
        releaseInbox = void 0;
        throw new Error(`Pub/Sub initialization failed: ${safeError(error)}`);
      }
    },
    stop: async () => {
      stopping = true;
      if (pollTimer) {
        clearTimeout(pollTimer);
        pollTimer = null;
      }
      activeAbort?.abort();
      while (pollRunning) await new Promise((resolve3) => setTimeout(resolve3, 50));
      await workerPromise;
      await releaseInbox?.();
      releaseInbox = void 0;
      logger.info("Pub/Sub listener stopped");
    }
  });
}
export {
  register as default
};
