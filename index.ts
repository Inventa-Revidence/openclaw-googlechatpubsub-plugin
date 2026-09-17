/**
 * googlechatpubsub — OpenClaw channel plugin
 *
 * Listens to Google Chat spaces via Workspace Events API + Cloud Pub/Sub.
 * Routes messages to agents by keyword or alwaysListen rules.
 * Processes messages IN-PROCESS via the OpenClaw pipeline (proper sessions).
 * Replies via Google Chat API using service account credentials.
 *
 * No @mention required. Messages arrive via Pub/Sub, not the Chat webhook.
 */

import { readFileSync, writeFileSync, existsSync, readdirSync, lstatSync, unlinkSync, chmodSync } from "node:fs";
import { resolve, join, extname, isAbsolute } from "node:path";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { atomicJson, privateDirectory, safeError, ApiError, readBoundedResponse, readMediaFile, fetchMedia, assertResource, SPACE_RE, USER_RE, DEFAULT_MEDIA_BYTES } from "./src/safety.js";
import { ServiceAccountTokens } from "./src/auth.js";
import { DurableInbox, lockInbox } from "./src/inbox.js";
import { maintainSubscription, MESSAGE_EVENT } from "./src/subscriptions.js";
import {
  resolveInboundRouteEnvelopeBuilderWithRuntime,
} from "openclaw/plugin-sdk/inbound-envelope";
import {
  createReplyPrefixOptions,
} from "openclaw/plugin-sdk/channel-outbound";

// ── Types ────────────────────────────────────────────────────────────────────

interface AgentBinding {
  agentId: string;
  mentionKeyword?: string;
  alwaysListen?: boolean;
}

interface SpaceBinding {
  space: string;
  replyInThread?: boolean;
  threadSessionIsolation?: boolean;
  agents: AgentBinding[];
  allowFrom?: string[];
  allowAllSpaceMembers?: boolean;
  commandAllowFrom?: string[];
}

interface PubSubConfig {
  enabled?: boolean;
  projectId: string;
  topicId: string;
  subscriptionId: string;
  pollIntervalSeconds?: number;
  renewalBufferMinutes?: number;
  serviceAccountFile?: string;
  auth?: { mode: "service-account" | "user-oauth" };
  allowFrom?: string[];
  allowAllSpaceMembers?: boolean;
  commandAllowFrom?: string[];
  reactionsEnabled?: boolean;
  agentTimeoutSeconds?: number;
  maxPendingMessages?: number;
  media?: {
    inboundEnabled?: boolean;
    outboundEnabled?: boolean;
    allowRemoteUrls?: boolean;
    localRoots?: string[];
    maxBytes?: number;
    maxAttachments?: number;
    retentionHours?: number;
  };
  oauth?: {
    clientId: string;
    clientSecret: string;
    redirectUri?: string;
    tokensFile: string;
  };
  bindings: SpaceBinding[];
  crossAgentDispatch?: boolean;
  silentReply?: boolean;
}

interface RoutingEntry {
  keywordAgents: Map<string, AgentBinding>;
  alwaysListen: AgentBinding[];
  pattern: RegExp | null;
  replyInThread: boolean;
  threadSessionIsolation: boolean;
}

interface TokenCache {
  token: string | null;
  expiresAt: number;
}

// ── State (module-scoped) ────────────────────────────────────────────────────

let config: PubSubConfig;
let serviceAccountFile: string;
let routingTable: Map<string, RoutingEntry>;
let targetSpaces: Set<string>;
let oauthCache: TokenCache = { token: null, expiresAt: 0 };
let processedMsgIds = new Set<string>();
/**
 * Chat member name (`users/<id>`) of the OAuth identity this plugin sends as.
 *
 * `attachments:upload` only accepts USER auth, so attachment messages the bot
 * posts are recorded by Google with `sender.type === "HUMAN"` and this id — they
 * are indistinguishable from a real person by type alone, and would otherwise be
 * re-ingested as fresh inbound messages (self-echo feedback loop).
 *
 * Latched opportunistically from the `sender.name` of messages this plugin
 * creates; stays null until the first successful send resolves it.
 */
let selfUserId: string | null = null;
let lastRenewalCheck = 0;
let subscriptionState: Record<string, any> = {};
let logger: any;
let pluginApi: any; // Store the full api object for runtime access
let pollTimer: ReturnType<typeof setTimeout> | null = null;
let authTokens: ServiceAccountTokens | undefined;
let oauthRefresh: Promise<string> | undefined;
let inbox: DurableInbox<QueuedMessage> | undefined;
let stopping = true;
let pollRunning = false;
let workerPromise: Promise<void> | undefined;
let activeAbort: AbortController | undefined;
let fatalInboxError = false;
let releaseInbox: (() => Promise<void>) | undefined;

const MAX_DEDUP = 500;

/**
 * Record a message id as already handled.
 *
 * Used for inbound dedup and, critically, for every message this plugin sends:
 * outbound ids are registered immediately so an echoed copy arriving over
 * Pub/Sub is dropped even before `selfUserId` has been resolved.
 */
function markProcessed(msgName?: string | null): void {
  if (!msgName) return;
  if (processedMsgIds.size > MAX_DEDUP) processedMsgIds.clear();
  processedMsgIds.add(msgName);
}

/**
 * True when an inbound message was posted by this plugin itself.
 *
 * Only meaningful once `selfUserId` is known; until then this is a no-op and
 * the outbound dedup in `markProcessed()` is what prevents the echo.
 */
function isSelfMessage(sender: any): boolean {
  return Boolean(selfUserId) && sender?.name === selfUserId;
}

/**
 * Latch this plugin's own Chat identity from a message it just created.
 * Idempotent and best-effort: the first send that reports a sender wins.
 */
function rememberSelfIdentity(created: any): void {
  if (selfUserId) return;
  const name = created?.sender?.name;
  if (typeof name === "string" && name.startsWith("users/")) {
    selfUserId = name;
    logger?.info(`[self] Resolved own Chat identity: ${selfUserId}`);
  }
}
const RENEWAL_INTERVAL = 60_000; // Inspect server state once per minute
const STATE_FILE_NAME = "gchat-pubsub-subscription-state.json";

// ── Per-session message queue (prevents drops when session is busy) ──────────

interface QueuedMessage {
  agent: AgentBinding;
  space: string;
  spaceDisplayName: string;
  senderId: string;
  senderName: string;
  text: string;
  messageName: string;
  threadName: string;
  eventTime: string;
  replyInThread: boolean;
  threadSessionIsolation: boolean;
  attachmentPaths: string[];
  crossDispatchChainKey?: string;
  rawAttachments?: any[];
}

/** Sessions currently running a pipeline dispatch */
const sessionBusy = new Set<string>();

/** Queued messages waiting for a busy session to finish */

/** Cross-agent dispatch chain tracker — prevents infinite loops */
const crossDispatchChains = new Map<string, Set<string>>();
const CROSS_DISPATCH_TTL_MS = 5 * 60 * 1000; // clean up chains after 5 min

/**
 * Derive the session key that processMessageInPipeline would use.
 * Must mirror the logic in processMessageInPipeline's resolveInboundRoute call.
 */
function deriveSessionKey(agentId: string, space: string, threadName: string, threadSessionIsolation: boolean): string {
  const threadId = threadName ? threadName.split("/").pop() : "";
  const peerId = threadSessionIsolation && threadId
    ? `${space}:thread:${threadId}`
    : space;
  return `agent:${agentId}:googlechatpubsub:group:${peerId}`.toLowerCase();
}

/** The durable worker owns admission; never complete work by enqueueing it in memory. */
async function dispatchMessage(msg: QueuedMessage): Promise<void> {
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
      crossDispatchChainKey: msg.crossDispatchChainKey,
    });
    logger.info(`✅ [${msg.agent.agentId}] Pipeline complete for ${msg.space}`);
  } catch (err: any) {
    logger.error(`[${msg.agent.agentId}] Pipeline error: ${safeError(err)}`);
    throw err;
  } finally {
    sessionBusy.delete(sessionKey);
  }

}

/**
 * Cross-agent dispatch: when an agent's reply mentions other agents' keywords,
 * internally route to those agents so they see the message.
 * Only runs when config.crossAgentDispatch === true.
 * Prevents infinite loops via a per-chain dispatch tracker.
 */
async function crossAgentDispatch(params: {
  replyText: string;
  sourceAgentId: string;
  space: string;
  spaceDisplayName: string;
  senderName: string;
  messageName: string;
  threadName: string;
  replyInThread: boolean;
  threadSessionIsolation: boolean;
  chainKey: string;
}): Promise<void> {
  const {
    replyText, sourceAgentId, space, spaceDisplayName,
    senderName, messageName, threadName,
    replyInThread, threadSessionIsolation, chainKey,
  } = params;

  const entry = routingTable.get(space);
  if (!entry || !entry.pattern) return;

  let chain = crossDispatchChains.get(chainKey);
  if (!chain) {
    chain = new Set<string>();
    crossDispatchChains.set(chainKey, chain);
    setTimeout(() => crossDispatchChains.delete(chainKey), CROSS_DISPATCH_TTL_MS).unref();
  }

  const matches = replyText.matchAll(new RegExp(entry.pattern.source, "gi"));
  const toDispatch: AgentBinding[] = [];

  for (const m of matches) {
    const kw = m[1].toLowerCase();
    const agent = entry.keywordAgents.get(kw);
    if (!agent) continue;
    if (agent.agentId === sourceAgentId) continue;
    if (chain.has(agent.agentId)) continue;
    if (agent.alwaysListen) continue;
    toDispatch.push(agent);
    chain.add(agent.agentId);
    logger.info(`🔀 Cross-dispatch: ${sourceAgentId} mentioned '${kw}' → dispatching to ${agent.agentId}`);
  }

  if (!toDispatch.length) return;

  chain.add(sourceAgentId);

  const contextText = `[via ${senderName}] ${replyText}`;

  const dispatchPromises: Promise<void>[] = [];
  for (const agent of toDispatch) {
    logger.info(`🔀 [${agent.agentId}] Cross-dispatch from ${sourceAgentId} in ${space}`);
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
        eventTime: new Date().toISOString(),
        replyInThread,
        threadSessionIsolation,
        attachmentPaths: [],
        crossDispatchChainKey: chainKey,
      })
    );
  }
  // A rejection must not release ownership while another agent is still running.
  const outcomes = await Promise.allSettled(dispatchPromises);
  const failure = outcomes.find((outcome): outcome is PromiseRejectedResult => outcome.status === "rejected");
  if (failure) throw failure.reason;
}

// ── HTTP helper (stdlib only) ────────────────────────────────────────────────

async function httpJson(
  url: string,
  opts: { method?: string; headers?: Record<string, string>; body?: any; timeoutMs?: number } = {}
): Promise<{ status: number; data: any }> {
  const { method = "GET", headers = {}, body, timeoutMs = 15000 } = opts;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method, headers: { "Content-Type": "application/json", ...headers },
      body: body === undefined ? undefined : JSON.stringify(body), signal: controller.signal,
      redirect: "error",
    });
    const text = (await readBoundedResponse(res, 2 * 1024 * 1024)).toString("utf8");
    let data: any;
    try { data = JSON.parse(text); } catch { data = null; }
    return { status: res.status, data };
  } finally { clearTimeout(timer); }
}

async function httpForm(url: string, params: Record<string, string>): Promise<any> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15_000);
  try {
    const res = await fetch(url, {
      method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams(params).toString(), signal: controller.signal, redirect: "error",
    });
    const data = JSON.parse((await readBoundedResponse(res, 64 * 1024)).toString("utf8"));
    if (!res.ok) throw new ApiError("OAuth token request", res.status);
    return data;
  } finally { clearTimeout(timer); }
}

function authMode(): "service-account" | "user-oauth" {
  return config.auth?.mode ?? (config.oauth ? "user-oauth" : "service-account");
}

async function getOAuthToken(): Promise<string> {
  if (authMode() !== "user-oauth" || !config.oauth) throw new Error("Feature requires user-oauth authentication");
  if (oauthCache.token && Date.now() / 1000 < oauthCache.expiresAt - 60) return oauthCache.token;
  if (oauthRefresh) return oauthRefresh;
  const oauth = config.oauth;
  oauthRefresh = (async () => {
    const st = lstatSync(oauth.tokensFile);
    if (!st.isFile() || st.isSymbolicLink() || st.size > 64 * 1024) throw new Error("Unsafe OAuth token file");
    chmodSync(oauth.tokensFile, 0o600);
    const tokens = JSON.parse(readFileSync(oauth.tokensFile, "utf8"));
    if (typeof tokens.refresh_token !== "string" || !tokens.refresh_token) throw new Error("Missing OAuth refresh token");
    const result = await httpForm("https://oauth2.googleapis.com/token", {
      client_id: oauth.clientId, client_secret: oauth.clientSecret,
      refresh_token: tokens.refresh_token, grant_type: "refresh_token",
    });
    if (typeof result.access_token !== "string" || !result.access_token
      || !Number.isFinite(result.expires_in) || result.expires_in <= 0) throw new Error("Invalid OAuth access token response");
    // Refresh tokens can rotate. Access tokens remain in memory only.
    if (typeof result.refresh_token === "string" && result.refresh_token) tokens.refresh_token = result.refresh_token;
    delete tokens.access_token;
    atomicJson(oauth.tokensFile, tokens);
    oauthCache = { token: result.access_token, expiresAt: Date.now() / 1000 + result.expires_in };
    return result.access_token as string;
  })().finally(() => { oauthRefresh = undefined; });
  return oauthRefresh;
}

function serviceToken(purpose: "chat" | "events" | "pubsub"): Promise<string> {
  authTokens ??= new ServiceAccountTokens(serviceAccountFile);
  return authTokens.token(purpose);
}
async function getBotToken(): Promise<string> { return serviceToken("chat"); }
async function getEventsToken(): Promise<string> { return authMode() === "service-account" ? serviceToken("events") : getOAuthToken(); }
async function getPubSubToken(): Promise<string> { return authMode() === "service-account" ? serviceToken("pubsub") : getOAuthToken(); }

function assertDestination(space: string, thread?: string): void {
  if (!SPACE_RE.test(space) || !config.bindings.some(b => b.space === space)) throw new Error("Destination space is not configured");
  assertResource(space, thread || undefined, "threads");
}
function assertChatMessage(data: any, space: string): string {
  if (typeof data?.name !== "string" || !data.name) throw new Error("Chat response has no message identity; delivery outcome uncertain");
  assertResource(space, data.name, "messages");
  return data.name;
}
function isSenderAllowed(space: string, sender: string): boolean {
  const binding = config.bindings.find(b => b.space === space);
  if (!binding || !USER_RE.test(sender)) return false;
  return (binding.allowAllSpaceMembers ?? config.allowAllSpaceMembers ?? false)
    || (binding.allowFrom ?? config.allowFrom ?? []).includes(sender);
}
function isCommandAuthorized(space: string, sender: string): boolean {
  return isSenderAllowed(space, sender)
    && (config.bindings.find(b => b.space === space)?.commandAllowFrom ?? config.commandAllowFrom ?? []).includes(sender);
}
function mediaLimit(): number { return config?.media?.maxBytes ?? DEFAULT_MEDIA_BYTES; }
function requireOutboundMedia(): void {
  if (!config?.media?.outboundEnabled) throw new Error("Outbound media is disabled");
  if (authMode() !== "user-oauth") throw new Error("Outbound uploads require user-oauth authentication");
}
function requireReactions(): void {
  if (!config.reactionsEnabled || authMode() !== "user-oauth") throw new Error("Reactions require explicit enablement and user-oauth authentication");
}
function validateConfig(): void {
  for (const value of [config.projectId, config.topicId, config.subscriptionId]) {
    if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_.~+%-]{0,254}$/.test(value) || value.includes("%")) throw new Error("Invalid project/topic/subscription identifier");
  }
  if (!Array.isArray(config.bindings) || !config.bindings.length) throw new Error("At least one binding is required");
  const spaces = new Set<string>();
  for (const binding of config.bindings) {
    if (!SPACE_RE.test(binding.space) || spaces.has(binding.space)) throw new Error("Invalid or duplicate space binding");
    spaces.add(binding.space);
    if (!Array.isArray(binding.agents) || !binding.agents.length || binding.agents.length > 10 || new Set(binding.agents.map(a => a.agentId)).size !== binding.agents.length || binding.agents.some(a => !/^[a-zA-Z0-9_-]{1,80}$/.test(a.agentId))) throw new Error("Invalid agent binding");
    for (const list of [binding.allowFrom, binding.commandAllowFrom]) {
      if (list !== undefined && (!Array.isArray(list) || list.some(v => !USER_RE.test(v)))) throw new Error("Allowlists require stable numeric users/<id> identifiers");
    }
    if (!(binding.allowAllSpaceMembers ?? config.allowAllSpaceMembers) && !(binding.allowFrom ?? config.allowFrom)?.length) throw new Error("Configure allowFrom or explicitly allowAllSpaceMembers for every space");
  }
  if (config.media?.localRoots !== undefined && (!Array.isArray(config.media.localRoots)
    || config.media.localRoots.some(root => typeof root !== "string" || !isAbsolute(root)))) throw new Error("Media roots must be absolute paths");
  for (const list of [config.allowFrom, config.commandAllowFrom]) {
    if (list !== undefined && (!Array.isArray(list) || list.some(v => !USER_RE.test(v)))) throw new Error("Allowlists require stable numeric users/<id> identifiers");
  }
  if (config.auth && !["service-account", "user-oauth"].includes(config.auth.mode)) throw new Error("Invalid authentication mode");
  if (authMode() === "user-oauth" && (!config.oauth?.clientId || !config.oauth.clientSecret || !config.oauth.tokensFile)) throw new Error("User OAuth configuration is incomplete");
  if (authMode() === "service-account" && (config.reactionsEnabled || config.media?.outboundEnabled)) throw new Error("App authentication does not support user-only reactions/uploads");
  for (const [value, min, max] of [
    [config.pollIntervalSeconds ?? 3, 1, 300], [config.agentTimeoutSeconds ?? 60, 10, 3600],
    [config.maxPendingMessages ?? 200, 1, 1000], [config.renewalBufferMinutes ?? 30, 1, 60],
    [mediaLimit(), 1, 200 * 1024 * 1024], [config.media?.maxAttachments ?? 5, 1, 10],
    [config.media?.retentionHours ?? 24, 1, 168],
  ]) { if (!Number.isInteger(value) || value < min || value > max) throw new Error("Invalid numeric limit in plugin configuration"); }
}

// ── Routing ──────────────────────────────────────────────────────────────────

function buildRoutingTable(
  bindings: SpaceBinding[]
): Map<string, RoutingEntry> {
  const table = new Map<string, RoutingEntry>();

  for (const binding of bindings) {
    const keywordAgents = new Map<string, AgentBinding>();
    const alwaysListen: AgentBinding[] = [];
    const keywords: string[] = [];

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

    let pattern: RegExp | null = null;
    if (keywords.length) {
      pattern = new RegExp(
        `(?:^|[\\s@<])(${keywords.join("|")})(?=[\\s>,.:!?'")}]|$)`,
        "i"
      );
    }

    const replyInThread = binding.replyInThread ?? false;
    // threadSessionIsolation defaults to true when replyInThread is enabled
    const threadSessionIsolation = binding.threadSessionIsolation ?? replyInThread;

    table.set(binding.space, { keywordAgents, alwaysListen, pattern, replyInThread, threadSessionIsolation });
  }

  return table;
}

function routeMessage(text: string, space: string): AgentBinding[] {
  const entry = routingTable.get(space);
  if (!entry) return [];

  const matched: AgentBinding[] = [];
  const seen = new Set<string>();

  // 1) Always include alwaysListen agents
  for (const agent of entry.alwaysListen) {
    if (!seen.has(agent.agentId)) {
      matched.push(agent);
      seen.add(agent.agentId);
    }
  }

  // 2) Add keyword-matched agents on top (deduped)
  if (entry.pattern) {
    const matches = text.matchAll(new RegExp(entry.pattern.source, "gi"));
    for (const m of matches) {
      const kw = m[1].toLowerCase();
      const agent = entry.keywordAgents.get(kw);
      if (agent && !seen.has(agent.agentId)) {
        matched.push(agent);
        seen.add(agent.agentId);
        logger.info(`🎯 Keyword '${kw}' → agent '${agent.agentId}'`);
      }
    }
  }

  if (matched.length) {
    logger.info(`📨 Routed to ${matched.length} agent(s): ${matched.map(a => a.agentId).join(', ')}`);
  }

  return matched;
}

// ── Pub/Sub ──────────────────────────────────────────────────────────────────

async function pullMessages(token: string): Promise<any[]> {
  const sub = `projects/${config.projectId}/subscriptions/${config.subscriptionId}`;
  const { status, data } = await httpJson(`https://pubsub.googleapis.com/v1/${sub}:pull`, {
    method: "POST", headers: { Authorization: `Bearer ${token}` }, body: { maxMessages: 10, returnImmediately: true },
  });
  if (status >= 400 || !data) throw new ApiError("Pub/Sub pull", status);
  return Array.isArray(data.receivedMessages) ? data.receivedMessages : [];
}
async function ackMessages(token: string, ackIds: string[]): Promise<void> {
  if (!ackIds.length) return;
  const sub = `projects/${config.projectId}/subscriptions/${config.subscriptionId}`;
  const { status } = await httpJson(`https://pubsub.googleapis.com/v1/${sub}:acknowledge`, {
    method: "POST", headers: { Authorization: `Bearer ${token}` }, body: { ackIds },
  });
  if (status >= 400) throw new ApiError("Pub/Sub acknowledgement", status);
}

function resolveStateDir(): string {
  const state = pluginApi?.runtime?.state?.resolveStateDir?.();
  if (!state) throw new Error("OpenClaw runtime state directory is unavailable");
  return state;
}
function pluginStateDir(): string { return join(resolveStateDir(), "googlechatpubsub"); }
function loadSubState(): Record<string, any> {
  const path = join(pluginStateDir(), STATE_FILE_NAME);
  if (!existsSync(path)) return { subscriptions: {} };
  const stat = lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 1024 * 1024) throw new Error("Unsafe subscription state file");
  const data = JSON.parse(readFileSync(path, "utf8"));
  if (!data || typeof data.subscriptions !== "object") throw new Error("Invalid subscription state");
  return data;
}
function saveSubState(state: Record<string, any>): void { atomicJson(join(pluginStateDir(), STATE_FILE_NAME), state); }
async function ensureSubscription(space: string, token: string): Promise<void> {
  const entry = await maintainSubscription({
    space, topic: `projects/${config.projectId}/topics/${config.topicId}`,
    current: subscriptionState.subscriptions?.[space], token,
    inspectionToken: authMode() === "service-account" ? await getBotToken() : token,
    bufferSeconds: (config.renewalBufferMinutes ?? 30) * 60, request: httpJson,
  });
  subscriptionState.subscriptions ??= {};
  subscriptionState.subscriptions[space] = entry;
  saveSubState(subscriptionState);
}
async function checkAndRenewAll(): Promise<void> {
  const token = await getEventsToken();
  for (const space of targetSpaces) await ensureSubscription(space, token);
}

// ── Attachment downloader ────────────────────────────────────────────────────

interface DownloadedAttachment {
  localPath: string;
  mimeType: string;
  filename: string;
}

/**
 * Download Google Chat attachments via the Chat API media.download endpoint.
 * Files are saved to ~/.openclaw/media/inbound/ so OpenClaw picks them up as MediaUrls.
 */
function pruneInboundMedia(): void {
  const mediaDir = join(resolveStateDir(), "media", "inbound", "googlechatpubsub");
  if (!existsSync(mediaDir)) return;
  const directory = lstatSync(mediaDir);
  if (!directory.isDirectory() || directory.isSymbolicLink()) throw new Error("Unsafe media directory");
  const cutoff = Date.now() - (config.media?.retentionHours ?? 24) * 3600_000;
  for (const name of readdirSync(mediaDir)) {
    if (!/^[0-9a-f-]{36}\.[A-Za-z0-9]{1,10}$/.test(name)) continue;
    const path = join(mediaDir, name);
    const st = lstatSync(path);
    if (st.isFile() && !st.isSymbolicLink() && st.mtimeMs < cutoff) unlinkSync(path);
  }
}

async function downloadAttachments(attachments: any[], token: string): Promise<DownloadedAttachment[]> {
  if (!config.media?.inboundEnabled || !attachments?.length) return [];
  if (attachments.length > (config.media.maxAttachments ?? 5)) throw new Error("Too many inbound attachments");
  const mediaDir = join(resolveStateDir(), "media", "inbound", "googlechatpubsub");
  privateDirectory(mediaDir);
  pruneInboundMedia();
  const results: DownloadedAttachment[] = [];
  for (const att of attachments) {
    const resource = att.attachmentDataRef?.resourceName || att.name;
    if (typeof resource !== "string" || !/^[A-Za-z0-9_./=+-]+$/.test(resource)
      || resource.split("/").some((part: string) => part === "." || part === "..")) continue;
    let ext = extname(typeof att.contentName === "string" ? att.contentName : "");
    if (!/^\.[A-Za-z0-9]{1,10}$/.test(ext)) ext = ".bin";
    const filename = `${randomUUID()}${ext}`;
    const localPath = join(mediaDir, filename);
    const url = `https://chat.googleapis.com/v1/media/${resource.split("/").map(encodeURIComponent).join("/")}?alt=media`;
    const downloaded = await fetchMedia(url, mediaLimit(), { Authorization: `Bearer ${token}` });
    writeFileSync(localPath, downloaded.buffer, { flag: "wx", mode: 0o600 });
    results.push({ localPath, filename, mimeType: downloaded.headers.get("content-type") || "application/octet-stream" });
  }
  return results;
}

// ── Attachment uploader (outbound) ───────────────────────────────────────────

const MAX_UPLOAD_BYTES = 200 * 1024 * 1024; // Google Chat hard limit: 200 MB

const EXT_TO_MIME: Record<string, string> = {
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
  ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
};

function guessMimeType(filename: string): string {
  return EXT_TO_MIME[extname(filename).toLowerCase()] || "application/octet-stream";
}

interface ResolvedMedia {
  buffer: Buffer;
  filename: string;
  mimeType: string;
}

/**
 * Resolve one outbound media reference (local path, file:// URL, or http(s) URL)
 * into an in-memory buffer plus filename/MIME metadata.
 */
async function resolveOutboundMedia(ref: string, opts: { readFile?: (p: string) => Promise<Buffer> } = {}): Promise<ResolvedMedia> {
  requireOutboundMedia();
  const trimmed = String(ref || "").trim();
  if (!trimmed) throw new Error("Empty media reference");
  if (/^https?:\/\//i.test(trimmed)) {
    if (!config.media?.allowRemoteUrls) throw new Error("Remote media URLs are disabled");
    const result = await fetchMedia(trimmed, mediaLimit());
    let filename = "";
    const disposition = result.headers.get("content-disposition") || "";
    const match = /filename\*?=(?:UTF-8'')?"?([^";]+)"?/i.exec(disposition);
    if (match) { try { filename = decodeURIComponent(match[1]); } catch {} }
    if (!filename) { try { filename = decodeURIComponent(new URL(result.url).pathname.split("/").pop() || ""); } catch {} }
    filename = filename.replace(/[\\/\r\n\x00]/g, "_").slice(0, 200) || "attachment";
    const headerMime = (result.headers.get("content-type") || "").split(";")[0].trim();
    const mimeType = /^[A-Za-z0-9!#$&^_.+-]+\/[A-Za-z0-9!#$&^_.+-]+$/.test(headerMime) ? headerMime : guessMimeType(filename);
    if (!extname(filename)) {
      const ext = Object.entries(EXT_TO_MIME).find(([, mime]) => mime === mimeType)?.[0];
      if (ext) filename += ext;
    }
    return { buffer: result.buffer, filename, mimeType };
  }
  const localPath = trimmed.startsWith("file://") ? fileURLToPath(trimmed) : resolve(trimmed);
  // A host-supplied reader is an existing authorization capability. Otherwise require explicit roots.
  const buffer = opts.readFile ? await opts.readFile(localPath)
    : await readMediaFile(localPath, config.media?.localRoots ?? [], mediaLimit());
  if (buffer.length > mediaLimit()) throw new Error("Media exceeds configured byte limit");
  return { buffer, filename: localPath.split("/").pop() || "attachment", mimeType: guessMimeType(localPath) };
}

/**
 * Upload one file to a Google Chat space via the media.upload endpoint and
 * return the short-lived attachmentUploadToken used when creating a message.
 *
 * Uses multipart/related (Google's standard uploadType=multipart protocol):
 * a JSON metadata part followed by the raw file bytes.
 */
async function uploadAttachment(params: {
  space: string;
  media: ResolvedMedia;
  token: string;
  timeoutMs?: number;
}): Promise<string> {
  const { space, media, token, timeoutMs = 120_000 } = params;
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

  const boundary = `openclaw-${randomUUID()}`;
  const head = Buffer.from(
    `--${boundary}\r\n` +
      `Content-Type: application/json; charset=UTF-8\r\n\r\n` +
      `${JSON.stringify({ filename: media.filename })}\r\n` +
      `--${boundary}\r\n` +
      `Content-Type: ${media.mimeType}\r\n\r\n`,
    "utf8"
  );
  const tail = Buffer.from(`\r\n--${boundary}--\r\n`, "utf8");
  const body = Buffer.concat([head, media.buffer, tail]);

  const url = `https://chat.googleapis.com/upload/v1/${space}/attachments:upload?uploadType=multipart`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let resp: Response;
  let text: string;
  try {
    resp = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": `multipart/related; boundary=${boundary}`,
      },
      body,
      signal: controller.signal,
      redirect: "error",
    });
    text = (await readBoundedResponse(resp, 64 * 1024)).toString("utf8");
  } finally {
    clearTimeout(timer);
  }
  if (!resp.ok) {
    throw new Error(`Attachment upload failed (${resp.status}): request rejected`);
  }
  let data: any;
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

/**
 * Upload media refs and post one Chat message carrying them as attachments.
 * Google Chat accepts at most one attachment per message, so multiple refs are
 * sent as a sequence; the caption rides on the first message only.
 *
 * NOTE: `attachments:upload` requires USER authentication. Google rejects
 * service-account/app auth there with ACCESS_TOKEN_SCOPE_INSUFFICIENT, and the
 * returned upload token is bound to the uploading user — so the message that
 * carries it is created with that same OAuth credential.
 */
async function sendMediaMessages(params: {
  space: string;
  mediaRefs: string[];
  token: string;
  text?: string;
  threadName?: string;
  replyMessageOption?: string;
  readFile?: (p: string) => Promise<Buffer>;
}): Promise<{ sent: number; failed: string[]; messageIds: string[] }> {
  const {
    space,
    mediaRefs,
    token,
    text,
    threadName,
    replyMessageOption = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD",
    readFile,
  } = params;

  requireOutboundMedia();
  assertDestination(space, threadName);
  if (mediaRefs.length > (config.media?.maxAttachments ?? 5)) throw new Error("Too many outbound attachments");
  const failed: string[] = [];
  const messageIds: string[] = [];
  let sent = 0;
  let captionPending = text?.trim() || "";

  for (const ref of mediaRefs) {
    try {
      const media = await resolveOutboundMedia(ref, { readFile });
      const uploadToken = await uploadAttachment({ space, media, token });

      const msgBody: any = {
        attachment: [{ attachmentDataRef: { attachmentUploadToken: uploadToken } }],
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
        timeoutMs: 60_000,
      });
      if (status >= 400) {
        throw new Error(`Message create failed (${status}): request rejected`);
      }
      captionPending = "";
      assertChatMessage(data, space);
      sent++;
      if (data?.name) messageIds.push(data.name);
      // This message was posted with the USER OAuth credential, so Google will
      // republish it over Pub/Sub typed HUMAN. Register it before that can
      // happen so the echo is dropped even if selfUserId is not resolved yet.
      markProcessed(data?.name);
      rememberSelfIdentity(data);
      logger.info(`[upload] Sent attachment to ${space}`);
    } catch (err: any) {
      logger.error(`[upload] Failed: ${safeError(err)}`);
      failed.push(`Attachment delivery failed: ${safeError(err)}`);
    }
  }

  // Caption survives when every upload failed — deliver it as plain text so the
  // agent's words are never silently dropped.
  if (captionPending && sent === 0) {
    const msgBody: any = { text: captionPending };
    let url = `https://chat.googleapis.com/v1/${space}/messages`;
    if (threadName) {
      msgBody.thread = { name: threadName };
      url += `?messageReplyOption=${replyMessageOption}`;
    }
    await httpJson(url, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      body: msgBody,
    })
      .then(({ status, data }) => {
        if (status >= 400) return;
        assertChatMessage(data, space);
        // Also user-auth'd, so it echoes back typed HUMAN like the uploads.
        markProcessed(data?.name);
        rememberSelfIdentity(data);
      })
      .catch(() => {});
  }

  return { sent, failed, messageIds };
}

// ── In-process message pipeline ─────────────────────────────────────────────

async function processMessageInPipeline(params: {
  agentId: string;
  space: string;
  spaceDisplayName: string;
  senderId: string;
  senderName: string;
  text: string;
  messageName: string;
  threadName: string;
  eventTime?: string;
  replyInThread: boolean;
  threadSessionIsolation: boolean;
  attachmentPaths?: string[];
  crossDispatchChainKey?: string;
  rawAttachments?: any[];
}): Promise<void> {
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
    crossDispatchChainKey,
  } = params;

  // Cross-dispatch chain key: groups all dispatches from the same originating message
  const chainKey = crossDispatchChainKey || `${threadName || space}:${messageName}:${Date.now()}`;
  // Collect all delivered text for cross-dispatch scanning
  const deliveredChunks: string[] = [];

  assertDestination(space, threadName || undefined);
  const api = pluginApi;
  const cfg = api.config;
  const runtime = api.runtime;

  // 1) Resolve route + build envelope — creates proper session key
  //    Uses agentId as accountId so bindings resolve to the correct agent
  //    Thread-isolated: agent:{agentId}:googlechatpubsub:{agentId}:group:spaces/...:thread:...
  //    Space-scoped:    agent:{agentId}:googlechatpubsub:{agentId}:group:spaces/...

  // Determine the effective thread ID for this message
  const effectiveThreadId = threadName || "";

  // Determine peer ID — if thread isolation is on and we have a thread, include it in peer ID
  const peerId = threadSessionIsolation && effectiveThreadId
    ? `${space}:thread:${effectiveThreadId.split("/").pop()}`
    : space;

  // Use agentId as accountId — bindings in openclaw.json map
  // { channel: "googlechatpubsub", accountId: "main" } → agentId: "main"
  // { channel: "googlechatpubsub", accountId: "rd" } → agentId: "rd"
  // { channel: "googlechatpubsub", accountId: "ca_hc" } → agentId: "ca_hc"
  const { route, buildEnvelope } = resolveInboundRouteEnvelopeBuilderWithRuntime({
    cfg,
    channel: "googlechatpubsub",
    accountId: agentId,
    peer: {
      kind: "group" as const,
      id: peerId,
    },
    runtime: runtime.channel,
    sessionStore: cfg.session?.store,
  });

  logger.info(`🔑 Session key: ${route.sessionKey} (agent=${agentId}, threadIsolation=${threadSessionIsolation}, thread=${effectiveThreadId || 'none'})`);

  const fromLabel = spaceDisplayName || `space:${space}`;
  const { storePath, body } = buildEnvelope({
    channel: "Google Chat",
    from: fromLabel,
    timestamp: eventTime ? Date.parse(eventTime) : undefined,
    body: text,
  });

  // 2) Build inbound context payload (same structure as stock googlechat)
  // Use MediaPaths (not MediaUrls) for local files — normalizeAttachments sets path: void 0
  // when only MediaUrls is provided, treating them as remote URLs to fetch instead of local paths.
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
    SenderName: senderName || undefined,
    SenderId: senderId,
    WasMentioned: false,
    CommandAuthorized: isCommandAuthorized(space, senderId),
    Provider: "googlechatpubsub",
    Surface: "googlechatpubsub",
    MessageSid: messageName,
    MessageSidFull: messageName,
    ReplyToId: threadName || undefined,
    ReplyToIdFull: threadName || undefined,
    GroupSpace: spaceDisplayName || undefined,
    OriginatingChannel: "googlechatpubsub",
    OriginatingTo: `googlechatpubsub:${space}`,
    ...(attachmentPaths.length > 0 && {
      MediaPaths: attachmentPaths,          // local file paths → sets path: value in normalizeAttachments
      MediaUrls: attachmentPaths,           // also set for compat/dedup logic
    }),
  });

  // 3) Record session meta — makes session visible in /session
  void runtime.channel.session
    .recordSessionMetaFromInbound({
      storePath,
      sessionKey: ctxPayload.SessionKey ?? route.sessionKey,
      ctx: ctxPayload,
    })
    .catch((err: any) => {
      logger.error(
        `googlechatpubsub: failed updating session meta: ${safeError(err)}`
      );
    });

  // 4) Determine reply thread target
  //    replyInThread=true + no existing thread → create new thread on the original message
  //    replyInThread=true + existing thread → reply in that thread
  //    replyInThread=false → reply in main window (use thread only if message was already in one)
  let replyThreadName = threadName; // default: follow the incoming message's thread
  let replyMessageOption = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD";

  if (replyInThread) {
    if (!threadName) {
      // Message in main window, replyInThread=true → start new thread on original message
      // Google Chat: set thread.name = spaces/xxx/threads/xxx where thread ID = message ID portion
      // Actually: to start a thread on a message, use the message's thread name from the API
      // The Chat API will auto-create a thread when we reply with messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD
      // and thread = { name: <original message's thread.name> }
      // In Google Chat, every message has a thread — even in "flat" conversations
      // We use the original message name to target the thread
      replyThreadName = messageName ? `${space}/threads/${messageName.split("/").pop()}` : "";
      replyMessageOption = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD";
      logger.info(`🧵 replyInThread: creating new thread on message ${messageName}`);
    } else {
      // Already in a thread, stay there
      replyMessageOption = "REPLY_MESSAGE_OR_FAIL";
      logger.info(`🧵 replyInThread: continuing in existing thread ${threadName}`);
    }
  }

  // 5) Typing indicator (skipped when silentReply is true)
  //    silentReply=true (default): no typing message posted — reply sent directly as a new POST
  //    silentReply=false: legacy behaviour — post "_typing..._" then PATCH with the reply
  const silentReply = config.silentReply !== false; // default true
  let typingMessageName: string | undefined;
  if (!silentReply) {
    try {
      const botToken = await getBotToken();
      const typingBody: any = { text: "_typing..._" };
      if (replyInThread && replyThreadName) {
        typingBody.thread = { name: replyThreadName };
      } else if (threadName) {
        typingBody.thread = { name: threadName };
      }
      const typingUrl = replyInThread && replyThreadName
        ? `https://chat.googleapis.com/v1/${space}/messages?messageReplyOption=${replyMessageOption}`
        : `https://chat.googleapis.com/v1/${space}/messages`;

      logger.info(`⏳ Sending typing indicator to ${space} (thread: ${replyThreadName || threadName || 'none'}, replyInThread: ${replyInThread})`);
      const result = await httpJson(typingUrl, {
        method: "POST",
        headers: { Authorization: `Bearer ${botToken}` },
        body: typingBody,
      });
      logger.info(`⏳ Typing indicator result: status=${result.status} name=${result.data?.name || 'none'}`);
      if (result.status < 400 && result.data?.name) {
        typingMessageName = result.data.name;
        // Capture the actual thread name from the response (important for new threads)
        if (result.data?.thread?.name && !replyThreadName) {
          replyThreadName = result.data.thread.name;
        }
      } else {
        logger.warn(`Typing indicator failed: ${result.status} request rejected`);
      }
    } catch (err: any) {
      logger.warn(`Typing indicator exception: ${safeError(err)}`);
    }
  } else {
    logger.info(`🔇 silentReply=true — skipping typing indicator`);
  }

  // 6) Dispatch reply through the OpenClaw agent pipeline
  const { onModelSelected, ...prefixOptions } = createReplyPrefixOptions({
    cfg,
    agentId: route.agentId,
    channel: "googlechatpubsub",
    accountId: agentId,
  });

  let pipelineError: Error | undefined;
  await runtime.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
    ctx: ctxPayload,
    cfg,
    dispatcherOptions: {
      ...prefixOptions,
      deliver: async (payload: {
        text?: string;
        mediaUrls?: string[];
        mediaUrl?: string;
        replyToId?: string;
      }, info?: any) => {
        logger.debug(`Reply delivery: kind=${info?.kind || "unknown"}`);
        if (activeAbort?.signal.aborted) throw new Error("Reply cancelled");
        assertDestination(space, threadName || undefined);
        const botToken = await getBotToken();
        const replyText = payload.text?.trim();

        // Collect media refs from either payload shape.
        const mediaRefs = [
          ...(payload.mediaUrls || []),
          ...(payload.mediaUrl ? [payload.mediaUrl] : []),
        ].filter((m): m is string => Boolean(m && String(m).trim()));
        const uniqueMediaRefs = [...new Set(mediaRefs)];

        if (uniqueMediaRefs.length > 0) {
          requireOutboundMedia();
          // Media path: attachments carry the text as the first message's caption.
          const effectiveThread = replyInThread ? replyThreadName : threadName;

          // Retire the typing placeholder first so the attachment is not orphaned behind it.
          if (typingMessageName) {
            await httpJson(`https://chat.googleapis.com/v1/${typingMessageName}`, {
              method: "DELETE",
              headers: { Authorization: `Bearer ${botToken}` },
            }).catch(() => {});
            typingMessageName = undefined;
          }

          if (replyText) deliveredChunks.push(replyText);

          const result = await sendMediaMessages({
            space,
            mediaRefs: uniqueMediaRefs,
            token: await getOAuthToken(),
            text: replyText,
            threadName: effectiveThread,
            replyMessageOption,
          });
          if (result.failed.length) throw new Error("One or more media deliveries failed");
          logger.info(
            `📎 Media delivery: ${result.sent}/${uniqueMediaRefs.length} sent` +
              (result.failed.length ? ` | failures: ${result.failed.join("; ").slice(0, 300)}` : "")
          );
          return;
        }

        if (!replyText) return;

        // Track delivered text for cross-agent dispatch
        deliveredChunks.push(replyText);

        const chunkLimit = 4000;
        const chunks: string[] = [];
        if (replyText.length <= chunkLimit) {
          chunks.push(replyText);
        } else {
          // Simple chunking by newlines
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
              // Edit typing indicator with first chunk
              logger.info(`📝 PATCHing typing message: ${typingMessageName}`);
              const patchResult = await httpJson(
                `https://chat.googleapis.com/v1/${typingMessageName}?updateMask=text`,
                {
                  method: "PATCH",
                  headers: { Authorization: `Bearer ${botToken}` },
                  body: { text: chunk },
                }
              );
              if (patchResult.status >= 400) {
                logger.warn(`PATCH failed (${patchResult.status}), falling back to POST: request rejected`);
                // Fallback: send as new message
                const msgBody: any = { text: chunk };
                let url = `https://chat.googleapis.com/v1/${space}/messages`;
                const effectiveThread = replyInThread ? replyThreadName : threadName;
                if (effectiveThread) {
                  msgBody.thread = { name: effectiveThread };
                  url += `?messageReplyOption=${replyMessageOption}`;
                }
                const postResult = await httpJson(url, {
                  method: "POST",
                  headers: { Authorization: `Bearer ${botToken}` },
                  body: msgBody,
                });
                if (postResult.status >= 400) throw new ApiError("Chat reply", postResult.status);
                assertChatMessage(postResult.data, space);
                logger.info(`POST fallback result: ${postResult.status}`);
              } else {
                assertChatMessage(patchResult.data, space);
                logger.info(`✅ PATCH succeeded`);
              }
              typingMessageName = undefined;
            } else {
              const msgBody: any = { text: chunk };
              let url = `https://chat.googleapis.com/v1/${space}/messages`;
              const effectiveThread = replyInThread ? replyThreadName : threadName;
              if (effectiveThread) {
                msgBody.thread = { name: effectiveThread };
                url += `?messageReplyOption=${replyMessageOption}`;
              }
              const postResult = await httpJson(url, {
                method: "POST",
                headers: { Authorization: `Bearer ${botToken}` },
                body: msgBody,
              });
              if (postResult.status >= 400) throw new ApiError("Chat reply", postResult.status);
                assertChatMessage(postResult.data, space);
              logger.info(`📨 POST result: ${postResult.status}`);
            }
          } catch (err: any) {
            logger.error(`Chat API reply failed: ${safeError(err)}`);
            pipelineError = new Error("Chat API reply failed");
            throw pipelineError;
          }
        }
      },
      onSkip: (payload: any, info: any) => {
        logger.debug(`Reply skipped: kind=${info?.kind || "unknown"}`);
      },
      onHeartbeatStrip: () => {
        logger.info(`💓 Heartbeat strip triggered`);
      },
      onError: (err: any, info: any) => {
        pipelineError = new Error("Agent or reply delivery failed");
        logger.error(
          `googlechatpubsub reply ${info?.kind || "?"} failed: ${safeError(err)}`
        );
        // Clean up typing indicator on error
        if (typingMessageName) {
          getBotToken()
            .then((t) =>
              httpJson(
                `https://chat.googleapis.com/v1/${typingMessageName}`,
                {
                  method: "DELETE",
                  headers: { Authorization: `Bearer ${t}` },
                }
              )
            )
            .catch(() => {});
        }
      },
    },
    replyOptions: {
      onModelSelected,
      abortSignal: activeAbort?.signal,
      onAgentRunTerminalOutcome: (outcome: string) => { if (outcome === "failed") pipelineError = new Error("Agent run failed"); },
    },
  });

  // Clean up orphaned typing indicator (e.g. agent replied NO_REPLY / silent skip)
  // If deliver was called, typingMessageName was set to undefined inside deliver.
  // If we get here and it's still set, no delivery happened — delete the stale message.
  if (typingMessageName) {
    logger.info(`🧹 Cleaning up orphaned typing message: ${typingMessageName}`);
    try {
      const cleanupToken = await getBotToken();
      await httpJson(`https://chat.googleapis.com/v1/${typingMessageName}`, {
        method: "DELETE",
        headers: { Authorization: `Bearer ${cleanupToken}` },
      });
      logger.info(`✅ Orphaned typing message deleted`);
    } catch (err: any) {
      logger.error(`Failed to delete orphaned typing message: ${safeError(err)}`);
    }
  }

  if (pipelineError) throw pipelineError;

  // 7) Cross-agent dispatch: scan delivered text for mentions of other agents
  //    Only runs when config.crossAgentDispatch === true
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
        chainKey,
      });
    } catch (err: any) {
      logger.error(`Cross-agent dispatch error: ${safeError(err)}`);
      throw err;
    }
  }
}

// ── Chat API reaction ───────────────────────────────────────────────────────

// ── Poll loop ───────────────────────────────────────────────────────────────

function admittedJobs(received: any): QueuedMessage[] {
  if (received.message?.attributes?.["ce-type"] !== MESSAGE_EVENT) return [];
  const raw = received.message?.data;
  if (typeof raw !== "string" || raw.length > 90_000) return [];
  let data: any;
  try { data = JSON.parse(Buffer.from(raw, "base64").toString("utf8")); } catch { return []; }
  const message = data.message;
  if (!message || message.sender?.type !== "HUMAN" || isSelfMessage(message.sender)) return [];
  const space = message.space?.name;
  const sender = message.sender?.name;
  if (typeof space !== "string" || typeof sender !== "string" || !isSenderAllowed(space, sender)) return [];
  const messageName = message.name;
  if (typeof messageName !== "string" || !messageName) return [];
  const threadName = message.thread?.name || "";
  try { assertResource(space, messageName, "messages"); assertResource(space, threadName || undefined, "threads"); } catch { return []; }
  if (processedMsgIds.has(messageName)) return [];
  const text = typeof message.text === "string" ? message.text.trim() : "";
  if (Buffer.byteLength(text) > 32 * 1024) return [];
  const rawAttachments = config.media?.inboundEnabled ? (message.attachment || message.attachments || []) : [];
  if (!Array.isArray(rawAttachments) || rawAttachments.length > (config.media?.maxAttachments ?? 5)) return [];
  const attachments = rawAttachments.map((att: any) => ({
    name: typeof att?.name === "string" ? att.name.slice(0, 2048) : undefined,
    contentName: typeof att?.contentName === "string" ? att.contentName.slice(0, 200) : undefined,
    attachmentDataRef: { resourceName: typeof att?.attachmentDataRef?.resourceName === "string" ? att.attachmentDataRef.resourceName.slice(0, 2048) : undefined },
  }));
  if (!text && !attachments.length) return [];
  const entry = routingTable.get(space)!;
  return (text ? routeMessage(text, space) : entry.alwaysListen).map(agent => ({
    agent, space, spaceDisplayName: space, senderId: sender,
    senderName: typeof message.sender.displayName === "string" ? message.sender.displayName.slice(0, 200) : sender,
    text, messageName, threadName, eventTime: message.createTime || new Date().toISOString(),
    replyInThread: entry.replyInThread, threadSessionIsolation: entry.threadSessionIsolation,
    attachmentPaths: [], rawAttachments: attachments,
  }));
}

function startInboxWorker(): void {
  if (workerPromise || stopping || !inbox || fatalInboxError) return;
  workerPromise = (async () => {
    while (!stopping && !fatalInboxError) {
      const record = inbox!.ready();
      if (!record?.payload) return;
      const msg = record.payload;
      let dispatched = false;
      const attempts = record.attempts + 1;
      try {
        // Re-evaluate admission and routing after a restart/configuration change.
        const current = config.bindings.find(b => b.space === msg.space);
        if (!isSenderAllowed(msg.space, msg.senderId) || !current?.agents.some(a => a.agentId === msg.agent.agentId)) {
          inbox!.update(record.id, { state: "done", reason: "admission revoked" });
          continue;
        }
        inbox!.update(record.id, { attempts });
        if (msg.rawAttachments?.length && config.media?.inboundEnabled) {
          const token = authMode() === "service-account" ? await getBotToken() : await getOAuthToken();
          msg.attachmentPaths = (await downloadAttachments(msg.rawAttachments, token)).map(item => item.localPath);
        }
        inbox!.update(record.id, { state: "running" });
        dispatched = true;
        const controller = new AbortController();
        activeAbort = controller;
        const timer = setTimeout(() => {
          controller.abort();
          logger.warn("Agent timeout: cancellation requested; waiting for the active run to settle");
        }, (config.agentTimeoutSeconds ?? 60) * 1000);
        try {
          await dispatchMessage(msg);
          if (controller.signal.aborted) throw new Error("Agent run cancelled; inspect possible side effects");
        } finally { clearTimeout(timer); activeAbort = undefined; }
        inbox!.update(record.id, { state: "done" });
        markProcessed(msg.messageName);
      } catch (error) {
        logger.error(`Inbox processing failed: ${safeError(error)}`);
        try {
          // Only pre-dispatch failures are safe to retry automatically.
          inbox!.update(record.id, dispatched || attempts >= 3
            ? { state: "failed", reason: dispatched ? "agent/delivery outcome uncertain; operator review required" : "pre-dispatch retries exhausted" }
            : { state: "pending", nextAttempt: Date.now() + Math.min(60_000, 1000 * 2 ** attempts), reason: "pre-dispatch retry" });
        } catch (storageError) {
          fatalInboxError = true;
          logger.error(`Inbox persistence failed; listener paused: ${safeError(storageError)}`);
        }
      }
    }
  })().catch(error => {
    fatalInboxError = true;
    logger.error(`Inbox worker stopped: ${safeError(error)}`);
  }).finally(() => { workerPromise = undefined; });
}

async function pollOnce(): Promise<void> {
  if (pollRunning || stopping || fatalInboxError) return;
  pollRunning = true;
  try {
    if (Date.now() - lastRenewalCheck > RENEWAL_INTERVAL) {
      lastRenewalCheck = Date.now();
      try { pruneInboundMedia(); await checkAndRenewAll(); }
      catch (error) { logger.error(`Subscription maintenance failed: ${safeError(error)}`); }
    }
    const token = await getPubSubToken();
    const received = await pullMessages(token);
    for (const message of received) {
      if (stopping) break;
      if (typeof message.ackId !== "string" || !message.ackId) continue;
      const jobs = admittedJobs(message);
      // One atomic admission for every routed agent; a write/capacity failure prevents ACK.
      inbox!.admit(jobs.map(payload => ({ id: `${payload.messageName}:${payload.agent.agentId}`, payload })));
      await ackMessages(token, [message.ackId]);
    }
  } catch (error) { logger.error(`Pub/Sub poll failed: ${safeError(error)}`); }
  finally { pollRunning = false; startInboxWorker(); }
}

// ── Plugin registration ─────────────────────────────────────────────────────

export default function register(api: any) {
  logger = api.logger ?? console;
  pluginApi = api;

  api.registerChannel({
    id: "googlechatpubsub",
    meta: {
      id: "googlechatpubsub",
      label: "Google Chat (Pub/Sub)",
      selectionLabel: "Google Chat Pub/Sub (no-mention listening)",
      docsPath: "/channels/googlechatpubsub",
      blurb:
        "Listen to Google Chat spaces via Workspace Events + Pub/Sub. No @mention required.",
      aliases: ["gchatpubsub", "gchat-pubsub"],
    },
    capabilities: { chatTypes: ["group"], reactions: true, media: true },
    describeMessageTool: () => ({
      actions: ["send", "react", "reactions", "upload-file"] as const,
      capabilities: null,
      schema: null,
    }),
    messaging: {
      targetResolver: {
        hint: "spaces/<SPACE_ID>",
        looksLikeId: (raw: string) => /^spaces\/[a-zA-Z0-9_-]+$/.test(raw.trim()),
        resolveTarget: async ({ normalized }: any) => {
          const to = normalized?.trim();
          if (!to || !/^spaces\/[a-zA-Z0-9_-]+$/.test(to)) return null;
          return { to, kind: "group", source: "normalized" };
        },
      },
    },
    config: {
      listAccountIds: () => ["default"],
      resolveAccount: (cfg: any) => {
        // Primary: channels.googlechatpubsub (standard channel convention)
        // Fallback: plugins.entries.googlechatpubsub.config (legacy ≤0.1.x)
        const pluginCfg =
          cfg.channels?.googlechatpubsub ||
          cfg.plugins?.entries?.googlechatpubsub?.config ||
          {};
        return { accountId: "default", ...pluginCfg };
      },
    },
    handleAction: async (ctx: any) => {
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
      } catch { return { ok: false, error: "Action denied by channel policy" }; }

      if (action === "react") {
        const messageName = params.messageId || params.message_id || params.target;
        const emoji = params.emoji || "👀";
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
              emoji: { unicode: emoji },
            },
          });
          if (status >= 400) {
            return { ok: false, error: `Chat API react failed (${status}): request rejected` };
          }
          return { ok: true, added: emoji };
        } catch (e: any) {
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
            headers: { Authorization: `Bearer ${token}` },
          });
          if (status >= 400) return { ok: false, error: `Chat API reactions failed (${status})` };
          return { ok: true, reactions: data?.reactions || [] };
        } catch (e: any) {
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
          // Respect replyInThread: if threadId/replyTo is provided, reply in that thread.
          const replyToThread = params.threadId || params.replyTo;
          const binding = config?.bindings?.find((b: any) => b.space === space);
          const bindingReplyInThread = binding?.replyInThread ?? false;
          const msgBody: any = { text };
          let url = `https://chat.googleapis.com/v1/${space}/messages`;
          if (replyToThread) {
            msgBody.thread = { name: replyToThread };
            url += `?messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD`;
          } else if (bindingReplyInThread) {
            logger.warn(`[handleAction.send] replyInThread=true for ${space} but no threadId provided — landing in main chat`);
          }
          const { status, data } = await httpJson(url, {
            method: "POST",
            headers: { Authorization: `Bearer ${token}` },
            body: msgBody,
          });
          if (status >= 400) throw new ApiError("Chat send", status);
          const messageId = assertChatMessage(data, space);
          markProcessed(messageId);
          return { ok: true, messageId };
        } catch (e: any) {
          return { ok: false, error: `Send failed: ${safeError(e)}` };
        }
      }

      if (action === "upload-file") {
        const target = params.target;
        if (!target) {
          return { ok: false, error: "target (spaces/<SPACE_ID>) is required for upload-file" };
        }
        const mediaRefs = [
          ...(Array.isArray(params.media) ? params.media : params.media ? [params.media] : []),
          ...(params.path ? [params.path] : []),
          ...(Array.isArray(params.paths) ? params.paths : []),
          ...(Array.isArray(params.attachments)
            ? params.attachments.map((a: any) => a?.media).filter(Boolean)
            : []),
        ].filter((m: any): m is string => Boolean(m && String(m).trim()));

        if (mediaRefs.length === 0) {
          return { ok: false, error: "media (path or URL) is required for upload-file" };
        }

        try {
          // attachments:upload requires user auth, not the bot service account.
          const token = await getOAuthToken();
          const binding = config?.bindings?.find((b: any) => b.space === target);
          const replyToThread = params.threadId || params.replyTo;
          const threadName = replyToThread ? String(replyToThread) : undefined;
          if (binding?.replyInThread && !replyToThread) {
            logger.warn(
              `[handleAction.upload-file] replyInThread=true for ${target} but no threadId provided — landing in main chat`
            );
          }

          const result = await sendMediaMessages({
            space: target,
            mediaRefs: [...new Set(mediaRefs)],
            token,
            text: params.message || params.text || params.caption,
            threadName,
          });

          if (result.sent === 0) {
            return {
              ok: false,
              error: `All uploads failed: ${result.failed.join("; ").slice(0, 500)}`,
            };
          }
          return {
            ok: true,
            sent: result.sent,
            messageIds: result.messageIds,
            ...(result.failed.length ? { partialFailures: result.failed } : {}),
          };
        } catch (e: any) {
          return { ok: false, error: `Upload failed: ${safeError(e)}` };
        }
      }

      return { ok: false, error: `Unsupported action: ${action}` };
    },
    outbound: {
      deliveryMode: "direct",
      sendText: async ({ text, target, threadId, replyTo }: any) => {
        try {
          const space = target || config?.bindings?.[0]?.space;
          if (!space) return { ok: false };
          assertDestination(space, threadId || replyTo);
          const token = await getBotToken();

          const replyToThread = threadId || replyTo;
          const binding = config?.bindings?.find((b: any) => b.space === space);
          const bindingReplyInThread = binding?.replyInThread ?? false;
          const msgBody: any = { text };
          let url = `https://chat.googleapis.com/v1/${space}/messages`;
          if (replyToThread) {
            msgBody.thread = { name: replyToThread };
            url += `?messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD`;
          } else if (bindingReplyInThread) {
            logger.warn(`[outbound.sendText] replyInThread=true for ${space} but no threadId — landing in main chat`);
          }
          const { status, data } = await httpJson(url, {
            method: "POST",
            headers: { Authorization: `Bearer ${token}` },
            body: msgBody,
          });
          // Surface the platform message id so core does not suppress delivery
          // with `adapter_returned_no_identity`.
          if (status >= 400) throw new ApiError("Chat send", status);
          const messageId = assertChatMessage(data, space);
          markProcessed(messageId);
          return { ok: true, messageId, target: space };
        } catch (e: any) {
          logger.error(`outbound sendText error: ${safeError(e)}`);
          return { ok: false };
        }
      },
      sendMedia: async ({ text, target, threadId, replyTo, mediaUrl, mediaReadFile }: any) => {
        try {
          requireOutboundMedia();
          // attachments:upload requires user auth, not the bot service account.
          const space = target || config?.bindings?.[0]?.space;
          if (!space) return { ok: false };
          assertDestination(space, threadId || replyTo);
          const token = await getOAuthToken();
          if (!mediaUrl) return { ok: false };

          const replyToThread = threadId || replyTo;
          const binding = config?.bindings?.find((b: any) => b.space === space);
          if (binding?.replyInThread && !replyToThread) {
            logger.warn(
              `[outbound.sendMedia] replyInThread=true for ${space} but no threadId — landing in main chat`
            );
          }

          const result = await sendMediaMessages({
            space,
            mediaRefs: [String(mediaUrl)],
            token,
            text,
            threadName: replyToThread ? String(replyToThread) : undefined,
            readFile: mediaReadFile,
          });
          // Core suppresses delivery as `adapter_returned_no_identity` unless a
          // concrete platform message id comes back, so always surface one.
          return {
            ok: result.sent > 0,
            messageId: result.messageIds[0],
            target: space,
          };
        } catch (e: any) {
          logger.error(`outbound sendMedia error: ${safeError(e)}`);
          return { ok: false };
        }
      },
    },
  });

  // Register background service
  api.registerService({
      id: "googlechatpubsub-listener",

      start: async () => {
        if (releaseInbox) throw new Error("Pub/Sub listener is already started");
        const cfg = api.config;
        const selected: PubSubConfig = cfg.channels?.googlechatpubsub || cfg.plugins?.entries?.googlechatpubsub?.config;
        if (!selected?.enabled) return;
        config = selected;
        serviceAccountFile = config.serviceAccountFile || cfg.channels?.googlechat?.serviceAccountFile || "";
        if (!serviceAccountFile) throw new Error("serviceAccountFile is required");
        const credentials = lstatSync(serviceAccountFile);
        if (!credentials.isFile() || credentials.isSymbolicLink() || credentials.size > 64 * 1024) throw new Error("Unsafe service account file");
        if (process.platform !== "win32" && (credentials.mode & 0o077)) throw new Error("Service account file must have mode 0600");
        const key = JSON.parse(readFileSync(serviceAccountFile, "utf8"));
        if (key.type !== "service_account" || !key.client_email || !key.private_key) throw new Error("Expected service_account credentials without user impersonation");
        validateConfig();
        privateDirectory(pluginStateDir());
        releaseInbox = await lockInbox(join(pluginStateDir(), "inbox.json"));
        try {
          authTokens = new ServiceAccountTokens(serviceAccountFile);
          oauthCache = { token: null, expiresAt: 0 };
          selfUserId = null; processedMsgIds.clear();
          routingTable = buildRoutingTable(config.bindings);
          targetSpaces = new Set(config.bindings.map(b => b.space));
          subscriptionState = loadSubState();
          inbox = new DurableInbox(join(pluginStateDir(), "inbox.json"), config.maxPendingMessages ?? 200);
          fatalInboxError = false;
          await getPubSubToken(); await getBotToken(); await checkAndRenewAll();
          pruneInboundMedia();
          lastRenewalCheck = Date.now();
          stopping = false;
          const run = async () => {
            await pollOnce();
            if (!stopping) pollTimer = setTimeout(run, (config.pollIntervalSeconds ?? 3) * 1000);
          };
          pollTimer = setTimeout(run, 0);
          const failed = inbox.snapshot().filter(item => item.state === "failed").length;
          logger.info(`Pub/Sub listener started (${authMode()}); ${failed} inbox record(s) require operator review`);
        } catch (error) {
          stopping = true; await releaseInbox(); releaseInbox = undefined;
          throw new Error(`Pub/Sub initialization failed: ${safeError(error)}`);
        }
      },
      stop: async () => {
        stopping = true;
        if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
        activeAbort?.abort();
        // Keep ownership until every active request and agent has settled.
        while (pollRunning) await new Promise(resolve => setTimeout(resolve, 50));
        await workerPromise;
        await releaseInbox?.(); releaseInbox = undefined;
        logger.info("Pub/Sub listener stopped");
      },
    });
}
