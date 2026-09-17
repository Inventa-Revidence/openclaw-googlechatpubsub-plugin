import { ApiError } from "./safety.js";

export const MESSAGE_EVENT = "google.workspace.chat.message.v1.created";
type Request = (url: string, opts?: { method?: string; headers?: Record<string, string>; body?: any }) => Promise<{ status: number; data: any }>;
const BASE = "https://workspaceevents.googleapis.com/v1/";
const validName = (name: unknown): name is string => typeof name === "string" && /^subscriptions\/[A-Za-z0-9_-]+$/.test(name);

export async function maintainSubscription(params: {
  space: string; topic: string; current?: { name?: string }; token: string; inspectionToken: string;
  bufferSeconds: number; request: Request; now?: number;
}): Promise<{ name: string; expiresAt: number; space: string }> {
  const { space, topic, token, inspectionToken, request } = params;
  const now = params.now ?? Date.now() / 1000;
  const headers = { Authorization: `Bearer ${token}` };
  const inspectHeaders = { Authorization: `Bearer ${inspectionToken}` };
  const matches = (sub: any) => validName(sub?.name)
    && sub.targetResource === `//chat.googleapis.com/${space}`
    && sub.notificationEndpoint?.pubsubTopic === topic
    && sub.state !== "DELETED"
    && sub.payloadOptions?.includeResource === true && !sub.payloadOptions.fieldMask
    && sub.eventTypes?.length === 1 && sub.eventTypes[0] === MESSAGE_EVENT;

  async function operation(result: { status: number; data: any }): Promise<any> {
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
      await new Promise(resolve => setTimeout(resolve, 1000));
      const polled = await request(BASE + data.name, { headers: inspectHeaders });
      if (polled.status >= 400) throw new ApiError("Workspace Events operation poll", polled.status);
      data = polled.data;
    }
    throw new Error("Workspace Events operation did not finish in time");
  }
  let sub: any;
  if (validName(params.current?.name)) {
    const result = await request(BASE + params.current.name, { headers: inspectHeaders });
    if (result.status >= 400 && result.status !== 404) throw new ApiError("Subscription inspection", result.status);
    if (result.status < 400 && matches(result.data)) sub = result.data;
  }
  async function discover(): Promise<any> {
    let page = "";
    for (let i = 0; i < 20; i++) {
      const filter = encodeURIComponent(`event_types:"${MESSAGE_EVENT}" AND target_resource="//chat.googleapis.com/${space}"`);
      const result = await request(`${BASE}subscriptions?filter=${filter}&pageSize=100${page ? `&pageToken=${encodeURIComponent(page)}` : ""}`, { headers: inspectHeaders });
      if (result.status >= 400) throw new ApiError("Subscription discovery", result.status);
      const found = result.data?.subscriptions?.find(matches);
      if (found) return found;
      page = result.data?.nextPageToken || "";
      if (!page) return undefined;
    }
    throw new Error("Subscription discovery exceeded page limit");
  }
  if (!sub) sub = await discover();
  if (!sub) {
    const created = await request(BASE + "subscriptions", {
      method: "POST", headers,
      body: { targetResource: `//chat.googleapis.com/${space}`, eventTypes: [MESSAGE_EVENT],
        notificationEndpoint: { pubsubTopic: topic }, payloadOptions: { includeResource: true }, ttl: "14400s" },
    });
    sub = created.status === 409 ? await discover() : await operation(created);
    if (!sub) throw new Error("Conflicting subscription does not match configured target/topic");
  }
  if (!matches(sub)) throw new Error("Workspace subscription target/topic mismatch");
  if (sub.state === "SUSPENDED") {
    sub = await operation(await request(BASE + sub.name + ":reactivate", { method: "POST", headers }));
  }
  const expiry = Date.parse(sub.expireTime) / 1000;
  if (!Number.isFinite(expiry) || expiry <= now + params.bufferSeconds) {
    sub = await operation(await request(BASE + sub.name + "?updateMask=ttl", {
      method: "PATCH", headers, body: { name: sub.name, ttl: "14400s" },
    }));
  }
  const expiresAt = Date.parse(sub?.expireTime) / 1000;
  if (!matches(sub) || sub.state !== "ACTIVE" || !Number.isFinite(expiresAt) || expiresAt <= now) throw new Error("Workspace subscription is not active with a valid expiry");
  return { name: sub.name, expiresAt, space };
}
