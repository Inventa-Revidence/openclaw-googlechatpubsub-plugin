import { openSync, closeSync, writeFileSync, fsyncSync, renameSync, unlinkSync, lstatSync, mkdirSync, chmodSync, realpathSync } from "node:fs";
import { dirname, resolve, relative, isAbsolute } from "node:path";
import { randomUUID } from "node:crypto";
import { root as openMediaRoot } from "openclaw/plugin-sdk/security-runtime";
import { fetchWithSsrFGuard } from "openclaw/plugin-sdk/ssrf-runtime";

export const SPACE_RE = /^spaces\/[A-Za-z0-9_-]+$/;
export const USER_RE = /^users\/[0-9]+$/;
export const DEFAULT_MEDIA_BYTES = 20 * 1024 * 1024;

export function privateDirectory(path: string): void {
  mkdirSync(path, { recursive: true, mode: 0o700 });
  if (!lstatSync(path).isDirectory() || lstatSync(path).isSymbolicLink()) throw new Error("Unsafe state directory");
  chmodSync(path, 0o700);
}

/** Commit before ACK: restrictive permissions, atomic replacement and durable flush. */
export function atomicJson(path: string, value: unknown): void {
  const parent = dirname(path);
  const tmp = resolve(parent, `.${randomUUID()}.tmp`);
  let fd: number | undefined;
  try {
    fd = openSync(tmp, "wx", 0o600);
    writeFileSync(fd, JSON.stringify(value));
    fsyncSync(fd);
    closeSync(fd); fd = undefined;
    renameSync(tmp, path);
    const directory = openSync(parent, "r");
    try { fsyncSync(directory); } finally { closeSync(directory); }
  } finally {
    if (fd !== undefined) closeSync(fd);
    try { unlinkSync(tmp); } catch (e: any) { if (e.code !== "ENOENT") throw e; }
  }
}

export class ApiError extends Error {
  readonly code: string;
  constructor(operation: string, status: number) {
    super(`${operation} failed (${status})`);
    this.name = "ApiError";
    this.code = `HTTP_${status}`;
  }
}

export function safeError(error: unknown): string {
  // API errors can contain credentials, user text and URLs. Log classifications only.
  const name = error instanceof Error ? error.name : "Error";
  const status = (error as any)?.response?.status;
  const code = Number.isInteger(status) && status >= 100 && status <= 599
    ? `HTTP_${status}` : (error as any)?.code;
  return `${/^[A-Za-z0-9_]{1,60}$/.test(name) ? name : "Error"}${typeof code === "string" && /^[A-Z0-9_]{1,40}$/.test(code) ? ` (${code})` : ""}`;
}

export async function readBoundedResponse(response: Response, limit: number): Promise<Buffer> {
  const length = response.headers.get("content-length");
  if (length && (!/^\d+$/.test(length) || Number(length) > limit)) {
    await response.body?.cancel();
    throw new Error("Response exceeds configured byte limit");
  }
  if (!response.body) return Buffer.alloc(0);
  const reader = response.body.getReader();
  const chunks: Buffer[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > limit) throw new Error("Response exceeds configured byte limit");
      chunks.push(Buffer.from(value));
    }
    return Buffer.concat(chunks, size);
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export async function readMediaFile(path: string, roots: readonly string[], maxBytes: number): Promise<Buffer> {
  for (const base of roots) {
    let rel = relative(resolve(base), resolve(path));
    if (rel === ".." || rel.startsWith("../") || isAbsolute(rel)) {
      // A configured root may itself have a platform alias (e.g. /var on macOS).
      // Resolve only the trusted root; never canonicalize an untrusted leaf.
      rel = relative(realpathSync(base), resolve(path));
    }
    if (!rel || rel === ".." || rel.startsWith("../") || isAbsolute(rel)) continue;
    const directory = await openMediaRoot(resolve(base), { symlinks: "reject", hardlinks: "reject", maxBytes });
    // Host helper bounds allocation, checks file identity, and rejects symlink/hardlink escapes.
    return (await directory.read(rel)).buffer;
  }
  throw new Error("Media path is outside configured local roots");
}

export async function fetchMedia(url: string, maxBytes: number, headers: Record<string, string> = {}, timeoutMs = 60_000): Promise<{ buffer: Buffer; headers: Headers; url: string }> {
  const parsed = new URL(url);
  if (parsed.username || parsed.password) throw new Error("Media URL credentials are not allowed");
  // Own the timer to avoid the SDK timeout logger including signed URL paths.
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let guarded: Awaited<ReturnType<typeof fetchWithSsrFGuard>> | undefined;
  try {
    guarded = await fetchWithSsrFGuard({
      url, requireHttps: true, signal: controller.signal, maxRedirects: 3,
      mode: "strict", pinDns: true, capture: false, init: { headers },
    });
    if (!guarded.response.ok) throw new ApiError("Fetch media", guarded.response.status);
    return { buffer: await readBoundedResponse(guarded.response, maxBytes), headers: guarded.response.headers, url: guarded.finalUrl };
  } finally { clearTimeout(timer); await guarded?.release(); }
}

export function assertResource(space: string, value: string | undefined, kind: "messages" | "threads"): void {
  if (value !== undefined && !new RegExp(`^${space}/${kind}/[A-Za-z0-9_.-]+$`).test(value)) {
    throw new Error(`Invalid ${kind} resource for destination space`);
  }
}
