/// <reference path="./openclaw-file-lock.d.ts" />

import { readFileSync, existsSync, lstatSync } from "node:fs";
import { acquireFileLock } from "openclaw/plugin-sdk/file-lock";
import { atomicJson } from "./safety.js";

export interface InboxRecord<T> {
  id: string;
  state: "pending" | "running" | "failed" | "done";
  payload?: T;
  createdAt: number;
  updatedAt: number;
  attempts: number;
  nextAttempt: number;
  reason?: string;
}

/** Single-process, bounded, durable admission journal. Unknown side effects never auto-replay. */
export class DurableInbox<T> {
  private records: InboxRecord<T>[];
  constructor(private path: string, private maxPending = 200, private maxHistory = 2000) {
    if (existsSync(path)) {
      const stat = lstatSync(path);
      if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 128 * 1024 * 1024) throw new Error("Unsafe inbox file");
      const saved = JSON.parse(readFileSync(path, "utf8"));
      if (saved.version !== 1 || !Array.isArray(saved.records)) throw new Error("Unsupported inbox state; refusing to discard pending work");
      this.records = saved.records;
      const ids = new Set<string>();
      for (const r of this.records) {
        if (!r || typeof r.id !== "string" || !r.id || ids.has(r.id)
          || !["pending", "running", "failed", "done"].includes(r.state)
          || (r.state !== "done" && (r.payload === undefined || r.payload === null))
          || !Number.isInteger(r.attempts) || r.attempts < 0
          || !Number.isFinite(r.nextAttempt) || !Number.isFinite(r.createdAt) || !Number.isFinite(r.updatedAt)) throw new Error("Invalid inbox record");
        ids.add(r.id);
        if (r.state === "running") { r.state = "failed"; r.reason = "interrupted; delivery or tool side effects may have occurred"; }
      }
      this.commit(this.records);
    } else { this.records = []; }
  }
  private commit(records: InboxRecord<T>[]): void {
    atomicJson(this.path, { version: 1, records });
    this.records = records;
  }
  admit(jobs: Array<{ id: string; payload: T }>): void {
    const next = structuredClone(this.records);
    const seen = new Set(next.map(r => r.id));
    const added: InboxRecord<T>[] = [];
    for (const job of jobs) {
      if (seen.has(job.id)) continue;
      seen.add(job.id);
      added.push({ ...job, state: "pending", createdAt: Date.now(), updatedAt: Date.now(), attempts: 0, nextAttempt: 0 });
    }
    if (!added.length) return;
    if (next.filter(r => r.state !== "done").length + added.length > this.maxPending) throw new Error("Inbox full; apply backpressure without ACK");
    const live = next.filter(r => r.state !== "done");
    const done = next.filter(r => r.state === "done").slice(-this.maxHistory);
    this.commit([...done, ...live, ...added]);
  }
  ready(now = Date.now()): InboxRecord<T> | undefined {
    return structuredClone(this.records.find(r => r.state === "pending" && r.nextAttempt <= now));
  }
  update(id: string, patch: Partial<InboxRecord<T>>): void {
    const next = structuredClone(this.records);
    const record = next.find(r => r.id === id);
    if (!record) throw new Error("Unknown inbox record");
    Object.assign(record, patch, { updatedAt: Date.now() });
    if (record.state === "done") delete record.payload;
    this.commit(next);
  }
  snapshot(): InboxRecord<T>[] { return structuredClone(this.records); }
}

/** The host lock helper verifies owner identity and stale sidecars before reclamation. */
export async function lockInbox(path: string): Promise<() => Promise<void>> {
  const lock = await acquireFileLock(path, {
    retries: { retries: 0, factor: 1, minTimeout: 1, maxTimeout: 1 },
    stale: Number.MAX_SAFE_INTEGER, staleRecovery: "remove-if-unchanged",
  });
  return lock.release;
}
