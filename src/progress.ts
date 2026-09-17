export interface ProgressConfig {
  enabled?: boolean;
  updateIntervalSeconds?: number;
  maxRecentActivities?: number;
  includeDetails?: boolean;
}

interface ToolEvent {
  itemId?: string;
  toolCallId?: string;
  name?: string;
  phase?: string;
  args?: Record<string, unknown>;
}
interface ItemEvent extends ToolEvent {
  kind?: string;
  status?: string;
}
type Activity = { label: string; detail?: string; status: string };

// Only explicitly selected display fields reach Chat. Never use item meta/title,
// command/code, tool results, or model reasoning as a fallback for missing titles.
function displayText(value: unknown): string | undefined {
  if (typeof value !== "string") return;
  const text = value
    .replace(/-----BEGIN [\s\S]*?(?:-----END [^\n]+-----|$)/g, "[redacted]")
    .replace(/\b(?:Bearer|Basic)\s+\S+/gi, "[redacted]")
    .replace(/\b(?:password|passwd|secret|(?:access_|refresh_|api_)?token|api[_-]?key|authorization)\b["']?\s*[:=]\s*(?:"[^"]*"|'[^']*'|\S+)/gi, "[redacted]")
    .replace(/\b(?:sk-[\w-]{16,}|gh[pousr]_[\w]{20,}|github_pat_[\w]{20,}|AIza[\w-]{20,}|ya29\.[\w.-]+)/g, "[redacted]")
    .replace(/https?:\/\/\S+/gi, "[link]")
    .replace(/<[^>]*>/g, "")
    .replace(/[<>*_~`\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/g, " ")
    .replace(/\s+/g, " ").trim();
  return text ? Array.from(text).slice(0, 160).join("") : undefined;
}

function toolLabel(name = ""): string {
  const short = name.split(/[.:/]/).pop()!.toLowerCase();
  if (["exec", "exec_command", "shell", "bash", "run_command"].includes(short)) return "Commands";
  const known: Record<string, string> = {
    memory_search: "Memory Search", memory_get: "Read Memory", read: "Read File",
    read_file: "Read File", write: "Write File", edit: "Edit File", apply_patch: "Edit File",
    web_search: "Web Search", web_fetch: "Fetch Page", tool_search: "Search Tools",
  };
  if (known[short]) return known[short];
  return /^[a-z][a-z0-9_-]{0,39}$/.test(short)
    ? short.replace(/[_-]+/g, " ").replace(/\b\w/g, c => c.toUpperCase()) : "Other Tools";
}

/** One bounded activity draft per run. Final delivery must await stop() before
 * editing/deleting the placeholder, so a slow progress PATCH cannot overwrite it. */
export class ActivityProgress {
  private activities = new Map<string, Activity>();
  private counts = new Map<string, number>();
  private recent: Activity[] = [];
  private sequence = 0;
  private timer?: ReturnType<typeof setTimeout>;
  private inFlight?: Promise<void>;
  private stopped = false;
  private visible = false;
  private lastText = "";
  private lastEdit = Date.now();
  private readonly abort = () => { void this.stop(); };

  constructor(private readonly options: {
    update: (text: string) => Promise<void>;
    onError: (error: unknown) => void;
    intervalMs: number;
    maxRecent: number;
    includeDetails: boolean;
    signal?: AbortSignal;
  }) {
    if (options.signal?.aborted) this.stopped = true;
    else options.signal?.addEventListener("abort", this.abort, { once: true });
  }

  onToolStart(event: ToolEvent): boolean {
    return this.record(event, false);
  }

  onItemEvent(event: ItemEvent): boolean {
    if (event.kind !== "tool" && event.kind !== "command") return false;
    return this.record(event, true);
  }

  private record(event: ItemEvent, item: boolean): boolean {
    if (this.stopped) return false;
    const id = event.toolCallId || event.itemId?.replace(/^(?:tool|command):/, "");
    // Item and tool callbacks can describe the same invocation. Anonymous item
    // updates cannot be correlated safely; count anonymous tool starts only.
    if (!id && (item || (event.phase && event.phase !== "start"))) return false;
    const key = id || `anonymous:${++this.sequence}`;
    let activity = this.activities.get(key);
    if (!activity) {
      if (this.activities.size >= 10000) return false;
      let label = toolLabel(event.name || (event.kind === "command" ? "exec" : ""));
      if (!this.counts.has(label) && this.counts.size >= 8) label = "Other Tools";
      activity = { label, status: "running" };
      this.activities.set(key, activity);
      this.counts.set(label, (this.counts.get(label) || 0) + 1);
      this.recent.push(activity);
      if (this.recent.length > this.options.maxRecent) this.recent.shift();
    }
    if (!item && this.options.includeDetails) {
      const detail = displayText(event.args?.title) ||
        (["Memory Search", "Web Search", "Search Tools"].includes(activity.label) ? displayText(event.args?.query) : undefined);
      if (detail) activity.detail = detail;
    }
    if (["failed", "error", "blocked", "cancelled", "canceled"].includes(event.status || "")) activity.status = "failed";
    else if (["completed", "complete", "done"].includes(event.status || "")) activity.status = "completed";
    this.schedule();
    // Queuing an edit is not visible delivery. Tell the SDK only after a PATCH
    // has actually succeeded; counters describe invocations, not successes.
    return this.visible;
  }

  private render(): string {
    const counts = [...this.counts].map(([label, count]) => `${label} ×${count}`).join(" · ");
    const lines = this.recent.map(a => `${a.status === "completed" ? "✓" : a.status === "failed" ? "!" : "…"} ${a.label}${a.detail ? ` — ${a.detail}` : ""}`);
    return ["_OpenClaw is working…_", counts, ...lines].filter(Boolean).join("\n");
  }

  private schedule(): void {
    if (this.stopped || this.timer || this.inFlight || this.lastText === this.render()) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      if (this.stopped) return;
      const text = this.render();
      if (text === this.lastText) return;
      this.inFlight = (async () => {
        try {
          await this.options.update(text);
          this.lastText = text;
          this.visible = true;
        } catch (error) {
          // Progress is best effort: no retries or replacement status posts.
          this.stopped = true;
          this.options.onError(error);
        } finally {
          this.lastEdit = Date.now();
          this.inFlight = undefined;
          this.schedule();
        }
      })();
    }, Math.max(0, this.options.intervalMs - (Date.now() - this.lastEdit)));
    this.timer.unref();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    this.options.signal?.removeEventListener("abort", this.abort);
    await this.inFlight;
  }
}
