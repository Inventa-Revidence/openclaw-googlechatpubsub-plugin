import { GoogleAuth } from "google-auth-library";

export const AUTH_SCOPES = {
  chat: "https://www.googleapis.com/auth/chat.bot",
  events: "https://www.googleapis.com/auth/chat.app.messages.readonly",
  pubsub: "https://www.googleapis.com/auth/pubsub",
} as const;

/** Separate clients keep app, event and Cloud IAM scopes from being mixed. */
export class ServiceAccountTokens {
  private clients = new Map<string, GoogleAuth>();
  private pending = new Map<string, Promise<string>>();
  constructor(private keyFile: string) {}

  token(purpose: keyof typeof AUTH_SCOPES): Promise<string> {
    const existing = this.pending.get(purpose);
    if (existing) return existing;
    let auth = this.clients.get(purpose);
    if (!auth) {
      auth = new GoogleAuth({
        keyFile: this.keyFile, scopes: [AUTH_SCOPES[purpose]],
        clientOptions: { transporterOptions: { timeout: 15_000, retryConfig: { retry: 2 } } },
      });
      this.clients.set(purpose, auth);
    }
    const pending = auth.getAccessToken().then(token => {
      if (!token) throw new Error("Service account returned no access token");
      return token;
    }).finally(() => { this.pending.delete(purpose); });
    this.pending.set(purpose, pending);
    return pending;
  }
}
