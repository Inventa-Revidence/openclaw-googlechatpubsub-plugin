// The OpenClaw package exposes this SDK entry at runtime without TypeScript declarations.
declare module "openclaw/plugin-sdk/file-lock" {
  export function acquireFileLock(
    path: string,
    options: {
      retries: { retries: number; factor: number; minTimeout: number; maxTimeout: number };
      stale: number;
      staleRecovery: "remove-if-unchanged";
    },
  ): Promise<{ release: () => Promise<void> }>;
}
