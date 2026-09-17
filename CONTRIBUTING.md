# Contributing

The runtime entrypoint is the built `dist/index.js`. Source lives in `index.ts` and `src/`. Keep the built entrypoint in sync; OpenClaw loads JavaScript from the package rather than transpiling this source checkout.

## Local checks

```bash
npm ci --ignore-scripts
npm run typecheck
npm test
npm run test:pack
```

`npm test` builds first and runs Node's test runner. Tests use local mock servers, a pinned OpenClaw SDK and synthetic credentials/identifiers; no Google credentials are needed. See [test/README.md](test/README.md). The development dependency pins OpenClaw 2026.8.2 so SDK behavior is repeatable. Package consumers use their own compatible host.

Security changes need regressions covering the public callback or delivery path where possible. Do not bypass network/file guards in a security test. Avoid raw message text, credential values, file contents, token responses and private URLs in fixtures, logs and PR descriptions; use synthetic markers to verify redaction.

Update both configuration schemas in `openclaw.plugin.json` and the README when behavior changes. The [CI template](.github/verify-workflow.yml.example) verifies typechecking, tests, reproducible `dist/index.js`, and package contents/registration. A maintainer can enable it by copying it to `.github/workflows/ci.yml`; publishing that file requires GitHub workflow permission. Do not publish a release just to test a PR.

## Live checks and pull requests

Use a dedicated Chat space and Pub/Sub subscription, separate from production. Validate passive event receipt, app-auth scopes/admin approval, routing, sender/command policy, thread replies, one actual renewal, and restart behavior. Test enabled media/reaction features separately under user OAuth. Include the exact OpenClaw/Node versions and distinguish mocks from real integration evidence.

Keep changes focused and explain the triggering problem, resulting behavior, migration requirements and tests. A coordinated authentication/security change may span several paths; keep commits reviewable. If live access is unavailable, open a draft PR and list outstanding integration checks explicitly.

Use conventional commit messages (`fix:`, `feat:`, `test:`, `docs:`). By contributing, you agree to the repository's MIT license.
