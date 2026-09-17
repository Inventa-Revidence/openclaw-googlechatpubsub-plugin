# Regression tests

Run `npm ci --ignore-scripts`, `npm run typecheck`, `npm test` and `npm run test:pack`. The test script rebuilds first. `npm run test:coverage` produces Node's whole-bundle coverage report. Tests require permission to bind ephemeral localhost ports; no Google access or real credentials are needed.

The harness loads the built `dist/index.js` into a temporary module and exports otherwise-private functions for tests. It substitutes the Google Chat/Pub/Sub URL origins, stubs two route/envelope helpers, and supplies a logger/configuration. It **retains real OpenClaw filesystem, SSRF and locking helpers** and the real GoogleAuth client construction. Routing context is captured at the dispatcher boundary; these are not full gateway/LLM integration tests.

Older binary/multipart tests register local file-server fixtures. Only those explicit fixture origins bypass the SSRF guard to exercise content formatting over local HTTP. Security tests do not register those hosts: they exercise the actual guard with injected DNS/transport responses, including private addresses, rebinding, cross-origin credential stripping, pinned dispatchers, and redirects. No guard bypass exists in production code.

The suite covers sender/command admission, destination restrictions, media defaults, filesystem escapes, bounded streaming, token-client scope separation and refresh coalescing, durable admission before ACK, cross-process restart/locking, deduplication, storage failure, uncertain agent outcomes, nonoverlapping polling, cooperative timeout settlement, log redaction, and Google subscription lifecycle operations. Existing binary-integrity/upload/caption/self-echo regressions remain; expectations that formerly required sensitive error details now require redaction.

Activity regressions feed the real dispatcher callback payload shapes into the plugin with mocked Chat responses. They cover deduplication, throttling, bounded recent activities, selected/redacted display fields, space/thread routing, a slow edit racing final delivery, cancellation, silent/error cleanup, API failures, and opt-in configuration. Virtual timers make edit-rate assertions deterministic. These do not run a real model or Google Chat UI.

`test:pack` checks the tarball's exact file allowlist and runtime bytes and loads its real module with OpenClaw 2026.8.2 to register a disabled service. Live Google consent/admin approval, real token refresh, event delivery, expiry/renewal and a running gateway remain staging checks. A passing mocked suite is not proof of production compatibility.
