# googlechatpubsub — OpenClaw channel plugin

Listen to selected Google Chat spaces through Workspace Events and Cloud Pub/Sub without requiring an @mention. Messages are admitted by space and sender policy, saved to a durable inbox, and routed to OpenClaw agents. Replies use the Chat API. Keyword routing, thread replies and thread session isolation are supported.

**Beta.** This revision adds service-account app authentication and changes the admission/media defaults. Automated tests use OpenClaw **2026.8.2** and mocked Google services. Live app approval, event delivery and renewal must be validated in a test space before production deployment. The older visual setup guide describes the previous OAuth-only configuration.

## Authentication

| Operation | `service-account` | `user-oauth` |
| --- | --- | --- |
| Workspace Events create/renew/reactivate | App, `chat.app.messages.readonly` | Consenting user, `chat.messages.readonly` or `chat.messages` |
| Subscription inspection and operation polling | App, `chat.bot` | Consenting user |
| Pub/Sub pull/ACK | Service account, `pubsub` + IAM | Consenting user, `pubsub` + IAM |
| Text replies | Chat app, `chat.bot` | Chat app, `chat.bot` |
| Inbound uploaded attachments (opt in) | Chat app | Consenting user |
| Uploads and reaction actions (opt in) | Rejected at startup | Consenting user |

Scope names in the table have the prefix `https://www.googleapis.com/auth/`. The service-account mode does not use user refresh tokens or impersonate a Workspace user. Domain-wide delegation is not implemented. Separate Google authentication clients cache the app, Events and Pub/Sub tokens; concurrent refresh requests are coalesced.

Google supports app-authenticated subscriptions to spaces the app belongs to, with administrator approval for `chat.app` scopes. See [subscription creation](https://developers.google.com/workspace/events/reference/rest/v1/subscriptions/create) and [app authentication](https://developers.google.com/workspace/chat/authenticate-authorize-chat-app).

## Private app setup

1. Use a Google Workspace organization and a Cloud project with Chat, Workspace Events and Pub/Sub APIs enabled. Configure the Chat app for your internal audience and add it to each intended space.
2. Follow Google's [administrator approval procedure](https://developers.google.com/workspace/chat/authenticate-authorize-chat-app#receive_administrator_approval): create a Marketplace-compatible OAuth client for the service account, configure the Marketplace SDK with private visibility, and obtain approval for `chat.app.messages.readonly`. Do not add `chat.bot` to the Marketplace SDK scope list. Approval does not require making the app public.
3. Create a dedicated Pub/Sub topic and pull subscription in the same Cloud project as the Workspace Events subscription.
4. Grant `chat-api-push@system.gserviceaccount.com` `roles/pubsub.publisher` on that topic. Restrict other publishers: message sender IDs are trusted because they arrive through this controlled topic; the plugin cannot authenticate an arbitrary forged Pub/Sub publisher.
5. Grant the consuming service account `roles/pubsub.subscriber` on the specific Pub/Sub subscription. Configure one active plugin consumer and persistent local storage for its inbox. Cloud IAM access and Workspace app approval are separate requirements.
6. Mount the service-account JSON as a regular, nonsymlink file readable only by the gateway user (`0600` on POSIX). Keep it out of source control and Chat. This version requires a key file; workload identity is not implemented.

See [Pub/Sub access control](https://cloud.google.com/pubsub/docs/access-control). For `403` failures, check the relevant scope/admin approval, app membership, and IAM binding rather than broadening the service account to project Owner.

## Installation and configuration

Use OpenClaw 2026.8.2 or later and a Node version supported by that host. Development is pinned to OpenClaw 2026.8.2; later host versions need their own compatibility check.

For a reviewed source checkout:

```bash
npm ci --ignore-scripts
npm run typecheck
npm test
npm pack
```

Install the resulting tarball with your host's plugin installer. Verify the exact commit and package checksum, retain the artifact for rollback, and load only one copy of plugin ID `googlechatpubsub`. A tarball pins the plugin code; also record the host and resolved runtime dependency versions during staging. Installing the upstream npm package does not select an unmerged PR revision.

A minimal service-account configuration (all IDs and paths below are placeholders):

```json
{
  "plugins": { "allow": ["googlechatpubsub"] },
  "channels": {
    "googlechatpubsub": {
      "enabled": true,
      "auth": { "mode": "service-account" },
      "projectId": "example-project",
      "topicId": "openclaw-chat-events",
      "subscriptionId": "openclaw-chat-events-sub",
      "serviceAccountFile": "/run/secrets/chat-service-account.json",
      "allowFrom": ["users/123456789012345678901"],
      "commandAllowFrom": [],
      "silentReply": true,
      "crossAgentDispatch": false,
      "reactionsEnabled": false,
      "media": { "inboundEnabled": false, "outboundEnabled": false },
      "bindings": [
        {
          "space": "spaces/ABC123example",
          "replyInThread": true,
          "threadSessionIsolation": true,
          "agents": [{ "agentId": "main", "alwaysListen": true }]
        }
      ]
    }
  },
  "bindings": [
    { "agentId": "main", "match": { "channel": "googlechatpubsub", "accountId": "main" } }
  ]
}
```

Merge these keys into existing configuration; preserve other plugins and bindings. OpenClaw's top-level agent bindings are separate from this plugin's space/keyword bindings. For multiple agents, add corresponding top-level bindings and use `mentionKeyword`, `alwaysListen`, or both within each space. Use stable numeric Chat `users/<id>` resource names from the Chat API, not email addresses or display names.

### Reply in the space or in a thread

Set these fields on the relevant `channels.googlechatpubsub.bindings[]` entry to post answers directly in the space and share one conversation session across that space:

```json
{
  "replyInThread": false,
  "threadSessionIsolation": false
}
```

`replyInThread: false` is the default. It omits thread routing from outgoing messages even when the incoming event, OpenClaw, or a message action supplies a thread ID. This applies to text, attachments, caption fallbacks, typing placeholders and later reply chunks. Older versions still followed incoming thread IDs when this setting was false; install this revision to get space-level replies.

Google represents a new space-level post as the start of a new thread; the bot's answer appears in the main space conversation instead of as a nested reply. The plugin does not reuse the thread returned for a typing placeholder when space-level mode is enabled. See Google's [message reply options](https://developers.google.com/workspace/chat/api/reference/rest/v1/spaces.messages/create#messagereplyoption).

Set `replyInThread: true` to keep replies within the incoming thread. `threadSessionIsolation` controls agent conversation history independently and defaults to the value of `replyInThread`. Existing configurations that explicitly set it to true must also change it to false if one shared space session is wanted. Queued jobs use the current binding's reply/session settings when processing resumes.

### Admission and command policy

- Only `google.workspace.chat.message.v1.created` events from configured spaces and human senders are considered. Message/thread names must belong to that space. Message text is limited to 32 KiB; the encoded event is limited to 90,000 characters.
- Every bound space must have a nonempty `allowFrom` list or explicitly opt into `allowAllSpaceMembers: true`. Global policies can be overridden per binding. A per-space list replaces its global list, including an empty list. An explicitly false per-space membership flag overrides a global true flag.
- `commandAllowFrom` is a separate, default-empty list. A user must first pass message admission to receive command authority. OpenClaw's own command, owner, tool and sandbox restrictions still apply. Ordinary admitted messages can still request agent work; choose those agent tools deliberately.
- All plugin send/upload/reaction callbacks restrict destinations to configured spaces and validate thread/message relationships. These checks do not constrain other plugins or tools installed in the gateway.
- Self messages and known outbound message IDs are suppressed. Cross-agent dispatch is opt in and carries no human command authorization; all child runs must settle before the parent completes.

### Media

Both inbound downloads and outbound uploads default to **disabled**. Reaction actions also default to disabled; automatic receipt reactions are no longer emitted.

For user-OAuth uploads, enable `media.outboundEnabled`. Local reads require either an OpenClaw-supplied authorized media reader or an explicit `media.localRoots` list of absolute directories. The plugin's own reader rejects traversal, symlinks, hardlinks and oversized files. A host reader remains subject to the host's access policy and the plugin's post-read byte limit. Do not allow credential/state directories as media roots.

Remote uploads additionally require `media.allowRemoteUrls`. They accept HTTPS, with DNS pinning and destination checks on every redirect. Private, loopback and link-local destinations are rejected. Credentials are stripped on cross-origin redirects. Response bodies are streamed with a byte cap and a timeout covering the full read. Proxy environment settings do not bypass these checks.

| Setting | Default | Range |
| --- | --- | --- |
| `media.maxBytes` | 20 MiB per file | 1 byte–200 MiB |
| `media.maxAttachments` | 5 per message | 1–10 |
| `media.retentionHours` | 24 | 1–168 |
| `media.localRoots` | `[]` | Absolute directories |
| `media.allowRemoteUrls` | `false` | Boolean |

Inbound uploads are saved under the host state directory's `media/inbound/googlechatpubsub/`, with random filenames and mode `0600`. Only this plugin's generated files are pruned, on download, startup and periodic maintenance. Retention is enforced while the listener runs; stopped gateways require external cleanup. Drive links are not downloaded. Per-file limits and retention are not a total disk quota; use a bounded persistent volume and monitor free space when enabling media. Agent transcripts and copies made by other tools have separate retention.

### Existing OAuth installations

Configurations with an `oauth` block and no `auth.mode` continue to select `user-oauth`. Otherwise the default is `service-account`. Set the mode explicitly when migrating. The service account is still required for bot text replies in OAuth mode.

Keep `oauth.clientId`, `oauth.clientSecret`, `oauth.tokensFile` and optional `oauth.redirectUri`. The token file must contain a valid `refresh_token` for the selected user's consent grant; access tokens are cached only in memory and rotated refresh tokens are written atomically. The consenting user must belong to the selected spaces and have Pub/Sub Subscriber access. Use `chat.messages` for uploads, `chat.messages.reactions` for reaction actions, and `pubsub` for pull/ACK. Follow Google's [user authorization guide](https://developers.google.com/workspace/chat/authenticate-authorize-chat-user) for consent and refresh-token acquisition.

Before enabling an existing installation:

1. Add explicit sender admission and optional command users. Previous unconditional command authorization is removed.
2. Opt into any needed media or reaction features. Configure local roots; arbitrary filesystem reads and HTTP URLs are no longer accepted.
3. Protect the service-account key, OAuth token file, configuration and state backups. OAuth client secrets remain in your configuration unless your host provides a compatible secret mechanism.
4. Stop the old listener before switching. Old in-memory queued work was not durable. Subscription discovery locates a matching server-side subscription even if the old state file is absent.

## Delivery, recovery and subscriptions

The listener atomically saves **all routed jobs before Pub/Sub ACK**. Files are flushed and renamed with restrictive permissions under `<OpenClaw state>/googlechatpubsub/`. Use local persistent storage with working atomic rename/fsync semantics; ephemeral storage loses acknowledged work. A host lock prevents two processes from owning the same inbox. A shared network filesystem or multi-host consumer group is not supported.

One durable worker processes admitted jobs. Polls do not overlap. The default inbox capacity is 200 unresolved jobs (`maxPendingMessages`, 1–1000); failed jobs count toward capacity. A full inbox or failed admission write prevents ACK, allowing Pub/Sub redelivery. Completion removes message content and keeps up to roughly 2,000 durable deduplication records plus currently outstanding jobs. Very old redeliveries beyond that bounded history can execute again.

Pending jobs resume after restart and sender admission is rechecked. Pre-dispatch failures retry up to three times with backoff. Once an agent may have run tools or delivered a reply, a failure or interruption becomes a `failed` record requiring operator review. Such work is never automatically replayed. Agent timeouts request cancellation and wait for actual settlement; a host run that ignores cancellation blocks the worker and shutdown until it settles. This avoids starting overlapping work after a timer fires.

**Recovery procedure:** stop the gateway, back up `inbox.json`, and inspect failed records plus the relevant Chat thread and agent/tool history. If side effects completed, mark that record `done` and remove its `payload`. Only after confirming replay is appropriate, change it to `pending` with `nextAttempt: 0` and `attempts: 0`. Preserve IDs, timestamps, state-file version and other records; keep mode `0600`. Restart and verify the outcome. Never delete the whole inbox to clear an error. Retain failed payloads only as long as needed for incident review; they are not automatically expired. This is recoverable delivery, not exactly-once tool execution.

Workspace subscriptions are inspected once per minute, discovered by event type/target/topic and full resource payload, and renewed with `PATCH` before the server's actual expiry. Create/renew/reactivate unwrap and, when needed, poll Google Operation responses. Expired/missing subscriptions are rediscovered or created; suspended subscriptions are reactivated. Outages that exceed subscription lifetime can create event gaps; this plugin does not backfill historical Chat messages. See [listing](https://developers.google.com/workspace/events/reference/rest/v1/subscriptions/list), [renewal](https://developers.google.com/workspace/events/reference/rest/v1/subscriptions/patch) and [operation polling](https://developers.google.com/workspace/events/reference/rest/v1/operations/get).

| Listener setting | Default | Range |
| --- | --- | --- |
| `pollIntervalSeconds` | 3 | 1–300 |
| `renewalBufferMinutes` | 30 | 1–60 |
| `agentTimeoutSeconds` | 60 | 10–3600 |
| `maxPendingMessages` | 200 | 1–1000 |
| `silentReply` | `true` | Boolean |

Logs omit message bodies, remote URLs, credentials and raw API error responses. They include resource/session identifiers, operation classifications and HTTP status where available. OpenClaw's network guard can log blocked destination origins (without paths or query strings); agent transcripts have their own logging policy. Monitor inbox failures/backpressure, token/permission errors, subscription maintenance failures, and disk usage.

## Validation and rollout

See [CONTRIBUTING.md](CONTRIBUTING.md) and [test/README.md](test/README.md) for automated tests and their mock boundaries. Before production, validate the intended OpenClaw/Node versions, private-app approval, a real subscription create and renewal, allowed/denied senders, thread replies, a restart, and credential revocation in a dedicated test space. Start with text, one agent and one space. Back up the current artifact, configuration and inbox before upgrades; rollback must preserve pending work.

## License

MIT. See [LICENSE](LICENSE).
