# Assistant sessions and providers

Aruna supports user-managed bearer sessions and node-side assistant providers. Provider credentials
belong to one user, remain sealed with the issuing node's credential key, and are never returned by
the API after creation.

## User sessions

Every bearer minted by the user session API has a ULID session id and one of these kinds:
`portal`, `assistant`, or `api`.

- `POST /api/v1/users/sessions` creates a session from an unrestricted bearer. Its lifetime is the
  shortest of the requested lifetime, the parent bearer's remaining lifetime, and 24 hours. The
  token is returned only once.
- `GET /api/v1/users/sessions` lists records held by the issuing node. `current` identifies the
  session used for that request.
- `DELETE /api/v1/users/sessions/{session_id}` is idempotent and revokes only a session owned by the
  caller.

Revocation writes the token hash to the realm's existing replicated revocation set. The local
session record retains `revoked: true` for listing, while the replicated set is the authority that
stops the token on every realm node. `GET /api/v1/users/token` keeps its `{ "token": ... }` response
and records the issued bearer as a `portal` session.

## Portal provider boundary

The portal has three provider paths:

- Claude BYOK calls Claude directly from the browser. The user-supplied key is kept in
  `sessionStorage` and never sent through an Aruna provider route.
- OpenAI-compatible BYOK calls the configured endpoint directly from the browser. The official
  OpenAI preset uses Responses; local and custom roots may select Responses or Chat Completions.
- Codex device login remains node-managed, sealed, and proxied because the production Codex
  endpoint blocks browser origins.

Direct BYOK keys never pass through provider routes or Aruna storage. Direct local HTTP endpoints
require loopback CSP/CORS admission (`localhost`, `127.0.0.1`, or `::1`); arbitrary LAN HTTP is not
admitted. The node provider API documented below remains a separate contract.

## Assistant providers

Provider routes require an unrestricted realm bearer and are self-scoped under
`/api/v1/system/assistant/providers`.

| Kind | Default base URL |
| --- | --- |
| `anthropic` | `https://api.anthropic.com` |
| `openai` | `https://api.openai.com` |
| `openrouter` | `https://openrouter.ai/api` |
| `openai_compatible` | No default; `base_url` is required |
| `chatgpt` | `https://chatgpt.com/backend-api/codex` |

The collection supports `GET` and `POST`. A provider resource supports `PATCH` and `DELETE`.
Summaries include ids, labels, models, defaults, creation time, and status. They never contain API
keys, custom header values, access tokens, refresh tokens, or ChatGPT account ids.

`GET /{id}/models` requests `/v1/models` for API providers and removes model ids containing
`embed`, `whisper`, `tts`, `image`, `dall-e`, `audio`, `moderation`, or `realtime`. ChatGPT returns
the backend's model list when it answers, else the static set `gpt-5.6-sol`, `gpt-5.6-luna`,
`gpt-5.6-terra`, `gpt-5.5`, `gpt-5.4`, `gpt-5.3-codex`, `gpt-5`. `POST /{id}/test` performs the same
models request, or checks ChatGPT token freshness, and returns only an `ok` flag and a safe message.

On server nodes a provider URL must use HTTPS and a public host. Loopback, private, link-local,
unique-local, `localhost`, and `.local` hosts are rejected when a provider is created or patched and
again before forwarding. A User-kind device node may use private or loopback HTTP endpoints for
local services such as Ollama, LM Studio, and vLLM.

## Proxy contract

The proxy is available at
`/api/v1/system/assistant/providers/{id}/proxy/{path}`. Only these method and path combinations are
accepted:

| Kind | Requests |
| --- | --- |
| `anthropic` | `POST /v1/messages`, `GET /v1/models` |
| `openai`, `openrouter`, `openai_compatible` | `POST /v1/chat/completions`, `POST /v1/responses`, `GET /v1/models` |
| `chatgpt` | `POST /responses` |

Inbound bodies are limited to 4 MiB. `authorization`, `x-api-key`, `cookie`, `host`,
`content-length`, and hop-by-hop headers are removed. Stored custom headers are added, followed by
provider authentication. Anthropic receives `x-api-key` and `anthropic-version: 2023-06-01`.
OpenAI-shaped providers receive a bearer header. ChatGPT receives its access bearer,
`chatgpt-account-id`, and a fresh UUID `session_id`; its JSON body always has `store: false`.

The upstream response status, safe end-to-end headers, and body stream are passed through. Response
bodies, including SSE, are not buffered. There is a connect timeout but no overall response read
timeout, so long model streams can remain open.

## ChatGPT subscription login

Start login with `POST /api/v1/system/assistant/providers/chatgpt/login`. The response contains the
provider id, user code, verification URL, polling interval, and expiry. The provider remains
`pending_login`. The portal calls `POST /api/v1/system/assistant/providers/{id}/login/poll` once per
interval; each call performs at most one upstream poll and returns `pending`, `ready`, `expired`, or
`denied`.

When login becomes ready, the node seals the access token, refresh token, and account id. An access
token older than eight days is refreshed before use. A proxy response with status 401 triggers one
refresh and one retry.

The Codex client id used by this flow is OpenAI's and may stop working.

## Configuration

`ASSISTANT_PROXY=enabled|disabled` controls every provider, login, models, test, and proxy route.
The default is `enabled`. When disabled these routes return 404 with code
`assistant_proxy_disabled`.

## CEL session attribute

Request policies can inspect `request.session.sid`, `request.session.kind`, and
`request.session.label`. The label is currently empty in the signed request context. Anonymous and
unbound legacy requests expose empty session fields. For example, this deny policy blocks assistant
sessions on REST, S3, and other transports that use the shared authorization boundary:

```cel
request.session.kind == "assistant"
```
