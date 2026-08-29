# Model Context Protocol

Aruna exposes an authenticated MCP Streamable HTTP endpoint at `<node-base-url>/mcp`. It uses the MCP `2026-07-28` stateless protocol. The endpoint is outside `/api/v1` and shares the API listener, request tracing, bearer validation, and rate limits.

Set `MCP=enabled` or `MCP=disabled`. The default is `enabled`. When enabled, `GET /api/v1/info/realm` advertises the endpoint as `interfaces.mcp.url`.

## Authentication

Every request requires an existing Aruna realm bearer token:

```http
Authorization: Bearer <token>
```

Anonymous requests receive HTTP 401 with `WWW-Authenticate: Bearer` before JSON-RPC dispatch. Tokens issued by another realm and path-restricted tokens are refused. Tokens are never returned by MCP tools or written to logs.

The MCP transport accepts `Mcp-Method`, `Mcp-Name`, `MCP-Protocol-Version`, and `Mcp-Session-Id` in browser preflight requests. The protocol itself is stateless and does not create MCP sessions.

## Tools

Context:

- `whoami`: Return the caller id, display name when available, realm roles, and group roles.
- `list_groups`: List the caller's groups and assigned roles.

Data:

- `list_buckets`: List readable buckets on this node.
- `list_objects`: List at most 200 objects by bucket, prefix, and cursor.
- `read_object`: Read at most 1 MiB of UTF-8 object text from an optional offset.
- `write_object`: Write at most 1 MiB of UTF-8 text through the native object operation.
- `search`: Search documents, buckets, groups, and users through unified search.

Metadata:

- `list_profiles`: List visible Profile documents under `profiles/` with summaries.
- `get_profile`: Read a Profile raw crate and its embedded SHACL Turtle text.
- `search_datasets`: Search datasets by text, Profile conformance, and group.
- `get_dataset`: Read a dataset's raw accepted crate and projection state.
- `validate_dataset`: Preview structural and Profile validation without writing.
- `create_dataset`: Create a dataset from an RO-Crate.
- `replace_dataset`: Replace a dataset's RO-Crate and optional visibility.
- `sparql_query`: Run a bounded SELECT or ASK query globally or against one document.
- `find_references`: Find visible metadata documents that reference an IRI.

Compute:

- `list_runtimes`: List the pinned Python, Deno, and Bash quick-run runtimes.
- `run_script`: Stage a script and dependencies, then submit it through native jobs.
- `submit_job`: Submit the complete native execution request shape.
- `get_job`: Return state, timestamps, result, and bounded stdout and stderr tails.
- `list_jobs`: List owned jobs with optional group, state, and limit filters.
- `cancel_job`: Request cancellation of an owned job.

Resources:

- `aruna://profiles/{id}`: Raw Profile crate JSON.
- `aruna://docs/metadata-profiles`: Embedded metadata Profile documentation.

Prompt:

- `create-dataset(group_id, profile_id?)`: Guide a validate, repair, and create loop using Aruna RO-Crate conventions.

## Request policies

Every tool authorization supplies `request.operation = "mcp:<tool>"`, string-valued tool arguments in `request.params`, empty headers, and no body. Existing deny-only CEL request policies can block the entire MCP surface:

```cel
request.operation.startsWith("mcp:")
```

The MCP layer does not grant access. Each tool also uses the permission and canonical path used by the corresponding REST or S3 operation.

## Client configuration

Claude Code:

```bash
claude mcp add --transport http aruna https://node.example.test/mcp --header "Authorization: Bearer <token>"
```

Cursor or VS Code:

```json
{
  "servers": {
    "aruna": {
      "type": "http",
      "url": "https://node.example.test/mcp",
      "headers": {
        "Authorization": "Bearer <token>"
      }
    }
  }
}
```

Codex CLI in `config.toml`:

```toml
[mcp_servers.aruna]
url = "https://node.example.test/mcp"
http_headers = { Authorization = "Bearer <token>" }
```

Gemini CLI in `settings.json`:

```json
{
  "mcpServers": {
    "aruna": {
      "httpUrl": "https://node.example.test/mcp",
      "headers": {
        "Authorization": "Bearer <token>"
      }
    }
  }
}
```
