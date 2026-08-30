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

Every tool declaration states what the tool does, when to use it instead of its neighbour, what it returns, and the call that supplies its ids. Every input property carries a description with the exact format, one example, where the value comes from, and its default and bounds. Ids follow two shapes: group, document, and job ids are bare 26-character ULIDs, while a user id is `<ulid>@<realm>`.

Context:

- `whoami`: Return the caller id, display name when available, realm roles, and group roles.
- `list_groups`: List the caller's groups and assigned roles. It supplies the `group_id` the write tools need.
- `get_group`: Read one realm group by ULID with its display name and full role list.
- `list_group_members`: List a group's members and the roles that assign them. Membership required.
- `get_group_usage`: Read a group's local and realm-wide storage usage, document counts, and quota status. Membership required.
- `get_realm_info`: Describe the realm, its nodes, OIDC providers, quota configuration, and endpoints.
- `get_node_info`: Describe this node's version, capabilities, addresses, and service status.

Data:

- `list_buckets`: List readable buckets on this node. It supplies the bucket names the other data tools need.
- `list_objects`: List objects by bucket, key prefix, and cursor. Default 100, at most 200.
- `read_object`: Read at most 1 MiB of UTF-8 object text from an optional offset. Binary content is refused.
- `write_object`: Replace an object with at most 1 MiB of UTF-8 text.
- `search`: Search documents, buckets, groups, and users through unified search, ten hits per section.

Metadata:

- `list_profiles`: List visible Profile documents under `profiles/` with summaries. A Profile's conformsTo IRI is `https://w3id.org/aruna/profile/<document_id>`.
- `get_profile`: Read a Profile raw crate and its embedded SHACL Turtle text.
- `search_datasets`: Search datasets by text, exact conformance IRI, and group. Default 25 hits, at most 100.
- `get_dataset`: Read a dataset's raw accepted crate and projection state.
- `validate_dataset`: Preview structural and Profile validation without writing anything.
- `create_dataset`: Create a document from an RO-Crate at a `group/path` document path.
- `replace_dataset`: Replace a document's whole RO-Crate and optional visibility.
- `sparql_query`: Run a SELECT or ASK query over one document, or over all visible metadata when it is an ASK or a single-pattern SELECT DISTINCT.
- `find_references`: Find visible metadata documents that reference an absolute IRI. Default 25, at most 100.

Compute:

- `list_runtimes`: List the pinned Python, Deno, and Bash quick-run runtimes.
- `run_script`: Stage a script and its dependencies into an existing bucket, then submit it through native jobs.
- `submit_job`: Submit the complete native execution request shape.
- `get_job`: Return state, timestamps, result, and bounded stdout and stderr tails.
- `list_jobs`: List owned jobs, newest first, with optional group, state, and limit filters. Default 50, at most 200.
- `cancel_job`: Request cancellation of an owned job. Cancellation is a request, not an immediate stop.

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

The MCP layer does not grant access. Each tool also uses the permission and canonical path used by the corresponding REST or S3 operation. Directory reads such as `whoami`, `list_groups`, `get_group`, `list_group_members`, `get_group_usage`, `get_realm_info`, and `get_node_info` carry no permission path in REST either, so they are gated by the realm request policies and by the membership rule the route enforces itself.

Every tool declares an `outputSchema` of `type: "object"`. Tools that return a REST response shape declare a permissive object schema rather than the response type, because that shape is not generated from a schema.

## Errors

A refusal carries the REST status code and error code of the mirrored route, and the message names the field, the expected format, and the next call. A malformed id answers with the required ULID shape and the tool that lists valid ids rather than a bare `Bad request`, and a missing bucket, document, or job says which listing tool to call. Structured metadata violations and Profile findings stay in `structured_content` next to the message.

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
