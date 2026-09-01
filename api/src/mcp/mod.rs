use crate::auth::require_unrestricted_realm_auth;
use crate::cors::CorsConfig;
use crate::server_state::ServerState;
use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use rmcp::ServerHandler;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::model::{Implementation, ServerCapabilities, ServerInfo};
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::streamable_http_server::tower::{
    StreamableHttpServerConfig, StreamableHttpService,
};
use std::collections::BTreeMap;
use std::sync::Arc;

mod compute;
mod context;
mod data;
mod metadata;
mod prompts;
mod resources;

const MCP_BODY_LIMIT: usize = 4 * 1024 * 1024;

/// A bare `serde_json::Value` carries the JSON Schema `true`, which MCP clients
/// reject wherever a schema object is required. This wrapper serializes
/// identically and declares an object schema instead.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct JsonPayload(pub serde_json::Value);

impl rmcp::schemars::JsonSchema for JsonPayload {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        "JsonPayload".into()
    }

    fn json_schema(_: &mut rmcp::schemars::SchemaGenerator) -> rmcp::schemars::Schema {
        rmcp::schemars::json_schema!({ "type": "object", "additionalProperties": true })
    }
}

#[derive(Clone, Debug)]
pub struct McpServer {
    pub(crate) state: Arc<ServerState>,
    tool_router: ToolRouter<Self>,
}

impl McpServer {
    pub fn new(state: Arc<ServerState>) -> Self {
        let mut tool_router = context::toolset();
        tool_router.merge(compute::toolset());
        tool_router.merge(data::toolset());
        tool_router.merge(metadata::toolset());
        Self { state, tool_router }
    }
}

#[rmcp::tool_handler(router = self.tool_router)]
impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
        )
        .with_server_info(Implementation::new("aruna", env!("CARGO_PKG_VERSION")))
    }

    async fn list_resources(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::ListResourcesResult, rmcp::ErrorData> {
        resources::list_resources(self, context).await
    }

    async fn list_resource_templates(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::ListResourceTemplatesResult, rmcp::ErrorData> {
        resources::list_templates(self, context).await
    }

    async fn read_resource(
        &self,
        request: rmcp::model::ReadResourceRequestParams,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::ReadResourceResponse, rmcp::ErrorData> {
        resources::read_resource(self, request, context).await
    }

    async fn list_prompts(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::ListPromptsResult, rmcp::ErrorData> {
        prompts::list_prompts(self, context).await
    }

    async fn get_prompt(
        &self,
        request: rmcp::model::GetPromptRequestParams,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::GetPromptResponse, rmcp::ErrorData> {
        prompts::get_prompt(self, request, context).await
    }
}

pub(crate) fn request_auth(
    parts: &http::request::Parts,
) -> Result<aruna_core::structs::AuthContext, CallToolResult> {
    parts
        .extensions
        .get::<Option<aruna_core::structs::AuthContext>>()
        .cloned()
        .flatten()
        .ok_or_else(|| server_error(crate::error::ServerError::Unauthorized))
}

pub(crate) fn empty_extras(tool: &str) -> aruna_operations::request_policy::PolicyRequestExtras {
    aruna_operations::request_policy::PolicyRequestExtras {
        operation: format!("mcp:{tool}"),
        params: BTreeMap::new(),
        headers: BTreeMap::new(),
        body: None,
    }
}

pub(crate) fn tool_extras<T: serde::Serialize>(
    tool: &str,
    arguments: &T,
) -> Result<aruna_operations::request_policy::PolicyRequestExtras, CallToolResult> {
    let value = serde_json::to_value(arguments)
        .map_err(|error| internal_error(format!("failed to encode tool arguments: {error}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| internal_error("tool arguments did not encode as an object"))?;
    let params = object
        .iter()
        .map(|(key, value)| {
            let value = value
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string());
            (key.clone(), value)
        })
        .collect();
    Ok(aruna_operations::request_policy::PolicyRequestExtras {
        operation: format!("mcp:{tool}"),
        params,
        headers: BTreeMap::new(),
        body: None,
    })
}

pub(crate) fn server_error(error: crate::error::ServerError) -> CallToolResult {
    error_result(error.response_body())
}

pub(crate) fn internal_error(message: impl std::fmt::Display) -> CallToolResult {
    server_error(crate::error::ServerError::InternalError(
        message.to_string(),
    ))
}

pub(crate) fn bad_request(message: impl std::fmt::Display) -> CallToolResult {
    server_error(crate::error::ServerError::BadRequestReason(
        message.to_string(),
    ))
}

/// REST answers a tool caller with bare text such as "Not found" or "Bad
/// request". The status code is kept while the message gains the reason.
pub(crate) fn explained(
    error: crate::error::ServerError,
    reason: impl std::fmt::Display,
) -> CallToolResult {
    let mut body = error.response_body();
    body.error = format!("{}: {reason}", body.error);
    error_result(body)
}

/// Group ids are the bare canonical ULID form, unlike a user id, which is
/// `<ulid>@<realm>`.
pub(crate) fn parse_ulid(
    field: &str,
    value: &str,
    source: &str,
) -> Result<ulid::Ulid, CallToolResult> {
    ulid::Ulid::from_string(value).map_err(|_| {
        bad_request(format!(
            "{field} must be a 26-character ULID such as 01JZ8Y6T0K4W7M2N9Q5R3S8V1X; {source}"
        ))
    })
}

pub(crate) async fn authorize_tool(
    state: &ServerState,
    auth: &aruna_core::structs::AuthContext,
    path: String,
    permission: aruna_core::structs::Permission,
    extras: aruna_operations::request_policy::PolicyRequestExtras,
) -> Result<(), crate::error::ServerError> {
    crate::auth::ensure_permission_with(state, auth, path, permission, extras).await
}

/// Self-scoped tools mirror REST routes that carry no permission path: only
/// the realm deny policies apply, and unreadable policy state refuses.
pub(crate) async fn authorize_self(
    state: &ServerState,
    auth: &aruna_core::structs::AuthContext,
    permission: aruna_core::structs::Permission,
    extras: aruna_operations::request_policy::PolicyRequestExtras,
) -> Result<(), crate::error::ServerError> {
    let realm_id = state.get_realm_id();
    let request = aruna_operations::request_policy::policy_request_with(
        &format!("/{realm_id}/u/{}", auth.user_id),
        &permission,
        Some(auth),
        extras,
    );
    aruna_operations::request_policy::enforce_policies(&state.get_ctx(), realm_id, &request)
        .await
        .map_err(|_| crate::error::ServerError::Forbidden)
}

fn error_result(body: crate::error::ErrorResponse) -> CallToolResult {
    let text = body.error.clone();
    let mut result = CallToolResult::error(vec![ContentBlock::text(text)]);
    result.structured_content = serde_json::to_value(body).ok();
    result
}

pub fn router(state: Arc<ServerState>, cors: &CorsConfig, api_public_url: Option<&str>) -> Router {
    let config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_allowed_hosts(allowed_hosts(api_public_url))
        .with_allowed_origins(cors.mcp_origins())
        .with_max_request_body_bytes(MCP_BODY_LIMIT)
        .with_cancellation_token(state.shutdown_token());
    let handler_state = state.clone();
    let service: StreamableHttpService<McpServer, NeverSessionManager> = StreamableHttpService::new(
        move || Ok(McpServer::new(handler_state.clone())),
        Arc::new(NeverSessionManager::default()),
        config,
    );

    Router::new()
        .nest_service("/mcp", service)
        .route_layer(from_fn_with_state(state.clone(), mcp_auth))
        .route_layer(from_fn_with_state(
            state.clone(),
            crate::rate_limit::principal_middleware,
        ))
        .route_layer(from_fn_with_state(
            state.clone(),
            crate::auth::auth_middleware,
        ))
        .route_layer(from_fn_with_state(
            state,
            crate::telemetry::request_tracing_middleware,
        ))
}

fn allowed_hosts(api_public_url: Option<&str>) -> Vec<String> {
    let mut hosts = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    if let Some(host) = api_public_url
        .and_then(|value| url::Url::parse(value).ok())
        .and_then(|url| url.host_str().map(str::to_string))
        && !hosts.contains(&host)
    {
        hosts.push(host);
    }
    hosts
}

async fn mcp_auth(State(state): State<Arc<ServerState>>, request: Request, next: Next) -> Response {
    let auth = request
        .extensions()
        .get::<Option<aruna_core::structs::AuthContext>>()
        .cloned()
        .flatten();
    match require_unrestricted_realm_auth(&state, auth) {
        Ok(_) => next.run(request).await,
        Err(crate::error::ServerError::Unauthorized) => auth_error(StatusCode::UNAUTHORIZED),
        Err(_) => auth_error(StatusCode::FORBIDDEN),
    }
}

fn auth_error(status: StatusCode) -> Response {
    let mut response = status.into_response();
    if status == StatusCode::UNAUTHORIZED {
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    }
    response
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    fn all_tools() -> Vec<rmcp::model::Tool> {
        let mut router = super::context::toolset();
        router.merge(super::compute::toolset());
        router.merge(super::data::toolset());
        router.merge(super::metadata::toolset());
        router.list_all()
    }

    fn is_schema_object(value: &Value) -> bool {
        value.as_object().is_some_and(|object| {
            ["type", "$ref", "anyOf", "oneOf", "allOf", "enum", "const"]
                .iter()
                .any(|key| object.contains_key(*key))
        })
    }

    fn check_schema(schema: &Value, label: &str) {
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (name, property) in properties {
                assert!(
                    is_schema_object(property),
                    "{label} property {name} is not a schema object: {property}"
                );
            }
        }
        for keyword in ["$defs", "definitions"] {
            let Some(entries) = schema.get(keyword).and_then(Value::as_object) else {
                continue;
            };
            for (name, entry) in entries {
                assert!(
                    is_schema_object(entry),
                    "{label} {keyword} {name} is not a schema object: {entry}"
                );
                check_schema(entry, label);
            }
        }
    }

    fn collect_missing(schema: &Value, path: &str, missing: &mut Vec<String>) {
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (name, property) in properties {
                let description = property.get("description").and_then(Value::as_str);
                if description.is_none_or(|text| text.trim().is_empty()) {
                    missing.push(format!("{path}.{name}"));
                }
            }
        }
        for keyword in ["$defs", "definitions"] {
            let Some(entries) = schema.get(keyword).and_then(Value::as_object) else {
                continue;
            };
            for (name, entry) in entries {
                collect_missing(entry, &format!("{path}/{name}"), missing);
            }
        }
    }

    fn error_body(result: rmcp::model::CallToolResult) -> Value {
        result
            .structured_content
            .expect("a tool error carries the structured body")
    }

    #[test]
    fn errors_carry_reasons() {
        // "Bad request" and "Not found" alone leave the caller nothing to fix.
        let body =
            error_body(super::parse_ulid("group_id", "nope", "call list_groups").unwrap_err());
        assert_eq!(body["code"], "Bad request");
        let text = body["error"].as_str().unwrap_or_default();
        assert!(text.contains("group_id"), "{text}");
        assert!(text.contains("26-character ULID"), "{text}");
        assert!(text.contains("call list_groups"), "{text}");

        let body = error_body(super::explained(
            crate::error::ServerError::NotFound,
            "call list_buckets",
        ));
        assert_eq!(body["code"], "Not found");
        let text = body["error"].as_str().unwrap_or_default();
        assert_eq!(text, "Not found: call list_buckets");
    }

    #[test]
    fn inputs_are_described() {
        // Without a per-property description the calling model guesses id
        // formats, path shapes, and bounds, and the tool answers Bad request.
        let mut missing = Vec::new();
        for tool in all_tools() {
            let name = tool.name.as_ref();
            assert!(
                tool.description
                    .as_ref()
                    .is_some_and(|text| !text.trim().is_empty()),
                "{name} has no tool description"
            );
            let input = Value::Object((*tool.input_schema).clone());
            collect_missing(&input, name, &mut missing);
        }
        assert!(
            missing.is_empty(),
            "MCP input properties without a description: {missing:?}"
        );
    }

    #[test]
    fn schemas_are_objects() {
        // The portal MCP client validates tools/list and drops every tool when
        // one schema is not an object schema.
        for tool in all_tools() {
            let name = tool.name.as_ref();
            let input = Value::Object((*tool.input_schema).clone());
            assert_eq!(
                input.get("type").and_then(Value::as_str),
                Some("object"),
                "{name} inputSchema is not type object: {input}"
            );
            check_schema(&input, &format!("{name} inputSchema"));
            let Some(output) = tool.output_schema else {
                continue;
            };
            let output = Value::Object((*output).clone());
            assert_eq!(
                output.get("type").and_then(Value::as_str),
                Some("object"),
                "{name} outputSchema is not type object: {output}"
            );
            check_schema(&output, &format!("{name} outputSchema"));
        }
    }
}
