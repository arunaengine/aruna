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
        .layer(from_fn_with_state(state.clone(), mcp_auth))
        .layer(from_fn_with_state(
            state.clone(),
            crate::rate_limit::principal_middleware,
        ))
        .layer(from_fn_with_state(
            state.clone(),
            crate::auth::auth_middleware,
        ))
        .layer(from_fn_with_state(
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
