use super::metadata::{IdInput, load_raw, request_bearer};
use super::{McpServer, request_auth};
use rmcp::model::{
    ErrorData, ListResourceTemplatesResult, ListResourcesResult, ReadResourceRequestParams,
    ReadResourceResponse, ReadResourceResult, Resource, ResourceContents, ResourceTemplate,
};
use rmcp::service::{RequestContext, RoleServer};

const PROFILE_PREFIX: &str = "aruna://profiles/";
const PROFILE_TEMPLATE: &str = "aruna://profiles/{id}";
const DOCS_URI: &str = "aruna://docs/metadata-profiles";
const PROFILE_DOCS: &str = include_str!("../../../docs/metadata-profiles.md");

pub(crate) async fn list_resources(
    _server: &McpServer,
    _context: RequestContext<RoleServer>,
) -> Result<ListResourcesResult, ErrorData> {
    Ok(ListResourcesResult::with_all_items(vec![
        Resource::new(DOCS_URI, "metadata-profiles")
            .with_title("Aruna metadata profiles")
            .with_description("Aruna Profile authoring and validation documentation")
            .with_mime_type("text/markdown")
            .with_size(PROFILE_DOCS.len() as u64),
    ]))
}

pub(crate) async fn list_templates(
    _server: &McpServer,
    _context: RequestContext<RoleServer>,
) -> Result<ListResourceTemplatesResult, ErrorData> {
    Ok(ListResourceTemplatesResult::with_all_items(vec![
        ResourceTemplate::new(PROFILE_TEMPLATE, "profile")
            .with_title("Aruna Profile RO-Crate")
            .with_description("Raw Profile crate including embedded SHACL Turtle")
            .with_mime_type("application/json"),
    ]))
}

pub(crate) async fn read_resource(
    server: &McpServer,
    request: ReadResourceRequestParams,
    context: RequestContext<RoleServer>,
) -> Result<ReadResourceResponse, ErrorData> {
    if request.uri == DOCS_URI {
        return Ok(ReadResourceResult::new(vec![
            ResourceContents::text(PROFILE_DOCS, DOCS_URI).with_mime_type("text/markdown"),
        ])
        .into());
    }
    let Some(id) = request.uri.strip_prefix(PROFILE_PREFIX) else {
        return Err(ErrorData::resource_not_found("resource not found", None));
    };
    if id.is_empty() || id.contains('/') {
        return Err(ErrorData::resource_not_found("resource not found", None));
    }
    let parts = context
        .extensions
        .get::<http::request::Parts>()
        .ok_or_else(|| ErrorData::internal_error("HTTP request context is missing", None))?;
    let auth = request_auth(parts).map_err(resource_error)?;
    let (record, value) = load_raw(
        server,
        &auth,
        request_bearer(parts),
        &IdInput { id: id.to_string() },
        "get_profile",
    )
    .await
    .map_err(resource_error)?;
    if !record.document_path.starts_with("profiles/") {
        return Err(ErrorData::resource_not_found("resource not found", None));
    }
    let text = serde_json::to_string_pretty(&value).map_err(|error| {
        ErrorData::internal_error(format!("failed to encode Profile resource: {error}"), None)
    })?;
    Ok(ReadResourceResult::new(vec![
        ResourceContents::text(text, request.uri).with_mime_type("application/json"),
    ])
    .into())
}

fn resource_error(error: rmcp::model::CallToolResult) -> ErrorData {
    let data = error.structured_content;
    if data
        .as_ref()
        .and_then(|value| value.get("code"))
        .and_then(serde_json::Value::as_str)
        == Some("Not found")
    {
        return ErrorData::resource_not_found("resource not found", data);
    }
    ErrorData::internal_error("resource read failed", data)
}
