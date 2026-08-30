use super::data::{ReadObjectInput, read_text};
use super::{
    JsonPayload, McpServer, authorize_tool, bad_request, empty_extras, internal_error,
    request_auth, server_error, tool_extras,
};
use aruna_core::StructuredId;
use aruna_core::structs::{
    Actor, AuthContext, MetadataRegistryRecord, Permission, WatchEvent, WatchEventDetail,
    WatchEventKind,
};
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload, mint_forward_document, mint_local_document,
};
use aruna_operations::metadata::api::{
    ExportMetadataRoCrateRequest, MetadataDocumentQueryRequest, MetadataQueryRequest,
    MetadataReferencesRequest, MetadataRoCrateExportView, MetadataSearchRequest, load_realm_config,
    query_metadata, query_metadata_document, references_metadata, search_metadata,
};
use aruna_operations::metadata::forward::{
    create_metadata_document_routed, export_rocrate_routed, is_user_origin,
    update_metadata_document_routed,
};
use aruna_operations::metadata::profile_validation::preview_submission;
use aruna_operations::notifications::watch::emit::emit_resource_watch_event;
use aruna_operations::update_metadata_document::UpdateMetadataDocumentMutation;
use rmcp::Json;
use rmcp::handler::server::tool::Extension;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use ulid::Ulid;

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct IdInput {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct DatasetSearchInput {
    pub q: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conforms_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ValidateInput {
    pub rocrate: JsonPayload,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct CreateDatasetInput {
    pub group_id: String,
    pub path: String,
    pub rocrate: JsonPayload,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ReplaceDatasetInput {
    pub id: String,
    pub rocrate: JsonPayload,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct SparqlInput {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ReferencesInput {
    pub iri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

pub(crate) fn toolset() -> rmcp::handler::server::router::tool::ToolRouter<McpServer> {
    McpServer::metadata_router()
}

#[tool_router(router = metadata_router)]
impl McpServer {
    #[tool(
        description = "List visible Aruna Profile documents stored under profiles/ with RO-Crate summaries",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn list_profiles(
        &self,
        Extension(parts): Extension<http::request::Parts>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        metadata_probe(self, &auth, "list_profiles", empty_extras("list_profiles")).await?;
        let response = crate::routes::metadata::run_list_metadata_documents(
            &self.state,
            Some(auth.clone()),
            crate::routes::metadata::ListMetadataQuery {
                group_id: None,
                path_prefix: Some("profiles/".to_string()),
                include: Some("summary".to_string()),
                limit: Some(1_000),
                offset: Some(0),
                order: None,
            },
            None,
        )
        .await
        .map_err(server_error)?;
        for document in &response.documents {
            authorize_summary(
                self,
                &auth,
                &document.group_id,
                &document.document_path,
                &document.document_id,
                "list_profiles",
                empty_extras("list_profiles"),
            )
            .await?;
        }
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "Get a Profile's raw RO-Crate together with embedded SHACL Turtle text",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn get_profile(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<IdInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let bearer = request_bearer(&parts);
        let (record, crate_value) = load_raw(self, &auth, bearer, &input, "get_profile").await?;
        if !record.document_path.starts_with("profiles/") {
            return Err(server_error(crate::error::ServerError::NotFound));
        }
        let mut turtle = Vec::new();
        let mut artifact_urls = Vec::new();
        collect_turtle(&crate_value, &mut turtle, &mut artifact_urls);
        for artifact_url in artifact_urls {
            let (bucket, key) = artifact_location(&artifact_url).ok_or_else(|| {
                bad_request(format!(
                    "Profile Turtle contentUrl is not an S3 object URL: {artifact_url}"
                ))
            })?;
            let artifact = read_text(
                self,
                &auth,
                ReadObjectInput {
                    bucket,
                    key,
                    offset: None,
                    max_bytes: Some(1024 * 1024),
                },
                tool_extras("get_profile", &input)?,
            )
            .await?;
            if artifact.truncated {
                return Err(server_error(crate::error::ServerError::PayloadTooLarge(
                    "Profile Turtle exceeds 1 MiB".to_string(),
                )));
            }
            if !turtle.iter().any(|text| text == &artifact.text) {
                turtle.push(artifact.text);
            }
        }
        Ok(Json(JsonPayload(json!({
            "profile": crate_value,
            "shacl_turtle": turtle,
        }))))
    }

    #[tool(
        description = "Search visible dataset metadata by text, Profile conformance, and group",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn search_datasets(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<DatasetSearchInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("search_datasets", &input)?;
        let group_id = input
            .group_id
            .as_deref()
            .map(crate::auth::parse_group_id)
            .transpose()
            .map_err(server_error)?;
        match group_id {
            Some(group_id) => {
                authorize_tool(
                    &self.state,
                    &auth,
                    metadata_group_path(self, group_id),
                    Permission::READ,
                    extras,
                )
                .await
                .map_err(server_error)?;
            }
            None => metadata_probe(self, &auth, "search_datasets", extras).await?,
        }
        let result = search_metadata(
            self.state.get_ctx().as_ref(),
            self.state.get_realm_id(),
            self.state.get_node_id(),
            MetadataSearchRequest {
                auth: Some(auth),
                bearer_token: request_bearer(&parts).map(|carrier| carrier.as_str().to_string()),
                graph_iris: None,
                query: input.q,
                conforms_to: input.conforms_to,
                group_id,
                limit: input.limit,
                cursor: None,
                mode: None,
                target_nodes: None,
            },
        )
        .await
        .map_err(crate::routes::metadata::map_metadata_api_error)
        .map_err(server_error)?;
        let response = crate::routes::metadata::MetadataSearchResponse {
            hits: result
                .hits
                .into_iter()
                .map(crate::routes::metadata::map_search_hit)
                .collect(),
            next_cursor: result.next_cursor,
            nodes_queried: result.fanout_stats.nodes_queried,
            nodes_failed: result.fanout_stats.nodes_failed,
            truncated: result.truncated,
        };
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "Get a dataset's raw accepted RO-Crate revision and projection state",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn get_dataset(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<IdInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let (_, value) =
            load_raw(self, &auth, request_bearer(&parts), &input, "get_dataset").await?;
        Ok(Json(JsonPayload(value)))
    }

    #[tool(
        description = "Validate a draft RO-Crate with the same structural and Profile verdict as metadata writes without storing it",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn validate_dataset(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<ValidateInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        metadata_probe(
            self,
            &auth,
            "validate_dataset",
            tool_extras("validate_dataset", &input)?,
        )
        .await?;
        let jsonld = crate::routes::metadata::serialize_jsonld_object(&input.rocrate.0)
            .map_err(server_error)?;
        let preview = preview_submission(&self.state.get_ctx(), &jsonld)
            .await
            .map_err(crate::routes::metadata::map_metadata_error)
            .map_err(server_error)?;
        let response = crate::routes::metadata::ProfileValidationPreviewResponse::from(preview);
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "Create a dataset from a validated RO-Crate and return its accepted registry summary",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn create_dataset(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<CreateDatasetInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("create_dataset", &input)?;
        let group_id = crate::auth::parse_group_id(&input.group_id).map_err(server_error)?;
        let path = MetadataRegistryRecord::normalize_document_path(&input.path);
        if path.is_empty() {
            return Err(bad_request("dataset path must not be empty"));
        }
        let jsonld = crate::routes::metadata::serialize_jsonld_object(&input.rocrate.0)
            .map_err(server_error)?;
        let ctx = self.state.get_ctx();
        let realm = load_realm_config(ctx.as_ref(), self.state.get_realm_id())
            .await
            .ok_or_else(|| server_error(crate::error::ServerError::ServiceUnavailable))?;
        let actor = Actor {
            node_id: self.state.get_node_id(),
            user_id: auth.user_id,
            realm_id: self.state.get_realm_id(),
        };
        let user_origin = is_user_origin(&ctx, self.state.get_realm_id(), self.state.get_node_id())
            .await
            .map_err(crate::routes::metadata::map_metadata_api_error)
            .map_err(server_error)?;
        let document_id = if user_origin {
            mint_forward_document(&realm, &actor, group_id, &path)
                .map_err(crate::routes::metadata::map_create_error)
                .map_err(server_error)?
                .as_ulid()
        } else {
            match mint_local_document(&realm, &actor, group_id, &path) {
                Ok(document_id) => document_id.as_ulid(),
                Err(CreateMetadataDocumentError::OriginHoldsNoBucket) => {
                    mint_forward_document(&realm, &actor, group_id, &path)
                        .map_err(crate::routes::metadata::map_create_error)
                        .map_err(server_error)?
                        .as_ulid()
                }
                Err(error) => {
                    return Err(server_error(crate::routes::metadata::map_create_error(
                        error,
                    )));
                }
            }
        };
        authorize_tool(
            &self.state,
            &auth,
            metadata_group_path(self, group_id),
            Permission::WRITE,
            extras.clone(),
        )
        .await
        .map_err(server_error)?;
        authorize_tool(
            &self.state,
            &auth,
            MetadataRegistryRecord::permission_path_for(
                &auth.realm_id,
                group_id,
                &path,
                document_id,
            ),
            Permission::WRITE,
            extras,
        )
        .await
        .map_err(server_error)?;
        let created = create_metadata_document_routed(
            CreateMetadataDocumentOperation::new_for_generated_document_id(
                CreateMetadataDocumentConfig {
                    actor,
                    group_id,
                    document_id,
                    document_path: path,
                    public: input.public.unwrap_or(false),
                    payload: CreateMetadataDocumentPayload::RoCrate { jsonld },
                },
            ),
            ctx.clone(),
            crate::routes::metadata::forwarded_auth_token(request_bearer(&parts))
                .map_err(server_error)?,
        )
        .await
        .map_err(crate::routes::metadata::map_metadata_write_error)
        .map_err(server_error)?;
        let record = created.record;
        emit_resource_watch_event(
            ctx.as_ref(),
            WatchEvent {
                event_id: Ulid::generate(),
                realm_id: self.state.get_realm_id(),
                kind: WatchEventKind::MetadataCreated,
                path: format!("meta/{}/{}", record.group_id, record.document_path),
                actor: auth.user_id,
                occurred_at_ms: unix_timestamp_millis(),
                detail: WatchEventDetail::MetadataCreated {
                    group_id: record.group_id,
                    document_id: record.document_id,
                },
            },
        )
        .await;
        let summary = crate::routes::metadata::MetadataDocumentSummary::from(&record);
        Ok(Json(JsonPayload(
            serde_json::to_value(summary).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "Replace a dataset's RO-Crate and optionally change its visibility",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn replace_dataset(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<ReplaceDatasetInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let document_id =
            crate::routes::metadata::parse_document_id(&input.id).map_err(server_error)?;
        let jsonld = crate::routes::metadata::serialize_jsonld_object(&input.rocrate.0)
            .map_err(server_error)?;
        let extras = tool_extras("replace_dataset", &input)?;
        let record = crate::routes::metadata::local_write_record(
            &self.state,
            &auth,
            document_id,
            extras.clone(),
        )
        .await
        .map_err(server_error)?;
        if record.is_none() {
            metadata_probe(self, &auth, "replace_dataset", extras).await?;
        }
        let updated = update_metadata_document_routed(
            &self.state.get_ctx(),
            Actor {
                node_id: self.state.get_node_id(),
                user_id: auth.user_id,
                realm_id: self.state.get_realm_id(),
            },
            record.as_ref(),
            document_id,
            input.public,
            UpdateMetadataDocumentMutation::ReplaceRoCrate { jsonld },
            crate::routes::metadata::forwarded_auth_token(request_bearer(&parts))
                .map_err(server_error)?,
        )
        .await
        .map_err(crate::routes::metadata::map_metadata_write_error)
        .map_err(server_error)?;
        let summary = crate::routes::metadata::MetadataDocumentSummary::from(&updated);
        Ok(Json(JsonPayload(
            serde_json::to_value(summary).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "Run a bounded SELECT or ASK SPARQL query over visible metadata or one document",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn sparql_query(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<SparqlInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("sparql_query", &input)?;
        let bearer = request_bearer(&parts).map(|carrier| carrier.as_str().to_string());
        let execution = if let Some(document_id) = input.document_id.as_deref() {
            let document_id =
                crate::routes::metadata::parse_document_id(document_id).map_err(server_error)?;
            let record =
                crate::routes::metadata::load_metadata_record_by_document(&self.state, document_id)
                    .await
                    .map_err(server_error)?;
            authorize_tool(
                &self.state,
                &auth,
                record.permission_path,
                Permission::READ,
                extras,
            )
            .await
            .map_err(server_error)?;
            query_metadata_document(
                self.state.get_ctx().as_ref(),
                self.state.get_realm_id(),
                self.state.get_node_id(),
                MetadataDocumentQueryRequest {
                    document_id,
                    auth: Some(auth.clone()),
                    bearer_token: bearer,
                    query: input.query,
                    mode: None,
                    allow_partial: true,
                },
            )
            .await
        } else {
            metadata_probe(self, &auth, "sparql_query", extras).await?;
            query_metadata(
                self.state.get_ctx().as_ref(),
                self.state.get_realm_id(),
                self.state.get_node_id(),
                MetadataQueryRequest {
                    auth: Some(auth),
                    bearer_token: bearer,
                    graph_iris: None,
                    query: input.query,
                    mode: None,
                    target_nodes: None,
                    allow_partial: true,
                },
            )
            .await
        }
        .map_err(crate::routes::metadata::map_metadata_api_error)
        .map_err(server_error)?;
        let response =
            crate::routes::metadata::map_query_results(execution.results, execution.fanout_stats)
                .map_err(server_error)?;
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "Find visible metadata documents that reference an absolute IRI",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn find_references(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<ReferencesInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        metadata_probe(
            self,
            &auth,
            "find_references",
            tool_extras("find_references", &input)?,
        )
        .await?;
        let execution = references_metadata(
            self.state.get_ctx().as_ref(),
            self.state.get_realm_id(),
            MetadataReferencesRequest {
                auth: Some(auth),
                iri: input.iri,
                predicate: input.predicate,
                limit: input.limit,
                resolve: false,
            },
        )
        .await
        .map_err(crate::routes::metadata::map_metadata_api_error)
        .map_err(server_error)?;
        let response = crate::routes::metadata::map_references_response(execution);
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }
}

pub(crate) fn request_bearer(
    parts: &http::request::Parts,
) -> Option<crate::auth::ValidatedArunaBearerTokenCarrier> {
    parts
        .extensions
        .get::<Option<crate::auth::ValidatedArunaBearerTokenCarrier>>()
        .cloned()
        .flatten()
}

async fn metadata_probe(
    server: &McpServer,
    auth: &AuthContext,
    _tool: &str,
    extras: aruna_operations::request_policy::PolicyRequestExtras,
) -> Result<(), CallToolResult> {
    super::authorize_self(&server.state, auth, Permission::READ, extras)
        .await
        .map_err(server_error)
}

fn metadata_group_path(server: &McpServer, group_id: Ulid) -> String {
    format!("/{}/g/{group_id}/meta/**", server.state.get_realm_id())
}

async fn authorize_summary(
    server: &McpServer,
    auth: &AuthContext,
    group_id: &str,
    path: &str,
    document_id: &str,
    _tool: &str,
    extras: aruna_operations::request_policy::PolicyRequestExtras,
) -> Result<(), CallToolResult> {
    let group_id = crate::auth::parse_group_id(group_id).map_err(server_error)?;
    let document_id =
        crate::routes::metadata::parse_document_id(document_id).map_err(server_error)?;
    authorize_tool(
        &server.state,
        auth,
        MetadataRegistryRecord::permission_path_for(
            &server.state.get_realm_id(),
            group_id,
            path,
            document_id,
        ),
        Permission::READ,
        extras,
    )
    .await
    .map_err(server_error)
}

pub(crate) async fn load_raw(
    server: &McpServer,
    auth: &AuthContext,
    bearer: Option<crate::auth::ValidatedArunaBearerTokenCarrier>,
    input: &IdInput,
    tool: &str,
) -> Result<(MetadataRegistryRecord, Value), CallToolResult> {
    let document_id =
        crate::routes::metadata::parse_document_id(&input.id).map_err(server_error)?;
    let record =
        crate::routes::metadata::load_metadata_record_by_document(&server.state, document_id)
            .await
            .map_err(server_error)?;
    authorize_tool(
        &server.state,
        auth,
        record.permission_path.clone(),
        Permission::READ,
        tool_extras(tool, input)?,
    )
    .await
    .map_err(server_error)?;
    let params = crate::routes::metadata::MetadataRoCrateExportParams {
        view: Some(crate::routes::metadata::MetadataRoCrateView::Raw),
        limit: None,
        offset: None,
        after: None,
    };
    let export = export_rocrate_routed(
        &server.state.get_ctx(),
        server.state.get_realm_id(),
        ExportMetadataRoCrateRequest {
            document_id,
            auth: Some(auth.clone()),
            view: MetadataRoCrateExportView::Raw,
            limit: None,
            offset: None,
            after: None,
        },
        crate::routes::metadata::forwarded_auth_token(bearer).map_err(server_error)?,
        server.state.rocrate_limits().metadata_bytes,
    )
    .await
    .map_err(crate::routes::metadata::map_metadata_api_error)
    .map_err(server_error)?;
    let response = crate::routes::metadata::map_rocrate_export_response(
        export,
        &params,
        crate::routes::metadata::MetadataRoCrateView::Raw,
    )
    .map_err(server_error)?;
    Ok((
        record,
        serde_json::to_value(response).map_err(internal_error)?,
    ))
}

fn collect_turtle(value: &Value, turtle: &mut Vec<String>, artifact_urls: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            let is_turtle = object
                .get("encodingFormat")
                .is_some_and(|value| value.to_string().contains("text/turtle"));
            if is_turtle
                && let Some(text) = object.get("text").and_then(Value::as_str)
                && !turtle.iter().any(|existing| existing == text)
            {
                turtle.push(text.to_string());
            }
            if is_turtle
                && object.get("text").and_then(Value::as_str).is_none()
                && let Some(url) = object.get("contentUrl").and_then(resource_id)
                && !artifact_urls.iter().any(|existing| existing == url)
            {
                artifact_urls.push(url.to_string());
            }
            for value in object.values() {
                collect_turtle(value, turtle, artifact_urls);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_turtle(value, turtle, artifact_urls);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn resource_id(value: &Value) -> Option<&str> {
    value
        .as_str()
        .or_else(|| value.as_object()?.get("@id")?.as_str())
}

fn artifact_location(value: &str) -> Option<(String, String)> {
    let url = url::Url::parse(value).ok()?;
    if url.scheme() == "s3" {
        let bucket = url.host_str()?.to_string();
        let key = url.path().trim_start_matches('/').to_string();
        return (!key.is_empty()).then_some((bucket, key));
    }
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    let mut segments = url.path_segments()?;
    let bucket = segments.next()?.to_string();
    let key = segments.collect::<Vec<_>>().join("/");
    (!bucket.is_empty() && !key.is_empty()).then_some((bucket, key))
}
