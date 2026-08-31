use super::data::{ReadObjectInput, read_text};
use super::{
    JsonPayload, McpServer, authorize_tool, bad_request, empty_extras, explained, internal_error,
    parse_ulid, request_auth, server_error, tool_extras,
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
    /// The metadata document's bare 26-character ULID, for example
    /// `01JZ8Y6T0K4W7M2N9Q5R3S8V1X`. Read `document_id` from a `search_datasets`
    /// hit, a `list_profiles` entry, or a `create_dataset` answer. It is the id
    /// alone, never the `path@id` permission form and never a graph IRI.
    pub id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct DatasetSearchInput {
    /// Free-text query over the indexed name, description, keywords, and
    /// identifier literals, for example `rna-seq mouse liver`. Plain terms only:
    /// quotes, wildcards, and boolean operators are stripped and the remaining
    /// terms are combined with OR. May be empty when `conforms_to` is set.
    pub q: String,
    /// Exact absolute IRI the root entity must declare in `conformsTo`, for
    /// example `https://w3id.org/ro/crate/1.3` for the specification or
    /// `https://w3id.org/aruna/profile/<document id>` for a Profile from
    /// `list_profiles`. Matched exactly, never as a prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conforms_to: Option<String>,
    /// Restrict hits to one group's bare 26-character ULID. Call `list_groups`
    /// for the ids the caller may use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    /// Maximum hits to return. Defaults to 25 and is silently clamped to the
    /// range 1 to 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ValidateInput {
    /// The draft crate as one RO-Crate JSON-LD object with a top-level
    /// `@context` and a `@graph` array, not a bare array of entities. The graph
    /// needs the root Dataset `./` carrying name, description, and exactly one
    /// datePublished, plus the `ro-crate-metadata.json` descriptor of type
    /// CreativeWork whose `about` points at `./`. At most one non-specification
    /// `conformsTo` IRI on the root, and File entities use `s3://bucket/key`
    /// contentUrl values.
    pub rocrate: JsonPayload,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct CreateDatasetInput {
    /// Owning group's bare 26-character ULID, for example
    /// `01JZ8Y6T0K4W7M2N9Q5R3S8V1X`. Call `list_groups` for the ids the caller
    /// may use; the caller needs write permission on the group.
    pub group_id: String,
    /// Document path inside the group, for example
    /// `datasets/mouse-liver-2026`. Leading and trailing slashes are trimmed
    /// and the remainder must not be empty. It is a metadata document path, not
    /// a bucket and key and not a URL; `profiles/` holds Aruna Profiles.
    pub path: String,
    /// The crate to store, as one RO-Crate JSON-LD object with a top-level
    /// `@context` and a `@graph` array. Check it with `validate_dataset` first:
    /// the same structural and Profile rules decide this write.
    pub rocrate: JsonPayload,
    /// Whether the document is readable outside the owning group. Defaults to
    /// false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ReplaceDatasetInput {
    /// The document's bare 26-character ULID, for example
    /// `01JZ8Y6T0K4W7M2N9Q5R3S8V1X`, from `search_datasets`, `get_dataset`, or
    /// the `create_dataset` answer.
    pub id: String,
    /// The complete replacement crate as one RO-Crate JSON-LD object with a
    /// top-level `@context` and a `@graph` array. It replaces the stored crate
    /// rather than merging into it, so start from `get_dataset`.
    pub rocrate: JsonPayload,
    /// New visibility for the document. Omit to keep the current one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct SparqlInput {
    /// SPARQL query text of at most 65536 bytes, for example
    /// `SELECT DISTINCT ?s WHERE { ?s <http://schema.org/name> ?n }`. SELECT and
    /// ASK only: CONSTRUCT, DESCRIBE, updates, any SERVICE clause, and a LIMIT
    /// above 10000 are refused. Declare every prefix the query uses. Without
    /// `document_id` it must additionally be an ASK or a SELECT DISTINCT over a
    /// single triple pattern with no OFFSET.
    pub query: String,
    /// Run the query against one document's graph instead of all visible
    /// metadata, given as that document's bare 26-character ULID from
    /// `search_datasets`. The single-pattern restriction does not apply then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ReferencesInput {
    /// Absolute IRI to find backlinks for, matched as the object of a triple,
    /// for example `https://w3id.org/aruna/profile/01JZ8Y6T0K4W7M2N9Q5R3S8V1X`.
    /// A relative or malformed IRI is refused.
    pub iri: String,
    /// Absolute predicate IRI to match exactly, for example
    /// `http://purl.org/dc/terms/conformsTo`. Omit to accept any predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
    /// Maximum references to return. Defaults to 25 and is silently clamped to
    /// the range 1 to 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

pub(crate) fn toolset() -> rmcp::handler::server::router::tool::ToolRouter<McpServer> {
    McpServer::metadata_router()
}

#[tool_router(router = metadata_router)]
impl McpServer {
    #[tool(
        description = "List the Aruna Profile documents under `profiles/` that the caller may read, each with document_id, group_id, document_path, visibility, timestamps, and an RO-Crate summary. Call it to find the Profile a dataset should conform to: a Profile's conformsTo IRI is `https://w3id.org/aruna/profile/<document_id>`. Use get_profile for the full crate and its SHACL rules. Takes no arguments and returns at most 1000 entries.",
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
        description = "Read one Profile and return its raw RO-Crate together with every embedded SHACL Turtle rule, including rules stored as separate objects. Call list_profiles for a valid id. Read the Turtle to learn which properties a conforming crate must carry, then check a draft with validate_dataset. A document outside `profiles/` answers Not found.",
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
            return Err(explained(
                crate::error::ServerError::NotFound,
                "that document is not stored under profiles/, so it is not a Profile; call \
                 list_profiles for Profile ids or get_dataset for a plain document",
            ));
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
        description = "Search visible dataset metadata by free text, exact conformance IRI, and group, returning ranked hits with document_id, document_path, graph_iri, subject_iri, subject_types, title, and snippet. One hit is one matched RDF subject, so a crate's root dataset and its file entities match separately and subject_types carries the subject's rdf:type IRIs. Give q, conforms_to, or both; an empty q with no conforms_to is refused. Pass a hit's document_id to get_dataset for the full crate. Use search when the target may be a bucket, group, or user instead.",
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
        let group_id = input.group_id.as_deref().map(parse_group).transpose()?;
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
        .map_err(search_error)?;
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
        description = "Read one metadata document and return its raw accepted RO-Crate revision with the projection state. Call search_datasets or list_profiles for a valid document_id. The answer is the stored crate, so use it as the starting point for replace_dataset rather than rebuilding a crate from a summary.",
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
        description = "Run the structural and Profile checks a metadata write applies and return the verdict without storing anything. Call it before create_dataset and replace_dataset and repair every reported item until `accepted` is true. Structural violations carry a code and a JSON pointer, Profile findings carry the failing SHACL rule and its focus node. It creates and changes nothing.",
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
        let jsonld = rocrate_json(&input.rocrate.0)?;
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
        description = "Create a new metadata document from an RO-Crate and return the accepted registry summary, including the new document_id. Check the crate with validate_dataset first and call list_groups for a group_id the caller may write to. Use replace_dataset to change an existing document instead of creating a second one at another path. Acceptance is durable, but the document may need a moment before get_dataset can read it.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn create_dataset(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<CreateDatasetInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("create_dataset", &input)?;
        let group_id = parse_group(&input.group_id)?;
        let path = MetadataRegistryRecord::normalize_document_path(&input.path);
        if path.is_empty() {
            return Err(bad_request(
                "path must name a document inside the group, such as \
                 datasets/mouse-liver-2026; it is empty once leading and trailing slashes are \
                 trimmed",
            ));
        }
        let jsonld = rocrate_json(&input.rocrate.0)?;
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
        .map_err(write_error)?;
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
        .map_err(write_error)?;
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
        description = "Replace one document's entire RO-Crate and optionally change its visibility, returning the updated registry summary. The crate is replaced, not merged, so read the current one with get_dataset and edit that. Validate the result with validate_dataset before writing. Use create_dataset when the document does not exist yet.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn replace_dataset(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<ReplaceDatasetInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let document_id = parse_document(&input.id)?;
        let jsonld = rocrate_json(&input.rocrate.0)?;
        let extras = tool_extras("replace_dataset", &input)?;
        let record = crate::routes::metadata::local_write_record(
            &self.state,
            &auth,
            document_id,
            extras.clone(),
        )
        .await
        .map_err(update_error)?;
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
        .map_err(update_error)?;
        let summary = crate::routes::metadata::MetadataDocumentSummary::from(&updated);
        Ok(Json(JsonPayload(
            serde_json::to_value(summary).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "Run a read-only SPARQL SELECT or ASK query over visible metadata, or over one document when document_id is given, and return the solutions or the boolean with the fan-out counters. Use search_datasets for plain text lookup and this tool for questions about relationships between entities. Without document_id the query fans out across realm nodes and must be an ASK or a SELECT DISTINCT over a single triple pattern with no OFFSET; with document_id it runs locally without that restriction. At most 10000 rows are returned and the query is cut off after ten seconds.",
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
        let document_scoped = input.document_id.is_some();
        let execution = if let Some(document_id) = input.document_id.as_deref() {
            let document_id = parse_document(document_id)?;
            let record =
                crate::routes::metadata::load_metadata_record_by_document(&self.state, document_id)
                    .await
                    .map_err(|error| query_error(error, true))?;
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
        .map_err(|error| query_error(error, document_scoped))?;
        let response =
            crate::routes::metadata::map_query_results(execution.results, execution.fanout_stats)
                .map_err(server_error)?;
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "Find the visible metadata documents that reference an absolute IRI, optionally through one predicate, returning document_id, document_path, graph_iri, and the referring subjects. Use it to answer which datasets conform to a Profile or cite a given entity, and sparql_query for anything more structured. It scans this node only and returns at most 100 references.",
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
        .map_err(references_error)?;
        let response = crate::routes::metadata::map_references_response(execution);
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }
}

/// The REST parsers answer a malformed id or crate with a bare "Bad request",
/// which leaves a tool caller nothing to correct.
fn parse_document(id: &str) -> Result<Ulid, CallToolResult> {
    crate::routes::metadata::parse_document_id(id).map_err(|_| {
        bad_request(
            "document id must be a bare 26-character ULID such as 01JZ8Y6T0K4W7M2N9Q5R3S8V1X; read \
             document_id from search_datasets, list_profiles, or a create_dataset answer",
        )
    })
}

fn parse_group(group_id: &str) -> Result<Ulid, CallToolResult> {
    parse_ulid(
        "group_id",
        group_id,
        "call list_groups for the ids the caller may use",
    )
}

fn rocrate_json(value: &Value) -> Result<String, CallToolResult> {
    crate::routes::metadata::serialize_jsonld_object(value).map_err(|_| {
        bad_request(
            "rocrate must be one JSON object holding a top-level @context and a @graph array; send \
             the object itself, not a bare array of entities and not a JSON string",
        )
    })
}

fn missing_document() -> CallToolResult {
    explained(
        crate::error::ServerError::NotFound,
        "no metadata document with that id is visible to the caller; call search_datasets or \
         list_profiles for ids that are",
    )
}

fn update_error(error: crate::error::ServerError) -> CallToolResult {
    match error {
        crate::error::ServerError::NotFound => explained(
            error,
            "no metadata document with that id exists here; call search_datasets for an existing \
             id, or create_dataset when the document is new",
        ),
        crate::error::ServerError::Forbidden => explained(
            error,
            "the caller holds no write permission on that document's group; call list_groups for \
             the groups it belongs to",
        ),
        error => server_error(error),
    }
}

fn write_error(error: crate::error::ServerError) -> CallToolResult {
    match error {
        crate::error::ServerError::Forbidden => explained(
            error,
            "the caller holds no write permission on that group's metadata; call list_groups for \
             the groups it belongs to",
        ),
        error => server_error(error),
    }
}

fn document_error(error: crate::error::ServerError) -> CallToolResult {
    match error {
        crate::error::ServerError::NotFound => missing_document(),
        crate::error::ServerError::Forbidden => explained(
            error,
            "the caller may not read that document; a document is visible to its group and, when \
             public, to the realm",
        ),
        error => server_error(error),
    }
}

/// The metadata API refuses an unsupported query with a bare "Bad request".
fn query_error(error: crate::error::ServerError, document_scoped: bool) -> CallToolResult {
    match error {
        crate::error::ServerError::BadRequest if document_scoped => explained(
            error,
            "query must be a SELECT or ASK SPARQL query of at most 65536 bytes, with every prefix \
             declared, no SERVICE clause, and no LIMIT above 10000",
        ),
        crate::error::ServerError::BadRequest => explained(
            error,
            "a query over all visible metadata must be an ASK or a SELECT DISTINCT over a single \
             triple pattern with no OFFSET, at most 65536 bytes, with every prefix declared, no \
             SERVICE clause, and no LIMIT above 10000; pass document_id to run a full query \
             against one document",
        ),
        crate::error::ServerError::NotFound => missing_document(),
        error => server_error(error),
    }
}

fn search_error(error: crate::error::ServerError) -> CallToolResult {
    match error {
        crate::error::ServerError::BadRequest => explained(
            error,
            "give a non-empty q, a conforms_to IRI, or both; conforms_to must be an absolute IRI \
             such as https://w3id.org/ro/crate/1.3",
        ),
        error => server_error(error),
    }
}

fn references_error(error: crate::error::ServerError) -> CallToolResult {
    match error {
        crate::error::ServerError::BadRequest => explained(
            error,
            "iri must be a non-empty absolute IRI such as \
             https://w3id.org/aruna/profile/01JZ8Y6T0K4W7M2N9Q5R3S8V1X, and predicate, when given, \
             must be an absolute IRI too",
        ),
        error => server_error(error),
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
    let document_id = parse_document(&input.id)?;
    let record =
        crate::routes::metadata::load_metadata_record_by_document(&server.state, document_id)
            .await
            .map_err(document_error)?;
    authorize_tool(
        &server.state,
        auth,
        record.permission_path.clone(),
        Permission::READ,
        tool_extras(tool, input)?,
    )
    .await
    .map_err(document_error)?;
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
