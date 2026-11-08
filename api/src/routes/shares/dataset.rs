//! Dataset share links: visitors read the dataset this node holds and download the files,
//! folders and buckets it names on this node, each checked as the link's creator.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission, SessionKind, SessionRef};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::share::{ShareLink, ShareTarget, TicketBody};
use aruna_operations::blob::permission_paths::ResolvePathsOperation;
use aruna_operations::distribution::guard::bound_view;
use aruna_operations::distribution::resolve::{GlobalResolution, resolve_content, resolve_version};
use aruna_operations::distribution::status::read_view;
use aruna_operations::driver::drive;
use aruna_operations::metadata::api::{
    ExportMetadataRequest, ExportMetadataResult, MetadataApiError, RoCrateExportView,
};
use aruna_operations::metadata::forward::export_rocrate_here;
use aruna_operations::shares::dataset::{
    DatasetFile, FilePlace, MemberSet, NamedSets, dataset_files, named_sets,
};
use aruna_operations::shares::rules::{
    NodeScope, bucket_pattern, exact_pattern, relative_path, visit_context,
};
use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::relay::share_access;
use super::visit::{
    TicketResponse, VisitListQuery, VisitListing, authorize_object, head_version, list_below,
    sign_ticket, unavailable, visit_scope, visited_link,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage::deletion::bucket_info;
use crate::server::state::ServerState;

/// How long resolving one global version or content hash of a dataset may take.
const RESOLVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(OpenApi)]
#[openapi()]
pub struct DatasetDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(DatasetDoc::openapi())
        .routes(routes!(read_dataset))
        .routes(routes!(list_part))
        .routes(routes!(dataset_ticket))
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct DatasetFileView {
    pub entity_id: String,
    pub name: Option<String>,
    /// `file`, or `folder` for a folder or bucket part listed with `/shares/visit/dataset/files`.
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct DatasetView {
    pub dataset_id: String,
    /// The dataset's RO-Crate metadata as JSON-LD.
    #[schema(value_type = Object)]
    pub rocrate: serde_json::Value,
    /// The files and folders visitors can download from this node.
    pub files: Vec<DatasetFileView>,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PartQuery {
    /// The `entity_id` of a listed folder.
    pub part: String,
    /// A folder below the part, ending with `/`; empty for the part itself.
    pub prefix: Option<String>,
    /// The `next_cursor` of the previous page.
    pub cursor: Option<String>,
    /// Entries per page, 1 to 1000; defaults to 100.
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetTicketRequest {
    /// The `entity_id` of a listed file or folder.
    pub entity_id: String,
    /// For a folder: the file's path below it, as its listing shows.
    pub name: Option<String>,
}

/// A local part of the dataset: one object, or every key below a prefix.
struct LocalPart {
    file: DatasetFile,
    bucket: String,
    key: String,
    version_id: Option<Ulid>,
    folder: bool,
}

impl LocalPart {
    fn covers(&self, bucket: &str, key: &str, version_id: Option<Ulid>) -> bool {
        self.bucket == bucket
            && match self.folder {
                true => key.starts_with(&self.key),
                false => {
                    self.key == key && (self.version_id.is_none() || self.version_id == version_id)
                }
            }
    }
}

fn export_error(error: MetadataApiError) -> ServerError {
    match error {
        MetadataApiError::NotFound => ServerError::Refused(
            StatusCode::NOT_FOUND,
            "dataset_not_here",
            "this node holds no readable copy of the dataset".to_string(),
        ),
        MetadataApiError::Forbidden | MetadataApiError::Unauthorized => ServerError::Forbidden,
        error => ServerError::ServiceUnavailableReason(error.to_string()),
    }
}

async fn export_here(
    state: &ServerState,
    auth: AuthContext,
    document_id: Ulid,
) -> Result<(MetadataRegistryRecord, String), MetadataApiError> {
    let request = ExportMetadataRequest {
        document_id,
        auth: Some(auth),
        view: RoCrateExportView::Full,
        limit: None,
        offset: None,
        after: None,
    };
    let metadata_bytes = state.rocrate_limits().metadata_bytes;
    let context = state.get_ctx();
    match export_rocrate_here(&context, state.get_realm_id(), request, metadata_bytes).await? {
        ExportMetadataResult::Full { record, jsonld } => Ok((record, jsonld)),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// The group, target and permission path of a new dataset link; this node must hold it.
pub(crate) async fn resolve_dataset(
    state: &ServerState,
    auth: &AuthContext,
    dataset_id: &str,
) -> ServerResult<(Ulid, ShareTarget, String)> {
    let document_id = Ulid::from_string(dataset_id)
        .map_err(|_| ServerError::BadRequestReason("dataset_id is not a dataset id".into()))?;
    let (record, _) = export_here(state, auth.clone(), document_id)
        .await
        .map_err(export_error)?;
    let target = ShareTarget::Dataset { document_id };
    Ok((record.group_id, target, record.permission_path))
}

/// The creator acting for a dataset visit; the document id itself confines the read.
fn dataset_reader(link: &ShareLink, state: &ServerState) -> AuthContext {
    AuthContext {
        user_id: link.created_by,
        realm_id: state.get_realm_id(),
        path_restrictions: None,
        session: Some(SessionRef {
            sid: link.id.to_string(),
            kind: SessionKind::Share,
            name: None,
            via: None,
        }),
    }
}

/// The dataset's crate and its parts on this node, as the creator may read them now.
async fn local_parts(
    state: &ServerState,
    link: &ShareLink,
) -> ServerResult<(Ulid, serde_json::Value, Vec<LocalPart>)> {
    let ShareTarget::Dataset { document_id } = link.target else {
        return Err(ServerError::Refused(
            StatusCode::BAD_REQUEST,
            "not_dataset",
            "this link does not share a dataset".to_string(),
        ));
    };
    let (record, jsonld) = export_here(state, dataset_reader(link, state), document_id)
        .await
        .map_err(|_| unavailable())?;
    let rocrate: serde_json::Value = serde_json::from_str(&jsonld)
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let (realm, node) = (state.get_realm_id(), state.get_node_id());
    let named = named_sets(&rocrate, &record.graph_iri, realm);
    let sets = member_sets(state, named).await;
    let mut parts = Vec::new();
    for file in dataset_files(&rocrate, &record.graph_iri, (realm, node), &sets) {
        let (bucket, key, version_id, folder) = match &file.place {
            FilePlace::Object {
                bucket,
                key,
                version_id,
            } => (bucket.clone(), key.clone(), *version_id, false),
            FilePlace::Prefix { bucket, prefix } => (bucket.clone(), prefix.clone(), None, true),
            FilePlace::Hash(hash) => {
                let indexed = drive(ResolvePathsOperation::new(*hash), &state.get_ctx())
                    .await
                    .unwrap_or_default();
                let local = (indexed.into_iter())
                    .find(|index| index.realm_id == realm && index.node_id == node)
                    .map(|index| (index.bucket, index.key, index.version_id));
                let found = match local {
                    Some(found) => Some(found),
                    None => member_content(state, *hash, &sets).await,
                };
                let Some((bucket, key, version_id)) = found else {
                    continue;
                };
                (bucket, key, Some(version_id), false)
            }
            FilePlace::Version(version_id) => {
                let deadline = Instant::now() + RESOLVE_TIMEOUT;
                let resolved = resolve_version(&state.get_ctx(), *version_id, deadline).await;
                let GlobalResolution::Found(found) = resolved else {
                    continue;
                };
                if !sets.iter().any(|set| set.set_id == found.set_id) {
                    continue;
                }
                (found.bucket, found.key, Some(found.version_id), false)
            }
        };
        parts.push(LocalPart {
            file,
            bucket,
            key,
            version_id,
            folder,
        });
    }
    Ok((document_id, rocrate, parts))
}

/// The member sets among the buckets and sets a dataset names: distributed buckets this node is
/// a bound member of. A bucket or set this node cannot read is left out.
async fn member_sets(state: &ServerState, named: NamedSets) -> Vec<MemberSet> {
    let context = state.get_ctx();
    let mut buckets = named.buckets;
    for set_id in named.sets {
        if let Ok(Some(view)) = read_view(&context, set_id).await {
            buckets.insert(view.snapshot().config.bucket.clone());
        }
    }
    let mut sets = Vec::new();
    for bucket in buckets {
        let Ok(info) = bucket_info(state, &bucket).await else {
            continue;
        };
        let Some(binding) = info.distribution.as_ref() else {
            continue;
        };
        if let Ok(view) = bound_view(&context, binding).await {
            let members = &view.snapshot().config.members;
            sets.push(MemberSet {
                set_id: binding.set_id,
                bucket,
                nodes: members.iter().map(|member| member.node_id).collect(),
            });
        }
    }
    sets
}

/// The first version of `hash` in one of `sets`, by the sets' content indexes.
async fn member_content(
    state: &ServerState,
    hash: [u8; 32],
    sets: &[MemberSet],
) -> Option<(String, String, Ulid)> {
    if sets.is_empty() {
        return None;
    }
    let deadline = Instant::now() + RESOLVE_TIMEOUT;
    let found = resolve_content(&state.get_ctx(), hash, deadline).await;
    (found.versions.into_iter())
        .find(|found| sets.iter().any(|set| set.set_id == found.set_id))
        .map(|found| (found.bucket, found.key, found.version_id))
}

/// The bucket of one part and the creator narrowed to `pattern` in it; `pattern` gets the
/// pattern of the bucket's permission path, on every member of a distributed one, with `/`.
async fn part_scope(
    state: &ServerState,
    link: &ShareLink,
    bucket: &str,
    pattern: impl FnOnce(&str) -> String,
) -> ServerResult<(String, BucketInfo, AuthContext)> {
    let info = bucket_info(state, bucket)
        .await
        .map_err(|_| unavailable())?;
    let nodes = NodeScope::of(&info, state.get_node_id());
    let base = bucket_pattern(state.get_realm_id(), info.group_id, nodes, bucket);
    let auth = visit_context(link, state.get_realm_id(), pattern(&format!("{base}/")));
    Ok((bucket.to_string(), info, auth))
}

/// The scope of one object of a part: the creator narrowed to exactly that object.
async fn object_scope(
    state: &ServerState,
    link: &ShareLink,
    bucket: &str,
    key: &str,
) -> ServerResult<(String, BucketInfo, AuthContext)> {
    part_scope(state, link, bucket, |base| {
        format!("{base}{}", exact_pattern(key))
    })
    .await
}

/// Checks that a ticket names an object the link still covers, and returns its scope.
pub(crate) async fn ticket_scope(
    state: &ServerState,
    link: &ShareLink,
    ticket: &TicketBody,
) -> ServerResult<(String, BucketInfo, AuthContext)> {
    if !matches!(link.target, ShareTarget::Dataset { .. }) {
        let (bucket, info, auth) = visit_scope(state, link).await?;
        let covered = link.target.covers(&bucket, &ticket.key) && bucket == ticket.bucket;
        return match covered {
            true => Ok((bucket, info, auth)),
            false => Err(unavailable()),
        };
    }
    let (_, _, parts) = local_parts(state, link).await?;
    let listed = parts
        .iter()
        .any(|part| part.covers(&ticket.bucket, &ticket.key, ticket.version_id));
    match listed {
        true => object_scope(state, link, &ticket.bucket, &ticket.key).await,
        false => Err(unavailable()),
    }
}

#[utoipa::path(
    get,
    path = "/shares/visit/dataset",
    tag = "shares",
    summary = "Read the dataset of an open share link",
    description = r#"Returns the shared dataset's RO-Crate metadata and the parts visitors can download here.

**Authentication**: `Authorization: Share <visit token>` of a dataset link.

**Behavior**
- Files and folder or bucket parts on this node are listed; a folder part is browsed with
  `/shares/visit/dataset/files`.
- Parts of distributed buckets this node is a member of are listed too, named by set, by
  global version, by content hash or by another member's ARN; other members serve their keys.
- Parts on other nodes, in other distributed buckets or outside Aruna stay in the metadata
  only.
- Files the creator can no longer read, and parts in buckets this node lacks, are left out."#,
    responses(
        (status = 200, description = "The dataset and its local parts", body = DatasetView, example = json!({"dataset_id": "01JMETADATA0123456789ABCDE", "rocrate": {"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": []}, "files": [{"entity_id": "https://w3id.org/aruna/data/<blake3>", "name": "table.csv", "kind": "file"}]})),
        (status = 400, description = "`not_dataset`", body = ErrorResponse),
        (status = 404, description = "`share_unavailable`", body = ErrorResponse)
    ),
    security(("share_token" = []))
)]
pub async fn read_dataset(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> ServerResult<Json<DatasetView>> {
    let (link, _) = visited_link(&state, &headers).await?;
    let (document_id, rocrate, parts) = local_parts(&state, &link).await?;
    let mut visible = Vec::new();
    for part in parts {
        // A part in a bucket this node does not hold is skipped, never the whole dataset.
        let readable = match part.folder {
            true => bucket_info(&state, &part.bucket).await.is_ok(),
            false => match object_scope(&state, &link, &part.bucket, &part.key).await {
                Ok((bucket, info, auth)) => {
                    let permission = (Permission::READ, "share.read");
                    authorize_object(&state, &auth, (&bucket, &info), &part.key, permission)
                        .await
                        .is_ok()
                }
                Err(_) => false,
            },
        };
        if readable {
            visible.push(DatasetFileView {
                entity_id: part.file.entity_id,
                name: part.file.name,
                kind: if part.folder { "folder" } else { "file" }.to_string(),
            });
        }
    }
    Ok(Json(DatasetView {
        dataset_id: document_id.to_string(),
        rocrate,
        files: visible,
    }))
}

/// The local part a visitor names by entity id.
async fn named_part(
    state: &ServerState,
    link: &ShareLink,
    entity_id: &str,
) -> ServerResult<LocalPart> {
    let (_, _, parts) = local_parts(state, link).await?;
    parts
        .into_iter()
        .find(|part| part.file.entity_id == entity_id)
        .ok_or(ServerError::NotFound)
}

#[utoipa::path(
    get,
    path = "/shares/visit/dataset/files",
    tag = "shares",
    summary = "List a folder part of a shared dataset",
    description = r#"Lists one folder level below a folder or bucket part of the shared dataset.

**Authentication**: `Authorization: Share <visit token>` of a dataset link.

**Behavior**: names are relative to the part; only what the creator may still read shows."#,
    params(PartQuery),
    responses(
        (status = 200, description = "One page of the folder", body = VisitListing, example = json!({"files": [{"name": "run-1/table.csv", "size": 2048, "last_modified": "2026-10-30T08:00:00Z"}], "folders": ["run-1/plots/"], "next_cursor": null})),
        (status = 400, description = "Malformed prefix, cursor or limit, a file part, or `not_dataset`", body = ErrorResponse),
        (status = 404, description = "`share_unavailable`, or no such part here", body = ErrorResponse)
    ),
    security(("share_token" = []))
)]
pub async fn list_part(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(query): Query<PartQuery>,
) -> ServerResult<Json<VisitListing>> {
    let (link, visit_end) = visited_link(&state, &headers).await?;
    let part = named_part(&state, &link, &query.part).await?;
    if !part.folder {
        return Err(ServerError::BadRequestReason(
            "this part is a file; request a ticket for it".into(),
        ));
    }
    let prefix = part.key.clone();
    let (bucket, info, auth) = part_scope(&state, &link, &part.bucket, |base| {
        format!("{base}{}**", exact_pattern(&prefix))
    })
    .await?;
    let list = VisitListQuery {
        prefix: query.prefix,
        cursor: query.cursor,
        limit: query.limit,
    };
    let covers = |key: &str| key.starts_with(&part.key);
    let scope = (bucket.as_str(), &info, &auth);
    let visit = (&link, visit_end);
    let listing = list_below(&state, scope, &part.key, list, visit, covers).await?;
    Ok(Json(listing))
}

#[utoipa::path(
    post,
    path = "/shares/visit/dataset/tickets",
    tag = "shares",
    summary = "Get a download URL for a dataset file",
    description = r#"Returns a single-use download URL, valid for one minute, for one dataset file here.

**Authentication**: `Authorization: Share <visit token>` of a dataset link.

**Behavior**: a file inside a folder part is named by the part's `entity_id` and its `name`."#,
    request_body(content = DatasetTicketRequest, example = json!({"entity_id": "https://w3id.org/aruna/data/<blake3>"})),
    responses(
        (status = 200, description = "The download URL path", body = TicketResponse, example = json!({"path": "/api/v1/shares/download?ticket=<single-use-ticket>", "expires_at": "2026-10-31T10:01:00Z"})),
        (status = 400, description = "Malformed name, or `not_dataset`", body = ErrorResponse),
        (status = 404, description = "`share_unavailable`, or the dataset names no such file here", body = ErrorResponse)
    ),
    security(("share_token" = []))
)]
pub async fn dataset_ticket(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<DatasetTicketRequest>,
) -> ServerResult<Json<TicketResponse>> {
    let (link, visit_end) = visited_link(&state, &headers).await?;
    let part = named_part(&state, &link, &request.entity_id).await?;
    let key = match (part.folder, request.name.as_deref()) {
        (true, Some(name)) => {
            relative_path(name)
                .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
            format!("{}{name}", part.key)
        }
        (false, None) => part.key.clone(),
        _ => {
            return Err(ServerError::BadRequestReason(
                "name a file below a folder part, and only there".into(),
            ));
        }
    };
    let (bucket, info, auth) = object_scope(&state, &link, &part.bucket, &key).await?;
    let permission = (Permission::READ, "share.read");
    authorize_object(&state, &auth, (&bucket, &info), &key, permission).await?;
    let version = match part.version_id {
        Some(version) => Some(version),
        None => {
            let user = share_access(&state, &link, &auth, visit_end);
            head_version(&state, (&bucket, &info), &key, &user).await?
        }
    };
    Ok(Json(sign_ticket(
        (&link, visit_end),
        (&bucket, &key),
        version,
    )?))
}
