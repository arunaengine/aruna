//! Dataset share links: visitors read the dataset this node holds and download the files it
//! names on this node, each checked as the link's creator.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission, SessionKind, SessionRef};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::share::{ShareLink, ShareTarget, TicketBody};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::blob::permission_paths::ResolvePathsOperation;
use aruna_operations::driver::drive;
use aruna_operations::metadata::api::{
    ExportMetadataRequest, ExportMetadataResult, MetadataApiError, RoCrateExportView,
};
use aruna_operations::metadata::forward::export_rocrate_here;
use aruna_operations::shares::dataset::{DatasetFile, FilePlace, dataset_files};
use aruna_operations::shares::rules::{exact_pattern, visit_context};
use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::visit::{
    TicketResponse, authorize_object, current_version, sign_ticket, unavailable, visit_scope,
    visited_link,
};
use crate::auth::ensure_permission_with;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage::deletion::bucket_info;
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct DatasetDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(DatasetDoc::openapi())
        .routes(routes!(read_dataset))
        .routes(routes!(dataset_ticket))
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct DatasetFileView {
    pub entity_id: String,
    pub name: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct DatasetView {
    pub dataset_id: String,
    /// The dataset's RO-Crate metadata as JSON-LD.
    #[schema(value_type = Object)]
    pub rocrate: serde_json::Value,
    /// The files visitors can download from this node.
    pub files: Vec<DatasetFileView>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetTicketRequest {
    /// The `entity_id` of a listed file.
    pub entity_id: String,
}

/// A local dataset file with its resolved object.
struct LocalFile {
    file: DatasetFile,
    bucket: String,
    key: String,
    version_id: Option<Ulid>,
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
) -> Result<
    (
        aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord,
        String,
    ),
    MetadataApiError,
> {
    let request = ExportMetadataRequest {
        document_id,
        auth: Some(auth),
        view: RoCrateExportView::Full,
        limit: None,
        offset: None,
        after: None,
    };
    let metadata_bytes = state.rocrate_limits().metadata_bytes;
    match export_rocrate_here(
        &state.get_ctx(),
        state.get_realm_id(),
        request,
        metadata_bytes,
    )
    .await?
    {
        ExportMetadataResult::Full { record, jsonld } => Ok((record, jsonld)),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// The creator's check for a new dataset link: the dataset must be readable on this node.
pub(crate) async fn checked_dataset(
    state: &ServerState,
    auth: &AuthContext,
    dataset_id: &str,
) -> ServerResult<(Ulid, ShareTarget)> {
    let document_id = Ulid::from_string(dataset_id)
        .map_err(|_| ServerError::BadRequestReason("dataset_id is not a dataset id".into()))?;
    let (record, _) = export_here(state, auth.clone(), document_id)
        .await
        .map_err(export_error)?;
    let mut extras = PolicyRequestExtras::operation("share.create");
    extras.body =
        Some(serde_json::json!({ "target": { "kind": "dataset", "dataset_id": dataset_id } }));
    ensure_permission_with(
        state,
        auth,
        record.permission_path.clone(),
        Permission::READ,
        extras,
    )
    .await?;
    Ok((record.group_id, ShareTarget::Dataset { document_id }))
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

/// The dataset's crate and its files on this node, as the creator may read them now.
async fn local_files(
    state: &ServerState,
    link: &ShareLink,
) -> ServerResult<(Ulid, serde_json::Value, Vec<LocalFile>)> {
    let ShareTarget::Dataset { document_id } = link.target else {
        return Err(ServerError::Refused(
            StatusCode::BAD_REQUEST,
            "not_dataset",
            "this link does not share a dataset".to_string(),
        ));
    };
    let (_, jsonld) = export_here(state, dataset_reader(link, state), document_id)
        .await
        .map_err(|_| unavailable())?;
    let rocrate: serde_json::Value = serde_json::from_str(&jsonld)
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let (realm, node) = (state.get_realm_id(), state.get_node_id());
    let mut files = Vec::new();
    for file in dataset_files(&rocrate, realm, node) {
        let (bucket, key, version_id) = match &file.place {
            FilePlace::Object {
                bucket,
                key,
                version_id,
            } => (bucket.clone(), key.clone(), *version_id),
            FilePlace::Hash(hash) => {
                let indexed = drive(ResolvePathsOperation::new(*hash), &state.get_ctx())
                    .await
                    .unwrap_or_default();
                let Some(index) = indexed
                    .into_iter()
                    .find(|index| index.realm_id == realm && index.node_id == node)
                else {
                    continue;
                };
                (index.bucket, index.key, Some(index.version_id))
            }
        };
        files.push(LocalFile {
            file,
            bucket,
            key,
            version_id,
        });
    }
    Ok((document_id, rocrate, files))
}

/// The bucket and narrowed creator for one object of a visit.
async fn object_scope(
    state: &ServerState,
    link: &ShareLink,
    bucket: &str,
    key: &str,
) -> ServerResult<(String, BucketInfo, AuthContext)> {
    let info = bucket_info(state, bucket)
        .await
        .map_err(|_| unavailable())?;
    if info.distribution.is_some() {
        return Err(unavailable());
    }
    let path = object_permission_path(
        state.get_realm_id(),
        info.group_id,
        state.get_node_id(),
        bucket,
        key,
    );
    let auth = visit_context(link, state.get_realm_id(), exact_pattern(&path));
    Ok((bucket.to_string(), info, auth))
}

/// Checks that a ticket names an object the link still covers, and returns its scope.
pub(crate) async fn ticket_scope(
    state: &ServerState,
    link: &ShareLink,
    ticket: &TicketBody,
) -> ServerResult<(String, BucketInfo, AuthContext)> {
    if !matches!(link.target, ShareTarget::Dataset { .. }) {
        let (bucket, info, auth) = visit_scope(state, link).await?;
        return match link.target.covers(&bucket, &ticket.key) && bucket == ticket.bucket {
            true => Ok((bucket, info, auth)),
            false => Err(unavailable()),
        };
    }
    let (_, _, files) = local_files(state, link).await?;
    let listed = files.iter().any(|file| {
        file.bucket == ticket.bucket
            && file.key == ticket.key
            && (file.version_id.is_none() || file.version_id == ticket.version_id)
    });
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
    description = r#"Returns the shared dataset's RO-Crate metadata and the files visitors can download here.

**Authentication**: `Authorization: Share <visit token>` of a dataset link.

**Behavior**: files stored on other nodes or outside Aruna stay listed in the metadata but are
not downloadable through the link; files the creator can no longer read are left out."#,
    responses(
        (status = 200, description = "The dataset and its local files", body = DatasetView, example = json!({"dataset_id": "01JMETADATA0123456789ABCDE", "rocrate": {"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": []}, "files": [{"entity_id": "https://w3id.org/aruna/data/<blake3>", "name": "table.csv"}]})),
        (status = 400, description = "`not_dataset`", body = ErrorResponse),
        (status = 404, description = "`share_unavailable`", body = ErrorResponse)
    ),
    security(("share_token" = []))
)]
pub async fn read_dataset(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> ServerResult<Json<DatasetView>> {
    let link = visited_link(&state, &headers).await?;
    let (document_id, rocrate, files) = local_files(&state, &link).await?;
    let mut visible = Vec::new();
    for file in files {
        let (bucket, info, auth) = object_scope(&state, &link, &file.bucket, &file.key).await?;
        let readable = authorize_object(
            &state,
            &auth,
            (&bucket, &info),
            &file.key,
            (Permission::READ, "share.read"),
        )
        .await
        .is_ok();
        if readable {
            visible.push(DatasetFileView {
                entity_id: file.file.entity_id,
                name: file.file.name,
            });
        }
    }
    Ok(Json(DatasetView {
        dataset_id: document_id.to_string(),
        rocrate,
        files: visible,
    }))
}

#[utoipa::path(
    post,
    path = "/shares/visit/dataset/tickets",
    tag = "shares",
    summary = "Get a download URL for a dataset file",
    description = r#"Returns a single-use download URL, valid for one minute, for one dataset file here.

**Authentication**: `Authorization: Share <visit token>` of a dataset link."#,
    request_body(content = DatasetTicketRequest, example = json!({"entity_id": "https://w3id.org/aruna/data/<blake3>"})),
    responses(
        (status = 200, description = "The download URL path", body = TicketResponse, example = json!({"path": "/api/v1/shares/download?ticket=<single-use-ticket>", "expires_at": "2026-10-31T10:01:00Z"})),
        (status = 400, description = "`not_dataset`", body = ErrorResponse),
        (status = 404, description = "`share_unavailable`, or the dataset names no such file here", body = ErrorResponse)
    ),
    security(("share_token" = []))
)]
pub async fn dataset_ticket(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<DatasetTicketRequest>,
) -> ServerResult<Json<TicketResponse>> {
    let link = visited_link(&state, &headers).await?;
    let (_, _, files) = local_files(&state, &link).await?;
    let file = files
        .into_iter()
        .find(|file| file.file.entity_id == request.entity_id)
        .ok_or(ServerError::NotFound)?;
    let (bucket, info, auth) = object_scope(&state, &link, &file.bucket, &file.key).await?;
    authorize_object(
        &state,
        &auth,
        (&bucket, &info),
        &file.key,
        (Permission::READ, "share.read"),
    )
    .await?;
    let version = match file.version_id {
        Some(version) => Some(version),
        None => current_version(&state, &file.bucket, &file.key).await?,
    };
    Ok(Json(sign_ticket(
        &link,
        (&file.bucket, &file.key),
        version,
    )?))
}
