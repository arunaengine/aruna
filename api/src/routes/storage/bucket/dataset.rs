//! Describes a bucket as a dataset in one step: creates the dataset with the bucket as its part
//! and links it as the bucket's description.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::identifier::{Identifier, IdentifierTarget};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::bucket_metadata::{BucketMetadata, validate_identifiers};
use aruna_core::structs::storage::data::link::DataLink;
use aruna_core::structs::storage::dataset_description::DatasetDescription;
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::metadata::create_document::CreateDocumentPayload;
use aruna_operations::metadata::forward::route_metadata_delete;
use aruna_operations::s3::bucket::metadata::{
    DescriptionError, bucket_metadata, dataset_description,
};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::super::deletion::bucket_info;
use super::super::routing::ensure_group_admin;
use super::metadata::{
    BucketIdentifierBody, BucketMetadataBody, DatasetDescriptionBody, write_metadata,
};
use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::{forwarded_auth_token, local_write_record, run_create_metadata};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct DatasetApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(DatasetApiDoc::openapi()).routes(routes!(describe_bucket))
}

/// What the new dataset says about the bucket.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DescribeBucketRequest {
    pub title: String,
    pub description: String,
    pub version: Option<String>,
    /// A license URL or SPDX id, such as `CC-BY-4.0`.
    pub license: Option<String>,
    #[serde(default)]
    pub identifiers: Vec<BucketIdentifierBody>,
    /// Anyone may read the dataset; a public database needs a public dataset.
    #[serde(default)]
    pub public: bool,
    /// The dataset's path in the group; `buckets/<bucket>` when absent.
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DescribeBucketResponse {
    pub bucket: String,
    /// The new dataset's id.
    pub dataset: String,
    pub metadata: BucketMetadataBody,
    pub description: DatasetDescriptionBody,
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/dataset",
    tag = "data/storage",
    summary = "Describe a bucket as a dataset",
    description = r#"Creates a dataset in the bucket's group that describes the bucket and has the whole bucket as its part, then links it in the bucket's metadata.

**Authentication**: realm bearer token with WRITE on the owning group's admin path and on the
group's metadata path.

**Behavior**
- The dataset's root entity carries the title, description, version, license and identifiers;
  it is edited like any dataset afterwards. The bucket's other metadata stays as it is.
- A bucket that already links a dataset is refused, so a repeated request creates nothing.
- The link applies only while the bucket's metadata is still what was read; a concurrent
  change fails it with 409. When the link is refused, the new dataset is deleted again. When
  the outcome is not known, or the deletion fails, the dataset stays and the 503 answer names
  it; `GET` shows whether it is linked and `PUT /data/buckets/{bucket}/metadata` links it.
- The new dataset is read like any other once it is materialized, which takes a moment. A
  distributed bucket links it only once it can be read, after a few seconds at most; until
  then the 503 answer names it."#,
    params(("bucket" = String, Path, description = "Bucket name on this node")),
    request_body(content = DescribeBucketRequest, example = json!({
        "title": "Bakta database",
        "description": "Annotation database for Bakta",
        "version": "6.0",
        "license": "CC-BY-4.0",
        "identifiers": [{"name": "DOI", "value": "10.5281/zenodo.14916843", "url": "https://doi.org/10.5281/zenodo.14916843"}],
        "public": true
    })),
    responses(
        (status = 201, description = "The dataset was created and linked", body = DescribeBucketResponse, example = json!({
            "bucket": "bakta-db",
            "dataset": "01JMETADATA0123456789ABCDE",
            "metadata": {"database": false, "parser": null, "dataset": "01JMETADATA0123456789ABCDE"},
            "description": {"title": "Bakta database", "description": "Annotation database for Bakta", "version": "6.0", "license": "https://spdx.org/licenses/CC-BY-4.0.html", "identifiers": [{"name": "DOI", "value": "10.5281/zenodo.14916843", "url": "https://doi.org/10.5281/zenodo.14916843"}]}
        })),
        (status = 400, description = "An unknown field, an empty title or description, or an identifier without an http or https link; or the dataset was refused", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the owning group's admin or metadata path", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "The bucket already links a dataset, or its metadata changed meanwhile", body = ErrorResponse),
        (status = 503, description = "The dataset was not accepted, or it was created and may not be linked; the message names it then", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn describe_bucket(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(bucket): Path<String>,
    request: Result<Json<DescribeBucketRequest>, JsonRejection>,
) -> ServerResult<(StatusCode, Json<DescribeBucketResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let Json(request) =
        request.map_err(|error| ServerError::BadRequestReason(error.body_text()))?;
    let identifiers = checked_identifiers(&request)?;
    let info = bucket_info(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, info.group_id).await?;
    let current = bucket_metadata(&state.get_ctx(), &bucket, &info)
        .await
        .map_err(ServerError::ServiceUnavailableReason)?
        .unwrap_or_default();
    if let Some(dataset) = current.dataset {
        return Err(ServerError::Conflict(format!(
            "the bucket is already described by dataset {dataset}; edit that dataset"
        )));
    }
    let part = bucket_part(&state, &bucket, &info).to_id();
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let jsonld = dataset_crate(&request, &identifiers, (&bucket, &part), &today);
    let path = (request.path.clone()).unwrap_or_else(|| format!("buckets/{bucket}"));
    let created = run_create_metadata(
        &state,
        &auth,
        PolicyRequestExtras::rest(),
        bearer.clone(),
        info.group_id,
        path,
        request.public,
        CreateDocumentPayload::RoCrate {
            jsonld: jsonld.to_string(),
        },
        Some(format!("Describe bucket {bucket}")),
    )
    .await?;
    let dataset = created.document_id;
    // A set's publisher reads the dataset before it accepts the link, so it must be readable.
    if info.distribution.is_some() {
        let token = forwarded_auth_token(bearer.clone())?;
        let (ctx, realm_id) = (state.get_ctx(), state.get_realm_id());
        let caller = (Some(&auth), token);
        let read = || dataset_description(&ctx, realm_id, dataset, caller.clone());
        if !becomes_readable(read).await {
            return Err(ServerError::ServiceUnavailableReason(format!(
                "dataset {dataset} was created but cannot be read yet; link it with \
                 PUT /data/buckets/{bucket}/metadata once it can"
            )));
        }
    }
    let metadata = BucketMetadata {
        dataset: Some(dataset),
        ..current.clone()
    };
    // Links only while the metadata is still what was read, so no concurrent change is lost.
    let change = (Some(metadata.clone()), Some(current));
    let linked = write_metadata(&state, (&auth, bearer.clone()), (&bucket, &info), change);
    if let Err(error) = linked.await {
        if !refused(&error) {
            return Err(ServerError::ServiceUnavailableReason(format!(
                "dataset {dataset} was created, but whether it was linked is not known ({error}); \
                 read GET /data/buckets/{bucket}/metadata, or link it with \
                 PUT /data/buckets/{bucket}/metadata"
            )));
        }
        return Err(match remove_dataset(&state, &auth, bearer, dataset).await {
            Ok(()) => error,
            Err(removal) => ServerError::ServiceUnavailableReason(format!(
                "dataset {dataset} was created but not linked ({error}) nor removed \
                 ({removal}); link it with PUT /data/buckets/{bucket}/metadata or delete it"
            )),
        });
    }
    let description = DatasetDescriptionBody {
        title: Some(request.title),
        description: Some(request.description),
        version: request.version,
        license: request.license.as_deref().map(license_url),
        identifiers: identifiers.into_iter().map(Into::into).collect(),
    };
    Ok((
        StatusCode::CREATED,
        Json(DescribeBucketResponse {
            bucket,
            dataset: dataset.to_string(),
            metadata: metadata.into(),
            description,
        }),
    ))
}

/// Milliseconds between reads of a new dataset; materializing it takes a moment, not forever.
const READY_DELAYS: [u64; 6] = [100, 200, 400, 800, 1600, 3200];

/// Whether `read` gives the new dataset's description within the bounded wait.
async fn becomes_readable<R>(mut read: impl FnMut() -> R) -> bool
where
    R: Future<Output = Result<DatasetDescription, DescriptionError>>,
{
    for delay in READY_DELAYS {
        if read().await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    read().await.is_ok()
}

/// A link the bucket's node or publisher definitely refused; any other failure may have applied.
fn refused(error: &ServerError) -> bool {
    matches!(
        error,
        ServerError::Conflict(_)
            | ServerError::BadRequestReason(_)
            | ServerError::BadRequestMessage(_)
            | ServerError::Unauthorized
            | ServerError::Forbidden
            | ServerError::NotFound
    )
}

/// Deletes a dataset this request created but could not link.
async fn remove_dataset(
    state: &ServerState,
    auth: &AuthContext,
    bearer: Option<ValidatedBearer>,
    dataset: Ulid,
) -> ServerResult<()> {
    let extras = PolicyRequestExtras::rest();
    let record = local_write_record(state, auth, dataset, extras).await?;
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: state.get_realm_id(),
    };
    let token = forwarded_auth_token(bearer)?;
    let ctx = state.get_ctx();
    route_metadata_delete(&ctx, actor, record.as_ref(), dataset, token)
        .await
        .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))
}

fn checked_identifiers(request: &DescribeBucketRequest) -> ServerResult<Vec<Identifier>> {
    if request.title.trim().is_empty() || request.description.trim().is_empty() {
        return Err(ServerError::BadRequestReason(
            "a dataset needs a title and a description".to_string(),
        ));
    }
    let identifiers: Vec<Identifier> = (request.identifiers.iter())
        .map(|identifier| Identifier {
            name: identifier.name.clone(),
            value: identifier.value.clone(),
            url: identifier.url.clone(),
            target: IdentifierTarget::Key,
        })
        .collect();
    validate_identifiers(&identifiers).map_err(ServerError::BadRequestReason)?;
    Ok(identifiers)
}

/// The whole bucket as a dataset part: its set's global id, or the bucket-prefix ARN.
fn bucket_part(state: &ServerState, bucket: &str, info: &BucketInfo) -> DataLink {
    match &info.distribution {
        Some(binding) => DataLink::SetPrefix {
            realm_id: state.get_realm_id(),
            set_id: binding.set_id,
            prefix: String::new(),
        },
        None => DataLink::NodePrefix {
            realm_id: state.get_realm_id(),
            node_id: state.get_node_id(),
            bucket: bucket.to_string(),
            prefix: String::new(),
        },
    }
}

/// An SPDX id as its license page; a URL stays as it is.
fn license_url(license: &str) -> String {
    let license = license.trim();
    match license.contains("://") {
        true => license.to_string(),
        false => format!("https://spdx.org/licenses/{license}.html"),
    }
}

/// The RO-Crate of the dataset that describes a bucket, with the bucket root as its part.
fn dataset_crate(
    request: &DescribeBucketRequest,
    identifiers: &[Identifier],
    (bucket, part): (&str, &str),
    today: &str,
) -> Value {
    let mut root = json!({
        "@id": "./",
        "@type": "Dataset",
        "name": request.title.trim(),
        "description": request.description.trim(),
        "datePublished": today,
        "hasPart": [{"@id": part}]
    });
    let mut graph = vec![json!({"@id": part, "@type": "Dataset", "name": bucket})];
    if let Some(version) = (request.version.as_deref().map(str::trim)).filter(|v| !v.is_empty()) {
        root["version"] = json!(version);
    }
    if let Some(license) = (request.license.as_deref()).filter(|l| !l.trim().is_empty()) {
        let url = license_url(license);
        root["license"] = json!({"@id": url});
        graph.push(json!({"@id": url, "@type": "CreativeWork", "name": license.trim()}));
    }
    if !identifiers.is_empty() {
        root["identifier"] = (identifiers.iter())
            .map(|identifier| json!({"@id": identifier.url}))
            .collect();
        graph.extend(identifiers.iter().map(|identifier| {
            json!({
                "@id": identifier.url,
                "@type": "PropertyValue",
                "name": identifier.name,
                "value": identifier.value,
                "url": identifier.url
            })
        }));
    }
    let descriptor = json!({
        "@id": "ro-crate-metadata.json",
        "@type": "CreativeWork",
        "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
        "about": {"@id": "./"}
    });
    graph.insert(0, root);
    graph.insert(0, descriptor);
    json!({"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": graph})
}

#[cfg(test)]
#[path = "dataset_tests.rs"]
mod tests;
