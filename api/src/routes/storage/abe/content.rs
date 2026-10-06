//! Reads a pinned version with an archive-bound transient object key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::download::{self, AdmissionError};
use crate::object_key::ObjectKey;
use crate::rate_limit::LocalKey;
use crate::routes::execution::jobs::range_request;
use aruna_operations::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};
use axum::response::Response;
use http::{HeaderMap, header};

fn read_error(error: GetObjectError) -> ServerError {
    match error {
        GetObjectError::Abe(e) => abe_error(e),
        GetObjectError::InvalidRange => ServerError::Refused(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "invalid_range",
            "the range is not satisfiable".into(),
        ),
        GetObjectError::ConversionError(aruna_core::errors::ConversionError::BucketKey(
            aruna_core::structs::storage::encryption::BucketKeyError::Locked(_),
        )) => abe_error(AbeError::Required),
        GetObjectError::NoSuchKey | GetObjectError::NoSuchVersion => ServerError::NotFound,
        _ => ServerError::ServiceUnavailable,
    }
}
#[utoipa::path(get, path = "/data/blobs/content", tag = "data/blobs",
    summary = "Read a pinned object",
    description = r#"Streams the plaintext of one pinned version, optionally over one HTTP range.

**Authentication**: A realm bearer token and full READ authorization are required before key admission.

**Behavior**
- Binary fields use padded base64; version and request ids use ULIDs.
- Encrypted versions without an envelope require bucket unlock."#,
    params(("bucket" = String, Query, description = "Node-local S3 bucket name"), ("key" = String, Query, description = "Literal object key"), ("version_id" = String, Query, description = "Pinned version ULID"),
        ("x-aruna-object-key" = Option<String>, Header, description = "Padded base64 of a 32-byte object private key, removed before tracing"),
        ("Range" = Option<String>, Header, description = "One HTTP bytes range")),
    responses((status = 200, description = "Pinned plaintext stream", content_type = "application/octet-stream"),
        (status = 206, description = "Pinned plaintext range", content_type = "application/octet-stream"),
        (status = 400, body = ErrorResponse, description = "Invalid encoding or range syntax"), (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 403, body = ErrorResponse, description = "READ refused or wrong object key"), (status = 404, body = ErrorResponse, description = "Version missing"),
        (status = 409, body = ErrorResponse, description = "Envelope pending or stale version"), (status = 416, body = ErrorResponse, description = "Range not satisfiable"),
        (status = 423, body = ErrorResponse, description = "Locked, object key required"),
        (status = 503, body = ErrorResponse, description = "Download or storage unavailable")), security(("bearer_auth" = [])))]
pub async fn content(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(object): Extension<ObjectKey>,
    Query(query): Query<VersionQuery>,
    headers: HeaderMap,
) -> ServerResult<Response> {
    let auth = crate::auth::require_realm_auth(&state, auth)?;
    let (version, info) = authorize_version(&state, &auth, &query).await?;
    let private = object.0.map_err(|_| ServerError::BadRequest)?;
    let range = range_request(&headers).map_err(|_| ServerError::BadRequest)?;
    let mut operation = GetObjectOperation::new(GetObjectInput {
        bucket: query.bucket.clone(),
        key: query.key.clone(),
        version_id: Some(version),
        range,
        group_id: info.group_id,
        user_identity: auth.user_id,
        node_id: state.get_node_id(),
    })
    .with_restrictions(auth.path_restrictions.clone());
    let keyed = private.is_some();
    if let Some(private) = private {
        let (envelope, archive) = read_envelope(&state, &query, version).await?;
        operation = operation.with_object(envelope, archive, private);
    }
    let permit = download::admit(&state, LocalKey::User(auth.user_id)).map_err(|e| match e {
        AdmissionError::Total => ServerError::ServiceUnavailable,
        AdmissionError::User => ServerError::Refused(
            StatusCode::TOO_MANY_REQUESTS,
            "download_capacity",
            "download capacity exhausted".into(),
        ),
    })?;
    let result = match drive(operation, &state.get_ctx()).await {
        Err(GetObjectError::ConversionError(aruna_core::errors::ConversionError::BucketKey(
            aruna_core::structs::storage::encryption::BucketKeyError::Locked(_),
        ))) if !keyed => {
            // A version without an envelope needs bucket unlock, not an object key.
            read_envelope(&state, &query, version).await?;
            return Err(abe_error(AbeError::Required));
        }
        result => result.map_err(read_error)?,
    };
    let mut response = Response::new(download::body(result.blob, permit));
    let (status, length) = match result.resolved_range {
        Some(range) => {
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                range
                    .content_range
                    .parse()
                    .map_err(|_| ServerError::BadRequest)?,
            );
            (StatusCode::PARTIAL_CONTENT, range.content_length as u64)
        }
        None => (StatusCode::OK, result.info.size),
    };
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        length
            .to_string()
            .parse()
            .map_err(|_| ServerError::BadRequest)?,
    );
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        header::ACCEPT_RANGES,
        http::HeaderValue::from_static("bytes"),
    );
    Ok(response)
}
