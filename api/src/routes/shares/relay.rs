//! Reaches the key ranges other members of a distributed bucket hold for a share visit, as the
//! link's creator with one `share:` delegation per request (D33).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, UNIX_EPOCH};

use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::share::{SHARE_CREDENTIAL_PREFIX, ShareLink};
use aruna_operations::s3::object::pages::{ObjectPage, RangeCall, RangeRelay};
use axum::http::StatusCode;
use bytes::Bytes;
use s3s::dto::StreamingBlob;

use super::visit::{range_unavailable, unavailable};
use crate::error::{ServerError, ServerResult};
use crate::mcp::data::ObjectOutput;
use crate::s3::service::ArunaS3Service;
use crate::server::state::ServerState;

/// What a visit sends other members: the creator narrowed as `auth` is, until `expiry` (Unix
/// seconds). It is no stored credential and has no secret; holders check it on their own path.
pub(crate) fn share_access(
    state: &ServerState,
    link: &ShareLink,
    auth: &AuthContext,
    expiry: u64,
) -> UserAccess {
    // An expiry past what `SystemTime` holds becomes the latest one it always holds.
    let expiry = UNIX_EPOCH
        .checked_add(Duration::from_secs(expiry))
        .unwrap_or(UNIX_EPOCH + Duration::from_secs(u32::MAX.into()));
    UserAccess {
        access_key: format!("{SHARE_CREDENTIAL_PREFIX}{}", link.id),
        user_identity: link.created_by,
        group_id: link.group_id,
        secret: EncryptedS3Secret::empty(),
        expiry,
        path_restrictions: auth.path_restrictions.clone(),
        issued_by: *state.get_node_id().as_bytes(),
        revoked_at: None,
    }
}

/// A member's refusal as a visitor's answer; a denial reads like an ended link.
pub(crate) fn member_error(error: &s3s::S3Error) -> ServerError {
    let message = error.message().unwrap_or_default().to_string();
    match error.status_code().map(|status| status.as_u16()) {
        Some(403) => unavailable(),
        Some(404) => ServerError::NotFound,
        Some(400) => ServerError::BadRequestReason(message),
        _ => range_unavailable("cannot be reached"),
    }
}

/// The body of a visitor's upload.
pub(crate) type UploadStream = BackendStream<Result<Bytes, StreamError>>;

/// An upload body to send on, counting into `counted` the bytes it yields.
pub(crate) fn counted_blob(stream: UploadStream, counted: Arc<AtomicU64>) -> StreamingBlob {
    // The lock only lends the stream `Sync`; one task polls it.
    let stream = Mutex::new(stream.0);
    StreamingBlob::wrap(futures_util::stream::poll_fn(move |cx| {
        let mut stream = stream.lock().unwrap_or_else(|error| error.into_inner());
        let item = stream.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(bytes))) = &item {
            counted.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        item
    }))
}

/// A coordinator's refusal of a visitor's upload as the visitor's answer.
pub(crate) fn upload_error(error: &s3s::S3Error) -> ServerError {
    match error.code().as_str() {
        "PreconditionFailed" => ServerError::Refused(
            StatusCode::CONFLICT,
            "name_taken",
            "another file took this name during the upload; upload again".to_string(),
        ),
        "QuotaExceeded" => ServerError::Conflict(error.message().unwrap_or_default().to_string()),
        _ if error
            .status_code()
            .is_none_or(|code| code.is_server_error()) =>
        {
            range_unavailable("takes no uploads")
        }
        _ => member_error(error),
    }
}

/// Lists the ranges other members keep for one visit.
pub(crate) struct ShareRelay<'a> {
    pub state: &'a ServerState,
    pub user: UserAccess,
}

impl RangeRelay for ShareRelay<'_> {
    type Object = ObjectOutput;
    type Error = ServerError;

    async fn list_range(
        &self,
        call: RangeCall<'_>,
    ) -> ServerResult<Option<ObjectPage<ObjectOutput>>> {
        let state = self.state;
        let service =
            ArunaS3Service::new(state.get_ctx(), state.get_realm_id(), state.get_node_id()).await;
        let nodes = (call.set_id, call.node_id, call.copy);
        let asked = (call.request, call.cursor);
        let page = match service
            .member_list(&self.user, nodes, call.range, asked)
            .await
        {
            Ok(page) => page,
            // Another holder may answer the range instead.
            Err(error)
                if error
                    .status_code()
                    .is_none_or(|code| code.is_server_error()) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(member_error(&error)),
        };
        let objects = (page.objects.into_iter())
            .map(|object| ObjectOutput {
                key: object.key,
                version_id: String::new(),
                size: object.size,
                etag: object.etag,
                last_modified: (object.last_modified)
                    .and_then(|time| chrono::DateTime::parse_from_rfc3339(&time).ok())
                    .map(|time| time.with_timezone(&chrono::Utc).to_rfc3339()),
                content_type: None,
                referenced: false,
                content_id: None,
                link: None,
            })
            .collect();
        Ok(Some(ObjectPage {
            objects,
            prefixes: page.prefixes,
            next_cursor: page.next_cursor,
            ..ObjectPage::default()
        }))
    }

    fn is_fault(error: &ServerError) -> bool {
        error.status_code().is_server_error()
    }

    fn checks_here(&self) -> bool {
        true
    }
}
