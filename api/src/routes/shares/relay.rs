//! Reaches the key ranges other members of a distributed bucket hold for a share visit, as the
//! link's creator with one `share:` delegation per request (D33).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, UNIX_EPOCH};

use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::outbound::RangeRefusal;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::share::{SHARE_CREDENTIAL_PREFIX, ShareLink};
use aruna_operations::s3::object::member_read::{MemberFailure, MemberRefusal};
use aruna_operations::s3::object::range_list::MemberAccess;
use axum::http::StatusCode;
use bytes::Bytes;
use s3s::dto::StreamingBlob;

use super::visit::{range_unavailable, unavailable};
use crate::error::ServerError;
use crate::s3::service::delegated_caller;
use crate::server::state::ServerState;

/// What a visit sends other members about `info`'s bucket: the creator narrowed as `auth` is,
/// until `expiry` (Unix seconds). No stored credential and no secret; holders check it there.
pub(crate) fn share_access(
    state: &ServerState,
    link: &ShareLink,
    (auth, info): (&AuthContext, &BucketInfo),
    expiry: u64,
) -> UserAccess {
    // An expiry past what `SystemTime` holds becomes the latest one it always holds.
    let expiry = UNIX_EPOCH
        .checked_add(Duration::from_secs(expiry))
        .unwrap_or(UNIX_EPOCH + Duration::from_secs(u32::MAX.into()));
    UserAccess {
        access_key: format!("{SHARE_CREDENTIAL_PREFIX}{}", link.id),
        user_identity: link.created_by,
        // A dataset's part may lie in a bucket of another group than the dataset's.
        group_id: info.group_id,
        secret: EncryptedS3Secret::empty(),
        expiry,
        path_restrictions: auth.path_restrictions.clone(),
        issued_by: *state.get_node_id().as_bytes(),
        revoked_at: None,
    }
}

/// How a visit's listing reaches other members: as `share_access` names the visitor, and
/// checked again here for each entry.
pub(crate) fn share_listing(
    state: &ServerState,
    link: &ShareLink,
    (auth, info): (&AuthContext, &BucketInfo),
    expiry: u64,
) -> MemberAccess {
    let user = share_access(state, link, (auth, info), expiry);
    MemberAccess::Delegated(delegated_caller(&user))
}

/// A member's refusal as a visitor's answer; a denial reads like an ended link.
fn refused_visit(status: Option<u16>, message: String) -> ServerError {
    match status {
        Some(403) => unavailable(),
        Some(404) => ServerError::NotFound,
        Some(400) => ServerError::BadRequestReason(message),
        _ => range_unavailable("cannot be reached"),
    }
}

/// A member's refusal of a listing as a visitor's answer.
pub(crate) fn listing_refusal(refusal: RangeRefusal) -> ServerError {
    refused_visit(Some(refusal.status), refusal.message)
}

/// A failed read at another member as a visitor's answer.
pub(crate) fn read_refusal(failure: MemberFailure) -> ServerError {
    let status = failure.refusal.status();
    let message = match failure.refusal {
        MemberRefusal::Refused { message, .. } => message,
        _ => String::new(),
    };
    refused_visit(Some(status), message)
}

fn member_error(error: &s3s::S3Error) -> ServerError {
    let status = error.status_code().map(|status| status.as_u16());
    refused_visit(status, error.message().unwrap_or_default().to_string())
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
