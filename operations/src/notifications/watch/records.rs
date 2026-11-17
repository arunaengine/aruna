//! Validates watch subscriptions and builds the replication records of their writes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::IoError;
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentOutboxRecord,
    DocumentSyncRevision, DocumentTarget,
};
use aruna_core::metrics::WatchMetricReason;
use aruna_core::structs::execution::notification_watch::{
    MAX_PREFIX_LEN, WatchAuthorizationBinding, WatchEventMask, WatchSubscription, parse_watch_key,
};
use aruna_core::structs::placement::record::PlacementRef;
use thiserror::Error;
use ulid::Ulid;

use crate::sync::outbox_record::new_identified_record;

/// Stable reject reason for a cap-exceeded create; matched verbatim by the
/// holder proxy to surface a 409 to the API layer.
pub(crate) const WATCH_CAP_REACHED: &str = "notification watch subscription cap reached";

/// Stable reject reason for an unauthorized create; matched verbatim by the
/// holder proxy to surface a 403 to the API layer.
pub(crate) const WATCH_SUBSCRIPTION_UNAUTHORIZED: &str =
    "watch subscription owner lacks READ on the watched path";
pub(crate) const WATCH_SUBSCRIPTION_UNAVAILABLE: &str =
    "watch subscription authorization is temporarily unavailable";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WatchSubscriptionError {
    #[error("{WATCH_SUBSCRIPTION_UNAUTHORIZED}: {}", .0.as_str())]
    Unauthorized(WatchMetricReason),
    #[error("{WATCH_SUBSCRIPTION_UNAVAILABLE}: {0}")]
    AuthorizationUnavailable(String),
    #[error("watch path prefix must not be empty")]
    EmptyPrefix,
    #[error("watch path prefix must not start with a slash")]
    LeadingSlash,
    #[error("watch path prefix exceeds maximum length")]
    PrefixTooLong,
    #[error("watch event mask must select at least one event")]
    EmptyMask,
    #[error("watch event mask contains an unknown event")]
    InvalidMask,
    #[error("notification watch subscription cap reached")]
    CapExceeded,
    #[error("{0}")]
    Storage(String),
}

impl From<IoError> for WatchSubscriptionError {
    fn from(error: IoError) -> Self {
        WatchSubscriptionError::Storage(error.to_string())
    }
}

/// The revision and sync outbox record a holder writes with a watch change.
#[derive(Clone, Debug, PartialEq)]
pub struct WatchReplication {
    pub(crate) revision: DocumentChange,
    pub(crate) outbox: DocumentOutboxRecord,
}

pub(crate) fn validated_subscription(
    owner: UserId,
    path_prefix: String,
    event_mask: WatchEventMask,
    now_ms: u64,
    authorization: WatchAuthorizationBinding,
) -> Result<WatchSubscription, WatchSubscriptionError> {
    validate_subscription_fields(&path_prefix, event_mask)?;
    if !authorization.is_valid() {
        return Err(WatchSubscriptionError::Unauthorized(
            WatchMetricReason::InvalidState,
        ));
    }

    Ok(WatchSubscription::new_with_authorization(
        owner,
        path_prefix,
        event_mask,
        now_ms,
        authorization,
    ))
}

pub(crate) fn validate_subscription_fields(
    path_prefix: &str,
    event_mask: WatchEventMask,
) -> Result<(), WatchSubscriptionError> {
    if path_prefix.is_empty() {
        return Err(WatchSubscriptionError::EmptyPrefix);
    }
    // Event paths have no leading slash, so such a prefix can never match.
    // Reject it behind the API validation.
    if path_prefix.starts_with('/') {
        return Err(WatchSubscriptionError::LeadingSlash);
    }
    if path_prefix.len() > MAX_PREFIX_LEN {
        return Err(WatchSubscriptionError::PrefixTooLong);
    }
    if event_mask.is_empty() {
        return Err(WatchSubscriptionError::EmptyMask);
    }
    if event_mask.bits()
        & !(WatchEventMask::METADATA_CREATED
            | WatchEventMask::DATA_UPLOADED
            | WatchEventMask::SYNC_COMPLETED
            | WatchEventMask::SYNC_FAILED)
        != 0
    {
        return Err(WatchSubscriptionError::InvalidMask);
    }

    Ok(())
}

pub(crate) fn watch_subscription_target(owner: UserId, watch_id: Ulid) -> DocumentTarget {
    DocumentTarget::WatchSubscription { owner, watch_id }
}

pub(crate) fn watch_upsert_replication(
    local_node_id: aruna_core::NodeId,
    subscription: &WatchSubscription,
) -> Result<WatchReplication, WatchSubscriptionError> {
    let outbox_id = Ulid::generate();
    let revision = DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: outbox_id,
            actor: local_node_id,
            updated_at_ms: subscription.created_at_ms,
        },
        kind: DocumentChangeKind::Upsert,
        placement: PlacementRef::NIL,
    };
    let target = watch_subscription_target(subscription.owner, subscription.watch_id);
    let outbox = new_identified_record(
        outbox_id,
        local_node_id,
        target,
        Vec::new(),
        DocumentOutboxEvent::Upsert {
            bytes: subscription
                .to_bytes()
                .map_err(|error| WatchSubscriptionError::Storage(error.to_string()))?,
            change: revision,
        },
        PlacementRef::NIL,
        false,
    );
    Ok(WatchReplication { revision, outbox })
}

pub(crate) fn watch_delete_replication(
    local_node_id: aruna_core::NodeId,
    owner: UserId,
    watch_id: Ulid,
    now_ms: u64,
) -> WatchReplication {
    let outbox_id = Ulid::generate();
    let revision = DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 2,
            event_id: outbox_id,
            actor: local_node_id,
            updated_at_ms: now_ms,
        },
        kind: DocumentChangeKind::Delete,
        placement: PlacementRef::NIL,
    };
    let outbox = new_identified_record(
        outbox_id,
        local_node_id,
        watch_subscription_target(owner, watch_id),
        Vec::new(),
        DocumentOutboxEvent::Delete { change: revision },
        PlacementRef::NIL,
        false,
    );
    WatchReplication { revision, outbox }
}

pub(crate) fn decode_stored_subscription(
    key: &[u8],
    value: &[u8],
) -> Result<WatchSubscription, WatchSubscriptionError> {
    let (key_owner, key_watch_id) =
        parse_watch_key(key).map_err(|error| WatchSubscriptionError::Storage(error.to_string()))?;
    let subscription = WatchSubscription::from_bytes(value)
        .map_err(|error| WatchSubscriptionError::Storage(error.to_string()))?;
    if key_owner.is_nil()
        || key_watch_id.is_nil()
        || subscription.owner != key_owner
        || subscription.watch_id != key_watch_id
    {
        return Err(WatchSubscriptionError::Storage(
            "stored watch subscription key does not match payload identity".to_string(),
        ));
    }
    validate_subscription_fields(&subscription.path_prefix, subscription.event_mask).map_err(
        |error| {
            WatchSubscriptionError::Storage(format!(
                "stored watch subscription violates invariants: {error}"
            ))
        },
    )?;
    if !subscription.authorization.is_valid()
        || subscription.authorization.watch_path_prefix != subscription.path_prefix
    {
        return Err(WatchSubscriptionError::Storage(
            "stored watch subscription has invalid authorization binding".to_string(),
        ));
    }
    Ok(subscription)
}
