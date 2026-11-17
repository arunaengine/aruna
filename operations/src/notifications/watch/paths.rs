//! Builds the permission paths that decide whether a watch may be created or delivered.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::str::FromStr;

use aruna_core::NodeId;
use aruna_core::metrics::WatchMetricReason;
use aruna_core::structs::execution::notification::NotificationKind;
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, WatchEventMask, parse_watch_path,
    watch_resource_path,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchAuthorizationDenial {
    InvalidState,
    InvalidOwner,
    TokenExpired,
    TokenRevoked,
    TokenRestricted,
    PermissionDenied,
}

impl WatchAuthorizationDenial {
    pub const fn metric_reason(self) -> WatchMetricReason {
        match self {
            Self::InvalidState => WatchMetricReason::InvalidState,
            Self::InvalidOwner => WatchMetricReason::InvalidOwner,
            Self::TokenExpired => WatchMetricReason::TokenExpired,
            Self::TokenRevoked => WatchMetricReason::TokenRevoked,
            Self::TokenRestricted => WatchMetricReason::TokenRestricted,
            Self::PermissionDenied => WatchMetricReason::PermissionDenied,
        }
    }
}

pub enum WatchAuthorization {
    Authorized,
    Denied(WatchAuthorizationDenial),
    Unavailable(String),
}

pub fn watch_permission_path(
    realm_id: RealmId,
    path_prefix: &str,
    event_mask: WatchEventMask,
) -> Option<String> {
    match event_mask.bits() {
        WatchEventMask::METADATA_CREATED => {
            let remainder = path_prefix.strip_prefix("meta/")?;
            let (raw_group_id, document_path_prefix) = remainder.split_once('/')?;
            let group_id = Ulid::from_str(raw_group_id).ok()?;
            // An empty document prefix is the group-wide watch: every dataset
            // of the group, derived as the group's `meta/` permission path.
            if group_id.is_nil()
                || raw_group_id != group_id.to_string()
                || MetadataRegistryRecord::normalize_document_path(document_path_prefix)
                    != document_path_prefix
            {
                return None;
            }
            Some(format!(
                "/{realm_id}/g/{group_id}/meta/{document_path_prefix}"
            ))
        }
        bits if bits != 0
            && bits
                & !(WatchEventMask::DATA_UPLOADED
                    | WatchEventMask::SYNC_COMPLETED
                    | WatchEventMask::SYNC_FAILED)
                == 0 =>
        {
            let resource = parse_watch_path(path_prefix)?;
            Some(object_permission_path(
                realm_id,
                resource.group_id,
                resource.node_id,
                resource.bucket,
                resource.key_prefix,
            ))
        }
        _ => None,
    }
}

pub(crate) fn event_permission_path(event: &WatchEvent) -> Option<String> {
    if event.actor.is_nil() {
        return None;
    }
    match (&event.kind, &event.detail) {
        (
            WatchEventKind::MetadataCreated,
            WatchEventDetail::MetadataCreated {
                group_id,
                document_id,
            },
        ) => metadata_permission_path(event.realm_id, &event.path, *group_id, *document_id),
        (
            WatchEventKind::DataUploaded,
            WatchEventDetail::DataUploaded {
                group_id,
                node_id,
                bucket,
                key,
                ..
            },
        ) => data_permission_path(
            event.realm_id,
            &event.path,
            *group_id,
            *node_id,
            bucket,
            key,
        ),
        (
            WatchEventKind::SyncCompleted,
            WatchEventDetail::SyncCompleted {
                group_id,
                node_id,
                bucket,
                relationship_id,
                ..
            },
        )
        | (
            WatchEventKind::SyncFailed,
            WatchEventDetail::SyncFailed {
                group_id,
                node_id,
                bucket,
                relationship_id,
                ..
            },
        ) => sync_permission_path(
            event.realm_id,
            &event.path,
            *group_id,
            *node_id,
            bucket,
            *relationship_id,
        ),
        _ => None,
    }
}

pub(crate) fn notification_permission_path(
    realm_id: RealmId,
    kind: &NotificationKind,
) -> Option<String> {
    match kind {
        NotificationKind::MetadataCreated {
            path,
            group_id,
            document_id,
            actor_user_id,
        } if !actor_user_id.is_nil() => {
            metadata_permission_path(realm_id, path, *group_id, *document_id)
        }
        NotificationKind::DataUploaded {
            path,
            group_id,
            node_id,
            bucket,
            key,
            actor_user_id,
            ..
        } if !actor_user_id.is_nil() => {
            data_permission_path(realm_id, path, *group_id, *node_id, bucket, key)
        }
        NotificationKind::SyncCompleted {
            path,
            group_id,
            node_id,
            bucket,
            relationship_id,
            actor_user_id,
            ..
        }
        | NotificationKind::SyncFailed {
            path,
            group_id,
            node_id,
            bucket,
            relationship_id,
            actor_user_id,
            ..
        } if !actor_user_id.is_nil() => sync_permission_path(
            realm_id,
            path,
            *group_id,
            *node_id,
            bucket,
            *relationship_id,
        ),
        _ => None,
    }
}

fn metadata_permission_path(
    realm_id: RealmId,
    path: &str,
    group_id: Ulid,
    document_id: Ulid,
) -> Option<String> {
    if group_id.is_nil() || document_id.is_nil() {
        return None;
    }
    let document_path = path.strip_prefix(&format!("meta/{group_id}/"))?;
    if document_path.is_empty()
        || MetadataRegistryRecord::normalize_document_path(document_path) != document_path
    {
        return None;
    }
    Some(MetadataRegistryRecord::permission_path_for(
        &realm_id,
        group_id,
        document_path,
        document_id,
    ))
}

fn data_permission_path(
    realm_id: RealmId,
    path: &str,
    group_id: Ulid,
    node_id: NodeId,
    bucket: &str,
    key: &str,
) -> Option<String> {
    if group_id.is_nil()
        || bucket.is_empty()
        || key.is_empty()
        || path != watch_resource_path(group_id, node_id, bucket, key)
    {
        return None;
    }
    Some(object_permission_path(
        realm_id, group_id, node_id, bucket, key,
    ))
}

fn sync_permission_path(
    realm_id: RealmId,
    path: &str,
    group_id: Ulid,
    node_id: NodeId,
    bucket: &str,
    relationship_id: Ulid,
) -> Option<String> {
    let resource = parse_watch_path(path)?;
    if group_id.is_nil()
        || relationship_id.is_nil()
        || resource.group_id != group_id
        || resource.node_id != node_id
        || resource.bucket != bucket
    {
        return None;
    }
    if resource.key_prefix.is_empty() {
        Some(bucket_permission_path(realm_id, group_id, node_id, bucket))
    } else {
        Some(object_permission_path(
            realm_id,
            group_id,
            node_id,
            bucket,
            resource.key_prefix,
        ))
    }
}
