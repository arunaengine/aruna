use std::str::FromStr;

use aruna_core::NodeId;
use aruna_core::structs::{
    AuthContext, MetadataRegistryRecord, Permission, RealmId, WatchEventMask, WatchSubscription,
    blob_bucket_permission_path, parse_data_watch_resource_path,
};
use tracing::warn;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::notifications::placement::filter_locally_held_watch_subscriptions;

pub struct AuthorizedWatchSubscriptions {
    pub subscriptions: Vec<WatchSubscription>,
    pub dropped: bool,
    pub check_failed: bool,
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
            if group_id.is_nil()
                || raw_group_id != group_id.to_string()
                || MetadataRegistryRecord::normalize_document_path(document_path_prefix)
                    != document_path_prefix
            {
                return None;
            }
            Some(format!("/{realm_id}/g/{group_id}/meta/**"))
        }
        WatchEventMask::DATA_UPLOADED => {
            let resource = parse_data_watch_resource_path(path_prefix)?;
            Some(blob_bucket_permission_path(
                realm_id,
                resource.group_id,
                resource.node_id,
                resource.bucket,
            ))
        }
        _ => None,
    }
}

pub async fn filter_authorized_watch_subscriptions(
    context: &DriverContext,
    realm_id: RealmId,
    realm_config: &aruna_core::structs::RealmConfigDocument,
    local_node_id: NodeId,
    subscriptions: Vec<WatchSubscription>,
) -> Result<AuthorizedWatchSubscriptions, String> {
    let (subscriptions, found_stale) =
        filter_locally_held_watch_subscriptions(subscriptions, realm_config, local_node_id)
            .map_err(|error| error.to_string())?;
    let mut authorized = Vec::with_capacity(subscriptions.len());
    let mut dropped = found_stale;
    let mut check_failed = false;

    for subscription in subscriptions {
        let Some(permission_path) =
            watch_permission_path(realm_id, &subscription.path_prefix, subscription.event_mask)
        else {
            dropped = true;
            continue;
        };
        if subscription.owner.is_nil() || subscription.owner.realm_id != realm_id {
            dropped = true;
            continue;
        }
        let auth_context = AuthContext {
            user_id: subscription.owner,
            realm_id,
            path_restrictions: None,
        };
        match drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context,
                path: permission_path.clone(),
                required_permission: Permission::READ,
            }),
            context,
        )
        .await
        {
            Ok(true) => authorized.push(subscription),
            Ok(false) => dropped = true,
            Err(error) => {
                dropped = true;
                check_failed = true;
                warn!(
                    owner = %subscription.owner,
                    path = %permission_path,
                    %error,
                    "Watch permission re-check failed closed"
                );
            }
        }
    }

    Ok(AuthorizedWatchSubscriptions {
        subscriptions: authorized,
        dropped,
        check_failed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{WatchEventKind, data_watch_resource_path};

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn permission_paths_are_derived_from_canonical_resource_identity() {
        let realm_id = RealmId([1u8; 32]);
        let group_id = Ulid::from_bytes([2u8; 16]);
        let node_id = node(3);
        let data_prefix = data_watch_resource_path(group_id, node_id, "bucket", "reports/");

        assert_eq!(
            watch_permission_path(
                realm_id,
                &data_prefix,
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            ),
            Some(blob_bucket_permission_path(
                realm_id, group_id, node_id, "bucket"
            ))
        );
        assert_eq!(
            watch_permission_path(
                realm_id,
                &format!("meta/{group_id}/datasets/project"),
                WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            ),
            Some(format!("/{realm_id}/g/{group_id}/meta/**"))
        );
    }

    #[test]
    fn mixed_event_mask_has_no_canonical_permission_identity() {
        let mask = WatchEventMask::from_kinds([
            WatchEventKind::MetadataCreated,
            WatchEventKind::DataUploaded,
        ]);
        assert!(watch_permission_path(RealmId([1u8; 32]), "meta/x/y", mask).is_none());
    }

    #[test]
    fn nil_group_ids_are_not_canonical_resource_identities() {
        let realm_id = RealmId([1u8; 32]);
        let metadata_mask = WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]);
        let data_mask = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);
        let nil_group = Ulid::nil();

        assert!(
            watch_permission_path(
                realm_id,
                &format!("meta/{nil_group}/datasets"),
                metadata_mask,
            )
            .is_none()
        );
        assert!(
            watch_permission_path(
                realm_id,
                &data_watch_resource_path(nil_group, node(3), "bucket", ""),
                data_mask,
            )
            .is_none()
        );
    }
}
