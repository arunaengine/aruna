use std::str::FromStr;

use aruna_core::NodeId;
use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{
    AuthContext, MetadataRegistryRecord, Permission, RealmId, WatchEventMask, WatchSubscription,
    blob_bucket_permission_path, parse_data_watch_resource_path,
};
use aruna_core::types::UserId;
use tracing::warn;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::notifications::placement::filter_locally_held_watch_subscriptions;
use crate::notifications::watch::subscriptions::list_watch_subscriptions;

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

/// The single canonical authorization result every watch surface shares: the
/// owner must still hold READ on the permission path derived from the watched
/// prefix. A prefix with no canonical resource identity, and any non-user owner,
/// is unauthorized. A check that cannot be evaluated is an error, never a grant,
/// so callers fail closed.
pub async fn is_watch_authorized(
    context: &DriverContext,
    realm_id: RealmId,
    owner: UserId,
    path_prefix: &str,
    event_mask: WatchEventMask,
) -> Result<bool, String> {
    let Some(permission_path) = watch_permission_path(realm_id, path_prefix, event_mask) else {
        return Ok(false);
    };
    if owner.is_nil() || owner.realm_id != realm_id {
        return Ok(false);
    }
    match drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: owner,
                realm_id,
                path_restrictions: None,
            },
            path: permission_path,
            required_permission: Permission::READ,
        }),
        context,
    )
    .await
    {
        Ok(allowed) => Ok(allowed),
        // A path whose realm, group or authorization state is absent is simply
        // unreadable. Answering it exactly as a denied role keeps a watch from
        // separating "does not exist" from "you may not read it", which is the
        // same choice the metadata surface makes.
        Err(
            AuthorizationError::InvalidRealmId
            | AuthorizationError::InvalidGroupId
            | AuthorizationError::GroupNotFound
            | AuthorizationError::AuthDocNotFound,
        ) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

/// Holder-side enumeration of one user's watches, filtered through the same
/// authorization result delivery uses, so a revoked watch stops being visible as
/// well as stopping delivery.
pub async fn list_authorized_watch_subscriptions(
    context: &DriverContext,
    owner: UserId,
) -> Result<Vec<WatchSubscription>, String> {
    let subscriptions = list_watch_subscriptions(&context.storage_handle, owner)
        .await
        .map_err(|error| error.to_string())?;
    let mut authorized = Vec::with_capacity(subscriptions.len());
    for subscription in subscriptions {
        if is_watch_authorized(
            context,
            owner.realm_id,
            subscription.owner,
            &subscription.path_prefix,
            subscription.event_mask,
        )
        .await?
        {
            authorized.push(subscription);
        }
    }
    Ok(authorized)
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
        match is_watch_authorized(
            context,
            realm_id,
            subscription.owner,
            &subscription.path_prefix,
            subscription.event_mask,
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
                    path = %subscription.path_prefix,
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
    use aruna_storage::FjallStorage;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    // Anonymous callers carry a nil user id and own no inbox to deliver into, so
    // a watch stays a user-plane feature even where the watched resource is
    // publicly readable. The nil owner is refused before any role is evaluated.
    #[tokio::test]
    async fn anonymous_owner_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let context = DriverContext {
            storage_handle: FjallStorage::open(dir.path().to_str().expect("temp path"))
                .expect("storage opens"),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        let realm_id = RealmId([9u8; 32]);
        let prefix = data_watch_resource_path(Ulid::from_bytes([4u8; 16]), node(3), "bucket", "");

        assert_eq!(
            is_watch_authorized(
                &context,
                realm_id,
                UserId::nil(realm_id),
                &prefix,
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            )
            .await,
            Ok(false)
        );
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
