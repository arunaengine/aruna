use std::collections::HashSet;
use std::str::FromStr;

use aruna_core::NodeId;
use aruna_core::auth::TOKEN_REVOCATION_LIST_KEY;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::API_STATE_KEYSPACE;
use aruna_core::structs::{
    AuthContext, MetadataRegistryRecord, Permission, RealmId, WatchAuthorizationBinding,
    WatchEventMask, WatchSubscription, blob_object_permission_path, parse_data_watch_resource_path,
};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_secs;
use byteview::ByteView;
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

enum WatchAuthorization {
    Authorized,
    Denied,
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
        WatchEventMask::DATA_UPLOADED => {
            let resource = parse_data_watch_resource_path(path_prefix)?;
            Some(blob_object_permission_path(
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
    authorization: &WatchAuthorizationBinding,
) -> Result<bool, String> {
    Ok(matches!(
        evaluate_watch_authorization(
            context,
            realm_id,
            owner,
            path_prefix,
            event_mask,
            authorization,
        )
        .await?,
        WatchAuthorization::Authorized
    ))
}

async fn evaluate_watch_authorization(
    context: &DriverContext,
    realm_id: RealmId,
    owner: UserId,
    path_prefix: &str,
    event_mask: WatchEventMask,
    authorization: &WatchAuthorizationBinding,
) -> Result<WatchAuthorization, String> {
    let Some(permission_path) = watch_permission_path(realm_id, path_prefix, event_mask) else {
        return Ok(WatchAuthorization::Denied);
    };
    if owner.is_nil()
        || owner.realm_id != realm_id
        || !authorization.is_valid()
        || unix_timestamp_secs() > authorization.expires_at_secs
        || token_is_revoked(context, &authorization.token_hash).await?
    {
        return Ok(WatchAuthorization::Denied);
    }
    match drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: owner,
                realm_id,
                path_restrictions: authorization.path_restrictions.clone(),
            },
            path: permission_path,
            required_permission: Permission::READ,
        }),
        context,
    )
    .await
    {
        Ok(true) => Ok(WatchAuthorization::Authorized),
        Ok(false) => Ok(WatchAuthorization::Denied),
        // A path whose realm, group or authorization state is absent is simply
        // unreadable. Answering it exactly as a denied role keeps a watch from
        // separating "does not exist" from "you may not read it", which is the
        // same choice the metadata surface makes. Internally, retain that the
        // state was unavailable so interest publication leaves a retry marker.
        Err(
            error @ (AuthorizationError::InvalidRealmId
            | AuthorizationError::InvalidGroupId
            | AuthorizationError::GroupNotFound
            | AuthorizationError::AuthDocNotFound),
        ) => Ok(WatchAuthorization::Unavailable(error.to_string())),
        Err(error) => Err(error.to_string()),
    }
}

async fn token_is_revoked(context: &DriverContext, token_hash: &str) -> Result<bool, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: API_STATE_KEYSPACE.to_string(),
            key: ByteView::from(TOKEN_REVOCATION_LIST_KEY),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => postcard::from_bytes::<HashSet<String>>(&bytes)
            .map(|revoked| revoked.contains(token_hash))
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(false),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

/// Holder-side enumeration of one user's watches through the same authorization
/// result delivery uses. Revoked watches retain only their opaque deletion id;
/// protected watch details are redacted so the owner can still release quota.
pub async fn list_authorized_watch_subscriptions(
    context: &DriverContext,
    owner: UserId,
) -> Result<Vec<WatchSubscription>, String> {
    let subscriptions = list_watch_subscriptions(&context.storage_handle, owner)
        .await
        .map_err(|error| error.to_string())?;
    let mut authorized = Vec::with_capacity(subscriptions.len());
    for mut subscription in subscriptions {
        if !is_watch_authorized(
            context,
            owner.realm_id,
            subscription.owner,
            &subscription.path_prefix,
            subscription.event_mask,
            &subscription.authorization,
        )
        .await?
        {
            subscription.path_prefix.clear();
            subscription.event_mask = WatchEventMask::empty();
            subscription.created_at_ms = 0;
        }
        authorized.push(subscription);
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
        match evaluate_watch_authorization(
            context,
            realm_id,
            subscription.owner,
            &subscription.path_prefix,
            subscription.event_mask,
            &subscription.authorization,
        )
        .await
        {
            Ok(WatchAuthorization::Authorized) => authorized.push(subscription),
            Ok(WatchAuthorization::Denied) => dropped = true,
            Ok(WatchAuthorization::Unavailable(error)) | Err(error) => {
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
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, RealmAuthorizationDocument, RealmConfigDocument,
        RealmNodeKind, WatchEventKind, data_watch_resource_path,
    };
    use aruna_storage::FjallStorage;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    // A public (Everyone) role granting READ on the watched bucket would let a
    // nil caller through on the permission path alone. Anonymous callers own no
    // inbox to deliver into, so the nil-owner guard must still refuse the watch
    // before any role is evaluated, even where the resource is publicly readable.
    #[tokio::test]
    async fn anonymous_owner_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let realm_id = RealmId([9u8; 32]);
        let owner = UserId::new(Ulid::from_bytes([1u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([4u8; 16]);
        let node_id = node(3);

        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let mut group_auth =
            GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
        group_auth
            .roles
            .values_mut()
            .find(|role| role.name == "viewer")
            .expect("viewer role")
            .assigned_users
            .insert(UserId::nil(realm_id));
        for (key, value) in [
            (
                realm_id.as_bytes().to_vec(),
                realm_auth.to_bytes(&actor).unwrap(),
            ),
            (
                group_id.to_bytes().to_vec(),
                group_auth.to_bytes(&actor).unwrap(),
            ),
        ] {
            match storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: AUTH_KEYSPACE.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await
            {
                Event::Storage(StorageEvent::WriteResult { .. }) => {}
                other => panic!("unexpected auth write event: {other:?}"),
            }
        }

        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        let prefix = data_watch_resource_path(group_id, node_id, "bucket", "");

        assert_eq!(
            is_watch_authorized(
                &context,
                realm_id,
                UserId::nil(realm_id),
                &prefix,
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
                &WatchAuthorizationBinding::default(),
            )
            .await,
            Ok(false)
        );
    }

    #[tokio::test]
    async fn missing_authorization_is_denied_and_marked_retryable() {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let realm_id = RealmId([8u8; 32]);
        let owner = UserId::new(Ulid::from_bytes([1u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([2u8; 16]);
        let node_id = node(3);
        let path = data_watch_resource_path(group_id, node_id, "bucket", "");
        let event_mask = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);
        let subscription = WatchSubscription::new(owner, path.clone(), event_mask, 1);
        let mut realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        realm_config.ensure_node(node_id, RealmNodeKind::Server);
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        assert_eq!(
            is_watch_authorized(
                &context,
                realm_id,
                owner,
                &path,
                event_mask,
                &subscription.authorization,
            )
            .await,
            Ok(false),
            "external callers still fail closed without exposing missing state"
        );
        let filtered = filter_authorized_watch_subscriptions(
            &context,
            realm_id,
            &realm_config,
            node_id,
            vec![subscription],
        )
        .await
        .expect("filter succeeds");
        assert!(filtered.subscriptions.is_empty());
        assert!(filtered.dropped);
        assert!(filtered.check_failed, "interest publication must retry");
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
            Some(blob_object_permission_path(
                realm_id, group_id, node_id, "bucket", "reports/"
            ))
        );
        assert_eq!(
            watch_permission_path(
                realm_id,
                &format!("meta/{group_id}/datasets/project"),
                WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            ),
            Some(format!("/{realm_id}/g/{group_id}/meta/datasets/project"))
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
