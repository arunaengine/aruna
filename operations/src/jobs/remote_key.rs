//! Waits on bucket keys of another node over the job-control transport, and the wakes back.
//! Registration and wake grant no read: the staging read still passes the key node's admission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::NodeId;
use aruna_core::jobs::{JobRequest, JobResponse};
use aruna_core::keyspaces::PATHS_INDEX_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::execution::job::{JobId, JobRecord, JobState, KeyWait};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{HashIndex, object_permission_path};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::time::unix_timestamp_millis;
use tracing::warn;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::jobs::key_wake::locked_contents;
use crate::jobs::route::{JobRouteOperation, JobRouteOutcome};
use crate::jobs::runtime::key_unlocked;
use crate::jobs::store::{
    ack_wake, iter_prefix_page, owed_wakes, queue_remote_wakes, read_job_record, read_key_waits,
    register_remote_wait, satisfy_key_wait,
};

/// Contents one registration may name; more is refused rather than scanned.
pub const MAX_AWAITED: usize = 1024;

/// Key node: registers `job_id` of `peer` on the locked keys of `contents`, then answers only
/// the keys still locked. A key unlocked in between is either seen here or wakes the job later.
pub(crate) async fn register_waits(
    context: &DriverContext,
    (node_id, peer): (NodeId, NodeId),
    auth: &AuthContext,
    job_id: JobId,
    contents: &[[u8; 32]],
) -> Result<Vec<KeyWait>, String> {
    if contents.len() > MAX_AWAITED {
        return Err(format!("at most {MAX_AWAITED} contents per key wait"));
    }
    // Only contents the caller may READ here count, and only their buckets are named.
    let (readable, buckets) = readable_contents(context, node_id, auth, contents).await?;
    let waits = locked_contents(context, readable.into_iter(), node_id).await?;
    let waits: Vec<_> = waits
        .into_iter()
        .filter(|wait| buckets.contains(&wait.bucket))
        .collect();
    for wait in &waits {
        register_remote_wait(&context.storage_handle, wait.key, peer, job_id, auth).await?;
    }
    let mut locked = Vec::new();
    for wait in waits {
        if !key_unlocked(context, wait.key).await {
            locked.push(wait);
        }
    }
    Ok(locked)
}

/// Aliases on this node checked per content; a content beyond them stays unknown.
const ALIAS_PAGE: usize = 64;

/// Contents with a version on this node that `auth` may READ, within its path restrictions,
/// and the buckets of those versions. Other contents are treated as unknown.
async fn readable_contents(
    context: &DriverContext,
    node_id: NodeId,
    auth: &AuthContext,
    contents: &[[u8; 32]],
) -> Result<(Vec<[u8; 32]>, BTreeSet<String>), String> {
    let mut readable = Vec::new();
    let mut buckets = BTreeSet::new();
    for content in contents {
        let prefix = HashIndex::hash_prefix(content).map_err(|error| error.to_string())?;
        let (rows, _) = iter_prefix_page(
            &context.storage_handle,
            PATHS_INDEX_KEYSPACE,
            Some(prefix.into()),
            None,
            ALIAS_PAGE,
            None,
        )
        .await?;
        let mut allowed = false;
        for (row, _) in &rows {
            let Ok(alias) = HashIndex::from_bytes(row) else {
                continue;
            };
            if alias.node_id != node_id || alias.realm_id != auth.realm_id {
                continue;
            }
            let path = object_permission_path(
                auth.realm_id,
                alias.group_id,
                node_id,
                &alias.bucket,
                &alias.key,
            );
            let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: auth.clone(),
                path,
                required_permission: Permission::READ,
            });
            if drive(check, context).await.unwrap_or(false) {
                allowed = true;
                buckets.insert(alias.bucket);
            }
        }
        if allowed {
            readable.push(*content);
        }
    }
    Ok((readable, buckets))
}

/// Waiting node: applies a wake from `peer`. False while the job may still be parking, so the
/// key node delivers again; any other state acknowledges, as the wake cannot matter any more.
pub(crate) async fn accept_wake(
    context: &DriverContext,
    peer: NodeId,
    auth: &AuthContext,
    job_id: JobId,
    key: BucketKeyRef,
) -> Result<bool, String> {
    let storage = &context.storage_handle;
    let Some(record) = read_job_record(storage, job_id, None).await? else {
        return Ok(true);
    };
    if record.created_by != auth.user_id {
        return Ok(true);
    }
    match record.state {
        JobState::Claimed | JobState::Preparing => Ok(false),
        JobState::AwaitingKey => {
            let waits = read_key_waits(storage, job_id).await?;
            if waits
                .iter()
                .any(|wait| wait.node_id == peer && wait.key == key)
            {
                satisfy_key_wait(storage, job_id, key, unix_timestamp_millis())
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok(true)
        }
        _ => Ok(true),
    }
}

/// Sends one job-control request straight to `node`.
async fn ask(
    context: &DriverContext,
    node: NodeId,
    job_id: JobId,
    request: JobRequest,
) -> Option<JobResponse> {
    let net = context.net_handle.as_ref()?;
    let operation = JobRouteOperation::new(*net.realm_id(), net.node_id(), job_id, Some(request))
        .with_responder(Some(node));
    match drive(operation, context).await {
        Ok(JobRouteOutcome::Remote(response)) => Some(response),
        Ok(JobRouteOutcome::Local) => None,
        Err(error) => {
            warn!(job_id = %job_id, node = %node, %error, "Key wait request failed");
            None
        }
    }
}

/// Waiting node: registers the job on each source node of its remote inputs and collects the
/// keys still locked there. An unreachable source is left to staging, as before.
pub(crate) async fn remote_waits(
    context: &DriverContext,
    record: &JobRecord,
    node_id: NodeId,
) -> Vec<KeyWait> {
    let mut by_node: BTreeMap<NodeId, Vec<[u8; 32]>> = BTreeMap::new();
    for input in &record.captured_inputs {
        if input.source_node_id != node_id {
            by_node
                .entry(input.source_node_id)
                .or_default()
                .push(input.blake3);
        }
    }
    let auth = owner_auth(record);
    let mut waits = Vec::new();
    for (node, contents) in by_node {
        for contents in contents.chunks(MAX_AWAITED) {
            let request = JobRequest::AwaitKeys {
                auth_token: AuthToken::Internal(auth.clone()),
                job_id: record.job_id,
                contents: contents.to_vec(),
            };
            match ask(context, node, record.job_id, request).await {
                Some(JobResponse::KeysLocked(locked)) => {
                    // The answering node names its own keys only.
                    waits.extend(locked.into_iter().filter(|wait| wait.node_id == node));
                }
                Some(response) => {
                    warn!(job_id = %record.job_id, node = %node, ?response, "Key wait refused")
                }
                None => {}
            }
        }
    }
    waits
}

/// The job owner this node vouches for in key wait requests.
fn owner_auth(record: &JobRecord) -> AuthContext {
    AuthContext {
        user_id: record.created_by,
        realm_id: record.created_by.realm_id,
        path_restrictions: None,
        session: None,
    }
}

/// Key node: turns the registrations on `key` into owed wakes, in bounded pages.
pub(crate) async fn queue_wakes(context: &DriverContext, key: BucketKeyRef) -> Result<(), String> {
    while queue_remote_wakes(&context.storage_handle, key)
        .await
        .map_err(|error| error.to_string())?
    {}
    Ok(())
}

/// Key node: sends one page of owed wakes and drops each one its waiting node acknowledged.
/// Unacknowledged wakes stay for the next delivery. Answers the acknowledged count.
pub async fn deliver_owed_wakes(context: &DriverContext) -> usize {
    let owed = match owed_wakes(&context.storage_handle).await {
        Ok(owed) => owed,
        Err(error) => {
            warn!(%error, "Owed key wakes could not be read");
            return 0;
        }
    };
    let mut acked = 0;
    for wake in owed {
        let request = JobRequest::KeyWake {
            auth_token: AuthToken::Internal(wake.auth.clone()),
            job_id: wake.job_id,
            key: wake.key,
        };
        if !matches!(
            ask(context, wake.waiter, wake.job_id, request).await,
            Some(JobResponse::KeyWakeAcked)
        ) {
            continue;
        }
        let storage = &context.storage_handle;
        match ack_wake(storage, wake.waiter, wake.job_id, wake.key).await {
            Ok(()) => acked += 1,
            Err(error) => warn!(job_id = %wake.job_id, %error, "Acknowledged wake not dropped"),
        }
    }
    acked
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::store::{ClaimOutcome, claim_job, insert_job, park_job};
    use aruna_core::UserId;
    use aruna_core::structs::execution::job::JobPayload;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    #[tokio::test]
    async fn wake_needs_parked_job() {
        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let storage = &context.storage_handle;
        let node = |seed| iroh::SecretKey::from_bytes(&[seed; 32]).public();
        let (local, source) = (node(1), node(2));
        let job_id = JobId::from_bytes([5; 16]);
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32]));
        let record = JobRecord::new(job_id, payload, owner, local, 1_000, 1_000, None);
        insert_job(storage, &record).await.unwrap();
        let ClaimOutcome::Claimed(claimed) =
            claim_job(storage, job_id, local, 2_000).await.unwrap()
        else {
            panic!("job must be claimed")
        };
        let auth = owner_auth(&claimed);
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);

        // A wake that overtakes the parking is delivered again later.
        assert_eq!(
            accept_wake(&context, source, &auth, job_id, key).await,
            Ok(false)
        );

        let wait = KeyWait {
            node_id: source,
            bucket: "b".to_string(),
            group_id: None,
            key,
        };
        let token = claimed.claim.unwrap().claim_token;
        park_job(storage, job_id, token, 3_000, vec![wait])
            .await
            .unwrap();
        // Only the node holding the key wakes the job; another node's wake is acknowledged only.
        assert_eq!(
            accept_wake(&context, local, &auth, job_id, key).await,
            Ok(true)
        );
        let parked = read_job_record(storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(parked.state, JobState::AwaitingKey);

        assert_eq!(
            accept_wake(&context, source, &auth, job_id, key).await,
            Ok(true)
        );
        let woken = read_job_record(storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(woken.state, JobState::Queued);
    }

    /// A node holding content `[9; 32]` as `sealed/data.bin` of group `[4; 16]`, sealed with a
    /// locked key, and an owner who may read that group.
    async fn guarded_node() -> (tempfile::TempDir, DriverContext, NodeId, UserId) {
        use aruna_core::effects::StorageEffect;
        use aruna_core::keyspaces::{
            AUTH_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, GROUP_KEYSPACE,
            REALM_CONFIG_KEYSPACE,
        };
        use aruna_core::structs::identity::auth::Actor;
        use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
        use aruna_core::structs::identity::realm::{
            RealmAuthorizationDocument, RealmConfigDocument,
        };
        use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, BlobLocationKey};
        use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
        use aruna_core::structs::storage::format::{EncodingClass, PithosLayout, StoredFormat};

        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = RealmId([1; 32]);
        let owner = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let group_id = Ulid::from_bytes([4; 16]);
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let actor = Actor {
            node_id: node,
            user_id: owner,
            realm_id,
        };
        let group_doc = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
        let group = Group {
            display_name: "group".to_string(),
            group_id,
            realm_id,
            roles: group_doc.roles.keys().copied().collect(),
            owner,
        };
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(key.bucket_id),
            key_generation: 1,
            ..Default::default()
        };
        let layout = PithosLayout {
            stored_size: 130,
            metadata_digest: [4; 32],
        };
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::from_bytes([3; 16]),
            format: StoredFormat::pithos(layout, key),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            staging: false,
            partial: false,
            blob_size: 100,
            hashes: Default::default(),
        };
        let row = BlobLocationKey::new([9; 32], EncodingClass::Raw, BackendRef::node_default());
        let alias = HashIndex::new(
            [9; 32],
            Ulid::from_bytes([6; 16]),
            realm_id,
            group_id,
            node,
            "sealed".to_string(),
            "data.bin".to_string(),
        );
        let realm_doc = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        let rows = [
            (
                AUTH_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                realm_doc.to_bytes(&actor).unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group_doc.to_bytes(&actor).unwrap(),
            ),
            (
                REALM_CONFIG_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                config.to_bytes(&actor).unwrap(),
            ),
            (
                GROUP_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
            (
                BUCKET_ENCRYPTION_KEYSPACE,
                b"sealed".to_vec(),
                settings.to_bytes().unwrap(),
            ),
            (
                BLOB_LOCATIONS_KEYSPACE,
                row.to_bytes(),
                location.to_bytes().unwrap(),
            ),
            (PATHS_INDEX_KEYSPACE, alias.to_bytes().unwrap(), Vec::new()),
        ];
        for (key_space, key, value) in rows {
            let effect = StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            };
            context.storage_handle.send_storage_effect(effect).await;
        }
        (dir, context, node, owner)
    }

    async fn registrations(context: &DriverContext) -> usize {
        use aruna_core::keyspaces::JOB_KEY_WAIT_KEYSPACE;
        let prefix = Some(b"r".to_vec().into());
        let rows = iter_prefix_page(
            &context.storage_handle,
            JOB_KEY_WAIT_KEYSPACE,
            prefix,
            None,
            8,
            None,
        );
        rows.await.unwrap().0.len()
    }

    #[tokio::test]
    async fn waits_need_read() {
        use aruna_core::structs::identity::auth::PathRestriction;

        let (_dir, context, node, owner) = guarded_node().await;
        let peer = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let job_id = JobId::from_bytes([5; 16]);
        let auth = |user_id: UserId, path_restrictions| AuthContext {
            user_id,
            realm_id: RealmId([1; 32]),
            path_restrictions,
            session: None,
        };
        let content = [[9; 32]];

        // A caller without READ learns nothing and subscribes to nothing.
        let stranger = UserId::local(Ulid::from_bytes([3; 16]), RealmId([1; 32]));
        let denied = auth(stranger, None);
        let waits = register_waits(&context, (node, peer), &denied, job_id, &content);
        assert_eq!(waits.await, Ok(Vec::new()));
        // A token restricted to another path is refused even for the owner.
        let elsewhere = vec![PathRestriction {
            pattern: "elsewhere/**".to_string(),
            permission: Permission::READ,
        }];
        let restricted = auth(owner, Some(elsewhere));
        let waits = register_waits(&context, (node, peer), &restricted, job_id, &content);
        assert_eq!(waits.await, Ok(Vec::new()));
        assert_eq!(registrations(&context).await, 0);

        let allowed = auth(owner, None);
        let waits = register_waits(&context, (node, peer), &allowed, job_id, &content);
        let waits = waits.await.unwrap();
        assert_eq!(waits.len(), 1);
        assert_eq!(waits[0].bucket, "sealed");
        assert_eq!(registrations(&context).await, 1);
    }
}
