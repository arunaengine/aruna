use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::{
    AuthContext, BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo,
    CurrentVersionPointer, Permission, RealmId, VersionKey, W3idDataIdentifier,
    blob_object_permission_path,
};
use aruna_core::types::{GroupId, Key, TxnId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::request_policy::{
    PolicyEnforcementError, PolicyEvaluator, PolicyRequestExtras, policy_request_with,
};

const HEAD_SCAN_BATCH: usize = 1_000;
pub const OBJECT_SEARCH_MAX_LIMIT: usize = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ObjectKeyMatch {
    Substring,
    Prefix,
}

impl ObjectKeyMatch {
    fn matches(self, key: &str, query: &str) -> bool {
        match self {
            Self::Substring => key.contains(query),
            Self::Prefix => key.starts_with(query),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SearchObjectsInput {
    pub auth: AuthContext,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub query: String,
    pub key_match: ObjectKeyMatch,
    pub bucket: Option<String>,
    pub limit: usize,
    pub start_after: Option<Vec<u8>>,
    pub as_of: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectInventoryChecksum {
    pub algorithm: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectInventoryHit {
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub bucket: String,
    pub key: String,
    pub content_w3id: Option<String>,
    pub checksum: Option<ObjectInventoryChecksum>,
    pub size: Option<u64>,
    pub updated_at: Option<SystemTime>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectSearchNodeHit {
    pub hit: ObjectInventoryHit,
    pub cursor_key: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectSearchNodePage {
    pub hits: Vec<ObjectSearchNodeHit>,
    pub next_start_after: Option<Vec<u8>>,
    pub observed_at: SystemTime,
}

#[derive(Debug, Error)]
pub enum SearchObjectsError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PolicyEnforcementError),
    #[error("object inventory storage failed: {0}")]
    Storage(String),
    #[error("object inventory scan budget exhausted")]
    Unavailable,
}

#[derive(Debug)]
struct LiveHead {
    head: BlobHeadKey,
    cursor_key: Vec<u8>,
    version: BlobVersion,
}

#[derive(Debug)]
struct CandidateBatch {
    candidates: Vec<ObjectSearchNodeHit>,
    next_start_after: Option<Vec<u8>>,
}

/// Searches one node's current object heads. Every candidate is authorized on
/// its full object path; hidden matches are consumed internally and never
/// affect a returned total or continuation signal.
pub async fn search_local_objects(
    context: &DriverContext,
    input: SearchObjectsInput,
) -> Result<ObjectSearchNodePage, SearchObjectsError> {
    let limit = input.limit.clamp(1, OBJECT_SEARCH_MAX_LIMIT);
    let mut start_after = input.start_after.clone();
    let mut visible = Vec::with_capacity(limit + 1);
    let mut evaluators: HashMap<(RealmId, GroupId), PolicyEvaluator> = HashMap::new();

    loop {
        let batch =
            scan_candidate_batch(context, &input, start_after.clone(), HEAD_SCAN_BATCH).await?;

        for candidate in batch.candidates {
            let path = blob_object_permission_path(
                input.realm_id,
                candidate.hit.group_id,
                input.node_id,
                &candidate.hit.bucket,
                &candidate.hit.key,
            );
            let allowed = drive(
                CheckPermissionsOperation::new(CheckPermissionsConfig {
                    auth_context: input.auth.clone(),
                    path: path.clone(),
                    required_permission: Permission::READ,
                }),
                context,
            )
            .await
            .unwrap_or(false);
            if !allowed {
                continue;
            }
            let policy_scope = (input.realm_id, candidate.hit.group_id);
            if !evaluators.contains_key(&policy_scope) {
                evaluators.extend(PolicyEvaluator::load_bulk(context, vec![policy_scope]).await?);
            }
            let request = policy_request_with(
                &path,
                &Permission::READ,
                Some(&input.auth.user_id),
                PolicyRequestExtras::rest(),
            );
            match evaluators
                .get(&policy_scope)
                .ok_or(SearchObjectsError::Unavailable)?
                .evaluate(&request)
            {
                Ok(()) => visible.push(candidate),
                Err(PolicyEnforcementError::Denied { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
            if visible.len() > limit {
                let next_start_after = visible
                    .get(limit - 1)
                    .map(|candidate| candidate.cursor_key.clone());
                visible.truncate(limit);
                return Ok(ObjectSearchNodePage {
                    hits: visible,
                    next_start_after,
                    observed_at: SystemTime::now(),
                });
            }
        }

        match batch.next_start_after {
            Some(next) => start_after = Some(next),
            None => {
                return Ok(ObjectSearchNodePage {
                    hits: visible,
                    next_start_after: None,
                    observed_at: SystemTime::now(),
                });
            }
        }
    }
}

async fn scan_candidate_batch(
    context: &DriverContext,
    input: &SearchObjectsInput,
    start_after: Option<Vec<u8>>,
    scan_limit: usize,
) -> Result<CandidateBatch, SearchObjectsError> {
    let txn_id = match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: true })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        event => return Err(storage_event_error(event)),
    };

    let result =
        scan_candidate_batch_in_transaction(context, input, start_after, scan_limit, txn_id).await;
    let completion = if result.is_ok() {
        StorageEffect::CommitTransaction { txn_id }
    } else {
        StorageEffect::AbortTransaction { txn_id }
    };
    let completion = context.storage_handle.send_storage_effect(completion).await;

    match result {
        Err(error) => Err(error),
        Ok(batch) => match completion {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(batch),
            event => Err(storage_event_error(event)),
        },
    }
}

async fn scan_candidate_batch_in_transaction(
    context: &DriverContext,
    input: &SearchObjectsInput,
    start_after: Option<Vec<u8>>,
    scan_limit: usize,
    txn_id: TxnId,
) -> Result<CandidateBatch, SearchObjectsError> {
    let prefix = input
        .bucket
        .as_deref()
        .map(BlobHeadKey::bucket_prefix)
        .transpose()?;
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: aruna_core::keyspaces::BLOB_HEAD_KEYSPACE.to_string(),
            prefix: prefix.map(Into::into),
            start: start_after.map(|key| IterStart::After(key.into())),
            limit: scan_limit,
            txn_id: Some(txn_id),
        })
        .await;
    let Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after,
    }) = event
    else {
        return Err(storage_event_error(event));
    };
    let mut heads = Vec::new();
    for (key, value) in values {
        let head = BlobHeadKey::from_bytes(key.as_ref())?;
        if !input.key_match.matches(&head.key, &input.query) {
            continue;
        }
        let pointer = CurrentVersionPointer::from_bytes(value.as_ref())?;
        heads.push((head, pointer.version_id, key.to_vec()));
    }
    if heads.is_empty() {
        return Ok(CandidateBatch {
            candidates: Vec::new(),
            next_start_after: next_start_after.map(|key| key.to_vec()),
        });
    }

    let version_reads = heads
        .iter()
        .map(|(head, version_id, _)| {
            VersionKey::new(&head.bucket, &head.key, *version_id)
                .to_bytes()
                .map(|key| (BLOB_VERSIONS_KEYSPACE.to_string(), Key::from(key)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let versions = batch_read(context, version_reads, txn_id).await?;
    if versions.len() != heads.len() {
        return Err(SearchObjectsError::Storage(
            "object inventory version batch length mismatch".to_string(),
        ));
    }

    let mut live = Vec::new();
    for ((head, _version_id, cursor_key), (_, value)) in heads.into_iter().zip(versions) {
        let Some(value) = value else {
            continue;
        };
        let version = BlobVersion::from_bytes(value.as_ref())?;
        if version.is_deleted() || version.created_at > input.as_of {
            continue;
        }
        live.push(LiveHead {
            head,
            cursor_key,
            version,
        });
    }
    if live.is_empty() {
        return Ok(CandidateBatch {
            candidates: Vec::new(),
            next_start_after: next_start_after.map(|key| key.to_vec()),
        });
    }

    let bucket_reads = live
        .iter()
        .map(|candidate| {
            (
                S3_BUCKET_KEYSPACE.to_string(),
                Key::from(candidate.head.bucket.as_bytes().to_vec()),
            )
        })
        .collect();
    let buckets = batch_read(context, bucket_reads, txn_id).await?;
    if buckets.len() != live.len() {
        return Err(SearchObjectsError::Storage(
            "object inventory bucket batch length mismatch".to_string(),
        ));
    }

    let location_reads = live
        .iter()
        .enumerate()
        .filter_map(|(index, candidate)| {
            candidate.version.location_key().map(|key| {
                (
                    index,
                    (
                        BLOB_LOCATIONS_KEYSPACE.to_string(),
                        Key::from(key.to_bytes()),
                    ),
                )
            })
        })
        .collect::<Vec<_>>();
    let locations = batch_read(
        context,
        location_reads
            .iter()
            .map(|(_, read)| read.clone())
            .collect(),
        txn_id,
    )
    .await?;
    let mut location_by_index = HashMap::new();
    for ((index, _), (_, value)) in location_reads.into_iter().zip(locations) {
        if let Some(value) = value {
            location_by_index.insert(index, BackendLocation::from_bytes(value.as_ref())?);
        }
    }

    let mut candidates = Vec::new();
    for (index, (candidate, (_, bucket))) in live.into_iter().zip(buckets).enumerate() {
        let Some(bucket) = bucket else {
            continue;
        };
        let bucket = BucketInfo::from_bytes(bucket.as_ref())?;
        let (content_w3id, checksum, size, updated_at) = match &candidate.version.state {
            BlobVersionState::Materialized { blob_hash, .. } => {
                let digest = hex::encode(blob_hash);
                (
                    Some(W3idDataIdentifier::ContentHash(*blob_hash).to_w3id()),
                    Some(ObjectInventoryChecksum {
                        algorithm: "blake3".to_string(),
                        value: digest,
                    }),
                    location_by_index
                        .get(&index)
                        .map(|location| location.blob_size),
                    Some(candidate.version.created_at),
                )
            }
            BlobVersionState::Reference {
                cached_metadata, ..
            } => (
                None,
                None,
                Some(cached_metadata.content_length),
                cached_metadata
                    .last_modified
                    .or(Some(candidate.version.created_at)),
            ),
            BlobVersionState::Deleted => continue,
        };
        candidates.push(ObjectSearchNodeHit {
            cursor_key: candidate.cursor_key,
            hit: ObjectInventoryHit {
                node_id: input.node_id,
                group_id: bucket.group_id,
                bucket: candidate.head.bucket,
                key: candidate.head.key,
                content_w3id,
                checksum,
                size,
                updated_at,
            },
        });
    }

    Ok(CandidateBatch {
        candidates,
        next_start_after: next_start_after.map(|key| key.to_vec()),
    })
}

async fn batch_read(
    context: &DriverContext,
    reads: Vec<(String, Key)>,
    txn_id: TxnId,
) -> Result<Vec<(Key, Option<aruna_core::types::Value>)>, SearchObjectsError> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => Ok(values),
        event => Err(storage_event_error(event)),
    }
}

fn storage_event_error(event: Event) -> SearchObjectsError {
    match event {
        Event::Storage(StorageEvent::Error { error }) => {
            SearchObjectsError::Storage(error.to_string())
        }
        event => SearchObjectsError::Storage(format!("unexpected storage event: {event:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};
    use std::time::{Duration, UNIX_EPOCH};

    use aruna_core::UserId;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::{
        Actor, BackendRef, BlobLocationKey, Group, GroupAuthorizationDocument, PathRestriction,
        RealmAuthorizationDocument, RealmConfigDocument, Role,
    };
    use aruna_storage::storage;
    use byteview::ByteView;
    use tempfile::TempDir;
    use ulid::Ulid;

    struct Fixture {
        _directory: TempDir,
        context: DriverContext,
        realm_id: RealmId,
        node_id: NodeId,
        owner: UserId,
        caller: UserId,
        visible_group: GroupId,
        hidden_group: GroupId,
    }

    async fn write_value(context: &DriverContext, keyspace: &str, key: Vec<u8>, value: Vec<u8>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: keyspace.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(value),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn setup() -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let owner = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let caller = UserId::local(Ulid::from_bytes([6u8; 16]), realm_id);
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        write_value(
            &context,
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
        write_value(
            &context,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmAuthorizationDocument::new_default_realm_doc(realm_id)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;

        let visible_group = Ulid::from_bytes([7u8; 16]);
        let hidden_group = Ulid::from_bytes([8u8; 16]);
        for (group_id, bucket, readable) in [
            (visible_group, "visible", true),
            (hidden_group, "hidden", false),
        ] {
            let mut auth =
                GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
            if readable {
                let role_id = Ulid::generate();
                auth.roles.insert(
                    role_id,
                    Role {
                        role_id,
                        name: "inventory-reader".to_string(),
                        permissions: HashMap::from([(
                            format!("/{realm_id}/g/{group_id}/data/**"),
                            Permission::READ,
                        )]),
                        assigned_users: HashSet::from([caller]),
                    },
                );
            }
            write_value(
                &context,
                AUTH_KEYSPACE,
                group_id.to_bytes().to_vec(),
                auth.to_bytes(&actor).unwrap(),
            )
            .await;
            write_value(
                &context,
                GROUP_KEYSPACE,
                group_id.to_bytes().to_vec(),
                Group {
                    display_name: bucket.to_string(),
                    group_id,
                    realm_id,
                    roles: auth.roles.keys().copied().collect(),
                    owner,
                }
                .to_bytes(&actor)
                .unwrap(),
            )
            .await;
            write_value(
                &context,
                S3_BUCKET_KEYSPACE,
                bucket.as_bytes().to_vec(),
                BucketInfo {
                    group_id,
                    created_at: UNIX_EPOCH,
                    created_by: owner,
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                }
                .to_bytes()
                .unwrap(),
            )
            .await;
        }

        Fixture {
            _directory: directory,
            context,
            realm_id,
            node_id,
            owner,
            caller,
            visible_group,
            hidden_group,
        }
    }

    async fn seed_materialized(
        fixture: &Fixture,
        bucket: &str,
        key: &str,
        created_at: SystemTime,
        hash_byte: u8,
    ) {
        let version_id = Ulid::generate();
        let blob_hash = [hash_byte; 32];
        write_value(
            &fixture.context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new(bucket, key).to_bytes().unwrap(),
            CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
        )
        .await;
        write_value(
            &fixture.context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new(bucket, key, version_id).to_bytes().unwrap(),
            BlobVersion::materialized(
                blob_hash,
                BackendRef::node_default(),
                created_at,
                fixture.owner,
                None,
            )
            .to_bytes()
            .unwrap(),
        )
        .await;
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/objects".to_string(),
            storage_bucket: bucket.to_string(),
            backend_path: key.to_string(),
            ulid: Ulid::generate(),
            compressed: false,
            encrypted: false,
            created_by: fixture.owner,
            created_at,
            staging: false,
            partial: false,
            blob_size: 42,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), blob_hash.to_vec())]),
        };
        write_value(
            &fixture.context,
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::new(blob_hash, BackendRef::node_default()).to_bytes(),
            location.to_bytes().unwrap(),
        )
        .await;
    }

    async fn seed_deleted(fixture: &Fixture, bucket: &str, key: &str) {
        let version_id = Ulid::generate();
        write_value(
            &fixture.context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new(bucket, key).to_bytes().unwrap(),
            CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
        )
        .await;
        write_value(
            &fixture.context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new(bucket, key, version_id).to_bytes().unwrap(),
            BlobVersion::deleted(UNIX_EPOCH + Duration::from_secs(10), fixture.owner)
                .to_bytes()
                .unwrap(),
        )
        .await;
    }

    fn input(
        fixture: &Fixture,
        auth: AuthContext,
        query: &str,
        limit: usize,
    ) -> SearchObjectsInput {
        SearchObjectsInput {
            auth,
            realm_id: fixture.realm_id,
            node_id: fixture.node_id,
            query: query.to_string(),
            key_match: ObjectKeyMatch::Substring,
            bucket: None,
            limit,
            start_after: None,
            as_of: SystemTime::now(),
        }
    }

    #[tokio::test]
    async fn authorization_filters_without_hidden_pagination_signals() {
        let fixture = setup().await;
        seed_materialized(&fixture, "hidden", "needle-hidden", UNIX_EPOCH, 1).await;
        seed_materialized(&fixture, "visible", "needle-visible", UNIX_EPOCH, 2).await;

        let page = search_local_objects(
            &fixture.context,
            input(
                &fixture,
                AuthContext {
                    user_id: fixture.caller,
                    realm_id: fixture.realm_id,
                    path_restrictions: None,
                },
                "needle",
                10,
            ),
        )
        .await
        .unwrap();

        assert_eq!(page.hits.len(), 1);
        assert_eq!(page.hits[0].hit.key, "needle-visible");
        assert_eq!(page.hits[0].hit.group_id, fixture.visible_group);
        assert_eq!(page.next_start_after, None);
        assert_eq!(page.hits[0].hit.size, Some(42));
        let expected_w3id = format!("https://w3id.org/aruna/data/{}", "02".repeat(32));
        assert_eq!(
            page.hits[0].hit.content_w3id.as_deref(),
            Some(expected_w3id.as_str())
        );
        assert_ne!(page.hits[0].hit.group_id, fixture.hidden_group);
    }

    #[tokio::test]
    async fn path_restrictions_are_enforced_per_object() {
        let fixture = setup().await;
        seed_materialized(&fixture, "visible", "report-allowed", UNIX_EPOCH, 3).await;
        seed_materialized(&fixture, "visible", "report-hidden", UNIX_EPOCH, 4).await;
        let allowed_path = blob_object_permission_path(
            fixture.realm_id,
            fixture.visible_group,
            fixture.node_id,
            "visible",
            "report-allowed",
        );

        let page = search_local_objects(
            &fixture.context,
            input(
                &fixture,
                AuthContext {
                    user_id: fixture.owner,
                    realm_id: fixture.realm_id,
                    path_restrictions: Some(vec![PathRestriction {
                        pattern: allowed_path,
                        permission: Permission::READ,
                    }]),
                },
                "report-",
                10,
            ),
        )
        .await
        .unwrap();

        assert_eq!(
            page.hits
                .iter()
                .map(|hit| hit.hit.key.as_str())
                .collect::<Vec<_>>(),
            vec!["report-allowed"]
        );
        assert_eq!(page.next_start_after, None);
    }

    #[tokio::test]
    async fn delete_marked_current_heads_are_excluded() {
        let fixture = setup().await;
        seed_materialized(&fixture, "visible", "state-live", UNIX_EPOCH, 5).await;
        seed_deleted(&fixture, "visible", "state-deleted").await;

        let page = search_local_objects(
            &fixture.context,
            input(
                &fixture,
                AuthContext {
                    user_id: fixture.owner,
                    realm_id: fixture.realm_id,
                    path_restrictions: None,
                },
                "state-",
                10,
            ),
        )
        .await
        .unwrap();

        assert_eq!(page.hits.len(), 1);
        assert_eq!(page.hits[0].hit.key, "state-live");
    }

    #[tokio::test]
    async fn keyset_and_as_of_cursor_stay_stable_across_mutations() {
        let fixture = setup().await;
        for (key, hash_byte) in [("page-alpha", 6), ("page-charlie", 7), ("page-echo", 8)] {
            seed_materialized(&fixture, "visible", key, UNIX_EPOCH, hash_byte).await;
        }
        let auth = AuthContext {
            user_id: fixture.owner,
            realm_id: fixture.realm_id,
            path_restrictions: None,
        };
        let as_of = SystemTime::now();
        let mut first_input = input(&fixture, auth.clone(), "page-", 2);
        first_input.as_of = as_of;
        let first = search_local_objects(&fixture.context, first_input)
            .await
            .unwrap();
        assert_eq!(
            first
                .hits
                .iter()
                .map(|hit| hit.hit.key.as_str())
                .collect::<Vec<_>>(),
            vec!["page-alpha", "page-charlie"]
        );
        let cursor = first.next_start_after.unwrap();

        let after_snapshot = as_of + Duration::from_secs(1);
        seed_materialized(&fixture, "visible", "page-bravo", after_snapshot, 9).await;
        seed_materialized(&fixture, "visible", "page-delta", after_snapshot, 10).await;
        let mut second_input = input(&fixture, auth, "page-", 2);
        second_input.as_of = as_of;
        second_input.start_after = Some(cursor);
        let second = search_local_objects(&fixture.context, second_input)
            .await
            .unwrap();

        assert_eq!(
            second
                .hits
                .iter()
                .map(|hit| hit.hit.key.as_str())
                .collect::<Vec<_>>(),
            vec!["page-echo"]
        );
        assert_eq!(second.next_start_after, None);
    }
}
