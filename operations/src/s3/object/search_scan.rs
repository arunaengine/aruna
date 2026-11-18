//! Searches object keys across a realm's buckets and filters hits by the caller's permissions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::object::search::{
    ObjectInventoryChecksum, ObjectInventoryHit, ObjectKeyMatch, SEARCH_MAX_LIMIT, SearchNodeHit,
    SearchNodePage, SearchObjectsError, SearchObjectsInput,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::table::{metadata_matches, query_terms};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer,
    VersionKey, object_permission_path,
};
use aruna_core::structs::storage::replication::W3idIdentifier;
use aruna_core::types::{Key, Value};

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::enforcement::{PolicyBulkOperation, PolicyEvaluators};
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

#[path = "index_search.rs"]
mod index_search;

pub use index_search::sealed_position;
pub(crate) use index_search::{open_position, seal_position};

const HEAD_SCAN_BATCH: usize = 1_000;
/// Changed keys one node page passes at most; the page then ends at the last of them.
const CHANGED_PAGE_CAP: usize = SEARCH_MAX_LIMIT;
/// Versions one read returns while learning whether a changed key existed at the snapshot.
const VERSION_BATCH: usize = 64;

type SearchTurn<T> = Result<Next<T>, SearchObjectsError>;

#[derive(Debug, PartialEq)]
struct LiveHead {
    head: BlobHeadKey,
    cursor_key: Vec<u8>,
    version: BlobVersion,
    /// The key existed at `as_of`, but its head was replaced or deleted since.
    changed: bool,
}

#[derive(Debug, PartialEq)]
struct Candidate {
    hit: SearchNodeHit,
    changed: bool,
}

#[derive(Debug, PartialEq)]
struct HeadCandidate {
    head: BlobHeadKey,
    version_id: ulid::Ulid,
    cursor_key: Vec<u8>,
}

#[derive(Debug, PartialEq)]
struct CandidateBatch {
    candidates: Vec<Candidate>,
    next_start: Option<IterStart>,
    distributed: HashMap<String, bool>,
}

/// One page of a scan; the distributed buckets it skipped count as unsearched, since only their
/// range indexes cover them.
fn node_page(
    hits: Vec<SearchNodeHit>,
    changed: Vec<Vec<u8>>,
    (next_start_after, now_ms): (Option<Vec<u8>>, u64),
    distributed: &HashMap<String, bool>,
) -> SearchNodePage {
    let mut skipped_buckets: Vec<String> = (distributed.iter())
        .filter(|(_, bound)| **bound)
        .map(|(name, _)| name.clone())
        .collect();
    skipped_buckets.sort_unstable();
    SearchNodePage {
        hits,
        next_start_after,
        observed_at: UNIX_EPOCH + Duration::from_millis(now_ms),
        incomplete_ranges: skipped_buckets.len(),
        remote_ranges: 0,
        index_progress: Vec::new(),
        partial_ranges: Vec::new(),
        skipped_buckets,
        changed: Some(changed),
        layout: None,
    }
}

/// Keeps the changed keys up to `next`, after which the following page starts again.
pub(crate) fn changed_until(changed: &mut Vec<Vec<u8>>, next: Option<&[u8]>) {
    if let Some(next) = next {
        changed.retain(|key| key.as_slice() <= next);
    }
}

/// Where a permission check of one hit stands.
enum Checked {
    Call(IoCall),
    Decided(bool),
}

#[derive(Debug, Default, PartialEq)]
enum CheckWait {
    #[default]
    Permission,
    Policy(String),
}

/// Whether the caller may read a hit: group READ on its full object path, token path
/// restrictions and the request policies of its group, each loaded once.
#[derive(Debug, Default, PartialEq)]
struct HitCheck {
    evaluators: PolicyEvaluators,
    path: String,
    wait: CheckWait,
}

impl HitCheck {
    fn begin(&mut self, input: &SearchObjectsInput, hit: &SearchNodeHit) -> IoCall {
        self.path = object_permission_path(
            input.realm_id,
            hit.hit.group_id,
            input.node_id,
            &hit.hit.bucket,
            &hit.hit.key,
        );
        self.wait = CheckWait::Permission;
        IoCall::child(CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: input.auth.clone(),
            path: self.path.clone(),
            required_permission: Permission::READ,
        }))
    }

    fn answer(
        &mut self,
        input: &SearchObjectsInput,
        hit: &SearchNodeHit,
        result: IoResult,
    ) -> Result<Checked, SearchObjectsError> {
        let scope = (input.realm_id, hit.hit.group_id);
        match std::mem::take(&mut self.wait) {
            CheckWait::Permission => {
                if !(result.child::<bool, AuthorizationError>()?).unwrap_or(false) {
                    return Ok(Checked::Decided(false));
                }
                if !self.evaluators.contains_key(&scope) {
                    self.wait = CheckWait::Policy(self.path.clone());
                    let bulk = PolicyBulkOperation::new(vec![scope]);
                    return Ok(Checked::Call(IoCall::child(bulk)));
                }
            }
            CheckWait::Policy(_) => {
                let loaded = result.child::<PolicyEvaluators, PolicyEnforcementError>()??;
                self.evaluators.extend(loaded);
            }
        }
        let request = policy_request_with(
            &self.path,
            &Permission::READ,
            Some(&input.auth),
            PolicyRequestExtras::rest(),
        );
        let evaluator = (self.evaluators.get(&scope)).ok_or(SearchObjectsError::Unavailable)?;
        match evaluator.evaluate(&request) {
            Ok(()) => Ok(Checked::Decided(true)),
            Err(PolicyEnforcementError::Denied { .. }) => Ok(Checked::Decided(false)),
            Err(error) => Err(error.into()),
        }
    }
}

/// How walking the candidates of a page goes on.
enum WalkTurn {
    Call(IoCall),
    /// Every candidate was looked at and the page has room.
    Drained,
    /// The page is full and continues after this position.
    Full(Option<Vec<u8>>),
}

/// Authorizes candidates one after another and collects the page they make.
#[derive(Debug, Default, PartialEq)]
struct HitWalk {
    check: HitCheck,
    pending: VecDeque<Candidate>,
    current: Option<Candidate>,
    visible: Vec<SearchNodeHit>,
    changed: Vec<Vec<u8>>,
}

impl HitWalk {
    fn add(&mut self, candidates: Vec<Candidate>) {
        self.pending.extend(candidates);
    }

    fn advance(&mut self, input: &SearchObjectsInput) -> WalkTurn {
        let Some(candidate) = self.pending.pop_front() else {
            return WalkTurn::Drained;
        };
        let call = self.check.begin(input, &candidate.hit);
        self.current = Some(candidate);
        WalkTurn::Call(call)
    }

    fn answer(
        &mut self,
        input: &SearchObjectsInput,
        result: IoResult,
    ) -> Result<WalkTurn, SearchObjectsError> {
        let candidate = self.current.take().ok_or(IoError::Unfinished)?;
        match self.check.answer(input, &candidate.hit, result)? {
            Checked::Call(call) => {
                self.current = Some(candidate);
                Ok(WalkTurn::Call(call))
            }
            Checked::Decided(false) => Ok(self.advance(input)),
            Checked::Decided(true) => Ok(self.accept(input, candidate)),
        }
    }

    fn accept(&mut self, input: &SearchObjectsInput, candidate: Candidate) -> WalkTurn {
        if candidate.changed {
            self.changed.push(candidate.hit.cursor_key);
            if self.changed.len() >= CHANGED_PAGE_CAP {
                return WalkTurn::Full(self.changed.last().cloned());
            }
            return self.advance(input);
        }
        self.visible.push(candidate.hit);
        let limit = input.limit.clamp(1, SEARCH_MAX_LIMIT);
        if self.visible.len() <= limit {
            return self.advance(input);
        }
        let next = (self.visible.get(limit - 1)).map(|hit| hit.cursor_key.clone());
        self.visible.truncate(limit);
        changed_until(&mut self.changed, next.as_deref());
        WalkTurn::Full(next)
    }
}

/// Keeps the heads whose loaded version matches a metadata query; key queries keep every head.
fn retain_matches(input: &SearchObjectsInput, live: &mut Vec<LiveHead>) {
    if input.key_match == ObjectKeyMatch::Metadata {
        let (text, exact) = query_terms(&input.query);
        // A changed head's metadata at `as_of` is unknown, so it is kept as a possible match.
        live.retain(|head| head.changed || metadata_matches(&head.version.metadata, &text, &exact));
    }
}

/// What building hits answers.
enum BuildTurn {
    Call(IoCall),
    Done(Vec<Candidate>),
}

#[derive(Debug, PartialEq)]
struct ExistedRead {
    candidate: HeadCandidate,
    version: BlobVersion,
    prefix: Vec<u8>,
    /// The newest version created by `as_of`, and whether it is live.
    visible: Option<(SystemTime, bool)>,
}

#[derive(Debug, PartialEq)]
enum BuildStep {
    Versions(Vec<HeadCandidate>),
    Existed(Box<ExistedRead>),
    Buckets,
    Locations(Vec<Option<Value>>, Vec<usize>),
}

/// Turns current heads into hits inside the read transaction of the operation that holds it:
/// their versions, whether a changed key existed at the snapshot, buckets and locations.
#[derive(Debug, PartialEq)]
struct HitBuilder {
    input: SearchObjectsInput,
    loaded: VecDeque<(HeadCandidate, BlobVersion)>,
    live: Vec<LiveHead>,
    step: BuildStep,
}

impl HitBuilder {
    fn new(input: SearchObjectsInput) -> Self {
        Self {
            input,
            loaded: VecDeque::new(),
            live: Vec::new(),
            step: BuildStep::Buckets,
        }
    }

    fn start(&mut self, heads: Vec<HeadCandidate>) -> Result<BuildTurn, SearchObjectsError> {
        if heads.is_empty() {
            return Ok(BuildTurn::Done(Vec::new()));
        }
        let reads = (heads.iter())
            .map(|candidate| {
                let head = &candidate.head;
                VersionKey::new(&head.bucket, &head.key, candidate.version_id)
                    .to_bytes()
                    .map(|key| (BLOB_VERSIONS_KEYSPACE.to_string(), Key::from(key)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.step = BuildStep::Versions(heads);
        Ok(BuildTurn::Call(IoCall::Read(reads)))
    }

    /// Takes the next loaded version; a key whose first version is newer than the snapshot did
    /// not exist at it.
    fn settle(&mut self) -> Result<BuildTurn, SearchObjectsError> {
        while let Some((candidate, version)) = self.loaded.pop_front() {
            if version.created_at > self.input.as_of {
                let head = &candidate.head;
                let prefix = VersionKey::object_prefix(&head.bucket, &head.key)?;
                let call = versions_page(prefix.clone(), None);
                self.step = BuildStep::Existed(Box::new(ExistedRead {
                    candidate,
                    version,
                    prefix,
                    visible: None,
                }));
                return Ok(BuildTurn::Call(call));
            }
            if !version.is_deleted() {
                self.keep(candidate, version, false);
            }
        }
        self.read_buckets()
    }

    fn keep(&mut self, candidate: HeadCandidate, version: BlobVersion, changed: bool) {
        self.live.push(LiveHead {
            head: candidate.head,
            cursor_key: candidate.cursor_key,
            version,
            changed,
        });
    }

    fn read_buckets(&mut self) -> Result<BuildTurn, SearchObjectsError> {
        retain_matches(&self.input, &mut self.live);
        if self.live.is_empty() {
            return Ok(BuildTurn::Done(Vec::new()));
        }
        let reads = (self.live.iter())
            .map(|candidate| {
                let bucket = candidate.head.bucket.as_bytes().to_vec();
                (S3_BUCKET_KEYSPACE.to_string(), Key::from(bucket))
            })
            .collect();
        self.step = BuildStep::Buckets;
        Ok(BuildTurn::Call(IoCall::Read(reads)))
    }

    fn read_locations(
        &mut self,
        buckets: Vec<Option<Value>>,
    ) -> Result<BuildTurn, SearchObjectsError> {
        if buckets.len() != self.live.len() {
            return Err(SearchObjectsError::Storage(
                "object inventory bucket batch length mismatch".to_string(),
            ));
        }
        let (mut slots, mut reads) = (Vec::new(), Vec::new());
        for (index, candidate) in self.live.iter().enumerate() {
            if let Some(key) = candidate.version.location_key() {
                slots.push(index);
                reads.push((
                    BLOB_LOCATIONS_KEYSPACE.to_string(),
                    Key::from(key.to_bytes()),
                ));
            }
        }
        if reads.is_empty() {
            return self.build(buckets, HashMap::new());
        }
        self.step = BuildStep::Locations(buckets, slots);
        Ok(BuildTurn::Call(IoCall::Read(reads)))
    }

    fn build(
        &mut self,
        buckets: Vec<Option<Value>>,
        location_by_index: HashMap<usize, BackendLocation>,
    ) -> Result<BuildTurn, SearchObjectsError> {
        let input = &self.input;
        let live = std::mem::take(&mut self.live);
        let mut candidates = Vec::new();
        for (index, (candidate, bucket)) in live.into_iter().zip(buckets).enumerate() {
            let Some(bucket) = bucket else {
                continue;
            };
            let bucket = BucketInfo::from_bytes(bucket.as_ref())?;
            let (content_w3id, checksum, size, updated_at) = match &candidate.version.state {
                // Only its key is used: it is authorized and counted, never shown.
                _ if candidate.changed => (None, None, None, None),
                BlobVersionState::Materialized { blob_hash, .. } => {
                    let digest = hex::encode(blob_hash);
                    (
                        Some(W3idIdentifier::ContentHash(*blob_hash).to_w3id()),
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
                // No content identity is claimed before the hash is verified.
                BlobVersionState::PendingContent { .. } => {
                    (None, None, None, Some(candidate.version.created_at))
                }
                BlobVersionState::Deleted => continue,
            };
            let hit = SearchNodeHit {
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
                    metadata: match input.key_match {
                        ObjectKeyMatch::Metadata => kept_metadata(&candidate.version.metadata),
                        ObjectKeyMatch::Substring | ObjectKeyMatch::Prefix => BTreeMap::new(),
                    },
                },
            };
            let changed = candidate.changed;
            candidates.push(Candidate { hit, changed });
        }
        Ok(BuildTurn::Done(candidates))
    }

    fn answer(&mut self, result: IoResult) -> Result<BuildTurn, SearchObjectsError> {
        match std::mem::replace(&mut self.step, BuildStep::Buckets) {
            BuildStep::Versions(heads) => {
                let versions = result.values()?;
                if versions.len() != heads.len() {
                    return Err(SearchObjectsError::Storage(
                        "object inventory version batch length mismatch".to_string(),
                    ));
                }
                for (candidate, value) in heads.into_iter().zip(versions) {
                    if let Some(value) = value {
                        let version = BlobVersion::from_bytes(value.as_ref())?;
                        self.loaded.push_back((candidate, version));
                    }
                }
                self.settle()
            }
            // Ids of one millisecond may swap, so every kept version is read.
            BuildStep::Existed(mut read) => {
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    let version = BlobVersion::from_bytes(value.as_ref())?;
                    let newer = |(at, _): &(SystemTime, bool)| version.created_at >= *at;
                    if version.created_at <= self.input.as_of
                        && read.visible.as_ref().is_none_or(newer)
                    {
                        read.visible = Some((version.created_at, !version.is_deleted()));
                    }
                }
                if let Some(next) = next {
                    let call = versions_page(read.prefix.clone(), Some(next));
                    self.step = BuildStep::Existed(read);
                    return Ok(BuildTurn::Call(call));
                }
                let ExistedRead {
                    candidate,
                    version,
                    visible,
                    ..
                } = *read;
                if visible.is_some_and(|(_, live)| live) {
                    self.keep(candidate, version, true);
                }
                self.settle()
            }
            BuildStep::Buckets => self.read_locations(result.values()?),
            BuildStep::Locations(buckets, slots) => {
                let mut locations = HashMap::new();
                for (index, value) in slots.into_iter().zip(result.values()?) {
                    if let Some(value) = value {
                        locations.insert(index, BackendLocation::from_bytes(value.as_ref())?);
                    }
                }
                self.build(buckets, locations)
            }
        }
    }
}

fn versions_page(prefix: Vec<u8>, after: Option<Key>) -> IoCall {
    IoCall::Page {
        space: BLOB_VERSIONS_KEYSPACE.to_string(),
        prefix: Key::from(prefix),
        after,
        limit: VERSION_BATCH,
    }
}

#[derive(Debug, PartialEq)]
enum ScanStep {
    Heads,
    Buckets(Vec<String>, Vec<(Key, Value)>),
    Hits,
}

/// One batch of current heads in one read snapshot, as hits to authorize.
#[derive(Debug, PartialEq)]
struct ScanBatchOperation {
    input: SearchObjectsInput,
    start: Option<IterStart>,
    distributed: HashMap<String, bool>,
    next_start: Option<IterStart>,
    hits: HitBuilder,
    step: ScanStep,
}

type BatchTurn = SearchTurn<CandidateBatch>;

impl ScanBatchOperation {
    fn new(
        input: SearchObjectsInput,
        start: Option<IterStart>,
        distributed: HashMap<String, bool>,
    ) -> Self {
        Self {
            hits: HitBuilder::new(input.clone()),
            input,
            start,
            distributed,
            next_start: None,
            step: ScanStep::Heads,
        }
    }

    /// Which buckets of a realm-wide batch are distributed: their keys are searched only
    /// through their range indexes, so the scan skips them.
    fn learn_buckets(&mut self, values: Vec<(Key, Value)>) -> BatchTurn {
        let mut names = Vec::new();
        for (key, _) in &values {
            let head = BlobHeadKey::from_bytes(key.as_ref())?;
            if !self.distributed.contains_key(&head.bucket) && !names.contains(&head.bucket) {
                names.push(head.bucket);
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
            // No content identity is claimed before the hash is verified.
            BlobVersionState::PendingContent { .. } => {
                (None, None, None, Some(candidate.version.created_at))
            }
            BlobVersionState::Deleted => continue,
        };
        let hit = SearchNodeHit {
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
                metadata: match input.key_match {
                    ObjectKeyMatch::Metadata => kept_metadata(&candidate.version.metadata),
                    ObjectKeyMatch::Substring | ObjectKeyMatch::Prefix => BTreeMap::new(),
                },
            },
        };
        let changed = candidate.changed;
        candidates.push(Candidate { hit, changed });
    }

    Ok(candidates)
}

/// A version's own metadata, without the `aruna-` entries that only steer search.
fn kept_metadata(metadata: &HashMap<String, String>) -> BTreeMap<String, String> {
    (metadata.iter())
        .filter(|(key, _)| !key.starts_with("aruna-"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
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
        event => {
            SearchObjectsError::Storage(format!("unexpected storage event: {}", event.label()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;

    use std::collections::{HashMap, HashSet};
    use std::time::{Duration, UNIX_EPOCH};

    use aruna_core::UserId;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::identity::auth::{Actor, PathRestriction, Role};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};
    use aruna_core::structs::storage::blob::{BackendRef, BlobLocationKey};
    use aruna_storage::storage;
    use byteview::ByteView;
    use tempfile::TempDir;
    use ulid::Ulid;

    pub(super) struct Fixture {
        _directory: TempDir,
        pub(super) context: DriverContext,
        pub(super) realm_id: RealmId,
        pub(super) node_id: NodeId,
        pub(super) owner: UserId,
        pub(super) caller: UserId,
        pub(super) visible_group: GroupId,
        hidden_group: GroupId,
    }

    pub(super) async fn write_value(
        context: &DriverContext,
        keyspace: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) {
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

    pub(super) async fn setup() -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
            RealmAuthorizationDocument::default_realm_doc(realm_id)
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
            let mut auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
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
                    compression: Compression::Off,
                    distribution: None,
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

    pub(super) async fn seed_materialized(
        fixture: &Fixture,
        bucket: &str,
        key: &str,
        created_at: SystemTime,
        hash_byte: u8,
    ) -> Ulid {
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
                EncodingClass::Raw,
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
            format: StoredFormat::default(),
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
            BlobLocationKey::new(blob_hash, EncodingClass::Raw, BackendRef::node_default())
                .to_bytes(),
            location.to_bytes().unwrap(),
        )
        .await;
        version_id
    }

    async fn seed_deleted(fixture: &Fixture, bucket: &str, key: &str, created_at: SystemTime) {
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
            BlobVersion::deleted(created_at, fixture.owner)
                .to_bytes()
                .unwrap(),
        )
        .await;
    }

    pub(super) fn input(
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
    async fn authorization_hides_pages() {
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
                    session: None,
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
    async fn restrictions_enforced() {
        let fixture = setup().await;
        seed_materialized(&fixture, "visible", "report-allowed", UNIX_EPOCH, 3).await;
        seed_materialized(&fixture, "visible", "report-hidden", UNIX_EPOCH, 4).await;
        let allowed_path = object_permission_path(
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
                    session: None,
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
    async fn deleted_heads_excluded() {
        let fixture = setup().await;
        seed_materialized(&fixture, "visible", "state-live", UNIX_EPOCH, 5).await;
        let deleted_at = UNIX_EPOCH + Duration::from_secs(10);
        seed_deleted(&fixture, "visible", "state-deleted", deleted_at).await;

        let page = search_local_objects(
            &fixture.context,
            input(
                &fixture,
                AuthContext {
                    user_id: fixture.owner,
                    realm_id: fixture.realm_id,
                    path_restrictions: None,
                    session: None,
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
    async fn cursor_stays_stable() {
        let fixture = setup().await;
        for (key, hash_byte) in [("page-alpha", 6), ("page-charlie", 7), ("page-echo", 8)] {
            seed_materialized(&fixture, "visible", key, UNIX_EPOCH, hash_byte).await;
        }
        let auth = AuthContext {
            user_id: fixture.owner,
            realm_id: fixture.realm_id,
            path_restrictions: None,
            session: None,
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

    #[tokio::test]
    async fn counts_changed_keys() {
        // Keys replaced or deleted after the snapshot are counted on the page that passes them.
        let fixture = setup().await;
        for (key, hash_byte) in [
            ("page-alpha", 11),
            ("page-bravo", 12),
            ("page-charlie", 13),
            ("page-delta", 14),
            ("page-echo", 15),
        ] {
            seed_materialized(&fixture, "visible", key, UNIX_EPOCH, hash_byte).await;
        }
        let auth = AuthContext {
            user_id: fixture.owner,
            realm_id: fixture.realm_id,
            path_restrictions: None,
            session: None,
        };
        let as_of = SystemTime::now();
        let later = as_of + Duration::from_secs(1);
        seed_materialized(&fixture, "visible", "page-bravo", later, 16).await;
        seed_deleted(&fixture, "visible", "page-delta", later).await;
        seed_materialized(&fixture, "visible", "page-foxtrot", later, 17).await;
        let keys = |page: &SearchNodePage| {
            let hits = page.hits.iter().map(|hit| hit.hit.key.clone()).collect();
            let changed = page.changed.iter().flatten();
            let changed = changed.map(|key| String::from_utf8(key.clone()).unwrap());
            (hits, changed.collect())
        };

        let mut first_input = input(&fixture, auth.clone(), "page-", 2);
        first_input.as_of = as_of;
        let first = search_local_objects(&fixture.context, first_input)
            .await
            .unwrap();
        let (hits, changed): (Vec<String>, Vec<String>) = keys(&first);
        assert_eq!(hits, ["page-alpha", "page-charlie"]);
        assert_eq!(changed, ["visible/page-bravo"]);

        let mut second_input = input(&fixture, auth, "page-", 2);
        second_input.as_of = as_of;
        second_input.start_after = first.next_start_after;
        let second = search_local_objects(&fixture.context, second_input)
            .await
            .unwrap();
        let (hits, changed): (Vec<String>, Vec<String>) = keys(&second);
        assert_eq!(hits, ["page-echo"]);
        assert_eq!(changed, ["visible/page-delta"]);
        assert_eq!(second.next_start_after, None);
    }

    #[tokio::test]
    async fn recreated_keys_unchanged() {
        // A key deleted before the snapshot and created again after it did not exist then.
        let fixture = setup().await;
        let at = |secs| UNIX_EPOCH + Duration::from_secs(secs);
        seed_materialized(&fixture, "visible", "page-gone", at(0), 21).await;
        seed_deleted(&fixture, "visible", "page-gone", at(10)).await;
        seed_materialized(&fixture, "visible", "page-gone", at(30), 22).await;
        let auth = AuthContext {
            user_id: fixture.owner,
            realm_id: fixture.realm_id,
            path_restrictions: None,
            session: None,
        };
        let mut input = input(&fixture, auth, "page-", 10);
        input.as_of = at(20);

        let page = search_local_objects(&fixture.context, input).await.unwrap();
        assert!(page.hits.is_empty());
        assert_eq!(page.changed, Some(Vec::new()));
    }
}
