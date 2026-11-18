//! Stores Git packs and LFS content as Aruna objects and reads them from any document holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, UNIX_EPOCH};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitEffect, GitEvent, GitPack, LOCAL_OBJECTS, MAX_PACK_BYTES, StoredObject};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::dataset_location::default_bucket;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_core::types::GroupId;
use bytes::Bytes;
use tracing::debug;
use ulid::Ulid;

use super::calls::{git_call, git_event};
use super::records::{InsertRowOperation, LoadRowOperation, SyncedOperation};
use super::{GitError, records};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::groups::routing::{BucketRules, RoutingInputsError, SnapshotOperation};
use crate::placement::policy::{GateContext, GateContextError, GateOperation};
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};

pub type Blob = BackendStream<Result<Bytes, StreamError>>;

type Document = MetadataRegistryRecord;

fn index_key(document: &Document, sha256: &str) -> Vec<u8> {
    [
        document.document_id.to_bytes().as_slice(),
        sha256.as_bytes(),
    ]
    .concat()
}

/// What the pending call of finding the dataset bucket answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BucketStep {
    Found,
    Created,
    Reread,
}

/// This node's group-local dataset bucket, created through ordinary bucket operations.
#[derive(Debug, PartialEq)]
pub struct EnsureBucketOperation {
    name: String,
    group_id: GroupId,
    created_at_ms: u64,
    creator: aruna_core::UserId,
    step: BucketStep,
}

type BucketTurn = Result<Next<BucketInfo>, GitError>;

impl EnsureBucketOperation {
    pub(super) fn new(document: &Document, auth: &AuthContext) -> Self {
        Self {
            name: default_bucket(document.group_id),
            group_id: document.group_id,
            created_at_ms: document.created_at_ms,
            creator: auth.user_id,
            step: BucketStep::Found,
        }
    }

    fn read(&self) -> IoCall {
        IoCall::child(GetBucketOperation::new(self.name.clone()))
    }

    fn owned(&self, bucket: BucketInfo) -> BucketTurn {
        match bucket.group_id == self.group_id {
            true => Ok(Next::Done(bucket)),
            false => Err(GitError::Conflict),
        }
    }
}

impl Operation for EnsureBucketOperation {
    type Output = BucketInfo;
    type Error = GitError;

    fn first(&mut self) -> BucketTurn {
        Ok(Next::Call(self.read()))
    }

    fn next(&mut self, result: IoResult) -> BucketTurn {
        match self.step {
            BucketStep::Found => match result.child::<BucketInfo, GetBucketError>()? {
                Ok(bucket) => self.owned(bucket),
                Err(GetBucketError::NotFound) => {
                    let info = BucketInfo {
                        group_id: self.group_id,
                        created_at: UNIX_EPOCH + Duration::from_millis(self.created_at_ms),
                        created_by: self.creator,
                        cors_configuration: None,
                        storage_routing: Vec::new(),
                        placement_policies: Vec::new(),
                        placement_policy_generation: 0,
                        compression: Compression::Off,
                        distribution: None,
                    };
                    self.step = BucketStep::Created;
                    let create = CreateBucketOperation::new(self.name.clone(), info);
                    Ok(Next::Call(IoCall::child(create)))
                }
                Err(_) => Err(GitError::Unavailable),
            },
            BucketStep::Created => match result.child::<BucketInfo, CreateBucketError>()? {
                Ok(_) | Err(CreateBucketError::BucketAlreadyExists) => {
                    self.step = BucketStep::Reread;
                    Ok(Next::Call(self.read()))
                }
                Err(_) => Err(GitError::Unavailable),
            },
            BucketStep::Reread => {
                let bucket = result.child::<BucketInfo, GetBucketError>()?;
                self.owned(bucket.map_err(|_| GitError::Unavailable)?)
            }
        }
    }
}

/// This node's own copy of content with `sha256`, if it has stored one.
pub(super) fn copy(document: &Document, sha256: &str) -> LoadRowOperation<StoredObject> {
    records::load(LOCAL_OBJECTS, index_key(document, sha256))
}

/// What the pending call of storing content answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoreStep {
    Copy,
    Bucket,
    Put,
    Index,
}

/// Stores content under `key` in this node's dataset bucket after checking its SHA-256.
#[derive(Debug, PartialEq)]
pub struct StoreObjectOperation {
    node: Option<NodeId>,
    routing: NodeRouting,
    auth: AuthContext,
    document: Document,
    key: String,
    size: u64,
    sha256: String,
    body: Option<Blob>,
    step: StoreStep,
}

type StoreTurn = Result<Next<StoredObject>, GitError>;

impl StoreObjectOperation {
    pub(super) fn new(
        (node, routing): (Option<NodeId>, NodeRouting),
        auth: &AuthContext,
        document: &Document,
        (key, size, sha256): (String, u64, &str),
        body: Blob,
    ) -> Self {
        Self {
            node,
            routing,
            auth: auth.clone(),
            document: document.clone(),
            key,
            size,
            sha256: sha256.to_string(),
            body: Some(body),
            step: StoreStep::Copy,
        }
    }
}

impl Operation for StoreObjectOperation {
    type Output = StoredObject;
    type Error = GitError;

    fn first(&mut self) -> StoreTurn {
        let copy = copy(&self.document, &self.sha256);
        Ok(Next::Call(IoCall::child(copy)))
    }

    fn next(&mut self, result: IoResult) -> StoreTurn {
        match self.step {
            StoreStep::Copy => {
                if let Some(existing) = result.child::<Option<StoredObject>, GitError>()?? {
                    return Ok(Next::Done(existing));
                }
                self.step = StoreStep::Bucket;
                let bucket = EnsureBucketOperation::new(&self.document, &self.auth);
                Ok(Next::Call(IoCall::child(bucket)))
            }
            StoreStep::Bucket => {
                let bucket = result.child::<BucketInfo, GitError>()??;
                let body = self.body.take().ok_or(IoError::Unfinished)?;
                let put = PutBlobOperation::new(
                    (self.node, &self.routing),
                    &self.auth,
                    (default_bucket(self.document.group_id), bucket),
                    (self.key.clone(), self.size, self.sha256.clone()),
                    body,
                );
                self.step = StoreStep::Put;
                Ok(Next::Call(IoCall::child(put)))
            }
            StoreStep::Put => {
                let stored = result.child::<StoredObject, GitError>()??;
                let key = index_key(&self.document, &self.sha256);
                let index: SyncedOperation<InsertRowOperation<StoredObject>> =
                    records::insert(LOCAL_OBJECTS, key, &stored);
                self.step = StoreStep::Index;
                Ok(Next::Call(IoCall::child(index)))
            }
            StoreStep::Index => Ok(Next::Done(result.child::<StoredObject, GitError>()??)),
        }
    }
}

/// What the pending call of writing content answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutStep {
    Routing,
    Gate,
    Quota,
    Put,
    Describe,
}

/// Writes content to `key` in a bucket of this node through the ordinary put with its quota,
/// gate and routing guards, after checking its SHA-256.
pub fn put(
    scope: (Option<NodeId>, &NodeRouting),
    auth: &AuthContext,
    (name, bucket): (&str, BucketInfo),
    (key, size, sha256): (String, u64, &str),
    body: Blob,
) -> PutBlobOperation {
    PutBlobOperation::new(
        scope,
        auth,
        (name.to_string(), bucket),
        (key, size, sha256.to_string()),
        body,
    )
}

#[derive(Debug, PartialEq)]
pub struct PutBlobOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    name: String,
    bucket: Option<BucketInfo>,
    group_id: GroupId,
    key: String,
    size: u64,
    sha256: String,
    body: Option<Blob>,
    routing: Option<SnapshotOperation>,
    snapshot: Option<RoutingSnapshot>,
    gate: Option<GateContext>,
    step: PutStep,
    now_ms: u64,
}

type PutTurn = Result<Next<StoredObject>, GitError>;

impl PutBlobOperation {
    fn new(
        (node, routing): (Option<NodeId>, &NodeRouting),
        auth: &AuthContext,
        (name, bucket): (String, BucketInfo),
        (key, size, sha256): (String, u64, String),
        body: Blob,
    ) -> Self {
        let rules = BucketRules::of(&bucket);
        Self {
            node,
            auth: auth.clone(),
            name,
            group_id: bucket.group_id,
            routing: Some(SnapshotOperation::new(routing, bucket.group_id, rules)),
            bucket: Some(bucket),
            key,
            size,
            sha256,
            body: Some(body),
            snapshot: None,
            gate: None,
            step: PutStep::Routing,
            now_ms: 0,
        }
    }

    fn put(&mut self, config: RealmConfigDocument) -> PutTurn {
        let auth = &self.auth;
        let digest = hex::decode(&self.sha256).map_err(|_| GitError::Invalid)?;
        let mut put = PutObjectOperation::new(PutObjectConfig {
            user_id: auth.user_id,
            group_id: self.group_id,
            realm_id: auth.realm_id,
            node_id: self.node.ok_or(GitError::Unavailable)?,
            request: PutObjectInput {
                bucket: self.name.clone(),
                key: self.key.clone(),
                content_length: Some(self.size),
                body: Some(self.body.take().ok_or(IoError::Unfinished)?),
            },
            expected_checksums: vec![ExpectedChecksum {
                algorithm: ChecksumAlgorithm::Sha256,
                digest,
            }],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: config.quota.effective_group_ceiling(&self.group_id),
            routing: self.snapshot.take().ok_or(IoError::Unfinished)?,
        })
        .with_bucket_guard(self.bucket.take().ok_or(IoError::Unfinished)?)
        .with_restrictions(auth.path_restrictions.clone());
        if let Some(gate) = self.gate.take() {
            put = put.with_gate(gate);
        }
        self.step = PutStep::Put;
        Ok(Next::Call(IoCall::child(put)))
    }
}

impl Operation for PutBlobOperation {
    type Output = StoredObject;
    type Error = GitError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PutTurn {
        self.node.ok_or(GitError::Unavailable)?;
        let routing = self.routing.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(routing)))
    }

    fn next(&mut self, result: IoResult) -> PutTurn {
        match self.step {
            PutStep::Routing => {
                let snapshot = result.child::<RoutingSnapshot, RoutingInputsError>()?;
                self.snapshot = Some(snapshot.map_err(|_| GitError::Unavailable)?);
                self.step = PutStep::Gate;
                let gate = GateOperation::new(self.auth.realm_id, self.now_ms);
                Ok(Next::Call(IoCall::child(gate)))
            }
            PutStep::Gate => {
                let gate = result.child::<Option<GateContext>, GateContextError>()?;
                self.gate = gate.map_err(|_| GitError::Unavailable)?;
                self.step = PutStep::Quota;
                let config = GetConfigOperation::new(self.auth.realm_id);
                Ok(Next::Call(IoCall::child(config)))
            }
            PutStep::Quota => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                self.put(config.map_err(|_| GitError::Unavailable)?)
            }
            PutStep::Put => {
                let stored = result.child::<PutObjectResult, PutObjectError>()?;
                let stored = stored.map_err(|error| match error {
                    PutObjectError::IncompleteBody
                    | PutObjectError::MissingBody
                    | PutObjectError::ChecksumMismatch(_) => GitError::Invalid,
                    _ => GitError::Unavailable,
                })?;
                self.step = PutStep::Describe;
                let describe = DescribeObjectOperation {
                    node: self.node,
                    name: self.name.clone(),
                    group_id: self.group_id,
                    key: self.key.clone(),
                    version: Some(stored.version_id),
                };
                Ok(Next::Call(IoCall::child(describe)))
            }
            PutStep::Describe => {
                let described = result.child::<Option<StoredObject>, GitError>()??;
                Ok(Next::Done(described.ok_or(GitError::Unavailable)?))
            }
        }
    }
}

/// The size and digests of an object version of this node, or of its current version with
/// `None`; `None` when there is no such object. Callers authorize the read.
pub fn described(
    node: Option<NodeId>,
    (name, group_id): (&str, GroupId),
    key: &str,
    version: Option<Ulid>,
) -> DescribeObjectOperation {
    DescribeObjectOperation {
        node,
        name: name.to_string(),
        group_id,
        key: key.to_string(),
        version,
    }
}

#[derive(Debug, PartialEq)]
pub struct DescribeObjectOperation {
    node: Option<NodeId>,
    name: String,
    group_id: GroupId,
    key: String,
    version: Option<Ulid>,
}

impl Operation for DescribeObjectOperation {
    type Output = Option<StoredObject>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Option<StoredObject>>, GitError> {
        self.node.ok_or(GitError::Unavailable)?;
        let head = HeadObjectOperation::new(HeadObjectInput {
            bucket: self.name.clone(),
            key: self.key.clone(),
            version_id: self.version,
        });
        Ok(Next::Call(IoCall::child(head)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<StoredObject>>, GitError> {
        let node_id = self.node.ok_or(GitError::Unavailable)?;
        let head = match result.child::<HeadObjectResult, HeadObjectError>()? {
            Ok(head) => head,
            Err(_) if self.version.is_none() => return Ok(Next::Done(None)),
            Err(_) => return Err(GitError::Unavailable),
        };
        let (Some(location), Some(version_id)) = (head.location, head.version_id) else {
            return Ok(Next::Done(None));
        };
        let digest = |name: &str| location.hashes.get(name).cloned();
        let blake3 = digest("blake3").and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok());
        let (Some(blake3), Some(sha256)) = (blake3, digest("sha256")) else {
            return Ok(Next::Done(None));
        };
        Ok(Next::Done(Some(StoredObject {
            node_id,
            group_id: Some(self.group_id),
            bucket: self.name.clone(),
            key: self.key.clone(),
            version_id,
            size: location.blob_size,
            sha256: hex::encode(sha256),
            blake3,
            global: None,
        })))
    }
}

/// Describes a pack for a record. Larger packs are refused: large files belong in Git LFS.
pub fn describe_pack(pack: &Bytes) -> Result<GitPack, GitError> {
    if pack.len() > MAX_PACK_BYTES {
        return Err(GitError::Refused(format!(
            "the pushed Git objects exceed {} MiB; track large files with Git LFS",
            MAX_PACK_BYTES >> 20
        )));
    }
    let hashes = aruna_core::structs::checksum::Hasher::new_with_bytes(pack).to_map();
    let sha256 = hashes
        .get("sha256")
        .map(hex::encode)
        .ok_or(GitError::Unavailable)?;
    Ok(GitPack {
        sha256,
        size: pack.len() as u64,
    })
}

/// Notes a published pack that was made from this node's cache as imported there, so the next
/// projection does not index objects the cache already holds. A failure only costs that import.
pub fn mark_imported(
    (document_id, actor): (Ulid, aruna_core::UserId),
    pack: &GitPack,
) -> MarkImportedOperation {
    MarkImportedOperation {
        document_id,
        actor,
        digest: pack.sha256.clone(),
    }
}

#[derive(Debug, PartialEq)]
pub struct MarkImportedOperation {
    document_id: Ulid,
    actor: aruna_core::UserId,
    digest: String,
}

impl Operation for MarkImportedOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let effect = GitEffect::MarkImported {
            document_id: self.document_id,
            digest: self.digest.clone(),
        };
        Ok(Next::Call(git_call(effect, self.actor)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if !matches!(
            git_event(result),
            Ok(GitEvent::Imported(_) | GitEvent::Initialized)
        ) {
            let document_id = self.document_id;
            debug!(%document_id, "Published pack not marked as imported");
        }
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
