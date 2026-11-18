//! Answers which copy of one version this node holds, for local callers and peer requests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::LocationSummaryError;
use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::realm::peer_trust::ensure_realm_peer;
use crate::replication::protocol::{
    CopyCompliance, LocationCopyStorage, LocationSummary, LocationSummaryRequest,
    VersionReplicationMessage,
};
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::managed_copy::{CopyRequest, read_effect, registration_for, serve_reads};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::request_policy::{CompiledPolicySet, PolicyDecision};
use aruna_core::rows::read_row;
use aruna_core::structs::ORIGIN_DEVICE_TAG;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::node_subject::NodeSubjectRecord;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
    CurrentVersionPointer, ManagedCopyKey, VersionKey, bucket_permission_path,
    object_permission_path,
};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::Value;
use std::time::SystemTime;
use ulid::Ulid;

type Turn = Result<Next<LocalSummary>, LocationSummaryError>;

#[derive(Clone, Debug, Eq, PartialEq)]
enum SummaryState {
    Init,
    ReadRealm,
    ReadBucket,
    Snapshot,
    ReadPolicy,
    ReadHead,
    ReadVersion,
    ReadLocation,
    ReadCopyOrigin,
    CheckManagedCopy,
    ReadBackend,
    SendSummary,
    SendDenial,
    Close,
}

/// A local answer, plus the content hash that never goes on the wire and the
/// bucket record the answer was authorized against. The hash is what lets the
/// caller ask the durable holder index which other nodes store these bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct LocalSummary {
    pub summary: LocationSummary,
    pub blake3: Option<[u8; 32]>,
    pub bucket: Option<BucketInfo>,
    /// The resolved version is a delete marker. Relationship and queue policy
    /// may decline to replicate one, so no copy is coming for it.
    pub delete_marker: bool,
}

/// Answers "which copy does THIS node hold" for one version, locally or on a peer's stream; the
/// peer path also proves realm membership. The READ check and the answer's rows are read by a
/// child of this type in one read snapshot.
#[derive(Debug, PartialEq)]
pub struct LocationSummaryOperation {
    request: LocationSummaryRequest,
    peer: Option<NodeId>,
    local_node: Option<NodeId>,
    stream_id: Option<Ulid>,
    snapshot: bool,
    bucket: Option<BucketInfo>,
    /// Identity of the bucket record the READ check was evaluated against.
    authorized: Option<(Ulid, SystemTime, UserId)>,
    permission_path: Option<String>,
    policy_allowed: bool,
    policy_current: bool,
    state: SummaryState,
    version_id: Option<Ulid>,
    blake3: Option<[u8; 32]>,
    delete_marker: bool,
    summary: LocationSummary,
    /// Refs of the resolved version, compared against its registration before
    /// the summary may claim the copy is held.
    version_refs: Vec<PlacementPolicyRef>,
    pending_copy: Option<(ManagedCopyKey, BackendLocation)>,
    /// The answer sent on the stream, returned once the stream is closed.
    answer: Option<LocalSummary>,
}

impl LocationSummaryOperation {
    /// Local read. The operation reads the bucket and checks READ itself, so
    /// the answer and its authorization come from the same bucket record.
    pub fn new_local(local_node: NodeId, request: LocationSummaryRequest) -> Self {
        Self::build(request, None, Some(local_node), None)
    }

    pub fn new_incoming(
        peer: NodeId,
        local_node: NodeId,
        stream_id: Ulid,
        request: LocationSummaryRequest,
    ) -> Self {
        Self::build(request, Some(peer), Some(local_node), Some(stream_id))
    }

    fn build(
        request: LocationSummaryRequest,
        peer: Option<NodeId>,
        local_node: Option<NodeId>,
        stream_id: Option<Ulid>,
    ) -> Self {
        let version_id = request.version_id;
        Self {
            request,
            peer,
            local_node,
            stream_id,
            snapshot: false,
            bucket: None,
            authorized: None,
            permission_path: None,
            policy_allowed: false,
            policy_current: false,
            state: SummaryState::Init,
            version_id,
            blake3: None,
            delete_marker: false,
            summary: LocationSummary::absent(),
            version_refs: Vec::new(),
            pending_copy: None,
            answer: None,
        }
    }

    pub fn with_policy(mut self, allowed: bool) -> Self {
        self.policy_allowed = allowed;
        self
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            SummaryState::Init => "init",
            SummaryState::ReadRealm => "read_realm",
            SummaryState::ReadBucket => "read_bucket",
            SummaryState::Snapshot => "snapshot",
            SummaryState::ReadPolicy => "read_policy",
            SummaryState::ReadHead => "read_head",
            SummaryState::ReadVersion => "read_version",
            SummaryState::ReadLocation => "read_location",
            SummaryState::ReadCopyOrigin => "read_copy_origin",
            SummaryState::CheckManagedCopy => "check_managed_copy",
            SummaryState::ReadBackend => "read_backend",
            SummaryState::SendSummary => "send_summary",
            SummaryState::SendDenial => "send_denial",
            SummaryState::Close => "close",
        }
    }

    fn unexpected(&self, event: Event) -> Turn {
        Err(LocationSummaryError::Unexpected {
            state: self.state_name(),
            event: event.label().to_string(),
        })
    }

    fn call(&mut self, state: SummaryState, call: IoCall) -> Turn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn read(&mut self, state: SummaryState, space: &str, key: Vec<u8>) -> Turn {
        self.call(state, read_row(space, key))
    }

    fn read_realm(&mut self) -> Turn {
        let key = self.request.realm_id.as_bytes().to_vec();
        self.read(SummaryState::ReadRealm, REALM_CONFIG_KEYSPACE, key)
    }

    fn read_bucket(&mut self) -> Turn {
        let key = self.request.bucket.as_bytes().to_vec();
        self.read(SummaryState::ReadBucket, S3_BUCKET_KEYSPACE, key)
    }

    fn snapshot_child(&self) -> Self {
        Self {
            snapshot: true,
            bucket: self.bucket.clone(),
            authorized: self.authorized,
            permission_path: self.permission_path.clone(),
            policy_allowed: self.policy_allowed,
            ..Self::build(self.request.clone(), self.peer, self.local_node, None)
        }
    }

    fn read_policy(&mut self) -> Turn {
        let Some(bucket) = self.bucket.as_ref() else {
            return Err(LocationSummaryError::BucketNotFound);
        };
        let mut reads = vec![
            (
                S3_BUCKET_KEYSPACE.to_string(),
                self.request.bucket.as_bytes().to_vec().into(),
            ),
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                self.request.realm_id.as_bytes().to_vec().into(),
            ),
            (AUTH_KEYSPACE.to_string(), bucket.group_id.to_bytes().into()),
        ];
        // The caller's roles are read in this snapshot, after the three policy rows.
        let roles = self.read_check().map(|check| check.reads());
        let Some(Ok(roles)) = roles else {
            return Err(LocationSummaryError::Denied);
        };
        reads.extend(roles);
        self.call(SummaryState::ReadPolicy, IoCall::Read(reads))
    }

    fn read_check(&self) -> Option<CheckPermissionsConfig> {
        Some(CheckPermissionsConfig {
            auth_context: self.request.auth_context.clone(),
            path: self.permission_path.clone()?,
            required_permission: Permission::READ,
        })
    }

    fn read_head(&mut self) -> Turn {
        let key = BlobHeadKey::new(&self.request.bucket, &self.request.key).to_bytes()?;
        self.read(SummaryState::ReadHead, BLOB_HEAD_KEYSPACE, key)
    }

    fn read_version(&mut self) -> Turn {
        let Some(version_id) = self.version_id else {
            return self.finish_snapshot();
        };
        let key = VersionKey::new(&self.request.bucket, &self.request.key, version_id);
        let key = key.to_bytes()?;
        self.read(SummaryState::ReadVersion, BLOB_VERSIONS_KEYSPACE, key)
    }

    fn read_location(&mut self, key: BlobLocationKey) -> Turn {
        let key = key.to_bytes();
        self.read(SummaryState::ReadLocation, BLOB_LOCATIONS_KEYSPACE, key)
    }

    fn read_backend(&mut self, backend_id: Ulid) -> Turn {
        let key = backend_id.to_bytes().to_vec();
        self.read(SummaryState::ReadBackend, STORAGE_BACKEND_KEYSPACE, key)
    }

    /// The snapshot's outcome; the runner closes the snapshot with it.
    fn finish_snapshot(&mut self) -> Turn {
        self.summary.version_id = self.version_id;
        self.summary.group_id = self.bucket.as_ref().map(|bucket| bucket.group_id);
        Ok(Next::Done(LocalSummary {
            summary: self.summary.clone(),
            blake3: self.blake3,
            bucket: self.bucket.clone(),
            delete_marker: self.delete_marker,
        }))
    }

    fn send(&mut self, state: SummaryState, stream_id: Ulid, payload: Vec<u8>) -> Turn {
        let send = BlobEffect::SendMessage { stream_id, payload };
        self.call(state, Effect::Blob(send).into())
    }

    fn close(&mut self, stream_id: Ulid) -> Turn {
        let close = BlobEffect::CloseConnection { stream_id };
        self.call(SummaryState::Close, Effect::Blob(close).into())
    }

    fn accept_snapshot(&mut self, outcome: Result<LocalSummary, LocationSummaryError>) -> Turn {
        let local = match outcome {
            Ok(local) => local,
            Err(LocationSummaryError::Denied) => return self.deny(),
            Err(error) => return Err(error),
        };
        let Some(stream_id) = self.stream_id else {
            return Ok(Next::Done(local));
        };
        let summary = local.summary.clone();
        let payload = VersionReplicationMessage::LocationSummaryResponse(summary).to_bytes()?;
        self.answer = Some(local);
        self.send(SummaryState::SendSummary, stream_id, payload)
    }

    /// A refusal is answered rather than dropped: a caller must be able to tell
    /// a node that will not answer from one that cannot.
    fn deny(&mut self) -> Turn {
        let Some(stream_id) = self.stream_id else {
            return Err(LocationSummaryError::Denied);
        };
        let payload = VersionReplicationMessage::LocationSummaryDenied.to_bytes()?;
        self.send(SummaryState::SendDenial, stream_id, payload)
    }

    fn accept_sent(&mut self, event: Event) -> Turn {
        let Event::Blob(BlobEvent::MessageSent { stream_id }) = event else {
            return self.unexpected(event);
        };
        self.close(stream_id)
    }

    fn accept_closed(&mut self, event: Event) -> Turn {
        let Event::Blob(BlobEvent::ConnectionClosed { stream_id }) = event else {
            return self.unexpected(event);
        };
        if Some(stream_id) != self.stream_id {
            return self.unexpected(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
        }
        self.stream_id = None;
        match self.answer.take() {
            Some(local) => Ok(Next::Done(local)),
            None => Err(LocationSummaryError::Denied),
        }
    }

    fn accept_realm(&mut self, value: Option<Value>) -> Turn {
        let (Some(value), Some(peer)) = (value, self.peer) else {
            return Err(LocationSummaryError::PeerDenied);
        };
        let document = RealmConfigDocument::from_bytes(&value)?;
        if ensure_realm_peer(&document, peer, self.request.realm_id, true).is_err() {
            return Err(LocationSummaryError::PeerDenied);
        }
        self.read_bucket()
    }

    fn accept_bucket(&mut self, value: Option<Value>) -> Turn {
        let Some(value) = value else {
            return Err(LocationSummaryError::BucketNotFound);
        };
        let bucket = BucketInfo::from_bytes(&value)?;
        let Some(local_node) = self.local_node else {
            return Err(LocationSummaryError::PeerDenied);
        };
        let group_id = bucket.group_id;
        self.authorized = Some(bucket.identity());
        self.bucket = Some(bucket);
        let path = if self.request.key.is_empty() {
            bucket_permission_path(
                self.request.realm_id,
                group_id,
                local_node,
                &self.request.bucket,
            )
        } else {
            object_permission_path(
                self.request.realm_id,
                group_id,
                local_node,
                &self.request.bucket,
                &self.request.key,
            )
        };
        self.permission_path = Some(path);
        let child = IoCall::child(self.snapshot_child());
        self.call(SummaryState::Snapshot, child)
    }

    fn accept_policy(&mut self, values: Vec<Option<Value>>) -> Turn {
        let denied = || Err(LocationSummaryError::Denied);
        let Some(Some(bucket_value)) = values.first() else {
            return denied();
        };
        let current = BucketInfo::from_bytes(bucket_value)?;
        if Some(current.identity()) != self.authorized {
            return denied();
        }
        let Some(Some(realm_value)) = values.get(1) else {
            return denied();
        };
        let realm = RealmConfigDocument::from_bytes(realm_value)?;
        if realm.realm_id != self.request.realm_id {
            return denied();
        }
        if let Some(peer) = self.peer
            && ensure_realm_peer(&realm, peer, self.request.realm_id, true).is_err()
        {
            return denied();
        }
        let Some(Some(group_value)) = values.get(2) else {
            return denied();
        };
        let group = GroupAuthorizationDocument::from_bytes(group_value)?;
        if group.group_id != current.group_id {
            return denied();
        }
        let Some(path) = self.permission_path.as_deref() else {
            return denied();
        };
        let request = policy_request_with(
            path,
            &Permission::READ,
            Some(&self.request.auth_context),
            PolicyRequestExtras::operation("s3.GetObject"),
        );
        let Ok(realm_set) = CompiledPolicySet::compile(&realm.request_policies) else {
            return denied();
        };
        let Ok(group_set) = CompiledPolicySet::compile(&group.policies) else {
            return denied();
        };
        self.policy_current = self.policy_allowed
            && matches!(realm_set.evaluate(&request), PolicyDecision::Allowed)
            && matches!(group_set.evaluate(&request), PolicyDecision::Allowed);
        let roles = values.iter().skip(3).cloned().collect();
        let allowed = self.read_check().map(|check| check.allowed(roles));
        match allowed {
            Some(Ok(true)) if self.policy_current => self.resolve_version(),
            _ => denied(),
        }
    }

    fn resolve_version(&mut self) -> Turn {
        match self.version_id {
            Some(_) => self.read_version(),
            None => self.read_head(),
        }
    }

    fn accept_head(&mut self, value: Option<Value>) -> Turn {
        let Some(value) = value else {
            return self.finish_snapshot();
        };
        let pointer = CurrentVersionPointer::from_bytes(&value)?;
        self.version_id = Some(pointer.version_id);
        self.read_version()
    }

    fn accept_version(&mut self, value: Option<Value>) -> Turn {
        // An unknown version is the caller's "not found": no copy, no id.
        let Some(value) = value else {
            self.version_id = None;
            return self.finish_snapshot();
        };
        let version = BlobVersion::from_bytes(&value)?;
        self.delete_marker = version.is_deleted();
        self.version_refs = version.placement_policies.clone();
        self.summary.origin_device = version.metadata.get(ORIGIN_DEVICE_TAG).cloned();
        match version.location_key() {
            Some(key) => {
                self.blake3 = Some(key.blake3_hash);
                self.summary.materialized = true;
                self.read_location(key)
            }
            None => self.finish_snapshot(),
        }
    }

    fn accept_location(&mut self, value: Option<Value>) -> Turn {
        let Some(value) = value else {
            return self.finish_snapshot();
        };
        let location = BackendLocation::from_bytes(&value)?;
        self.summary.blob_size = Some(location.blob_size);
        self.summary.hashes = location.hashes.clone().into_iter().collect();
        let Some(version_id) = self.version_id else {
            return self.finish_snapshot();
        };
        let key = ManagedCopyKey::new(
            VersionKey::new(
                self.request.bucket.clone(),
                self.request.key.clone(),
                version_id,
            ),
            location.backend.clone(),
        );
        // An ungoverned copy is advertised from its location record alone, as
        // before; its registration is read only to learn where it came from.
        let (effect, state) = match self.version_refs.is_empty() {
            true => (read_effect(&key, None)?, SummaryState::ReadCopyOrigin),
            false => (serve_reads(&key, None)?, SummaryState::CheckManagedCopy),
        };
        let reads = match effect {
            Effect::Storage(StorageEffect::Read { key_space, key, .. }) => vec![(key_space, key)],
            Effect::Storage(StorageEffect::BatchRead { reads, .. }) => reads,
            _ => {
                return Err(LocationSummaryError::Unexpected {
                    state: self.state_name(),
                    event: "a copy read of another form".to_string(),
                });
            }
        };
        self.pending_copy = Some((key, location));
        self.call(state, IoCall::Read(reads))
    }

    /// The registration matching exactly this copy, whatever its state.
    fn copy_request<'a>(&'a self, key: &'a ManagedCopyKey) -> CopyRequest<'a> {
        CopyRequest {
            key,
            node_id: self.local_node,
            blake3: self.blake3,
            refs: &self.version_refs,
            subject_generation: None,
        }
    }

    fn accept_copy_origin(&mut self, value: Option<Value>) -> Turn {
        let Some((key, location)) = self.pending_copy.take() else {
            return self.finish_snapshot();
        };
        if let Ok(record) = registration_for(value.as_deref(), &self.copy_request(&key)) {
            self.summary.origin = record.origin;
        }
        // No rule governs this version, so no rule can refuse the copy either.
        self.summary.compliance = CopyCompliance::Allowed;
        self.describe(location)
    }

    /// A governed copy the node is not serving is listed with its verdict rather
    /// than hidden: the caller already holds READ, and no rule is disclosed.
    fn accept_managed_copy(&mut self, values: Vec<Option<Value>>) -> Turn {
        let Some((key, location)) = self.pending_copy.take() else {
            return self.finish_snapshot();
        };
        let Ok([copy, subject]) = <[Option<Value>; 2]>::try_from(values) else {
            return self.finish_snapshot();
        };
        let Ok(record) = registration_for(copy.as_deref(), &self.copy_request(&key)) else {
            return self.finish_snapshot();
        };
        self.summary.origin = record.origin;
        // A subject row that is missing or unreadable leaves the copy held but
        // unserveable, which is a quarantine verdict rather than an omission.
        let subject = subject.and_then(|value| NodeSubjectRecord::from_bytes(&value).ok());
        self.summary.compliance = match subject {
            Some(subject)
                if record.state.is_serveable()
                    && record.subject_generation == subject.subject.generation
                    && !subject.serving_blocked =>
            {
                CopyCompliance::Allowed
            }
            _ => CopyCompliance::Quarantined,
        };
        self.describe(location)
    }

    fn describe(&mut self, location: BackendLocation) -> Turn {
        self.summary.held = true;
        match location.backend {
            BackendRef::Node(_) => {
                self.summary.storage = Some(LocationCopyStorage::NodeManaged {
                    storage_class: location.storage_class,
                });
                self.finish_snapshot()
            }
            BackendRef::Group(backend_id) => {
                self.summary.storage = Some(LocationCopyStorage::GroupBackend {
                    backend_id,
                    name: None,
                });
                self.read_backend(backend_id)
            }
        }
    }

    fn accept_backend(&mut self, value: Option<Value>) -> Turn {
        if let Some(value) = value
            && let Ok(record) = GroupStorage::from_bytes(&value)
            && let Some(LocationCopyStorage::GroupBackend { name, .. }) =
                self.summary.storage.as_mut()
        {
            *name = Some(record.name);
        }
        self.finish_snapshot()
    }
}

impl Operation for LocationSummaryOperation {
    type Output = LocalSummary;
    type Error = LocationSummaryError;

    fn first(&mut self) -> Turn {
        if self.snapshot {
            return self.read_policy();
        }
        let user_id = self.request.auth_context.user_id;
        if self.request.auth_context.realm_id != self.request.realm_id
            || (user_id.is_nil() && user_id.realm_id != self.request.realm_id)
        {
            return self.deny();
        }
        match self.peer {
            Some(_) => self.read_realm(),
            None => self.read_bucket(),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            SummaryState::Init => self.first(),
            SummaryState::ReadRealm => self.accept_realm(result.value()?),
            SummaryState::ReadBucket => self.accept_bucket(result.value()?),
            SummaryState::Snapshot => {
                self.accept_snapshot(result.child::<LocalSummary, LocationSummaryError>()?)
            }
            SummaryState::ReadPolicy => self.accept_policy(result.values()?),
            SummaryState::ReadHead => self.accept_head(result.value()?),
            SummaryState::ReadVersion => self.accept_version(result.value()?),
            SummaryState::ReadLocation => self.accept_location(result.value()?),
            SummaryState::ReadCopyOrigin => self.accept_copy_origin(result.value()?),
            SummaryState::CheckManagedCopy => self.accept_managed_copy(result.values()?),
            SummaryState::ReadBackend => self.accept_backend(result.value()?),
            SummaryState::SendSummary | SummaryState::SendDenial => {
                self.accept_sent(result.event()?)
            }
            SummaryState::Close => self.accept_closed(result.event()?),
        }
    }

    fn transaction(&self) -> TransactionMode {
        match self.snapshot {
            true => TransactionMode::ReadSnapshot,
            false => TransactionMode::None,
        }
    }

    /// A failed run closes the stream it still holds.
    fn abort(&mut self) -> Vec<IoCall> {
        let close = |stream_id| Effect::Blob(BlobEffect::CloseConnection { stream_id }).into();
        self.stream_id.take().map(close).into_iter().collect()
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{LocalSummary, LocationSummaryError, LocationSummaryOperation, SummaryState, Turn};
    use crate::replication::protocol::{
        CopyCompliance, LocationCopyStorage, VersionReplicationMessage,
    };
    use crate::tests::locations::{node_id, realm_id, request};
    use aruna_core::UserId;
    use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
    use aruna_core::effects::{BlobEffect, Effect};
    use aruna_core::events::{BlobEvent, Event};
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::identity::auth::{Permission, Role};
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
    };
    use aruna_core::structs::storage::blob::{
        BackendLocation, BackendRef, BlobVersion, BucketInfo,
    };
    use aruna_core::structs::storage::blob::{CopyOrigin, VersionKey};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::types::Value;
    use std::collections::{HashMap, HashSet};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn location(backend: BackendRef, storage_class: Option<String>) -> BackendLocation {
        BackendLocation {
            backend,
            storage_class,
            root: "root".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "raw/run1.tar_0".to_string(),
            ulid: Ulid::from_bytes([2u8; 16]),
            format: StoredFormat::default(),
            created_by: UserId::nil(realm_id()),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: HashMap::new(),
        }
    }

    fn bucket_info() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_bytes([8u8; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(realm_id()),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
    }

    /// Answers the bucket read and the READ check every local answer starts with.
    fn authorized(version_id: Option<Ulid>) -> LocationSummaryOperation {
        let (_, mut child) = reading(true, version_id);
        child.next(policy_batch()).unwrap();
        child
    }

    /// A local run at its snapshot, with the child that reads it, on its first read.
    fn reading(
        allowed: bool,
        version_id: Option<Ulid>,
    ) -> (LocationSummaryOperation, LocationSummaryOperation) {
        let mut parent = LocationSummaryOperation::new_local(node_id(5), request(version_id))
            .with_policy(allowed);
        parent.first().unwrap();
        let child = enter_snapshot(&mut parent, bucket_info().to_bytes().unwrap());
        (parent, child)
    }

    /// Answers the bucket read and starts the snapshot child.
    fn enter_snapshot(
        parent: &mut LocationSummaryOperation,
        bucket: Vec<u8>,
    ) -> LocationSummaryOperation {
        let turn = parent.next(read_result(Some(bucket)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(parent.state, SummaryState::Snapshot);
        let mut child = parent.snapshot_child();
        assert_eq!(child.transaction(), TransactionMode::ReadSnapshot);
        child.first().unwrap();
        child
    }

    fn done(turn: Turn) -> LocalSummary {
        match turn {
            Ok(Next::Done(local)) => local,
            other => panic!("expected the answer, got {other:?}"),
        }
    }

    fn denied(turn: Turn) {
        assert!(
            matches!(turn, Err(LocationSummaryError::Denied)),
            "{turn:?}"
        );
    }

    fn snapshot_denied() -> IoResult {
        let outcome = Err::<LocalSummary, _>(LocationSummaryError::Denied);
        IoResult::Child(ChildOutcome::new(outcome))
    }

    fn event(event: BlobEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Blob(event)))
    }

    fn asked(turn: &Turn) -> &Effect {
        match turn {
            Ok(Next::Call(IoCall::Effect(effect))) => effect,
            other => panic!("expected an effect call, got {other:?}"),
        }
    }

    fn policy_batch() -> IoResult {
        let realm = RealmConfigDocument::default_for_realm(realm_id(), Vec::new());
        let group = GroupAuthorizationDocument::default_group_doc(
            UserId::nil(realm_id()),
            realm_id(),
            bucket_info().group_id,
        );
        let realm_roles = RealmAuthorizationDocument::default_realm_doc(realm_id());
        // The test caller is the nil user, which holds only public roles.
        let group_id = bucket_info().group_id;
        let mut group_roles = GroupAuthorizationDocument::default_group_doc(
            UserId::nil(realm_id()),
            realm_id(),
            group_id,
        );
        let role_id = Ulid::from(20u128);
        group_roles.roles.insert(
            role_id,
            Role {
                role_id,
                name: "public".to_string(),
                permissions: HashMap::from([(
                    format!("/{}/g/{group_id}/**", realm_id()),
                    Permission::READ,
                )]),
                assigned_users: HashSet::from([UserId::nil(realm_id())]),
            },
        );
        let actor = aruna_core::structs::identity::auth::Actor {
            node_id: node_id(5),
            user_id: UserId::nil(realm_id()),
            realm_id: realm_id(),
        };
        IoResult::Values(vec![
            Some(bucket_info().to_bytes().unwrap().into()),
            Some(postcard::to_allocvec(&realm).unwrap().into()),
            Some(group.to_bytes(&actor).unwrap().into()),
            Some(realm_roles.to_bytes(&actor).unwrap().into()),
            Some(group_roles.to_bytes(&actor).unwrap().into()),
        ])
    }

    /// The policy rows of a caller whose group role row is gone, so the check refuses it.
    fn refused_batch() -> IoResult {
        with_row(4, None)
    }

    /// The policy rows with one of them replaced.
    fn with_row(index: usize, value: Option<Value>) -> IoResult {
        let IoResult::Values(mut values) = policy_batch() else {
            unreachable!();
        };
        values[index] = value;
        IoResult::Values(values)
    }

    fn read_result(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    fn materialized() -> BlobVersion {
        BlobVersion::materialized(
            [7u8; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            SystemTime::UNIX_EPOCH,
            UserId::nil(realm_id()),
            None,
        )
    }

    #[test]
    fn reports_copy_class() {
        let version_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = authorized(Some(version_id));
        operation
            .next(read_result(Some(materialized().to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(
                location(BackendRef::node_default(), Some("cold".to_string()))
                    .to_bytes()
                    .unwrap(),
            )))
            .unwrap();
        let local = done(operation.next(read_result(None)));
        assert_eq!(local.blake3, Some([7u8; 32]));
        let summary = local.summary;
        assert!(summary.held);
        assert_eq!(summary.version_id, Some(version_id));
        assert_eq!(
            summary.storage,
            Some(LocationCopyStorage::NodeManaged {
                storage_class: Some("cold".to_string()),
            })
        );
    }

    #[test]
    fn names_origin_device() {
        // The device a version came from is answered under the same READ as its bytes.
        let version_id = Ulid::from_bytes([3u8; 16]);
        let mut version = materialized();
        version.metadata.insert(
            aruna_core::structs::ORIGIN_DEVICE_TAG.to_string(),
            "device-ada".to_string(),
        );
        let mut operation = authorized(Some(version_id));
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(
                location(BackendRef::node_default(), None)
                    .to_bytes()
                    .unwrap(),
            )))
            .unwrap();
        let summary = done(operation.next(read_result(None))).summary;
        assert_eq!(summary.origin_device.as_deref(), Some("device-ada"));
    }

    fn hashed_location() -> BackendLocation {
        let mut location = location(BackendRef::node_default(), None);
        location.hashes.insert(
            aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
            [7u8; 32].to_vec(),
        );
        location
    }

    fn policy_ref() -> aruna_core::structs::placement::policy::PlacementPolicyRef {
        aruna_core::structs::placement::policy::PlacementPolicyRef {
            policy_id: Ulid::from_bytes([4u8; 16]),
            digest: [5u8; 32],
        }
    }

    fn copy_record(
        version_id: Ulid,
        state: aruna_core::structs::storage::blob::ManagedCopyState,
        origin: CopyOrigin,
        governed: bool,
    ) -> aruna_core::structs::storage::blob::ManagedCopyRecord {
        let refs = match governed {
            true => vec![policy_ref()],
            false => Vec::new(),
        };
        aruna_core::structs::storage::blob::ManagedCopyRecord::new(
            VersionKey::new("raw", "run1.tar", version_id),
            node_id(5),
            hashed_location(),
            refs,
            7,
            state,
        )
        .expect("record builds")
        .stored_under(match governed {
            true => 1,
            false => 0,
        })
        .from_origin(origin)
    }

    /// Copy row plus subject row, the pair a governed answer reads at once.
    fn serve_batch(
        record: &aruna_core::structs::storage::blob::ManagedCopyRecord,
        blocked: bool,
    ) -> IoResult {
        let mut subject = aruna_core::structs::placement::node_subject::NodeSubjectRecord::seed(
            aruna_core::structs::placement::policy::PlacementSubject {
                node_id: node_id(5),
                generation: 1,
                location: "eu-west".to_string(),
                labels: Default::default(),
                executor_kind: None,
                local_to_controller: true,
            },
        )
        .expect("subject is valid");
        subject.serving_blocked = blocked;
        IoResult::Values(vec![
            Some(record.to_bytes().unwrap().into()),
            Some(subject.to_bytes().unwrap().into()),
        ])
    }

    #[test]
    fn reports_copy_origin() {
        // An ungoverned copy is advertised as before and now names its cause.
        let version_id = Ulid::from_bytes([3u8; 16]);
        let relationship_id = Ulid::from_bytes([6u8; 16]);
        let mut operation = authorized(Some(version_id));
        operation
            .next(read_result(Some(materialized().to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(hashed_location().to_bytes().unwrap())))
            .unwrap();
        let summary = done(
            operation.next(read_result(Some(
                copy_record(
                    version_id,
                    aruna_core::structs::storage::blob::ManagedCopyState::Registered,
                    CopyOrigin::Sync { relationship_id },
                    false,
                )
                .to_bytes()
                .unwrap(),
            ))),
        )
        .summary;
        assert!(summary.held);
        assert_eq!(summary.origin, CopyOrigin::Sync { relationship_id });
        assert_eq!(summary.compliance, CopyCompliance::Allowed);
    }

    fn governed_version() -> BlobVersion {
        materialized()
            .with_policies(vec![policy_ref()])
            .expect("refs stored")
    }

    #[test]
    fn lists_quarantined_copy() {
        // A copy this node holds but refuses to serve is reported rather than
        // hidden, and it names no rule.
        let version_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = authorized(Some(version_id));
        operation
            .next(read_result(Some(governed_version().to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(hashed_location().to_bytes().unwrap())))
            .unwrap();
        let summary = done(operation.next(serve_batch(
            &copy_record(
                version_id,
                aruna_core::structs::storage::blob::ManagedCopyState::Quarantined(
                    aruna_core::structs::storage::blob::ManagedCopyQuarantine::PolicyViolation,
                ),
                CopyOrigin::Replicate,
                true,
            ),
            false,
        )))
        .summary;
        assert!(summary.held);
        assert_eq!(summary.origin, CopyOrigin::Replicate);
        assert_eq!(summary.compliance, CopyCompliance::Quarantined);
    }

    #[test]
    fn reports_blocked_copy() {
        // Revalidation blocks serving; the copy is still held and reported with
        // its origin rather than dropped from the answer.
        let version_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = authorized(Some(version_id));
        operation
            .next(read_result(Some(governed_version().to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(hashed_location().to_bytes().unwrap())))
            .unwrap();
        let summary = done(operation.next(serve_batch(
            &copy_record(
                version_id,
                aruna_core::structs::storage::blob::ManagedCopyState::Registered,
                CopyOrigin::Replicate,
                true,
            ),
            true,
        )))
        .summary;
        assert!(summary.held);
        assert_eq!(summary.origin, CopyOrigin::Replicate);
        assert_eq!(summary.compliance, CopyCompliance::Quarantined);
    }

    #[test]
    fn allows_served_copy() {
        let version_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = authorized(Some(version_id));
        operation
            .next(read_result(Some(governed_version().to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(hashed_location().to_bytes().unwrap())))
            .unwrap();
        let summary = done(operation.next(serve_batch(
            &copy_record(
                version_id,
                aruna_core::structs::storage::blob::ManagedCopyState::Registered,
                CopyOrigin::Write,
                true,
            ),
            false,
        )))
        .summary;
        assert!(summary.held);
        assert_eq!(summary.origin, CopyOrigin::Write);
        assert_eq!(summary.compliance, CopyCompliance::Allowed);
    }

    #[test]
    fn names_group_backend() {
        // Tenant-owned storage is the durability signal, so id and name ship.
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let mut operation = authorized(Some(Ulid::from_bytes([3u8; 16])));
        operation
            .next(read_result(Some(materialized().to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(
                location(BackendRef::Group(backend_id), None)
                    .to_bytes()
                    .unwrap(),
            )))
            .unwrap();
        operation.next(read_result(None)).unwrap();
        let summary = done(operation.next(read_result(None))).summary;
        assert_eq!(
            summary.storage,
            Some(LocationCopyStorage::GroupBackend {
                backend_id,
                name: None,
            })
        );
    }

    #[test]
    fn unknown_version_empty() {
        // An unknown version is no copy anywhere, not a copy in an unknown state.
        let mut operation = authorized(Some(Ulid::from_bytes([3u8; 16])));
        let local = done(operation.next(read_result(None)));
        assert!(local.blake3.is_none());
        assert!(!local.summary.held);
        assert_eq!(local.summary.version_id, None);
    }

    #[test]
    fn carries_bucket_record() {
        // The caller must fan out from the record the answer was authorized on.
        let mut operation = authorized(Some(Ulid::from_bytes([3u8; 16])));
        let local = done(operation.next(read_result(None)));

        assert_eq!(local.bucket, Some(bucket_info()));
    }

    #[test]
    fn checks_serving_realm() {
        // A user of another realm is answered here; a session of another realm, or an
        // anonymous identity of another realm, is denied.
        let foreign = aruna_core::structs::identity::realm::RealmId::from_bytes([8u8; 32]);
        for (user_id, serving_realm, accepted) in [
            (UserId::local(Ulid::from(5u128), foreign), realm_id(), true),
            (UserId::local(Ulid::from(5u128), realm_id()), foreign, false),
            (UserId::nil(foreign), realm_id(), false),
        ] {
            let mut request = request(None);
            request.auth_context.user_id = user_id;
            request.auth_context.realm_id = serving_realm;
            let mut operation = LocationSummaryOperation::new_local(node_id(5), request);
            let turn = operation.first();
            if accepted {
                assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
            } else {
                denied(turn);
            }
        }
    }

    #[test]
    fn replaced_bucket_denied() {
        // A bucket deleted and recreated under another group must not answer to
        // a caller authorized against the generation the read started on.
        let (_, mut child) = reading(true, Some(Ulid::from_bytes([3u8; 16])));
        let replaced = BucketInfo {
            group_id: Ulid::from_bytes([9u8; 16]),
            created_at: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1),
            ..bucket_info()
        };
        denied(child.next(with_row(0, Some(replaced.to_bytes().unwrap().into()))));
    }

    #[test]
    fn deleted_bucket_fails() {
        // A bucket that vanished mid-read has no answer to give.
        let (_, mut child) = reading(true, Some(Ulid::from_bytes([3u8; 16])));
        denied(child.next(with_row(0, None)));
    }

    #[test]
    fn local_refuses_denied() {
        // Without a stream there is no one to answer, so the read just fails.
        let (mut parent, mut child) = reading(true, Some(Ulid::from_bytes([3u8; 16])));
        denied(child.next(refused_batch()));
        denied(parent.next(snapshot_denied()));
        assert!(parent.abort().is_empty());
    }

    #[test]
    fn policy_refuses_denied() {
        let (_, mut child) = reading(false, Some(Ulid::from_bytes([3u8; 16])));
        denied(child.next(policy_batch()));
    }

    #[test]
    fn policy_snapshot_denies() {
        let (_, mut child) = reading(true, Some(Ulid::from_bytes([3u8; 16])));
        let mut realm = RealmConfigDocument::default_for_realm(realm_id(), Vec::new());
        realm.request_policies.push(RequestPolicy {
            policy_id: Ulid::from(12u128),
            name: "deny-all".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: "true".to_string(),
            enabled: true,
        });
        let realm = postcard::to_allocvec(&realm).unwrap();
        denied(child.next(with_row(1, Some(realm.into()))));
    }

    #[test]
    fn answers_on_stream() {
        // The peer path must never reply before the permission check passed.
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let peer = node_id(4);
        let mut operation = LocationSummaryOperation::new_incoming(
            peer,
            node_id(5),
            stream_id,
            request(Some(Ulid::from_bytes([3u8; 16]))),
        )
        .with_policy(true);
        let turn = operation.first();
        let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
            panic!("expected a realm read first, got {turn:?}")
        };
        assert_eq!(reads[0].0, aruna_core::keyspaces::REALM_CONFIG_KEYSPACE);

        // The answer goes out after the snapshot, and the run ends once the stream is closed.
        operation.next(read_result(Some(peer_realm(peer)))).unwrap();
        let mut child = enter_snapshot(&mut operation, bucket_info().to_bytes().unwrap());
        let mut batch = policy_batch();
        let IoResult::Values(values) = &mut batch else {
            unreachable!();
        };
        values[1] = Some(peer_realm(peer).into());
        child.next(batch).unwrap();
        let local = done(child.next(read_result(None)));
        let outcome = ChildOutcome::new(Ok::<_, LocationSummaryError>(local.clone()));
        let turn = operation.next(IoResult::Child(outcome));
        let Effect::Blob(BlobEffect::SendMessage { payload, .. }) = asked(&turn) else {
            panic!("expected the summary, got {turn:?}")
        };
        assert_eq!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::LocationSummaryResponse(local.summary.clone())
        );
        let turn = operation.next(event(BlobEvent::MessageSent { stream_id }));
        assert!(matches!(
            asked(&turn),
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
        let closed = operation.next(event(BlobEvent::ConnectionClosed { stream_id }));
        assert_eq!(done(closed), local);
        assert!(operation.abort().is_empty());
    }

    fn peer_realm(peer: aruna_core::NodeId) -> Vec<u8> {
        let mut realm = RealmConfigDocument::default_for_realm(realm_id(), Vec::new());
        realm.ensure_node(peer, RealmNodeKind::Server);
        postcard::to_allocvec(&realm).unwrap()
    }

    #[test]
    fn peer_removed_read() {
        // A peer removed in the fenced realm snapshot must not disclose data.
        let peer = node_id(4);
        let stream_id = Ulid::from(13u128);
        let mut operation = LocationSummaryOperation::new_incoming(
            peer,
            node_id(5),
            stream_id,
            request(Some(Ulid::from_bytes([3u8; 16]))),
        )
        .with_policy(true);
        operation.first().unwrap();
        operation.next(read_result(Some(peer_realm(peer)))).unwrap();
        let mut child = enter_snapshot(&mut operation, bucket_info().to_bytes().unwrap());
        let current = RealmConfigDocument::default_for_realm(realm_id(), Vec::new());
        let current = postcard::to_allocvec(&current).unwrap();
        denied(child.next(with_row(1, Some(current.into()))));

        let turn = operation.next(snapshot_denied());
        assert!(matches!(
            asked(&turn),
            Effect::Blob(BlobEffect::SendMessage { .. })
        ));
        operation
            .next(event(BlobEvent::MessageSent { stream_id }))
            .unwrap();
        denied(operation.next(event(BlobEvent::ConnectionClosed { stream_id })));
        assert!(operation.abort().is_empty());
    }

    #[test]
    fn answers_denial() {
        // A refused peer must say so, so the caller does not read it as offline.
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let peer = node_id(4);
        let realm = peer_realm(peer);
        let mut operation = LocationSummaryOperation::new_incoming(
            peer,
            node_id(5),
            stream_id,
            request(Some(Ulid::from_bytes([3u8; 16]))),
        )
        .with_policy(true);
        operation.first().unwrap();
        operation.next(read_result(Some(realm.clone()))).unwrap();
        operation
            .next(read_result(Some(bucket_info().to_bytes().unwrap())))
            .unwrap();
        // The caller's roles are read with the policy rows, in the same snapshot.
        let mut child = operation.snapshot_child();
        let turn = child.first();
        let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
            panic!("expected the policy read, got {turn:?}")
        };
        assert_eq!(reads.len(), 5);
        let IoResult::Values(mut values) = refused_batch() else {
            unreachable!();
        };
        values[1] = Some(realm.into());
        denied(child.next(IoResult::Values(values)));

        let turn = operation.next(snapshot_denied());
        let Effect::Blob(BlobEffect::SendMessage { payload, .. }) = asked(&turn) else {
            panic!("expected an explicit denial, got {turn:?}")
        };
        assert_eq!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::LocationSummaryDenied
        );
    }

    #[test]
    fn close_rejects_stray() {
        // Only the matching close ends the answer; anything else is an error.
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let mut operation = LocationSummaryOperation::new_incoming(
            node_id(4),
            node_id(5),
            stream_id,
            request(None),
        );
        operation.state = SummaryState::Close;

        let turn = operation.next(event(BlobEvent::ConnectionClosed {
            stream_id: Ulid::from_bytes([6u8; 16]),
        }));

        assert!(matches!(turn, Err(LocationSummaryError::Unexpected { .. })));
        assert_eq!(operation.abort().len(), 1);
        assert!(operation.abort().is_empty());
    }
}
