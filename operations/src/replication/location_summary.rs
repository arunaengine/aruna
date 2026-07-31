use std::collections::BTreeSet;

use crate::blob::blob_keyspace_helper::blob_location_read;
use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_REPLICATION_JOB_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    GROUP_STORAGE_BACKEND_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
    CurrentVersionPointer, GroupStorageBackend, Permission, RealmConfigDocument, VersionKey,
    blob_object_permission_path,
};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::protocol::{
    LocationCopyStorage, LocationSummary, LocationSummaryRequest, VersionReplicationMessage,
};
use super::queue::BlobReplicationJobRecord;
use super::version_replication::{ReplicateScopeInput, ReplicateScopeTarget};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::realm_peer::ensure_realm_peer;

#[derive(Clone, Debug, Eq, PartialEq)]
enum SummaryState {
    Init,
    ReadRealm,
    ReadBucket,
    CheckPermission,
    ReadHead,
    ReadVersion,
    ReadLocation,
    ReadBackend,
    SendSummary,
    SendDenial,
    Close,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum LocationSummaryError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("peer is not a member of the realm")]
    PeerDenied,
    #[error("read access denied")]
    Denied,
    #[error("bucket not found")]
    BucketNotFound,
    #[error("unexpected event in state {state}: {event}")]
    Unexpected { state: &'static str, event: String },
    #[error("peer did not answer before the request deadline")]
    Aborted,
}

/// A local answer, plus the content hash that never goes on the wire. The hash
/// is what lets the caller ask the durable holder index which other nodes store
/// these bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct LocalSummary {
    pub summary: LocationSummary,
    pub blake3: Option<[u8; 32]>,
}

/// Answers "which copy does THIS node hold" for one version. One state machine
/// serves the local read and, with a reply stream, an inbound peer request; the
/// peer path additionally proves realm membership and READ access.
#[derive(Debug, PartialEq)]
pub struct LocationSummaryOperation {
    request: LocationSummaryRequest,
    peer: Option<NodeId>,
    local_node: Option<NodeId>,
    stream_id: Option<Ulid>,
    state: SummaryState,
    version_id: Option<Ulid>,
    blake3: Option<[u8; 32]>,
    summary: LocationSummary,
    output: Option<Result<LocalSummary, LocationSummaryError>>,
}

impl LocationSummaryOperation {
    /// Local read: the caller has already authorized the request.
    pub fn new_local(request: LocationSummaryRequest) -> Self {
        Self::build(request, None, None, None)
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
            state: SummaryState::Init,
            version_id,
            blake3: None,
            summary: LocationSummary::absent(),
            output: None,
        }
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            SummaryState::Init => "init",
            SummaryState::ReadRealm => "read_realm",
            SummaryState::ReadBucket => "read_bucket",
            SummaryState::CheckPermission => "check_permission",
            SummaryState::ReadHead => "read_head",
            SummaryState::ReadVersion => "read_version",
            SummaryState::ReadLocation => "read_location",
            SummaryState::ReadBackend => "read_backend",
            SummaryState::SendSummary => "send_summary",
            SummaryState::SendDenial => "send_denial",
            SummaryState::Close => "close",
            SummaryState::Finish => "finish",
            SummaryState::Error => "error",
        }
    }

    fn fail(&mut self, error: LocationSummaryError) -> Effects {
        self.output = Some(Err(error));
        self.state = SummaryState::Error;
        match self.stream_id {
            Some(stream_id) => smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })],
            None => smallvec![],
        }
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        let state = self.state_name();
        self.fail(LocationSummaryError::Unexpected {
            state,
            event: format!("{event:?}"),
        })
    }

    fn local_answer(&self) -> LocalSummary {
        LocalSummary {
            summary: self.summary.clone(),
            blake3: self.blake3,
        }
    }

    fn read_realm(&mut self) -> Effects {
        self.state = SummaryState::ReadRealm;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: self.request.realm_id.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn read_bucket(&mut self) -> Effects {
        self.state = SummaryState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.request.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn read_head(&mut self) -> Effects {
        let key = match BlobHeadKey::new(&self.request.bucket, &self.request.key).to_bytes() {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.state = SummaryState::ReadHead;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn read_version(&mut self) -> Effects {
        let Some(version_id) = self.version_id else {
            return self.answer();
        };
        let key =
            match VersionKey::new(&self.request.bucket, &self.request.key, version_id).to_bytes() {
                Ok(key) => key,
                Err(error) => return self.fail(error.into()),
            };
        self.state = SummaryState::ReadVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn read_location(&mut self, key: BlobLocationKey) -> Effects {
        self.state = SummaryState::ReadLocation;
        smallvec![blob_location_read(&key, None)]
    }

    fn read_backend(&mut self, backend_id: Ulid) -> Effects {
        self.state = SummaryState::ReadBackend;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
            key: backend_id.to_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn answer(&mut self) -> Effects {
        self.summary.version_id = self.version_id;
        let summary = self.summary.clone();
        let Some(stream_id) = self.stream_id else {
            self.state = SummaryState::Finish;
            self.output = Some(Ok(self.local_answer()));
            return smallvec![];
        };
        let payload = match VersionReplicationMessage::LocationSummaryResponse(summary).to_bytes() {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.state = SummaryState::SendSummary;
        smallvec![Effect::Blob(BlobEffect::SendMessage { stream_id, payload })]
    }

    fn handle_realm(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let (Some(value), Some(peer)) = (value, self.peer) else {
            return self.fail(LocationSummaryError::PeerDenied);
        };
        let document = match RealmConfigDocument::from_bytes(&value) {
            Ok(document) => document,
            Err(error) => return self.fail(error.into()),
        };
        if ensure_realm_peer(&document, peer, self.request.realm_id, true).is_err() {
            return self.fail(LocationSummaryError::PeerDenied);
        }
        self.read_bucket()
    }

    fn handle_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.fail(LocationSummaryError::BucketNotFound);
        };
        let bucket = match BucketInfo::from_bytes(&value) {
            Ok(bucket) => bucket,
            Err(error) => return self.fail(error.into()),
        };
        let Some(local_node) = self.local_node else {
            return self.fail(LocationSummaryError::PeerDenied);
        };
        let path = blob_object_permission_path(
            self.request.realm_id,
            bucket.group_id,
            local_node,
            &self.request.bucket,
            &self.request.key,
        );
        self.state = SummaryState::CheckPermission;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.request.auth_context.clone(),
                path,
                required_permission: Permission::READ,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn handle_permission(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected(event);
        };
        match allowed {
            Ok(true) => self.resolve_version(),
            _ => self.deny(),
        }
    }

    /// A refusal is answered rather than dropped: a caller must be able to tell
    /// a node that will not answer from one that cannot.
    fn deny(&mut self) -> Effects {
        let Some(stream_id) = self.stream_id else {
            return self.fail(LocationSummaryError::Denied);
        };
        let payload = match VersionReplicationMessage::LocationSummaryDenied.to_bytes() {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.output = Some(Err(LocationSummaryError::Denied));
        self.state = SummaryState::SendDenial;
        smallvec![Effect::Blob(BlobEffect::SendMessage { stream_id, payload })]
    }

    fn resolve_version(&mut self) -> Effects {
        match self.version_id {
            Some(_) => self.read_version(),
            None => self.read_head(),
        }
    }

    fn handle_head(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.answer();
        };
        match CurrentVersionPointer::from_bytes(&value) {
            Ok(pointer) => {
                self.version_id = Some(pointer.version_id);
                self.read_version()
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        // An unknown version is the caller's "not found": no copy, no id.
        let Some(value) = value else {
            self.version_id = None;
            return self.answer();
        };
        let version = match BlobVersion::from_bytes(&value) {
            Ok(version) => version,
            Err(error) => return self.fail(error.into()),
        };
        match version.location_key() {
            Some(key) => {
                self.blake3 = Some(key.blake3_hash);
                self.read_location(key)
            }
            None => self.answer(),
        }
    }

    fn handle_location(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.answer();
        };
        let location = match BackendLocation::from_bytes(&value) {
            Ok(location) => location,
            Err(error) => return self.fail(error.into()),
        };
        self.summary.held = true;
        match location.backend {
            BackendRef::Node(_) => {
                self.summary.storage = Some(LocationCopyStorage::NodeManaged {
                    storage_class: location.storage_class,
                });
                self.answer()
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

    fn handle_backend(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        if let Some(value) = value
            && let Ok(record) = GroupStorageBackend::from_bytes(&value)
            && let Some(LocationCopyStorage::GroupBackend { name, .. }) =
                self.summary.storage.as_mut()
        {
            *name = Some(record.name);
        }
        self.answer()
    }
}

impl Operation for LocationSummaryOperation {
    type Output = LocalSummary;
    type Error = LocationSummaryError;

    fn start(&mut self) -> Effects {
        match self.peer {
            Some(_) => self.read_realm(),
            None => self.resolve_version(),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            SummaryState::Init => self.start(),
            SummaryState::ReadRealm => self.handle_realm(event),
            SummaryState::ReadBucket => self.handle_bucket(event),
            SummaryState::CheckPermission => self.handle_permission(event),
            SummaryState::ReadHead => self.handle_head(event),
            SummaryState::ReadVersion => self.handle_version(event),
            SummaryState::ReadLocation => self.handle_location(event),
            SummaryState::ReadBackend => self.handle_backend(event),
            SummaryState::SendSummary => {
                let Event::Blob(BlobEvent::MessageSent { stream_id }) = event else {
                    return self.unexpected(event);
                };
                self.output = Some(Ok(self.local_answer()));
                self.state = SummaryState::Close;
                smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
            }
            SummaryState::SendDenial => {
                let Event::Blob(BlobEvent::MessageSent { stream_id }) = event else {
                    return self.unexpected(event);
                };
                self.state = SummaryState::Close;
                smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
            }
            SummaryState::Close => {
                let Event::Blob(BlobEvent::ConnectionClosed { stream_id }) = event else {
                    return self.unexpected(event);
                };
                if Some(stream_id) != self.stream_id {
                    return self.unexpected(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
                }
                self.state = SummaryState::Finish;
                smallvec![]
            }
            SummaryState::Finish | SummaryState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SummaryState::Finish | SummaryState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(LocationSummaryError::Unexpected {
            state: "finalize",
            event: "summary ended without an answer".to_string(),
        }))
    }

    fn abort(&mut self) -> Effects {
        self.state = SummaryState::Error;
        match self.stream_id {
            Some(stream_id) => smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })],
            None => smallvec![],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RemoteState {
    Init,
    Open,
    Send,
    Read,
    Close,
    Finish,
    Error,
}

/// Asks one peer whether it holds a version and on what storage. Read-only: a
/// summary never mutates the peer's state.
#[derive(Debug, PartialEq)]
pub struct RemoteLocationSummaryOperation {
    node_id: NodeId,
    request: LocationSummaryRequest,
    stream_id: Option<Ulid>,
    state: RemoteState,
    output: Option<Result<LocationSummary, LocationSummaryError>>,
}

impl RemoteLocationSummaryOperation {
    pub fn new(node_id: NodeId, request: LocationSummaryRequest) -> Self {
        Self {
            node_id,
            request,
            stream_id: None,
            state: RemoteState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: LocationSummaryError) -> Effects {
        self.output = Some(Err(error));
        self.state = RemoteState::Error;
        match self.stream_id.take() {
            Some(stream_id) => smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })],
            None => smallvec![],
        }
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        self.fail(LocationSummaryError::Unexpected {
            state: "remote",
            event: format!("{event:?}"),
        })
    }
}

impl Operation for RemoteLocationSummaryOperation {
    type Output = LocationSummary;
    type Error = LocationSummaryError;

    fn start(&mut self) -> Effects {
        self.state = RemoteState::Open;
        smallvec![Effect::Blob(BlobEffect::OpenConnection {
            node_id: self.node_id,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Blob(BlobEvent::Error(error)) => return self.fail(error.into()),
            event => event,
        };
        match self.state {
            RemoteState::Init => self.start(),
            RemoteState::Open => {
                let Event::Blob(BlobEvent::ConnectionEstablished { stream_id }) = event else {
                    return self.unexpected(event);
                };
                self.stream_id = Some(stream_id);
                let payload =
                    match VersionReplicationMessage::LocationSummaryRequest(self.request.clone())
                        .to_bytes()
                    {
                        Ok(payload) => payload,
                        Err(error) => return self.fail(error.into()),
                    };
                self.state = RemoteState::Send;
                smallvec![Effect::Blob(BlobEffect::SendMessage { stream_id, payload })]
            }
            RemoteState::Send => {
                let Event::Blob(BlobEvent::MessageSent { stream_id }) = event else {
                    return self.unexpected(event);
                };
                self.state = RemoteState::Read;
                smallvec![Effect::Blob(BlobEffect::ReadMessage { stream_id })]
            }
            RemoteState::Read => {
                let Event::Blob(BlobEvent::MessageReceived { stream_id, payload }) = event else {
                    return self.unexpected(event);
                };
                match VersionReplicationMessage::from_bytes(&payload) {
                    Ok(VersionReplicationMessage::LocationSummaryResponse(summary)) => {
                        self.output = Some(Ok(summary));
                        self.state = RemoteState::Close;
                        smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
                    }
                    Ok(VersionReplicationMessage::LocationSummaryDenied) => {
                        self.output = Some(Err(LocationSummaryError::Denied));
                        self.state = RemoteState::Close;
                        smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
                    }
                    Ok(_) => self.fail(LocationSummaryError::Unexpected {
                        state: "remote_read",
                        event: "unexpected location summary response".to_string(),
                    }),
                    Err(error) => self.fail(error.into()),
                }
            }
            RemoteState::Close => {
                let Event::Blob(BlobEvent::ConnectionClosed { stream_id }) = event else {
                    return self.unexpected(event);
                };
                if Some(stream_id) != self.stream_id {
                    return self.unexpected(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
                }
                self.state = RemoteState::Finish;
                smallvec![]
            }
            RemoteState::Finish | RemoteState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RemoteState::Finish | RemoteState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(LocationSummaryError::Unexpected {
            state: "finalize",
            event: "remote summary ended without an answer".to_string(),
        }))
    }

    fn abort(&mut self) -> Effects {
        self.state = RemoteState::Error;
        self.output
            .get_or_insert(Err(LocationSummaryError::Aborted));
        match self.stream_id.take() {
            Some(stream_id) => smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })],
            None => smallvec![],
        }
    }
}

const QUEUED_JOB_PAGE_SIZE: usize = 256;
const QUEUED_JOB_MAX_PAGES: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
enum QueuedState {
    Init,
    Scan,
    Finish,
    Error,
}

/// Nodes with a queued replication job, plus what the scan could not see: a
/// page cap reached before the keyspace ended, and records that would not
/// decode. Either one means a queued copy may be missing from `nodes`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueuedReplicas {
    pub nodes: BTreeSet<NodeId>,
    pub truncated: bool,
    pub skipped: usize,
}

/// Collects nodes with a queued replication job for one version. They are the
/// copies a caller must see as `pending`: no location record for them exists
/// anywhere yet, so nothing else would report them.
#[derive(Debug, PartialEq)]
pub struct QueuedReplicaNodesOperation {
    bucket: String,
    key: String,
    version_id: Ulid,
    pages: usize,
    found: QueuedReplicas,
    state: QueuedState,
    output: Option<Result<QueuedReplicas, LocationSummaryError>>,
}

impl QueuedReplicaNodesOperation {
    pub fn new(bucket: String, key: String, version_id: Ulid) -> Self {
        Self {
            bucket,
            key,
            version_id,
            pages: 0,
            found: QueuedReplicas::default(),
            state: QueuedState::Init,
            output: None,
        }
    }

    fn scan(&mut self, start_after: Option<Key>) -> Effects {
        self.pages = self.pages.saturating_add(1);
        self.state = QueuedState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: QUEUED_JOB_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn covers(&self, input: &ReplicateScopeInput) -> bool {
        if input.bucket != self.bucket {
            return false;
        }
        match &input.target {
            ReplicateScopeTarget::Bucket => true,
            ReplicateScopeTarget::Prefix(prefix) => self.key.starts_with(prefix),
            ReplicateScopeTarget::Object { key } => key == &self.key,
            ReplicateScopeTarget::Version { key, version_id } => {
                key == &self.key && *version_id == self.version_id
            }
        }
    }

    fn finish(&mut self, truncated: bool) -> Effects {
        self.state = QueuedState::Finish;
        self.found.truncated = truncated;
        self.output = Some(Ok(std::mem::take(&mut self.found)));
        smallvec![]
    }
}

impl Operation for QueuedReplicaNodesOperation {
    type Output = QueuedReplicas;
    type Error = LocationSummaryError;

    fn start(&mut self) -> Effects {
        self.scan(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueuedState::Init => self.start(),
            QueuedState::Scan => {
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    self.state = QueuedState::Error;
                    self.output = Some(Err(LocationSummaryError::Unexpected {
                        state: "queued_scan",
                        event: format!("{event:?}"),
                    }));
                    return smallvec![];
                };
                for (_, value) in values {
                    let Ok(record) = BlobReplicationJobRecord::from_bytes(value.as_ref()) else {
                        self.found.skipped = self.found.skipped.saturating_add(1);
                        continue;
                    };
                    if self.covers(&record.input) {
                        self.found.nodes.insert(record.input.target_node_id);
                    }
                }
                match next_start_after {
                    Some(start) if self.pages < QUEUED_JOB_MAX_PAGES => self.scan(Some(start)),
                    Some(_) => self.finish(true),
                    None => self.finish(false),
                }
            }
            QueuedState::Finish | QueuedState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, QueuedState::Finish | QueuedState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(LocationSummaryError::Unexpected {
            state: "finalize",
            event: "queued replica scan ended without an answer".to_string(),
        }))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{LocationSummaryOperation, QueuedReplicaNodesOperation};
    use crate::replication::protocol::{LocationCopyStorage, LocationSummaryRequest};
    use crate::replication::queue::BlobReplicationJobRecord;
    use crate::replication::version_replication::{ReplicateScopeInput, ReplicateScopeTarget};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{AuthContext, BackendLocation, BackendRef, BlobVersion, RealmId};
    use aruna_core::types::{NodeId, UserId};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn realm_id() -> RealmId {
        RealmId::from_bytes([1u8; 32])
    }

    fn node_id(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn auth() -> AuthContext {
        AuthContext {
            user_id: UserId::nil(realm_id()),
            realm_id: realm_id(),
            path_restrictions: None,
        }
    }

    fn request(version_id: Option<Ulid>) -> LocationSummaryRequest {
        LocationSummaryRequest {
            realm_id: realm_id(),
            bucket: "raw".to_string(),
            key: "run1.tar".to_string(),
            version_id,
            auth_context: auth(),
        }
    }

    fn location(backend: BackendRef, storage_class: Option<String>) -> BackendLocation {
        BackendLocation {
            backend,
            storage_class,
            root: "root".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "raw/run1.tar_0".to_string(),
            ulid: Ulid::from_bytes([2u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: UserId::nil(realm_id()),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: HashMap::new(),
        }
    }

    fn read_result(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: b"k".to_vec().into(),
            value: value.map(Into::into),
        })
    }

    fn materialized() -> BlobVersion {
        BlobVersion::materialized(
            [7u8; 32],
            BackendRef::node_default(),
            SystemTime::UNIX_EPOCH,
            UserId::nil(realm_id()),
            None,
        )
    }

    #[test]
    fn reports_copy_class() {
        let version_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = LocationSummaryOperation::new_local(request(Some(version_id)));
        operation.start();
        operation.step(read_result(Some(materialized().to_bytes().unwrap())));
        operation.step(read_result(Some(
            location(BackendRef::node_default(), Some("cold".to_string()))
                .to_bytes()
                .unwrap(),
        )));

        let local = operation.finalize().unwrap();
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
    fn names_group_backend() {
        // Tenant-owned storage is the durability signal, so id and name ship.
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let mut operation =
            LocationSummaryOperation::new_local(request(Some(Ulid::from_bytes([3u8; 16]))));
        operation.start();
        operation.step(read_result(Some(materialized().to_bytes().unwrap())));
        operation.step(read_result(Some(
            location(BackendRef::Group(backend_id), None)
                .to_bytes()
                .unwrap(),
        )));
        operation.step(read_result(None));

        let summary = operation.finalize().unwrap().summary;
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
        let mut operation =
            LocationSummaryOperation::new_local(request(Some(Ulid::from_bytes([3u8; 16]))));
        operation.start();
        operation.step(read_result(None));

        let local = operation.finalize().unwrap();
        assert!(local.blake3.is_none());
        assert!(!local.summary.held);
        assert_eq!(local.summary.version_id, None);
    }

    #[test]
    fn answers_on_stream() {
        // The peer path must never reply before the permission check passed.
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let mut operation = LocationSummaryOperation::new_incoming(
            node_id(4),
            node_id(5),
            stream_id,
            request(Some(Ulid::from_bytes([3u8; 16]))),
        );
        let effects = operation.start();

        let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
            panic!("expected a realm read first, got {effects:?}")
        };
        assert_eq!(key_space, aruna_core::keyspaces::REALM_CONFIG_KEYSPACE);
    }

    #[test]
    fn answers_denial() {
        // A refused peer must say so, so the caller does not read it as offline.
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let mut operation = LocationSummaryOperation::new_incoming(
            node_id(4),
            node_id(5),
            stream_id,
            request(Some(Ulid::from_bytes([3u8; 16]))),
        );
        operation.state = super::SummaryState::CheckPermission;

        let effects = operation.step(Event::SubOperation(
            aruna_core::events::SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));

        let [Effect::Blob(aruna_core::effects::BlobEffect::SendMessage { payload, .. })] =
            effects.as_slice()
        else {
            panic!("expected an explicit denial, got {effects:?}")
        };
        assert_eq!(
            crate::replication::protocol::VersionReplicationMessage::from_bytes(payload).unwrap(),
            crate::replication::protocol::VersionReplicationMessage::LocationSummaryDenied
        );
    }

    #[test]
    fn reports_remote_denial() {
        // The asking side turns the denial back into a Denied error, never a
        // transport failure.
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let mut operation = super::RemoteLocationSummaryOperation::new(node_id(4), request(None));
        operation.start();
        operation.step(Event::Blob(
            aruna_core::events::BlobEvent::ConnectionEstablished { stream_id },
        ));
        operation.step(Event::Blob(aruna_core::events::BlobEvent::MessageSent {
            stream_id,
        }));

        operation.step(Event::Blob(
            aruna_core::events::BlobEvent::MessageReceived {
                stream_id,
                payload:
                    crate::replication::protocol::VersionReplicationMessage::LocationSummaryDenied
                        .to_bytes()
                        .unwrap(),
            },
        ));

        assert_eq!(
            operation.finalize(),
            Err(super::LocationSummaryError::Denied)
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
        operation.state = super::SummaryState::Close;

        operation.step(Event::Blob(
            aruna_core::events::BlobEvent::ConnectionClosed {
                stream_id: Ulid::from_bytes([6u8; 16]),
            },
        ));

        assert_eq!(operation.state, super::SummaryState::Error);
    }

    #[test]
    fn remote_close_rejects() {
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let mut operation = super::RemoteLocationSummaryOperation::new(node_id(4), request(None));
        operation.start();
        operation.step(Event::Blob(
            aruna_core::events::BlobEvent::ConnectionEstablished { stream_id },
        ));
        operation.state = super::RemoteState::Close;

        operation.step(Event::Blob(aruna_core::events::BlobEvent::MessageSent {
            stream_id,
        }));

        assert_eq!(operation.state, super::RemoteState::Error);
    }

    #[test]
    fn abort_closes_stream() {
        // A deadline must release the stream; only CloseConnection unregisters it.
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let mut operation = super::RemoteLocationSummaryOperation::new(node_id(4), request(None));
        operation.start();
        operation.step(Event::Blob(
            aruna_core::events::BlobEvent::ConnectionEstablished { stream_id },
        ));

        let effects = operation.abort();

        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(
                aruna_core::effects::BlobEffect::CloseConnection { stream_id }
            )]
        );
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(super::LocationSummaryError::Aborted)
        );
    }

    fn job(target: ReplicateScopeTarget, target_node: NodeId) -> BlobReplicationJobRecord {
        BlobReplicationJobRecord::new(
            ReplicateScopeInput {
                bucket: "raw".to_string(),
                target,
                target_node_id: target_node,
                auth_context: auth(),
                replicate_delete_markers: true,
                mode: crate::replication::protocol::ReplicationMode::OnDemand,
            },
            None,
            0,
        )
    }

    #[test]
    fn names_queued_nodes() {
        let version_id = Ulid::from_bytes([3u8; 16]);
        let wanted = node_id(6);
        let mut operation =
            QueuedReplicaNodesOperation::new("raw".to_string(), "run1.tar".to_string(), version_id);
        operation.start();

        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                (
                    b"a".to_vec().into(),
                    job(ReplicateScopeTarget::Bucket, wanted)
                        .to_bytes()
                        .unwrap()
                        .into(),
                ),
                (
                    b"b".to_vec().into(),
                    job(
                        ReplicateScopeTarget::Object {
                            key: "other".to_string(),
                        },
                        wanted,
                    )
                    .to_bytes()
                    .unwrap()
                    .into(),
                ),
            ],
            next_start_after: None,
        }));

        let queued = operation.finalize().unwrap();
        assert_eq!(queued.nodes.len(), 1);
        assert!(queued.nodes.contains(&wanted));
        assert!(!queued.truncated);
    }

    #[test]
    fn counts_skipped_records() {
        // A record that will not decode may name a node; the scan is then not
        // an exhaustive answer and must say so.
        let mut operation = QueuedReplicaNodesOperation::new(
            "raw".to_string(),
            "run1.tar".to_string(),
            Ulid::from_bytes([3u8; 16]),
        );
        operation.start();

        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(b"a".to_vec().into(), vec![0xffu8; 8].into())],
            next_start_after: None,
        }));

        let queued = operation.finalize().unwrap();
        assert_eq!(queued.skipped, 1);
        assert!(queued.nodes.is_empty());
    }

    #[test]
    fn signals_truncated_scan() {
        // A capped scan must not look like an exhausted one: a queued copy past
        // the cap is otherwise indistinguishable from absent.
        let wanted = node_id(6);
        let mut operation = QueuedReplicaNodesOperation::new(
            "raw".to_string(),
            "run1.tar".to_string(),
            Ulid::from_bytes([3u8; 16]),
        );
        operation.start();

        for _ in 0..super::QUEUED_JOB_MAX_PAGES {
            operation.step(Event::Storage(StorageEvent::IterResult {
                values: vec![(
                    b"a".to_vec().into(),
                    job(ReplicateScopeTarget::Bucket, wanted)
                        .to_bytes()
                        .unwrap()
                        .into(),
                )],
                next_start_after: Some(b"a".to_vec().into()),
            }));
        }

        let queued = operation.finalize().unwrap();
        assert!(queued.truncated);
        assert!(queued.nodes.contains(&wanted));
    }
}
