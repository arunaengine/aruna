use super::LocationSummaryError;
use crate::blob::blob_keyspace_helper::blob_location_read;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::realm_peer::ensure_realm_peer;
use crate::replication::protocol::{
    LocationCopyStorage, LocationSummary, LocationSummaryRequest, VersionReplicationMessage,
};
use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, GROUP_STORAGE_BACKEND_KEYSPACE,
    REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
    CurrentVersionPointer, GroupStorageBackend, Permission, RealmConfigDocument, VersionKey,
    blob_object_permission_path,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use ulid::Ulid;

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

/// Answers "which copy does THIS node hold" for one version. One state machine
/// serves the local read and, with a reply stream, an inbound peer request; the
/// peer path additionally proves realm membership and READ access.
#[derive(Debug, PartialEq)]
pub struct LocationSummaryOperation {
    request: LocationSummaryRequest,
    peer: Option<NodeId>,
    local_node: Option<NodeId>,
    stream_id: Option<Ulid>,
    bucket: Option<BucketInfo>,
    state: SummaryState,
    version_id: Option<Ulid>,
    blake3: Option<[u8; 32]>,
    delete_marker: bool,
    summary: LocationSummary,
    output: Option<Result<LocalSummary, LocationSummaryError>>,
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
            bucket: None,
            state: SummaryState::Init,
            version_id,
            blake3: None,
            delete_marker: false,
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
            bucket: self.bucket.clone(),
            delete_marker: self.delete_marker,
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
        let group_id = bucket.group_id;
        self.bucket = Some(bucket);
        let path = blob_object_permission_path(
            self.request.realm_id,
            group_id,
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
        self.delete_marker = version.is_deleted();
        match version.location_key() {
            Some(key) => {
                self.blake3 = Some(key.blake3_hash);
                self.summary.materialized = true;
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
            None => self.read_bucket(),
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

#[cfg(test)]
mod tests {
    use super::{LocationSummaryError, LocationSummaryOperation};
    use crate::replication::location_summary::fixtures::{node_id, realm_id, request};
    use crate::replication::protocol::LocationCopyStorage;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{BackendLocation, BackendRef, BlobVersion, BucketInfo};
    use aruna_core::types::UserId;
    use std::collections::HashMap;
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

    fn bucket_info() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_bytes([8u8; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(realm_id()),
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
        }
    }

    /// Answers the bucket read and the READ check every local answer starts with.
    fn authorized(version_id: Option<Ulid>) -> LocationSummaryOperation {
        let mut operation = LocationSummaryOperation::new_local(node_id(5), request(version_id));
        operation.start();
        operation.step(read_result(Some(bucket_info().to_bytes().unwrap())));
        operation.step(Event::SubOperation(
            aruna_core::events::SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        operation
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
        let mut operation = authorized(Some(version_id));
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
        let mut operation = authorized(Some(Ulid::from_bytes([3u8; 16])));
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
        let mut operation = authorized(Some(Ulid::from_bytes([3u8; 16])));
        operation.step(read_result(None));

        let local = operation.finalize().unwrap();
        assert!(local.blake3.is_none());
        assert!(!local.summary.held);
        assert_eq!(local.summary.version_id, None);
    }

    #[test]
    fn carries_bucket_record() {
        // The caller must fan out from the record the answer was authorized on.
        let mut operation = authorized(Some(Ulid::from_bytes([3u8; 16])));
        operation.step(read_result(None));

        assert_eq!(operation.finalize().unwrap().bucket, Some(bucket_info()));
    }

    #[test]
    fn local_refuses_denied() {
        // Without a stream there is no one to answer, so the read just fails.
        let mut operation = LocationSummaryOperation::new_local(
            node_id(5),
            request(Some(Ulid::from_bytes([3u8; 16]))),
        );
        operation.start();
        operation.step(read_result(Some(bucket_info().to_bytes().unwrap())));

        operation.step(Event::SubOperation(
            aruna_core::events::SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));

        assert_eq!(operation.finalize(), Err(LocationSummaryError::Denied));
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
}
