use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    BackendLocation, BlobVersion, BucketInfo, HashPathIndexKey, Permission, RealmConfigDocument,
    RealmId, VersionKey, VersionedObjectArn, blob_object_permission_path,
};
use aruna_core::types::Effects;
use bytes::Bytes;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget, VersionReplicationMessage};
use crate::blob::blob_keyspace_helper::iter_hash_path_index_effect;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::realm_peer::ensure_realm_peer;

#[derive(Debug, PartialEq)]
pub enum BaoReadOutput {
    Metadata {
        size: u64,
        blake3: [u8; 32],
    },
    Stream {
        blob: BackendStream<Result<Bytes, StreamError>>,
        size: u64,
        blake3: [u8; 32],
    },
}

#[derive(Debug, Error, PartialEq)]
pub enum BaoReadError {
    #[error("bao read was refused: {0:?}")]
    Refused(BaoReadRefusal),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in state {state}: {event}")]
    Unexpected { state: &'static str, event: String },
    #[error("bao read did not finish")]
    NotFinished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BaoReadState {
    Init,
    Open,
    Send,
    ReadResponse,
    Receive,
    CloseMetadata,
    Close,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct BaoReadOperation {
    node_id: NodeId,
    request: BaoReadRequest,
    stream_id: Option<Ulid>,
    state: BaoReadState,
    output: Option<Result<BaoReadOutput, BaoReadError>>,
    close_error: Option<BaoReadError>,
    accepted_blake3: Option<[u8; 32]>,
}

impl BaoReadOperation {
    pub fn new(node_id: NodeId, request: BaoReadRequest) -> Self {
        Self {
            node_id,
            request,
            stream_id: None,
            state: BaoReadState::Init,
            output: None,
            close_error: None,
            accepted_blake3: None,
        }
    }

    fn fail(&mut self, error: BaoReadError) -> Effects {
        let Some(stream_id) = self.stream_id else {
            self.state = BaoReadState::Error;
            self.output = Some(Err(error));
            return smallvec![];
        };
        self.state = BaoReadState::Close;
        self.close_error = Some(error);
        smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        self.fail(BaoReadError::Unexpected {
            state: self.state_name(),
            event: format!("{event:?}"),
        })
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            BaoReadState::Init => "init",
            BaoReadState::Open => "open",
            BaoReadState::Send => "send",
            BaoReadState::ReadResponse => "read_response",
            BaoReadState::Receive => "receive",
            BaoReadState::CloseMetadata => "close_metadata",
            BaoReadState::Close => "close",
            BaoReadState::Finish => "finish",
            BaoReadState::Error => "error",
        }
    }
}

impl Operation for BaoReadOperation {
    type Output = BaoReadOutput;
    type Error = BaoReadError;

    fn start(&mut self) -> Effects {
        self.state = BaoReadState::Open;
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
            BaoReadState::Open => {
                let Event::Blob(BlobEvent::ConnectionEstablished { stream_id }) = event else {
                    return self.unexpected(event);
                };
                self.stream_id = Some(stream_id);
                self.state = BaoReadState::Send;
                let payload = match VersionReplicationMessage::BaoReadRequest(self.request.clone())
                    .to_bytes()
                {
                    Ok(payload) => payload,
                    Err(error) => return self.fail(error.into()),
                };
                smallvec![Effect::Blob(BlobEffect::SendMessage { stream_id, payload })]
            }
            BaoReadState::Send => {
                let Event::Blob(BlobEvent::MessageSent { stream_id }) = event else {
                    return self.unexpected(event);
                };
                self.state = BaoReadState::ReadResponse;
                smallvec![Effect::Blob(BlobEffect::ReadMessage { stream_id })]
            }
            BaoReadState::ReadResponse => {
                let Event::Blob(BlobEvent::MessageReceived { stream_id, payload }) = event else {
                    return self.unexpected(event);
                };
                match VersionReplicationMessage::from_bytes(&payload) {
                    Ok(VersionReplicationMessage::BaoReadAccepted { size, blake3 }) => {
                        if self
                            .request
                            .expected_blake3
                            .is_some_and(|expected| expected != blake3)
                        {
                            return self.fail(BaoReadError::Refused(BaoReadRefusal::HashMismatch));
                        }
                        if self.request.metadata_only {
                            self.output = Some(Ok(BaoReadOutput::Metadata { size, blake3 }));
                            self.state = BaoReadState::CloseMetadata;
                            return smallvec![Effect::Blob(BlobEffect::CloseConnection {
                                stream_id,
                            })];
                        }
                        self.accepted_blake3 = Some(blake3);
                        self.state = BaoReadState::Receive;
                        smallvec![Effect::Blob(BlobEffect::ReceiveRead {
                            stream_id,
                            size,
                            expected_blake3: blake3,
                        })]
                    }
                    Ok(VersionReplicationMessage::BaoReadRefused(reason)) => {
                        self.fail(BaoReadError::Refused(reason))
                    }
                    Ok(_) => self.fail(BaoReadError::Unexpected {
                        state: self.state_name(),
                        event: "unexpected bao read response".to_string(),
                    }),
                    Err(error) => self.fail(error.into()),
                }
            }
            BaoReadState::Receive => {
                let Event::Blob(BlobEvent::ReadFinished { blob, stream_size }) = event else {
                    return self.unexpected(event);
                };
                let Some(blake3) = self.accepted_blake3.take() else {
                    return self.fail(BaoReadError::NotFinished);
                };
                self.output = Some(Ok(BaoReadOutput::Stream {
                    blob,
                    size: stream_size,
                    blake3,
                }));
                self.state = BaoReadState::Finish;
                smallvec![]
            }
            BaoReadState::CloseMetadata => {
                let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
                    return self.unexpected(event);
                };
                self.state = BaoReadState::Finish;
                smallvec![]
            }
            BaoReadState::Close => {
                let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
                    return self.unexpected(event);
                };
                self.output = Some(Err(self
                    .close_error
                    .take()
                    .unwrap_or(BaoReadError::NotFinished)));
                self.state = BaoReadState::Error;
                smallvec![]
            }
            BaoReadState::Init => self.unexpected(event),
            BaoReadState::Finish | BaoReadState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, BaoReadState::Finish | BaoReadState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(BaoReadError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.stream_id {
            Some(stream_id) => {
                self.state = BaoReadState::Close;
                self.close_error = Some(BaoReadError::NotFinished);
                smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
            }
            None => {
                self.state = BaoReadState::Error;
                self.output = Some(Err(BaoReadError::NotFinished));
                smallvec![]
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncomingBaoReadResult {
    Served,
    Probed,
    Refused(BaoReadRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncomingBaoReadState {
    Init,
    ReadRealm,
    ReadExactVersion,
    ReadExactBucket,
    ReadHashAliases,
    ReadHashVersion,
    CheckPermission,
    ReadLocation,
    SendAccepted,
    ServeRead,
    CloseMetadata,
    SendRefusal,
    CloseRefusal,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct IncomingBaoReadOperation {
    peer: NodeId,
    local_node: NodeId,
    local_realm: RealmId,
    stream_id: Ulid,
    request: BaoReadRequest,
    state: IncomingBaoReadState,
    candidates: VecDeque<HashPathIndexKey>,
    candidate: Option<HashPathIndexKey>,
    blob_hash: Option<[u8; 32]>,
    location: Option<BackendLocation>,
    had_denial: bool,
    refusal: Option<BaoReadRefusal>,
    output: Option<Result<IncomingBaoReadResult, BaoReadError>>,
}

impl IncomingBaoReadOperation {
    pub fn new(
        peer: NodeId,
        local_node: NodeId,
        local_realm: RealmId,
        stream_id: Ulid,
        request: BaoReadRequest,
    ) -> Self {
        Self {
            peer,
            local_node,
            local_realm,
            stream_id,
            request,
            state: IncomingBaoReadState::Init,
            candidates: VecDeque::new(),
            candidate: None,
            blob_hash: None,
            location: None,
            had_denial: false,
            refusal: None,
            output: None,
        }
    }

    fn exact_target(&self) -> Option<&VersionedObjectArn> {
        match &self.request.target {
            BaoReadTarget::ExactVersion(target) => Some(target),
            BaoReadTarget::Blake3(_) => None,
        }
    }

    fn hash_target(&self) -> Option<[u8; 32]> {
        match &self.request.target {
            BaoReadTarget::Blake3(hash) => Some(*hash),
            BaoReadTarget::ExactVersion(_) => None,
        }
    }

    fn read_realm(&mut self) -> Effects {
        self.state = IncomingBaoReadState::ReadRealm;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(self.request.realm_id.as_bytes().to_vec()),
            txn_id: None,
        })]
    }

    fn read_exact_version(&mut self) -> Effects {
        let target = self
            .exact_target()
            .expect("exact target required in exact-version state");
        let key = match VersionKey::new(&target.bucket, &target.key, target.version).to_bytes() {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingBaoReadState::ReadExactVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn read_exact_bucket(&mut self) -> Effects {
        let bucket = self
            .exact_target()
            .expect("exact target required while reading bucket")
            .bucket
            .clone();
        self.state = IncomingBaoReadState::ReadExactBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: bucket.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn read_hash_aliases(&mut self) -> Effects {
        let Some(hash) = self.hash_target() else {
            return self.send_refusal(BaoReadRefusal::InvalidTarget);
        };
        self.state = IncomingBaoReadState::ReadHashAliases;
        match iter_hash_path_index_effect(&hash, None) {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(error.into()),
        }
    }

    fn next_candidate(&mut self) -> Effects {
        let Some(candidate) = self.candidates.pop_front() else {
            return self.send_refusal(if self.had_denial {
                BaoReadRefusal::ReadDenied
            } else {
                BaoReadRefusal::NotFound
            });
        };
        let version_key = VersionKey::new(&candidate.bucket, &candidate.key, candidate.version_id);
        let key = match version_key.to_bytes() {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.candidate = Some(candidate);
        self.state = IncomingBaoReadState::ReadHashVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn check_permission(&mut self, path: String) -> Effects {
        self.state = IncomingBaoReadState::CheckPermission;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.request.auth_context.clone(),
                path,
                required_permission: Permission::READ,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn read_location(&mut self) -> Effects {
        let Some(blob_hash) = self.blob_hash else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        self.state = IncomingBaoReadState::ReadLocation;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            key: blob_hash.to_vec().into(),
            txn_id: None,
        })]
    }

    fn send_accepted(&mut self, location: BackendLocation) -> Effects {
        let Some(blake3) = self.blob_hash else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let payload = match (VersionReplicationMessage::BaoReadAccepted {
            size: location.blob_size,
            blake3,
        })
        .to_bytes()
        {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.location = Some(location);
        self.state = IncomingBaoReadState::SendAccepted;
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn send_refusal(&mut self, refusal: BaoReadRefusal) -> Effects {
        let payload = match VersionReplicationMessage::BaoReadRefused(refusal).to_bytes() {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.refusal = Some(refusal);
        self.state = IncomingBaoReadState::SendRefusal;
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn fail(&mut self, error: BaoReadError) -> Effects {
        self.state = IncomingBaoReadState::Error;
        self.output = Some(Err(error));
        smallvec![Effect::Blob(BlobEffect::CloseConnection {
            stream_id: self.stream_id,
        })]
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        self.fail(BaoReadError::Unexpected {
            state: self.state_name(),
            event: format!("{event:?}"),
        })
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            IncomingBaoReadState::Init => "init",
            IncomingBaoReadState::ReadRealm => "read_realm",
            IncomingBaoReadState::ReadExactVersion => "read_exact_version",
            IncomingBaoReadState::ReadExactBucket => "read_exact_bucket",
            IncomingBaoReadState::ReadHashAliases => "read_hash_aliases",
            IncomingBaoReadState::ReadHashVersion => "read_hash_version",
            IncomingBaoReadState::CheckPermission => "check_permission",
            IncomingBaoReadState::ReadLocation => "read_location",
            IncomingBaoReadState::SendAccepted => "send_accepted",
            IncomingBaoReadState::ServeRead => "serve_read",
            IncomingBaoReadState::CloseMetadata => "close_metadata",
            IncomingBaoReadState::SendRefusal => "send_refusal",
            IncomingBaoReadState::CloseRefusal => "close_refusal",
            IncomingBaoReadState::Finish => "finish",
            IncomingBaoReadState::Error => "error",
        }
    }

    fn handle_realm(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.send_refusal(BaoReadRefusal::RealmPeerDenied);
        };
        let document = match RealmConfigDocument::from_bytes(&value) {
            Ok(document) => document,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        if ensure_realm_peer(&document, self.peer, self.request.realm_id, true).is_err() {
            return self.send_refusal(BaoReadRefusal::RealmPeerDenied);
        }
        match &self.request.target {
            BaoReadTarget::ExactVersion(target) => {
                if target.realm_id != self.request.realm_id || target.node_id != self.local_node {
                    self.send_refusal(BaoReadRefusal::InvalidTarget)
                } else {
                    self.read_exact_bucket()
                }
            }
            BaoReadTarget::Blake3(hash) => {
                if self
                    .request
                    .expected_blake3
                    .is_some_and(|expected| expected != *hash)
                {
                    self.send_refusal(BaoReadRefusal::HashMismatch)
                } else {
                    self.read_hash_aliases()
                }
            }
        }
    }

    fn handle_exact_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let version = match BlobVersion::from_bytes(&value) {
            Ok(version) => version,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        let Some(blob_hash) = version.blob_hash().copied() else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        if self
            .request
            .expected_blake3
            .is_some_and(|expected| expected != blob_hash)
        {
            return self.send_refusal(BaoReadRefusal::HashMismatch);
        }
        self.blob_hash = Some(blob_hash);
        self.read_location()
    }

    fn handle_exact_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let bucket = match BucketInfo::from_bytes(&value) {
            Ok(bucket) => bucket,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        let target = self
            .exact_target()
            .expect("exact target required after exact bucket read");
        self.check_permission(blob_object_permission_path(
            self.request.realm_id,
            bucket.group_id,
            self.local_node,
            &target.bucket,
            &target.key,
        ))
    }

    fn handle_hash_aliases(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(hash) = self.hash_target() else {
            return self.send_refusal(BaoReadRefusal::InvalidTarget);
        };
        let mut candidates = Vec::with_capacity(values.len());
        for (key, _) in values {
            let candidate = match HashPathIndexKey::from_bytes(key.as_ref()) {
                Ok(candidate) => candidate,
                Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
            };
            if candidate.realm_id == self.request.realm_id
                && candidate.node_id == self.local_node
                && candidate.blake3_hash == hash
            {
                candidates.push(candidate);
            }
        }
        candidates
            .sort_by_cached_key(|candidate| (candidate.permission_path(), candidate.version_id));
        self.candidates = candidates.into();
        self.next_candidate()
    }

    fn handle_hash_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.next_candidate();
        };
        let version = match BlobVersion::from_bytes(&value) {
            Ok(version) => version,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        let Some(hash) = self.hash_target() else {
            return self.send_refusal(BaoReadRefusal::InvalidTarget);
        };
        if version.blob_hash() != Some(&hash) {
            return self.next_candidate();
        }
        self.blob_hash = Some(hash);
        let path = self
            .candidate
            .as_ref()
            .expect("hash candidate required after version read")
            .permission_path();
        self.check_permission(path)
    }

    fn handle_permission(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected(event);
        };
        match allowed {
            Ok(true) => {
                if matches!(&self.request.target, BaoReadTarget::ExactVersion(_)) {
                    self.read_exact_version()
                } else {
                    self.read_location()
                }
            }
            Ok(false) => {
                self.had_denial = true;
                if matches!(&self.request.target, BaoReadTarget::Blake3(_)) {
                    self.next_candidate()
                } else {
                    self.send_refusal(BaoReadRefusal::ReadDenied)
                }
            }
            Err(_) => self.send_refusal(BaoReadRefusal::BackendFailure),
        }
    }

    fn handle_location(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return if matches!(&self.request.target, BaoReadTarget::Blake3(_)) {
                self.next_candidate()
            } else {
                self.send_refusal(BaoReadRefusal::NotFound)
            };
        };
        let location = match BackendLocation::from_bytes(&value) {
            Ok(location) => location,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        let Some(blake3) = self.blob_hash else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        if location.get_blake3() != Some(blake3.as_slice()) {
            return self.send_refusal(BaoReadRefusal::HashMismatch);
        }
        self.send_accepted(location)
    }
}

impl Operation for IncomingBaoReadOperation {
    type Output = IncomingBaoReadResult;
    type Error = BaoReadError;

    fn start(&mut self) -> Effects {
        if self.request.realm_id != self.local_realm
            || self.request.auth_context.realm_id != self.request.realm_id
        {
            return self.send_refusal(BaoReadRefusal::RealmPeerDenied);
        }
        self.read_realm()
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { .. }) => {
                return self.send_refusal(BaoReadRefusal::BackendFailure);
            }
            Event::Blob(BlobEvent::Error(error)) => return self.fail(error.into()),
            event => event,
        };

        match self.state {
            IncomingBaoReadState::ReadRealm => self.handle_realm(event),
            IncomingBaoReadState::ReadExactVersion => self.handle_exact_version(event),
            IncomingBaoReadState::ReadExactBucket => self.handle_exact_bucket(event),
            IncomingBaoReadState::ReadHashAliases => self.handle_hash_aliases(event),
            IncomingBaoReadState::ReadHashVersion => self.handle_hash_version(event),
            IncomingBaoReadState::CheckPermission => self.handle_permission(event),
            IncomingBaoReadState::ReadLocation => self.handle_location(event),
            IncomingBaoReadState::SendAccepted => {
                let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
                    return self.unexpected(event);
                };
                if self.request.metadata_only {
                    self.state = IncomingBaoReadState::CloseMetadata;
                    return smallvec![Effect::Blob(BlobEffect::CloseConnection {
                        stream_id: self.stream_id,
                    })];
                }
                let Some(location) = self.location.clone() else {
                    return self.fail(BaoReadError::NotFinished);
                };
                let Some(expected_blake3) = self.blob_hash else {
                    return self.fail(BaoReadError::NotFinished);
                };
                self.state = IncomingBaoReadState::ServeRead;
                smallvec![Effect::Blob(BlobEffect::ServeRead {
                    stream_id: self.stream_id,
                    location,
                    expected_blake3,
                })]
            }
            IncomingBaoReadState::ServeRead => {
                let Event::Blob(BlobEvent::ReadServed { .. }) = event else {
                    return self.unexpected(event);
                };
                self.state = IncomingBaoReadState::Finish;
                self.output = Some(Ok(IncomingBaoReadResult::Served));
                smallvec![]
            }
            IncomingBaoReadState::CloseMetadata => {
                let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
                    return self.unexpected(event);
                };
                self.state = IncomingBaoReadState::Finish;
                self.output = Some(Ok(IncomingBaoReadResult::Probed));
                smallvec![]
            }
            IncomingBaoReadState::SendRefusal => {
                let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
                    return self.unexpected(event);
                };
                self.state = IncomingBaoReadState::CloseRefusal;
                smallvec![Effect::Blob(BlobEffect::CloseConnection {
                    stream_id: self.stream_id,
                })]
            }
            IncomingBaoReadState::CloseRefusal => {
                let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
                    return self.unexpected(event);
                };
                let refusal = self
                    .refusal
                    .take()
                    .unwrap_or(BaoReadRefusal::BackendFailure);
                self.state = IncomingBaoReadState::Finish;
                self.output = Some(Ok(IncomingBaoReadResult::Refused(refusal)));
                smallvec![]
            }
            IncomingBaoReadState::Init => self.unexpected(event),
            IncomingBaoReadState::Finish | IncomingBaoReadState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            IncomingBaoReadState::Finish | IncomingBaoReadState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(BaoReadError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        self.fail(BaoReadError::NotFinished)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use aruna_core::UserId;
    use aruna_core::effects::{BlobEffect, Effect};
    use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::{
        AuthContext, BackendLocation, BlobVersion, BucketInfo, RealmConfigDocument, RealmId,
        RealmNodeKind, VersionedObjectArn,
    };
    use aruna_core::types::Effects;
    use ulid::Ulid;

    use super::{BaoReadOperation, BaoReadOutput, IncomingBaoReadOperation, IncomingBaoReadResult};
    use crate::replication::protocol::{
        BaoReadRefusal, BaoReadRequest, BaoReadTarget, VersionReplicationMessage,
    };

    fn test_realm() -> RealmId {
        RealmId::from_bytes([7u8; 32])
    }

    fn node_from_seed(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn read_request(local_node: aruna_core::NodeId, hash: [u8; 32]) -> BaoReadRequest {
        let realm_id = test_realm();
        BaoReadRequest {
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id),
                realm_id,
                path_restrictions: None,
            },
            realm_id,
            target: BaoReadTarget::ExactVersion(
                VersionedObjectArn::new(
                    realm_id,
                    local_node,
                    "bucket",
                    "path/file.txt",
                    Ulid::from(4u128),
                )
                .unwrap(),
            ),
            expected_blake3: Some(hash),
            metadata_only: false,
        }
    }

    fn realm_value(peer: aruna_core::NodeId) -> byteview::ByteView {
        let mut config = RealmConfigDocument::default_for_realm(test_realm(), Vec::new());
        config.ensure_node(peer, RealmNodeKind::Server);
        postcard::to_allocvec(&config).unwrap().into()
    }

    fn version_value(hash: [u8; 32]) -> byteview::ByteView {
        BlobVersion::materialized(
            hash,
            SystemTime::UNIX_EPOCH,
            UserId::nil(test_realm()),
            None,
        )
        .to_bytes()
        .unwrap()
        .into()
    }

    fn bucket_value() -> byteview::ByteView {
        BucketInfo {
            group_id: Ulid::from(5u128),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(test_realm()),
            cors_configuration: None,
        }
        .to_bytes()
        .unwrap()
        .into()
    }

    fn location_value(hash: [u8; 32]) -> (BackendLocation, byteview::ByteView) {
        let location = BackendLocation {
            root: "/data".to_string(),
            storage_bucket: "blob-0".to_string(),
            backend_path: "object".to_string(),
            ulid: Ulid::from(6u128),
            compressed: false,
            encrypted: false,
            created_by: UserId::nil(test_realm()),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 42,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), hash.to_vec())]),
        };
        let value = location.to_bytes().unwrap().into();
        (location, value)
    }

    fn refusal_from(effects: &Effects) -> BaoReadRefusal {
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected refusal frame")
        };
        let VersionReplicationMessage::BaoReadRefused(reason) =
            VersionReplicationMessage::from_bytes(payload).unwrap()
        else {
            panic!("expected typed refusal")
        };
        reason
    }

    #[test]
    fn exact_probe_hash() {
        let remote_node = node_from_seed(1);
        let hash = [4u8; 32];
        let stream_id = Ulid::from(9u128);
        let mut request = read_request(remote_node, hash);
        request.expected_blake3 = None;
        request.metadata_only = true;
        let mut operation = BaoReadOperation::new(remote_node, request);

        operation.start();
        operation.step(Event::Blob(BlobEvent::ConnectionEstablished { stream_id }));
        operation.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        let payload = VersionReplicationMessage::BaoReadAccepted {
            size: 42,
            blake3: hash,
        }
        .to_bytes()
        .unwrap();
        let effects = operation.step(Event::Blob(BlobEvent::MessageReceived {
            stream_id,
            payload,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::CloseConnection { stream_id: id })] if *id == stream_id
        ));
        operation.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));

        assert_eq!(
            operation.finalize().unwrap(),
            BaoReadOutput::Metadata {
                size: 42,
                blake3: hash,
            }
        );
    }

    #[test]
    fn rejects_unknown_peer() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let configured_peer = node_from_seed(3);
        let mut operation = IncomingBaoReadOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, [4u8; 32]),
        );

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(configured_peer)),
        }));

        assert_eq!(refusal_from(&effects), BaoReadRefusal::RealmPeerDenied);
    }

    #[test]
    fn rejects_user_peer() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let mut config = RealmConfigDocument::default_for_realm(test_realm(), Vec::new());
        config.ensure_node(peer, RealmNodeKind::User);
        let mut operation = IncomingBaoReadOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, [4u8; 32]),
        );

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(postcard::to_allocvec(&config).unwrap().into()),
        }));

        assert_eq!(refusal_from(&effects), BaoReadRefusal::RealmPeerDenied);
    }

    #[test]
    fn denies_wire_read() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let hash = [4u8; 32];
        let mut operation = IncomingBaoReadOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, hash),
        );

        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(peer)),
        }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(bucket_value()),
        }));
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));
        assert_eq!(refusal_from(&effects), BaoReadRefusal::ReadDenied);
    }

    #[test]
    fn serves_exact_blob() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let hash = [4u8; 32];
        let stream_id = Ulid::from(9u128);
        let mut request = read_request(local_node, hash);
        request.expected_blake3 = None;
        let mut operation =
            IncomingBaoReadOperation::new(peer, local_node, test_realm(), stream_id, request);

        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(peer)),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(bucket_value()),
        }));
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(version_value(hash)),
        }));
        let (location, value) = location_value(hash);
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(value),
        }));
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected accepted frame")
        };
        assert_eq!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::BaoReadAccepted {
                size: 42,
                blake3: hash,
            }
        );

        let effects = operation.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ServeRead {
                stream_id: id,
                location: selected,
                expected_blake3,
            })] if *id == stream_id && selected == &location && expected_blake3 == &hash
        ));
        assert!(
            operation
                .step(Event::Blob(BlobEvent::ReadServed { stream_id }))
                .is_empty()
        );
        assert_eq!(operation.finalize().unwrap(), IncomingBaoReadResult::Served);
    }

    #[test]
    fn rejects_hash_mismatch() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let mut operation = IncomingBaoReadOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, [4u8; 32]),
        );

        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(peer)),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(bucket_value()),
        }));
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(version_value([5u8; 32])),
        }));

        assert_eq!(refusal_from(&effects), BaoReadRefusal::HashMismatch);
    }
}
