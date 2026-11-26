//! Reads blob bytes from a peer over bao, negotiating placement rules and serving such reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event, SubOperationEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE,
    DISTRIBUTION_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, VERSION_INDEX_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::request_policy::{CompiledPolicySet, PolicyDecision};
use aruna_core::rows::read_row;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::node_subject::storage_subject;
use aruna_core::structs::placement::policy::{PlacementPolicyRef, PlacementSubject};
use aruna_core::structs::placement::record::NodePlacementEntry;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobLocationKey, BlobVersion, BlobVersionState, BucketInfo,
    HashIndex, ManagedCopyKey, VersionKey, object_permission_path,
};
use aruna_core::structs::storage::distribution::MemberState;
use aruna_core::structs::storage::distribution::inventory::{IndexEntry, Position, index_key};
use aruna_core::structs::storage::distribution::snapshot::DistributionView;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRef, ReadLease,
};
use aruna_core::structs::storage::link::LinkTarget;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use aruna_core::user::id::UserId;
use bytes::Bytes;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::resolver::{ResolveBindingInput, resolve_binding_effect};
use crate::distribution::held::{HeldElsewhere, kept_location, kept_reads};
use crate::s3::object::get::reference_archive;
use crate::s3::object::put::abe::keyed;
use aruna_core::managed_copy::{
    CopyRequest, serve_reads, split_serve_reads, validate_registration,
};
use aruna_core::replication_protocol::{
    BaoReadRefusal, BaoReadRequest, BaoReadTarget, VersionReplicationMessage,
};
use aruna_core::request_policy::{PolicyRequestExtras, policy_request_with};
use aruna_ops_identity::auth::check_permissions::CheckPermissionsConfig;
use aruna_ops_identity::placement::policy::gate::{
    GateContext, PolicyGateError, PolicyGateOutcome, gate_decision, write_gate,
};
use aruna_ops_identity::placement::policy::read::ReadPolicyError;
use aruna_ops_identity::realm::mutate_placement::node_kind;
use aruna_ops_identity::realm::peer_trust::ensure_realm_peer;

#[derive(Debug, PartialEq)]
pub enum BaoReadOutput {
    Metadata {
        size: u64,
        blake3: [u8; 32],
        etag: Option<String>,
        hashes: HashMap<String, Vec<u8>>,
    },
    /// Authorization-only probe succeeded for a `LinkAccess` target; carries no digest or bytes.
    Authorized,
    Stream {
        blob: BackendStream<Result<Bytes, StreamError>>,
        size: u64,
        blake3: [u8; 32],
        etag: Option<String>,
        hashes: HashMap<String, Vec<u8>>,
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
    #[error("bao read failed: {0}")]
    Io(IoError),
    #[error(transparent)]
    ManagedCopy(#[from] aruna_core::managed_copy::ManagedCopyError),
    /// The requester must resolve these rules and ask again.
    #[error("the destination has not resolved every required placement policy")]
    PolicyRequired { refs: Vec<PlacementPolicyRef> },
    #[error("placement policy denies this destination")]
    PolicyDenied { policy_ids: Vec<Ulid> },
    #[error(transparent)]
    Gate(#[from] PolicyGateError),
    /// This node advertises no subject, or stopped admitting while in
    /// transition, so nothing governed may land here.
    #[error("this node is not a legal destination for governed data")]
    NoDestination,
    /// A User node is never a governed destination, so policy-covered content
    /// cannot be read here at all. Terminal and honest: retrying cannot help.
    #[error("governed content is not available on a user node")]
    GovernedUnavailable,
}

/// A run cut off at its deadline or hang limit is a read that did not finish.
impl From<IoError> for BaoReadError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished => Self::NotFinished,
            other => Self::Io(other),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BaoReadState {
    Open,
    Send,
    ReadResponse,
    Receive,
    CloseMetadata,
}

type ReadStep = Result<Next<BaoReadOutput>, BaoReadError>;

/// Reads blob bytes or their metadata from a peer over one stream. A refusal or failure ends
/// with the error; the runner's abort then closes the stream.
#[derive(Debug, PartialEq)]
pub struct BaoReadOperation {
    node_id: NodeId,
    request: BaoReadRequest,
    /// The open stream; `None` before it opens and once it closed or the bytes took it over.
    stream_id: Option<Ulid>,
    state: BaoReadState,
    /// The metadata answer, given once its stream is closed.
    metadata: Option<BaoReadOutput>,
    accepted_blake3: Option<[u8; 32]>,
    accepted_etag: Option<String>,
    accepted_hashes: HashMap<String, Vec<u8>>,
}

fn blob_call<T>(effect: BlobEffect) -> Next<T> {
    Next::Call(IoCall::blob(effect))
}

impl BaoReadOperation {
    pub fn new(node_id: NodeId, request: BaoReadRequest) -> Self {
        Self {
            node_id,
            request,
            stream_id: None,
            state: BaoReadState::Open,
            metadata: None,
            accepted_blake3: None,
            accepted_etag: None,
            accepted_hashes: HashMap::new(),
        }
    }

    fn unexpected(&self, event: &Event) -> BaoReadError {
        let state = match self.state {
            BaoReadState::Open => "open",
            BaoReadState::Send => "send",
            BaoReadState::ReadResponse => "read_response",
            BaoReadState::Receive => "receive",
            BaoReadState::CloseMetadata => "close_metadata",
        };
        BaoReadError::Unexpected {
            state,
            event: event.label().to_string(),
        }
    }

    fn response(&mut self, stream_id: Ulid, payload: &[u8]) -> ReadStep {
        let is_link = matches!(self.request.target, BaoReadTarget::LinkAccess(_));
        let (size, blake3, etag, hashes) = match VersionReplicationMessage::from_bytes(payload)? {
            VersionReplicationMessage::BaoReadAuthorized
                if is_link && self.request.metadata_only =>
            {
                self.metadata = Some(BaoReadOutput::Authorized);
                self.state = BaoReadState::CloseMetadata;
                return Ok(blob_call(BlobEffect::CloseConnection { stream_id }));
            }
            VersionReplicationMessage::BaoReadAccepted {
                size,
                blake3,
                etag,
                hashes,
            } if !is_link => (size, blake3, etag, hashes),
            VersionReplicationMessage::BaoReadRefused(reason) => {
                return Err(BaoReadError::Refused(reason));
            }
            // The source teaches the rule before bytes: the caller
            // resolves it, caches it, and retries only when compliant.
            VersionReplicationMessage::PlacementPolicyRequired { refs } => {
                return Err(BaoReadError::PolicyRequired { refs });
            }
            VersionReplicationMessage::PlacementPolicyDenied { policy_ids } => {
                return Err(BaoReadError::PolicyDenied { policy_ids });
            }
            _ => {
                return Err(BaoReadError::Unexpected {
                    state: "read_response",
                    event: "unexpected bao read response".to_string(),
                });
            }
        };
        if (self.request.expected_blake3).is_some_and(|expected| expected != blake3) {
            return Err(BaoReadError::Refused(BaoReadRefusal::HashMismatch));
        }
        if !range_fits(self.request.range.as_ref(), size) {
            return Err(BaoReadError::Refused(BaoReadRefusal::InvalidTarget));
        }
        if self.request.metadata_only {
            self.metadata = Some(BaoReadOutput::Metadata {
                size,
                blake3,
                etag,
                hashes: hashes.into_iter().collect(),
            });
            self.state = BaoReadState::CloseMetadata;
            return Ok(blob_call(BlobEffect::CloseConnection { stream_id }));
        }
        self.accepted_blake3 = Some(blake3);
        self.accepted_etag = etag;
        self.accepted_hashes = hashes.into_iter().collect();
        self.state = BaoReadState::Receive;
        Ok(blob_call(BlobEffect::ReceiveRead {
            stream_id,
            size,
            expected_blake3: blake3,
            range: self.request.range.clone(),
        }))
    }
}

impl Operation for BaoReadOperation {
    type Output = BaoReadOutput;
    type Error = BaoReadError;

    fn first(&mut self) -> ReadStep {
        Ok(blob_call(BlobEffect::OpenConnection {
            node_id: self.node_id,
        }))
    }

    fn next(&mut self, result: IoResult) -> ReadStep {
        let event = match result.event()?.into_blob() {
            Ok(BlobEvent::Error(error)) => return Err(error.into()),
            Ok(event) => event,
            Err(other) => return Err(self.unexpected(&other)),
        };
        match (self.state, event) {
            (BaoReadState::Open, BlobEvent::ConnectionEstablished { stream_id }) => {
                self.stream_id = Some(stream_id);
                self.state = BaoReadState::Send;
                let message = VersionReplicationMessage::BaoReadRequest(self.request.clone());
                let payload = message.to_bytes()?;
                Ok(blob_call(BlobEffect::SendMessage { stream_id, payload }))
            }
            (BaoReadState::Send, BlobEvent::MessageSent { stream_id }) => {
                self.state = BaoReadState::ReadResponse;
                Ok(blob_call(BlobEffect::ReadMessage { stream_id }))
            }
            (BaoReadState::ReadResponse, BlobEvent::MessageReceived { stream_id, payload }) => {
                self.response(stream_id, &payload)
            }
            (BaoReadState::Receive, BlobEvent::ReadFinished { blob, stream_size }) => {
                // The byte stream owns the connection now.
                self.stream_id = None;
                let blake3 = self
                    .accepted_blake3
                    .take()
                    .ok_or(BaoReadError::NotFinished)?;
                Ok(Next::Done(BaoReadOutput::Stream {
                    blob,
                    size: stream_size,
                    blake3,
                    etag: self.accepted_etag.take(),
                    hashes: std::mem::take(&mut self.accepted_hashes),
                }))
            }
            (BaoReadState::CloseMetadata, BlobEvent::ConnectionClosed { .. }) => {
                self.stream_id = None;
                let metadata = self.metadata.take().ok_or(BaoReadError::NotFinished)?;
                Ok(Next::Done(metadata))
            }
            (_, event) => Err(self.unexpected(&Event::blob(event))),
        }
    }

    /// A deadline or failure must release the stream; only `CloseConnection` unregisters it.
    fn abort(&mut self) -> Vec<IoCall> {
        let close = |stream_id| {
            let close = Effect::blob(BlobEffect::CloseConnection { stream_id });
            IoCall::Effect(Box::new(close))
        };
        self.stream_id.take().map(close).into_iter().collect()
    }
}

/// Whether a requested byte range is non-empty and inside a `size` byte object.
fn range_fits(range: Option<&std::ops::Range<u64>>, size: u64) -> bool {
    range.is_none_or(|range| range.start < range.end && range.end <= size)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncomingBaoResult {
    Served,
    Probed,
    Refused(BaoReadRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncomingBaoState {
    Init,
    ReadRealm,
    /// A logical version target is looked up in this node's member bucket of the set.
    LocateVersion,
    ReadExactVersion,
    /// A record-only version is served from this node's attachment or cache entry.
    ReadKept,
    ReadExactBucket,
    ResolveSource,
    ReadPolicy,
    ReadHashVersion,
    ReadLocation,
    CheckManagedCopy,
    PolicyChallenge,
    /// A plain copy reads its bucket's encryption settings before it is offered.
    ReadContentSettings,
    /// A sealed copy, or a plain copy of an encrypting bucket, waits for a read lease.
    AdmitSealed,
    SendAccepted,
    ServeRead,
    CloseMetadata,
    SendRefusal,
    CloseRefusal,
}

type ServeTurn = Result<Next<IncomingBaoResult>, BaoReadError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PolicyNext {
    Exact,
    Hash,
}

#[derive(Debug, PartialEq)]
pub struct IncomingBaoOperation {
    peer: NodeId,
    local_node: NodeId,
    local_realm: RealmId,
    stream_id: Ulid,
    request: BaoReadRequest,
    state: IncomingBaoState,
    candidates: VecDeque<HashIndex>,
    candidate: Option<HashIndex>,
    blob_hash: Option<[u8; 32]>,
    location_key: Option<BlobLocationKey>,
    location: Option<BackendLocation>,
    candidates_ready: bool,
    had_denial: bool,
    refusal: Option<BaoReadRefusal>,
    /// The operation ended itself with an error, so the stream only closes.
    failed: bool,
    policy_paths: HashSet<String>,
    snapshot: bool,
    policy_path: Option<String>,
    policy_group: Option<GroupId>,
    policy_next: Option<PolicyNext>,
    policy_current: bool,
    /// The version this serve resolved and the refs it carries. A governed copy
    /// answers the destination challenge before a single byte is offered.
    version_key: Option<VersionKey>,
    version_refs: Vec<PlacementPolicyRef>,
    pending_location: Option<BackendLocation>,
    /// The requesting peer's placement as this realm records it. The challenge
    /// is decided against it, never against the subject the peer asserts.
    peer_placement: Option<NodePlacementEntry>,
    /// The owner this node is bound to, when it is a device. A device holds one
    /// person's files and never receives the group policy documents the realm
    /// check needs, so its observations are served under the owner binding.
    local_owner: Option<UserId>,
    /// Whether the requesting peer is a sync-eligible realm node.
    peer_is_infra: bool,
    policy_allowed: bool,
    /// The staging source an observation is served from, with the identity it
    /// was observed under.
    source_access: Option<ResolvedSourceAccess>,
    source_size: Option<u64>,
    source_fingerprint: Option<String>,
    now_ms: u64,
    /// Read lease of a sealed copy; the plaintext is served only under it.
    lease: Option<ReadLease>,
    /// Active key of the encrypting bucket of a plain copy being served.
    plain_key: Option<BucketKeyRef>,
    /// Where a logical version target lies in this node's member bucket.
    located: Option<VersionedObjectArn>,
    /// A record-only version whose kept copy is being looked up.
    kept: Option<HeldElsewhere>,
}

impl IncomingBaoOperation {
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
            state: IncomingBaoState::Init,
            candidates: VecDeque::new(),
            candidate: None,
            blob_hash: None,
            location_key: None,
            location: None,
            candidates_ready: false,
            had_denial: false,
            refusal: None,
            failed: false,
            policy_paths: HashSet::new(),
            snapshot: false,
            policy_path: None,
            policy_group: None,
            policy_next: None,
            policy_current: false,
            version_key: None,
            version_refs: Vec::new(),
            pending_location: None,
            peer_placement: None,
            local_owner: None,
            peer_is_infra: false,
            policy_allowed: false,
            source_access: None,
            source_size: None,
            source_fingerprint: None,
            now_ms: 0,
            lease: None,
            plain_key: None,
            located: None,
            kept: None,
        }
    }

    /// Cache freshness for the challenge; the operation stays sans-I/O by
    /// taking the clock as configuration.
    pub fn with_now(mut self, now_ms: u64) -> Self {
        self.now_ms = now_ms;
        self
    }

    pub fn with_policy_paths(mut self, paths: HashSet<String>) -> Self {
        self.policy_paths = paths;
        self
    }

    pub fn with_policy_candidates(mut self, candidates: Vec<HashIndex>, had_denial: bool) -> Self {
        self.candidates = candidates.into();
        self.candidates_ready = true;
        self.had_denial = had_denial;
        self
    }

    pub fn with_snapshot(mut self) -> Self {
        self.snapshot = true;
        self
    }

    fn exact_target(&self) -> Option<&VersionedObjectArn> {
        match &self.request.target {
            BaoReadTarget::ExactVersion(target) | BaoReadTarget::LinkAccess(target) => Some(target),
            BaoReadTarget::Version { .. } => self.located.as_ref(),
            BaoReadTarget::Blake3(_) => None,
        }
    }

    fn hash_target(&self) -> Option<[u8; 32]> {
        match &self.request.target {
            BaoReadTarget::Blake3(hash) => Some(*hash),
            BaoReadTarget::ExactVersion(_)
            | BaoReadTarget::LinkAccess(_)
            | BaoReadTarget::Version { .. } => None,
        }
    }

    fn read_one(&mut self, state: IncomingBaoState, space: &str, key: impl Into<Key>) -> ServeTurn {
        self.state = state;
        Ok(Next::Call(read_row(space, key)))
    }

    fn read_rows(&mut self, state: IncomingBaoState, reads: Vec<(KeySpace, Key)>) -> ServeTurn {
        self.state = state;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn read_realm(&mut self) -> ServeTurn {
        let key = self.request.realm_id.as_bytes().to_vec();
        self.read_one(IncomingBaoState::ReadRealm, REALM_CONFIG_KEYSPACE, key)
    }

    fn read_exact_version(&mut self) -> ServeTurn {
        let Some(target) = self.exact_target() else {
            return self.fail(BaoReadError::NotFinished);
        };
        let key = VersionKey::new(&target.bucket, &target.key, target.version).to_bytes();
        let key = match key {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.read_one(
            IncomingBaoState::ReadExactVersion,
            BLOB_VERSIONS_KEYSPACE,
            key,
        )
    }

    fn read_exact_bucket(&mut self) -> ServeTurn {
        let Some(target) = self.exact_target() else {
            return self.fail(BaoReadError::NotFinished);
        };
        let key = target.bucket.as_bytes().to_vec();
        self.read_one(IncomingBaoState::ReadExactBucket, S3_BUCKET_KEYSPACE, key)
    }

    fn start_policy(&mut self, group_id: GroupId, path: String, next: PolicyNext) -> ServeTurn {
        self.policy_group = Some(group_id);
        self.policy_path = Some(path.clone());
        self.policy_next = Some(next);
        if !self.snapshot {
            return self.continue_policy(self.policy_paths.contains(&path));
        }
        let mut reads = vec![
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                self.request.realm_id.as_bytes().to_vec().into(),
            ),
            (AUTH_KEYSPACE.to_string(), group_id.to_bytes().into()),
        ];
        // The caller's roles are read in this transaction, after the two policy rows.
        match self.read_check(path).reads() {
            Ok(roles) => reads.extend(roles),
            Err(_) => return self.continue_policy(false),
        }
        self.read_rows(IncomingBaoState::ReadPolicy, reads)
    }

    fn read_check(&self, path: String) -> CheckPermissionsConfig {
        CheckPermissionsConfig {
            auth_context: self.request.auth_context.clone(),
            path,
            required_permission: Permission::READ,
        }
    }

    fn continue_policy(&mut self, allowed: bool) -> ServeTurn {
        let Some(next) = self.policy_next else {
            return self.fail(BaoReadError::NotFinished);
        };
        self.policy_allowed = allowed;
        if !allowed {
            return match next {
                // A device never receives the group's policy documents, so this
                // check cannot pass there: its own observations are owner-bound.
                PolicyNext::Exact if self.serves_own_data() => self.read_exact_version(),
                PolicyNext::Exact => self.send_refusal(BaoReadRefusal::ReadDenied),
                PolicyNext::Hash => {
                    self.had_denial = true;
                    self.next_candidate()
                }
            };
        }
        match next {
            PolicyNext::Exact => self.read_exact_version(),
            PolicyNext::Hash => {
                let Some(candidate) = self.candidate.as_ref() else {
                    return self.fail(BaoReadError::NotFinished);
                };
                let key =
                    match VersionKey::new(&candidate.bucket, &candidate.key, candidate.version_id)
                        .to_bytes()
                    {
                        Ok(key) => key,
                        Err(error) => return self.fail(error.into()),
                    };
                self.read_one(
                    IncomingBaoState::ReadHashVersion,
                    BLOB_VERSIONS_KEYSPACE,
                    key,
                )
            }
        }
    }

    fn handle_policy(&mut self, values: Vec<Option<Value>>) -> ServeTurn {
        let Some(Some(realm_value)) = values.first() else {
            return self.continue_policy(false);
        };
        let Some(Some(group_value)) = values.get(1) else {
            return self.continue_policy(false);
        };
        let Ok(realm) = RealmConfigDocument::from_bytes(realm_value) else {
            return self.continue_policy(false);
        };
        let Ok(group) = GroupAuthorizationDocument::from_bytes(group_value) else {
            return self.continue_policy(false);
        };
        let Some(group_id) = self.policy_group else {
            return self.continue_policy(false);
        };
        if group.group_id != group_id {
            return self.continue_policy(false);
        }
        let Some(path) = self.policy_path.as_deref() else {
            return self.continue_policy(false);
        };
        let request = policy_request_with(
            path,
            &Permission::READ,
            Some(&self.request.auth_context),
            PolicyRequestExtras::operation("s3.GetObject"),
        );
        let realm_set = match CompiledPolicySet::compile(&realm.request_policies) {
            Ok(set) => set,
            Err(_) => return self.continue_policy(false),
        };
        let group_set = match CompiledPolicySet::compile(&group.policies) {
            Ok(set) => set,
            Err(_) => return self.continue_policy(false),
        };
        self.policy_current = matches!(realm_set.evaluate(&request), PolicyDecision::Allowed)
            && matches!(group_set.evaluate(&request), PolicyDecision::Allowed);
        let roles = values[2..].to_vec();
        let allowed = self.read_check(path.to_string()).allowed(roles);
        self.continue_policy(self.policy_current && matches!(allowed, Ok(true)))
    }

    fn next_candidate(&mut self) -> ServeTurn {
        let Some(candidate) = self.candidates.pop_front() else {
            return self.send_refusal(if self.had_denial {
                BaoReadRefusal::ReadDenied
            } else {
                BaoReadRefusal::NotFound
            });
        };
        let path = candidate.permission_path();
        let group_id = candidate.group_id;
        self.candidate = Some(candidate);
        self.start_policy(group_id, path, PolicyNext::Hash)
    }

    fn read_location(&mut self) -> ServeTurn {
        let Some(key) = self.location_key.clone() else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let key = key.to_bytes();
        self.read_one(IncomingBaoState::ReadLocation, BLOB_LOCATIONS_KEYSPACE, key)
    }

    /// A sealed copy is offered only once its bucket admits the read; a locked bucket refuses.
    fn send_accepted(&mut self, location: BackendLocation) -> ServeTurn {
        if self.request.disallow_encrypted && location.format.bucket_key().is_some() {
            return self.send_refusal(BaoReadRefusal::ReadDenied);
        }
        if self.request.metadata_only && !self.request.disallow_encrypted {
            return self.offer_location(location);
        }
        if let Some(key) = location.format.bucket_key() {
            let archive = ArchiveKey::of(&location);
            self.location = Some(location);
            self.state = IncomingBaoState::AdmitSealed;
            return Ok(blob_call(BlobEffect::AdmitRead { key, archive }));
        }
        // A plain copy of an encrypting bucket, not converted yet, needs the bucket unlocked too.
        let Some(version) = self.version_key.clone() else {
            if self.request.disallow_encrypted {
                return self.send_refusal(BaoReadRefusal::ReadDenied);
            }
            return self.offer_location(location);
        };
        self.location = Some(location);
        self.read_one(
            IncomingBaoState::ReadContentSettings,
            BUCKET_ENCRYPTION_KEYSPACE,
            version.bucket.as_bytes().to_vec(),
        )
    }

    fn content_settings_read(&mut self, value: Option<Value>) -> ServeTurn {
        // An observation has no location; it is offered from its source once admitted.
        let location = self.location.take();
        let settings = match BucketEncryption::from_row(value.as_deref()) {
            Ok(settings) => settings,
            Err(error) => return self.fail(error.into()),
        };
        if self.request.disallow_encrypted && settings.is_encrypted() {
            return self.send_refusal(BaoReadRefusal::ReadDenied);
        }
        if self.request.metadata_only {
            return self.offer_admitted(location);
        }
        let Some(key) = settings.active_key() else {
            return self.offer_admitted(location);
        };
        // A copy's lease pins its physical location; an observation has none to pin.
        let archive = location
            .as_ref()
            .map_or_else(|| reference_archive(key), ArchiveKey::of);
        self.plain_key = Some(key);
        self.location = location;
        self.state = IncomingBaoState::AdmitSealed;
        Ok(blob_call(BlobEffect::AdmitRead { key, archive }))
    }

    fn handle_admission(&mut self, event: Event) -> ServeTurn {
        let location = self.location.take();
        let sealed = location
            .as_ref()
            .and_then(|location| Some((location.format.bucket_key()?, ArchiveKey::of(location))));
        let target = match (sealed, self.plain_key) {
            (Some(target), _) => target,
            (None, Some(key)) => {
                let archive = location
                    .as_ref()
                    .map_or_else(|| reference_archive(key), ArchiveKey::of);
                (key, archive)
            }
            (None, None) => return self.fail(BaoReadError::NotFinished),
        };
        match event.into_blob() {
            Ok(BlobEvent::ReadAdmitted { lease })
                if (lease.key, lease.archive.clone()) == target =>
            {
                self.lease = Some(lease);
                self.offer_admitted(location)
            }
            Ok(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(id)))) => {
                self.send_refusal(BaoReadRefusal::BucketLocked(id))
            }
            Ok(BlobEvent::Error(_)) => self.send_refusal(BaoReadRefusal::BackendFailure),
            Ok(other) => {
                let other = Event::blob(other);
                self.unexpected(other)
            }
            Err(other) => self.unexpected(other),
        }
    }

    /// Offers the admitted copy, link authorization, or observation once checked.
    fn offer_admitted(&mut self, location: Option<BackendLocation>) -> ServeTurn {
        if matches!(self.request.target, BaoReadTarget::LinkAccess(_)) {
            return self.send_authorized();
        }
        match location {
            Some(location) => self.offer_location(location),
            None => self.accept_observation(),
        }
    }

    fn send_authorized(&mut self) -> ServeTurn {
        let payload = match VersionReplicationMessage::BaoReadAuthorized.to_bytes() {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingBaoState::SendAccepted;
        Ok(blob_call(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        }))
    }

    fn offer_location(&mut self, location: BackendLocation) -> ServeTurn {
        let Some(blake3) = self.blob_hash else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        if !range_fits(self.request.range.as_ref(), location.blob_size) {
            return self.send_refusal(BaoReadRefusal::InvalidTarget);
        }
        let payload = match (VersionReplicationMessage::BaoReadAccepted {
            size: location.blob_size,
            blake3,
            etag: location.hashes.get(HASH_MD5).map(hex::encode),
            hashes: location.hashes.clone().into_iter().collect(),
        })
        .to_bytes()
        {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.location = Some(location);
        self.state = IncomingBaoState::SendAccepted;
        Ok(blob_call(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        }))
    }

    fn send_refusal(&mut self, refusal: BaoReadRefusal) -> ServeTurn {
        let payload = match VersionReplicationMessage::BaoReadRefused(refusal).to_bytes() {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.refusal = Some(refusal);
        self.state = IncomingBaoState::SendRefusal;
        Ok(blob_call(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        }))
    }

    fn fail(&mut self, error: BaoReadError) -> ServeTurn {
        self.failed = true;
        Err(error)
    }

    fn close(&mut self, state: IncomingBaoState) -> ServeTurn {
        self.state = state;
        Ok(blob_call(BlobEffect::CloseConnection {
            stream_id: self.stream_id,
        }))
    }

    /// The event answering a stream call; a failed stream ends the serve.
    fn stream_event(&mut self, result: IoResult) -> Result<Event, BaoReadError> {
        match (result.event()?).into_blob() {
            Ok(BlobEvent::Error(error)) => {
                self.failed = true;
                Err(error.into())
            }
            Ok(event) => {
                let event = Event::blob(event);
                Ok(event)
            }
            Err(event) => Ok(event),
        }
    }

    /// Whether the reader was already answered, so a failure can only close the stream.
    fn answered(&self) -> bool {
        matches!(
            self.state,
            IncomingBaoState::SendAccepted
                | IncomingBaoState::ServeRead
                | IncomingBaoState::CloseMetadata
                | IncomingBaoState::SendRefusal
                | IncomingBaoState::CloseRefusal
        )
    }

    fn unexpected(&mut self, event: Event) -> ServeTurn {
        self.fail(BaoReadError::Unexpected {
            state: self.state_name(),
            event: event.label().to_string(),
        })
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            IncomingBaoState::Init => "init",
            IncomingBaoState::ReadRealm => "read_realm",
            IncomingBaoState::LocateVersion => "locate_version",
            IncomingBaoState::ReadExactVersion => "read_exact_version",
            IncomingBaoState::ReadKept => "read_kept",
            IncomingBaoState::ReadExactBucket => "read_exact_bucket",
            IncomingBaoState::ResolveSource => "resolve_source",
            IncomingBaoState::ReadPolicy => "read_policy",
            IncomingBaoState::ReadHashVersion => "read_hash_version",
            IncomingBaoState::ReadLocation => "read_location",
            IncomingBaoState::CheckManagedCopy => "check_managed_copy",
            IncomingBaoState::PolicyChallenge => "policy_challenge",
            IncomingBaoState::ReadContentSettings => "read_content_settings",
            IncomingBaoState::AdmitSealed => "admit_sealed",
            IncomingBaoState::SendAccepted => "send_accepted",
            IncomingBaoState::ServeRead => "serve_read",
            IncomingBaoState::CloseMetadata => "close_metadata",
            IncomingBaoState::SendRefusal => "send_refusal",
            IncomingBaoState::CloseRefusal => "close_refusal",
        }
    }

    /// Who this serve runs as. A User peer is owner-bound: admitted as a realm member
    /// without internal trust and forced to its config-named owner, so a device cannot
    /// read as anyone else (D12). Other kinds keep the internal-trust gate.
    fn admit_peer(&mut self, document: &RealmConfigDocument) -> Option<BaoReadRefusal> {
        let owner = node_kind(document, self.peer).and_then(|kind| kind.owner());
        let internal_trust = owner.is_none();
        if ensure_realm_peer(document, self.peer, self.request.realm_id, internal_trust).is_err() {
            return Some(BaoReadRefusal::RealmPeerDenied);
        }
        if let Some(owner) = owner {
            if self.request.auth_context.user_id != owner {
                return Some(BaoReadRefusal::ReadDenied);
            }
            self.request.auth_context.user_id = owner;
        }
        None
    }

    fn handle_realm(&mut self, value: Option<Value>) -> ServeTurn {
        let Some(value) = value else {
            return self.send_refusal(BaoReadRefusal::RealmPeerDenied);
        };
        let document = match RealmConfigDocument::from_bytes(&value) {
            Ok(document) => document,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        if let Some(refusal) = self.admit_peer(&document) {
            return self.send_refusal(refusal);
        }
        self.local_owner = node_kind(&document, self.local_node).and_then(|kind| kind.owner());
        self.peer_is_infra = document
            .sync_eligible_nodes()
            .is_ok_and(|ids| ids.contains(&self.peer));
        self.peer_placement = document.placement_entry(self.peer).cloned();
        match &self.request.target {
            BaoReadTarget::ExactVersion(target) | BaoReadTarget::LinkAccess(target) => {
                if target.realm_id != self.request.realm_id || target.node_id != self.local_node {
                    self.send_refusal(BaoReadRefusal::InvalidTarget)
                } else {
                    self.read_exact_bucket()
                }
            }
            BaoReadTarget::Version { set_id, version_id } => {
                let (set_id, version_id) = (*set_id, *version_id);
                let reads = vec![
                    (DISTRIBUTION_KEYSPACE.to_string(), set_id.to_bytes().into()),
                    (
                        VERSION_INDEX_KEYSPACE.to_string(),
                        index_key(set_id, version_id).into(),
                    ),
                ];
                self.read_rows(IncomingBaoState::LocateVersion, reads)
            }
            BaoReadTarget::Blake3(hash) => {
                if self
                    .request
                    .expected_blake3
                    .is_some_and(|expected| expected != *hash)
                {
                    self.send_refusal(BaoReadRefusal::HashMismatch)
                } else if self.candidates_ready {
                    self.next_candidate()
                } else {
                    self.send_refusal(BaoReadRefusal::BackendFailure)
                }
            }
        }
    }

    /// Finds a logical version in this node's member bucket: the record names the bucket, the
    /// version id index the key. The bucket read that follows checks the binding.
    fn handle_located(&mut self, values: Vec<Option<Value>>) -> ServeTurn {
        let BaoReadTarget::Version { set_id, version_id } = self.request.target else {
            return self.send_refusal(BaoReadRefusal::InvalidTarget);
        };
        let (Some(Some(record)), Some(Some(index))) = (values.first(), values.get(1)) else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let bucket = match DistributionView::from_bytes(record) {
            Ok(view) => view.snapshot().config.bucket.clone(),
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        let position = IndexEntry::from_bytes(index)
            .and_then(|entry| Position::decode(&entry.position))
            .ok()
            .filter(|position| position.set_id == set_id);
        let Some(position) = position else {
            return self.send_refusal(BaoReadRefusal::BackendFailure);
        };
        self.located = Some(VersionedObjectArn {
            realm_id: self.request.realm_id,
            node_id: self.local_node,
            bucket,
            key: position.key,
            version: version_id,
        });
        self.read_exact_bucket()
    }

    fn handle_exact_version(&mut self, value: Option<Value>) -> ServeTurn {
        let Some(value) = value else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let version = match BlobVersion::from_bytes(&value) {
            Ok(version) => version,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        self.version_refs = version.placement_policies.clone();
        if matches!(self.request.target, BaoReadTarget::LinkAccess(_)) {
            return self.probe_link(&version);
        }
        if let Some(held) = self.held_version(&version) {
            return self.read_kept(held);
        }
        let Some(blob_hash) = version.blob_hash().copied() else {
            return self.serve_observation(version);
        };
        // A materialized copy is realm data: it is served only when the group's
        // own policy check passed, exactly as before.
        if !self.policy_allowed {
            return self.send_refusal(BaoReadRefusal::ReadDenied);
        }
        if self
            .request
            .expected_blake3
            .is_some_and(|expected| expected != blob_hash)
        {
            return self.send_refusal(BaoReadRefusal::HashMismatch);
        }
        self.blob_hash = Some(blob_hash);
        self.location_key = version.location_key();
        self.version_key = self
            .exact_target()
            .map(|target| VersionKey::new(&target.bucket, &target.key, target.version));
        self.read_location()
    }

    /// A record-only version of a distributed bucket this node may keep a copy of, or an
    /// external reference of it, whose kept copy serves while its source is unavailable.
    fn held_version(&self, version: &BlobVersion) -> Option<HeldElsewhere> {
        let BlobVersionState::Reference {
            source,
            cached_metadata,
            ..
        } = &version.state
        else {
            return None;
        };
        let external = match self.request.target {
            BaoReadTarget::Version { set_id, .. } => version.identity.map(|id| (set_id, id)),
            _ => None,
        };
        // A native record's identity is the BLAKE3 of its bytes, an external reference's not.
        let (set_id, identity, blake3) = match HeldElsewhere::of(source, version.identity) {
            Some((set_id, identity)) => (set_id, identity, Some(identity)),
            None => external.map(|(set_id, identity)| (set_id, identity, None))?,
        };
        Some(HeldElsewhere {
            set_id,
            version_id: self.exact_target()?.version,
            identity,
            blake3,
            size: cached_metadata.content_length,
            metadata: HashMap::new(),
            version_created_at: None,
            source_policies: Vec::new(),
        })
    }

    /// Serves an attachment or cache entry of a version this node keeps only the record of,
    /// under the same checks as a materialized copy; governed versions are never kept.
    fn read_kept(&mut self, held: HeldElsewhere) -> ServeTurn {
        if !self.policy_allowed {
            return self.send_refusal(BaoReadRefusal::ReadDenied);
        }
        if (self.request.expected_blake3.zip(held.blake3))
            .is_some_and(|(asked, known)| asked != known)
        {
            return self.send_refusal(BaoReadRefusal::HashMismatch);
        }
        if !self.version_refs.is_empty() {
            return self.send_refusal(BaoReadRefusal::NotFound);
        }
        self.blob_hash = held.blake3;
        self.version_key = self
            .exact_target()
            .map(|target| VersionKey::new(&target.bucket, &target.key, target.version));
        let mut reads = kept_reads(&held);
        // An exact read of a record without a copy here names a holder of its bytes instead.
        if let BaoReadTarget::ExactVersion(_) = self.request.target {
            reads.push((
                DISTRIBUTION_KEYSPACE.to_string(),
                held.set_id.to_bytes().into(),
            ));
        }
        self.kept = Some(held);
        self.read_rows(IncomingBaoState::ReadKept, reads)
    }

    fn handle_kept(&mut self, values: Vec<Option<Value>>) -> ServeTurn {
        let Some(held) = self.kept.take() else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let Some(location) = kept_location(&held, &keyed(&values)) else {
            let record = values.get(2).and_then(|value| value.as_deref());
            let holder = record.and_then(|record| self.byte_holder(record));
            return self.send_refusal(match holder {
                Some(holder) => BaoReadRefusal::HeldBy {
                    holder,
                    set_id: held.set_id,
                },
                None => BaoReadRefusal::NotFound,
            });
        };
        // The copy's verified hash serves an external reference, whose record names none.
        let kept = location
            .get_blake3()
            .and_then(|hash| <[u8; 32]>::try_from(hash).ok());
        let asked = self.blob_hash.or(self.request.expected_blake3);
        match kept {
            Some(kept) if asked.is_none_or(|asked| asked == kept) => {
                self.blob_hash = Some(kept);
                self.send_accepted(location)
            }
            _ => self.send_refusal(BaoReadRefusal::HashMismatch),
        }
    }

    /// A ready member other than this node that keeps the bytes of the exact target's range.
    fn byte_holder(&self, record: &[u8]) -> Option<NodeId> {
        let view = DistributionView::from_bytes(record).ok()?;
        let snapshot = view.snapshot();
        let range = snapshot.range_for(&self.exact_target()?.key)?;
        (snapshot.byte_holders(range, None).into_iter())
            .filter(|member| member.state == MemberState::Ready)
            .map(|member| member.node_id)
            .find(|node_id| *node_id != self.local_node)
    }

    /// Whether this node may answer out of its own observations at all: only a
    /// device does, and only for its owner, to a realm node or the owner.
    fn serves_own_data(&self) -> bool {
        self.local_owner
            .is_some_and(|owner| self.peer_is_infra || self.request.auth_context.user_id == owner)
    }

    /// Probes an authorized link version without exposing target bytes or synthetic digests.
    fn probe_link(&mut self, version: &BlobVersion) -> ServeTurn {
        if !self.request.metadata_only
            || self.request.range.is_some()
            || self.request.expected_blake3.is_some()
        {
            return self.send_refusal(BaoReadRefusal::InvalidTarget);
        }
        if !self.policy_allowed || !self.version_refs.is_empty() {
            return self.send_refusal(BaoReadRefusal::ReadDenied);
        }
        let BlobVersionState::Reference { source, .. } = &version.state else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        if LinkTarget::of(source, self.local_realm).is_none() {
            return self.send_refusal(BaoReadRefusal::NotFound);
        }
        self.version_key = self
            .exact_target()
            .map(|target| VersionKey::new(&target.bucket, &target.key, target.version));
        self.observation_settings()
    }

    /// Serves one version this node never materialized: a device streams the
    /// owner's own file from the staging source, under the identity the
    /// requester named and the fingerprint the observation recorded.
    fn serve_observation(&mut self, version: BlobVersion) -> ServeTurn {
        // A realm node holds no observations, so it answers as it always did.
        let Some(owner) = self.local_owner else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        if !(self.peer_is_infra || self.request.auth_context.user_id == owner) {
            return self.send_refusal(BaoReadRefusal::ReadDenied);
        }
        let BlobVersionState::Reference {
            cached_metadata,
            source,
            ..
        } = version.state
        else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        // A governed reference answers the destination challenge or nothing.
        if !self.version_refs.is_empty() {
            return self.send_refusal(BaoReadRefusal::ReadDenied);
        }
        // A probe would have to echo an identity this node has not verified, and an observation
        // is only ever served whole.
        if self.request.metadata_only || self.request.range.is_some() {
            return self.send_refusal(BaoReadRefusal::ReadDenied);
        }
        // The requester names the expected bytes; serving refuses anything that
        // does not hash to them, so an unverified identity is never streamed.
        let Some(expected) = self.request.expected_blake3 else {
            return self.send_refusal(BaoReadRefusal::HashMismatch);
        };
        // Without the observed fingerprint the adapter has nothing to re-check.
        let Some(fingerprint) = cached_metadata.etag.clone() else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        self.blob_hash = Some(expected);
        self.source_size = Some(cached_metadata.content_length);
        self.source_fingerprint = Some(fingerprint);
        self.version_key = self
            .exact_target()
            .map(|target| VersionKey::new(&target.bucket, &target.key, target.version));
        self.state = IncomingBaoState::ResolveSource;
        let resolve = resolve_binding_effect(ResolveBindingInput {
            source,
            group_id: None,
        });
        Ok(Next::Call(resolve.into()))
    }

    /// An observation of an encrypting bucket is served only under its bucket lease.
    fn observation_settings(&mut self) -> ServeTurn {
        let Some(version) = self.version_key.clone() else {
            return self.accept_observation();
        };
        self.location = None;
        self.read_one(
            IncomingBaoState::ReadContentSettings,
            BUCKET_ENCRYPTION_KEYSPACE,
            version.bucket.as_bytes().to_vec(),
        )
    }

    /// Announces an observation the same way a materialized copy is announced,
    /// from the size the observation recorded rather than a blob location.
    fn accept_observation(&mut self) -> ServeTurn {
        let (Some(blake3), Some(size)) = (self.blob_hash, self.source_size) else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let payload = match (VersionReplicationMessage::BaoReadAccepted {
            size,
            blake3,
            etag: self.source_fingerprint.clone(),
            hashes: Default::default(),
        })
        .to_bytes()
        {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingBaoState::SendAccepted;
        Ok(blob_call(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        }))
    }

    fn handle_exact_bucket(&mut self, value: Option<Value>) -> ServeTurn {
        let Some(value) = value else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let bucket = match BucketInfo::from_bytes(&value) {
            Ok(bucket) => bucket,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        // Only the member bucket bound to the set answers for it, never a bucket of the name.
        if let BaoReadTarget::Version { set_id, .. } = self.request.target
            && bucket.distribution.as_ref().map(|binding| binding.set_id) != Some(set_id)
        {
            return self.send_refusal(BaoReadRefusal::NotFound);
        }
        let target = self
            .exact_target()
            .expect("exact target required after exact bucket read");
        let path = object_permission_path(
            self.request.realm_id,
            bucket.group_id,
            self.local_node,
            &target.bucket,
            &target.key,
        );
        self.start_policy(bucket.group_id, path, PolicyNext::Exact)
    }

    fn handle_hash_version(&mut self, value: Option<Value>) -> ServeTurn {
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
        self.location_key = version.location_key();
        self.version_refs = version.placement_policies.clone();
        self.version_key = self.candidate.as_ref().map(|candidate| {
            VersionKey::new(&candidate.bucket, &candidate.key, candidate.version_id)
        });
        self.read_location()
    }

    fn handle_location(&mut self, value: Option<Value>) -> ServeTurn {
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
        if self.version_refs.is_empty() {
            return self.send_accepted(location);
        }
        self.check_managed_copy(location)
    }

    /// A governed copy is only offered from a registration this node can still
    /// serve, and only to a destination it evaluated itself.
    fn check_managed_copy(&mut self, location: BackendLocation) -> ServeTurn {
        let Some(version) = self.version_key.clone() else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let key = ManagedCopyKey::new(version, location.backend.clone());
        let reads = match serve_reads(&key, None) {
            Ok(Effect::Storage(StorageEffect::BatchRead { reads, .. })) => reads,
            Ok(_) => return self.fail(BaoReadError::NotFinished),
            Err(error) => return self.fail(error.into()),
        };
        self.pending_location = Some(location);
        self.read_rows(IncomingBaoState::CheckManagedCopy, reads)
    }

    fn handle_managed_copy(&mut self, values: Vec<Option<Value>>) -> ServeTurn {
        let Some((copy, subject)) = split_serve_reads(keyed(&values)).ok() else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let (Some(location), Some(version)) =
            (self.pending_location.clone(), self.version_key.clone())
        else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let key = ManagedCopyKey::new(version, location.backend.clone());
        if validate_registration(
            copy.as_deref(),
            &CopyRequest {
                key: &key,
                node_id: Some(self.local_node),
                blake3: self.blob_hash,
                refs: &self.version_refs,
                subject_generation: Some(subject.subject.generation),
            },
        )
        .is_err()
        {
            return self.send_refusal(BaoReadRefusal::NotFound);
        }
        self.challenge_destination(location)
    }

    /// The subject this realm places the authenticated peer at. The generation
    /// is node-local, so it is carried over from the claim; every other
    /// attribute must match, or the claim is not this peer's subject.
    fn expected_subject(&self, claimed: &PlacementSubject) -> Option<PlacementSubject> {
        let entry = self.peer_placement.as_ref()?;
        let expected = storage_subject(entry, claimed.generation);
        (*claimed == expected).then_some(expected)
    }

    /// Teaches the requester every unresolved rule, then evaluates the authenticated
    /// destination independently. Authorization already passed, so refs may be
    /// disclosed; echoing one is never authority.
    fn challenge_destination(&mut self, location: BackendLocation) -> ServeTurn {
        let missing: Vec<PlacementPolicyRef> = self
            .version_refs
            .iter()
            .filter(|policy_ref| !self.request.known_refs.contains(policy_ref))
            .copied()
            .collect();
        if !missing.is_empty() {
            return self.send_required(missing);
        }
        let Some(claimed) = self.request.destination.as_ref() else {
            return self.send_denied(Vec::new());
        };
        // The requester asserts its own subject, so it is only ever a claim: the
        // realm's own placement of the authenticated peer decides.
        let Some(destination) = self.expected_subject(claimed) else {
            return self.send_denied(Vec::new());
        };
        let context = GateContext {
            realm_id: self.local_realm,
            subject: destination,
            now_ms: self.now_ms,
            admitting: true,
        };
        // The bucket the peer would write into is its own, so only that node
        // can check who owns it; here the subject decides.
        match write_gate(Some(&context), &self.version_refs, None) {
            Ok(None) => self.send_accepted(location),
            Ok(Some(gate)) => {
                self.state = IncomingBaoState::PolicyChallenge;
                Ok(Next::Call(IoCall::child(gate)))
            }
            Err(_) => self.send_denied(Vec::new()),
        }
    }

    fn finish_challenge(
        &mut self,
        outcome: Result<PolicyGateOutcome, ReadPolicyError>,
    ) -> ServeTurn {
        let Some(location) = self.pending_location.clone() else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let decision = outcome
            .map_err(PolicyGateError::from)
            .and_then(gate_decision);
        match decision {
            Ok(()) => self.send_accepted(location),
            Err(PolicyGateError::Denied { policy_ids })
            | Err(PolicyGateError::Unavailable { policy_ids }) => self.send_denied(policy_ids),
            Err(PolicyGateError::Required { refs }) => self.send_required(refs),
            Err(_) => self.send_denied(Vec::new()),
        }
    }

    fn send_required(&mut self, refs: Vec<PlacementPolicyRef>) -> ServeTurn {
        self.send_policy(VersionReplicationMessage::PlacementPolicyRequired { refs })
    }

    fn send_denied(&mut self, policy_ids: Vec<Ulid>) -> ServeTurn {
        self.send_policy(VersionReplicationMessage::PlacementPolicyDenied { policy_ids })
    }

    fn send_policy(&mut self, message: VersionReplicationMessage) -> ServeTurn {
        let payload = match message.to_bytes() {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.refusal = Some(BaoReadRefusal::ReadDenied);
        self.state = IncomingBaoState::SendRefusal;
        Ok(blob_call(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        }))
    }
}

impl IncomingBaoOperation {
    /// The accepted frame went out: closes a probe, otherwise serves the bytes.
    fn serve_accepted(&mut self) -> ServeTurn {
        if self.request.metadata_only {
            return self.close(IncomingBaoState::CloseMetadata);
        }
        let Some(expected_blake3) = self.blob_hash else {
            return self.fail(BaoReadError::NotFinished);
        };
        let stream_id = self.stream_id;
        if let Some(access) = self.source_access.clone() {
            let (Some(size), Some(fingerprint)) =
                (self.source_size, self.source_fingerprint.clone())
            else {
                return self.fail(BaoReadError::NotFinished);
            };
            self.state = IncomingBaoState::ServeRead;
            return Ok(blob_call(BlobEffect::ServeSourceRead {
                stream_id,
                access,
                size,
                expected_blake3,
                fingerprint,
            }));
        }
        let Some(location) = self.location.clone() else {
            return self.fail(BaoReadError::NotFinished);
        };
        self.state = IncomingBaoState::ServeRead;
        let range = self.request.range.clone();
        if let Some(lease) = self.lease.take() {
            return Ok(blob_call(BlobEffect::ServeSealedRead {
                stream_id,
                location,
                expected_blake3,
                lease: Box::new(lease),
                range,
            }));
        }
        Ok(blob_call(BlobEffect::ServeRead {
            stream_id,
            location,
            expected_blake3,
            range,
        }))
    }

    /// The next step after a stream call answered.
    fn stream_step(&mut self, event: Event) -> ServeTurn {
        match (self.state, event.into_blob()) {
            (IncomingBaoState::SendAccepted, Ok(BlobEvent::MessageSent { .. })) => {
                self.serve_accepted()
            }
            (IncomingBaoState::ServeRead, Ok(BlobEvent::ReadServed { .. })) => {
                Ok(Next::Done(IncomingBaoResult::Served))
            }
            (IncomingBaoState::CloseMetadata, Ok(BlobEvent::ConnectionClosed { .. })) => {
                Ok(Next::Done(IncomingBaoResult::Probed))
            }
            (IncomingBaoState::SendRefusal, Ok(BlobEvent::MessageSent { .. })) => {
                self.close(IncomingBaoState::CloseRefusal)
            }
            (IncomingBaoState::CloseRefusal, Ok(BlobEvent::ConnectionClosed { .. })) => {
                let refusal = self.refusal.unwrap_or(BaoReadRefusal::BackendFailure);
                Ok(Next::Done(IncomingBaoResult::Refused(refusal)))
            }
            (_, Ok(event)) => self.unexpected(Event::blob(event)),
            (_, Err(event)) => self.unexpected(event),
        }
    }
}

impl Operation for IncomingBaoOperation {
    type Output = IncomingBaoResult;
    type Error = BaoReadError;

    fn first(&mut self) -> ServeTurn {
        let user_id = self.request.auth_context.user_id;
        if self.request.realm_id != self.local_realm
            || self.request.auth_context.realm_id != self.request.realm_id
            || (user_id.is_nil() && user_id.realm_id != self.request.realm_id)
        {
            return self.send_refusal(BaoReadRefusal::RealmPeerDenied);
        }
        self.read_realm()
    }

    fn next(&mut self, result: IoResult) -> ServeTurn {
        match self.state {
            IncomingBaoState::ReadRealm => self.handle_realm(result.value()?),
            IncomingBaoState::LocateVersion => self.handle_located(result.values()?),
            IncomingBaoState::ReadExactVersion => self.handle_exact_version(result.value()?),
            IncomingBaoState::ReadKept => self.handle_kept(result.values()?),
            IncomingBaoState::ReadExactBucket => self.handle_exact_bucket(result.value()?),
            IncomingBaoState::ResolveSource => match result.event()? {
                Event::SubOperation(SubOperationEvent::VersionAccessResolved { result }) => {
                    match *result {
                        Ok(access) => {
                            self.source_access = Some(access);
                            self.observation_settings()
                        }
                        Err(_) => self.send_refusal(BaoReadRefusal::NotFound),
                    }
                }
                other => self.unexpected(other),
            },
            IncomingBaoState::ReadPolicy => self.handle_policy(result.values()?),
            IncomingBaoState::ReadHashVersion => self.handle_hash_version(result.value()?),
            IncomingBaoState::ReadLocation => self.handle_location(result.value()?),
            IncomingBaoState::CheckManagedCopy => self.handle_managed_copy(result.values()?),
            IncomingBaoState::PolicyChallenge => {
                self.finish_challenge(result.child::<PolicyGateOutcome, ReadPolicyError>()?)
            }
            IncomingBaoState::ReadContentSettings => self.content_settings_read(result.value()?),
            // A refused admission becomes a typed refusal to the reader, not a failed serve.
            IncomingBaoState::AdmitSealed => self.handle_admission(result.event()?),
            IncomingBaoState::SendAccepted
            | IncomingBaoState::ServeRead
            | IncomingBaoState::CloseMetadata
            | IncomingBaoState::SendRefusal
            | IncomingBaoState::CloseRefusal => {
                let event = self.stream_event(result)?;
                self.stream_step(event)
            }
            IncomingBaoState::Init => self.fail(BaoReadError::Unexpected {
                state: self.state_name(),
                event: result.label().to_string(),
            }),
        }
    }

    /// One read snapshot covers the whole serve when the caller asks for it.
    fn transaction(&self) -> TransactionMode {
        match self.snapshot {
            true => TransactionMode::ReadSnapshot,
            false => TransactionMode::None,
        }
    }

    /// A storage failure or a cut-off before any answer still tells the reader, as a backend
    /// refusal; every failed serve closes its stream.
    fn abort(&mut self) -> Vec<IoCall> {
        let stream_id = self.stream_id;
        let mut calls = Vec::new();
        if !self.failed && !self.answered() {
            let refusal = VersionReplicationMessage::BaoReadRefused(BaoReadRefusal::BackendFailure);
            if let Ok(payload) = refusal.to_bytes() {
                let send = BlobEffect::SendMessage { stream_id, payload };
                calls.push(Effect::blob(send).into());
            }
        }
        calls.push(Effect::blob(BlobEffect::CloseConnection { stream_id }).into());
        calls
    }
}

#[cfg(test)]
#[path = "bao_read_tests.rs"]
mod pure_tests;
