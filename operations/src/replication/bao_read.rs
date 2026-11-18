//! Reads blob bytes from a peer over bao, negotiating placement rules and serving such reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event, SubOperationEvent};
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
use aruna_core::structs::storage::distribution::inventory::{IndexEntry, Position, index_key};
use aruna_core::structs::storage::distribution::{DistributionView, MemberState};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRef, ReadLease,
};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use aruna_core::{NodeId, UserId};
use bytes::Bytes;
use thiserror::Error;
use ulid::Ulid;

use super::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget, VersionReplicationMessage};
use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::connectors::resolver::{ResolveBindingInput, resolve_binding_effect};
use crate::distribution::held::{HeldElsewhere, kept_location, kept_reads};
use crate::placement::policy::{
    GateContext, PolicyGateError, PolicyGateOutcome, ReadPolicyError, gate_decision, write_gate,
};
use crate::realm::mutate_placement::node_kind;
use crate::realm::peer_trust::ensure_realm_peer;
use crate::s3::object::get::reference_archive;
use crate::s3::object::put::abe::keyed;
use aruna_core::managed_copy::{
    CopyRequest, serve_reads, split_serve_reads, validate_registration,
};

#[derive(Debug, PartialEq)]
pub enum BaoReadOutput {
    Metadata {
        size: u64,
        blake3: [u8; 32],
        etag: Option<String>,
        hashes: HashMap<String, Vec<u8>>,
    },
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
    Next::Call(IoCall::Effect(Box::new(Effect::Blob(effect))))
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
        let (size, blake3, etag, hashes) = match VersionReplicationMessage::from_bytes(payload)? {
            VersionReplicationMessage::BaoReadAccepted {
                size,
                blake3,
                etag,
                hashes,
            } => (size, blake3, etag, hashes),
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
        let event = result.event()?;
        if let Event::Blob(BlobEvent::Error(error)) = event {
            return Err(error.into());
        }
        match (self.state, event) {
            (BaoReadState::Open, Event::Blob(BlobEvent::ConnectionEstablished { stream_id })) => {
                self.stream_id = Some(stream_id);
                self.state = BaoReadState::Send;
                let message = VersionReplicationMessage::BaoReadRequest(self.request.clone());
                let payload = message.to_bytes()?;
                Ok(blob_call(BlobEffect::SendMessage { stream_id, payload }))
            }
            (BaoReadState::Send, Event::Blob(BlobEvent::MessageSent { stream_id })) => {
                self.state = BaoReadState::ReadResponse;
                Ok(blob_call(BlobEffect::ReadMessage { stream_id }))
            }
            (
                BaoReadState::ReadResponse,
                Event::Blob(BlobEvent::MessageReceived { stream_id, payload }),
            ) => self.response(stream_id, &payload),
            (BaoReadState::Receive, Event::Blob(BlobEvent::ReadFinished { blob, stream_size })) => {
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
            (BaoReadState::CloseMetadata, Event::Blob(BlobEvent::ConnectionClosed { .. })) => {
                self.stream_id = None;
                let metadata = self.metadata.take().ok_or(BaoReadError::NotFinished)?;
                Ok(Next::Done(metadata))
            }
            (_, event) => Err(self.unexpected(&event)),
        }
    }

    /// A deadline or failure must release the stream; only `CloseConnection` unregisters it.
    fn abort(&mut self) -> Vec<IoCall> {
        let close = |stream_id| {
            let close = Effect::Blob(BlobEffect::CloseConnection { stream_id });
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
            BaoReadTarget::ExactVersion(target) => Some(target),
            BaoReadTarget::Version { .. } => self.located.as_ref(),
            BaoReadTarget::Blake3(_) => None,
        }
    }

    fn hash_target(&self) -> Option<[u8; 32]> {
        match &self.request.target {
            BaoReadTarget::Blake3(hash) => Some(*hash),
            BaoReadTarget::ExactVersion(_) | BaoReadTarget::Version { .. } => None,
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
        if self.request.metadata_only {
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
        match event {
            Event::Blob(BlobEvent::ReadAdmitted { lease })
                if (lease.key, lease.archive.clone()) == target =>
            {
                self.lease = Some(lease);
                self.offer_admitted(location)
            }
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(id)))) => {
                self.send_refusal(BaoReadRefusal::BucketLocked(id))
            }
            Event::Blob(BlobEvent::Error(_)) => self.send_refusal(BaoReadRefusal::BackendFailure),
            other => self.unexpected(other),
        }
    }

    /// Offers the admitted copy, or the observation when no location was resolved.
    fn offer_admitted(&mut self, location: Option<BackendLocation>) -> ServeTurn {
        match location {
            Some(location) => self.offer_location(location),
            None => self.accept_observation(),
        }
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
        match result.event()? {
            Event::Blob(BlobEvent::Error(error)) => {
                self.failed = true;
                Err(error.into())
            }
            event => Ok(event),
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
            BaoReadTarget::ExactVersion(target) => {
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
        match (self.state, event) {
            (IncomingBaoState::SendAccepted, Event::Blob(BlobEvent::MessageSent { .. })) => {
                self.serve_accepted()
            }
            (IncomingBaoState::ServeRead, Event::Blob(BlobEvent::ReadServed { .. })) => {
                Ok(Next::Done(IncomingBaoResult::Served))
            }
            (IncomingBaoState::CloseMetadata, Event::Blob(BlobEvent::ConnectionClosed { .. })) => {
                Ok(Next::Done(IncomingBaoResult::Probed))
            }
            (IncomingBaoState::SendRefusal, Event::Blob(BlobEvent::MessageSent { .. })) => {
                self.close(IncomingBaoState::CloseRefusal)
            }
            (IncomingBaoState::CloseRefusal, Event::Blob(BlobEvent::ConnectionClosed { .. })) => {
                let refusal = self.refusal.unwrap_or(BaoReadRefusal::BackendFailure);
                Ok(Next::Done(IncomingBaoResult::Refused(refusal)))
            }
            (_, event) => self.unexpected(event),
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
                Event::SubOperation(SubOperationEvent::VersionAccessResolved {
                    result: Ok(access),
                }) => {
                    self.source_access = Some(access);
                    self.observation_settings()
                }
                Event::SubOperation(SubOperationEvent::VersionAccessResolved {
                    result: Err(_),
                }) => self.send_refusal(BaoReadRefusal::NotFound),
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
                calls.push(Effect::Blob(send).into());
            }
        }
        calls.push(Effect::Blob(BlobEffect::CloseConnection { stream_id }).into());
        calls
    }
}

#[cfg(test)]
mod pure_tests {
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;
    use std::collections::{HashMap, HashSet};
    use std::time::SystemTime;

    use aruna_core::UserId;
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::operation::{EffectOperation, Operation};
    use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_MD5};
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
    use aruna_core::structs::placement::policy::PlacementPolicyRef;
    use aruna_core::structs::storage::blob::{
        BackendLocation, BackendRef, BlobVersion, BucketInfo,
    };
    use aruna_core::structs::storage::replication::VersionedObjectArn;
    use aruna_core::types::Effects;
    use ulid::Ulid;

    use super::{
        BaoReadError, BaoReadOperation, BaoReadOutput, IncomingBaoOperation, IncomingBaoResult,
    };
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
                session: None,
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
            destination: None,
            known_refs: Vec::new(),
            range: None,
        }
    }

    fn read_path(local_node: aruna_core::NodeId) -> String {
        aruna_core::structs::storage::blob::object_permission_path(
            test_realm(),
            Ulid::from(5u128),
            local_node,
            "bucket",
            "path/file.txt",
        )
    }

    fn realm_value(peer: aruna_core::NodeId) -> byteview::ByteView {
        let mut config = RealmConfigDocument::default_for_realm(test_realm(), Vec::new());
        config.ensure_node(peer, RealmNodeKind::Server);
        postcard::to_allocvec(&config).unwrap().into()
    }

    fn version_value(hash: [u8; 32]) -> byteview::ByteView {
        BlobVersion::materialized(
            hash,
            BackendRef::node_default(),
            EncodingClass::Raw,
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
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
        .to_bytes()
        .unwrap()
        .into()
    }

    fn location_value(hash: [u8; 32]) -> (BackendLocation, byteview::ByteView) {
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "blob-0".to_string(),
            backend_path: "object".to_string(),
            ulid: Ulid::from(6u128),
            format: StoredFormat::default(),
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

    /// One observation of a local directory, as a device records it.
    fn observation(size: u64, etag: Option<&str>) -> BlobVersion {
        use aruna_core::structs::execution::offered_directory::OFFERED_DIRECTORY_BUCKET;
        use aruna_core::structs::execution::source_access::SourceMetadata;
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        use aruna_core::structs::execution::staging::{
            PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
        };
        BlobVersion::reference(
            VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind: SourceConnectorKind::LocalDirectory,
                    public_config: HashMap::from([(
                        OFFERED_DIRECTORY_BUCKET.to_string(),
                        "folder-x".to_string(),
                    )]),
                    source_path: "path/file.txt".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                    namespace: None,
                    root: None,
                },
                connector_id: None,
                mirrors: Vec::new(),
            },
            SourceMetadata {
                content_length: size,
                content_type: None,
                etag: etag.map(ToOwned::to_owned),
                last_modified: None,
                source_version: None,
                digests: Default::default(),
            },
            SystemTime::UNIX_EPOCH,
            UserId::nil(test_realm()),
            SystemTime::UNIX_EPOCH,
        )
    }

    /// A serve on a device, for its own owner, asked by realm infrastructure.
    fn device_serve(hash: Option<[u8; 32]>) -> IncomingBaoOperation {
        let local_node = node_from_seed(1);
        let mut request = read_request(local_node, [4u8; 32]);
        request.expected_blake3 = hash;
        let mut operation = IncomingBaoOperation::new(
            node_from_seed(2),
            local_node,
            test_realm(),
            Ulid::from(9u128),
            request,
        );
        operation.local_owner = Some(UserId::nil(test_realm()));
        operation.peer_is_infra = true;
        operation
    }

    #[test]
    fn realm_refuses_observation() {
        // A realm node holds no observations, so it answers as it always did.
        let mut operation = device_serve(Some([5u8; 32]));
        operation.local_owner = None;
        let effects = operation.serve_observation(observation(5, Some("5-1")));
        assert_eq!(refusal_from(&effects), BaoReadRefusal::NotFound);
    }

    #[test]
    fn device_refuses_stranger() {
        // Neither infrastructure nor the owner: the entry is not even looked up.
        let mut operation = device_serve(Some([5u8; 32]));
        operation.peer_is_infra = false;
        operation.request.auth_context.user_id = UserId::local(Ulid::from(77u128), test_realm());
        assert!(!operation.serves_own_data());
        let effects = operation.serve_observation(observation(5, Some("5-1")));
        assert_eq!(refusal_from(&effects), BaoReadRefusal::ReadDenied);
    }

    #[test]
    fn device_needs_hash() {
        // Without a named identity this node would have to invent one.
        let mut operation = device_serve(None);
        let effects = operation.serve_observation(observation(5, Some("5-1")));
        assert_eq!(refusal_from(&effects), BaoReadRefusal::HashMismatch);
    }

    #[test]
    fn device_needs_fingerprint() {
        // Without the observed fingerprint the adapter cannot re-check anything.
        let mut operation = device_serve(Some([5u8; 32]));
        let effects = operation.serve_observation(observation(5, None));
        assert_eq!(refusal_from(&effects), BaoReadRefusal::NotFound);
    }

    #[test]
    fn device_refuses_governed() {
        // A governed reference answers the destination challenge or nothing.
        let mut operation = device_serve(Some([5u8; 32]));
        operation.version_refs = vec![PlacementPolicyRef {
            policy_id: Ulid::from(3u128),
            digest: [2u8; 32],
        }];
        let effects = operation.serve_observation(observation(5, Some("5-1")));
        assert_eq!(refusal_from(&effects), BaoReadRefusal::ReadDenied);
    }

    #[test]
    fn device_refuses_probe() {
        // A metadata probe would echo an identity this node has not verified.
        let mut operation = device_serve(Some([5u8; 32]));
        operation.request.metadata_only = true;
        let effects = operation.serve_observation(observation(5, Some("5-1")));
        assert_eq!(refusal_from(&effects), BaoReadRefusal::ReadDenied);
    }

    #[test]
    fn device_serves_file() {
        let mut operation = device_serve(Some([5u8; 32]));
        let effects = operation.serve_observation(observation(5, Some("5-1")));
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        assert_eq!(operation.source_fingerprint.as_deref(), Some("5-1"));
    }

    #[test]
    fn denied_keeps_blob() {
        // A device may bypass the group check for its own observations, never
        // for a materialized copy it holds.
        let mut operation = device_serve(Some([4u8; 32]));
        operation.policy_allowed = false;
        let (location, _) = location_value([4u8; 32]);
        let materialized = BlobVersion::materialized(
            [4u8; 32],
            location.backend.clone(),
            EncodingClass::Raw,
            SystemTime::UNIX_EPOCH,
            UserId::nil(test_realm()),
            None,
        );
        let value: byteview::ByteView = materialized.to_bytes().unwrap().into();
        let effects = operation.handle_exact_version(Event::Storage(StorageEvent::ReadResult {
            key: byteview::ByteView::from(vec![0u8]),
            value: Some(value),
        }));
        assert_eq!(refusal_from(&effects), BaoReadRefusal::ReadDenied);
    }

    #[test]
    fn source_rejects_event() {
        // An event the resolve state cannot explain must fail, not be ignored.
        let mut operation = device_serve(Some([5u8; 32]));
        operation.state = super::IncomingBaoState::ResolveSource;
        operation.step(Event::Storage(StorageEvent::SyncAllFinished));
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(BaoReadError::Unexpected { .. })
        ));
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

        let _ = operation.first();
        let _ = operation.next(blob(BlobEvent::ConnectionEstablished { stream_id }));
        let _ = operation.next(blob(BlobEvent::MessageSent { stream_id }));
        let payload = VersionReplicationMessage::BaoReadAccepted {
            size: 42,
            blake3: hash,
            etag: Some("etag-1".to_string()),
            hashes: [("md5".to_string(), vec![3u8; 16])].into_iter().collect(),
        }
        .to_bytes()
        .unwrap();
        let close = operation.next(blob(BlobEvent::MessageReceived { stream_id, payload }));
        assert!(matches!(
            blob_effect(&close),
            Some(BlobEffect::CloseConnection { stream_id: id }) if *id == stream_id
        ));
        let done = operation.next(blob(BlobEvent::ConnectionClosed { stream_id }));
        assert!(operation.abort().is_empty());

        assert_eq!(
            done.unwrap(),
            Next::Done(BaoReadOutput::Metadata {
                size: 42,
                blake3: hash,
                etag: Some("etag-1".to_string()),
                hashes: HashMap::from([("md5".to_string(), vec![3u8; 16])]),
            })
        );
    }

    fn blob(event: BlobEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Blob(event)))
    }

    /// The blob effect a requester step asks for, if it asks for one.
    fn blob_effect(step: &Result<Next<BaoReadOutput>, BaoReadError>) -> Option<&BlobEffect> {
        let Ok(Next::Call(IoCall::Effect(effect))) = step else {
            return None;
        };
        match effect.as_ref() {
            Effect::Blob(effect) => Some(effect),
            _ => None,
        }
    }

    /// Drives a requester read to the response frame and returns its outcome there; the
    /// runner's abort then closes the open stream.
    fn respond(message: VersionReplicationMessage) -> Result<Next<BaoReadOutput>, BaoReadError> {
        let remote_node = node_from_seed(1);
        let stream_id = Ulid::from(9u128);
        let mut operation =
            BaoReadOperation::new(remote_node, read_request(remote_node, [4u8; 32]));
        let _ = operation.first();
        let _ = operation.next(blob(BlobEvent::ConnectionEstablished { stream_id }));
        let _ = operation.next(blob(BlobEvent::MessageSent { stream_id }));
        let payload = message.to_bytes().unwrap();
        let answer = operation.next(blob(BlobEvent::MessageReceived { stream_id, payload }));
        let close = Effect::Blob(BlobEffect::CloseConnection { stream_id });
        assert_eq!(operation.abort(), vec![IoCall::Effect(Box::new(close))]);
        answer
    }

    // A range outside the announced object is refused before any byte is received.
    #[test]
    fn ranged_read_bounds() {
        let remote_node = node_from_seed(1);
        let stream_id = Ulid::from(9u128);
        let mut request = read_request(remote_node, [4u8; 32]);
        request.range = Some(40..43);
        let mut operation = BaoReadOperation::new(remote_node, request.clone());
        let _ = operation.first();
        let _ = operation.next(blob(BlobEvent::ConnectionEstablished { stream_id }));
        let _ = operation.next(blob(BlobEvent::MessageSent { stream_id }));
        let accepted = VersionReplicationMessage::BaoReadAccepted {
            size: 42,
            blake3: [4u8; 32],
            etag: None,
            hashes: Default::default(),
        };
        let payload = accepted.to_bytes().unwrap();
        let refused = operation.next(blob(BlobEvent::MessageReceived { stream_id, payload }));
        assert_eq!(
            refused,
            Err(BaoReadError::Refused(BaoReadRefusal::InvalidTarget))
        );

        request.range = Some(2..10);
        let mut operation = BaoReadOperation::new(remote_node, request);
        let _ = operation.first();
        let _ = operation.next(blob(BlobEvent::ConnectionEstablished { stream_id }));
        let _ = operation.next(blob(BlobEvent::MessageSent { stream_id }));
        let payload = accepted.to_bytes().unwrap();
        let receive = operation.next(blob(BlobEvent::MessageReceived { stream_id, payload }));
        assert!(matches!(
            blob_effect(&receive),
            Some(BlobEffect::ReceiveRead { size: 42, range: Some(range), .. })
                if *range == (2..10)
        ));
    }

    #[test]
    fn required_teaches_refs() {
        // The source teaches the rule instead of streaming, so the requester can
        // resolve it and ask again.
        let policy_ref = PlacementPolicyRef {
            policy_id: Ulid::from(3u128),
            digest: [2u8; 32],
        };
        let answer = respond(VersionReplicationMessage::PlacementPolicyRequired {
            refs: vec![policy_ref],
        });
        assert_eq!(
            answer,
            Err(BaoReadError::PolicyRequired {
                refs: vec![policy_ref]
            })
        );
    }

    #[test]
    fn denied_never_streams() {
        let answer = respond(VersionReplicationMessage::PlacementPolicyDenied {
            policy_ids: vec![Ulid::from(3u128)],
        });
        assert_eq!(
            answer,
            Err(BaoReadError::PolicyDenied {
                policy_ids: vec![Ulid::from(3u128)]
            })
        );
    }

    #[test]
    fn echoed_refs_denied() {
        // The requester claims to know the rule, but the source still evaluates
        // the destination itself and refuses it.
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let policy_ref = PlacementPolicyRef {
            policy_id: Ulid::from(3u128),
            digest: [2u8; 32],
        };
        let mut request = read_request(local_node, [4u8; 32]);
        request.known_refs = vec![policy_ref];
        request.destination = None;
        let mut operation =
            IncomingBaoOperation::new(peer, local_node, test_realm(), Ulid::from(9u128), request);
        operation.version_refs = vec![policy_ref];
        operation.request.known_refs = vec![policy_ref];

        // No destination subject means nothing governed may be served, even
        // though every ref was echoed back.
        let (location, _) = location_value([4u8; 32]);
        let effects = operation.challenge_destination(location);
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected a policy frame")
        };
        assert!(matches!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::PlacementPolicyDenied { .. }
        ));
    }

    #[test]
    fn spoofed_subject_denied() {
        // The requester may assert any subject; only the realm's placement of
        // the authenticated peer decides where governed bytes may go.
        use aruna_core::structs::placement::node_subject::storage_subject;
        use aruna_core::structs::placement::policy::PlacementSubject;
        use aruna_core::structs::placement::record::{DEFAULT_NODE_WEIGHT, NodePlacementEntry};

        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let policy_ref = PlacementPolicyRef {
            policy_id: Ulid::from(3u128),
            digest: [2u8; 32],
        };
        let entry = NodePlacementEntry {
            node_id: peer,
            location: "us-east".to_string(),
            weight: DEFAULT_NODE_WEIGHT,
            full: false,
            draining: false,
            labels: std::collections::BTreeMap::new(),
        };
        let claimed = PlacementSubject {
            location: "eu-west".to_string(),
            ..storage_subject(&entry, 4)
        };
        let mut request = read_request(local_node, [4u8; 32]);
        request.known_refs = vec![policy_ref];
        request.destination = Some(claimed);
        let mut operation =
            IncomingBaoOperation::new(peer, local_node, test_realm(), Ulid::from(9u128), request);
        operation.version_refs = vec![policy_ref];
        operation.peer_placement = Some(entry.clone());

        let (location, _) = location_value([4u8; 32]);
        let effects = operation.challenge_destination(location.clone());
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected a policy frame")
        };
        assert!(matches!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::PlacementPolicyDenied { .. }
        ));

        // The honest claim resolves the rule instead of being refused outright.
        operation.request.destination = Some(storage_subject(&entry, 4));
        let effects = operation.challenge_destination(location);
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
    }

    #[test]
    fn folds_read_check() {
        // The caller's roles are read with the policy rows, and they decide the read.
        use aruna_core::structs::identity::auth::{Actor, Permission, Role};
        use aruna_core::structs::identity::group::GroupAuthorizationDocument;
        use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
        use aruna_core::structs::storage::blob::object_permission_path;

        let (realm, local, peer) = (test_realm(), node_from_seed(1), node_from_seed(2));
        let (group_id, nil) = (Ulid::from(8u128), UserId::nil(realm));
        let path = object_permission_path(realm, group_id, local, "bucket", "path/file.txt");
        let actor = Actor {
            node_id: local,
            user_id: nil,
            realm_id: realm,
        };
        let group = GroupAuthorizationDocument::default_group_doc(nil, realm, group_id);
        let mut roles = GroupAuthorizationDocument::default_group_doc(nil, realm, group_id);
        // The test caller is the nil user, which holds only public roles.
        let role_id = Ulid::from(9u128);
        let role = Role {
            role_id,
            name: "public".to_string(),
            permissions: HashMap::from([(format!("/{realm}/g/{group_id}/**"), Permission::READ)]),
            assigned_users: HashSet::from([nil]),
        };
        roles.roles.insert(role_id, role);
        let config = RealmConfigDocument::default_for_realm(realm, Vec::new());
        let realm_roles = RealmAuthorizationDocument::default_realm_doc(realm);
        let decide = |group_roles: Option<Vec<u8>>| {
            let request = read_request(local, [4u8; 32]);
            let mut operation =
                IncomingBaoOperation::new(peer, local, realm, Ulid::from(9u128), request);
            let txn_id = Ulid::from(10u128);
            operation.txn_id = Some(txn_id);
            let next = super::PolicyNext::Exact;
            let effects = operation.start_policy(group_id, path.clone(), next);
            let [
                Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: read,
                }),
            ] = effects.as_slice()
            else {
                panic!("expected the policy read, got {effects:?}")
            };
            assert_eq!((reads.len(), *read), (4, Some(txn_id)));
            let rows = [
                Some(postcard::to_allocvec(&config).unwrap()),
                Some(group.to_bytes(&actor).unwrap()),
                Some(realm_roles.to_bytes(&actor).unwrap()),
                group_roles,
            ];
            let values = reads
                .iter()
                .zip(rows)
                .map(|((_, key), row)| (key.clone(), row.map(Into::into)))
                .collect();
            operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
            operation.policy_allowed
        };
        assert!(decide(Some(roles.to_bytes(&actor).unwrap())));
        assert!(!decide(Some(group.to_bytes(&actor).unwrap())));
        assert!(!decide(None));
    }

    fn device_realm_value(peer: aruna_core::NodeId, owner: UserId) -> byteview::ByteView {
        let mut config = RealmConfigDocument::default_for_realm(test_realm(), Vec::new());
        config.ensure_node(peer, RealmNodeKind::User { owner });
        postcard::to_allocvec(&config).unwrap().into()
    }

    fn device_operation(
        local_node: aruna_core::NodeId,
        peer: aruna_core::NodeId,
        asserted: UserId,
    ) -> IncomingBaoOperation {
        let mut request = read_request(local_node, [4u8; 32]);
        request.auth_context.user_id = asserted;
        IncomingBaoOperation::new(peer, local_node, test_realm(), Ulid::from(9u128), request)
    }

    // A device is a realm member without internal trust: it is admitted, and the
    // serve is authorized as the owner its realm config names.
    #[test]
    fn serves_device_owner() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let owner = UserId::new(Ulid::from(11u128), test_realm());
        let mut operation = device_operation(local_node, peer, owner);

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(device_realm_value(peer, owner)),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        assert_eq!(operation.request.auth_context.user_id, owner);
        // A device is never a placement destination, so nothing governed lands.
        assert_eq!(operation.peer_placement, None);
    }

    #[test]
    fn refuses_device_impersonation() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let owner = UserId::new(Ulid::from(11u128), test_realm());
        let other = UserId::new(Ulid::from(12u128), test_realm());
        let mut operation = device_operation(local_node, peer, other);

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(device_realm_value(peer, owner)),
        }));

        assert_eq!(refusal_from(&effects), BaoReadRefusal::ReadDenied);
    }

    // Governed content stays out of reach: the device claims no subject, so the
    // destination challenge denies before a byte is offered.
    #[test]
    fn refuses_governed_device() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let owner = UserId::new(Ulid::from(11u128), test_realm());
        let policy_ref = PlacementPolicyRef {
            policy_id: Ulid::from(21u128),
            digest: [7u8; 32],
        };
        let mut operation = device_operation(local_node, peer, owner);
        operation.request.known_refs = vec![policy_ref];
        operation.version_refs = vec![policy_ref];
        operation.peer_placement = None;

        let (location, _) = location_value([4u8; 32]);
        let effects = operation.challenge_destination(location);

        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected a policy frame")
        };
        assert!(matches!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::PlacementPolicyDenied { .. }
        ));
    }

    // A device gets one honest, terminal answer for governed content instead of
    // a negotiation it can never finish.
    #[test]
    fn device_answer_terminal() {
        let policy_ref = PlacementPolicyRef {
            policy_id: Ulid::from(31u128),
            digest: [5u8; 32],
        };
        for error in [
            BaoReadError::PolicyRequired {
                refs: vec![policy_ref],
            },
            BaoReadError::PolicyDenied {
                policy_ids: vec![policy_ref.policy_id],
            },
            BaoReadError::NoDestination,
        ] {
            assert_eq!(
                crate::replication::managed_read::device_answer(Err(error)),
                Err(BaoReadError::GovernedUnavailable)
            );
        }
        assert_eq!(
            crate::replication::managed_read::device_answer(Err(BaoReadError::Refused(
                BaoReadRefusal::NotFound
            ))),
            Err(BaoReadError::Refused(BaoReadRefusal::NotFound))
        );
    }

    #[test]
    fn rejects_unknown_peer() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let configured_peer = node_from_seed(3);
        let mut operation = IncomingBaoOperation::new(
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
    fn snapshot_reads_txn() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let mut operation = IncomingBaoOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, [4u8; 32]),
        )
        .with_snapshot();

        assert!(matches!(
            operation.start().as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        ));
        let txn_id = Ulid::from(10u128);
        assert!(matches!(
            operation
                .step(Event::Storage(StorageEvent::TransactionStarted { txn_id }))
                .as_slice(),
            [Effect::Storage(StorageEffect::Read {
                txn_id: Some(read_txn),
                ..
            })] if *read_txn == txn_id
        ));
    }

    #[test]
    fn checks_serving_realm() {
        // A user of another realm reads through this realm; a session of another realm, or an
        // anonymous identity of another realm, is refused.
        let (local_node, peer) = (node_from_seed(1), node_from_seed(2));
        let foreign = RealmId::from_bytes([8u8; 32]);
        for (user_id, serving_realm, accepted) in [
            (
                UserId::local(Ulid::from(5u128), foreign),
                test_realm(),
                true,
            ),
            (
                UserId::local(Ulid::from(5u128), test_realm()),
                foreign,
                false,
            ),
            (UserId::nil(foreign), test_realm(), false),
        ] {
            let mut request = read_request(local_node, [4u8; 32]);
            request.auth_context.user_id = user_id;
            request.auth_context.realm_id = serving_realm;
            let stream_id = Ulid::from(9u128);
            let mut operation =
                IncomingBaoOperation::new(peer, local_node, test_realm(), stream_id, request);
            let effects = operation.start();
            if accepted {
                assert!(matches!(
                    effects.as_slice(),
                    [Effect::Storage(StorageEffect::Read { .. })]
                ));
            } else {
                assert_eq!(refusal_from(&effects), BaoReadRefusal::RealmPeerDenied);
            }
        }
    }

    #[test]
    fn denies_wire_read() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let hash = [4u8; 32];
        let mut operation = IncomingBaoOperation::new(
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
        assert_eq!(refusal_from(&effects), BaoReadRefusal::ReadDenied);
    }

    /// A request for one version of `set_id` by its logical identity.
    fn version_request(set_id: Ulid, hash: [u8; 32]) -> BaoReadRequest {
        let mut request = read_request(node_from_seed(1), hash);
        request.target = BaoReadTarget::Version {
            set_id,
            version_id: Ulid::from(4u128),
        };
        request
    }

    fn bound_bucket(
        binding: Option<aruna_core::structs::storage::distribution::DistributionBinding>,
    ) -> byteview::ByteView {
        let mut bucket = BucketInfo::from_bytes(&bucket_value()).unwrap();
        bucket.distribution = binding;
        bucket.to_bytes().unwrap().into()
    }

    // A logical version is found through the set's record and version id index in this node's
    // member bucket, then served like an exact version of that bucket.
    #[test]
    fn serves_logical_version() {
        use aruna_core::structs::storage::distribution::OriginalEpoch;
        use aruna_core::structs::storage::distribution::inventory::{
            IndexEntry, VersionEntry, version_position,
        };
        let (snapshot, record) = crate::distribution::test_record::record(true);
        let set_id = snapshot.set_id();
        let (local_node, peer) = (node_from_seed(1), node_from_seed(2));
        let hash = [4u8; 32];
        let path = aruna_core::structs::storage::blob::object_permission_path(
            test_realm(),
            Ulid::from(5u128),
            local_node,
            "spire",
            "path/file.txt",
        );
        let index = IndexEntry {
            position: version_position(
                set_id,
                "path/file.txt",
                OriginalEpoch(1),
                1,
                Ulid::from(4u128),
            ),
            version: VersionEntry::default(),
        };
        let start = || {
            let mut operation = IncomingBaoOperation::new(
                peer,
                local_node,
                test_realm(),
                Ulid::from(9u128),
                version_request(set_id, hash),
            )
            .with_policy_paths(HashSet::from([path.clone()]));
            operation.start();
            let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
                key: Vec::<u8>::new().into(),
                value: Some(realm_value(peer)),
            }));
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::BatchRead { reads, .. })] if reads.len() == 2
            ));
            let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
                values: vec![
                    (Vec::new().into(), Some(record.clone().into())),
                    (Vec::new().into(), Some(index.to_bytes().unwrap().into())),
                ],
            }));
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::Read { key, .. })] if key.as_ref() == b"spire"
            ));
            operation
        };

        // A bucket of the name that is not the set's member never answers for it.
        let mut stranger = start();
        let effects = stranger.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(bound_bucket(None)),
        }));
        assert_eq!(refusal_from(&effects), BaoReadRefusal::NotFound);

        let mut operation = start();
        let binding = crate::distribution::test_record::binding(&snapshot, 1, 5);
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(bound_bucket(Some(binding))),
        }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(version_value(hash)),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
    }

    // A node that keeps only the record of a distributed version serves its verified attachment.
    #[test]
    fn serves_kept_copy() {
        use aruna_core::structs::execution::source_access::SourceMetadata;
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        use aruna_core::structs::execution::staging::{
            PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
        };
        use aruna_core::structs::storage::distribution::on_use::Attachment;
        let (local_node, peer) = (node_from_seed(1), node_from_seed(2));
        let hash = [4u8; 32];
        let set_id = Ulid::from(8u128);
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::ArunaNative,
                public_config: HashMap::from([("set_id".to_string(), set_id.to_string())]),
                source_path: "bucket/path/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        let observed = SourceMetadata {
            content_length: 42,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let record_only = BlobVersion::reference(
            source,
            observed,
            SystemTime::UNIX_EPOCH,
            UserId::nil(test_realm()),
            SystemTime::UNIX_EPOCH,
        )
        .with_identity(Some(hash));
        let mut operation = IncomingBaoOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, hash),
        )
        .with_policy_paths(HashSet::from([read_path(local_node)]));
        operation.start();
        for value in [realm_value(peer), bucket_value()] {
            operation.step(Event::Storage(StorageEvent::ReadResult {
                key: Vec::<u8>::new().into(),
                value: Some(value),
            }));
        }
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(record_only.to_bytes().unwrap().into()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { reads, .. })] if reads.len() == 3
        ));
        let (location, _) = location_value(hash);
        let attached = Attachment {
            blake3: hash,
            location: location.clone(),
        };
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (Vec::new().into(), Some(attached.to_bytes().unwrap().into())),
                (Vec::new().into(), None),
            ],
        }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: None,
        }));
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected accepted frame")
        };
        assert!(matches!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::BaoReadAccepted { size: 42, blake3, .. } if blake3 == hash
        ));
    }

    // An exact read of a record kept without a copy names a member holding the range's bytes,
    // so a device can read the version there.
    #[test]
    fn names_byte_holder() {
        use aruna_core::structs::execution::source_access::SourceMetadata;
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        use aruna_core::structs::execution::staging::{
            PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
        };
        let (snapshot, record) = crate::distribution::test_record::record(true);
        let set_id = snapshot.set_id();
        let (local_node, peer) = (node_from_seed(1), node_from_seed(2));
        let hash = [4u8; 32];
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::ArunaNative,
                public_config: HashMap::from([("set_id".to_string(), set_id.to_string())]),
                source_path: "bucket/path/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        let observed = SourceMetadata {
            content_length: 42,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let record_only = BlobVersion::reference(
            source,
            observed,
            SystemTime::UNIX_EPOCH,
            UserId::nil(test_realm()),
            SystemTime::UNIX_EPOCH,
        )
        .with_identity(Some(hash));
        let mut operation = IncomingBaoOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, hash),
        )
        .with_policy_paths(HashSet::from([read_path(local_node)]));
        operation.start();
        for value in [realm_value(peer), bucket_value()] {
            operation.step(Event::Storage(StorageEvent::ReadResult {
                key: Vec::<u8>::new().into(),
                value: Some(value),
            }));
        }
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(record_only.to_bytes().unwrap().into()),
        }));
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (Vec::new().into(), None),
                (Vec::new().into(), None),
                (Vec::new().into(), Some(record.into())),
            ],
        }));
        let holder = crate::distribution::test_record::secret(2).public();
        assert_eq!(
            refusal_from(&effects),
            BaoReadRefusal::HeldBy { holder, set_id }
        );
    }

    // A holder serves its verified copy of an external reference by the copy's own BLAKE3: the
    // reference's identity digests its source observation, not its bytes.
    #[test]
    fn serves_kept_reference() {
        use aruna_core::structs::execution::source_access::SourceMetadata;
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        use aruna_core::structs::execution::staging::{
            PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
        };
        use aruna_core::structs::storage::distribution::on_use::CacheEntry;
        let (local_node, peer) = (node_from_seed(1), node_from_seed(2));
        let (identity, bytes) = ([6u8; 32], [4u8; 32]);
        let set_id = Ulid::from(8u128);
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::S3,
                public_config: HashMap::from([("bucket".to_string(), "reads".to_string())]),
                source_path: "path/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        let observed = SourceMetadata {
            content_length: 42,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let reference = BlobVersion::reference(
            source,
            observed,
            SystemTime::UNIX_EPOCH,
            UserId::nil(test_realm()),
            SystemTime::UNIX_EPOCH,
        )
        .with_identity(Some(identity));
        let mut request = version_request(set_id, bytes);
        request.expected_blake3 = None;
        let mut operation =
            IncomingBaoOperation::new(peer, local_node, test_realm(), Ulid::from(9u128), request);
        operation.policy_allowed = true;
        operation.located = Some(VersionedObjectArn {
            realm_id: test_realm(),
            node_id: local_node,
            bucket: "bucket".to_string(),
            key: "path/file.txt".to_string(),
            version: Ulid::from(4u128),
        });
        let held = operation
            .held_version(&reference)
            .expect("a kept reference");
        assert_eq!((held.identity, held.blake3), (identity, None));
        operation.read_kept(held);
        let (location, _) = location_value(bytes);
        let entry = CacheEntry {
            key: identity,
            blake3: bytes,
            location,
            size: 42,
            order: Ulid::from(3u128),
        };
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (Vec::new().into(), None),
                (Vec::new().into(), Some(entry.to_bytes().unwrap().into())),
            ],
        }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: None,
        }));
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected accepted frame")
        };
        assert!(matches!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::BaoReadAccepted { size: 42, blake3, .. } if blake3 == bytes
        ));
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
            IncomingBaoOperation::new(peer, local_node, test_realm(), stream_id, request)
                .with_policy_paths(HashSet::from([read_path(local_node)]));

        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(peer)),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(bucket_value()),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(version_value(hash)),
        }));
        let (location, value) = location_value(hash);
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(value),
        }));
        // The bucket has no encryption settings, so the plain copy is offered without a lease.
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: None,
        }));
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected accepted frame")
        };
        assert_eq!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::BaoReadAccepted {
                size: 42,
                blake3: hash,
                etag: location.hashes.get(HASH_MD5).map(hex::encode),
                hashes: location.hashes.clone().into_iter().collect(),
            }
        );

        let effects = operation.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ServeRead {
                stream_id: id,
                location: selected,
                expected_blake3,
                range: None,
            })] if *id == stream_id && selected == &location && expected_blake3 == &hash
        ));
        assert!(
            operation
                .step(Event::Blob(BlobEvent::ReadServed { stream_id }))
                .is_empty()
        );
        assert_eq!(operation.finalize().unwrap(), IncomingBaoResult::Served);
    }

    #[test]
    fn rejects_hash_mismatch() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let mut operation = IncomingBaoOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, [4u8; 32]),
        )
        .with_policy_paths(HashSet::from([read_path(local_node)]));

        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(peer)),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(bucket_value()),
        }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(version_value([5u8; 32])),
        }));

        assert_eq!(refusal_from(&effects), BaoReadRefusal::HashMismatch);
    }

    #[test]
    fn hash_requires_candidates() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let hash = [4u8; 32];
        let mut request = read_request(local_node, hash);
        request.target = BaoReadTarget::Blake3(hash);
        let mut operation =
            IncomingBaoOperation::new(peer, local_node, test_realm(), Ulid::from(9u128), request);

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(peer)),
        }));

        assert_eq!(refusal_from(&effects), BaoReadRefusal::BackendFailure);
    }

    /// An incoming read that resolved to a sealed copy of `bucket_id`, ready to be offered.
    fn sealed_serve(bucket_id: Ulid) -> (IncomingBaoOperation, BackendLocation) {
        use aruna_core::structs::storage::encryption::BucketKeyRef;
        use aruna_core::structs::storage::format::PithosLayout;

        let local_node = node_from_seed(1);
        let hash = [4u8; 32];
        let mut operation = IncomingBaoOperation::new(
            node_from_seed(2),
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, hash),
        );
        operation.blob_hash = Some(hash);
        let layout = PithosLayout {
            stored_size: 64,
            metadata_digest: [1; 32],
            storage_generation: 0,
        };
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "sealed".to_string(),
            ulid: Ulid::from(12u128),
            format: StoredFormat::pithos(layout, BucketKeyRef::new(bucket_id, 1)),
            created_by: UserId::nil(test_realm()),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 10,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), hash.to_vec())]),
        };
        (operation, location)
    }

    #[test]
    fn locked_copy_refused() {
        use aruna_core::errors::BlobError;
        use aruna_core::structs::storage::encryption::BucketKeyError;

        let bucket_id = Ulid::from(30u128);
        let (mut operation, location) = sealed_serve(bucket_id);
        let effects = operation.send_accepted(location);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::AdmitRead { key, .. })] if key.bucket_id == bucket_id
        ));
        let locked = BlobError::BucketKey(BucketKeyError::Locked(bucket_id));
        let effects = operation.step(Event::Blob(BlobEvent::Error(locked)));
        assert_eq!(
            refusal_from(&effects),
            BaoReadRefusal::BucketLocked(bucket_id)
        );
    }

    #[test]
    fn unlocked_copy_leased() {
        use aruna_core::structs::storage::blob::ArchiveKey;
        use aruna_core::structs::storage::encryption::{BucketKeyRef, ReadLease};
        use std::sync::Arc;

        let bucket_id = Ulid::from(30u128);
        let (mut operation, location) = sealed_serve(bucket_id);
        operation.send_accepted(location.clone());
        let key = BucketKeyRef::new(bucket_id, 1);
        let lease = ReadLease::new(
            key,
            ArchiveKey::of(&location),
            Ulid::from(31u128),
            Arc::new(()),
        );
        let effects = operation.step(Event::Blob(BlobEvent::ReadAdmitted { lease }));
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected the accepted frame, got {effects:?}")
        };
        assert!(matches!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::BaoReadAccepted { .. }
        ));
        let effects = operation.step(Event::Blob(BlobEvent::MessageSent {
            stream_id: Ulid::from(9u128),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ServeSealedRead { lease, .. })] if lease.key == key
        ));
    }

    #[test]
    fn unconverted_copy_refused() {
        use aruna_core::errors::BlobError;
        use aruna_core::structs::storage::blob::{ArchiveKey, VersionKey};
        use aruna_core::structs::storage::encryption::{
            BucketEncryption, BucketKeyError, BucketKeyRef, EncryptionMode, ReadLease,
        };
        use std::sync::Arc;

        let bucket_id = Ulid::from(30u128);
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(bucket_id),
            key_generation: 1,
            ..Default::default()
        };
        let settings_row = || {
            Event::Storage(StorageEvent::ReadResult {
                key: b"bucket".to_vec().into(),
                value: Some(settings.to_bytes().unwrap().into()),
            })
        };
        let plain = || {
            let (mut operation, mut location) = sealed_serve(bucket_id);
            location.format = StoredFormat::default();
            operation.version_key = Some(VersionKey::new(
                "bucket".to_string(),
                "path/file.txt".to_string(),
                Ulid::from(4u128),
            ));
            (operation, location)
        };
        let key = BucketKeyRef::new(bucket_id, 1);
        let archive = ArchiveKey::new(Ulid::from(12u128), BackendRef::node_default());

        // Locked: a raw copy not converted yet is refused like a sealed one.
        let (mut operation, location) = plain();
        let effects = operation.send_accepted(location);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        let effects = operation.step(settings_row());
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::AdmitRead { key: admitted, archive: named })]
                if *admitted == key && *named == archive
        ));
        let locked = BlobError::BucketKey(BucketKeyError::Locked(bucket_id));
        let effects = operation.step(Event::Blob(BlobEvent::Error(locked)));
        assert_eq!(
            refusal_from(&effects),
            BaoReadRefusal::BucketLocked(bucket_id)
        );

        // Unlocked: it is served under the bucket lease.
        let (mut operation, location) = plain();
        operation.send_accepted(location);
        operation.step(settings_row());
        let lease = ReadLease::new(key, archive, Ulid::from(31u128), Arc::new(()));
        operation.step(Event::Blob(BlobEvent::ReadAdmitted { lease }));
        let effects = operation.step(Event::Blob(BlobEvent::MessageSent {
            stream_id: Ulid::from(9u128),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ServeSealedRead { lease, .. })] if lease.key == key
        ));
    }

    #[test]
    fn locked_observation_refused() {
        use aruna_core::errors::BlobError;
        use aruna_core::events::SubOperationEvent;
        use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        use aruna_core::structs::storage::blob::ArchiveKey;
        use aruna_core::structs::storage::encryption::{
            BucketEncryption, BucketKeyError, BucketKeyRef, EncryptionMode, ReadLease,
        };
        use std::sync::Arc;

        let bucket_id = Ulid::from(40u128);
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(bucket_id),
            key_generation: 1,
            ..Default::default()
        };
        let resolved = || {
            let mut operation = device_serve(Some([5u8; 32]));
            operation.serve_observation(observation(5, Some("5-1")));
            let access = ResolvedSourceAccess::OpenDal {
                kind: SourceConnectorKind::Http,
                config: HashMap::new(),
                path: "file.txt".to_string(),
                version: None,
            };
            let effects = operation.step(Event::SubOperation(
                SubOperationEvent::VersionAccessResolved { result: Ok(access) },
            ));
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::Read { .. })]
            ));
            let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
                key: b"bucket".to_vec().into(),
                value: Some(settings.to_bytes().unwrap().into()),
            }));
            assert!(matches!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::AdmitRead { key, .. })] if key.bucket_id == bucket_id
            ));
            operation
        };

        // Locked: the device's file is refused before any accepted frame.
        let mut operation = resolved();
        let locked = BlobError::BucketKey(BucketKeyError::Locked(bucket_id));
        let effects = operation.step(Event::Blob(BlobEvent::Error(locked)));
        assert_eq!(
            refusal_from(&effects),
            BaoReadRefusal::BucketLocked(bucket_id)
        );

        // Unlocked: the file is served while the operation holds the bucket lease.
        let mut operation = resolved();
        let key = BucketKeyRef::new(bucket_id, 1);
        let archive = ArchiveKey::new(bucket_id, BackendRef::node_default());
        let guard = Arc::new(());
        let lease = ReadLease::new(key, archive, Ulid::from(41u128), guard.clone());
        operation.step(Event::Blob(BlobEvent::ReadAdmitted { lease }));
        let effects = operation.step(Event::Blob(BlobEvent::MessageSent {
            stream_id: Ulid::from(9u128),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ServeSourceRead { .. })]
        ));
        assert_eq!(Arc::strong_count(&guard), 2);
    }
}
