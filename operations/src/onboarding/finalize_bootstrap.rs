//! Finishes a node join: claims the secret, adds the node to the realm, issues a sync ticket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, NetEvent, TopicPeerError};
use aruna_core::onboarding::{
    OnboardingMode, OnboardingSecretError, OnboardingSecretRecord, OnboardingTicket,
};
use aruna_core::operation::Operation;
use aruna_core::placement::{holds_placement, target_placement_ref};
use aruna_core::rows::event;
use aruna_core::structs::execution::notification::ResourceEvent;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{
    METADATA_REPLICATION_FACTOR, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    RealmNodeKind,
};
use aruna_core::structs::placement::record::{
    NodePlacementEntry, PlacementRef, normalize_placement_input,
};
use aruna_core::structs::storage::node_info::reserved_label;
use ed25519_dalek::SigningKey;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::notifications::emit::{
    EmitNotificationsError, EmitNotificationsInput, EmitNotificationsOperation,
};
use crate::notifications::routing::{RoutingContext, route_resource_event};
use crate::notifications::watch::digest::{InterestError, InterestOperation};
use crate::onboarding::consume_secret::{
    ConsumeSecretError, ConsumeSecretInput, ConsumeSecretOperation, ConsumeSecretOutput,
};
use crate::onboarding::issue_ticket::{
    IssueSyncError, IssueSyncInput, IssueSyncOperation, TICKET_TTL_SECS,
};
use crate::onboarding::reserve_secret::{
    ReserveSecretError, ReserveSecretInput, ReserveSecretOperation,
};
use crate::placement::expand_placement::{
    EnsureActivatedOperation, ExpandPlacementOperation, mutate_call,
};
use crate::placement::process_placements::{
    PlacementReconcileOutcome, ReconcilePlacementsOperation,
};
use crate::realm::ensure_config::{EnsureConfigError, EnsureConfigOperation, EnsureConfigParams};
use crate::realm::mutate_placement::{MutatePlacementError, RealmPlacementMutation};
use crate::realm::read_authorization::{ReadAuthorizationError, ReadAuthorizationOperation};

const RESERVATION_TTL_SECS: u64 = 300;
const NODE_UPDATE_RETRIES: usize = 5;

#[derive(Clone, Debug, PartialEq)]
pub struct BootstrapFinalizeInput {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub node_id: NodeId,
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    pub realm_signing_key: SigningKey,
    pub now: u64,
    /// Joiner's placement location (`None` ⇒ realm default).
    pub node_location: Option<String>,
    /// Joiner's placement weight (`None` ⇒ default weight).
    pub node_weight: Option<u32>,
    /// Joiner's placement labels. Payload-sourced, so derived labels are
    /// rejected here (bypasses the config-parse rejection).
    pub node_labels: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapFinalizeOutput {
    pub mode: OnboardingMode,
    pub onboarding_sync_ticket: String,
}

#[derive(Debug, Error, PartialEq)]
pub enum BootstrapFinalizeError {
    #[error(transparent)]
    Reserve(#[from] ReserveSecretError),
    #[error(transparent)]
    EnsureRealmConfig(#[from] EnsureConfigError),
    #[error(transparent)]
    SetNodePlacement(#[from] MutatePlacementError),
    #[error(transparent)]
    GetRealmConfig(#[from] GetConfigError),
    #[error(transparent)]
    IssueTicket(#[from] IssueSyncError),
    #[error(transparent)]
    Consume(#[from] ConsumeSecretError),
    #[error(transparent)]
    OnboardingSecret(#[from] OnboardingSecretError),
    #[error("net handle unavailable")]
    NetHandleUnavailable,
    #[error("document sync peer admission failed: {0}")]
    PeerAdmission(String),
    #[error("placement labels must not set the derived label `{0}`")]
    ReservedNodeLabel(String),
    #[error("placement location must be at most 64 characters")]
    LongNodeLocation,
    #[error("ticketed user {0} has no placement")]
    UserPlacementUnavailable(UserId),
    #[error("onboarding step failed: {0}")]
    Step(String),
}

impl From<IoError> for BootstrapFinalizeError {
    fn from(error: IoError) -> Self {
        Self::Step(error.to_string())
    }
}

#[derive(Debug, PartialEq)]
enum FinalizeStage {
    Reserve,
    Activate,
    Node { attempts: usize },
    Interest,
    Joiner,
    Expand,
    Reconcile,
    Issue,
    Topics,
    Ensure(Vec<::irokle::TopicId>),
    Allow,
    Consume,
    Notify,
}

type FinalizeTurn = Result<Next<BootstrapFinalizeOutput>, BootstrapFinalizeError>;

fn topic_peers_done(result: IoResult) -> Result<(), BootstrapFinalizeError> {
    match event(result)? {
        Event::Net(NetEvent::TopicPeers(Ok(()))) => Ok(()),
        Event::Net(NetEvent::TopicPeers(Err(TopicPeerError::NoNetwork))) => {
            Err(BootstrapFinalizeError::NetHandleUnavailable)
        }
        Event::Net(NetEvent::TopicPeers(Err(TopicPeerError::Failed(error)))) => {
            Err(BootstrapFinalizeError::PeerAdmission(error))
        }
        other => Err(BootstrapFinalizeError::PeerAdmission(format!(
            "unexpected answer {}",
            other.label()
        ))),
    }
}

/// Finishes a join: reserves the secret, registers the node, starts placement expansion,
/// issues the sync ticket, admits the joiner to its topics, and consumes the secret last.
#[derive(Debug, PartialEq)]
pub struct FinalizeBootstrapOperation {
    input: BootstrapFinalizeInput,
    stage: FinalizeStage,
    entry: Option<NodePlacementEntry>,
    mode: Option<OnboardingMode>,
    ticket: Option<OnboardingTicket>,
    encoded_ticket: Option<String>,
}

impl FinalizeBootstrapOperation {
    pub fn new(input: BootstrapFinalizeInput) -> Self {
        Self {
            input,
            stage: FinalizeStage::Reserve,
            entry: None,
            mode: None,
            ticket: None,
            encoded_ticket: None,
        }
    }

    fn mode(&self) -> Result<OnboardingMode, BootstrapFinalizeError> {
        (self.mode.clone()).ok_or_else(|| BootstrapFinalizeError::Step("no reservation".into()))
    }

    fn ensure_node(&mut self, attempts: usize) -> FinalizeTurn {
        self.stage = FinalizeStage::Node { attempts };
        let input = &self.input;
        Ok(Next::Call(IoCall::child(EnsureConfigOperation::new(
            EnsureConfigParams {
                actor: realm_actor(input),
                target_node_id: input.node_id,
                target_node_kind: onboarding_node_kind(self.mode()?),
                metadata_replication_factor: METADATA_REPLICATION_FACTOR,
                realm_description: String::new(),
                create_if_missing: false,
                reject_kind_mismatch: true,
            },
        ))))
    }

    fn set_joiner(&mut self) -> FinalizeTurn {
        let entry = (self.entry.take())
            .ok_or_else(|| BootstrapFinalizeError::Step("no placement entry".into()))?;
        self.stage = FinalizeStage::Joiner;
        let upsert = RealmPlacementMutation::UpsertNode(entry);
        Ok(Next::Call(mutate_call(&realm_actor(&self.input), upsert)))
    }

    fn issue(&mut self) -> FinalizeTurn {
        self.stage = FinalizeStage::Issue;
        let input = &self.input;
        Ok(Next::Call(IoCall::child(IssueSyncOperation::new(
            IssueSyncInput {
                realm_signing_key: input.realm_signing_key.clone(),
                realm_id: input.realm_id,
                node_id: input.node_id,
                issuer_node_id: input.local_node_id,
                now: input.now,
                ttl_secs: TICKET_TTL_SECS,
            },
        ))))
    }

    /// A device carries no sync responsibility, so enrolling one must not add it
    /// to any topic every realm node then dials for the rest of its life.
    fn admit(&mut self, ticket: OnboardingTicket) -> FinalizeTurn {
        self.encoded_ticket = Some(ticket.encode()?);
        if !onboarding_node_kind(self.mode()?).is_sync_eligible() {
            return self.consume();
        }
        self.stage = FinalizeStage::Topics;
        let topics = SyncTopicsOperation::new(self.input.realm_id, self.input.node_id, &ticket);
        self.ticket = Some(ticket);
        Ok(Next::Call(IoCall::child(topics)))
    }

    /// The issuer may create shared topics. Only rank-zero holders create shard geneses,
    /// so the joiner becomes a shard member without minting.
    fn ensure_topics(&mut self, topics: OnboardingSyncTopics) -> FinalizeTurn {
        let mut all_topics = topics.shared.clone();
        all_topics.extend(topics.shard);
        self.stage = FinalizeStage::Ensure(all_topics);
        let effect = NetEffect::EnsureTopics {
            topics: topics.shared,
            peers: vec![self.input.node_id],
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Net(effect)))))
    }

    fn consume(&mut self) -> FinalizeTurn {
        self.stage = FinalizeStage::Consume;
        let input = &self.input;
        Ok(Next::Call(IoCall::child(ConsumeSecretOperation::new(
            ConsumeSecretInput {
                enrollment_id: input.enrollment_id,
                secret_hash: input.secret_hash.clone(),
                node_id: input.node_id.to_string(),
                now: input.now,
            },
        ))))
    }

    fn output(&mut self) -> FinalizeTurn {
        let onboarding_sync_ticket = (self.encoded_ticket.take())
            .ok_or_else(|| BootstrapFinalizeError::Step("no ticket".into()))?;
        Ok(Next::Done(BootstrapFinalizeOutput {
            mode: self.mode()?,
            onboarding_sync_ticket,
        }))
    }
}

impl Operation for FinalizeBootstrapOperation {
    type Output = BootstrapFinalizeOutput;
    type Error = BootstrapFinalizeError;

    fn first(&mut self) -> FinalizeTurn {
        self.entry = Some(build_joiner_entry(&self.input)?);
        let input = &self.input;
        Ok(Next::Call(IoCall::child(ReserveSecretOperation::new(
            ReserveSecretInput {
                enrollment_id: input.enrollment_id,
                secret_hash: input.secret_hash.clone(),
                node_id: input.node_id.to_string(),
                now: input.now,
                reservation_expires_at: input.now.saturating_add(RESERVATION_TTL_SECS),
                finalizing: true,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> FinalizeTurn {
        match std::mem::replace(&mut self.stage, FinalizeStage::Notify) {
            FinalizeStage::Reserve => {
                let reserved = result.child::<OnboardingSecretRecord, ReserveSecretError>()??;
                self.mode = Some(reserved.mode);
                // Close activation before registering the joiner so the transition below
                // moves buckets.
                self.stage = FinalizeStage::Activate;
                let activate = EnsureActivatedOperation::new(&realm_actor(&self.input));
                Ok(Next::Call(IoCall::child(activate)))
            }
            FinalizeStage::Activate => {
                result.child::<RealmConfigDocument, MutatePlacementError>()??;
                self.ensure_node(1)
            }
            FinalizeStage::Node { attempts } => {
                match result.child::<RealmConfigDocument, EnsureConfigError>()? {
                    Ok(_) => {
                        self.stage = FinalizeStage::Interest;
                        let mark = InterestOperation::mark(self.input.realm_id);
                        Ok(Next::Call(IoCall::child(mark)))
                    }
                    Err(EnsureConfigError::StorageError(StorageError::TransactionConflict))
                        if attempts < NODE_UPDATE_RETRIES =>
                    {
                        self.ensure_node(attempts + 1)
                    }
                    Err(error) => Err(error.into()),
                }
            }
            FinalizeStage::Interest => {
                if let Err(error) = result.child::<bool, InterestError>()? {
                    warn!(%error, "Failed to rebuild watch placement after onboarding realm change");
                }
                self.set_joiner()
            }
            FinalizeStage::Joiner => {
                result.child::<RealmConfigDocument, MutatePlacementError>()??;
                self.stage = FinalizeStage::Expand;
                let expand = ExpandPlacementOperation::new(&realm_actor(&self.input));
                Ok(Next::Call(IoCall::child(expand)))
            }
            FinalizeStage::Expand => {
                result.child::<Vec<Ulid>, MutatePlacementError>()??;
                // Expansion arms placement timers, keeping transition steps outside bootstrap.
                // Only topic membership is reconciled inline.
                self.stage = FinalizeStage::Reconcile;
                let input = &self.input;
                let reconcile =
                    ReconcilePlacementsOperation::new(input.realm_id, input.local_node_id, false);
                Ok(Next::Call(IoCall::child(reconcile)))
            }
            FinalizeStage::Reconcile => {
                result.child::<PlacementReconcileOutcome, IoError>()??;
                self.issue()
            }
            FinalizeStage::Issue => {
                let ticket = result.child::<OnboardingTicket, IssueSyncError>()??;
                self.admit(ticket)
            }
            FinalizeStage::Topics => {
                let topics = result.child::<OnboardingSyncTopics, BootstrapFinalizeError>()??;
                self.ensure_topics(topics)
            }
            FinalizeStage::Ensure(topics) => {
                topic_peers_done(result)?;
                self.stage = FinalizeStage::Allow;
                let effect = NetEffect::AllowTopicPeers {
                    topics,
                    peers: vec![self.input.node_id],
                };
                Ok(Next::Call(IoCall::Effect(Box::new(Effect::Net(effect)))))
            }
            FinalizeStage::Allow => {
                topic_peers_done(result)?;
                self.consume()
            }
            FinalizeStage::Consume => {
                let consumed = result.child::<ConsumeSecretOutput, ConsumeSecretError>()??;
                if !consumed.consumed_now {
                    return self.output();
                }
                self.stage = FinalizeStage::Notify;
                let notice = OnboardedNoticeOperation::new(self.input.realm_id, self.input.node_id);
                Ok(Next::Call(IoCall::child(notice)))
            }
            FinalizeStage::Notify => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Failed to emit onboarding notifications");
                }
                self.output()
            }
        }
    }
}

/// Tells realm admins that a node joined. A failure is logged and never fails the join.
#[derive(Debug, PartialEq)]
struct OnboardedNoticeOperation {
    realm_id: RealmId,
    node_id: NodeId,
    now_ms: u64,
    emitting: bool,
}

impl OnboardedNoticeOperation {
    fn new(realm_id: RealmId, node_id: NodeId) -> Self {
        Self {
            realm_id,
            node_id,
            now_ms: 0,
            emitting: false,
        }
    }
}

impl Operation for OnboardedNoticeOperation {
    type Output = ();
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let read = ReadAuthorizationOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.emitting {
            if let Err(error) = result.child::<(), EmitNotificationsError>()? {
                warn!(%error, "Failed to emit onboarding notifications");
            }
            return Ok(Next::Done(()));
        }
        let (realm_id, node_id) = (self.realm_id, self.node_id);
        let read = result.child::<Option<RealmAuthorizationDocument>, ReadAuthorizationError>()?;
        let realm_auth = match read {
            Ok(Some(document)) => document,
            Ok(None) => {
                warn!(realm_id = %realm_id, "Skipping node onboarding notification: no realm authorization document");
                return Ok(Next::Done(()));
            }
            Err(error) => {
                warn!(error = ?error, "Skipping node onboarding notification: realm authorization read failed");
                return Ok(Next::Done(()));
            }
        };
        let records = route_resource_event(
            &ResourceEvent::NodeOnboarded { realm_id, node_id },
            RoutingContext {
                group_auth: None,
                realm_auth: Some(&realm_auth),
                realm_id,
            },
            self.now_ms,
        );
        if records.is_empty() {
            return Ok(Next::Done(()));
        }
        self.emitting = true;
        let emit = EmitNotificationsOperation::new(EmitNotificationsInput { records });
        Ok(Next::Call(IoCall::child(emit)))
    }
}

/// Membership kind an enrollment mode grants the joiner.
fn onboarding_node_kind(mode: OnboardingMode) -> RealmNodeKind {
    match mode {
        OnboardingMode::Management => RealmNodeKind::Management,
        OnboardingMode::Server => RealmNodeKind::Server,
        OnboardingMode::User { owner } => RealmNodeKind::User { owner },
    }
}

fn build_joiner_entry(
    input: &BootstrapFinalizeInput,
) -> Result<NodePlacementEntry, BootstrapFinalizeError> {
    if let Some(label) = reserved_label(&input.node_labels) {
        return Err(BootstrapFinalizeError::ReservedNodeLabel(label.to_string()));
    }
    let (location, weight) =
        normalize_placement_input(input.node_location.as_deref(), input.node_weight)
            .map_err(|_| BootstrapFinalizeError::LongNodeLocation)?;

    Ok(NodePlacementEntry {
        node_id: input.node_id,
        location,
        weight,
        full: false,
        draining: false,
        labels: input.node_labels.clone(),
    })
}

fn realm_actor(input: &BootstrapFinalizeInput) -> Actor {
    Actor {
        node_id: input.local_node_id,
        user_id: UserId::nil(input.realm_id),
        realm_id: input.realm_id,
    }
}

#[derive(Debug, PartialEq)]
struct OnboardingSyncTopics {
    shared: Vec<::irokle::TopicId>,
    shard: Vec<::irokle::TopicId>,
}

/// Derives the sync topics for a ticket's documents so the issuer can add the joiner:
/// shared realm targets ignore placement, user documents ride their shard only if the
/// joiner holds it. Only shared topics may be created; shard topics are join-only.
#[derive(Debug, PartialEq)]
struct SyncTopicsOperation {
    realm_id: RealmId,
    joiner_node_id: NodeId,
    documents: Vec<DocumentTarget>,
}

impl SyncTopicsOperation {
    fn new(realm_id: RealmId, joiner_node_id: NodeId, ticket: &OnboardingTicket) -> Self {
        Self {
            realm_id,
            joiner_node_id,
            documents: ticket.payload.documents.clone(),
        }
    }

    fn topics(
        &self,
        config: &RealmConfigDocument,
    ) -> Result<OnboardingSyncTopics, BootstrapFinalizeError> {
        let realm_id = self.realm_id;
        let mut topics = OnboardingSyncTopics {
            shared: Vec::new(),
            shard: Vec::new(),
        };
        for document in &self.documents {
            if !document.uses_shard_topic() {
                let topic = document.sync_topic_id(realm_id, &PlacementRef::NIL);
                topics.shared.push(topic);
                continue;
            }
            let DocumentTarget::User { user_id } = document else {
                continue;
            };
            let placement = target_placement_ref(config, document, Default::default());
            if placement == PlacementRef::NIL {
                return Err(BootstrapFinalizeError::UserPlacementUnavailable(*user_id));
            }
            if holds_placement(config, &placement, self.joiner_node_id) {
                topics
                    .shard
                    .push(document.sync_topic_id(realm_id, &placement));
            }
        }
        Ok(topics)
    }
}

impl Operation for SyncTopicsOperation {
    type Output = OnboardingSyncTopics;
    type Error = BootstrapFinalizeError;

    fn first(&mut self) -> Result<Next<OnboardingSyncTopics>, BootstrapFinalizeError> {
        let read = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<OnboardingSyncTopics>, BootstrapFinalizeError> {
        let config = result.child::<RealmConfigDocument, GetConfigError>()??;
        Ok(Next::Done(self.topics(&config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BootstrapFinalizeError, BootstrapFinalizeInput, FinalizeBootstrapOperation,
        OnboardedNoticeOperation, SyncTopicsOperation, onboarding_node_kind,
    };
    use crate::document_repository::{GetConfigError, GetConfigOperation};
    use crate::driver::{DriverContext, drive};
    use crate::onboarding::create_secret::{CreateSecretInput, CreateSecretOperation};
    use crate::onboarding::reserve_secret::ReserveSecretError;
    use crate::onboarding::secret_state::secret_state_key;
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_core::NodeId;
    use aruna_core::UserId;
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, NOTIFICATION_OUTBOX_KEYSPACE, ONBOARDING_KEYSPACE};
    use aruna_core::onboarding::{
        OnboardingMode, OnboardingPurpose, OnboardingSecretRecord, OnboardingSecretState,
        OnboardingStateRecord, OnboardingTicket,
    };
    use aruna_core::structs::execution::notification::{
        NotificationKind, NotificationOutboxRecord,
    };
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmId, RealmNodeKind,
    };
    use aruna_core::structs::placement::record::{BindingScope, DocumentClass};
    use aruna_core::structs::storage::node_info::KIND_LABEL_KEY;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use ed25519_dalek::SigningKey;
    use irokle::Storage;
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    const LOCAL_NODE_SECRET: [u8; 32] = [4u8; 32];
    const SECRET_EXPIRES_AT: u64 = 1_000;

    #[test]
    fn kind_follows_mode() {
        // The admitter records the kind the secret was minted for; admitting a
        // management joiner as a server would 403 it on every config mutation.
        let owner = UserId::local(Ulid::generate(), RealmId::from_bytes([7u8; 32]));
        assert_eq!(
            onboarding_node_kind(OnboardingMode::Management),
            RealmNodeKind::Management
        );
        assert_eq!(
            onboarding_node_kind(OnboardingMode::Server),
            RealmNodeKind::Server
        );
        assert_eq!(
            onboarding_node_kind(OnboardingMode::User { owner }),
            RealmNodeKind::User { owner }
        );
    }

    struct FinalizeFixture {
        _tempdir: TempDir,
        storage_handle: storage::StorageHandle,
        context: Arc<DriverContext>,
        realm_signing_key: SigningKey,
        realm_id: RealmId,
        local_node_id: NodeId,
        joiner_node_id: NodeId,
        enrollment_id: Ulid,
    }

    async fn setup_finalize_fixture() -> FinalizeFixture {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        let realm_signing_key = SigningKey::from_bytes(&[3u8; 32]);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let local_node_id = iroh::SecretKey::from_bytes(&LOCAL_NODE_SECRET).public();
        let joiner_node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let user_id = UserId::local(Ulid::generate(), realm_id);

        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id: local_node_id,
                    user_id,
                    realm_id,
                },
                realm_description: "Realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            context.as_ref(),
        )
        .await
        .unwrap();

        let enrollment_id = Ulid::generate();
        drive(
            CreateSecretOperation::new(CreateSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id,
                    secret_hash: "abc".to_string(),
                    mode: OnboardingMode::Server,
                    purpose: OnboardingPurpose::NodeEnrollment,
                    expires_at: SECRET_EXPIRES_AT,
                    claimed_node_id: None,
                },
            }),
            context.as_ref(),
        )
        .await
        .unwrap();

        FinalizeFixture {
            _tempdir: tempdir,
            storage_handle,
            context,
            realm_signing_key,
            realm_id,
            local_node_id,
            joiner_node_id,
            enrollment_id,
        }
    }

    fn finalize_input(
        fixture: &FinalizeFixture,
        node_id: NodeId,
        now: u64,
    ) -> BootstrapFinalizeInput {
        BootstrapFinalizeInput {
            enrollment_id: fixture.enrollment_id,
            secret_hash: "abc".to_string(),
            node_id,
            realm_id: fixture.realm_id,
            local_node_id: fixture.local_node_id,
            realm_signing_key: fixture.realm_signing_key.clone(),
            now,
            node_location: Some("eu-central".to_string()),
            node_weight: Some(250),
            node_labels: Default::default(),
        }
    }

    async fn read_secret_state(
        storage_handle: &storage::StorageHandle,
        enrollment_id: Ulid,
    ) -> OnboardingSecretState {
        let state_value = match storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: ONBOARDING_KEYSPACE.to_string(),
                key: secret_state_key(enrollment_id),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(value), ..
            }) => value,
            other => panic!("unexpected state read result: {other:?}"),
        };
        let state_record: OnboardingStateRecord = postcard::from_bytes(&state_value).unwrap();
        state_record.state
    }

    async fn assert_realm_node(context: &DriverContext, realm_id: RealmId, node_id: NodeId) {
        let document = drive(GetConfigOperation::new(realm_id), context)
            .await
            .unwrap();
        assert!(
            document
                .nodes
                .iter()
                .any(|node| node.node_id == node_id.to_string())
        );
    }

    async fn assert_placement_entry(
        context: &DriverContext,
        realm_id: RealmId,
        node_id: NodeId,
        location: &str,
        weight: u32,
    ) {
        let document = drive(GetConfigOperation::new(realm_id), context)
            .await
            .unwrap();
        let entry = document
            .placement_entry(node_id)
            .expect("joiner placement entry set during finalize");
        assert_eq!(entry.location, location);
        assert_eq!(entry.weight, weight);
    }

    async fn assert_realm_excludes(context: &DriverContext, realm_id: RealmId, node_id: NodeId) {
        let document = drive(GetConfigOperation::new(realm_id), context)
            .await
            .unwrap();
        assert!(!document.has_node(node_id));
        assert!(document.placement_entry(node_id).is_none());
    }

    async fn context_with_net(fixture: &FinalizeFixture) -> (Arc<DriverContext>, NetHandle) {
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                secret_key: Some(iroh::SecretKey::from_bytes(&LOCAL_NODE_SECRET)),
                realm_id: fixture.realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            fixture.storage_handle.clone(),
        )
        .await
        .unwrap();

        (
            Arc::new(DriverContext {
                storage_handle: fixture.storage_handle.clone(),
                net_handle: Some(net_handle.clone()),
                blob_handle: None,
                metadata_handle: None,
                graph_handle: None,
                task_handle: None,
                compute_handle: None,
                outbound_handle: None,
            }),
            net_handle,
        )
    }

    async fn write_realm_admins(fixture: &FinalizeFixture, admins: &[UserId]) {
        let mut doc = RealmAuthorizationDocument::default_realm_doc(fixture.realm_id);
        for role in doc.roles.values_mut() {
            if role.name == "realm_admin" {
                role.assigned_users = admins.iter().copied().collect();
            }
        }
        let actor = Actor {
            node_id: fixture.local_node_id,
            user_id: UserId::nil(fixture.realm_id),
            realm_id: fixture.realm_id,
        };
        let value = doc.to_bytes(&actor).unwrap();
        fixture
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: (*fixture.realm_id.as_bytes()).into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
    }

    async fn clear_placement_strategies(fixture: &FinalizeFixture) {
        let mut config = drive(
            GetConfigOperation::new(fixture.realm_id),
            fixture.context.as_ref(),
        )
        .await
        .unwrap();
        config.strategies.clear();
        let actor = Actor {
            node_id: fixture.local_node_id,
            user_id: UserId::nil(fixture.realm_id),
            realm_id: fixture.realm_id,
        };
        let target = DocumentTarget::RealmConfig {
            realm_id: fixture.realm_id,
        };
        let event = fixture
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn read_outbox_rows(
        storage_handle: &storage::StorageHandle,
    ) -> Vec<NotificationOutboxRecord> {
        match storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| postcard::from_bytes(&value).unwrap())
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn failure_marks_finalizing() {
        let fixture = setup_finalize_fixture().await;

        let result = drive(
            FinalizeBootstrapOperation::new(finalize_input(&fixture, fixture.joiner_node_id, 10)),
            &fixture.context.clone(),
        )
        .await;
        assert_eq!(result, Err(BootstrapFinalizeError::NetHandleUnavailable));
        assert_realm_node(
            fixture.context.as_ref(),
            fixture.realm_id,
            fixture.joiner_node_id,
        )
        .await;
        // The joiner's location/weight from finalize_input landed in the
        // placement map next to its RealmConfigNodeEnsured membership.
        assert_placement_entry(
            fixture.context.as_ref(),
            fixture.realm_id,
            fixture.joiner_node_id,
            "eu-central",
            250,
        )
        .await;

        assert_eq!(
            read_secret_state(&fixture.storage_handle, fixture.enrollment_id).await,
            OnboardingSecretState::Finalizing {
                node_id: fixture.joiner_node_id.to_string(),
            }
        );
    }

    #[tokio::test]
    async fn finalizing_retry_completes() {
        let fixture = setup_finalize_fixture().await;

        let first = drive(
            FinalizeBootstrapOperation::new(finalize_input(&fixture, fixture.joiner_node_id, 10)),
            &fixture.context.clone(),
        )
        .await;
        assert_eq!(first, Err(BootstrapFinalizeError::NetHandleUnavailable));

        let (retry_context, net_handle) = context_with_net(&fixture).await;
        let retry = drive(
            FinalizeBootstrapOperation::new(finalize_input(
                &fixture,
                fixture.joiner_node_id,
                SECRET_EXPIRES_AT + 1,
            )),
            &retry_context,
        )
        .await
        .unwrap();
        assert_eq!(retry.mode, OnboardingMode::Server);
        assert!(!retry.onboarding_sync_ticket.is_empty());
        assert_eq!(
            read_secret_state(&fixture.storage_handle, fixture.enrollment_id).await,
            OnboardingSecretState::Consumed {
                node_id: fixture.joiner_node_id.to_string(),
            }
        );
        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn ticket_admits_issuer() {
        let fixture = setup_finalize_fixture().await;
        let (context, net_handle) = context_with_net(&fixture).await;

        let result = drive(
            FinalizeBootstrapOperation::new(finalize_input(&fixture, fixture.joiner_node_id, 10)),
            &context,
        )
        .await
        .unwrap();
        let target = DocumentTarget::NodeInfo {
            realm_id: fixture.realm_id,
            node_id: fixture.local_node_id,
        };
        let ticket = OnboardingTicket::decode(&result.onboarding_sync_ticket).unwrap();

        ticket.verify(fixture.joiner_node_id, &target, 10).unwrap();
        let state = net_handle
            .document_sync_node()
            .storage()
            .topic_state(&target.sync_topic_id(
                fixture.realm_id,
                &aruna_core::structs::placement::record::PlacementRef::NIL,
            ))
            .unwrap()
            .expect("issuer node-info topic admitted during finalize");
        assert!(state.members.contains(&irokle::PeerId::from_bytes(
            *fixture.joiner_node_id.as_bytes()
        )));

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn device_skips_topics() {
        // A User-kind joiner is not sync eligible, so finalize must neither
        // create a shared realm topic for it nor admit it to one.
        let fixture = setup_finalize_fixture().await;
        let (context, net_handle) = context_with_net(&fixture).await;
        let owner = UserId::local(Ulid::generate(), fixture.realm_id);
        let device_enrollment = Ulid::generate();
        drive(
            CreateSecretOperation::new(CreateSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id: device_enrollment,
                    secret_hash: "device".to_string(),
                    mode: OnboardingMode::User { owner },
                    purpose: OnboardingPurpose::NodeEnrollment,
                    expires_at: SECRET_EXPIRES_AT,
                    claimed_node_id: None,
                },
            }),
            context.as_ref(),
        )
        .await
        .unwrap();

        let mut device_input = finalize_input(&fixture, fixture.joiner_node_id, 10);
        device_input.enrollment_id = device_enrollment;
        device_input.secret_hash = "device".to_string();
        let device = drive(
            FinalizeBootstrapOperation::new(device_input),
            &context.clone(),
        )
        .await
        .unwrap();
        assert_eq!(device.mode, OnboardingMode::User { owner });

        let shared_topic = DocumentTarget::NodeInfo {
            realm_id: fixture.realm_id,
            node_id: fixture.local_node_id,
        }
        .sync_topic_id(
            fixture.realm_id,
            &aruna_core::structs::placement::record::PlacementRef::NIL,
        );
        let storage = net_handle.document_sync_node().storage().clone();
        assert!(storage.topic_state(&shared_topic).unwrap().is_none());

        let server_node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let server = drive(
            FinalizeBootstrapOperation::new(finalize_input(&fixture, server_node_id, 10)),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(server.mode, OnboardingMode::Server);
        let state = storage
            .topic_state(&shared_topic)
            .unwrap()
            .expect("a server joiner still gets the shared topics");
        assert!(
            state
                .members
                .contains(&irokle::PeerId::from_bytes(*server_node_id.as_bytes()))
        );
        assert!(!state.members.contains(&irokle::PeerId::from_bytes(
            *fixture.joiner_node_id.as_bytes()
        )));

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn topic_errors_propagate() {
        let fixture = setup_finalize_fixture().await;
        clear_placement_strategies(&fixture).await;
        let user_id = UserId::local(Ulid::from_bytes([44u8; 16]), fixture.realm_id);
        let ticket = OnboardingTicket::issue(
            &fixture.realm_signing_key,
            &fixture.realm_id,
            fixture.joiner_node_id,
            100,
            vec![DocumentTarget::User { user_id }],
        )
        .unwrap();

        assert!(matches!(
            drive(SyncTopicsOperation::new(fixture.realm_id, fixture.joiner_node_id, &ticket), &fixture.context)
            .await,
            Err(BootstrapFinalizeError::UserPlacementUnavailable(id)) if id == user_id
        ));

        let missing_realm_id = RealmId::from_bytes([9u8; 32]);
        let missing_ticket = OnboardingTicket::issue(
            &fixture.realm_signing_key,
            &missing_realm_id,
            fixture.joiner_node_id,
            100,
            Vec::new(),
        )
        .unwrap();
        assert!(matches!(
            drive(
                SyncTopicsOperation::new(missing_realm_id, fixture.joiner_node_id, &missing_ticket),
                &fixture.context
            )
            .await,
            Err(BootstrapFinalizeError::GetRealmConfig(
                GetConfigError::DocumentNotFound
            ))
        ));
    }

    #[tokio::test]
    async fn nonholder_shard_excluded() {
        let fixture = setup_finalize_fixture().await;
        let mut config = drive(
            GetConfigOperation::new(fixture.realm_id),
            fixture.context.as_ref(),
        )
        .await
        .unwrap();
        config.ensure_node(fixture.joiner_node_id, RealmNodeKind::Server);
        let user_strategy_id = config
            .strategy_bindings
            .iter()
            .find(|binding| binding.scope == BindingScope::Class(DocumentClass::User))
            .unwrap()
            .strategy_id;
        config
            .strategies
            .iter_mut()
            .find(|strategy| strategy.strategy_id == user_strategy_id)
            .unwrap()
            .replica_count = Some(1);
        // Stand in for the expansion transition: a node registered after the
        // realm activated its map holds nothing until one names it.
        config.candidate_maps.clear();
        config.placement_activations.clear();
        config.snapshot_candidate_map();

        let actor = Actor {
            node_id: fixture.local_node_id,
            user_id: UserId::nil(fixture.realm_id),
            realm_id: fixture.realm_id,
        };
        let config_target = DocumentTarget::RealmConfig {
            realm_id: fixture.realm_id,
        };
        let event = fixture
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: config_target.storage_keyspace().to_string(),
                key: config_target.storage_key(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let mut held = None;
        let mut nonholder = None;
        for seed in 0u16..=u16::MAX {
            let mut bytes = [0u8; 16];
            bytes[..2].copy_from_slice(&seed.to_be_bytes());
            let target = DocumentTarget::User {
                user_id: UserId::local(Ulid::from_bytes(bytes), fixture.realm_id),
            };
            let placement =
                aruna_core::placement::target_placement_ref(&config, &target, Default::default());
            if aruna_core::placement::holds_placement(&config, &placement, fixture.joiner_node_id) {
                held.get_or_insert(target);
            } else {
                nonholder.get_or_insert(target);
            }
            if held.is_some() && nonholder.is_some() {
                break;
            }
        }
        let held = held.expect("joiner holds a capped user shard");
        let nonholder = nonholder.expect("joiner does not hold every capped user shard");
        let held_placement =
            aruna_core::placement::target_placement_ref(&config, &held, Default::default());
        let ticket = OnboardingTicket::issue(
            &fixture.realm_signing_key,
            &fixture.realm_id,
            fixture.joiner_node_id,
            100,
            vec![nonholder, held.clone()],
        )
        .unwrap();

        let topics = drive(
            SyncTopicsOperation::new(fixture.realm_id, fixture.joiner_node_id, &ticket),
            &fixture.context,
        )
        .await
        .unwrap();

        assert_eq!(
            topics.shard,
            vec![held.sync_topic_id(fixture.realm_id, &held_placement)]
        );
    }

    #[tokio::test]
    async fn other_node_rejected() {
        let fixture = setup_finalize_fixture().await;

        let first = drive(
            FinalizeBootstrapOperation::new(finalize_input(&fixture, fixture.joiner_node_id, 10)),
            &fixture.context.clone(),
        )
        .await;
        assert_eq!(first, Err(BootstrapFinalizeError::NetHandleUnavailable));

        let other_node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let rejected = drive(
            FinalizeBootstrapOperation::new(finalize_input(
                &fixture,
                other_node_id,
                SECRET_EXPIRES_AT + 1,
            )),
            &fixture.context.clone(),
        )
        .await;
        assert_eq!(
            rejected,
            Err(BootstrapFinalizeError::Reserve(
                ReserveSecretError::AlreadyClaimed
            ))
        );
    }

    #[tokio::test]
    async fn onboarding_emits_admins() {
        let fixture = setup_finalize_fixture().await;
        let admin_one = UserId::new(Ulid::from_bytes([40u8; 16]), fixture.realm_id);
        let admin_two = UserId::new(Ulid::from_bytes([41u8; 16]), fixture.realm_id);
        write_realm_admins(&fixture, &[admin_one, admin_two]).await;

        let first = drive(
            FinalizeBootstrapOperation::new(finalize_input(&fixture, fixture.joiner_node_id, 10)),
            &fixture.context.clone(),
        )
        .await;
        assert_eq!(first, Err(BootstrapFinalizeError::NetHandleUnavailable));

        let (retry_context, net_handle) = context_with_net(&fixture).await;
        let retry = drive(
            FinalizeBootstrapOperation::new(finalize_input(
                &fixture,
                fixture.joiner_node_id,
                SECRET_EXPIRES_AT + 1,
            )),
            &retry_context,
        )
        .await
        .unwrap();
        assert_eq!(retry.mode, OnboardingMode::Server);

        let rows = read_outbox_rows(&fixture.storage_handle).await;
        assert_eq!(rows.len(), 2);
        let mut recipients: Vec<UserId> = rows.iter().map(|row| row.record.recipient).collect();
        recipients.sort();
        let mut expected = vec![admin_one, admin_two];
        expected.sort();
        assert_eq!(recipients, expected);
        for row in &rows {
            assert_eq!(
                row.record.kind,
                NotificationKind::NodeOnboarded {
                    realm_id: fixture.realm_id,
                    node_id: fixture.joiner_node_id,
                }
            );
        }
        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn consumed_retry_once() {
        let fixture = setup_finalize_fixture().await;
        let admin_one = UserId::new(Ulid::from_bytes([42u8; 16]), fixture.realm_id);
        let admin_two = UserId::new(Ulid::from_bytes([43u8; 16]), fixture.realm_id);
        write_realm_admins(&fixture, &[admin_one, admin_two]).await;

        let (context, net_handle) = context_with_net(&fixture).await;
        drive(
            FinalizeBootstrapOperation::new(finalize_input(&fixture, fixture.joiner_node_id, 10)),
            &context.clone(),
        )
        .await
        .unwrap();
        drive(
            FinalizeBootstrapOperation::new(finalize_input(
                &fixture,
                fixture.joiner_node_id,
                SECRET_EXPIRES_AT + 1,
            )),
            &context,
        )
        .await
        .unwrap();

        let rows = read_outbox_rows(&fixture.storage_handle).await;
        assert_eq!(rows.len(), 2);
        let mut recipients: Vec<UserId> = rows.iter().map(|row| row.record.recipient).collect();
        recipients.sort();
        let mut expected = vec![admin_one, admin_two];
        expected.sort();
        assert_eq!(recipients, expected);

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn emit_warns_absent() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[7u8; 32]).public();

        drive(OnboardedNoticeOperation::new(realm_id, node_id), &context)
            .await
            .unwrap();

        assert!(read_outbox_rows(&storage_handle).await.is_empty());
    }

    #[tokio::test]
    async fn onboarding_without_admin() {
        let fixture = setup_finalize_fixture().await;

        let first = drive(
            FinalizeBootstrapOperation::new(finalize_input(&fixture, fixture.joiner_node_id, 10)),
            &fixture.context.clone(),
        )
        .await;
        assert_eq!(first, Err(BootstrapFinalizeError::NetHandleUnavailable));

        let (retry_context, net_handle) = context_with_net(&fixture).await;
        let retry = drive(
            FinalizeBootstrapOperation::new(finalize_input(
                &fixture,
                fixture.joiner_node_id,
                SECRET_EXPIRES_AT + 1,
            )),
            &retry_context,
        )
        .await
        .unwrap();
        assert_eq!(retry.mode, OnboardingMode::Server);

        assert!(read_outbox_rows(&fixture.storage_handle).await.is_empty());
        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn failed_finalize_silent() {
        let fixture = setup_finalize_fixture().await;

        let result = drive(
            FinalizeBootstrapOperation::new(finalize_input(&fixture, fixture.joiner_node_id, 10)),
            &fixture.context.clone(),
        )
        .await;
        assert_eq!(result, Err(BootstrapFinalizeError::NetHandleUnavailable));

        assert!(read_outbox_rows(&fixture.storage_handle).await.is_empty());
    }

    #[tokio::test]
    async fn finalize_rejects_label() {
        let fixture = setup_finalize_fixture().await;
        let mut input = finalize_input(&fixture, fixture.joiner_node_id, 10);
        input
            .node_labels
            .insert(KIND_LABEL_KEY.to_string(), "Server".to_string());

        let result = drive(
            FinalizeBootstrapOperation::new(input),
            &fixture.context.clone(),
        )
        .await;
        assert_eq!(
            result,
            Err(BootstrapFinalizeError::ReservedNodeLabel(
                KIND_LABEL_KEY.to_string()
            ))
        );
        assert_eq!(
            read_secret_state(&fixture.storage_handle, fixture.enrollment_id).await,
            OnboardingSecretState::Available
        );
        assert_realm_excludes(
            fixture.context.as_ref(),
            fixture.realm_id,
            fixture.joiner_node_id,
        )
        .await;
    }

    #[tokio::test]
    async fn finalize_rejects_location() {
        let fixture = setup_finalize_fixture().await;
        let mut input = finalize_input(&fixture, fixture.joiner_node_id, 10);
        input.node_location = Some("x".repeat(65));

        let result = drive(
            FinalizeBootstrapOperation::new(input),
            &fixture.context.clone(),
        )
        .await;
        assert_eq!(result, Err(BootstrapFinalizeError::LongNodeLocation));
        assert_eq!(
            read_secret_state(&fixture.storage_handle, fixture.enrollment_id).await,
            OnboardingSecretState::Available
        );
        assert_realm_excludes(
            fixture.context.as_ref(),
            fixture.realm_id,
            fixture.joiner_node_id,
        )
        .await;
    }
}

#[cfg(test)]
#[path = "finalize_tests.rs"]
mod call_tests;
