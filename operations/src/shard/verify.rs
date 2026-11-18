//! Reconciles a new holder's shard digest against a co-holder and stores the verification row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{ShardManifest, shard_topic_id};
use aruna_core::effects::{Effect, EffectGroup, GroupForm, ShardNetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult, ShardNetEvent};
use aruna_core::keyspaces::SHARD_VERIFICATION_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::placement::resolve_shard_holders;
use aruna_core::rows::{PrefixPages, done, encoded_row, read_row, value};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::Key;
use aruna_net::shard_manifest::manifest_entry_digest;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::{
    AssembleManifestOperation, ManifestError, client::FetchManifestOperation, shard_answer,
    shard_call,
};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::shard::pull::PullTopicsOperation;
use crate::sync::shard_placement::{placement_key, placement_prefix};

/// Page size for scanning persisted shard verification markers.
const SHARD_PAGE_SIZE: usize = 256;

/// A new holder retries digest reconciliation this many times against the first
/// reachable co-holder before leaving it unverified for the next pass.
pub const VERIFICATION_MAX_ATTEMPTS: usize = 3;

/// Limits startup verification work while allowing independent shards to make
/// progress concurrently. Matches the existing bounded metadata fanout.
const VERIFICATION_CONCURRENCY_LIMIT: usize = 8;

/// Persisted proof that the local node reconciled a shard against a co-holder.
/// Presence of the row (keyed like a pending placement) means verified, so a
/// restart resumes only shards without one. One-shot signal, not a guarantee.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardVerificationRecord {
    pub placement: PlacementRef,
    pub verified_against: Option<NodeId>,
    pub digest: [u8; 32],
    pub verified_at_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShardVerificationSummary {
    pub held: usize,
    pub already_verified: usize,
    pub newly_verified: usize,
    pub unverified: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShardVerdict {
    Known,
    Verified,
    Open,
}

fn verification_key(realm_id: RealmId, placement: &PlacementRef) -> Key {
    placement_key(realm_id, placement)
}

/// The shards this node holds with their co-holders in rank order, so the first
/// reachable co-holder is the highest-ranked one.
fn held_shards(config: &RealmConfigDocument, node_id: NodeId) -> Vec<(PlacementRef, Vec<NodeId>)> {
    (config.strategies.iter())
        .flat_map(|strategy| {
            (0..strategy.shard_count).filter_map(move |shard| {
                let placement = PlacementRef {
                    strategy_id: strategy.strategy_id,
                    shard,
                };
                let holders = resolve_shard_holders(config, &placement);
                holders.contains(&node_id).then(|| {
                    let others = holders.into_iter().filter(|peer| *peer != node_id);
                    (placement, others.collect())
                })
            })
        })
        .collect()
}

/// Reconciles every newly held shard against the first reachable co-holder in
/// rank order, comparing digest and entries, then retrying via anti-entropy.
/// Verified markers skip, so it is idempotent across restarts.
#[derive(Debug, PartialEq)]
pub struct VerifyHeldOperation {
    node_id: NodeId,
    realm_id: RealmId,
    verifying: bool,
}

impl VerifyHeldOperation {
    pub fn new(node_id: NodeId, realm_id: RealmId) -> Self {
        Self {
            node_id,
            realm_id,
            verifying: false,
        }
    }

    fn verify_all(&mut self, config: &RealmConfigDocument) -> Next<ShardVerificationSummary> {
        let shards = held_shards(config, self.node_id);
        if shards.is_empty() {
            return Next::Done(ShardVerificationSummary::default());
        }
        self.verifying = true;
        let members = (shards.into_iter())
            .map(|(placement, co_holders)| {
                let verify = VerifyShardOperation::new(self.realm_id, placement, co_holders);
                Effect::SubOperation(boxed_suboperation(verify, |result| {
                    Event::Child(ChildOutcome::new(result))
                }))
            })
            .collect();
        Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: VERIFICATION_CONCURRENCY_LIMIT,
            form: GroupForm::All { deadline: None },
        }))
    }
}

impl Operation for VerifyHeldOperation {
    type Output = ShardVerificationSummary;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<ShardVerificationSummary>, IoError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ShardVerificationSummary>, IoError> {
        if !self.verifying {
            return Ok(
                match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => self.verify_all(&config),
                    Err(_) => Next::Done(ShardVerificationSummary::default()),
                },
            );
        }
        let IoResult::Group(GroupEvent::Finished { results }) = result else {
            return Err(IoError::Unexpected {
                expected: "the verification group",
                got: result.label(),
            });
        };
        let mut summary = ShardVerificationSummary::default();
        for member in results {
            summary.held += 1;
            let verdict = match member {
                GroupResult::Done(event) => match *event {
                    Event::Child(outcome) => IoResult::Child(outcome)
                        .child::<ShardVerdict, IoError>()?
                        .unwrap_or(ShardVerdict::Open),
                    _ => ShardVerdict::Open,
                },
                _ => ShardVerdict::Open,
            };
            match verdict {
                ShardVerdict::Known => summary.already_verified += 1,
                ShardVerdict::Verified => summary.newly_verified += 1,
                ShardVerdict::Open => summary.unverified += 1,
            }
        }
        Ok(Next::Done(summary))
    }
}

#[derive(Debug, PartialEq)]
enum VerifyStage {
    Known,
    SoleExists,
    SoleManifest,
    Converge,
    Mark,
}

type VerifyTurn = Result<Next<ShardVerdict>, IoError>;

/// Verifies one shard unless a marker says it already was.
#[derive(Debug, PartialEq)]
struct VerifyShardOperation {
    realm_id: RealmId,
    placement: PlacementRef,
    co_holders: Vec<NodeId>,
    now_ms: u64,
    stage: VerifyStage,
    against: Option<NodeId>,
}

impl VerifyShardOperation {
    fn new(realm_id: RealmId, placement: PlacementRef, co_holders: Vec<NodeId>) -> Self {
        Self {
            realm_id,
            placement,
            co_holders,
            now_ms: 0,
            stage: VerifyStage::Known,
            against: None,
        }
    }

    fn topic(&self) -> irokle::TopicId {
        shard_topic_id(self.realm_id, &self.placement)
    }

    fn mark(&mut self, digest: [u8; 32]) -> VerifyTurn {
        let record = ShardVerificationRecord {
            placement: self.placement,
            verified_against: self.against,
            digest,
            verified_at_ms: self.now_ms,
        };
        self.stage = VerifyStage::Mark;
        let mark = MarkVerifiedOperation {
            key: verification_key(self.realm_id, &self.placement),
            record,
        };
        Ok(Next::Call(IoCall::child(mark)))
    }

    fn verdict(&self) -> ShardVerdict {
        match self.against {
            Some(co_holder) => info!(
                strategy = %self.placement.strategy_id,
                shard = self.placement.shard,
                co_holder = %co_holder,
                "Verified shard against co-holder",
            ),
            None => info!(
                strategy = %self.placement.strategy_id,
                shard = self.placement.shard,
                "Verified sole-holder shard",
            ),
        }
        ShardVerdict::Verified
    }
}

impl Operation for VerifyShardOperation {
    type Output = ShardVerdict;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> VerifyTurn {
        let key = verification_key(self.realm_id, &self.placement);
        Ok(Next::Call(read_row(SHARD_VERIFICATION_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> VerifyTurn {
        match self.stage {
            VerifyStage::Known => {
                if value(result)?.is_some() {
                    return Ok(Next::Done(ShardVerdict::Known));
                }
                // A sole holder converges only after its genesis exists: a genesis-less
                // topic has an empty fingerprint, so the digest alone is insufficient.
                if self.co_holders.is_empty() {
                    self.stage = VerifyStage::SoleExists;
                    let topics = vec![self.topic()];
                    return Ok(Next::Call(shard_call(ShardNetEffect::Existing(topics))));
                }
                self.stage = VerifyStage::Converge;
                let converge =
                    ConvergeOperation::new(self.realm_id, self.placement, &self.co_holders, None);
                Ok(Next::Call(IoCall::child(converge)))
            }
            VerifyStage::SoleExists => match shard_answer(result)? {
                ShardNetEvent::Existing(topics) if topics.contains(&self.topic()) => {
                    self.stage = VerifyStage::SoleManifest;
                    let assemble = AssembleManifestOperation::new(self.realm_id, self.placement);
                    Ok(Next::Call(IoCall::child(assemble)))
                }
                _ => {
                    debug!(
                        strategy = %self.placement.strategy_id,
                        shard = self.placement.shard,
                        "Sole-holder shard has no local genesis yet; deferring verification"
                    );
                    Ok(Next::Done(ShardVerdict::Open))
                }
            },
            VerifyStage::SoleManifest => match result.child::<ShardManifest, ManifestError>()? {
                Ok(manifest) => self.mark(manifest.digest),
                Err(error) => {
                    warn!(error = %error, "Failed to assemble sole-holder shard manifest for verification");
                    Ok(Next::Done(ShardVerdict::Open))
                }
            },
            VerifyStage::Converge => {
                match result.child::<Option<(NodeId, [u8; 32])>, IoError>()?? {
                    Some((co_holder, digest)) => {
                        self.against = Some(co_holder);
                        self.mark(digest)
                    }
                    None => Ok(Next::Done(ShardVerdict::Open)),
                }
            }
            VerifyStage::Mark => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(error = %error, "Failed to persist shard verification marker");
                }
                Ok(Next::Done(self.verdict()))
            }
        }
    }
}

/// Writes one verification marker.
#[derive(Debug, PartialEq)]
struct MarkVerifiedOperation {
    key: Key,
    record: ShardVerificationRecord,
}

impl Operation for MarkVerifiedOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let key = self.key.clone();
        let row = encoded_row(SHARD_VERIFICATION_KEYSPACE, key, &self.record)?;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        done(result)?;
        Ok(Next::Done(()))
    }
}

/// Whether the local cursor covers the required barrier join. Vacuous without
/// a requirement; an undecodable local cursor never satisfies one.
fn dominates_required(cursor: &[u8], required: Option<&irokle::ActorClock>) -> bool {
    let Some(required) = required else {
        return true;
    };
    postcard::from_bytes::<irokle::ActorClock>(cursor).is_ok_and(|local| local.dominates(required))
}

fn manifests_converged(local: &ShardManifest, remote: &ShardManifest) -> bool {
    local.digest == remote.digest
        && manifest_entry_digest(&local.entries) == manifest_entry_digest(&remote.entries)
}

#[derive(Debug, PartialEq)]
enum ConvergeStage {
    Fetch,
    Local,
    Exists(Box<ShardManifest>),
    Pull,
    Refetch,
}

type ConvergeOutput = Option<(NodeId, [u8; 32])>;
type ConvergeTurn = Result<Next<ConvergeOutput>, IoError>;

/// Reconciles the local shard copy against the first reachable co-holder, returning it with
/// the digest a completion proof commits to, or `None`. With `required`, the local cursor must
/// also dominate that frontier, so a target cannot prove while missing a holder's writes.
#[derive(Debug, PartialEq)]
pub(crate) struct ConvergeOperation {
    realm_id: RealmId,
    placement: PlacementRef,
    /// Reversed, so the next co-holder pops off the end.
    co_holders: Vec<NodeId>,
    required: Option<irokle::ActorClock>,
    holder: Option<NodeId>,
    remote: Option<ShardManifest>,
    attempts: usize,
    stage: ConvergeStage,
}

impl ConvergeOperation {
    pub(crate) fn new(
        realm_id: RealmId,
        placement: PlacementRef,
        co_holders: &[NodeId],
        required: Option<irokle::ActorClock>,
    ) -> Self {
        Self {
            realm_id,
            placement,
            co_holders: co_holders.iter().rev().copied().collect(),
            required,
            holder: None,
            remote: None,
            attempts: 0,
            stage: ConvergeStage::Fetch,
        }
    }

    fn topic(&self) -> irokle::TopicId {
        shard_topic_id(self.realm_id, &self.placement)
    }

    fn fetch(&self, peer: NodeId) -> IoCall {
        IoCall::child(FetchManifestOperation::new(
            peer,
            self.realm_id,
            self.placement,
        ))
    }

    fn next_holder(&mut self) -> ConvergeTurn {
        let Some(peer) = self.co_holders.pop() else {
            return Ok(Next::Done(None));
        };
        self.holder = Some(peer);
        self.attempts = 0;
        self.stage = ConvergeStage::Fetch;
        Ok(Next::Call(self.fetch(peer)))
    }

    fn assemble(&mut self, remote: ShardManifest) -> ConvergeTurn {
        self.remote = Some(remote);
        self.stage = ConvergeStage::Local;
        let assemble = AssembleManifestOperation::new(self.realm_id, self.placement);
        Ok(Next::Call(IoCall::child(assemble)))
    }

    fn holder(&self) -> Result<NodeId, IoError> {
        self.holder.ok_or(IoError::Unexpected {
            expected: "a co-holder",
            got: "none",
        })
    }
}

impl Operation for ConvergeOperation {
    type Output = ConvergeOutput;
    type Error = IoError;

    fn first(&mut self) -> ConvergeTurn {
        self.next_holder()
    }

    fn next(&mut self, result: IoResult) -> ConvergeTurn {
        let holder = self.holder()?;
        match std::mem::replace(&mut self.stage, ConvergeStage::Pull) {
            ConvergeStage::Fetch => match result.child::<ShardManifest, ManifestError>()? {
                Ok(remote) => self.assemble(remote),
                Err(error) => {
                    debug!(
                        co_holder = %holder,
                        error = %error,
                        "Shard manifest fetch failed; trying next co-holder"
                    );
                    self.next_holder()
                }
            },
            ConvergeStage::Local => match result.child::<ShardManifest, ManifestError>()? {
                Ok(local) => {
                    self.stage = ConvergeStage::Exists(Box::new(local));
                    let topics = vec![self.topic()];
                    Ok(Next::Call(shard_call(ShardNetEffect::Existing(topics))))
                }
                Err(error) => {
                    warn!(error = %error, "Failed to assemble local shard manifest for verification");
                    Ok(Next::Done(None))
                }
            },
            // Genesis-less holders share the empty fingerprint, so a local genesis
            // must exist before manifests are compared.
            ConvergeStage::Exists(local) => {
                let exists = matches!(
                    shard_answer(result)?,
                    ShardNetEvent::Existing(topics) if topics.contains(&self.topic())
                );
                let converged = (self.remote.as_ref())
                    .is_some_and(|remote| manifests_converged(&local, remote));
                if exists && converged && dominates_required(&local.cursor, self.required.as_ref())
                {
                    return Ok(Next::Done(Some((holder, local.digest))));
                }
                // Topic or entry digests differ: pull the co-holder's events and re-compare.
                self.stage = ConvergeStage::Pull;
                let pull = PullTopicsOperation::new(vec![self.topic()], vec![holder]);
                Ok(Next::Call(IoCall::child(pull)))
            }
            ConvergeStage::Pull => {
                drop(result.child::<bool, IoError>()?);
                self.stage = ConvergeStage::Refetch;
                Ok(Next::Call(self.fetch(holder)))
            }
            ConvergeStage::Refetch => match result.child::<ShardManifest, ManifestError>()? {
                Ok(remote) => {
                    self.attempts += 1;
                    if self.attempts < VERIFICATION_MAX_ATTEMPTS {
                        self.assemble(remote)
                    } else {
                        self.next_holder()
                    }
                }
                Err(error) => {
                    debug!(co_holder = %holder, error = %error, "Re-fetch of shard manifest failed");
                    self.next_holder()
                }
            },
        }
    }
}

/// Deletes a shard's verification marker (e.g. when the local node leaves the
/// shard's holder set), so a later re-entry re-verifies from scratch.
pub(crate) fn forget_verification(realm_id: RealmId, placement: &PlacementRef) -> IoCall {
    IoCall::Delete(vec![(
        SHARD_VERIFICATION_KEYSPACE.to_string(),
        verification_key(realm_id, placement),
    )])
}

/// Topic ids of every shard the local node has durably verified in `realm_id`.
/// Reconciliation installs a former-holder history cutoff only for these: an
/// unverified shard's clock is not a trustworthy cutover boundary until proven.
#[derive(Debug, PartialEq)]
pub struct VerifiedTopicsOperation {
    realm_id: RealmId,
    pages: PrefixPages,
    topics: BTreeSet<irokle::TopicId>,
}

impl VerifiedTopicsOperation {
    pub fn new(realm_id: RealmId) -> Self {
        let prefix = placement_prefix(realm_id);
        Self {
            realm_id,
            pages: PrefixPages::new(SHARD_VERIFICATION_KEYSPACE, prefix, SHARD_PAGE_SIZE),
            topics: BTreeSet::new(),
        }
    }
}

impl Operation for VerifiedTopicsOperation {
    type Output = BTreeSet<irokle::TopicId>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<BTreeSet<irokle::TopicId>>, IoError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BTreeSet<irokle::TopicId>>, IoError> {
        for (_, row) in self.pages.take(result)? {
            if let Ok(record) = postcard::from_bytes::<ShardVerificationRecord>(&row) {
                let topic = shard_topic_id(self.realm_id, &record.placement);
                self.topics.insert(topic);
            }
        }
        Ok(match self.pages.done() {
            true => Next::Done(std::mem::take(&mut self.topics)),
            false => Next::Call(self.pages.call()),
        })
    }
}

/// Whether the local node has a persisted verification marker for a shard.
#[derive(Debug, PartialEq)]
pub struct ShardVerifiedOperation {
    key: Key,
}

impl ShardVerifiedOperation {
    pub fn new(realm_id: RealmId, placement: &PlacementRef) -> Self {
        Self {
            key: verification_key(realm_id, placement),
        }
    }
}

impl Operation for ShardVerifiedOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let key = self.key.clone();
        Ok(Next::Call(read_row(SHARD_VERIFICATION_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        Ok(Next::Done(value(result)?.is_some()))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::document::{
        DocumentSyncRevision, DocumentTarget, ShardManifest, ShardManifestEntry,
    };
    use ulid::Ulid;

    fn node_id(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn placement() -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([9; 16]),
            shard: 7,
        }
    }

    fn entry(document: u8, generation: u64) -> ShardManifestEntry {
        ShardManifestEntry {
            target: DocumentTarget::MetadataDocumentLifecycle {
                document_id: Ulid::from_bytes([document; 16]),
            },
            revision: DocumentSyncRevision {
                generation,
                event_id: Ulid::from_bytes([generation as u8; 16]),
                actor: node_id(3),
                updated_at_ms: generation,
            },
        }
    }

    fn manifest(entries: Vec<ShardManifestEntry>) -> ShardManifest {
        ShardManifest {
            placement: placement(),
            holder: node_id(1),
            entries,
            cursor: Vec::new(),
            digest: [7; 32],
            updated_at_ms: 0,
        }
    }

    #[test]
    fn equal_digest_diverges() {
        let local = manifest(vec![entry(1, 1)]);
        let remote = manifest(vec![entry(2, 1)]);

        assert_eq!(local.digest, remote.digest);
        assert!(!manifests_converged(&local, &remote));
    }
}

#[cfg(test)]
#[path = "converge_tests.rs"]
mod call_tests;
