//! Reads the bytes of a distributed version this member keeps the record of but no copy, from
//! a holder of the version's range, verified by bao against its BLAKE3.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The read runs under the caller's authority on that holder; the bytes are then kept as the
// member's data strategy says.

use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::{Arc, LazyLock, Mutex as StdMutex};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, NetEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::heartbeat::HeartbeatEffect;
use aruna_core::holder_choice::{HolderCandidate, HolderChoiceError, HolderChoiceOperation, pick};
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, DISTRIBUTION_KEYSPACE, FILL_CLAIM_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{outside_answer, outside_call, read_row, value};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::storage::blob::{BackendLocation, BucketInfo};
use aruna_core::structs::storage::distribution::on_use::{
    CACHE_CAPACITY_BYTES, FillClaim, KeptKind, PullPlan, claim_key,
};
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionView, MemberState, RangeBehaviour,
};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Key;
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::warn;
use ulid::Ulid;

use super::held::{HeldElsewhere, PullRequest, admitted};
use super::keep::{
    ClaimFillOperation, Claimed, KeepError, KeptCopy, PublishKeptOperation, Published,
};
use super::purge_steps::{DropCopyOperation, PurgeError};
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::managed_read::ManagedReadOperation;
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};
use crate::s3::object::get::{GetObjectError, GetObjectResult, ResolvedObjectRange};
use crate::s3::object::read::HolderFailures;

type Blob = BackendStream<Result<Bytes, StreamError>>;

/// Fills that run detached from their read at once; a read finding none free keeps nothing.
const DETACHED_FILLS: usize = 8;

static FILL_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(DETACHED_FILLS)));

/// Kept copies being read, by backend path, with their reader counts.
static PINS: LazyLock<StdMutex<HashMap<String, usize>>> = LazyLock::new(Default::default);

/// A slot for one detached fill, or `None` when all run; nobody waits for one.
pub(crate) fn fill_slot() -> Option<OwnedSemaphorePermit> {
    FILL_SLOTS.clone().try_acquire_owned().ok()
}

/// One read's hold on a kept copy: eviction leaves the bytes of a held copy to the hidden
/// sweep, which spares them too while a read holds them.
struct ReadPin(String);

impl Drop for ReadPin {
    fn drop(&mut self) {
        let mut pins = PINS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = pins.get_mut(&self.0) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&self.0);
            }
        }
    }
}

/// Whether a read holds the kept copy at `backend_path`.
pub fn pinned_path(backend_path: &str) -> bool {
    let pins = PINS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    pins.contains_key(backend_path)
}

/// `blob` of the kept copy at `location`, which stays held until the stream ends or drops.
pub fn pinned_blob(location: &BackendLocation, blob: Blob) -> Blob {
    let path = location.backend_path.clone();
    let mut pins = PINS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    *pins.entry(path.clone()).or_default() += 1;
    drop(pins);
    let pin = ReadPin(path);
    let held = blob.0.inspect(move |_| {
        let _ = &pin;
    });
    BackendStream(Box::pin(held))
}

/// How this member answers reads of the version's range.
#[derive(Clone, Debug, PartialEq)]
struct Member {
    member_id: Ulid,
    strategy: DataStrategy,
    behaviour: RangeBehaviour,
    /// An encrypting bucket keeps nothing in plain text, so it only streams.
    keeping: bool,
    holders: Vec<NodeId>,
}

/// What the pending call of finding this member's holders answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MemberStep {
    Bucket,
    Record,
    Spread,
    Fresh,
    Encryption,
}

/// This member's strategy for the key's range and the holders of its bytes: the replicas that
/// copy it, then its coordinator.
#[derive(Debug, PartialEq)]
struct PullMemberOperation {
    request: PullRequest,
    step: MemberStep,
    member_id: Option<Ulid>,
    member: Option<Member>,
    replicas: usize,
}

type MemberTurn = Result<Next<Member>, GetObjectError>;

impl PullMemberOperation {
    fn new(request: PullRequest) -> Self {
        Self {
            request,
            step: MemberStep::Bucket,
            member_id: None,
            member: None,
            replicas: 0,
        }
    }

    fn member(&mut self) -> Result<&mut Member, IoError> {
        self.member.as_mut().ok_or(IoError::Unfinished)
    }

    fn choice(&self, holders: &[NodeId]) -> IoCall {
        let choice = HolderChoiceOperation::new(holders, unix_timestamp_millis());
        IoCall::child(choice)
    }

    fn recorded(&mut self, record: &[u8], member_id: Ulid) -> MemberTurn {
        let view = DistributionView::from_bytes(record)?;
        let snapshot = view.snapshot();
        let member = snapshot
            .member(member_id)
            .ok_or(GetObjectError::HoldersUnavailable)?;
        let range = snapshot
            .range_for(&self.request.key)
            .ok_or(GetObjectError::HoldersUnavailable)?;
        let ready = |strategies: &'static [DataStrategy]| {
            range.replicas.iter().filter(move |replica| {
                snapshot.member(**replica).is_some_and(|replica| {
                    strategies.contains(&replica.strategy) && replica.state == MemberState::Ready
                })
            })
        };
        let copies: Vec<&Ulid> = (ready(&[DataStrategy::Copy]))
            .filter(|replica| **replica != member.member_id)
            .collect();
        self.replicas = copies.len();
        // Members that keep copies on use may serve one; they are tried after the coordinator.
        let keepers = ready(&[DataStrategy::CopyOnUse, DataStrategy::CacheOnUse]);
        let holders: Vec<NodeId> = (copies.into_iter())
            .chain(std::iter::once(&range.coordinator))
            .chain(keepers)
            .filter(|holder| **holder != member.member_id)
            .filter_map(|holder| snapshot.member(*holder).map(|holder| holder.node_id))
            .collect();
        self.member = Some(Member {
            member_id: member.member_id,
            strategy: member.strategy,
            behaviour: snapshot.config.range_behaviour(member),
            keeping: false,
            holders,
        });
        // Replicas share the load by free download slots; the coordinator stays the fallback.
        if self.replicas > 1 {
            let replicas = self.replicas;
            let holders = self.member()?.holders[..replicas].to_vec();
            let spread = self.choice(&holders);
            self.step = MemberStep::Spread;
            return Ok(Next::Call(spread));
        }
        self.look_fresh()
    }

    fn spread(&mut self, candidates: Vec<HolderCandidate>) -> MemberTurn {
        let replicas = self.replicas;
        let chosen = pick(candidates, Ulid::generate().random());
        let holders = &mut self.member()?.holders;
        let at = chosen.and_then(|chosen| holders.iter().position(|id| *id == chosen.node_id));
        match at {
            Some(at) => holders[..=at].rotate_right(1),
            None => {
                let spread = (Ulid::generate().random() % replicas as u128) as usize;
                holders[..replicas].rotate_left(spread);
            }
        }
        self.look_fresh()
    }

    /// Holders that failed lately or have no fresh heartbeat are asked only when no other is.
    fn look_fresh(&mut self) -> MemberTurn {
        let holders = self.member()?.holders.clone();
        self.step = MemberStep::Fresh;
        Ok(Next::Call(self.choice(&holders)))
    }
}

impl Operation for PullMemberOperation {
    type Output = Member;
    type Error = GetObjectError;

    fn first(&mut self) -> MemberTurn {
        let key = self.request.bucket.as_bytes().to_vec();
        Ok(Next::Call(read_row(S3_BUCKET_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> MemberTurn {
        match self.step {
            MemberStep::Bucket => {
                let bucket = value(result)?.ok_or(GetObjectError::NoSuchKey)?;
                let binding = BucketInfo::from_bytes(&bucket)?
                    .distribution
                    .filter(|binding| binding.set_id == self.request.held.set_id)
                    .ok_or(GetObjectError::HoldersUnavailable)?;
                self.member_id = Some(binding.member_id);
                self.step = MemberStep::Record;
                let key = binding.set_id.to_bytes().to_vec();
                Ok(Next::Call(read_row(DISTRIBUTION_KEYSPACE, key)))
            }
            MemberStep::Record => {
                let record = value(result)?.ok_or(GetObjectError::HoldersUnavailable)?;
                let member_id = self.member_id.ok_or(IoError::Unfinished)?;
                self.recorded(&record, member_id)
            }
            MemberStep::Spread => {
                let found = result.child::<Vec<HolderCandidate>, HolderChoiceError>()?;
                self.spread(found.unwrap_or_default())
            }
            MemberStep::Fresh => {
                let found = result.child::<Vec<HolderCandidate>, HolderChoiceError>()?;
                let fresh: Vec<NodeId> = (found.unwrap_or_default().into_iter())
                    .map(|candidate| candidate.node_id)
                    .collect();
                if !fresh.is_empty() {
                    self.member()?
                        .holders
                        .retain(|holder| fresh.contains(holder));
                }
                self.step = MemberStep::Encryption;
                let key = self.request.bucket.as_bytes().to_vec();
                Ok(Next::Call(read_row(BUCKET_ENCRYPTION_KEYSPACE, key)))
            }
            MemberStep::Encryption => {
                let settings = value(result)?;
                // Governed versions are served only from registered copies, never kept ones.
                let keeping = BucketEncryption::from_row(settings.as_deref())?
                    .active_key()
                    .is_none()
                    && self.request.held.source_policies.is_empty();
                let mut member = self.member.take().ok_or(IoError::Unfinished)?;
                member.keeping = keeping;
                Ok(Next::Done(member))
            }
        }
    }
}

/// Streams the version, or only `range` of it, from the first holder that serves it.
#[derive(Debug, PartialEq)]
struct FromHoldersOperation {
    request: BaoReadRequest,
    holders: VecDeque<NodeId>,
    asked: Option<NodeId>,
    failures: HolderFailures,
    /// A failed holder is being noted before the next one is asked.
    avoiding: bool,
}

type HolderTurn = Result<Next<Blob>, GetObjectError>;

impl FromHoldersOperation {
    fn new(request: &PullRequest, holders: &[NodeId], range: Option<Range<u64>>) -> Self {
        let held = &request.held;
        Self {
            request: BaoReadRequest {
                auth_context: request.auth.clone(),
                realm_id: request.auth.realm_id,
                target: BaoReadTarget::Version {
                    set_id: held.set_id,
                    version_id: held.version_id,
                },
                expected_blake3: held.blake3,
                metadata_only: false,
                destination: None,
                known_refs: Vec::new(),
                range,
            },
            holders: holders.iter().copied().collect(),
            asked: None,
            failures: HolderFailures::default(),
            avoiding: false,
        }
    }

    fn ask_next(&mut self) -> HolderTurn {
        let Some(holder) = self.holders.pop_front() else {
            return Err(std::mem::take(&mut self.failures).into_error());
        };
        self.asked = Some(holder);
        let read = ManagedReadOperation::new(holder, self.request.clone());
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for FromHoldersOperation {
    type Output = Blob;
    type Error = GetObjectError;

    fn first(&mut self) -> HolderTurn {
        self.ask_next()
    }

    fn next(&mut self, result: IoResult) -> HolderTurn {
        if std::mem::take(&mut self.avoiding) {
            result.event()?;
            return self.ask_next();
        }
        let holder = self.asked.take().ok_or(IoError::Unfinished)?;
        let read = result.child::<BaoReadOutput, BaoReadError>()?;
        // A holder that failed to serve is skipped by the next holder choices for a while.
        let failed = matches!(
            read,
            Err(BaoReadError::Blob(_) | BaoReadError::Refused(BaoReadRefusal::BackendFailure))
        );
        if let Some(BaoReadOutput::Stream { blob, .. }) = self.failures.consider(read) {
            return Ok(Next::Done(blob));
        }
        if !failed {
            return self.ask_next();
        }
        self.avoiding = true;
        let now_ms = unix_timestamp_millis();
        let avoid = HeartbeatEffect::Avoid { holder, now_ms };
        let effect = Effect::Net(NetEffect::Heartbeat(Box::new(avoid)));
        Ok(Next::Call(IoCall::from(effect)))
    }
}

/// A read through the holders, with the fill the caller may run beside it.
#[derive(Debug, PartialEq)]
pub struct Pulled {
    pub result: GetObjectResult,
    pub fill: Option<FillOperation>,
}

/// What the pending call of a read through the holders answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PullStep {
    Member,
    Serve,
    Whole,
    Spool,
    Fill,
    Kept,
}

/// Reads the version through a holder per this member's strategy and range behaviour. No
/// reachable holder is an unavailable error, never absence.
#[derive(Debug, PartialEq)]
pub struct PullThroughOperation {
    request: PullRequest,
    step: PullStep,
    holders: Vec<NodeId>,
    resolved: Option<ResolvedObjectRange>,
    range: Option<Range<u64>>,
    fill: Option<FillOperation>,
    location: Option<BackendLocation>,
}

type PullTurn = Result<Next<Pulled>, GetObjectError>;

impl PullThroughOperation {
    pub fn new(request: PullRequest) -> Self {
        Self {
            request,
            step: PullStep::Member,
            holders: Vec::new(),
            resolved: None,
            range: None,
            fill: None,
            location: None,
        }
    }

    fn from_holders(&mut self, range: Option<Range<u64>>, step: PullStep) -> PullTurn {
        let read = FromHoldersOperation::new(&self.request, &self.holders, range);
        self.step = step;
        Ok(Next::Call(IoCall::child(read)))
    }

    fn planned(&mut self, member: Member) -> PullTurn {
        let size = self.request.held.size;
        let range = self.request.range.as_ref();
        self.resolved = range.map(|range| range.resolve(size)).transpose()?;
        self.range = self
            .resolved
            .as_ref()
            .map(|resolved| resolved.range.clone());
        let plan = PullPlan::choose(
            member.strategy,
            member.behaviour,
            self.range.is_some(),
            member.keeping,
        );
        self.holders = member.holders.clone();
        let range = self.range.clone();
        match plan {
            PullPlan::Stream => self.from_holders(range, PullStep::Serve),
            PullPlan::SpoolFirst => self.from_holders(None, PullStep::Whole),
            PullPlan::FillFirst(kind) => {
                let fill = FillOperation::new(self.request.clone(), kind, member);
                self.step = PullStep::Fill;
                Ok(Next::Call(IoCall::child(fill)))
            }
            PullPlan::ServeAndFill(kind) => {
                self.fill = Some(FillOperation::new(self.request.clone(), kind, member));
                self.from_holders(range, PullStep::Serve)
            }
        }
    }

    /// Reads the whole version into a local spool; `range` of it is served once all bytes came.
    fn spool(&mut self, whole: Blob) -> PullTurn {
        let spool = BlobEffect::SpoolLocal {
            size: self.request.held.size,
            deadline: None,
            range: self.range.clone(),
            blob: whole,
        };
        self.step = PullStep::Spool;
        Ok(Next::Call(IoCall::from(Effect::Blob(spool))))
    }

    fn spooled(&mut self, event: Event) -> PullTurn {
        let expected = self.request.held.blake3;
        match event {
            Event::Blob(BlobEvent::LocalSpooled { hashes, blob, .. })
                if expected.is_none_or(|blake3| {
                    hashes.get(HASH_BLAKE3).map(Vec::as_slice) == Some(blake3.as_slice())
                }) =>
            {
                self.finish(blob)
            }
            Event::Blob(BlobEvent::LocalSpooled { .. }) => {
                Err(GetObjectError::HolderIntegrityFailure)
            }
            Event::Blob(BlobEvent::Error(BlobError::SpoolBudgetExhausted { .. })) => {
                Err(GetObjectError::SpoolBudgetExhausted)
            }
            _ => Err(GetObjectError::HoldersUnavailable),
        }
    }

    /// Reads `range` of the kept copy.
    fn read_kept(&mut self, location: BackendLocation) -> PullTurn {
        let read = match self.range.clone() {
            Some(range) => BlobEffect::ReadRange {
                location: location.clone(),
                range,
            },
            None => BlobEffect::Read {
                location: location.clone(),
            },
        };
        self.location = Some(location);
        self.step = PullStep::Kept;
        Ok(Next::Call(IoCall::from(Effect::Blob(read))))
    }

    fn finish(&mut self, blob: Blob) -> PullTurn {
        let held = self.request.held.clone();
        Ok(Next::Done(Pulled {
            result: GetObjectResult {
                blob,
                location: None,
                info: held.info(),
                metadata: held.metadata,
                source_metadata: None,
                source_binding: None,
                last_refresh: None,
                version_id: Some(held.version_id),
                resolved_version_id: Some(held.version_id),
                resolved_range: self.resolved.take(),
                source_policies: held.source_policies,
                served_source: None,
            },
            fill: self.fill.take(),
        }))
    }
}

impl Operation for PullThroughOperation {
    type Output = Pulled;
    type Error = GetObjectError;

    fn first(&mut self) -> PullTurn {
        let member = PullMemberOperation::new(self.request.clone());
        Ok(Next::Call(IoCall::child(member)))
    }

    fn next(&mut self, result: IoResult) -> PullTurn {
        match self.step {
            PullStep::Member => self.planned(result.child::<Member, GetObjectError>()??),
            PullStep::Serve => {
                let blob = result.child::<Blob, GetObjectError>()??;
                self.finish(blob)
            }
            PullStep::Whole => {
                let whole = result.child::<Blob, GetObjectError>()??;
                self.spool(whole)
            }
            PullStep::Spool => self.spooled(result.event()?),
            PullStep::Fill => match result.child::<Option<BackendLocation>, IoError>()?? {
                Some(location) => self.read_kept(location),
                None => {
                    let range = self.range.clone();
                    self.from_holders(range, PullStep::Serve)
                }
            },
            PullStep::Kept => match result.event()? {
                Event::Blob(BlobEvent::ReadFinished { blob, .. }) => {
                    let location = self.location.take().ok_or(IoError::Unfinished)?;
                    self.finish(pinned_blob(&location, blob))
                }
                _ => Err(GetObjectError::GetObjectFailed),
            },
        }
    }
}

/// What the pending call of a fill answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FillStep {
    Claim,
    Holders,
    Spool,
    Drop,
    Release,
    Publish,
}

/// Keeps a verified copy of the whole version once: claims the fill, reads the version through
/// a holder, checks its BLAKE3 and size, then publishes it. `None` when another fill runs or
/// keeping failed; the read then streams instead.
#[derive(Debug, PartialEq)]
pub struct FillOperation {
    request: PullRequest,
    kind: KeptKind,
    member: Member,
    claim: Vec<u8>,
    step: FillStep,
    location: Option<BackendLocation>,
}

type FillTurn = Result<Next<Option<BackendLocation>>, IoError>;

impl FillOperation {
    fn new(request: PullRequest, kind: KeptKind, member: Member) -> Self {
        let held = &request.held;
        let claim = claim_key(kind, held.set_id, held.version_id, held.identity);
        Self {
            request,
            kind,
            member,
            claim,
            step: FillStep::Claim,
            location: None,
        }
    }

    fn held(&self) -> &HeldElsewhere {
        &self.request.held
    }

    /// Gives the claim back after a failed copy, so the next read may fill again.
    fn failed(&mut self, reason: &str) -> FillTurn {
        let (set_id, version_id) = (self.held().set_id, self.held().version_id);
        warn!(reason, %set_id, %version_id, "A kept copy was not written");
        self.release()
    }

    fn release(&mut self) -> FillTurn {
        self.step = FillStep::Release;
        let release = ReleaseClaimOperation::new(self.claim.clone());
        Ok(Next::Call(IoCall::child(release)))
    }

    /// Writes the whole version to a hidden copy.
    fn spool(&mut self, whole: Blob) -> FillTurn {
        let held = self.held();
        let name = match self.kind {
            KeptKind::Attachment => format!("{}-{}", held.set_id, held.version_id),
            KeptKind::Cache => hex::encode(held.identity),
        };
        let spool = BlobEffect::SpoolHidden {
            namespace: self.kind.namespace(),
            name,
            created_by: self.request.auth.user_id,
            max_bytes: Some(held.size),
            deadline: None,
            blob: whole,
        };
        self.step = FillStep::Spool;
        Ok(Next::Call(IoCall::from(Effect::Blob(spool))))
    }

    /// Checks the copy against the record, then publishes it.
    fn spooled(&mut self, event: Event) -> FillTurn {
        let held = self.held();
        let (size_wanted, blake3_wanted) = (held.size, held.blake3);
        match event {
            Event::Blob(BlobEvent::HiddenSpooled {
                location,
                blake3,
                size,
            }) if blake3_wanted.unwrap_or(blake3) == blake3 && size == size_wanted => {
                let copy = KeptCopy {
                    kind: self.kind,
                    set_id: self.held().set_id,
                    version_id: self.held().version_id,
                    identity: self.held().identity,
                    blake3,
                    size,
                    location: location.clone(),
                    claim_key: self.claim.clone(),
                    capacity: CACHE_CAPACITY_BYTES,
                    order: Ulid::generate(),
                    member_id: self.member.member_id,
                };
                self.location = Some(location);
                self.step = FillStep::Publish;
                Ok(Next::Call(IoCall::child(KeepCopyOperation::new(copy))))
            }
            Event::Blob(BlobEvent::HiddenSpooled { location, .. }) => {
                self.step = FillStep::Drop;
                let drop = DropCopyOperation::new(location);
                Ok(Next::Call(IoCall::child(drop)))
            }
            Event::Blob(BlobEvent::Error(error)) => self.failed(&error.to_string()),
            _ => self.failed("unexpected answer to the copy write"),
        }
    }
}

impl Operation for FillOperation {
    type Output = Option<BackendLocation>;
    type Error = IoError;

    fn first(&mut self) -> FillTurn {
        // A copy the cache can never hold is not fetched at all.
        if !admitted(self.kind, self.held().size) {
            return Ok(Next::Done(None));
        }
        let claimed = FillClaim {
            claim_id: Ulid::generate(),
            claimed_at_ms: unix_timestamp_millis(),
        };
        let claim = ClaimFillOperation::new(self.claim.clone(), claimed);
        Ok(Next::Call(IoCall::child(claim)))
    }

    fn next(&mut self, result: IoResult) -> FillTurn {
        match self.step {
            FillStep::Claim => match result.child::<Claimed, KeepError>()? {
                Ok(Claimed::Claimed) => {
                    let holders = &self.member.holders;
                    let read = FromHoldersOperation::new(&self.request, holders, None);
                    self.step = FillStep::Holders;
                    Ok(Next::Call(IoCall::child(read)))
                }
                Ok(Claimed::Busy) => Ok(Next::Done(None)),
                Err(error) => {
                    warn!(%error, "A kept copy was not claimed");
                    Ok(Next::Done(None))
                }
            },
            FillStep::Holders => match result.child::<Blob, GetObjectError>()? {
                Ok(whole) => self.spool(whole),
                Err(error) => self.failed(&error.to_string()),
            },
            FillStep::Spool => self.spooled(result.event()?),
            FillStep::Drop => {
                result.child::<(), PurgeError>()?.ok();
                self.failed("the copy does not match the version")
            }
            FillStep::Release => {
                result.child::<(), IoError>()??;
                Ok(Next::Done(None))
            }
            FillStep::Publish => match result.child::<bool, KeepError>()? {
                Ok(true) => Ok(Next::Done(self.location.take())),
                Ok(false) => Ok(Next::Done(None)),
                Err(error) => {
                    warn!(%error, "A kept copy was not published");
                    Ok(Next::Done(None))
                }
            },
        }
    }
}

/// What the pending call of publishing a kept copy answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeepStep {
    Publish,
    Evicted,
    Unkept,
    Failed,
    Release,
}

/// Publishes a written copy and deletes the copies that evicted; a copy a read still holds is
/// left to the sweep. `true` once kept. A copy that stays unpublished is deleted again, and a
/// failed publish also gives the claim back.
#[derive(Debug, PartialEq)]
pub(super) struct KeepCopyOperation {
    copy: Option<KeptCopy>,
    location: BackendLocation,
    claim: Vec<u8>,
    step: KeepStep,
    evicted: VecDeque<BackendLocation>,
    error: Option<KeepError>,
}

type KeepTurn = Result<Next<bool>, KeepError>;

fn drop_call(location: BackendLocation) -> IoCall {
    IoCall::child(DropCopyOperation::new(location))
}

impl KeepCopyOperation {
    pub(super) fn new(copy: KeptCopy) -> Self {
        Self {
            location: copy.location.clone(),
            claim: copy.claim_key.clone(),
            copy: Some(copy),
            step: KeepStep::Publish,
            evicted: VecDeque::new(),
            error: None,
        }
    }

    fn next_evicted(&mut self) -> KeepTurn {
        match self.evicted.pop_front() {
            Some(location) => {
                self.step = KeepStep::Evicted;
                Ok(Next::Call(drop_call(location)))
            }
            None => Ok(Next::Done(true)),
        }
    }

    fn published(&mut self, published: Result<Published, KeepError>) -> KeepTurn {
        match published {
            Ok(Published::Kept { evicted }) => {
                self.evicted = (evicted.into_iter())
                    .filter(|copy| !pinned_path(&copy.backend_path))
                    .collect();
                self.next_evicted()
            }
            Ok(Published::Unkept) => {
                self.step = KeepStep::Unkept;
                Ok(Next::Call(drop_call(self.location.clone())))
            }
            Err(error) => {
                self.error = Some(error);
                self.step = KeepStep::Failed;
                Ok(Next::Call(drop_call(self.location.clone())))
            }
        }
    }

    fn take_error(&mut self) -> KeepError {
        self.error.take().unwrap_or(KeepError::NotFinished)
    }
}

impl Operation for KeepCopyOperation {
    type Output = bool;
    type Error = KeepError;

    fn first(&mut self) -> KeepTurn {
        let copy = self.copy.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(PublishKeptOperation::new(copy))))
    }

    fn next(&mut self, result: IoResult) -> KeepTurn {
        match self.step {
            KeepStep::Publish => self.published(result.child::<Published, KeepError>()?),
            KeepStep::Evicted => {
                result.child::<(), PurgeError>()?.ok();
                self.next_evicted()
            }
            KeepStep::Unkept => {
                result.child::<(), PurgeError>()?.ok();
                Ok(Next::Done(false))
            }
            KeepStep::Failed => {
                result.child::<(), PurgeError>()?.ok();
                self.step = KeepStep::Release;
                let release = ReleaseClaimOperation::new(self.claim.clone());
                Ok(Next::Call(IoCall::child(release)))
            }
            KeepStep::Release => {
                result.child::<(), IoError>()??;
                Err(self.take_error())
            }
        }
    }
}

/// Releases a fill claim, so the next read may fill again; a claim that stays goes stale.
#[derive(Debug, PartialEq)]
pub(super) struct ReleaseClaimOperation {
    claim: Vec<u8>,
}

impl ReleaseClaimOperation {
    pub(super) fn new(claim: Vec<u8>) -> Self {
        Self { claim }
    }
}

impl Operation for ReleaseClaimOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let key = Key::from(self.claim.clone());
        let row = (FILL_CLAIM_KEYSPACE.to_string(), key);
        Ok(Next::Call(outside_call(IoCall::Delete(vec![row]))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if let Err(error) = outside_answer(result) {
            warn!(%error, "A fill claim stays until it goes stale");
        }
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
#[path = "pull_tests.rs"]
mod tests;
