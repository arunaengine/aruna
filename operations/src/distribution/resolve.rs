//! Resolves a distributed bucket's global version id, or a bare content hash, on any realm
//! node: candidate sets by hint, then a local index or one probe per set or range.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Budgets are bounded; an exhausted budget or a silent range is "not resolvable now", never
// absence.

use aruna_core::rows::{outside_answer, outside_call};
use std::collections::{HashMap, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    CONTENT_INDEX_KEYSPACE, DISTRIBUTION_KEYSPACE, VERSION_INDEX_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, value};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::inventory::{
    IndexEntry, Position, content_prefix, index_key,
};
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionSnapshot, DistributionView, MemberState, MetadataCoverage, SetHint,
};
use aruna_core::types::Key;
use thiserror::Error;
use tokio::time::Instant;
use ulid::Ulid;

use crate::distribution::counts::MAX_SETS;
use crate::distribution::version_key::{MemberAnswer, VersionKeyError, VersionKeyOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_core::structs::identity::realm::NodeRealm;

/// At most this many candidate sets are looked at for one id.
const MAX_CANDIDATES: usize = 4;
/// At most this many members are asked for one id, over all candidate sets.
pub const MAX_PROBES: usize = 16;
/// At most this many versions of one content identity are read from one index or returned.
pub const MAX_CONTENT_VERSIONS: usize = 16;
/// How long resolving one id inside a longer request may take.
pub const RESOLVE_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

/// Set records one page reads.
const SET_PAGE: usize = 256;

/// Why an index of this node could not answer.
#[derive(Clone, Debug, Error, PartialEq)]
#[error("{0}")]
pub struct ResolveError(String);

impl From<IoError> for ResolveError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

fn failed(error: impl std::fmt::Display) -> ResolveError {
    ResolveError(error.to_string())
}

/// Where one global version id lives.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedVersion {
    pub set_id: Ulid,
    /// The one bucket name every member's local bucket carries.
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    /// This node's member bucket is bound to the set and its index holds the version.
    pub local: bool,
    /// Ready members that keep the bytes of the key's range: its coordinator, its `copy`
    /// replicas and then the replicas keeping copies on use, this node included when it is one.
    pub holders: Vec<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GlobalResolution {
    Found(ResolvedVersion),
    /// Every candidate set answered and none holds the version.
    NotFound,
    /// No record names a candidate set yet, the budget ran out or a range did not answer.
    Unavailable,
    /// More than one set holds the version id.
    Ambiguous,
}

/// The ready members that keep the bytes of `key`'s range: its coordinator and `copy` replicas,
/// then the replicas that keep copies on use, which serve a kept copy or read it through.
pub fn range_holders(snapshot: &DistributionSnapshot, key: &str) -> Vec<NodeId> {
    let Some(range) = snapshot.range_for(key) else {
        return Vec::new();
    };
    let ready = |member_id: &Ulid, strategies: &[DataStrategy]| {
        snapshot.member(*member_id).filter(|member| {
            member.state == MemberState::Ready
                && (strategies.is_empty() || strategies.contains(&member.strategy))
        })
    };
    let keeping = [DataStrategy::CopyOnUse, DataStrategy::CacheOnUse];
    std::iter::once(ready(&range.coordinator, &[]))
        .chain(
            range
                .replicas
                .iter()
                .map(|id| ready(id, &[DataStrategy::Copy])),
        )
        .chain(range.replicas.iter().map(|id| ready(id, &keeping)))
        .flatten()
        .map(|member| member.node_id)
        .collect()
}

/// The ready members that keep the bytes of `key`'s range in `set_id`, from this node's
/// record; none without a readable record.
#[derive(Debug, PartialEq)]
pub struct KeyHoldersOperation {
    set_id: Ulid,
    key: String,
}

/// The holders of `key` in `set_id`; see [`KeyHoldersOperation`].
pub fn key_holders(set_id: Ulid, key: &str) -> KeyHoldersOperation {
    KeyHoldersOperation {
        set_id,
        key: key.to_string(),
    }
}

impl Operation for KeyHoldersOperation {
    type Output = Vec<NodeId>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Vec<NodeId>>, IoError> {
        let read = read_row(DISTRIBUTION_KEYSPACE, self.set_id.to_bytes().to_vec());
        Ok(Next::Call(outside_call(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<NodeId>>, IoError> {
        let stored = outside_answer(result).and_then(|read| read.value());
        let holders = (stored.ok().flatten())
            .and_then(|value| DistributionView::from_bytes(&value).ok())
            .map(|view| range_holders(view.snapshot(), &self.key));
        Ok(Next::Done(holders.unwrap_or_default()))
    }
}

/// Whom to ask for a version in `snapshot`, as groups of alternatives that each cover part of
/// the set: the ready `all` coverage members, else each range's ready record holders, its
/// coordinator first. `local` is never asked; an empty group is a range only it holds.
pub fn probe_targets(snapshot: &DistributionSnapshot, local: NodeId) -> Vec<Vec<NodeId>> {
    let full: Vec<NodeId> = (snapshot.config.members.iter())
        .filter(|member| {
            member.node_id != local
                && member.state == MemberState::Ready
                && member.coverage == MetadataCoverage::All
        })
        .map(|member| member.node_id)
        .collect();
    if !full.is_empty() {
        return vec![full];
    }
    let mut groups: Vec<Vec<NodeId>> = Vec::new();
    for range in &snapshot.config.ranges {
        let mut holders = snapshot.record_holders(range);
        if let Some(at) = (holders.iter()).position(|member| member.member_id == range.coordinator)
        {
            holders[..=at].rotate_right(1);
        }
        let group: Vec<NodeId> = (holders.into_iter())
            .map(|member| member.node_id)
            .filter(|node_id| *node_id != local)
            .collect();
        if !groups.contains(&group) {
            groups.push(group);
        }
    }
    groups
}

/// Whether this node's member of `snapshot` is ready, so its own index proves absence.
fn ready_here(snapshot: &DistributionSnapshot, local: NodeId) -> bool {
    (snapshot.config.members.iter())
        .any(|member| member.node_id == local && member.state == MemberState::Ready)
}

/// Which records a scan keeps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SetFilter {
    All,
    Hinted(SetHint),
}

impl SetFilter {
    fn keeps(self, snapshot: &DistributionSnapshot) -> bool {
        match self {
            Self::All => true,
            Self::Hinted(hint) => SetHint::of_set(snapshot.set_id()) == hint,
        }
    }
}

/// The snapshots of this node's records the filter accepts, at most `wanted` of them, and
/// whether accepted records were dropped or records left unread because [`MAX_SETS`] were read.
#[derive(Debug, PartialEq)]
struct ScanSetsOperation {
    filter: SetFilter,
    wanted: usize,
    after: Option<Key>,
    seen: usize,
    found: Vec<DistributionSnapshot>,
    dropped: bool,
}

impl ScanSetsOperation {
    fn new(filter: SetFilter, wanted: usize) -> Self {
        Self {
            filter,
            wanted,
            after: None,
            seen: 0,
            found: Vec::new(),
            dropped: false,
        }
    }

    fn page(&self) -> IoCall {
        IoCall::Page {
            space: DISTRIBUTION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.clone(),
            limit: SET_PAGE,
        }
    }
}

type Scanned = (Vec<DistributionSnapshot>, bool);

impl Operation for ScanSetsOperation {
    type Output = Scanned;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Scanned>, IoError> {
        Ok(Next::Call(self.page()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Scanned>, IoError> {
        let (rows, next) = result.page()?;
        for (_, value) in rows {
            self.seen += 1;
            let Ok(view) = DistributionView::from_bytes(&value) else {
                continue;
            };
            let snapshot = view.snapshot();
            let keep = self.filter.keeps(snapshot);
            match keep && self.found.len() < self.wanted {
                true => self.found.push(snapshot.clone()),
                false => self.dropped |= keep,
            }
        }
        let found = std::mem::take(&mut self.found);
        match next {
            Some(next) if self.seen < MAX_SETS => {
                self.found = found;
                self.after = Some(next);
                Ok(Next::Call(self.page()))
            }
            Some(_) => Ok(Next::Done((found, true))),
            None => Ok(Next::Done((found, self.dropped))),
        }
    }
}

/// The key this node's version id index holds for a version in a set.
#[derive(Debug, PartialEq)]
pub struct IndexedKeyOperation {
    set_id: Ulid,
    version_id: Ulid,
}

/// The indexed key of `version_id`; see [`IndexedKeyOperation`].
pub fn indexed_key(set_id: Ulid, version_id: Ulid) -> IndexedKeyOperation {
    IndexedKeyOperation { set_id, version_id }
}

impl Operation for IndexedKeyOperation {
    type Output = Option<String>;
    type Error = ResolveError;

    fn first(&mut self) -> Result<Next<Option<String>>, ResolveError> {
        let key = index_key(self.set_id, self.version_id);
        Ok(Next::Call(read_row(VERSION_INDEX_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<String>>, ResolveError> {
        let Some(stored) = value(result)? else {
            return Ok(Next::Done(None));
        };
        let entry = IndexEntry::from_bytes(&stored).map_err(failed)?;
        let position = Position::decode(&entry.position).map_err(failed)?;
        Ok(Next::Done(Some(position.key)))
    }
}

/// Whether a bucket read shows this node's bucket bound to the set.
fn bound_to(result: IoResult, set_id: Ulid) -> Result<bool, IoError> {
    let info = result.child::<BucketInfo, GetBucketError>()?;
    let binding = info.ok().and_then(|info| info.distribution);
    Ok(binding.is_some_and(|binding| binding.set_id == set_id))
}

fn bucket_read(snapshot: &DistributionSnapshot) -> IoCall {
    IoCall::child(GetBucketOperation::new(snapshot.config.bucket.clone()))
}

/// The members still to ask for a set, group by group.
#[derive(Debug, Default, PartialEq)]
struct ProbeWalk {
    groups: VecDeque<Vec<NodeId>>,
    group: VecDeque<NodeId>,
    covered: bool,
    answered: HashMap<NodeId, bool>,
    /// The member asked now.
    asked: Option<NodeId>,
}

/// What the walk does next.
enum Probe {
    Ask(NodeId),
    /// One group ended; `covered` tells whether a member of it answered.
    GroupEnd {
        covered: bool,
    },
}

impl ProbeWalk {
    fn new(snapshot: &DistributionSnapshot, local: NodeId) -> Self {
        Self {
            groups: probe_targets(snapshot, local).into(),
            ..Self::default()
        }
    }

    /// Opens the next group; an empty one is covered when this node's own index proves it.
    fn open(&mut self, local_proves: bool) -> bool {
        let Some(group) = self.groups.pop_front() else {
            return false;
        };
        self.covered = group.is_empty() && local_proves;
        self.group = group.into();
        true
    }

    fn step(&mut self) -> Probe {
        while !self.covered {
            let Some(node_id) = self.group.pop_front() else {
                break;
            };
            match self.answered.get(&node_id) {
                Some(known) => self.covered = *known,
                None => {
                    self.asked = Some(node_id);
                    return Probe::Ask(node_id);
                }
            }
        }
        self.group.clear();
        Probe::GroupEnd {
            covered: self.covered,
        }
    }

    fn record(&mut self, covered: bool) {
        self.covered = covered;
        if let Some(node_id) = self.asked.take() {
            self.answered.insert(node_id, covered);
        }
    }
}

/// What one candidate set says about a version.
#[derive(Clone, Debug, Eq, PartialEq)]
enum SetAnswer {
    Found { key: String, local: bool },
    Absent,
    Unknown,
}

/// What the pending call of asking one set answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AskStep {
    Bound,
    Index,
    Probe,
}

/// Asks this node's index and then the set's members for a version, within the probe budget.
#[derive(Debug, PartialEq)]
struct AskSetOperation {
    ids: NodeRealm,
    snapshot: DistributionSnapshot,
    version_id: Ulid,
    probes: usize,
    deadline: Option<Instant>,
    step: AskStep,
    local_proves: bool,
    complete: bool,
    walk: ProbeWalk,
}

type Asked = (SetAnswer, usize);
type Asking = Result<Next<Asked>, IoError>;

impl AskSetOperation {
    fn new(
        ids: NodeRealm,
        snapshot: DistributionSnapshot,
        version_id: Ulid,
        (probes, deadline): (usize, Option<Instant>),
    ) -> Self {
        Self {
            ids,
            snapshot,
            version_id,
            probes,
            deadline,
            step: AskStep::Bound,
            local_proves: false,
            complete: true,
            walk: ProbeWalk::default(),
        }
    }

    fn answer(&self, answer: SetAnswer) -> Asking {
        Ok(Next::Done((answer, self.probes)))
    }

    /// Starts on the set's members; a catching-up member's own index proves no absence.
    fn probe_members(&mut self, bound: bool) -> Asking {
        let Some((local, _)) = self.ids else {
            return self.answer(SetAnswer::Unknown);
        };
        self.local_proves = bound && ready_here(&self.snapshot, local);
        self.walk = ProbeWalk::new(&self.snapshot, local);
        self.next_group()
    }

    fn next_group(&mut self) -> Asking {
        if !self.walk.open(self.local_proves) {
            return self.answer(match self.complete {
                true => SetAnswer::Absent,
                false => SetAnswer::Unknown,
            });
        }
        self.next_probe()
    }

    fn next_probe(&mut self) -> Asking {
        let node_id = match self.walk.step() {
            Probe::Ask(node_id) => node_id,
            Probe::GroupEnd { covered } => {
                self.complete &= covered;
                return self.next_group();
            }
        };
        let Some((_, realm_id)) = self.ids else {
            return self.answer(SetAnswer::Unknown);
        };
        let late = self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        if self.probes >= MAX_PROBES || late {
            return self.answer(SetAnswer::Unknown);
        }
        self.probes += 1;
        self.step = AskStep::Probe;
        let set_id = self.snapshot.set_id();
        let lookup = VersionKeyOperation::new(node_id, realm_id, set_id, self.version_id);
        Ok(Next::Call(IoCall::child(lookup)))
    }
}

impl Operation for AskSetOperation {
    type Output = Asked;
    type Error = IoError;

    fn first(&mut self) -> Asking {
        Ok(Next::Call(bucket_read(&self.snapshot)))
    }

    fn next(&mut self, result: IoResult) -> Asking {
        match self.step {
            AskStep::Bound => {
                if !bound_to(result, self.snapshot.set_id())? {
                    return self.probe_members(false);
                }
                self.step = AskStep::Index;
                let index = indexed_key(self.snapshot.set_id(), self.version_id);
                Ok(Next::Call(IoCall::child(index)))
            }
            AskStep::Index => match result.child::<Option<String>, ResolveError>()? {
                Ok(Some(key)) => self.answer(SetAnswer::Found { key, local: true }),
                Ok(None) => self.probe_members(true),
                Err(_) => self.answer(SetAnswer::Unknown),
            },
            AskStep::Probe => match result.child::<MemberAnswer, VersionKeyError>()? {
                Ok(MemberAnswer::Key(Some(key))) => {
                    self.answer(SetAnswer::Found { key, local: false })
                }
                Ok(MemberAnswer::Key(None)) => {
                    self.walk.record(true);
                    self.next_probe()
                }
                Ok(MemberAnswer::Versions { .. }) | Err(_) => {
                    self.walk.record(false);
                    self.next_probe()
                }
            },
        }
    }
}

/// Resolves a version id within its deadline; the caller runs it with the same deadline, so a
/// probe that hangs ends the run.
#[derive(Debug, PartialEq)]
pub struct ResolveVersionOperation {
    ids: NodeRealm,
    version_id: Ulid,
    deadline: Option<Instant>,
    candidates: VecDeque<DistributionSnapshot>,
    current: Option<DistributionSnapshot>,
    probes: usize,
    found: Option<ResolvedVersion>,
    complete: bool,
}

/// The resolution of `version_id` by `deadline`; see [`ResolveVersionOperation`].
pub fn resolve_version(
    ids: NodeRealm,
    version_id: Ulid,
    deadline: Instant,
) -> ResolveVersionOperation {
    ResolveVersionOperation::new(ids, version_id, Some(deadline))
}

/// The resolution of `version_id` for a caller whose own group deadline bounds the run.
pub(crate) fn resolve_bounded(ids: NodeRealm, version_id: Ulid) -> ResolveVersionOperation {
    ResolveVersionOperation::new(ids, version_id, None)
}

/// The resolution a run reached; a run that did not finish in time resolves nothing now.
pub fn resolved(run: Result<GlobalResolution, IoError>) -> GlobalResolution {
    run.unwrap_or(GlobalResolution::Unavailable)
}

/// Where a version lives according to a finished run, if it is resolvable now.
pub fn version_found(run: Result<GlobalResolution, IoError>) -> Option<ResolvedVersion> {
    match resolved(run) {
        GlobalResolution::Found(found) => Some(found),
        _ => None,
    }
}

type Resolving = Result<Next<GlobalResolution>, IoError>;

impl ResolveVersionOperation {
    pub(crate) fn new(ids: NodeRealm, version_id: Ulid, deadline: Option<Instant>) -> Self {
        Self {
            ids,
            version_id,
            deadline,
            candidates: VecDeque::new(),
            current: None,
            probes: 0,
            found: None,
            complete: false,
        }
    }

    fn next_set(&mut self) -> Resolving {
        let Some(snapshot) = self.candidates.pop_front() else {
            return Ok(Next::Done(match (self.found.take(), self.complete) {
                (Some(found), _) => GlobalResolution::Found(found),
                (None, true) => GlobalResolution::NotFound,
                (None, false) => GlobalResolution::Unavailable,
            }));
        };
        let budget = (self.probes, self.deadline);
        let ask = AskSetOperation::new(self.ids, snapshot.clone(), self.version_id, budget);
        self.current = Some(snapshot);
        Ok(Next::Call(IoCall::child(ask)))
    }
}

impl Operation for ResolveVersionOperation {
    type Output = GlobalResolution;
    type Error = IoError;

    fn first(&mut self) -> Resolving {
        let hint = SetHint::of_version(self.version_id);
        let scan = ScanSetsOperation::new(SetFilter::Hinted(hint), MAX_CANDIDATES);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn next(&mut self, result: IoResult) -> Resolving {
        let Some(snapshot) = self.current.take() else {
            let Ok((candidates, truncated)) = result.child::<Scanned, IoError>()? else {
                return Ok(Next::Done(GlobalResolution::Unavailable));
            };
            if candidates.is_empty() {
                return Ok(Next::Done(GlobalResolution::Unavailable));
            }
            // Candidates left out may hold it, so their silence proves nothing.
            self.complete = !truncated;
            self.candidates = candidates.into();
            return self.next_set();
        };
        let (answer, probes) = result.child::<Asked, IoError>()??;
        self.probes = probes;
        match answer {
            SetAnswer::Found { key, local } => {
                if self.found.is_some() {
                    return Ok(Next::Done(GlobalResolution::Ambiguous));
                }
                self.found = Some(ResolvedVersion {
                    set_id: snapshot.set_id(),
                    bucket: snapshot.config.bucket.clone(),
                    holders: range_holders(&snapshot, &key),
                    key,
                    version_id: self.version_id,
                    local,
                });
            }
            SetAnswer::Absent => {}
            SetAnswer::Unknown => self.complete = false,
        }
        self.next_set()
    }
}

/// The versions of one content identity this node's content index holds in `set_id`, at most
/// `limit`, and whether more exist.
#[derive(Debug, PartialEq)]
pub struct ContentVersionsOperation {
    set_id: Ulid,
    identity: [u8; 32],
    limit: usize,
    ids: VecDeque<Ulid>,
    current: Option<Ulid>,
    versions: Vec<(Ulid, String)>,
    more: bool,
    listed: bool,
}

/// The local versions of `identity`; see [`ContentVersionsOperation`].
pub fn content_versions(
    set_id: Ulid,
    identity: &[u8; 32],
    limit: usize,
) -> ContentVersionsOperation {
    ContentVersionsOperation {
        set_id,
        identity: *identity,
        limit,
        ids: VecDeque::new(),
        current: None,
        versions: Vec::new(),
        more: false,
        listed: false,
    }
}

type ContentPage = (Vec<(Ulid, String)>, bool);

impl ContentVersionsOperation {
    fn next_key(&mut self) -> Result<Next<ContentPage>, ResolveError> {
        let Some(version_id) = self.ids.pop_front() else {
            let versions = std::mem::take(&mut self.versions);
            return Ok(Next::Done((versions, self.more)));
        };
        self.current = Some(version_id);
        let index = indexed_key(self.set_id, version_id);
        Ok(Next::Call(IoCall::child(index)))
    }
}

impl Operation for ContentVersionsOperation {
    type Output = ContentPage;
    type Error = ResolveError;

    fn first(&mut self) -> Result<Next<ContentPage>, ResolveError> {
        Ok(Next::Call(IoCall::Page {
            space: CONTENT_INDEX_KEYSPACE.to_string(),
            prefix: content_prefix(self.set_id, &self.identity).into(),
            after: None,
            limit: self.limit.saturating_add(1),
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ContentPage>, ResolveError> {
        if !self.listed {
            self.listed = true;
            let (rows, _) = result.page()?;
            self.more = rows.len() > self.limit;
            self.ids = (rows.into_iter().take(self.limit))
                .filter_map(|(row, _)| {
                    let id = row.get(row.len().checked_sub(16)?..)?;
                    Some(Ulid::from_bytes(<[u8; 16]>::try_from(id).ok()?))
                })
                .collect();
            return self.next_key();
        }
        let version_id = self.current.take().ok_or(IoError::Unfinished)?;
        if let Some(key) = result.child::<Option<String>, ResolveError>()?? {
            self.versions.push((version_id, key));
        }
        self.next_key()
    }
}

/// The versions of a content identity across distributed buckets.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ContentResolution {
    pub versions: Vec<ResolvedVersion>,
    /// A set or range was not asked or did not answer, or a page was cut: more may exist.
    pub incomplete: bool,
}

impl ContentResolution {
    fn add(&mut self, snapshot: &DistributionSnapshot, found: Vec<(Ulid, String)>, local: bool) {
        for (version_id, key) in found {
            if self.versions.len() >= MAX_CONTENT_VERSIONS {
                self.incomplete = true;
                return;
            }
            if self
                .versions
                .iter()
                .any(|known| known.version_id == version_id)
            {
                continue;
            }
            self.versions.push(ResolvedVersion {
                set_id: snapshot.set_id(),
                bucket: snapshot.config.bucket.clone(),
                holders: range_holders(snapshot, &key),
                key,
                version_id,
                local,
            });
        }
    }
}

/// Whether this node's ready member of `snapshot` holds the records of every range.
fn holds_every_range(snapshot: &DistributionSnapshot, local: NodeId) -> bool {
    let member = snapshot
        .config
        .members
        .iter()
        .find(|member| member.node_id == local && member.state == MemberState::Ready);
    member.is_some_and(|member| {
        snapshot.held_ranges(member.member_id).len() == snapshot.config.ranges.len()
    })
}

/// What the pending call of a content resolution answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContentStep {
    Sets,
    Bound,
    Index,
    Probe,
}

/// Resolves a content identity within its deadline: this node's content index for its own
/// members, otherwise one `all` coverage member per set or the coordinator of every range.
/// Distributed buckets do not announce their objects in the DHT; this is their lookup.
#[derive(Debug, PartialEq)]
pub struct ResolveContentOperation {
    ids: NodeRealm,
    identity: [u8; 32],
    deadline: Instant,
    step: ContentStep,
    sets: VecDeque<DistributionSnapshot>,
    current: Option<DistributionSnapshot>,
    bound: bool,
    probes: usize,
    walk: ProbeWalk,
    resolution: ContentResolution,
}

/// The resolution of a content identity by `deadline`; see [`ResolveContentOperation`].
pub fn resolve_content(
    ids: NodeRealm,
    identity: [u8; 32],
    deadline: Instant,
) -> ResolveContentOperation {
    ResolveContentOperation {
        ids,
        identity,
        deadline,
        step: ContentStep::Sets,
        sets: VecDeque::new(),
        current: None,
        bound: false,
        probes: 0,
        walk: ProbeWalk::default(),
        resolution: ContentResolution::default(),
    }
}

/// The resolution a run reached; a run that did not finish in time is incomplete.
pub fn content_resolved(run: Result<ContentResolution, IoError>) -> ContentResolution {
    run.unwrap_or(ContentResolution {
        versions: Vec::new(),
        incomplete: true,
    })
}

type ResolvingContent = Result<Next<ContentResolution>, IoError>;

impl ResolveContentOperation {
    fn done(&mut self) -> ResolvingContent {
        Ok(Next::Done(std::mem::take(&mut self.resolution)))
    }

    fn snapshot(&self) -> Result<&DistributionSnapshot, IoError> {
        self.current.as_ref().ok_or(IoError::Unfinished)
    }

    fn next_set(&mut self) -> ResolvingContent {
        let Some(snapshot) = self.sets.pop_front() else {
            return self.done();
        };
        if self.resolution.versions.len() >= MAX_CONTENT_VERSIONS {
            self.resolution.incomplete = true;
            return self.done();
        }
        let read = bucket_read(&snapshot);
        self.current = Some(snapshot);
        self.step = ContentStep::Bound;
        Ok(Next::Call(read))
    }

    /// Asks the set's members once this node's own index said what it can.
    fn probe_members(&mut self, local: NodeId) -> ResolvingContent {
        self.walk = ProbeWalk::new(self.snapshot()?, local);
        self.next_group(local)
    }

    fn next_group(&mut self, local: NodeId) -> ResolvingContent {
        let proves = self.bound && ready_here(self.snapshot()?, local);
        match self.walk.open(proves) {
            true => self.next_probe(local),
            false => self.next_set(),
        }
    }

    fn next_probe(&mut self, local: NodeId) -> ResolvingContent {
        let node_id = match self.walk.step() {
            Probe::Ask(node_id) => node_id,
            Probe::GroupEnd { covered } => {
                self.resolution.incomplete |= !covered;
                return self.next_group(local);
            }
        };
        if self.probes >= MAX_PROBES || Instant::now() >= self.deadline {
            self.resolution.incomplete = true;
            return self.done();
        }
        self.probes += 1;
        let (_, realm_id): (NodeId, RealmId) = self.ids.ok_or(IoError::Unfinished)?;
        let set_id = self.snapshot()?.set_id();
        let lookup = VersionKeyOperation::content(node_id, realm_id, set_id, self.identity);
        self.step = ContentStep::Probe;
        Ok(Next::Call(IoCall::child(lookup)))
    }

    fn local(&self) -> Result<NodeId, IoError> {
        self.ids.map(|(local, _)| local).ok_or(IoError::Unfinished)
    }
}

impl Operation for ResolveContentOperation {
    type Output = ContentResolution;
    type Error = IoError;

    fn first(&mut self) -> ResolvingContent {
        let scan = ScanSetsOperation::new(SetFilter::All, MAX_SETS);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn next(&mut self, result: IoResult) -> ResolvingContent {
        match self.step {
            ContentStep::Sets => {
                let Ok((sets, truncated)) = result.child::<Scanned, IoError>()? else {
                    self.resolution.incomplete = true;
                    return self.done();
                };
                self.resolution.incomplete = truncated;
                if self.ids.is_none() {
                    self.resolution.incomplete |= !sets.is_empty();
                    return self.done();
                }
                self.sets = sets.into();
                self.next_set()
            }
            ContentStep::Bound => {
                let snapshot = self.snapshot()?;
                let set_id = snapshot.set_id();
                self.bound = bound_to(result, set_id)?;
                if !self.bound {
                    return self.probe_members(self.local()?);
                }
                let read = content_versions(set_id, &self.identity, MAX_CONTENT_VERSIONS);
                self.step = ContentStep::Index;
                Ok(Next::Call(IoCall::child(read)))
            }
            ContentStep::Index => {
                let snapshot = self.current.take().ok_or(IoError::Unfinished)?;
                match result.child::<ContentPage, ResolveError>()? {
                    Ok((found, more)) => {
                        self.resolution.incomplete |= more;
                        self.resolution.add(&snapshot, found, true);
                    }
                    Err(_) => self.resolution.incomplete = true,
                }
                let local = self.local()?;
                let every = holds_every_range(&snapshot, local);
                self.current = Some(snapshot);
                match every {
                    true => self.next_set(),
                    false => self.probe_members(local),
                }
            }
            ContentStep::Probe => {
                let answer = result.child::<MemberAnswer, VersionKeyError>()?;
                let covered = match answer {
                    Ok(MemberAnswer::Versions { versions, more }) => {
                        self.resolution.incomplete |= more;
                        let snapshot = self.current.take().ok_or(IoError::Unfinished)?;
                        self.resolution.add(&snapshot, versions, false);
                        self.current = Some(snapshot);
                        true
                    }
                    _ => false,
                };
                self.walk.record(covered);
                self.next_probe(self.local()?)
            }
        }
    }
}

#[cfg(test)]
#[path = "resolve_tests.rs"]
mod tests;
