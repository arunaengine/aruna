//! Brings a holder's repository cache up to date from the document's Git records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::calls::{configured_call, git_call, git_event, git_flag};
use super::state::{Ancestry, GitState, reduce};
use super::{GitError, download, publish, records};
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitChange, GitEffect, GitEvent, GitPack, GitRecord, record_writes};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use lru::LruCache;
use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{LazyLock, Mutex};
use ulid::Ulid;

type Document = MetadataRegistryRecord;

/// How many rounds of ancestry answers a projection asks for.
const MAX_ROUNDS: usize = 64;

/// The last completed projection of the document on this node, whatever changed since.
pub fn last(document_id: Ulid) -> Option<Projection> {
    let mut recent = RECENT.lock().ok()?;
    recent
        .get(&document_id)
        .map(|(_, projection)| projection.clone())
}

/// The refs each document's cache last served, which a push must build on.
static SERVED: LazyLock<
    std::sync::Mutex<std::collections::HashMap<Ulid, BTreeMap<String, String>>>,
> = LazyLock::new(Default::default);

/// The refs this node last served for the document, if it projected it.
pub fn served(document_id: Ulid) -> Option<BTreeMap<String, String>> {
    SERVED.lock().ok()?.get(&document_id).cloned()
}

#[derive(Clone, Debug, PartialEq)]
pub struct Projection {
    pub state: GitState,
    /// Shared, so reusing a remembered projection copies no records.
    pub records: std::sync::Arc<Vec<GitRecord>>,
    pub holders: Vec<NodeId>,
    /// [`record_writes`] before the records were read.
    pub writes: u64,
}

/// Documents whose last refresh and ancestry answers this node keeps; older ones drop first.
const REMEMBERED: NonZeroUsize = NonZeroUsize::new(256).unwrap();
/// More kept ancestry answers than this for one document are dropped instead.
const MAX_ANSWERS: usize = 4096;

/// The node, document's last event, update time and whether other holders may generate yet.
type Inputs = (Option<NodeId>, Ulid, u64, bool);

type Recent = Mutex<LruCache<Ulid, (Inputs, Projection)>>;
static RECENT: LazyLock<Recent> = LazyLock::new(|| Mutex::new(LruCache::new(REMEMBERED)));

/// Answers that a commit is an ancestor never change, so later projections reuse them.
static ANSWERS: LazyLock<Mutex<LruCache<Ulid, Ancestry>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(REMEMBERED)));

fn inputs(node: Option<NodeId>, document: &MetadataRegistryRecord, overdue: bool) -> Inputs {
    (
        node,
        document.last_event_id,
        document.updated_at_ms,
        overdue,
    )
}

/// The projection of the last refresh, if the document, its holders and its records,
/// replicated ones included, are unchanged since then. Reads no Git record.
pub(super) fn recent(node: Option<NodeId>, document: &Document, overdue: bool) -> RecentOperation {
    RecentOperation {
        node,
        document: document.clone(),
        overdue,
        known: None,
        configured: false,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct RecentOperation {
    node: Option<NodeId>,
    document: Document,
    overdue: bool,
    known: Option<Projection>,
    configured: bool,
}

impl Operation for RecentOperation {
    type Output = Option<Projection>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Option<Projection>>, GitError> {
        self.known = remembered(self.node, &self.document, self.overdue);
        Ok(match self.known {
            Some(_) => Next::Call(configured_call(self.document.document_id)),
            None => Next::Done(None),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<Projection>>, GitError> {
        if !self.configured {
            if !git_flag(result)? {
                return Ok(Next::Done(None));
            }
            self.configured = true;
            let holders = publish::holders(self.node, &self.document);
            return Ok(Next::Call(IoCall::child(holders)));
        }
        let holders = result.child::<Vec<NodeId>, GitError>()??;
        let known = self.known.take().ok_or(IoError::Unfinished)?;
        let unchanged = record_writes(self.document.document_id) == known.writes;
        Ok(Next::Done(
            (holders == known.holders && unchanged).then_some(known),
        ))
    }
}

fn remembered(
    node: Option<NodeId>,
    document: &MetadataRegistryRecord,
    overdue: bool,
) -> Option<Projection> {
    let mut recent = RECENT.lock().ok()?;
    let (known, projection) = recent.get(&document.document_id)?;
    (*known == inputs(node, document, overdue)).then(|| projection.clone())
}

pub fn remember(
    node: Option<NodeId>,
    document: &MetadataRegistryRecord,
    overdue: bool,
    projection: &Projection,
) {
    if let Ok(mut recent) = RECENT.lock() {
        let entry = (inputs(node, document, overdue), projection.clone());
        recent.put(document.document_id, entry);
    }
}

/// Drops the remembered refresh, so the next one projects again.
pub fn forget(document_id: Ulid) {
    if let Ok(mut recent) = RECENT.lock() {
        recent.pop(&document_id);
    }
}

fn answered(document_id: Ulid) -> Ancestry {
    ANSWERS
        .lock()
        .ok()
        .and_then(|mut known| known.get(&document_id).cloned())
        .unwrap_or_default()
}

fn learn(document_id: Ulid, ancestry: &Ancestry) {
    let answers: Ancestry = ancestry
        .iter()
        .filter(|(_, answer)| **answer)
        .map(|(pair, answer)| (pair.clone(), *answer))
        .collect();
    if let Ok(mut known) = ANSWERS.lock() {
        if answers.len() > MAX_ANSWERS {
            known.pop(&document_id);
        } else {
            known.put(document_id, answers);
        }
    }
}

/// Remote holders serve a pack to the user who stored it.
pub fn author(user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id: user_id.realm_id,
        path_restrictions: None,
        session: None,
    }
}

/// What the pending call of a projection answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectStep {
    Holders,
    Initialize,
    Records,
    Imported,
    Pack,
    Import,
    Ancestry,
    Refs,
    SetRefs,
}

/// Imports missing packs and moves local refs to the records' reduced state. The caller
/// holds the document's repository. A deleted cache is rebuilt the same way.
pub fn project(node: Option<NodeId>, document: &Document) -> ProjectOperation {
    ProjectOperation {
        node,
        document: document.clone(),
        step: ProjectStep::Holders,
        holders: Vec::new(),
        writes: 0,
        records: Vec::new(),
        state: GitState::default(),
        needs: Vec::new(),
        ancestry: Ancestry::new(),
        missing: VecDeque::new(),
        importing: None,
        rounds: 0,
    }
}

#[derive(Debug, PartialEq)]
pub struct ProjectOperation {
    node: Option<NodeId>,
    document: Document,
    step: ProjectStep,
    holders: Vec<NodeId>,
    writes: u64,
    records: Vec<GitRecord>,
    state: GitState,
    needs: Vec<(String, String)>,
    ancestry: Ancestry,
    missing: VecDeque<GitPack>,
    importing: Option<String>,
    rounds: usize,
}

type ProjectTurn = Result<Next<Projection>, GitError>;

impl ProjectOperation {
    fn id(&self) -> Ulid {
        self.document.document_id
    }

    fn store(&mut self, step: ProjectStep, effect: GitEffect) -> ProjectTurn {
        self.step = step;
        let actor = UserId::nil(self.document.realm_id);
        Ok(Next::Call(git_call(effect, actor)))
    }

    /// Reads the next missing pack from the record that named it first; a checkpoint only
    /// repeats it.
    fn next_pack(&mut self) -> ProjectTurn {
        let Some(pack) = self.missing.pop_front() else {
            self.ancestry = answered(self.id());
            if !self.ancestry.is_empty() {
                (self.state, self.needs) = reduce(&self.records, &self.ancestry);
            }
            return self.next_round();
        };
        let owner = self.records.iter().find(|record| {
            matches!(&record.change, GitChange::Objects { pack: Some(own), .. } if *own == pack)
        });
        let owner = owner.ok_or(GitError::Unavailable)?;
        let read = download::pack_bytes(self.node, &self.document, &pack, owner);
        self.importing = Some(pack.sha256);
        self.step = ProjectStep::Pack;
        Ok(Next::Call(IoCall::child(read)))
    }

    /// Each round answers every pair the previous one needed, so few rounds suffice.
    fn next_round(&mut self) -> ProjectTurn {
        if self.rounds >= MAX_ROUNDS {
            return Err(GitError::Unavailable);
        }
        self.rounds += 1;
        let id = self.id();
        if self.needs.is_empty() {
            return self.store(ProjectStep::Refs, GitEffect::Refs(id));
        }
        let effect = GitEffect::Ancestry {
            document_id: id,
            pairs: self.needs.clone(),
        };
        self.store(ProjectStep::Ancestry, effect)
    }

    fn finish(&mut self) -> ProjectTurn {
        let id = self.id();
        learn(id, &self.ancestry);
        if let Ok(mut served) = SERVED.lock() {
            served.insert(id, self.state.refs.clone());
        }
        Ok(Next::Done(Projection {
            state: std::mem::take(&mut self.state),
            records: std::mem::take(&mut self.records).into(),
            holders: std::mem::take(&mut self.holders),
            writes: self.writes,
        }))
    }
}

impl Operation for ProjectOperation {
    type Output = Projection;
    type Error = GitError;

    fn first(&mut self) -> ProjectTurn {
        let holders = publish::holders(self.node, &self.document);
        Ok(Next::Call(IoCall::child(holders)))
    }

    fn next(&mut self, result: IoResult) -> ProjectTurn {
        let id = self.id();
        match self.step {
            ProjectStep::Holders => {
                self.holders = result.child::<Vec<NodeId>, GitError>()??;
                self.store(ProjectStep::Initialize, GitEffect::Initialize(id))
            }
            ProjectStep::Initialize => {
                git_event(result)?;
                // Read first, so a record stored during this projection makes the next one run.
                self.writes = record_writes(id);
                self.step = ProjectStep::Records;
                Ok(Next::Call(IoCall::child(records::scan(id))))
            }
            ProjectStep::Records => {
                self.records = result.child::<Vec<GitRecord>, GitError>()??;
                (self.state, self.needs) = reduce(&self.records, &Ancestry::new());
                self.store(ProjectStep::Imported, GitEffect::Imported(id))
            }
            ProjectStep::Imported => {
                let GitEvent::Imported(known) = git_event(result)? else {
                    return Err(GitError::Unavailable);
                };
                let packs = self.state.packs.iter();
                self.missing = packs
                    .filter(|pack| !known.contains(&pack.sha256))
                    .cloned()
                    .collect();
                self.next_pack()
            }
            ProjectStep::Pack => {
                let pack = result.child::<Bytes, GitError>()??;
                let digest = self.importing.take().ok_or(IoError::Unfinished)?;
                let effect = GitEffect::Import {
                    document_id: id,
                    digest,
                    pack,
                };
                self.store(ProjectStep::Import, effect)
            }
            ProjectStep::Import => {
                git_event(result)?;
                self.next_pack()
            }
            ProjectStep::Ancestry => {
                let GitEvent::Ancestry(answers) = git_event(result)? else {
                    return Err(GitError::Unavailable);
                };
                let needs = std::mem::take(&mut self.needs);
                self.ancestry.extend(needs.into_iter().zip(answers));
                (self.state, self.needs) = reduce(&self.records, &self.ancestry);
                self.next_round()
            }
            ProjectStep::Refs => {
                let GitEvent::Refs(current) = git_event(result)? else {
                    return Err(GitError::Unavailable);
                };
                let effect = GitEffect::SetRefs {
                    document_id: id,
                    expected: current,
                    target: self.state.refs.clone(),
                };
                self.store(ProjectStep::SetRefs, effect)
            }
            ProjectStep::SetRefs => {
                git_event(result)?;
                self.finish()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::answered;
    use super::super::testing::{
        self, asks_child, asks_store, child, dataset, dataset_of, node, ran,
    };
    use super::*;
    use aruna_core::git::GitAnswer;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;

    fn document() -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([7; 32]);
        let document_id = Ulid::generate();
        MetadataRegistryRecord {
            realm_id,
            group_id: Ulid::from_parts(7, 1),
            document_id,
            document_path: "datasets/cached".into(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: String::new(),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from(1),
            last_event_id: Ulid::from(1),
        }
    }

    #[test]
    fn inputs_decide_reuse() {
        let node = Some(iroh::SecretKey::from_bytes(&[1; 32]).public());
        let document = document();
        let mut projection = Projection {
            state: GitState::default(),
            records: Default::default(),
            holders: Vec::new(),
            writes: 0,
        };
        projection
            .state
            .refs
            .insert("refs/heads/main".into(), "a".repeat(40));
        remember(node, &document, false, &projection);
        let reused = remembered(node, &document, false).expect("unchanged inputs reuse");
        assert_eq!(reused.state, projection.state);
        let other = Some(iroh::SecretKey::from_bytes(&[2; 32]).public());
        assert!(remembered(other, &document, false).is_none());
        // Other holders may generate once the revision is overdue, so that refreshes again.
        assert!(remembered(node, &document, true).is_none());
        let edited = MetadataRegistryRecord {
            last_event_id: Ulid::from(2),
            updated_at_ms: 2,
            ..document.clone()
        };
        assert!(remembered(node, &edited, false).is_none());
        forget(document.document_id);
        assert!(remembered(node, &document, false).is_none());
    }

    #[test]
    fn keeps_true_answers() {
        let id = Ulid::generate();
        let pair = |first: char, second: char| (first.to_string(), second.to_string());
        let ancestry = Ancestry::from([(pair('a', 'b'), true), (pair('b', 'c'), false)]);
        learn(id, &ancestry);
        // A missing commit answers false until its pack arrives, so false is asked again.
        assert_eq!(answered(id), Ancestry::from([(pair('a', 'b'), true)]));
        let many = (0..=MAX_ANSWERS)
            .map(|index| ((index.to_string(), "x".into()), true))
            .collect();
        learn(id, &many);
        assert!(answered(id).is_empty());
    }

    fn ran_refs() -> IoResult {
        ran(GitEvent::Refs(Default::default()))
    }

    // Nothing remembered means nothing to reuse and nothing to ask.
    #[test]
    fn recent_needs_memory() {
        let mut known = recent(Some(node(1)), &dataset_of(30), false);
        assert_eq!(known.first(), Ok(Next::Done(None)));
    }

    // A remembered projection is dropped when the local repository lost its files.
    #[test]
    fn recent_needs_repository() {
        let document = dataset_of(31);
        remember(Some(node(1)), &document, false, &testing::projection(&[]));
        let mut known = recent(Some(node(1)), &document, false);
        assert!(asks_store(&known.first()));
        let gone = answered(GitAnswer::Configured(false));
        assert_eq!(known.next(gone), Ok(Next::Done(None)));
    }

    // Unchanged holders and records reuse the projection; other holders do not.
    #[test]
    fn recent_compares_holders() {
        let document = dataset_of(32);
        let remembered = testing::projection(&[]);
        remember(Some(node(1)), &document, false, &remembered);
        let run = |holders: Vec<NodeId>| {
            let mut known = recent(Some(node(1)), &document, false);
            known.first().unwrap();
            assert!(asks_child(
                &known.next(answered(GitAnswer::Configured(true)))
            ));
            known.next(child(Ok::<_, GitError>(holders)))
        };
        let same = Ok(Next::Done(Some(remembered.clone())));
        assert_eq!(run(vec![node(1)]), same);
        assert_eq!(run(vec![node(2)]), Ok(Next::Done(None)));
    }

    /// A projection stepped to the answer of its pack listing.
    fn listed(records: Vec<GitRecord>) -> ProjectOperation {
        let mut run = project(Some(node(1)), &dataset());
        assert!(asks_child(&run.first()));
        let holders = child(Ok::<_, GitError>(vec![node(1)]));
        assert!(asks_store(&run.next(holders)));
        assert!(asks_child(&run.next(ran(GitEvent::Initialized))));
        let scanned = child(Ok::<_, GitError>(records));
        assert!(asks_store(&run.next(scanned)));
        run
    }

    // A repository without records ends with its refs set and the holders it read.
    #[test]
    fn project_sets_refs() {
        let mut run = listed(Vec::new());
        let imported = run.next(ran(GitEvent::Imported(Default::default())));
        assert!(asks_store(&imported));
        assert!(asks_store(&run.next(ran_refs())));
        let Ok(Next::Done(done)) = run.next(ran(GitEvent::Initialized)) else {
            panic!("expected the projection");
        };
        assert_eq!(done.holders, vec![node(1)]);
    }

    // A pack the cache lacks is read and imported before the refs move.
    #[test]
    fn project_imports_pack() {
        let pack = GitPack {
            sha256: "ab".repeat(32),
            size: 4,
        };
        let change = GitChange::Objects {
            pack: Some(pack),
            refs: Vec::new(),
            lfs: Vec::new(),
            revision: None,
            digest: None,
            made: Vec::new(),
        };
        let mut run = listed(vec![testing::record(1, change)]);
        assert!(asks_child(
            &run.next(ran(GitEvent::Imported(Default::default())))
        ));
        let bytes = child(Ok::<_, GitError>(Bytes::from_static(b"pack")));
        assert!(asks_store(&run.next(bytes)));
        assert!(asks_store(&run.next(ran(GitEvent::Initialized))));
    }

    // Ancestry questions that never settle end the projection.
    #[test]
    fn project_bounds_rounds() {
        let mut run = project(Some(node(1)), &dataset());
        run.rounds = MAX_ROUNDS;
        assert_eq!(run.next_round(), Err(GitError::Unavailable));
    }
}
