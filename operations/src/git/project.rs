//! Brings a holder's repository cache up to date from the document's Git records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::calls::{
    StageTimes, ancestry_answer, ancestry_call, configured_call, git_call, git_event, git_flag,
    kept_projection, projected_call, recent_call, writes_answer, writes_call,
};
use super::state::reduce;
use super::{GitError, download, publish, records};
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitChange, GitEffect, GitEvent, GitPack, GitRecord};
use aruna_core::git_state::{Ancestry, GitState, Projection, ProjectionInputs};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use std::collections::VecDeque;
use ulid::Ulid;

type Document = MetadataRegistryRecord;

/// How many rounds of ancestry answers a projection asks for.
const MAX_ROUNDS: usize = 64;

/// What a remembered projection of `document` depends on besides the records.
pub(super) fn inputs(node: Option<NodeId>, document: &Document, overdue: bool) -> ProjectionInputs {
    ProjectionInputs {
        node,
        last_event_id: document.last_event_id,
        updated_at_ms: document.updated_at_ms,
        overdue,
    }
}

/// What the pending call of checking a remembered projection answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecentStep {
    Kept,
    Configured,
    Holders,
    Writes,
}

/// The projection of the last refresh, if the document, its holders and its records,
/// replicated ones included, are unchanged since then. Reads no Git record.
pub(super) fn recent(node: Option<NodeId>, document: &Document, overdue: bool) -> RecentOperation {
    RecentOperation {
        node,
        document: document.clone(),
        overdue,
        known: None,
        holders: Vec::new(),
        step: RecentStep::Kept,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct RecentOperation {
    node: Option<NodeId>,
    document: Document,
    overdue: bool,
    known: Option<Projection>,
    holders: Vec<NodeId>,
    step: RecentStep,
}

impl Operation for RecentOperation {
    type Output = Option<Projection>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Option<Projection>>, GitError> {
        let kept = inputs(self.node, &self.document, self.overdue);
        Ok(Next::Call(recent_call(self.document.document_id, kept)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<Projection>>, GitError> {
        let id = self.document.document_id;
        match self.step {
            RecentStep::Kept => {
                self.known = kept_projection(result)?;
                if self.known.is_none() {
                    return Ok(Next::Done(None));
                }
                self.step = RecentStep::Configured;
                Ok(Next::Call(configured_call(id)))
            }
            RecentStep::Configured => {
                if !git_flag(result)? {
                    return Ok(Next::Done(None));
                }
                self.step = RecentStep::Holders;
                let holders = publish::holders(self.node, &self.document);
                Ok(Next::Call(IoCall::child(holders)))
            }
            RecentStep::Holders => {
                self.holders = result.child::<Vec<NodeId>, GitError>()??;
                self.step = RecentStep::Writes;
                Ok(Next::Call(writes_call(id)))
            }
            RecentStep::Writes => {
                let writes = writes_answer(result)?;
                let known = self.known.take().ok_or(IoError::Unfinished)?;
                let same = self.holders == known.holders && writes == known.writes;
                Ok(Next::Done(same.then_some(known)))
            }
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
    Writes,
    Records,
    Imported,
    Pack,
    Import,
    Known,
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
        stages: StageTimes::default(),
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
    stages: StageTimes,
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
            self.step = ProjectStep::Known;
            return Ok(Next::Call(ancestry_call(self.id())));
        };
        let owner = self.records.iter().find(|record| {
            matches!(&record.change, GitChange::Objects { pack: Some(own), .. } if *own == pack)
        });
        let owner = owner.ok_or(GitError::Unavailable)?;
        let read = download::pack_bytes(self.node, &self.document, &pack, owner);
        self.importing = Some(pack.sha256);
        self.step = ProjectStep::Pack;
        self.stages.start("git_pack_read");
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
        let ancestry = std::mem::take(&mut self.ancestry);
        let keep = projected_call(self.id(), ancestry, self.state.refs.clone());
        let projection = Projection {
            state: std::mem::take(&mut self.state),
            records: std::mem::take(&mut self.records).into(),
            holders: std::mem::take(&mut self.holders),
            writes: self.writes,
        };
        Ok(self.stages.finish(projection, vec![keep]))
    }
}

impl Operation for ProjectOperation {
    type Output = Projection;
    type Error = GitError;

    fn abort(&mut self) -> Vec<IoCall> {
        self.stages.report()
    }

    fn stamp(&mut self, now_ms: u64) {
        self.stages.stamp(now_ms);
    }

    fn first(&mut self) -> ProjectTurn {
        let holders = publish::holders(self.node, &self.document);
        Ok(Next::Call(IoCall::child(holders)))
    }

    fn next(&mut self, result: IoResult) -> ProjectTurn {
        let id = self.id();
        self.stages.stop();
        match self.step {
            ProjectStep::Holders => {
                self.holders = result.child::<Vec<NodeId>, GitError>()??;
                self.store(ProjectStep::Initialize, GitEffect::Initialize(id))
            }
            ProjectStep::Initialize => {
                git_event(result)?;
                self.step = ProjectStep::Writes;
                Ok(Next::Call(writes_call(id)))
            }
            ProjectStep::Writes => {
                // Read first, so a record stored during this projection makes the next one run.
                self.writes = writes_answer(result)?;
                self.step = ProjectStep::Records;
                self.stages.start("git_records");
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
            ProjectStep::Known => {
                self.ancestry = ancestry_answer(result)?;
                if !self.ancestry.is_empty() {
                    (self.state, self.needs) = reduce(&self.records, &self.ancestry);
                }
                self.next_round()
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
    use super::super::testing::{
        self, answered, asks_child, asks_store, child, dataset, dataset_of, finished, node, ran,
    };
    use super::*;
    use aruna_core::git::GitAnswer;

    fn kept(projection: Option<Projection>) -> IoResult {
        answered(GitAnswer::Projection(projection.map(Box::new)))
    }

    fn writes(count: u64) -> IoResult {
        answered(GitAnswer::RecordWrites(count))
    }

    // Nothing remembered means nothing to check.
    #[test]
    fn recent_needs_memory() {
        let mut known = recent(Some(node(1)), &dataset_of(30), false);
        assert!(asks_store(&known.first()));
        assert_eq!(known.next(kept(None)), Ok(Next::Done(None)));
    }

    // A remembered projection is dropped when the local repository lost its files.
    #[test]
    fn recent_needs_repository() {
        let mut known = recent(Some(node(1)), &dataset_of(31), false);
        known.first().unwrap();
        assert!(asks_store(
            &known.next(kept(Some(testing::projection(&[]))))
        ));
        let gone = answered(GitAnswer::Configured(false));
        assert_eq!(known.next(gone), Ok(Next::Done(None)));
    }

    // Unchanged holders and record writes reuse the projection; anything else does not.
    #[test]
    fn recent_compares_state() {
        let remembered = testing::projection(&[]);
        let run = |holders: Vec<NodeId>, count: u64| {
            let mut known = recent(Some(node(1)), &dataset_of(32), false);
            known.first().unwrap();
            known.next(kept(Some(testing::projection(&[])))).unwrap();
            let configured = answered(GitAnswer::Configured(true));
            assert!(asks_child(&known.next(configured)));
            assert!(asks_store(&known.next(child(Ok::<_, GitError>(holders)))));
            known.next(writes(count))
        };
        let same = Ok(Next::Done(Some(remembered)));
        assert_eq!(run(vec![node(1)], 0), same);
        assert_eq!(run(vec![node(2)], 0), Ok(Next::Done(None)));
        assert_eq!(run(vec![node(1)], 1), Ok(Next::Done(None)));
    }

    /// A projection stepped to the answer of its pack listing.
    fn listed(records: Vec<GitRecord>) -> ProjectOperation {
        let mut run = project(Some(node(1)), &dataset());
        run.stamp(100);
        assert!(asks_child(&run.first()));
        let holders = child(Ok::<_, GitError>(vec![node(1)]));
        assert!(asks_store(&run.next(holders)));
        assert!(asks_store(&run.next(ran(GitEvent::Initialized))));
        assert!(asks_child(&run.next(writes(3))));
        run.stamp(125);
        let scanned = child(Ok::<_, GitError>(records));
        assert!(asks_store(&run.next(scanned)));
        run
    }

    fn known_ancestry() -> IoResult {
        answered(GitAnswer::Ancestry(Ancestry::new()))
    }

    // A repository without records ends with its refs set, its answers kept for the store.
    #[test]
    fn project_sets_refs() {
        let mut run = listed(Vec::new());
        let imported = run.next(ran(GitEvent::Imported(Default::default())));
        assert!(asks_store(&imported));
        assert!(asks_store(&run.next(known_ancestry())));
        assert!(asks_store(
            &run.next(ran(GitEvent::Refs(Default::default())))
        ));
        let (done, calls) = finished(run.next(ran(GitEvent::Initialized)));
        assert_eq!(done.holders, vec![node(1)]);
        assert_eq!(done.writes, 3);
        // The kept answers and the record read time.
        assert_eq!(calls, 2);
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
        assert_eq!(run.step, ProjectStep::Known);
    }

    // Ancestry questions that never settle end the projection.
    #[test]
    fn project_bounds_rounds() {
        let mut run = project(Some(node(1)), &dataset());
        run.rounds = MAX_ROUNDS;
        assert_eq!(run.next_round(), Err(GitError::Unavailable));
    }

    // A failed projection still reports the stages it finished.
    #[test]
    fn abort_reports_stages() {
        let mut run = listed(Vec::new());
        assert_eq!(run.abort().len(), 1);
        assert!(run.abort().is_empty());
    }
}
