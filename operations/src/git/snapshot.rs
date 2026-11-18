//! Publishes ARC snapshots and checkpoints as Git records on document holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::git::{
    CHECKPOINT_AFTER, GitEffect, GitEvent, GitRecord, GitStatus, PENDING, PendingMerge, STATUS,
};
use aruna_core::git_state::{GitState, Projection};
use aruna_core::keyspaces::{MATERIALIZATION_CAPTURE_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE};
use aruna_core::metadata::{
    MaterializationState, MaterializationStatusRecord, MetadataEffect, MetadataEvent,
    MetadataRawRevision,
};
use aruna_core::operation::{HoldScope, Operation, Runnable, boxed_suboperation};
use aruna_core::repo_layout::{Layout, metadata_layout};
use aruna_core::storage_entries::{document_job_prefix, materialization_status_key};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use tracing::warn;
use ulid::Ulid;

use super::access::Repository;
use super::calls::{
    StageTimes, busy_call, canonical_answer, canonical_call, configured_call, forget_call,
    git_call, git_event, git_flag, has_store, kept_projection, last_call, metadata_call,
    metadata_event, noted, refresh_once_call, remember_call, timed_call, timed_outcome,
};
use super::generate::{checkpoint, generate};
use super::project::{inputs, project, recent};
use super::{GitError, GitNode, document, pending, publish, records, repository};
use crate::metadata::raw_events::RawReadError;
use crate::metadata::revision_read::{MetadataRawView, RawDisplayOperation};

type Document = MetadataRegistryRecord;
type PendingRows = Vec<(Vec<u8>, PendingMerge)>;

/// How long other holders leave a new revision to the first holder before generating it.
const FAILOVER_MS: u64 = 60_000;
/// Background snapshot work at least this long is logged with its stages.
const SLOW_WORK_MS: u64 = 300;

fn overdue(now_ms: u64, document: &Document) -> bool {
    now_ms.saturating_sub(document.updated_at_ms) > FAILOVER_MS
}

fn pending_rows(document: &Document) -> IoCall {
    let prefix = document.document_id.to_bytes().to_vec();
    IoCall::child(records::prefixed::<PendingMerge>(PENDING, prefix))
}

/// What the pending call of reading the current graph answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CurrentStep {
    Capture,
    Raw,
    Status,
    Export,
}

/// The materialized graph this node would snapshot, if it is ready. During materialization
/// the graph is current before its status says so, which `materializing` states.
pub(super) fn current(
    document: &Document,
    revision: Option<&MetadataRawRevision>,
    materializing: bool,
) -> CurrentOperation {
    CurrentOperation {
        document: document.clone(),
        revision: revision.cloned(),
        materializing,
        step: CurrentStep::Capture,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct CurrentOperation {
    document: Document,
    revision: Option<MetadataRawRevision>,
    materializing: bool,
    step: CurrentStep,
}

type Source = Option<(Ulid, String)>;
type CurrentTurn = Result<Next<Source>, GitError>;

impl CurrentOperation {
    fn read_raw(&mut self) -> CurrentTurn {
        if let Some(raw) = self.revision.take() {
            return Ok(Next::Done(Some((raw.winning_event_id, raw.jsonld))));
        }
        self.step = CurrentStep::Raw;
        let read = RawDisplayOperation::new(self.document.document_id);
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for CurrentOperation {
    type Output = Source;
    type Error = GitError;

    fn first(&mut self) -> CurrentTurn {
        if self.materializing {
            return self.read_raw();
        }
        // A job that published its graph, a late merge too, stores it only when it finishes, so
        // until then the raw view can be older. A job still waiting for dependencies holds nothing.
        Ok(Next::Call(IoCall::Page {
            space: MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
            prefix: document_job_prefix(self.document.document_id),
            after: None,
            limit: 1,
        }))
    }

    fn next(&mut self, result: IoResult) -> CurrentTurn {
        let document = &self.document;
        match self.step {
            CurrentStep::Capture => {
                let (rows, _) = result.page()?;
                if !rows.is_empty() {
                    return Ok(Next::Done(None));
                }
                self.read_raw()
            }
            CurrentStep::Raw => {
                let raw = result.child::<Option<MetadataRawView>, RawReadError>()?;
                let raw = raw.map_err(|_| GitError::Unavailable)?;
                if let Some(view) = raw {
                    let raw = view.revision;
                    return Ok(Next::Done(Some((raw.winning_event_id, raw.jsonld))));
                }
                let key = materialization_status_key(document.document_id).to_vec();
                let load = records::load::<MaterializationStatusRecord>(
                    MATERIALIZATION_STATUS_KEYSPACE,
                    key,
                );
                self.step = CurrentStep::Status;
                Ok(Next::Call(IoCall::child(load)))
            }
            CurrentStep::Status => {
                let status = result.child::<Option<MaterializationStatusRecord>, GitError>()??;
                let materializing = self.materializing;
                let ready = status.is_some_and(|status| {
                    (materializing || status.state == MaterializationState::Materialized)
                        && status.event_id == document.last_event_id
                });
                if !ready {
                    return Ok(Next::Done(None));
                }
                let graph_iri = document.graph_iri.clone();
                self.step = CurrentStep::Export;
                Ok(Next::Call(metadata_call(MetadataEffect::ExportRoCrate {
                    graph_iri,
                })))
            }
            CurrentStep::Export => match metadata_event(result)? {
                MetadataEvent::RoCrateExportResult { jsonld, .. } => {
                    Ok(Next::Done(Some((document.last_event_id, jsonld))))
                }
                _ => Err(GitError::Unavailable),
            },
        }
    }
}

/// What the pending call of updating a repository answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UpdateStep {
    Project,
    Pending,
    Current,
    Canonical,
    Misplaced,
    Generate,
    Reproject,
    Checkpoint,
    Final,
}

/// Projects the records, applies pushed metadata, then publishes a missing snapshot or a due
/// checkpoint. The first holder acts at once; the others take over when a revision stays
/// unpublished too long.
fn update(
    node: &GitNode,
    document: &Document,
    revision: Option<MetadataRawRevision>,
    materializing: bool,
) -> UpdateOperation {
    UpdateOperation {
        node: node.clone(),
        document: document.clone(),
        revision,
        materializing,
        stages: StageTimes::default(),
        step: UpdateStep::Project,
        projection: None,
        source: None,
    }
}

#[derive(Debug, PartialEq)]
struct UpdateOperation {
    node: GitNode,
    document: Document,
    revision: Option<MetadataRawRevision>,
    materializing: bool,
    stages: StageTimes,
    step: UpdateStep,
    projection: Option<Projection>,
    source: Source,
}

type UpdateTurn = Result<Next<Projection>, GitError>;

impl UpdateOperation {
    fn projection(&self) -> Result<&Projection, IoError> {
        self.projection.as_ref().ok_or(IoError::Unfinished)
    }

    fn leading(&self) -> Result<bool, IoError> {
        Ok(self.node.node() == self.projection()?.holders.first().copied())
    }

    fn project(&mut self, step: UpdateStep) -> UpdateTurn {
        self.step = step;
        let run = project(self.node.node(), &self.document);
        Ok(Next::Call(IoCall::child(run)))
    }

    fn generate(&mut self, digest: Option<[u8; 32]>) -> UpdateTurn {
        let source = self.source.take().ok_or(IoError::Unfinished)?;
        self.step = UpdateStep::Generate;
        let run = generate(
            self.node.ids,
            &self.document,
            self.projection()?,
            source,
            digest,
        );
        Ok(Next::Call(IoCall::child(run)))
    }

    /// The graph's content decides, so a late older edit that changes it is captured too.
    fn canonicalize(&mut self, source: Source) -> UpdateTurn {
        let Some(jsonld) = source.as_ref().map(|(_, jsonld)| jsonld.clone()) else {
            return self.fold();
        };
        self.source = source;
        self.step = UpdateStep::Canonical;
        Ok(Next::Call(canonical_call(jsonld)))
    }

    fn with_digest(&mut self, digest: Option<[u8; 32]>) -> UpdateTurn {
        let due = self.leading()? || overdue(self.stages.now_ms(), &self.document);
        let Some(digest) = digest.filter(|_| due) else {
            self.source = None;
            return self.fold();
        };
        if self.projection()?.state.digest != Some(digest) {
            return self.generate(Some(digest));
        }
        match self.misplaced_call()? {
            Some(call) => {
                self.step = UpdateStep::Misplaced;
                Ok(Next::Call(call))
            }
            None => self.fold(),
        }
    }

    /// Asks the layout of a main that Aruna made as an ARC although the metadata has no ARC
    /// markers, as earlier versions did for every dataset; such a main becomes a plain RO-Crate.
    fn misplaced_call(&self) -> Result<Option<IoCall>, GitError> {
        let (_, jsonld) = self.source.as_ref().ok_or(IoError::Unfinished)?;
        let state = &self.projection()?.state;
        let Some(main) = state.refs.get("refs/heads/main") else {
            return Ok(None);
        };
        let plain = serde_json::from_str(jsonld)
            .is_ok_and(|value| metadata_layout(&value) == Layout::RoCrate);
        if !plain || !state.made.contains(main) {
            return Ok(None);
        }
        let effect = GitEffect::Layout {
            document_id: self.document.document_id,
            revision: main.clone(),
        };
        Ok(Some(git_call(effect, UserId::nil(self.document.realm_id))))
    }

    fn fold(&mut self) -> UpdateTurn {
        let projection = self.projection()?;
        let uncovered = publish::uncovered(&projection.records).len();
        let leading = self.leading()?;
        if uncovered > CHECKPOINT_AFTER && (leading || uncovered > 2 * CHECKPOINT_AFTER) {
            let fold = checkpoint(self.node.ids, &self.document, projection);
            self.step = UpdateStep::Checkpoint;
            return Ok(Next::Call(IoCall::child(fold)));
        }
        self.finish()
    }

    fn finish(&mut self) -> UpdateTurn {
        let projection = self.projection.take().ok_or(IoError::Unfinished)?;
        Ok(self.stages.finish(projection, Vec::new()))
    }
}

impl Operation for UpdateOperation {
    type Output = Projection;
    type Error = GitError;

    fn abort(&mut self) -> Vec<IoCall> {
        self.stages.report()
    }

    fn stamp(&mut self, now_ms: u64) {
        self.stages.stamp(now_ms);
    }

    fn first(&mut self) -> UpdateTurn {
        self.stages.start("git_project");
        self.project(UpdateStep::Project)
    }

    fn next(&mut self, result: IoResult) -> UpdateTurn {
        self.stages.stop();
        match self.step {
            UpdateStep::Project => {
                self.projection = Some(result.child::<Projection, GitError>()??);
                self.step = UpdateStep::Pending;
                let apply = pending::apply(&self.node, &self.document);
                Ok(Next::Call(IoCall::child(apply)))
            }
            UpdateStep::Pending => {
                if let Err(error) = result.child::<(), GitError>()? {
                    let document_id = self.document.document_id;
                    warn!(%document_id, %error, "Pushed metadata waits");
                }
                self.step = UpdateStep::Current;
                self.stages.start("git_source");
                let revision = self.revision.as_ref();
                let read = current(&self.document, revision, self.materializing);
                Ok(Next::Call(IoCall::child(read)))
            }
            UpdateStep::Current => self.canonicalize(result.child::<Source, GitError>()??),
            UpdateStep::Canonical => self.with_digest(canonical_answer(result)?),
            UpdateStep::Misplaced => {
                // A switch records no digest, so the next refresh confirms the new snapshot.
                match git_event(result)? {
                    GitEvent::Layout(Some(Layout::Arc)) => self.generate(None),
                    _ => self.fold(),
                }
            }
            UpdateStep::Generate => {
                result.child::<(), GitError>()??;
                self.project(UpdateStep::Reproject)
            }
            UpdateStep::Reproject => {
                self.projection = Some(result.child::<Projection, GitError>()??);
                self.fold()
            }
            // A missing checkpoint only delays folding; it must never fail the request.
            UpdateStep::Checkpoint => match result.child::<GitRecord, GitError>()? {
                Ok(_) => self.project(UpdateStep::Final),
                Err(error) => {
                    warn!(%error, "Git checkpoint not written");
                    self.finish()
                }
            },
            UpdateStep::Final => {
                self.projection = Some(result.child::<Projection, GitError>()??);
                self.finish()
            }
        }
    }
}

/// What the pending call of a refresh answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RefreshStep {
    Recent,
    Update,
    Pending,
}

/// Brings the repository up to date, or reuses the last refresh when nothing changed. The
/// caller holds the document's repository.
pub fn refresh(node: &GitNode, document: &Document) -> RefreshOperation {
    RefreshOperation {
        node: node.clone(),
        document: document.clone(),
        now_ms: 0,
        step: RefreshStep::Recent,
        projection: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct RefreshOperation {
    node: GitNode,
    document: Document,
    now_ms: u64,
    step: RefreshStep,
    projection: Option<Projection>,
}

impl RefreshOperation {
    fn overdue(&self) -> bool {
        overdue(self.now_ms, &self.document)
    }
}

/// Keeps a projection for later refreshes under the state it was made in.
fn keep_call(node: &GitNode, document: &Document, due: bool, projection: &Projection) -> IoCall {
    let made = inputs(node.node(), document, due);
    remember_call(document.document_id, made, projection)
}

impl Operation for RefreshOperation {
    type Output = Projection;
    type Error = GitError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> UpdateTurn {
        let known = recent(self.node.node(), &self.document, self.overdue());
        Ok(Next::Call(IoCall::child(known)))
    }

    fn next(&mut self, result: IoResult) -> UpdateTurn {
        match self.step {
            RefreshStep::Recent => {
                if let Some(known) = result.child::<Option<Projection>, GitError>()?? {
                    return Ok(Next::Done(known));
                }
                self.step = RefreshStep::Update;
                let run = update(&self.node, &self.document, None, false);
                Ok(Next::Call(IoCall::child(run)))
            }
            RefreshStep::Update => {
                self.projection = Some(result.child::<Projection, GitError>()??);
                self.step = RefreshStep::Pending;
                Ok(Next::Call(pending_rows(&self.document)))
            }
            RefreshStep::Pending => {
                // Pushed metadata that still waits must be retried by the next refresh.
                let rows = result.child::<PendingRows, GitError>()?;
                let output = self.projection.take().ok_or(IoError::Unfinished)?;
                if !rows.is_ok_and(|rows| rows.is_empty()) {
                    return Ok(Next::Done(output));
                }
                let due = self.overdue();
                let keep = keep_call(&self.node, &self.document, due, &output);
                Ok(Next::Then {
                    output,
                    calls: vec![keep],
                })
            }
        }
    }
}

/// Refreshes one document under its repository hold and logs a failure.
pub(super) fn held_refresh(node: &GitNode, document: &Document) -> HeldRefreshOperation {
    HeldRefreshOperation {
        refresh: Some(refresh(node, document)),
        document_id: document.document_id,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct HeldRefreshOperation {
    refresh: Option<RefreshOperation>,
    document_id: Ulid,
}

impl Operation for HeldRefreshOperation {
    type Output = ();
    type Error = GitError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::GitRepository(self.document_id))
    }

    fn first(&mut self) -> Result<Next<()>, GitError> {
        let refresh = self.refresh.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(refresh)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        if let Err(error) = result.child::<Projection, GitError>()? {
            warn!(document_id = %self.document_id, %error, "Git refresh failed");
        }
        Ok(Next::Done(()))
    }
}

/// Runs background snapshot work under its own stage timers and logs them when it is slow.
fn timed_work<O, K>(document_id: Ulid, work: O) -> TimedWorkOperation
where
    O: Runnable<K, Output = (), Error = GitError> + 'static,
    K: 'static,
{
    TimedWorkOperation {
        document_id,
        call: Some(timed_call(work)),
        now_ms: 0,
        started_ms: 0,
    }
}

#[derive(Debug, PartialEq)]
struct TimedWorkOperation {
    document_id: Ulid,
    call: Option<IoCall>,
    now_ms: u64,
    started_ms: u64,
}

impl Operation for TimedWorkOperation {
    type Output = ();
    type Error = GitError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<()>, GitError> {
        self.started_ms = self.now_ms;
        Ok(Next::Call(self.call.take().ok_or(IoError::Unfinished)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        let (outcome, stages) = timed_outcome::<(), GitError>(result)?;
        let total_ms = self.now_ms.saturating_sub(self.started_ms);
        if total_ms >= SLOW_WORK_MS {
            tracing::info!(document_id = %self.document_id, total_ms, %stages,
                "Slow Git snapshot work");
        }
        outcome.ok_or(GitError::Unavailable)??;
        Ok(Next::Done(()))
    }
}

/// Starts the full refresh once beside the caller, while a background slot is free. A lost
/// run is found again by the next read; what it acts on is stored durably.
fn background_call(node: &GitNode, document: &Document) -> IoCall {
    let id = document.document_id;
    let run = timed_work(id, held_refresh(node, document));
    refresh_once_call(id, boxed_suboperation(run, |_| Event::Delayed))
}

/// What the pending call of viewing a repository answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ViewStep {
    Recent,
    Project,
    Pending,
    Background,
}

/// Like [`refresh`] for reads, which never wait for a snapshot. When the records lag behind
/// the metadata, pushed metadata waits or a checkpoint is due, the full refresh runs in the
/// background and `true` marks the projection as pending. Holds the repository.
pub(super) fn view(node: &GitNode, document: &Document) -> ViewOperation {
    ViewOperation {
        node: node.clone(),
        document: document.clone(),
        now_ms: 0,
        step: ViewStep::Recent,
        projection: None,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct ViewOperation {
    node: GitNode,
    document: Document,
    now_ms: u64,
    step: ViewStep,
    projection: Option<Projection>,
}

type Viewed = (Projection, bool);
type ViewTurn = Result<Next<Viewed>, GitError>;

impl ViewOperation {
    fn overdue(&self) -> bool {
        overdue(self.now_ms, &self.document)
    }

    fn settle(&mut self, waiting: bool) -> ViewTurn {
        let projection = self.projection.as_ref().ok_or(IoError::Unfinished)?;
        let pending = waiting
            || projection.state.revision != Some(self.document.last_event_id)
            || publish::uncovered(&projection.records).len() > CHECKPOINT_AFTER;
        if pending {
            self.step = ViewStep::Background;
            return Ok(Next::Call(background_call(&self.node, &self.document)));
        }
        let keep = keep_call(&self.node, &self.document, self.overdue(), projection);
        let projection = self.projection.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Then {
            output: (projection, false),
            calls: vec![keep],
        })
    }
}

impl Operation for ViewOperation {
    type Output = Viewed;
    type Error = GitError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::GitRepository(self.document.document_id))
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> ViewTurn {
        let known = recent(self.node.node(), &self.document, self.overdue());
        Ok(Next::Call(IoCall::child(known)))
    }

    fn next(&mut self, result: IoResult) -> ViewTurn {
        match self.step {
            ViewStep::Recent => {
                if let Some(known) = result.child::<Option<Projection>, GitError>()?? {
                    return Ok(Next::Done((known, false)));
                }
                self.step = ViewStep::Project;
                let run = project(self.node.node(), &self.document);
                Ok(Next::Call(IoCall::child(run)))
            }
            ViewStep::Project => {
                self.projection = Some(result.child::<Projection, GitError>()??);
                self.step = ViewStep::Pending;
                Ok(Next::Call(pending_rows(&self.document)))
            }
            ViewStep::Pending => {
                let rows = result.child::<PendingRows, GitError>()??;
                self.settle(!rows.is_empty())
            }
            // Whether a refresh started changes nothing: the next read asks again.
            ViewStep::Background => {
                let projection = self.projection.take().ok_or(IoError::Unfinished)?;
                Ok(Next::Done((projection, true)))
            }
        }
    }
}

/// What the pending call of a read view answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadStep {
    Busy,
    Last,
    View,
}

/// [`view`] without waiting behind a running snapshot: while one holds the repository, the last
/// completed projection is served as pending. Only a document never projected here waits.
pub(super) fn read_view(node: &GitNode, document: &Document) -> ReadViewOperation {
    ReadViewOperation {
        node: node.clone(),
        document: document.clone(),
        step: ReadStep::Busy,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct ReadViewOperation {
    node: GitNode,
    document: Document,
    step: ReadStep,
}

impl ReadViewOperation {
    fn view(&mut self) -> ViewTurn {
        self.step = ReadStep::View;
        Ok(Next::Call(IoCall::child(view(&self.node, &self.document))))
    }
}

impl Operation for ReadViewOperation {
    type Output = Viewed;
    type Error = GitError;

    fn first(&mut self) -> ViewTurn {
        Ok(Next::Call(busy_call(self.document.document_id)))
    }

    fn next(&mut self, result: IoResult) -> ViewTurn {
        match self.step {
            ReadStep::Busy => {
                if !git_flag(result)? {
                    return self.view();
                }
                self.step = ReadStep::Last;
                Ok(Next::Call(last_call(self.document.document_id)))
            }
            ReadStep::Last => match kept_projection(result)? {
                Some(projection) => Ok(Next::Done((projection, true))),
                None => self.view(),
            },
            ReadStep::View => Ok(Next::Done(result.child::<Viewed, GitError>()??)),
        }
    }
}

/// Called after materialization; nodes that do not hold the document have nothing to do.
pub fn capture(
    node: &GitNode,
    record: &Document,
    revision: Option<&MetadataRawRevision>,
) -> CaptureOperation {
    CaptureOperation {
        held: Some(HeldCaptureOperation {
            node: node.clone(),
            record: record.clone(),
            revision: revision.cloned(),
            forgotten: false,
        }),
        document_id: record.document_id,
    }
}

#[derive(Debug, PartialEq)]
pub struct CaptureOperation {
    held: Option<HeldCaptureOperation>,
    document_id: Ulid,
}

impl Operation for CaptureOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> Result<Next<()>, GitError> {
        Ok(Next::Call(configured_call(self.document_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        let Some(held) = self.held.take() else {
            result.child::<(), GitError>()??;
            return Ok(Next::Done(()));
        };
        if !has_store(result)? {
            return Ok(Next::Done(()));
        }
        let work = timed_work(self.document_id, held);
        Ok(Next::Call(IoCall::child(work)))
    }
}

/// Snapshots the materialized graph under the repository hold.
#[derive(Debug, PartialEq)]
struct HeldCaptureOperation {
    node: GitNode,
    record: Document,
    revision: Option<MetadataRawRevision>,
    forgotten: bool,
}

impl Operation for HeldCaptureOperation {
    type Output = ();
    type Error = GitError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::GitRepository(self.record.document_id))
    }

    fn first(&mut self) -> Result<Next<()>, GitError> {
        Ok(Next::Call(forget_call(self.record.document_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        if !self.forgotten {
            self.forgotten = true;
            noted(result)?;
            let run = update(&self.node, &self.record, self.revision.take(), true);
            return Ok(Next::Call(IoCall::child(run)));
        }
        match result.child::<Projection, GitError>()? {
            Ok(_) | Err(GitError::NotHolder) => Ok(Next::Done(())),
            Err(error) => Err(error),
        }
    }
}

/// The snapshot status, the projection, the layout of main when main exists, the document and
/// whether a newer snapshot is still being made.
pub type StatusView = (
    Option<GitStatus>,
    Projection,
    Option<Layout>,
    Document,
    bool,
);

/// What the pending call of reading the repository status answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StatusStep {
    Repository,
    Busy,
    Last,
    View,
    Status,
    Layout,
}

/// Reads the repository status. It never waits for a first snapshot either; a repository
/// another run holds reports its last projection, or an empty pending state.
pub fn status(node: &GitNode, auth: &AuthContext, id: Ulid) -> StatusOperation {
    StatusOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        step: StatusStep::Repository,
        document: None,
        viewed: None,
        status: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct StatusOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    step: StatusStep,
    document: Option<Document>,
    viewed: Option<Viewed>,
    status: Option<GitStatus>,
}

type StatusTurn = Result<Next<StatusView>, GitError>;

impl StatusOperation {
    fn read_status(&mut self, viewed: Viewed) -> StatusTurn {
        self.viewed = Some(viewed);
        self.step = StatusStep::Status;
        let key = self.id.to_bytes().to_vec();
        Ok(Next::Call(IoCall::child(records::load::<GitStatus>(
            STATUS, key,
        ))))
    }

    fn finish(&mut self, layout: Option<Layout>) -> StatusTurn {
        let (projection, pending) = self.viewed.take().ok_or(IoError::Unfinished)?;
        let document = self.document.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Done((
            self.status.take(),
            projection,
            layout,
            document,
            pending,
        )))
    }
}

impl Operation for StatusOperation {
    type Output = StatusView;
    type Error = GitError;

    fn first(&mut self) -> StatusTurn {
        let read = repository(self.node.node(), &self.auth, self.id, Permission::READ);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> StatusTurn {
        match self.step {
            StatusStep::Repository => {
                let (document, _) = result.child::<Repository, GitError>()??;
                self.document = Some(document);
                self.step = StatusStep::Busy;
                Ok(Next::Call(busy_call(self.id)))
            }
            StatusStep::Busy => {
                if !git_flag(result)? {
                    let document = self.document.as_ref().ok_or(IoError::Unfinished)?;
                    let run = view(&self.node, document);
                    self.step = StatusStep::View;
                    return Ok(Next::Call(IoCall::child(run)));
                }
                self.step = StatusStep::Last;
                Ok(Next::Call(last_call(self.id)))
            }
            StatusStep::Last => {
                let projection = kept_projection(result)?.unwrap_or_else(|| Projection {
                    state: GitState::default(),
                    records: Default::default(),
                    holders: Vec::new(),
                    writes: 0,
                });
                self.read_status((projection, true))
            }
            StatusStep::View => {
                let viewed = result.child::<Viewed, GitError>()??;
                self.read_status(viewed)
            }
            StatusStep::Status => {
                self.status = result.child::<Option<GitStatus>, GitError>()??;
                let viewed = self.viewed.as_ref().ok_or(IoError::Unfinished)?;
                let Some(main) = viewed.0.state.refs.get("refs/heads/main") else {
                    return self.finish(None);
                };
                let effect = GitEffect::Layout {
                    document_id: self.id,
                    revision: main.clone(),
                };
                self.step = StatusStep::Layout;
                Ok(Next::Call(git_call(effect, self.auth.user_id)))
            }
            StatusStep::Layout => match git_event(result)? {
                GitEvent::Layout(layout) => self.finish(layout),
                _ => Err(GitError::Unavailable),
            },
        }
    }
}

/// What the pending call of exporting a revision answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExportStep {
    Document,
    View,
    Resolve,
    Export,
}

/// Exports one revision as RO-Crate metadata.
pub fn export(node: &GitNode, auth: &AuthContext, id: Ulid, revision: String) -> ExportOperation {
    ExportOperation {
        node: node.clone(),
        auth: auth.clone(),
        id,
        revision,
        step: ExportStep::Document,
    }
}

#[derive(Debug, PartialEq)]
pub struct ExportOperation {
    node: GitNode,
    auth: AuthContext,
    id: Ulid,
    revision: String,
    step: ExportStep,
}

impl Operation for ExportOperation {
    type Output = Bytes;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Bytes>, GitError> {
        let access = document(&self.auth, self.id, Permission::READ);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Bytes>, GitError> {
        let actor = self.auth.user_id;
        match self.step {
            ExportStep::Document => {
                let document = result.child::<Document, GitError>()??;
                self.step = ExportStep::View;
                Ok(Next::Call(IoCall::child(read_view(&self.node, &document))))
            }
            ExportStep::View => {
                result.child::<Viewed, GitError>()??;
                let known = GitEffect::Resolve {
                    document_id: self.id,
                    revision: self.revision.clone(),
                };
                self.step = ExportStep::Resolve;
                Ok(Next::Call(git_call(known, actor)))
            }
            ExportStep::Resolve => {
                if !matches!(git_event(result)?, GitEvent::Resolved(Some(_))) {
                    return Err(GitError::NotFound);
                }
                let effect = GitEffect::Export {
                    document_id: self.id,
                    revision: std::mem::take(&mut self.revision),
                };
                self.step = ExportStep::Export;
                Ok(Next::Call(git_call(effect, actor)))
            }
            ExportStep::Export => {
                let GitEvent::Exported(bytes) = git_event(result)? else {
                    return Err(GitError::Unavailable);
                };
                let exported: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(|_| GitError::Unavailable)?;
                if exported.get("error").is_some() {
                    return Err(GitError::Invalid);
                }
                Ok(Next::Done(bytes))
            }
        }
    }
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
