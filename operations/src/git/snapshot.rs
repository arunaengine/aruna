//! Publishes ARC snapshots and checkpoints as Git records on document holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::git::{
    CHECKPOINT_AFTER, GitEffect, GitEvent, GitRecord, GitStatus, PENDING, PendingMerge, STATUS,
};
use aruna_core::keyspaces::{MATERIALIZATION_CAPTURE_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE};
use aruna_core::metadata::{
    MaterializationState, MaterializationStatusRecord, MetadataEffect, MetadataEvent,
    MetadataRawRevision,
};
use aruna_core::operation::{HoldScope, Operation, boxed_suboperation};
use aruna_core::repo_layout::{Layout, metadata_layout};
use aruna_core::storage_entries::{document_job_prefix, materialization_status_key};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use tracing::warn;
use ulid::Ulid;

use super::access::Repository;
use super::calls::{
    busy_call, configured_call, git_call, git_event, git_flag, has_store, metadata_call,
    metadata_event,
};
use super::generate::{checkpoint, generate};
use super::project::{Projection, forget, last, project, recent, remember};
use super::state::GitState;
use super::{GitError, GitNode, document, pending, publish, records, repository};
use crate::metadata::raw_events::RawReadError;
use crate::metadata::revision_read::{MetadataRawView, RawDisplayOperation};

type Document = MetadataRegistryRecord;
type PendingRows = Vec<(Vec<u8>, PendingMerge)>;

/// How long other holders leave a new revision to the first holder before generating it.
const FAILOVER_MS: u64 = 60_000;

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
        now_ms: 0,
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
    now_ms: u64,
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
    fn with_source(&mut self, source: Source) -> UpdateTurn {
        let Some(source) = source else {
            return self.fold();
        };
        let canonical = craqle::canonicalize_jsonld(&source.1);
        let due = self.leading()? || overdue(self.now_ms, &self.document);
        let Some(canonical) = canonical.ok().filter(|_| due) else {
            return self.fold();
        };
        let stale = self.projection()?.state.digest != Some(canonical.digest);
        let jsonld = source.1.clone();
        self.source = Some(source);
        if stale {
            return self.generate(Some(canonical.digest));
        }
        match self.misplaced_call(&jsonld)? {
            Some(call) => {
                self.step = UpdateStep::Misplaced;
                Ok(Next::Call(call))
            }
            None => self.fold(),
        }
    }

    /// Asks the layout of a main that Aruna made as an ARC although the metadata has no ARC
    /// markers, as earlier versions did for every dataset; such a main becomes a plain RO-Crate.
    fn misplaced_call(&self, jsonld: &str) -> Result<Option<IoCall>, GitError> {
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
        Ok(Next::Done(
            self.projection.take().ok_or(IoError::Unfinished)?,
        ))
    }
}

impl Operation for UpdateOperation {
    type Output = Projection;
    type Error = GitError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> UpdateTurn {
        self.project(UpdateStep::Project)
    }

    fn next(&mut self, result: IoResult) -> UpdateTurn {
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
                let revision = self.revision.as_ref();
                let read = current(&self.document, revision, self.materializing);
                Ok(Next::Call(IoCall::child(read)))
            }
            UpdateStep::Current => self.with_source(result.child::<Source, GitError>()??),
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
            UpdateStep::Final => Ok(Next::Done(result.child::<Projection, GitError>()??)),
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
                let projection = self.projection.take().ok_or(IoError::Unfinished)?;
                if rows.is_ok_and(|rows| rows.is_empty()) {
                    let due = self.overdue();
                    remember(self.node.node(), &self.document, due, &projection);
                }
                Ok(Next::Done(projection))
            }
        }
    }
}

/// Documents with a background refresh queued or running on this node.
static REFRESHING: LazyLock<Mutex<HashSet<Ulid>>> = LazyLock::new(Default::default);

/// Marks one document's background refresh as running until it is dropped.
#[derive(Debug, PartialEq)]
struct RefreshClaim(Ulid);

impl RefreshClaim {
    fn take(document_id: Ulid) -> Option<Self> {
        let mut running = REFRESHING.lock().ok()?;
        running.insert(document_id).then_some(Self(document_id))
    }
}

impl Drop for RefreshClaim {
    fn drop(&mut self) {
        if let Ok(mut running) = REFRESHING.lock() {
            running.remove(&self.0);
        }
    }
}

/// Refreshes one document under its repository hold and logs a failure.
pub(super) fn held_refresh(node: &GitNode, document: &Document) -> HeldRefreshOperation {
    HeldRefreshOperation {
        refresh: Some(refresh(node, document)),
        document_id: document.document_id,
        claim: None,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct HeldRefreshOperation {
    refresh: Option<RefreshOperation>,
    document_id: Ulid,
    claim: Option<RefreshClaim>,
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
        self.claim = None;
        Ok(Next::Done(()))
    }
}

/// Starts the full refresh once beside the caller, while a background slot is free. A lost
/// run is found again by the next read; what it acts on is stored durably.
fn background_call(node: &GitNode, document: &Document) -> Option<IoCall> {
    let claim = RefreshClaim::take(document.document_id)?;
    let run = HeldRefreshOperation {
        claim: Some(claim),
        ..held_refresh(node, document)
    };
    let child = boxed_suboperation(run, |_| Event::Delayed);
    Some(Effect::Background(child).into())
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

    fn finish(&mut self, pending: bool) -> ViewTurn {
        let projection = self.projection.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Done((projection, pending)))
    }

    fn settle(&mut self, waiting: bool) -> ViewTurn {
        let projection = self.projection.as_ref().ok_or(IoError::Unfinished)?;
        let pending = waiting
            || projection.state.revision != Some(self.document.last_event_id)
            || publish::uncovered(&projection.records).len() > CHECKPOINT_AFTER;
        if !pending {
            let due = self.overdue();
            remember(self.node.node(), &self.document, due, projection);
            return self.finish(false);
        }
        match background_call(&self.node, &self.document) {
            Some(call) => {
                self.step = ViewStep::Background;
                Ok(Next::Call(call))
            }
            None => self.finish(true),
        }
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
            // Whether a slot was free changes nothing: the next read asks again.
            ViewStep::Background => self.finish(true),
        }
    }
}

/// [`view`] without waiting behind a running snapshot: while one holds the repository, the last
/// completed projection is served as pending. Only a document never projected here waits.
pub(super) fn read_view(node: &GitNode, document: &Document) -> ReadViewOperation {
    ReadViewOperation {
        node: node.clone(),
        document: document.clone(),
        asked: false,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct ReadViewOperation {
    node: GitNode,
    document: Document,
    asked: bool,
}

impl Operation for ReadViewOperation {
    type Output = Viewed;
    type Error = GitError;

    fn first(&mut self) -> ViewTurn {
        Ok(Next::Call(busy_call(self.document.document_id)))
    }

    fn next(&mut self, result: IoResult) -> ViewTurn {
        if self.asked {
            return Ok(Next::Done(result.child::<Viewed, GitError>()??));
        }
        self.asked = true;
        if git_flag(result)?
            && let Some(projection) = last(self.document.document_id)
        {
            return Ok(Next::Done((projection, true)));
        }
        Ok(Next::Call(IoCall::child(view(&self.node, &self.document))))
    }
}

/// Called after materialization; nodes that do not hold the document have nothing to do.
pub fn capture(
    node: &GitNode,
    record: &Document,
    revision: Option<&MetadataRawRevision>,
) -> CaptureOperation {
    CaptureOperation {
        node: node.clone(),
        record: record.clone(),
        revision: revision.cloned(),
        checked: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct CaptureOperation {
    node: GitNode,
    record: Document,
    revision: Option<MetadataRawRevision>,
    checked: bool,
}

impl Operation for CaptureOperation {
    type Output = ();
    type Error = GitError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::GitRepository(self.record.document_id))
    }

    fn first(&mut self) -> Result<Next<()>, GitError> {
        Ok(Next::Call(configured_call(self.record.document_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        if !self.checked {
            self.checked = true;
            if !has_store(result)? {
                return Ok(Next::Done(()));
            }
            forget(self.record.document_id);
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
                let projection = last(self.id).unwrap_or_else(|| Projection {
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
