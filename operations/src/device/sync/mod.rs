//! Owns the device's two-way synced folders and drives the repeated reconcile passes.
//! Local files form a read-only bucket the realm node pulls from, and local data wins.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod actions;
pub mod download;
pub mod folders;
pub mod materialize;
pub mod outbox;
pub(crate) mod pull;
pub mod reconcile;
pub mod remove;
pub mod repository;

use std::collections::{BTreeMap, VecDeque};
use std::sync::LazyLock;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{SYNC_UPLOAD_KEYSPACE, SYNCED_FOLDER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::{outside_answer, outside_call};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::{
    FolderMode, FolderState, Observed, RemoteBinding, RemoteHead, SyncListCursor, SyncPageLimit,
    SyncRefusal, SyncVersionPage, SyncedFolder,
};
use aruna_core::task::TaskKey;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Key;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::warn;
use ulid::Ulid;

use crate::device::sync_status::ArmTimerOperation;
use crate::driver::DriverContext;
use crate::replication::managed_read::LocalUserOperation;
use crate::staging::offered_directory::{
    OfferDirectoryInput, OfferDirectoryResult, OfferedDirectoryError, offer_directory,
};

use download::ApplyDownloadsOperation;
use folders::bind::RequestVersionsOperation;
use folders::{FolderError, read_bound, store_progress};
use reconcile::{ReconcileError, ReconcileFolderOperation, ReconcileInput, ReconcilePlan};
use repository::SYNC_PAGE_SIZE;

pub use outbox::{UPLOAD_CONTINUE_AFTER, UPLOAD_DEFER_AFTER, drain_sync_outbox};

/// Delay before the next pass when every folder is settled.
pub const RECONCILE_IDLE_AFTER: Duration = Duration::from_secs(30);

/// Delay before a deferred pass looks for the realm again.
pub const RECONCILE_RETRY_AFTER: Duration = Duration::from_secs(15);

/// Delay between passes while a folder still has work.
pub const RECONCILE_CONTINUE_AFTER: Duration = Duration::from_millis(250);

/// Remote heads one pass holds in memory. A folder beyond this converges over
/// several passes instead of building one unbounded listing.
const MAX_REMOTE_HEADS: usize = 16_384;

#[derive(Debug, Error, PartialEq)]
pub enum ReconcileFolderError {
    #[error(transparent)]
    Sweep(#[from] OfferedDirectoryError),
    #[error("the realm node refused the folder sync: {0:?}")]
    Refused(SyncRefusal),
    #[error("the realm node is unreachable: {0}")]
    Unreachable(String),
    #[error("could not decide the folder: {0}")]
    Decide(#[from] ReconcileError),
    #[error("the folder sync is unavailable")]
    Unavailable,
}

impl From<IoError> for ReconcileFolderError {
    fn from(_: IoError) -> Self {
        Self::Unavailable
    }
}

impl ReconcileFolderError {
    /// The owner-facing reason, naming the remote the folder is bound to.
    pub fn describe(&self, remote: &RemoteBinding) -> String {
        match self {
            Self::Refused(SyncRefusal::NotFound) => format!(
                "the bucket \"{}\" does not exist on node {}",
                remote.bucket, remote.node_id
            ),
            Self::Refused(SyncRefusal::Unauthorized | SyncRefusal::Forbidden) => {
                format!("access to bucket \"{}\" is forbidden", remote.bucket)
            }
            Self::Unreachable(reason) => {
                format!("node {} is unreachable: {reason}", remote.node_id)
            }
            other => other.to_string(),
        }
    }
}

static FOLDER_GATES: LazyLock<[Mutex<()>; 64]> =
    LazyLock::new(|| std::array::from_fn(|_| Mutex::new(())));

/// Held by whoever runs a folder's pass, binding or cleanup for its whole run, so an unbind
/// waits for a pass in flight.
pub fn folder_gate(folder_id: Ulid) -> &'static Mutex<()> {
    &FOLDER_GATES[usize::from(folder_id.to_bytes()[15]) % 64]
}

fn subset<T: Clone>(source: &BTreeMap<String, T>, keys: &[String]) -> BTreeMap<String, T> {
    keys.iter()
        .filter_map(|key| source.get_key_value(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Reconciles one folder: observes the directory, lists the realm heads, decides every entry,
/// then writes what the decisions allow. The caller holds the folder's gate.
pub fn folder_pass(context: &DriverContext, folder: SyncedFolder) -> FolderPassOperation {
    let ids = (context.net_handle.as_ref()).map(|net| (*net.realm_id(), net.node_id()));
    FolderPassOperation {
        ids,
        stored: folder.clone(),
        folder,
        step: PassStep::Current,
        local: BTreeMap::new(),
        view: RemoteView::default(),
        pages: VecDeque::new(),
        plan: ReconcilePlan::default(),
        failure: None,
    }
}

/// What the pending call of a folder pass answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PassStep {
    Current,
    Sweep,
    Heads,
    Decide,
    Download,
    Store,
    Arm,
    StoreFailure,
}

#[derive(Debug, PartialEq)]
pub struct FolderPassOperation {
    ids: Option<(RealmId, NodeId)>,
    folder: SyncedFolder,
    stored: SyncedFolder,
    step: PassStep,
    local: BTreeMap<String, Observed>,
    view: RemoteView,
    pages: VecDeque<Vec<String>>,
    plan: ReconcilePlan,
    failure: Option<ReconcileFolderError>,
}

type PassTurn = Result<Next<ReconcilePlan>, ReconcileFolderError>;

impl FolderPassOperation {
    fn realm_id(&self) -> Result<RealmId, IoError> {
        (self.ids.map(|(realm_id, _)| realm_id)).ok_or(IoError::Unfinished)
    }

    /// Stops the pass and stores why on the folder row.
    fn fail(&mut self, error: ReconcileFolderError) -> PassTurn {
        self.stored.last_error = Some(error.describe(&self.folder.remote));
        self.stored.last_error_ms = Some(unix_timestamp_millis());
        self.failure = Some(error);
        self.step = PassStep::StoreFailure;
        Ok(Next::Call(IoCall::child(store_progress(&self.stored))))
    }

    fn sweep(&mut self) -> PassTurn {
        let Some((realm_id, node_id)) = self.ids else {
            return self.fail(ReconcileFolderError::Unavailable);
        };
        let folder = &self.folder;
        let offer = offer_directory(OfferDirectoryInput {
            bucket: folder.local_bucket.clone(),
            root: folder.root.clone(),
            group_id: folder.group_id,
            realm_id,
            node_id,
            user_id: folder.created_by,
        });
        self.step = PassStep::Sweep;
        Ok(Next::Call(IoCall::child(offer)))
    }

    fn swept(&mut self, sweep: OfferDirectoryResult) -> PassTurn {
        self.stored.observed_files = sweep.files as u64;
        self.local = (sweep.observed.into_iter())
            .map(|file| {
                let observed = Observed {
                    fingerprint: file.fingerprint,
                    size: file.size,
                    blake3: None,
                    modified_at_ms: file.modified_at_ms,
                    version_id: Some(file.version_id),
                    stat: file.stat,
                };
                (file.relative, observed)
            })
            .collect();
        if self.folder.mode == FolderMode::UploadOnly {
            return self.decide_all();
        }
        let auth = AuthContext {
            user_id: self.folder.created_by,
            realm_id: self.realm_id()?,
            path_restrictions: None,
            session: None,
        };
        self.step = PassStep::Heads;
        let heads = RemoteHeadsOperation::new(&self.folder, auth);
        Ok(Next::Call(IoCall::child(heads)))
    }

    /// Splits the keys this pass may judge into pages. It never judges a key it did not list:
    /// that is how a folder larger than one listing converges without inventing deletions.
    fn decide_all(&mut self) -> PassTurn {
        let mut keys: Vec<String> = (self.local.keys())
            .chain(self.view.heads.keys())
            .filter(|key| self.view.covers(key))
            .cloned()
            .collect();
        keys.sort();
        keys.dedup();
        self.pages = keys
            .chunks(SYNC_PAGE_SIZE)
            .map(<[String]>::to_vec)
            .collect();
        self.plan.truncated = self.view.next_cursor.is_some();
        self.decide_next()
    }

    fn decide_next(&mut self) -> PassTurn {
        let Some(page) = self.pages.pop_front() else {
            return self.download();
        };
        let input = ReconcileInput {
            folder: self.folder.clone(),
            local: subset(&self.local, &page),
            remote: subset(&self.view.heads, &page),
            now_ms: unix_timestamp_millis(),
        };
        self.step = PassStep::Decide;
        let decide = ReconcileFolderOperation::new(input);
        Ok(Next::Call(IoCall::child(decide)))
    }

    fn download(&mut self) -> PassTurn {
        let downloads = std::mem::take(&mut self.plan.downloads);
        let apply = ApplyDownloadsOperation::new(self.realm_id()?, self.folder.clone(), downloads);
        self.step = PassStep::Download;
        Ok(Next::Call(IoCall::child(apply)))
    }

    fn store(&mut self) -> PassTurn {
        self.stored.last_reconcile_ms = Some(unix_timestamp_millis());
        self.stored.last_error = None;
        self.stored.last_error_ms = None;
        self.stored.list_cursor = self.view.next_cursor.take();
        self.step = PassStep::Store;
        Ok(Next::Call(IoCall::child(store_progress(&self.stored))))
    }

    fn finish(&mut self) -> PassTurn {
        Ok(Next::Done(std::mem::take(&mut self.plan)))
    }
}

impl Operation for FolderPassOperation {
    type Output = ReconcilePlan;
    type Error = ReconcileFolderError;

    fn first(&mut self) -> PassTurn {
        Ok(Next::Call(IoCall::child(read_bound(self.folder.folder_id))))
    }

    fn next(&mut self, result: IoResult) -> PassTurn {
        match self.step {
            // A folder unbound or paused since the pass listed it is left alone.
            PassStep::Current => match result.child::<SyncedFolder, FolderError>()? {
                Ok(current) if current.state == FolderState::Active => self.sweep(),
                _ => self.finish(),
            },
            PassStep::Sweep => {
                match result.child::<OfferDirectoryResult, OfferedDirectoryError>()? {
                    Ok(sweep) => self.swept(sweep),
                    Err(error) => self.fail(error.into()),
                }
            }
            PassStep::Heads => match result.child::<RemoteView, ReconcileFolderError>()? {
                Ok(view) => {
                    self.view = view;
                    self.decide_all()
                }
                Err(error) => self.fail(error),
            },
            PassStep::Decide => match result.child::<ReconcilePlan, ReconcileError>()? {
                Ok(page) => {
                    self.plan.absorb(page);
                    self.decide_next()
                }
                Err(error) => self.fail(error.into()),
            },
            PassStep::Download => {
                result.child::<usize, IoError>()?.ok();
                self.store()
            }
            PassStep::Store => {
                let stored = result.child::<SyncedFolder, FolderError>()?;
                stored.map_err(|_| ReconcileFolderError::Unavailable)?;
                if self.plan.uploads == 0 {
                    return self.finish();
                }
                self.step = PassStep::Arm;
                let arm = ArmTimerOperation::new(TaskKey::DrainUploadOutbox);
                Ok(Next::Call(IoCall::child(arm)))
            }
            PassStep::Arm => {
                result.child::<(), IoError>()?.ok();
                self.finish()
            }
            PassStep::StoreFailure => {
                if let Err(error) = result.child::<SyncedFolder, FolderError>()? {
                    let folder = self.folder.folder_id;
                    warn!(%folder, reason = %error, "Could not store the folder error");
                }
                Err(self
                    .failure
                    .take()
                    .unwrap_or(ReconcileFolderError::Unavailable))
            }
        }
    }
}

/// What one pass listed of the realm side, and the window it may decide about.
#[derive(Debug, Default, PartialEq)]
struct RemoteView {
    heads: BTreeMap<String, RemoteHead>,
    /// Exclusive lower bound this pass resumed from.
    resume_after: Option<String>,
    /// Inclusive upper bound the listing reached, when it was cut short.
    boundary: Option<String>,
    /// Where the next pass resumes. `None` restarts from the beginning.
    next_cursor: Option<SyncListCursor>,
}

impl RemoteView {
    /// Whether this pass listed far enough to decide about `key`.
    fn covers(&self, key: &str) -> bool {
        self.resume_after.as_deref().is_none_or(|after| key > after)
            && self.boundary.as_deref().is_none_or(|bound| key <= bound)
    }
}

/// Closes one listing window. An exhausted listing covers every remaining key,
/// so its boundary clears and the next pass starts over instead of repeating
/// exactly this window.
fn close_window(view: &mut RemoteView, next: Option<SyncListCursor>) {
    if next.is_none() {
        view.boundary = None;
    }
    view.next_cursor = next;
}

/// Every current head under the folder's bound prefix, as bounded pages served by the folder's
/// realm node with the owner's authority.
#[derive(Debug, PartialEq)]
struct RemoteHeadsOperation {
    remote: RemoteBinding,
    auth: AuthContext,
    cursor: Option<SyncListCursor>,
    view: RemoteView,
}

type HeadsTurn = Result<Next<RemoteView>, ReconcileFolderError>;

impl RemoteHeadsOperation {
    fn new(folder: &SyncedFolder, auth: AuthContext) -> Self {
        let cursor = folder.list_cursor.clone();
        let resume_after =
            (cursor.as_ref()).and_then(|cursor| folder.remote.relative_path(&cursor.key));
        Self {
            remote: folder.remote.clone(),
            auth,
            cursor,
            view: RemoteView {
                resume_after,
                ..RemoteView::default()
            },
        }
    }

    fn page(&mut self) -> HeadsTurn {
        let page = RequestVersionsOperation::new(
            (&self.remote, self.auth.clone()),
            self.cursor.take(),
            SyncPageLimit::default(),
        );
        Ok(Next::Call(IoCall::child(page)))
    }

    fn close(&mut self, next: Option<SyncListCursor>) -> HeadsTurn {
        close_window(&mut self.view, next);
        Ok(Next::Done(std::mem::take(&mut self.view)))
    }
}

impl Operation for RemoteHeadsOperation {
    type Output = RemoteView;
    type Error = ReconcileFolderError;

    fn first(&mut self) -> HeadsTurn {
        self.page()
    }

    fn next(&mut self, result: IoResult) -> HeadsTurn {
        let page = result.child::<SyncVersionPage, ReconcileFolderError>()??;
        let (heads, next) = page.into_parts();
        for head in heads {
            self.view.boundary = Some(head.relative.clone());
            self.view.heads.insert(head.relative.clone(), head);
        }
        // A folder past the in-memory bound stops here and resumes next pass; the boundary
        // keeps this pass from judging the keys it never saw.
        if self.view.heads.len() >= MAX_REMOTE_HEADS || next.is_none() {
            return self.close(next);
        }
        self.cursor = next;
        self.page()
    }
}

/// Which device timers a starting node arms.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SyncTimers {
    pub folders: bool,
    pub uploads: bool,
}

/// Finds the device timers to arm. The folder pass is armed for a bound folder and on every
/// device, because it is also the beat a device fetches the realm-wide documents on.
pub fn sync_timers(context: &DriverContext) -> SyncTimersOperation {
    SyncTimersOperation {
        realm_id: context.net_handle.as_ref().map(|net| *net.realm_id()),
        step: TimerStep::Folders,
        timers: SyncTimers::default(),
    }
}

/// What the pending call of finding the timers answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimerStep {
    Folders,
    Device,
    Uploads,
}

#[derive(Debug, PartialEq)]
pub struct SyncTimersOperation {
    realm_id: Option<RealmId>,
    step: TimerStep,
    timers: SyncTimers,
}

type TimerTurn = Result<Next<SyncTimers>, IoError>;

/// Asks for one row of a keyspace; a failed read counts as an empty one.
fn any_row(space: &str) -> IoCall {
    outside_call(IoCall::Page {
        space: space.to_string(),
        prefix: Key::from(Vec::new()),
        after: None,
        limit: 1,
    })
}

fn has_row(result: IoResult) -> bool {
    (outside_answer(result).and_then(IoResult::page)).is_ok_and(|(rows, _)| !rows.is_empty())
}

impl SyncTimersOperation {
    fn uploads(&mut self) -> TimerTurn {
        self.step = TimerStep::Uploads;
        Ok(Next::Call(any_row(SYNC_UPLOAD_KEYSPACE)))
    }
}

impl Operation for SyncTimersOperation {
    type Output = SyncTimers;
    type Error = IoError;

    fn first(&mut self) -> TimerTurn {
        Ok(Next::Call(any_row(SYNCED_FOLDER_KEYSPACE)))
    }

    fn next(&mut self, result: IoResult) -> TimerTurn {
        match self.step {
            TimerStep::Folders => {
                self.timers.folders = has_row(result);
                match self.realm_id.filter(|_| !self.timers.folders) {
                    Some(realm_id) => {
                        self.step = TimerStep::Device;
                        let local = LocalUserOperation::new(realm_id);
                        Ok(Next::Call(IoCall::child(local)))
                    }
                    None => self.uploads(),
                }
            }
            TimerStep::Device => {
                self.timers.folders = result.child::<bool, IoError>()?.unwrap_or(false);
                self.uploads()
            }
            TimerStep::Uploads => {
                self.timers.uploads = has_row(result);
                Ok(Next::Done(self.timers))
            }
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    fn view(boundary: &str) -> RemoteView {
        RemoteView {
            boundary: Some(boundary.to_string()),
            ..RemoteView::default()
        }
    }

    // The in-memory bound and the listing end can coincide: the window must cover
    // the keys after the last head, and the next pass restarts instead of repeating it.
    #[test]
    fn clears_final_boundary() {
        let mut exhausted = view("m.txt");
        close_window(&mut exhausted, None);
        assert!(exhausted.next_cursor.is_none());
        assert!(exhausted.covers("z.txt"));

        let mut truncated = view("m.txt");
        close_window(
            &mut truncated,
            Some(SyncListCursor {
                key: "m.txt".to_string(),
                version_id: None,
            }),
        );
        assert!(truncated.next_cursor.is_some());
        assert!(!truncated.covers("z.txt"));
        assert!(truncated.covers("a.txt"));
    }

    // A resumed pass never re-decides the keys the previous one already passed.
    #[test]
    fn window_skips_resumed() {
        let resumed = RemoteView {
            resume_after: Some("m.txt".to_string()),
            ..RemoteView::default()
        };
        assert!(!resumed.covers("a.txt"));
        assert!(resumed.covers("z.txt"));
    }
}
