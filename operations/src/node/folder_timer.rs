//! Runs the synced-folder reconcile timer: reconciles bound folders, then refreshes
//! the device's realm documents and metadata replicas with backoff memory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::synced_folder::{FolderState, SyncedFolder};
use tracing::warn;
use ulid::Ulid;

use crate::triggers::{TimerInput, TimerMemory, TimerPlan};
use aruna_ops_data::device::publish_queue::DrainOutcome;
use aruna_ops_data::device::sync::folders::FolderError;
use aruna_ops_data::device::sync::folders::bind::finish_binding;
use aruna_ops_data::device::sync::folders::reads::list_folders;
use aruna_ops_data::device::sync::folders::unbind::finish_unbind;
use aruna_ops_data::device::sync::reconcile::ReconcilePlan;
use aruna_ops_data::device::sync::{
    RECONCILE_CONTINUE_AFTER, RECONCILE_IDLE_AFTER, RECONCILE_RETRY_AFTER, ReconcileFolderError,
    folder_pass,
};
use aruna_ops_data::metadata::device::refresh::refresh_replicas;
use aruna_ops_identity::device_node::realm_documents::fetch_realm_documents;
use aruna_ops_identity::device_node::sync_note::{NoteError, NoteSyncOperation, SyncNote};
use aruna_ops_identity::queue_backoff::retry_delay_ms;

/// How long a device keeps its copy of the realm documents before asking for them again.
const REALM_DOCUMENTS_AFTER: Duration = Duration::from_secs(60);

/// How long one attempt may spend fetching the realm documents as a whole.
const REALM_DOCUMENTS_BUDGET: Duration = Duration::from_secs(20);

#[derive(Clone, Debug, PartialEq)]
enum FolderStep {
    List,
    Unbind(Ulid),
    Bind(Ulid),
    Reconcile(Ulid),
}

/// Reconciles every bound folder once, finishing interrupted binds and unbinds.
#[derive(Clone, Debug, PartialEq)]
pub struct ReconcileFoldersOperation {
    serving: Option<(NodeId, RealmId)>,
    folders: VecDeque<SyncedFolder>,
    work: bool,
    step: FolderStep,
}

impl ReconcileFoldersOperation {
    pub fn new(serving: Option<(NodeId, RealmId)>) -> Self {
        Self {
            serving,
            folders: VecDeque::new(),
            work: false,
            step: FolderStep::List,
        }
    }

    fn advance(&mut self) -> Result<Next<DrainOutcome>, IoError> {
        let Some((node_id, _)) = self.serving else {
            return Ok(Next::Done(DrainOutcome::Deferred));
        };
        while let Some(folder) = self.folders.pop_front() {
            let folder_id = folder.folder_id;
            match folder.state {
                // An unbind finishes here, also after a cancelled call or a restart.
                FolderState::Deleting => {
                    self.step = FolderStep::Unbind(folder_id);
                    let unbind = finish_unbind(self.serving, folder_id, node_id);
                    return Ok(Next::Call(IoCall::child(unbind)));
                }
                // A bind cut short by a lost reply, a failed write or a restart finishes here.
                FolderState::Binding { .. } => {
                    self.step = FolderStep::Bind(folder_id);
                    return Ok(Next::Call(IoCall::child(finish_binding(folder_id))));
                }
                FolderState::Active => {
                    self.step = FolderStep::Reconcile(folder_id);
                    let pass = folder_pass(self.serving, folder);
                    return Ok(Next::Call(IoCall::child(pass)));
                }
                _ => {}
            }
        }
        Ok(Next::Done(match self.work {
            true => DrainOutcome::Recheck,
            false => DrainOutcome::Idle,
        }))
    }
}

impl Operation for ReconcileFoldersOperation {
    type Output = DrainOutcome;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<DrainOutcome>, IoError> {
        if self.serving.is_none() {
            return Ok(Next::Done(DrainOutcome::Deferred));
        }
        Ok(Next::Call(IoCall::child(list_folders())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DrainOutcome>, IoError> {
        match self.step {
            FolderStep::List => match result.child::<Vec<SyncedFolder>, FolderError>()? {
                Ok(folders) => {
                    self.folders = folders.into();
                    self.advance()
                }
                Err(_) => Ok(Next::Done(DrainOutcome::Deferred)),
            },
            FolderStep::Unbind(folder_id) => {
                if let Err(error) = result.child::<(), FolderError>()? {
                    warn!(folder = %folder_id, reason = %error, "Could not finish the unbind");
                }
                self.advance()
            }
            FolderStep::Bind(folder_id) => match result.child::<SyncedFolder, FolderError>()? {
                Ok(_) => {
                    self.work = true;
                    self.advance()
                }
                Err(error) => {
                    warn!(folder = %folder_id, reason = %error, "Could not finish the folder binding");
                    self.advance()
                }
            },
            FolderStep::Reconcile(folder_id) => {
                match result.child::<ReconcilePlan, ReconcileFolderError>()? {
                    Ok(plan) => self.work |= plan.uploads > 0 || plan.truncated,
                    Err(error) => {
                        warn!(folder = %folder_id, reason = %error, "Could not reconcile the folder");
                    }
                }
                self.advance()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimerStep {
    Folders,
    Documents,
    NoteDoc,
    Replicas,
    NoteReplica,
}

/// Runs the synced-folder timer: folders first, then realm documents and replica refreshes
/// so an unreachable realm never holds up the owner's own files.
#[derive(Clone, Debug, PartialEq)]
pub struct FolderTimerOperation {
    serving: Option<(NodeId, RealmId)>,
    started_ms: u64,
    now_ms: u64,
    memory: TimerMemory,
    /// When the next folder pass is due, fixed once the folders finish, so the refreshes after
    /// them never push it later.
    deadline_ms: u64,
    step: TimerStep,
}

impl FolderTimerOperation {
    pub fn new(input: &TimerInput) -> Self {
        Self {
            serving: input.serving,
            started_ms: input.now_ms,
            now_ms: input.now_ms,
            memory: input.memory.clone(),
            deadline_ms: input
                .now_ms
                .saturating_add(RECONCILE_IDLE_AFTER.as_millis() as u64),
            step: TimerStep::Folders,
        }
    }

    fn fetch_documents(&mut self) -> Result<Next<TimerPlan>, IoError> {
        if self.memory.due_ms > self.now_ms {
            return self.start_replicas();
        }
        self.started_ms = self.now_ms;
        self.step = TimerStep::Documents;
        let fetch = fetch_realm_documents(self.serving, REALM_DOCUMENTS_BUDGET);
        Ok(Next::Call(IoCall::child(fetch)))
    }

    fn start_replicas(&mut self) -> Result<Next<TimerPlan>, IoError> {
        self.step = TimerStep::Replicas;
        let pass = refresh_replicas(self.serving);
        Ok(Next::Call(IoCall::child(pass)))
    }

    fn finish(&self) -> Result<Next<TimerPlan>, IoError> {
        let left = self.deadline_ms.saturating_sub(self.now_ms);
        Ok(Next::Done(TimerPlan {
            after: Some(Duration::from_millis(left)),
            memory: self.memory.clone(),
        }))
    }
}

impl Operation for FolderTimerOperation {
    type Output = TimerPlan;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<TimerPlan>, IoError> {
        let pass = ReconcileFoldersOperation::new(self.serving);
        Ok(Next::Call(IoCall::child(pass)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TimerPlan>, IoError> {
        match self.step {
            TimerStep::Folders => {
                let outcome = result
                    .child::<DrainOutcome, IoError>()?
                    .unwrap_or(DrainOutcome::Deferred);
                let after = match outcome {
                    DrainOutcome::Deferred => RECONCILE_RETRY_AFTER,
                    DrainOutcome::Recheck => RECONCILE_CONTINUE_AFTER,
                    DrainOutcome::Idle => RECONCILE_IDLE_AFTER,
                };
                self.deadline_ms = self.now_ms.saturating_add(after.as_millis() as u64);
                self.fetch_documents()
            }
            TimerStep::Documents => {
                let fetched = result.child::<bool, IoError>()?.unwrap_or(false);
                let attempts = match fetched {
                    true => 0,
                    false => self.memory.attempts.saturating_add(1),
                };
                let delay = match fetched {
                    true => REALM_DOCUMENTS_AFTER.as_millis() as u64,
                    false => retry_delay_ms(attempts),
                };
                self.memory.attempts = attempts;
                self.memory.due_ms = self.started_ms.saturating_add(delay);
                let note = match fetched {
                    true => SyncNote::Contact,
                    false => SyncNote::Failure,
                };
                self.step = TimerStep::NoteDoc;
                let call = NoteSyncOperation::new(note, self.now_ms);
                Ok(Next::Call(IoCall::child(call)))
            }
            TimerStep::NoteDoc => {
                if let Err(error) = result.child::<(), NoteError>()? {
                    warn!(%error, "Failed to note the device sync state");
                }
                self.start_replicas()
            }
            TimerStep::Replicas => {
                if result.child::<usize, IoError>()?.unwrap_or(0) > 0 {
                    self.step = TimerStep::NoteReplica;
                    let note = NoteSyncOperation::new(SyncNote::Finished, self.now_ms);
                    return Ok(Next::Call(IoCall::child(note)));
                }
                self.finish()
            }
            TimerStep::NoteReplica => {
                if let Err(error) = result.child::<(), NoteError>()? {
                    warn!(%error, "Failed to note the device sync state");
                }
                self.finish()
            }
        }
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn serving() -> Option<(NodeId, RealmId)> {
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        Some((node, RealmId::from_bytes([2; 32])))
    }

    #[test]
    fn reconcile_folders_outcome() {
        let mut offline = ReconcileFoldersOperation::new(None);
        assert_eq!(offline.first(), Ok(Next::Done(DrainOutcome::Deferred)));

        let mut empty = ReconcileFoldersOperation::new(serving());
        assert!(matches!(empty.first(), Ok(Next::Call(IoCall::Child(_)))));
        let listed = ChildOutcome::new(Ok::<Vec<SyncedFolder>, FolderError>(Vec::new()));
        assert_eq!(
            empty.next(IoResult::Child(listed)),
            Ok(Next::Done(DrainOutcome::Idle))
        );
    }

    #[test]
    fn folder_timer_backoff() {
        let mut input = TimerInput::new(10_000, serving().map(|(node, _)| node));
        input.serving = serving();
        let mut operation = FolderTimerOperation::new(&input);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));

        // Folders idle, realm documents due: fetches documents and backs off on failure.
        let idle = ChildOutcome::new(Ok::<DrainOutcome, IoError>(DrainOutcome::Idle));
        assert!(matches!(
            operation.next(IoResult::Child(idle)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation.stamp(11_000);
        let missed = ChildOutcome::new(Ok::<bool, IoError>(false));
        assert!(matches!(
            operation.next(IoResult::Child(missed)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let noted = ChildOutcome::new(Ok::<(), NoteError>(()));
        assert!(matches!(
            operation.next(IoResult::Child(noted)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let refreshed = ChildOutcome::new(Ok::<usize, IoError>(0));
        let Ok(Next::Done(plan)) = operation.next(IoResult::Child(refreshed)) else {
            panic!("timer ends with plan");
        };
        // The second the refresh took comes off the folder delay.
        let left = RECONCILE_IDLE_AFTER - Duration::from_millis(1_000);
        assert_eq!(plan.after, Some(left));
        assert_eq!(plan.memory.attempts, 1);
        assert_eq!(plan.memory.due_ms, 10_000 + retry_delay_ms(1));

        // Before due_ms, a re-run skips realm documents and goes straight to replicas.
        input.now_ms = plan.memory.due_ms - 1;
        input.memory = plan.memory;
        let mut skipped = FolderTimerOperation::new(&input);
        skipped.first().unwrap();
        let recheck = ChildOutcome::new(Ok::<DrainOutcome, IoError>(DrainOutcome::Recheck));
        assert!(matches!(
            skipped.next(IoResult::Child(recheck)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(skipped.step, TimerStep::Replicas);
        let refreshed = ChildOutcome::new(Ok::<usize, IoError>(1));
        assert!(matches!(
            skipped.next(IoResult::Child(refreshed)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(skipped.step, TimerStep::NoteReplica);
        let noted = ChildOutcome::new(Ok::<(), NoteError>(()));
        let Ok(Next::Done(plan)) = skipped.next(IoResult::Child(noted)) else {
            panic!("timer ends after replica note");
        };
        assert_eq!(plan.after, Some(RECONCILE_CONTINUE_AFTER));
    }

    #[test]
    fn slow_refresh_due() {
        // A refresh that outlasts the folder delay leaves the next pass due at once.
        let mut input = TimerInput::new(10_000, serving().map(|(node, _)| node));
        input.serving = serving();
        input.memory.due_ms = u64::MAX;
        let mut operation = FolderTimerOperation::new(&input);
        operation.first().unwrap();
        let recheck = ChildOutcome::new(Ok::<DrainOutcome, IoError>(DrainOutcome::Recheck));
        operation.next(IoResult::Child(recheck)).unwrap();
        operation.stamp(10_000 + RECONCILE_CONTINUE_AFTER.as_millis() as u64 + 1);
        let refreshed = ChildOutcome::new(Ok::<usize, IoError>(0));
        let Ok(Next::Done(plan)) = operation.next(IoResult::Child(refreshed)) else {
            panic!("timer ends with plan");
        };
        assert_eq!(plan.after, Some(Duration::ZERO));
    }
}
