//! Binds a directory to a realm bucket prefix and creates realm buckets through the node plane.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::SYNCED_FOLDER_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, outside_answer, outside_call, read_row, value};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::{
    FolderState, RemoteBinding, SyncPageLimit, SyncRefusal, SyncVersionPage, SyncedFolder,
};
use aruna_core::task::TaskKey;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{GroupId, Key};
use tracing::warn;
use ulid::Ulid;

use crate::device::sync_status::ArmTimerOperation;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::transport::{peer_answer, peer_call};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::transport_message_kind;
use crate::staging::offered_directory::{OfferedDirectoryError, check_root};

use super::super::ReconcileFolderError;
use super::super::repository::{MAX_SYNCED_FOLDERS, folder_bucket, folder_entry, folder_key};
use super::reads::read_bound;
use super::{BindFolderInput, FolderChange, FolderError, RealmBucketInput, UpdateFolderOperation};

type RemoteError = ReconcileFolderError;

fn owner_auth(user_id: aruna_core::UserId, realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

fn remote_call(node: NodeId, message: &MetadataTransportMessage) -> Result<IoCall, RemoteError> {
    peer_call(node, message).map_err(RemoteError::Unreachable)
}

fn remote_answer(result: IoResult) -> Result<MetadataTransportMessage, RemoteError> {
    peer_answer(result)?.map_err(RemoteError::Unreachable)
}

fn unexpected_reply(other: &MetadataTransportMessage) -> RemoteError {
    let kind = transport_message_kind(other);
    RemoteError::Unreachable(format!("unexpected metadata response: {kind}"))
}

fn arm_folders() -> IoCall {
    IoCall::child(ArmTimerOperation::new(TaskKey::ReconcileSyncedFolders))
}

fn map_remote_error(error: RemoteError, remote: &RemoteBinding) -> FolderError {
    match error {
        RemoteError::Refused(SyncRefusal::NotFound) => FolderError::RemoteBucketMissing {
            node: remote.node_id,
            bucket: remote.bucket.clone(),
        },
        RemoteError::Refused(SyncRefusal::Unauthorized | SyncRefusal::Forbidden) => {
            FolderError::RemoteForbidden {
                bucket: remote.bucket.clone(),
            }
        }
        RemoteError::Refused(SyncRefusal::Invalid(reason)) => FolderError::RemoteBucketConflict {
            bucket: remote.bucket.clone(),
            reason,
        },
        RemoteError::Unreachable(reason) => FolderError::RemoteUnreachable {
            node: remote.node_id,
            reason,
        },
        RemoteError::Refused(refusal) => FolderError::RemoteUnreachable {
            node: remote.node_id,
            reason: format!("{refusal:?}"),
        },
        RemoteError::Unavailable | RemoteError::Sweep(_) | RemoteError::Decide(_) => {
            FolderError::Unavailable
        }
    }
}

/// Whether two roots are the same directory or one contains the other. The comparison is
/// textual: a folder is bound by the path the owner gave.
pub(super) fn overlaps(left: &str, right: &str) -> bool {
    let left = left.trim_end_matches('/');
    let right = right.trim_end_matches('/');
    left == right
        || right.starts_with(&format!("{left}/"))
        || left.starts_with(&format!("{right}/"))
}

/// The count, overlap and bucket checks a new folder must pass against the bound ones.
fn admit(bound: &[SyncedFolder], folder: &SyncedFolder) -> Result<(), FolderError> {
    if bound.len() >= MAX_SYNCED_FOLDERS {
        return Err(FolderError::TooManyFolders(MAX_SYNCED_FOLDERS));
    }
    if let Some(existing) = (bound.iter()).find(|bound| overlaps(&bound.root, &folder.root)) {
        return Err(FolderError::RootOverlaps(existing.root.clone()));
    }
    if (bound.iter()).any(|bound| bound.local_bucket == folder.local_bucket) {
        return Err(FolderError::BucketBound(folder.local_bucket.clone()));
    }
    Ok(())
}

/// Reserves a folder for a directory and answers before the directory is walked. The caller
/// then finishes the binding, and the first reconcile pass observes the directory.
pub fn bind_folder(input: BindFolderInput) -> BindFolderOperation {
    let folder = SyncedFolder {
        folder_id: input.folder_id,
        root: input.root,
        local_bucket: folder_bucket(input.folder_id),
        group_id: input.group_id,
        remote: input.remote,
        mode: input.mode,
        propagate_deletes: input.propagate_deletes,
        state: FolderState::Binding {
            create_bucket: input.create_bucket,
        },
        created_by: input.user_id,
        created_at_ms: unix_timestamp_millis(),
        last_reconcile_ms: None,
        last_error: None,
        last_error_ms: None,
        observed_files: 0,
        list_cursor: None,
    };
    BindFolderOperation {
        realm_id: input.realm_id,
        folder,
        step: BindStep::Root,
    }
}

/// What the pending call of a bind answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BindStep {
    Root,
    Node,
    Reserve,
}

#[derive(Debug, PartialEq)]
pub struct BindFolderOperation {
    realm_id: RealmId,
    folder: SyncedFolder,
    step: BindStep,
}

impl Operation for BindFolderOperation {
    type Output = SyncedFolder;
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<SyncedFolder>, FolderError> {
        Ok(Next::Call(IoCall::child(check_root(&self.folder.root))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncedFolder>, FolderError> {
        match self.step {
            BindStep::Root => {
                result.child::<(), OfferedDirectoryError>()??;
                self.step = BindStep::Node;
                let node = self.folder.remote.node_id;
                let check = EnsureRealmOperation::new(self.realm_id, node);
                Ok(Next::Call(IoCall::child(check)))
            }
            BindStep::Node => {
                result.child::<(), FolderError>()??;
                self.step = BindStep::Reserve;
                let reserve = ReserveBindingOperation::new(self.folder.clone());
                Ok(Next::Call(IoCall::child(reserve)))
            }
            BindStep::Reserve => {
                result.child::<(), FolderError>()??;
                Ok(Next::Done(self.folder.clone()))
            }
        }
    }
}

/// Admits a folder and records it in one transaction. Its scan conflicts with any concurrent
/// reservation, so a retried request cannot bind the same root twice.
#[derive(Debug, PartialEq)]
pub(super) struct ReserveBindingOperation {
    folder: SyncedFolder,
    bound: Vec<SyncedFolder>,
    writing: bool,
}

impl ReserveBindingOperation {
    pub(super) fn new(folder: SyncedFolder) -> Self {
        Self {
            folder,
            bound: Vec::new(),
            writing: false,
        }
    }

    fn page(after: Option<Key>) -> Next<()> {
        Next::Call(IoCall::Page {
            space: SYNCED_FOLDER_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: MAX_SYNCED_FOLDERS,
        })
    }
}

impl Operation for ReserveBindingOperation {
    type Output = ();
    type Error = FolderError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, FolderError> {
        Ok(Self::page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, FolderError> {
        if self.writing {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let (rows, next) = result.page()?;
        let rows = rows.into_iter();
        self.bound
            .extend(rows.filter_map(|(_, row)| SyncedFolder::from_bytes(&row).ok()));
        if next.is_some() {
            return Ok(Self::page(next));
        }
        admit(&self.bound, &self.folder)?;
        let row = folder_entry(&self.folder).map_err(|_| FolderError::Unavailable)?;
        self.writing = true;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

/// Refuses a node that cannot hold realm buckets, such as this computer.
#[derive(Debug, PartialEq)]
struct EnsureRealmOperation {
    realm_id: RealmId,
    node_id: NodeId,
}

impl EnsureRealmOperation {
    fn new(realm_id: RealmId, node_id: NodeId) -> Self {
        Self { realm_id, node_id }
    }
}

impl Operation for EnsureRealmOperation {
    type Output = ();
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<()>, FolderError> {
        let config = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, FolderError> {
        let config = result.child::<RealmConfigDocument, GetConfigError>()?;
        let config = config.map_err(|_| FolderError::Unavailable)?;
        let eligible = (config.sync_eligible_nodes()).map_err(|_| FolderError::Unavailable)?;
        match eligible.contains(&self.node_id) {
            true => Ok(Next::Done(())),
            false => Err(FolderError::NotRealmNode(self.node_id)),
        }
    }
}

/// One page of the versions under a prefix, served by a realm node with the owner's authority.
#[derive(Debug, PartialEq)]
pub(in crate::device::sync) struct RequestVersionsOperation {
    node_id: NodeId,
    auth: AuthContext,
    bucket: String,
    prefix: String,
    cursor: Option<aruna_core::structs::SyncListCursor>,
    limit: SyncPageLimit,
}

impl RequestVersionsOperation {
    pub(in crate::device::sync) fn new(
        (remote, auth): (&RemoteBinding, AuthContext),
        cursor: Option<aruna_core::structs::SyncListCursor>,
        limit: SyncPageLimit,
    ) -> Self {
        Self {
            node_id: remote.node_id,
            auth,
            bucket: remote.bucket.clone(),
            prefix: remote.prefix.clone(),
            cursor,
            limit,
        }
    }
}

impl Operation for RequestVersionsOperation {
    type Output = SyncVersionPage;
    type Error = RemoteError;

    fn first(&mut self) -> Result<Next<SyncVersionPage>, RemoteError> {
        let message = MetadataTransportMessage::ForwardListVersions {
            auth_token: AuthToken::internal(self.auth.clone()),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            cursor: self.cursor.clone(),
            limit: self.limit,
        };
        Ok(Next::Call(remote_call(self.node_id, &message)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncVersionPage>, RemoteError> {
        match remote_answer(result)? {
            MetadataTransportMessage::ForwardedVersions { result: Ok(page) } => {
                Ok(Next::Done(page))
            }
            MetadataTransportMessage::ForwardedVersions {
                result: Err(refusal),
            } => Err(RemoteError::Refused(refusal)),
            other => Err(unexpected_reply(&other)),
        }
    }
}

/// Creates the remote bucket of a binding. The realm node accepts a bucket of the same group
/// that already exists, so creating again is safe.
#[derive(Debug, PartialEq)]
struct ForwardCreateOperation {
    auth: AuthContext,
    remote: RemoteBinding,
    group_id: GroupId,
}

impl Operation for ForwardCreateOperation {
    type Output = ();
    type Error = RemoteError;

    fn first(&mut self) -> Result<Next<()>, RemoteError> {
        let message = MetadataTransportMessage::ForwardCreateBucket {
            auth_token: AuthToken::internal(self.auth.clone()),
            bucket: self.remote.bucket.clone(),
            group_id: self.group_id,
        };
        Ok(Next::Call(remote_call(self.remote.node_id, &message)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, RemoteError> {
        match remote_answer(result)? {
            MetadataTransportMessage::ForwardedBucketCreated { result } => {
                result.map_err(RemoteError::Refused)?;
                Ok(Next::Done(()))
            }
            other => Err(unexpected_reply(&other)),
        }
    }
}

/// Creates a bucket on a realm node for the owner without binding a folder to it. A bucket of
/// that name the node already holds, in any group, is refused, never adopted.
pub fn create_realm_bucket(input: RealmBucketInput) -> CreateRealmOperation {
    CreateRealmOperation {
        input,
        asking: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct CreateRealmOperation {
    input: RealmBucketInput,
    asking: bool,
}

impl CreateRealmOperation {
    fn remote(&self) -> RemoteBinding {
        RemoteBinding {
            node_id: self.input.node_id,
            bucket: self.input.bucket.clone(),
            prefix: String::new(),
        }
    }

    /// The node decides existence inside its create transaction, so concurrent creates cannot
    /// both succeed.
    fn created(&self, result: IoResult) -> Result<bool, RemoteError> {
        match remote_answer(result)? {
            MetadataTransportMessage::ForwardedNewBucket { result } => {
                result.map_err(RemoteError::Refused)
            }
            other => Err(unexpected_reply(&other)),
        }
    }
}

impl Operation for CreateRealmOperation {
    type Output = ();
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<()>, FolderError> {
        let check = EnsureRealmOperation::new(self.input.realm_id, self.input.node_id);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, FolderError> {
        let remote = self.remote();
        if !self.asking {
            result.child::<(), FolderError>()??;
            self.asking = true;
            let auth = owner_auth(self.input.user_id, self.input.realm_id);
            let message = MetadataTransportMessage::ForwardNewBucket {
                auth_token: AuthToken::internal(auth),
                bucket: remote.bucket.clone(),
                group_id: self.input.group_id,
            };
            let call = remote_call(remote.node_id, &message);
            return Ok(Next::Call(
                call.map_err(|error| map_remote_error(error, &remote))?,
            ));
        }
        let created = self.created(result);
        match created.map_err(|error| map_remote_error(error, &remote))? {
            true => Ok(Next::Done(())),
            false => Err(FolderError::BucketExists {
                node: remote.node_id,
                bucket: remote.bucket,
            }),
        }
    }
}

/// Wakes the background pass for a folder whose root is still a usable directory.
pub fn request_sync(folder: &SyncedFolder) -> RequestSyncOperation {
    RequestSyncOperation {
        root: folder.root.clone(),
        arming: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct RequestSyncOperation {
    root: String,
    arming: bool,
}

impl Operation for RequestSyncOperation {
    type Output = ();
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<()>, FolderError> {
        Ok(Next::Call(IoCall::child(check_root(&self.root))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, FolderError> {
        if self.arming {
            result.child::<(), IoError>()?.ok();
            return Ok(Next::Done(()));
        }
        result.child::<(), OfferedDirectoryError>()??;
        self.arming = true;
        Ok(Next::Call(arm_folders()))
    }
}

/// Confirms the realm bucket of a binding in progress and records the outcome: an accepted
/// remote activates the folder, a refusal drops the binding, and a failure without an answer
/// keeps it for the next folder pass.
pub fn finish_binding(folder_id: Ulid) -> FinishBindingOperation {
    FinishBindingOperation {
        folder_id,
        step: FinishStep::Read,
        folder: None,
        failure: None,
    }
}

/// What the pending call of finishing a binding answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FinishStep {
    Read,
    Sync,
    Remote,
    Activate,
    Arm,
    Drop,
    Note,
}

#[derive(Debug, PartialEq)]
pub struct FinishBindingOperation {
    folder_id: Ulid,
    step: FinishStep,
    folder: Option<SyncedFolder>,
    failure: Option<FolderError>,
}

type FinishTurn = Result<Next<SyncedFolder>, FolderError>;

impl FinishBindingOperation {
    fn folder(&self) -> Result<&SyncedFolder, IoError> {
        self.folder.as_ref().ok_or(IoError::Unfinished)
    }

    fn auth(&self) -> Result<AuthContext, IoError> {
        let owner = self.folder()?.created_by;
        Ok(owner_auth(owner, owner.realm_id))
    }

    fn create_bucket(&mut self) -> FinishTurn {
        let folder = self.folder()?;
        let create = ForwardCreateOperation {
            auth: self.auth()?,
            remote: folder.remote.clone(),
            group_id: folder.group_id,
        };
        self.step = FinishStep::Remote;
        Ok(Next::Call(IoCall::child(create)))
    }

    fn check_bucket(&mut self) -> FinishTurn {
        let auth = self.auth()?;
        let remote = &self.folder()?.remote;
        let page = SyncPageLimit::new(1);
        let list = RequestVersionsOperation::new((remote, auth), None, page);
        self.step = FinishStep::Remote;
        Ok(Next::Call(IoCall::child(list)))
    }

    fn change(&mut self, change: FolderChange, step: FinishStep) -> FinishTurn {
        self.step = step;
        let update = UpdateFolderOperation::new(self.folder_id, change);
        Ok(Next::Call(IoCall::child(update)))
    }

    fn settled(&mut self, remote: Result<(), FolderError>) -> FinishTurn {
        match remote {
            Ok(()) => self.change(FolderChange::Activate, FinishStep::Activate),
            Err(
                error @ (FolderError::RemoteBucketMissing { .. }
                | FolderError::RemoteForbidden { .. }
                | FolderError::RemoteBucketConflict { .. }),
            ) => {
                self.failure = Some(error);
                self.step = FinishStep::Drop;
                let drop = DropBindingOperation::new(self.folder_id);
                Ok(Next::Call(IoCall::child(drop)))
            }
            Err(error) => {
                let note = FolderChange::NoteError {
                    reason: error.to_string(),
                    at_ms: unix_timestamp_millis(),
                };
                self.failure = Some(error);
                self.change(note, FinishStep::Note)
            }
        }
    }

    fn failed(&mut self) -> FinishTurn {
        Err(self.failure.take().unwrap_or(FolderError::Unavailable))
    }
}

impl Operation for FinishBindingOperation {
    type Output = SyncedFolder;
    type Error = FolderError;

    fn first(&mut self) -> FinishTurn {
        Ok(Next::Call(IoCall::child(read_bound(self.folder_id))))
    }

    fn next(&mut self, result: IoResult) -> FinishTurn {
        match self.step {
            // The caller's copy may be stale: a binding unbound or finished since is left alone.
            FinishStep::Read => {
                let folder = result.child::<SyncedFolder, FolderError>()??;
                let FolderState::Binding { create_bucket } = folder.state else {
                    return Ok(Next::Done(folder));
                };
                self.folder = Some(folder);
                match create_bucket {
                    // The binding must be on disk before the node may create a bucket for it.
                    true => {
                        self.step = FinishStep::Sync;
                        Ok(Next::Call(outside_call(IoCall::Sync)))
                    }
                    false => self.check_bucket(),
                }
            }
            FinishStep::Sync => match outside_answer(result).and_then(done) {
                Ok(()) => self.create_bucket(),
                Err(_) => self.settled(Err(FolderError::Unavailable)),
            },
            FinishStep::Remote => {
                let remote = self.folder()?.remote.clone();
                let answer = match self.folder()?.state {
                    FolderState::Binding {
                        create_bucket: true,
                    } => result.child::<(), RemoteError>()?,
                    _ => result.child::<SyncVersionPage, RemoteError>()?.map(|_| ()),
                };
                self.settled(answer.map_err(|error| map_remote_error(error, &remote)))
            }
            FinishStep::Activate => {
                let active = result.child::<SyncedFolder, FolderError>()??;
                self.folder = Some(active);
                self.step = FinishStep::Arm;
                Ok(Next::Call(arm_folders()))
            }
            FinishStep::Arm => {
                result.child::<(), IoError>()?.ok();
                let active = self.folder.take().ok_or(IoError::Unfinished)?;
                Ok(Next::Done(active))
            }
            FinishStep::Drop => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(folder = %self.folder_id, %error, "Could not drop the refused binding");
                }
                self.failed()
            }
            FinishStep::Note => {
                if let Err(error) = result.child::<SyncedFolder, FolderError>()? {
                    warn!(folder = %self.folder_id, %error, "Could not note the binding error");
                }
                self.failed()
            }
        }
    }
}

/// Removes a binding the realm refused, unless it changed meanwhile.
#[derive(Debug, PartialEq)]
struct DropBindingOperation {
    folder_id: Ulid,
    deleting: bool,
}

impl DropBindingOperation {
    fn new(folder_id: Ulid) -> Self {
        Self {
            folder_id,
            deleting: false,
        }
    }
}

impl Operation for DropBindingOperation {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let key = folder_key(self.folder_id);
        Ok(Next::Call(read_row(SYNCED_FOLDER_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.deleting {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let folder = value(result)?.and_then(|row| SyncedFolder::from_bytes(&row).ok());
        if !folder.is_some_and(|folder| matches!(folder.state, FolderState::Binding { .. })) {
            return Ok(Next::Done(()));
        }
        self.deleting = true;
        let row = (
            SYNCED_FOLDER_KEYSPACE.to_string(),
            folder_key(self.folder_id),
        );
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }
}

#[cfg(test)]
#[path = "bind_tests.rs"]
mod tests;
