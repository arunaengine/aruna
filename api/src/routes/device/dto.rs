//! Wire shapes of the device's synced folders. Every enum is snake_case on the
//! wire, so the desktop reads one vocabulary across folders, entries, actions
//! and transfers.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use aruna_core::structs::{
    ActionKind, ActionOutcome, ActionScope, EntrySide, EntryState, FolderMode, FolderState,
    ReplaceReason, SyncActionRecord, SyncBase, SyncedFolder,
};
use aruna_operations::device::sync::folders::FolderCounters;
use aruna_operations::device::sync::repository::{SyncUpload, UploadState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EntryStateName {
    InSync,
    LocalNew,
    LocalChanged,
    RemoteNew,
    RemoteChanged,
    Conflict,
    PendingReplace,
    RemoteDeleted,
    LocalDeleted,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FolderModeName {
    TwoWay,
    UploadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FolderStateName {
    Active,
    Paused,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReplaceReasonName {
    BaseUnknown,
    LocalModified,
}

/// What the owner may decide about one entry or one whole folder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EntryAction {
    ReplaceLocal,
    KeepLocal,
    RemoveLocal,
    Resolve,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionScopeName {
    Entry,
    AllPending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionOutcomeName {
    Applied,
    Stale,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransferState {
    Queued,
    Running,
    Retrying,
    Failed,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirection {
    Upload,
    Download,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RemoteBindingView {
    pub node_id: String,
    pub bucket: String,
    /// Key prefix inside the bucket. Empty binds the whole bucket.
    pub prefix: String,
}

/// How much of a folder still needs attention.
#[derive(Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct FolderCountersView {
    pub in_sync: usize,
    pub uploading: usize,
    pub conflicts: usize,
    pub pending_replacements: usize,
    pub remote_deleted: usize,
    pub errors: usize,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SyncedFolderView {
    pub folder_id: String,
    /// Absolute path on this machine, exactly as the owner gave it.
    pub root: String,
    /// Device-local bucket the directory is observed as. It is derived from
    /// the folder id, never named by the owner.
    pub local_bucket: String,
    pub group_id: String,
    pub remote: RemoteBindingView,
    pub mode: FolderModeName,
    pub propagate_deletes: bool,
    pub state: FolderStateName,
    /// Why the folder is in this state, when it has a reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub counters: FolderCountersView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_reconcile_ms: Option<u64>,
    pub created_at_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SyncedFolderList {
    pub folders: Vec<SyncedFolderView>,
}

/// One side of an entry as it was last seen.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct EntrySideView {
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_at_ms: Option<u64>,
    /// Weak identity of the local file: size and modification time. An action
    /// echoes it back as the bytes the owner decided about.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    /// Hex-encoded blake3.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blake3: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FolderEntryView {
    /// Path relative to the folder root.
    pub path: String,
    pub state: EntryStateName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local: Option<EntrySideView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote: Option<EntrySideView>,
    /// Why a replacement needs the owner, when the entry is pending one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<ReplaceReasonName>,
    /// Relative path of the conflicted copy the sync added beside the file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflicted_copy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FolderEntryPage {
    pub entries: Vec<FolderEntryView>,
    /// Opaque cursor of the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ActionRecordView {
    pub action: EntryAction,
    pub scope: ActionScopeName,
    /// The entry the action named, or the trash path after a removal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub actor: String,
    pub at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_blake3: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_blake3: Option<String>,
    pub outcome: ActionOutcomeName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ActionRecordPage {
    pub actions: Vec<ActionRecordView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// One transfer between this device and its realm node.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeviceTransfer {
    pub id: String,
    pub direction: TransferDirection,
    pub folder_id: String,
    /// Path relative to the folder root.
    pub path: String,
    /// Bucket the transfer reads from or writes to on the realm node.
    pub bucket: String,
    pub key: String,
    pub state: TransferState,
    pub bytes_total: u64,
    /// Bytes already moved. The realm node pulls in one pass, so this is 0
    /// until the transfer settles.
    pub bytes_done: u64,
    pub attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_attempt_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeviceTransferList {
    pub uploads: Vec<DeviceTransfer>,
    pub downloads: Vec<DeviceTransfer>,
}

pub fn hex_hash(hash: &[u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn side_view(side: &EntrySide) -> EntrySideView {
    EntrySideView {
        size: side.size,
        modified_at_ms: side.modified_at_ms,
        fingerprint: side.fingerprint.clone(),
        blake3: side.blake3.as_ref().map(hex_hash),
        version_id: side.version_id.map(|id| id.to_string()),
    }
}

fn state_name(entry: &EntryState) -> EntryStateName {
    match entry {
        EntryState::InSync => EntryStateName::InSync,
        EntryState::LocalNew => EntryStateName::LocalNew,
        EntryState::LocalChanged => EntryStateName::LocalChanged,
        EntryState::RemoteNew => EntryStateName::RemoteNew,
        EntryState::RemoteChanged => EntryStateName::RemoteChanged,
        EntryState::Conflict { .. } => EntryStateName::Conflict,
        EntryState::PendingReplace { .. } => EntryStateName::PendingReplace,
        EntryState::RemoteDeleted { .. } => EntryStateName::RemoteDeleted,
        EntryState::LocalDeleted => EntryStateName::LocalDeleted,
        EntryState::Error { .. } => EntryStateName::Error,
    }
}

pub fn entry_view(path: String, base: SyncBase) -> FolderEntryView {
    let (reason, conflicted_copy, message) = match &base.entry {
        EntryState::Conflict {
            conflicted_copy, ..
        } => (None, Some(conflicted_copy.clone()), None),
        EntryState::PendingReplace { reason, .. } => (
            Some(match reason {
                ReplaceReason::BaseUnknown => ReplaceReasonName::BaseUnknown,
                ReplaceReason::LocalModified => ReplaceReasonName::LocalModified,
            }),
            None,
            None,
        ),
        EntryState::Error { reason } => (None, None, Some(reason.clone())),
        _ => (None, None, None),
    };
    FolderEntryView {
        path,
        state: state_name(&base.entry),
        local: base.local.as_ref().map(side_view),
        remote: base.remote.as_ref().map(side_view),
        reason,
        conflicted_copy,
        message,
        updated_at_ms: base.synced_at_ms,
    }
}

pub fn folder_view(folder: SyncedFolder, counters: FolderCounters) -> SyncedFolderView {
    let (state, message) = match folder.state {
        FolderState::Active => (FolderStateName::Active, None),
        FolderState::Paused => (FolderStateName::Paused, None),
        FolderState::Error { reason } => (FolderStateName::Error, Some(reason)),
    };
    SyncedFolderView {
        folder_id: folder.folder_id.to_string(),
        root: folder.root,
        local_bucket: folder.local_bucket,
        group_id: folder.group_id.to_string(),
        remote: RemoteBindingView {
            node_id: folder.remote.node_id.to_string(),
            bucket: folder.remote.bucket,
            prefix: folder.remote.prefix,
        },
        mode: match folder.mode {
            FolderMode::TwoWay => FolderModeName::TwoWay,
            FolderMode::UploadOnly => FolderModeName::UploadOnly,
        },
        propagate_deletes: folder.propagate_deletes,
        state,
        message,
        counters: FolderCountersView {
            in_sync: counters.in_sync,
            uploading: counters.uploading,
            conflicts: counters.conflicts,
            pending_replacements: counters.pending_replacements,
            remote_deleted: counters.remote_deleted,
            errors: counters.errors,
        },
        last_reconcile_ms: folder.last_reconcile_ms,
        created_at_ms: folder.created_at_ms,
    }
}

pub fn action_view(record: SyncActionRecord) -> ActionRecordView {
    let (scope, path) = match record.scope {
        ActionScope::Entry { relative } => (ActionScopeName::Entry, Some(relative)),
        ActionScope::AllPending => (ActionScopeName::AllPending, None),
    };
    let (outcome, message) = match record.outcome {
        ActionOutcome::Applied => (ActionOutcomeName::Applied, None),
        ActionOutcome::Stale => (ActionOutcomeName::Stale, None),
        ActionOutcome::Failed { reason } => (ActionOutcomeName::Failed, Some(reason)),
    };
    ActionRecordView {
        action: match record.kind {
            ActionKind::Replace => EntryAction::ReplaceLocal,
            ActionKind::KeepLocal => EntryAction::KeepLocal,
            ActionKind::RemoveLocal => EntryAction::RemoveLocal,
            ActionKind::Resolve => EntryAction::Resolve,
        },
        scope,
        path,
        actor: record.actor.to_string(),
        at_ms: record.at_ms,
        before_blake3: record.before.as_ref().map(hex_hash),
        after_blake3: record.after.as_ref().map(hex_hash),
        outcome,
        message,
    }
}

/// One queued upload as a transfer. A row only exists while the realm node has
/// not confirmed the pull, so a listed upload is always still owed.
pub fn transfer_view(upload: SyncUpload, bucket: String, key: String) -> DeviceTransfer {
    let (state, attempts, next_attempt_ms, message) = match upload.state {
        UploadState::Pending {
            due_at_ms,
            attempts: 0,
            ..
        } => (TransferState::Queued, 0, Some(due_at_ms), None),
        UploadState::Pending {
            due_at_ms,
            attempts,
            last_error,
        } => (
            TransferState::Retrying,
            attempts,
            Some(due_at_ms),
            last_error,
        ),
        UploadState::Failed { reason, .. } => (TransferState::Failed, 0, None, Some(reason)),
    };
    DeviceTransfer {
        id: format!("{}:{}", upload.folder_id, upload.relative),
        direction: TransferDirection::Upload,
        folder_id: upload.folder_id.to_string(),
        path: upload.relative,
        bucket,
        key,
        state,
        bytes_total: upload.size,
        bytes_done: 0,
        attempts,
        next_attempt_ms,
        message,
    }
}

/// One entry the reconciler has decided to fetch but has not written yet.
pub fn download_view(
    folder_id: String,
    path: String,
    bucket: String,
    key: String,
    base: &SyncBase,
) -> DeviceTransfer {
    DeviceTransfer {
        id: format!("{folder_id}:{path}"),
        direction: TransferDirection::Download,
        folder_id,
        path,
        bucket,
        key,
        state: TransferState::Queued,
        bytes_total: base
            .remote
            .as_ref()
            .map(|side| side.size)
            .unwrap_or_default(),
        bytes_done: 0,
        attempts: 0,
        next_attempt_ms: None,
        message: None,
    }
}
