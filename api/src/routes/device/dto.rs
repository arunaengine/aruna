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
    /// The owner unbound it and the cleanup is still running.
    Deleting,
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
    pub observed: u64,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_at_ms: Option<u64>,
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
    /// Where a removal put the file, relative to the folder root.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trashed_to: Option<String>,
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
        EntryState::PendingReplace {
            reason,
            conflicted_copy,
            ..
        } => (
            Some(match reason {
                ReplaceReason::BaseUnknown => ReplaceReasonName::BaseUnknown,
                ReplaceReason::LocalModified => ReplaceReasonName::LocalModified,
            }),
            conflicted_copy.clone(),
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
        FolderState::Deleting => (FolderStateName::Deleting, None),
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
            observed: folder.observed_files,
            in_sync: counters.in_sync,
            uploading: counters.uploading,
            conflicts: counters.conflicts,
            pending_replacements: counters.pending_replacements,
            remote_deleted: counters.remote_deleted,
            errors: counters.errors,
        },
        last_reconcile_ms: folder.last_reconcile_ms,
        last_error: folder.last_error,
        last_error_at_ms: folder.last_error_at_ms,
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
        trashed_to: record.trashed_to,
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
    let (state, attempts, message) = match &base.entry {
        EntryState::Error { reason } => (TransferState::Failed, 1, Some(reason.clone())),
        _ => (TransferState::Queued, 0, None),
    };
    DeviceTransfer {
        id: format!("{folder_id}:{path}"),
        direction: TransferDirection::Download,
        folder_id,
        path,
        bucket,
        key,
        state,
        bytes_total: base
            .remote
            .as_ref()
            .map(|side| side.size)
            .unwrap_or_default(),
        bytes_done: 0,
        attempts,
        next_attempt_ms: None,
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::RemoteBinding;
    use ulid::Ulid;

    fn base_with(entry: EntryState) -> SyncBase {
        SyncBase {
            synced: None,
            local_version_id: None,
            synced_at_ms: 42,
            entry,
            pending_at: None,
            local: None,
            remote: None,
        }
    }

    #[test]
    fn reports_download_error() {
        // A failed remote read must not remain indistinguishable from queued work.
        let base = base_with(EntryState::Error {
            reason: "remote read failed".to_string(),
        });
        let transfer = download_view(
            "folder".to_string(),
            "notes.txt".to_string(),
            "bucket".to_string(),
            "notes.txt".to_string(),
            &base,
        );
        assert_eq!(transfer.state, TransferState::Failed);
        assert_eq!(transfer.attempts, 1);
        assert_eq!(transfer.message.as_deref(), Some("remote read failed"));
    }

    #[test]
    fn pending_replace_reason() {
        let base = base_with(EntryState::PendingReplace {
            reason: ReplaceReason::BaseUnknown,
            remote_version: Ulid::generate(),
            conflicted_copy: Some("notes (1).txt".to_string()),
        });
        let view = entry_view("notes.txt".to_string(), base);
        assert_eq!(view.state, EntryStateName::PendingReplace);
        assert_eq!(view.reason, Some(ReplaceReasonName::BaseUnknown));
        assert_eq!(view.conflicted_copy.as_deref(), Some("notes (1).txt"));
        assert_eq!(view.updated_at_ms, 42);
    }

    #[test]
    fn conflict_reports_copy() {
        let base = base_with(EntryState::Conflict {
            remote_version: Ulid::generate(),
            conflicted_copy: "notes (conflict).txt".to_string(),
        });
        let view = entry_view("notes.txt".to_string(), base);
        assert_eq!(view.state, EntryStateName::Conflict);
        assert!(view.reason.is_none());
        assert_eq!(
            view.conflicted_copy.as_deref(),
            Some("notes (conflict).txt")
        );
    }

    #[test]
    fn folder_error_message() {
        let node = iroh::PublicKey::from_bytes(
            &ed25519_dalek::SigningKey::from_bytes(&[9u8; 32])
                .verifying_key()
                .to_bytes(),
        )
        .unwrap();
        let folder = SyncedFolder {
            folder_id: Ulid::generate(),
            root: "/home/user/data".to_string(),
            local_bucket: "device-bucket".to_string(),
            group_id: Ulid::generate(),
            remote: RemoteBinding {
                node_id: node,
                bucket: "realm-bucket".to_string(),
                prefix: "sub".to_string(),
            },
            mode: FolderMode::UploadOnly,
            propagate_deletes: true,
            state: FolderState::Error {
                reason: "watcher stopped".to_string(),
            },
            created_by: aruna_core::UserId::local(
                Ulid::generate(),
                aruna_core::structs::RealmId::from_bytes([5u8; 32]),
            ),
            created_at_ms: 7,
            last_reconcile_ms: None,
            last_error: Some("watcher stopped".to_string()),
            last_error_at_ms: Some(9),
            observed_files: 3,
            list_cursor: None,
        };
        let counters = FolderCounters {
            in_sync: 1,
            uploading: 2,
            conflicts: 0,
            pending_replacements: 0,
            remote_deleted: 0,
            errors: 1,
        };
        let view = folder_view(folder, counters);
        assert_eq!(view.state, FolderStateName::Error);
        assert_eq!(view.message.as_deref(), Some("watcher stopped"));
        assert_eq!(view.mode, FolderModeName::UploadOnly);
        assert_eq!(view.counters.observed, 3);
        assert_eq!(view.counters.uploading, 2);
        assert_eq!(view.remote.prefix, "sub");
        assert!(!view.remote.node_id.is_empty());
    }

    #[test]
    fn failed_action_reason() {
        let record = SyncActionRecord {
            action_id: Ulid::generate(),
            folder_id: Ulid::generate(),
            kind: ActionKind::RemoveLocal,
            scope: ActionScope::Entry {
                relative: "notes.txt".to_string(),
            },
            actor: aruna_core::UserId::local(
                Ulid::generate(),
                aruna_core::structs::RealmId::from_bytes([5u8; 32]),
            ),
            at_ms: 11,
            before: Some([1u8; 32]),
            after: None,
            outcome: ActionOutcome::Failed {
                reason: "disk full".to_string(),
            },
            trashed_to: Some(".trash/notes.txt".to_string()),
            entries: 0,
        };
        let view = action_view(record);
        assert_eq!(view.action, EntryAction::RemoveLocal);
        assert_eq!(view.scope, ActionScopeName::Entry);
        assert_eq!(view.path.as_deref(), Some("notes.txt"));
        assert_eq!(view.outcome, ActionOutcomeName::Failed);
        assert_eq!(view.message.as_deref(), Some("disk full"));
        assert_eq!(
            view.before_blake3.as_deref(),
            Some(hex_hash(&[1u8; 32]).as_str())
        );
        assert!(view.after_blake3.is_none());
    }

    #[test]
    fn maps_upload_state() {
        let upload = |state| SyncUpload {
            folder_id: Ulid::from_bytes([2u8; 16]),
            relative: "a.txt".to_string(),
            deleted: false,
            fingerprint: "fp".to_string(),
            blake3: None,
            size: 5,
            local_version: None,
            queued_at_ms: 1,
            state,
        };
        let queued = transfer_view(
            upload(UploadState::Pending {
                due_at_ms: 10,
                attempts: 0,
                last_error: None,
            }),
            "bucket".to_string(),
            "a.txt".to_string(),
        );
        assert_eq!(queued.state, TransferState::Queued);
        assert_eq!(queued.next_attempt_ms, Some(10));

        let retrying = transfer_view(
            upload(UploadState::Pending {
                due_at_ms: 20,
                attempts: 3,
                last_error: Some("timeout".to_string()),
            }),
            "bucket".to_string(),
            "a.txt".to_string(),
        );
        assert_eq!(retrying.state, TransferState::Retrying);
        assert_eq!(retrying.attempts, 3);
        assert_eq!(retrying.message.as_deref(), Some("timeout"));

        let failed = transfer_view(
            upload(UploadState::Failed {
                reason: "rejected".to_string(),
                retryable: false,
            }),
            "bucket".to_string(),
            "a.txt".to_string(),
        );
        assert_eq!(failed.state, TransferState::Failed);
        assert_eq!(failed.message.as_deref(), Some("rejected"));
    }

    #[test]
    fn projects_entry_sides() {
        // A populated entry projects both observed sides, hashes hex-encoded.
        let mut base = base_with(EntryState::LocalChanged);
        base.local = Some(EntrySide {
            size: 10,
            modified_at_ms: Some(5),
            fingerprint: Some("fp".to_string()),
            blake3: Some([0xabu8; 32]),
            version_id: None,
        });
        base.remote = Some(EntrySide {
            size: 20,
            modified_at_ms: None,
            fingerprint: None,
            blake3: None,
            version_id: Some(Ulid::from_bytes([2u8; 16])),
        });
        let view = entry_view("data.bin".to_string(), base);
        assert_eq!(view.state, EntryStateName::LocalChanged);
        let local = view.local.unwrap();
        assert_eq!(local.size, 10);
        assert_eq!(local.fingerprint.as_deref(), Some("fp"));
        assert_eq!(
            local.blake3.as_deref(),
            Some(hex_hash(&[0xabu8; 32]).as_str())
        );
        let remote = view.remote.unwrap();
        assert_eq!(remote.size, 20);
        assert!(remote.blake3.is_none());
        assert_eq!(
            remote.version_id,
            Some(Ulid::from_bytes([2u8; 16]).to_string())
        );
    }

    #[test]
    fn projects_deleting_folder() {
        let node = iroh::PublicKey::from_bytes(
            &ed25519_dalek::SigningKey::from_bytes(&[9u8; 32])
                .verifying_key()
                .to_bytes(),
        )
        .unwrap();
        let folder = SyncedFolder {
            folder_id: Ulid::generate(),
            root: "/home/user/data".to_string(),
            local_bucket: "device-bucket".to_string(),
            group_id: Ulid::generate(),
            remote: RemoteBinding {
                node_id: node,
                bucket: "realm-bucket".to_string(),
                prefix: String::new(),
            },
            mode: FolderMode::TwoWay,
            propagate_deletes: false,
            state: FolderState::Deleting,
            created_by: aruna_core::UserId::local(
                Ulid::generate(),
                aruna_core::structs::RealmId::from_bytes([5u8; 32]),
            ),
            created_at_ms: 7,
            last_reconcile_ms: Some(9),
            last_error: None,
            last_error_at_ms: None,
            observed_files: 0,
            list_cursor: None,
        };
        let view = folder_view(folder, FolderCounters::default());
        assert_eq!(view.state, FolderStateName::Deleting);
        assert_eq!(view.mode, FolderModeName::TwoWay);
        assert!(view.message.is_none());
        assert!(!view.propagate_deletes);
        assert_eq!(view.last_reconcile_ms, Some(9));
    }

    #[test]
    fn hex_hash_lowercase() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xab;
        bytes[31] = 0x0f;
        let hex = hex_hash(&bytes);
        assert_eq!(hex.len(), 64);
        assert!(hex.starts_with("ab"));
        assert!(hex.ends_with("0f"));
    }
}
