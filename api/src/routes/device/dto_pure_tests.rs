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
        remote_version: Ulid::from_bytes([1u8; 16]),
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
        remote_version: Ulid::from_bytes([2u8; 16]),
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
        folder_id: Ulid::from_bytes([3u8; 16]),
        root: "/home/user/data".to_string(),
        local_bucket: "device-bucket".to_string(),
        group_id: Ulid::from_bytes([4u8; 16]),
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
            Ulid::from_bytes([5u8; 16]),
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
        action_id: Ulid::from_bytes([6u8; 16]),
        folder_id: Ulid::from_bytes([7u8; 16]),
        kind: ActionKind::RemoveLocal,
        scope: ActionScope::Entry {
            relative: "notes.txt".to_string(),
        },
        actor: aruna_core::UserId::local(
            Ulid::from_bytes([8u8; 16]),
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
        folder_id: Ulid::from_bytes([9u8; 16]),
        root: "/home/user/data".to_string(),
        local_bucket: "device-bucket".to_string(),
        group_id: Ulid::from_bytes([10u8; 16]),
        remote: RemoteBinding {
            node_id: node,
            bucket: "realm-bucket".to_string(),
            prefix: String::new(),
        },
        mode: FolderMode::TwoWay,
        propagate_deletes: false,
        state: FolderState::Deleting,
        created_by: aruna_core::UserId::local(
            Ulid::from_bytes([11u8; 16]),
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
