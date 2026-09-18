//! Owns the persisted and wire records, grouped into domain modules.
//! Callers name a record through its domain, such as structs::storage::blob::BlobVersion.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod execution;
pub mod identity;
pub mod placement;
pub mod storage;

mod assistant_chat;
mod assistant_provider;
pub mod checksum;
mod info;
mod path_claim;
mod persistent_id;
mod sync_quarantine;
mod sync_relationship;
mod synced_folder;

pub use assistant_chat::{
    AssistantChatHead, AssistantChatTurn, MAX_ASSISTANT_BYTES, MAX_ASSISTANT_CHATS,
    MAX_ASSISTANT_TURNS, MAX_TURN_BYTES,
};
pub use assistant_provider::{
    AssistantHeaders, AssistantProvider, AssistantProviderKind, AssistantProviderSecret,
    AssistantProviderStatus, AssistantSecretError,
};
pub use info::{
    BackendState, BlobState, ConnectionAddressState, ConnectionAddressStatus,
    ConnectionMonitorState, NetState, NetworkDiagnosticsState, OpenConnection, PeerConnectionState,
    PeerConnectionStatus, ProtocolConnectionState, RequestSummaryState, Status,
};
pub use path_claim::{PathClaimRecord, PathResolution, resolve_path_claim};
pub use persistent_id::{
    MintPersistentSpec, PersistentIdFailure, PersistentIdKind, PersistentIdMapping,
    PersistentIdProvider, PersistentIdRevision, PersistentIdStatus, persistent_id_change,
    persistent_id_key, persistent_id_target,
};
pub use sync_quarantine::{
    QUARANTINE_MAX_BYTES, QUARANTINE_MAX_RECORDS, QUARANTINE_USAGE_KEY, SyncQuarantineCapacity,
    SyncQuarantineError, SyncQuarantineEvidence, SyncQuarantineFamily, SyncQuarantineIdentity,
    SyncQuarantineInput, SyncQuarantineRecord, SyncQuarantineUsage, SyncQuarantineWrite,
    build_quarantine_entries, check_quarantine_capacity, quarantine_row_entry,
    quarantine_usage_entry, sync_quarantine_key,
};
pub use sync_relationship::{
    ReferenceHandling, SyncCounters, SyncMode, SyncRelationship, SyncState, SyncStatusSnapshot,
    sync_relationship_key, sync_relationship_prefix, sync_state_key,
};
pub use synced_folder::{
    ActionKind, ActionOutcome, ActionScope, EntrySide, EntryState, FolderMode, FolderState,
    MAX_SYNC_PAGE, Observed, PendingMark, RemoteBinding, RemoteHead, ReplaceReason, SYNC_TRASH_DIR,
    SYNC_VERSION_TAG, SyncAction, SyncActionRecord, SyncBase, SyncListCursor, SyncPageLimit,
    SyncPolicy, SyncPullAck, SyncRefusal, SyncVersionPage, SyncedBytes, SyncedFolder, WriteGuard,
    decide,
};
