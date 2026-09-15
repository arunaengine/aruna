//! Persisted and wire records, grouped into domain modules. Small families
//! stay flat; callers name a record through its domain, e.g.
//! `structs::storage::blob::BlobVersion`.

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
    AssistantChatHead, AssistantChatTurn, MAX_ASSISTANT_CHAT_BYTES, MAX_ASSISTANT_CHAT_TURNS,
    MAX_ASSISTANT_CHATS, MAX_ASSISTANT_TURN_BYTES,
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
    SYNC_QUARANTINE_MAX_BYTES, SYNC_QUARANTINE_MAX_RECORDS, SYNC_QUARANTINE_USAGE_KEY,
    SyncQuarantineCapacity, SyncQuarantineError, SyncQuarantineEvidence, SyncQuarantineFamily,
    SyncQuarantineIdentity, SyncQuarantineInput, SyncQuarantineRecord, SyncQuarantineUsage,
    SyncQuarantineWrite, build_quarantine_entries, check_quarantine_capacity, quarantine_row_entry,
    quarantine_usage_entry, sync_quarantine_key,
};
pub use sync_relationship::{
    ReferenceHandling, SyncCounters, SyncMode, SyncRelationship, SyncState, SyncStatusSnapshot,
    sync_relationship_key, sync_relationship_prefix, sync_state_key,
};
pub use synced_folder::{
    ActionKind, ActionOutcome, ActionScope, EntrySide, EntryState, FolderMode, FolderState,
    MAX_SYNC_PAGE, Observed, PendingMark, RemoteBinding, RemoteHead, ReplaceReason,
    SYNC_SOURCE_VERSION_TAG, SYNC_TRASH_DIR, SyncAction, SyncActionRecord, SyncBase,
    SyncListCursor, SyncPageLimit, SyncPolicy, SyncPullAck, SyncRefusal, SyncVersionPage,
    SyncedBytes, SyncedFolder, WriteGuard, decide,
};

// Temporary flat re-exports of records that moved into the domain modules.
// TODO(polisher): migrate callers to the domain paths, then delete this block.
pub use execution::harvest::*;
pub use execution::job::*;
pub use execution::notification::*;
pub use execution::notification_watch::*;
pub use execution::offered_directory::*;
pub use execution::source_access::*;
pub use execution::source_connector::*;
pub use execution::staging::*;
pub use identity::auth::*;
pub use identity::group::*;
pub use identity::realm::*;
pub use identity::s3_session::*;
pub use identity::user::*;
pub use identity::user_session::*;
pub use identity::user_vault::*;
pub use placement::binding_directory::*;
pub use placement::compute_config::*;
pub use placement::handle_allocation::*;
pub use placement::node_subject::*;
pub use placement::placement_policy::*;
pub use placement::placement_record::*;
pub use placement::placement_transition::*;
pub use placement::policy_attachment::*;
pub use placement::policy_document::*;
pub use storage::backends::*;
pub use storage::blob::*;
pub use storage::cleanup::*;
pub use storage::delete_audit::*;
pub use storage::group_backend::*;
pub use storage::metadata_registry::*;
pub use storage::multipart::*;
pub use storage::node_info::*;
pub use storage::replication::*;
pub use storage::routing::*;
pub use storage::storage_purge::*;
pub use storage::usage::*;
