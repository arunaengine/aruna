//! Two-way synced folders on an owner's device.
//!
//! Local data takes precedence over convergence (LB1): the automatic sync may
//! only add files that are absent, add conflicted copies beside files that
//! diverged, and replace a file whose current fingerprint AND blake3 still
//! equal the recorded base. Replacing divergent or unknown-base bytes and
//! removing a file are explicit, audited owner actions, and a remote deletion
//! never deletes a local file.

use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::types::{GroupId, UserId};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Remote heads one listing page may carry.
pub const MAX_SYNC_PAGE: usize = 256;

/// Version metadata tag naming the device version a realm object was pulled
/// from. It makes a replayed pull idempotent instead of a second version.
pub const SYNC_SOURCE_VERSION_TAG: &str = "aruna:sync-source-version";

/// Directory a folder's move-aside puts files into, relative to the root.
pub const SYNC_TRASH_DIR: &str = ".aruna/trash";

/// Which directions a folder syncs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FolderMode {
    /// The device publishes its files and never writes to disk.
    UploadOnly,
    TwoWay,
}

/// Whether a folder currently reconciles.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FolderState {
    Active,
    Paused,
    Error { reason: String },
}

/// The realm side of a binding: one bucket prefix on one named realm node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteBinding {
    pub node_id: NodeId,
    pub bucket: String,
    /// Key prefix inside the bucket. Empty binds the whole bucket.
    pub prefix: String,
}

impl RemoteBinding {
    /// The realm key one relative path maps to.
    pub fn remote_key(&self, relative: &str) -> String {
        match self.prefix.is_empty() {
            true => relative.to_string(),
            false => format!("{}/{relative}", self.prefix.trim_end_matches('/')),
        }
    }

    /// The relative path one realm key maps back to, or `None` when the key is
    /// outside the bound prefix.
    pub fn relative_path(&self, key: &str) -> Option<String> {
        if self.prefix.is_empty() {
            return Some(key.to_string());
        }
        let prefix = format!("{}/", self.prefix.trim_end_matches('/'));
        key.strip_prefix(&prefix).map(ToOwned::to_owned)
    }
}

/// One local directory bound to a realm bucket prefix. The record is
/// device-local: the root path is the one fact about the owner's machine that
/// must never leave it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncedFolder {
    pub folder_id: Ulid,
    /// Root as the owner registered it; every access re-resolves it.
    pub root: String,
    /// Device-local bucket the folder's files are observed as.
    pub local_bucket: String,
    pub group_id: GroupId,
    pub remote: RemoteBinding,
    pub mode: FolderMode,
    /// Whether a local delete becomes a realm delete marker.
    pub propagate_deletes: bool,
    pub state: FolderState,
    pub created_by: UserId,
    pub created_at_ms: u64,
    pub last_reconcile_ms: Option<u64>,
}

impl SyncedFolder {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn policy(&self) -> SyncPolicy {
        SyncPolicy {
            mode: self.mode,
            propagate_deletes: self.propagate_deletes,
        }
    }
}

/// The folder settings one decision depends on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncPolicy {
    pub mode: FolderMode,
    pub propagate_deletes: bool,
}

/// Why a file may not be replaced automatically.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplaceReason {
    /// No base was ever recorded for this path.
    BaseUnknown,
    /// The local bytes no longer equal the recorded base.
    LocalModified,
}

/// What the sync currently knows about one entry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EntryState {
    InSync,
    LocalNew,
    LocalChanged,
    RemoteNew,
    RemoteChanged,
    /// Both sides moved: the local bytes stay, the remote version landed beside
    /// them and the owner still has to review.
    Conflict {
        remote_version: Ulid,
        conflicted_copy: String,
    },
    /// The remote version may replace the local file only through an explicit
    /// owner action.
    PendingReplace {
        reason: ReplaceReason,
        remote_version: Ulid,
    },
    /// The realm deleted the object. The file is kept and reported.
    RemoteDeleted {
        remote_version: Ulid,
    },
    LocalDeleted,
    Error {
        reason: String,
    },
}

impl EntryState {
    /// Whether the entry waits for an explicit owner decision.
    pub fn is_pending(&self) -> bool {
        matches!(
            self,
            EntryState::Conflict { .. }
                | EntryState::PendingReplace { .. }
                | EntryState::RemoteDeleted { .. }
        )
    }

    /// Stable name the API and the UI filter entries by.
    pub fn name(&self) -> &'static str {
        match self {
            EntryState::InSync => "in_sync",
            EntryState::LocalNew => "local_new",
            EntryState::LocalChanged => "local_changed",
            EntryState::RemoteNew => "remote_new",
            EntryState::RemoteChanged => "remote_changed",
            EntryState::Conflict { .. } => "conflict",
            EntryState::PendingReplace { .. } => "pending_replace",
            EntryState::RemoteDeleted { .. } => "remote_deleted",
            EntryState::LocalDeleted => "local_deleted",
            EntryState::Error { .. } => "error",
        }
    }
}

/// What the last successful sync recorded about one path. It is the merge base
/// every LB1 decision is taken against.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncBase {
    pub fingerprint: String,
    pub blake3: [u8; 32],
    pub size: u64,
    /// Device-local observation version the base was taken from.
    pub local_version_id: Option<Ulid>,
    /// Realm version the base was taken from.
    pub remote_version_id: Option<Ulid>,
    pub synced_at_ms: u64,
    pub entry: EntryState,
}

impl SyncBase {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Whether the file on disk still carries exactly the bytes the base
    /// recorded. Both the weak fingerprint and the strong hash must match: this
    /// is the only condition under which the sync may overwrite a file.
    pub fn matches(&self, local: &Observed) -> bool {
        self.fingerprint == local.fingerprint && local.blake3 == Some(self.blake3)
    }

    /// The guard a guarded replace of this entry must carry.
    pub fn guard(&self) -> WriteGuard {
        WriteGuard::MatchesBase {
            fingerprint: self.fingerprint.clone(),
            blake3: self.blake3,
        }
    }
}

/// One file as the device currently observes it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Observed {
    pub fingerprint: String,
    pub size: u64,
    /// Strong hash of exactly these bytes, when the device has computed one.
    /// An absent hash can never satisfy a base match.
    pub blake3: Option<[u8; 32]>,
    pub version_id: Option<Ulid>,
}

/// One current realm head inside the bound prefix.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteHead {
    /// Path relative to the folder root.
    pub relative: String,
    pub version_id: Ulid,
    pub size: u64,
    pub blake3: Option<[u8; 32]>,
    pub deleted: bool,
}

/// Condition the adapter re-verifies immediately before it renames a file into
/// place. It repeats the operation's decision on purpose: the file may change
/// between the decision and the write.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WriteGuard {
    /// The target must not exist. Nothing is ever replaced.
    MustNotExist,
    /// The target must still carry exactly these bytes.
    MatchesBase {
        fingerprint: String,
        blake3: [u8; 32],
    },
}

/// What the automatic reconciliation does with one entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncAction {
    /// Both sides already agree.
    Nothing,
    /// Record the observed bytes as the base; neither side is touched.
    AdoptBase,
    /// Publish the local version to the realm.
    Upload { deleted: bool },
    /// Write the remote version to disk under `guard`.
    Materialize {
        remote_version: Ulid,
        guard: WriteGuard,
    },
    /// Keep the local bytes and add the remote version beside them. `upload`
    /// also publishes the local bytes as a new realm version.
    ConflictCopy { remote_version: Ulid, upload: bool },
    /// Preserve and report; only an explicit owner action changes this entry.
    Report(EntryState),
    /// Neither side holds the entry any more: drop the base row.
    Forget,
}

/// The one place LB1 is decided. It is pure, total and takes no I/O: the
/// adapter's guard repeats the same rule when it renames.
pub fn decide(
    policy: SyncPolicy,
    local: Option<&Observed>,
    base: Option<&SyncBase>,
    remote: Option<&RemoteHead>,
) -> SyncAction {
    let remote = remote.filter(|head| !head.deleted);
    match policy.mode {
        FolderMode::UploadOnly => decide_upload(policy, local, base),
        FolderMode::TwoWay => decide_two_way(policy, local, base, remote),
    }
}

/// An upload-only folder never writes to disk, so the realm side cannot decide
/// anything: the device publishes what it observes.
fn decide_upload(
    policy: SyncPolicy,
    local: Option<&Observed>,
    base: Option<&SyncBase>,
) -> SyncAction {
    match (local, base) {
        (Some(local), Some(base)) if base.matches(local) => SyncAction::Nothing,
        (Some(_), _) => SyncAction::Upload { deleted: false },
        (None, Some(_)) if policy.propagate_deletes => SyncAction::Upload { deleted: true },
        (None, Some(_)) => SyncAction::Report(EntryState::LocalDeleted),
        (None, None) => SyncAction::Nothing,
    }
}

fn decide_two_way(
    policy: SyncPolicy,
    local: Option<&Observed>,
    base: Option<&SyncBase>,
    remote: Option<&RemoteHead>,
) -> SyncAction {
    match (local, base, remote) {
        (None, None, None) => SyncAction::Nothing,
        (None, Some(_), None) => SyncAction::Forget,
        // Nothing on disk: adding the remote file loses nothing, and the guard
        // still refuses if something appeared in the meantime.
        (None, base, Some(remote)) => match base {
            Some(base) if base.remote_version_id == Some(remote.version_id) => decide_gone(policy),
            _ => SyncAction::Materialize {
                remote_version: remote.version_id,
                guard: WriteGuard::MustNotExist,
            },
        },
        (Some(_), None, None) => SyncAction::Upload { deleted: false },
        (Some(_), Some(base), None) => match base.remote_version_id {
            // The realm removed it; the file is kept whatever the owner decides.
            Some(remote_version) => {
                SyncAction::Report(EntryState::RemoteDeleted { remote_version })
            }
            // The realm never held it, so the upload is simply unfinished.
            None => SyncAction::Upload { deleted: false },
        },
        // No base: the bytes may be the same file or two unrelated ones, so
        // nothing is replaced and nothing is published without the owner.
        (Some(local), None, Some(remote)) => match same_bytes(local, remote) {
            true => SyncAction::AdoptBase,
            false => SyncAction::ConflictCopy {
                remote_version: remote.version_id,
                upload: false,
            },
        },
        (Some(local), Some(base), Some(remote)) => {
            let local_changed = !base.matches(local);
            let remote_changed = base.remote_version_id != Some(remote.version_id);
            match (local_changed, remote_changed) {
                (false, false) => SyncAction::Nothing,
                (true, false) => SyncAction::Upload { deleted: false },
                (false, true) => SyncAction::Materialize {
                    remote_version: remote.version_id,
                    guard: base.guard(),
                },
                (true, true) => SyncAction::ConflictCopy {
                    remote_version: remote.version_id,
                    upload: true,
                },
            }
        }
    }
}

/// A file the owner removed while the realm head stayed the base's.
fn decide_gone(policy: SyncPolicy) -> SyncAction {
    match policy.propagate_deletes {
        true => SyncAction::Upload { deleted: true },
        false => SyncAction::Report(EntryState::LocalDeleted),
    }
}

/// Whether the local and the remote side provably hold the same bytes. An
/// unknown hash on either side is never a match.
fn same_bytes(local: &Observed, remote: &RemoteHead) -> bool {
    local.size == remote.size
        && matches!((local.blake3, remote.blake3), (Some(left), Some(right)) if left == right)
}

/// One explicit owner decision about an entry or a whole folder.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ActionKind {
    /// Replace the local bytes with the remote version.
    Replace,
    /// Keep the local bytes and publish them as a new realm version.
    KeepLocal,
    /// Move the local file into the folder's trash. It is never unlinked.
    RemoveLocal,
    /// Accept the current state and clear the pending flag.
    Resolve,
}

/// What one explicit action covered.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ActionScope {
    Entry {
        relative: String,
    },
    /// Every entry of the folder that was pending when the action ran.
    AllPending,
}

/// Outcome of one explicit action, recorded whether it applied or not.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ActionOutcome {
    Applied,
    /// The bytes the owner saw are no longer the bytes on disk.
    Stale,
    Failed {
        reason: String,
    },
}

/// One audit row. It is written in the same transaction as the write it
/// records, so an applied replacement can never be missing from the log.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncActionRecord {
    pub action_id: Ulid,
    pub folder_id: Ulid,
    pub kind: ActionKind,
    pub scope: ActionScope,
    pub actor: UserId,
    pub at_ms: u64,
    /// Hashes as they were before and after the action, for the entry it named.
    pub before: Option<[u8; 32]>,
    pub after: Option<[u8; 32]>,
    pub outcome: ActionOutcome,
    /// Entries an all-pending action covered.
    pub entries: usize,
}

impl SyncActionRecord {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// What a realm node answers to one accepted sync pull.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncPullAck {
    pub version_id: Ulid,
    /// The same device version had already been committed.
    pub already_applied: bool,
}

/// Why a realm node refused a sync pull or a listing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncRefusal {
    Unauthorized,
    Forbidden,
    NotFound,
    Invalid(String),
    Unavailable,
}

/// Opaque continuation of a bounded head listing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncListCursor {
    pub key: String,
    pub version_id: Option<Ulid>,
}

/// A bounded page of remote heads. The bound holds at decode too, because a
/// peer supplies the bytes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "SyncPageParts")]
pub struct SyncVersionPage {
    heads: Vec<RemoteHead>,
    next_cursor: Option<SyncListCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SyncPageParts {
    heads: Vec<RemoteHead>,
    next_cursor: Option<SyncListCursor>,
}

impl SyncVersionPage {
    pub fn new(
        heads: Vec<RemoteHead>,
        next_cursor: Option<SyncListCursor>,
    ) -> Result<Self, ConversionError> {
        if heads.len() > MAX_SYNC_PAGE {
            return Err(ConversionError::FromStrError(
                "a synced-folder listing page is bounded".to_string(),
            ));
        }
        Ok(Self { heads, next_cursor })
    }

    pub fn heads(&self) -> &[RemoteHead] {
        &self.heads
    }

    pub fn next_cursor(&self) -> Option<&SyncListCursor> {
        self.next_cursor.as_ref()
    }

    pub fn into_parts(self) -> (Vec<RemoteHead>, Option<SyncListCursor>) {
        (self.heads, self.next_cursor)
    }
}

impl TryFrom<SyncPageParts> for SyncVersionPage {
    type Error = ConversionError;

    fn try_from(parts: SyncPageParts) -> Result<Self, Self::Error> {
        Self::new(parts.heads, parts.next_cursor)
    }
}

/// Requested page size, clamped to the documented maximum.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "usize")]
pub struct SyncPageLimit(usize);

impl SyncPageLimit {
    pub fn new(limit: usize) -> Self {
        Self(limit.clamp(1, MAX_SYNC_PAGE))
    }

    pub fn get(self) -> usize {
        self.0
    }
}

impl Default for SyncPageLimit {
    fn default() -> Self {
        Self(MAX_SYNC_PAGE)
    }
}

impl TryFrom<usize> for SyncPageLimit {
    type Error = ConversionError;

    fn try_from(limit: usize) -> Result<Self, Self::Error> {
        if limit == 0 || limit > MAX_SYNC_PAGE {
            return Err(ConversionError::FromStrError(
                "a synced-folder page limit is bounded".to_string(),
            ));
        }
        Ok(Self(limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::RealmId;

    fn base(fingerprint: &str, hash: u8, remote: Option<Ulid>) -> SyncBase {
        SyncBase {
            fingerprint: fingerprint.to_string(),
            blake3: [hash; 32],
            size: 4,
            local_version_id: None,
            remote_version_id: remote,
            synced_at_ms: 1,
            entry: EntryState::InSync,
        }
    }

    fn observed(fingerprint: &str, hash: Option<u8>) -> Observed {
        Observed {
            fingerprint: fingerprint.to_string(),
            size: 4,
            blake3: hash.map(|byte| [byte; 32]),
            version_id: None,
        }
    }

    fn head(version: Ulid, hash: Option<u8>, deleted: bool) -> RemoteHead {
        RemoteHead {
            relative: "note.txt".to_string(),
            version_id: version,
            size: 4,
            blake3: hash.map(|byte| [byte; 32]),
            deleted,
        }
    }

    fn two_way() -> SyncPolicy {
        SyncPolicy {
            mode: FolderMode::TwoWay,
            propagate_deletes: true,
        }
    }

    #[test]
    fn adds_remote_file() {
        // A file that does not exist locally may always be added, guarded.
        let remote = head(Ulid::from_bytes([1u8; 16]), Some(3), false);
        assert_eq!(
            decide(two_way(), None, None, Some(&remote)),
            SyncAction::Materialize {
                remote_version: remote.version_id,
                guard: WriteGuard::MustNotExist,
            }
        );
    }

    #[test]
    fn replaces_unchanged_file() {
        // The one automatic overwrite: fingerprint AND blake3 equal the base.
        let old = Ulid::from_bytes([1u8; 16]);
        let new = Ulid::from_bytes([2u8; 16]);
        let base = base("4-1", 9, Some(old));
        let local = observed("4-1", Some(9));
        let remote = head(new, Some(7), false);
        assert_eq!(
            decide(two_way(), Some(&local), Some(&base), Some(&remote)),
            SyncAction::Materialize {
                remote_version: new,
                guard: base.guard(),
            }
        );
    }

    #[test]
    fn keeps_drifted_file() {
        // A changed local file is never replaced: the remote lands beside it.
        let old = Ulid::from_bytes([1u8; 16]);
        let new = Ulid::from_bytes([2u8; 16]);
        let base = base("4-1", 9, Some(old));
        let local = observed("4-2", Some(5));
        let remote = head(new, Some(7), false);
        assert_eq!(
            decide(two_way(), Some(&local), Some(&base), Some(&remote)),
            SyncAction::ConflictCopy {
                remote_version: new,
                upload: true,
            }
        );
    }

    #[test]
    fn refuses_stale_fingerprint() {
        // A same-size rewrite that keeps the hash unknown must not count as the
        // base: an unknown strong hash can never satisfy a match.
        let base = base("4-1", 9, Some(Ulid::from_bytes([1u8; 16])));
        assert!(!base.matches(&observed("4-1", None)));
        assert!(!base.matches(&observed("4-1", Some(8))));
        assert!(base.matches(&observed("4-1", Some(9))));
    }

    #[test]
    fn holds_unknown_base() {
        // Two files under one path with no base: neither side is replaced.
        let remote = head(Ulid::from_bytes([2u8; 16]), Some(7), false);
        let local = observed("4-1", Some(9));
        assert_eq!(
            decide(two_way(), Some(&local), None, Some(&remote)),
            SyncAction::ConflictCopy {
                remote_version: remote.version_id,
                upload: false,
            }
        );
    }

    #[test]
    fn adopts_equal_bytes() {
        let hash = 7u8;
        let remote = head(Ulid::from_bytes([2u8; 16]), Some(hash), false);
        let local = observed("4-1", Some(hash));
        assert_eq!(
            decide(two_way(), Some(&local), None, Some(&remote)),
            SyncAction::AdoptBase
        );
    }

    #[test]
    fn keeps_deleted_remote() {
        // A remote deletion never removes a local file, changed or not.
        let old = Ulid::from_bytes([1u8; 16]);
        let base = base("4-1", 9, Some(old));
        let remote = head(Ulid::from_bytes([2u8; 16]), None, true);
        let unchanged = observed("4-1", Some(9));
        let changed = observed("4-9", Some(3));
        let expected = SyncAction::Report(EntryState::RemoteDeleted {
            remote_version: old,
        });
        assert_eq!(
            decide(two_way(), Some(&unchanged), Some(&base), Some(&remote)),
            expected
        );
        assert_eq!(
            decide(two_way(), Some(&changed), Some(&base), None),
            expected
        );
    }

    #[test]
    fn uploads_local_change() {
        let old = Ulid::from_bytes([1u8; 16]);
        let base = base("4-1", 9, Some(old));
        let local = observed("4-2", Some(5));
        let remote = head(old, Some(9), false);
        assert_eq!(
            decide(two_way(), Some(&local), Some(&base), Some(&remote)),
            SyncAction::Upload { deleted: false }
        );
        assert_eq!(
            decide(two_way(), Some(&local), None, None),
            SyncAction::Upload { deleted: false }
        );
    }

    #[test]
    fn propagates_local_delete() {
        let old = Ulid::from_bytes([1u8; 16]);
        let base = base("4-1", 9, Some(old));
        let remote = head(old, Some(9), false);
        assert_eq!(
            decide(two_way(), None, Some(&base), Some(&remote)),
            SyncAction::Upload { deleted: true }
        );
        let keep = SyncPolicy {
            propagate_deletes: false,
            ..two_way()
        };
        assert_eq!(
            decide(keep, None, Some(&base), Some(&remote)),
            SyncAction::Report(EntryState::LocalDeleted)
        );
    }

    #[test]
    fn restores_changed_remote() {
        // The owner deleted the file and the realm changed it: adding the newer
        // remote version back loses nothing that is still on disk.
        let base = base("4-1", 9, Some(Ulid::from_bytes([1u8; 16])));
        let remote = head(Ulid::from_bytes([2u8; 16]), Some(7), false);
        assert_eq!(
            decide(two_way(), None, Some(&base), Some(&remote)),
            SyncAction::Materialize {
                remote_version: remote.version_id,
                guard: WriteGuard::MustNotExist,
            }
        );
    }

    #[test]
    fn forgets_absent_entry() {
        let base = base("4-1", 9, Some(Ulid::from_bytes([1u8; 16])));
        assert_eq!(
            decide(two_way(), None, Some(&base), None),
            SyncAction::Forget
        );
        assert_eq!(decide(two_way(), None, None, None), SyncAction::Nothing);
    }

    #[test]
    fn upload_only_ignores_remote() {
        // An upload-only folder never writes to disk, whatever the realm holds.
        let policy = SyncPolicy {
            mode: FolderMode::UploadOnly,
            propagate_deletes: true,
        };
        let remote = head(Ulid::from_bytes([2u8; 16]), Some(7), false);
        assert_eq!(
            decide(policy, None, None, Some(&remote)),
            SyncAction::Nothing
        );
        let local = observed("4-1", Some(9));
        assert_eq!(
            decide(policy, Some(&local), None, Some(&remote)),
            SyncAction::Upload { deleted: false }
        );
    }

    #[test]
    fn maps_prefix_keys() {
        let binding = RemoteBinding {
            node_id: NodeId::from_bytes(&[3u8; 32]).expect("node id parses"),
            bucket: "shared".to_string(),
            prefix: "notes/".to_string(),
        };
        assert_eq!(binding.remote_key("a/b.txt"), "notes/a/b.txt");
        assert_eq!(
            binding.relative_path("notes/a/b.txt").as_deref(),
            Some("a/b.txt")
        );
        assert_eq!(binding.relative_path("other/b.txt"), None);
    }

    #[test]
    fn record_roundtrip() {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let folder = SyncedFolder {
            folder_id: Ulid::from_bytes([1u8; 16]),
            root: "/home/ada/data".to_string(),
            local_bucket: "data".to_string(),
            group_id: Ulid::from_bytes([2u8; 16]),
            remote: RemoteBinding {
                node_id: NodeId::from_bytes(&[3u8; 32]).expect("node id parses"),
                bucket: "shared".to_string(),
                prefix: String::new(),
            },
            mode: FolderMode::TwoWay,
            propagate_deletes: true,
            state: FolderState::Active,
            created_by: UserId::new(Ulid::from_bytes([4u8; 16]), realm_id),
            created_at_ms: 7,
            last_reconcile_ms: None,
        };
        assert_eq!(
            SyncedFolder::from_bytes(&folder.to_bytes().expect("folder encodes")).expect("decodes"),
            folder
        );
        let base = base("4-1", 9, Some(Ulid::from_bytes([1u8; 16])));
        assert_eq!(
            SyncBase::from_bytes(&base.to_bytes().expect("base encodes")).expect("decodes"),
            base
        );
    }

    #[test]
    fn rejects_wide_page() {
        // A peer may not answer with a page larger than the contract allows.
        let heads = vec![head(Ulid::from_bytes([1u8; 16]), None, false); MAX_SYNC_PAGE + 1];
        assert!(SyncVersionPage::new(heads.clone(), None).is_err());
        let parts = SyncPageParts {
            heads,
            next_cursor: None,
        };
        let encoded = postcard::to_allocvec(&parts).expect("parts encode");
        assert!(postcard::from_bytes::<SyncVersionPage>(&encoded).is_err());
        assert!(SyncVersionPage::new(Vec::new(), None).is_ok());
    }

    #[test]
    fn clamps_page_limit() {
        assert_eq!(SyncPageLimit::new(usize::MAX).get(), MAX_SYNC_PAGE);
        assert_eq!(SyncPageLimit::new(0).get(), 1);
        let over = postcard::to_allocvec(&(MAX_SYNC_PAGE + 1)).expect("limit encodes");
        assert!(postcard::from_bytes::<SyncPageLimit>(&over).is_err());
    }
}
