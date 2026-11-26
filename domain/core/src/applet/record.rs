//! The applet record: lifecycle, access, versions, approved grants, hosts and the approval queue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::structs::identity::auth::Permission;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::blob::{bucket_permission_path, object_permission_path};
use crate::types::{GroupId, Key, RoleId};
use crate::user::id::UserId;

/// Approved versions kept for rollback besides the live one.
pub const KEPT_APPROVED: usize = 3;
/// Private versions kept per applet; the oldest draft goes first.
pub const KEPT_DRAFTS: usize = 5;

/// Where an applet is in its life. The feature switch never changes it: a `Live` applet whose
/// feature is off serves as paused and comes back by itself (see [`Applet::serving`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lifecycle {
    Private,
    WaitingForApproval,
    Live,
    /// Paused by its owner; only an owner resumes it.
    Paused,
    TakenDown,
}

/// Who may open an applet once it is live.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Access {
    Private,
    GroupMembers,
    Public,
}

/// What the edge does for a host of an applet right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Serving {
    Live {
        version: u32,
    },
    /// Only the creator and approvers see the newest private version.
    Preview {
        version: u32,
    },
    FeaturePaused,
    Paused,
    Gone,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersionState {
    Private,
    Approved,
    TakenDown,
}

/// One pinned object in the held deployment area.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedObject {
    pub node: NodeId,
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub blake3: String,
}

/// A version as the record lists it; its files live in the manifest object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRef {
    pub version: u32,
    /// The Git commit it was built or released from, if any.
    pub commit: Option<String>,
    pub manifest: PinnedObject,
    pub state: VersionState,
    pub created_by: UserId,
    pub created_at_ms: u64,
    pub approved_by: Option<UserId>,
}

/// A build preview: a builder job's output, shown only to previewers until it is kept or expires.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Draft {
    /// Names the held bucket folder of the build's sources and outputs.
    pub build_id: Ulid,
    pub job_id: Ulid,
    pub commit: String,
    pub started_by: UserId,
    pub started_at_ms: u64,
    pub expires_at_ms: u64,
    /// The draft manifest, once the build's outputs are captured.
    pub manifest: Option<PinnedObject>,
    pub failed: Option<String>,
}

/// What an applet starts on behalf of its visitors.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StartGrant {
    Tool { tool_id: String, version: String },
    Workflow { workflow_id: Ulid, version: u32 },
}

/// One verified visitor upload attached to an applet form submission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionFile {
    pub name: String,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
}

/// Stored receipt of a visitor's form submission and its verified upload pins.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionReceipt {
    pub applet_id: Ulid,
    pub version_id: Ulid,
    pub pseudonym: String,
    pub guest: bool,
    pub bucket: String,
    pub key: String,
    pub files: Vec<SubmissionFile>,
    pub created_at_ms: u64,
}

impl SubmissionReceipt {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Row prefix of stored applet submission receipts in `APPLETS_KEYSPACE`.
pub const SUBMISSION_ROW: u8 = 4;

/// Key of one applet submission receipt in `APPLETS_KEYSPACE`.
pub fn submission_key(applet_id: Ulid, version_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(33);
    row.push(SUBMISSION_ROW);
    row.extend_from_slice(&applet_id.to_bytes());
    row.extend_from_slice(&version_id.to_bytes());
    Key::from(row)
}

/// A folder of the group: a bucket and a key prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Folder {
    pub bucket: String,
    pub prefix: String,
}

impl Folder {
    /// Permission-path root matching `intake_key` and `visitor_folder` (trims trailing `/` only).
    pub fn permission_root(&self, realm_id: RealmId, group_id: GroupId, node: NodeId) -> String {
        match self.prefix.trim_end_matches('/') {
            "" => bucket_permission_path(realm_id, group_id, node, &self.bucket),
            prefix => object_permission_path(realm_id, group_id, node, &self.bucket, prefix),
        }
    }
}

/// Bounds of one run and of the run counts an applet may start.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub cpu_cores: Option<u32>,
    pub ram_bytes: Option<u64>,
    pub walltime_ms: Option<u64>,
    pub upload_bytes: Option<u64>,
    pub visitor_hourly: Option<u32>,
    pub visitor_daily: Option<u32>,
    pub applet_daily: Option<u32>,
    #[serde(default)]
    pub active_runs: Option<u32>,
}

/// What an approver granted. Grants never come from code; only an approval changes them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grants {
    /// Permission path patterns inside the group that the applet reads.
    pub read: Vec<String>,
    pub references: Vec<String>,
    pub start: Option<StartGrant>,
    /// Where submissions go; each visitor writes below its own pseudonym folder.
    pub submissions: Option<Folder>,
    pub limits: Limits,
}

impl Grants {
    /// Whether the applet reads, starts and receives nothing, so it needs no account.
    pub fn is_empty(&self) -> bool {
        self.read.is_empty()
            && self.references.is_empty()
            && self.start.is_none()
            && self.submissions.is_none()
    }
}

/// Exact `https://` parent origins allowed to frame the applet.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Embedding {
    pub enabled: bool,
    pub origins: Vec<String>,
}

/// Submissions without an account, switched on by a group admin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestPolicy {
    pub enabled_by: UserId,
    pub in_frames: bool,
    pub cpu_cores: u32,
    pub ram_bytes: u64,
    pub walltime_ms: u64,
    pub file_bytes: u64,
    pub hourly: u32,
    pub daily: u32,
    pub expiry_days: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainState {
    Pending,
    Active,
    Suspended,
}

/// A custom subdomain that points to the applet host.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomDomain {
    pub host: String,
    /// 128 bit random value the `_aruna-verify` TXT record must carry, as hex.
    pub nonce: String,
    pub state: DomainState,
    pub failures: u8,
    pub checked_at_ms: u64,
}

impl CustomDomain {
    /// Applies one check; two failures in a row suspend the host. Answers whether it changed.
    pub fn record_check(&mut self, passed: bool, now_ms: u64) -> bool {
        let before = (self.state, self.failures);
        self.checked_at_ms = now_ms;
        if passed {
            self.state = DomainState::Active;
            self.failures = 0;
        } else {
            self.failures = self.failures.saturating_add(1);
            if self.failures >= 2 && self.state != DomainState::Pending {
                self.state = DomainState::Suspended;
            }
        }
        before != (self.state, self.failures)
    }
}

/// A change that waits for a person's approval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub version: u32,
    pub access: Access,
    pub grants: Grants,
    pub origins: Vec<String>,
    pub requested_by: UserId,
    pub requested_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Applet {
    pub applet_id: Ulid,
    pub group_id: GroupId,
    pub name: String,
    pub slug: String,
    /// The applet's own host below the node's applet domain.
    pub host: String,
    /// The metadata document whose Git repository holds the source, if it has one.
    pub repository: Option<Ulid>,
    /// The node that decides about the applet and keeps its held files.
    pub host_node: NodeId,
    pub lifecycle: Lifecycle,
    pub access: Access,
    pub live: Option<u32>,
    pub versions: Vec<VersionRef>,
    pub drafts: Vec<Draft>,
    pub grants: Grants,
    pub embedding: Embedding,
    pub guests: Option<GuestPolicy>,
    pub domains: Vec<CustomDomain>,
    pub pending: Option<ApprovalRequest>,
    /// The node-managed account the applet acts as, made at the first approval with grants.
    pub account: Option<UserId>,
    pub role_id: Option<RoleId>,
    /// Key of the visitor pseudonyms, so a visitor finds only their own submissions.
    pub visitor_salt: [u8; 32],
    pub created_by: UserId,
    pub created_at_ms: u64,
    /// Raised by every write of the record.
    pub revision: u64,
}

/// Why a lifecycle change is refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TransitionError {
    #[error("the applet is taken down")]
    TakenDown,
    #[error("no such version")]
    NoVersion,
    #[error("the version is not approved or was taken down")]
    NotApproved,
    #[error("nothing waits for approval of that version")]
    NotPending,
    #[error("the applet is not live")]
    NotLive,
    #[error("the applet is not paused by its owner")]
    NotPaused,
}

impl Applet {
    /// What the edge serves now: the owner's pause and a takedown win, and a switched off
    /// feature or hosting pauses only a live applet without changing its lifecycle.
    pub fn serving(&self, feature_on: bool, hosting_on: bool) -> Serving {
        match (self.lifecycle, self.live) {
            (Lifecycle::TakenDown, _) => Serving::Gone,
            (Lifecycle::Paused, _) => Serving::Paused,
            _ if !hosting_on => Serving::Gone,
            (Lifecycle::Live, Some(_)) if !feature_on => Serving::FeaturePaused,
            (Lifecycle::Live, Some(version)) => Serving::Live { version },
            _ if !feature_on => Serving::FeaturePaused,
            _ => match self.newest_draft() {
                Some(version) => Serving::Preview { version },
                None => Serving::Gone,
            },
        }
    }

    pub fn version(&self, version: u32) -> Option<&VersionRef> {
        self.versions.iter().find(|entry| entry.version == version)
    }

    pub fn draft(&self, build_id: Ulid) -> Option<&Draft> {
        self.drafts.iter().find(|draft| draft.build_id == build_id)
    }

    /// How many more drafts fit below `max`, counting running builds.
    pub fn draft_room(&self, max: usize) -> usize {
        max.saturating_sub(self.drafts.len())
    }

    /// Drops the drafts whose lifetime ended at `now_ms` and answers them.
    pub fn expire_drafts(&mut self, now_ms: u64) -> Vec<Draft> {
        let (expired, kept) = std::mem::take(&mut self.drafts)
            .into_iter()
            .partition(|draft| draft.expires_at_ms <= now_ms);
        self.drafts = kept;
        expired
    }

    /// The number the next version gets.
    pub fn next_version(&self) -> u32 {
        self.versions
            .iter()
            .map(|entry| entry.version)
            .max()
            .unwrap_or(0)
            + 1
    }

    fn newest_draft(&self) -> Option<u32> {
        (self.versions.iter())
            .filter(|entry| entry.state == VersionState::Private)
            .map(|entry| entry.version)
            .max()
    }

    fn alive(&self) -> Result<(), TransitionError> {
        match self.lifecycle {
            Lifecycle::TakenDown => Err(TransitionError::TakenDown),
            _ => Ok(()),
        }
    }

    /// Queues `request`; an applet that never went live waits as a whole.
    pub fn request_approval(&mut self, request: ApprovalRequest) -> Result<(), TransitionError> {
        self.alive()?;
        let entry = self
            .version(request.version)
            .ok_or(TransitionError::NoVersion)?;
        if entry.state == VersionState::TakenDown {
            return Err(TransitionError::NotApproved);
        }
        if self.lifecycle == Lifecycle::Private {
            self.lifecycle = Lifecycle::WaitingForApproval;
        }
        self.pending = Some(request);
        Ok(())
    }

    /// Applies the pending change for `version`: the version goes live with its access, grants
    /// and origins. A paused applet stays paused.
    pub fn approve(&mut self, version: u32, approver: UserId) -> Result<(), TransitionError> {
        self.alive()?;
        let request = (self.pending.take())
            .filter(|request| request.version == version)
            .ok_or(TransitionError::NotPending)?;
        let entry = (self.versions.iter_mut())
            .find(|entry| entry.version == version)
            .ok_or(TransitionError::NoVersion)?;
        entry.state = VersionState::Approved;
        entry.approved_by = Some(approver);
        self.live = Some(version);
        self.access = request.access;
        self.grants = request.grants;
        self.embedding.origins = request.origins;
        if self.lifecycle != Lifecycle::Paused {
            self.lifecycle = Lifecycle::Live;
        }
        Ok(())
    }

    /// Drops the pending change; an applet that never went live is private again.
    pub fn reject(&mut self) -> Result<(), TransitionError> {
        self.pending.take().ok_or(TransitionError::NotPending)?;
        if self.lifecycle == Lifecycle::WaitingForApproval {
            self.lifecycle = Lifecycle::Private;
        }
        Ok(())
    }

    /// Serves an earlier approved version that was not taken down.
    pub fn rollback(&mut self, version: u32) -> Result<(), TransitionError> {
        self.alive()?;
        let entry = self.version(version).ok_or(TransitionError::NoVersion)?;
        if entry.state != VersionState::Approved {
            return Err(TransitionError::NotApproved);
        }
        self.live = Some(version);
        Ok(())
    }

    pub fn pause(&mut self) -> Result<(), TransitionError> {
        self.alive()?;
        if self.lifecycle != Lifecycle::Live {
            return Err(TransitionError::NotLive);
        }
        self.lifecycle = Lifecycle::Paused;
        Ok(())
    }

    pub fn resume(&mut self) -> Result<(), TransitionError> {
        if self.lifecycle != Lifecycle::Paused {
            return Err(TransitionError::NotPaused);
        }
        self.lifecycle = Lifecycle::Live;
        Ok(())
    }

    /// Takes the applet down for good, or only `version` when one is named.
    pub fn take_down(&mut self, version: Option<u32>) -> Result<(), TransitionError> {
        let Some(version) = version else {
            self.lifecycle = Lifecycle::TakenDown;
            self.pending = None;
            return Ok(());
        };
        let entry = (self.versions.iter_mut())
            .find(|entry| entry.version == version)
            .ok_or(TransitionError::NoVersion)?;
        entry.state = VersionState::TakenDown;
        if self.live == Some(version) {
            self.live = None;
            self.lifecycle = Lifecycle::Paused;
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.version == version)
        {
            self.pending = None;
        }
        Ok(())
    }

    /// Drops versions beyond the live one, [`KEPT_APPROVED`] approved and [`KEPT_DRAFTS`]
    /// private ones, newest first; a pending version is kept. Answers the dropped versions.
    pub fn prune(&mut self) -> Vec<VersionRef> {
        let pending = self.pending.as_ref().map(|request| request.version);
        let mut ordered = std::mem::take(&mut self.versions);
        ordered.sort_by_key(|entry| std::cmp::Reverse(entry.version));
        let (mut approved, mut drafts) = (0, 0);
        let mut dropped = Vec::new();
        for entry in ordered {
            let keep = match entry.state {
                _ if Some(entry.version) == self.live || Some(entry.version) == pending => true,
                VersionState::Approved => {
                    approved += 1;
                    approved <= KEPT_APPROVED
                }
                VersionState::Private => {
                    drafts += 1;
                    drafts <= KEPT_DRAFTS
                }
                VersionState::TakenDown => false,
            };
            match keep {
                true => self.versions.push(entry),
                false => dropped.push(entry),
            }
        }
        self.versions.reverse();
        dropped
    }

    /// The paths the applet's own role holds: READ on what it reads, WRITE below the submission
    /// folder and, only with a granted start, WRITE on the exact group path, which admits a run
    /// but grants nothing below it. The account has no credential; only the node acts as it.
    pub fn role_paths(&self, realm_id: RealmId, node: NodeId) -> Vec<(String, Permission)> {
        let grants = &self.grants;
        let mut paths: Vec<(String, Permission)> = (grants.read.iter())
            .chain(&grants.references)
            .map(|pattern| (pattern.clone(), Permission::READ))
            .collect();
        if let Some(folder) = &grants.submissions {
            let root = folder.permission_root(realm_id, self.group_id, node);
            let escaped = crate::permission_path::escape_permission_path(&root);
            paths.push((format!("{escaped}/**"), Permission::WRITE));
        }
        if grants.start.is_some() {
            let path = crate::structs::execution::workflow::workflow_permission_path(
                realm_id,
                self.group_id,
            );
            paths.push((path, Permission::WRITE));
        }
        paths
    }

    /// Drops expired drafts and the versions [`Applet::prune`] drops; answers whether anything
    /// went.
    pub fn retain(&mut self, now_ms: u64) -> bool {
        let expired = !self.expire_drafts(now_ms).is_empty();
        let pruned = !self.prune().is_empty();
        expired || pruned
    }

    /// Whether the applet's account should be active with its role: a live applet with grants.
    pub fn wants_account(&self) -> bool {
        self.lifecycle == Lifecycle::Live && !self.grants.is_empty()
    }

    /// Who the applet's account and role act for: the approver of the live version, else the
    /// approver of the newest approved version.
    pub fn granter(&self) -> Option<UserId> {
        let live = self.live.and_then(|live| self.version(live));
        let newest = (self.versions.iter())
            .filter(|entry| entry.approved_by.is_some())
            .max_by_key(|entry| entry.version);
        live.or(newest).and_then(|entry| entry.approved_by)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// The bucket of `group_id` on `node` that holds the pinned files of every applet version: one
/// per group and node, owned by the group, and never pruned by run-area retention.
pub fn held_bucket(group_id: GroupId, node: NodeId) -> String {
    let node = hex::encode(&node.as_bytes()[..6]);
    format!("applets-{}-{node}", group_id.to_string().to_lowercase())
}

const RECORD_ROW: u8 = 0;
const GROUP_ROW: u8 = 1;

/// The row of an applet's record in `APPLETS_KEYSPACE`.
pub fn applet_key(applet_id: Ulid) -> Key {
    let mut row = vec![RECORD_ROW];
    row.extend_from_slice(&applet_id.to_bytes());
    Key::from(row)
}

/// The prefix of every applet record row.
pub fn record_prefix() -> Key {
    Key::from(vec![RECORD_ROW])
}

/// The group index row that lists `applet_id` under `group_id`; its value is empty.
pub fn group_key(group_id: GroupId, applet_id: Ulid) -> Key {
    let mut row = vec![GROUP_ROW];
    row.extend_from_slice(&group_id.to_bytes());
    row.extend_from_slice(&applet_id.to_bytes());
    Key::from(row)
}

/// The prefix of every group index row of `group_id`.
pub fn group_prefix(group_id: GroupId) -> Key {
    let mut row = vec![GROUP_ROW];
    row.extend_from_slice(&group_id.to_bytes());
    Key::from(row)
}

/// A visitor's folder name: a keyed hash of the account, so names never reveal who it is.
pub fn pseudonym(salt: &[u8; 32], user: &UserId) -> String {
    let hash = blake3::keyed_hash(salt, &user.to_bytes());
    hex::encode(&hash.as_bytes()[..16])
}

/// A guest's folder name from the id the edge gave it, keyed like [`pseudonym`].
pub fn guest_pseudonym(salt: &[u8; 32], guest: &str) -> String {
    let mut hasher = blake3::Hasher::new_keyed(salt);
    hasher.update(b"guest:");
    hasher.update(guest.as_bytes());
    hex::encode(&hasher.finalize().as_bytes()[..16])
}

#[cfg(test)]
#[path = "record_tests.rs"]
pub(crate) mod tests;
