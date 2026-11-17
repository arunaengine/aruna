//! Merges one account into another after both proved a fresh login, in one transaction.
//! The merged account keeps its id for history, deactivated and pointing at the survivor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod changes;
pub mod handoff;
mod plan;
mod stage;
#[cfg(test)]
mod tests;

use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::auth::{REVOCATION_GRACE_SECS, user_cutoff_expiry, user_cutoff_hash};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::VaultQuery;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE, SUBJECT_CLAIMS_KEYSPACE, SUBJECT_INDEX_KEYSPACE,
    SYNC_REVISION_KEYSPACE,
};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, TransactionMode, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::rows::{PrefixPages, done};
use aruna_core::storage_entries::{reducer_state_key, subject_index_key, sync_revision_key};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Role};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
};
use aruna_core::structs::identity::user::User;
use aruna_core::structs::identity::user::vault::VaultRecords;
use aruna_core::types::{RoleId, Value};
use aruna_core::user::validation::UserAttributeError;
use aruna_core::{NodeId, UserId};
use byteview::ByteView;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tracing::info;

use self::stage::Batch;
use crate::auth::epoch_due::DueMarks;
use crate::auth::revoke_token::{
    Revocation, RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenError, RevokeTokenOperation,
};
use crate::sync::outbox_record::schedule_drain_effect;
use crate::users::vault_read::{ReadVaultConfig, ReadVaultOperation};
use aruna_core::link::fresh_local;

/// What the realm config records about the one node that runs account merges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coordinator {
    /// No node is recorded, as in a realm created before merges had a coordinator.
    Unpinned,
    /// The recorded node, a management node or one this node does not know yet; never treated
    /// as absent, since peers defer a replacement of it too.
    Pinned(NodeId),
    /// The recorded node is known here as removed or as no longer a management node.
    Left,
}

/// The merge coordinator the realm config names. Only that node hands the role on, so it stops
/// merging before another one starts.
pub fn coordinator(
    config: &RealmConfigDocument,
    state: Option<&AdminDocumentState>,
) -> Coordinator {
    let Some(pinned) = state.and_then(AdminDocumentState::materialized_merge_coordinator) else {
        return Coordinator::Unpinned;
    };
    let name = pinned.to_string();
    let kind = config
        .nodes
        .iter()
        .find(|node| node.node_id == name)
        .map(|node| &node.kind);
    let removed = state.is_some_and(|state| state.removed_config_nodes().contains(&pinned));
    match kind {
        Some(RealmNodeKind::Management) => Coordinator::Pinned(pinned),
        Some(_) => Coordinator::Left,
        None if removed => Coordinator::Left,
        None => Coordinator::Pinned(pinned),
    }
}

/// Group authorization documents read per scan page.
const SCAN_PAGE: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub struct MergeUsersConfig {
    /// This node, acting for the surviving account.
    pub actor: Actor,
    /// The caller's session: the account that survives.
    pub survivor: AuthContext,
    pub survivor_login: Option<u64>,
    /// The caller's token, forwarded to its vault holders when the merged account has a vault.
    pub survivor_token: Option<AuthToken>,
    /// The verified session of the account that is merged.
    pub merged: AuthContext,
    pub merged_login: Option<u64>,
    /// Whether the merged session passed revocation and cutoffs; a repeat may present a cut one.
    pub merged_live: bool,
    /// The merged session's token, forwarded to its vault holders.
    pub merged_token: Option<AuthToken>,
    pub vault_deadline: Duration,
    pub now: u64,
}

/// What a merge moved; all counts are zero when the request repeated a finished merge.
#[derive(Clone, Debug, PartialEq)]
pub struct MergeOutcome {
    pub survivor: UserId,
    pub merged: UserId,
    pub subjects: usize,
    pub groups: usize,
    pub realm_roles: usize,
    pub devices: usize,
    pub attributes: usize,
    /// Keys of attributes the survivor had no room for; they stay on the merged account.
    pub skipped_attributes: Vec<String>,
    /// Earlier secondaries of the merged account, now secondaries of the survivor.
    pub secondaries: usize,
    pub repeated: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum MergeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Admin(#[from] AdminDocumentError),
    #[error("both accounts need a portal login of this realm from the last 5 minutes")]
    StaleLogin,
    #[error("the token of the other account is no longer valid")]
    TokenInvalid,
    #[error("this node holds no record of one of the accounts")]
    NotFound,
    #[error("both tokens belong to the same account")]
    SameAccount,
    #[error("the accounts belong to different realms")]
    OtherRealm,
    #[error("one of the accounts is deactivated")]
    Inactive,
    #[error("service accounts are never merged")]
    ServiceAccount,
    #[error(transparent)]
    Attributes(#[from] UserAttributeError),
    #[error(
        "the role {0} of the other account denies part of what it hands over and part of this \
         account's own access; remove the deny or the role first"
    )]
    RoleConflict(String),
    #[error("the other account holds a vault that differs from this account's; copy it here first")]
    VaultPresent,
    #[error("the vault of the other account could not be read: {0}")]
    Vault(String),
    #[error("this node does not coordinate account merges; retry the request")]
    NotCoordinator,
    #[error("the caller may not name the merge coordinator")]
    Unauthorized,
    #[error("only a management node of this realm coordinates account merges")]
    InvalidCoordinator,
    #[error(transparent)]
    Authorization(#[from] aruna_core::errors::AuthorizationError),
    #[error("a document moved to new holders during the merge; retry it")]
    PlacementFenced,
    #[error("the credential cutoff of the merged account failed: {0}")]
    Cutoff(String),
    #[error("merge did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for MergeError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "Merge".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

/// Where a merge stands. The checks before the transaction run in the outer operation; the
/// transaction runs as a child of the same type in [`Phase::Write`].
#[derive(Debug, Clone, PartialEq)]
enum MergeState {
    ReadAccounts,
    CheckVault,
    /// The merged account has a vault; it may stay only as an unchanged copy of the survivor's.
    CheckCopy {
        merged: Vec<[u8; 32]>,
    },
    /// The outer operation waits for the merge transaction.
    Transaction,
    /// The cutoff of a repeat, alone in its own transaction.
    Cutoff,
    /// The cutoff of a merge, written first inside the merge transaction.
    JoinedCutoff,
    ReadDocuments,
    ScanGroups,
    ReadDetails,
    Write,
    DeleteStale,
    ReadFence,
    Marks,
}

/// Whether the operation runs the checks or the merge transaction.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Phase {
    Check,
    Write,
}

/// The documents the merge transaction read, before any change.
#[derive(Debug, PartialEq)]
struct Documents {
    survivor: User,
    merged: User,
    survivor_state: Option<Value>,
    merged_state: Option<Value>,
    survivor_revision: Option<Value>,
    merged_revision: Option<Value>,
    config: RealmConfigDocument,
    config_state: Option<Value>,
    realm_auth: Option<RealmAuthorizationDocument>,
    realm_state: Option<Value>,
    /// Accounts merged into the merged account earlier; they move to the survivor too.
    secondaries: Vec<Account>,
}

/// A stored user with its reducer state and sync revision.
#[derive(Debug, PartialEq)]
struct Account {
    user: User,
    state: Option<Value>,
    revision: Option<Value>,
}

/// Merges one account into another: checks both logins and the vault outside any transaction,
/// then cuts off the merged account's credentials and moves its documents in one transaction.
#[derive(Debug, PartialEq)]
pub struct MergeUsersOperation {
    config: MergeUsersConfig,
    phase: Phase,
    state: MergeState,
    documents: Option<Documents>,
    groups: Vec<GroupAuthorizationDocument>,
    /// Roles of groups only the survivor holds a role in; they only limit which denies move.
    survivor_groups: Vec<HashMap<RoleId, Role>>,
    batch: Batch,
    outcome: Option<MergeOutcome>,
    pages: PrefixPages,
    revocation: Option<Revocation>,
    marks: Option<DueMarks>,
}

type MergeTurn = Result<Next<MergeOutcome>, MergeError>;

impl MergeUsersOperation {
    pub fn new(config: MergeUsersConfig) -> Self {
        Self {
            config,
            phase: Phase::Check,
            state: MergeState::ReadAccounts,
            documents: None,
            groups: Vec::new(),
            survivor_groups: Vec::new(),
            batch: Batch::default(),
            outcome: None,
            pages: PrefixPages::new(AUTH_KEYSPACE, Vec::new(), SCAN_PAGE),
            revocation: None,
            marks: None,
        }
    }

    fn survivor_id(&self) -> UserId {
        self.config.survivor.user_id
    }

    fn merged_id(&self) -> UserId {
        self.config.merged.user_id
    }

    fn acting(&self, user_id: UserId) -> Actor {
        Actor {
            user_id,
            ..self.config.actor.clone()
        }
    }

    fn begin(&mut self) -> MergeTurn {
        let realm_id = self.config.actor.realm_id;
        if self.config.actor.user_id != self.survivor_id()
            || !fresh_local(
                &self.config.survivor,
                self.config.survivor_login,
                self.config.now,
            )
        {
            return Err(MergeError::StaleLogin);
        }
        if self.config.merged.realm_id != realm_id || self.merged_id().realm_id != realm_id {
            return Err(MergeError::OtherRealm);
        }
        self.state = MergeState::ReadAccounts;
        let reads = [self.survivor_id(), self.merged_id()]
            .iter()
            .map(user_read)
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn accept_accounts(&mut self, values: Vec<Option<Value>>) -> MergeTurn {
        let [survivor, merged] = parse_users(&values)?;
        if plan::check_accounts(&survivor, &merged)? {
            self.outcome = Some(self.empty_outcome(true));
            self.state = MergeState::Cutoff;
            let cutoff = RevokeTokenOperation::new(self.cutoff_config());
            return Ok(Next::Call(IoCall::child(cutoff)));
        }
        if !self.config.merged_live {
            return Err(MergeError::TokenInvalid);
        }
        if !fresh_local(
            &self.config.merged,
            self.config.merged_login,
            self.config.now,
        ) {
            return Err(MergeError::StaleLogin);
        }
        self.state = MergeState::CheckVault;
        let read = self.read_vault(self.merged_id(), self.config.merged_token.clone());
        Ok(Next::Call(read))
    }

    /// Reads the digests of a user's saved vault payloads from its holders.
    fn read_vault(&self, user_id: UserId, auth_token: Option<AuthToken>) -> IoCall {
        let read = ReadVaultOperation::new(ReadVaultConfig {
            node_id: self.config.actor.node_id,
            realm_id: self.config.actor.realm_id,
            user_id,
            query: VaultQuery::Heads { auth_token },
            deadline: self.config.vault_deadline,
        });
        IoCall::Child(boxed_suboperation(read, |result| {
            Event::SubOperation(SubOperationEvent::VaultHeld {
                result: result
                    .map(|records| match records {
                        VaultRecords::Heads(heads) => heads
                            .iter()
                            .filter_map(|head| head.payload.as_deref())
                            .map(|payload| *blake3::hash(payload.as_bytes()).as_bytes())
                            .collect(),
                        VaultRecords::Keys(_) => Vec::new(),
                    })
                    .map_err(|error| error.to_string()),
            })
        }))
    }

    /// Hands the merge itself to a child of the same type that runs in one write transaction.
    fn transaction(&mut self) -> Next<MergeOutcome> {
        self.state = MergeState::Transaction;
        let write = Self {
            phase: Phase::Write,
            ..Self::new(self.config.clone())
        };
        Next::Call(IoCall::child(write))
    }

    fn read_documents(&mut self) -> Next<MergeOutcome> {
        self.state = MergeState::ReadDocuments;
        let realm_id = self.config.actor.realm_id;
        let mut reads: Vec<_> = [self.survivor_id(), self.merged_id()]
            .iter()
            .map(user_read)
            .collect();
        for user_id in [self.survivor_id(), self.merged_id()] {
            reads.push(state_read(&AdminDocumentTarget::User { user_id }));
        }
        for user_id in [self.survivor_id(), self.merged_id()] {
            let document = DocumentTarget::User { user_id };
            reads.push((
                SYNC_REVISION_KEYSPACE.to_string(),
                sync_revision_key(&document),
            ));
        }
        let config = DocumentTarget::RealmConfig { realm_id };
        let realm_auth = DocumentTarget::RealmAuthorization { realm_id };
        reads.extend([
            (config.storage_keyspace().to_string(), config.storage_key()),
            state_read(&AdminDocumentTarget::RealmConfig { realm_id }),
            (
                realm_auth.storage_keyspace().to_string(),
                realm_auth.storage_key(),
            ),
            state_read(&AdminDocumentTarget::Realm { realm_id }),
        ]);
        Next::Call(IoCall::Read(reads))
    }

    fn accept_documents(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<MergeOutcome>, MergeError> {
        let [survivor, merged] = parse_users(&values[..2.min(values.len())])?;
        // A merge that finished meanwhile is reported by the next request as a repeat.
        if plan::check_accounts(&survivor, &merged)? {
            return Err(MergeError::Inactive);
        }
        let value = |index: usize| values.get(index).cloned().flatten();
        let config = value(6)
            .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
            .transpose()?
            .ok_or(MergeError::NotCoordinator)?;
        // One node merges at a time, so opposite merges of two accounts never both commit.
        let config_state = value(7)
            .map(|bytes| decode_reducer_state(&bytes).map_err(ConversionError::from))
            .transpose()?;
        if coordinator(&config, config_state.as_ref())
            != Coordinator::Pinned(self.config.actor.node_id)
        {
            return Err(MergeError::NotCoordinator);
        }
        let realm_auth = value(8)
            .map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes))
            .transpose()?;
        self.documents = Some(Documents {
            survivor,
            merged,
            survivor_state: value(2),
            merged_state: value(3),
            survivor_revision: value(4),
            merged_revision: value(5),
            config,
            config_state: value(7),
            realm_auth,
            realm_state: value(9),
            secondaries: Vec::new(),
        });
        self.state = MergeState::ScanGroups;
        Ok(Next::Call(self.pages.call()))
    }

    fn accept_groups(&mut self, result: IoResult) -> Result<Next<MergeOutcome>, MergeError> {
        let merged = self.merged_id();
        for (key, value) in self.pages.take(result)? {
            // The realm's own authorization document shares the keyspace under its longer key.
            if key.len() != 16 {
                continue;
            }
            let document = GroupAuthorizationDocument::from_bytes(&value)?;
            let holds = |user: UserId| {
                (document.roles.values()).any(|role| role.assigned_users.contains(&user))
            };
            if holds(merged) {
                self.groups.push(document);
            } else if holds(self.survivor_id()) {
                self.survivor_groups.push(document.roles);
            }
        }
        if !self.pages.done() {
            return Ok(Next::Call(self.pages.call()));
        }
        self.state = MergeState::ReadDetails;
        let mut reads: Vec<_> = self
            .groups
            .iter()
            .map(|group| {
                state_read(&AdminDocumentTarget::Group {
                    group_id: group.group_id,
                })
            })
            .collect();
        for user_id in self.secondary_ids() {
            reads.push(user_read(&user_id));
            reads.push(state_read(&AdminDocumentTarget::User { user_id }));
            reads.push((
                SYNC_REVISION_KEYSPACE.to_string(),
                sync_revision_key(&DocumentTarget::User { user_id }),
            ));
        }
        for subject_id in self.merged_subjects() {
            let key = subject_index_key(&subject_id);
            reads.push((SUBJECT_CLAIMS_KEYSPACE.to_string(), key.clone()));
            reads.push((SUBJECT_INDEX_KEYSPACE.to_string(), key));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn secondary_ids(&self) -> Vec<UserId> {
        self.documents
            .as_ref()
            .map(|documents| documents.merged.merged_user_ids())
            .unwrap_or_default()
    }

    fn merged_subjects(&self) -> Vec<String> {
        self.documents
            .as_ref()
            .map(|documents| documents.merged.subject_ids.clone())
            .unwrap_or_default()
    }

    fn empty_outcome(&self, repeated: bool) -> MergeOutcome {
        MergeOutcome {
            survivor: self.survivor_id(),
            merged: self.merged_id(),
            subjects: 0,
            groups: 0,
            realm_roles: 0,
            devices: 0,
            attributes: 0,
            skipped_attributes: Vec::new(),
            secondaries: 0,
            repeated,
        }
    }

    fn after_write(&mut self) -> MergeTurn {
        if !self.batch.deletes.is_empty() {
            self.state = MergeState::DeleteStale;
            return Ok(Next::Call(IoCall::Delete(std::mem::take(
                &mut self.batch.deletes,
            ))));
        }
        Ok(self.read_fence())
    }

    /// Takes the buckets' fences inside the transaction, so a departing holder's close
    /// rejects or conflicts the merge.
    fn read_fence(&mut self) -> Next<MergeOutcome> {
        if self.batch.fence.is_empty() {
            return self.mark();
        }
        self.state = MergeState::ReadFence;
        Next::Call(IoCall::Read(self.batch.fence.reads()))
    }

    /// The merged account loses READ everywhere, so encrypted buckets are marked for a new epoch.
    fn mark(&mut self) -> Next<MergeOutcome> {
        self.state = MergeState::Marks;
        let marks = DueMarks::new(None, aruna_core::time::unix_timestamp_millis());
        let first = marks.first();
        self.marks = Some(marks);
        Next::Call(first)
    }

    /// The cutoff of every credential of the merged account issued before the merge.
    fn cutoff_config(&self) -> RevokeTokenConfig {
        let cutoff = self.config.now.saturating_add(REVOCATION_GRACE_SECS);
        let merged = self.merged_id();
        RevokeTokenConfig {
            actor: self.acting(merged),
            token_hash: user_cutoff_hash(&merged),
            expires_at: user_cutoff_expiry(cutoff),
            token_owner: merged,
            admission: RevokeTokenAdmission::Privileged,
            now: self.config.now,
        }
    }

    /// Wakes the outbox drain after the commit; a failed timer leaves the outbox retryable.
    fn committed(&mut self) -> MergeTurn {
        let output = self.outcome.take().ok_or(MergeError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }

    fn finish(&mut self, outcome: MergeOutcome) -> MergeTurn {
        info!(
            survivor = %outcome.survivor,
            merged = %outcome.merged,
            subjects = outcome.subjects,
            groups = outcome.groups,
            realm_roles = outcome.realm_roles,
            devices = outcome.devices,
            secondaries = outcome.secondaries,
            repeated = outcome.repeated,
            "Merged account into surviving account"
        );
        Ok(Next::Done(outcome))
    }

    fn vault_held(event: Box<Event>) -> Result<Vec<[u8; 32]>, MergeError> {
        match *event {
            Event::SubOperation(SubOperationEvent::VaultHeld { result }) => {
                result.map_err(MergeError::Vault)
            }
            other => Err(MergeError::UnexpectedEvent {
                state: "CheckVault".to_string(),
                expected: "the vault read result",
                got: other.label(),
            }),
        }
    }

    fn check_step(&mut self, result: IoResult) -> MergeTurn {
        match (self.state.clone(), result) {
            (MergeState::ReadAccounts, IoResult::Values(values)) => self.accept_accounts(values),
            (MergeState::CheckVault, IoResult::Event(event)) => {
                let merged = Self::vault_held(event)?;
                if merged.is_empty() {
                    return Ok(self.transaction());
                }
                self.state = MergeState::CheckCopy { merged };
                let token = self.config.survivor_token.clone();
                Ok(Next::Call(self.read_vault(self.survivor_id(), token)))
            }
            (MergeState::CheckCopy { merged }, IoResult::Event(event)) => {
                let held = Self::vault_held(event)?;
                if !merged.iter().all(|digest| held.contains(digest)) {
                    return Err(MergeError::VaultPresent);
                }
                Ok(self.transaction())
            }
            (MergeState::Cutoff, result) => {
                result
                    .child::<RealmConfigDocument, RevokeTokenError>()?
                    .map_err(|error| MergeError::Cutoff(error.to_string()))?;
                let outcome = self.outcome.take().ok_or(MergeError::NotFinished)?;
                self.finish(outcome)
            }
            (MergeState::Transaction, result) => {
                let outcome = result.child::<MergeOutcome, MergeError>()??;
                self.finish(outcome)
            }
            (_, result) => Err(MergeError::UnexpectedEvent {
                state: format!("{:?}", self.state),
                expected: "the result of the current merge step",
                got: result.label(),
            }),
        }
    }

    fn write_step(&mut self, result: IoResult) -> MergeTurn {
        match (self.state.clone(), result) {
            (MergeState::JoinedCutoff, result) => {
                let revocation = self.revocation.as_mut().ok_or(MergeError::NotFinished)?;
                let cutoff = |error: RevokeTokenError| MergeError::Cutoff(error.to_string());
                if let Some(call) = revocation.next(result).map_err(cutoff)? {
                    return Ok(Next::Call(call));
                }
                revocation
                    .outcome()
                    .and_then(|revoked| revoked.result())
                    .map_err(cutoff)?;
                Ok(self.read_documents())
            }
            (MergeState::ReadDocuments, IoResult::Values(values)) => self.accept_documents(values),
            (MergeState::ScanGroups, result) => self.accept_groups(result),
            (MergeState::ReadDetails, IoResult::Values(values)) => self.stage_all(values),
            (MergeState::Write, result) => {
                done(result)?;
                self.after_write()
            }
            (MergeState::DeleteStale, result) => {
                done(result)?;
                Ok(self.read_fence())
            }
            (MergeState::ReadFence, IoResult::Values(values)) => {
                let keys = self.batch.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.batch.fence.admits(&rows) {
                    return Err(MergeError::PlacementFenced);
                }
                Ok(self.mark())
            }
            (MergeState::Marks, result) => {
                let marks = self.marks.as_mut().ok_or(MergeError::NotFinished)?;
                match marks.next(result, true) {
                    Ok(Some(call)) => Ok(Next::Call(call)),
                    Ok(None) => self.committed(),
                    Err(_) => Err(StorageError::WriteError("epoch marking".to_string()).into()),
                }
            }
            (_, result) => Err(MergeError::UnexpectedEvent {
                state: format!("{:?}", self.state),
                expected: "the result of the current merge step",
                got: result.label(),
            }),
        }
    }
}

fn user_read(user_id: &UserId) -> (String, ByteView) {
    let document = DocumentTarget::User { user_id: *user_id };
    (
        document.storage_keyspace().to_string(),
        document.storage_key(),
    )
}

fn state_read(target: &AdminDocumentTarget) -> (String, ByteView) {
    (
        DOCUMENT_STATE_KEYSPACE.to_string(),
        reducer_state_key(target),
    )
}

fn parse_users(values: &[Option<Value>]) -> Result<[User; 2], MergeError> {
    let [survivor, merged] = values else {
        return Err(MergeError::NotFound);
    };
    let parse = |value: &Option<Value>| -> Result<User, MergeError> {
        User::from_bytes(value.as_ref().ok_or(MergeError::NotFound)?).map_err(Into::into)
    };
    Ok([parse(survivor)?, parse(merged)?])
}

impl Operation for MergeUsersOperation {
    type Output = MergeOutcome;
    type Error = MergeError;

    fn transaction(&self) -> TransactionMode {
        match self.phase {
            Phase::Check => TransactionMode::None,
            Phase::Write => TransactionMode::Write,
        }
    }

    /// Inside the transaction the cutoff comes first, so no merge ever lands without it.
    fn first(&mut self) -> MergeTurn {
        if self.phase == Phase::Check {
            return self.begin();
        }
        self.outcome = None;
        self.state = MergeState::JoinedCutoff;
        let revocation = Revocation::new(self.cutoff_config());
        let first = revocation.first();
        self.revocation = Some(revocation);
        Ok(Next::Call(first))
    }

    fn next(&mut self, result: IoResult) -> MergeTurn {
        match self.phase {
            Phase::Check => self.check_step(result),
            Phase::Write => self.write_step(result),
        }
    }
}
