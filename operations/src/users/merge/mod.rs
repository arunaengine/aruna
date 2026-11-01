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
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect, VaultQuery};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE, SUBJECT_CLAIMS_KEYSPACE, SUBJECT_INDEX_KEYSPACE,
    SYNC_REVISION_KEYSPACE,
};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::storage_entries::{reducer_state_key, subject_index_key, sync_revision_key};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Role};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
};
use aruna_core::structs::identity::user::User;
use aruna_core::structs::identity::user::vault::VaultRecords;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, RoleId, TxnId, Value};
use aruna_core::user::validation::UserAttributeError;
use aruna_core::{NodeId, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tracing::info;

use self::stage::Batch;
use crate::auth::revoke_token::{RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenOperation};
use crate::federation::link::fresh_local;
use crate::sync::document_outbox::schedule_drain_effect;
use crate::users::vault_read::{ReadVaultConfig, ReadVaultOperation};

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
        got: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum MergeState {
    Init,
    ReadAccounts,
    CheckVault,
    /// The merged account has a vault; it may stay only as an unchanged copy of the survivor's.
    CheckCopy {
        merged: Vec<[u8; 32]>,
    },
    StartTransaction,
    ReadDocuments {
        txn_id: TxnId,
    },
    ScanGroups {
        txn_id: TxnId,
    },
    ReadDetails {
        txn_id: TxnId,
    },
    Write {
        txn_id: TxnId,
    },
    DeleteStale {
        txn_id: TxnId,
    },
    ReadFence {
        txn_id: TxnId,
    },
    Commit {
        txn_id: TxnId,
    },
    ScheduleDrain,
    /// The cutoff of a repeat, alone in its own transaction.
    Cutoff,
    /// The cutoff of a merge, written first inside the merge transaction.
    JoinedCutoff {
        txn_id: TxnId,
    },
    Finish,
    Error,
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

#[derive(Debug, PartialEq)]
pub struct MergeUsersOperation {
    config: MergeUsersConfig,
    state: MergeState,
    documents: Option<Documents>,
    groups: Vec<GroupAuthorizationDocument>,
    /// Roles of groups only the survivor holds a role in; they only limit which denies move.
    survivor_groups: Vec<HashMap<RoleId, Role>>,
    batch: Batch,
    outcome: Option<MergeOutcome>,
    output: Option<Result<MergeOutcome, MergeError>>,
}

impl MergeUsersOperation {
    pub fn new(config: MergeUsersConfig) -> Self {
        Self {
            config,
            state: MergeState::Init,
            documents: None,
            groups: Vec::new(),
            survivor_groups: Vec::new(),
            batch: Batch::default(),
            outcome: None,
            output: None,
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

    fn fail(&mut self, error: MergeError) -> Effects {
        let cleanup = self.abort();
        self.state = MergeState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        self.fail(MergeError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        })
    }

    fn begin(&mut self) -> Result<Effects, MergeError> {
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
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: [self.survivor_id(), self.merged_id()]
                .iter()
                .map(user_read)
                .collect(),
            txn_id: None,
        })])
    }

    fn accept_accounts(
        &mut self,
        values: &[(ByteView, Option<Value>)],
    ) -> Result<Effects, MergeError> {
        let [survivor, merged] = parse_users(values)?;
        if plan::check_accounts(&survivor, &merged)? {
            self.outcome = Some(self.empty_outcome(true));
            return Ok(self.cutoff(None));
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
        Ok(smallvec![self.read_vault(
            self.merged_id(),
            self.config.merged_token.clone()
        )])
    }

    /// Reads the digests of a user's saved vault payloads from its holders.
    fn read_vault(&self, user_id: UserId, auth_token: Option<AuthToken>) -> Effect {
        let read = ReadVaultOperation::new(ReadVaultConfig {
            node_id: self.config.actor.node_id,
            realm_id: self.config.actor.realm_id,
            user_id,
            query: VaultQuery::Heads { auth_token },
            deadline: self.config.vault_deadline,
        });
        Effect::SubOperation(boxed_suboperation(read, |result| {
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

    fn start_transaction(&mut self) -> Effects {
        self.state = MergeState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn read_documents(&mut self, txn_id: TxnId) -> Effects {
        self.state = MergeState::ReadDocuments { txn_id };
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
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    fn accept_documents(
        &mut self,
        txn_id: TxnId,
        values: Vec<(ByteView, Option<Value>)>,
    ) -> Result<Effects, MergeError> {
        let [survivor, merged] = parse_users(&values[..2.min(values.len())])?;
        // A merge that finished meanwhile is reported by the next request as a repeat.
        if plan::check_accounts(&survivor, &merged)? {
            return Err(MergeError::Inactive);
        }
        let value = |index: usize| values.get(index).and_then(|(_, value)| value.clone());
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
        Ok(self.scan_groups(txn_id, None))
    }

    fn scan_groups(&mut self, txn_id: TxnId, after: Option<ByteView>) -> Effects {
        self.state = MergeState::ScanGroups { txn_id };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: AUTH_KEYSPACE.to_string(),
            prefix: None,
            start: after.map(IterStart::After),
            limit: SCAN_PAGE,
            txn_id: Some(txn_id),
        })]
    }

    fn accept_groups(
        &mut self,
        txn_id: TxnId,
        values: Vec<(ByteView, Value)>,
        next: Option<ByteView>,
    ) -> Result<Effects, MergeError> {
        let merged = self.merged_id();
        for (key, value) in values {
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
        if let Some(after) = next {
            return Ok(self.scan_groups(txn_id, Some(after)));
        }
        self.state = MergeState::ReadDetails { txn_id };
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
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })])
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

    fn after_write(&mut self, txn_id: TxnId) -> Effects {
        if !self.batch.deletes.is_empty() {
            self.state = MergeState::DeleteStale { txn_id };
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: std::mem::take(&mut self.batch.deletes),
                txn_id: Some(txn_id),
            })];
        }
        self.read_fence(txn_id)
    }

    /// Takes the buckets' fences inside the transaction, so a departing holder's close
    /// rejects or conflicts the merge.
    fn read_fence(&mut self, txn_id: TxnId) -> Effects {
        if self.batch.fence.is_empty() {
            return self.commit(txn_id);
        }
        self.state = MergeState::ReadFence { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: self.batch.fence.reads(),
            txn_id: Some(txn_id),
        })]
    }

    /// The merged account loses READ everywhere, so encrypted buckets are marked for a new epoch.
    fn commit(&mut self, txn_id: TxnId) -> Effects {
        self.state = MergeState::Commit { txn_id };
        smallvec![crate::abe::mark_due(None, txn_id)]
    }

    /// Cuts off every credential of the merged account issued before the merge, on every node.
    /// Inside `txn_id` it commits with the merge, so no merge ever lands without it.
    fn cutoff(&mut self, txn_id: Option<TxnId>) -> Effects {
        let cutoff = self.config.now.saturating_add(REVOCATION_GRACE_SECS);
        let merged = self.merged_id();
        let config = RevokeTokenConfig {
            actor: self.acting(merged),
            token_hash: user_cutoff_hash(&merged),
            expires_at: user_cutoff_expiry(cutoff),
            token_owner: merged,
            admission: RevokeTokenAdmission::Privileged,
            now: self.config.now,
        };
        let operation = match txn_id {
            Some(txn_id) => {
                self.state = MergeState::JoinedCutoff { txn_id };
                RevokeTokenOperation::joined(config, txn_id)
            }
            None => {
                self.state = MergeState::Cutoff;
                RevokeTokenOperation::new(config)
            }
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            operation,
            |result| Event::SubOperation(SubOperationEvent::TokenRevoked {
                result: result.map(|_| ()).map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn finish(&mut self) -> Effects {
        let outcome = self
            .outcome
            .take()
            .unwrap_or_else(|| self.empty_outcome(true));
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
        self.state = MergeState::Finish;
        self.output = Some(Ok(outcome));
        smallvec![]
    }

    fn step_storage(&mut self, event: Event) -> Result<Effects, MergeError> {
        match (self.state.clone(), event) {
            (
                MergeState::ReadAccounts,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => self.accept_accounts(&values),
            (
                MergeState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => Ok(self.cutoff(Some(txn_id))),
            (
                MergeState::JoinedCutoff { txn_id },
                Event::SubOperation(SubOperationEvent::TokenRevoked { result }),
            ) => match result {
                Ok(()) => Ok(self.read_documents(txn_id)),
                Err(error) => Err(MergeError::Cutoff(error)),
            },
            (
                MergeState::ReadDocuments { txn_id },
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => self.accept_documents(txn_id, values),
            (
                MergeState::ScanGroups { txn_id },
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.accept_groups(txn_id, values, next_start_after),
            (
                MergeState::ReadDetails { txn_id },
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => self.stage_all(txn_id, values),
            (
                MergeState::Write { txn_id },
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => Ok(self.after_write(txn_id)),
            (
                MergeState::DeleteStale { txn_id },
                Event::Storage(StorageEvent::BatchDeleteResult { .. }),
            ) => Ok(self.read_fence(txn_id)),
            (
                MergeState::ReadFence { txn_id },
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => {
                if !self.batch.fence.admits(&values) {
                    return Err(MergeError::PlacementFenced);
                }
                Ok(self.commit(txn_id))
            }
            (
                MergeState::Commit { txn_id },
                event @ Event::SubOperation(SubOperationEvent::EpochsMarked { .. }),
            ) => crate::abe::marked(&event, txn_id)
                .unwrap_or(Err(StorageError::WriteError("epoch marking".to_string())))
                .map_err(MergeError::from),
            (
                MergeState::Commit { .. },
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.state = MergeState::ScheduleDrain;
                Ok(smallvec![schedule_drain_effect()])
            }
            (
                MergeState::ScheduleDrain,
                Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. }),
            ) => Ok(self.finish()),
            (
                MergeState::CheckVault,
                Event::SubOperation(SubOperationEvent::VaultHeld { result }),
            ) => match result {
                Ok(merged) if merged.is_empty() => Ok(self.start_transaction()),
                Ok(merged) => {
                    self.state = MergeState::CheckCopy { merged };
                    let token = self.config.survivor_token.clone();
                    Ok(smallvec![self.read_vault(self.survivor_id(), token)])
                }
                Err(error) => Err(MergeError::Vault(error)),
            },
            (
                MergeState::CheckCopy { merged },
                Event::SubOperation(SubOperationEvent::VaultHeld { result }),
            ) => match result {
                Ok(held) if merged.iter().all(|digest| held.contains(digest)) => {
                    Ok(self.start_transaction())
                }
                Ok(_) => Err(MergeError::VaultPresent),
                Err(error) => Err(MergeError::Vault(error)),
            },
            (
                MergeState::Cutoff,
                Event::SubOperation(SubOperationEvent::TokenRevoked { result }),
            ) => match result {
                Ok(()) => Ok(self.finish()),
                Err(error) => Err(MergeError::Cutoff(error)),
            },
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, event) => Ok(self.unexpected("the event of the current merge step", event)),
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

fn parse_users(values: &[(ByteView, Option<Value>)]) -> Result<[User; 2], MergeError> {
    let [(_, survivor), (_, merged)] = values else {
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

    fn start(&mut self) -> Effects {
        self.begin().unwrap_or_else(|error| self.fail(error))
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            MergeState::Init => return self.unexpected("no event before start", event),
            MergeState::Finish | MergeState::Error => return smallvec![],
            _ => {}
        }
        self.step_storage(event)
            .unwrap_or_else(|error| self.fail(error))
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, MergeState::Finish | MergeState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(MergeError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            MergeState::JoinedCutoff { txn_id }
            | MergeState::ReadDocuments { txn_id }
            | MergeState::ScanGroups { txn_id }
            | MergeState::ReadDetails { txn_id }
            | MergeState::Write { txn_id }
            | MergeState::DeleteStale { txn_id }
            | MergeState::ReadFence { txn_id }
            | MergeState::Commit { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}
