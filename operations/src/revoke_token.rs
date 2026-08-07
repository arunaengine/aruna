use aruna_core::admin_document_reducer::{
    AdminDocumentReducerError, AdminDocumentReducerState, MAX_LIVE_REVOCATIONS_PER_ORIGIN,
};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::auth::valid_token_hash;
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    ADMIN_DOCUMENT_STATE_KEYSPACE, TOKEN_REVOCATION_OUTBOX_INDEX_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{Actor, RealmConfigDocument};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, UserId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::document_sync_outbox::{
    admin_outbox_prefix, new_outbox_record_with_id, outbox_write_entry, revocation_index_entry,
    schedule_outbox_drain_effect,
};
use crate::placement::placement_ref_for_target;

#[derive(Debug, Clone, PartialEq)]
pub struct RevokeTokenConfig {
    pub actor: Actor,
    /// Hash of the revoked bearer token; the token never enters replicated state.
    pub token_hash: String,
    /// The revoked token's own expiry, which bounds how long the entry is kept.
    pub expires_at: u64,
    /// Verified subject of the revoked bearer token.
    pub token_owner: UserId,
    /// Current unix seconds, used to prune revocations that expired.
    pub now: u64,
}

/// Appends one bearer token hash to the realm's revocation set. Rides the admin
/// document machinery of the other realm config settings, so the revocation
/// replicates realm-wide instead of denying the token on this node only.
#[derive(Debug, PartialEq)]
pub struct RevokeTokenOperation {
    config: RevokeTokenConfig,
    txn_id: Option<TxnId>,
    state: RevokeTokenState,
    output: Option<Result<RealmConfigDocument, RevokeTokenError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum RevokeTokenState {
    Init,
    StartTransaction,
    ReadCurrent,
    ReadOutboxCapacity {
        document: RealmConfigDocument,
        reducer_state: AdminDocumentReducerState,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    WriteDocumentAndAdminState {
        document: RealmConfigDocument,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
        schedule_drain: bool,
    },
    DeleteStaleAdminConflicts {
        document: RealmConfigDocument,
        schedule_drain: bool,
    },
    CommitNoop {
        document: RealmConfigDocument,
    },
    CommitTransaction {
        document: RealmConfigDocument,
    },
    ScheduleDocumentSyncOutboxDrain {
        document: RealmConfigDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RevokeTokenError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("realm config document missing")]
    RealmConfigNotFound,
    #[error("revoked bearer token hash is malformed")]
    InvalidTokenHash,
    #[error("token revocation capacity reached")]
    CapacityReached,
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl RevokeTokenOperation {
    pub fn new(config: RevokeTokenConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: RevokeTokenState::Init,
            output: None,
        }
    }

    fn document_ref(&self) -> DocumentSyncTarget {
        DocumentSyncTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn emit_read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = RevokeTokenState::ReadCurrent;
        let document = self.document_ref();
        let target = self.admin_target();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key(),
                ),
                (
                    ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                    admin_document_reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_read_result(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, RevokeTokenError> {
        if !valid_token_hash(&self.config.token_hash) {
            return Err(RevokeTokenError::InvalidTokenHash);
        }

        let Some(document_value) = document_value else {
            return Err(RevokeTokenError::RealmConfigNotFound);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        let previous_document = document.clone();
        let target = self.admin_target();
        let previous_reducer_state = reducer_state_value
            .as_ref()
            .map(|value| {
                aruna_core::admin_document_reducer::decode_admin_document_reducer_state(
                    value.as_ref(),
                )
                .map_err(ConversionError::from)
            })
            .transpose()?;
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentReducerError::TargetMismatch.into());
        }

        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentReducerState::new(target));
        reducer_state.compact_revocations(self.config.now);
        document.merge_revocations(&reducer_state, self.config.now);
        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let canonical_changed = previous_reducer_state
            .as_ref()
            .map(|state| state != &reducer_state)
            .unwrap_or(true)
            || document != previous_document;
        let existing_expiry = reducer_state
            .materialized_revoked_tokens()
            .get(&self.config.token_hash)
            .copied();

        if existing_expiry.is_some_and(|expires_at| expires_at >= self.config.expires_at) {
            self.output = Some(Ok(document.clone()));
            if canonical_changed {
                return self.emit_write(document, reducer_state, stale_conflict_deletes, false);
            }
            return Ok(self.emit_commit_noop(document));
        }

        let same_origin = reducer_state
            .revocation_origin(&self.config.token_hash)
            .is_some_and(|origin| origin == self.config.actor.node_id);
        if !same_origin
            && reducer_state.live_revocation_count(&self.config.actor.node_id)
                >= MAX_LIVE_REVOCATIONS_PER_ORIGIN
        {
            return Err(RevokeTokenError::CapacityReached);
        }

        self.emit_capacity_read(document, reducer_state, stale_conflict_deletes)
    }

    fn emit_capacity_read(
        &mut self,
        document: RealmConfigDocument,
        reducer_state: AdminDocumentReducerState,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    ) -> Result<Effects, RevokeTokenError> {
        let Some(txn_id) = self.txn_id else {
            return Err(RevokeTokenError::MissingTransaction);
        };
        self.state = RevokeTokenState::ReadOutboxCapacity {
            document,
            reducer_state,
            stale_conflict_deletes,
        };
        Ok(smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: TOKEN_REVOCATION_OUTBOX_INDEX_KEYSPACE.to_string(),
            prefix: Some(admin_outbox_prefix(self.config.actor.node_id)),
            start: None,
            limit: MAX_LIVE_REVOCATIONS_PER_ORIGIN.saturating_add(1),
            txn_id: Some(txn_id),
        })])
    }

    fn emit_write(
        &mut self,
        mut document: RealmConfigDocument,
        mut reducer_state: AdminDocumentReducerState,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
        apply_event: bool,
    ) -> Result<Effects, RevokeTokenError> {
        let Some(txn_id) = self.txn_id else {
            return Err(RevokeTokenError::MissingTransaction);
        };
        let admin_event = apply_event.then(|| {
            reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash: self.config.token_hash.clone(),
                    expires_at: self.config.expires_at,
                    token_owner: self.config.token_owner,
                },
            )
        });
        let admin_event = admin_event.transpose()?;
        reducer_state.compact_revocations(self.config.now);
        document.merge_revocations(&reducer_state, self.config.now);

        let document_target = self.document_ref();
        let placement = placement_ref_for_target(&document, &document_target, Default::default());
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.config.actor)?.into(),
            ),
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        if let Some(admin_event) = admin_event {
            let record = new_outbox_record_with_id(
                admin_event.event_id,
                self.config.actor.node_id,
                document_target,
                Vec::new(),
                DocumentSyncOutboxEvent::AdminOperation {
                    event: Box::new(admin_event),
                },
                placement,
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
            writes.push(revocation_index_entry(&record));
        }
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(document.clone()));
        self.state = RevokeTokenState::WriteDocumentAndAdminState {
            document,
            stale_conflict_deletes,
            schedule_drain: apply_event,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit_noop(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(RevokeTokenError::MissingTransaction);
        };
        self.state = RevokeTokenState::CommitNoop { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn emit_commit(&mut self, document: RealmConfigDocument, schedule_drain: bool) -> Effects {
        if schedule_drain {
            self.emit_commit_transaction(document)
        } else {
            self.emit_commit_noop(document)
        }
    }

    fn emit_commit_transaction(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(RevokeTokenError::MissingTransaction);
        };
        self.state = RevokeTokenState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: RevokeTokenError) -> Effects {
        let cleanup = self.abort();
        self.state = RevokeTokenState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(RevokeTokenError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for RevokeTokenOperation {
    type Output = RealmConfigDocument;
    type Error = RevokeTokenError;

    fn start(&mut self) -> Effects {
        self.state = RevokeTokenState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            RevokeTokenState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            RevokeTokenState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document_value), (_, reducer_state_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with realm config and reducer state",
                            format!("{values:?}"),
                        );
                    };
                    match self.emit_read_result(document_value.clone(), reducer_state_value.clone())
                    {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            RevokeTokenState::ReadOutboxCapacity {
                document,
                reducer_state,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    if values.len() >= MAX_LIVE_REVOCATIONS_PER_ORIGIN {
                        return self.fail(RevokeTokenError::CapacityReached);
                    }
                    match self.emit_write(document, reducer_state, stale_conflict_deletes, true) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("outbox capacity iteration result", format!("{other:?}"))
                }
            },
            RevokeTokenState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
                schedule_drain,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(RevokeTokenError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = RevokeTokenState::DeleteStaleAdminConflicts {
                            document,
                            schedule_drain,
                        };
                        return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                            deletes: stale_conflict_deletes,
                            txn_id: Some(txn_id),
                        })];
                    }
                    self.emit_commit(document, schedule_drain)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch write result", format!("{other:?}")),
            },
            RevokeTokenState::DeleteStaleAdminConflicts {
                document,
                schedule_drain,
            } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit(document, schedule_drain)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            RevokeTokenState::CommitNoop { .. } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = RevokeTokenState::Finish;
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            RevokeTokenState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = RevokeTokenState::ScheduleDocumentSyncOutboxDrain { document };
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            RevokeTokenState::ScheduleDocumentSyncOutboxDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = RevokeTokenState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule token revocation outbox drain; durable outbox remains retryable");
                    self.state = RevokeTokenState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            RevokeTokenState::Finish | RevokeTokenState::Error | RevokeTokenState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RevokeTokenState::Finish | RevokeTokenState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.expect("revoke token operation must set output")
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ADMIN_DOCUMENT_STATE_KEYSPACE, AdminDocumentReducerState, AdminDocumentTarget, Event,
        MAX_LIVE_REVOCATIONS_PER_ORIGIN, RevokeTokenConfig, RevokeTokenError, RevokeTokenOperation,
        StorageEffect, StorageEvent, admin_document_reducer_state_key,
    };
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::document_sync_outbox::admin_outbox_prefix;
    use crate::driver::{DriverContext, drive};
    use crate::get_realm_config::GetRealmConfigOperation;
    use aruna_core::UserId;
    use aruna_core::admin_documents::AdminDocumentOperation;
    use aruna_core::auth::bearer_token_hash;
    use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncOutboxRecord};
    use aruna_core::keyspaces::{
        DOCUMENT_SYNC_OUTBOX_KEYSPACE, TOKEN_REVOCATION_OUTBOX_INDEX_KEYSPACE,
    };
    use aruna_core::storage_entries::admin_document_reducer_state_write_entry;
    use aruna_core::structs::{Actor, RealmId};
    use aruna_core::types::{Key, Value};
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    async fn setup_realm() -> (tempfile::TempDir, DriverContext, Actor) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        };
        let realm_id = RealmId([31u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([6u8; 16]), realm_id),
            realm_id,
        };
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "revocation realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &context,
        )
        .await
        .unwrap();
        (dir, context, actor)
    }

    fn revocation(actor: &Actor, token_hash: &str, expires_at: u64, now: u64) -> RevokeTokenConfig {
        RevokeTokenConfig {
            actor: actor.clone(),
            token_hash: token_hash.to_string(),
            expires_at,
            token_owner: actor.user_id,
            now,
        }
    }

    fn seed_state(actor: &Actor, count: usize) -> AdminDocumentReducerState {
        let target = AdminDocumentTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        let mut state = AdminDocumentReducerState::new(target);
        for index in 0..count {
            state
                .apply_operation(
                    actor,
                    AdminDocumentOperation::RealmConfigTokenRevoked {
                        token_hash: bearer_token_hash(&format!("seed-token-{index}")),
                        expires_at: 2_000,
                        token_owner: actor.user_id,
                    },
                )
                .unwrap();
        }
        state
    }

    async fn write_state(context: &DriverContext, state: &AdminDocumentReducerState) {
        let (key_space, key, value) = admin_document_reducer_state_write_entry(state).unwrap();
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected reducer state write event: {other:?}"),
        }
    }

    async fn write_index(context: &DriverContext, actor: &Actor, count: usize) {
        let prefix = admin_outbox_prefix(actor.node_id);
        let writes = (0..count)
            .map(|index| {
                let mut key = prefix.to_vec();
                key.extend_from_slice(&(index as u64).to_be_bytes());
                (
                    TOKEN_REVOCATION_OUTBOX_INDEX_KEYSPACE.to_string(),
                    key.into(),
                    vec![1u8].into(),
                )
            })
            .collect();
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected index write event: {other:?}"),
        }
    }

    async fn iter_values(
        context: &DriverContext,
        key_space: &str,
        prefix: Option<Key>,
    ) -> Vec<(Key, Value)> {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix,
                start: None,
                limit: MAX_LIVE_REVOCATIONS_PER_ORIGIN.saturating_add(1),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values,
            other => panic!("unexpected iteration event: {other:?}"),
        }
    }

    async fn token_records(
        context: &DriverContext,
        actor: &Actor,
    ) -> Vec<DocumentSyncOutboxRecord> {
        iter_values(
            context,
            DOCUMENT_SYNC_OUTBOX_KEYSPACE,
            Some(admin_outbox_prefix(actor.node_id)),
        )
        .await
        .into_iter()
        .filter_map(|(_, value)| postcard::from_bytes(&value).ok())
        .filter(|record: &DocumentSyncOutboxRecord| {
            matches!(
                &record.event,
                DocumentSyncOutboxEvent::AdminOperation { event }
                    if matches!(
                        &event.op,
                        AdminDocumentOperation::RealmConfigTokenRevoked { .. }
                    )
            )
        })
        .collect()
    }

    async fn index_values(context: &DriverContext, actor: &Actor) -> Vec<(Key, Value)> {
        iter_values(
            context,
            TOKEN_REVOCATION_OUTBOX_INDEX_KEYSPACE,
            Some(admin_outbox_prefix(actor.node_id)),
        )
        .await
    }

    #[tokio::test]
    async fn stores_revocation() {
        // The hash lands on the stored realm config and a later read returns it.
        let (_dir, context, actor) = setup_realm().await;
        let token_hash = bearer_token_hash("revoked-token");
        let document = drive(
            RevokeTokenOperation::new(revocation(&actor, &token_hash, 2_000, 1_000)),
            &context,
        )
        .await
        .unwrap();
        assert!(document.token_revoked(&token_hash, 1_000));

        let read = drive(GetRealmConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        assert!(read.token_revoked(&token_hash, 1_000));
    }

    #[tokio::test]
    async fn repeat_revocation_accumulates() {
        // Revoking twice is idempotent, and a second token does not evict the first.
        let (_dir, context, actor) = setup_realm().await;
        let first = bearer_token_hash("first-token");
        let second = bearer_token_hash("second-token");
        drive(
            RevokeTokenOperation::new(revocation(&actor, &first, 2_000, 1_000)),
            &context,
        )
        .await
        .unwrap();
        drive(
            RevokeTokenOperation::new(revocation(&actor, &first, 2_000, 1_000)),
            &context,
        )
        .await
        .unwrap();

        assert_eq!(index_values(&context, &actor).await.len(), 1);
        assert_eq!(token_records(&context, &actor).await.len(), 1);
        let state = reducer_state(&context, actor.realm_id).await;
        assert_eq!(
            state
                .user_subject_ids
                .keys()
                .filter(|path| path.contains(&first))
                .count(),
            1
        );

        drive(
            RevokeTokenOperation::new(revocation(&actor, &second, 2_000, 1_000)),
            &context,
        )
        .await
        .unwrap();

        let read = drive(GetRealmConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        assert_eq!(read.revoked_tokens.len(), 2);
        assert!(read.token_revoked(&first, 1_000));
        assert!(read.token_revoked(&second, 1_000));
    }

    #[tokio::test]
    async fn rejects_live_cap() {
        let (_dir, context, actor) = setup_realm().await;
        let state = seed_state(&actor, MAX_LIVE_REVOCATIONS_PER_ORIGIN);
        write_state(&context, &state).await;

        let result = drive(
            RevokeTokenOperation::new(revocation(
                &actor,
                &bearer_token_hash("capacity-token"),
                2_000,
                1_000,
            )),
            &context,
        )
        .await;

        assert!(matches!(result, Err(RevokeTokenError::CapacityReached)));
        assert_eq!(reducer_state(&context, actor.realm_id).await, state);
        assert!(index_values(&context, &actor).await.is_empty());
    }

    #[tokio::test]
    async fn rejects_transfer_cap() {
        let (_dir, context, actor) = setup_realm().await;
        let other = Actor {
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            user_id: actor.user_id,
            realm_id: actor.realm_id,
        };
        let transferred = bearer_token_hash("transfer-token");
        let mut state = seed_state(&actor, MAX_LIVE_REVOCATIONS_PER_ORIGIN);
        state
            .apply_operation(
                &other,
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash: transferred.clone(),
                    expires_at: 2_000,
                    token_owner: actor.user_id,
                },
            )
            .unwrap();
        write_state(&context, &state).await;

        let result = drive(
            RevokeTokenOperation::new(revocation(&actor, &transferred, 3_000, 1_000)),
            &context,
        )
        .await;

        assert!(matches!(result, Err(RevokeTokenError::CapacityReached)));
        assert_eq!(reducer_state(&context, actor.realm_id).await, state);
        assert!(index_values(&context, &actor).await.is_empty());
    }

    #[tokio::test]
    async fn rejects_index_cap() {
        let (_dir, context, actor) = setup_realm().await;
        let state = seed_state(&actor, 1);
        write_state(&context, &state).await;
        write_index(&context, &actor, MAX_LIVE_REVOCATIONS_PER_ORIGIN).await;

        let result = drive(
            RevokeTokenOperation::new(revocation(
                &actor,
                &bearer_token_hash("index-capacity-token"),
                2_000,
                1_000,
            )),
            &context,
        )
        .await;

        assert!(matches!(result, Err(RevokeTokenError::CapacityReached)));
        assert_eq!(reducer_state(&context, actor.realm_id).await, state);
        assert_eq!(
            index_values(&context, &actor).await.len(),
            MAX_LIVE_REVOCATIONS_PER_ORIGIN
        );
    }

    #[tokio::test]
    async fn prunes_expired_revocations() {
        // Once the revoked token can no longer validate its entry is dropped,
        // so the replicated set stays bounded and is not scanned forever.
        let (_dir, context, actor) = setup_realm().await;
        let stale = bearer_token_hash("stale-token");
        let fresh = bearer_token_hash("fresh-token");
        drive(
            RevokeTokenOperation::new(revocation(&actor, &stale, 2_000, 1_000)),
            &context,
        )
        .await
        .unwrap();

        let document = drive(
            RevokeTokenOperation::new(revocation(&actor, &fresh, 4_000, 3_000)),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(document.revoked_tokens.len(), 1);
        assert!(!document.token_revoked(&stale, 3_000));
        assert!(document.token_revoked(&fresh, 3_000));

        let read = drive(GetRealmConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        assert_eq!(read.revoked_tokens.len(), 1);
    }

    #[tokio::test]
    async fn compacts_reducer_state() {
        // The reducer state behind the replicated set must shed expired entries
        // too, so repeated revocations cannot grow the stored state for good.
        let (_dir, context, actor) = setup_realm().await;
        let stale = bearer_token_hash("stale-token");
        let fresh = bearer_token_hash("fresh-token");
        for config in [
            revocation(&actor, &stale, 2_000, 1_000),
            revocation(&actor, &fresh, 4_000, 3_000),
        ] {
            drive(RevokeTokenOperation::new(config), &context)
                .await
                .unwrap();
        }

        let state = reducer_state(&context, actor.realm_id).await;
        assert_eq!(
            state.materialized_revoked_tokens(),
            std::collections::BTreeMap::from([(fresh, 4_000)])
        );
        assert!(
            !state
                .user_subject_ids
                .keys()
                .any(|path| path.contains(&stale))
        );
    }

    async fn reducer_state(
        context: &DriverContext,
        realm_id: RealmId,
    ) -> AdminDocumentReducerState {
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                key: admin_document_reducer_state_key(&target),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => aruna_core::admin_document_reducer::decode_admin_document_reducer_state(&bytes)
                .expect("reducer state decodes"),
            other => panic!("unexpected reducer state read: {other:?}"),
        }
    }

    #[tokio::test]
    async fn rejects_malformed_hash() {
        let (_dir, context, actor) = setup_realm().await;
        let result = drive(
            RevokeTokenOperation::new(revocation(&actor, "not-a-hash", 2_000, 1_000)),
            &context,
        )
        .await;
        assert!(matches!(result, Err(RevokeTokenError::InvalidTokenHash)));
    }
}
