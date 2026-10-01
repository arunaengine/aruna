//! Appends one vault revision or public key record on a holder of the user's vault placement.
//! The rows, their sync sidecars and the outbox publish commit in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::vault::{
    MAX_KEY_RECORDS, MAX_PREDECESSORS, MAX_VAULT_HEADS, UserKeyRecord, VaultRecordError,
    VaultRevision, head_rows, record_rows, user_record_prefix,
};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use aruna_core::vault_format::key_fingerprint;
use aruna_core::{NodeId, UserId};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::placement::{PlacementResolveError, fence, holds_placement, plan_target_placement};
use crate::sync::document_outbox::{new_outbox_record, outbox_write_entry, schedule_drain_effect};

/// One change a user makes to their own vault records.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultChange {
    /// A new head replacing `predecessors`, the heads the client read.
    Save {
        payload: String,
        predecessors: Vec<Ulid>,
    },
    /// Replaces every current head with a delete marker.
    Delete,
    PublishKey {
        key_id: String,
        public_key: [u8; 32],
        has_recovery: bool,
    },
}

/// Shows the payload size only, so no formatted message carries vault ciphertext.
impl std::fmt::Debug for VaultChange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Save {
                payload,
                predecessors,
            } => formatter
                .debug_struct("Save")
                .field("payload_bytes", &payload.len())
                .field("predecessors", predecessors)
                .finish(),
            Self::Delete => formatter.write_str("Delete"),
            Self::PublishKey { key_id, .. } => formatter
                .debug_struct("PublishKey")
                .field("key_id", key_id)
                .finish_non_exhaustive(),
        }
    }
}

/// Names an event without its values, so no error message carries vault bytes.
pub(super) fn event_label(event: &Event) -> &'static str {
    match event {
        Event::Storage(StorageEvent::ReadResult { .. }) => "storage read",
        Event::Storage(StorageEvent::IterResult { .. }) => "storage scan",
        Event::Storage(_) => "storage result",
        Event::Net(_) => "net result",
        Event::Task(_) => "task result",
        _ => "other event",
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppendVaultConfig {
    pub node_id: NodeId,
    pub user_id: UserId,
    /// Chosen where the request arrived, so a forwarded change keeps its id.
    pub record_id: Ulid,
    pub change: VaultChange,
    pub now_ms: u64,
}

/// The heads after a save or delete, or the published key record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultAppended {
    Heads(Vec<VaultRevision>),
    Key(Box<UserKeyRecord>),
}

#[derive(Debug, Error, PartialEq)]
pub enum AppendVaultError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Record(#[from] VaultRecordError),
    #[error(transparent)]
    PlacementResolve(#[from] PlacementResolveError),
    #[error("realm config document missing")]
    RealmConfigMissing,
    #[error("no placement strategy governs user vaults")]
    PlacementUnavailable,
    /// This node holds no replica of the user's vault; the caller forwards to `holders`.
    #[error("node holds no replica of the user's vault")]
    NotHolder { holders: Vec<NodeId> },
    #[error("vault placement cut over mid-write; retry")]
    PlacementFenced,
    #[error("the user has published the most key records allowed")]
    TooManyKeys,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
struct Planned {
    placement: PlacementRef,
    holders: Vec<NodeId>,
    generation: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum AppendState {
    Init,
    StartTransaction,
    ReadConfig,
    ReadFence(Box<Planned>),
    ReadRows(Box<Planned>),
    Delete(Vec<(KeySpace, Key, Value)>),
    Write,
    Commit,
    ScheduleDrain,
    Finish,
    Error,
}

impl AppendState {
    fn label(&self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::StartTransaction => "start transaction",
            Self::ReadConfig => "read config",
            Self::ReadFence(_) => "read fence",
            Self::ReadRows(_) => "read rows",
            Self::Delete(_) => "delete retired heads",
            Self::Write => "write",
            Self::Commit => "commit",
            Self::ScheduleDrain => "schedule drain",
            Self::Finish => "finish",
            Self::Error => "error",
        }
    }
}

/// Adds one immutable vault or key record for the user on a holder of the user's
/// vault placement. A non-holder fails with `NotHolder`, so the caller forwards.
#[derive(Debug, PartialEq)]
pub struct AppendVaultOperation {
    config: AppendVaultConfig,
    txn_id: Option<TxnId>,
    state: AppendState,
    output: Option<Result<VaultAppended, AppendVaultError>>,
    delete_heads: Vec<Ulid>,
    delete_markers: Vec<VaultRevision>,
    delete_live: bool,
}

impl AppendVaultOperation {
    pub fn new(config: AppendVaultConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: AppendState::Init,
            output: None,
            delete_heads: Vec::new(),
            delete_markers: Vec::new(),
            delete_live: false,
        }
    }

    fn target(&self) -> DocumentTarget {
        let AppendVaultConfig {
            user_id, record_id, ..
        } = self.config;
        match self.config.change {
            VaultChange::PublishKey { .. } => DocumentTarget::UserKey { user_id, record_id },
            VaultChange::Save { .. } | VaultChange::Delete => DocumentTarget::VaultRevision {
                user_id,
                revision_id: record_id,
            },
        }
    }

    fn txn(&self) -> Result<TxnId, AppendVaultError> {
        self.txn_id
            .ok_or(StorageError::TransactionNotFound)
            .map_err(Into::into)
    }

    fn plan(&mut self, value: Option<Value>) -> Result<Effects, AppendVaultError> {
        let value = value.ok_or(AppendVaultError::RealmConfigMissing)?;
        let config = RealmConfigDocument::from_bytes(&value)?;
        let plan = plan_target_placement(&config, &self.target(), Default::default())?
            .ok_or(AppendVaultError::PlacementUnavailable)?;
        if !holds_placement(&config, &plan.placement, self.config.node_id) {
            return Err(AppendVaultError::NotHolder {
                holders: plan.holders,
            });
        }
        let planned = Box::new(Planned {
            placement: plan.placement,
            holders: plan.holders,
            generation: fence::write_generation(&config, &plan.placement).unwrap_or_default(),
        });
        if planned.generation == 0 {
            return self.read_rows(planned, None);
        }
        let (key_space, key) = fence::fence_read(&self.config.user_id.realm_id, &planned.placement);
        let txn_id = Some(self.txn()?);
        self.state = AppendState::ReadFence(planned);
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space,
            key,
            txn_id,
        })])
    }

    fn read_rows(
        &mut self,
        planned: Box<Planned>,
        start: Option<Key>,
    ) -> Result<Effects, AppendVaultError> {
        let limit = match self.config.change {
            VaultChange::PublishKey { .. } => MAX_KEY_RECORDS,
            VaultChange::Save { .. } | VaultChange::Delete => MAX_VAULT_HEADS,
        };
        let txn_id = Some(self.txn()?);
        self.state = AppendState::ReadRows(planned);
        Ok(smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: self.target().storage_keyspace().to_string(),
            prefix: Some(user_record_prefix(self.config.user_id)),
            start: start.map(IterStart::After),
            limit,
            txn_id,
        })])
    }

    /// Builds the record from the rows read in this transaction and emits its writes.
    fn append(
        &mut self,
        planned: &Planned,
        rows: Vec<(Key, Value)>,
    ) -> Result<Effects, AppendVaultError> {
        let AppendVaultConfig {
            node_id,
            user_id,
            record_id,
            now_ms,
            ..
        } = self.config;
        let (rows, deletes, output, bytes, change) = match self.config.change.clone() {
            VaultChange::PublishKey {
                key_id,
                public_key,
                has_recovery,
            } => {
                if rows.len() >= MAX_KEY_RECORDS {
                    return Err(AppendVaultError::TooManyKeys);
                }
                let record = UserKeyRecord {
                    user_id,
                    record_id,
                    key_id,
                    public_key,
                    fingerprint: key_fingerprint(&public_key),
                    has_recovery,
                    node_id,
                    placement: planned.placement,
                    created_at_ms: now_ms,
                };
                record.validate()?;
                let (bytes, change) = (record.to_bytes()?, record.sync_change());
                let rows = record_rows(&record.target(), &bytes, &change)?;
                let output = VaultAppended::Key(Box::new(record));
                (rows, Vec::new(), output, bytes, change)
            }
            VaultChange::Save {
                payload,
                predecessors,
            } => {
                let mut heads = rows
                    .iter()
                    .map(|(_, value)| VaultRevision::from_bytes(value))
                    .collect::<Result<Vec<_>, _>>()?;
                let record = VaultRevision {
                    user_id,
                    revision_id: record_id,
                    predecessors,
                    payload: Some(payload),
                    node_id,
                    placement: planned.placement,
                    created_at_ms: now_ms,
                };
                record.validate()?;
                let (bytes, change) = (record.to_bytes()?, record.sync_change());
                let (rows, deletes) = head_rows(&record, &bytes)?;
                heads.retain(|head| !record.predecessors.contains(&head.revision_id));
                heads.push(record);
                heads.sort_by_key(|head| head.revision_id);
                (rows, deletes, VaultAppended::Heads(heads), bytes, change)
            }
            VaultChange::Delete => return self.delete_heads(planned),
        };
        let mut writes = rows;
        let target = self.target();
        let outbox = new_outbox_record(
            node_id,
            target,
            planned.holders.clone(),
            DocumentOutboxEvent::Upsert { bytes, change },
            planned.placement,
            false,
        )
        .fenced_at(planned.generation);
        writes.push(outbox_write_entry(&outbox).map_err(ConversionError::from)?);
        self.output = Some(Ok(output));
        self.write_rows(writes, deletes)
    }

    fn delete_heads(&mut self, planned: &Planned) -> Result<Effects, AppendVaultError> {
        if !self.delete_live {
            self.output = Some(Ok(VaultAppended::Heads(std::mem::take(
                &mut self.delete_markers,
            ))));
            self.state = AppendState::Finish;
            return Ok(self.abort());
        }
        let mut heads = std::mem::take(&mut self.delete_heads)
            .into_iter()
            .peekable();
        let mut previous = None;
        let mut writes = Vec::new();
        let mut deletes = Vec::new();
        loop {
            let mut predecessors: Vec<_> = previous.into_iter().collect();
            predecessors.extend(heads.by_ref().take(MAX_PREDECESSORS - predecessors.len()));
            let final_head = heads.peek().is_none();
            let record = VaultRevision {
                user_id: self.config.user_id,
                revision_id: if final_head {
                    self.config.record_id
                } else {
                    Ulid::generate()
                },
                predecessors,
                payload: None,
                node_id: self.config.node_id,
                placement: planned.placement,
                created_at_ms: self.config.now_ms,
            };
            record.validate()?;
            let bytes = record.to_bytes()?;
            let change = record.sync_change();
            let (mut rows, retired) = head_rows(&record, &bytes)?;
            if !final_head {
                rows.retain(|(key_space, _, _)| {
                    key_space != aruna_core::keyspaces::VAULT_REVISION_KEYSPACE
                });
            }
            writes.extend(rows);
            deletes.extend(retired);
            let outbox = new_outbox_record(
                self.config.node_id,
                record.target(),
                planned.holders.clone(),
                DocumentOutboxEvent::Upsert { bytes, change },
                planned.placement,
                false,
            )
            .fenced_at(planned.generation);
            writes.push(outbox_write_entry(&outbox).map_err(ConversionError::from)?);
            if final_head {
                self.output = Some(Ok(VaultAppended::Heads(vec![record])));
                return self.write_rows(writes, deletes);
            }
            previous = Some(record.revision_id);
        }
    }

    fn write_rows(
        &mut self,
        writes: Vec<(KeySpace, Key, Value)>,
        deletes: Vec<(KeySpace, Key)>,
    ) -> Result<Effects, AppendVaultError> {
        let txn_id = Some(self.txn()?);
        if deletes.is_empty() {
            self.state = AppendState::Write;
            return Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id
            })]);
        }
        self.state = AppendState::Delete(writes);
        Ok(smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id
        })])
    }

    fn handle(&mut self, state: AppendState, event: Event) -> Result<Effects, AppendVaultError> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            if matches!(state, AppendState::Commit) {
                self.txn_id = None;
            }
            return Err(error.into());
        }
        match (state, event) {
            (
                AppendState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = AppendState::ReadConfig;
                let target = DocumentTarget::RealmConfig {
                    realm_id: self.config.user_id.realm_id,
                };
                Ok(smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: target.storage_keyspace().to_string(),
                    key: target.storage_key(),
                    txn_id: Some(txn_id),
                })])
            }
            (AppendState::ReadConfig, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.plan(value)
            }
            (
                AppendState::ReadFence(planned),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => {
                if !fence::admits(value.as_ref(), planned.generation) {
                    return Err(AppendVaultError::PlacementFenced);
                }
                self.read_rows(planned, None)
            }
            (
                AppendState::ReadRows(planned),
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                if matches!(self.config.change, VaultChange::Delete) {
                    for (_, value) in &values {
                        let head = VaultRevision::from_bytes(value)?;
                        self.delete_heads.push(head.revision_id);
                        self.delete_live |= head.payload.is_some();
                        if head.payload.is_none() && self.delete_markers.len() < MAX_VAULT_HEADS {
                            self.delete_markers.push(head);
                        }
                    }
                    if let Some(start) = next_start_after {
                        return self.read_rows(planned, Some(start));
                    }
                }
                self.append(&planned, values)
            }
            (
                AppendState::Delete(writes),
                Event::Storage(StorageEvent::BatchDeleteResult { .. }),
            ) => {
                self.state = AppendState::Write;
                Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: Some(self.txn()?),
                })])
            }
            (AppendState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.state = AppendState::Commit;
                Ok(smallvec![Effect::Storage(
                    StorageEffect::CommitTransaction {
                        txn_id: self.txn()?
                    }
                )])
            }
            (AppendState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = AppendState::ScheduleDrain;
                Ok(smallvec![schedule_drain_effect()])
            }
            (AppendState::ScheduleDrain, Event::Task(event)) => {
                if let TaskEvent::Error { message, .. } = event {
                    warn!(error = %message, "Vault outbox drain was not scheduled; the outbox stays retryable");
                }
                self.state = AppendState::Finish;
                Ok(smallvec![])
            }
            (state, event) => Err(AppendVaultError::UnexpectedEvent {
                state: state.label().to_string(),
                expected: "the event of the current step",
                got: event_label(&event).to_string(),
            }),
        }
    }

    fn fail(&mut self, error: AppendVaultError) -> Effects {
        let cleanup = self.abort();
        self.state = AppendState::Error;
        self.output = Some(Err(error));
        cleanup
    }
}

impl Operation for AppendVaultOperation {
    type Output = VaultAppended;
    type Error = AppendVaultError;

    fn start(&mut self) -> Effects {
        if let VaultChange::Save { payload, .. } = &self.config.change
            && payload.len() > aruna_core::structs::identity::user::vault::MAX_VAULT_BYTES
        {
            return self.fail(VaultRecordError::TooLarge.into());
        }
        self.state = AppendState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = std::mem::replace(&mut self.state, AppendState::Error);
        if matches!(state, AppendState::Finish | AppendState::Error) {
            self.state = state;
            return smallvec![];
        }
        match self.handle(state, event) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, AppendState::Finish | AppendState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(AppendVaultError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            AppendVaultError::NotHolder { .. }
                | AppendVaultError::Record(_)
                | AppendVaultError::TooManyKeys
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::{VAULT_RETIRED_KEYSPACE, VAULT_REVISION_KEYSPACE};
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
    use aruna_core::structs::identity::user::vault::user_record_key;
    use aruna_core::task::TaskKey;
    use std::time::Duration;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn user() -> UserId {
        UserId::local(Ulid::from_bytes([7; 16]), RealmId::from_bytes([3; 32]))
    }

    fn txn() -> TxnId {
        Ulid::from_bytes([6; 16])
    }

    /// Four servers and two vault replicas, so the realm has holders and non-holders.
    fn config_bytes() -> (Value, Vec<NodeId>) {
        let mut config = RealmConfigDocument::new(user().realm_id, Vec::new(), 2);
        config.seed_default_placement();
        for seed in 1..=4 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        let target = DocumentTarget::VaultRevision {
            user_id: user(),
            revision_id: Ulid::nil(),
        };
        let holders = plan_target_placement(&config, &target, Default::default())
            .unwrap()
            .unwrap()
            .holders;
        let actor = Actor {
            node_id: node(1),
            user_id: user(),
            realm_id: user().realm_id,
        };
        (config.to_bytes(&actor).unwrap().into(), holders)
    }

    fn operation(node_id: NodeId, change: VaultChange) -> AppendVaultOperation {
        AppendVaultOperation::new(AppendVaultConfig {
            node_id,
            user_id: user(),
            record_id: Ulid::from_bytes([9; 16]),
            change,
            now_ms: 5,
        })
    }

    /// Runs the operation up to the row read; `None` when it ended before that.
    fn read_rows(operation: &mut AppendVaultOperation, config: Value) -> Option<Effects> {
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: txn(),
        }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Key::from(&[][..]),
            value: Some(config),
        }));
        matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Iter { .. }))
        )
        .then_some(effects)
    }

    fn head(seed: u8, payload: Option<&str>) -> (Key, Value) {
        let revision = VaultRevision {
            user_id: user(),
            revision_id: Ulid::from_bytes([seed; 16]),
            predecessors: Vec::new(),
            payload: payload.map(str::to_string),
            node_id: node(1),
            placement: PlacementRef::NIL,
            created_at_ms: 1,
        };
        (
            revision.target().storage_key(),
            revision.to_bytes().unwrap().into(),
        )
    }

    fn rows(operation: &mut AppendVaultOperation, values: Vec<(Key, Value)>) -> Effects {
        operation.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }))
    }

    fn written(effects: &Effects) -> Vec<(KeySpace, Key)> {
        match effects.first() {
            Some(Effect::Storage(StorageEffect::BatchWrite { writes, .. })) => writes
                .iter()
                .map(|(key_space, key, _)| (key_space.clone(), key.clone()))
                .collect(),
            other => panic!("expected a batch write, got {other:?}"),
        }
    }

    #[test]
    fn saves_on_holder() {
        let (config, holders) = config_bytes();
        let previous = Ulid::from_bytes([1; 16]);
        let mut operation = operation(
            holders[0],
            VaultChange::Save {
                payload: "sealed".to_string(),
                predecessors: vec![previous],
            },
        );
        read_rows(&mut operation, config).expect("a holder reads the heads");
        let effects = rows(
            &mut operation,
            vec![head(1, Some("old")), head(2, Some("other"))],
        );
        let previous_row = user_record_key(user(), previous);
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::BatchDelete { deletes, .. }))
                if deletes == &vec![(VAULT_REVISION_KEYSPACE.to_string(), previous_row.clone())]
        ));
        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        let writes = written(&effects);
        let own_row = user_record_key(user(), Ulid::from_bytes([9; 16]));
        assert!(writes.contains(&(VAULT_REVISION_KEYSPACE.to_string(), own_row)));
        assert!(writes.contains(&(VAULT_RETIRED_KEYSPACE.to_string(), previous_row)));
        assert!(
            writes
                .iter()
                .any(|(key_space, _)| key_space == aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE),
            "the record is queued for the other holders in the same transaction"
        );
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: txn(),
        }));
        operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainSyncOutbox,
            after: Duration::ZERO,
        }));
        assert!(operation.is_complete());
        let VaultAppended::Heads(heads) = operation.finalize().unwrap() else {
            panic!("a save answers heads");
        };
        let ids: Vec<_> = heads.iter().map(|head| head.revision_id).collect();
        assert_eq!(ids, [Ulid::from_bytes([2; 16]), Ulid::from_bytes([9; 16])]);
    }

    #[test]
    fn refuses_non_holder() {
        let (config, holders) = config_bytes();
        let outsider = (1..=4)
            .map(node)
            .find(|node| !holders.contains(node))
            .unwrap();
        let mut operation = operation(outsider, VaultChange::Delete);
        assert!(read_rows(&mut operation, config).is_none());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(AppendVaultError::NotHolder { holders })
        );
    }

    #[test]
    fn deletes_live_heads() {
        let (config, holders) = config_bytes();
        let mut operation = operation(holders[0], VaultChange::Delete);
        read_rows(&mut operation, config.clone()).unwrap();
        // Only delete markers left: nothing to write.
        let effects = rows(&mut operation, vec![head(1, None)]);
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::AbortTransaction { .. }))
        ));
        assert!(
            matches!(operation.finalize(), Ok(VaultAppended::Heads(heads)) if heads.len() == 1)
        );

        let mut operation = self::operation(holders[0], VaultChange::Delete);
        read_rows(&mut operation, config).unwrap();
        let effects = rows(&mut operation, vec![head(1, None), head(2, Some("sealed"))]);
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::BatchDelete { deletes, .. })) if deletes.len() == 2
        ));
    }

    #[test]
    fn deletes_snapshot_heads() {
        use aruna_core::document::DocumentOutboxRecord;
        use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
        use std::collections::{BTreeMap, BTreeSet};

        for count in [33, 65] {
            let (config, holders) = config_bytes();
            let mut operation = operation(holders[0], VaultChange::Delete);
            operation.config.record_id = Ulid::from_bytes([250; 16]);
            let original: Vec<_> = (1..=count)
                .map(|id| head(id, (id == count).then_some("sealed")))
                .collect();
            read_rows(&mut operation, config).unwrap();
            let AppendState::ReadRows(planned) = &mut operation.state else {
                panic!("expected scan");
            };
            planned.generation = 7;
            let mut effects = smallvec![];
            for (index, page) in original.chunks(MAX_VAULT_HEADS).enumerate() {
                let next = (index * MAX_VAULT_HEADS + page.len() < original.len())
                    .then(|| page.last().unwrap().0.clone());
                effects = operation.step(Event::Storage(StorageEvent::IterResult {
                    values: page.to_vec(),
                    next_start_after: next.clone(),
                }));
                if let Some(next) = next {
                    assert!(matches!(effects.first(),
                        Some(Effect::Storage(StorageEffect::Iter {
                            start: Some(IterStart::After(start)), limit, txn_id, ..
                        })) if start == &next && *limit == MAX_VAULT_HEADS && *txn_id == Some(txn())));
                }
            }
            let Some(Effect::Storage(StorageEffect::BatchDelete { deletes, txn_id })) =
                effects.first()
            else {
                panic!("expected snapshot deletes");
            };
            assert_eq!(*txn_id, Some(txn()));
            let deletes = deletes.clone();
            assert!(original.iter().all(|(key, _)| {
                deletes.contains(&(VAULT_REVISION_KEYSPACE.to_string(), key.clone()))
            }));
            let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
                entries: Vec::new(),
            }));
            let Some(Effect::Storage(StorageEffect::BatchWrite { writes, txn_id })) =
                effects.first()
            else {
                panic!("expected atomic writes");
            };
            assert_eq!(*txn_id, Some(txn()));
            let outbox: Vec<_> = writes
                .iter()
                .filter(|(space, _, _)| space == SYNC_OUTBOX_KEYSPACE)
                .map(|(_, _, bytes)| postcard::from_bytes::<DocumentOutboxRecord>(bytes).unwrap())
                .collect();
            assert_eq!(outbox.len(), if count == 33 { 2 } else { 3 });
            let mut covered = BTreeSet::new();
            let mut markers = BTreeSet::new();
            for entry in &outbox {
                assert_eq!(entry.generation, 7);
                let DocumentOutboxEvent::Upsert { bytes, change } = &entry.event else {
                    panic!("expected upsert");
                };
                let marker = VaultRevision::from_bytes(bytes).unwrap();
                assert_eq!(marker.validate(), Ok(()));
                assert!(marker.payload.is_none());
                assert_eq!(entry.target, marker.target());
                assert_eq!(*change, marker.sync_change());
                assert!(
                    writes.contains(
                        &aruna_core::storage_entries::sync_revision_entry(&entry.target, change)
                            .unwrap()
                    )
                );
                assert!(
                    writes.contains(
                        &aruna_core::storage_entries::shard_manifest_entry(&entry.target, change)
                            .unwrap()
                            .unwrap()
                    )
                );
                covered.extend(marker.predecessors);
                markers.insert(marker.revision_id);
            }
            let final_id = operation.config.record_id;
            assert!(
                markers
                    .iter()
                    .filter(|id| **id != final_id)
                    .all(|id| covered.contains(id))
            );
            for (key, value) in &original {
                assert!(covered.contains(&VaultRevision::from_bytes(value).unwrap().revision_id));
                assert!(
                    writes.iter().any(
                        |(space, retired, _)| space == VAULT_RETIRED_KEYSPACE && retired == key
                    )
                );
            }
            let concurrent = head(251, Some("later"));
            let mut live: BTreeMap<_, _> = original.into_iter().collect();
            live.insert(concurrent.0.clone(), concurrent.1.clone());
            for (_, key) in &deletes {
                live.remove(key);
            }
            for (space, key, value) in writes {
                if space == VAULT_REVISION_KEYSPACE {
                    live.insert(key.clone(), value.clone());
                }
            }
            assert_eq!(live.len(), 2);
            assert_eq!(live.get(&concurrent.0), Some(&concurrent.1));
            assert!(
                VaultRevision::from_bytes(&live[&user_record_key(user(), final_id)])
                    .unwrap()
                    .payload
                    .is_none()
            );
            let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
                entries: Vec::new(),
            }));
            assert!(
                matches!(effects.first(), Some(Effect::Storage(StorageEffect::CommitTransaction { txn_id })) if *txn_id == txn())
            );
        }
    }

    #[test]
    fn bounds_key_records() {
        let (config, holders) = config_bytes();
        let change = VaultChange::PublishKey {
            key_id: "key-1".to_string(),
            public_key: [4; 32],
            has_recovery: false,
        };
        let mut operation = operation(holders[0], change);
        read_rows(&mut operation, config).unwrap();
        let full = (0..MAX_KEY_RECORDS)
            .map(|index| (Key::from(vec![index as u8]), Value::from(&[][..])))
            .collect();
        rows(&mut operation, full);
        assert_eq!(operation.finalize(), Err(AppendVaultError::TooManyKeys));
    }

    #[test]
    fn rejects_unexpected_event() {
        let (_, holders) = config_bytes();
        let mut operation = operation(holders[0], VaultChange::Delete);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: txn(),
        }));
        let effects = rows(&mut operation, Vec::new());
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::AbortTransaction { .. }))
        ));
        assert!(matches!(
            operation.finalize(),
            Err(AppendVaultError::UnexpectedEvent { .. })
        ));
    }
}
