//! Writes user and group secrets as last-writer-wins registers: the new immutable documents,
//! their sync outbox entries and the audit row commit together on this node and replicate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentClock, AdminDocumentTarget, NodeInterval};
use aruna_core::compute::SecretBytes;
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentSyncRevision, DocumentTarget,
};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, OWNER_SECRETS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::realm_vault::{RealmVaultError, seal_named};
use aruna_core::reducer::decode_reducer_state;
use aruna_core::register::{RegisterError, RegisterStamp};
use aruna_core::registers::{
    Eligibility, SecretBody, SecretDocument, SecretEntry, SecretInfo, SecretLifecycle, SecretOwner,
    SecretPart, SecretValue, current_entry, entry_prefix, owner_prefix, writer_eligibility,
};
use aruna_core::secrets::{
    SecretError, SecretName, check_description, check_rules, check_settings,
};
use aruna_core::storage_entries::{reducer_state_key, sync_revision_entry};
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord, SecretAudit};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, TxnId};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::{ChangeError, SecretChange};
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

/// Register documents read per page.
const PAGE: usize = 256;

/// One user or group secret change on this node, with its time and write ids.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerChange {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub owner: SecretOwner,
    pub actor: UserId,
    pub change: SecretChange,
    /// Whether a created user secret may be read back; refused for groups.
    pub readable: bool,
    pub now_ms: u64,
    /// Ids for the lifecycle, info and value writes this change may make.
    pub write_ids: [Ulid; 3],
    /// A parent operation's transaction to write in; the parent commits it. `None` opens and
    /// commits its own.
    pub txn_id: Option<TxnId>,
}

#[derive(Debug, Error, PartialEq)]
pub enum OwnerSecretError {
    #[error(transparent)]
    Change(#[from] ChangeError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Register(#[from] RegisterError),
    #[error(transparent)]
    Vault(#[from] RealmVaultError),
    #[error("unexpected event in state {state}: {got}")]
    UnexpectedEvent { state: &'static str, got: String },
    #[error("this secret cannot be read back")]
    NotReadable,
    #[error("operation did not finish")]
    NotFinished,
}

impl From<SecretError> for OwnerSecretError {
    fn from(error: SecretError) -> Self {
        Self::Change(ChangeError::Invalid(error))
    }
}

/// What a reader holds: the realm config clock and node history that decide writers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriterView {
    pub clock: AdminDocumentClock,
    pub intervals: Vec<NodeInterval>,
}

impl WriterView {
    pub fn counts(&self, document: &SecretDocument) -> bool {
        writer_eligibility(document, &self.intervals, &self.clock) == Eligibility::Counts
    }
}

/// Reads the writer view, then every document of one name, page by page.
#[derive(Debug, PartialEq)]
pub(crate) struct EntryReader {
    realm_id: RealmId,
    prefix: Vec<u8>,
    pub view: WriterView,
    pub documents: Vec<SecretDocument>,
    reading_view: bool,
    done: bool,
}

impl EntryReader {
    pub(crate) fn new(realm_id: RealmId, owner: &SecretOwner, name: &SecretName) -> Self {
        Self {
            realm_id,
            prefix: entry_prefix(owner, name),
            view: WriterView::default(),
            documents: Vec::new(),
            reading_view: true,
            done: false,
        }
    }

    pub(crate) fn start(&self, txn_id: Option<TxnId>) -> Effect {
        let target = AdminDocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        Effect::Storage(StorageEffect::Read {
            key_space: DOCUMENT_STATE_KEYSPACE.to_string(),
            key: reducer_state_key(&target),
            txn_id,
        })
    }

    fn page(&self, start: Option<IterStart>, txn_id: Option<TxnId>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: OWNER_SECRETS_KEYSPACE.to_string(),
            prefix: Some(self.prefix.clone().into()),
            start,
            limit: PAGE,
            txn_id,
        })
    }

    /// The next read, or `None` once the view and every document are read.
    pub(crate) fn step(
        &mut self,
        event: Event,
        txn_id: Option<TxnId>,
    ) -> Result<Option<Effect>, OwnerSecretError> {
        match (self.reading_view, event) {
            (true, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                if let Some(value) = value {
                    let state = decode_reducer_state(&value).map_err(ConversionError::from)?;
                    self.view = WriterView {
                        intervals: state.node_intervals(),
                        clock: state.clock,
                    };
                }
                self.reading_view = false;
                Ok(Some(self.page(None, txn_id)))
            }
            (
                false,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                for (_, value) in values {
                    self.documents.push(SecretDocument::from_bytes(&value)?);
                }
                match next_start_after {
                    Some(after) => Ok(Some(self.page(Some(IterStart::After(after)), txn_id))),
                    None => {
                        self.done = true;
                        Ok(None)
                    }
                }
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, other) => Err(OwnerSecretError::UnexpectedEvent {
                state: "EntryReader",
                got: format!("{other:?}"),
            }),
        }
    }

    pub(crate) fn entry(&self) -> Option<SecretEntry> {
        current_entry(&self.documents, |document| self.view.counts(document))
    }
}

#[derive(Debug, PartialEq)]
enum State {
    Init,
    StartTransaction,
    Reading,
    Write,
    Commit,
    ScheduleDrain,
    Done,
}

/// Creates, replaces, rotates or deletes one user or group secret on this node.
#[derive(Debug, PartialEq)]
pub struct OwnerSecretOperation {
    input: OwnerChange,
    reader: EntryReader,
    state: State,
    txn_id: Option<TxnId>,
    result: Option<Option<SecretEntry>>,
    output: Option<Result<Option<SecretEntry>, OwnerSecretError>>,
}

impl OwnerSecretOperation {
    pub fn new(input: OwnerChange) -> Self {
        let reader = EntryReader::new(input.realm_id, &input.owner, input.change.name());
        Self {
            input,
            reader,
            state: State::Init,
            txn_id: None,
            result: None,
            output: None,
        }
    }

    fn fail(&mut self, error: OwnerSecretError) -> Effects {
        self.output = Some(Err(error));
        self.state = State::Done;
        self.abort()
    }

    /// One document of this change with the next stamp of its register on this node.
    fn document(
        &self,
        part: SecretPart,
        id: Ulid,
        incarnation: Option<Ulid>,
        body: SecretBody,
    ) -> Result<SecretDocument, OwnerSecretError> {
        let held = (self.reader.documents.iter())
            .filter(|document| document.part() == part)
            .map(|document| document.stamp.counter)
            .max();
        Ok(SecretDocument {
            owner: self.input.owner,
            name: self.input.change.name().clone(),
            stamp: RegisterStamp::next(held, self.input.node_id, id)?,
            incarnation: incarnation.unwrap_or(id),
            config_clock: self.reader.view.clock.clone(),
            author: self.input.actor,
            written_at_ms: self.input.now_ms,
            body,
        })
    }

    fn value(
        &self,
        incarnation: Ulid,
        id: Ulid,
        fields: &aruna_core::secrets::SecretFields,
    ) -> Result<SecretDocument, OwnerSecretError> {
        let name = self.input.change.name();
        let sealed = seal_named(
            &self.input.realm_id,
            &self.input.owner,
            name,
            incarnation,
            id,
            &fields.encode()?,
        )?;
        let body = SecretBody::Value(SecretValue {
            fields: fields.names().map(str::to_string).collect(),
            sealed,
        });
        self.document(SecretPart::Value, id, Some(incarnation), body)
    }

    /// Decides the change against the current entry and builds its documents.
    fn plan(&self) -> Result<(Vec<SecretDocument>, AdminAuditAction), OwnerSecretError> {
        let current = self.reader.entry();
        let scope = self.input.owner.scope();
        let [first, second, third] = self.input.write_ids;
        if let (Some(entry), Some(expected)) = (&current, expected(&self.input.change))
            && entry.revision() != expected
        {
            return Err(ChangeError::Stale.into());
        }
        let info_body = |description: &str, settings, uses| {
            SecretBody::Info(SecretInfo {
                description: description.to_string(),
                settings,
                uses,
            })
        };
        Ok(match (&self.input.change, &current) {
            (SecretChange::Create { .. }, Some(_)) => return Err(ChangeError::Exists.into()),
            (
                SecretChange::Create {
                    kind,
                    description,
                    settings,
                    uses,
                    fields,
                    ..
                },
                None,
            ) => {
                if self.input.readable && !matches!(self.input.owner, SecretOwner::User(_)) {
                    return Err(SecretError::Rule.into());
                }
                check_description(description)?;
                check_settings(kind, settings)?;
                check_rules(scope, kind, uses)?;
                fields.check(kind)?;
                let created = SecretLifecycle::Created {
                    kind: kind.clone(),
                    readable: self.input.readable,
                };
                let lifecycle = self.document(
                    SecretPart::Lifecycle,
                    first,
                    None,
                    SecretBody::Lifecycle(created),
                )?;
                let info_doc = info_body(description, settings.clone(), uses.clone());
                let info = self.document(SecretPart::Info, second, Some(first), info_doc)?;
                let value = self.value(first, third, fields)?;
                (
                    vec![lifecycle, info, value],
                    AdminAuditAction::SecretCreated,
                )
            }
            (_, None) => return Err(ChangeError::NotFound.into()),
            (
                SecretChange::Replace {
                    description,
                    settings,
                    uses,
                    ..
                },
                Some(entry),
            ) => {
                let kind = entry.kind().ok_or(ChangeError::NotFound)?;
                check_description(description)?;
                check_settings(kind, settings)?;
                check_rules(scope, kind, uses)?;
                let incarnation = Some(entry.lifecycle.stamp.write_id);
                let body = info_body(description, settings.clone(), uses.clone());
                let info = self.document(SecretPart::Info, first, incarnation, body)?;
                (vec![info], AdminAuditAction::SecretReplaced)
            }
            (SecretChange::Rotate { fields, .. }, Some(entry)) => {
                fields.check(entry.kind().ok_or(ChangeError::NotFound)?)?;
                let value = self.value(entry.lifecycle.stamp.write_id, first, fields)?;
                (vec![value], AdminAuditAction::SecretRotated)
            }
            (SecretChange::Delete { .. }, Some(entry)) => {
                let incarnation = Some(entry.lifecycle.stamp.write_id);
                let body = SecretBody::Lifecycle(SecretLifecycle::Deleted);
                let delete = self.document(SecretPart::Lifecycle, first, incarnation, body)?;
                (vec![delete], AdminAuditAction::SecretDeleted)
            }
        })
    }

    fn write(&mut self) -> Result<Effects, OwnerSecretError> {
        let (documents, action) = self.plan()?;
        let mut writes = Vec::new();
        for document in &documents {
            let target = DocumentTarget::OwnerSecret {
                realm_id: self.input.realm_id,
                owner: document.owner,
                name: document.name.clone(),
                part: document.part(),
                write_id: document.stamp.write_id,
            };
            let bytes = document.to_bytes()?;
            let change = DocumentChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: 1,
                    event_id: document.stamp.write_id,
                    actor: self.input.node_id,
                    updated_at_ms: self.input.now_ms,
                },
                kind: DocumentChangeKind::Upsert,
                placement: PlacementRef::NIL,
            };
            let outbox = new_identified_record(
                change.current.event_id,
                self.input.node_id,
                target.clone(),
                Vec::new(),
                DocumentOutboxEvent::Upsert {
                    bytes: bytes.clone(),
                    change,
                },
                PlacementRef::NIL,
                false,
            );
            writes.push((
                target.storage_keyspace().to_string(),
                target.storage_key(),
                bytes.into(),
            ));
            writes.push(sync_revision_entry(&target, &change)?);
            writes.push(outbox_write_entry(&outbox).map_err(ConversionError::from)?);
        }
        let incarnation = documents.first().map(|document| document.incarnation);
        let audit = AdminAuditRecord::for_secret(
            action,
            self.input.actor,
            SecretAudit {
                scope: self.input.owner.scope(),
                name: self.input.change.name().clone(),
                incarnation,
                write_id: documents.last().map(|document| document.stamp.write_id),
            },
        );
        writes.extend(audit.entries()?);
        self.reader.documents.extend(documents);
        self.result = Some(self.reader.entry());
        self.state = State::Write;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })])
    }
}

fn expected(change: &SecretChange) -> Option<&str> {
    match change {
        SecretChange::Create { .. } => None,
        SecretChange::Replace { expected, .. }
        | SecretChange::Rotate { expected, .. }
        | SecretChange::Delete { expected, .. } => expected.as_deref(),
    }
}

impl Operation for OwnerSecretOperation {
    type Output = Option<SecretEntry>;
    type Error = OwnerSecretError;

    fn start(&mut self) -> Effects {
        if let Some(txn_id) = self.input.txn_id {
            self.txn_id = Some(txn_id);
            self.state = State::Reading;
            return smallvec![self.reader.start(self.txn_id)];
        }
        self.state = State::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            if self.state == State::Commit {
                self.txn_id = None;
            }
            return self.fail(error.into());
        }
        match (&self.state, event) {
            (
                State::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = State::Reading;
                smallvec![self.reader.start(self.txn_id)]
            }
            (State::Reading, event) => match self.reader.step(event, self.txn_id) {
                Ok(Some(effect)) => smallvec![effect],
                Ok(None) => self.write().unwrap_or_else(|error| self.fail(error)),
                Err(error) => self.fail(error),
            },
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. }))
                if self.input.txn_id.is_some() =>
            {
                // The parent commits its transaction and schedules the outbox drain.
                self.txn_id = None;
                self.state = State::Done;
                self.output = Some(Ok(self.result.take().flatten()));
                smallvec![]
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(OwnerSecretError::NotFinished);
                };
                self.state = State::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = State::ScheduleDrain;
                smallvec![schedule_drain_effect()]
            }
            (State::ScheduleDrain, Event::Task(TaskEvent::TimerScheduled { .. })) => {
                self.state = State::Done;
                self.output = Some(Ok(self.result.take().flatten()));
                smallvec![]
            }
            (State::ScheduleDrain, Event::Task(TaskEvent::Error { message, .. })) => {
                warn!(error = %message, "Outbox drain not scheduled; the durable outbox retries");
                self.state = State::Done;
                self.output = Some(Ok(self.result.take().flatten()));
                smallvec![]
            }
            (State::Init | State::Done, _) => smallvec![],
            (_, event) => {
                let got = format!("{event:?}");
                self.fail(OwnerSecretError::UnexpectedEvent {
                    state: "OwnerSecret",
                    got,
                })
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(OwnerSecretError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        let own = self.input.txn_id.is_none();
        match self.txn_id.take() {
            Some(txn_id) if own => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

/// Lists the current entries of one owner by name, or reads one name, from this node's view.
#[derive(Debug, PartialEq)]
pub struct OwnerSecretsOperation {
    realm_id: RealmId,
    prefix: Vec<u8>,
    start: Option<IterStart>,
    limit: usize,
    view: Option<WriterView>,
    documents: Vec<SecretDocument>,
    output: Option<Result<(Vec<SecretEntry>, bool), OwnerSecretError>>,
}

impl OwnerSecretsOperation {
    /// The entries after `after`; `limit` is clamped to 1..=200.
    pub fn new(
        realm_id: RealmId,
        owner: SecretOwner,
        after: Option<&SecretName>,
        limit: usize,
    ) -> Self {
        // Part tags are 1 to 3, so 0xff sorts after every document of the cursor name.
        let start = after
            .map(|name| IterStart::After([entry_prefix(&owner, name), vec![0xff]].concat().into()));
        Self {
            realm_id,
            prefix: owner_prefix(&owner),
            start,
            limit: limit.clamp(1, 200),
            view: None,
            documents: Vec::new(),
            output: None,
        }
    }

    pub fn named(realm_id: RealmId, owner: SecretOwner, name: &SecretName) -> Self {
        Self {
            prefix: entry_prefix(&owner, name),
            ..Self::new(realm_id, owner, None, 1)
        }
    }

    fn page(&self, start: Option<IterStart>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: OWNER_SECRETS_KEYSPACE.to_string(),
            prefix: Some(self.prefix.clone().into()),
            start,
            limit: PAGE,
            txn_id: None,
        })]
    }

    fn names(&self) -> Vec<&SecretName> {
        let mut names: Vec<&SecretName> = self
            .documents
            .iter()
            .map(|document| &document.name)
            .collect();
        names.dedup();
        names
    }

    /// The selected entries of every name whose documents are all read: all names on the last
    /// page, all but the newest one before. Deleted names and ignored writers select nothing.
    fn entries(&self, last_page: bool) -> Vec<SecretEntry> {
        let view = self.view.clone().unwrap_or_default();
        let mut names = self.names();
        if !last_page {
            names.pop();
        }
        (names.into_iter())
            .filter_map(|name| {
                let documents = (self.documents.iter()).filter(|document| document.name == *name);
                current_entry(documents, |document| view.counts(document))
            })
            .collect()
    }

    /// Ends once more than `limit` entries are selected or nothing is left, so a page of
    /// deleted names never ends a listing early.
    fn finish(&mut self, mut entries: Vec<SecretEntry>) {
        let more = entries.len() > self.limit;
        entries.truncate(self.limit);
        self.output = Some(Ok((entries, more)));
    }
}

impl Operation for OwnerSecretsOperation {
    /// The entries and whether more names follow.
    type Output = (Vec<SecretEntry>, bool);
    type Error = OwnerSecretError;

    fn start(&mut self) -> Effects {
        let reader = EntryReader {
            realm_id: self.realm_id,
            prefix: Vec::new(),
            view: WriterView::default(),
            documents: Vec::new(),
            reading_view: true,
            done: false,
        };
        smallvec![reader.start(None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.output.is_some() {
            return smallvec![];
        }
        let result: Result<(), OwnerSecretError> = match (self.view.is_none(), event) {
            (true, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let state = value.map(|value| decode_reducer_state(&value)).transpose();
                match state.map_err(ConversionError::from) {
                    Ok(state) => {
                        self.view =
                            Some(state.map_or_else(WriterView::default, |state| WriterView {
                                intervals: state.node_intervals(),
                                clock: state.clock,
                            }));
                        return self.page(self.start.clone());
                    }
                    Err(error) => Err(OwnerSecretError::from(error)),
                }
            }
            (
                false,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                let decoded: Result<Vec<_>, _> = values
                    .iter()
                    .map(|(_, value)| SecretDocument::from_bytes(value))
                    .collect();
                match decoded {
                    Ok(documents) => {
                        self.documents.extend(documents);
                        match next_start_after {
                            Some(after) => {
                                let entries = self.entries(false);
                                if entries.len() > self.limit {
                                    self.finish(entries);
                                    return smallvec![];
                                }
                                return self.page(Some(IterStart::After(after)));
                            }
                            None => {
                                let entries = self.entries(true);
                                self.finish(entries);
                                return smallvec![];
                            }
                        }
                    }
                    Err(error) => Err(error.into()),
                }
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, other) => Err(OwnerSecretError::UnexpectedEvent {
                state: "OwnerSecrets",
                got: format!("{other:?}"),
            }),
        };
        if let Err(error) = result {
            self.output = Some(Err(error));
        }
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(OwnerSecretError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Selects a readable user secret on a management node and writes the `SecretRead` audit row
/// before anyone opens the value; a failed audit write ends it without a value.
#[derive(Debug, PartialEq)]
pub struct ReadbackOperation {
    reader: EntryReader,
    owner: SecretOwner,
    actor: UserId,
    entry: Option<SecretEntry>,
    output: Option<Result<SecretEntry, OwnerSecretError>>,
}

impl ReadbackOperation {
    pub fn new(realm_id: RealmId, owner: SecretOwner, name: SecretName, actor: UserId) -> Self {
        Self {
            reader: EntryReader::new(realm_id, &owner, &name),
            owner,
            actor,
            entry: None,
            output: None,
        }
    }

    fn audit(&mut self) -> Result<Effects, OwnerSecretError> {
        let entry = self.reader.entry().filter(SecretEntry::complete);
        let entry = entry.ok_or(ChangeError::NotFound)?;
        let readable = entry.readable() && self.owner == SecretOwner::User(self.actor);
        if !readable {
            return Err(OwnerSecretError::NotReadable);
        }
        let audit = AdminAuditRecord::for_secret(
            AdminAuditAction::SecretRead,
            self.actor,
            SecretAudit {
                scope: self.owner.scope(),
                name: entry.lifecycle.name.clone(),
                incarnation: Some(entry.lifecycle.stamp.write_id),
                write_id: entry.value.as_ref().map(|value| value.stamp.write_id),
            },
        );
        self.entry = Some(entry);
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: audit.entries()?,
            txn_id: None,
        })])
    }
}

impl Operation for ReadbackOperation {
    type Output = SecretEntry;
    type Error = OwnerSecretError;

    fn start(&mut self) -> Effects {
        smallvec![self.reader.start(None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.output.is_some() {
            return smallvec![];
        }
        let result = match (&self.entry, event) {
            (None, event) => match self.reader.step(event, None) {
                Ok(Some(effect)) => return smallvec![effect],
                Ok(None) => match self.audit() {
                    Ok(effects) => return effects,
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            },
            (Some(entry), Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                Ok(entry.clone())
            }
            (Some(_), Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (Some(_), other) => Err(OwnerSecretError::UnexpectedEvent {
                state: "Readback",
                got: format!("{other:?}"),
            }),
        };
        self.output = Some(result);
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(OwnerSecretError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Opens the value of a readable user secret for its owner on a management node.
pub fn open_value(
    key: &aruna_core::realm_vault::RealmSealKey,
    realm_id: &RealmId,
    entry: &SecretEntry,
) -> Result<SecretBytes, OwnerSecretError> {
    let value = entry.value.as_ref().ok_or(ChangeError::NotFound)?;
    let SecretBody::Value(sealed) = &value.body else {
        return Err(ChangeError::NotFound.into());
    };
    Ok(key.open_named(
        realm_id,
        &value.owner,
        &value.name,
        value.incarnation,
        value.stamp.write_id,
        &sealed.sealed,
    )?)
}
