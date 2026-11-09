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
use aruna_core::secrets::{SecretError, SecretName, check_rules, check_settings};
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
