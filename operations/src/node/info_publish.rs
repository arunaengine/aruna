//! Replicates this node's info document and clears the revision it owed to replication.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::COMPUTE_DEPARTURE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;

use crate::node::info_write::PUBLISH_OWED_KEY;
use crate::realm::mutate_placement::node_kind;
use crate::sync::replicate_documents::{ReplicateDocumentsConfig, ReplicateDocumentsOperation};

#[derive(Debug, Error, PartialEq)]
pub enum PublishInfoError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("failed to read realm config for node info: the config is missing")]
    ConfigMissing,
    #[error("node info replication failed: {0}")]
    Replication(String),
    #[error("unexpected event while {0}")]
    UnexpectedEvent(&'static str),
    #[error("the node info publication has not finished")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum Phase {
    Idle,
    Read,
    Replicate,
    Begin,
    Settle(TxnId),
    Delete(TxnId),
    Commit,
    Done(Result<(), PublishInfoError>),
}

impl Phase {
    fn label(&self) -> &'static str {
        match self {
            Phase::Idle => "idle",
            Phase::Read => "reading the owed revision",
            Phase::Replicate => "replicating the node info",
            Phase::Begin => "starting the settle transaction",
            Phase::Settle(_) => "rereading the owed revision",
            Phase::Delete(_) => "clearing the owed revision",
            Phase::Commit => "committing the settled revision",
            Phase::Done(_) => "done",
        }
    }
}

/// An owed revision; an unreadable value counts as owed.
fn owed_revision(value: Option<&Value>) -> Option<u64> {
    value.map(|bytes| <[u8; 8]>::try_from(bytes.as_ref()).map_or(0, u64::from_be_bytes))
}

fn owed_key() -> Key {
    Key::from(PUBLISH_OWED_KEY.to_vec())
}

/// Hands the info document to replication over the shared realm topic, then clears the owed
/// revision it published unless a newer one was committed meanwhile. A device's document stays
/// local: it belongs to no sync topic, so an outbox row for it could never be published.
#[derive(Debug, PartialEq)]
pub(crate) struct PublishInfoOperation {
    realm_id: RealmId,
    node_id: NodeId,
    /// Publish only while a committed revision is owed, as the heartbeat does.
    only_owed: bool,
    owed: Option<u64>,
    phase: Phase,
}

impl PublishInfoOperation {
    pub(crate) fn new(realm_id: RealmId, node_id: NodeId, only_owed: bool) -> Self {
        Self {
            realm_id,
            node_id,
            only_owed,
            owed: None,
            phase: Phase::Idle,
        }
    }

    fn finish(&mut self, result: Result<(), PublishInfoError>) -> Effects {
        self.phase = Phase::Done(result);
        smallvec![]
    }

    fn abort_with(&mut self, txn_id: TxnId, error: PublishInfoError) -> Effects {
        self.phase = Phase::Done(Err(error));
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn read(&mut self, values: &[(Key, Option<Value>)]) -> Result<Effects, PublishInfoError> {
        let [(_, owed), (_, config)] = values else {
            return Err(PublishInfoError::UnexpectedEvent(self.phase.label()));
        };
        self.owed = owed_revision(owed.as_ref());
        if self.only_owed && self.owed.is_none() {
            return Ok(self.finish(Ok(())));
        }
        let config = config.as_ref().ok_or(PublishInfoError::ConfigMissing)?;
        let config = RealmConfigDocument::from_bytes(config)?;
        if node_kind(&config, self.node_id).is_some_and(|kind| !kind.is_sync_eligible()) {
            return Ok(self.settle());
        }
        self.phase = Phase::Replicate;
        let replicate = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id: self.realm_id,
            local_node_id: self.node_id,
            excluded_peers: Vec::new(),
            documents: vec![DocumentTarget::NodeInfo {
                realm_id: self.realm_id,
                node_id: self.node_id,
            }],
            // Shared-topic genesis is bootstrapped by publish_core_documents; explicit
            // publishes and periodic heartbeats only publish into it.
            allow_genesis: false,
        })
        .strict();
        let replicate = boxed_suboperation(replicate, |result| {
            Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                result: result.map_err(|error| error.to_string()),
            })
        });
        Ok(smallvec![Effect::SubOperation(replicate)])
    }

    fn settle(&mut self) -> Effects {
        if self.owed.is_none() {
            return self.finish(Ok(()));
        }
        self.phase = Phase::Begin;
        smallvec![Effect::Storage(StorageEffect::StartTransaction { read: false })]
    }

    fn reread(&mut self, txn_id: TxnId, value: Option<Value>) -> Effects {
        let published = self.owed.unwrap_or_default();
        if owed_revision(value.as_ref()).is_some_and(|owed| owed <= published) {
            self.phase = Phase::Delete(txn_id);
            return smallvec![Effect::Storage(StorageEffect::Delete {
                key_space: COMPUTE_DEPARTURE_KEYSPACE.to_string(),
                key: owed_key(),
                txn_id: Some(txn_id),
            })];
        }
        self.commit(txn_id)
    }

    fn commit(&mut self, txn_id: TxnId) -> Effects {
        self.phase = Phase::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn advance(&mut self, event: Event) -> Effects {
        let unexpected = PublishInfoError::UnexpectedEvent(self.phase.label());
        match (std::mem::replace(&mut self.phase, Phase::Idle), event) {
            (Phase::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.phase = Phase::Read;
                match self.read(&values) {
                    Ok(effects) => effects,
                    Err(error) => self.finish(Err(error)),
                }
            }
            (
                Phase::Replicate,
                Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }),
            ) => match result {
                Ok(()) => self.settle(),
                Err(error) => self.finish(Err(PublishInfoError::Replication(error))),
            },
            (Phase::Begin, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.phase = Phase::Settle(txn_id);
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: COMPUTE_DEPARTURE_KEYSPACE.to_string(),
                    key: owed_key(),
                    txn_id: Some(txn_id),
                })]
            }
            (Phase::Settle(txn_id), Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.reread(txn_id, value)
            }
            (Phase::Delete(txn_id), Event::Storage(StorageEvent::DeleteResult { .. })) => {
                self.commit(txn_id)
            }
            (Phase::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.finish(Ok(()))
            }
            // A conflict means another revision was committed; it stays owed for the next tick.
            (
                Phase::Commit,
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }),
            ) => self.finish(Ok(())),
            (
                Phase::Read | Phase::Begin | Phase::Commit,
                Event::Storage(StorageEvent::Error { error }),
            ) => self.finish(Err(error.into())),
            (
                Phase::Settle(txn_id) | Phase::Delete(txn_id),
                Event::Storage(StorageEvent::Error { error }),
            ) => self.abort_with(txn_id, error.into()),
            (Phase::Settle(txn_id) | Phase::Delete(txn_id), _) => {
                self.abort_with(txn_id, unexpected)
            }
            _ => self.finish(Err(unexpected)),
        }
    }
}

impl Operation for PublishInfoOperation {
    type Output = ();
    type Error = PublishInfoError;

    fn start(&mut self) -> Effects {
        self.phase = Phase::Read;
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (COMPUTE_DEPARTURE_KEYSPACE.to_string(), owed_key()),
                (config.storage_keyspace().to_string(), config.storage_key()),
            ],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.advance(event)
    }

    fn is_complete(&self) -> bool {
        matches!(self.phase, Phase::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.phase {
            Phase::Done(result) => result,
            _ => Err(PublishInfoError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.phase {
            Phase::Settle(txn_id) | Phase::Delete(txn_id) => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

