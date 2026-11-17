//! Replicates this node's info document and clears the revision it owed to replication.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::keyspaces::COMPUTE_DEPARTURE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Key, Value};
use thiserror::Error;

use crate::node::info_write::PUBLISH_OWED_KEY;
use crate::realm::mutate_placement::node_kind;
use crate::sync::replicate_documents::{
    ReplicateDocumentsConfig, ReplicateDocumentsError, ReplicateDocumentsOperation,
};

#[derive(Debug, Error, PartialEq)]
pub enum PublishInfoError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("failed to read realm config for node info: the config is missing")]
    ConfigMissing,
    #[error("node info replication failed: {0}")]
    Replication(String),
    #[error("clearing the owed node info revision failed: {0}")]
    Settle(String),
}

/// An owed revision; an unreadable value counts as owed.
fn owed_revision(value: Option<&Value>) -> Option<u64> {
    value.map(|bytes| <[u8; 8]>::try_from(bytes.as_ref()).map_or(0, u64::from_be_bytes))
}

fn owed_row() -> (String, Key) {
    let key = Key::from(PUBLISH_OWED_KEY.to_vec());
    (COMPUTE_DEPARTURE_KEYSPACE.to_string(), key)
}

fn replicated(result: Result<(), ReplicateDocumentsError>) -> Event {
    let result = result.map_err(|error| error.to_string());
    Event::SubOperation(SubOperationEvent::DocumentSyncResult { result })
}

/// A lost conflict means another revision was committed; it stays owed for the next tick.
fn settled(result: Result<(), PublishInfoError>) -> Event {
    let result = match result {
        Err(PublishInfoError::Io(IoError::Conflict)) => Ok(()),
        result => result.map_err(|error| error.to_string()),
    };
    Event::SubOperation(SubOperationEvent::DocumentSyncResult { result })
}

#[derive(Debug, PartialEq)]
enum Phase {
    Read,
    Replicate,
    Settle,
}

type Turn = Result<Next<()>, PublishInfoError>;

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
            phase: Phase::Read,
        }
    }

    fn read(&mut self, owed: Option<Value>, config: Option<Value>) -> Turn {
        self.owed = owed_revision(owed.as_ref());
        if self.only_owed && self.owed.is_none() {
            return Ok(Next::Done(()));
        }
        let config = config.ok_or(PublishInfoError::ConfigMissing)?;
        let config = RealmConfigDocument::from_bytes(&config)?;
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
        let child = boxed_suboperation(replicate, replicated);
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn settle(&mut self) -> Next<()> {
        let Some(published) = self.owed else {
            return Next::Done(());
        };
        self.phase = Phase::Settle;
        let child = boxed_suboperation(SettleOwedOperation { published }, settled);
        Next::Call(IoCall::Child(child))
    }
}

impl Operation for PublishInfoOperation {
    type Output = ();
    type Error = PublishInfoError;

    fn first(&mut self) -> Turn {
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        let config = (config.storage_keyspace().to_string(), config.storage_key());
        Ok(Next::Call(IoCall::Read(vec![owed_row(), config])))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        let synced = |result: &IoResult| match result {
            IoResult::Event(Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                result,
            })) => Some(result.clone()),
            _ => None,
        };
        match (&self.phase, synced(&result), result) {
            (Phase::Read, _, IoResult::Values(values)) => {
                let mut values = values.into_iter();
                self.read(values.next().flatten(), values.next().flatten())
            }
            (Phase::Replicate, Some(Ok(())), _) => Ok(self.settle()),
            (Phase::Replicate, Some(Err(error)), _) => Err(PublishInfoError::Replication(error)),
            (Phase::Settle, Some(Ok(())), _) => Ok(Next::Done(())),
            (Phase::Settle, Some(Err(error)), _) => Err(PublishInfoError::Settle(error)),
            _ => Err(IoError::Unexpected {
                expected: "the answer to the node info publication step",
                got: "another result",
            }
            .into()),
        }
    }
}

/// Clears the owed revision `published` in one transaction, unless a newer one is owed now.
#[derive(Debug, PartialEq)]
struct SettleOwedOperation {
    published: u64,
}

impl Operation for SettleOwedOperation {
    type Output = ();
    type Error = PublishInfoError;

    fn first(&mut self) -> Turn {
        Ok(Next::Call(IoCall::Read(vec![owed_row()])))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match result {
            IoResult::Values(values) => {
                let owed = values.into_iter().next().flatten();
                if owed_revision(owed.as_ref()).is_some_and(|owed| owed <= self.published) {
                    return Ok(Next::Call(IoCall::Delete(vec![owed_row()])));
                }
                Ok(Next::Done(()))
            }
            IoResult::Done => Ok(Next::Done(())),
            _ => Err(IoError::Unexpected {
                expected: "the owed revision or its delete",
                got: "another result",
            }
            .into()),
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod state_machine_tests {
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use ulid::Ulid;

    use super::*;

    const REALM: RealmId = RealmId([3; 32]);

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn config(kind: RealmNodeKind) -> Value {
        let mut config = RealmConfigDocument::default_for_realm(REALM, Vec::new());
        config.ensure_node(node(), kind);
        Value::from(postcard::to_allocvec(&config).unwrap())
    }

    fn device() -> RealmNodeKind {
        RealmNodeKind::User {
            owner: aruna_core::UserId::new(Ulid::from_parts(2, 2), REALM),
        }
    }

    fn revision(owed: Option<u64>) -> Option<Value> {
        owed.map(|owed| Value::from(owed.to_be_bytes().to_vec()))
    }

    fn read(owed: Option<u64>, config: Option<Value>) -> IoResult {
        IoResult::Values(vec![revision(owed), config])
    }

    fn synced(result: Result<(), String>) -> IoResult {
        IoResult::Event(Event::SubOperation(SubOperationEvent::DocumentSyncResult {
            result,
        }))
    }

    fn settle(published: u64) -> Turn {
        let child = boxed_suboperation(SettleOwedOperation { published }, settled);
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn done() -> Turn {
        Ok(Next::Done(()))
    }

    #[test]
    fn replicates_then_settles() {
        let mut operation = PublishInfoOperation::new(REALM, node(), false);
        let config_target = DocumentTarget::RealmConfig { realm_id: REALM };
        let config_row = (
            config_target.storage_keyspace().to_string(),
            config_target.storage_key(),
        );
        let first = Ok(Next::Call(IoCall::Read(vec![owed_row(), config_row])));
        assert_eq!(operation.first(), first);
        let answer = operation.next(read(Some(4), Some(config(RealmNodeKind::Server))));
        assert!(matches!(answer, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(operation.next(synced(Ok(()))), settle(4));
        assert_eq!(operation.next(synced(Ok(()))), done());
    }

    #[test]
    fn settle_clears_published() {
        let mut settle = SettleOwedOperation { published: 4 };
        assert!(settle.transaction());
        let read = Ok(Next::Call(IoCall::Read(vec![owed_row()])));
        assert_eq!(settle.first(), read);
        let delete = Ok(Next::Call(IoCall::Delete(vec![owed_row()])));
        assert_eq!(
            settle.next(IoResult::Values(vec![revision(Some(4))])),
            delete
        );
        assert_eq!(settle.next(IoResult::Done), done());
        // A revision committed meanwhile stays owed: nothing is deleted.
        let mut newer = SettleOwedOperation { published: 4 };
        assert_eq!(
            newer.next(IoResult::Values(vec![revision(Some(5))])),
            done()
        );
        // A lost commit conflict keeps the newer revision owed and still succeeds.
        let conflict = settled(Err(IoError::Conflict.into()));
        let ok = Event::SubOperation(SubOperationEvent::DocumentSyncResult { result: Ok(()) });
        assert_eq!(conflict, ok);
    }

    #[test]
    fn skips_without_work() {
        // Only an owed revision publishes on a heartbeat.
        let mut heartbeat = PublishInfoOperation::new(REALM, node(), true);
        let answer = heartbeat.next(read(None, Some(config(RealmNodeKind::Server))));
        assert_eq!(answer, done());
        // A device replicates nothing and only settles what it owed.
        let mut local = PublishInfoOperation::new(REALM, node(), false);
        assert_eq!(local.next(read(Some(2), Some(config(device())))), settle(2));
        let mut nothing = PublishInfoOperation::new(REALM, node(), false);
        assert_eq!(nothing.next(read(None, Some(config(device())))), done());
    }

    #[test]
    fn failures_end_publication() {
        let mut missing = PublishInfoOperation::new(REALM, node(), false);
        let answer = missing.next(read(Some(1), None));
        assert_eq!(answer, Err(PublishInfoError::ConfigMissing));
        let mut refused = PublishInfoOperation::new(REALM, node(), false);
        refused
            .next(read(Some(1), Some(config(RealmNodeKind::Server))))
            .unwrap();
        let error = PublishInfoError::Replication("offline".to_string());
        assert_eq!(refused.next(synced(Err("offline".to_string()))), Err(error));
        let mut stray = PublishInfoOperation::new(REALM, node(), false);
        assert!(matches!(
            stray.next(synced(Ok(()))),
            Err(PublishInfoError::Io(_))
        ));
    }
}
