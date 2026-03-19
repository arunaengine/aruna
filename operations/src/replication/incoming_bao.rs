use crate::replication::error::ReplicationError;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::BLOB_LOCATION_DB;
use aruna_core::operation::Operation;
use aruna_core::structs::{BackendLocation, NegotiationResult};
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncomingBaoState {
    Init,
    NegotiateReplication,
    HandleReplication,
    StartTransaction,
    CreateBlob,
    CreatePermissions,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum IncomingBaoError {
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: IncomingBaoState,
        expected: IncomingBaoState,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: IncomingBaoState,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct IncomingBaoOperation {
    state: IncomingBaoState,
    stream_id: Ulid,
    node_id: NodeId,
    location: Option<BackendLocation>,
    txn_id: Option<TxnId>,
    result: Result<(), ReplicationError>,
}

impl IncomingBaoOperation {
    pub fn new(stream_id: Ulid, node_id: NodeId) -> Self {
        Self {
            state: IncomingBaoState::Init,
            stream_id,
            node_id,
            location: None,
            txn_id: None,
            result: Ok(()),
        }
    }

    fn handle_error(&mut self, error: ReplicationError) -> Effects {
        self.state = IncomingBaoState::Error;
        self.result = Err(error);
        self.abort()
    }

    fn handle_init(&mut self) -> Effects {
        self.state = IncomingBaoState::NegotiateReplication;
        smallvec![Effect::Blob(BlobEffect::NegotiateIncoming {
            stream_id: self.stream_id
        })]
    }

    fn handle_negotiation_result(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::NegotiationFinished(result)) = event else {
            return self.handle_error(
                IncomingBaoError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Blob(BlobEvent::NegotiationResult)",
                    received: event,
                }
                .into(),
            );
        };

        match result {
            NegotiationResult::Accepted(replication_id) => {
                self.state = IncomingBaoState::HandleReplication;
                smallvec![Effect::Blob(BlobEffect::HandleReplication {
                    replication_id,
                    stream_id: self.stream_id
                })]
            }
            NegotiationResult::Rejected(reason) => {
                self.handle_error(ReplicationError::ReplicationRejected(reason))
            }
        }
    }

    fn handle_replication_result(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ReplicationFinished { location }) = event else {
            return self.handle_error(
                IncomingBaoError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Blob(BlobEvent::ReplicationResult)",
                    received: event,
                }
                .into(),
            );
        };

        self.location = Some(location);
        self.state = IncomingBaoState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }
    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.handle_error(
                IncomingBaoError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Storage(StorageEvent::TransactionStarted)",
                    received: event,
                }
                .into(),
            );
        };
        self.txn_id = Some(txn_id);

        let Some(location) = &self.location else {
            return self.handle_error(ReplicationError::NoSuchKey);
        };
        let Some(blake3) = location.get_blake3() else {
            return self.handle_error(ReplicationError::HashMissing);
        };
        let bytes = match location.to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => return self.handle_error(err.into()),
        };

        self.state = IncomingBaoState::CreateBlob;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: BLOB_LOCATION_DB.to_string(),
            key: blake3.as_slice().into(),
            value: bytes.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_blob_created(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.handle_error(
                IncomingBaoError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Storage(StorageEvent::WriteResult)",
                    received: event,
                }
                .into(),
            );
        };

        //TODO: Create permissions

        let Some(txn_id) = self.txn_id else {
            return self.handle_error(ReplicationError::TransactionMissing);
        };
        self.state = IncomingBaoState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_permissions_created(&mut self, _event: Event) -> Effects {
        //TODO
        smallvec![]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.handle_error(
                IncomingBaoError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                    received: event,
                }
                .into(),
            );
        };

        self.txn_id = None;
        self.state = IncomingBaoState::Finish;
        smallvec![]
    }
}

impl Operation for IncomingBaoOperation {
    type Output = Result<(), ReplicationError>;
    type Error = ReplicationError;

    fn start(&mut self) -> Effects {
        if self.state != IncomingBaoState::Init {
            return self.handle_error(
                IncomingBaoError::InvalidState {
                    current: self.state.clone(),
                    expected: IncomingBaoState::Init,
                }
                .into(),
            );
        }

        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            IncomingBaoState::Init => self.handle_init(),
            IncomingBaoState::NegotiateReplication => self.handle_negotiation_result(event),
            IncomingBaoState::HandleReplication => self.handle_replication_result(event),
            IncomingBaoState::StartTransaction => self.handle_transaction_started(event),
            IncomingBaoState::CreateBlob => self.handle_blob_created(event),
            IncomingBaoState::CreatePermissions => self.handle_permissions_created(event),
            IncomingBaoState::CommitTransaction => self.handle_transaction_committed(event),
            IncomingBaoState::Finish => smallvec![],
            IncomingBaoState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            IncomingBaoState::Finish | IncomingBaoState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if IncomingBaoState::Error == self.state {
            self.result?;
            return Err(ReplicationError::ReplicationFailed);
        }
        Ok(self.result)
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::keyspaces::BLOB_LOCATION_DB;
    use aruna_core::structs::{BackendLocation, NegotiationResult};
    use aruna_core::types::Key;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn make_location() -> BackendLocation {
        let ulid = Ulid::new();
        let mut hashes = HashMap::new();
        hashes.insert("blake3".to_string(), vec![0u8; 32]);

        BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: format!("obj/key_{ulid}"),
            ulid,
            compressed: false,
            encrypted: false,
            created_by: Ulid::new(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 1024,
            hashes,
        }
    }

    #[tokio::test]
    async fn test_incoming_bao() {
        let node_key = iroh::SecretKey::from_str(
            "98f15bd901074f210926f8dfb2e3f179e858bf15b49ab8faefe23cea0dcdd9ac",
        )
        .unwrap();
        let node_id = node_key.public();
        let stream_id = Ulid::new();
        let mut op = IncomingBaoOperation::new(stream_id, node_id);

        // 1. Start -> Should transition to NegotiateReplication and emit Blob::NegotiateIncoming
        let effects = op.start();
        assert_eq!(op.state, IncomingBaoState::NegotiateReplication);
        assert_eq!(effects.len(), 1);
        assert_eq!(
            effects[0],
            Effect::Blob(BlobEffect::NegotiateIncoming { stream_id })
        );

        // 2. Feed NegotiationFinished(Accepted) -> Should transition to HandleReplication
        let replication_id = Ulid::new();
        let event = Event::Blob(BlobEvent::NegotiationFinished(NegotiationResult::Accepted(
            replication_id,
        )));
        let effects = op.step(event);
        assert_eq!(op.state, IncomingBaoState::HandleReplication);
        assert_eq!(effects.len(), 1);
        assert_eq!(
            effects[0],
            Effect::Blob(BlobEffect::HandleReplication {
                replication_id,
                stream_id,
            })
        );

        // 3. Feed ReplicationFinished -> Should transition to StartTransaction
        let location = make_location();
        let event = Event::Blob(BlobEvent::ReplicationFinished {
            location: location.clone(),
        });
        let effects = op.step(event);
        assert_eq!(op.state, IncomingBaoState::StartTransaction);
        assert_eq!(effects.len(), 1);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::StartTransaction { read: false })
        );

        // 4. Feed TransactionStarted -> Should transition to CreateBlob and emit Storage::Write
        let txn_id = Ulid::new();
        let event = Event::Storage(StorageEvent::TransactionStarted { txn_id });
        let effects = op.step(event);
        assert_eq!(op.state, IncomingBaoState::CreateBlob);
        assert_eq!(effects.len(), 1);
        let blake3 = location.get_blake3().expect("blake3 hash set");
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::Write {
                key_space: BLOB_LOCATION_DB.to_string(),
                key: Key::from(blake3.as_slice().to_vec()),
                value: location.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
        );

        // 5. Feed WriteResult -> Should transition to CommitTransaction
        let event = Event::Storage(StorageEvent::WriteResult {
            key: Key::from(blake3.as_slice().to_vec()),
        });
        let effects = op.step(event);
        assert_eq!(op.state, IncomingBaoState::CommitTransaction);
        assert_eq!(effects.len(), 1);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::CommitTransaction { txn_id })
        );

        // 6. Feed TransactionCommitted -> Should transition to Finish
        let event = Event::Storage(StorageEvent::TransactionCommitted { txn_id });
        let effects = op.step(event);
        assert_eq!(op.state, IncomingBaoState::Finish);
        assert_eq!(effects.len(), 0);
        assert!(op.is_complete());

        // 7. Finalize
        let result = op.finalize();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_incoming_bao_invalid_steps() {
        let node_key = iroh::SecretKey::from_str(
            "98f15bd901074f210926f8dfb2e3f179e858bf15b49ab8faefe23cea0dcdd9ac",
        )
        .unwrap();
        let node_id = node_key.public();
        let stream_id = Ulid::new();

        // 1. Invalid state: start twice
        let mut op = IncomingBaoOperation::new(stream_id, node_id);
        op.start();
        let effects = op.start();
        assert!(effects.is_empty());
        assert_eq!(op.state, IncomingBaoState::Error);
        assert_eq!(
            op.finalize().unwrap_err(),
            ReplicationError::IncomingBaoError(IncomingBaoError::InvalidState {
                current: IncomingBaoState::NegotiateReplication,
                expected: IncomingBaoState::Init,
            })
        );

        // 2. Invalid event at NegotiateReplication
        let mut op = IncomingBaoOperation::new(stream_id, node_id);
        op.start();
        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: Key::from(vec![0u8; 32]),
        }));
        assert!(effects.is_empty());
        assert_eq!(op.state, IncomingBaoState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            ReplicationError::IncomingBaoError(IncomingBaoError::InvalidStateEvent { .. })
        ));

        // 3. Invalid event at HandleReplication
        let mut op = IncomingBaoOperation::new(stream_id, node_id);
        op.start();
        let replication_id = Ulid::new();
        op.step(Event::Blob(BlobEvent::NegotiationFinished(
            NegotiationResult::Accepted(replication_id),
        )));
        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: Key::from(vec![0u8; 32]),
        }));
        assert!(effects.is_empty());
        assert_eq!(op.state, IncomingBaoState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            ReplicationError::IncomingBaoError(IncomingBaoError::InvalidStateEvent { .. })
        ));

        // 4. Invalid event at StartTransaction
        let mut op = IncomingBaoOperation::new(stream_id, node_id);
        op.start();
        let replication_id = Ulid::new();
        op.step(Event::Blob(BlobEvent::NegotiationFinished(
            NegotiationResult::Accepted(replication_id),
        )));
        let location = make_location();
        op.step(Event::Blob(BlobEvent::ReplicationFinished {
            location: location.clone(),
        }));
        let effects = op.step(Event::Blob(BlobEvent::NegotiationFinished(
            NegotiationResult::Rejected("bad".to_string()),
        )));
        assert!(effects.is_empty());
        assert_eq!(op.state, IncomingBaoState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            ReplicationError::IncomingBaoError(IncomingBaoError::InvalidStateEvent { .. })
        ));

        // 5. Invalid event at CreateBlob
        let mut op = IncomingBaoOperation::new(stream_id, node_id);
        op.start();
        let replication_id = Ulid::new();
        op.step(Event::Blob(BlobEvent::NegotiationFinished(
            NegotiationResult::Accepted(replication_id),
        )));
        let location = make_location();
        op.step(Event::Blob(BlobEvent::ReplicationFinished {
            location: location.clone(),
        }));
        let txn_id = Ulid::new();
        op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = op.step(Event::Blob(BlobEvent::NegotiationFinished(
            NegotiationResult::Rejected("bad".to_string()),
        )));
        assert!(effects.is_empty());
        assert_eq!(op.state, IncomingBaoState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            ReplicationError::IncomingBaoError(IncomingBaoError::InvalidStateEvent { .. })
        ));

        // 6. Invalid event at CommitTransaction
        let mut op = IncomingBaoOperation::new(stream_id, node_id);
        op.start();
        let replication_id = Ulid::new();
        op.step(Event::Blob(BlobEvent::NegotiationFinished(
            NegotiationResult::Accepted(replication_id),
        )));
        let location = make_location();
        op.step(Event::Blob(BlobEvent::ReplicationFinished {
            location: location.clone(),
        }));
        let txn_id = Ulid::new();
        op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let blake3 = location.get_blake3().unwrap();
        op.step(Event::Storage(StorageEvent::WriteResult {
            key: Key::from(blake3.as_slice().to_vec()),
        }));
        let effects = op.step(Event::Blob(BlobEvent::NegotiationFinished(
            NegotiationResult::Rejected("bad".to_string()),
        )));
        assert!(effects.is_empty());
        assert_eq!(op.state, IncomingBaoState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            ReplicationError::IncomingBaoError(IncomingBaoError::InvalidStateEvent { .. })
        ));
    }
}
