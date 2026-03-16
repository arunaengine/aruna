use crate::replication::error::ReplicationError;
use aruna_blob::blob::BLOB_LOCATION_DB;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::{BlobInfo, NegotiationResult};
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
    blob_info: Option<BlobInfo>,
    txn_id: Option<TxnId>,
    result: Result<(), ReplicationError>,
}

impl IncomingBaoOperation {
    pub fn new(stream_id: Ulid, node_id: NodeId) -> Self {
        Self {
            state: IncomingBaoState::Init,
            stream_id,
            node_id,
            blob_info: None,
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
        let Event::Blob(BlobEvent::ReplicationFinished { blob_info }) = event else {
            return self.handle_error(
                IncomingBaoError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Blob(BlobEvent::ReplicationResult)",
                    received: event,
                }
                .into(),
            );
        };

        self.blob_info = Some(blob_info);
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

        let Some(blob_info) = &self.blob_info else {
            return self.handle_error(ReplicationError::NoSuchKey);
        };
        let Some(blake3) = blob_info.get_blake3() else {
            return self.handle_error(ReplicationError::HashMissing);
        };
        let bytes = match blob_info.to_bytes() {
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
            if let Err(error) = self.result {
                return Err(error);
            }
            return Err(ReplicationError::ReplicationFailed);
        }
        Ok(self.result)
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
pub mod test {}
