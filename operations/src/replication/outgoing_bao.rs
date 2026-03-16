use crate::replication::error::ReplicationError;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{BlobInfo, NegotiationResult};
use aruna_core::types::Effects;
use aruna_core::NodeId;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq)]
pub enum OutgoingBaoState {
    Init,
    GetBlobInfo,
    OpenConnection,
    NegotiateReplication,
    Replicate,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum OutgoingBaoError {
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: OutgoingBaoState,
        expected: OutgoingBaoState,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: OutgoingBaoState,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct OutgoingBaoOperation {
    state: OutgoingBaoState,
    node_id: NodeId,
    hash: [u8; 32], // or path?
    blob_info: Option<BlobInfo>,
    stream_id: Option<Ulid>,
    replication_id: Option<Ulid>,
    result: Result<(), ReplicationError>,
}

impl OutgoingBaoOperation {
    pub fn new(node_id: NodeId, hash: [u8; 32]) -> Self {
        Self {
            state: OutgoingBaoState::Init,
            node_id,
            hash,
            blob_info: None,
            replication_id: None,
            stream_id: None,
            result: Ok(()),
        }
    }

    fn handle_error(&mut self, error: ReplicationError) -> Effects {
        self.state = OutgoingBaoState::Error;
        self.result = Err(error);
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        if self.state != OutgoingBaoState::Init {
            return self.handle_error(
                OutgoingBaoError::InvalidState {
                    current: self.state.clone(),
                    expected: OutgoingBaoState::Init,
                }
                .into(),
            );
        }

        self.state = OutgoingBaoState::OpenConnection;
        smallvec![Effect::Blob(BlobEffect::OpenConnection {
            node_id: self.node_id
        })]
    }

    fn handle_info_received(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.handle_error(
                OutgoingBaoError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Storage(StorageEvent::ReadResult)",
                    received: event,
                }
                .into(),
            );
        };

        let Some(val) = value else {
            return self.handle_error(ReplicationError::NoSuchKey);
        };

        let blob_info = match BlobInfo::from_bytes(val.as_ref()) {
            Ok(info) => info,
            Err(err) => return self.handle_error(ReplicationError::ConversionError(err)),
        };

        self.blob_info = Some(blob_info);
        self.state = OutgoingBaoState::OpenConnection;
        smallvec![Effect::Blob(BlobEffect::OpenConnection {
            node_id: self.node_id
        })]
    }

    fn handle_connection_opened(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ConnectionEstablished { stream_id }) = event else {
            return self.handle_error(
                OutgoingBaoError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Blob(BlobEvent::ConnectionStored)",
                    received: event,
                }
                .into(),
            );
        };
        let Some(blob_info) = &self.blob_info else {
            return self.handle_error(ReplicationError::NoSuchKey);
        };

        let replication_id = Ulid::new();
        self.replication_id = Some(replication_id);
        self.state = OutgoingBaoState::NegotiateReplication;
        smallvec![Effect::Blob(BlobEffect::NegotiateOutgoing {
            replication_id,
            stream_id,
            blob_info: blob_info.clone(),
        })]
    }

    fn handle_negotiation_result(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::NegotiationFinished(result)) = event else {
            return self.handle_error(
                OutgoingBaoError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Blob(BlobEvent::NegotiationResult)",
                    received: event,
                }
                .into(),
            );
        };

        match result {
            NegotiationResult::Accepted(replication_id) => {
                let Some(stream_id) = &self.stream_id else {
                    return self.handle_error(ReplicationError::ConnectionMissing);
                };
                let Some(blob_info) = &self.blob_info else {
                    return self.handle_error(ReplicationError::NoSuchKey);
                };

                self.state = OutgoingBaoState::Replicate;
                smallvec![Effect::Blob(BlobEffect::Replicate {
                    replication_id,
                    stream_id: stream_id.clone(),
                    blob_info: blob_info.to_owned(),
                })]
            }
            NegotiationResult::Rejected(reason) => {
                self.handle_error(ReplicationError::ReplicationRejected(reason))
            }
        }
    }

    fn handle_replication_finished(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ReplicationFinished { .. }) = event else {
            return self.handle_error(
                OutgoingBaoError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Blob(BlobEvent::ReplicationFinished)",
                    received: event,
                }
                .into(),
            );
        };

        self.state = OutgoingBaoState::Finish;
        smallvec![]
    }
}

impl Operation for OutgoingBaoOperation {
    type Output = Result<(), ReplicationError>;
    type Error = ReplicationError;

    fn start(&mut self) -> Effects {
        if self.state != OutgoingBaoState::Init {
            return self.handle_error(
                OutgoingBaoError::InvalidState {
                    current: self.state.clone(),
                    expected: OutgoingBaoState::Init,
                }
                .into(),
            );
        }

        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            OutgoingBaoState::Init => self.handle_init(),
            OutgoingBaoState::GetBlobInfo => self.handle_info_received(event),
            OutgoingBaoState::OpenConnection => self.handle_connection_opened(event),
            OutgoingBaoState::NegotiateReplication => self.handle_negotiation_result(event),
            OutgoingBaoState::Replicate => self.handle_replication_finished(event),
            OutgoingBaoState::Finish => smallvec![],
            OutgoingBaoState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            OutgoingBaoState::Finish | OutgoingBaoState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if OutgoingBaoState::Error == self.state {
            if let Err(error) = self.result {
                return Err(error);
            }
            return Err(ReplicationError::ReplicationFailed);
        }
        Ok(self.result)
    }

    fn abort(&mut self) -> Effects {
        //TODO: Send abortion message to receiving node?
        smallvec![]
    }
}
