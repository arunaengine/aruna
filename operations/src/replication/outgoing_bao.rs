use crate::replication::error::ReplicationError;
use aruna_blob::blob::BLOB_LOCATION_DB;
use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{BlobInfo, NegotiationResult};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq)]
pub enum OutgoingBaoState {
    Init,
    OpenConnection,
    GetBlobInfo,
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

        self.state = OutgoingBaoState::GetBlobInfo;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_LOCATION_DB.to_string(),
            key: Key::from(self.hash.to_vec()),
            txn_id: None,
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
                    expected: "Event::Blob(BlobEvent::ConnectionEstablished)",
                    received: event,
                }
                .into(),
            );
        };
        let Some(blob_info) = &self.blob_info else {
            return self.handle_error(ReplicationError::NoSuchKey);
        };

        self.stream_id = Some(stream_id);
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
                    stream_id: *stream_id,
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
            self.result?;
            return Err(ReplicationError::ReplicationFailed);
        }
        Ok(self.result)
    }

    fn abort(&mut self) -> Effects {
        //TODO: Send abortion message to receiving node?
        smallvec![]
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use aruna_core::structs::BackendLocation;
    use byteview::ByteView;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::time::SystemTime;

    fn make_blob_info() -> BlobInfo {
        BlobInfo {
            location: BackendLocation {
                root: "/tmp".to_string(),
                storage_bucket: "bucket".to_string(),
                object_bucket: "obj".to_string(),
                object_key: "key".to_string(),
                ulid: Ulid::new(),
                compressed: false,
                encrypted: false,
            },
            created_by: Ulid::new(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 1024,
            hashes: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_outgoing_bao() {
        let node_key = iroh::SecretKey::from_str(
            "98f15bd901074f210926f8dfb2e3f179e858bf15b49ab8faefe23cea0dcdd9ac",
        )
        .unwrap();
        let node_id = node_key.public();
        let hash = [0u8; 32];
        let mut op = OutgoingBaoOperation::new(node_id, hash);

        // 1. Start -> Should transition to GetBlobInfo and emit Storage::Read
        let effects = op.start();
        assert_eq!(op.state, OutgoingBaoState::GetBlobInfo);
        assert_eq!(effects.len(), 1);
        let key = Key::from(hash.to_vec());
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::Read {
                key_space: BLOB_LOCATION_DB.to_string(),
                key: key.clone(),
                txn_id: None,
            })
        );

        // 2. Feed BlobInfo -> Should transition to OpenConnection and emit Blob::OpenConnection
        let blob_info = make_blob_info();
        let blob_info_bytes = blob_info.to_bytes().unwrap();
        let event = Event::Storage(StorageEvent::ReadResult {
            key,
            value: Some(ByteView::from(blob_info_bytes)),
        });

        let effects = op.step(event);
        assert_eq!(op.state, OutgoingBaoState::OpenConnection);
        assert_eq!(effects.len(), 1);
        assert_eq!(
            effects[0],
            Effect::Blob(BlobEffect::OpenConnection { node_id })
        );

        // 3. Feed ConnectionEstablished -> Should transition to NegotiateReplication
        let stream_id = Ulid::new();
        let event = Event::Blob(BlobEvent::ConnectionEstablished { stream_id });
        let effects = op.step(event);
        assert_eq!(op.state, OutgoingBaoState::NegotiateReplication);
        assert_eq!(effects.len(), 1);
        if let Effect::Blob(BlobEffect::NegotiateOutgoing {
            replication_id,
            stream_id: s_id,
            blob_info: b_info,
        }) = &effects[0]
        {
            assert_eq!(*s_id, stream_id);
            assert_eq!(b_info, &blob_info);
            // Verify op state
            assert_eq!(op.replication_id, Some(*replication_id));
            assert_eq!(op.stream_id, Some(stream_id));
        } else {
            panic!("Expected NegotiateOutgoing effect");
        }

        // 4. Feed NegotiationFinished(Accepted) -> Should transition to Replicate
        let replication_id = op.replication_id.unwrap();
        let event = Event::Blob(BlobEvent::NegotiationFinished(NegotiationResult::Accepted(
            replication_id,
        )));
        let effects = op.step(event);
        assert_eq!(op.state, OutgoingBaoState::Replicate);
        assert_eq!(effects.len(), 1);
        assert_eq!(
            effects[0],
            Effect::Blob(BlobEffect::Replicate {
                replication_id,
                stream_id,
                blob_info: blob_info.clone(),
            })
        );

        // 5. Feed ReplicationFinished -> Should transition to Finish
        let event = Event::Blob(BlobEvent::ReplicationFinished {
            blob_info: blob_info.clone(),
        });
        let effects = op.step(event);
        assert_eq!(op.state, OutgoingBaoState::Finish);
        assert_eq!(effects.len(), 0);
        assert!(op.is_complete());

        // 6. Finalize
        let result = op.finalize();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_outgoing_bao_invalid_steps() {
        let node_key = iroh::SecretKey::from_str(
            "98f15bd901074f210926f8dfb2e3f179e858bf15b49ab8faefe23cea0dcdd9ac",
        )
        .unwrap();
        let node_id = node_key.public();
        let hash = [0u8; 32];

        // 1. Invalid state: start twice
        let mut op = OutgoingBaoOperation::new(node_id, hash);
        op.start();
        let effects = op.start();
        assert!(effects.is_empty());
        assert_eq!(op.state, OutgoingBaoState::Error);
        assert_eq!(
            op.finalize().unwrap_err(),
            ReplicationError::OutgoingBaoError(OutgoingBaoError::InvalidState {
                current: OutgoingBaoState::GetBlobInfo,
                expected: OutgoingBaoState::Init,
            })
        );

        // 2. Invalid event at GetBlobInfo
        let mut op = OutgoingBaoOperation::new(node_id, hash);
        op.start();
        let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));
        assert!(effects.is_empty());
        assert_eq!(op.state, OutgoingBaoState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            ReplicationError::OutgoingBaoError(OutgoingBaoError::InvalidStateEvent { .. })
        ));

        // 3. Invalid event at OpenConnection
        let mut op = OutgoingBaoOperation::new(node_id, hash);
        op.start();
        let key = Key::from(hash.to_vec());
        let blob_info = make_blob_info();
        let blob_info_bytes = blob_info.to_bytes().unwrap();
        let event = Event::Storage(StorageEvent::ReadResult {
            key: key.clone(),
            value: Some(ByteView::from(blob_info_bytes)),
        });
        op.step(event);
        let effects = op.step(Event::Storage(StorageEvent::WriteResult { key }));
        assert!(effects.is_empty());
        assert_eq!(op.state, OutgoingBaoState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            ReplicationError::OutgoingBaoError(OutgoingBaoError::InvalidStateEvent { .. })
        ));

        // 4. Invalid event at NegotiateReplication
        let mut op = OutgoingBaoOperation::new(node_id, hash);
        op.start();
        let key = Key::from(hash.to_vec());
        let blob_info = make_blob_info();
        let blob_info_bytes = blob_info.to_bytes().unwrap();
        let event = Event::Storage(StorageEvent::ReadResult {
            key,
            value: Some(ByteView::from(blob_info_bytes)),
        });
        op.step(event);
        let stream_id = Ulid::new();
        let event = Event::Blob(BlobEvent::ConnectionEstablished { stream_id });
        op.step(event);
        let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));
        assert!(effects.is_empty());
        assert_eq!(op.state, OutgoingBaoState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            ReplicationError::OutgoingBaoError(OutgoingBaoError::InvalidStateEvent { .. })
        ));

        // 5. Invalid event at Replicate
        let mut op = OutgoingBaoOperation::new(node_id, hash);
        op.start();
        let key = Key::from(hash.to_vec());
        let blob_info = make_blob_info();
        let blob_info_bytes = blob_info.to_bytes().unwrap();
        let event = Event::Storage(StorageEvent::ReadResult {
            key,
            value: Some(ByteView::from(blob_info_bytes)),
        });
        op.step(event);
        let stream_id = Ulid::new();
        let event = Event::Blob(BlobEvent::ConnectionEstablished { stream_id });
        op.step(event);
        let replication_id = op.replication_id.unwrap();
        let event = Event::Blob(BlobEvent::NegotiationFinished(NegotiationResult::Accepted(
            replication_id,
        )));
        op.step(event);
        let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));
        assert!(effects.is_empty());
        assert_eq!(op.state, OutgoingBaoState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            ReplicationError::OutgoingBaoError(OutgoingBaoError::InvalidStateEvent { .. })
        ));
    }
}
