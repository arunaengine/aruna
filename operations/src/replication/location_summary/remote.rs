use super::LocationSummaryError;
use crate::replication::protocol::{
    LocationSummary, LocationSummaryRequest, VersionReplicationMessage,
};
use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
enum RemoteState {
    Init,
    Open,
    Send,
    Read,
    Close,
    Finish,
    Error,
}

/// Asks one peer whether it holds a version and on what storage. Read-only: a
/// summary never mutates the peer's state.
#[derive(Debug, PartialEq)]
pub struct RemoteLocationSummaryOperation {
    node_id: NodeId,
    request: LocationSummaryRequest,
    stream_id: Option<Ulid>,
    state: RemoteState,
    output: Option<Result<LocationSummary, LocationSummaryError>>,
}

impl RemoteLocationSummaryOperation {
    pub fn new(node_id: NodeId, request: LocationSummaryRequest) -> Self {
        Self {
            node_id,
            request,
            stream_id: None,
            state: RemoteState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: LocationSummaryError) -> Effects {
        self.output = Some(Err(error));
        self.state = RemoteState::Error;
        match self.stream_id.take() {
            Some(stream_id) => smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })],
            None => smallvec![],
        }
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        self.fail(LocationSummaryError::Unexpected {
            state: "remote",
            event: format!("{event:?}"),
        })
    }
}

impl Operation for RemoteLocationSummaryOperation {
    type Output = LocationSummary;
    type Error = LocationSummaryError;

    fn start(&mut self) -> Effects {
        self.state = RemoteState::Open;
        smallvec![Effect::Blob(BlobEffect::OpenConnection {
            node_id: self.node_id,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Blob(BlobEvent::Error(error)) => return self.fail(error.into()),
            event => event,
        };
        match self.state {
            RemoteState::Init => self.start(),
            RemoteState::Open => {
                let Event::Blob(BlobEvent::ConnectionEstablished { stream_id }) = event else {
                    return self.unexpected(event);
                };
                self.stream_id = Some(stream_id);
                let payload =
                    match VersionReplicationMessage::LocationSummaryRequest(self.request.clone())
                        .to_bytes()
                    {
                        Ok(payload) => payload,
                        Err(error) => return self.fail(error.into()),
                    };
                self.state = RemoteState::Send;
                smallvec![Effect::Blob(BlobEffect::SendMessage { stream_id, payload })]
            }
            RemoteState::Send => {
                let Event::Blob(BlobEvent::MessageSent { stream_id }) = event else {
                    return self.unexpected(event);
                };
                self.state = RemoteState::Read;
                smallvec![Effect::Blob(BlobEffect::ReadMessage { stream_id })]
            }
            RemoteState::Read => {
                let Event::Blob(BlobEvent::MessageReceived { stream_id, payload }) = event else {
                    return self.unexpected(event);
                };
                match VersionReplicationMessage::from_bytes(&payload) {
                    Ok(VersionReplicationMessage::LocationSummaryResponse(summary)) => {
                        self.output = Some(Ok(summary));
                        self.state = RemoteState::Close;
                        smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
                    }
                    Ok(VersionReplicationMessage::LocationSummaryDenied) => {
                        self.output = Some(Err(LocationSummaryError::Denied));
                        self.state = RemoteState::Close;
                        smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
                    }
                    Ok(_) => self.fail(LocationSummaryError::Unexpected {
                        state: "remote_read",
                        event: "unexpected location summary response".to_string(),
                    }),
                    Err(error) => self.fail(error.into()),
                }
            }
            RemoteState::Close => {
                let Event::Blob(BlobEvent::ConnectionClosed { stream_id }) = event else {
                    return self.unexpected(event);
                };
                if Some(stream_id) != self.stream_id {
                    return self.unexpected(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
                }
                self.state = RemoteState::Finish;
                smallvec![]
            }
            RemoteState::Finish | RemoteState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RemoteState::Finish | RemoteState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(LocationSummaryError::Unexpected {
            state: "finalize",
            event: "remote summary ended without an answer".to_string(),
        }))
    }

    fn abort(&mut self) -> Effects {
        self.state = RemoteState::Error;
        self.output
            .get_or_insert(Err(LocationSummaryError::Aborted));
        match self.stream_id.take() {
            Some(stream_id) => smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::replication::location_summary::fixtures::{node_id, request};
    use aruna_core::effects::Effect;
    use aruna_core::events::Event;
    use aruna_core::operation::Operation;
    use ulid::Ulid;

    #[test]
    fn reports_remote_denial() {
        // The asking side turns the denial back into a Denied error, never a
        // transport failure.
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let mut operation = super::RemoteLocationSummaryOperation::new(node_id(4), request(None));
        operation.start();
        operation.step(Event::Blob(
            aruna_core::events::BlobEvent::ConnectionEstablished { stream_id },
        ));
        operation.step(Event::Blob(aruna_core::events::BlobEvent::MessageSent {
            stream_id,
        }));

        operation.step(Event::Blob(
            aruna_core::events::BlobEvent::MessageReceived {
                stream_id,
                payload:
                    crate::replication::protocol::VersionReplicationMessage::LocationSummaryDenied
                        .to_bytes()
                        .unwrap(),
            },
        ));

        assert_eq!(
            operation.finalize(),
            Err(super::LocationSummaryError::Denied)
        );
    }

    #[test]
    fn carries_peer_verdict() {
        // Provenance and the peer's own compliance verdict survive the wire.
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let relationship_id = Ulid::from_bytes([6u8; 16]);
        let answer = crate::replication::protocol::LocationSummary {
            held: true,
            origin: aruna_core::structs::CopyOrigin::Sync { relationship_id },
            compliance: crate::replication::protocol::CopyCompliance::Quarantined,
            ..crate::replication::protocol::LocationSummary::absent()
        };
        let mut operation = super::RemoteLocationSummaryOperation::new(node_id(4), request(None));
        operation.start();
        operation.step(Event::Blob(
            aruna_core::events::BlobEvent::ConnectionEstablished { stream_id },
        ));
        operation.step(Event::Blob(aruna_core::events::BlobEvent::MessageSent {
            stream_id,
        }));

        operation.step(Event::Blob(
            aruna_core::events::BlobEvent::MessageReceived {
                stream_id,
                payload:
                    crate::replication::protocol::VersionReplicationMessage::LocationSummaryResponse(
                        answer.clone(),
                    )
                    .to_bytes()
                    .unwrap(),
            },
        ));

        assert_eq!(operation.finalize(), Ok(answer));
    }

    #[test]
    fn remote_close_rejects() {
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let mut operation = super::RemoteLocationSummaryOperation::new(node_id(4), request(None));
        operation.start();
        operation.step(Event::Blob(
            aruna_core::events::BlobEvent::ConnectionEstablished { stream_id },
        ));
        operation.state = super::RemoteState::Close;

        operation.step(Event::Blob(aruna_core::events::BlobEvent::MessageSent {
            stream_id,
        }));

        assert_eq!(operation.state, super::RemoteState::Error);
    }

    #[test]
    fn abort_closes_stream() {
        // A deadline must release the stream; only CloseConnection unregisters it.
        let stream_id = Ulid::from_bytes([5u8; 16]);
        let mut operation = super::RemoteLocationSummaryOperation::new(node_id(4), request(None));
        operation.start();
        operation.step(Event::Blob(
            aruna_core::events::BlobEvent::ConnectionEstablished { stream_id },
        ));

        let effects = operation.abort();

        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(
                aruna_core::effects::BlobEffect::CloseConnection { stream_id }
            )]
        );
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(super::LocationSummaryError::Aborted)
        );
    }
}
