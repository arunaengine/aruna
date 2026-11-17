//! Asks one peer whether it holds a version and on what storage, without changing peer state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::LocationSummaryError;
use crate::replication::protocol::{
    LocationSummary, LocationSummaryRequest, VersionReplicationMessage,
};
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::operation::Operation;
use ulid::Ulid;

type Step = Result<Next<LocationSummary>, LocationSummaryError>;

/// Asks one peer whether it holds a version and on what storage. Read-only: a
/// summary never mutates the peer's state.
#[derive(Debug, PartialEq)]
pub struct RemoteLocationOperation {
    node_id: NodeId,
    request: LocationSummaryRequest,
    /// The open stream, closed on every end; `None` once closed.
    stream_id: Option<Ulid>,
    /// The peer's answer, given once the stream is closed.
    answer: Option<Result<LocationSummary, LocationSummaryError>>,
}

fn blob_call(effect: BlobEffect) -> Next<LocationSummary> {
    Next::Call(IoCall::Effect(Box::new(Effect::Blob(effect))))
}

impl RemoteLocationOperation {
    pub fn new(node_id: NodeId, request: LocationSummaryRequest) -> Self {
        Self {
            node_id,
            request,
            stream_id: None,
            answer: None,
        }
    }

    fn opened(&mut self, stream_id: Ulid) -> Step {
        self.stream_id = Some(stream_id);
        let message = VersionReplicationMessage::LocationSummaryRequest(self.request.clone());
        let payload = message.to_bytes()?;
        Ok(blob_call(BlobEffect::SendMessage { stream_id, payload }))
    }

    fn received(&mut self, stream_id: Ulid, payload: &[u8]) -> Step {
        let answer = match VersionReplicationMessage::from_bytes(payload)? {
            VersionReplicationMessage::LocationSummaryResponse(summary) => Ok(summary),
            VersionReplicationMessage::LocationSummaryDenied => Err(LocationSummaryError::Denied),
            _ => {
                return Err(LocationSummaryError::Unexpected {
                    state: "remote_read",
                    event: "unexpected location summary response".to_string(),
                });
            }
        };
        self.answer = Some(answer);
        Ok(blob_call(BlobEffect::CloseConnection { stream_id }))
    }

    fn closed(&mut self, stream_id: Ulid) -> Step {
        if Some(stream_id) != self.stream_id {
            return Err(unexpected("the close of this stream", "another stream"));
        }
        self.stream_id = None;
        let answer = self.answer.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Done(answer?))
    }
}

fn unexpected(expected: &'static str, got: &'static str) -> LocationSummaryError {
    IoError::Unexpected { expected, got }.into()
}

impl Operation for RemoteLocationOperation {
    type Output = LocationSummary;
    type Error = LocationSummaryError;

    fn first(&mut self) -> Step {
        Ok(blob_call(BlobEffect::OpenConnection {
            node_id: self.node_id,
        }))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match result.event()? {
            Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
            Event::Blob(BlobEvent::ConnectionEstablished { stream_id })
                if self.stream_id.is_none() =>
            {
                self.opened(stream_id)
            }
            Event::Blob(BlobEvent::MessageSent { stream_id })
                if Some(stream_id) == self.stream_id && self.answer.is_none() =>
            {
                Ok(blob_call(BlobEffect::ReadMessage { stream_id }))
            }
            Event::Blob(BlobEvent::MessageReceived { stream_id, payload })
                if Some(stream_id) == self.stream_id && self.answer.is_none() =>
            {
                self.received(stream_id, &payload)
            }
            Event::Blob(BlobEvent::ConnectionClosed { stream_id }) if self.answer.is_some() => {
                self.closed(stream_id)
            }
            other => Err(unexpected(
                "the answer of the current stream step",
                other.label(),
            )),
        }
    }

    /// A deadline or a failure must release the stream; only `CloseConnection` unregisters it.
    fn abort(&mut self) -> Vec<IoCall> {
        let close = |stream_id| {
            let close = Effect::Blob(BlobEffect::CloseConnection { stream_id });
            IoCall::Effect(Box::new(close))
        };
        self.stream_id.take().map(close).into_iter().collect()
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{LocationSummaryError, RemoteLocationOperation, Step};
    use crate::replication::protocol::{LocationSummary, VersionReplicationMessage};
    use crate::tests::locations::{node_id, request};
    use aruna_core::calls::{IoCall, IoError, IoResult, Next};
    use aruna_core::effects::{BlobEffect, Effect};
    use aruna_core::events::{BlobEvent, Event};
    use aruna_core::operation::Operation;
    use ulid::Ulid;

    fn stream() -> Ulid {
        Ulid::from_bytes([5u8; 16])
    }

    fn blob(event: BlobEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Blob(event)))
    }

    /// Runs up to the peer's answer and returns the step after it.
    fn answered(message: VersionReplicationMessage) -> (RemoteLocationOperation, Step) {
        let mut operation = RemoteLocationOperation::new(node_id(4), request(None));
        let _ = operation.first();
        let stream_id = stream();
        let _ = operation.next(blob(BlobEvent::ConnectionEstablished { stream_id }));
        let _ = operation.next(blob(BlobEvent::MessageSent { stream_id }));
        let payload = message.to_bytes().unwrap();
        let turn = operation.next(blob(BlobEvent::MessageReceived { stream_id, payload }));
        (operation, turn)
    }

    fn close_call() -> IoCall {
        let close = BlobEffect::CloseConnection {
            stream_id: stream(),
        };
        IoCall::Effect(Box::new(Effect::Blob(close)))
    }

    fn closed() -> IoResult {
        blob(BlobEvent::ConnectionClosed {
            stream_id: stream(),
        })
    }

    #[test]
    fn reports_remote_denial() {
        // The asking side turns the denial back into a Denied error, never a
        // transport failure, after it closed the stream.
        let (mut operation, turn) = answered(VersionReplicationMessage::LocationSummaryDenied);
        assert_eq!(turn, Ok(Next::Call(close_call())));
        assert_eq!(operation.next(closed()), Err(LocationSummaryError::Denied));
        assert!(operation.abort().is_empty());
    }

    #[test]
    fn carries_peer_verdict() {
        // Provenance and the peer's own compliance verdict survive the wire.
        let relationship_id = Ulid::from_bytes([6u8; 16]);
        let answer = LocationSummary {
            held: true,
            origin: aruna_core::structs::storage::blob::CopyOrigin::Sync { relationship_id },
            compliance: crate::replication::protocol::CopyCompliance::Quarantined,
            ..LocationSummary::absent()
        };
        let response = VersionReplicationMessage::LocationSummaryResponse(answer.clone());
        let (mut operation, _) = answered(response);
        assert_eq!(operation.next(closed()), Ok(Next::Done(answer)));
    }

    #[test]
    fn early_close_rejects() {
        let mut operation = RemoteLocationOperation::new(node_id(4), request(None));
        let _ = operation.first();
        let stream_id = stream();
        let _ = operation.next(blob(BlobEvent::ConnectionEstablished { stream_id }));
        assert!(matches!(
            operation.next(closed()),
            Err(LocationSummaryError::Io(IoError::Unexpected { .. }))
        ));
    }

    #[test]
    fn abort_closes_stream() {
        // A deadline must release the stream; only CloseConnection unregisters it.
        let mut operation = RemoteLocationOperation::new(node_id(4), request(None));
        assert!(operation.abort().is_empty());
        let _ = operation.first();
        let stream_id = stream();
        let _ = operation.next(blob(BlobEvent::ConnectionEstablished { stream_id }));
        assert_eq!(operation.abort(), vec![close_call()]);
        let aborted = LocationSummaryError::from(IoError::Aborted);
        assert_eq!(aborted, LocationSummaryError::Aborted);
    }
}
