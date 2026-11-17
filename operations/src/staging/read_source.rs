//! Resolves a connector and opens a byte stream for one staging source path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::{ResolveConnectorInput, resolve_connector_effect};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{SourceResolutionError, StagingSourceError};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::types::GroupId;
use bytes::Bytes;
use std::ops::Range;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadSourceInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub range: Option<Range<u64>>,
    /// The observation the read must stay bound to, if any.
    pub expected: Option<SourceMetadata>,
}

#[derive(Debug, PartialEq)]
pub struct ReadSourceResult {
    pub connector: SourceConnector,
    pub metadata: SourceMetadata,
    pub stream: BackendStream<Result<Bytes, StreamError>>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadSourceError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Resolve(#[from] SourceResolutionError),
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("unexpected staging answer {0}")]
    UnexpectedEvent(&'static str),
}

/// Resolves the connector, then opens the read of the source path.
#[derive(Debug, PartialEq)]
pub struct ReadSourceOperation {
    input: ReadSourceInput,
    connector: Option<SourceConnector>,
}

impl ReadSourceOperation {
    pub fn new(input: ReadSourceInput) -> Self {
        Self {
            input,
            connector: None,
        }
    }

    fn read(&mut self, event: Event) -> Result<IoCall, ReadSourceError> {
        let Event::SubOperation(SubOperationEvent::SourceConnectorResolved { result }) = event
        else {
            return Err(ReadSourceError::UnexpectedEvent(event.label()));
        };
        let resolved = (*result)?;
        self.connector = Some(resolved.connector);
        let read = StagingSourceEffect::Read {
            access: resolved.access,
            range: self.input.range.clone(),
            expected: self.input.expected.clone(),
        };
        Ok(IoCall::Effect(Box::new(Effect::StagingSource(read))))
    }
}

impl Operation for ReadSourceOperation {
    type Output = ReadSourceResult;
    type Error = ReadSourceError;

    fn first(&mut self) -> Result<Next<ReadSourceResult>, ReadSourceError> {
        Ok(Next::Call(IoCall::from(resolve_connector_effect(
            ResolveConnectorInput {
                group_id: self.input.group_id,
                connector_id: self.input.connector_id,
                source_path: self.input.source_path.clone(),
                allow_root: false,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ReadSourceResult>, ReadSourceError> {
        let event = result.event()?;
        let Some(connector) = self.connector.take() else {
            return Ok(Next::Call(self.read(event)?));
        };
        match event {
            Event::StagingSource(StagingSourceEvent::ReadResult { metadata, stream }) => {
                Ok(Next::Done(ReadSourceResult {
                    connector,
                    metadata,
                    stream,
                }))
            }
            Event::StagingSource(StagingSourceEvent::Error { error }) => Err(error.into()),
            other => Err(ReadSourceError::UnexpectedEvent(other.label())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use aruna_core::structs::execution::source_access::ResolvedSourceConnector;
    use crate::tests::staging::{
        create_http_connector, sample_connector, sample_resolved_connector, setup_driver_context,
    };
    use futures_util::{StreamExt, stream};
    use std::time::SystemTime;

    fn sample_input() -> ReadSourceInput {
        ReadSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
            source_path: "folder/file.txt".to_string(),
            range: Some(5..11),
            expected: None,
        }
    }

    fn sample_metadata() -> SourceMetadata {
        SourceMetadata {
            content_length: 6,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag-2".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
            digests: Default::default(),
        }
    }

    fn sample_stream() -> BackendStream<Result<Bytes, StreamError>> {
        BackendStream::new(stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"hello ")),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"world")),
        ]))
    }

    fn answer(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    fn resolved(result: Result<ResolvedSourceConnector, SourceResolutionError>) -> IoResult {
        answer(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(result),
            },
        ))
    }

    fn opened() -> ReadSourceOperation {
        let mut operation = ReadSourceOperation::new(sample_input());
        operation.first().unwrap();
        operation
            .next(resolved(Ok(sample_resolved_connector())))
            .unwrap();
        operation
    }

    #[test]
    fn resolved_emits_read() {
        let mut operation = ReadSourceOperation::new(sample_input());
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let resolved_connector = sample_resolved_connector();
        let read = StagingSourceEffect::Read {
            access: resolved_connector.access.clone(),
            range: Some(5..11),
            expected: None,
        };
        let call = IoCall::Effect(Box::new(Effect::StagingSource(read)));
        let effect = operation.next(resolved(Ok(resolved_connector)));
        assert_eq!(effect, Ok(Next::Call(call)));
    }

    #[test]
    fn exposes_resolve_error() {
        let mut operation = ReadSourceOperation::new(sample_input());
        operation.first().unwrap();
        let failed = resolved(Err(SourceResolutionError::InvalidSourcePath));
        assert_eq!(
            operation.next(failed),
            Err(ReadSourceError::Resolve(
                SourceResolutionError::InvalidSourcePath,
            )),
        );
    }

    #[tokio::test]
    async fn read_finishes_operation() {
        let mut operation = opened();
        let read = answer(Event::StagingSource(StagingSourceEvent::ReadResult {
            metadata: sample_metadata(),
            stream: sample_stream(),
        }));
        let Ok(Next::Done(mut result)) = operation.next(read) else {
            panic!("expected the read");
        };
        let first = result.stream.next().await.unwrap().unwrap();
        let second = result.stream.next().await.unwrap().unwrap();
        assert_eq!(result.connector, sample_connector());
        assert_eq!(result.metadata, sample_metadata());
        assert_eq!(first, Bytes::from_static(b"hello "));
        assert_eq!(second, Bytes::from_static(b"world"));
    }

    #[test]
    fn exposes_staging_error() {
        let mut operation = opened();
        let failed = answer(Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::ReadError("boom".to_string()),
        }));
        assert_eq!(
            operation.next(failed),
            Err(ReadSourceError::Staging(StagingSourceError::ReadError(
                "boom".to_string()
            ))),
        );
    }

    #[test]
    fn unexpected_describes_event() {
        let mut operation = ReadSourceOperation::new(sample_input());
        operation.first().unwrap();
        let task = answer(Event::Task(aruna_core::task::TaskEvent::Error {
            key: None,
            message: "ignored".to_string(),
        }));
        assert_eq!(
            operation.next(task),
            Err(ReadSourceError::UnexpectedEvent("TaskEvent::Error"))
        );
    }

    #[tokio::test]
    async fn read_hits_runtime() {
        let test_context = setup_driver_context().await;
        let group_id = Ulid::generate();
        let connector =
            create_http_connector(&test_context.driver_context, group_id, "http://127.0.0.1:1")
                .await;

        let result = drive(
            ReadSourceOperation::new(ReadSourceInput {
                group_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".to_string(),
                range: None,
                expected: None,
            }),
            &test_context.driver_context,
        )
        .await;

        assert!(matches!(result, Err(ReadSourceError::Staging(_))));
    }
}
