//! Resolves a connector and opens a byte stream for one staging source path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::{ResolveConnectorInput, resolve_connector_effect};
use crate::staging::describe_event;
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{SourceResolutionError, StagingSourceError};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::types::{Effects, GroupId};
use bytes::Bytes;
use smallvec::smallvec;
use std::ops::Range;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadSourceInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub range: Option<Range<u64>>,
}

#[derive(Debug, PartialEq)]
pub struct ReadSourceResult {
    pub connector: SourceConnector,
    pub metadata: SourceMetadata,
    pub stream: BackendStream<Result<Bytes, StreamError>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadSourceState {
    Init,
    ResolveConnector,
    ReadSource,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadSourceError {
    #[error(transparent)]
    Resolve(#[from] SourceResolutionError),
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: ReadSourceState,
        expected: &'static str,
        got: String,
    },
    #[error("Read staging source failed")]
    ReadSourceFailed,
}

#[derive(Debug, PartialEq)]
pub struct ReadSourceOperation {
    input: ReadSourceInput,
    state: ReadSourceState,
    connector: Option<SourceConnector>,
    output: Option<Result<ReadSourceResult, ReadSourceError>>,
}

impl ReadSourceOperation {
    pub fn new(input: ReadSourceInput) -> Self {
        Self {
            input,
            state: ReadSourceState::Init,
            connector: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ReadSourceError) -> Effects {
        self.state = ReadSourceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn emit_unexpected(&mut self, expected: &'static str, event: &Event) -> Effects {
        self.emit_error(ReadSourceError::UnexpectedEvent {
            state: self.state.clone(),
            expected,
            got: describe_event(event),
        })
    }

    fn handle_init(&mut self) -> Effects {
        self.state = ReadSourceState::ResolveConnector;
        smallvec![resolve_connector_effect(ResolveConnectorInput {
            group_id: self.input.group_id,
            connector_id: self.input.connector_id,
            source_path: self.input.source_path.clone(),
            allow_root: false,
        })]
    }

    fn handle_resolved_connector(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::SourceConnectorResolved { result }) => {
                match *result {
                    Ok(resolved) => {
                        self.connector = Some(resolved.connector);
                        self.state = ReadSourceState::ReadSource;
                        smallvec![Effect::StagingSource(StagingSourceEffect::Read {
                            access: resolved.access,
                            range: self.input.range.clone(),
                        })]
                    }
                    Err(error) => self.emit_error(error.into()),
                }
            }
            other => self.emit_unexpected(
                "Event::SubOperation(SubOperationEvent::SourceConnectorResolved)",
                &other,
            ),
        }
    }

    fn handle_read_result(&mut self, event: Event) -> Effects {
        match event {
            Event::StagingSource(StagingSourceEvent::ReadResult { metadata, stream }) => {
                let Some(connector) = self.connector.clone() else {
                    return self.emit_error(ReadSourceError::ReadSourceFailed);
                };

                self.state = ReadSourceState::Finish;
                self.output = Some(Ok(ReadSourceResult {
                    connector,
                    metadata,
                    stream,
                }));
                smallvec![]
            }
            Event::StagingSource(StagingSourceEvent::Error { error }) => {
                self.emit_error(error.into())
            }
            other => self.emit_unexpected(
                "Event::StagingSource(StagingSourceEvent::ReadResult)",
                &other,
            ),
        }
    }
}

impl Operation for ReadSourceOperation {
    type Output = ReadSourceResult;
    type Error = ReadSourceError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReadSourceState::Init => self.handle_init(),
            ReadSourceState::ResolveConnector => self.handle_resolved_connector(event),
            ReadSourceState::ReadSource => self.handle_read_result(event),
            ReadSourceState::Finish => smallvec![],
            ReadSourceState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReadSourceState::Finish | ReadSourceState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ReadSourceState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ReadSourceError::ReadSourceFailed);
        }

        self.output.ok_or(ReadSourceError::ReadSourceFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::tests::staging::{create_http_connector, setup_driver_context};
    use aruna_core::structs::execution::source_access::{
        ResolvedSourceAccess, ResolvedSourceConnector,
    };
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use futures_util::{StreamExt, stream};
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn sample_input() -> ReadSourceInput {
        ReadSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
            source_path: "folder/file.txt".to_string(),
            range: Some(5..11),
        }
    }

    fn sample_connector() -> SourceConnector {
        SourceConnector::new(
            Ulid::from_bytes([2u8; 16]),
            Ulid::from_bytes([1u8; 16]),
            "staging-http".to_string(),
            SourceConnectorKind::Http,
            HashMap::from([("endpoint".to_string(), "http://127.0.0.1:1".to_string())]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        )
    }

    fn sample_resolved_connector() -> ResolvedSourceConnector {
        ResolvedSourceConnector {
            connector: sample_connector(),
            secret_fingerprint: None,
            access: ResolvedSourceAccess::OpenDal {
                kind: SourceConnectorKind::Http,
                config: HashMap::from([("endpoint".to_string(), "http://127.0.0.1:1".to_string())]),
                path: "folder/file.txt".to_string(),
                version: None,
            },
        }
    }

    fn sample_metadata() -> SourceMetadata {
        SourceMetadata {
            content_length: 6,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag-2".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
        }
    }

    fn sample_stream() -> BackendStream<Result<Bytes, StreamError>> {
        BackendStream::new(stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"hello ")),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"world")),
        ]))
    }

    #[test]
    fn start_emits_resolve() {
        let mut operation = ReadSourceOperation::new(sample_input());

        let effects = operation.start();

        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        assert_eq!(operation.state, ReadSourceState::ResolveConnector);
    }

    #[test]
    fn resolved_emits_read() {
        let mut operation = ReadSourceOperation::new(sample_input());
        operation.start();
        let resolved = sample_resolved_connector();
        let expected_access = resolved.access.clone();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Ok(resolved)),
            },
        ));

        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::Read { access, range })]
                if access == &expected_access && range == &Some(5..11)
        ));
        assert_eq!(operation.state, ReadSourceState::ReadSource);
    }

    #[test]
    fn exposes_resolve_error() {
        let mut operation = ReadSourceOperation::new(sample_input());
        operation.start();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Err(SourceResolutionError::InvalidSourcePath)),
            },
        ));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(ReadSourceError::Resolve(
                SourceResolutionError::InvalidSourcePath,
            )),
        );
    }

    #[tokio::test]
    async fn read_finishes_operation() {
        let mut operation = ReadSourceOperation::new(sample_input());
        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Ok(sample_resolved_connector())),
            },
        ));
        let metadata = sample_metadata();

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::ReadResult {
            metadata: metadata.clone(),
            stream: sample_stream(),
        }));

        assert!(effects.is_empty());
        let mut result = operation.finalize().unwrap();
        let first = result.stream.next().await.unwrap().unwrap();
        let second = result.stream.next().await.unwrap().unwrap();

        assert_eq!(result.connector, sample_connector());
        assert_eq!(result.metadata, metadata);
        assert_eq!(first, Bytes::from_static(b"hello "));
        assert_eq!(second, Bytes::from_static(b"world"));
    }

    #[test]
    fn exposes_staging_error() {
        let mut operation = ReadSourceOperation::new(sample_input());
        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Ok(sample_resolved_connector())),
            },
        ));

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::ReadError("boom".to_string()),
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(ReadSourceError::Staging(StagingSourceError::ReadError(
                "boom".to_string()
            ),)),
        );
    }

    #[test]
    fn unexpected_describes_event() {
        let mut operation = ReadSourceOperation::new(sample_input());
        operation.start();

        let effects = operation.step(Event::Task(aruna_core::task::TaskEvent::Error {
            key: None,
            message: "ignored".to_string(),
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(ReadSourceError::UnexpectedEvent {
                state: ReadSourceState::ResolveConnector,
                expected: "Event::SubOperation(SubOperationEvent::SourceConnectorResolved)",
                got: "Event::Task".to_string(),
            })
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
            }),
            &test_context.driver_context,
        )
        .await;

        assert!(matches!(result, Err(ReadSourceError::Staging(_))));
    }
}
