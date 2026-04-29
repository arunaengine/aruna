use crate::connectors::{ResolveSourceConnectorInput, resolve_source_connector_suboperation};
use crate::staging::describe_event;
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{SourceConnectorResolutionError, StagingSourceError};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{SourceConnector, SourceMetadata};
use aruna_core::types::{Effects, GroupId};
use bytes::Bytes;
use smallvec::smallvec;
use std::ops::Range;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadStagingSourceInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub range: Option<Range<u64>>,
}

#[derive(Debug, PartialEq)]
pub struct ReadStagingSourceResult {
    pub connector: SourceConnector,
    pub metadata: SourceMetadata,
    pub stream: BackendStream<Result<Bytes, StreamError>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadStagingSourceState {
    Init,
    ResolveConnector,
    ReadSource,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadStagingSourceError {
    #[error(transparent)]
    Resolve(#[from] SourceConnectorResolutionError),
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: ReadStagingSourceState,
        expected: &'static str,
        got: String,
    },
    #[error("Read staging source failed")]
    ReadStagingSourceFailed,
}

#[derive(Debug, PartialEq)]
pub struct ReadStagingSourceOperation {
    input: ReadStagingSourceInput,
    state: ReadStagingSourceState,
    connector: Option<SourceConnector>,
    output: Option<Result<ReadStagingSourceResult, ReadStagingSourceError>>,
}

impl ReadStagingSourceOperation {
    pub fn new(input: ReadStagingSourceInput) -> Self {
        Self {
            input,
            state: ReadStagingSourceState::Init,
            connector: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ReadStagingSourceError) -> Effects {
        self.state = ReadStagingSourceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn emit_unexpected(&mut self, expected: &'static str, event: &Event) -> Effects {
        self.emit_error(ReadStagingSourceError::UnexpectedEvent {
            state: self.state.clone(),
            expected,
            got: describe_event(event),
        })
    }

    fn handle_init(&mut self) -> Effects {
        self.state = ReadStagingSourceState::ResolveConnector;
        smallvec![resolve_source_connector_suboperation(
            ResolveSourceConnectorInput {
                group_id: self.input.group_id,
                connector_id: self.input.connector_id,
                source_path: self.input.source_path.clone(),
            }
        )]
    }

    fn handle_resolved_connector(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::SourceConnectorResolved {
                result: Ok(resolved),
            }) => {
                self.connector = Some(resolved.connector);
                self.state = ReadStagingSourceState::ReadSource;
                smallvec![Effect::StagingSource(StagingSourceEffect::Read {
                    access: resolved.access,
                    range: self.input.range.clone(),
                })]
            }
            Event::SubOperation(SubOperationEvent::SourceConnectorResolved {
                result: Err(error),
            }) => self.emit_error(error.into()),
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
                    return self.emit_error(ReadStagingSourceError::ReadStagingSourceFailed);
                };

                self.state = ReadStagingSourceState::Finish;
                self.output = Some(Ok(ReadStagingSourceResult {
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

impl Operation for ReadStagingSourceOperation {
    type Output = ReadStagingSourceResult;
    type Error = ReadStagingSourceError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReadStagingSourceState::Init => self.handle_init(),
            ReadStagingSourceState::ResolveConnector => self.handle_resolved_connector(event),
            ReadStagingSourceState::ReadSource => self.handle_read_result(event),
            ReadStagingSourceState::Finish => smallvec![],
            ReadStagingSourceState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReadStagingSourceState::Finish | ReadStagingSourceState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ReadStagingSourceState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ReadStagingSourceError::ReadStagingSourceFailed);
        }

        self.output
            .ok_or(ReadStagingSourceError::ReadStagingSourceFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::staging::test_utils::{create_http_connector, setup_driver_context};
    use aruna_core::structs::{ResolvedSourceAccess, ResolvedSourceConnector, SourceConnectorKind};
    use futures_util::{StreamExt, stream};
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn sample_input() -> ReadStagingSourceInput {
        ReadStagingSourceInput {
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
    fn start_emits_resolve_connector_suboperation() {
        let mut operation = ReadStagingSourceOperation::new(sample_input());

        let effects = operation.start();

        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        assert_eq!(operation.state, ReadStagingSourceState::ResolveConnector);
    }

    #[test]
    fn resolved_connector_emits_read_effect_with_range() {
        let mut operation = ReadStagingSourceOperation::new(sample_input());
        operation.start();
        let resolved = sample_resolved_connector();
        let expected_access = resolved.access.clone();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Ok(resolved),
            },
        ));

        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::Read { access, range })]
                if access == &expected_access && range == &Some(5..11)
        ));
        assert_eq!(operation.state, ReadStagingSourceState::ReadSource);
    }

    #[test]
    fn resolve_error_is_exposed() {
        let mut operation = ReadStagingSourceOperation::new(sample_input());
        operation.start();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Err(SourceConnectorResolutionError::InvalidSourcePath),
            },
        ));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(ReadStagingSourceError::Resolve(
                SourceConnectorResolutionError::InvalidSourcePath,
            )),
        );
    }

    #[tokio::test]
    async fn read_result_finishes_operation_and_exposes_stream() {
        let mut operation = ReadStagingSourceOperation::new(sample_input());
        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Ok(sample_resolved_connector()),
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
    fn staging_error_is_exposed() {
        let mut operation = ReadStagingSourceOperation::new(sample_input());
        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Ok(sample_resolved_connector()),
            },
        ));

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::ReadError("boom".to_string()),
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(ReadStagingSourceError::Staging(
                StagingSourceError::ReadError("boom".to_string()),
            )),
        );
    }

    #[test]
    fn unexpected_event_uses_event_description() {
        let mut operation = ReadStagingSourceOperation::new(sample_input());
        operation.start();

        let effects = operation.step(Event::Task(aruna_core::task::TaskEvent::Error {
            key: None,
            message: "ignored".to_string(),
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(ReadStagingSourceError::UnexpectedEvent {
                state: ReadStagingSourceState::ResolveConnector,
                expected: "Event::SubOperation(SubOperationEvent::SourceConnectorResolved)",
                got: "Event::Task".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn read_operation_resolves_connector_and_hits_runtime() {
        let test_context = setup_driver_context().await;
        let group_id = Ulid::new();
        let connector =
            create_http_connector(&test_context.driver_context, group_id, "http://127.0.0.1:1")
                .await;

        let result = drive(
            ReadStagingSourceOperation::new(ReadStagingSourceInput {
                group_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".to_string(),
                range: None,
            }),
            &test_context.driver_context,
        )
        .await;

        assert!(matches!(result, Err(ReadStagingSourceError::Staging(_))));
    }
}
