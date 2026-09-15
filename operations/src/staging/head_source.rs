use crate::connectors::{ResolveConnectorInput, resolve_connector_effect};
use crate::staging::describe_event;
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{SourceResolutionError, StagingSourceError};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadSourceInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub source_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadSourceResult {
    pub connector: SourceConnector,
    pub secret_fingerprint: Option<[u8; 16]>,
    pub metadata: SourceMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeadSourceState {
    Init,
    ResolveConnector,
    HeadSource,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum HeadSourceError {
    #[error(transparent)]
    Resolve(#[from] SourceResolutionError),
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: HeadSourceState,
        expected: &'static str,
        got: String,
    },
    #[error("Head staging source failed")]
    HeadStagingSourceFailed,
}

#[derive(Debug, PartialEq)]
pub struct HeadSourceOperation {
    input: HeadSourceInput,
    state: HeadSourceState,
    connector: Option<SourceConnector>,
    secret_fingerprint: Option<[u8; 16]>,
    output: Option<Result<HeadSourceResult, HeadSourceError>>,
}

impl HeadSourceOperation {
    pub fn new(input: HeadSourceInput) -> Self {
        Self {
            input,
            state: HeadSourceState::Init,
            connector: None,
            secret_fingerprint: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: HeadSourceError) -> Effects {
        self.state = HeadSourceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn emit_unexpected(&mut self, expected: &'static str, event: &Event) -> Effects {
        self.emit_error(HeadSourceError::UnexpectedEvent {
            state: self.state.clone(),
            expected,
            got: describe_event(event),
        })
    }

    fn handle_init(&mut self) -> Effects {
        self.state = HeadSourceState::ResolveConnector;
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
                        self.secret_fingerprint = resolved.secret_fingerprint;
                        self.state = HeadSourceState::HeadSource;
                        smallvec![Effect::StagingSource(StagingSourceEffect::Head {
                            access: resolved.access,
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

    fn handle_head_result(&mut self, event: Event) -> Effects {
        match event {
            Event::StagingSource(StagingSourceEvent::HeadResult { metadata }) => {
                let Some(connector) = self.connector.clone() else {
                    return self.emit_error(HeadSourceError::HeadStagingSourceFailed);
                };

                self.state = HeadSourceState::Finish;
                self.output = Some(Ok(HeadSourceResult {
                    connector,
                    secret_fingerprint: self.secret_fingerprint,
                    metadata,
                }));
                smallvec![]
            }
            Event::StagingSource(StagingSourceEvent::Error { error }) => {
                self.emit_error(error.into())
            }
            other => self.emit_unexpected(
                "Event::StagingSource(StagingSourceEvent::HeadResult)",
                &other,
            ),
        }
    }
}

impl Operation for HeadSourceOperation {
    type Output = HeadSourceResult;
    type Error = HeadSourceError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            HeadSourceState::Init => self.handle_init(),
            HeadSourceState::ResolveConnector => self.handle_resolved_connector(event),
            HeadSourceState::HeadSource => self.handle_head_result(event),
            HeadSourceState::Finish => smallvec![],
            HeadSourceState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, HeadSourceState::Finish | HeadSourceState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == HeadSourceState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(HeadSourceError::HeadStagingSourceFailed);
        }

        self.output
            .ok_or(HeadSourceError::HeadStagingSourceFailed)?
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
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn sample_input() -> HeadSourceInput {
        HeadSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
            source_path: "folder/file.txt".to_string(),
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
            content_length: 42,
            content_type: Some("application/octet-stream".to_string()),
            etag: Some("etag-1".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
        }
    }

    #[test]
    fn start_emits_resolve() {
        let mut operation = HeadSourceOperation::new(sample_input());

        let effects = operation.start();

        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        assert_eq!(operation.state, HeadSourceState::ResolveConnector);
    }

    #[test]
    fn resolved_emits_head() {
        let mut operation = HeadSourceOperation::new(sample_input());
        operation.start();
        let resolved = sample_resolved_connector();
        let expected_access = resolved.access.clone();
        let expected_connector = resolved.connector.clone();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Ok(resolved)),
            },
        ));

        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::Head { access })]
                if access == &expected_access
        ));
        assert_eq!(operation.state, HeadSourceState::HeadSource);
        assert_eq!(operation.connector, Some(expected_connector));
    }

    #[test]
    fn exposes_resolve_error() {
        let mut operation = HeadSourceOperation::new(sample_input());
        operation.start();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Err(SourceResolutionError::NotFound)),
            },
        ));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(HeadSourceError::Resolve(SourceResolutionError::NotFound,)),
        );
    }

    #[test]
    fn head_finishes_operation() {
        let mut operation = HeadSourceOperation::new(sample_input());
        operation.start();
        let expected_connector = sample_connector();
        let expected_metadata = sample_metadata();
        let resolved = sample_resolved_connector();
        operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Ok(resolved)),
            },
        ));

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::HeadResult {
            metadata: expected_metadata.clone(),
        }));

        assert!(effects.is_empty());
        assert_eq!(operation.state, HeadSourceState::Finish);
        assert_eq!(
            operation.finalize(),
            Ok(HeadSourceResult {
                connector: expected_connector,
                secret_fingerprint: None,
                metadata: expected_metadata,
            })
        );
    }

    #[test]
    fn exposes_staging_error() {
        let mut operation = HeadSourceOperation::new(sample_input());
        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Ok(sample_resolved_connector())),
            },
        ));

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::NotFound,
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(HeadSourceError::Staging(StagingSourceError::NotFound,)),
        );
    }

    #[test]
    fn unexpected_describes_event() {
        let mut operation = HeadSourceOperation::new(sample_input());
        operation.start();

        let effects = operation.step(Event::Search());

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(HeadSourceError::UnexpectedEvent {
                state: HeadSourceState::ResolveConnector,
                expected: "Event::SubOperation(SubOperationEvent::SourceConnectorResolved)",
                got: "Event::Search".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn head_hits_runtime() {
        let test_context = setup_driver_context().await;
        let group_id = Ulid::generate();
        let connector =
            create_http_connector(&test_context.driver_context, group_id, "http://127.0.0.1:1")
                .await;

        let result = drive(
            HeadSourceOperation::new(HeadSourceInput {
                group_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".to_string(),
            }),
            &test_context.driver_context,
        )
        .await;

        assert!(matches!(result, Err(HeadSourceError::Staging(_))));
    }
}
