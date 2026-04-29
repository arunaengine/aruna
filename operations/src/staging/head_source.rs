use crate::connectors::{ResolveSourceConnectorInput, resolve_source_connector_suboperation};
use crate::staging::describe_event;
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{SourceConnectorResolutionError, StagingSourceError};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{SourceConnector, SourceMetadata};
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadStagingSourceInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub source_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadStagingSourceResult {
    pub connector: SourceConnector,
    pub secret_fingerprint: Option<[u8; 16]>,
    pub metadata: SourceMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeadStagingSourceState {
    Init,
    ResolveConnector,
    HeadSource,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum HeadStagingSourceError {
    #[error(transparent)]
    Resolve(#[from] SourceConnectorResolutionError),
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: HeadStagingSourceState,
        expected: &'static str,
        got: String,
    },
    #[error("Head staging source failed")]
    HeadStagingSourceFailed,
}

#[derive(Debug, PartialEq)]
pub struct HeadStagingSourceOperation {
    input: HeadStagingSourceInput,
    state: HeadStagingSourceState,
    connector: Option<SourceConnector>,
    secret_fingerprint: Option<[u8; 16]>,
    output: Option<Result<HeadStagingSourceResult, HeadStagingSourceError>>,
}

impl HeadStagingSourceOperation {
    pub fn new(input: HeadStagingSourceInput) -> Self {
        Self {
            input,
            state: HeadStagingSourceState::Init,
            connector: None,
            secret_fingerprint: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: HeadStagingSourceError) -> Effects {
        self.state = HeadStagingSourceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn emit_unexpected(&mut self, expected: &'static str, event: &Event) -> Effects {
        self.emit_error(HeadStagingSourceError::UnexpectedEvent {
            state: self.state.clone(),
            expected,
            got: describe_event(event),
        })
    }

    fn handle_init(&mut self) -> Effects {
        self.state = HeadStagingSourceState::ResolveConnector;
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
                self.secret_fingerprint = resolved.secret_fingerprint;
                self.state = HeadStagingSourceState::HeadSource;
                smallvec![Effect::StagingSource(StagingSourceEffect::Head {
                    access: resolved.access,
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

    fn handle_head_result(&mut self, event: Event) -> Effects {
        match event {
            Event::StagingSource(StagingSourceEvent::HeadResult { metadata }) => {
                let Some(connector) = self.connector.clone() else {
                    return self.emit_error(HeadStagingSourceError::HeadStagingSourceFailed);
                };

                self.state = HeadStagingSourceState::Finish;
                self.output = Some(Ok(HeadStagingSourceResult {
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

impl Operation for HeadStagingSourceOperation {
    type Output = HeadStagingSourceResult;
    type Error = HeadStagingSourceError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            HeadStagingSourceState::Init => self.handle_init(),
            HeadStagingSourceState::ResolveConnector => self.handle_resolved_connector(event),
            HeadStagingSourceState::HeadSource => self.handle_head_result(event),
            HeadStagingSourceState::Finish => smallvec![],
            HeadStagingSourceState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            HeadStagingSourceState::Finish | HeadStagingSourceState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == HeadStagingSourceState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(HeadStagingSourceError::HeadStagingSourceFailed);
        }

        self.output
            .ok_or(HeadStagingSourceError::HeadStagingSourceFailed)?
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
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn sample_input() -> HeadStagingSourceInput {
        HeadStagingSourceInput {
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
    fn start_emits_resolve_connector_suboperation() {
        let mut operation = HeadStagingSourceOperation::new(sample_input());

        let effects = operation.start();

        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        assert_eq!(operation.state, HeadStagingSourceState::ResolveConnector);
    }

    #[test]
    fn resolved_connector_emits_head_effect() {
        let mut operation = HeadStagingSourceOperation::new(sample_input());
        operation.start();
        let resolved = sample_resolved_connector();
        let expected_access = resolved.access.clone();
        let expected_connector = resolved.connector.clone();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Ok(resolved),
            },
        ));

        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::Head { access })]
                if access == &expected_access
        ));
        assert_eq!(operation.state, HeadStagingSourceState::HeadSource);
        assert_eq!(operation.connector, Some(expected_connector));
    }

    #[test]
    fn resolve_error_is_exposed() {
        let mut operation = HeadStagingSourceOperation::new(sample_input());
        operation.start();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Err(SourceConnectorResolutionError::NotFound),
            },
        ));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(HeadStagingSourceError::Resolve(
                SourceConnectorResolutionError::NotFound,
            )),
        );
    }

    #[test]
    fn head_result_finishes_operation() {
        let mut operation = HeadStagingSourceOperation::new(sample_input());
        operation.start();
        let expected_connector = sample_connector();
        let expected_metadata = sample_metadata();
        let resolved = sample_resolved_connector();
        operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Ok(resolved),
            },
        ));

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::HeadResult {
            metadata: expected_metadata.clone(),
        }));

        assert!(effects.is_empty());
        assert_eq!(operation.state, HeadStagingSourceState::Finish);
        assert_eq!(
            operation.finalize(),
            Ok(HeadStagingSourceResult {
                connector: expected_connector,
                secret_fingerprint: None,
                metadata: expected_metadata,
            })
        );
    }

    #[test]
    fn staging_error_is_exposed() {
        let mut operation = HeadStagingSourceOperation::new(sample_input());
        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Ok(sample_resolved_connector()),
            },
        ));

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::NotFound,
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(HeadStagingSourceError::Staging(
                StagingSourceError::NotFound,
            )),
        );
    }

    #[test]
    fn unexpected_event_uses_event_description() {
        let mut operation = HeadStagingSourceOperation::new(sample_input());
        operation.start();

        let effects = operation.step(Event::Search());

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(HeadStagingSourceError::UnexpectedEvent {
                state: HeadStagingSourceState::ResolveConnector,
                expected: "Event::SubOperation(SubOperationEvent::SourceConnectorResolved)",
                got: "Event::Search".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn head_operation_resolves_connector_and_hits_runtime() {
        let test_context = setup_driver_context().await;
        let group_id = Ulid::new();
        let connector =
            create_http_connector(&test_context.driver_context, group_id, "http://127.0.0.1:1")
                .await;

        let result = drive(
            HeadStagingSourceOperation::new(HeadStagingSourceInput {
                group_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".to_string(),
            }),
            &test_context.driver_context,
        )
        .await;

        assert!(matches!(result, Err(HeadStagingSourceError::Staging(_))));
    }
}
