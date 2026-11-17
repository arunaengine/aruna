//! Resolves a connector and heads one staging source path to get its metadata.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::child_call;
use crate::connectors::{ResolveConnectorInput, resolve_connector_effect};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{SourceResolutionError, StagingSourceError};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::types::GroupId;
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

#[derive(Debug, Error, PartialEq)]
pub enum HeadSourceError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Resolve(#[from] SourceResolutionError),
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("unexpected staging answer {0}")]
    UnexpectedEvent(&'static str),
}

/// Resolves the connector, then heads the source path.
#[derive(Debug, PartialEq)]
pub struct HeadSourceOperation {
    input: HeadSourceInput,
    resolved: Option<(SourceConnector, Option<[u8; 16]>)>,
}

impl HeadSourceOperation {
    pub fn new(input: HeadSourceInput) -> Self {
        Self {
            input,
            resolved: None,
        }
    }

    fn head(&mut self, event: Event) -> Result<IoCall, HeadSourceError> {
        let Event::SubOperation(SubOperationEvent::SourceConnectorResolved { result }) = event
        else {
            return Err(HeadSourceError::UnexpectedEvent(event.label()));
        };
        let resolved = (*result)?;
        self.resolved = Some((resolved.connector, resolved.secret_fingerprint));
        let head = StagingSourceEffect::Head {
            access: resolved.access,
        };
        Ok(IoCall::Effect(Box::new(Effect::StagingSource(head))))
    }
}

impl Operation for HeadSourceOperation {
    type Output = HeadSourceResult;
    type Error = HeadSourceError;

    fn first(&mut self) -> Result<Next<HeadSourceResult>, HeadSourceError> {
        Ok(Next::Call(child_call(resolve_connector_effect(
            ResolveConnectorInput {
                group_id: self.input.group_id,
                connector_id: self.input.connector_id,
                source_path: self.input.source_path.clone(),
                allow_root: false,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<HeadSourceResult>, HeadSourceError> {
        let event = result.event()?;
        let Some((connector, secret_fingerprint)) = self.resolved.take() else {
            return Ok(Next::Call(self.head(event)?));
        };
        match event {
            Event::StagingSource(StagingSourceEvent::HeadResult { metadata }) => {
                Ok(Next::Done(HeadSourceResult {
                    connector,
                    secret_fingerprint,
                    metadata,
                }))
            }
            Event::StagingSource(StagingSourceEvent::Error { error }) => Err(error.into()),
            other => Err(HeadSourceError::UnexpectedEvent(other.label())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use aruna_core::structs::execution::source_access::ResolvedSourceConnector;
    use crate::tests::staging::{
        create_http_connector, sample_connector, sample_metadata, sample_resolved_connector,
        setup_driver_context,
    };

    fn sample_input() -> HeadSourceInput {
        HeadSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
            source_path: "folder/file.txt".to_string(),
        }
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

    #[test]
    fn resolved_emits_head() {
        let mut operation = HeadSourceOperation::new(sample_input());
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let resolved_connector = sample_resolved_connector();
        let head = StagingSourceEffect::Head {
            access: resolved_connector.access.clone(),
        };
        let call = IoCall::Effect(Box::new(Effect::StagingSource(head)));
        let effect = operation.next(resolved(Ok(resolved_connector)));
        assert_eq!(effect, Ok(Next::Call(call)));
    }

    #[test]
    fn exposes_resolve_error() {
        let mut operation = HeadSourceOperation::new(sample_input());
        operation.first().unwrap();
        assert_eq!(
            operation.next(resolved(Err(SourceResolutionError::NotFound))),
            Err(HeadSourceError::Resolve(SourceResolutionError::NotFound)),
        );
    }

    #[test]
    fn head_finishes_operation() {
        let mut operation = HeadSourceOperation::new(sample_input());
        operation.first().unwrap();
        operation
            .next(resolved(Ok(sample_resolved_connector())))
            .unwrap();
        let headed = answer(Event::StagingSource(StagingSourceEvent::HeadResult {
            metadata: sample_metadata(),
        }));
        assert_eq!(
            operation.next(headed),
            Ok(Next::Done(HeadSourceResult {
                connector: sample_connector(),
                secret_fingerprint: None,
                metadata: sample_metadata(),
            }))
        );
    }

    #[test]
    fn exposes_staging_error() {
        let mut operation = HeadSourceOperation::new(sample_input());
        operation.first().unwrap();
        operation
            .next(resolved(Ok(sample_resolved_connector())))
            .unwrap();
        let failed = answer(Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::NotFound,
        }));
        assert_eq!(
            operation.next(failed),
            Err(HeadSourceError::Staging(StagingSourceError::NotFound)),
        );
    }

    #[test]
    fn unexpected_describes_event() {
        let mut operation = HeadSourceOperation::new(sample_input());
        operation.first().unwrap();
        assert_eq!(
            operation.next(answer(Event::Search())),
            Err(HeadSourceError::UnexpectedEvent("Event::Search"))
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
