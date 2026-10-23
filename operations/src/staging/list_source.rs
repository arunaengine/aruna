//! Resolves a connector and lists one staging source path in pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::{ResolveConnectorInput, resolve_connector_effect};
use crate::staging::describe_event;
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{SourceResolutionError, StagingSourceError};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::{ListCursor, ListRequest, SourceEntry};
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListStagingInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub request: ListRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListStagingResult {
    pub entries: Vec<SourceEntry>,
    /// `None` once the listing is exhausted or reached its stop key.
    pub next_cursor: Option<ListCursor>,
    /// The connector as resolved for this page, for writes that re-check it at commit.
    pub connector: SourceConnector,
    pub secret_fingerprint: Option<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListStagingState {
    Init,
    Resolve,
    List,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListStagingError {
    #[error(transparent)]
    Resolve(#[from] SourceResolutionError),
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: ListStagingState,
        expected: &'static str,
        got: String,
    },
    #[error("List staging source failed")]
    ListFailed,
}

#[derive(Debug, PartialEq)]
pub struct ListStagingOperation {
    input: ListStagingInput,
    resolved: Option<(SourceConnector, Option<[u8; 16]>)>,
    state: ListStagingState,
    output: Option<Result<ListStagingResult, ListStagingError>>,
}

impl ListStagingOperation {
    pub fn new(input: ListStagingInput) -> Self {
        Self {
            input,
            resolved: None,
            state: ListStagingState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListStagingError) -> Effects {
        self.state = ListStagingState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for ListStagingOperation {
    type Output = ListStagingResult;
    type Error = ListStagingError;

    fn start(&mut self) -> Effects {
        self.state = ListStagingState::Resolve;
        smallvec![resolve_connector_effect(ResolveConnectorInput {
            group_id: self.input.group_id,
            connector_id: self.input.connector_id,
            source_path: self.input.source_path.clone(),
            allow_root: true,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (
                ListStagingState::Resolve,
                Event::SubOperation(SubOperationEvent::SourceConnectorResolved { result }),
            ) => match *result {
                Ok(resolved) => {
                    self.state = ListStagingState::List;
                    self.resolved = Some((resolved.connector, resolved.secret_fingerprint));
                    smallvec![Effect::StagingSource(StagingSourceEffect::List {
                        access: resolved.access,
                        request: self.input.request.clone(),
                    })]
                }
                Err(error) => self.emit_error(error.into()),
            },
            (
                ListStagingState::List,
                Event::StagingSource(StagingSourceEvent::ListResult {
                    entries,
                    next_cursor,
                }),
            ) => {
                let Some((connector, secret_fingerprint)) = self.resolved.take() else {
                    return self.emit_error(ListStagingError::ListFailed);
                };
                self.state = ListStagingState::Finish;
                self.output = Some(Ok(ListStagingResult {
                    entries,
                    next_cursor,
                    connector,
                    secret_fingerprint,
                }));
                smallvec![]
            }
            (ListStagingState::List, Event::StagingSource(StagingSourceEvent::Error { error })) => {
                self.emit_error(error.into())
            }
            (ListStagingState::Finish, _) => smallvec![],
            (ListStagingState::Error, _) => self.abort(),
            (ListStagingState::Resolve, event) => {
                self.emit_error(ListStagingError::UnexpectedEvent {
                    state: self.state.clone(),
                    expected: "Event::SubOperation(SubOperationEvent::SourceConnectorResolved)",
                    got: describe_event(&event),
                })
            }
            (_, event) => self.emit_error(ListStagingError::UnexpectedEvent {
                state: self.state.clone(),
                expected: "Event::StagingSource(StagingSourceEvent::ListResult)",
                got: describe_event(&event),
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListStagingState::Finish | ListStagingState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ListStagingError::ListFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::structs::execution::source_access::{
        ResolvedSourceAccess, ResolvedSourceConnector,
    };
    use aruna_core::structs::execution::source_connector::{SourceConnector, SourceConnectorKind};
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn sample_input() -> ListStagingInput {
        ListStagingInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
            source_path: "prefix".to_string(),
            request: ListRequest {
                limit: 20,
                recursive: true,
                files_only: true,
                ..Default::default()
            },
        }
    }

    fn sample_connector() -> ResolvedSourceConnector {
        ResolvedSourceConnector {
            connector: SourceConnector::new(
                Ulid::from_bytes([2u8; 16]),
                Ulid::from_bytes([1u8; 16]),
                "source".to_string(),
                SourceConnectorKind::Http,
                HashMap::new(),
                SystemTime::UNIX_EPOCH,
                SystemTime::UNIX_EPOCH,
                Default::default(),
            ),
            secret_fingerprint: None,
            access: ResolvedSourceAccess::OpenDal {
                kind: SourceConnectorKind::Http,
                config: HashMap::new(),
                path: "prefix".to_string(),
                version: None,
            },
        }
    }

    #[test]
    fn list_emits_options() {
        let mut operation = ListStagingOperation::new(sample_input());
        operation.start();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Ok(sample_connector())),
            },
        ));

        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::List { request, .. })]
                if request.limit == 20 && request.recursive && request.files_only
                    && request.cursor.is_none()
        ));
    }

    #[test]
    fn list_rejects_event() {
        let mut operation = ListStagingOperation::new(sample_input());
        operation.start();

        let effects = operation.step(Event::Search());

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(ListStagingError::UnexpectedEvent { .. })
        ));
    }

    #[test]
    fn list_returns_cursor() {
        let mut operation = ListStagingOperation::new(sample_input());
        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Ok(sample_connector())),
            },
        ));

        let cursor = Some(ListCursor::After("prefix/a".to_string()));
        operation.step(Event::StagingSource(StagingSourceEvent::ListResult {
            entries: Vec::new(),
            next_cursor: cursor.clone(),
        }));

        let result = operation.finalize().unwrap();
        assert_eq!(result.next_cursor, cursor);
        assert_eq!(result.connector, sample_connector().connector);
    }
}
