use crate::connectors::{ResolveConnectorInput, resolve_connector_effect};
use crate::staging::describe_event;
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{SourceResolutionError, StagingSourceError};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::SourceEntry;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListStagingInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub offset: usize,
    pub limit: usize,
    pub recursive: bool,
    pub files_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListStagingResult {
    pub entries: Vec<SourceEntry>,
    pub truncated: bool,
    pub next_offset: Option<usize>,
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
    state: ListStagingState,
    output: Option<Result<ListStagingResult, ListStagingError>>,
}

impl ListStagingOperation {
    pub fn new(input: ListStagingInput) -> Self {
        Self {
            input,
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
                    smallvec![Effect::StagingSource(StagingSourceEffect::List {
                        access: resolved.access,
                        offset: self.input.offset,
                        limit: self.input.limit,
                        recursive: self.input.recursive,
                        files_only: self.input.files_only,
                    })]
                }
                Err(error) => self.emit_error(error.into()),
            },
            (
                ListStagingState::List,
                Event::StagingSource(StagingSourceEvent::ListResult { entries, truncated }),
            ) => {
                self.state = ListStagingState::Finish;
                let next_offset = truncated.then_some(self.input.offset + entries.len());
                self.output = Some(Ok(ListStagingResult {
                    entries,
                    truncated,
                    next_offset,
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
    use aruna_core::structs::{
        ResolvedSourceAccess, ResolvedSourceConnector, SourceConnector, SourceConnectorKind,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn sample_input() -> ListStagingInput {
        ListStagingInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
            source_path: "prefix".to_string(),
            offset: 0,
            limit: 20,
            recursive: true,
            files_only: true,
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
            [Effect::StagingSource(StagingSourceEffect::List {
                limit: 20,
                offset: 0,
                recursive: true,
                files_only: true,
                ..
            })]
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
    fn list_preserves_truncation() {
        let mut operation = ListStagingOperation::new(sample_input());
        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Ok(sample_connector())),
            },
        ));

        operation.step(Event::StagingSource(StagingSourceEvent::ListResult {
            entries: Vec::new(),
            truncated: true,
        }));

        assert_eq!(
            operation.finalize(),
            Ok(ListStagingResult {
                entries: Vec::new(),
                truncated: true,
                next_offset: Some(0),
            })
        );
    }
}
