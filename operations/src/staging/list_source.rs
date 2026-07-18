use crate::connectors::{ResolveSourceConnectorInput, resolve_source_connector_suboperation};
use crate::staging::describe_event;
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{SourceConnectorResolutionError, StagingSourceError};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::SourceEntry;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListStagingSourceInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub limit: usize,
    pub recursive: bool,
    pub files_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListStagingSourceResult {
    pub entries: Vec<SourceEntry>,
    pub truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListStagingSourceState {
    Init,
    Resolve,
    List,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListStagingSourceError {
    #[error(transparent)]
    Resolve(#[from] SourceConnectorResolutionError),
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: ListStagingSourceState,
        expected: &'static str,
        got: String,
    },
    #[error("List staging source failed")]
    ListFailed,
}

#[derive(Debug, PartialEq)]
pub struct ListStagingSourceOperation {
    input: ListStagingSourceInput,
    state: ListStagingSourceState,
    output: Option<Result<ListStagingSourceResult, ListStagingSourceError>>,
}

impl ListStagingSourceOperation {
    pub fn new(input: ListStagingSourceInput) -> Self {
        Self {
            input,
            state: ListStagingSourceState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListStagingSourceError) -> Effects {
        self.state = ListStagingSourceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for ListStagingSourceOperation {
    type Output = ListStagingSourceResult;
    type Error = ListStagingSourceError;

    fn start(&mut self) -> Effects {
        self.state = ListStagingSourceState::Resolve;
        smallvec![resolve_source_connector_suboperation(
            ResolveSourceConnectorInput {
                group_id: self.input.group_id,
                connector_id: self.input.connector_id,
                source_path: self.input.source_path.clone(),
                allow_root: true,
            }
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (
                ListStagingSourceState::Resolve,
                Event::SubOperation(SubOperationEvent::SourceConnectorResolved { result }),
            ) => match *result {
                Ok(resolved) => {
                    self.state = ListStagingSourceState::List;
                    smallvec![Effect::StagingSource(StagingSourceEffect::List {
                        access: resolved.access,
                        limit: self.input.limit,
                        recursive: self.input.recursive,
                        files_only: self.input.files_only,
                    })]
                }
                Err(error) => self.emit_error(error.into()),
            },
            (
                ListStagingSourceState::List,
                Event::StagingSource(StagingSourceEvent::ListResult { entries, truncated }),
            ) => {
                self.state = ListStagingSourceState::Finish;
                self.output = Some(Ok(ListStagingSourceResult { entries, truncated }));
                smallvec![]
            }
            (
                ListStagingSourceState::List,
                Event::StagingSource(StagingSourceEvent::Error { error }),
            ) => self.emit_error(error.into()),
            (ListStagingSourceState::Finish, _) => smallvec![],
            (ListStagingSourceState::Error, _) => self.abort(),
            (ListStagingSourceState::Resolve, event) => {
                self.emit_error(ListStagingSourceError::UnexpectedEvent {
                    state: self.state.clone(),
                    expected: "Event::SubOperation(SubOperationEvent::SourceConnectorResolved)",
                    got: describe_event(&event),
                })
            }
            (_, event) => self.emit_error(ListStagingSourceError::UnexpectedEvent {
                state: self.state.clone(),
                expected: "Event::StagingSource(StagingSourceEvent::ListResult)",
                got: describe_event(&event),
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListStagingSourceState::Finish | ListStagingSourceState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ListStagingSourceError::ListFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{
        ResolvedSourceAccess, ResolvedSourceConnector, SourceConnector, SourceConnectorKind,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn sample_input() -> ListStagingSourceInput {
        ListStagingSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
            source_path: "prefix".to_string(),
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
        let mut operation = ListStagingSourceOperation::new(sample_input());
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
                recursive: true,
                files_only: true,
                ..
            })]
        ));
    }

    #[test]
    fn list_preserves_truncation() {
        let mut operation = ListStagingSourceOperation::new(sample_input());
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
            Ok(ListStagingSourceResult {
                entries: Vec::new(),
                truncated: true,
            })
        );
    }
}
