//! Resolves a connector and lists one staging source path in pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::{ResolveConnectorInput, resolve_connector_effect};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{SourceResolutionError, StagingSourceError};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::{ListCursor, ListRequest, SourceEntry};
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::types::GroupId;
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

#[derive(Debug, Error, PartialEq)]
pub enum ListStagingError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Resolve(#[from] SourceResolutionError),
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("unexpected staging answer {0}")]
    UnexpectedEvent(&'static str),
}

/// Resolves the connector, then lists one page of the source path.
#[derive(Debug, PartialEq)]
pub struct ListStagingOperation {
    input: ListStagingInput,
    resolved: Option<(SourceConnector, Option<[u8; 16]>)>,
}

impl ListStagingOperation {
    pub fn new(input: ListStagingInput) -> Self {
        Self {
            input,
            resolved: None,
        }
    }

    fn list(&mut self, event: Event) -> Result<IoCall, ListStagingError> {
        let Event::SubOperation(SubOperationEvent::SourceConnectorResolved { result }) = event
        else {
            return Err(ListStagingError::UnexpectedEvent(event.label()));
        };
        let resolved = (*result)?;
        self.resolved = Some((resolved.connector, resolved.secret_fingerprint));
        let list = StagingSourceEffect::List {
            access: resolved.access,
            request: self.input.request.clone(),
        };
        Ok(IoCall::Effect(Box::new(Effect::StagingSource(list))))
    }
}

impl Operation for ListStagingOperation {
    type Output = ListStagingResult;
    type Error = ListStagingError;

    fn first(&mut self) -> Result<Next<ListStagingResult>, ListStagingError> {
        Ok(Next::Call(IoCall::from(resolve_connector_effect(
            ResolveConnectorInput {
                group_id: self.input.group_id,
                connector_id: self.input.connector_id,
                source_path: self.input.source_path.clone(),
                allow_root: true,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ListStagingResult>, ListStagingError> {
        let event = result.event()?;
        let Some((connector, secret_fingerprint)) = self.resolved.take() else {
            return Ok(Next::Call(self.list(event)?));
        };
        match event {
            Event::StagingSource(StagingSourceEvent::ListResult {
                entries,
                next_cursor,
            }) => Ok(Next::Done(ListStagingResult {
                entries,
                next_cursor,
                connector,
                secret_fingerprint,
            })),
            Event::StagingSource(StagingSourceEvent::Error { error }) => Err(error.into()),
            other => Err(ListStagingError::UnexpectedEvent(other.label())),
        }
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

    fn resolved() -> IoResult {
        IoResult::Event(Box::new(Event::SubOperation(
            SubOperationEvent::SourceConnectorResolved {
                result: Box::new(Ok(sample_connector())),
            },
        )))
    }

    #[test]
    fn list_emits_options() {
        let mut operation = ListStagingOperation::new(sample_input());
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let Ok(Next::Call(IoCall::Effect(effect))) = operation.next(resolved()) else {
            panic!("expected the list effect");
        };
        assert!(matches!(
            *effect,
            Effect::StagingSource(StagingSourceEffect::List { request, .. })
                if request.limit == 20 && request.recursive && request.files_only
                    && request.cursor.is_none()
        ));
    }

    #[test]
    fn list_rejects_event() {
        let mut operation = ListStagingOperation::new(sample_input());
        operation.first().unwrap();
        let search = IoResult::Event(Box::new(Event::Search()));
        assert!(matches!(
            operation.next(search),
            Err(ListStagingError::UnexpectedEvent(_))
        ));
    }

    #[test]
    fn list_returns_cursor() {
        let mut operation = ListStagingOperation::new(sample_input());
        operation.first().unwrap();
        operation.next(resolved()).unwrap();
        let cursor = Some(ListCursor::After("prefix/a".to_string()));
        let listed = IoResult::Event(Box::new(Event::StagingSource(
            StagingSourceEvent::ListResult {
                entries: Vec::new(),
                next_cursor: cursor.clone(),
            },
        )));
        let Ok(Next::Done(result)) = operation.next(listed) else {
            panic!("expected the page");
        };
        assert_eq!(result.next_cursor, cursor);
        assert_eq!(result.connector, sample_connector().connector);
    }
}
