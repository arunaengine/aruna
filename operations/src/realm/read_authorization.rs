//! Reads the realm authorization document and reports absence when no document is stored.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use crate::document_repository::read_effect;

#[derive(Debug, PartialEq)]
pub struct ReadAuthorizationOperation {
    realm_id: RealmId,
    state: ReadAuthorizationState,
    output: Option<Result<Option<RealmAuthorizationDocument>, ReadAuthorizationError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ReadAuthorizationState {
    Init,
    ReadAuthorization,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadAuthorizationError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("read realm authorization did not finish")]
    NotFinished,
}

impl ReadAuthorizationOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            state: ReadAuthorizationState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ReadAuthorizationError) -> Effects {
        self.state = ReadAuthorizationState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_authorization_read(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                let result = value
                    .map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes).map_err(Into::into))
                    .transpose();
                self.state = ReadAuthorizationState::Finish;
                self.output = Some(result);
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.fail(ReadAuthorizationError::UnexpectedEvent {
                state: format!("{:?}", self.state),
                expected: "storage read result",
                got: format!("{other:?}"),
            }),
        }
    }
}

impl Operation for ReadAuthorizationOperation {
    type Output = Option<RealmAuthorizationDocument>;
    type Error = ReadAuthorizationError;

    fn start(&mut self) -> Effects {
        self.state = ReadAuthorizationState::ReadAuthorization;
        smallvec![read_effect(
            &DocumentTarget::RealmAuthorization {
                realm_id: self.realm_id,
            },
            None,
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReadAuthorizationState::ReadAuthorization => self.handle_authorization_read(event),
            ReadAuthorizationState::Init
            | ReadAuthorizationState::Finish
            | ReadAuthorizationState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReadAuthorizationState::Finish | ReadAuthorizationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ReadAuthorizationError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
