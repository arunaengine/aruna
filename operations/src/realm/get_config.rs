//! Reads the realm config document, alone or inside a transaction the caller already started.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::TxnId;
use smallvec::smallvec;
use thiserror::Error;

use crate::document_repository::read_effect;

#[derive(Debug, PartialEq)]
pub struct GetConfigOperation {
    document: DocumentTarget,
    txn_id: Option<TxnId>,
    external_txn: bool,
    state: GetConfigState,
    output: Option<Result<RealmConfigDocument, GetConfigError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum GetConfigState {
    Init,
    ReadDocument,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetConfigError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("document not found")]
    DocumentNotFound,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl GetConfigOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            document: DocumentTarget::RealmConfig { realm_id },
            txn_id: None,
            external_txn: false,
            state: GetConfigState::Init,
            output: None,
        }
    }

    pub fn new_with_txn(realm_id: RealmId, txn_id: TxnId) -> Self {
        Self {
            document: DocumentTarget::RealmConfig { realm_id },
            txn_id: Some(txn_id),
            external_txn: true,
            state: GetConfigState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GetConfigError) -> aruna_core::types::Effects {
        self.state = GetConfigState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(GetConfigError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for GetConfigOperation {
    type Output = RealmConfigDocument;
    type Error = GetConfigError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = GetConfigState::ReadDocument;
        smallvec![read_effect(&self.document, self.txn_id)]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            GetConfigState::ReadDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return self.fail(GetConfigError::DocumentNotFound);
                    };
                    match RealmConfigDocument::from_bytes(&value) {
                        Ok(document) => {
                            self.state = GetConfigState::Finish;
                            self.output = Some(Ok(document));
                            smallvec![]
                        }
                        Err(error) => self.fail(error.into()),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            GetConfigState::Finish | GetConfigState::Error | GetConfigState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetConfigState::Finish | GetConfigState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GetConfigError::NotFinished))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        if self.external_txn {
            return smallvec![];
        }
        smallvec![]
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    #[test]
    fn incomplete_finalize_errors() {
        // A deadline can finalize an operation that never reached a terminal
        // state; that must report an error instead of panicking.
        let mut operation = GetConfigOperation::new(RealmId::from_bytes([7; 32]));
        operation.start();

        assert!(!operation.is_complete());
        assert_eq!(operation.finalize(), Err(GetConfigError::NotFinished));
    }
}
