use aruna_core::document::DocumentSyncTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{RealmConfigDocument, RealmId};
use aruna_core::types::TxnId;
use smallvec::smallvec;
use thiserror::Error;

use crate::document_repository::read_effect;

#[derive(Debug, PartialEq)]
pub struct GetRealmConfigOperation {
    document: DocumentSyncTarget,
    txn_id: Option<TxnId>,
    external_txn: bool,
    state: GetRealmConfigState,
    output: Option<Result<RealmConfigDocument, GetRealmConfigError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum GetRealmConfigState {
    Init,
    ReadDocument,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetRealmConfigError {
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

impl GetRealmConfigOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            document: DocumentSyncTarget::RealmConfig { realm_id },
            txn_id: None,
            external_txn: false,
            state: GetRealmConfigState::Init,
            output: None,
        }
    }

    pub fn new_with_txn(realm_id: RealmId, txn_id: TxnId) -> Self {
        Self {
            document: DocumentSyncTarget::RealmConfig { realm_id },
            txn_id: Some(txn_id),
            external_txn: true,
            state: GetRealmConfigState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GetRealmConfigError) -> aruna_core::types::Effects {
        self.state = GetRealmConfigState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(GetRealmConfigError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for GetRealmConfigOperation {
    type Output = RealmConfigDocument;
    type Error = GetRealmConfigError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = GetRealmConfigState::ReadDocument;
        smallvec![read_effect(&self.document, self.txn_id)]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            GetRealmConfigState::ReadDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return self.fail(GetRealmConfigError::DocumentNotFound);
                    };
                    match RealmConfigDocument::from_bytes(&value) {
                        Ok(document) => {
                            self.state = GetRealmConfigState::Finish;
                            self.output = Some(Ok(document));
                            smallvec![]
                        }
                        Err(error) => self.fail(error.into()),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            GetRealmConfigState::Finish
            | GetRealmConfigState::Error
            | GetRealmConfigState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetRealmConfigState::Finish | GetRealmConfigState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GetRealmConfigError::NotFinished))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        if self.external_txn {
            return smallvec![];
        }
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_finalize_errors() {
        // A deadline can finalize an operation that never reached a terminal
        // state; that must report an error instead of panicking.
        let mut operation = GetRealmConfigOperation::new(RealmId::from_bytes([7; 32]));
        operation.start();

        assert!(!operation.is_complete());
        assert_eq!(operation.finalize(), Err(GetRealmConfigError::NotFinished));
    }
}
