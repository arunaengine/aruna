//! Removes an onboarding secret record and its state entry in one storage transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::onboarding::create_secret::secret_record_key;
use crate::onboarding::secret_state::secret_state_key;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSecretInput {
    pub enrollment_id: Ulid,
}

#[derive(Debug, PartialEq)]
pub struct DeleteSecretOperation {
    input: DeleteSecretInput,
    state: DeleteSecretState,
    output: Option<Result<OnboardingSecretRecord, DeleteSecretError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum DeleteSecretState {
    Init,
    StartTransaction,
    ReadRecord {
        txn_id: TxnId,
    },
    DeleteRecord {
        txn_id: TxnId,
        record: OnboardingSecretRecord,
    },
    CommitTransaction {
        record: OnboardingSecretRecord,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteSecretError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("onboarding secret not found")]
    NotFound,
    #[error("deleting onboarding secret did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl DeleteSecretOperation {
    pub fn new(input: DeleteSecretInput) -> Self {
        Self {
            input,
            state: DeleteSecretState::Init,
            output: None,
        }
    }
}

impl Operation for DeleteSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = DeleteSecretError;

    fn start(&mut self) -> Effects {
        self.state = DeleteSecretState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, DeleteSecretError::StorageError(error));
            }
            other => other,
        };

        match self.state.clone() {
            DeleteSecretState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return fail(
                        self,
                        DeleteSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction started",
                            got,
                        },
                    );
                };

                self.state = DeleteSecretState::ReadRecord { txn_id };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ONBOARDING_KEYSPACE.to_string(),
                    key: secret_record_key(self.input.enrollment_id),
                    txn_id: Some(txn_id),
                })]
            }
            DeleteSecretState::ReadRecord { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return fail(
                        self,
                        DeleteSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "read result",
                            got,
                        },
                    );
                };

                let Some(value) = value else {
                    return fail(self, DeleteSecretError::NotFound);
                };
                let record = match postcard::from_bytes::<OnboardingSecretRecord>(&value) {
                    Ok(record) => record,
                    Err(error) => {
                        return fail(self, DeleteSecretError::ConversionError(error.into()));
                    }
                };

                self.state = DeleteSecretState::DeleteRecord {
                    txn_id,
                    record: record.clone(),
                };
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: vec![
                        (
                            ONBOARDING_KEYSPACE.to_string(),
                            secret_record_key(self.input.enrollment_id),
                        ),
                        (
                            ONBOARDING_KEYSPACE.to_string(),
                            secret_state_key(self.input.enrollment_id),
                        ),
                    ],
                    txn_id: Some(txn_id),
                })]
            }
            DeleteSecretState::DeleteRecord { txn_id, record } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
                    return fail(
                        self,
                        DeleteSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "batch delete result",
                            got,
                        },
                    );
                };

                self.state = DeleteSecretState::CommitTransaction {
                    record: record.clone(),
                };
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            DeleteSecretState::CommitTransaction { record } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return fail(
                        self,
                        DeleteSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction committed",
                            got,
                        },
                    );
                };

                self.state = DeleteSecretState::Finish;
                self.output = Some(Ok(record));
                smallvec![]
            }
            DeleteSecretState::Init | DeleteSecretState::Finish | DeleteSecretState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteSecretState::Finish | DeleteSecretState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(DeleteSecretError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            DeleteSecretState::ReadRecord { txn_id }
            | DeleteSecretState::DeleteRecord { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn fail(operation: &mut DeleteSecretOperation, error: DeleteSecretError) -> Effects {
    let cleanup = operation.abort();
    operation.state = DeleteSecretState::Error;
    operation.output = Some(Err(error));
    cleanup
}
