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

use crate::create_onboarding_secret::secret_record_key;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteOnboardingSecretInput {
    pub enrollment_id: Ulid,
}

#[derive(Debug, PartialEq)]
pub struct DeleteOnboardingSecretOperation {
    input: DeleteOnboardingSecretInput,
    state: DeleteOnboardingSecretState,
    output: Option<Result<OnboardingSecretRecord, DeleteOnboardingSecretError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum DeleteOnboardingSecretState {
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
pub enum DeleteOnboardingSecretError {
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

impl DeleteOnboardingSecretOperation {
    pub fn new(input: DeleteOnboardingSecretInput) -> Self {
        Self {
            input,
            state: DeleteOnboardingSecretState::Init,
            output: None,
        }
    }
}

impl Operation for DeleteOnboardingSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = DeleteOnboardingSecretError;

    fn start(&mut self) -> Effects {
        self.state = DeleteOnboardingSecretState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, DeleteOnboardingSecretError::StorageError(error));
            }
            other => other,
        };

        match self.state.clone() {
            DeleteOnboardingSecretState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return fail(
                        self,
                        DeleteOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction started",
                            got,
                        },
                    );
                };

                self.state = DeleteOnboardingSecretState::ReadRecord { txn_id };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ONBOARDING_KEYSPACE.to_string(),
                    key: secret_record_key(self.input.enrollment_id),
                    txn_id: Some(txn_id),
                })]
            }
            DeleteOnboardingSecretState::ReadRecord { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return fail(
                        self,
                        DeleteOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "read result",
                            got,
                        },
                    );
                };

                let Some(value) = value else {
                    return fail(self, DeleteOnboardingSecretError::NotFound);
                };
                let record = match postcard::from_bytes::<OnboardingSecretRecord>(&value) {
                    Ok(record) => record,
                    Err(error) => {
                        return fail(
                            self,
                            DeleteOnboardingSecretError::ConversionError(error.into()),
                        );
                    }
                };

                self.state = DeleteOnboardingSecretState::DeleteRecord {
                    txn_id,
                    record: record.clone(),
                };
                smallvec![Effect::Storage(StorageEffect::Delete {
                    key_space: ONBOARDING_KEYSPACE.to_string(),
                    key: secret_record_key(self.input.enrollment_id),
                    txn_id: Some(txn_id),
                })]
            }
            DeleteOnboardingSecretState::DeleteRecord { txn_id, record } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
                    return fail(
                        self,
                        DeleteOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "delete result",
                            got,
                        },
                    );
                };

                self.state = DeleteOnboardingSecretState::CommitTransaction {
                    record: record.clone(),
                };
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            DeleteOnboardingSecretState::CommitTransaction { record } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return fail(
                        self,
                        DeleteOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction committed",
                            got,
                        },
                    );
                };

                self.state = DeleteOnboardingSecretState::Finish;
                self.output = Some(Ok(record));
                smallvec![]
            }
            DeleteOnboardingSecretState::Init
            | DeleteOnboardingSecretState::Finish
            | DeleteOnboardingSecretState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteOnboardingSecretState::Finish | DeleteOnboardingSecretState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(DeleteOnboardingSecretError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            DeleteOnboardingSecretState::ReadRecord { txn_id }
            | DeleteOnboardingSecretState::DeleteRecord { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn fail(
    operation: &mut DeleteOnboardingSecretOperation,
    error: DeleteOnboardingSecretError,
) -> Effects {
    let cleanup = operation.abort();
    operation.state = DeleteOnboardingSecretState::Error;
    operation.output = Some(Err(error));
    cleanup
}
