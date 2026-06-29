use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::{OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::onboarding_secret_state::secret_state_write_entry;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateOnboardingSecretInput {
    pub record: OnboardingSecretRecord,
}

#[derive(Debug, PartialEq)]
pub struct CreateOnboardingSecretOperation {
    input: CreateOnboardingSecretInput,
    state: CreateOnboardingSecretState,
    output: Option<Result<OnboardingSecretRecord, CreateOnboardingSecretError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum CreateOnboardingSecretState {
    Init,
    StartTransaction,
    WriteRecord { txn_id: TxnId },
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateOnboardingSecretError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("creating onboarding secret did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl CreateOnboardingSecretOperation {
    pub fn new(input: CreateOnboardingSecretInput) -> Self {
        Self {
            input,
            state: CreateOnboardingSecretState::Init,
            output: None,
        }
    }
}

impl Operation for CreateOnboardingSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = CreateOnboardingSecretError;

    fn start(&mut self) -> Effects {
        self.state = CreateOnboardingSecretState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, CreateOnboardingSecretError::StorageError(error));
            }
            other => other,
        };

        match self.state {
            CreateOnboardingSecretState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return fail(
                        self,
                        CreateOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction started",
                            got,
                        },
                    );
                };

                self.state = CreateOnboardingSecretState::WriteRecord { txn_id };
                let key = secret_record_key(self.input.record.enrollment_id);
                let value = match postcard::to_allocvec(&self.input.record) {
                    Ok(value) => value,
                    Err(error) => {
                        return fail(
                            self,
                            CreateOnboardingSecretError::ConversionError(error.into()),
                        );
                    }
                };
                let state_entry = match secret_state_write_entry(
                    self.input.record.enrollment_id,
                    OnboardingSecretState::Available,
                ) {
                    Ok(entry) => entry,
                    Err(error) => {
                        return fail(self, CreateOnboardingSecretError::ConversionError(error));
                    }
                };
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes: vec![
                        (ONBOARDING_KEYSPACE.to_string(), key, ByteView::from(value)),
                        state_entry
                    ],
                    txn_id: Some(txn_id),
                })]
            }
            CreateOnboardingSecretState::WriteRecord { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return fail(
                        self,
                        CreateOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "batch write result",
                            got,
                        },
                    );
                };
                self.state = CreateOnboardingSecretState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            CreateOnboardingSecretState::CommitTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return fail(
                        self,
                        CreateOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction committed",
                            got,
                        },
                    );
                };
                self.state = CreateOnboardingSecretState::Finish;
                self.output = Some(Ok(self.input.record.clone()));
                smallvec![]
            }
            CreateOnboardingSecretState::Init
            | CreateOnboardingSecretState::Finish
            | CreateOnboardingSecretState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateOnboardingSecretState::Finish | CreateOnboardingSecretState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(CreateOnboardingSecretError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            CreateOnboardingSecretState::WriteRecord { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn fail(
    operation: &mut CreateOnboardingSecretOperation,
    error: CreateOnboardingSecretError,
) -> Effects {
    let cleanup = operation.abort();
    operation.state = CreateOnboardingSecretState::Error;
    operation.output = Some(Err(error));
    cleanup
}

pub fn secret_record_key(enrollment_id: Ulid) -> ByteView {
    ByteView::from(format!("secret:{enrollment_id}").into_bytes())
}
