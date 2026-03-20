use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::create_onboarding_secret::secret_record_key;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumeOnboardingSecretInput {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub now: u64,
}

#[derive(Debug, PartialEq)]
pub struct ConsumeOnboardingSecretOperation {
    input: ConsumeOnboardingSecretInput,
    state: ConsumeOnboardingSecretState,
    output: Option<Result<OnboardingSecretRecord, ConsumeOnboardingSecretError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum ConsumeOnboardingSecretState {
    Init,
    StartTransaction,
    ReadRecord { txn_id: TxnId },
    WriteConsumed {
        txn_id: TxnId,
        record: OnboardingSecretRecord,
    },
    CommitTransaction { record: OnboardingSecretRecord },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ConsumeOnboardingSecretError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("onboarding secret not found")]
    NotFound,
    #[error("onboarding secret expired")]
    Expired,
    #[error("onboarding secret already consumed")]
    AlreadyConsumed,
    #[error("onboarding secret does not match")]
    InvalidSecret,
    #[error("consuming onboarding secret did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl ConsumeOnboardingSecretOperation {
    pub fn new(input: ConsumeOnboardingSecretInput) -> Self {
        Self {
            input,
            state: ConsumeOnboardingSecretState::Init,
            output: None,
        }
    }
}

impl Operation for ConsumeOnboardingSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = ConsumeOnboardingSecretError;

    fn start(&mut self) -> Effects {
        self.state = ConsumeOnboardingSecretState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, ConsumeOnboardingSecretError::StorageError(error));
            }
            other => other,
        };

        match self.state.clone() {
            ConsumeOnboardingSecretState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return fail(
                        self,
                        ConsumeOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction started",
                            got,
                        },
                    );
                };

                self.state = ConsumeOnboardingSecretState::ReadRecord { txn_id };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ONBOARDING_KEYSPACE.to_string(),
                    key: secret_record_key(self.input.enrollment_id),
                    txn_id: Some(txn_id),
                })]
            }
            ConsumeOnboardingSecretState::ReadRecord { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return fail(
                        self,
                        ConsumeOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "read result",
                            got,
                        },
                    );
                };

                let Some(value) = value else {
                    return fail(self, ConsumeOnboardingSecretError::NotFound);
                };
                let mut record: OnboardingSecretRecord = match postcard::from_bytes(&value) {
                    Ok(record) => record,
                    Err(error) => {
                        return fail(self, ConsumeOnboardingSecretError::ConversionError(error.into()));
                    }
                };

                if record.consumed {
                    return fail(self, ConsumeOnboardingSecretError::AlreadyConsumed);
                }
                if record.expires_at < self.input.now {
                    return fail(self, ConsumeOnboardingSecretError::Expired);
                }
                if record.secret_hash != self.input.secret_hash {
                    return fail(self, ConsumeOnboardingSecretError::InvalidSecret);
                }

                record.consumed = true;
                let value = match postcard::to_allocvec(&record) {
                    Ok(value) => value,
                    Err(error) => {
                        return fail(self, ConsumeOnboardingSecretError::ConversionError(error.into()));
                    }
                };
                self.state = ConsumeOnboardingSecretState::WriteConsumed {
                    txn_id,
                    record: record.clone(),
                };

                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: ONBOARDING_KEYSPACE.to_string(),
                    key: secret_record_key(self.input.enrollment_id),
                    value: ByteView::from(value),
                    txn_id: Some(txn_id),
                })]
            }
            ConsumeOnboardingSecretState::WriteConsumed { txn_id, record } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return fail(
                        self,
                        ConsumeOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "write result",
                            got,
                        },
                    );
                };

                self.state = ConsumeOnboardingSecretState::CommitTransaction {
                    record: record.clone(),
                };
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            ConsumeOnboardingSecretState::CommitTransaction { record } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return fail(
                        self,
                        ConsumeOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction committed",
                            got,
                        },
                    );
                };

                self.state = ConsumeOnboardingSecretState::Finish;
                self.output = Some(Ok(record));
                smallvec![]
            }
            ConsumeOnboardingSecretState::Init
            | ConsumeOnboardingSecretState::Finish
            | ConsumeOnboardingSecretState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ConsumeOnboardingSecretState::Finish | ConsumeOnboardingSecretState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ConsumeOnboardingSecretError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            ConsumeOnboardingSecretState::ReadRecord { txn_id }
            | ConsumeOnboardingSecretState::WriteConsumed { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn fail(
    operation: &mut ConsumeOnboardingSecretOperation,
    error: ConsumeOnboardingSecretError,
) -> Effects {
    let cleanup = operation.abort();
    operation.state = ConsumeOnboardingSecretState::Error;
    operation.output = Some(Err(error));
    cleanup
}

#[cfg(test)]
mod tests {
    use super::{
        ConsumeOnboardingSecretInput, ConsumeOnboardingSecretOperation,
        ConsumeOnboardingSecretError,
    };
    use crate::create_onboarding_secret::{
        CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_core::onboarding::{OnboardingMode, OnboardingSecretRecord};
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn consumes_secret_once() {
        let tempdir = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            task_handle: None,
        };

        let enrollment_id = Ulid::new();
        let record = OnboardingSecretRecord {
            enrollment_id,
            secret_hash: "abc".to_string(),
            mode: OnboardingMode::Server,
            expires_at: 100,
            consumed: false,
        };

        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: record.clone(),
            }),
            &context,
        )
        .await
        .unwrap();

        let consumed = drive(
            ConsumeOnboardingSecretOperation::new(ConsumeOnboardingSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                now: 10,
            }),
            &context,
        )
        .await
        .unwrap();
        assert!(consumed.consumed);

        let second = drive(
            ConsumeOnboardingSecretOperation::new(ConsumeOnboardingSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                now: 10,
            }),
            &context,
        )
        .await;
        assert_eq!(second, Err(ConsumeOnboardingSecretError::AlreadyConsumed));
    }
}
