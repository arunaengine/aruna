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

use crate::onboarding::create_secret::secret_record_key;
use crate::onboarding::secret_state::{resolve_secret_state, secret_state_entry, secret_state_key};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumeSecretInput {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub node_id: String,
    pub now: u64,
}

#[derive(Debug, PartialEq)]
pub struct ConsumeSecretOutput {
    pub record: OnboardingSecretRecord,
    pub consumed_now: bool,
}

#[derive(Debug, PartialEq)]
pub struct ConsumeSecretOperation {
    input: ConsumeSecretInput,
    state: ConsumeSecretState,
    output: Option<Result<ConsumeSecretOutput, ConsumeSecretError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum ConsumeSecretState {
    Init,
    StartTransaction,
    ReadRecords {
        txn_id: TxnId,
    },
    WriteConsumed {
        txn_id: TxnId,
        record: OnboardingSecretRecord,
    },
    CommitTransaction {
        record: OnboardingSecretRecord,
        consumed_now: bool,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ConsumeSecretError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("onboarding secret not found")]
    NotFound,
    #[error("onboarding secret expired")]
    Expired,
    #[error("onboarding secret already claimed")]
    AlreadyClaimed,
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

impl ConsumeSecretOperation {
    pub fn new(input: ConsumeSecretInput) -> Self {
        Self {
            input,
            state: ConsumeSecretState::Init,
            output: None,
        }
    }
}

impl Operation for ConsumeSecretOperation {
    type Output = ConsumeSecretOutput;
    type Error = ConsumeSecretError;

    fn start(&mut self) -> Effects {
        self.state = ConsumeSecretState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, ConsumeSecretError::StorageError(error));
            }
            other => other,
        };

        match self.state.clone() {
            ConsumeSecretState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return fail(
                        self,
                        ConsumeSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction started",
                            got,
                        },
                    );
                };

                self.state = ConsumeSecretState::ReadRecords { txn_id };
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
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
            ConsumeSecretState::ReadRecords { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return fail(
                        self,
                        ConsumeSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "batch read result",
                            got,
                        },
                    );
                };
                let [(_, record_value), (_, state_value)] = values.as_slice() else {
                    return fail(
                        self,
                        ConsumeSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "record and state batch read result",
                            got: format!("{values:?}"),
                        },
                    );
                };

                let Some(value) = record_value else {
                    return fail(self, ConsumeSecretError::NotFound);
                };
                let mut record: OnboardingSecretRecord = match postcard::from_bytes(value) {
                    Ok(record) => record,
                    Err(error) => {
                        return fail(self, ConsumeSecretError::ConversionError(error.into()));
                    }
                };

                let resolved_state = if record.expires_at < self.input.now {
                    let state = match resolve_secret_state(&record, state_value.as_ref()) {
                        Ok(state) => state,
                        Err(error) => {
                            return fail(self, ConsumeSecretError::ConversionError(error));
                        }
                    };
                    if !matches!(
                        state,
                        OnboardingSecretState::Reserved { ref node_id, expires_at }
                            if node_id == &self.input.node_id && expires_at >= self.input.now
                    ) && !matches!(
                        state,
                        OnboardingSecretState::Finalizing { ref node_id }
                            if node_id == &self.input.node_id
                    ) && !matches!(
                        state,
                        OnboardingSecretState::Consumed { ref node_id }
                            if node_id == &self.input.node_id
                    ) {
                        return fail(self, ConsumeSecretError::Expired);
                    }
                    Some(state)
                } else {
                    None
                };
                if record.secret_hash != self.input.secret_hash {
                    return fail(self, ConsumeSecretError::InvalidSecret);
                }
                let state = match resolved_state {
                    Some(state) => state,
                    None => match resolve_secret_state(&record, state_value.as_ref()) {
                        Ok(state) => state,
                        Err(error) => {
                            return fail(self, ConsumeSecretError::ConversionError(error));
                        }
                    },
                };
                match state {
                    OnboardingSecretState::Available => {}
                    OnboardingSecretState::Reserved { node_id, .. }
                        if node_id == self.input.node_id => {}
                    OnboardingSecretState::Finalizing { node_id }
                        if node_id == self.input.node_id => {}
                    OnboardingSecretState::Consumed { node_id }
                        if node_id == self.input.node_id =>
                    {
                        record.claimed_node_id = Some(self.input.node_id.clone());
                        self.state = ConsumeSecretState::CommitTransaction {
                            record: record.clone(),
                            consumed_now: false,
                        };
                        return smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                            txn_id,
                        })];
                    }
                    OnboardingSecretState::Reserved { .. }
                    | OnboardingSecretState::Finalizing { .. }
                    | OnboardingSecretState::Consumed { .. } => {
                        return fail(self, ConsumeSecretError::AlreadyClaimed);
                    }
                }

                record.claimed_node_id = Some(self.input.node_id.clone());
                let value = match postcard::to_allocvec(&record) {
                    Ok(value) => value,
                    Err(error) => {
                        return fail(self, ConsumeSecretError::ConversionError(error.into()));
                    }
                };
                self.state = ConsumeSecretState::WriteConsumed {
                    txn_id,
                    record: record.clone(),
                };
                let state_entry = match secret_state_entry(
                    self.input.enrollment_id,
                    OnboardingSecretState::Consumed {
                        node_id: self.input.node_id.clone(),
                    },
                ) {
                    Ok(entry) => entry,
                    Err(error) => {
                        return fail(self, ConsumeSecretError::ConversionError(error));
                    }
                };

                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes: vec![
                        (
                            ONBOARDING_KEYSPACE.to_string(),
                            secret_record_key(self.input.enrollment_id),
                            ByteView::from(value),
                        ),
                        state_entry,
                    ],
                    txn_id: Some(txn_id),
                })]
            }
            ConsumeSecretState::WriteConsumed { txn_id, record } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return fail(
                        self,
                        ConsumeSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "batch write result",
                            got,
                        },
                    );
                };

                self.state = ConsumeSecretState::CommitTransaction {
                    record: record.clone(),
                    consumed_now: true,
                };
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            ConsumeSecretState::CommitTransaction {
                record,
                consumed_now,
            } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return fail(
                        self,
                        ConsumeSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "transaction committed",
                            got,
                        },
                    );
                };

                self.state = ConsumeSecretState::Finish;
                self.output = Some(Ok(ConsumeSecretOutput {
                    record,
                    consumed_now,
                }));
                smallvec![]
            }
            ConsumeSecretState::Init | ConsumeSecretState::Finish | ConsumeSecretState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ConsumeSecretState::Finish | ConsumeSecretState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ConsumeSecretError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            ConsumeSecretState::ReadRecords { txn_id }
            | ConsumeSecretState::WriteConsumed { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn fail(operation: &mut ConsumeSecretOperation, error: ConsumeSecretError) -> Effects {
    let cleanup = operation.abort();
    operation.state = ConsumeSecretState::Error;
    operation.output = Some(Err(error));
    cleanup
}

#[cfg(test)]
mod tests {
    use super::{ConsumeSecretError, ConsumeSecretInput, ConsumeSecretOperation};
    use crate::driver::{DriverContext, drive};
    use crate::onboarding::create_secret::{CreateSecretInput, CreateSecretOperation};
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn claims_same_node() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let enrollment_id = Ulid::generate();
        let record = OnboardingSecretRecord {
            enrollment_id,
            secret_hash: "abc".to_string(),
            mode: OnboardingMode::Server,
            purpose: OnboardingPurpose::NodeEnrollment,
            expires_at: 100,
            claimed_node_id: None,
        };

        drive(
            CreateSecretOperation::new(CreateSecretInput {
                record: record.clone(),
            }),
            &context,
        )
        .await
        .unwrap();

        let consumed = drive(
            ConsumeSecretOperation::new(ConsumeSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-a".to_string(),
                now: 10,
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(consumed.record.claimed_node_id.as_deref(), Some("node-a"));
        assert!(consumed.consumed_now);

        let second = drive(
            ConsumeSecretOperation::new(ConsumeSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-a".to_string(),
                now: 10,
            }),
            &context,
        )
        .await;
        assert!(!second.unwrap().consumed_now);

        let different_node = drive(
            ConsumeSecretOperation::new(ConsumeSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-b".to_string(),
                now: 10,
            }),
            &context,
        )
        .await;
        assert_eq!(different_node, Err(ConsumeSecretError::AlreadyClaimed));
    }
}
