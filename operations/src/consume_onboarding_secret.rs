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

use crate::create_onboarding_secret::secret_record_key;
use crate::onboarding_secret_state::{
    resolve_secret_state, secret_state_key, secret_state_write_entry,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumeOnboardingSecretInput {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub node_id: String,
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
    ReadRecords {
        txn_id: TxnId,
    },
    WriteConsumed {
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
pub enum ConsumeOnboardingSecretError {
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

                self.state = ConsumeOnboardingSecretState::ReadRecords { txn_id };
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
            ConsumeOnboardingSecretState::ReadRecords { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return fail(
                        self,
                        ConsumeOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "batch read result",
                            got,
                        },
                    );
                };
                let [(_, record_value), (_, state_value)] = values.as_slice() else {
                    return fail(
                        self,
                        ConsumeOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "record and state batch read result",
                            got: format!("{values:?}"),
                        },
                    );
                };

                let Some(value) = record_value else {
                    return fail(self, ConsumeOnboardingSecretError::NotFound);
                };
                let mut record: OnboardingSecretRecord = match postcard::from_bytes(value) {
                    Ok(record) => record,
                    Err(error) => {
                        return fail(
                            self,
                            ConsumeOnboardingSecretError::ConversionError(error.into()),
                        );
                    }
                };

                let resolved_state = if record.expires_at < self.input.now {
                    let state = match resolve_secret_state(&record, state_value.as_ref()) {
                        Ok(state) => state,
                        Err(error) => {
                            return fail(
                                self,
                                ConsumeOnboardingSecretError::ConversionError(error),
                            );
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
                        return fail(self, ConsumeOnboardingSecretError::Expired);
                    }
                    Some(state)
                } else {
                    None
                };
                if record.secret_hash != self.input.secret_hash {
                    return fail(self, ConsumeOnboardingSecretError::InvalidSecret);
                }
                let state = match resolved_state {
                    Some(state) => state,
                    None => match resolve_secret_state(&record, state_value.as_ref()) {
                        Ok(state) => state,
                        Err(error) => {
                            return fail(
                                self,
                                ConsumeOnboardingSecretError::ConversionError(error),
                            );
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
                        self.state = ConsumeOnboardingSecretState::CommitTransaction {
                            record: record.clone(),
                        };
                        return smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                            txn_id,
                        })];
                    }
                    OnboardingSecretState::Reserved { .. }
                    | OnboardingSecretState::Finalizing { .. }
                    | OnboardingSecretState::Consumed { .. } => {
                        return fail(self, ConsumeOnboardingSecretError::AlreadyClaimed);
                    }
                }

                record.claimed_node_id = Some(self.input.node_id.clone());
                let value = match postcard::to_allocvec(&record) {
                    Ok(value) => value,
                    Err(error) => {
                        return fail(
                            self,
                            ConsumeOnboardingSecretError::ConversionError(error.into()),
                        );
                    }
                };
                self.state = ConsumeOnboardingSecretState::WriteConsumed {
                    txn_id,
                    record: record.clone(),
                };
                let state_entry = match secret_state_write_entry(
                    self.input.enrollment_id,
                    OnboardingSecretState::Consumed {
                        node_id: self.input.node_id.clone(),
                    },
                ) {
                    Ok(entry) => entry,
                    Err(error) => {
                        return fail(self, ConsumeOnboardingSecretError::ConversionError(error));
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
            ConsumeOnboardingSecretState::WriteConsumed { txn_id, record } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return fail(
                        self,
                        ConsumeOnboardingSecretError::UnexpectedEvent {
                            state: format!("{:?}", self.state),
                            expected: "batch write result",
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
        self.output
            .ok_or(ConsumeOnboardingSecretError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            ConsumeOnboardingSecretState::ReadRecords { txn_id }
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
        ConsumeOnboardingSecretError, ConsumeOnboardingSecretInput,
        ConsumeOnboardingSecretOperation,
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
    async fn claims_secret_idempotently_for_same_node() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let enrollment_id = Ulid::new();
        let record = OnboardingSecretRecord {
            enrollment_id,
            secret_hash: "abc".to_string(),
            mode: OnboardingMode::Server,
            expires_at: 100,
            claimed_node_id: None,
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
                node_id: "node-a".to_string(),
                now: 10,
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(consumed.claimed_node_id.as_deref(), Some("node-a"));

        let second = drive(
            ConsumeOnboardingSecretOperation::new(ConsumeOnboardingSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-a".to_string(),
                now: 10,
            }),
            &context,
        )
        .await;
        assert!(second.is_ok());

        let different_node = drive(
            ConsumeOnboardingSecretOperation::new(ConsumeOnboardingSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-b".to_string(),
                now: 10,
            }),
            &context,
        )
        .await;
        assert_eq!(
            different_node,
            Err(ConsumeOnboardingSecretError::AlreadyClaimed)
        );
    }
}
