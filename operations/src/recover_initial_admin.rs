use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::{OnboardingPurpose, OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::create_onboarding_secret::secret_record_key;
use crate::onboarding_secret_state::{
    resolve_secret_state, secret_state_key, secret_state_write_entry,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoverInitialAdminInput {
    pub record: OnboardingSecretRecord,
}

#[derive(Debug, PartialEq)]
pub struct RecoverInitialAdminOperation {
    input: RecoverInitialAdminInput,
    state: RecoverInitialAdminState,
    output: Option<Result<OnboardingSecretRecord, RecoverInitialAdminError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum RecoverInitialAdminState {
    Init,
    StartTransaction,
    ReadRecords {
        txn_id: TxnId,
    },
    ReadStates {
        txn_id: TxnId,
        records: Vec<OnboardingSecretRecord>,
    },
    DeleteRecords {
        txn_id: TxnId,
    },
    WriteReplacement {
        txn_id: TxnId,
    },
    CommitTransaction {
        txn_id: TxnId,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RecoverInitialAdminError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("recovering initial administrator did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl RecoverInitialAdminOperation {
    pub fn new(input: RecoverInitialAdminInput) -> Self {
        Self {
            input,
            state: RecoverInitialAdminState::Init,
            output: None,
        }
    }

    fn replacement_writes(&self) -> Result<Vec<(KeySpace, Key, Value)>, RecoverInitialAdminError> {
        let value = postcard::to_allocvec(&self.input.record).map_err(ConversionError::from)?;
        let state = secret_state_write_entry(
            self.input.record.enrollment_id,
            OnboardingSecretState::Available,
        )?;
        Ok(vec![
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_record_key(self.input.record.enrollment_id),
                ByteView::from(value),
            ),
            state,
        ])
    }

    fn write_replacement(&mut self, txn_id: TxnId) -> Effects {
        let writes = match self.replacement_writes() {
            Ok(writes) => writes,
            Err(error) => return self.fail(error),
        };
        self.state = RecoverInitialAdminState::WriteReplacement { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        self.fail(RecoverInitialAdminError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        })
    }

    fn fail(&mut self, error: RecoverInitialAdminError) -> Effects {
        let cleanup = self.abort();
        self.state = RecoverInitialAdminState::Error;
        self.output = Some(Err(error));
        cleanup
    }
}

impl Operation for RecoverInitialAdminOperation {
    type Output = OnboardingSecretRecord;
    type Error = RecoverInitialAdminError;

    fn start(&mut self) -> Effects {
        self.state = RecoverInitialAdminState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }

        match self.state.clone() {
            RecoverInitialAdminState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.unexpected("transaction started", event);
                };
                self.state = RecoverInitialAdminState::ReadRecords { txn_id };
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: ONBOARDING_KEYSPACE.to_string(),
                    prefix: Some(ByteView::from(b"secret:".as_slice())),
                    start: None,
                    limit: usize::MAX,
                    txn_id: Some(txn_id),
                })]
            }
            RecoverInitialAdminState::ReadRecords { txn_id } => {
                let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
                    return self.unexpected("onboarding secret iteration result", event);
                };
                let records = values
                    .into_iter()
                    .map(|(_, value)| postcard::from_bytes::<OnboardingSecretRecord>(&value))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(ConversionError::from);
                let records = match records {
                    Ok(records) => records,
                    Err(error) => return self.fail(error.into()),
                };
                if records.is_empty() {
                    return self.write_replacement(txn_id);
                }

                let reads = records
                    .iter()
                    .map(|record| {
                        (
                            ONBOARDING_KEYSPACE.to_string(),
                            secret_state_key(record.enrollment_id),
                        )
                    })
                    .collect();
                self.state = RecoverInitialAdminState::ReadStates { txn_id, records };
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: Some(txn_id),
                })]
            }
            RecoverInitialAdminState::ReadStates { txn_id, records } => {
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return self.unexpected("onboarding secret states", event);
                };
                if values.len() != records.len() {
                    return self.fail(RecoverInitialAdminError::UnexpectedEvent {
                        state: format!("{:?}", self.state),
                        expected: "one state per onboarding secret",
                        got: format!("{values:?}"),
                    });
                }

                let mut deletes = Vec::new();
                for (record, (_, state_value)) in records.into_iter().zip(values) {
                    let state = match resolve_secret_state(&record, state_value.as_ref()) {
                        Ok(state) => state,
                        Err(error) => return self.fail(error.into()),
                    };
                    if record.purpose == OnboardingPurpose::InitialAdministrator
                        && state.claimed_node_id().is_none()
                    {
                        deletes.push((
                            ONBOARDING_KEYSPACE.to_string(),
                            secret_record_key(record.enrollment_id),
                        ));
                        deletes.push((
                            ONBOARDING_KEYSPACE.to_string(),
                            secret_state_key(record.enrollment_id),
                        ));
                    }
                }

                if deletes.is_empty() {
                    return self.write_replacement(txn_id);
                }
                self.state = RecoverInitialAdminState::DeleteRecords { txn_id };
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes,
                    txn_id: Some(txn_id),
                })]
            }
            RecoverInitialAdminState::DeleteRecords { txn_id } => {
                if !matches!(
                    event,
                    Event::Storage(StorageEvent::BatchDeleteResult { .. })
                ) {
                    return self.unexpected("onboarding secret deletion result", event);
                }
                self.write_replacement(txn_id)
            }
            RecoverInitialAdminState::WriteReplacement { txn_id } => {
                if !matches!(event, Event::Storage(StorageEvent::BatchWriteResult { .. })) {
                    return self.unexpected("replacement secret write result", event);
                }
                self.state = RecoverInitialAdminState::CommitTransaction { txn_id };
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            RecoverInitialAdminState::CommitTransaction { .. } => {
                if !matches!(
                    event,
                    Event::Storage(StorageEvent::TransactionCommitted { .. })
                ) {
                    return self.unexpected("transaction committed", event);
                }
                self.state = RecoverInitialAdminState::Finish;
                self.output = Some(Ok(self.input.record.clone()));
                smallvec![]
            }
            RecoverInitialAdminState::Init => self.unexpected("operation start", event),
            RecoverInitialAdminState::Finish | RecoverInitialAdminState::Error => {
                self.unexpected("no event after completion", event)
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RecoverInitialAdminState::Finish | RecoverInitialAdminState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(RecoverInitialAdminError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        let txn_id = match &self.state {
            RecoverInitialAdminState::ReadRecords { txn_id }
            | RecoverInitialAdminState::ReadStates { txn_id, .. }
            | RecoverInitialAdminState::DeleteRecords { txn_id }
            | RecoverInitialAdminState::WriteReplacement { txn_id }
            | RecoverInitialAdminState::CommitTransaction { txn_id } => Some(*txn_id),
            _ => None,
        };
        txn_id
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::{RecoverInitialAdminInput, RecoverInitialAdminOperation};
    use crate::consume_onboarding_secret::{
        ConsumeOnboardingSecretInput, ConsumeOnboardingSecretOperation,
    };
    use crate::create_onboarding_secret::{
        CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
    };
    use crate::driver::{DriverContext, drive};
    use crate::list_onboarding_secrets::ListOnboardingSecretsOperation;
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn replaces_unclaimed_admin() {
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

        let old_id = Ulid::generate();
        let other_old_id = Ulid::generate();
        let claimed_id = Ulid::generate();
        let node_id = Ulid::generate();
        for record in [
            OnboardingSecretRecord {
                enrollment_id: old_id,
                secret_hash: "old".to_string(),
                mode: OnboardingMode::Local,
                purpose: OnboardingPurpose::InitialAdministrator,
                expires_at: u64::MAX,
                claimed_node_id: None,
            },
            OnboardingSecretRecord {
                enrollment_id: other_old_id,
                secret_hash: "other-old".to_string(),
                mode: OnboardingMode::Local,
                purpose: OnboardingPurpose::InitialAdministrator,
                expires_at: u64::MAX,
                claimed_node_id: None,
            },
            OnboardingSecretRecord {
                enrollment_id: claimed_id,
                secret_hash: "claimed".to_string(),
                mode: OnboardingMode::Local,
                purpose: OnboardingPurpose::InitialAdministrator,
                expires_at: u64::MAX,
                claimed_node_id: None,
            },
            OnboardingSecretRecord {
                enrollment_id: node_id,
                secret_hash: "node".to_string(),
                mode: OnboardingMode::Server,
                purpose: OnboardingPurpose::NodeEnrollment,
                expires_at: u64::MAX,
                claimed_node_id: None,
            },
        ] {
            drive(
                CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput { record }),
                &context,
            )
            .await
            .unwrap();
        }
        drive(
            ConsumeOnboardingSecretOperation::new(ConsumeOnboardingSecretInput {
                enrollment_id: claimed_id,
                secret_hash: "claimed".to_string(),
                node_id: "node-a".to_string(),
                now: 1,
            }),
            &context,
        )
        .await
        .unwrap();

        let replacement = OnboardingSecretRecord {
            enrollment_id: Ulid::generate(),
            secret_hash: "replacement".to_string(),
            mode: OnboardingMode::Local,
            purpose: OnboardingPurpose::InitialAdministrator,
            expires_at: u64::MAX,
            claimed_node_id: None,
        };
        assert_eq!(
            drive(
                RecoverInitialAdminOperation::new(RecoverInitialAdminInput {
                    record: replacement.clone(),
                }),
                &context,
            )
            .await
            .unwrap(),
            replacement
        );

        let entries = drive(ListOnboardingSecretsOperation::new(), &context)
            .await
            .unwrap();
        assert!(entries.iter().all(|entry| {
            entry.record.enrollment_id != old_id && entry.record.enrollment_id != other_old_id
        }));
        assert!(entries.iter().any(|entry| {
            entry.record.enrollment_id == claimed_id
                && entry.record.claimed_node_id.as_deref() == Some("node-a")
        }));
        assert!(
            entries
                .iter()
                .any(|entry| entry.record.enrollment_id == node_id)
        );
        assert!(entries.iter().any(|entry| entry.record == replacement));
    }
}
