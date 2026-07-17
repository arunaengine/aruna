use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::{OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::create_onboarding_secret::secret_record_key;
use crate::onboarding_secret_state::{resolve_secret_state, secret_state_key};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectOnboardingSecretInput {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub node_id: String,
    pub now: u64,
}

#[derive(Debug, PartialEq)]
pub struct InspectOnboardingSecretOperation {
    input: InspectOnboardingSecretInput,
    state: InspectOnboardingSecretState,
    output: Option<Result<OnboardingSecretRecord, InspectOnboardingSecretError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum InspectOnboardingSecretState {
    Init,
    ReadRecords,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum InspectOnboardingSecretError {
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
    #[error("inspecting onboarding secret did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl InspectOnboardingSecretOperation {
    pub fn new(input: InspectOnboardingSecretInput) -> Self {
        Self {
            input,
            state: InspectOnboardingSecretState::Init,
            output: None,
        }
    }
}

impl Operation for InspectOnboardingSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = InspectOnboardingSecretError;

    fn start(&mut self) -> Effects {
        self.state = InspectOnboardingSecretState::ReadRecords;
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
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                self.state = InspectOnboardingSecretState::Error;
                self.output = Some(Err(InspectOnboardingSecretError::StorageError(error)));
                return smallvec![];
            }
            other => other,
        };

        match self.state {
            InspectOnboardingSecretState::ReadRecords => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    self.state = InspectOnboardingSecretState::Error;
                    self.output = Some(Err(InspectOnboardingSecretError::UnexpectedEvent {
                        state: "ReadRecords".to_string(),
                        expected: "batch read result",
                        got,
                    }));
                    return smallvec![];
                };
                let [(_, record_value), (_, state_value)] = values.as_slice() else {
                    self.state = InspectOnboardingSecretState::Error;
                    self.output = Some(Err(InspectOnboardingSecretError::UnexpectedEvent {
                        state: "ReadRecords".to_string(),
                        expected: "record and state batch read result",
                        got: format!("{values:?}"),
                    }));
                    return smallvec![];
                };

                let Some(value) = record_value else {
                    self.state = InspectOnboardingSecretState::Error;
                    self.output = Some(Err(InspectOnboardingSecretError::NotFound));
                    return smallvec![];
                };

                let record = match postcard::from_bytes::<OnboardingSecretRecord>(value) {
                    Ok(record) => record,
                    Err(error) => {
                        self.state = InspectOnboardingSecretState::Error;
                        self.output = Some(Err(InspectOnboardingSecretError::ConversionError(
                            error.into(),
                        )));
                        return smallvec![];
                    }
                };
                let secret_state = match resolve_secret_state(&record, state_value.as_ref()) {
                    Ok(secret_state) => secret_state,
                    Err(error) => {
                        self.state = InspectOnboardingSecretState::Error;
                        self.output =
                            Some(Err(InspectOnboardingSecretError::ConversionError(error)));
                        return smallvec![];
                    }
                };

                let validation = if matches!(
                    &secret_state,
                    OnboardingSecretState::Finalizing { node_id }
                        if node_id != &self.input.node_id
                ) {
                    Err(InspectOnboardingSecretError::AlreadyClaimed)
                } else if record.expires_at < self.input.now
                    && !matches!(
                        &secret_state,
                        OnboardingSecretState::Finalizing { node_id }
                            if node_id == &self.input.node_id
                    )
                {
                    Err(InspectOnboardingSecretError::Expired)
                } else if record.secret_hash != self.input.secret_hash {
                    Err(InspectOnboardingSecretError::InvalidSecret)
                } else {
                    Ok(record)
                };

                match validation {
                    Ok(record) => {
                        self.state = InspectOnboardingSecretState::Finish;
                        self.output = Some(Ok(record));
                    }
                    Err(error) => {
                        self.state = InspectOnboardingSecretState::Error;
                        self.output = Some(Err(error));
                    }
                }
                smallvec![]
            }
            InspectOnboardingSecretState::Init
            | InspectOnboardingSecretState::Finish
            | InspectOnboardingSecretState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            InspectOnboardingSecretState::Finish | InspectOnboardingSecretState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(InspectOnboardingSecretError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{
        InspectOnboardingSecretError, InspectOnboardingSecretInput,
        InspectOnboardingSecretOperation,
    };
    use crate::create_onboarding_secret::{
        CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
    };
    use crate::driver::{DriverContext, drive};
    use crate::reserve_onboarding_secret::{
        ReserveOnboardingSecretInput, ReserveOnboardingSecretOperation,
    };
    use aruna_core::onboarding::{OnboardingMode, OnboardingSecretRecord};
    use aruna_storage::storage;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    struct InspectFixture {
        _tempdir: TempDir,
        context: DriverContext,
        enrollment_id: Ulid,
    }

    async fn setup_finalizing_secret(node_id: &str) -> InspectFixture {
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
        let enrollment_id = Ulid::r#gen();
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id,
                    secret_hash: "abc".to_string(),
                    mode: OnboardingMode::Server,
                    expires_at: 100,
                    claimed_node_id: None,
                },
            }),
            &context,
        )
        .await
        .unwrap();
        drive(
            ReserveOnboardingSecretOperation::new(ReserveOnboardingSecretInput {
                enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: node_id.to_string(),
                now: 10,
                reservation_expires_at: 20,
                finalizing: true,
            }),
            &context,
        )
        .await
        .unwrap();

        InspectFixture {
            _tempdir: tempdir,
            context,
            enrollment_id,
        }
    }

    #[tokio::test]
    async fn expired_finalizing_secret_inspects_for_same_node() {
        let fixture = setup_finalizing_secret("node-a").await;

        let inspected = drive(
            InspectOnboardingSecretOperation::new(InspectOnboardingSecretInput {
                enrollment_id: fixture.enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-a".to_string(),
                now: 101,
            }),
            &fixture.context,
        )
        .await
        .unwrap();

        assert_eq!(inspected.claimed_node_id.as_deref(), Some("node-a"));
    }

    #[tokio::test]
    async fn expired_finalizing_secret_rejects_different_node() {
        let fixture = setup_finalizing_secret("node-a").await;

        let inspected = drive(
            InspectOnboardingSecretOperation::new(InspectOnboardingSecretInput {
                enrollment_id: fixture.enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-b".to_string(),
                now: 101,
            }),
            &fixture.context,
        )
        .await;

        assert_eq!(inspected, Err(InspectOnboardingSecretError::AlreadyClaimed));
    }
}
