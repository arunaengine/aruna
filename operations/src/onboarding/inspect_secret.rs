//! Reads an onboarding secret and checks that it matches, is unclaimed and not expired.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

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

use crate::onboarding::create_secret::secret_record_key;
use crate::onboarding::secret_state::{resolve_secret_state, secret_state_key};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectSecretInput {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub node_id: String,
    pub now: u64,
}

#[derive(Debug, PartialEq)]
pub struct InspectSecretOperation {
    input: InspectSecretInput,
    state: InspectSecretState,
    output: Option<Result<OnboardingSecretRecord, InspectSecretError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum InspectSecretState {
    Init,
    ReadRecords,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum InspectSecretError {
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

impl InspectSecretOperation {
    pub fn new(input: InspectSecretInput) -> Self {
        Self {
            input,
            state: InspectSecretState::Init,
            output: None,
        }
    }
}

impl Operation for InspectSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = InspectSecretError;

    fn start(&mut self) -> Effects {
        self.state = InspectSecretState::ReadRecords;
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
                self.state = InspectSecretState::Error;
                self.output = Some(Err(InspectSecretError::StorageError(error)));
                return smallvec![];
            }
            other => other,
        };

        match self.state {
            InspectSecretState::ReadRecords => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    self.state = InspectSecretState::Error;
                    self.output = Some(Err(InspectSecretError::UnexpectedEvent {
                        state: "ReadRecords".to_string(),
                        expected: "batch read result",
                        got,
                    }));
                    return smallvec![];
                };
                let [(_, record_value), (_, state_value)] = values.as_slice() else {
                    self.state = InspectSecretState::Error;
                    self.output = Some(Err(InspectSecretError::UnexpectedEvent {
                        state: "ReadRecords".to_string(),
                        expected: "record and state batch read result",
                        got: format!("{values:?}"),
                    }));
                    return smallvec![];
                };

                let Some(value) = record_value else {
                    self.state = InspectSecretState::Error;
                    self.output = Some(Err(InspectSecretError::NotFound));
                    return smallvec![];
                };

                let record = match postcard::from_bytes::<OnboardingSecretRecord>(value) {
                    Ok(record) => record,
                    Err(error) => {
                        self.state = InspectSecretState::Error;
                        self.output = Some(Err(InspectSecretError::ConversionError(error.into())));
                        return smallvec![];
                    }
                };
                let secret_state = match resolve_secret_state(&record, state_value.as_ref()) {
                    Ok(secret_state) => secret_state,
                    Err(error) => {
                        self.state = InspectSecretState::Error;
                        self.output = Some(Err(InspectSecretError::ConversionError(error)));
                        return smallvec![];
                    }
                };

                let validation = if matches!(
                    &secret_state,
                    OnboardingSecretState::Finalizing { node_id }
                        if node_id != &self.input.node_id
                ) {
                    Err(InspectSecretError::AlreadyClaimed)
                } else if record.expires_at < self.input.now
                    && !matches!(
                        &secret_state,
                        OnboardingSecretState::Finalizing { node_id }
                            if node_id == &self.input.node_id
                    )
                {
                    Err(InspectSecretError::Expired)
                } else if record.secret_hash != self.input.secret_hash {
                    Err(InspectSecretError::InvalidSecret)
                } else {
                    Ok(record)
                };

                match validation {
                    Ok(record) => {
                        self.state = InspectSecretState::Finish;
                        self.output = Some(Ok(record));
                    }
                    Err(error) => {
                        self.state = InspectSecretState::Error;
                        self.output = Some(Err(error));
                    }
                }
                smallvec![]
            }
            InspectSecretState::Init | InspectSecretState::Finish | InspectSecretState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            InspectSecretState::Finish | InspectSecretState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(InspectSecretError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{InspectSecretError, InspectSecretInput, InspectSecretOperation};
    use crate::driver::{DriverContext, drive};
    use crate::onboarding::create_secret::{CreateSecretInput, CreateSecretOperation};
    use crate::onboarding::reserve_secret::{ReserveSecretInput, ReserveSecretOperation};
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
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
        let enrollment_id = Ulid::generate();
        drive(
            CreateSecretOperation::new(CreateSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id,
                    secret_hash: "abc".to_string(),
                    mode: OnboardingMode::Server,
                    purpose: OnboardingPurpose::NodeEnrollment,
                    expires_at: 100,
                    claimed_node_id: None,
                },
            }),
            &context,
        )
        .await
        .unwrap();
        drive(
            ReserveSecretOperation::new(ReserveSecretInput {
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
    async fn expired_inspects_same() {
        let fixture = setup_finalizing_secret("node-a").await;

        let inspected = drive(
            InspectSecretOperation::new(InspectSecretInput {
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
    async fn expired_rejects_other() {
        let fixture = setup_finalizing_secret("node-a").await;

        let inspected = drive(
            InspectSecretOperation::new(InspectSecretInput {
                enrollment_id: fixture.enrollment_id,
                secret_hash: "abc".to_string(),
                node_id: "node-b".to_string(),
                now: 101,
            }),
            &fixture.context,
        )
        .await;

        assert_eq!(inspected, Err(InspectSecretError::AlreadyClaimed));
    }
}
