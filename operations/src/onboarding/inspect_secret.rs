//! Reads an onboarding secret and checks that it matches, is unclaimed and not expired.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::{OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
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
        got: &'static str,
    },
}

impl From<IoError> for InspectSecretError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ReadRecords".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl InspectSecretOperation {
    pub fn new(input: InspectSecretInput) -> Self {
        Self { input }
    }

    fn check(
        &self,
        record: OnboardingSecretRecord,
        state: OnboardingSecretState,
    ) -> Result<OnboardingSecretRecord, InspectSecretError> {
        let claimed_by = match &state {
            OnboardingSecretState::Finalizing { node_id } => Some(node_id),
            _ => None,
        };
        let own_claim = claimed_by == Some(&self.input.node_id);
        if claimed_by.is_some() && !own_claim {
            return Err(InspectSecretError::AlreadyClaimed);
        }
        if record.expires_at < self.input.now && !own_claim {
            return Err(InspectSecretError::Expired);
        }
        if record.secret_hash != self.input.secret_hash {
            return Err(InspectSecretError::InvalidSecret);
        }
        Ok(record)
    }
}

impl Operation for InspectSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = InspectSecretError;

    fn first(&mut self) -> Result<Next<OnboardingSecretRecord>, InspectSecretError> {
        let enrollment_id = self.input.enrollment_id;
        Ok(Next::Call(IoCall::Read(vec![
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_record_key(enrollment_id),
            ),
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_state_key(enrollment_id),
            ),
        ])))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<OnboardingSecretRecord>, InspectSecretError> {
        let [record, state] = values::<2>(result)?;
        let record = record.ok_or(InspectSecretError::NotFound)?;
        let record: OnboardingSecretRecord =
            postcard::from_bytes(&record).map_err(ConversionError::from)?;
        let state = resolve_secret_state(&record, state.as_ref())?;
        self.check(record, state).map(Next::Done)
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
