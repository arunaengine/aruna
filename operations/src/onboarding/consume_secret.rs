//! Runs the operation that claims an onboarding secret for one node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::{OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::types::Value;
use byteview::ByteView;
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

/// Claims the secret for one node in one write transaction; a repeat by the same node succeeds
/// without a write.
#[derive(Debug, PartialEq)]
pub struct ConsumeSecretOperation {
    input: ConsumeSecretInput,
    claimed: Option<OnboardingSecretRecord>,
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
        got: &'static str,
    },
}

impl From<IoError> for ConsumeSecretError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ConsumeSecret".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

type ConsumeTurn = Result<Next<ConsumeSecretOutput>, ConsumeSecretError>;

impl ConsumeSecretOperation {
    pub fn new(input: ConsumeSecretInput) -> Self {
        Self {
            input,
            claimed: None,
        }
    }

    /// Whether the stored state still lets this node finish its claim after the secret expired.
    fn held_by_node(&self, state: &OnboardingSecretState) -> bool {
        let node = &self.input.node_id;
        match state {
            OnboardingSecretState::Reserved {
                node_id,
                expires_at,
            } => node_id == node && *expires_at >= self.input.now,
            OnboardingSecretState::Finalizing { node_id }
            | OnboardingSecretState::Consumed { node_id } => node_id == node,
            OnboardingSecretState::Available => false,
        }
    }

    fn claim(&mut self, record_value: Option<Value>, state_value: Option<Value>) -> ConsumeTurn {
        let value = record_value.ok_or(ConsumeSecretError::NotFound)?;
        let mut record: OnboardingSecretRecord =
            postcard::from_bytes(&value).map_err(ConversionError::from)?;
        let expired = record.expires_at < self.input.now;
        if expired {
            let state = resolve_secret_state(&record, state_value.as_ref())?;
            if !self.held_by_node(&state) {
                return Err(ConsumeSecretError::Expired);
            }
        }
        if record.secret_hash != self.input.secret_hash {
            return Err(ConsumeSecretError::InvalidSecret);
        }
        let node = &self.input.node_id;
        match resolve_secret_state(&record, state_value.as_ref())? {
            OnboardingSecretState::Available => {}
            OnboardingSecretState::Reserved { node_id, .. } if node_id == *node => {}
            OnboardingSecretState::Finalizing { node_id } if node_id == *node => {}
            OnboardingSecretState::Consumed { node_id } if node_id == *node => {
                record.claimed_node_id = Some(node.clone());
                return Ok(Next::Done(ConsumeSecretOutput {
                    record,
                    consumed_now: false,
                }));
            }
            OnboardingSecretState::Reserved { .. }
            | OnboardingSecretState::Finalizing { .. }
            | OnboardingSecretState::Consumed { .. } => {
                return Err(ConsumeSecretError::AlreadyClaimed);
            }
        }

        record.claimed_node_id = Some(node.clone());
        let value = postcard::to_allocvec(&record).map_err(ConversionError::from)?;
        let state_entry = secret_state_entry(
            self.input.enrollment_id,
            OnboardingSecretState::Consumed {
                node_id: node.clone(),
            },
        )?;
        self.claimed = Some(record);
        Ok(Next::Call(IoCall::Write(vec![
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_record_key(self.input.enrollment_id),
                ByteView::from(value),
            ),
            state_entry,
        ])))
    }
}

impl Operation for ConsumeSecretOperation {
    type Output = ConsumeSecretOutput;
    type Error = ConsumeSecretError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ConsumeTurn {
        Ok(Next::Call(IoCall::Read(vec![
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_record_key(self.input.enrollment_id),
            ),
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_state_key(self.input.enrollment_id),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> ConsumeTurn {
        let Some(record) = self.claimed.take() else {
            let [record, state] = values::<2>(result)?;
            return self.claim(record, state);
        };
        done(result)?;
        Ok(Next::Done(ConsumeSecretOutput {
            record,
            consumed_now: true,
        }))
    }
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
