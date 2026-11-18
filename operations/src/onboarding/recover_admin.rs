//! Drops the unclaimed initial administrator secrets and writes one fresh recovery secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::{OnboardingPurpose, OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, done};
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use thiserror::Error;

use crate::onboarding::create_secret::{secret_pages, secret_record_key};
use crate::onboarding::secret_state::{resolve_secret_state, secret_state_entry, secret_state_key};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoverInitialInput {
    pub record: OnboardingSecretRecord,
}

/// What the recovery transaction waits for.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Stage {
    Records,
    States,
    Delete,
    Write,
}

/// Drops the unclaimed initial administrator secrets and writes the replacement in one write
/// transaction.
#[derive(Debug, PartialEq)]
pub struct RecoverInitialOperation {
    input: RecoverInitialInput,
    stage: Stage,
    pages: PrefixPages,
    records: Vec<OnboardingSecretRecord>,
}

#[derive(Debug, Error, PartialEq)]
pub enum RecoverInitialError {
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
        got: &'static str,
    },
}

impl From<IoError> for RecoverInitialError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "RecoverInitial".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

type RecoverTurn = Result<Next<OnboardingSecretRecord>, RecoverInitialError>;

impl RecoverInitialOperation {
    pub fn new(input: RecoverInitialInput) -> Self {
        Self {
            input,
            stage: Stage::Records,
            pages: secret_pages(),
            records: Vec::new(),
        }
    }

    fn replacement_writes(&self) -> Result<Vec<(KeySpace, Key, Value)>, RecoverInitialError> {
        let value = postcard::to_allocvec(&self.input.record).map_err(ConversionError::from)?;
        let state = secret_state_entry(
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

    fn write_replacement(&mut self) -> RecoverTurn {
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(self.replacement_writes()?)))
    }

    /// Collects every stored secret, then reads their states.
    fn handle_page(&mut self, result: IoResult) -> RecoverTurn {
        for (_, value) in self.pages.take(result)? {
            let record = postcard::from_bytes::<OnboardingSecretRecord>(&value)
                .map_err(ConversionError::from)?;
            self.records.push(record);
        }
        if !self.pages.done() {
            return Ok(Next::Call(self.pages.call()));
        }
        if self.records.is_empty() {
            return self.write_replacement();
        }
        let reads = (self.records.iter())
            .map(|record| {
                (
                    ONBOARDING_KEYSPACE.to_string(),
                    secret_state_key(record.enrollment_id),
                )
            })
            .collect();
        self.stage = Stage::States;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn handle_states(&mut self, values: Vec<Option<Value>>) -> RecoverTurn {
        if values.len() != self.records.len() {
            return Err(RecoverInitialError::UnexpectedEvent {
                state: "States".to_string(),
                expected: "one state per onboarding secret",
                got: "a different number of states",
            });
        }
        let mut deletes: Vec<(KeySpace, Key)> = Vec::new();
        for (record, state_value) in std::mem::take(&mut self.records).into_iter().zip(values) {
            let state = resolve_secret_state(&record, state_value.as_ref())?;
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
            return self.write_replacement();
        }
        self.stage = Stage::Delete;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

impl Operation for RecoverInitialOperation {
    type Output = OnboardingSecretRecord;
    type Error = RecoverInitialError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> RecoverTurn {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> RecoverTurn {
        match self.stage {
            Stage::Records => self.handle_page(result),
            Stage::States => {
                let IoResult::Values(values) = result else {
                    return Err(RecoverInitialError::NotFinished);
                };
                self.handle_states(values)
            }
            Stage::Delete => {
                done(result)?;
                self.write_replacement()
            }
            Stage::Write => {
                done(result)?;
                Ok(Next::Done(self.input.record.clone()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RecoverInitialInput, RecoverInitialOperation};
    use crate::driver::{DriverContext, drive};
    use crate::onboarding::consume_secret::{
        ConsumeSecretError, ConsumeSecretInput, ConsumeSecretOperation,
    };
    use crate::onboarding::create_secret::{CreateSecretInput, CreateSecretOperation};
    use crate::onboarding::list_secrets::ListSecretsOperation;
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn context_at(path: &str) -> DriverContext {
        DriverContext {
            storage_handle: storage::FjallStorage::open(path).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    fn admin_record(secret_hash: &str) -> OnboardingSecretRecord {
        OnboardingSecretRecord {
            enrollment_id: Ulid::generate(),
            secret_hash: secret_hash.to_string(),
            mode: OnboardingMode::Server,
            purpose: OnboardingPurpose::InitialAdministrator,
            expires_at: u64::MAX,
            claimed_node_id: None,
        }
    }

    async fn consume(
        context: &DriverContext,
        record: &OnboardingSecretRecord,
        claimant: &str,
    ) -> Result<Option<String>, ConsumeSecretError> {
        drive(
            ConsumeSecretOperation::new(ConsumeSecretInput {
                enrollment_id: record.enrollment_id,
                secret_hash: record.secret_hash.clone(),
                node_id: claimant.to_string(),
                now: 1,
            }),
            context,
        )
        .await
        .map(|consumed| consumed.record.claimed_node_id)
    }

    // A crash after the secret was consumed but before the administrator claim
    // leaves a used secret and no administrator.
    #[tokio::test]
    async fn recovers_after_reopen() {
        let tempdir = tempdir().unwrap();
        let path = tempdir.path().to_str().unwrap().to_string();
        let interrupted = admin_record("interrupted");
        {
            let context = context_at(&path);
            drive(
                CreateSecretOperation::new(CreateSecretInput {
                    record: interrupted.clone(),
                }),
                &context,
            )
            .await
            .unwrap();
            consume(&context, &interrupted, "claimant-a").await.unwrap();
            context.storage_handle.close().await;
        }

        let context = context_at(&path);
        assert_eq!(
            consume(&context, &interrupted, "claimant-b").await,
            Err(ConsumeSecretError::AlreadyClaimed)
        );

        let replacement = admin_record("replacement");
        drive(
            RecoverInitialOperation::new(RecoverInitialInput {
                record: replacement.clone(),
            }),
            &context,
        )
        .await
        .unwrap();

        assert_eq!(
            consume(&context, &interrupted, "claimant-b").await,
            Err(ConsumeSecretError::AlreadyClaimed)
        );
        assert_eq!(
            consume(&context, &replacement, "claimant-b").await,
            Ok(Some("claimant-b".to_string()))
        );
        assert_eq!(
            consume(&context, &interrupted, "claimant-a").await,
            Ok(Some("claimant-a".to_string()))
        );
    }

    #[tokio::test]
    async fn replaces_unclaimed_admin() {
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

        let old_id = Ulid::generate();
        let other_old_id = Ulid::generate();
        let claimed_id = Ulid::generate();
        let node_id = Ulid::generate();
        for record in [
            OnboardingSecretRecord {
                enrollment_id: old_id,
                secret_hash: "old".to_string(),
                mode: OnboardingMode::Server,
                purpose: OnboardingPurpose::InitialAdministrator,
                expires_at: u64::MAX,
                claimed_node_id: None,
            },
            OnboardingSecretRecord {
                enrollment_id: other_old_id,
                secret_hash: "other-old".to_string(),
                mode: OnboardingMode::Server,
                purpose: OnboardingPurpose::InitialAdministrator,
                expires_at: u64::MAX,
                claimed_node_id: None,
            },
            OnboardingSecretRecord {
                enrollment_id: claimed_id,
                secret_hash: "claimed".to_string(),
                mode: OnboardingMode::Server,
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
                CreateSecretOperation::new(CreateSecretInput { record }),
                &context,
            )
            .await
            .unwrap();
        }
        drive(
            ConsumeSecretOperation::new(ConsumeSecretInput {
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
            mode: OnboardingMode::Server,
            purpose: OnboardingPurpose::InitialAdministrator,
            expires_at: u64::MAX,
            claimed_node_id: None,
        };
        assert_eq!(
            drive(
                RecoverInitialOperation::new(RecoverInitialInput {
                    record: replacement.clone(),
                }),
                &context,
            )
            .await
            .unwrap(),
            replacement
        );

        let entries = drive(ListSecretsOperation::new(), &context).await.unwrap();
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
