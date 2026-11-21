//! Removes one expired onboarding secret unless an enrollment is finishing it, judged inside the
//! write transaction that deletes it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::{OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::values;
use ulid::Ulid;

use crate::onboarding::create_secret::{secret_expiry_key, secret_record_key};
use crate::onboarding::list_secrets::ListSecretsError;
use crate::onboarding::secret_reads;
use crate::onboarding::secret_state::{resolve_secret_state, secret_state_key};

/// What a prune did to one secret.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PruneOutcome {
    Removed,
    /// Still valid, or an enrollment is finishing it.
    Kept,
    /// Another pruner or a revocation removed it first.
    Gone,
}

/// Reads the record and state in its write transaction and deletes all three rows only while the
/// secret is expired at `now` and not finalizing; a vanished secret is no error.
#[derive(Debug, PartialEq)]
pub struct PruneSecretOperation {
    enrollment_id: Ulid,
    now: u64,
    deleting: bool,
}

impl PruneSecretOperation {
    pub fn new(enrollment_id: Ulid, now: u64) -> Self {
        Self {
            enrollment_id,
            now,
            deleting: false,
        }
    }

    fn deletes(&self, record: &OnboardingSecretRecord) -> IoCall {
        let id = self.enrollment_id;
        IoCall::Delete(vec![
            (ONBOARDING_KEYSPACE.to_string(), secret_record_key(id)),
            (ONBOARDING_KEYSPACE.to_string(), secret_state_key(id)),
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_expiry_key(record.expires_at, id),
            ),
        ])
    }
}

impl Operation for PruneSecretOperation {
    type Output = PruneOutcome;
    type Error = ListSecretsError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<PruneOutcome>, ListSecretsError> {
        Ok(Next::Call(secret_reads(self.enrollment_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PruneOutcome>, ListSecretsError> {
        if self.deleting {
            result.done()?;
            return Ok(Next::Done(PruneOutcome::Removed));
        }
        let [record, state] = values::<2>(result)?;
        let Some(record) = record else {
            return Ok(Next::Done(PruneOutcome::Gone));
        };
        let record: OnboardingSecretRecord =
            postcard::from_bytes(&record).map_err(ConversionError::from)?;
        let state = resolve_secret_state(&record, state.as_ref())?;
        let finishing = matches!(state, OnboardingSecretState::Finalizing { .. });
        if record.expires_at >= self.now || finishing {
            return Ok(Next::Done(PruneOutcome::Kept));
        }
        self.deleting = true;
        Ok(Next::Call(self.deletes(&record)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::onboarding::secret_state::secret_state_entry;
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose};
    use byteview::ByteView;

    fn record(expires_at: u64) -> OnboardingSecretRecord {
        OnboardingSecretRecord {
            enrollment_id: Ulid::from_bytes([1; 16]),
            secret_hash: "hash".into(),
            mode: OnboardingMode::Server,
            purpose: OnboardingPurpose::NodeEnrollment,
            expires_at,
            claimed_node_id: None,
        }
    }

    fn rows(record: &OnboardingSecretRecord, state: OnboardingSecretState) -> IoResult {
        let value = ByteView::from(postcard::to_allocvec(record).unwrap());
        let (_, _, state) = secret_state_entry(record.enrollment_id, state).unwrap();
        IoResult::Values(vec![Some(value), Some(state)])
    }

    #[test]
    fn keeps_finishing_rows() {
        // A secret an enrollment finalizes stays though it expired; a vanished one is no error.
        let finishing = OnboardingSecretState::Finalizing {
            node_id: "node-a".into(),
        };
        let mut prune = PruneSecretOperation::new(Ulid::from_bytes([1; 16]), 10);
        assert_eq!(prune.transaction(), TransactionMode::Write);
        prune.first().unwrap();
        let kept = prune.next(rows(&record(5), finishing));
        assert_eq!(kept, Ok(Next::Done(PruneOutcome::Kept)));
        let mut gone = PruneSecretOperation::new(Ulid::from_bytes([1; 16]), 10);
        gone.first().unwrap();
        let answer = gone.next(IoResult::Values(vec![None, None]));
        assert_eq!(answer, Ok(Next::Done(PruneOutcome::Gone)));
    }

    #[test]
    fn removes_expired_rows() {
        let mut prune = PruneSecretOperation::new(Ulid::from_bytes([1; 16]), 10);
        prune.first().unwrap();
        let available = rows(&record(5), OnboardingSecretState::Available);
        let Ok(Next::Call(IoCall::Delete(keys))) = prune.next(available) else {
            panic!("expected the deletes");
        };
        assert_eq!(keys.len(), 3);
        assert_eq!(
            prune.next(IoResult::Done),
            Ok(Next::Done(PruneOutcome::Removed))
        );
    }
}
