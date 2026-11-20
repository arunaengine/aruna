//! One page of onboarding secrets from a cursor, soonest expiry first, with the expired secrets
//! of that page that no enrollment finishes pruned and left out.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::onboarding::OnboardingSecretState;
use aruna_core::operation::{Operation, boxed_retry};
use byteview::ByteView;

use crate::onboarding::expiry_page::ExpiryPageOperation;
use crate::onboarding::list_secrets::{ListSecretsError, OnboardingSecretEntry, SecretPage};
use crate::onboarding::prune_secret::{PruneOutcome, PruneSecretOperation};

/// Reads one expiry page after `after`, then prunes each expired entry of it that is not
/// finalizing, one conditional delete at a time that a lost conflict reruns; an expired
/// finalizing entry stays listed.
#[derive(Debug, PartialEq)]
pub struct LivePageOperation {
    now: u64,
    page: Option<ExpiryPageOperation>,
    pending: VecDeque<OnboardingSecretEntry>,
    pruning: Option<OnboardingSecretEntry>,
    kept: Vec<OnboardingSecretEntry>,
    next: Option<ByteView>,
}

impl LivePageOperation {
    pub fn new(now: u64, after: Option<ByteView>, limit: usize) -> Self {
        Self {
            now,
            page: Some(ExpiryPageOperation::new(after, limit)),
            pending: VecDeque::new(),
            pruning: None,
            kept: Vec::new(),
            next: None,
        }
    }

    /// Whether `entry` is expired at `now` and no enrollment finishes it.
    fn stale(&self, entry: &OnboardingSecretEntry) -> bool {
        entry.record.expires_at < self.now
            && !matches!(entry.state, OnboardingSecretState::Finalizing { .. })
    }

    fn advance(&mut self) -> Next<SecretPage> {
        while let Some(entry) = self.pending.pop_front() {
            if self.stale(&entry) {
                let (id, now) = (entry.record.enrollment_id, self.now);
                let build = move || PruneSecretOperation::new(id, now);
                let prune = boxed_retry(build, &id.to_bytes(), |result| {
                    Event::Child(ChildOutcome::new(result))
                });
                self.pruning = Some(entry);
                return Next::Call(IoCall::Child(prune));
            }
            self.kept.push(entry);
        }
        Next::Done(SecretPage {
            entries: std::mem::take(&mut self.kept),
            next: self.next.take(),
        })
    }
}

impl Operation for LivePageOperation {
    type Output = SecretPage;
    type Error = ListSecretsError;

    fn first(&mut self) -> Result<Next<SecretPage>, ListSecretsError> {
        let page = self.page.take().ok_or(ListSecretsError::NotFinished)?;
        Ok(Next::Call(IoCall::child(page)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SecretPage>, ListSecretsError> {
        if let Some(entry) = self.pruning.take() {
            if result.child::<PruneOutcome, ListSecretsError>()?? == PruneOutcome::Kept {
                self.kept.push(entry);
            }
            return Ok(self.advance());
        }
        let page = result.child::<SecretPage, ListSecretsError>()??;
        self.pending = page.entries.into();
        self.next = page.next;
        Ok(self.advance())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::Effect;
    use aruna_core::errors::StorageError;
    use aruna_core::events::StorageEvent;
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
    use ulid::Ulid;

    fn entry(seed: u8, expires_at: u64, state: OnboardingSecretState) -> OnboardingSecretEntry {
        OnboardingSecretEntry {
            record: OnboardingSecretRecord {
                enrollment_id: Ulid::from_bytes([seed; 16]),
                secret_hash: "hash".into(),
                mode: OnboardingMode::Server,
                purpose: OnboardingPurpose::NodeEnrollment,
                expires_at,
                claimed_node_id: None,
            },
            state,
        }
    }

    fn child<T: Send + std::fmt::Debug + PartialEq + 'static>(value: T) -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, ListSecretsError>(value)))
    }

    #[test]
    fn prunes_own_page() {
        // Only the page's stale entry is pruned; a finalizing one and a valid one stay listed.
        let mut live = LivePageOperation::new(10, None, 3);
        assert!(matches!(live.first(), Ok(Next::Call(IoCall::Child(_)))));
        let finishing = OnboardingSecretState::Finalizing {
            node_id: "node-a".into(),
        };
        let page = SecretPage {
            entries: vec![
                entry(1, 5, OnboardingSecretState::Available),
                entry(2, 6, finishing),
                entry(3, 20, OnboardingSecretState::Available),
            ],
            next: Some(ByteView::from(b"next".as_slice())),
        };
        assert!(matches!(
            live.next(child(page)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let Ok(Next::Done(page)) = live.next(child(PruneOutcome::Removed)) else {
            panic!("expected the page");
        };
        let ids: Vec<u8> = (page.entries.iter())
            .map(|entry| entry.record.enrollment_id.to_bytes()[0])
            .collect();
        assert_eq!(ids, [2, 3]);
        assert_eq!(page.next, Some(ByteView::from(b"next".as_slice())));
    }

    #[test]
    fn prune_reruns_conflict() {
        // A pruner that loses to a concurrent one rereads its rows instead of failing the page.
        let mut live = LivePageOperation::new(10, None, 1);
        live.first().unwrap();
        let page = SecretPage {
            entries: vec![entry(1, 5, OnboardingSecretState::Available)],
            next: None,
        };
        let Ok(Next::Call(IoCall::Child(mut prune))) = live.next(child(page)) else {
            panic!("expected the prune");
        };
        assert_eq!(prune.start().len(), 1);
        let conflict = StorageEvent::Error {
            error: StorageError::TransactionConflict,
        };
        let effects = prune.step(Event::Storage(conflict));
        assert!(matches!(effects.as_slice(), [Effect::Delay(_)]));
        assert!(!prune.is_complete());
    }
}
