//! Accepts a request-bound delegation once: verified, issued by a realm infrastructure node and
//! carrying a nonce no earlier acceptance spent.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::delegation::{
    Delegation, DelegationError, MAX_VALID_SECS, SignedDelegation,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use super::nonce::{NonceError, SpendNonceOperation};
use crate::document_repository::GetConfigOperation;
use crate::realm::peer_trust::is_sync_eligible;

#[derive(Debug, Error, PartialEq)]
pub enum DelegateError {
    #[error(transparent)]
    Delegation(#[from] DelegationError),
    #[error("the delegation comes from a node that is not a realm infrastructure node")]
    Issuer,
    #[error("the delegation was already used")]
    Replayed,
    #[error("delegations need the realm transport and configuration")]
    Unavailable,
}

impl From<NonceError> for DelegateError {
    fn from(error: NonceError) -> Self {
        match error {
            NonceError::Replayed => DelegateError::Replayed,
            NonceError::Unavailable => DelegateError::Unavailable,
        }
    }
}

#[derive(Debug, PartialEq)]
enum AcceptStep {
    Init,
    Config(GetConfigOperation),
    Nonce(SpendNonceOperation),
    Finish,
}

/// Accepts `signed` for the node `node_id` of `realm_id` at `now` (Unix seconds). The caller
/// still checks the request digest and the user's permissions on its own path.
#[derive(Debug, PartialEq)]
pub struct AcceptDelegationOperation {
    signed: SignedDelegation,
    realm_id: RealmId,
    node_id: NodeId,
    now: u64,
    step: AcceptStep,
    output: Option<Result<Delegation, DelegateError>>,
}

impl AcceptDelegationOperation {
    pub fn new(signed: SignedDelegation, realm_id: RealmId, node_id: NodeId, now: u64) -> Self {
        Self {
            signed,
            realm_id,
            node_id,
            now,
            step: AcceptStep::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<Delegation, DelegateError>, effects: Effects) -> Effects {
        self.step = AcceptStep::Finish;
        self.output = Some(output);
        effects
    }

    fn spend(&mut self) -> Effects {
        let delegation = &self.signed.delegation;
        let expires = delegation.issued_at.saturating_add(2 * MAX_VALID_SECS);
        let mut nonce = SpendNonceOperation::new(delegation.nonce, expires, self.now);
        let effects = nonce.start();
        self.step = AcceptStep::Nonce(nonce);
        effects
    }
}

impl Operation for AcceptDelegationOperation {
    type Output = Delegation;
    type Error = DelegateError;

    fn start(&mut self) -> Effects {
        let verified = (self.signed.verify(self.realm_id, self.node_id, self.now)).map(|_| ());
        if let Err(error) = verified {
            return self.finish(Err(error.into()), smallvec![]);
        }
        let mut config = GetConfigOperation::new(self.realm_id);
        let effects = config.start();
        self.step = AcceptStep::Config(config);
        effects
    }

    fn step(&mut self, event: Event) -> Effects {
        match std::mem::replace(&mut self.step, AcceptStep::Finish) {
            AcceptStep::Config(mut config) => {
                let effects = config.step(event);
                if !config.is_complete() {
                    self.step = AcceptStep::Config(config);
                    return effects;
                }
                match config.finalize() {
                    Ok(config) if is_sync_eligible(&config, self.signed.delegation.issuer) => {
                        self.spend()
                    }
                    Ok(_) => self.finish(Err(DelegateError::Issuer), effects),
                    Err(_) => self.finish(Err(DelegateError::Unavailable), effects),
                }
            }
            AcceptStep::Nonce(mut nonce) => {
                let effects = nonce.step(event);
                if !nonce.is_complete() {
                    self.step = AcceptStep::Nonce(nonce);
                    return effects;
                }
                let output = nonce.finalize().map(|()| self.signed.delegation.clone());
                self.finish(output.map_err(Into::into), effects)
            }
            AcceptStep::Init | AcceptStep::Finish => {
                self.finish(Err(DelegateError::Unavailable), smallvec![])
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(DelegateError::Unavailable))
    }

    fn abort(&mut self) -> Effects {
        match &mut self.step {
            AcceptStep::Config(config) => config.abort(),
            AcceptStep::Nonce(nonce) => nonce.abort(),
            AcceptStep::Init | AcceptStep::Finish => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::StorageEvent;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
    use aruna_core::structs::storage::distribution::delegation::Payload;
    use ulid::Ulid;

    const NOW: u64 = 105;
    const TXN: Ulid = Ulid::from_bytes([8; 16]);

    fn secret(seed: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[seed; 32])
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([9; 32])
    }

    fn signed() -> SignedDelegation {
        Delegation {
            realm_id: realm(),
            issuer: secret(1).public(),
            target: secret(2).public(),
            set_id: Ulid::from_bytes([3; 16]),
            range_id: Ulid::from_bytes([4; 16]),
            user_id: UserId::local(Ulid::from_bytes([5; 16]), realm()),
            group_id: Ulid::from_bytes([6; 16]),
            credential_id: "AKEY".to_string(),
            restrictions: None,
            credential_expiry: 1_000,
            operation: "PutObject".to_string(),
            bucket: "spire".to_string(),
            key: "a/b".to_string(),
            copy_source: None,
            request_digest: [0; 32],
            payload: Payload::Framed { checksum: None },
            nonce: [7; 16],
            issued_at: 100,
            valid_secs: 10,
        }
        .sign(&secret(1))
    }

    fn operation() -> AcceptDelegationOperation {
        AcceptDelegationOperation::new(signed(), realm(), secret(2).public(), NOW)
    }

    /// The stored realm config, naming the issuer as a node of `kind`.
    fn config(kind: RealmNodeKind) -> Event {
        let actor = Actor {
            node_id: secret(1).public(),
            user_id: signed().delegation.user_id,
            realm_id: realm(),
        };
        let mut document = RealmConfigDocument::new(realm(), Vec::new(), 3);
        document.ensure_node(secret(1).public(), kind);
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(document.to_bytes(&actor).unwrap().into()),
        })
    }

    fn spend() -> SpendNonceOperation {
        SpendNonceOperation::new([7; 16], 100 + 2 * MAX_VALID_SECS, NOW)
    }

    /// The events of a spend that finds the nonce fresh and prunes nothing.
    fn spend_events() -> Vec<Event> {
        let key = || Vec::new().into();
        let events = [
            StorageEvent::TransactionStarted { txn_id: TXN },
            StorageEvent::ReadResult {
                key: key(),
                value: None,
            },
            StorageEvent::WriteResult { key: key() },
            StorageEvent::TransactionCommitted { txn_id: TXN },
            StorageEvent::IterResult {
                values: Vec::new(),
                next_start_after: None,
            },
        ];
        events.into_iter().map(Event::Storage).collect()
    }

    // A verified delegation from an infrastructure node spends its nonce and is accepted.
    #[test]
    fn accepts_once() {
        let mut expected = spend();
        let mut steps = vec![(config(RealmNodeKind::Server), expected.start().into_vec())];
        for (event, mirror) in spend_events().into_iter().zip(spend_events()) {
            steps.push((event, expected.step(mirror).into_vec()));
        }
        let mut accept = operation();
        let read = GetConfigOperation::new(realm()).start();
        check_trace(&mut accept, &read, steps);
        assert_eq!(accept.finalize(), Ok(signed().delegation));
    }

    // A delegation for another node, from a device or with a spent nonce is refused.
    #[test]
    fn refuses_bad_delegation() {
        let mut accept = AcceptDelegationOperation::new(signed(), realm(), secret(3).public(), NOW);
        assert!(accept.start().is_empty());
        assert_eq!(accept.finalize(), Err(DelegationError::WrongTarget.into()));

        let owner = signed().delegation.user_id;
        let mut accept = operation();
        accept.start();
        assert!(
            accept
                .step(config(RealmNodeKind::User { owner }))
                .is_empty()
        );
        assert_eq!(accept.finalize(), Err(DelegateError::Issuer));

        let in_transaction = || {
            let mut accept = operation();
            accept.start();
            accept.step(config(RealmNodeKind::Server));
            let started = StorageEvent::TransactionStarted { txn_id: TXN };
            accept.step(Event::Storage(started));
            accept
        };
        let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id: TXN });
        check_abort(&mut in_transaction(), std::slice::from_ref(&abort));
        let mut accept = in_transaction();
        let spent = StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(Vec::new().into()),
        };
        assert_eq!(accept.step(Event::Storage(spent)).into_vec(), vec![abort]);
        let aborted = StorageEvent::TransactionAborted { txn_id: TXN };
        assert!(accept.step(Event::Storage(aborted)).is_empty());
        assert_eq!(accept.finalize(), Err(DelegateError::Replayed));
    }

    // A failed config read leaves the check unavailable; other events are refused.
    #[test]
    fn rejects_unexpected() {
        let mut accept = operation();
        accept.start();
        accept.step(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::Timeout,
        }));
        assert_eq!(accept.finalize(), Err(DelegateError::Unavailable));
        let all = sample_events().len();
        assert_eq!(
            rejects_events(operation, sample_events, |_| false).len(),
            all
        );
        rejects_finalize(operation());
        assert!(operation().abort().is_empty());
    }
}
