//! Reading one immutable policy document by ref: local row first, then a
//! bounded fetch from the holders the policy id resolves to. No catalog is
//! consulted, and a document that does not hash to the requested digest is
//! refused rather than returned.

use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{
    Effect, HolderList, MAX_POLICY_FETCH_HOLDERS, NetEffect, PolicyFetchEffect, StorageEffect,
};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, PolicyFetchEvent, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    PlacementPolicyDocument, PlacementPolicyError, PlacementPolicyRef, RealmConfigDocument,
    RealmId, VerifiedPolicy, placement_policy_target,
};
use aruna_core::types::{Effects, Value};
use smallvec::smallvec;
use std::time::Duration;
use thiserror::Error;

use crate::placement::{PlacementResolveError, read_holder_sets};

/// Wall-clock budget one policy fetch may spend across its holders.
pub const POLICY_FETCH_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq)]
pub struct ReadPolicyConfig {
    pub realm_id: RealmId,
    /// Id and digest: an id-only read could accept changed bytes under a known
    /// id, which policy immutability forbids.
    pub policy_ref: PlacementPolicyRef,
    pub local_node_id: aruna_core::NodeId,
}

/// Where a verified policy came from, so a caller can tell a holder-local read
/// from a resolved fetch or a durable cache hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicySource {
    Cached,
    Local,
    Fetched,
}

#[derive(Debug, PartialEq)]
pub struct ReadPolicyOperation {
    config: ReadPolicyConfig,
    state: ReadPolicyState,
    output: Option<Result<(VerifiedPolicy, PolicySource), ReadPolicyError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ReadPolicyState {
    Init,
    ReadLocal,
    ReadConfig,
    /// Holders this fetch was sent to; a holder that answers with another
    /// definition is dropped and the remainder is asked again.
    Fetch {
        asked: Vec<aruna_core::NodeId>,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadPolicyError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error("realm config document missing")]
    RealmConfigMissing,
    #[error("no placement strategy governs policy documents")]
    PlacementUnavailable,
    #[error(transparent)]
    PlacementResolve(#[from] PlacementResolveError),
    /// Every reached holder answered without the document.
    #[error("policy {policy_id} is unknown to its holders")]
    NotFound { policy_id: ulid::Ulid },
    /// No holder answered: an availability fact, never a denial.
    #[error("policy holders are unavailable: {0}")]
    Unavailable(String),
    /// The answering holder returned bytes that do not match the requested ref.
    #[error("policy document does not match the requested ref")]
    DigestMismatch,
    /// A document stored or served for another realm is not this rule.
    #[error("policy document belongs to another realm")]
    RealmMismatch,
    /// An answer from a node this fetch never asked is not holder evidence.
    #[error("policy answer came from an unasked node")]
    UnexpectedPublisher,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl ReadPolicyOperation {
    pub fn new(config: ReadPolicyConfig) -> Self {
        Self {
            config,
            state: ReadPolicyState::Init,
            output: None,
        }
    }

    fn target(&self) -> DocumentSyncTarget {
        placement_policy_target(self.config.policy_ref.policy_id)
    }

    /// Accepts a candidate only when it verifies and hashes to the requested
    /// ref, so neither a stale local row nor a peer can substitute a rule.
    fn accept(
        &self,
        policy: aruna_core::structs::PlacementPolicy,
    ) -> Result<VerifiedPolicy, ReadPolicyError> {
        let verified = VerifiedPolicy::verify(policy)?;
        if verified.policy_ref() != self.config.policy_ref {
            return Err(ReadPolicyError::DigestMismatch);
        }
        Ok(verified)
    }

    /// A stored row counts only when it is this realm's document and hashes to
    /// the requested ref.
    fn accept_local(
        &self,
        value: &Value,
    ) -> Result<(VerifiedPolicy, PolicySource), ReadPolicyError> {
        let document = PlacementPolicyDocument::from_bytes(value)?;
        if document.realm_id != self.config.realm_id {
            return Err(ReadPolicyError::RealmMismatch);
        }
        Ok((self.accept(document.policy)?, PolicySource::Local))
    }

    fn emit_read_config(&mut self) -> Effects {
        self.state = ReadPolicyState::ReadConfig;
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })]
    }

    fn plan_fetch(&mut self, config_value: Option<Value>) -> Result<Effects, ReadPolicyError> {
        let Some(config_value) = config_value else {
            return Err(ReadPolicyError::RealmConfigMissing);
        };
        let config = RealmConfigDocument::from_bytes(&config_value)?;
        let placement =
            crate::placement::plan_target_placement(&config, &self.target(), Default::default())?
                .ok_or(ReadPolicyError::PlacementUnavailable)?
                .placement;
        let holders: Vec<_> = read_holder_sets(&config, &placement)?
            .into_iter()
            .filter(|holder| *holder != self.config.local_node_id)
            .take(MAX_POLICY_FETCH_HOLDERS)
            .collect();
        self.emit_fetch(holders)
    }

    fn emit_fetch(&mut self, holders: Vec<aruna_core::NodeId>) -> Result<Effects, ReadPolicyError> {
        let asked = holders.clone();
        let holders = HolderList::new(holders)
            .map_err(|error| ReadPolicyError::Unavailable(error.to_string()))?;

        self.state = ReadPolicyState::Fetch { asked };
        Ok(smallvec![Effect::Net(NetEffect::PolicyFetch(Box::new(
            PolicyFetchEffect {
                realm_id: self.config.realm_id,
                holders,
                policy_ref: self.config.policy_ref,
                deadline: POLICY_FETCH_DEADLINE,
            }
        )))])
    }

    /// Accepts one holder answer or drops that holder and asks the rest, so a
    /// single corrupt or stale holder cannot deny a resolvable policy.
    fn accept_fetched(
        &mut self,
        asked: Vec<aruna_core::NodeId>,
        publisher: aruna_core::NodeId,
        policy: aruna_core::structs::PlacementPolicy,
    ) -> Effects {
        if !asked.contains(&publisher) {
            return self.finish(Err(ReadPolicyError::UnexpectedPublisher));
        }
        match self.accept(policy) {
            Ok(policy) => self.finish(Ok((policy, PolicySource::Fetched))),
            Err(error) => {
                let remaining: Vec<_> = asked
                    .into_iter()
                    .filter(|holder| *holder != publisher)
                    .collect();
                if remaining.is_empty() {
                    return self.finish(Err(error));
                }
                match self.emit_fetch(remaining) {
                    Ok(effects) => effects,
                    Err(error) => self.finish(Err(error)),
                }
            }
        }
    }

    fn finish(
        &mut self,
        result: Result<(VerifiedPolicy, PolicySource), ReadPolicyError>,
    ) -> Effects {
        self.state = if result.is_ok() {
            ReadPolicyState::Finish
        } else {
            ReadPolicyState::Error
        };
        self.output = Some(result);
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.finish(Err(ReadPolicyError::UnexpectedEvent {
            state,
            expected,
            got,
        }))
    }
}

impl Operation for ReadPolicyOperation {
    type Output = (VerifiedPolicy, PolicySource);
    type Error = ReadPolicyError;

    fn start(&mut self) -> Effects {
        self.state = ReadPolicyState::ReadLocal;
        let target = self.target();
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            ReadPolicyState::ReadLocal => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => match value {
                    Some(value) => {
                        let result = self.accept_local(&value);
                        self.finish(result)
                    }
                    None => self.emit_read_config(),
                },
                Event::Storage(StorageEvent::Error { error }) => self.finish(Err(error.into())),
                other => self.unexpected_event("local policy read", format!("{other:?}")),
            },
            ReadPolicyState::ReadConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    match self.plan_fetch(value) {
                        Ok(effects) => effects,
                        Err(error) => self.finish(Err(error)),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.finish(Err(error.into())),
                other => self.unexpected_event("realm config read", format!("{other:?}")),
            },
            ReadPolicyState::Fetch { asked } => match event {
                Event::Net(NetEvent::PolicyFetch(fetched)) => match fetched {
                    PolicyFetchEvent::Fetched { publisher, policy } => {
                        self.accept_fetched(asked, publisher, *policy)
                    }
                    PolicyFetchEvent::NotFound => self.finish(Err(ReadPolicyError::NotFound {
                        policy_id: self.config.policy_ref.policy_id,
                    })),
                    PolicyFetchEvent::Unavailable(reason) => {
                        self.finish(Err(ReadPolicyError::Unavailable(reason)))
                    }
                },
                other => self.unexpected_event("policy fetch result", format!("{other:?}")),
            },
            ReadPolicyState::Init | ReadPolicyState::Finish | ReadPolicyState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReadPolicyState::Finish | ReadPolicyState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ReadPolicyError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            ReadPolicyError::NotFound { .. }
                | ReadPolicyError::Unavailable(_)
                | ReadPolicyError::DigestMismatch
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
    use aruna_core::effects::NetEffect;
    use aruna_core::structs::{
        Actor, LabelMatch, PlacementPolicy, PlacementPolicyDocument, PlacementSelector,
        RealmNodeKind,
    };
    use aruna_core::types::UserId;
    use byteview::ByteView;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm_id() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    fn config() -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 2);
        config.seed_default_placement();
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        config
    }

    fn policy(location: &str) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([8u8; 16]),
            "eu-only".to_string(),
            vec![PlacementSelector {
                node_id: None,
                location: Some(location.to_string()),
                labels: vec![LabelMatch {
                    key: "tier".to_string(),
                    value: "hot".to_string(),
                }],
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn read_result(value: Option<ByteView>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(Vec::new()),
            value,
        })
    }

    fn operation(local: NodeId, policy_ref: PlacementPolicyRef) -> ReadPolicyOperation {
        ReadPolicyOperation::new(ReadPolicyConfig {
            realm_id: realm_id(),
            policy_ref,
            local_node_id: local,
        })
    }

    fn document(policy: &VerifiedPolicy) -> ByteView {
        ByteView::from(
            PlacementPolicyDocument::new(
                realm_id(),
                policy,
                UserId::local(Ulid::from_bytes([2u8; 16]), realm_id()),
                node(1),
                Ulid::from_bytes([5u8; 16]),
                7,
            )
            .to_bytes()
            .expect("document encodes"),
        )
    }

    fn foreign_document(policy: &VerifiedPolicy) -> ByteView {
        ByteView::from(
            PlacementPolicyDocument::new(
                RealmId::from_bytes([7u8; 32]),
                policy,
                UserId::local(Ulid::from_bytes([2u8; 16]), realm_id()),
                node(1),
                Ulid::from_bytes([5u8; 16]),
                7,
            )
            .to_bytes()
            .expect("document encodes"),
        )
    }

    /// Drives one operation to its first fetch and reports the holders it asked.
    fn start_fetch(policy_ref: PlacementPolicyRef) -> (ReadPolicyOperation, Vec<NodeId>) {
        let mut operation = operation(node(9), policy_ref);
        operation.start();
        operation.step(read_result(None));
        let actor = Actor {
            node_id: node(1),
            user_id: UserId::local(Ulid::from_bytes([2u8; 16]), realm_id()),
            realm_id: realm_id(),
        };
        let effects = operation.step(read_result(Some(ByteView::from(
            config().to_bytes(&actor).expect("config encodes"),
        ))));
        let Some(Effect::Net(NetEffect::PolicyFetch(fetch))) = effects.first() else {
            panic!("expected a policy fetch, got {effects:?}");
        };
        let holders = fetch.holders.as_slice().to_vec();
        (operation, holders)
    }

    fn fetched(publisher: NodeId, policy: &VerifiedPolicy) -> Event {
        Event::Net(NetEvent::PolicyFetch(PolicyFetchEvent::Fetched {
            publisher,
            policy: Box::new(policy.policy().clone()),
        }))
    }

    #[test]
    fn fetches_from_holders() {
        // A non-holder resolves the rule's holders from the policy id alone, with
        // no catalog lookup, and only then asks them for the bytes.
        let policy = policy("eu-west");
        let config = config();
        let mut operation = operation(node(9), policy.policy_ref());
        operation.start();
        operation.step(read_result(None));
        let actor = Actor {
            node_id: node(1),
            user_id: UserId::local(Ulid::from_bytes([2u8; 16]), realm_id()),
            realm_id: realm_id(),
        };
        let effects = operation.step(read_result(Some(ByteView::from(
            config.to_bytes(&actor).expect("config encodes"),
        ))));

        let Some(Effect::Net(NetEffect::PolicyFetch(fetch))) = effects.first() else {
            panic!("expected a policy fetch, got {effects:?}");
        };
        assert_eq!(fetch.policy_ref, policy.policy_ref());
        let placement = config
            .policy_placement(policy.policy().policy_id)
            .expect("policy bucket resolves");
        let expected = crate::placement::read_holder_sets(&config, &placement).expect("holders");
        assert_eq!(fetch.holders.as_slice(), expected.as_slice());

        operation.step(fetched(expected[0], &policy));
        assert_eq!(
            operation.finalize().expect("policy resolves"),
            (policy, PolicySource::Fetched)
        );
    }

    #[test]
    fn rejects_other_digest() {
        // A holder answering with another definition under the requested id must
        // never be accepted: identity is the id together with its digest.
        let requested = policy("eu-west");
        let mut operation = operation(node(9), requested.policy_ref());
        operation.start();
        operation.step(read_result(Some(document(&policy("us-east")))));
        assert_eq!(
            operation.finalize(),
            Err(ReadPolicyError::DigestMismatch),
            "a substituted definition must fail closed"
        );
    }

    #[test]
    fn reads_local_row() {
        let policy = policy("eu-west");
        let mut operation = operation(node(1), policy.policy_ref());
        operation.start();
        operation.step(read_result(Some(document(&policy))));
        assert_eq!(
            operation.finalize().expect("policy resolves"),
            (policy, PolicySource::Local)
        );
    }

    #[test]
    fn rejects_foreign_realm() {
        // A row published for another realm is not this realm's rule, however
        // well it hashes.
        let policy = policy("eu-west");
        let mut operation = operation(node(1), policy.policy_ref());
        operation.start();
        operation.step(read_result(Some(foreign_document(&policy))));
        assert_eq!(operation.finalize(), Err(ReadPolicyError::RealmMismatch));
    }

    #[test]
    fn skips_corrupt_holder() {
        // One holder answering with another definition must not deny a policy the
        // remaining holders can still serve.
        let requested = policy("eu-west");
        let (mut operation, holders) = start_fetch(requested.policy_ref());
        assert!(holders.len() >= 2, "the fixture needs a fallback holder");

        let effects = operation.step(fetched(holders[0], &policy("us-east")));
        let Some(Effect::Net(NetEffect::PolicyFetch(retry))) = effects.first() else {
            panic!("expected a retry against the remaining holders, got {effects:?}");
        };
        assert_eq!(retry.holders.as_slice(), &holders[1..]);

        operation.step(fetched(holders[1], &requested));
        assert_eq!(
            operation.finalize().expect("policy resolves"),
            (requested, PolicySource::Fetched)
        );
    }

    #[test]
    fn exhausts_bad_holders() {
        // With no honest holder left the read fails closed instead of accepting
        // the last answer it received.
        let requested = policy("eu-west");
        let (mut operation, holders) = start_fetch(requested.policy_ref());
        for holder in &holders {
            operation.step(fetched(*holder, &policy("us-east")));
        }
        assert_eq!(operation.finalize(), Err(ReadPolicyError::DigestMismatch));
    }

    #[test]
    fn rejects_unasked_answer() {
        // An answer from a node this fetch never asked is not holder evidence.
        let requested = policy("eu-west");
        let (mut operation, _) = start_fetch(requested.policy_ref());
        operation.step(fetched(node(9), &requested));
        assert_eq!(
            operation.finalize(),
            Err(ReadPolicyError::UnexpectedPublisher)
        );
    }
}
