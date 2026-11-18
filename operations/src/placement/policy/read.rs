//! Reads one immutable policy document by ref: local row first, then a bounded holder fetch.
//! A document failing the hash or the admin authority check is refused, not returned.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{HolderList, MAX_FETCH_HOLDERS, PolicyFetchEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, PolicyFetchEvent};
use aruna_core::operation::{Operation, SubOperation, boxed_suboperation};
use aruna_core::rows::{event, values};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::placement::policy::document::{
    PlacementPolicyDocument, PolicyAuthorityError, placement_policy_target, verify_policy_authority,
};
use aruna_core::structs::placement::policy::{
    PlacementPolicyError, PlacementPolicyRef, VerifiedPolicy,
};
use aruna_core::types::Value;
use std::time::Duration;
use thiserror::Error;

use aruna_core::placement::{PlacementResolveError, read_holder_sets};

use crate::forward::fetch::{Answer, Budget, Fetched, HolderFetchOperation, HolderRecord};
use crate::metadata::protocol::MetadataTransportMessage;

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

/// One verified policy together with the publication it was authenticated
/// against, so a caller can cache or audit its provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthenticPolicy {
    pub document: PlacementPolicyDocument,
    pub policy: VerifiedPolicy,
}

/// The replicated realm view one publication is verified against.
#[derive(Debug, Clone, PartialEq)]
struct RealmView {
    config: RealmConfigDocument,
    auth: RealmAuthorizationDocument,
}

#[derive(Debug, PartialEq)]
pub struct ReadPolicyOperation {
    config: ReadPolicyConfig,
    realm: Option<Box<RealmView>>,
    state: ReadPolicyState,
}

type ReadTurn = Result<Next<(AuthenticPolicy, PolicySource)>, ReadPolicyError>;

/// A candidate document waiting for the owning group's authorization state.
/// `remaining` names the holders still to ask when this one is refused.
#[derive(Debug, Clone, PartialEq)]
struct PendingAuthority {
    document: PlacementPolicyDocument,
    policy: VerifiedPolicy,
    source: PolicySource,
    remaining: Vec<aruna_core::NodeId>,
}

#[derive(Debug, Clone, PartialEq)]
enum ReadPolicyState {
    ReadLocal,
    /// Holders this fetch was sent to; a holder that answers with another
    /// definition is dropped and the remainder is asked again.
    Fetch {
        asked: Vec<aruna_core::NodeId>,
    },
    /// A group-owned candidate is authenticated only once its owner's roles
    /// are in hand; without them the read fails closed.
    ReadGroupAuth {
        pending: Box<PendingAuthority>,
    },
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadPolicyError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    /// The publication does not prove realm-admin authority for this rule.
    #[error(transparent)]
    Authority(#[from] PolicyAuthorityError),
    #[error("realm config document missing")]
    RealmConfigMissing,
    #[error("no placement strategy governs policy documents")]
    PlacementUnavailable,
    #[error(transparent)]
    PlacementResolve(#[from] PlacementResolveError),
    /// Every reached holder answered without the document.
    #[error("policy {policy_id} is unknown to its holders")]
    NotFound { policy_id: ulid::Ulid },
    /// No holder answered: an availability detail, never a denial.
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
        got: &'static str,
    },
}

impl From<IoError> for ReadPolicyError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ReadPolicy".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

impl ReadPolicyOperation {
    pub fn new(config: ReadPolicyConfig) -> Self {
        Self {
            config,
            realm: None,
            state: ReadPolicyState::ReadLocal,
        }
    }

    fn target(&self) -> DocumentTarget {
        placement_policy_target(self.config.policy_ref.policy_id)
    }

    /// Accepts a candidate only when it verifies and hashes to the requested
    /// ref, so neither a stale local row nor a peer can substitute a rule.
    fn accept(
        &self,
        policy: aruna_core::structs::placement::policy::PlacementPolicy,
    ) -> Result<VerifiedPolicy, ReadPolicyError> {
        let verified = VerifiedPolicy::verify(policy)?;
        if verified.policy_ref() != self.config.policy_ref {
            return Err(ReadPolicyError::DigestMismatch);
        }
        Ok(verified)
    }

    /// A document counts only when it is this realm's and hashes to the
    /// requested ref; its publication is authenticated separately, because a
    /// group-owned rule needs its owner's roles.
    fn check_document(
        &self,
        document: PlacementPolicyDocument,
    ) -> Result<(PlacementPolicyDocument, VerifiedPolicy), ReadPolicyError> {
        if document.realm_id != self.config.realm_id {
            return Err(ReadPolicyError::RealmMismatch);
        }
        let policy = self.accept(document.policy.clone())?;
        Ok((document, policy))
    }

    fn authenticate(
        &self,
        document: PlacementPolicyDocument,
        policy: VerifiedPolicy,
        group_auth: Option<&GroupAuthorizationDocument>,
    ) -> Result<AuthenticPolicy, ReadPolicyError> {
        let realm = self
            .realm
            .as_ref()
            .ok_or_else(|| ReadPolicyError::Unavailable("realm view unavailable".to_string()))?;
        verify_policy_authority(&document, &realm.config, &realm.auth, group_auth)?;
        Ok(AuthenticPolicy { document, policy })
    }

    /// Either finishes the candidate or reads the roles of the group that owns
    /// it. `remaining` is what a refused candidate falls back to.
    fn decide(
        &mut self,
        document: PlacementPolicyDocument,
        source: PolicySource,
        remaining: Vec<aruna_core::NodeId>,
    ) -> ReadTurn {
        let (document, policy) = match self.check_document(document) {
            Ok(checked) => checked,
            Err(error) => return self.refuse(error, remaining),
        };
        let Some(group_id) = document.policy.owner_group_id else {
            return match self.authenticate(document, policy, None) {
                Ok(authentic) => Ok(Next::Done((authentic, source))),
                Err(error) => self.refuse(error, remaining),
            };
        };
        let target = DocumentTarget::GroupAuthorization { group_id };
        self.state = ReadPolicyState::ReadGroupAuth {
            pending: Box::new(PendingAuthority {
                document,
                policy,
                source,
                remaining,
            }),
        };
        Ok(Next::Call(IoCall::Read(vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
        )])))
    }

    fn handle_group_auth(&mut self, pending: PendingAuthority, value: Option<Value>) -> ReadTurn {
        let group_auth = match value
            .as_ref()
            .map(|value| GroupAuthorizationDocument::from_bytes(value))
            .transpose()
        {
            Ok(group_auth) => group_auth,
            Err(error) => return self.refuse(error.into(), pending.remaining),
        };
        let PendingAuthority {
            document,
            policy,
            source,
            remaining,
        } = pending;
        match self.authenticate(document, policy, group_auth.as_ref()) {
            Ok(authentic) => Ok(Next::Done((authentic, source))),
            Err(error) => self.refuse(error, remaining),
        }
    }

    /// A refused candidate ends the read only when no holder is left to ask.
    fn refuse(&mut self, error: ReadPolicyError, remaining: Vec<aruna_core::NodeId>) -> ReadTurn {
        if remaining.is_empty() {
            return Err(error);
        }
        self.emit_fetch(remaining)
    }

    /// Keeps the realm view every publication is verified against. Without it
    /// nothing is authenticated, so the read reports unavailable.
    fn store_realm(
        &mut self,
        config_value: Option<Value>,
        auth_value: Option<Value>,
    ) -> Result<(), ReadPolicyError> {
        let Some(config_value) = config_value else {
            return Err(ReadPolicyError::RealmConfigMissing);
        };
        let Some(auth_value) = auth_value else {
            return Err(ReadPolicyError::Unavailable(
                "realm authorization document missing".to_string(),
            ));
        };
        self.realm = Some(Box::new(RealmView {
            config: RealmConfigDocument::from_bytes(&config_value)?,
            auth: RealmAuthorizationDocument::from_bytes(&auth_value)?,
        }));
        Ok(())
    }

    fn plan_fetch(&mut self) -> ReadTurn {
        let holders = {
            let Some(realm) = self.realm.as_ref() else {
                return Err(ReadPolicyError::RealmConfigMissing);
            };
            let placement = aruna_core::placement::plan_target_placement(
                &realm.config,
                &self.target(),
                Default::default(),
            )?
            .ok_or(ReadPolicyError::PlacementUnavailable)?
            .placement;
            read_holder_sets(&realm.config, &placement)?
                .into_iter()
                .filter(|holder| *holder != self.config.local_node_id)
                .take(MAX_FETCH_HOLDERS)
                .collect()
        };
        self.emit_fetch(holders)
    }

    fn emit_fetch(&mut self, holders: Vec<aruna_core::NodeId>) -> ReadTurn {
        let asked = holders.clone();
        let holders = HolderList::new(holders)
            .map_err(|error| ReadPolicyError::Unavailable(error.to_string()))?;

        self.state = ReadPolicyState::Fetch { asked };
        Ok(Next::Call(IoCall::Child(fetch_child(&PolicyFetchEffect {
            realm_id: self.config.realm_id,
            holders,
            policy_ref: self.config.policy_ref,
            deadline: POLICY_FETCH_DEADLINE,
        }))))
    }

    /// Accepts one holder answer or drops that holder and asks the rest, so a
    /// single corrupt or stale holder cannot deny a resolvable policy.
    fn accept_fetched(
        &mut self,
        asked: Vec<aruna_core::NodeId>,
        publisher: aruna_core::NodeId,
        document: PlacementPolicyDocument,
    ) -> ReadTurn {
        if !asked.contains(&publisher) {
            return Err(ReadPolicyError::UnexpectedPublisher);
        }
        let remaining: Vec<_> = asked
            .into_iter()
            .filter(|holder| *holder != publisher)
            .collect();
        self.decide(document, PolicySource::Fetched, remaining)
    }
}

impl Operation for ReadPolicyOperation {
    type Output = (AuthenticPolicy, PolicySource);
    type Error = ReadPolicyError;

    /// The realm view is read with the row: a local hit needs it to verify
    /// publication authority, and a miss needs it to resolve the holders.
    fn first(&mut self) -> ReadTurn {
        let target = self.target();
        let config = DocumentTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        let auth = DocumentTarget::RealmAuthorization {
            realm_id: self.config.realm_id,
        };
        Ok(Next::Call(IoCall::Read(vec![
            (target.storage_keyspace().to_string(), target.storage_key()),
            (config.storage_keyspace().to_string(), config.storage_key()),
            (auth.storage_keyspace().to_string(), auth.storage_key()),
        ])))
    }

    fn next(&mut self, result: IoResult) -> ReadTurn {
        match self.state.clone() {
            ReadPolicyState::ReadLocal => {
                let [policy_value, config_value, auth_value] = values::<3>(result)?;
                self.store_realm(config_value, auth_value)?;
                match policy_value {
                    Some(value) => {
                        let document = PlacementPolicyDocument::from_bytes(&value)?;
                        self.decide(document, PolicySource::Local, Vec::new())
                    }
                    None => self.plan_fetch(),
                }
            }
            ReadPolicyState::Fetch { asked } => match event(result)? {
                Event::Net(NetEvent::PolicyFetch(fetched)) => match fetched {
                    PolicyFetchEvent::Fetched {
                        publisher,
                        document,
                    } => self.accept_fetched(asked, publisher, *document),
                    PolicyFetchEvent::NotFound => Err(ReadPolicyError::NotFound {
                        policy_id: self.config.policy_ref.policy_id,
                    }),
                    PolicyFetchEvent::Unavailable(reason) => {
                        Err(ReadPolicyError::Unavailable(reason))
                    }
                },
                other => Err(ReadPolicyError::UnexpectedEvent {
                    state: "fetch".to_string(),
                    expected: "policy fetch result",
                    got: other.label(),
                }),
            },
            ReadPolicyState::ReadGroupAuth { pending } => {
                let [value] = values::<1>(result)?;
                self.handle_group_auth(*pending, value)
            }
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            ReadPolicyError::NotFound { .. }
                | ReadPolicyError::Unavailable(_)
                | ReadPolicyError::DigestMismatch
                | ReadPolicyError::Authority(_)
        )
    }
}

impl HolderRecord for Box<PlacementPolicyDocument> {
    const LABEL: &'static str = "policy";

    fn read(message: MetadataTransportMessage) -> Answer<Self> {
        match message {
            MetadataTransportMessage::ForwardedPlacementPolicy {
                result: Ok(Some(document)),
            } => Answer::Found(document),
            MetadataTransportMessage::ForwardedPlacementPolicy { result: Ok(None) } => {
                Answer::Missing
            }
            _ => Answer::Skip,
        }
    }
}

/// Asks the holders in rank order under one deadline; the answer arrives as a policy fetch event.
fn fetch_child(fetch: &PolicyFetchEffect) -> Box<dyn SubOperation> {
    let request = MetadataTransportMessage::ForwardPlacementPolicy {
        policy_ref: fetch.policy_ref,
    };
    let holders = fetch.holders.as_slice();
    let operation = HolderFetchOperation::<Box<PlacementPolicyDocument>>::new(
        holders,
        &request,
        fetch.deadline,
        Budget::Shared,
    );
    boxed_suboperation(operation, |result| {
        Event::Net(NetEvent::PolicyFetch(match result {
            Ok(Fetched::Found { holder, value }) => PolicyFetchEvent::Fetched {
                publisher: holder,
                document: value,
            },
            Ok(Fetched::NotFound) => PolicyFetchEvent::NotFound,
            Ok(Fetched::Denied) => PolicyFetchEvent::Unavailable("a holder refused".to_string()),
            Ok(Fetched::Unavailable(reason)) => PolicyFetchEvent::Unavailable(reason),
            Err(error) => PolicyFetchEvent::Unavailable(error.to_string()),
        }))
    })
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::tests::policy::{admin_user, fetch_requests, realm_view, signed_document};
    use aruna_core::NodeId;
    use aruna_core::UserId;
    use aruna_core::effects::Effect;
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use aruna_core::structs::placement::policy::document::PolicyPublicationClaim;
    use aruna_core::structs::placement::policy::{PlacementPolicy, PlacementSelector};
    use aruna_core::structs::placement::record::LabelMatch;
    use aruna_core::types::Effects;
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

    /// The one batch the read starts with: policy row plus the realm view every
    /// publication is verified against. The config is passed in because each
    /// fixture mints its own strategy ids.
    fn opened(config: &RealmConfigDocument, policy_row: Option<Value>) -> IoResult {
        let (config_value, auth_value) = realm_view(config, admin_user(realm_id()));
        IoResult::Values(vec![policy_row, Some(config_value), Some(auth_value)])
    }

    /// Starts the read and answers its first batch.
    fn begin(operation: &mut ReadPolicyOperation, answer: IoResult) -> ReadTurn {
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        operation.next(answer)
    }

    /// The holders a fetch child asks and the request it sends them.
    fn asked(turn: ReadTurn) -> (Vec<NodeId>, MetadataTransportMessage) {
        let Ok(Next::Call(IoCall::Child(child))) = turn else {
            panic!("expected a holder fetch child");
        };
        let effects: Effects = [Effect::SubOperation(child)].into_iter().collect();
        fetch_requests(effects)
    }

    /// The outcome of a read that ended.
    fn ended(turn: ReadTurn) -> Result<(AuthenticPolicy, PolicySource), ReadPolicyError> {
        match turn? {
            Next::Done(outcome) => Ok(outcome),
            _ => panic!("expected the read to end"),
        }
    }

    fn operation(local: NodeId, policy_ref: PlacementPolicyRef) -> ReadPolicyOperation {
        ReadPolicyOperation::new(ReadPolicyConfig {
            realm_id: realm_id(),
            policy_ref,
            local_node_id: local,
        })
    }

    fn encoded(document: &PlacementPolicyDocument) -> Value {
        Value::from(document.to_bytes().expect("document encodes"))
    }

    fn document(policy: &VerifiedPolicy) -> Value {
        encoded(&signed_document(realm_id(), policy, 1))
    }

    fn foreign_document(policy: &VerifiedPolicy) -> Value {
        encoded(&signed_document(RealmId::from_bytes([7u8; 32]), policy, 1))
    }

    /// A publication signed by node `seed` naming `created_by` as its authority.
    fn authored(policy: &VerifiedPolicy, seed: u8, created_by: UserId) -> PlacementPolicyDocument {
        let secret = iroh::SecretKey::from_bytes(&[seed; 32]);
        let publication = PolicyPublicationClaim::new(
            realm_id(),
            policy,
            secret.public(),
            created_by,
            Ulid::from_bytes([5u8; 16]),
            7,
            [0u8; 32],
        )
        .sign(&secret);
        PlacementPolicyDocument::new(realm_id(), policy, publication)
    }

    /// Drives one operation to its first fetch and reports the holders it asked.
    fn start_fetch(policy_ref: PlacementPolicyRef) -> (ReadPolicyOperation, Vec<NodeId>) {
        let mut operation = operation(node(9), policy_ref);
        let (holders, _) = asked(begin(&mut operation, opened(&config(), None)));
        (operation, holders)
    }

    fn fetched(publisher: NodeId, document: PlacementPolicyDocument) -> IoResult {
        let fetched = PolicyFetchEvent::Fetched {
            publisher,
            document: Box::new(document),
        };
        IoResult::Event(Box::new(Event::Net(NetEvent::PolicyFetch(fetched))))
    }

    /// Answers each holder in turn and returns the last outcome.
    fn answer_all(
        operation: &mut ReadPolicyOperation,
        holders: &[NodeId],
        answer: impl Fn(NodeId) -> IoResult,
    ) -> ReadTurn {
        let mut turn = Err(ReadPolicyError::NotFinished);
        for holder in holders {
            turn = operation.next(answer(*holder));
        }
        turn
    }

    fn answered(publisher: NodeId, policy: &VerifiedPolicy) -> IoResult {
        fetched(publisher, signed_document(realm_id(), policy, 1))
    }

    #[test]
    fn fetches_from_holders() {
        // A non-holder resolves the rule's holders from the policy id alone, with
        // no catalog lookup, and only then asks them for the bytes.
        let policy = policy("eu-west");
        let config = config();
        let mut operation = operation(node(9), policy.policy_ref());
        let (holders, request) = asked(begin(&mut operation, opened(&config, None)));
        assert_eq!(
            request,
            MetadataTransportMessage::ForwardPlacementPolicy {
                policy_ref: policy.policy_ref()
            }
        );
        let placement = config
            .policy_placement(policy.policy().policy_id)
            .expect("policy bucket resolves");
        let expected =
            aruna_core::placement::read_holder_sets(&config, &placement).expect("holders");
        assert_eq!(holders.as_slice(), expected.as_slice());

        let turn = operation.next(answered(expected[0], &policy));
        let (authentic, source) = ended(turn).expect("policy resolves");
        assert_eq!(authentic.policy, policy);
        assert_eq!(source, PolicySource::Fetched);
    }

    #[test]
    fn rejects_other_digest() {
        // A holder answering with another definition under the requested id must
        // never be accepted: identity is the id together with its digest.
        let requested = policy("eu-west");
        let mut operation = operation(node(9), requested.policy_ref());
        let opened = opened(&config(), Some(document(&policy("us-east"))));
        assert_eq!(
            ended(begin(&mut operation, opened)),
            Err(ReadPolicyError::DigestMismatch),
            "a substituted definition must fail closed"
        );
    }

    #[test]
    fn reads_local_row() {
        let policy = policy("eu-west");
        let mut operation = operation(node(1), policy.policy_ref());
        let opened = opened(&config(), Some(document(&policy)));
        let (authentic, source) = ended(begin(&mut operation, opened)).expect("policy resolves");
        assert_eq!(authentic.policy, policy);
        assert_eq!(source, PolicySource::Local);
    }

    #[test]
    fn rejects_foreign_realm() {
        // A row published for another realm is not this realm's rule, however
        // well it hashes.
        let policy = policy("eu-west");
        let mut operation = operation(node(1), policy.policy_ref());
        let opened = opened(&config(), Some(foreign_document(&policy)));
        assert_eq!(
            ended(begin(&mut operation, opened)),
            Err(ReadPolicyError::RealmMismatch)
        );
    }

    #[test]
    fn rejects_forged_bytes() {
        // A holder that rewrites the publication onto a definition it prefers is
        // refused: the signature no longer binds this realm and digest.
        let requested = policy("eu-west");
        let mut forged = signed_document(realm_id(), &requested, 1);
        forged.publication.created_by = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id());
        let (mut operation, holders) = start_fetch(requested.policy_ref());
        let turn = answer_all(&mut operation, &holders, |holder| {
            fetched(holder, forged.clone())
        });
        assert_eq!(
            ended(turn),
            Err(ReadPolicyError::Authority(PolicyAuthorityError::Signature))
        );
    }

    #[test]
    fn rejects_unauthorized_author() {
        // A validly signed policy whose authorizing user holds no realm-admin
        // write is not an authentic publication.
        let requested = policy("eu-west");
        let outsider = UserId::local(Ulid::from_bytes([6u8; 16]), realm_id());
        let (mut operation, holders) = start_fetch(requested.policy_ref());
        let turn = answer_all(&mut operation, &holders, |holder| {
            fetched(holder, authored(&requested, 1, outsider))
        });
        assert_eq!(
            ended(turn),
            Err(ReadPolicyError::Authority(
                PolicyAuthorityError::Unauthorized
            ))
        );
    }

    #[test]
    fn rejects_foreign_publisher() {
        // A node outside the realm cannot publish a rule, however well it signs.
        let requested = policy("eu-west");
        let (mut operation, holders) = start_fetch(requested.policy_ref());
        let turn = answer_all(&mut operation, &holders, |holder| {
            fetched(holder, authored(&requested, 9, admin_user(realm_id())))
        });
        assert_eq!(
            ended(turn),
            Err(ReadPolicyError::Authority(PolicyAuthorityError::Publisher))
        );
    }

    #[test]
    fn skips_corrupt_holder() {
        // One holder answering with another definition must not deny a policy the
        // remaining holders can still serve.
        let requested = policy("eu-west");
        let (mut operation, holders) = start_fetch(requested.policy_ref());
        assert!(holders.len() >= 2, "the fixture needs a fallback holder");

        let (retry, _) = asked(operation.next(answered(holders[0], &policy("us-east"))));
        assert_eq!(retry.as_slice(), &holders[1..]);

        let turn = operation.next(answered(holders[1], &requested));
        assert_eq!(ended(turn).expect("policy resolves").0.policy, requested);
    }

    #[test]
    fn exhausts_bad_holders() {
        // With no honest holder left the read fails closed instead of accepting
        // the last answer it received.
        let requested = policy("eu-west");
        let (mut operation, holders) = start_fetch(requested.policy_ref());
        let turn = answer_all(&mut operation, &holders, |holder| {
            answered(holder, &policy("us-east"))
        });
        assert_eq!(ended(turn), Err(ReadPolicyError::DigestMismatch));
    }

    #[test]
    fn rejects_unasked_answer() {
        // An answer from a node this fetch never asked is not holder evidence.
        let requested = policy("eu-west");
        let (mut operation, _) = start_fetch(requested.policy_ref());
        assert_eq!(
            ended(operation.next(answered(node(9), &requested))),
            Err(ReadPolicyError::UnexpectedPublisher)
        );
    }

    #[test]
    fn requires_realm_view() {
        // Without the replicated authorization document nothing can be
        // authenticated, so the read reports unavailable instead of trusting it.
        let policy = policy("eu-west");
        let mut operation = operation(node(1), policy.policy_ref());
        let (config_value, _) = realm_view(&config(), admin_user(realm_id()));
        let opened = IoResult::Values(vec![Some(document(&policy)), Some(config_value), None]);
        assert!(matches!(
            ended(begin(&mut operation, opened)),
            Err(ReadPolicyError::Unavailable(_))
        ));
    }
}
