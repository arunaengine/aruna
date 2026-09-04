//! Reading one immutable policy document by ref: local row first, then a
//! bounded fetch from the holders the policy id resolves to. No catalog is
//! consulted; a document that does not hash to the requested digest, or whose
//! publication was not accepted under realm-admin authority, is refused rather
//! than returned.

use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{
    Effect, HolderList, MAX_POLICY_FETCH_HOLDERS, NetEffect, PolicyFetchEffect, StorageEffect,
};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, PolicyFetchEvent, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    GroupAuthorizationDocument, PlacementPolicyDocument, PlacementPolicyError, PlacementPolicyRef,
    PolicyAuthorityError, RealmAuthorizationDocument, RealmConfigDocument, RealmId, VerifiedPolicy,
    placement_policy_target, verify_policy_authority,
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
    output: Option<Result<(AuthenticPolicy, PolicySource), ReadPolicyError>>,
}

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
    Init,
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
        got: String,
    },
}

impl ReadPolicyOperation {
    pub fn new(config: ReadPolicyConfig) -> Self {
        Self {
            config,
            realm: None,
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
    ) -> Effects {
        let (document, policy) = match self.check_document(document) {
            Ok(checked) => checked,
            Err(error) => return self.refuse(error, remaining),
        };
        let Some(group_id) = document.policy.owner_group_id else {
            return match self.authenticate(document, policy, None) {
                Ok(authentic) => self.finish(Ok((authentic, source))),
                Err(error) => self.refuse(error, remaining),
            };
        };
        let target = DocumentSyncTarget::GroupAuthorization { group_id };
        self.state = ReadPolicyState::ReadGroupAuth {
            pending: Box::new(PendingAuthority {
                document,
                policy,
                source,
                remaining,
            }),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })]
    }

    fn handle_group_auth(&mut self, pending: PendingAuthority, value: Option<Value>) -> Effects {
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
            Ok(authentic) => self.finish(Ok((authentic, source))),
            Err(error) => self.refuse(error, remaining),
        }
    }

    /// A refused candidate ends the read only when no holder is left to ask.
    fn refuse(&mut self, error: ReadPolicyError, remaining: Vec<aruna_core::NodeId>) -> Effects {
        if remaining.is_empty() {
            return self.finish(Err(error));
        }
        match self.emit_fetch(remaining) {
            Ok(effects) => effects,
            Err(error) => self.finish(Err(error)),
        }
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

    fn plan_fetch(&mut self) -> Result<Effects, ReadPolicyError> {
        let holders = {
            let Some(realm) = self.realm.as_ref() else {
                return Err(ReadPolicyError::RealmConfigMissing);
            };
            let placement = crate::placement::plan_target_placement(
                &realm.config,
                &self.target(),
                Default::default(),
            )?
            .ok_or(ReadPolicyError::PlacementUnavailable)?
            .placement;
            read_holder_sets(&realm.config, &placement)?
                .into_iter()
                .filter(|holder| *holder != self.config.local_node_id)
                .take(MAX_POLICY_FETCH_HOLDERS)
                .collect()
        };
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
        document: PlacementPolicyDocument,
    ) -> Effects {
        if !asked.contains(&publisher) {
            return self.finish(Err(ReadPolicyError::UnexpectedPublisher));
        }
        let remaining: Vec<_> = asked
            .into_iter()
            .filter(|holder| *holder != publisher)
            .collect();
        self.decide(document, PolicySource::Fetched, remaining)
    }

    fn finish(
        &mut self,
        result: Result<(AuthenticPolicy, PolicySource), ReadPolicyError>,
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
    type Output = (AuthenticPolicy, PolicySource);
    type Error = ReadPolicyError;

    /// The realm view is read with the row: a local hit needs it to verify
    /// publication authority, and a miss needs it to resolve the holders.
    fn start(&mut self) -> Effects {
        self.state = ReadPolicyState::ReadLocal;
        let target = self.target();
        let config = DocumentSyncTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        let auth = DocumentSyncTarget::RealmAuthorization {
            realm_id: self.config.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (target.storage_keyspace().to_string(), target.storage_key()),
                (config.storage_keyspace().to_string(), config.storage_key()),
                (auth.storage_keyspace().to_string(), auth.storage_key()),
            ],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            ReadPolicyState::ReadLocal => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, policy_value), (_, config_value), (_, auth_value)] = values.as_slice()
                    else {
                        return self
                            .unexpected_event("policy row and realm view", format!("{values:?}"));
                    };
                    if let Err(error) = self.store_realm(config_value.clone(), auth_value.clone()) {
                        return self.finish(Err(error));
                    }
                    match policy_value.clone() {
                        Some(value) => match PlacementPolicyDocument::from_bytes(&value) {
                            Ok(document) => self.decide(document, PolicySource::Local, Vec::new()),
                            Err(error) => self.finish(Err(error.into())),
                        },
                        None => match self.plan_fetch() {
                            Ok(effects) => effects,
                            Err(error) => self.finish(Err(error)),
                        },
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.finish(Err(error.into())),
                other => self.unexpected_event("local policy read", format!("{other:?}")),
            },
            ReadPolicyState::Fetch { asked } => match event {
                Event::Net(NetEvent::PolicyFetch(fetched)) => match fetched {
                    PolicyFetchEvent::Fetched {
                        publisher,
                        document,
                    } => self.accept_fetched(asked, publisher, *document),
                    PolicyFetchEvent::NotFound => self.finish(Err(ReadPolicyError::NotFound {
                        policy_id: self.config.policy_ref.policy_id,
                    })),
                    PolicyFetchEvent::Unavailable(reason) => {
                        self.finish(Err(ReadPolicyError::Unavailable(reason)))
                    }
                },
                other => self.unexpected_event("policy fetch result", format!("{other:?}")),
            },
            ReadPolicyState::ReadGroupAuth { pending } => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    self.handle_group_auth(*pending, value)
                }
                Event::Storage(StorageEvent::Error { error }) => self.finish(Err(error.into())),
                other => self.unexpected_event("group authorization read", format!("{other:?}")),
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
                | ReadPolicyError::Authority(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement_policy::tests::{admin_user, realm_view, signed_document};
    use aruna_core::NodeId;
    use aruna_core::effects::NetEffect;
    use aruna_core::structs::{
        LabelMatch, PlacementPolicy, PlacementSelector, PolicyPublicationClaim, RealmNodeKind,
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

    /// The one batch the read starts with: policy row plus the realm view every
    /// publication is verified against. The config is passed in because each
    /// fixture mints its own strategy ids.
    fn opened(config: &RealmConfigDocument, policy_row: Option<Value>) -> Event {
        let (config_value, auth_value) = realm_view(config, admin_user(realm_id()));
        let key = ByteView::from(Vec::new());
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (key.clone(), policy_row),
                (key.clone(), Some(config_value)),
                (key, Some(auth_value)),
            ],
        })
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
        operation.start();
        let effects = operation.step(opened(&config(), None));
        let Some(Effect::Net(NetEffect::PolicyFetch(fetch))) = effects.first() else {
            panic!("expected a policy fetch, got {effects:?}");
        };
        let holders = fetch.holders.as_slice().to_vec();
        (operation, holders)
    }

    fn fetched(publisher: NodeId, document: PlacementPolicyDocument) -> Event {
        Event::Net(NetEvent::PolicyFetch(PolicyFetchEvent::Fetched {
            publisher,
            document: Box::new(document),
        }))
    }

    fn answered(publisher: NodeId, policy: &VerifiedPolicy) -> Event {
        fetched(publisher, signed_document(realm_id(), policy, 1))
    }

    #[test]
    fn fetches_from_holders() {
        // A non-holder resolves the rule's holders from the policy id alone, with
        // no catalog lookup, and only then asks them for the bytes.
        let policy = policy("eu-west");
        let config = config();
        let mut operation = operation(node(9), policy.policy_ref());
        operation.start();
        let effects = operation.step(opened(&config, None));

        let Some(Effect::Net(NetEffect::PolicyFetch(fetch))) = effects.first() else {
            panic!("expected a policy fetch, got {effects:?}");
        };
        assert_eq!(fetch.policy_ref, policy.policy_ref());
        let placement = config
            .policy_placement(policy.policy().policy_id)
            .expect("policy bucket resolves");
        let expected = crate::placement::read_holder_sets(&config, &placement).expect("holders");
        assert_eq!(fetch.holders.as_slice(), expected.as_slice());

        operation.step(answered(expected[0], &policy));
        let (authentic, source) = operation.finalize().expect("policy resolves");
        assert_eq!(authentic.policy, policy);
        assert_eq!(source, PolicySource::Fetched);
    }

    #[test]
    fn rejects_other_digest() {
        // A holder answering with another definition under the requested id must
        // never be accepted: identity is the id together with its digest.
        let requested = policy("eu-west");
        let mut operation = operation(node(9), requested.policy_ref());
        operation.start();
        operation.step(opened(&config(), Some(document(&policy("us-east")))));
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
        operation.step(opened(&config(), Some(document(&policy))));
        let (authentic, source) = operation.finalize().expect("policy resolves");
        assert_eq!(authentic.policy, policy);
        assert_eq!(source, PolicySource::Local);
    }

    #[test]
    fn rejects_foreign_realm() {
        // A row published for another realm is not this realm's rule, however
        // well it hashes.
        let policy = policy("eu-west");
        let mut operation = operation(node(1), policy.policy_ref());
        operation.start();
        operation.step(opened(&config(), Some(foreign_document(&policy))));
        assert_eq!(operation.finalize(), Err(ReadPolicyError::RealmMismatch));
    }

    #[test]
    fn rejects_forged_bytes() {
        // A holder that rewrites the publication onto a definition it prefers is
        // refused: the signature no longer binds this realm and digest.
        let requested = policy("eu-west");
        let mut forged = signed_document(realm_id(), &requested, 1);
        forged.publication.created_by = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id());
        let (mut operation, holders) = start_fetch(requested.policy_ref());
        for holder in &holders {
            operation.step(fetched(*holder, forged.clone()));
        }
        assert_eq!(
            operation.finalize(),
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
        for holder in &holders {
            operation.step(fetched(*holder, authored(&requested, 1, outsider)));
        }
        assert_eq!(
            operation.finalize(),
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
        for holder in &holders {
            operation.step(fetched(
                *holder,
                authored(&requested, 9, admin_user(realm_id())),
            ));
        }
        assert_eq!(
            operation.finalize(),
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

        let effects = operation.step(answered(holders[0], &policy("us-east")));
        let Some(Effect::Net(NetEffect::PolicyFetch(retry))) = effects.first() else {
            panic!("expected a retry against the remaining holders, got {effects:?}");
        };
        assert_eq!(retry.holders.as_slice(), &holders[1..]);

        operation.step(answered(holders[1], &requested));
        assert_eq!(
            operation.finalize().expect("policy resolves").0.policy,
            requested
        );
    }

    #[test]
    fn exhausts_bad_holders() {
        // With no honest holder left the read fails closed instead of accepting
        // the last answer it received.
        let requested = policy("eu-west");
        let (mut operation, holders) = start_fetch(requested.policy_ref());
        for holder in &holders {
            operation.step(answered(*holder, &policy("us-east")));
        }
        assert_eq!(operation.finalize(), Err(ReadPolicyError::DigestMismatch));
    }

    #[test]
    fn rejects_unasked_answer() {
        // An answer from a node this fetch never asked is not holder evidence.
        let requested = policy("eu-west");
        let (mut operation, _) = start_fetch(requested.policy_ref());
        operation.step(answered(node(9), &requested));
        assert_eq!(
            operation.finalize(),
            Err(ReadPolicyError::UnexpectedPublisher)
        );
    }

    #[test]
    fn requires_realm_view() {
        // Without the replicated authorization document nothing can be
        // authenticated, so the read reports unavailable instead of trusting it.
        let policy = policy("eu-west");
        let mut operation = operation(node(1), policy.policy_ref());
        operation.start();
        let key = ByteView::from(Vec::new());
        let (config_value, _) = realm_view(&config(), admin_user(realm_id()));
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (key.clone(), Some(document(&policy))),
                (key.clone(), Some(config_value)),
                (key, None),
            ],
        }));
        assert!(matches!(
            operation.finalize(),
            Err(ReadPolicyError::Unavailable(_))
        ));
    }
}
