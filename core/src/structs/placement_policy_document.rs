//! The immutable placed document carrying one [`PlacementPolicy`] to its holders.
//! The holder set answers where the rule is obtained; the policy's own selectors
//! answer where governed data may live. Neither set is derived from the other.

use crate::NodeId;
use crate::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncRevision, DocumentSyncTarget,
};
use crate::errors::ConversionError;
use crate::permission_path::compile_permission_matcher;
use crate::structs::{
    GroupAuthorizationDocument, Permission, PlacementPolicy, PlacementPolicyError,
    PlacementPolicyRef, PlacementRef, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    Role, VerifiedPolicy,
};
use crate::types::{GroupId, UserId};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

const POLICY_PUBLICATION_DOMAIN: &[u8] = b"aruna-placement-policy-publication-v1";

/// A policy id names one immutable definition, so its row has exactly one
/// generation and only its provenance can still be folded in.
const POLICY_DOCUMENT_GENERATION: u64 = 1;

/// Why a publication is not authentic realm-admin provenance. Every variant is
/// fail-closed: an unauthenticated rule is never reinterpreted as a grant.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum PolicyAuthorityError {
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error("publication signature does not bind this realm and definition")]
    Signature,
    #[error("original publisher is not a permitted realm node")]
    Publisher,
    #[error("authorizing user does not hold realm configuration write")]
    Unauthorized,
    #[error("realm view belongs to another realm")]
    RealmMismatch,
    /// The owning group's authorization state is missing or names another
    /// group, so group-admin authority cannot be decided here.
    #[error("owning group's authorization state is unavailable")]
    GroupUnavailable,
}

/// The tuple one policy publication signs. Reconstructed from the document, so a
/// publication cannot be replayed onto another realm, policy id, or definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyPublicationClaim {
    pub realm_id: RealmId,
    pub policy_ref: PlacementPolicyRef,
    /// Node that published the definition and checked the authorization below.
    pub publisher: NodeId,
    /// User whose realm-admin permission the publisher checked.
    pub created_by: UserId,
    pub created_at_ms: u64,
    pub event_id: Ulid,
    /// Realm-configuration digest the admin check ran against: the membership
    /// snapshot of the publication epoch, retained as audit evidence.
    pub config_digest: [u8; 32],
}

impl PolicyPublicationClaim {
    pub fn new(
        realm_id: RealmId,
        policy: &VerifiedPolicy,
        publisher: NodeId,
        created_by: UserId,
        event_id: Ulid,
        created_at_ms: u64,
        config_digest: [u8; 32],
    ) -> Self {
        Self {
            realm_id,
            policy_ref: policy.policy_ref(),
            publisher,
            created_by,
            created_at_ms,
            event_id,
            config_digest,
        }
    }

    pub fn signing_bytes(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(POLICY_PUBLICATION_DOMAIN);
        hasher.update(&postcard::to_allocvec(self).expect("publication claim serializes"));
        *hasher.finalize().as_bytes()
    }

    pub fn sign(&self, secret: &iroh::SecretKey) -> PolicyPublication {
        self.signed_with(|message| secret.sign(message))
    }

    /// Signs through the node's own signer, for publishers that hold a handle
    /// rather than the key itself.
    pub fn signed_with(&self, sign: impl FnOnce(&[u8]) -> iroh::Signature) -> PolicyPublication {
        PolicyPublication {
            publisher: self.publisher,
            created_by: self.created_by,
            created_at_ms: self.created_at_ms,
            event_id: self.event_id,
            config_digest: self.config_digest,
            signature: sign(&self.signing_bytes()),
        }
    }
}

/// Token-free evidence that one node published this definition after checking
/// the authorizing user's realm-admin permission. A relay that restates the
/// document cannot become its author: the signature names the original
/// publisher and covers the realm, policy id, and digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyPublication {
    pub publisher: NodeId,
    pub created_by: UserId,
    pub created_at_ms: u64,
    pub event_id: Ulid,
    pub config_digest: [u8; 32],
    pub signature: iroh::Signature,
}

impl PolicyPublication {
    pub fn claim(
        &self,
        realm_id: RealmId,
        policy_ref: PlacementPolicyRef,
    ) -> PolicyPublicationClaim {
        PolicyPublicationClaim {
            realm_id,
            policy_ref,
            publisher: self.publisher,
            created_by: self.created_by,
            created_at_ms: self.created_at_ms,
            event_id: self.event_id,
            config_digest: self.config_digest,
        }
    }

    /// Canonical order over publications of one definition. Two authorized
    /// publications of byte-identical bytes converge on the smaller digest, so
    /// an unauthenticated timestamp can never take provenance from the other.
    pub fn claim_digest(&self, realm_id: RealmId, policy_ref: PlacementPolicyRef) -> [u8; 32] {
        self.claim(realm_id, policy_ref).signing_bytes()
    }

    pub fn verify(
        &self,
        realm_id: RealmId,
        policy_ref: PlacementPolicyRef,
    ) -> Result<(), PolicyAuthorityError> {
        let claim = self.claim(realm_id, policy_ref);
        self.publisher
            .verify(&claim.signing_bytes(), &self.signature)
            .map_err(|_| PolicyAuthorityError::Signature)
    }
}

/// Permission path a realm-wide policy publication is authorized against. The
/// creating operation and every verifier read this one path, so they cannot
/// drift.
pub fn policy_admin_path(realm_id: RealmId) -> String {
    format!("/{realm_id}/admin/config")
}

/// Permission path a group-owned policy is authorized against: administering
/// the owning group, never the realm configuration.
pub fn group_admin_path(realm_id: RealmId, group_id: GroupId) -> String {
    format!("/{realm_id}/g/{group_id}/admin")
}

/// The path one policy's publication has to be authorized against.
pub fn policy_authority_path(realm_id: RealmId, owner_group_id: Option<GroupId>) -> String {
    match owner_group_id {
        Some(group_id) => group_admin_path(realm_id, group_id),
        None => policy_admin_path(realm_id),
    }
}

/// Publication authority behind one document: the original publisher must be a
/// permitted realm node, and the authorizing user must still hold write on the
/// path the rule's ownership names, in the verifier's own replicated view. A
/// realm-wide rule needs realm-configuration write; a group-owned rule needs
/// admin write on its owning group, decided over the realm and group roles
/// together, exactly as the creating operation decided it. Neither a relay nor
/// a current holder can supply that authority for someone else, and a group
/// whose authorization state is missing fails closed.
pub fn verify_policy_authority(
    document: &PlacementPolicyDocument,
    config: &RealmConfigDocument,
    auth: &RealmAuthorizationDocument,
    group_auth: Option<&GroupAuthorizationDocument>,
) -> Result<(), PolicyAuthorityError> {
    if config.realm_id != document.realm_id || auth.realm_id != document.realm_id {
        return Err(PolicyAuthorityError::RealmMismatch);
    }
    document
        .publication
        .verify(document.realm_id, document.policy_ref()?)?;
    let publisher = document.publication.publisher;
    if !config
        .sync_eligible_node_ids()
        .is_ok_and(|eligible| eligible.contains(&publisher))
    {
        return Err(PolicyAuthorityError::Publisher);
    }
    let created_by = document.publication.created_by;
    if created_by.is_nil() || created_by.realm_id != document.realm_id {
        return Err(PolicyAuthorityError::Unauthorized);
    }
    let Some(group_id) = document.policy.owner_group_id else {
        return holds_admin_write(
            created_by,
            &policy_admin_path(document.realm_id),
            auth.roles.values(),
        )
        .then_some(())
        .ok_or(PolicyAuthorityError::Unauthorized);
    };
    let Some(group_auth) = group_auth.filter(|group| group.group_id == group_id) else {
        return Err(PolicyAuthorityError::GroupUnavailable);
    };
    holds_admin_write(
        created_by,
        &group_admin_path(document.realm_id, group_id),
        auth.roles.values().chain(group_auth.roles.values()),
    )
    .then_some(())
    .ok_or(PolicyAuthorityError::Unauthorized)
}

/// Whether the given roles grant this user write on the path. An uncompilable
/// pattern denies, so a malformed role never widens authority.
fn holds_admin_write<'a>(
    user_id: UserId,
    path: &str,
    roles: impl Iterator<Item = &'a Role>,
) -> bool {
    let mut allowed = false;
    for role in roles {
        if !role.assigned_users.contains(&user_id) {
            continue;
        }
        for (pattern, permission) in &role.permissions {
            let Ok(glob) = compile_permission_matcher(pattern) else {
                return false;
            };
            if !glob.is_match(path) {
                continue;
            }
            match permission {
                Permission::DENY => return false,
                Permission::WRITE => allowed = true,
                Permission::READ => {}
            }
        }
    }
    allowed
}

/// STATE-PLACEMENT-POLICY: one immutable residency rule replicated to the holders
/// its policy id resolves to. Identity is `(policy_id, digest)`, so a known id with
/// different bytes is refused as reuse; the publication proves who published it
/// under whose realm-admin authority and stays outside the digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementPolicyDocument {
    pub realm_id: RealmId,
    pub policy: PlacementPolicy,
    pub publication: PolicyPublication,
}

impl PlacementPolicyDocument {
    /// Only a verified policy is publishable, so a holder never stores bytes
    /// that could not be matched against a subject.
    pub fn new(realm_id: RealmId, policy: &VerifiedPolicy, publication: PolicyPublication) -> Self {
        Self {
            realm_id,
            policy: policy.policy().clone(),
            publication,
        }
    }

    pub fn policy_id(&self) -> Ulid {
        self.policy.policy_id
    }

    /// Boundary re-check: decoded or replicated bytes are a candidate until they
    /// pass verification again.
    pub fn verified(&self) -> Result<VerifiedPolicy, PlacementPolicyError> {
        VerifiedPolicy::verify(self.policy.clone())
    }

    pub fn policy_ref(&self) -> Result<PlacementPolicyRef, PlacementPolicyError> {
        Ok(self.verified()?.policy_ref())
    }

    /// Signature check alone: the publication binds this realm and definition.
    /// Membership and realm-admin authority need a realm view and live in
    /// [`verify_policy_authority`].
    pub fn verify_publication(&self) -> Result<(), PolicyAuthorityError> {
        self.publication.verify(self.realm_id, self.policy_ref()?)
    }

    /// Whether both documents define the same policy id with the same canonical
    /// bytes. Provenance is deliberately excluded.
    pub fn same_definition(&self, other: &Self) -> bool {
        self.policy.policy_id == other.policy.policy_id
            && self.policy.canonical_bytes() == other.policy.canonical_bytes()
    }

    /// Folds a replicated publication into the local document. The caller
    /// verifies realm-admin authority first; here a different definition under a
    /// known id fails closed and the smaller claim digest is canonical.
    pub fn merge(&mut self, incoming: &Self) -> Result<bool, PolicyAuthorityError> {
        if self.policy.policy_id != incoming.policy.policy_id {
            return Ok(false);
        }
        if !self.same_definition(incoming) {
            return Err(PlacementPolicyError::PolicyIdReuse {
                policy_id: self.policy.policy_id,
            }
            .into());
        }
        let policy_ref = self.policy_ref()?;
        incoming.publication.verify(self.realm_id, policy_ref)?;
        if incoming.publication.claim_digest(self.realm_id, policy_ref)
            < self.publication.claim_digest(self.realm_id, policy_ref)
        {
            *self = incoming.clone();
            return Ok(true);
        }
        Ok(false)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Document key: the policy id alone, so a replay resolves the same row.
pub fn placement_policy_key(policy_id: Ulid) -> Vec<u8> {
    policy_id.to_bytes().to_vec()
}

pub fn placement_policy_target(policy_id: Ulid) -> DocumentSyncTarget {
    DocumentSyncTarget::PlacementPolicy { policy_id }
}

/// Sync change a policy row publishes and records. Derived purely from the row,
/// so two holders of one document write byte-identical manifest entries. The
/// definition is immutable, so the generation is fixed: a provenance merge must
/// not look like a regression to a peer that already holds the rule.
pub fn placement_policy_change(
    document: &PlacementPolicyDocument,
    placement: PlacementRef,
) -> DocumentSyncChange {
    DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: POLICY_DOCUMENT_GENERATION,
            event_id: document.publication.event_id,
            actor: document.publication.publisher,
            updated_at_ms: document.publication.created_at_ms,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{LabelMatch, PlacementSelector, RealmNodeKind, Role};
    use std::collections::{HashMap, HashSet};

    fn secret(seed: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[seed; 32])
    }

    fn node(seed: u8) -> NodeId {
        secret(seed).public()
    }

    fn realm() -> RealmId {
        RealmId([9; 32])
    }

    fn admin() -> UserId {
        UserId::local(Ulid::from_bytes([2; 16]), realm())
    }

    fn group() -> Ulid {
        Ulid::from_bytes([3; 16])
    }

    /// The same rule bound to `group()`, so only that group's admins publish it.
    fn owned_policy(seed: u8) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            "eu-only".to_string(),
            vec![PlacementSelector {
                node_id: None,
                location: Some("eu-west".to_string()),
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid")
        .owned_by(group())
        .expect("owner is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn group_roles(user: UserId, pattern: &str) -> crate::structs::GroupAuthorizationDocument {
        let role = Role {
            role_id: Ulid::from_bytes([2; 16]),
            name: "group_admin".to_string(),
            permissions: HashMap::from([(pattern.to_string(), Permission::WRITE)]),
            assigned_users: HashSet::from([user]),
        };
        crate::structs::GroupAuthorizationDocument {
            group_id: group(),
            roles: HashMap::from([(role.role_id, role)]),
            policies: Vec::new(),
        }
    }

    fn owned_document(seed: u8) -> PlacementPolicyDocument {
        let policy = owned_policy(seed);
        let publication = publication(seed, &policy, 10);
        PlacementPolicyDocument::new(realm(), &policy, publication)
    }

    /// A realm role that grants nothing outside the realm admin namespace.
    fn narrow_realm() -> RealmAuthorizationDocument {
        RealmAuthorizationDocument {
            realm_id: realm(),
            roles: HashMap::new(),
            operation_restrictions: HashMap::new(),
        }
    }

    #[test]
    fn owned_needs_group_admin() {
        // A group-owned rule is decided against its owner's roles: a realm-config
        // admin alone does not publish it, and its own admin does.
        let document = owned_document(1);
        let member = UserId::local(Ulid::from_bytes([9; 16]), realm());
        assert_eq!(
            verify_policy_authority(
                &document,
                &config(),
                &narrow_realm(),
                Some(&group_roles(
                    admin(),
                    &format!("/{}/g/{}/**", realm(), group())
                )),
            ),
            Ok(())
        );
        assert_eq!(
            verify_policy_authority(
                &document,
                &config(),
                &narrow_realm(),
                Some(&group_roles(
                    member,
                    &format!("/{}/g/{}/**", realm(), group())
                )),
            ),
            Err(PolicyAuthorityError::Unauthorized),
            "the signing admin of another user's group holds no authority here"
        );
    }

    #[test]
    fn owned_fails_closed() {
        // Without the owner's roles nothing is decided, and a document from a
        // foreign group's state is never accepted either.
        let document = owned_document(1);
        assert_eq!(
            verify_policy_authority(&document, &config(), &authorization(admin()), None),
            Err(PolicyAuthorityError::GroupUnavailable)
        );
        let mut foreign = group_roles(admin(), &format!("/{}/g/{}/**", realm(), group()));
        foreign.group_id = Ulid::from_bytes([7; 16]);
        assert_eq!(
            verify_policy_authority(
                &document,
                &config(),
                &authorization(admin()),
                Some(&foreign)
            ),
            Err(PolicyAuthorityError::GroupUnavailable)
        );
    }

    #[test]
    fn owner_changes_digest() {
        // The owner is inside the canonical bytes, so binding a rule to a group
        // mints a different reference and cannot be swapped in silently.
        let realm_wide = policy(1, "eu-west");
        let owned = owned_policy(1);
        assert_ne!(realm_wide.policy_ref(), owned.policy_ref());
        assert_eq!(realm_wide.policy().policy_id, owned.policy().policy_id);
    }

    /// Pinned so a canonical-bytes or digest-domain change is deliberate.
    #[test]
    fn digest_domain_pinned() {
        assert_eq!(
            hex::encode(policy(1, "eu-west").digest()),
            "a41f027aea08b68d469068dea97ae7aaba74726a2c37ba5e826287b6eaf51365"
        );
        assert_eq!(
            hex::encode(owned_policy(1).digest()),
            "f60af76d1feed5c3ccc30cc6be00c31a9cead0dfeada81427cb2c5b4b2a773d5"
        );
    }

    fn policy(seed: u8, location: &str) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
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

    /// One publication of `policy` signed by node `seed` for the admin user.
    fn publication(seed: u8, policy: &VerifiedPolicy, created_at_ms: u64) -> PolicyPublication {
        PolicyPublicationClaim::new(
            realm(),
            policy,
            node(seed),
            admin(),
            Ulid::from_bytes([seed; 16]),
            created_at_ms,
            [7; 32],
        )
        .sign(&secret(seed))
    }

    fn document(seed: u8, location: &str, created_at_ms: u64) -> PlacementPolicyDocument {
        let policy = policy(seed, location);
        let publication = publication(seed, &policy, created_at_ms);
        PlacementPolicyDocument::new(realm(), &policy, publication)
    }

    fn config() -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
        config.seed_default_placement();
        config.ensure_node(node(1), RealmNodeKind::Server);
        config.ensure_node(node(2), RealmNodeKind::Server);
        config.ensure_node(
            node(3),
            RealmNodeKind::User {
                owner: UserId::nil(realm()),
            },
        );
        config
    }

    fn authorization(user: UserId) -> RealmAuthorizationDocument {
        let role = Role {
            role_id: Ulid::from_bytes([1; 16]),
            name: "realm_admin".to_string(),
            permissions: HashMap::from([(format!("/{}/admin/**", realm()), Permission::WRITE)]),
            assigned_users: HashSet::from([user]),
        };
        RealmAuthorizationDocument {
            realm_id: realm(),
            roles: HashMap::from([(role.role_id, role)]),
            operation_restrictions: HashMap::new(),
        }
    }

    #[test]
    fn document_roundtrips() {
        let document = document(1, "eu-west", 10);
        assert_eq!(
            PlacementPolicyDocument::from_bytes(&document.to_bytes().unwrap()).unwrap(),
            document
        );
        assert_eq!(
            document.policy_ref().unwrap(),
            document.verified().unwrap().policy_ref()
        );
        assert_eq!(document.verify_publication(), Ok(()));
    }

    #[test]
    fn rejects_forged_origin() {
        // A relay that restates the document as its own is not its author, and
        // a rewritten authorizing user breaks the same signature.
        let document = document(1, "eu-west", 10);
        let mut relayed = document.clone();
        relayed.publication.publisher = node(2);
        assert_eq!(
            relayed.verify_publication(),
            Err(PolicyAuthorityError::Signature)
        );

        let mut renamed = document.clone();
        renamed.publication.created_by = UserId::local(Ulid::from_bytes([6; 16]), realm());
        assert_eq!(
            renamed.verify_publication(),
            Err(PolicyAuthorityError::Signature)
        );
    }

    #[test]
    fn rejects_realm_replay() {
        // The same signed bytes restated for another realm bind nothing.
        let mut replayed = document(1, "eu-west", 10);
        replayed.realm_id = RealmId([4; 32]);
        assert_eq!(
            replayed.verify_publication(),
            Err(PolicyAuthorityError::Signature)
        );
        assert_eq!(
            verify_policy_authority(&replayed, &config(), &authorization(admin()), None),
            Err(PolicyAuthorityError::RealmMismatch)
        );
    }

    #[test]
    fn requires_admin_author() {
        let document = document(1, "eu-west", 10);
        assert_eq!(
            verify_policy_authority(&document, &config(), &authorization(admin()), None),
            Ok(())
        );

        let outsider = UserId::local(Ulid::from_bytes([5; 16]), realm());
        assert_eq!(
            verify_policy_authority(&document, &config(), &authorization(outsider), None),
            Err(PolicyAuthorityError::Unauthorized),
            "a validly signed policy authored by a non-admin must be refused"
        );
    }

    #[test]
    fn requires_permitted_publisher() {
        // A user-kind node and an unknown node are never permitted publishers.
        let policy = policy(1, "eu-west");
        for seed in [3u8, 8u8] {
            let document =
                PlacementPolicyDocument::new(realm(), &policy, publication(seed, &policy, 10));
            assert_eq!(
                verify_policy_authority(&document, &config(), &authorization(admin()), None),
                Err(PolicyAuthorityError::Publisher)
            );
        }
    }

    #[test]
    fn merge_rejects_reuse() {
        let mut local = document(1, "eu-west", 10);
        let reused = document(1, "us-east", 10);
        assert_eq!(
            local.merge(&reused),
            Err(PolicyAuthorityError::Policy(
                PlacementPolicyError::PolicyIdReuse {
                    policy_id: Ulid::from_bytes([1; 16])
                }
            ))
        );
        assert_eq!(local, document(1, "eu-west", 10));
    }

    #[test]
    fn merge_keeps_authentic() {
        // Provenance follows the canonical claim digest, never the timestamp, so
        // a backdated republication cannot take authorship of the same bytes.
        let policy = policy(1, "eu-west");
        let first = PlacementPolicyDocument::new(realm(), &policy, publication(1, &policy, 20));
        let second = PlacementPolicyDocument::new(realm(), &policy, publication(2, &policy, 5));
        let policy_ref = policy.policy_ref();
        let winner = if second.publication.claim_digest(realm(), policy_ref)
            < first.publication.claim_digest(realm(), policy_ref)
        {
            second.clone()
        } else {
            first.clone()
        };

        let mut left = first.clone();
        left.merge(&second)
            .expect("both publications are authentic");
        let mut right = second.clone();
        right
            .merge(&first)
            .expect("both publications are authentic");
        assert_eq!(left, winner);
        assert_eq!(right, winner);
    }

    #[test]
    fn merge_rejects_unsigned() {
        // An incoming document whose publication does not verify never replaces
        // provenance, however small its claim digest.
        let policy = policy(1, "eu-west");
        let mut local = PlacementPolicyDocument::new(realm(), &policy, publication(1, &policy, 20));
        let mut forged = local.clone();
        forged.publication.created_at_ms = 1;
        assert_eq!(local.merge(&forged), Err(PolicyAuthorityError::Signature));
        assert_eq!(local.publication.created_at_ms, 20);
    }

    #[test]
    fn merge_ignores_others() {
        let mut local = document(1, "eu-west", 10);
        assert!(!local.merge(&document(2, "us-east", 1)).unwrap());
        assert_eq!(local, document(1, "eu-west", 10));
    }

    #[test]
    fn change_follows_row() {
        // The generation is fixed: folding in an earlier publication changes the
        // provenance a holder records, never the revision peers order by.
        let placement = PlacementRef {
            strategy_id: Ulid::from_bytes([3; 16]),
            shard: 4,
        };
        let change = placement_policy_change(&document(7, "eu-west", 42), placement);
        let earlier = placement_policy_change(&document(7, "eu-west", 1), placement);

        assert_eq!(change.current.generation, POLICY_DOCUMENT_GENERATION);
        assert_eq!(earlier.current.generation, change.current.generation);
        assert_eq!(change.current.event_id, Ulid::from_bytes([7; 16]));
        assert_eq!(change.current.actor, node(7));
        assert_eq!(change.kind, DocumentSyncChangeKind::Upsert);
        assert_eq!(change.placement, placement);
    }
}
