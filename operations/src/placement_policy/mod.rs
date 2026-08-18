//! Placement-policy documents: creation on a holder, read by ref, and the
//! fetch transport a non-holder resolves through.
//!
//! Placement of the document and placement allowed by the policy are separate
//! facts. Nothing in this module derives one from the other: the document's
//! holders come from its policy id, while the subjects the policy admits come
//! from its selectors and are evaluated only by `evaluate_placement`.

pub mod cache;
pub mod create;
pub mod gate;
pub mod read;
pub mod resolve;
pub mod subject;
pub mod transport;

pub use cache::{PolicyCacheEntry, PolicyCacheError, PolicyCacheStats};
pub use create::{CreatePolicyConfig, CreatePolicyError, CreatePolicyOperation};
pub use gate::{
    GateContext, GatedBucket, PolicyGateConfig, PolicyGateError, PolicyGateOperation,
    PolicyGateOutcome, gate_decision, union_refs, write_gate,
};
pub use read::{
    AuthenticPolicy, PolicySource, ReadPolicyConfig, ReadPolicyError, ReadPolicyOperation,
};
pub use resolve::{ResolvePolicyConfig, ResolvePolicyOperation, ResolvedPolicy};
pub use subject::{
    SubjectScanConfig, SubjectScanError, SubjectScanMode, SubjectScanOperation, SubjectScanResult,
};
pub(crate) use transport::{fetch_policy, serve_local_policy, sign_publication};

/// What production wiring establishes before a governed write is possible: an
/// advertised subject and the policies this node has already resolved.
#[cfg(test)]
pub(crate) mod fixtures {
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{NODE_SUBJECT_KEYSPACE, PLACEMENT_POLICY_CACHE_KEYSPACE};
    use aruna_core::structs::{
        NODE_SUBJECT_KEY, NodeSubjectRecord, PlacementSubject, RealmId, VerifiedPolicy,
    };
    use aruna_core::types::NodeId;
    use std::collections::BTreeMap;

    use super::cache::{PolicyCacheEntry, cache_key};
    use crate::driver::DriverContext;

    pub fn subject(node_id: NodeId, location: &str) -> PlacementSubject {
        PlacementSubject {
            node_id,
            generation: 1,
            location: location.to_string(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        }
    }

    pub async fn seed_gate(
        context: &DriverContext,
        realm_id: RealmId,
        subject: PlacementSubject,
        policies: &[VerifiedPolicy],
    ) {
        let record = NodeSubjectRecord::seed(subject).expect("subject is valid");
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: NODE_SUBJECT_KEYSPACE.to_string(),
                key: NODE_SUBJECT_KEY.to_vec().into(),
                value: record.to_bytes().expect("record encodes").into(),
                txn_id: None,
            })
            .await;
        for policy in policies {
            let entry = PolicyCacheEntry::verified(realm_id, policy, 0);
            let _ = context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: PLACEMENT_POLICY_CACHE_KEYSPACE.to_string(),
                    key: cache_key(&policy.policy_ref()),
                    value: entry.to_bytes().expect("entry encodes").into(),
                    txn_id: None,
                })
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::NodeId;
    use aruna_core::structs::{
        Actor, DEFAULT_NODE_WEIGHT, NodePlacementEntry, Permission, PlacementDecision,
        PlacementPolicy, PlacementPolicyDocument, PlacementSelector, PlacementSubject,
        PolicyPublicationClaim, PolicyResolution, RealmAuthorizationDocument, RealmConfigDocument,
        RealmId, RealmNodeKind, Role, VerifiedPolicy, evaluate_placement,
    };
    use aruna_core::types::{UserId, Value};
    use std::collections::{BTreeMap, HashMap, HashSet};
    use ulid::Ulid;

    use crate::placement::resolve_shard_holders;

    /// The authorizing user every policy fixture publishes under.
    pub(crate) fn admin_user(realm_id: RealmId) -> UserId {
        UserId::local(Ulid::from_bytes([2u8; 16]), realm_id)
    }

    /// One authentic publication of `policy` by node `seed`, so fixtures carry
    /// provenance a verifier accepts instead of asserted fields.
    pub(crate) fn signed_document(
        realm_id: RealmId,
        policy: &VerifiedPolicy,
        seed: u8,
    ) -> PlacementPolicyDocument {
        let secret = iroh::SecretKey::from_bytes(&[seed; 32]);
        let publication = PolicyPublicationClaim::new(
            realm_id,
            policy,
            secret.public(),
            admin_user(realm_id),
            Ulid::from_bytes([5u8; 16]),
            7,
            [0u8; 32],
        )
        .sign(&secret);
        PlacementPolicyDocument::new(realm_id, policy, publication)
    }

    /// Realm authorization granting `user` the realm-configuration write every
    /// policy publication is verified against.
    pub(crate) fn realm_authorization(
        realm_id: RealmId,
        user: UserId,
    ) -> RealmAuthorizationDocument {
        let role = Role {
            role_id: Ulid::from_bytes([1u8; 16]),
            name: "realm_admin".to_string(),
            permissions: HashMap::from([(format!("/{realm_id}/admin/**"), Permission::WRITE)]),
            assigned_users: HashSet::from([user]),
        };
        RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([(role.role_id, role)]),
            operation_restrictions: HashMap::new(),
        }
    }

    /// Encoded realm config and authorization, the view a policy read verifies
    /// every publication against.
    pub(crate) fn realm_view(config: &RealmConfigDocument, user: UserId) -> (Value, Value) {
        let actor = Actor {
            node_id: node(1),
            user_id: user,
            realm_id: config.realm_id,
        };
        (
            Value::from(config.to_bytes(&actor).expect("config encodes")),
            Value::from(
                realm_authorization(config.realm_id, user)
                    .to_bytes(&actor)
                    .expect("authorization encodes"),
            ),
        )
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    /// Four server nodes split across two locations, so a bounded holder set is
    /// a strict subset of the realm.
    fn config() -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([3u8; 32]), Vec::new(), 2);
        config.seed_default_placement();
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
            config.placement_map.push(NodePlacementEntry {
                node_id: node(seed),
                location: if seed <= 2 { "eu-west" } else { "us-east" }.to_string(),
                weight: DEFAULT_NODE_WEIGHT,
                full: false,
                draining: false,
                labels: BTreeMap::new(),
            });
        }
        config
    }

    fn policy(policy_id: Ulid, allowed: Vec<PlacementSelector>) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(policy_id, "residency".to_string(), allowed)
            .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn exact_node(node_id: NodeId) -> PlacementSelector {
        PlacementSelector {
            node_id: Some(node_id),
            location: None,
            labels: Vec::new(),
            executor_kind: None,
        }
    }

    fn subject(node_id: NodeId) -> PlacementSubject {
        PlacementSubject {
            node_id,
            generation: 1,
            location: "eu-west".to_string(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        }
    }

    fn decision(policy: &VerifiedPolicy, node_id: NodeId) -> PlacementDecision {
        let resolved = BTreeMap::from([(
            policy.policy().policy_id,
            PolicyResolution::Known(policy.clone()),
        )]);
        evaluate_placement(&[policy.policy_ref()], &resolved, &subject(node_id))
    }

    #[test]
    fn holder_is_denied() {
        // Holding the document answers "where can I obtain the rule?"; the
        // selectors answer "where may governed data live?".
        let config = config();
        let policy_id = Ulid::from_bytes([8u8; 16]);
        let placement = config
            .policy_placement(policy_id)
            .expect("policy bucket resolves");
        let holders = resolve_shard_holders(&config, &placement);
        assert!(!holders.is_empty(), "the policy document must have holders");

        let outsider = (1..=4u8)
            .map(node)
            .find(|candidate| !holders.contains(candidate))
            .expect("a bounded holder set leaves a non-holder");
        // The rule admits exactly one node, and that node holds no copy of it.
        let policy = policy(policy_id, vec![exact_node(outsider)]);
        assert_eq!(decision(&policy, outsider), PlacementDecision::Allowed);
        for holder in &holders {
            assert_eq!(
                decision(&policy, *holder),
                PlacementDecision::Denied {
                    policy_ids: vec![policy_id]
                },
                "holding the document must not admit the holder as a data site"
            );
        }
    }

    #[test]
    fn selectors_never_place() {
        // Changing what a rule allows never moves the document, and moving the
        // document never changes what the rule allows.
        let config = config();
        let policy_id = Ulid::from_bytes([8u8; 16]);

        let wide = policy(
            policy_id,
            vec![
                exact_node(node(1)),
                exact_node(node(2)),
                exact_node(node(3)),
            ],
        );
        let narrow = policy(policy_id, vec![exact_node(node(4))]);
        assert_eq!(
            config.policy_placement(wide.policy().policy_id),
            config.policy_placement(narrow.policy().policy_id),
            "selectors are not part of the placement subject"
        );

        let other_id = Ulid::from_bytes([9u8; 16]);
        let moved = policy(other_id, narrow.policy().allowed.clone());
        assert_ne!(
            config.policy_placement(policy_id),
            config.policy_placement(other_id),
            "the id alone chooses the bucket"
        );
        assert_eq!(
            decision(&moved, node(4)),
            decision(&narrow, node(4)),
            "the same selectors admit the same subject wherever the rule lives"
        );
    }
}
