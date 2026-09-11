//! Placement-policy documents: creation, read by ref, and fetch transport.
//! Document and selector placement are separate: holders come from the policy
//! id, while admitted subjects come from selectors via `evaluate_placement`.

pub mod cache;
pub mod create;
pub mod diagnostics;
pub mod forward;
pub mod gate;
pub mod list;
pub mod names;
pub mod quarantine;
pub mod read;
pub mod resolve;
pub mod resolve_set;
pub mod subject;
pub mod transport;

pub use cache::{PolicyCacheEntry, PolicyCacheError, PolicyCacheStats};
pub use create::{CreatePolicyConfig, CreatePolicyError, CreatePolicyOperation};
pub use diagnostics::{
    CacheCoverage, CopyViolation, DiagnosticsInput, DiagnosticsReport, PolicyDiagnosticsOperation,
};
pub(crate) use forward::apply_forwarded_policy;
pub use forward::{PolicyForwardError, create_policy_routed};
pub use gate::{
    GateContext, GatedBucket, PolicyGateConfig, PolicyGateError, PolicyGateOperation,
    PolicyGateOutcome, drift_reads, gate_decision, split_drift_reads, union_refs, write_gate,
};
pub use list::{
    ListPoliciesError, ListPoliciesInput, ListPoliciesOperation, POLICY_LIST_DEFAULT,
    POLICY_LIST_LIMIT, PolicyListPage,
};
pub use names::{MAX_NAMED_REFS, PolicyName, PolicyNamesError, PolicyNamesOperation};
pub use quarantine::{
    QuarantineError, QuarantineResolution, ResolveQuarantineConfig, ResolveQuarantineOperation,
};
pub use read::{
    AuthenticPolicy, PolicySource, ReadPolicyConfig, ReadPolicyError, ReadPolicyOperation,
};
pub use resolve::{ResolvePolicyConfig, ResolvePolicyOperation, ResolvedPolicy};
pub use resolve_set::{PolicySetResolver, ResolveMode, ResolveStep};
pub use subject::{
    SubjectScanConfig, SubjectScanError, SubjectScanMode, SubjectScanOperation, SubjectScanResult,
    observe_placement, sync_subject,
};
pub(crate) use transport::{fetch_policy, serve_local_policy, sign_publication};

use aruna_core::structs::PolicyResolution;
use aruna_core::types::GroupId;
use std::collections::BTreeMap;
use ulid::Ulid;

/// The first resolved rule another group owns. A group-owned rule governs only
/// its owner's own buckets, so a reference from elsewhere is refused whoever
/// attaches it; a rule that could not be resolved is decided by the gate.
pub fn foreign_owner(
    resolved: &BTreeMap<Ulid, PolicyResolution>,
    group_id: GroupId,
) -> Option<Ulid> {
    resolved
        .iter()
        .find_map(|(policy_id, resolution)| match resolution {
            PolicyResolution::Known(policy) => policy
                .policy()
                .owner_group_id
                .is_some_and(|owner| owner != group_id)
                .then_some(*policy_id),
            PolicyResolution::Unresolved => None,
        })
}

#[cfg(test)]
pub(crate) mod tests {
    use aruna_core::NodeId;
    use aruna_core::structs::{
        DEFAULT_NODE_WEIGHT, NodePlacementEntry, Permission, PlacementDecision, PlacementPolicy,
        PlacementSelector, PlacementSubject, PolicyResolution, RealmAuthorizationDocument,
        RealmConfigDocument, RealmId, RealmNodeKind, Role, VerifiedPolicy, evaluate_placement,
    };
    use aruna_core::types::UserId;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use ulid::Ulid;

    use crate::placement::resolve_shard_holders;

    pub(crate) mod fixtures;

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
