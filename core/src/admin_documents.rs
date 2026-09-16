use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::UserId;
use crate::structs::identity::auth::{Actor, Permission, Role};
use crate::structs::identity::realm::{
    MetadataReplicationConfig, OidcProviderConfig, QuotaConfig, RealmDiscoveryConfig, RealmId,
    RealmNodeKind,
};
use crate::structs::placement::compute_config::RealmComputeConfig;
use crate::structs::placement::record::{
    BandPool, BindingScope, HandleRange, NodePlacementEntry, PlacementBinding, PlacementOverride,
    PlacementRef, PlacementStrategy, StrategyBinding,
};
use crate::structs::placement::transition::{
    CandidatePlacementMap, CompletionProof, TransitionPlan,
};
use crate::types::{GroupId, RoleId};

/// Domain separator for the origin signature over an administrative event.
pub const DOCUMENT_EVENT_DOMAIN: &str = "aruna-admin-document-event-v1";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentClock {
    pub origins: BTreeMap<NodeId, u64>,
}

impl AdminDocumentClock {
    pub fn sequence_for(&self, origin_node_id: &NodeId) -> u64 {
        self.origins
            .get(origin_node_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn observes(&self, dot: &AdminDocumentDot) -> bool {
        self.sequence_for(&dot.origin_node_id) >= dot.origin_seq
    }

    pub fn advance(&mut self, origin_node_id: NodeId, origin_seq: u64) {
        let current = self.origins.entry(origin_node_id).or_default();
        *current = (*current).max(origin_seq);
    }

    pub fn with_observed(mut self, origin_node_id: NodeId, origin_seq: u64) -> Self {
        self.advance(origin_node_id, origin_seq);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AdminDocumentDot {
    pub event_id: Ulid,
    pub origin_node_id: NodeId,
    pub origin_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminDocumentTarget {
    Group { group_id: GroupId },
    Realm { realm_id: RealmId },
    User { user_id: UserId },
    RealmConfig { realm_id: RealmId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminRoleDefinition {
    pub role_id: RoleId,
    pub name: String,
    pub permissions: BTreeMap<String, Permission>,
}

impl From<&Role> for AdminRoleDefinition {
    fn from(role: &Role) -> Self {
        Self {
            role_id: role.role_id,
            name: role.name.clone(),
            permissions: role
                .permissions
                .iter()
                .map(|(path, permission)| (path.clone(), permission.clone()))
                .collect(),
        }
    }
}

impl From<Role> for AdminRoleDefinition {
    fn from(role: Role) -> Self {
        Self::from(&role)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminDocumentOperation {
    GroupRoleAdded {
        role_id: RoleId,
    },
    #[serde(rename = "GroupRoleUserAssignmentAdded")]
    GroupAssignmentAdded {
        role_id: RoleId,
        user_id: UserId,
    },
    #[serde(rename = "GroupRoleUserAssignmentRemoved")]
    GroupAssignmentRemoved {
        role_id: RoleId,
        user_id: UserId,
    },
    UserAttributeSet {
        key: String,
        value: String,
    },
    UserAttributeRemoved {
        key: String,
    },
    UserNameSet {
        name: String,
    },
    #[serde(rename = "UserSubjectIdAdded")]
    SubjectIdAdded {
        subject_id: String,
    },
    #[serde(rename = "UserSubjectIdRemoved")]
    SubjectIdRemoved {
        subject_id: String,
    },
    RealmRoleAdded {
        role_id: RoleId,
    },
    #[serde(rename = "RealmRoleUserAssignmentAdded")]
    RealmAssignmentAdded {
        role_id: RoleId,
        user_id: UserId,
    },
    #[serde(rename = "RealmRoleUserAssignmentRemoved")]
    RealmAssignmentRemoved {
        role_id: RoleId,
        user_id: UserId,
    },
    GroupRoleCreated {
        role: AdminRoleDefinition,
    },
    GroupRoleRemoved {
        role_id: RoleId,
    },
    RealmRoleCreated {
        role: AdminRoleDefinition,
    },
    #[serde(rename = "RealmConfigNodeEnsured")]
    ConfigNodeEnsured {
        node_id: NodeId,
        kind: RealmNodeKind,
    },
    #[serde(rename = "RealmConfigOidcProviderUpserted")]
    OidcProviderUpserted {
        provider: OidcProviderConfig,
    },
    #[serde(rename = "RealmConfigOidcProviderRemoved")]
    OidcProviderRemoved {
        provider_id: String,
    },
    #[serde(rename = "RealmConfigSettingsSet")]
    ConfigSettingsSet {
        metadata_replication: MetadataReplicationConfig,
        discovery: RealmDiscoveryConfig,
    },
    GroupCreated {
        realm_id: RealmId,
        display_name: String,
        owner: UserId,
    },
    #[serde(rename = "RealmConfigDescriptionSet")]
    ConfigDescriptionSet {
        description: String,
    },
    #[serde(rename = "RealmConfigQuotaSet")]
    ConfigQuotaSet {
        quota: QuotaConfig,
    },
    #[serde(rename = "RealmConfigNodePlacementSet")]
    NodePlacementSet {
        entry: NodePlacementEntry,
    },
    #[serde(rename = "RealmConfigNodePlacementRemoved")]
    NodePlacementRemoved {
        node_id: NodeId,
    },
    #[serde(rename = "RealmConfigPlacementStrategyUpserted")]
    PlacementStrategyUpserted {
        strategy: PlacementStrategy,
    },
    #[serde(rename = "RealmConfigPlacementStrategyRemoved")]
    PlacementStrategyRemoved {
        strategy_id: Ulid,
    },
    #[serde(rename = "RealmConfigDefaultStrategySet")]
    ConfigStrategySet {
        strategy_id: Ulid,
    },
    #[serde(rename = "RealmConfigStrategyBindingSet")]
    StrategyBindingSet {
        binding: StrategyBinding,
    },
    #[serde(rename = "RealmConfigStrategyBindingRemoved")]
    StrategyBindingRemoved {
        scope: BindingScope,
    },
    #[serde(rename = "RealmConfigPlacementOverrideSet")]
    PlacementOverrideSet {
        record: PlacementOverride,
    },
    #[serde(rename = "RealmConfigPlacementOverrideRemoved")]
    PlacementOverrideRemoved {
        subject: Vec<u8>,
    },
    /// Appends an immutable placement binding (append-only, no remove twin). As
    /// an admin operation it can never be relayed (K1): only Management/Server
    /// origins may emit it, and receivers converge it through the reducer.
    #[serde(rename = "RealmConfigPlacementBindingAppended")]
    PlacementBindingAppended {
        binding: PlacementBinding,
    },
    /// Grants an append-only handle range. Only Management may emit it;
    /// overlapping grants fail closed in the derived directory.
    #[serde(rename = "RealmConfigHandleRangeGranted")]
    HandleRangeGranted {
        range: HandleRange,
    },
    /// Assigns an append-only coordinator band pool. Pools form a causal
    /// delegation tree resolved by lineage, not by assignment order.
    #[serde(rename = "RealmConfigBandPoolAssigned")]
    BandPoolAssigned {
        pool: BandPool,
    },
    /// Publishes an immutable candidate map. Two divergent maps at one epoch
    /// conflict and leave the epoch unusable.
    #[serde(rename = "RealmConfigCandidateMapPublished")]
    CandidateMapPublished {
        map: CandidatePlacementMap,
    },
    /// Activates a published map for every bucket of a strategy that has no
    /// activation yet. Explicit, so nothing initializes as a create side effect.
    #[serde(rename = "RealmConfigActivationsInitialized")]
    ConfigActivationsInitialized {
        strategy_id: Ulid,
        candidate_map_epoch: u64,
    },
    /// Starts a proof-gated handoff of a strategy's buckets to a target map.
    #[serde(rename = "RealmConfigTransitionStarted")]
    ConfigTransitionStarted {
        plan: TransitionPlan,
    },
    /// An old holder's frozen frontier for one bucket.
    #[serde(rename = "RealmConfigTransitionBarrierReported")]
    TransitionBarrierReported {
        transition_id: Ulid,
        bucket: u32,
        reported_by: NodeId,
        frontier: Vec<u8>,
    },
    /// A target holder's signed completion proof. Admitted only when the origin
    /// is the signing holder and the signature covers this exact tuple.
    #[serde(rename = "RealmConfigTransitionProofSubmitted")]
    TransitionProofSubmitted {
        transition_id: Ulid,
        strategy_id: Ulid,
        proof: CompletionProof,
    },
    #[serde(rename = "RealmConfigTransitionAborted")]
    ConfigTransitionAborted {
        transition_id: Ulid,
    },
    /// Cuts one bucket over without every proof; the reducer still requires at
    /// least one verified proof.
    #[serde(rename = "RealmConfigTransitionBucketForced")]
    TransitionBucketForced {
        transition_id: Ulid,
        bucket: u32,
        at_risk_report: String,
    },
    /// Diagnostics only: a stall never moves authority.
    #[serde(rename = "RealmConfigTransitionStallReported")]
    TransitionStallReported {
        transition_id: Ulid,
        bucket: u32,
        reported_by: NodeId,
        reason: String,
    },
    #[serde(rename = "RealmConfigPoliciesSet")]
    ConfigPoliciesSet {
        policies: Vec<crate::request_policy::RequestPolicy>,
    },
    /// Revokes one token hash until expiry. The trusted origin attests its owner;
    /// receivers recheck owner or admin authority without replicating the token.
    #[serde(rename = "RealmConfigTokenRevoked")]
    ConfigTokenRevoked {
        token_hash: String,
        expires_at: u64,
        token_owner: UserId,
    },
    GroupPoliciesSet {
        policies: Vec<crate::request_policy::RequestPolicy>,
    },
    /// A departing old holder's statement that its outbox holds nothing for
    /// the bucket any more, so its retention may end (postcard append-only:
    /// new variants only at the enum end).
    #[serde(rename = "RealmConfigTransitionDrainReported")]
    TransitionDrainReported {
        transition_id: Ulid,
        bucket: u32,
        reported_by: NodeId,
    },
    /// Stores the realm's submission-family placement strategy at creation. The
    /// reducer refuses a nil id and any later change, so family routing is
    /// immutable once one node has observed it.
    #[serde(rename = "RealmConfigJobFamilySet")]
    JobFamilySet {
        strategy_id: Ulid,
    },
    /// Replaces the realm's compute configuration wholesale: the directed
    /// location links the planner estimates transfers with, and the standing
    /// group compute quotas new admissions are decided against.
    #[serde(rename = "RealmConfigComputeSet")]
    ConfigComputeSet {
        compute: RealmComputeConfig,
    },
    /// Drops a node from realm membership. The node keeps its keys but stops
    /// being an admitted peer wherever the configuration replicates, so an
    /// evicted device loses its connections at the next membership refresh.
    #[serde(rename = "RealmConfigNodeRemoved")]
    ConfigNodeRemoved {
        node_id: NodeId,
    },
    /// Renames a group after creation. Only the label changes; the group id and
    /// every permission path stay as they are (postcard append-only: new
    /// variants only at the enum end).
    #[serde(rename = "GroupDisplayNameSet")]
    DisplayNameSet {
        display_name: String,
    },
    GroupJoinRequested {
        request: crate::join_request::JoinRequest,
    },
    GroupJoinDecided {
        decision: crate::join_request::JoinDecision,
    },
}

#[cfg(test)]
mod tests {
    use super::{AdminDocumentOperation, AdminDocumentTarget, AdminRoleDefinition};
    use crate::NodeId;
    use crate::UserId;
    use crate::structs::identity::auth::Permission;
    use crate::structs::identity::realm::{
        MetadataReplicationConfig, OidcProviderConfig, QuotaConfig, RealmDiscoveryConfig, RealmId,
        RealmNodeKind,
    };
    use crate::structs::placement::compute_config::RealmComputeConfig;
    use crate::structs::placement::record::{
        AffinityEffect, AffinityRule, BandPool, BindingScope, DocumentClass, HandleRange,
        LabelMatch, NodePlacementEntry, PlacementBinding, PlacementOverride, PlacementScope,
        PlacementStrategy, StrategyBinding,
    };
    use crate::structured_id::PlacementHandle;
    use crate::types::{GroupId, RoleId};
    use std::collections::BTreeMap;
    use ulid::Ulid;

    fn role_id(seed: u8) -> RoleId {
        Ulid::from_bytes([seed; 16])
    }

    fn group_id(seed: u8) -> GroupId {
        Ulid::from_bytes([seed; 16])
    }

    fn user_id(seed: u8) -> UserId {
        UserId::local(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([9; 32]))
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn role_definition(role_id: RoleId) -> AdminRoleDefinition {
        AdminRoleDefinition {
            role_id,
            name: "admin".to_string(),
            permissions: BTreeMap::from([("/dataset/**".to_string(), Permission::READ)]),
        }
    }

    fn request_policy(expression: &str) -> crate::request_policy::RequestPolicy {
        crate::request_policy::RequestPolicy {
            policy_id: Ulid::from_bytes([2; 16]),
            name: "test".to_string(),
            kind: crate::request_policy::PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }
    }

    fn oidc_provider(id: &str) -> OidcProviderConfig {
        OidcProviderConfig {
            id: id.to_string(),
            issuer: format!("https://issuer.example/{id}"),
            audience: "aruna".to_string(),
            discovery_url: format!("https://issuer.example/{id}/.well-known/openid-configuration"),
        }
    }

    fn postcard_roundtrip<T>(value: T) -> T
    where
        T: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        let bytes = postcard::to_allocvec(&value).expect("value serializes");
        postcard::from_bytes(&bytes).expect("value deserializes")
    }

    #[test]
    fn admin_document_roundtrip() {
        let role_id = role_id(1);
        let assigned_user_id = user_id(2);
        let realm_id = RealmId::from_bytes([9; 32]);
        let operations = vec![
            AdminDocumentOperation::GroupRoleAdded { role_id },
            AdminDocumentOperation::GroupAssignmentAdded {
                role_id,
                user_id: assigned_user_id,
            },
            AdminDocumentOperation::GroupAssignmentRemoved {
                role_id,
                user_id: assigned_user_id,
            },
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "biology".to_string(),
            },
            AdminDocumentOperation::UserAttributeRemoved {
                key: "department".to_string(),
            },
            AdminDocumentOperation::UserNameSet {
                name: "Alice".to_string(),
            },
            AdminDocumentOperation::SubjectIdAdded {
                subject_id: "subject-1".to_string(),
            },
            AdminDocumentOperation::SubjectIdRemoved {
                subject_id: "subject-1".to_string(),
            },
            AdminDocumentOperation::RealmRoleAdded { role_id },
            AdminDocumentOperation::RealmAssignmentAdded {
                role_id,
                user_id: assigned_user_id,
            },
            AdminDocumentOperation::RealmAssignmentRemoved {
                role_id,
                user_id: assigned_user_id,
            },
            AdminDocumentOperation::GroupRoleCreated {
                role: role_definition(role_id),
            },
            AdminDocumentOperation::GroupRoleRemoved { role_id },
            AdminDocumentOperation::RealmRoleCreated {
                role: role_definition(role_id),
            },
            AdminDocumentOperation::ConfigNodeEnsured {
                node_id: node(1),
                kind: RealmNodeKind::Management,
            },
            AdminDocumentOperation::ConfigNodeRemoved { node_id: node(1) },
            AdminDocumentOperation::OidcProviderUpserted {
                provider: oidc_provider("default"),
            },
            AdminDocumentOperation::OidcProviderRemoved {
                provider_id: "default".to_string(),
            },
            AdminDocumentOperation::ConfigSettingsSet {
                metadata_replication: MetadataReplicationConfig::new(3),
                discovery: RealmDiscoveryConfig::Static {
                    endpoints: Vec::new(),
                },
            },
            AdminDocumentOperation::GroupCreated {
                realm_id,
                display_name: "Engineering".to_string(),
                owner: user_id(3),
            },
            AdminDocumentOperation::ConfigDescriptionSet {
                description: "Demo Realm".to_string(),
            },
            AdminDocumentOperation::ConfigQuotaSet {
                quota: QuotaConfig::default(),
            },
            AdminDocumentOperation::ConfigComputeSet {
                compute: RealmComputeConfig::default(),
            },
            AdminDocumentOperation::NodePlacementSet {
                entry: placement_entry(node(1)),
            },
            AdminDocumentOperation::NodePlacementRemoved { node_id: node(1) },
            AdminDocumentOperation::PlacementStrategyUpserted {
                strategy: placement_strategy(Ulid::from_bytes([4; 16])),
            },
            AdminDocumentOperation::PlacementStrategyRemoved {
                strategy_id: Ulid::from_bytes([4; 16]),
            },
            AdminDocumentOperation::ConfigStrategySet {
                strategy_id: Ulid::from_bytes([4; 16]),
            },
            AdminDocumentOperation::StrategyBindingSet {
                binding: StrategyBinding {
                    scope: BindingScope::Class(DocumentClass::MetadataRegistry),
                    strategy_id: Ulid::from_bytes([4; 16]),
                },
            },
            AdminDocumentOperation::StrategyBindingRemoved {
                scope: BindingScope::Class(DocumentClass::MetadataRegistry),
            },
            AdminDocumentOperation::PlacementOverrideSet {
                record: placement_override(b"document-subject".to_vec()),
            },
            AdminDocumentOperation::PlacementOverrideRemoved {
                subject: b"document-subject".to_vec(),
            },
            AdminDocumentOperation::PlacementBindingAppended {
                binding: PlacementBinding {
                    handle: PlacementHandle::new(7).unwrap(),
                    scope: PlacementScope::Realm(realm_id),
                    document_class: DocumentClass::MetadataRegistry,
                    strategy_id: Ulid::from_bytes([4; 16]),
                    allocator_range_id: Some(Ulid::from_bytes([5; 16])),
                    allocated_by: Some(node(1)),
                    allocated_at_ms: Some(1_700_000_000_000),
                },
            },
            AdminDocumentOperation::HandleRangeGranted {
                range: HandleRange {
                    range_id: Ulid::from_bytes([6; 16]),
                    owner: node(1),
                    start: 1,
                    end: 1025,
                },
            },
            AdminDocumentOperation::BandPoolAssigned {
                pool: BandPool {
                    pool_id: Ulid::from_bytes([7; 16]),
                    parent: None,
                    issuer: node(1),
                    owner: node(1),
                    start: 3,
                    end: 1027,
                },
            },
            AdminDocumentOperation::ConfigPoliciesSet {
                policies: vec![request_policy("permission == 'write'")],
            },
            AdminDocumentOperation::ConfigTokenRevoked {
                token_hash: blake3::hash(b"bearer-token").to_string(),
                expires_at: 1_900_000_000,
                token_owner: user_id(8),
            },
            AdminDocumentOperation::GroupPoliciesSet {
                policies: vec![request_policy("anonymous")],
            },
        ];

        for op in operations {
            assert_eq!(postcard_roundtrip(op.clone()), op);
        }
    }

    fn placement_entry(node_id: NodeId) -> NodePlacementEntry {
        NodePlacementEntry {
            node_id,
            location: "eu-west".to_string(),
            weight: 100,
            full: false,
            draining: false,
            labels: BTreeMap::from([("tier".to_string(), "hot".to_string())]),
        }
    }

    fn placement_strategy(strategy_id: Ulid) -> PlacementStrategy {
        PlacementStrategy {
            strategy_id,
            name: "default".to_string(),
            replica_count: Some(3),
            distinct_locations: true,
            affinity: vec![AffinityRule {
                matcher: LabelMatch {
                    key: "tier".to_string(),
                    value: "hot".to_string(),
                },
                effect: AffinityEffect::Multiply { permille: 1500 },
            }],
            shard_count: 64,
        }
    }

    fn placement_override(subject: Vec<u8>) -> PlacementOverride {
        PlacementOverride {
            subject,
            pinned: vec![node(4)],
            excluded: vec![node(5)],
            strategy_id: Some(Ulid::from_bytes([4; 16])),
        }
    }

    #[test]
    fn admin_targets_roundtrip() {
        let realm_id = RealmId::from_bytes([9; 32]);
        let targets = [
            AdminDocumentTarget::Group {
                group_id: group_id(1),
            },
            AdminDocumentTarget::Realm { realm_id },
            AdminDocumentTarget::User {
                user_id: user_id(2),
            },
            AdminDocumentTarget::RealmConfig { realm_id },
        ];

        for target in targets {
            assert_eq!(postcard_roundtrip(target.clone()), target);
        }
    }

    #[test]
    fn signature_binds_envelope() {
        // The origin's signature covers the placement, actor and origin, so a
        // relay cannot move or rewrite the envelope it republishes.
        use crate::admin_documents::{AdminDocumentClock, AdminDocumentEvent};
        use crate::structs::identity::auth::Actor;
        use crate::structs::placement::record::PlacementRef;

        let realm_id = RealmId::from_bytes([9; 32]);
        let secret = iroh::SecretKey::from_bytes(&[11; 32]);
        let placement = PlacementRef {
            strategy_id: Ulid::from_bytes([3; 16]),
            shard: 5,
        };
        let event = AdminDocumentEvent {
            event_id: Ulid::from_bytes([4; 16]),
            target: AdminDocumentTarget::Group {
                group_id: group_id(1),
            },
            origin_node_id: secret.public(),
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: Actor {
                node_id: secret.public(),
                user_id: user_id(2),
                realm_id,
            },
            op: AdminDocumentOperation::GroupCreated {
                realm_id,
                display_name: "Engineering".to_string(),
                owner: user_id(2),
            },
        };
        let bytes = event.signing_bytes(&placement).expect("event signs");
        assert_eq!(bytes, event.signing_bytes(&placement).expect("stable"));
        let signature = secret.sign(&bytes);
        assert!(event.origin_signed(&placement, &signature));

        let mut elsewhere = placement;
        elsewhere.shard += 1;
        assert!(!event.origin_signed(&elsewhere, &signature));

        let mut tampered = event.clone();
        tampered.actor.user_id = user_id(3);
        assert!(!tampered.origin_signed(&placement, &signature));

        let mut foreign = event;
        foreign.origin_node_id = iroh::SecretKey::from_bytes(&[12; 32]).public();
        assert!(!foreign.origin_signed(&placement, &signature));
    }

    #[test]
    fn realm_config_roundtrips() {
        let operation = AdminDocumentOperation::ConfigNodeEnsured {
            node_id: node(3),
            kind: RealmNodeKind::Server,
        };

        assert_eq!(postcard_roundtrip(operation.clone()), operation);
    }

    #[test]
    fn realm_config_roundtrip() {
        let operations = [
            AdminDocumentOperation::OidcProviderUpserted {
                provider: oidc_provider("default"),
            },
            AdminDocumentOperation::OidcProviderRemoved {
                provider_id: "default".to_string(),
            },
        ];

        for operation in operations {
            assert_eq!(postcard_roundtrip(operation.clone()), operation);
        }
    }

    #[test]
    fn realm_settings_roundtrips() {
        let operation = AdminDocumentOperation::ConfigSettingsSet {
            metadata_replication: MetadataReplicationConfig::new(3),
            discovery: RealmDiscoveryConfig::Static {
                endpoints: Vec::new(),
            },
        };

        assert_eq!(postcard_roundtrip(operation.clone()), operation);
    }

    #[test]
    fn group_created_roundtrips() {
        let operation = AdminDocumentOperation::GroupCreated {
            realm_id: RealmId::from_bytes([9; 32]),
            display_name: "Engineering".to_string(),
            owner: user_id(3),
        };

        assert_eq!(postcard_roundtrip(operation.clone()), operation);
    }

    #[test]
    fn realm_description_roundtrips() {
        let operation = AdminDocumentOperation::ConfigDescriptionSet {
            description: "Demo Realm".to_string(),
        };

        assert_eq!(postcard_roundtrip(operation.clone()), operation);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentEvent {
    pub event_id: Ulid,
    pub target: AdminDocumentTarget,
    pub origin_node_id: NodeId,
    pub origin_seq: u64,
    pub observed: AdminDocumentClock,
    pub actor: Actor,
    pub op: AdminDocumentOperation,
}

impl AdminDocumentEvent {
    pub fn dot(&self) -> AdminDocumentDot {
        AdminDocumentDot {
            event_id: self.event_id,
            origin_node_id: self.origin_node_id,
            origin_seq: self.origin_seq,
        }
    }

    /// Deterministic bytes the origin signs. Binds the whole envelope plus the
    /// placement it rides, so a relay can forward but never re-target, re-actor,
    /// or re-shard another origin's event.
    pub fn signing_bytes(&self, placement: &PlacementRef) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(&(DOCUMENT_EVENT_DOMAIN, self, placement))
    }

    /// Whether `signature` is the origin node's signature over this envelope.
    pub fn origin_signed(&self, placement: &PlacementRef, signature: &iroh::Signature) -> bool {
        self.signing_bytes(placement)
            .is_ok_and(|bytes| self.origin_node_id.verify(&bytes, signature).is_ok())
    }
}
