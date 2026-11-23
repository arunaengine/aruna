//! Tests realm config digests, family placement, revocation floors, policies and node rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use crate::auth::REVOCATION_GRACE_SECS;
use crate::id::NodeId;
use crate::reducer::AdminDocumentState;
use crate::request_policy::{PolicyKind, RequestPolicy};
use crate::structs::execution::job::family::SubmissionId;
use crate::structs::identity::auth::Actor;
use crate::structs::identity::realm::{
    DynamicDiscoveryMethod, MetadataPathOverride, MetadataReplicationOverride, OidcProviderConfig,
    RealmAuthorizationDocument, RealmConfigDocument, RealmDiscoveryConfig, RealmId, RealmNodeKind,
    TokenRevocation, default_discovery_config,
};
use crate::structs::placement::record::{
    BindingScope, DocumentClass, PlacementOverride, PlacementRef, PlacementStrategy,
    StrategyBinding, shard_for_subject,
};
use crate::structs::placement::transition::CandidatePlacementMap;
use crate::structs::storage::node_info::{KIND_LABEL_KEY, NODE_LABEL_KEY};
use crate::user::id::UserId;
use std::collections::BTreeMap;
use ulid::Ulid;

use super::{
    JobFamilyError, MAX_NODE_NAME, NodeCoordinates, NodePlace, PortalFeatureError,
    normalize_node_name, validate_portal_features,
};

#[test]
pub fn realm_auth_roundtrip() {
    let auth_doc = RealmAuthorizationDocument::default_realm_doc(RealmId([0u8; 32]));
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
        user_id: crate::user::id::UserId::new(Ulid::generate(), RealmId([0u8; 32])),
        realm_id: RealmId([0u8; 32]),
    };
    let bytes = auth_doc.to_bytes(&actor).unwrap();
    let hydrated_auth_doc = RealmAuthorizationDocument::from_bytes(&bytes).unwrap();

    assert_eq!(auth_doc, hydrated_auth_doc);
    assert!(
        hydrated_auth_doc
            .roles
            .iter()
            .any(|(_id, role)| { role.name == "realm_admin" && role.assigned_users.is_empty() })
    );
}

#[test]
pub fn realm_config_roundtrip() {
    let group_id = Ulid::generate();
    let document = RealmConfigDocument {
        realm_id: RealmId([4u8; 32]),
        metadata_replication: super::MetadataReplicationConfig {
            default_replication_factor: 3,
            group_overrides: vec![MetadataReplicationOverride {
                group_id,
                replication_factor: 5,
            }],
            path_overrides: vec![MetadataPathOverride {
                group_id,
                path_prefix: "/datasets/demo".to_string(),
                replication_factor: 7,
            }],
        },
        oidc_providers: vec![OidcProviderConfig {
            id: "main".to_string(),
            issuer: "https://issuer.example".to_string(),
            audience: "aruna-api".to_string(),
            discovery_url: "https://issuer.example/.well-known/openid-configuration".to_string(),
        }],
        discovery: default_discovery_config(),
        nodes: Vec::new(),
        quota: super::QuotaConfig::default(),
        request_policies: Vec::new(),
        revoked_tokens: Vec::new(),
        revocation_floor: 0,
        compute: Default::default(),
        description: "Example Realm".to_string(),
        placement_map: Vec::new(),
        strategies: Vec::new(),
        default_strategy_id: None,
        family_strategy_id: Ulid::from_bytes([3u8; 16]),
        strategy_bindings: Vec::new(),
        placement_overrides: Vec::new(),
        placement_bindings: Vec::new(),
        placement_handle_ranges: Vec::new(),
        band_pools: Vec::new(),
        candidate_maps: Vec::new(),
        placement_activations: Vec::new(),
        placement_transitions: Vec::new(),
        federation: None,
        portal_features: BTreeMap::new(),
        replica_quota: Default::default(),
        email: None,
        vault: Vec::new(),
        secrets: Vec::new(),
        plugins: Vec::new(),
        installs: Vec::new(),
    };
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[14u8; 32]).public(),
        user_id: crate::user::id::UserId::new(Ulid::generate(), RealmId([4u8; 32])),
        realm_id: RealmId([4u8; 32]),
    };

    let bytes = document.to_bytes(&actor).expect("to bytes");
    let restored = RealmConfigDocument::from_bytes(&bytes).expect("from bytes");

    assert_eq!(document, restored);
}

#[test]
fn config_digest_changes() {
    let config = RealmConfigDocument::new(RealmId([4u8; 32]), Vec::new(), 3);
    let mut changed = config.clone();
    changed.description = "changed".to_string();

    assert_eq!(config.digest().unwrap(), config.clone().digest().unwrap());
    assert_ne!(config.digest().unwrap(), changed.digest().unwrap());
}

#[test]
fn config_bytes_canonical() {
    // Pins the canonical wire form. Postcard is positional, so the field
    // occupies a fixed slot that a shorter encoding cannot satisfy.
    const ENCODED: &str = "01010101010101010101010101010101010101010101010101010101010101010300000001020001026e300101ac023c00006e5500010300000000000000001a3032303831303430473230383130343047323038313034304732000000000000000000000000a0f8fa05e0a712b0ea01000000000000000000e0a712c0ee6d000000000000000000";
    let mut config = RealmConfigDocument::new(RealmId([1u8; 32]), Vec::new(), 3);
    config.family_strategy_id = Ulid::from_bytes([2u8; 16]);

    let bytes = postcard::to_allocvec(&config).unwrap();
    assert_eq!(hex::encode(&bytes), ENCODED);
    assert_eq!(RealmConfigDocument::from_bytes(&bytes).unwrap(), config);

    // Dropping the field's length-prefixed slot must fail to decode.
    let text = config.family_strategy_id.to_string();
    let start = bytes
        .windows(text.len())
        .position(|window| window == text.as_bytes())
        .expect("field is encoded as canonical ulid text");
    let mut without = bytes.clone();
    without.drain(start - 1..start + text.len());
    assert!(RealmConfigDocument::from_bytes(&without).is_err());
}

#[test]
fn seeds_family_strategy() {
    // Fresh creation must select a real strategy, not the nil placeholder.
    let mut config = RealmConfigDocument::new(RealmId([9u8; 32]), Vec::new(), 3);
    assert!(config.family_strategy_id.is_nil());
    config.seed_default_placement();

    assert!(!config.family_strategy_id.is_nil());
    assert!(config.strategy(&config.family_strategy_id).is_some());
    assert_ne!(Some(config.family_strategy_id), config.default_strategy_id);
}

#[test]
fn derives_family_placement() {
    // Two independently built configs sharing the stored strategy must
    // derive one identical placement for the same submission.
    let mut left = RealmConfigDocument::new(RealmId([10u8; 32]), Vec::new(), 3);
    left.seed_default_placement();
    let mut right = RealmConfigDocument::new(RealmId([11u8; 32]), Vec::new(), 5);
    right.strategies = left.strategies.clone();
    right.family_strategy_id = left.family_strategy_id;

    let submission = SubmissionId([7u8; 32]);
    let placement = left.family_placement(submission).unwrap();
    assert_eq!(placement.strategy_id, left.family_strategy_id);
    assert_eq!(Ok(placement), right.family_placement(submission));
    assert_ne!(
        placement.shard,
        left.family_placement(SubmissionId([8u8; 32]))
            .unwrap()
            .shard
    );
}

#[test]
fn refuses_unset_family() {
    let mut config = RealmConfigDocument::new(RealmId([12u8; 32]), Vec::new(), 3);
    let submission = SubmissionId([1u8; 32]);
    assert_eq!(
        config.family_placement(submission),
        Err(JobFamilyError::Unset)
    );

    let dangling = Ulid::from_bytes([5u8; 16]);
    config.family_strategy_id = dangling;
    assert_eq!(
        config.family_placement(submission),
        Err(JobFamilyError::Dangling(dangling))
    );
}

#[test]
fn digest_binds_family() {
    let config = RealmConfigDocument::new(RealmId([12u8; 32]), Vec::new(), 3);
    let mut changed = config.clone();
    changed.family_strategy_id = Ulid::from_bytes([1u8; 16]);

    assert_eq!(config.digest().unwrap(), config.clone().digest().unwrap());
    assert_ne!(config.digest().unwrap(), changed.digest().unwrap());
}

#[test]
fn digest_ignores_revocations() {
    // A revocation converges on its own; if it moved the digest, forwarded
    // requests would be rejected between nodes until it replicated.
    let config = RealmConfigDocument::new(RealmId([5u8; 32]), Vec::new(), 3);
    let mut revoked = config.clone();
    revoked.revoked_tokens.push(TokenRevocation {
        token_hash: crate::auth::bearer_token_hash("token"),
        expires_at: 2_000,
    });
    revoked.revocation_floor = 1_500;

    assert!(revoked.token_revoked(&crate::auth::bearer_token_hash("token"), 1_000));
    assert_eq!(config.digest().unwrap(), revoked.digest().unwrap());
}

#[test]
fn digest_ignores_policies() {
    // A node holding no realm-config bucket never sees a policy set; if it
    // moved the digest, that node could never forward a write again.
    let config = RealmConfigDocument::new(RealmId([7u8; 32]), Vec::new(), 3);
    let mut denied = config.clone();
    denied.request_policies.push(RequestPolicy {
        policy_id: Ulid::from_bytes([9u8; 16]),
        name: "deny-writes".to_string(),
        kind: PolicyKind::Deny,
        when: None,
        expression: "permission == 'write'".to_string(),
        enabled: true,
    });

    assert_eq!(config.digest().unwrap(), denied.digest().unwrap());
}

#[test]
fn digest_ignores_vault() {
    // Email and vault records never route requests; a change must not reject forwarded writes.
    use crate::realm_vault::{RealmVaultEntry, RealmVaultPurpose, RealmVaultRecord};
    let config = RealmConfigDocument::new(RealmId([7u8; 32]), Vec::new(), 3);
    let mut sealed = config.clone();
    sealed.email = Some(crate::structs::execution::email::RealmEmailProfile {
        revision: Ulid::from_bytes([6; 16]),
        host: "smtp.example.org".to_string(),
        port: None,
        tls: crate::structs::execution::email::SmtpTls::Starttls,
        username: None,
        from: "aruna@example.org".to_string(),
        password: None,
        changed_by: crate::user::id::UserId::local(Ulid::from_bytes([4; 16]), config.realm_id),
        changed_at_ms: 5,
    });
    sealed.vault.push(RealmVaultRecord {
        entry: RealmVaultEntry::new(RealmVaultPurpose::SmtpPassword, Ulid::from_bytes([1; 16])),
        revision: Ulid::from_bytes([2; 16]),
        sealed: vec![3; 40],
        changed_by: crate::user::id::UserId::local(Ulid::from_bytes([4; 16]), config.realm_id),
        changed_at_ms: 5,
    });

    assert_eq!(config.digest().unwrap(), sealed.digest().unwrap());
    let bytes = postcard::to_allocvec(&sealed).unwrap();
    assert_eq!(RealmConfigDocument::from_bytes(&bytes).unwrap(), sealed);
}

#[test]
fn expired_revocation_ignored() {
    // Past its token's expiry an entry denies nothing and is pruned, so the
    // replicated set cannot grow without bound.
    let realm_id = RealmId([6u8; 32]);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    let token_hash = crate::auth::bearer_token_hash("token");
    config.revoked_tokens.push(TokenRevocation {
        token_hash: token_hash.clone(),
        expires_at: 1_000,
    });

    assert!(config.token_revoked(&token_hash, 1_000));
    assert!(!config.token_revoked(&token_hash, 1_001));

    let reducer_state =
        AdminDocumentState::new(crate::admin_documents::AdminDocumentTarget::RealmConfig {
            realm_id,
        });
    // Retained for one grace window so a skewed peer still sees it, then pruned.
    config.merge_revocations(&reducer_state, 1_001);
    assert_eq!(config.revoked_tokens.len(), 1);
    config.merge_revocations(&reducer_state, 1_001 + REVOCATION_GRACE_SECS + 1);
    assert!(config.revoked_tokens.is_empty());
}

#[test]
fn user_cutoff_latest() {
    // The latest live cutoff of a user wins; other users and lapsed cutoffs deny nothing.
    let realm_id = RealmId([9u8; 32]);
    let user = crate::user::id::UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
    let other = crate::user::id::UserId::local(Ulid::from_bytes([10u8; 16]), realm_id);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    for cutoff in [1_000, 2_000] {
        config.revoked_tokens.push(TokenRevocation {
            token_hash: crate::auth::user_cutoff_hash(&user),
            expires_at: crate::auth::user_cutoff_expiry(cutoff),
        });
    }

    assert_eq!(config.user_cutoff(&user, 2_500), Some(2_000));
    assert_eq!(config.user_cutoff(&other, 2_500), None);
    let lapsed = crate::auth::user_cutoff_expiry(2_000) + 1;
    assert_eq!(config.user_cutoff(&user, lapsed), None);
}

#[test]
fn floor_blocks_rollback() {
    // A compacted hash must stay expired when the wall clock moves back.
    let realm_id = RealmId([8u8; 32]);
    let owner = crate::user::id::UserId::local(Ulid::from_bytes([8u8; 16]), realm_id);
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
        user_id: owner,
        realm_id,
    };
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let token_hash = crate::auth::bearer_token_hash("rollback-token");
    let mut reducer = AdminDocumentState::new(target);
    reducer
        .apply_operation(
            &actor,
            AdminDocumentOperation::ConfigTokenRevoked {
                token_hash: token_hash.clone(),
                expires_at: 1_000,
                token_owner: owner,
            },
            ulid::Ulid::generate(),
        )
        .unwrap();

    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    let index = reducer.revocation_index(1_000);
    config.merge_revocation_index(&index, 1_000);
    assert!(config.token_revoked(&token_hash, 999));

    reducer.compact_revocations(2_000);
    let index = reducer.revocation_index(2_000);
    config.merge_revocation_index(&index, 2_000);
    assert!(config.revoked_tokens.is_empty());
    assert_eq!(config.revocation_floor, 2_000);
    assert!(config.token_revoked(&token_hash, 2_000 - REVOCATION_GRACE_SECS - 1));
    assert!(!config.token_revoked(&token_hash, 2_000));
}

#[test]
fn floor_tolerates_skew() {
    // The floor carries the merging node's clock, so a node a few seconds
    // behind it must keep serving instead of denying every bearer token.
    let realm_id = RealmId([9u8; 32]);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.revocation_floor = 2_000;

    let token_hash = crate::auth::bearer_token_hash("skewed-token");
    assert!(!config.token_revoked(&token_hash, 2_000 - REVOCATION_GRACE_SECS));
    assert!(config.token_revoked(&token_hash, 2_000 - REVOCATION_GRACE_SECS - 1));
}

#[test]
fn owner_overlay_deduplicates() {
    let realm_id = RealmId([7u8; 32]);
    let owner_a = crate::user::id::UserId::local(Ulid::from_bytes([1u8; 16]), realm_id);
    let owner_b = crate::user::id::UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
    let actor_a = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
        user_id: owner_a,
        realm_id,
    };
    let actor_b = Actor {
        node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
        user_id: owner_b,
        realm_id,
    };
    let mut reducer = AdminDocumentState::new(AdminDocumentTarget::RealmConfig { realm_id });
    let token_hash = crate::auth::bearer_token_hash("owner-overlay");
    reducer
        .apply_operation(
            &actor_a,
            AdminDocumentOperation::ConfigTokenRevoked {
                token_hash: token_hash.clone(),
                expires_at: 2_000,
                token_owner: owner_a,
            },
            ulid::Ulid::generate(),
        )
        .unwrap();
    reducer
        .apply_operation(
            &actor_b,
            AdminDocumentOperation::ConfigTokenRevoked {
                token_hash: token_hash.clone(),
                expires_at: 2_000,
                token_owner: owner_b,
            },
            ulid::Ulid::generate(),
        )
        .unwrap();

    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.merge_revocations(&reducer, 1_000);

    assert_eq!(
        config.revoked_tokens,
        vec![TokenRevocation {
            token_hash,
            expires_at: 2_000,
        }]
    );
}

#[test]
fn seeds_job_control() {
    // Each seeded band must resolve for its own node and for no other.
    fn node_id(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }
    let mut config = RealmConfigDocument::new(RealmId([12u8; 32]), Vec::new(), 3);
    config.seed_default_placement();
    let first = config.seed_job_control(node_id(1), 0);
    let second = config.seed_job_control(node_id(2), 1);

    assert_ne!(first, second);
    assert_eq!(config.job_control_handle(&node_id(1)), Some(first));
    assert_eq!(config.job_control_handle(&node_id(2)), Some(second));
    assert_eq!(config.job_control_handle(&node_id(3)), None);
    assert!(config.binding_directory().resolve(first).is_ok());
    let ranges = &config.placement_handle_ranges;
    assert_ne!(ranges[0].range_id, ranges[1].range_id);
}

#[test]
fn snapshot_freezes_view() {
    use crate::structs::placement::record::NodePlacementEntry;

    fn node_id(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }
    let mut config = RealmConfigDocument::new(RealmId([11u8; 32]), Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(node_id(1), RealmNodeKind::Server);
    config.placement_map.push(NodePlacementEntry {
        node_id: node_id(1),
        location: "eu".to_string(),
        weight: 250,
        full: false,
        draining: false,
        labels: Default::default(),
    });

    assert_eq!(config.snapshot_candidate_map(), 1);
    let buckets: u32 = config.strategies.iter().map(|s| s.shard_count).sum();
    assert_eq!(config.placement_activations.len(), buckets as usize);
    let strategy_id = config.default_strategy_id.unwrap();
    let activation = *config
        .activation(&strategy_id, 7)
        .expect("bucket activated");
    assert_eq!(activation.activation_epoch, 1);
    assert_eq!(activation.candidate_map_epoch, 1);
    assert_eq!(activation.transition_id, None);

    // A later config edit changes the live view but never a published map.
    config.placement_map[0].weight = 1;
    config.ensure_node(node_id(2), RealmNodeKind::Server);
    let frozen = config.candidate_map(1).expect("epoch 1 is unconflicted");
    assert_eq!(frozen.nodes.len(), 1);
    assert_eq!(frozen.nodes[0].weight, 250);
    assert_eq!(
        frozen.nodes[0].labels.get(KIND_LABEL_KEY),
        Some(&"server".to_string())
    );
    // Every candidate carries its own id as an immutable label.
    assert_eq!(
        frozen.nodes[0].labels.get(NODE_LABEL_KEY),
        Some(&frozen.nodes[0].node_id.to_string())
    );
    assert_eq!(config.candidate_nodes().len(), 2);

    // A second snapshot opens a new epoch and leaves activations pinned.
    assert_eq!(config.snapshot_candidate_map(), 2);
    assert_eq!(config.newest_map_epoch(), Some(2));
    assert_eq!(config.activation(&strategy_id, 7), Some(&activation));
    assert_eq!(config.candidate_map(2).unwrap().nodes.len(), 2);

    // Divergent maps at one epoch keep the epoch unusable.
    let conflicting = CandidatePlacementMap {
        epoch: 2,
        nodes: Vec::new(),
        selectors: Vec::new(),
        shard_overrides: Vec::new(),
    };
    config.candidate_maps.push(conflicting);
    assert!(config.candidate_map(2).is_none());
}

#[test]
fn digest_binds_maps() {
    // Identical activations over divergent same-epoch map contents must
    // digest differently, while unreferenced maps stay excluded.
    let mut config = RealmConfigDocument::new(RealmId([6u8; 32]), Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(
        iroh::SecretKey::from_bytes(&[1; 32]).public(),
        RealmNodeKind::Management,
    );
    config.snapshot_candidate_map();
    let base = config.digest().unwrap();

    let mut divergent = config.clone();
    divergent.candidate_maps[0].nodes[0].weight += 1;
    assert_ne!(base, divergent.digest().unwrap());

    // Reordered equivalent maps digest identically.
    let mut reordered = config.clone();
    let extra = reordered.freeze_map(9);
    reordered.candidate_maps.push(extra.clone());
    reordered.placement_activations.push(
        crate::structs::placement::transition::PlacementActivation {
            strategy_id: reordered.default_strategy_id.unwrap(),
            shard: 63,
            activation_epoch: 2,
            candidate_map_epoch: 9,
            transition_id: None,
        },
    );
    let mut swapped = reordered.clone();
    swapped.candidate_maps.swap(0, 1);
    assert_eq!(reordered.digest().unwrap(), swapped.digest().unwrap());

    // An unreferenced newest map does not perturb the digest.
    let mut unreferenced = config.clone();
    let newest = unreferenced.freeze_map(10);
    unreferenced.candidate_maps.push(newest);
    assert_eq!(base, unreferenced.digest().unwrap());
}

#[test]
fn digest_ignores_order() {
    let mut config = RealmConfigDocument::new(
        RealmId([4u8; 32]),
        vec![
            OidcProviderConfig {
                id: "z".to_string(),
                issuer: "z".to_string(),
                audience: "z".to_string(),
                discovery_url: "z".to_string(),
            },
            OidcProviderConfig {
                id: "a".to_string(),
                issuer: "a".to_string(),
                audience: "a".to_string(),
                discovery_url: "a".to_string(),
            },
        ],
        3,
    );
    config.seed_default_placement();
    config.ensure_node(
        iroh::SecretKey::from_bytes(&[1; 32]).public(),
        RealmNodeKind::Management,
    );
    config.ensure_node(
        iroh::SecretKey::from_bytes(&[2; 32]).public(),
        RealmNodeKind::Server,
    );
    let mut reordered = config.clone();
    reordered.oidc_providers.reverse();
    reordered.nodes.reverse();
    reordered.strategies.reverse();
    reordered.strategy_bindings.reverse();
    reordered.placement_bindings.reverse();

    assert_eq!(config.digest().unwrap(), reordered.digest().unwrap());
}

#[test]
fn group_ceiling_grace() {
    let group = Ulid::from_bytes([1u8; 16]);
    let other = Ulid::from_bytes([2u8; 16]);
    let quota = super::QuotaConfig {
        default_quota_bytes: Some(1_000),
        grace_factor_percent: 110,
        group_overrides: vec![super::GroupQuotaOverride {
            group_id: group,
            quota_bytes: Some(2_000),
            grace_factor_percent: Some(150),
        }],
        ..super::QuotaConfig::default()
    };

    // Override quota_bytes and grace win for the overridden group.
    assert_eq!(quota.effective_group_ceiling(&group), Some(3_000));
    // Default quota and global grace apply otherwise.
    assert_eq!(quota.effective_group_ceiling(&other), Some(1_100));

    // No default and no override => unlimited (no gate).
    let unlimited = super::QuotaConfig {
        default_quota_bytes: None,
        ..super::QuotaConfig::default()
    };
    assert_eq!(unlimited.effective_group_ceiling(&other), None);

    // An existing override with quota_bytes: None is explicitly unlimited even
    // when a finite default exists.
    let unlimited_override = super::QuotaConfig {
        default_quota_bytes: Some(1_000),
        grace_factor_percent: 110,
        group_overrides: vec![super::GroupQuotaOverride {
            group_id: group,
            quota_bytes: None,
            grace_factor_percent: None,
        }],
        ..super::QuotaConfig::default()
    };
    assert_eq!(unlimited_override.effective_group_ceiling(&group), None);
    assert_eq!(
        unlimited_override.effective_group_ceiling(&other),
        Some(1_100)
    );

    let huge = super::QuotaConfig {
        default_quota_bytes: Some(u64::MAX),
        grace_factor_percent: 100,
        ..super::QuotaConfig::default()
    };
    assert_eq!(huge.effective_group_ceiling(&other), Some(u64::MAX));

    let over_huge = super::QuotaConfig {
        default_quota_bytes: Some(u64::MAX),
        grace_factor_percent: 110,
        ..super::QuotaConfig::default()
    };
    assert_eq!(over_huge.effective_group_ceiling(&other), Some(u64::MAX));
}

#[test]
fn replicas_follow_groups() {
    let (on, off, other) = (
        Ulid::from_bytes([2u8; 16]),
        Ulid::from_bytes([3u8; 16]),
        Ulid::from_bytes([4u8; 16]),
    );
    let mut replicas = super::ReplicaQuota {
        count: false,
        groups: BTreeMap::from([(on, true), (off, false)]),
    };
    assert!(
        !super::ReplicaQuota::default().counts(&other),
        "off by default"
    );
    assert!(replicas.counts(&on) && !replicas.counts(&other));
    replicas.count = true;
    assert!(replicas.counts(&other) && !replicas.counts(&off));
}

#[test]
pub fn replication_resolution() {
    let group_id = Ulid::generate();
    let other_group_id = Ulid::generate();
    let document = RealmConfigDocument {
        realm_id: RealmId([5u8; 32]),
        metadata_replication: super::MetadataReplicationConfig {
            default_replication_factor: 3,
            group_overrides: vec![MetadataReplicationOverride {
                group_id,
                replication_factor: 5,
            }],
            path_overrides: vec![
                MetadataPathOverride {
                    group_id,
                    path_prefix: "/datasets".to_string(),
                    replication_factor: 6,
                },
                MetadataPathOverride {
                    group_id,
                    path_prefix: "/datasets/important".to_string(),
                    replication_factor: 7,
                },
            ],
        },
        oidc_providers: vec![],
        discovery: default_discovery_config(),
        nodes: Vec::new(),
        quota: super::QuotaConfig::default(),
        request_policies: Vec::new(),
        revoked_tokens: Vec::new(),
        revocation_floor: 0,
        compute: Default::default(),
        description: String::new(),
        placement_map: Vec::new(),
        strategies: Vec::new(),
        default_strategy_id: None,
        family_strategy_id: Ulid::nil(),
        strategy_bindings: Vec::new(),
        placement_overrides: Vec::new(),
        placement_bindings: Vec::new(),
        placement_handle_ranges: Vec::new(),
        band_pools: Vec::new(),
        candidate_maps: Vec::new(),
        placement_activations: Vec::new(),
        placement_transitions: Vec::new(),
        federation: None,
        portal_features: BTreeMap::new(),
        replica_quota: Default::default(),
        email: None,
        vault: Vec::new(),
        secrets: Vec::new(),
        plugins: Vec::new(),
        installs: Vec::new(),
    };

    assert_eq!(document.replication_factor_for(other_group_id, None), 3);
    assert_eq!(document.replication_factor_for(group_id, None), 5);
    assert_eq!(
        document.replication_factor_for(group_id, Some("/datasets/demo")),
        6
    );
    assert_eq!(
        document.replication_factor_for(group_id, Some("/datasets/important/item")),
        7
    );
}

#[test]
fn replication_uses_strategy() {
    let mut document = RealmConfigDocument::new(RealmId([6u8; 32]), Vec::new(), 5);
    document.seed_default_placement();

    let default_strategy_id = document.default_strategy_id.unwrap();
    assert_eq!(
        document
            .strategy(&default_strategy_id)
            .unwrap()
            .replica_count,
        Some(5)
    );
    assert_eq!(document.effective_replication_factor(), Some(5));

    document
        .strategies
        .iter_mut()
        .find(|strategy| strategy.strategy_id == default_strategy_id)
        .unwrap()
        .replica_count = Some(2);
    assert_eq!(document.effective_replication_factor(), Some(2));

    document
        .strategies
        .iter_mut()
        .find(|strategy| strategy.strategy_id == default_strategy_id)
        .unwrap()
        .replica_count = None;
    assert_eq!(document.effective_replication_factor(), None);

    document.default_strategy_id = None;
    assert_eq!(document.effective_replication_factor(), None);

    document.strategies.clear();
    assert_eq!(document.effective_replication_factor(), Some(5));
}

#[test]
pub fn default_discovery() {
    let discovery = default_discovery_config();

    match discovery {
        RealmDiscoveryConfig::Dynamic { methods } => {
            assert!(matches!(
                methods.as_slice(),
                [
                    DynamicDiscoveryMethod::IrohDns { .. },
                    DynamicDiscoveryMethod::DhtSigned {
                        ttl_secs: 300,
                        refresh_after_secs: 60,
                    }
                ]
            ));
        }
        other => panic!("unexpected default discovery config: {other:?}"),
    }
}

#[test]
fn excludes_user_nodes() {
    fn node_id(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    let server = node_id(1);
    let user_device = node_id(2);
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let owner = UserId::nil(realm_id);
    let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    document.ensure_node(server, RealmNodeKind::Server);
    document.ensure_node(user_device, RealmNodeKind::User { owner });

    assert_eq!(document.node_ids().unwrap(), vec![server, user_device]);
    assert_eq!(document.sync_eligible_nodes().unwrap(), vec![server]);
    assert_eq!(document.nodes[1].kind.owner(), Some(owner));
}

#[test]
fn owner_survives_rebalance() {
    // The derived owner is a pure function of band + binding; arbitrary
    // placement-map, strategy, and override changes never move it.
    use crate::structs::execution::job::identity::JobId;
    use crate::structs::identity::realm::JobOwnerError;
    use crate::structs::placement::record::{
        DocumentClass, FIRST_GRANTABLE_HANDLE, HANDLE_RANGE_SIZE, HandleRange, NodePlacementEntry,
        PlacementBinding, PlacementOverride, PlacementScope,
    };
    use crate::structured_id::{BucketId, PlacementHandle};

    fn node_id(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }
    let realm_id = RealmId([7u8; 32]);
    let owner = node_id(1);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.seed_default_placement();
    let range_id = Ulid::from_bytes([9; 16]);
    config.placement_handle_ranges.push(HandleRange {
        range_id,
        owner,
        start: FIRST_GRANTABLE_HANDLE,
        end: FIRST_GRANTABLE_HANDLE + HANDLE_RANGE_SIZE,
    });
    let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap();
    config.placement_bindings.push(PlacementBinding {
        handle,
        scope: PlacementScope::Realm(realm_id),
        document_class: DocumentClass::JobControl,
        strategy_id: config.default_strategy_id.unwrap(),
        allocator_range_id: Some(range_id),
        allocated_by: Some(owner),
        allocated_at_ms: Some(1),
    });
    assert_eq!(config.job_control_handle(&owner), Some(handle));
    assert_eq!(config.job_control_handle(&node_id(2)), None);

    let job_id = JobId::from_parts(1, handle, BucketId::new(5).unwrap(), 9).unwrap();
    assert_eq!(config.job_owner(job_id), Ok(owner));

    // Holders move: nodes join, the owner is excluded everywhere, weights
    // shift, the default strategy is replaced. The owner does not.
    for seed in 2..6u8 {
        config.ensure_node(node_id(seed), RealmNodeKind::Management);
        config.placement_map.push(NodePlacementEntry {
            node_id: node_id(seed),
            location: String::new(),
            weight: 500,
            full: false,
            draining: false,
            labels: Default::default(),
        });
    }
    config.placement_overrides.push(PlacementOverride {
        subject: b"any-subject".to_vec(),
        pinned: vec![node_id(2)],
        excluded: vec![owner],
        strategy_id: None,
    });
    assert_eq!(config.job_owner(job_id), Ok(owner));

    // An unknown handle is fail-closed 503 material, never absence.
    let unsynced = PlacementHandle::new(FIRST_GRANTABLE_HANDLE + HANDLE_RANGE_SIZE).unwrap();
    let foreign = JobId::from_parts(1, unsynced, BucketId::new(0).unwrap(), 9).unwrap();
    assert!(matches!(
        config.job_owner(foreign),
        Err(JobOwnerError::Unavailable(_))
    ));

    // A non-JobControl handle proves the id is no job of this realm.
    let metadata = PlacementHandle::new(super::METADATA_HANDLE).unwrap();
    let invalid = JobId::from_parts(1, metadata, BucketId::new(0).unwrap(), 9).unwrap();
    assert_eq!(config.job_owner(invalid), Err(JobOwnerError::NotJobControl));

    // A bucket past the strategy capacity is proof too.
    let wide = JobId::from_parts(1, handle, BucketId::new(64).unwrap(), 9).unwrap();
    assert_eq!(config.job_owner(wide), Err(JobOwnerError::NotJobControl));

    // A conflicted binding fails closed as unavailable.
    let mut divergent = config.placement_bindings.last().unwrap().clone();
    divergent.allocated_at_ms = Some(2);
    config.placement_bindings.push(divergent);
    assert!(matches!(
        config.job_owner(job_id),
        Err(JobOwnerError::Unavailable(_))
    ));
    assert_eq!(config.job_control_handle(&owner), None);
}

#[test]
fn directory_rebuilds_state() {
    use crate::structs::placement::record::{
        DocumentClass, HandleRange, PlacementBinding, PlacementScope,
    };
    use crate::structured_id::PlacementHandle;

    let owner = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let range_id = Ulid::from_bytes([8; 16]);
    fn binding(handle: u32, seed: u8, owner: NodeId, range_id: Ulid) -> PlacementBinding {
        PlacementBinding {
            handle: PlacementHandle::new(handle).unwrap(),
            scope: PlacementScope::Group(Ulid::from_bytes([seed; 16])),
            document_class: DocumentClass::Metadata,
            strategy_id: Ulid::from_bytes([seed.wrapping_add(1); 16]),
            allocator_range_id: Some(range_id),
            allocated_by: Some(owner),
            allocated_at_ms: Some(1),
        }
    }

    let mut config = RealmConfigDocument::new(RealmId([2u8; 32]), Vec::new(), 3);
    config.placement_handle_ranges.push(HandleRange {
        range_id,
        owner,
        start: 3,
        end: 20,
    });
    let first = binding(10, 1, owner, range_id);
    config.placement_bindings.push(first.clone());

    let directory = config.binding_directory();
    assert_eq!(
        directory.resolve(first.handle).map(|t| t.strategy_id),
        Ok(first.strategy_id)
    );
    assert_eq!(directory.allocated(), 1);

    let mut provenance_conflict = config.clone();
    let mut divergent_provenance = first.clone();
    divergent_provenance.allocated_at_ms = Some(2);
    provenance_conflict
        .placement_bindings
        .push(divergent_provenance);
    assert_eq!(provenance_conflict.binding_directory().conflicted(), 1);

    // A same-handle, different-tuple entry fails closed as conflicted.
    config
        .placement_bindings
        .push(binding(10, 2, owner, range_id));
    assert!(config.binding_directory().resolve(first.handle).is_err());
    assert_eq!(config.binding_directory().conflicted(), 1);
}

#[test]
fn seeds_policy_binding() {
    // Policy documents ride the replica-capped default, not the everywhere
    // strategy, so a realm always has non-holders that must resolve by id.
    let mut config = RealmConfigDocument::new(RealmId([5u8; 32]), Vec::new(), 2);
    config.seed_default_placement();
    let bound = config
        .class_strategy(crate::structs::placement::record::DocumentClass::PlacementPolicy)
        .expect("binding resolves")
        .expect("a strategy is bound");
    assert_eq!(Some(bound.strategy_id), config.default_strategy_id);
    assert_eq!(bound.replica_count, Some(2));
}

#[test]
fn seeds_vault_binding() {
    // An explicit binding lets a realm admin give vaults their own strategy.
    let mut config = RealmConfigDocument::new(RealmId([5u8; 32]), Vec::new(), 2);
    config.seed_default_placement();
    let vault_class = BindingScope::Class(DocumentClass::UserVault);
    let binding = config
        .strategy_bindings
        .iter()
        .find(|binding| binding.scope == vault_class)
        .expect("vault class binding is seeded");
    assert_eq!(Some(binding.strategy_id), config.default_strategy_id);
}

#[test]
fn bucket_follows_id() {
    let mut config = RealmConfigDocument::new(RealmId([5u8; 32]), Vec::new(), 2);
    config.seed_default_placement();
    let first = config
        .policy_placement(Ulid::from_bytes([1u8; 16]))
        .expect("bucket resolves");
    let repeat = config
        .policy_placement(Ulid::from_bytes([1u8; 16]))
        .expect("bucket resolves");
    let other = config
        .policy_placement(Ulid::from_bytes([2u8; 16]))
        .expect("bucket resolves");
    assert_eq!(first, repeat);
    assert_eq!(Some(first.strategy_id), config.default_strategy_id);
    assert_ne!(first, other);
}

fn strategy(seed: u8, shard_count: u32) -> PlacementStrategy {
    PlacementStrategy {
        strategy_id: Ulid::from_bytes([seed; 16]),
        name: format!("strategy-{seed}"),
        replica_count: Some(2),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count,
    }
}

fn expect_ref(strategy: &PlacementStrategy, policy_id: Ulid) -> PlacementRef {
    PlacementRef {
        strategy_id: strategy.strategy_id,
        shard: shard_for_subject(&policy_id.to_bytes(), strategy.shard_count),
    }
}

#[test]
fn policy_class_binding() {
    let bound = strategy(1, 4);
    let default = strategy(2, 8);
    let mut config = RealmConfigDocument::new(RealmId([5u8; 32]), Vec::new(), 2);
    config.default_strategy_id = Some(default.strategy_id);
    config.strategies = vec![default, bound.clone()];
    config.strategy_bindings = vec![StrategyBinding {
        scope: BindingScope::Class(DocumentClass::PlacementPolicy),
        strategy_id: bound.strategy_id,
    }];
    let policy_id = Ulid::from_bytes([3u8; 16]);
    assert_eq!(
        config.policy_placement(policy_id),
        Some(expect_ref(&bound, policy_id))
    );
}

#[test]
fn policy_override_steers() {
    let bound = strategy(1, 4);
    let steered = strategy(2, 8);
    let mut config = RealmConfigDocument::new(RealmId([5u8; 32]), Vec::new(), 2);
    config.default_strategy_id = Some(bound.strategy_id);
    config.strategies = vec![bound.clone(), steered.clone()];
    config.strategy_bindings = vec![StrategyBinding {
        scope: BindingScope::Class(DocumentClass::PlacementPolicy),
        strategy_id: bound.strategy_id,
    }];
    let policy_id = Ulid::from_bytes([3u8; 16]);
    config.placement_overrides.push(PlacementOverride {
        subject: policy_id.to_bytes().to_vec(),
        pinned: Vec::new(),
        excluded: Vec::new(),
        strategy_id: Some(steered.strategy_id),
    });
    assert_eq!(
        config.policy_placement(policy_id),
        Some(expect_ref(&steered, policy_id))
    );
}

#[test]
fn override_dangling_fails() {
    let bound = strategy(1, 4);
    let mut config = RealmConfigDocument::new(RealmId([5u8; 32]), Vec::new(), 2);
    config.default_strategy_id = Some(bound.strategy_id);
    config.strategies = vec![bound.clone()];
    config.strategy_bindings = vec![StrategyBinding {
        scope: BindingScope::Class(DocumentClass::PlacementPolicy),
        strategy_id: bound.strategy_id,
    }];
    let policy_id = Ulid::from_bytes([3u8; 16]);
    config.placement_overrides.push(PlacementOverride {
        subject: policy_id.to_bytes().to_vec(),
        pinned: Vec::new(),
        excluded: Vec::new(),
        strategy_id: Some(Ulid::from_bytes([9u8; 16])),
    });
    assert_eq!(config.policy_placement(policy_id), None);
}

#[test]
fn class_dangling_fails() {
    let fallback = strategy(1, 4);
    let mut config = RealmConfigDocument::new(RealmId([5u8; 32]), Vec::new(), 2);
    config.strategies = vec![fallback];
    config.strategy_bindings = vec![StrategyBinding {
        scope: BindingScope::Class(DocumentClass::PlacementPolicy),
        strategy_id: Ulid::from_bytes([9u8; 16]),
    }];
    assert_eq!(config.policy_placement(Ulid::from_bytes([3u8; 16])), None);
}

#[test]
fn policy_strategy_fallback() {
    let first = strategy(1, 4);
    let mut config = RealmConfigDocument::new(RealmId([5u8; 32]), Vec::new(), 2);
    config.strategies = vec![first.clone(), strategy(2, 8)];
    let policy_id = Ulid::from_bytes([3u8; 16]);
    assert_eq!(
        config.policy_placement(policy_id),
        Some(expect_ref(&first, policy_id))
    );
}

#[test]
fn rejects_missing_compute() {
    // Fresh format: the compute block is mandatory on decode, so bytes that
    // stop where the predecessor record ended must not decode at all.
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
        user_id: crate::user::id::UserId::new(Ulid::generate(), RealmId([7u8; 32])),
        realm_id: RealmId([7u8; 32]),
    };
    let config = RealmConfigDocument::new(RealmId([7u8; 32]), Vec::new(), 2);
    let encoded = config.to_bytes(&actor).expect("config encodes");
    let tail = postcard::to_allocvec(&(
        &config.compute,
        &config.federation,
        &config.portal_features,
        &config.replica_quota,
        &config.email,
        &config.vault,
        &config.secrets,
        &config.plugins,
        &config.installs,
    ))
    .expect("tail encodes");
    assert!(encoded.ends_with(&tail));

    let truncated = &encoded[..encoded.len() - tail.len()];

    assert!(RealmConfigDocument::from_bytes(truncated).is_err());
    assert_eq!(
        RealmConfigDocument::from_bytes(&encoded).expect("full record decodes"),
        config
    );
}

#[test]
fn node_name_rule() {
    // Every node replicates the name, so the one rule must refuse what a portal cannot show.
    assert_eq!(
        normalize_node_name("  Giessen 1 "),
        Some("Giessen 1".to_string())
    );
    assert_eq!(
        normalize_node_name(&"ä".repeat(MAX_NODE_NAME)).map(|name| name.chars().count()),
        Some(MAX_NODE_NAME)
    );
    assert_eq!(normalize_node_name(&"a".repeat(MAX_NODE_NAME + 1)), None);
    assert_eq!(normalize_node_name("   "), None);
    assert_eq!(normalize_node_name("node\u{7}one"), None);
    assert_eq!(normalize_node_name("two\nlines"), None);
}

#[test]
fn node_coordinates_bounded() {
    // Replicated values are whole numbers, so every node stores the same bytes for a point.
    let giessen = NodeCoordinates::from_degrees(50.583_6, 8.678_4).unwrap();
    assert_eq!((giessen.lat_e7, giessen.lon_e7), (505_836_000, 86_784_000));
    assert_eq!(giessen.degrees(), (50.583_6, 8.678_4));
    let corner = NodeCoordinates::from_degrees(-90.0, 180.0).unwrap();
    assert!(corner.valid());
    for (lat, lon) in [
        (90.1, 0.0),
        (0.0, -180.5),
        (f64::NAN, 0.0),
        (0.0, f64::INFINITY),
    ] {
        assert_eq!(NodeCoordinates::from_degrees(lat, lon), None);
    }
    let out = NodeCoordinates {
        lat_e7: 900_000_001,
        lon_e7: 0,
    };
    assert!(!out.valid());
    assert!(
        !NodePlace::default().valid(),
        "an empty place is a clear, not a value"
    );
    let padded = NodePlace {
        name: Some(" Giessen".to_string()),
        coordinates: None,
    };
    assert!(!padded.valid());
}

#[test]
fn portal_feature_names() {
    let named = |name: &str| BTreeMap::from([(name.to_string(), false)]);
    assert_eq!(validate_portal_features(&named("dataset-export2")), Ok(()));
    assert_eq!(validate_portal_features(&named(&"a".repeat(64))), Ok(()));
    for name in ["", "Compute", "a_b", "a.b", "ü", &"a".repeat(65)] {
        assert_eq!(
            validate_portal_features(&named(name)),
            Err(PortalFeatureError::InvalidName)
        );
    }
    let many = (0..65).map(|index| (format!("f{index}"), true)).collect();
    assert_eq!(
        validate_portal_features(&many),
        Err(PortalFeatureError::TooMany)
    );
}
