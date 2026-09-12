//! What production wiring establishes before a governed write is possible: an
//! advertised subject and the policies this node has already resolved.

use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NODE_SUBJECT_KEYSPACE, PLACEMENT_POLICY_CACHE_KEYSPACE};
use aruna_core::structs::{
    Actor, NODE_SUBJECT_KEY, NodeSubjectRecord, Permission, PlacementPolicyDocument,
    PlacementSubject, PolicyPublicationClaim, RealmConfigDocument, RealmId, RealmNodeKind, Role,
    VerifiedPolicy,
};
use aruna_core::types::{GroupId, Key, NodeId, UserId, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::placement::policy::cache::{PolicyCacheEntry, cache_key};
use crate::placement::policy::tests::{node, realm_authorization};

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

/// Group authorization granting the policy fixtures' admin user write on
/// that group's admin path, which a group-owned publication needs.
pub(crate) fn group_authorization(
    realm_id: RealmId,
    group_id: Ulid,
) -> aruna_core::structs::GroupAuthorizationDocument {
    let role = Role {
        role_id: Ulid::from_bytes([4u8; 16]),
        name: "group_admin".to_string(),
        permissions: HashMap::from([(format!("/{realm_id}/g/{group_id}/**"), Permission::WRITE)]),
        assigned_users: HashSet::from([admin_user(realm_id)]),
    };
    aruna_core::structs::GroupAuthorizationDocument {
        group_id,
        roles: HashMap::from([(role.role_id, role)]),
        policies: Vec::new(),
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

pub fn authority(realm_id: RealmId) -> Event {
    authority_view(realm_id, None)
}

/// The same realm view plus the owning group's roles, which a group-owned
/// rule is authenticated against.
pub fn group_authority(realm_id: RealmId, group_id: GroupId) -> Event {
    authority_view(realm_id, Some(group_id))
}

fn authority_view(realm_id: RealmId, group_id: Option<GroupId>) -> Event {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    config.seed_default_placement();
    for seed in [1, 9] {
        config.ensure_node(
            iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            RealmNodeKind::Server,
        );
    }
    let (config_value, auth_value) = realm_view(&config, admin_user(realm_id));
    let key: Key = Vec::new().into();
    let mut values = vec![
        (key.clone(), Some(config_value)),
        (key.clone(), Some(auth_value)),
    ];
    if let Some(group_id) = group_id {
        let document = group_authorization(realm_id, group_id);
        values.push((
            key,
            Some(
                document
                    .to_bytes(&aruna_core::structs::Actor {
                        node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
                        user_id: admin_user(realm_id),
                        realm_id,
                    })
                    .expect("group authorization encodes")
                    .into(),
            ),
        ));
    }
    Event::Storage(StorageEvent::BatchReadResult { values })
}

pub async fn seed_gate(
    context: &DriverContext,
    realm_id: RealmId,
    user_id: UserId,
    subject: PlacementSubject,
    policies: &[VerifiedPolicy],
) {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    config.seed_default_placement();
    config.ensure_node(subject.node_id, RealmNodeKind::Server);
    config.ensure_node(
        iroh::SecretKey::from_bytes(&[9; 32]).public(),
        RealmNodeKind::Server,
    );
    let (config_value, auth_value) = realm_view(&config, user_id);
    for (target, value) in [
        (DocumentSyncTarget::RealmConfig { realm_id }, config_value),
        (
            DocumentSyncTarget::RealmAuthorization { realm_id },
            auth_value,
        ),
    ] {
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value,
                txn_id: None,
            })
            .await;
    }
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
        let publisher = record.subject.node_id;
        let publication = PolicyPublicationClaim::new(
            realm_id,
            policy,
            publisher,
            user_id,
            ulid::Ulid::from_bytes([5u8; 16]),
            7,
            [0u8; 32],
        )
        .signed_with(|message| {
            context
                .net_handle
                .as_ref()
                .expect("fixture has a net handle")
                .sign(message)
        });
        let document = PlacementPolicyDocument::new(realm_id, policy, publication);
        let entry = PolicyCacheEntry::verified(&document, 0);
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
