//! Seeds policy gate storage fixtures on top of the pure identity policy helpers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::{NODE_SUBJECT_KEYSPACE, POLICY_CACHE_KEYSPACE};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::node_subject::{NODE_SUBJECT_KEY, NodeSubjectRecord};
use aruna_core::structs::placement::policy::document::{
    PlacementPolicyDocument, PolicyPublicationClaim,
};
use aruna_core::structs::placement::policy::{PlacementSubject, VerifiedPolicy};
use aruna_core::user::id::UserId;
use aruna_drivers::driver::DriverContext;
use aruna_ops_identity::placement::policy::cache::{PolicyCacheEntry, cache_key};

pub(crate) use aruna_ops_identity::tests::policy::*;

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
        (DocumentTarget::RealmConfig { realm_id }, config_value),
        (DocumentTarget::RealmAuthorization { realm_id }, auth_value),
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
                key_space: POLICY_CACHE_KEYSPACE.to_string(),
                key: cache_key(&policy.policy_ref()),
                value: entry.to_bytes().expect("entry encodes").into(),
                txn_id: None,
            })
            .await;
    }
}
