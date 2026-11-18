//! Seeds the advertised node subject and resolved policies that governed write tests need.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{NODE_SUBJECT_KEYSPACE, POLICY_CACHE_KEYSPACE};
use aruna_core::metadata::MetadataError;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::node_subject::{NODE_SUBJECT_KEY, NodeSubjectRecord};
use aruna_core::structs::placement::policy::document::{
    PlacementPolicyDocument, PolicyPublicationClaim,
};
use aruna_core::structs::placement::policy::{
    PlacementPolicyRef, PlacementSubject, PolicyResolution, VerifiedPolicy,
};
use aruna_core::types::{Effects, Value};
use std::collections::BTreeMap;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::protocol::{MetadataTransportMessage, decode_message};
use crate::placement::policy::cache::{PolicyCacheEntry, cache_key};
use crate::placement::policy::tests::{node, realm_authorization};
use crate::placement::policy::{
    PolicySetError, PolicySource, ReadPolicyError, Resolutions, ResolvedPolicy,
};

/// The holders a holder fetch sub-operation asks, in order, and the request it sends them. Every
/// holder is answered with a failure, so the walk visits all of them.
pub(crate) fn fetch_requests(effects: Effects) -> (Vec<NodeId>, MetadataTransportMessage) {
    let Some(Effect::SubOperation(mut fetch)) = effects.into_iter().next() else {
        panic!("expected a holder fetch");
    };
    let (mut holders, mut asked) = (Vec::new(), None);
    let mut next = fetch.start();
    while let Some(Effect::Net(NetEffect::MetadataRequest(request))) = next.into_iter().next() {
        asked.get_or_insert_with(|| decode_message(&request.frame).expect("request decodes"));
        holders.push(request.peer);
        next = fetch.step(Event::Net(NetEvent::MetadataAnswer(Box::new(
            MetadataAnswer {
                peer: request.peer,
                elapsed: std::time::Duration::ZERO,
                result: Err(PeerRequestError::Failed(MetadataError::Backend("test".to_string()))),
            },
        ))));
    }
    (holders, asked.expect("the fetch asked a holder"))
}

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

/// A resolver child that resolved every one of `policies`.
pub(crate) fn resolved_set(policies: &[VerifiedPolicy]) -> Event {
    let resolved: Resolutions = policies
        .iter()
        .map(|policy| {
            (
                policy.policy().policy_id,
                PolicyResolution::Known(policy.clone()),
            )
        })
        .collect();
    Event::Child(ChildOutcome::new(Ok::<_, PolicySetError>(resolved)))
}

/// A strict resolver child that could not authenticate `policy_ref`.
pub(crate) fn unresolved_set(policy_ref: PlacementPolicyRef) -> Event {
    let source = ReadPolicyError::Unavailable("no holder answered".to_string());
    let error = PolicySetError::Unresolved { policy_ref, source };
    Event::Child(ChildOutcome::new(Err::<Resolutions, _>(error)))
}

/// A resolve child that obtained `policy` from the cache.
pub(crate) fn resolved_policy(policy: &VerifiedPolicy) -> Event {
    let resolved = ResolvedPolicy {
        policy: policy.clone(),
        source: PolicySource::Cached,
        stats: Default::default(),
    };
    Event::Child(ChildOutcome::new(Ok::<_, ReadPolicyError>(resolved)))
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
