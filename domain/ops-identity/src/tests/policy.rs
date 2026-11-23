//! Builds pure placement-policy fixtures and resolved child outcomes for policy tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::id::NodeId;
use aruna_core::metadata::MetadataError;
use aruna_core::metadata::protocol::{MetadataTransportMessage, decode_message};
use aruna_core::structs::identity::auth::{Actor, Permission, Role};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::placement::policy::document::{
    PlacementPolicyDocument, PolicyPublicationClaim,
};
use aruna_core::structs::placement::policy::{
    PlacementPolicyRef, PlacementSubject, PolicyResolution, VerifiedPolicy,
};
use aruna_core::types::{Effects, Value};
use aruna_core::user::id::UserId;
use std::collections::{BTreeMap, HashMap, HashSet};
use ulid::Ulid;

use crate::placement::policy::read::{PolicySource, ReadPolicyError};
use crate::placement::policy::resolve::ResolvedPolicy;
use crate::placement::policy::resolve_set::{PolicySetError, Resolutions};

/// Realm authorization granting `user` the realm-configuration write every
/// policy publication is verified against.
pub fn realm_authorization(realm_id: RealmId, user: UserId) -> RealmAuthorizationDocument {
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

/// The holders a holder fetch sub-operation asks, in order, and the request it sends them. Every
/// holder is answered with a failure, so the walk visits all of them.
pub fn fetch_requests(effects: Effects) -> (Vec<NodeId>, MetadataTransportMessage) {
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
                result: Err(PeerRequestError::Failed(MetadataError::Backend(
                    "test".to_string(),
                ))),
            },
        ))));
    }
    (holders, asked.expect("the fetch asked a holder"))
}

/// The authorizing user every policy fixture publishes under.
pub fn admin_user(realm_id: RealmId) -> UserId {
    UserId::local(Ulid::from_bytes([2u8; 16]), realm_id)
}

/// One authentic publication of `policy` by node `seed`, so fixtures carry
/// provenance a verifier accepts instead of asserted fields.
pub fn signed_document(
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
pub fn realm_view(config: &RealmConfigDocument, user: UserId) -> (Value, Value) {
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
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
pub fn resolved_set(policies: &[VerifiedPolicy]) -> Event {
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
pub fn unresolved_set(policy_ref: PlacementPolicyRef) -> Event {
    let source = ReadPolicyError::Unavailable("no holder answered".to_string());
    let error = PolicySetError::Unresolved { policy_ref, source };
    Event::Child(ChildOutcome::new(Err::<Resolutions, _>(error)))
}

/// A resolve child that obtained `policy` from the cache.
pub fn resolved_policy(policy: &VerifiedPolicy) -> Event {
    let resolved = ResolvedPolicy {
        policy: policy.clone(),
        source: PolicySource::Cached,
        stats: Default::default(),
    };
    Event::Child(ChildOutcome::new(Ok::<_, ReadPolicyError>(resolved)))
}

/// A verified residency policy for `location`, with an id from `seed`.
pub fn residency(seed: u8, location: &str) -> VerifiedPolicy {
    let selector = aruna_core::structs::placement::policy::PlacementSelector {
        node_id: None,
        location: Some(location.to_string()),
        labels: Vec::new(),
        executor_kind: None,
    };
    let policy = aruna_core::structs::placement::policy::PlacementPolicy::new(
        Ulid::from_bytes([seed; 16]),
        "residency".to_string(),
        vec![selector],
    )
    .expect("policy is valid");
    VerifiedPolicy::verify(policy).expect("policy verifies")
}
