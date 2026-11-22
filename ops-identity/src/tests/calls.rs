//! Builds the results operations get from children and shard effects, and reads their calls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect, ShardNetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, ShardNetEvent, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::request_policy::{CompiledPolicySet, CompiledSets};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementStrategy;
use aruna_core::types::GroupId;
use aruna_core::user::id::UserId;
use ulid::Ulid;

pub const REALM: RealmId = RealmId([3u8; 32]);

pub fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub fn record_node(seed: u8) -> NodeId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    iroh::SecretKey::from_bytes(&bytes).public()
}

pub fn user() -> UserId {
    UserId::local(Ulid::from_bytes([7u8; 16]), REALM)
}

pub fn actor(node_id: NodeId) -> Actor {
    Actor {
        node_id,
        user_id: user(),
        realm_id: REALM,
    }
}

/// A child that ended with `result`.
pub fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + Debug + PartialEq + 'static,
    E: Send + Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

/// The call a turn asks for.
pub fn called<T: Debug, E: Debug>(turn: Result<Next<T>, E>) -> IoCall {
    match turn {
        Ok(Next::Call(call)) => call,
        other => panic!("expected a call, got {other:?}"),
    }
}

/// Whether a turn asks to run a child.
pub fn runs_child<T: Debug, E: Debug>(turn: Result<Next<T>, E>) -> bool {
    matches!(called(turn), IoCall::Child(_))
}

pub fn shard_event(event: ShardNetEvent) -> IoResult {
    let event = Event::Net(NetEvent::Shard(Box::new(event)));
    IoResult::Event(Box::new(event))
}

/// The shard effect a turn asks for.
pub fn shard_effect<T: Debug, E: Debug>(turn: Result<Next<T>, E>) -> ShardNetEffect {
    match called(turn) {
        IoCall::Effect(effect) => match *effect {
            Effect::Net(NetEffect::Shard(effect)) => *effect,
            other => panic!("expected a shard effect, got {other:?}"),
        },
        other => panic!("expected a shard effect, got {other:?}"),
    }
}

/// An unrestricted user of `realm_id`.
pub fn caller_of(realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::from_bytes([4; 16]), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

pub fn owned_group(group_id: GroupId, owner: &AuthContext) -> Group {
    Group {
        display_name: "group".to_string(),
        group_id,
        realm_id: owner.realm_id,
        roles: Default::default(),
        owner: owner.user_id,
    }
}

/// A realm with one activated single-bucket strategy held by server `nodes`.
pub fn bucket_realm(realm_id: RealmId, strategy_id: Ulid, nodes: &[NodeId]) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.strategies = vec![PlacementStrategy {
        strategy_id,
        name: "default".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 1,
    }];
    config.default_strategy_id = Some(strategy_id);
    for node_id in nodes {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config.snapshot_candidate_map();
    config
}

/// The storage handle's answer to a policy compile call, compiled here without its cache.
pub fn compiled(call: IoCall) -> IoResult {
    let sets = match call {
        IoCall::Effect(effect) => match *effect {
            Effect::Storage(StorageEffect::CompilePolicies(sets)) => sets,
            other => panic!("expected the policy compile, got {other:?}"),
        },
        other => panic!("expected the policy compile, got {other:?}"),
    };
    let sets = (sets.iter())
        .map(|set| CompiledPolicySet::compile(set).map(std::sync::Arc::new))
        .collect();
    let event = Event::Storage(StorageEvent::PoliciesCompiled(CompiledSets(sets)));
    IoResult::Event(Box::new(event))
}

/// A read answer with `values` in order.
pub fn values_of(values: Vec<Option<Vec<u8>>>) -> IoResult {
    IoResult::Values(
        values
            .into_iter()
            .map(|value| value.map(Into::into))
            .collect(),
    )
}

/// The peer and timeout of the metadata request a turn sends.
pub fn asked<T: Debug, E: Debug>(turn: Result<Next<T>, E>) -> (NodeId, std::time::Duration) {
    let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
        panic!("expected a peer request");
    };
    let Effect::Net(NetEffect::MetadataRequest(request)) = *effect else {
        panic!("expected a metadata request");
    };
    (request.peer, request.timeout)
}

/// A realm config with nodes 1 to 4 as servers and a default strategy, and a placement in it.
pub fn placed_config() -> (
    RealmConfigDocument,
    aruna_core::structs::placement::record::PlacementRef,
) {
    let mut config = RealmConfigDocument::new(RealmId::from_bytes([7u8; 32]), Vec::new(), 3);
    let strategy = PlacementStrategy {
        strategy_id: Ulid::from_bytes([4u8; 16]),
        name: "default".to_string(),
        replica_count: Some(2),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 64,
    };
    config.default_strategy_id = Some(strategy.strategy_id);
    config.strategies = vec![strategy.clone()];
    for seed in 1..=4u8 {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    let placement = aruna_core::structs::placement::record::PlacementRef {
        strategy_id: strategy.strategy_id,
        shard: 9,
    };
    (config, placement)
}
