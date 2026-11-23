//! Tests the pure DHT state machine: lookups, routing updates and value checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::test_support::{make_node, make_secret};
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::user::id::UserId;
use std::collections::BTreeMap;
use std::sync::Arc;

fn user_kinds(peer: NodeId) -> PeerKinds {
    Arc::new(parking_lot::RwLock::new(BTreeMap::from([(
        peer,
        RealmNodeKind::User {
            owner: UserId::nil(make_realm(1)),
        },
    )])))
}

fn make_realm(seed: u8) -> RealmId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    RealmId::from_bytes(bytes)
}

fn make_trace_context(seed: u8) -> DistributedTraceContext {
    DistributedTraceContext::new(
        format!("00-{seed:032x}-{seed:016x}-01"),
        Some(format!("test={seed}")),
    )
}

fn make_value(
    seed: u8,
    key: DhtKeyId,
    realm_id: RealmId,
    value: &[u8],
    expires_at: u64,
) -> StoredValue {
    make_version(seed, key, realm_id, value, expires_at, 1)
}

fn make_version(
    seed: u8,
    key: DhtKeyId,
    realm_id: RealmId,
    value: &[u8],
    expires_at: u64,
    revision: u64,
) -> StoredValue {
    let secret = make_secret(seed);
    let publisher = secret.public();
    let signed_data = signed_record_bytes(&key, &publisher, &realm_id, value, expires_at, revision);
    StoredValue {
        publisher,
        realm_id,
        value: value.to_vec(),
        expires_at,
        revision,
        signature: secret.sign(&signed_data),
    }
}

fn make_entry(
    seed: u8,
    key: DhtKeyId,
    realm_id: RealmId,
    value: &[u8],
    expires_at: u64,
) -> StoredEntry {
    let value = make_value(seed, key, realm_id, value, expires_at);
    StoredEntry {
        publisher: value.publisher,
        realm_id: value.realm_id,
        value: value.value,
        expires_at: value.expires_at,
        revision: value.revision,
        signature: value.signature,
        retain_until: value.expires_at,
    }
}

fn start_get(state: &mut DhtStateMachine, op_id: OpId, key: DhtKeyId, peer: NodeId) {
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));
    let effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));
    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::IoRequest(inner)
                if matches!(
                    &**inner,
                    DhtIoRequest::RpcRequest {
                        phase: RpcPhase::GetLookup,
                        peer: request_peer,
                        ..
                    } if *request_peer == peer
                )
        )
    }));
}

fn start_scoped(
    state: &mut DhtStateMachine,
    op_id: OpId,
    key: DhtKeyId,
    peer: NodeId,
    completion: DhtCompletion,
) {
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id,
        key,
        realm_filter: None,
        completion,
        trace_context: None,
    }));
    let _ = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));
}

#[test]
fn cancel_releases_op() {
    // A released operation drops its pending RPC bookkeeping, so a late
    // reply cannot resurrect it or reach the caller a second time.
    let local_secret = make_secret(150);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"cancel-releases-op");
    let peer = make_node(151);
    start_get(&mut state, 200, key, peer);
    assert!(state.contains_op(200));

    let released = state.step(DhtInput::Cmd(DhtCmd::Cancel {
        op_id: 200,
        error: DhtIoError::Timeout,
    }));
    assert!(matches!(
        released.as_slice(),
        [DhtEffect::Output(DhtOutput::Failed {
            op_id: 200,
            error: DhtIoError::Timeout
        })]
    ));
    assert!(!state.contains_op(200));

    let late = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 200,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![make_value(152, key, make_realm(1), b"late", 2_000)],
            closer_nodes: Vec::new(),
        },
    }));
    assert!(late.is_empty());
    assert!(!state.contains_op(200));

    // A second release is inert rather than a duplicate caller reply.
    let again = state.step(DhtInput::Cmd(DhtCmd::Cancel {
        op_id: 200,
        error: DhtIoError::Timeout,
    }));
    assert!(again.is_empty());
}

#[test]
fn first_usable_stops() {
    let local_secret = make_secret(153);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"first-usable-stops");
    let realm_id = make_realm(1);
    let peer = make_node(154);
    let publisher = make_node(155);
    start_scoped(
        &mut state,
        201,
        key,
        peer,
        DhtCompletion::FirstUsable {
            realm_id,
            publisher,
        },
    );

    let completed = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 201,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![make_value(155, key, realm_id, b"endpoint", 2_000)],
            closer_nodes: Vec::new(),
        },
    }));
    assert!(completed.iter().any(|effect| matches!(
        effect,
        DhtEffect::Output(DhtOutput::Completed {
            result: DhtOutputValue::GetValues { values, stats },
            ..
        }) if values.len() == 1
            && values[0].node_id == publisher
            && stats.completed_reason == GetCompletedReason::FirstUsable
    )));
    assert!(!state.contains_op(201));
}

#[test]
fn first_usable_filters() {
    // Wrong publisher, wrong realm, expired, and forged records may not
    // complete an expected-publisher lookup.
    let local_secret = make_secret(156);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"first-usable-filters");
    let realm_id = make_realm(1);
    let other_realm = make_realm(2);
    let peer = make_node(157);
    let publisher = make_node(158);
    start_scoped(
        &mut state,
        202,
        key,
        peer,
        DhtCompletion::FirstUsable {
            realm_id,
            publisher,
        },
    );

    let pending = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 202,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![
                make_value(159, key, realm_id, b"other-publisher", 2_000),
                make_value(158, key, other_realm, b"other-realm", 2_000),
                make_value(158, key, realm_id, b"expired", 900),
                {
                    let mut invalid = make_value(158, key, realm_id, b"invalid-signature", 2_000);
                    let attacker = make_secret(160);
                    let signed = signed_record_bytes(
                        &key,
                        &invalid.publisher,
                        &invalid.realm_id,
                        &invalid.value,
                        invalid.expires_at,
                        invalid.revision,
                    );
                    invalid.signature = attacker.sign(&signed);
                    invalid
                },
            ],
            closer_nodes: Vec::new(),
        },
    }));
    assert!(
        pending
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );
    assert!(state.contains_op(202));
}

#[test]
fn exhaustive_keeps_publishers() {
    // The default policy may not stop on the first verified record; every
    // publisher answered before frontier completion stays in the result.
    let local_secret = make_secret(160);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"exhaustive-keeps-publishers");
    let realm_id = make_realm(1);
    let first_peer = make_node(161);
    let second_peer = make_node(162);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
        node_id: first_peer,
    }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
        node_id: second_peer,
    }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 203,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));
    let _ = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 203,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));

    let first = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 203,
        phase: RpcPhase::GetLookup,
        peer: first_peer,
        response: DhtResponse::Value {
            entries: vec![make_value(163, key, realm_id, b"first", 2_000)],
            closer_nodes: Vec::new(),
        },
    }));
    assert!(
        first
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );

    let pending = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 203,
        phase: RpcPhase::GetLookup,
        peer: second_peer,
        response: DhtResponse::Value {
            entries: vec![make_value(164, key, realm_id, b"second", 2_000)],
            closer_nodes: Vec::new(),
        },
    }));
    let complete = finish_get(&mut state, 203, &pending);
    assert!(complete.iter().any(|effect| matches!(
        effect,
        DhtEffect::Output(DhtOutput::Completed {
            result: DhtOutputValue::GetValues { values, stats },
            ..
        }) if values.len() == 2
            && stats.completed_reason == GetCompletedReason::RemoteValue
    )));
}

fn finish_get(
    state: &mut DhtStateMachine,
    op_id: OpId,
    effects: &[DhtEffect],
) -> SmallVec<[DhtEffect; 4]> {
    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::IoRequest(inner)
                if matches!(&**inner, DhtIoRequest::StorageMerge {
                    stage: StorageStage::GetRemoteMerge,
                    ..
                })
        )
    }));
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );
    state.step(DhtInput::Io(DhtIo::StorageWriteResult {
        op_id,
        stage: StorageStage::GetRemoteMerge,
    }))
}

fn start_put(state: &mut DhtStateMachine, op_id: OpId, key: DhtKeyId) -> SmallVec<[DhtEffect; 4]> {
    let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
        op_id,
        key,
        realm_id: make_realm(1),
        value: b"value".to_vec(),
        ttl: std::time::Duration::from_secs(60),
        trace_context: None,
    }));
    let _ = reserve_put(state, op_id);
    state.step(DhtInput::Io(DhtIo::StorageWriteResult {
        op_id,
        stage: StorageStage::PutLocalMerge,
    }))
}

fn reserve_put(state: &mut DhtStateMachine, op_id: OpId) -> SmallVec<[DhtEffect; 4]> {
    state.step(DhtInput::Io(DhtIo::StorageRevisionResult {
        op_id,
        stage: StorageStage::PutRevision,
        revision: op_id.max(1),
    }))
}

fn bucket_nodes(local_id: NodeId, count: usize) -> Vec<NodeId> {
    let mut buckets: HashMap<usize, Vec<NodeId>> = HashMap::new();
    for seed in 1u64..100_000 {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        let node = iroh::SecretKey::from_bytes(&bytes).public();
        let bucket = local_id.bucket_index(&node);
        if bucket >= 256 {
            continue;
        }
        let nodes = buckets.entry(bucket).or_default();
        nodes.push(node);
        if nodes.len() == count {
            return nodes.clone();
        }
    }
    panic!("failed to generate peers for one bucket");
}

#[test]
fn forged_value_rejected() {
    let local_secret = make_secret(70);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"forged-value");
    let peer = make_node(71);
    start_get(&mut state, 70, key, peer);

    let mut forged = make_value(72, key, make_realm(1), b"authentic", 2_000);
    forged.value = b"forged".to_vec();
    let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 70,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![forged],
            closer_nodes: Vec::new(),
        },
    }));

    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Failed { op_id: 70, .. })
        )
    }));
}

#[test]
fn expiry_tamper_rejected() {
    let local_secret = make_secret(73);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"expiry-tamper");
    let peer = make_node(74);
    start_get(&mut state, 71, key, peer);

    let mut forged = make_value(75, key, make_realm(1), b"value", 2_000);
    forged.expires_at = 3_000;
    let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 71,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![forged],
            closer_nodes: Vec::new(),
        },
    }));

    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Failed { op_id: 71, .. })
        )
    }));
}

#[test]
fn revision_tamper_rejected() {
    let local_secret = make_secret(91);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"revision-tamper");
    let peer = make_node(92);
    start_get(&mut state, 79, key, peer);

    let mut forged = make_value(93, key, make_realm(1), b"value", 2_000);
    forged.revision = forged.revision.saturating_add(1);
    let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 79,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![forged],
            closer_nodes: Vec::new(),
        },
    }));

    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Failed { op_id: 79, .. })]
    ));
}

#[test]
fn get_fails_unavailable() {
    let local_secret = make_secret(76);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"unavailable-get");
    let peer = make_node(77);
    start_get(&mut state, 72, key, peer);

    let effects = state.step(DhtInput::Io(DhtIo::RpcError {
        op_id: 72,
        phase: RpcPhase::GetLookup,
        peer,
        error: DhtIoError::Timeout,
    }));
    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Failed {
            op_id: 72,
            error: DhtIoError::Network(_)
        })]
    ));
}

#[test]
fn get_empty_succeeds() {
    let local_secret = make_secret(78);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"empty-get");
    let peer = make_node(79);
    start_get(&mut state, 73, key, peer);

    let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 73,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: Vec::new(),
            closer_nodes: Vec::new(),
        },
    }));
    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Completed {
            result: DhtOutputValue::GetValues { values, .. },
            ..
        })] if values.is_empty()
    ));
}

#[test]
fn get_storage_fallback() {
    let local_secret = make_secret(83);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"storage-fallback");
    let peer = make_node(84);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 80,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));

    let effects = state.step(DhtInput::Io(DhtIo::StorageError {
        op_id: 80,
        stage: StorageStage::GetLocalRead,
        error: DhtIoError::storage("local read failed"),
    }));
    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::IoRequest(request)]
            if matches!(
                &**request,
                DhtIoRequest::RpcRequest {
                    phase: RpcPhase::GetLookup,
                    peer: request_peer,
                    ..
                } if *request_peer == peer
            )
    ));

    let value = make_value(85, key, make_realm(1), b"remote", 2_000);
    let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 80,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![value],
            closer_nodes: Vec::new(),
        },
    }));
    let effects = finish_get(&mut state, 80, &effects);
    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Completed {
            result: DhtOutputValue::GetValues { values, .. },
            ..
        })] if values.len() == 1 && values[0].value == b"remote"
    ));
}

#[test]
fn old_floor_rejected() {
    let local_secret = make_secret(86);
    let now_secs = MAX_TTL_SECS + MAX_CLOCK_SKEW + 1_000;
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, now_secs);
    let key = DhtKeyId::from_data(b"old-remote-floor");
    let peer = make_node(87);
    start_get(&mut state, 81, key, peer);

    let floor = make_version(
        88,
        key,
        make_realm(1),
        b"expired",
        now_secs - MAX_TTL_SECS - MAX_CLOCK_SKEW,
        2,
    );
    let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 81,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![floor],
            closer_nodes: Vec::new(),
        },
    }));

    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Failed { op_id: 81, .. })]
    ));
    assert!(effects.iter().all(|effect| !matches!(
        effect,
        DhtEffect::IoRequest(inner)
            if matches!(&**inner, DhtIoRequest::StorageMerge { .. })
    )));
}

#[test]
fn remote_floor_wins() {
    let local_secret = make_secret(89);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"remote-floor-wins");
    let realm_id = make_realm(1);
    let first_peer = make_node(90);
    let second_peer = make_node(91);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
        node_id: first_peer,
    }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
        node_id: second_peer,
    }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 82,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));
    let _ = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 82,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));

    let first = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 82,
        phase: RpcPhase::GetLookup,
        peer: first_peer,
        response: DhtResponse::Value {
            entries: vec![make_version(92, key, realm_id, b"live", 2_000, 1)],
            closer_nodes: Vec::new(),
        },
    }));
    assert!(
        first
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );

    let pending = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 82,
        phase: RpcPhase::GetLookup,
        peer: second_peer,
        response: DhtResponse::Value {
            entries: vec![make_version(92, key, realm_id, b"floor", 900, 2)],
            closer_nodes: Vec::new(),
        },
    }));
    assert!(pending.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::IoRequest(inner)
                if matches!(&**inner, DhtIoRequest::StorageMerge {
                    stage: StorageStage::GetRemoteMerge,
                    entries,
                    ..
                } if entries.len() == 1
                    && entries[0].revision == 2
                    && entries[0].expires_at == 900)
        )
    }));

    let complete = finish_get(&mut state, 82, &pending);
    assert!(matches!(
        complete.as_slice(),
        [DhtEffect::Output(DhtOutput::Completed {
            result: DhtOutputValue::GetValues { values, stats },
            ..
        })] if values.is_empty()
            && stats.completed_reason == GetCompletedReason::LookupExhausted
            && stats.local_value_count == 0
            && stats.remote_value_count == 0
    ));
}

#[test]
fn writeback_error_completes() {
    // A failed read-repair write-back (here a full store) still returns the
    // signature-validated values the lookup already collected.
    let local_secret = make_secret(93);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"remote-cache-failure");
    let peer = make_node(94);
    start_get(&mut state, 83, key, peer);

    let pending = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 83,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![make_value(95, key, make_realm(1), b"remote", 2_000)],
            closer_nodes: Vec::new(),
        },
    }));
    assert!(pending.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::IoRequest(inner)
                if matches!(&**inner, DhtIoRequest::StorageMerge {
                    stage: StorageStage::GetRemoteMerge,
                    ..
                })
        )
    }));
    assert!(
        pending
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );

    let effects = state.step(DhtInput::Io(DhtIo::StorageError {
        op_id: 83,
        stage: StorageStage::GetRemoteMerge,
        error: DhtIoError::StorageFull,
    }));
    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Completed {
            op_id: 83,
            result: DhtOutputValue::GetValues { values, stats },
        })] if values.len() == 1
            && values[0].value == b"remote".to_vec()
            && stats.completed_reason == GetCompletedReason::RemoteValue
            && stats.remote_value_count == 1
    ));
}

#[test]
fn writeback_stale_completes() {
    // A concurrent inbound put can advance the stored revision past the fetched
    // one, so the write-back merge fails as stale; the get must still complete.
    let local_secret = make_secret(96);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"remote-cache-stale");
    let peer = make_node(97);
    start_get(&mut state, 86, key, peer);

    let pending = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 86,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![make_value(98, key, make_realm(1), b"remote", 2_000)],
            closer_nodes: Vec::new(),
        },
    }));
    assert!(
        pending
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );

    let effects = state.step(DhtInput::Io(DhtIo::StorageError {
        op_id: 86,
        stage: StorageStage::GetRemoteMerge,
        error: DhtIoError::invalid_request("record version is stale or conflicting"),
    }));
    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Completed {
            op_id: 86,
            result: DhtOutputValue::GetValues { values, .. },
        })] if values.len() == 1 && values[0].value == b"remote".to_vec()
    ));
}

#[test]
fn equal_conflict_fails() {
    let local_secret = make_secret(99);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"equal-revision-conflict");
    let realm_id = make_realm(1);
    let first_peer = make_node(100);
    let second_peer = make_node(101);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
        node_id: first_peer,
    }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
        node_id: second_peer,
    }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 84,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));
    let _ = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 84,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));

    let _ = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 84,
        phase: RpcPhase::GetLookup,
        peer: first_peer,
        response: DhtResponse::Value {
            entries: vec![make_value(102, key, realm_id, b"first", 2_000)],
            closer_nodes: Vec::new(),
        },
    }));
    let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 84,
        phase: RpcPhase::GetLookup,
        peer: second_peer,
        response: DhtResponse::Value {
            entries: vec![make_value(102, key, realm_id, b"second", 2_100)],
            closer_nodes: Vec::new(),
        },
    }));

    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Failed {
            op_id: 84,
            error: DhtIoError::InvalidResponse(_)
        })]
    ));
}

#[test]
fn cache_capacity_fails() {
    let local_secret = make_secret(103);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"cache-capacity-bound");
    let peer = make_node(104);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 85,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));

    let entries = (0..super::super::constants::ENTRIES_PER_KEY)
        .map(|seed| make_entry(seed as u8, key, make_realm(seed as u8), b"local", 2_000))
        .collect();
    let _ = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 85,
        stage: StorageStage::GetLocalRead,
        entries,
    }));

    let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 85,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![make_value(0, key, make_realm(1), b"remote", 2_000)],
            closer_nodes: Vec::new(),
        },
    }));
    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Failed {
            op_id: 85,
            error: DhtIoError::StorageFull
        })]
    ));
}

#[test]
fn tick_sets_expiry() {
    let local_secret = make_secret(80);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let _ = state.step(DhtInput::Tick {
        now_tick: 1,
        now_secs: 10_000,
    });

    let key = DhtKeyId::from_data(b"sampled-time");
    let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
        op_id: 74,
        key,
        realm_id: make_realm(1),
        value: b"value".to_vec(),
        ttl: std::time::Duration::from_secs(60),
        trace_context: None,
    }));
    let effects = reserve_put(&mut state, 74);
    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::IoRequest(inner)
                if matches!(
                    &**inner,
                    DhtIoRequest::StorageMerge { entries, .. }
                        if entries.len() == 1 && entries[0].expires_at == 10_060
                )
        )
    }));
}

#[test]
fn oversized_put_rejected() {
    let local_secret = make_secret(81);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let effects = state.step(DhtInput::Cmd(DhtCmd::Put {
        op_id: 75,
        key: DhtKeyId::from_data(b"oversized-put"),
        realm_id: make_realm(1),
        value: vec![0; MAX_VALUE_SIZE + 1],
        ttl: std::time::Duration::from_secs(60),
        trace_context: None,
    }));

    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Failed {
            op_id: 75,
            error: DhtIoError::InvalidRequest(_)
        })]
    ));
}

#[test]
fn long_ttl_rejected() {
    let local_secret = make_secret(82);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let effects = state.step(DhtInput::Cmd(DhtCmd::Put {
        op_id: 76,
        key: DhtKeyId::from_data(b"long-ttl"),
        realm_id: make_realm(1),
        value: b"value".to_vec(),
        ttl: std::time::Duration::from_secs(MAX_TTL_SECS + 1),
        trace_context: None,
    }));

    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Failed {
            op_id: 76,
            error: DhtIoError::InvalidRequest(_)
        })]
    ));
}

#[test]
fn eviction_probe_serialized() {
    let local_secret = make_secret(83);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
    let nodes = bucket_nodes(local_id, K + 2);
    for node_id in &nodes[..K] {
        assert!(
            state
                .step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: *node_id }))
                .is_empty()
        );
    }

    let first = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[K] }));
    assert_eq!(
        first
            .iter()
            .filter(|effect| matches!(
                effect,
                DhtEffect::IoRequest(inner)
                    if matches!(&**inner, DhtIoRequest::RpcRequest {
                        phase: RpcPhase::EvictionPing,
                        ..
                    })
            ))
            .count(),
        1
    );

    let duplicate = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
        node_id: nodes[K + 1],
    }));
    assert!(duplicate.is_empty());
}

#[test]
fn pong_refreshes_lru() {
    let local_secret = make_secret(84);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
    let nodes = bucket_nodes(local_id, K + 2);
    for node_id in &nodes[..K] {
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: *node_id }));
    }

    let effects = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[K] }));
    let op_id = effects
        .iter()
        .find_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    op_id,
                    phase: RpcPhase::EvictionPing,
                    peer,
                    ..
                } = &**inner
                && *peer == nodes[0]
            {
                Some(*op_id)
            } else {
                None
            }
        })
        .expect("eviction probe");
    let _ = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id,
        phase: RpcPhase::EvictionPing,
        peer: nodes[0],
        response: DhtResponse::Pong,
    }));

    let next = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
        node_id: nodes[K + 1],
    }));
    assert!(next.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::IoRequest(inner)
                if matches!(&**inner, DhtIoRequest::RpcRequest {
                    phase: RpcPhase::EvictionPing,
                    peer,
                    ..
                } if *peer == nodes[1])
        )
    }));
}

#[test]
fn eviction_removes_probed() {
    let local_secret = make_secret(85);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
    let nodes = bucket_nodes(local_id, K + 1);
    for node_id in &nodes[..K] {
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: *node_id }));
    }

    let effects = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[K] }));
    let op_id = effects
        .iter()
        .find_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    op_id,
                    phase: RpcPhase::EvictionPing,
                    ..
                } = &**inner
            {
                Some(*op_id)
            } else {
                None
            }
        })
        .expect("eviction probe");
    let _ = state.step(DhtInput::Io(DhtIo::RpcError {
        op_id,
        phase: RpcPhase::EvictionPing,
        peer: nodes[0],
        error: DhtIoError::Timeout,
    }));

    let peers: HashSet<NodeId> = state
        .routing_table
        .all_peers()
        .into_iter()
        .map(|peer| peer.node_id)
        .collect();
    assert_eq!(peers.len(), K);
    assert!(!peers.contains(&nodes[0]));
    assert!(peers.contains(&nodes[1]));
    assert!(peers.contains(&nodes[K]));
}

#[test]
fn stale_probe_preserves() {
    let local_secret = make_secret(90);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
    let nodes = bucket_nodes(local_id, K + 1);
    for node_id in &nodes[..K] {
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: *node_id }));
    }

    let effects = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[K] }));
    let op_id = effects
        .iter()
        .find_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    op_id,
                    phase: RpcPhase::EvictionPing,
                    ..
                } = &**inner
            {
                Some(*op_id)
            } else {
                None
            }
        })
        .expect("eviction probe");
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[0] }));
    let _ = state.step(DhtInput::Io(DhtIo::RpcError {
        op_id,
        phase: RpcPhase::EvictionPing,
        peer: nodes[0],
        error: DhtIoError::Timeout,
    }));

    let peers: HashSet<NodeId> = state
        .routing_table
        .all_peers()
        .into_iter()
        .map(|peer| peer.node_id)
        .collect();
    assert_eq!(peers.len(), K);
    assert!(peers.contains(&nodes[0]));
    assert!(!peers.contains(&nodes[K]));
}

#[test]
fn eviction_aba_preserves() {
    let local_secret = make_secret(91);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
    let nodes = bucket_nodes(local_id, K + 1);
    for node_id in &nodes[..K] {
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: *node_id }));
    }

    let effects = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[K] }));
    let op_id = effects
        .iter()
        .find_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    op_id,
                    phase: RpcPhase::EvictionPing,
                    ..
                } = &**inner
            {
                Some(*op_id)
            } else {
                None
            }
        })
        .expect("eviction probe");

    let _ = state.routing_table.remove(&nodes[0]);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: nodes[0] }));
    let _ = state.step(DhtInput::Io(DhtIo::RpcError {
        op_id,
        phase: RpcPhase::EvictionPing,
        peer: nodes[0],
        error: DhtIoError::Timeout,
    }));

    let peers = state.routing_table.all_peers();
    assert!(peers.iter().any(|peer| peer.node_id == nodes[0]));
    assert!(!peers.iter().any(|peer| peer.node_id == nodes[K]));
}

#[test]
fn expiry_breaks_ties() {
    let local_secret = make_secret(92);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
    let key = DhtKeyId::from_data(b"expiry-tie-break");
    let realm_id = make_realm(1);
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 91,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));
    let OpState::Get(mut op) = state.ops.remove(&91).expect("get operation") else {
        panic!("expected get operation");
    };

    let shorter = make_value(93, key, realm_id, b"same", 1_500);
    let longer = make_value(93, key, realm_id, b"same", 1_600);
    assert!(insert_get_value(&mut op, shorter.clone()));
    assert!(insert_get_value(&mut op, longer));
    assert!(!insert_get_value(&mut op, shorter));
    assert_eq!(op.values[0].expires_at, 1_600);
}

#[test]
fn put_uses_responders() {
    let local_secret = make_secret(86);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
    let first = make_node(87);
    let second = make_node(88);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: first }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: second }));
    let key = DhtKeyId::from_data(b"responsive-targets");
    let lookup = start_put(&mut state, 77, key);
    let lookup_peers: Vec<NodeId> = lookup
        .iter()
        .filter_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    phase: RpcPhase::PutLookup,
                    peer,
                    ..
                } = &**inner
            {
                Some(*peer)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(lookup_peers.len(), 2);

    let responsive = lookup_peers[0];
    let failed = lookup_peers[1];
    let _ = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 77,
        phase: RpcPhase::PutLookup,
        peer: responsive,
        response: DhtResponse::Nodes { nodes: Vec::new() },
    }));
    let store = state.step(DhtInput::Io(DhtIo::RpcError {
        op_id: 77,
        phase: RpcPhase::PutLookup,
        peer: failed,
        error: DhtIoError::Timeout,
    }));

    let store_peers: Vec<NodeId> = store
        .iter()
        .filter_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest {
                    phase: RpcPhase::PutStore,
                    peer,
                    ..
                } = &**inner
            {
                Some(*peer)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(store_peers, vec![responsive]);
}

#[test]
fn put_uses_fallbacks() {
    let local_secret = make_secret(89);
    let local_id = local_secret.public();
    let state = DhtStateMachine::new(local_id, local_secret.clone(), 1_000);
    let key = DhtKeyId::from_data(b"store-fallbacks");
    let realm_id = make_realm(1);
    let value = b"value".to_vec();
    let expires_at = 2_000;
    let revision = 1;
    let signed_data = signed_record_bytes(&key, &local_id, &realm_id, &value, expires_at, revision);
    let mut op = PutOp {
        key,
        realm_id,
        value,
        expires_at,
        revision: Some(revision),
        signature: Some(local_secret.sign(&signed_data)),
        frontier: LookupFrontier::default(),
        local_write_complete: true,
        lookup_finished: true,
        store_candidates: (1..=K + 1).map(|seed| make_node(seed as u8)).collect(),
        store_attempt_count: 0,
        store_count: 0,
        trace_context: None,
        pending: HashMap::new(),
    };

    let mut initial = SmallVec::new();
    state.queue_put_stores(78, &mut op, &mut initial);
    assert_eq!(pending_rpc_count(&op.pending, RpcPhase::PutStore), K);
    let failed = initial
        .iter()
        .find_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::RpcRequest { peer, .. } = &**inner
            {
                Some(*peer)
            } else {
                None
            }
        })
        .expect("initial store target");
    assert!(take_pending_rpc(
        &mut op.pending,
        RpcPhase::PutStore,
        failed
    ));

    let mut fallback = SmallVec::new();
    state.queue_put_stores(78, &mut op, &mut fallback);
    assert_eq!(fallback.len(), 1);
    assert_eq!(op.store_attempt_count, K + 1);
}

#[test]
fn lookups_seed_fallbacks() {
    let local_secret = make_secret(98);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);
    for seed in 1..=K + 1 {
        let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
            node_id: make_node(seed as u8),
        }));
    }
    assert_eq!(state.routing_table.all_peers().len(), K + 1);

    let get_key = DhtKeyId::from_data(b"get-known-fallbacks");
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 79,
        key: get_key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));
    let _ = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 79,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));
    let OpState::Get(get) = state.ops.get(&79).expect("get operation") else {
        panic!("expected get operation");
    };
    assert_eq!(get.frontier.discovered.len(), K + 1);

    let put_key = DhtKeyId::from_data(b"put-known-fallbacks");
    let _ = start_put(&mut state, 80, put_key);
    let OpState::Put(put) = state.ops.get(&80).expect("put operation") else {
        panic!("expected put operation");
    };
    assert_eq!(put.frontier.discovered.len(), K + 1);
}

#[test]
fn frontier_stays_bounded() {
    let target = [0u8; 32];
    let local_id = make_node(255);
    let mut nodes = (1..=LOOKUP_MAX_QUERIES + 1)
        .map(|seed| make_node(seed as u8))
        .collect::<Vec<_>>();
    nodes.sort_unstable_by(|left, right| compare_distance(&target, left, right));
    let closest = nodes[0];
    let mut frontier = LookupFrontier::default();
    frontier.add_candidates(nodes.into_iter().skip(1), local_id, &target);
    assert_eq!(frontier.discovered.len(), LOOKUP_MAX_QUERIES);

    frontier.add_candidates([closest], local_id, &target);

    assert_eq!(frontier.discovered.len(), LOOKUP_MAX_QUERIES);
    assert!(frontier.discovered.contains(&closest));
}

#[test]
fn step_smallvec_four() {
    let local_secret = iroh::SecretKey::from_bytes(&[7u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let effects = state.step(DhtInput::Cmd(DhtCmd::RoutingTableSize { op_id: 1 }));

    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Completed {
            op_id: 1,
            result: DhtOutputValue::RoutingTableSize(0)
        })]
    ));
}

#[test]
fn add_peer_noop() {
    let local_secret = iroh::SecretKey::from_bytes(&[11u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let peer = make_node(1);
    let effects = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
    assert!(effects.is_empty());
}

#[test]
fn deterministic_replay_effects() {
    let local_secret_a = iroh::SecretKey::from_bytes(&[21u8; 32]);
    let local_id_a = local_secret_a.public();
    let local_secret_b = iroh::SecretKey::from_bytes(&[21u8; 32]);
    let local_id_b = local_secret_b.public();

    let mut state_a = DhtStateMachine::new(local_id_a, local_secret_a, 100);
    let mut state_b = DhtStateMachine::new(local_id_b, local_secret_b, 100);

    let key = DhtKeyId::from_data(b"deterministic-key");
    let trace = [
        DhtInput::Cmd(DhtCmd::Put {
            op_id: 1,
            key,
            realm_id: make_realm(1),
            value: b"hello".to_vec(),
            ttl: std::time::Duration::from_secs(30),
            trace_context: None,
        }),
        DhtInput::Io(DhtIo::StorageRevisionResult {
            op_id: 1,
            stage: StorageStage::PutRevision,
            revision: 1,
        }),
        DhtInput::Io(DhtIo::StorageWriteResult {
            op_id: 1,
            stage: StorageStage::PutLocalMerge,
        }),
    ];

    let outputs_a: Vec<String> = trace
        .iter()
        .map(|input| format!("{:?}", state_a.step(input.clone())))
        .collect();
    let outputs_b: Vec<String> = trace
        .iter()
        .map(|input| format!("{:?}", state_b.step(input.clone())))
        .collect();

    assert_eq!(outputs_a, outputs_b);
}

#[test]
fn late_response_ignored() {
    let local_secret = iroh::SecretKey::from_bytes(&[23u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let peer = make_node(42);
    let effects = state.step(DhtInput::Cmd(DhtCmd::Bootstrap {
        op_id: 5,
        nodes: vec![peer],
        trace_context: None,
    }));

    assert!(effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::RpcRequest {
                    op_id,
                    phase: RpcPhase::Bootstrap,
                    peer: req_peer,
                    ..
                }
             if op_id == 5 && req_peer == peer),
            _ => false,
        }
    }));

    let timeout_effects = state.step(DhtInput::Tick {
        now_tick: RPC_TIMEOUT_TICKS,
        now_secs: 0,
    });
    assert!(timeout_effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Failed {
                op_id: 5,
                error: DhtIoError::Timeout
            })
        )
    }));

    let late_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 5,
        phase: RpcPhase::Bootstrap,
        peer,
        response: DhtResponse::Pong,
    }));
    assert!(late_effects.is_empty());
}

#[test]
fn get_uses_closer() {
    let local_secret = iroh::SecretKey::from_bytes(&[31u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let first_peer = make_node(1);
    let second_peer = make_node(2);
    let key = DhtKeyId::from_data(b"iterative-get");

    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
        node_id: first_peer,
    }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 9,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));

    let local_read_effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 9,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));

    assert!(local_read_effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::RpcRequest {
                    op_id,
                    phase: RpcPhase::GetLookup,
                    peer,
                    request: DhtRequest::GetValue { .. },
                    ..
                } if op_id == 9 && peer == first_peer),
            _ => false,
        }
    }));

    let follow_up = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 9,
        phase: RpcPhase::GetLookup,
        peer: first_peer,
        response: DhtResponse::Value {
            entries: Vec::new(),
            closer_nodes: vec![second_peer],
        },
    }));

    assert!(follow_up.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::RpcRequest {
                    op_id,
                    phase: RpcPhase::GetLookup,
                    peer,
                    request: DhtRequest::GetValue { .. },
                    ..
            } if op_id == 9 && peer == second_peer),
            _ => false,
        }
    }));
}

#[test]
fn get_propagates_trace() {
    let local_secret = iroh::SecretKey::from_bytes(&[32u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let peer = make_node(3);
    let key = DhtKeyId::from_data(b"trace-context-get");
    let trace_context = make_trace_context(1);

    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 10,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: Some(trace_context.clone()),
    }));

    let effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 10,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));

    let propagated = effects.iter().find_map(|effect| {
        if let DhtEffect::IoRequest(inner) = effect
            && let DhtIoRequest::RpcRequest {
                phase: RpcPhase::GetLookup,
                trace_context,
                ..
            } = &(**inner)
        {
            trace_context.clone()
        } else {
            None
        }
    });

    assert_eq!(propagated, Some(trace_context));
}

#[test]
fn bootstrap_propagates_trace() {
    let local_secret = iroh::SecretKey::from_bytes(&[33u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let peer = make_node(4);
    let trace_context = make_trace_context(2);
    let effects = state.step(DhtInput::Cmd(DhtCmd::Bootstrap {
        op_id: 10,
        nodes: vec![peer],
        trace_context: Some(trace_context.clone()),
    }));

    let propagated = effects.iter().find_map(|effect| {
        if let DhtEffect::IoRequest(inner) = effect
            && let DhtIoRequest::RpcRequest {
                phase: RpcPhase::Bootstrap,
                trace_context,
                ..
            } = &(**inner)
        {
            trace_context.clone()
        } else {
            None
        }
    });

    assert_eq!(propagated, Some(trace_context));
}

#[test]
fn put_reports_local() {
    let local_secret = iroh::SecretKey::from_bytes(&[41u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

    let key = DhtKeyId::from_data(b"strict-put-no-targets");
    let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
        op_id: 11,
        key,
        realm_id: make_realm(1),
        value: b"value".to_vec(),
        ttl: std::time::Duration::from_secs(60),
        trace_context: None,
    }));
    let _ = reserve_put(&mut state, 11);
    let effects = state.step(DhtInput::Io(DhtIo::StorageWriteResult {
        op_id: 11,
        stage: StorageStage::PutLocalMerge,
    }));

    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Completed {
                op_id: 11,
                result: DhtOutputValue::PutStored { stats }
            }) if stats.remote_attempt_count == 0 && stats.remote_store_count == 0
        )
    }));
}

#[test]
fn zero_ttl_clamped() {
    let local_secret = iroh::SecretKey::from_bytes(&[61u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

    let key = DhtKeyId::from_data(b"zero-ttl-local-put");
    let realm_id = make_realm(1);
    let value = b"value".to_vec();
    let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
        op_id: 24,
        key,
        realm_id,
        value: value.clone(),
        ttl: std::time::Duration::ZERO,
        trace_context: None,
    }));
    let effects = reserve_put(&mut state, 24);

    let entry = effects
        .iter()
        .find_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::StorageMerge {
                    stage: StorageStage::PutLocalMerge,
                    entries,
                    ..
                } = &(**inner)
            {
                entries.first()
            } else {
                None
            }
        })
        .expect("put should enqueue atomic local storage merge");
    assert_eq!(entry.expires_at, 1_000 + MIN_TTL_SECS);

    let signed_data = signed_record_bytes(
        &key,
        &local_id,
        &realm_id,
        &value,
        1_000 + MIN_TTL_SECS,
        entry.revision,
    );
    assert!(local_id.verify(&signed_data, &entry.signature).is_ok());
}

#[test]
fn put_reports_timeout() {
    let local_secret = iroh::SecretKey::from_bytes(&[42u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 100);

    let peer = make_node(4);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));

    let key = DhtKeyId::from_data(b"strict-put-timeout");
    let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
        op_id: 12,
        key,
        realm_id: make_realm(1),
        value: b"value".to_vec(),
        ttl: std::time::Duration::from_secs(60),
        trace_context: None,
    }));
    let _ = reserve_put(&mut state, 12);
    let write_effects = state.step(DhtInput::Io(DhtIo::StorageWriteResult {
        op_id: 12,
        stage: StorageStage::PutLocalMerge,
    }));

    assert!(
        write_effects
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );

    assert!(write_effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::RpcRequest {
                    op_id: 12,
                    phase: RpcPhase::PutLookup,
                    peer: req_peer,
                    ..
                } if req_peer == peer),
            _ => false,
        }
    }));

    let store_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 12,
        phase: RpcPhase::PutLookup,
        peer,
        response: DhtResponse::Nodes { nodes: Vec::new() },
    }));

    assert!(store_effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::RpcRequest {
                    op_id: 12,
                    phase: RpcPhase::PutStore,
                    peer: req_peer,
                    ..
                } if req_peer == peer),
            _ => false,
        }
    }));

    let fail_effects = state.step(DhtInput::Io(DhtIo::RpcError {
        op_id: 12,
        phase: RpcPhase::PutStore,
        peer,
        error: DhtIoError::Timeout,
    }));

    assert!(fail_effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Completed {
                op_id: 12,
                result: DhtOutputValue::PutStored { stats }
            }) if stats.remote_attempt_count == 1 && stats.remote_store_count == 0
        )
    }));
}

#[test]
fn put_reports_rejection() {
    let local_secret = iroh::SecretKey::from_bytes(&[43u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 100);

    let peer = make_node(5);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));

    let key = DhtKeyId::from_data(b"strict-put-non-stored");
    let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
        op_id: 13,
        key,
        realm_id: make_realm(1),
        value: b"value".to_vec(),
        ttl: std::time::Duration::from_secs(60),
        trace_context: None,
    }));
    let _ = reserve_put(&mut state, 13);
    let write_effects = state.step(DhtInput::Io(DhtIo::StorageWriteResult {
        op_id: 13,
        stage: StorageStage::PutLocalMerge,
    }));

    assert!(
        write_effects
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );

    let _ = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 13,
        phase: RpcPhase::PutLookup,
        peer,
        response: DhtResponse::Nodes { nodes: Vec::new() },
    }));

    let fail_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 13,
        phase: RpcPhase::PutStore,
        peer,
        response: DhtResponse::Error {
            code: ErrorCode::Internal,
            message: "store failed".to_string(),
        },
    }));

    assert!(fail_effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Completed {
                op_id: 13,
                result: DhtOutputValue::PutStored { stats }
            }) if stats.remote_attempt_count == 1 && stats.remote_store_count == 0
        )
    }));
}

#[test]
fn put_reports_replication() {
    let local_secret = iroh::SecretKey::from_bytes(&[44u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 100);

    let peer_a = make_node(6);
    let peer_b = make_node(7);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_a }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_b }));

    let key = DhtKeyId::from_data(b"strict-put-first-ack");
    let _ = state.step(DhtInput::Cmd(DhtCmd::Put {
        op_id: 14,
        key,
        realm_id: make_realm(1),
        value: b"value".to_vec(),
        ttl: std::time::Duration::from_secs(60),
        trace_context: None,
    }));
    let _ = reserve_put(&mut state, 14);
    let write_effects = state.step(DhtInput::Io(DhtIo::StorageWriteResult {
        op_id: 14,
        stage: StorageStage::PutLocalMerge,
    }));

    let mut lookup_peers = Vec::new();
    for effect in &write_effects {
        if let DhtEffect::IoRequest(inner) = effect
            && let DhtIoRequest::RpcRequest {
                op_id: 14,
                phase: RpcPhase::PutLookup,
                peer,
                ..
            } = **inner
        {
            lookup_peers.push(peer);
        }
    }
    assert!(
        lookup_peers.len() >= 2,
        "expected lookup fanout to both peers"
    );
    assert!(
        write_effects
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );

    let _ = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 14,
        phase: RpcPhase::PutLookup,
        peer: lookup_peers[0],
        response: DhtResponse::Nodes { nodes: Vec::new() },
    }));
    let store_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 14,
        phase: RpcPhase::PutLookup,
        peer: lookup_peers[1],
        response: DhtResponse::Nodes { nodes: Vec::new() },
    }));

    let mut store_peers = Vec::new();
    for effect in &store_effects {
        if let DhtEffect::IoRequest(inner) = effect
            && let DhtIoRequest::RpcRequest {
                op_id: 14,
                phase: RpcPhase::PutStore,
                peer,
                ..
            } = **inner
        {
            store_peers.push(peer);
        }
    }
    assert!(
        store_peers.len() >= 2,
        "expected store fanout to both peers"
    );

    let ack_peer = store_peers[0];
    let late_peer = *store_peers
        .iter()
        .find(|peer| **peer != ack_peer)
        .expect("expected second pending store peer");

    let complete_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 14,
        phase: RpcPhase::PutStore,
        peer: ack_peer,
        response: DhtResponse::Stored,
    }));
    assert!(complete_effects.is_empty());

    let late_effects = state.step(DhtInput::Io(DhtIo::RpcError {
        op_id: 14,
        phase: RpcPhase::PutStore,
        peer: late_peer,
        error: DhtIoError::Timeout,
    }));
    assert!(late_effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Completed {
                op_id: 14,
                result: DhtOutputValue::PutStored { stats }
            }) if stats.remote_attempt_count == 2 && stats.remote_store_count == 1
        )
    }));
}

#[test]
fn get_merges_local() {
    let local_secret = iroh::SecretKey::from_bytes(&[45u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

    let peer = make_node(8);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));

    let key = DhtKeyId::from_data(b"get-short-circuit-local");
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 15,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));

    let effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 15,
        stage: StorageStage::GetLocalRead,
        entries: vec![make_entry(9, key, make_realm(1), b"cached", 2_000)],
    }));

    assert!(effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::RpcRequest {
                    phase: RpcPhase::GetLookup,
                    peer: req_peer,
                    ..
                } if req_peer == peer
            ),
            _ => false,
        }
    }));

    let complete = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 15,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: Vec::new(),
            closer_nodes: Vec::new(),
        },
    }));
    assert!(complete.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Completed {
                op_id: 15,
                result: DhtOutputValue::GetValues { values, stats }
            }) if values.iter().any(|entry| entry.value == b"cached".to_vec())
                && stats.completed_reason == GetCompletedReason::LocalValue
                && stats.local_value_count == 1
                && stats.remote_value_count == 0
                && stats.queried_peer_count == 1
                && stats.peer_error_count == 0
        )
    }));
}

#[test]
fn highwater_blocks_get() {
    let local_secret = make_secret(96);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 801);
    let peer = make_node(97);
    let key = DhtKeyId::from_data(b"get-highwater");
    let realm_id = make_realm(1);
    let publisher_secret = make_secret(98);
    let publisher = publisher_secret.public();
    let value = b"newer".to_vec();
    let expires_at = 500;
    let revision = 2;
    let signed = signed_record_bytes(&key, &publisher, &realm_id, &value, expires_at, revision);
    let highwater = StoredEntry {
        publisher,
        realm_id,
        value,
        expires_at,
        revision,
        signature: publisher_secret.sign(&signed),
        retain_until: 86_800,
    };

    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 92,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));
    let lookup = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 92,
        stage: StorageStage::GetLocalRead,
        entries: vec![highwater],
    }));
    assert!(lookup.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::IoRequest(inner)
                if matches!(&**inner, DhtIoRequest::RpcRequest {
                    phase: RpcPhase::GetLookup,
                    peer: request_peer,
                    ..
                } if *request_peer == peer)
        )
    }));

    let effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 92,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Value {
            entries: vec![make_value(98, key, realm_id, b"older", 1_000)],
            closer_nodes: Vec::new(),
        },
    }));
    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::Output(DhtOutput::Completed {
            result: DhtOutputValue::GetValues { values, .. },
            ..
        })] if values.is_empty()
    ));
}

#[test]
fn local_realm_filter() {
    let local_secret = iroh::SecretKey::from_bytes(&[58u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

    let key = DhtKeyId::from_data(b"get-realm-filter-local");
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 22,
        key,
        realm_filter: Some(make_realm(1)),
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));

    let effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 22,
        stage: StorageStage::GetLocalRead,
        entries: vec![make_entry(23, key, make_realm(2), b"foreign", 2_000)],
    }));

    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Failed { op_id: 22, .. })
        )
    }));
}

#[test]
fn get_forwards_filter() {
    let local_secret = iroh::SecretKey::from_bytes(&[59u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

    let peer = make_node(24);
    let realm_filter = make_realm(3);
    let key = DhtKeyId::from_data(b"get-realm-filter-remote");

    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 23,
        key,
        realm_filter: Some(realm_filter),
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));

    let effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 23,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));

    assert!(effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
            &(**inner),
            DhtIoRequest::RpcRequest {
                op_id: 23,
                phase: RpcPhase::GetLookup,
                peer: req_peer,
                request: DhtRequest::GetValue {
                    key: req_key,
                    realm_filter: Some(req_realm_filter),
                },
                ..
            } if *req_peer == peer && *req_key == key && *req_realm_filter == realm_filter),
            _ => false,
        }
    }));
}

#[test]
fn get_collects_publishers() {
    let local_secret = iroh::SecretKey::from_bytes(&[46u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

    let peer_a = make_node(10);
    let peer_b = make_node(11);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_a }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_b }));

    let key = DhtKeyId::from_data(b"get-short-circuit-remote");
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 16,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));

    let local_effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 16,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));

    let mut lookup_peers = Vec::new();
    for effect in &local_effects {
        if let DhtEffect::IoRequest(inner) = effect
            && let DhtIoRequest::RpcRequest {
                op_id: 16,
                phase: RpcPhase::GetLookup,
                peer,
                ..
            } = **inner
        {
            lookup_peers.push(peer);
        }
    }
    assert!(
        lookup_peers.len() >= 2,
        "expected lookup fanout to both peers"
    );

    let response_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 16,
        phase: RpcPhase::GetLookup,
        peer: lookup_peers[0],
        response: DhtResponse::Value {
            entries: vec![make_value(12, key, make_realm(1), b"first", 2_000)],
            closer_nodes: Vec::new(),
        },
    }));

    assert!(
        response_effects
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );

    let pending = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 16,
        phase: RpcPhase::GetLookup,
        peer: lookup_peers[1],
        response: DhtResponse::Value {
            entries: vec![make_value(13, key, make_realm(1), b"second", 2_000)],
            closer_nodes: Vec::new(),
        },
    }));
    let complete = finish_get(&mut state, 16, &pending);
    assert!(complete.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Completed {
                op_id: 16,
                result: DhtOutputValue::GetValues { values, stats }
            }) if values.iter().any(|entry| entry.value == b"first".to_vec())
                && values.iter().any(|entry| entry.value == b"second".to_vec())
                && stats.completed_reason == GetCompletedReason::RemoteValue
                && stats.local_value_count == 0
                && stats.remote_value_count == 2
                && stats.queried_peer_count == lookup_peers.len()
        )
    }));
}

#[test]
fn get_reports_errors() {
    let local_secret = iroh::SecretKey::from_bytes(&[61u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

    let peer_a = make_node(28);
    let peer_b = make_node(29);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_a }));
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer_b }));

    let op_id = 24;
    let key = DhtKeyId::from_data(b"get-stats-peer-errors");
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));

    let local_effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));

    let mut lookup_peers = Vec::new();
    for effect in &local_effects {
        if let DhtEffect::IoRequest(inner) = effect
            && let DhtIoRequest::RpcRequest {
                op_id: request_op_id,
                phase: RpcPhase::GetLookup,
                peer,
                ..
            } = **inner
            && request_op_id == op_id
        {
            lookup_peers.push(peer);
        }
    }
    assert_eq!(lookup_peers.len(), 2);

    let failed_peer = lookup_peers[0];
    let value_peer = lookup_peers[1];
    let error_effects = state.step(DhtInput::Io(DhtIo::RpcError {
        op_id,
        phase: RpcPhase::GetLookup,
        peer: failed_peer,
        error: DhtIoError::Timeout,
    }));
    assert!(
        error_effects
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );

    let response_effects = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id,
        phase: RpcPhase::GetLookup,
        peer: value_peer,
        response: DhtResponse::Value {
            entries: vec![make_value(30, key, make_realm(1), b"after-error", 2_000)],
            closer_nodes: Vec::new(),
        },
    }));
    let response_effects = finish_get(&mut state, op_id, &response_effects);

    let (values, stats) = response_effects
        .iter()
        .find_map(|effect| {
            if let DhtEffect::Output(DhtOutput::Completed {
                op_id: completed_op_id,
                result: DhtOutputValue::GetValues { values, stats },
            }) = effect
                && *completed_op_id == op_id
            {
                Some((values, stats))
            } else {
                None
            }
        })
        .expect("get should complete after remote value");

    assert!(
        values
            .iter()
            .any(|entry| entry.value == b"after-error".to_vec())
    );
    assert_eq!(stats.completed_reason, GetCompletedReason::RemoteValue);
    assert_eq!(stats.local_value_count, 0);
    assert_eq!(stats.remote_value_count, 1);
    assert_eq!(stats.queried_peer_count, lookup_peers.len());
    assert!(
        lookup_peers
            .iter()
            .all(|peer| stats.queried_peers.contains(peer))
    );
    assert_eq!(stats.peer_error_count, 1);
    assert_eq!(stats.peer_errors.len(), 1);
    assert_eq!(stats.peer_errors[0].peer, failed_peer);
    assert_eq!(stats.peer_errors[0].error, "timeout");
}

#[test]
fn bootstrap_partial_success() {
    let local_secret = iroh::SecretKey::from_bytes(&[47u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let peer_a = make_node(14);
    let peer_b = make_node(15);
    let _ = state.step(DhtInput::Cmd(DhtCmd::Bootstrap {
        op_id: 17,
        nodes: vec![peer_a, peer_b],
        trace_context: None,
    }));

    let first = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 17,
        phase: RpcPhase::Bootstrap,
        peer: peer_a,
        response: DhtResponse::Pong,
    }));
    assert!(
        first
            .iter()
            .all(|effect| !matches!(effect, DhtEffect::Output(_)))
    );

    let second = state.step(DhtInput::Io(DhtIo::RpcError {
        op_id: 17,
        phase: RpcPhase::Bootstrap,
        peer: peer_b,
        error: DhtIoError::Timeout,
    }));
    assert!(second.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Completed {
                op_id: 17,
                result: DhtOutputValue::Unit
            })
        )
    }));
}

#[test]
fn wrong_phase_ignored() {
    let local_secret = iroh::SecretKey::from_bytes(&[48u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let peer = make_node(16);
    let _ = state.step(DhtInput::Cmd(DhtCmd::Bootstrap {
        op_id: 18,
        nodes: vec![peer],
        trace_context: None,
    }));

    let wrong_phase = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 18,
        phase: RpcPhase::GetLookup,
        peer,
        response: DhtResponse::Pong,
    }));
    assert!(wrong_phase.is_empty());

    let complete = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id: 18,
        phase: RpcPhase::Bootstrap,
        peer,
        response: DhtResponse::Pong,
    }));
    assert!(complete.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Completed {
                op_id: 18,
                result: DhtOutputValue::Unit
            })
        )
    }));
}

#[test]
fn wrong_stage_ignored() {
    let local_secret = iroh::SecretKey::from_bytes(&[49u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let key = DhtKeyId::from_data(b"wrong-stage");
    let _ = state.step(DhtInput::Cmd(DhtCmd::Get {
        op_id: 19,
        key,
        realm_filter: None,
        completion: DhtCompletion::Exhaustive,
        trace_context: None,
    }));

    let wrong_stage = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 19,
        stage: StorageStage::PutLocalRead,
        entries: Vec::new(),
    }));
    assert!(wrong_stage.is_empty());

    let complete = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: 19,
        stage: StorageStage::GetLocalRead,
        entries: Vec::new(),
    }));
    assert!(complete.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::Output(DhtOutput::Failed { op_id: 19, .. })
        )
    }));
}

#[test]
fn unsigned_put_rejected() {
    let local_secret = iroh::SecretKey::from_bytes(&[50u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let publisher = make_node(17);
    let key = DhtKeyId::from_data(b"missing-signature");
    let realm_id = make_realm(1);
    let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
        inbound_id: 7,
        peer: make_node(18),
        request: DhtRequest::PutValue {
            key,
            realm_id,
            value: b"value".to_vec(),
            expires_at: 30,
            revision: 1,
            publisher,
            signature: None,
        },
        trace_context: None,
    }));

    assert!(effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::RpcResponse {
                    inbound_id: 7,
                    response: DhtResponse::Error {
                        code: ErrorCode::InvalidSignature,
                        ..
                    }
                }
            ),
            _ => false,
        }
    }));
}

#[test]
fn expired_put_rejected() {
    let local_secret = iroh::SecretKey::from_bytes(&[62u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let publisher_secret = iroh::SecretKey::from_bytes(&[63u8; 32]);
    let key = DhtKeyId::from_data(b"zero-ttl-inbound-put");
    let realm_id = make_realm(1);
    let value = b"value".to_vec();
    let publisher = publisher_secret.public();
    let expires_at = 0;
    let revision = 1;
    let signed_data =
        signed_record_bytes(&key, &publisher, &realm_id, &value, expires_at, revision);
    let signature = publisher_secret.sign(&signed_data);

    let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
        inbound_id: 11,
        peer: make_node(28),
        request: DhtRequest::PutValue {
            key,
            realm_id,
            value,
            expires_at,
            revision,
            publisher,
            signature: Some(signature),
        },
        trace_context: None,
    }));

    assert!(effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                &(**inner),
                DhtIoRequest::RpcResponse {
                    inbound_id: 11,
                    response: DhtResponse::Error {
                        code: ErrorCode::InvalidRequest,
                        message,
                    }
                } if message.contains("expiry")
            ),
            _ => false,
        }
    }));
    assert!(!effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::IoRequest(inner)
                if matches!(&**inner, DhtIoRequest::StorageMerge { .. })
        )
    }));
}

#[test]
fn inbound_realm_filter() {
    let local_secret = iroh::SecretKey::from_bytes(&[60u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 1_000);

    let key = DhtKeyId::from_data(b"inbound-get-realm-filter");
    let realm_filter = make_realm(4);

    let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
        inbound_id: 10,
        peer: make_node(25),
        request: DhtRequest::GetValue {
            key,
            realm_filter: Some(realm_filter),
        },
        trace_context: None,
    }));

    let op_id = effects
        .iter()
        .find_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::StorageRead {
                    op_id,
                    stage: StorageStage::InboundGetRead,
                    key: req_key,
                    realm_filter: Some(req_realm_filter),
                    now_secs,
                } = &(**inner)
            {
                if *req_key == key && *req_realm_filter == realm_filter && *now_secs == 1_000 {
                    Some(op_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .expect("inbound get should enqueue filtered storage read");

    let response_effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id: *op_id,
        stage: StorageStage::InboundGetRead,
        entries: vec![
            make_entry(26, key, realm_filter, b"allowed", 2_000),
            make_entry(27, key, make_realm(5), b"foreign", 2_000),
        ],
    }));

    assert!(response_effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                &(**inner),
                DhtIoRequest::RpcResponse {
                    inbound_id: 10,
                    response: DhtResponse::Value {
                        entries,
                        closer_nodes: _,
                    },
            } if entries.len() == 1
                && entries[0].realm_id == realm_filter
                && entries[0].value == b"allowed".to_vec()),
            _ => false,
        }
    }));
}

#[test]
fn expired_floor_propagates() {
    let local_secret = make_secret(96);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 1_000);
    let key = DhtKeyId::from_data(b"propagated-floor");

    let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
        inbound_id: 12,
        peer: make_node(97),
        request: DhtRequest::GetValue {
            key,
            realm_filter: None,
        },
        trace_context: None,
    }));
    let op_id = effects
        .iter()
        .find_map(|effect| {
            if let DhtEffect::IoRequest(inner) = effect
                && let DhtIoRequest::StorageRead {
                    op_id,
                    stage: StorageStage::InboundGetRead,
                    ..
                } = &**inner
            {
                Some(*op_id)
            } else {
                None
            }
        })
        .expect("inbound get storage read");
    let mut floor = make_entry(98, key, make_realm(1), b"floor", 900);
    floor.retain_until = retention_deadline(floor.expires_at, 1_000);

    let effects = state.step(DhtInput::Io(DhtIo::StorageReadResult {
        op_id,
        stage: StorageStage::InboundGetRead,
        entries: vec![floor],
    }));
    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            DhtEffect::IoRequest(inner)
                if matches!(&**inner, DhtIoRequest::RpcResponse {
                    inbound_id: 12,
                    response: DhtResponse::Value { entries, .. },
                } if entries.len() == 1 && entries[0].expires_at == 900)
        )
    }));
}

#[test]
fn invalid_put_rejected() {
    let local_secret = iroh::SecretKey::from_bytes(&[51u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let publisher_secret = iroh::SecretKey::from_bytes(&[52u8; 32]);
    let attacker_secret = iroh::SecretKey::from_bytes(&[53u8; 32]);

    let key = DhtKeyId::from_data(b"invalid-signature");
    let realm_id = make_realm(1);
    let value = b"value".to_vec();
    let expires_at: u64 = 45;
    let revision = 1;
    let publisher = publisher_secret.public();
    let signed_data =
        signed_record_bytes(&key, &publisher, &realm_id, &value, expires_at, revision);
    let invalid_signature = attacker_secret.sign(&signed_data);

    let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
        inbound_id: 8,
        peer: make_node(19),
        request: DhtRequest::PutValue {
            key,
            realm_id,
            value,
            expires_at,
            revision,
            publisher,
            signature: Some(invalid_signature),
        },
        trace_context: None,
    }));

    assert!(effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::RpcResponse {
                    inbound_id: 8,
                    response: DhtResponse::Error {
                        code: ErrorCode::InvalidSignature,
                        ..
                    }
                }
            ),
            _ => false,
        }
    }));
    assert!(state.routing_table.all_peers().is_empty());
}

#[test]
fn valid_put_merges() {
    let local_secret = iroh::SecretKey::from_bytes(&[56u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let publisher_secret = iroh::SecretKey::from_bytes(&[57u8; 32]);
    let key = DhtKeyId::from_data(b"valid-signature");
    let realm_id = make_realm(1);
    let value = b"value".to_vec();
    let expires_at: u64 = 45;
    let revision = 1;
    let publisher = publisher_secret.public();

    let signed_data =
        signed_record_bytes(&key, &publisher, &realm_id, &value, expires_at, revision);
    let signature = publisher_secret.sign(&signed_data);

    let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
        inbound_id: 9,
        peer: make_node(22),
        request: DhtRequest::PutValue {
            key,
            realm_id,
            value,
            expires_at,
            revision,
            publisher,
            signature: Some(signature),
        },
        trace_context: None,
    }));

    assert!(effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
            **inner,
            DhtIoRequest::StorageMerge {
                stage: StorageStage::InboundPutMerge,
                key: req_key,
                ..
            } if req_key == key),
            _ => false,
        }
    }));
    assert_eq!(state.routing_table.all_peers().len(), 1);
}

#[test]
fn refuses_user_put() {
    // A user device is DHT read-only: its own signed record is refused and
    // the attempt never puts it in the routing table.
    let local_secret = make_secret(58);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 0);
    let device_secret = make_secret(59);
    let device = device_secret.public();
    state.set_peer_kinds(user_kinds(device));

    let key = DhtKeyId::from_data(b"refuses-user-put");
    let realm_id = make_realm(1);
    let value = b"presence".to_vec();
    let expires_at: u64 = 45;
    let revision = 1;
    let signed_data = signed_record_bytes(&key, &device, &realm_id, &value, expires_at, revision);
    let signature = device_secret.sign(&signed_data);

    let effects = state.step(DhtInput::Io(DhtIo::InboundRequest {
        inbound_id: 41,
        peer: device,
        request: DhtRequest::PutValue {
            key,
            realm_id,
            value,
            expires_at,
            revision,
            publisher: device,
            signature: Some(signature),
        },
        trace_context: None,
    }));

    assert!(effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::RpcResponse {
                    inbound_id: 41,
                    response: DhtResponse::Error {
                        code: ErrorCode::InvalidRequest,
                        ..
                    }
                }
            ),
            _ => false,
        }
    }));
    assert!(!effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => {
                matches!(**inner, DhtIoRequest::StorageMerge { .. })
            }
            _ => false,
        }
    }));
    assert!(state.routing_table.all_peers().is_empty());
}

#[test]
fn user_reads_unrouted() {
    // Discovery reads stay open for a device; only routing membership and
    // publication are closed.
    let local_secret = make_secret(60);
    let mut state = DhtStateMachine::new(local_secret.public(), local_secret, 0);
    let device = make_node(61);
    state.set_peer_kinds(user_kinds(device));

    let pong = state.step(DhtInput::Io(DhtIo::InboundRequest {
        inbound_id: 42,
        peer: device,
        request: DhtRequest::Ping,
        trace_context: None,
    }));
    assert!(pong.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::RpcResponse {
                    inbound_id: 42,
                    response: DhtResponse::Pong
                }
            ),
            _ => false,
        }
    }));

    let read = state.step(DhtInput::Io(DhtIo::InboundRequest {
        inbound_id: 43,
        peer: device,
        request: DhtRequest::GetValue {
            key: DhtKeyId::from_data(b"user-reads-unrouted"),
            realm_filter: None,
        },
        trace_context: None,
    }));
    assert!(read.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
                **inner,
                DhtIoRequest::StorageRead {
                    stage: StorageStage::InboundGetRead,
                    ..
                }
            ),
            _ => false,
        }
    }));
    assert!(state.routing_table.all_peers().is_empty());

    // A sync-eligible peer still joins through the same path.
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer {
        node_id: make_node(62),
    }));
    assert_eq!(state.routing_table.all_peers().len(), 1);
}

#[test]
fn maintenance_timeout_evicts() {
    let local_secret = iroh::SecretKey::from_bytes(&[54u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let peer = make_node(20);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));

    let before = state.step(DhtInput::Cmd(DhtCmd::RoutingTableSize { op_id: 20 }));
    assert!(matches!(
        before.as_slice(),
        [DhtEffect::Output(DhtOutput::Completed {
            op_id: 20,
            result: DhtOutputValue::RoutingTableSize(1)
        })]
    ));

    let tick_effects = state.step(DhtInput::Tick {
        now_tick: 1,
        now_secs: 0,
    });
    assert!(tick_effects.iter().any(|effect| {
        match effect {
            DhtEffect::IoRequest(inner) => matches!(
            &(**inner),
            DhtIoRequest::RpcRequest {
                phase: RpcPhase::MaintenancePing,
                peer: req_peer,
                trace_context: None,
                ..
            } if *req_peer == peer),
            _ => false,
        }
    }));

    let _ = state.step(DhtInput::Tick {
        now_tick: 1 + RPC_TIMEOUT_TICKS,
        now_secs: 0,
    });

    let after = state.step(DhtInput::Cmd(DhtCmd::RoutingTableSize { op_id: 21 }));
    assert!(matches!(
        after.as_slice(),
        [DhtEffect::Output(DhtOutput::Completed {
            op_id: 21,
            result: DhtOutputValue::RoutingTableSize(0)
        })]
    ));
}

#[test]
fn stale_maintenance_preserves() {
    let local_secret = make_secret(94);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);
    let peer = make_node(95);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
    let _ = state.step(DhtInput::Tick {
        now_tick: 1,
        now_secs: 0,
    });
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));
    let _ = state.step(DhtInput::Tick {
        now_tick: 1 + RPC_TIMEOUT_TICKS,
        now_secs: 0,
    });

    assert!(
        state
            .routing_table
            .all_peers()
            .iter()
            .any(|entry| entry.node_id == peer)
    );
}

#[test]
fn invalid_ping_evicts() {
    let local_secret = make_secret(96);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);
    let peer = make_node(97);
    let _ = state.step(DhtInput::Cmd(DhtCmd::AddPeer { node_id: peer }));

    let effects = state.step(DhtInput::Tick {
        now_tick: 1,
        now_secs: 0,
    });
    let op_id = effects
        .iter()
        .find_map(|effect| match effect {
            DhtEffect::IoRequest(request) => match &**request {
                DhtIoRequest::RpcRequest {
                    op_id,
                    phase: RpcPhase::MaintenancePing,
                    ..
                } => Some(*op_id),
                _ => None,
            },
            DhtEffect::Output(_) => None,
        })
        .expect("maintenance ping");

    let _ = state.step(DhtInput::Io(DhtIo::RpcResponse {
        op_id,
        phase: RpcPhase::MaintenancePing,
        peer,
        response: DhtResponse::Error {
            code: ErrorCode::InvalidRequest,
            message: "invalid ping".to_string(),
        },
    }));

    assert!(state.routing_table.all_peers().is_empty());
}

#[test]
fn cleanup_runs_parallel() {
    let local_secret = make_secret(98);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 100);

    let effects = state.step(DhtInput::Tick {
        now_tick: 1,
        now_secs: 100,
    });

    assert_eq!(
        effects
            .iter()
            .filter(|effect| matches!(effect, DhtEffect::IoRequest(request) if matches!(&**request, DhtIoRequest::StorageIter { .. })))
            .count(),
        2
    );
    assert!(state.cleanup_active);
    assert!(state.cleanup_floor);
}

#[test]
fn cleanup_drains_heads() {
    let local_secret = make_secret(99);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 100);
    let _ = state.step(DhtInput::Tick {
        now_tick: 1,
        now_secs: 100,
    });
    let entry = DeadlineEntry {
        deadline: 100,
        key: DhtKeyId::from_data(b"due-head"),
    };

    let effects = state.step(DhtInput::Io(DhtIo::StorageIterResult {
        op_id: CLEANUP_OP_ID,
        stage: StorageStage::CleanupActiveIter,
        index: DeadlineIndex::Active,
        entries: vec![entry],
    }));
    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::IoRequest(request)]
            if matches!(&**request, DhtIoRequest::StoragePrune {
                stage: StorageStage::CleanupActivePrune,
                entry: request_entry,
                ..
            } if *request_entry == entry)
    ));

    let effects = state.step(DhtInput::Io(DhtIo::StorageWriteResult {
        op_id: CLEANUP_OP_ID,
        stage: StorageStage::CleanupActivePrune,
    }));
    assert!(matches!(
        effects.as_slice(),
        [DhtEffect::IoRequest(request)]
            if matches!(&**request, DhtIoRequest::StorageIter {
                stage: StorageStage::CleanupActiveIter,
                ..
            })
    ));
    assert!(state.cleanup_active);
    assert!(state.cleanup_floor);
}

#[test]
fn unknown_io_ignored() {
    let local_secret = iroh::SecretKey::from_bytes(&[55u8; 32]);
    let local_id = local_secret.public();
    let mut state = DhtStateMachine::new(local_id, local_secret, 0);

    let effects = state.step(DhtInput::Io(DhtIo::RpcError {
        op_id: 999,
        phase: RpcPhase::Bootstrap,
        peer: make_node(21),
        error: DhtIoError::Timeout,
    }));
    assert!(effects.is_empty());
}
