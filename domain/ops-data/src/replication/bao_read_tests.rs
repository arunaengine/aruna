//! Tests bao peer reads and placement rule negotiation with fixed inputs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::format::StoredFormat;
use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_MD5};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, BlobVersion, BucketInfo};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::user::id::UserId;
use ulid::Ulid;

use super::{
    BaoReadError, BaoReadOperation, BaoReadOutput, IncomingBaoOperation, IncomingBaoResult,
    ServeTurn,
};
use aruna_core::replication_protocol::{
    BaoReadRefusal, BaoReadRequest, BaoReadTarget, VersionReplicationMessage,
};

fn test_realm() -> RealmId {
    RealmId::from_bytes([7u8; 32])
}

fn node_from_seed(seed: u8) -> aruna_core::id::NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn read_request(local_node: aruna_core::id::NodeId, hash: [u8; 32]) -> BaoReadRequest {
    let realm_id = test_realm();
    BaoReadRequest {
        auth_context: AuthContext {
            user_id: UserId::nil(realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        },
        realm_id,
        target: BaoReadTarget::ExactVersion(
            VersionedObjectArn::new(
                realm_id,
                local_node,
                "bucket",
                "path/file.txt",
                Ulid::from(4u128),
            )
            .unwrap(),
        ),
        expected_blake3: Some(hash),
        metadata_only: false,
        destination: None,
        known_refs: Vec::new(),
        range: None,
        disallow_encrypted: false,
    }
}

fn read_path(local_node: aruna_core::id::NodeId) -> String {
    aruna_core::structs::storage::blob::object_permission_path(
        test_realm(),
        Ulid::from(5u128),
        local_node,
        "bucket",
        "path/file.txt",
    )
}

fn realm_value(peer: aruna_core::id::NodeId) -> byteview::ByteView {
    let mut config = RealmConfigDocument::default_for_realm(test_realm(), Vec::new());
    config.ensure_node(peer, RealmNodeKind::Server);
    postcard::to_allocvec(&config).unwrap().into()
}

fn version_value(hash: [u8; 32]) -> byteview::ByteView {
    BlobVersion::materialized(
        hash,
        BackendRef::node_default(),
        EncodingClass::Raw,
        SystemTime::UNIX_EPOCH,
        UserId::nil(test_realm()),
        None,
    )
    .to_bytes()
    .unwrap()
    .into()
}

fn bucket_value() -> byteview::ByteView {
    BucketInfo {
        group_id: Ulid::from(5u128),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::nil(test_realm()),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
    .to_bytes()
    .unwrap()
    .into()
}

fn location_value(hash: [u8; 32]) -> (BackendLocation, byteview::ByteView) {
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/data".to_string(),
        storage_bucket: "blob-0".to_string(),
        backend_path: "object".to_string(),
        ulid: Ulid::from(6u128),
        format: StoredFormat::default(),
        created_by: UserId::nil(test_realm()),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 42,
        hashes: HashMap::from([(HASH_BLAKE3.to_string(), hash.to_vec())]),
    };
    let value = location.to_bytes().unwrap().into();
    (location, value)
}

/// One observation of a local directory, as a device records it.
fn observation(size: u64, etag: Option<&str>) -> BlobVersion {
    use aruna_core::structs::execution::offered_directory::OFFERED_DIRECTORY_BUCKET;
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    BlobVersion::reference(
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::LocalDirectory,
                public_config: HashMap::from([(
                    OFFERED_DIRECTORY_BUCKET.to_string(),
                    "folder-x".to_string(),
                )]),
                source_path: "path/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        },
        SourceMetadata {
            content_length: size,
            content_type: None,
            etag: etag.map(ToOwned::to_owned),
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        },
        SystemTime::UNIX_EPOCH,
        UserId::nil(test_realm()),
        SystemTime::UNIX_EPOCH,
    )
}

/// A serve on a device, for its own owner, asked by realm infrastructure.
fn device_serve(hash: Option<[u8; 32]>) -> IncomingBaoOperation {
    let local_node = node_from_seed(1);
    let mut request = read_request(local_node, [4u8; 32]);
    request.expected_blake3 = hash;
    let mut operation = IncomingBaoOperation::new(
        node_from_seed(2),
        local_node,
        test_realm(),
        Ulid::from(9u128),
        request,
    );
    operation.local_owner = Some(UserId::nil(test_realm()));
    operation.peer_is_infra = true;
    operation
}

#[test]
fn realm_refuses_observation() {
    // A realm node holds no observations, so it answers as it always did.
    let mut operation = device_serve(Some([5u8; 32]));
    operation.local_owner = None;
    let turn = operation.serve_observation(observation(5, Some("5-1")));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::NotFound);
}

#[test]
fn device_refuses_stranger() {
    // Neither infrastructure nor the owner: the entry is not even looked up.
    let mut operation = device_serve(Some([5u8; 32]));
    operation.peer_is_infra = false;
    operation.request.auth_context.user_id = UserId::local(Ulid::from(77u128), test_realm());
    assert!(!operation.serves_own_data());
    let turn = operation.serve_observation(observation(5, Some("5-1")));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::ReadDenied);
}

#[test]
fn device_needs_hash() {
    // Without a named identity this node would have to invent one.
    let mut operation = device_serve(None);
    let turn = operation.serve_observation(observation(5, Some("5-1")));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::HashMismatch);
}

#[test]
fn device_needs_fingerprint() {
    // Without the observed fingerprint the adapter cannot re-check anything.
    let mut operation = device_serve(Some([5u8; 32]));
    let turn = operation.serve_observation(observation(5, None));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::NotFound);
}

#[test]
fn device_refuses_governed() {
    // A governed reference answers the destination challenge or nothing.
    let mut operation = device_serve(Some([5u8; 32]));
    operation.version_refs = vec![PlacementPolicyRef {
        policy_id: Ulid::from(3u128),
        digest: [2u8; 32],
    }];
    let turn = operation.serve_observation(observation(5, Some("5-1")));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::ReadDenied);
}

#[test]
fn device_refuses_probe() {
    // A metadata probe would echo an identity this node has not verified.
    let mut operation = device_serve(Some([5u8; 32]));
    operation.request.metadata_only = true;
    let turn = operation.serve_observation(observation(5, Some("5-1")));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::ReadDenied);
}

#[test]
fn device_serves_file() {
    let mut operation = device_serve(Some([5u8; 32]));
    let turn = operation.serve_observation(observation(5, Some("5-1")));
    assert!(matches!(&turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.source_fingerprint.as_deref(), Some("5-1"));
}

#[test]
fn denied_keeps_blob() {
    // A device may bypass the group check for its own observations, never
    // for a materialized copy it holds.
    let mut operation = device_serve(Some([4u8; 32]));
    operation.policy_allowed = false;
    let (location, _) = location_value([4u8; 32]);
    let materialized = BlobVersion::materialized(
        [4u8; 32],
        location.backend.clone(),
        EncodingClass::Raw,
        SystemTime::UNIX_EPOCH,
        UserId::nil(test_realm()),
        None,
    );
    let value: byteview::ByteView = materialized.to_bytes().unwrap().into();
    let turn = operation.handle_exact_version(Some(value));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::ReadDenied);
}

#[test]
fn source_rejects_event() {
    // An event the resolve state cannot explain must fail, not be ignored.
    let mut operation = device_serve(Some([5u8; 32]));
    operation.state = super::IncomingBaoState::ResolveSource;
    let event = Event::Storage(StorageEvent::SyncAllFinished);
    let turn = operation.next(IoResult::Event(Box::new(event)));
    assert!(matches!(turn, Err(BaoReadError::Unexpected { .. })));
}

fn row(value: Option<byteview::ByteView>) -> IoResult {
    IoResult::Values(vec![value])
}

fn rows(values: Vec<Option<byteview::ByteView>>) -> IoResult {
    IoResult::Values(values)
}

fn asked(turn: &ServeTurn) -> &Effect {
    match turn {
        Ok(Next::Call(IoCall::Effect(effect))) => effect,
        other => panic!("expected an effect call, got {other:?}"),
    }
}

fn refusal_from(turn: &ServeTurn) -> BaoReadRefusal {
    let Some(BlobEffect::SendMessage { payload, .. }) = asked(turn).as_blob() else {
        panic!("expected refusal frame")
    };
    let VersionReplicationMessage::BaoReadRefused(reason) =
        VersionReplicationMessage::from_bytes(payload).unwrap()
    else {
        panic!("expected typed refusal")
    };
    reason
}

/// The refusal frame and the close of a failed serve, or only the close.
fn abort_shape(calls: &[IoCall]) -> (Option<BaoReadRefusal>, bool) {
    fn effect(call: &IoCall) -> &BlobEffect {
        match call {
            IoCall::Effect(effect) => match effect.as_blob() {
                Some(effect) => effect,
                None => panic!("expected a stream call, got {effect:?}"),
            },
            other => panic!("expected a stream call, got {other:?}"),
        }
    }
    let closes = calls
        .last()
        .is_some_and(|call| matches!(effect(call), BlobEffect::CloseConnection { .. }));
    let refusal = (calls.len() == 2).then(|| match effect(&calls[0]) {
        BlobEffect::SendMessage { payload, .. } => {
            match VersionReplicationMessage::from_bytes(payload).unwrap() {
                VersionReplicationMessage::BaoReadRefused(reason) => reason,
                other => panic!("expected a refusal, got {other:?}"),
            }
        }
        other => panic!("expected the refusal frame, got {other:?}"),
    });
    (refusal, closes)
}

#[test]
fn abort_tells_reader() {
    // A storage failure before any answer still reaches the reader as a refusal.
    let mut operation = device_serve(Some([5u8; 32]));
    operation.first().unwrap();
    let expected = (Some(BaoReadRefusal::BackendFailure), true);
    assert_eq!(abort_shape(&operation.abort()), expected);

    // Once the reader was answered, a failure only closes the stream.
    let mut operation = device_serve(Some([5u8; 32]));
    operation.send_refusal(BaoReadRefusal::NotFound).unwrap();
    assert_eq!(abort_shape(&operation.abort()), (None, true));

    // The operation's own failure closes the stream without inventing an answer.
    let mut operation = device_serve(Some([5u8; 32]));
    operation.first().unwrap();
    operation.fail(BaoReadError::NotFinished).unwrap_err();
    assert_eq!(abort_shape(&operation.abort()), (None, true));
}

#[test]
fn exact_probe_hash() {
    let remote_node = node_from_seed(1);
    let hash = [4u8; 32];
    let stream_id = Ulid::from(9u128);
    let mut request = read_request(remote_node, hash);
    request.expected_blake3 = None;
    request.metadata_only = true;
    let mut operation = BaoReadOperation::new(remote_node, request);

    let _ = operation.first();
    let _ = operation.next(blob(BlobEvent::ConnectionEstablished { stream_id }));
    let _ = operation.next(blob(BlobEvent::MessageSent { stream_id }));
    let payload = VersionReplicationMessage::BaoReadAccepted {
        size: 42,
        blake3: hash,
        etag: Some("etag-1".to_string()),
        hashes: [("md5".to_string(), vec![3u8; 16])].into_iter().collect(),
    }
    .to_bytes()
    .unwrap();
    let close = operation.next(blob(BlobEvent::MessageReceived { stream_id, payload }));
    assert!(matches!(
        blob_effect(&close),
        Some(BlobEffect::CloseConnection { stream_id: id }) if *id == stream_id
    ));
    let done = operation.next(blob(BlobEvent::ConnectionClosed { stream_id }));
    assert!(operation.abort().is_empty());

    assert_eq!(
        done.unwrap(),
        Next::Done(BaoReadOutput::Metadata {
            size: 42,
            blake3: hash,
            etag: Some("etag-1".to_string()),
            hashes: HashMap::from([("md5".to_string(), vec![3u8; 16])]),
        })
    );
}

fn blob(event: BlobEvent) -> IoResult {
    IoResult::Event(Box::new(Event::blob(event)))
}

/// The blob effect a requester step asks for, if it asks for one.
fn blob_effect(step: &Result<Next<BaoReadOutput>, BaoReadError>) -> Option<&BlobEffect> {
    let Ok(Next::Call(IoCall::Effect(effect))) = step else {
        return None;
    };
    effect.as_blob()
}

/// Drives a requester read to the response frame and returns its outcome there; the
/// runner's abort then closes the open stream.
fn respond(message: VersionReplicationMessage) -> Result<Next<BaoReadOutput>, BaoReadError> {
    let remote_node = node_from_seed(1);
    let stream_id = Ulid::from(9u128);
    let mut operation = BaoReadOperation::new(remote_node, read_request(remote_node, [4u8; 32]));
    let _ = operation.first();
    let _ = operation.next(blob(BlobEvent::ConnectionEstablished { stream_id }));
    let _ = operation.next(blob(BlobEvent::MessageSent { stream_id }));
    let payload = message.to_bytes().unwrap();
    let answer = operation.next(blob(BlobEvent::MessageReceived { stream_id, payload }));
    let close = Effect::blob(BlobEffect::CloseConnection { stream_id });
    assert_eq!(operation.abort(), vec![IoCall::Effect(Box::new(close))]);
    answer
}

// A range outside the announced object is refused before any byte is received.
#[test]
fn ranged_read_bounds() {
    let remote_node = node_from_seed(1);
    let stream_id = Ulid::from(9u128);
    let mut request = read_request(remote_node, [4u8; 32]);
    request.range = Some(40..43);
    let mut operation = BaoReadOperation::new(remote_node, request.clone());
    let _ = operation.first();
    let _ = operation.next(blob(BlobEvent::ConnectionEstablished { stream_id }));
    let _ = operation.next(blob(BlobEvent::MessageSent { stream_id }));
    let accepted = VersionReplicationMessage::BaoReadAccepted {
        size: 42,
        blake3: [4u8; 32],
        etag: None,
        hashes: Default::default(),
    };
    let payload = accepted.to_bytes().unwrap();
    let refused = operation.next(blob(BlobEvent::MessageReceived { stream_id, payload }));
    assert_eq!(
        refused,
        Err(BaoReadError::Refused(BaoReadRefusal::InvalidTarget))
    );

    request.range = Some(2..10);
    let mut operation = BaoReadOperation::new(remote_node, request);
    let _ = operation.first();
    let _ = operation.next(blob(BlobEvent::ConnectionEstablished { stream_id }));
    let _ = operation.next(blob(BlobEvent::MessageSent { stream_id }));
    let payload = accepted.to_bytes().unwrap();
    let receive = operation.next(blob(BlobEvent::MessageReceived { stream_id, payload }));
    assert!(matches!(
        blob_effect(&receive),
        Some(BlobEffect::ReceiveRead { size: 42, range: Some(range), .. })
            if *range == (2..10)
    ));
}

#[test]
fn required_teaches_refs() {
    // The source teaches the rule instead of streaming, so the requester can
    // resolve it and ask again.
    let policy_ref = PlacementPolicyRef {
        policy_id: Ulid::from(3u128),
        digest: [2u8; 32],
    };
    let answer = respond(VersionReplicationMessage::PlacementPolicyRequired {
        refs: vec![policy_ref],
    });
    assert_eq!(
        answer,
        Err(BaoReadError::PolicyRequired {
            refs: vec![policy_ref]
        })
    );
}

#[test]
fn denied_never_streams() {
    let answer = respond(VersionReplicationMessage::PlacementPolicyDenied {
        policy_ids: vec![Ulid::from(3u128)],
    });
    assert_eq!(
        answer,
        Err(BaoReadError::PolicyDenied {
            policy_ids: vec![Ulid::from(3u128)]
        })
    );
}

#[test]
fn echoed_refs_denied() {
    // The requester claims to know the rule, but the source still evaluates
    // the destination itself and refuses it.
    let local_node = node_from_seed(1);
    let peer = node_from_seed(2);
    let policy_ref = PlacementPolicyRef {
        policy_id: Ulid::from(3u128),
        digest: [2u8; 32],
    };
    let mut request = read_request(local_node, [4u8; 32]);
    request.known_refs = vec![policy_ref];
    request.destination = None;
    let mut operation =
        IncomingBaoOperation::new(peer, local_node, test_realm(), Ulid::from(9u128), request);
    operation.version_refs = vec![policy_ref];
    operation.request.known_refs = vec![policy_ref];

    // No destination subject means nothing governed may be served, even
    // though every ref was echoed back.
    let (location, _) = location_value([4u8; 32]);
    let turn = operation.challenge_destination(location);
    let Some(BlobEffect::SendMessage { payload, .. }) = asked(&turn).as_blob() else {
        panic!("expected a policy frame")
    };
    assert!(matches!(
        VersionReplicationMessage::from_bytes(payload).unwrap(),
        VersionReplicationMessage::PlacementPolicyDenied { .. }
    ));
}

#[test]
fn spoofed_subject_denied() {
    // The requester may assert any subject; only the realm's placement of
    // the authenticated peer decides where governed bytes may go.
    use aruna_core::structs::placement::node_subject::storage_subject;
    use aruna_core::structs::placement::policy::PlacementSubject;
    use aruna_core::structs::placement::record::{DEFAULT_NODE_WEIGHT, NodePlacementEntry};

    let local_node = node_from_seed(1);
    let peer = node_from_seed(2);
    let policy_ref = PlacementPolicyRef {
        policy_id: Ulid::from(3u128),
        digest: [2u8; 32],
    };
    let entry = NodePlacementEntry {
        node_id: peer,
        location: "us-east".to_string(),
        weight: DEFAULT_NODE_WEIGHT,
        full: false,
        draining: false,
        labels: std::collections::BTreeMap::new(),
    };
    let claimed = PlacementSubject {
        location: "eu-west".to_string(),
        ..storage_subject(&entry, 4)
    };
    let mut request = read_request(local_node, [4u8; 32]);
    request.known_refs = vec![policy_ref];
    request.destination = Some(claimed);
    let mut operation =
        IncomingBaoOperation::new(peer, local_node, test_realm(), Ulid::from(9u128), request);
    operation.version_refs = vec![policy_ref];
    operation.peer_placement = Some(entry.clone());

    let (location, _) = location_value([4u8; 32]);
    let turn = operation.challenge_destination(location.clone());
    let Some(BlobEffect::SendMessage { payload, .. }) = asked(&turn).as_blob() else {
        panic!("expected a policy frame")
    };
    assert!(matches!(
        VersionReplicationMessage::from_bytes(payload).unwrap(),
        VersionReplicationMessage::PlacementPolicyDenied { .. }
    ));

    // The honest claim resolves the rule instead of being refused outright.
    operation.request.destination = Some(storage_subject(&entry, 4));
    let turn = operation.challenge_destination(location);
    assert!(matches!(&turn, Ok(Next::Call(IoCall::Child(_)))));
}

#[test]
fn folds_read_check() {
    // The caller's roles are read with the policy rows, and they decide the read.
    use aruna_core::structs::identity::auth::{Actor, Permission, Role};
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use aruna_core::structs::storage::blob::object_permission_path;

    let (realm, local, peer) = (test_realm(), node_from_seed(1), node_from_seed(2));
    let (group_id, nil) = (Ulid::from(8u128), UserId::nil(realm));
    let path = object_permission_path(realm, group_id, local, "bucket", "path/file.txt");
    let actor = Actor {
        node_id: local,
        user_id: nil,
        realm_id: realm,
    };
    let group = GroupAuthorizationDocument::default_group_doc(nil, realm, group_id);
    let mut roles = GroupAuthorizationDocument::default_group_doc(nil, realm, group_id);
    // The test caller is the nil user, which holds only public roles.
    let role_id = Ulid::from(9u128);
    let role = Role {
        role_id,
        name: "public".to_string(),
        permissions: HashMap::from([(format!("/{realm}/g/{group_id}/**"), Permission::READ)]),
        assigned_users: HashSet::from([nil]),
    };
    roles.roles.insert(role_id, role);
    let config = RealmConfigDocument::default_for_realm(realm, Vec::new());
    let realm_roles = RealmAuthorizationDocument::default_realm_doc(realm);
    let decide = |group_roles: Option<Vec<u8>>| {
        let request = read_request(local, [4u8; 32]);
        let mut operation =
            IncomingBaoOperation::new(peer, local, realm, Ulid::from(9u128), request)
                .with_snapshot();
        let next = super::PolicyNext::Exact;
        let turn = operation.start_policy(group_id, path.clone(), next);
        let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
            panic!("expected the policy read, got {turn:?}")
        };
        assert_eq!(reads.len(), 4);
        let values = [
            Some(postcard::to_allocvec(&config).unwrap()),
            Some(group.to_bytes(&actor).unwrap()),
            Some(realm_roles.to_bytes(&actor).unwrap()),
            group_roles,
        ];
        let values = values.into_iter().map(|row| row.map(Into::into)).collect();
        operation.next(IoResult::Values(values)).unwrap();
        operation.policy_allowed
    };
    assert!(decide(Some(roles.to_bytes(&actor).unwrap())));
    assert!(!decide(Some(group.to_bytes(&actor).unwrap())));
    assert!(!decide(None));
}

fn device_realm_value(peer: aruna_core::id::NodeId, owner: UserId) -> byteview::ByteView {
    let mut config = RealmConfigDocument::default_for_realm(test_realm(), Vec::new());
    config.ensure_node(peer, RealmNodeKind::User { owner });
    postcard::to_allocvec(&config).unwrap().into()
}

fn device_operation(
    local_node: aruna_core::id::NodeId,
    peer: aruna_core::id::NodeId,
    asserted: UserId,
) -> IncomingBaoOperation {
    let mut request = read_request(local_node, [4u8; 32]);
    request.auth_context.user_id = asserted;
    IncomingBaoOperation::new(peer, local_node, test_realm(), Ulid::from(9u128), request)
}

// A device is a realm member without internal trust: it is admitted, and the
// serve is authorized as the owner its realm config names.
#[test]
fn serves_device_owner() {
    let local_node = node_from_seed(1);
    let peer = node_from_seed(2);
    let owner = UserId::new(Ulid::from(11u128), test_realm());
    let mut operation = device_operation(local_node, peer, owner);

    operation.first().unwrap();
    let turn = operation.next(row(Some(device_realm_value(peer, owner))));

    assert!(matches!(&turn, Ok(Next::Call(IoCall::Read(_)))));
    assert_eq!(operation.request.auth_context.user_id, owner);
    // A device is never a placement destination, so nothing governed lands.
    assert_eq!(operation.peer_placement, None);
}

#[test]
fn refuses_device_impersonation() {
    let local_node = node_from_seed(1);
    let peer = node_from_seed(2);
    let owner = UserId::new(Ulid::from(11u128), test_realm());
    let other = UserId::new(Ulid::from(12u128), test_realm());
    let mut operation = device_operation(local_node, peer, other);

    operation.first().unwrap();
    let turn = operation.next(row(Some(device_realm_value(peer, owner))));

    assert_eq!(refusal_from(&turn), BaoReadRefusal::ReadDenied);
}

// Governed content stays out of reach: the device claims no subject, so the
// destination challenge denies before a byte is offered.
#[test]
fn refuses_governed_device() {
    let local_node = node_from_seed(1);
    let peer = node_from_seed(2);
    let owner = UserId::new(Ulid::from(11u128), test_realm());
    let policy_ref = PlacementPolicyRef {
        policy_id: Ulid::from(21u128),
        digest: [7u8; 32],
    };
    let mut operation = device_operation(local_node, peer, owner);
    operation.request.known_refs = vec![policy_ref];
    operation.version_refs = vec![policy_ref];
    operation.peer_placement = None;

    let (location, _) = location_value([4u8; 32]);
    let turn = operation.challenge_destination(location);

    let Some(BlobEffect::SendMessage { payload, .. }) = asked(&turn).as_blob() else {
        panic!("expected a policy frame")
    };
    assert!(matches!(
        VersionReplicationMessage::from_bytes(payload).unwrap(),
        VersionReplicationMessage::PlacementPolicyDenied { .. }
    ));
}

// A device gets one honest, terminal answer for governed content instead of
// a negotiation it can never finish.
#[test]
fn device_answer_terminal() {
    let policy_ref = PlacementPolicyRef {
        policy_id: Ulid::from(31u128),
        digest: [5u8; 32],
    };
    for error in [
        BaoReadError::PolicyRequired {
            refs: vec![policy_ref],
        },
        BaoReadError::PolicyDenied {
            policy_ids: vec![policy_ref.policy_id],
        },
        BaoReadError::NoDestination,
    ] {
        assert_eq!(
            crate::replication::managed_read::device_answer(Err(error)),
            Err(BaoReadError::GovernedUnavailable)
        );
    }
    assert_eq!(
        crate::replication::managed_read::device_answer(Err(BaoReadError::Refused(
            BaoReadRefusal::NotFound
        ))),
        Err(BaoReadError::Refused(BaoReadRefusal::NotFound))
    );
}

#[test]
fn rejects_unknown_peer() {
    let local_node = node_from_seed(1);
    let peer = node_from_seed(2);
    let configured_peer = node_from_seed(3);
    let mut operation = IncomingBaoOperation::new(
        peer,
        local_node,
        test_realm(),
        Ulid::from(9u128),
        read_request(local_node, [4u8; 32]),
    );

    operation.first().unwrap();
    let turn = operation.next(row(Some(realm_value(configured_peer))));

    assert_eq!(refusal_from(&turn), BaoReadRefusal::RealmPeerDenied);
}

#[test]
fn snapshot_reads_txn() {
    let local_node = node_from_seed(1);
    let peer = node_from_seed(2);
    let mut operation = IncomingBaoOperation::new(
        peer,
        local_node,
        test_realm(),
        Ulid::from(9u128),
        read_request(local_node, [4u8; 32]),
    )
    .with_snapshot();

    // Every read of the serve joins one read snapshot.
    assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
}

#[test]
fn checks_serving_realm() {
    // A user of another realm reads through this realm; a session of another realm, or an
    // anonymous identity of another realm, is refused.
    let (local_node, peer) = (node_from_seed(1), node_from_seed(2));
    let foreign = RealmId::from_bytes([8u8; 32]);
    for (user_id, serving_realm, accepted) in [
        (
            UserId::local(Ulid::from(5u128), foreign),
            test_realm(),
            true,
        ),
        (
            UserId::local(Ulid::from(5u128), test_realm()),
            foreign,
            false,
        ),
        (UserId::nil(foreign), test_realm(), false),
    ] {
        let mut request = read_request(local_node, [4u8; 32]);
        request.auth_context.user_id = user_id;
        request.auth_context.realm_id = serving_realm;
        let stream_id = Ulid::from(9u128);
        let mut operation =
            IncomingBaoOperation::new(peer, local_node, test_realm(), stream_id, request);
        let turn = operation.first();
        if accepted {
            assert!(matches!(&turn, Ok(Next::Call(IoCall::Read(_)))));
        } else {
            assert_eq!(refusal_from(&turn), BaoReadRefusal::RealmPeerDenied);
        }
    }
}

#[test]
fn denies_wire_read() {
    let local_node = node_from_seed(1);
    let peer = node_from_seed(2);
    let hash = [4u8; 32];
    let mut operation = IncomingBaoOperation::new(
        peer,
        local_node,
        test_realm(),
        Ulid::from(9u128),
        read_request(local_node, hash),
    );

    operation.first().unwrap();
    operation.next(row(Some(realm_value(peer)))).unwrap();
    let turn = operation.next(row(Some(bucket_value())));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::ReadDenied);
}

/// A request for one version of `set_id` by its logical identity.
fn version_request(set_id: Ulid, hash: [u8; 32]) -> BaoReadRequest {
    let mut request = read_request(node_from_seed(1), hash);
    request.target = BaoReadTarget::Version {
        set_id,
        version_id: Ulid::from(4u128),
    };
    request
}

fn bound_bucket(
    binding: Option<aruna_core::structs::storage::distribution::DistributionBinding>,
) -> byteview::ByteView {
    let mut bucket = BucketInfo::from_bytes(&bucket_value()).unwrap();
    bucket.distribution = binding;
    bucket.to_bytes().unwrap().into()
}

// A logical version is found through the set's record and version id index in this node's
// member bucket, then served like an exact version of that bucket.
#[test]
fn serves_logical_version() {
    use aruna_core::structs::storage::distribution::OriginalEpoch;
    use aruna_core::structs::storage::distribution::inventory::{
        IndexEntry, VersionEntry, version_position,
    };
    let (snapshot, record) = crate::distribution::test_record::record(true);
    let set_id = snapshot.set_id();
    let (local_node, peer) = (node_from_seed(1), node_from_seed(2));
    let hash = [4u8; 32];
    let path = aruna_core::structs::storage::blob::object_permission_path(
        test_realm(),
        Ulid::from(5u128),
        local_node,
        "spire",
        "path/file.txt",
    );
    let index = IndexEntry {
        position: version_position(
            set_id,
            "path/file.txt",
            OriginalEpoch(1),
            1,
            Ulid::from(4u128),
        ),
        version: VersionEntry::default(),
    };
    let start = || {
        let mut operation = IncomingBaoOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            version_request(set_id, hash),
        )
        .with_policy_paths(HashSet::from([path.clone()]));
        operation.first().unwrap();
        let turn = operation.next(row(Some(realm_value(peer))));
        assert!(matches!(
            &turn,
        Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 2
        ));
        let turn = operation.next(rows(vec![
            Some(record.clone().into()),
            Some(index.to_bytes().unwrap().into()),
        ]));
        assert!(matches!(
            &turn,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].1.as_ref() == b"spire"
        ));
        operation
    };

    // A bucket of the name that is not the set's member never answers for it.
    let mut stranger = start();
    let turn = stranger.next(row(Some(bound_bucket(None))));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::NotFound);

    let mut operation = start();
    let binding = crate::distribution::test_record::binding(&snapshot, 1, 5);
    operation
        .next(row(Some(bound_bucket(Some(binding)))))
        .unwrap();
    let turn = operation.next(row(Some(version_value(hash))));
    assert!(matches!(&turn, Ok(Next::Call(IoCall::Read(_)))));
}

// A node that keeps only the record of a distributed version serves its verified attachment.
#[test]
fn serves_kept_copy() {
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::storage::distribution::on_use::Attachment;
    let (local_node, peer) = (node_from_seed(1), node_from_seed(2));
    let hash = [4u8; 32];
    let set_id = Ulid::from(8u128);
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::ArunaNative,
            public_config: HashMap::from([("set_id".to_string(), set_id.to_string())]),
            source_path: "bucket/path/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: None,
        mirrors: Vec::new(),
    };
    let observed = SourceMetadata {
        content_length: 42,
        content_type: None,
        etag: None,
        last_modified: None,
        source_version: None,
        digests: Default::default(),
    };
    let record_only = BlobVersion::reference(
        source,
        observed,
        SystemTime::UNIX_EPOCH,
        UserId::nil(test_realm()),
        SystemTime::UNIX_EPOCH,
    )
    .with_identity(Some(hash));
    let mut operation = IncomingBaoOperation::new(
        peer,
        local_node,
        test_realm(),
        Ulid::from(9u128),
        read_request(local_node, hash),
    )
    .with_policy_paths(HashSet::from([read_path(local_node)]));
    operation.first().unwrap();
    for value in [realm_value(peer), bucket_value()] {
        operation.next(row(Some(value))).unwrap();
    }
    let turn = operation.next(row(Some(record_only.to_bytes().unwrap().into())));
    assert!(matches!(
        &turn,
        Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 3
    ));
    let (location, _) = location_value(hash);
    let attached = Attachment {
        blake3: hash,
        location: location.clone(),
    };
    operation
        .next(rows(vec![Some(attached.to_bytes().unwrap().into()), None]))
        .unwrap();
    let turn = operation.next(row(None));
    let Some(BlobEffect::SendMessage { payload, .. }) = asked(&turn).as_blob() else {
        panic!("expected accepted frame")
    };
    assert!(matches!(
        VersionReplicationMessage::from_bytes(payload).unwrap(),
        VersionReplicationMessage::BaoReadAccepted { size: 42, blake3, .. } if blake3 == hash
    ));
}

// An exact read of a record kept without a copy names a member holding the range's bytes,
// so a device can read the version there.
#[test]
fn names_byte_holder() {
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    let (snapshot, record) = crate::distribution::test_record::record(true);
    let set_id = snapshot.set_id();
    let (local_node, peer) = (node_from_seed(1), node_from_seed(2));
    let hash = [4u8; 32];
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::ArunaNative,
            public_config: HashMap::from([("set_id".to_string(), set_id.to_string())]),
            source_path: "bucket/path/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: None,
        mirrors: Vec::new(),
    };
    let observed = SourceMetadata {
        content_length: 42,
        content_type: None,
        etag: None,
        last_modified: None,
        source_version: None,
        digests: Default::default(),
    };
    let record_only = BlobVersion::reference(
        source,
        observed,
        SystemTime::UNIX_EPOCH,
        UserId::nil(test_realm()),
        SystemTime::UNIX_EPOCH,
    )
    .with_identity(Some(hash));
    let mut operation = IncomingBaoOperation::new(
        peer,
        local_node,
        test_realm(),
        Ulid::from(9u128),
        read_request(local_node, hash),
    )
    .with_policy_paths(HashSet::from([read_path(local_node)]));
    operation.first().unwrap();
    for value in [realm_value(peer), bucket_value()] {
        operation.next(row(Some(value))).unwrap();
    }
    operation
        .next(row(Some(record_only.to_bytes().unwrap().into())))
        .unwrap();
    let turn = operation.next(rows(vec![None, None, Some(record.into())]));
    let holder = crate::distribution::test_record::secret(2).public();
    assert_eq!(
        refusal_from(&turn),
        BaoReadRefusal::HeldBy { holder, set_id }
    );
}

// A holder serves its verified copy of an external reference by the copy's own BLAKE3: the
// reference's identity digests its source observation, not its bytes.
#[test]
fn serves_kept_reference() {
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::storage::distribution::on_use::CacheEntry;
    let (local_node, peer) = (node_from_seed(1), node_from_seed(2));
    let (identity, bytes) = ([6u8; 32], [4u8; 32]);
    let set_id = Ulid::from(8u128);
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::S3,
            public_config: HashMap::from([("bucket".to_string(), "reads".to_string())]),
            source_path: "path/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: None,
        mirrors: Vec::new(),
    };
    let observed = SourceMetadata {
        content_length: 42,
        content_type: None,
        etag: None,
        last_modified: None,
        source_version: None,
        digests: Default::default(),
    };
    let reference = BlobVersion::reference(
        source,
        observed,
        SystemTime::UNIX_EPOCH,
        UserId::nil(test_realm()),
        SystemTime::UNIX_EPOCH,
    )
    .with_identity(Some(identity));
    let mut request = version_request(set_id, bytes);
    request.expected_blake3 = None;
    let mut operation =
        IncomingBaoOperation::new(peer, local_node, test_realm(), Ulid::from(9u128), request);
    operation.policy_allowed = true;
    operation.located = Some(VersionedObjectArn {
        realm_id: test_realm(),
        node_id: local_node,
        bucket: "bucket".to_string(),
        key: "path/file.txt".to_string(),
        version: Ulid::from(4u128),
    });
    let held = operation
        .held_version(&reference)
        .expect("a kept reference");
    assert_eq!((held.identity, held.blake3), (identity, None));
    operation.read_kept(held).unwrap();
    let (location, _) = location_value(bytes);
    let entry = CacheEntry {
        key: identity,
        blake3: bytes,
        location,
        size: 42,
        order: Ulid::from(3u128),
    };
    operation
        .next(rows(vec![None, Some(entry.to_bytes().unwrap().into())]))
        .unwrap();
    let turn = operation.next(row(None));
    let Some(BlobEffect::SendMessage { payload, .. }) = asked(&turn).as_blob() else {
        panic!("expected accepted frame")
    };
    assert!(matches!(
        VersionReplicationMessage::from_bytes(payload).unwrap(),
        VersionReplicationMessage::BaoReadAccepted { size: 42, blake3, .. } if blake3 == bytes
    ));
}

#[test]
fn serves_exact_blob() {
    let local_node = node_from_seed(1);
    let peer = node_from_seed(2);
    let hash = [4u8; 32];
    let stream_id = Ulid::from(9u128);
    let mut request = read_request(local_node, hash);
    request.expected_blake3 = None;
    let mut operation =
        IncomingBaoOperation::new(peer, local_node, test_realm(), stream_id, request)
            .with_policy_paths(HashSet::from([read_path(local_node)]));

    operation.first().unwrap();
    operation.next(row(Some(realm_value(peer)))).unwrap();
    operation.next(row(Some(bucket_value()))).unwrap();
    operation.next(row(Some(version_value(hash)))).unwrap();
    let (location, value) = location_value(hash);
    operation.next(row(Some(value))).unwrap();
    // The bucket has no encryption settings, so the plain copy is offered without a lease.
    let turn = operation.next(row(None));
    let Some(BlobEffect::SendMessage { payload, .. }) = asked(&turn).as_blob() else {
        panic!("expected accepted frame")
    };
    assert_eq!(
        VersionReplicationMessage::from_bytes(payload).unwrap(),
        VersionReplicationMessage::BaoReadAccepted {
            size: 42,
            blake3: hash,
            etag: location.hashes.get(HASH_MD5).map(hex::encode),
            hashes: location.hashes.clone().into_iter().collect(),
        }
    );

    let turn = operation.next(blob(BlobEvent::MessageSent { stream_id }));
    assert!(
        matches!((asked(&turn)).as_blob(), Some(BlobEffect::ServeRead {
            stream_id: id,
            location: selected,
            expected_blake3,
            range: None,
        }) if *id == stream_id && selected == &location && expected_blake3 == &hash)
    );
    assert_eq!(
        operation.next(blob(BlobEvent::ReadServed { stream_id })),
        Ok(Next::Done(IncomingBaoResult::Served))
    );
}

#[test]
fn rejects_hash_mismatch() {
    let local_node = node_from_seed(1);
    let peer = node_from_seed(2);
    let mut operation = IncomingBaoOperation::new(
        peer,
        local_node,
        test_realm(),
        Ulid::from(9u128),
        read_request(local_node, [4u8; 32]),
    )
    .with_policy_paths(HashSet::from([read_path(local_node)]));

    operation.first().unwrap();
    operation.next(row(Some(realm_value(peer)))).unwrap();
    operation.next(row(Some(bucket_value()))).unwrap();
    let turn = operation.next(row(Some(version_value([5u8; 32]))));

    assert_eq!(refusal_from(&turn), BaoReadRefusal::HashMismatch);
}

#[test]
fn hash_requires_candidates() {
    let local_node = node_from_seed(1);
    let peer = node_from_seed(2);
    let hash = [4u8; 32];
    let mut request = read_request(local_node, hash);
    request.target = BaoReadTarget::Blake3(hash);
    let mut operation =
        IncomingBaoOperation::new(peer, local_node, test_realm(), Ulid::from(9u128), request);

    operation.first().unwrap();
    let turn = operation.next(row(Some(realm_value(peer))));

    assert_eq!(refusal_from(&turn), BaoReadRefusal::BackendFailure);
}

/// An incoming read that resolved to a sealed copy of `bucket_id`, ready to be offered.
fn sealed_serve(bucket_id: Ulid) -> (IncomingBaoOperation, BackendLocation) {
    use aruna_core::structs::storage::encryption::BucketKeyRef;
    use aruna_core::structs::storage::format::PithosLayout;

    let local_node = node_from_seed(1);
    let hash = [4u8; 32];
    let mut operation = IncomingBaoOperation::new(
        node_from_seed(2),
        local_node,
        test_realm(),
        Ulid::from(9u128),
        read_request(local_node, hash),
    );
    operation.blob_hash = Some(hash);
    let layout = PithosLayout {
        stored_size: 64,
        metadata_digest: [1; 32],
        storage_generation: 0,
    };
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: "sealed".to_string(),
        ulid: Ulid::from(12u128),
        format: StoredFormat::pithos(layout, BucketKeyRef::new(bucket_id, 1)),
        created_by: UserId::nil(test_realm()),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 10,
        hashes: HashMap::from([(HASH_BLAKE3.to_string(), hash.to_vec())]),
    };
    (operation, location)
}

#[test]
fn locked_copy_refused() {
    use aruna_core::errors::BlobError;
    use aruna_core::structs::storage::encryption::BucketKeyError;

    let bucket_id = Ulid::from(30u128);
    let (mut operation, location) = sealed_serve(bucket_id);
    let turn = operation.send_accepted(location);
    assert!(
        matches!((asked(&turn)).as_blob(), Some(BlobEffect::AdmitRead { key, .. }) if key.bucket_id == bucket_id)
    );
    let locked = BlobError::BucketKey(BucketKeyError::Locked(bucket_id));
    let turn = operation.next(blob(BlobEvent::Error(locked)));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::BucketLocked(bucket_id));
}

#[test]
fn unlocked_copy_leased() {
    use aruna_core::structs::storage::blob::ArchiveKey;
    use aruna_core::structs::storage::encryption::{BucketKeyRef, ReadLease};
    use std::sync::Arc;

    let bucket_id = Ulid::from(30u128);
    let (mut operation, location) = sealed_serve(bucket_id);
    operation.send_accepted(location.clone()).unwrap();
    let key = BucketKeyRef::new(bucket_id, 1);
    let lease = ReadLease::new(
        key,
        ArchiveKey::of(&location),
        Ulid::from(31u128),
        Arc::new(()),
    );
    let turn = operation.next(blob(BlobEvent::ReadAdmitted { lease }));
    let Some(BlobEffect::SendMessage { payload, .. }) = asked(&turn).as_blob() else {
        panic!("expected the accepted frame, got {turn:?}")
    };
    assert!(matches!(
        VersionReplicationMessage::from_bytes(payload).unwrap(),
        VersionReplicationMessage::BaoReadAccepted { .. }
    ));
    let turn = operation.next(blob(BlobEvent::MessageSent {
        stream_id: Ulid::from(9u128),
    }));
    assert!(
        matches!((asked(&turn)).as_blob(), Some(BlobEffect::ServeSealedRead { lease, .. }) if lease.key == key)
    );
}

#[test]
fn unconverted_copy_refused() {
    use aruna_core::errors::BlobError;
    use aruna_core::structs::storage::blob::{ArchiveKey, VersionKey};
    use aruna_core::structs::storage::encryption::{
        BucketEncryption, BucketKeyError, BucketKeyRef, EncryptionMode, ReadLease,
    };
    use std::sync::Arc;

    let bucket_id = Ulid::from(30u128);
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(bucket_id),
        key_generation: 1,
        ..Default::default()
    };
    let settings_row = || row(Some(settings.to_bytes().unwrap().into()));
    let plain = || {
        let (mut operation, mut location) = sealed_serve(bucket_id);
        location.format = StoredFormat::default();
        operation.version_key = Some(VersionKey::new(
            "bucket".to_string(),
            "path/file.txt".to_string(),
            Ulid::from(4u128),
        ));
        (operation, location)
    };
    let key = BucketKeyRef::new(bucket_id, 1);
    let archive = ArchiveKey::new(Ulid::from(12u128), BackendRef::node_default());

    // Locked: a raw copy not converted yet is refused like a sealed one.
    let (mut operation, location) = plain();
    let turn = operation.send_accepted(location);
    assert!(matches!(&turn, Ok(Next::Call(IoCall::Read(_)))));
    let turn = operation.next(settings_row());
    assert!(
        matches!((asked(&turn)).as_blob(), Some(BlobEffect::AdmitRead { key: admitted, archive: named })
            if *admitted == key && *named == archive)
    );
    let locked = BlobError::BucketKey(BucketKeyError::Locked(bucket_id));
    let turn = operation.next(blob(BlobEvent::Error(locked)));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::BucketLocked(bucket_id));

    // Unlocked: it is served under the bucket lease.
    let (mut operation, location) = plain();
    operation.send_accepted(location).unwrap();
    operation.next(settings_row()).unwrap();
    let lease = ReadLease::new(key, archive, Ulid::from(31u128), Arc::new(()));
    operation
        .next(blob(BlobEvent::ReadAdmitted { lease }))
        .unwrap();
    let turn = operation.next(blob(BlobEvent::MessageSent {
        stream_id: Ulid::from(9u128),
    }));
    assert!(
        matches!((asked(&turn)).as_blob(), Some(BlobEffect::ServeSealedRead { lease, .. }) if lease.key == key)
    );

    // An unprotected workflow read refuses both sealed blobs and encrypting buckets.
    let (mut operation, sealed_loc) = sealed_serve(bucket_id);
    operation.request.disallow_encrypted = true;
    let turn = operation.send_accepted(sealed_loc);
    assert_eq!(refusal_from(&turn), BaoReadRefusal::ReadDenied);

    let (mut operation, location) = plain();
    operation.request.disallow_encrypted = true;
    operation.request.metadata_only = true;
    let turn = operation.send_accepted(location);
    assert!(matches!(&turn, Ok(Next::Call(IoCall::Read(_)))));
    let turn = operation.next(settings_row());
    assert_eq!(refusal_from(&turn), BaoReadRefusal::ReadDenied);
}

#[test]
fn locked_observation_refused() {
    use aruna_core::errors::BlobError;
    use aruna_core::events::SubOperationEvent;
    use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::storage::blob::ArchiveKey;
    use aruna_core::structs::storage::encryption::{
        BucketEncryption, BucketKeyError, BucketKeyRef, EncryptionMode, ReadLease,
    };
    use std::sync::Arc;

    let bucket_id = Ulid::from(40u128);
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(bucket_id),
        key_generation: 1,
        ..Default::default()
    };
    let resolved = || {
        let mut operation = device_serve(Some([5u8; 32]));
        operation
            .serve_observation(observation(5, Some("5-1")))
            .unwrap();
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::new(),
            path: "file.txt".to_string(),
            version: None,
        };
        let resolved = SubOperationEvent::VersionAccessResolved {
            result: Box::new(Ok(access)),
        };
        let event = Event::SubOperation(resolved);
        let turn = operation.next(IoResult::Event(Box::new(event)));
        assert!(matches!(&turn, Ok(Next::Call(IoCall::Read(_)))));
        let turn = operation.next(row(Some(settings.to_bytes().unwrap().into())));
        assert!(
            matches!((asked(&turn)).as_blob(), Some(BlobEffect::AdmitRead { key, .. }) if key.bucket_id == bucket_id)
        );
        operation
    };

    // Locked: the device's file is refused before any accepted frame.
    let mut operation = resolved();
    let locked = BlobError::BucketKey(BucketKeyError::Locked(bucket_id));
    let turn = operation.next(blob(BlobEvent::Error(locked)));
    assert_eq!(refusal_from(&turn), BaoReadRefusal::BucketLocked(bucket_id));

    // Unlocked: the file is served while the operation holds the bucket lease.
    let mut operation = resolved();
    let key = BucketKeyRef::new(bucket_id, 1);
    let archive = ArchiveKey::new(bucket_id, BackendRef::node_default());
    let guard = Arc::new(());
    let lease = ReadLease::new(key, archive, Ulid::from(41u128), guard.clone());
    operation
        .next(blob(BlobEvent::ReadAdmitted { lease }))
        .unwrap();
    let turn = operation.next(blob(BlobEvent::MessageSent {
        stream_id: Ulid::from(9u128),
    }));
    assert!(matches!(
        (asked(&turn)).as_blob(),
        Some(BlobEffect::ServeSourceRead { .. })
    ));
    assert_eq!(Arc::strong_count(&guard), 2);
}
