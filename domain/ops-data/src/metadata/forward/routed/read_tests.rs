//! Drives the routed read through its config, holder and fallback answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::device::replica::ReplicaOrigin;
use crate::metadata::forward::holder_reads::peer_error_result;
use aruna_core::calls::ChildOutcome;
use aruna_core::events::{MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::metadata::MetadataError;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::placement::record::METADATA_HANDLE;
use aruna_core::structured_id::{BucketId, MetaResourceId, PlacementHandle, StructuredId};
use std::time::Duration;
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn request() -> GetVisibleRequest {
    GetVisibleRequest {
        document_id: MetaResourceId::from_parts(
            1,
            PlacementHandle::new(METADATA_HANDLE).unwrap(),
            BucketId::new(1).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid(),
        auth: None,
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn forwarded(
    peer: NodeId,
    result: Result<Box<MetadataRegistryRecord>, MetadataReadError>,
) -> GroupResult {
    let message = MetadataTransportMessage::ForwardedRead { result };
    let answer = MetadataAnswer {
        peer,
        elapsed: Duration::ZERO,
        result: Ok(encode_message(&message).unwrap()),
    };
    GroupResult::Done(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        answer,
    )))))
}

fn config_with_nodes(seeds: &[u8]) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    config.seed_default_placement();
    for &seed in seeds {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    config
}

fn record_for(
    config: &RealmConfigDocument,
    document_id: Ulid,
    placement: PlacementRef,
) -> MetadataRegistryRecord {
    let group_id = Ulid::from_parts(2, 2);
    MetadataRegistryRecord {
        realm_id: config.realm_id,
        group_id,
        document_id,
        document_path: "datasets/one".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &config.realm_id,
            group_id,
            "datasets/one",
            document_id,
        ),
        placement,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_parts(4, 4),
        last_event_id: Ulid::from_parts(4, 4),
    }
}

/// A read past its config whose holders are the given peers.
fn routed(holders: Vec<NodeId>) -> RoutedReadOperation {
    let mut read = RoutedReadOperation::new(Some(node(1)), realm(), request(), None);
    read.config = Some(RealmConfigDocument::default_for_realm(realm(), Vec::new()));
    read.placement = Some(PlacementRef::NIL);
    read.holders = holders;
    read
}

#[test]
fn offline_reads_here() {
    let mut read = RoutedReadOperation::new(None, realm(), request(), None);
    assert!(matches!(read.first(), Ok(Next::Call(IoCall::Child(_)))));
    let missing = child(Err::<MetadataRegistryRecord, _>(MetadataApiError::NotFound));
    assert_eq!(read.next(missing), Err(MetadataApiError::NotFound));
}

#[test]
fn unreadable_config_unavailable() {
    let mut read = RoutedReadOperation::new(Some(node(1)), realm(), request(), None);
    read.first().unwrap();
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert_eq!(
        read.next(missing),
        Err(MetadataApiError::ServiceUnavailable)
    );
}

#[test]
fn sole_local_skips() {
    // Only when this node is the sole holder does a read skip the holder fanout group.
    let config = config_with_nodes(&[1]);
    let req = request();
    let placement = resolve_metadata_id(&config, realm(), None, req.document_id).unwrap();
    let record = record_for(&config, req.document_id, placement);

    let mut read = RoutedReadOperation::new(Some(node(1)), realm(), req, None);
    read.first().unwrap();
    let turn = read.next(child(Ok::<_, GetConfigError>(config))).unwrap();
    assert!(matches!(turn, Next::Call(IoCall::Child(_))));
    assert_eq!(read.stage, ReadStage::SoleLocal);
    let done = read
        .next(child(Ok::<_, MetadataApiError>(record.clone())))
        .unwrap();
    assert_eq!(done, Next::Done(record));
}

#[test]
fn multi_holder_fans() {
    // With multiple holders, even a local holder queries every holder under one 12s deadline.
    let config = config_with_nodes(&[1, 2, 3]);
    let req = request();
    let placement = resolve_metadata_id(&config, realm(), None, req.document_id).unwrap();
    let holders = read_holder_sets(&config, &placement).unwrap();
    assert!(holders.len() > 1);

    let mut read = RoutedReadOperation::new(Some(holders[0]), realm(), req, None);
    read.first().unwrap();
    let Next::Call(IoCall::Group(group)) =
        read.next(child(Ok::<_, GetConfigError>(config))).unwrap()
    else {
        panic!("expected multi-holder fanout group");
    };
    assert_eq!(group.members.len(), holders.len());
    assert_eq!(
        group.form,
        GroupForm::All {
            deadline: Some(METADATA_READ_DEADLINE)
        }
    );
}

#[test]
fn conflict_fails_closed() {
    // A lagging holder's record must not override another holder's NotFound or Forbidden.
    let config = config_with_nodes(&[1, 2, 3]);
    let req = request();
    let placement = resolve_metadata_id(&config, realm(), None, req.document_id).unwrap();
    let record = record_for(&config, req.document_id, placement);

    let mut read = RoutedReadOperation::new(Some(node(9)), realm(), req.clone(), None);
    read.config = Some(config.clone());
    read.placement = Some(placement);
    read.holders = vec![node(2), node(3)];
    let results = vec![
        forwarded(node(2), Ok(Box::new(record.clone()))),
        forwarded(node(3), Err(MetadataReadError::NotFound)),
    ];
    assert_eq!(
        read.collect(results),
        Err(MetadataApiError::ServiceUnavailable)
    );

    let mut denied = RoutedReadOperation::new(Some(node(9)), realm(), req, None);
    denied.config = Some(config);
    denied.placement = Some(placement);
    denied.holders = vec![node(2), node(3)];
    let results = vec![
        forwarded(node(2), Ok(Box::new(record))),
        forwarded(node(3), Err(MetadataReadError::Forbidden)),
    ];
    assert_eq!(denied.collect(results), Err(MetadataApiError::Forbidden));
}

#[test]
fn every_holder_missing() {
    let mut read = routed(vec![node(2), node(3)]);
    let results = vec![
        forwarded(node(2), Err(MetadataReadError::NotFound)),
        forwarded(node(3), Err(MetadataReadError::NotFound)),
    ];
    assert_eq!(read.collect(results), Err(MetadataApiError::NotFound));
}

#[test]
fn refusal_wins() {
    let mut read = routed(vec![node(2), node(3)]);
    let results = vec![
        forwarded(node(2), Err(MetadataReadError::NotFound)),
        forwarded(node(3), Err(MetadataReadError::Forbidden)),
    ];
    assert_eq!(read.collect(results), Err(MetadataApiError::Forbidden));
}

#[test]
fn late_holder_unavailable() {
    let mut read = routed(vec![node(2), node(3)]);
    let results = vec![
        forwarded(node(2), Err(MetadataReadError::NotFound)),
        GroupResult::Unfinished,
    ];
    assert_eq!(
        read.collect(results),
        Err(MetadataApiError::ServiceUnavailable)
    );
}

fn unselected_replica(record: MetadataRegistryRecord) -> ReplicaRecord {
    let mut replica = ReplicaRecord::new(
        record.document_id,
        record.group_id,
        record.document_path.clone(),
        ReplicaOrigin::Realm,
    );
    replica.selected = false;
    replica.record = Some(Box::new(record));
    replica.displayed_jsonld = "{}".to_string();
    replica
}

#[test]
fn conflict_skips_replica() {
    // A device with an unselected cached replica must not fall back to it when holders disagree
    // or when any reachable holder reports NotFound, even after avoiding another faulted peer.
    let config = config_with_nodes(&[1, 2, 3, 4]);
    let req = request();
    let placement = resolve_metadata_id(&config, realm(), None, req.document_id).unwrap();
    let record = record_for(&config, req.document_id, placement);

    let mut conflict = RoutedReadOperation::new(Some(node(9)), realm(), req.clone(), None);
    conflict.config = Some(config.clone());
    conflict.placement = Some(placement);
    conflict.replica = Some(unselected_replica(record.clone()));
    conflict.holders = vec![node(2), node(3)];
    assert_eq!(
        conflict.collect(vec![
            forwarded(node(2), Ok(Box::new(record.clone()))),
            forwarded(node(3), Err(MetadataReadError::NotFound)),
        ]),
        Err(MetadataApiError::ServiceUnavailable)
    );

    let mut avoided = RoutedReadOperation::new(Some(node(9)), realm(), req.clone(), None);
    avoided.config = Some(config.clone());
    avoided.placement = Some(placement);
    avoided.replica = Some(unselected_replica(record.clone()));
    avoided.holders = vec![node(2), node(3), node(4)];
    assert!(matches!(
        avoided.collect(vec![
            forwarded(node(2), Ok(Box::new(record.clone()))),
            forwarded(node(3), Err(MetadataReadError::NotFound)),
            peer_error_result(node(4), PeerRequestError::TimedOut),
        ]),
        Ok(Next::Call(IoCall::Group(_)))
    ));
    let done = IoResult::Group(GroupEvent::Finished {
        results: vec![GroupResult::Done(Box::new(Event::Net(
            NetEvent::HolderAvoided,
        )))],
    });
    assert_eq!(
        avoided.next(done),
        Err(MetadataApiError::ServiceUnavailable)
    );

    // Genuine unavailability with no holder reporting NotFound still falls back to the replica.
    let mut offline = RoutedReadOperation::new(Some(node(9)), realm(), req, None);
    offline.config = Some(config);
    offline.placement = Some(placement);
    offline.replica = Some(unselected_replica(record));
    offline.holders = vec![node(2), node(3)];
    assert!(matches!(
        offline.collect(vec![
            peer_error_result(
                node(2),
                PeerRequestError::NotSent(MetadataError::LocalUnavailable(
                    "too many pooled connections".to_string()
                ))
            ),
            peer_error_result(
                node(3),
                PeerRequestError::NotSent(MetadataError::LocalUnavailable(
                    "too many pooled connections".to_string()
                ))
            ),
        ]),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert!(matches!(offline.stage, ReadStage::Readable(_)));
}

#[test]
fn avoids_peer_faults() {
    // Peer-attributable timeouts and dial failures emit AvoidHolder; local pool exhaustion,
    // handle and frame errors never penalize healthy peers.
    let mut read = routed(vec![node(2), node(3), node(4), node(5), node(6)]);
    read.stamp(5_000);
    let results = vec![
        peer_error_result(node(2), PeerRequestError::TimedOut),
        peer_error_result(
            node(3),
            PeerRequestError::NotSent(MetadataError::InvalidInput("bad frame".to_string())),
        ),
        peer_error_result(
            node(4),
            PeerRequestError::Failed(MetadataError::HandleMissing),
        ),
        peer_error_result(
            node(5),
            PeerRequestError::NotSent(MetadataError::LocalUnavailable(
                "too many pooled connections".to_string(),
            )),
        ),
        peer_error_result(
            node(6),
            PeerRequestError::NotSent(MetadataError::Backend("connection failed".to_string())),
        ),
    ];
    let Next::Call(IoCall::Group(avoid)) = read.collect(results).unwrap() else {
        panic!("expected AvoidHolder group for peer faults");
    };
    assert_eq!(
        avoid.members,
        vec![
            Effect::Net(NetEffect::AvoidHolder {
                node_id: node(2),
                now_ms: 5_000,
            }),
            Effect::Net(NetEffect::AvoidHolder {
                node_id: node(6),
                now_ms: 5_000,
            }),
        ]
    );
    let finished = IoResult::Group(GroupEvent::Finished {
        results: vec![
            GroupResult::Done(Box::new(Event::Net(NetEvent::HolderAvoided))),
            GroupResult::Done(Box::new(Event::Net(NetEvent::HolderAvoided))),
        ],
    });
    assert_eq!(
        read.next(finished),
        Err(MetadataApiError::ServiceUnavailable)
    );
}

#[test]
fn local_holder_mapped() {
    let read = routed(vec![node(1)]);
    let denied = Err::<MetadataRegistryRecord, _>(MetadataApiError::Forbidden);
    let member = GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(denied))));
    assert_eq!(
        read.holder_record(node(1), member),
        Err(MetadataReadError::Forbidden)
    );
}
