//! Drives what follows a routed export's holder answers, without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{ChildOutcome, IoError};
use aruna_core::events::{MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::metadata::MetadataError;
use aruna_core::structs::identity::realm::{RealmNode, RealmNodeKind};
use aruna_core::structs::placement::record::{METADATA_HANDLE, PlacementRef};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structured_id::{BucketId, MetaResourceId, PlacementHandle, StructuredId};
use aruna_core::user::id::UserId;
use ulid::Ulid;

use super::*;
use crate::device::replica::ReplicaOrigin;
use crate::metadata::forward::holder_reads::peer_error_result;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn owner() -> UserId {
    UserId::local(Ulid::from_bytes([7; 16]), realm())
}

fn export() -> ExportMetadataResult {
    let document_id = Ulid::from_parts(3, 3);
    let record = MetadataRegistryRecord {
        realm_id: realm(),
        group_id: Ulid::from_parts(2, 2),
        document_id,
        document_path: "datasets/exported".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: false,
        permission_path: String::new(),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_parts(4, 4),
        last_event_id: Ulid::from_parts(5, 5),
    };
    ExportMetadataResult::Full {
        record,
        jsonld: "{}".to_string(),
    }
}

fn forwarded_export(
    peer: NodeId,
    result: Result<Box<ExportMetadataResult>, MetadataReadError>,
) -> GroupResult {
    let message = MetadataTransportMessage::ExportAnswer { result };
    let answer = MetadataAnswer {
        peer,
        elapsed: Duration::ZERO,
        result: Ok(encode_message(&message).unwrap()),
    };
    GroupResult::Done(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        answer,
    )))))
}

fn export_request(document_id: Ulid) -> ExportMetadataRequest {
    ExportMetadataRequest {
        document_id,
        auth: None,
        view: RoCrateExportView::Full,
        limit: None,
        offset: None,
        after: None,
    }
}

/// An export on the owner's device, as `user_id`, whose one holder answered.
fn answered(user_id: UserId) -> RoutedExportOperation {
    let request = ExportMetadataRequest {
        document_id: Ulid::from_parts(3, 3),
        auth: Some(AuthContext {
            user_id,
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        }),
        view: RoCrateExportView::Full,
        limit: None,
        offset: None,
        after: None,
    };
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    config.nodes.push(RealmNode {
        node_id: node(1).to_string(),
        kind: RealmNodeKind::User { owner: owner() },
    });
    let mut routed = RoutedExportOperation::new(Some(node(1)), realm(), request, None, 1_000);
    routed.config = Some(config);
    routed.holders = vec![node(2)];
    routed
}

#[test]
fn owner_export_kept() {
    let mut routed = answered(owner());
    let results = vec![forwarded_export(node(2), Ok(Box::new(export())))];
    assert!(matches!(
        routed.collect(results),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    // Keeping is best effort, so its failure still answers with the export.
    let failed = Err::<(), _>(IoError::Unfinished);
    let answer = routed.next(IoResult::Child(ChildOutcome::new(failed)));
    assert_eq!(answer, Ok(Next::Done(export())));
}

#[test]
fn other_caller_answers() {
    let mut routed = answered(UserId::local(Ulid::from_bytes([8; 16]), realm()));
    let results = vec![forwarded_export(node(2), Ok(Box::new(export())))];
    assert_eq!(routed.collect(results), Ok(Next::Done(export())));
}

#[test]
fn sole_local_skips() {
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    config.seed_default_placement();
    config.ensure_node(node(1), RealmNodeKind::Server);
    let document_id = MetaResourceId::from_parts(
        1,
        PlacementHandle::new(METADATA_HANDLE).unwrap(),
        BucketId::new(1).unwrap(),
        1,
    )
    .unwrap()
    .as_ulid();
    let mut routed = RoutedExportOperation::new(
        Some(node(1)),
        realm(),
        export_request(document_id),
        None,
        1_000,
    );
    routed.first().unwrap();
    let turn = routed
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            config,
        ))))
        .unwrap();
    assert!(matches!(turn, Next::Call(IoCall::Child(_))));
    assert_eq!(routed.stage, RoutedStage::SoleLocal);
    let done = routed
        .next(IoResult::Child(ChildOutcome::new(
            Ok::<_, MetadataApiError>(export()),
        )))
        .unwrap();
    assert_eq!(done, Next::Done(export()));
}

#[test]
fn multi_holder_conflicts() {
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    config.seed_default_placement();
    for seed in [1u8, 2, 3] {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    let document_id = MetaResourceId::from_parts(
        1,
        PlacementHandle::new(METADATA_HANDLE).unwrap(),
        BucketId::new(1).unwrap(),
        1,
    )
    .unwrap()
    .as_ulid();
    let placement = resolve_metadata_id(&config, realm(), None, document_id).unwrap();
    let holders = read_holder_sets(&config, &placement).unwrap();
    assert!(holders.len() > 1);

    let mut routed = RoutedExportOperation::new(
        Some(holders[0]),
        realm(),
        export_request(document_id),
        None,
        1_000,
    );
    routed.first().unwrap();
    let Next::Call(IoCall::Group(group)) = routed
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            config.clone(),
        ))))
        .unwrap()
    else {
        panic!("expected multi-holder export group");
    };
    assert_eq!(group.members.len(), holders.len());
    assert_eq!(
        group.form,
        GroupForm::All {
            deadline: Some(METADATA_READ_DEADLINE)
        }
    );

    // One holder returning NotFound alongside another returning Ok fails closed as ServiceUnavailable.
    let mut conflict = RoutedExportOperation::new(
        Some(node(9)),
        realm(),
        export_request(document_id),
        None,
        1_000,
    );
    conflict.config = Some(config.clone());
    conflict.holders = vec![node(2), node(3)];
    assert_eq!(
        conflict.collect(vec![
            forwarded_export(node(2), Ok(Box::new(export()))),
            forwarded_export(node(3), Err(MetadataReadError::NotFound)),
        ]),
        Err(MetadataApiError::ServiceUnavailable)
    );

    // A Forbidden from one holder wins over a lagging peer's Ok.
    let mut denied = RoutedExportOperation::new(
        Some(node(9)),
        realm(),
        export_request(document_id),
        None,
        1_000,
    );
    denied.config = Some(config.clone());
    denied.holders = vec![node(2), node(3)];
    assert_eq!(
        denied.collect(vec![
            forwarded_export(node(2), Ok(Box::new(export()))),
            forwarded_export(node(3), Err(MetadataReadError::Forbidden)),
        ]),
        Err(MetadataApiError::Forbidden)
    );

    // Only peer-attributable transport errors trigger AvoidHolder; local capacity errors do not.
    let mut faulted = RoutedExportOperation::new(
        Some(node(9)),
        realm(),
        export_request(document_id),
        None,
        1_000,
    );
    faulted.stamp(7_000);
    faulted.config = Some(config);
    faulted.holders = vec![node(2), node(3), node(4)];
    let Next::Call(IoCall::Group(avoid)) = faulted
        .collect(vec![
            peer_error_result(node(2), PeerRequestError::TimedOut),
            peer_error_result(
                node(3),
                PeerRequestError::NotSent(MetadataError::InvalidInput("bad".to_string())),
            ),
            peer_error_result(
                node(4),
                PeerRequestError::NotSent(MetadataError::LocalUnavailable(
                    "too many pooled connections".to_string(),
                )),
            ),
        ])
        .unwrap()
    else {
        panic!("expected AvoidHolder for timed-out peer only");
    };
    assert_eq!(
        avoid.members,
        vec![Effect::Net(NetEffect::AvoidHolder {
            node_id: node(2),
            now_ms: 7_000,
        })]
    );
}

#[test]
fn conflict_skips_replica() {
    let sample = export();
    let ExportMetadataResult::Full { record, .. } = &sample else {
        unreachable!();
    };
    let document_id = record.document_id;
    let mut replica = ReplicaRecord::new(
        document_id,
        record.group_id,
        record.document_path.clone(),
        ReplicaOrigin::Realm,
    );
    replica.selected = false;
    replica.record = Some(Box::new(record.clone()));
    replica.displayed_jsonld = "{}".to_string();

    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    config.seed_default_placement();
    for seed in [1u8, 2, 3, 4] {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }

    let mut conflict = RoutedExportOperation::new(
        Some(node(9)),
        realm(),
        export_request(document_id),
        None,
        1_000,
    );
    conflict.config = Some(config.clone());
    conflict.replica = Some(replica.clone());
    conflict.holders = vec![node(2), node(3)];
    assert_eq!(
        conflict.collect(vec![
            forwarded_export(node(2), Ok(Box::new(sample.clone()))),
            forwarded_export(node(3), Err(MetadataReadError::NotFound)),
        ]),
        Err(MetadataApiError::ServiceUnavailable)
    );

    let mut avoided = RoutedExportOperation::new(
        Some(node(9)),
        realm(),
        export_request(document_id),
        None,
        1_000,
    );
    avoided.config = Some(config.clone());
    avoided.replica = Some(replica.clone());
    avoided.holders = vec![node(2), node(3), node(4)];
    assert!(matches!(
        avoided.collect(vec![
            forwarded_export(node(2), Ok(Box::new(sample))),
            forwarded_export(node(3), Err(MetadataReadError::NotFound)),
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

    // Genuine unavailability without any NotFound still falls back to the device replica.
    let mut offline = RoutedExportOperation::new(
        Some(node(9)),
        realm(),
        export_request(document_id),
        None,
        1_000,
    );
    offline.config = Some(config);
    offline.replica = Some(replica);
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
    assert_eq!(offline.stage, RoutedStage::Device);
}
