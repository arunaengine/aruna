use crate::device::replica::ReplicaRecord;
use crate::driver::DriverContext;
use crate::metadata::api::MetadataApiError;
use crate::metadata::create_document::CreateMetadataDocumentConfig;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::handle::MetadataRequestDelivery;
use crate::metadata::handle::MetadataRequestError;
use crate::metadata::protocol::MetadataAuthToken;
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::placement::resolve_shard_holders;
use aruna_core::MetaResourceId;
use aruna_core::NodeId;
use aruna_core::StructuredId;
use aruna_core::admin_documents::AdminDocumentEvent;
use aruna_core::auth::bearer_token_hash;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::Event;
use aruna_core::events::StorageEvent;
use aruna_core::metadata::MetadataError;
use aruna_core::metadata::MetadataMergedRevision;
use aruna_core::metadata::MetadataProfileValidationStatus;
use aruna_core::structs::Actor;
use aruna_core::structs::Group;
use aruna_core::structs::GroupAuthorizationDocument;
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::structs::PlacementRef;
use aruna_core::structs::RealmConfigDocument;
use aruna_core::structs::RealmId;
use aruna_core::structs::RealmNodeKind;
use aruna_core::types::UserId;
use std::sync::Arc;
use tokio::time::Instant;
use ulid::Ulid;

use super::admin::TOKEN_REVOKE_DEADLINE;
use super::admin::TOKEN_REVOKE_PEER_LIMIT;
use super::admin::rank_revoke_peers;
use super::admin::run_revoke;
use super::admin::{RelayAdmission, admit_relayed_admin};
use super::device::DEVICE_GROUP_SCAN_PAGE;
use super::device::device_group_documents;
use super::read::device_raw_revision;
use super::read::keep_status;
use super::replay::create_record_matches;
use super::replay::routed_record_matches;
use super::replay::update_record_matches;
use super::routing::distinct_holders;
use super::routing::holder_intersection;
use super::transport::RetryDisposition;
use super::transport::retry_disposition;

use super::*;

use super::routing::holds_metadata_id;
use crate::device::replica::ReplicaOrigin;
use aruna_core::metadata::{MetadataProfileValidationCompleteness, MetadataProfileValidationState};
use aruna_core::structs::{METADATA_HANDLE, PlacementStrategy};
use aruna_core::structured_id::{BucketId, PlacementHandle};

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn validation_status(revision: Ulid) -> MetadataProfileValidationStatus {
    MetadataProfileValidationStatus {
        document_id: Ulid::nil(),
        dataset_revision: revision,
        state: MetadataProfileValidationState::NotProfiled,
        profile_id: None,
        profile_iri: None,
        profile_revision: None,
        evaluator: "test".to_string(),
        validated_at_ms: None,
        findings: Vec::new(),
        completeness: MetadataProfileValidationCompleteness::Complete,
        stale_reason: None,
        dataset_digest: None,
    }
}

#[test]
fn raw_includes_candidate() {
    let document_id = Ulid::generate();
    let mut replica = ReplicaRecord::new(
        document_id,
        Ulid::generate(),
        "notes".to_string(),
        ReplicaOrigin::Realm,
    );
    replica.displayed_jsonld = "displayed".to_string();
    replica.dataset_digest = Some([3u8; 32]);
    replica.findings = 2;
    let revision = device_raw_revision(
        &replica.displayed_jsonld,
        replica.dataset_digest,
        replica.findings,
        Ulid::from_bytes([4u8; 16]),
        Some("candidate".to_string()),
    );

    assert_eq!(revision.jsonld, "displayed");
    assert_eq!(revision.dataset_digest, Some([3u8; 32]));
    assert!(matches!(
        revision.merged,
        Some(MetadataMergedRevision { jsonld, findings: 2 }) if jsonld == "candidate"
    ));
}

#[tokio::test]
async fn finds_later_membership() {
    // The only matching group sits just beyond the legacy default page.
    let dir = tempfile::tempdir().unwrap();
    let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let member = UserId::local(Ulid::from_bytes([8u8; 16]), realm_id);
    let other = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
    let actor = Actor {
        node_id: node(1),
        user_id: member,
        realm_id,
    };
    let mut writes = Vec::with_capacity((DEVICE_GROUP_SCAN_PAGE + 1) * 2);
    for seed in 1..=DEVICE_GROUP_SCAN_PAGE + 1 {
        let group_id = Ulid::from(seed as u128);
        let group = Group {
            display_name: seed.to_string(),
            group_id,
            realm_id,
            roles: Default::default(),
            owner: other,
        };
        let authorization = GroupAuthorizationDocument::default_group_doc(
            if seed > DEVICE_GROUP_SCAN_PAGE {
                member
            } else {
                other
            },
            realm_id,
            group_id,
        );
        for (target, bytes) in [
            (
                DocumentSyncTarget::Group { group_id },
                group.to_bytes(&actor).unwrap(),
            ),
            (
                DocumentSyncTarget::GroupAuthorization { group_id },
                authorization.to_bytes(&actor).unwrap(),
            ),
        ] {
            writes.push((
                target.storage_keyspace().to_string(),
                target.storage_key(),
                aruna_core::types::Value::from(bytes),
            ));
        }
    }
    assert!(matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));

    let documents = device_group_documents(&context, member).await;
    assert_eq!(documents.len(), 1);
    assert_eq!(
        documents[0].group.group_id,
        Ulid::from((DEVICE_GROUP_SCAN_PAGE + 1) as u128)
    );
}

#[test]
fn prefers_exact_status() {
    let expected = Ulid::from_bytes([1; 16]);
    let stale = Ulid::from_bytes([2; 16]);
    let mut selected = None;

    keep_status(&mut selected, validation_status(stale), expected);
    keep_status(&mut selected, validation_status(expected), expected);
    keep_status(&mut selected, validation_status(stale), expected);

    assert_eq!(selected.unwrap().dataset_revision, expected);
}

#[test]
fn auth_policies_differ() {
    let success = || Some("export");
    assert!(matches!(
        reduce_holder_reads(
            success(),
            Some(MetadataReadError::Forbidden),
            false,
            false,
            false,
            AuthFailure::Fatal,
        ),
        ReadDecision::Auth(MetadataReadError::Forbidden)
    ));
    assert!(matches!(
        reduce_holder_reads(
            success(),
            Some(MetadataReadError::Forbidden),
            false,
            false,
            false,
            AuthFailure::Unavailable,
        ),
        ReadDecision::Success("export")
    ));
    assert!(matches!(
        reduce_holder_reads(None::<&str>, None, true, false, false, AuthFailure::Fatal),
        ReadDecision::NotFound
    ));
    assert!(matches!(
        reduce_holder_reads(success(), None, true, true, false, AuthFailure::Fatal),
        ReadDecision::Unavailable
    ));
}

fn config_and_placement() -> (RealmConfigDocument, PlacementRef) {
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
    (
        config,
        PlacementRef {
            strategy_id: strategy.strategy_id,
            shard: 9,
        },
    )
}

fn relay_fixture() -> (RealmConfigDocument, PlacementRef, NodeId) {
    let (mut config, placement) = config_and_placement();
    let device = node(9);
    let owner = UserId::nil(config.realm_id);
    config.ensure_node(device, RealmNodeKind::User { owner });
    let holder = resolve_shard_holders(&config, &placement)
        .into_iter()
        .next()
        .expect("fixture has holders");
    (config, placement, holder)
}

fn signed_event(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
    origin_seed: u8,
) -> (AdminDocumentEvent, iroh::Signature) {
    let secret = iroh::SecretKey::from_bytes(&[origin_seed; 32]);
    let user_id = UserId::nil(config.realm_id);
    let event = AdminDocumentEvent {
        event_id: Ulid::from_bytes([5u8; 16]),
        target: aruna_core::admin_documents::AdminDocumentTarget::Group {
            group_id: Ulid::from_bytes([6u8; 16]),
        },
        origin_node_id: secret.public(),
        origin_seq: 1,
        observed: Default::default(),
        actor: Actor {
            node_id: secret.public(),
            user_id,
            realm_id: config.realm_id,
        },
        op: aruna_core::admin_documents::AdminDocumentOperation::GroupCreated {
            realm_id: config.realm_id,
            display_name: "Engineering".to_string(),
            owner: user_id,
        },
    };
    let signature = secret.sign(&event.signing_bytes(placement).expect("event signs"));
    (event, signature)
}

#[test]
fn holder_accepts_relay() {
    let (config, placement, holder) = relay_fixture();
    let (event, signature) = signed_event(&config, &placement, 1);

    assert_eq!(
        admit_relayed_admin(&config, holder, node(2), &event, &placement, &signature),
        RelayAdmission::Accept
    );
}

#[test]
fn relay_rejects_forged() {
    // A relay that rewrites the actor invalidates the origin signature.
    let (config, placement, holder) = relay_fixture();
    let (event, signature) = signed_event(&config, &placement, 1);
    let mut forged = event;
    forged.actor.user_id = UserId::local(Ulid::from_bytes([8u8; 16]), config.realm_id);

    assert!(matches!(
        admit_relayed_admin(&config, holder, node(2), &forged, &placement, &signature),
        RelayAdmission::Reject(reason)
            if reason == "relayed admin event is not signed by its origin"
    ));
}

#[test]
fn rejects_device_origin() {
    // A relayed admin event originated by a device may never be published.
    let (config, placement, holder) = relay_fixture();
    let (event, signature) = signed_event(&config, &placement, 9);

    assert!(matches!(
        admit_relayed_admin(&config, holder, node(2), &event, &placement, &signature),
        RelayAdmission::Reject(reason)
            if reason == "relayed admin event origin may not publish"
    ));
}

#[test]
fn rejects_device_peer() {
    // A device is not a relay: it may not hand an admin event to a holder.
    let (config, placement, holder) = relay_fixture();
    let (event, signature) = signed_event(&config, &placement, 1);

    assert_eq!(
        admit_relayed_admin(&config, holder, node(9), &event, &placement, &signature),
        RelayAdmission::Forbidden
    );
}

#[test]
fn nonholder_defers_relay() {
    // Another holder can still take it, so this is unavailable, not a reject.
    let (config, placement, holder) = relay_fixture();
    let (event, signature) = signed_event(&config, &placement, 1);
    let non_holder = (1..=4u8)
        .map(node)
        .find(|candidate| {
            *candidate != holder && !resolve_shard_holders(&config, &placement).contains(candidate)
        })
        .expect("fixture has a non-holder");

    assert_eq!(
        admit_relayed_admin(&config, non_holder, node(2), &event, &placement, &signature),
        RelayAdmission::Unavailable
    );
}

#[test]
fn peer_binds_owner() {
    // A device may forward only for the owner its realm config names.
    let (config, _, _) = relay_fixture();
    let owner = UserId::nil(config.realm_id);
    let other = UserId::local(Ulid::from_bytes([8u8; 16]), config.realm_id);

    assert!(peer_acts_for(&config, node(9), owner));
    assert!(!peer_acts_for(&config, node(9), other));
    assert!(peer_acts_for(&config, node(1), other));
}

#[test]
fn holder_writes_local() {
    let (config, placement) = config_and_placement();
    let holders = resolve_shard_holders(&config, &placement);

    assert_eq!(
        write_route(Some(&config), &placement, holders[0]),
        MetadataWriteRoute::Local
    );
}

#[test]
fn nonholder_writes_forward() {
    // Rank order is the holder set's own: rank-0 is tried first, the rest on
    // failure. Replica 2 of 4 servers guarantees a non-holder exists.
    let (config, placement) = config_and_placement();
    let holders = resolve_shard_holders(&config, &placement);
    let outsider = (1..=4u8)
        .map(node)
        .find(|candidate| !holders.contains(candidate))
        .expect("a replica-capped bucket leaves a non-holder");

    assert_eq!(
        write_route(Some(&config), &placement, outsider),
        MetadataWriteRoute::Forward(holders)
    );
}

#[test]
fn user_writes_forward() {
    // A User-kind node is never sync-eligible and holds no bucket, so every write must
    // be forwarded.
    let (mut config, placement) = config_and_placement();
    let owner = UserId::nil(config.realm_id);
    config.ensure_node(node(9), RealmNodeKind::User { owner });

    assert!(matches!(
        write_route(Some(&config), &placement, node(9)),
        MetadataWriteRoute::Forward(_)
    ));
}

#[test]
fn missing_config_forwards() {
    let (_, placement) = config_and_placement();

    assert_eq!(
        write_route(None, &placement, node(1)),
        MetadataWriteRoute::Forward(Vec::new())
    );
}

#[test]
fn unplaced_writes_local() {
    // No strategy governs a NIL ref (early bootstrap): nowhere to forward to,
    // and no sharding to respect.
    let (config, _) = config_and_placement();

    assert_eq!(
        write_route(Some(&config), &PlacementRef::NIL, node(1)),
        MetadataWriteRoute::Local
    );
    assert_eq!(
        write_route(None, &PlacementRef::NIL, node(1)),
        MetadataWriteRoute::Local
    );
}

#[test]
fn ambiguous_delivery_stops() {
    assert_eq!(
        retry_disposition(MetadataRequestDelivery::PossiblySent),
        RetryDisposition::Stop
    );
    assert_eq!(
        retry_disposition(MetadataRequestDelivery::DefinitelyNotSent),
        RetryDisposition::TryNext
    );
}

#[test]
fn nonholder_read_rejected() {
    let realm_id = RealmId::from_bytes([8u8; 32]);
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for seed in 1..=4u8 {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    let document_id = MetaResourceId::from_parts(
        1,
        PlacementHandle::new(METADATA_HANDLE).unwrap(),
        BucketId::new(9).unwrap(),
        1,
    )
    .unwrap()
    .as_ulid();
    let placement = resolve_metadata_id(&config, realm_id, None, document_id).unwrap();
    let holders = resolve_shard_holders(&config, &placement);
    let outsider = (1..=4u8)
        .map(node)
        .find(|candidate| !holders.contains(candidate))
        .unwrap();

    assert!(!holds_metadata_id(&config, realm_id, outsider, document_id));
}

#[test]
fn response_records_checked() {
    let realm_id = RealmId::from_bytes([8u8; 32]);
    let group_id = Ulid::from_bytes([3u8; 16]);
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    let document_id = MetaResourceId::from_parts(
        1,
        PlacementHandle::new(METADATA_HANDLE).unwrap(),
        BucketId::new(9).unwrap(),
        1,
    )
    .unwrap()
    .as_ulid();
    let placement = resolve_metadata_id(&config, realm_id, Some(group_id), document_id).unwrap();
    let record = MetadataRegistryRecord {
        realm_id,
        group_id,
        document_id,
        document_path: "docs/one".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "docs/one",
            document_id,
        ),
        placement,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_bytes([4u8; 16]),
        last_event_id: Ulid::from_bytes([4u8; 16]),
    };

    assert!(routed_record_matches(
        &config,
        realm_id,
        document_id,
        &placement,
        &record,
    ));
    let mut substituted = record.clone();
    substituted.document_id = Ulid::from_bytes([5u8; 16]);
    assert!(!routed_record_matches(
        &config,
        realm_id,
        document_id,
        &placement,
        &substituted,
    ));

    let create = CreateMetadataDocumentConfig {
        actor: Actor {
            node_id: node(1),
            user_id: aruna_core::UserId::local(Ulid::from_bytes([6u8; 16]), realm_id),
            realm_id,
        },
        group_id,
        document_id: Ulid::nil(),
        document_path: "/docs/one/".to_string(),
        public: true,
        payload: crate::metadata::create_document::CreateMetadataDocumentPayload::Scaffold {
            name: "one".to_string(),
            description: String::new(),
            date_published: "2026-01-01".to_string(),
            license: None,
        },
    };
    assert!(create_record_matches(
        &create,
        document_id,
        &placement,
        &record
    ));
    let mut moved = record.clone();
    moved.placement.shard += 1;
    assert!(!create_record_matches(
        &create,
        document_id,
        &placement,
        &moved
    ));

    let mut changed = record.clone();
    changed.document_path = "docs/two".to_string();
    assert!(!update_record_matches(&record, &changed));
}

fn revoke_message() -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardTokenRevocation {
        auth_token: MetadataAuthToken::bearer("caller-token").unwrap(),
        token: "target-token".to_string(),
    }
}

#[tokio::test]
async fn capacity_then_success() {
    let peers = [node(1), node(2)];
    let order = rank_revoke_peers(
        peers.iter().copied(),
        bearer_token_hash("target-token").as_bytes(),
    );
    let mut calls = Vec::new();
    let result = run_revoke(
        &order,
        revoke_message(),
        Instant::now() + TOKEN_REVOKE_DEADLINE,
        |peer, _| {
            calls.push(peer);
            std::future::ready(Ok(if peer == order[0] {
                MetadataTransportMessage::ForwardedTokenRevocationCapacity
            } else {
                MetadataTransportMessage::ForwardedTokenRevoked
            }))
        },
    )
    .await;

    assert!(result.is_ok());
    assert_eq!(calls, order);
}

#[tokio::test]
async fn retries_possible_send() {
    let peers = [node(1), node(2)];
    let order = rank_revoke_peers(
        peers.iter().copied(),
        bearer_token_hash("target-token").as_bytes(),
    );
    let mut calls = Vec::new();
    let result = run_revoke(
        &order,
        revoke_message(),
        Instant::now() + TOKEN_REVOKE_DEADLINE,
        |peer, _| {
            calls.push(peer);
            if peer == order[0] {
                std::future::ready(Err(MetadataRequestError::possibly_sent(
                    MetadataError::HandleMissing,
                )))
            } else {
                std::future::ready(Ok(MetadataTransportMessage::ForwardedTokenRevoked))
            }
        },
    )
    .await;

    assert!(result.is_ok());
    assert_eq!(calls, order);
}

#[tokio::test]
async fn all_capacity_unavailable() {
    let peers = [node(1), node(2)];
    let order = rank_revoke_peers(
        peers.iter().copied(),
        bearer_token_hash("target-token").as_bytes(),
    );
    let mut calls = Vec::new();
    let result = run_revoke(
        &order,
        revoke_message(),
        Instant::now() + TOKEN_REVOKE_DEADLINE,
        |peer, _| {
            calls.push(peer);
            std::future::ready(Ok(
                MetadataTransportMessage::ForwardedTokenRevocationCapacity,
            ))
        },
    )
    .await;

    assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
    assert_eq!(calls, order);
}

#[tokio::test]
async fn reject_stops_retry() {
    let peers = [node(1), node(2)];
    let order = rank_revoke_peers(
        peers.iter().copied(),
        bearer_token_hash("target-token").as_bytes(),
    );
    let mut calls = Vec::new();
    let result = run_revoke(
        &order,
        revoke_message(),
        Instant::now() + TOKEN_REVOKE_DEADLINE,
        |peer, _| {
            calls.push(peer);
            std::future::ready(Ok(MetadataTransportMessage::Reject(
                "invalid token".to_string(),
            )))
        },
    )
    .await;

    assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
    assert_eq!(calls, vec![order[0]]);
}

#[tokio::test]
async fn no_retry_loop() {
    let peer = node(1);
    let peers = vec![peer, peer];
    let mut calls = Vec::new();
    let result = run_revoke(
        &peers,
        revoke_message(),
        Instant::now() + TOKEN_REVOKE_DEADLINE,
        |peer, _| {
            calls.push(peer);
            std::future::ready(Ok(
                MetadataTransportMessage::ForwardedTokenRevocationCapacity,
            ))
        },
    )
    .await;

    assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
    assert_eq!(calls, vec![peer]);
}

#[test]
fn bounded_peer_order() {
    let peers = (1..=16).map(node).collect::<Vec<_>>();
    let reversed = peers.iter().copied().rev().collect::<Vec<_>>();
    let subject = bearer_token_hash("target-token");
    let first = rank_revoke_peers(peers.iter().copied(), subject.as_bytes());
    let second = rank_revoke_peers(reversed.iter().copied(), subject.as_bytes());

    assert_eq!(first, second);
    assert_eq!(first.len(), TOKEN_REVOKE_PEER_LIMIT);
    assert!(first.iter().all(|peer| peers.contains(peer)));
}

#[test]
fn holders_deduplicate() {
    let first = node(1);
    let second = node(2);

    assert_eq!(
        distinct_holders(&[first, second, first, second]),
        vec![first, second]
    );
}

#[test]
fn frozen_holders_intersect() {
    let current = [node(1), node(2)];
    let frozen = [node(2), node(3)];

    assert_eq!(holder_intersection(&current, &frozen), vec![node(2)]);
    assert!(holder_intersection(&[node(1)], &[node(2)]).is_empty());
}

#[tokio::test]
async fn deadline_stops_calls() {
    let peers = vec![node(1), node(2)];
    let mut calls = Vec::new();
    let result = run_revoke(&peers, revoke_message(), Instant::now(), |peer, _| {
        calls.push(peer);
        std::future::ready(Ok(MetadataTransportMessage::ForwardedTokenRevoked))
    })
    .await;

    assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
    assert!(calls.is_empty());
}
