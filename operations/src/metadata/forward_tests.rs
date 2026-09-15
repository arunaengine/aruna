use crate::device::replica::ReplicaRecord;
use crate::forward::authorize::peer_acts_for;
use crate::forward::replay::create_record_matches;
use crate::forward::replay::routed_record_matches;
use crate::forward::replay::update_record_matches;
use crate::forward::routing::MetadataWriteRoute;
use crate::forward::routing::distinct_holders;
use crate::forward::routing::holder_intersection;
use crate::forward::routing::holds_metadata_id;
use crate::forward::routing::write_route;
use crate::forward::transport::RetryDisposition;
use crate::forward::transport::retry_disposition;
use crate::metadata::create_document::CreateDocumentConfig;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::handle::MetadataRequestDelivery;
use crate::metadata::protocol::MetadataReadError;
use crate::placement::resolve_shard_holders;
use aruna_core::MetaResourceId;
use aruna_core::NodeId;
use aruna_core::StructuredId;
use aruna_core::UserId;
use aruna_core::metadata::MetadataMergedRevision;
use aruna_core::metadata::ProfileValidationStatus;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::placement::placement_record::PlacementRef;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::realm::RealmNodeKind;
use ulid::Ulid;

use super::read::device_raw_revision;
use super::read::keep_status;

use super::*;

use crate::device::replica::ReplicaOrigin;
use aruna_core::metadata::{ProfileValidationCompleteness, ProfileValidationState};
use aruna_core::structs::placement::placement_record::{METADATA_HANDLE, PlacementStrategy};
use aruna_core::structured_id::{BucketId, PlacementHandle};

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn validation_status(revision: Ulid) -> ProfileValidationStatus {
    ProfileValidationStatus {
        document_id: Ulid::nil(),
        dataset_revision: revision,
        state: ProfileValidationState::NotProfiled,
        profile_id: None,
        profile_iri: None,
        profile_revision: None,
        evaluator: "test".to_string(),
        validated_at_ms: None,
        findings: Vec::new(),
        completeness: ProfileValidationCompleteness::Complete,
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

#[test]
fn peer_binds_owner() {
    // A device may forward only for the owner its realm config names.
    let (mut config, _) = config_and_placement();
    let owner = UserId::nil(config.realm_id);
    let other = UserId::local(Ulid::from_bytes([8u8; 16]), config.realm_id);
    config.ensure_node(node(9), RealmNodeKind::User { owner });

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

    let create = CreateDocumentConfig {
        actor: Actor {
            node_id: node(1),
            user_id: aruna_core::UserId::local(Ulid::from_bytes([6u8; 16]), realm_id),
            realm_id,
        },
        group_id,
        document_id: Ulid::nil(),
        document_path: "/docs/one/".to_string(),
        public: true,
        payload: crate::metadata::create_document::CreateDocumentPayload::Scaffold {
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
