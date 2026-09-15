use std::cmp::Ordering;

use super::{
    DocumentApplyDecision, DocumentChange, DocumentChangeKind, DocumentEvent, DocumentOutboxEvent,
    DocumentSyncPublish, DocumentSyncRevision, DocumentTarget, compare_sync_revisions,
    shard_topic_id, sync_apply_decision,
};
use crate::NodeId;
use crate::TopicId;
use crate::UserId;
use crate::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE, METADATA_INDEX_KEYSPACE,
    REALM_CONFIG_KEYSPACE, USER_KEYSPACE,
};
use crate::structs::PlacementRef;
use crate::structs::RealmId;
use ulid::Ulid;

fn test_ulid(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

fn test_node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn revision(generation: u64, event_seed: u8, actor_seed: u8) -> DocumentSyncRevision {
    DocumentSyncRevision {
        generation,
        event_id: test_ulid(event_seed),
        actor: test_node(actor_seed),
        updated_at_ms: u64::from(event_seed),
    }
}

fn change(
    kind: DocumentChangeKind,
    base: Option<DocumentSyncRevision>,
    generation: u64,
    event_seed: u8,
    actor_seed: u8,
) -> DocumentChange {
    DocumentChange {
        base,
        current: revision(generation, event_seed, actor_seed),
        kind,
        placement: crate::structs::PlacementRef::NIL,
    }
}

fn test_realm(seed: u8) -> RealmId {
    RealmId::from_bytes([seed; 32])
}

fn graph_topic_ulid(graph_iri: &str) -> Ulid {
    let hash = blake3::hash(graph_iri.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    Ulid::from_bytes(bytes)
}

fn graph_lifecycle_key(graph_iri: &str) -> Vec<u8> {
    blake3::hash(graph_iri.as_bytes()).as_bytes().to_vec()
}

#[test]
fn document_sync_stable() {
    let group_id = test_ulid(1);
    let realm_id = test_realm(2);
    let user_id = UserId::new(test_ulid(3), realm_id);
    let document_id = test_ulid(4);
    let event_id = test_ulid(5);
    let graph_iri = "https://example.com/graphs/stable";

    let cases = [
        (DocumentTarget::Group { group_id }, TopicId::group(group_id)),
        (
            DocumentTarget::GroupAuthorization { group_id },
            TopicId::group(group_id),
        ),
        (
            DocumentTarget::RealmAuthorization { realm_id },
            TopicId::realm(realm_id),
        ),
        (
            DocumentTarget::RealmConfig { realm_id },
            TopicId::realm(realm_id),
        ),
        (DocumentTarget::User { user_id }, TopicId::users(realm_id)),
        (
            DocumentTarget::MetadataRegistry {
                group_id,
                document_id,
            },
            TopicId::metadata(document_id),
        ),
        (
            DocumentTarget::MetadataCreateEvent {
                document_id,
                event_id,
            },
            TopicId::metadata(document_id),
        ),
        (
            DocumentTarget::MetadataDocumentLifecycle { document_id },
            TopicId::metadata(document_id),
        ),
        (
            DocumentTarget::MetadataGraphLifecycle {
                graph_iri: graph_iri.to_string(),
            },
            TopicId::metadata(graph_topic_ulid(graph_iri)),
        ),
    ];

    for (target, expected_topic) in cases {
        assert_eq!(target.topic_id(), expected_topic, "{target:?}");
    }
}

#[test]
fn document_target_stable() {
    let group_id = test_ulid(1);
    let realm_id = test_realm(2);
    let user_id = UserId::new(test_ulid(3), realm_id);
    let document_id = test_ulid(4);
    let event_id = test_ulid(5);
    let graph_iri = "https://example.com/graphs/stable";

    let mut user_key = Vec::with_capacity(48);
    user_key.extend_from_slice(realm_id.as_bytes());
    user_key.extend_from_slice(&user_id.user_ulid.to_bytes());

    let mut registry_key = Vec::with_capacity(32);
    registry_key.extend_from_slice(&group_id.to_bytes());
    registry_key.extend_from_slice(&document_id.to_bytes());

    let mut event_key = Vec::with_capacity(32);
    event_key.extend_from_slice(&document_id.to_bytes());
    event_key.extend_from_slice(&event_id.to_bytes());

    let cases = [
        (
            DocumentTarget::Group { group_id },
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
        ),
        (
            DocumentTarget::GroupAuthorization { group_id },
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
        ),
        (
            DocumentTarget::RealmAuthorization { realm_id },
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
        ),
        (
            DocumentTarget::RealmConfig { realm_id },
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
        ),
        (DocumentTarget::User { user_id }, USER_KEYSPACE, user_key),
        (
            DocumentTarget::MetadataRegistry {
                group_id,
                document_id,
            },
            METADATA_INDEX_KEYSPACE,
            registry_key,
        ),
        (
            DocumentTarget::MetadataCreateEvent {
                document_id,
                event_id,
            },
            METADATA_EVENT_LOG_KEYSPACE,
            event_key,
        ),
        (
            DocumentTarget::MetadataDocumentLifecycle { document_id },
            METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
            document_id.to_bytes().to_vec(),
        ),
        (
            DocumentTarget::MetadataGraphLifecycle {
                graph_iri: graph_iri.to_string(),
            },
            METADATA_GRAPH_LIFECYCLE_KEYSPACE,
            graph_lifecycle_key(graph_iri),
        ),
    ];

    for (target, expected_keyspace, expected_key) in cases {
        assert_eq!(target.storage_keyspace(), expected_keyspace, "{target:?}");
        assert_eq!(
            target.storage_key().as_ref(),
            expected_key.as_slice(),
            "{target:?}"
        );
    }
}

#[test]
fn shard_classed_topics() {
    let group_id = test_ulid(1);
    let realm_id = test_realm(2);
    let document_id = test_ulid(4);
    let event_id = test_ulid(5);
    let placement_a = PlacementRef {
        strategy_id: test_ulid(9),
        shard: 3,
    };
    let placement_b = PlacementRef {
        shard: 4,
        ..placement_a
    };

    // Group and its authorization are one logical subject: with the same
    // placement they ride a single shard topic, derived purely from it.
    let group = DocumentTarget::Group { group_id };
    let group_auth = DocumentTarget::GroupAuthorization { group_id };
    assert_eq!(
        group.sync_topic_id(realm_id, &placement_a),
        group_auth.sync_topic_id(realm_id, &placement_a)
    );
    assert_eq!(
        group.sync_topic_id(realm_id, &placement_a),
        shard_topic_id(realm_id, &placement_a)
    );
    assert_ne!(
        group.sync_topic_id(realm_id, &placement_a),
        group.sync_topic_id(realm_id, &placement_b)
    );

    // The three metadata variants of one document collapse onto its shard.
    let registry = DocumentTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    let create = DocumentTarget::MetadataCreateEvent {
        document_id,
        event_id,
    };
    let lifecycle = DocumentTarget::MetadataDocumentLifecycle { document_id };
    assert_eq!(
        registry.sync_topic_id(realm_id, &placement_a),
        create.sync_topic_id(realm_id, &placement_a)
    );
    assert_eq!(
        registry.sync_topic_id(realm_id, &placement_a),
        lifecycle.sync_topic_id(realm_id, &placement_a)
    );

    // Shared realm-scoped targets keep their per-domain topics and ignore
    // the placement entirely.
    let realm_auth = DocumentTarget::RealmAuthorization { realm_id };
    let realm_config = DocumentTarget::RealmConfig { realm_id };
    assert_eq!(realm_auth.topic_id(), realm_config.topic_id());
    assert_ne!(
        realm_auth.sync_topic_id(realm_id, &placement_a),
        realm_config.sync_topic_id(realm_id, &placement_a)
    );
    assert_eq!(
        realm_config.sync_topic_id(realm_id, &placement_a),
        realm_config.sync_topic_id(realm_id, &placement_b)
    );
    assert_ne!(
        realm_config.sync_topic_id(realm_id, &placement_a),
        shard_topic_id(realm_id, &placement_a)
    );
}

#[test]
fn shard_topic_identity() {
    let realm_id = test_realm(2);
    let placement = PlacementRef {
        strategy_id: test_ulid(4),
        shard: 5,
    };
    // Fixed inputs → fixed topic id: the stage-2 cross-node canary. A change
    // here means co-holders would derive different shard topics.
    assert_eq!(
        shard_topic_id(realm_id, &placement).to_string(),
        "b375275475edc34ab568776cea1fdf4053e57458816e8ed35c5925e3caa07cf4"
    );

    // A second pinned vector guards the pad slot: shard 0 of the nil
    // strategy is the bootstrap topic every node derives before any config.
    assert_eq!(
        shard_topic_id(test_realm(1), &PlacementRef::NIL).to_string(),
        "50b84e3436901b73ce31928007b79483807c50a12827539f2265e8a5d9bcc575"
    );

    // Realm, strategy, and shard are the only inputs that move the topic.
    let other_shard = PlacementRef {
        shard: 6,
        ..placement
    };
    let other_strategy = PlacementRef {
        strategy_id: test_ulid(5),
        ..placement
    };
    assert_ne!(
        shard_topic_id(realm_id, &placement),
        shard_topic_id(realm_id, &other_shard)
    );
    assert_ne!(
        shard_topic_id(realm_id, &placement),
        shard_topic_id(realm_id, &other_strategy)
    );
    assert_ne!(
        shard_topic_id(realm_id, &placement),
        shard_topic_id(test_realm(3), &placement)
    );
}

#[test]
fn node_usage_keys() {
    use crate::keyspaces::USAGE_NODE_STATS_KEYSPACE;
    use crate::structs::{usage_global_key, usage_snapshot_key};

    let realm_id = test_realm(2);
    let node_id = test_node(1);
    let group_id = test_ulid(4);

    let global = DocumentTarget::NodeUsage {
        realm_id,
        node_id,
        group_id: None,
    };
    let group = DocumentTarget::NodeUsage {
        realm_id,
        node_id,
        group_id: Some(group_id),
    };

    // Both map onto the realm domain topic and the single shared sync topic.
    let nil = PlacementRef::NIL;
    assert_eq!(global.topic_id(), TopicId::realm(realm_id));
    assert_eq!(global.topic_id(), group.topic_id());
    assert_eq!(
        global.sync_topic_id(realm_id, &nil),
        group.sync_topic_id(realm_id, &nil)
    );

    // A different node's usage rides the very same shared topic.
    let other = DocumentTarget::NodeUsage {
        realm_id,
        node_id: test_node(9),
        group_id: None,
    };
    assert_eq!(
        global.sync_topic_id(realm_id, &nil),
        other.sync_topic_id(realm_id, &nil)
    );
    // But is distinct from the realm-config topic on the same domain.
    assert_ne!(
        global.sync_topic_id(realm_id, &nil),
        DocumentTarget::RealmConfig { realm_id }.sync_topic_id(realm_id, &nil)
    );

    assert_eq!(global.storage_keyspace(), USAGE_NODE_STATS_KEYSPACE);
    assert_eq!(
        global.storage_key().as_ref(),
        usage_global_key(node_id).as_slice()
    );
    assert_eq!(
        group.storage_key().as_ref(),
        usage_snapshot_key(group_id, node_id).as_slice()
    );
}

#[test]
fn node_info_keys() {
    use crate::keyspaces::NODE_INFO_KEYSPACE;
    use crate::structs::node_info_key;

    let realm_id = test_realm(2);
    let node_id = test_node(1);
    let target = DocumentTarget::NodeInfo { realm_id, node_id };

    // Rides the realm domain topic and one shared sync topic across nodes.
    let nil = PlacementRef::NIL;
    assert_eq!(target.topic_id(), TopicId::realm(realm_id));
    let other = DocumentTarget::NodeInfo {
        realm_id,
        node_id: test_node(9),
    };
    assert_eq!(
        target.sync_topic_id(realm_id, &nil),
        other.sync_topic_id(realm_id, &nil)
    );
    // Distinct from the node-usage and watch-interest topics on the same realm.
    assert_ne!(
        target.sync_topic_id(realm_id, &nil),
        DocumentTarget::NodeUsage {
            realm_id,
            node_id,
            group_id: None,
        }
        .sync_topic_id(realm_id, &nil)
    );
    assert_ne!(
        target.sync_topic_id(realm_id, &nil),
        DocumentTarget::WatchInterest { realm_id, node_id }.sync_topic_id(realm_id, &nil)
    );

    assert_eq!(target.storage_keyspace(), NODE_INFO_KEYSPACE);
    assert_eq!(
        target.storage_key().as_ref(),
        node_info_key(node_id).as_slice()
    );
}

#[test]
fn watch_interest_keys() {
    use crate::keyspaces::{
        NOTIFICATION_WATCH_INTEREST_KEYSPACE, NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE,
    };
    use crate::structs::{interest_node_key, watch_subscription_key};

    let realm_id = test_realm(2);
    let node_id = test_node(1);
    let other = DocumentTarget::WatchInterest {
        realm_id,
        node_id: test_node(9),
    };
    let target = DocumentTarget::WatchInterest { realm_id, node_id };
    let owner = UserId::new(test_ulid(10), realm_id);
    let subscription = DocumentTarget::WatchSubscription {
        owner,
        watch_id: test_ulid(11),
    };

    // Rides the realm domain topic and one shared sync topic across nodes.
    let nil = PlacementRef::NIL;
    assert_eq!(target.topic_id(), TopicId::realm(realm_id));
    assert_eq!(
        target.sync_topic_id(realm_id, &nil),
        other.sync_topic_id(realm_id, &nil)
    );
    assert_eq!(
        target.sync_topic_id(realm_id, &nil),
        subscription.sync_topic_id(realm_id, &nil)
    );
    // Distinct from the node-usage topic that shares the same realm domain.
    assert_ne!(
        target.sync_topic_id(realm_id, &nil),
        DocumentTarget::NodeUsage {
            realm_id,
            node_id,
            group_id: None,
        }
        .sync_topic_id(realm_id, &nil)
    );

    assert_eq!(
        target.storage_keyspace(),
        NOTIFICATION_WATCH_INTEREST_KEYSPACE
    );
    assert_eq!(
        target.storage_key().as_ref(),
        interest_node_key(realm_id, node_id).as_slice()
    );
    assert_eq!(
        subscription.storage_keyspace(),
        NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE
    );
    assert_eq!(
        subscription.storage_key().as_ref(),
        watch_subscription_key(owner, test_ulid(11)).as_ref()
    );
}

#[test]
fn upsert_uses_helpers() {
    let event_id = test_ulid(10);
    let target = DocumentTarget::RealmConfig {
        realm_id: test_realm(11),
    };
    let change = change(DocumentChangeKind::Upsert, None, 1, 12, 1);
    let outbox = DocumentOutboxEvent::Upsert {
        bytes: vec![1, 2],
        change,
    };
    let publish = DocumentSyncPublish::Upsert {
        event_id,
        target: target.clone(),
        bytes: vec![1, 2],
        change,
        allow_genesis: true,
    };
    let event = DocumentEvent::Upsert {
        event_id,
        target: target.clone(),
        bytes: vec![1, 2],
        change,
    };

    assert_eq!(outbox.kind(), b"upsert");
    assert_eq!(publish.target(), &target);
    assert_eq!(publish.event_id(), event_id);
    assert!(publish.allow_genesis());
    assert_eq!(event.target(), &target);
    assert_eq!(event.event_id(), event_id);
}

#[test]
fn delete_uses_helpers() {
    let event_id = test_ulid(13);
    let target = DocumentTarget::RealmConfig {
        realm_id: test_realm(14),
    };
    let change = change(DocumentChangeKind::Delete, None, 2, 15, 1);
    let outbox = DocumentOutboxEvent::Delete { change };
    let publish = DocumentSyncPublish::Delete {
        event_id,
        target: target.clone(),
        change,
        allow_genesis: false,
    };
    let event = DocumentEvent::Delete {
        event_id,
        target: target.clone(),
        change,
    };

    assert_eq!(outbox.kind(), b"delete");
    assert_eq!(publish.target(), &target);
    assert_eq!(publish.event_id(), event_id);
    assert!(!publish.allow_genesis());
    assert_eq!(event.target(), &target);
    assert_eq!(event.event_id(), event_id);
}

#[test]
fn document_revision_stable() {
    let older = revision(1, 9, 1);
    let newer = revision(2, 1, 1);
    let same_generation_a = revision(2, 1, 1);
    let same_generation_b = revision(2, 2, 1);

    assert_eq!(compare_sync_revisions(&older, &newer), Ordering::Less);
    assert_eq!(compare_sync_revisions(&newer, &older), Ordering::Greater);
    assert_eq!(
        compare_sync_revisions(&newer, &same_generation_a),
        Ordering::Equal
    );
    assert_eq!(
        compare_sync_revisions(&same_generation_a, &same_generation_b),
        Ordering::Less
    );
}

#[test]
fn document_sync_changes() {
    let local = change(DocumentChangeKind::Upsert, None, 1, 1, 1);
    let incoming = change(DocumentChangeKind::Upsert, Some(local.current), 2, 2, 1);

    assert_eq!(
        sync_apply_decision(None, &local),
        DocumentApplyDecision::Apply
    );
    assert_eq!(
        sync_apply_decision(Some(&local), &incoming),
        DocumentApplyDecision::Apply
    );
    assert_eq!(
        sync_apply_decision(Some(&local), &local),
        DocumentApplyDecision::Apply
    );
}

#[test]
fn document_apply_changes() {
    let stale = change(DocumentChangeKind::Upsert, None, 1, 1, 1);
    let newer = change(DocumentChangeKind::Upsert, Some(stale.current), 2, 2, 1);
    let tombstone = change(DocumentChangeKind::Delete, Some(stale.current), 2, 3, 1);

    assert_eq!(
        sync_apply_decision(Some(&newer), &stale),
        DocumentApplyDecision::SkipStale
    );
    assert_eq!(
        sync_apply_decision(Some(&tombstone), &stale),
        DocumentApplyDecision::SkipTombstoned
    );
}

#[test]
fn document_decision_changes() {
    let local = change(DocumentChangeKind::Upsert, None, 1, 1, 1);
    let same_generation = change(DocumentChangeKind::Upsert, None, 1, 2, 2);
    let unobserved_newer = change(DocumentChangeKind::Delete, None, 2, 3, 2);

    assert_eq!(
        sync_apply_decision(Some(&local), &same_generation),
        DocumentApplyDecision::Conflict
    );
    assert_eq!(
        sync_apply_decision(Some(&local), &unobserved_newer),
        DocumentApplyDecision::Conflict
    );
}

#[test]
fn metadata_document_scoped() {
    let document_id = test_ulid(4);
    let lifecycle = DocumentTarget::MetadataDocumentLifecycle { document_id };
    let create = DocumentTarget::MetadataCreateEvent {
        document_id,
        event_id: test_ulid(5),
    };

    assert_eq!(lifecycle.topic_id(), create.topic_id());
    assert_eq!(lifecycle.storage_key().as_ref(), document_id.to_bytes());
}

#[test]
fn metadata_document_delete() {
    let realm_id = test_realm(2);
    let document_id = test_ulid(4);
    let placement = PlacementRef {
        strategy_id: test_ulid(9),
        shard: 2,
    };
    let upsert_target = DocumentTarget::MetadataDocumentLifecycle { document_id };
    let delete_target = DocumentTarget::MetadataDocumentLifecycle { document_id };

    assert_eq!(upsert_target.topic_id(), delete_target.topic_id());
    assert_eq!(
        upsert_target.sync_topic_id(realm_id, &placement),
        delete_target.sync_topic_id(realm_id, &placement)
    );
}
