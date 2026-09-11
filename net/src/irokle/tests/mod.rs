use super::*;
use crate::test_support::test_endpoint;
use aruna_core::admin_document_reducer::REALM_CONFIG_DEFAULT_STRATEGY_PATH;
use aruna_core::admin_documents::{
    AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentRoleDefinition,
    AdminDocumentTarget,
};
use aruna_core::alpn::Alpn;
use aruna_core::auth::{MAX_BEARER_TOKEN_LIFETIME_SECS, REVOCATION_GRACE_SECS};
use aruna_core::document::{DocumentSyncChangeKind, DocumentSyncRevision};
use aruna_core::keyspaces::{
    ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE,
    DOCUMENT_SYNC_REVISION_KEYSPACE, GROUP_KEYSPACE, METADATA_CREATE_ACCEPTANCE_KEYSPACE,
    METADATA_DOCUMENT_INDEX_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_GRAPH_PRUNE_JOB_KEYSPACE, METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE,
    USER_KEYSPACE, USER_SUBJECT_CLAIMS_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE,
};
use aruna_core::metadata::MetadataCreateEventPayload;
use aruna_core::storage_entries::{
    admin_document_reducer_conflict_key, admin_document_reducer_state_key,
    metadata_create_acceptance_key, metadata_document_key, metadata_event_log_key,
    metadata_registry_key, subject_index_key, subject_index_value,
};
use aruna_core::structs::{
    Actor, BandPool, BindingScope, DocumentClass, FIRST_GRANTABLE_HANDLE, Group,
    GroupAuthorizationDocument, GroupQuotaOverride, HANDLE_BANDS, HandleRange, JobId,
    METADATA_HANDLE, MetadataReplicationConfig, NodePlacementEntry, OidcProviderConfig, Permission,
    PlacementBinding, PlacementOverride, PlacementRef, PlacementStrategy, QuotaConfig,
    RealmAuthorizationDocument, RealmConfigDocument, RealmDiscoveryConfig, RealmId, RealmNodeKind,
    Role, SYNC_QUARANTINE_MAX_RECORDS, StaticRealmEndpoint, StrategyBinding, SyncQuarantineFamily,
    SyncQuarantineRecord, UserGroupCapOverride, band_start,
};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::{MetaResourceId, StructuredId, UserId};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::{env, process::Command};
use tempfile::TempDir;

mod admin_validation;
mod fanout;
mod fixtures;
mod group_targets;
mod lifecycle;
mod publish_eviction;
mod quarantine;
mod realm_config;
mod reconcile;
mod restart;
mod role_assignments;
mod shard;
mod validation;

use fixtures::*;

const DOCUMENT_SYNC_RESTART_CHILD_PATH_ENV: &str = "ARUNA_NET_DOCUMENT_SYNC_RESTART_CHILD_PATH";
const DOCUMENT_SYNC_RESTART_CHILD_TEST: &str =
    "document_sync::tests::restart::buffered_document_sync_publish_restart_child_process";

// Two services fork one admin topic (each mints its own genesis carrying a
// unique admin event). The genesis tie-break resets exactly the losing side,
// whose admin event is evicted and decodes back into a re-emittable outbox
// publish that preserves the original embedded event id (the applier dedup
// key) and refuses to mint a rival genesis.
#[tokio::test]
async fn forked_admin_topic_eviction_reemits_with_preserved_event_id() {
    let (_dir_a, storage_a) = test_storage();
    let (_dir_b, storage_b) = test_storage();
    let doc_a = tempfile::tempdir().expect("doc a");
    let doc_b = tempfile::tempdir().expect("doc b");
    let realm_id = RealmId::from_bytes([71; 32]);
    let service_a = DocumentSyncService::open_with_persist_policy(
        test_endpoint(71).await,
        storage_a,
        doc_a.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("service a opens");
    let service_b = DocumentSyncService::open_with_persist_policy(
        test_endpoint(72).await,
        storage_b,
        doc_b.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("service b opens");

    let node_a = service_a.local_node_id().expect("node a id");
    let node_b = service_b.local_node_id().expect("node b id");

    let user_id = UserId::local(Ulid::from_parts(7, 1), realm_id);
    let target = DocumentSyncTarget::User { user_id };
    let admin_target = AdminDocumentTarget::User { user_id };
    let placement = PlacementRef {
        strategy_id: Ulid::from_parts(71, 7),
        shard: 1,
    };
    let topic_id = target.sync_topic_id(realm_id, &placement);

    let event_a_id = Ulid::from_parts(0xA1, 1);
    let event_b_id = Ulid::from_parts(0xB2, 2);
    // Each side originates its own event: only the origin signs an envelope.
    let admin_a = test_admin_event(
        event_a_id,
        admin_target.clone(),
        &Actor {
            node_id: node_a,
            user_id,
            realm_id,
        },
        1,
        AdminDocumentOperation::UserNameSet {
            name: "from-a".into(),
        },
    );
    let admin_b = test_admin_event(
        event_b_id,
        admin_target.clone(),
        &Actor {
            node_id: node_b,
            user_id,
            realm_id,
        },
        1,
        AdminDocumentOperation::UserNameSet {
            name: "from-b".into(),
        },
    );

    // Shard-classed admin topics are created eagerly; each side lists the
    // other in its genesis peer set so the loser is a member after reset.
    service_a
        .ensure_document_sync_topics(&[topic_id], vec![node_b])
        .expect("service a topic genesis");
    service_b
        .ensure_document_sync_topics(&[topic_id], vec![node_a])
        .expect("service b topic genesis");

    let published_a = service_a
        .publish_documents(
            vec![DocumentSyncPublish::AdminOperation {
                target: target.clone(),
                event: Box::new(admin_a),
                placement,
                allow_genesis: true,
                origin_signature: None,
            }],
            vec![node_b],
        )
        .await;
    assert!(
        matches!(published_a, DocumentSyncNetEvent::DocumentsPublished { .. }),
        "service a publish: {published_a:?}"
    );
    let published_b = service_b
        .publish_documents(
            vec![DocumentSyncPublish::AdminOperation {
                target: target.clone(),
                event: Box::new(admin_b),
                placement,
                allow_genesis: true,
                origin_signature: None,
            }],
            vec![node_a],
        )
        .await;
    assert!(
        matches!(published_b, DocumentSyncNetEvent::DocumentsPublished { .. }),
        "service b publish: {published_b:?}"
    );

    let node_a_handle = service_a.node();
    let node_b_handle = service_b.node();
    let genesis_a = node_a_handle
        .storage()
        .topic_state(&topic_id)
        .unwrap()
        .unwrap()
        .genesis;
    let genesis_b = node_b_handle
        .storage()
        .topic_state(&topic_id)
        .unwrap()
        .unwrap()
        .genesis;
    assert_ne!(
        genesis_a, genesis_b,
        "the two nodes forked distinct genesis"
    );

    // The smaller genesis wins; the side holding the larger genesis loses.
    let (loser, loser_node, winner_node, loser_event_id, winner_genesis, winner_peer, loser_peer) =
        if genesis_a > genesis_b {
            (
                &service_a,
                &node_a_handle,
                &node_b_handle,
                event_a_id,
                genesis_b,
                node_id_to_peer_id(&node_b),
                node_id_to_peer_id(&node_a),
            )
        } else {
            (
                &service_b,
                &node_b_handle,
                &node_a_handle,
                event_b_id,
                genesis_a,
                node_id_to_peer_id(&node_a),
                node_id_to_peer_id(&node_b),
            )
        };

    let winner_ops = irokle_crate::oplog::topological(winner_node.storage(), &topic_id).unwrap();
    let loser_ops = irokle_crate::oplog::topological(loser_node.storage(), &topic_id).unwrap();

    // The winner keeps its genesis and produces no eviction.
    let (_winner_ack, winner_side) = winner_node
        .receive_sync_data_from_evicting(
            loser_peer,
            SyncData {
                topic_id,
                ops: loser_ops,
            },
        )
        .unwrap();
    assert!(
        winner_side.is_empty(),
        "winner keeps its genesis; no eviction"
    );

    // The loser resets and evicts its own admin chain.
    let (_loser_ack, evictions) = loser_node
        .receive_sync_data_from_evicting(
            winner_peer,
            SyncData {
                topic_id,
                ops: winner_ops,
            },
        )
        .unwrap();
    assert_eq!(evictions.len(), 1, "exactly the loser side resets");

    // Both sides now agree on the winning genesis.
    assert_eq!(
        loser_node
            .storage()
            .topic_state(&topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        winner_genesis
    );
    assert_eq!(
        winner_node
            .storage()
            .topic_state(&topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        winner_genesis
    );

    // The loser published, so its applied-ops cursor covers the chain the
    // tie-break just replaced.
    assert!(
        loser
            .storage_read(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                topic_cursor_key(topic_id),
            )
            .await
            .expect("cursor read")
            .is_some(),
        "the publish left an applied-ops cursor to invalidate"
    );

    // The evicted admin event re-emits with its original event id and
    // placement preserved, and allow_genesis cleared.
    let reemitted = loser
        .consume_eviction(evictions.into_iter().next().unwrap())
        .await;

    // The winning chain renumbers every actor sequence from one, so a
    // surviving cursor would skip its first ops forever.
    assert!(
        loser
            .storage_read(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                topic_cursor_key(topic_id),
            )
            .await
            .expect("cursor read")
            .is_none(),
        "the replaced chain's applied-ops cursor must not survive its eviction"
    );
    let reemitted = reemitted.expect("the eviction journals a pending entry");
    assert_eq!(reemitted.documents.len(), 1);
    let document = &reemitted.documents[0];
    assert_eq!(&document.target, &target);
    assert!(
        !document.allow_genesis,
        "re-emission must not mint a rival genesis"
    );
    assert_eq!(
        document.event_id, loser_event_id,
        "the outbox record id must be the evicted event's own id"
    );
    assert_eq!(document.placement, placement);
    match &document.event {
        DocumentSyncOutboxEvent::AdminOperation { event, .. } => {
            assert_eq!(
                event.event_id, loser_event_id,
                "embedded admin event id must survive for applier dedup"
            );
        }
        other => panic!("expected an AdminOperation re-emission, got {other:?}"),
    }

    let foreign_event_id = Ulid::from_parts(0xC3, 3);
    let foreign_admin = test_admin_event(
        foreign_event_id,
        admin_target,
        &test_actor(3, user_id, realm_id),
        1,
        AdminDocumentOperation::UserNameSet {
            name: "foreign".into(),
        },
    );
    let foreign_payload = DocumentSyncEvent::AdminOperation {
        target: target.clone(),
        origin_signature: sign_as_origin(&foreign_admin, &placement),
        event: Box::new(foreign_admin),
        placement,
    };
    let foreign = loser.decode_eviction(TopicEviction {
        topic_id,
        losing_genesis: winner_genesis,
        winning_genesis: winner_genesis,
        evicted: vec![irokle_crate::EvictedOp {
            op_id: irokle_crate::OpId::from_bytes([91; 32]),
            actor_id: irokle_crate::actor_id_for(topic_id, winner_peer),
            author: winner_peer,
            actor_seq: 2,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&foreign_payload).expect("event encodes"),
            ),
        }],
    });
    assert_eq!(foreign.len(), 1);
    assert_eq!(foreign[0].event_id, foreign_event_id);

    // Irokle already removed the losing chain, so the payload is durable
    // before it is handed out and stays recoverable until it is released.
    let journalled = loser
        .pending_evictions()
        .await
        .expect("eviction journal reads");
    assert_eq!(journalled, vec![reemitted.clone()]);
    loser
        .clear_eviction(reemitted.key)
        .await
        .expect("journal entry releases");
    assert!(
        loser
            .pending_evictions()
            .await
            .expect("eviction journal reads")
            .is_empty()
    );
}
#[tokio::test]
async fn reconcile_retries_admin_events_deferred_before_realm_config() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([68; 32]);
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(68).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");

    let admin_user_id = UserId::local(Ulid::from_parts(1_625, 1), realm_id);
    let bootstrap_actor = test_actor(68, UserId::nil(realm_id), realm_id);
    let admin_actor = test_actor(68, admin_user_id, realm_id);
    let role_id = Ulid::from_parts(1_626, 1);
    let auth_target = DocumentSyncTarget::RealmAuthorization { realm_id };
    let auth_topic = auth_target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let config_target = DocumentSyncTarget::RealmConfig { realm_id };
    let config_topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
    assert_ne!(auth_topic, config_topic);

    let role = test_admin_event(
        Ulid::from_parts(1_627, 1),
        AdminDocumentTarget::Realm { realm_id },
        &bootstrap_actor,
        1,
        AdminDocumentOperation::RealmRoleCreated {
            role: test_admin_role_definition(
                role_id,
                "realm_admin",
                &format!("/{realm_id}/admin/**"),
                Permission::WRITE,
            ),
        },
    );
    let mut assignment = test_admin_event(
        Ulid::from_parts(1_628, 1),
        AdminDocumentTarget::Realm { realm_id },
        &admin_actor,
        2,
        AdminDocumentOperation::RealmRoleUserAssignmentAdded {
            role_id,
            user_id: admin_user_id,
        },
    );
    assignment.observed.advance(admin_actor.node_id, 1);
    let ensure = test_admin_event(
        Ulid::from_parts(1_629, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &bootstrap_actor,
        1,
        AdminDocumentOperation::RealmConfigNodeEnsured {
            node_id: bootstrap_actor.node_id,
            kind: RealmNodeKind::Management,
        },
    );
    let mut settings = test_admin_event(
        Ulid::from_parts(1_630, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &bootstrap_actor,
        2,
        AdminDocumentOperation::RealmConfigSettingsSet {
            metadata_replication: MetadataReplicationConfig::new(3),
            discovery: test_discovery(68, "https://management.example:443"),
        },
    );
    settings.observed.advance(bootstrap_actor.node_id, 1);

    for (target, events) in [
        (
            auth_target.clone(),
            vec![Box::new(role), Box::new(assignment)],
        ),
        (
            config_target.clone(),
            vec![Box::new(settings), Box::new(ensure)],
        ),
    ] {
        let documents = events
            .into_iter()
            .map(|event| DocumentSyncPublish::AdminOperation {
                target: target.clone(),
                event,
                placement: PlacementRef::NIL,
                allow_genesis: true,
                origin_signature: None,
            })
            .collect();
        assert!(matches!(
            service.publish_documents(documents, Vec::new()).await,
            DocumentSyncNetEvent::DocumentsPublished { .. }
        ));
        reset_test_cursor(&service, target.sync_topic_id(realm_id, &PlacementRef::NIL)).await;
    }

    service
        .reconcile_document_topics([auth_topic])
        .await
        .expect("realm authorization reconciliation defers the assignment");
    let auth_before_config = read_realm_auth_doc(&storage, realm_id).await;
    assert!(
        auth_before_config
            .roles
            .get(&role_id)
            .expect("bootstrap role exists")
            .assigned_users
            .is_empty(),
        "the assignment must wait for realm configuration"
    );
    let deferred_cursor = read_test_cursor(&storage, auth_topic)
        .await
        .unwrap_or_default();
    let auth_clock = service
        .node()
        .storage()
        .actor_clock(&auth_topic)
        .expect("auth topic clock");
    assert!(!deferred_cursor.dominates(&auth_clock));
    let deferred_topics: BTreeMap<DocumentSyncDependency, BTreeSet<irokle_crate::TopicId>> =
        postcard::from_bytes(
            &read_storage_value(
                &storage,
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                deferred_topics_key(),
            )
            .await
            .expect("deferred topic registry is persisted"),
        )
        .expect("deferred topic registry decodes");
    assert_eq!(
        deferred_topics
            .get(&DocumentSyncDependency::RealmConfig(realm_id))
            .and_then(|topics| topics.get(&auth_topic)),
        Some(&auth_topic)
    );
    assert!(
        service
            .document_topic_ids()
            .expect("document topics list")
            .contains(&auth_topic),
        "deferred auth topic must remain discoverable"
    );

    let result = service
        .reconcile_document_topics([config_topic])
        .await
        .expect("realm config reconciliation retries deferred admin topics");
    assert!(result.targets.contains(&config_target));
    assert!(
        result.targets.contains(&auth_target),
        "realm authorization target was not retried: {:?}",
        result.targets
    );
    let auth = read_realm_auth_doc(&storage, realm_id).await;
    assert!(
        auth.roles
            .get(&role_id)
            .expect("realm admin role exists")
            .assigned_users
            .contains(&admin_user_id),
        "the deferred assignment must apply after realm configuration"
    );
    let applied_cursor = read_test_cursor(&storage, auth_topic)
        .await
        .expect("applied cursor is stored");
    assert!(applied_cursor.dominates(&auth_clock));

    let group_id = Ulid::from_parts(1_631, 1);
    let group_target = DocumentSyncTarget::GroupAuthorization { group_id };
    let group_placement = admin_test_placement();
    // Shard topics are join-only at publish; create the genesis eagerly.
    service
        .ensure_document_sync_topics(
            &[group_target.sync_topic_id(realm_id, &group_placement)],
            Vec::new(),
        )
        .expect("group shard topic genesis");
    let group_role_id = Ulid::from_parts(1_632, 1);
    let mut group_role = test_admin_event(
        Ulid::from_parts(1_633, 1),
        AdminDocumentTarget::Group { group_id },
        &admin_actor,
        2,
        AdminDocumentOperation::GroupRoleCreated {
            role: test_admin_role_definition(
                group_role_id,
                "member",
                &format!("/{realm_id}/g/{group_id}/**"),
                Permission::WRITE,
            ),
        },
    );
    group_role.observed.advance(admin_actor.node_id, 1);
    let group_create = test_admin_event(
        Ulid::from_parts(1_634, 1),
        AdminDocumentTarget::Group { group_id },
        &admin_actor,
        1,
        AdminDocumentOperation::GroupCreated {
            realm_id,
            display_name: "reordered".to_string(),
            owner: admin_user_id,
        },
    );
    assert!(matches!(
        service
            .publish_documents(
                vec![
                    DocumentSyncPublish::AdminOperation {
                        target: group_target.clone(),
                        event: Box::new(group_role),
                        placement: group_placement,
                        allow_genesis: true,
                        origin_signature: None,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: group_target.clone(),
                        event: Box::new(group_create),
                        placement: group_placement,
                        allow_genesis: true,
                        origin_signature: None,
                    },
                ],
                Vec::new(),
            )
            .await,
        DocumentSyncNetEvent::DocumentsPublished { .. }
    ));
    reset_test_cursor(
        &service,
        group_target.sync_topic_id(realm_id, &group_placement),
    )
    .await;
    service
        .reconcile_document_topics([group_target.sync_topic_id(realm_id, &group_placement)])
        .await
        .expect("same-topic group prerequisite is retried");
    assert!(
        read_group_auth_doc(&storage, group_id)
            .await
            .roles
            .contains_key(&group_role_id)
    );

    service.shutdown().await;
}
