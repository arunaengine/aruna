use std::sync::Arc;
use std::time::Duration;

use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{METADATA_DOCUMENT_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::storage_entries::metadata_document_key;
use aruna_core::structs::{
    Actor, MetadataRegistryRecord, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::api::{
    ExportMetadataRoCrateRequest, ExportMetadataRoCrateResult, MetadataApiError,
    MetadataRoCrateExportView, export_metadata_rocrate,
};
use aruna_operations::metadata::materialization_queue::process_metadata_materialization_batch;
use aruna_operations::metadata::projector::{
    drain_pending_metadata_projection_queue, replay_metadata_event_log,
};

use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tempfile::TempDir;
use ulid::Ulid;

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

// On a `>replica` realm the registry record is everywhere-placed but the graph
// lives only on the document's holders. A non-holder's RO-Crate export must fall
// through to a holder over the `Alpn::Metadata` fan-out instead of `503`-ing
// forever; when every holder is down the export degrades to a retryable 503.
#[tokio::test]
async fn rocrate_export_routes_to_graph_holder_and_503s_when_all_down() {
    let realm_id = RealmId([71u8; 32]);
    let node_a = spawn_node(realm_id).await;
    let node_b = spawn_node(realm_id).await;

    // One-way reachability: B can dial A for the export request, but A cannot dial
    // B, so A never replicates the graph to B and B stays a pure non-holder.
    node_b.net.add_peer_addr(node_a.net.endpoint_addr()).await;

    let config = build_config(realm_id, &[node_a.net.node_id(), node_b.net.node_id()]);
    install_config(&node_a, &config).await;
    install_config(&node_b, &config).await;

    let group_id = Ulid::new();
    let document_id = Ulid::new();
    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: node_a.net.node_id(),
                user_id: UserId::local(Ulid::new(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            document_path: "datasets/export".to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Export Dataset".to_string(),
                description: "Graph lives only on node A".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        node_a.context.as_ref(),
    )
    .await
    .expect("create on A")
    .record;

    // Materialize the graph on node A only.
    materialize_local(&node_a, &created.graph_iri).await;

    // Record everywhere: A holds it from the create; give B the same record (holder
    // pinned to A) so B can resolve the document — but B never gets the graph.
    let mut b_record = created.clone();
    b_record.holder_node_ids = vec![node_a.net.node_id()];
    write_document_record(&node_b, &b_record).await;

    // The non-holder B exports by falling through to the holder A.
    let export = export_metadata_rocrate(
        node_b.context.as_ref(),
        realm_id,
        node_b.net.node_id(),
        export_request(document_id),
    )
    .await
    .expect("B exports the document via its holder");
    let ExportMetadataRoCrateResult::Full { jsonld, .. } = export else {
        panic!("expected a full export");
    };
    assert!(
        jsonld.contains(&created.graph_iri),
        "the routed export must carry the document graph: {jsonld}"
    );

    // Every graph holder is now down: B's local graph is absent and no holder
    // answers, so the export degrades to a retryable 503 rather than a hard error.
    node_a.net.shutdown().await;
    let error = export_metadata_rocrate(
        node_b.context.as_ref(),
        realm_id,
        node_b.net.node_id(),
        export_request(document_id),
    )
    .await
    .expect_err("no reachable holder is a 503");
    assert!(
        matches!(error, MetadataApiError::ServiceUnavailable),
        "unexpected error: {error:?}"
    );

    node_b.net.shutdown().await;
}

fn export_request(document_id: Ulid) -> ExportMetadataRoCrateRequest {
    ExportMetadataRoCrateRequest {
        document_id,
        auth: None,
        bearer_token: None,
        view: MetadataRoCrateExportView::Full,
        limit: None,
        offset: None,
        after: None,
    }
}

async fn materialize_local(node: &TestNode, graph_iri: &str) {
    let handle = node
        .context
        .metadata_handle
        .as_ref()
        .expect("metadata handle");
    for _ in 0..100 {
        let drained = drain_pending_metadata_projection_queue(node.context.as_ref())
            .await
            .expect("drain projection queue");
        if drained.markers_examined == 0 {
            replay_metadata_event_log(node.context.as_ref())
                .await
                .expect("replay event log");
        }
        process_metadata_materialization_batch(node.context.as_ref())
            .await
            .expect("process materialization batch");
        if handle
            .export_rocrate_jsonld(graph_iri.to_string())
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("graph did not materialize on the holder within the sampling bound");
}

fn build_config(realm_id: RealmId, node_ids: &[aruna_core::NodeId]) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for node_id in node_ids {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config
}

async fn install_config(node: &TestNode, config: &RealmConfigDocument) {
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: UserId::nil(config.realm_id),
        realm_id: config.realm_id,
    };
    let bytes = config.to_bytes(&actor).expect("config serializes");
    write_storage(
        node,
        REALM_CONFIG_KEYSPACE,
        config.realm_id.as_bytes().to_vec(),
        bytes,
    )
    .await;
    node.net
        .refresh_realm_peers_from_document(config)
        .await
        .expect("refresh realm peers");
}

async fn write_document_record(node: &TestNode, record: &MetadataRegistryRecord) {
    write_storage(
        node,
        METADATA_DOCUMENT_INDEX_KEYSPACE,
        metadata_document_key(record.document_id).to_vec(),
        postcard::to_allocvec(record).expect("record serializes"),
    )
    .await;
}

async fn write_storage(node: &TestNode, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    match node
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected write event for {key_space}: {other:?}"),
    }
}

async fn spawn_node(realm_id: RealmId) -> TestNode {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle");
    let task_handle = TaskHandle::new();
    let metadata_handle = MetadataHandle::new(
        temp_dir.path().join("metadata"),
        net.node_id(),
        storage.clone(),
        Some(net.clone()),
        Some(net.document_sync_node()),
        Some(net.document_sync_database()),
    )
    .expect("metadata handle");
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: Some(task_handle.clone()),
    });
    // Only the net-incoming dispatcher is wired: it serves the `Alpn::Metadata`
    // export fan-out. The task-incoming loop is left off so this test drives
    // projection and materialization deterministically without background races.
    let _ = task_handle;
    initialize_net_incoming(context.clone());
    TestNode {
        _temp_dir: temp_dir,
        net,
        context,
    }
}
