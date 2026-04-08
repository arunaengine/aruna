use std::sync::Arc;
use std::time::Duration;

use aruna_core::alpn::Alpn;
use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::structs::{Actor, MetadataDocument, RealmId};
use aruna_net::InboundEventHandler;
use aruna_net::streams::BiStream;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::automerge::AutomergeHandle;
use aruna_operations::automerge::repository::{read_effect, write_effect};
use aruna_operations::automerge_announce::AnnounceAutomergeDocumentOperation;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::incoming_automerge::IncomingAutomergeOperation;
use aruna_operations::outgoing_automerge::OutgoingAutomergeOperation;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use async_trait::async_trait;
use tempfile::tempdir;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, timeout};
use ulid::Ulid;

#[tokio::test]
async fn metadata_automerge_sync_converges_between_nodes() -> Result<(), Box<dyn std::error::Error>>
{
    let temp_a = tempdir()?;
    let temp_b = tempdir()?;
    let storage_a = FjallStorage::open(temp_a.path().to_str().ok_or("invalid temp path")?)?;
    let storage_b = FjallStorage::open(temp_b.path().to_str().ok_or("invalid temp path")?)?;

    let cfg = || NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
        use_dns_discovery: false,
        ..NetConfig::default()
    };

    let net_a = NetHandle::new(cfg(), storage_a.clone()).await?;
    let net_b = NetHandle::new(cfg(), storage_b.clone()).await?;

    net_a.add_peer_addr(net_b.endpoint_addr()).await;
    net_b.add_peer_addr(net_a.endpoint_addr()).await;

    let task_a = TaskHandle::new();
    let task_b = TaskHandle::new();
    let automerge_a = AutomergeHandle::new(Some(net_a.clone()));
    let automerge_b = AutomergeHandle::new(Some(net_b.clone()));

    let context_a = Arc::new(DriverContext {
        storage_handle: storage_a.clone(),
        net_handle: Some(net_a.clone()),
        blob_handle: None,
        automerge_handle: Some(automerge_a),
        metadata_handle: None,
        task_handle: Some(task_a.clone()),
    });
    let context_b = Arc::new(DriverContext {
        storage_handle: storage_b.clone(),
        net_handle: Some(net_b.clone()),
        blob_handle: None,
        automerge_handle: Some(automerge_b),
        metadata_handle: None,
        task_handle: Some(task_b.clone()),
    });

    initialize_task_incoming(context_a.clone(), task_a).await;
    initialize_task_incoming(context_b.clone(), task_b).await;

    let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel();
    net_b.set_inbound_handler(Arc::new(TestInboundHandler {
        context: context_b.clone(),
        inbound_results: incoming_tx,
    }));

    let group_id = Ulid::new();
    let document_id = Ulid::new();
    let document = AutomergeDocumentVariant::Metadata {
        group_id,
        document_id,
    };

    let actor_a = Actor {
        node_id: net_a.node_id(),
        user_id: Ulid::new(),
        realm_id: RealmId([7u8; 32]),
    };
    let actor_b = Actor {
        node_id: net_b.node_id(),
        user_id: Ulid::new(),
        realm_id: RealmId([7u8; 32]),
    };

    let base = MetadataDocument::new(group_id, document_id, String::new());
    let base_bytes = base.to_bytes(&actor_a)?;
    write_document(&storage_a, &document, base_bytes.clone()).await?;
    write_document(&storage_b, &document, base_bytes).await?;

    apply_metadata_triple(
        &storage_a,
        &document,
        &actor_a,
        "<http://example.org/root> <http://schema.org/name> \"from-a\"",
    )
    .await?;
    apply_metadata_triple(
        &storage_b,
        &document,
        &actor_b,
        "<http://example.org/root> <http://schema.org/description> \"from-b\"",
    )
    .await?;

    let outbound_result = drive(
        OutgoingAutomergeOperation::new(net_b.node_id(), document.clone()),
        context_a.as_ref(),
    )
    .await;

    let inbound_result = timeout(Duration::from_secs(5), incoming_rx.recv()).await?;
    if let Err(error) = outbound_result {
        return Err(format!("outbound sync failed: {error}; inbound={inbound_result:?}").into());
    }
    match inbound_result {
        Some(Ok(())) => {}
        Some(Err(error)) => return Err(error.into()),
        None => return Err("missing inbound automerge result".into()),
    }

    let expected = [
        "<http://example.org/root> <http://schema.org/name> \"from-a\"".to_string(),
        "<http://example.org/root> <http://schema.org/description> \"from-b\"".to_string(),
    ]
    .into_iter()
    .collect();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let current_a = drive(
            GetMetadataDocumentOperation::new(group_id, document_id),
            context_a.as_ref(),
        )
        .await?;
        let current_b = drive(
            GetMetadataDocumentOperation::new(group_id, document_id),
            context_b.as_ref(),
        )
        .await?;

        if current_a.triples == expected && current_b.triples == expected {
            break;
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "metadata docs did not converge: a={:?} b={:?}",
                current_a.triples, current_b.triples
            )
            .into());
        }

        sleep(Duration::from_millis(50)).await;
    }

    net_a.shutdown().await;
    net_b.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn metadata_automerge_sync_populates_missing_document()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_a = tempdir()?;
    let temp_b = tempdir()?;
    let storage_a = FjallStorage::open(temp_a.path().to_str().ok_or("invalid temp path")?)?;
    let storage_b = FjallStorage::open(temp_b.path().to_str().ok_or("invalid temp path")?)?;

    let cfg = || NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
        use_dns_discovery: false,
        ..NetConfig::default()
    };

    let net_a = NetHandle::new(cfg(), storage_a.clone()).await?;
    let net_b = NetHandle::new(cfg(), storage_b.clone()).await?;

    net_a.add_peer_addr(net_b.endpoint_addr()).await;
    net_b.add_peer_addr(net_a.endpoint_addr()).await;

    let task_a = TaskHandle::new();
    let task_b = TaskHandle::new();
    let automerge_a = AutomergeHandle::new(Some(net_a.clone()));
    let automerge_b = AutomergeHandle::new(Some(net_b.clone()));

    let context_a = Arc::new(DriverContext {
        storage_handle: storage_a.clone(),
        net_handle: Some(net_a.clone()),
        blob_handle: None,
        automerge_handle: Some(automerge_a),
        metadata_handle: None,
        task_handle: Some(task_a.clone()),
    });
    let context_b = Arc::new(DriverContext {
        storage_handle: storage_b.clone(),
        net_handle: Some(net_b.clone()),
        blob_handle: None,
        automerge_handle: Some(automerge_b),
        metadata_handle: None,
        task_handle: Some(task_b.clone()),
    });

    initialize_task_incoming(context_a.clone(), task_a).await;
    initialize_task_incoming(context_b.clone(), task_b).await;

    let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel();
    net_b.set_inbound_handler(Arc::new(TestInboundHandler {
        context: context_b.clone(),
        inbound_results: incoming_tx,
    }));

    let group_id = Ulid::new();
    let document_id = Ulid::new();
    let document = AutomergeDocumentVariant::Metadata {
        group_id,
        document_id,
    };

    let actor_a = Actor {
        node_id: net_a.node_id(),
        user_id: Ulid::new(),
        realm_id: RealmId([9u8; 32]),
    };

    let mut doc_a = MetadataDocument::new(group_id, document_id, String::new());
    doc_a
        .triples
        .insert("<http://example.org/root> <http://schema.org/name> \"bootstrap\"".to_string());
    write_document(&storage_a, &document, doc_a.to_bytes(&actor_a)?).await?;

    let outbound_result = drive(
        OutgoingAutomergeOperation::new(net_b.node_id(), document.clone()),
        context_a.as_ref(),
    )
    .await;

    let inbound_result = timeout(Duration::from_secs(5), incoming_rx.recv()).await?;
    if let Err(error) = outbound_result {
        return Err(format!("outbound sync failed: {error}; inbound={inbound_result:?}").into());
    }
    match inbound_result {
        Some(Ok(())) => {}
        Some(Err(error)) => return Err(error.into()),
        None => return Err("missing inbound automerge result".into()),
    }

    let restored = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        context_b.as_ref(),
    )
    .await?;
    assert_eq!(restored.triples, doc_a.triples);

    net_a.shutdown().await;
    net_b.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn metadata_gossip_announcement_triggers_sync() -> Result<(), Box<dyn std::error::Error>> {
    let temp_a = tempdir()?;
    let temp_b = tempdir()?;
    let storage_a = FjallStorage::open(temp_a.path().to_str().ok_or("invalid temp path")?)?;
    let storage_b = FjallStorage::open(temp_b.path().to_str().ok_or("invalid temp path")?)?;

    let cfg = || NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
        use_dns_discovery: false,
        ..NetConfig::default()
    };

    let net_a = NetHandle::new(cfg(), storage_a.clone()).await?;
    let net_b = NetHandle::new(cfg(), storage_b.clone()).await?;

    net_a.add_peer_addr(net_b.endpoint_addr()).await;
    net_b.add_peer_addr(net_a.endpoint_addr()).await;

    let task_a = TaskHandle::new();
    let task_b = TaskHandle::new();
    let automerge_a = AutomergeHandle::new(Some(net_a.clone()));
    let automerge_b = AutomergeHandle::new(Some(net_b.clone()));

    let context_a = Arc::new(DriverContext {
        storage_handle: storage_a.clone(),
        net_handle: Some(net_a.clone()),
        blob_handle: None,
        automerge_handle: Some(automerge_a),
        metadata_handle: None,
        task_handle: Some(task_a.clone()),
    });
    let context_b = Arc::new(DriverContext {
        storage_handle: storage_b.clone(),
        net_handle: Some(net_b.clone()),
        blob_handle: None,
        automerge_handle: Some(automerge_b),
        metadata_handle: None,
        task_handle: Some(task_b.clone()),
    });

    initialize_net_incoming(context_a.clone());
    initialize_net_incoming(context_b.clone());
    initialize_task_incoming(context_a.clone(), task_a).await;
    initialize_task_incoming(context_b.clone(), task_b).await;

    let group_id = Ulid::new();
    let document_id = Ulid::new();
    let document = AutomergeDocumentVariant::Metadata {
        group_id,
        document_id,
    };

    let actor_a = Actor {
        node_id: net_a.node_id(),
        user_id: Ulid::new(),
        realm_id: RealmId([11u8; 32]),
    };
    let actor_b = Actor {
        node_id: net_b.node_id(),
        user_id: Ulid::new(),
        realm_id: RealmId([11u8; 32]),
    };

    let base = MetadataDocument::new(group_id, document_id, String::new());
    let base_bytes = base.to_bytes(&actor_a)?;
    write_document(&storage_a, &document, base_bytes.clone()).await?;
    write_document(&storage_b, &document, base_bytes).await?;

    apply_metadata_triple(
        &storage_a,
        &document,
        &actor_a,
        "<http://example.org/root> <http://schema.org/name> \"from-a\"",
    )
    .await?;
    apply_metadata_triple(
        &storage_b,
        &document,
        &actor_b,
        "<http://example.org/root> <http://schema.org/description> \"from-b\"",
    )
    .await?;

    drive(
        AnnounceAutomergeDocumentOperation::new(document.clone(), net_b.node_id()),
        context_b.as_ref(),
    )
    .await?;
    drive(
        AnnounceAutomergeDocumentOperation::new(document.clone(), net_a.node_id()),
        context_a.as_ref(),
    )
    .await?;
    sleep(Duration::from_millis(250)).await;
    drive(
        AnnounceAutomergeDocumentOperation::new(document.clone(), net_a.node_id()),
        context_a.as_ref(),
    )
    .await?;
    drive(
        AnnounceAutomergeDocumentOperation::new(document.clone(), net_b.node_id()),
        context_b.as_ref(),
    )
    .await?;

    let expected = [
        "<http://example.org/root> <http://schema.org/name> \"from-a\"".to_string(),
        "<http://example.org/root> <http://schema.org/description> \"from-b\"".to_string(),
    ]
    .into_iter()
    .collect();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let current_a = drive(
            GetMetadataDocumentOperation::new(group_id, document_id),
            context_a.as_ref(),
        )
        .await?;
        let current_b = drive(
            GetMetadataDocumentOperation::new(group_id, document_id),
            context_b.as_ref(),
        )
        .await?;

        if current_a.triples == expected && current_b.triples == expected {
            break;
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "gossip-triggered sync did not converge: a={:?} b={:?}",
                current_a.triples, current_b.triples
            )
            .into());
        }

        sleep(Duration::from_millis(50)).await;
    }

    net_a.shutdown().await;
    net_b.shutdown().await;
    Ok(())
}

async fn write_document(
    storage: &aruna_storage::StorageHandle,
    document: &AutomergeDocumentVariant,
    bytes: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    match storage
        .send_effect(write_effect(document, bytes, None))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string().into()),
        other => Err(format!("unexpected storage event: {other:?}").into()),
    }
}

async fn read_document(
    storage: &aruna_storage::StorageHandle,
    document: &AutomergeDocumentVariant,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    match storage.send_effect(read_effect(document, None)).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| value.to_vec())
            .ok_or_else(|| "missing document bytes".into()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string().into()),
        other => Err(format!("unexpected storage event: {other:?}").into()),
    }
}

async fn apply_metadata_triple(
    storage: &aruna_storage::StorageHandle,
    document: &AutomergeDocumentVariant,
    actor: &Actor,
    triple: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let current = read_document(storage, document).await?;
    let mut metadata = MetadataDocument::from_bytes(&current)?;
    metadata.triples.insert(triple.to_string());

    write_document(
        storage,
        document,
        metadata.reconcile_bytes(Some(&current), actor)?,
    )
    .await
}

#[derive(Debug)]
struct TestInboundHandler {
    context: Arc<DriverContext>,
    inbound_results: mpsc::UnboundedSender<Result<(), String>>,
}

#[async_trait]
impl InboundEventHandler for TestInboundHandler {
    async fn handle_gossip_message(
        &self,
        _topic: aruna_core::TopicId,
        _sender: aruna_core::NodeId,
        _data: Vec<u8>,
    ) {
    }

    async fn handle_incoming_stream(
        &self,
        alpn: Alpn,
        stream: BiStream,
        node_id: aruna_core::NodeId,
    ) {
        if !matches!(alpn, Alpn::Automerge) {
            return;
        }

        let Some(automerge_handle) = self.context.automerge_handle.clone() else {
            let _ = self
                .inbound_results
                .send(Err("missing automerge handle".to_string()));
            return;
        };

        let sync_id = automerge_handle
            .register_inbound_stream(stream, node_id)
            .await;
        let Some(net_handle) = self.context.net_handle.as_ref() else {
            let _ = self
                .inbound_results
                .send(Err("missing net handle".to_string()));
            return;
        };
        let result = drive(
            IncomingAutomergeOperation::new(sync_id, node_id, net_handle.node_id()),
            self.context.as_ref(),
        )
        .await
        .map_err(|error| error.to_string());
        let _ = self.inbound_results.send(result);
    }
}
