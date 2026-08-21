use crate::config::PersistedNodeState;
use aruna_api::server_state::{
    INITIAL_LOCAL_ONBOARDING_SECRET_KEY, load_persisted_state, persist_state,
};
use aruna_core::document::{DocumentSyncNetEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE, USER_KEYSPACE};
use aruna_core::onboarding::{
    OnboardingMode, OnboardingPurpose, OnboardingSecret, OnboardingSyncTicket,
};
use aruna_core::{DocumentSyncEffect, NodeId, UserId};
use aruna_operations::create_onboarding_secret::{
    CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::notifications::watch::interest::{
    ensure_local_watch_interest_digest, mark_watch_interest_dirty,
};
use aruna_operations::placement::placement_ref_for_target;
use aruna_operations::replicate_documents::{
    ReplicateDocumentsConfig, ReplicateDocumentsOperation,
};
use byteview::ByteView;
use crypto_box::{
    SalsaBox, SecretKey as BoxSecretKey,
    aead::{Aead, AeadCore, OsRng as CryptoOsRng},
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use tracing::warn;

const ONBOARDING_PLACEMENT_RETRY_INTERVAL: Duration = Duration::from_millis(100);

pub async fn realm_bootstrap_exists(
    driver_ctx: &DriverContext,
    realm_id: &aruna_core::structs::RealmId,
) -> Result<bool, Box<dyn std::error::Error>> {
    let key = ByteView::from(*realm_id.as_bytes());

    for key_space in [AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE] {
        match driver_ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.clone(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) => {}
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => return Ok(false),
            Event::Storage(StorageEvent::Error { error }) => return Err(Box::new(error)),
            other => return Err(format!("unexpected storage event: {other:?}").into()),
        }
    }

    Ok(true)
}

pub async fn publish_core_documents(
    driver_ctx: &DriverContext,
    node_id: NodeId,
    realm_id: aruna_core::structs::RealmId,
    allow_genesis: bool,
    documents: Vec<DocumentSyncTarget>,
) -> Result<(), Box<dyn std::error::Error>> {
    if documents.is_empty() {
        return Ok(());
    }

    drive(
        ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node_id,
            excluded_peers: Vec::new(),
            documents,
            // Only the realm-bootstrap node may mint shared-topic genesis;
            // joining/provisioned nodes announce with false and join it.
            allow_genesis,
        }),
        driver_ctx,
    )
    .await?;

    Ok(())
}

fn watch_target_needed(digest_created: bool, allow_genesis: bool, topic_exists: bool) -> bool {
    digest_created || allow_genesis && !topic_exists
}

pub async fn prepare_core_documents(
    driver_ctx: &DriverContext,
    node_id: NodeId,
    realm_id: aruna_core::structs::RealmId,
    allow_genesis: bool,
    include_node_info: bool,
) -> Result<Vec<DocumentSyncTarget>, Box<dyn std::error::Error>> {
    let digest_created =
        ensure_local_watch_interest_digest(&driver_ctx.storage_handle, realm_id, node_id)
            .await
            .map_err(|error| {
                format!("failed to initialize local watch interest digest: {error}")
            })?;
    if digest_created {
        mark_watch_interest_dirty(driver_ctx, realm_id)
            .await
            .map_err(|error| format!("failed to mark local watch interest dirty: {error}"))?;
    }

    let mut documents = vec![
        DocumentSyncTarget::RealmAuthorization { realm_id },
        DocumentSyncTarget::RealmConfig { realm_id },
        // Announce the shared realm-scoped node-usage topic so every realm node
        // subscribes to it and receives all peers' usage snapshots.
        DocumentSyncTarget::NodeUsage {
            realm_id,
            node_id,
            group_id: None,
        },
    ];
    if include_node_info {
        // Initial and joining nodes announce before their first heartbeat;
        // provisioned restarts leave refresh publication to the timer.
        documents.push(DocumentSyncTarget::NodeInfo { realm_id, node_id });
    }
    let watch_target = DocumentSyncTarget::WatchInterest { realm_id, node_id };
    let topic_exists = if allow_genesis {
        let net_handle = driver_ctx
            .net_handle
            .as_ref()
            .ok_or("net handle unavailable while checking watch interest genesis")?;
        net_handle
            .document_sync_topic_exists(
                watch_target.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL),
            )
            .map_err(|error| format!("failed to check watch interest topic: {error}"))?
    } else {
        true
    };
    // A newly stored digest must be announced once. The authoritative node also
    // repairs a missing topic after a partial first boot without republishing on
    // an unchanged healthy restart.
    if watch_target_needed(digest_created, allow_genesis, topic_exists) {
        documents.push(watch_target);
    }

    match driver_ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Iter {
            key_space: USER_KEYSPACE.to_string(),
            prefix: Some(UserId::storage_prefix(realm_id)),
            start: None,
            limit: 10_000,
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => {
            documents.extend(values.into_iter().filter_map(|(key, _)| {
                UserId::from_storage_key(&key)
                    .ok()
                    .filter(|user_id| user_id.realm_id == realm_id)
                    .map(|user_id| DocumentSyncTarget::User { user_id })
            }));
            Ok(documents)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(Box::new(error)),
        other => Err(format!("unexpected user iter result: {other:?}").into()),
    }
}

pub async fn fetch_core_onboarding_documents(
    driver_ctx: &DriverContext,
    node_state: &PersistedNodeState,
    realm_id: &aruna_core::structs::RealmId,
    bootstrap_peer: Option<NodeId>,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_peer = bootstrap_peer.ok_or("missing bootstrap peer")?;
    let onboarding_sync_ticket = node_state
        .onboarding_sync_ticket
        .as_deref()
        .ok_or("missing onboarding sync ticket")?;
    let onboarding_sync_ticket = OnboardingSyncTicket::decode(onboarding_sync_ticket)?;
    let Some(net_handle) = driver_ctx.net_handle.as_ref() else {
        return Err("net handle unavailable".into());
    };
    let realm_id = *realm_id;

    // Fetch the shared realm documents first (they include the realm config), so
    // the shard-classed user documents can then be routed onto their shard
    // topics via the freshly synced config.
    let mut user_documents = Vec::new();
    for document in onboarding_sync_ticket.payload.documents.clone() {
        if matches!(document, DocumentSyncTarget::User { .. }) {
            user_documents.push(document);
            continue;
        }
        let topic = document.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL);
        sync_topic_from_peer(net_handle, topic, bootstrap_peer, &document, timeout).await?;
    }

    if !user_documents.is_empty() {
        let config = load_realm_config(driver_ctx, realm_id).await;
        let mut synced_topics = HashSet::new();
        for document in user_documents {
            let placement = match config.as_ref() {
                Some(config) => placement_ref_for_target(config, &document, Default::default()),
                None => aruna_core::structs::PlacementRef::NIL,
            };
            if placement == aruna_core::structs::PlacementRef::NIL {
                warn!(document = ?document, "Skipping onboarding user document without a shard placement");
                continue;
            }
            let Some(topic) =
                unique_user_topic(&mut synced_topics, realm_id, &placement, &document)
            else {
                continue;
            };
            sync_topic_from_peer(net_handle, topic, bootstrap_peer, &document, timeout).await?;
        }
    }

    Ok(())
}

pub async fn wait_for_onboarding_placement(
    driver_ctx: &DriverContext,
    realm_id: aruna_core::structs::RealmId,
    node_id: NodeId,
    bootstrap_peer: Option<NodeId>,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_peer = bootstrap_peer.ok_or("missing bootstrap peer")?;
    let target = DocumentSyncTarget::RealmConfig { realm_id };

    tokio::time::timeout(timeout, async {
        loop {
            let config = drive(GetRealmConfigOperation::new(realm_id), driver_ctx).await?;
            if node_is_ready(&config, node_id) {
                return Ok::<(), Box<dyn std::error::Error>>(());
            }

            sync_topic_from_peer(
                driver_ctx
                    .net_handle
                    .as_ref()
                    .ok_or("net handle unavailable")?,
                target.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL),
                bootstrap_peer,
                &target,
                timeout,
            )
            .await?;
            tokio::time::sleep(ONBOARDING_PLACEMENT_RETRY_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| {
        format!("timed out after {timeout:?} waiting for onboarding placement for node {node_id}")
    })?
}

fn node_is_ready(config: &aruna_core::structs::RealmConfigDocument, node_id: NodeId) -> bool {
    // A usable band grant plus its JobControl binding: the node must be able
    // to mint owner-encoded JobIds before it starts serving.
    config.has_node(node_id)
        && config.placement_entry(node_id).is_some()
        && !config
            .handle_range_directory()
            .granted_to(&node_id)
            .is_empty()
        && config.job_control_handle(&node_id).is_some()
}

async fn load_realm_config(
    driver_ctx: &DriverContext,
    realm_id: aruna_core::structs::RealmId,
) -> Option<aruna_core::structs::RealmConfigDocument> {
    match driver_ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(*realm_id.as_bytes()),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .and_then(|bytes| aruna_core::structs::RealmConfigDocument::from_bytes(&bytes).ok()),
        _ => None,
    }
}

fn unique_user_topic(
    synced_topics: &mut HashSet<::irokle::TopicId>,
    realm_id: aruna_core::structs::RealmId,
    placement: &aruna_core::structs::PlacementRef,
    document: &DocumentSyncTarget,
) -> Option<::irokle::TopicId> {
    let topic = document.sync_topic_id(realm_id, placement);
    synced_topics.insert(topic).then_some(topic)
}

async fn sync_topic_from_peer(
    net_handle: &aruna_net::NetHandle,
    topic: ::irokle::TopicId,
    bootstrap_peer: NodeId,
    document_for_error: &DocumentSyncTarget,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let document_for_error = document_for_error.clone();
    let sync = net_handle.send_effect(Effect::Net(NetEffect::DocumentSync(
        DocumentSyncEffect::SyncDocument {
            topic,
            peers: vec![bootstrap_peer],
        },
    )));
    let event = tokio::time::timeout(timeout, sync)
        .await
        .map_err(|_| {
            format!(
                "timed out after {timeout:?} fetching onboarding document {document_for_error:?} from bootstrap peer {bootstrap_peer}"
            )
        })?;

    match event {
        Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsReconciled {
            ..
        })) => Ok(()),
        Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error { error, .. })) => {
            Err(error.into())
        }
        Event::Net(NetEvent::Error(error)) => Err(format!("{error:?}").into()),
        other => Err(format!("unexpected document sync result: {other:?}").into()),
    }
}

/// Persisted onboarding secret, encrypted at rest under the node key.
#[derive(Serialize, Deserialize)]
struct SealedOnboardingSecret {
    nonce: [u8; 24],
    ciphertext: Vec<u8>,
}

fn onboarding_secret_box(net_secret_key: &[u8; 32]) -> SalsaBox {
    let secret = BoxSecretKey::from(*net_secret_key);
    let public = secret.public_key();
    SalsaBox::new(&public, &secret)
}

pub async fn ensure_initial_local_onboarding_secret(
    driver_ctx: &DriverContext,
    seed_url: String,
    net_secret_key: &[u8; 32],
    realm_id: aruna_core::structs::RealmId,
) -> Result<OnboardingSecret, Box<dyn std::error::Error>> {
    if let Some(sealed) = load_persisted_state::<SealedOnboardingSecret>(
        driver_ctx,
        INITIAL_LOCAL_ONBOARDING_SECRET_KEY,
    )
    .await
    {
        let nonce = crypto_box::Nonce::from(sealed.nonce);
        let plaintext = onboarding_secret_box(net_secret_key)
            .decrypt(&nonce, sealed.ciphertext.as_ref())
            .map_err(|_| "failed to decrypt persisted onboarding secret")?;
        return Ok(postcard::from_bytes(&plaintext)?);
    }

    let mut secret_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut secret_bytes);
    let onboarding_secret = OnboardingSecret {
        seed_url,
        enrollment_id: ulid::Ulid::generate(),
        secret: secret_bytes,
        mode: OnboardingMode::Local,
        realm_id,
        purpose: OnboardingPurpose::InitialAdministrator,
    };
    let record = aruna_core::onboarding::OnboardingSecretRecord {
        enrollment_id: onboarding_secret.enrollment_id,
        secret_hash: onboarding_secret.secret_hash(),
        mode: OnboardingMode::Local,
        purpose: OnboardingPurpose::InitialAdministrator,
        expires_at: u64::MAX,
        claimed_node_id: None,
    };

    drive(
        CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput { record }),
        driver_ctx,
    )
    .await?;

    let plaintext = postcard::to_allocvec(&onboarding_secret)?;
    let nonce = SalsaBox::generate_nonce(&mut CryptoOsRng);
    let ciphertext = onboarding_secret_box(net_secret_key)
        .encrypt(&nonce, plaintext.as_ref())
        .map_err(|_| "failed to encrypt onboarding secret")?;
    let mut nonce_bytes = [0u8; 24];
    nonce_bytes.copy_from_slice(nonce.as_slice());
    persist_state(
        driver_ctx,
        INITIAL_LOCAL_ONBOARDING_SECRET_KEY,
        &SealedOnboardingSecret {
            nonce: nonce_bytes,
            ciphertext,
        },
    )
    .await;
    Ok(onboarding_secret)
}

#[cfg(test)]
mod tests {
    use super::{
        node_is_ready, prepare_core_documents, publish_core_documents, sync_topic_from_peer,
        unique_user_topic, watch_target_needed,
    };
    use aruna_core::NodeId;
    use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{NOTIFICATION_WATCH_INTEREST_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::structs::{
        Actor, NodePlacementEntry, PlacementRef, RealmConfigDocument, RealmId, RealmNodeKind,
        WatchEventKind, WatchEventMask, WatchInterestDigest, WatchInterestEntry,
        watch_interest_dirty_key, watch_interest_node_key,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::document_sync_outbox::read_outbox_records;
    use aruna_operations::driver::DriverContext;
    use aruna_operations::incoming::initialize_net_incoming;
    use aruna_operations::notifications::watch::interest::publish_watch_interest;
    use aruna_operations::task_incoming::OutboxDrainer;
    use aruna_storage::FjallStorage;
    use byteview::ByteView;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn user_topics_deduplicate() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let first = DocumentSyncTarget::User {
            user_id: aruna_core::UserId::local(ulid::Ulid::from_bytes([2u8; 16]), realm_id),
        };
        let second = DocumentSyncTarget::User {
            user_id: aruna_core::UserId::local(ulid::Ulid::from_bytes([3u8; 16]), realm_id),
        };
        let first_shard = aruna_core::structs::PlacementRef {
            strategy_id: ulid::Ulid::from_bytes([4u8; 16]),
            shard: 7,
        };
        let second_shard = aruna_core::structs::PlacementRef {
            shard: 8,
            ..first_shard
        };
        let mut synced_topics = std::collections::HashSet::new();

        assert!(unique_user_topic(&mut synced_topics, realm_id, &first_shard, &first).is_some());
        assert_eq!(
            unique_user_topic(&mut synced_topics, realm_id, &first_shard, &second),
            None
        );
        assert!(unique_user_topic(&mut synced_topics, realm_id, &second_shard, &second).is_some());
        assert_eq!(synced_topics.len(), 2);
    }

    #[test]
    fn watch_target_cases() {
        assert!(watch_target_needed(true, true, false));
        assert!(watch_target_needed(true, false, true));
        assert!(!watch_target_needed(false, true, true));
        assert!(watch_target_needed(false, true, false));
        assert!(!watch_target_needed(false, false, true));
        assert!(!watch_target_needed(false, false, false));
    }

    fn context(storage_handle: aruna_storage::StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn context_net(
        storage_handle: aruna_storage::StorageHandle,
        net_handle: NetHandle,
    ) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    async fn net_context(
        realm_id: RealmId,
        seed: u8,
    ) -> (tempfile::TempDir, DriverContext, NetHandle) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                secret_key: Some(iroh::SecretKey::from_bytes(&[seed; 32])),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .unwrap();
        let context = context_net(storage, net.clone());
        (dir, context, net)
    }

    async fn write_digest(
        context: &DriverContext,
        realm_id: RealmId,
        node_id: NodeId,
        digest: &WatchInterestDigest,
    ) {
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                    key: watch_interest_node_key(realm_id, node_id).into(),
                    value: ByteView::from(digest.to_bytes().unwrap()),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn read_marker(context: &DriverContext, realm_id: RealmId) -> Option<ByteView> {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                key: watch_interest_dirty_key(realm_id).into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("unexpected marker read result: {other:?}"),
        }
    }

    async fn write_config(context: &DriverContext, realm_id: RealmId, node_id: NodeId) {
        write_config_nodes(context, realm_id, node_id, &[node_id]).await;
    }

    async fn write_config_nodes(
        context: &DriverContext,
        realm_id: RealmId,
        node_id: NodeId,
        nodes: &[NodeId],
    ) {
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        for node in nodes {
            config.ensure_node(*node, RealmNodeKind::Server);
        }
        config.seed_default_placement();
        config.placement_map = nodes
            .iter()
            .map(|node| NodePlacementEntry {
                node_id: *node,
                location: String::new(),
                weight: 100,
                full: false,
                draining: false,
                labels: Default::default(),
            })
            .collect();
        let actor = Actor {
            node_id,
            user_id: aruna_core::UserId::nil(realm_id),
            realm_id,
        };
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: REALM_CONFIG_KEYSPACE.to_string(),
                    key: realm_id.as_bytes().to_vec().into(),
                    value: ByteView::from(config.to_bytes(&actor).unwrap()),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn repair_topic(
        context: &Arc<DriverContext>,
        node_id: NodeId,
        realm_id: RealmId,
        target: &DocumentSyncTarget,
    ) {
        for _ in 0..2 {
            let targets = prepare_core_documents(context, node_id, realm_id, true, false)
                .await
                .unwrap();
            assert!(targets.contains(target));
        }
        publish_core_documents(context, node_id, realm_id, true, vec![target.clone()])
            .await
            .unwrap();
        OutboxDrainer::new(context.clone()).run_once().await;
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        assert!(
            context
                .net_handle
                .as_ref()
                .unwrap()
                .document_sync_topic_exists(topic)
                .unwrap()
        );
    }

    async fn seed_topic(
        context: &Arc<DriverContext>,
        net: &NetHandle,
        realm_id: RealmId,
        node_id: NodeId,
        peer_id: NodeId,
    ) -> ::irokle::TopicId {
        let target = DocumentSyncTarget::WatchInterest { realm_id, node_id };
        prepare_core_documents(context, node_id, realm_id, true, false)
            .await
            .unwrap();
        publish_core_documents(context, node_id, realm_id, true, vec![target.clone()])
            .await
            .unwrap();
        OutboxDrainer::new(context.clone()).run_once().await;
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        assert!(net.document_sync_topic_exists(topic).unwrap());
        assert_eq!(net.realm_peers().await, vec![peer_id]);
        net.reconcile_document_sync_topics(vec![topic])
            .await
            .unwrap();
        topic
    }

    #[tokio::test]
    async fn first_boot_watch() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let (_dir, context, net) = net_context(realm_id, 7).await;
        let node_id = net.node_id();
        write_config(&context, realm_id, node_id).await;
        let targets = prepare_core_documents(&context, node_id, realm_id, true, false)
            .await
            .unwrap();
        let target = DocumentSyncTarget::WatchInterest { realm_id, node_id };

        assert!(targets.contains(&target));
        publish_core_documents(&context, node_id, realm_id, true, vec![target.clone()])
            .await
            .unwrap();
        let batch = read_outbox_records(&context.storage_handle, &[], None, 8)
            .await
            .unwrap();
        assert_eq!(batch.records.len(), 1);
        assert!(batch.records[0].1.allow_genesis);
        OutboxDrainer::new(Arc::new(context)).run_once().await;
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        assert!(net.document_sync_topic_exists(topic).unwrap());
        net.shutdown().await;
    }

    #[tokio::test]
    async fn missing_topic_repair() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let (_dir, context, net) = net_context(realm_id, 8).await;
        let node_id = net.node_id();
        write_config(&context, realm_id, node_id).await;
        write_digest(
            &context,
            realm_id,
            node_id,
            &WatchInterestDigest {
                node_id,
                entries: Vec::new(),
            },
        )
        .await;
        let target = DocumentSyncTarget::WatchInterest { realm_id, node_id };
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        assert!(!net.document_sync_topic_exists(topic).unwrap());

        let targets = prepare_core_documents(&context, node_id, realm_id, true, false)
            .await
            .unwrap();

        assert!(targets.contains(&target));
        publish_core_documents(&context, node_id, realm_id, true, vec![target.clone()])
            .await
            .unwrap();
        let batch = read_outbox_records(&context.storage_handle, &[], None, 8)
            .await
            .unwrap();
        assert_eq!(batch.records.len(), 1);
        assert!(batch.records[0].1.allow_genesis);
        OutboxDrainer::new(Arc::new(context)).run_once().await;
        assert!(net.document_sync_topic_exists(topic).unwrap());
        net.shutdown().await;
    }

    #[tokio::test]
    async fn restart_stays_quiet() {
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let (_dir, context, net) = net_context(realm_id, 9).await;
        let node_id = net.node_id();
        let target = DocumentSyncTarget::WatchInterest { realm_id, node_id };
        write_config(&context, realm_id, node_id).await;
        write_digest(
            &context,
            realm_id,
            node_id,
            &WatchInterestDigest {
                node_id,
                entries: Vec::new(),
            },
        )
        .await;
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        net.ensure_document_sync_topics(&[topic], Vec::new())
            .unwrap();

        for _ in 0..2 {
            let targets = prepare_core_documents(&context, node_id, realm_id, true, false)
                .await
                .unwrap();
            assert!(!targets.contains(&target));
            assert!(!publish_watch_interest(&context, node_id).await.unwrap());
            assert!(read_marker(&context, realm_id).await.is_none());
            let batch = read_outbox_records(&context.storage_handle, &[], None, 8)
                .await
                .unwrap();
            assert!(batch.records.is_empty());
        }
        net.shutdown().await;
    }

    #[tokio::test]
    async fn joiner_announces_watch() {
        let realm_id = RealmId::from_bytes([10u8; 32]);
        let (_bootstrap_dir, bootstrap_context, bootstrap_net) = net_context(realm_id, 10).await;
        let (_joiner_dir, joiner_context, joiner_net) = net_context(realm_id, 11).await;
        let bootstrap_context = Arc::new(bootstrap_context);
        let joiner_context = Arc::new(joiner_context);
        initialize_net_incoming(bootstrap_context.clone());
        initialize_net_incoming(joiner_context.clone());
        let bootstrap_id = bootstrap_net.node_id();
        let joiner_id = joiner_net.node_id();
        write_config_nodes(
            &bootstrap_context,
            realm_id,
            bootstrap_id,
            &[bootstrap_id, joiner_id],
        )
        .await;
        write_config_nodes(
            &joiner_context,
            realm_id,
            joiner_id,
            &[bootstrap_id, joiner_id],
        )
        .await;
        bootstrap_net.reload_realm_peers().await.unwrap();
        joiner_net.reload_realm_peers().await.unwrap();
        bootstrap_net
            .add_peer_addr(joiner_net.endpoint_addr())
            .await;
        joiner_net
            .add_peer_addr(bootstrap_net.endpoint_addr())
            .await;

        let bootstrap_target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: bootstrap_id,
        };
        let topic = seed_topic(
            &bootstrap_context,
            &bootstrap_net,
            realm_id,
            bootstrap_id,
            joiner_id,
        )
        .await;
        sync_topic_from_peer(
            &joiner_net,
            topic,
            bootstrap_id,
            &bootstrap_target,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
        assert!(joiner_net.document_sync_topic_exists(topic).unwrap());

        let target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: joiner_id,
        };
        let targets = prepare_core_documents(&joiner_context, joiner_id, realm_id, false, false)
            .await
            .unwrap();

        assert!(targets.contains(&target));
        publish_core_documents(
            &joiner_context,
            joiner_id,
            realm_id,
            false,
            vec![target.clone()],
        )
        .await
        .unwrap();
        let batch = read_outbox_records(&joiner_context.storage_handle, &[], None, 8)
            .await
            .unwrap();
        assert_eq!(batch.records.len(), 1);
        assert!(!batch.records[0].1.allow_genesis);
        OutboxDrainer::new(joiner_context.clone()).run_once().await;
        assert!(joiner_net.document_sync_topic_exists(topic).unwrap());
        bootstrap_net.shutdown().await;
        joiner_net.shutdown().await;
    }

    #[tokio::test]
    async fn publication_retries() {
        let realm_id = RealmId::from_bytes([11u8; 32]);
        let (_dir, context, net) = net_context(realm_id, 11).await;
        let context = Arc::new(context);
        let node_id = net.node_id();
        write_config(&context, realm_id, node_id).await;
        let target = DocumentSyncTarget::WatchInterest { realm_id, node_id };
        let targets = prepare_core_documents(&context, node_id, realm_id, true, false)
            .await
            .unwrap();
        assert!(targets.contains(&target));
        assert!(read_marker(&context, realm_id).await.is_some());

        // A restart after the digest write must repair the missing shared topic.
        repair_topic(&context, node_id, realm_id, &target).await;
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        assert!(net.document_sync_topic_exists(topic).unwrap());

        write_digest(
            &context,
            realm_id,
            node_id,
            &WatchInterestDigest {
                node_id,
                entries: vec![WatchInterestEntry {
                    path_prefix: "bucket/".to_string(),
                    event_mask: WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
                }],
            },
        )
        .await;
        assert!(read_marker(&context, realm_id).await.is_some());
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: REALM_CONFIG_KEYSPACE.to_string(),
                    key: realm_id.as_bytes().to_vec().into(),
                    value: ByteView::from(b"corrupt".to_vec()),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        assert!(publish_watch_interest(&context, node_id).await.is_err());
        assert!(read_marker(&context, realm_id).await.is_some());

        write_config(&context, realm_id, node_id).await;
        let restarted = Arc::new(context_net(context.storage_handle.clone(), net.clone()));
        let targets = prepare_core_documents(&restarted, node_id, realm_id, true, false)
            .await
            .unwrap();
        assert!(!targets.contains(&target));
        assert!(read_marker(&restarted, realm_id).await.is_some());
        publish_core_documents(&restarted, node_id, realm_id, false, vec![target.clone()])
            .await
            .unwrap();
        let batch = read_outbox_records(&restarted.storage_handle, &[], None, 8)
            .await
            .unwrap();
        assert_eq!(batch.records.len(), 1);
        assert!(!batch.records[0].1.allow_genesis);
        let DocumentSyncOutboxEvent::Upsert { bytes, .. } = &batch.records[0].1.event else {
            panic!("watch digest publication must enqueue an upsert")
        };
        assert_eq!(
            WatchInterestDigest::from_bytes(bytes)
                .unwrap()
                .entries
                .len(),
            1
        );
        OutboxDrainer::new(restarted.clone()).run_once().await;
        assert!(net.document_sync_topic_exists(topic).unwrap());
        assert!(publish_watch_interest(&restarted, node_id).await.unwrap());
        assert!(read_marker(&restarted, realm_id).await.is_none());

        net.shutdown().await;
    }

    #[test]
    fn readiness_requires_all() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());

        assert!(!node_is_ready(&config, node_id));
        config.ensure_node(node_id, RealmNodeKind::Server);
        assert!(!node_is_ready(&config, node_id));
        config.placement_map.push(NodePlacementEntry {
            node_id,
            location: String::new(),
            weight: 100,
            full: false,
            draining: false,
            labels: Default::default(),
        });
        config.seed_default_placement();
        config
            .placement_handle_ranges
            .push(aruna_core::structs::HandleRange {
                range_id: ulid::Ulid::from_bytes([3; 16]),
                owner: node_id,
                start: aruna_core::structs::FIRST_GRANTABLE_HANDLE,
                end: aruna_core::structs::FIRST_GRANTABLE_HANDLE
                    + aruna_core::structs::HANDLE_RANGE_SIZE,
            });
        // A grant without its JobControl binding is not ready yet.
        assert!(!node_is_ready(&config, node_id));
        config
            .placement_bindings
            .push(aruna_core::structs::PlacementBinding {
                handle: aruna_core::structured_id::PlacementHandle::new(
                    aruna_core::structs::FIRST_GRANTABLE_HANDLE,
                )
                .unwrap(),
                scope: aruna_core::structs::PlacementScope::Realm(realm_id),
                document_class: aruna_core::structs::DocumentClass::JobControl,
                strategy_id: config.default_strategy_id.unwrap(),
                allocator_range_id: Some(ulid::Ulid::from_bytes([3; 16])),
                allocated_by: Some(node_id),
                allocated_at_ms: Some(1),
            });
        assert!(node_is_ready(&config, node_id));
    }

    async fn read_digest(
        context: &DriverContext,
        realm_id: RealmId,
        node_id: aruna_core::NodeId,
    ) -> WatchInterestDigest {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                key: watch_interest_node_key(realm_id, node_id).into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => WatchInterestDigest::from_bytes(&bytes).unwrap(),
            other => panic!("unexpected digest read result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn initial_watch_digest() {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let (_dir, context, net) = net_context(realm_id, 4).await;
        let node_id = net.node_id();

        let targets = prepare_core_documents(&context, node_id, realm_id, true, true)
            .await
            .unwrap();

        assert!(targets.contains(&DocumentSyncTarget::WatchInterest { realm_id, node_id }));
        assert_eq!(
            read_digest(&context, realm_id, node_id).await,
            WatchInterestDigest {
                node_id,
                entries: Vec::new(),
            }
        );
        net.shutdown().await;
    }

    #[tokio::test]
    async fn existing_watch_digest() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = context(storage);
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let digest = WatchInterestDigest {
            node_id,
            entries: vec![WatchInterestEntry {
                path_prefix: "bucket/".to_string(),
                event_mask: WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            }],
        };
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                key: watch_interest_node_key(realm_id, node_id).into(),
                value: ByteView::from(digest.to_bytes().unwrap()),
                txn_id: None,
            })
            .await;

        prepare_core_documents(&context, node_id, realm_id, false, true)
            .await
            .unwrap();

        assert_eq!(read_digest(&context, realm_id, node_id).await, digest);
    }
}
