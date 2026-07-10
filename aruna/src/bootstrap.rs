use crate::config::PersistedNodeState;
use aruna_api::server_state::{
    INITIAL_LOCAL_ONBOARDING_SECRET_KEY, load_persisted_state, persist_state,
};
use aruna_core::document::{DocumentSyncNetEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE, USER_KEYSPACE};
use aruna_core::onboarding::{OnboardingMode, OnboardingSecret, OnboardingSyncTicket};
use aruna_core::{DocumentSyncEffect, NodeId, UserId};
use aruna_operations::create_onboarding_secret::{
    CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::notifications::watch::interest::ensure_local_watch_interest_digest;
use aruna_operations::replicate_documents::{
    ReplicateDocumentsConfig, ReplicateDocumentsOperation,
};
use byteview::ByteView;
use rand::Rng;
use std::time::Duration;
use tracing::warn;

const ONBOARDING_DOCUMENT_SYNC_TIMEOUT: Duration = Duration::from_secs(60);

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

pub async fn announce_core_documents(
    driver_ctx: &DriverContext,
    node_id: NodeId,
    realm_id: &aruna_core::structs::RealmId,
    allow_genesis: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let documents = core_document_targets(driver_ctx, node_id, *realm_id).await?;
    if documents.is_empty() {
        return Ok(());
    }

    let driver_ctx = driver_ctx.clone();
    let realm_id = *realm_id;
    tokio::spawn(async move {
        if let Err(error) = drive(
            ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
                realm_id,
                local_node_id: node_id,
                excluded_peers: Vec::new(),
                documents,
                // Only the realm-bootstrap node may mint shared-topic genesis;
                // joining/provisioned nodes announce with false and join it.
                allow_genesis,
            }),
            &driver_ctx,
        )
        .await
        {
            warn!(error = ?error, "Failed to queue core document replication");
        }
    });

    Ok(())
}

async fn core_document_targets(
    driver_ctx: &DriverContext,
    node_id: NodeId,
    realm_id: aruna_core::structs::RealmId,
) -> Result<Vec<DocumentSyncTarget>, Box<dyn std::error::Error>> {
    ensure_local_watch_interest_digest(&driver_ctx.storage_handle, realm_id, node_id)
        .await
        .map_err(|error| format!("failed to initialize local watch interest digest: {error}"))?;

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
        // Announce the shared realm-scoped watch-interest topic so every realm
        // node subscribes to it and receives all peers' watch-interest digests.
        DocumentSyncTarget::WatchInterest { realm_id, node_id },
    ];

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
    _realm_id: &aruna_core::structs::RealmId,
    bootstrap_peer: Option<NodeId>,
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

    for document in onboarding_sync_ticket.payload.documents.clone() {
        sync_document_from_peer(net_handle, document, bootstrap_peer).await?;
    }

    Ok(())
}

async fn sync_document_from_peer(
    net_handle: &aruna_net::NetHandle,
    document: DocumentSyncTarget,
    bootstrap_peer: NodeId,
) -> Result<(), Box<dyn std::error::Error>> {
    let document_for_error = document.clone();
    let sync = net_handle.send_effect(Effect::Net(NetEffect::DocumentSync(
        DocumentSyncEffect::SyncDocument {
            target: document,
            peers: vec![bootstrap_peer],
        },
    )));
    let event = tokio::time::timeout(ONBOARDING_DOCUMENT_SYNC_TIMEOUT, sync)
        .await
        .map_err(|_| {
            format!(
                "timed out after {:?} fetching onboarding document {:?} from bootstrap peer {}",
                ONBOARDING_DOCUMENT_SYNC_TIMEOUT, document_for_error, bootstrap_peer
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

pub async fn ensure_initial_local_onboarding_secret(
    driver_ctx: &DriverContext,
    seed_url: String,
) -> Result<OnboardingSecret, Box<dyn std::error::Error>> {
    if let Some(secret) =
        load_persisted_state::<OnboardingSecret>(driver_ctx, INITIAL_LOCAL_ONBOARDING_SECRET_KEY)
            .await
    {
        return Ok(secret);
    }

    let mut secret_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut secret_bytes);
    let onboarding_secret = OnboardingSecret {
        seed_url,
        enrollment_id: ulid::Ulid::new(),
        secret: secret_bytes,
        mode: OnboardingMode::Local,
    };
    let record = aruna_core::onboarding::OnboardingSecretRecord {
        enrollment_id: onboarding_secret.enrollment_id,
        secret_hash: onboarding_secret.secret_hash(),
        mode: OnboardingMode::Local,
        expires_at: u64::MAX,
        claimed_node_id: None,
    };

    drive(
        CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput { record }),
        driver_ctx,
    )
    .await?;
    persist_state(
        driver_ctx,
        INITIAL_LOCAL_ONBOARDING_SECRET_KEY,
        &onboarding_secret,
    )
    .await;
    Ok(onboarding_secret)
}

#[cfg(test)]
mod tests {
    use super::core_document_targets;
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::NOTIFICATION_WATCH_INTEREST_KEYSPACE;
    use aruna_core::structs::{
        RealmId, WatchEventKind, WatchEventMask, WatchInterestDigest, WatchInterestEntry,
        watch_interest_node_key,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_storage::FjallStorage;
    use byteview::ByteView;
    use tempfile::tempdir;

    fn context(storage_handle: aruna_storage::StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
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
    async fn core_targets_initialize_empty_local_watch_interest_digest() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = context(storage);
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();

        let targets = core_document_targets(&context, node_id, realm_id)
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
    }

    #[tokio::test]
    async fn core_targets_preserve_existing_local_watch_interest_digest() {
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

        core_document_targets(&context, node_id, realm_id)
            .await
            .unwrap();

        assert_eq!(read_digest(&context, realm_id, node_id).await, digest);
    }
}
