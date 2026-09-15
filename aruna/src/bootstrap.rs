//! Realm bootstrap duties after identity resolution: core-document preparation,
//! publication, and fetch, placement waits, and the local onboarding secret.
//! Persisted identity and enrollment live in `crate::identity`.

use crate::identity::PersistedNodeState;
use aruna_api::server_state::{
    INITIAL_LOCAL_ONBOARDING_SECRET_KEY, load_persisted_state, persist_state,
};
use aruna_core::document::{DocumentNetEvent, DocumentTarget};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE, USER_KEYSPACE};
use aruna_core::onboarding::{
    OnboardingMode, OnboardingPurpose, OnboardingSecret, OnboardingTicket,
};
use aruna_core::{DocumentEffect, NodeId, UserId};
use aruna_operations::device::realm_documents::fetch_from_peers;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::notifications::watch::interest::{
    ensure_interest_digest, mark_interest_dirty,
};
use aruna_operations::onboarding::create_secret::{CreateSecretInput, CreateSecretOperation};
use aruna_operations::placement::target_placement_ref;
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::sync::replicate_documents::{
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
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const ONBOARDING_PLACEMENT_RETRY_INTERVAL: Duration = Duration::from_millis(100);
/// Ceiling for the doubling retry delay, so a peer that is not ready yet is
/// asked patiently instead of ten times a second for the whole budget.
const ONBOARDING_PLACEMENT_RETRY_MAX: Duration = Duration::from_secs(5);

/// Longest a device waits on one answer, so a hung peer costs one attempt
/// rather than the whole onboarding budget.
const DEVICE_FETCH_BUDGET: Duration = Duration::from_secs(10);

/// Delay before retry `attempt`, doubling from the base interval up to the cap.
fn backoff(attempt: u32) -> Duration {
    ONBOARDING_PLACEMENT_RETRY_INTERVAL
        .saturating_mul(2u32.saturating_pow(attempt.min(16)))
        .min(ONBOARDING_PLACEMENT_RETRY_MAX)
}

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
    documents: Vec<DocumentTarget>,
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
) -> Result<Vec<DocumentTarget>, Box<dyn std::error::Error>> {
    let digest_created = ensure_interest_digest(&driver_ctx.storage_handle, realm_id, node_id)
        .await
        .map_err(|error| format!("failed to initialize local watch interest digest: {error}"))?;
    if digest_created {
        mark_interest_dirty(driver_ctx, realm_id)
            .await
            .map_err(|error| format!("failed to mark local watch interest dirty: {error}"))?;
    }

    let mut documents = vec![
        DocumentTarget::RealmAuthorization { realm_id },
        DocumentTarget::RealmConfig { realm_id },
    ];
    if include_node_info {
        // Initial and joining nodes publish before timers run. Provisioned restarts restore
        // shared topics and leave publication to the timers.
        documents.push(DocumentTarget::NodeUsage {
            realm_id,
            node_id,
            group_id: None,
        });
        documents.push(DocumentTarget::NodeInfo { realm_id, node_id });
    }
    let watch_target = DocumentTarget::WatchInterest { realm_id, node_id };
    let topic_exists = if allow_genesis {
        let net_handle = driver_ctx
            .net_handle
            .as_ref()
            .ok_or("net handle unavailable while checking watch interest genesis")?;
        net_handle
            .sync_topic_exists(
                watch_target.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL),
            )
            .map_err(|error| format!("failed to check watch interest topic: {error}"))?
    } else {
        true
    };
    // New digests publish once. The authority also repairs a topic lost during first boot
    // without republishing during a healthy restart.
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
                    .map(|user_id| DocumentTarget::User { user_id })
            }));
            Ok(documents)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(Box::new(error)),
        other => Err(format!("unexpected user iter result: {other:?}").into()),
    }
}

/// Fetches the documents a joining node needs before it serves its realm.
/// Infrastructure syncs the ticket's topics from the bootstrap peer; a device is
/// refused that protocol, so it reads the realm documents over metadata instead.
pub async fn fetch_core_documents(
    driver_ctx: &Arc<DriverContext>,
    node_state: &PersistedNodeState,
    realm_id: &aruna_core::structs::RealmId,
    bootstrap_peer: Option<NodeId>,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_peer = bootstrap_peer.ok_or("missing bootstrap peer")?;
    // Devices cannot use infrastructure sync, so they route realm reads through the peer.
    // Their user document arrives later by the same route because they hold no shard topic.
    if let Some(owner) = node_state.identity.owner() {
        return fetch_device_documents(driver_ctx, owner, bootstrap_peer, timeout).await;
    }
    let onboarding_sync_ticket = node_state
        .onboarding_sync_ticket
        .as_deref()
        .ok_or("missing onboarding sync ticket")?;
    let onboarding_sync_ticket = OnboardingTicket::decode(onboarding_sync_ticket)?;
    let Some(net_handle) = driver_ctx.net_handle.as_ref() else {
        return Err("net handle unavailable".into());
    };
    let realm_id = *realm_id;

    // Shared documents include the config needed to route user documents to shard topics.
    let mut user_documents = Vec::new();
    for document in onboarding_sync_ticket.payload.documents.clone() {
        if matches!(document, DocumentTarget::User { .. }) {
            user_documents.push(document);
            continue;
        }
        let topic = document.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL);
        sync_with_retry(net_handle, topic, bootstrap_peer, &document, timeout).await?;
    }

    if !user_documents.is_empty() {
        let config = load_realm_config(driver_ctx, realm_id).await;
        let mut synced_topics = HashSet::new();
        for document in user_documents {
            let placement = match config.as_ref() {
                Some(config) => target_placement_ref(config, &document, Default::default()),
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
            // The peer serves shard topics only to members. Placement expansion admits the
            // joiner after any transition already in flight.
            if let Err(error) =
                sync_peer_topic(net_handle, topic, bootstrap_peer, &document, timeout).await
            {
                warn!(error = %error, document = ?document, "Leaving an onboarding user document to placement sync");
            }
        }
    }

    Ok(())
}

/// Asks the bootstrap peer for the realm documents until it serves them. The
/// realm-config update that admits the device may land after the enrollment
/// answer, and until it does the peer refuses the read as unauthorized.
async fn fetch_device_documents(
    driver_ctx: &Arc<DriverContext>,
    owner: UserId,
    bootstrap_peer: NodeId,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + timeout;
    let budget = timeout.min(DEVICE_FETCH_BUDGET);
    let mut attempt = 0;
    loop {
        if fetch_from_peers(driver_ctx, owner, vec![bootstrap_peer], budget).await {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out after {timeout:?} fetching the realm documents for this device from bootstrap peer {bootstrap_peer}"
            )
            .into());
        }
        warn!(peer = %bootstrap_peer, "Retrying the device realm document fetch");
        tokio::time::sleep(backoff(attempt)).await;
        attempt = attempt.saturating_add(1);
    }
}

/// Waits until the realm configuration names this node as ready, re-reading it
/// from the bootstrap peer between checks: over document sync, or as a routed
/// read when `device_owner` says this node is a device.
pub async fn wait_for_placement(
    driver_ctx: &Arc<DriverContext>,
    realm_id: aruna_core::structs::RealmId,
    node_id: NodeId,
    device_owner: Option<UserId>,
    bootstrap_peer: Option<NodeId>,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_peer = bootstrap_peer.ok_or("missing bootstrap peer")?;
    let target = DocumentTarget::RealmConfig { realm_id };

    tokio::time::timeout(timeout, async {
        let mut attempt = 0;
        loop {
            let config = drive(GetConfigOperation::new(realm_id), driver_ctx).await?;
            if node_is_ready(&config, node_id) {
                info!(
                    realm_id = %realm_id,
                    node_id = %node_id,
                    kind = kind_label(&config, node_id),
                    "Onboarding complete: the node joined its realm"
                );
                return Ok::<(), Box<dyn std::error::Error>>(());
            }

            match device_owner {
                // A device has no membership in the realm-config topic, so it
                // re-reads the documents the same way it first fetched them.
                Some(owner) => {
                    let budget = timeout.min(DEVICE_FETCH_BUDGET);
                    if !fetch_from_peers(driver_ctx, owner, vec![bootstrap_peer], budget).await {
                        warn!(peer = %bootstrap_peer, "Retrying the device realm document fetch");
                    }
                }
                None => {
                    if let Err(error) = sync_peer_topic(
                        driver_ctx
                            .net_handle
                            .as_ref()
                            .ok_or("net handle unavailable")?,
                        target.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL),
                        bootstrap_peer,
                        &target,
                        timeout,
                    )
                    .await
                    {
                        warn!(error = %error, "Retrying onboarding placement sync");
                    }
                }
            }
            tokio::time::sleep(backoff(attempt)).await;
            attempt = attempt.saturating_add(1);
        }
    })
    .await
    .map_err(|_| {
        format!("timed out after {timeout:?} waiting for onboarding placement for node {node_id}")
    })?
}

/// Node-kind label for the completion log; the owner of a device stays out of it.
fn kind_label(config: &aruna_core::structs::RealmConfigDocument, node_id: NodeId) -> &'static str {
    let node_id = node_id.to_string();
    match config
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .map(|node| &node.kind)
    {
        Some(aruna_core::structs::RealmNodeKind::Management) => "management",
        Some(aruna_core::structs::RealmNodeKind::Server) => "server",
        Some(aruna_core::structs::RealmNodeKind::User { .. }) => "user",
        None => "unknown",
    }
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
    document: &DocumentTarget,
) -> Option<::irokle::TopicId> {
    let topic = document.sync_topic_id(realm_id, placement);
    synced_topics.insert(topic).then_some(topic)
}

async fn sync_peer_topic(
    net_handle: &aruna_net::NetHandle,
    topic: ::irokle::TopicId,
    bootstrap_peer: NodeId,
    document_for_error: &DocumentTarget,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let document_for_error = document_for_error.clone();
    let sync = net_handle.send_effect(Effect::Net(NetEffect::DocumentSync(
        DocumentEffect::SyncDocument {
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
        Event::Net(NetEvent::DocumentSync(DocumentNetEvent::DocumentsReconciled { .. })) => Ok(()),
        Event::Net(NetEvent::DocumentSync(DocumentNetEvent::Error { error, .. })) => {
            Err(error.into())
        }
        Event::Net(NetEvent::Error(error)) => Err(format!("{error:?}").into()),
        other => Err(format!("unexpected document sync result: {other:?}").into()),
    }
}

/// The bootstrap peer may refuse a fresh joiner until the config update that
/// admits it reaches the peer, so failures retry within the same time budget.
async fn sync_with_retry(
    net_handle: &aruna_net::NetHandle,
    topic: ::irokle::TopicId,
    bootstrap_peer: NodeId,
    document: &DocumentTarget,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempt = 0;
    loop {
        match sync_peer_topic(net_handle, topic, bootstrap_peer, document, timeout).await {
            Ok(()) => return Ok(()),
            Err(error) if tokio::time::Instant::now() < deadline => {
                warn!(error = %error, document = ?document, "Retrying onboarding document sync");
                tokio::time::sleep(backoff(attempt)).await;
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

/// Persisted onboarding secret, encrypted at rest under the node key.
#[derive(Serialize, Deserialize)]
struct EncryptedOnboardingSecret {
    nonce: [u8; 24],
    ciphertext: Vec<u8>,
}

fn onboarding_secret_box(net_secret_key: &[u8; 32]) -> SalsaBox {
    let secret = BoxSecretKey::from(*net_secret_key);
    let public = secret.public_key();
    SalsaBox::new(&public, &secret)
}

pub async fn ensure_onboarding_secret(
    driver_ctx: &DriverContext,
    seed_url: String,
    net_secret_key: &[u8; 32],
    realm_id: aruna_core::structs::RealmId,
) -> Result<OnboardingSecret, Box<dyn std::error::Error>> {
    if let Some(encrypted) = load_persisted_state::<EncryptedOnboardingSecret>(
        driver_ctx,
        INITIAL_LOCAL_ONBOARDING_SECRET_KEY,
    )
    .await
    {
        let nonce = crypto_box::Nonce::from(encrypted.nonce);
        let plaintext = onboarding_secret_box(net_secret_key)
            .decrypt(&nonce, encrypted.ciphertext.as_ref())
            .map_err(|_| "failed to decrypt persisted onboarding secret")?;
        return Ok(postcard::from_bytes(&plaintext)?);
    }

    let mut secret_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut secret_bytes);
    let onboarding_secret = OnboardingSecret {
        seed_url,
        enrollment_id: ulid::Ulid::generate(),
        secret: secret_bytes,
        mode: OnboardingMode::Server,
        realm_id,
        purpose: OnboardingPurpose::InitialAdministrator,
    };
    let record = aruna_core::onboarding::OnboardingSecretRecord {
        enrollment_id: onboarding_secret.enrollment_id,
        secret_hash: onboarding_secret.secret_hash(),
        mode: OnboardingMode::Server,
        purpose: OnboardingPurpose::InitialAdministrator,
        expires_at: u64::MAX,
        claimed_node_id: None,
    };

    drive(
        CreateSecretOperation::new(CreateSecretInput { record }),
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
        &EncryptedOnboardingSecret {
            nonce: nonce_bytes,
            ciphertext,
        },
    )
    .await;
    Ok(onboarding_secret)
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
