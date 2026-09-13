//! DHT-signed realm discovery and persisted peer state: announcement
//! publication, endpoint lookup/validation, and the peer authorization list.
//!
//! Publication and lookup are I/O; selection and validation below them are
//! pure decisions over verified records, so the tests can pin them without a
//! live DHT.

use std::sync::Arc;
use std::time::Duration;

use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{DhtGetOptions, StorageEffect};
use aruna_core::events::{DhtEntry, Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keys::realm_endpoint_key;
use aruna_core::structs::{
    NetworkDiagnosticsState, RealmConfigDocument, RealmEndpointAnnouncement, RealmId,
    endpoint_signing_bytes,
};
use aruna_core::time::unix_timestamp_secs;
use aruna_storage::StorageHandle;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::{Endpoint, EndpointAddr, TransportAddr};
use parking_lot::RwLock;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::connection_pool::ConnectionPool;
use crate::dht::{self, DhtHandle};
use crate::error::{NetError, Result};
use crate::unique_peer_nodes;

pub(crate) const DHT_SIGNED_MAX_CLOCK_SKEW_SECS: u64 = 300;
// The DHT sub-budget of OPEN_STREAM_TIMEOUT, enforced inside the driver so the
// lookup is released rather than left running when the caller stops waiting.
pub(crate) const DHT_SIGNED_LOOKUP_TIMEOUT: Duration = Duration::from_secs(4);

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_signed_publisher(
    dht: Arc<DhtHandle>,
    endpoint: Endpoint,
    realm_id: RealmId,
    configured_relay_urls: Vec<String>,
    ttl: Duration,
    refresh_after: Duration,
    diagnostics: Arc<Mutex<NetworkDiagnosticsState>>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (ttl, refresh_after) = normalize_signed_timing(ttl, refresh_after);
        let mut sequence = 0u64;

        loop {
            sequence = sequence.saturating_add(1);
            match publish_endpoint_announcement(
                &dht,
                &endpoint,
                realm_id,
                &configured_relay_urls,
                ttl,
                sequence,
            )
            .await
            {
                Ok(()) => {
                    diagnostics.lock().await.requests.record_success();
                    debug!(
                        realm_id = %realm_id,
                        node_id = %endpoint.id(),
                        ttl_secs = ttl.as_secs(),
                        sequence,
                        "Published DHT-signed realm endpoint announcement"
                    );
                }
                Err(err) => {
                    diagnostics
                        .lock()
                        .await
                        .requests
                        .record_failure(format!("publish DHT-signed endpoint: {err}"));
                    warn!(
                        realm_id = %realm_id,
                        node_id = %endpoint.id(),
                        error = %err,
                        "Failed to publish DHT-signed realm endpoint announcement"
                    );
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(refresh_after) => {}
            }
        }
    })
}

fn normalize_signed_timing(ttl: Duration, refresh_after: Duration) -> (Duration, Duration) {
    let ttl_secs = ttl.as_secs().clamp(1, dht::constants::MAX_TTL_SECS);
    let mut refresh_secs = refresh_after.as_secs().max(1);
    if refresh_secs >= ttl_secs && ttl_secs > 1 {
        refresh_secs = (ttl_secs / 2).max(1);
    }

    (
        Duration::from_secs(ttl_secs),
        Duration::from_secs(refresh_secs),
    )
}

async fn publish_endpoint_announcement(
    dht: &DhtHandle,
    endpoint: &Endpoint,
    realm_id: RealmId,
    configured_relay_urls: &[String],
    ttl: Duration,
    sequence: u64,
) -> Result<()> {
    let ttl_secs = ttl.as_secs().max(1);
    let issued_at = unix_timestamp_secs();
    let expires_at = issued_at.saturating_add(ttl_secs);
    let node_id = endpoint.id();
    let endpoint_addr = local_endpoint_addr(endpoint, configured_relay_urls);
    if endpoint_addr.addrs.is_empty() {
        return Err(NetError::Dht(
            "refusing to publish DHT-signed endpoint announcement without dialable addresses"
                .to_string(),
        ));
    }
    let signing_bytes = endpoint_signing_bytes(
        &realm_id,
        &node_id,
        &endpoint_addr,
        issued_at,
        expires_at,
        sequence,
    )
    .map_err(|err| NetError::Dht(format!("encode endpoint announcement signing bytes: {err}")))?;
    let signature = endpoint.secret_key().sign(&signing_bytes);
    let announcement = RealmEndpointAnnouncement {
        realm_id,
        node_id,
        endpoint_addr,
        issued_at,
        expires_at,
        sequence,
        signature,
    };
    let value = postcard::to_allocvec(&announcement)
        .map_err(|err| NetError::Dht(format!("encode endpoint announcement: {err}")))?;
    let key = realm_endpoint_key(&realm_id, &node_id);

    let stats = dht
        .put(&key, realm_id, value, Duration::from_secs(ttl_secs))
        .await?;
    if stats.remote_store_count == 0 {
        return Err(NetError::Dht(
            "DHT announcement was stored locally but not by a remote peer".to_string(),
        ));
    }
    Ok(())
}

async fn lookup_signed_endpoint(
    dht: &DhtHandle,
    realm_id: RealmId,
    authorized_nodes: &[NodeId],
    peer: NodeId,
    config: Option<(Duration, Duration)>,
) -> Result<Option<EndpointAddr>> {
    let Some((ttl, _)) = config else {
        return Ok(None);
    };
    if !authorized_nodes.contains(&peer) {
        return Ok(None);
    }

    let key = realm_endpoint_key(&realm_id, &peer);
    // An announcement is usable only from the publisher in this realm, so the
    // driver stops at the first valid record; the sub-budget keeps the resolve bounded.
    let entries = dht
        .get(
            &key,
            Some(realm_id),
            DhtGetOptions::first_usable(DHT_SIGNED_LOOKUP_TIMEOUT, realm_id, peer),
        )
        .await?;
    let now = unix_timestamp_secs();

    Ok(select_signed_endpoint(
        entries,
        peer,
        realm_id,
        authorized_nodes,
        now,
        ttl.as_secs().max(1),
    ))
}

pub(crate) fn select_signed_endpoint(
    entries: Vec<DhtEntry>,
    peer: NodeId,
    realm_id: RealmId,
    authorized_nodes: &[NodeId],
    now: u64,
    max_ttl_secs: u64,
) -> Option<EndpointAddr> {
    let mut best = None::<RealmEndpointAnnouncement>;

    for entry in entries {
        let Ok(announcement) = postcard::from_bytes::<RealmEndpointAnnouncement>(&entry.value)
        else {
            continue;
        };
        if validate_endpoint_announcement(
            &announcement,
            entry.node_id,
            peer,
            realm_id,
            authorized_nodes,
            now,
            max_ttl_secs,
        )
        .is_err()
        {
            continue;
        }

        let replace = best.as_ref().is_none_or(|current| {
            announcement.issued_at > current.issued_at
                || (announcement.issued_at == current.issued_at
                    && announcement.sequence > current.sequence)
                || (announcement.issued_at == current.issued_at
                    && announcement.sequence == current.sequence
                    && announcement.expires_at > current.expires_at)
        });
        if replace {
            best = Some(announcement);
        }
    }

    best.map(|announcement| announcement.endpoint_addr)
}

pub(crate) async fn resolve_signed_endpoint(
    dht: &DhtHandle,
    realm_id: RealmId,
    authorized_nodes: &[NodeId],
    peer: NodeId,
    config: Option<(Duration, Duration)>,
    diagnostics: &Arc<Mutex<NetworkDiagnosticsState>>,
) -> Result<Option<EndpointAddr>> {
    if !signed_lookup_enabled(config, authorized_nodes, peer) {
        return Ok(None);
    }

    let result = lookup_signed_endpoint(dht, realm_id, authorized_nodes, peer, config).await;
    match &result {
        Ok(_) => diagnostics.lock().await.requests.record_success(),
        Err(err) => {
            diagnostics
                .lock()
                .await
                .requests
                .record_failure(format!("resolve DHT-signed endpoint for {peer}: {err}"));
        }
    }

    result
}

fn signed_lookup_enabled(
    config: Option<(Duration, Duration)>,
    authorized_nodes: &[NodeId],
    peer: NodeId,
) -> bool {
    config.is_some() && authorized_nodes.contains(&peer)
}

pub(crate) fn validate_endpoint_announcement(
    announcement: &RealmEndpointAnnouncement,
    entry_publisher: NodeId,
    requested_peer: NodeId,
    realm_id: RealmId,
    authorized_nodes: &[NodeId],
    now: u64,
    max_ttl_secs: u64,
) -> std::result::Result<(), String> {
    if !authorized_nodes.contains(&requested_peer) {
        return Err("node is not authorized for realm discovery".to_string());
    }
    if announcement.realm_id != realm_id {
        return Err("announcement realm does not match lookup realm".to_string());
    }
    if announcement.node_id != requested_peer {
        return Err("announcement node does not match requested peer".to_string());
    }
    if entry_publisher != requested_peer {
        return Err("DHT entry publisher does not match announcement node".to_string());
    }
    if announcement.endpoint_addr.id != requested_peer {
        return Err("announcement endpoint id does not match requested peer".to_string());
    }
    if announcement.endpoint_addr.addrs.is_empty() {
        return Err("announcement endpoint address has no dialable addresses".to_string());
    }
    if announcement.issued_at > announcement.expires_at || announcement.expires_at <= now {
        return Err("announcement is expired or has invalid timestamps".to_string());
    }
    if announcement.issued_at > now.saturating_add(DHT_SIGNED_MAX_CLOCK_SKEW_SECS) {
        return Err("announcement is issued too far in the future".to_string());
    }
    if announcement
        .expires_at
        .saturating_sub(announcement.issued_at)
        > max_ttl_secs
    {
        return Err("announcement ttl exceeds configured maximum".to_string());
    }

    let signing_bytes = endpoint_signing_bytes(
        &announcement.realm_id,
        &announcement.node_id,
        &announcement.endpoint_addr,
        announcement.issued_at,
        announcement.expires_at,
        announcement.sequence,
    )
    .map_err(|err| err.to_string())?;
    announcement
        .node_id
        .verify(&signing_bytes, &announcement.signature)
        .map_err(|err| err.to_string())
}

pub(crate) async fn install_signed_endpoint(
    address_lookup: &MemoryLookup,
    dht: &DhtHandle,
    connection_pool: &ConnectionPool,
    endpoint_addr: EndpointAddr,
) {
    let node_id = endpoint_addr.id;
    address_lookup.set_endpoint_info(endpoint_addr);
    // The newly validated address invalidates the failures recorded against the
    // previous one, so the next request dials instead of failing fast.
    if let Err(error) = connection_pool.clear_failures(node_id).await {
        debug!(node_id = %node_id, %error, "Connection pool stopped during signed address installation");
    }
    if let Err(err) = dht.add_peer(node_id) {
        debug!(
            node_id = %node_id,
            error = %err,
            "Failed to add DHT-signed endpoint peer"
        );
    }
}

pub(crate) fn local_endpoint_addr(
    endpoint: &Endpoint,
    configured_relay_urls: &[String],
) -> EndpointAddr {
    let observed = endpoint.addr();
    let mut addrs = Vec::new();

    for transport in observed.addrs {
        push_transport_addr(&mut addrs, transport);
    }
    for relay in configured_relay_urls {
        if let Ok(relay) = relay.parse::<iroh::RelayUrl>() {
            push_transport_addr(&mut addrs, TransportAddr::Relay(relay));
        }
    }
    for socket in endpoint.bound_sockets() {
        if !socket.ip().is_unspecified() {
            push_transport_addr(&mut addrs, TransportAddr::Ip(socket));
        }
    }

    EndpointAddr::from_parts(endpoint.id(), addrs)
}

fn push_transport_addr(addrs: &mut Vec<TransportAddr>, addr: TransportAddr) {
    if matches!(&addr, TransportAddr::Ip(socket) if socket.ip().is_unspecified()) {
        return;
    }
    if !addrs.iter().any(|existing| existing == &addr) {
        addrs.push(addr);
    }
}

pub(crate) async fn read_persisted_peers(
    storage: &StorageHandle,
    realm_id: RealmId,
    local_id: NodeId,
) -> Result<Option<Vec<NodeId>>> {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                let document = RealmConfigDocument::from_bytes(&bytes)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                if document.realm_id != realm_id {
                    return Err(NetError::Bootstrap(format!(
                        "realm config {} does not match net realm {}",
                        document.realm_id, realm_id
                    )));
                }
                let nodes = document
                    .node_ids()
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                Ok(unique_peer_nodes(nodes, local_id))
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(NetError::Bootstrap(error.to_string()))
        }
        other => Err(NetError::Bootstrap(format!(
            "unexpected storage event while reading realm config: {other:?}"
        ))),
    }
}

pub(crate) fn authorize_signed_node(
    authorized_nodes: &Arc<RwLock<Vec<NodeId>>>,
    node_id: NodeId,
    local_id: NodeId,
) {
    if node_id == local_id {
        return;
    }

    let mut nodes = authorized_nodes.write();
    if nodes.contains(&node_id) {
        return;
    }
    nodes.push(node_id);
    nodes.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
}

pub(crate) fn replace_authorized_nodes(
    authorized_nodes: &Arc<RwLock<Vec<NodeId>>>,
    nodes: &[NodeId],
    local_id: NodeId,
) {
    *authorized_nodes.write() = unique_peer_nodes(nodes.to_vec(), local_id);
}
