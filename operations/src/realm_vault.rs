//! Reads realm vault secrets: management nodes open them, other nodes fetch them once and cache
//! the current revision in their node vault. Management nodes also open notification secrets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::compute::SecretBytes;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::NODE_VAULT_KEYSPACE;
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::realm_vault::{
    FetchScope, RealmSealed, RealmSecretRef, RealmVaultError, RealmVaultRecord,
};
use aruna_core::structs::execution::setting::{SettingChannel, validate_setting};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use std::future::Future;
use thiserror::Error;
use tracing::debug;
use ulid::Ulid;

use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::MetadataReadError;
use crate::node_vault::{delete_secret, parse_secret, read_secret, write_secret};
use crate::realm::mutate_placement::{is_management, node_kind};

/// Cache rows read per page while pruning.
const PRUNE_PAGE: usize = 256;

/// Why a secret is not available now. Every case may pass once replication or a peer catches up.
#[derive(Debug, Error)]
pub enum RealmSecretError {
    #[error("the realm vault entry is not replicated to this node yet")]
    NotReplicated,
    #[error("no management node handed out the secret: {0}")]
    Unavailable(String),
    #[error(transparent)]
    Vault(#[from] RealmVaultError),
    #[error("the node vault cache failed: {0}")]
    Cache(String),
}

/// The secret `reference` names, when it is the current revision in `config`.
pub async fn read_realm_secret(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    reference: RealmSecretRef,
) -> Result<SecretBytes, RealmSecretError> {
    let metadata = ctx.metadata_handle.as_ref();
    secret_from(ctx, config, reference, move || async move {
        let metadata = metadata.ok_or_else(|| {
            RealmSecretError::Unavailable("metadata transport unavailable".to_string())
        })?;
        fetch_secret(metadata, config, reference).await
    })
    .await
}

/// Opens with the realm keys this node holds, else reads the cache, else fetches and caches.
async fn secret_from<F, R>(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    reference: RealmSecretRef,
    fetch: F,
) -> Result<SecretBytes, RealmSecretError>
where
    F: FnOnce() -> R,
    R: Future<Output = Result<SecretBytes, RealmSecretError>>,
{
    let record = current_record(config, reference).ok_or(RealmSecretError::NotReplicated)?;
    match open_sealed(ctx, record_sealed(config, record)).await {
        Err(RealmVaultError::NoKey) => {}
        opened => return Ok(opened?),
    }
    let entry = cache_entry(reference);
    let event = ctx
        .storage_handle
        .send_effect(read_secret(entry, None))
        .await;
    let cached = parse_secret(event, entry, |bytes| Ok(cached_secret(bytes, reference)))
        .map_err(|error| RealmSecretError::Cache(error.to_string()))?;
    if let Some(Some(secret)) = cached {
        return Ok(secret);
    }
    let secret = fetch().await?;
    let value = [
        &[reference.entry.purpose.tag()][..],
        &reference.revision.to_bytes(),
        secret.expose(),
    ];
    let write = write_secret(entry, SecretBytes::new(value.concat()), None);
    match ctx.storage_handle.send_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(secret),
        other => Err(RealmSecretError::Cache(other.label())),
    }
}

fn current_record(
    config: &RealmConfigDocument,
    reference: RealmSecretRef,
) -> Option<&RealmVaultRecord> {
    (config.vault.iter())
        .find(|record| record.entry == reference.entry && record.revision == reference.revision)
}

fn cache_entry(reference: RealmSecretRef) -> VaultEntry {
    VaultEntry::new(VaultPurpose::RealmSecret, reference.entry.id)
}

/// A cache row is `purpose tag || revision || secret`; another purpose or revision is a miss.
fn cached_secret(bytes: &[u8], reference: RealmSecretRef) -> Option<SecretBytes> {
    let (tag, rest) = bytes.split_first()?;
    let (revision, secret) = rest.split_first_chunk::<16>()?;
    (*tag == reference.entry.purpose.tag() && Ulid::from_bytes(*revision) == reference.revision)
        .then(|| SecretBytes::new(secret.to_vec()))
}

/// Asks the management nodes in realm config order; the first answer wins.
async fn fetch_secret(
    metadata: &crate::metadata::MetadataHandle,
    config: &RealmConfigDocument,
    reference: RealmSecretRef,
) -> Result<SecretBytes, RealmSecretError> {
    let mut last = "the realm has no management node".to_string();
    let managers = (config.nodes.iter())
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| node.node_id.parse::<NodeId>().ok());
    for node_id in managers {
        match metadata.request_realm_secret(node_id, reference).await {
            Ok(secret) => return Ok(secret),
            Err(error) => last = format!("{node_id}: {error:?}"),
        }
    }
    Err(RealmSecretError::Unavailable(last))
}

/// The management side of a fetch. Only a key holder answers, only listed peers in the
/// purpose's fetch scope get an answer, and only for the current revision.
pub(crate) async fn serve_realm_secret(
    ctx: &DriverContext,
    peer: NodeId,
    request: RealmSecretRef,
) -> Result<SecretBytes, MetadataReadError> {
    let net_handle = ctx
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let config = drive(GetConfigOperation::new(*net_handle.realm_id()), ctx)
        .await
        .map_err(|_| MetadataReadError::Unavailable)?;
    let record = check_fetch(&config, net_handle.node_id(), peer, request)?;
    let secret = open_sealed(ctx, record_sealed(&config, record))
        .await
        .map_err(|_| MetadataReadError::Unavailable)?;
    debug!(%peer, entry = ?request.entry, revision = %request.revision, "Served a realm vault entry");
    Ok(secret)
}

fn check_fetch(
    config: &RealmConfigDocument,
    local: NodeId,
    peer: NodeId,
    request: RealmSecretRef,
) -> Result<&RealmVaultRecord, MetadataReadError> {
    if !is_management(config, local) {
        return Err(MetadataReadError::Unavailable);
    }
    let scope = (request.entry.purpose.fetch_scope()).ok_or(MetadataReadError::Forbidden)?;
    if !node_kind(config, peer).is_some_and(|kind| scope.allows(&kind)) {
        return Err(MetadataReadError::Forbidden);
    }
    let current = (config.vault.iter()).find(|record| record.entry == request.entry);
    match current {
        Some(record) if record.revision == request.revision => Ok(record),
        // An older revision is never handed out; a newer one has not reached this node yet.
        Some(record) if record.revision > request.revision => Err(MetadataReadError::Forbidden),
        _ => Err(MetadataReadError::Unavailable),
    }
}

/// The management side of opening a setting secret for a delivery. Only a key holder answers,
/// only infrastructure peers get an answer, and only for a well-formed setting document.
pub(crate) async fn serve_open(
    ctx: &DriverContext,
    peer: NodeId,
    target: DocumentTarget,
    bytes: Vec<u8>,
) -> Result<SecretBytes, MetadataReadError> {
    let config = management_config(ctx).await?;
    let sealed = sealed_setting(&config, peer, &target, &bytes)?;
    open_sealed(ctx, sealed).await.map_err(setting_error)
}

/// A node without the realm keys cannot answer; a secret that does not open is refused.
fn setting_error(error: RealmVaultError) -> MetadataReadError {
    match error {
        RealmVaultError::NoKey => MetadataReadError::Unavailable,
        _ => MetadataReadError::Forbidden,
    }
}

/// Asks the storage backend, which holds the realm keys on a management node, to open `sealed`.
async fn open_sealed(
    ctx: &DriverContext,
    sealed: RealmSealed,
) -> Result<SecretBytes, RealmVaultError> {
    let effect = StorageEffect::RealmOpen(Box::new(sealed));
    match ctx.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::RealmOpened { result }) => result,
        _ => Err(RealmVaultError::Open),
    }
}

fn record_sealed(config: &RealmConfigDocument, record: &RealmVaultRecord) -> RealmSealed {
    RealmSealed::Record {
        realm_id: config.realm_id,
        record: record.clone(),
    }
}

/// The sealed secret of a valid setting document; an off setting has none.
pub(crate) fn sealed_setting(
    config: &RealmConfigDocument,
    peer: NodeId,
    target: &DocumentTarget,
    bytes: &[u8],
) -> Result<RealmSealed, MetadataReadError> {
    infrastructure_peer(config, peer)?;
    let head = validate_setting(target, bytes).map_err(|_| MetadataReadError::Forbidden)?;
    let (channel, owner, id) =
        SettingChannel::of_target(target).ok_or(MetadataReadError::Forbidden)?;
    let sealed = channel
        .sealed(bytes)
        .map_err(|_| MetadataReadError::Forbidden)?;
    let sealed = sealed
        .filter(|_| head.on)
        .ok_or(MetadataReadError::NotFound)?;
    Ok(RealmSealed::Setting {
        realm_id: config.realm_id,
        channel,
        owner,
        id,
        sealed,
    })
}

/// The realm config of a node that holds the realm key; other nodes keep no settings.
async fn management_config(ctx: &DriverContext) -> Result<RealmConfigDocument, MetadataReadError> {
    let net_handle = ctx
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let config = drive(GetConfigOperation::new(*net_handle.realm_id()), ctx)
        .await
        .map_err(|_| MetadataReadError::Unavailable)?;
    match is_management(&config, net_handle.node_id()) {
        true => Ok(config),
        false => Err(MetadataReadError::Unavailable),
    }
}

/// Server and management nodes may open settings; user devices never.
fn infrastructure_peer(
    config: &RealmConfigDocument,
    peer: NodeId,
) -> Result<(), MetadataReadError> {
    match node_kind(config, peer) {
        Some(kind) if FetchScope::Infrastructure.allows(&kind) => Ok(()),
        _ => Err(MetadataReadError::Forbidden),
    }
}

/// Deletes cached secrets whose entry or revision is no longer current in `config`.
pub async fn prune_secret_cache(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
) -> Result<usize, String> {
    let prefix = vec![VaultPurpose::RealmSecret.tag()];
    let mut start = None;
    let mut pruned = 0;
    loop {
        let page = StorageEffect::Iter {
            key_space: NODE_VAULT_KEYSPACE.to_string(),
            prefix: Some(prefix.clone().into()),
            start: start.take(),
            limit: PRUNE_PAGE,
            txn_id: None,
        };
        let (keys, next) = match ctx.storage_handle.send_storage_effect(page).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values.into_iter().map(|(key, _)| key), next_start_after),
            other => return Err(format!("node vault page: {}", other.label())),
        };
        for key in keys {
            let Some(id) = key.get(1..).and_then(|id| <[u8; 16]>::try_from(id).ok()) else {
                continue;
            };
            let entry = VaultEntry::new(VaultPurpose::RealmSecret, Ulid::from_bytes(id));
            let event = ctx
                .storage_handle
                .send_effect(read_secret(entry, None))
                .await;
            let current = parse_secret(event, entry, |bytes| Ok(cached_current(config, id, bytes)))
                .map_err(|error| error.to_string())?;
            if current == Some(true) {
                continue;
            }
            match ctx
                .storage_handle
                .send_effect(delete_secret(entry, None))
                .await
            {
                Event::Storage(StorageEvent::DeleteResult { .. }) => pruned += 1,
                other => return Err(format!("node vault delete: {}", other.label())),
            }
        }
        match next {
            Some(after) => start = Some(IterStart::After(after)),
            None => return Ok(pruned),
        }
    }
}

fn cached_current(config: &RealmConfigDocument, id: [u8; 16], bytes: &[u8]) -> bool {
    let mut fetchable =
        (config.vault.iter()).filter(|record| record.entry.purpose.fetch_scope().is_some());
    fetchable.any(|record| {
        let reference = RealmSecretRef {
            entry: record.entry,
            revision: record.revision,
        };
        record.entry.id == Ulid::from_bytes(id) && cached_secret(bytes, reference).is_some()
    })
}

#[cfg(test)]
#[path = "realm_vault_tests.rs"]
mod tests;
