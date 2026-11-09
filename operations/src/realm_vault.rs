//! Reads realm vault secrets: management nodes open them, other nodes fetch them once and cache
//! the current revision in their node vault. Management nodes also keep notification settings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::SecretBytes;
use aruna_core::document::{DocumentChange, DocumentChangeKind, DocumentTarget};
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    NODE_VAULT_KEYSPACE, SYNC_REVISION_KEYSPACE, TENTATIVE_SETTINGS_KEYSPACE,
};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::realm_vault::{
    FetchScope, RealmSealKey, RealmSecretRef, RealmVaultError, RealmVaultKey, RealmVaultRecord,
};
use aruna_core::storage_entries::{sync_revision_entry, sync_revision_key};
use aruna_core::structs::execution::setting::{
    SettingChannel, SettingHead, TentativeSetting, current_setting, tentative_key, validate_setting,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::types::{Key, TxnId, Value};
use aruna_core::{NodeId, UserId};
use aruna_storage::StorageHandle;
use std::future::Future;
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::MetadataReadError;
use crate::node_vault::{delete_secret, parse_secret, read_secret, write_secret};
use crate::notifications::settings::{
    SettingAnswer, SettingWinner, StoreAnswer, confirmed_settings, tentative_settings,
};
use crate::realm::get_config::GetConfigOperation;
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
    let key = metadata.and_then(|metadata| metadata.realm_vault_key());
    secret_from(ctx, key.as_deref(), config, reference, move || async move {
        let metadata = metadata.ok_or_else(|| {
            RealmSecretError::Unavailable("metadata transport unavailable".to_string())
        })?;
        fetch_secret(metadata, config, reference).await
    })
    .await
}

/// Opens locally with the realm vault key, else reads the cache, else fetches and caches.
async fn secret_from<F, R>(
    ctx: &DriverContext,
    key: Option<&RealmVaultKey>,
    config: &RealmConfigDocument,
    reference: RealmSecretRef,
    fetch: F,
) -> Result<SecretBytes, RealmSecretError>
where
    F: FnOnce() -> R,
    R: Future<Output = Result<SecretBytes, RealmSecretError>>,
{
    let record = current_record(config, reference).ok_or(RealmSecretError::NotReplicated)?;
    if let Some(key) = key {
        return Ok(key.open(&config.realm_id, record)?);
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
        other => Err(RealmSecretError::Cache(format!("{other:?}"))),
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
    key: Option<&RealmVaultKey>,
    peer: NodeId,
    request: RealmSecretRef,
) -> Result<SecretBytes, MetadataReadError> {
    let key = key.ok_or(MetadataReadError::Unavailable)?;
    let net_handle = ctx
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let config = drive(GetConfigOperation::new(*net_handle.realm_id()), ctx)
        .await
        .map_err(|_| MetadataReadError::Unavailable)?;
    let record = check_fetch(&config, net_handle.node_id(), peer, request)?;
    let secret = key
        .open(&config.realm_id, record)
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
    let scope = request.entry.purpose.fetch_scope();
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

/// The management answer to a setting read: the highest held version and the confirmed winner.
pub(crate) async fn serve_setting(
    ctx: &DriverContext,
    key: Option<&RealmSealKey>,
    peer: NodeId,
    owner: UserId,
    channel: SettingChannel,
) -> Result<SettingAnswer, MetadataReadError> {
    let key = key.ok_or(MetadataReadError::Unavailable)?;
    let config = management_config(ctx).await?;
    infrastructure_peer(&config, peer)?;
    read_setting(&ctx.storage_handle, key, &config, owner, channel).await
}

pub(crate) async fn read_setting(
    storage: &StorageHandle,
    key: &RealmSealKey,
    config: &RealmConfigDocument,
    owner: UserId,
    channel: SettingChannel,
) -> Result<SettingAnswer, MetadataReadError> {
    let confirmed = confirmed_settings(storage, channel, &owner, None).await;
    let tentative = tentative_settings(storage, channel, &owner, None).await;
    let (confirmed, tentative) = (unavailable(confirmed)?, unavailable(tentative)?);
    let versions = (confirmed.iter().map(|(head, _)| head.version))
        .chain(tentative.iter().map(|(head, _)| head.version));
    let max_version = versions.max().unwrap_or(0);
    let Some((head, bytes)) = current_setting(confirmed, |(head, _)| *head) else {
        return Ok(SettingAnswer {
            max_version,
            winner: None,
        });
    };
    let secret = match channel
        .sealed(&bytes)
        .map_err(|_| MetadataReadError::Unavailable)?
    {
        Some(sealed) => Some(
            key.open(&config.realm_id, channel, &owner, head.id, &sealed)
                .map_err(|_| MetadataReadError::Unavailable)?,
        ),
        None => None,
    };
    Ok(SettingAnswer {
        max_version,
        winner: Some(SettingWinner { bytes, secret }),
    })
}

/// Phase one of a write on a management node: keeps the document tentative.
pub(crate) async fn serve_store(
    ctx: &DriverContext,
    peer: NodeId,
    target: DocumentTarget,
    bytes: Vec<u8>,
    change: DocumentChange,
) -> Result<StoreAnswer, MetadataReadError> {
    let config = management_config(ctx).await?;
    store_setting(&ctx.storage_handle, &config, peer, target, bytes, change).await
}

/// Admits a store like document sync admits an upsert, then stores it tentative when its
/// version is above every version held. A known id counts as stored only when identical.
pub(crate) async fn store_setting(
    storage: &StorageHandle,
    config: &RealmConfigDocument,
    peer: NodeId,
    target: DocumentTarget,
    bytes: Vec<u8>,
    change: DocumentChange,
) -> Result<StoreAnswer, MetadataReadError> {
    infrastructure_peer(config, peer)?;
    let upsert = change.kind == DocumentChangeKind::Upsert && change.current.generation == 1;
    if change.current.actor != peer || !upsert {
        return Err(MetadataReadError::Forbidden);
    }
    let head = validate_setting(&target, &bytes).map_err(|_| MetadataReadError::Forbidden)?;
    let (channel, owner, id) =
        SettingChannel::of_target(&target).ok_or(MetadataReadError::Forbidden)?;
    for _ in 0..2 {
        let txn_id = start_txn(storage).await?;
        let confirmed =
            unavailable(confirmed_settings(storage, channel, &owner, Some(txn_id)).await);
        let tentative =
            unavailable(tentative_settings(storage, channel, &owner, Some(txn_id)).await);
        let (confirmed, tentative) = match (confirmed, tentative) {
            (Ok(confirmed), Ok(tentative)) => (confirmed, tentative),
            (Err(error), _) | (_, Err(error)) => return abort_with(storage, txn_id, error).await,
        };
        let existing = match confirmed.iter().any(|(held, _)| held.id == id) {
            true => {
                let revision = read_revision(storage, &target, txn_id).await;
                let same = (confirmed.iter()).any(|(held, row)| held.id == id && *row == bytes);
                Some(same && revision == Some(change))
            }
            false => (tentative.iter().find(|(held, _)| held.id == id))
                .map(|(_, held)| held.bytes == bytes && held.change == change),
        };
        let heads: Vec<_> = (confirmed.iter().map(|(held, _)| *held))
            .chain(tentative.iter().map(|(held, _)| *held))
            .collect();
        let answer = decide_store(&heads, existing, &head);
        let mut writes = Vec::new();
        if answer == StoreDecision::Write {
            let row = TentativeSetting {
                bytes: bytes.clone(),
                change,
            };
            let Ok(value) = row.to_bytes() else {
                return abort_with(storage, txn_id, MetadataReadError::Unavailable).await;
            };
            let key = tentative_key(channel, &owner, id);
            writes.push((
                TENTATIVE_SETTINGS_KEYSPACE.to_string(),
                key.into(),
                value.into(),
            ));
        }
        // A document that arrived through sync is confirmed; its tentative copy is dropped.
        let confirmed_ids: Vec<_> = confirmed.iter().map(|(held, _)| held.id).collect();
        let deletes = (tentative.iter())
            .filter(|(held, _)| confirmed_ids.contains(&held.id))
            .map(|(held, _)| {
                let key = tentative_key(channel, &owner, held.id);
                (TENTATIVE_SETTINGS_KEYSPACE.to_string(), key.into())
            })
            .collect();
        match commit(storage, txn_id, deletes, writes).await {
            Ok(()) => return Ok(answer.into()),
            Err(Retry::Conflict) => continue,
            Err(Retry::Failed) => return Err(MetadataReadError::Unavailable),
        }
    }
    Err(MetadataReadError::Unavailable)
}

/// What a management node does with a stored document.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreDecision {
    Write,
    Stored,
    Conflict { max_version: u64 },
}

impl From<StoreDecision> for StoreAnswer {
    fn from(decision: StoreDecision) -> Self {
        match decision {
            StoreDecision::Write | StoreDecision::Stored => Self::Stored,
            StoreDecision::Conflict { max_version } => Self::Conflict { max_version },
        }
    }
}

/// `existing` says whether a document with the incoming id is held and identical.
pub(crate) fn decide_store(
    held: &[SettingHead],
    existing: Option<bool>,
    incoming: &SettingHead,
) -> StoreDecision {
    let max_version = held.iter().map(|head| head.version).max().unwrap_or(0);
    match existing {
        Some(true) => StoreDecision::Stored,
        None if incoming.version > max_version => StoreDecision::Write,
        Some(false) | None => StoreDecision::Conflict { max_version },
    }
}

/// Phase two of a write on a management node: moves the tentative document into the document
/// keyspace.
pub(crate) async fn serve_confirm(
    ctx: &DriverContext,
    peer: NodeId,
    target: DocumentTarget,
) -> Result<(), MetadataReadError> {
    let config = management_config(ctx).await?;
    confirm_setting(&ctx.storage_handle, &config, peer, target).await
}

/// Only the actor of the stored document confirms it; a repeat on a confirmed id succeeds.
pub(crate) async fn confirm_setting(
    storage: &StorageHandle,
    config: &RealmConfigDocument,
    peer: NodeId,
    target: DocumentTarget,
) -> Result<(), MetadataReadError> {
    infrastructure_peer(config, peer)?;
    let (channel, owner, id) =
        SettingChannel::of_target(&target).ok_or(MetadataReadError::Forbidden)?;
    let tentative_row = tentative_key(channel, &owner, id);
    for _ in 0..2 {
        let txn_id = start_txn(storage).await?;
        let reads = vec![
            (target.storage_keyspace().to_string(), target.storage_key()),
            (
                TENTATIVE_SETTINGS_KEYSPACE.to_string(),
                tentative_row.clone().into(),
            ),
        ];
        let read = StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        };
        let (confirmed, tentative) = match storage.send_storage_effect(read).await {
            Event::Storage(StorageEvent::BatchReadResult { values }) => match values.as_slice() {
                [(_, confirmed), (_, tentative)] => (confirmed.is_some(), tentative.clone()),
                _ => return abort_with(storage, txn_id, MetadataReadError::Unavailable).await,
            },
            _ => return abort_with(storage, txn_id, MetadataReadError::Unavailable).await,
        };
        let tentative = tentative
            .map(|row| TentativeSetting::from_bytes(&row))
            .transpose();
        let Ok(tentative) = tentative else {
            return abort_with(storage, txn_id, MetadataReadError::Unavailable).await;
        };
        let delete = vec![(
            TENTATIVE_SETTINGS_KEYSPACE.to_string(),
            tentative_row.clone().into(),
        )];
        let (deletes, writes) = match (confirmed, tentative) {
            (true, held) => (if held.is_some() { delete } else { Vec::new() }, Vec::new()),
            (false, Some(held)) if held.change.current.actor == peer => {
                let row = (
                    target.storage_keyspace().to_string(),
                    target.storage_key(),
                    held.bytes.clone().into(),
                );
                let revision = sync_revision_entry(&target, &held.change);
                let Ok(revision) = revision else {
                    return abort_with(storage, txn_id, MetadataReadError::Unavailable).await;
                };
                (delete, vec![row, revision])
            }
            (false, Some(_)) => {
                return abort_with(storage, txn_id, MetadataReadError::Forbidden).await;
            }
            (false, None) => return abort_with(storage, txn_id, MetadataReadError::NotFound).await,
        };
        match commit(storage, txn_id, deletes, writes).await {
            Ok(()) => return Ok(()),
            Err(Retry::Conflict) => continue,
            Err(Retry::Failed) => return Err(MetadataReadError::Unavailable),
        }
    }
    Err(MetadataReadError::Unavailable)
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

/// Server and management nodes may read and write settings; user devices never.
fn infrastructure_peer(
    config: &RealmConfigDocument,
    peer: NodeId,
) -> Result<(), MetadataReadError> {
    match node_kind(config, peer) {
        Some(kind) if FetchScope::Infrastructure.allows(&kind) => Ok(()),
        _ => Err(MetadataReadError::Forbidden),
    }
}

fn unavailable<T>(result: Result<T, String>) -> Result<T, MetadataReadError> {
    result.map_err(|error| {
        warn!(%error, "Reading notification settings failed");
        MetadataReadError::Unavailable
    })
}

async fn read_revision(
    storage: &StorageHandle,
    target: &DocumentTarget,
    txn_id: TxnId,
) -> Option<DocumentChange> {
    let read = StorageEffect::Read {
        key_space: SYNC_REVISION_KEYSPACE.to_string(),
        key: sync_revision_key(target),
        txn_id: Some(txn_id),
    };
    match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => postcard::from_bytes(&value).ok(),
        _ => None,
    }
}

enum Retry {
    Conflict,
    Failed,
}

async fn start_txn(storage: &StorageHandle) -> Result<TxnId, MetadataReadError> {
    match (storage)
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        _ => Err(MetadataReadError::Unavailable),
    }
}

async fn abort_with<T>(
    storage: &StorageHandle,
    txn_id: TxnId,
    error: MetadataReadError,
) -> Result<T, MetadataReadError> {
    let _ = (storage)
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
    Err(error)
}

async fn commit(
    storage: &StorageHandle,
    txn_id: TxnId,
    deletes: Vec<(String, Key)>,
    writes: Vec<(String, Key, Value)>,
) -> Result<(), Retry> {
    let mut effects = Vec::new();
    if !deletes.is_empty() {
        effects.push(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        });
    }
    if !writes.is_empty() {
        effects.push(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        });
    }
    for effect in effects {
        match storage.send_storage_effect(effect).await {
            Event::Storage(StorageEvent::BatchDeleteResult { .. })
            | Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            _ => {
                let _ = abort_with::<()>(storage, txn_id, MetadataReadError::Unavailable).await;
                return Err(Retry::Failed);
            }
        }
    }
    match (storage)
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => Err(Retry::Conflict),
        _ => Err(Retry::Failed),
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
            other => return Err(format!("node vault page: {other:?}")),
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
                other => return Err(format!("node vault delete: {other:?}")),
            }
        }
        match next {
            Some(after) => start = Some(IterStart::After(after)),
            None => return Ok(pruned),
        }
    }
}

fn cached_current(config: &RealmConfigDocument, id: [u8; 16], bytes: &[u8]) -> bool {
    (config.vault.iter()).any(|record| {
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
