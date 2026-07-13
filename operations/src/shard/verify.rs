use std::collections::BTreeSet;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::document::shard_topic_id;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::SHARD_VERIFICATION_KEYSPACE;
use aruna_core::structs::{PlacementRef, RealmConfigDocument, RealmId};
use aruna_core::types::Key;
use aruna_core::util::unix_timestamp_millis;
use aruna_net::NetHandle;
use byteview::ByteView;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::driver::DriverContext;
use crate::placement::resolve_shard_holders;
use crate::shard::client::fetch_shard_manifest;
use crate::shard::{assemble_shard_manifest, manifest_entry_digest};
use crate::startup::apply_restored_reconcile;
use crate::sync_placement::{placement_key, placement_prefix};

/// Page size for scanning persisted shard verification markers.
const VERIFIED_SHARD_SCAN_PAGE_SIZE: usize = 256;

/// A new holder retries digest reconciliation this many times against the first
/// reachable co-holder before leaving the shard unverified for the next pass.
pub const SHARD_VERIFICATION_MAX_ATTEMPTS: usize = 3;

/// Limits startup verification work while allowing independent shards to make
/// progress concurrently. Matches the existing bounded metadata fanout.
const SHARD_VERIFICATION_CONCURRENCY_LIMIT: usize = 8;

/// Persisted proof that the local node reconciled a shard against a co-holder.
/// Presence of the row (keyed like a pending placement) means verified; a
/// restart resumes only shards without one. The marker is a one-shot join-time
/// signal, not a continuous consistency guarantee: it is written once on the
/// first successful reconcile and deleted when the node leaves the holder set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardVerificationRecord {
    pub placement: PlacementRef,
    pub verified_against: Option<NodeId>,
    pub digest: [u8; 32],
    pub verified_at_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShardVerificationSummary {
    pub held: usize,
    pub already_verified: usize,
    pub newly_verified: usize,
    pub unverified: usize,
}

/// Reconciles every shard the local node newly holds against a co-holder with
/// bounded concurrency: fetch the first reachable co-holder's manifest in rank
/// order and compare the topic digest plus manifest entry digest. Equal ⇒ mark
/// verified; differing ⇒ one anti-entropy pass against that co-holder and retry,
/// bounded by [`SHARD_VERIFICATION_MAX_ATTEMPTS`]. Already verified shards (a
/// persisted marker) are skipped, so this is idempotent and cheap in steady
/// state and resumes unverified shards after a restart.
pub async fn verify_held_shards(
    context: &Arc<DriverContext>,
    node_id: NodeId,
    realm_id: RealmId,
) -> ShardVerificationSummary {
    let mut summary = ShardVerificationSummary::default();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return summary;
    };
    let Some(net_handle) = context.net_handle.clone() else {
        return summary;
    };

    let held_shards: Vec<_> = config
        .strategies
        .iter()
        .flat_map(|strategy| {
            (0..strategy.shard_count).filter_map(|shard| {
                let placement = PlacementRef {
                    strategy_id: strategy.strategy_id,
                    epoch: 0,
                    shard,
                };
                let holders = resolve_shard_holders(&config, &placement);
                holders.contains(&node_id).then(|| {
                    // Rank order is preserved by `resolve_shard_holders`; keep it so the
                    // first reachable co-holder is the highest-ranked one.
                    let co_holders: Vec<NodeId> = holders
                        .into_iter()
                        .filter(|candidate| *candidate != node_id)
                        .collect();
                    (placement, co_holders)
                })
            })
        })
        .collect();

    let net_handle = &net_handle;
    let pending = stream::iter(held_shards.into_iter().enumerate().map(
        |(shard_index, (placement, co_holders))| async move {
            let mut shard_summary = ShardVerificationSummary {
                held: 1,
                ..ShardVerificationSummary::default()
            };
            if is_shard_verified(context, realm_id, &placement).await {
                shard_summary.already_verified = 1;
            } else if verify_one_shard(
                context,
                net_handle,
                node_id,
                realm_id,
                placement,
                &co_holders,
            )
            .await
            {
                shard_summary.newly_verified = 1;
            } else {
                shard_summary.unverified = 1;
            }

            (shard_index, shard_summary)
        },
    ))
    .buffer_unordered(SHARD_VERIFICATION_CONCURRENCY_LIMIT);
    futures_util::pin_mut!(pending);

    let mut shard_summaries = Vec::new();
    while let Some(shard_summary) = pending.next().await {
        shard_summaries.push(shard_summary);
    }
    shard_summaries.sort_unstable_by_key(|(shard_index, _)| *shard_index);
    for (_, shard_summary) in shard_summaries {
        summary.held += shard_summary.held;
        summary.already_verified += shard_summary.already_verified;
        summary.newly_verified += shard_summary.newly_verified;
        summary.unverified += shard_summary.unverified;
    }
    summary
}

async fn verify_one_shard(
    context: &Arc<DriverContext>,
    net_handle: &NetHandle,
    node_id: NodeId,
    realm_id: RealmId,
    placement: PlacementRef,
    co_holders: &[NodeId],
) -> bool {
    let topic = shard_topic_id(realm_id, &placement);

    // A sole holder is trivially consistent with itself, but only once its
    // genesis exists. A genesis-less topic still reports the (non-zero) empty
    // fingerprint, so gate on the local topic actually existing — never on the
    // digest value — or a rank-0 create still pending would be marked verified.
    if co_holders.is_empty() {
        if !net_handle
            .document_sync_topic_exists(topic)
            .unwrap_or(false)
        {
            debug!(
                strategy = %placement.strategy_id,
                shard = placement.shard,
                "Sole-holder shard has no local genesis yet; deferring verification"
            );
            return false;
        }
        let digest = match assemble_shard_manifest(context, realm_id, placement).await {
            Ok(manifest) => manifest.digest,
            Err(error) => {
                warn!(error = %error, "Failed to assemble sole-holder shard manifest for verification");
                return false;
            }
        };
        mark_verified(context, realm_id, placement, None, digest).await;
        info!(
            strategy = %placement.strategy_id,
            shard = placement.shard,
            "Verified sole-holder shard",
        );
        return true;
    }

    for co_holder in co_holders {
        let mut remote =
            match fetch_shard_manifest(net_handle, *co_holder, realm_id, placement).await {
                Ok(manifest) => manifest,
                Err(error) => {
                    debug!(
                        co_holder = %co_holder,
                        error = %error,
                        "Shard manifest fetch failed; trying next co-holder"
                    );
                    continue;
                }
            };

        // First reachable co-holder: reconcile against it with a bounded number
        // of anti-entropy passes.
        for _ in 0..SHARD_VERIFICATION_MAX_ATTEMPTS {
            let local = match assemble_shard_manifest(context, realm_id, placement).await {
                Ok(manifest) => manifest,
                Err(error) => {
                    warn!(error = %error, "Failed to assemble local shard manifest for verification");
                    return false;
                }
            };
            // Never certify convergence without a local genesis: two genesis-less
            // holders share the (non-zero) empty fingerprint and would otherwise
            // match, so require the local topic to exist before comparing.
            if net_handle
                .document_sync_topic_exists(topic)
                .unwrap_or(false)
                && manifests_converged(&local, &remote)
            {
                mark_verified(context, realm_id, placement, Some(*co_holder), local.digest).await;
                info!(
                    strategy = %placement.strategy_id,
                    shard = placement.shard,
                    co_holder = %co_holder,
                    entries = local.entries.len(),
                    "Verified shard against co-holder",
                );
                return true;
            }
            // Topic or entry digests differ: pull the co-holder's events and re-compare.
            let event = net_handle
                .sync_document_topics(vec![topic], vec![*co_holder])
                .await;
            apply_restored_reconcile(context, node_id, event).await;
            remote = match fetch_shard_manifest(net_handle, *co_holder, realm_id, placement).await {
                Ok(manifest) => manifest,
                Err(error) => {
                    debug!(co_holder = %co_holder, error = %error, "Re-fetch of shard manifest failed");
                    break;
                }
            };
        }
        // The first reachable co-holder did not converge within the retry
        // budget; leave the shard unverified for the next pass.
        return false;
    }
    false
}

fn manifests_converged(
    local: &aruna_core::document::ShardManifest,
    remote: &aruna_core::document::ShardManifest,
) -> bool {
    local.digest == remote.digest
        && manifest_entry_digest(&local.entries) == manifest_entry_digest(&remote.entries)
}

/// Deletes a shard's verification marker (e.g. when the local node leaves the
/// shard's holder set), so a later re-entry re-verifies from scratch.
pub async fn delete_shard_verification(
    context: &DriverContext,
    realm_id: RealmId,
    placement: &PlacementRef,
) {
    if let Event::Storage(StorageEvent::Error { error }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Delete {
            key_space: SHARD_VERIFICATION_KEYSPACE.to_string(),
            key: verification_key(realm_id, placement),
            txn_id: None,
        })
        .await
    {
        warn!(error = %error, "Failed to delete shard verification marker");
    }
}

/// Topic ids of every shard the local node has durably verified in `realm_id`.
///
/// Reconciliation installs a former-holder history cutoff only for these topics:
/// an unverified shard's local clock is not a trustworthy cutover boundary, so
/// its history is left admissible until verification proves the transfer.
pub async fn load_verified_shard_topics(
    context: &DriverContext,
    realm_id: RealmId,
) -> BTreeSet<::irokle::TopicId> {
    let mut topics = BTreeSet::new();
    let mut start_after: Option<Key> = None;
    loop {
        let (values, next) = match context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: SHARD_VERIFICATION_KEYSPACE.to_string(),
                prefix: Some(placement_prefix(realm_id)),
                start: start_after.take().map(IterStart::After),
                limit: VERIFIED_SHARD_SCAN_PAGE_SIZE,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(%realm_id, error = %error, "Failed to scan verified shards");
                return topics;
            }
            other => {
                warn!(%realm_id, event = ?other, "Unexpected verified shard scan result");
                return topics;
            }
        };
        for (_, value) in &values {
            if let Ok(record) = postcard::from_bytes::<ShardVerificationRecord>(value) {
                topics.insert(shard_topic_id(realm_id, &record.placement));
            }
        }
        match next {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
    topics
}

/// Whether the local node has a persisted verification marker for a shard.
pub async fn is_shard_verified(
    context: &DriverContext,
    realm_id: RealmId,
    placement: &PlacementRef,
) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: SHARD_VERIFICATION_KEYSPACE.to_string(),
                key: verification_key(realm_id, placement),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
    )
}

async fn mark_verified(
    context: &DriverContext,
    realm_id: RealmId,
    placement: PlacementRef,
    verified_against: Option<NodeId>,
    digest: [u8; 32],
) {
    let record = ShardVerificationRecord {
        placement,
        verified_against,
        digest,
        verified_at_ms: unix_timestamp_millis(),
    };
    let value = match postcard::to_allocvec(&record) {
        Ok(value) => value,
        Err(error) => {
            warn!(error = %error, "Failed to encode shard verification record");
            return;
        }
    };
    if let Event::Storage(StorageEvent::Error { error }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: SHARD_VERIFICATION_KEYSPACE.to_string(),
            key: verification_key(realm_id, &placement),
            value: ByteView::from(value),
            txn_id: None,
        })
        .await
    {
        warn!(error = %error, "Failed to persist shard verification marker");
    }
}

fn verification_key(realm_id: RealmId, placement: &PlacementRef) -> Key {
    placement_key(realm_id, placement)
}

async fn load_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Option<RealmConfigDocument> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(realm_id.as_bytes().to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            value.and_then(|bytes| RealmConfigDocument::from_bytes(&bytes).ok())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{
        DocumentSyncRevision, DocumentSyncTarget, ShardManifest, ShardManifestEntry,
    };
    use ulid::Ulid;

    fn node_id(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn placement() -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([9; 16]),
            epoch: 0,
            shard: 7,
        }
    }

    fn entry(document: u8, generation: u64) -> ShardManifestEntry {
        ShardManifestEntry {
            target: DocumentSyncTarget::MetadataDocumentLifecycle {
                document_id: Ulid::from_bytes([document; 16]),
            },
            revision: DocumentSyncRevision {
                generation,
                event_id: Ulid::from_bytes([generation as u8; 16]),
                actor: node_id(3),
                updated_at_ms: generation,
            },
        }
    }

    fn manifest(entries: Vec<ShardManifestEntry>) -> ShardManifest {
        ShardManifest {
            placement: placement(),
            holder: node_id(1),
            entries,
            cursor: Vec::new(),
            digest: [7; 32],
            updated_at_ms: 0,
        }
    }

    #[test]
    fn equal_topic_digest_with_different_entries_does_not_converge() {
        let local = manifest(vec![entry(1, 1)]);
        let remote = manifest(vec![entry(2, 1)]);

        assert_eq!(local.digest, remote.digest);
        assert!(!manifests_converged(&local, &remote));
    }
}
