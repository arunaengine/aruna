use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::document::shard_topic_id;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::SHARD_VERIFICATION_KEYSPACE;
use aruna_core::structs::{PlacementRef, RealmConfigDocument, RealmId};
use aruna_core::types::Key;
use aruna_core::util::unix_timestamp_millis;
use aruna_net::NetHandle;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::driver::DriverContext;
use crate::placement::resolve_shard_holders;
use crate::shard::assemble_shard_manifest;
use crate::shard::client::fetch_shard_manifest;
use crate::startup::apply_restored_reconcile;
use crate::sync_placement::placement_key;

/// A new holder retries digest reconciliation this many times against the first
/// reachable co-holder before leaving the shard unverified for the next pass.
pub const SHARD_VERIFICATION_MAX_ATTEMPTS: usize = 3;

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

/// Reconciles every shard the local node newly holds against a co-holder: fetch
/// the first reachable co-holder's manifest in rank order and compare digests.
/// Equal ⇒ mark verified; differing ⇒ one anti-entropy pass against that
/// co-holder and retry, bounded by [`SHARD_VERIFICATION_MAX_ATTEMPTS`]. Already
/// verified shards (a persisted marker) are skipped, so this is idempotent and
/// cheap in steady state and resumes unverified shards after a restart.
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

    for strategy in &config.strategies {
        for shard in 0..strategy.shard_count {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                shard,
            };
            let holders = resolve_shard_holders(&config, &placement);
            if !holders.contains(&node_id) {
                continue;
            }
            summary.held += 1;
            if is_shard_verified(context, realm_id, &placement).await {
                summary.already_verified += 1;
                continue;
            }
            // Rank order is preserved by `resolve_shard_holders`; keep it so the
            // first reachable co-holder is the highest-ranked one.
            let co_holders: Vec<NodeId> = holders
                .into_iter()
                .filter(|candidate| *candidate != node_id)
                .collect();
            if verify_one_shard(
                context,
                &net_handle,
                node_id,
                realm_id,
                placement,
                &co_holders,
            )
            .await
            {
                summary.newly_verified += 1;
            } else {
                summary.unverified += 1;
            }
        }
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
                && local.digest == remote.digest
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
            // Digests differ: pull the co-holder's events and re-compare.
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
