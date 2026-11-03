//! Computes the realm-wide totals a realm publishes: data bytes, objects and datasets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::federation::RealmTotals;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::usage::UsageCounters;
use aruna_core::time::unix_timestamp_secs;

use crate::driver::DriverContext;
use crate::metadata::stats::count_realm_documents;
use crate::node::usage_stats::{RealmUsageScope, load_realm_usage};

/// The realm's totals from the realm usage sum and the live document count. Fails when one
/// part is unknown, so a caller shows nothing instead of a zero.
pub async fn realm_totals(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
) -> Result<RealmTotals, String> {
    let datasets = count_realm_documents(context, realm_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or("no metadata subsystem for the dataset count")?;
    let (objects, data_bytes) = data_totals(context, node_id).await?;
    Ok(RealmTotals {
        realm_id,
        data_bytes,
        objects,
        datasets,
        observed_at: unix_timestamp_secs(),
    })
}

/// The realm's current objects and data bytes, stored and referenced alike, private data
/// included, from the realm usage sum.
pub async fn data_totals(context: &DriverContext, node_id: NodeId) -> Result<(u64, u64), String> {
    let usage = load_realm_usage(context, node_id, RealmUsageScope::Global).await?;
    Ok(data_of(&usage.counters))
}

/// Objects and stored plus referenced bytes; both count each object once.
fn data_of(counters: &UsageCounters) -> (u64, u64) {
    let data_bytes = counters
        .logical_bytes
        .saturating_add(counters.referenced_bytes);
    (counters.objects, data_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn data_counted_once() {
        // Physical copies (`stored_bytes`) never enter the data total.
        let counters = UsageCounters {
            objects: 3,
            stored_bytes: 900,
            logical_bytes: 100,
            referenced_bytes: 20,
            ..UsageCounters::default()
        };
        assert_eq!(data_of(&counters), (3, 120));
    }

    #[tokio::test]
    async fn unknown_count_fails() {
        // Without a metadata subsystem the dataset count is unknown, never zero.
        let dir = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let result = realm_totals(&context, RealmId::from_bytes([7; 32]), node).await;
        assert_eq!(
            result.unwrap_err(),
            "no metadata subsystem for the dataset count"
        );
    }
}
