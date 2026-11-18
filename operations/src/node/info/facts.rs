//! Reads what only this node's handles and host know for its info document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_compute::registry::ExecutorOffer;
use aruna_core::NodeId;
use aruna_core::download_slots::DownloadSlots;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::node_info::CLASS_LABEL_PREFIX;
use aruna_core::structs::storage::routing::BackendCatalog;
use tracing::warn;

use crate::driver::{DriverContext, node_routing};

/// What only this node's handles and host know, read once before a flow starts.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeFacts {
    /// What each enabled executor backend offers, before a placement subject is applied.
    pub offers: Vec<ExecutorOffer>,
    pub class_labels: BTreeMap<String, String>,
    pub load_permille: Option<u32>,
    pub storage_capacity: Option<u64>,
    pub download_slots: Option<DownloadSlots>,
    /// This node and its realm while a net transport runs.
    pub serving: Option<(NodeId, RealmId)>,
}

impl NodeFacts {
    pub fn gather(ctx: &DriverContext) -> Self {
        Self {
            offers: (ctx.compute_handle.as_ref())
                .map(|registry| registry.offers())
                .unwrap_or_default(),
            class_labels: (ctx.blob_handle.as_ref())
                .map(|blob| class_labels(&blob.routing().catalog))
                .unwrap_or_default(),
            load_permille: read_load_permille(),
            storage_capacity: node_routing(ctx).catalog.capacity(),
            download_slots: (ctx.net_handle.as_ref()).map(|net| net.download_gauge().snapshot()),
            serving: ctx.node_and_realm(),
        }
    }
}

/// Derived from the node's own registry, so a class disappears from the labels as soon as the
/// operator stops offering it.
pub(in crate::node) fn class_labels(catalog: &BackendCatalog) -> BTreeMap<String, String> {
    catalog
        .classes()
        .into_iter()
        .map(|class| (format!("{CLASS_LABEL_PREFIX}{class}"), "true".to_string()))
        .collect()
}

/// 1-minute OS load average scaled to permille of logical core capacity, or
/// `None` (with a warning) when the core count is unavailable.
fn read_load_permille() -> Option<u32> {
    match std::thread::available_parallelism() {
        Ok(cores) => Some(permille_of(
            sysinfo::System::load_average().one,
            cores.get() as u64,
        )),
        Err(error) => {
            warn!(%error, "failed to read logical core count for node info load");
            None
        }
    }
}

/// Load average per core scaled to permille and clamped to `0..=1000`. Zero
/// cores yields `0` instead of dividing by zero.
pub(in crate::node) fn permille_of(load1: f64, cores: u64) -> u32 {
    if cores == 0 {
        return 0;
    }
    (load1 / cores as f64 * 1000.0).round().clamp(0.0, 1000.0) as u32
}
