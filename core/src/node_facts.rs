//! Holds what only this node's handles and host know for its info document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use crate::NodeId;
use crate::compute::offer::ExecutorOffer;
use crate::download_slots::DownloadSlots;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::node_info::CLASS_LABEL_PREFIX;
use crate::structs::storage::routing::BackendCatalog;

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

/// Derived from the node's own registry, so a class disappears from the labels as soon as the
/// operator stops offering it.
pub fn class_labels(catalog: &BackendCatalog) -> BTreeMap<String, String> {
    catalog
        .classes()
        .into_iter()
        .map(|class| (format!("{CLASS_LABEL_PREFIX}{class}"), "true".to_string()))
        .collect()
}

/// Load average per core scaled to permille and clamped to `0..=1000`. Zero
/// cores yields `0` instead of dividing by zero.
pub fn permille_of(load1: f64, cores: u64) -> u32 {
    if cores == 0 {
        return 0;
    }
    (load1 / cores as f64 * 1000.0).round().clamp(0.0, 1000.0) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_become_labels() {
        // Operator-only classes are advertised too: placement is operator domain.
        let catalog = BackendCatalog::new("hot".to_string())
            .with_backend("hot".to_string(), Some("hot".to_string()))
            .with_reserved("tape".to_string(), Some("archive".to_string()))
            .with_backend("plain".to_string(), None);

        let labels = class_labels(&catalog);

        assert_eq!(labels.len(), 2);
        assert_eq!(
            labels.get(&format!("{CLASS_LABEL_PREFIX}hot")),
            Some(&"true".to_string())
        );
        assert!(labels.contains_key(&format!("{CLASS_LABEL_PREFIX}archive")));
    }

    #[test]
    fn permille_exact_values() {
        assert_eq!(permille_of(1.0, 4), 250);
        assert_eq!(permille_of(3.0, 4), 750);
        assert_eq!(permille_of(0.0, 8), 0);
        assert_eq!(permille_of(4.0, 4), 1000);
    }

    #[test]
    fn permille_clamps_high() {
        assert_eq!(permille_of(9.0, 4), 1000);
        assert_eq!(permille_of(10.0, 2), 1000);
    }

    #[test]
    fn permille_zero_cores() {
        assert_eq!(permille_of(5.0, 0), 0);
    }
}
