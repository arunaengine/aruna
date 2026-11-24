//! The one place that decides whether the applets feature is on for a group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use crate::structs::identity::features::{Feature, resolve};
use crate::structs::identity::realm::RealmConfigDocument;
use crate::types::GroupId;

/// Feature name of applets in the realm's feature settings.
pub const FEATURE: &str = "applets";

/// Whether applets are on for `group_id` under the realm feature policy. Callers with node-level
/// overrides should resolve `Feature::Applets` with their node's disabled feature set.
pub fn feature_on(config: &RealmConfigDocument, group_id: GroupId) -> bool {
    let none = BTreeSet::new();
    resolve(config, &none, Feature::Applets, Some(&group_id)).on
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::structs::identity::features::FeatureMode;
    use crate::structs::identity::realm::RealmId;

    #[test]
    fn off_by_default() {
        let mut config = RealmConfigDocument::default_for_realm(RealmId([1; 32]), Vec::new());
        let group = ulid::Ulid::from_bytes([1; 16]);
        let other = ulid::Ulid::from_bytes([2; 16]);
        assert!(!feature_on(&config, group));
        config.features.insert(
            Feature::Applets,
            FeatureMode::Scoped {
                default: false,
                groups: BTreeMap::from([(group, true)]),
            },
        );
        assert!(feature_on(&config, group));
        assert!(!feature_on(&config, other));
        config
            .features
            .insert(Feature::Applets, FeatureMode::ForcedOff);
        assert!(!feature_on(&config, group));
    }
}
