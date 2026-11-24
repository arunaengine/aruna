//! The one place that decides whether the applets feature is on for a group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::structs::identity::realm::RealmConfigDocument;
use crate::types::GroupId;

/// Feature name of applets in the realm's feature settings.
pub const FEATURE: &str = "applets";

/// Whether applets are on for `group_id`: off unless the realm switched the feature on. Feature
/// governance replaces this body; callers never read the realm settings themselves.
pub fn feature_on(config: &RealmConfigDocument, group_id: GroupId) -> bool {
    let _ = group_id;
    config
        .portal_features
        .get(FEATURE)
        .copied()
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::realm::RealmId;

    #[test]
    fn off_by_default() {
        let mut config = RealmConfigDocument::default_for_realm(RealmId([1; 32]), Vec::new());
        let group = ulid::Ulid::nil();
        assert!(!feature_on(&config, group));
        config.portal_features.insert(FEATURE.to_string(), true);
        assert!(feature_on(&config, group));
    }
}
