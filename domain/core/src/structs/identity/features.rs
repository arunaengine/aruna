//! Realm feature policy: the features an instance can turn off and how one resolves for a group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::structs::identity::realm::RealmConfigDocument;
use crate::types::GroupId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// An Aruna feature a node, a realm or a group can have off. Postcard stores the variant index,
/// so new features go only at the end.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Feature {
    Runs,
    Workflows,
    Notebooks,
    Applets,
    Vault,
    Assistant,
    Plugins,
    Connectors,
    Shares,
    Placement,
    Profiles,
    Repositories,
    S3Keys,
    Watches,
    Federation,
}

/// How the realm sets one feature. Off never deletes data; it refuses new starts and writes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureMode {
    /// The realm default with per-group exceptions.
    Scoped {
        default: bool,
        groups: BTreeMap<GroupId, bool>,
    },
    /// Off in the whole realm; group exceptions do not apply.
    ForcedOff,
}

/// Which layer decided a feature's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Node,
    Realm,
    Group,
    Default,
    Dependency,
}

impl Scope {
    pub fn name(self) -> &'static str {
        match self {
            Scope::Node => "node",
            Scope::Realm => "realm",
            Scope::Group => "group",
            Scope::Default => "default",
            Scope::Dependency => "dependency",
        }
    }
}

/// A feature's state for one node and group, the layer that decided it and, when a feature it
/// depends on is off, that feature.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub on: bool,
    pub scope: Scope,
    pub dependency: Option<Feature>,
}

impl Feature {
    pub const ALL: [Feature; 15] = [
        Feature::Runs,
        Feature::Workflows,
        Feature::Notebooks,
        Feature::Applets,
        Feature::Vault,
        Feature::Assistant,
        Feature::Plugins,
        Feature::Connectors,
        Feature::Shares,
        Feature::Placement,
        Feature::Profiles,
        Feature::Repositories,
        Feature::S3Keys,
        Feature::Watches,
        Feature::Federation,
    ];

    /// The key used in configuration, API bodies and errors.
    pub fn name(self) -> &'static str {
        match self {
            Feature::Runs => "runs",
            Feature::Workflows => "workflows",
            Feature::Notebooks => "notebooks",
            Feature::Applets => "applets",
            Feature::Vault => "vault",
            Feature::Assistant => "assistant",
            Feature::Plugins => "plugins",
            Feature::Connectors => "connectors",
            Feature::Shares => "shares",
            Feature::Placement => "placement",
            Feature::Profiles => "profiles",
            Feature::Repositories => "repositories",
            Feature::S3Keys => "s3-keys",
            Feature::Watches => "watches",
            Feature::Federation => "federation",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|feature| feature.name() == name)
    }

    /// The feature this one needs; derived here, never stored.
    pub fn parent(self) -> Option<Feature> {
        match self {
            Feature::Workflows | Feature::Notebooks => Some(Feature::Runs),
            _ => None,
        }
    }

    /// The state when neither the node nor the realm sets the feature.
    pub fn built_in(self) -> bool {
        !matches!(self, Feature::Applets)
    }
}

/// Resolves `feature` on a node whose operator turned `node_off` off, for `group` or at realm
/// level. A realm without a `runs` entry falls back to the portal default named `compute`.
pub fn resolve(
    config: &RealmConfigDocument,
    node_off: &BTreeSet<Feature>,
    feature: Feature,
    group: Option<&GroupId>,
) -> Resolved {
    let own = own_state(config, node_off, feature, group);
    match feature.parent() {
        Some(parent) if own.on && !resolve(config, node_off, parent, group).on => Resolved {
            on: false,
            scope: Scope::Dependency,
            dependency: Some(parent),
        },
        _ => own,
    }
}

fn own_state(
    config: &RealmConfigDocument,
    node_off: &BTreeSet<Feature>,
    feature: Feature,
    group: Option<&GroupId>,
) -> Resolved {
    let (on, scope) = if node_off.contains(&feature) {
        (false, Scope::Node)
    } else {
        match config.features.get(&feature) {
            Some(FeatureMode::ForcedOff) => (false, Scope::Realm),
            Some(FeatureMode::Scoped { default, groups }) => {
                match group.and_then(|group| groups.get(group)) {
                    Some(on) => (*on, Scope::Group),
                    None => (*default, Scope::Realm),
                }
            }
            None => match (feature, config.portal_features.get("compute")) {
                (Feature::Runs, Some(on)) => (*on, Scope::Realm),
                _ => (feature.built_in(), Scope::Default),
            },
        }
    };
    Resolved {
        on,
        scope,
        dependency: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn config(features: &[(Feature, FeatureMode)]) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([1; 32]), Vec::new(), 3);
        config.features = features.iter().cloned().collect();
        config
    }

    fn scoped(default: bool, groups: &[(GroupId, bool)]) -> FeatureMode {
        FeatureMode::Scoped {
            default,
            groups: groups.iter().copied().collect(),
        }
    }

    fn state(on: bool, scope: Scope) -> Resolved {
        Resolved {
            on,
            scope,
            dependency: None,
        }
    }

    #[test]
    fn names_match_serde() {
        for feature in Feature::ALL {
            let json = serde_json::to_string(&feature).unwrap();
            assert_eq!(json, format!("\"{}\"", feature.name()));
            assert_eq!(Feature::from_name(feature.name()), Some(feature));
        }
        assert_eq!(Feature::from_name("compute"), None);
    }

    #[test]
    fn layers_resolve_ordered() {
        let group = Ulid::from_bytes([2; 16]);
        let other = Ulid::from_bytes([3; 16]);
        let none = BTreeSet::new();
        let shares = config(&[(Feature::Shares, scoped(false, &[(group, true)]))]);
        let resolve_shares = |node_off, group| resolve(&shares, node_off, Feature::Shares, group);
        assert_eq!(
            resolve_shares(&none, Some(&group)),
            state(true, Scope::Group)
        );
        assert_eq!(
            resolve_shares(&none, Some(&other)),
            state(false, Scope::Realm)
        );
        assert_eq!(resolve_shares(&none, None), state(false, Scope::Realm));
        let node_off = BTreeSet::from([Feature::Shares]);
        assert_eq!(
            resolve_shares(&node_off, Some(&group)),
            state(false, Scope::Node)
        );

        let forced = config(&[(Feature::Shares, FeatureMode::ForcedOff)]);
        let forced_state = resolve(&forced, &none, Feature::Shares, Some(&group));
        assert_eq!(forced_state, state(false, Scope::Realm));
    }

    #[test]
    fn missing_uses_defaults() {
        let empty = config(&[]);
        let none = BTreeSet::new();
        let vault = resolve(&empty, &none, Feature::Vault, None);
        assert_eq!(vault, state(true, Scope::Default));
        let applets = resolve(&empty, &none, Feature::Applets, None);
        assert_eq!(applets, state(false, Scope::Default));

        let mut legacy = config(&[]);
        legacy.portal_features.insert("compute".to_string(), false);
        let runs = resolve(&legacy, &none, Feature::Runs, None);
        assert_eq!(runs, state(false, Scope::Realm));
        legacy.features.insert(Feature::Runs, scoped(true, &[]));
        let runs = resolve(&legacy, &none, Feature::Runs, None);
        assert_eq!(runs, state(true, Scope::Realm));
    }

    #[test]
    fn dependency_disables_children() {
        let group = Ulid::from_bytes([2; 16]);
        let runs_off = config(&[(Feature::Runs, scoped(true, &[(group, false)]))]);
        let none = BTreeSet::new();
        let off = Resolved {
            on: false,
            scope: Scope::Dependency,
            dependency: Some(Feature::Runs),
        };
        for child in [Feature::Workflows, Feature::Notebooks] {
            assert_eq!(resolve(&runs_off, &none, child, Some(&group)), off);
            assert!(resolve(&runs_off, &none, child, None).on);
        }
        let node_off = BTreeSet::from([Feature::Runs]);
        let empty = config(&[]);
        assert_eq!(resolve(&empty, &node_off, Feature::Workflows, None), off);
        // A child that is off itself reports its own layer, not the dependency.
        let both = config(&[(Feature::Workflows, FeatureMode::ForcedOff)]);
        let workflows = resolve(&both, &node_off, Feature::Workflows, None);
        assert_eq!(workflows, state(false, Scope::Realm));
    }
}
