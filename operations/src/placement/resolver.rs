//! Placement view assembly and holder resolution over the weighted
//! two-level rendezvous primitives in [`crate::placement::selector`].

use std::collections::{BTreeMap, HashSet};
use std::str::FromStr;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::structs::{
    AffinityEffect, BindingScope, DEFAULT_LOCATION, DEFAULT_NODE_WEIGHT, DocumentClass,
    KIND_LABEL_KEY, LabelMatch, MetadataRegistryRecord, PlacementOverride, PlacementStrategy,
    RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::types::GroupId;

use crate::placement::selector::{ROLE_LOCATION, ROLE_NODE, rank_weighted};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedNode {
    pub node_id: NodeId,
    pub kind: RealmNodeKind,
    pub location: String,
    pub weight: u32,
    pub full: bool,
    pub draining: bool,
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementView {
    pub nodes: Vec<ResolvedNode>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlacementResolutionContext<'a> {
    pub group_id: Option<GroupId>,
    pub metadata_path: Option<&'a str>,
}

/// Assembles one [`ResolvedNode`] per configured node with a parseable id.
///
/// Placement fields come from the matching `placement_map` entry; unmapped
/// nodes fall back to [`DEFAULT_LOCATION`] / [`DEFAULT_NODE_WEIGHT`] and clear
/// status flags. Labels are the entry's `labels`, overlaid by the derived
/// read-only kind label (which always wins). The label input is the replicated
/// class-1 placement-map entry, never the eventually-consistent `NodeInfo`, so
/// holder sets stay a pure function of the placement map.
pub fn build_view(config: &RealmConfigDocument) -> PlacementView {
    let mut nodes = Vec::with_capacity(config.nodes.len());
    for realm_node in &config.nodes {
        let Ok(node_id) = NodeId::from_str(&realm_node.node_id) else {
            continue;
        };
        let entry = config.placement_entry(node_id);
        let location = entry
            .map(|entry| entry.effective_location().to_string())
            .unwrap_or_else(|| DEFAULT_LOCATION.to_string());
        let weight = entry
            .map(|entry| entry.weight)
            .unwrap_or(DEFAULT_NODE_WEIGHT);
        let full = entry.is_some_and(|entry| entry.full);
        let draining = entry.is_some_and(|entry| entry.draining);

        let mut labels = entry.map(|entry| entry.labels.clone()).unwrap_or_default();
        labels.insert(
            KIND_LABEL_KEY.to_string(),
            kind_label(&realm_node.kind).to_string(),
        );

        nodes.push(ResolvedNode {
            node_id,
            kind: realm_node.kind.clone(),
            location,
            weight,
            full,
            draining,
            labels,
        });
    }
    PlacementView { nodes }
}

/// Resolves holders for `subject` in rank order (downstream retry order).
///
/// Available pinned nodes lead (bypassing affinity filters), then the weighted
/// two-level walk fills up to `strategy.replica_count` from the eligible nodes;
/// `None` takes every eligible node.
pub fn resolve_holders(
    view: &PlacementView,
    strategy: &PlacementStrategy,
    subject: &[u8],
    override_: Option<&PlacementOverride>,
) -> Vec<NodeId> {
    let target = strategy.replica_count.map(|count| count as usize);
    let reached = |holders: &[NodeId]| target.is_some_and(|target| holders.len() >= target);

    let excluded: HashSet<NodeId> = override_
        .map(|over| over.excluded.iter().copied().collect())
        .unwrap_or_default();

    let mut result: Vec<NodeId> = Vec::new();
    let mut used_nodes: HashSet<NodeId> = HashSet::new();
    let mut seen_locations: HashSet<String> = HashSet::new();

    if let Some(over) = override_ {
        for pin in &over.pinned {
            if reached(&result) {
                return result;
            }
            if used_nodes.contains(pin) {
                continue;
            }
            // Exclusion wins over a pin: a node listed in both is never selected.
            if excluded.contains(pin) {
                continue;
            }
            let Some(node) = view.nodes.iter().find(|node| node.node_id == *pin) else {
                continue;
            };
            if !node.kind.is_sync_eligible() || !is_available(node, strategy) {
                continue;
            }
            result.push(*pin);
            used_nodes.insert(*pin);
            seen_locations.insert(node.location.clone());
        }
    }

    let ranked = ranked_locations(view, strategy, subject);
    'outer: for location in &ranked {
        if reached(&result) {
            break;
        }
        if location.w_loc == 0 {
            continue;
        }
        if strategy.distinct_locations && seen_locations.contains(location.name) {
            continue;
        }
        for node_index in ranked_nodes(view, &location.members, strategy, subject) {
            let node = &view.nodes[node_index];
            if used_nodes.contains(&node.node_id) {
                continue;
            }
            if !is_eligible(node, strategy, &excluded) {
                continue;
            }
            result.push(node.node_id);
            used_nodes.insert(node.node_id);
            seen_locations.insert(node.location.clone());
            if reached(&result) {
                break 'outer;
            }
            if strategy.distinct_locations {
                continue 'outer;
            }
        }
    }
    result
}

/// Resolves the placement strategy and any subject override for `target`.
///
/// Precedence: override strategy id > longest matching metadata path prefix >
/// group binding > class binding > realm binding > `default_strategy_id`,
/// falling back to the first configured strategy only when no configured ref
/// applies. Returns `None` when a configured ref points at no strategy.
pub fn strategy_for_target<'a>(
    config: &'a RealmConfigDocument,
    target: &DocumentSyncTarget,
    context: PlacementResolutionContext<'_>,
) -> Option<(&'a PlacementStrategy, Option<&'a PlacementOverride>)> {
    let subject = subject_bytes(target);
    let override_ = config
        .placement_overrides
        .iter()
        .find(|over| over.subject == subject);

    let strategy = match resolve_strategy(config, target, context, override_) {
        Ok(Some(strategy)) => strategy,
        Ok(None) => config.strategies.first()?,
        Err(()) => return None,
    };
    Some((strategy, override_))
}

/// Resolves a class binding without subject, path, or group precedence.
pub(super) fn strategy_for_class(
    config: &RealmConfigDocument,
    class: DocumentClass,
) -> Option<&PlacementStrategy> {
    let strategy = match resolve_class_strategy(config, class) {
        Ok(Some(strategy)) => strategy,
        Ok(None) => config.strategies.first()?,
        Err(()) => return None,
    };
    Some(strategy)
}

/// Coarse document class used for class-scoped strategy bindings.
pub fn document_class(target: &DocumentSyncTarget) -> DocumentClass {
    match target {
        DocumentSyncTarget::Group { .. } | DocumentSyncTarget::GroupAuthorization { .. } => {
            DocumentClass::Group
        }
        DocumentSyncTarget::User { .. } => DocumentClass::User,
        DocumentSyncTarget::MetadataRegistry { .. } => DocumentClass::MetadataRegistry,
        DocumentSyncTarget::MetadataCreateEvent { .. }
        | DocumentSyncTarget::MetadataDocumentLifecycle { .. }
        | DocumentSyncTarget::MetadataGraphLifecycle { .. } => DocumentClass::Metadata,
        DocumentSyncTarget::RealmAuthorization { .. }
        | DocumentSyncTarget::RealmConfig { .. }
        | DocumentSyncTarget::NodeUsage { .. }
        | DocumentSyncTarget::WatchInterest { .. }
        | DocumentSyncTarget::WatchSubscription { .. }
        | DocumentSyncTarget::NodeInfo { .. } => DocumentClass::Admin,
    }
}

/// Canonical grouping subject: all per-document metadata variants of one
/// document collapse to its document id; realm-shared targets fall back to
/// their storage key.
pub fn subject_bytes(target: &DocumentSyncTarget) -> Vec<u8> {
    match target {
        DocumentSyncTarget::Group { group_id }
        | DocumentSyncTarget::GroupAuthorization { group_id } => group_id.to_bytes().to_vec(),
        DocumentSyncTarget::User { user_id } => user_id.to_bytes(),
        DocumentSyncTarget::MetadataRegistry { document_id, .. }
        | DocumentSyncTarget::MetadataCreateEvent { document_id, .. }
        | DocumentSyncTarget::MetadataDocumentLifecycle { document_id } => {
            document_id.to_bytes().to_vec()
        }
        DocumentSyncTarget::MetadataGraphLifecycle { graph_iri } => graph_iri.as_bytes().to_vec(),
        _ => target.storage_key().as_ref().to_vec(),
    }
}

/// Canonical bucket-choice subject for a Meta Resource (spec 6.3.6): the byte
/// serialization of `(realm_id, group_id, normalized_canonical_path)`. It is
/// known before the MetaResourceId is generated, so bucket choice never
/// substitutes the id and becomes circular. The fixed-width `realm_id ‖ group_id`
/// prefix keeps the trailing path unambiguous.
pub fn meta_bucket_subject(realm_id: RealmId, group_id: GroupId, normalized_path: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(32 + 16 + normalized_path.len());
    bytes.extend_from_slice(realm_id.as_bytes());
    bytes.extend_from_slice(&group_id.to_bytes());
    bytes.extend_from_slice(normalized_path.as_bytes());
    bytes
}

struct RankedLocation<'a> {
    name: &'a str,
    w_loc: u64,
    members: Vec<usize>,
}

fn ranked_locations<'a>(
    view: &'a PlacementView,
    strategy: &PlacementStrategy,
    subject: &[u8],
) -> Vec<RankedLocation<'a>> {
    let mut groups: BTreeMap<&'a str, (u64, Vec<usize>)> = BTreeMap::new();
    for (index, node) in view.nodes.iter().enumerate() {
        let slot = groups
            .entry(node.location.as_str())
            .or_insert((0, Vec::new()));
        // Availability status and subject-level exclusions deliberately do not
        // affect W_loc, preserving location order when a candidate is rejected.
        if node.kind.is_sync_eligible() && passes_filters(node, strategy) {
            slot.0 = slot.0.saturating_add(effective_weight(node, strategy));
        }
        slot.1.push(index);
    }

    let locations: Vec<RankedLocation<'a>> = groups
        .into_iter()
        .map(|(name, (w_loc, members))| RankedLocation {
            name,
            w_loc,
            members,
        })
        .collect();
    let candidates: Vec<(&[u8], u64)> = locations
        .iter()
        .map(|location| (location.name.as_bytes(), location.w_loc))
        .collect();
    let order = rank_weighted(ROLE_LOCATION, subject, &candidates);

    let mut slots: Vec<Option<RankedLocation<'a>>> = locations.into_iter().map(Some).collect();
    order
        .into_iter()
        .map(|position| slots[position].take().expect("rank is a permutation"))
        .collect()
}

fn ranked_nodes(
    view: &PlacementView,
    members: &[usize],
    strategy: &PlacementStrategy,
    subject: &[u8],
) -> Vec<usize> {
    let candidates: Vec<([u8; 32], u64)> = members
        .iter()
        .map(|&index| {
            let node = &view.nodes[index];
            (*node.node_id.as_bytes(), effective_weight(node, strategy))
        })
        .collect();
    rank_weighted(ROLE_NODE, subject, &candidates)
        .into_iter()
        .map(|position| members[position])
        .collect()
}

fn is_eligible(
    node: &ResolvedNode,
    strategy: &PlacementStrategy,
    excluded: &HashSet<NodeId>,
) -> bool {
    node.kind.is_sync_eligible()
        && is_available(node, strategy)
        && !excluded.contains(&node.node_id)
        && passes_filters(node, strategy)
}

fn is_available(node: &ResolvedNode, strategy: &PlacementStrategy) -> bool {
    !node.full && !node.draining && effective_weight(node, strategy) > 0
}

fn passes_filters(node: &ResolvedNode, strategy: &PlacementStrategy) -> bool {
    strategy.affinity.iter().all(|rule| match rule.effect {
        AffinityEffect::Filter => label_matches(&node.labels, &rule.matcher),
        AffinityEffect::Multiply { .. } => true,
    })
}

fn effective_weight(node: &ResolvedNode, strategy: &PlacementStrategy) -> u64 {
    let mut weight = u64::from(node.weight);
    for rule in &strategy.affinity {
        if let AffinityEffect::Multiply { permille } = rule.effect
            && label_matches(&node.labels, &rule.matcher)
        {
            weight = weight.saturating_mul(u64::from(permille)) / 1000;
        }
    }
    weight
}

fn label_matches(labels: &BTreeMap<String, String>, matcher: &LabelMatch) -> bool {
    labels
        .get(&matcher.key)
        .is_some_and(|value| value == &matcher.value)
}

fn kind_label(kind: &RealmNodeKind) -> &'static str {
    match kind {
        RealmNodeKind::Management => "management",
        RealmNodeKind::Server => "server",
        RealmNodeKind::Local => "local",
        RealmNodeKind::User => "user",
    }
}

fn group_id_of(target: &DocumentSyncTarget) -> Option<GroupId> {
    match target {
        DocumentSyncTarget::Group { group_id }
        | DocumentSyncTarget::GroupAuthorization { group_id }
        | DocumentSyncTarget::MetadataRegistry { group_id, .. } => Some(*group_id),
        _ => None,
    }
}

fn resolve_strategy<'a>(
    config: &'a RealmConfigDocument,
    target: &DocumentSyncTarget,
    context: PlacementResolutionContext<'_>,
    override_: Option<&PlacementOverride>,
) -> Result<Option<&'a PlacementStrategy>, ()> {
    if let Some(id) = override_.and_then(|over| over.strategy_id) {
        return config.strategy(&id).map(Some).ok_or(());
    }

    let class = document_class(target);
    if matches!(
        class,
        DocumentClass::Metadata | DocumentClass::MetadataRegistry
    ) && let Some(path) = context.metadata_path
    {
        let path = MetadataRegistryRecord::normalize_document_path(path);
        let best = config
            .strategy_bindings
            .iter()
            .filter_map(|binding| match &binding.scope {
                BindingScope::MetadataPathPrefix(prefix) => {
                    metadata_path_prefix_match_len(&path, prefix).map(|len| (len, binding))
                }
                _ => None,
            })
            .max_by_key(|(len, _)| *len)
            .map(|(_, binding)| binding);
        if let Some(binding) = best {
            return config.strategy(&binding.strategy_id).map(Some).ok_or(());
        }
    }

    if let Some(group_id) = context.group_id.or_else(|| group_id_of(target))
        && let Some(strategy) = binding_strategy(config, &BindingScope::Group(group_id))?
    {
        return Ok(Some(strategy));
    }
    if let Some(strategy) = binding_strategy(config, &BindingScope::Class(class))? {
        return Ok(Some(strategy));
    }
    if let Some(strategy) = binding_strategy(config, &BindingScope::Realm)? {
        return Ok(Some(strategy));
    }
    if let Some(id) = config.default_strategy_id {
        return config.strategy(&id).map(Some).ok_or(());
    }
    Ok(None)
}

fn resolve_class_strategy(
    config: &RealmConfigDocument,
    class: DocumentClass,
) -> Result<Option<&PlacementStrategy>, ()> {
    if let Some(strategy) = binding_strategy(config, &BindingScope::Class(class))? {
        return Ok(Some(strategy));
    }
    if let Some(strategy) = binding_strategy(config, &BindingScope::Realm)? {
        return Ok(Some(strategy));
    }
    if let Some(id) = config.default_strategy_id {
        return config.strategy(&id).map(Some).ok_or(());
    }
    Ok(None)
}

fn metadata_path_prefix_match_len(normalized_path: &str, prefix: &str) -> Option<usize> {
    let prefix = MetadataRegistryRecord::normalize_document_path(prefix);
    if prefix.is_empty()
        || normalized_path == prefix
        || normalized_path
            .strip_prefix(prefix.as_str())
            .is_some_and(|suffix| suffix.starts_with('/'))
    {
        Some(prefix.len())
    } else {
        None
    }
}

fn binding_strategy<'a>(
    config: &'a RealmConfigDocument,
    scope: &BindingScope,
) -> Result<Option<&'a PlacementStrategy>, ()> {
    config
        .strategy_bindings
        .iter()
        .find(|binding| &binding.scope == scope)
        .map(|binding| config.strategy(&binding.strategy_id).ok_or(()))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::admin_document_reducer::{
        AdminDocumentReducerState, overlay_realm_config_placement_reducer_materialization,
    };
    use aruna_core::admin_documents::AdminDocumentTarget;
    use aruna_core::structs::{
        AffinityRule, NodePlacementEntry, RealmId, RealmNode, StrategyBinding,
    };
    use aruna_core::types::UserId;
    use proptest::prelude::*;
    use ulid::Ulid;

    fn node_id(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    fn resolved(seed: u8, kind: RealmNodeKind, location: &str, weight: u32) -> ResolvedNode {
        ResolvedNode {
            node_id: node_id(seed),
            kind,
            location: location.to_string(),
            weight,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        }
    }

    fn strategy(replica_count: Option<u32>, distinct_locations: bool) -> PlacementStrategy {
        PlacementStrategy {
            strategy_id: Ulid::from_bytes([1u8; 16]),
            name: "test".to_string(),
            replica_count,
            distinct_locations,
            affinity: Vec::new(),
            shard_count: 64,
        }
    }

    fn location_of<'a>(view: &'a PlacementView, node: &NodeId) -> &'a str {
        view.nodes
            .iter()
            .find(|candidate| candidate.node_id == *node)
            .map(|candidate| candidate.location.as_str())
            .unwrap()
    }

    fn filter_rule(key: &str, value: &str) -> AffinityRule {
        AffinityRule {
            matcher: LabelMatch {
                key: key.to_string(),
                value: value.to_string(),
            },
            effect: AffinityEffect::Filter,
        }
    }

    fn multiply_rule(key: &str, value: &str, permille: u32) -> AffinityRule {
        AffinityRule {
            matcher: LabelMatch {
                key: key.to_string(),
                value: value.to_string(),
            },
            effect: AffinityEffect::Multiply { permille },
        }
    }

    fn sid(seed: u8) -> Ulid {
        Ulid::from_bytes([seed; 16])
    }

    fn strat(seed: u8) -> PlacementStrategy {
        PlacementStrategy {
            strategy_id: sid(seed),
            name: format!("s{seed}"),
            replica_count: Some(3),
            distinct_locations: true,
            affinity: Vec::new(),
            shard_count: 64,
        }
    }

    fn binding(scope: BindingScope, seed: u8) -> StrategyBinding {
        StrategyBinding {
            scope,
            strategy_id: sid(seed),
        }
    }

    #[test]
    fn build_view_applies_defaults_labels_and_skips_unparseable() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mapped = node_id(1);
        let unmapped = node_id(2);

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(mapped, RealmNodeKind::Server);
        config.ensure_node(unmapped, RealmNodeKind::Local);
        config.nodes.push(RealmNode {
            node_id: "not-a-valid-key".to_string(),
            kind: RealmNodeKind::Server,
        });
        config.placement_map.push(NodePlacementEntry {
            node_id: mapped,
            location: "eu".to_string(),
            weight: 250,
            full: true,
            draining: false,
            labels: BTreeMap::from([
                ("tier".to_string(), "hot".to_string()),
                (KIND_LABEL_KEY.to_string(), "override-bogus".to_string()),
            ]),
        });

        let view = build_view(&config);
        assert_eq!(view.nodes.len(), 2);

        let a = view.nodes.iter().find(|n| n.node_id == mapped).unwrap();
        assert_eq!(a.location, "eu");
        assert_eq!(a.weight, 250);
        assert!(a.full);
        assert!(!a.draining);
        // entry labels < derived kind label (derived wins).
        assert_eq!(a.labels.get("tier"), Some(&"hot".to_string()));
        assert_eq!(a.labels.get(KIND_LABEL_KEY), Some(&"server".to_string()));

        let b = view.nodes.iter().find(|n| n.node_id == unmapped).unwrap();
        assert_eq!(b.location, DEFAULT_LOCATION);
        assert_eq!(b.weight, DEFAULT_NODE_WEIGHT);
        assert!(!b.full);
        assert!(!b.draining);
        assert_eq!(b.labels.get(KIND_LABEL_KEY), Some(&"local".to_string()));
    }

    #[test]
    fn entry_labels_from_config_drive_affinity_filter() {
        // The chain ARUNA_NODE_LABELS -> onboarding -> placement-map entry ->
        // build_view -> Filter selects only labeled nodes.
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let labeled = node_id(1);
        let plain = node_id(2);

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(labeled, RealmNodeKind::Server);
        config.ensure_node(plain, RealmNodeKind::Server);
        config.placement_map.push(NodePlacementEntry {
            node_id: labeled,
            location: "eu".to_string(),
            weight: 100,
            full: false,
            draining: false,
            labels: BTreeMap::from([("tier".to_string(), "hot".to_string())]),
        });

        let view = build_view(&config);
        let mut strategy = strategy(None, false);
        strategy.affinity = vec![filter_rule("tier", "hot")];

        let holders = resolve_holders(&view, &strategy, b"subject", None);
        assert_eq!(holders, vec![labeled]);
    }

    #[test]
    fn resolve_is_deterministic_and_order_independent() {
        let nodes = vec![
            resolved(1, RealmNodeKind::Server, "a", 100),
            resolved(2, RealmNodeKind::Server, "a", 300),
            resolved(3, RealmNodeKind::Server, "b", 200),
            resolved(4, RealmNodeKind::Server, "c", 50),
            resolved(5, RealmNodeKind::Server, "c", 400),
        ];
        let view = PlacementView {
            nodes: nodes.clone(),
        };
        let strategy = strategy(Some(3), true);

        let first = resolve_holders(&view, &strategy, b"subject", None);
        let second = resolve_holders(&view, &strategy, b"subject", None);
        assert_eq!(first, second);

        let mut reversed = nodes;
        reversed.reverse();
        let reversed_view = PlacementView { nodes: reversed };
        let third = resolve_holders(&reversed_view, &strategy, b"subject", None);
        assert_eq!(first, third);
    }

    #[test]
    fn distinct_locations_yields_pairwise_distinct_holders() {
        let mut nodes = Vec::new();
        let mut seed = 1u8;
        for location in ["a", "b", "c"] {
            for _ in 0..2 {
                nodes.push(resolved(seed, RealmNodeKind::Server, location, 100));
                seed += 1;
            }
        }
        let view = PlacementView { nodes };
        let strategy = strategy(Some(3), true);

        let holders = resolve_holders(&view, &strategy, b"subject", None);
        assert_eq!(holders.len(), 3);
        let locations: HashSet<&str> = holders
            .iter()
            .map(|node| location_of(&view, node))
            .collect();
        assert_eq!(locations.len(), 3);
    }

    #[test]
    fn rejection_stability_replaces_only_full_node() {
        let nodes: Vec<ResolvedNode> = (1..=5)
            .map(|seed| resolved(seed, RealmNodeKind::Server, "solo", 100))
            .collect();
        let view = PlacementView {
            nodes: nodes.clone(),
        };

        let full_order = resolve_holders(&view, &strategy(None, false), b"subject", None);
        assert_eq!(full_order.len(), 5);

        let baseline = resolve_holders(&view, &strategy(Some(3), false), b"subject", None);
        assert_eq!(baseline, full_order[..3].to_vec());

        let mut mutated = nodes;
        for node in mutated.iter_mut() {
            if node.node_id == full_order[0] {
                node.full = true;
            }
        }
        let mutated_view = PlacementView { nodes: mutated };
        let after = resolve_holders(&mutated_view, &strategy(Some(3), false), b"subject", None);
        // Only the rejected node changes: the next candidate slots in, order held.
        assert_eq!(after, full_order[1..4].to_vec());
    }

    #[test]
    fn zero_weight_and_user_kind_never_selected() {
        let nodes = vec![
            resolved(1, RealmNodeKind::Server, "x", 0),
            resolved(2, RealmNodeKind::User, "x", 100),
            resolved(3, RealmNodeKind::Server, "x", 100),
        ];
        let view = PlacementView { nodes };
        let holders = resolve_holders(&view, &strategy(None, false), b"subject", None);
        assert_eq!(holders, vec![node_id(3)]);
    }

    #[test]
    fn multiply_permille_zero_makes_node_ineligible() {
        let mut zeroed = resolved(1, RealmNodeKind::Server, "x", 100);
        zeroed.labels.insert("tier".to_string(), "cold".to_string());
        let keep = resolved(2, RealmNodeKind::Server, "x", 100);
        let view = PlacementView {
            nodes: vec![zeroed, keep],
        };
        let mut strategy = strategy(None, false);
        strategy.affinity = vec![multiply_rule("tier", "cold", 0)];

        let holders = resolve_holders(&view, &strategy, b"subject", None);
        assert_eq!(holders, vec![node_id(2)]);
    }

    #[test]
    fn filter_rules_are_conjunctive() {
        let mut both = resolved(1, RealmNodeKind::Server, "x", 100);
        both.labels.insert("tier".to_string(), "hot".to_string());
        both.labels.insert("region".to_string(), "eu".to_string());
        let mut wrong_region = resolved(2, RealmNodeKind::Server, "x", 100);
        wrong_region
            .labels
            .insert("tier".to_string(), "hot".to_string());
        wrong_region
            .labels
            .insert("region".to_string(), "us".to_string());
        let mut wrong_tier = resolved(3, RealmNodeKind::Server, "x", 100);
        wrong_tier
            .labels
            .insert("tier".to_string(), "cold".to_string());
        wrong_tier
            .labels
            .insert("region".to_string(), "eu".to_string());
        let view = PlacementView {
            nodes: vec![both, wrong_region, wrong_tier],
        };
        let mut strategy = strategy(None, false);
        strategy.affinity = vec![filter_rule("tier", "hot"), filter_rule("region", "eu")];

        let holders = resolve_holders(&view, &strategy, b"subject", None);
        assert_eq!(holders, vec![node_id(1)]);
    }

    #[test]
    fn filtered_nodes_contribute_zero_to_location_weight() {
        let mut hot_a = resolved(1, RealmNodeKind::Server, "a", 100);
        hot_a.labels.insert("tier".to_string(), "hot".to_string());
        let mut hot_b = resolved(2, RealmNodeKind::Server, "b", 100);
        hot_b.labels.insert("tier".to_string(), "hot".to_string());
        let mut cold_b = resolved(3, RealmNodeKind::Server, "b", 900);
        cold_b.labels.insert("tier".to_string(), "cold".to_string());
        let view = PlacementView {
            nodes: vec![hot_a, hot_b, cold_b],
        };
        let mut strategy = strategy(Some(1), false);
        strategy.affinity = vec![filter_rule("tier", "hot")];

        let weights: BTreeMap<String, u64> = ranked_locations(&view, &strategy, b"subject")
            .into_iter()
            .map(|location| (location.name.to_string(), location.w_loc))
            .collect();

        assert_eq!(
            weights,
            BTreeMap::from([("a".to_string(), 100), ("b".to_string(), 100)])
        );
    }

    #[test]
    fn unavailable_pins_are_skipped_and_eligible_pin_counts_toward_target() {
        let mut pinned_full = resolved(3, RealmNodeKind::Server, "c", 100);
        pinned_full.full = true;
        let mut pinned_draining = resolved(4, RealmNodeKind::Server, "d", 100);
        pinned_draining.draining = true;
        let pinned_zero_weight = resolved(5, RealmNodeKind::Server, "e", 0);
        let view = PlacementView {
            nodes: vec![
                resolved(1, RealmNodeKind::Server, "a", 100),
                resolved(2, RealmNodeKind::Server, "b", 100),
                pinned_full,
                pinned_draining,
                pinned_zero_weight,
                resolved(6, RealmNodeKind::Server, "f", 100),
            ],
        };
        let override_ = PlacementOverride {
            subject: Vec::new(),
            pinned: vec![node_id(3), node_id(4), node_id(5), node_id(6)],
            excluded: vec![node_id(2)],
            strategy_id: None,
        };

        let holders = resolve_holders(
            &view,
            &strategy(Some(2), true),
            b"subject",
            Some(&override_),
        );
        assert_eq!(holders, vec![node_id(6), node_id(1)]);
    }

    #[test]
    fn pinned_and_excluded_node_is_not_selected() {
        let view = PlacementView {
            nodes: vec![
                resolved(1, RealmNodeKind::Server, "a", 100),
                resolved(2, RealmNodeKind::Server, "b", 100),
            ],
        };
        // Node 1 is both pinned and excluded: exclusion wins, it never appears.
        let override_ = PlacementOverride {
            subject: Vec::new(),
            pinned: vec![node_id(1)],
            excluded: vec![node_id(1)],
            strategy_id: None,
        };

        let holders = resolve_holders(&view, &strategy(None, false), b"subject", Some(&override_));
        assert!(!holders.contains(&node_id(1)));
        assert_eq!(holders, vec![node_id(2)]);
    }

    #[test]
    fn everywhere_strategy_returns_all_eligible() {
        let mut full = resolved(4, RealmNodeKind::Server, "b", 100);
        full.full = true;
        let view = PlacementView {
            nodes: vec![
                resolved(1, RealmNodeKind::Server, "a", 100),
                resolved(2, RealmNodeKind::User, "a", 100),
                resolved(3, RealmNodeKind::Server, "b", 100),
                full,
                resolved(5, RealmNodeKind::Server, "c", 100),
            ],
        };

        let holders = resolve_holders(&view, &strategy(None, false), b"subject", None);
        let selected: HashSet<NodeId> = holders.iter().copied().collect();
        assert_eq!(holders.len(), 3);
        assert_eq!(
            selected,
            HashSet::from([node_id(1), node_id(3), node_id(5)])
        );
    }

    #[test]
    fn strategy_for_target_precedence() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = sid(2);
        let document_id = sid(3);
        let target = DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        };
        let path = Some("/datasets/important/x");

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        for seed in [10u8, 11, 12, 13, 14, 15, 16] {
            config.strategies.push(strat(seed));
        }
        config.default_strategy_id = Some(sid(16));
        config.strategy_bindings = vec![
            binding(
                BindingScope::MetadataPathPrefix("/datasets/important".to_string()),
                11,
            ),
            binding(
                BindingScope::MetadataPathPrefix("/datasets".to_string()),
                12,
            ),
            binding(BindingScope::Group(group_id), 13),
            binding(BindingScope::Class(DocumentClass::MetadataRegistry), 14),
            binding(BindingScope::Realm, 15),
        ];
        config.placement_overrides = vec![PlacementOverride {
            subject: subject_bytes(&target),
            pinned: Vec::new(),
            excluded: Vec::new(),
            strategy_id: Some(sid(10)),
        }];

        let winner = |config: &RealmConfigDocument| {
            strategy_for_target(
                config,
                &target,
                PlacementResolutionContext {
                    metadata_path: path,
                    ..Default::default()
                },
            )
            .unwrap()
            .0
            .strategy_id
        };

        assert_eq!(winner(&config), sid(10)); // override
        config.placement_overrides.clear();
        assert_eq!(winner(&config), sid(11)); // longest path prefix
        config
            .strategy_bindings
            .retain(|b| !matches!(&b.scope, BindingScope::MetadataPathPrefix(p) if p == "/datasets/important"));
        assert_eq!(winner(&config), sid(12)); // shorter path prefix
        config
            .strategy_bindings
            .retain(|b| !matches!(&b.scope, BindingScope::MetadataPathPrefix(_)));
        assert_eq!(winner(&config), sid(13)); // group
        config
            .strategy_bindings
            .retain(|b| !matches!(&b.scope, BindingScope::Group(_)));
        assert_eq!(winner(&config), sid(14)); // class
        config
            .strategy_bindings
            .retain(|b| !matches!(&b.scope, BindingScope::Class(_)));
        assert_eq!(winner(&config), sid(15)); // realm
        config.strategy_bindings.clear();
        assert_eq!(winner(&config), sid(16)); // default
        config.default_strategy_id = None;
        assert_eq!(winner(&config), sid(10)); // first strategy fallback
    }

    #[test]
    fn strategy_for_target_dangling_configured_refs_do_not_fallback() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = sid(2);
        let target = DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id: sid(3),
        };
        let missing = sid(99);

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.strategies = vec![strat(10), strat(11)];

        config.default_strategy_id = Some(missing);
        let datasets = PlacementResolutionContext {
            metadata_path: Some("datasets/x"),
            ..Default::default()
        };
        assert!(strategy_for_target(&config, &target, datasets).is_none());

        config.default_strategy_id = Some(sid(11));
        config.strategy_bindings = vec![binding(
            BindingScope::MetadataPathPrefix("datasets".to_string()),
            99,
        )];
        assert!(strategy_for_target(&config, &target, datasets).is_none());

        config.strategy_bindings = vec![binding(BindingScope::Group(group_id), 99)];
        let other = PlacementResolutionContext {
            metadata_path: Some("other/x"),
            ..Default::default()
        };
        assert!(strategy_for_target(&config, &target, other).is_none());

        config.strategy_bindings = vec![binding(
            BindingScope::Class(DocumentClass::MetadataRegistry),
            99,
        )];
        assert!(strategy_for_target(&config, &target, other).is_none());

        config.strategy_bindings = vec![binding(BindingScope::Realm, 99)];
        assert!(strategy_for_target(&config, &target, other).is_none());

        config.strategy_bindings.clear();
        config.placement_overrides = vec![PlacementOverride {
            subject: subject_bytes(&target),
            pinned: Vec::new(),
            excluded: Vec::new(),
            strategy_id: Some(missing),
        }];
        assert!(strategy_for_target(&config, &target, datasets).is_none());

        config.placement_overrides.clear();
        config.default_strategy_id = None;
        assert_eq!(
            strategy_for_target(&config, &target, datasets)
                .unwrap()
                .0
                .strategy_id,
            sid(10)
        );
    }

    #[test]
    fn reducer_materialization_repairs_dangling_refs_and_keeps_resolution_nonempty() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let live = sid(10);
        let missing = sid(99);
        let holder = node_id(1);
        let excluded = node_id(2);
        let target = DocumentSyncTarget::RealmConfig { realm_id };
        let subject = subject_bytes(&target);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(holder, RealmNodeKind::Server);
        config.strategies = vec![strat(10)];
        config.default_strategy_id = Some(missing);
        config.strategy_bindings = vec![binding(BindingScope::Realm, 99)];
        config.placement_overrides = vec![PlacementOverride {
            subject: subject.clone(),
            pinned: vec![holder],
            excluded: vec![excluded],
            strategy_id: Some(missing),
        }];
        let reducer_state =
            AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id });

        overlay_realm_config_placement_reducer_materialization(&mut config, &reducer_state);

        assert_eq!(config.default_strategy_id, Some(live));
        assert_eq!(config.strategy_bindings[0].strategy_id, live);
        assert_eq!(config.placement_overrides[0].strategy_id, Some(live));
        assert_eq!(config.placement_overrides[0].pinned, vec![holder]);
        assert_eq!(config.placement_overrides[0].excluded, vec![excluded]);
        let (strategy, override_) =
            strategy_for_target(&config, &target, PlacementResolutionContext::default())
                .expect("repaired placement remains resolvable");
        assert_eq!(strategy.strategy_id, live);
        assert!(!resolve_holders(&build_view(&config), strategy, &subject, override_).is_empty());
    }

    #[test]
    fn metadata_path_prefix_binding_requires_path_boundary() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = sid(2);
        let target = DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id: sid(3),
        };

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.strategies = vec![strat(10), strat(11), strat(12)];
        config.strategy_bindings = vec![
            binding(
                BindingScope::MetadataPathPrefix(" /datasets/ ".to_string()),
                10,
            ),
            binding(BindingScope::Class(DocumentClass::MetadataRegistry), 11),
        ];
        config.default_strategy_id = Some(sid(12));

        let strategy_id = |path: &str| {
            strategy_for_target(
                &config,
                &target,
                PlacementResolutionContext {
                    metadata_path: Some(path),
                    ..Default::default()
                },
            )
            .unwrap()
            .0
            .strategy_id
        };

        assert_eq!(strategy_id("datasets"), sid(10));
        assert_eq!(strategy_id("/datasets/x"), sid(10));
        assert_eq!(strategy_id("datasets-private/x"), sid(11));
    }

    #[test]
    fn metadata_lifecycle_uses_group_and_path_from_context() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = sid(2);
        let target = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: sid(3),
        };
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.strategies = vec![strat(10), strat(11), strat(12)];
        config.strategy_bindings = vec![
            binding(
                BindingScope::MetadataPathPrefix("datasets/important".to_string()),
                10,
            ),
            binding(BindingScope::Group(group_id), 11),
        ];
        config.default_strategy_id = Some(sid(12));
        let context = PlacementResolutionContext {
            group_id: Some(group_id),
            metadata_path: Some("datasets/important/item"),
        };

        assert_eq!(
            strategy_for_target(&config, &target, context)
                .unwrap()
                .0
                .strategy_id,
            sid(10)
        );
        config
            .strategy_bindings
            .retain(|binding| !matches!(&binding.scope, BindingScope::MetadataPathPrefix(_)));
        assert_eq!(
            strategy_for_target(&config, &target, context)
                .unwrap()
                .0
                .strategy_id,
            sid(11)
        );
    }

    #[test]
    fn subject_bytes_groups_document_variants() {
        let document_id = sid(4);
        let registry = DocumentSyncTarget::MetadataRegistry {
            group_id: sid(5),
            document_id,
        };
        let create = DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id: sid(6),
        };
        let lifecycle = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };

        let expected = document_id.to_bytes().to_vec();
        assert_eq!(subject_bytes(&registry), expected);
        assert_eq!(subject_bytes(&create), expected);
        assert_eq!(subject_bytes(&lifecycle), expected);

        let other = DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: sid(7),
        };
        assert_ne!(subject_bytes(&other), expected);
    }

    #[test]
    fn meta_bucket_subject_vector() {
        // Portable 6.3.6 vector: fixed-width realm_id ‖ group_id, then the path.
        let subject = meta_bucket_subject(
            RealmId::from_bytes([1u8; 32]),
            Ulid::from_bytes([2u8; 16]),
            "datasets/x",
        );
        let mut expected = vec![1u8; 32];
        expected.extend_from_slice(&[2u8; 16]);
        expected.extend_from_slice(b"datasets/x");
        assert_eq!(subject, expected);
    }

    #[test]
    fn all_document_variants_share_one_shard() {
        use aruna_core::structs::shard_for_subject;

        let document_id = sid(4);
        let variants = [
            DocumentSyncTarget::MetadataRegistry {
                group_id: sid(5),
                document_id,
            },
            DocumentSyncTarget::MetadataCreateEvent {
                document_id,
                event_id: sid(6),
            },
            DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
        ];
        let expected = shard_for_subject(&subject_bytes(&variants[0]), 64);
        for target in &variants {
            assert_eq!(shard_for_subject(&subject_bytes(target), 64), expected);
        }
    }

    #[test]
    fn document_class_maps_variants() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let user_id = UserId::local(sid(2), realm_id);
        let targets = [
            (
                DocumentSyncTarget::Group { group_id: sid(1) },
                DocumentClass::Group,
            ),
            (
                DocumentSyncTarget::GroupAuthorization { group_id: sid(1) },
                DocumentClass::Group,
            ),
            (
                DocumentSyncTarget::RealmAuthorization { realm_id },
                DocumentClass::Admin,
            ),
            (
                DocumentSyncTarget::RealmConfig { realm_id },
                DocumentClass::Admin,
            ),
            (DocumentSyncTarget::User { user_id }, DocumentClass::User),
            (
                DocumentSyncTarget::MetadataRegistry {
                    group_id: sid(1),
                    document_id: sid(2),
                },
                DocumentClass::MetadataRegistry,
            ),
            (
                DocumentSyncTarget::MetadataCreateEvent {
                    document_id: sid(2),
                    event_id: sid(3),
                },
                DocumentClass::Metadata,
            ),
            (
                DocumentSyncTarget::MetadataDocumentLifecycle {
                    document_id: sid(2),
                },
                DocumentClass::Metadata,
            ),
            (
                DocumentSyncTarget::MetadataGraphLifecycle {
                    graph_iri: "https://example.test/graph".to_string(),
                },
                DocumentClass::Metadata,
            ),
            (
                DocumentSyncTarget::NodeUsage {
                    realm_id,
                    node_id: node_id(1),
                    group_id: Some(sid(1)),
                },
                DocumentClass::Admin,
            ),
            (
                DocumentSyncTarget::WatchInterest {
                    realm_id,
                    node_id: node_id(1),
                },
                DocumentClass::Admin,
            ),
            (
                DocumentSyncTarget::WatchSubscription {
                    owner: user_id,
                    watch_id: sid(4),
                },
                DocumentClass::Admin,
            ),
            (
                DocumentSyncTarget::NodeInfo {
                    realm_id,
                    node_id: node_id(1),
                },
                DocumentClass::Admin,
            ),
        ];

        for (target, expected) in targets {
            assert_eq!(document_class(&target), expected, "{target:?}");
        }
    }

    proptest! {
        #[test]
        fn location_order_ignores_status_flips(
            specs in prop::collection::vec((0u8..6, 1u32..1000, any::<bool>(), any::<bool>()), 1..12),
        ) {
            let build = |flip: bool| {
                let nodes = specs
                    .iter()
                    .enumerate()
                    .map(|(index, &(location, weight, full, draining))| ResolvedNode {
                        node_id: node_id((index as u8) + 1),
                        kind: RealmNodeKind::Server,
                        location: format!("loc{location}"),
                        weight,
                        full: full ^ flip,
                        draining: draining ^ flip,
                        labels: BTreeMap::new(),
                    })
                    .collect();
                PlacementView { nodes }
            };

            let strategy = strategy(None, false);
            let baseline: Vec<String> = ranked_locations(&build(false), &strategy, b"subject")
                .iter()
                .map(|location| location.name.to_string())
                .collect();
            let flipped: Vec<String> = ranked_locations(&build(true), &strategy, b"subject")
                .iter()
                .map(|location| location.name.to_string())
                .collect();
            prop_assert_eq!(baseline, flipped);
        }

        #[test]
        fn location_weight_sum_excludes_non_sync_eligible(
            specs in prop::collection::vec((0u8..4, 1u32..1000, any::<bool>()), 1..12),
        ) {
            let nodes: Vec<ResolvedNode> = specs
                .iter()
                .enumerate()
                .map(|(index, &(location, weight, is_user))| ResolvedNode {
                    node_id: node_id((index as u8) + 1),
                    kind: if is_user {
                        RealmNodeKind::User
                    } else {
                        RealmNodeKind::Server
                    },
                    location: format!("loc{location}"),
                    weight,
                    full: false,
                    draining: false,
                    labels: BTreeMap::new(),
                })
                .collect();
            let view = PlacementView { nodes: nodes.clone() };
            let strategy = strategy(None, false);

            // W_loc counts only sync-eligible node weights; User weights never
            // contribute to a location's rendezvous weight.
            for location in ranked_locations(&view, &strategy, b"subject") {
                let expected: u64 = nodes
                    .iter()
                    .filter(|node| {
                        node.location == location.name && node.kind.is_sync_eligible()
                    })
                    .map(|node| u64::from(node.weight))
                    .sum();
                prop_assert_eq!(location.w_loc, expected);
            }
        }
    }
}
