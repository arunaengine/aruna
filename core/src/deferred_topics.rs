//! Tracks document sync topics that wait for a document they depend on, with bounded capacity.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use irokle::TopicId;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::admin_documents::{AdminDocumentEvent, AdminDocumentOperation};
use crate::document::DocumentTarget;
use crate::structs::identity::realm::RealmId;
use crate::types::GroupId;

/// All waiting topics may name at most this many topics in total.
pub const MAX_DEFERRED_TOPICS: usize = 1_024;
/// One dependency may hold at most this many waiting topics.
pub const TOPICS_PER_DEPENDENCY: usize = 256;

/// A document a deferred topic waits for. Stored in the deferred-topic registry row, so the
/// variants and their order are a stored format.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DocumentSyncDependency {
    RealmConfig(RealmId),
    RealmAuthorization(RealmId),
    PlacementStrategy {
        realm_id: RealmId,
        strategy_id: Ulid,
    },
    /// Roles of the group that owns a placement policy; without them its
    /// publication authority cannot be decided here.
    GroupAuthorization(GroupId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeferredRegistrationOutcome {
    Inserted,
    AlreadyRegistered,
    CapacityExceeded,
}

/// The dependencies an applied admin event can satisfy.
pub fn satisfied_dependencies(
    target: &DocumentTarget,
    event: &AdminDocumentEvent,
) -> Vec<DocumentSyncDependency> {
    let mut dependencies = Vec::new();
    match target {
        DocumentTarget::RealmConfig { realm_id } => {
            dependencies.push(DocumentSyncDependency::RealmConfig(*realm_id));
            if let AdminDocumentOperation::PlacementStrategyUpserted { strategy } = &event.op {
                dependencies.push(DocumentSyncDependency::PlacementStrategy {
                    realm_id: *realm_id,
                    strategy_id: strategy.strategy_id,
                });
            }
        }
        DocumentTarget::RealmAuthorization { realm_id } => {
            dependencies.push(DocumentSyncDependency::RealmAuthorization(*realm_id));
        }
        DocumentTarget::GroupAuthorization { group_id } => {
            dependencies.push(DocumentSyncDependency::GroupAuthorization(*group_id));
        }
        _ => {}
    }
    dependencies
}

pub fn register_deferred_topic(
    deferred_topics: &mut BTreeMap<DocumentSyncDependency, BTreeSet<TopicId>>,
    dependency: DocumentSyncDependency,
    topic_id: TopicId,
) -> DeferredRegistrationOutcome {
    if deferred_topics
        .get(&dependency)
        .is_some_and(|topics| topics.contains(&topic_id))
    {
        return DeferredRegistrationOutcome::AlreadyRegistered;
    }
    let total_topics = deferred_topics.values().map(BTreeSet::len).sum::<usize>();
    let dependency_topics = deferred_topics
        .get(&dependency)
        .map(BTreeSet::len)
        .unwrap_or_default();
    if total_topics >= MAX_DEFERRED_TOPICS || dependency_topics >= TOPICS_PER_DEPENDENCY {
        return DeferredRegistrationOutcome::CapacityExceeded;
    }
    deferred_topics
        .entry(dependency)
        .or_default()
        .insert(topic_id);
    DeferredRegistrationOutcome::Inserted
}

pub fn remove_deferred_topic(
    deferred_topics: &mut BTreeMap<DocumentSyncDependency, BTreeSet<TopicId>>,
    topic_id: TopicId,
) {
    for topics in deferred_topics.values_mut() {
        topics.remove(&topic_id);
    }
    deferred_topics.retain(|_, topics| !topics.is_empty());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(seed: u8) -> TopicId {
        TopicId::from_bytes([seed; 32])
    }

    #[test]
    fn registry_bounds_topics() {
        let dependency = DocumentSyncDependency::RealmConfig(RealmId::from_bytes([1; 32]));
        let mut registry = BTreeMap::new();
        assert_eq!(
            register_deferred_topic(&mut registry, dependency, topic(1)),
            DeferredRegistrationOutcome::Inserted
        );
        assert_eq!(
            register_deferred_topic(&mut registry, dependency, topic(1)),
            DeferredRegistrationOutcome::AlreadyRegistered
        );
        let full: BTreeSet<_> = (0..TOPICS_PER_DEPENDENCY)
            .map(|index| {
                let mut bytes = [7u8; 32];
                bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
                TopicId::from_bytes(bytes)
            })
            .collect();
        registry.insert(dependency, full);
        assert_eq!(
            register_deferred_topic(&mut registry, dependency, topic(2)),
            DeferredRegistrationOutcome::CapacityExceeded
        );
    }

    #[test]
    fn removal_drops_empty() {
        let first = DocumentSyncDependency::RealmConfig(RealmId::from_bytes([1; 32]));
        let second = DocumentSyncDependency::RealmAuthorization(RealmId::from_bytes([1; 32]));
        let mut registry = BTreeMap::new();
        register_deferred_topic(&mut registry, first, topic(1));
        register_deferred_topic(&mut registry, second, topic(1));
        register_deferred_topic(&mut registry, second, topic(2));
        remove_deferred_topic(&mut registry, topic(1));
        assert!(!registry.contains_key(&first));
        assert_eq!(registry.get(&second), Some(&BTreeSet::from([topic(2)])));
    }
}
