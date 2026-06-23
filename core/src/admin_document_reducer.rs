use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::admin_documents::{
    AdminDocumentClock, AdminDocumentDot, AdminDocumentEvent, AdminDocumentOperation,
    AdminDocumentTarget,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminDocumentApplyStatus {
    Applied,
    Duplicate,
    StaleOriginSequence,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AdminDocumentReducerError {
    #[error("admin document event target does not match reducer state")]
    TargetMismatch,
    #[error("admin document reducer supports only user targets in this slice")]
    UnsupportedTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentAttributeVersion {
    pub value: Option<String>,
    pub dot: AdminDocumentDot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentConflictValue {
    pub value: Option<String>,
    pub dot: AdminDocumentDot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentConflict {
    pub path: String,
    pub values: Vec<AdminDocumentConflictValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentReducerState {
    pub target: AdminDocumentTarget,
    pub clock: AdminDocumentClock,
    pub applied_event_ids: BTreeSet<Ulid>,
    pub user_attributes: BTreeMap<String, AdminDocumentAttributeVersion>,
    pub conflicts: BTreeMap<String, AdminDocumentConflict>,
}

impl AdminDocumentReducerState {
    pub fn new(target: AdminDocumentTarget) -> Self {
        Self {
            target,
            clock: AdminDocumentClock::default(),
            applied_event_ids: BTreeSet::new(),
            user_attributes: BTreeMap::new(),
            conflicts: BTreeMap::new(),
        }
    }

    pub fn apply(
        &mut self,
        event: &AdminDocumentEvent,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        if event.target != self.target {
            return Err(AdminDocumentReducerError::TargetMismatch);
        }
        if !matches!(event.target, AdminDocumentTarget::User { .. }) {
            return Err(AdminDocumentReducerError::UnsupportedTarget);
        }
        if self.applied_event_ids.contains(&event.event_id) {
            return Ok(AdminDocumentApplyStatus::Duplicate);
        }
        if event.origin_seq <= self.clock.sequence_for(&event.origin_node_id) {
            return Ok(AdminDocumentApplyStatus::StaleOriginSequence);
        }

        match &event.op {
            AdminDocumentOperation::UserAttributeSet { key, value } => {
                self.apply_user_attribute(event, key, Some(value.clone()));
            }
            AdminDocumentOperation::UserAttributeRemoved { key } => {
                self.apply_user_attribute(event, key, None);
            }
        }

        self.applied_event_ids.insert(event.event_id);
        self.clock.advance(event.origin_node_id, event.origin_seq);
        Ok(AdminDocumentApplyStatus::Applied)
    }

    pub fn materialized_user_attributes(&self) -> BTreeMap<String, String> {
        self.user_attributes
            .iter()
            .filter_map(|(key, version)| {
                version
                    .value
                    .as_ref()
                    .map(|value| (key.clone(), value.clone()))
            })
            .collect()
    }

    fn apply_user_attribute(
        &mut self,
        event: &AdminDocumentEvent,
        key: &str,
        value: Option<String>,
    ) {
        let dot = event.dot();
        let path = user_attribute_path(key);

        if self.conflict_is_observed(event, &path) {
            self.conflicts.remove(&path);
            self.user_attributes.insert(
                key.to_string(),
                AdminDocumentAttributeVersion { value, dot },
            );
            return;
        }

        if self.conflicts.contains_key(&path) {
            self.record_conflict_value(&path, value, dot);
            self.user_attributes.remove(key);
            return;
        }

        let Some(current) = self.user_attributes.get(key).cloned() else {
            self.user_attributes.insert(
                key.to_string(),
                AdminDocumentAttributeVersion { value, dot },
            );
            return;
        };

        if event_observes_dot(event, &current.dot) {
            self.user_attributes.insert(
                key.to_string(),
                AdminDocumentAttributeVersion { value, dot },
            );
            return;
        }

        if current.value != value {
            self.user_attributes.remove(key);
            self.record_conflict_value(&path, current.value, current.dot);
            self.record_conflict_value(&path, value, dot);
        }
    }

    fn conflict_is_observed(&self, event: &AdminDocumentEvent, path: &str) -> bool {
        self.conflicts.get(path).is_some_and(|conflict| {
            conflict
                .values
                .iter()
                .all(|value| event_observes_dot(event, &value.dot))
        })
    }

    fn record_conflict_value(&mut self, path: &str, value: Option<String>, dot: AdminDocumentDot) {
        let conflict =
            self.conflicts
                .entry(path.to_string())
                .or_insert_with(|| AdminDocumentConflict {
                    path: path.to_string(),
                    values: Vec::new(),
                });

        if !conflict.values.iter().any(|candidate| candidate.dot == dot) {
            conflict
                .values
                .push(AdminDocumentConflictValue { value, dot });
            conflict.values.sort_by_key(|value| value.dot);
        }
    }
}

fn event_observes_dot(event: &AdminDocumentEvent, dot: &AdminDocumentDot) -> bool {
    event.observed.observes(dot)
        || (event.origin_node_id == dot.origin_node_id && event.origin_seq > dot.origin_seq)
}

fn user_attribute_path(key: &str) -> String {
    format!("user.attributes.{key}")
}

#[cfg(test)]
mod tests {
    use super::{AdminDocumentApplyStatus, AdminDocumentReducerState};
    use crate::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
    };
    use crate::structs::{Actor, RealmId};
    use crate::{NodeId, UserId};
    use std::collections::BTreeMap;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm_id() -> RealmId {
        RealmId::from_bytes([9u8; 32])
    }

    fn user_id() -> UserId {
        UserId::local(Ulid::from_bytes([8u8; 16]), realm_id())
    }

    fn actor(origin_node_id: NodeId) -> Actor {
        Actor {
            node_id: origin_node_id,
            user_id: user_id(),
            realm_id: realm_id(),
        }
    }

    fn user_state() -> AdminDocumentReducerState {
        AdminDocumentReducerState::new(AdminDocumentTarget::User { user_id: user_id() })
    }

    fn event(
        event_seed: u8,
        origin_node_id: NodeId,
        origin_seq: u64,
        observed: AdminDocumentClock,
        op: AdminDocumentOperation,
    ) -> AdminDocumentEvent {
        AdminDocumentEvent {
            event_id: Ulid::from_bytes([event_seed; 16]),
            target: AdminDocumentTarget::User { user_id: user_id() },
            origin_node_id,
            origin_seq,
            observed,
            actor: actor(origin_node_id),
            op,
        }
    }

    fn set_attr(event_seed: u8, origin_seed: u8, key: &str, value: &str) -> AdminDocumentEvent {
        event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: key.to_string(),
                value: value.to_string(),
            },
        )
    }

    #[test]
    fn user_disjoint_attribute_updates_merge() {
        let mut state = user_state();

        assert_eq!(
            state.apply(&set_attr(1, 1, "orcid", "0000-0002-1825-0097")),
            Ok(AdminDocumentApplyStatus::Applied)
        );
        assert_eq!(
            state.apply(&set_attr(2, 2, "department", "biology")),
            Ok(AdminDocumentApplyStatus::Applied)
        );

        assert_eq!(
            state.materialized_user_attributes(),
            BTreeMap::from([
                ("department".to_string(), "biology".to_string()),
                ("orcid".to_string(), "0000-0002-1825-0097".to_string()),
            ])
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn same_user_attribute_conflict_is_recorded() {
        let mut state = user_state();

        state
            .apply(&set_attr(1, 1, "department", "physics"))
            .unwrap();
        state
            .apply(&set_attr(2, 2, "department", "biology"))
            .unwrap();

        assert!(
            !state
                .materialized_user_attributes()
                .contains_key("department")
        );
        let conflict = state
            .conflicts
            .get("user.attributes.department")
            .expect("conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some("physics"))
        );
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some("biology"))
        );
    }

    #[test]
    fn duplicate_event_id_is_idempotent() {
        let mut state = user_state();
        let event = set_attr(1, 1, "department", "biology");

        assert_eq!(state.apply(&event), Ok(AdminDocumentApplyStatus::Applied));
        let applied_once = state.clone();

        assert_eq!(state.apply(&event), Ok(AdminDocumentApplyStatus::Duplicate));
        assert_eq!(state, applied_once);
    }

    #[test]
    fn stale_origin_sequence_is_ignored() {
        let mut state = user_state();
        let origin = node(1);
        let newer = event(
            2,
            origin,
            2,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "biology".to_string(),
            },
        );
        let stale = event(
            1,
            origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: "orcid".to_string(),
                value: "0000-0002-1825-0097".to_string(),
            },
        );

        state.apply(&newer).unwrap();
        let before_stale = state.clone();

        assert_eq!(
            state.apply(&stale),
            Ok(AdminDocumentApplyStatus::StaleOriginSequence)
        );
        assert_eq!(state, before_stale);
    }

    #[test]
    fn observed_sequential_user_attribute_update_replaces_prior_value() {
        let mut state = user_state();
        let first_origin = node(1);
        let first = event(
            1,
            first_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "physics".to_string(),
            },
        );
        let second = event(
            2,
            node(2),
            1,
            AdminDocumentClock::default().with_observed(first_origin, 1),
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "biology".to_string(),
            },
        );

        state.apply(&first).unwrap();
        state.apply(&second).unwrap();

        assert_eq!(
            state
                .materialized_user_attributes()
                .get("department")
                .map(String::as_str),
            Some("biology")
        );
        assert!(state.conflicts.is_empty());
    }
}
