use crate::admin_document_reducer::AdminDocumentReducerState;
use crate::admin_documents::AdminDocumentTarget;
use crate::types::{GroupId, RoleId, UserId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use ulid::Ulid;

const REQUEST_PREFIX: &str = "group.join.requests.";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRequest {
    pub request_id: Ulid,
    pub group_id: GroupId,
    pub user_id: UserId,
    pub message: Option<String>,
    pub created_at: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinDecisionKind {
    Denied,
    Withdrawn,
    Approved,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinDecision {
    pub request_id: Ulid,
    pub user_id: UserId,
    pub kind: JoinDecisionKind,
    pub decided_by: UserId,
    pub reason: Option<String>,
    pub decided_at: u64,
    pub role_ids: BTreeSet<RoleId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinRequestState {
    pub request: JoinRequest,
    pub decision: Option<JoinDecision>,
}

pub fn request_path(request_id: Ulid) -> String {
    format!("{REQUEST_PREFIX}{request_id}")
}

pub fn decision_path(request_id: Ulid) -> String {
    format!("group.join.decisions.{request_id}")
}

pub fn valid_message(message: &Option<String>) -> bool {
    message.as_ref().is_none_or(|value| {
        value.len() <= 2000
            && !value
                .chars()
                .any(|ch| ch.is_control() && ch != '\n' && ch != '\t')
    })
}

impl AdminDocumentReducerState {
    pub fn join_requests(&self) -> Vec<JoinRequestState> {
        let AdminDocumentTarget::Group { group_id } = self.target else {
            return Vec::new();
        };
        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let request_id = path.strip_prefix(REQUEST_PREFIX)?.parse::<Ulid>().ok()?;
                if self.conflicts.contains_key(path) {
                    return None;
                }
                let request: JoinRequest = serde_json::from_str(version.value.as_deref()?).ok()?;
                if request.group_id != group_id || request.request_id != request_id {
                    return None;
                }
                let path = decision_path(request_id);
                let current = self
                    .user_subject_ids
                    .get(&path)
                    .and_then(|entry| entry.value.as_deref());
                let conflicts = self
                    .conflicts
                    .get(&path)
                    .into_iter()
                    .flat_map(|conflict| &conflict.values)
                    .filter_map(|entry| entry.value.as_deref());
                // Concurrent approvals grant membership, so they must not appear rejected.
                let decision = current
                    .into_iter()
                    .chain(conflicts)
                    .filter_map(|value| serde_json::from_str::<JoinDecision>(value).ok())
                    .filter(|decision| {
                        decision.request_id == request_id && decision.user_id == request.user_id
                    })
                    .max_by_key(|decision| {
                        (
                            match decision.kind {
                                JoinDecisionKind::Approved => 2,
                                JoinDecisionKind::Withdrawn => 1,
                                JoinDecisionKind::Denied => 0,
                            },
                            decision.decided_at,
                            decision.decided_by,
                        )
                    });
                Some(JoinRequestState { request, decision })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_documents::{AdminDocumentOperation, AdminDocumentRoleDefinition};
    use crate::structs::{Actor, RealmId};
    use std::collections::BTreeMap;

    fn actor(seed: u8) -> Actor {
        let realm_id = RealmId::from_bytes([7; 32]);
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([seed; 16]), realm_id),
            realm_id,
        }
    }

    fn pending() -> (AdminDocumentReducerState, JoinRequest, Ulid) {
        let admin = actor(1);
        let member = actor(2);
        let group_id = Ulid::from_bytes([3; 16]);
        let role_id = Ulid::from_bytes([4; 16]);
        let mut state = AdminDocumentReducerState::new(AdminDocumentTarget::Group { group_id });
        state
            .apply_operation(
                &admin,
                AdminDocumentOperation::GroupRoleCreated {
                    role: AdminDocumentRoleDefinition {
                        role_id,
                        name: "user".into(),
                        permissions: BTreeMap::new(),
                    },
                },
            )
            .unwrap();
        let request = JoinRequest {
            request_id: Ulid::from_bytes([5; 16]),
            group_id,
            user_id: member.user_id,
            message: Some("Please admit me".into()),
            created_at: 1,
        };
        state
            .apply_operation(
                &member,
                AdminDocumentOperation::GroupJoinRequested {
                    request: request.clone(),
                },
            )
            .unwrap();
        (state, request, role_id)
    }

    #[test]
    fn concurrent_decisions_converge() {
        let (initial, request, role_id) = pending();
        let mut approved = initial.clone();
        let mut denied = initial;
        let admin = actor(1);
        let other = actor(6);
        let decision = |kind, by: &Actor, roles| JoinDecision {
            request_id: request.request_id,
            user_id: request.user_id,
            kind,
            decided_by: by.user_id,
            reason: None,
            decided_at: 2,
            role_ids: roles,
        };
        let approval = approved
            .apply_operation(
                &admin,
                AdminDocumentOperation::GroupJoinDecided {
                    decision: decision(
                        JoinDecisionKind::Approved,
                        &admin,
                        BTreeSet::from([role_id]),
                    ),
                },
            )
            .unwrap();
        let rejection = denied
            .apply_operation(
                &other,
                AdminDocumentOperation::GroupJoinDecided {
                    decision: decision(JoinDecisionKind::Denied, &other, BTreeSet::new()),
                },
            )
            .unwrap();
        approved.apply(&rejection).unwrap();
        denied.apply(&approval).unwrap();
        assert_eq!(approved.join_requests(), denied.join_requests());
        assert_eq!(
            approved.join_requests()[0].decision.as_ref().unwrap().kind,
            JoinDecisionKind::Approved
        );
        assert!(
            approved.materialized_group_role_user_assignments()[&role_id]
                .contains(&request.user_id)
        );
        assert!(
            denied.materialized_group_role_user_assignments()[&role_id].contains(&request.user_id)
        );
        let before = approved.clone();
        approved.apply(&approval).unwrap();
        assert_eq!(approved, before);
    }

    #[test]
    fn rejects_forged_withdrawal() {
        let (mut state, request, _) = pending();
        let admin = actor(1);
        let before = state.clone();
        assert!(
            state
                .apply_operation(
                    &admin,
                    AdminDocumentOperation::GroupJoinDecided {
                        decision: JoinDecision {
                            request_id: request.request_id,
                            user_id: request.user_id,
                            kind: JoinDecisionKind::Withdrawn,
                            decided_by: admin.user_id,
                            reason: None,
                            decided_at: 2,
                            role_ids: BTreeSet::new()
                        },
                    }
                )
                .is_err()
        );
        assert_eq!(state, before);
    }
}
