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
