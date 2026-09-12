use super::*;

impl AdminDocumentReducerState {
    pub(super) fn apply_group(
        &mut self,
        event: &AdminDocumentEvent,
        group_id: &crate::types::GroupId,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        match &event.op {
            AdminDocumentOperation::GroupCreated {
                realm_id,
                display_name,
                owner,
            } => self.apply_group_created(event, realm_id, display_name, owner),
            AdminDocumentOperation::GroupRoleAdded { role_id } => {
                self.apply_group_role(event, role_id, role_id.to_string());
            }
            AdminDocumentOperation::GroupRoleCreated { role } => {
                self.apply_group_role(event, &role.role_id, role_definition_value(role));
            }
            AdminDocumentOperation::GroupRoleRemoved { role_id } => {
                self.remove_group_role(event, role_id);
            }
            AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id } => {
                self.apply_group_assignment(event, role_id, user_id, Some(user_id.to_string()));
            }
            AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id } => {
                self.apply_group_assignment(event, role_id, user_id, None);
            }
            AdminDocumentOperation::GroupDisplayNameSet { display_name } => {
                self.apply_group_field(
                    event,
                    GROUP_DISPLAY_NAME_PATH,
                    Some(display_name.to_string()),
                );
            }
            AdminDocumentOperation::GroupJoinRequested { request } => {
                if request.group_id != *group_id
                    || request.request_id.is_nil()
                    || request.user_id != event.actor.user_id
                    || request.user_id.is_nil()
                    || request.user_id.realm_id != event.actor.realm_id
                    || !crate::join_request::valid_message(&request.message)
                {
                    return Err(AdminDocumentReducerError::InvalidJoinRequest);
                }
                let value = serde_json::to_string(request)
                    .map_err(|_| AdminDocumentReducerError::InvalidJoinRequest)?;
                self.apply_group_field(
                    event,
                    &crate::join_request::request_path(request.request_id),
                    Some(value),
                );
            }
            AdminDocumentOperation::GroupJoinDecided { decision } => {
                use crate::join_request::JoinDecisionKind;
                if decision.request_id.is_nil()
                    || decision.user_id.is_nil()
                    || decision.user_id.realm_id != event.actor.realm_id
                    || decision.decided_by != event.actor.user_id
                    || !crate::join_request::valid_message(&decision.reason)
                    || decision.role_ids.iter().any(Ulid::is_nil)
                    || match decision.kind {
                        JoinDecisionKind::Approved => decision.role_ids.is_empty(),
                        JoinDecisionKind::Denied | JoinDecisionKind::Withdrawn => {
                            !decision.role_ids.is_empty()
                        }
                    }
                    || (decision.kind == JoinDecisionKind::Withdrawn
                        && decision.user_id != event.actor.user_id)
                {
                    return Err(AdminDocumentReducerError::InvalidJoinRequest);
                }
                let value = serde_json::to_string(decision)
                    .map_err(|_| AdminDocumentReducerError::InvalidJoinRequest)?;
                self.apply_group_field(
                    event,
                    &crate::join_request::decision_path(decision.request_id),
                    Some(value),
                );
                for role_id in &decision.role_ids {
                    self.apply_group_assignment(
                        event,
                        role_id,
                        &decision.user_id,
                        Some(decision.user_id.to_string()),
                    );
                }
            }
            AdminDocumentOperation::GroupPoliciesSet { policies } => {
                self.apply_group_field(event, GROUP_POLICIES_PATH, Some(policies_value(policies)));
            }
            _ => return Err(AdminDocumentReducerError::UnsupportedTarget),
        }
        Ok(AdminDocumentApplyStatus::Applied)
    }
}
