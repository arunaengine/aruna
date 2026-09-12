use super::*;

impl AdminDocumentReducerState {
    pub(super) fn apply_realm(
        &mut self,
        event: &AdminDocumentEvent,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        match &event.op {
            AdminDocumentOperation::RealmRoleAdded { role_id } => {
                self.apply_realm_role(event, role_id, role_id.to_string());
            }
            AdminDocumentOperation::RealmRoleCreated { role } => {
                self.apply_realm_role(event, &role.role_id, role_definition_value(role));
            }
            AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id } => {
                self.apply_realm_assignment(event, role_id, user_id, Some(user_id.to_string()));
            }
            AdminDocumentOperation::RealmRoleUserAssignmentRemoved { role_id, user_id } => {
                self.apply_realm_assignment(event, role_id, user_id, None);
            }
            _ => return Err(AdminDocumentReducerError::UnsupportedTarget),
        }
        Ok(AdminDocumentApplyStatus::Applied)
    }
}
