use super::*;

impl AdminDocumentReducer {
    pub(super) fn apply_user(
        &mut self,
        event: &AdminDocumentEvent,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        match &event.op {
            AdminDocumentOperation::UserNameSet { name } => self.apply_user_name(event, name),
            AdminDocumentOperation::UserSubjectIdAdded { subject_id } => {
                self.apply_user_subject(event, subject_id, Some(subject_id.clone()));
            }
            AdminDocumentOperation::UserSubjectIdRemoved { subject_id } => {
                self.apply_user_subject(event, subject_id, None);
            }
            AdminDocumentOperation::UserAttributeSet { key, value } => {
                validate_attribute_key(key)?;
                validate_attribute_value(key, value)?;
                self.apply_user_attribute(event, key, Some(value.clone()));
            }
            AdminDocumentOperation::UserAttributeRemoved { key } => {
                validate_attribute_key(key)?;
                self.apply_user_attribute(event, key, None);
            }
            _ => return Err(AdminDocumentReducerError::UnsupportedTarget),
        }
        Ok(AdminDocumentApplyStatus::Applied)
    }
}
