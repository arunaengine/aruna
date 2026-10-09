//! Applies user events for names, subject ids, linked logins and validated attributes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

impl AdminDocumentState {
    pub(super) fn apply_user(
        &mut self,
        event: &AdminDocumentEvent,
    ) -> Result<AdminApplyStatus, AdminDocumentError> {
        match &event.op {
            AdminDocumentOperation::UserNameSet { name } => self.apply_user_name(event, name),
            AdminDocumentOperation::SubjectIdAdded { subject_id } => {
                self.apply_user_subject(event, subject_id, Some(subject_id.clone()));
            }
            AdminDocumentOperation::SubjectIdRemoved { subject_id } => {
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
            AdminDocumentOperation::UserAliasAdded { alias }
            | AdminDocumentOperation::UserAliasRemoved { alias } => {
                let AdminDocumentTarget::User { user_id } = &event.target else {
                    return Err(AdminDocumentError::UnsupportedTarget);
                };
                // Only a login of another realm is linked; never a local or public principal.
                if alias.realm_id == user_id.realm_id || alias.is_nil() {
                    return Err(AdminDocumentError::UnsupportedTarget);
                }
                let added = matches!(event.op, AdminDocumentOperation::UserAliasAdded { .. });
                let path = user_alias_path(alias);
                let value = added.then(|| alias.to_string());
                let current = self.user_subject_ids.get(&path).cloned();
                match self.reduce_value(event, &path, current, value) {
                    Some(version) => self.user_subject_ids.insert(path, version),
                    None => self.user_subject_ids.remove(&path),
                };
            }
            _ => return Err(AdminDocumentError::UnsupportedTarget),
        }
        Ok(AdminApplyStatus::Applied)
    }
}
