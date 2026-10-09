//! Decides which user name and attributes are public and answers public searches.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::structs::identity::user::User;
use std::collections::HashMap;

pub const VISIBILITY_PREFIX: &str = "profile.visibility.";

impl User {
    /// Names are public by default; attributes require an explicit public choice.
    pub fn field_public(&self, key: &str) -> bool {
        if key.starts_with(VISIBILITY_PREFIX) {
            return false;
        }
        match self.attributes.get(&format!("{VISIBILITY_PREFIX}{key}")) {
            Some(value) => value == "public",
            None => key == "name",
        }
    }

    pub fn public_name(&self) -> String {
        if self.field_public("name") && !self.name.is_empty() {
            self.name.clone()
        } else {
            self.user_id.to_string()
        }
    }

    pub fn public_attributes(&self) -> HashMap<String, String> {
        self.attributes
            .iter()
            .filter(|(key, _)| self.field_public(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    /// Whether this active, non-service account's public name is `query` in lowercase.
    pub fn public_name_is(&self, query: &str) -> bool {
        self.field_public("name")
            && !query.is_empty()
            && self.name.to_lowercase() == query
            && !self.is_deactivated()
            && self.service_group().is_none()
    }

    pub fn public_matches(&self, query: &str) -> bool {
        (self.field_public("name") && self.name.to_lowercase().contains(query))
            || self
                .attributes
                .iter()
                .any(|(key, value)| self.field_public(key) && value.to_lowercase().contains(query))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UserId;
    use crate::structs::identity::realm::RealmId;
    use ulid::Ulid;

    #[test]
    fn respects_visibility() {
        let mut user = User {
            user_id: UserId::local(Ulid::from_bytes([1; 16]), RealmId::from_bytes([2; 32])),
            name: "Alice".into(),
            subject_ids: vec!["private-subject".into()],
            alias_user_ids: Default::default(),
            attributes: HashMap::from([
                ("email".into(), "private@example.test".into()),
                ("affiliation".into(), "University".into()),
            ]),
        };
        assert!(user.public_matches("alice"));
        assert!(!user.public_matches("private"));
        assert!(!user.public_matches("university"));
        assert!(user.public_attributes().is_empty());
        user.attributes
            .insert("profile.visibility.email".into(), "public".into());
        user.attributes
            .insert("profile.visibility.name".into(), "private".into());
        assert!(!user.public_matches("alice"));
        assert!(user.public_matches("private@example"));
        assert_eq!(user.public_name(), user.user_id.to_string());
        assert_eq!(
            user.public_attributes(),
            HashMap::from([("email".into(), "private@example.test".into()),])
        );
        user.attributes
            .insert("profile.visibility.email".into(), "private".into());
        assert!(!user.public_matches("private@example"));
    }

    #[test]
    fn hint_needs_public() {
        // Only an active, non-service account's public name matches, whole and without case.
        let mut user = User {
            user_id: UserId::local(Ulid::from_bytes([1; 16]), RealmId::from_bytes([2; 32])),
            name: "Anna Smith".into(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: HashMap::new(),
        };
        assert!(user.public_name_is("anna smith"));
        assert!(!user.public_name_is("anna"));
        let private = format!("{VISIBILITY_PREFIX}name");
        user.attributes.insert(private.clone(), "private".into());
        assert!(!user.public_name_is("anna smith"));
        user.attributes.remove(&private);
        let deactivated = crate::user::validation::DEACTIVATED_ATTRIBUTE.to_string();
        user.attributes.insert(deactivated, "true".into());
        assert!(!user.public_name_is("anna smith"));
    }
}
