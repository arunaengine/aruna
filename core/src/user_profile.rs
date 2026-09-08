use crate::structs::User;
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
    use crate::structs::RealmId;
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
}
