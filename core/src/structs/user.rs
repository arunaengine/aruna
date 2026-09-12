use crate::errors::ConversionError;
use crate::structs::Actor;
use crate::types::UserId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct User {
    pub user_id: UserId,
    pub name: String,
    pub subject_ids: Vec<String>,
    pub alias_user_ids: HashSet<UserId>,
    pub attributes: HashMap<String, String>,
}

impl User {
    pub fn to_bytes(&self, _actor: &Actor) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn reconcile_bytes(
        &self,
        current: Option<&[u8]>,
        actor: &Actor,
    ) -> Result<Vec<u8>, ConversionError> {
        let _ = (current, actor);
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::User;
    use crate::UserId;
    use crate::structs::{Actor, RealmId};
    use std::collections::HashMap;
    use ulid::Ulid;

    #[test]
    fn user_attributes_roundtrip() {
        let realm_id = RealmId([2u8; 32]);
        let user_id = UserId::new(Ulid::generate(), realm_id);
        let user = User {
            user_id,
            name: "alice".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: HashMap::from([("orcid".to_string(), "0000-0002-1825-0097".to_string())]),
        };
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
            user_id,
            realm_id,
        };

        let bytes = user.to_bytes(&actor).unwrap();
        let hydrated_user = User::from_bytes(&bytes).unwrap();

        assert_eq!(user, hydrated_user);
    }
}
