//! Looks up each holder's public keys in the key directory, one user after another.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::VaultQuery;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::vault::VaultRecords;
use aruna_core::structs::storage::holders::KeyLookup;
use aruna_core::{NodeId, UserId};
use thiserror::Error;

use crate::users::vault_read::{ReadVaultConfig, ReadVaultOperation};

/// How long one key directory lookup may wait for the holders of a user's vault.
const KEY_LOOKUP_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Error, PartialEq)]
pub enum LookupError {
    #[error("could not look up holder keys: {0}")]
    Io(#[from] IoError),
}

/// The key directory answer for each user. Any failed lookup counts as unavailable, never as
/// a user without keys.
#[derive(Debug, PartialEq)]
pub struct LookupKeysOperation {
    realm_id: RealmId,
    node_id: NodeId,
    pending: VecDeque<UserId>,
    asked: Option<UserId>,
    lookups: BTreeMap<UserId, KeyLookup>,
}

type Lookups = BTreeMap<UserId, KeyLookup>;

impl LookupKeysOperation {
    pub fn new(
        realm_id: RealmId,
        node_id: NodeId,
        users: impl IntoIterator<Item = UserId>,
    ) -> Self {
        Self {
            realm_id,
            node_id,
            pending: users.into_iter().collect(),
            asked: None,
            lookups: BTreeMap::new(),
        }
    }

    fn ask_next(&mut self) -> Next<Lookups> {
        let Some(user_id) = self.pending.pop_front() else {
            return Next::Done(std::mem::take(&mut self.lookups));
        };
        self.asked = Some(user_id);
        let config = ReadVaultConfig {
            node_id: self.node_id,
            realm_id: self.realm_id,
            user_id,
            query: VaultQuery::Keys,
            deadline: KEY_LOOKUP_DEADLINE,
        };
        let read = ReadVaultOperation::new(config);
        Next::Call(IoCall::Child(boxed_suboperation(read, |result| {
            let lookup = match result {
                Ok(VaultRecords::Keys(keys)) if keys.is_empty() => KeyLookup::Missing,
                Ok(VaultRecords::Keys(keys)) => KeyLookup::Keys(keys),
                Ok(VaultRecords::Heads(_)) | Err(_) => KeyLookup::Unavailable,
            };
            Event::SubOperation(SubOperationEvent::KeysLooked { lookup })
        })))
    }
}

impl Operation for LookupKeysOperation {
    type Output = Lookups;
    type Error = LookupError;

    fn first(&mut self) -> Result<Next<Lookups>, LookupError> {
        Ok(self.ask_next())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Lookups>, LookupError> {
        let event = result.event()?;
        let (Some(user_id), Event::SubOperation(SubOperationEvent::KeysLooked { lookup })) =
            (self.asked.take(), event)
        else {
            let got = "an event other than the asked key lookup";
            let expected = "SubOperationEvent::KeysLooked";
            return Err(IoError::Unexpected { expected, got }.into());
        };
        self.lookups.insert(user_id, lookup);
        Ok(self.ask_next())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::test_context;
    use crate::tests::s3::test_storage;
    use ulid::Ulid;

    fn user(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([3; 32]))
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn looked(lookup: KeyLookup) -> IoResult {
        IoResult::Event(Box::new(Event::SubOperation(
            SubOperationEvent::KeysLooked { lookup },
        )))
    }

    #[test]
    fn asks_users_sequentially() {
        let realm_id = RealmId::from_bytes([3; 32]);
        let mut operation = LookupKeysOperation::new(realm_id, node(), [user(1), user(2)]);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let next = operation.next(looked(KeyLookup::Missing));
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
        let Ok(Next::Done(lookups)) = operation.next(looked(KeyLookup::Unavailable)) else {
            panic!("expected the lookups");
        };
        assert_eq!(lookups.get(&user(1)), Some(&KeyLookup::Missing));
        assert_eq!(lookups.get(&user(2)), Some(&KeyLookup::Unavailable));
    }

    #[test]
    fn empty_finishes_immediately() {
        let mut operation = LookupKeysOperation::new(RealmId::from_bytes([3; 32]), node(), []);
        assert_eq!(operation.first(), Ok(Next::Done(BTreeMap::new())));
    }

    #[test]
    fn rejects_unasked_answer() {
        // An answer nobody asked for is refused.
        let mut fresh = LookupKeysOperation::new(RealmId::from_bytes([3; 32]), node(), [user(1)]);
        assert!(fresh.next(looked(KeyLookup::Missing)).is_err());
        let mut asking = LookupKeysOperation::new(RealmId::from_bytes([3; 32]), node(), [user(1)]);
        let _ = asking.first();
        assert!(asking.next(IoResult::Done).is_err());
    }

    #[tokio::test]
    async fn unreachable_directory_unavailable() {
        // Without a realm configuration no vault holder can be resolved: that is unknown
        // readiness, never a user without keys.
        let (_dir, storage) = test_storage();
        let context = test_context(storage);
        let user = user(7);
        let lookup = LookupKeysOperation::new(user.realm_id, node(), [user]);
        let lookups = crate::driver::drive(lookup, &context).await.unwrap();
        assert_eq!(lookups.get(&user), Some(&KeyLookup::Unavailable));
    }
}
