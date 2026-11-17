//! Looks up each holder's public keys in the key directory, one user after another.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use aruna_core::effects::{Effect, VaultQuery};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{EffectOperation, boxed_suboperation};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::vault::VaultRecords;
use aruna_core::structs::storage::holders::KeyLookup;
use aruna_core::types::Effects;
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use thiserror::Error;

use crate::users::vault_read::{ReadVaultConfig, ReadVaultOperation};

/// How long one key directory lookup may wait for the holders of a user's vault.
const KEY_LOOKUP_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Error, PartialEq)]
pub enum LookupError {
    #[error("unexpected event while looking up holder keys: {0}")]
    UnexpectedEvent(String),
    #[error("the key lookup did not finish")]
    NotFinished,
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
    error: Option<LookupError>,
    finished: bool,
}

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
            error: None,
            finished: false,
        }
    }

    fn ask_next(&mut self) -> Effects {
        let Some(user_id) = self.pending.pop_front() else {
            self.finished = true;
            return smallvec![];
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
        smallvec![Effect::SubOperation(boxed_suboperation(read, |result| {
            let lookup = match result {
                Ok(VaultRecords::Keys(keys)) if keys.is_empty() => KeyLookup::Missing,
                Ok(VaultRecords::Keys(keys)) => KeyLookup::Keys(keys),
                Ok(VaultRecords::Heads(_)) | Err(_) => KeyLookup::Unavailable,
            };
            Event::SubOperation(SubOperationEvent::KeysLooked { lookup })
        }))]
    }
}

impl EffectOperation for LookupKeysOperation {
    type Output = BTreeMap<UserId, KeyLookup>;
    type Error = LookupError;

    fn start(&mut self) -> Effects {
        self.ask_next()
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.asked.take(), event) {
            (Some(user_id), Event::SubOperation(SubOperationEvent::KeysLooked { lookup }))
                if !self.finished && self.error.is_none() =>
            {
                self.lookups.insert(user_id, lookup);
                self.ask_next()
            }
            (_, event) => {
                self.error = Some(LookupError::UnexpectedEvent(event.label().to_string()));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.finished || self.error.is_some()
    }

    fn finalize(self) -> Result<Self::Output, LookupError> {
        match (self.finished, self.error) {
            (_, Some(error)) => Err(error),
            (true, None) => Ok(self.lookups),
            (false, None) => Err(LookupError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::test_context;
    use crate::tests::s3::test_storage;
    use aruna_core::operation_check::{
        check_abort, rejects_events, rejects_finalize, sample_events,
    };
    use ulid::Ulid;

    fn user(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([3; 32]))
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn looked(lookup: KeyLookup) -> Event {
        Event::SubOperation(SubOperationEvent::KeysLooked { lookup })
    }

    #[test]
    fn asks_users_sequentially() {
        let realm_id = RealmId::from_bytes([3; 32]);
        let mut operation = LookupKeysOperation::new(realm_id, node(), [user(1), user(2)]);
        assert!(matches!(
            operation.start().as_slice(),
            [Effect::SubOperation(_)]
        ));
        let next = operation.step(looked(KeyLookup::Missing));
        assert!(matches!(next.as_slice(), [Effect::SubOperation(_)]));
        assert!(operation.step(looked(KeyLookup::Unavailable)).is_empty());
        let lookups = operation.finalize().unwrap();
        assert_eq!(lookups.get(&user(1)), Some(&KeyLookup::Missing));
        assert_eq!(lookups.get(&user(2)), Some(&KeyLookup::Unavailable));
    }

    #[test]
    fn empty_finishes_immediately() {
        let mut operation = LookupKeysOperation::new(RealmId::from_bytes([3; 32]), node(), []);
        assert!(operation.start().is_empty());
        assert_eq!(operation.finalize(), Ok(BTreeMap::new()));
    }

    #[test]
    fn rejects_other_events() {
        let asking = || {
            let mut operation =
                LookupKeysOperation::new(RealmId::from_bytes([3; 32]), node(), [user(1)]);
            operation.start();
            operation
        };
        let refused = rejects_events(asking, sample_events, |_| false);
        assert_eq!(refused.len(), sample_events().len());
        // An answer nobody asked for is refused too.
        let mut fresh = LookupKeysOperation::new(RealmId::from_bytes([3; 32]), node(), [user(1)]);
        fresh.step(looked(KeyLookup::Missing));
        assert!(fresh.is_complete());
        assert!(fresh.finalize().is_err());
    }

    #[test]
    fn abort_and_finalize() {
        let mut asking = LookupKeysOperation::new(RealmId::from_bytes([3; 32]), node(), [user(1)]);
        asking.start();
        check_abort(&mut asking, &[]);
        rejects_finalize(asking);
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
