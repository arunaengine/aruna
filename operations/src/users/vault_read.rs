//! Reads one user's vault heads or public key records, locally on a holder or from the holders.
//! A miss on every reached holder is an empty answer; no answer at all is unavailable.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::document::DocumentTarget;
use aruna_core::effects::{
    Effect, HolderList, MAX_FETCH_HOLDERS, NetEffect, StorageEffect, VaultFetchEffect, VaultQuery,
};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, StorageEvent, VaultFetchEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::vault::{
    MAX_KEY_RECORDS, MAX_VAULT_HEADS, VaultRecords, user_record_prefix,
};
use aruna_core::types::{Effects, Key, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::placement::{
    PlacementResolveError, holds_placement, plan_target_placement, read_holder_sets,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ReadVaultConfig {
    pub node_id: NodeId,
    pub user_id: UserId,
    pub query: VaultQuery,
    pub deadline: Duration,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadVaultError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    PlacementResolve(#[from] PlacementResolveError),
    #[error("realm config document missing")]
    RealmConfigMissing,
    #[error("no placement strategy governs user vaults")]
    PlacementUnavailable,
    /// A holder did not accept the forwarded caller.
    #[error("a vault holder refused the caller")]
    Denied,
    /// No holder answered; this never means the records are absent.
    #[error("no vault holder answered: {0}")]
    Unavailable(String),
    #[error("a vault holder answered with records of another user or kind")]
    ForeignRecords,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadState {
    Init,
    ReadConfig,
    ReadLocal,
    Fetch,
    Finish,
    Error,
}

/// Reads the user's records from this node when it holds the vault placement,
/// otherwise from the resolved holders through one fetch effect.
#[derive(Debug, PartialEq)]
pub struct ReadVaultOperation {
    config: ReadVaultConfig,
    state: ReadState,
    output: Option<Result<VaultRecords, ReadVaultError>>,
}

impl ReadVaultOperation {
    pub fn new(config: ReadVaultConfig) -> Self {
        Self {
            config,
            state: ReadState::Init,
            output: None,
        }
    }

    fn reads_heads(&self) -> bool {
        matches!(self.config.query, VaultQuery::Heads { .. })
    }

    fn route(&mut self, value: Option<Value>) -> Result<Effects, ReadVaultError> {
        let config =
            RealmConfigDocument::from_bytes(&value.ok_or(ReadVaultError::RealmConfigMissing)?)?;
        let user_id = self.config.user_id;
        let target = if self.reads_heads() {
            DocumentTarget::VaultRevision {
                user_id,
                revision_id: Ulid::nil(),
            }
        } else {
            DocumentTarget::UserKey {
                user_id,
                record_id: Ulid::nil(),
            }
        };
        let plan = plan_target_placement(&config, &target, Default::default())?
            .ok_or(ReadVaultError::PlacementUnavailable)?;
        if holds_placement(&config, &plan.placement, self.config.node_id) {
            let limit = if self.reads_heads() {
                MAX_VAULT_HEADS
            } else {
                MAX_KEY_RECORDS
            };
            self.state = ReadState::ReadLocal;
            return Ok(smallvec![Effect::Storage(StorageEffect::Iter {
                key_space: target.storage_keyspace().to_string(),
                prefix: Some(user_record_prefix(user_id)),
                start: None,
                limit,
                txn_id: None,
            })]);
        }
        let mut holders = read_holder_sets(&config, &plan.placement)?;
        holders.retain(|holder| *holder != self.config.node_id);
        holders.truncate(MAX_FETCH_HOLDERS);
        let holders = HolderList::new(holders)
            .map_err(|_| ReadVaultError::Unavailable("the vault has no holder".to_string()))?;
        self.state = ReadState::Fetch;
        Ok(smallvec![Effect::Net(NetEffect::VaultFetch(Box::new(
            VaultFetchEffect {
                holders,
                user_id,
                query: self.config.query.clone(),
                deadline: self.config.deadline,
            }
        )))])
    }

    fn local(&self, rows: Vec<(Key, Value)>) -> Result<VaultRecords, ReadVaultError> {
        let values = rows.iter().map(|(_, value)| value.as_ref());
        let records = if self.reads_heads() {
            VaultRecords::Heads(
                values
                    .map(aruna_core::structs::identity::user::vault::VaultRevision::from_bytes)
                    .collect::<Result<_, _>>()?,
            )
        } else {
            VaultRecords::Keys(
                values
                    .map(aruna_core::structs::identity::user::vault::UserKeyRecord::from_bytes)
                    .collect::<Result<_, _>>()?,
            )
        };
        Ok(records)
    }

    /// A fetched answer is accepted only for this user and this query.
    fn fetched(&self, event: VaultFetchEvent) -> Result<VaultRecords, ReadVaultError> {
        let user_id = self.config.user_id;
        match event {
            VaultFetchEvent::Fetched { records, .. } => {
                let fits = match (&records, self.reads_heads()) {
                    (VaultRecords::Heads(heads), true) => heads
                        .iter()
                        .all(|head| head.user_id == user_id && head.validate().is_ok()),
                    (VaultRecords::Keys(keys), false) => keys
                        .iter()
                        .all(|key| key.user_id == user_id && key.validate().is_ok()),
                    _ => false,
                };
                if !fits || records.len() > MAX_KEY_RECORDS.max(MAX_VAULT_HEADS) {
                    return Err(ReadVaultError::ForeignRecords);
                }
                Ok(records)
            }
            VaultFetchEvent::NotFound if self.reads_heads() => Ok(VaultRecords::Heads(Vec::new())),
            VaultFetchEvent::NotFound => Ok(VaultRecords::Keys(Vec::new())),
            VaultFetchEvent::Denied => Err(ReadVaultError::Denied),
            VaultFetchEvent::Unavailable(reason) => Err(ReadVaultError::Unavailable(reason)),
        }
    }

    fn finish(&mut self, output: Result<VaultRecords, ReadVaultError>) -> Effects {
        self.state = if output.is_ok() {
            ReadState::Finish
        } else {
            ReadState::Error
        };
        self.output = Some(output);
        smallvec![]
    }
}

impl Operation for ReadVaultOperation {
    type Output = VaultRecords;
    type Error = ReadVaultError;

    fn start(&mut self) -> Effects {
        self.state = ReadState::ReadConfig;
        let target = DocumentTarget::RealmConfig {
            realm_id: self.config.user_id.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (ReadState::Finish | ReadState::Error, _) => smallvec![],
            (_, Event::Storage(StorageEvent::Error { error })) => self.finish(Err(error.into())),
            (ReadState::ReadConfig, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                match self.route(value) {
                    Ok(effects) => effects,
                    Err(error) => self.finish(Err(error)),
                }
            }
            (ReadState::ReadLocal, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let output = self.local(values);
                self.finish(output)
            }
            (ReadState::Fetch, Event::Net(NetEvent::VaultFetch(event))) => {
                let output = self.fetched(event);
                self.finish(output)
            }
            (state, event) => self.finish(Err(ReadVaultError::UnexpectedEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                got: super::vault_write::event_label(&event).to_string(),
            })),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReadState::Finish | ReadState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ReadVaultError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
    use aruna_core::structs::identity::user::vault::VaultRevision;
    use aruna_core::structs::placement::record::PlacementRef;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn user(seed: u8) -> UserId {
        UserId::local(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([3; 32]))
    }

    /// Four servers and two vault replicas, so the realm has holders and non-holders.
    fn realm() -> (Value, Vec<NodeId>) {
        let mut config = RealmConfigDocument::new(user(7).realm_id, Vec::new(), 2);
        config.seed_default_placement();
        for seed in 1..=4 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        let target = DocumentTarget::UserKey {
            user_id: user(7),
            record_id: Ulid::nil(),
        };
        let holders = plan_target_placement(&config, &target, Default::default())
            .unwrap()
            .unwrap()
            .holders;
        let actor = Actor {
            node_id: node(1),
            user_id: user(7),
            realm_id: user(7).realm_id,
        };
        (config.to_bytes(&actor).unwrap().into(), holders)
    }

    /// Starts a read on a holder or, with `holds` false, on a node outside the holder set.
    fn started(holds: bool, query: VaultQuery) -> (ReadVaultOperation, Effects, Vec<NodeId>) {
        let (config, holders) = realm();
        let node_id = if holds {
            holders[0]
        } else {
            (1..=4)
                .map(node)
                .find(|node| !holders.contains(node))
                .unwrap()
        };
        let mut operation = ReadVaultOperation::new(ReadVaultConfig {
            node_id,
            user_id: user(7),
            query,
            deadline: Duration::from_secs(10),
        });
        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Key::from(&[][..]),
            value: Some(config),
        }));
        (operation, effects, holders)
    }

    fn fetched(operation: &mut ReadVaultOperation, event: VaultFetchEvent) {
        operation.step(Event::Net(NetEvent::VaultFetch(event)));
    }

    #[test]
    fn fetches_from_holders() {
        let (mut operation, effects, holders) = started(false, VaultQuery::Keys);
        let Some(Effect::Net(NetEffect::VaultFetch(fetch))) = effects.first() else {
            panic!("a non-holder fetches, got {effects:?}");
        };
        assert_eq!(fetch.holders.as_slice(), holders.as_slice());
        assert_eq!(fetch.user_id, user(7));
        // Every reached holder answered without records: the user has none.
        fetched(&mut operation, VaultFetchEvent::NotFound);
        assert_eq!(operation.finalize(), Ok(VaultRecords::Keys(Vec::new())));
    }

    #[test]
    fn reads_holder_locally() {
        let (_, effects, _) = started(true, VaultQuery::Keys);
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Iter { prefix: Some(prefix), .. }))
                if *prefix == user_record_prefix(user(7))
        ));
    }

    #[test]
    fn unavailable_stays_unavailable() {
        // No answer is never reported as an absent vault.
        let query = VaultQuery::Heads { auth_token: None };
        let (mut operation, _, _) = started(false, query);
        fetched(
            &mut operation,
            VaultFetchEvent::Unavailable("down".to_string()),
        );
        assert_eq!(
            operation.finalize(),
            Err(ReadVaultError::Unavailable("down".to_string()))
        );
        let query = VaultQuery::Heads { auth_token: None };
        let (mut operation, _, _) = started(false, query);
        fetched(&mut operation, VaultFetchEvent::Denied);
        assert_eq!(operation.finalize(), Err(ReadVaultError::Denied));
    }

    #[test]
    fn rejects_foreign_records() {
        let foreign = VaultRevision {
            user_id: user(8),
            revision_id: Ulid::from_bytes([1; 16]),
            predecessors: Vec::new(),
            payload: Some("sealed".to_string()),
            node_id: node(1),
            placement: PlacementRef::NIL,
            created_at_ms: 1,
        };
        let query = VaultQuery::Heads { auth_token: None };
        let (mut operation, _, _) = started(false, query);
        fetched(
            &mut operation,
            VaultFetchEvent::Fetched {
                holder: node(1),
                records: VaultRecords::Heads(vec![foreign]),
            },
        );
        assert_eq!(operation.finalize(), Err(ReadVaultError::ForeignRecords));
        // An answer of the other kind is refused too.
        let query = VaultQuery::Heads { auth_token: None };
        let (mut operation, _, _) = started(false, query);
        fetched(
            &mut operation,
            VaultFetchEvent::Fetched {
                holder: node(1),
                records: VaultRecords::Keys(Vec::new()),
            },
        );
        assert_eq!(operation.finalize(), Err(ReadVaultError::ForeignRecords));
    }
}
