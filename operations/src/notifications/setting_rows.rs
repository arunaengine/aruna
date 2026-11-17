//! Reads the current notification setting of a user and writes a new register write of it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::compute::SecretBytes;
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentSyncRevision,
};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::realm_vault::seal_setting;
use aruna_core::register::{RegisterError, RegisterStamp};
use aruna_core::storage_entries::sync_revision_entry;
use aruna_core::structs::execution::email::EmailPreference;
use aruna_core::structs::execution::setting::{SettingChannel, SettingHead, current_setting};
use aruna_core::structs::execution::webhook::WebhookSubscription;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SettingError {
    #[error("the realm configuration cannot be read now: {0}")]
    Config(String),
    #[error("the setting could not be built: {0}")]
    Build(String),
    #[error("the setting could not be stored: {0}")]
    Storage(String),
    #[error(transparent)]
    Register(#[from] RegisterError),
    #[error("unexpected event while handling the setting: {0}")]
    UnexpectedEvent(String),
    #[error("the setting operation did not finish")]
    NotFinished,
}

/// Rows read per page from a settings keyspace.
const SETTING_PAGE: usize = 256;

/// The register writes of one user and channel read so far, with their heads.
#[derive(Debug, Default, PartialEq)]
struct SettingScan {
    rows: Vec<(SettingHead, Vec<u8>)>,
}

impl SettingScan {
    fn page(channel: SettingChannel, owner: UserId, start: Option<Key>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: channel.keyspace().to_string(),
            prefix: Some(owner.to_bytes().into()),
            start: start.map(IterStart::After),
            limit: SETTING_PAGE,
            txn_id: None,
        })]
    }

    fn add(&mut self, channel: SettingChannel, values: Vec<(Key, Value)>) {
        for (_, value) in values {
            match channel.head(&value) {
                Ok(head) => self.rows.push((head, value.to_vec())),
                Err(error) => warn!(%error, "Skipping an undecodable notification setting"),
            }
        }
    }
}

fn unexpected(event: Event) -> SettingError {
    match event {
        Event::Storage(StorageEvent::Error { error }) => SettingError::Storage(error.to_string()),
        other => SettingError::UnexpectedEvent(format!("{other:?}")),
    }
}

#[derive(Debug, Default, PartialEq)]
enum ReadState {
    #[default]
    Init,
    Scan(SettingScan),
    Done(Option<Vec<u8>>),
    Failed(SettingError),
}

/// The current setting of one user and channel on this node when it is on. A node that has not
/// received a recent write yet shows the previous one.
#[derive(Debug, PartialEq)]
pub struct ReadSettingOperation {
    channel: SettingChannel,
    owner: UserId,
    state: ReadState,
}

impl ReadSettingOperation {
    pub fn new(channel: SettingChannel, owner: UserId) -> Self {
        Self {
            channel,
            owner,
            state: ReadState::Init,
        }
    }
}

impl Operation for ReadSettingOperation {
    type Output = Option<Vec<u8>>;
    type Error = SettingError;

    fn start(&mut self) -> Effects {
        self.state = ReadState::Scan(SettingScan::default());
        SettingScan::page(self.channel, self.owner, None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::take(&mut self.state), event) {
            (
                ReadState::Scan(mut scan),
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                scan.add(self.channel, values);
                if next_start_after.is_some() {
                    self.state = ReadState::Scan(scan);
                    return SettingScan::page(self.channel, self.owner, next_start_after);
                }
                let winner = current_setting(scan.rows, |(head, _)| *head);
                let on = winner.filter(|(head, _)| head.on).map(|(_, bytes)| bytes);
                self.state = ReadState::Done(on);
                smallvec![]
            }
            (_, event) => {
                self.state = ReadState::Failed(unexpected(event));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReadState::Done(_) | ReadState::Failed(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            ReadState::Done(bytes) => Ok(bytes),
            ReadState::Failed(error) => Err(error),
            _ => Err(SettingError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// What a new register write says: on with its secret, or off.
#[derive(Clone, PartialEq, Eq)]
pub enum SettingValue {
    /// Email for `categories` to the address in `address`; `None` turns email off.
    Email {
        categories: BTreeSet<String>,
        address: Option<SecretBytes>,
    },
    /// Signed deliveries to `url` with `secret`; `None` turns the webhook off.
    Webhook {
        target: Option<(String, SecretBytes)>,
    },
}

/// Shows neither the address nor the secret.
impl std::fmt::Debug for SettingValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SettingValue::Email { address, .. } => f
                .debug_struct("Email")
                .field("on", &address.is_some())
                .finish_non_exhaustive(),
            SettingValue::Webhook { target } => f
                .debug_struct("Webhook")
                .field("on", &target.is_some())
                .finish_non_exhaustive(),
        }
    }
}

impl SettingValue {
    fn channel(&self) -> SettingChannel {
        match self {
            SettingValue::Email { .. } => SettingChannel::Email,
            SettingValue::Webhook { .. } => SettingChannel::Webhook,
        }
    }
}

/// One new register write; ids and time come from the caller.
#[derive(Debug, PartialEq)]
pub struct SettingWrite {
    pub owner: UserId,
    pub local: NodeId,
    pub write_id: Ulid,
    pub event_id: Ulid,
    pub now_ms: u64,
    pub value: SettingValue,
}

impl SettingWrite {
    /// The stored bytes of this write with `stamp`; a secret is sealed to the realm key.
    fn bytes(&self, stamp: RegisterStamp) -> Result<Vec<u8>, SettingError> {
        let owner = self.owner;
        let seal = |secret: &SecretBytes| {
            let channel = self.value.channel();
            seal_setting(&owner.realm_id, channel, &owner, stamp.write_id, secret)
                .map_err(|error| SettingError::Build(error.to_string()))
        };
        let encoded = match &self.value {
            SettingValue::Email {
                categories,
                address,
            } => EmailPreference {
                preference_id: stamp.write_id,
                user_id: owner,
                stamp,
                categories: categories.clone(),
                sealed: address.as_ref().map(seal).transpose()?,
                created_at_ms: self.now_ms,
            }
            .to_bytes(),
            SettingValue::Webhook { target } => {
                let sealed = target
                    .as_ref()
                    .map(|(_, secret)| seal(secret))
                    .transpose()?;
                WebhookSubscription {
                    subscription_id: stamp.write_id,
                    user_id: owner,
                    stamp,
                    url: target.as_ref().map(|(url, _)| url.clone()),
                    sealed,
                    created_at_ms: self.now_ms,
                }
                .to_bytes()
            }
        };
        encoded.map_err(|error| SettingError::Build(error.to_string()))
    }

    /// The document row, its revision and the sync outbox upsert, written in one batch.
    fn rows(&self, write_id: Ulid, bytes: &[u8]) -> Result<Effect, SettingError> {
        let storage = |error: String| SettingError::Storage(error);
        let target = self.value.channel().target(self.owner, write_id);
        let change = DocumentChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: self.event_id,
                actor: self.local,
                updated_at_ms: self.now_ms,
            },
            kind: DocumentChangeKind::Upsert,
            placement: PlacementRef::NIL,
        };
        let revision = sync_revision_entry(&target, &change).map_err(|e| storage(e.to_string()))?;
        let outbox = new_identified_record(
            change.current.event_id,
            self.local,
            target.clone(),
            Vec::new(),
            DocumentOutboxEvent::Upsert {
                bytes: bytes.to_vec(),
                change,
            },
            PlacementRef::NIL,
            false,
        );
        let outbox = outbox_write_entry(&outbox).map_err(|e| storage(e.to_string()))?;
        let row = (
            target.storage_keyspace().to_string(),
            target.storage_key(),
            bytes.to_vec().into(),
        );
        Ok(Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![row, revision, outbox],
            txn_id: None,
        }))
    }
}

#[derive(Debug, Default, PartialEq)]
enum WriteState {
    #[default]
    Init,
    Scan(SettingScan),
    Store(Vec<u8>),
    Schedule(Vec<u8>),
    Done(Vec<u8>),
    Failed(SettingError),
}

/// Writes a new register write on this node and returns its bytes. The counter is one above
/// the highest this node holds for the register; document sync replicates the write.
#[derive(Debug, PartialEq)]
pub struct WriteSettingOperation {
    write: SettingWrite,
    state: WriteState,
}

impl WriteSettingOperation {
    pub fn new(write: SettingWrite) -> Self {
        Self {
            write,
            state: WriteState::Init,
        }
    }

    fn store(&mut self, scan: SettingScan) -> Effects {
        let held = scan.rows.iter().map(|(head, _)| head.stamp.counter).max();
        let built = RegisterStamp::next(held, self.write.local, self.write.write_id)
            .map_err(SettingError::from)
            .and_then(|stamp| Ok((stamp, self.write.bytes(stamp)?)))
            .and_then(|(stamp, bytes)| Ok((self.write.rows(stamp.write_id, &bytes)?, bytes)));
        match built {
            Ok((rows, bytes)) => {
                self.state = WriteState::Store(bytes);
                smallvec![rows]
            }
            Err(error) => {
                self.state = WriteState::Failed(error);
                smallvec![]
            }
        }
    }
}

impl Operation for WriteSettingOperation {
    type Output = Vec<u8>;
    type Error = SettingError;

    fn start(&mut self) -> Effects {
        self.state = WriteState::Scan(SettingScan::default());
        SettingScan::page(self.write.value.channel(), self.write.owner, None)
    }

    fn step(&mut self, event: Event) -> Effects {
        let channel = self.write.value.channel();
        match (std::mem::take(&mut self.state), event) {
            (
                WriteState::Scan(mut scan),
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                scan.add(channel, values);
                if next_start_after.is_some() {
                    self.state = WriteState::Scan(scan);
                    return SettingScan::page(channel, self.write.owner, next_start_after);
                }
                self.store(scan)
            }
            (WriteState::Store(bytes), Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.state = WriteState::Schedule(bytes);
                smallvec![schedule_drain_effect()]
            }
            // The write is stored; a failed schedule waits for the drain's own re-arm.
            (WriteState::Schedule(bytes), Event::Task(_)) => {
                self.state = WriteState::Done(bytes);
                smallvec![]
            }
            (_, event) => {
                self.state = WriteState::Failed(unexpected(event));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, WriteState::Done(_) | WriteState::Failed(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            WriteState::Done(bytes) => Ok(bytes),
            WriteState::Failed(error) => Err(error),
            _ => Err(SettingError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
