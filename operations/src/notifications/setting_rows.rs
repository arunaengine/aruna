//! Reads the current notification setting of a user and writes a new register write of it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SecretBytes;
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentSyncRevision,
};
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::realm_vault::seal_setting;
use aruna_core::register::{RegisterError, RegisterStamp};
use aruna_core::storage_entries::sync_revision_entry;
use aruna_core::structs::execution::email::EmailPreference;
use aruna_core::structs::execution::setting::{SettingChannel, SettingHead, current_setting};
use aruna_core::structs::execution::webhook::WebhookSubscription;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::{Key, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::notifications::unexpected;
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
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Rows read per page from a settings keyspace.
const SETTING_PAGE: usize = 256;

/// The register writes of one user and channel read so far, with their heads.
#[derive(Debug, Default, PartialEq)]
struct SettingScan {
    rows: Vec<(SettingHead, Vec<u8>)>,
}

impl SettingScan {
    fn page(channel: SettingChannel, owner: UserId, after: Option<Key>) -> IoCall {
        IoCall::Page {
            space: channel.keyspace().to_string(),
            prefix: owner.to_bytes().into(),
            after,
            limit: SETTING_PAGE,
        }
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

/// The current setting of one user and channel on this node when it is on. A node that has not
/// received a recent write yet shows the previous one.
#[derive(Debug, PartialEq)]
pub struct ReadSettingOperation {
    channel: SettingChannel,
    owner: UserId,
    scan: SettingScan,
}

impl ReadSettingOperation {
    pub fn new(channel: SettingChannel, owner: UserId) -> Self {
        Self {
            channel,
            owner,
            scan: SettingScan::default(),
        }
    }
}

impl Operation for ReadSettingOperation {
    type Output = Option<Vec<u8>>;
    type Error = SettingError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        Ok(Next::Call(SettingScan::page(
            self.channel,
            self.owner,
            None,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let IoResult::Page { rows, next } = result else {
            return Err(unexpected("IoResult::Page", &result).into());
        };
        self.scan.add(self.channel, rows);
        if next.is_some() {
            return Ok(Next::Call(SettingScan::page(
                self.channel,
                self.owner,
                next,
            )));
        }
        let rows = std::mem::take(&mut self.scan.rows);
        let winner = current_setting(rows, |(head, _)| *head);
        Ok(Next::Done(
            winner.filter(|(head, _)| head.on).map(|(_, bytes)| bytes),
        ))
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
    fn rows(&self, write_id: Ulid, bytes: &[u8]) -> Result<IoCall, SettingError> {
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
        Ok(IoCall::Write(vec![row, revision, outbox]))
    }
}

#[derive(Debug, Default, PartialEq)]
enum WriteState {
    #[default]
    Scan,
    Store(Vec<u8>),
    Schedule(Vec<u8>),
}

/// Writes a new register write on this node and returns its bytes. The counter is one above
/// the highest this node holds for the register; document sync replicates the write.
#[derive(Debug, PartialEq)]
pub struct WriteSettingOperation {
    write: SettingWrite,
    scan: SettingScan,
    state: WriteState,
}

impl WriteSettingOperation {
    pub fn new(write: SettingWrite) -> Self {
        Self {
            write,
            scan: SettingScan::default(),
            state: WriteState::Scan,
        }
    }

    fn store(&mut self) -> Result<Next<Vec<u8>>, SettingError> {
        let held = self
            .scan
            .rows
            .iter()
            .map(|(head, _)| head.stamp.counter)
            .max();
        let stamp = RegisterStamp::next(held, self.write.local, self.write.write_id)?;
        let bytes = self.write.bytes(stamp)?;
        let rows = self.write.rows(stamp.write_id, &bytes)?;
        self.state = WriteState::Store(bytes);
        Ok(Next::Call(rows))
    }
}

impl Operation for WriteSettingOperation {
    type Output = Vec<u8>;
    type Error = SettingError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        let channel = self.write.value.channel();
        Ok(Next::Call(SettingScan::page(
            channel,
            self.write.owner,
            None,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let channel = self.write.value.channel();
        match (std::mem::take(&mut self.state), result) {
            (WriteState::Scan, IoResult::Page { rows, next }) => {
                self.scan.add(channel, rows);
                match next {
                    Some(_) => Ok(Next::Call(SettingScan::page(
                        channel,
                        self.write.owner,
                        next,
                    ))),
                    None => self.store(),
                }
            }
            (WriteState::Store(bytes), IoResult::Done) => {
                self.state = WriteState::Schedule(bytes);
                Ok(Next::Call(IoCall::from(schedule_drain_effect())))
            }
            // The write is stored; a failed schedule waits for the drain's own re-arm.
            (WriteState::Schedule(bytes), IoResult::Event(event))
                if matches!(*event, Event::Task(_)) =>
            {
                Ok(Next::Done(bytes))
            }
            (_, other) => Err(unexpected("a setting scan, write or schedule", &other).into()),
        }
    }
}
