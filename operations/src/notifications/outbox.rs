//! Drains the notification outbox: delivers each record to its recipient's inbox holder, here or
//! on a peer, and deletes the delivered rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, NetEffect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::{NOTIFICATION_OUTBOX_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::{NotificationOutboxRecord, NotificationRecord};
use aruna_core::structs::execution::notification_transport::{
    NotificationRequest, NotificationTransportMessage,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::notifications::enqueue::EnqueueDeliveriesOperation;
use crate::notifications::inbox::UpsertInboxOperation;
use crate::notifications::placement::resolve_inbox_holder;

pub const OUTBOX_BATCH_SIZE: usize = 512;
pub const DELIVERY_RETRY_AFTER: Duration = Duration::from_secs(30);
pub const OUTBOX_RETENTION_MS: u64 = 48 * 60 * 60 * 1000;

#[derive(Debug, Error, PartialEq)]
pub enum DrainError {
    #[error("unexpected event in {state}: {got}")]
    UnexpectedEvent { state: &'static str, got: String },
    #[error("the notification outbox drain did not finish")]
    NotFinished,
}

/// One page of outbox rows on its way to the holders.
#[derive(Debug, Default, PartialEq)]
struct Page {
    has_more: bool,
    records: Vec<(Key, NotificationOutboxRecord)>,
    local_records: Vec<NotificationRecord>,
    local_keys: Vec<Key>,
    remote: VecDeque<(NodeId, Vec<NotificationRecord>, Vec<Key>)>,
}

#[derive(Debug, Default, PartialEq)]
enum DrainState {
    #[default]
    Init,
    Snapshot,
    Page,
    Expire,
    Configs {
        realms: Vec<RealmId>,
    },
    Upsert(Box<UpsertInboxOperation>),
    Wake {
        held: Vec<NotificationRecord>,
    },
    Enqueue(Box<EnqueueDeliveriesOperation>),
    DeleteLocal,
    Remote {
        holder: NodeId,
        keys: Vec<Key>,
    },
    DeleteRemote,
    /// Waits for the cleanup of a failed inbox upsert or delivery queue, then goes on.
    Cleanup {
        left: usize,
    },
    Close,
    Head,
    Done(Option<Duration>),
    Failed(DrainError),
}

impl DrainState {
    fn name(&self) -> &'static str {
        match self {
            DrainState::Init => "init",
            DrainState::Snapshot => "snapshot",
            DrainState::Page => "page",
            DrainState::Expire => "expire",
            DrainState::Configs { .. } => "realm configs",
            DrainState::Upsert(_) => "inbox upsert",
            DrainState::Wake { .. } => "wake",
            DrainState::Enqueue(_) => "delivery queue",
            DrainState::DeleteLocal => "local delete",
            DrainState::Remote { .. } => "remote delivery",
            DrainState::DeleteRemote => "remote delete",
            DrainState::Cleanup { .. } => "cleanup",
            DrainState::Close => "close",
            DrainState::Head => "head",
            DrainState::Done(_) => "done",
            DrainState::Failed(_) => "failed",
        }
    }
}

/// One drain run over a read snapshot of the outbox, page by page, so a dead holder cannot hide
/// healthy records behind it while rows appended during the run wait for the next run. Records
/// for this node go into the local inbox and queue their deliveries; the others go to their
/// holder, one attempt per holder and run. Rows stay until delivered; expired ones are dropped.
/// The output is when the drain runs again.
#[derive(Debug, PartialEq)]
pub struct DrainOutboxOperation {
    local: Option<NodeId>,
    now_ms: u64,
    state: DrainState,
    snapshot: Option<TxnId>,
    start_after: Option<Key>,
    configs: BTreeMap<RealmId, Option<RealmConfigDocument>>,
    failed_holders: BTreeSet<NodeId>,
    retry: bool,
    page: Page,
}

impl DrainOutboxOperation {
    pub fn new(local: Option<NodeId>, now_ms: u64) -> Self {
        Self {
            local,
            now_ms,
            state: DrainState::Init,
            snapshot: None,
            start_after: None,
            configs: BTreeMap::new(),
            failed_holders: BTreeSet::new(),
            retry: false,
            page: Page::default(),
        }
    }

    fn done(&mut self, rearm: Option<Duration>) -> Effects {
        self.state = DrainState::Done(rearm);
        smallvec![]
    }

    fn read_page(&mut self) -> Effects {
        self.state = DrainState::Page;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: self.start_after.clone().map(IterStart::After),
            limit: OUTBOX_BATCH_SIZE + 1,
            txn_id: self.snapshot,
        })]
    }

    fn delete(&self, keys: Vec<Key>) -> Effects {
        let space = || NOTIFICATION_OUTBOX_KEYSPACE.to_string();
        let deletes = keys.into_iter().map(|key| (space(), key)).collect();
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: None
        })]
    }

    fn page_read(&mut self, values: Vec<(Key, Value)>) -> Effects {
        let (records, has_more, next) = decode_page(values, OUTBOX_BATCH_SIZE);
        self.start_after = next;
        if records.is_empty() {
            return match has_more && self.start_after.is_some() {
                true => self.read_page(),
                false => self.close(),
            };
        }
        let mut expired = Vec::new();
        let mut kept = Vec::with_capacity(records.len());
        for (key, record) in records {
            let age_ms = self.now_ms.saturating_sub(record.outbox_id.timestamp_ms());
            if age_ms > OUTBOX_RETENTION_MS {
                warn!(outbox_id = %record.outbox_id, age_ms, "Dropping expired notification outbox record");
                expired.push(key);
            } else {
                kept.push((key, record));
            }
        }
        self.page = Page {
            has_more,
            records: kept,
            ..Page::default()
        };
        if expired.is_empty() {
            return self.read_configs();
        }
        self.state = DrainState::Expire;
        self.delete(expired)
    }

    fn read_configs(&mut self) -> Effects {
        let realms: BTreeSet<RealmId> = (self.page.records.iter())
            .map(|(_, record)| record.record.recipient.realm_id)
            .filter(|realm_id| !self.configs.contains_key(realm_id))
            .collect();
        if realms.is_empty() {
            return self.group();
        }
        let realms: Vec<RealmId> = realms.into_iter().collect();
        let reads = (realms.iter())
            .map(|realm_id| (REALM_CONFIG_KEYSPACE.to_string(), Key::from(realm_id.as_bytes().to_vec())))
            .collect();
        self.state = DrainState::Configs { realms };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None
        })]
    }

    fn configs_read(&mut self, realms: Vec<RealmId>, values: Vec<(Key, Option<Value>)>) -> Effects {
        for (realm_id, (_, value)) in realms.into_iter().zip(values) {
            let config = value.and_then(|bytes| match RealmConfigDocument::from_bytes(&bytes) {
                Ok(config) => Some(config),
                Err(error) => {
                    warn!(realm_id = %realm_id, %error, "Failed to decode realm config for notification drain");
                    None
                }
            });
            self.configs.insert(realm_id, config);
        }
        self.group()
    }

}
