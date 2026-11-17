//! Drains the notification outbox: delivers each record to its recipient's inbox holder, here or
//! on a peer, and deletes the delivered rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::{NOTIFICATION_OUTBOX_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::{NotificationOutboxRecord, NotificationRecord};
use aruna_core::structs::execution::notification::{
    NotificationRequest, NotificationTransportMessage,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::notifications::enqueue::EnqueueDeliveriesOperation;
use crate::notifications::inbox::UpsertInboxOperation;
use crate::notifications::placement::resolve_inbox_holder;

pub(crate) const OUTBOX_BATCH_SIZE: usize = 512;
pub(crate) const DELIVERY_RETRY_AFTER: Duration = Duration::from_secs(30);
pub(crate) const OUTBOX_RETENTION_MS: u64 = 48 * 60 * 60 * 1000;

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

/// One drain run over a read snapshot, page by page, so a dead holder hides no healthy record.
/// Local records go to the inbox, others to their holder once per run; rows stay until
/// delivered, expired ones are dropped. The output is when the drain runs again.
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
            .map(|realm_id| {
                (
                    REALM_CONFIG_KEYSPACE.to_string(),
                    Key::from(realm_id.as_bytes().to_vec()),
                )
            })
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

    /// Splits the page into the local batch and one batch per remote holder.
    fn group(&mut self) -> Effects {
        let Some(local) = self.local else {
            return self.close();
        };
        let mut remote: BTreeMap<NodeId, (Vec<NotificationRecord>, Vec<Key>)> = BTreeMap::new();
        for (key, outbox) in std::mem::take(&mut self.page.records) {
            let record = outbox.record;
            let realm_id = record.recipient.realm_id;
            let Some(config) = self.configs.get(&realm_id).and_then(Option::as_ref) else {
                warn!(realm_id = %realm_id, "Notification realm config unavailable; retrying delivery");
                self.retry = true;
                continue;
            };
            let holder = match resolve_inbox_holder(&record.recipient, config) {
                Ok(Some(holder)) => holder,
                Ok(None) => {
                    warn!(recipient = %record.recipient, "No eligible notification inbox holder; retrying delivery");
                    self.retry = true;
                    continue;
                }
                Err(error) => {
                    warn!(recipient = %record.recipient, %error, "Failed to resolve notification inbox holder");
                    self.retry = true;
                    continue;
                }
            };
            if holder == local {
                self.page.local_records.push(record);
                self.page.local_keys.push(key);
            } else if self.failed_holders.contains(&holder) {
                self.retry = true;
            } else {
                let group = remote.entry(holder).or_default();
                group.0.push(record);
                group.1.push(key);
            }
        }
        self.page.remote = (remote.into_iter())
            .map(|(holder, (records, keys))| (holder, records, keys))
            .collect();
        if self.page.local_records.is_empty() {
            return self.next_remote();
        }
        let records = std::mem::take(&mut self.page.local_records);
        self.run_upsert(UpsertInboxOperation::new(records))
    }

    fn run_upsert(&mut self, mut upsert: UpsertInboxOperation) -> Effects {
        let effects = upsert.start();
        if !upsert.is_complete() {
            self.state = DrainState::Upsert(Box::new(upsert));
            return effects;
        }
        self.upserted(upsert, effects)
    }

    /// Goes on with the next remote batch once the `cleanup` of a failed child ran.
    fn after_cleanup(&mut self, cleanup: Effects) -> Effects {
        if cleanup.is_empty() {
            return self.next_remote();
        }
        self.state = DrainState::Cleanup {
            left: cleanup.len(),
        };
        cleanup
    }

    fn upserted(&mut self, upsert: UpsertInboxOperation, cleanup: Effects) -> Effects {
        match upsert.finalize() {
            Ok(outcome) if outcome.recipients.is_empty() => self.run_enqueue(outcome.held),
            Ok(outcome) => {
                self.state = DrainState::Wake { held: outcome.held };
                smallvec![Effect::Net(NetEffect::WakeInbox(outcome.recipients))]
            }
            Err(error) => {
                warn!(%error, "Failed to deliver notifications to local inbox");
                self.retry = true;
                self.after_cleanup(cleanup)
            }
        }
    }

    fn run_enqueue(&mut self, held: Vec<NotificationRecord>) -> Effects {
        let Some(local) = self.local else {
            return self.next_remote();
        };
        let config = (held.first())
            .and_then(|record| self.configs.get(&record.recipient.realm_id))
            .and_then(Option::as_ref);
        let mut enqueue = EnqueueDeliveriesOperation::new(local, config, &held, self.now_ms);
        let effects = enqueue.start();
        if !enqueue.is_complete() {
            self.state = DrainState::Enqueue(Box::new(enqueue));
            return effects;
        }
        self.enqueued(enqueue, effects)
    }

    /// The outbox rows stay until their deliveries are queued.
    fn enqueued(&mut self, enqueue: EnqueueDeliveriesOperation, cleanup: Effects) -> Effects {
        if let Err(error) = enqueue.finalize() {
            warn!(%error, "Failed to queue notification deliveries");
            self.retry = true;
            return self.after_cleanup(cleanup);
        }
        let keys = std::mem::take(&mut self.page.local_keys);
        self.state = DrainState::DeleteLocal;
        self.delete(keys)
    }

    fn next_remote(&mut self) -> Effects {
        let Some((holder, records, keys)) = self.page.remote.pop_front() else {
            return match self.page.has_more {
                true => self.read_page(),
                false => self.close(),
            };
        };
        self.state = DrainState::Remote { holder, keys };
        let message = NotificationTransportMessage::DeliverBatch { records };
        let request = NotificationRequest {
            node: holder,
            message,
        };
        smallvec![Effect::Net(NetEffect::Notification(Box::new(request)))]
    }

    fn answered(
        &mut self,
        holder: NodeId,
        keys: Vec<Key>,
        answer: Result<Box<NotificationTransportMessage>, String>,
    ) -> Effects {
        let error = match answer.map(|answer| *answer) {
            Ok(NotificationTransportMessage::DeliverAck { .. }) => {
                self.state = DrainState::DeleteRemote;
                return self.delete(keys);
            }
            Ok(NotificationTransportMessage::Reject(reason)) | Err(reason) => reason,
            Ok(_) => "unexpected notification response".to_string(),
        };
        warn!(holder = %holder, %error, "Failed to deliver notifications to remote holder");
        self.failed_holders.insert(holder);
        self.retry = true;
        self.next_remote()
    }

    fn deleted(&mut self, event: &Event) {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            warn!(%error, "Failed to delete notification outbox records");
            self.retry = true;
        }
    }

    fn close(&mut self) -> Effects {
        let Some(txn_id) = self.snapshot.take() else {
            return self.after_close();
        };
        self.state = DrainState::Close;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    /// A clean run checks for rows appended meanwhile; a failed one runs again later.
    fn after_close(&mut self) -> Effects {
        if self.retry {
            return self.done(Some(DELIVERY_RETRY_AFTER));
        }
        self.state = DrainState::Head;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 2,
            txn_id: None,
        })]
    }

    fn unexpected(&mut self, state: DrainState, event: Event) -> Effects {
        let error = DrainError::UnexpectedEvent {
            state: state.name(),
            got: format!("{event:?}"),
        };
        self.state = state;
        let cleanup = self.abort();
        self.state = DrainState::Failed(error);
        cleanup
    }
}

/// Decodes one page read with one row more than `limit`; a malformed row stays for recovery.
fn decode_page(
    values: Vec<(Key, Value)>,
    limit: usize,
) -> (Vec<(Key, NotificationOutboxRecord)>, bool, Option<Key>) {
    let has_more = values.len() > limit;
    let page: Vec<_> = values.into_iter().take(limit).collect();
    let next = page.last().map(|(key, _)| key.clone());
    let records = (page.into_iter())
        .filter_map(|(key, value)| match NotificationOutboxRecord::from_bytes(&value) {
            Ok(record) => Some((key, record)),
            Err(error) => {
                warn!(%error, key = ?key.as_ref(), "Retaining malformed notification outbox record for recovery");
                None
            }
        })
        .collect();
    (records, has_more, next)
}

/// When the drain runs again after a clean run: at once while records wait, later while only
/// malformed rows wait, never while the outbox is empty.
fn head_rearm(values: Vec<(Key, Value)>) -> Option<Duration> {
    let (records, has_more, _) = decode_page(values, 1);
    if !records.is_empty() {
        Some(Duration::ZERO)
    } else if has_more {
        Some(DELIVERY_RETRY_AFTER)
    } else {
        None
    }
}

impl Operation for DrainOutboxOperation {
    type Output = Option<Duration>;
    type Error = DrainError;

    fn start(&mut self) -> Effects {
        if self.local.is_none() {
            warn!("Cannot drain notification outbox without net handle");
            return self.done(Some(DELIVERY_RETRY_AFTER));
        }
        self.state = DrainState::Snapshot;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = std::mem::take(&mut self.state);
        match (state, event) {
            (DrainState::Snapshot, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.snapshot = Some(txn_id);
                self.read_page()
            }
            (DrainState::Snapshot, Event::Storage(StorageEvent::Error { error })) => {
                warn!(%error, "Failed to start notification outbox snapshot");
                self.done(Some(DELIVERY_RETRY_AFTER))
            }
            (DrainState::Page, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.page_read(values)
            }
            (DrainState::Page, Event::Storage(StorageEvent::Error { error })) => {
                warn!(%error, "Failed to read notification outbox record");
                self.retry = true;
                self.close()
            }
            (
                DrainState::Expire,
                event @ Event::Storage(
                    StorageEvent::BatchDeleteResult { .. } | StorageEvent::Error { .. },
                ),
            ) => {
                self.deleted(&event);
                self.read_configs()
            }
            (
                DrainState::Configs { realms },
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => self.configs_read(realms, values),
            (DrainState::Configs { realms }, Event::Storage(StorageEvent::Error { error })) => {
                warn!(%error, "Failed to read realm config for notification drain");
                let values = realms
                    .iter()
                    .map(|_| (Key::from(Vec::new()), None))
                    .collect();
                self.configs_read(realms, values)
            }
            (DrainState::Upsert(mut upsert), event) => {
                let effects = upsert.step(event);
                if !upsert.is_complete() {
                    self.state = DrainState::Upsert(upsert);
                    return effects;
                }
                self.upserted(*upsert, effects)
            }
            (DrainState::Wake { held }, Event::Net(NetEvent::InboxWoken)) => self.run_enqueue(held),
            (DrainState::Enqueue(mut enqueue), event) => {
                let effects = enqueue.step(event);
                if !enqueue.is_complete() {
                    self.state = DrainState::Enqueue(enqueue);
                    return effects;
                }
                self.enqueued(*enqueue, effects)
            }
            (
                DrainState::DeleteLocal | DrainState::DeleteRemote,
                event @ Event::Storage(
                    StorageEvent::BatchDeleteResult { .. } | StorageEvent::Error { .. },
                ),
            ) => {
                self.deleted(&event);
                self.next_remote()
            }
            (DrainState::Cleanup { left }, _) if left > 1 => {
                self.state = DrainState::Cleanup { left: left - 1 };
                smallvec![]
            }
            (DrainState::Cleanup { .. }, _) => self.next_remote(),
            (DrainState::Remote { holder, keys }, Event::Net(NetEvent::Notification(answer))) => {
                self.answered(holder, keys, answer)
            }
            (DrainState::Close, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.after_close()
            }
            (DrainState::Close, Event::Storage(StorageEvent::Error { error })) => {
                warn!(%error, "Failed to close notification outbox snapshot");
                self.retry = true;
                self.after_close()
            }
            (DrainState::Head, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let rearm = head_rearm(values);
                self.done(rearm)
            }
            (DrainState::Head, Event::Storage(StorageEvent::Error { error })) => {
                warn!(%error, "Failed to check for notification outbox records appended during drain");
                self.done(Some(DELIVERY_RETRY_AFTER))
            }
            (state, event) => self.unexpected(state, event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, DrainState::Done(_) | DrainState::Failed(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            DrainState::Done(rearm) => Ok(rearm),
            DrainState::Failed(error) => Err(error),
            _ => Err(DrainError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        let mut effects = match &mut self.state {
            DrainState::Upsert(upsert) => upsert.abort(),
            DrainState::Enqueue(enqueue) => enqueue.abort(),
            _ => smallvec![],
        };
        if let Some(txn_id) = self.snapshot.take() {
            effects.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
        }
        effects
    }
}

/// Finds whether the outbox holds rows and, when asked to, arms the drain timer for them. It
/// only shortens a pending deadline, never pushes it later. The output is whether rows wait.
#[derive(Debug, PartialEq)]
pub struct RestoreOutboxOperation {
    shorten: Option<Duration>,
    state: RestoreState,
}

#[derive(Debug, Default, PartialEq)]
enum RestoreState {
    #[default]
    Init,
    Scan,
    Arm,
    Done(bool),
    Failed(DrainError),
}

impl RestoreOutboxOperation {
    /// Arms the drain timer within `after` when rows wait.
    pub fn shorten(after: Duration) -> Self {
        Self {
            shorten: Some(after),
            state: RestoreState::Init,
        }
    }

    /// Only reports whether rows wait; the caller decides how to arm the timer.
    pub fn probe() -> Self {
        Self {
            shorten: None,
            state: RestoreState::Init,
        }
    }
}

impl Operation for RestoreOutboxOperation {
    type Output = bool;
    type Error = DrainError;

    fn start(&mut self) -> Effects {
        self.state = RestoreState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::take(&mut self.state), event) {
            (RestoreState::Scan, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                match (values.is_empty(), self.shorten) {
                    (false, Some(after)) => {
                        self.state = RestoreState::Arm;
                        smallvec![Effect::Task(TaskEffect::ShortenTimer {
                            key: TaskKey::DrainNotificationOutbox,
                            after,
                        })]
                    }
                    (empty, _) => {
                        self.state = RestoreState::Done(!empty);
                        smallvec![]
                    }
                }
            }
            (RestoreState::Arm, Event::Task(TaskEvent::TimerScheduled { .. })) => {
                self.state = RestoreState::Done(true);
                smallvec![]
            }
            (state, event) => {
                self.state = RestoreState::Failed(DrainError::UnexpectedEvent {
                    state: match state {
                        RestoreState::Arm => "restore arm",
                        _ => "restore scan",
                    },
                    got: format!("{event:?}"),
                });
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RestoreState::Done(_) | RestoreState::Failed(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            RestoreState::Done(pending) => Ok(pending),
            RestoreState::Failed(error) => Err(error),
            _ => Err(DrainError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::{record, user};
    use aruna_core::UserId;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::storage_entries::outbox_write_entry;
    use aruna_core::structs::execution::notification::NotificationClass;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use ulid::Ulid;

    const NOW_MS: u64 = 1_700_000_000_000;
    const SNAPSHOT: TxnId = TxnId::from_parts(3, 3);

    fn outbox_row(record: NotificationRecord) -> (Key, Value, NotificationOutboxRecord) {
        let outbox = NotificationOutboxRecord {
            outbox_id: Ulid::from_parts(NOW_MS - 1_000, 1),
            record,
        };
        let (_, key, value) = outbox_write_entry(&outbox).expect("row encodes");
        (key, value, outbox)
    }

    /// A realm config whose only holder is `holder`.
    fn config_bytes(holder: NodeId) -> (RealmId, Value) {
        let realm_id = user(1, 2).realm_id;
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.ensure_node(holder, RealmNodeKind::Server);
        let actor = Actor {
            node_id: holder,
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        (
            realm_id,
            Value::from(config.to_bytes(&actor).expect("config encodes")),
        )
    }

    fn page_read(start: Option<Key>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(IterStart::After),
            limit: OUTBOX_BATCH_SIZE + 1,
            txn_id: Some(SNAPSHOT),
        })
    }

    fn started() -> DrainOutboxOperation {
        let mut operation = DrainOutboxOperation::new(Some(node_id(1)), NOW_MS);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: SNAPSHOT,
        }));
        operation
    }

    #[test]
    fn remote_success_trace() {
        let holder = node_id(2);
        let notice = record(user(1, 2), NotificationClass::Direct, NOW_MS);
        let (key, value, _) = outbox_row(notice.clone());
        let (realm_id, config) = config_bytes(holder);
        let config_key = Key::from(realm_id.as_bytes().to_vec());
        let mut operation = DrainOutboxOperation::new(Some(node_id(1)), NOW_MS);
        let request = NotificationRequest {
            node: holder,
            message: NotificationTransportMessage::DeliverBatch {
                records: vec![notice],
            },
        };
        check_trace(
            &mut operation,
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: true,
            })],
            vec![
                (
                    Event::Storage(StorageEvent::TransactionStarted { txn_id: SNAPSHOT }),
                    vec![page_read(None)],
                ),
                (
                    Event::Storage(StorageEvent::IterResult {
                        values: vec![(key.clone(), value)],
                        next_start_after: None,
                    }),
                    vec![Effect::Storage(StorageEffect::BatchRead {
                        reads: vec![(REALM_CONFIG_KEYSPACE.to_string(), config_key.clone())],
                        txn_id: None,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::BatchReadResult {
                        values: vec![(config_key, Some(config))],
                    }),
                    vec![Effect::Net(NetEffect::Notification(Box::new(request)))],
                ),
                (
                    Event::Net(NetEvent::Notification(Ok(Box::new(
                        NotificationTransportMessage::DeliverAck { written: 1 },
                    )))),
                    vec![Effect::Storage(StorageEffect::BatchDelete {
                        deletes: vec![(NOTIFICATION_OUTBOX_KEYSPACE.to_string(), key)],
                        txn_id: None,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::BatchDeleteResult {
                        entries: Vec::new(),
                    }),
                    vec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: SNAPSHOT,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::TransactionCommitted { txn_id: SNAPSHOT }),
                    vec![Effect::Storage(StorageEffect::Iter {
                        key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                        prefix: None,
                        start: None,
                        limit: 2,
                        txn_id: None,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::IterResult {
                        values: Vec::new(),
                        next_start_after: None,
                    }),
                    vec![],
                ),
            ],
        );
        assert_eq!(operation.finalize(), Ok(None));
    }

    #[test]
    fn rejected_holder_retries() {
        let holder = node_id(2);
        let (key, value, _) = outbox_row(record(user(1, 2), NotificationClass::Direct, NOW_MS));
        let (realm_id, config) = config_bytes(holder);
        let mut operation = started();
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(key, value)],
            next_start_after: None,
        }));
        let config_key = Key::from(realm_id.as_bytes().to_vec());
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(config_key, Some(config))],
        }));
        let effects = operation.step(Event::Net(NetEvent::Notification(Ok(Box::new(
            NotificationTransportMessage::Reject("not the holder".to_string()),
        )))));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction {
                txn_id: SNAPSHOT
            })]
        );
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: SNAPSHOT,
        }));
        assert_eq!(operation.finalize(), Ok(Some(DELIVERY_RETRY_AFTER)));
    }

    #[test]
    fn expired_rows_dropped() {
        let notice = record(user(1, 2), NotificationClass::Direct, NOW_MS);
        let (key, value, _) = outbox_row(notice);
        let mut operation =
            DrainOutboxOperation::new(Some(node_id(1)), NOW_MS + OUTBOX_RETENTION_MS + 10_000);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: SNAPSHOT,
        }));
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(key.clone(), value)],
            next_start_after: None,
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete {
                deletes: vec![(NOTIFICATION_OUTBOX_KEYSPACE.to_string(), key)],
                txn_id: None,
            })]
        );
    }

    #[test]
    fn malformed_rows_kept() {
        let mut operation = started();
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(Key::from(vec![1]), Value::from(vec![1, 2, 3]))],
            next_start_after: None,
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction {
                txn_id: SNAPSHOT
            })]
        );
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: SNAPSHOT,
        }));
        let rearm = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                (Key::from(vec![1]), Value::from(vec![1, 2, 3])),
                (Key::from(vec![2]), Value::from(vec![1, 2, 3])),
            ],
            next_start_after: None,
        }));
        assert!(rearm.is_empty());
        assert_eq!(operation.finalize(), Ok(Some(DELIVERY_RETRY_AFTER)));
    }

    #[test]
    fn storage_errors_retry() {
        let mut snapshot = DrainOutboxOperation::new(Some(node_id(1)), NOW_MS);
        snapshot.start();
        snapshot.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(snapshot.finalize(), Ok(Some(DELIVERY_RETRY_AFTER)));

        let mut page = started();
        let effects = page.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction {
                txn_id: SNAPSHOT
            })]
        );
        page.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: SNAPSHOT,
        }));
        assert_eq!(page.finalize(), Ok(Some(DELIVERY_RETRY_AFTER)));
    }

    #[test]
    fn no_transport_retries() {
        let mut operation = DrainOutboxOperation::new(None, NOW_MS);
        assert!(operation.start().is_empty());
        assert_eq!(operation.finalize(), Ok(Some(DELIVERY_RETRY_AFTER)));
    }

    #[test]
    fn unexpected_events_fail() {
        let expected = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        };
        let refusals = rejects_events(started, sample_events, expected);
        assert!(!refusals.is_empty());
        for (_, effects, _) in refusals {
            assert_eq!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::AbortTransaction {
                    txn_id: SNAPSHOT
                })]
            );
        }
    }

    #[test]
    fn abort_per_phase() {
        let mut fresh = DrainOutboxOperation::new(Some(node_id(1)), NOW_MS);
        fresh.start();
        check_abort(&mut fresh, &[]);
        check_abort(
            &mut started(),
            &[Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: SNAPSHOT,
            })],
        );
    }

    #[test]
    fn unfinished_drain_fails() {
        rejects_finalize(started());
        rejects_finalize(RestoreOutboxOperation::probe());
    }

    #[test]
    fn restore_arms_pending() {
        let row = (Key::from(vec![1]), Value::from(vec![1]));
        let arm = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::DrainNotificationOutbox,
            after: Duration::ZERO,
        });
        let mut shorten = RestoreOutboxOperation::shorten(Duration::ZERO);
        check_trace(
            &mut shorten,
            &[Effect::Storage(StorageEffect::Iter {
                key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1,
                txn_id: None,
            })],
            vec![
                (
                    Event::Storage(StorageEvent::IterResult {
                        values: vec![row],
                        next_start_after: None,
                    }),
                    vec![arm],
                ),
                (
                    Event::Task(TaskEvent::TimerScheduled {
                        key: TaskKey::DrainNotificationOutbox,
                        after: Duration::ZERO,
                    }),
                    vec![],
                ),
            ],
        );
        assert_eq!(shorten.finalize(), Ok(true));

        let mut empty = RestoreOutboxOperation::shorten(Duration::ZERO);
        empty.start();
        empty.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        assert_eq!(empty.finalize(), Ok(false));

        let mut failed = RestoreOutboxOperation::probe();
        failed.start();
        failed.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert!(failed.finalize().is_err());
    }
}
