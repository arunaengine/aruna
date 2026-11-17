//! Drains the notification outbox: delivers each record to its recipient's inbox holder, here or
//! on a peer, and deletes the delivered rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::{NOTIFICATION_OUTBOX_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::{
    InboxWriteOutcome, NotificationOutboxRecord, NotificationRecord,
};
use aruna_core::structs::execution::notification::{
    NotificationRequest, NotificationTransportMessage,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, Value};
use tracing::warn;

use crate::notifications::enqueue::{EnqueueDeliveriesOperation, EnqueueError};
use crate::notifications::inbox::{InboxError, UpsertInboxOperation};
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::{unexpected, unexpected_event};

pub(crate) const OUTBOX_BATCH_SIZE: usize = 512;
pub(crate) const DELIVERY_RETRY_AFTER: Duration = Duration::from_secs(30);
pub(crate) const OUTBOX_RETENTION_MS: u64 = 48 * 60 * 60 * 1000;

/// One page of outbox rows on its way to the holders.
#[derive(Debug, Default, PartialEq)]
struct Page {
    has_more: bool,
    records: Vec<(Key, NotificationOutboxRecord)>,
    local_keys: Vec<Key>,
    remote: VecDeque<(NodeId, Vec<NotificationRecord>, Vec<Key>)>,
}

#[derive(Debug, Default, PartialEq)]
enum DrainState {
    #[default]
    Page,
    Expire,
    Configs {
        realms: Vec<RealmId>,
    },
    Upsert,
    Wake {
        held: Vec<NotificationRecord>,
    },
    Enqueue,
    DeleteLocal,
    Remote {
        holder: NodeId,
        keys: Vec<Key>,
    },
    DeleteRemote,
    Head,
}

type Turn = Result<Next<Option<Duration>>, IoError>;

/// One drain run, page by page after a cursor, so a dead holder hides no healthy record. Local
/// records go to the inbox, others to their holder once per run; rows stay until delivered,
/// expired ones are dropped. The output is when the drain runs again.
#[derive(Debug, PartialEq)]
pub struct DrainOutboxOperation {
    local: Option<NodeId>,
    now_ms: u64,
    state: DrainState,
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
            state: DrainState::Page,
            start_after: None,
            configs: BTreeMap::new(),
            failed_holders: BTreeSet::new(),
            retry: false,
            page: Page::default(),
        }
    }

    fn scan(&mut self, state: DrainState, after: Option<Key>, limit: usize) -> Turn {
        self.state = state;
        Ok(Next::Call(IoCall::Page {
            space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit,
        }))
    }

    fn read_page(&mut self) -> Turn {
        let after = self.start_after.clone();
        self.scan(DrainState::Page, after, OUTBOX_BATCH_SIZE + 1)
    }

    fn delete(&mut self, state: DrainState, keys: Vec<Key>) -> Turn {
        self.state = state;
        let space = || NOTIFICATION_OUTBOX_KEYSPACE.to_string();
        let deletes = keys.into_iter().map(|key| (space(), key)).collect();
        // Sent as an effect, so a failed delete comes back as an event and the run goes on.
        let delete = StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        };
        Ok(Next::Call(IoCall::from(Effect::Storage(delete))))
    }

    /// Logs a failed delete: the rows stay and the drain runs again later.
    fn deleted(&mut self, event: Event) -> Result<(), IoError> {
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(%error, "Failed to delete notification outbox records");
                self.retry = true;
                Ok(())
            }
            other => Err(unexpected_event("a delete result", &other)),
        }
    }

    fn page_read(&mut self, rows: Vec<(Key, Value)>) -> Turn {
        let (records, has_more, next) = decode_page(rows, OUTBOX_BATCH_SIZE);
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
        self.delete(DrainState::Expire, expired)
    }

    fn read_configs(&mut self) -> Turn {
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
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn configs_read(&mut self, realms: Vec<RealmId>, values: Vec<Option<Value>>) -> Turn {
        for (realm_id, value) in realms.into_iter().zip(values) {
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
    fn group(&mut self) -> Turn {
        let Some(local) = self.local else {
            return self.close();
        };
        let mut local_records = Vec::new();
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
                local_records.push(record);
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
        if local_records.is_empty() {
            return self.next_remote();
        }
        self.state = DrainState::Upsert;
        Ok(Next::Call(IoCall::child(UpsertInboxOperation::new(
            local_records,
        ))))
    }

    fn run_enqueue(&mut self, held: Vec<NotificationRecord>) -> Turn {
        let Some(local) = self.local else {
            return self.next_remote();
        };
        let config = (held.first())
            .and_then(|record| self.configs.get(&record.recipient.realm_id))
            .and_then(Option::as_ref);
        let enqueue = EnqueueDeliveriesOperation::new(local, config, &held, self.now_ms);
        self.state = DrainState::Enqueue;
        Ok(Next::Call(IoCall::child(enqueue)))
    }

    /// The outbox rows stay until their deliveries are queued.
    fn enqueued(&mut self, result: Result<(), EnqueueError>) -> Turn {
        match result {
            Ok(()) => {
                let keys = std::mem::take(&mut self.page.local_keys);
                self.delete(DrainState::DeleteLocal, keys)
            }
            Err(error) => {
                warn!(%error, "Failed to queue notification deliveries");
                self.retry = true;
                self.next_remote()
            }
        }
    }

    fn next_remote(&mut self) -> Turn {
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
        let effect = Effect::Net(NetEffect::Notification(Box::new(request)));
        Ok(Next::Call(IoCall::from(effect)))
    }

    fn answered(
        &mut self,
        holder: NodeId,
        keys: Vec<Key>,
        answer: Result<Box<NotificationTransportMessage>, String>,
    ) -> Turn {
        let error = match answer.map(|answer| *answer) {
            Ok(NotificationTransportMessage::DeliverAck { .. }) => {
                return self.delete(DrainState::DeleteRemote, keys);
            }
            Ok(NotificationTransportMessage::Reject(reason)) | Err(reason) => reason,
            Ok(_) => "unexpected notification response".to_string(),
        };
        warn!(holder = %holder, %error, "Failed to deliver notifications to remote holder");
        self.failed_holders.insert(holder);
        self.retry = true;
        self.next_remote()
    }

    /// A clean run checks for rows appended meanwhile; a failed one runs again later.
    fn close(&mut self) -> Turn {
        if self.retry {
            return Ok(Next::Done(Some(DELIVERY_RETRY_AFTER)));
        }
        self.scan(DrainState::Head, None, 2)
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
    type Error = IoError;

    fn first(&mut self) -> Turn {
        if self.local.is_none() {
            warn!("Cannot drain notification outbox without net handle");
            return Ok(Next::Done(Some(DELIVERY_RETRY_AFTER)));
        }
        self.read_page()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match (std::mem::take(&mut self.state), result) {
            (DrainState::Page, IoResult::Page { rows, .. }) => self.page_read(rows),
            (DrainState::Expire, IoResult::Event(event)) => {
                self.deleted(*event)?;
                self.read_configs()
            }
            (DrainState::Configs { realms }, IoResult::Values(values)) => {
                self.configs_read(realms, values)
            }
            (DrainState::Upsert, result) => {
                match result.child::<InboxWriteOutcome, InboxError>()? {
                    Ok(outcome) if outcome.recipients.is_empty() => self.run_enqueue(outcome.held),
                    Ok(outcome) => {
                        self.state = DrainState::Wake { held: outcome.held };
                        let wake = Effect::Net(NetEffect::WakeInbox(outcome.recipients));
                        Ok(Next::Call(IoCall::from(wake)))
                    }
                    Err(error) => {
                        warn!(%error, "Failed to deliver notifications to local inbox");
                        self.retry = true;
                        self.next_remote()
                    }
                }
            }
            (DrainState::Wake { held }, IoResult::Event(event))
                if matches!(*event, Event::Net(NetEvent::InboxWoken)) =>
            {
                self.run_enqueue(held)
            }
            (DrainState::Enqueue, result) => self.enqueued(result.child::<(), EnqueueError>()?),
            (DrainState::DeleteLocal | DrainState::DeleteRemote, IoResult::Event(event)) => {
                self.deleted(*event)?;
                self.next_remote()
            }
            (DrainState::Remote { holder, keys }, IoResult::Event(event)) => match *event {
                Event::Net(NetEvent::Notification(answer)) => self.answered(holder, keys, answer),
                other => Err(unexpected_event("NetEvent::Notification", &other)),
            },
            (DrainState::Head, IoResult::Page { rows, .. }) => Ok(Next::Done(head_rearm(rows))),
            (_, other) => Err(unexpected("an outbox drain result", &other)),
        }
    }
}

/// Finds whether the outbox holds rows and, when asked to, arms the drain timer for them. It
/// only shortens a pending deadline, never pushes it later. The output is whether rows wait.
#[derive(Debug, PartialEq)]
pub struct RestoreOutboxOperation {
    shorten: Option<Duration>,
    /// Whether the scan found rows and the timer is being armed.
    arming: bool,
}

impl RestoreOutboxOperation {
    /// Arms the drain timer within `after` when rows wait.
    pub fn shorten(after: Duration) -> Self {
        Self {
            shorten: Some(after),
            arming: false,
        }
    }

    /// Only reports whether rows wait; the caller decides how to arm the timer.
    pub fn probe() -> Self {
        Self {
            shorten: None,
            arming: false,
        }
    }
}

impl Operation for RestoreOutboxOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(IoCall::Page {
            space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: 1,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        match (self.arming, result) {
            (false, IoResult::Page { rows, .. }) => match (rows.is_empty(), self.shorten) {
                (false, Some(after)) => {
                    self.arming = true;
                    let key = TaskKey::DrainNotificationOutbox;
                    let arm = TaskEffect::ShortenTimer { key, after };
                    Ok(Next::Call(IoCall::from(Effect::Task(arm))))
                }
                (empty, _) => Ok(Next::Done(!empty)),
            },
            (true, IoResult::Event(event))
                if matches!(*event, Event::Task(TaskEvent::TimerScheduled { .. })) =>
            {
                Ok(Next::Done(true))
            }
            (_, other) => Err(unexpected("an outbox page or a timer", &other)),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::{record, user};
    use aruna_core::UserId;
    use aruna_core::storage_entries::outbox_write_entry;
    use aruna_core::structs::execution::notification::NotificationClass;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use ulid::Ulid;

    const NOW_MS: u64 = 1_700_000_000_000;

    fn outbox_row(record: NotificationRecord) -> (Key, Value) {
        let outbox = NotificationOutboxRecord {
            outbox_id: Ulid::from_parts(NOW_MS - 1_000, 1),
            record,
        };
        let (_, key, value) = outbox_write_entry(&outbox).expect("row encodes");
        (key, value)
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

    fn scan(after: Option<Key>, limit: usize) -> Turn {
        Ok(Next::Call(IoCall::Page {
            space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit,
        }))
    }

    fn rows(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    fn delete_call(key: Key) -> Turn {
        let delete = StorageEffect::BatchDelete {
            deletes: vec![(NOTIFICATION_OUTBOX_KEYSPACE.to_string(), key)],
            txn_id: None,
        };
        Ok(Next::Call(IoCall::from(Effect::Storage(delete))))
    }

    fn delete_result() -> IoResult {
        IoResult::Event(Box::new(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        })))
    }

    fn answer(message: NotificationTransportMessage) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::Notification(Ok(Box::new(
            message,
        ))))))
    }

    /// Reads one page with one remote row and its realm config, up to the remote delivery.
    fn at_remote(holder: NodeId) -> (DrainOutboxOperation, Key) {
        let notice = record(user(1, 2), NotificationClass::Direct, NOW_MS);
        let (key, value) = outbox_row(notice.clone());
        let (realm_id, config) = config_bytes(holder);
        let mut operation = DrainOutboxOperation::new(Some(node_id(1)), NOW_MS);
        assert_eq!(operation.first(), scan(None, OUTBOX_BATCH_SIZE + 1));
        let config_key = Key::from(realm_id.as_bytes().to_vec());
        assert_eq!(
            operation.next(rows(vec![(key.clone(), value)])),
            Ok(Next::Call(IoCall::Read(vec![(
                REALM_CONFIG_KEYSPACE.to_string(),
                config_key
            )])))
        );
        let request = NotificationRequest {
            node: holder,
            message: NotificationTransportMessage::DeliverBatch {
                records: vec![notice],
            },
        };
        assert_eq!(
            operation.next(IoResult::Values(vec![Some(config)])),
            Ok(Next::Call(IoCall::from(Effect::Net(
                NetEffect::Notification(Box::new(request))
            ))))
        );
        (operation, key)
    }

    #[test]
    fn remote_success_calls() {
        let (mut operation, key) = at_remote(node_id(2));
        let ack = answer(NotificationTransportMessage::DeliverAck { written: 1 });
        assert_eq!(operation.next(ack), delete_call(key));
        assert_eq!(operation.next(delete_result()), scan(None, 2));
        assert_eq!(operation.next(rows(Vec::new())), Ok(Next::Done(None)));
    }

    /// A failed delete keeps the rows, goes on with the run and asks for a retry.
    #[test]
    fn delete_failure_continues() {
        let (mut operation, _) = at_remote(node_id(2));
        let ack = answer(NotificationTransportMessage::DeliverAck { written: 1 });
        operation.next(ack).unwrap();
        let failed = IoResult::Event(Box::new(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::Timeout,
        })));
        assert_eq!(
            operation.next(failed),
            Ok(Next::Done(Some(DELIVERY_RETRY_AFTER)))
        );
    }

    /// A failed local upsert keeps the rows and asks for a retry; the run goes on.
    #[test]
    fn local_failure_retries() {
        let local = node_id(1);
        let (key, value) = outbox_row(record(user(1, 2), NotificationClass::Direct, NOW_MS));
        let (_, config) = config_bytes(local);
        let mut operation = DrainOutboxOperation::new(Some(local), NOW_MS);
        operation.first().unwrap();
        operation.next(rows(vec![(key, value)])).unwrap();
        assert!(matches!(
            operation.next(IoResult::Values(vec![Some(config)])),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let failed = Err::<InboxWriteOutcome, _>(InboxError::Conflict);
        let failed = IoResult::Child(aruna_core::calls::ChildOutcome::new(failed));
        assert_eq!(
            operation.next(failed),
            Ok(Next::Done(Some(DELIVERY_RETRY_AFTER)))
        );
    }

    #[test]
    fn rejected_holder_retries() {
        let (mut operation, _) = at_remote(node_id(2));
        let reject = answer(NotificationTransportMessage::Reject(
            "not the holder".to_string(),
        ));
        assert_eq!(
            operation.next(reject),
            Ok(Next::Done(Some(DELIVERY_RETRY_AFTER)))
        );
    }

    #[test]
    fn expired_rows_dropped() {
        let (key, value) = outbox_row(record(user(1, 2), NotificationClass::Direct, NOW_MS));
        let late = NOW_MS + OUTBOX_RETENTION_MS + 10_000;
        let mut operation = DrainOutboxOperation::new(Some(node_id(1)), late);
        operation.first().unwrap();
        assert_eq!(
            operation.next(rows(vec![(key.clone(), value)])),
            delete_call(key)
        );
    }

    #[test]
    fn malformed_rows_kept() {
        let mut operation = DrainOutboxOperation::new(Some(node_id(1)), NOW_MS);
        operation.first().unwrap();
        let malformed = || (Key::from(vec![1]), Value::from(vec![1, 2, 3]));
        assert_eq!(operation.next(rows(vec![malformed()])), scan(None, 2));
        let head = rows(vec![
            malformed(),
            (Key::from(vec![2]), Value::from(vec![1])),
        ]);
        assert_eq!(
            operation.next(head),
            Ok(Next::Done(Some(DELIVERY_RETRY_AFTER)))
        );
    }

    #[test]
    fn no_transport_retries() {
        let mut operation = DrainOutboxOperation::new(None, NOW_MS);
        assert_eq!(
            operation.first(),
            Ok(Next::Done(Some(DELIVERY_RETRY_AFTER)))
        );
    }

    #[test]
    fn unexpected_results_fail() {
        let mut operation = DrainOutboxOperation::new(Some(node_id(1)), NOW_MS);
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(IoError::Unexpected { .. })
        ));
    }

    #[test]
    fn restore_arms_pending() {
        let row = (Key::from(vec![1]), Value::from(vec![1]));
        let page = |rows| IoResult::Page { rows, next: None };
        let arm = TaskEffect::ShortenTimer {
            key: TaskKey::DrainNotificationOutbox,
            after: Duration::ZERO,
        };
        let mut shorten = RestoreOutboxOperation::shorten(Duration::ZERO);
        assert!(matches!(
            shorten.first(),
            Ok(Next::Call(IoCall::Page { limit: 1, .. }))
        ));
        assert_eq!(
            shorten.next(page(vec![row.clone()])),
            Ok(Next::Call(IoCall::from(Effect::Task(arm))))
        );
        let scheduled = Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainNotificationOutbox,
            after: Duration::ZERO,
        });
        assert_eq!(
            shorten.next(IoResult::Event(Box::new(scheduled))),
            Ok(Next::Done(true))
        );

        let mut empty = RestoreOutboxOperation::shorten(Duration::ZERO);
        assert_eq!(empty.next(page(Vec::new())), Ok(Next::Done(false)));
        let mut probe = RestoreOutboxOperation::probe();
        assert_eq!(probe.next(page(vec![row])), Ok(Next::Done(true)));
        assert!(probe.next(IoResult::Done).is_err());
    }
}
