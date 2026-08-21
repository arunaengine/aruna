//! Replication of locally published records to the other family holders.
//!
//! The append-only store queues every record this node authored and proved
//! against the replicated chain. Delivery is asynchronous and needs no quorum:
//! every current holder eventually accepts the immutable record, and an
//! unreachable family leaves the entry queued instead of losing the record.

use std::time::Duration;

use aruna_core::effects::{
    Effect, HolderList, JobRecordEffect, JobRecordFrame, MAX_JOB_RECORD_HOLDERS, NetEffect,
    StorageEffect,
};
use aruna_core::events::{Event, JobRecordEvent, NetEvent, StorageEvent};
use aruna_core::keyspaces::{
    JOB_FAMILY_OUTBOX_KEYSPACE, JOB_FAMILY_RECORD_KEYSPACE, NODE_STATE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{JobRecordEnvelope, JobRecordKey, PlacementRef, RealmId};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, Key, NodeId};
use smallvec::smallvec;
use tracing::{debug, warn};

use super::LifecycleError;
use crate::driver::{DriverContext, drive};
use crate::jobs::records::keys::record_key;
use crate::jobs::records::rows::{OutboxEntry, from_bytes, to_bytes};
use crate::jobs::store::iter_prefix_page;
use crate::metadata::api::load_realm_config;
use crate::placement::resolve_shard_holders;

/// Records one drain pass delivers before it re-arms.
pub const OUTBOX_DRAIN_BATCH: usize = 32;
/// Wall-clock budget of one record publish.
pub const PUBLISH_DEADLINE: Duration = Duration::from_secs(10);
/// Spacing between drain passes while entries remain undeliverable.
pub const OUTBOX_RETRY_AFTER: Duration = Duration::from_secs(5);
/// Definitive refusals one record is offered through before it is dropped.
/// Unreachable holders never count: only a holder's own refusal does.
pub const MAX_PUBLISH_REJECTIONS: u32 = 16;
const OUTBOX_CURSOR_KEY: &[u8] = b"job_family_outbox_cursor";

/// Kicks the outbox drain without persisting a timer of its own.
pub fn schedule_outbox_drain(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainJobFamilyOutbox,
        after,
    })
}

/// Asks the drain to run now, after a record was queued for replication.
pub async fn kick(context: &DriverContext) {
    if let Some(task) = context.task_handle.as_ref() {
        use aruna_core::handle::Handle;
        let _ = task
            .send_effect(schedule_outbox_drain(Duration::ZERO))
            .await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRecordConfig {
    pub realm_id: RealmId,
    pub placement: PlacementRef,
    pub holders: Vec<NodeId>,
    pub record: Box<JobRecordFrame>,
    pub key: JobRecordKey,
}

/// One queued record offered to the family holders. The entry is cleared only
/// after every current holder durably accepted it; an unreachable family keeps
/// it queued.
#[derive(Debug, PartialEq)]
pub struct PublishRecordOperation {
    config: PublishRecordConfig,
    queued_at_ms: u64,
    delivered: Vec<NodeId>,
    next_holder: u32,
    rejections: u32,
    state: PublishState,
    outcome: Option<Result<bool, LifecycleError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishState {
    Init,
    Read,
    Offer,
    Store,
    Clear,
    Finish,
    Error,
}

impl PublishRecordOperation {
    pub fn new(config: PublishRecordConfig) -> Self {
        Self {
            config,
            queued_at_ms: 0,
            delivered: Vec::new(),
            next_holder: 0,
            rejections: 0,
            state: PublishState::Init,
            outcome: None,
        }
    }

    fn clear(&mut self) -> Effects {
        self.state = PublishState::Clear;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: JOB_FAMILY_OUTBOX_KEYSPACE.to_string(),
            key: record_key(&self.config.key),
            txn_id: None,
        })]
    }

    fn offer(&mut self) -> Effects {
        let holder_count = self.config.holders.len();
        let start = self.next_holder as usize % holder_count.max(1);
        let mut pending = Vec::new();
        let mut last_index = None;
        for offset in 0..holder_count {
            let index = (start + offset) % holder_count;
            let holder = self.config.holders[index];
            if self.delivered.contains(&holder) {
                continue;
            }
            pending.push(holder);
            last_index = Some(index);
            if pending.len() == MAX_JOB_RECORD_HOLDERS {
                break;
            }
        }
        if pending.is_empty() {
            return self.clear();
        }
        if let Some(index) = last_index {
            self.next_holder = u32::try_from((index + 1) % holder_count).unwrap_or_default();
        }
        let Ok(holders) = HolderList::new(pending) else {
            return self.fail(LifecycleError::NotHolder);
        };
        self.state = PublishState::Offer;
        smallvec![Effect::Net(NetEffect::JobRecord(Box::new(
            JobRecordEffect::Publish {
                realm_id: self.config.realm_id,
                placement: self.config.placement,
                holders,
                record: self.config.record.clone(),
                deadline: PUBLISH_DEADLINE,
            }
        )))]
    }

    fn store(&mut self) -> Result<Effects, LifecycleError> {
        let value = to_bytes(&OutboxEntry {
            queued_at_ms: self.queued_at_ms,
            delivered: self.delivered.clone(),
            next_holder: self.next_holder,
            rejections: self.rejections,
        })?;
        self.state = PublishState::Store;
        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: JOB_FAMILY_OUTBOX_KEYSPACE.to_string(),
            key: record_key(&self.config.key),
            value: value.into(),
            txn_id: None,
        })])
    }

    fn settle(&mut self, delivered: bool) -> Effects {
        self.outcome = Some(Ok(delivered));
        self.state = PublishState::Finish;
        smallvec![]
    }

    fn fail(&mut self, error: LifecycleError) -> Effects {
        self.outcome = Some(Err(error));
        self.state = PublishState::Error;
        smallvec![]
    }
}

impl Operation for PublishRecordOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        self.state = PublishState::Read;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: JOB_FAMILY_OUTBOX_KEYSPACE.to_string(),
            key: record_key(&self.config.key),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PublishState::Read => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return self.settle(false);
                    };
                    match from_bytes::<OutboxEntry>(&value) {
                        Ok(entry) => {
                            self.queued_at_ms = entry.queued_at_ms;
                            self.delivered = entry
                                .delivered
                                .into_iter()
                                .filter(|holder| self.config.holders.contains(holder))
                                .collect();
                            self.next_holder = entry.next_holder;
                            self.rejections = entry.rejections;
                            self.offer()
                        }
                        Err(error) => self.fail(error.into()),
                    }
                }
                other => self.fail(LifecycleError::UnexpectedEvent {
                    state: "Read".to_string(),
                    expected: "outbox read result",
                    got: format!("{other:?}"),
                }),
            },
            PublishState::Offer => match event {
                Event::Net(NetEvent::JobRecord(JobRecordEvent::Published { holder })) => {
                    debug!(peer = %holder, "Job record replicated to a family holder");
                    if self.config.holders.contains(&holder) && !self.delivered.contains(&holder) {
                        self.delivered.push(holder);
                    }
                    if self
                        .config
                        .holders
                        .iter()
                        .all(|holder| self.delivered.contains(holder))
                    {
                        self.clear()
                    } else {
                        match self.store() {
                            Ok(effects) => effects,
                            Err(error) => self.fail(error),
                        }
                    }
                }
                Event::Net(NetEvent::JobRecord(JobRecordEvent::PublishedMany { holders })) => {
                    for holder in holders {
                        if self.config.holders.contains(&holder)
                            && !self.delivered.contains(&holder)
                        {
                            self.delivered.push(holder);
                        }
                    }
                    if self
                        .config
                        .holders
                        .iter()
                        .all(|holder| self.delivered.contains(holder))
                    {
                        self.clear()
                    } else {
                        match self.store() {
                            Ok(effects) => effects,
                            Err(error) => self.fail(error),
                        }
                    }
                }
                Event::Net(NetEvent::JobRecord(JobRecordEvent::Rejected { holder, reason })) => {
                    warn!(peer = %holder, reason = ?reason, "Family holder refused a record");
                    self.rejections = self.rejections.saturating_add(1);
                    // A record every holder stands behind refusing will never be
                    // accepted, so it stops consuming the queue.
                    if self.rejections >= MAX_PUBLISH_REJECTIONS {
                        warn!(
                            kind = ?self.config.key.kind,
                            rejections = self.rejections,
                            "Dropping a job record the family holders refuse"
                        );
                        return self.clear();
                    }
                    match self.store() {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Net(NetEvent::JobRecord(JobRecordEvent::Unavailable(message))) => {
                    debug!(message, "No family holder accepted the record yet");
                    match self.store() {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                other => self.fail(LifecycleError::UnexpectedEvent {
                    state: "Offer".to_string(),
                    expected: "job record publish result",
                    got: format!("{other:?}"),
                }),
            },
            PublishState::Store => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.settle(false),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(LifecycleError::UnexpectedEvent {
                    state: "Store".to_string(),
                    expected: "outbox write result",
                    got: format!("{other:?}"),
                }),
            },
            PublishState::Clear => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => self.settle(true),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(LifecycleError::UnexpectedEvent {
                    state: "Clear".to_string(),
                    expected: "outbox delete result",
                    got: format!("{other:?}"),
                }),
            },
            PublishState::Init | PublishState::Finish | PublishState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PublishState::Finish | PublishState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(LifecycleError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// One bounded drain pass. Returns true while entries remain, so the caller
/// re-arms instead of spinning.
pub async fn drain_family_outbox(context: &DriverContext) -> bool {
    let Some(net) = context.net_handle.as_ref() else {
        return false;
    };
    let realm_id = *net.realm_id();
    let local = net.node_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return true;
    };
    let start_after = match read_outbox_cursor(context).await {
        Ok(cursor) => cursor,
        Err(error) => {
            warn!(error = %error, "Job family outbox cursor read failed");
            return true;
        }
    };
    let (entries, next_cursor) = match iter_prefix_page(
        &context.storage_handle,
        JOB_FAMILY_OUTBOX_KEYSPACE,
        None,
        start_after,
        OUTBOX_DRAIN_BATCH,
        None,
    )
    .await
    {
        Ok(page) => page,
        Err(error) => {
            warn!(error = %error, "Job family outbox scan failed");
            return true;
        }
    };
    for (key, _) in entries {
        let Ok(record_key) = JobRecordKey::from_bytes(&key) else {
            continue;
        };
        let Some(frame) = read_record(context, &key).await else {
            continue;
        };
        let placement = match config.family_placement(record_key.family.submission_id) {
            Ok(placement) => placement,
            Err(error) => {
                warn!(error = %error, "Queued record has no derivable family placement");
                continue;
            }
        };
        let holders: Vec<NodeId> = resolve_shard_holders(&config, &placement)
            .into_iter()
            .filter(|holder| *holder != local)
            .collect();
        if holders.is_empty() {
            if !clear_entry(context, key).await {
                return true;
            }
            continue;
        }
        let operation = PublishRecordOperation::new(PublishRecordConfig {
            realm_id,
            placement,
            holders,
            record: Box::new(frame),
            key: record_key,
        });
        if let Err(error) = drive(operation, context).await {
            warn!(error = %error, "Job record replication failed");
        }
    }
    if let Err(error) = write_outbox_cursor(context, next_cursor.as_ref()).await {
        warn!(error = %error, "Job family outbox cursor write failed");
        return true;
    }
    match iter_prefix_page(
        &context.storage_handle,
        JOB_FAMILY_OUTBOX_KEYSPACE,
        None,
        None,
        1,
        None,
    )
    .await
    {
        Ok((entries, _)) => !entries.is_empty(),
        Err(error) => {
            warn!(error = %error, "Job family outbox rescan failed");
            true
        }
    }
}

async fn read_outbox_cursor(context: &DriverContext) -> Result<Option<Key>, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: Key::from(OUTBOX_CURSOR_KEY.to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected outbox cursor read event: {other:?}")),
    }
}

async fn write_outbox_cursor(context: &DriverContext, cursor: Option<&Key>) -> Result<(), String> {
    let event = match cursor {
        Some(cursor) => {
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: NODE_STATE_KEYSPACE.to_string(),
                    key: Key::from(OUTBOX_CURSOR_KEY.to_vec()),
                    value: cursor.clone(),
                    txn_id: None,
                })
                .await
        }
        None => {
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Delete {
                    key_space: NODE_STATE_KEYSPACE.to_string(),
                    key: Key::from(OUTBOX_CURSOR_KEY.to_vec()),
                    txn_id: None,
                })
                .await
        }
    };
    match event {
        Event::Storage(StorageEvent::WriteResult { .. })
        | Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected outbox cursor write event: {other:?}")),
    }
}

async fn clear_entry(context: &DriverContext, key: Key) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: JOB_FAMILY_OUTBOX_KEYSPACE.to_string(),
                key,
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::DeleteResult { .. })
    )
}

async fn read_record(context: &DriverContext, key: &Key) -> Option<JobRecordFrame> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: JOB_FAMILY_RECORD_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(bytes), ..
    }) = event
    else {
        return None;
    };
    let envelope = from_bytes::<JobRecordEnvelope>(&bytes).ok()?;
    JobRecordFrame::new(envelope).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::records::tests::fixture::{Family, node};
    use aruna_core::types::Value;

    #[test]
    fn drops_refused_record() {
        // Holders that stand behind refusing a record must not keep it queued
        // for every drain pass forever.
        let fixture = Family::new([12u8; 32]);
        let envelope = fixture.sign(
            &fixture.holder,
            aruna_core::structs::JobFamilyRecord::Spec(Box::new(fixture.spec())),
        );
        let key = envelope.key();
        let frame = JobRecordFrame::new(envelope).expect("record frame");
        let mut operation = PublishRecordOperation::new(PublishRecordConfig {
            realm_id: fixture.config.realm_id,
            placement: fixture.placement,
            holders: vec![node(10)],
            record: Box::new(frame),
            key,
        });
        let _ = operation.start();
        let _ = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: record_key(&key),
            value: Some(Value::from(
                to_bytes(&OutboxEntry {
                    queued_at_ms: 1,
                    delivered: Vec::new(),
                    next_holder: 0,
                    rejections: MAX_PUBLISH_REJECTIONS - 1,
                })
                .expect("outbox entry"),
            )),
        }));

        let effects = operation.step(Event::Net(NetEvent::JobRecord(JobRecordEvent::Rejected {
            holder: node(10),
            reason: aruna_core::events::JobRecordRejection::Invalid,
        })));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { .. })]
        ));
    }

    #[test]
    fn batches_all_holders() {
        // Delivery state must advance beyond one bounded network fan-out.
        let fixture = Family::new([7u8; 32]);
        let spec = fixture.spec();
        let envelope = fixture.sign(
            &fixture.holder,
            aruna_core::structs::JobFamilyRecord::Spec(Box::new(spec)),
        );
        let key = envelope.key();
        let frame = JobRecordFrame::new(envelope).expect("record frame");
        let holders: Vec<NodeId> = (10..20).map(node).collect();
        let config = PublishRecordConfig {
            realm_id: fixture.config.realm_id,
            placement: fixture.placement,
            holders: holders.clone(),
            record: Box::new(frame),
            key,
        };

        let mut first = PublishRecordOperation::new(config.clone());
        let _ = first.start();
        let effects = first.step(Event::Storage(StorageEvent::ReadResult {
            key: record_key(&key),
            value: Some(Value::from(
                to_bytes(&OutboxEntry {
                    queued_at_ms: 1,
                    delivered: Vec::new(),
                    next_holder: 0,
                    rejections: 0,
                })
                .expect("outbox entry"),
            )),
        }));
        let [Effect::Net(NetEffect::JobRecord(effect))] = effects.as_slice() else {
            panic!("expected first publish")
        };
        let JobRecordEffect::Publish {
            holders: offered, ..
        } = effect.as_ref()
        else {
            panic!("expected publish effect")
        };
        assert_eq!(offered.as_slice(), &holders[..MAX_JOB_RECORD_HOLDERS]);
        let effects = first.step(Event::Net(NetEvent::JobRecord(
            JobRecordEvent::Unavailable("unreachable".to_string()),
        )));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected cursor write")
        };
        let entry = from_bytes::<OutboxEntry>(value).expect("outbox entry decodes");
        assert!(entry.delivered.is_empty());
        assert_eq!(entry.next_holder as usize, MAX_JOB_RECORD_HOLDERS);

        let mut second = PublishRecordOperation::new(config);
        let _ = second.start();
        let effects = second.step(Event::Storage(StorageEvent::ReadResult {
            key: record_key(&key),
            value: Some(Value::from(to_bytes(&entry).expect("outbox entry"))),
        }));
        let [Effect::Net(NetEffect::JobRecord(effect))] = effects.as_slice() else {
            panic!("expected second publish")
        };
        let JobRecordEffect::Publish {
            holders: offered, ..
        } = effect.as_ref()
        else {
            panic!("expected publish effect")
        };
        assert_eq!(
            &offered.as_slice()[..holders.len() - MAX_JOB_RECORD_HOLDERS],
            &holders[MAX_JOB_RECORD_HOLDERS..]
        );
    }

    #[tokio::test]
    async fn pages_past_blocked() {
        // A persisted continuation must let the next pass reach later rows.
        let directory = tempfile::tempdir().expect("temp directory");
        let storage = aruna_storage::FjallStorage::open(directory.path().to_str().unwrap())
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let blocked = Key::from(b"blocked".to_vec());
        let later = Key::from(b"later".to_vec());
        for key in [&blocked, &later] {
            assert!(matches!(
                storage
                    .send_storage_effect(StorageEffect::Write {
                        key_space: JOB_FAMILY_OUTBOX_KEYSPACE.to_string(),
                        key: key.clone(),
                        value: Value::from(b"queued".to_vec()),
                        txn_id: None,
                    })
                    .await,
                Event::Storage(StorageEvent::WriteResult { .. })
            ));
        }

        let (page, next) = iter_prefix_page(
            &context.storage_handle,
            JOB_FAMILY_OUTBOX_KEYSPACE,
            None,
            read_outbox_cursor(&context).await.expect("cursor read"),
            1,
            None,
        )
        .await
        .expect("first page");
        assert_eq!(page[0].0, blocked);
        write_outbox_cursor(&context, next.as_ref())
            .await
            .expect("cursor write");

        let (page, next) = iter_prefix_page(
            &context.storage_handle,
            JOB_FAMILY_OUTBOX_KEYSPACE,
            None,
            read_outbox_cursor(&context).await.expect("cursor read"),
            1,
            None,
        )
        .await
        .expect("second page");
        assert_eq!(page[0].0, later);
        assert!(next.is_none());
    }
}
