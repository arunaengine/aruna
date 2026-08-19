//! Replication of locally published records to the other family holders.
//!
//! The append-only store queues every record this node authored and proved
//! against the replicated chain. Delivery is asynchronous and needs no quorum:
//! one current holder accepting the immutable record is enough, and an
//! unreachable family leaves the entry queued instead of losing the record.

use std::time::Duration;

use aruna_core::effects::{
    Effect, HolderList, JobRecordEffect, JobRecordFrame, MAX_JOB_RECORD_HOLDERS, NetEffect,
    StorageEffect,
};
use aruna_core::events::{Event, JobRecordEvent, NetEvent, StorageEvent};
use aruna_core::keyspaces::{JOB_FAMILY_OUTBOX_KEYSPACE, JOB_FAMILY_RECORD_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{JobRecordEnvelope, JobRecordKey, PlacementRef, RealmId};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, Key, NodeId};
use smallvec::smallvec;
use tracing::{debug, warn};

use super::LifecycleError;
use crate::driver::{DriverContext, drive};
use crate::jobs::records::keys::record_key;
use crate::jobs::records::rows::from_bytes;
use crate::jobs::store::iter_prefix_page;
use crate::metadata::api::load_realm_config;
use crate::placement::resolve_shard_holders;

/// Records one drain pass delivers before it re-arms.
pub const OUTBOX_DRAIN_BATCH: usize = 32;
/// Wall-clock budget of one record publish.
pub const PUBLISH_DEADLINE: Duration = Duration::from_secs(10);
/// Spacing between drain passes while entries remain undeliverable.
pub const OUTBOX_RETRY_AFTER: Duration = Duration::from_secs(5);

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
    pub holders: HolderList<MAX_JOB_RECORD_HOLDERS>,
    pub record: Box<JobRecordFrame>,
    pub key: JobRecordKey,
}

/// One queued record offered to the family holders. The entry is cleared only
/// when a holder durably accepted it or definitively refused it; an unreachable
/// family keeps it queued.
#[derive(Debug, PartialEq)]
pub struct PublishRecordOperation {
    config: PublishRecordConfig,
    delivered: bool,
    state: PublishState,
    outcome: Option<Result<bool, LifecycleError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishState {
    Init,
    Offer,
    Clear,
    Finish,
    Error,
}

impl PublishRecordOperation {
    pub fn new(config: PublishRecordConfig) -> Self {
        Self {
            config,
            delivered: false,
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
        self.state = PublishState::Offer;
        smallvec![Effect::Net(NetEffect::JobRecord(Box::new(
            JobRecordEffect::Publish {
                realm_id: self.config.realm_id,
                placement: self.config.placement,
                holders: self.config.holders.clone(),
                record: self.config.record.clone(),
                deadline: PUBLISH_DEADLINE,
            }
        )))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PublishState::Offer => match event {
                Event::Net(NetEvent::JobRecord(JobRecordEvent::Published { holder })) => {
                    debug!(peer = %holder, "Job record replicated to a family holder");
                    self.delivered = true;
                    self.clear()
                }
                // A definitive refusal will not become an acceptance on retry;
                // the record itself stays durable in the immutable keyspace.
                Event::Net(NetEvent::JobRecord(JobRecordEvent::Rejected { holder, reason })) => {
                    warn!(peer = %holder, reason = ?reason, "Family holder refused a record");
                    self.clear()
                }
                Event::Net(NetEvent::JobRecord(JobRecordEvent::Unavailable(message))) => {
                    debug!(message, "No family holder accepted the record yet");
                    self.settle(false)
                }
                other => self.fail(LifecycleError::UnexpectedEvent {
                    state: "Offer".to_string(),
                    expected: "job record publish result",
                    got: format!("{other:?}"),
                }),
            },
            PublishState::Clear => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    let delivered = self.delivered;
                    self.settle(delivered)
                }
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
    let entries = match iter_prefix_page(
        &context.storage_handle,
        JOB_FAMILY_OUTBOX_KEYSPACE,
        None,
        None,
        OUTBOX_DRAIN_BATCH,
        None,
    )
    .await
    {
        Ok((entries, _)) => entries,
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
            .take(MAX_JOB_RECORD_HOLDERS)
            .collect();
        if holders.is_empty() {
            if !clear_entry(context, key).await {
                return true;
            }
            continue;
        }
        let Ok(holders) = HolderList::new(holders) else {
            continue;
        };
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
