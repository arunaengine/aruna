use super::outbox::document_publish_from_outbox;
use super::outbox::partition_drain_records;
use super::restore::drain_delay;
use super::*;
use crate::jobs::store::{ClaimOutcome, claim_job, insert_job, read_job_record};
use crate::sync::document_sync_outbox::{
    outbox_key, read_outbox_record, restore_document_sync_outbox_timers, write_outbox_effect,
};
use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncOutboxRecord,
    DocumentSyncRevision,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::{
    METADATA_GRAPH_PRUNE_JOB_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE, TASK_TIMER_KEYSPACE,
};
use aruna_core::metadata::{MetadataGraphLifecycleRecord, MetadataGraphPruneJobRecord};
use aruna_core::storage_entries::notification_outbox_write_entry;
use aruna_core::structs::{
    Actor, FIRST_GRANTABLE_HANDLE, JobId, JobPayload, JobRecord, JobState, NotificationClass,
    NotificationKind, NotificationOutboxRecord, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::types::UserId;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_storage::FjallStorage;
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use async_trait::async_trait;
use tempfile::tempdir;
use tokio::sync::mpsc;
use ulid::Ulid;

use crate::notifications::outbox::new_notification_outbox_record;

mod harness;
mod notification;
mod outbox;
mod restore;
