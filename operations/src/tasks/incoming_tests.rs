use super::outbox::partition_drain_records;
use super::outbox::publish_from_outbox;
use super::restore::drain_delay;
use super::*;
use crate::jobs::store::{ClaimOutcome, claim_job, insert_job, read_job_record};
use crate::sync::document_outbox::{outbox_key, read_outbox_record};
use aruna_core::UserId;
use aruna_core::document::{DocumentChange, DocumentChangeKind, DocumentSyncRevision};
use aruna_core::keyspaces::{NOTIFICATION_INBOX_KEYSPACE, TASK_TIMER_KEYSPACE};
use aruna_core::metadata::GraphLifecycleRecord;
use aruna_core::storage_entries::outbox_write_entry;
use aruna_core::structs::{
    Actor, JobPayload, JobRecord, JobState, NotificationClass, NotificationKind,
    NotificationOutboxRecord, RealmNodeKind,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_storage::FjallStorage;
use tempfile::tempdir;
use tokio::sync::mpsc;
use ulid::Ulid;

use crate::notifications::outbox::new_outbox_record;

#[path = "incoming_tests_harness.rs"]
mod harness;
#[path = "incoming_tests_notification.rs"]
mod notification;
#[path = "incoming_tests_outbox.rs"]
mod outbox;
#[path = "incoming_tests_restore.rs"]
mod restore;
