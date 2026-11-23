//! Wires up the task queue test modules for the harness, notifications, outbox and restore.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentNetEvent, DocumentOutboxEvent,
    DocumentOutboxRecord, DocumentSyncRevision, DocumentTarget,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::NetEvent;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::keyspaces::{NOTIFICATION_INBOX_KEYSPACE, TASK_TIMER_KEYSPACE};
use aruna_core::metadata::GraphLifecycleRecord;
use aruna_core::storage_entries::outbox_write_entry;
use aruna_core::structs::execution::job::identity::JobState;
use aruna_core::structs::execution::job::input::JobPayload;
use aruna_core::structs::execution::job::local::JobRecord;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationOutboxRecord,
};
use aruna_core::structs::execution::notification::{NotificationRecord, new_outbox_record};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::user::id::UserId;
use aruna_ops_work::jobs::store::job_calls::ClaimOutcome;
use aruna_ops_work::notifications::placement::resolve_inbox_holder;
use aruna_ops_work::sync::outbox::batches::{
    DrainSubBatch, DrainSyncOutcome, OUTBOX_CONTINUATION_AFTER, OUTBOX_CONTINUATION_STREAK,
    OUTBOX_INVOCATION_PAGES, OUTBOX_INVOCATION_RECORDS, publish_from_outbox,
};
use aruna_ops_work::sync::outbox::record::{OUTBOX_DRAIN_SIZE, outbox_key};
use aruna_ops_work::sync::shard_placement::DEFER_RETRY_AFTER;
use aruna_p2p::NetHandle;
use aruna_p2p::config::{DiscoveryMethod, NetConfig, RelayMethod};
use aruna_storage::storage::worker::FjallStorage;
use aruna_test_harness::documents::restore_outbox_timers;
use aruna_test_harness::documents::{read_outbox_record, read_outbox_records};
use byteview::ByteView;
use tempfile::tempdir;
use tokio::sync::mpsc;
use ulid::Ulid;

#[path = "tests_harness.rs"]
mod harness;
#[path = "tests_notification.rs"]
mod notification;
#[path = "tests_outbox.rs"]
mod outbox;
#[path = "tests_restore.rs"]
mod restore;
#[path = "tests_support.rs"]
mod support;
