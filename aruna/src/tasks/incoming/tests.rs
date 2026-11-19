//! Wires up the task queue test modules for the harness, notifications, outbox and restore.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::outbox::partition_drain_records;
use super::outbox::publish_from_outbox;
use super::restore::drain_delay;
use super::*;
use aruna_core::UserId;
use aruna_core::document::{DocumentChange, DocumentChangeKind, DocumentSyncRevision};
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::keyspaces::{NOTIFICATION_INBOX_KEYSPACE, TASK_TIMER_KEYSPACE};
use aruna_core::metadata::GraphLifecycleRecord;
use aruna_core::storage_entries::outbox_write_entry;
use aruna_core::structs::execution::job::{JobPayload, JobRecord, JobState};
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationOutboxRecord,
};
use aruna_core::structs::execution::notification::{NotificationRecord, new_outbox_record};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::jobs::store::job_calls::ClaimOutcome;
use aruna_operations::notifications::placement::resolve_inbox_holder;
use aruna_operations::sync::outbox_record::outbox_key;
use aruna_storage::FjallStorage;
use aruna_test_support::documents::restore_outbox_timers;
use aruna_test_support::documents::restore_outbox_timers;
use aruna_test_support::documents::{read_outbox_record, read_outbox_records};
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
