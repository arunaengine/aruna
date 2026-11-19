//! Declares trigger kinds, what each one needs, and the fields a trigger may carry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::manifest::Setting;
use crate::permission::Permission;

/// Timers are coarse schedules, not sleep loops.
pub const MIN_INTERVAL: u64 = 60;
pub const MAX_INTERVAL: u64 = 366 * 24 * 3600;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    Manual,
    Every,
    Webhook,
    DatasetCreated,
    DataUploaded,
    SyncCompleted,
    SyncFailed,
    DatasetUpdated,
    /// A job the install started ended, after the run that started it succeeded.
    JobFinished,
}

impl TriggerKind {
    pub const ALL: [TriggerKind; 9] = [
        Self::Manual,
        Self::Every,
        Self::Webhook,
        Self::DatasetCreated,
        Self::DataUploaded,
        Self::SyncCompleted,
        Self::SyncFailed,
        Self::DatasetUpdated,
        Self::JobFinished,
    ];

    /// The kind an author names in `on`, such as `timer` for [`TriggerKind::Every`].
    pub fn named(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == name)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Every => "timer",
            Self::Webhook => "webhook",
            Self::DatasetCreated => "dataset_created",
            Self::DataUploaded => "data_uploaded",
            Self::SyncCompleted => "sync_completed",
            Self::SyncFailed => "sync_failed",
            Self::DatasetUpdated => "dataset_updated",
            Self::JobFinished => "job_finished",
        }
    }

    /// The read permission an event trigger needs before any event is delivered.
    pub fn needs(self) -> Option<Permission> {
        match self {
            Self::DatasetCreated | Self::DatasetUpdated => Some(Permission::MetadataRead),
            Self::DataUploaded | Self::SyncCompleted | Self::SyncFailed => {
                Some(Permission::ObjectsRead)
            }
            Self::Manual | Self::Every | Self::Webhook | Self::JobFinished => None,
        }
    }
}

/// One trigger. Which fields apply depends on its kind.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trigger {
    /// `manual` only: the form a person fills in to start a run.
    #[serde(default)]
    pub form: BTreeMap<String, Setting>,
    /// `every` only: a whole number followed by `m`, `h` or `d`.
    #[serde(default)]
    pub interval: Option<String>,
    /// `webhook` only: the secret slot that checks each request.
    #[serde(default)]
    pub verify: Option<String>,
    /// `webhook` only: the lowercase header carrying the provider's delivery id.
    #[serde(default)]
    pub delivery_id: Option<String>,
}

impl Trigger {
    /// The timer interval in seconds, when it is a valid interval.
    pub fn interval_secs(&self) -> Option<u64> {
        let text = self.interval.as_deref()?;
        let unit = match text.chars().last()? {
            'm' => 60,
            'h' => 3600,
            'd' => 86_400,
            _ => return None,
        };
        let count: u64 = text[..text.len() - 1].parse().ok()?;
        let secs = count.checked_mul(unit)?;
        (MIN_INTERVAL..=MAX_INTERVAL)
            .contains(&secs)
            .then_some(secs)
    }
}
