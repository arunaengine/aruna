//! Runs of a plugin install on its receiver, with their saved invocation, state and call log, and
//! the journaled source events that start runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::UserId;
use crate::errors::ConversionError;
use crate::keyspaces::EVENT_JOURNAL_KEYSPACE;
use crate::metadata::MetadataEventRecord;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::blob::object_permission_path;
use crate::types::GroupId;

/// Runs of one install that run at once on its receiver.
pub const RUNS_PARALLEL: usize = 4;
/// Unfinished and unresolved runs one install keeps at most; an admission beyond is refused.
pub const RUNS_QUEUED: usize = 100;
/// How long a finished run, its intents and its stored replies are kept.
pub const RUN_RETENTION_MS: u64 = 30 * 24 * 3600 * 1000;
/// Starts of a run before a host or infrastructure error fails it.
pub const MAX_ATTEMPTS: u32 = 5;
/// Bytes of a manual run's JSON input.
pub const MAX_INPUT: usize = 64 * 1024;
/// Lines and bytes of one run's call log.
pub const MAX_LOG_LINES: usize = 200;
pub const MAX_LOG_BYTES: usize = 64 * 1024;
/// Bytes of a run's outcome message.
pub const MAX_MESSAGE: usize = 4 * 1024;

/// Row kinds in the runs keyspace, before the install id.
pub const RUN_ROW: u8 = b'r';
const TIMER_ROW: u8 = b't';

/// What started a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunTrigger {
    /// A user started it by hand with a JSON input.
    Manual { by: UserId, input: String },
    /// An `every` timer occurrence that was due at `occurrence_ms`.
    Timer { occurrence_ms: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunState {
    Queued,
    Running,
    Succeeded,
    Failed,
    /// An effect may have happened without its reply being stored; a person decides.
    NeedsAttention,
    Cancelled,
}

impl RunState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::NeedsAttention => "needs-attention",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the run ended; a `needs-attention` run ended and waits for a resolution.
    pub fn finished(self) -> bool {
        !matches!(self, Self::Queued | Self::Running)
    }
}

/// One line the guest logged, with its level.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    pub at_ms: u64,
    pub level: String,
    pub message: String,
}

/// One run of an install, kept on the install's receiver.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginRun {
    pub run_id: Ulid,
    pub install_id: Ulid,
    /// The install's group, plugin and version, so the run stays readable and resolvable by
    /// the group's admins after the install is removed.
    pub group_id: GroupId,
    pub plugin: String,
    pub version: String,
    /// The install revision and the artifact the run was admitted with.
    pub revision: u64,
    pub digest: String,
    pub handler: String,
    /// The install's settings as a JSON object, as they were at admission.
    pub settings: String,
    pub trigger: RunTrigger,
    pub state: RunState,
    pub attempts: u32,
    /// The run is not started before this time.
    pub due_ms: u64,
    pub created_ms: u64,
    pub finished_ms: Option<u64>,
    /// Why the run failed, was cancelled or needs attention, or how it was resolved.
    pub message: Option<String>,
    pub output: Option<String>,
    pub log: Vec<LogLine>,
}

impl PluginRun {
    /// Appends a log line while the log stays within its bounds; later lines are dropped.
    pub fn push_log(&mut self, line: LogLine) -> bool {
        let bytes: usize = self.log.iter().map(|line| line.message.len()).sum();
        let fits = self.log.len() < MAX_LOG_LINES && bytes + line.message.len() <= MAX_LOG_BYTES;
        if fits {
            self.log.push(line);
        }
        fits
    }

    /// Ends the run in `state` with a message cut to its bound.
    pub fn finish(&mut self, state: RunState, message: Option<String>, now_ms: u64) {
        self.state = state;
        self.message = message.map(|text| cut(text, MAX_MESSAGE));
        self.finished_ms = Some(now_ms);
    }
}

/// `text` cut to at most `max` bytes at a character boundary.
pub fn cut(mut text: String, max: usize) -> String {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}

/// Key of a run in the runs keyspace: by install, then by run id in creation order.
pub fn run_key(install_id: Ulid, run_id: Ulid) -> Vec<u8> {
    let mut key = run_prefix(install_id);
    key.extend_from_slice(&run_id.to_bytes());
    key
}

pub fn run_prefix(install_id: Ulid) -> Vec<u8> {
    let mut key = vec![RUN_ROW];
    key.extend_from_slice(&install_id.to_bytes());
    key
}

/// Key of the last due occurrence of an install's `every` timer.
pub fn timer_key(install_id: Ulid) -> Vec<u8> {
    let mut key = vec![TIMER_ROW];
    key.extend_from_slice(&install_id.to_bytes());
    key
}

/// Value of a queue row whose run ended needing attention: it waits for a person, not to run,
/// and still counts against the install's limit.
pub const ATTENTION_ROW: &[u8] = b"a";

/// Key of an unfinished or unresolved run in the queue keyspace: by install, then by run id.
pub fn queue_key(install_id: Ulid, run_id: Ulid) -> Vec<u8> {
    let mut key = install_id.to_bytes().to_vec();
    key.extend_from_slice(&run_id.to_bytes());
    key
}

/// The install and run ids of a queue key.
pub fn parse_queue_key(key: &[u8]) -> Option<(Ulid, Ulid)> {
    let install: [u8; 16] = key.get(..16)?.try_into().ok()?;
    let run: [u8; 16] = key.get(16..32)?.try_into().ok()?;
    (key.len() == 32).then(|| (Ulid::from_bytes(install), Ulid::from_bytes(run)))
}

/// How long a node keeps its journal rows for replay.
pub const JOURNAL_RETENTION_MS: u64 = 7 * 24 * 3600 * 1000;

/// Row kinds of the journal keyspace: kept rows for replay, and rows not matched yet.
pub const JOURNAL_ROW: u8 = b'j';
pub const PENDING_ROW: u8 = b'u';

/// The event kinds this node journals.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SourceKind {
    DatasetCreated,
    DataUploaded,
}

impl SourceKind {
    /// The trigger name a manifest uses for this kind.
    pub fn name(self) -> &'static str {
        match self {
            Self::DatasetCreated => "dataset_created",
            Self::DataUploaded => "data_uploaded",
        }
    }
}

/// One committed source event, as the committing node journaled it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceEvent {
    pub kind: SourceKind,
    pub group_id: GroupId,
    /// The policy path the grants and `authorize()` decide on.
    pub path: String,
    /// What the guest names: a document id or an object path.
    pub resource: String,
    /// The document event or the object version.
    pub revision: String,
    /// The canonical identity, the same on every node that journals the event.
    pub source: String,
    /// The origin time the event's dedup expiry counts from; never renewed.
    pub anchor_ms: u64,
    /// When this node journaled it, by its own clock; orders replay and pruning only.
    pub journaled_ms: u64,
}

impl SourceEvent {
    /// The creation of a metadata document, from its create event.
    pub fn dataset_created(event: &MetadataEventRecord, now_ms: u64) -> Self {
        Self {
            kind: SourceKind::DatasetCreated,
            group_id: event.record.group_id,
            path: event.record.permission_path.clone(),
            resource: event.record.document_id.to_string(),
            revision: event.event_id.to_string(),
            source: format!("dataset_created/{}", event.event_id),
            anchor_ms: event.event_id.timestamp_ms(),
            journaled_ms: now_ms,
        }
    }

    /// A new object version committed on this node; its first commit time is the anchor.
    pub fn data_uploaded(
        (realm_id, group_id, node): (RealmId, GroupId, NodeId),
        (bucket, key): (&str, &str),
        version_id: Ulid,
        now_ms: u64,
    ) -> Self {
        let path = object_permission_path(realm_id, group_id, node, bucket, key);
        Self {
            kind: SourceKind::DataUploaded,
            group_id,
            resource: path.clone(),
            path,
            revision: version_id.to_string(),
            source: format!("data_uploaded/{node}/{bucket}/{version_id}/{key}"),
            anchor_ms: now_ms,
            journaled_ms: now_ms,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Key of a journal row of `kind`: journal time, then a unique suffix.
pub fn journal_key(kind: u8, journaled_ms: u64, unique: Ulid) -> Vec<u8> {
    let mut key = vec![kind];
    key.extend_from_slice(&journaled_ms.to_be_bytes());
    key.extend_from_slice(&unique.to_bytes());
    key
}

/// The kept row and the pending row of one event, for the committing transaction's batch.
pub fn journal_entries(
    event: &SourceEvent,
) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
    let unique = Ulid::generate();
    let value = ByteView::from(event.to_bytes()?);
    Ok([JOURNAL_ROW, PENDING_ROW]
        .into_iter()
        .map(|kind| {
            let key = journal_key(kind, event.journaled_ms, unique);
            (
                EVENT_JOURNAL_KEYSPACE.to_string(),
                key.into(),
                value.clone(),
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_log() {
        let mut run = PluginRun {
            run_id: Ulid::from_parts(2, 1),
            install_id: Ulid::from_parts(1, 1),
            group_id: Ulid::from_parts(1, 2),
            plugin: "announcer".into(),
            version: "0.1.0".into(),
            revision: 1,
            digest: String::new(),
            handler: "on_manual".into(),
            settings: "{}".into(),
            trigger: RunTrigger::Timer { occurrence_ms: 5 },
            state: RunState::Queued,
            attempts: 0,
            due_ms: 5,
            created_ms: 5,
            finished_ms: None,
            message: None,
            output: None,
            log: Vec::new(),
        };
        let line = |message: String| LogLine {
            at_ms: 1,
            level: "info".into(),
            message,
        };
        assert!(run.push_log(line("x".repeat(MAX_LOG_BYTES))));
        assert!(!run.push_log(line("y".into())));
        run.finish(RunState::Failed, Some("é".repeat(MAX_MESSAGE)), 9);
        let message = run.message.unwrap_or_default();
        assert!(message.len() <= MAX_MESSAGE && message.starts_with('é'));
        assert!(RunState::NeedsAttention.finished() && !RunState::Running.finished());
    }

    #[test]
    fn ordered_keys() {
        let install = Ulid::from_parts(1, 1);
        let (early, late) = (Ulid::from_parts(2, 1), Ulid::from_parts(3, 1));
        assert!(run_key(install, early) < run_key(install, late));
        assert!(run_key(install, late).starts_with(&run_prefix(install)));
        assert_ne!(timer_key(install)[0], run_prefix(install)[0]);
        let key = queue_key(install, late);
        assert_eq!(parse_queue_key(&key), Some((install, late)));
        assert_eq!(parse_queue_key(&key[1..]), None);
    }

    #[test]
    fn journal_rows() {
        let event = SourceEvent {
            kind: SourceKind::DataUploaded,
            group_id: Ulid::from_parts(1, 1),
            path: "/r/g/x/data/n/b/k".into(),
            resource: "/r/g/x/data/n/b/k".into(),
            revision: Ulid::from_parts(2, 2).to_string(),
            source: "data_uploaded/n/b/k/v".into(),
            anchor_ms: 2,
            journaled_ms: 5,
        };
        let entries = journal_entries(&event).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].1[0], JOURNAL_ROW);
        assert_eq!(entries[1].1[0], PENDING_ROW);
        assert_eq!(entries[0].1[1..], entries[1].1[1..]);
        assert_eq!(SourceEvent::from_bytes(&entries[0].2).unwrap(), event);
        let early = journal_key(JOURNAL_ROW, 4, Ulid::from_parts(9, 9));
        assert!(early < entries[0].1.to_vec());
    }
}
