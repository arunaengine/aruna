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
use crate::metadata::{MetadataEventPayload, MetadataEventRecord};
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
    /// A journaled source event an event trigger of the install matched.
    Event(SourceEvent),
    /// A verified webhook request.
    Webhook(HookRequest),
}

/// A webhook request after its check, without credential, relay and hop-by-hop headers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookRequest {
    pub delivery_id: Option<String>,
    pub method: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// How long a receiver keeps a webhook's delivery id, so a repeat starts nothing.
pub const HOOK_RETENTION_MS: u64 = 7 * 24 * 3600 * 1000;
/// Row kinds of the webhook keyspace: delivery ids by install, and the same by receipt time.
pub const HOOK_ROW: u8 = b'd';
pub const HOOK_TIME_ROW: u8 = b't';

/// Key of a delivery id an install received.
pub fn hook_key(install_id: Ulid, delivery_id: &str) -> Vec<u8> {
    let mut key = vec![HOOK_ROW];
    key.extend_from_slice(&install_id.to_bytes());
    key.extend_from_slice(delivery_id.as_bytes());
    key
}

/// Key of the same delivery id by receipt time, for the prune.
pub fn hook_time_key(received_ms: u64, install_id: Ulid, delivery_id: &str) -> Vec<u8> {
    let mut key = vec![HOOK_TIME_ROW];
    key.extend_from_slice(&received_ms.to_be_bytes());
    key.extend_from_slice(&hook_key(install_id, delivery_id)[1..]);
    key
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

/// How long a receiver keeps an event's dedup record after its anchor: the journal retention,
/// the send expiry, the clock skew bound and a day. A row older than this is refused.
pub const DEDUP_RETENTION_MS: u64 =
    JOURNAL_RETENTION_MS + SEND_EXPIRY_MS + 5 * 60 * 1000 + 24 * 3600 * 1000;

/// How long an event waits on the receiver for its install record or account writes.
pub const WAITING_EXPIRY_MS: u64 = 3 * 24 * 3600 * 1000;

/// Value of a queue row that holds the slot of an event waiting on this receiver; its key names
/// the waiting row instead of a run.
pub const WAITING_ROW: &[u8] = b"w";

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

/// Row kinds of the journal keyspace: kept rows for replay, rows not matched yet, and the
/// source-id rows that keep each source id from being journaled twice on a node.
pub const JOURNAL_ROW: u8 = b'j';
pub const PENDING_ROW: u8 = b'u';
pub const SOURCE_ROW: u8 = b'i';

/// Key of the source-id row of `source`; its value is the event's anchor.
pub fn source_key(source: &str) -> Vec<u8> {
    [&[SOURCE_ROW][..], source.as_bytes()].concat()
}

/// The event kinds this node journals.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SourceKind {
    DatasetCreated,
    DataUploaded,
    DatasetUpdated,
}

impl SourceKind {
    /// The trigger name a manifest uses for this kind.
    pub fn name(self) -> &'static str {
        match self {
            Self::DatasetCreated => "dataset_created",
            Self::DataUploaded => "data_uploaded",
            Self::DatasetUpdated => "dataset_updated",
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
    /// The account whose change it reports, when the kind knows it.
    pub actor: Option<UserId>,
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
            actor: Some(event.user_id),
        }
    }

    /// An accepted change of a metadata document, from its update event.
    pub fn dataset_updated(event: &MetadataEventRecord, now_ms: u64) -> Self {
        Self {
            kind: SourceKind::DatasetUpdated,
            source: format!("dataset_updated/{}", event.event_id),
            ..Self::dataset_created(event, now_ms)
        }
    }

    /// The event a metadata event reports, if any: a create or a content change.
    pub fn from_metadata(event: &MetadataEventRecord, now_ms: u64) -> Option<Self> {
        match event.payload {
            MetadataEventPayload::Scaffold { .. } | MetadataEventPayload::RoCrate { .. } => {
                Some(Self::dataset_created(event, now_ms))
            }
            MetadataEventPayload::ReplaceRoCrate { .. }
            | MetadataEventPayload::UpsertDataEntity { .. }
            | MetadataEventPayload::UpsertContextualEntity { .. }
            | MetadataEventPayload::ApplyBatch { .. } => Some(Self::dataset_updated(event, now_ms)),
            MetadataEventPayload::Checkpoint { .. } => None,
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
            actor: None,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Key of a journal row of `kind`: journal time, then a random unique suffix, so rows of the
/// same millisecond have no set order.
pub fn journal_key(kind: u8, journaled_ms: u64, unique: Ulid) -> Vec<u8> {
    let mut key = vec![kind];
    key.extend_from_slice(&journaled_ms.to_be_bytes());
    key.extend_from_slice(&unique.to_bytes());
    key
}

/// The kept row, the pending row and the source-id row of one event, for the committing
/// transaction's batch.
pub fn journal_entries(
    event: &SourceEvent,
) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
    let unique = Ulid::generate();
    let value = ByteView::from(event.to_bytes()?);
    let mut entries: Vec<_> = [JOURNAL_ROW, PENDING_ROW]
        .into_iter()
        .map(|kind| {
            let key = journal_key(kind, event.journaled_ms, unique);
            (
                EVENT_JOURNAL_KEYSPACE.to_string(),
                key.into(),
                value.clone(),
            )
        })
        .collect();
    entries.push((
        EVENT_JOURNAL_KEYSPACE.to_string(),
        source_key(&event.source).into(),
        event.anchor_ms.to_be_bytes().to_vec().into(),
    ));
    Ok(entries)
}

/// Forward rows of one install a node keeps at most; matching beyond records a gap.
pub const EVENTS_QUEUED: usize = 10_000;
/// How long a forward row is sent before it is dropped as a gap.
pub const SEND_EXPIRY_MS: u64 = 3 * 24 * 3600 * 1000;

/// A matched event this node sends to the install's receiver.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardRow {
    pub install_id: Ulid,
    /// The install revision the event was matched with; the receiver waits until it has it.
    pub revision: u64,
    pub event: SourceEvent,
    /// When this node queued the row; its send expiry counts from here.
    pub queued_ms: u64,
}

/// Key of a forward row: the install, then the event's pending row key without its kind.
pub fn outbox_key(install_id: Ulid, pending: &[u8]) -> Vec<u8> {
    let mut key = install_id.to_bytes().to_vec();
    key.extend_from_slice(pending.get(1..).unwrap_or_default());
    key
}

/// Why matched events of an install were not delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum GapReason {
    /// The install's forward rows on this node were at their bound.
    Full,
    /// The event's journal row passed its retention before it was matched.
    Pruned,
    /// The forward row was not decided by the receiver before its send expiry.
    Expired,
    /// The event waited on the receiver for its install record past the waiting expiry.
    Waited,
    /// The event reached the receiver after its dedup record would have expired.
    Late,
}

impl GapReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Pruned => "pruned",
            Self::Expired => "expired",
            Self::Waited => "waited",
            Self::Late => "late",
        }
    }
}

/// Undelivered events of one install, kind and reason, as the node that lost them counts them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapRow {
    pub kind: SourceKind,
    pub reason: GapReason,
    pub count: u64,
    /// Whether a range of events of unknown size was lost as well; `count` is a lower bound.
    pub unknown: bool,
    /// Anchors of the earliest and the latest event counted.
    pub earliest_ms: u64,
    pub latest_ms: u64,
    /// Raised with each counted event; the receiver acknowledges the sequence it has.
    pub sequence: u64,
    pub acked: u64,
}

impl GapRow {
    pub fn new(kind: SourceKind, reason: GapReason) -> Self {
        Self {
            kind,
            reason,
            count: 0,
            unknown: false,
            earliest_ms: u64::MAX,
            latest_ms: 0,
            sequence: 0,
            acked: 0,
        }
    }

    /// Counts one more event with the anchor `anchor_ms`.
    pub fn add(&mut self, anchor_ms: u64) {
        self.count += 1;
        self.earliest_ms = self.earliest_ms.min(anchor_ms);
        self.latest_ms = self.latest_ms.max(anchor_ms);
        self.sequence += 1;
    }

    /// Counts a lost range of events of unknown size between two times.
    pub fn add_range(&mut self, from_ms: u64, to_ms: u64) {
        self.unknown = true;
        self.earliest_ms = self.earliest_ms.min(from_ms);
        self.latest_ms = self.latest_ms.max(to_ms);
        self.sequence += 1;
    }
}

/// Row kind of a gap this node counted as a sender.
pub const SENDER_GAP: u8 = b's';

/// Row kind of the latest gap report a receiver keeps per sender.
pub const RECEIVER_GAP: u8 = b'r';

/// Key of a reported gap row: by sender, install, event kind and reason.
pub fn reported_key(
    sender: NodeId,
    install_id: Ulid,
    kind: SourceKind,
    reason: GapReason,
) -> Vec<u8> {
    let mut key = vec![RECEIVER_GAP];
    key.extend_from_slice(sender.as_bytes());
    key.extend_from_slice(&gap_key(install_id, kind, reason)[1..]);
    key
}

/// Key of a sender's gap row: by install, then event kind and reason.
pub fn gap_key(install_id: Ulid, kind: SourceKind, reason: GapReason) -> Vec<u8> {
    let mut key = vec![SENDER_GAP];
    key.extend_from_slice(&install_id.to_bytes());
    key.extend_from_slice(&[kind as u8, reason as u8]);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    // Journal, forward and gap rows store kinds by index; the update kind was appended, so rows
    // written before it keep their meaning.
    #[test]
    fn kind_indexes_kept() {
        let index = |kind: SourceKind| postcard::to_allocvec(&kind).unwrap();
        assert_eq!(index(SourceKind::DatasetCreated), [0]);
        assert_eq!(index(SourceKind::DataUploaded), [1]);
        assert_eq!(index(SourceKind::DatasetUpdated), [2]);
        assert_eq!(
            gap_key(Ulid::nil(), SourceKind::DataUploaded, GapReason::Full)[17],
            1
        );
    }

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
    fn upload_identity() {
        // One version copied into two keys of one bucket is two events.
        let realm_id = crate::structs::identity::realm::RealmId::from_bytes([1u8; 32]);
        let node = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let scope = (realm_id, Ulid::from_parts(1, 1), node);
        let version = Ulid::from_parts(3, 3);
        let first = SourceEvent::data_uploaded(scope, ("b", "x/one"), version, 7);
        let second = SourceEvent::data_uploaded(scope, ("b", "x/two"), version, 9);
        assert_ne!(first.source, second.source);
        assert_eq!((first.anchor_ms, second.anchor_ms), (7, 9));
        assert!(first.path.ends_with("/b/x/one") && first.resource == first.path);
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
            actor: None,
        };
        let entries = journal_entries(&event).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].1[0], JOURNAL_ROW);
        assert_eq!(entries[1].1[0], PENDING_ROW);
        assert_eq!(entries[2].1.to_vec(), source_key(&event.source));
        assert_eq!(entries[2].2.to_vec(), 2u64.to_be_bytes());
        assert_eq!(entries[0].1[1..], entries[1].1[1..]);
        assert_eq!(SourceEvent::from_bytes(&entries[0].2).unwrap(), event);
        let early = journal_key(JOURNAL_ROW, 4, Ulid::from_parts(9, 9));
        assert!(early < entries[0].1.to_vec());
    }
}
