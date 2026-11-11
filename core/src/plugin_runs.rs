//! Runs of a plugin install on its receiver: the saved invocation, the state and the call log.
//! A run keeps what it executes with, so a later change of the install never retargets it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::UserId;

/// Runs of one install that run at once on its receiver.
pub const RUNS_PARALLEL: usize = 4;
/// Unfinished runs one install keeps at most; an admission beyond it is refused.
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

/// Key of an unfinished run in the queue keyspace: by install, then by run id.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_log() {
        let mut run = PluginRun {
            run_id: Ulid::from_parts(2, 1),
            install_id: Ulid::from_parts(1, 1),
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
}
