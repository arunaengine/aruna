//! Records operation runs and their ordered events for replay; off unless it is enabled.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use aruna_core::events::Event;

/// How much the recorder keeps: at most `total_bytes` of text, oldest records dropped first,
/// and at most `entry_bytes` of text per record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordLimits {
    pub total_bytes: usize,
    pub entry_bytes: usize,
}

/// One recorded step. `sequence` orders every record of every run on this node, so the
/// storage commits that runs saw acknowledged are ordered too.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub sequence: u64,
    pub run: u64,
    /// The runner's clock when the record was made, as stamped on the operation.
    pub at_ms: u64,
    pub entry: Entry,
}

/// What a record holds. Texts are `Debug` output with secret fields redacted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    Started {
        operation: &'static str,
        input: String,
    },
    Event {
        label: &'static str,
        detail: String,
    },
    Ended {
        outcome: String,
    },
}

/// The records taken out of the recorder, oldest first, and how many older ones the size
/// bound dropped since the last take.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Recorded {
    pub records: Vec<Record>,
    pub dropped: u64,
}

static ENABLED: AtomicBool = AtomicBool::new(false);
static ENTRY_BYTES: AtomicUsize = AtomicUsize::new(0);
static RUNS: AtomicU64 = AtomicU64::new(0);
static BUFFER: Mutex<Buffer> = Mutex::new(Buffer::new());

/// Starts recording every run on this node.
pub fn enable(limits: RecordLimits) {
    if let Ok(mut buffer) = BUFFER.lock() {
        buffer.total_bytes = limits.total_bytes;
    }
    ENTRY_BYTES.store(limits.entry_bytes, Ordering::Relaxed);
    ENABLED.store(true, Ordering::Relaxed);
}

/// Stops recording; records already kept stay until they are taken.
pub fn disable() {
    ENABLED.store(false, Ordering::Relaxed);
}

/// Takes every kept record.
pub fn take() -> Recorded {
    BUFFER
        .lock()
        .map(|mut buffer| buffer.take())
        .unwrap_or_default()
}

/// Opens a run record when recording is on and returns the run's number.
pub(crate) fn begin(
    operation: &'static str,
    at_ms: u64,
    input: impl FnOnce() -> String,
) -> Option<u64> {
    if !ENABLED.load(Ordering::Relaxed) {
        return None;
    }
    let run = RUNS.fetch_add(1, Ordering::Relaxed);
    let input = shown(&input());
    push(run, at_ms, Entry::Started { operation, input });
    Some(run)
}

pub(crate) fn event(run: u64, at_ms: u64, event: &Event) {
    let detail = shown(&format!("{event:?}"));
    let label = event.label();
    push(run, at_ms, Entry::Event { label, detail });
}

pub(crate) fn end(run: u64, at_ms: u64, outcome: impl FnOnce() -> String) {
    let outcome = shown(&outcome());
    push(run, at_ms, Entry::Ended { outcome });
}

fn push(run: u64, at_ms: u64, entry: Entry) {
    if let Ok(mut buffer) = BUFFER.lock() {
        buffer.push(run, at_ms, entry);
    }
}

/// `text` redacted and cut to the per-record bound.
fn shown(text: &str) -> String {
    let mut text = redact(text);
    let limit = ENTRY_BYTES.load(Ordering::Relaxed);
    if text.len() > limit {
        let mut end = limit;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

/// Field names whose values never enter a record.
const SECRET_MARKERS: [&str; 5] = ["secret", "token", "password", "credential", "private"];

/// `Debug` text with the value of every field whose name holds a secret marker replaced.
fn redact(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(colon) = rest.find(": ") {
        let (head, tail) = rest.split_at(colon);
        let tail = &tail[2..];
        out.push_str(head);
        out.push_str(": ");
        let name = head
            .rsplit(|c: char| !(c.is_alphanumeric() || c == '_'))
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        rest = match SECRET_MARKERS.iter().any(|marker| name.contains(marker)) {
            true => {
                out.push_str("<redacted>");
                &tail[value_end(tail)..]
            }
            false => tail,
        };
    }
    out.push_str(rest);
    out
}

/// The end of one `Debug` field value: the first `,` or closing bracket outside nested
/// brackets and strings.
fn value_end(text: &str) -> usize {
    let (mut depth, mut quoted, mut escaped) = (0usize, false, false);
    for (index, c) in text.char_indices() {
        match (quoted, c) {
            (true, _) if escaped => escaped = false,
            (true, '\\') => escaped = true,
            (true, '"') => quoted = false,
            (true, _) => {}
            (false, '"') => quoted = true,
            (false, '(' | '[' | '{') => depth += 1,
            (false, ')' | ']' | '}' | ',') if depth == 0 => return index,
            (false, ')' | ']' | '}') => depth -= 1,
            _ => {}
        }
    }
    text.len()
}

/// The kept records and the bytes of their texts.
struct Buffer {
    total_bytes: usize,
    bytes: usize,
    sequence: u64,
    dropped: u64,
    records: VecDeque<Record>,
}

impl Buffer {
    const fn new() -> Self {
        Self {
            total_bytes: 0,
            bytes: 0,
            sequence: 0,
            dropped: 0,
            records: VecDeque::new(),
        }
    }

    fn push(&mut self, run: u64, at_ms: u64, entry: Entry) {
        let size = entry_bytes(&entry);
        while self.bytes + size > self.total_bytes
            && let Some(old) = self.records.pop_front()
        {
            self.bytes -= entry_bytes(&old.entry);
            self.dropped += 1;
        }
        self.sequence += 1;
        self.bytes += size;
        let sequence = self.sequence;
        let record = Record {
            sequence,
            run,
            at_ms,
            entry,
        };
        self.records.push_back(record);
    }

    fn take(&mut self) -> Recorded {
        self.bytes = 0;
        Recorded {
            records: self.records.drain(..).collect(),
            dropped: std::mem::take(&mut self.dropped),
        }
    }
}

fn entry_bytes(entry: &Entry) -> usize {
    match entry {
        Entry::Started { operation, input } => operation.len() + input.len(),
        Entry::Event { label, detail } => label.len() + detail.len(),
        Entry::Ended { outcome } => outcome.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_secret_fields() {
        let text =
            r#"Login { user: "ann", password: "p,a)ss", session_token: Some([1, 2]), id: 7 }"#;
        assert_eq!(
            redact(text),
            r#"Login { user: "ann", password: <redacted>, session_token: <redacted>, id: 7 }"#
        );
    }

    #[test]
    fn drops_oldest_records() {
        let mut buffer = Buffer::new();
        buffer.total_bytes = 10;
        for outcome in ["aaaa", "bbbb", "cccc"] {
            let outcome = outcome.to_string();
            buffer.push(1, 0, Entry::Ended { outcome });
        }
        let taken = buffer.take();
        assert_eq!(taken.dropped, 1);
        let sequences: Vec<u64> = taken.records.iter().map(|record| record.sequence).collect();
        assert_eq!(sequences, [2, 3]);
        assert_eq!(buffer.take(), Recorded::default());
    }
}
