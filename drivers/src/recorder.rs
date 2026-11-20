//! Records the ordered steps of operation runs by label, outcome and size, never their content;
//! off unless it is enabled.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::fmt::Debug;
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

/// What a record holds: the type label, outcome and `Debug` size of each step, never its content,
/// as inputs, events and results can carry private data in any field or nested payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    Started {
        operation: &'static str,
        input_bytes: usize,
    },
    Event {
        label: &'static str,
        detail_bytes: usize,
    },
    Ended {
        outcome: &'static str,
        outcome_bytes: usize,
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
pub(crate) fn begin(operation: &'static str, at_ms: u64, input: &dyn Debug) -> Option<u64> {
    if !ENABLED.load(Ordering::Relaxed) {
        return None;
    }
    let run = RUNS.fetch_add(1, Ordering::Relaxed);
    push(run, at_ms, started(operation, input));
    Some(run)
}

pub(crate) fn event(run: u64, at_ms: u64, event: &Event) {
    push(run, at_ms, delivered(event));
}

/// Closes a run record with its outcome, such as `Ok` or an event label, and its result.
pub(crate) fn end(run: u64, at_ms: u64, outcome: &'static str, value: &dyn Debug) {
    push(run, at_ms, ended(outcome, value));
}

fn push(run: u64, at_ms: u64, entry: Entry) {
    if let Ok(mut buffer) = BUFFER.lock() {
        buffer.push(run, at_ms, entry);
    }
}

fn started(operation: &'static str, input: &dyn Debug) -> Entry {
    let input_bytes = debug_bytes(input);
    Entry::Started {
        operation,
        input_bytes,
    }
}

fn delivered(event: &Event) -> Entry {
    let detail_bytes = debug_bytes(event);
    let label = event.label();
    Entry::Event {
        label,
        detail_bytes,
    }
}

fn ended(outcome: &'static str, value: &dyn Debug) -> Entry {
    let outcome_bytes = debug_bytes(value);
    Entry::Ended {
        outcome,
        outcome_bytes,
    }
}

/// The length of `value`'s `Debug` text.
fn debug_bytes(value: &dyn Debug) -> usize {
    format!("{value:?}").len()
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

/// The bytes a record keeps: its label and its size field.
fn entry_bytes(entry: &Entry) -> usize {
    let label = match entry {
        Entry::Started { operation, .. } => operation,
        Entry::Event { label, .. } => label,
        Entry::Ended { outcome, .. } => outcome,
    };
    label.len() + std::mem::size_of::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;

    // A run's input, a grouped read and a raw byte result leave only labels, outcomes and sizes
    // in the record, never the private data they carry.
    #[test]
    fn private_data_hidden() {
        use aruna_core::events::{GroupEvent, GroupResult, StorageEvent};
        let private = "inbox entry of ann";
        let read = Event::Storage(StorageEvent::ReadResult {
            key: byteview::ByteView::from(b"key".as_slice()),
            value: Some(byteview::ByteView::from(private.as_bytes())),
        });
        let grouped = Event::Group(GroupEvent::Finished {
            results: vec![GroupResult::Done(Box::new(read))],
        });
        let result: Result<Vec<u8>, ()> = Ok(private.as_bytes().to_vec());
        let entries = [
            started("PeerOperation", &private),
            delivered(&grouped),
            ended("Ok", &result),
        ];
        for entry in &entries {
            let text = format!("{entry:?}");
            assert!(!text.contains("inbox"), "{text}");
            assert!(!text.contains("105, 110, 98"), "{text}");
        }
        assert!(matches!(entries[1], Entry::Event { detail_bytes, .. } if detail_bytes > 0));
    }

    #[test]
    fn drops_oldest_records() {
        let mut buffer = Buffer::new();
        // Each record keeps a four byte label and its size field.
        buffer.total_bytes = 2 * (4 + std::mem::size_of::<usize>());
        for outcome in ["aaaa", "bbbb", "cccc"] {
            let outcome_bytes = 1;
            buffer.push(1, 0, Entry::Ended { outcome, outcome_bytes });
        }
        let taken = buffer.take();
        assert_eq!(taken.dropped, 1);
        let sequences: Vec<u64> = taken.records.iter().map(|record| record.sequence).collect();
        assert_eq!(sequences, [2, 3]);
        assert_eq!(buffer.take(), Recorded::default());
    }
}
