//! Records the ordered steps of operation runs by label, outcome and size, never their content;
//! off unless it is enabled.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use aruna_core::events::{Event, StorageEvent};

/// How much the recorder keeps: at most `total_bytes` of record storage, oldest records dropped
/// first, and `entry_bytes` bounds the formatting one record may count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordLimits {
    pub total_bytes: usize,
    pub entry_bytes: usize,
}

/// One recorded step. `sequence` orders the records as runs received their steps; the storage
/// commit order is the `commit` of the commit events.
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
    /// The run header. `entropy` is the id-mint input, which every minted id shows anyway; with
    /// the records' `at_ms` it gives the run's ids again. Secret seeds never come from it.
    Started {
        operation: &'static str,
        input_bytes: usize,
        entropy: u128,
    },
    /// `commit` is the storage's sequence of an acknowledged commit, numbered when it applied.
    Event {
        label: &'static str,
        detail_bytes: usize,
        commit: Option<u64>,
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
        buffer.limit(limits.total_bytes);
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

/// Opens a run record with the run's entropy when recording is on and returns the run's number.
pub(crate) fn begin(
    operation: &'static str,
    at_ms: u64,
    input: &dyn Debug,
    entropy: u128,
) -> Option<u64> {
    if !ENABLED.load(Ordering::Relaxed) {
        return None;
    }
    let run = RUNS.fetch_add(1, Ordering::Relaxed);
    push(run, at_ms, started(operation, input, entropy, entry_cap()));
    Some(run)
}

pub(crate) fn event(run: u64, at_ms: u64, event: &Event) {
    push(run, at_ms, delivered(event, entry_cap()));
}

/// Closes a run record with its outcome, such as `Ok` or an event label, and its result.
pub(crate) fn end(run: u64, at_ms: u64, outcome: &'static str, value: &dyn Debug) {
    push(run, at_ms, ended(outcome, value, entry_cap()));
}

fn entry_cap() -> usize {
    ENTRY_BYTES.load(Ordering::Relaxed)
}

fn push(run: u64, at_ms: u64, entry: Entry) {
    if let Ok(mut buffer) = BUFFER.lock() {
        buffer.push(run, at_ms, entry);
    }
}

fn started(operation: &'static str, input: &dyn Debug, entropy: u128, cap: usize) -> Entry {
    let input_bytes = debug_bytes(input, cap);
    Entry::Started {
        operation,
        input_bytes,
        entropy,
    }
}

fn delivered(event: &Event, cap: usize) -> Entry {
    let detail_bytes = debug_bytes(event, cap);
    let label = event.label();
    let commit = match event {
        Event::Storage(StorageEvent::TransactionCommitted { sequence, .. }) => Some(*sequence),
        _ => None,
    };
    Entry::Event {
        label,
        detail_bytes,
        commit,
    }
}

fn ended(outcome: &'static str, value: &dyn Debug, cap: usize) -> Entry {
    let outcome_bytes = debug_bytes(value, cap);
    Entry::Ended {
        outcome,
        outcome_bytes,
    }
}

/// The length of `value`'s `Debug` text up to `cap`, counted without keeping the text; the
/// formatting stops once it passes `cap`.
fn debug_bytes(value: &dyn Debug, cap: usize) -> usize {
    let mut counted = Counted { bytes: 0, cap };
    let _ = std::fmt::write(&mut counted, format_args!("{value:?}"));
    counted.bytes.min(cap)
}

/// A text sink that only counts, and fails once it passed its cap so formatting stops there.
struct Counted {
    bytes: usize,
    cap: usize,
}

impl std::fmt::Write for Counted {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.bytes = self.bytes.saturating_add(text.len());
        match self.bytes > self.cap {
            true => Err(std::fmt::Error),
            false => Ok(()),
        }
    }
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

    /// Sets the budget and reserves room for its records, so the deque never grows past it.
    fn limit(&mut self, total_bytes: usize) {
        self.total_bytes = total_bytes;
        while self.bytes > total_bytes && self.records.pop_front().is_some() {
            self.bytes -= RECORD_BYTES;
            self.dropped += 1;
        }
        let count = total_bytes / RECORD_BYTES;
        self.records.shrink_to(count);
        self.records.reserve_exact(count - self.records.len());
    }

    fn push(&mut self, run: u64, at_ms: u64, entry: Entry) {
        // A budget below one record refuses it instead of dropping every other one.
        if RECORD_BYTES > self.total_bytes {
            self.dropped += 1;
            return;
        }
        while self.bytes + RECORD_BYTES > self.total_bytes && self.records.pop_front().is_some() {
            self.bytes -= RECORD_BYTES;
            self.dropped += 1;
        }
        self.sequence += 1;
        self.bytes += RECORD_BYTES;
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

/// The memory one kept record takes; its labels are static, so they add nothing.
const RECORD_BYTES: usize = std::mem::size_of::<Record>();

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
            started("PeerOperation", &private, 0, 4096),
            delivered(&grouped, 4096),
            ended("Ok", &result, 4096),
        ];
        for entry in &entries {
            let text = format!("{entry:?}");
            assert!(!text.contains("inbox"), "{text}");
            assert!(!text.contains("105, 110, 98"), "{text}");
        }
        assert!(matches!(entries[1], Entry::Event { detail_bytes, .. } if detail_bytes > 0));
    }

    #[test]
    fn commit_sequence_kept() {
        let committed = Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: ulid::Ulid::from(3_u128),
            sequence: 9,
        });
        for (event, sequence) in [(committed, Some(9)), (Event::Delayed, None)] {
            let Entry::Event { commit, .. } = delivered(&event, 4096) else {
                panic!("an event is recorded as one");
            };
            assert_eq!(commit, sequence);
        }
    }

    // A large result is counted only up to the per-record bound; its text is never kept.
    // The budget counts each record's own storage, and the deque keeps the room it reserved.
    #[test]
    fn charge_matches_storage() {
        let mut buffer = Buffer::new();
        buffer.limit(3 * RECORD_BYTES);
        let reserved = buffer.records.capacity();
        for _ in 0..5 {
            buffer.push(1, 0, ended("Ok", &(), 64));
        }
        assert_eq!(buffer.records.len(), 3);
        assert_eq!(buffer.dropped, 2);
        assert_eq!(buffer.records.capacity(), reserved);
    }

    #[test]
    fn large_result_capped() {
        let large = vec![7u8; 1 << 20];
        let entry = ended("Ok", &large, 64);
        assert_eq!(
            entry,
            Entry::Ended {
                outcome: "Ok",
                outcome_bytes: 64
            }
        );
    }

    // A budget below one record refuses each record and keeps none.
    #[test]
    fn oversized_record_refused() {
        let mut buffer = Buffer::new();
        buffer.limit(RECORD_BYTES - 1);
        let refused = Entry::Ended {
            outcome: "Ok",
            outcome_bytes: 1,
        };
        buffer.push(1, 0, refused);
        let taken = buffer.take();
        assert_eq!(taken.dropped, 1);
        assert!(taken.records.is_empty());
    }

    #[test]
    fn drops_oldest_records() {
        let mut buffer = Buffer::new();
        buffer.limit(2 * RECORD_BYTES);
        for outcome in ["aaaa", "bbbb", "cccc"] {
            let outcome_bytes = 1;
            buffer.push(
                1,
                0,
                Entry::Ended {
                    outcome,
                    outcome_bytes,
                },
            );
        }
        let taken = buffer.take();
        assert_eq!(taken.dropped, 1);
        let sequences: Vec<u64> = taken.records.iter().map(|record| record.sequence).collect();
        assert_eq!(sequences, [2, 3]);
        assert_eq!(buffer.take(), Recorded::default());
    }
}
