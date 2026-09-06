//! The per-session event log: a bounded ring the stream resumes from, plus the
//! caps that keep one runaway cell from filling it.

use serde::Serialize;
use std::collections::VecDeque;

/// Events one session keeps for a reconnecting client.
pub const MAX_RING_EVENTS: usize = 4096;
/// Bytes one session keeps for a reconnecting client.
pub const MAX_RING_BYTES: usize = 4 * 1024 * 1024;
/// Outputs one cell may emit before the rest are dropped.
pub const MAX_CELL_OUTPUTS: usize = 512;
/// Output bytes one cell may emit before the rest are dropped.
pub const MAX_CELL_OUTPUT_BYTES: usize = 1024 * 1024;
/// What the client sees once a cell hit either output cap.
pub const TRUNCATED_NOTICE: &str = "[output truncated by the node]";

/// Event types the stream carries. `session` is built on connect and is not
/// kept in the ring, so it never moves a resume point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
    Cell,
    Output,
    Kernel,
    Credential,
    Ended,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::Cell => "cell",
            EventKind::Output => "output",
            EventKind::Kernel => "kernel",
            EventKind::Credential => "credential",
            EventKind::Ended => "ended",
        }
    }
}

/// One stream frame. `data` is already serialized so the ring can measure it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionEvent {
    pub id: u64,
    pub kind: EventKind,
    pub data: String,
}

/// The bounded event history of one session.
#[derive(Debug, Default)]
pub struct EventRing {
    events: VecDeque<SessionEvent>,
    bytes: usize,
    last_id: u64,
}

impl EventRing {
    /// Appends one frame and returns it. The caller broadcasts what it gets
    /// back, so a live reader and a resuming reader see the same bytes.
    pub fn push<T: Serialize>(&mut self, kind: EventKind, data: &T) -> SessionEvent {
        let data = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string());
        self.last_id = self.last_id.saturating_add(1);
        let event = SessionEvent {
            id: self.last_id,
            kind,
            data,
        };
        self.bytes = self.bytes.saturating_add(event.data.len());
        self.events.push_back(event.clone());
        while self.events.len() > MAX_RING_EVENTS || self.bytes > MAX_RING_BYTES {
            match self.events.pop_front() {
                Some(dropped) => self.bytes = self.bytes.saturating_sub(dropped.data.len()),
                None => break,
            }
        }
        event
    }

    pub fn last_id(&self) -> u64 {
        self.last_id
    }

    /// The oldest id still held, or the next id when the ring is empty.
    pub fn first_id(&self) -> u64 {
        self.events
            .front()
            .map_or_else(|| self.last_id.saturating_add(1), |event| event.id)
    }

    /// Frames after `after`. `Err(from)` means the ring no longer holds that
    /// point, so the client is told about the gap instead of losing it
    /// silently.
    pub fn since(&self, after: u64) -> Result<Vec<SessionEvent>, u64> {
        if after < self.first_id().saturating_sub(1) {
            return Err(self.first_id());
        }
        Ok(self
            .events
            .iter()
            .filter(|event| event.id > after)
            .cloned()
            .collect())
    }
}

/// How much of one cell's output budget is spent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CellBudget {
    outputs: usize,
    bytes: usize,
    truncated: bool,
}

/// What the session does with one more output of a cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetVerdict {
    Keep,
    Truncate,
    Drop,
}

impl CellBudget {
    /// Charges one output. The first refusal reports `Truncate` so exactly one
    /// notice is appended; every later one is dropped.
    pub fn charge(&mut self, bytes: usize) -> BudgetVerdict {
        if self.truncated {
            return BudgetVerdict::Drop;
        }
        if self.outputs >= MAX_CELL_OUTPUTS
            || self.bytes.saturating_add(bytes) > MAX_CELL_OUTPUT_BYTES
        {
            self.truncated = true;
            return BudgetVerdict::Truncate;
        }
        self.outputs += 1;
        self.bytes = self.bytes.saturating_add(bytes);
        BudgetVerdict::Keep
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ring_reports_gap() {
        // A resume point older than the ring must be a gap, never silence.
        let mut ring = EventRing::default();
        for index in 0..(MAX_RING_EVENTS + 8) {
            ring.push(EventKind::Kernel, &json!({ "state": index }));
        }
        assert_eq!(ring.last_id(), (MAX_RING_EVENTS + 8) as u64);
        assert!(ring.since(1).is_err());
        let tail = ring
            .since(ring.last_id() - 2)
            .expect("recent point resumes");
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].id, ring.last_id() - 1);
    }

    #[test]
    fn ring_resumes_from_zero() {
        let mut ring = EventRing::default();
        ring.push(EventKind::Kernel, &json!({ "state": "idle" }));
        let all = ring.since(0).expect("a fresh ring holds everything");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, 1);
    }

    #[test]
    fn ring_drops_by_bytes() {
        let mut ring = EventRing::default();
        let chunk = "x".repeat(MAX_RING_BYTES / 4);
        for _ in 0..8 {
            ring.push(EventKind::Output, &json!({ "text": chunk }));
        }
        assert!(ring.bytes <= MAX_RING_BYTES);
        assert!(ring.events.len() < 8);
    }

    #[test]
    fn budget_truncates_once() {
        let mut budget = CellBudget::default();
        for _ in 0..MAX_CELL_OUTPUTS {
            assert_eq!(budget.charge(1), BudgetVerdict::Keep);
        }
        assert_eq!(budget.charge(1), BudgetVerdict::Truncate);
        assert_eq!(budget.charge(1), BudgetVerdict::Drop);
    }

    #[test]
    fn budget_counts_bytes() {
        let mut budget = CellBudget::default();
        assert_eq!(budget.charge(MAX_CELL_OUTPUT_BYTES), BudgetVerdict::Keep);
        assert_eq!(budget.charge(1), BudgetVerdict::Truncate);
    }
}
