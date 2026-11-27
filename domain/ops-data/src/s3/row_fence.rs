//! Guards a version transaction on rows it must read as given, and records its version there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::errors::StorageError;
use aruna_core::structs::storage::blob::CurrentVersionPointer;
use aruna_core::types::{Key, Value};
use ulid::Ulid;

/// Head rows one range page reads at most.
const RANGE_PAGE: usize = 256;

/// A head range that must hold no live head when the transaction commits. Its rows are read in
/// the transaction, so a concurrent insert below `prefix` makes the commit conflict.
#[derive(Clone, Debug, PartialEq)]
pub struct RangeGuard {
    pub key_space: String,
    pub prefix: Key,
    /// The one head below `prefix` that may stay live, such as the directory's own marker.
    pub exclude_key: Option<Key>,
}

/// Rows one version transaction must still read as given, checked before it writes its target
/// rows, and rows it writes, deletes or records its version id under after them. A concurrent
/// writer of a guard fails the commit, so its retry finds the guard taken.
#[derive(Clone, Debug, PartialEq)]
pub struct RowFence {
    guards: Vec<(String, Key, Option<Value>)>,
    receipts: Vec<(String, Key)>,
    writes: Vec<(String, Key, Value)>,
    deletes: Vec<(String, Key)>,
    ranges: Vec<RangeGuard>,
    /// The range being read and the key its next page continues after.
    range: usize,
    after: Option<Key>,
    step: FenceStep,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FenceStep {
    Unread,
    Reading,
    /// The single guards hold; the next range page is due.
    Ranges,
    Paging,
    Checked,
    Writing,
    Written,
    Deleting,
    Done,
}

/// Why a fenced transaction may not go on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FenceError {
    /// A guard no longer reads as given.
    Taken,
    Storage(StorageError),
    /// The event does not answer the effect the fence sent.
    Unexpected,
}

impl RowFence {
    pub fn new(guards: Vec<(String, Key, Option<Value>)>, receipts: Vec<(String, Key)>) -> Self {
        Self {
            guards,
            receipts,
            writes: Vec::new(),
            deletes: Vec::new(),
            ranges: Vec::new(),
            range: 0,
            after: None,
            step: FenceStep::Unread,
        }
    }

    /// Rows the transaction writes after its own mutation, such as receipts and markers.
    pub fn with_writes(mut self, writes: Vec<(String, Key, Value)>) -> Self {
        self.writes = writes;
        self
    }

    /// Rows the transaction deletes after its own mutation, such as watch keys.
    pub fn with_deletes(mut self, deletes: Vec<(String, Key)>) -> Self {
        self.deletes = deletes;
        self
    }

    /// Head ranges that must hold no live head, read after the single guards.
    pub fn with_range_guards(mut self, ranges: Vec<RangeGuard>) -> Self {
        self.ranges = ranges;
        self
    }

    /// The next guard read, made before the transaction writes or deletes any target row, so
    /// the read stays in its conflict set: the single guards, then each range page by page.
    /// `None` once every guard is checked.
    pub fn check_guards(&mut self) -> Option<IoCall> {
        if self.step == FenceStep::Unread {
            self.step = FenceStep::Ranges;
            if !self.guards.is_empty() {
                self.step = FenceStep::Reading;
                let reads = self.guards.iter();
                let reads = reads.map(|(space, key, _)| (space.clone(), key.clone()));
                return Some(IoCall::Read(reads.collect()));
            }
        }
        if self.step != FenceStep::Ranges {
            return None;
        }
        let Some(range) = self.ranges.get(self.range) else {
            self.step = FenceStep::Checked;
            return None;
        };
        self.step = FenceStep::Paging;
        Some(IoCall::Page {
            space: range.key_space.clone(),
            prefix: range.prefix.clone(),
            after: self.after.clone(),
            limit: RANGE_PAGE,
        })
    }

    /// Whether every row of one range page is excluded, a watch key or not live. A row that
    /// does not decode counts as live, so the range fails closed.
    fn page_free(&self, rows: &[(Key, Value)]) -> bool {
        let exclude = self
            .ranges
            .get(self.range)
            .and_then(|range| range.exclude_key.as_ref());
        rows.iter().all(|(key, value)| {
            let skipped = Some(key) == exclude || key.as_ref().first() == Some(&0);
            let head = CurrentVersionPointer::from_bytes(value.as_ref());
            skipped || head.is_ok_and(|head| head.life_id.is_none())
        })
    }

    /// The next write or delete after the transaction's own mutation, or `None` once done.
    /// Guards not checked yet are read first, so no fenced write ever skips them.
    pub fn apply_mutations(&mut self, version_id: Ulid) -> Option<IoCall> {
        if let Some(read) = self.check_guards() {
            return Some(read);
        }
        if self.step == FenceStep::Checked {
            self.step = FenceStep::Written;
            let value: Value = version_id.to_bytes().to_vec().into();
            let receipts = self.receipts.iter();
            let receipts = receipts.map(|(space, key)| (space.clone(), key.clone(), value.clone()));
            let writes: Vec<_> = receipts.chain(self.writes.iter().cloned()).collect();
            if !writes.is_empty() {
                self.step = FenceStep::Writing;
                return Some(IoCall::Write(writes));
            }
        }
        if self.step == FenceStep::Written {
            self.step = FenceStep::Done;
            if !self.deletes.is_empty() {
                self.step = FenceStep::Deleting;
                return Some(IoCall::Delete(self.deletes.clone()));
            }
        }
        None
    }

    /// Takes the result of the call `check_guards` or `apply_mutations` returned.
    pub fn answer_call(&mut self, result: IoResult) -> Result<(), FenceError> {
        let storage = |error: IoError| FenceError::Storage(error.into());
        match self.step {
            FenceStep::Reading => {
                let values = result.values().map_err(storage)?;
                let expected = self.guards.iter().map(|(_, _, value)| value);
                if values.len() != self.guards.len() || !values.iter().eq(expected) {
                    return Err(FenceError::Taken);
                }
                self.step = FenceStep::Ranges;
                Ok(())
            }
            FenceStep::Paging => {
                let (rows, next) = result.page().map_err(storage)?;
                if !self.page_free(&rows) {
                    return Err(FenceError::Taken);
                }
                match next {
                    Some(next) => self.after = Some(next),
                    None => (self.range, self.after) = (self.range + 1, None),
                }
                self.step = FenceStep::Ranges;
                Ok(())
            }
            FenceStep::Writing => {
                result.done().map_err(storage)?;
                self.step = FenceStep::Written;
                Ok(())
            }
            FenceStep::Deleting => {
                result.done().map_err(storage)?;
                self.step = FenceStep::Done;
                Ok(())
            }
            FenceStep::Unread
            | FenceStep::Ranges
            | FenceStep::Checked
            | FenceStep::Written
            | FenceStep::Done => Err(FenceError::Unexpected),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence() -> RowFence {
        let key: Key = b"receipt".to_vec().into();
        RowFence::new(
            vec![("receipts".to_string(), key.clone(), None)],
            vec![("receipts".to_string(), key)],
        )
    }

    fn read(value: Option<Value>) -> IoResult {
        IoResult::Values(vec![value])
    }

    #[test]
    fn records_free_guard() {
        // An absent guard lets the transaction record its version id under the receipt.
        let version_id = Ulid::from_parts(2, 2);
        let mut fence = fence();
        assert!(matches!(fence.check_guards(), Some(IoCall::Read(_))));
        assert_eq!(fence.answer_call(read(None)), Ok(()));
        assert_eq!(fence.check_guards(), None, "guards are read once");
        let Some(IoCall::Write(writes)) = fence.apply_mutations(version_id) else {
            panic!("the receipt is written in the transaction");
        };
        assert_eq!(writes[0].2.as_ref(), version_id.to_bytes().as_slice());
        assert_eq!(fence.answer_call(IoResult::Done), Ok(()));
        assert_eq!(fence.apply_mutations(version_id), None);
    }

    #[test]
    fn refuses_taken_guard() {
        // A transaction run again reads the receipt of the commit that won.
        let mut fence = fence();
        fence.check_guards();
        let winner: Value = Ulid::from_parts(3, 3).to_bytes().to_vec().into();
        assert_eq!(
            fence.answer_call(read(Some(winner))),
            Err(FenceError::Taken)
        );
    }

    #[test]
    fn rejects_stray_results() {
        // Only the answer to the call the fence made moves it on.
        let mut fence = fence();
        assert_eq!(
            fence.answer_call(IoResult::Done),
            Err(FenceError::Unexpected)
        );
        fence.check_guards();
        assert!(matches!(
            fence.answer_call(IoResult::Done),
            Err(FenceError::Storage(_))
        ));
    }

    #[test]
    fn split_fence_order() {
        // Guards are read before the mutation; writes, then deletes, follow it.
        let space = "nfs".to_string();
        let (guard, marker, watch): (Key, Key, Key) = (
            b"g".to_vec().into(),
            b"m".to_vec().into(),
            b"w".to_vec().into(),
        );
        let mut fence = RowFence::new(
            vec![
                (space.clone(), guard.clone(), None),
                (space.clone(), marker.clone(), None),
            ],
            Vec::new(),
        )
        .with_writes(vec![(
            space.clone(),
            marker.clone(),
            Value::from(Vec::new()),
        )])
        .with_deletes(vec![(space.clone(), watch.clone())]);
        let Some(IoCall::Read(reads)) = fence.check_guards() else {
            panic!("both guards are read first");
        };
        assert_eq!(
            reads,
            [(space.clone(), guard), (space.clone(), marker.clone())]
        );
        let free = IoResult::Values(vec![None, None]);
        assert_eq!(fence.answer_call(free), Ok(()));
        let version_id = Ulid::from_parts(4, 4);
        let Some(IoCall::Write(writes)) = fence.apply_mutations(version_id) else {
            panic!("the marker is written after the mutation");
        };
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].1, marker);
        assert_eq!(
            fence.apply_mutations(version_id),
            None,
            "a write in flight sends nothing more"
        );
        assert_eq!(fence.answer_call(IoResult::Done), Ok(()));
        assert_eq!(
            fence.apply_mutations(version_id),
            Some(IoCall::Delete(vec![(space, watch)]))
        );
        assert_eq!(fence.answer_call(IoResult::Done), Ok(()));
        assert_eq!(fence.apply_mutations(version_id), None);
    }

    #[test]
    fn mutations_read_guards() {
        // A path that skipped the guard step still reads the guards before any write.
        let mut fence = fence();
        let version_id = Ulid::from_parts(5, 5);
        assert!(matches!(
            fence.apply_mutations(version_id),
            Some(IoCall::Read(_))
        ));
        assert_eq!(fence.answer_call(read(None)), Ok(()));
        assert!(matches!(
            fence.apply_mutations(version_id),
            Some(IoCall::Write(_))
        ));
    }

    fn head_row(key: &[u8], life: Option<u128>) -> (Key, Value) {
        let life = life.map(|seed| Ulid::from_parts(9, seed));
        let head = CurrentVersionPointer::new(Ulid::from_parts(1, 1)).with_life(life);
        (key.to_vec().into(), head.to_bytes().unwrap().into())
    }

    fn ranged(exclude: Option<&[u8]>) -> RowFence {
        RowFence::new(Vec::new(), Vec::new()).with_range_guards(vec![RangeGuard {
            key_space: "heads".to_string(),
            prefix: b"b/d/".to_vec().into(),
            exclude_key: exclude.map(|key| key.to_vec().into()),
        }])
    }

    #[test]
    fn range_refuses_live() {
        // A live head below the prefix, a hidden S3 key included, fails the fence in its read.
        let mut fence = ranged(None);
        let Some(IoCall::Page { prefix, after, .. }) = fence.check_guards() else {
            panic!("the range is paged in the transaction");
        };
        assert_eq!((prefix.as_ref(), after), (&b"b/d/"[..], None));
        let rows = vec![head_row(b"b/d/x", Some(1))];
        let page = IoResult::Page { rows, next: None };
        assert_eq!(fence.answer_call(page), Err(FenceError::Taken));
    }

    #[test]
    fn range_skips_markers() {
        // Delete markers, watch keys and the excluded key are no live child; pages continue.
        let mut fence = ranged(Some(b"b/d/"));
        fence.check_guards();
        let rows = vec![head_row(b"b/d/", Some(2)), head_row(b"b/d/gone", None)];
        let next: Key = b"b/d/gone".to_vec().into();
        let page = IoResult::Page {
            rows,
            next: Some(next.clone()),
        };
        assert_eq!(fence.answer_call(page), Ok(()));
        let Some(IoCall::Page { after, .. }) = fence.check_guards() else {
            panic!("the next page follows");
        };
        assert_eq!(after, Some(next));
        let watch = (Key::from(vec![0, b'b']), Value::from(vec![1]));
        let page = IoResult::Page {
            rows: vec![watch],
            next: None,
        };
        assert_eq!(fence.answer_call(page), Ok(()));
        assert_eq!(fence.check_guards(), None);
        assert_eq!(fence.apply_mutations(Ulid::from_parts(6, 6)), None);
    }

    #[test]
    fn ranges_follow_guards() {
        // A path that skipped the guard step still reads every range before any write.
        let key: Key = b"g".to_vec().into();
        let mut fence = ranged(None).with_writes(vec![(
            "rows".to_string(),
            key.clone(),
            Value::from(vec![1]),
        )]);
        fence.guards = vec![("rows".to_string(), key, None)];
        let version_id = Ulid::from_parts(7, 7);
        assert!(matches!(
            fence.apply_mutations(version_id),
            Some(IoCall::Read(_))
        ));
        assert_eq!(fence.answer_call(read(None)), Ok(()));
        assert!(matches!(
            fence.apply_mutations(version_id),
            Some(IoCall::Page { .. })
        ));
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(fence.answer_call(empty), Ok(()));
        assert!(matches!(
            fence.apply_mutations(version_id),
            Some(IoCall::Write(_))
        ));
    }

    #[test]
    fn several_guards_compared() {
        // Every guard must match: a changed second guard refuses the transaction.
        let space = "rows".to_string();
        let held: Value = b"held".to_vec().into();
        let mut fence = RowFence::new(
            vec![
                (space.clone(), b"a".to_vec().into(), Some(held.clone())),
                (space, b"b".to_vec().into(), None),
            ],
            Vec::new(),
        );
        fence.check_guards();
        let changed = IoResult::Values(vec![Some(held.clone()), Some(held)]);
        assert_eq!(fence.answer_call(changed), Err(FenceError::Taken));
    }
}
