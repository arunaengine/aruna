//! Runs the version rows of one migration run, several at once for a rename, and tells how far
//! its cursor may move.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

/// Why a run starts no more rows.
#[derive(Debug, PartialEq)]
pub(super) enum Stop {
    /// The run worked enough rows or long enough; the next run goes on at once.
    More,
    /// The caller paused or aborted the rename.
    Halted,
}

/// The version rows of one run in storage order, each with its key and the linked file name a
/// move gives it. The rows of one key are adjacent and run one after another; two rows of one
/// key or of one new name never run at once.
#[derive(Debug, PartialEq)]
pub(super) struct Batch {
    rows: Vec<(String, Option<String>)>,
    running: Vec<usize>,
    /// Rows whose earlier rows of the same key all finished.
    waiting: BTreeSet<usize>,
    finished: Vec<bool>,
    /// The first row of the next key no row has started of.
    next: usize,
    /// Rows finished in storage order from the first one on.
    done: usize,
}

impl Batch {
    pub(super) fn new(rows: Vec<(String, Option<String>)>) -> Self {
        let finished = vec![false; rows.len()];
        Self {
            rows,
            running: Vec::new(),
            waiting: BTreeSet::new(),
            finished,
            next: 0,
            done: 0,
        }
    }

    /// The next row that may start: the next version of a key whose last one finished, else
    /// the first version of the next key, unless a running row has its key or name.
    pub(super) fn ready(&self) -> Option<usize> {
        let free = |row: &usize| {
            !self
                .running
                .iter()
                .any(|running| self.clash(*running, *row))
        };
        let first = (self.next < self.rows.len()).then_some(self.next);
        self.waiting
            .iter()
            .copied()
            .find(free)
            .or(first.filter(free))
    }

    fn clash(&self, one: usize, other: usize) -> bool {
        let ((key, name), (other_key, other_name)) = (&self.rows[one], &self.rows[other]);
        key == other_key || (name.is_some() && name == other_name)
    }

    /// Marks `row` as finished by an earlier run, so it neither runs nor counts again. A waiting
    /// row hands its turn to the next unfinished row of its key.
    pub(super) fn passed(&mut self, row: usize) {
        self.finished[row] = true;
        if self.waiting.remove(&row) {
            let next = (row + 1..self.key_end(row)).find(|later| !self.finished[*later]);
            self.waiting.extend(next);
        }
        self.pass_finished();
    }

    pub(super) fn start(&mut self, row: usize) {
        self.waiting.remove(&row);
        if row == self.next {
            self.next = self.key_end(row);
            self.pass_finished();
        }
        self.running.push(row);
    }

    /// The row after the last row of `row`'s key.
    fn key_end(&self, row: usize) -> usize {
        let key = &self.rows[row].0;
        let end = (row + 1..self.rows.len()).find(|later| self.rows[*later].0 != *key);
        end.unwrap_or(self.rows.len())
    }

    /// Opens the keys at `next` whose first rows an earlier run finished; their next row waits.
    fn pass_finished(&mut self) {
        while self.finished.get(self.next) == Some(&true) {
            let (start, end) = (self.next, self.key_end(self.next));
            self.next = end;
            if let Some(row) = (start..end).find(|row| !self.finished[*row]) {
                self.waiting.insert(row);
            }
        }
        while self.finished.get(self.done) == Some(&true) {
            self.done += 1;
        }
    }

    /// The rows started and not finished.
    pub(super) fn running(&self) -> usize {
        self.running.len()
    }

    /// Takes back a started row that did no work; it stays unfinished.
    pub(super) fn abandon(&mut self, row: usize) {
        self.running.retain(|running| *running != row);
    }

    pub(super) fn finish(&mut self, row: usize) {
        self.running.retain(|running| *running != row);
        self.finished[row] = true;
        // The key's next unfinished row; one an earlier run finished never runs again.
        let next = (row + 1..self.key_end(row)).find(|later| !self.finished[*later]);
        self.waiting.extend(next);
        while self.finished.get(self.done) == Some(&true) {
            self.done += 1;
        }
    }

    /// Rows finished in storage order from the first one on; the cursor moves over these only.
    pub(super) fn done(&self) -> usize {
        self.done
    }

    /// The finished rows after the first unfinished one.
    pub(super) fn finished_after(&self) -> impl Iterator<Item = usize> + '_ {
        (self.done..self.rows.len()).filter(|row| self.finished[*row])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(keys: &[&str]) -> Batch {
        Batch::new(keys.iter().map(|key| (key.to_string(), None)).collect())
    }

    /// Starts the next ready row.
    fn start_next(rows: &mut Batch) -> Option<usize> {
        let row = rows.ready()?;
        rows.start(row);
        Some(row)
    }

    #[test]
    fn keeps_name_apart() {
        // Two keys that get one new name never run at once, and later keys wait behind them.
        let mut rows = Batch::new(vec![
            ("a".to_string(), Some("raw/x".to_string())),
            ("b".to_string(), Some("raw/x".to_string())),
            ("c".to_string(), None),
        ]);
        assert_eq!(rows.ready(), Some(0));
        rows.start(0);
        assert_eq!(rows.ready(), None);
        rows.finish(0);
        assert_eq!(rows.ready(), Some(1));
        rows.start(1);
        assert_eq!(rows.ready(), Some(2));
    }

    #[test]
    fn passes_finished_rows() {
        // Rows an earlier run finished never run again, and the cursor moves over them.
        let mut rows = batch(&["a", "b", "b", "c"]);
        rows.passed(1);
        assert_eq!(rows.ready(), Some(0));
        rows.start(0);
        assert_eq!(rows.ready(), Some(2));
        rows.start(2);
        assert_eq!(rows.ready(), Some(3));
        rows.start(3);
        rows.finish(3);
        assert_eq!(rows.finished_after().collect::<Vec<_>>(), [1, 3]);
        rows.finish(0);
        assert_eq!(
            (rows.done(), rows.finished_after().collect::<Vec<_>>()),
            (2, vec![3])
        );
        rows.finish(2);
        assert_eq!(rows.done(), 4);
    }

    #[test]
    fn passes_whole_keys() {
        // Rows of a key an earlier run finished never run again, also once the rows before them
        // are gone; the key's next unfinished row still runs.
        let mut rows = batch(&["a", "a"]);
        rows.passed(0);
        rows.passed(1);
        assert_eq!((rows.ready(), rows.done()), (None, 2));
        let mut rows = batch(&["a", "a", "a"]);
        rows.passed(0);
        rows.passed(1);
        assert_eq!(rows.ready(), Some(2));
    }

    #[test]
    fn skips_counted_successor() {
        // A version that appeared before one an earlier run finished: once it finishes, the
        // finished one does not run again, but a later unfinished version of the key does.
        let mut rows = batch(&["a", "b", "b", "b"]);
        rows.passed(2);
        for row in [0, 1] {
            assert_eq!(rows.ready(), Some(row));
            rows.start(row);
        }
        rows.finish(1);
        assert_eq!(rows.ready(), Some(3));
        rows.start(3);
        rows.finish(3);
        rows.finish(0);
        assert_eq!((rows.ready(), rows.done()), (None, 4));
    }

    #[test]
    fn runs_keys_together() {
        // Versions of several keys run at once.
        let mut rows = batch(&["a", "b", "c", "d"]);
        for expected in 0..4 {
            assert_eq!(start_next(&mut rows), Some(expected));
        }
        assert_eq!(rows.running(), 4);
        (0..4).for_each(|row| rows.finish(row));
        assert_eq!(rows.done(), 4);
    }

    #[test]
    fn orders_key_versions() {
        // The versions of one key run one after another, in storage order.
        let mut rows = batch(&["a", "a", "b", "b"]);
        assert_eq!(start_next(&mut rows), Some(0));
        assert_eq!(start_next(&mut rows), Some(2));
        assert_eq!(rows.ready(), None);
        rows.finish(2);
        assert_eq!(start_next(&mut rows), Some(3));
        assert_eq!(rows.ready(), None);
        rows.finish(0);
        rows.finish(3);
        assert_eq!(start_next(&mut rows), Some(1));
        rows.finish(1);
        assert_eq!(rows.done(), 4);
    }

    #[test]
    fn stops_mid_batch() {
        // The cursor stays before the first row that did not finish.
        let mut rows = batch(&["a", "a", "b"]);
        assert_eq!(start_next(&mut rows), Some(0));
        assert_eq!(start_next(&mut rows), Some(2));
        rows.finish(2);
        rows.finish(0);
        assert_eq!(rows.done(), 1);
        assert_eq!(rows.finished_after().collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn abandoned_row_unfinished() {
        // A row that stopped before its work frees its key and does not count.
        let mut rows = batch(&["a", "b"]);
        assert_eq!(start_next(&mut rows), Some(0));
        rows.abandon(0);
        assert_eq!((rows.running(), rows.done()), (0, 0));
    }
}
