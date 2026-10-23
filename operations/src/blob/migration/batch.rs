//! Runs the version rows of one migration run, several at once for a rename, and tells how far
//! its cursor may move.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::collections::BTreeSet;
use std::future::Future;

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
    fn ready(&self) -> Option<usize> {
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

    fn start(&mut self, row: usize) {
        self.waiting.remove(&row);
        if row == self.next {
            let key = &self.rows[row].0;
            let end = (row + 1..self.rows.len()).find(|later| self.rows[*later].0 != *key);
            self.next = end.unwrap_or(self.rows.len());
        }
        self.running.push(row);
    }

    fn finish(&mut self, row: usize) {
        self.running.retain(|running| *running != row);
        self.finished[row] = true;
        if row + 1 < self.next && self.rows[row + 1].0 == self.rows[row].0 {
            self.waiting.insert(row + 1);
        }
        while self.finished.get(self.done) == Some(&true) {
            self.done += 1;
        }
    }

    /// Rows finished in storage order from the first one on; the cursor moves over these only.
    pub(super) fn done(&self) -> usize {
        self.done
    }
}

/// Starts the rows of `batch` in order, at most `limit` at once, while `proceed` agrees before
/// each start. Rows already started always finish, also after a stop or an error, and their
/// results come back by row.
pub(super) async fn run_rows<T, F, P>(
    batch: &mut Batch,
    limit: usize,
    mut start: impl FnMut(usize) -> F,
    mut proceed: impl FnMut() -> P,
) -> (Vec<Option<T>>, Result<Option<Stop>, String>)
where
    F: Future<Output = T>,
    P: Future<Output = Result<Option<Stop>, String>>,
{
    let mut results: Vec<Option<T>> = batch.rows.iter().map(|_| None).collect();
    let mut running = FuturesUnordered::new();
    let mut stop = Ok(None);
    loop {
        while matches!(stop, Ok(None))
            && running.len() < limit
            && let Some(row) = batch.ready()
        {
            stop = proceed().await;
            if !matches!(stop, Ok(None)) {
                break;
            }
            batch.start(row);
            let started = start(row);
            running.push(async move { (row, started.await) });
        }
        let Some((row, result)) = running.next().await else {
            return (results, stop);
        };
        batch.finish(row);
        results[row] = Some(result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{mpsc, oneshot};

    fn batch(keys: &[&str]) -> Batch {
        Batch::new(keys.iter().map(|key| (key.to_string(), None)).collect())
    }

    /// A row that runs until the test finishes it.
    type Started = (usize, oneshot::Sender<()>);

    /// Each started row reports on the channel and runs until its sender is used.
    fn reporting(starts: mpsc::UnboundedSender<Started>) -> impl FnMut(usize) -> Running {
        move |row| {
            let (finish, finished) = oneshot::channel();
            starts.send((row, finish)).unwrap();
            Box::pin(async move {
                finished.await.unwrap();
                row
            })
        }
    }

    type Running = std::pin::Pin<Box<dyn Future<Output = usize> + Send>>;

    /// Agrees to `allowed` starts, then answers `then`.
    fn allowing(
        mut allowed: usize,
        then: Stop,
    ) -> impl FnMut() -> std::future::Ready<Result<Option<Stop>, String>> {
        let mut then = Some(then);
        move || {
            let answer = match allowed {
                0 => then.take(),
                _ => {
                    allowed -= 1;
                    None
                }
            };
            std::future::ready(Ok(answer))
        }
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

    #[tokio::test]
    async fn runs_keys_together() {
        // Versions of several keys run at once, up to the limit.
        let (sender, mut starts) = mpsc::unbounded_channel();
        let mut rows = batch(&["a", "b", "c", "d"]);
        let run = run_rows(&mut rows, 3, reporting(sender), allowing(9, Stop::More));
        let control = async {
            let mut open = Vec::new();
            for expected in 0..3 {
                let (row, finish) = starts.recv().await.unwrap();
                assert_eq!(row, expected);
                open.push(finish);
            }
            assert!(starts.try_recv().is_err());
            open.drain(..).for_each(|finish| finish.send(()).unwrap());
            let (row, finish) = starts.recv().await.unwrap();
            assert_eq!(row, 3);
            finish.send(()).unwrap();
        };
        let ((results, stop), ()) = tokio::join!(run, control);
        assert_eq!(results, vec![Some(0), Some(1), Some(2), Some(3)]);
        assert_eq!(stop, Ok(None));
        assert_eq!(rows.done(), 4);
    }

    #[tokio::test]
    async fn orders_key_versions() {
        // The versions of one key run one after another, in storage order.
        let (sender, mut starts) = mpsc::unbounded_channel();
        let mut rows = batch(&["a", "a", "b", "b"]);
        let run = run_rows(&mut rows, 8, reporting(sender), allowing(9, Stop::More));
        let control = async {
            let (first, finish_first) = starts.recv().await.unwrap();
            let (second, finish_second) = starts.recv().await.unwrap();
            assert_eq!((first, second), (0, 2));
            assert!(starts.try_recv().is_err());
            finish_second.send(()).unwrap();
            let (row, finish) = starts.recv().await.unwrap();
            assert_eq!(row, 3);
            assert!(starts.try_recv().is_err());
            finish_first.send(()).unwrap();
            finish.send(()).unwrap();
            let (row, finish) = starts.recv().await.unwrap();
            assert_eq!(row, 1);
            finish.send(()).unwrap();
        };
        let ((results, _), ()) = tokio::join!(run, control);
        assert_eq!(results, vec![Some(0), Some(1), Some(2), Some(3)]);
    }

    #[tokio::test]
    async fn stops_mid_batch() {
        // A pause starts nothing more, rows already started still finish, and the cursor stops
        // before the first row that did not finish.
        let (sender, mut starts) = mpsc::unbounded_channel();
        let mut rows = batch(&["a", "a", "b"]);
        let run = run_rows(&mut rows, 8, reporting(sender), allowing(2, Stop::Halted));
        let control = async {
            let (first, finish_first) = starts.recv().await.unwrap();
            let (second, finish_second) = starts.recv().await.unwrap();
            assert_eq!((first, second), (0, 2));
            finish_second.send(()).unwrap();
            finish_first.send(()).unwrap();
        };
        let ((results, stop), ()) = tokio::join!(run, control);
        assert!(starts.try_recv().is_err());
        assert_eq!(results, vec![Some(0), None, Some(2)]);
        assert_eq!(stop, Ok(Some(Stop::Halted)));
        assert_eq!(rows.done(), 1);
    }

    #[tokio::test]
    async fn finishes_after_error() {
        // A failed check stops the run, but the row already running still finishes.
        let (sender, mut starts) = mpsc::unbounded_channel();
        let mut rows = batch(&["a", "b"]);
        let mut checks = 0;
        let proceed = move || {
            checks += 1;
            std::future::ready(match checks {
                1 => Ok(None),
                _ => Err("unreadable".to_string()),
            })
        };
        let run = run_rows(&mut rows, 8, reporting(sender), proceed);
        let control = async {
            let (row, finish) = starts.recv().await.unwrap();
            assert_eq!(row, 0);
            finish.send(()).unwrap();
        };
        let ((results, stop), ()) = tokio::join!(run, control);
        assert_eq!(results, vec![Some(0), None]);
        assert_eq!(stop, Err("unreadable".to_string()));
        assert_eq!(rows.done(), 1);
    }
}
