//! Works through the due rows of the family prune index, one family at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::FAMILY_PRUNE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobFamilyId;
use aruna_core::structs::execution::job::record_keys::{PRUNE_DUE_PREFIX, parse_prune_due};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::Key;

use super::prune::{
    FAMILY_PRUNE_PAGE, FamilyPruneOutcome, PruneCommitOperation, PruneError, PruneStep,
};
use super::prune_family::PruneFamilyOperation;

/// Works through the due rows of the family prune index at the run's time.
#[derive(Debug, PartialEq)]
pub struct PruneFamiliesOperation {
    serving: Option<(NodeId, RealmId)>,
    now_ms: u64,
    due: VecDeque<(Key, Option<(u64, JobFamilyId)>)>,
    outcome: FamilyPruneOutcome,
    read: bool,
    /// The last child dropped a malformed row instead of pruning a family.
    dropping: bool,
}

type PassNext = Result<Next<FamilyPruneOutcome>, PruneError>;

impl PruneFamiliesOperation {
    pub fn new(serving: Option<(NodeId, RealmId)>) -> Self {
        Self {
            serving,
            now_ms: 0,
            due: VecDeque::new(),
            outcome: FamilyPruneOutcome::default(),
            read: false,
            dropping: false,
        }
    }

    fn next_row(&mut self) -> PassNext {
        let Some((key, parsed)) = self.due.pop_front() else {
            return Ok(Next::Done(std::mem::take(&mut self.outcome)));
        };
        self.dropping = parsed.is_none();
        let Some((due_ms, family)) = parsed else {
            let malformed = vec![(FAMILY_PRUNE_KEYSPACE.to_string(), key)];
            let commit = PruneCommitOperation::new(None, malformed, Vec::new());
            return Ok(Next::Call(IoCall::child(commit)));
        };
        if due_ms > self.now_ms {
            self.outcome.next_due_after = Some(Duration::from_millis(due_ms - self.now_ms));
            self.outcome.has_more = false;
            return Ok(Next::Done(std::mem::take(&mut self.outcome)));
        }
        let prune = PruneFamilyOperation::new(family, due_ms, self.serving);
        Ok(Next::Call(IoCall::child(prune)))
    }
}

impl Operation for PruneFamiliesOperation {
    type Output = FamilyPruneOutcome;
    type Error = PruneError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PassNext {
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_PRUNE_KEYSPACE.to_string(),
            prefix: Key::from([PRUNE_DUE_PREFIX].as_slice()),
            after: None,
            limit: FAMILY_PRUNE_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> PassNext {
        if !std::mem::replace(&mut self.read, true) {
            let IoResult::Page { rows, next } = result else {
                return Err(IoError::Unfinished.into());
            };
            self.outcome.has_more = next.is_some();
            self.due = (rows.into_iter())
                .map(|(key, _)| {
                    let parsed = parse_prune_due(&key);
                    (key, parsed)
                })
                .collect();
            return self.next_row();
        }
        if self.dropping {
            result.child::<bool, IoError>()??;
            return self.next_row();
        }
        match result.child::<PruneStep, PruneError>()?? {
            PruneStep::Pruned => self.outcome.pruned += 1,
            PruneStep::Paged => self.outcome.has_more = true,
            PruneStep::Moved => {}
        }
        self.next_row()
    }
}
