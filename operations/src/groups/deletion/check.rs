//! Reports what blocks a group deletion on each realm node, without changing any state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::calls::{IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::group_delete::GroupDeletionError;
use aruna_core::types::GroupId;

use super::resources::EmptyCheck;

/// Runs the deletion emptiness scan in one read snapshot.
#[derive(Debug, PartialEq)]
pub(super) struct CheckOperation {
    scan: EmptyCheck,
}

impl CheckOperation {
    pub fn new(group_id: GroupId, now: SystemTime) -> Self {
        Self {
            scan: EmptyCheck::new(group_id, now),
        }
    }
}

impl Operation for CheckOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        Ok(Next::Call(self.scan.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        Ok(match self.scan.take(result)? {
            Some(call) => Next::Call(call),
            None => Next::Done(()),
        })
    }
}
