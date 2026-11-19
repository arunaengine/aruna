//! Runs the steps a realm document write shares: the read, the write, the stale conflict deletes
//! and, when a role change narrowed access, the key epoch marks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::errors::StorageError;
use aruna_core::id_mint::IdMint;
use aruna_core::rows::values;
use aruna_core::types::{Key, KeySpace, Value};

use crate::auth::epoch_due::DueMarks;

/// What the write transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
    Marks,
}

/// What a realm document write does with the next answer.
pub(crate) enum WriteStep {
    /// The stored document and its reducer state, to build the write from.
    Read([Option<Value>; 2]),
    Call(IoCall),
    Done,
}

/// The shared state of a realm document write and the run's ids and time.
#[derive(Debug, PartialEq)]
pub(crate) struct WriteStage {
    stage: Stage,
    deletes: Vec<(KeySpace, Key)>,
    narrowed: bool,
    marks: DueMarks,
    pub(crate) ids: IdMint,
    pub(crate) now_ms: u64,
}

impl Default for WriteStage {
    fn default() -> Self {
        Self {
            stage: Stage::Read,
            deletes: Vec::new(),
            narrowed: false,
            marks: DueMarks::new(None, 0),
            ids: IdMint::default(),
            now_ms: 0,
        }
    }
}

impl WriteStage {
    /// Takes the run's time for the ids and the epoch marks.
    pub(crate) fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
        self.marks.stamp(now_ms);
    }

    /// Notes the sent write: `deletes` follow it, and a write that `narrowed` access marks key
    /// epochs due after them.
    pub(crate) fn written(&mut self, deletes: Vec<(KeySpace, Key)>, narrowed: bool) {
        self.deletes = deletes;
        self.narrowed = narrowed;
        self.stage = Stage::Write;
    }

    /// The step after `result`; a failed epoch mark is a storage write error.
    pub(crate) fn step(&mut self, result: IoResult) -> Result<WriteStep, IoError> {
        match self.stage {
            Stage::Read => Ok(WriteStep::Read(values::<2>(result)?)),
            Stage::Write if !self.deletes.is_empty() => {
                result.done()?;
                self.stage = Stage::Delete;
                let deletes = std::mem::take(&mut self.deletes);
                Ok(WriteStep::Call(IoCall::Delete(deletes)))
            }
            Stage::Write | Stage::Delete => {
                result.done()?;
                if !self.narrowed {
                    return Ok(WriteStep::Done);
                }
                self.stage = Stage::Marks;
                Ok(WriteStep::Call(self.marks.first()))
            }
            Stage::Marks => match self.marks.next(result, true) {
                Ok(Some(call)) => Ok(WriteStep::Call(call)),
                Ok(None) => Ok(WriteStep::Done),
                Err(error) => Err(IoError::Storage(StorageError::WriteError(
                    error.to_string(),
                ))),
            },
        }
    }
}
