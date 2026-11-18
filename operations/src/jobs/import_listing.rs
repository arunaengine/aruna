//! Notes one listed backend object as imported, guarded by everything the listing read, in one
//! write transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, IMPORT_INTENT_KEYSPACE, IMPORTED_OBJECT_KEYSPACE, PURGE_FENCE_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, done, values};
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::s3::imports::folders::shares_folder;
use crate::s3::purge_fence::{PurgeFenceError, check_fence_row, fence_key};

/// Rules one page of the folder sharing check reads.
const SHARING_PAGE: usize = 128;

#[derive(Debug, Error, PartialEq)]
pub(crate) enum NoteError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("{0}")]
    System(String),
}

/// What else a row update reads, writes and deletes in its transaction.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ListingNote {
    /// Values that must still be stored as they were read.
    pub guards: Vec<(String, Key, Option<Value>)>,
    /// The write intents the committed row makes obsolete.
    pub retired: Vec<Key>,
    /// Rows the committed row comes with.
    pub writes: Vec<(String, Key, Value)>,
    /// The rules that must share the folder at commit, read again in the transaction.
    pub sharing: Option<BTreeSet<Ulid>>,
    /// The bucket key a permanent purge must not hold at commit.
    pub fenced: Option<String>,
}

#[derive(Debug, PartialEq)]
enum NoteStep {
    Guards,
    Sharing,
    Fence,
    Write,
    Retire,
}

/// Replaces the imported-object row at `row_key` only while the rule exists and the row still
/// holds `seen` (`None`: no row), and answers whether it wrote. Otherwise nothing is written and
/// the next run looks at the object again.
#[derive(Debug, PartialEq)]
pub(crate) struct NoteListingOperation {
    rule: BackendImport,
    row_key: Key,
    seen: Option<Value>,
    row: Value,
    note: ListingNote,
    pages: PrefixPages,
    found: BTreeSet<Ulid>,
    step: NoteStep,
}

type NoteNext = Result<Next<bool>, NoteError>;

impl NoteListingOperation {
    pub(crate) fn new(
        rule: BackendImport,
        row_key: Key,
        seen: Option<Value>,
        row: Value,
        note: ListingNote,
    ) -> Self {
        let prefix = rule.group_id.to_bytes().to_vec();
        Self {
            pages: PrefixPages::new(BACKEND_IMPORT_KEYSPACE, prefix, SHARING_PAGE),
            rule,
            row_key,
            seen,
            row,
            note,
            found: BTreeSet::new(),
            step: NoteStep::Guards,
        }
    }

    fn held(&self, values: &[Option<Value>]) -> bool {
        let expected = self.note.guards.iter().map(|(.., value)| value.as_ref());
        match values {
            [Some(_), value, rest @ ..] if value.as_ref() == self.seen.as_ref() => {
                rest.len() == self.note.guards.len() && rest.iter().map(Option::as_ref).eq(expected)
            }
            _ => false,
        }
    }

    fn sharing(&mut self, result: IoResult) -> NoteNext {
        for (_, value) in self.pages.take(result)? {
            let other = BackendImport::from_bytes(&value)
                .map_err(|error| NoteError::System(error.to_string()))?;
            if shares_folder(&self.rule, &other) {
                self.found.insert(other.import_id);
            }
        }
        if !self.pages.done() {
            return Ok(Next::Call(self.pages.call()));
        }
        if self.note.sharing.as_ref() != Some(&self.found) {
            return Ok(Next::Done(false));
        }
        self.fence()
    }

    fn fence(&mut self) -> NoteNext {
        if self.note.fenced.is_none() {
            return self.write();
        }
        self.step = NoteStep::Fence;
        let row = (
            PURGE_FENCE_KEYSPACE.to_string(),
            fence_key(&self.rule.bucket),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn write(&mut self) -> NoteNext {
        self.step = NoteStep::Write;
        let row = (
            IMPORTED_OBJECT_KEYSPACE.to_string(),
            self.row_key.clone(),
            self.row.clone(),
        );
        let mut writes = vec![row];
        writes.extend(std::mem::take(&mut self.note.writes));
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for NoteListingOperation {
    type Output = bool;
    type Error = NoteError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> NoteNext {
        let mut reads = vec![
            (BACKEND_IMPORT_KEYSPACE.to_string(), self.rule.key()),
            (IMPORTED_OBJECT_KEYSPACE.to_string(), self.row_key.clone()),
        ];
        let guards = self.note.guards.iter();
        reads.extend(guards.map(|(space, key, _)| (space.clone(), key.clone())));
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> NoteNext {
        match self.step {
            NoteStep::Guards => {
                let IoResult::Values(values) = result else {
                    return Err(IoError::Unexpected {
                        expected: "IoResult::Values",
                        got: result.label(),
                    }
                    .into());
                };
                if !self.held(&values) {
                    return Ok(Next::Done(false));
                }
                if self.note.sharing.is_none() {
                    return self.fence();
                }
                self.step = NoteStep::Sharing;
                Ok(Next::Call(self.pages.call()))
            }
            NoteStep::Sharing => self.sharing(result),
            NoteStep::Fence => {
                let [value] = values::<1>(result)?;
                let key = self.note.fenced.as_deref().unwrap_or_default();
                match check_fence_row(value.as_deref(), &self.rule.bucket, key) {
                    Ok(()) => self.write(),
                    Err(PurgeFenceError::Suspended) => Ok(Next::Done(false)),
                    Err(error) => Err(NoteError::System(error.to_string())),
                }
            }
            NoteStep::Write => {
                done(result)?;
                self.step = NoteStep::Retire;
                let retired = std::mem::take(&mut self.note.retired).into_iter();
                let deletes = retired.map(|key| (IMPORT_INTENT_KEYSPACE.to_string(), key));
                Ok(Next::Call(IoCall::Delete(deletes.collect())))
            }
            NoteStep::Retire => {
                done(result)?;
                Ok(Next::Done(true))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::store::mutation::{Rows, run_in_memory};

    fn rule() -> BackendImport {
        BackendImport {
            import_id: Ulid::from_parts(4, 4),
            group_id: Ulid::from_parts(3, 3),
            backend_id: Ulid::from_parts(5, 5),
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: "in/".to_string(),
            schedule: None,
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: "{dir}{name}".to_string(),
        }
    }

    fn row(space: &str, key: &Key) -> (String, Vec<u8>) {
        (space.to_string(), key.to_vec())
    }

    // A note writes the row and retires its intents while the rule and every guard still hold.
    #[test]
    fn writes_while_guards_hold() {
        let rule = rule();
        let rule_row = (
            row(BACKEND_IMPORT_KEYSPACE, &rule.key()),
            rule.to_bytes().unwrap(),
        );
        let (row_key, intent) = (Key::from(b"row".to_vec()), Key::from(b"intent".to_vec()));
        let guard = (
            "guard".to_string(),
            Key::from(b"g".to_vec()),
            Some(Value::from(vec![1])),
        );
        let note = || ListingNote {
            guards: vec![guard.clone()],
            retired: vec![intent.clone()],
            ..ListingNote::default()
        };
        let build = |note| {
            NoteListingOperation::new(rule.clone(), row_key.clone(), None, vec![9].into(), note)
        };
        let mut rows = Rows::from([
            rule_row.clone(),
            (row(IMPORT_INTENT_KEYSPACE, &intent), vec![1]),
        ]);
        let moved = rows.clone();
        assert_eq!(build(note()).transaction(), TransactionMode::Write);
        assert_eq!(run_in_memory(build(note()), &mut rows), Ok(false));
        assert_eq!(rows, moved);

        rows.insert(row("guard", &guard.1), vec![1]);
        assert_eq!(run_in_memory(build(note()), &mut rows), Ok(true));
        assert_eq!(rows[&row(IMPORTED_OBJECT_KEYSPACE, &row_key)], vec![9]);
        assert!(!rows.contains_key(&row(IMPORT_INTENT_KEYSPACE, &intent)));
        let written = rows.clone();
        assert_eq!(run_in_memory(build(note()), &mut rows), Ok(false));
        assert_eq!(rows, written);
    }

    // A note expecting another rule in the folder is refused once that rule is gone.
    #[test]
    fn refuses_changed_sharing() {
        let rule = rule();
        let rule_row = (
            row(BACKEND_IMPORT_KEYSPACE, &rule.key()),
            rule.to_bytes().unwrap(),
        );
        let row_key = Key::from(b"row".to_vec());
        let shared = ListingNote {
            sharing: Some(BTreeSet::from([Ulid::from_parts(9, 9)])),
            ..ListingNote::default()
        };
        let operation =
            NoteListingOperation::new(rule.clone(), row_key.clone(), None, vec![9].into(), shared);
        let mut rows = Rows::from([rule_row.clone()]);
        assert_eq!(run_in_memory(operation, &mut rows), Ok(false));

        let alone = ListingNote {
            sharing: Some(BTreeSet::new()),
            ..ListingNote::default()
        };
        let operation =
            NoteListingOperation::new(rule.clone(), row_key, None, vec![9].into(), alone);
        assert_eq!(run_in_memory(operation, &mut rows), Ok(true));
    }
}
