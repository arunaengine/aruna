//! Reads what linked storage records: a bucket's routing, second copies and the rules on a backend.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::groups::routing::{BucketRules, RoutingInputsError, SnapshotOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, IMPORTED_OBJECT_KEYSPACE, RENAMED_ROW_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::storage::backend_import::{BackendImport, ImportedObject};
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, BucketInfo, VersionKey};
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_core::types::{GroupId, Key, Value};
use std::collections::VecDeque;
use thiserror::Error;
use ulid::Ulid;

const RULE_PAGE: usize = 128;

#[derive(Debug, Error, PartialEq)]
pub enum ImportRowsError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Routing(#[from] RoutingInputsError),
}

/// The routing a move resolves against; `None` once the bucket is gone.
#[derive(Debug, PartialEq)]
pub struct BucketRoutingOperation {
    routing: NodeRouting,
    bucket: String,
    resolving: bool,
}

type RoutingTurn = Result<Next<Option<RoutingSnapshot>>, ImportRowsError>;

impl BucketRoutingOperation {
    pub fn new(routing: NodeRouting, bucket: &str) -> Self {
        Self {
            routing,
            bucket: bucket.to_string(),
            resolving: false,
        }
    }
}

impl Operation for BucketRoutingOperation {
    type Output = Option<RoutingSnapshot>;
    type Error = ImportRowsError;

    fn first(&mut self) -> RoutingTurn {
        let key = self.bucket.as_bytes().to_vec();
        Ok(Next::Call(read_row(S3_BUCKET_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> RoutingTurn {
        if self.resolving {
            let snapshot = result.child::<RoutingSnapshot, RoutingInputsError>()??;
            return Ok(Next::Done(Some(snapshot)));
        }
        let Some(info) = result.value()? else {
            return Ok(Next::Done(None));
        };
        let info = BucketInfo::from_bytes(&info)?;
        self.resolving = true;
        let rules = BucketRules::of(&info);
        let snapshot = SnapshotOperation::new(&self.routing, info.group_id, rules);
        Ok(Next::Call(IoCall::child(snapshot)))
    }
}

/// A second copy: the file, its key index row and the version its row names.
pub type SecondCopy = (BackendLocation, Key, Ulid);

type CopiesTurn = Result<Next<Vec<SecondCopy>>, ImportRowsError>;

/// The second copies of `key` an import registered for one of `versions`: files with a
/// version's content whose row names that version.
#[derive(Debug, PartialEq)]
pub struct SecondCopiesOperation {
    key: String,
    versions: Vec<Ulid>,
    prefix: Vec<u8>,
    imports: VecDeque<Ulid>,
    import: Option<Ulid>,
    pages: Option<PrefixPages>,
    rows: VecDeque<(Key, Value)>,
    /// The index row and file whose import row is being read.
    reading: Option<(Key, Value)>,
    found: Vec<SecondCopy>,
}

impl SecondCopiesOperation {
    pub fn new(
        routing: Option<&RoutingSnapshot>,
        (bucket, key): (&str, &str),
        versions: &[Ulid],
    ) -> Result<Self, ConversionError> {
        let rules = routing.map_or(&[][..], |routing| routing.imports.as_slice());
        Ok(Self {
            key: key.to_string(),
            versions: versions.to_vec(),
            prefix: VersionKey::object_prefix(bucket, key)?,
            imports: (rules.iter())
                .filter(|rule| rule.bucket == bucket)
                .map(|rule| rule.import_id)
                .collect(),
            import: None,
            pages: None,
            rows: VecDeque::new(),
            reading: None,
            found: Vec::new(),
        })
    }

    fn scan(&self, import: Ulid) -> Vec<u8> {
        [&import.to_bytes()[..], &self.prefix[..]].concat()
    }

    fn next_import(&mut self) -> CopiesTurn {
        let Some(import) = self.imports.pop_front() else {
            return Ok(Next::Done(std::mem::take(&mut self.found)));
        };
        let pages = PrefixPages::new(RENAMED_ROW_KEYSPACE, self.scan(import), RULE_PAGE);
        let page = pages.call();
        self.import = Some(import);
        self.pages = Some(pages);
        Ok(Next::Call(page))
    }

    fn next_row(&mut self) -> CopiesTurn {
        let Some(import) = self.import else {
            return self.next_import();
        };
        let scan_len = self.scan(import).len();
        while let Some((index, value)) = self.rows.pop_front() {
            // A renamed row names no file of its own.
            if value.is_empty() {
                continue;
            }
            let path = index.get(scan_len..).unwrap_or_default();
            let row_key = [&import.to_bytes()[..], path].concat();
            self.reading = Some((index, value));
            return Ok(Next::Call(read_row(IMPORTED_OBJECT_KEYSPACE, row_key)));
        }
        match self.pages.as_ref() {
            Some(pages) if !pages.done() => Ok(Next::Call(pages.call())),
            _ => self.next_import(),
        }
    }

    fn row_read(&mut self, (index, file): (Key, Value), row: Option<Value>) -> CopiesTurn {
        if let Some(row) = row {
            let row = ImportedObject::from_bytes(&row)?;
            let named = row.key.as_deref() == Some(self.key.as_str());
            if named && self.versions.contains(&row.version_id) {
                let file = BackendLocation::from_bytes(&file)?;
                self.found.push((file, index, row.version_id));
            }
        }
        self.next_row()
    }
}

impl Operation for SecondCopiesOperation {
    type Output = Vec<SecondCopy>;
    type Error = ImportRowsError;

    fn first(&mut self) -> CopiesTurn {
        self.next_import()
    }

    fn next(&mut self, result: IoResult) -> CopiesTurn {
        if let Some(reading) = self.reading.take() {
            let row = result.value()?;
            return self.row_read(reading, row);
        }
        if let Some(pages) = self.pages.as_mut() {
            self.rows = pages.take(result)?.into();
        }
        self.next_row()
    }
}

type RulesTurn = Result<Next<Vec<BackendImport>>, ImportRowsError>;

/// The group's rules that read or write on `backend`, whose rows may record a copy.
#[derive(Debug, PartialEq)]
pub struct BackendRulesOperation {
    backend: BackendRef,
    pages: PrefixPages,
    rules: Vec<BackendImport>,
}

impl BackendRulesOperation {
    pub fn new(group_id: GroupId, backend: BackendRef) -> Self {
        let prefix = group_id.to_bytes().to_vec();
        Self {
            backend,
            pages: PrefixPages::new(BACKEND_IMPORT_KEYSPACE, prefix, RULE_PAGE),
            rules: Vec::new(),
        }
    }
}

impl Operation for BackendRulesOperation {
    type Output = Vec<BackendImport>;
    type Error = ImportRowsError;

    fn first(&mut self) -> RulesTurn {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> RulesTurn {
        for (_, value) in self.pages.take(result)? {
            let rule = BackendImport::from_bytes(&value)?;
            let on = |id: Ulid| BackendRef::Group(id) == self.backend;
            if on(rule.backend_id) || on(rule.write_backend()) {
                self.rules.push(rule);
            }
        }
        if self.pages.done() {
            return Ok(Next::Done(std::mem::take(&mut self.rules)));
        }
        Ok(Next::Call(self.pages.call()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::bucket::rename::tests::{BUCKET, group, rule};

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Page { rows, next }
    }

    fn routed() -> RoutingSnapshot {
        let mut snapshot = NodeRouting::default().snapshot(group());
        snapshot.imports = vec![rule()];
        snapshot
    }

    fn copies(versions: &[Ulid]) -> SecondCopiesOperation {
        let routing = routed();
        SecondCopiesOperation::new(Some(&routing), (BUCKET, "gone/a"), versions).unwrap()
    }

    fn index_row(operation: &SecondCopiesOperation, path: &str) -> Key {
        let import = rule().import_id;
        [&operation.scan(import)[..], path.as_bytes()]
            .concat()
            .into()
    }

    #[test]
    fn gone_bucket_unrouted() {
        let mut operation = BucketRoutingOperation::new(NodeRouting::default(), BUCKET);
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        let turn = operation.next(IoResult::Values(vec![None]));
        assert_eq!(turn, Ok(Next::Done(None)));
    }

    #[test]
    fn ruleless_skips_scan() {
        let mut operation =
            SecondCopiesOperation::new(None, (BUCKET, "gone/a"), &[Ulid::nil()]).unwrap();
        assert_eq!(operation.first(), Ok(Next::Done(Vec::new())));
    }

    #[test]
    fn renamed_rows_skipped() {
        let mut operation = copies(&[Ulid::nil()]);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let renamed = (index_row(&operation, "raw"), Value::from(Vec::new()));
        let turn = operation.next(page(vec![renamed], None));
        assert_eq!(turn, Ok(Next::Done(Vec::new())));
    }

    #[test]
    fn missing_row_skipped() {
        let mut operation = copies(&[Ulid::nil()]);
        operation.first().unwrap();
        let row = (index_row(&operation, "raw"), Value::from(vec![1]));
        let turn = operation.next(page(vec![row], None));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
        let turn = operation.next(IoResult::Values(vec![None]));
        assert_eq!(turn, Ok(Next::Done(Vec::new())));
    }

    #[test]
    fn scan_follows_pages() {
        let mut operation = copies(&[Ulid::nil()]);
        operation.first().unwrap();
        let cursor = Key::from(vec![9]);
        let turn = operation.next(page(Vec::new(), Some(cursor.clone())));
        assert!(matches!(
            turn,
            Ok(Next::Call(IoCall::Page { after: Some(after), .. })) if after == cursor
        ));
    }

    #[test]
    fn rules_filter_backend() {
        let backend = BackendRef::Group(rule().backend_id);
        let mut operation = BackendRulesOperation::new(group(), backend);
        operation.first().unwrap();
        let mut other = rule();
        other.backend_id = Ulid::from_bytes([9; 16]);
        other.write_backend_id = None;
        let rows = [rule(), other]
            .map(|rule| (rule.key().to_vec().into(), rule.to_bytes().unwrap().into()))
            .to_vec();
        assert_eq!(
            operation.next(page(rows, None)),
            Ok(Next::Done(vec![rule()]))
        );
    }
}
