//! Decides which rule imports an unmapped file of a backend folder that several rules read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::folders::shares_folder;
use super::{ImportRuleError, ListImportsOperation};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    IMPORT_DISCOVERY_KEYSPACE, IMPORT_INTENT_KEYSPACE, IMPORT_PATTERN_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::storage::backend_import::{
    BackendImport, WriteIntent, import_key, object_key, object_prefix, read_patterns,
};
use aruna_core::structs::storage::discovery::{DiscoveryRule, DiscoveryRules, file_identity};
use aruna_core::types::{Key, Value};
use std::collections::{BTreeMap, HashSet, VecDeque};
use ulid::Ulid;

/// Rows and intents one range read returns.
const RANGE_PAGE: usize = 500;

/// Another rule that reads a folder overlapping the importing rule's folder.
#[derive(Clone, Debug, PartialEq)]
pub struct SharedRule {
    pub rule: BackendImport,
    /// Patterns the rule wrote with before, which still name its files.
    pub earlier: Vec<String>,
    pub discovery: Vec<DiscoveryRule>,
}

/// Which rule imports one unmapped file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Claim {
    Own,
    /// Another rule recorded the file, has a write intent for it, or alone names it.
    Other,
    /// No single rule names the file, so none imports it.
    Conflict,
}

type SharedTurn = Result<Next<Vec<SharedRule>>, ImportRuleError>;

/// The other rules of the group that read an overlapping folder of the same backend.
#[derive(Debug, PartialEq)]
pub struct ReadSharedOperation {
    rule: BackendImport,
    listed: bool,
    others: VecDeque<BackendImport>,
    reading: Option<BackendImport>,
    shared: Vec<SharedRule>,
}

impl ReadSharedOperation {
    pub fn new(rule: &BackendImport) -> Self {
        Self {
            rule: rule.clone(),
            listed: false,
            others: VecDeque::new(),
            reading: None,
            shared: Vec::new(),
        }
    }

    fn next_rule(&mut self) -> SharedTurn {
        let Some(other) = self.others.pop_front() else {
            return Ok(Next::Done(std::mem::take(&mut self.shared)));
        };
        let reads = vec![
            (
                IMPORT_DISCOVERY_KEYSPACE.to_string(),
                import_key(other.group_id, other.import_id),
            ),
            (
                IMPORT_PATTERN_KEYSPACE.to_string(),
                object_prefix(other.import_id),
            ),
        ];
        self.reading = Some(other);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn rule_read(&mut self, [discovery, earlier]: [Option<Value>; 2]) -> SharedTurn {
        let rule = self
            .reading
            .take()
            .ok_or(ImportRuleError::Unexpected("Shared"))?;
        let discovery = (discovery.as_deref())
            .map(DiscoveryRules::from_bytes)
            .transpose()?
            .map(|rules| rules.rules)
            .unwrap_or_default();
        self.shared.push(SharedRule {
            rule,
            earlier: read_patterns(earlier.as_deref())?,
            discovery,
        });
        self.next_rule()
    }
}

impl Operation for ReadSharedOperation {
    type Output = Vec<SharedRule>;
    type Error = ImportRuleError;

    fn first(&mut self) -> SharedTurn {
        let list = ListImportsOperation::new(self.rule.group_id);
        Ok(Next::Call(IoCall::child(list)))
    }

    fn next(&mut self, result: IoResult) -> SharedTurn {
        if self.listed {
            return self.rule_read(values::<2>(result)?);
        }
        self.listed = true;
        let rules = result.child::<Vec<BackendImport>, ImportRuleError>()??;
        let rule = &self.rule;
        self.others = (rules.into_iter())
            .filter(|other| shares_folder(rule, other))
            .collect();
        self.next_rule()
    }
}

/// The page of a rule's rows in `space` from `start`. The first page starts at a row itself,
/// which only a storage scan can do.
fn range_page(space: &str, import_id: Ulid, start: IterStart) -> IoCall {
    Effect::Storage(StorageEffect::Iter {
        key_space: space.to_string(),
        prefix: Some(object_prefix(import_id)),
        start: Some(start),
        limit: RANGE_PAGE,
        txn_id: None,
    })
    .into()
}

type RangeRows = (Vec<(Key, Value)>, Option<Key>);

fn range_rows(event: Event) -> Result<RangeRows, ImportRuleError> {
    match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(ImportRuleError::Read(other.label().to_string())),
    }
}

/// The path of an intent key: the import id, the path, a zero byte and the version id.
fn intent_path(key: &[u8]) -> Option<&str> {
    let end = key.len().checked_sub(17)?;
    let path = key.get(16..end)?;
    (key[end] == 0).then_some(())?;
    std::str::from_utf8(path).ok()
}

/// The write intents by path.
pub type PathIntents = BTreeMap<String, Vec<(Key, WriteIntent)>>;

/// The write intents of every path from the first to the last of `paths`, read as one range, so
/// a page of known files costs one read however many paths it has.
#[derive(Debug, PartialEq)]
pub struct PathIntentsOperation {
    import_id: Ulid,
    bounds: Option<(String, String)>,
    intents: PathIntents,
}

type IntentsTurn = Result<Next<PathIntents>, ImportRuleError>;

impl PathIntentsOperation {
    pub fn new(import_id: Ulid, paths: &[&str]) -> Self {
        let first = paths.iter().min().map(|path| path.to_string());
        let last = paths.iter().max().map(|path| path.to_string());
        Self {
            import_id,
            bounds: first.zip(last),
            intents: BTreeMap::new(),
        }
    }

    fn page(&self, start: IterStart) -> IntentsTurn {
        let page = range_page(IMPORT_INTENT_KEYSPACE, self.import_id, start);
        Ok(Next::Call(page))
    }
}

impl Operation for PathIntentsOperation {
    type Output = PathIntents;
    type Error = ImportRuleError;

    fn first(&mut self) -> IntentsTurn {
        let Some((first, _)) = self.bounds.as_ref() else {
            return Ok(Next::Done(PathIntents::new()));
        };
        self.page(IterStart::At(object_key(self.import_id, first)))
    }

    fn next(&mut self, result: IoResult) -> IntentsTurn {
        let (rows, next) = range_rows(result.event()?)?;
        let last = self.bounds.as_ref().map_or("", |(_, last)| last.as_str());
        for (key, value) in rows {
            let path = intent_path(&key);
            let path = path
                .ok_or_else(|| ImportRuleError::Read("a malformed write intent key".to_string()))?;
            if path > last {
                return Ok(Next::Done(std::mem::take(&mut self.intents)));
            }
            let path = path.to_string();
            let intent = WriteIntent::from_bytes(&value)?;
            self.intents.entry(path).or_default().push((key, intent));
        }
        match next {
            Some(next) => self.page(IterStart::After(next)),
            None => Ok(Next::Done(std::mem::take(&mut self.intents))),
        }
    }
}

type TakenTurn = Result<Next<HashSet<String>>, ImportRuleError>;

/// The paths among `paths` that another rule recorded or has a write intent for. Each rule
/// costs one range read per keyspace over the paths, however many there are.
#[derive(Debug, PartialEq)]
pub struct TakenPathsOperation {
    wanted: HashSet<String>,
    bounds: Option<(String, String)>,
    scans: VecDeque<(Ulid, &'static str)>,
    scan: Option<(Ulid, &'static str)>,
    taken: HashSet<String>,
}

impl TakenPathsOperation {
    pub fn new(shared: &[SharedRule], paths: &[&str]) -> Self {
        let first = paths.iter().min().map(|path| path.to_string());
        let last = paths.iter().max().map(|path| path.to_string());
        let scans = (shared.iter())
            .flat_map(|other| {
                let import_id = other.rule.import_id;
                [
                    (import_id, IMPORTED_OBJECT_KEYSPACE),
                    (import_id, IMPORT_INTENT_KEYSPACE),
                ]
            })
            .collect();
        Self {
            wanted: paths.iter().map(|path| path.to_string()).collect(),
            bounds: first.zip(last),
            scans,
            scan: None,
            taken: HashSet::new(),
        }
    }

    fn next_scan(&mut self) -> TakenTurn {
        let (Some((first, _)), Some((import_id, space))) =
            (self.bounds.as_ref(), self.scans.pop_front())
        else {
            return Ok(Next::Done(std::mem::take(&mut self.taken)));
        };
        let start = IterStart::At(object_key(import_id, first));
        self.scan = Some((import_id, space));
        Ok(Next::Call(range_page(space, import_id, start)))
    }
}

impl Operation for TakenPathsOperation {
    type Output = HashSet<String>;
    type Error = ImportRuleError;

    fn first(&mut self) -> TakenTurn {
        self.next_scan()
    }

    fn next(&mut self, result: IoResult) -> TakenTurn {
        let (import_id, space) = self.scan.ok_or(ImportRuleError::Unexpected("Taken"))?;
        let (rows, next) = range_rows(result.event()?)?;
        let last = self.bounds.as_ref().map_or("", |(_, last)| last.as_str());
        let mut past = false;
        for (key, _) in rows {
            let rest = key.get(16..).unwrap_or_default();
            // An intent key ends in a zero byte and the 16 bytes of its version id.
            let path = match space == IMPORT_INTENT_KEYSPACE {
                true => rest
                    .get(..rest.len().saturating_sub(17))
                    .unwrap_or_default(),
                false => rest,
            };
            let path = std::str::from_utf8(path).unwrap_or_default();
            past |= path > last;
            if self.wanted.contains(path) {
                self.taken.insert(path.to_string());
            }
        }
        match next {
            Some(next) if !past => {
                let page = range_page(space, import_id, IterStart::After(next));
                Ok(Next::Call(page))
            }
            _ => self.next_scan(),
        }
    }
}

/// A rule's view of one unmapped file: placed by a pattern, or at a root key of a bucket.
#[derive(Debug, PartialEq)]
enum Place {
    Named,
    Root(String, String),
}

fn place_file(
    (rule, earlier, discovery): (&BackendImport, &[String], &[DiscoveryRule]),
    path: &str,
) -> Place {
    match file_identity(rule, earlier, discovery, path) {
        Some(Ok(found)) if !found.matched => Place::Root(rule.bucket.clone(), found.key),
        _ => Place::Named,
    }
}

/// A file another rule recorded or writes is its own; else exactly one rule must name it by
/// pattern, or be the only rule that reads it. Rules that all give it the same root key of
/// one bucket agree on its identity, so the oldest of them imports it.
pub fn claim_file(
    own: (&BackendImport, &[String], &[DiscoveryRule]),
    shared: &[SharedRule],
    taken: bool,
    path: &str,
) -> Claim {
    let readers: Vec<&SharedRule> = shared
        .iter()
        .filter(|other| path.starts_with(&other.rule.backend_prefix))
        .collect();
    if readers.is_empty() {
        return Claim::Own;
    }
    if taken {
        return Claim::Other;
    }
    let mine = place_file(own, path);
    let places: Vec<Place> = readers
        .iter()
        .map(|other| place_file((&other.rule, &other.earlier, &other.discovery), path))
        .collect();
    let named = places
        .iter()
        .filter(|place| **place == Place::Named)
        .count();
    match (&mine, named) {
        (Place::Named, 0) => Claim::Own,
        (Place::Root(..), 1) => Claim::Other,
        (Place::Root(..), 0) if places.iter().all(|place| *place == mine) => {
            let oldest = readers
                .iter()
                .all(|other| own.0.import_id < other.rule.import_id);
            match oldest {
                true => Claim::Own,
                false => Claim::Other,
            }
        }
        _ => Claim::Conflict,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::testing::test_context;
    use crate::tests::s3::test_storage;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::BACKEND_IMPORT_KEYSPACE;
    use aruna_core::structs::storage::backend_import::{DEFAULT_PATTERN, ImportedObject};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn rule(id: u64, key_prefix: &str) -> BackendImport {
        BackendImport {
            import_id: Ulid::from_parts(id, 1),
            group_id: Ulid::from_parts(1, 1),
            backend_id: Ulid::from_parts(3, 3),
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: key_prefix.to_string(),
            schedule: None,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: DEFAULT_PATTERN.to_string(),
        }
    }

    fn pattern(pattern: &str) -> Vec<DiscoveryRule> {
        let folder = "sorted/".to_string();
        let pattern = pattern.to_string();
        vec![DiscoveryRule { pattern, folder }]
    }

    async fn put(context: &DriverContext, space: &str, key: Vec<u8>, value: Vec<u8>) {
        let effect = StorageEffect::Write {
            key_space: space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        let event = context.storage_handle.send_storage_effect(effect).await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn claims_shared_files() {
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let (foo, bar) = (rule(10, "foo/"), rule(11, "bar/"));
        for rule in [&foo, &bar] {
            let value = rule.to_bytes().unwrap();
            put(
                &context,
                BACKEND_IMPORT_KEYSPACE,
                rule.key().to_vec(),
                value,
            )
            .await;
        }
        // `foo/report.txt` was written to `raw/report.txt` and recorded by its rule.
        let written =
            ImportedObject::written(Ulid::from_parts(20, 1), "foo/report.txt".into(), 4, None);
        let row = object_key(foo.import_id, "raw/report.txt").to_vec();
        put(
            &context,
            IMPORTED_OBJECT_KEYSPACE,
            row,
            written.to_bytes().unwrap(),
        )
        .await;

        // An open write intent of the other rule claims its path too.
        let intent = aruna_core::structs::storage::backend_import::intent_key(
            foo.import_id,
            "raw/inflight.txt",
            Ulid::from_parts(21, 1),
        );
        put(&context, IMPORT_INTENT_KEYSPACE, intent.to_vec(), vec![1]).await;

        let shared = drive(ReadSharedOperation::new(&bar), &context)
            .await
            .unwrap();
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].rule.import_id, foo.import_id);
        let paths = ["raw/a.fna", "raw/baz", "raw/inflight.txt", "raw/report.txt"];
        let taken = drive(TakenPathsOperation::new(&shared, &paths), &context)
            .await
            .unwrap();
        let expected = ["raw/inflight.txt", "raw/report.txt"].map(str::to_string);
        assert_eq!(taken, HashSet::from(expected));
        let own = pattern("*.fna");
        let claim = |path: &str| claim_file((&bar, &[], &own), &shared, taken.contains(path), path);
        // A file the other rule recorded or writes is its own; only the naming rule takes a new one.
        assert_eq!(claim("raw/report.txt"), Claim::Other);
        assert_eq!(claim("raw/inflight.txt"), Claim::Other);
        assert_eq!(claim("raw/a.fna"), Claim::Own);
        // Both rules would put `baz` at the root key of the same bucket; the older rule takes it.
        assert_eq!(claim("raw/baz"), Claim::Other);
        let back = vec![SharedRule {
            rule: bar.clone(),
            earlier: Vec::new(),
            discovery: own.clone(),
        }];
        assert_eq!(
            claim_file((&foo, &[], &[]), &back, false, "raw/baz"),
            Claim::Own
        );
    }

    #[test]
    fn decides_claims() {
        let (foo, mut bar) = (rule(10, "foo/"), rule(11, "bar/"));
        let shared = |rule: &BackendImport, discovery: Vec<DiscoveryRule>| {
            vec![SharedRule {
                rule: rule.clone(),
                earlier: Vec::new(),
                discovery,
            }]
        };
        let claim = |others: &[SharedRule], own: &[DiscoveryRule], path| {
            claim_file((&foo, &[], own), others, false, path)
        };
        // Nobody else reads the folder.
        assert_eq!(claim(&[], &[], "raw/x"), Claim::Own);
        let fna = pattern("*.fna");
        assert_eq!(
            claim(&shared(&bar, Vec::new()), &fna, "raw/a.fna"),
            Claim::Own
        );
        assert_eq!(
            claim(&shared(&bar, fna.clone()), &[], "raw/a.fna"),
            Claim::Other
        );
        assert_eq!(
            claim(&shared(&bar, fna.clone()), &fna, "raw/a.fna"),
            Claim::Conflict
        );
        // Different buckets give different keys, so nobody takes an unnamed file.
        bar.bucket = "other".to_string();
        assert_eq!(
            claim(&shared(&bar, Vec::new()), &[], "raw/x"),
            Claim::Conflict
        );
    }

    fn intent() -> WriteIntent {
        WriteIntent {
            key: "a".to_string(),
            version_id: Ulid::from_parts(3, 3),
            backend: aruna_core::structs::storage::blob::BackendRef::node_default(),
            created_by: Default::default(),
            written: None,
            mover: None,
        }
    }

    fn scanned(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
            values: rows,
            next_start_after: next,
        })))
    }

    #[test]
    fn pathless_skips_scan() {
        let mut intents = PathIntentsOperation::new(Ulid::nil(), &[]);
        assert_eq!(intents.first(), Ok(Next::Done(PathIntents::new())));
        let mut taken = TakenPathsOperation::new(&[], &["a"]);
        assert_eq!(taken.first(), Ok(Next::Done(HashSet::new())));
    }

    #[test]
    fn intents_stop_early() {
        let id = Ulid::from_parts(2, 2);
        let version = Ulid::from_parts(3, 3);
        let inside = aruna_core::structs::storage::backend_import::intent_key(id, "a", version);
        let past = aruna_core::structs::storage::backend_import::intent_key(id, "z", version);
        let value: Value = intent().to_bytes().unwrap().into();
        let mut operation = PathIntentsOperation::new(id, &["a"]);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        let rows = vec![(inside.clone(), value.clone()), (past, value)];
        let turn = operation.next(scanned(rows, Some(Key::from(vec![1]))));
        let Ok(Next::Done(intents)) = turn else {
            panic!("the range ended: {turn:?}");
        };
        assert_eq!(intents["a"], vec![(inside, intent())]);
    }
}
