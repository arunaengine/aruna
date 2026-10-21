//! Decides which rule imports an unmapped file of a backend folder that several rules read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::discovery::read_discovery;
use super::{ImportRuleError, folders_overlap, list_imports, read_earlier};
use crate::driver::DriverContext;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{IMPORT_INTENT_KEYSPACE, IMPORTED_OBJECT_KEYSPACE};
use aruna_core::structs::storage::backend_import::{BackendImport, object_key, object_prefix};
use aruna_core::structs::storage::discovery::{DiscoveryRule, file_identity};
use std::collections::HashSet;

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

/// The other rules of the group that read an overlapping folder of the same backend.
pub async fn read_shared(
    context: &DriverContext,
    rule: &BackendImport,
) -> Result<Vec<SharedRule>, ImportRuleError> {
    let mut shared = Vec::new();
    for other in list_imports(context, rule.group_id).await? {
        if other.import_id == rule.import_id
            || other.backend_id != rule.backend_id
            || !folders_overlap(&other.backend_prefix, &rule.backend_prefix)
        {
            continue;
        }
        let discovery = read_discovery(context, other.group_id, other.import_id).await?;
        let earlier = read_earlier(context, other.import_id).await?;
        shared.push(SharedRule {
            rule: other,
            earlier,
            discovery,
        });
    }
    Ok(shared)
}

/// The paths among `paths` that another rule recorded or has a write intent for. Each rule
/// costs one range read per keyspace over the paths, however many there are.
pub async fn taken_paths(
    context: &DriverContext,
    shared: &[SharedRule],
    paths: &[&str],
) -> Result<HashSet<String>, ImportRuleError> {
    let wanted: HashSet<&str> = paths.iter().copied().collect();
    let mut taken = HashSet::new();
    let (Some(first), Some(last)) = (paths.iter().min(), paths.iter().max()) else {
        return Ok(taken);
    };
    for other in shared {
        let import_id = other.rule.import_id;
        for space in [IMPORTED_OBJECT_KEYSPACE, IMPORT_INTENT_KEYSPACE] {
            let mut start = IterStart::At(object_key(import_id, first));
            loop {
                let effect = StorageEffect::Iter {
                    key_space: space.to_string(),
                    prefix: Some(object_prefix(import_id)),
                    start: Some(start),
                    limit: RANGE_PAGE,
                    txn_id: None,
                };
                let event = context.storage_handle.send_storage_effect(effect).await;
                let (values, next) = match event {
                    Event::Storage(StorageEvent::IterResult {
                        values,
                        next_start_after,
                    }) => (values, next_start_after),
                    Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
                    other => return Err(ImportRuleError::Read(format!("{other:?}"))),
                };
                let mut past = false;
                for (key, _) in values {
                    let rest = key.get(16..).unwrap_or_default();
                    // An intent key ends in a zero byte and the 16 bytes of its version id.
                    let path = match space == IMPORT_INTENT_KEYSPACE {
                        true => rest
                            .get(..rest.len().saturating_sub(17))
                            .unwrap_or_default(),
                        false => rest,
                    };
                    let path = std::str::from_utf8(path).unwrap_or_default();
                    past |= path > *last;
                    if wanted.contains(path) {
                        taken.insert(path.to_string());
                    }
                }
                match next {
                    Some(next) if !past => start = IterStart::After(next),
                    _ => break,
                }
            }
        }
    }
    Ok(taken)
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
    use crate::tests::s3::{test_context, test_storage};
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

        let shared = read_shared(&context, &bar).await.unwrap();
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].rule.import_id, foo.import_id);
        let paths = ["raw/a.fna", "raw/baz", "raw/inflight.txt", "raw/report.txt"];
        let taken = taken_paths(&context, &shared, &paths).await.unwrap();
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
}
