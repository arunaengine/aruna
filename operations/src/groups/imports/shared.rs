//! Decides which rule imports an unmapped file of a backend folder that several rules read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::discovery::read_discovery;
use super::{ImportRuleError, folders_overlap, list_imports};
use crate::driver::DriverContext;
use crate::groups::backends::parse_read;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::IMPORTED_OBJECT_KEYSPACE;
use aruna_core::structs::storage::backend_import::{BackendImport, object_key};
use aruna_core::structs::storage::discovery::{DiscoveryRule, Placement, listed_name, place};

/// Another rule that reads a folder overlapping the importing rule's folder.
#[derive(Clone, Debug, PartialEq)]
pub struct SharedRule {
    pub rule: BackendImport,
    pub discovery: Vec<DiscoveryRule>,
}

/// Which rule imports one unmapped file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Claim {
    Own,
    /// Another rule recorded the file or alone names it.
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
        shared.push(SharedRule {
            rule: other,
            discovery,
        });
    }
    Ok(shared)
}

/// A file another rule recorded is its own; else exactly one rule must name it by pattern,
/// or be the only rule that reads it.
pub async fn claim_file(
    context: &DriverContext,
    (rule, discovery): (&BackendImport, &[DiscoveryRule]),
    shared: &[SharedRule],
    path: &str,
) -> Result<Claim, ImportRuleError> {
    let readers: Vec<&SharedRule> = shared
        .iter()
        .filter(|other| path.starts_with(&other.rule.backend_prefix))
        .collect();
    for other in &readers {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
                key: object_key(other.rule.import_id, path),
                txn_id: None,
            })
            .await;
        if parse_read(event, |bytes| Ok(bytes.to_vec()))?.is_some() {
            return Ok(Claim::Other);
        }
    }
    let named = |rule: &BackendImport, discovery: &[DiscoveryRule]| {
        let name = path.rsplit('/').next().unwrap_or(path);
        !matches!(place(discovery, listed_name(rule, name)), Placement::Root)
    };
    let others = readers
        .iter()
        .filter(|other| named(&other.rule, &other.discovery))
        .count();
    Ok(decide_claim(named(rule, discovery), others, readers.len()))
}

fn decide_claim(own: bool, others: usize, readers: usize) -> Claim {
    match (own, others) {
        _ if readers == 0 => Claim::Own,
        (true, 0) => Claim::Own,
        (false, 1) => Claim::Other,
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

        let shared = read_shared(&context, &bar).await.unwrap();
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].rule.import_id, foo.import_id);
        let own = pattern("*.fna");
        let claim = |path: &'static str| claim_file(&context, (&bar, &own), &shared, path);
        // A file the other rule recorded is its own; only the naming rule takes a new one.
        assert_eq!(claim("raw/report.txt").await.unwrap(), Claim::Other);
        assert_eq!(claim("raw/a.fna").await.unwrap(), Claim::Own);
        assert_eq!(claim("raw/baz").await.unwrap(), Claim::Conflict);
    }

    #[test]
    fn decides_claims() {
        assert_eq!(decide_claim(false, 0, 0), Claim::Own);
        assert_eq!(decide_claim(true, 0, 1), Claim::Own);
        assert_eq!(decide_claim(false, 1, 1), Claim::Other);
        assert_eq!(decide_claim(true, 1, 1), Claim::Conflict);
        assert_eq!(decide_claim(false, 2, 2), Claim::Conflict);
        assert_eq!(decide_claim(false, 0, 1), Claim::Conflict);
    }
}
