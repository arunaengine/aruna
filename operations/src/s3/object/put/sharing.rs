//! Reads the group's import rules inside an import's version transaction, so a rule that
//! started to share the folder since the run looked refuses the version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::groups::imports::shared::shares_folder;
use aruna_core::effects::IterStart;
use aruna_core::keyspaces::BACKEND_IMPORT_KEYSPACE;
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::types::Key;
use std::collections::BTreeSet;

const RULE_PAGE: usize = 128;

/// The rules a run saw sharing its folder, and what the transaction has read so far.
#[derive(Debug, PartialEq)]
pub(crate) struct RuleScan {
    rule: BackendImport,
    expected: BTreeSet<Ulid>,
    found: BTreeSet<Ulid>,
    after: Option<Key>,
    done: bool,
}

impl RuleScan {
    pub(crate) fn new(rule: BackendImport, expected: BTreeSet<Ulid>) -> Self {
        Self {
            rule,
            expected,
            found: BTreeSet::new(),
            after: None,
            done: false,
        }
    }

    /// A retried transaction reads all rules again.
    pub(super) fn restart(&mut self) {
        (self.found, self.after, self.done) = (BTreeSet::new(), None, false);
    }
}

/// The other rules of a group that read one path, read page by page in a transaction, so a
/// rule created since the write was routed counts too.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ReaderScan {
    after: Option<Key>,
    found: Vec<Ulid>,
    done: bool,
}

impl ReaderScan {
    pub(crate) fn done(&self) -> bool {
        self.done
    }

    /// The next page of the group's rules.
    pub(crate) fn page(&self, group_id: Ulid, txn_id: Option<Ulid>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
            prefix: Some(group_id.to_bytes().to_vec().into()),
            start: self.after.clone().map(IterStart::After),
            limit: RULE_PAGE,
            txn_id,
        })
    }

    /// Adds a page; the readers once all pages are read.
    pub(crate) fn read(
        &mut self,
        event: Event,
        (import_id, backend, path): (Ulid, &BackendRef, &str),
    ) -> Result<Option<Vec<Ulid>>, String> {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return Err(format!("unexpected answer {event:?}"));
        };
        for (_, value) in values {
            let rule = BackendImport::from_bytes(&value).map_err(|error| error.to_string())?;
            let reads = *backend == BackendRef::Group(rule.backend_id)
                && path.starts_with(&rule.backend_prefix);
            if rule.import_id != import_id && reads {
                self.found.push(rule.import_id);
            }
        }
        self.after = next_start_after;
        self.done = self.after.is_none();
        Ok(self.done.then(|| self.found.clone()))
    }
}

impl PutObjectOperation {
    /// Refuses the version unless exactly the `scan` rules share the folder at commit.
    pub(crate) fn with_rule_scan(mut self, scan: RuleScan) -> Self {
        self.rule_scan = Some(scan);
        self
    }

    /// The next page of the group's rules, or `None` once all are read.
    pub(super) fn rule_page(&mut self, txn_id: Ulid) -> Option<Effects> {
        let scan = self.rule_scan.as_ref().filter(|scan| !scan.done)?;
        let prefix: Key = scan.rule.group_id.to_bytes().to_vec().into();
        let start = scan.after.clone().map(IterStart::After);
        self.state = PutObjectState::ReadRules;
        Some(smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start,
            limit: RULE_PAGE,
            txn_id: Some(txn_id),
        })])
    }

    pub(super) fn rules_read(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(PutObjectError::NoTransactionFound);
        };
        let (values, next) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return self.emit_error(error.into()),
            _ => return self.emit_error(PutObjectError::InvalidOperationState),
        };
        let Some(scan) = self.rule_scan.as_mut() else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        for (_, value) in values {
            match BackendImport::from_bytes(&value) {
                Ok(other) if shares_folder(&scan.rule, &other) => {
                    scan.found.insert(other.import_id);
                }
                Ok(_) => {}
                Err(error) => return self.emit_error(error.into()),
            }
        }
        scan.done = next.is_none();
        scan.after = next;
        if scan.done && scan.found != scan.expected {
            return self.emit_error(PutObjectError::HeadChanged);
        }
        self.write_cleanup_row(txn_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::object::put::test::put_config;

    fn rule(id: u64, prefix: &str) -> BackendImport {
        BackendImport {
            import_id: Ulid::from_parts(id, 1),
            group_id: Ulid::from_parts(1, 1),
            backend_id: Ulid::from_parts(2, 2),
            backend_prefix: prefix.to_string(),
            bucket: "b".to_string(),
            key_prefix: String::new(),
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

    #[test]
    fn reads_sharing_rules() {
        let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let own = rule(1, "raw/");
        let scan = |expected: &[Ulid]| {
            let config = put_config(realm_id, own.group_id, node_id);
            let expected = expected.iter().copied().collect();
            let mut op = PutObjectOperation::new(config)
                .with_rule_scan(RuleScan::new(own.clone(), expected));
            op.txn_id = Some(Ulid::nil());
            let effects = op.rule_page(Ulid::nil()).unwrap();
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::Iter {
                    txn_id: Some(_),
                    ..
                })]
            ));
            let rows = [own.clone(), rule(2, "raw/sub/"), rule(3, "other/")];
            let values = rows
                .iter()
                .map(|rule| (rule.key(), rule.to_bytes().unwrap().into()));
            op.rules_read(Event::Storage(StorageEvent::IterResult {
                values: values.collect(),
                next_start_after: None,
            }));
            op
        };
        // The rule seen by the run still shares the folder; nothing else does.
        let op = scan(&[Ulid::from_parts(2, 1)]);
        assert!(!matches!(op.output, Some(Err(PutObjectError::HeadChanged))));
        // A rule added on the folder since the run looked refuses the version.
        let op = scan(&[]);
        assert!(matches!(op.output, Some(Err(PutObjectError::HeadChanged))));
    }
}
