//! Stores the file name discovery rules of an import rule and previews where files would go.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::shared::{Claim, claim_file, read_shared, taken_paths};
use super::{ImportRuleError, read_earlier, validate_folder};
use crate::auth::request_authorization::{AuthorizeError, BucketAccess};
use crate::blob::migration::misplaced::{RenamePlan, rename_page};
use crate::driver::{DriverContext, drive};
use crate::groups::backends::parse_read;
use crate::groups::backends::query::GetBackendOperation;
use crate::groups::fence::{parse_group_record, read_group_record};
use crate::jobs::backend_import::{BlobRequest, listed_intents};
use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, IMPORT_DISCOVERY_KEYSPACE, IMPORT_PATTERN_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::backend_import::{
    BackendImport, ImportedObject, PathIdentity, WriteIntent, import_key, object_key, object_prefix,
};
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::discovery::{
    DiscoveryRule, DiscoveryRules, IdentityError, check_naming, check_patterns, file_identity,
};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use ulid::Ulid;

/// Files one preview page lists.
pub const PREVIEW_PAGE: usize = 200;
/// Listed pages one preview reads past when the caller may see none of their files.
const HIDDEN_PAGES: usize = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SetState {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Finish,
}

/// Replaces the discovery rules of one rule in a transaction that rereads the rule, so rules
/// never outlive it. They apply to files without a recorded mapping from the next page on.
#[derive(Debug, PartialEq)]
pub struct SetDiscoveryOperation {
    group_id: GroupId,
    import_id: Ulid,
    rules: Vec<DiscoveryRule>,
    state: SetState,
    txn_id: Option<TxnId>,
    output: Option<Result<Vec<DiscoveryRule>, ImportRuleError>>,
}

impl SetDiscoveryOperation {
    pub fn new(group_id: GroupId, import_id: Ulid, rules: Vec<DiscoveryRule>) -> Self {
        Self {
            group_id,
            import_id,
            rules,
            state: SetState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: ImportRuleError) -> Effects {
        self.state = SetState::Finish;
        self.output = Some(Err(error));
        self.abort()
    }

    fn write(&mut self, rule: Option<aruna_core::types::Value>) -> Effects {
        let Some(rule) = rule else {
            return self.fail(ImportRuleError::NotFound);
        };
        let pattern = BackendImport::from_bytes(&rule).map(|rule| rule.write_pattern);
        match pattern.map(|pattern| check_naming(&self.rules, &pattern)) {
            Ok(Ok(())) => {}
            Ok(Err(reason)) => return self.fail(ImportRuleError::Invalid(reason)),
            Err(error) => return self.fail(error.into()),
        }
        let key = import_key(self.group_id, self.import_id);
        let effect = match self.rules.is_empty() {
            true => StorageEffect::Delete {
                key_space: IMPORT_DISCOVERY_KEYSPACE.to_string(),
                key,
                txn_id: self.txn_id,
            },
            false => {
                let rules = DiscoveryRules {
                    rules: self.rules.clone(),
                };
                match rules.to_bytes() {
                    Ok(value) => StorageEffect::Write {
                        key_space: IMPORT_DISCOVERY_KEYSPACE.to_string(),
                        key,
                        value: value.into(),
                        txn_id: self.txn_id,
                    },
                    Err(error) => return self.fail(error.into()),
                }
            }
        };
        self.state = SetState::Write;
        smallvec![Effect::Storage(effect)]
    }
}

/// Patterns must not overlap; every folder is a relative folder ending with `/`.
pub fn check_rules(rules: &[DiscoveryRule]) -> Result<(), ImportRuleError> {
    check_patterns(rules).map_err(ImportRuleError::Invalid)?;
    for rule in rules {
        if rule.folder.is_empty() {
            return Err(ImportRuleError::Invalid(
                "a discovery folder must not be empty".to_string(),
            ));
        }
        validate_folder("folder", &rule.folder)?;
    }
    // An upload below the Aruna folder of a storage folder pattern must have one storage path.
    for (index, rule) in rules.iter().enumerate() {
        let nested = rules.iter().enumerate().find(|(other, them)| {
            *other != index
                && rule.storage_folder().is_some()
                && super::folders_overlap(&rule.folder, &them.folder)
        });
        if let Some((_, them)) = nested {
            return Err(ImportRuleError::Invalid(format!(
                "the folders `{}` and `{}` overlap",
                rule.folder, them.folder
            )));
        }
    }
    Ok(())
}

impl SetDiscoveryOperation {
    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(ImportRuleError::Unexpected("Write"));
        };
        self.state = SetState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl Operation for SetDiscoveryOperation {
    type Output = Vec<DiscoveryRule>;
    type Error = ImportRuleError;

    fn start(&mut self) -> Effects {
        if let Err(error) = check_rules(&self.rules) {
            return self.fail(error);
        }
        self.state = SetState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error.into()),
            (SetState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = SetState::Read;
                let key = import_key(self.group_id, self.import_id);
                smallvec![read_group_record(
                    self.group_id,
                    BACKEND_IMPORT_KEYSPACE,
                    key,
                    txn_id
                )]
            }
            (SetState::Read, event) => match parse_group_record(event) {
                Ok(rule) => self.write(rule),
                Err(error) => self.fail(error.into()),
            },
            // No rules delete the record, others write it; only that answer counts.
            (SetState::Write, Event::Storage(StorageEvent::WriteResult { .. }))
                if !self.rules.is_empty() =>
            {
                self.commit()
            }
            (SetState::Write, Event::Storage(StorageEvent::DeleteResult { .. }))
                if self.rules.is_empty() =>
            {
                self.commit()
            }
            (SetState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = SetState::Finish;
                self.output = Some(Ok(std::mem::take(&mut self.rules)));
                smallvec![]
            }
            _ => self.fail(ImportRuleError::Unexpected("SetDiscovery")),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == SetState::Finish
    }

    fn finalize(self) -> Result<Vec<DiscoveryRule>, ImportRuleError> {
        self.output
            .unwrap_or(Err(ImportRuleError::Unexpected("Finish")))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}

/// The stored discovery rules of a rule; none when it has no row.
pub async fn read_discovery(
    context: &DriverContext,
    group_id: GroupId,
    import_id: Ulid,
) -> Result<Vec<DiscoveryRule>, ImportRuleError> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: IMPORT_DISCOVERY_KEYSPACE.to_string(),
            key: import_key(group_id, import_id),
            txn_id: None,
        })
        .await;
    let rules = parse_read(event, DiscoveryRules::from_bytes)?;
    Ok(rules.map(|rules| rules.rules).unwrap_or_default())
}

/// The caller's WRITE on the rule's bucket, which every key a rule places lies in. A token
/// limited to some paths is refused: rules place keys anywhere below the rule's folder.
pub async fn bucket_access(
    context: &DriverContext,
    auth: &AuthContext,
    node_id: NodeId,
    rule: &BackendImport,
) -> Result<BucketAccess, ImportRuleError> {
    if auth.path_restrictions.is_some() {
        return Err(ImportRuleError::Forbidden);
    }
    let scope = (auth.realm_id, rule.group_id, node_id);
    let access = BucketAccess::load(context, auth, scope, &rule.bucket)
        .await
        .map_err(|error| match error {
            AuthorizeError::PermissionDenied | AuthorizeError::Policy(_) => {
                ImportRuleError::Forbidden
            }
            AuthorizeError::Storage(error) => ImportRuleError::Storage(error),
            AuthorizeError::CheckFailed(message) => ImportRuleError::Read(message),
        })?;
    match access.allows(None, &Permission::WRITE) {
        true => Ok(access),
        false => Err(ImportRuleError::Forbidden),
    }
}

/// The rule and its earlier patterns as stored, read before its renames are checked, so a
/// rename starts only for the files the check saw.
pub async fn rename_guards(
    context: &DriverContext,
    rule: &BackendImport,
) -> Result<Vec<(String, Key, Option<Value>)>, ImportRuleError> {
    let reads = vec![
        (BACKEND_IMPORT_KEYSPACE.to_string(), rule.key()),
        (
            IMPORT_PATTERN_KEYSPACE.to_string(),
            object_prefix(rule.import_id),
        ),
    ];
    let effect = StorageEffect::BatchRead {
        reads: reads.clone(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) if values.len() == 2 => {
            let values = values.into_iter().map(|(_, value)| value);
            Ok(reads
                .into_iter()
                .zip(values)
                .map(|((space, key), value)| (space, key, value))
                .collect())
        }
        other => Err(ImportRuleError::Read(format!(
            "unexpected answer {other:?}"
        ))),
    }
}

/// The plan of a rename of a rule's files after `bucket_access`, over all its keys; files
/// whose key the caller lacks `permission` on count as withheld.
pub async fn authorized_renames(
    context: &DriverContext,
    (auth, node_id): (&AuthContext, NodeId),
    rule: &BackendImport,
    permission: Permission,
) -> Result<RenamePlan, ImportRuleError> {
    let access = bucket_access(context, auth, node_id, rule).await?;
    let allow = |key: &str| access.allows(Some(key), &permission);
    let all = |_: &str| true;
    let planned = rename_page(context, rule, (&all, &allow), (None, None, 0)).await;
    Ok(planned.map_err(ImportRuleError::Read)?.plan)
}

/// A preview after `bucket_access`; it shows only files whose key the caller may read.
pub async fn authorized_preview(
    context: &DriverContext,
    (auth, node_id): (&AuthContext, NodeId),
    rule: &BackendImport,
    rules: &[DiscoveryRule],
    start_after: Option<String>,
) -> Result<DiscoveryPreview, ImportRuleError> {
    let access = bucket_access(context, auth, node_id, rule).await?;
    let mut after = start_after;
    // `next` names only a file the caller sees, so hidden pages are read past here.
    for _ in 0..HIDDEN_PAGES {
        let mut preview = preview_discovery(context, rule, rules, after).await?;
        preview.files.retain(|file| readable(&access, rule, file));
        if preview.next.is_none() {
            return Ok(preview);
        }
        if let Some(last) = preview.files.last() {
            preview.next = Some(last.path.clone());
            return Ok(preview);
        }
        after = preview.next;
    }
    Err(ImportRuleError::Invalid(
        "too many files in a row are hidden from the caller".to_string(),
    ))
}

/// A conflict has no key; its file is decided by its key at the bucket root.
fn readable(access: &BucketAccess, rule: &BackendImport, file: &PreviewFile) -> bool {
    let root = || {
        file.path
            .strip_prefix(&rule.backend_prefix)
            .map(str::to_string)
    };
    let key = file.key.clone().or_else(root);
    let keys = key.iter().chain(&file.claimed);
    key.is_some()
        && keys
            .into_iter()
            .all(|key| access.allows(Some(key), &Permission::READ))
}

/// Where a run would put one listed file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileOutcome {
    /// A recorded mapping keeps the file at its key; rules never move it.
    Known,
    /// One pattern matched.
    Matched,
    /// No pattern matched, so the file stays below the rule's own folder.
    Root,
    /// Several patterns matched, the name fits several keys or versions, open writes disagree,
    /// or no single rule of a shared folder names the file; a run reports it and imports nothing.
    Conflict(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreviewFile {
    pub path: String,
    /// The bucket key; `None` for a conflict.
    pub key: Option<String>,
    /// The version the file name or the write intent names, which a run registers it as.
    pub version_id: Option<Ulid>,
    /// Keys unfinished writes name for the file; the caller must be able to read all of them.
    pub claimed: Vec<String>,
    pub outcome: FileOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryPreview {
    pub files: Vec<PreviewFile>,
    /// The path to continue after; `None` on the last page.
    pub next: Option<String>,
}

/// Lists one page of the rule's folder and places each file with `rules`, without writing.
pub async fn preview_discovery(
    context: &DriverContext,
    rule: &BackendImport,
    rules: &[DiscoveryRule],
    start_after: Option<String>,
) -> Result<DiscoveryPreview, ImportRuleError> {
    check_rules(rules)?;
    let backend = drive(GetBackendOperation::new(rule.backend_id), context)
        .await
        .map_err(|error| ImportRuleError::Read(error.to_string()))?
        .filter(|backend| backend.group_id == rule.group_id)
        .ok_or_else(|| ImportRuleError::Invalid("the storage backend is missing".to_string()))?;
    let listing = BlobEffect::ListExternal {
        backend: BackendRef::Group(rule.backend_id),
        prefix: rule.backend_prefix.clone(),
        start_after,
        limit: PREVIEW_PAGE,
    };
    let (objects, next) = match drive(BlobRequest::new(listing), context).await {
        Ok(BlobEvent::ExternalListed { objects, next }) => (objects, next),
        Ok(other) => {
            return Err(ImportRuleError::Read(format!(
                "unexpected answer {other:?}"
            )));
        }
        Err(error) => return Err(ImportRuleError::Read(error.reason())),
    };
    let objects: Vec<_> = objects
        .into_iter()
        .filter(|object| !backend.is_reserved(&object.key))
        .collect();
    let reads = objects
        .iter()
        .map(|object| {
            let key = object_key(rule.import_id, &object.key);
            (IMPORTED_OBJECT_KEYSPACE.to_string(), key)
        })
        .collect();
    let rows = match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        other => {
            return Err(ImportRuleError::Read(format!(
                "unexpected answer {other:?}"
            )));
        }
    };
    let shared = read_shared(context, rule).await?;
    let earlier = read_earlier(context, rule.import_id).await?;
    let intents = listed_intents(context, rule, &objects).await;
    let intents = intents.map_err(ImportRuleError::Read)?;
    let paths: Vec<&str> = objects.iter().map(|object| object.key.as_str()).collect();
    let taken = taken_paths(context, &shared, &paths).await?;
    let mut files = Vec::with_capacity(objects.len());
    for (object, (_, row)) in objects.iter().zip(rows) {
        let row = row
            .map(|row| ImportedObject::from_bytes(&row))
            .transpose()?;
        let path = object.key.clone();
        let names = (rule, earlier.as_slice(), rules);
        let open = intents.get(&path).map_or(&[][..], Vec::as_slice);
        // An empty file of an open write is its reservation, as a run finds too.
        if object.size == 0 && !open.is_empty() {
            continue;
        }
        let file = match row {
            // A literal import whose name encodes a version is recovered as that version.
            Some(row)
                if open.is_empty() && (row.key.is_some() || !decodes(rule, &earlier, &path)) =>
            {
                PreviewFile {
                    key: row_key(rule, &row, &path),
                    version_id: None,
                    claimed: Vec::new(),
                    outcome: FileOutcome::Known,
                    path,
                }
            }
            // An open write is decided by a run that reads the file; until then it is unresolved.
            Some(row) if !open.is_empty() => {
                let mapped = row_key(rule, &row, &path);
                intended(path, open, mapped)
            }
            _ if !open.is_empty() => intended(path, open, None),
            Some(_) => named(names, path),
            // A file of a shared folder that another rule takes is not listed here.
            None => match claim_file(names, &shared, taken.contains(&path), &path) {
                Claim::Other => continue,
                Claim::Conflict => unresolved(path, Vec::new()),
                Claim::Own => named(names, path),
            },
        };
        files.push(file);
    }
    Ok(DiscoveryPreview { files, next })
}

/// The key a row keeps its file at; an older row without one keeps the rule's own mapping.
fn row_key(rule: &BackendImport, row: &ImportedObject, path: &str) -> Option<String> {
    row.key.clone().or_else(|| rule.aruna_key(path))
}

fn unresolved(path: String, patterns: Vec<String>) -> PreviewFile {
    PreviewFile {
        path,
        key: None,
        version_id: None,
        claimed: Vec::new(),
        outcome: FileOutcome::Conflict(patterns),
    }
}

fn decodes(rule: &BackendImport, earlier: &[String], path: &str) -> bool {
    rule.decode_any(path, earlier) != PathIdentity::Unmatched
}

/// The key the open writes of a file name, as a run reads them before the file name.
/// A run trusts an unfinished write only for the bytes it recorded, which the preview does not
/// read, so the file is unresolved. Every key it may get counts for who may see it.
fn intended(path: String, open: &[(Key, WriteIntent)], mapped: Option<String>) -> PreviewFile {
    let claims = open.iter().map(|(_, intent)| intent.key.clone());
    PreviewFile {
        claimed: mapped.into_iter().chain(claims).collect(),
        ..unresolved(path, Vec::new())
    }
}

/// Where a run puts a file by its name, with the version id the name encodes.
fn named(
    (rule, earlier, rules): (&BackendImport, &[String], &[DiscoveryRule]),
    path: String,
) -> PreviewFile {
    match file_identity(rule, earlier, rules, &path) {
        None | Some(Err(IdentityError::Ambiguous)) => unresolved(path, Vec::new()),
        Some(Err(IdentityError::Conflict(patterns))) => unresolved(path, patterns),
        Some(Ok(found)) => PreviewFile {
            path,
            key: Some(found.key),
            version_id: found.version_id,
            claimed: Vec::new(),
            outcome: match found.matched {
                true => FileOutcome::Matched,
                false => FileOutcome::Root,
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn import_rule() -> BackendImport {
        BackendImport {
            import_id: Ulid::from_parts(2, 2),
            group_id: Ulid::from_parts(1, 1),
            backend_id: Ulid::from_parts(3, 3),
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
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

    #[tokio::test]
    async fn checks_bucket_access() {
        use crate::tests::s3::{test_context, test_storage};
        use aruna_core::structs::identity::realm::RealmId;
        use aruna_core::structs::storage::blob::object_permission_path;
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let rule = import_rule();
        let realm = RealmId::from_bytes([1; 32]);
        let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let secret = object_permission_path(realm, rule.group_id, node, &rule.bucket, "secret/**");
        let scope = (realm, rule.group_id, node);
        let auth = crate::tests::access::seed_access(&context, scope, &secret).await;

        let access = bucket_access(&context, &auth, node, &rule).await.unwrap();
        let file = |path: &str, key: Option<&str>| PreviewFile {
            path: path.to_string(),
            key: key.map(str::to_string),
            version_id: None,
            claimed: Vec::new(),
            outcome: FileOutcome::Matched,
        };
        // A file shows only when the caller may read its key, also a conflict without one.
        assert!(readable(&access, &rule, &file("raw/a", Some("a"))));
        assert!(!readable(&access, &rule, &file("raw/a", Some("secret/a"))));
        assert!(!readable(&access, &rule, &file("raw/secret/b", None)));
        // An unresolved file shows only when every key a write claims for it is readable.
        let claimed = PreviewFile {
            claimed: vec!["secret/b".to_string()],
            ..file("raw/b", None)
        };
        assert!(!readable(&access, &rule, &claimed));

        let mut restricted = auth.clone();
        restricted.path_restrictions = Some(Vec::new());
        let refused = bucket_access(&context, &restricted, node, &rule).await;
        assert!(matches!(refused, Err(ImportRuleError::Forbidden)));
        let mut stranger = auth;
        stranger.user_id = aruna_core::UserId::local(Ulid::from_parts(9, 9), realm);
        let refused = bucket_access(&context, &stranger, node, &rule).await;
        assert!(matches!(refused, Err(ImportRuleError::Forbidden)));
    }
    fn storage(event: StorageEvent) -> Event {
        Event::Storage(event)
    }

    fn rules() -> Vec<DiscoveryRule> {
        vec![DiscoveryRule {
            pattern: "*.faa.gz".to_string(),
            folder: "proteins/".to_string(),
        }]
    }

    #[test]
    fn previews_run_identity() {
        let mut rule = import_rule();
        rule.write_pattern = "{dir}{name}~{id}".to_string();
        let id = Ulid::from_parts(5, 5);
        // A name the write pattern wrote keeps its version and goes where the rules place it.
        let file = named((&rule, &[], &rules()), format!("raw/run/s.faa.gz~{id}"));
        let key = Some("proteins/run/s.faa.gz".to_string());
        assert_eq!((file.key, file.version_id), (key, Some(id)));
        assert_eq!(file.outcome, FileOutcome::Matched);
        let file = named((&rule, &[], &rules()), "raw/baz".to_string());
        assert_eq!(
            (file.key.as_deref(), file.outcome),
            (Some("baz"), FileOutcome::Root)
        );
        // An unfinished write names the file only while all its intents agree.
        let intent = |key: &str| {
            let intent = WriteIntent {
                key: key.to_string(),
                version_id: id,
                backend: BackendRef::Group(rule.backend_id),
                created_by: Default::default(),
                written: None,
                mover: None,
            };
            (Key::from(Vec::new()), intent)
        };
        // The preview reads no bytes, so even a recorded write leaves the file unresolved.
        let mut proven = intent("in/a");
        proven.1.written = Some([1; 32]);
        let file = intended("raw/a".to_string(), &[proven], Some("old/a".to_string()));
        assert_eq!(file.outcome, FileOutcome::Conflict(Vec::new()));
        assert_eq!(file.claimed, ["old/a", "in/a"]);
        let file = intended("raw/a".to_string(), &[intent("in/a"), intent("in/b")], None);
        // An older row without a key is checked by the key the rule gives the file.
        let legacy = ImportedObject::written(id, String::new(), 1, None);
        let legacy = ImportedObject {
            key: None,
            ..legacy
        };
        assert_eq!(
            row_key(&rule, &legacy, "raw/run/x"),
            Some("run/x".to_string())
        );
        assert_eq!(file.outcome, FileOutcome::Conflict(Vec::new()));
    }

    #[test]
    fn rejects_bad_rules() {
        assert!(check_rules(&rules()).is_ok());
        for folder in ["", "proteins", "../x/", "/abs/"] {
            let rule = DiscoveryRule {
                folder: folder.to_string(),
                ..rules()[0].clone()
            };
            assert!(check_rules(&[rule]).is_err(), "{folder}");
        }
        let mut overlapping = rules();
        overlapping.push(DiscoveryRule {
            pattern: "sample.*".to_string(),
            folder: "other/".to_string(),
        });
        let mut operation = SetDiscoveryOperation::new(Ulid::nil(), Ulid::nil(), overlapping);
        assert!(operation.start().is_empty());
        assert!(matches!(
            operation.finalize(),
            Err(ImportRuleError::Invalid(_))
        ));
    }

    #[test]
    fn checks_folder_patterns() {
        let rule = |pattern: &str, folder: &str| DiscoveryRule {
            pattern: pattern.to_string(),
            folder: folder.to_string(),
        };
        let both = [rule("*.faa", "proteins/"), rule("lars/{name}", "foobar/")];
        assert!(check_rules(&both).is_ok());
        for bad in [
            vec![
                rule("lars/{name}", "foobar/"),
                rule("lars/sub/{name}", "other/"),
            ],
            vec![rule("lars/{name}", "proteins/"), rule("*.faa", "proteins/")],
            vec![rule("../{name}", "foobar/")],
            vec![rule("la*/{name}", "foobar/")],
        ] {
            assert!(check_rules(&bad).is_err(), "{bad:?}");
        }
    }

    /// Runs the operation up to its write, answering the rule read with `rule`.
    fn written(
        rules: Vec<DiscoveryRule>,
        rule: Option<Vec<u8>>,
    ) -> (SetDiscoveryOperation, Effects) {
        let mut operation = SetDiscoveryOperation::new(Ulid::nil(), Ulid::nil(), rules);
        operation.start();
        let txn_id = TxnId::from(3);
        operation.step(storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(Vec::new()), rule.map(Into::into)),
                (Key::from(Vec::new()), None),
            ],
        }));
        (operation, effects)
    }

    #[test]
    fn needs_dir_naming() {
        // A storage folder mapping would lose its folder under a naming without `{dir}`.
        let folders = vec![DiscoveryRule {
            pattern: "lars/{name}".to_string(),
            folder: "foobar/".to_string(),
        }];
        let mut rule = import_rule();
        rule.write_pattern = "{name}~{id}".to_string();
        let (operation, _) = written(folders, Some(rule.to_bytes().unwrap()));
        assert!(matches!(
            operation.finalize(),
            Err(ImportRuleError::Invalid(_))
        ));
    }

    #[test]
    fn stores_with_rule() {
        let (mut operation, effects) = written(rules(), Some(import_rule().to_bytes().unwrap()));
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                value,
                txn_id,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the rules write, got {effects:?}")
        };
        assert_eq!(key_space, IMPORT_DISCOVERY_KEYSPACE);
        assert_eq!(txn_id, &Some(TxnId::from(3)));
        assert_eq!(DiscoveryRules::from_bytes(value).unwrap().rules, rules());
        operation.step(storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        operation.step(storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::from(3),
        }));
        assert_eq!(operation.finalize(), Ok(rules()));

        // A delete answer does not count for a write.
        let (mut wrong, _) = written(rules(), Some(import_rule().to_bytes().unwrap()));
        wrong.step(storage(StorageEvent::DeleteResult {
            key: Vec::new().into(),
        }));
        assert!(matches!(
            wrong.finalize(),
            Err(ImportRuleError::Unexpected(_))
        ));

        // No rules delete the row.
        let (_, effects) = written(Vec::new(), Some(import_rule().to_bytes().unwrap()));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { key_space, .. })]
                if key_space == IMPORT_DISCOVERY_KEYSPACE
        ));

        // A removed rule gets no rules, and its transaction is aborted.
        let (operation, effects) = written(rules(), None);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(operation.finalize(), Err(ImportRuleError::NotFound));
    }
}
