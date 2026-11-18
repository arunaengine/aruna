//! Stores the file name discovery rules of an import rule and previews where files would go.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::shared::{
    Claim, PathIntents, PathIntentsOperation, ReadSharedOperation, SharedRule, TakenPathsOperation,
    claim_file,
};
use super::{ImportRuleError, ReadEarlierOperation, validate_folder};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::{BucketAccess, BucketAccessOperation};
use crate::blob::migration::misplaced::{RenamePlan, rename_page};
use crate::driver::{DriverContext, drive};
use crate::groups::backends::query::{GetBackendOperation, GroupQueryError};
use crate::groups::fence::group_fence_key;
use crate::jobs::blob_request::BlobRequest;
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::BlobEffect;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, IMPORT_DISCOVERY_KEYSPACE, IMPORT_PATTERN_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{read_row, values};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::group_delete::check_group_write;
use aruna_core::structs::storage::backend_import::{
    BackendImport, ExternalObject, ImportedObject, PathIdentity, WriteIntent, import_key,
    object_key, object_prefix,
};
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::discovery::{
    DiscoveryRule, DiscoveryRules, IdentityError, check_naming, check_patterns, file_identity,
};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::{GroupId, Key, Value};
use std::collections::HashSet;
use ulid::Ulid;

/// Files one preview page lists.
pub const PREVIEW_PAGE: usize = 200;
/// Listed pages one preview reads past when the caller may see none of their files.
const HIDDEN_PAGES: usize = 10;

type SetTurn = Result<Next<Vec<DiscoveryRule>>, ImportRuleError>;

/// Replaces the discovery rules of one rule in a transaction that rereads the rule, so rules
/// never outlive it. They apply to files without a recorded mapping from the next page on.
#[derive(Debug, PartialEq)]
pub struct SetDiscoveryOperation {
    group_id: GroupId,
    import_id: Ulid,
    rules: Vec<DiscoveryRule>,
    written: bool,
}

impl SetDiscoveryOperation {
    pub fn new(group_id: GroupId, import_id: Ulid, rules: Vec<DiscoveryRule>) -> Self {
        Self {
            group_id,
            import_id,
            rules,
            written: false,
        }
    }

    fn write(&mut self, [rule, fence]: [Option<Value>; 2]) -> SetTurn {
        check_group_write(fence.as_deref())?;
        let rule = rule.ok_or(ImportRuleError::NotFound)?;
        let pattern = BackendImport::from_bytes(&rule)?.write_pattern;
        check_naming(&self.rules, &pattern).map_err(ImportRuleError::Invalid)?;
        let key = import_key(self.group_id, self.import_id);
        let space = IMPORT_DISCOVERY_KEYSPACE.to_string();
        self.written = true;
        if self.rules.is_empty() {
            return Ok(Next::Call(IoCall::Delete(vec![(space, key)])));
        }
        let rules = DiscoveryRules {
            rules: self.rules.clone(),
        };
        let value = rules.to_bytes()?.into();
        Ok(Next::Call(IoCall::Write(vec![(space, key, value)])))
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

impl Operation for SetDiscoveryOperation {
    type Output = Vec<DiscoveryRule>;
    type Error = ImportRuleError;

    fn first(&mut self) -> SetTurn {
        check_rules(&self.rules)?;
        let reads = vec![
            (
                BACKEND_IMPORT_KEYSPACE.to_string(),
                import_key(self.group_id, self.import_id),
            ),
            group_fence_key(self.group_id),
        ];
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> SetTurn {
        if self.written {
            result.done()?;
            return Ok(Next::Done(std::mem::take(&mut self.rules)));
        }
        self.write(values::<2>(result)?)
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// The stored discovery rules of a rule; none when it has no row.
#[derive(Debug, PartialEq)]
pub struct ReadDiscoveryOperation {
    key: Key,
}

impl ReadDiscoveryOperation {
    pub fn new(group_id: GroupId, import_id: Ulid) -> Self {
        Self {
            key: import_key(group_id, import_id),
        }
    }
}

impl Operation for ReadDiscoveryOperation {
    type Output = Vec<DiscoveryRule>;
    type Error = ImportRuleError;

    fn first(&mut self) -> SetTurn {
        let read = read_row(IMPORT_DISCOVERY_KEYSPACE, self.key.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> SetTurn {
        let stored = result.value()?;
        let rules = (stored.as_deref())
            .map(DiscoveryRules::from_bytes)
            .transpose()?;
        Ok(Next::Done(
            rules.map(|rules| rules.rules).unwrap_or_default(),
        ))
    }
}

/// The caller's WRITE on the rule's bucket, which every key a rule places lies in. A token
/// limited to some paths is refused: rules place keys anywhere below the rule's folder.
#[derive(Debug, PartialEq)]
pub struct RuleAccessOperation {
    auth: AuthContext,
    node_id: NodeId,
    group_id: GroupId,
    bucket: String,
}

impl RuleAccessOperation {
    pub fn new(auth: &AuthContext, node_id: NodeId, rule: &BackendImport) -> Self {
        Self {
            auth: auth.clone(),
            node_id,
            group_id: rule.group_id,
            bucket: rule.bucket.clone(),
        }
    }
}

impl Operation for RuleAccessOperation {
    type Output = BucketAccess;
    type Error = ImportRuleError;

    fn first(&mut self) -> Result<Next<BucketAccess>, ImportRuleError> {
        if self.auth.path_restrictions.is_some() {
            return Err(ImportRuleError::Forbidden);
        }
        let scope = (self.auth.realm_id, self.group_id, self.node_id);
        let load = BucketAccessOperation::new(&self.auth, scope, &self.bucket);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BucketAccess>, ImportRuleError> {
        let access =
            result
                .child::<BucketAccess, AuthorizeError>()?
                .map_err(|error| match error {
                    AuthorizeError::PermissionDenied | AuthorizeError::Policy(_) => {
                        ImportRuleError::Forbidden
                    }
                    AuthorizeError::Storage(error) => ImportRuleError::Storage(error),
                    AuthorizeError::CheckFailed(message) => ImportRuleError::Read(message),
                })?;
        match access.allows(None, &Permission::WRITE) {
            true => Ok(Next::Done(access)),
            false => Err(ImportRuleError::Forbidden),
        }
    }
}

/// The rows a rename's guards keep as read.
pub type RenameGuards = Vec<(String, Key, Option<Value>)>;

/// The rule and its earlier patterns as stored, read before its renames are checked, so a
/// rename starts only for the files the check saw.
#[derive(Debug, PartialEq)]
pub struct RenameGuardsOperation {
    reads: Vec<(String, Key)>,
}

impl RenameGuardsOperation {
    pub fn new(rule: &BackendImport) -> Self {
        Self {
            reads: vec![
                (BACKEND_IMPORT_KEYSPACE.to_string(), rule.key()),
                (
                    IMPORT_PATTERN_KEYSPACE.to_string(),
                    object_prefix(rule.import_id),
                ),
            ],
        }
    }
}

impl Operation for RenameGuardsOperation {
    type Output = RenameGuards;
    type Error = ImportRuleError;

    fn first(&mut self) -> Result<Next<RenameGuards>, ImportRuleError> {
        Ok(Next::Call(IoCall::Read(self.reads.clone())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RenameGuards>, ImportRuleError> {
        let [rule, patterns] = values::<2>(result)?;
        let guards = (self.reads.iter().cloned())
            .zip([rule, patterns])
            .map(|((space, key), value)| (space, key, value))
            .collect();
        Ok(Next::Done(guards))
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
    let access = drive(RuleAccessOperation::new(auth, node_id, rule), context).await?;
    let allow = |key: &str| access.allows(Some(key), &permission);
    let all = |_: &str| true;
    let planned = rename_page(context, rule, (&all, &allow), (None, None, 0)).await;
    Ok(planned.map_err(ImportRuleError::Read)?.plan)
}

type AccessPreviewTurn = Result<Next<DiscoveryPreview>, ImportRuleError>;

/// A preview for a caller with WRITE on the bucket; it shows only files whose key the caller may
/// read, and reads past pages that show none.
#[derive(Debug, PartialEq)]
pub struct AuthorizedPreviewOperation {
    auth: AuthContext,
    node_id: NodeId,
    rule: BackendImport,
    rules: Vec<DiscoveryRule>,
    after: Option<String>,
    access: Option<BucketAccess>,
    pages: usize,
}

impl AuthorizedPreviewOperation {
    pub fn new(
        (auth, node_id): (&AuthContext, NodeId),
        rule: &BackendImport,
        rules: &[DiscoveryRule],
        start_after: Option<String>,
    ) -> Self {
        Self {
            auth: auth.clone(),
            node_id,
            rule: rule.clone(),
            rules: rules.to_vec(),
            after: start_after,
            access: None,
            pages: 0,
        }
    }

    fn next_page(&mut self) -> AccessPreviewTurn {
        if self.pages == HIDDEN_PAGES {
            return Err(ImportRuleError::Invalid(
                "too many files in a row are hidden from the caller".to_string(),
            ));
        }
        self.pages += 1;
        let preview = PreviewDiscoveryOperation::new(&self.rule, &self.rules, self.after.take());
        Ok(Next::Call(IoCall::child(preview)))
    }

    fn page_read(&mut self, mut preview: DiscoveryPreview) -> AccessPreviewTurn {
        let access = self.access.as_ref();
        let access = access.ok_or(ImportRuleError::Unexpected("Preview"))?;
        let rule = &self.rule;
        preview.files.retain(|file| readable(access, rule, file));
        if preview.next.is_none() {
            return Ok(Next::Done(preview));
        }
        // `next` names only a file the caller sees, so hidden pages are read past here.
        if let Some(last) = preview.files.last() {
            preview.next = Some(last.path.clone());
            return Ok(Next::Done(preview));
        }
        self.after = preview.next;
        self.next_page()
    }
}

impl Operation for AuthorizedPreviewOperation {
    type Output = DiscoveryPreview;
    type Error = ImportRuleError;

    fn first(&mut self) -> AccessPreviewTurn {
        let access = RuleAccessOperation::new(&self.auth, self.node_id, &self.rule);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> AccessPreviewTurn {
        if self.access.is_none() {
            self.access = Some(result.child::<BucketAccess, ImportRuleError>()??);
            return self.next_page();
        }
        self.page_read(result.child::<DiscoveryPreview, ImportRuleError>()??)
    }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreviewStep {
    Backend,
    List,
    Rows,
    Shared,
    Earlier,
    Intents,
    Taken,
}

type PreviewTurn = Result<Next<DiscoveryPreview>, ImportRuleError>;

/// Lists one page of the rule's folder and places each file with `rules`, without writing.
#[derive(Debug, PartialEq)]
pub struct PreviewDiscoveryOperation {
    rule: BackendImport,
    rules: Vec<DiscoveryRule>,
    start_after: Option<String>,
    step: PreviewStep,
    backend: Option<GroupStorage>,
    objects: Vec<ExternalObject>,
    next_page: Option<String>,
    rows: Vec<Option<Value>>,
    shared: Vec<SharedRule>,
    earlier: Vec<String>,
    intents: PathIntents,
}

impl PreviewDiscoveryOperation {
    pub fn new(rule: &BackendImport, rules: &[DiscoveryRule], start_after: Option<String>) -> Self {
        Self {
            rule: rule.clone(),
            rules: rules.to_vec(),
            start_after,
            step: PreviewStep::Backend,
            backend: None,
            objects: Vec::new(),
            next_page: None,
            rows: Vec::new(),
            shared: Vec::new(),
            earlier: Vec::new(),
            intents: PathIntents::new(),
        }
    }

    fn call(&mut self, step: PreviewStep, call: IoCall) -> PreviewTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn backend_read(
        &mut self,
        backend: Result<Option<GroupStorage>, GroupQueryError>,
    ) -> PreviewTurn {
        let backend = backend.map_err(|error| ImportRuleError::Read(error.to_string()))?;
        let group_id = self.rule.group_id;
        let backend = backend
            .filter(|backend| backend.group_id == group_id)
            .ok_or_else(|| {
                ImportRuleError::Invalid("the storage backend is missing".to_string())
            })?;
        self.backend = Some(backend);
        let listing = BlobRequest::new(BlobEffect::ListExternal {
            backend: BackendRef::Group(self.rule.backend_id),
            prefix: self.rule.backend_prefix.clone(),
            start_after: self.start_after.take(),
            limit: PREVIEW_PAGE,
        });
        self.call(PreviewStep::List, IoCall::child(listing))
    }

    fn listed(&mut self, listed: Result<BlobEvent, BlobError>) -> PreviewTurn {
        let (objects, next) = match listed {
            Ok(BlobEvent::ExternalListed { objects, next }) => (objects, next),
            Ok(other) => {
                return Err(ImportRuleError::Read(format!(
                    "unexpected answer {other:?}"
                )));
            }
            Err(error) => return Err(ImportRuleError::Read(error.reason())),
        };
        let backend = self.backend.as_ref();
        let backend = backend.ok_or(ImportRuleError::Unexpected("Preview"))?;
        self.objects = (objects.into_iter())
            .filter(|object| !backend.is_reserved(&object.key))
            .collect();
        self.next_page = next;
        if self.objects.is_empty() {
            return self.read_shared();
        }
        let reads = (self.objects.iter())
            .map(|object| {
                let key = object_key(self.rule.import_id, &object.key);
                (IMPORTED_OBJECT_KEYSPACE.to_string(), key)
            })
            .collect();
        self.call(PreviewStep::Rows, IoCall::Read(reads))
    }

    fn read_shared(&mut self) -> PreviewTurn {
        let shared = ReadSharedOperation::new(&self.rule);
        self.call(PreviewStep::Shared, IoCall::child(shared))
    }

    fn paths(&self) -> Vec<&str> {
        (self.objects.iter())
            .map(|object| object.key.as_str())
            .collect()
    }

    /// Where a run would put each listed file.
    fn placed(&mut self, taken: HashSet<String>) -> PreviewTurn {
        let rule = &self.rule;
        let earlier = self.earlier.as_slice();
        let names = (rule, earlier, self.rules.as_slice());
        let mut files = Vec::with_capacity(self.objects.len());
        let rows = std::mem::take(&mut self.rows);
        for (object, row) in self.objects.iter().zip(rows) {
            let row = (row.as_deref())
                .map(ImportedObject::from_bytes)
                .transpose()?;
            let path = object.key.clone();
            let open = self.intents.get(&path).map_or(&[][..], Vec::as_slice);
            // An empty file of an open write is its reservation, as a run finds too.
            if object.size == 0 && !open.is_empty() {
                continue;
            }
            let file = match row {
                // A literal import whose name encodes a version is recovered as that version.
                Some(row)
                    if open.is_empty() && (row.key.is_some() || !decodes(rule, earlier, &path)) =>
                {
                    PreviewFile {
                        key: row_key(rule, &row, &path),
                        version_id: None,
                        claimed: Vec::new(),
                        outcome: FileOutcome::Known,
                        path,
                    }
                }
                // An open write is decided by a run that reads the file; until then it is
                // unresolved.
                Some(row) if !open.is_empty() => {
                    let mapped = row_key(rule, &row, &path);
                    intended(path, open, mapped)
                }
                _ if !open.is_empty() => intended(path, open, None),
                Some(_) => named(names, path),
                // A file of a shared folder that another rule takes is not listed here.
                None => match claim_file(names, &self.shared, taken.contains(&path), &path) {
                    Claim::Other => continue,
                    Claim::Conflict => unresolved(path, Vec::new()),
                    Claim::Own => named(names, path),
                },
            };
            files.push(file);
        }
        Ok(Next::Done(DiscoveryPreview {
            files,
            next: self.next_page.take(),
        }))
    }
}

impl Operation for PreviewDiscoveryOperation {
    type Output = DiscoveryPreview;
    type Error = ImportRuleError;

    fn first(&mut self) -> PreviewTurn {
        check_rules(&self.rules)?;
        let backend = GetBackendOperation::new(self.rule.backend_id);
        self.call(PreviewStep::Backend, IoCall::child(backend))
    }

    fn next(&mut self, result: IoResult) -> PreviewTurn {
        match self.step {
            PreviewStep::Backend => {
                self.backend_read(result.child::<Option<GroupStorage>, GroupQueryError>()?)
            }
            PreviewStep::List => self.listed(result.child::<BlobEvent, BlobError>()?),
            PreviewStep::Rows => {
                self.rows = result.values()?;
                self.read_shared()
            }
            PreviewStep::Shared => {
                self.shared = result.child::<Vec<SharedRule>, ImportRuleError>()??;
                let earlier = ReadEarlierOperation::new(self.rule.import_id);
                self.call(PreviewStep::Earlier, IoCall::child(earlier))
            }
            PreviewStep::Earlier => {
                self.earlier = result.child::<Vec<String>, ImportRuleError>()??;
                let intents = PathIntentsOperation::new(self.rule.import_id, &self.paths());
                self.call(PreviewStep::Intents, IoCall::child(intents))
            }
            PreviewStep::Intents => {
                self.intents = result.child::<PathIntents, ImportRuleError>()??;
                let taken = TakenPathsOperation::new(&self.shared, &self.paths());
                self.call(PreviewStep::Taken, IoCall::child(taken))
            }
            PreviewStep::Taken => self.placed(result.child::<HashSet<String>, ImportRuleError>()??),
        }
    }
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
        use crate::testing::test_context;
        use crate::tests::s3::test_storage;
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

        let load =
            |auth: &AuthContext| drive(RuleAccessOperation::new(auth, node, &rule), &context);
        let access = load(&auth).await.unwrap();
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
        let refused = load(&restricted).await;
        assert!(matches!(refused, Err(ImportRuleError::Forbidden)));
        let mut stranger = auth;
        stranger.user_id = aruna_core::UserId::local(Ulid::from_parts(9, 9), realm);
        let refused = load(&stranger).await;
        assert!(matches!(refused, Err(ImportRuleError::Forbidden)));
    }
    fn rules() -> Vec<DiscoveryRule> {
        vec![DiscoveryRule {
            pattern: "*.faa.gz".to_string(),
            folder: "proteins/".to_string(),
        }]
    }

    #[test]
    fn restricted_token_refused() {
        let realm = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
        let auth = AuthContext {
            user_id: aruna_core::UserId::nil(realm),
            realm_id: realm,
            path_restrictions: Some(Vec::new()),
            session: None,
        };
        let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let mut operation = RuleAccessOperation::new(&auth, node, &import_rule());
        assert!(matches!(operation.first(), Err(ImportRuleError::Forbidden)));
    }

    #[test]
    fn foreign_backend_refused() {
        let mut operation = PreviewDiscoveryOperation::new(&import_rule(), &rules(), None);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert!(matches!(
            operation.backend_read(Ok(None)),
            Err(ImportRuleError::Invalid(_))
        ));
    }

    #[test]
    fn guards_keep_rows() {
        let rule = import_rule();
        let mut operation = RenameGuardsOperation::new(&rule);
        operation.first().unwrap();
        let stored = Some(Value::from(vec![1]));
        let turn = operation.next(IoResult::Values(vec![stored.clone(), None]));
        let Ok(Next::Done(guards)) = turn else {
            panic!("the guards are returned");
        };
        assert_eq!(
            guards[0],
            (BACKEND_IMPORT_KEYSPACE.to_string(), rule.key(), stored)
        );
        assert_eq!(guards[1].2, None);
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
        assert!(matches!(
            operation.first(),
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
    ) -> (SetDiscoveryOperation, SetTurn) {
        let mut operation = SetDiscoveryOperation::new(Ulid::nil(), Ulid::nil(), rules);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.first().unwrap();
        let turn = operation.next(IoResult::Values(vec![rule.map(Into::into), None]));
        (operation, turn)
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
        let (_, turn) = written(folders, Some(rule.to_bytes().unwrap()));
        assert!(matches!(turn, Err(ImportRuleError::Invalid(_))));
    }

    #[test]
    fn stores_with_rule() {
        let (mut operation, turn) = written(rules(), Some(import_rule().to_bytes().unwrap()));
        let Ok(Next::Call(IoCall::Write(writes))) = turn else {
            panic!("expected the rules write, got {turn:?}")
        };
        assert_eq!(writes[0].0, IMPORT_DISCOVERY_KEYSPACE);
        assert_eq!(
            DiscoveryRules::from_bytes(&writes[0].2).unwrap().rules,
            rules()
        );
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(rules())));

        // No rules delete the row.
        let (_, turn) = written(Vec::new(), Some(import_rule().to_bytes().unwrap()));
        let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
            panic!("expected the rules delete, got {turn:?}")
        };
        assert_eq!(deletes[0].0, IMPORT_DISCOVERY_KEYSPACE);

        // A removed rule gets no rules.
        let (_, turn) = written(rules(), None);
        assert_eq!(turn, Err(ImportRuleError::NotFound));
    }
}
