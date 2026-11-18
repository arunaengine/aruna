//! Lists draft files an anonymous reader could not fetch once the document is public.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::id::NodeId;
use aruna_core::metadata::MetadataError;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{HashIndex, object_permission_path};
use aruna_core::structs::storage::data::identity::{DataIdentity, text_values};
use aruna_core::types::GroupId;
use serde_json::Value as JsonValue;

use crate::auth::error::AuthorizeError;
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::blob::holders::{GetHoldersError, GetHoldersOperation};
use crate::blob::permission_paths::{ResolvePathsError, ResolvePathsOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::replication::locations::{LocalSummary, LocationSummaryError, LocationSummaryOperation};
use crate::replication::protocol::LocationSummaryRequest;

const FILE_TYPES: [&str; 4] = [
    "File",
    "MediaObject",
    "http://schema.org/MediaObject",
    "https://schema.org/MediaObject",
];
/// A draft past these bounds reports what was resolved rather than fanning out
/// further; the preview is an advisory warning, not an authorization decision.
const MAX_DRAFT_FILES: usize = 256;
const MAX_ENTITY_PATHS: usize = 16;
/// The whole preview's budget; files past it leave the preview incomplete.
const PREVIEW_BUDGET_MS: u64 = 10_000;

/// A draft data entity the realm's anonymous principal may not read. The
/// location fields are omitted when the caller may not read the object either.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestrictedFile {
    pub entity_id: String,
    pub group_id: Option<GroupId>,
    /// The object's permission path, so a grant can name exactly this object.
    pub permission_path: Option<String>,
    pub bucket: Option<String>,
    pub key: Option<String>,
}

#[derive(Debug, PartialEq)]
pub struct RestrictedFilesPreview {
    pub files: Vec<RestrictedFile>,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct ObjectPath {
    group_id: GroupId,
    bucket: String,
    key: String,
    path: String,
}

/// What one draft file adds to the preview.
#[derive(Debug, PartialEq)]
pub(crate) enum FileAccess {
    /// Anonymous READ reaches it.
    Public,
    /// Its paths or holders could not all be checked, so the preview is incomplete.
    Unchecked,
    Restricted(RestrictedFile),
}

/// One deadline-bounded call: a member that does not finish in `remaining` is unfinished.
fn bounded<O>(operation: O, remaining: Duration) -> IoCall
where
    O: Operation + Send + 'static,
    O::Output: std::fmt::Debug + Send + PartialEq + 'static,
    O::Error: std::fmt::Debug + Send + PartialEq + 'static,
{
    let member = Effect::SubOperation(boxed_suboperation(operation, |result| {
        Event::Child(ChildOutcome::new(result))
    }));
    IoCall::Group(EffectGroup {
        members: vec![member],
        limit: 1,
        form: GroupForm::All {
            deadline: Some(remaining),
        },
    })
}

/// The answer of a [`bounded`] call; `None` when it ran out of time or failed to run.
fn bounded_result<T: 'static, E: 'static>(result: IoResult) -> Option<Result<T, E>> {
    let IoResult::Group(GroupEvent::Finished { results }) = result else {
        return None;
    };
    match results.into_iter().next()? {
        GroupResult::Done(event) => match *event {
            Event::Child(outcome) => outcome.take(),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Debug, PartialEq)]
enum FileStage {
    /// The location summary of the local object, for the principal at this index.
    Summary(usize),
    Aliases,
    /// The READ check of the path at this index, anonymous first, then the caller.
    Read {
        path: usize,
        anonymous: bool,
    },
    Holders,
}

/// Checks one draft data entity: resolves the permission paths of its object here, then
/// whether anonymous READ reaches any of them, which path the caller may read, and whether
/// another node holds the content. Only paths the caller may read are reported.
#[derive(Debug, PartialEq)]
pub(crate) struct FileAccessOperation {
    realm_id: RealmId,
    node_id: NodeId,
    auth: AuthContext,
    entity_id: String,
    identity: DataIdentity,
    remote_possible: bool,
    deadline_ms: u64,
    now_ms: u64,
    local: Option<(String, String, Option<ulid::Ulid>)>,
    paths: Vec<ObjectPath>,
    seen: BTreeSet<String>,
    complete: bool,
    checked: bool,
    visible: Option<ObjectPath>,
    stage: FileStage,
}

impl FileAccessOperation {
    fn new(
        (realm_id, node_id, auth): (RealmId, NodeId, AuthContext),
        (entity_id, identity): (String, DataIdentity),
        (remote_possible, deadline_ms): (bool, u64),
    ) -> Self {
        let exact = (identity.exact.as_ref())
            .filter(|exact| exact.realm_id == realm_id && exact.node_id == node_id)
            .map(|exact| (exact.bucket.clone(), exact.key.clone(), Some(exact.version)));
        // Without an exact version, an `s3://` location names the local object's current version.
        let local = exact.or_else(|| {
            (identity.location.as_ref())
                .filter(|_| identity.exact.is_none())
                .map(|location| (location.bucket.clone(), location.key.clone(), None))
        });
        Self {
            realm_id,
            node_id,
            auth,
            entity_id,
            identity,
            remote_possible,
            deadline_ms,
            now_ms: 0,
            local,
            paths: Vec::new(),
            seen: BTreeSet::new(),
            complete: true,
            checked: true,
            visible: None,
            stage: FileStage::Summary(0),
        }
    }

    fn remaining(&self) -> Duration {
        Duration::from_millis(self.deadline_ms.saturating_sub(self.now_ms))
    }

    fn principal(&self, index: usize) -> AuthContext {
        match index {
            0 => AuthContext::anonymous(self.realm_id),
            _ => self.auth.clone(),
        }
    }

    fn summary(&mut self, index: usize) -> Result<Next<FileAccess>, MetadataError> {
        let Some((bucket, key, version)) = self.local.clone() else {
            return self.aliases();
        };
        if index > 1 {
            if self.paths.is_empty() && version.is_some() {
                self.complete = false;
            }
            return self.aliases();
        }
        let request = LocationSummaryRequest {
            realm_id: self.realm_id,
            bucket,
            key,
            version_id: version,
            auth_context: self.principal(index),
        };
        let summary = LocationSummaryOperation::new_local(self.node_id, request).with_policy(true);
        self.stage = FileStage::Summary(index);
        Ok(Next::Call(bounded(summary, self.remaining())))
    }

    fn summarized(
        &mut self,
        index: usize,
        result: IoResult,
    ) -> Result<Next<FileAccess>, MetadataError> {
        let (bucket, key, version) = self.local.clone().unwrap_or_default();
        let summary =
            bounded_result::<LocalSummary, LocationSummaryError>(result).and_then(Result::ok);
        if let Some(summary) = summary
            && version.is_none_or(|version| summary.summary.version_id == Some(version))
            && summary.summary.materialized
            && summary.summary.blob_size.is_some()
            && let Some(group_id) = summary.summary.group_id
        {
            let path = object_permission_path(self.realm_id, group_id, self.node_id, &bucket, &key);
            self.seen.insert(path.clone());
            self.paths.push(ObjectPath {
                group_id,
                bucket,
                key,
                path,
            });
            return self.summary(2);
        }
        self.summary(index + 1)
    }

    fn aliases(&mut self) -> Result<Next<FileAccess>, MetadataError> {
        let (realm_id, node_id) = (self.realm_id, self.node_id);
        if (self.identity.exact.as_ref())
            .is_some_and(|exact| exact.node_id != node_id || exact.realm_id != realm_id)
        {
            self.complete = false;
        }
        let hash = (self.identity.hash).filter(|_| {
            self.identity
                .hash_realm
                .is_none_or(|realm| realm == realm_id)
        });
        let Some(hash) = hash else {
            return self.check(0, true);
        };
        self.stage = FileStage::Aliases;
        Ok(Next::Call(IoCall::child(ResolvePathsOperation::new(hash))))
    }

    fn aliased(&mut self, aliases: Vec<HashIndex>) -> Result<Next<FileAccess>, MetadataError> {
        for alias in aliases
            .iter()
            .filter(|alias| alias.realm_id == self.realm_id)
        {
            if alias.node_id != self.node_id {
                self.complete = false;
                continue;
            }
            let path = alias.permission_path();
            if !self.seen.insert(path.clone()) {
                continue;
            }
            if self.paths.len() >= MAX_ENTITY_PATHS {
                self.complete = false;
                break;
            }
            self.paths.push(ObjectPath {
                group_id: alias.group_id,
                bucket: alias.bucket.clone(),
                key: alias.key.clone(),
                path,
            });
        }
        self.check(0, true)
    }

    /// The READ check of one path for the anonymous principal or the caller.
    fn check(&mut self, path: usize, anonymous: bool) -> Result<Next<FileAccess>, MetadataError> {
        if self.paths.is_empty() {
            return Ok(Next::Done(FileAccess::Unchecked));
        }
        if path == 0 && anonymous {
            self.checked = self.complete;
        }
        let Some(object) = self.paths.get(path) else {
            return self.holders();
        };
        let principal = match anonymous {
            true => AuthContext::anonymous(self.realm_id),
            false => self.auth.clone(),
        };
        let extras = PolicyRequestExtras::operation("s3.GetObject");
        let authorize = AuthorizeOperation::new(
            self.realm_id,
            &principal,
            &object.path,
            &Permission::READ,
            extras,
        );
        self.stage = FileStage::Read { path, anonymous };
        Ok(Next::Call(IoCall::child(authorize)))
    }

    fn read(
        &mut self,
        (path, anonymous): (usize, bool),
        result: IoResult,
    ) -> Result<Next<FileAccess>, MetadataError> {
        let readable = match result.child::<(), AuthorizeError>()? {
            Ok(()) => Some(true),
            Err(
                AuthorizeError::PermissionDenied
                | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }),
            ) => Some(false),
            Err(_) => None,
        };
        if readable.is_none() {
            self.checked = false;
        }
        if anonymous {
            if readable == Some(true) {
                return Ok(Next::Done(FileAccess::Public));
            }
            return self.check(path, false);
        }
        if readable == Some(true) && self.visible.is_none() {
            self.visible = self.paths.get(path).cloned();
        }
        self.check(path + 1, true)
    }

    fn holders(&mut self) -> Result<Next<FileAccess>, MetadataError> {
        match self.identity.hash.filter(|_| self.remote_possible) {
            Some(hash) => {
                let holders = GetHoldersOperation::new(hash, self.realm_id, self.node_id);
                self.stage = FileStage::Holders;
                Ok(Next::Call(bounded(holders, self.remaining())))
            }
            None => Ok(Next::Done(self.verdict())),
        }
    }

    fn verdict(&mut self) -> FileAccess {
        if !self.checked {
            return FileAccess::Unchecked;
        }
        let visible = self.visible.take();
        FileAccess::Restricted(RestrictedFile {
            entity_id: std::mem::take(&mut self.entity_id),
            group_id: visible.as_ref().map(|path| path.group_id),
            permission_path: visible.as_ref().map(|path| path.path.clone()),
            bucket: visible.as_ref().map(|path| path.bucket.clone()),
            key: visible.map(|path| path.key),
        })
    }
}

impl Operation for FileAccessOperation {
    type Output = FileAccess;
    type Error = MetadataError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<FileAccess>, MetadataError> {
        self.summary(0)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<FileAccess>, MetadataError> {
        match self.stage {
            FileStage::Summary(index) => self.summarized(index, result),
            FileStage::Aliases => match result.child::<Vec<HashIndex>, ResolvePathsError>()? {
                Ok(aliases) => self.aliased(aliases),
                Err(_) => Ok(Next::Done(FileAccess::Unchecked)),
            },
            FileStage::Read { path, anonymous } => self.read((path, anonymous), result),
            FileStage::Holders => {
                let holders = bounded_result::<Vec<NodeId>, GetHoldersError>(result);
                self.checked &= matches!(holders, Some(Ok(holders)) if holders.is_empty());
                Ok(Next::Done(self.verdict()))
            }
        }
    }
}

/// Lists the draft's Aruna data entities that anonymous READ would not reach, so a public
/// publish can warn about them, within one time budget. Only paths the caller may read are
/// resolved; nothing beyond "not publicly readable" about a foreign object leaks.
#[derive(Debug, PartialEq)]
pub struct RestrictedFilesOperation {
    realm_id: RealmId,
    node_id: NodeId,
    auth: AuthContext,
    files: VecDeque<(String, DataIdentity)>,
    complete: bool,
    remote_possible: bool,
    deadline_ms: Option<u64>,
    now_ms: u64,
    restricted: Vec<RestrictedFile>,
    configured: bool,
}

impl RestrictedFilesOperation {
    pub fn new(
        (realm_id, node_id): (RealmId, NodeId),
        auth: AuthContext,
        rocrate: &JsonValue,
    ) -> Self {
        let files = draft_files(rocrate);
        let complete = files.len() <= MAX_DRAFT_FILES;
        let files = (files.into_iter().take(MAX_DRAFT_FILES))
            .map(|(entity_id, content_urls)| {
                let identity = DataIdentity::read(&entity_id, &content_urls);
                (entity_id, identity)
            })
            .filter(|(_, identity)| identity.is_aruna())
            .collect();
        Self {
            realm_id,
            node_id,
            auth,
            files,
            complete,
            remote_possible: true,
            deadline_ms: None,
            now_ms: 0,
            restricted: Vec::new(),
            configured: false,
        }
    }

    fn next_file(&mut self) -> Result<Next<RestrictedFilesPreview>, MetadataError> {
        let deadline_ms = self.deadline_ms.unwrap_or(self.now_ms);
        let Some((entity_id, identity)) = self.files.pop_front() else {
            return Ok(Next::Done(self.preview()));
        };
        if self.now_ms >= deadline_ms {
            self.complete = false;
            return Ok(Next::Done(self.preview()));
        }
        let file = FileAccessOperation::new(
            (self.realm_id, self.node_id, self.auth.clone()),
            (entity_id, identity),
            (self.remote_possible, deadline_ms),
        );
        Ok(Next::Call(IoCall::child(file)))
    }

    fn preview(&mut self) -> RestrictedFilesPreview {
        RestrictedFilesPreview {
            files: std::mem::take(&mut self.restricted),
            complete: self.complete,
        }
    }
}

impl Operation for RestrictedFilesOperation {
    type Output = RestrictedFilesPreview;
    type Error = MetadataError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
        self.deadline_ms
            .get_or_insert(now_ms.saturating_add(PREVIEW_BUDGET_MS));
    }

    fn first(&mut self) -> Result<Next<RestrictedFilesPreview>, MetadataError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RestrictedFilesPreview>, MetadataError> {
        if !self.configured {
            self.configured = true;
            let config = result.child::<RealmConfigDocument, GetConfigError>()?;
            let node = self.node_id;
            // Another node may hold the content unless this node is the realm's only one.
            self.remote_possible = config.map_or(true, |config| {
                !config.has_node(node)
                    || (config.nodes.iter()).any(|other| other.node_id != node.to_string())
            });
            return self.next_file();
        }
        match result.child::<FileAccess, MetadataError>()?? {
            FileAccess::Public => {}
            FileAccess::Unchecked => self.complete = false,
            FileAccess::Restricted(file) => self.restricted.push(file),
        }
        self.next_file()
    }
}

fn draft_files(rocrate: &JsonValue) -> Vec<(String, Vec<String>)> {
    let Some(graph) = rocrate.get("@graph").and_then(JsonValue::as_array) else {
        return Vec::new();
    };
    graph
        .iter()
        .filter_map(|entity| {
            let entity = entity.as_object()?;
            let entity_id = entity.get("@id")?.as_str()?;
            is_file(entity.get("@type")?)
                .then(|| (entity_id.to_string(), text_values(entity.get("contentUrl"))))
        })
        .take(MAX_DRAFT_FILES + 1)
        .collect()
}

pub(crate) fn is_file(value: &JsonValue) -> bool {
    match value {
        JsonValue::String(value) => FILE_TYPES.contains(&value.as_str()),
        JsonValue::Array(values) => values.iter().any(is_file),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};

    async fn restricted_files(
        context: &DriverContext,
        realm_id: RealmId,
        node_id: NodeId,
        auth: &AuthContext,
        rocrate: &JsonValue,
    ) -> Result<RestrictedFilesPreview, MetadataError> {
        let preview = RestrictedFilesOperation::new((realm_id, node_id), auth.clone(), rocrate);
        drive(preview, context).await
    }

    #[test]
    fn foreign_entities_skipped() {
        // Entities without an Aruna identity need no reads beyond the realm config.
        let realm_id = RealmId::from_bytes([1; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let rocrate = json!({"@graph": [{"@id": "https://example.test/a", "@type": "File"}]});
        let auth = AuthContext::anonymous(realm_id);
        let mut preview = RestrictedFilesOperation::new((realm_id, node_id), auth, &rocrate);
        preview.stamp(1);
        assert!(matches!(preview.first(), Ok(Next::Call(IoCall::Child(_)))));
        let config = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
        let done = preview.next(IoResult::Child(ChildOutcome::new(config)));
        let expected = RestrictedFilesPreview {
            files: Vec::new(),
            complete: true,
        };
        assert_eq!(done, Ok(Next::Done(expected)));
    }
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, PATHS_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE,
        S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::identity::auth::{Actor, Role};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
    };
    use aruna_core::structs::storage::blob::{BucketInfo, HashIndex};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::replication::ARUNA_DATA_PREFIX;
    use serde_json::json;
    use std::collections::{HashMap, HashSet};
    use ulid::Ulid;

    const BUCKET: &str = "reads";
    const KEY: &str = "raw/one.csv";

    struct Fixture {
        context: DriverContext,
        realm_id: RealmId,
        node_id: NodeId,
        owner: AuthContext,
        hash: [u8; 32],
        permission_path: String,
        _tempdir: tempfile::TempDir,
    }

    async fn fixture(anonymous_read: bool) -> Fixture {
        let staging = crate::tests::staging::setup_driver_context().await;
        let context = staging.driver_context;
        let realm_id = RealmId::from_bytes([61; 32]);
        let owner = UserId::local(Ulid::from_bytes([62; 16]), realm_id);
        let group_id = Ulid::from_bytes([63; 16]);
        let version_id = Ulid::from_bytes([64; 16]);
        let node_id = iroh::SecretKey::from_bytes(&[65; 32]).public();
        let hash = [66; 32];
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.ensure_node(node_id, RealmNodeKind::Server);
        let mut realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
        if anonymous_read {
            let role_id = Ulid::from_bytes([67; 16]);
            realm_auth.roles.insert(
                role_id,
                Role {
                    role_id,
                    name: "everyone".to_string(),
                    permissions: HashMap::from([(
                        format!("/{realm_id}/g/{group_id}/**"),
                        Permission::READ,
                    )]),
                    assigned_users: HashSet::from([UserId::nil(realm_id)]),
                },
            );
        }
        let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
        let group = Group {
            display_name: "preview".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner,
        };
        let bucket = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: owner,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let alias = HashIndex::new(hash, version_id, realm_id, group_id, node_id, BUCKET, KEY);
        let writes = vec![
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                realm_id.as_bytes().to_vec().into(),
                config.to_bytes(&actor).unwrap().into(),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                realm_id.as_bytes().to_vec().into(),
                realm_auth.to_bytes(&actor).unwrap().into(),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                group_id.to_bytes().to_vec().into(),
                group_auth.to_bytes(&actor).unwrap().into(),
            ),
            (
                GROUP_KEYSPACE.to_string(),
                group_id.to_bytes().to_vec().into(),
                group.to_bytes(&actor).unwrap().into(),
            ),
            (
                S3_BUCKET_KEYSPACE.to_string(),
                BUCKET.as_bytes().to_vec().into(),
                bucket.to_bytes().unwrap().into(),
            ),
            (
                PATHS_INDEX_KEYSPACE.to_string(),
                alias.to_bytes().unwrap().into(),
                Vec::new().into(),
            ),
        ];
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes,
                    txn_id: None
                })
                .await,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
        Fixture {
            context,
            realm_id,
            node_id,
            owner: AuthContext {
                user_id: owner,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            hash,
            permission_path: object_permission_path(realm_id, group_id, node_id, BUCKET, KEY),
            _tempdir: staging._tempdir,
        }
    }

    fn draft(hash: [u8; 32]) -> JsonValue {
        json!({
            "@graph": [
                {"@id": "./", "@type": "Dataset", "name": "draft"},
                {
                    "@id": format!("{ARUNA_DATA_PREFIX}{}", hex::encode(hash)),
                    "@type": "File",
                    "name": "one.csv",
                    "contentUrl": format!("s3://{BUCKET}/{KEY}")
                }
            ]
        })
    }

    #[tokio::test]
    async fn lists_private_object() {
        let fixture = fixture(false).await;

        let restricted = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &fixture.owner,
            &draft(fixture.hash),
        )
        .await
        .unwrap();

        assert_eq!(restricted.files.len(), 1);
        assert_eq!(restricted.files[0].bucket.as_deref(), Some(BUCKET));
        assert_eq!(restricted.files[0].key.as_deref(), Some(KEY));
        assert_eq!(
            restricted.files[0].permission_path.as_deref(),
            Some(fixture.permission_path.as_str())
        );
    }

    #[tokio::test]
    async fn skips_public_object() {
        let fixture = fixture(true).await;

        let restricted = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &fixture.owner,
            &draft(fixture.hash),
        )
        .await
        .unwrap();

        assert!(restricted.files.is_empty());
    }

    #[tokio::test]
    async fn hides_foreign_object() {
        let fixture = fixture(false).await;
        let stranger = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([68; 16]), fixture.realm_id),
            realm_id: fixture.realm_id,
            path_restrictions: None,
            session: None,
        };

        let restricted = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &stranger,
            &draft(fixture.hash),
        )
        .await
        .unwrap();

        assert_eq!(restricted.files.len(), 1);
        assert!(restricted.files[0].permission_path.is_none());
        assert!(restricted.files[0].bucket.is_none());
        assert!(restricted.files[0].key.is_none());
    }

    #[tokio::test]
    async fn ignores_unknown_object() {
        let fixture = fixture(false).await;

        let restricted = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &fixture.owner,
            &draft([99; 32]),
        )
        .await
        .unwrap();

        assert!(restricted.files.is_empty());
        assert!(!restricted.complete);
    }

    async fn add_alias(fixture: &Fixture, group_id: GroupId, key: &str) {
        let alias = HashIndex::new(
            fixture.hash,
            Ulid::generate(),
            fixture.realm_id,
            group_id,
            fixture.node_id,
            BUCKET,
            key,
        );
        let event = fixture
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: PATHS_INDEX_KEYSPACE.to_string(),
                key: alias.to_bytes().unwrap().into(),
                value: Vec::<u8>::new().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn skips_foreign_alias() {
        let fixture = fixture(true).await;
        add_alias(&fixture, Ulid::from_bytes([1; 16]), "foreign.csv").await;
        let preview = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &fixture.owner,
            &draft(fixture.hash),
        )
        .await
        .unwrap();
        assert!(preview.files.is_empty());
        assert!(preview.complete);
    }

    #[tokio::test]
    async fn marks_unavailable_policy() {
        let fixture = fixture(false).await;
        let event = fixture
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: GROUP_KEYSPACE.to_string(),
                key: Ulid::from_bytes([63; 16]).to_bytes().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::DeleteResult { .. })
        ));
        let preview = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &fixture.owner,
            &draft(fixture.hash),
        )
        .await
        .unwrap();
        assert!(preview.files.is_empty());
        assert!(!preview.complete);
    }

    #[tokio::test]
    async fn marks_alias_limit() {
        let fixture = fixture(false).await;
        for index in 0..MAX_ENTITY_PATHS {
            add_alias(
                &fixture,
                Ulid::from_bytes([63; 16]),
                &format!("alias-{index}.csv"),
            )
            .await;
        }
        let preview = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &fixture.owner,
            &draft(fixture.hash),
        )
        .await
        .unwrap();
        assert!(preview.files.is_empty());
        assert!(!preview.complete);
    }

    #[tokio::test]
    async fn marks_file_limit() {
        let fixture = fixture(true).await;
        let file = draft(fixture.hash)["@graph"][1].clone();
        let document = json!({"@graph": vec![file; MAX_DRAFT_FILES + 1]});
        let preview = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &fixture.owner,
            &document,
        )
        .await
        .unwrap();
        assert!(preview.files.is_empty());
        assert!(!preview.complete);
    }

    #[tokio::test]
    async fn marks_remote_exact() {
        let fixture = fixture(true).await;
        let remote = iroh::SecretKey::from_bytes(&[99; 32]).public();
        let exact = aruna_core::structs::storage::replication::VersionedObjectArn::new(
            fixture.realm_id,
            remote,
            BUCKET.to_string(),
            KEY.to_string(),
            Ulid::generate(),
        )
        .unwrap();
        let document = json!({"@graph": [{"@id": exact.to_w3id(), "@type": "File"}]});
        let preview = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &fixture.owner,
            &document,
        )
        .await
        .unwrap();
        assert!(preview.files.is_empty());
        assert!(!preview.complete);
    }

    #[tokio::test]
    async fn checks_anonymous_first() {
        let fixture = fixture(true).await;
        let mut caller = fixture.owner.clone();
        caller.path_restrictions =
            Some(vec![aruna_core::structs::identity::auth::PathRestriction {
                pattern: format!("/{}/g/*/meta/**", fixture.realm_id),
                permission: Permission::READ,
            }]);
        let preview = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &caller,
            &draft(fixture.hash),
        )
        .await
        .unwrap();
        assert!(preview.files.is_empty());
        assert!(preview.complete);
    }

    #[tokio::test]
    async fn skips_external_files() {
        let fixture = fixture(false).await;
        let document =
            json!({"@graph": [{"@id": "https://example.org/file.csv", "@type": "File"}]});
        let preview = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &fixture.owner,
            &document,
        )
        .await
        .unwrap();
        assert!(preview.files.is_empty());
        assert!(preview.complete);
    }

    #[tokio::test]
    async fn marks_missing_version() {
        let fixture = fixture(false).await;
        let exact = aruna_core::structs::storage::replication::VersionedObjectArn::new(
            fixture.realm_id,
            fixture.node_id,
            BUCKET,
            KEY,
            Ulid::generate(),
        )
        .unwrap();
        let document = json!({"@graph": [{"@id": exact.to_w3id(), "@type": "File"}]});
        let preview = restricted_files(
            &fixture.context,
            fixture.realm_id,
            fixture.node_id,
            &fixture.owner,
            &document,
        )
        .await
        .unwrap();
        assert!(preview.files.is_empty());
        assert!(!preview.complete);
    }

    #[test]
    fn selects_data_entities() {
        let document = json!({
            "@graph": [
                {"@id": "./", "@type": "Dataset", "name": "draft"},
                {"@id": "one", "@type": "File", "contentUrl": "s3://reads/one.csv"},
                {"@id": "two", "@type": ["MediaObject", "Thing"],
                 "contentUrl": ["s3://reads/two.csv", {"@id": "s3://reads/dup.csv"}]},
                {"@id": "three", "@type": "CreativeWork"}
            ]
        });

        let files = draft_files(&document);

        assert_eq!(
            files,
            vec![
                ("one".to_string(), vec!["s3://reads/one.csv".to_string()]),
                (
                    "two".to_string(),
                    vec![
                        "s3://reads/two.csv".to_string(),
                        "s3://reads/dup.csv".to_string()
                    ]
                ),
            ]
        );
    }

    #[test]
    fn ignores_missing_graph() {
        assert!(draft_files(&json!({"@id": "./"})).is_empty());
    }
}
