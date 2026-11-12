//! Serves one run's host calls on the receiver: every call passes the per call check, and a
//! mutating call records its intent before its effect. Objects answer info and listings only.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use aruna_core::NodeId;
use aruna_core::injection::{OutgoingRequest, expose_response, inject_secret, scrub};
use aruna_core::metadata::{AuthToken, MetadataError};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{LogLine, PluginRun};
use aruna_core::secrets::{Delivery, ResponseExposure, SecretFields, header_name};
use aruna_core::structs::identity::auth::{
    Actor, AuthContext, PathRestriction, Permission as Access,
};
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::data::identity::{CONTENT_URL, DataIdentity};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::broker::{
    Broker, CallContext, Created, Document, Entry, ErrorKind, Field, FileChange, FilePage,
    FullResponse, HostError, Intent, Listing, MetadataCreate, MetadataPatch, ObjectInfo,
    ObjectPage, Response, SendRequest, StatePut,
};
use aruna_plugins::connection::{Readable, relative_path};
use aruna_plugins::permission::Permission;
use async_trait::async_trait;
use base64::Engine;
use serde_json::Value;
use tracing::warn;
use ulid::Ulid;

use super::check::{RunScope, authorize_call, check_call, matching, unavailable};
use super::creates::{Lookup, Step, claim_key, next_step, settle_key};
use super::grants::{InstallScope, accesses, parse_grants};
use super::held::{HeldSecrets, ask_release};
use super::install::{pick_version, read_config, read_manifest};
use super::intents::{Begun, begin, clear_in, mark_unknown, settle_in, written};
use super::outbound::{Answer, Egress, Outbound, SendError};
use super::rate::take_call;
use super::release::{ReleaseError, ReleaseKeys, ReleaseRequest, Released};
use super::state::{PutError, check_key, put_state, read_state};
use super::store::{StoreError, Txn, transact};
use crate::auth::request_authorization::AuthorizeError;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::forward::transport::MetadataWriteError;
use crate::metadata::api::{
    ExportMetadataRequest, ExportMetadataResult, GetVisibleRequest, ListVisibleRequest,
    MetadataApiError, MetadataListOrder, RoCrateExportView, list_visible_documents,
};
use crate::metadata::create_document::{CreateDocumentError, CreateDocumentPayload};
use crate::metadata::forward::{
    CreateAuthorizedError, PinnedCreate, create_with_id, export_rocrate_routed,
    get_metadata_routed, route_metadata_update,
};
use crate::metadata::handle::normalize_property;
use crate::metadata::update_document::{UpdateDocumentError, UpdateDocumentMutation};
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::object::head::{HeadObjectInput, HeadObjectOperation};
use crate::s3::object::list::{ListBucketInput, ListBucketOperation, ListContinuationToken};

/// Entries of one metadata or object listing page.
const PAGE: usize = 100;

/// The host side of one run.
pub struct RunBroker {
    ctx: Arc<DriverContext>,
    /// Sends connection requests; the node's screened egress client unless a test replaces it.
    outbound: Arc<dyn Outbound>,
    /// The realm keys a management node releases group and realm secrets with.
    keys: ReleaseKeys,
    /// The secrets this run holds, reused while their value is unchanged.
    held: tokio::sync::Mutex<HeldSecrets>,
    /// The values handed to the code, removed from the text the run keeps.
    shown: Mutex<Vec<SecretFields>>,
    scope: RunScope,
    /// A call at a known ordinal came with other arguments than before.
    changed: AtomicBool,
    /// Guest log lines, bounded as the run keeps them.
    log: Mutex<PluginRun>,
}

impl RunBroker {
    pub fn new(ctx: Arc<DriverContext>, scope: RunScope) -> Self {
        let mut lines = scope.run.clone();
        lines.log.clear();
        Self {
            outbound: Arc::new(Egress(ctx.blob_handle.clone())),
            keys: (ctx.metadata_handle.as_ref())
                .map(|handle| ReleaseKeys {
                    seal: handle.realm_seal_key(),
                    vault: handle.realm_vault_key(),
                })
                .unwrap_or_default(),
            held: tokio::sync::Mutex::default(),
            shown: Mutex::default(),
            ctx,
            log: Mutex::new(lines),
            scope,
            changed: AtomicBool::new(false),
        }
    }

    /// The broker with other realm keys for releases.
    pub fn with_keys(mut self, keys: ReleaseKeys) -> Self {
        self.keys = keys;
        self
    }

    /// The broker with another sender for connection requests.
    pub fn with_outbound(mut self, outbound: Arc<dyn Outbound>) -> Self {
        self.outbound = outbound;
        self
    }

    /// The log lines the guest wrote, within the run's bounds.
    pub fn take_log(&self) -> Vec<LogLine> {
        let mut run = self
            .log
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut run.log)
    }

    /// Steps 0 to 2 and 5: the run may call now, within the install's call rate.
    async fn admitted(
        &self,
        target: Option<(Permission, &str)>,
    ) -> Result<PluginInstall, HostError> {
        let install = check_call(&self.ctx, &self.scope, target).await?;
        match take_call(install.install_id, std::time::Instant::now()) {
            true => Ok(install),
            false => Err(HostError::new(
                ErrorKind::Limit,
                "the install makes calls too fast",
            )),
        }
    }

    /// Whether the run called differently than an earlier attempt, so it fails as changed.
    pub fn changed(&self) -> bool {
        self.changed.load(Ordering::Relaxed)
    }

    /// Steps 0 to 4 for a target, and the account restricted to the matching grants.
    async fn checked(
        &self,
        permission: Permission,
        target: &str,
    ) -> Result<(PluginInstall, AuthContext), HostError> {
        let install = self.admitted(Some((permission, target))).await?;
        let patterns = matching(&install, permission, target)?;
        Ok((
            install.clone(),
            self.account(&install, &patterns, permission),
        ))
    }

    /// Steps 0 to 2, and the account restricted to every grant of `permission`.
    async fn listing(
        &self,
        permission: Permission,
    ) -> Result<(PluginInstall, AuthContext), HostError> {
        let install = self.admitted(None).await?;
        let grants = parse_grants(&install.grants);
        let patterns = grants.get(&permission).cloned().unwrap_or_default();
        Ok((
            install.clone(),
            self.account(&install, &patterns, permission),
        ))
    }

    /// The account restricted to `patterns`, at the access `permission` needs.
    fn account(
        &self,
        install: &PluginInstall,
        patterns: &BTreeSet<String>,
        permission: Permission,
    ) -> AuthContext {
        let access = match accesses(permission).contains(&Access::WRITE) {
            true => Access::WRITE,
            false => Access::READ,
        };
        let restrictions = (patterns.iter())
            .map(|pattern| PathRestriction {
                pattern: pattern.clone(),
                permission: access.clone(),
            })
            .collect();
        AuthContext {
            user_id: install.account,
            realm_id: self.scope.realm_id,
            path_restrictions: Some(restrictions),
            session: None,
        }
    }

    /// The registry record and JSON-LD of a document the account may read.
    async fn export(
        &self,
        (document_id, view): (Ulid, RoCrateExportView),
        auth: AuthContext,
        max_bytes: usize,
    ) -> Result<(MetadataRegistryRecord, String), HostError> {
        let request = ExportMetadataRequest {
            document_id,
            auth: Some(auth.clone()),
            view,
            limit: None,
            offset: None,
            after: None,
        };
        let token = Some(AuthToken::Internal(auth));
        let export = export_rocrate_routed(
            &self.ctx,
            self.scope.realm_id,
            request,
            token,
            max_bytes as u64,
        );
        match export.await.map_err(api_error)? {
            ExportMetadataResult::Full { record, jsonld }
            | ExportMetadataResult::Summary { record, jsonld } => Ok((record, jsonld)),
            _ => Err(unavailable("the document has no such export")),
        }
    }

    /// Runs a revision-checked change of one file entity under the write grants. A file it names
    /// must be readable through `objects.read`, as for a restricted REST writer.
    async fn file_write(
        &self,
        cx: &CallContext,
        change: &FileChange,
        mutation: UpdateDocumentMutation,
        file: Option<Value>,
    ) -> Result<String, HostError> {
        let document_id = parse_document(&change.resource)?;
        let expected = Ulid::from_string(&change.expected)
            .map_err(|_| HostError::new(ErrorKind::Invalid, "bad revision"))?;
        let (_, auth) = self.listing(Permission::MetadataWrite).await?;
        let read = (document_id, RoCrateExportView::Summary);
        let (record, _) = self.export(read, auth, cx.max_bytes).await?;
        let (install, auth) =
            (self.checked(Permission::MetadataWrite, &record.permission_path)).await?;
        if let Some(file) = file {
            self.objects_named(&install, &file).await?;
        }
        let run_id = self.scope.run.run_id;
        let earlier = written(&self.ctx, run_id, &change.intent).await;
        if !earlier.map_err(|error| unavailable(error.0))? {
            same_revision(record.last_event_id, expected)?;
        }
        if let Some(reply) = self.begun(&change.intent).await? {
            return Ok(reply);
        }
        let actor = Actor {
            node_id: self.scope.local,
            user_id: install.account,
            realm_id: self.scope.realm_id,
        };
        let message = format!(
            "plugin {} install {} run {run_id}",
            install.plugin, install.install_id
        );
        let update = route_metadata_update(
            &self.ctx,
            actor,
            Some(&record),
            document_id,
            None,
            mutation,
            Some(expected),
            Some(AuthToken::Internal(auth)),
            Some(message),
        );
        let outcome = update
            .await
            .map(|record| record.last_event_id.to_string())
            .map_err(write_outcome);
        self.record(&change.intent, outcome).await
    }

    /// The dataset entity of a document from the given export view, checked under the read grants.
    async fn dataset_read(
        &self,
        cx: &CallContext,
        resource: &str,
        view: RoCrateExportView,
    ) -> Result<Document, HostError> {
        let document_id = parse_document(resource)?;
        let (_, auth) = self.listing(Permission::MetadataRead).await?;
        let (record, jsonld) = self.export((document_id, view), auth, cx.max_bytes).await?;
        self.checked(Permission::MetadataRead, &record.permission_path)
            .await?;
        Ok(Document {
            resource: resource.to_string(),
            revision: record.last_event_id.to_string(),
            json: dataset_of(&jsonld)?,
        })
    }

    /// Creates the keyed document once: the stored create goes to its stored node with its stored
    /// arguments, and a key from an earlier call is looked up first, so a found one is adopted.
    async fn keyed_create(
        &self,
        (actor, auth): (Actor, &AuthContext),
        (install, key): (&PluginInstall, &str),
        minted: PinnedCreate,
    ) -> Result<String, Outcome> {
        let stored = claim_key(&self.ctx, (install, key), minted).await;
        let (pinned, claim) = stored.map_err(|error| Outcome::None(unavailable(error.0)))?;
        let mut lookup = None;
        loop {
            match next_step(claim, lookup) {
                Step::Dispatch => {
                    let extras = PolicyRequestExtras::operation("plugin.metadata.create");
                    let created = create_with_id(&self.ctx, actor, (auth, extras), &pinned).await;
                    let outcome = created.map_err(create_outcome);
                    // A known outcome closes the attempt; a refusal of the arguments lets a
                    // later call correct them. An unknown one keeps them fixed.
                    let rejected = match &outcome {
                        Ok(_) => Some(false),
                        Err(Outcome::None(error)) => Some(error.kind == ErrorKind::Invalid),
                        Err(Outcome::Maybe(_)) => None,
                    };
                    if let Some(rejected) = rejected {
                        self.settle((install, key), rejected).await;
                    }
                    let created = outcome?;
                    return Ok(created_text(
                        &created.document_id,
                        &created.establishing_event_id,
                    ));
                }
                Step::Lookup => lookup = Some(self.look_up(pinned.document_id, auth).await),
                Step::Adopt(revision) => {
                    self.settle((install, key), false).await;
                    return Ok(created_text(&pinned.document_id, &revision));
                }
                Step::Unknown => {
                    let reason = "the document of an earlier create with this key is not reachable";
                    return Err(Outcome::Maybe(reason.to_string()));
                }
            }
        }
    }

    /// Checks each Aruna object a file entity names through the plugin's own `objects.read` call
    /// check, grants and policies included, then that it lies in a bucket of the install's group
    /// here. Forms that name no single object here (content hashes, global ids) are refused.
    async fn objects_named(&self, install: &PluginInstall, file: &Value) -> Result<(), HostError> {
        let realm_id = self.scope.realm_id;
        // Every spelling the write converter stores as the id or as `contentUrl` is checked.
        let mut values: Vec<&str> = (["@id", "id"].iter())
            .filter_map(|key| file.get(*key)?.as_str())
            .collect();
        for (property, value) in file.as_object().into_iter().flatten() {
            if normalize_property(property) == CONTENT_URL {
                reference_values(value, &mut values);
            }
        }
        let refused = |what: &str| {
            let message = format!("{what} names no object a plugin may check; use its path");
            HostError::new(ErrorKind::Denied, message)
        };
        let group = install.group_id;
        for value in values {
            let identity = DataIdentity::read(value, &[]);
            let path = if value.starts_with(&format!("/{realm_id}/g/")) {
                value.to_string()
            } else if let Some(exact) = identity.exact {
                if exact.realm_id != realm_id {
                    return Err(refused(value));
                }
                let (bucket, key) = (&exact.bucket, &exact.key);
                object_permission_path(realm_id, group, exact.node_id, bucket, key)
            } else if let Some(at) = identity.location {
                object_permission_path(realm_id, group, self.scope.local, &at.bucket, &at.key)
            } else if identity.is_aruna() {
                return Err(refused(value));
            } else {
                continue;
            };
            self.checked(Permission::ObjectsRead, &path).await?;
            self.object_at(install, &path).await?;
        }
        Ok(())
    }

    /// Closes an attempt of a create key. A failure only keeps the key's arguments fixed.
    async fn settle(&self, key: (&PluginInstall, &str), rejected: bool) {
        if let Err(error) = settle_key(&self.ctx, key, rejected).await {
            warn!(%error, "A plugin create key could not record its attempt's outcome");
        }
    }

    /// Looks a keyed create's document up on its holders under the account's authority.
    async fn look_up(&self, document_id: Ulid, auth: &AuthContext) -> Lookup {
        let request = GetVisibleRequest {
            document_id,
            auth: Some(auth.clone()),
        };
        let token = Some(AuthToken::Internal(auth.clone()));
        match get_metadata_routed(&self.ctx, self.scope.realm_id, request, token).await {
            Ok(record) => Lookup::Found(record.establishing_event_id),
            Err(MetadataApiError::NotFound) => Lookup::Missing,
            Err(_) => Lookup::Unreachable,
        }
    }

    /// Runs a fresh effect's outcome into its intent: a reply settles it, a refusal that had
    /// no effect clears it, and an error that may have committed leaves it unknown.
    async fn record(
        &self,
        intent: &Intent,
        outcome: Result<String, Outcome>,
    ) -> Result<String, HostError> {
        let run_id = self.scope.run.run_id;
        match outcome {
            Ok(reply) => {
                let settle =
                    async |txn: &Txn<'_>| settle_in(txn, run_id, intent, reply.clone()).await;
                match transact(&self.ctx, settle).await {
                    Ok(()) => Ok(reply),
                    Err(error) => Err(self.unknown(intent, &error.0).await),
                }
            }
            Err(Outcome::None(error)) => {
                let clear = async |txn: &Txn<'_>| clear_in(txn, run_id, intent).await;
                transact::<(), StoreError>(&self.ctx, clear)
                    .await
                    .map_err(|error| unavailable(error.0))?;
                Err(error)
            }
            Err(Outcome::Maybe(reason)) => Err(self.unknown(intent, &reason).await),
        }
    }

    async fn unknown(&self, intent: &Intent, reason: &str) -> HostError {
        let _ = mark_unknown(&self.ctx, self.scope.run.run_id, intent, reason).await;
        HostError::new(ErrorKind::UnknownOutcome, reason)
    }

    /// The intent's earlier outcome, or `None` when the effect may be dispatched now.
    async fn begun(&self, intent: &Intent) -> Result<Option<String>, HostError> {
        let begun = begin(&self.ctx, self.scope.run.run_id, intent).await;
        match begun.map_err(|error| unavailable(error.0))? {
            Begun::Fresh => Ok(None),
            Begun::Settled(reply) => Ok(Some(reply)),
            Begun::Changed => {
                self.changed.store(true, Ordering::Relaxed);
                Err(HostError::new(
                    ErrorKind::Conflict,
                    "the call differs from an earlier attempt",
                ))
            }
            Begun::Unknown(reason) => Err(HostError::new(ErrorKind::UnknownOutcome, reason)),
        }
    }

    /// The local bucket and key of an object path below the install's group data.
    async fn object_at(
        &self,
        install: &PluginInstall,
        path: &str,
    ) -> Result<(String, String), HostError> {
        let invalid =
            || HostError::new(ErrorKind::Invalid, format!("{path} is not an object path"));
        let root = format!("/{}/g/{}/data/", self.scope.realm_id, install.group_id);
        let rest = path.strip_prefix(&root).ok_or_else(invalid)?;
        let mut parts = rest.splitn(3, '/');
        let (node, bucket) = (
            parts.next().unwrap_or_default(),
            parts.next().ok_or_else(invalid)?,
        );
        let key = parts.next().unwrap_or_default().to_string();
        let node = NodeId::from_str(node).map_err(|_| invalid())?;
        if node != self.scope.local {
            return Err(unavailable(
                "objects of another node are not reachable from plugins yet",
            ));
        }
        let info = drive(GetBucketOperation::new(bucket.to_string()), &self.ctx).await;
        match info {
            Ok(info) if info.group_id == install.group_id => Ok((bucket.to_string(), key)),
            _ => Err(HostError::new(
                ErrorKind::NotFound,
                format!("no bucket at {path}"),
            )),
        }
    }
}

impl RunBroker {
    /// Whether the account may reach a listed target now: the grants must cover it and it must
    /// pass the same authorization as an individual call. Only an unavailable check fails.
    async fn authorized(
        &self,
        install: &PluginInstall,
        permission: Permission,
        target: &str,
    ) -> Result<bool, HostError> {
        let Ok(patterns) = matching(install, permission, target) else {
            return Ok(false);
        };
        match authorize_call(&self.ctx, install, permission, target, &patterns).await {
            Ok(()) => Ok(true),
            Err(error) if error.kind == ErrorKind::Unavailable => Err(error),
            Err(_) => Ok(false),
        }
    }

    /// The listed objects of a local bucket the account may read, each named by the permission
    /// path of its own bucket and key, never by the text of the listing prefix.
    async fn visible(
        &self,
        install: &PluginInstall,
        bucket: &str,
        objects: Vec<(String, u64, String)>,
    ) -> Result<Vec<ObjectInfo>, HostError> {
        let (realm, group, node) = (self.scope.realm_id, install.group_id, self.scope.local);
        let mut visible = Vec::new();
        for (key, size, version) in objects {
            let path = object_permission_path(realm, group, node, bucket, &key);
            if self
                .authorized(install, Permission::ObjectsRead, &path)
                .await?
            {
                visible.push(ObjectInfo {
                    path,
                    size,
                    version,
                });
            }
        }
        Ok(visible)
    }
}

/// How a mutating call ended without a reply.
enum Outcome {
    /// It had no effect.
    None(HostError),
    /// It may have had its effect.
    Maybe(String),
}

#[async_trait]
impl Broker for RunBroker {
    async fn granted(&self, _cx: &CallContext, permission: Permission, resource: &str) -> bool {
        self.checked(permission, resource).await.is_ok()
    }

    async fn metadata_get(&self, cx: &CallContext, resource: &str) -> Result<Document, HostError> {
        self.dataset_read(cx, resource, RoCrateExportView::Full)
            .await
    }

    async fn metadata_summary(
        &self,
        cx: &CallContext,
        resource: &str,
    ) -> Result<Document, HostError> {
        self.dataset_read(cx, resource, RoCrateExportView::Summary)
            .await
    }

    async fn metadata_files(
        &self,
        cx: &CallContext,
        resource: &str,
        cursor: Option<String>,
    ) -> Result<FilePage, HostError> {
        let document_id = parse_document(resource)?;
        let (_, auth) = self.listing(Permission::MetadataRead).await?;
        let request = ExportMetadataRequest {
            document_id,
            auth: Some(auth.clone()),
            view: RoCrateExportView::Page,
            limit: Some(FILE_PAGE),
            offset: None,
            after: cursor,
        };
        // The holder counts the cursor and the room around each entity in this bound.
        let bound = cx.max_bytes as u64;
        let token = Some(AuthToken::Internal(auth));
        let export = export_rocrate_routed(&self.ctx, self.scope.realm_id, request, token, bound);
        let ExportMetadataResult::Page { record, page } = export.await.map_err(api_error)? else {
            return Err(unavailable("the document has no page export"));
        };
        self.checked(Permission::MetadataRead, &record.permission_path)
            .await?;
        Ok(FilePage {
            files: file_entities(&page.jsonld)?,
            cursor: page.next_cursor,
        })
    }

    async fn metadata_list(
        &self,
        _cx: &CallContext,
        cursor: Option<String>,
    ) -> Result<Listing, HostError> {
        let (install, auth) = self.listing(Permission::MetadataRead).await?;
        let offset = match cursor {
            Some(cursor) => cursor
                .parse::<usize>()
                .map_err(|_| HostError::new(ErrorKind::Invalid, "bad cursor"))?,
            None => 0,
        };
        let request = ListVisibleRequest {
            group_id: Some(install.group_id),
            path_prefix: None,
            include_summary: false,
            limit: Some(PAGE),
            offset: Some(offset),
            order: MetadataListOrder::Created,
            auth: Some(auth),
        };
        let listed = list_visible_documents(&self.ctx, self.scope.realm_id, request).await;
        let listed = listed.map_err(api_error)?;
        let full = listed.documents.len() >= PAGE;
        let mut resources = Vec::new();
        for document in &listed.documents {
            let path = &document.record.permission_path;
            if self
                .authorized(&install, Permission::MetadataRead, path)
                .await?
            {
                resources.push(document.record.document_id.to_string());
            }
        }
        let cursor = full.then(|| (offset + listed.documents.len()).to_string());
        Ok(Listing { resources, cursor })
    }

    async fn metadata_patch(
        &self,
        cx: &CallContext,
        patch: MetadataPatch,
    ) -> Result<String, HostError> {
        let document_id = parse_document(&patch.resource)?;
        let expected = Ulid::from_string(&patch.expected)
            .map_err(|_| HostError::new(ErrorKind::Invalid, "bad revision"))?;
        // The target is resolved under the write grants; the plugin never sees what is read.
        let (_, auth) = self.listing(Permission::MetadataWrite).await?;
        // The summary has no file links, so a large dataset is not exported whole for a patch.
        let read = (document_id, RoCrateExportView::Summary);
        let (record, jsonld) = self.export(read, auth, cx.max_bytes).await?;
        let path = &record.permission_path;
        let (install, auth) = self.checked(Permission::MetadataWrite, path).await?;
        // A new call starts only from the copy it expects; an earlier attempt's intent decides.
        let run_id = self.scope.run.run_id;
        let earlier = written(&self.ctx, run_id, &patch.intent).await;
        if !earlier.map_err(|error| unavailable(error.0))? {
            same_revision(record.last_event_id, expected)?;
        }
        // An invalid patch is refused before its intent exists, so it leaves no uncertainty.
        let change = dataset_change(&jsonld, &patch.patch)?;
        if let Some(reply) = self.begun(&patch.intent).await? {
            return Ok(reply);
        }
        let actor = Actor {
            node_id: self.scope.local,
            user_id: install.account,
            realm_id: self.scope.realm_id,
        };
        let run = self.scope.run.run_id;
        let message = format!(
            "plugin {} install {} run {run}",
            install.plugin, install.install_id
        );
        let mutation = UpdateDocumentMutation::UpsertContextualEntity { jsonld: change };
        let update = route_metadata_update(
            &self.ctx,
            actor,
            Some(&record),
            document_id,
            None,
            mutation,
            Some(expected),
            Some(AuthToken::Internal(auth)),
            Some(message),
        );
        let outcome = update
            .await
            .map(|record| record.last_event_id.to_string())
            .map_err(write_outcome);
        self.record(&patch.intent, outcome).await
    }

    async fn metadata_create(
        &self,
        _cx: &CallContext,
        create: MetadataCreate,
    ) -> Result<Created, HostError> {
        let invalid = |message: &str| HostError::new(ErrorKind::Invalid, message);
        let path = MetadataRegistryRecord::normalize_document_path(&create.path);
        if path.is_empty() || path.contains('@') {
            return Err(invalid("a dataset path is not empty and has no @"));
        }
        if create.key.is_empty() || create.key.len() > MAX_KEY {
            return Err(invalid("a create key has 1 to 256 bytes"));
        }
        let current = self.admitted(None).await?;
        let realm_id = self.scope.realm_id;
        let target = format!("/{realm_id}/g/{}/meta/{path}", current.group_id);
        let (install, auth) = self.checked(Permission::MetadataCreate, &target).await?;
        // An invalid dataset is refused before its intent exists, so it leaves no uncertainty.
        let jsonld = dataset_crate(&create.dataset)?;
        if let Some(reply) = self.begun(&create.intent).await? {
            return stored_created(&reply);
        }
        let actor = Actor {
            node_id: self.scope.local,
            user_id: install.account,
            realm_id,
        };
        let run = self.scope.run.run_id;
        let message = format!(
            "plugin {} install {} run {run}",
            install.plugin, install.install_id
        );
        let payload = (CreateDocumentPayload::RoCrate { jsonld }, Some(message));
        let minted =
            PinnedCreate::mint(&self.ctx, &actor, (install.group_id, &path), payload).await;
        let outcome = match minted {
            Ok(minted) => {
                let key = (&install, create.key.as_str());
                self.keyed_create((actor, &auth), key, minted).await
            }
            Err(error) => Err(Outcome::None(unavailable(error.to_string()))),
        };
        let reply = self.record(&create.intent, outcome).await?;
        stored_created(&reply)
    }

    async fn metadata_put_file(
        &self,
        cx: &CallContext,
        change: FileChange,
    ) -> Result<String, HostError> {
        let file = file_entity(&change.file)?;
        let mutation = UpdateDocumentMutation::ReplaceDataEntity {
            jsonld: file.clone(),
        };
        let file: Value = serde_json::from_str(&file).unwrap_or(Value::Null);
        self.file_write(cx, &change, mutation, Some(file)).await
    }

    async fn objects_info(&self, _cx: &CallContext, path: &str) -> Result<ObjectInfo, HostError> {
        let (install, _) = self.checked(Permission::ObjectsRead, path).await?;
        let (bucket, key) = self.object_at(&install, path).await?;
        let head = HeadObjectInput {
            bucket,
            key,
            version_id: None,
        };
        let head = drive(HeadObjectOperation::new(head), &self.ctx).await;
        let head =
            head.map_err(|_| HostError::new(ErrorKind::NotFound, format!("no object at {path}")))?;
        Ok(ObjectInfo {
            path: path.to_string(),
            size: head.location.map_or(0, |location| location.blob_size),
            version: head
                .resolved_version_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
        })
    }

    async fn objects_list(
        &self,
        _cx: &CallContext,
        prefix: &str,
        cursor: Option<String>,
    ) -> Result<ObjectPage, HostError> {
        let (install, _) = self.listing(Permission::ObjectsRead).await?;
        let (bucket, key) = self.object_at(&install, prefix).await?;
        let invalid = || HostError::new(ErrorKind::Invalid, "bad cursor");
        let token = match cursor {
            Some(cursor) => Some(
                ListContinuationToken::from_bytes(&hex::decode(cursor).map_err(|_| invalid())?)
                    .map_err(|_| invalid())?,
            ),
            None => None,
        };
        let input = ListBucketInput {
            bucket: bucket.clone(),
            group_id: install.group_id,
            continuation_token: token,
            max_keys: Some(PAGE),
            prefix: (!key.is_empty()).then_some(key),
            delimiter: None,
            start_after: None,
        };
        let listed = drive(ListBucketOperation::new(input), &self.ctx).await;
        let listed = listed.map_err(|error| unavailable(error.to_string()))?;
        let listed_objects = (listed.objects.into_iter())
            .map(|object| {
                let size = object.location.map_or(0, |location| location.blob_size);
                (object.head.key, size, object.version_id.to_string())
            })
            .collect();
        let objects = self.visible(&install, &bucket, listed_objects).await?;
        let cursor = match listed.continuation_token {
            Some(token) => Some(hex::encode(
                token
                    .to_bytes()
                    .map_err(|error| unavailable(error.to_string()))?,
            )),
            None => None,
        };
        Ok(ObjectPage { objects, cursor })
    }

    async fn connection_send(
        &self,
        cx: &CallContext,
        request: SendRequest,
    ) -> Result<Response, HostError> {
        let mut ready = self.ready(request).await?;
        // A whole answer is only for `send-full`; here the code reads fields at most.
        if ready.exposure == ResponseExposure::Full {
            ready.exposure = ResponseExposure::Status;
        }
        let Ready {
            outgoing,
            exposure,
            fields,
            body,
            intent,
            ..
        } = ready;
        let Some(intent) = intent else {
            let answer = self.outbound.send(outgoing, body).await;
            let answer = answer.map_err(send_refusal)?;
            return Ok(exposed(&exposure, &fields, &answer));
        };
        if let Some(reply) = self.begun(&intent).await? {
            return Ok(kept(stored_reply(&reply)?, &exposure));
        }
        // A sent request's answer that does not fit is stored as a marker before it is refused,
        // so a rerun answers the same refusal and never sends again.
        let outcome = match self.outbound.send(outgoing, body).await {
            Ok(answer) => {
                let shown = exposed(&exposure, &fields, &answer);
                let size = shown.json.as_ref().map_or(0, String::len) + 2;
                Ok(match size > cx.max_bytes {
                    true => too_large(answer.status),
                    false => reply_text(&shown),
                })
            }
            Err(SendError::TooLarge { status }) => Ok(too_large(status)),
            Err(SendError::NotSent(why)) => Err(Outcome::None(unavailable(why))),
            Err(SendError::Unknown(why)) => Err(Outcome::Maybe(why)),
        };
        stored_reply(&self.record(&intent, outcome).await?)
    }

    async fn connection_send_full(
        &self,
        cx: &CallContext,
        request: SendRequest,
    ) -> Result<FullResponse, HostError> {
        let ready = self.ready(request).await?;
        // The manifest reads whole answers, and the secret's current rule exposes them.
        if !ready.whole || ready.exposure != ResponseExposure::Full {
            let narrower = "this connection does not expose its whole answer";
            return Err(HostError::new(ErrorKind::Denied, narrower));
        }
        let Some(intent) = ready.intent else {
            let answer = self.outbound.send(ready.outgoing, ready.body).await;
            let answer = answer.map_err(send_refusal)?;
            return Ok(whole(&ready.fields, &answer));
        };
        if let Some(reply) = self.begun(&intent).await? {
            return stored_whole(&reply);
        }
        let outcome = match self.outbound.send(ready.outgoing, ready.body).await {
            Ok(answer) => {
                let shown = whole(&ready.fields, &answer);
                let head: usize = (shown.headers.iter())
                    .map(|field| field.name.len() + field.value.len())
                    .sum();
                Ok(match head + shown.body.len() + 2 > cx.max_bytes {
                    true => too_large(answer.status),
                    false => whole_text(&shown),
                })
            }
            Err(SendError::TooLarge { status }) => Ok(too_large(status)),
            Err(SendError::NotSent(why)) => Err(Outcome::None(unavailable(why))),
            Err(SendError::Unknown(why)) => Err(Outcome::Maybe(why)),
        };
        stored_whole(&self.record(&intent, outcome).await?)
    }

    async fn state_get(&self, _cx: &CallContext, key: &str) -> Result<Option<Entry>, HostError> {
        check_key(key)?;
        let install = self.admitted(None).await?;
        let entry = read_state(&self.ctx, install.install_id, key).await;
        entry.map_err(|error| unavailable(error.0))
    }

    async fn state_put(&self, _cx: &CallContext, put: StatePut) -> Result<u64, HostError> {
        check_key(&put.key)?;
        let install = self.admitted(None).await?;
        if let Some(reply) = self.begun(&put.intent).await? {
            return reply
                .parse()
                .map_err(|_| unavailable("a stored reply is not a version"));
        }
        let run_id = self.scope.run.run_id;
        match put_state(&self.ctx, run_id, install.install_id, &put).await {
            Ok(version) => Ok(version),
            Err(PutError::Refused(message, kind)) => {
                let clear = async |txn: &Txn<'_>| clear_in(txn, run_id, &put.intent).await;
                transact::<(), StoreError>(&self.ctx, clear)
                    .await
                    .map_err(|error| unavailable(error.0))?;
                Err(HostError::new(kind, message))
            }
            Err(PutError::Store(error)) => Err(self.unknown(&put.intent, &error.0).await),
        }
    }

    async fn secret_value(&self, _cx: &CallContext, slot: &str) -> Result<Vec<Field>, HostError> {
        let current = self.admitted(None).await?;
        let path = install_scope(&current).secret(slot);
        let (install, _) = self.checked(Permission::SecretsValue, &path).await?;
        let config = read_config(&self.ctx, self.scope.realm_id).await;
        let config = config.map_err(|error| unavailable(error.to_string()))?;
        let use_of = (Delivery::Value, None);
        let (_, fields) = self.obtain(&install, slot, use_of, &config).await?;
        let values = (fields.names())
            .filter_map(|name| {
                let value = fields.get(name)?.to_string();
                let name = name.to_string();
                Some(Field { name, value })
            })
            .collect();
        let shown = self.shown.lock();
        (shown.unwrap_or_else(|poisoned| poisoned.into_inner())).push(fields);
        Ok(values)
    }

    async fn log(&self, _cx: &CallContext, level: &str, message: &str) {
        let message = &self.scrub(message);
        if self.admitted(None).await.is_ok() {
            let mut run = self
                .log
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            run.push_log(LogLine {
                at_ms: unix_timestamp_millis(),
                level: level.to_string(),
                message: message.to_string(),
            });
        }
    }

    async fn abandoned(&self, _cx: &CallContext, intent: &Intent) {
        let reason = "the call did not finish in time";
        let _ = mark_unknown(&self.ctx, self.scope.run.run_id, intent, reason).await;
    }
}

/// The type a JSON literal comes back with from the stored graph.
const JSON_LITERAL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON";

/// The dataset entity of a crate, the one its metadata descriptor is about.
fn dataset_mut(crate_json: &mut Value) -> Result<&mut Value, HostError> {
    let graph = (crate_json.get_mut("@graph").and_then(Value::as_array_mut))
        .ok_or_else(|| unavailable("the stored crate has no graph"))?;
    let id_of = |entity: &Value| {
        entity
            .get("@id")
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    let root = (graph.iter())
        .find(|entity| id_of(entity).as_deref() == Some("ro-crate-metadata.json"))
        .and_then(|descriptor| {
            descriptor
                .get("about")?
                .get("@id")?
                .as_str()
                .map(str::to_string)
        })
        .unwrap_or_else(|| "./".to_string());
    (graph.iter_mut())
        .find(|entity| id_of(entity).as_deref() == Some(root.as_str()))
        .ok_or_else(|| HostError::new(ErrorKind::Invalid, "the crate has no dataset entity"))
}

/// The fields of a crate's dataset entity, as a plugin reads and patches them.
pub(crate) fn dataset_of(jsonld: &str) -> Result<String, HostError> {
    let mut crate_json: Value =
        serde_json::from_str(jsonld).map_err(|_| unavailable("the stored crate is not JSON"))?;
    let dataset = dataset_mut(&mut crate_json)?;
    plain_objects(dataset);
    Ok(dataset.to_string())
}

/// The change a patch makes to a crate's dataset entity: its id, types and name, and each
/// patched field with its value after a JSON merge patch, `null` removing one. Other fields and
/// other entities are not part of it, so they stay as they are.
pub(crate) fn dataset_change(jsonld: &str, patch: &str) -> Result<String, HostError> {
    let invalid = |message: &str| HostError::new(ErrorKind::Invalid, message);
    let patch: Value = serde_json::from_str(patch).map_err(|_| invalid("the patch is not JSON"))?;
    let Value::Object(changes) = patch else {
        return Err(invalid("the patch is not a JSON object"));
    };
    if changes.contains_key("@id") {
        return Err(invalid("a patch cannot change the dataset's @id"));
    }
    let mut crate_json: Value =
        serde_json::from_str(jsonld).map_err(|_| unavailable("the stored crate is not JSON"))?;
    let dataset = dataset_mut(&mut crate_json)?;
    plain_objects(dataset);
    let mut merged = dataset.clone();
    let fields: Vec<String> = changes.keys().cloned().collect();
    merge_into(&mut merged, Value::Object(changes));
    let mut change = serde_json::Map::new();
    for field in ["@id", "@type", "name"]
        .map(str::to_string)
        .into_iter()
        .chain(fields)
    {
        let value = merged.get(&field).cloned().unwrap_or(Value::Null);
        change.insert(field, value);
    }
    let mut change = Value::Object(change);
    literal_objects(&mut change);
    Ok(change.to_string())
}

/// The strings a property value names: itself, each entry, or an object's `@id` or `@value`.
fn reference_values<'a>(value: &'a Value, values: &mut Vec<&'a str>) {
    match value {
        Value::String(text) => values.push(text),
        Value::Array(entries) => entries
            .iter()
            .for_each(|entry| reference_values(entry, values)),
        Value::Object(fields) => {
            let named = ["@id", "id", "@value"]
                .iter()
                .filter_map(|key| fields.get(*key));
            named.for_each(|entry| reference_values(entry, values));
        }
        _ => {}
    }
}

/// File entities one page asks for; a page over the byte bound comes back smaller.
const FILE_PAGE: usize = 100;

/// The `File` entities of an exported page, each as the plain JSON object a plugin writes.
fn file_entities(jsonld: &str) -> Result<Vec<String>, HostError> {
    let page: Value =
        serde_json::from_str(jsonld).map_err(|_| unavailable("the stored page is not JSON"))?;
    let graph = (page.get("@graph").and_then(Value::as_array))
        .ok_or_else(|| unavailable("the stored page has no graph"))?;
    // RO-Crate's `File` is `schema:MediaObject`; an export may name it by either term.
    let named = |kind: &Value| kind == "File" || kind == "MediaObject";
    let file = |entity: &&Value| match entity.get("@type") {
        Some(Value::Array(kinds)) => kinds.iter().any(named),
        Some(kind) => named(kind),
        None => false,
    };
    let files = graph.iter().filter(file).cloned().map(|mut entity| {
        plain_objects(&mut entity);
        entity.to_string()
    });
    Ok(files.collect())
}

/// A file entity as a change stores it: one object with an `@id` that is neither the root nor
/// the descriptor, typed `File`, naming other entities by `@id` alone.
fn file_entity(file: &str) -> Result<String, HostError> {
    let invalid = |message: &str| HostError::new(ErrorKind::Invalid, message);
    let file: Value = serde_json::from_str(file).map_err(|_| invalid("the file is not JSON"))?;
    let Value::Object(fields) = &file else {
        return Err(invalid("the file is not a JSON object"));
    };
    let id = fields
        .get("@id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if matches!(id, "" | "./" | "ro-crate-metadata.json") {
        return Err(invalid("the file needs its own @id"));
    }
    let named = |kind: &Value| kind == "File" || kind == "MediaObject";
    let typed = match fields.get("@type") {
        Some(Value::Array(kinds)) => kinds.iter().any(named),
        Some(kind) => named(kind),
        None => false,
    };
    if !typed || fields.contains_key("@graph") || nested_entity(fields) {
        return Err(invalid("the file is one File entity naming others by @id"));
    }
    let mut file = file;
    literal_objects(&mut file);
    Ok(file.to_string())
}

/// Whether a field holds an entity of its own rather than a reference by `@id` alone.
fn nested_entity(fields: &serde_json::Map<String, Value>) -> bool {
    let entity = |value: &Value| {
        (value.as_object()).is_some_and(|object| {
            let reference = object.len() == 1 && object.contains_key("@id");
            object.keys().any(|key| key.starts_with('@')) && !reference
        })
    };
    fields.values().any(|value| match value {
        Value::Array(entries) => entries.iter().any(entity),
        value => entity(value),
    })
}

/// The crate a plugin create stores: the metadata descriptor and the dataset entity as its root.
/// Plain object values become JSON literals as in a patch; nested entities are refused.
pub(crate) fn dataset_crate(dataset: &str) -> Result<String, HostError> {
    let invalid = |message: &str| HostError::new(ErrorKind::Invalid, message);
    let dataset: Value =
        serde_json::from_str(dataset).map_err(|_| invalid("the dataset is not JSON"))?;
    let Value::Object(mut fields) = dataset else {
        return Err(invalid("the dataset is not a JSON object"));
    };
    if fields.get("@id").is_some_and(|id| id != "./") || fields.contains_key("@graph") {
        return Err(invalid("the dataset is one root entity without @graph"));
    }
    let kinds = fields
        .get("@type")
        .cloned()
        .unwrap_or(Value::from("Dataset"));
    let dataset_kind = match &kinds {
        Value::String(kind) => kind == "Dataset",
        Value::Array(kinds) => kinds.iter().any(|kind| kind == "Dataset"),
        _ => false,
    };
    if !dataset_kind {
        return Err(invalid("the dataset's @type is not Dataset"));
    }
    if nested_entity(&fields) {
        return Err(invalid("the dataset names other entities only by @id"));
    }
    fields.insert("@id".to_string(), Value::from("./"));
    fields.insert("@type".to_string(), kinds);
    let mut dataset = Value::Object(fields);
    literal_objects(&mut dataset);
    let descriptor = serde_json::json!({
        "@id": "ro-crate-metadata.json",
        "@type": "CreativeWork",
        "about": {"@id": "./"},
        "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
    });
    let graph = serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [descriptor, dataset],
    });
    let graph = graph.to_string();
    craqle::validate_rocrate_jsonld(&graph).map_err(|error| invalid(&error.to_string()))?;
    Ok(graph)
}

/// Bytes of a create key at most.
const MAX_KEY: usize = 256;

/// The intent reply of a create: the document id and its establishing revision.
fn created_text(document_id: &Ulid, revision: &Ulid) -> String {
    format!("{document_id} {revision}")
}

fn stored_created(reply: &str) -> Result<Created, HostError> {
    let (resource, revision) = (reply.split_once(' '))
        .ok_or_else(|| unavailable("the stored create reply is not readable"))?;
    Ok(Created {
        resource: resource.to_string(),
        revision: revision.to_string(),
    })
}

/// Whether a failed create had no effect, or may have had one.
fn create_outcome(error: CreateAuthorizedError) -> Outcome {
    let none = |kind, message: String| Outcome::None(HostError::new(kind, message));
    match error {
        CreateAuthorizedError::EmptyPath => none(ErrorKind::Invalid, error.to_string()),
        CreateAuthorizedError::Forbidden
        | CreateAuthorizedError::Authorize(
            AuthorizeError::PermissionDenied | AuthorizeError::Policy(_),
        ) => none(ErrorKind::Denied, error.to_string()),
        CreateAuthorizedError::Authorize(_) | CreateAuthorizedError::Api(_) => {
            Outcome::None(unavailable(error.to_string()))
        }
        CreateAuthorizedError::Create(CreateDocumentError::MetadataError(
            MetadataError::ProfileValidation(_)
            | MetadataError::Validation(_)
            | MetadataError::InvalidInput(_),
        )) => none(ErrorKind::Invalid, error.to_string()),
        CreateAuthorizedError::Create(CreateDocumentError::HoldsNoBucket) => {
            Outcome::None(unavailable(error.to_string()))
        }
        CreateAuthorizedError::Create(_) => Outcome::Maybe(error.to_string()),
        CreateAuthorizedError::Write(error) => write_outcome(error),
    }
}

/// Turns the dataset's JSON literals back into the objects a plugin wrote.
fn plain_objects(dataset: &mut Value) {
    let Value::Object(fields) = dataset else {
        return;
    };
    for value in fields.values_mut() {
        let literal = (value.get("@type").and_then(Value::as_str))
            .is_some_and(|kind| kind == "@json" || kind == JSON_LITERAL);
        let inner = value.get("@value").cloned();
        if let (true, Some(inner)) = (literal, inner) {
            *value = match inner {
                Value::String(text) => serde_json::from_str(&text).unwrap_or(Value::String(text)),
                other => other,
            };
        }
    }
}

/// Applies a JSON merge patch to `target`.
fn merge_into(target: &mut Value, patch: Value) {
    let Value::Object(changes) = patch else {
        *target = patch;
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Default::default());
    }
    let Value::Object(fields) = target else {
        return;
    };
    for (key, value) in changes {
        match value {
            Value::Null => drop(fields.remove(&key)),
            value => merge_into(fields.entry(key).or_insert(Value::Null), value),
        }
    }
}

/// Stores each plain object value of the change, one without JSON-LD keywords, as a JSON
/// literal, so it stays one value instead of becoming an untyped node.
fn literal_objects(change: &mut Value) {
    let Value::Object(fields) = change else {
        return;
    };
    let plain = |value: &Value| {
        (value.as_object()).is_some_and(|object| !object.keys().any(|key| key.starts_with('@')))
    };
    let literal =
        |value: &Value| serde_json::json!({ "@type": JSON_LITERAL, "@value": value.to_string() });
    for value in fields.values_mut() {
        match value {
            Value::Array(entries) => {
                for entry in entries.iter_mut().filter(|entry| plain(entry)) {
                    *entry = literal(entry);
                }
            }
            entry if plain(entry) => *entry = literal(entry),
            _ => {}
        }
    }
}

/// Refuses a copy at another revision than the patch expects: one behind it is retryable,
/// one ahead of it is a conflict the plugin resolves by reading again.
fn same_revision(exported: Ulid, expected: Ulid) -> Result<(), HostError> {
    match exported.cmp(&expected) {
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Less => Err(unavailable(format!(
            "the copy read is at {exported}, before the expected {expected}; try again"
        ))),
        std::cmp::Ordering::Greater => Err(HostError::new(
            ErrorKind::Conflict,
            format!("the document is at {exported}, not at the expected {expected}"),
        )),
    }
}

fn parse_document(resource: &str) -> Result<Ulid, HostError> {
    Ulid::from_string(resource).map_err(|_| {
        HostError::new(
            ErrorKind::Invalid,
            format!("{resource} is not a document id"),
        )
    })
}

/// A read error as the guest sees it; an unreachable or not yet visible document is retryable.
fn api_error(error: MetadataApiError) -> HostError {
    match error {
        MetadataApiError::NotFound => HostError::new(ErrorKind::NotFound, "no such document"),
        MetadataApiError::TooLarge => {
            HostError::new(ErrorKind::Limit, "one entity is larger than a reply may be")
        }
        MetadataApiError::Unauthorized | MetadataApiError::Forbidden => {
            HostError::new(ErrorKind::Denied, "the account may not read this")
        }
        MetadataApiError::BadRequest
        | MetadataApiError::InvalidCursor(_)
        | MetadataApiError::InvalidQuery(_) => {
            HostError::new(ErrorKind::Invalid, error.to_string())
        }
        other => unavailable(other.to_string()),
    }
}

/// Whether a failed write had no effect, or may have had one.
fn write_outcome(error: MetadataWriteError) -> Outcome {
    let none = |kind, message: String| Outcome::None(HostError::new(kind, message));
    match error {
        MetadataWriteError::Unauthorized | MetadataWriteError::Forbidden => {
            none(ErrorKind::Denied, error.to_string())
        }
        MetadataWriteError::NotFound => none(ErrorKind::NotFound, error.to_string()),
        MetadataWriteError::Update(UpdateDocumentError::RevisionConflict { .. }) => {
            none(ErrorKind::Conflict, error.to_string())
        }
        MetadataWriteError::Update(UpdateDocumentError::StorageError(_))
        | MetadataWriteError::Undeliverable(_) => Outcome::Maybe(error.to_string()),
        other => none(ErrorKind::Invalid, other.to_string()),
    }
}

/// A checked connection request: the outgoing request with any secret added, the exposure of
/// its answer, the secret's fields for echo removal, and whether the manifest reads it whole.
struct Ready {
    outgoing: OutgoingRequest,
    exposure: ResponseExposure,
    fields: SecretFields,
    body: Vec<u8>,
    intent: Option<Intent>,
    whole: bool,
}

impl RunBroker {
    /// The per call check, the address below the connection, the guest's own headers, and the
    /// secret released and added; every use of a secret, a replay included, passes the check.
    async fn ready(&self, request: SendRequest) -> Result<Ready, HostError> {
        let current = self.admitted(None).await?;
        let path = install_scope(&current).connection(&request.connection);
        let (install, _) = self.checked(Permission::ConnectionsSend, &path).await?;
        let invalid = |why: &str| HostError::new(ErrorKind::Invalid, why);
        let config = read_config(&self.ctx, self.scope.realm_id).await;
        let config = config.map_err(|error| unavailable(error.to_string()))?;
        let plugin = pick_version(&config, &install.plugin, Some(&install.digest));
        let manifest = plugin.and_then(|plugin| read_manifest(&plugin));
        let manifest = manifest.map_err(|error| unavailable(error.to_string()))?;
        let connection = manifest.connections.get(&request.connection);
        let connection =
            connection.ok_or_else(|| invalid("the manifest has no such connection"))?;
        let method = declared_method(request.method);
        if !connection.methods.contains(&method) {
            return Err(invalid("the connection does not allow this method"));
        }
        if !relative_path(&request.path) {
            return Err(invalid("the path must stay below the connection's address"));
        }
        // The guest built its request from the settings saved at admission; the address follows
        // them, while the grant and release checks use the current install.
        let saved = &self.scope.run.settings;
        let settings: Value = serde_json::from_str(saved).unwrap_or(Value::Null);
        let mut url = connection.base(&settings).map_err(invalid)?;
        let joined = format!("{}/{}", url.path().trim_end_matches('/'), request.path);
        url.set_path(&joined);
        if !request.query.is_empty() {
            let pairs = request
                .query
                .iter()
                .map(|field| (&field.name, &field.value));
            url.query_pairs_mut().extend_pairs(pairs);
        }
        let headers = (request.headers.iter())
            .filter(|field| header_name(&field.name) && !credential(&field.name))
            .map(|field| (field.name.to_ascii_lowercase(), field.value.clone()))
            .collect();
        let mut outgoing = OutgoingRequest {
            method: method.as_str().to_string(),
            url,
            headers,
        };
        let (exposure, fields) = match &connection.auth {
            None => (manifest_exposure(&connection.response), None),
            Some(slot) => {
                let (released, fields) =
                    self.inject(&install, slot, &config, &mut outgoing).await?;
                (
                    narrowed(&released.exposure, &connection.response),
                    Some(fields),
                )
            }
        };
        Ok(Ready {
            outgoing,
            exposure,
            fields: fields.unwrap_or_else(|| SecretFields::new(Default::default())),
            body: request.body,
            intent: request.intent,
            whole: connection.response.full(),
        })
    }

    /// Releases the slot's secret for this request and adds it as its kind says.
    async fn inject(
        &self,
        install: &PluginInstall,
        slot: &str,
        config: &aruna_core::structs::identity::realm::RealmConfigDocument,
        outgoing: &mut OutgoingRequest,
    ) -> Result<(Released, SecretFields), HostError> {
        let target = Some((outgoing.method.clone(), outgoing.url.to_string()));
        let use_of = (Delivery::Inject, target);
        let (released, fields) = self.obtain(install, slot, use_of, config).await?;
        let injected = inject_secret(&released.kind, &released.settings, &fields, outgoing);
        injected.map_err(|error| HostError::new(ErrorKind::Denied, error.to_string()))?;
        Ok((released, fields))
    }

    /// One checked release of the slot's secret for this run, reusing fields it holds.
    async fn obtain(
        &self,
        install: &PluginInstall,
        slot: &str,
        (delivery, target): (Delivery, Option<(String, String)>),
        config: &aruna_core::structs::identity::realm::RealmConfigDocument,
    ) -> Result<(Released, SecretFields), HostError> {
        let request = ReleaseRequest {
            install_id: install.install_id,
            slot: slot.to_string(),
            delivery,
            target,
            held: None,
            run_id: Some(self.scope.run.run_id),
        };
        let place = (self.scope.local, &self.keys);
        let ask = |request| ask_release(&self.ctx, config, place, request);
        let mut held = self.held.lock().await;
        held.obtain(request, ask)
            .await
            .map_err(|error| match error {
                ReleaseError::Refused(_) => HostError::new(ErrorKind::Denied, error.to_string()),
                ReleaseError::Unavailable(why) => unavailable(why),
            })
    }

    /// Records fields as handed to the code, as `secret_value` does.
    #[cfg(test)]
    pub(crate) fn shows(&self, fields: SecretFields) {
        let shown = self.shown.lock();
        (shown.unwrap_or_else(|poisoned| poisoned.into_inner())).push(fields);
    }

    /// Text the run keeps with every form of a value handed to the code removed, as extra
    /// protection only: the code holds the value and may change it.
    pub fn scrub(&self, text: &str) -> String {
        let shown = self.shown.lock();
        let shown = shown.unwrap_or_else(|poisoned| poisoned.into_inner());
        (shown.iter()).fold(text.to_string(), |text, fields| scrub(&text, fields))
    }
}

/// The scope of an install, for its generated grant paths.
fn install_scope(install: &PluginInstall) -> InstallScope {
    InstallScope {
        realm_id: install.account.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    }
}

/// The manifest's name of a method the guest called.
fn declared_method(method: aruna_plugins::broker::Method) -> aruna_plugins::connection::Method {
    use aruna_plugins::broker::Method as Called;
    use aruna_plugins::connection::Method;
    match method {
        Called::Get => Method::Get,
        Called::Head => Method::Head,
        Called::Post => Method::Post,
        Called::Put => Method::Put,
        Called::Patch => Method::Patch,
        Called::Delete => Method::Delete,
    }
}

/// Headers a guest may never set; the host owns credentials.
fn credential(name: &str) -> bool {
    ["authorization", "cookie", "proxy-authorization"].contains(&name.to_ascii_lowercase().as_str())
}

/// The answer as the guest may see it: the one place an exposure applies to an answer.
fn exposed(exposure: &ResponseExposure, fields: &SecretFields, answer: &Answer) -> Response {
    let shown = expose_response(
        exposure,
        fields,
        answer.status,
        &answer.headers,
        &answer.body,
    );
    Response {
        status: shown.status,
        json: shown
            .body
            .map(|body| String::from_utf8_lossy(&body).into_owned()),
    }
}

/// The exposure of a connection without a secret: what its manifest reads.
fn manifest_exposure(reads: &Readable) -> ResponseExposure {
    match reads {
        Readable::Full(_) => ResponseExposure::Full,
        Readable::Fields(fields) => fields_exposure(fields.to_vec()),
    }
}

fn fields_exposure(pointers: Vec<String>) -> ResponseExposure {
    match pointers.is_empty() {
        true => ResponseExposure::Status,
        false => ResponseExposure::Fields {
            pointers,
            headers: Vec::new(),
        },
    }
}

/// The rule's exposure, never wider than what the manifest lets the code read.
fn narrowed(rule: &ResponseExposure, reads: &Readable) -> ResponseExposure {
    match (rule, reads) {
        (ResponseExposure::Status, _) => ResponseExposure::Status,
        (ResponseExposure::Full, reads) => manifest_exposure(reads),
        (ResponseExposure::Fields { pointers, .. }, Readable::Full(_)) => {
            fields_exposure(pointers.clone())
        }
        (ResponseExposure::Fields { pointers, .. }, Readable::Fields(fields)) => fields_exposure(
            pointers
                .iter()
                .filter(|p| fields.contains(p))
                .cloned()
                .collect(),
        ),
    }
}

/// The whole answer as the code may see it, with echoes of the secret removed.
fn whole(fields: &SecretFields, answer: &Answer) -> FullResponse {
    let shown = expose_response(
        &ResponseExposure::Full,
        fields,
        answer.status,
        &answer.headers,
        &answer.body,
    );
    FullResponse {
        status: shown.status,
        headers: (shown.headers.into_iter())
            .map(|(name, value)| Field { name, value })
            .collect(),
        body: shown.body.unwrap_or_default(),
    }
}

fn whole_text(reply: &FullResponse) -> String {
    let headers: Vec<(&str, &str)> = (reply.headers.iter())
        .map(|field| (field.name.as_str(), field.value.as_str()))
        .collect();
    let body = base64::engine::general_purpose::STANDARD.encode(&reply.body);
    serde_json::json!({ "status": reply.status, "headers": headers, "body": body }).to_string()
}

/// The stored reply of a sent request whose answer did not fit the call.
fn too_large(status: u16) -> String {
    serde_json::json!({ "status": status, "too_large": true }).to_string()
}

/// The refusal a stored too-large marker answers with, every time it is replayed.
fn refuse_large(value: &Value) -> Result<(), HostError> {
    if value.get("too_large").and_then(Value::as_bool) != Some(true) {
        return Ok(());
    }
    let status = value
        .get("status")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let message = format!("the answer with status {status} is larger than the call allows");
    Err(HostError::new(ErrorKind::Limit, message))
}

/// A send that has no stored intent: an answer that does not fit is a limit, others unavailable.
fn send_refusal(error: SendError) -> HostError {
    match error {
        SendError::TooLarge { .. } => HostError::new(ErrorKind::Limit, error.to_string()),
        error => unavailable(error.to_string()),
    }
}

fn stored_whole(reply: &str) -> Result<FullResponse, HostError> {
    let broken = || unavailable("a stored whole answer does not decode");
    let value: Value = serde_json::from_str(reply).map_err(|_| broken())?;
    refuse_large(&value)?;
    let status = value
        .get("status")
        .and_then(Value::as_u64)
        .and_then(|s| u16::try_from(s).ok());
    let headers: Vec<(String, String)> =
        serde_json::from_value(value.get("headers").cloned().unwrap_or(Value::Null))
            .map_err(|_| broken())?;
    let body = value
        .get("body")
        .and_then(Value::as_str)
        .ok_or_else(broken)?;
    let body = base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|_| broken())?;
    Ok(FullResponse {
        status: status.ok_or_else(broken)?,
        headers: (headers.into_iter())
            .map(|(name, value)| Field { name, value })
            .collect(),
        body,
    })
}

/// A stored reply under the current exposure: each stored field stays or goes by its pointer
/// key, so a replay can only narrow what the guest saw, never widen it.
fn kept(reply: Response, exposure: &ResponseExposure) -> Response {
    let json = match exposure {
        ResponseExposure::Status => None,
        ResponseExposure::Full => reply.json,
        ResponseExposure::Fields { pointers, .. } => (reply.json)
            .and_then(|json| serde_json::from_str::<serde_json::Map<String, Value>>(&json).ok())
            .map(|stored| {
                let kept: serde_json::Map<String, Value> = (stored.into_iter())
                    .filter(|(pointer, _)| pointers.contains(pointer))
                    .collect();
                Value::Object(kept).to_string()
            }),
    };
    Response {
        status: reply.status,
        json,
    }
}

/// The stored reply of a settled send.
fn reply_text(response: &Response) -> String {
    serde_json::json!({ "status": response.status, "json": response.json }).to_string()
}

fn stored_reply(reply: &str) -> Result<Response, HostError> {
    let value: Value =
        serde_json::from_str(reply).map_err(|error| unavailable(error.to_string()))?;
    refuse_large(&value)?;
    let status = value
        .get("status")
        .and_then(Value::as_u64)
        .and_then(|s| u16::try_from(s).ok());
    Ok(Response {
        status: status.ok_or_else(|| unavailable("a stored reply has no status"))?,
        json: value
            .get("json")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

#[cfg(test)]
#[path = "broker_tests.rs"]
mod tests;
