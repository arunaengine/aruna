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
use aruna_core::secrets::{Delivery, ResponseExposure, SecretFields};
use aruna_core::structs::checksum::{ChecksumAlgorithm, HASH_BLAKE3};
use aruna_core::structs::execution::job::UploadForm;
use aruna_core::structs::identity::auth::{
    Actor, AuthContext, PathRestriction, Permission as Access,
};
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::data::identity::DataIdentity;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::broker::{
    Broker, CallContext, Created, Document, Entry, ErrorKind, Field, FileChange, FilePage,
    HostError, Intent, JobCopy, JobDownload, JobStatus, JobUpload, Listing, MetadataCreate,
    MetadataPatch, ObjectInfo, ObjectPage, Response, SendRequest, StatePut, StateRemove,
};
use aruna_plugins::permission::Permission;
use async_trait::async_trait;
use base64::Engine;
use craqle::GraphId;
use futures_util::StreamExt;
use oxrdf::Term;
use serde_json::Value;
use tracing::warn;
use ulid::Ulid;

use super::check::{RunScope, authorize_call, check_call, matching, unavailable};
use super::creates::{Lookup, Step, claim_key, next_step, settle_key};
use super::grants::{InstallScope, accesses, parse_grants, patterns_for};
use super::held::{HeldSecrets, ask_release};
use super::install::{pick_version, read_config, read_manifest};
use super::intents::{Begun, begin, clear_in, mark_unknown, settle_in, written};
use super::jobs::{cancel_job, job_outputs, job_row, job_status};
use super::marks::{is_marked, unmarked};
use super::outbound::{Answer, Egress, Outbound, SendError};
use super::outgoing::{node_keys, outgoing, request_context};
use super::rate::take_call;
use super::release::{ReleaseError, ReleaseKeys, ReleaseRequest, Released};
use super::state::{PutError, check_key, put_state, read_state, remove_state};
use super::store::{StoreError, Txn, transact};
use super::submit::{
    CopyRequest, DownloadRequest, UploadRequest, start_copy, start_download, start_upload,
};
use super::work::RequestContext;
use crate::auth::request_authorization::AuthorizeError;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::forward::transport::MetadataWriteError;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export_types::{
    ExportMetadataRequest, ExportMetadataResult, GetVisibleRequest, RoCrateExportView,
};
use crate::metadata::api::list::list_visible_documents;
use crate::metadata::api::list_types::{ListVisibleRequest, MetadataListOrder};
use crate::metadata::create_document::{CreateDocumentError, CreateDocumentPayload};
use crate::metadata::forward::read::{export_rocrate_routed, get_metadata_routed};
use crate::metadata::forward::write::{
    CreateAuthorizedError, PinnedCreate, create_with_id, route_metadata_update,
};
use crate::metadata::update_document::{UpdateDocumentError, UpdateDocumentMutation};
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::object::head::{HeadObjectInput, HeadObjectOperation};
use crate::s3::object::list::{ListBucketInput, ListBucketOperation, ListContinuationToken};
use aruna_core::entity_patch::craqle_patch_request;

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
            keys: node_keys(&ctx),
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
        let patterns = patterns_for(&grants, permission);
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
            content_urls(&file)?;
            self.objects_named(&install, (&record.graph_iri, &file))
                .await?;
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

    /// Checks each Aruna object an entity names through the plugin's own `objects.read` call
    /// check and that it lies in a group bucket here. A content hash or global id needs one too.
    /// The dataset's own JSON-LD context can make an export name others; it checks the exporter.
    async fn objects_named(
        &self,
        install: &PluginInstall,
        (graph, file): (&str, &Value),
    ) -> Result<(), HostError> {
        let realm_id = self.scope.realm_id;
        // Export renders each stored value as is, without `./`, or as literal text, under any term
        // the context gives (craqle `context_value`), so every value of every predicate is checked.
        let converted = craqle_patch_request(&GraphId::new(graph), &file.to_string());
        let converted =
            converted.map_err(|error| HostError::new(ErrorKind::Invalid, error.to_string()))?;
        let entity = converted.entity;
        let objects = entity
            .additional_triples
            .iter()
            .filter_map(|(_, object)| match object {
                Term::NamedNode(node) => Some(node.as_str().to_string()),
                Term::Literal(literal) => Some(literal.value().to_string()),
                _ => None,
            });
        let own = [&entity.entity_id, &entity.entity_type, &entity.name].map(String::clone);
        let stored: Vec<String> = own.into_iter().chain(objects).collect();
        let bare = stored.iter().filter_map(|value| value.strip_prefix("./"));
        let bare: Vec<String> = bare.map(str::to_string).collect();
        let values: Vec<String> = stored.into_iter().chain(bare).collect();
        let refused = |what: &str| {
            let message = format!("{what} names no object a plugin may check; use its path");
            HostError::new(ErrorKind::Denied, message)
        };
        let group = install.group_id;
        // A content hash or global id names no object alone; a concrete one must come with it.
        let (mut objects, mut hash, mut unplaced) = (Vec::new(), None, None);
        for value in values.iter().map(String::as_str) {
            let identity = DataIdentity::read(value, &[]);
            if value.starts_with(&format!("/{realm_id}/g/")) {
                objects.push((value.to_string(), None));
            } else if let Some(exact) = identity.exact {
                if exact.realm_id != realm_id {
                    return Err(refused(value));
                }
                let (node, bucket, key) = (exact.node_id, &exact.bucket, &exact.key);
                let path = object_permission_path(realm_id, group, node, bucket, key);
                objects.push((path, Some(exact.version)));
            } else if let Some(at) = identity.location {
                let local = self.scope.local;
                objects.push((
                    object_permission_path(realm_id, group, local, &at.bucket, &at.key),
                    None,
                ));
            } else if identity.is_aruna() {
                // Readers keep one hash of a file, so two different ones are never admitted.
                if let Some(found) = identity.hash {
                    if hash.is_some_and(|known| known != found) {
                        let message = "the file names two different content hashes";
                        return Err(HostError::new(ErrorKind::Invalid, message));
                    }
                    hash = Some(found);
                }
                unplaced.get_or_insert(value);
            }
        }
        if let (Some(value), true) = (unplaced, objects.is_empty()) {
            return Err(refused(value));
        }
        for (path, version) in objects {
            self.checked(Permission::ObjectsRead, &path).await?;
            let (bucket, key) = self.object_at(install, &path).await?;
            if let Some(hash) = hash {
                self.same_content((bucket, key, version), hash).await?;
            }
        }
        Ok(())
    }

    /// Admits the hash a file names an object by only when the object's verified BLAKE3 digest is
    /// recorded and equal; a pending upload without one cannot vouch for any hash.
    async fn same_content(
        &self,
        (bucket, key, version_id): (String, String, Option<Ulid>),
        hash: [u8; 32],
    ) -> Result<(), HostError> {
        let head = HeadObjectInput {
            bucket,
            key,
            version_id,
        };
        let head = drive(HeadObjectOperation::new(head), &self.ctx).await;
        let head = head.map_err(|_| HostError::new(ErrorKind::NotFound, "the object is gone"))?;
        let stored = head
            .location
            .and_then(|location| location.hashes.get(HASH_BLAKE3).cloned());
        let invalid = |message: &str| HostError::new(ErrorKind::Invalid, message);
        match stored {
            None => Err(invalid(
                "the object has no verified digest for the file's hash id yet",
            )),
            Some(stored) if stored != hash => Err(invalid(
                "the object's content does not match the file's hash id",
            )),
            Some(_) => Ok(()),
        }
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

    /// Starts native work through the run's call history: other arguments at a known ordinal are
    /// refused before any work is reserved, a settled call answers its job id again, and an earlier
    /// attempt is safe to repeat, as the work row names the job by run and ordinal.
    async fn native(
        &self,
        intent: &Intent,
        start: impl std::future::Future<Output = Result<Ulid, HostError>>,
    ) -> Result<String, HostError> {
        match self.begun(intent).await {
            Ok(Some(reply)) => return Ok(reply),
            Ok(None) => {}
            Err(error) if error.kind == ErrorKind::UnknownOutcome => {}
            Err(error) => return Err(error),
        }
        // A refused start leaves no job, or one its work row finds again, so the call may be made
        // again; its intent goes.
        let outcome = start.await.map(|work_id| work_id.to_string());
        self.record(intent, outcome.map_err(Outcome::None)).await
    }

    /// The full permission path of an object path the plugin gave; see [`object_path`].
    fn expand(&self, text: &str) -> Result<String, HostError> {
        let place = (self.scope.realm_id, self.scope.local);
        object_path(place, self.scope.run.group_id, text)
    }

    /// The local bucket and key of an object path below the install's group data.
    async fn object_at(
        &self,
        install: &PluginInstall,
        path: &str,
    ) -> Result<(String, String), HostError> {
        let place = (self.scope.realm_id, self.scope.local);
        local_object(&self.ctx, place, install, path).await
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

    /// Whether a job with a secret wrote this version of `bucket/key`.
    async fn marked(&self, bucket: &str, key: &str, version: &str) -> Result<bool, HostError> {
        let Ok(version_id) = Ulid::from_string(version) else {
            return Ok(false);
        };
        let marked = is_marked(&self.ctx, (bucket, key, version_id)).await;
        marked.map_err(|error| unavailable(error.0))
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
                let marked = self.marked(bucket, &key, &version).await?;
                visible.push(ObjectInfo {
                    path: format!("{bucket}/{key}"),
                    size,
                    version,
                    marked,
                });
            }
        }
        Ok(visible)
    }
}

/// The full permission path of an object path a plugin gives: a path below the realm as it is,
/// or `bucket/key` (a prefix ends in `/`) on this node. Expanded before any grant check, so a
/// path the plugin did not mean is never checked.
pub(crate) fn object_path(
    (realm_id, local): (aruna_core::structs::identity::realm::RealmId, NodeId),
    group: aruna_core::types::GroupId,
    text: &str,
) -> Result<String, HostError> {
    if text.starts_with('/') {
        return Ok(text.to_string());
    }
    let invalid = || {
        let message = format!("`{text}` is not a bucket path such as `bucket/key`");
        HostError::new(ErrorKind::Invalid, message)
    };
    let (bucket, key) = text.split_once('/').unwrap_or((text, ""));
    let segments: Vec<&str> = key.split('/').collect();
    let last = segments.len() - 1;
    let plain = (segments.iter().enumerate()).all(|(index, segment)| {
        !matches!(*segment, "." | "..") && (index == last || !segment.is_empty())
    });
    if matches!(bucket, "" | "." | "..") || !plain {
        return Err(invalid());
    }
    Ok(object_permission_path(realm_id, group, local, bucket, key))
}

/// The version id a plugin gave as text.
fn version_id(text: &str) -> Result<Ulid, HostError> {
    let invalid = || HostError::new(ErrorKind::Invalid, format!("`{text}` is not a version id"));
    Ulid::from_string(text).map_err(|_| invalid())
}

/// A byte bound as people read it: `1 MiB` rather than `1048576 bytes`.
fn byte_size(bytes: usize) -> String {
    match bytes % (1 << 20) {
        0 => format!("{} MiB", bytes >> 20),
        _ => format!("{bytes} bytes"),
    }
}

/// The `bucket/key` of a permission path of an object on `local` in the install's group; any other
/// text stays as it is.
pub(crate) fn short_path(path: &str, (install, local): (&PluginInstall, NodeId)) -> String {
    let realm = install.account.realm_id;
    let root = format!("/{realm}/g/{}/data/{local}/", install.group_id);
    path.strip_prefix(&root).unwrap_or(path).to_string()
}

/// Refuses a `contentUrl` that names nothing readers can resolve, such as a short bucket path:
/// it must be an absolute IRI such as an `s3://` address, an object path below the realm, or a
/// reference inside the crate.
fn content_urls(file: &Value) -> Result<(), HostError> {
    let urls = match file.get("contentUrl") {
        Some(Value::Array(urls)) => urls.iter().collect(),
        Some(url) => vec![url],
        None => Vec::new(),
    };
    for url in urls {
        let text = (url.get("@id").unwrap_or(url).as_str()).unwrap_or_default();
        let scheme = text
            .split_once(':')
            .is_some_and(|(scheme, _)| !scheme.is_empty() && !scheme.contains('/'));
        if !(scheme || text.starts_with('/') || text.starts_with("./")) {
            let message = format!(
                "the contentUrl `{text}` names no object; use the object's s3:// address or path"
            );
            return Err(HostError::new(ErrorKind::Invalid, message));
        }
    }
    Ok(())
}

/// The bucket and key on `local` of an object path below the install's group data; a bucket of
/// another node or another group is refused.
pub(crate) async fn local_object(
    ctx: &DriverContext,
    (realm_id, local): (aruna_core::structs::identity::realm::RealmId, NodeId),
    install: &PluginInstall,
    path: &str,
) -> Result<(String, String), HostError> {
    let invalid = || HostError::new(ErrorKind::Invalid, format!("{path} is not an object path"));
    let root = format!("/{realm_id}/g/{}/data/", install.group_id);
    let rest = path.strip_prefix(&root).ok_or_else(invalid)?;
    let mut parts = rest.splitn(3, '/');
    let (node, bucket) = (
        parts.next().unwrap_or_default(),
        parts.next().ok_or_else(invalid)?,
    );
    let key = parts.next().unwrap_or_default().to_string();
    let node = NodeId::from_str(node).map_err(|_| invalid())?;
    if node != local {
        return Err(unavailable(
            "objects of another node are not reachable from plugins yet",
        ));
    }
    let info = drive(GetBucketOperation::new(bucket.to_string()), ctx).await;
    match info {
        Ok(info) if info.group_id == install.group_id => Ok((bucket.to_string(), key)),
        _ => Err(HostError::new(
            ErrorKind::NotFound,
            format!("no bucket at {path}"),
        )),
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
        // The dataset entity is checked as stored, like a file; the new graph's name is not known.
        let stored: Value = serde_json::from_str(&jsonld).unwrap_or(Value::Null);
        let graph = MetadataRegistryRecord::graph_iri_for(Ulid::nil());
        self.objects_named(&install, (&graph, &stored["@graph"][1]))
            .await?;
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

    async fn metadata_remove_file(
        &self,
        cx: &CallContext,
        change: FileChange,
    ) -> Result<String, HostError> {
        if change.file.trim().is_empty() {
            return Err(HostError::new(ErrorKind::Invalid, "a removal names an @id"));
        }
        let id = change.file.clone();
        let mutation = UpdateDocumentMutation::RemoveDataEntity { id };
        self.file_write(cx, &change, mutation, None).await
    }

    async fn objects_info(
        &self,
        _cx: &CallContext,
        text: &str,
        version: Option<String>,
    ) -> Result<ObjectInfo, HostError> {
        let path = self.expand(text)?;
        let (install, _) = self.checked(Permission::ObjectsRead, &path).await?;
        let (bucket, key) = self.object_at(&install, &path).await?;
        let short = format!("{bucket}/{key}");
        // A given version is checked as the current one: the same grant, and its own mark.
        let input = HeadObjectInput {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: (version.as_deref()).map(version_id).transpose()?,
        };
        let head = drive(HeadObjectOperation::new(input), &self.ctx).await;
        let head =
            head.map_err(|_| HostError::new(ErrorKind::NotFound, format!("no object at {path}")))?;
        let version = (head.resolved_version_id.map(|id| id.to_string())).unwrap_or_default();
        Ok(ObjectInfo {
            path: short,
            size: head.location.map_or(0, |location| location.blob_size),
            marked: self.marked(&bucket, &key, &version).await?,
            version,
        })
    }

    async fn objects_list(
        &self,
        _cx: &CallContext,
        prefix: &str,
        cursor: Option<String>,
    ) -> Result<ObjectPage, HostError> {
        let (install, _) = self.listing(Permission::ObjectsRead).await?;
        let (bucket, key) = self.object_at(&install, &self.expand(prefix)?).await?;
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

    async fn objects_read(
        &self,
        cx: &CallContext,
        text: &str,
        version: Option<String>,
    ) -> Result<Vec<u8>, HostError> {
        let path = self.expand(text)?;
        let (install, auth) = self.checked(Permission::ObjectsRead, &path).await?;
        let (bucket, key) = self.object_at(&install, &path).await?;
        let version = (version.as_deref()).map(version_id).transpose()?;
        let missing = || HostError::new(ErrorKind::NotFound, format!("no object at {path}"));
        let version = unmarked(&self.ctx, (&bucket, &key), version).await?;
        let input = GetObjectInput {
            bucket,
            key,
            version_id: Some(version.ok_or_else(missing)?),
            range: None,
            group_id: install.group_id,
            user_identity: install.account,
            realm_id: self.scope.realm_id,
            node_id: self.scope.local,
        };
        let read = GetObjectOperation::new(input).with_restrictions(auth.path_restrictions);
        let mut got = drive(read, &self.ctx).await.map_err(|error| match error {
            GetObjectError::NoSuchKey | GetObjectError::NoSuchVersion => missing(),
            other => unavailable(other.to_string()),
        })?;
        let too_large = || {
            let message = format!(
                "objects larger than {} cannot be read by a plugin; give them to a task",
                byte_size(cx.max_bytes)
            );
            HostError::new(ErrorKind::Limit, message)
        };
        if got.info.size > cx.max_bytes as u64 {
            return Err(too_large());
        }
        let mut bytes = Vec::with_capacity(got.info.size as usize);
        while let Some(chunk) = got.blob.next().await {
            bytes.extend_from_slice(&chunk.map_err(|error| unavailable(error.to_string()))?);
            if bytes.len() > cx.max_bytes {
                return Err(too_large());
            }
        }
        Ok(bytes)
    }

    async fn connection_send(
        &self,
        cx: &CallContext,
        request: SendRequest,
    ) -> Result<Response, HostError> {
        let ready = self.ready(request).await?;
        self.send_whole(cx, ready).await
    }

    async fn jobs_copy(&self, _cx: &CallContext, copy: JobCopy) -> Result<String, HostError> {
        self.admitted(None).await?;
        let version = (copy.version.as_deref()).map(version_id).transpose()?;
        let request = CopyRequest {
            source: self.expand(&copy.source)?,
            source_version: version,
            target: self.expand(&copy.target)?,
            ordinal: copy.intent.ordinal,
        };
        let started = self.native(&copy.intent, start_copy(&self.ctx, &self.scope, request));
        match started.await {
            Err(error) if error.kind == ErrorKind::Conflict => {
                self.changed.store(true, Ordering::Relaxed);
                Err(error)
            }
            answered => answered,
        }
    }

    async fn jobs_download(
        &self,
        _cx: &CallContext,
        download: JobDownload,
    ) -> Result<String, HostError> {
        let target = self.expand(&download.target)?;
        let checksums = expected(&download.checksums)?;
        let request = SendRequest {
            connection: download.service,
            method: aruna_plugins::broker::Method::Get,
            path: download.path,
            query: download.query,
            headers: download.headers,
            body: Vec::new(),
            intent: None,
        };
        // The whole answer lands in a bucket, so the rule must let the plugin see it whole.
        let ready = self.ready(request).await?;
        if ready.exposure != ResponseExposure::Full {
            let narrower = "this connection does not expose its whole answer";
            return Err(HostError::new(ErrorKind::Denied, narrower));
        }
        let request = DownloadRequest {
            context: ready.context,
            target,
            checksums,
            max_bytes: download.size_limit,
            secret: ready.secret,
            ordinal: download.intent.ordinal,
        };
        let started = start_download(&self.ctx, &self.scope, request);
        match self.native(&download.intent, started).await {
            Err(error) if error.kind == ErrorKind::Conflict => {
                self.changed.store(true, Ordering::Relaxed);
                Err(error)
            }
            answered => answered,
        }
    }

    async fn jobs_upload(&self, _cx: &CallContext, upload: JobUpload) -> Result<String, HostError> {
        use aruna_plugins::broker::Method;
        if !matches!(upload.method, Method::Put | Method::Post) {
            let message = "an upload is sent with PUT or POST";
            return Err(HostError::new(ErrorKind::Invalid, message));
        }
        let source = self.expand(&upload.source)?;
        let version = (upload.version.as_deref()).map(version_id).transpose()?;
        let request = SendRequest {
            connection: upload.service,
            method: upload.method,
            path: upload.path,
            query: upload.query,
            headers: upload.headers,
            body: Vec::new(),
            intent: None,
        };
        // The same checks as a request the run sends itself, with its secret released.
        let ready = self.ready(request).await?;
        let file_name = source.rsplit('/').next().unwrap_or_default().to_string();
        let form = upload.file_field.map(|file_field| UploadForm {
            file_field,
            file_name,
            fields: (upload.fields.into_iter())
                .map(|field| (field.name, field.value))
                .collect(),
        });
        let request = UploadRequest {
            context: ready.context,
            source,
            version,
            form,
            ordinal: upload.intent.ordinal,
        };
        let started = start_upload(&self.ctx, &self.scope, request);
        match self.native(&upload.intent, started).await {
            Err(error) if error.kind == ErrorKind::Conflict => {
                self.changed.store(true, Ordering::Relaxed);
                Err(error)
            }
            answered => answered,
        }
    }

    async fn jobs_get(&self, _cx: &CallContext, id: &str) -> Result<JobStatus, HostError> {
        let install = self.admitted(None).await?;
        let mut status = job_status(&self.ctx, (&install, self.scope.local), id).await?;
        if status.reply_status.is_some() {
            let row = job_row(&self.ctx, &install, id).await?;
            let shown = match &row.context {
                Some(context) => self.reply_shown(&install, context).await,
                None => false,
            };
            // A refused check answers the state without the reply.
            if !shown {
                (status.reply_status, status.reply_body) = (None, None);
                status.reply_headers.clear();
            }
        }
        Ok(status)
    }

    async fn jobs_outputs(
        &self,
        _cx: &CallContext,
        id: &str,
        cursor: Option<String>,
    ) -> Result<ObjectPage, HostError> {
        let install = self.admitted(None).await?;
        job_outputs(&self.ctx, (&install, self.scope.local), id, cursor).await
    }

    async fn jobs_cancel(
        &self,
        _cx: &CallContext,
        id: &str,
        _intent: Intent,
    ) -> Result<(), HostError> {
        // Asking again changes nothing, so a rerun cancels again instead of reading an intent.
        let install = self.admitted(None).await?;
        cancel_job(&self.ctx, &install, id, unix_timestamp_millis()).await
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

    async fn state_remove(&self, _cx: &CallContext, remove: StateRemove) -> Result<(), HostError> {
        check_key(&remove.key)?;
        let install = self.admitted(None).await?;
        if self.begun(&remove.intent).await?.is_some() {
            return Ok(());
        }
        let run_id = self.scope.run.run_id;
        match remove_state(&self.ctx, run_id, install.install_id, &remove).await {
            Ok(()) => Ok(()),
            Err(PutError::Refused(message, kind)) => {
                let clear = async |txn: &Txn<'_>| clear_in(txn, run_id, &remove.intent).await;
                transact::<(), StoreError>(&self.ctx, clear)
                    .await
                    .map_err(|error| unavailable(error.0))?;
                Err(HostError::new(kind, message))
            }
            Err(PutError::Store(error)) => Err(self.unknown(&remove.intent, &error.0).await),
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
        MetadataWriteError::Update(UpdateDocumentError::GraphPending { .. }) => {
            Outcome::None(unavailable(error.to_string()))
        }
        MetadataWriteError::Update(UpdateDocumentError::StorageError(_))
        | MetadataWriteError::Undeliverable(_) => Outcome::Maybe(error.to_string()),
        other => none(ErrorKind::Invalid, other.to_string()),
    }
}

/// A checked connection request: the outgoing request with any secret added, the exposure its
/// secret's rule allows, and the secret's fields for echo removal.
struct Ready {
    outgoing: OutgoingRequest,
    exposure: ResponseExposure,
    fields: SecretFields,
    body: Vec<u8>,
    intent: Option<Intent>,
    /// The request as a work row keeps it, and whether the connection injects a secret.
    context: RequestContext,
    secret: bool,
}

impl RunBroker {
    /// Sends and answers the whole answer. Install writes every rule of a plugin secret with
    /// the whole answer; one an admin narrowed by hand refuses the call instead of filtering it.
    async fn send_whole(&self, cx: &CallContext, ready: Ready) -> Result<Response, HostError> {
        if ready.exposure != ResponseExposure::Full {
            let narrower = "the secret's use rule no longer lets this plugin read the answer";
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

    /// The per call check, the address below the connection, the guest's own headers, and the
    /// secret released and added; every use of a secret, a replay included, passes the check.
    async fn ready(&self, request: SendRequest) -> Result<Ready, HostError> {
        let current = self.admitted(None).await?;
        let path = install_scope(&current).connection(&request.connection);
        let (install, _) = self.checked(Permission::ConnectionsSend, &path).await?;
        let config = read_config(&self.ctx, self.scope.realm_id).await;
        let config = config.map_err(|error| unavailable(error.to_string()))?;
        let plugin = pick_version(&config, &install.plugin, Some(&install.digest));
        let manifest = plugin.and_then(|plugin| read_manifest(&plugin));
        let manifest = manifest.map_err(|error| unavailable(error.to_string()))?;
        // The guest built its request from the settings saved at admission; the address follows
        // them, while the grant and release checks use the current install.
        let pairs = |fields: &[Field]| -> Vec<(String, String)> {
            let pairs = fields
                .iter()
                .map(|field| (field.name.clone(), field.value.clone()));
            pairs.collect()
        };
        let (query, headers) = (pairs(&request.query), pairs(&request.headers));
        let called = (request.connection.as_str(), declared_method(request.method));
        let parts = (request.path.as_str(), &query[..], &headers[..]);
        let context = request_context(&manifest, called, &self.scope.run.settings, parts)?;
        let (mut outgoing, slot) = outgoing(&manifest, &context)?;
        let (exposure, fields) = match &slot {
            None => (ResponseExposure::Full, None),
            Some(slot) => {
                let (released, fields) =
                    self.inject(&install, slot, &config, &mut outgoing).await?;
                (released.exposure, Some(fields))
            }
        };
        Ok(Ready {
            outgoing,
            exposure,
            secret: fields.is_some(),
            fields: fields.unwrap_or_else(|| SecretFields::new(Default::default())),
            body: request.body,
            intent: request.intent,
            context,
        })
    }

    /// Whether a kept reply of `context` may be shown now: the connection is still granted and
    /// declared, and its secret's rule still releases for the request and shows it whole.
    async fn reply_shown(&self, install: &PluginInstall, context: &RequestContext) -> bool {
        let path = install_scope(install).connection(&context.connection);
        if self
            .checked(Permission::ConnectionsSend, &path)
            .await
            .is_err()
        {
            return false;
        }
        let Ok(config) = read_config(&self.ctx, self.scope.realm_id).await else {
            return false;
        };
        let plugin = pick_version(&config, &install.plugin, Some(&install.digest));
        let Ok(manifest) = plugin.and_then(|plugin| read_manifest(&plugin)) else {
            return false;
        };
        let Ok((mut request, slot)) = outgoing(&manifest, context) else {
            return false;
        };
        match slot {
            None => true,
            Some(slot) => {
                let released = self.inject(install, &slot, &config, &mut request).await;
                released.is_ok_and(|(released, _)| released.exposure == ResponseExposure::Full)
            }
        }
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
/// The expected digests of a download: a known algorithm and a hex digest of its length.
fn expected(fields: &[Field]) -> Result<Vec<(ChecksumAlgorithm, Vec<u8>)>, HostError> {
    use ChecksumAlgorithm::{Crc32, Crc32c, Md5, Sha1, Sha256};
    let mut found = Vec::with_capacity(fields.len());
    for field in fields {
        let invalid = || {
            let message = format!("`{}` is not a checksum this node checks", field.name);
            HostError::new(ErrorKind::Invalid, message)
        };
        let algorithm = match field.name.to_ascii_lowercase().as_str() {
            "md5" => Md5,
            "sha1" => Sha1,
            "sha256" => Sha256,
            "crc32" => Crc32,
            "crc32c" => Crc32c,
            _ => return Err(invalid()),
        };
        let digest = hex::decode(field.value.trim()).map_err(|_| invalid())?;
        if digest.len() != algorithm.digest_len() {
            return Err(invalid());
        }
        found.push((algorithm, digest));
    }
    Ok(found)
}

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

/// The whole answer as the code may see it, with echoes of the secret removed.
fn whole(fields: &SecretFields, answer: &Answer) -> Response {
    let shown = expose_response(
        &ResponseExposure::Full,
        fields,
        answer.status,
        &answer.headers,
        &answer.body,
    );
    Response {
        status: shown.status,
        headers: (shown.headers.into_iter())
            .map(|(name, value)| Field { name, value })
            .collect(),
        body: shown.body.unwrap_or_default(),
    }
}

pub(crate) fn whole_text(reply: &Response) -> String {
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

fn stored_whole(reply: &str) -> Result<Response, HostError> {
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
    Ok(Response {
        status: status.ok_or_else(broken)?,
        headers: (headers.into_iter())
            .map(|(name, value)| Field { name, value })
            .collect(),
        body,
    })
}

#[cfg(test)]
#[path = "broker_tests.rs"]
mod tests;
