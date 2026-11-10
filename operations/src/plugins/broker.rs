//! Serves one run's host calls on the receiver: every call passes the per call check, and a
//! mutating call records its intent before its effect. Objects answer info and listings only.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use aruna_core::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::identity::auth::{
    Actor, AuthContext, PathRestriction, Permission as Access,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_plugins::broker::{
    Broker, CallContext, Document, Entry, ErrorKind, HostError, Intent, Listing, MetadataPatch,
    ObjectInfo, ObjectPage, Response, SendRequest, StatePut,
};
use aruna_plugins::permission::Permission;
use async_trait::async_trait;
use ulid::Ulid;

use super::check::{RunScope, check_call, matching, unavailable};
use super::grants::{accesses, parse_grants};
use super::intents::{Begun, begin, mark_unknown, settle_in};
use super::store::{StoreError, Txn, transact};
use crate::driver::{DriverContext, drive};
use crate::forward::transport::MetadataWriteError;
use crate::metadata::api::{
    ExportMetadataRequest, ExportMetadataResult, ListVisibleRequest, MetadataApiError,
    MetadataListOrder, RoCrateExportView, list_visible_documents,
};
use crate::metadata::forward::{export_rocrate_routed, route_metadata_update};
use crate::metadata::update_document::{UpdateDocumentError, UpdateDocumentMutation};
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::object::head::{HeadObjectInput, HeadObjectOperation};
use crate::s3::object::list::{ListBucketInput, ListBucketOperation, ListContinuationToken};

/// Entries of one metadata or object listing page.
const PAGE: usize = 100;

/// The host side of one run.
pub struct RunBroker {
    ctx: Arc<DriverContext>,
    scope: RunScope,
    /// A call at a known ordinal came with other arguments than before.
    changed: AtomicBool,
}

impl RunBroker {
    pub fn new(ctx: Arc<DriverContext>, scope: RunScope) -> Self {
        Self {
            ctx,
            scope,
            changed: AtomicBool::new(false),
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
        let install = check_call(&self.ctx, &self.scope, Some((permission, target))).await?;
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
        let install = check_call(&self.ctx, &self.scope, None).await?;
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
        document_id: Ulid,
        auth: AuthContext,
        max_bytes: usize,
    ) -> Result<(MetadataRegistryRecord, String), HostError> {
        let request = ExportMetadataRequest {
            document_id,
            auth: Some(auth.clone()),
            view: RoCrateExportView::Full,
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
            ExportMetadataResult::Full { record, jsonld } => Ok((record, jsonld)),
            _ => Err(unavailable("the document has no full export")),
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
                let clear =
                    async |txn: &Txn<'_>| super::intents::clear_in(txn, run_id, intent).await;
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
        let document_id = parse_document(resource)?;
        let (_, auth) = self.listing(Permission::MetadataRead).await?;
        let (record, json) = self.export(document_id, auth, cx.max_bytes).await?;
        self.checked(Permission::MetadataRead, &record.permission_path)
            .await?;
        Ok(Document {
            resource: resource.to_string(),
            revision: record.last_event_id.to_string(),
            json,
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
        let resources = (listed.documents.iter())
            .filter(|document| {
                matching(
                    &install,
                    Permission::MetadataRead,
                    &document.record.permission_path,
                )
                .is_ok()
            })
            .map(|document| document.record.document_id.to_string())
            .collect();
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
        let (_, auth) = self.listing(Permission::MetadataRead).await?;
        let (record, _) = self.export(document_id, auth, cx.max_bytes).await?;
        let path = &record.permission_path;
        let (install, auth) = self.checked(Permission::MetadataWrite, path).await?;
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
        let mutation = UpdateDocumentMutation::UpsertDataEntity {
            jsonld: patch.patch,
        };
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
        let bucket_root = prefix
            .strip_suffix(key.as_str())
            .unwrap_or(prefix)
            .to_string();
        let invalid = || HostError::new(ErrorKind::Invalid, "bad cursor");
        let token = match cursor {
            Some(cursor) => Some(
                ListContinuationToken::from_bytes(&hex::decode(cursor).map_err(|_| invalid())?)
                    .map_err(|_| invalid())?,
            ),
            None => None,
        };
        let input = ListBucketInput {
            bucket,
            group_id: install.group_id,
            continuation_token: token,
            max_keys: Some(PAGE),
            prefix: (!key.is_empty()).then_some(key),
            delimiter: None,
            start_after: None,
        };
        let listed = drive(ListBucketOperation::new(input), &self.ctx).await;
        let listed = listed.map_err(|error| unavailable(error.to_string()))?;
        let objects = (listed.objects.into_iter())
            .map(|object| ObjectInfo {
                path: format!("{bucket_root}{}", object.head.key),
                size: object.location.map_or(0, |location| location.blob_size),
                version: object.version_id.to_string(),
            })
            .filter(|object| matching(&install, Permission::ObjectsRead, &object.path).is_ok())
            .collect();
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
        _cx: &CallContext,
        _request: SendRequest,
    ) -> Result<Response, HostError> {
        Err(unavailable("connections are not available in this release"))
    }

    async fn state_get(&self, _cx: &CallContext, _key: &str) -> Result<Option<Entry>, HostError> {
        Err(unavailable("private state is not available yet"))
    }

    async fn state_put(&self, _cx: &CallContext, _put: StatePut) -> Result<u64, HostError> {
        Err(unavailable("private state is not available yet"))
    }

    async fn abandoned(&self, _cx: &CallContext, intent: &Intent) {
        let reason = "the call did not finish in time";
        let _ = mark_unknown(&self.ctx, self.scope.run.run_id, intent, reason).await;
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

#[cfg(test)]
#[path = "broker_tests.rs"]
mod tests;
