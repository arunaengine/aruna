//! Serves one run's host calls on the receiver: every call passes the per call check, and a
//! mutating call records its intent before its effect. Objects answer info and listings only.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_clock::unix_timestamp_millis;
use aruna_core::injection::{OutgoingRequest, expose_response, inject_secret, scrub};
use aruna_core::metadata::api::{RoCrateExportView, UpdateDocumentMutation};
use aruna_core::outbound::{ConnectionAnswer, ConnectionError};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{LogLine, PluginRun, StateRemoval, StateWrite};
use aruna_core::plugins::{ReleaseError, ReleaseRequest, Released};
use aruna_core::secrets::{Delivery, ResponseExposure, SecretFields};
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::execution::job::UploadForm;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_drivers::driver::{DriverContext, drive};
use aruna_operations::plugins::admitted::{
    Admitted, CancelAsk, ChangeAsk, InfoAsk, ListAsk, NativeAsk, OutputsAsk, StateAsk,
};
use aruna_operations::plugins::check::RunScope;
use aruna_operations::plugins::creates::{CreatePlan, stored_created};
use aruna_operations::plugins::dataset::file_entity;
use aruna_operations::plugins::grants::object_path;
use aruna_operations::plugins::held::{AskRelease, HeldSecrets};
use aruna_operations::plugins::host_calls::{
    CallEnd, CallFailure, RecordCall, StateChange, begun_reply, call_intent, version_id,
};
use aruna_operations::plugins::host_steps::{
    AdmitCall, ChangeInput, ChangeMutation, ChangePlan, CheckConfigured, CheckReply, Checked,
    ConfiguredCall, ConnectionAsk, ConnectionPlan, Granted, GrantedCall, ListDatasets, NativeCall,
    PrepareChange, PrepareConnection, PrepareCreate, ReadDataset, ReadFiles, ReadPlace, ReplyPlan,
};
use aruna_operations::plugins::intents::{begin, unknown_row};
use aruna_operations::plugins::job_work::JobState;
use aruna_operations::plugins::objects::{LocalObject, Unmarked};
use aruna_operations::plugins::outgoing::{
    SendConnection, declared_method, stored_whole, too_large, whole_text,
};
use aruna_operations::plugins::rows::write_rows;
use aruna_operations::plugins::state::MAX_STATE_KEY;
use aruna_operations::plugins::submit::{CopyRequest, DownloadRequest, UploadRequest, WorkRequest};
use aruna_operations::plugins::unavailable;
use aruna_operations::plugins::work::RequestContext;
use aruna_operations::plugins::work_calls::JobRow;
use aruna_operations::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};
use aruna_plugin_types::calls::{
    Created, Document, Entry, ErrorKind, Field, FilePage, HostError, Intent, JobStatus, Listing,
    ObjectInfo, ObjectPage, Response,
};
use aruna_plugin_types::permission::Permission;
use aruna_plugins::broker::{
    Broker, CallContext, FileChange, JobCopy, JobDownload, JobUpload, MetadataCreate,
    MetadataPatch, SendRequest, StatePut, StateRemove,
};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use ulid::Ulid;

/// The host side of one run.
pub struct RunBroker {
    ctx: Arc<DriverContext>,
    /// The secrets this run holds, reused while their value is unchanged.
    held: tokio::sync::Mutex<HeldSecrets>,
    /// The values handed to the code, removed from the text the run keeps.
    pub(super) shown: Mutex<Vec<SecretFields>>,
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
            held: tokio::sync::Mutex::default(),
            shown: Mutex::default(),
            ctx,
            log: Mutex::new(lines),
            scope,
            changed: AtomicBool::new(false),
        }
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
        Ok(drive(AdmitCall::new(&self.scope, target), &self.ctx).await?)
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
        let check = GrantedCall::target(&self.scope, permission, target);
        let Granted { install, auth } = drive(check, &self.ctx).await?;
        Ok((install, auth))
    }

    fn read_place(&self, cx: &CallContext) -> ReadPlace {
        ReadPlace {
            scope: self.scope.clone(),
            network_node: self.ctx.net_handle.as_ref().map(|net| net.node_id()),
            max_bytes: cx.max_bytes,
        }
    }

    /// A metadata change: its checks, then the write routed and its outcome recorded in the call's
    /// intent.
    fn change(
        &self,
        cx: &CallContext,
        (resource, expected, intent): (&str, &str, &Intent),
        (input, mutation): (ChangeInput, ChangeMutation),
    ) -> Result<Checked<PrepareChange, ChangePlan>, HostError> {
        let (place, revision) = (self.read_place(cx), (resource, expected));
        let prepare = PrepareChange::new(place, revision, call_intent(intent), input)?;
        let plan = ChangePlan {
            scope: self.scope.clone(),
            network_node: self.ctx.net_handle.as_ref().map(|net| net.node_id()),
            mutation,
            intent: call_intent(intent),
        };
        Ok(Checked::new(prepare, plan))
    }

    /// A revision-checked change of one file entity under the write grants. A file it names must
    /// be readable through `objects.read`, as for a restricted REST writer.
    fn file_change(
        &self,
        cx: &CallContext,
        change: &FileChange,
        mutation: UpdateDocumentMutation,
        file: Option<Value>,
    ) -> Result<Checked<PrepareChange, ChangePlan>, HostError> {
        let target = (change.resource.as_str(), change.expected.as_str(), &change.intent);
        let input = (ChangeInput::File(file), ChangeMutation::Given(mutation));
        self.change(cx, target, input)
    }

    /// Records a fresh effect's outcome in its intent.
    async fn record(
        &self,
        intent: &Intent,
        outcome: Result<String, CallEnd>,
    ) -> Result<String, HostError> {
        let run_id = self.scope.run.run_id;
        let record = RecordCall::new(run_id, call_intent(intent), outcome);
        drive(record, &self.ctx)
            .await
            .map_err(|failure| failure.error)
    }

    /// The error of a failed call; one that differs from an earlier attempt marks the run.
    fn failed(&self, failure: CallFailure) -> HostError {
        if failure.changed {
            self.changed.store(true, Ordering::Relaxed);
        }
        failure.error
    }

    /// The intent's earlier outcome, or `None` when the effect may be dispatched now.
    async fn begun(&self, intent: &Intent) -> Result<Option<String>, HostError> {
        let begun = drive(begin(self.scope.run.run_id, call_intent(intent)), &self.ctx).await;
        let begun = begun.map_err(|error| unavailable(error.0))?;
        begun_reply(begun).map_err(|failure| self.failed(failure))
    }

    /// Native work through the run's call history, with its time and id minted here.
    fn native_ask(&self, intent: &Intent, request: WorkRequest) -> NativeAsk {
        let minted = (unix_timestamp_millis(), Ulid::generate());
        NativeAsk {
            scope: self.scope.clone(),
            intent: call_intent(intent),
            request,
            place: (minted, self.ctx.node_and_realm()),
        }
    }

    /// Starts native work through the run's call history.
    async fn native(&self, intent: &Intent, request: WorkRequest) -> Result<String, HostError> {
        let ask = self.native_ask(intent, request);
        let call = NativeCall::new(&ask.scope, ask.intent, ask.request, ask.place);
        let started = drive(call, &self.ctx).await;
        started.map_err(|failure| self.failed(failure))
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
        Ok(drive(LocalObject::new(place, install, path), &self.ctx).await?)
    }
}

/// A key the guest may use: not empty, within its bound.
fn check_key(key: &str) -> Result<(), HostError> {
    match key.is_empty() || key.len() > MAX_STATE_KEY {
        true => Err(HostError::new(
            ErrorKind::Invalid,
            "a state key has 1 to 256 bytes",
        )),
        false => Ok(()),
    }
}

/// A byte bound as people read it: `1 MiB` rather than `1048576 bytes`.
fn byte_size(bytes: usize) -> String {
    match bytes % (1 << 20) {
        0 => format!("{} MiB", bytes >> 20),
        _ => format!("{bytes} bytes"),
    }
}

#[async_trait]
impl Broker for RunBroker {
    async fn metadata_get(&self, cx: &CallContext, resource: &str) -> Result<Document, HostError> {
        let read = ReadDataset::new(self.read_place(cx), resource, RoCrateExportView::Full);
        Ok(drive(read, &self.ctx).await?)
    }

    async fn metadata_summary(
        &self,
        cx: &CallContext,
        resource: &str,
    ) -> Result<Document, HostError> {
        let view = RoCrateExportView::Summary;
        let read = ReadDataset::new(self.read_place(cx), resource, view);
        Ok(drive(read, &self.ctx).await?)
    }

    async fn metadata_files(
        &self,
        cx: &CallContext,
        resource: &str,
        cursor: Option<String>,
    ) -> Result<FilePage, HostError> {
        let read = ReadFiles::new(self.read_place(cx), resource, cursor);
        Ok(drive(read, &self.ctx).await?)
    }

    async fn metadata_list(
        &self,
        _cx: &CallContext,
        cursor: Option<String>,
    ) -> Result<Listing, HostError> {
        Ok(drive(ListDatasets::new(&self.scope, cursor), &self.ctx).await?)
    }

    async fn metadata_patch(
        &self,
        cx: &CallContext,
        patch: MetadataPatch,
    ) -> Result<String, HostError> {
        let target = (patch.resource.as_str(), patch.expected.as_str(), &patch.intent);
        let input = (ChangeInput::Patch(patch.patch.clone()), ChangeMutation::Dataset);
        let change = self.change(cx, target, input)?;
        let written = drive(change, &self.ctx).await;
        written.map_err(|failure| self.failed(failure))
    }

    async fn metadata_create(
        &self,
        _cx: &CallContext,
        create: MetadataCreate,
    ) -> Result<Created, HostError> {
        let place = (create.path.as_str(), create.key.as_str());
        let intent = call_intent(&create.intent);
        let prepare = PrepareCreate::new(&self.scope, place, &create.dataset, intent)?;
        let plan = CreatePlan {
            network_node: self.ctx.net_handle.as_ref().map(|net| net.node_id()),
            key: create.key.clone(),
            run_id: self.scope.run.run_id,
            intent: call_intent(&create.intent),
        };
        let created = drive(Checked::new(prepare, plan), &self.ctx).await;
        stored_created(&created.map_err(|failure| self.failed(failure))?)
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
        let write = self.file_change(cx, &change, mutation, Some(file))?;
        let written = drive(write, &self.ctx).await;
        written.map_err(|failure| self.failed(failure))
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
        let write = self.file_change(cx, &change, mutation, None)?;
        let written = drive(write, &self.ctx).await;
        written.map_err(|failure| self.failed(failure))
    }

    async fn objects_info(
        &self,
        _cx: &CallContext,
        text: &str,
        version: Option<String>,
    ) -> Result<ObjectInfo, HostError> {
        let path = self.expand(text)?;
        let place = (self.scope.realm_id, self.scope.local);
        let ask = InfoAsk {
            place,
            path: path.clone(),
            version,
        };
        let info = Admitted::new(&self.scope, Some((Permission::ObjectsRead, &path)), ask);
        let info = drive(info, &self.ctx).await;
        info.map_err(|failure| self.failed(failure))
    }

    async fn objects_list(
        &self,
        _cx: &CallContext,
        prefix: &str,
        cursor: Option<String>,
    ) -> Result<ObjectPage, HostError> {
        let ask = ListAsk {
            place: (self.scope.realm_id, self.scope.local),
            group_id: self.scope.run.group_id,
            prefix: prefix.to_string(),
            cursor,
        };
        let page = drive(Admitted::new(&self.scope, None, ask), &self.ctx).await;
        page.map_err(|failure| self.failed(failure))
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
        let version = drive(Unmarked::new((&bucket, &key), version), &self.ctx).await?;
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
        let version = (copy.version.as_deref()).map(version_id).transpose()?;
        let request = CopyRequest {
            source: self.expand(&copy.source)?,
            source_version: version,
            target: self.expand(&copy.target)?,
            ordinal: copy.intent.ordinal,
        };
        let ask = self.native_ask(&copy.intent, WorkRequest::Copy(request));
        let started = drive(Admitted::new(&self.scope, None, ask), &self.ctx).await;
        started.map_err(|failure| self.failed(failure))
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
            method: aruna_plugin_types::calls::Method::Get,
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
        (self.native(&download.intent, WorkRequest::Download(request))).await
    }

    async fn jobs_upload(&self, _cx: &CallContext, upload: JobUpload) -> Result<String, HostError> {
        use aruna_plugin_types::calls::Method;
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
        (self.native(&upload.intent, WorkRequest::Upload(request))).await
    }

    async fn jobs_get(&self, _cx: &CallContext, id: &str) -> Result<JobStatus, HostError> {
        let install = self.admitted(None).await?;
        let place = (&install, self.scope.local);
        let mut status = drive(JobState::new(place, id), &self.ctx).await?;
        if status.reply_status.is_some() {
            let row = drive(JobRow::new(&install, id), &self.ctx).await?;
            let shown = match &row.context {
                Some(context) => self.reply_shown(context).await,
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
        let ask = OutputsAsk {
            local: self.scope.local,
            id: id.to_string(),
            cursor,
        };
        Ok(drive(Admitted::new(&self.scope, None, ask), &self.ctx).await?)
    }

    async fn jobs_cancel(
        &self,
        _cx: &CallContext,
        id: &str,
        _intent: Intent,
    ) -> Result<(), HostError> {
        // Asking again changes nothing, so a rerun cancels again instead of reading an intent.
        let cancel = CancelAsk { id: id.to_string() };
        Ok(drive(Admitted::new(&self.scope, None, cancel), &self.ctx).await?)
    }

    async fn state_get(&self, _cx: &CallContext, key: &str) -> Result<Option<Entry>, HostError> {
        check_key(key)?;
        let read = StateAsk {
            key: key.to_string(),
        };
        let entry = drive(Admitted::new(&self.scope, None, read), &self.ctx).await?;
        Ok(entry.map(|entry| Entry {
            version: entry.version,
            value: entry.value,
        }))
    }

    async fn state_put(&self, _cx: &CallContext, put: StatePut) -> Result<u64, HostError> {
        check_key(&put.key)?;
        let change = StateChange::Put(StateWrite {
            intent: call_intent(&put.intent),
            key: put.key,
            value: put.value,
            expected: put.expected,
        });
        let run_id = self.scope.run.run_id;
        let put = Admitted::new(&self.scope, None, ChangeAsk { run_id, change });
        let put = drive(put, &self.ctx).await;
        put.map_err(|failure| self.failed(failure))
    }

    async fn state_remove(&self, _cx: &CallContext, remove: StateRemove) -> Result<(), HostError> {
        check_key(&remove.key)?;
        let change = StateChange::Remove(StateRemoval {
            intent: call_intent(&remove.intent),
            key: remove.key,
            expected: remove.expected,
        });
        let run_id = self.scope.run.run_id;
        let remove = Admitted::new(&self.scope, None, ChangeAsk { run_id, change });
        let removed = drive(remove, &self.ctx).await;
        removed.map(drop).map_err(|failure| self.failed(failure))
    }

    async fn secret_value(&self, _cx: &CallContext, slot: &str) -> Result<Vec<Field>, HostError> {
        let call = drive(CheckConfigured::secret(&self.scope, slot), &self.ctx).await?;
        let use_of = (Delivery::Value, None);
        let (_, fields) = (self.obtain(&call.install, slot, use_of, &call.config)).await?;
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
        let admit = AdmitCall::new(&self.scope, None);
        if drive(admit, &self.ctx).await.is_ok() {
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

    /// Marks a call whose effect may have happened without a recorded reply; a failure leaves the
    /// intent pending, which a later attempt reads as unknown as well.
    async fn abandoned(&self, _cx: &CallContext, intent: &Intent) {
        let reason = "the call did not finish in time";
        if let Ok(row) = unknown_row(self.scope.run.run_id, &call_intent(intent), reason) {
            let _ = drive(write_rows(vec![row]), &self.ctx).await;
        }
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
            let send = SendConnection::new(ready.outgoing, ready.body);
            let answer = drive(send, &self.ctx).await.map_err(send_refusal)?;
            return Ok(whole(&ready.fields, &answer));
        };
        if let Some(reply) = self.begun(&intent).await? {
            return stored_whole(&reply);
        }
        let send = SendConnection::new(ready.outgoing, ready.body);
        let outcome = match drive(send, &self.ctx).await {
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
            Err(ConnectionError::TooLarge { status }) => Ok(too_large(status)),
            Err(ConnectionError::NotSent(why)) => Err(CallEnd::None(unavailable(why))),
            Err(ConnectionError::Unknown(why)) => Err(CallEnd::Maybe(why)),
        };
        stored_whole(&self.record(&intent, outcome).await?)
    }

    /// The per call check, the address below the connection, the guest's own headers, and the
    /// secret released and added; every use of a secret, a replay included, passes the check.
    async fn ready(&self, request: SendRequest) -> Result<Ready, HostError> {
        let pairs = |fields: &[Field]| -> Vec<(String, String)> {
            let pairs = fields
                .iter()
                .map(|field| (field.name.clone(), field.value.clone()));
            pairs.collect()
        };
        let ask = ConnectionAsk {
            connection: request.connection.clone(),
            method: declared_method(request.method),
            path: request.path.clone(),
            query: pairs(&request.query),
            headers: pairs(&request.headers),
        };
        let prepare = PrepareConnection::new(&self.scope, ask);
        let ConnectionPlan {
            call,
            context,
            mut outgoing,
            slot,
        } = drive(prepare, &self.ctx).await?;
        let (exposure, fields) = match &slot {
            None => (ResponseExposure::Full, None),
            Some(slot) => {
                let (released, fields) = (self.inject(&call, slot, &mut outgoing)).await?;
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
    async fn reply_shown(&self, context: &RequestContext) -> bool {
        let check = CheckReply::new(&self.scope, context);
        let Ok(Some(plan)) = drive(check, &self.ctx).await else {
            return false;
        };
        let ReplyPlan {
            call,
            mut outgoing,
            slot,
        } = plan;
        match slot {
            None => true,
            Some(slot) => {
                let released = self.inject(&call, &slot, &mut outgoing).await;
                released.is_ok_and(|(released, _)| released.exposure == ResponseExposure::Full)
            }
        }
    }

    /// Releases the slot's secret for this request and adds it as its kind says.
    async fn inject(
        &self,
        call: &ConfiguredCall,
        slot: &str,
        outgoing: &mut OutgoingRequest,
    ) -> Result<(Released, SecretFields), HostError> {
        let target = Some((outgoing.method.clone(), outgoing.url.to_string()));
        let use_of = (Delivery::Inject, target);
        let (released, fields) = (self.obtain(&call.install, slot, use_of, &call.config)).await?;
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
        config: &Arc<RealmConfigDocument>,
    ) -> Result<(Released, SecretFields), HostError> {
        let mut request = ReleaseRequest {
            install_id: install.install_id,
            slot: slot.to_string(),
            delivery,
            target,
            held: None,
            run_id: Some(self.scope.run.run_id),
        };
        let mut held = self.held.lock().await;
        held.prepare(&mut request);
        let ask = AskRelease::new(config.clone(), self.scope.local, request);
        let released = drive(ask, &self.ctx).await.map_err(|failure| failure.0);
        released
            .and_then(|released| held.absorb(slot, released))
            .map_err(|error| match error {
                ReleaseError::Refused(_) => HostError::new(ErrorKind::Denied, error.to_string()),
                ReleaseError::Unavailable(why) => unavailable(why),
            })
    }

    /// Text the run keeps with every form of a value handed to the code removed, as extra
    /// protection only: the code holds the value and may change it.
    pub fn scrub(&self, text: &str) -> String {
        let shown = self.shown.lock();
        let shown = shown.unwrap_or_else(|poisoned| poisoned.into_inner());
        (shown.iter()).fold(text.to_string(), |text, fields| scrub(&text, fields))
    }
}

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

/// The whole answer as the code may see it, with echoes of the secret removed.
fn whole(fields: &SecretFields, answer: &ConnectionAnswer) -> Response {
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

/// A send that has no stored intent: an answer that does not fit is a limit, others unavailable.
fn send_refusal(error: ConnectionError) -> HostError {
    match error {
        ConnectionError::TooLarge { .. } => HostError::new(ErrorKind::Limit, error.to_string()),
        error => unavailable(error.to_string()),
    }
}

#[cfg(test)]
#[path = "broker_tests.rs"]
mod tests;
