//! Serves one run's host calls on the receiver: every call passes the per call check, and a
//! mutating call records its intent before its effect. Objects answer info and listings only.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_clock::unix_timestamp_millis;
use aruna_core::injection::scrub;
use aruna_core::metadata::api::{RoCrateExportView, UpdateDocumentMutation};
use aruna_core::plugin_runs::{LogLine, PluginRun, StateRemoval, StateWrite};
use aruna_core::secrets::SecretFields;
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::execution::job::input::UploadForm;
use aruna_drivers::driver::{DriverContext, drive};
use aruna_operations::jobs::records::pages::ServingNode;
use aruna_operations::plugins::admitted::{
    Admitted, CancelAsk, ChangeAsk, InfoAsk, ListAsk, NativeAsk, OutputsAsk, StateAsk,
};
use aruna_operations::plugins::check::RunScope;
use aruna_operations::plugins::creates::{CreatePlan, stored_created};
use aruna_operations::plugins::dataset::file_entity;
use aruna_operations::plugins::grants::object_path;
use aruna_operations::plugins::held::SecretValue;
use aruna_operations::plugins::host::calls::{CallFailure, StateChange, call_intent, version_id};
use aruna_operations::plugins::host::steps::{
    AdmitCall, ChangeInput, ChangeMutation, ChangePlan, Checked, ConnectionAsk, ListDatasets,
    PrepareChange, PrepareCreate, ReadDataset, ReadFiles, ReadPlace,
};
use aruna_operations::plugins::intents::unknown_row;
use aruna_operations::plugins::job_work::ReadJob;
use aruna_operations::plugins::objects::ReadObject;
use aruna_operations::plugins::outgoing::declared_method;
use aruna_operations::plugins::rows::write_rows;
use aruna_operations::plugins::send_calls::{SendCall, Transfer, TransferCall};
use aruna_operations::plugins::state::MAX_STATE_KEY;
use aruna_operations::plugins::submit::{CopyRequest, WorkRequest};
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
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use ulid::Ulid;

/// The host side of one run.
pub struct RunBroker {
    ctx: Arc<DriverContext>,
    /// This broker's id, which keys the secrets it holds apart from other attempts of the run.
    holder: Ulid,
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
            holder: Ulid::generate(),
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

    /// Whether the run called differently than an earlier attempt, so it fails as changed.
    pub fn changed(&self) -> bool {
        self.changed.load(Ordering::Relaxed)
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
        let target = (
            change.resource.as_str(),
            change.expected.as_str(),
            &change.intent,
        );
        let input = (ChangeInput::File(file), ChangeMutation::Given(mutation));
        self.change(cx, target, input)
    }

    /// The error of a failed call; one that differs from an earlier attempt marks the run.
    fn failed(&self, failure: CallFailure) -> HostError {
        if failure.changed {
            self.changed.store(true, Ordering::Relaxed);
        }
        failure.error
    }

    /// The time and id native work is minted with here, and the node that serves it.
    fn minted(&self) -> ((u64, Ulid), ServingNode) {
        let minted = (unix_timestamp_millis(), Ulid::generate());
        (minted, self.ctx.node_and_realm())
    }

    /// Native work through the run's call history.
    fn native_ask(&self, intent: &Intent, request: WorkRequest) -> NativeAsk {
        NativeAsk {
            scope: self.scope.clone(),
            intent: call_intent(intent),
            request,
            place: self.minted(),
        }
    }

    /// The checked parts of a connection request the guest sends or hands to a transfer.
    fn ask(request: &SendRequest) -> ConnectionAsk {
        let pairs = |fields: &[Field]| -> Vec<(String, String)> {
            let pairs = fields
                .iter()
                .map(|field| (field.name.clone(), field.value.clone()));
            pairs.collect()
        };
        ConnectionAsk {
            connection: request.connection.clone(),
            method: declared_method(request.method),
            path: request.path.clone(),
            query: pairs(&request.query),
            headers: pairs(&request.headers),
        }
    }

    /// The full permission path of an object path the plugin gave; see [`object_path`].
    fn expand(&self, text: &str) -> Result<String, HostError> {
        let place = (self.scope.realm_id, self.scope.local);
        object_path(place, self.scope.run.group_id, text)
    }
}

/// The secrets this broker held stay in the plugin host state only while it lives.
impl Drop for RunBroker {
    fn drop(&mut self) {
        let holder = (self.scope.run.run_id.into(), self.holder.into());
        aruna_plugins::host_state::forget_held(holder);
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
        let target = (
            patch.resource.as_str(),
            patch.expected.as_str(),
            &patch.intent,
        );
        let input = (
            ChangeInput::Patch(patch.patch.clone()),
            ChangeMutation::Dataset,
        );
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
        let read = ReadObject::new(&self.scope, &path, version, cx.max_bytes);
        Ok(drive(read, &self.ctx).await?.0)
    }

    async fn connection_send(
        &self,
        cx: &CallContext,
        request: SendRequest,
    ) -> Result<Response, HostError> {
        let ask = Self::ask(&request);
        let parts = (request.body, request.intent.as_ref().map(call_intent));
        let send = SendCall::new((&self.scope, self.holder), ask, parts, cx.max_bytes);
        let answer = drive(send, &self.ctx).await;
        answer.map_err(|failure| self.failed(failure))
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
        let transfer = Transfer::Download {
            target: self.expand(&download.target)?,
            checksums: expected(&download.checksums)?,
            max_bytes: download.size_limit,
            ordinal: download.intent.ordinal,
        };
        let request = SendRequest {
            connection: download.service,
            method: aruna_plugin_types::calls::Method::Get,
            path: download.path,
            query: download.query,
            headers: download.headers,
            body: Vec::new(),
            intent: None,
        };
        let parts = (transfer, call_intent(&download.intent));
        let held_by = (&self.scope, self.holder);
        let call = TransferCall::new(held_by, Self::ask(&request), parts, self.minted());
        let started = drive(call, &self.ctx).await;
        started.map_err(|failure| self.failed(failure))
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
        let file_name = source.rsplit('/').next().unwrap_or_default().to_string();
        let form = upload.file_field.map(|file_field| UploadForm {
            file_field,
            file_name,
            fields: (upload.fields.into_iter())
                .map(|field| (field.name, field.value))
                .collect(),
        });
        let transfer = Transfer::Upload {
            source,
            version,
            form,
            ordinal: upload.intent.ordinal,
        };
        // The same checks as a request the run sends itself, with its secret released.
        let parts = (transfer, call_intent(&upload.intent));
        let held_by = (&self.scope, self.holder);
        let call = TransferCall::new(held_by, Self::ask(&request), parts, self.minted());
        let started = drive(call, &self.ctx).await;
        started.map_err(|failure| self.failed(failure))
    }

    async fn jobs_get(&self, _cx: &CallContext, id: &str) -> Result<JobStatus, HostError> {
        let read = ReadJob::new((&self.scope, self.holder), id);
        Ok(drive(read, &self.ctx).await?)
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
        let value = SecretValue::new((&self.scope, self.holder), slot);
        let fields = drive(value, &self.ctx).await?;
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

impl RunBroker {
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

#[cfg(test)]
#[path = "broker_tests.rs"]
mod tests;
