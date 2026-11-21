//! Implements the guest-facing WIT imports on top of the run's grant and budget checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::sync::Arc;

use aruna_plugin_types::calls::{self, Intent};
use aruna_plugin_types::permission::Permission;

use crate::broker::{
    Broker, CallContext, FileChange, JobCopy, JobDownload, JobTask, JobUpload, MetadataCreate,
    MetadataPatch, SendRequest, StatePut, StateRemove,
};
use crate::host::wit::aruna::plugin::jobs::Status as JobStatus;
use crate::host::wit::aruna::plugin::metadata_create::Created;
use crate::host::wit::aruna::plugin::metadata_read::{Document, FilePage, Listing};
use crate::host::wit::aruna::plugin::objects_read::{ObjectInfo, ObjectPage};
use crate::host::wit::aruna::plugin::services::{Method, Response};
use crate::host::wit::aruna::plugin::state::Entry;
use crate::host::wit::aruna::plugin::types::{Field, HostError};
use crate::host::wit::aruna::plugin::{
    jobs, log, metadata_create, metadata_read, metadata_write, objects_read, secrets, services,
    state, types,
};
use crate::host::{Needs, Session};
use crate::values::{guest_fields, node_fields};

const MAX_LOG: usize = 1024;

/// Whether a broker call may be dropped with its guest or must finish recording.
enum Effect {
    Read,
    Write(Intent),
}

impl Session {
    /// Runs one broker call between the checks of `begin` and `finish`. Writes run as tasks
    /// tracked by the node and the run, so no guest end interrupts recording their outcome,
    /// and each has a deadline after which the broker records the outcome as unknown.
    async fn brokered<R, F, Fut>(
        &mut self,
        size: usize,
        effect: Effect,
        needs: Needs<'_>,
        call: F,
        reply_size: impl FnOnce(&R) -> usize,
    ) -> wasmtime::Result<Result<R, HostError>>
    where
        F: FnOnce(Arc<dyn Broker>, CallContext) -> Fut,
        Fut: Future<Output = Result<R, calls::HostError>> + Send + 'static,
        R: Send + 'static,
    {
        let started = match self.begin(size, needs)? {
            Ok(started) => started,
            Err(error) => return Ok(Err(error)),
        };
        let (broker, cx, deadline) = (started.broker.clone(), started.cx.clone(), started.deadline);
        let future = call(started.broker, started.cx);
        let reply = match effect {
            Effect::Read => future.await,
            Effect::Write(intent) => {
                let bounded = async move {
                    match tokio::time::timeout(deadline, future).await {
                        Ok(reply) => reply,
                        Err(_) => {
                            // The effect may have happened; the begun intent stays unknown if this fails too.
                            let _ = tokio::time::timeout(deadline, broker.abandoned(&cx, &intent))
                                .await;
                            Err(unknown("the host call did not finish in time"))
                        }
                    }
                };
                (started
                    .tracker
                    .spawn(started.writes.track_future(bounded))
                    .await)
                    .unwrap_or_else(|_| Err(unknown("the host call ended without a result")))
            }
        };
        let size = match &reply {
            Ok(reply) => reply_size(reply),
            Err(error) => error.message.len(),
        };
        self.finish(reply.map_err(HostError::from), size)
    }
}

fn unknown(message: &str) -> calls::HostError {
    calls::HostError::new(calls::ErrorKind::UnknownOutcome, message)
}

fn fields_len(fields: &[calls::Field]) -> usize {
    fields.iter().map(|f| f.name.len() + f.value.len()).sum()
}

fn field_bytes(fields: &[calls::Field]) -> Vec<u8> {
    let mut out = Vec::with_capacity(fields_len(fields) + fields.len() * 16);
    for field in fields {
        for part in [&field.name, &field.value] {
            out.extend_from_slice(&(part.len() as u64).to_le_bytes());
            out.extend_from_slice(part.as_bytes());
        }
    }
    out
}

fn method_name(method: Method) -> &'static str {
    match method {
        Method::Get => "GET",
        Method::Head => "HEAD",
        Method::Post => "POST",
        Method::Put => "PUT",
        Method::Patch => "PATCH",
        Method::Delete => "DELETE",
    }
}

impl metadata_read::Host for Session {
    async fn get(&mut self, resource: String) -> wasmtime::Result<Result<Document, HostError>> {
        self.brokered(
            resource.len(),
            Effect::Read,
            Needs::Permission(Permission::MetadataRead),
            |broker, cx| async move { broker.metadata_get(&cx, &resource).await },
            |doc: &calls::Document| doc.resource.len() + doc.revision.len() + doc.json.len(),
        )
        .await
        .map(|reply| reply.map(Document::from))
    }

    async fn list(
        &mut self,
        cursor: Option<String>,
    ) -> wasmtime::Result<Result<Listing, HostError>> {
        self.brokered(
            cursor.as_ref().map_or(0, String::len),
            Effect::Read,
            Needs::Permission(Permission::MetadataRead),
            |broker, cx| async move { broker.metadata_list(&cx, cursor).await },
            |page: &calls::Listing| {
                let names: usize = page.resources.iter().map(String::len).sum();
                names + page.cursor.as_ref().map_or(0, String::len)
            },
        )
        .await
        .map(|reply| reply.map(Listing::from))
    }

    async fn files(
        &mut self,
        resource: String,
        cursor: Option<String>,
    ) -> wasmtime::Result<Result<FilePage, HostError>> {
        self.brokered(
            resource.len() + cursor.as_ref().map_or(0, String::len),
            Effect::Read,
            Needs::Permission(Permission::MetadataRead),
            |broker, cx| async move { broker.metadata_files(&cx, &resource, cursor).await },
            |page: &calls::FilePage| {
                let files: usize = page.files.iter().map(|file| file.len() + 8).sum();
                files + page.cursor.as_ref().map_or(0, String::len)
            },
        )
        .await
        .map(|reply| reply.map(FilePage::from))
    }

    async fn summary(&mut self, resource: String) -> wasmtime::Result<Result<Document, HostError>> {
        self.brokered(
            resource.len(),
            Effect::Read,
            Needs::Permission(Permission::MetadataRead),
            |broker, cx| async move { broker.metadata_summary(&cx, &resource).await },
            |doc: &calls::Document| doc.resource.len() + doc.revision.len() + doc.json.len(),
        )
        .await
        .map(|reply| reply.map(Document::from))
    }
}

impl metadata_write::Host for Session {
    async fn patch(
        &mut self,
        resource: String,
        patch: String,
        expected: String,
    ) -> wasmtime::Result<Result<String, HostError>> {
        let size = resource.len() + patch.len() + expected.len();
        let parts: [&[u8]; 3] = [resource.as_bytes(), patch.as_bytes(), expected.as_bytes()];
        let intent = Intent::new(self.next_ordinal(), &parts);
        let effect = Effect::Write(intent.clone());
        let patch = MetadataPatch {
            resource,
            patch,
            expected,
            intent,
        };
        self.brokered(
            size,
            effect,
            Needs::Permission(Permission::MetadataWrite),
            |broker, cx| async move { broker.metadata_patch(&cx, patch).await },
            String::len,
        )
        .await
    }

    async fn put_file(
        &mut self,
        resource: String,
        file: String,
        expected: String,
    ) -> wasmtime::Result<Result<String, HostError>> {
        let size = resource.len() + file.len() + expected.len();
        let parts: [&[u8]; 4] = [
            b"put",
            resource.as_bytes(),
            file.as_bytes(),
            expected.as_bytes(),
        ];
        let intent = Intent::new(self.next_ordinal(), &parts);
        let effect = Effect::Write(intent.clone());
        let change = FileChange {
            resource,
            file,
            expected,
            intent,
        };
        self.brokered(
            size,
            effect,
            Needs::Permission(Permission::MetadataWrite),
            |broker, cx| async move { broker.metadata_put_file(&cx, change).await },
            String::len,
        )
        .await
    }

    async fn remove_file(
        &mut self,
        resource: String,
        id: String,
        expected: String,
    ) -> wasmtime::Result<Result<String, HostError>> {
        let size = resource.len() + id.len() + expected.len();
        let parts: [&[u8]; 4] = [
            b"remove",
            resource.as_bytes(),
            id.as_bytes(),
            expected.as_bytes(),
        ];
        let intent = Intent::new(self.next_ordinal(), &parts);
        let effect = Effect::Write(intent.clone());
        let change = FileChange {
            resource,
            file: id,
            expected,
            intent,
        };
        self.brokered(
            size,
            effect,
            Needs::Permission(Permission::MetadataWrite),
            |broker, cx| async move { broker.metadata_remove_file(&cx, change).await },
            String::len,
        )
        .await
    }
}

impl metadata_create::Host for Session {
    async fn create(
        &mut self,
        path: String,
        dataset: String,
    ) -> wasmtime::Result<Result<Created, HostError>> {
        let size = path.len() + dataset.len();
        let parts: [&[u8]; 2] = [path.as_bytes(), dataset.as_bytes()];
        let intent = Intent::new(self.next_ordinal(), &parts);
        let effect = Effect::Write(intent.clone());
        // One path names one document, so the path is the create's key across runs.
        let create = MetadataCreate {
            key: path.clone(),
            path,
            dataset,
            intent,
        };
        self.brokered(
            size,
            effect,
            Needs::Permission(Permission::MetadataCreate),
            |broker, cx| async move { broker.metadata_create(&cx, create).await },
            |created: &calls::Created| created.resource.len() + created.revision.len(),
        )
        .await
        .map(|reply| reply.map(Created::from))
    }
}

impl objects_read::Host for Session {
    async fn read(
        &mut self,
        path: String,
        version: Option<String>,
    ) -> wasmtime::Result<Result<Vec<u8>, HostError>> {
        let size = path.len() + version.as_ref().map_or(0, String::len);
        self.brokered(
            size,
            Effect::Read,
            Needs::Permission(Permission::ObjectsRead),
            |broker, cx| async move { broker.objects_read(&cx, &path, version).await },
            Vec::len,
        )
        .await
    }

    async fn info(
        &mut self,
        path: String,
        version: Option<String>,
    ) -> wasmtime::Result<Result<ObjectInfo, HostError>> {
        let size = path.len() + version.as_ref().map_or(0, String::len);
        self.brokered(
            size,
            Effect::Read,
            Needs::Permission(Permission::ObjectsRead),
            |broker, cx| async move { broker.objects_info(&cx, &path, version).await },
            |info: &calls::ObjectInfo| info.path.len() + info.version.len() + 8,
        )
        .await
        .map(|reply| reply.map(ObjectInfo::from))
    }

    async fn list(
        &mut self,
        prefix: String,
        cursor: Option<String>,
    ) -> wasmtime::Result<Result<ObjectPage, HostError>> {
        let size = prefix.len() + cursor.as_ref().map_or(0, String::len);
        self.brokered(
            size,
            Effect::Read,
            Needs::Permission(Permission::ObjectsRead),
            |broker, cx| async move { broker.objects_list(&cx, &prefix, cursor).await },
            page_size,
        )
        .await
        .map(|reply| reply.map(ObjectPage::from))
    }
}

impl jobs::Host for Session {
    async fn copy(
        &mut self,
        source: String,
        version: Option<String>,
        target: String,
    ) -> wasmtime::Result<Result<String, HostError>> {
        let size = source.len() + version.as_ref().map_or(0, String::len) + target.len();
        let pinned = version.as_deref().unwrap_or_default().as_bytes();
        let parts: [&[u8]; 4] = [b"copy", source.as_bytes(), pinned, target.as_bytes()];
        let intent = Intent::new(self.next_ordinal(), &parts);
        let effect = Effect::Write(intent.clone());
        let copy = JobCopy {
            source,
            version,
            target,
            intent,
        };
        self.brokered(
            size,
            effect,
            Needs::Permission(Permission::ObjectsCopy),
            |broker, cx| async move { broker.jobs_copy(&cx, copy).await },
            String::len,
        )
        .await
    }

    async fn download(
        &mut self,
        request: jobs::DownloadRequest,
    ) -> wasmtime::Result<Result<String, HostError>> {
        let query = node_fields(request.query);
        let headers = node_fields(request.headers);
        let checksums = node_fields(request.checksums);
        let size = request.service.len()
            + request.path.len()
            + fields_len(&query)
            + fields_len(&headers)
            + request.target.len()
            + fields_len(&checksums);
        let limit = request.size_limit.map(u64::to_le_bytes).unwrap_or_default();
        let parts: [&[u8]; 8] = [
            b"download",
            request.service.as_bytes(),
            request.path.as_bytes(),
            &field_bytes(&query),
            &field_bytes(&headers),
            request.target.as_bytes(),
            &limit,
            &field_bytes(&checksums),
        ];
        let intent = Intent::new(self.next_ordinal(), &parts);
        let effect = Effect::Write(intent.clone());
        let service = request.service.clone();
        let download = JobDownload {
            service: request.service,
            path: request.path,
            query,
            headers,
            target: request.target,
            size_limit: request.size_limit,
            checksums,
            intent,
        };
        self.brokered(
            size,
            effect,
            Needs::Transfer(Permission::TransfersDownload, &service),
            |broker, cx| async move { broker.jobs_download(&cx, download).await },
            String::len,
        )
        .await
    }

    async fn upload(
        &mut self,
        request: jobs::UploadRequest,
    ) -> wasmtime::Result<Result<String, HostError>> {
        let version = request.version.as_deref().unwrap_or_default();
        let file_field = request.file_field.as_deref().unwrap_or_default();
        let (query, headers) = (node_fields(request.query), node_fields(request.headers));
        let fields = node_fields(request.fields);
        let size = request.service.len()
            + request.path.len()
            + fields_len(&query)
            + fields_len(&headers)
            + request.source.len()
            + version.len()
            + file_field.len()
            + fields_len(&fields);
        let method = format!("{:?}", request.method);
        let parts: [&[u8]; 10] = [
            b"upload",
            request.service.as_bytes(),
            method.as_bytes(),
            request.path.as_bytes(),
            &field_bytes(&query),
            &field_bytes(&headers),
            request.source.as_bytes(),
            version.as_bytes(),
            file_field.as_bytes(),
            &field_bytes(&fields),
        ];
        let intent = Intent::new(self.next_ordinal(), &parts);
        let effect = Effect::Write(intent.clone());
        let service = request.service.clone();
        let upload = JobUpload {
            service: request.service,
            method: request.method.into(),
            path: request.path,
            query,
            headers,
            source: request.source,
            version: request.version,
            file_field: request.file_field,
            fields,
            intent,
        };
        self.brokered(
            size,
            effect,
            Needs::Transfer(Permission::TransfersUpload, &service),
            |broker, cx| async move { broker.jobs_upload(&cx, upload).await },
            String::len,
        )
        .await
    }

    async fn start_task(
        &mut self,
        request: jobs::TaskRequest,
    ) -> wasmtime::Result<Result<String, HostError>> {
        let inputs: Vec<(String, String, Option<String>)> = (request.inputs.into_iter())
            .map(|input| (input.path, input.source, input.version))
            .collect();
        let files: Vec<(String, Vec<u8>)> = (request.files.into_iter())
            .map(|file| (file.path, file.content))
            .collect();
        let input_bytes: Vec<u8> = (inputs.iter())
            .flat_map(|(path, source, version)| {
                let version = version.as_deref().unwrap_or_default();
                let parts = [path.as_bytes(), source.as_bytes(), version.as_bytes()];
                parts.map(|part| [part, b"\0"].concat()).concat()
            })
            .collect();
        let file_bytes: Vec<u8> = (files.iter())
            .flat_map(|(path, content)| {
                let length = (content.len() as u64).to_le_bytes();
                [path.as_bytes(), b"\0", &length, content].concat()
            })
            .collect();
        let (parameters, outputs) = (
            node_fields(request.parameters),
            node_fields(request.outputs),
        );
        let size = request.task.len()
            + fields_len(&parameters)
            + input_bytes.len()
            + file_bytes.len()
            + fields_len(&outputs);
        let parts: [&[u8]; 6] = [
            b"task",
            request.task.as_bytes(),
            &field_bytes(&parameters),
            &input_bytes,
            &file_bytes,
            &field_bytes(&outputs),
        ];
        let intent = Intent::new(self.next_ordinal(), &parts);
        let effect = Effect::Write(intent.clone());
        let task = JobTask {
            task: request.task,
            parameters,
            inputs,
            files,
            outputs,
            intent,
        };
        self.brokered(
            size,
            effect,
            Needs::Permission(Permission::JobsRun),
            |broker, cx| async move { broker.jobs_start_task(&cx, task).await },
            String::len,
        )
        .await
    }

    async fn get(&mut self, id: String) -> wasmtime::Result<Result<JobStatus, HostError>> {
        self.brokered(
            id.len(),
            Effect::Read,
            Needs::Free,
            |broker, cx| async move { broker.jobs_get(&cx, &id).await },
            status_size,
        )
        .await
        .map(|reply| reply.map(JobStatus::from))
    }

    async fn outputs(
        &mut self,
        id: String,
        cursor: Option<String>,
    ) -> wasmtime::Result<Result<ObjectPage, HostError>> {
        let size = id.len() + cursor.as_ref().map_or(0, String::len);
        self.brokered(
            size,
            Effect::Read,
            Needs::Free,
            |broker, cx| async move { broker.jobs_outputs(&cx, &id, cursor).await },
            page_size,
        )
        .await
        .map(|reply| reply.map(ObjectPage::from))
    }

    /// A cancel changes the job, so it takes its ordinal in the run.
    async fn cancel(&mut self, id: String) -> wasmtime::Result<Result<(), HostError>> {
        let parts: [&[u8]; 2] = [b"cancel", id.as_bytes()];
        let intent = Intent::new(self.next_ordinal(), &parts);
        self.brokered(
            id.len(),
            Effect::Write(intent.clone()),
            Needs::Free,
            |broker, cx| async move { broker.jobs_cancel(&cx, &id, intent).await },
            |_| 0,
        )
        .await
    }
}

fn status_size(status: &calls::JobStatus) -> usize {
    let texts = [&status.id, &status.kind, &status.path, &status.state];
    let optional = [&status.message, &status.version, &status.report];
    let body = status.reply_body.as_ref().map_or(0, Vec::len);
    let fixed: usize = texts.iter().map(|text| text.len()).sum();
    let rest: usize = optional
        .iter()
        .map(|text| text.as_ref().map_or(0, String::len))
        .sum();
    fixed + rest + body + fields_len(&status.reply_headers) + 16
}

fn page_size(page: &calls::ObjectPage) -> usize {
    let items: usize = (page.objects.iter())
        .map(|object| object.path.len() + object.version.len() + 8)
        .sum();
    items + page.cursor.as_ref().map_or(0, String::len)
}

impl services::Host for Session {
    async fn send(
        &mut self,
        request: services::Request,
    ) -> wasmtime::Result<Result<Response, HostError>> {
        let (size, effect, outbound) = self.outbound(request);
        let connection = outbound.connection.clone();
        self.brokered(
            size,
            effect,
            Needs::Connection(&connection),
            // The broker applies the exposure; the reply passes through as it answers.
            |broker, cx| async move { broker.connection_send(&cx, outbound).await },
            |reply: &calls::Response| fields_len(&reply.headers) + reply.body.len() + 2,
        )
        .await
        .map(|reply| reply.map(Response::from))
    }
}

impl Session {
    /// A service request as the broker gets it, its size, and its intent when it may change
    /// remote state.
    fn outbound(&mut self, request: services::Request) -> (usize, Effect, SendRequest) {
        let (query, headers) = (node_fields(request.query), node_fields(request.headers));
        let size = request.service.len()
            + request.path.len()
            + fields_len(&query)
            + fields_len(&headers)
            + request.body.len();
        let mutating = !matches!(request.method, Method::Get | Method::Head);
        let intent = mutating.then(|| {
            let parts: [&[u8]; 6] = [
                request.service.as_bytes(),
                method_name(request.method).as_bytes(),
                request.path.as_bytes(),
                &field_bytes(&query),
                &field_bytes(&headers),
                &request.body,
            ];
            Intent::new(self.next_ordinal(), &parts)
        });
        let effect = match &intent {
            Some(intent) => Effect::Write(intent.clone()),
            None => Effect::Read,
        };
        let outbound = SendRequest {
            connection: request.service,
            method: request.method.into(),
            path: request.path,
            query,
            headers,
            body: request.body,
            intent,
        };
        (size, effect, outbound)
    }
}

impl state::Host for Session {
    async fn get(&mut self, key: String) -> wasmtime::Result<Result<Option<Entry>, HostError>> {
        self.brokered(
            key.len(),
            Effect::Read,
            Needs::Free,
            |broker, cx| async move { broker.state_get(&cx, &key).await },
            |entry: &Option<calls::Entry>| entry.as_ref().map_or(0, |e| e.value.len() + 8),
        )
        .await
        .map(|reply| reply.map(|entry| entry.map(Entry::from)))
    }

    async fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
        expected: Option<u64>,
    ) -> wasmtime::Result<Result<u64, HostError>> {
        let size = key.len() + value.len();
        let version = expected.map(u64::to_le_bytes);
        let parts: [&[u8]; 3] = [
            key.as_bytes(),
            &value,
            version.as_ref().map_or(&[][..], |v| &v[..]),
        ];
        let intent = Intent::new(self.next_ordinal(), &parts);
        let effect = Effect::Write(intent.clone());
        let put = StatePut {
            key,
            value,
            expected,
            intent,
        };
        self.brokered(
            size,
            effect,
            Needs::Free,
            |broker, cx| async move { broker.state_put(&cx, put).await },
            |_| 8,
        )
        .await
    }

    async fn remove(
        &mut self,
        key: String,
        expected: u64,
    ) -> wasmtime::Result<Result<(), HostError>> {
        let version = expected.to_le_bytes();
        let parts: [&[u8]; 3] = [b"remove", key.as_bytes(), &version];
        let intent = Intent::new(self.next_ordinal(), &parts);
        let size = key.len();
        let remove = StateRemove {
            key,
            expected,
            intent: intent.clone(),
        };
        self.brokered(
            size,
            Effect::Write(intent),
            Needs::Free,
            |broker, cx| async move { broker.state_remove(&cx, remove).await },
            |_| 0,
        )
        .await
    }
}

impl secrets::Host for Session {
    async fn value(&mut self, slot: String) -> wasmtime::Result<Result<Vec<Field>, HostError>> {
        self.brokered(
            slot.len(),
            Effect::Read,
            Needs::Permission(Permission::SecretsValue),
            |broker, cx| async move { broker.secret_value(&cx, &slot).await },
            |fields: &Vec<calls::Field>| fields_len(fields),
        )
        .await
        .map(|reply| reply.map(guest_fields))
    }
}

impl log::Host for Session {
    async fn log(&mut self, level: log::Level, message: String) -> wasmtime::Result<()> {
        self.count_call()?;
        self.charge(message.len())?;
        let mut end = message.len().min(MAX_LOG);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        let text = &message[..end];
        let name = match level {
            log::Level::Debug => "debug",
            log::Level::Info => "info",
            log::Level::Warn => "warn",
            log::Level::Error => "error",
        };
        match level {
            log::Level::Debug => tracing::debug!(target: "aruna_plugins::guest", "{text}"),
            log::Level::Info => tracing::info!(target: "aruna_plugins::guest", "{text}"),
            log::Level::Warn => tracing::warn!(target: "aruna_plugins::guest", "{text}"),
            log::Level::Error => tracing::error!(target: "aruna_plugins::guest", "{text}"),
        }
        let (broker, cx) = self.log_target();
        broker.log(&cx, name, text).await;
        Ok(())
    }
}

impl types::Host for Session {}
