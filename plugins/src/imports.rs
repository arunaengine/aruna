//! Implements the guest-facing WIT imports on top of the run's grant and budget checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::sync::Arc;

use crate::broker::{
    Broker, CallContext, Created, Document, Entry, ErrorKind, Field, FullResponse, HostError,
    Intent, Listing, MetadataCreate, MetadataPatch, Method, ObjectInfo, ObjectPage, Response,
    SendRequest, StatePut,
};
use crate::host::wit::aruna::plugin::{
    connections, grants, log, metadata_create, metadata_read, metadata_write, objects_read,
    secrets, state, types,
};
use crate::host::{Needs, Session};
use crate::permission::Permission;

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
        Fut: Future<Output = Result<R, HostError>> + Send + 'static,
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
        self.finish(reply, size)
    }
}

fn unknown(message: &str) -> HostError {
    HostError::new(ErrorKind::UnknownOutcome, message)
}

fn fields_len(fields: &[Field]) -> usize {
    fields.iter().map(|f| f.name.len() + f.value.len()).sum()
}

fn field_bytes(fields: &[Field]) -> Vec<u8> {
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

impl grants::Host for Session {
    async fn granted(&mut self, permission: String, resource: String) -> wasmtime::Result<bool> {
        let size = permission.len() + resource.len();
        let Ok(permission) = permission.parse::<Permission>() else {
            self.count_call()?;
            self.charge(size)?;
            return Ok(false);
        };
        let reply = self
            .brokered(
                size,
                Effect::Read,
                Needs::Free,
                |broker, cx| async move { Ok(broker.granted(&cx, permission, &resource).await) },
                |_| 0,
            )
            .await?;
        Ok(reply.unwrap_or(false))
    }
}

impl metadata_read::Host for Session {
    async fn get(&mut self, resource: String) -> wasmtime::Result<Result<Document, HostError>> {
        self.brokered(
            resource.len(),
            Effect::Read,
            Needs::Permission(Permission::MetadataRead),
            |broker, cx| async move { broker.metadata_get(&cx, &resource).await },
            |doc: &Document| doc.resource.len() + doc.revision.len() + doc.json.len(),
        )
        .await
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
            |page: &Listing| {
                let names: usize = page.resources.iter().map(String::len).sum();
                names + page.cursor.as_ref().map_or(0, String::len)
            },
        )
        .await
    }
}

impl metadata_write::Host for Session {
    async fn patch(
        &mut self,
        resource: String,
        patch: String,
        expected: String,
        key: Option<String>,
    ) -> wasmtime::Result<Result<String, HostError>> {
        let size =
            resource.len() + patch.len() + expected.len() + key.as_ref().map_or(0, String::len);
        let parts: [&[u8]; 3] = [resource.as_bytes(), patch.as_bytes(), expected.as_bytes()];
        let intent = Intent::new(self.next_ordinal(), key, &parts);
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
}

impl metadata_create::Host for Session {
    async fn create(
        &mut self,
        path: String,
        dataset: String,
        key: String,
    ) -> wasmtime::Result<Result<Created, HostError>> {
        let size = path.len() + dataset.len() + key.len();
        let parts: [&[u8]; 3] = [path.as_bytes(), dataset.as_bytes(), key.as_bytes()];
        let intent = Intent::new(self.next_ordinal(), Some(key.clone()), &parts);
        let effect = Effect::Write(intent.clone());
        let create = MetadataCreate {
            path,
            dataset,
            key,
            intent,
        };
        self.brokered(
            size,
            effect,
            Needs::Permission(Permission::MetadataCreate),
            |broker, cx| async move { broker.metadata_create(&cx, create).await },
            |created: &Created| created.resource.len() + created.revision.len(),
        )
        .await
    }
}

impl objects_read::Host for Session {
    async fn info(&mut self, path: String) -> wasmtime::Result<Result<ObjectInfo, HostError>> {
        self.brokered(
            path.len(),
            Effect::Read,
            Needs::Permission(Permission::ObjectsRead),
            |broker, cx| async move { broker.objects_info(&cx, &path).await },
            |info: &ObjectInfo| info.path.len() + info.version.len() + 8,
        )
        .await
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
            |page: &ObjectPage| {
                let items: usize = page
                    .objects
                    .iter()
                    .map(|o| o.path.len() + o.version.len() + 8)
                    .sum();
                items + page.cursor.as_ref().map_or(0, String::len)
            },
        )
        .await
    }
}

impl connections::Host for Session {
    async fn send(
        &mut self,
        request: connections::Request,
    ) -> wasmtime::Result<Result<Response, HostError>> {
        let (size, effect, outbound) = self.outbound(request);
        let connection = outbound.connection.clone();
        self.brokered(
            size,
            effect,
            Needs::Connection(&connection),
            // The broker applies the exposure; the reply passes through as it answers.
            |broker, cx| async move { broker.connection_send(&cx, outbound).await },
            |reply: &Response| reply.json.as_ref().map_or(0, String::len) + 2,
        )
        .await
    }

    async fn send_full(
        &mut self,
        request: connections::Request,
    ) -> wasmtime::Result<Result<FullResponse, HostError>> {
        let (size, effect, outbound) = self.outbound(request);
        let connection = outbound.connection.clone();
        self.brokered(
            size,
            effect,
            Needs::Connection(&connection),
            |broker, cx| async move { broker.connection_send_full(&cx, outbound).await },
            |reply: &FullResponse| fields_len(&reply.headers) + reply.body.len() + 2,
        )
        .await
    }
}

impl Session {
    /// A connection request as the broker gets it, its size, and its intent when it may
    /// change remote state.
    fn outbound(&mut self, request: connections::Request) -> (usize, Effect, SendRequest) {
        let size = request.connection.len()
            + request.path.len()
            + fields_len(&request.query)
            + fields_len(&request.headers)
            + request.body.len()
            + request.key.as_ref().map_or(0, String::len);
        let mutating = !matches!(request.method, Method::Get | Method::Head);
        let intent = mutating.then(|| {
            let parts: [&[u8]; 6] = [
                request.connection.as_bytes(),
                method_name(request.method).as_bytes(),
                request.path.as_bytes(),
                &field_bytes(&request.query),
                &field_bytes(&request.headers),
                &request.body,
            ];
            Intent::new(self.next_ordinal(), request.key.clone(), &parts)
        });
        let effect = match &intent {
            Some(intent) => Effect::Write(intent.clone()),
            None => Effect::Read,
        };
        let outbound = SendRequest {
            connection: request.connection,
            method: request.method,
            path: request.path,
            query: request.query,
            headers: request.headers,
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
            |entry: &Option<Entry>| entry.as_ref().map_or(0, |e| e.value.len() + 8),
        )
        .await
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
        let intent = Intent::new(self.next_ordinal(), None, &parts);
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
}

impl secrets::Host for Session {
    async fn value(&mut self, slot: String) -> wasmtime::Result<Result<Vec<Field>, HostError>> {
        self.brokered(
            slot.len(),
            Effect::Read,
            Needs::Permission(Permission::SecretsValue),
            |broker, cx| async move { broker.secret_value(&cx, &slot).await },
            |fields| fields_len(fields),
        )
        .await
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
