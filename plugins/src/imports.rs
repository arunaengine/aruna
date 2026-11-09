//! Implements the guest-facing WIT imports on top of the run's grant and budget checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::sync::Arc;

use wasmtime::component::{Accessor, HasSelf};

use crate::broker::{
    Broker, CallContext, Document, Entry, ErrorKind, Field, HostError, Intent, Listing,
    MetadataPatch, Method, ObjectInfo, ObjectPage, Response, SendRequest, StatePut,
};
use crate::host::wit::aruna::plugin::{
    connections, grants, log, metadata_read, metadata_write, objects_read, state, types,
};
use crate::host::{Needs, Session};
use crate::permission::Permission;

const MAX_LOG: usize = 1024;

/// Whether a broker call may be dropped with its guest or must finish recording.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Effect {
    Read,
    Write,
}

/// Runs one broker call between the checks of `begin` and `finish`. Writes run as tasks
/// tracked by the node and the run, so no guest end interrupts recording their outcome.
async fn brokered<T: Send, R, F, Fut>(
    accessor: &Accessor<T, HasSelf<Session>>,
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
    let started = match accessor.with(|mut access| access.get().begin(size, needs))? {
        Ok(started) => started,
        Err(error) => return Ok(Err(error)),
    };
    let future = call(started.broker, started.cx);
    let reply = match effect {
        Effect::Read => future.await,
        Effect::Write => started
            .tracker
            .spawn(started.writes.track_future(future))
            .await
            .unwrap_or_else(|_| {
                Err(HostError::new(
                    ErrorKind::UnknownOutcome,
                    "the host call ended without a result",
                ))
            }),
    };
    let size = reply.as_ref().map_or(0, reply_size);
    accessor.with(|mut access| access.get().finish(size))?;
    Ok(reply)
}

fn ordinal<T: Send>(accessor: &Accessor<T, HasSelf<Session>>) -> u32 {
    accessor.with(|mut access| access.get().next_ordinal())
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

impl<T: Send> grants::HostWithStore<T> for HasSelf<Session> {
    async fn granted(
        accessor: &Accessor<T, Self>,
        permission: String,
        resource: String,
    ) -> wasmtime::Result<bool> {
        let size = permission.len() + resource.len();
        let Ok(permission) = permission.parse::<Permission>() else {
            accessor.with(|mut access| access.get().charge(size))?;
            return Ok(false);
        };
        let reply = brokered(
            accessor,
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

impl<T: Send> metadata_read::HostWithStore<T> for HasSelf<Session> {
    async fn get(
        accessor: &Accessor<T, Self>,
        resource: String,
    ) -> wasmtime::Result<Result<Document, HostError>> {
        brokered(
            accessor,
            resource.len(),
            Effect::Read,
            Needs::Permission(Permission::MetadataRead),
            |broker, cx| async move { broker.metadata_get(&cx, &resource).await },
            |doc: &Document| doc.resource.len() + doc.revision.len() + doc.json.len(),
        )
        .await
    }

    async fn list(
        accessor: &Accessor<T, Self>,
        cursor: Option<String>,
    ) -> wasmtime::Result<Result<Listing, HostError>> {
        brokered(
            accessor,
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

impl<T: Send> metadata_write::HostWithStore<T> for HasSelf<Session> {
    async fn patch(
        accessor: &Accessor<T, Self>,
        resource: String,
        patch: String,
        expected: String,
        key: Option<String>,
    ) -> wasmtime::Result<Result<String, HostError>> {
        let size =
            resource.len() + patch.len() + expected.len() + key.as_ref().map_or(0, String::len);
        let parts: [&[u8]; 3] = [resource.as_bytes(), patch.as_bytes(), expected.as_bytes()];
        let intent = Intent::new(ordinal(accessor), key, &parts);
        let patch = MetadataPatch {
            resource,
            patch,
            expected,
            intent,
        };
        brokered(
            accessor,
            size,
            Effect::Write,
            Needs::Permission(Permission::MetadataWrite),
            |broker, cx| async move { broker.metadata_patch(&cx, patch).await },
            String::len,
        )
        .await
    }
}

impl<T: Send> objects_read::HostWithStore<T> for HasSelf<Session> {
    async fn info(
        accessor: &Accessor<T, Self>,
        path: String,
    ) -> wasmtime::Result<Result<ObjectInfo, HostError>> {
        brokered(
            accessor,
            path.len(),
            Effect::Read,
            Needs::Permission(Permission::ObjectsRead),
            |broker, cx| async move { broker.objects_info(&cx, &path).await },
            |info: &ObjectInfo| info.path.len() + info.version.len() + 8,
        )
        .await
    }

    async fn list(
        accessor: &Accessor<T, Self>,
        prefix: String,
        cursor: Option<String>,
    ) -> wasmtime::Result<Result<ObjectPage, HostError>> {
        let size = prefix.len() + cursor.as_ref().map_or(0, String::len);
        brokered(
            accessor,
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

impl<T: Send> connections::HostWithStore<T> for HasSelf<Session> {
    async fn send(
        accessor: &Accessor<T, Self>,
        request: connections::Request,
    ) -> wasmtime::Result<Result<Response, HostError>> {
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
            Intent::new(ordinal(accessor), request.key.clone(), &parts)
        });
        let connection = request.connection.clone();
        let outbound = SendRequest {
            connection: request.connection,
            method: request.method,
            path: request.path,
            query: request.query,
            headers: request.headers,
            body: request.body,
            intent,
        };
        let effect = if mutating {
            Effect::Write
        } else {
            Effect::Read
        };
        brokered(
            accessor,
            size,
            effect,
            Needs::Connection(&connection),
            |broker, cx| async move { broker.connection_send(&cx, outbound).await },
            |reply: &Response| reply.json.as_ref().map_or(0, String::len) + 2,
        )
        .await
    }
}

impl<T: Send> state::HostWithStore<T> for HasSelf<Session> {
    async fn get(
        accessor: &Accessor<T, Self>,
        key: String,
    ) -> wasmtime::Result<Result<Option<Entry>, HostError>> {
        brokered(
            accessor,
            key.len(),
            Effect::Read,
            Needs::Free,
            |broker, cx| async move { broker.state_get(&cx, &key).await },
            |entry: &Option<Entry>| entry.as_ref().map_or(0, |e| e.value.len() + 8),
        )
        .await
    }

    async fn put(
        accessor: &Accessor<T, Self>,
        key: String,
        value: Vec<u8>,
        expected: Option<u64>,
    ) -> wasmtime::Result<Result<u64, HostError>> {
        let size = key.len() + value.len();
        let put = StatePut {
            key,
            value,
            expected,
        };
        brokered(
            accessor,
            size,
            Effect::Write,
            Needs::Free,
            |broker, cx| async move { broker.state_put(&cx, put).await },
            |_| 8,
        )
        .await
    }
}

impl log::Host for Session {
    fn log(&mut self, level: log::Level, message: String) -> wasmtime::Result<()> {
        self.charge(message.len())?;
        let mut end = message.len().min(MAX_LOG);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        let text = &message[..end];
        match level {
            log::Level::Debug => tracing::debug!(target: "aruna_plugins::guest", "{text}"),
            log::Level::Info => tracing::info!(target: "aruna_plugins::guest", "{text}"),
            log::Level::Warn => tracing::warn!(target: "aruna_plugins::guest", "{text}"),
            log::Level::Error => tracing::error!(target: "aruna_plugins::guest", "{text}"),
        }
        Ok(())
    }
}

impl types::Host for Session {}
impl grants::Host for Session {}
impl metadata_read::Host for Session {}
impl metadata_write::Host for Session {}
impl objects_read::Host for Session {}
impl connections::Host for Session {}
impl state::Host for Session {}
