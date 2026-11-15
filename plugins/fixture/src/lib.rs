//! Test guest for the plugin runtime tests: probes host calls and exhausts each budget.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Rebuild: RUSTFLAGS="--remap-path-prefix=$HOME=~" aruna-cli plugin build plugins/fixture, then
// copy target/wasm32-unknown-unknown/release/fixture-0.2.0.wasm to plugins/tests/fixture.wasm.

use std::hint::black_box;

use aruna_plugin::{Context, Error, Event, Level, Method, Result};
use serde_json::{Value, json};

use aruna_plugin::bindings::aruna::plugin::jobs;

/// A host error of a call the SDK does not wrap yet.
fn host(error: aruna_plugin::bindings::aruna::plugin::types::HostError) -> Error {
    Error::Host {
        kind: error.kind,
        message: error.message,
    }
}

mod wasi {
    wit_bindgen::generate!({ path: "wit", world: "aruna-test:wasi/probe", generate_all });
}

/// Runs the host call named by the manual input and returns what it answered.
async fn probe(ctx: Context, event: Event) -> Result<Value> {
    let Event::Manual(input) = event else {
        return Err(Error::Input("probe needs manual input".into()));
    };
    let text = |name: &str| {
        input
            .get(name)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let number = |name: &str| input.get(name).and_then(Value::as_u64).unwrap_or_default();
    let call = text("call");
    Ok(match call.as_str() {
        "settings" => ctx.settings::<Value>()?,
        "metadata_get" => {
            let doc = ctx.metadata().get(&text("resource")).await?;
            json!({ "revision": doc.revision, "json": doc.json })
        }
        "metadata_patch" => {
            let patch = input.get("patch").cloned().unwrap_or(Value::Null);
            let metadata = ctx.metadata();
            let first = metadata
                .patch(&text("resource"), &patch, &text("expected"))
                .await?;
            let second = metadata.patch(&text("resource"), &patch, &first).await?;
            json!([first, second])
        }
        "metadata_create" => {
            let dataset = input.get("dataset").cloned().unwrap_or(Value::Null);
            let metadata = ctx.metadata();
            let created = metadata.create(&text("path"), &dataset).await?;
            json!({ "resource": created.resource, "revision": created.revision })
        }
        "metadata_summary" => {
            let doc = ctx.metadata().summary(&text("resource")).await?;
            json!({ "revision": doc.revision, "json": doc.json })
        }
        "metadata_files" => {
            let cursor = input.get("cursor").and_then(Value::as_str);
            let page = ctx.metadata().files(&text("resource"), cursor).await?;
            json!({ "files": page.files, "cursor": page.cursor })
        }
        "put_file" => {
            let file = input.get("file").cloned().unwrap_or(Value::Null);
            let metadata = ctx.metadata();
            json!(
                metadata
                    .put_file(&text("resource"), &file, &text("expected"))
                    .await?
            )
        }
        "remove_file" => {
            let (resource, id, expected) = (text("resource"), text("id"), text("expected"));
            let metadata = ctx.metadata();
            json!(metadata.remove_file(&resource, &id, &expected).await?)
        }
        "read" => {
            let version = input.get("version").and_then(Value::as_str);
            let read =
                aruna_plugin::bindings::aruna::plugin::objects_read::read(&text("path"), version);
            json!(String::from_utf8_lossy(&read.map_err(host)?))
        }
        "objects_info" => {
            let info = ctx.objects().info(&text("path")).await?;
            json!({ "path": info.path, "size": info.size, "version": info.version })
        }
        "send" => {
            let method = match text("method").as_str() {
                "POST" => Method::Post,
                _ => Method::Get,
            };
            let body = input
                .get("body")
                .map(serde_json::to_vec)
                .transpose()?
                .unwrap_or_default();
            let reply = ctx
                .http(&text("connection"))
                .send(method, &text("path"), body)
                .await?;
            // A status-only answer has no body; it reads as no fields.
            let fields = reply.json::<Value>().unwrap_or_else(|_| json!({}));
            json!({ "status": reply.status, "fields": fields })
        }
        "copy" => {
            let version = input.get("version").and_then(Value::as_str);
            let copied = jobs::copy(&text("source"), version, &text("target"));
            json!(copied.map_err(host)?)
        }
        "state_remove" => {
            let key = text("key");
            let state = ctx.state();
            let version = state.put(&key, b"v".to_vec(), None).await?;
            aruna_plugin::bindings::aruna::plugin::state::remove(&key, version).map_err(host)?;
            json!(state.get(&key).await?.is_none())
        }
        "cancel" => {
            jobs::cancel(&text("id")).map_err(host)?;
            json!(true)
        }
        "copy_mapped" => {
            // A copy, then the mapping its `job_finished` run reads, as a plugin keeps it.
            let id = jobs::copy(&text("source"), None, &text("target")).map_err(host)?;
            let key = format!("job/{id}");
            ctx.state().put(&key, b"mapped".to_vec(), None).await?;
            json!(id)
        }
        "job" => {
            let status = jobs::get(&text("id")).map_err(host)?;
            let page = jobs::outputs(&text("id"), None).map_err(host)?;
            let outputs: Vec<_> = page.objects.into_iter().map(|object| object.path).collect();
            json!({ "kind": status.kind, "state": status.state, "path": status.path,
                "bytes": status.bytes, "outputs": outputs })
        }
        "random" => {
            let len = input.get("len").and_then(Value::as_u64).unwrap_or_default();
            json!(wasi::wasi::random::random::get_random_bytes(len).len())
        }
        "clock" => json!(wasi::wasi::clocks::monotonic_clock::now() > 0),
        "write" | "zeroes" => {
            let out = wasi::wasi::cli::stdout::get_stdout();
            let size = number("size") as usize;
            let chunk = vec![b'x'; size];
            for _ in 0..number("times") {
                let written = match call.as_str() {
                    "write" => out.blocking_write_and_flush(&chunk),
                    _ => out.write_zeroes(size as u64),
                };
                written.map_err(|_| Error::failed("the stream refused"))?;
            }
            json!(true)
        }
        "poll" => {
            // The stream must outlive its pollable, so it is held until the end.
            let out = wasi::wasi::cli::stdout::get_stdout();
            let pollable = out.subscribe();
            let list = vec![&pollable; number("size") as usize];
            json!(wasi::wasi::io::poll::poll(&list).len())
        }
        "fill" => {
            let mut buffer = vec![0u8; number("size") as usize];
            black_box(&mut buffer).fill(1);
            let mut value = 0u64;
            for _ in 0..number("spin") {
                value = black_box(value.wrapping_add(1));
            }
            if input.get("slow").is_some() {
                ctx.metadata().get("slow").await?;
            }
            for _ in 0..number("logs") {
                ctx.log(Level::Debug, "after the fill");
            }
            json!(buffer[0])
        }
        "chatty" => {
            // Logs until the host call budget ends the run.
            for line in 0..u32::MAX {
                ctx.log(Level::Debug, &format!("line {line}"));
            }
            json!(null)
        }
        "errors" => {
            let mut failed = 0;
            for _ in 0..1000 {
                failed += u32::from(ctx.metadata().get("missing").await.is_err());
            }
            json!(failed)
        }
        "state" => {
            let state = ctx.state();
            let version = state.put("counter", b"1".to_vec(), None).await?;
            let entry = state.get("counter").await?;
            json!({ "version": version, "stored": entry.map(|e| e.version) })
        }
        other => return Err(Error::Input(format!("unknown call `{other}`"))),
    })
}

/// Returns the run identity, settings and trigger it was called with.
/// A finished job with the mapping the run that started it kept under `job/<id>`.
async fn finished(ctx: Context, event: Event) -> Result<Value> {
    let Event::Change(change) = event else {
        return Err(Error::Input("job_finished needs a job".into()));
    };
    let mapping = ctx.state().get(&format!("job/{}", change.resource)).await?;
    let mapping = mapping.map(|entry| String::from_utf8_lossy(&entry.value).into_owned());
    Ok(json!({ "job": change.resource, "state": change.revision, "mapping": mapping }))
}

async fn echo(ctx: Context, event: Event) -> Result<String> {
    let trigger = match event {
        Event::Change(change) => format!(
            "change {} {} {}",
            change.kind, change.resource, change.revision
        ),
        Event::Manual(input) => format!("manual {input}"),
        Event::Webhook { delivery_id, .. } => format!("webhook {delivery_id:?}"),
        Event::Timer(tick) => format!("timer {tick}"),
    };
    let settings = ctx.settings::<Value>()?;
    Ok(format!(
        "{} {} {} {settings} {trigger}",
        ctx.run(),
        ctx.install(),
        ctx.group()
    ))
}

/// Allocates until the memory budget ends the run.
async fn hoard(_: Context, _: ()) -> Result<()> {
    let mut blocks = Vec::new();
    loop {
        blocks.push(black_box(vec![1u8; 1 << 20]));
    }
}

/// Computes until the fuel budget ends the run.
async fn spin(_: Context, _: ()) -> Result<()> {
    let mut value = 0u64;
    loop {
        value = black_box(value.wrapping_add(1));
    }
}

aruna_plugin::export! {
    manual => probe,
    timer => spin,
    dataset_created => echo,
    dataset_updated => echo,
    data_uploaded => hoard,
    sync_completed => echo,
    sync_failed => echo,
    job_finished => finished,
}
