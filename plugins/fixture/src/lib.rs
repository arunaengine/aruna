//! Test guest for the plugin runtime tests: probes host calls and exhausts each budget.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Rebuild: RUSTFLAGS="--remap-path-prefix=$HOME=~" aruna-cli plugin build plugins/fixture, then
// copy target/wasm32-unknown-unknown/release/fixture-0.2.0.wasm to plugins/tests/fixture.wasm.

use std::hint::black_box;

use aruna_plugin::{Context, Error, Event, Level, Method, Result};
use serde_json::{Value, json};

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
        "granted" => json!(ctx.granted(&text("permission"), &text("resource")).await),
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
            let created = metadata
                .create(&text("path"), &dataset, &text("key"))
                .await?;
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
                    .put_file(&text("resource"), &file, &text("expected"), None)
                    .await?
            )
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
                .send(method, &text("path"), body, None)
                .await?;
            json!({ "status": reply.status, "fields": reply.fields })
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

/// Logs until the host call budget ends the run.
async fn chatty(ctx: Context, _: ()) -> Result<()> {
    for line in 0..u32::MAX {
        ctx.log(Level::Debug, &format!("line {line}"));
    }
    Ok(())
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

aruna_plugin::export!(probe, echo, chatty, hoard, spin);
