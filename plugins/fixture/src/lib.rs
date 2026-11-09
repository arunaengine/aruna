//! Test guest for the plugin runtime tests: probes host calls and exhausts each budget.
//! Rebuild with `aruna-cli plugin build plugins/fixture` and copy it to plugins/tests/fixture.wasm.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::hint::black_box;

use aruna_plugin::{Context, Error, Event, Level, Method, Result};
use serde_json::{Value, json};

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
            json!({ "status": reply.status, "json": reply.json })
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
