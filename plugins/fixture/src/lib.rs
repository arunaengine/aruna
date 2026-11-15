//! Test guest for the plugin runtime tests: probes host calls and exhausts each budget.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Rebuild: RUSTFLAGS="--remap-path-prefix=$HOME=~" aruna-cli plugin build plugins/fixture, then
// copy target/wasm32-unknown-unknown/release/fixture-0.2.0.wasm to plugins/tests/fixture.wasm.

use std::hint::black_box;

use aruna_plugin::bindings::aruna::plugin::types::HostError;
use aruna_plugin::bindings::aruna::plugin::{
    jobs, metadata_create, metadata_read, metadata_write, objects_read, state,
};
use aruna_plugin::{Error, Form, Level, Method, Result, fail, run};
use serde_json::{Value, json};

mod wasi {
    wit_bindgen::generate!({ path: "wit", world: "aruna-test:wasi/probe", generate_all });
}

/// A metadata read as the probes answer it: the revision and the dataset entity.
fn document(read: std::result::Result<metadata_read::Document, HostError>) -> Result<Value> {
    let doc = read?;
    let json: Value = serde_json::from_str(&doc.json)?;
    Ok(json!({ "revision": doc.revision, "json": json }))
}

/// Runs the host call named by the manual input and returns what it answered. The probes call
/// the host interface itself, as a plugin may without the library.
fn probe(form: Form) -> Result<Value> {
    let input = Value::Object(form.fields().clone());
    let text = |name: &str| {
        input
            .get(name)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let json_of = |name: &str| input.get(name).cloned().unwrap_or(Value::Null).to_string();
    let number = |name: &str| input.get(name).and_then(Value::as_u64).unwrap_or_default();
    let call = text("call");
    Ok(match call.as_str() {
        "settings" => aruna_plugin::settings::<Value>()?,
        "metadata_get" => document(metadata_read::get(&text("resource")))?,
        "metadata_patch" => {
            let (resource, patch) = (text("resource"), json_of("patch"));
            let first = metadata_write::patch(&resource, &patch, &text("expected"))?;
            let second = metadata_write::patch(&resource, &patch, &first)?;
            json!([first, second])
        }
        "metadata_create" => {
            let created = metadata_create::create(&text("path"), &json_of("dataset"))?;
            json!({ "resource": created.resource, "revision": created.revision })
        }
        "metadata_summary" => document(metadata_read::summary(&text("resource")))?,
        "metadata_files" => {
            let cursor = input.get("cursor").and_then(Value::as_str);
            let page = metadata_read::files(&text("resource"), cursor)?;
            let files = (page.files.iter())
                .map(|file| serde_json::from_str(file))
                .collect::<std::result::Result<Vec<Value>, _>>()?;
            json!({ "files": files, "cursor": page.cursor })
        }
        "put_file" => {
            let (resource, file) = (text("resource"), json_of("file"));
            json!(metadata_write::put_file(
                &resource,
                &file,
                &text("expected")
            )?)
        }
        "remove_file" => {
            let (resource, id, expected) = (text("resource"), text("id"), text("expected"));
            json!(metadata_write::remove_file(&resource, &id, &expected)?)
        }
        "read" => {
            let version = input.get("version").and_then(Value::as_str);
            json!(String::from_utf8_lossy(&objects_read::read(
                &text("path"),
                version
            )?))
        }
        "objects_info" => {
            let version = input.get("version").and_then(Value::as_str);
            let info = objects_read::info(&text("path"), version)?;
            json!({ "path": info.path, "size": info.size, "version": info.version })
        }
        "send" => {
            let method = match text("method").as_str() {
                "POST" => Method::Post,
                _ => Method::Get,
            };
            let mut request = aruna_plugin::service(&text("connection"))
                .request(method)
                .path(&text("path"))
                .check(false);
            if let Some(body) = input.get("body") {
                request = request.json(body);
            }
            let reply = request.send()?;
            // A status-only answer has no body; it reads as no fields.
            let fields = reply.json::<Value>().unwrap_or_else(|_| json!({}));
            json!({ "status": reply.status, "fields": fields })
        }
        "copy" => {
            let version = input.get("version").and_then(Value::as_str);
            json!(jobs::copy(&text("source"), version, &text("target"))?)
        }
        "state_remove" => {
            let key = text("key");
            let version = state::put(&key, b"v", None)?;
            state::remove(&key, version)?;
            json!(state::get(&key)?.is_none())
        }
        "cancel" => {
            jobs::cancel(&text("id"))?;
            json!(true)
        }
        "copy_mapped" => {
            // A copy, then the mapping its `job_finished` run reads, as a plugin keeps it.
            let id = jobs::copy(&text("source"), None, &text("target"))?;
            state::put(&format!("job/{id}"), b"mapped", None)?;
            json!(id)
        }
        "job" => {
            let status = jobs::get(&text("id"))?;
            let page = jobs::outputs(&text("id"), None)?;
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
                written.map_err(|_| fail("the stream refused"))?;
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
                metadata_read::get("slow")?;
            }
            for _ in 0..number("logs") {
                aruna_plugin::log(Level::Debug, "after the fill");
            }
            json!(buffer[0])
        }
        "chatty" => {
            // Logs until the host call budget ends the run.
            for line in 0..u32::MAX {
                aruna_plugin::log(Level::Debug, &format!("line {line}"));
            }
            json!(null)
        }
        "errors" => {
            let mut failed = 0;
            for _ in 0..1000 {
                failed += u32::from(metadata_read::get("missing").is_err());
            }
            json!(failed)
        }
        "state" => {
            let version = state::put("counter", b"1", None)?;
            let entry = state::get("counter")?;
            json!({ "version": version, "stored": entry.map(|e| e.version) })
        }
        other => return Err(Error::InvalidInput(format!("unknown call `{other}`"))),
    })
}

/// A finished job with the mapping the run that started it kept under `job/<id>`.
fn finished() -> Result<Value> {
    let change = run()
        .event
        .ok_or_else(|| Error::InvalidInput("job_finished needs a job".into()))?;
    let mapping = state::get(&format!("job/{}", change.resource))?;
    let mapping = mapping.map(|entry| String::from_utf8_lossy(&entry.value).into_owned());
    Ok(json!({ "job": change.resource, "state": change.revision, "mapping": mapping }))
}

/// Returns the run identity, settings and the event it was called with.
fn echo() -> Result<String> {
    let run = run();
    let change = run
        .event
        .ok_or_else(|| Error::InvalidInput("echo needs an event".into()))?;
    let trigger = format!(
        "change {} {} {}",
        change.kind, change.resource, change.revision
    );
    let settings = aruna_plugin::settings::<Value>()?;
    Ok(format!(
        "{} {} {} {settings} {trigger}",
        run.id, run.install, run.group
    ))
}

/// Allocates until the memory budget ends the run.
fn hoard() -> Result<()> {
    let mut blocks = Vec::new();
    loop {
        blocks.push(black_box(vec![1u8; 1 << 20]));
    }
}

/// Computes until the fuel budget ends the run.
fn spin() -> Result<()> {
    let mut value = 0u64;
    loop {
        value = black_box(value.wrapping_add(1));
    }
}

aruna_plugin::plugin! {
    manual => probe,
    timer => spin,
    dataset_created => echo,
    dataset_updated => echo,
    data_uploaded => hoard,
    sync_completed => echo,
    sync_failed => echo,
    job_finished => finished,
}
