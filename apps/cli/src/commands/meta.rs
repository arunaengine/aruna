//! Creates, imports, edits, validates, lists, finds and exports RO-Crate metadata documents.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::jobs::wait_job;
use super::{Context, resolve_group};
use crate::args::{MetaCommand, MetaCreateArgs, MetaImportArgs, MetaLinkArgs};
use crate::client::{Api, read_json};
use crate::error::CliError;
use crate::remote;
use crate::rocrate::{self, load_crate};
use reqwest::Method;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::Path;

pub async fn meta(context: &mut Context<'_>, command: MetaCommand) -> Result<(), CliError> {
    let api = context.api()?;
    match command {
        MetaCommand::Create(args) => create(context, &api, args).await,
        MetaCommand::Import(args) => import(context, &api, args).await,
        MetaCommand::Update {
            id,
            source,
            visibility,
            message,
        } => {
            let rocrate = load_crate(&source)?;
            let body =
                json!({ "rocrate": rocrate, "public": visibility.choice(), "message": message });
            let answer: Value = api
                .send(Method::PUT, &format!("metadata/{id}/rocrate"), &body)
                .await?;
            report(context, &answer, &format!("Updated {id}."))
        }
        MetaCommand::Edit { id, message } => edit(context, &api, &id, message).await,
        MetaCommand::Validate {
            source,
            group,
            profile,
            public,
        } => {
            let mut rocrate = load_crate(&source)?;
            if let Some(profile) = profile {
                rocrate::add_profile(&mut rocrate, &profile)?;
            }
            let mut body = json!({ "rocrate": rocrate, "public": public });
            if let Some(group) = group {
                body["group_id"] = resolve_group(&api, &group).await?.into();
            }
            let answer: Value = api
                .send(Method::POST, "metadata/profile/validation/preview", &body)
                .await?;
            print_verdict(context, &answer)?;
            match answer["accepted"].as_bool() {
                Some(true) => Ok(()),
                _ => Err(CliError::usage("the node would refuse this crate")),
            }
        }
        MetaCommand::Ls {
            group,
            public,
            limit,
            offset,
        } => {
            let path = match group {
                Some(group) => format!("metadata/groups/{}", resolve_group(&api, &group).await?),
                None => "metadata".to_string(),
            };
            let query = [
                ("order", "recent".to_string()),
                ("include", "summary".to_string()),
                ("limit", limit.to_string()),
                ("offset", offset.to_string()),
            ];
            // Without the token the node lists exactly what the public may read.
            let reader = match public {
                true => api.anonymous(),
                false => api,
            };
            let page: Value = reader.get(&path, &query).await?;
            let documents = page["documents"].as_array().cloned().unwrap_or_default();
            let rows = documents.iter().map(document_row).collect();
            context.out.listing(
                &page,
                &["DOCUMENT", "NAME", "PATH", "PUBLIC", "UPDATED"],
                rows,
            )
        }
        MetaCommand::Search {
            query,
            group,
            conforms_to,
            limit,
            cursor,
        } => {
            let mut params = vec![("q", query), ("limit", limit.to_string())];
            if let Some(group) = group {
                params.push(("group_id", resolve_group(&api, &group).await?));
            }
            if let Some(conforms_to) = conforms_to {
                params.push(("conforms_to", conforms_to));
            }
            if let Some(cursor) = cursor {
                params.push(("cursor", cursor));
            }
            let page: Value = api.get("metadata/search", &params).await?;
            if let Some(failed) = page["nodes_failed"].as_u64().filter(|failed| *failed > 0) {
                eprintln!("warning: {failed} nodes did not answer; hits may be missing");
            }
            let hits = page["hits"].as_array().cloned().unwrap_or_default();
            let rows = hits
                .iter()
                .map(|hit| {
                    ["title", "document_id", "document_path", "group_id"]
                        .iter()
                        .map(|field| hit[field].as_str().unwrap_or_default().to_string())
                        .collect()
                })
                .collect();
            context
                .out
                .listing(&page, &["TITLE", "DOCUMENT", "PATH", "GROUP"], rows)?;
            if let (false, Some(next)) = (context.out.json, page["next_cursor"].as_str()) {
                context.out.line(format!("More hits: --cursor {next}"))?;
            }
            Ok(())
        }
        MetaCommand::Show { id } => {
            let summary: Value = api.get(&format!("metadata/{id}"), &[]).await?;
            if context.out.json {
                return context.out.json(&summary);
            }
            let rows = [
                "document_id",
                "group_id",
                "document_path",
                "graph_iri",
                "public",
                "replicas",
                "created_at",
                "updated_at",
                "revision",
            ]
            .iter()
            .map(|field| vec![field.to_string(), text(&summary[field])])
            .collect::<Vec<_>>();
            context.out.table(&["", ""], &rows)
        }
        MetaCommand::Export { id, output } => {
            let rocrate = export_crate(&api, &id).await?;
            let text = serde_json::to_string_pretty(&rocrate)?;
            match output {
                Some(path) => {
                    std::fs::write(&path, format!("{text}\n"))?;
                    eprintln!("Wrote {}.", path.display());
                    Ok(())
                }
                None => context.out.line(text),
            }
        }
        MetaCommand::Link(args) => link(context, &api, args).await,
    }
}

async fn create(
    context: &mut Context<'_>,
    api: &Api,
    args: MetaCreateArgs,
) -> Result<(), CliError> {
    let rocrate = load_crate(&args.source)?;
    let path = match args.path {
        Some(path) => path,
        None => rocrate::default_path(&rocrate)?,
    };
    let group_id = resolve_group(api, &args.group).await?;
    let mut body = json!({
        "group_id": group_id,
        "path": path,
        "public": args.public,
        "rocrate": rocrate,
    });
    if let Some(message) = args.message {
        body["message"] = message.into();
    }
    let created: Value = api.send(Method::POST, "metadata", &body).await?;
    let visibility = match args.public {
        true => "public",
        false => "private",
    };
    let line = format!(
        "Created {} at {path} ({visibility}); search and export follow in a moment.",
        text(&created["document_id"])
    );
    report(context, &created, &line)
}

#[derive(Debug, Deserialize)]
struct Upload {
    upload_id: String,
}

async fn import(
    context: &mut Context<'_>,
    api: &Api,
    args: MetaImportArgs,
) -> Result<(), CliError> {
    let packed = tempfile::Builder::new().suffix(".zip").tempfile()?;
    let archive = match args.source.is_dir() {
        true => {
            let files = rocrate::pack_folder(&args.source, packed.path())?;
            eprintln!("Packed {files} files from {}.", args.source.display());
            packed.path().to_path_buf()
        }
        false if rocrate::is_archive(&args.source)? => args.source.clone(),
        false => {
            return Err(CliError::usage(
                "meta import takes a crate folder or a zip archive; use meta create for a file",
            ));
        }
    };
    let rocrate = load_crate(&archive)?;
    let path = match args.path {
        Some(path) => path,
        None => rocrate::default_path(&rocrate)?,
    };
    let group_id = resolve_group(api, &args.group).await?;
    let upload = upload_archive(api, &archive).await?;
    let body = json!({
        "source": { "kind": "upload", "upload_id": upload.upload_id },
        "target": { "bucket": args.bucket, "prefix": args.prefix },
        "metadata": { "group_id": group_id, "path": path, "public": args.public },
        "idempotency_key": upload.upload_id,
    });
    let job: Value = api
        .send(Method::POST, "metadata/rocrate/imports", &body)
        .await?;
    if !args.wait {
        let line = format!(
            "Import job {} started; follow it with --wait or at {}",
            text(&job["job_id"]),
            text(&job["status_url"])
        );
        return report(context, &job, &line);
    }
    let status_url = job["status_url"]
        .as_str()
        .ok_or_else(|| CliError::usage("the node returned no status URL"))?;
    let status = wait_job(api, status_url).await?;
    match status["state"].as_str() {
        Some("succeeded") => report(context, &status, "Import finished."),
        _ => Err(CliError::usage(format!(
            "the import job ended as {}: {}",
            text(&status["state"]),
            status["error"]
        ))),
    }
}

async fn upload_archive(api: &Api, archive: &Path) -> Result<Upload, CliError> {
    let file = tokio::fs::File::open(archive).await?;
    let size = file.metadata().await?.len();
    let response = api
        .request(Method::POST, "metadata/rocrate/uploads")?
        .header(CONTENT_TYPE, "application/zip")
        .header(reqwest::header::CONTENT_LENGTH, size)
        .body(reqwest::Body::from(file))
        .send()
        .await?;
    read_json(response).await
}

async fn edit(
    context: &mut Context<'_>,
    api: &Api,
    id: &str,
    message: Option<String>,
) -> Result<(), CliError> {
    let summary: Value = api.get(&format!("metadata/{id}"), &[]).await?;
    let rocrate = export_crate(api, id).await?;
    let file = tempfile::Builder::new()
        .prefix("ro-crate-")
        .suffix(".json")
        .tempfile()?;
    let before = serde_json::to_string_pretty(&rocrate)?;
    std::fs::write(file.path(), &before)?;
    let editor = std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| "vi".to_string());
    let mut words = editor.split_whitespace();
    let program = words.next().unwrap_or("vi");
    let status = std::process::Command::new(program)
        .args(words)
        .arg(file.path())
        .status()
        .map_err(|error| CliError::usage(format!("cannot start editor {editor}: {error}")))?;
    if !status.success() {
        return Err(CliError::usage("the editor failed; nothing was saved"));
    }
    let after = std::fs::read_to_string(file.path())?;
    if after == before {
        return context.out.line("No change.");
    }
    let edited: Value = serde_json::from_str(&after)
        .map_err(|error| CliError::usage(format!("the edited crate is not JSON: {error}")))?;
    let body = json!({
        "rocrate": edited,
        "expected_revision": summary["revision"],
        "message": message,
    });
    let answer: Value = api
        .send(Method::PUT, &format!("metadata/{id}/rocrate"), &body)
        .await?;
    report(context, &answer, &format!("Saved {id}."))
}

/// The bare RO-Crate document, which the node sends for an `application/ld+json` request.
async fn export_crate(api: &Api, id: &str) -> Result<Value, CliError> {
    let response = api
        .request(Method::GET, &format!("metadata/{id}/rocrate"))?
        .header(ACCEPT, "application/ld+json")
        .send()
        .await?;
    let value: Value = read_json(response).await?;
    // A node that answers with the wrapper object still carries the crate inside it.
    Ok(match value.get("rocrate") {
        Some(inner) if value.get("@graph").is_none() => inner.clone(),
        _ => value,
    })
}

async fn link(context: &mut Context<'_>, api: &Api, args: MetaLinkArgs) -> Result<(), CliError> {
    let target = remote::RemotePath::parse(&args.bucket)?;
    if !target.key.is_empty() {
        return Err(CliError::usage(
            "the API describes whole buckets only; pass a bucket without a folder",
        ));
    }
    let (_, node_api) = remote::locate(api, &target.bucket, context.node.as_deref()).await?;
    let body = json!({
        "title": args.title,
        "description": args.description,
        "license": args.license,
        "public": args.public,
        "path": args.path,
    });
    let answer: Value = node_api
        .send(
            Method::POST,
            &format!("data/buckets/{}/dataset", target.bucket),
            &body,
        )
        .await?;
    let line = format!(
        "Created dataset {} for bucket {}.",
        text(&answer["dataset"]),
        target.bucket
    );
    report(context, &answer, &line)
}

fn print_verdict(context: &mut Context<'_>, answer: &Value) -> Result<(), CliError> {
    if context.out.json {
        return context.out.json(answer);
    }
    let verdict = match answer["accepted"].as_bool() {
        Some(true) => "accepted",
        _ => "refused",
    };
    context.out.line(format!(
        "Verdict: {verdict} ({}), Profile {}",
        text(&answer["state"]),
        answer["profile_iri"].as_str().unwrap_or("none")
    ))?;
    let mut rows: Vec<Vec<String>> = Vec::new();
    for violation in answer["structural_violations"]
        .as_array()
        .into_iter()
        .flatten()
    {
        rows.push(vec![
            "structure".into(),
            text(&violation["code"]),
            text(&violation["pointer"]),
            text(&violation["message"]),
        ]);
    }
    for finding in answer["findings"].as_array().into_iter().flatten() {
        rows.push(vec![
            text(&finding["severity"]),
            text(&finding["code"]),
            text(&finding["focus_node"]),
            text(&finding["message"]),
        ]);
    }
    for file in answer["restricted_files"].as_array().into_iter().flatten() {
        rows.push(vec![
            "not public".into(),
            String::new(),
            text(&file["entity_id"]),
            "the public cannot read this file".into(),
        ]);
    }
    if !rows.is_empty() {
        context
            .out
            .table(&["KIND", "CODE", "WHERE", "MESSAGE"], &rows)?;
    }
    Ok(())
}

fn document_row(document: &Value) -> Vec<String> {
    let name = rocrate::root_name(&document["rocrate_summary"]).unwrap_or_default();
    vec![
        text(&document["document_id"]),
        name,
        text(&document["document_path"]),
        text(&document["public"]),
        text(&document["updated_at"]),
    ]
}

fn report(context: &mut Context<'_>, answer: &Value, line: &str) -> Result<(), CliError> {
    match context.out.json {
        true => context.out.json(answer),
        false => context.out.line(line),
    }
}

/// A JSON value as plain text: strings without quotes, null as nothing.
fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}
