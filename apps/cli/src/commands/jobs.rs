//! Submits, lists, cancels and follows GA4GH TES tasks, and waits for node jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::Context;
use crate::args::JobsCommand;
use crate::client::{Api, read_json};
use crate::error::CliError;
use reqwest::Method;
use serde_json::Value;
use std::time::Duration;

const TES: &str = "ga4gh/tes/v1/tasks";
const TES_ENDED: [&str; 5] = [
    "COMPLETE",
    "EXECUTOR_ERROR",
    "SYSTEM_ERROR",
    "CANCELED",
    "PREEMPTED",
];
const JOB_ENDED: [&str; 3] = ["succeeded", "failed", "cancelled"];

pub async fn jobs(context: &mut Context<'_>, command: JobsCommand) -> Result<(), CliError> {
    let api = context.api()?;
    match command {
        JobsCommand::Submit { file } => {
            let task: Value = serde_json::from_slice(&std::fs::read(&file)?).map_err(|error| {
                CliError::usage(format!("{} is not a TES task: {error}", file.display()))
            })?;
            let created: Value = api.send(Method::POST, TES, &task).await?;
            let id = created["id"].as_str().unwrap_or_default();
            match context.out.json {
                true => context.out.json(&created),
                false => context.out.line(format!("Submitted task {id}.")),
            }
        }
        JobsCommand::Ls { limit } => {
            let query = [
                ("view", "BASIC".to_string()),
                ("page_size", limit.to_string()),
            ];
            let page: Value = api.get(TES, &query).await?;
            let tasks = page["tasks"].as_array().cloned().unwrap_or_default();
            let rows = tasks.iter().map(task_row).collect();
            context
                .out
                .listing(&page, &["TASK", "STATE", "NAME", "CREATED"], rows)
        }
        JobsCommand::Status { id } => {
            let task = get_task(&api, &id).await?;
            match context.out.json {
                true => context.out.json(&task),
                false => context
                    .out
                    .table(&["TASK", "STATE", "NAME", "CREATED"], &[task_row(&task)]),
            }
        }
        JobsCommand::Cancel { id } => {
            let _: Value = api
                .send(
                    Method::POST,
                    &format!("{TES}/{id}:cancel"),
                    &Value::Object(Default::default()),
                )
                .await?;
            context.out.line(format!("Asked to cancel task {id}."))
        }
        JobsCommand::Watch { id, interval } => {
            let mut last = String::new();
            loop {
                let task = get_task(&api, &id).await?;
                let state = task["state"].as_str().unwrap_or("UNKNOWN").to_string();
                if state != last && !context.out.json {
                    context.out.line(format!("{id}: {state}"))?;
                }
                if TES_ENDED.contains(&state.as_str()) {
                    return match context.out.json {
                        true => context.out.json(&task),
                        false => Ok(()),
                    };
                }
                last = state;
                tokio::time::sleep(Duration::from_secs(interval)).await;
            }
        }
    }
}

async fn get_task(api: &Api, id: &str) -> Result<Value, CliError> {
    api.get(&format!("{TES}/{id}"), &[("view", "BASIC".to_string())])
        .await
}

fn task_row(task: &Value) -> Vec<String> {
    ["id", "state", "name", "creation_time"]
        .iter()
        .map(|field| task[field].as_str().unwrap_or_default().to_string())
        .collect()
}

/// Polls a node job at its status URL until it succeeds, fails or is cancelled.
pub async fn wait_job(api: &Api, status_url: &str) -> Result<Value, CliError> {
    let mut last = String::new();
    loop {
        let request = api.request_url(Method::GET, status_url)?;
        let status: Value = read_json(request.send().await?).await?;
        let state = status["state"].as_str().unwrap_or("unknown").to_string();
        if state != last {
            eprintln!(
                "job {}: {state}",
                status["job_id"].as_str().unwrap_or_default()
            );
        }
        if JOB_ENDED.contains(&state.as_str()) {
            return Ok(status);
        }
        last = state;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
