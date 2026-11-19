//! Jobs the install started: copies, downloads, uploads and tasks. A start answers the job at
//! once; `job_finished` delivers it once it ended and the run that started it succeeded.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::de::DeserializeOwned;

use crate::bindings::aruna::plugin::jobs::Status;
use crate::datasets::Pages;
use crate::host;
use crate::{Object, Reply, Result};

/// One job of the install. `state` is `queued`, `running`, `succeeded`, `failed`, `error`
/// (an attempt failed and is retried), `cancelled` or `unknown`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: String,
    /// `copy`, `download`, `upload` or `task`.
    pub kind: String,
    /// The bucket path it wrote or read; empty for a task.
    pub path: String,
    pub state: String,
    pub ok: bool,
    pub message: Option<String>,
    pub bytes: Option<u64>,
    pub version: Option<String>,
    pub exit_code: Option<i32>,
    /// An upload's answer.
    pub reply: Option<Reply>,
    /// A task's report, cleaned of its secrets; absent after a node restart during the task.
    pub report: Option<String>,
}

/// The job `id` of the install, as it stands now.
pub fn get(id: &str) -> Result<Job> {
    Ok(Job::from_status(host::with(|host| host.job_get(id))?))
}

impl Job {
    fn from_status(status: Status) -> Self {
        let reply = status.reply_status.map(|code| Reply {
            status: code,
            headers: (status.reply_headers.into_iter())
                .map(|field| (field.name, field.value))
                .collect(),
            body: status.reply_body.unwrap_or_default(),
        });
        Self {
            ok: status.state == "succeeded",
            id: status.id,
            kind: status.kind,
            path: status.path,
            state: status.state,
            message: status.message,
            bytes: status.bytes,
            version: status.version,
            exit_code: status.exit_code,
            reply,
            report: status.report,
        }
    }

    /// A job just started: queued until it is read again.
    pub(crate) fn started(id: String, kind: &str, path: &str) -> Self {
        Self {
            id,
            kind: kind.into(),
            path: path.into(),
            state: "queued".into(),
            ok: false,
            message: None,
            bytes: None,
            version: None,
            exit_code: None,
            reply: None,
            report: None,
        }
    }

    /// Reads the job again.
    pub fn refresh(&mut self) -> Result<()> {
        *self = get(&self.id)?;
        Ok(())
    }

    /// Asks the job to stop; a job that ended stays as it ended.
    pub fn cancel(&self) -> Result<()> {
        Ok(host::with(|host| host.job_cancel(&self.id))?)
    }

    /// The objects a task wrote, one page at a time.
    pub fn outputs(&self) -> impl Iterator<Item = Result<Object>> + use<> {
        let id = self.id.clone();
        Pages::new(move |cursor| {
            let page = host::with(|host| host.job_outputs(&id, cursor.as_deref()))?;
            let objects = page.objects.into_iter().map(Object::from_info).collect();
            Ok((objects, page.cursor))
        })
    }

    /// The report as JSON, or `None` when it was left out.
    pub fn report_json<T: DeserializeOwned>(&self) -> Result<Option<T>> {
        (self.report.as_deref())
            .map(|report| Ok(serde_json::from_str(report)?))
            .transpose()
    }
}

/// A copy to start: `objects::copy(source, target).version(v).start()?`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copy {
    pub(crate) source: String,
    pub(crate) target: String,
    pub(crate) version: Option<String>,
}

impl Copy {
    /// Copies this version of the source instead of its current one.
    pub fn version(mut self, version: &str) -> Self {
        self.version = Some(version.to_string());
        self
    }

    /// Starts the copy; it needs read on the source and write on the target. A rerun of the
    /// run gets the same job back.
    pub fn start(self) -> Result<Job> {
        let version = self.version.as_deref();
        let id = host::with(|host| host.job_copy(&self.source, version, &self.target))?;
        Ok(Job::started(id, "copy", &self.target))
    }
}

/// A container task to start: `aruna::task("map").parameter("reference", r).start()?`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    name: String,
    parameters: Vec<(String, String)>,
    inputs: Vec<(String, String, Option<String>)>,
    files: Vec<(String, Vec<u8>)>,
    outputs: Vec<(String, String)>,
}

/// The task `name` the manifest declares under `[tasks.<name>]`.
pub fn task(name: &str) -> Task {
    Task {
        name: name.to_string(),
        parameters: Vec::new(),
        inputs: Vec::new(),
        files: Vec::new(),
        outputs: Vec::new(),
    }
}

impl Task {
    /// A value for the `{name}` part of the command; it stays one argument.
    pub fn parameter(mut self, name: &str, value: &str) -> Self {
        self.parameters.push((name.to_string(), value.to_string()));
        self
    }

    /// The object or bucket path the task reads at `path`; a source ending in `/` gives every
    /// object below it.
    pub fn input(mut self, path: &str, source: &str) -> Self {
        self.inputs
            .push((path.to_string(), source.to_string(), None));
        self
    }

    /// One object version the task reads at `path`.
    pub fn object(mut self, path: &str, object: &Object) -> Self {
        let pinned = Some(object.version.clone());
        self.inputs
            .push((path.to_string(), object.path.clone(), pinned));
        self
    }

    /// A small file the plugin writes for the task; a task's files hold 64 KiB together.
    pub fn file(mut self, path: &str, content: impl Into<Vec<u8>>) -> Self {
        self.files.push((path.to_string(), content.into()));
        self
    }

    /// The container directory whose files the task writes below the bucket path `prefix`.
    pub fn output(mut self, directory: &str, prefix: &str) -> Self {
        self.outputs
            .push((directory.to_string(), prefix.to_string()));
        self
    }

    /// Starts the task; it needs read on every input and write on every output. A rerun of
    /// the run gets the same job back.
    pub fn start(self) -> Result<Job> {
        use crate::bindings::aruna::plugin::jobs::{TaskFile, TaskInput, TaskRequest};
        use crate::bindings::aruna::plugin::types::Field;
        let field = |(name, value): (String, String)| Field { name, value };
        let request = TaskRequest {
            task: self.name,
            parameters: self.parameters.into_iter().map(field).collect(),
            inputs: (self.inputs.into_iter())
                .map(|(path, source, version)| TaskInput {
                    path,
                    source,
                    version,
                })
                .collect(),
            files: (self.files.into_iter())
                .map(|(path, content)| TaskFile { path, content })
                .collect(),
            outputs: self.outputs.into_iter().map(field).collect(),
        };
        let id = host::with(|host| host.job_task(&request))?;
        Ok(Job::started(id, "task", ""))
    }
}

#[cfg(test)]
#[path = "jobs_tests.rs"]
mod tests;
