//! Builds the job spec of one plugin task from its recipe and what the run asked for: the
//! filled command, resources, network, reserved tags, inputs, inline files and outputs. Pure.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::execution::job::{
    ComputeResources, ExecutionSpec, InputMode, InputSelection, InputSource, MAX_INLINE_BYTES,
    OutputDestination, OutputSelection, PLUGIN_INSTALL_TAG, PLUGIN_REPORT_TAG, PLUGIN_WORK_TAG,
};
use aruna_plugin_types::recipe::{Network, Recipe, container_path};
use ulid::Ulid;

use crate::jobs::workflow::prepare::NETWORK_TAG_KEY;

/// What a run gives a task, with every object path already resolved to a bucket on the
/// receiver: an object input whose key ends in `/` is every object below it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskIo {
    /// Container path, bucket, key and pinned version.
    pub objects: Vec<(String, String, String, Option<Ulid>)>,
    /// Container path and the bytes the plugin wrote.
    pub files: Vec<(String, Vec<u8>)>,
    /// Container directory (ending in `/`), bucket and key prefix the task writes below.
    pub outputs: Vec<(String, String, String)>,
}

/// The spec of work `work_id` running recipe `recipe` of `install`. Every container path is
/// absolute and used once, inline files stay within their bound, outputs are directories, and
/// no output covers the report, so no export can take a raw copy of it.
pub fn task_spec(
    install: &PluginInstall,
    work_id: Ulid,
    (recipe, parameters): (&Recipe, &BTreeMap<String, String>),
    io: &TaskIo,
) -> Result<ExecutionSpec, String> {
    let command = recipe.fill(parameters)?;
    check_paths(recipe, io)?;
    let mut tags = BTreeMap::from([
        (
            PLUGIN_INSTALL_TAG.to_string(),
            install.install_id.to_string(),
        ),
        (PLUGIN_WORK_TAG.to_string(), work_id.to_string()),
    ]);
    if let Some(report) = &recipe.report {
        tags.insert(PLUGIN_REPORT_TAG.to_string(), report.clone());
    }
    if recipe.network == Network::Open {
        tags.insert(NETWORK_TAG_KEY.to_string(), "open".to_string());
    }
    let objects = (io.objects.iter()).map(|(path, bucket, key, version)| InputSelection {
        source: InputSource::S3 {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: version.map(|version| version.to_string()),
        },
        source_node_id: None,
        dest_key: path.clone(),
        mode: InputMode::Snapshot,
        container_path: Some(path.clone()),
        name: None,
        description: None,
    });
    let files = (io.files.iter()).map(|(path, bytes)| InputSelection {
        source: InputSource::Inline {
            bytes: bytes.clone(),
        },
        source_node_id: None,
        dest_key: path.clone(),
        mode: InputMode::Snapshot,
        container_path: Some(path.clone()),
        name: None,
        description: None,
    });
    let outputs = (io.outputs.iter()).map(|(directory, bucket, prefix)| {
        let directory = directory.trim_end_matches('/');
        OutputSelection {
            container_path: format!("{directory}/**"),
            path_prefix: Some(directory.to_string()),
            destination_node_id: None,
            destination: OutputDestination::S3 {
                bucket: bucket.clone(),
                key: prefix.trim_end_matches('/').to_string(),
            },
            name: None,
            description: None,
        }
    });
    Ok(ExecutionSpec {
        group_id: install.group_id,
        name: Some(format!("{} task", install.plugin)),
        description: None,
        tags,
        image: recipe.image.clone(),
        entrypoint: recipe.command.as_ref().map(|_| command),
        command: Vec::new(),
        workdir: None,
        env: BTreeMap::new(),
        resources: ComputeResources {
            cpu_cores: Some(recipe.cpu),
            ram_bytes: Some(recipe.memory),
            disk_bytes: None,
            max_walltime_ms: Some(recipe.time.saturating_mul(1000)),
            preemptible: false,
        },
        executor_constraint: None,
        backend: None,
        inputs: objects.chain(files).collect(),
        file_outputs: outputs.collect(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    })
}

fn check_paths(recipe: &Recipe, io: &TaskIo) -> Result<(), String> {
    if io.outputs.is_empty() {
        return Err("a task needs at least one output".to_string());
    }
    let inputs = (io.objects.iter()).map(|(path, ..)| path);
    let files = (io.files.iter()).map(|(path, _)| path);
    let outputs: Vec<&String> = (io.outputs.iter()).map(|(path, ..)| path).collect();
    let mut seen = BTreeSet::new();
    for path in inputs.chain(files).chain(outputs.iter().copied()) {
        let plain = path.trim_end_matches('/');
        if !container_path(plain) || !seen.insert(plain) {
            return Err(format!(
                "`{path}` is not an absolute container path used once"
            ));
        }
    }
    if let Some(output) = outputs.iter().find(|path| !path.ends_with('/')) {
        return Err(format!(
            "the output `{output}` must be a directory ending in `/`"
        ));
    }
    let inline: usize = (io.files.iter()).map(|(_, bytes)| bytes.len()).sum();
    if inline > MAX_INLINE_BYTES {
        return Err("the task's files have more than 64 KiB together".to_string());
    }
    let report = recipe.report.as_deref();
    let covered = |report: &str| {
        outputs
            .iter()
            .any(|output| report.starts_with(output.as_str()))
    };
    match report.filter(|report| covered(report) || seen.contains(report)) {
        Some(report) => Err(format!(
            "the report `{report}` lies in an output or input path"
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
#[path = "tasks_tests.rs"]
mod tests;
