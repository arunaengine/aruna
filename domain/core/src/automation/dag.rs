//! Bounded static DAG validation, stage reference checking and scope verification.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use ulid::Ulid;

use super::glob::expand_copy;
use super::record::{Spec, Stage, StageKind, Trigger};
use super::scope::{Limits, MAX_CHAIN_DEPTH};
use crate::id::NodeId;
use crate::permission_path::{path_within, permission_pattern_matches, role_path_confined};
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::blob::object_permission_path;
use crate::types::GroupId;

/// Hard upper bound on stages and triggers in one automation DAG.
pub const MAX_STAGES: usize = 16;
const MAX_NAME_LEN: usize = 64;
const MAX_MESSAGE_LEN: usize = 1000;

/// Validated topological execution order and upstream stage indices for an automation DAG.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DagPlan {
    pub order: Vec<usize>,
    pub needs: Vec<Vec<usize>>,
}

/// Effective upstream stage names per stage; omitted `needs` defaults to `[previous_stage]`.
pub fn effective_needs(stages: &[Stage]) -> Vec<Vec<String>> {
    stages
        .iter()
        .enumerate()
        .map(|(index, stage)| match &stage.needs {
            Some(list) => list.clone(),
            None if index > 0 => vec![stages[index - 1].name.clone()],
            None => Vec::new(),
        })
        .collect()
}

/// Extracts stage names referenced as `{{stages.<name>...}}` inside a JSON or text template.
pub fn stage_refs(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find("{{") {
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else {
            break;
        };
        let expr = after[..close].trim();
        if let Some(tail) = expr.strip_prefix("stages.") {
            let name = tail.split('.').next().unwrap_or_default().trim();
            if !name.is_empty() {
                found.push(name.to_string());
            }
        }
        rest = &after[close + 2..];
    }
    found
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn check_text(label: &str, text: &str) -> Result<(), String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    if trimmed.len() > MAX_MESSAGE_LEN {
        return Err(format!("{label} exceeds {MAX_MESSAGE_LEN} characters"));
    }
    if trimmed.contains('<') || trimmed.contains('>') {
        return Err(format!("{label} must be plain text without HTML tags"));
    }
    Ok(())
}

fn check_limits(limits: &Limits) -> Result<(), String> {
    if limits.max_active_firings == 0 || limits.max_concurrent_workflows == 0 {
        return Err("concurrency limits must be at least 1".into());
    }
    if limits.per_hour == 0 || limits.per_day == 0 {
        return Err("rate limits must be at least 1".into());
    }
    if limits.max_chain_depth == 0 || limits.max_chain_depth > MAX_CHAIN_DEPTH {
        return Err(format!("max_chain_depth must be in 1..={MAX_CHAIN_DEPTH}"));
    }
    if limits.cpu_cores == Some(0) || limits.ram_bytes == Some(0) || limits.walltime_ms == Some(0) {
        return Err("compute limits must be greater than zero".into());
    }
    Ok(())
}

fn check_triggers(triggers: &[Trigger]) -> Result<(), String> {
    if triggers.is_empty() || triggers.len() > MAX_STAGES {
        return Err(format!("automations need 1..={MAX_STAGES} triggers"));
    }
    for trigger in triggers {
        if let Trigger::DataUploaded {
            bucket,
            prefix,
            pattern,
        } = trigger
        {
            if bucket.is_empty() {
                return Err("data_uploaded trigger needs a bucket".into());
            }
            if prefix.starts_with('/') || prefix.contains("..") {
                return Err("data_uploaded prefix must be relative without `..`".into());
            }
            if let Some(pat) = pattern {
                if pat.is_empty() || pat.contains('/') {
                    return Err("data_uploaded filename pattern must not contain `/`".into());
                }
            }
        }
    }
    Ok(())
}

fn check_stage(stage: &Stage, needs: &BTreeSet<&str>, spec: &Spec) -> Result<(), String> {
    let name = &stage.name;
    match &stage.kind {
        StageKind::Run {
            version,
            values_json,
            inputs_json,
            output_bucket,
            output_prefix,
            ..
        } => {
            if *version == 0 {
                return Err(format!("stage `{name}` needs a workflow version >= 1"));
            }
            if let Some(prefix) = output_prefix {
                if output_bucket.as_deref().unwrap_or_default().is_empty() {
                    return Err(format!("stage `{name}` needs output_bucket with prefix"));
                }
                if prefix.starts_with('/') || prefix.contains("..") {
                    return Err(format!("stage `{name}` has an invalid output_prefix"));
                }
            }
            for text in std::iter::once(values_json.as_str()).chain(inputs_json.as_deref()) {
                for referenced in stage_refs(text) {
                    if !needs.contains(referenced.as_str()) {
                        return Err(format!(
                            "stage `{name}` references `{referenced}` outside its `needs`"
                        ));
                    }
                }
            }
        }
        StageKind::Copy {
            from_stage,
            dest_bucket,
            dest_template,
            allow_downgrade,
            ..
        } => {
            if !needs.contains(from_stage.as_str()) {
                return Err(format!("copy `{name}` must list `{from_stage}` in `needs`"));
            }
            if dest_bucket.is_empty() {
                return Err(format!("copy `{name}` needs a destination bucket"));
            }
            expand_copy(
                dest_template,
                "2026-01-01",
                "01K00000000000000000000000",
                "f",
            )
            .map_err(|err| format!("copy `{name}`: {err}"))?;
            if *allow_downgrade && !spec.scope.allow_downgrade {
                return Err(format!(
                    "copy `{name}` enables allow_downgrade outside scope"
                ));
            }
        }
        StageKind::Dataset {
            from_stage,
            metadata_path,
            name_template,
            description,
            ..
        } => {
            if !needs.contains(from_stage.as_str()) {
                return Err(format!(
                    "dataset `{name}` must list `{from_stage}` in `needs`"
                ));
            }
            if metadata_path.is_empty() || metadata_path.contains("..") {
                return Err(format!("dataset `{name}` has an invalid metadata_path"));
            }
            check_text("dataset name_template", name_template)?;
            if !description.is_empty() {
                check_text("dataset description", description)?;
            }
        }
        StageKind::Notify { message, .. } => check_text("notify message", message)?,
        StageKind::Approval {
            message,
            timeout_ms,
        } => {
            check_text("approval message", message)?;
            if *timeout_ms == Some(0) {
                return Err(format!("approval `{name}` timeout must be > 0"));
            }
        }
    }
    Ok(())
}

/// Validates triggers, limits, stage names, references and DAG acyclicity.
pub fn validate_spec(spec: &Spec) -> Result<DagPlan, String> {
    check_triggers(&spec.triggers)?;
    check_limits(&spec.scope.limits)?;
    if spec.stages.is_empty() || spec.stages.len() > MAX_STAGES {
        return Err(format!("automations need 1..={MAX_STAGES} stages"));
    }
    let mut by_name = BTreeMap::new();
    for (index, stage) in spec.stages.iter().enumerate() {
        if !valid_name(&stage.name) {
            return Err(format!("stage name `{}` must match [a-z0-9_-]", stage.name));
        }
        if by_name.insert(stage.name.as_str(), index).is_some() {
            return Err(format!("duplicate stage name `{}`", stage.name));
        }
    }
    let raw_needs = effective_needs(&spec.stages);
    let mut needs = vec![Vec::new(); spec.stages.len()];
    let mut dependents = vec![Vec::new(); spec.stages.len()];
    let mut indegree = vec![0usize; spec.stages.len()];
    for (index, stage) in spec.stages.iter().enumerate() {
        let mut seen = BTreeSet::new();
        for need in &raw_needs[index] {
            let Some(&upstream) = by_name.get(need.as_str()) else {
                return Err(format!(
                    "stage `{}` needs unknown stage `{need}`",
                    stage.name
                ));
            };
            if upstream == index {
                return Err(format!("stage `{}` cannot depend on itself", stage.name));
            }
            if seen.insert(need.as_str()) {
                needs[index].push(upstream);
                dependents[upstream].push(index);
                indegree[index] += 1;
            }
        }
        check_stage(stage, &seen, spec)?;
    }
    let mut ready: VecDeque<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, &deg)| (deg == 0).then_some(index))
        .collect();
    let mut order = Vec::with_capacity(spec.stages.len());
    while let Some(index) = ready.pop_front() {
        order.push(index);
        for &child in &dependents[index] {
            indegree[child] -= 1;
            if indegree[child] == 0 {
                ready.push_back(child);
            }
        }
    }
    if order.len() != spec.stages.len() {
        return Err("automation stages contain a cycle".into());
    }
    Ok(DagPlan { order, needs })
}

/// Verifies every stage's targets and permissions lie inside the declared `spec.scope`.
pub fn validate_scope(
    spec: &Spec,
    realm_id: RealmId,
    group_id: GroupId,
    host_node: NodeId,
) -> Result<(), String> {
    let root = format!("/{realm_id}/g/{group_id}");
    for pattern in spec.scope.read.iter().chain(&spec.scope.write) {
        if !role_path_confined(pattern, &root) {
            return Err(format!("scope path `{pattern}` escapes group `{group_id}`"));
        }
    }
    for stage in &spec.stages {
        let name = &stage.name;
        match &stage.kind {
            StageKind::Run {
                workflow_id,
                version,
                secrets,
                ..
            } => {
                if !spec.scope.workflows.contains(&(*workflow_id, *version)) {
                    return Err(format!(
                        "stage `{name}` workflow is outside scope.workflows"
                    ));
                }
                for secret in secrets.keys() {
                    if !spec.scope.secrets.contains(secret) {
                        return Err(format!("stage `{name}` secret `{secret}` is outside scope"));
                    }
                }
            }
            StageKind::Copy {
                dest_bucket,
                dest_template,
                ..
            } => {
                let sample = expand_copy(
                    dest_template,
                    "2026-01-01",
                    "01K00000000000000000000000",
                    "f",
                )
                .map_err(|err| format!("copy `{name}`: {err}"))?;
                let path =
                    object_permission_path(realm_id, group_id, host_node, dest_bucket, &sample);
                if !spec
                    .scope
                    .write
                    .iter()
                    .any(|p| permission_pattern_matches(p, &path))
                {
                    return Err(format!(
                        "copy `{name}` path `{path}` is outside scope.write"
                    ));
                }
            }
            StageKind::Dataset { metadata_path, .. } => {
                let target = metadata_path.trim_matches('/');
                let covered = spec.scope.metadata.iter().any(|allowed| {
                    let prefix = allowed.trim_matches('/');
                    prefix.is_empty() || path_within(target, prefix)
                });
                if !covered {
                    return Err(format!("dataset `{name}` path is outside scope.metadata"));
                }
            }
            StageKind::Notify { audience, .. } => {
                if !spec.scope.audiences.contains(audience) {
                    return Err(format!(
                        "notify `{name}` audience is outside scope.audiences"
                    ));
                }
            }
            StageKind::Approval { .. } => {}
        }
    }
    Ok(())
}

/// Workflow `(id, version)` pairs referenced by `run` stages in `spec`.
pub fn spec_workflows(spec: &Spec) -> Vec<(Ulid, u32)> {
    spec.stages
        .iter()
        .filter_map(|stage| match &stage.kind {
            StageKind::Run {
                workflow_id,
                version,
                ..
            } => Some((*workflow_id, *version)),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::automation::record::When;
    use crate::automation::scope::Audience;

    fn run_stage(name: &str, needs: Option<Vec<&str>>, values: &str) -> Stage {
        Stage {
            name: name.into(),
            needs: needs.map(|list| list.into_iter().map(String::from).collect()),
            when: When::Passed,
            kind: StageKind::Run {
                workflow_id: Ulid::from_parts(1, 1),
                version: 1,
                values_json: values.into(),
                inputs_json: None,
                output_bucket: None,
                output_prefix: None,
                placement_site: None,
                secrets: BTreeMap::new(),
                provenance_crate: false,
            },
        }
    }

    #[test]
    fn validates_dag_refs() {
        let mut spec = Spec::default();
        spec.triggers.push(Trigger::Manual { form_json: None });
        spec.stages.push(run_stage("qc", None, "{}"));
        spec.stages
            .push(run_stage("align", None, r#"{"in":"{{stages.qc.out}}"}"#));
        let plan = validate_spec(&spec).unwrap();
        assert_eq!(plan.order, vec![0, 1]);
        assert_eq!(plan.needs, vec![vec![], vec![0]]);

        let mut bad_ref = spec.clone();
        bad_ref.stages[1].needs = Some(vec![]);
        assert!(validate_spec(&bad_ref).is_err());

        let mut cycle = spec.clone();
        cycle.stages[0].needs = Some(vec!["align".into()]);
        cycle.stages[1].needs = Some(vec!["qc".into()]);
        assert!(validate_spec(&cycle).is_err());

        let mut too_many = Spec::default();
        too_many.triggers.push(Trigger::Manual { form_json: None });
        for idx in 0..=MAX_STAGES {
            too_many
                .stages
                .push(run_stage(&format!("s{idx}"), Some(vec![]), "{}"));
        }
        assert!(validate_spec(&too_many).is_err());

        let mut html = spec.clone();
        html.stages.push(Stage {
            name: "alert".into(),
            needs: Some(vec!["align".into()]),
            when: When::Always,
            kind: StageKind::Notify {
                audience: Audience::GroupAdmins,
                message: "<b>done</b>".into(),
            },
        });
        assert!(validate_spec(&html).is_err());
    }
}
