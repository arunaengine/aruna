//! Approval diff and safety warnings: trigger/target loops, outside input and applet overlap.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use ulid::Ulid;

use super::record::{Spec, Stage, StageKind, Trigger};
use super::scope::{Audience, Limits};

/// Summary of another active or draft automation in the same group for loop detection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerSummary {
    pub name: String,
    pub triggers: Vec<Trigger>,
    pub write_targets: Vec<(String, String)>,
    pub run_workflows: Vec<Ulid>,
}

/// Environment facts supplied when computing an approval diff.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiffContext {
    pub outside_input: bool,
    pub unsandboxed: bool,
    pub peers: Vec<PeerSummary>,
    /// Applets referenced by `applet_submission` triggers that also hold a direct `start` grant.
    pub start_applets: Vec<(Ulid, String)>,
}

/// Structured diff shown to a human group admin before approving an automation or change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovalDiff {
    pub triggers_added: Vec<Trigger>,
    pub triggers_removed: Vec<Trigger>,
    pub read_added: Vec<String>,
    pub read_removed: Vec<String>,
    pub write_added: Vec<String>,
    pub write_removed: Vec<String>,
    pub metadata_added: Vec<String>,
    pub metadata_removed: Vec<String>,
    pub workflows_added: Vec<(Ulid, u32)>,
    pub workflows_removed: Vec<(Ulid, u32)>,
    pub audiences_added: Vec<Audience>,
    pub audiences_removed: Vec<Audience>,
    pub secrets_added: Vec<String>,
    pub secrets_removed: Vec<String>,
    pub allow_downgrade: (bool, bool),
    pub stages_added: Vec<String>,
    pub stages_removed: Vec<String>,
    pub stages_changed: Vec<String>,
    pub limits_before: Option<Limits>,
    pub limits_after: Limits,
    pub outside_input: bool,
    pub unsandboxed: bool,
    pub loop_warnings: Vec<String>,
    pub applet_warnings: Vec<String>,
}

fn static_prefix(template: &str) -> String {
    match template.find('{') {
        Some(pos) => template[..pos].trim_start_matches('/').to_string(),
        None => template.trim_start_matches('/').to_string(),
    }
}

/// Bucket and key prefix pairs written by `copy` and `run` stages of `spec`.
pub fn write_targets(spec: &Spec) -> Vec<(String, String)> {
    let mut targets = Vec::new();
    for stage in &spec.stages {
        match &stage.kind {
            StageKind::Copy {
                dest_bucket,
                dest_template,
                ..
            } => targets.push((dest_bucket.clone(), static_prefix(dest_template))),
            StageKind::Run {
                output_bucket: Some(bucket),
                output_prefix,
                ..
            } => targets.push((
                bucket.clone(),
                output_prefix
                    .as_deref()
                    .unwrap_or_default()
                    .trim_start_matches('/')
                    .to_string(),
            )),
            _ => {}
        }
    }
    targets
}

/// Workflow ids executed by `run` stages of `spec`.
pub fn run_workflows(spec: &Spec) -> Vec<Ulid> {
    spec.stages
        .iter()
        .filter_map(|stage| match &stage.kind {
            StageKind::Run { workflow_id, .. } => Some(*workflow_id),
            _ => None,
        })
        .collect()
}

fn prefixes_overlap(first: &str, second: &str) -> bool {
    let a = first.trim_start_matches('/');
    let b = second.trim_start_matches('/');
    a.is_empty() || b.is_empty() || a.starts_with(b) || b.starts_with(a)
}

fn vec_diff<T: Clone + PartialEq>(before: &[T], after: &[T]) -> (Vec<T>, Vec<T>) {
    let added = after
        .iter()
        .filter(|item| !before.contains(item))
        .cloned()
        .collect();
    let removed = before
        .iter()
        .filter(|item| !after.contains(item))
        .cloned()
        .collect();
    (added, removed)
}

fn check_pair(
    source_name: &str,
    targets: &[(String, String)],
    workflows: &[Ulid],
    target_name: &str,
    triggers: &[Trigger],
    warnings: &mut Vec<String>,
) {
    for trigger in triggers {
        match trigger {
            Trigger::DataUploaded { bucket, prefix, .. } => {
                for (dest_bucket, dest_prefix) in targets {
                    if dest_bucket == bucket && prefixes_overlap(dest_prefix, prefix) {
                        warnings.push(format!(
                            "{source_name} writes `{dest_bucket}/{dest_prefix}` which overlaps {target_name} upload trigger `{bucket}/{prefix}`"
                        ));
                    }
                }
            }
            Trigger::RunFinished { workflow_id, .. } => {
                if workflows.contains(workflow_id) {
                    warnings.push(format!(
                        "{source_name} runs workflow `{workflow_id}` which matches a run_finished trigger on {target_name}"
                    ));
                }
            }
            _ => {}
        }
    }
}

/// Computes the approval diff and warnings for creating or changing an automation to `next`.
pub fn approval_diff(current: Option<&Spec>, next: &Spec, ctx: &DiffContext) -> ApprovalDiff {
    let empty = Spec::default();
    let before = current.unwrap_or(&empty);
    let (triggers_added, triggers_removed) = vec_diff(&before.triggers, &next.triggers);
    let (read_added, read_removed) = vec_diff(&before.scope.read, &next.scope.read);
    let (write_added, write_removed) = vec_diff(&before.scope.write, &next.scope.write);
    let (metadata_added, metadata_removed) = vec_diff(&before.scope.metadata, &next.scope.metadata);
    let (workflows_added, workflows_removed) =
        vec_diff(&before.scope.workflows, &next.scope.workflows);
    let (audiences_added, audiences_removed) =
        vec_diff(&before.scope.audiences, &next.scope.audiences);
    let (secrets_added, secrets_removed) = vec_diff(&before.scope.secrets, &next.scope.secrets);

    let old_stages: BTreeMap<&str, &Stage> = before
        .stages
        .iter()
        .map(|stage| (stage.name.as_str(), stage))
        .collect();
    let new_stages: BTreeMap<&str, &Stage> = next
        .stages
        .iter()
        .map(|stage| (stage.name.as_str(), stage))
        .collect();
    let mut stages_added = Vec::new();
    let mut stages_removed = Vec::new();
    let mut stages_changed = Vec::new();
    for (&name, &stage) in &new_stages {
        match old_stages.get(name) {
            None => stages_added.push(name.to_string()),
            Some(prev) if *prev != stage => stages_changed.push(name.to_string()),
            Some(_) => {}
        }
    }
    for &name in old_stages.keys() {
        if !new_stages.contains_key(name) {
            stages_removed.push(name.to_string());
        }
    }

    let own_writes = write_targets(next);
    let own_workflows = run_workflows(next);
    let mut loop_warnings = Vec::new();
    check_pair(
        "this automation",
        &own_writes,
        &own_workflows,
        "this automation",
        &next.triggers,
        &mut loop_warnings,
    );
    for peer in &ctx.peers {
        check_pair(
            "this automation",
            &own_writes,
            &own_workflows,
            &peer.name,
            &peer.triggers,
            &mut loop_warnings,
        );
        check_pair(
            &peer.name,
            &peer.write_targets,
            &peer.run_workflows,
            "this automation",
            &next.triggers,
            &mut loop_warnings,
        );
    }

    let applet_warnings = ctx
        .start_applets
        .iter()
        .map(|(applet_id, slug)| {
            format!(
                "applet `{slug}` ({applet_id}) also grants direct run starts; submissions may trigger duplicate work"
            )
        })
        .collect();

    ApprovalDiff {
        triggers_added,
        triggers_removed,
        read_added,
        read_removed,
        write_added,
        write_removed,
        metadata_added,
        metadata_removed,
        workflows_added,
        workflows_removed,
        audiences_added,
        audiences_removed,
        secrets_added,
        secrets_removed,
        allow_downgrade: (before.scope.allow_downgrade, next.scope.allow_downgrade),
        stages_added,
        stages_removed,
        stages_changed,
        limits_before: current.map(|spec| spec.scope.limits.clone()),
        limits_after: next.scope.limits.clone(),
        outside_input: ctx.outside_input,
        unsandboxed: ctx.unsandboxed,
        loop_warnings,
        applet_warnings,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::automation::record::When;

    #[test]
    fn flags_loop_warnings() {
        let wf = Ulid::from_parts(9, 1);
        let applet_id = Ulid::from_parts(9, 2);
        let mut next = Spec::default();
        next.triggers.push(Trigger::AppletSubmission { applet_id });
        next.stages.push(Stage {
            name: "run".into(),
            needs: None,
            when: When::Passed,
            kind: StageKind::Run {
                workflow_id: wf,
                version: 1,
                values_json: "{}".into(),
                inputs_json: None,
                output_bucket: Some("b".into()),
                output_prefix: Some("out/".into()),
                placement_site: None,
                secrets: BTreeMap::new(),
                provenance_crate: false,
            },
        });
        let ctx = DiffContext {
            outside_input: true,
            unsandboxed: true,
            peers: vec![PeerSummary {
                name: "downstream".into(),
                triggers: vec![Trigger::DataUploaded {
                    bucket: "b".into(),
                    prefix: "out/".into(),
                    pattern: None,
                }],
                write_targets: vec![],
                run_workflows: vec![],
            }],
            start_applets: vec![(applet_id, "intake".into())],
        };
        let diff = approval_diff(None, &next, &ctx);
        assert!(diff.outside_input);
        assert!(diff.unsandboxed);
        assert_eq!(diff.loop_warnings.len(), 1);
        assert_eq!(diff.applet_warnings.len(), 1);
        assert_eq!(diff.stages_added, vec!["run"]);
    }
}
