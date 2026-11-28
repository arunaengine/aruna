//! Static visual DAG of an automation's triggers and stages matching the workflow graph shape.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::dag::effective_needs;
use super::record::{Spec, StageKind, Trigger};
use super::schedule::{ScheduleEvery, ScheduleSpec, days_text};

/// Node category matching `WorkflowGraphBody` in the API wire contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AutoNodeKind {
    Input,
    Step,
    Subworkflow,
    Output,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoGraphNode {
    pub id: String,
    pub kind: AutoNodeKind,
    pub label: String,
    pub tool: Option<String>,
    pub version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoGraphEdge {
    pub from_node: String,
    pub from_port: String,
    pub to_node: String,
    pub to_port: String,
}

/// Static visual DAG of an automation's triggers and stages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoGraph {
    pub nodes: Vec<AutoGraphNode>,
    pub edges: Vec<AutoGraphEdge>,
    pub complete: bool,
}

fn schedule_label(schedule: &ScheduleSpec) -> String {
    match schedule.every {
        ScheduleEvery::Day => format!("Every day at {} {}", schedule.at, schedule.zone),
        ScheduleEvery::Week => format!(
            "{} at {} {}",
            days_text(&schedule.days),
            schedule.at,
            schedule.zone
        ),
        ScheduleEvery::Hours => {
            let hours = schedule.hours.unwrap_or(1);
            let unit = if hours == 1 { "hour" } else { "hours" };
            format!(
                "Every {hours} {unit} from {} {}",
                schedule.at, schedule.zone
            )
        }
    }
}

fn trigger_label(trigger: &Trigger) -> (&'static str, String) {
    match trigger {
        Trigger::Manual { .. } => ("manual", "Manual start".into()),
        Trigger::DataUploaded { bucket, prefix, .. } => {
            ("data_uploaded", format!("{bucket}/{prefix}"))
        }
        Trigger::RunFinished { workflow_id, .. } => {
            ("run_finished", format!("Workflow {workflow_id}"))
        }
        Trigger::AppletSubmission {
            applet_id: Some(id),
        } => ("applet_submission", format!("Applet {id}")),
        Trigger::AppletSubmission { applet_id: None } => {
            ("applet_submission", "Any linked applet".into())
        }
        Trigger::Schedule(schedule) => ("schedule", schedule_label(schedule)),
    }
}

/// Builds a static graph of trigger and stage nodes matching the workflow graph contract.
pub fn build_graph(spec: &Spec) -> AutoGraph {
    let mut nodes = Vec::with_capacity(spec.triggers.len() + spec.stages.len());
    let mut edges = Vec::new();
    for (index, trigger) in spec.triggers.iter().enumerate() {
        let (tool, label) = trigger_label(trigger);
        nodes.push(AutoGraphNode {
            id: format!("trigger:{index}"),
            kind: AutoNodeKind::Input,
            label,
            tool: Some(tool.into()),
            version: None,
        });
    }
    let raw_needs = effective_needs(&spec.stages);
    for (index, stage) in spec.stages.iter().enumerate() {
        let (kind, version) = match &stage.kind {
            StageKind::Run { version, .. } => {
                (AutoNodeKind::Subworkflow, Some(format!("v{version}")))
            }
            StageKind::Copy { .. } | StageKind::Dataset { .. } => (AutoNodeKind::Output, None),
            StageKind::Notify { .. } | StageKind::Approval { .. } => (AutoNodeKind::Step, None),
        };
        let stage_id = format!("stage:{}", stage.name);
        nodes.push(AutoGraphNode {
            id: stage_id.clone(),
            kind,
            label: stage.name.clone(),
            tool: Some(stage.kind.kind_name().into()),
            version,
        });
        if raw_needs[index].is_empty() {
            for trigger_idx in 0..spec.triggers.len() {
                edges.push(AutoGraphEdge {
                    from_node: format!("trigger:{trigger_idx}"),
                    from_port: "fire".into(),
                    to_node: stage_id.clone(),
                    to_port: "in".into(),
                });
            }
        } else {
            for need in &raw_needs[index] {
                edges.push(AutoGraphEdge {
                    from_node: format!("stage:{need}"),
                    from_port: stage.when.as_str().into(),
                    to_node: stage_id.clone(),
                    to_port: "in".into(),
                });
            }
        }
    }
    AutoGraph {
        nodes,
        edges,
        complete: true,
    }
}
