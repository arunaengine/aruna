//! Composed `.uwe.yaml` workflows: takes the `aruna` key off each step and checks its settings,
//! then lowers the rest to wir with uwe and turns every finding into a diagnostic with a pointer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::path::Path;

use aruna_workflow_contract::preflight::{
    ComposedLowering, Diagnostic, DiagnosticPointer, EdgePointer, GraphEdge, GraphNode, NodeKind,
    Severity, WorkflowGraph,
};
use aruna_workflow_contract::settings::{ArunaStepSettings, SETTINGS_KEY};
use serde_json::Value;
use tool_definition::{Definition, Fit};
use uwe::tools::composed::{ComposedWorkflow, from_json};
use uwe::tools::{LoweringError, ToolLibrary, lower};

/// A composed workflow without its `aruna` keys, and what they held.
pub struct Stripped {
    /// The workflow as JSON text, which reads as YAML too; empty when the text is no YAML.
    pub document: String,
    pub value: Option<Value>,
    pub settings: BTreeMap<String, ArunaStepSettings>,
    pub diagnostics: Vec<Diagnostic>,
}

impl Stripped {
    pub fn errors(&self) -> impl Iterator<Item = &Diagnostic> {
        (self.diagnostics.iter()).filter(|diagnostic| diagnostic.severity == Severity::Error)
    }
}

/// Removes the `aruna` key of every step and reads and checks the settings it held.
pub fn strip_settings(text: &str) -> Stripped {
    let mut stripped = Stripped {
        document: String::new(),
        value: None,
        settings: BTreeMap::new(),
        diagnostics: Vec::new(),
    };
    let mut value = match tool_definition::yaml::read(text) {
        Ok(value) => value,
        Err(message) => {
            stripped
                .diagnostics
                .push(error(message, DiagnosticPointer::default()));
            return stripped;
        }
    };
    let steps = (value.get_mut("steps")).and_then(Value::as_object_mut);
    for (id, step) in steps.into_iter().flatten() {
        let Some(given) = step
            .as_object_mut()
            .and_then(|step| step.remove(SETTINGS_KEY))
        else {
            continue;
        };
        let pointer = |field: String| DiagnosticPointer {
            line: step_line(text, id, Some(SETTINGS_KEY)),
            node: Some(id.clone()),
            edge: None,
            field: Some(field),
        };
        match ArunaStepSettings::from_value(given) {
            Err(issue) => {
                let message = format!("step {id}: {}", issue.message);
                stripped
                    .diagnostics
                    .push(error(message, pointer(issue.field)));
            }
            Ok(settings) => {
                let issues = settings.validate();
                for issue in &issues {
                    let message = format!("step {id}: {} {}", issue.field, issue.message);
                    let field = format!("{SETTINGS_KEY}.{}", issue.field);
                    stripped.diagnostics.push(error(message, pointer(field)));
                }
                if issues.is_empty() && !settings.is_empty() {
                    stripped.settings.insert(id.clone(), settings);
                }
            }
        }
    }
    stripped.document = value.to_string();
    stripped.value = Some(value);
    stripped
}

/// Checks and lowers a composed workflow with the catalog tools, whose source files lie in
/// `folder`, and gives the wir document when nothing refuses it.
pub fn lower_composed(text: &str, catalog: &[Value], folder: &Path) -> ComposedLowering {
    let stripped = strip_settings(text);
    let mut lowering = ComposedLowering {
        diagnostics: stripped.diagnostics,
        step_settings: stripped.settings,
        ..ComposedLowering::default()
    };
    let Some(value) = stripped.value else {
        return lowering;
    };
    let mut workflow = match from_json(&value) {
        Ok(workflow) => workflow,
        Err(message) => {
            let pointer = pointer_of(text, &message);
            lowering.diagnostics.push(error(message, pointer));
            return lowering;
        }
    };
    let mut library = ToolLibrary::new();
    // Relative source addresses of the tools resolve in `folder`.
    let origin = folder.join("catalog");
    for (index, tool) in catalog.iter().enumerate() {
        match serde_json::from_value::<Definition>(tool.clone()) {
            Ok(definition) => {
                library.add(definition, Some(origin.clone()));
            }
            Err(problem) => {
                let message = format!("catalog tool {index} is no tool definition: {problem}");
                lowering
                    .diagnostics
                    .push(warning(message, DiagnosticPointer::default()));
            }
        }
    }
    for reference in &workflow.references {
        let message =
            format!("the tool files of {reference} are not read here; the catalog gives the tools");
        lowering
            .diagnostics
            .push(warning(message, DiagnosticPointer::default()));
    }
    lowering.graph = graph_of(&workflow);
    if let Err(message) = workflow.resolve(&mut library, None) {
        let pointer = pointer_of(text, &message);
        lowering.diagnostics.push(error(message, pointer));
        return lowering;
    }
    lowering.graph = graph_of(&workflow);
    let tools = workflow.used_tools(&library);
    let report = tool_definition::graph::check(&workflow.graph_with(&tools), &tools);
    lowering.check_json = serde_json::to_value(&report).ok();
    for node in &report.nodes {
        for finding in node
            .findings
            .iter()
            .filter(|finding| finding.fit != Fit::Fits)
        {
            let pointer = DiagnosticPointer {
                line: step_line(text, &node.id, None),
                node: Some(node.id.clone()),
                ..DiagnosticPointer::default()
            };
            let message = format!("step {}: {}", node.id, finding.reason);
            lowering
                .diagnostics
                .push(by_fit(finding.fit, message, pointer));
        }
    }
    for (edge, check) in workflow.graph.edges.iter().zip(&report.edges) {
        let ends = EdgePointer {
            from: format!("{}/{}", edge.from.node, edge.from.port),
            to: format!("{}/{}", edge.to.node, edge.to.port),
        };
        for finding in check
            .findings
            .iter()
            .filter(|finding| finding.fit != Fit::Fits)
        {
            let pointer = DiagnosticPointer {
                line: step_line(text, &edge.to.node, Some("in")),
                node: Some(edge.to.node.clone()),
                edge: Some(ends.clone()),
                field: None,
            };
            let message = format!("{} -> {}: {}", ends.from, ends.to, finding.reason);
            lowering
                .diagnostics
                .push(by_fit(finding.fit, message, pointer));
        }
    }
    if report.fit == Fit::DoesNotFit || stripped_errors(&lowering) {
        return lowering;
    }
    let generated = folder.join("generated");
    match lower(&workflow, &library, (folder, &generated)) {
        Ok(lowered) => match serde_json::to_value(&lowered.document) {
            Ok(document) => lowering.wir_json = Some(document),
            Err(problem) => {
                let message = format!("the wir document does not write as JSON: {problem}");
                lowering
                    .diagnostics
                    .push(error(message, DiagnosticPointer::default()));
            }
        },
        Err(LoweringError::Node { node, reason }) => {
            let pointer = DiagnosticPointer {
                line: step_line(text, &node, None),
                node: Some(node.clone()),
                ..DiagnosticPointer::default()
            };
            lowering
                .diagnostics
                .push(error(format!("step {node}: {reason}"), pointer));
        }
        Err(other) => {
            let message = other.to_string();
            lowering
                .diagnostics
                .push(error(message, DiagnosticPointer::default()));
        }
    }
    lowering
}

fn stripped_errors(lowering: &ComposedLowering) -> bool {
    (lowering.diagnostics.iter()).any(|diagnostic| diagnostic.severity == Severity::Error)
}

/// The `aruna` settings of one step, with the names its tasks may carry as their step.
#[derive(Clone, Debug, PartialEq)]
pub struct StepMatch {
    pub step: String,
    /// The step id, and for a composed step the id and title of its tool, as uwe names the
    /// tasks of a tool after its process, rule or template.
    pub names: Vec<String>,
    pub settings: ArunaStepSettings,
}

/// The settings by step with tool names for every composed step, so sibling steps using the same
/// tool with default settings are flagged as ambiguous rather than inheriting an override.
pub fn step_matches(
    entry: &Path,
    settings: &BTreeMap<String, ArunaStepSettings>,
) -> Result<Vec<StepMatch>, String> {
    if uwe::tools::composed::is_composed(entry) && !settings.is_empty() {
        let mut library = ToolLibrary::new();
        let workflow = uwe::tools::composed::load(entry, &mut library)?;
        let mut matches = Vec::with_capacity(workflow.graph.nodes.len());
        let mut seen = std::collections::BTreeSet::new();
        for node in &workflow.graph.nodes {
            let mut names = vec![node.id.clone()];
            if let Some(found) = library.get(&node.tool, Some(&node.version)) {
                let definition = &found.definition;
                names.push(definition.id.clone());
                names.push(definition.title.clone());
            }
            seen.insert(node.id.clone());
            matches.push(StepMatch {
                step: node.id.clone(),
                names,
                settings: settings.get(&node.id).cloned().unwrap_or_default(),
            });
        }
        for (step, entry) in settings {
            if !seen.contains(step) {
                matches.push(StepMatch {
                    step: step.clone(),
                    names: vec![step.clone()],
                    settings: entry.clone(),
                });
            }
        }
        return Ok(matches);
    }
    Ok((settings.iter())
        .map(|(step, settings)| StepMatch {
            step: step.clone(),
            names: vec![step.clone()],
            settings: settings.clone(),
        })
        .collect())
}

/// The step names allowed open network access, refusing any override whose tool aliases also
/// match a sibling step that keeps network isolation.
pub fn allowed_network_steps(matches: &[StepMatch]) -> Result<Vec<String>, String> {
    let mut allowed = Vec::new();
    for entry in matches
        .iter()
        .filter(|entry| entry.settings.execution.network == Some(true))
    {
        for other in matches.iter().filter(|other| {
            other.step != entry.step && other.settings.execution.network != Some(true)
        }) {
            if let Some(shared) = entry
                .names
                .iter()
                .find(|name| other.names.iter().any(|candidate| candidate == *name))
            {
                return Err(format!(
                    "step {} enables network access while step {} shares tool name {shared} with \
                     network isolated; give them distinct tools",
                    entry.step, other.step
                ));
            }
        }
        for name in &entry.names {
            if !allowed.contains(name) {
                allowed.push(name.clone());
            }
        }
    }
    Ok(allowed)
}

/// The settings of the tasks of `step`, as uwe names it. Two steps that share a name but not
/// their settings cannot be told apart, which is refused rather than guessed.
pub fn settings_for(
    matches: &[StepMatch],
    step: &str,
) -> Result<Option<ArunaStepSettings>, String> {
    let named = |name: &String| {
        name == step || step.ends_with(&format!(".{name}")) || step.ends_with(&format!(":{name}"))
    };
    let found: Vec<&StepMatch> = (matches.iter())
        .filter(|entry| entry.names.iter().any(named))
        .collect();
    match found.as_slice() {
        [] => Ok(None),
        [first, rest @ ..] if rest.iter().all(|other| other.settings == first.settings) => {
            Ok(Some(first.settings.clone()))
        }
        several => {
            let steps: Vec<&str> = several.iter().map(|entry| entry.step.as_str()).collect();
            Err(format!(
                "the steps {} run tasks named {step} with different Aruna settings, and uwe \
                 does not say which step a task belongs to; give them distinct tools",
                steps.join(", ")
            ))
        }
    }
}

/// The step graph of a composed workflow: its inputs, steps, outputs and the edges between.
pub fn graph_of(workflow: &ComposedWorkflow) -> WorkflowGraph {
    let mut graph = WorkflowGraph::default();
    for input in &workflow.inputs {
        let id = format!("input:{}", input.name);
        graph.nodes.push(node(&id, &input.name, NodeKind::Input));
        for end in &input.to {
            graph.edges.push(GraphEdge {
                from: id.clone(),
                to: end.node.clone(),
                from_port: Some(input.name.clone()),
                to_port: Some(end.port.clone()),
            });
        }
    }
    for step in &workflow.graph.nodes {
        let mut entry = node(&step.id, &step.id, NodeKind::Step);
        entry.tool = Some(step.tool.clone());
        entry.version = (!step.version.is_empty()).then(|| step.version.clone());
        graph.nodes.push(entry);
    }
    for edge in &workflow.graph.edges {
        graph.edges.push(GraphEdge {
            from: edge.from.node.clone(),
            to: edge.to.node.clone(),
            from_port: Some(edge.from.port.clone()),
            to_port: Some(edge.to.port.clone()),
        });
    }
    for output in &workflow.outputs {
        let id = format!("output:{}", output.name);
        graph.nodes.push(node(&id, &output.name, NodeKind::Output));
        graph.edges.push(GraphEdge {
            from: output.from.node.clone(),
            to: id,
            from_port: Some(output.from.port.clone()),
            to_port: Some(output.name.clone()),
        });
    }
    graph
}

fn node(id: &str, label: &str, kind: NodeKind) -> GraphNode {
    GraphNode {
        id: id.to_owned(),
        label: label.to_owned(),
        kind,
        tool: None,
        version: None,
    }
}

/// The line of a step in the `steps` mapping, or of one of its keys, counting from 1.
pub fn step_line(text: &str, step: &str, key: Option<&str>) -> Option<u32> {
    let lines: Vec<&str> = text.lines().collect();
    let indent = |line: &str| line.len() - line.trim_start().len();
    let opens = |line: &str, name: &str| {
        let trimmed = line.trim_start();
        let quoted = [
            format!("{name}:"),
            format!("\"{name}\":"),
            format!("'{name}':"),
        ];
        quoted
            .iter()
            .any(|start| trimmed.starts_with(start.as_str()))
    };
    let steps = lines
        .iter()
        .position(|line| indent(line) == 0 && opens(line, "steps"))?;
    let mut found = None;
    for (number, line) in lines.iter().enumerate().skip(steps + 1) {
        if !line.trim().is_empty() && !line.trim_start().starts_with('#') && indent(line) == 0 {
            break;
        }
        if indent(line) > 0 && opens(line, step) {
            found = Some((number, indent(line)));
            break;
        }
    }
    let (start, depth) = found?;
    let Some(key) = key else {
        return u32::try_from(start + 1).ok();
    };
    for (number, line) in lines.iter().enumerate().skip(start + 1) {
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        if indent(line) <= depth {
            break;
        }
        if opens(line, key) {
            return u32::try_from(number + 1).ok();
        }
    }
    u32::try_from(start + 1).ok()
}

/// A pointer to the step a uwe message names as `step <id>: ...`.
fn pointer_of(text: &str, message: &str) -> DiagnosticPointer {
    let step = (message.strip_prefix("step "))
        .and_then(|rest| rest.split_once(':'))
        .map(|(id, _)| id.to_owned());
    DiagnosticPointer {
        line: step.as_deref().and_then(|id| step_line(text, id, None)),
        node: step,
        ..DiagnosticPointer::default()
    }
}

fn by_fit(fit: Fit, message: String, pointer: DiagnosticPointer) -> Diagnostic {
    match fit {
        Fit::DoesNotFit => error(message, pointer),
        Fit::Unknown | Fit::Fits => warning(message, pointer),
    }
}

pub fn error(message: impl Into<String>, pointer: DiagnosticPointer) -> Diagnostic {
    Diagnostic {
        severity: Severity::Error,
        message: message.into(),
        pointer,
    }
}

pub fn warning(message: impl Into<String>, pointer: DiagnosticPointer) -> Diagnostic {
    Diagnostic {
        severity: Severity::Warning,
        message: message.into(),
        pointer,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT: &str = "uwe_version: \"1\"\nname: qc\nsteps:\n  count:\n    tool: count\n    \
                        aruna:\n      resources:\n        cpus: 2\n  sort:\n    tool: sort\n    \
                        aruna:\n      resources:\n        cpus: 0\n      environment:\n        \
                        container: x\n        conda: y\n";

    #[test]
    fn strips_step_settings() {
        let stripped = strip_settings(TEXT);
        let value = stripped.value.as_ref().expect("reads");
        assert!(value["steps"]["count"].get(SETTINGS_KEY).is_none());
        assert!(value["steps"]["sort"].get(SETTINGS_KEY).is_none());
        assert_eq!(stripped.settings.len(), 1);
        assert_eq!(stripped.settings["count"].resources.cpus, Some(2.0));
        let fields: Vec<_> = (stripped.errors())
            .map(|diagnostic| (diagnostic.pointer.field.clone(), diagnostic.pointer.line))
            .collect();
        assert_eq!(
            fields,
            [
                (Some("aruna.environment".into()), Some(11)),
                (Some("aruna.resources.cpus".into()), Some(11)),
            ]
        );
        let reread: Value = serde_json::from_str(&stripped.document).expect("JSON");
        assert_eq!(&reread, value);
    }

    #[test]
    fn matches_tool_steps() {
        let settings = |cpus: f64| {
            ArunaStepSettings::from_value(serde_json::json!({"resources": {"cpus": cpus}}))
                .expect("reads")
        };
        let entry = |step: &str, names: &[&str], cpus: f64| StepMatch {
            step: step.into(),
            names: names.iter().map(|name| (*name).to_owned()).collect(),
            settings: settings(cpus),
        };
        let matches = [
            entry("first", &["first", "reverse", "reverse"], 1.0),
            entry("shout", &["shout", "upper", "UPPER"], 2.0),
        ];
        assert_eq!(settings_for(&matches, "reverse"), Ok(Some(settings(1.0))));
        assert_eq!(settings_for(&matches, "wf:UPPER"), Ok(Some(settings(2.0))));
        assert_eq!(settings_for(&matches, "other"), Ok(None));
        let shared = [
            matches[0].clone(),
            entry("second", &["second", "reverse"], 3.0),
        ];
        assert!(settings_for(&shared, "reverse").is_err());
        let same = [
            matches[0].clone(),
            entry("second", &["second", "reverse"], 1.0),
        ];
        assert_eq!(settings_for(&same, "reverse"), Ok(Some(settings(1.0))));
    }

    #[test]
    fn rejects_shared_network() {
        let open =
            ArunaStepSettings::from_value(serde_json::json!({"execution": {"network": true}}))
                .expect("reads");
        let isolated = ArunaStepSettings::default();
        let matches = [
            StepMatch {
                step: "fetch".into(),
                names: vec!["fetch".into(), "curl".into(), "Curl".into()],
                settings: open,
            },
            StepMatch {
                step: "check".into(),
                names: vec!["check".into(), "curl".into(), "Curl".into()],
                settings: isolated,
            },
        ];
        let err = allowed_network_steps(&matches).unwrap_err();
        assert!(err.contains("shares tool name curl"), "{err}");
    }

    #[test]
    fn finds_step_lines() {
        assert_eq!(step_line(TEXT, "count", None), Some(4));
        assert_eq!(step_line(TEXT, "count", Some("aruna")), Some(6));
        assert_eq!(step_line(TEXT, "sort", Some("tool")), Some(10));
        assert_eq!(step_line(TEXT, "missing", None), None);
    }
}
