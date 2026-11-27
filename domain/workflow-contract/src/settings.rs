//! The `aruna` key of a composed workflow step: its environment, resources, placement and
//! execution, with the portal's key names and binary sizes, and the checks of its values.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

/// The step key in a `.uwe.yaml` file that holds these settings.
pub const SETTINGS_KEY: &str = "aruna";

const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

/// What Aruna adds to one step; every part and every value may be left out.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArunaStepSettings {
    #[serde(default, skip_serializing_if = "StepEnvironment::is_empty")]
    pub environment: StepEnvironment,
    #[serde(default, skip_serializing_if = "StepResources::is_empty")]
    pub resources: StepResources,
    #[serde(default, skip_serializing_if = "StepPlacement::is_empty")]
    pub placement: StepPlacement,
    #[serde(default, skip_serializing_if = "StepExecution::is_empty")]
    pub execution: StepExecution,
}

/// Where the step's tool comes from: a container image or a Conda environment.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepEnvironment {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conda: Option<String>,
}

/// The size of each task of the step. Memory and disk are in GiB, 1024 MiB each.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepResources {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpus: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_gib: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_gib: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpus: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub walltime_minutes: Option<u64>,
    /// How often a failed task runs again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,
}

/// Where the tasks of the step must run; a named node or site never falls back silently.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepPlacement {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,
}

/// How the step spreads over its inputs and whether its tasks reach the network.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepExecution {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scatter: Option<ScatterMode>,
    /// How many tasks of the step run at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<u32>,
    /// False runs the step without network, which makes its results reusable from the cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<bool>,
}

/// How a step pairs the items of the lists it maps over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScatterMode {
    None,
    Dot,
    FlatCross,
    NestedCross,
}

/// One value that does not pass a check, by its dotted key, like `resources.cpus`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettingsIssue {
    pub field: String,
    pub message: String,
}

impl ArunaStepSettings {
    /// Reads the JSON value of an `aruna` key, refusing unknown keys and mistyped values.
    pub fn from_value(value: serde_json::Value) -> Result<ArunaStepSettings, SettingsIssue> {
        serde_json::from_value(value).map_err(|error| SettingsIssue {
            field: SETTINGS_KEY.into(),
            message: error.to_string(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self == &ArunaStepSettings::default()
    }

    /// Every value that is out of range or ill-formed; empty when all pass.
    pub fn validate(&self) -> Vec<SettingsIssue> {
        let mut issues = Vec::new();
        let mut refuse = |field: &str, message: &str| {
            issues.push(SettingsIssue {
                field: field.into(),
                message: message.into(),
            });
        };
        let environment = &self.environment;
        for (field, value) in [
            ("environment.container", &environment.container),
            ("environment.conda", &environment.conda),
            ("placement.node", &self.placement.node),
            ("placement.site", &self.placement.site),
        ] {
            if value.as_deref().is_some_and(|text| !is_name(text)) {
                refuse(
                    field,
                    "must be a nonempty name without spaces or control characters",
                );
            }
        }
        if environment.container.is_some() && environment.conda.is_some() {
            refuse(
                "environment",
                "set either a container or a Conda environment, not both",
            );
        }
        let resources = &self.resources;
        for (field, value) in [
            ("resources.cpus", resources.cpus),
            ("resources.memory_gib", resources.memory_gib),
            ("resources.disk_gib", resources.disk_gib),
        ] {
            if value.is_some_and(|number| !number.is_finite() || number <= 0.0) {
                refuse(field, "must be a number above zero");
            }
        }
        if resources.walltime_minutes == Some(0) {
            refuse("resources.walltime_minutes", "must be at least one minute");
        }
        if self.execution.parallelism == Some(0) {
            refuse("execution.parallelism", "must be at least one");
        }
        issues
    }

    /// The memory of a task in bytes, rounded up.
    pub fn memory_bytes(&self) -> Option<u64> {
        self.resources.memory_gib.map(gib_bytes)
    }

    /// The disk of a task in bytes, rounded up.
    pub fn disk_bytes(&self) -> Option<u64> {
        self.resources.disk_gib.map(gib_bytes)
    }

    pub fn walltime_seconds(&self) -> Option<u64> {
        (self.resources.walltime_minutes).map(|minutes| minutes.saturating_mul(60))
    }
}

impl StepEnvironment {
    pub fn is_empty(&self) -> bool {
        self == &StepEnvironment::default()
    }
}

impl StepResources {
    pub fn is_empty(&self) -> bool {
        self == &StepResources::default()
    }
}

impl StepPlacement {
    pub fn is_empty(&self) -> bool {
        self == &StepPlacement::default()
    }
}

impl StepExecution {
    pub fn is_empty(&self) -> bool {
        self == &StepExecution::default()
    }
}

fn is_name(text: &str) -> bool {
    !text.is_empty()
        && !text
            .chars()
            .any(|letter| letter.is_whitespace() || letter.is_control())
}

/// Saturates at `u64::MAX` for sizes beyond it; the check refuses values that are not finite.
fn gib_bytes(gib: f64) -> u64 {
    (gib * GIB).ceil() as u64
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn read(value: serde_json::Value) -> ArunaStepSettings {
        ArunaStepSettings::from_value(value).expect("reads")
    }

    #[test]
    fn reads_portal_keys() {
        let settings = read(json!({
            "environment": {"container": "quay.io/biocontainers/fastqc:0.12.1"},
            "resources": {"cpus": 2, "memory_gib": 1.5, "disk_gib": 10, "gpus": 1,
                "walltime_minutes": 90, "retries": 2},
            "placement": {"node": "node-a", "site": "giessen"},
            "execution": {"scatter": "flat_cross", "parallelism": 4, "network": false},
        }));
        assert!(settings.validate().is_empty());
        assert_eq!(settings.resources.cpus, Some(2.0));
        assert_eq!(settings.memory_bytes(), Some(1536 * 1024 * 1024));
        assert_eq!(settings.disk_bytes(), Some(10 << 30));
        assert_eq!(settings.walltime_seconds(), Some(5400));
        assert_eq!(settings.execution.scatter, Some(ScatterMode::FlatCross));
        assert_eq!(settings.execution.network, Some(false));
        let written = serde_json::to_value(&settings).expect("writes");
        assert_eq!(read(written), settings);
    }

    #[test]
    fn omits_empty_parts() {
        let settings = read(json!({"resources": {"cpus": 1}}));
        let written = serde_json::to_value(&settings).expect("writes");
        assert_eq!(written, json!({"resources": {"cpus": 1.0}}));
        assert!(read(json!({})).is_empty());
    }

    #[test]
    fn refuses_unknown_keys() {
        for value in [
            json!({"resource": {}}),
            json!({"resources": {"memory": 4}}),
            json!({"execution": {"scatter": "zip"}}),
            json!({"resources": {"gpus": -1}}),
            json!({"placement": {"node": 3}}),
        ] {
            let issue = ArunaStepSettings::from_value(value.clone()).expect_err("refused");
            assert_eq!(issue.field, SETTINGS_KEY, "{value}");
        }
    }

    #[test]
    fn validates_values() {
        let settings = read(json!({
            "environment": {"container": "has space", "conda": "env"},
            "resources": {"cpus": 0, "memory_gib": -1, "disk_gib": 0.5, "walltime_minutes": 0},
            "placement": {"site": ""},
            "execution": {"parallelism": 0},
        }));
        let fields: Vec<String> = (settings.validate().into_iter())
            .map(|issue| issue.field)
            .collect();
        assert_eq!(
            fields,
            [
                "environment.container",
                "placement.site",
                "environment",
                "resources.cpus",
                "resources.memory_gib",
                "resources.walltime_minutes",
                "execution.parallelism",
            ]
        );
    }
}
