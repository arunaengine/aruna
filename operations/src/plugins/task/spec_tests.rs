//! Tests the pure task spec build: what a recipe and a run's request become, and every refusal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::lifecycle::screen::tests::install;

fn recipe() -> Recipe {
    Recipe {
        image: format!("harbor.example/map@sha256:{}", "a".repeat(64)),
        command: Some(vec!["/opt/map.sh".to_string(), "{reference}".to_string()]),
        cpu: 8,
        memory: 32 << 30,
        time: 3600,
        network: Network::Open,
        parameters: BTreeMap::from([(
            "reference".to_string(),
            aruna_plugin_types::recipe::DEFAULT_PATTERN.to_string(),
        )]),
        report: Some("/out/report.json".to_string()),
    }
}

fn values() -> BTreeMap<String, String> {
    BTreeMap::from([("reference".to_string(), "GCF_000005845.2".to_string())])
}

fn io() -> TaskIo {
    TaskIo {
        objects: vec![(
            "/in/reads/".to_string(),
            "raw".to_string(),
            "run-7/".to_string(),
            None,
        )],
        files: vec![("/in/samples.tsv".to_string(), b"reads.fastq\tA\n".to_vec())],
        outputs: vec![(
            "/out/results/".to_string(),
            "results".to_string(),
            "run-7/".to_string(),
        )],
    }
}

#[test]
fn builds_task_spec() {
    let work_id = Ulid::from_bytes([6; 16]);
    let spec = task_spec(&install(), work_id, (&recipe(), &values()), &io()).unwrap();
    assert_eq!(
        spec.tags[PLUGIN_INSTALL_TAG],
        Ulid::from_bytes([1; 16]).to_string()
    );
    assert_eq!(spec.tags[PLUGIN_WORK_TAG], work_id.to_string());
    assert_eq!(spec.tags[PLUGIN_REPORT_TAG], "/out/report.json");
    assert_eq!(spec.tags[NETWORK_TAG_KEY], "open");
    assert_eq!(
        spec.entrypoint,
        Some(vec![
            "/opt/map.sh".to_string(),
            "GCF_000005845.2".to_string()
        ])
    );
    assert!(spec.command.is_empty());
    assert_eq!(spec.resources.cpu_cores, Some(8));
    assert_eq!(spec.resources.ram_bytes, Some(32 << 30));
    assert_eq!(spec.resources.max_walltime_ms, Some(3_600_000));
    assert!(matches!(&spec.inputs[0].source, InputSource::S3 { key, .. } if key == "run-7/"));
    assert!(matches!(&spec.inputs[1].source, InputSource::Inline { bytes } if bytes.len() == 14));
    let output = &spec.file_outputs[0];
    assert_eq!(output.container_path, "/out/results/**");
    assert_eq!(output.path_prefix.as_deref(), Some("/out/results"));
    assert_eq!(
        output.destination,
        OutputDestination::S3 {
            bucket: "results".to_string(),
            key: "run-7".to_string()
        }
    );
    // No network unless the recipe opens it; the image's entrypoint without a command.
    let mut closed = recipe();
    (closed.network, closed.command) = (Network::None, None);
    closed.parameters.clear();
    let spec = task_spec(&install(), work_id, (&closed, &BTreeMap::new()), &io()).unwrap();
    assert!(!spec.tags.contains_key(NETWORK_TAG_KEY));
    assert_eq!(spec.entrypoint, None);
}

#[test]
fn refuses_bad_request() {
    let refused = |io: TaskIo| task_spec(&install(), Ulid::nil(), (&recipe(), &values()), &io);
    let mut twice = io();
    twice.files[0].0 = "/in/reads".to_string();
    assert!(refused(twice).is_err());
    let mut relative = io();
    relative.files[0].0 = "in/samples.tsv".to_string();
    assert!(refused(relative).is_err());
    let mut file_output = io();
    file_output.outputs[0].0 = "/out/results".to_string();
    assert!(refused(file_output).is_err());
    let mut large = io();
    large.files[0].1 = vec![b'a'; MAX_INLINE_BYTES + 1];
    assert!(refused(large).is_err());
    let mut covering = io();
    covering.outputs[0].0 = "/out/".to_string();
    assert!(
        refused(covering).is_err(),
        "an output may not cover the report"
    );
    let mut outputless = io();
    outputless.outputs.clear();
    assert!(refused(outputless).is_err(), "a task needs an output");
    let wrong = BTreeMap::from([("reference".to_string(), "--rm".to_string())]);
    assert!(task_spec(&install(), Ulid::nil(), (&recipe(), &wrong), &io()).is_err());
}
