//! Runs the sync natively against the fake eLabFTW and Aruna: links, both directions, conflicts,
//! reservations, recovery, file merges, bodies, drift and the report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod support;

use elabftw_sync::host::Kind;
use elabftw_sync::progress::{DAY_MS, STALE_MS};
use elabftw_sync::sync::Mode;
use serde_json::{Value, json};
use support::{BUCKET, Runner, START};

#[test]
fn enrols_tagged_experiment() {
    let runner = Runner::new();
    let id = runner
        .world
        .experiment("Run 7", "<p>Mix <b>A</b></p>", &["aruna", "pcr"]);
    runner.world.experiment("Not shared", "", &["other"]);
    runner.world.upload(id, 1, "gel.png", 42);
    runner.poll();

    let (dataset, stored) = runner.world.only_dataset();
    assert_eq!(stored.path, format!("elabftw/{id}"));
    let entity = &stored.entity;
    assert_eq!(entity["name"], "Run 7");
    assert_eq!(
        entity["keywords"],
        json!(["pcr"]),
        "the sync tag is no keyword"
    );
    assert_eq!(entity["text"], "<p>Mix <b>A</b></p>");
    assert_eq!(entity["description"], "Mix A");
    assert_eq!(entity["encodingFormat"], "text/html");
    assert_eq!(entity["creativeWorkStatus"], "Running");
    assert_eq!(entity["identifier"]["elabftw"]["id"], id.to_string());
    let file = &stored.files["elabftw/uploads/1"];
    assert_eq!(
        file["contentUrl"],
        format!("{BUCKET}/elabftw/ab/1-uuid.bin")
    );
    assert_eq!(
        (file["name"].clone(), file["contentSize"].clone()),
        (json!("gel.png"), json!("42"))
    );
    let extra = runner.world.extra(id);
    assert_eq!(extra["Aruna dataset id"]["value"], dataset);
    assert_eq!(
        extra["Aruna dataset"]["value"],
        format!("https://portal.example.org/datasets/{dataset}")
    );
    assert_eq!(extra["Aruna files"]["value"], json!([]));

    // Nothing changed, so a second pass writes nothing on either side.
    let (revision, modified) = (stored.revision, runner.world.get(id)["modified_at"].clone());
    runner.poll();
    runner.event(&dataset);
    assert_eq!(runner.world.dataset(&dataset).revision, revision);
    assert_eq!(runner.world.get(id)["modified_at"], modified);
    assert_eq!(runner.marks(id), Value::Null);
}

#[test]
fn link_fields_repaired() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    runner.world.edit(id, |experiment| {
        let mine = json!({ "extra_fields": { "Sample": { "type": "text", "value": "S1" } } });
        experiment["metadata"] = json!(mine.to_string());
    });
    runner.poll();
    let (dataset, _) = runner.world.only_dataset();
    let extra = runner.world.extra(id);
    assert_eq!(
        (
            extra["Sample"]["value"].clone(),
            extra["Aruna dataset id"]["value"].clone()
        ),
        (json!("S1"), json!(dataset))
    );

    // A person deletes a link field; the next sync of the experiment creates it again.
    runner.world.edit(id, |experiment| {
        let mut extra = extra_fields(experiment);
        extra.remove("Aruna files");
        experiment["metadata"] = json!(json!({ "extra_fields": extra }).to_string());
        experiment["title"] = json!("Run 8");
    });
    runner.poll();
    let extra = runner.world.extra(id);
    assert_eq!(extra["Aruna files"]["value"], json!([]));
    assert_eq!(extra["Sample"]["value"], "S1");
    assert_eq!(runner.marks(id), Value::Null);
}

fn extra_fields(experiment: &Value) -> serde_json::Map<String, Value> {
    let metadata: Value = serde_json::from_str(experiment["metadata"].as_str().unwrap()).unwrap();
    metadata["extra_fields"].as_object().unwrap().clone()
}

#[test]
fn title_both_ways() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "<p>x</p>", &["aruna"]);
    runner.poll();
    let (dataset, _) = runner.world.only_dataset();

    runner
        .world
        .edit(id, |experiment| experiment["title"] = json!("Run 8"));
    runner.poll();
    assert_eq!(runner.world.dataset(&dataset).entity["name"], "Run 8");

    runner
        .world
        .aruna_edit(&dataset, json!({ "name": "Run 9" }));
    runner.event(&dataset);
    assert_eq!(runner.world.get(id)["title"], "Run 9");
    // The plugin's own write comes back in the next pass and is skipped.
    let revision = runner.world.dataset(&dataset).revision;
    runner.poll();
    assert_eq!(runner.world.dataset(&dataset).revision, revision);
}

#[test]
fn conflict_then_resolved() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "<p>x</p>", &["aruna"]);
    runner.poll();
    let (dataset, _) = runner.world.only_dataset();
    runner
        .world
        .edit(id, |experiment| experiment["title"] = json!("From eLabFTW"));
    runner.world.aruna_edit(
        &dataset,
        json!({ "name": "From Aruna", "keywords": ["new"] }),
    );
    runner.poll();

    // The title is not overwritten either way; the keyword change merges as usual.
    assert_eq!(runner.world.get(id)["title"], "From eLabFTW");
    assert_eq!(runner.world.dataset(&dataset).entity["name"], "From Aruna");
    assert_eq!(runner.world.get(id)["tags"], "aruna|new");
    let conflict = runner.marks(id)["conflict"].clone();
    assert!(
        conflict[0]
            .as_str()
            .unwrap()
            .starts_with("title elabftw 12:"),
        "{conflict}"
    );
    let report = runner.manual(json!({ "report": true }));
    assert_eq!(report["totals"]["conflict"], 1);
    assert_eq!(report["marks"][0]["experiment"], id);

    runner.manual(json!({ "resolve": id, "keep": "aruna" }));
    assert_eq!(runner.world.get(id)["title"], "From Aruna");
    assert_eq!(runner.marks(id), Value::Null);
    let report = runner.manual(json!({ "report": true }));
    assert_eq!(report["totals"]["conflict"], 0);
}

#[test]
fn tag_change_reconciled() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna", "a"]);
    runner.poll();
    let (dataset, _) = runner.world.only_dataset();
    // A tag change leaves `modified_at` alone, so the quick pass cannot see it.
    runner.world.retag(id, &["aruna", "a", "b"]);
    runner.poll();
    assert_eq!(
        runner.world.dataset(&dataset).entity["keywords"],
        json!(["a"])
    );
    runner.manual(json!({ "reconcile": true }));
    assert_eq!(
        runner.world.dataset(&dataset).entity["keywords"],
        json!(["a", "b"])
    );

    // Removing the sync tag ends nothing; the link is marked gone.
    runner.world.retag(id, &["a", "b"]);
    runner.poll();
    runner.now.set(runner.now.get() + DAY_MS);
    runner.poll();
    runner.poll();
    assert_eq!(runner.marks(id)["gone"], json!(["tag"]));
    let log = runner.world.lock().log.clone();
    assert!(
        log.iter().any(|line| line.contains("\"gone\":1")),
        "{log:?}"
    );
}

#[test]
fn failed_link_retried() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    runner.poll();
    let (dataset, _) = runner.world.only_dataset();
    runner.world.lock().edit_mode = true;
    runner
        .world
        .aruna_edit(&dataset, json!({ "name": "Run 8" }));
    runner.event(&dataset);
    let failed = runner.marks(id)["failed"][0].as_str().unwrap().to_string();
    assert!(failed.contains("exclusive edit mode"), "{failed}");
    assert!(runner.link(id)["retry_at"].as_u64().is_some());

    runner.world.lock().edit_mode = false;
    runner.poll();
    assert_eq!(runner.world.get(id)["title"], "Run 7", "not due yet");
    runner.now.set(runner.now.get() + 20 * 60 * 1000);
    runner.poll();
    assert_eq!(runner.world.get(id)["title"], "Run 8");
    assert_eq!(runner.marks(id), Value::Null);
}

#[test]
fn reservation_blocks_other() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    let held = json!({ "reservation": { "kind": "dataset", "run": "other", "at": START + 1 } });
    runner.world.put_state(&format!("link/{id}"), &held);
    runner.poll();
    assert!(
        runner.world.lock().datasets.is_empty(),
        "another run holds the link"
    );

    runner.now.set(START + STALE_MS + 2);
    runner.poll();
    assert_eq!(
        runner.world.lock().datasets.len(),
        1,
        "a stale reservation is taken over"
    );
}

#[test]
fn keyed_create_recovered() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    runner.world.lock().create_unknown = true;
    runner.at(|sync| sync.experiment(id, Mode::Quick)).unwrap();
    let failed = runner.marks(id)["failed"].clone();
    assert!(failed.to_string().contains("may have happened"), "{failed}");
    runner
        .world
        .edit(id, |experiment| experiment["title"] = json!("Run 7b"));
    runner.now.set(runner.now.get() + 20 * 60 * 1000);
    runner.poll();

    // The same key gives the same dataset; the edit since the first attempt is an update.
    let (dataset, stored) = runner.world.only_dataset();
    assert_eq!(stored.entity["name"], "Run 7b");
    assert_eq!(runner.link(id)["dataset"], dataset);
    assert_eq!(
        runner.world.state(&format!("dataset/{dataset}")).unwrap()["experiment"],
        id
    );
}

#[test]
fn refused_create_corrected() {
    let runner = Runner::new();
    let id = runner.world.experiment("", "", &["aruna"]);
    runner.poll();
    assert!(
        runner.marks(id)["failed"]
            .to_string()
            .contains("needs a name")
    );
    runner
        .world
        .edit(id, |experiment| experiment["title"] = json!("Named"));
    runner.now.set(runner.now.get() + 20 * 60 * 1000);
    runner.poll();
    let (_, stored) = runner.world.only_dataset();
    assert_eq!(stored.entity["name"], "Named");
}

#[test]
fn aruna_dataset_creates() {
    let runner = Runner::new();
    let entity = json!({ "@id": "./", "@type": "Dataset", "name": "From Aruna", "keywords": ["x"], "text": "<p>a<br>b</p>", "encodingFormat": "text/html" });
    let dataset = runner.world.aruna_dataset("elabftw/study", entity);
    runner
        .world
        .aruna_file(&dataset, json!({ "@id": "raw.csv", "@type": "File" }));
    runner.event(&dataset);

    let experiment = runner
        .world
        .lock()
        .experiments
        .keys()
        .copied()
        .next()
        .unwrap();
    let stored = runner.world.get(experiment);
    assert_eq!(stored["title"], "From Aruna");
    assert_eq!(stored["tags"], "x|aruna");
    let extra = runner.world.extra(experiment);
    let file = format!("https://portal.example.org/datasets/{dataset}/files/raw.csv");
    assert_eq!(extra["Aruna files"]["value"], json!([file]));
    // eLabFTW's filtered body comes back as the text, with the description and the identifier.
    let entity = runner.world.dataset(&dataset).entity;
    assert_eq!(entity["text"], "<p>a<br />b</p>");
    assert_eq!(entity["description"], "a\nb");
    assert_eq!(
        entity["identifier"]["elabftw"]["id"],
        experiment.to_string()
    );
    assert_eq!(runner.world.lock().posts, 1);

    // A dataset elsewhere is not the plugin's.
    let other = runner
        .world
        .aruna_dataset("other/place", json!({ "name": "x" }));
    runner.event(&other);
    assert_eq!(runner.world.lock().posts, 1);
}

#[test]
fn unknown_post_kept() {
    let runner = Runner::new();
    let dataset = runner
        .world
        .aruna_dataset("elabftw/study", json!({ "name": "Study" }));
    runner.world.lock().post_outcome = Some((Kind::Unknown, false));
    runner.event(&dataset);
    let pointer = runner.world.state(&format!("dataset/{dataset}")).unwrap();
    assert_eq!(pointer["reservation"]["unknown"], true);
    assert!(pointer["marks"]["unknown_create"].is_array());

    // A negative lookup keeps the reservation: no second `POST` without a person.
    runner.event(&dataset);
    runner.poll();
    assert_eq!(runner.world.lock().posts, 1);
    assert!(runner.world.lock().experiments.is_empty());

    runner.manual(json!({ "resolve_create": dataset, "experiment": "absent" }));
    assert_eq!(runner.world.lock().posts, 2);
    assert_eq!(runner.world.lock().experiments.len(), 1);
    let pointer = runner.world.state(&format!("dataset/{dataset}")).unwrap();
    assert!(
        pointer["experiment"].is_u64() && pointer["marks"].is_null(),
        "{pointer}"
    );
}

#[test]
fn unknown_post_found() {
    let runner = Runner::new();
    let dataset = runner
        .world
        .aruna_dataset("elabftw/study", json!({ "name": "Study" }));
    runner.world.lock().post_outcome = Some((Kind::Unknown, true));
    runner.event(&dataset);
    // The first `POST` did land: the lookup by the dataset id field finds and adopts it.
    runner.poll();
    let experiments: Vec<u64> = runner.world.lock().experiments.keys().copied().collect();
    assert_eq!((experiments.len(), runner.world.lock().posts), (1, 1));
    assert_eq!(
        runner.world.state(&format!("dataset/{dataset}")).unwrap()["experiment"],
        experiments[0]
    );
    assert_eq!(runner.link(experiments[0])["dataset"], dataset);
}

#[test]
fn unsent_post_retried() {
    let runner = Runner::new();
    let dataset = runner
        .world
        .aruna_dataset("elabftw/study", json!({ "name": "Study" }));
    runner.world.lock().post_outcome = Some((Kind::Unavailable, false));
    runner.event(&dataset);
    assert!(runner.world.lock().experiments.is_empty());
    runner.now.set(runner.now.get() + 20 * 60 * 1000);
    runner.poll();
    assert_eq!(runner.world.lock().experiments.len(), 1);
}
