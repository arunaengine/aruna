//! Marks, the report and bounds against the fake eLabFTW and Aruna: drift, storage sizes, too
//! large messages, blocked merges, write access, file merges and the paged report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod support;

use std::collections::BTreeSet;

use aruna_plugins::limits::Limits;
use elabftw_sync::compare::Snapshot;
use elabftw_sync::host::{Host, Kind, Meter};
use elabftw_sync::progress::{DAY_MS, MAX_VALUE, Mark, PART_BYTES, REPORT_BYTES, Record, store};
use elabftw_sync::sync::{Budget, Room, Sync};
use serde_json::{Value, json};
use support::{BUCKET, Runner, World, settings};

#[test]
fn drift_reported() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "<p>x</p>", &["aruna"]);
    runner.poll();
    let (dataset, _) = runner.world.only_dataset();
    // eLabFTW answers the title write but keeps nothing, so the two sides drift apart.
    runner.world.lock().drop_writes = true;
    runner
        .world
        .aruna_edit(&dataset, json!({ "name": "Run 8" }));
    runner.event(&dataset);
    runner.world.lock().drop_writes = false;
    assert_eq!(
        runner.marks(id),
        Value::Null,
        "the quick sync cannot see a lost write"
    );

    runner.manual(json!({ "reconcile": true }));
    let drift = runner.marks(id)["out_of_sync"].clone();
    assert!(drift[0].as_str().unwrap().starts_with("title "), "{drift}");
    let report = runner.manual(json!({ "report": true }));
    assert_eq!(report["totals"]["out_of_sync"], 1);
    assert_eq!(report["next"], Value::Null);

    // A link with a change pending is not drift: an eLabFTW edit is synced, not marked.
    runner
        .world
        .edit(id, |experiment| experiment["title"] = json!("Run 9"));
    runner.manual(json!({ "reconcile": true }));
    assert_eq!(runner.world.dataset(&dataset).entity["name"], "Run 9");
    assert_eq!(runner.marks(id), Value::Null);
}

#[test]
fn storage_size_checked() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    runner.world.upload(id, 1, "a.bin", 10);
    runner.poll();
    let path = format!("{BUCKET}/elabftw/ab/1-uuid.bin");
    runner.world.lock().objects.insert(path, 11);
    runner.manual(json!({ "reconcile": true }));
    assert_eq!(runner.marks(id)["changed_in_storage"], json!(["upload 1"]));
}

#[test]
fn files_merge_annotations() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    runner.world.upload(id, 1, "a.bin", 10);
    runner.poll();
    let (dataset, stored) = runner.world.only_dataset();
    let mut file = stored.files["elabftw/uploads/1"].clone();
    (file["license"], file["comment"]) = (json!("CC-BY-4.0"), json!("checked"));
    runner.world.aruna_file(&dataset, file);
    runner
        .world
        .aruna_file(&dataset, json!({ "@id": "raw.csv", "@type": "File" }));

    // A replaced upload changes the sync-owned facts; a person's edit refuses the first write.
    runner.world.upload(id, 1, "a2.bin", 12);
    runner.world.lock().race_file = Some(json!({ "keywords": ["k"] }));
    runner.poll();
    let file = runner.world.dataset(&dataset).files["elabftw/uploads/1"].clone();
    assert_eq!(
        (file["name"].clone(), file["contentSize"].clone()),
        (json!("a2.bin"), json!("12"))
    );
    assert_eq!(
        (file["license"].clone(), file["comment"].clone()),
        (json!("CC-BY-4.0"), json!("checked"))
    );
    assert!(runner.world.extra(id).get("Aruna files").is_none());
    assert_eq!(runner.world.get(id)["tags"], "aruna|k");

    // A deleted upload leaves the dataset; Aruna's own file stays.
    runner.world.lock().uploads.get_mut(&id).unwrap()[0]["state"] = json!(3);
    runner.world.edit(id, |_| {});
    runner.poll();
    let files: Vec<String> = runner
        .world
        .dataset(&dataset)
        .files
        .keys()
        .cloned()
        .collect();
    assert_eq!(files, ["raw.csv"]);
}

#[test]
fn too_large_marked() {
    let runner = Runner::new();
    let big = runner.world.experiment("Big", "", &["aruna"]);
    let small = runner.world.experiment("Small", "", &["aruna"]);
    runner.world.lock().huge.insert(big);
    runner.poll();
    let marked = runner.marks(big)["too_large"][0]
        .as_str()
        .unwrap()
        .to_string();
    assert!(marked.contains("larger than the call allows"), "{marked}");
    assert_eq!(
        runner.link(small)["marks"],
        Value::Null,
        "other links still sync"
    );

    // A body whose write to eLabFTW would pass 1 MiB is marked and not sent: a description edit
    // whose escaped HTML grows past the bound, while the dataset read stays below it.
    runner.world.lock().huge.clear();
    runner.poll();
    let dataset = runner.link(small)["dataset"].as_str().unwrap().to_string();
    runner
        .world
        .aruna_edit(&dataset, json!({ "description": "<".repeat(270 << 10) }));
    runner.event(&dataset);
    let marked = runner.marks(small)["too_large"][0]
        .as_str()
        .unwrap()
        .to_string();
    assert!(marked.contains("eLabFTW write"), "{marked}");
    assert_eq!(runner.world.get(small)["body"], "");
}

/// A budget small enough that the tests' traffic spans several runs.
fn small(total: (u32, u64), link: (u32, u64), step: (u32, u64)) -> Budget {
    let room = |(calls, bytes)| Room { calls, bytes };
    Budget {
        total: room(total),
        link: room(link),
        step: room(step),
    }
}

#[test]
fn backlog_bounded() {
    // A large first backlog: every run stays in its budget and saves progress.
    let runner = Runner::new();
    let ids: Vec<u64> = (0..120)
        .map(|n| runner.world.experiment(&format!("E{n}"), "", &["aruna"]))
        .collect();
    runner
        .budget
        .set(small((150, 15 << 20), (40, 2 << 20), (20, 1 << 20)));
    let mut runs = 0;
    while runner.world.lock().datasets.len() < ids.len() {
        let before = runner.world.lock().datasets.len();
        runner.poll();
        runs += 1;
        let used = runner.used.get();
        assert!(used.calls <= 150, "run {runs} made {} calls", used.calls);
        assert!(
            runner.world.lock().datasets.len() > before,
            "run {runs} made progress"
        );
        assert!(runs < 100);
    }
    assert!(runs > 5, "the backlog took {runs} runs");
    // The walk ends within a few more runs, and then the runs find nothing new.
    let walking = |runner: &Runner| runner.world.state("cursor").unwrap().get("walk").is_some();
    for _ in 0..10 {
        if !walking(&runner) {
            break;
        }
        runner.poll();
        assert!(runner.used.get().calls <= 150);
    }
    assert!(!walking(&runner));
    assert_eq!(runner.poll()["synced"], 0);
}

#[test]
fn file_pages_resume() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    runner.poll();
    let (dataset, _) = runner.world.only_dataset();
    for n in 0..240 {
        runner.world.aruna_file(
            &dataset,
            json!({ "@id": format!("raw/{n:03}.csv"), "@type": "File" }),
        );
    }
    // 120 file pages need several runs; each continues the stored scan.
    runner
        .budget
        .set(small((60, 15 << 20), (30, 4 << 20), (15, 1 << 20)));
    runner.event(&dataset);
    assert!(
        runner.marks(id)["unfinished"].is_array(),
        "{}",
        runner.marks(id)
    );
    let mut runs = 1;
    while !runner.marks(id).is_null() {
        runner.poll();
        runs += 1;
        assert!(
            runner.used.get().calls <= 60,
            "run {runs} made {} calls",
            runner.used.get().calls
        );
        assert!(runs < 40);
    }
    assert!(runner.world.extra(id).get("Aruna files").is_none());
    assert!(runs > 2, "{runs} runs");
}

#[test]
fn experiment_stays_small() {
    // 6000 other files with long ids: nothing about them goes into the experiment, so every
    // eLabFTW answer stays far below the per-call bound and title and body keep syncing.
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "<p>a</p>", &["aruna"]);
    runner.poll();
    let (dataset, _) = runner.world.only_dataset();
    for n in 0..6000 {
        let file = format!("measurements/run-2026-11-13/sample-{n:05}/raw-data.csv");
        runner
            .world
            .aruna_file(&dataset, json!({ "@id": file, "@type": "File" }));
    }
    runner.world.lock().file_page = 200;
    runner
        .world
        .aruna_edit(&dataset, json!({ "name": "Renamed", "text": "<p>b</p>" }));
    runner.event(&dataset);
    let mut runs = 0;
    while !runner.marks(id).is_null() {
        runner.poll();
        runs += 1;
        assert!(runs < 20, "{}", runner.marks(id));
    }
    let experiment = runner.world.get(id);
    assert_eq!(
        (experiment["title"].clone(), experiment["body"].clone()),
        (json!("Renamed"), json!("<p>b</p>"))
    );
    let metadata = experiment["metadata"].as_str().unwrap().len();
    assert!(metadata < 1024, "the metadata holds {metadata} bytes");

    // The scan reads past the other files and keeps none of them in state.
    let prefix = format!("files/{dataset}/");
    assert!(
        runner
            .world
            .state_keys()
            .iter()
            .all(|key| !key.starts_with(&prefix))
    );
    let head = serde_json::to_vec(&runner.world.state(&format!("files/{dataset}")).unwrap());
    assert!(head.unwrap().len() < 1024);

    // Later edits on both sides still sync.
    runner
        .world
        .edit(id, |experiment| experiment["title"] = json!("From eLabFTW"));
    runner.poll();
    assert_eq!(
        runner.world.dataset(&dataset).entity["name"],
        "From eLabFTW"
    );
}

#[test]
fn wide_dataset_scanned() {
    // 3000 uploads: the dataset's file entities spread over several scan parts.
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    for upload in 1..=3000 {
        runner
            .world
            .upload(id, upload, &format!("measurement-{upload:05}.raw"), upload);
    }
    runner.world.lock().file_page = 200;
    let mut runs = 0;
    while runner.world.lock().datasets.is_empty() || !runner.marks(id).is_null() {
        runner.poll();
        runs += 1;
        assert!(runs < 30, "{}", runner.marks(id));
    }
    // A change in Aruna reads the files again from the stored parts.
    let (dataset, stored) = runner.world.only_dataset();
    assert_eq!(stored.files.len(), 3000);
    runner
        .world
        .aruna_edit(&dataset, json!({ "name": "Renamed" }));
    runner.event(&dataset);
    assert_eq!(runner.world.get(id)["title"], "Renamed");
    let prefix = format!("files/{dataset}/");
    let parts = runner
        .world
        .state_keys()
        .iter()
        .filter(|key| key.starts_with(&prefix))
        .count();
    assert!(parts >= 3, "{parts} parts");
}

#[test]
fn annotated_files_scanned() {
    // 100 synced files with 3 KiB notes of a person, and one with a 100 KiB note, on one page:
    // the page splits into parts below the state bound, and every note stays.
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    for upload in 1..=101 {
        runner
            .world
            .upload(id, upload, &format!("u{upload}.bin"), upload);
    }
    while runner.world.lock().datasets.is_empty() || !runner.marks(id).is_null() {
        runner.poll();
    }
    let (dataset, stored) = runner.world.only_dataset();
    for (n, (_, file)) in stored.files.iter().enumerate() {
        let mut file = file.clone();
        let size = if n == 0 { 100 << 10 } else { 3 << 10 };
        file["note"] = json!("n".repeat(size));
        runner.world.aruna_file(&dataset, file);
    }
    runner.world.lock().file_page = 101;
    runner
        .world
        .aruna_edit(&dataset, json!({ "name": "Renamed" }));
    runner.event(&dataset);
    assert_eq!(runner.marks(id), Value::Null);
    assert_eq!(runner.world.get(id)["title"], "Renamed");
    let files = runner.world.dataset(&dataset).files;
    assert!(files.values().all(|file| {
        file["note"]
            .as_str()
            .is_some_and(|note| note.len() >= 3 << 10)
    }));
    let prefix = format!("files/{dataset}/");
    let parts: Vec<String> = runner
        .world
        .state_keys()
        .into_iter()
        .filter(|key| key.starts_with(&prefix))
        .collect();
    assert!(parts.len() >= 5, "{} parts", parts.len());
    // A part stays within 64 KiB unless it holds one larger entity alone.
    for key in &parts {
        let part = runner.world.state(key).unwrap();
        let stored = serde_json::to_vec(&part).unwrap().len();
        let ours = part["ours"].as_object().map_or(0, |ours| ours.len());
        let entities = ours + part["others"].as_array().map_or(0, Vec::len);
        let within = entities == 1 || stored <= PART_BYTES + 64;
        assert!(within, "{key}: {entities} entities, {stored} bytes");
    }
}

#[test]
fn uploads_bounded() {
    // 80 uploads: the file writes, and later the storage check, go on across runs.
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    for upload in 1..=80 {
        runner
            .world
            .upload(id, upload, &format!("u{upload}.bin"), upload);
    }
    runner
        .budget
        .set(small((80, 15 << 20), (30, 4 << 20), (15, 1 << 20)));
    let mut runs = 0;
    let files = |runner: &Runner| {
        let datasets = runner.world.lock().datasets.clone();
        datasets
            .values()
            .next()
            .map_or(0, |dataset| dataset.files.len())
    };
    while files(&runner) < 80 || !runner.marks(id).is_null() {
        runner.poll();
        runs += 1;
        assert!(
            runner.used.get().calls <= 80,
            "run {runs} made {} calls",
            runner.used.get().calls
        );
        assert!(runs < 30);
    }
    assert!(runs >= 2, "{runs} runs");

    let path = format!("{BUCKET}/elabftw/ab/77-uuid.bin");
    runner.world.lock().objects.insert(path, 1);
    runner.manual(json!({ "reconcile": true }));
    let mut runs = 0;
    while runner.marks(id)["changed_in_storage"].is_null() {
        runner.poll();
        runs += 1;
        assert!(
            runner.used.get().calls <= 80,
            "run {runs} made {} calls",
            runner.used.get().calls
        );
        assert!(runs < 30);
    }
    assert_eq!(runner.marks(id)["changed_in_storage"], json!(["upload 77"]));
}

#[test]
fn host_ceiling_kept() {
    // A scan of large file pages that ends anywhere near the budget's edge, then body, title and
    // link writes whose eLabFTW answers hold the whole experiment: no run passes the budget,
    // let alone the host's byte ceiling.
    let (budget, ceiling) = (Budget::default(), Limits::default().bytes);
    let body = |fill: &str| format!("<p>{}</p>", fill.repeat(400 << 10));
    let note = "n".repeat(150 << 10);
    let mut most = 0;
    for files in (40..=100).step_by(4) {
        let runner = Runner::new();
        let id = runner.world.experiment("Run 7", &body("b"), &["aruna"]);
        runner.poll();
        let (dataset, _) = runner.world.only_dataset();
        for file in 0..files {
            let entity =
                json!({ "@id": format!("raw/{file:03}.csv"), "@type": "File", "note": note });
            runner.world.aruna_file(&dataset, entity);
        }
        runner
            .world
            .aruna_edit(&dataset, json!({ "name": "Renamed", "text": body("a") }));
        runner.event(&dataset);
        let used = runner.used.get().bytes;
        assert!(
            used <= budget.total.bytes && used < ceiling,
            "{files} files: {used} bytes"
        );
        most = most.max(used);
        let mut runs = 0;
        while !runner.marks(id).is_null() {
            runs += 1;
            assert!(runs < 10, "{files} files: no progress");
            runner.poll();
            let used = runner.used.get().bytes;
            assert!(
                used <= budget.total.bytes,
                "{files} files, a later run: {used} bytes"
            );
        }
        assert_eq!(runner.world.get(id)["title"], "Renamed");
    }
    assert!(most > 10 << 20, "the runs went to the edge: {most} bytes");
}

/// A linked experiment with 20 shared tags and a 400 KiB body, so each tag change answers with
/// the whole experiment and 20 of them do not fit one run.
fn shared_tags() -> (Runner, u64, String) {
    let runner = Runner::new();
    let mut tags: Vec<String> = (0..20).map(|n| format!("t{n:02}")).collect();
    tags.push("aruna".into());
    let tags: Vec<&str> = tags.iter().map(String::as_str).collect();
    let body = format!("<p>{}</p>", "b".repeat(400 << 10));
    let id = runner.world.experiment("Run 7", &body, &tags);
    runner.poll();
    let (dataset, _) = runner.world.only_dataset();
    (runner, id, dataset)
}

fn elab_tags(runner: &Runner, id: u64) -> BTreeSet<String> {
    let tags = runner.world.get(id)["tags"].as_str().unwrap().to_string();
    tags.split('|')
        .filter(|tag| !tag.is_empty())
        .map(String::from)
        .collect()
}

fn keywords(runner: &Runner, dataset: &str) -> BTreeSet<String> {
    let keywords = runner.world.dataset(dataset).entity["keywords"].clone();
    let keywords = keywords.as_array().cloned().unwrap_or_default();
    keywords
        .iter()
        .filter_map(Value::as_str)
        .map(String::from)
        .collect()
}

/// Runs until the link has no marks, each run within the budget.
fn settle_runs(runner: &Runner, id: u64) {
    let mut runs = 0;
    while !runner.marks(id).is_null() {
        runner.poll();
        runs += 1;
        assert!(runs < 20, "{}", runner.marks(id));
        assert!(runner.used.get().bytes <= Budget::default().total.bytes);
    }
}

fn set(tags: &[&str]) -> BTreeSet<String> {
    tags.iter().map(|tag| tag.to_string()).collect()
}

#[test]
fn tag_retry_keeps_restore() {
    // A clear-to-empty batch stops; meanwhile Aruna restores t19 and eLabFTW changes the title.
    // The resume removes the rest, and an unrelated Aruna edit refuses its patch: the retry
    // keeps the restored t19 on both sides.
    let (runner, id, dataset) = shared_tags();
    runner.world.aruna_edit(&dataset, json!({ "keywords": [] }));
    runner.event(&dataset);
    assert!(
        runner.marks(id)["unfinished"].is_array(),
        "{}",
        runner.marks(id)
    );
    assert!(elab_tags(&runner, id).contains("t19"));
    runner
        .world
        .aruna_edit(&dataset, json!({ "keywords": ["t19"] }));
    runner
        .world
        .edit(id, |experiment| experiment["title"] = json!("Run 8"));
    runner.world.lock().race_patch = Some(json!({ "creativeWorkStatus": "edited" }));
    settle_runs(&runner, id);
    assert_eq!(elab_tags(&runner, id), set(&["aruna", "t19"]));
    assert_eq!(keywords(&runner, &dataset), set(&["t19"]));
    assert_eq!(runner.world.dataset(&dataset).entity["name"], "Run 8");
}

#[test]
fn tag_target_waits() {
    // A clear-to-empty batch stops, then the sync user loses write access for a while: the
    // target waits, and the batch goes on once access is back.
    let (runner, id, dataset) = shared_tags();
    runner.world.aruna_edit(&dataset, json!({ "keywords": [] }));
    runner.event(&dataset);
    assert!(
        runner.marks(id)["unfinished"].is_array(),
        "{}",
        runner.marks(id)
    );
    let readonly = json!({ "teams": [], "teamgroups": [], "users": [] }).to_string();
    runner
        .world
        .edit(id, |experiment| experiment["canwrite"] = json!(readonly));
    runner.poll();
    runner.poll();
    assert!(
        runner.marks(id)["no_write"].is_array(),
        "{}",
        runner.marks(id)
    );
    let shared = json!({ "teams": [], "teamgroups": [], "users": [7] }).to_string();
    runner
        .world
        .edit(id, |experiment| experiment["canwrite"] = json!(shared));
    settle_runs(&runner, id);
    assert_eq!(elab_tags(&runner, id), set(&["aruna"]));
    assert_eq!(keywords(&runner, &dataset), set(&[]));
}

#[test]
fn large_tags_wait() {
    // Tag sets above 8 KiB keep no base sets: a stopped batch is a conflict, and it stays one
    // through a later eLabFTW tag edit until a person settles it.
    let runner = Runner::new();
    let mut tags: Vec<String> = (0..120)
        .map(|n| format!("tag-{n:03}-{}", "x".repeat(72)))
        .collect();
    tags.push("aruna".into());
    let body = format!("<p>{}</p>", "b".repeat(400 << 10));
    let id = runner.world.experiment(
        "Run 7",
        &body,
        &tags.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    while runner.world.lock().datasets.is_empty() || !runner.marks(id).is_null() {
        runner.poll();
    }
    let (dataset, _) = runner.world.only_dataset();
    runner.world.aruna_edit(&dataset, json!({ "keywords": [] }));
    runner.event(&dataset);
    runner.poll();
    assert!(
        runner.marks(id)["conflict"].is_array(),
        "{}",
        runner.marks(id)
    );

    let mut held: Vec<String> = elab_tags(&runner, id).into_iter().collect();
    held.push("added".into());
    runner
        .world
        .retag(id, &held.iter().map(String::as_str).collect::<Vec<_>>());
    runner.world.edit(id, |_| {});
    runner.poll();
    assert_eq!(
        keywords(&runner, &dataset),
        set(&[]),
        "the partial set is not copied"
    );
    assert!(
        runner.marks(id)["conflict"].is_array(),
        "{}",
        runner.marks(id)
    );

    runner.manual(json!({ "resolve": id, "keep": "aruna" }));
    settle_runs(&runner, id);
    assert_eq!(elab_tags(&runner, id), set(&["aruna"]));
    assert_eq!(keywords(&runner, &dataset), set(&[]));
    assert!(runner.link(id)["snapshot"].get("tag_target").is_none());
}

#[test]
fn tag_batch_resumes() {
    // Aruna clears its keywords; the 20 removals span runs, and a change of mind halfway goes
    // out too.
    let (runner, id, dataset) = shared_tags();
    runner.world.aruna_edit(&dataset, json!({ "keywords": [] }));
    runner.event(&dataset);
    let left = elab_tags(&runner, id).len();
    assert!(
        left > 1 && left < 21,
        "{left} tags left after the first run"
    );
    assert!(
        runner.marks(id)["unfinished"].is_array(),
        "{}",
        runner.marks(id)
    );
    runner
        .world
        .aruna_edit(&dataset, json!({ "keywords": ["t19", "new"] }));
    settle_runs(&runner, id);
    assert_eq!(elab_tags(&runner, id), set(&["aruna", "new", "t19"]));
    assert_eq!(keywords(&runner, &dataset), set(&["new", "t19"]));

    // Both sides change: eLabFTW adds a tag, Aruna clears its keywords. The merged target keeps
    // the eLabFTW addition through a stopped batch, and an eLabFTW addition made in the gap too.
    let (runner, id, dataset) = shared_tags();
    let mut tags: Vec<String> = elab_tags(&runner, id).into_iter().collect();
    tags.push("elab-added".into());
    runner
        .world
        .retag(id, &tags.iter().map(String::as_str).collect::<Vec<_>>());
    runner.world.aruna_edit(&dataset, json!({ "keywords": [] }));
    runner.event(&dataset);
    assert!(
        runner.marks(id)["unfinished"].is_array(),
        "{}",
        runner.marks(id)
    );
    assert!(elab_tags(&runner, id).contains("elab-added"));
    let mut tags: Vec<String> = elab_tags(&runner, id).into_iter().collect();
    tags.push("later".into());
    runner
        .world
        .retag(id, &tags.iter().map(String::as_str).collect::<Vec<_>>());
    settle_runs(&runner, id);
    assert_eq!(
        elab_tags(&runner, id),
        set(&["aruna", "elab-added", "later"])
    );
    assert_eq!(keywords(&runner, &dataset), set(&["elab-added", "later"]));
    assert_eq!(runner.marks(id), Value::Null);
}

#[test]
fn bytes_bounded() {
    // Large bodies: each run stops starting links before its byte budget.
    let runner = Runner::new();
    let body = format!("<p>{}</p>", "b".repeat(200 << 10));
    let ids: Vec<u64> = (0..6)
        .map(|n| runner.world.experiment(&format!("E{n}"), &body, &["aruna"]))
        .collect();
    runner
        .budget
        .set(small((1000, 3 << 20), (100, 2 << 20), (30, 512 << 10)));
    let mut runs = 0;
    while runner.world.lock().datasets.len() < ids.len() {
        let before = runner.world.lock().datasets.len();
        runner.poll();
        runs += 1;
        assert!(
            runner.used.get().bytes <= 3 << 20,
            "run {runs} moved {} bytes",
            runner.used.get().bytes
        );
        assert!(runner.world.lock().datasets.len() > before);
        assert!(runs < 20);
    }
    assert!(runs >= 3, "{runs} runs");
}

#[test]
fn retry_page_resumes() {
    // 25 failed links on one mark page, all due; each retry fails again as too large.
    let runner = Runner::new();
    let ids: Vec<u64> = (0..25)
        .map(|n| runner.world.experiment(&format!("E{n}"), "", &[]))
        .collect();
    for id in &ids {
        runner.world.lock().huge.insert(*id);
        let mut record = Record::load(&runner.world, &format!("link/{id}")).unwrap();
        record.link.experiment = Some(*id);
        record.link.fail("earlier", support::START, 0);
        record.save(&runner.world).unwrap();
    }
    let large = |runner: &Runner| {
        let large = ids
            .iter()
            .filter(|id| runner.marks(**id)["too_large"].is_array());
        large.count()
    };
    runner
        .budget
        .set(small((80, 15 << 20), (15, 1 << 20), (5, 1 << 20)));
    runner.poll();
    let first = large(&runner);
    assert!(first > 0 && first < 25, "one run retries {first} links");
    let mut runs = 1;
    while large(&runner) < 25 {
        let before = large(&runner);
        runner.poll();
        runs += 1;
        assert!(
            large(&runner) > before,
            "run {runs} went on inside the page"
        );
        assert!(runs < 10);
    }
}

#[test]
fn due_links_retried() {
    // The first 20 failed links of the page are not due; the 5 after them are retried at once.
    let runner = Runner::new();
    let ids: Vec<u64> = (0..25)
        .map(|n| runner.world.experiment(&format!("E{n}"), "", &[]))
        .collect();
    for (n, id) in ids.iter().enumerate() {
        runner.world.lock().huge.insert(*id);
        let mut record = Record::load(&runner.world, &format!("link/{id}")).unwrap();
        record.link.experiment = Some(*id);
        let delay = if n < 20 { 365 * DAY_MS } else { 0 };
        record.link.fail("earlier", support::START, delay);
        record.save(&runner.world).unwrap();
    }
    runner.poll();
    let large: Vec<bool> = ids
        .iter()
        .map(|id| runner.marks(*id)["too_large"].is_array())
        .collect();
    assert_eq!(large, [vec![false; 20], vec![true; 5]].concat());
}

#[test]
fn bucket_checked() {
    let world = World::default();
    let mut elsewhere = settings();
    elsewhere.bucket = "/r/g/lab/data/other-node/shared".into();
    let meter = Meter::new(&world);
    let fault = Sync::new(&meter, &elsewhere, "run-1", 1)
        .poll()
        .unwrap_err();
    assert_eq!(fault.kind, Kind::NotFound);
    assert!(fault.message.contains("receiver node"), "{}", fault.message);
    assert!(
        Sync::new(&meter, &settings(), "run-1", 1).poll().is_ok(),
        "an empty bucket is fine"
    );
}

#[test]
fn blocked_until_changed() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    runner.world.edit(id, |experiment| {
        let field =
            json!({ "extra_fields": { "Aruna dataset": { "type": "text", "value": "mine" } } });
        experiment["metadata"] = json!(field.to_string());
    });
    runner.poll();
    let blocked = runner.marks(id)["blocked"].clone();
    assert_eq!(blocked[0], "Aruna dataset", "{blocked}");
    let (dataset, _) = runner.world.only_dataset();
    runner
        .world
        .edit(id, |experiment| experiment["title"] = json!("Run 8"));
    runner.manual(json!({ "experiment": id }));
    assert_eq!(runner.world.dataset(&dataset).entity["name"], "Run 8");
    assert_eq!(runner.marks(id)["blocked"][0], "Aruna dataset");
}

#[test]
fn readonly_one_way() {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", "", &["aruna"]);
    let canwrite = json!({ "teams": [], "teamgroups": [], "users": [] }).to_string();
    runner
        .world
        .edit(id, |experiment| experiment["canwrite"] = json!(canwrite));
    runner.poll();
    assert_eq!(runner.marks(id)["no_write"], json!(["canwrite"]));
    let (dataset, _) = runner.world.only_dataset();
    assert!(
        runner.world.extra(id).is_empty(),
        "no link fields without write access"
    );

    runner
        .world
        .aruna_edit(&dataset, json!({ "name": "From Aruna" }));
    runner.event(&dataset);
    assert_eq!(runner.world.get(id)["title"], "Run 7");
    runner
        .world
        .edit(id, |experiment| experiment["status_title"] = json!("Done"));
    runner.poll();
    assert_eq!(
        runner.world.dataset(&dataset).entity["creativeWorkStatus"],
        "Done"
    );

    // Write access for the whole team comes from the top-level base.
    runner
        .world
        .edit(id, |experiment| experiment["canwrite_base"] = json!(30));
    runner
        .world
        .aruna_edit(&dataset, json!({ "name": "Team writable" }));
    runner.event(&dataset);
    assert_eq!(runner.world.get(id)["title"], "Team writable");
    assert_eq!(runner.marks(id)["no_write"], Value::Null);
}

#[test]
fn report_pages_bounded() {
    let world = World::default();
    let details: Vec<String> = (0..8).map(|n| format!("{n}{}", "d".repeat(200))).collect();
    let kinds = [Mark::Conflict, Mark::OutOfSync, Mark::Gone, Mark::Failed];
    for experiment in 0..150u64 {
        let mut record = Record::load(&world, &format!("link/{experiment}")).unwrap();
        record.link.experiment = Some(experiment);
        for mark in kinds {
            record.link.mark(mark, Some(details.clone()));
        }
        record.save(&world).unwrap();
    }
    let mut cleared = Record::load(&world, "link/3").unwrap();
    cleared.link.marks.clear();
    cleared.save(&world).unwrap();

    let settings = settings();
    let meter = Meter::new(&world);
    let sync = Sync::new(&meter, &settings, "run-1", 1);
    let (mut after, mut seen) = (json!(0), BTreeSet::new());
    while let Some(page) = after.as_u64() {
        let report = sync
            .manual(&json!({ "report": true, "after": page }))
            .unwrap();
        assert!(
            report.to_string().len() < REPORT_BYTES,
            "page {page} is too large"
        );
        let entries = report["marks"].as_array().unwrap();
        assert!(entries.len() <= 100);
        seen.extend(
            entries
                .iter()
                .map(|entry| entry["experiment"].as_u64().unwrap()),
        );
        assert_eq!(report["totals"]["conflict"], 149);
        after = report["next"].clone();
    }
    assert_eq!(seen.len(), 149);
    assert!(!seen.contains(&3));
}

#[test]
fn state_values_bounded() {
    let world = World::default();
    let fault = store(&world, "big", &"x".repeat(MAX_VALUE), None).unwrap_err();
    assert_eq!(fault.kind, Kind::Limit);
    assert!(
        Host::get(&world, "big").unwrap().is_none(),
        "nothing was sent"
    );

    // A record that would reach the bound keeps only the prints of its tag sets.
    let mut snapshot = Snapshot::default();
    snapshot.elab.tag_set = Some((0..30_000).map(|n| format!("tag-{n:05}")).collect());
    let mut record = Record::load(&world, "link/1").unwrap();
    (record.link.experiment, record.link.snapshot) = (Some(1), Some(snapshot));
    record.save(&world).unwrap();
    let stored = world.state("link/1").unwrap();
    assert!(
        stored["snapshot"]["elab"].get("tag_set").is_none(),
        "{}",
        stored.to_string().len()
    );
}
