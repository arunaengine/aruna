//! Marks, the report and bounds against the fake eLabFTW and Aruna: drift, storage sizes, too
//! large messages, blocked merges, write access, file merges and the paged report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod support;

use std::collections::BTreeSet;

use aruna_plugins::limits::Limits;
use elabftw_sync::compare::Snapshot;
use elabftw_sync::host::{Host, Kind, Meter};
use elabftw_sync::progress::{DAY_MS, MAX_VALUE, Mark, REPORT_BYTES, Record, store};
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
    let links = runner.world.extra(id)["Aruna files"]["value"].clone();
    assert_eq!(
        links,
        json!([format!(
            "https://portal.example.org/datasets/{dataset}/files/raw.csv"
        )])
    );
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
    let links = runner.world.extra(id)["Aruna files"]["value"].clone();
    assert_eq!(links.as_array().unwrap().len(), 240);
    assert!(runs > 2, "{runs} runs");
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

#[test]
fn tag_batch_resumes() {
    // 20 tag removals, each answered with the whole experiment and its 400 KiB body: the batch
    // spans runs within the host's bounds, and a change of mind halfway goes out too.
    let runner = Runner::new();
    let tags: Vec<String> = (0..20).map(|n| format!("t{n:02}")).collect();
    let mut all: Vec<&str> = tags.iter().map(String::as_str).collect();
    all.push("aruna");
    let body = format!("<p>{}</p>", "b".repeat(400 << 10));
    let id = runner.world.experiment("Run 7", &body, &all);
    runner.poll();
    let (dataset, _) = runner.world.only_dataset();
    runner.world.aruna_edit(&dataset, json!({ "keywords": [] }));
    runner.event(&dataset);
    let left = runner.world.get(id)["tags"]
        .as_str()
        .unwrap()
        .split('|')
        .count();
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
    let mut runs = 0;
    while !runner.marks(id).is_null() {
        runner.poll();
        runs += 1;
        assert!(runs < 20, "{}", runner.marks(id));
        assert!(runner.used.get().bytes <= Budget::default().total.bytes);
    }
    let mut held: Vec<String> = runner.world.get(id)["tags"]
        .as_str()
        .unwrap()
        .split('|')
        .map(String::from)
        .collect();
    held.sort();
    assert_eq!(held, ["aruna", "new", "t19"]);
    let keywords = runner.world.dataset(&dataset).entity["keywords"].clone();
    let keywords: BTreeSet<&str> = keywords
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert_eq!(keywords, BTreeSet::from(["new", "t19"]));
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
