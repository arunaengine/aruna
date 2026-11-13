//! Marks, the report and bounds against the fake eLabFTW and Aruna: drift, storage sizes, too
//! large messages, blocked merges, write access, file merges and the paged report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod support;

use std::collections::BTreeSet;

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

    // A body whose write to eLabFTW would pass 1 MiB is marked and not sent.
    runner.world.lock().huge.clear();
    runner.poll();
    let dataset = runner.link(small)["dataset"].as_str().unwrap().to_string();
    let text = format!("<p>{}</p>", "x".repeat(1 << 20));
    runner.world.aruna_edit(&dataset, json!({ "text": text }));
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
fn retries_resume_inside_page() {
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
fn not_due_cost_nothing() {
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
