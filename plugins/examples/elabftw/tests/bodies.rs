//! Body sync both ways against the fake eLabFTW, which filters HTML bodies like eLabFTW does:
//! write-back of the stored form, round trips, undo, description edits and body conflicts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod support;

use elabftw_sync::progress::STALE_MS;
use serde_json::{Value, json};
use support::Runner;

/// A linked experiment with this HTML body, and its dataset.
fn linked(body: &str) -> (Runner, u64, String) {
    let runner = Runner::new();
    let id = runner.world.experiment("Run 7", body, &["aruna"]);
    runner.poll();
    let (dataset, _) = runner.world.only_dataset();
    (runner, id, dataset)
}

fn text(runner: &Runner, dataset: &str) -> Value {
    runner.world.dataset(dataset).entity["text"].clone()
}

#[test]
fn filtered_body_returns() {
    let (runner, id, dataset) = linked("<p>a</p>");
    let edit = json!({ "text": "<p>b<br>c</p><script>x()</script>", "description": "ignored" });
    runner.world.aruna_edit(&dataset, edit);
    runner.event(&dataset);
    // eLabFTW's filtered body becomes the text, and text and description agree again.
    assert_eq!(runner.world.get(id)["body"], "<p>b<br />c</p>");
    let entity = runner.world.dataset(&dataset).entity;
    assert_eq!(entity["text"], "<p>b<br />c</p>");
    assert_eq!(entity["description"], "b\nc");
    assert_eq!(runner.marks(id), Value::Null);
}

#[test]
fn round_trips_stable() {
    let (runner, id, dataset) = linked("<p>start</p>");
    runner
        .world
        .aruna_edit(&dataset, json!({ "text": "<p>a<br>b</p>" }));
    runner.event(&dataset);
    let settled = (
        runner.world.get(id)["body"].clone(),
        text(&runner, &dataset),
    );
    let revision = runner.world.dataset(&dataset).revision;
    for _ in 0..10 {
        runner.poll();
        runner.event(&dataset);
    }
    assert_eq!(
        (
            runner.world.get(id)["body"].clone(),
            text(&runner, &dataset)
        ),
        settled
    );
    assert_eq!(runner.world.dataset(&dataset).revision, revision);
    assert_eq!(runner.marks(id), Value::Null);
}

#[test]
fn undo_reaches_elab() {
    let (runner, id, dataset) = linked("<p>A</p>");
    // Aruna edits A to B; while B goes to eLabFTW and its filtered form comes back, the edit
    // is undone back to A.
    runner.world.aruna_edit(
        &dataset,
        json!({ "text": "<p>B<br>b</p>", "description": "B" }),
    );
    runner.world.lock().race_patch = Some(json!({ "text": "<p>A</p>", "description": "A" }));
    runner.event(&dataset);
    assert_eq!(
        runner.world.get(id)["body"],
        "<p>A</p>",
        "the undo is a new change"
    );
    assert_eq!(text(&runner, &dataset), "<p>A</p>");
    assert_eq!(runner.marks(id), Value::Null);

    // A later eLabFTW edit is a normal one-sided change.
    runner
        .world
        .edit(id, |experiment| experiment["body"] = json!("<p>C</p>"));
    runner.poll();
    assert_eq!(text(&runner, &dataset), "<p>C</p>");
    assert_eq!(runner.marks(id), Value::Null);
}

#[test]
fn refused_writeback_compared() {
    let (runner, id, dataset) = linked("<p>a</p>");
    runner
        .world
        .aruna_edit(&dataset, json!({ "text": "<p>b<br>c</p>" }));
    // A person renames the dataset between the read and the write-back.
    runner.world.lock().race_patch = Some(json!({ "name": "Renamed" }));
    runner.event(&dataset);
    assert_eq!(runner.world.get(id)["body"], "<p>b<br />c</p>");
    assert_eq!(text(&runner, &dataset), "<p>b<br />c</p>");
    assert_eq!(runner.world.get(id)["title"], "Renamed");
    assert_eq!(runner.marks(id), Value::Null);
}

#[test]
fn refused_round_keeps() {
    let (runner, id, dataset) = linked("<p>a</p>");
    // eLabFTW's title change and Aruna's body change meet a refused write-back.
    runner
        .world
        .edit(id, |experiment| experiment["title"] = json!("From eLabFTW"));
    runner
        .world
        .aruna_edit(&dataset, json!({ "text": "<p>b<br>c</p>" }));
    runner.world.lock().race_patch = Some(json!({ "keywords": ["k"] }));
    runner.event(&dataset);
    let entity = runner.world.dataset(&dataset).entity;
    assert_eq!(entity["name"], "From eLabFTW");
    assert_eq!(entity["text"], "<p>b<br />c</p>");
    assert_eq!(runner.world.get(id)["tags"], "aruna|k");
    assert_eq!(runner.marks(id), Value::Null);
}

#[test]
fn refused_create_writeback() {
    // A rename lands while the new experiment's filtered body goes back to the dataset.
    let runner = Runner::new();
    let entity =
        json!({ "@id": "./", "@type": "Dataset", "name": "Made", "text": "<p>a<br>b</p>" });
    let dataset = runner.world.aruna_dataset("elabftw/made", entity);
    runner.world.lock().race_patch = Some(json!({ "name": "Renamed" }));
    runner.event(&dataset);
    assert_eq!(
        text(&runner, &dataset),
        "<p>a<br>b</p>",
        "the write-back was refused"
    );

    runner.event(&dataset);
    let id = *runner.world.lock().experiments.keys().next().unwrap();
    assert_eq!(runner.world.get(id)["title"], "Renamed");
    assert_eq!(text(&runner, &dataset), "<p>a<br />b</p>");
    assert_eq!(runner.world.dataset(&dataset).entity["description"], "a\nb");
    assert_eq!(runner.marks(id), Value::Null);
}

#[test]
fn stopped_change_keeps_conflict() {
    let (runner, id, dataset) = linked("<p>a</p>");
    runner.world.edit(id, |experiment| {
        experiment["body"] = json!("<p>from eLabFTW</p>")
    });
    runner.world.aruna_edit(
        &dataset,
        json!({ "text": "<p>from Aruna</p>", "name": "Renamed" }),
    );
    // The title goes to eLabFTW, then the node stops before the link is saved.
    runner.world.lock().stop_after_patch = true;
    let found = runner.world.dataset(&dataset);
    let path = format!("/r/g/lab/meta/{}@{dataset}", found.path);
    let stopped = runner.at(|sync| sync.changed(&dataset, &path, &found.revision));
    assert!(stopped.is_err());
    assert_eq!(runner.world.get(id)["title"], "Renamed");
    runner.world.lock().state_down = false;

    // The recovered run takes over only the sent title; the bodies still conflict.
    runner.now.set(runner.now.get() + STALE_MS + 1);
    runner.poll();
    assert_eq!(text(&runner, &dataset), "<p>from Aruna</p>");
    assert_eq!(runner.world.get(id)["body"], "<p>from eLabFTW</p>");
    let conflict = runner.marks(id)["conflict"].clone();
    assert!(
        conflict[0].as_str().unwrap().starts_with("body "),
        "{conflict}"
    );
}

#[test]
fn description_builds_body() {
    let (runner, id, dataset) = linked("<p><b>rich</b></p>");
    runner
        .world
        .aruna_edit(&dataset, json!({ "description": "x < y\n\nnext" }));
    runner.event(&dataset);
    assert_eq!(runner.world.get(id)["body"], "<p>x &lt; y</p><p>next</p>");

    // A Markdown body is sent as the description says.
    runner.world.edit(id, |experiment| {
        (experiment["body"], experiment["content_type"]) = (json!("*md*"), json!(2));
    });
    runner.poll();
    assert_eq!(
        runner.world.dataset(&dataset).entity["encodingFormat"],
        "text/markdown"
    );
    runner
        .world
        .aruna_edit(&dataset, json!({ "description": "# plain\n\n`code`" }));
    runner.event(&dataset);
    assert_eq!(runner.world.get(id)["body"], "# plain\n\n`code`");
}

#[test]
fn text_wins_description() {
    let (runner, id, dataset) = linked("<p>a</p>");
    runner.world.aruna_edit(
        &dataset,
        json!({ "text": "<p>text</p>", "description": "other" }),
    );
    runner.event(&dataset);
    assert_eq!(runner.world.get(id)["body"], "<p>text</p>");
    assert_eq!(runner.world.dataset(&dataset).entity["description"], "text");
}

#[test]
fn href_conflict_marked() {
    let (runner, id, dataset) = linked(r#"<p><a href="x">l</a></p>"#);
    runner.world.edit(id, |experiment| {
        experiment["body"] = json!(r#"<p><a href="y">l</a></p>"#)
    });
    runner
        .world
        .aruna_edit(&dataset, json!({ "text": r#"<p><a href="z">l</a></p>"# }));
    runner.poll();
    let conflict = runner.marks(id)["conflict"][0]
        .as_str()
        .unwrap()
        .to_string();
    assert!(conflict.starts_with("body elabftw "), "{conflict}");
    assert_eq!(text(&runner, &dataset), r#"<p><a href="z">l</a></p>"#);
    assert_eq!(runner.world.get(id)["body"], r#"<p><a href="y">l</a></p>"#);
}

#[test]
fn cosmetic_change_stays() {
    let (runner, id, dataset) = linked(r#"<p class="a" id="b">x</p>"#);
    let revision = runner.world.dataset(&dataset).revision;
    // Attribute order and quotes are markup only: nothing is written anywhere.
    runner.world.edit(id, |experiment| {
        experiment["body"] = json!("<p id='b' class='a'>x</p>")
    });
    runner.poll();
    assert_eq!(runner.world.dataset(&dataset).revision, revision);
    // A changed whitespace in the text is content.
    runner.world.edit(id, |experiment| {
        experiment["body"] = json!("<p id='b' class='a'>x </p>")
    });
    runner.poll();
    assert_eq!(text(&runner, &dataset), "<p id='b' class='a'>x </p>");
}

#[test]
fn format_change_content() {
    let (runner, id, dataset) = linked("plain");
    runner
        .world
        .aruna_edit(&dataset, json!({ "encodingFormat": "text/markdown" }));
    runner.event(&dataset);
    assert_eq!(runner.world.get(id)["content_type"], 2);
    assert_eq!(runner.world.get(id)["body"], "plain");
}
