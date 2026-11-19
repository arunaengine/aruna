//! Tests the provenance crate: links, nested run crates, cycles, hidden and missing hops.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::structs::storage::data::identity::content_id;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use ulid::Ulid;

use super::*;

fn arn(seed: u8, key: &str) -> String {
    let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
    let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
    VersionedObjectArn::new(realm_id, node, "bucket", key, Ulid::from_bytes([seed; 16]))
        .unwrap()
        .to_w3id()
}

fn copied(from: &str) -> Value {
    json!({ "created_at": "2026-10-27T10:00:00Z", "origin": { "kind": "copy", "from": from } })
}

/// A crate about `start` whose rows are all held in memory.
fn built(start: &str, published: &str) -> ProvenanceCrate {
    let state = ExportState::new(Ulid::nil(), start, published);
    ProvenanceCrate::new(state, Workspace::complete(Ulid::nil()))
}

/// The root metadata as the export pages it: the head, the parts and the entities in order.
fn root(mut built: ProvenanceCrate) -> Value {
    let mut out = built.root_head();
    let (_, rows) = built.into_parts();
    for (index, (key, _)) in rows.rows_of(workspace::PART).into_iter().enumerate() {
        out.push_str(&part_piece(index == 0, workspace::row_name(&key)));
    }
    out.push_str(PARTS_END);
    for (_, value) in rows.rows_of(workspace::ENTITY) {
        out.push_str(&entity_piece(&value).unwrap_or_default());
    }
    out.push_str(GRAPH_END);
    serde_json::from_slice(out.as_bytes()).unwrap()
}

/// Reads hops the way the route does, from `answers` by asked id; a missing answer is hidden.
fn walk(start: &str, answers: &BTreeMap<String, Value>) -> (Value, Vec<String>, usize) {
    let mut built = built(start, "2026-10-27T12:00:00Z");
    let first = built.version_start();
    built.push([first]);
    let (mut files, mut reads) = (Vec::new(), 0);
    while let Some(hop) = built.pop() {
        if built.known(&hop) {
            continue;
        }
        reads += 1;
        match &hop.target {
            Target::Version(id) => match answers.get(id) {
                Some(origin) => {
                    let next = built.version(&hop, origin);
                    built.push(next);
                }
                None => built.hidden(&hop),
            },
            Target::Run(run_id) => {
                let (next, file) = built.run(&hop, answers.get(&format!("run:{run_id}")));
                built.push(next);
                files.extend(file.map(|file| file.path));
            }
            Target::Release(id) => {
                let file = built.release(&hop, answers.get(&format!("release:{id}")));
                files.extend(file.map(|file| file.path));
            }
        }
    }
    (root(built), files, reads)
}

fn node<'a>(document: &'a Value, id: &str) -> &'a Value {
    let graph = document["@graph"].as_array().unwrap();
    graph.iter().find(|node| node["@id"] == id).unwrap()
}

#[test]
fn cycles_read_once() {
    let (a, b) = (arn(1, "a.txt"), arn(2, "b.txt"));
    let answers = BTreeMap::from([(a.clone(), copied(&b)), (b.clone(), copied(&a))]);
    let (document, _, reads) = walk(&a, &answers);
    assert_eq!(reads, 2, "each version is read once");
    assert_eq!(node(&document, &a)["isBasedOn"][0]["@id"], b.as_str());
    assert_eq!(node(&document, &b)["isBasedOn"][0]["@id"], a.as_str());
    assert_eq!(node(&document, "./")["creativeWorkStatus"], "Complete");
    assert_eq!(node(&document, &a)["name"], "a.txt");
}

#[test]
fn runs_nest_crates() {
    // Two inputs with equal bytes but different versions stay two hops; an input without an
    // exact version is listed as such; the run crate becomes a nested crate.
    let output = arn(1, "out.txt");
    let (first, second) = (arn(3, "in-1.txt"), arn(4, "in-2.txt"));
    let digest = content_id([9; 32]);
    let run_id = Ulid::from_bytes([5; 16]).to_string();
    let run_crate = json!({ "@graph": [
        { "@id": "#run", "@type": "CreateAction",
          "object": [{ "@id": digest }, { "@id": "s3://bucket/in-2.txt" }, { "@id": "s3://bucket/old.txt" }] },
        { "@id": digest, "@type": "File", "contentUrl": ["s3://bucket/in-1.txt", first] },
        { "@id": "s3://bucket/in-2.txt", "@type": "File", "contentUrl": ["s3://bucket/in-2.txt", second] },
        { "@id": "s3://bucket/old.txt", "@type": "File", "contentUrl": "s3://bucket/old.txt" }
    ]});
    let answers = BTreeMap::from([
        (
            output.clone(),
            json!({ "origin": { "kind": "job", "run_id": run_id } }),
        ),
        (format!("run:{run_id}"), run_crate),
        (first.clone(), json!({ "origin": { "kind": "upload" } })),
        (second.clone(), json!({ "origin": { "kind": "upload" } })),
    ]);
    let (document, files, _) = walk(&output, &answers);
    assert_eq!(files, vec![format!("runs/{run_id}/ro-crate-metadata.json")]);
    let run = node(&document, &format!("#run-{run_id}"));
    assert_eq!(run["result"][0]["@id"], output.as_str());
    let objects: Vec<_> = run["object"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["@id"].clone())
        .collect();
    assert!(objects.contains(&json!(first)) && objects.contains(&json!(second)));
    assert!(objects.contains(&json!("s3://bucket/old.txt")));
    assert_eq!(
        node(&document, "s3://bucket/old.txt")["description"],
        "No exact version recorded"
    );
    let nested = node(&document, &format!("runs/{run_id}/"));
    assert_eq!(nested["conformsTo"]["@id"], NESTED_CRATE);
}

#[test]
fn hidden_hops_private() {
    // A hop the caller cannot read shows neither its bucket nor its key.
    let output = arn(1, "out.txt");
    let secret = arn(6, "private/secret.txt");
    let run_id = Ulid::from_bytes([5; 16]).to_string();
    let digest = content_id([7; 32]);
    let run_crate = json!({ "@graph": [
        { "@id": "#run", "@type": "CreateAction", "object": [{ "@id": digest }] },
        { "@id": digest, "@type": "File", "contentUrl": ["s3://bucket/private/secret.txt", secret] }
    ]});
    let answers = BTreeMap::from([
        (
            output.clone(),
            json!({ "origin": { "kind": "copy", "from": arn(8, "x/y.txt") } }),
        ),
        (format!("run:{run_id}"), run_crate.clone()),
    ]);
    let (document, _, _) = walk(&output, &answers);
    let text = document.to_string();
    assert!(text.contains("#hidden-1"));
    assert!(!text.contains("x/y.txt") && !text.contains(&arn(8, "x/y.txt")));

    let mut built = built(&output, "2026-10-27T12:00:00Z");
    let run = Hop {
        target: Target::Run(run_id),
        from: output.clone(),
        property: "result",
        digest: None,
    };
    let (next, _) = built.run(&run, Some(&run_crate));
    built.hidden(&next[0]);
    let unreachable = Hop {
        target: Target::Version(arn(9, "far.txt")),
        ..next[0].clone()
    };
    built.unreachable(&unreachable);
    let document = root(built);
    assert_eq!(node(&document, &digest)["name"], "Not visible to you");
    assert!(!document["@graph"].to_string().contains(&secret));
    let status = node(&document, "./")["creativeWorkStatus"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(status.starts_with("Incomplete"));
}

#[test]
fn stored_dataset_parts() {
    // A stored dataset names its files `MediaObject`, with `contentUrl` values in any order.
    let exact = arn(3, "in.txt");
    let document = json!({ "@graph": [
        { "@id": "./", "@type": "Dataset", "hasPart": [{ "@id": "#a" }, { "@id": "#b" }] },
        { "@id": "#a", "@type": "MediaObject", "contentUrl": [exact, "s3://bucket/in.txt"] },
        { "@id": "#b", "@type": "MediaObject", "contentUrl": "s3://bucket/old.txt" }
    ]});
    let mut built = built("dataset", "2026-10-27T12:00:00Z");
    let (hops, file) = built.dataset(&document);
    assert_eq!(hops.len(), 1);
    assert_eq!(hops[0].target, Target::Version(exact));
    assert_eq!(file.path, "dataset/ro-crate-metadata.json");
    let root = root(built);
    assert_eq!(
        node(&root, "#b")["description"],
        "No exact version recorded"
    );
}

#[test]
fn run_results_linked() {
    // Two outputs of one run, the second reached from the first, are both results of the run.
    let (first, second) = (arn(1, "a.txt"), arn(2, "b.txt"));
    let run_id = Ulid::from_bytes([5; 16]).to_string();
    let job = json!({ "origin": { "kind": "job", "run_id": run_id } });
    let mut first_origin = job.clone();
    first_origin["hop"] = json!({ "from": second });
    let answers = BTreeMap::from([
        (first.clone(), first_origin),
        (second.clone(), job),
        (format!("run:{run_id}"), json!({ "@graph": [] })),
    ]);
    let (document, _, reads) = walk(&first, &answers);
    let run = node(&document, &format!("#run-{run_id}"));
    let mut results: Vec<_> = run["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["@id"].clone())
        .collect();
    results.sort_by_key(|id| id.to_string());
    assert_eq!(results, vec![json!(first), json!(second)]);
    assert_eq!(reads, 3, "the run is read once");
}

#[test]
fn queued_state_streams() {
    // Links come back in the order queued and every entity is written into the root.
    let mut built = built("start", "2026-10-27T12:00:00Z");
    let hops: Vec<_> = (0..500)
        .map(|index| Hop {
            target: Target::Version(arn(1, &format!("f{index}"))),
            from: "./".to_string(),
            property: "hasPart",
            digest: None,
        })
        .collect();
    built.push(hops.clone());
    let mut popped = Vec::new();
    while let Some(hop) = built.pop() {
        built.hidden(&hop);
        popped.push(hop);
    }
    assert_eq!(popped, hops);
    let document = root(built);
    let graph = document["@graph"].as_array().unwrap();
    let hidden = graph
        .iter()
        .filter(|node| node["name"] == "Not visible to you")
        .count();
    assert_eq!(hidden, 500);
    assert_eq!(node(&document, "./")["hasPart"], json!([]));
}

#[test]
fn unwritten_run_status() {
    // A run whose crate was denied says so instead of reading as not visible.
    let output = arn(1, "out.txt");
    let run_id = Ulid::from_bytes([5; 16]).to_string();
    let mut built = built(&output, "2026-10-27T12:00:00Z");
    let run = Hop {
        target: Target::Run(run_id.clone()),
        from: output.clone(),
        property: "result",
        digest: None,
    };
    assert!(built.unwritten(&run, "denied", Some("metadata write access denied")));
    assert!(!built.unwritten(&run, "written", None));
    let document = root(built);
    let entity = node(&document, &format!("#run-{run_id}"));
    assert_eq!(entity["creativeWorkStatus"], "RunRecordDenied");
    assert_eq!(entity["description"], "metadata write access denied");

    // A crate that is still being written says so, not that the record is hidden.
    let mut built = built(&output, "2026-10-27T12:00:00Z");
    assert!(built.unwritten(&run, "pending", None));
    let document = root(built);
    let entity = node(&document, &format!("#run-{run_id}"));
    assert_eq!(entity["creativeWorkStatus"], "RunRecordPending");
    assert!(
        entity["description"]
            .as_str()
            .unwrap()
            .starts_with("The run record is still being written")
    );
}

#[test]
fn releases_join_export() {
    // A version names the release that pins it, and says when membership was not checked
    // everywhere.
    let version = arn(1, "db/a.fasta");
    let release = arn(2, "db/release.json");
    let origin = json!({
        "created_at": "2026-10-27T10:00:00Z",
        "releases": [{
            "crate_version": release,
            "label": "6.0",
            "upstream": [{"name": "DOI", "value": "10.5281/zenodo.1", "url": "https://doi.org/10.5281/zenodo.1"}]
        }],
        "releases_complete": false
    });
    let release_crate = json!({"@graph": [{"@id": "./", "@type": "Dataset"}]});
    let answers = BTreeMap::from([
        (version.clone(), origin),
        (format!("release:{release}"), release_crate),
    ]);
    let (document, files, _) = walk(&version, &answers);
    assert_eq!(
        node(&document, &version)["isPartOf"][0]["@id"],
        release.as_str()
    );
    // The readable release crate is nested and named from the release.
    let [nested] = files.as_slice() else {
        panic!("one nested crate: {files:?}");
    };
    let folder = nested.trim_end_matches("ro-crate-metadata.json");
    assert!(folder.starts_with("releases/"));
    let entity = node(&document, &release);
    assert_eq!(entity["subjectOf"][0]["@id"], folder);
    assert_eq!(entity["version"], "6.0");
    assert_eq!(
        entity["isBasedOn"][0]["@id"],
        "https://doi.org/10.5281/zenodo.1"
    );
    let marker = node(&document, &format!("{version}#release-membership"));
    assert_eq!(marker["value"], "incomplete");
}

#[test]
fn database_from_dataset() {
    // A database bucket's version is part of the dataset that describes the bucket now.
    let file = arn(1, "db/genes.tsv");
    let dataset = Ulid::from_bytes([7; 16]);
    let origin = json!({
        "origin": { "kind": "upload" },
        "bucket_metadata": { "database": true, "dataset": dataset.to_string() },
        "description": {
            "title": "Bakta database",
            "description": "Annotation database",
            "version": "6.0",
            "identifiers": [{ "name": "DOI", "value": "10.5281/zenodo.1",
                              "url": "https://doi.org/10.5281/zenodo.1" }]
        }
    });
    let (document, _, _) = walk(&file, &BTreeMap::from([(file.clone(), origin)]));
    let id = format!("https://w3id.org/aruna/{dataset}");
    assert_eq!(node(&document, &file)["isPartOf"][0]["@id"], id.as_str());
    let database = node(&document, &id);
    assert_eq!(database["name"], "Bakta database (current metadata)");
    assert_eq!(database["description"], "Annotation database");
    assert_eq!(
        database["identifier"][0],
        "https://doi.org/10.5281/zenodo.1"
    );
}

fn spool_row(key: Vec<u8>) -> (String, aruna_core::types::Key) {
    let space = aruna_core::keyspaces::PROVENANCE_SPOOL_KEYSPACE.to_string();
    (space, aruna_core::types::Key::from(key))
}

#[test]
fn step_loads_rows() {
    // A step pages the queue and reads the rows it missed before it answers.
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::operation::Operation;
    let export = Ulid::from_bytes([9; 16]);
    let start = arn(1, "start.txt");
    let state = ExportState::new(export, &start, "2026-10-27T12:00:00Z");
    let mut step = ExportStepOperation::new(state, ExportAnswer::Begin);
    let Ok(Next::Call(IoCall::Page { prefix, after, .. })) = step.first() else {
        panic!("expected the queue head first");
    };
    assert_eq!(
        prefix.to_vec(),
        workspace::prefix(export, Some(workspace::QUEUE))
    );
    assert_eq!(after, None);
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    let label = workspace::row_key(export, workspace::LABEL, start.as_bytes());
    let read = IoCall::Read(vec![spool_row(label)]);
    assert_eq!(step.next(empty), Ok(Next::Call(read)));
    assert_eq!(
        step.next(IoResult::Values(vec![None])),
        Ok(Next::Call(IoCall::Write(Vec::new())))
    );
    let queued = workspace::row_key(export, workspace::QUEUE, &1u64.to_be_bytes());
    let delete = IoCall::Delete(vec![spool_row(queued)]);
    assert_eq!(step.next(IoResult::Done), Ok(Next::Call(delete)));
    let Ok(Next::Done(outcome)) = step.next(IoResult::Done) else {
        panic!("expected the step's outcome");
    };
    let first = Hop {
        target: Target::Version(start),
        from: "./".to_string(),
        property: "mainEntity",
        digest: None,
    };
    assert_eq!(outcome.ask, ExportAsk::Hop(first));
    assert!(outcome.files.is_empty());
}

#[test]
fn revoked_hop_stops() {
    // A credential that ended before a read stops the walk and asks for the root.
    let mut built = built("start", "2026-10-27T12:00:00Z");
    let hop = Hop {
        target: Target::Version("start".to_string()),
        from: "./".to_string(),
        property: "mainEntity",
        digest: None,
    };
    built.state().awaiting = Some(Awaiting::Hop(hop));
    let (files, ask) = export::step(&mut built, ExportAnswer::Revoked);
    assert!(files.is_empty());
    assert_eq!(ask, ExportAsk::Root);
    let document = root(built);
    assert_eq!(
        node(&document, "./")["creativeWorkStatus"],
        json!("Incomplete: the token expired or was revoked")
    );
}

#[test]
fn root_parts_close() {
    // The root pages its parts one at a time and closes the list after the last.
    use aruna_core::calls::{IoResult, Next};
    use aruna_core::operation::Operation;
    let state = ExportState::new(Ulid::nil(), "start", "2026-10-27T12:00:00Z");
    let mut page = RootPageOperation::new(state, RootCursor::Parts { after: None });
    page.first().unwrap();
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    let closed = (
        PARTS_END.to_string(),
        Some(RootCursor::Entities { after: None }),
    );
    assert_eq!(page.next(empty), Ok(Next::Done(closed)));
}

#[test]
fn forget_deletes_pages() {
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::operation::Operation;
    let state = ExportState::new(Ulid::from_bytes([3; 16]), "start", "2026-10-27T12:00:00Z");
    let mut forget = ForgetSpoolOperation::export(&state);
    assert!(matches!(
        forget.first(),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let key = workspace::row_key(state.export(), workspace::PART, b"runs/");
    let rows = IoResult::Page {
        rows: vec![(
            spool_row(key.clone()).1,
            aruna_core::types::Value::from(Vec::new()),
        )],
        next: None,
    };
    let delete = IoCall::Delete(vec![spool_row(key)]);
    assert_eq!(forget.next(rows), Ok(Next::Call(delete)));
    assert!(matches!(
        forget.next(IoResult::Done),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(forget.next(empty), Ok(Next::Done(())));
}
