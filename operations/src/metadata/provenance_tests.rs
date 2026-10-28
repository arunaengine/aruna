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

/// The root metadata as the export writes it.
fn root(built: ProvenanceCrate) -> Value {
    let mut out = Vec::new();
    built.write_root(&mut out, &|| false).unwrap();
    serde_json::from_slice(&out).unwrap()
}

/// Reads hops the way the route does, from `answers` by asked id; a missing answer is hidden.
fn walk(start: &str, answers: &BTreeMap<String, Value>) -> (Value, Vec<String>, usize) {
    let mut built = ProvenanceCrate::new(start, "2026-10-27T12:00:00Z").unwrap();
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

    let mut built = ProvenanceCrate::new(&output, "2026-10-27T12:00:00Z").unwrap();
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
    let mut built = ProvenanceCrate::new("dataset", "2026-10-27T12:00:00Z").unwrap();
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
fn spooled_state_streams() {
    // The working state lives on disk: links come back in the order queued, every entity is
    // written into the root, and the working folder is gone once the crate is written.
    let mut built = ProvenanceCrate::new("start", "2026-10-27T12:00:00Z").unwrap();
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
    let folder = built.spool.path.clone();
    assert!(folder.exists());
    let document = root(built);
    assert!(!folder.exists());
    let graph = document["@graph"].as_array().unwrap();
    let hidden = graph
        .iter()
        .filter(|node| node["name"] == "Not visible to you")
        .count();
    assert_eq!(hidden, 500);
    assert_eq!(node(&document, "./")["hasPart"], json!([]));
}

#[test]
fn scratch_stays_private() {
    // Export working state lives in a folder only this user may open, in files only it may read.
    use std::os::unix::fs::PermissionsExt;
    let dir = scratch_dir().unwrap();
    let mode =
        |path: &std::path::Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&dir), 0o700);
    let path = dir.join(format!("{}.json", scratch_name()));
    drop(scratch_file(&path).unwrap());
    assert_eq!(mode(&path), 0o600);
    std::fs::remove_file(&path).unwrap();
    let built = ProvenanceCrate::new("start", "2026-10-28T00:00:00Z").unwrap();
    assert!(built.spool.path.starts_with(&dir));
}

#[test]
fn crashed_scratch_reclaimed() {
    // Entries of exports whose process is gone are removed before the next export; entries of
    // running processes and anything else stay.
    let dir = scratch_dir().unwrap();
    let mut child = std::process::Command::new("true").spawn().unwrap();
    let gone = child.id();
    child.wait().unwrap();
    let crashed = dir.join(format!("{gone}-{}", Ulid::generate()));
    std::fs::create_dir_all(crashed.join("journal")).unwrap();
    let crashed_root = dir.join(format!("{gone}-{}.json", Ulid::generate()));
    std::fs::write(&crashed_root, b"{}").unwrap();
    let live = dir.join(scratch_name());
    std::fs::create_dir(&live).unwrap();
    let other = dir.join(format!("{gone}-not-an-id"));
    std::fs::write(&other, b"").unwrap();

    scratch_dir().unwrap();
    assert!(!crashed.exists() && !crashed_root.exists());
    assert!(live.exists() && other.exists());
    std::fs::remove_dir(&live).unwrap();
    std::fs::remove_file(&other).unwrap();
}

#[test]
fn unwritten_run_status() {
    // A run whose crate was denied says so instead of reading as not visible.
    let output = arn(1, "out.txt");
    let run_id = Ulid::from_bytes([5; 16]).to_string();
    let mut built = ProvenanceCrate::new(&output, "2026-10-27T12:00:00Z").unwrap();
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
    let answers = BTreeMap::from([(version.clone(), origin)]);
    let (document, _, _) = walk(&version, &answers);
    assert_eq!(
        node(&document, &version)["isPartOf"][0]["@id"],
        release.as_str()
    );
    let entity = node(&document, &release);
    assert_eq!(entity["version"], "6.0");
    assert_eq!(
        entity["isBasedOn"][0]["@id"],
        "https://doi.org/10.5281/zenodo.1"
    );
    let marker = node(&document, &format!("{version}#release-membership"));
    assert_eq!(marker["value"], "incomplete");
}
