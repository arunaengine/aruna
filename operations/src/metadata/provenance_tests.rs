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

/// Reads hops the way the route does, from `answers` by asked id; a missing answer is hidden.
fn walk(start: &str, answers: &BTreeMap<String, Value>) -> (Value, Vec<String>, usize) {
    let mut built = ProvenanceCrate::new(start, "2026-10-27T12:00:00Z");
    let mut queue = vec![built.version_start()];
    let (mut files, mut reads) = (Vec::new(), 0);
    while let Some(hop) = queue.pop() {
        if built.known(&hop) {
            continue;
        }
        reads += 1;
        match &hop.target {
            Target::Version(id) => match answers.get(id) {
                Some(origin) => queue.extend(built.version(&hop, origin)),
                None => built.hidden(&hop),
            },
            Target::Run(run_id) => {
                let (next, file) = built.run(&hop, answers.get(&format!("run:{run_id}")));
                queue.extend(next);
                files.extend(file.map(|file| file.path));
            }
        }
    }
    (built.finish(), files, reads)
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

    let mut built = ProvenanceCrate::new(&output, "2026-10-27T12:00:00Z");
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
    let document = built.finish();
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
    let mut built = ProvenanceCrate::new("dataset", "2026-10-27T12:00:00Z");
    let (hops, file) = built.dataset(&document);
    assert_eq!(hops.len(), 1);
    assert_eq!(hops[0].target, Target::Version(exact));
    assert_eq!(file.path, "dataset/ro-crate-metadata.json");
    let root = built.finish();
    assert_eq!(
        node(&root, "#b")["description"],
        "No exact version recorded"
    );
}
