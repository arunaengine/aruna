//! Tests plain RO-Crate reads, push checks, metadata merges and snapshots with real Git.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::git::{GitSnapshot, Refs};
use ulid::Ulid;

async fn git(directory: &Path, args: &[&str]) -> String {
    let mut process = Command::new("git");
    process
        .current_dir(directory)
        .args([
            "-c",
            "user.name=Ada Lovelace",
            "-c",
            "user.email=ada@example.org",
        ])
        .args(["-c", "commit.gpgsign=false"])
        .args(args)
        // The host's global hooks and signing settings must not apply here.
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1");
    let output = exchange(process, Bytes::new(), false)
        .await
        .expect("git runs");
    String::from_utf8(output.to_vec())
        .expect("utf-8")
        .trim()
        .to_string()
}

fn document(parts: Value, extra: Vec<Value>) -> Value {
    let mut graph = vec![
        json!({"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
            "about": {"@id": "./"}, "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}}),
        json!({"@id": "./", "@type": "Dataset", "name": "Plain", "description": "Plain crate",
            "datePublished": "2026-09-28", "hasPart": parts,
            "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}}),
    ];
    graph.extend(extra);
    json!({"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": graph})
}

fn listed() -> Value {
    let file = json!({"@id": "data/a.csv", "@type": "File", "name": "a.csv"});
    document(json!([{"@id": "data/a.csv"}]), vec![file])
}

/// Writes `files`, removes `removed`, and commits everything as one new commit.
async fn commit(path: &Path, files: &[(&str, &str)], removed: &[&str]) -> String {
    for (name, content) in files {
        let file = path.join(name);
        tokio::fs::create_dir_all(file.parent().expect("parent"))
            .await
            .expect("folder");
        tokio::fs::write(file, content).await.expect("write");
    }
    for name in removed {
        tokio::fs::remove_file(path.join(name))
            .await
            .expect("remove");
    }
    git(path, &["add", "-A"]).await;
    git(path, &["commit", "-q", "--allow-empty", "-m", "Change"]).await;
    git(path, &["rev-parse", "HEAD"]).await
}

async fn repository() -> (tempfile::TempDir, String) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path();
    git(path, &["init", "-q", "--initial-branch=main"]).await;
    let text = listed().to_string();
    let files = [(CRATE_FILE, text.as_str()), ("data/a.csv", "a,b\n")];
    let first = commit(path, &files, &[]).await;
    (directory, first)
}

fn entity<'a>(document: &'a Value, id: &str) -> Option<&'a Value> {
    document["@graph"]
        .as_array()?
        .iter()
        .find(|entity| entity["@id"] == id)
}

#[tokio::test]
async fn exports_added_files() {
    let (directory, _) = repository().await;
    let path = directory.path();
    let pointer = "version https://git-lfs.github.com/spec/v1\noid sha256:\
        4d7a214614ab2935c943f9e0ff69d22eadbb8f32b1258daaa5e2ca24d17e2393\nsize 12345\n";
    let files = [
        ("data/b.csv", pointer),
        (".gitattributes", "*.csv filter=lfs\n"),
    ];
    let head = commit(path, &files, &[]).await;
    assert_eq!(layout(path, &head).await.unwrap(), Layout::RoCrate);
    let exported = crate::arc::export(path, "main").await.expect("export");
    assert_eq!(exported["commit"], head);
    let rocrate = &exported["rocrate"];
    let added = entity(rocrate, "data/b.csv").expect("unlisted file is described");
    assert_eq!(added["contentSize"], "12345");
    assert!(entity(rocrate, ".gitattributes").is_none());
    let parts = entity(rocrate, "./").unwrap()["hasPart"].clone();
    assert_eq!(parts, json!([{"@id": "data/a.csv"}, {"@id": "data/b.csv"}]));
}

#[tokio::test]
async fn checks_crate_file() {
    let (directory, first) = repository().await;
    let path = directory.path();
    check(path, &first).await.expect("valid crate");
    let invalid = json!({"@graph": []}).to_string();
    let head = commit(path, &[(CRATE_FILE, invalid.as_str())], &[]).await;
    let error = check(path, &head).await.expect_err("invalid crate");
    assert!(
        error.to_string().contains("is not a valid RO-Crate"),
        "{error}"
    );
    let head = commit(path, &[], &[CRATE_FILE]).await;
    let error = check(path, &head).await.expect_err("missing crate");
    assert!(
        error
            .to_string()
            .contains("ro-crate-metadata.json is required")
    );
}

#[tokio::test]
async fn refuses_dropped_entity() {
    let (directory, first) = repository().await;
    let path = directory.path();
    let bare = document(json!([]), Vec::new()).to_string();
    let dropped = commit(path, &[(CRATE_FILE, bare.as_str())], &[]).await;
    let error = kept(path, &first, &dropped).await.expect_err("file stays");
    assert!(
        error
            .to_string()
            .contains("data/a.csv is still in the repository")
    );
    let removed = commit(path, &[], &["data/a.csv"]).await;
    kept(path, &first, &removed)
        .await
        .expect("entity and file removed");
    git(path, &["reset", "-q", "--hard", &first]).await;
    let only_file = commit(path, &[], &["data/a.csv"]).await;
    kept(path, &first, &only_file)
        .await
        .expect("only the file removed");
}

#[tokio::test]
async fn merges_added_files() {
    let (directory, first) = repository().await;
    let path = directory.path();
    let head = commit(path, &[("data/b.csv", "c\n")], &[]).await;
    let live = listed().to_string();
    let merged = crate::arc::merge_metadata(path, Some(&first), &head, &live)
        .await
        .expect("merge runs")
        .expect("merge succeeds")
        .expect("graph changes");
    let merged: Value = serde_json::from_str(&merged).unwrap();
    assert_eq!(entity(&merged, "data/b.csv").unwrap()["@type"], "File");
    let parts = &entity(&merged, "./").unwrap()["hasPart"];
    assert!(
        parts
            .as_array()
            .unwrap()
            .contains(&json!({"@id": "data/b.csv"}))
    );
    let merged = crate::arc::merge_metadata(path, Some(&head), &head, &live).await;
    assert_eq!(merged.unwrap(), Ok(None));
}

fn snapshot(jsonld: String) -> GitSnapshot {
    GitSnapshot {
        document_id: Ulid::from(7),
        event_id: Ulid::from(8),
        occurred_at_ms: 1_700_000_000_000,
        jsonld,
        objects: Vec::new(),
        message: None,
    }
}

async fn tree(path: &Path, commit: &str) -> Vec<String> {
    let listing = git(path, &["ls-tree", "-r", "--name-only", commit]).await;
    listing.lines().map(str::to_owned).collect()
}

#[tokio::test]
async fn keeps_plain_snapshots() {
    let (directory, first) = repository().await;
    let path = directory.path();
    let mut edited = listed();
    edited["@graph"][1]["name"] = json!("Renamed");
    let refs = Refs::from([("refs/heads/main".to_string(), first.clone())]);
    let (aruna, main) = crate::arc::generate(path, snapshot(edited.to_string()), &refs)
        .await
        .expect("generate runs")
        .expect("plain snapshot");
    assert_eq!(tree(path, &aruna).await, [CRATE_FILE]);
    let main = main.expect("main follows the metadata");
    assert_eq!(tree(path, &main).await, ["data/a.csv", CRATE_FILE]);
    assert_eq!(layout(path, &main).await.unwrap(), Layout::RoCrate);
    let text = git(path, &["show", &format!("{main}:{CRATE_FILE}")]).await;
    let written: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(written, edited);
}

#[tokio::test]
async fn falls_back_plain() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path();
    git(path, &["init", "-q", "--initial-branch=main"]).await;
    let source = snapshot(listed().to_string());
    // The files a failed ARC conversion leaves the snapshot of a new repository with.
    let files = files(&source.jsonld, &source.objects).expect("plain files");
    let (aruna, main) = crate::arc::snapshot_commit(path, &source, &Refs::new(), files)
        .await
        .expect("commit");
    assert_eq!(main.as_deref(), Some(aruna.as_str()));
    assert_eq!(tree(path, &aruna).await, [CRATE_FILE]);
    assert_eq!(layout(path, &aruna).await.unwrap(), Layout::RoCrate);
}
