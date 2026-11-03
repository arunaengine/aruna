//! Tests that the read-only folder service lists in key order and never leaves its root.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use opendal::Operator;
use std::path::Path;
use std::sync::Mutex;
use tempfile::TempDir;

fn operator(root: &Path) -> Operator {
    Operator::new(ReadOnlyFsBuilder::default().root(root.to_str().unwrap())).unwrap()
}

fn filtered(root: &Path, filter: PathFilter) -> Operator {
    let builder = ReadOnlyFsBuilder::default()
        .root(root.to_str().unwrap())
        .filter(filter);
    Operator::new(builder).unwrap()
}

fn put(root: &Path, relative: &str, content: &str) {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// A root with names that sort differently per folder than as whole keys.
fn tree() -> TempDir {
    let root = tempfile::tempdir().unwrap();
    for name in ["b", "a/y/z", "a-b", "a/x", "a.c"] {
        put(root.path(), name, name);
    }
    root
}

async fn files(op: &Operator, path: &str, start_after: Option<&str>) -> Vec<String> {
    let mut listing = op.list_with(path).recursive(true);
    if let Some(after) = start_after {
        listing = listing.start_after(after);
    }
    let entries = listing.await.unwrap();
    entries
        .into_iter()
        .filter(|entry| entry.metadata().is_file())
        .map(|entry| entry.path().to_string())
        .collect()
}

#[tokio::test]
async fn lists_sorted_keys() {
    let root = tree();
    let listed = files(&operator(root.path()), "", None).await;

    assert_eq!(listed, ["a-b", "a.c", "a/x", "a/y/z", "b"]);
}

#[tokio::test]
async fn resumes_after_key() {
    let root = tree();
    let op = operator(root.path());

    assert_eq!(files(&op, "", Some("a/x")).await, ["a/y/z", "b"]);
    assert_eq!(
        files(&op, "", Some("a-b")).await,
        ["a.c", "a/x", "a/y/z", "b"]
    );
    // A key past a whole folder skips that folder without reading it.
    assert_eq!(files(&op, "", Some("a/zz")).await, ["b"]);
}

#[tokio::test]
async fn lists_name_prefix() {
    let root = tree();
    let op = operator(root.path());

    assert_eq!(files(&op, "a", None).await, ["a-b", "a.c", "a/x", "a/y/z"]);
    assert_eq!(files(&op, "a/", None).await, ["a/x", "a/y/z"]);
}

#[tokio::test]
async fn lists_one_level() {
    let root = tree();
    let entries = operator(root.path()).list_with("a/").await.unwrap();
    let paths: Vec<&str> = entries.iter().map(|entry| entry.path()).collect();

    assert_eq!(paths, ["a/", "a/x", "a/y/"]);
}

#[tokio::test]
async fn lists_missing_folder() {
    let root = tree();
    let op = operator(root.path());

    assert!(
        op.list_with("nothing/")
            .recursive(true)
            .await
            .unwrap()
            .is_empty()
    );
    let error = op.stat("nothing").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotFound);
}

#[tokio::test]
async fn skips_symlinks() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    put(outside.path(), "secret", "secret");
    put(root.path(), "plain", "plain");
    std::os::unix::fs::symlink(outside.path().join("secret"), root.path().join("link")).unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("folder")).unwrap();
    std::os::unix::fs::symlink(root.path().join("plain"), root.path().join("inside")).unwrap();
    let op = operator(root.path());

    assert_eq!(files(&op, "", None).await, ["plain"]);
    for path in ["link", "inside", "folder/secret", "folder/"] {
        let error = op.stat(path).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound, "stat {path}");
    }
    for path in ["link", "inside", "folder/secret"] {
        let error = op.read(path).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound, "read {path}");
    }
}

#[tokio::test]
async fn refuses_parent_paths() {
    let parent = tempfile::tempdir().unwrap();
    put(parent.path(), "secret", "secret");
    put(parent.path(), "root/plain", "plain");
    let op = operator(&parent.path().join("root"));

    assert!(op.read("../secret").await.is_err());
    assert!(op.read("plain/../../secret").await.is_err());
    assert!(path_parts("a/../b").is_err());
    assert!(path_parts("a/./b").is_err());
    assert_eq!(path_parts("/").unwrap(), Vec::<&str>::new());
}

#[tokio::test]
async fn filter_hides_paths() {
    let root = tempfile::tempdir().unwrap();
    for name in ["keep", "skip.tmp", "dir/keep", "dir/skip.tmp", "private/x"] {
        put(root.path(), name, name);
    }
    let asked = Arc::new(Mutex::new(Vec::new()));
    let seen = asked.clone();
    let filter: PathFilter = Arc::new(move |path: &str| {
        seen.lock().unwrap().push(path.to_string());
        !path.starts_with("private/") && !path.ends_with(".tmp")
    });
    let op = filtered(root.path(), filter);

    assert_eq!(files(&op, "", None).await, ["dir/keep", "keep"]);
    assert!(asked.lock().unwrap().contains(&"dir/".to_string()));
    let error = op.stat("private/x").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    let error = op.read("dir/skip.tmp").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(op.read("dir/keep").await.unwrap().to_vec(), b"dir/keep");
}

#[tokio::test]
async fn refuses_writes() {
    let root = tempfile::tempdir().unwrap();
    put(root.path(), "keep", "keep");
    let op = operator(root.path());

    assert_eq!(
        op.write("new", "new").await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert!(op.delete("keep").await.is_err());
    assert!(op.create_dir("folder/").await.is_err());
    assert!(root.path().join("keep").exists());
    assert!(!root.path().join("new").exists());
    assert!(!root.path().join("folder").exists());
}

#[tokio::test]
async fn reads_ranges() {
    let root = tempfile::tempdir().unwrap();
    put(root.path(), "digits", "0123456789");
    let op = operator(root.path());

    let part = op.read_with("digits").range(2..5).await.unwrap();
    assert_eq!(part.to_vec(), b"234");
    let metadata = op.stat("digits").await.unwrap();
    assert_eq!(metadata.content_length(), 10);
    assert!(metadata.last_modified().is_some());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn skips_fifo() {
    let root = tempfile::tempdir().unwrap();
    put(root.path(), "plain", "plain");
    let fifo = root.path().join("pipe");
    rustix::fs::mknodat(CWD, &fifo, FileType::Fifo, Mode::RUSR | Mode::WUSR, 0).unwrap();
    let op = operator(root.path());

    assert_eq!(files(&op, "", None).await, ["plain"]);
    assert_eq!(
        op.read("pipe").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
}
