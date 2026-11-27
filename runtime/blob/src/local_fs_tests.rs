//! Tests that the local folder service lists in key order and never leaves its root.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use opendal::Operator;
use std::path::Path;
use std::sync::Mutex;
use tempfile::TempDir;

fn operator(root: &Path) -> Operator {
    Operator::new(LocalFsBuilder::default().root(root.to_str().unwrap())).unwrap()
}

fn writable(root: &Path) -> Operator {
    let builder = LocalFsBuilder::default()
        .root(root.to_str().unwrap())
        .writable(true);
    Operator::new(builder).unwrap()
}

fn filtered(root: &Path, filter: PathFilter) -> Operator {
    let builder = LocalFsBuilder::default()
        .root(root.to_str().unwrap())
        .filter(filter);
    Operator::new(builder).unwrap()
}

#[cfg(unix)]
fn link(target: &Path, link: &Path) {
    std::os::unix::fs::symlink(target, link).unwrap();
}

#[cfg(windows)]
fn link(target: &Path, link: &Path) {
    if target.is_dir() {
        std::os::windows::fs::symlink_dir(target, link).unwrap();
    } else {
        std::os::windows::fs::symlink_file(target, link).unwrap();
    }
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
    link(&outside.path().join("secret"), &root.path().join("link"));
    link(outside.path(), &root.path().join("folder"));
    link(&root.path().join("plain"), &root.path().join("inside"));
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
async fn writes_in_place() {
    let root = tempfile::tempdir().unwrap();
    put(root.path(), "old", "old");
    let op = writable(root.path());

    op.write("deep/new/file", "new").await.unwrap();
    assert_eq!(op.read("deep/new/file").await.unwrap().to_vec(), b"new");
    op.write("old", "replaced").await.unwrap();
    assert_eq!(std::fs::read(root.path().join("old")).unwrap(), b"replaced");
    op.write("empty", "").await.unwrap();
    assert_eq!(op.stat("empty").await.unwrap().content_length(), 0);
    op.create_dir("made/folder/").await.unwrap();
    assert!(root.path().join("made/folder").is_dir());
    assert_eq!(
        files(&op, "", None).await,
        ["deep/new/file", "empty", "old"]
    );
}

#[tokio::test]
async fn creates_only_once() {
    let root = tempfile::tempdir().unwrap();
    put(root.path(), "taken", "first");
    let op = writable(root.path());

    let error = op
        .write_with("taken", "second")
        .if_not_exists(true)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ConditionNotMatch);
    assert_eq!(std::fs::read(root.path().join("taken")).unwrap(), b"first");
    op.write_with("free", "new")
        .if_not_exists(true)
        .await
        .unwrap();
    assert_eq!(std::fs::read(root.path().join("free")).unwrap(), b"new");
    // Neither write leaves a temporary file behind.
    let mut names: Vec<_> = std::fs::read_dir(root.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    assert_eq!(names, ["free", "taken"]);
}

#[tokio::test]
async fn hides_unfinished_writes() {
    let root = tempfile::tempdir().unwrap();
    let op = writable(root.path());

    let mut writer = op.writer("part").await.unwrap();
    writer.write("half").await.unwrap();
    assert!(files(&op, "", None).await.is_empty());
    writer.abort().await.unwrap();
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    put(root.path(), &format!("{TEMP_PREFIX}left"), "left");
    assert!(files(&op, "", None).await.is_empty());
}

#[tokio::test]
async fn deletes_entries() {
    let root = tempfile::tempdir().unwrap();
    put(root.path(), "a/file", "file");
    put(root.path(), "full/file", "file");
    let op = writable(root.path());

    op.delete("a/file").await.unwrap();
    op.delete("a/").await.unwrap();
    op.delete("missing").await.unwrap();
    op.delete("missing/deeper/file").await.unwrap();
    assert!(!root.path().join("a").exists());
    assert!(op.delete("full/").await.is_err());
    assert!(root.path().join("full/file").exists());
}

#[tokio::test]
async fn writes_avoid_symlinks() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    put(outside.path(), "secret", "secret");
    link(outside.path(), &root.path().join("folder"));
    link(&outside.path().join("secret"), &root.path().join("file"));
    let op = writable(root.path());

    assert!(op.write("folder/new", "new").await.is_err());
    assert!(op.create_dir("folder/new/").await.is_err());
    op.delete("folder/secret").await.unwrap();
    op.delete("file").await.unwrap();
    assert!(!outside.path().join("new").exists());
    assert_eq!(
        std::fs::read(outside.path().join("secret")).unwrap(),
        b"secret"
    );
    // A create-only write finds the symlink's name taken; a plain one replaces the link.
    let error = op.write_with("file", "x").if_not_exists(true).await;
    assert_eq!(error.unwrap_err().kind(), ErrorKind::ConditionNotMatch);
    op.write("file", "own").await.unwrap();
    assert_eq!(
        std::fs::read(outside.path().join("secret")).unwrap(),
        b"secret"
    );
}

#[tokio::test]
async fn serves_subfolder() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    put(root.path(), "top", "top");
    put(root.path(), "sub/inner", "inner");
    link(outside.path(), &root.path().join("linked"));
    let served = |folder: &str| {
        let builder = LocalFsBuilder::default()
            .root(root.path().to_str().unwrap())
            .folder(folder);
        Operator::new(builder).unwrap()
    };

    assert_eq!(files(&served("sub/"), "", None).await, ["inner"]);
    // A missing served folder fails every listing instead of listing nothing.
    for folder in ["linked/", "missing/"] {
        let error = served(folder).list("").await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid, "{folder}");
    }
    let builder = LocalFsBuilder::default().root("/").folder("../");
    assert!(Operator::new(builder).is_err());
    assert!(!served("sub/").info().capability().write);
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
    let mode = rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR;
    rustix::fs::mknodat(rustix::fs::CWD, &fifo, rustix::fs::FileType::Fifo, mode, 0).unwrap();
    let op = operator(root.path());

    assert_eq!(files(&op, "", None).await, ["plain"]);
    assert_eq!(
        op.read("pipe").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
}

/// A writable core that serves `folder` below `root`, built without the operator around it.
fn writable_core(root: &Path, folder: &[&str]) -> Core {
    Core {
        root: root.to_path_buf(),
        base: folder.iter().map(|name| name.to_string()).collect(),
        writable: true,
        filter: None,
    }
}

#[test]
fn keeps_folder_chain() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("served")).unwrap();
    let core = writable_core(root.path(), &["served"]);

    // Publish syncs the parent and every folder above it, down from the served one.
    let mut spool = Spool::open(&core, "a/b/blob").unwrap();
    assert_eq!(spool.above.len(), 2);
    spool.file.write_all(b"nested").unwrap();
    spool.size = 6;
    assert_eq!(spool.publish(false).unwrap(), 6);
    let written = root.path().join("served/a/b/blob");
    assert_eq!(std::fs::read(written).unwrap(), b"nested");
    let spool = Spool::open(&core, "top").unwrap();
    assert!(spool.above.is_empty());
    assert_eq!(spool.publish(true).unwrap(), 0);
    assert_eq!(std::fs::read(root.path().join("served/top")).unwrap(), b"");
}

#[tokio::test]
async fn publishes_nested_writes() {
    let root = tempfile::tempdir().unwrap();
    put(root.path(), "old/kept", "kept");
    let op = writable(root.path());

    op.write_with("x/y/z/blob", "created")
        .if_not_exists(true)
        .await
        .unwrap();
    assert_eq!(op.read("x/y/z/blob").await.unwrap().to_vec(), b"created");
    let mut writer = op.writer("old/blob").await.unwrap();
    writer.write("first ").await.unwrap();
    writer.write("second").await.unwrap();
    writer.close().await.unwrap();
    assert_eq!(op.read("old/blob").await.unwrap().to_vec(), b"first second");
    op.write("old/kept", "replaced").await.unwrap();
    assert_eq!(op.read("old/kept").await.unwrap().to_vec(), b"replaced");
    // No folder keeps a temporary file of the writes.
    for folder in ["x/y/z", "old"] {
        let names = std::fs::read_dir(root.path().join(folder)).unwrap();
        let temps = names
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().starts_with(TEMP_PREFIX))
            .count();
        assert_eq!(temps, 0, "{folder}");
    }
}

#[tokio::test]
async fn blocked_parent_fails() {
    let root = tempfile::tempdir().unwrap();
    put(root.path(), "taken", "file");
    let op = writable(root.path());

    let error = op.write("taken/blob", "data").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert!(op.create_dir("taken/sub/").await.is_err());
    assert_eq!(std::fs::read(root.path().join("taken")).unwrap(), b"file");
}

#[tokio::test]
async fn failed_close_ends() {
    let root = tempfile::tempdir().unwrap();
    put(root.path(), "target/inner", "busy");
    let mut writer = FileWriter {
        core: Arc::new(writable_core(root.path(), &[])),
        path: "target".to_string(),
        if_not_exists: false,
        state: WriteState::Unopened,
    };

    oio::Write::write(&mut writer, Buffer::from("data"))
        .await
        .unwrap();
    // The rename fails while a full folder holds the name, after the spool is used up.
    assert!(oio::Write::close(&mut writer).await.is_err());
    std::fs::remove_dir_all(root.path().join("target")).unwrap();
    // A retried close must not publish a new, empty spool over the now free name.
    let error = oio::Write::close(&mut writer).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    let error = oio::Write::write(&mut writer, Buffer::from("more"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
