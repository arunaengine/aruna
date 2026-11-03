//! Tests the sync plan against fixed local files and a fake remote listing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use std::collections::HashMap;

fn local(path: &str, size: u64) -> LocalFile {
    LocalFile {
        path: path.into(),
        size,
    }
}

fn remote(path: &str, size: u64, blake3: Option<&str>) -> RemoteFile {
    RemoteFile {
        path: path.into(),
        size,
        blake3: blake3.map(Into::into),
    }
}

fn digests(known: &[(&str, &str)]) -> HashMap<String, String> {
    known
        .iter()
        .map(|(path, hash)| (path.to_string(), hash.to_string()))
        .collect()
}

#[test]
fn plans_upload_changes() {
    let local_files = vec![
        local("a.txt", 3),
        local("b.txt", 4),
        local("c.txt", 5),
        local("d.txt", 6),
        local("e.txt", 7),
    ];
    let remote_files = vec![
        remote("b.txt", 9, Some("x")),
        remote("c.txt", 5, Some("same")),
        remote("d.txt", 6, Some("old")),
        remote("e.txt", 7, None),
        remote("gone.txt", 1, Some("y")),
    ];
    let known = digests(&[("c.txt", "SAME"), ("d.txt", "new")]);
    let mut read = Vec::new();
    let plan = plan(Direction::Upload, &local_files, &remote_files, |path| {
        read.push(path.to_string());
        Ok(known[path].clone())
    })
    .unwrap();
    let reasons: Vec<(&str, Reason)> = plan
        .transfers
        .iter()
        .map(|transfer| (transfer.path.as_str(), transfer.reason))
        .collect();
    assert_eq!(
        reasons,
        vec![
            ("a.txt", Reason::Missing),
            ("b.txt", Reason::Size),
            ("d.txt", Reason::Checksum),
        ]
    );
    assert_eq!(plan.same, 1);
    assert_eq!(plan.unverified, vec!["e.txt".to_string()]);
    assert_eq!(plan.extra, vec!["gone.txt".to_string()]);
    assert_eq!(plan.bytes(), 3 + 4 + 6);
    // Only equal sizes with a remote digest are hashed.
    assert_eq!(read, vec!["c.txt".to_string(), "d.txt".to_string()]);
}

#[test]
fn plans_download_changes() {
    let local_files = vec![local("keep.txt", 2), local("local-only.txt", 1)];
    let remote_files = vec![
        remote("keep.txt", 2, Some("h")),
        remote("new/deep.txt", 8, Some("n")),
    ];
    let known = digests(&[("keep.txt", "h")]);
    let plan = plan(Direction::Download, &local_files, &remote_files, |path| {
        Ok(known[path].clone())
    })
    .unwrap();
    assert_eq!(
        plan.transfers,
        vec![Transfer {
            path: "new/deep.txt".into(),
            size: 8,
            reason: Reason::Missing,
        }]
    );
    assert_eq!(plan.same, 1);
    assert_eq!(plan.extra, vec!["local-only.txt".to_string()]);
}

#[test]
fn passes_digest_failure() {
    let result = plan(
        Direction::Upload,
        &[local("a", 1)],
        &[remote("a", 1, Some("h"))],
        |_| Err(CliError::usage("unreadable")),
    );
    assert!(result.is_err());
}

#[test]
fn walks_nested_files() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("sub/deeper")).unwrap();
    std::fs::write(dir.path().join("top.txt"), b"1").unwrap();
    std::fs::write(dir.path().join("sub/deeper/low.txt"), b"22").unwrap();
    std::fs::write(dir.path().join("sub/skip.txt.aruna-part"), b"x").unwrap();
    let files = walk_local(dir.path()).unwrap();
    assert_eq!(
        files,
        vec![local("sub/deeper/low.txt", 2), local("top.txt", 1)]
    );
}

#[test]
fn refuses_unsafe_keys() {
    for key in ["a/b.txt", "a b/c:d", "x"] {
        assert_eq!(
            is_safe_key(key),
            !cfg!(windows) || !key.contains(':'),
            "{key}"
        );
    }
    for key in [
        "../x",
        "a/../x",
        "/abs",
        "a//b",
        "./a",
        "..\\outside.txt",
        "C:\\x",
        "",
    ] {
        assert!(!is_safe_key(key), "{key}");
    }
    let dir = tempfile::tempdir().unwrap();
    assert!(local_path(dir.path(), "..\\outside.txt").is_err());
    let path = local_path(dir.path(), "sub/new/a.txt").unwrap();
    assert_eq!(path, dir.path().join("sub").join("new").join("a.txt"));
}

#[cfg(unix)]
#[test]
fn refuses_linked_folders() {
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), dir.path().join("link")).unwrap();
    let error = local_path(dir.path(), "link/a.txt").unwrap_err();
    assert!(error.to_string().contains("is a link"), "{error}");
    std::fs::create_dir(dir.path().join("real")).unwrap();
    assert!(local_path(dir.path(), "real/a.txt").is_ok());
}

#[test]
fn finds_case_clashes() {
    let clashes = case_clashes(["A.txt", "b/c", "a.txt", "B/C", "b/c", "d"]);
    assert_eq!(clashes, vec!["A.txt", "a.txt", "B/C", "b/c"]);
    assert!(case_clashes(["a", "b", "a"]).is_empty());
}
