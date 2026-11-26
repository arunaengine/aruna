//! Component rules of both protocols, directory projection, case resolution, export
//! containment and the job input tree.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::VfsError;
use super::path::*;
use aruna_core::structs::storage::vfs::NfsMountTarget;
use aruna_core::vfs_effects::VfsProtocol::{Nfs, Smb};

fn lookup(name: &str, protocol: aruna_core::vfs_effects::VfsProtocol) -> bool {
    check_component(name, protocol, NameUse::Lookup, false).is_ok()
}

#[test]
fn shared_component_rules() {
    // Empty, dot, separator and control names are refused by both protocols.
    for name in ["", ".", "..", "a/b", "nul\0", "tab\t", "\u{1f}x"] {
        assert!(!lookup(name, Nfs), "{name:?} on NFS");
        assert!(!lookup(name, Smb), "{name:?} on SMB");
    }
    assert!(lookup("report.csv", Nfs) && lookup("report.csv", Smb));
    // Internal names are refused only at the bucket root.
    for internal in ["_jobs", "_parts"] {
        let root = check_component(internal, Nfs, NameUse::Lookup, true);
        assert_eq!(root, Err(VfsError::InvalidName));
        assert!(lookup(internal, Nfs));
    }
}

#[test]
fn protocol_length_limits() {
    // NFS counts UTF-8 bytes, SMB counts UTF-16 code units.
    let wide = "\u{1F600}".repeat(100);
    assert_eq!(wide.len(), 400);
    assert!(!lookup(&wide, Nfs));
    assert!(lookup(&wide, Smb));
    let long = "\u{1F600}".repeat(128);
    assert!(!lookup(&long, Smb));
    assert!(lookup(&"a".repeat(255), Nfs) && !lookup(&"a".repeat(256), Nfs));
}

#[test]
fn smb_extra_rules() {
    for name in ["a<b", "a>b", "a:b", "a\"b", "a\\b", "a|b", "a?b", "a*b"] {
        assert!(!lookup(name, Smb), "{name}");
        assert!(lookup(name, Nfs), "{name} stays valid on NFS");
    }
    for device in [
        "CON",
        "con",
        "Prn.txt",
        "aux",
        "NUL.tar.gz",
        "COM1",
        "lpt9.log",
    ] {
        assert!(device_name(device), "{device}");
        assert!(!lookup(device, Smb));
    }
    for plain in ["COM0", "COM10", "LPT", "console", "nullable"] {
        assert!(!device_name(plain), "{plain}");
    }
    // A trailing dot or space is refused only for a new name.
    for name in ["draft.", "draft "] {
        assert!(lookup(name, Smb));
        let create = check_component(name, Smb, NameUse::Create, false);
        assert_eq!(create, Err(VfsError::InvalidName));
    }
}

#[test]
fn directory_shadows_file() {
    let facts = |live_file, live_marker, descendant, staged_file, staged_below| NameFacts {
        live_file,
        live_marker,
        descendant,
        staged_file,
        staged_below,
    };
    assert_eq!(
        project_name(facts(true, false, false, false, false)),
        Projected::File
    );
    assert_eq!(
        project_name(facts(false, false, false, true, false)),
        Projected::File
    );
    // A marker, a live descendant or a staged name below makes a directory over the file.
    assert_eq!(
        project_name(facts(true, true, false, false, false)),
        Projected::Directory
    );
    assert_eq!(
        project_name(facts(true, false, true, false, false)),
        Projected::Directory
    );
    assert_eq!(
        project_name(facts(false, false, false, false, true)),
        Projected::Directory
    );
    assert_eq!(project_name(NameFacts::default()), Projected::Missing);
}

#[test]
fn case_resolution_order() {
    let names = ["Report.csv", "notes.txt", "NOTES.TXT"];
    let names = || names.iter().copied();
    assert_eq!(
        resolve_name("notes.txt", names(), Smb),
        Resolved::Exact("notes.txt".to_string())
    );
    assert_eq!(
        resolve_name("report.CSV", names(), Smb),
        Resolved::Folded("Report.csv".to_string())
    );
    // Two spellings fold together: only their exact spellings resolve.
    assert_eq!(resolve_name("Notes.Txt", names(), Smb), Resolved::Ambiguous);
    // NFS never folds case.
    assert_eq!(resolve_name("report.csv", names(), Nfs), Resolved::Missing);
}

#[test]
fn folder_containment_components() {
    let target = NfsMountTarget::for_folder("bucket", 7, "team").unwrap();
    assert!(export_holds(&target, "bucket", "team/a.txt"));
    assert!(export_holds(&target, "bucket", "team"));
    assert!(!export_holds(&target, "bucket", "team-old/a.txt"));
    assert!(!export_holds(&target, "bucket", "teamwork"));
    assert!(!export_holds(&target, "other", "team/a.txt"));
}

#[test]
fn key_helpers() {
    assert_eq!(child_key("", "a"), "a");
    assert_eq!(child_key("p/q", "a"), "p/q/a");
    assert_eq!(split_parent("p/q/a"), Some(("p/q", "a")));
    assert_eq!(split_parent("a"), Some(("", "a")));
    assert_eq!(split_parent(""), None);
    assert_eq!(ancestors("p/q/a"), vec!["p/q".to_string(), "p".to_string()]);
    assert!(within("p", "p/q") && !within("p", "pq"));
    assert!(creates_cycle("p", "p/q/p") && !creates_cycle("p", "p"));
}

fn entry(path: &str, identity: &str) -> JobEntry {
    JobEntry {
        path: path.to_string(),
        identity: identity.to_string(),
        size: 1,
    }
}

#[test]
fn job_ids_stable() {
    // Ids come from the sorted neutral list, never from input order or a caller's filter.
    let entries = [entry("in/b.txt", "s/b@1"), entry("in/a.txt", "s/a@1")];
    let reversed = [entries[1].clone(), entries[0].clone()];
    let (tree, again) = (
        JobTree::build(&entries).unwrap(),
        JobTree::build(&reversed).unwrap(),
    );
    assert_eq!(tree, again);
    let folder = tree.lookup(JOB_ROOT, "in").unwrap().node_id;
    let a = tree.lookup(folder, "a.txt").unwrap().node_id;
    let b = tree.lookup(folder, "b.txt").unwrap().node_id;
    assert!(a < b);
    // A caller who sees only `b.txt` keeps the same id for it.
    let shown: Vec<_> = tree
        .children(folder, |entry| entry.path == "in/b.txt")
        .collect();
    assert_eq!(shown.len(), 1);
    assert_eq!(shown[0].node_id, b);
    // A directory with no visible file below it is hidden as well.
    assert_eq!(tree.children(JOB_ROOT, |_| false).count(), 0);
    assert_eq!(
        tree.entry(shown[0]).map(|entry| entry.identity.as_str()),
        Some("s/b@1")
    );
}

#[test]
fn job_tree_ambiguity() {
    // A path that is a file and a directory at once is ambiguous.
    let clash = [entry("in", "s/x@1"), entry("in/a", "s/a@1")];
    assert_eq!(JobTree::build(&clash), Err(VfsError::Conflict));
    // The same path captured with two identities is ambiguous too.
    let twice = [entry("in/a", "s/a@1"), entry("in/a", "s/a@2")];
    assert_eq!(JobTree::build(&twice), Err(VfsError::Conflict));
    // An exact duplicate is one entry.
    let same = [entry("in/a", "s/a@1"), entry("in/a", "s/a@1")];
    assert!(JobTree::build(&same).is_ok());
    assert_eq!(
        JobTree::build(&[entry("in/..", "s/a@1")]),
        Err(VfsError::InvalidName)
    );
}
