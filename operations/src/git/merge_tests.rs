//! Tests editing drafts and merging branches call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::git::{FileChangeKind, LfsLock};
use serde_json::json;

use super::super::testing::{
    asks_child, asks_store, auth, child, commit, dataset, git_node, projection, ran, record,
};
use super::*;

const DRAFT: &str = "refs/heads/draft";

fn id() -> Ulid {
    dataset().document_id
}

fn opened(refs: &[(&str, &str)]) -> IoResult {
    child(Ok::<Opened, GitError>((dataset(), projection(refs))))
}

fn options(expected: Option<&str>) -> WriteOptions {
    WriteOptions {
        message: None,
        expected: expected.map(str::to_owned),
    }
}

fn published() -> IoResult {
    let change = aruna_core::git::GitChange::Unlock { id: Ulid::from(9) };
    child(Ok::<_, GitError>(record(1, change)))
}

fn changed(path: &str) -> IoResult {
    child(Ok::<_, GitError>(vec![FileChange {
        path: path.to_string(),
        change: FileChangeKind::Modified,
    }]))
}

fn editing() -> EditOperation {
    let edit = edit(
        &git_node(),
        &auth(),
        id(),
        "draft",
        "{}".to_string(),
        options(None),
    );
    edit
}

/// An edit stepped to the answer of the store's edit.
fn edited() -> EditOperation {
    let mut run = editing();
    assert!(asks_child(&run.first()));
    assert!(asks_child(&run.next(opened(&[(DRAFT, "old")]))));
    let links = child(Ok::<_, GitError>(Vec::<LinkedObject>::new()));
    assert!(asks_store(&run.next(links)));
    run
}

// Protected branches change only through the metadata document.
#[test]
fn protected_branch_refused() {
    let mut run = edit(
        &git_node(),
        &auth(),
        id(),
        "main",
        "{}".to_string(),
        options(None),
    );
    assert!(matches!(run.first(), Err(GitError::Refused(_))));
}

// Edits hold the repository and need the expected head.
#[test]
fn edit_checks_head() {
    let run = editing();
    assert_eq!(run.hold_scope(), Some(HoldScope::GitRepository(id())));
    let mut stale = edit(
        &git_node(),
        &auth(),
        id(),
        "draft",
        "{}".to_string(),
        options(Some("other")),
    );
    stale.first().unwrap();
    assert_eq!(stale.next(opened(&[(DRAFT, "old")])), Err(GitError::Stale));
}

#[test]
fn missing_draft_refused() {
    let mut run = editing();
    run.first().unwrap();
    assert_eq!(run.next(opened(&[])), Err(GitError::NotFound));
}

// An edit that changes nothing is answered without a record.
#[test]
fn unchanged_edit_unrecorded() {
    let mut run = edited();
    let same = ran(GitEvent::Edited(Ok("old".to_string())));
    assert!(asks_child(&run.next(same)));
    assert_eq!(run.step, EditStep::Log);
}

// The snapshot carries the runner's time, not a clock read.
#[test]
fn edit_uses_stamp() {
    let mut run = editing();
    run.stamp(77);
    assert_eq!(run.now_ms, 77);
}

// A new version is packed, recorded and shown with the moved branch.
#[test]
fn edit_recorded() {
    let mut run = edited();
    assert!(asks_child(
        &run.next(ran(GitEvent::Edited(Ok("new".to_string()))))
    ));
    assert!(asks_child(&run.next(changed("a.txt"))));
    let pack = child(Ok::<_, GitError>(Bytes::from_static(b"pack")));
    assert!(asks_child(&run.next(pack)));
    assert!(asks_child(&run.next(published())));
    let logged = child(Ok::<_, GitError>(vec![commit("new")]));
    assert!(asks_child(&run.next(logged)));
    let Ok(Next::Done(shown)) = run.next(child(Ok::<_, GitError>(Vec::<Named>::new()))) else {
        panic!("expected the version");
    };
    assert_eq!(shown.branches, vec!["draft".to_string()]);
}

// A file another user locked refuses the edit.
#[test]
fn locked_path_refused() {
    let mut run = edited();
    run.next(ran(GitEvent::Edited(Ok("new".to_string()))))
        .unwrap();
    let mut known = projection(&[(DRAFT, "old")]);
    let other = aruna_core::UserId::nil(aruna_core::structs::identity::realm::RealmId::from_bytes(
        [9; 32],
    ));
    let lock = LfsLock {
        id: Ulid::from(1),
        path: "a.txt".to_string(),
        user_id: other,
        locked_at_ms: 1,
        claim: Ulid::from(1),
    };
    known.state.locks.insert("a.txt".to_string(), lock);
    run.opened = Some((dataset(), known));
    let expected = GitError::Locked("a.txt".to_string());
    assert_eq!(run.next(changed("a.txt")), Err(expected));
}

fn into(refs: &[(&str, &str)], branch: &str, expected: Option<&str>) -> MergeIntoOperation {
    let scope = MergeScope {
        node: git_node(),
        auth: auth(),
        document: dataset(),
        projection: projection(refs),
    };
    merge_into(
        scope,
        "source".to_string(),
        branch,
        options(expected),
        Vec::new(),
    )
}

/// A merge into main stepped to the answer of the Git merge.
fn merging() -> MergeIntoOperation {
    let mut run = into(&[("refs/heads/main", "target")], "main", None);
    assert!(asks_store(&run.first()));
    assert!(asks_store(&run.next(ran(GitEvent::Resolved(None)))));
    run
}

#[test]
fn outer_merge_resolves() {
    let mut run = merge(&git_node(), &auth(), id(), "draft", "main", options(None));
    assert_eq!(run.hold_scope(), Some(HoldScope::GitRepository(id())));
    assert!(asks_child(&run.first()));
    assert!(asks_child(&run.next(opened(&[]))));
    let source = child(Ok::<_, GitError>("source".to_string()));
    assert!(asks_child(&run.next(source)));
    let merged = Merged {
        version: "new".to_string(),
        fast_forward: false,
    };
    let answer = run.next(child(Ok::<_, GitError>(merged.clone())));
    assert_eq!(answer, Ok(Next::Done(merged)));
}

#[test]
fn snapshot_branch_refused() {
    let mut run = into(&[], "aruna", None);
    assert!(matches!(run.first(), Err(GitError::Refused(_))));
}

#[test]
fn stale_target_refused() {
    let mut run = into(&[("refs/heads/main", "target")], "main", Some("other"));
    assert_eq!(run.first(), Err(GitError::Stale));
}

// Properties both sides changed differently refuse the merge before Git runs.
#[test]
fn property_conflict_refused() {
    let mut run = into(&[("refs/heads/main", "target")], "main", None);
    run.first().unwrap();
    assert!(asks_child(
        &run.next(ran(GitEvent::Resolved(Some("base".to_string()))))
    ));
    let side = |name: &str| {
        let graph = json!({"@graph": [{"@id": "./", "name": name}]});
        child(Ok::<_, GitError>(Some(graph)))
    };
    assert!(asks_child(&run.next(side("base"))));
    assert!(asks_child(&run.next(side("ours"))));
    assert!(matches!(
        run.next(side("theirs")),
        Err(GitError::MergeConflict(_))
    ));
}

// A side without readable metadata leaves the decision to Git.
#[test]
fn unreadable_crate_merges() {
    let mut run = into(&[("refs/heads/main", "target")], "main", None);
    run.first().unwrap();
    run.next(ran(GitEvent::Resolved(Some("base".to_string()))))
        .unwrap();
    let none = child(Ok::<Option<Value>, GitError>(None));
    assert!(asks_store(&run.next(none)));
    assert_eq!(run.step, IntoStep::Merge);
}

#[test]
fn file_conflict_reported() {
    let conflicts = MergeOutcome::Conflicts(vec!["a.txt".to_string()]);
    assert!(matches!(
        merging().next(ran(GitEvent::Merged(conflicts))),
        Err(GitError::MergeConflict(_))
    ));
}

// Nothing to merge records nothing.
#[test]
fn current_target_finishes() {
    let done = Merged {
        version: "target".to_string(),
        fast_forward: false,
    };
    let answer = merging().next(ran(GitEvent::Merged(MergeOutcome::UpToDate)));
    assert_eq!(answer, Ok(Next::Done(done)));
}

// A merge into main is recorded first, and its metadata follows without failing the merge.
#[test]
fn main_merge_applies() {
    let mut run = merging();
    let merged = MergeOutcome::Merged("new".to_string());
    assert!(asks_child(&run.next(ran(GitEvent::Merged(merged)))));
    assert!(asks_child(&run.next(changed("a.txt"))));
    let pack = child(Ok::<_, GitError>(Bytes::from_static(b"pack")));
    assert!(asks_child(&run.next(pack)));
    assert!(asks_child(&run.next(published())));
    assert_eq!(run.step, IntoStep::Pending);
    let waits = child(Err::<(), _>(GitError::Unavailable));
    let done = Merged {
        version: "new".to_string(),
        fast_forward: false,
    };
    assert_eq!(run.next(waits), Ok(Next::Done(done)));
}

fn conflict_ref(kind: &str, conflict: Ulid) -> String {
    format!("refs/conflicts/{kind}/draft/{conflict}")
}

fn resolving(conflict: Ulid) -> ResolveConflictOperation {
    let mut run = resolve_conflict(&git_node(), &auth(), (id(), conflict), None);
    assert_eq!(run.hold_scope(), Some(HoldScope::GitRepository(id())));
    assert!(asks_child(&run.first()));
    run
}

#[test]
fn unknown_conflict_missing() {
    let answer = resolving(Ulid::from(7)).next(opened(&[]));
    assert_eq!(answer, Err(GitError::NotFound));
}

// Tags never move, so a kept tag cannot be merged.
#[test]
fn kept_tag_refused() {
    let conflict = Ulid::from(7);
    let name = conflict_ref("tags", conflict);
    let answer = resolving(conflict).next(opened(&[(&name, "kept")]));
    assert!(matches!(answer, Err(GitError::Refused(_))));
}

#[test]
fn kept_branch_merged() {
    let conflict = Ulid::from(7);
    let name = conflict_ref("heads", conflict);
    let mut run = resolving(conflict);
    assert!(asks_child(&run.next(opened(&[(&name, "kept")]))));
    let merged = Merged {
        version: "new".to_string(),
        fast_forward: true,
    };
    let answer = run.next(child(Ok::<_, GitError>(merged.clone())));
    assert_eq!(answer, Ok(Next::Done(merged)));
}
