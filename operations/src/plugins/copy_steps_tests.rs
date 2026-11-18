//! Tests the copy decisions as calls and results: the checks that form the input, the version a
//! copy reads, and what a source behind a locked key waits for.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::structs::storage::multipart::MultipartChecksumType;
use aruna_plugins::broker::{ErrorKind, HostError};
use serde_json::json;

use super::*;
use crate::plugins::fixtures::{self, is_child, node};
use crate::plugins::rows::tests::child_result;

fn version() -> Ulid {
    Ulid::from_parts(5, 5)
}

fn job() -> Ulid {
    Ulid::from_parts(6, 6)
}

fn prepare(install: &PluginInstall, source_version: Option<Ulid>) -> PrepareCopy {
    let config = fixtures::config(install, json!({}));
    PrepareCopy::new(CopyWork {
        node: (fixtures::realm(), node(3)),
        job: job(),
        config: &config,
        install,
        source: "in/a",
        version: source_version,
        target: "out/a",
    })
}

fn place(bucket: &str, key: &str) -> IoResult {
    child_result(Ok::<Place, CallError>((
        bucket.to_string(),
        key.to_string(),
    )))
}

fn checked(copy: &mut PrepareCopy) {
    assert!(is_child(&copy.first()));
    let patterns = BTreeSet::from(["p".to_string()]);
    assert!(is_child(
        &copy.next(child_result(Ok::<_, CallError>(patterns)))
    ));
    assert!(is_child(&copy.next(place("in", "a"))));
    assert!(is_child(&copy.next(place("out", "a"))));
}

fn readable(found: Option<Ulid>) -> IoResult {
    child_result(Ok::<_, CallError>(found))
}

#[test]
fn checks_form_input() {
    let install = fixtures::install();
    let mut copy = prepare(&install, None);
    checked(&mut copy);
    let Ok(Next::Done(input)) = copy.next(readable(Some(version()))) else {
        panic!("the checked copy is ready");
    };
    let source = (input.source_bucket.as_str(), input.source_key.as_str());
    assert_eq!(source, ("in", "a"));
    assert_eq!(
        (input.dest_bucket.as_str(), input.dest_key.as_str()),
        ("out", "a")
    );
    assert_eq!(input.source_version_id, Some(version()));
    assert_eq!(input.user_id, install.account);
    assert_eq!(input.node_id, node(3));
    assert_eq!(input.job_id, Some(job()));
    assert_eq!(input.references, CopyReferences::Materialize);
    assert_eq!(
        input.restrictions,
        account(&install, &BTreeSet::from(["p".to_string()])).path_restrictions
    );
}

#[test]
fn named_version_stays() {
    let install = fixtures::install();
    let mut copy = prepare(&install, Some(version()));
    checked(&mut copy);
    let Ok(Next::Done(input)) = copy.next(readable(None)) else {
        panic!("the named version is copied");
    };
    assert_eq!(input.source_version_id, Some(version()));
}

#[test]
fn refusal_stops_copy() {
    let install = fixtures::install();
    let mut copy = prepare(&install, None);
    copy.first().unwrap();
    let denied = CallError(HostError::new(ErrorKind::Denied, "no"));
    let answer = child_result(Err::<BTreeSet<String>, _>(denied.clone()));
    assert_eq!(copy.next(answer), Err(denied));
}

#[test]
fn marked_source_stops() {
    let install = fixtures::install();
    let mut copy = prepare(&install, None);
    checked(&mut copy);
    let marked = CallError(HostError::new(ErrorKind::Denied, "marked"));
    let answer = child_result(Err::<Option<Ulid>, _>(marked.clone()));
    assert_eq!(copy.next(answer), Err(marked));
}

#[test]
fn locked_errors_recognized() {
    let key = BucketKeyError::Locked(version());
    let locked = GetObjectError::ConversionError(ConversionError::BucketKey(key));
    assert!(read_locked(&locked));
    assert!(copy_locked(&CopyObjectError::Get(locked)));
    assert!(!read_locked(&GetObjectError::NoSuchKey));
    assert!(!copy_locked(&CopyObjectError::Get(
        GetObjectError::NoSuchKey
    )));
}

fn head(resolved: Option<Ulid>) -> IoResult {
    child_result(Ok::<_, HeadObjectError>(HeadObjectResult {
        location: None,
        metadata: HashMap::new(),
        source_metadata: None,
        last_refresh: None,
        version_created_at: None,
        version_id: resolved,
        resolved_version_id: resolved,
        checksum_type: MultipartChecksumType::FullObject,
        composite_hashes: HashMap::new(),
        part_count: None,
        source_policies: Vec::new(),
        source_binding: None,
    }))
}

fn locked_source() -> LockedSource {
    let read = ("in".to_string(), "a".to_string(), Some(version()));
    LockedSource::new(node(3), &fixtures::install(), read)
}

#[test]
fn unlocked_source_retries() {
    let mut wait = locked_source();
    assert!(is_child(&wait.first()));
    assert!(is_child(&wait.next(head(Some(version())))));
    let unlocked = child_result(Ok::<Option<KeyWait>, KeyCheckError>(None));
    assert_eq!(wait.next(unlocked), Ok(Next::Done(None)));
}

#[test]
fn unknown_version_defers() {
    let mut wait = locked_source();
    wait.first().unwrap();
    assert!(wait.next(head(None)).is_err());
    let mut failed = locked_source();
    failed.first().unwrap();
    let gone = child_result(Err::<HeadObjectResult, _>(HeadObjectError::NoSuchVersion));
    assert!(failed.next(gone).is_err());
}

#[test]
fn check_failure_surfaces() {
    let mut wait = locked_source();
    wait.first().unwrap();
    wait.next(head(Some(version()))).unwrap();
    let broken = KeyCheckError("no row".to_string());
    let answer = child_result(Err::<Option<KeyWait>, _>(broken.clone()));
    assert_eq!(wait.next(answer), Err(broken));
}
