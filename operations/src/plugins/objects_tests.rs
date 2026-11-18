//! Tests resolving plugin object paths and readable versions as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;

use super::*;
use crate::plugins::work::tests::sample_install;

fn realm() -> RealmId {
    RealmId::from_bytes([1u8; 32])
}

fn install() -> PluginInstall {
    sample_install(Ulid::from_parts(1, 1))
}

fn object(path: &str) -> LocalObject {
    let install = install();
    LocalObject::new((realm(), install.receiver), &install, path)
}

fn data_path(rest: &str) -> String {
    let install = install();
    format!(
        "/{}/g/{}/data/{}/{rest}",
        realm(),
        install.group_id,
        install.receiver
    )
}

fn kind_of<T: std::fmt::Debug>(result: Result<T, CallError>) -> ErrorKind {
    result.unwrap_err().0.kind
}

fn child<T>(value: T) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(Ok::<T, String>(value)))
}

#[test]
fn resolves_local_object() {
    let mut local = object(&data_path("bucket/dir/key"));
    assert!(matches!(local.first(), Ok(Next::Call(IoCall::Child(_)))));
    let found = IoResult::Child(ChildOutcome::new(Ok::<bool, ()>(true)));
    let expected = ("bucket".to_string(), "dir/key".to_string());
    assert_eq!(local.next(found), Ok(Next::Done(expected)));
}

#[test]
fn refuses_foreign_paths() {
    assert_eq!(kind_of(object("/other").first()), ErrorKind::Invalid);
    let other = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let install = install();
    let path = format!("/{}/g/{}/data/{other}/b/k", realm(), install.group_id);
    assert_eq!(kind_of(object(&path).first()), ErrorKind::Unavailable);
    let mut local = object(&data_path("bucket/key"));
    local.first().unwrap();
    let foreign = IoResult::Child(ChildOutcome::new(Ok::<bool, ()>(false)));
    assert_eq!(kind_of(local.next(foreign)), ErrorKind::NotFound);
}

#[test]
fn unmarked_version_passes() {
    let version = Ulid::from_parts(5, 5);
    let mut read = Unmarked::new(("b", "k"), None);
    assert!(matches!(read.first(), Ok(Next::Call(IoCall::Child(_)))));
    let marks = read.next(child::<Option<Ulid>>(Some(version)));
    assert!(matches!(marks, Ok(Next::Call(IoCall::Child(_)))));
    let clean = IoResult::Child(ChildOutcome::new(Ok::<bool, StoreError>(false)));
    assert_eq!(read.next(clean), Ok(Next::Done(Some(version))));
}

#[test]
fn marked_version_refused() {
    let mut read = Unmarked::new(("b", "k"), None);
    read.next(child::<Option<Ulid>>(Some(Ulid::from_parts(5, 5))))
        .unwrap();
    let marked = IoResult::Child(ChildOutcome::new(Ok::<bool, StoreError>(true)));
    assert_eq!(kind_of(read.next(marked)), ErrorKind::Denied);
}

#[test]
fn missing_object_reads_nothing() {
    let mut read = Unmarked::new(("b", "k"), None);
    let none = read.next(child::<Option<Ulid>>(None));
    assert_eq!(none, Ok(Next::Done(None)));
    let mut failed = Unmarked::new(("b", "k"), None);
    let down = IoResult::Child(ChildOutcome::new(Err::<Option<Ulid>, _>(
        "down".to_string(),
    )));
    assert_eq!(kind_of(failed.next(down)), ErrorKind::Unavailable);
}
