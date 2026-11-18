//! Tests the steps of a secret release as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::secrets::SecretKind;

use super::*;
use crate::plugins::rows::tests::{child_result, values_result};
use crate::plugins::work::tests::sample_install;

fn bound_install(binding: &str) -> PluginInstall {
    let mut install = sample_install(Ulid::from_parts(1, 1));
    let binding = SecretBinding::parse(binding).unwrap();
    install.secrets.insert("slot".to_string(), binding);
    install
}

fn config_of(install: PluginInstall) -> Arc<RealmConfigDocument> {
    let mut config = RealmConfigDocument::new(install.account.realm_id, Vec::new(), 3);
    config.installs.push(install);
    Arc::new(config)
}

fn request(install: &PluginInstall) -> ReleaseRequest {
    ReleaseRequest {
        install_id: install.install_id,
        slot: "slot".to_string(),
        delivery: Delivery::Inject,
        target: None,
        held: None,
        run_id: None,
    }
}

fn other_node() -> NodeId {
    iroh::SecretKey::from_bytes(&[8u8; 32]).public()
}

fn release_of(install: &PluginInstall, peer: NodeId) -> ReleaseSecret {
    let nodes = (install.receiver, peer);
    ReleaseSecret::new(config_of(install.clone()), nodes, request(install))
}

fn refused(refusal: Refusal) -> ReleaseFailure {
    ReleaseFailure(ReleaseError::Refused(refusal))
}

fn written() -> IoResult {
    child_result(Ok::<(), StoreError>(()))
}

fn is_child<T>(next: Next<T>) -> bool {
    matches!(next, Next::Call(IoCall::Child(_)))
}

fn selected(value_id: Ulid) -> Selected {
    Selected {
        kind: SecretKind::parse("bearer").unwrap(),
        settings: SecretSettings::new(),
        rules: Vec::new(),
        incarnation: None,
        value_id,
        sealed: Sealed::Node(value_id),
    }
}

fn admitted(value_id: Ulid) -> Box<Admitted> {
    Box::new(Admitted {
        selected: selected(value_id),
        exposure: ResponseExposure::Full,
        origin: None,
    })
}

#[test]
fn unknown_install_refused() {
    let install = bound_install("group:api");
    let mut other = request(&install);
    other.install_id = Ulid::from_parts(9, 9);
    let nodes = (install.receiver, install.receiver);
    let mut release = ReleaseSecret::new(config_of(install), nodes, other);
    assert_eq!(release.first(), Err(refused(Refusal::Binding)));
}

#[test]
fn wrong_peer_audited() {
    let install = bound_install("group:api");
    let mut release = release_of(&install, other_node());
    assert!(is_child(release.first().unwrap()), "the audit row");
    assert_eq!(release.next(written()), Err(refused(Refusal::Peer)));
}

#[test]
fn failed_audit_still_refuses() {
    let install = bound_install("group:api");
    let mut release = release_of(&install, other_node());
    release.first().unwrap();
    let failed = child_result(Err::<(), _>(StoreError("down".into())));
    assert_eq!(release.next(failed), Err(refused(Refusal::Peer)));
}

#[test]
fn node_secret_stays_home() {
    let install = bound_install("node:api");
    let mut release = release_of(&install, install.receiver);
    release.local = other_node();
    assert!(is_child(release.first().unwrap()));
    assert_eq!(release.next(written()), Err(refused(Refusal::Binding)));
}

#[test]
fn missing_entry_refused() {
    let install = bound_install("group:api");
    let mut release = release_of(&install, install.receiver);
    assert!(is_child(release.first().unwrap()), "the selection");
    let none = child_result(Ok::<Option<Selected>, ReleaseFailure>(None));
    assert!(is_child(release.next(none).unwrap()), "the audit row");
    assert_eq!(release.next(written()), Err(refused(Refusal::Entry)));
}

#[test]
fn held_value_not_opened() {
    let install = bound_install("group:api");
    let value = Ulid::from_parts(5, 5);
    let mut release = release_of(&install, install.receiver);
    release.request.held = Some(value);
    release.phase = ReleasePhase::Auditing(admitted(value));
    let Next::Done(answer) = release.next(written()).unwrap() else {
        panic!("a held value needs no opening");
    };
    assert_eq!((answer.value_id, answer.fields), (value, None));
}

#[test]
fn audit_precedes_opening() {
    let install = bound_install("group:api");
    let value = Ulid::from_parts(5, 5);
    let mut release = release_of(&install, install.receiver);
    release.phase = ReleasePhase::Auditing(admitted(value));
    let failed = child_result(Err::<(), _>(StoreError("down".into())));
    let answer = release.next(failed).unwrap_err();
    assert!(matches!(answer.0, ReleaseError::Unavailable(_)));
    release.phase = ReleasePhase::Auditing(admitted(value));
    assert!(is_child(release.next(written()).unwrap()), "the opening");
    let bytes = SecretBytes::new(vec![1, 2]);
    let opened = child_result(Ok::<_, ReleaseFailure>(bytes.clone()));
    let Next::Done(answer) = release.next(opened).unwrap() else {
        panic!("the value ends the release");
    };
    assert_eq!(answer.fields, Some(bytes));
}

#[test]
fn inactive_install_refused() {
    let mut install = bound_install("group:api");
    install.state = InstallState::Paused;
    let mut release = release_of(&install, install.receiver);
    release.phase = ReleasePhase::ReadingAccount(admitted(Ulid::from_parts(5, 5)));
    let answer = release.next(values_result(vec![None]));
    assert!(is_child(answer.unwrap()), "no account refuses with a row");
    assert_eq!(release.next(written()), Err(refused(Refusal::Authority)));
}

fn select_of(binding: &str, config: Arc<RealmConfigDocument>) -> SelectSecret {
    let install = bound_install(binding);
    let binding = install.secrets["slot"].clone();
    SelectSecret::new(config, &install, &binding)
}

#[test]
fn selects_by_scope() {
    let config = config_of(bound_install("group:api"));
    let mut node = select_of("node:api", config.clone());
    assert!(matches!(node.first(), Ok(Next::Call(IoCall::Read(_)))));
    assert_eq!(node.next(values_result(vec![None])), Ok(Next::Done(None)));
    let mut realm = select_of("realm:api", config.clone());
    assert_eq!(realm.first(), Ok(Next::Done(None)));
    let mut group = select_of("group:api", config);
    assert!(is_child(group.first().unwrap()));
    let none = child_result(Ok::<Option<SecretEntry>, EntryError>(None));
    assert_eq!(group.next(none), Ok(Next::Done(None)));
}

fn storage(event: StorageEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Storage(event)))
}

#[test]
fn opens_node_secret() {
    let realm = RealmId::from_bytes([1u8; 32]);
    let mut open = OpenSealed::new(realm, Sealed::Node(Ulid::from_parts(5, 5)));
    assert!(matches!(open.first(), Ok(Next::Call(IoCall::Effect(_)))));
    let answer = open.next(storage(StorageEvent::RealmOpened {
        result: Ok(SecretBytes::new(vec![1])),
    }));
    assert!(matches!(
        answer,
        Err(ReleaseFailure(ReleaseError::Unavailable(_)))
    ));
}
